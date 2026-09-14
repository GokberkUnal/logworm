package com.gokgor.logworm.stream;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import com.gokgor.logworm.message.MessageDecoder;
import com.gokgor.logworm.message.RawMessage;

import lombok.extern.slf4j.Slf4j;

/**
 * The tail loop: seek to the end, poll, filter, rate-limit, emit.
 *
 * <p>Backpressure has two layers. Per second, at most {@code rate} matching records are sent;
 * the rest are dropped and counted. If the consumer itself falls more than
 * {@code rate × lagSkipFactor} records behind (the client cannot keep up with the topic at all),
 * it jumps to the end and reports how many records were skipped.
 */
@Slf4j
final class LiveTail implements Runnable {

    private static final Duration POLL = Duration.ofMillis(200);
    private static final long WINDOW_NANOS = Duration.ofSeconds(1).toNanos();

    private final Consumer<byte[], byte[]> consumer;
    private final StreamSink sink;
    private final String topic;
    private final List<TopicPartition> partitions;
    private final StreamQuery query;
    private final MessageDecoder decoder;
    private final Duration heartbeat;
    private final int lagSkipFactor;
    private final LongSupplier nanoClock;

    LiveTail(Consumer<byte[], byte[]> consumer, StreamSink sink, String topic, List<TopicPartition> partitions,
            StreamQuery query, MessageDecoder decoder, Duration heartbeat, int lagSkipFactor, LongSupplier nanoClock) {
        this.consumer = consumer;
        this.sink = sink;
        this.topic = topic;
        this.partitions = partitions;
        this.query = query;
        this.decoder = decoder;
        this.heartbeat = heartbeat;
        this.lagSkipFactor = lagSkipFactor;
        this.nanoClock = nanoClock;
    }

    @Override
    public void run() {
        try (consumer) {
            consumer.assign(partitions);
            consumer.seekToEnd(partitions);
            Map<Integer, Long> startOffsets = new HashMap<>();
            for (TopicPartition tp : partitions) {
                startOffsets.put(tp.partition(), consumer.position(tp));
            }
            sink.send("connected", new StreamEvents.Connected(topic, startOffsets, query.rate()));

            Predicate<RawMessage> filter = query.filter();
            long windowStart = nanoClock.getAsLong();
            long lastActivity = windowStart;
            long emitted = 0;
            long dropped = 0;

            while (sink.isOpen()) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(POLL)) {
                    RawMessage raw = decoder.toRaw(record);
                    if (!filter.test(raw)) {
                        continue;
                    }
                    if (emitted >= query.rate()) {
                        dropped++;
                        continue;
                    }
                    sink.send("message", decoder.toMessage(raw, query.format()));
                    emitted++;
                    lastActivity = nanoClock.getAsLong();
                }

                long now = nanoClock.getAsLong();
                if (now - windowStart >= WINDOW_NANOS) {
                    long skipped = skipIfTooFarBehind();
                    if (dropped > 0 || skipped > 0) {
                        sink.send("stats", new StreamEvents.Stats(emitted, dropped, skipped));
                        lastActivity = now;
                    }
                    windowStart = now;
                    emitted = 0;
                    dropped = 0;
                }
                if (now - lastActivity >= heartbeat.toNanos()) {
                    sink.comment("keep-alive");
                    lastActivity = now;
                }
            }
            sink.complete();
        } catch (IOException e) {
            log.debug("Live tail of {} ended: {}", topic, e.getMessage());
            sink.complete();
        } catch (RuntimeException e) {
            log.warn("Live tail of {} failed", topic, e);
            sink.fail(e);
        }
    }

    /** @return number of records jumped over, 0 if the consumer was keeping up */
    private long skipIfTooFarBehind() {
        long threshold = (long) query.rate() * lagSkipFactor;
        Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
        long lag = 0;
        for (TopicPartition tp : partitions) {
            lag += ends.get(tp) - consumer.position(tp);
        }
        if (lag <= threshold) {
            return 0;
        }
        consumer.seekToEnd(partitions);
        for (TopicPartition tp : partitions) {
            consumer.position(tp); // materialise the lazy seek
        }
        return lag;
    }
}
