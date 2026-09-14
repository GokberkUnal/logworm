package com.gokgor.logworm.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.gokgor.logworm.message.KafkaMessage;
import com.gokgor.logworm.message.MessageDecoder;
import com.gokgor.logworm.message.ValueFormat;

import tools.jackson.databind.json.JsonMapper;

/**
 * Drives the loop deterministically on the test thread: MockConsumer runs scheduled tasks on each
 * poll, so tasks add records, advance the fake clock and finally close the sink.
 */
class LiveTailTest {

    private static final String TOPIC = "t";
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
    private static final long SECOND = Duration.ofSeconds(1).toNanos();

    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("latest");
    private final RecordingSink sink = new RecordingSink();
    private final AtomicLong clock = new AtomicLong();
    private final MessageDecoder decoder = new MessageDecoder(JsonMapper.builder().build());
    private long nextOffset = 100;

    @BeforeEach
    void setUp() {
        consumer.updateEndOffsets(Map.of(P0, 100L));
    }

    @Test
    void emitsConnectedThenMatchingMessagesAndCompletesWhenClientLeaves() {
        var tail = tail(query(null, "ERROR", 10), 10);
        consumer.schedulePollTask(() -> {
            add("k1", "{\"level\":\"ERROR\"}");
            add("k2", "{\"level\":\"INFO\"}");
            add("k3", "{\"level\":\"ERROR\"}");
        });
        consumer.schedulePollTask(() -> sink.open = false);

        tail.run();

        assertThat(sink.events.getFirst().name()).isEqualTo("connected");
        var connected = (StreamEvents.Connected) sink.events.getFirst().data();
        assertThat(connected.topic()).isEqualTo(TOPIC);
        assertThat(connected.startOffsets()).containsEntry(0, 100L);
        assertThat(connected.rate()).isEqualTo(10);

        var messages = sink.data("message").stream().map(KafkaMessage.class::cast).toList();
        assertThat(messages).extracting(KafkaMessage::key).containsExactly("k1", "k3");
        assertThat(messages.getFirst().valueFormat()).isEqualTo("json");
        assertThat(sink.data("stats")).isEmpty();
        assertThat(sink.completed).isTrue();
        assertThat(consumer.closed()).isTrue();
    }

    @Test
    void rateLimitDropsExcessAndReportsStatsOncePerWindow() {
        var tail = tail(query(null, null, 2), 10);
        consumer.schedulePollTask(() -> {
            for (int i = 0; i < 5; i++) {
                add("k" + i, "v");
            }
        });
        consumer.schedulePollTask(() -> clock.addAndGet(SECOND)); // window closes → stats
        consumer.schedulePollTask(() -> add("k5", "v"));             // new window, allowed again
        consumer.schedulePollTask(() -> sink.open = false);

        tail.run();

        assertThat(sink.data("message")).hasSize(3);
        var stats = sink.data("stats").stream().map(StreamEvents.Stats.class::cast).toList();
        assertThat(stats).containsExactly(new StreamEvents.Stats(2, 3, 0));
    }

    @Test
    void jumpsToEndWhenConsumerFallsTooFarBehind() {
        var tail = tail(query(null, null, 1), 5); // threshold = 5 records
        consumer.schedulePollTask(() -> {
            consumer.updateEndOffsets(Map.of(P0, 150L)); // 50 unread records piled up
            clock.addAndGet(SECOND);
        });
        var positionAfterSkip = new AtomicLong();
        consumer.schedulePollTask(() -> {
            positionAfterSkip.set(consumer.position(P0));
            sink.open = false;
        });

        tail.run();

        var stats = sink.data("stats").stream().map(StreamEvents.Stats.class::cast).toList();
        assertThat(stats).containsExactly(new StreamEvents.Stats(0, 0, 50));
        assertThat(positionAfterSkip.get()).isEqualTo(150L);
    }

    @Test
    void sendsKeepAliveWhenIdle() {
        var tail = tail(query(null, null, 10), 10);
        consumer.schedulePollTask(() -> clock.addAndGet(Duration.ofSeconds(31).toNanos()));
        consumer.schedulePollTask(() -> sink.open = false);

        tail.run();

        assertThat(sink.comments).containsExactly("keep-alive");
        assertThat(sink.data("stats")).isEmpty();
    }

    @Test
    void failsSinkOnUnexpectedError() {
        var tail = tail(query(null, null, 10), 10);
        consumer.schedulePollTask(() -> {
            throw new IllegalStateException("boom");
        });

        tail.run();

        assertThat(sink.failure).isInstanceOf(IllegalStateException.class);
        assertThat(sink.completed).isFalse();
        assertThat(consumer.closed()).isTrue();
    }

    private LiveTail tail(StreamQuery query, int lagSkipFactor) {
        return new LiveTail(consumer, sink, TOPIC, List.of(P0), query, decoder,
                Duration.ofSeconds(30), lagSkipFactor, clock::get);
    }

    private static StreamQuery query(String key, String value, int rate) {
        return new StreamQuery(null, key, value, null, ValueFormat.AUTO, rate);
    }

    private void add(String key, String value) {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        byte[] v = value.getBytes(StandardCharsets.UTF_8);
        long offset = nextOffset++;
        consumer.updateEndOffsets(Map.of(P0, offset + 1));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, offset, TimestampType.CREATE_TIME,
                k.length, v.length, k, v, new RecordHeaders(), Optional.empty()));
    }
}
