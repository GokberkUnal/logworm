package com.gokgor.logworm.message;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import com.gokgor.logworm.kafka.BrowserConsumerFactory;
import com.gokgor.logworm.kafka.LogwormKafkaProperties;
import com.gokgor.logworm.kafka.PartitionResolver;

/**
 * Reads records with a short-lived, group-less consumer per request:
 * assign the partitions, seek to the computed start offsets, poll until every
 * partition reached its target end offset (or the request timeout hits).
 */
@Service
public class MessageService {

    private static final Duration POLL = Duration.ofMillis(250);

    private final BrowserConsumerFactory consumerFactory;
    private final Duration timeout;
    private final MessageDecoder decoder;

    public MessageService(BrowserConsumerFactory consumerFactory, LogwormKafkaProperties properties, MessageDecoder decoder) {
        this.consumerFactory = consumerFactory;
        this.timeout = properties.requestTimeout();
        this.decoder = decoder;
    }

    public MessagePage read(String topic, MessageQuery query) {
        try (Consumer<byte[], byte[]> consumer = consumerFactory.create("logworm-browser")) {
            List<TopicPartition> partitions = PartitionResolver.resolve(consumer, topic, query.partition(), timeout);
            consumer.assign(partitions);

            Map<TopicPartition, Long> beginning = consumer.beginningOffsets(partitions, timeout);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions, timeout);

            // exclusive stop offset per partition
            Map<TopicPartition, Long> stop = new HashMap<>();
            Set<TopicPartition> pending = new HashSet<>();
            for (TopicPartition tp : partitions) {
                long begin = beginning.get(tp);
                long endOffset = end.get(tp);
                long start;
                long stopOffset;
                if (query.isTail()) {
                    stopOffset = endOffset;
                    start = Math.max(begin, endOffset - query.scanWindow());
                } else {
                    start = Math.clamp(query.offset(), begin, endOffset);
                    stopOffset = Math.min(start + query.scanWindow(), endOffset);
                }
                consumer.seek(tp, start);
                stop.put(tp, stopOffset);
                if (start < stopOffset) {
                    pending.add(tp);
                }
            }

            var filter = MessageFilter.from(query);
            List<RawMessage> matched = new ArrayList<>();
            int scanned = 0;
            long deadline = System.nanoTime() + timeout.toNanos();

            while (!pending.isEmpty() && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(POLL)) {
                    var tp = new TopicPartition(record.topic(), record.partition());
                    if (record.offset() >= stop.get(tp)) {
                        continue;
                    }
                    scanned++;
                    RawMessage raw = decoder.toRaw(record);
                    if (filter.test(raw)) {
                        matched.add(raw);
                    }
                }
                pending.removeIf(tp -> consumer.position(tp) >= stop.get(tp));
            }

            List<KafkaMessage> messages = matched.stream()
                    .sorted(query.isTail() ? NEWEST_FIRST : OLDEST_FIRST)
                    .limit(query.limit())
                    .map(raw -> decoder.toMessage(raw, query.format()))
                    .toList();

            return new MessagePage(topic, messages.size(), scanned, messages);
        }
    }

    private static final Comparator<RawMessage> OLDEST_FIRST =
            Comparator.comparingLong((RawMessage m) -> m.record().offset());

    private static final Comparator<RawMessage> NEWEST_FIRST =
            Comparator.comparingLong((RawMessage m) -> m.record().timestamp()).reversed()
                    .thenComparing(Comparator.comparingLong((RawMessage m) -> m.record().offset()).reversed())
                    .thenComparingInt(m -> m.record().partition());
}
