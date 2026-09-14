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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.stereotype.Service;

import com.gokgor.logworm.kafka.LogwormKafkaProperties;
import com.gokgor.logworm.topic.TopicNotFoundException;

/**
 * Reads records with a short-lived, group-less consumer per request:
 * assign the partitions, seek to the computed start offsets, poll until every
 * partition reached its target end offset (or the request timeout hits).
 */
@Service
public class MessageService {

    private static final Duration POLL = Duration.ofMillis(250);

    private final Map<String, Object> consumerConfig;
    private final Duration timeout;
    private final MessageDecoder decoder;

    public MessageService(KafkaProperties kafkaProperties, LogwormKafkaProperties logwormProperties, MessageDecoder decoder) {
        this.consumerConfig = consumerConfig(kafkaProperties);
        this.timeout = logwormProperties.requestTimeout();
        this.decoder = decoder;
    }

    public MessagePage read(String topic, MessageQuery query) {
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig)) {
            List<TopicPartition> partitions = resolvePartitions(consumer, topic, query.partition());
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

    private List<TopicPartition> resolvePartitions(Consumer<byte[], byte[]> consumer, String topic, Integer partition) {
        List<PartitionInfo> infos = consumer.partitionsFor(topic, timeout);
        if (infos == null || infos.isEmpty()) {
            throw new TopicNotFoundException(topic);
        }
        if (partition == null) {
            return infos.stream().map(i -> new TopicPartition(topic, i.partition())).toList();
        }
        boolean exists = infos.stream().anyMatch(i -> i.partition() == partition);
        if (!exists) {
            throw new InvalidQueryException("Topic " + topic + " has no partition " + partition);
        }
        return List.of(new TopicPartition(topic, partition));
    }

    private static Map<String, Object> consumerConfig(KafkaProperties kafkaProperties) {
        Map<String, Object> config = new HashMap<>(kafkaProperties.buildConsumerProperties());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "logworm-browser");
        config.remove(ConsumerConfig.GROUP_ID_CONFIG); // manual assignment, no group membership
        return config;
    }
}
