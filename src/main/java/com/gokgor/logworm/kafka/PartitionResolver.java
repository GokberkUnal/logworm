package com.gokgor.logworm.kafka;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.gokgor.logworm.message.InvalidQueryException;
import com.gokgor.logworm.topic.TopicNotFoundException;

public final class PartitionResolver {

    private PartitionResolver() {
    }

    /** All partitions of the topic, or just the requested one. 404 if the topic is unknown, 400 if the partition is. */
    public static List<TopicPartition> resolve(Consumer<?, ?> consumer, String topic, Integer partition, Duration timeout) {
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
}
