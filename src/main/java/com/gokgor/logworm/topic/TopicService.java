package com.gokgor.logworm.topic;

import static com.gokgor.logworm.kafka.KafkaFutures.await;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Service;

import com.gokgor.logworm.kafka.LogwormKafkaProperties;
import com.gokgor.logworm.kafka.KafkaRequestException;

@Service
public class TopicService {

    private final Admin adminClient;
    private final Duration timeout;

    public TopicService(Admin adminClient, LogwormKafkaProperties properties) {
        this.adminClient = adminClient;
        this.timeout = properties.requestTimeout();
    }

    /** Lists all topics, internal ones (e.g. __consumer_offsets) included, sorted by name. */
    public List<TopicSummary> listTopics() {
        var names = await(adminClient
                .listTopics(new ListTopicsOptions().listInternal(true).timeoutMs(timeoutMs()))
                .names(), timeout);

        if (names.isEmpty()) {
            return List.of();
        }

        return describe(names).values().stream()
                .map(d -> new TopicSummary(
                        d.name(),
                        d.partitions().size(),
                        replicationFactor(d),
                        d.isInternal()))
                .sorted(Comparator.comparing(TopicSummary::name))
                .toList();
    }

    public TopicDetail getTopic(String name) {
        TopicDescription description;
        try {
            description = describe(Set.of(name)).get(name);
        } catch (UnknownTopicOrPartitionException e) {
            throw new TopicNotFoundException(name);
        }
        if (description == null) {
            throw new TopicNotFoundException(name);
        }

        var topicPartitions = description.partitions().stream()
                .map(p -> new TopicPartition(name, p.partition()))
                .toList();
        var earliest = listOffsets(topicPartitions, OffsetSpec.earliest());
        var latest = listOffsets(topicPartitions, OffsetSpec.latest());

        var partitions = description.partitions().stream()
                .sorted(Comparator.comparingInt(TopicPartitionInfo::partition))
                .map(p -> {
                    var tp = new TopicPartition(name, p.partition());
                    long start = earliest.get(tp).offset();
                    long end = latest.get(tp).offset();
                    return new PartitionInfo(
                            p.partition(),
                            p.leader() != null ? p.leader().id() : null,
                            ids(p.replicas()),
                            ids(p.isr()),
                            start,
                            end,
                            end - start);
                })
                .toList();

        long messageCount = partitions.stream().mapToLong(PartitionInfo::messageCount).sum();

        return new TopicDetail(
                name,
                description.isInternal(),
                partitions.size(),
                replicationFactor(description),
                messageCount,
                partitions);
    }

    private Map<String, TopicDescription> describe(Set<String> names) {
        var result = adminClient.describeTopics(names, new DescribeTopicsOptions().timeoutMs(timeoutMs()));
        return await(result.allTopicNames(), timeout);
    }

    private Map<TopicPartition, ListOffsetsResultInfo> listOffsets(List<TopicPartition> partitions, OffsetSpec spec) {
        var request = partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> spec));
        var result = adminClient.listOffsets(request, new ListOffsetsOptions().timeoutMs(timeoutMs()));
        return await(result.all(), timeout);
    }

    private static int replicationFactor(TopicDescription description) {
        // Replication factor is a per-topic setting; every partition has the same number of replicas.
        return description.partitions().isEmpty() ? 0 : description.partitions().getFirst().replicas().size();
    }

    private static List<Integer> ids(List<Node> nodes) {
        return nodes.stream().map(Node::id).toList();
    }

    private int timeoutMs() {
        return (int) timeout.toMillis();
    }
}
