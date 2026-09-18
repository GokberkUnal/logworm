package com.gokgor.logworm.consumergroup;

import static com.gokgor.logworm.kafka.KafkaFutures.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.springframework.stereotype.Service;

import com.gokgor.logworm.cluster.BrokerInfo;
import com.gokgor.logworm.kafka.LogwormKafkaProperties;

/**
 * Consumer group monitoring. Every call is a fixed number of Admin round trips regardless
 * of group count: list → describe (all groups at once) → committed offsets (all groups at once)
 * → end offsets (union of all partitions once).
 */
@Service
public class ConsumerGroupService {

    private final Admin adminClient;
    private final Duration timeout;

    public ConsumerGroupService(Admin adminClient, LogwormKafkaProperties properties) {
        this.adminClient = adminClient;
        this.timeout = properties.requestTimeout();
    }

    public List<ConsumerGroupSummary> listGroups() {
        Collection<ConsumerGroupListing> listings = await(adminClient
                .listConsumerGroups(new ListConsumerGroupsOptions().timeoutMs(timeoutMs()))
                .all(), timeout);
        if (listings.isEmpty()) {
            return List.of();
        }
        List<String> ids = listings.stream().map(ConsumerGroupListing::groupId).sorted().toList();

        Map<String, ConsumerGroupDescription> descriptions = describe(ids);
        Map<String, Map<TopicPartition, OffsetAndMetadata>> committed = committedOffsets(ids);
        Map<TopicPartition, Long> endOffsets = endOffsets(committed.values().stream()
                .flatMap(m -> m.keySet().stream())
                .collect(Collectors.toSet()));

        List<ConsumerGroupSummary> summaries = new ArrayList<>();
        for (String id : ids) {
            ConsumerGroupDescription d = descriptions.get(id);
            Map<TopicPartition, OffsetAndMetadata> offsets = committed.getOrDefault(id, Map.of());
            Set<String> topics = new TreeSet<>();
            offsets.keySet().forEach(tp -> topics.add(tp.topic()));
            d.members().forEach(m -> m.assignment().topicPartitions().forEach(tp -> topics.add(tp.topic())));
            summaries.add(new ConsumerGroupSummary(
                    id,
                    d.groupState().toString(),
                    d.type().toString(),
                    d.isSimpleConsumerGroup(),
                    d.members().size(),
                    List.copyOf(topics),
                    totalLag(offsets, endOffsets)));
        }
        return summaries;
    }

    public ConsumerGroupDetail getGroup(String groupId) {
        ConsumerGroupDescription d;
        Map<TopicPartition, OffsetAndMetadata> offsets;
        try {
            d = describe(List.of(groupId)).get(groupId);
            offsets = committedOffsets(List.of(groupId)).getOrDefault(groupId, Map.of());
        } catch (GroupIdNotFoundException e) {
            throw new ConsumerGroupNotFoundException(groupId);
        }
        // The broker answers DEAD (rather than an error) for a group it has never heard of.
        if (d == null || (d.groupState() == GroupState.DEAD && d.members().isEmpty() && offsets.isEmpty())) {
            throw new ConsumerGroupNotFoundException(groupId);
        }

        Map<TopicPartition, String> assignedTo = new HashMap<>();
        List<MemberInfo> members = new ArrayList<>();
        for (MemberDescription m : sortedMembers(d)) {
            var assignments = m.assignment().topicPartitions().stream()
                    .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                    .map(tp -> new MemberInfo.PartitionRef(tp.topic(), tp.partition()))
                    .toList();
            m.assignment().topicPartitions().forEach(tp -> assignedTo.put(tp, m.consumerId()));
            members.add(new MemberInfo(m.consumerId(), m.groupInstanceId().orElse(null), m.clientId(), m.host(), assignments));
        }

        Set<TopicPartition> partitions = new HashSet<>(offsets.keySet());
        partitions.addAll(assignedTo.keySet());
        Map<TopicPartition, Long> endOffsets = endOffsets(partitions);

        List<PartitionLag> lags = partitions.stream()
                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                .map(tp -> {
                    Long committedOffset = Optional.ofNullable(offsets.get(tp)).map(OffsetAndMetadata::offset).orElse(null);
                    long end = endOffsets.getOrDefault(tp, 0L);
                    Long lag = committedOffset == null ? null : Math.max(0, end - committedOffset);
                    return new PartitionLag(tp.topic(), tp.partition(), committedOffset, end, lag, assignedTo.get(tp));
                })
                .toList();

        return new ConsumerGroupDetail(
                groupId,
                d.groupState().toString(),
                d.type().toString(),
                d.isSimpleConsumerGroup(),
                d.partitionAssignor(),
                d.coordinator() != null ? BrokerInfo.from(d.coordinator()) : null,
                members,
                lags,
                lags.stream().filter(l -> l.lag() != null).mapToLong(PartitionLag::lag).sum());
    }

    // --- admin calls ---------------------------------------------------------------------------

    private Map<String, ConsumerGroupDescription> describe(Collection<String> ids) {
        return await(adminClient
                .describeConsumerGroups(ids, new DescribeConsumerGroupsOptions().timeoutMs(timeoutMs()))
                .all(), timeout);
    }

    private Map<String, Map<TopicPartition, OffsetAndMetadata>> committedOffsets(Collection<String> ids) {
        Map<String, ListConsumerGroupOffsetsSpec> specs = new LinkedHashMap<>();
        ids.forEach(id -> specs.put(id, new ListConsumerGroupOffsetsSpec()));
        var result = await(adminClient
                .listConsumerGroupOffsets(specs, new ListConsumerGroupOffsetsOptions().timeoutMs(timeoutMs()))
                .all(), timeout);
        // Kafka reports partitions without a commit as null values; drop them.
        Map<String, Map<TopicPartition, OffsetAndMetadata>> clean = new HashMap<>();
        result.forEach((group, offsets) -> {
            Map<TopicPartition, OffsetAndMetadata> nonNull = new HashMap<>();
            offsets.forEach((tp, om) -> {
                if (om != null) {
                    nonNull.put(tp, om);
                }
            });
            clean.put(group, nonNull);
        });
        return clean;
    }

    private Map<TopicPartition, Long> endOffsets(Set<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return Map.of();
        }
        var request = partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
        var result = await(adminClient
                .listOffsets(request, new ListOffsetsOptions().timeoutMs(timeoutMs()))
                .all(), timeout);
        Map<TopicPartition, Long> ends = new HashMap<>();
        result.forEach((tp, info) -> ends.put(tp, info.offset()));
        return ends;
    }

    // --- helpers -------------------------------------------------------------------------------

    private static long totalLag(Map<TopicPartition, OffsetAndMetadata> committed, Map<TopicPartition, Long> endOffsets) {
        long total = 0;
        for (var e : committed.entrySet()) {
            Long end = endOffsets.get(e.getKey());
            if (end != null) {
                total += Math.max(0, end - e.getValue().offset());
            }
        }
        return total;
    }

    private static List<MemberDescription> sortedMembers(ConsumerGroupDescription d) {
        return d.members().stream().sorted(Comparator.comparing(MemberDescription::consumerId)).toList();
    }

    private int timeoutMs() {
        return (int) timeout.toMillis();
    }
}
