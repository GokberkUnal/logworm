package com.gokgor.logworm.topic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.gokgor.logworm.kafka.LogwormKafkaProperties;

class TopicServiceTest {

    private static final Node N1 = new Node(1, "b1", 9092);
    private static final Node N2 = new Node(2, "b2", 9092);

    private final AdminClient adminClient = mock(AdminClient.class);
    private TopicService service;

    @BeforeEach
    void setUp() {
        service = new TopicService(adminClient, new LogwormKafkaProperties(Duration.ofSeconds(1)));
    }

    @Test
    void listsTopicsSortedByNameWithInternalFlag() {
        var names = mock(ListTopicsResult.class);
        when(names.names()).thenReturn(KafkaFuture.completedFuture(Set.of("zeta", "__consumer_offsets", "alpha")));
        when(adminClient.listTopics(any())).thenReturn(names);
        stubDescribe(Map.of(
                "zeta", topic("zeta", false, partition(0, N1, List.of(N1, N2), List.of(N1))),
                "__consumer_offsets", topic("__consumer_offsets", true, partition(0, N1, List.of(N1), List.of(N1))),
                "alpha", topic("alpha", false, partition(0, N1, List.of(N1), List.of(N1)), partition(1, N2, List.of(N2), List.of(N2)))));

        var topics = service.listTopics();

        assertThat(topics).extracting(TopicSummary::name).containsExactly("__consumer_offsets", "alpha", "zeta");
        assertThat(topics.get(0).internal()).isTrue();
        assertThat(topics.get(1)).isEqualTo(new TopicSummary("alpha", 2, 1, false));
        assertThat(topics.get(2).replicationFactor()).isEqualTo(2);
    }

    @Test
    void emptyClusterYieldsEmptyListWithoutDescribing() {
        var names = mock(ListTopicsResult.class);
        when(names.names()).thenReturn(KafkaFuture.completedFuture(Set.of()));
        when(adminClient.listTopics(any())).thenReturn(names);

        assertThat(service.listTopics()).isEmpty();
    }

    @Test
    void topicDetailComputesOffsetsAndCounts() {
        stubDescribe(Map.of("demo", topic("demo", false,
                partition(1, N2, List.of(N2, N1), List.of(N2)),
                partition(0, N1, List.of(N1, N2), List.of(N1, N2)))));
        stubOffsets(Map.of(
                new TopicPartition("demo", 0), new long[] {10, 25},
                new TopicPartition("demo", 1), new long[] {0, 7}));

        var detail = service.getTopic("demo");

        assertThat(detail.partitionCount()).isEqualTo(2);
        assertThat(detail.replicationFactor()).isEqualTo(2);
        assertThat(detail.messageCount()).isEqualTo(22);
        assertThat(detail.partitions()).extracting(PartitionInfo::id).containsExactly(0, 1);

        var p0 = detail.partitions().get(0);
        assertThat(p0.leader()).isEqualTo(1);
        assertThat(p0.replicas()).containsExactly(1, 2);
        assertThat(p0.inSyncReplicas()).containsExactly(1, 2);
        assertThat(p0.earliestOffset()).isEqualTo(10);
        assertThat(p0.latestOffset()).isEqualTo(25);
        assertThat(p0.messageCount()).isEqualTo(15);

        var p1 = detail.partitions().get(1);
        assertThat(p1.inSyncReplicas()).containsExactly(2);
        assertThat(p1.messageCount()).isEqualTo(7);
    }

    @Test
    void leaderIsNullWhenPartitionHasNoLeader() {
        stubDescribe(Map.of("demo", topic("demo", false, partition(0, null, List.of(N1), List.of()))));
        stubOffsets(Map.of(new TopicPartition("demo", 0), new long[] {0, 0}));

        var p0 = service.getTopic("demo").partitions().get(0);

        assertThat(p0.leader()).isNull();
        assertThat(p0.inSyncReplicas()).isEmpty();
    }

    @Test
    void unknownTopicBecomesTopicNotFound() {
        var failed = new KafkaFutureImpl<Map<String, TopicDescription>>();
        failed.completeExceptionally(new UnknownTopicOrPartitionException("nope"));
        var result = mock(DescribeTopicsResult.class);
        when(result.allTopicNames()).thenReturn(failed);
        when(adminClient.describeTopics(any(java.util.Collection.class), any())).thenReturn(result);

        assertThatThrownBy(() -> service.getTopic("nope"))
                .isInstanceOf(TopicNotFoundException.class)
                .hasMessage("Topic not found: nope");
    }

    // --- stubs -----------------------------------------------------------------------------

    private void stubDescribe(Map<String, TopicDescription> descriptions) {
        var result = mock(DescribeTopicsResult.class);
        when(result.allTopicNames()).thenReturn(KafkaFuture.completedFuture(descriptions));
        when(adminClient.describeTopics(any(java.util.Collection.class), any())).thenReturn(result);
    }

    /** Values are {earliest, latest}; the stub answers based on which OffsetSpec was requested. */
    @SuppressWarnings("unchecked")
    private void stubOffsets(Map<TopicPartition, long[]> offsets) {
        when(adminClient.listOffsets(anyMap(), any())).thenAnswer(invocation -> {
            Map<TopicPartition, OffsetSpec> request = invocation.getArgument(0);
            boolean earliest = request.values().iterator().next() instanceof OffsetSpec.EarliestSpec;
            Map<TopicPartition, ListOffsetsResultInfo> answer = new java.util.HashMap<>();
            request.keySet().forEach(tp -> answer.put(tp,
                    new ListOffsetsResultInfo(offsets.get(tp)[earliest ? 0 : 1], -1L, Optional.empty())));
            var result = mock(ListOffsetsResult.class);
            when(result.all()).thenReturn(KafkaFuture.completedFuture(answer));
            return result;
        });
    }

    private static TopicDescription topic(String name, boolean internal, TopicPartitionInfo... partitions) {
        return new TopicDescription(name, internal, List.of(partitions));
    }

    private static TopicPartitionInfo partition(int id, Node leader, List<Node> replicas, List<Node> isr) {
        return new TopicPartitionInfo(id, leader, replicas, isr);
    }
}
