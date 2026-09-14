package com.gokgor.logworm.consumergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.gokgor.logworm.kafka.LogwormKafkaProperties;

class ConsumerGroupServiceTest {

    private static final TopicPartition A0 = new TopicPartition("a", 0);
    private static final TopicPartition A1 = new TopicPartition("a", 1);
    private static final TopicPartition B0 = new TopicPartition("b", 0);
    private static final Node COORDINATOR = new Node(1, "b1", 9092);

    private final AdminClient adminClient = mock(AdminClient.class);
    private ConsumerGroupService service;

    @BeforeEach
    void setUp() {
        service = new ConsumerGroupService(adminClient, new LogwormKafkaProperties(Duration.ofSeconds(1)));
    }

    @Test
    void listsGroupsSortedWithTopicsMembersAndTotalLag() {
        stubListings("zeta", "alpha");
        stubDescriptions(Map.of(
                "alpha", description("alpha", GroupState.STABLE, member("m1", "c1", Set.of(A0, A1))),
                "zeta", description("zeta", GroupState.EMPTY)));
        stubCommitted(Map.of(
                "alpha", Map.of(A0, 10L, A1, 5L),
                "zeta", Map.of(B0, 3L)));
        stubEndOffsets(Map.of(A0, 12L, A1, 5L, B0, 100L));

        var groups = service.listGroups();

        assertThat(groups).extracting(ConsumerGroupSummary::groupId).containsExactly("alpha", "zeta");
        var alpha = groups.get(0);
        assertThat(alpha.state()).isEqualTo("Stable");
        assertThat(alpha.type()).isEqualTo("Classic");
        assertThat(alpha.memberCount()).isEqualTo(1);
        assertThat(alpha.topics()).containsExactly("a");
        assertThat(alpha.totalLag()).isEqualTo(2);
        var zeta = groups.get(1);
        assertThat(zeta.state()).isEqualTo("Empty");
        assertThat(zeta.memberCount()).isZero();
        assertThat(zeta.topics()).containsExactly("b");
        assertThat(zeta.totalLag()).isEqualTo(97);
    }

    @Test
    void emptyClusterMakesNoFurtherCalls() {
        stubListings();

        assertThat(service.listGroups()).isEmpty();
        verify(adminClient, never()).describeConsumerGroups(anyCollection(), any());
    }

    @Test
    void detailComputesPerPartitionLagAndAssignment() {
        stubDescriptions(Map.of("g", description("g", GroupState.STABLE,
                member("m1", "c1", Set.of(A0)),
                member("m2", "c2", Set.of(A1)))));
        // B0 has a commit but no member; A1 is assigned but never committed
        stubCommitted(Map.of("g", Map.of(A0, 7L, B0, 4L)));
        stubEndOffsets(Map.of(A0, 10L, A1, 20L, B0, 4L));

        var detail = service.getGroup("g");

        assertThat(detail.state()).isEqualTo("Stable");
        assertThat(detail.partitionAssignor()).isEqualTo("range");
        assertThat(detail.coordinator().id()).isEqualTo(1);
        assertThat(detail.members()).extracting(MemberInfo::memberId).containsExactly("m1", "m2");
        assertThat(detail.members().get(0).assignments())
                .containsExactly(new MemberInfo.PartitionRef("a", 0));

        assertThat(detail.partitions()).containsExactly(
                new PartitionLag("a", 0, 7L, 10L, 3L, "m1"),
                new PartitionLag("a", 1, null, 20L, null, "m2"),
                new PartitionLag("b", 0, 4L, 4L, 0L, null));
        assertThat(detail.totalLag()).isEqualTo(3);
    }

    @Test
    void lagNeverGoesNegative() {
        stubDescriptions(Map.of("g", description("g", GroupState.EMPTY)));
        stubCommitted(Map.of("g", Map.of(A0, 50L))); // committed beyond end (e.g. topic recreated)
        stubEndOffsets(Map.of(A0, 10L));

        assertThat(service.getGroup("g").partitions().getFirst().lag()).isZero();
    }

    @Test
    void deadGroupWithoutOffsetsIsNotFound() {
        stubDescriptions(Map.of("ghost", description("ghost", GroupState.DEAD)));
        stubCommitted(Map.of("ghost", Map.of()));

        assertThatThrownBy(() -> service.getGroup("ghost"))
                .isInstanceOf(ConsumerGroupNotFoundException.class)
                .hasMessage("Consumer group not found: ghost");
    }

    // --- stubs -----------------------------------------------------------------------------------

    private void stubListings(String... ids) {
        var result = mock(ListConsumerGroupsResult.class);
        List<ConsumerGroupListing> listings = java.util.Arrays.stream(ids)
                .map(id -> new ConsumerGroupListing(id, Optional.of(GroupState.STABLE), false))
                .toList();
        when(result.all()).thenReturn(KafkaFuture.completedFuture(listings));
        when(adminClient.listConsumerGroups(any())).thenReturn(result);
    }

    private void stubDescriptions(Map<String, ConsumerGroupDescription> descriptions) {
        var result = mock(DescribeConsumerGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(descriptions));
        when(adminClient.describeConsumerGroups(anyCollection(), any())).thenReturn(result);
    }

    private void stubCommitted(Map<String, Map<TopicPartition, Long>> committed) {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> converted = new HashMap<>();
        committed.forEach((g, m) -> {
            Map<TopicPartition, OffsetAndMetadata> om = new HashMap<>();
            m.forEach((tp, o) -> om.put(tp, new OffsetAndMetadata(o)));
            converted.put(g, om);
        });
        var result = mock(ListConsumerGroupOffsetsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(converted));
        when(adminClient.listConsumerGroupOffsets(anyMap(), any())).thenReturn(result);
    }

    private void stubEndOffsets(Map<TopicPartition, Long> ends) {
        Map<TopicPartition, ListOffsetsResultInfo> infos = new HashMap<>();
        ends.forEach((tp, o) -> infos.put(tp, new ListOffsetsResultInfo(o, -1L, Optional.empty())));
        var result = mock(ListOffsetsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(infos));
        when(adminClient.listOffsets(anyMap(), any())).thenReturn(result);
    }

    private static ConsumerGroupDescription description(String id, GroupState state, MemberDescription... members) {
        return new ConsumerGroupDescription(id, false, List.of(members), "range", GroupType.CLASSIC, state,
                COORDINATOR, Set.of(), Optional.empty(), Optional.empty());
    }

    private static MemberDescription member(String id, String clientId, Set<TopicPartition> assigned) {
        return new MemberDescription(id, Optional.empty(), clientId, "/10.0.0.1", new MemberAssignment(assigned));
    }
}
