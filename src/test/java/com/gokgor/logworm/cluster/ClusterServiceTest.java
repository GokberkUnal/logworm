package com.gokgor.logworm.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.gokgor.logworm.kafka.LogwormKafkaProperties;

class ClusterServiceTest {

    private final AdminClient adminClient = mock(AdminClient.class);
    private ClusterService service;

    @BeforeEach
    void setUp() {
        service = new ClusterService(adminClient, new LogwormKafkaProperties(Duration.ofSeconds(1)));
    }

    @Test
    void describesClusterWithBrokersSortedById() {
        var controller = new Node(2, "b2", 9092);
        stubCluster("cid", controller, List.of(new Node(3, "b3", 9092, "r1"), new Node(1, "b1", 9092), controller));
        stubMetadataVersion((short) 21);

        var info = service.describeCluster();

        assertThat(info.clusterId()).isEqualTo("cid");
        assertThat(info.controllerId()).isEqualTo(2);
        assertThat(info.metadataVersion()).isEqualTo("21");
        assertThat(info.brokers()).extracting(BrokerInfo::id).containsExactly(1, 2, 3);
        assertThat(info.brokers().get(2).rack()).isEqualTo("r1");
    }

    @Test
    void controllerIsNullWhenUnknown() {
        stubCluster("cid", null, List.of(new Node(1, "b1", 9092)));
        stubMetadataVersion((short) 1);

        assertThat(service.describeCluster().controllerId()).isNull();
    }

    @Test
    void metadataVersionIsNullWhenFeaturesApiFails() {
        stubCluster("cid", null, List.of());
        var failed = new KafkaFutureImpl<FeatureMetadata>();
        failed.completeExceptionally(new UnsupportedOperationException("old broker"));
        var features = mock(DescribeFeaturesResult.class);
        when(features.featureMetadata()).thenReturn(failed);
        when(adminClient.describeFeatures(any())).thenReturn(features);

        assertThat(service.describeCluster().metadataVersion()).isNull();
    }

    private void stubCluster(String clusterId, Node controller, List<Node> nodes) {
        var result = mock(DescribeClusterResult.class);
        when(result.clusterId()).thenReturn(KafkaFuture.completedFuture(clusterId));
        when(result.controller()).thenReturn(KafkaFuture.completedFuture(controller));
        when(result.nodes()).thenReturn(KafkaFuture.completedFuture(nodes));
        when(adminClient.describeCluster(any())).thenReturn(result);
    }

    private void stubMetadataVersion(short level) {
        var range = mock(FinalizedVersionRange.class);
        when(range.maxVersionLevel()).thenReturn(level);
        var metadata = mock(FeatureMetadata.class);
        when(metadata.finalizedFeatures()).thenReturn(Map.of("metadata.version", range));
        var features = mock(DescribeFeaturesResult.class);
        when(features.featureMetadata()).thenReturn(KafkaFuture.completedFuture(metadata));
        when(adminClient.describeFeatures(any())).thenReturn(features);
    }
}
