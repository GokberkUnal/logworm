package com.gokgor.logworm.cluster;

import static com.gokgor.logworm.kafka.KafkaFutures.await;

import java.time.Duration;
import java.util.Comparator;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.springframework.stereotype.Service;

import com.gokgor.logworm.kafka.KafkaProperties;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ClusterService {

    private static final String METADATA_VERSION_FEATURE = "metadata.version";

    private final AdminClient adminClient;
    private final Duration timeout;

    public ClusterService(AdminClient adminClient, KafkaProperties properties) {
        this.adminClient = adminClient;
        this.timeout = properties.requestTimeout();
    }

    public ClusterInfo describeCluster() {
        var result = adminClient.describeCluster(new DescribeClusterOptions().timeoutMs(timeoutMs()));

        String clusterId = await(result.clusterId(), timeout);
        var controller = await(result.controller(), timeout);
        var brokers = await(result.nodes(), timeout).stream()
                .sorted(Comparator.comparingInt(node -> node.id()))
                .map(BrokerInfo::from)
                .toList();

        return new ClusterInfo(
                clusterId,
                controller != null ? controller.id() : null,
                metadataVersion(),
                brokers);
    }

    /**
     * Kafka does not expose a plain "version" string; the closest thing is the
     * finalized metadata.version feature level on KRaft clusters. Older / ZooKeeper
     * clusters may not support the API, so this is best-effort.
     */
    private String metadataVersion() {
        try {
            var features = adminClient.describeFeatures(new DescribeFeaturesOptions().timeoutMs(timeoutMs()));
            FinalizedVersionRange range = await(features.featureMetadata(), timeout)
                    .finalizedFeatures()
                    .get(METADATA_VERSION_FEATURE);
            return range != null ? String.valueOf(range.maxVersionLevel()) : null;
        } catch (RuntimeException e) {
            log.debug("Could not read metadata.version feature: {}", e.getMessage());
            return null;
        }
    }

    private int timeoutMs() {
        return (int) timeout.toMillis();
    }
}
