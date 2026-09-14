package com.gokgor.logworm.health;

import static com.gokgor.logworm.kafka.KafkaFutures.await;

import java.time.Duration;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import com.gokgor.logworm.kafka.KafkaProperties;

/**
 * Shows up as the "kafka" component under /actuator/health.
 * Verifies the broker is actually reachable via describeCluster.
 */
@Component("kafka")
public class KafkaHealthIndicator implements HealthIndicator {

    private final AdminClient adminClient;
    private final Duration timeout;

    public KafkaHealthIndicator(AdminClient adminClient, KafkaProperties properties) {
        this.adminClient = adminClient;
        this.timeout = properties.requestTimeout();
    }

    @Override
    public Health health() {
        try {
            var cluster = adminClient.describeCluster(
                    new DescribeClusterOptions().timeoutMs((int) timeout.toMillis()));
            var clusterId = await(cluster.clusterId(), timeout);
            var nodeCount = await(cluster.nodes(), timeout).size();
            return Health.up()
                    .withDetail("clusterId", clusterId)
                    .withDetail("brokerCount", nodeCount)
                    .build();
        } catch (RuntimeException e) {
            return Health.down(e).build();
        }
    }
}
