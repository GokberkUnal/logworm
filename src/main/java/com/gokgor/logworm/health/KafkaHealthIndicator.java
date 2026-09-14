package com.gokgor.logworm.health;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/**
 * Shows up as the "kafka" component under /actuator/health.
 * Verifies the broker is actually reachable via describeCluster.
 */
@Component("kafka")
public class KafkaHealthIndicator implements HealthIndicator, AutoCloseable {

    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    private final AdminClient adminClient;

    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    @Override
    public Health health() {
        try {
            var cluster = adminClient.describeCluster(
                    new DescribeClusterOptions().timeoutMs((int) TIMEOUT.toMillis()));
            var clusterId = cluster.clusterId().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            var nodeCount = cluster.nodes().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS).size();
            return Health.up()
                    .withDetail("clusterId", clusterId)
                    .withDetail("brokerCount", nodeCount)
                    .build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Health.down(e).build();
        } catch (Exception e) {
            return Health.down(e).build();
        }
    }

    @Override
    public void close() {
        adminClient.close(TIMEOUT);
    }
}
