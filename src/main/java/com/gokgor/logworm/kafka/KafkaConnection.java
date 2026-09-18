package com.gokgor.logworm.kafka;

import static com.gokgor.logworm.kafka.KafkaFutures.await;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * The one place that knows which Kafka cluster we are talking to.
 *
 * <p>Holds the live {@link AdminClient} and the base client properties. {@link #connect(String)}
 * swaps the cluster at runtime: it verifies the new address first and keeps the old connection
 * if the new one fails. Services never hold an AdminClient directly; they use the
 * {@code Admin} bean from {@link KafkaAdminConfig}, which always delegates to the current one.
 */
@Component
@Slf4j
public class KafkaConnection {

    /**
     * @param bootstrapServers address the client was created with
     * @param clusterId        id reported by the broker
     * @param brokerCount      live brokers at connect time
     */
    public record Info(String bootstrapServers, String clusterId, int brokerCount) {
    }

    private final Map<String, Object> baseProperties;
    private final Duration timeout;
    private volatile AdminClient admin;
    private volatile Info info;

    public KafkaConnection(KafkaAdmin kafkaAdmin, LogwormKafkaProperties properties) {
        this.baseProperties = Map.copyOf(kafkaAdmin.getConfigurationProperties());
        this.timeout = properties.requestTimeout();
        String servers = configuredBootstrapServers();
        this.admin = AdminClient.create(baseProperties);
        this.info = new Info(servers, null, 0);
    }

    /** Address from configuration (application.yml), regardless of later reconnects. */
    public String configuredBootstrapServers() {
        return bootstrapServersAsString(baseProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }

    /** Spring Boot hands the setting over as a List; Kafka accepts either, humans want "a:1,b:2". */
    static String bootstrapServersAsString(Object value) {
        if (value instanceof java.util.Collection<?> c) {
            return c.stream().map(String::valueOf).collect(java.util.stream.Collectors.joining(","));
        }
        return String.valueOf(value);
    }

    public AdminClient admin() {
        return admin;
    }

    public Info info() {
        return info;
    }

    /** Client properties for the current cluster; consumers/producers created by Logworm start from these. */
    public Map<String, Object> clientProperties() {
        Map<String, Object> props = new HashMap<>(baseProperties);
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, info.bootstrapServers());
        return props;
    }

    /**
     * Verifies the current connection by describing the cluster.
     *
     * @throws KafkaRequestException when the broker does not answer within the request timeout
     */
    public Info verify() {
        Info verified = describe(admin, info.bootstrapServers());
        this.info = verified;
        return verified;
    }

    /**
     * Connects to another cluster. On failure the old connection stays in place and the
     * exception propagates.
     */
    public synchronized Info connect(String bootstrapServers) {
        Map<String, Object> props = new HashMap<>(baseProperties);
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AdminClient candidate = AdminClient.create(props);
        Info verified;
        try {
            verified = describe(candidate, bootstrapServers);
        } catch (RuntimeException e) {
            candidate.close(Duration.ZERO);
            throw e;
        }
        AdminClient old = this.admin;
        this.admin = candidate;
        this.info = verified;
        old.close(timeout);
        log.info("Connected to Kafka cluster {} via {}", verified.clusterId(), bootstrapServers);
        return verified;
    }

    private Info describe(AdminClient client, String servers) {
        try {
            var result = client.describeCluster(new DescribeClusterOptions().timeoutMs((int) timeout.toMillis()));
            String clusterId = await(result.clusterId(), timeout);
            int brokers = await(result.nodes(), timeout).size();
            return new Info(servers, clusterId, brokers);
        } catch (KafkaRequestException e) {
            throw e;
        } catch (org.apache.kafka.common.KafkaException e) {
            // e.g. TimeoutException("Timed out waiting for a node assignment") when nothing listens there
            throw new KafkaRequestException("Cannot reach Kafka at " + servers + ": " + e.getMessage(), e);
        }
    }

    public void close() {
        admin.close(timeout);
    }
}
