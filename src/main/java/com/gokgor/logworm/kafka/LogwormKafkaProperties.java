package com.gokgor.logworm.kafka;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Logworm-specific Kafka settings (prefix "logworm.kafka").
 *
 * @param requestTimeout how long to wait for a single AdminClient request
 */
@ConfigurationProperties(prefix = "logworm.kafka")
public record LogwormKafkaProperties(Duration requestTimeout) {

    public LogwormKafkaProperties {
        if (requestTimeout == null) {
            requestTimeout = Duration.ofSeconds(5);
        }
    }
}
