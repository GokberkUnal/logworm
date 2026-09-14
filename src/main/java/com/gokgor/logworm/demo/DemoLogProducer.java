package com.gokgor.logworm.demo;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Logworm needs test data to be developed against; this component produces
 * a fake log message to the demo-logs topic every second.
 * Can be disabled with logworm.demo-producer.enabled=false.
 */
@Component
@ConditionalOnProperty(name = "logworm.demo-producer.enabled", havingValue = "true")
@RequiredArgsConstructor
@Slf4j
public class DemoLogProducer {

    private static final List<String> SERVICES = List.of("auth-service", "order-service", "payment-service", "inventory-service");
    private static final List<String> LEVELS = List.of("INFO", "INFO", "INFO", "WARN", "ERROR");
    private static final List<String> MESSAGES = List.of(
            "Request processed successfully",
            "User login attempt",
            "Order created",
            "Payment gateway timeout",
            "Cache miss, falling back to database",
            "Connection pool exhausted",
            "Retrying failed request");

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${logworm.demo-producer.topic}")
    private String topic;

    @Scheduled(fixedRate = 1000)
    public void produceLog() {
        var random = ThreadLocalRandom.current();
        String service = SERVICES.get(random.nextInt(SERVICES.size()));
        String level = LEVELS.get(random.nextInt(LEVELS.size()));
        String message = MESSAGES.get(random.nextInt(MESSAGES.size()));

        String value = """
                {"timestamp":"%s","level":"%s","service":"%s","message":"%s"}"""
                .formatted(java.time.Instant.now(), level, service, message);

        // key = service name: logs from the same service land on the same partition
        kafkaTemplate.send(topic, service, value)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.warn("Failed to send demo log: {}", ex.getMessage());
                    }
                });
    }
}
