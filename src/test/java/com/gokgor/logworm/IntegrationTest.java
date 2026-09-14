package com.gokgor.logworm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

/**
 * Full Spring context wired to an in-process (KRaft) Kafka broker.
 * Sharing one annotation keeps the context identical across test classes so it is cached.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest(properties = "logworm.demo-producer.enabled=false")
@AutoConfigureMockMvc
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 3,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = "auto.create.topics.enable=false")
public @interface IntegrationTest {
}
