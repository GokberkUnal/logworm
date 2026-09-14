package com.gokgor.logworm.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.stereotype.Component;

/**
 * Creates short-lived, group-less raw-byte consumers for browsing and tailing.
 * Partitions are assigned manually, nothing is ever committed, topics are never auto-created.
 */
@Component
public class BrowserConsumerFactory {

    private final Map<String, Object> config;

    public BrowserConsumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> c = new HashMap<>(kafkaProperties.buildConsumerProperties());
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        c.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        c.remove(ConsumerConfig.GROUP_ID_CONFIG);
        this.config = Map.copyOf(c);
    }

    public Consumer<byte[], byte[]> create(String clientId) {
        Map<String, Object> c = new HashMap<>(config);
        c.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        return new KafkaConsumer<>(c);
    }
}
