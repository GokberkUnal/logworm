package com.gokgor.logworm.demo;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

/**
 * Fake Debezium change events for a {@code shop.customers} table on the demo-cdc topic: inserts,
 * updates of one or two columns, and deletes followed by a tombstone, every two seconds.
 * Runs together with {@link DemoLogProducer} (logworm.demo-producer.enabled).
 */
@Component
@ConditionalOnProperty(name = "logworm.demo-producer.enabled", havingValue = "true")
@RequiredArgsConstructor
@Slf4j
public class DemoCdcProducer {

    private static final List<String> NAMES = List.of("Ada", "Linus", "Grace", "Alan", "Barbara", "Ken");
    private static final List<String> STATUSES = List.of("active", "suspended", "vip");
    private static final int MAX_ROWS = 8;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper json;
    private final Map<Integer, Map<String, Object>> rows = new LinkedHashMap<>();
    private int nextId = 1001;

    @Value("${logworm.demo-producer.cdc-topic:demo-cdc}")
    private String topic;

    @Scheduled(fixedRate = 2000)
    public synchronized void produceChange() {
        var random = ThreadLocalRandom.current();
        int roll = random.nextInt(10);
        if (rows.size() < 3 || (roll < 3 && rows.size() < MAX_ROWS)) {
            insert();
        } else if (roll < 9) {
            update(randomId());
        } else {
            delete(randomId());
        }
    }

    private void insert() {
        int id = nextId++;
        String name = NAMES.get(ThreadLocalRandom.current().nextInt(NAMES.size()));
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("name", name);
        row.put("email", name.toLowerCase() + id + "@example.com");
        row.put("status", "active");
        row.put("balance", 0);
        rows.put(id, row);
        send(id, "c", null, row);
    }

    private void update(int id) {
        var random = ThreadLocalRandom.current();
        Map<String, Object> before = rows.get(id);
        Map<String, Object> after = new LinkedHashMap<>(before);
        after.put("balance", (Integer) before.get("balance") + random.nextInt(-50, 200));
        if (random.nextBoolean()) {
            after.put("status", STATUSES.get(random.nextInt(STATUSES.size())));
        }
        rows.put(id, after);
        send(id, "u", before, after);
    }

    private void delete(int id) {
        Map<String, Object> before = rows.remove(id);
        send(id, "d", before, null);
        kafkaTemplate.send(topic, key(id), null); // Debezium follows a delete with a tombstone for compaction
    }

    private int randomId() {
        List<Integer> ids = new ArrayList<>(rows.keySet());
        return ids.get(ThreadLocalRandom.current().nextInt(ids.size()));
    }

    private void send(int id, String op, Map<String, Object> before, Map<String, Object> after) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("before", before);
        envelope.put("after", after);
        envelope.put("source", Map.of("connector", "postgresql", "db", "demo", "schema", "shop", "table", "customers"));
        envelope.put("op", op);
        envelope.put("ts_ms", Instant.now().toEpochMilli());
        kafkaTemplate.send(topic, key(id), json.writeValueAsString(envelope))
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.warn("Failed to send demo change event: {}", ex.getMessage());
                    }
                });
    }

    private static String key(int id) {
        return "{\"id\":" + id + "}";
    }
}
