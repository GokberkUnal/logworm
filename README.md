# 🪱 Logworm

A [Kadeck](https://www.xeotek.com/kadeck/)-like Kafka monitoring and inspection tool for browsing messages/logs flowing through Kafka.

**Stack:** Java 25 · Spring Boot 4.1.1 · Spring Kafka · Maven

## What will it do?

- Connect to a Kafka cluster and discover topics, partitions, and consumer groups
- View, filter, and search messages within topics
- Watch messages live (tail mode)
- Track consumer lag and cluster health

---

## Roadmap

### Phase 0 — Foundation ✅
- [x] Spring Boot project skeleton (Kafka, WebMVC, Actuator)
- [x] Local Kafka environment via `docker-compose.yml` (KRaft, single broker)
- [x] Kafka connection configuration (`application.yml`, bootstrap servers)
- [x] Simple test log producer (`DemoLogProducer`, `demo-logs` topic)
- [x] Health check: Kafka connectivity via Actuator (`KafkaHealthIndicator`)

### Phase 1 — Cluster & Topic Discovery
- [ ] Cluster info via `AdminClient` (brokers, controller, version)
- [ ] Topic listing: name, partition count, replication factor
- [ ] Topic detail: partitions, earliest/latest offsets, approximate message count
- [ ] REST API: `GET /api/cluster`, `GET /api/topics`, `GET /api/topics/{name}`

### Phase 2 — Message Browsing
- [ ] Read messages from a topic/partition by offset range
- [ ] Fetch the latest N messages (Kadeck's core feature)
- [ ] Key/value deserialization: String and JSON support
- [ ] Display headers, timestamp, offset, and partition metadata
- [ ] Basic filtering: by key, header, or content
- [ ] REST API: `GET /api/topics/{name}/messages`

### Phase 3 — Live Tail
- [ ] Live message stream from a topic via SSE or WebSocket
- [ ] Filtering on the stream
- [ ] Backpressure / rate limiting (avoid overwhelming the browser on busy topics)

### Phase 4 — Consumer Group Monitoring
- [ ] List consumer groups and their states
- [ ] Partition assignments and committed offsets per group
- [ ] Lag calculation (end offset − committed offset)
- [ ] REST API: `GET /api/consumer-groups`, `GET /api/consumer-groups/{id}`

### Phase 5 — Web UI
- [ ] Technology choice (React SPA vs. Thymeleaf + HTMX)
- [ ] Topic list and detail screens
- [ ] Message browser (table + JSON viewer)
- [ ] Live tail screen
- [ ] Consumer group / lag dashboard

### Phase 6 — Advanced Features
- [ ] Produce messages to a topic
- [ ] Avro / Schema Registry support
- [ ] Multi-cluster support
- [ ] Alert rules (e.g. notify when lag exceeds a threshold)
- [ ] Topic management: create, delete, edit configs

---

## Development

```bash
# Start local Kafka
docker compose up -d

# Run the application
./mvnw spring-boot:run

# Tests
./mvnw test
```
