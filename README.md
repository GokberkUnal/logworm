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

### Phase 1 — Cluster & Topic Discovery ✅
- [x] Cluster info via `AdminClient` (brokers, controller, metadata version)
- [x] Topic listing: name, partition count, replication factor
- [x] Topic detail: partitions, earliest/latest offsets, approximate message count
- [x] REST API: `GET /api/cluster`, `GET /api/topics`, `GET /api/topics/{name}`

### Phase 2 — Message Browsing ✅
- [x] Read messages from a topic/partition by offset range
- [x] Fetch the latest N messages (Kadeck's core feature)
- [x] Key/value deserialization: String and JSON support
- [x] Display headers, timestamp, offset, and partition metadata
- [x] Basic filtering: by key, header, or content
- [x] REST API: `GET /api/topics/{name}/messages`

### Phase 3 — Live Tail ✅
- [x] Live message stream from a topic via SSE (`GET /api/topics/{name}/stream`)
- [x] Filtering on the stream (same key/value/header filters as browsing)
- [x] Backpressure: per-stream rate limit with `stats` events, jump-to-end when the consumer falls too far behind, bounded number of concurrent streams

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

### Testing

- **Unit tests** (`*ServiceTest`, `KafkaFuturesTest`): `AdminClient` is mocked with Mockito; no broker needed.
- **Controller slice tests** (`*ControllerTest`): `@WebMvcTest` with a mocked service, cover JSON shape and error mapping.
- **Integration tests** (`*IntegrationTest`): full Spring context against an in-process KRaft broker via `@EmbeddedKafka` (see `IntegrationTest` meta-annotation). No Docker required.

## API

| Endpoint | Description |
|---|---|
| `GET /api/cluster` | Cluster id, controller, metadata version, brokers |
| `GET /api/topics` | All topics (internal included) with partition count and replication factor |
| `GET /api/topics/{name}` | Partition leaders/replicas/ISR, earliest & latest offsets, approximate message count |
| `GET /api/topics/{name}/messages` | Browse records (see below) |
| `GET /api/topics/{name}/stream` | Live tail as Server-Sent Events (see below) |
| `GET /actuator/health` | Liveness, readiness and Kafka connectivity |

### Browsing messages

`GET /api/topics/{name}/messages` uses a short-lived, group-less consumer per request (no offsets are committed).

| Parameter | Default | Meaning |
|---|---|---|
| `partition` | all | Restrict to one partition |
| `offset` | – | Read forward from this offset (**range mode**, requires `partition`). Without it the newest records are returned (**tail mode**, newest first) |
| `limit` | 100 | Max records to return (1–1000) |
| `key` | – | Key must contain this substring |
| `value` | – | Raw value must contain this substring |
| `header` | – | `name` (header present) or `name=value` (exact match) |
| `format` | `AUTO` | `AUTO` parses values that look like JSON, `STRING` returns raw text |

When a filter is set, up to `limit × 10` records (max 5000) are scanned to find matches; the response's `scanned` field tells how many were read. Each message carries `partition`, `offset`, `timestamp`, `timestampType`, `key`, `value`, `valueFormat` (`json`/`string`), `headers`, `keySize` and `valueSize`.

```bash
curl 'localhost:8080/api/topics/demo-logs/messages?limit=5'                       # newest 5
curl 'localhost:8080/api/topics/demo-logs/messages?partition=0&offset=500&limit=50'
curl 'localhost:8080/api/topics/demo-logs/messages?value=ERROR&header=trace-id'
```

### Live tail

`GET /api/topics/{name}/stream` returns `text/event-stream`. The tail starts at the log end, so only records produced after connecting are streamed. Accepts `partition`, `key`, `value`, `header` and `format` exactly like browsing, plus `rate` (max messages/second, default 100, capped at 1000).

| Event | Payload | When |
|---|---|---|
| `connected` | `{topic, startOffsets, rate}` | once, after positioning |
| `message` | same shape as a browsed message | each matching record |
| `stats` | `{emitted, dropped, skipped}` | at most once per second, only when records were dropped (rate limit) or skipped (consumer lag exceeded `rate × 10`, jumped to the end) |
| `:keep-alive` | comment | after 15 s without output |

At most 20 streams may be open at once (`logworm.stream.max-concurrent`); beyond that the request gets `503`. Each stream runs on its own virtual thread with its own group-less consumer.

```bash
curl -N 'localhost:8080/api/topics/demo-logs/stream?value=ERROR&rate=50'
```

Errors follow RFC 9457 (`application/problem+json`): unknown topic → `404`, invalid query (bad limit, offset without partition, unknown partition) → `400`, Kafka unreachable/timeout → `503`.
AdminClient request timeout is configurable via `logworm.kafka.request-timeout` (default `5s`).
