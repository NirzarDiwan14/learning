# Kafka: What to Learn Next

You've finished Lessons 01–08 (producers, consumers, partitions, offsets, serialization, reliability,
error handling, transactions, and an event-driven project). This is a map for going further.
Take the topics roughly **in order**: each builds on the previous ones.

## How to learn each topic
The same method as the lessons:
1. **Read the concept** (official docs or one good article). Don't start with videos that only show slides.
2. **Run it locally** with Docker Compose and break it on purpose (kill brokers, send bad data, stop consumers).
3. **Write down** what you expected vs. what happened. Surprises are the lessons.
4. **Add it to the Lesson 08 project**, so every new topic lands in a system you already understand.

---

## 1. Running a real cluster: replication and failover
**Why:** so far you've had 1 broker. Production has 3+, and most reliability settings only matter there.
- **Learn:** replication factor, leader/follower, ISR, `min.insync.replicas`, leader election, KRaft controllers, rack awareness
- **Do:** a 3-broker `docker-compose.yml`; topic with `replication.factor=3`, `min.insync.replicas=2`;
  produce with `acks=all` while you `docker stop` brokers one by one. When do writes fail? Do you lose anything?
- **Revisit:** Lesson 05, Q2 (`acks=1` data loss): now you can actually reproduce it

## 2. Schemas done properly: Schema Registry
**Why:** in Lesson 04 and Exercise B the "contract" was a Python file. Across teams and languages you need a central registry.
- **Learn:** Avro / Protobuf / JSON Schema, Confluent Schema Registry, subjects, compatibility modes (BACKWARD, FORWARD, FULL)
- **Do:** add Schema Registry to Docker Compose; switch `OrderPlaced` to Avro with `confluent_kafka.schema_registry`;
  try registering an incompatible change and watch it get **rejected before** it can break a consumer
- **Read:** Confluent docs, "Schema Evolution and Compatibility"

## 3. Kafka Connect and Change Data Capture (Debezium)
**Why:** most data enters and leaves Kafka through databases and other systems, not hand-written producers.
Debezium is also the standard **outbox relay** (your Exercise C, done for real).
- **Learn:** source vs sink connectors, Kafka Connect workers, Debezium for Postgres/MySQL, the outbox event router
- **Do:** Postgres + Debezium in Docker Compose; write orders + outbox rows in one DB transaction; let Debezium publish them;
  add a sink connector (e.g. to Elasticsearch or another Postgres)
- **Read:** debezium.io → tutorial + "Outbox Event Router"

## 4. Stream processing
**Why:** your projector and shipping service are hand-written stream processors. Frameworks handle state, joins, windows and
exactly-once for you.
- **Learn:** stateless vs stateful processing, KTable vs KStream, joins, windowing (tumbling/hopping/session), state stores,
  changelog topics, compacted topics
- **Options:** **Kafka Streams** (Java, the reference), **ksqlDB** (SQL), **Faust / Bytewax / Quix Streams** (Python),
  **Apache Flink** (most powerful, used at large scale)
- **Do:** "revenue per minute per currency" from `shop-payments`; a join of orders + payments with no SQLite

## 5. Operating Kafka in production
**Why:** Lesson 08 Q8 was a taste. Running it is a skill of its own.
- **Learn:** monitoring (JMX metrics, consumer lag exporters, Prometheus + Grafana), retention and compaction, partition
  count planning, rebalancing protocols (cooperative-sticky, static membership, KIP-848), quotas, upgrades
- **Do:** Prometheus + Grafana + a lag exporter for the Lesson 08 project; alert on lag and DLQ messages
- **Tools:** Kafka UI (you already have it), Burrow / kafka-lag-exporter, Cruise Control

## 6. Security
**Why:** your local broker accepts anyone. Real clusters don't.
- **Learn:** TLS encryption, SASL authentication (SCRAM, OAuth), ACLs (who may read or write which topic)
- **Do:** enable SASL_SSL + ACLs locally; give `payment-service` read access to `shop-orders` only

## 7. Architecture patterns (theory + design)
**Why:** Kafka is a tool. These are the designs it enables.
- **Learn:** event sourcing, CQRS (your projector is one), sagas / choreography vs orchestration (compensating actions,
  e.g. "refund if shipping fails"), event-carried state transfer, data mesh basics
- **Do:** add a "cancel order" flow to Lesson 08 that refunds and un-reserves stock as a saga

---

## Recommended resources
| Resource | What for |
|---|---|
| **kafka.apache.org/documentation** | the source of truth: configs, design, semantics |
| **developer.confluent.io** (free courses) | Kafka 101, Schema Registry, Kafka Streams, Connect: hands-on |
| **"Kafka: The Definitive Guide", 2nd ed.** (O'Reilly) | the best book: internals, reliability, operations |
| **"Designing Data-Intensive Applications"** (Kleppmann) | *why* logs, streams and exactly-once work the way they do |
| **"Designing Event-Driven Systems"** (Ben Stopford, free e-book) | event-driven architecture with Kafka |
| **confluent-kafka-python docs + examples on GitHub** | the client you've been using |
| **librdkafka `CONFIGURATION.md`** | every client setting and its default (the defaults differ from Java's!) |

## Suggested order and time
| Step | Topic | Rough time |
|---|---|---|
| 1 | 3-broker cluster + replication | 2–3 days |
| 2 | Schema Registry + Avro | 2–3 days |
| 3 | Kafka Connect + Debezium outbox | 3–4 days |
| 4 | Stream processing (pick one framework) | 1–2 weeks |
| 5 | Monitoring with Prometheus/Grafana | 2–3 days |
| 6 | Security (SASL/TLS/ACLs) | 1–2 days |
| 7 | Patterns: event sourcing, CQRS, sagas | ongoing, while building |

**Milestone idea:** rebuild the Lesson 08 shop on a 3-broker cluster, with Avro schemas, a Debezium outbox, a stream
processor for live revenue, and a Grafana dashboard. If you can build and explain that, you know Kafka well.
