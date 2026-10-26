# Kafka Learning Roadmap

Do one lesson at a time: read the LESSON.md, run the experiments, answer the questions.

| # | Lesson | Status |
|---|--------|--------|
| 01 | Basic producer + consumer, consumer groups, `auto.offset.reset` (your `producer.py`, `consumer1.py`, `consumer2.py`) | Done |
| 02 | Partitions, keys, and how a consumer group splits work | Done |
| 03 | Offsets and commits: auto vs manual, at-least-once, replaying messages | Done |
| 04 | Serialization: JSON + pydantic, headers, schema evolution, poison pills | Done |
| 05 | Producer reliability: `acks`, retries, idempotence, batching, compression | Done |
| 06 | Error handling: transient vs permanent errors, backoff, dead-letter topic, replay | **Current** |
| 07 | Exactly-once with transactions (consume → process → produce) | |
| 08 | Mini project: order pipeline with FastAPI + Kafka | |

## Recap of Lesson 01 (what you already learned)

- A **producer** sends messages to a **topic**. `produce()` is async: it only queues the message.
  `flush()` waits until everything is actually sent, and the delivery callback tells you the result.
- A **consumer** calls `poll()` in a loop. `None` means "nothing right now", not an error.
- `group.id`: every **consumer group** gets its *own* copy of every message.
  That's why `consumer1.py` and `consumer2.py` (different groups) both saw all 10 messages.
- `auto.offset.reset: earliest` only applies when a group has **no committed offset yet**.
  Run `consumer1.py` again and it will NOT re-read old messages, because the group remembers where it stopped.

## Handy commands

```bash
# list topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# describe a topic (partitions, leaders)
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders

# see a group's members, partitions, offsets and LAG
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-group
```

Kafka UI is also running at http://localhost:8080.
