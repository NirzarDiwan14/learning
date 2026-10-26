# Kafka Learning Roadmap

A hands-on Kafka course in Python (`confluent-kafka`). **Start with Lesson 01**: it covers the setup.
Do one lesson at a time: read its `LESSON.md`, run the experiments, answer the questions yourself, and only then
read the "Questions & Answers" section at the end of the lesson.

Requirements: Docker, [uv](https://docs.astral.sh/uv/), Python 3.14 (see `pyproject.toml`). Run each lesson from its own folder.

| # | Lesson | Status |
|---|--------|--------|
| [01](lesson01_basics/LESSON.md) | Basic producer + consumer, consumer groups, `auto.offset.reset` + setup | Done |
| [02](lesson02_partitions/LESSON.md) | Partitions, keys, and how a consumer group splits work | Done |
| [03](lesson03_offsets/LESSON.md) | Offsets and commits: auto vs manual, at-least-once, replaying messages | Done |
| [04](lesson04_serialization/LESSON.md) | Serialization: JSON + pydantic, headers, schema evolution, poison pills | Done |
| [05](lesson05_producer_reliability/LESSON.md) | Producer reliability: `acks`, retries, idempotence, batching, compression | Done |
| [06](lesson06_error_handling/LESSON.md) | Error handling: transient vs permanent errors, backoff, dead-letter topic, replay | Done |
| [07](lesson07_transactions/LESSON.md) | Exactly-once with transactions (consume → process → produce) | Done |
| [08](lesson08_project/LESSON.md) | Final project: event-driven order system (FastAPI + payment service + projector) | Done |

## Quick recap of Lesson 01 (details in lesson01_basics/LESSON.md)

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

## What next?
All 8 lessons are done. See [NEXT_STEPS.md](NEXT_STEPS.md) for what to learn after this and how.
