# Lesson 01: Setup, Your First Producer & Consumer, Consumer Groups

## The idea (read this first)

Kafka is a **distributed, append-only log**. Programs write messages into it and other programs read them,
without knowing about each other.

```
producer.py ──► [ topic "my_topic" ]  ──► consumer1.py   (group my_consumer_group)
                  0  1  2  3  4 ...   ──► consumer2.py   (group my_consumer_group_2)
                  ▲ offsets
```

| Word | Meaning |
|---|---|
| **broker** | a Kafka server. We run **one**, in Docker |
| **topic** | a named stream of messages, like `my_topic` |
| **offset** | a message's position in the log: 0, 1, 2 … Messages are **not deleted when read** |
| **producer** | writes messages to a topic |
| **consumer** | reads messages from a topic, in a loop (`poll()`) |
| **consumer group** (`group.id`) | a named team of consumers. Kafka remembers **per group** how far it has read |

Two things that surprise everyone at first:
1. **Reading doesn't remove a message.** Each group has its own position in the log, so two groups both get *all* messages.
2. **`produce()` doesn't send immediately.** It puts the message in a local queue, and `flush()` waits until it's really
   sent. The **delivery callback** tells you whether it arrived (more in Lesson 05).

## Setup (once)

From the project root (`learning/`):
```bash
uv sync                          # installs confluent-kafka, fastapi, pydantic …
docker compose up -d             # starts Kafka (localhost:9092) + Kafka UI (http://localhost:8080)
docker ps                        # both containers should be "Up"
```

### ⚠️ The single-broker gotcha (already fixed in `docker-compose.yml`)
Kafka stores consumer-group positions in an internal topic, `__consumer_offsets`, which by default wants
**3 replicas**, meaning 3 brokers. With only 1 broker that topic can't be created, so **consumers silently receive
nothing**: `poll()` just keeps returning `None`. The broker log shows it:
```bash
docker logs kafka 2>&1 | grep -i "replication factor"
```
That's why `docker-compose.yml` sets:
```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```
**Debugging tip for the whole course:** when something hangs silently, look at `docker logs kafka` first.

## Files

- `producer.py`: sends `Message: 0` … `Message: 9` to `my_topic`, prints a delivery report for each
- `consumer1.py`: reads `my_topic` in group `my_consumer_group`
- `consumer2.py`: the same, but in group `my_consumer_group_2`

## Experiments

Run from `kafka_learning/lesson01_basics`.

### Experiment 1: produce
```bash
uv run python producer.py
```
Look at the delivery reports: which partition and offset did each message get? Run it a second time:
do the offsets start again at 0?

### Experiment 2: consume
```bash
uv run python consumer1.py      # Ctrl+C to stop
```
Did it get the messages you produced *before* it started? Why? (Look at `auto.offset.reset`.)

### Experiment 3: run it again
Stop `consumer1.py` and start it again. Does it print the old messages again? Now run `producer.py` once more while it
runs. What does it print now?

### Experiment 4: two groups
Start `consumer2.py` (a **different** group). Which messages does it print: only new ones, or everything?
Now keep **both** consumers running and run `producer.py`. Does each message go to one of them, or to both?

### Experiment 5: look inside
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic my_topic
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my_consumer_group
```
Also open Kafka UI at http://localhost:8080: Topics → `my_topic` → Messages, and Consumers.

### Experiment 6: forget `flush()`
Comment out `producer.flush()` in `producer.py` and run it. How many delivery reports do you see? Run `consumer1.py`:
did the messages arrive? Put `flush()` back. (Lesson 05 explains exactly what happened.)

## Questions & Answers (for revision)

Try to answer each question yourself before reading the answer.

**1. `consumer1.py` started *after* the producer. Why did it still get the old messages?**

Messages stay in the log after they're written (until retention deletes them, 7 days by default). A **new** group has
no saved position yet, so `auto.offset.reset` decides where to start: `earliest` means from the beginning (the default is
`latest`: only new messages).

**2. Why doesn't `consumer1.py` print the old messages again after a restart?**

The group **committed** its position (the offset of the *next* message to read) in `__consumer_offsets`. On restart
it continues from there. `auto.offset.reset` is used **only when a group has no committed position**.
To re-read everything you change the `group.id`, or reset the group's offsets (Lesson 03).

**3. `consumer1.py` and `consumer2.py` are both running. Who gets each message?**

**Both.** They're in **different groups**, and every group gets its own full copy of the topic (**fan-out**).
That's how, e.g., an email service and an analytics service can both receive every order.
If both consumers were in the **same** group, they would **split** the work instead. That's Lesson 02.

**4. What does `producer.flush()` do, and what happens without it?**

`produce()` only puts the message in an in-memory queue; a background thread sends it a few ms later. `flush()` blocks
until everything queued is sent and every delivery callback has run. Without it the script can exit **before** sending,
and the messages are silently lost, even though `produce()` raised no error.

**5. What is the delivery callback (`delivery_report`) for?**

It's the only way to learn the result of a send: `err` is set on failure, otherwise `msg` tells you the topic, partition
and offset where it was stored. It runs inside `poll()` / `flush()`, not at `produce()` time.

**6. Why does the consumer loop treat `msg is None` as normal?**

`poll(timeout=1.0)` returns `None` when no message arrived within 1 second. That just means "nothing new yet", not
an error, so the loop continues. Real errors come back as a message with `msg.error()` set.

**7. Why is there a `finally: consumer.close()`?**

`close()` commits the final position and tells the broker "I'm leaving the group", so the group can react immediately.
Without it (e.g. a crash, `kill -9`) the broker waits for a timeout (~45 s) before giving that consumer's work to
others (Lesson 02, Q6).

**8. (Bonus) Why did the consumers hang silently before `docker-compose.yml` was fixed?**

The group coordinator needs the `__consumer_offsets` topic. It wanted 3 replicas on a 1-broker cluster, so it was never
created. The consumer could never join its group, never got partitions assigned, and `poll()` returned `None`
forever. The producer worked fine because producing doesn't need consumer groups.

---
**Next:** [Lesson 02: Partitions, Keys & Consumer Groups](../lesson02_partitions/LESSON.md). Full list: [ROADMAP.md](../ROADMAP.md)
