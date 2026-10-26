# Lesson 02: Partitions, Keys & Consumer Groups

## The idea (read this first)

So far `my_topic` has **1 partition**. Real topics are split into several **partitions**:

```
topic "orders"
  partition 0:  [m0][m1][m2][m3] ...   <- each partition is an append-only log
  partition 1:  [m0][m1] ...              with its OWN offsets (0, 1, 2 ...)
  partition 2:  [m0][m1][m2] ...
```

Three rules to remember:

1. **Ordering is guaranteed only within one partition**, not across the whole topic.
2. **The key picks the partition**: `partition = hash(key) % num_partitions`.
   Same key means same partition, so all of `alice`'s orders stay in order.
   No key means messages get spread across partitions.
3. **In one consumer group, each partition is read by exactly ONE consumer.**
   Partitions are the unit of parallelism. 3 partitions means at most 3 consumers in a group doing work.

```
group "orders-group" with 2 consumers:
  partition 0 ──► consumer A
  partition 1 ──► consumer A
  partition 2 ──► consumer B
```

When a consumer joins or leaves, Kafka **rebalances**: it takes partitions away (revoke)
and hands them out again (assign).

## Files

- `create_topic.py`: creates topic `orders` with 3 partitions, using the AdminClient
- `producer.py`: sends 15 orders, keyed by user name, slowly (0.5 s apart)
- `consumer.py`: consumer in group `orders-group`; takes a name argument; prints assign/revoke events

Run everything from this folder (`kafka_learning/lesson02_partitions`) with `uv run python <file>`.

## Experiments (do them in order, keep notes on what you see)

### Experiment 1: create the topic
```bash
uv run python create_topic.py
uv run python create_topic.py      # run it twice. What happens?
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
```

### Experiment 2: keys and partitions
```bash
uv run python producer.py
```
Look at the output. Which partition does `alice` go to? Run the producer again. Does she go to the same one?

### Experiment 3: one consumer gets everything
Terminal 1: `uv run python consumer.py A`
Watch the `ASSIGNED` line. Which partitions did A get?

### Experiment 4: add a second consumer (rebalance!)
Keep A running. Terminal 2: `uv run python consumer.py B`
- Watch A's terminal: you'll see `REVOKED` and then a new `ASSIGNED`.
- Now in terminal 3 run `uv run python producer.py`. Who prints which users?

### Experiment 5: more consumers than partitions
Start C and D as well (4 consumers, 3 partitions). Look at the `ASSIGNED` lines.
Then run:
```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-group
```

### Experiment 6: a consumer dies
- Stop B with **Ctrl+C**. How quickly do the others get B's partitions?
- Start B again, then **close its terminal window** instead (or `kill -9` it). This time `consumer.close()`
  never runs. How long until its partitions move? (Hint: look up `session.timeout.ms`.)

### Experiment 7: LAG
Stop all consumers. Run the producer once. Then run the `--describe --group` command above.
Look at the `LAG` column. Now start one consumer and run `--describe` again.

## Questions & Answers (for revision)

**1. In Experiment 2, one partition got zero messages. Why? What problem does it cause, and what can you do?**

`partition = hash(key) % 3`. With only 5 keys, it was just bad luck that none of the 5 hashes landed on
partition 1. Nothing is wrong with Kafka; there were too few distinct keys.
- **Problem:** *partition skew* (a "hot partition"). One consumer sits idle while another does all the work,
  so adding consumers doesn't help.
- **Fixes:** use a key with **many distinct values** (high cardinality: order_id, device_id …).
  If you don't need ordering, send **no key** and Kafka spreads messages evenly.
  Watch out for one "giant" key (a huge customer): it always lands on a single partition.

**2. Why can't 4 consumers read a 3-partition topic faster than 3?**

In a group, one partition is read by exactly ONE consumer. With 3 partitions, at most 3 consumers get work.
The 4th is assigned nothing and sits idle. It's still useful as a **hot standby**: if a working consumer dies,
the standby takes over its partition right away. **Partitions = the maximum parallelism of a group.**

**3. `consumer.py A` (group `orders-group`) + `consumer1.py` (group `some-other-group`), both on `orders`. How many times is each order processed?**

**Twice**, once per group. Every group gets its own full copy of the topic and its own offsets.
This is **fan-out**, and it's a feature, not duplication: e.g. an email service and an analytics service each
see every order. Duplication *within one group* is the real problem (Lesson 03).

**4. "order created / paid / shipped" must be processed in order for each order. What key?**

**`order_id`.** All events of one order land on the same partition, so they stay in order.
`user_id` also works (it orders *all* of a user's events), but it's coarser than needed, and one big customer
makes one partition hot. **Rule: pick the smallest key that still gives you the ordering you need.**

**5. What is LAG, and why alert on it?**

Per partition: `LAG = latest offset in the partition − offset the group has committed`
= **how many messages are waiting, not yet processed**.
Lag that keeps growing means the consumers can't keep up, are stuck, or have crashed. Orders pile up unprocessed
while the service still looks "up", so lag is often the only signal something is wrong.
⚠️ Not "one consumer is slower than the others". Lag is about the group vs. the end of the log.

**6. Why did Ctrl+C rebalance quickly while `kill -9` took ~45 s?**

- **Ctrl+C** raises `KeyboardInterrupt`, so `finally` runs `consumer.close()`. That sends **LeaveGroup** to the broker,
  which rebalances immediately.
- **`kill -9`** ends the process instantly: no `finally`, no `close()`. The broker only notices when
  heartbeats stop for **`session.timeout.ms`** (default 45 s). Until then the dead consumer's partitions are stuck.

Lesson: **always close consumers**, and handle SIGTERM in real services (Docker/Kubernetes stop containers with SIGTERM).
