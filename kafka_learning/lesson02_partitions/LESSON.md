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

## Questions (answer these and send them to me)

1. In Experiment 2, one partition probably got **zero** messages. Why? What problem does this cause in a
   real system, and what could you do about it? 
   ans = yes alice goes to the same partition again, it makes same goes to the same but one consumer is sitting there ideal not doing anything, right now idk what to do about this
2. Why can't 4 consumers in one group read from a 3-partition topic faster than 3 consumers?
   ans = because one goes unassigned and it only 3 are effectively working at the same time 
3. You run `consumer.py A` with group `orders-group`, and `consumer1.py` from Lesson 01 is also running.
   If you changed `consumer1.py` to subscribe to `orders` with `group.id = "some-other-group"`, how many
   times would each order be processed in total?
   ans = i dont have done that, but i think it will get duplicated and processed twice if i do that 
4. An e-commerce app must process "order created", "order paid" and "order shipped" in order for each order.
   What would you use as the message key?
   ans = i think user_id will be a good choice, like a primary key from db for users,because it will fall in the same order and under same partition so tracking will be easy
5. What does `LAG` mean, and why would you put an alert on it in production?
   ans = i think lag is situation where one consumer is slower then others and if it happens in prod it can slow down the whole app, becuase there are the chances of apps being slow because of interdependent services 
6. (Bonus) Why did Ctrl+C rebalance quickly while `kill -9` took about 45 seconds?
   ans = i dont have done that. but i assumme because in code we have used finally and in ctrl c is does not execute it automatically and slowly will die so...
