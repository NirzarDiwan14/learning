# Lesson 03: Offsets & Commits

## The idea (read this first)

Each message in a partition has an **offset** (0, 1, 2 ...). For every group and every partition,
Kafka stores one number, the **committed offset**: "the NEXT message this group should read".
It lives in the `__consumer_offsets` topic, the one that was broken in Lesson 01.

```
partition 0:  [0][1][2][3][4][5][6][7][8][9]
                          ▲              ▲
              committed = 3              latest = 10      LAG = 10 - 3 = 7
```

- A consumer restarts **from the committed offset**, not from where it "really" was.
- `auto.offset.reset` is only used when there is **no** committed offset (new group). That's why
  `consumer1.py` never re-reads old messages.

**The big question: when do you commit?** Your code does two things per message: *process* it and *commit* it.
A crash can happen between them:

| Order | If it crashes in between | Name |
|-------|--------------------------|------|
| commit, then process | message is skipped and **lost** | at-most-once |
| process, then commit | message is processed **again** | at-least-once |

By default (`enable.auto.commit=True`), a **background timer** commits "whatever `poll()` returned"
every `auto.commit.interval.ms` (5 s). It doesn't know if you finished. So you can get **both** problems.

Most real systems choose **at-least-once**: commit manually after processing, and make processing
safe to repeat (**idempotent**). For example: "insert payment if `payment_id` not already in DB".

## Files

- `create_topic.py`: creates topic `payments` (1 partition)
- `producer.py`: sends `payment-0` … `payment-9`
- `consumer_auto_commit.py`: auto-commit consumer that can crash on `payment-3`, the way `kill -9` would
- `consumer_manual_commit.py`: commits after each message is processed
- `inspect_offsets.py <group>`: prints earliest / latest / committed / LAG for a group
- `replay.py [offset]`: re-reads messages from the beginning or from an offset

Every consumer takes 1 s per message ("slow work") and uses `session.timeout.ms=6000`,
so a crashed consumer is noticed quickly (remember Q6 from Lesson 02).

## Experiments

Run from `kafka_learning/lesson03_offsets`. First:
```bash
uv run python create_topic.py
uv run python producer.py
```

Before every restart below, **write down which payments you think it will print**. Then run it and check.

### Experiment 1: auto-commit, rarely (DUPLICATES)
```bash
uv run python consumer_auto_commit.py dupes crash   # crashes on payment-3
uv run python consumer_auto_commit.py dupes         # restart, then Ctrl+C when it's idle
```
Which payments were processed twice? Why? (Hint: `auto.commit.interval.ms` is 10 s here.)

### Experiment 2: auto-commit, often (LOSS)
```bash
uv run python consumer_auto_commit.py loss crash
uv run python consumer_auto_commit.py loss
```
Look closely for a payment that was **started but never finished**, and **never came back**. Which one, and why?
(Hint: the interval is 100 ms, and the work takes 1 s.)

### Experiment 3: manual commit (AT-LEAST-ONCE)
```bash
uv run python consumer_manual_commit.py crash
uv run python consumer_manual_commit.py
```
Where does it restart? Compare with Experiments 1 and 2.

### Experiment 4: LAG, by hand
```bash
uv run python inspect_offsets.py payments-manual
uv run python producer.py                         # 10 more payments
uv run python inspect_offsets.py payments-manual  # what's the LAG now?
```
Now run `uv run python consumer_manual_commit.py` and check `inspect_offsets.py` again.
Compare with `kafka-consumer-groups.sh --describe --group payments-manual`. Same numbers?

### Experiment 5: replay
```bash
uv run python replay.py       # everything again
uv run python replay.py 6     # from offset 6
uv run python inspect_offsets.py payments-manual   # did replaying change the committed offset?
```

### Experiment 6: reset a group's offset from the CLI
Stop all consumers in the group first (the reset is refused while they are running):
```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group payments-manual --topic payments --reset-offsets --to-earliest --dry-run
```
Then change `--dry-run` to `--execute` and run `consumer_manual_commit.py`. Other options to try:
`--to-offset 5`, `--shift-by -3`, `--to-latest`.

## Questions (answer these and send them to me)

1. In Experiment 1, which payments were duplicated, and exactly why?
2. In Experiment 2, which payment was lost? Explain the order of events (poll → commit → crash).
3. In `consumer_manual_commit.py`, what would happen if you moved `consumer.commit(...)` to the line
   **before** `time.sleep(1)`? Which guarantee would you get?
4. With manual commit, can a message still be processed twice? Describe exactly when.
   (Think: the crash happens *after* the work, but *before* the commit.)
5. You process payments and charge a credit card for each one. At-least-once means a customer might get
   charged twice. How would you make the processing **idempotent**?
6. Why does `replay.py` NOT commit? What would go wrong if it did?
7. (Bonus) `consumer.commit(message=msg, asynchronous=False)` waits for the broker on every message.
   Why might that be slow at 10,000 messages/second, and what could you do instead?
