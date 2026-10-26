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

## Questions & Answers (for revision)

**1. Experiment 1 (auto-commit, 10 s interval): which payments were duplicated, and why?**

**`payment-0`, `payment-1`, `payment-2`.** Timeline:
1. Processed 0, 1, 2 (about 3 s), started 3, crashed at about 4 s.
2. The first auto-commit was due at 10 s, so **nothing was ever committed**.
3. On restart the group had no committed offset, so `auto.offset.reset=earliest` applied: start from 0.

0–2 were finished twice (duplicates). 3 was started twice but finished once. 4–9 were processed once.
⚠️ Not "all were duplicated": only the ones *finished before the crash* ran twice.

**2. Experiment 2 (auto-commit, 100 ms interval): which payment was lost? Order of events?**

**`payment-3`.**
1. `poll()` returns payment-3, and the client marks offset 4 as "next to commit".
2. Within 100 ms the background timer **commits 4**, while the work (1 s) is still running.
3. Crash before the work finishes.
4. Restart reads the committed offset 4, so payment-3 is never processed. **Lost.**

**3. Move `consumer.commit(...)` before `time.sleep(1)` (the work). Which guarantee?**

**At-most-once.** Commit first, then work. If it crashes during the work, the offset already says "done",
so the message is **lost**. You never get duplicates, but you can lose messages.
Fine for things like metrics or logs; never for payments.

**4. With manual commit, can a message still be processed twice? Exactly when?**

Yes. **The work finished (card charged), but the crash happened before the commit reached the broker.**
On restart the committed offset still points at that message, so it's processed again. Other ways it happens:
the commit call fails (network error), or a **rebalance** moves the partition to another consumer before you committed.
This is **at-least-once**: never lost, sometimes repeated.

**5. Charging cards with at-least-once: how to make it idempotent?**

1. The message must carry a **unique id** created by the producer (`payment_id` / idempotency key),
   not something generated by the consumer.
2. Make "already done?" **atomic**: a `UNIQUE` constraint on `payment_id`, and
   `INSERT ... ON CONFLICT DO NOTHING`. If the insert affected 0 rows, it's a duplicate, so skip it.
   ⚠️ "SELECT, then INSERT" is not safe: two consumers (or a retry) can both pass the SELECT.
3. For the external call, pass the same key to the payment provider (e.g. Stripe's `Idempotency-Key` header),
   so even a repeated API call charges only once.

**6. Why does `replay.py` NOT commit? What would go wrong if it did?**

`replay.py` uses the **same group** as the real consumer (`payments-manual`). Committing would **move the real
consumer's position**:
- Group is at 15, you replay from 6 and commit, so the group jumps **back** to 7–10 and the real consumer
  re-processes payments 10–14 (duplicate charges!).
- Replay from 0, Ctrl+C at 3, commit 4: same problem.

Replaying is for debugging and recovery, a read-only look. Even better: replay with a **separate group id**
(e.g. `payments-replay`). With the same group id, starting `replay.py` while the real consumer runs also triggers a
rebalance and **steals partitions** from it.

**7. (Bonus) Sync commit per message is slow at 10k msg/s. What instead?**

Each sync commit is a network round trip (around 1–5 ms), so per-message sync commits cap you at a few hundred msg/s.
Options:
- **Commit less often.** Committing offset N covers *everything before it*, so commit every 500 messages or every
  1 s instead of every message.
- **Async commit** (`asynchronous=True`) doesn't block, but failures aren't retried and can arrive out of order.
  Common pattern: async commits while running, plus **one sync commit on shutdown / on partition revoke**.
- **Best in confluent-kafka:** `enable.auto.commit=True` + `enable.auto.offset.store=False`, then call
  `consumer.store_offsets(msg)` **after** processing. The background timer then only commits what you
  finished. You get at-least-once with no blocking.

⚠️ Async alone isn't the full answer: the big win is *batching* commits, and you still need a final sync commit.
