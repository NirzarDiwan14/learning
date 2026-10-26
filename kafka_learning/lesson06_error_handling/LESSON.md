# Lesson 06: Error Handling, Retries & Dead Letter Topics

## The idea (read this first)

In Lesson 04 you learned that a consumer that crashes on a message gets stuck forever (poison pill),
and that silently skipping it loses data. Production consumers sort every failure into one of two buckets:

| | **Transient error** | **Permanent error** |
|---|---|---|
| Examples | DB down, API timeout, HTTP 503, rate limit | invalid JSON, missing field, negative amount, unknown currency |
| Will retrying help? | **Yes**, probably in a few seconds | **No, never.** The bytes won't change |
| What to do | **retry with backoff** (0.5 s, 1 s, 2 s …), then give up | **don't retry**: send it to the DLQ immediately |

**Dead Letter Topic (DLQ)**: a separate topic (here `payment-requests.dlq`) where unprocessable messages go:
- the **original key, value and headers, byte for byte**, plus
- extra headers explaining **why** (`dlq.error.type`, `dlq.error.message`) and **where from**
  (`dlq.original.topic/partition/offset`, `dlq.consumer.group`, `dlq.failed.at`).

The main consumer **keeps going**, and nothing is lost. Someone gets alerted, looks at the DLQ, fixes the cause
(the bug, the data, or waits for the bank), and **replays** the messages back to the original topic.

```
payment-requests ──► consumer ──► ✅ charged
                        │
                        ├── permanent error ───────────────► payment-requests.dlq
                        └── transient error ─► retry, retry, retry ─► still failing ─┘
                                                                        │
                        ◄──────────── dlq_replay.py (after the fix) ◄───┘
```

**The golden rule:** you may only move past a message (store its offset) once it's **handled**:
either processed successfully **or safely written to the DLQ**. If the DLQ write fails, crash instead.
A crash is recoverable; a lost message is not.

### The hidden danger: `max.poll.interval.ms`
Retrying *in place* (sleeping inside the loop) means you don't call `poll()` for a while. If the gap between two
`poll()` calls exceeds `max.poll.interval.ms` (default 5 min), Kafka decides your consumer is **stuck**, kicks it out
of the group and rebalances. Its uncommitted work gets **processed again** by whoever gets the partition.

## Files

- `models.py`: `PaymentRequest` (with business rules: `amount_cents > 0`, known currency)
- `fake_bank.py`: a pretend bank. `pay-3`/`pay-6` fail twice then work; `pay-7` always fails while `BANK_OUTAGE=1`
- `create_topics.py`: `payment-requests` (3 partitions) + `payment-requests.dlq`
- `producer.py`: 10 payment requests: good ones, flaky ones, broken ones (see comments)
- `consumer.py`: validate, then retry with exponential backoff, then DLQ
- `dlq_inspect.py`: prints everything in the DLQ with its headers, then exits
- `dlq_replay.py <error_type>`: sends DLQ messages of one error type back to the original topic
- `slow_consumer.py`: processing takes 12 s, but `max.poll.interval.ms` is 10 s

## Experiments

Run from `kafka_learning/lesson06_error_handling`. First: `uv run python create_topics.py`

### Experiment 1: predict, then run
Read `producer.py` and `fake_bank.py`. For each of the 10 payments, **write down** what you expect:
charged on attempt 1 / charged after retries / DLQ (and which error type). Then:
```bash
uv run python producer.py
uv run python consumer.py      # Ctrl+C when idle
```

### Experiment 2: look inside the DLQ
```bash
uv run python dlq_inspect.py
```
Find `pay-5`, `pay-7`, `pay-8`, `pay-9`. For each one: could an engineer tell what went wrong and where to find the
original, **without** reading any logs? Also look at the DLQ in Kafka UI (http://localhost:8080).

### Experiment 3: blocking retries
Look at the order of the output in Experiment 1. While `pay-7` was retrying (3.5 s), what happened to the
other payments? Now set `BASE_BACKOFF_S = 5` in `consumer.py`, reset, and run it again:
```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group payment-processor --all-topics --reset-offsets --to-earliest --execute
uv run python consumer.py
```
How long is the whole consumer frozen now? Put `BASE_BACKOFF_S` back to `0.5` afterwards.

### Experiment 4: the bank is back, replay
```bash
uv run python dlq_replay.py retries_exhausted
BANK_OUTAGE=0 uv run python consumer.py      # the outage is over
uv run python dlq_replay.py retries_exhausted   # run it again: what happens?
```
Did `pay-7` get charged? Why doesn't the second replay send it again?

### Experiment 5: replay WITHOUT a fix
```bash
uv run python dlq_replay.py validation
uv run python consumer.py
uv run python dlq_inspect.py
```
What happened to `pay-5`, `pay-8`, `pay-9`? How many messages are in the DLQ now? Imagine a cron job that
replays the DLQ every minute: what would happen?

### Experiment 6: the consumer that got kicked out
```bash
uv run python slow_consumer.py      # let it run ~45 s, then Ctrl+C
```
Follow the log line by line: `processing` → `MAXPOLL ... leaving group` → `offset stored locally` →
`_MAX_POLL_EXCEEDED` → `REVOKED` → `ASSIGNED`. Which message gets processed **twice**? Does the consumer ever
make progress?

### Experiment 7 (coding exercise): make the consumer idempotent
Replays and rebalances mean the same payment **can** reach the consumer twice. Charging twice is not acceptable.
Make `consumer.py` idempotent using **SQLite** (built into Python: `import sqlite3`):
1. A table `charges (payment_id TEXT PRIMARY KEY, charged_at TEXT)` in a file `payments.db`.
2. Before charging: if `payment_id` is already in the table, print `⏭️ already charged, skipping` and don't
   call the bank.
3. After a successful charge: insert the row.

Test it:
```bash
uv run python producer.py          # same 10 payment ids again
uv run python consumer.py          # good payments should all say "already charged"
```

## Questions (answer these and send them to me, along with your Experiment 7 code)

1. Which of the 10 payments were **transient** failures and which were **permanent**? Why must they be handled
   differently? What would happen if you retried `pay-8` 4 times with backoff?
2. `send_to_dlq()` calls `flush()` and **crashes** if the DLQ write fails. Why not just log the error and move on?
3. Why does the DLQ store the **original bytes** with the error info in **headers**, instead of, say, a new JSON
   like `{"error": "...", "payment": {...}}`? (Think about `pay-5`.)
4. In Experiment 3 the whole consumer froze while one payment was retrying. A common alternative is a separate
   **retry topic**: put the failing message on `payment-requests.retry` and continue at once. What do you gain? What
   do you **lose**? (Hint: Lesson 02, ordering per key. Think "order paid" retrying while "order shipped" arrives.)
5. Explain exactly why a message was processed twice in Experiment 6. Name two ways to fix it.
6. When is it safe to replay the DLQ? What did Experiment 5 teach you about replaying without a fix?
7. (Bonus) In your Experiment 7 code, "charge at the bank" and "insert into SQLite" are two separate steps.
   Describe the exact moment where a crash still causes a double charge. How does sending the bank an
   **idempotency key** (`payment_id`) close that gap? (Lesson 03, Q5.)
