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
- `consumer_idempotent.py`: reference solution for Experiment 7 (consumer.py + SQLite)

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

## Experiment 7: reference solution

See **`consumer_idempotent.py`** in this folder: `consumer.py` plus SQLite (tested: on a second run with the same
10 payment ids, every already-charged payment prints `⏭️ already charged, skipping`). The important parts:

```python
db = sqlite3.connect("payments.db")
db.execute("CREATE TABLE IF NOT EXISTS charges (payment_id TEXT PRIMARY KEY, charged_at TEXT)")

# before calling the bank
if db.execute("SELECT 1 FROM charges WHERE payment_id = ?", (payment.payment_id,)).fetchone():
    print(f"   ⏭️  {payment.payment_id} already charged, skipping")
    return

# right after a successful charge
fake_bank.charge(payment)
with db:   # commits the SQLite transaction
    db.execute("INSERT INTO charges VALUES (?, ?)", (payment.payment_id, datetime.now(UTC).isoformat()))
```

Run it with `uv run python consumer_idempotent.py` (same group as `consumer.py`, so it replaces it).
Note that the invalid payments still go to the DLQ again on the second run. The table only records *successful*
charges, so it only protects against double charging, which is the part that costs money.

## Questions & Answers (for revision)

**1. Transient vs permanent, and why handle them differently?**

- **Transient:** `pay-3`, `pay-6` (flaky bank, succeeded on attempt 3) and `pay-7` (bank outage: retries ran out,
  so `retries_exhausted` in the DLQ, replayable once the bank is back).
- **Permanent:** `pay-5` (not JSON), `pay-8` (negative amount), `pay-9` (unknown currency), all `validation`.

Retrying only makes sense if **something outside the message can change** (the bank recovers). A permanent error is
**in the bytes themselves**: retrying `pay-8` 4 times would fail 4 times identically, wasting 3.5 s while blocking
every other payment behind it, and then end up in the DLQ anyway. So permanent errors go to the DLQ immediately.

**2. Why does `send_to_dlq()` flush and crash if the DLQ write fails?**

Because right after it, the consumer **stores the offset** (moves past the message). If the DLQ write silently
failed, the message would be in neither place: **lost**. Crashing leaves the offset uncommitted, so after a restart
the message is read again and the DLQ write is retried. **A crash is recoverable; a lost message is not.**
`flush()` is what turns "queued in RAM" into "confirmed written" (Lesson 05, Q1).

**3. Why the original bytes + error info in headers, instead of a new JSON?**

- `pay-5` **isn't JSON**, so you can't put it inside a JSON field without escaping or re-encoding it, which changes it.
- **Replay needs the exact original.** `dlq_replay.py` can send the bytes back untouched, and the consumer sees
  precisely what it saw the first time.
- **Debugging needs the exact original.** The bug might be in an encoding detail that "cleaning up" would hide.
- **One DLQ format for every topic.** It works the same for JSON, Avro, Protobuf or images, because metadata lives in headers.

**4. In-place (blocking) retries vs a retry topic?**

| | In-place retry (our `consumer.py`) | Retry topic (`payment-requests.retry`) |
|---|---|---|
| Other messages | **blocked** while retrying (in Exp 3: the whole consumer frozen) | keep flowing |
| Ordering per key | **kept** | **broken**: a later message for the same key can overtake the one being retried |
| Risk | long backoff can exceed `max.poll.interval.ms` (Exp 6) | more topics and consumers to run |

Example of lost ordering: `order paid` for order-42 fails and goes to the retry topic. `order shipped` for order-42
arrives and is processed first, so you ship an unpaid order.
**Rule:** short in-place retries when ordering matters; a retry topic when throughput matters and events are independent
(or the consumer can handle out-of-order events).

**5. Experiment 6: why was a message processed twice? Two fixes.**

1. Processing took 12 s with no `poll()`, which is more than `max.poll.interval.ms` (10 s).
2. librdkafka decided the consumer was stuck and **left the group**. `store_offsets` still "worked", but only
   **locally**: the consumer no longer owned the partition, so the offset was **never committed**.
3. The next `poll()` reported `_MAX_POLL_EXCEEDED`, the partitions were revoked and reassigned, and the consumer
   restarted from the **last committed** offset, so the same message was processed again. It repeats forever and never progresses.

Fixes:
- **Raise `max.poll.interval.ms`** above the worst-case processing time (retries × backoff + slowest call).
- **Make processing shorter**: fewer or shorter in-place retries, hand long retries to a retry topic or the DLQ, put
  timeouts on external calls.
- (Advanced) `pause()` the partitions, do the slow work in another thread while the main loop keeps calling `poll()`,
  then `resume()`.

And since it *can* still happen: **idempotent processing** (Experiment 7) makes a reprocess harmless.

**6. When is it safe to replay the DLQ? What did Experiment 5 teach?**

Only **after the cause is fixed**: the bank is back (`retries_exhausted`), a consumer bug is deployed (the code was wrong),
or the data was corrected. And the consumer must be **idempotent**, because some replayed messages may already have been
partly processed.
Experiment 5: replaying `validation` errors without a fix sends them straight back to the DLQ. **Now they're in the
DLQ twice.** An automatic replay every minute would create an **infinite loop**, doubling DLQ entries and alerts while
nothing is fixed. Replays must be deliberate, filtered by error type, and ideally capped (a `replay-count` header,
then "park" the message for good).

**7. (Bonus) Where can a double charge still happen? How does an idempotency key help?**

```
fake_bank.charge(payment)   ✅ money taken
💥 crash here               (before the INSERT)
db.execute("INSERT ...")    never happens
```
On restart the table says "not charged", so it charges again. The charge (at the bank) and the record (in your DB)
are two different systems, so they can't be one atomic step.
**Idempotency key:** send `payment_id` with the charge request (e.g. Stripe's `Idempotency-Key` header). The **bank**
remembers it, and a second request with the same key returns the first result instead of charging again. Now
the bank is the source of truth for "already charged?". Your table becomes just an optimization (it skips the call).
**Real rule: idempotency must live where the side effect happens.**
