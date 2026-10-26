# Lesson 07: Transactions & Exactly-Once (consume → process → produce)

## The idea (read this first)

A very common Kafka pattern is a **processor** that reads one topic and writes others:

```
orders-placed ──► processor ──► invoices
                      │
                      └───────► inventory-reservations
```

For **each** input message it does **three writes**: invoice, reservation, and the **offset commit**
("I'm done with this input"). Without transactions those are three separate steps, and a crash between them leaves
a mess:

| Crash after… | Result after restart |
|---|---|
| invoice, before reservation | invoice exists without a reservation, then the order is reprocessed: **duplicate invoice** |
| both writes, before offset commit | order reprocessed: **duplicate invoice AND duplicate reservation** |

Idempotence (Lesson 05) doesn't help here: it de-duplicates *retries of the same send*, not
"my whole program ran this order twice".

**Kafka transactions** make the three writes **atomic**: all of them become visible together, or none of them do.

```python
producer.init_transactions()                 # once, at startup
producer.begin_transaction()
producer.produce("invoices", ...)
producer.produce("inventory-reservations", ...)
producer.send_offsets_to_transaction(...)    # the input offset joins the SAME transaction
producer.commit_transaction()                # all 3 become visible together
# on error: producer.abort_transaction()     # none of them become visible
```

How "invisible" works: aborted messages **are physically written** to the log. The broker also writes a
**commit or abort marker**. Consumers with **`isolation.level=read_committed`** skip aborted messages, and they wait
at any transaction that is still open. (`read_committed` is the default in confluent-kafka; Java defaults to
`read_uncommitted`.)

**`transactional.id`**: a stable name for "this processor instance". When a new instance calls `init_transactions()`
with the same id, the broker **aborts** whatever the old instance left open, and **fences** the old instance: it can
never write again. That protects you from **zombies** (an old instance that was frozen, not dead, and wakes up later).

### ⚠️ The limit: exactly-once only *inside Kafka*
Transactions cover **Kafka → Kafka**. If your processor also charges a card, sends an email or writes to Postgres,
those side effects are **not** rolled back by `abort_transaction()`. For those you still need Lessons 03 and 06:
**idempotency** (keys, unique constraints) and the **Outbox pattern**.

## Files

- `create_topics.py`: `orders-placed` (input), `naive-*` and `tx-*` outputs (invoices + reservations)
- `producer.py [start]`: sends 10 orders (`order-<start>` … `order-<start+9>`)
- `processor_naive.py [crash]`: no transactions: invoice, reservation, commit as 3 separate steps
- `processor_tx.py [--crash] [--tx-id ID]`: the same, as **one transaction** per order
- `reader.py naive|tx [uncommitted]`: counts what a consumer sees: totals, duplicates, half-done orders
- `stuck_transaction.py`: opens a transaction and never commits it (the broker aborts it after 20 s)
- `processor_tx_batch.py [--batch-size N]`: reference solution for Experiment 5 (one transaction per batch)

## Experiments

Run from `kafka_learning/lesson07_transactions`. First:
```bash
uv run python create_topics.py
uv run python producer.py
```

### Experiment 1: the naive processor crashes
```bash
uv run python processor_naive.py crash   # crashes on order-5
uv run python reader.py naive            # state right after the crash
uv run python processor_naive.py         # restart, Ctrl+C when idle
uv run python reader.py naive
```
After the crash: which order is "half done"? After the restart: what got duplicated?

### Experiment 2: the transactional processor crashes
```bash
uv run python processor_tx.py --crash
uv run python reader.py tx                 # read_committed
uv run python reader.py tx uncommitted     # read_uncommitted
uv run python processor_tx.py              # restart, Ctrl+C when idle
uv run python reader.py tx
uv run python reader.py tx uncommitted
```
Compare the two readers, both after the crash and after the restart. Where is order-5's first invoice?
Did it disappear from the log, or is it just hidden? Look at the warning line when the processor restarts.

### Experiment 3: a stuck transaction blocks everyone
Terminal 1: `uv run python stuck_transaction.py`
Terminal 2, **immediately**, then again after ~25 s:
```bash
uv run python reader.py tx
uv run python reader.py tx uncommitted
```
`order-901` was written **without** a transaction, after the open one. Can a `read_committed` reader see it
while the transaction is open? And after the broker aborts it at 20 s? What about `order-900`?

### Experiment 4: zombie fencing
Terminal 1: `uv run python processor_tx.py --tx-id shared-id`
Terminal 2: `uv run python processor_tx.py --tx-id shared-id` (the same id!)
Terminal 3: `uv run python producer.py 11` (new orders 11–20)

What happens to the instance in terminal 1? Then check `uv run python reader.py tx`: any duplicates for orders 11–20?

### Experiment 5 (coding exercise): batch transactions
One transaction per message is **slow**: each `commit_transaction()` is several round trips to the broker.
Write `processor_tx_batch.py`: same logic, but with **one transaction per batch**:
- use `consumer.consume(num_messages=100, timeout=1.0)` (returns a list) instead of `poll()`
- begin → produce invoice + reservation for **every** message in the batch → send offsets → commit
- on an abortable error: abort, and rewind **every partition** in the batch to its first offset in that batch
  (`consumer.seek(...)`), so the whole batch is retried

Then measure: send 1,000 orders (`for s in $(seq 1 10 991); do uv run python producer.py $s; done`, or change
`producer.py`) and time the per-message processor against your batch processor (remove the `time.sleep(0.3)` in both).

## Experiment 5: reference solution

See **`processor_tx_batch.py`** in this folder. The key parts:

```python
batch = [m for m in consumer.consume(num_messages=args.batch_size, timeout=1.0) if not m.error()]

producer.begin_transaction()
try:
    for msg in batch:
        ...produce invoice + reservation...
    producer.send_offsets_to_transaction(consumer.position(consumer.assignment()),
                                         consumer.consumer_group_metadata())
    producer.commit_transaction()
except KafkaException as e:
    if e.args[0].txn_requires_abort():
        producer.abort_transaction()
        rewind(batch)   # seek each partition back to its FIRST offset in the batch
        continue
    raise
```

`rewind()` matters: after an abort, the consumer's in-memory position is already *past* the batch. Without seeking
back, the next `consume()` would continue after it and the aborted orders would be **skipped**, because the offset
commit was aborted too, but the consumer doesn't know that.

**Measured** (1,000 orders, no `sleep`, same machine):

| batch size | throughput |
|---|---|
| 1 (one transaction per order) | **~12 orders/s** |
| 100 | **~9,000 orders/s** (about 750× faster) |

Each `commit_transaction()` costs several round trips to the broker (add partitions, send offsets, commit,
write markers to every partition involved). With batches you pay that cost once per 100 orders instead of once per order.
The trade-off: a bigger batch means more latency per order and more work redone after an abort.

## Questions & Answers (for revision)

**1. Without transactions: the three writes, and what a crash between them leaves behind.**

1. produce the **invoice**
2. produce the **reservation**
3. **commit** the input offset

- crash between 1 and 2: an invoice with **no reservation** (half-done, visible to everyone); after restart the
  order is reprocessed, so a **duplicate invoice**.
- crash between 2 and 3: both outputs exist, but the offset wasn't committed; after restart, **duplicate invoice AND
  duplicate reservation**.
- crash after 3: fine.

(Committing *before* writing would turn duplicates into **losses**: at-most-once, Lesson 03.)

**2. The aborted invoice is still in the log. How does a `read_committed` consumer know to skip it?**

When a transaction ends, the broker writes a **control record (marker)**, COMMIT or ABORT, into every partition the
transaction touched. The broker also keeps an index of aborted transactions per partition. A `read_committed` consumer
gets that list with its fetch and **filters out** messages from aborted transactions. It also never reads past the
**Last Stable Offset (LSO)**: the first offset that belongs to a still-open transaction.
`read_uncommitted` ignores all of this and returns everything, which is why it showed order-5's invoice twice.
(The markers also take up offsets, which is why offsets in transactional topics have small "gaps".)

**3. Why commit the offset *inside* the transaction instead of `consumer.commit()` afterwards?**

Because then "outputs written" and "input marked done" would be **two separate steps again**:
- crash after `commit_transaction()` but before `consumer.commit()`: the outputs are committed, the offset isn't, so
  the order is reprocessed and the outputs are **duplicated**. That's exactly the problem transactions were supposed to solve.
- commit the offset first, then crash before `commit_transaction()`: the outputs are aborted but the input is marked done, so it's **lost**.

`send_offsets_to_transaction()` writes the offset to `__consumer_offsets` **as part of the same transaction**, so it
commits or aborts together with the outputs. That's what makes it exactly-once.

**4. What is `transactional.id` for? What happened in Experiment 4, and why is it good?**

It's a **stable identity** for one processor instance across restarts. The broker uses it to:
- **finish the past:** on `init_transactions()`, abort any transaction a previous instance with this id left open
  (that's how the crashed order-5 transaction got cleaned up in Experiment 2).
- **fence zombies:** each `init_transactions()` bumps an **epoch**. Any write from an older epoch is rejected.

In Experiment 4, the second instance bumped the epoch, and the first one got `_FENCED` on its next write and
died. That's good: imagine instance 1 wasn't dead, just **frozen** (a long GC pause or network partition). The group has
already given its partitions to someone else. If it woke up and committed its old transaction, you'd get duplicates.
Fencing guarantees **at most one live writer per transactional.id**.
⚠️ So the id must be **unique per running instance** (e.g. include the pod or host name) but **stable across restarts**.

**5. Why does one stuck transaction hide even non-transactional messages? Which setting limits it, and the trade-off?**

A partition is **one ordered log**, and `read_committed` consumers must return messages **in offset order**. The
stuck transaction's messages come *before* order-901. The consumer can't skip ahead, because it doesn't know yet whether
those earlier messages will be committed (and must be delivered first) or aborted. So it stops at the
**Last Stable Offset**, and everything after it waits, transactional or not.

The limit is **`transaction.timeout.ms`** (producer side, default 60 s; the broker caps it at
`transaction.max.timeout.ms`, default 15 min). After it, the broker aborts the transaction.
- **too short:** slow but healthy transactions (big batches, slow processing) get aborted and redone
- **too long:** a crashed or stuck producer can block every `read_committed` consumer of those partitions for that long

So: set it a bit above your worst-case transaction duration, and keep transactions short.

**6. Processor also sends an email; crash after the email, before `commit_transaction()`.**

After the restart, the transaction is aborted, the order is reprocessed, and **the customer gets the email twice**.
`abort_transaction()` can only undo Kafka writes. An email is a side effect **outside Kafka** and can't be taken back.
Options:
- **Move the side effect out of the transaction:** the processor only produces an `email_requested` event (inside the
  transaction, so exactly-once). A separate **email service** consumes it and sends.
- Make that email service **idempotent**: remember `event_id`s already sent (a unique constraint, Lesson 06) or use the
  email provider's idempotency key. There's still a tiny window (sent, then crashed before recording it), but that's
  the best you can do with an external system. Real exactly-once for emails doesn't exist.

**7. (Bonus) Idempotent producer vs transactions? Does a produce-only FastAPI app need transactions?**

| | Protects against |
|---|---|
| **Idempotent producer** | duplicates and reordering caused by the **producer's own retries** of one send, per partition, within one producer session |
| **Transactions** | **partial** results across *several* writes (multiple topics/partitions + the consumer offset): all-or-nothing, plus zombie fencing |

A produce-only API that sends **one event per request** usually doesn't need transactions: idempotence covers retries.
You'd want them only if one request must write **several** events atomically (e.g. `order_placed` + `stock_reserved` to two topics,
both or neither). And remember the real problem for a produce-only service: the **DB write + Kafka write** in one request
can't be atomic with Kafka transactions. That's the **Outbox pattern** (Lesson 05, Q6).
