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

## Questions (answer these and send them to me, along with your Experiment 5 code)

1. Without transactions, list the three separate writes the processor makes per order, and what a crash between each
   pair leaves behind.
2. In Experiment 2, the aborted invoice for order-5 is still **in the log**. So how does a `read_committed` consumer
   know to skip it?
3. Why must the offset be committed **inside** the transaction (`send_offsets_to_transaction`) rather than with a
   normal `consumer.commit()` after `commit_transaction()`? What could go wrong with the second way?
4. What is `transactional.id` for? What happened in Experiment 4, and why is that a **good** thing?
5. In Experiment 3, a single stuck transaction hid even non-transactional messages from `read_committed` readers.
   Why does Kafka do that? What setting limits how long it can last, and what's the trade-off in choosing it?
6. Your processor reads `orders-placed`, writes `invoices`, **and sends an email** for every order. A crash
   happens after the email but before `commit_transaction()`. What does the customer experience after the restart?
   What would you do about it?
7. (Bonus) Lesson 05's idempotent producer vs Lesson 07's transactions: what does each one protect against?
   Would you ever need transactions for a simple service that only *produces* (like your FastAPI app)?
