# Lesson 08 (Final Project): An Event-Driven Order System

Everything from Lessons 01–07 in one small but realistic system.

## The architecture

```
                   POST /orders                                    GET /orders/{id}
 client ──────────────────────────► api.py ◄──────────────────────────────── client
                                      │  ▲
               OrderPlaced            │  │ reads
               (waits for the ack)    ▼  │
                               ┌──────────────┐                     ┌──────────────┐
                               │ shop-orders  │                     │  orders.db   │ (read model)
                               │ 3 partitions │                     └──────────────┘
                               └──────────────┘                            ▲ upserts
                                │           │                              │
          group payment-service │           └────────────► projector.py ───┘
                                ▼                          (group order-status-projector)
                        payment_service.py ──► bank.py (external, flaky, idempotency key)
                          │          │                             ▲
             bad / bank down         │ PaymentResult               │
                          ▼          ▼                             │
                shop-orders-dlq   ┌───────────────┐                │
                  │               │ shop-payments │ ───────────────┘
      dlq_replay.py (after fix)   └───────────────┘
```

| Component | What it does | Lessons used |
|---|---|---|
| `api.py` | FastAPI. Validates the request, builds an `OrderPlaced` event, **waits for Kafka's ack**, answers `201` (or `503`). `GET` reads the read model. | 04 (schema), 05 (one producer, delivery reports, timeouts) |
| `payment_service.py` | Consumes orders, charges the bank with **retries + backoff**, publishes `payment_succeeded` / `payment_failed`, sends bad or unprocessable orders to the **DLQ** | 02 (scaling by partitions), 03 (store offsets after work), 06 (transient vs permanent, DLQ) |
| `projector.py` | Consumes **both** topics, routes by the `event_type` **header**, keeps an `orders` table up to date with **idempotent upserts** | 04 (headers), 06 (idempotency) |
| `bank.py` | A pretend external bank: slow, fails 20% of calls (half of those *after* charging!), supports an **idempotency key** | 03 Q5, 06 Q7 |
| `kafka_utils.py` | `EventProducer` (background poll, sync/async send), `make_consumer`, `send_to_dlq`, SIGTERM handling | 05 (Exp. 8 review), 02 Q6 |
| `lag.py`, `check.py`, `load_test.py`, `dlq_replay.py` | operations: lag monitoring, checking the system's promises, load, recovery | 03, 06 |
| `shipping_service.py` | **Exercise A solution**: payments → shipments in Kafka transactions | 07 |
| `api_outbox.py` | **Exercise C solution**: the API with the Outbox pattern | 05 Q6 |

### The promises this system makes
1. A `201` means the order **is durably in Kafka** (not just in RAM).
2. **No order is ever charged twice**: not on retries, replays, rebalances, crashes or duplicate HTTP requests.
3. Every order eventually ends up `paid` or `payment_failed` (or sits in the DLQ, visibly, until someone replays it).

`check.py` verifies 2 and 3 against the bank's records.

## How to run it

From `kafka_learning/lesson08_project`, in **4 terminals**:
```bash
uv run python create_topics.py          # once
uv run python payment_service.py        # terminal 1
uv run python projector.py              # terminal 2
uv run python api.py                    # terminal 3 → http://127.0.0.1:8000/docs
# terminal 4: experiments below
```
After the exercises (solutions below), also run `uv run python shipping_service.py` in another terminal, and
optionally `api_outbox.py` **instead of** `api.py`.
Data lives in `data/` (SQLite). To start completely fresh: stop everything, `rm -rf data`, and delete the topics
and groups (see "Reset" at the end).

## Experiments

### Experiment 1: one order, end to end
```bash
curl -s -X POST localhost:8000/orders -H 'content-type: application/json' \
  -d '{"customer_id":"alice","items":[{"sku":"book","qty":2,"unit_price_cents":1500}],"card_last4":"4242"}'
curl -s localhost:8000/orders/<order_id>
```
Follow it through the logs of all 3 services. The `POST` answer contains the partition and offset: why can the API
know those? Try the `GET` really fast after the `POST` (or with the projector stopped): what do you get, and why?

### Experiment 2: a declined card
Same, with `"card_last4":"0000"`. Where does it end up? Is it in the DLQ? Why not?

### Experiment 3: the client retries
Send the **same** request twice with the same header:
```bash
curl ... -H 'Idempotency-Key: abc-123' ...     # twice
uv run python check.py
```
Same `order_id` both times? How many charge *requests* vs *charges* did the bank see?

### Experiment 4: load and lag
Terminal 4: `uv run python lag.py --watch`. Terminal 5: `uv run python load_test.py -n 300`.
How fast does the API accept orders, and how fast does the payment service drain them? (The bank costs 10–50 ms per call,
plus retries.) When lag reaches 0, run `uv run python check.py`.

### Experiment 5: scale out
Start **2 more** `payment_service.py` (5 terminals now…). Watch the `ASSIGNED`/`REVOKED` lines. Run
`load_test.py -n 600` and compare the drain time with Experiment 4. Then start a **4th** instance: what does it get?

### Experiment 6: chaos
During `load_test.py -n 600`, kill one payment instance **hard**: `kill -9 <pid>` (the pid is in its log lines).
- How long until its partitions move to the others? (Hint: `session.timeout.ms` in `kafka_utils.py`.)
- When lag is 0: `uv run python check.py`. Any double charges? Any lost orders?
- Then stop one with **Ctrl+C** instead. How fast is the rebalance now?

### Experiment 7: bank outage
Stop all payment services. Start one with the bank completely down:
```bash
BANK_FAILURE_RATE=1 uv run python payment_service.py
uv run python load_test.py -n 5
uv run python check.py        # look at "pending", and at the bank's charges
```
Every order ends up in the DLQ, but **compare the bank's charge count before and after**. Some orders were charged
even though we gave up on them. Then stop it, start a normal one, and:
```bash
uv run python dlq_replay.py
uv run python check.py
```
Were those already-charged orders charged a second time?

### Experiment 8: Kafka down
`docker stop kafka`, then `POST` an order. How long until you get an answer, and which status code?
`docker start kafka`, then try again. Did the services recover on their own?

## Exercises (coding)

### Exercise A: a shipping service, with transactions (Lesson 07)
Write `shipping_service.py`:
- consumes `shop-payments`, and for every `payment_succeeded` produces a `ShipmentRequested` event to a new topic
  `shop-shipments` (key `order_id`)
- this is **Kafka → Kafka** only, so use a **transaction**: produce + `send_offsets_to_transaction` + commit,
  and `isolation.level=read_committed`
- `payment_failed` events: nothing to ship, but their offsets must still move forward (inside the transaction)
- extend `projector.py` to also consume `shop-shipments` and set `status = 'shipping'`
- careful: can `shipping` be overwritten by a late `payment_succeeded`? Fix it in the upsert.

### Exercise B: evolve the schema without breaking anyone (Lesson 04)
Add `currency: str = "INR"` to `OrderPlaced` and to the API request. Roll it out in the **safe order** (which service
first?) while everything is running, and check that `check.py` stays green.

### Exercise C (stretch): what's still missing?
Pick one and implement it:
- **Outbox pattern** in the API: store the order in a local DB table + a background relay to Kafka, so the API works
  even while Kafka is down (Experiment 8)
- a `GET /orders?customer_id=...` endpoint (the projector needs a new index or table)
- a `/metrics` endpoint on the API exposing orders accepted, `503`s and the producer queue length (`len(producer)`)

## Exercise solutions (all tested)

Run `uv run python create_topics.py` once more: it creates the new `shop-shipments` topic (the others just say
"already exists"). The projector adds the new `currency` column to your existing `orders.db` by itself.

### Exercise A: `shipping_service.py`

`shop-payments` → `shop-shipments`, **one Kafka transaction per batch** (Lesson 07's `processor_tx_batch.py` pattern):
- `isolation.level=read_committed`, `enable.auto.commit=False`; offsets go in via `send_offsets_to_transaction`
- `payment_failed` events produce nothing, but their offsets still move forward with the batch
- `transactional.id = shipping-service-<hostname>-<instance>`: unique per running instance, stable across restarts
- deterministic `event_id` (`uuid5("shipment:<order_id>")`)
- on shutdown, an **open transaction is aborted** instead of being left hanging (it would block every
  `read_committed` reader of `shop-shipments` for up to `transaction.timeout.ms`, Lesson 07 Exp. 3)

`projector.py` now also consumes `shop-shipments` and sets `status = 'shipping'`. **The trap:** a late or redelivered
`payment_succeeded` must not move `shipping` back to `paid`, so the payment upsert is guarded:
```sql
status = CASE WHEN orders.status = 'shipping' THEN orders.status ELSE excluded.status END
```

**Tested:** 393 orders, `kill -9` of the shipping service during the load, restart. Result: **exactly 393 shipment
events, at most 1 per order**, and `check.py` all green.
`lag.py` shows `LAG 1` per partition on `shop-shipments` even when fully caught up: that last offset is the
transaction's **commit marker**, which takes an offset but is never delivered (each high watermark was last offset + 2).

### Exercise B: `currency` (schema evolution)

- `events.py`: `currency: str = "INR"` on `OrderPlaced` (and `PaymentResult`). **A default** makes it compatible in
  both directions. It's a plain `str`, not a `Literal`: if a future producer sends `"GBP"`, old consumers must not crash.
- `api.py`: `currency: Literal["INR", "USD", "EUR"] = "INR"`, so it's validated strictly **at the edge**, where a bad value
  gets a `422` instead of becoming a poison pill. Header `schema_version: "2"`.
- `payment_service.py` copies it into `PaymentResult`.
- `projector.py` **migrates its own table** (`ALTER TABLE orders ADD COLUMN currency`) and stores it.

**Safe order: consumers first, then producers.** Tested both ways with a real rolling upgrade:

| Order | Result |
|---|---|
| ❌ API v2 first (consumers still v1) | **no errors**, but the v1 projector **dropped** the field. 3 USD orders ended up with `currency = NULL`, which displays as INR: wrong data, silently |
| ✅ consumers v2 first, then the API | USD stored correctly; old v1 events get `"INR"` from the model default |

"Compatible" (nothing crashes) ≠ "correct" (nothing is lost). Upgrading the readers first means that, by the time
the new field appears, someone is ready to store it.

**The repair:** the events are still in Kafka, so the read model can be **rebuilt**: stop the projector, reset its
offsets to the beginning, restart. All rows were fixed, and statuses stayed correct thanks to the idempotent upserts:
```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group order-status-projector --all-topics --reset-offsets --to-earliest --execute
```

### Exercise C: `api_outbox.py` (the Outbox pattern)

Run it **instead of** `api.py`. `POST /orders` only writes the event into a local SQLite `outbox` table and answers
`201`. A background relay thread publishes unpublished rows in insert order and marks them published.
- `order_id UNIQUE` + `INSERT OR IGNORE`: a retried request (same `Idempotency-Key`) is deduplicated **at the API**
  (`"duplicate_request": true`) and never even reaches Kafka twice.
- The relay marks only the **leading run** of delivered rows as published. If row 7 fails, rows 8+ are resent with it,
  so per-key order is kept.
- Crash after "Kafka acked" but before "marked published" means the row is sent again (**at-least-once**). That's harmless:
  the payload and `event_id` are identical, and every consumer is idempotent.
- `GET /outbox` shows unpublished rows: the "lag" of the API → Kafka hop. Alert if it grows.

**Tested** with Kafka unreachable:

| | `api.py` | `api_outbox.py` |
|---|---|---|
| POST while Kafka is down | `503` after ~5 s | **`201` in ~20 ms** |
| after Kafka is back | the client has to retry | relay published all rows on its own; all shipped |
| throughput (300 orders, 20 clients, my laptop) | ~680/s | ~115/s (a SQLite commit per request, sync endpoint) |

The trade-off: the outbox gives you **availability** (orders accepted during a Kafka outage) at the cost of an extra
DB write per request and a few hundred ms of extra delay. In a real system the outbox lives **in the same database and
transaction as your business data** (e.g. Postgres: `INSERT INTO orders` + `INSERT INTO outbox` in one commit). That's
what makes "saved" and "will be published" atomic. Tools like Debezium can do the relay by reading the DB's change log.

## Questions & Answers (for revision)

**1. Why does `POST /orders` wait for Kafka's delivery report? What could go wrong with `202` right after `produce()`?**

After `produce()` the event is only in the producer's **RAM** (Lesson 05, Q1/Q6). With an early `202`:
- the API process is killed or restarted (a deploy!) means queued orders are **gone**, but the client was told "accepted"
- Kafka is down for longer than `delivery.timeout.ms` means the delivery fails in a callback **nobody can report to
  the client anymore**
- the client never retries, because it got a success, so the order is silently lost

Waiting makes `201` mean "durably stored in Kafka" (`acks=all`), and any failure becomes a `503` the client *can* retry.
The cost is a few ms of latency per request. The alternative that is available *and* safe is the outbox (Exercise C).

**2. Right after a `201`, `GET` can return `404`. Why? How would a frontend handle it?**

The read model is updated **asynchronously**: API → Kafka → projector → SQLite. For a few ms (or seconds under load,
or minutes if the projector is down) the order exists in Kafka but not in `orders.db`. That's **eventual consistency**:
every write will be visible, just not instantly.
Frontend options:
- show "Order received, processing…" from the `201` response itself (it already has the `order_id` and `"pending"`); don't depend on an immediate `GET`
- **poll** `GET /orders/{id}` with backoff and treat `404` as "still pending" for a while
- push updates (WebSocket / Server-Sent Events) when the status changes
- "read your own writes": the API can return or remember what it just wrote for that user

**3. Trace a duplicate request with the same `Idempotency-Key` through all three services.**

1. **API:** same customer + key, so the same `order_id` (`uuid5`). With `api.py`, a **second `OrderPlaced` message**
   (new `event_id`, same `order_id`) lands in `shop-orders`. *(With `api_outbox.py` the duplicate stops here.)*
2. **Payment service:** processes both. `bank.charge(order_id, …)` uses `order_id` as the idempotency key, so the bank
   **returns the first result** without charging again. Both produce a `PaymentResult` with the **same deterministic
   `event_id`**.
3. **Projector:** sees 2× `order_placed` and 2× `payment_*` for one `order_id`. All are **upserts keyed by `order_id`**,
   so applying them twice gives the same row.

Duplicates exist in `shop-orders` and `shop-payments`, but at every point where they could cause harm (money, state)
the operation is idempotent. That's the end-to-end idea: **at-least-once delivery + idempotent processing =
effectively exactly-once results.**

**4. Why is a declined card a `payment_failed` event, not a DLQ message?**

A DLQ is for messages the system **couldn't process**: bugs, bad data, an unavailable dependency. They need a human or
a fix, then a replay. A declined card was processed **perfectly**: the bank gave a valid, final answer. It's a normal
**business outcome** that other services must react to (show "payment failed" to the customer, release stock, send an
email). Putting it in the DLQ would hide a normal fact from every downstream consumer and page an engineer for nothing.
Rule of thumb: **would retrying or a code fix change the result?** Yes means error (retry / DLQ). No means it's an
outcome, so publish it as an event.

**5. Why wouldn't Kafka transactions give exactly-once in the payment service? What guarantees "never charged twice"?**

The important side effect, **the charge, happens at the bank, outside Kafka**. A transaction can atomically commit
`PaymentResult` + the input offset, but if the service crashes after `bank.charge()` and before `commit_transaction()`,
the transaction is aborted, the order is reprocessed, and `bank.charge()` runs **again**. `abort_transaction()` can't
refund a card (Lesson 07, Q6).
What does guarantee it: the **bank-side idempotency key** (`order_id`). The bank itself refuses to charge the same key
twice, which also covers "the timeout happened after the money was taken" (Experiment 7). Idempotency must live where the
side effect happens (Lesson 06, Q7).
The shipping service, by contrast, is pure Kafka → Kafka, so there transactions *do* give exactly-once (Exercise A).

**6. Both topics are keyed by `order_id`, yet `payment_succeeded` can arrive before `order_placed`. How? How does the projector cope?**

Ordering is guaranteed **only within one partition of one topic** (Lesson 02). `shop-orders` and `shop-payments` are
**different topics**. The same key gives the same partition *number*, but in two separate logs. The projector fetches
from all its partitions independently, so if it's behind on `shop-orders` (a big backlog, a restart, a rebalance) it can
easily read the payment first. Replays (Exercise B's rebuild) mix the order even more.
The projector copes with **commutative upserts**:
- `order_placed` on an existing row only fills in the details and **never touches `status`**
- `payment_*` creates the row if it doesn't exist yet (with `customer_id`/`total` still NULL), and never downgrades `shipping`

Whatever order the events arrive in, the final row is the same.

**7. In Experiment 6, what happened to the messages the killed instance was processing?**

1. `kill -9`: the process vanished, with no `close()` and no final offset commit. Its last auto-commit (every 5 s) covered
   only messages whose offsets it had **stored**, i.e. fully handled.
2. For `session.timeout.ms` (10 s) the broker still considered it alive, so its partitions were **stuck**. Lag grew there.
3. Heartbeats timed out, the group rebalanced, and its partitions were assigned to the survivors.
4. The survivors started from the **last committed offset**. Everything after it was **processed again**: some orders
   that the dead instance had already charged (the bank returned the stored result: no second charge) and some whose
   `PaymentResult` was already published (a duplicate event with the same `event_id`; the projector upsert makes it
   harmless).
5. Nothing was lost (at-least-once), and nothing was charged twice (idempotency). `check.py` confirmed both.
With Ctrl+C, `close()` commits the stored offsets and leaves the group immediately, so the rebalance is near-instant and
there are fewer reprocessed messages.

**8. (Bonus) On call: what to put on the dashboard / alert?**

| Signal | Why | Example alert |
|---|---|---|
| **Consumer lag per group** (`payment-service`, `order-status-projector`, `shipping-service`) | the #1 Kafka health signal: growing means stuck, crashed or too slow | lag rising for 5 min, or > N messages / > X s behind |
| **DLQ message rate** | every DLQ message is a customer order not processed | any new message in `shop-orders-dlq` → alert |
| **API `503` rate + `POST` latency (p99)** | Kafka or producer trouble seen by customers | 503 > 1% for 2 min; p99 > 500 ms |
| **Orders stuck in `pending`** older than X min | end-to-end check: catches problems no single metric shows | count of pending older than 10 min > 0 |
| **Bank error/timeout rate** and **retries** | a dependency going bad before it becomes DLQ traffic | timeout rate > 20% |
| (outbox) **unpublished rows + oldest age** | the API → Kafka hop is falling behind | oldest unpublished > 1 min |
| broker basics: disk usage, under-replicated partitions | the platform itself | disk > 80% |

Alert on **symptoms customers feel** (lag age, pending orders, 503s), and use the rest of the dashboard to find the cause.

## Reset (start completely fresh)
Stop all services, then:
```bash
rm -rf data
K="docker exec kafka /opt/kafka/bin"
for t in shop-orders shop-payments shop-orders-dlq shop-shipments; do $K/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $t; done
for g in payment-service order-status-projector shipping-service shop-dlq-replayer-retries_exhausted; do
  $K/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group $g; done
uv run python create_topics.py
```
