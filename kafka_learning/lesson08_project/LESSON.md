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

## Questions (answer these and send them to me, along with your exercise code)

1. Why does `POST /orders` wait for Kafka's delivery report before answering? What exactly could go wrong if it
   answered `202` right after `produce()`? (Lesson 05, Q6)
2. Right after a `201`, `GET /orders/{id}` can return `404`. Why? This is called **eventual consistency**. How would
   you handle it in a frontend?
3. Trace a duplicate HTTP request with the same `Idempotency-Key` through **all three** services. At which points
   does a duplicate exist, and why does it never cause harm?
4. Why is a declined card a **`payment_failed` event** and not a DLQ message? What's the difference between an
   *error* and a *business outcome*?
5. `payment_service.py` doesn't use Kafka transactions. Why wouldn't they give exactly-once here (Lesson 07, Q6)?
   What guarantees "never charged twice" instead?
6. Both topics are keyed by `order_id`, and yet the projector can receive `payment_succeeded` **before**
   `order_placed` for the same order. How? How does `projector.py` cope with it?
7. In Experiment 6, what happened to the messages the killed instance was processing at that moment? Walk through it.
8. (Bonus) You're on call for this system. What 3–5 things would you put on a dashboard or alert, and at what thresholds?

## Reset (start completely fresh)
Stop all services, then:
```bash
rm -rf data
K="docker exec kafka /opt/kafka/bin"
for t in shop-orders shop-payments shop-orders-dlq; do $K/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $t; done
for g in payment-service order-status-projector shop-dlq-replayer-retries_exhausted; do
  $K/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group $g; done
uv run python create_topics.py
```
