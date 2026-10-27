# Lesson 03: Delivery in the Real World: Duplicates, Disorder, Idempotency

**Goal:** design consumers that stay correct when events arrive **twice**, **late**, or **in the wrong order**,
because in any real system they will.

## The idea (read this first)

### 1. Two ways to deliver a message

```
point-to-point queue (competing consumers)          publish/subscribe (fan-out)
            ┌──► worker 1                                        ┌──► email service     (gets every event)
 queue ─────┼──► worker 2   each message goes to ONE    topic ───┼──► analytics service (gets every event)
            └──► worker 3   worker (load balancing)              └──► loyalty service   (gets every event)
```

- **Queue:** "get this job done once". Used for commands and work distribution.
- **Pub/sub:** "tell everyone who cares". Used for events.
- **Kafka does both:** consumers in the *same* group compete (queue), *different* groups each get a copy (pub/sub).
  RabbitMQ: queues + exchanges. AWS: SQS (queue) + SNS/EventBridge (pub/sub).

### 2. Delivery guarantees (recap from Kafka)

| Guarantee | How you get it | Risk |
|---|---|---|
| at-most-once | commit, *then* process | crash = message lost |
| **at-least-once** | process, *then* commit | crash = message **processed again** |
| exactly-once | Kafka transactions, only **inside Kafka** (Kafka Lesson 07) | the moment you send an email or call a bank, you're back to at-least-once |

So the real-world rule is:
> **effectively-once = at-least-once delivery + idempotent processing**

### 3. Where duplicates come from

- the producer retries after a timeout, but the first send actually worked
- the consumer crashes after processing but before committing
- a rebalance hands a partition to another consumer mid-batch
- someone replays a topic or a DLQ on purpose
- the outbox relay (Lesson 04) re-publishes after a crash

### 4. Making a consumer idempotent

Idempotent = processing the same event twice gives the same result as processing it once.

| Technique | How | Example |
|---|---|---|
| **Naturally idempotent operation** | *set* instead of *add* | `status = 'paid'` ✅ vs `balance += 10` ❌ |
| **Dedupe table (inbox)** | store `event_id` in a `processed_events` table **in the same DB transaction** as the effect. Skip if it's already there | most general solution |
| **Idempotency key** for external calls | send `event_id` (or a business id) to the API, which remembers it | the `bank.py` in Kafka Lesson 08 |
| **Version / sequence check** | apply only if `event.version == current_version + 1` (or `>`) | also solves ordering, see below |

⚠️ The dedupe check and the effect **must be atomic**. "Check, then do, then record" in three separate steps leaves a
window where a crash causes a duplicate.

### 5. Ordering

- Kafka orders messages **only within a partition**. Same key → same partition → ordered.
- Different **topics** are never ordered relative to each other: `PaymentSucceeded` (payments topic) can arrive before
  `OrderPlaced` (orders topic) at a consumer that reads both.
- Retries and DLQ replays bring old events back later, out of order.

Strategies, from simplest:
1. **Key by entity** so one entity's events stay in order.
2. **Version each entity's events** (`order o-42, version 3`) and ignore anything older than what you've seen.
   This is *last-writer-wins by version*, not by arrival time or clock time.
3. **Handle "arrived early"**: create a placeholder row ("payment received for unknown order"), or park the event and retry later.
4. **Design commutative updates** where you can: operations whose order doesn't matter (adding to a set).

## Practical

> **Note:** the spec below is the plan. When you start this lesson, Claude writes and tests the code and replaces
> this section with **Files** + **Experiments** for you to run (like Lesson 01).

Standard library + `sqlite3`. Work in this folder.

1. **`unreliable_bus.py`:** a bus that delivers a list of events to a handler but, controlled by a seed:
   - delivers ~20% of events **twice**
   - **shuffles** events inside a sliding window of 5 (so nearby events arrive out of order)
2. **`events.py`:** generate 1,000 events for 50 bank accounts: `MoneyDeposited` / `MoneyWithdrawn`, each with
   `event_id`, `account_id`, `version` (per account, starting at 1) and `amount_cents`. Also compute the **true**
   final balances directly from the list, for checking.
3. **`naive_consumer.py`:** a handler that does `balance += amount` in SQLite. Run it through the unreliable bus and
   compare with the true balances. How many accounts are wrong?
4. **`dedupe_consumer.py`:** add a `processed_events(event_id PRIMARY KEY)` table. Insert the id and update the balance
   **in one transaction**. Are all balances right now? (Think about whether order matters for sums.)
5. **`status_consumer.py`:** a different read model: the account's `last_operation` and `last_amount`
   (*the latest* operation). Show that dedupe alone gives the wrong answer here, then fix it with the `version` field.
6. **Crash test:** in `dedupe_consumer.py`, raise an exception randomly **after** updating the balance but **before**
   commit, then "redeliver". Prove the result is still correct. Then deliberately break atomicity (commit the
   dedupe row in a separate transaction) and show it fail.

**Done when:**
- [ ] the naive consumer is wrong, and you can explain the size of the error
- [ ] the dedupe consumer matches the true balances for 5 different seeds
- [ ] the status consumer matches only once you use versions
- [ ] the crash test passes with one transaction and fails with two

## Questions (answer them in your own words, then send them to me)

1. Why is "exactly-once" in Kafka not enough once your consumer sends an email or calls a payment API?
2. In step 4, sums came out right even though events were shuffled. Why? In step 5, why weren't they enough?
3. Your dedupe table grows forever. How would you limit it, and what risk does that bring?
4. `OrderPlaced` and `PaymentSucceeded` are on different topics. The shipping consumer reads both and must ship only
   paid orders. Payment arrives first. Describe two ways to handle it.
5. A consumer calls an external API that does **not** support idempotency keys. What can you still do to reduce
   duplicate side effects? Can you remove them completely?
6. Would you use a queue or pub/sub for: (a) resizing uploaded images, (b) `UserDeleted`, (c) sending a password-reset
   email, (d) `PriceChanged`? Why?
7. Why is "latest timestamp wins" a dangerous way to resolve out-of-order events, compared with per-entity versions?
