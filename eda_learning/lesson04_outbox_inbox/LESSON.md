# Lesson 04: The Dual-Write Problem: Transactional Outbox and Inbox

**Goal:** guarantee that "the database changed" and "the event was published" happen **together or not at all**.

You already built a version of this in Kafka Lesson 08, Exercise C (`api_outbox.py`). This lesson explains *why* it's
needed, the variants, and the traps.

## The idea (read this first)

### 1. The problem

Almost every service has to do two things when something happens:

```python
db.save(order)                  # write 1: the database
producer.produce("orders", ...)  # write 2: the broker
```

These are **two different systems**, so there is **no transaction around both**. Every ordering breaks:

| Order | Crash between the two writes | Result |
|---|---|---|
| DB first, then publish | the order is saved, the event is never sent | **lost event**: nobody ships, nobody emails, but the customer sees the order |
| publish first, then DB | the event is sent, the DB write fails or rolls back | **ghost event**: shipping prepares an order that doesn't exist |

"It rarely crashes there" isn't good enough: deploys, OOM kills, network timeouts and broker outages all hit this
window, and at scale "rarely" means "every day".

Distributed transactions (2PC/XA) across a DB and Kafka aren't supported, and even where they exist they're slow and fragile.

### 2. The transactional outbox

Write the event **into the same database, in the same local transaction** as the business change. A separate
**relay** copies it to the broker later.

```
 service                              database (one ACID transaction)
 ───────                              ───────────────────────────────
 BEGIN                                orders:  | o-42 | placed | ... |
   INSERT INTO orders ...      ──►
   INSERT INTO outbox ...      ──►    outbox:  | id | type        | key  | payload | published_at |
 COMMIT                                        | 17 | OrderPlaced | o-42 | {...}   | NULL         |
                                                         │
                       relay (separate process) ─────────┘ reads unpublished rows in order,
                                                           produces to Kafka, waits for the ack,
                                                           then marks them published
```

Now either both rows exist or neither does. The relay **retries until the broker acks**, so the event is delivered
**at least once**. It can be published **twice** (crash after the ack, before marking). Consumers must be idempotent (Lesson 03).

### 3. Two kinds of relay

| | Polling publisher | Log tailing / CDC |
|---|---|---|
| How | `SELECT ... WHERE published_at IS NULL ORDER BY id` every N ms | reads the DB's transaction log (Postgres WAL, MySQL binlog) |
| Tool | your own code | **Debezium** (Kafka Connect) with the Outbox Event Router |
| Latency | the polling interval | near real time |
| DB load | queries all the time | very low |
| Complexity | simple | more infrastructure |

### 4. Traps

- **Ordering:** publish in `id` order and use the entity id as the Kafka key. If you run **several relay instances**,
  they must not grab the same rows or publish one entity's events out of order. In Postgres: `SELECT ... FOR UPDATE SKIP LOCKED`,
  or partition the work by key. (SQLite only allows one writer at a time anyway.)
- **Mark only after the ack.** Marking before the broker confirms brings back "lost event".
- **Cleanup:** delete or archive published rows, or the outbox grows forever.
- **A poison row** (payload too large, serialization error) can block everything behind it. Decide: skip to a
  dead-letter table, or stop and alert.

### 5. The inbox (consumer side)

The mirror image: when a consumer receives a message, it records `message_id` in an **inbox** table **in the same
transaction** as its own changes. That's the Lesson 03 dedupe table, given a name. If its handling also produces new
events, it writes them to its *own* outbox in that same transaction:

```
consume msg ──► BEGIN
                  INSERT INTO inbox(message_id)   -- fails if already processed → skip
                  UPDATE shipments ...
                  INSERT INTO outbox(...)         -- ShipmentScheduled
                COMMIT ──► then commit the Kafka offset
```

### 6. Alternative: "listen to yourself"

Publish the event **only** to Kafka, then have the service consume its own event to update its database. One write,
no dual-write. The downside: the service can't immediately read what it just wrote, and validating against current
state is harder. Useful sometimes, but the outbox is the default.

## Practical

> **Note:** the spec below is the plan. When you start this lesson, Claude writes and tests the code and replaces
> this section with **Files** + **Experiments** for you to run (like Lesson 01).

Kafka + SQLite. Start Kafka from the repo root with `docker compose up -d`. Use topic names starting with `eda-`.

1. **`broken_dual_write.py`:** an `OrderService.place_order()` that saves to SQLite then produces to `eda-orders`.
   Add a flag `--crash-between` that `os._exit(1)`s between the two writes on every 3rd order. Place 10 orders.
   Count rows in the DB vs messages in the topic.
2. **`order_service.py`:** the same, but with an `outbox` table written in the same transaction. No Kafka code here at all.
3. **`relay.py`:** a polling publisher. Reads unpublished rows in `id` order, produces with `key = order_id`,
   waits for the delivery report, then sets `published_at`. Add `--crash-after-ack` that kills it after the ack but
   before marking.
4. **`shipping_consumer.py`:** consumes `eda-orders` with an **inbox** table, and writes a `ShipmentScheduled` event
   to its own outbox in the same transaction. Commit offsets only after the DB commit.
5. **Chaos run:** place 50 orders, run the relay with `--crash-after-ack` a few times, restart it, then check:
   every order has exactly one shipment, some messages were published twice, and the inbox caught them.
6. Write `check.py` that verifies those three facts automatically.

**Done when:**
- [ ] `broken_dual_write.py` shows DB and Kafka disagreeing
- [ ] with the outbox, DB and Kafka always agree after the relay catches up, even with crashes
- [ ] you can point to a duplicate in Kafka and show that the inbox ignored it
- [ ] `check.py` passes after the chaos run

## Questions (answer them in your own words, then send them to me)

1. Explain both failure modes of a dual write ("lost" and "ghost") with a concrete example each.
2. The relay crashes after Kafka acked but before `published_at` was set. What happens on restart? Why is that
   acceptable, and what must be true of consumers?
3. Why must the relay mark rows only *after* the ack? What breaks if it marks first?
4. You run 3 relay instances for throughput. Describe two things that can go wrong, and how to prevent them.
5. Compare polling vs CDC (Debezium) for a service doing 5 orders/minute, and for one doing 5,000 orders/second.
6. What is the inbox pattern, and how is it related to your Lesson 03 dedupe table?
7. Your `api_outbox.py` from Kafka Lesson 08: what would you change after this lesson?
8. "Just use Kafka transactions instead." Why doesn't that solve the dual write between Postgres and Kafka?
