# Lesson 08: CQRS and Projections

**Goal:** serve fast, convenient reads from **read models** built from events, and deal honestly with the fact that
they are slightly behind.

## The idea (read this first)

### 1. CQRS

**Command Query Responsibility Segregation:** use a different model for changing data than for reading it.

```
                 commands                                         queries
 client ─────────────────────► write model                ┌──────────────────────── client
                               (aggregates, rules,          │
                                event store or DB)          │
                                       │ events             │
                                       ▼                    │
                                 projector(s) ─────► read model(s): denormalized, shaped for each screen
                                                    (SQL table, Elasticsearch, Redis, ...)
```

- The write side is optimized for **correctness** (rules, concurrency).
- Each read side is optimized for **one kind of query**, and there can be many.
- CQRS **doesn't require** event sourcing. Your Kafka Lesson 08 projector was CQRS on top of Kafka events.

### 2. Projections

A **projection** consumes events in order and keeps a read model up to date:

```python
for event in read_all(from_position=checkpoint):
    with db.transaction():
        apply(event)                                  # update the read model
        save_checkpoint(event.global_position)        # in the SAME transaction
```

- **Checkpoint in the same transaction** as the update. Then a crash never applies an event twice or skips one.
  (It's the inbox idea again.)
- Projections must handle **every** event type they subscribe to, including old versions (Lesson 09).
- They are **disposable**: delete the table, reset the checkpoint to 0, replay, done. That's how you fix projection
  bugs and add new read models over old history.
- **Rebuild without downtime:** build the new version into a new table alongside the old one, switch reads over when
  it has caught up, then drop the old one (blue/green).

### 3. Eventual consistency

The read model **lags** behind the write model (milliseconds to seconds, or longer under load or after a crash).
A user clicks "Deposit 100", gets redirected to the account page, and sees the old balance. Strategies:

| Strategy | How |
|---|---|
| **Read-your-own-writes** | the command returns the new version/position. The query waits (with a timeout) until the projection's checkpoint ≥ that position |
| **Return the result from the command** | the write side tells the client the new balance directly |
| **Optimistic UI** | the client shows the expected result right away |
| **Push** | WebSocket/SSE notifies the client when the projection updates |
| **Honest UI** | "processing…", like a bank showing "pending transactions" |

Don't fix it by reading the write model for every query: then you have no CQRS left.

### 4. When CQRS is too much

For a simple CRUD screen, one model is fine. CQRS pays off when reads and writes have very different shapes or loads,
when you need many views of the same data, or with event sourcing (where you can't query the event store efficiently anyway).

## Practical

> **Note:** the spec below is the plan. When you start this lesson, Claude writes and tests the code and replaces
> this section with **Files** + **Experiments** for you to run (like Lesson 01).

SQLite, building on your Lesson 07 event store. Work in this folder (import from `../lesson07_event_sourcing`, or copy).

1. **`projector.py`:** a generic runner: `run(projection, poll_interval)` reads `read_all(from_position)` and calls
   `projection.apply(event, tx)`, storing a checkpoint per projection name in the same transaction.
2. **Projections:**
   - `AccountSummary`: `account_id, owner, balance, status, last_activity`
   - `DailyTotals`: total deposits and withdrawals per day
   - `RichList`: the top 10 accounts by balance
3. **Lag:** add `--delay 2` to the projector. Make a deposit through your Lesson 07 handler and immediately query
   `AccountSummary`. See the stale result.
4. **Read-your-own-writes:** `handle(command)` returns the global position. Write `get_summary(account_id, min_position,
   timeout)` that waits until the checkpoint reaches it.
5. **Crash safety:** kill the projector at random points 10 times while 1,000 events are written. Compare the read
   models with a fresh full rebuild. They must be identical.
6. **New read model over old history:** add `OverdraftAlerts` (accounts that went below zero, and when) after all
   events exist, and backfill it by replaying from position 0.
7. **Blue/green rebuild:** change `AccountSummary` (add `transaction_count`), rebuild into `account_summary_v2` while
   v1 keeps serving, then switch.

**Done when:**
- [ ] the crash test produces read models identical to a clean rebuild
- [ ] read-your-own-writes returns the fresh balance even with `--delay 2`
- [ ] `OverdraftAlerts` works over history that existed before it was written
- [ ] you can describe the switch-over step of your blue/green rebuild

## Questions (answer them in your own words, then send them to me)

1. Why must the checkpoint be saved in the same transaction as the read-model update? What happens if it isn't, in each order?
2. Your projector crashes for 10 minutes. What do users see during that time, and after it restarts?
3. A product manager says "the balance must always be up to date." What do you reply, and which strategy would you offer?
4. Why can you delete a projection's table without losing data, but not the event store?
5. Can two projections read the same events and disagree for a moment? Is that a problem?
6. When would you **not** use CQRS?
7. Your Kafka Lesson 08 projector: which ideas from this lesson did it already use, and which were missing?
