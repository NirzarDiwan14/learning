# Lesson 07: Event Sourcing

**Goal:** store **what happened** instead of **what is**, and derive the current state from the history.

## The idea (read this first)

### 1. State vs history

A normal database stores the **current state** and overwrites it:

```
accounts:  | acc-1 | balance: 70 |        ← how did it get to 70? Unknown.
```

**Event sourcing** stores the **sequence of events** as the source of truth, append-only, never updated:

```
stream "acc-1":
  v1  AccountOpened     {owner: "alice"}
  v2  MoneyDeposited    {amount: 100}
  v3  MoneyWithdrawn    {amount: 50}
  v4  MoneyDeposited    {amount: 20}
                                            current state = fold(evolve, events, initial)  →  balance 70
```

The current state is a **left fold** over the events: `state = evolve(state, event)` for each one in order.

### 2. The command → events → state loop

```
              load events of stream ──► fold ──► current state
                                                     │
 command (Withdraw 80) ──────────────────► decide(state, command) ──► [] or raise (insufficient funds)
                                                     │                 or [MoneyWithdrawn{80}]
                                                     ▼
                              append new events with expected_version = 4
```

- `decide(state, command) -> list[event]`: business rules. Can reject. **No side effects.**
- `evolve(state, event) -> state`: pure. **Never rejects**: events are facts, they already happened.
- **Aggregate:** the consistency boundary (one account, one order). One stream per aggregate.

### 3. Optimistic concurrency

Two requests load `acc-1` at version 4 and both decide to withdraw 60 from a balance of 70. Without protection,
both append and the balance goes negative. Fix: append **with an expected version**. The store has a
`UNIQUE(stream_id, version)` constraint, so the second append fails, and that request reloads and decides again
(and this time gets "insufficient funds").

### 4. What you get

- **Full audit log** for free: how did we get here?
- **Time travel:** state at any point ("what was the balance on 1 March?")
- **New read models from old data:** build a report today over 3 years of history (Lesson 08)
- **Debugging:** replay the exact events that caused a bug
- **Natural fit with EDA:** the events already exist

### 5. What it costs

- **Events are forever:** you must be able to read every old event version, always (Lesson 09 upcasting).
- **Querying is hard:** "all accounts with balance > 1000" needs a projection (Lesson 08).
- **Long streams** are slow to load, so you use **snapshots** (save the state at version N, then load the snapshot +
  events after N).
- **Deleting personal data (GDPR)** on an immutable log: **crypto-shredding** (encrypt PII per person, delete the key).
- More concepts for the team. **Use it where history matters** (money, orders, compliance), not for every CRUD table.

### 6. Event sourcing ≠ event-driven architecture

- You can have EDA without event sourcing (Lessons 01–06), and event sourcing inside one service without publishing anything.
- Don't publish your internal event-sourced events as integration events directly: that freezes your internals.
  Translate them (Lesson 02: domain vs integration events).
- **Is Kafka an event store?** Usually not for the write side: it has no per-aggregate streams ("load all events of acc-1"
  means scanning a partition) and no "append only if the stream is at version N" check. People use a database table,
  EventStoreDB (now **KurrentDB**), or Marten / Axon, and then publish to Kafka through an outbox.

## Practical

> **Note:** the spec below is the plan. When you start this lesson, Claude writes and tests the code and replaces
> this section with **Files** + **Experiments** for you to run (like Lesson 01).

SQLite only. Work in this folder. Domain: **a bank account.**

1. **`event_store.py`:** table `events(global_position INTEGER PRIMARY KEY, stream_id, version, type, data JSON,
   recorded_at, UNIQUE(stream_id, version))`. Functions:
   - `append(stream_id, expected_version, events)`: raises `ConcurrencyError` on conflict
   - `load(stream_id, from_version=0)`
   - `read_all(from_position=0)` (for Lesson 08)
2. **`account.py`:** events `AccountOpened`, `MoneyDeposited`, `MoneyWithdrawn`, `AccountClosed`. Pure functions
   `evolve(state, event)` and `decide(state, command)`. Rules: no withdrawals beyond balance + an overdraft limit,
   nothing allowed on a closed account.
3. **`handler.py`:** `handle(command)` = load → fold → decide → append. On `ConcurrencyError`, retry up to 3 times.
4. **Concurrency test:** two threads each withdraw 60 from a balance of 70, 100 times over. The balance must never go
   below the limit, and you should see retries happen.
5. **Time travel:** `balance_at(stream_id, timestamp)`.
6. **Snapshots:** every 100 events, store `(stream_id, version, state)` in `snapshots`. Load = snapshot + newer events.
   Time loading a 10,000-event stream with and without snapshots.
7. **Bug-fix replay:** introduce a bug in `evolve` (e.g. withdrawals count double), see the wrong balance, fix the code,
   and show that the history was right all along. Nothing in the store changed.

**Done when:**
- [ ] the concurrency test never breaks the overdraft rule and shows retries
- [ ] `balance_at` returns correct past balances
- [ ] snapshots speed up loading, with numbers
- [ ] you can explain why the bug fix needed no data migration

## Questions (answer them in your own words, then send them to me)

1. Why must `evolve` never reject an event, while `decide` may reject a command?
2. Walk through what happens, step by step, when two withdrawals race. Where exactly is the conflict detected?
3. In a normal CRUD table, the evolve bug in step 7 would have corrupted data. Why didn't it here?
4. A snapshot was taken with the *buggy* `evolve`. What's the problem, and how do you deal with it?
5. A customer asks you to delete their personal data. How do you do that in an append-only store?
6. Why is Kafka a poor fit as the *write-side* event store for this account? What would you use Kafka for instead?
7. Name a part of a system you know where you would **not** use event sourcing, and why.
8. "Withdraw 80" is a command and "MoneyWithdrawn 80" is an event. Why does the store keep only the events?
