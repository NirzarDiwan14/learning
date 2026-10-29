# Lesson 01: Why Events?

## The idea (read this first)

Most code starts with **direct calls**: service A needs something done, so it calls B, then C, then D,
and waits for each one.

```
            direct calls (request/response)                     events (publish/subscribe)

 place_order ──► reserve_stock                         place_order ──► reserve_stock
      │                                                     │
      ├──────► send_confirmation_email                      └──► publish OrderPlaced ──► bus ──┬──► email
      │                                                                                       ├──► analytics
      └──────► record_analytics                                                               └──► (anyone new)
   A knows B, C, D, and waits for all of them                   A knows only the event. It doesn't know who listens.
```

In an **event-driven** design, A just announces **"this happened"** (`OrderPlaced`) and moves on.
Whoever cares subscribes. A doesn't know who they are or how many there are.

### Three kinds of messages

| Kind | Meaning | Tense | Receivers | Can it be refused? | Example |
|---|---|---|---|---|---|
| **Command** | "please do this" | imperative | exactly **one** owner | yes | `ReserveStock`, `ChargeCard` |
| **Event** | "this happened" | **past** tense | **zero or more**, unknown to the sender | **no**, it's already a fact | `OrderPlaced`, `PaymentFailed` |
| **Query** | "tell me something" | question | exactly one | n/a, no side effects | `GetOrderStatus` |

An event is a **fact**: immutable, in the past tense, owned by the service where it happened.
You can't "reject" `OrderPlaced`, just like you can't reject yesterday's weather. You can only react to it.

### Coupling: what events remove (and what they don't)

"Coupling" means *if X changes or breaks, Y has to care.* There are several kinds:

- **Knowledge coupling:** A's code names B, C and D. To add a feature, you edit A.
- **Temporal coupling:** B, C and D must all be **up and fast right now**, or A is slow or fails.
- **Failure coupling:** an error in C (email) becomes an error in A (the order).

Keep these three in mind during the experiments, and notice which ones each version actually removes.

## Files

- `v1_direct_calls.py`: the order service calls inventory, email and analytics directly
- `event_bus.py`: two tiny in-memory buses. `SyncEventBus` calls handlers immediately. `AsyncEventBus` puts events in a queue and a background thread delivers them
- `v2_events.py`: the same shop, but the order service publishes `OrderPlaced` and the others subscribe

Both shops start with `keyboard: 5, mouse: 0` and place the same 3 orders (order-2 asks for a mouse, which is out of stock).
Run from this folder with `uv run python <file> [flags]`. No Kafka needed for this lesson.

## Experiments (do them in order, keep notes on what you see)

### Experiment 1: the happy path
```bash
uv run python v1_direct_calls.py
uv run python v2_events.py
```
Same output? Compare how long each `place_order` takes.

### Experiment 2: the email server is down
```bash
uv run python v1_direct_calls.py --email-down
uv run python v2_events.py --email-down
```
For each version: which orders were confirmed? Look closely at **`stock at the end`** in v1. How many keyboards
were reserved, and for how many confirmed orders?

### Experiment 3: analytics is slow
```bash
uv run python v1_direct_calls.py --slow-analytics
uv run python v2_events.py --slow-analytics
```
Does switching to events (with `SyncEventBus`) make `place_order` faster? Why or why not? Look at `SyncEventBus.publish`.

### Experiment 4: go asynchronous
```bash
uv run python v2_events.py --async --slow-analytics
uv run python v2_events.py --async --email-down
```
How long does `place_order` take now? Look at **the order of the output lines**: when does the email get sent
compared to when the customer hears "confirmed"?

### Experiment 5: add a feature without touching the order service
Product wants **loyalty points**: 10 points per item ordered. In `v2_events.py`, add a handler
`award_loyalty_points(event)` that prints `[loyalty] +N points for <customer>`, and subscribe it.
**Rule: don't change `place_order`.**
Then think: what would you have to change in `v1_direct_calls.py` to do the same thing?

### Experiment 6: the process dies
```bash
uv run python v2_events.py --async --slow-analytics --crash
```
Three orders came in and two were "confirmed". How many confirmation emails went out? How many analytics records?

## Questions & Answers (for revision)

**1. In Experiment 2, v1 failed all orders, but the stock still went down. What happened, and why is it worse than "the email didn't get sent"?**

`place_order` runs three steps in a row: `reserve_stock` succeeded and changed the stock, then `send_confirmation_email`
raised, so the exception jumped out of `place_order` and the order was reported as failed. **Nothing undid step 1.**
End result: `keyboard: 5 → 2`. **3 keyboards are locked for orders that don't exist.**

Why it's worse:
- It's a **partial failure that leaves the system inconsistent**, not just a missing side effect. The stock is now lying.
- It's **silent and permanent**: nobody gets an error about the stock. Real customers later see "out of stock" for
  keyboards sitting on the shelf, so you lose sales.
- The customer was told "failed", so they **retry**, and each retry locks *more* stock.
- The email wasn't even important for the order's correctness. A **non-essential step** broke an essential one.

⚠️ Your answer found the cause (the ordering of the calls), but not the consequence: inconsistent state with no undo.
That consequence is the core of Lessons 04 and 06 (outbox, sagas, compensation).

**2. Which couplings are removed with (a) SyncEventBus and (b) AsyncEventBus?**

| Coupling | (a) `SyncEventBus` | (b) `AsyncEventBus` |
|---|---|---|
| **Knowledge** | ✅ removed: `place_order` only knows `OrderPlaced`, not email or analytics | ✅ removed |
| **Failure** | ✅ removed: the bus catches handler exceptions, so the email failing doesn't fail the order | ✅ removed |
| **Temporal** | ❌ **NOT removed**: handlers still run inside `publish()`, so slow analytics still made `place_order` take 2 s (Experiment 3) | ✅ removed *within the process*: `publish()` only queues |

⚠️ You said "all 3 in (b)", but you skipped (a), which is the interesting case. **Events alone don't make things
asynchronous.** The *bus* decides that. And even (b) only removes temporal coupling while the process is alive: the
handlers live in the same process, so if it dies, they die too (Experiment 6). Real temporal decoupling ("email
service is down for an hour, orders keep working") needs a **durable broker** between separate processes.
One coupling always remains: everyone depends on the **shape of the event** (Lessons 02 and 09).

**3. Why is `reserve_stock` still a direct call? What goes wrong if inventory just listens for `OrderPlaced`?**

Because **the order service needs the answer before it can continue.** Whether stock exists decides
"confirmed" or "rejected". That's a **command with a reply that can be refused**, not a fact.

If inventory only listened to `OrderPlaced`:
- the order service would publish `OrderPlaced` and say "confirmed" for order-2 **even though there are no mice**
- inventory **can't refuse an event**. It's already a fact. It could only react afterwards (`StockReservationFailed`),
  and then someone has to cancel the order and apologize to the customer (a compensation)
- the meaning of `OrderPlaced` becomes a lie: "placed" but maybe not really

⚠️ Your answer has a good instinct: inventory **owns** the stock, so it's the source of truth about it and should
publish facts about stock (`StockReserved`, `OutOfStock`). That's a real alternative design, where the order starts
as `PENDING` and becomes confirmed when `StockReserved` arrives. But then `OrderPlaced` means "requested, not
confirmed yet" and the customer gets the answer later. You'll build exactly that in Lessons 05 and 06. The key idea for
this question: **use a direct call (or a command with a reply) when you need the answer now**.

**4. Classify and fix the names.**

| Message | Kind | Name OK? |
|---|---|---|
| `ReserveStock` | command | ✅ |
| `OrderPlaced` | event | ✅ |
| `GetOrderStatus` | query | ✅ |
| `SendEmail` | command | too generic: *which* email, and why? `SendOrderConfirmation` |
| `PaymentFailed` | event | ✅ (or more precise: `PaymentDeclined`) |
| `CancelOrder` | command | ✅ (the event that follows is `OrderCancelled`) |
| `OrderShip` | **ambiguous** | ❌ not past tense, so it could be the command `ShipOrder` or the event `OrderShipped` |
| `UserUpdate` | **ambiguous** | ❌ not past tense, *and* a CRUD event: better a specific event like `EmailAddressChanged` |

⚠️ You wrote "names are fine" and then fixed two of them, which is a contradiction. Listen to that instinct: **a name that
isn't past tense is not an event**, and if you can't tell whether a message is a command or an event, the name is wrong.
`UserUpdated` is better than `UserUpdate` but still weak: it doesn't say *what* changed or *why* (Lesson 02).

**5. `SendWelcomeEmail` vs `CustomerRegistered`?**

✅ The teammate is right, and your reasoning is right too: the registration service should announce **what happened in
its domain**, and reacting to it (sending the email) is the email service's job.

Why it matters (Experiment 5): tomorrow marketing wants a CRM entry, loyalty wants a sign-up bonus, and analytics wants
a counter. With `CustomerRegistered` they all just subscribe, with no change to the registration service. With
`SendWelcomeEmail`, a CRM listening to an "email" message is absurd, so someone would add `CreateCrmEntry`,
`GiveSignupBonus`… to the registration service, and you're back to v1's knowledge coupling.
`SendWelcomeEmail` is a **command disguised as an event**. It names the *reaction* instead of the *fact*.

**6. Experiment 6: what was lost, and what does Kafka give you that `AsyncEventBus` doesn't?**

Lost: orders 1 and 3 were **confirmed to the customer**, but their 2 confirmation emails and 2 analytics records
(and your loyalty points) were never processed. The events existed **only in RAM** (the `queue.Queue`), and
`os._exit` threw RAM away.

What Kafka adds (✅ you got the main one: persistence):
- **Durability:** the event is written to disk (and replicated to other brokers in a real cluster) *before* the producer gets its ack
- **Consumers in separate processes:** the email service can crash, be redeployed or scaled without touching the order service
- **Committed offsets:** a restarted consumer continues where it stopped (at-least-once)
- **Replay:** rewind offsets to reprocess history, or start a brand-new consumer from the beginning
- **Retention:** events stay even when nobody is consuming right now
- **Partitions:** parallelism with ordering per key

⚠️ The producer must still **wait for the ack** before saying "confirmed" (Kafka Lesson 08, promise 1), and even then
the DB-write vs publish gap remains. That's Lesson 04.

**7. What new problems did EDA create that v1 didn't have?**

- **Silent failures:** the email failed and *nobody was told*. v1 at least returned an error. Now you need
  monitoring, retries and a DLQ to even notice.
- **Eventual consistency:** "confirmed" is said **before** the side effects happen. For a moment (or forever, if
  something fails) the system's parts disagree.
- **Lost events:** in-memory async = at-most-once (Experiment 6).
- **Harder debugging:** there's no single call stack. "What happened to order 1?" spans several handlers (Lesson 10).
- **Ordering:** with parallel consumers, events can be processed in a different order than they happened (Lesson 03).

⚠️ "Duplicates" isn't actually a problem of *this* bus: an in-memory queue delivers at most once. Duplicates arrive
when you switch to a **durable** broker with at-least-once delivery (Lesson 03). DLQ + retries are good fixes for
silent failures, but name the problem first: **nobody notices**, and **the customer was told "confirmed" too early**.

**8. One example where you'd keep a direct call.**

The question asks about a specific **interaction**, not about the size of the whole system. Keep a direct call when
**the caller needs the answer right now to continue**, or needs **strong consistency**:
- **Payment authorization at checkout:** you can't show "order complete" without knowing whether the card worked
- **Stock / seat availability** before confirming (Q3)
- **Login / permission checks:** "is this token valid?"
- **Price or shipping-cost calculation** shown on the page
- **Any query:** "get customer details" as an event + reply event is a slow, fragile HTTP call (an anti-pattern, Lesson 11)

⚠️ "Small systems with few microservices" is a fair point about whether to adopt EDA **at all** (Lesson 11), but even a
huge event-driven system still uses direct calls for the cases above.

### Your Experiment 5 code (loyalty points)
✅ Done right: in v2 you added `award_reward_points` and subscribed it **without touching `place_order`**, while in v1
you had to edit `place_order` itself. That's knowledge coupling, shown in code.
Also notice what v1's edit brought along: one more call that adds latency and one more way for an order to fail.
Small detail: the spec said 10 points per item (`event.qty * 10`); yours gives 1 per item.
