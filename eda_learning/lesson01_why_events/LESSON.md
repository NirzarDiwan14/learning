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

## Questions (answer them in your own words, then send them to me)

1. In Experiment 2, v1 failed **all** orders, but the stock still went down. Explain exactly what happened, and why
   this is worse than "the email didn't get sent".
2. For each of the three couplings (knowledge, temporal, failure), say whether it is **removed** in
   (a) v2 with `SyncEventBus` and (b) v2 with `AsyncEventBus`. One line of reasoning each.
3. `reserve_stock` is still a direct call in v2, not an event handler. Why? What would go wrong if inventory just
   *listened* for `OrderPlaced`?
4. Classify each as **command**, **event** or **query**, and fix the name if it's badly named for its kind:
   `ReserveStock`, `OrderPlaced`, `GetOrderStatus`, `SendEmail`, `PaymentFailed`, `CancelOrder`, `OrderShip`, `UserUpdate`.
5. The customer-registration service publishes `SendWelcomeEmail`. A teammate suggests renaming it to
   `CustomerRegistered`. Who is right, and why does it matter (think about Experiment 5)?
6. Experiment 6: what was lost, and why? List what Kafka would give you that `AsyncEventBus` doesn't. (Use what
   you learned in the Kafka course.)
7. With `AsyncEventBus` the customer sees "confirmed" before the email is sent, and if the email fails, nobody
   is told. What **new problems** did EDA just create that v1 didn't have? Name at least two.
8. Give one example where you would **not** use events and would keep a direct call. Why?
