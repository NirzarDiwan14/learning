# Lesson 06: Sagas: Long-Running Workflows and Compensation

**Goal:** keep data consistent across several services **without** a distributed transaction, including when a step
fails halfway, a service never answers, or the coordinator itself crashes.

## The idea (read this first)

### 1. Why we need sagas

Each service owns its own database, so "reserve stock + charge card + book courier" can't be one ACID transaction.
A **saga** (Garcia-Molina & Salem, 1987) is a **sequence of local transactions**, one per service. Each step has a
**compensating action** that semantically undoes it if a later step fails.

```
 T1 reserve stock ──► T2 charge card ──► T3 book courier ──► done ✅
                                               │ fails
          C1 release stock ◄── C2 refund card ◄┘          ──► order rejected, everything undone ↩
```

### 2. Compensation is not rollback

- A rollback makes it look like nothing happened. A compensation is a **new action** that offsets an old one:
  the customer *will* see a charge and then a refund on their statement.
- Some actions **can't** be compensated (an email already sent, a parcel already handed to the courier).
  Order your steps so these happen last.
- Compensations can fail too, so they must be **retryable** and **idempotent**.

### 3. Three kinds of steps (Chris Richardson's terms)

| Kind | Meaning | Example |
|---|---|---|
| **Compensatable** | can be undone, so it goes **early** | reserve stock, authorize payment |
| **Pivot** | the point of no return: once it succeeds the saga must finish | capture payment / confirm order |
| **Retriable** | after the pivot, can't fail for business reasons, so retry until it works | send confirmation, update analytics |

Good design: `compensatable* → pivot → retriable*`.

### 4. Sagas don't give you isolation

Between T1 and T3 the world can see half-done state: stock is reserved for an order that may still be rejected.
Countermeasures:
- **Semantic lock:** a status like `PENDING` that other operations respect ("don't ship PENDING orders").
- **Commutative updates:** design steps so their order doesn't matter.
- **Reread value:** re-check that data hasn't changed before the pivot.
- **Version check:** fail the step if the entity's version moved.

### 5. Orchestrated vs choreographed sagas

Both work (Lesson 05). Orchestrated sagas are much easier to reason about once there are compensations: the
orchestrator **persists the saga state** after every step, so after a crash it knows exactly what was done and
what to undo.

### 6. Timeouts and deadlines

"No answer" is the hardest case: the payment service may have charged the card and the reply got lost. Rules:
- Every step that waits gets a **deadline** stored with the saga state.
- On timeout you **don't know** whether the step happened. Either retry it (it must be idempotent), or compensate it
  (the compensation must be safe even if the step never happened: "refund if charged").
- A periodic job finds sagas past their deadline. Nothing else would ever wake them up.

## Practical

> **Note:** the spec below is the plan. When you start this lesson, Claude writes and tests the code and replaces
> this section with **Files** + **Experiments** for you to run (like Lesson 01).

Kafka + SQLite, building on your Lesson 05 orchestration code. Work in this folder.

Saga: **reserve stock → authorize payment → book courier → capture payment (pivot) → send confirmation.**

1. **`orchestrator.py`:** persist each saga in a `sagas` table (`order_id`, `state`, `step`, `deadline`,
   `compensations_done`) and update it **in the same transaction** as writing the next command to an outbox (Lesson 04).
2. **Participants** (`inventory.py`, `payments.py`, `couriers.py`): each handles its commands **and** its compensation
   commands idempotently. Random failure rates are set by flags.
3. **Failure paths:**
   - courier unavailable → refund the authorization + release the stock
   - the payment service sometimes **charges and then crashes** before replying → the orchestrator times out after 5 s;
     make sure the customer ends up not charged (or charged once if the saga completes)
   - a compensation fails the first two times → it's retried until it succeeds
4. **Crash the orchestrator** (`kill -9`) in the middle of 20 running sagas. Restart it. Every saga must reach a final state.
5. **`check.py` invariants:** for every order, either (completed, stock reserved, captured once, courier booked)
   or (rejected, no stock held, net charge = 0, no courier).

**Done when:**
- [ ] 100 orders with all failure flags on pass `check.py`
- [ ] the orchestrator survives `kill -9` and finishes every saga
- [ ] you can show one saga's history from the `sagas` table and explain each transition

## Questions (answer them in your own words, then send them to me)

1. What is the difference between a rollback and a compensation? Give an example of a step that can't be compensated.
2. Which step is your pivot, and why there? What would go wrong if "send confirmation email" came before it?
3. The payment service timed out. List what may have happened on its side, and how your design handles each case.
4. Why must saving the saga state and sending the next command happen in one transaction?
5. While a saga is running, another customer sees the item as "out of stock" even though the first order is later
   rejected. Is that a bug? Which countermeasure applies, and what's the business trade-off?
6. A compensation keeps failing for a day (the payment provider is down). What should your system do, and who should be told?
7. Would you implement this with choreography? What would be hardest?
8. When would you pick a workflow engine like Temporal instead of writing your own orchestrator?
