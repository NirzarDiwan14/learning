# Lesson 05: Choreography vs Orchestration

**Goal:** when a business process spans several services, decide **who knows what happens next**.

## The idea (read this first)

An order needs: payment → stock reservation → shipping. Two ways to coordinate that:

### Choreography: everyone reacts to events

```
 orders ──OrderPlaced──► payments ──PaymentSucceeded──► inventory ──StockReserved──► shipping ──OrderShipped──► orders
                           │                               │
                           └──PaymentFailed──► orders      └──OutOfStock──► payments (refund) ──► orders
```

No service is in charge. Each one listens for events it cares about, does its job, and publishes what happened.
Like dancers who know their own steps.

### Orchestration: one coordinator gives commands

```
                         ┌──── ChargePayment ────► payments ──── PaymentSucceeded ───┐
                         │                                                           │
 OrderPlaced ──► order orchestrator ◄─────────────────────────────────────────────────┘
  (holds the workflow    ├──── ReserveStock ─────► inventory ─── StockReserved ──────► (back to orchestrator)
   state for each order) └──── ScheduleShipping ──► shipping ─── ShipmentScheduled ──► (back to orchestrator)
```

One component (an **orchestrator** or **process manager**) holds the state of each order's workflow, sends
**commands** to participants, and decides the next step from their replies. Like a conductor.

### Trade-offs

| | Choreography | Orchestration |
|---|---|---|
| Coupling | services only know events | the orchestrator knows every participant |
| Where is the process? | **spread across services**: nobody can show you the whole flow | **in one place**, readable top to bottom |
| Changing the process (add a fraud check) | change several services, carefully | change the orchestrator |
| "Where is order 42 stuck?" | hard: dig through the logs of every service | easy: the orchestrator has its state |
| Timeouts ("payment never answered") | who owns them? Usually nobody | natural: the orchestrator sets a deadline |
| Single point of failure / bottleneck | no | the orchestrator (mitigated by making it stateless + a DB, and scaling it) |
| Risk | the **pinball machine**: events bouncing around, cycles, nobody understands the whole | the **god service**: all logic in the orchestrator, participants become dumb CRUD |

### Rules of thumb

- **Choreography** fits simple flows, notifications, and reactions across team boundaries
  ("when an order is placed, analytics and email do their thing"). Nobody needs to wait for them.
- **Orchestration** fits business-critical, multi-step processes with failure paths, deadlines and compensations
  (Lesson 06). Workflow engines like **Temporal**, **AWS Step Functions** or **Camunda** are orchestrators as a product.
- **Mix them:** orchestrate the core order process and publish events from it that others choreograph around.
- Keep business rules in the participants (payments decides *how* to charge). The orchestrator decides only *the order of steps*.

### Commands over messaging

The orchestrator sends **commands** (`ChargePayment`) to a channel owned by the receiving service, and waits for an
**event/reply** (`PaymentSucceeded` / `PaymentFailed`). Each command carries the `order_id` (and a
correlation id) so the reply can be matched to the right workflow instance.

## Practical

> **Note:** the spec below is the plan. When you start this lesson, Claude writes and tests the code and replaces
> this section with **Files** + **Experiments** for you to run (like Lesson 01).

You can use Lesson 01's `AsyncEventBus` (focus on design) or Kafka (more realistic). Work in this folder.

Flow: **order placed → charge payment → reserve stock → schedule shipping → order completed.**
Failure paths: payment declined (order rejected), out of stock (refund payment, order rejected).

1. **`choreography/`:** four services (orders, payments, inventory, shipping), each only subscribing to events and
   publishing new ones. Make payment decline for some customers and stock run out for some items.
2. **`orchestration/`:** the same participants, but they now handle **commands** and reply with events. An
   `OrderOrchestrator` keeps a state per order (`AWAITING_PAYMENT`, `AWAITING_STOCK`, …) and drives the flow.
3. **Change request:** product wants a **fraud check** before payment (`FraudCheckPassed` / `FraudSuspected`).
   Implement it in both versions. Keep a note of every file you change in each.
4. **Status query:** in both versions, write `status(order_id)` that tells you where an order is right now.
5. Run 20 orders through each with random failures, and print a final table: order, outcome, path taken.

**Done when:**
- [ ] both versions produce the same outcomes for the same seed
- [ ] you have the "files changed" list for the fraud-check change in each version
- [ ] you can show where `status()` gets its answer in each version, and which one was harder to write

## Questions (answer them in your own words, then send them to me)

1. For the fraud-check change, which version needed more changes, and in how many services? What does that tell you?
2. In the choreography version, how did you implement `status(order_id)`? What does that imply for debugging in production?
3. Payment never answers (the service is down for an hour). What happens in each version? Who notices?
4. Is the orchestrator a "single point of failure"? How would you make it reliable?
5. Draw (ASCII is fine) one process from a real system you know, and say which style you'd use and why.
6. What is the "god service" anti-pattern, and how do you avoid it while still using orchestration?
7. Can the email service in an orchestrated system still be choreographed? Why might that be a good idea?
