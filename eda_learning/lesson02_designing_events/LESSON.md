# Lesson 02: Designing Events

**Goal:** know what to put *in* an event, how to name it, and how to wrap it, so other teams can rely on it for years.

## The idea (read this first)

In Lesson 01 the event was a Python dataclass inside one process. Once events cross service boundaries they become a
**public API**: other teams build on them, you can't see who reads them, and they live in logs for a long time.
Designing them well is the most important skill in EDA.

### 1. Four things people mean by "event-driven"

Martin Fowler's article *"What do you mean by Event-Driven?"* (2017) separates four patterns that often get mixed up:

| Pattern | What the event carries | The consumer… |
|---|---|---|
| **Event notification** | just "X happened" + an id | calls back to the source if it needs details |
| **Event-carried state transfer (ECST)** | all the data consumers need | keeps its own local copy and never calls back |
| **Event sourcing** | every state change, stored as the source of truth | n/a, it's a storage pattern (Lesson 07) |
| **CQRS** | n/a | separate read and write models (Lesson 08) |

This lesson is about the first two.

### 2. Thin vs fat events

```
thin (notification)                           fat (event-carried state transfer)
{ "type": "OrderPlaced",                      { "type": "OrderPlaced",
  "order_id": "o-42" }                          "order_id": "o-42",
                                                "customer": {"id": "c-7", "email": "a@x.com"},
     consumer ──GET /orders/o-42──► orders       "lines": [{"sku": "kb-1", "qty": 2, "price_cents": 4999}],
                                                "total_cents": 9998, "currency": "EUR" }
```

| | Thin | Fat |
|---|---|---|
| Size | tiny | bigger |
| Consumer needs the source to be up? | **yes**: temporal coupling is back | no |
| Load on the source | one callback per consumer per event | none |
| Data freshness | the callback returns the *current* state, which may already differ from the moment of the event (**race**) | exactly the state *at the time of the event* |
| Coupling to the data shape | low | consumers depend on fields, so changing them is harder (Lesson 09) |
| Privacy | little personal data spread around | PII copied into every consumer and every log (GDPR: you may have to delete it) |

Neither is always right. A common middle ground is to **carry what the event is about** (the facts that changed, plus
the ids and fields consumers usually need), and nothing else.

### 3. Domain events vs integration events

- **Domain event:** internal to one service, fine-grained, may change freely (`CartLineAdded`).
- **Integration event:** published to other services, a stable contract, usually coarser (`OrderPlaced`).

Don't publish your internal events just because you have them. Every published field is a promise.

### 4. Naming

- **Past tense, business language:** `OrderPlaced`, `PaymentDeclined`, `ParcelDelivered`. Not `CreateOrder`
  (a command) and not `OrderEvent` (says nothing).
- **Avoid CRUD events** like `OrderUpdated`: the consumer has to diff the data to guess what happened. Say *why* it
  changed: `ShippingAddressChanged`, `OrderCancelled`, `DiscountApplied`.
- Name events after what happened **in the domain**, not after what you want someone else to do
  (`CustomerRegistered`, not `SendWelcomeEmail`).

### 5. The envelope: metadata vs data

Every event has two parts: **metadata** (the same fields for every event, used for routing, dedupe and tracing) and
**data** (the business payload). [CloudEvents](https://cloudevents.io) is the CNCF standard for the envelope:

```json
{
  "specversion": "1.0",
  "id": "0192f1a4-6b1e-7c3a-9d2b-5f0c8e7a1b23",
  "source": "/services/orders",
  "type": "com.myshop.order.placed.v1",
  "subject": "o-42",
  "time": "2026-10-27T10:15:00Z",
  "datacontenttype": "application/json",
  "correlationid": "req-9f1c",
  "causationid": "0192f1a4-0000-7000-8000-000000000001",
  "data": { "order_id": "o-42", "customer_id": "c-7", "total_cents": 9998, "currency": "EUR" }
}
```

| Field | Why it's there |
|---|---|
| `id` | unique per event. Consumers **dedupe** on it (Lesson 03) |
| `source` + `type` | who produced it and what it means. Consumers route on `type` |
| `subject` | the entity it's about. A natural Kafka **key** (ordering per entity) |
| `time` | when it **happened** (not when it was sent or received) |
| `correlationid`, `causationid` | CloudEvents *extensions*, not core fields. They trace a whole flow (Lesson 10) |
| version in `type` | lets the schema evolve (Lesson 09) |

Tip: Python 3.14 has `uuid.uuid7()`. Its ids are time-ordered, which makes them nicer as database keys than `uuid4()`.

In Kafka you can put the envelope in the message value (structured mode) or the metadata in **headers** and the data
in the value (binary mode). You did the second in Kafka Lesson 04 with the `event_type` header.

## Practical

> **Note:** the spec below is the plan. When you start this lesson, Claude writes and tests the code and replaces
> this section with **Files** + **Experiments** for you to run (like Lesson 01).

Work in this folder. Standard library only (you may use `pydantic`, which comes with FastAPI).

**Domain:** a food-delivery app with restaurants, customers and couriers.

1. **Design on paper first.** In `DESIGN.md`, list 8–10 **integration events** for the flow from "customer places an
   order" to "food delivered", including at least two failure paths (restaurant rejects, no courier available).
   For each event: name, producer, likely consumers, and the fields in `data`. Mark each event **thin** or **fat**
   and give a one-line reason.
2. **`envelope.py`:** a function `make_event(type_, source, subject, data, correlation_id=None, causation_id=None)`
   that returns a CloudEvents-style dict (ids from `uuid.uuid7()`, time in UTC ISO 8601), plus `to_json` / `from_json`.
   `from_json` must reject an event that is missing a required attribute.
3. **`thin_vs_fat.py`:** simulate the race.
   - A fake `OrderService` holds orders in a dict and has `get_order(id)` that sleeps 0.3 s (a network call).
   - Publish `OrderPlaced` thin and fat versions to two consumers through Lesson 01's `AsyncEventBus`.
   - **Right after** publishing, change the order's delivery address (`ShippingAddressChanged`).
   - Each consumer prints the address it will use for the delivery label.
4. Make `get_order` raise (service down) and run again.

**Done when:**
- [ ] `DESIGN.md` has the event list with at least one event you *deliberately* kept thin, with the reason
- [ ] `from_json` rejects a broken event with a clear error
- [ ] `thin_vs_fat.py` shows the thin consumer printing the *new* address and the fat consumer the *old* one, and you can explain which is "correct" for a delivery label (it depends!)
- [ ] with `get_order` down, you can say which consumer still works

## Questions (answer them in your own words, then send them to me)

1. In step 3, the two consumers printed different addresses. Which one is right? Give one business case where the
   thin consumer's behavior is what you want and one where the fat consumer's is.
2. A teammate proposes one event `OrderUpdated` with the full order every time anything changes. Give two concrete
   problems for consumers, and what you'd publish instead.
3. Why should `time` be when the event *happened* and not when it was *sent*? Describe a bug that the wrong choice causes.
4. Your `OrderPlaced` contains the customer's email and phone number. Name two risks, and one way to reduce each.
5. Which envelope field would you use as the Kafka message key, and why? What goes wrong if you use `id` instead?
6. What's the difference between a domain event and an integration event? Why not publish every domain event?
7. When is a thin event clearly the better choice? (Hint: think about huge payloads, or data the consumer isn't allowed to see.)
