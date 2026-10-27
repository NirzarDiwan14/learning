# Lesson 10: Observability: Following an Event Through the System

**Goal:** answer "what happened to order 42?" in one minute instead of one afternoon.

## The idea (read this first)

### 1. The problem

In a monolith, an error gives you a stack trace that shows the whole path. In an event-driven system, one business
action becomes many events, processed by many services, at different times, possibly hours apart. There's no stack,
and the logs are spread over many services. Without planning, debugging means grepping five services' logs for an
order id and lining up timestamps by hand.

### 2. Correlation and causation IDs

Two ids in every event's envelope (Lesson 02):

```
HTTP POST /orders  (correlation = C1)
 └─ OrderPlaced          id=E1  correlation=C1  causation=(the request)
     ├─ PaymentAuthorized    id=E2  correlation=C1  causation=E1
     │   └─ StockReserved        id=E4  correlation=C1  causation=E2
     │       └─ CourierBooked        id=E5  correlation=C1  causation=E4
     └─ ConfirmationEmailSent  id=E3  correlation=C1  causation=E1
```

- **correlation id:** the same for **everything** caused by one original action. "Show me the whole story."
- **causation id:** the id of the **direct parent** message. "What caused this one?" Together they let you rebuild the tree.
- **The rule:** every handler copies the incoming `correlation_id` and sets `causation_id = incoming.id` on
  everything it emits. Do it **once**, in your bus/producer wrapper, never by hand in each handler.

### 3. Structured logging

Log JSON lines with fixed fields, so a log tool can filter them:
```json
{"ts": "...", "service": "payments", "level": "info", "msg": "payment authorized",
 "event_id": "E2", "event_type": "PaymentAuthorized", "correlation_id": "C1", "causation_id": "E1", "order_id": "o-42"}
```
Put the ids into the logging context automatically when a handler starts (Python: `contextvars`).

### 4. Distributed tracing

**OpenTelemetry** standardizes this. The producer injects the trace context (the W3C `traceparent` header) into the
message headers. The consumer extracts it and starts its span as a child (or, for batches, with **span links**).
Tools like Jaeger, Tempo or Honeycomb then draw the whole flow as a timeline across services, including how long each
event **waited** in the broker.

### 5. Metrics that matter in EDA

| Metric | Tells you |
|---|---|
| **consumer lag** (per group) | a consumer is slow or stuck (Kafka course) |
| **end-to-end latency** = now − event `time` | how stale downstream data is: what users actually feel |
| processing time and error rate per handler | which step is the bottleneck or broken |
| **DLQ size / rate** | events nobody will process unless a human acts |
| **sagas past their deadline** | workflows that are silently stuck |
| events published per type | sudden drops = an upstream service stopped emitting (lag won't show that!) |

### 6. Documenting the flows

An **event catalog** (which events exist, who produces and who consumes each, with schemas) and generated flow
diagrams. Without them, nobody knows what breaks if an event changes (Lesson 09).

## Practical

> **Note:** the spec below is the plan. When you start this lesson, Claude writes and tests the code and replaces
> this section with **Files** + **Experiments** for you to run (like Lesson 01).

Build on your Lesson 06 saga (or Lesson 05 if you prefer). Work in this folder.

1. **Propagation:** change your producer/bus wrapper so `correlation_id` / `causation_id` are filled in
   **automatically** from the message being handled (use `contextvars`). No handler should set them by hand.
2. **Structured logs:** every service writes JSON lines to `logs/<service>.jsonl` with the fields from section 3.
3. **`trace.py <correlation_id>`:** reads all the log files and prints the causation **tree** with timestamps and the
   time spent between steps, like the diagram in section 2.
4. **`latency.py`:** run 200 orders, then print p50/p95/p99 of end-to-end latency (order placed → final state)
   and of each step.
5. **Find the bug:** add a hidden 1-in-20 delay of 3 s in one participant, without telling yourself which (have a
   script pick it at random). Use only your tools to find which service and which step.
6. **Optional:** add OpenTelemetry with Jaeger in Docker and see the same trace as a timeline.

**Done when:**
- [ ] `trace.py` prints a correct tree for any order, including failed and compensated ones
- [ ] you found the hidden delay using only `trace.py` / `latency.py`
- [ ] no handler code sets correlation or causation ids by hand

## Questions (answer them in your own words, then send them to me)

1. What's the difference between a correlation id and a causation id? Why do you need both?
2. Where should the correlation id be created, and what should happen when a request comes in that already has one?
3. Why should propagation live in the bus/producer wrapper and not in each handler?
4. Consumer lag is 0 everywhere, but customers say confirmations stopped arriving. Which metric would have caught it?
5. Why is end-to-end latency (now − event `time`) a better user-facing metric than per-service processing time?
6. A consumer processes events in batches of 100. Each came from a different request. How should tracing represent that?
7. What three dashboards/alerts would you set up first for the saga system, and why those?
