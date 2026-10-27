# Lesson 09: Contracts and Evolution

**Goal:** change event formats over months and years **without breaking consumers you don't control**.

## The idea (read this first)

### 1. Events are an API with invisible clients

A REST API at least gets requests that tell you who uses it. An event is read by consumers you may never hear of,
and old events stay in topics, DLQs and event stores long after you change the code. So:
- every change must be checked against **old data** and against **old consumers**
- "rename a field" is never a small change

### 2. Compatibility

| Kind | Meaning | Lets you |
|---|---|---|
| **Backward** | **new** consumers can read **old** events | upgrade consumers first, replay history |
| **Forward** | **old** consumers can read **new** events | upgrade producers first |
| **Full** | both | upgrade in any order |

| Change | Backward? | Forward? |
|---|---|---|
| add an **optional** field with a default | ✅ | ✅ (if old readers ignore unknown fields) |
| add a **required** field | ❌ old events lack it | ✅ |
| remove a field | ✅ | ❌ old readers expect it |
| rename a field | ❌ | ❌ |
| change a type (`"42"` → `42`) | ❌ | ❌ |
| change the **meaning** (`amount` was euros, now cents) | ✅ parses fine, **silently wrong** | 💀 the worst kind |

### 3. Tolerant reader

Consumers should be forgiving (Postel's law): ignore unknown fields, use defaults for missing optional ones, read only
the fields they need. This alone makes most additive changes safe.

### 4. Strategies for breaking changes

1. **Don't break it:** add a new field next to the old one, fill both for a while, deprecate the old one later.
2. **New event version:** `order.placed.v2` alongside `v1`. The producer **dual-publishes** during the migration, and
   consumers move over one by one. Then you stop `v1`.
3. **Upcasting:** when reading, convert old events to the newest shape **in one place** before any code sees them
   (`v1 → v2 → v3`). Essential for event sourcing, where old events can't be changed.
4. **New topic:** for big redesigns, publish to `orders.v2` and migrate consumers.
5. **Copy-and-transform the stream:** rewrite history into a new stream (rare, heavy, sometimes needed).

### 5. Tools

- **Schema Registry** (Confluent, Apicurio): stores schemas (Avro/Protobuf/JSON Schema) and **rejects incompatible
  changes when the producer registers them**, before any consumer sees bad data.
- **AsyncAPI** (v3): like OpenAPI, but for events. Documents channels, messages and schemas, and generates docs and code.
- **Consumer-driven contract tests** (e.g. Pact message contracts): each consumer states which fields it relies on, and the
  producer's CI checks it still provides them.
- An **event catalog**: one place to see which events exist, who owns them, who consumes them.

### 6. Process matters as much as tools

Owner per event, a deprecation policy ("v1 supported for 3 months after v2"), monitoring of which versions are still
consumed, and a changelog.

## Practical

> **Note:** the spec below is the plan. When you start this lesson, Claude writes and tests the code and replaces
> this section with **Files** + **Experiments** for you to run (like Lesson 01).

Standard library + `pydantic`. Work in this folder.

Current event, `order.placed.v1`:
```json
{"order_id": "o-1", "customer_name": "Alice Smith", "amount": 49.99, "items": ["kb-1", "ms-2"]}
```
New requirements: money must be integer cents **with a currency**, the name must be split into first/last,
and items need quantities.

1. **`v2_design.md`:** design `order.placed.v2`. For each change, state whether it's backward/forward compatible, and
   your migration plan (who upgrades first, how long `v1` lives).
2. **`schemas.py`:** pydantic models for v1 and v2, and **`upcast.py`** with `upcast(event_dict) -> OrderPlacedV2`
   handling v1 (assume EUR, quantity 1, split the name on the last space, and decide what to do with a single-word name).
3. **`consumers.py`:** an `old_consumer` that only understands v1 and a `new_consumer` that uses the upcaster.
   Make `old_consumer` a tolerant reader.
4. **`compat_test.py`:** a test matrix: {v1 events, v2 events, v2 events with an extra unknown field} × {old, new consumer}.
   Print which combinations work and explain each failure.
5. **The semantic trap:** create a "v1.1" where a producer started sending `amount` in cents *without* changing the
   version. Show that every test still passes, and write down how you would have caught it.
6. **`asyncapi.yaml`:** document the shop's channels and the v2 message with AsyncAPI 3.0.

**Done when:**
- [ ] the compatibility matrix prints, and you can explain every ❌
- [ ] the upcaster handles all v1 edge cases you could think of (list them)
- [ ] you have an answer for the semantic trap
- [ ] `asyncapi.yaml` validates (e.g. with the AsyncAPI CLI or studio.asyncapi.com)

## Questions (answer them in your own words, then send them to me)

1. Explain backward vs forward compatibility in your own words. Which one does **replaying** old events need?
2. You must rename `customer_name` to `full_name`. Describe a zero-downtime migration step by step.
3. Why is upcasting essential for event sourcing, but optional for a Kafka topic with 7 days of retention?
4. Why is a change in **meaning** more dangerous than a change in **shape**? How can you guard against it?
5. What does a Schema Registry protect you from, and what can it **not** catch?
6. What's a tolerant reader? Give one way being *too* tolerant can hide a real bug.
7. Who should own an event's schema: the producer, the consumers, or a central team? Why?
