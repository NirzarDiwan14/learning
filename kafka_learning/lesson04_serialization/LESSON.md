# Lesson 04: Serialization, Headers & Schemas

## The idea (read this first)

**Kafka only stores bytes.** It never looks inside your key or value. Turning objects into bytes
(**serialize**) and back (**deserialize**) is the job of your producer and consumer:

```
producer:  OrderEvent ──model_dump_json()──► JSON str ──.encode()──► bytes ──► Kafka
consumer:  Kafka ──► bytes ──model_validate_json()──► OrderEvent (validated!)
```

That means the producer and consumer must **agree on the format**. This agreement is the **schema** (or contract).
In this lesson the contract is the pydantic model in `models.py`.

A Kafka message has more than a key and a value:

| Part | What it's for |
|------|---------------|
| **key** | partitioning and ordering (Lesson 02) |
| **value** | the payload: your serialized event |
| **headers** | metadata: event type, schema version, trace id… You can read them without parsing the value |
| **timestamp** | when it was produced (ms since epoch), set automatically |

### Schema evolution: the real problem
Producers and consumers are deployed **separately**, often by different teams. So for a while,
an **old consumer** will read messages from a **new producer**:

- **Adding** an optional field is usually safe (*compatible*): old consumers ignore it.
- **Removing / renaming** a field the consumer needs, or **changing its type**, breaks consumers (*breaking change*).

And remember Lesson 03: a consumer that crashes on a message **doesn't commit it**, so after a restart
it reads the **same message again** and crashes again, forever. That message is a **poison pill**.

### Two small things the code also teaches
- **Money as integer cents** (`amount_cents: int`), never `float`. Floats can't represent 0.1 exactly.
- `consumer.py` uses the **store_offsets** pattern (your Lesson 03, Q7):
  `enable.auto.offset.store=False` + `consumer.store_offsets(msg)` after processing.

## Files

- `models.py`: the `OrderEvent` contract (pydantic)
- `create_topic.py`: creates `order-events` (3 partitions)
- `producer.py`: sends created/paid/shipped events for 3 orders, key = `order_id`, with headers
- `consumer.py`: validates every message into an `OrderEvent` and prints value, headers and timestamp
- `producer_v2.py`: another team's producer that adds a field (or, with `breaking`, renames one)
- `bad_producer.py`: sends bytes that aren't valid JSON

## Experiments

Run from `kafka_learning/lesson04_serialization`.

### Experiment 1: structured events
```bash
uv run python create_topic.py
uv run python producer.py
uv run python consumer.py          # Ctrl+C when idle
```
- Look at the order of events **within** `order-101`. Then look at the order **across** orders. (Lesson 02!)
- How many partitions actually got data? Why? (Lesson 02, Q1 again.)

### Experiment 2: look at the raw bytes
Open Kafka UI at http://localhost:8080 → Topics → `order-events` → Messages. Find the key, value, headers and timestamp.
Or use the CLI:
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic order-events --from-beginning --property print.key=true --property print.headers=true --timeout-ms 5000
```
Kafka doesn't know this is JSON. To Kafka it's only bytes.

### Experiment 3: a compatible change
```bash
uv run python producer_v2.py       # adds "currency"
uv run python consumer.py
```
The old consumer gets an extra field it doesn't know about. What happens? Why?
(Look up pydantic's default for `extra` fields.)

### Experiment 4: a breaking change
```bash
uv run python producer_v2.py breaking   # amount_cents -> amount
uv run python consumer.py
uv run python consumer.py               # and again...
```
Then check lag:
```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-events-processor
```
Now run `uv run python producer.py` to send new **good** orders. Does the consumer ever process them?

### Experiment 5 (coding exercise): survive the poison pill
**You write this one.** Change `consumer.py` so a message that fails to parse:
1. does **not** crash the consumer,
2. gets logged clearly: partition, offset, the raw value, and the error,
3. is **skipped** so the good messages behind it get processed.

Hints: `from pydantic import ValidationError`, try/except around `model_validate_json`,
and think about whether you call `store_offsets` for the bad message.

Test it with:
```bash
uv run python bad_producer.py
uv run python producer.py
uv run python consumer.py
```

## Experiment 5: review of your solution

Your `consumer.py` catches `ValidationError`, logs partition / offset / raw value / error, and keeps going. 👍
pydantic also raises `ValidationError` for **invalid JSON**, so one `except` covers both the breaking v2 event
and `bad_producer.py`.

**One bug: not calling `store_offsets` for the bad message.**
A committed offset is a **position** ("next message to read"), not a per-message "ack". So:
- If a good message comes after the bad one in the same partition, its `store_offsets` moves the position
  past the bad one anyway, and skipping `store_offsets` changed nothing.
- If the bad message is the **last one** in its partition, the position stays on it. **Every restart reads it
  and logs the poison pill again.** (Tested: bad message alone in the topic, two runs, logged twice.)

Fix: you *handled* it (logged and decided to skip it), so mark it as done:

```python
        except ValidationError as e:
            print(f"[POISON PILL] partition={msg.partition()} offset={msg.offset()} raw={msg.value()!r} error={e}")
            consumer.store_offsets(message=msg)  # handled = skipped on purpose; move past it
```

Two smaller robustness notes for real systems:
- `msg.value()` can be `None` (a "tombstone" message). Decide what that means for you.
- Header decoding is outside the `try`. A header with a `None` value or non-UTF-8 bytes would still crash
  the consumer. Real code parses *everything* from the message inside the protected block.

## Questions & Answers (for revision)

**1. Kafka doesn't care about the format. So where does it live, and who must agree on it?**

The format lives **in the producer's and consumer's code**: the serializer and deserializer (here: `models.py`).
**The broker never looks inside your bytes.** It doesn't validate, parse or reject anything.
The producer and *every* consumer must agree on it. In bigger companies the contract is stored centrally in a
**Schema Registry** (Avro / Protobuf / JSON Schema), which checks compatibility before a producer can use a new
version. Even then it's the *clients* that use it, never the broker.
⚠️ Not "the format lives on Kafka": Kafka only stores bytes.

**2. Why did the old consumer survive the new `currency` field? Which pydantic setting would make it crash?**

pydantic's default is `extra="ignore"`: unknown fields are silently dropped. With
`model_config = ConfigDict(extra="forbid")` it raises a `ValidationError` instead.
Ignoring unknown fields is called being a **tolerant reader**, and it's what makes "adding a field" a compatible change.

**3. Why does the consumer crash on every restart, and why are good orders behind it never processed?**

1. `poll()` returns the bad message, and `model_validate_json` raises.
2. The exception escapes the loop, so `store_offsets` is never called for it. `close()` commits only the offsets
   stored *before* it, so the committed position = the bad message's offset.
3. Restart: resume from the committed position, the same bad message, crash again. Forever: a **poison pill**.
4. Messages behind it in the same partition are never reached, and since the whole process dies, the
   consumer's *other* partitions stop too. Lag grows, and nothing gets processed.

**4. `event_type` in a header vs only inside the JSON value?**

Headers can be read **without deserializing the value**. Good uses:
- **Filtering / routing**: a consumer that only cares about `order_shipped` can skip others cheaply.
- Telling *how* to parse the value: `content_type`, `schema_version` pick the right deserializer *before* parsing.
- Cross-cutting metadata: trace / correlation ids, source service, retry count, error info (Lesson 06's DLQ uses this).

⚠️ Even when you skip a message based on its header, you still **store its offset**. Skipped means handled,
so the position must move past it.

**5. Rename `user_id` → `customer_id` without breaking other teams?**

Your answer is the right pattern: **expand → migrate → contract** (also called "parallel change"):
1. **Expand:** send **both** `user_id` and `customer_id` (same value). Bump `schema_version`.
2. **Migrate:** consumers switch to `customer_id`, falling back to `user_id` if it's missing.
3. **Contract:** only after *all* consumers have moved, stop sending `user_id`.

Two extra traps:
- **Old messages stay in the topic** until retention deletes them. A consumer that replays history still sees
  messages with only `user_id`, so keep the fallback while those exist.
- You often **don't know every consumer**. That's why companies use a Schema Registry with compatibility rules,
  or publish breaking changes to a **new topic** (`order-events-v2`).

**6. What's lost by skipping a bad message? What's better?**

You lose the **data**: an order is never processed, and nobody notices except a log line.
⚠️ **Re-sending it to the same topic doesn't help.** It fails again, forever, and wastes the consumer's time.
Better: separate the two kinds of failure:
- **Permanent errors** (bad format, missing field): send the original bytes plus error details (in headers) to a
  **Dead Letter Topic** (DLQ), alert, fix the bug, and replay from the DLQ.
- **Transient errors** (DB down, API timeout): retry with backoff, possibly via a separate retry topic.

That's Lesson 06.

**7. (Bonus) Why `amount_cents: int` instead of `amount: float`?**

Floats are binary fractions and can't represent most decimals exactly: `0.1 + 0.2 == 0.30000000000000004`.
Errors pile up in sums, `==` comparisons fail, and rounding differs between languages, which is a problem when
invoices must match to the paisa. Use **integer minor units** (cents / paise), or `Decimal` sent as a **string**.
Also send the **currency**: not every currency has 2 decimals (JPY has 0, KWD has 3).
