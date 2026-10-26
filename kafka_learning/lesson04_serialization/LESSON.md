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

## Questions (answer these and send them to me, along with your Experiment 5 code)

1. Kafka doesn't care whether you send JSON, plain text or images. So where does the "format" actually live,
   and who has to agree on it?
2. In Experiment 3, why did the old consumer survive the new `currency` field? Name a setting in pydantic
   that would have made it **crash** instead.
3. In Experiment 4, explain step by step (using what you learned about commits in Lesson 03) why the consumer
   crashes on every restart, and why the good orders sent after it never get processed.
4. What's the difference between putting `event_type` in a **header** vs. only inside the JSON **value**?
   Give one situation where the header is more useful.
5. Your teammate wants to rename `user_id` to `customer_id`. Other teams' consumers read this topic.
   How would you roll it out **without breaking anyone**?
6. In Experiment 5 you **skip** the bad message. What's lost by skipping? What would be better in production?
   (This is the topic of Lesson 06.)
7. (Bonus) Why `amount_cents: int` instead of `amount: float`? Try `0.1 + 0.2` in Python.
