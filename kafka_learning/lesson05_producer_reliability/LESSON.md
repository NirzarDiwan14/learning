# Lesson 05: Producer Reliability & Performance

## The idea (read this first)

`producer.produce()` does **not** send anything. It only puts the message in a **local in-memory queue**.
A background thread (inside librdkafka, the C library under `confluent_kafka`) does the real work:

```
your code                     librdkafka background thread                      broker
─────────                     ────────────────────────────                      ──────
produce() ──► local queue ──► group into BATCHES per partition ──► compress ──► send ──► write to log
                 ▲               (wait up to linger.ms to fill)                            │
                 │                                                                         ▼
poll()/flush() ◄─┴──────────── delivery report (success or error) ◄──────────── ack (acks=?)
```

So three things are YOUR job:
1. **Call `poll()` regularly.** It runs your delivery callbacks and frees space in the queue.
2. **Call `flush()` before exiting.** Otherwise whatever is still in the queue dies with your process.
3. **Check the delivery callback.** That's the only place you learn whether a message really made it.

### The settings that matter

| Setting | What it does | Trade-off |
|---------|--------------|-----------|
| `acks=0` | don't wait for the broker at all | fastest; message can vanish silently |
| `acks=1` | leader wrote it | lost if the leader dies before followers copy it |
| `acks=all` | leader + all in-sync replicas wrote it | safest; slightly slower in a real cluster |
| `enable.idempotence=True` | broker drops duplicate retries (producer id + sequence number) | requires `acks=all`; no real downside, **use it** |
| `linger.ms` | wait this long to fill a batch (librdkafka default 5) | more latency, more throughput |
| `compression.type` | compress each batch: `gzip`, `lz4`, `zstd`, `snappy` | CPU vs. network + disk |
| `delivery.timeout.ms` | total time to keep retrying before reporting failure (default 300 s) | longer = survive longer outages, but slower to report errors |

**Why duplicates happen without idempotence:**
```
producer ── batch #7 ──► broker writes it ✅
producer ◄── ack ──✖ (network blip, ack lost)
producer ── retry batch #7 ──► broker writes it AGAIN ❌ duplicate
```
With idempotence, every batch carries `(producer id, sequence number)`. The broker sees #7 again and
ignores it. This also keeps **ordering** intact during retries.

**`acks=all` + `min.insync.replicas`** (needs a real cluster, so it can't be shown on our single broker):
with 3 replicas and `min.insync.replicas=2`, a write succeeds only when at least 2 copies exist.
If too many brokers are down, writes **fail loudly** instead of being silently under-replicated.
That combination is the standard "don't lose data" setup.

## Files

- `create_topics.py`: creates `reliability` and `bench-none/gzip/lz4/zstd` (3 partitions each)
- `count.py [topic]`: how many messages a topic holds
- `mistakes.py no-flush | no-poll`: the two classic producer bugs
- `benchmark.py`: sends 200,000 JSON events with the `acks` / `linger.ms` / compression you choose
- `sizes.py`: compares the bench topics' size **on disk per message**
- `reliable_producer.py`: production-style config; sends 1 msg / 0.5 s for 60 s while you break the broker

## Experiments

Run from `kafka_learning/lesson05_producer_reliability`. First: `uv run python create_topics.py`

### Experiment 1: the forgotten flush
```bash
uv run python count.py
uv run python mistakes.py no-flush
uv run python count.py
```
`produce()` was called 1000 times with no error. How many messages arrived? Why?

### Experiment 2: the forgotten poll
```bash
uv run python mistakes.py no-poll
uv run python count.py
```
When did `BufferError` happen, and how many delivery callbacks had run by then? Look at
`queue.buffering.max.messages` in the code. Then look at how `benchmark.py` handles `BufferError`.

### Experiment 3: acks
```bash
uv run python benchmark.py --acks 0
uv run python benchmark.py --acks 1
uv run python benchmark.py --acks all
```
Compare msg/s, and look at `last offset seen` for `acks=0`. Why is it `None`?
Is the speed difference as big as you expected? Why not, on **one local broker**?

### Experiment 4: batching
```bash
uv run python benchmark.py --linger 0
uv run python benchmark.py --linger 5
uv run python benchmark.py --linger 100
```
Which one is slowest, and why? Is a bigger `linger.ms` always faster?

### Experiment 5: compression
```bash
uv run python benchmark.py --compression none --linger 20
uv run python benchmark.py --compression gzip --linger 20
uv run python benchmark.py --compression lz4  --linger 20
uv run python benchmark.py --compression zstd --linger 20
uv run python sizes.py
```
Write down speed and bytes/msg for each. Which one would you pick? Bonus: rerun zstd with
`--linger 0` into a fresh topic and compare bytes/msg. Why is it worse?

### Experiment 6: kill the broker, part 1 (short outage)
Terminal 1: `uv run python reliable_producer.py`
Terminal 2, after a few messages: `docker stop kafka`, wait **~10 s**, then `docker start kafka`.

Watch terminal 1: what happens to `waiting in local buffer`? What are the delivery times after the restart?
Read the SUMMARY: was anything lost?

### Experiment 7: kill the broker, part 2 (long outage)
Same again, but wait **~40 s** before `docker start kafka`. (`delivery.timeout.ms` is 30 s.)
Which messages failed, and which were still delivered? What would happen to the failed ones in a real app
if nobody handled the ❌ callback?

### Experiment 8 (coding exercise): a reusable producer
In Lesson 08 you'll send events from a FastAPI app. Write `event_producer.py` with a class you can reuse:

```python
class EventProducer:
    def __init__(self, bootstrap_servers: str): ...
    def send(self, topic: str, key: str, event: BaseModel, headers: dict | None = None) -> None: ...
    def close(self, timeout: float = 10) -> int: ...   # returns how many messages were NOT delivered
```

Requirements:
- safe config: idempotence, `acks=all`, a little `linger.ms`, compression
- `send()` serializes a pydantic model (Lesson 04), never raises `BufferError` (handle it), and serves callbacks
- failed deliveries are **logged** with topic, key and error (use `logging`, not `print`)
- `close()` flushes and reports what's left
- a small `if __name__ == "__main__":` demo that sends a few `OrderEvent`s (copy `models.py` from lesson 04)

## Questions (answer these and send them to me, along with your Experiment 8 code)

1. `produce()` returned without an error. Is the message in Kafka? When *do* you know?
   ans = yes its a msg, i know it from delivery callbacks 
2. Explain `acks=0`, `acks=1` and `acks=all`. With `acks=1`, describe how a message can be lost even though
   the producer got a success.
   ans = acks 0 is not waiting for any acks from any partition, acks 1 will wait for learder partion's write ack, and acks all will send acks from all the partitions
3. How can a retry create a duplicate, and how does `enable.idempotence` prevent it?
   ans = when we retry, for kafka it is a new msg so it goes in queue,its timestamp and service id differs are the same so enable idempotence work that way (genuinely i dont its answer clearly right now) 
4. A payments API (each request waits for its event to be written) vs. a click-tracking pipeline (millions of
   events, nobody waits). What `linger.ms` and `acks` would you pick for each, and why?
   ans = for payment api, i will use acks, for click tracking, i will use linger ms, for it linger ms will be fine tuned according to my system, acks i will take to 0 
5. Which compression gave the smallest data in your runs, and which was fastest? Why does compression work
   better with a larger `linger.ms`?
   ans = zstd gave was fastest, also compression works when big data is there for it to compress and in larger linger ms it is there to it works fast
6. During the short outage, where were the messages? What would have happened to them if the producer
   process had been killed (`kill -9`) during the outage?
   ans = they will be there as it is persisted 
7. (Bonus) In a FastAPI app, should you create one `Producer` per request or one for the whole app? When
   would you call `flush()`?
   ans = one for whole app, also i would call flust after closing the app 
