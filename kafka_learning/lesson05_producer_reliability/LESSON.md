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

## Experiment 8: review of your solution (`event_producer.py`)

You went further than asked and wired it into FastAPI with `lifespan`. Well done. The good parts:
safe config (idempotence + `acks=all`), `BufferError` handled, **one producer for the whole app**, `close()` flushes on
shutdown and reports what's left, and `202 Accepted` is the honest status code ("queued", not "done").

Issues, most important first:

1. **Delivery callbacks only run when the *next* request comes in.** Callbacks run only inside `poll()`/`flush()`.
   `send()` calls `poll(0)` right after `produce()`, but the message hasn't been delivered yet at that moment.
   Tested: two `POST /orders`, and both "Delivered" lines were logged **at shutdown**, seconds later. In a quiet
   app, a failed delivery could stay unlogged for hours.
   **Fix:** a background thread that calls `poll(0.1)` in a loop (see the reference below).
2. **`amount: float`**, which you yourself explained is wrong in Lesson 04, Q7. Use `amount_cents: int`.
3. **A different schema on the same topic.** You publish `{order_id, customer_id, amount, status}` to `order-events`,
   but Lesson 04's consumer expects `OrderEvent` (`event_id`, `event_type`, `user_id`, `amount_cents` …).
   Every message from this API would be a **poison pill** for that consumer. One topic = one contract.
4. **The API request model is used as the event.** Keep them separate: the API receives an `OrderCreateRequest`, and
   the server builds the event and adds `event_id`, `event_type`, `created_at`. Clients shouldn't control those,
   and the API and the event can then change independently.
5. **`send()` can block forever.** If the broker is down long enough for the queue to fill, the `BufferError` loop
   never ends and every request thread hangs. Give it a deadline and raise, and the endpoint turns that into a `503`.
6. Small things: logging every delivery at INFO is too noisy at high volume (use DEBUG). `_failed_deliveries` is
   counted but never used. `event_type: "order"` is vague: use `order_created` plus a `schema_version` header.

**Reference implementation** (tested):

```python
class EventProducer:
    def __init__(self, bootstrap_servers: str, max_block_s: float = 5.0):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 10,
            "compression.type": "zstd",
        })
        self._max_block_s = max_block_s
        # Callbacks only run inside poll()/flush(): keep them running in the background
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._poll_loop, daemon=True)
        self._poller.start()

    def _poll_loop(self):
        while not self._stop.is_set():
            self._producer.poll(0.1)

    @staticmethod
    def _on_delivery(err, msg):
        if err is not None:
            logger.error("delivery failed topic=%s key=%s error=%s", msg.topic(), msg.key(), err)
        else:
            logger.debug("delivered topic=%s p=%s offset=%s", msg.topic(), msg.partition(), msg.offset())

    def send(self, topic: str, key: str, event: BaseModel, headers: dict | None = None) -> None:
        deadline = time.monotonic() + self._max_block_s
        while True:
            try:
                self._producer.produce(topic, key=key, value=event.model_dump_json().encode(),
                                       headers=headers, on_delivery=self._on_delivery)
                return
            except BufferError:
                if time.monotonic() > deadline:
                    raise  # the caller turns this into a 503
                time.sleep(0.05)

    def close(self, timeout: float = 10) -> int:
        self._stop.set()
        self._poller.join()
        remaining = self._producer.flush(timeout)
        if remaining:
            logger.error("%d messages NOT delivered at shutdown", remaining)
        return remaining
```

## Questions & Answers (for revision)

**1. `produce()` returned without error. Is the message in Kafka? When do you know?**

**No.** It's only in the producer's **local in-memory queue**. It's in Kafka when the **delivery callback reports
success**, and callbacks only run inside `poll()` or `flush()`. Until then it can still fail (broker down,
timeout, too large …) or vanish (process killed, no `flush()`, as in Experiment 1: 1000 produced, 0 arrived).
⚠️ "produce() returned" ≠ "message in Kafka".

**2. `acks=0` / `1` / `all`, and how `acks=1` can lose a "successful" message.**

⚠️ acks are about **replicas** (copies of *one* partition on different brokers), not about partitions.
Each partition has one **leader** replica and some **follower** replicas. The in-sync ones form the **ISR**.
- `acks=0`: don't wait for anything. No error reports, no offsets (that's why `last offset seen=None`).
- `acks=1`: the **leader** wrote it to its log.
- `acks=all`: the leader **and every in-sync follower** have it.

Loss with `acks=1`: the leader writes the message and acks, and the producer sees success. The leader crashes **before
followers copied it**. A follower becomes the new leader **without that message**. It's gone, though the producer was told "success".
`acks=all` + `min.insync.replicas=2` prevents this.

**3. How can a retry create a duplicate, and how does idempotence prevent it?**

```
producer ── batch ──► broker writes it ✅
producer ◄── ack ──✖  (ack lost on the network / timed out)
producer ── retry SAME batch ──► broker writes it AGAIN ❌
```
The broker can't tell a retry from a new message: same bytes, nothing marks it as a resend.
With `enable.idempotence=True`, the producer gets a **producer id (PID)** from the broker and numbers every
message per partition with a **sequence number**. The broker remembers the last sequence per (PID, partition):
- same sequence again: it's a duplicate, so the broker **acks it but doesn't write it**
- a gap in sequences: error, so messages are never written out of order

⚠️ It's not based on timestamps or service ids. It's PID + sequence number.

**4. Payments API vs click tracking: `linger.ms` and `acks`?**

| | Payments API | Click tracking |
|---|---|---|
| `acks` | `all` + idempotence: losing a payment is unacceptable | `1` (or `all`): losing a few clicks is OK |
| `linger.ms` | small (0–5): a user is waiting, latency matters | larger (20–100) + compression: throughput and cost matter |

⚠️ About `acks=0`: you get **no errors at all**, so you'd never notice if Kafka were down for an hour. It's rarely
worth it. `acks=1` is already cheap. Both apps need *both* settings; it's not "acks for one, linger for the other".

**5. Which compression was smallest and fastest? Why does a larger `linger.ms` help?**

Typical results (my test run): **zstd smallest** (~10 bytes/msg vs ~199 uncompressed: about 20×), gzip ~16, lz4 ~31.
Speed differences were small, because the Python loop is the bottleneck here, not compression. In real systems, lz4/snappy
are the fastest and zstd gives the best ratio at good speed.

Why linger helps: **compression is applied per batch**. JSON events repeat the same keys (`"event_type"`,
`"order_id"` …), and the compressor can only exploit repetition *within* a batch. `linger.ms=0` gives tiny batches,
so there's little repetition and each batch has fixed overhead. Bigger batches compress much better.

**6. Short outage: where were the messages? What if the producer was `kill -9`'d?**

⚠️ **They were NOT persisted anywhere.** They were in the producer's **RAM** (librdkafka's local queue: that's the
`waiting in local buffer: 35` you saw). `kill -9` means **gone**, with no error and no callback.
In your FastAPI app the client already got `202`, so the order is silently lost.
For critical data:
- wait for the delivery result before answering the client (a future that the callback resolves), or
- the **Outbox pattern**: write the event to your **database** in the same transaction as the business data, and
  have a separate process relay it to Kafka. The DB is durable, the in-memory queue is not.

**7. (Bonus) FastAPI: one `Producer` per request or per app? When to `flush()`?**

✅ **One per app.** It's thread-safe, it keeps connections open, and batching only works if many requests share it.
One per request = new TCP connections + a metadata fetch + no batching, per request.
`flush()` goes in the **shutdown** part of `lifespan` (after `yield`), so the queue is drained before the process
exits, exactly as in your code. ⚠️ Never `flush()` per request: it kills batching and adds latency.
