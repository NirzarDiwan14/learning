import time

from confluent_kafka import KafkaError, Producer

config = {
    "bootstrap.servers": "localhost:9092",
    # Broker de-duplicates retries using a producer id + sequence number per partition.
    # Also keeps order intact when retrying. Requires acks=all.
    "enable.idempotence": True,
    "acks": "all",
    # Retry failed sends for up to 30 s in total, then report the message as failed
    # in the delivery callback. (librdkafka default: 300 s)
    "delivery.timeout.ms": 30_000,
    "linger.ms": 20,
    "compression.type": "zstd",
}

producer = Producer(config)

stats = {"ok": 0, "failed": 0}


def make_callback(i, queued_at):
    def on_delivery(err, msg):
        took = time.time() - queued_at
        if err is not None:
            stats["failed"] += 1
            timed_out = err.code() == KafkaError._MSG_TIMED_OUT
            print(f"  ❌ #{i} FAILED after {took:.1f}s: {err.name()}{' (gave up retrying)' if timed_out else ''}")
        else:
            stats["ok"] += 1
            print(f"  ✅ #{i} delivered to p{msg.partition()} offset {msg.offset()} in {took:.2f}s")

    return on_delivery


print("Sending 1 message every 0.5 s for 60 s. Try `docker stop kafka` / `docker start kafka` meanwhile.")

try:
    for i in range(120):
        producer.produce(
            "reliability",
            key=f"order-{i % 10}".encode(),
            value=f"payment #{i}".encode(),
            on_delivery=make_callback(i, time.time()),
        )
        print(f"queued #{i}  (waiting in local buffer: {len(producer)})")
        # Serve callbacks while we wait 0.5 s. poll(timeout) returns early as soon
        # as one callback has run, so keep polling until the 0.5 s are really over.
        next_send = time.time() + 0.5
        while (left := next_send - time.time()) > 0:
            producer.poll(left)
except KeyboardInterrupt:
    pass
finally:
    # flush() returns how many messages are STILL undelivered when the timeout ends
    remaining = producer.flush(timeout=35)
    print(f"\nSUMMARY: delivered={stats['ok']} failed={stats['failed']} still-undelivered={remaining}")
