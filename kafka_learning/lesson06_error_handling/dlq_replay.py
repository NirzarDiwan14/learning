import sys

from confluent_kafka import Consumer, KafkaError, Producer

# Usage:
#   python dlq_replay.py retries_exhausted   -> send those DLQ messages back to their original topic
#   python dlq_replay.py validation
# Each DLQ message is replayed only once: this script commits its own position in the DLQ,
# with one group PER error type, so replaying one type never skips over the others.
error_type = sys.argv[1] if len(sys.argv) > 1 else "retries_exhausted"

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": f"dlq-replayer-{error_type}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    }
)
producer = Producer({"bootstrap.servers": "localhost:9092", "enable.idempotence": True, "acks": "all"})
consumer.subscribe(["payment-requests.dlq"])

replayed = skipped = 0
while True:
    msg = consumer.poll(timeout=10.0)
    if msg is None:
        break
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            break
        print(f"Error: {msg.error()}")
        break

    headers = dict((k, v.decode()) for k, v in (msg.headers() or []))
    if headers.get("dlq.error.type") != error_type:
        skipped += 1
        continue

    # Back to the original topic, with the original headers (dlq.* removed) + a replay marker
    original_headers = [(k, v) for k, v in (msg.headers() or []) if not k.startswith("dlq.")]
    original_headers.append(("replayed.from.dlq.offset", str(msg.offset())))
    producer.produce(headers["dlq.original.topic"], key=msg.key(), value=msg.value(), headers=original_headers)
    replayed += 1
    print(f"replaying DLQ offset {msg.offset()} key={msg.key().decode()} -> {headers['dlq.original.topic']}")

# Commit our DLQ position only after the replayed messages are safely written
if replayed + skipped == 0:
    print("Nothing new in the DLQ")
elif producer.flush(10) == 0:
    consumer.commit(asynchronous=False)
else:
    print("Some replays were not delivered, not committing (run again)")

print(f"replayed={replayed}, skipped (other error types)={skipped}")
consumer.close()
