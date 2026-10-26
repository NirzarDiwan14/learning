import sys

import config
from confluent_kafka import Consumer, KafkaError, Producer

# Usage: python dlq_replay.py [error_type]     (default: retries_exhausted)
# Sends DLQ messages of one error type back to their original topic. Same design as Lesson 06.
error_type = sys.argv[1] if len(sys.argv) > 1 else "retries_exhausted"

consumer = Consumer(
    {
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": f"shop-dlq-replayer-{error_type}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    }
)
producer = Producer({"bootstrap.servers": config.BOOTSTRAP_SERVERS, "enable.idempotence": True})
consumer.subscribe([config.ORDERS_DLQ_TOPIC])

replayed = skipped = 0
while True:
    msg = consumer.poll(10.0)
    if msg is None:
        break
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            break
        print(f"Error: {msg.error()}")
        break
    headers = {k: v.decode() for k, v in (msg.headers() or [])}
    if headers.get("dlq.error.type") != error_type:
        skipped += 1
        continue
    original_headers = [(k, v) for k, v in (msg.headers() or []) if not k.startswith("dlq.")]
    producer.produce(headers["dlq.original.topic"], key=msg.key(), value=msg.value(), headers=original_headers)
    replayed += 1

if replayed + skipped == 0:
    print("Nothing new in the DLQ")
elif producer.flush(10) == 0:
    consumer.commit(asynchronous=False)
else:
    print("Some replays were not delivered, not committing (run again)")
print(f"replayed={replayed} skipped (other error types)={skipped}")
consumer.close()
