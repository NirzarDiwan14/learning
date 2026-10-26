from confluent_kafka import Consumer, KafkaError

# Prints everything in the DLQ. Never commits, so it shows the whole DLQ every time.
consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "dlq-inspector",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        # Get an event when we reach the end of a partition, so the script can exit
        "enable.partition.eof": True,
    }
)
consumer.subscribe(["payment-requests.dlq"])

count = 0
while True:
    msg = consumer.poll(timeout=10.0)
    if msg is None:
        break
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            break
        print(f"Error: {msg.error()}")
        break

    count += 1
    headers = {k: v.decode() for k, v in (msg.headers() or [])}
    print(f"--- DLQ offset {msg.offset()}  key={msg.key().decode() if msg.key() else None}")
    for k, v in headers.items():
        print(f"    {k:<24} {v.replace(chr(10), ' | ')[:120]}")
    print(f"    {'value':<24} {msg.value()[:100]!r}")

print(f"\n{count} message(s) in the DLQ")
consumer.close()
