import sys
from confluent_kafka import Consumer, KafkaError

# Run several copies with different names:  python consumer.py A
name = sys.argv[1] if len(sys.argv) > 1 else "A"

config = {
    "bootstrap.servers": "localhost:9092",
    # All copies share ONE group -> they split the partitions between them
    "group.id": "orders-group",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(config)


# Called when the group coordinator gives this consumer its partitions
def on_assign(consumer, partitions):
    print(f"[{name}] ASSIGNED partitions: {[p.partition for p in partitions]}")


# Called when partitions are taken away (another consumer joined or left)
def on_revoke(consumer, partitions):
    print(f"[{name}] REVOKED partitions: {[p.partition for p in partitions]}")


consumer.subscribe(["orders"], on_assign=on_assign, on_revoke=on_revoke)

print(f"[{name}] consumer is running in group 'orders-group'...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"[{name}] Error: {msg.error()}")
            break

        print(
            f"[{name}] partition {msg.partition()} offset {msg.offset():<3} "
            f"key={msg.key().decode('utf-8'):<8} value={msg.value().decode('utf-8')}"
        )

except KeyboardInterrupt:
    pass
finally:
    print(f"[{name}] Closing consumer...")
    consumer.close()
