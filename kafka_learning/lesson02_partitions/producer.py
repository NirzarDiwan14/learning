import time

from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

topic_name = "orders"

users = [
    "alice",
    "bob",
    "charlie",
    "dave",
    "eve",
    "Nirzar",
    "Rekha",
    "Helen",
    "Keller",
    "Rohit",
    "Sita",
    "Ram",
    "Shyam",
    "Gita",
    "Mohan",
]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"key={msg.key().decode('utf-8'):<8} -> partition {msg.partition()} "
            f"offset {msg.offset():<3} value={msg.value().decode('utf-8')}"
        )


print(f"Producing keyed messages to topic: {topic_name}")

for i in range(50):
    user = users[i % len(users)]
    producer.produce(
        topic=topic_name,
        # The KEY decides the partition: hash(key) % num_partitions.
        # Same key -> always the same partition -> ordered per user.
        key=user.encode("utf-8"),
        value=f"order-{i} by {user}".encode(),
        callback=delivery_report,
    )
    # poll(0) runs delivery callbacks for already-sent messages
    producer.poll(0)
    time.sleep(0.5)

producer.flush()
