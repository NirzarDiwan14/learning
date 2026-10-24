from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

topic_name = "payments"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"offset {msg.offset():<3} value={msg.value().decode('utf-8')}")


for i in range(10):
    producer.produce(
        topic=topic_name,
        value=f"payment-{i}".encode(),
        callback=delivery_report,
    )

producer.flush()
