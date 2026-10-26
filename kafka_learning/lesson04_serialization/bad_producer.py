from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

# A buggy service sends something that is not valid JSON at all
producer.produce(
    topic="order-events",
    key=b"order-666",
    value=b"{'order_id': 'order-666', oops this is not json",
)
producer.flush()
print("Sent a broken message to order-events")
