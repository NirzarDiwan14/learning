from confluent_kafka import Producer
from models import OrderEvent

producer = Producer({"bootstrap.servers": "localhost:9092"})

topic_name = "order-events"

orders = [("order-101", "alice", 2500), ("order-102", "bob", 999), ("order-103", "alice", 15000)]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"key={msg.key().decode()} -> partition {msg.partition()} offset {msg.offset()}")


for event_type in ["order_created", "order_paid", "order_shipped"]:
    for order_id, user_id, amount_cents in orders:
        event = OrderEvent(
            event_type=event_type,
            order_id=order_id,
            user_id=user_id,
            amount_cents=amount_cents,
        )
        producer.produce(
            topic=topic_name,
            key=order_id.encode(),  # Lesson 02, Q4: order_id keeps each order's events in order
            # SERIALIZE: Python object -> JSON string -> bytes
            value=event.model_dump_json().encode(),
            # Headers = metadata next to the value. Consumers can read them without parsing the body.
            headers={
                "event_type": event_type,
                "schema_version": "1",
                "content_type": "application/json",
            },
            callback=delivery_report,
        )

producer.flush()
