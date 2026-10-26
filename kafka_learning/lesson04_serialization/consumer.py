from datetime import UTC, datetime

from confluent_kafka import Consumer
from pydantic import ValidationError
from models import OrderEvent

config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-events-processor",
    "auto.offset.reset": "earliest",
    # Lesson 03, Q7: the timer commits in the background, but only offsets WE stored,
    # i.e. messages we finished. At-least-once without a blocking commit per message.
    "enable.auto.commit": True,
    "enable.auto.offset.store": False,
    "session.timeout.ms": 6000,
}

consumer = Consumer(config)
consumer.subscribe(["order-events"])

print("order-events consumer is running...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            break

        # headers() is a list of (str, bytes) tuples, or None
        headers = {k: v.decode() for k, v in (msg.headers() or [])}

        try:
            # DESERIALIZE: bytes -> JSON -> validated Python object
            event = OrderEvent.model_validate_json(msg.value())

            # Every message also has a timestamp (ms since epoch) set by the producer
            _, ts_ms = msg.timestamp()
            sent_at = datetime.fromtimestamp(ts_ms / 1000, UTC).strftime("%H:%M:%S")

            print(
                f"p{msg.partition()} off{msg.offset():<3} {sent_at} "
                f"v{headers.get('schema_version')} {event.event_type:<14} "
                f"{event.order_id} user={event.user_id:<6} amount={event.amount_cents / 100:.2f}"
            )

            consumer.store_offsets(message=msg)

        except ValidationError as e:
            print(
                f"[POISON PILL] Failed to parse message | "
                f"partition: {msg.partition()} | offset: {msg.offset()} | "
                f"raw_value: {msg.value()!r} | error: {e}"
            )
            # We skip calling store_offsets(message=msg) for the invalid message. 
            # The consumer loop continues to poll and process good messages behind it.

except KeyboardInterrupt:
    pass
finally:
    print("Closing consumer...")
    consumer.close()  # also commits the stored offsets one last time