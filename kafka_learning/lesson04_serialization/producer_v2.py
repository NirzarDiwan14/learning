import json
import sys
from datetime import UTC, datetime
from uuid import uuid4

from confluent_kafka import Producer

# Usage:
#   python producer_v2.py             -> v2 ADDS a field "currency"         (compatible?)
#   python producer_v2.py breaking    -> v2 RENAMES amount_cents -> amount  (compatible?)
breaking = "breaking" in sys.argv[1:]

producer = Producer({"bootstrap.servers": "localhost:9092"})

# Another team owns this producer and doesn't use our models.py: plain dicts.
event = {
    "event_id": str(uuid4()),
    "event_type": "order_created",
    "order_id": "order-201",
    "user_id": "charlie",
    "currency": "INR",
    "created_at": datetime.now(UTC).isoformat(),
}
if breaking:
    event["amount"] = 49.99
else:
    event["amount_cents"] = 4999

producer.produce(
    topic="order-events",
    key=event["order_id"].encode(),
    value=json.dumps(event).encode(),
    headers={"event_type": event["event_type"], "schema_version": "2"},
)
producer.flush()
print(f"Sent v2 event (breaking={breaking}): {event}")
