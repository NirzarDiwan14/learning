import json
import sys

from confluent_kafka import Producer

# Usage: python producer.py [start]   -> sends 10 orders: order-<start> … order-<start+9>  (default start=1)
start = int(sys.argv[1]) if len(sys.argv) > 1 else 1

producer = Producer({"bootstrap.servers": "localhost:9092", "enable.idempotence": True})

for n in range(start, start + 10):
    order = {"order_id": f"order-{n}", "sku": f"sku-{n % 4}", "qty": n % 3 + 1, "amount_cents": n * 1000}
    producer.produce("orders-placed", key=order["order_id"].encode(), value=json.dumps(order).encode())

producer.flush()
print(f"Sent order-{start} … order-{start + 9} to orders-placed")
