import json
import time

from confluent_kafka import Producer

# Opens a transaction, writes into it, and then... just hangs (like a processor stuck in a slow call).
tx_producer = Producer(
    {
        "bootstrap.servers": "localhost:9092",
        "transactional.id": "stuck-processor",
        # The broker aborts a transaction that stays open longer than this (default 60 s)
        "transaction.timeout.ms": 20_000,
    }
)
plain_producer = Producer({"bootstrap.servers": "localhost:9092"})

tx_producer.init_transactions()
tx_producer.begin_transaction()
tx_producer.produce("tx-invoices", key=b"order-900", value=json.dumps({"order_id": "order-900"}).encode(), partition=0)
tx_producer.flush()
print("order-900: written inside an OPEN transaction (partition 0)")

# A normal, non-transactional message written AFTER it, to the same partition
plain_producer.produce("tx-invoices", key=b"order-901", value=json.dumps({"order_id": "order-901"}).encode(), partition=0)
plain_producer.flush()
print("order-901: written normally, no transaction (partition 0)")

print("Now hanging for 30 s without committing. Run reader.py (both modes) in another terminal!")
for left in range(30, 0, -5):
    print(f"  {left}s left...")
    time.sleep(5)
print("Done. (The broker should already have aborted the transaction after 20 s.)")
