import json
import os
import sys
import time

from confluent_kafka import Consumer, Producer

# Usage:
#   python processor_naive.py crash   -> crashes on order-5 between the two writes
#   python processor_naive.py         -> run normally (e.g. the restart after the crash)
crash = "crash" in sys.argv[1:]

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "order-processor-naive",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 6000,
    }
)
producer = Producer({"bootstrap.servers": "localhost:9092", "enable.idempotence": True})
consumer.subscribe(["orders-placed"])

print(f"naive processor running (crash={crash})")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        order = json.loads(msg.value())
        key = msg.key()
        time.sleep(0.3)  # pretend to do some work

        # Write 1: the invoice
        invoice = {"order_id": order["order_id"], "total_cents": order["amount_cents"]}
        producer.produce("naive-invoices", key=key, value=json.dumps(invoice).encode())
        producer.flush()

        if crash and order["order_id"] == "order-5":
            print("💥 CRASH after the invoice was written, before the reservation")
            os._exit(1)

        # Write 2: the inventory reservation
        reservation = {"order_id": order["order_id"], "sku": order["sku"], "qty": order["qty"]}
        producer.produce("naive-reservations", key=key, value=json.dumps(reservation).encode())
        producer.flush()

        # Write 3: "I'm done with this input message"
        consumer.commit(message=msg, asynchronous=False)
        print(f"{order['order_id']}: invoice + reservation written, offset committed")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
