import argparse
import json
import os
import time

from confluent_kafka import Consumer, KafkaException, Producer

# Usage:
#   python processor_tx.py --crash               -> crashes on order-5 between the two writes
#   python processor_tx.py                       -> run normally
#   python processor_tx.py --tx-id my-id         -> choose the transactional.id (for the fencing experiment)
parser = argparse.ArgumentParser()
parser.add_argument("--crash", action="store_true")
parser.add_argument("--tx-id", default="order-processor-1")
args = parser.parse_args()

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "order-processor-tx",
        "auto.offset.reset": "earliest",
        # Offsets are committed INSIDE the transaction, never on their own
        "enable.auto.commit": False,
        # Don't read messages from transactions that were aborted or are still open
        "isolation.level": "read_committed",
        "session.timeout.ms": 6000,
    }
)
producer = Producer(
    {
        "bootstrap.servers": "localhost:9092",
        # Identifies THIS processor across restarts. Needed for transactions (and implies idempotence).
        "transactional.id": args.tx_id,
    }
)

# Registers the transactional.id with the broker. If a previous instance with the same id
# left a transaction open (e.g. it crashed), that transaction is ABORTED now, and the old
# instance is "fenced": it can never write again.
producer.init_transactions()
consumer.subscribe(["orders-placed"])

print(f"transactional processor running (transactional.id={args.tx_id}, crash={args.crash})")
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
        time.sleep(0.3)

        producer.begin_transaction()
        try:
            invoice = {"order_id": order["order_id"], "total_cents": order["amount_cents"]}
            producer.produce("tx-invoices", key=key, value=json.dumps(invoice).encode())
            producer.flush()  # really written to the broker... but not yet COMMITTED

            if args.crash and order["order_id"] == "order-5":
                print("💥 CRASH after the invoice was written, before the reservation")
                os._exit(1)

            reservation = {"order_id": order["order_id"], "sku": order["sku"], "qty": order["qty"]}
            producer.produce("tx-reservations", key=key, value=json.dumps(reservation).encode())

            # The input offset becomes part of the SAME transaction:
            # "invoice + reservation + I'm done with this input" happen together, or not at all.
            producer.send_offsets_to_transaction(
                consumer.position(consumer.assignment()),
                consumer.consumer_group_metadata(),
            )
            producer.commit_transaction()
            print(f"{order['order_id']}: invoice + reservation + offset committed in ONE transaction")
        except KafkaException as e:
            if e.args[0].txn_requires_abort():
                print(f"{order['order_id']}: aborting transaction: {e}")
                producer.abort_transaction()
                # Rewind so the order is processed again by the next transaction
                consumer.seek(msg)
            else:
                # e.g. we were fenced by another instance with the same transactional.id
                print(f"FATAL: {e}")
                raise
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
