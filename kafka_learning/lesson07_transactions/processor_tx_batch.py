import argparse
import json
import time

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

# Usage:
#   python processor_tx_batch.py                    -> up to 100 orders per transaction
#   python processor_tx_batch.py --batch-size 1     -> one order per transaction (like processor_tx.py)
parser = argparse.ArgumentParser()
parser.add_argument("--batch-size", type=int, default=100)
parser.add_argument("--tx-id", default="order-processor-batch-1")
args = parser.parse_args()

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "order-processor-tx",  # same group as processor_tx.py: it replaces it
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
        "session.timeout.ms": 6000,
    }
)
producer = Producer({"bootstrap.servers": "localhost:9092", "transactional.id": args.tx_id})
producer.init_transactions()
consumer.subscribe(["orders-placed"])


def rewind(batch):
    # Seek every partition back to the FIRST offset it had in this batch,
    # so the next consume() returns the whole batch again
    first = {}
    for msg in batch:
        first.setdefault((msg.topic(), msg.partition()), msg.offset())
    for (topic, partition), offset in first.items():
        consumer.seek(TopicPartition(topic, partition, offset))


print(f"batch processor running (batch size {args.batch_size}, transactional.id={args.tx_id})")
total, started = 0, None
try:
    while True:
        # Up to batch-size messages, or whatever arrived within 1 s
        batch = [m for m in consumer.consume(num_messages=args.batch_size, timeout=1.0) if not m.error()]
        if not batch:
            continue
        started = started or time.perf_counter()

        producer.begin_transaction()
        try:
            for msg in batch:
                order = json.loads(msg.value())
                invoice = {"order_id": order["order_id"], "total_cents": order["amount_cents"]}
                reservation = {"order_id": order["order_id"], "sku": order["sku"], "qty": order["qty"]}
                producer.produce("tx-invoices", key=msg.key(), value=json.dumps(invoice).encode())
                producer.produce("tx-reservations", key=msg.key(), value=json.dumps(reservation).encode())

            # position() = "next offset to read" for every assigned partition = right after this batch
            producer.send_offsets_to_transaction(
                consumer.position(consumer.assignment()),
                consumer.consumer_group_metadata(),
            )
            producer.commit_transaction()
        except KafkaException as e:
            if e.args[0].txn_requires_abort():
                print(f"aborting batch of {len(batch)}: {e}")
                producer.abort_transaction()
                rewind(batch)
                continue
            raise

        total += len(batch)
        elapsed = time.perf_counter() - started
        print(f"committed batch of {len(batch):>3} | total {total:>5} | {total / elapsed:,.0f} orders/s")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
