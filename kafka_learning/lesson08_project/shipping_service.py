"""Exercise A: shop-payments -> shop-shipments, exactly-once with a Kafka transaction (Lesson 07).

Kafka -> Kafka only (no bank, no email), so a transaction really gives exactly-once here:
"shipments produced" and "payments consumed" are committed together, or not at all.

Usage:
    python shipping_service.py                 # instance 1
    python shipping_service.py --instance 2    # a second instance (needs its own transactional.id)
"""

import argparse
import logging
import socket
from uuid import NAMESPACE_URL, uuid5

import config
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from events import PaymentResult, ShipmentRequested
from kafka_utils import stop_on_sigterm
from pydantic import ValidationError

GROUP = "shipping-service"

parser = argparse.ArgumentParser()
parser.add_argument("--instance", default="1")
args = parser.parse_args()

logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [shipping {args.instance}] %(message)s")
log = logging.getLogger("shipping")


def rewind(consumer, batch):
    first = {}
    for msg in batch:
        first.setdefault((msg.topic(), msg.partition()), msg.offset())
    for (topic, partition), offset in first.items():
        consumer.seek(TopicPartition(topic, partition, offset))


def main():
    stop_on_sigterm()
    consumer = Consumer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "group.id": GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # offsets are committed inside the transaction
            "isolation.level": "read_committed",
            "session.timeout.ms": 10_000,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            # Unique per running instance, stable across restarts (Lesson 07, Q4)
            "transactional.id": f"{GROUP}-{socket.gethostname()}-{args.instance}",
            "linger.ms": 5,
        }
    )
    producer.init_transactions()
    consumer.subscribe(
        [config.PAYMENTS_TOPIC],
        on_assign=lambda c, ps: log.info("ASSIGNED %s", [p.partition for p in ps]),
        on_revoke=lambda c, ps: log.info("REVOKED %s", [p.partition for p in ps]),
    )
    log.info("shipping-service running")

    in_transaction = False
    try:
        while True:
            batch = []
            for msg in consumer.consume(num_messages=100, timeout=1.0):
                if msg.error():
                    log.error("consumer error: %s", msg.error())
                else:
                    batch.append(msg)
            if not batch:
                continue

            producer.begin_transaction()
            in_transaction = True
            try:
                shipped = 0
                for msg in batch:
                    headers = {k: (v or b"").decode(errors="replace") for k, v in (msg.headers() or [])}
                    # payment_failed: nothing to ship, but its offset still moves forward with the batch
                    if headers.get("event_type") != "payment_succeeded":
                        continue
                    try:
                        payment = PaymentResult.model_validate_json(msg.value())
                    except ValidationError as e:
                        log.error("p%d off%d: invalid payment event, skipping: %s", msg.partition(), msg.offset(), e)
                        continue
                    shipment = ShipmentRequested(
                        # Deterministic: even if this ever ran twice, downstream sees the same event id
                        event_id=str(uuid5(NAMESPACE_URL, f"shipment:{payment.order_id}")),
                        order_id=payment.order_id,
                    )
                    producer.produce(
                        config.SHIPMENTS_TOPIC,
                        key=payment.order_id.encode(),
                        value=shipment.model_dump_json().encode(),
                        headers={"event_type": shipment.event_type, "schema_version": "1"},
                    )
                    shipped += 1

                # Positions of partitions we actually read from (others may have no valid position yet)
                positions = [tp for tp in consumer.position(consumer.assignment()) if tp.offset >= 0]
                producer.send_offsets_to_transaction(positions, consumer.consumer_group_metadata())
                producer.commit_transaction()
                in_transaction = False
                log.info("committed: %d payment events read, %d shipments requested", len(batch), shipped)
            except KafkaException as e:
                if e.args[0].txn_requires_abort():
                    log.warning("aborting batch of %d: %s", len(batch), e)
                    producer.abort_transaction()
                    in_transaction = False
                    rewind(consumer, batch)
                    continue
                raise
    except KeyboardInterrupt:
        pass
    finally:
        # Never leave a transaction open: it would block read_committed readers of shop-shipments
        # until transaction.timeout.ms (Lesson 07, Experiment 3)
        if in_transaction:
            log.info("aborting the open transaction before exiting")
            producer.abort_transaction()
        consumer.close()


if __name__ == "__main__":
    main()
