"""Builds the ORDER STATUS read model (SQLite) from events. The API answers GET /orders/{id} from it.

Every write is an idempotent UPSERT, so duplicates and replays are harmless.
"""

import logging
import sqlite3
from datetime import UTC, datetime

import config
from events import OrderPlaced, PaymentResult
from kafka_utils import make_consumer, stop_on_sigterm
from pydantic import ValidationError

GROUP = "order-status-projector"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [projector] %(message)s")
log = logging.getLogger("projector")


def connect():
    db = sqlite3.connect(config.ORDERS_DB, timeout=10)
    db.execute("PRAGMA journal_mode=WAL")  # the API can read while we write
    db.execute(
        "CREATE TABLE IF NOT EXISTS orders ("
        " order_id TEXT PRIMARY KEY, customer_id TEXT, total_cents INTEGER,"
        " status TEXT NOT NULL, reason TEXT, created_at TEXT, updated_at TEXT)"
    )
    return db


def apply_order_placed(db, event: OrderPlaced):
    # The payment result may have been projected FIRST (different topic = no ordering guarantee),
    # so on conflict only fill in the details and never touch the status.
    db.execute(
        "INSERT INTO orders (order_id, customer_id, total_cents, status, created_at, updated_at)"
        " VALUES (?, ?, ?, 'pending', ?, ?)"
        " ON CONFLICT(order_id) DO UPDATE SET"
        "  customer_id = excluded.customer_id, total_cents = excluded.total_cents, created_at = excluded.created_at",
        (event.order_id, event.customer_id, event.total_cents, event.created_at.isoformat(), _now()),
    )


def apply_payment_result(db, event: PaymentResult):
    status = "paid" if event.event_type == "payment_succeeded" else "payment_failed"
    db.execute(
        "INSERT INTO orders (order_id, status, reason, updated_at) VALUES (?, ?, ?, ?)"
        " ON CONFLICT(order_id) DO UPDATE SET status = excluded.status, reason = excluded.reason,"
        "  updated_at = excluded.updated_at",
        (event.order_id, status, event.reason, _now()),
    )


def _now():
    return datetime.now(UTC).isoformat()


HANDLERS = {
    "order_placed": (OrderPlaced, apply_order_placed),
    "payment_succeeded": (PaymentResult, apply_payment_result),
    "payment_failed": (PaymentResult, apply_payment_result),
}


def main():
    stop_on_sigterm()
    db = connect()
    consumer = make_consumer(GROUP, [config.ORDERS_TOPIC, config.PAYMENTS_TOPIC])
    log.info("projector running")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("consumer error: %s", msg.error())
                continue

            # Route by header: no need to parse the body to know what it is (Lesson 04, Q4)
            headers = {k: (v or b"").decode(errors="replace") for k, v in (msg.headers() or [])}
            handler = HANDLERS.get(headers.get("event_type"))
            if handler is None:
                log.warning("%s[%d]@%d: unknown event_type %r, skipping", msg.topic(), msg.partition(),
                            msg.offset(), headers.get("event_type"))
            else:
                model, apply = handler
                try:
                    event = model.model_validate_json(msg.value())
                    with db:  # one SQLite transaction per event
                        apply(db, event)
                    log.info("%s %s", event.order_id[:8], event.event_type)
                except ValidationError as e:
                    # A read model can't fix bad data. The payment service already DLQs bad orders.
                    log.error("%s[%d]@%d: invalid event, skipping: %s", msg.topic(), msg.partition(), msg.offset(), e)

            consumer.store_offsets(message=msg)  # only after the DB commit
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        db.close()


if __name__ == "__main__":
    main()
