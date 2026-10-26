import logging
import os
import time
from uuid import NAMESPACE_URL, uuid5

import bank
import config
from events import OrderPlaced, PaymentResult
from kafka_utils import EventProducer, make_consumer, send_to_dlq, stop_on_sigterm
from pydantic import ValidationError

GROUP = "payment-service"
MAX_ATTEMPTS = 5
BASE_BACKOFF_S = 0.2  # 0.2, 0.4, 0.8, 1.6 s

logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [payment pid={os.getpid()}] %(message)s")
log = logging.getLogger("payment")


def charge_with_retries(order: OrderPlaced) -> PaymentResult:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            # order_id as idempotency key: retries, replays and duplicates can never charge twice
            outcome = bank.charge(order.order_id, order.total_cents, order.card_last4)
            break
        except bank.BankUnavailable as e:
            if attempt == MAX_ATTEMPTS:
                raise
            delay = BASE_BACKOFF_S * 2 ** (attempt - 1)
            log.warning("%s: %s (attempt %d), retrying in %.1fs", order.order_id[:8], e, attempt, delay)
            time.sleep(delay)

    return PaymentResult(
        # Deterministic event_id: processing the same order twice yields the SAME event id,
        # so downstream consumers can recognize the duplicate
        event_id=str(uuid5(NAMESPACE_URL, f"payment:{order.order_id}")),
        event_type="payment_succeeded" if outcome == "approved" else "payment_failed",
        order_id=order.order_id,
        amount_cents=order.total_cents,
        reason=None if outcome == "approved" else "card_declined",
        currency=order.currency,  # v2: "INR" for old v1 events (model default)
    )


def main():
    stop_on_sigterm()
    producer = EventProducer()
    consumer = make_consumer(GROUP, [config.ORDERS_TOPIC])
    log.info("payment-service running")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("consumer error: %s", msg.error())
                continue

            # 1. Permanent error: bad data, straight to the DLQ
            try:
                order = OrderPlaced.model_validate_json(msg.value())
            except ValidationError as e:
                send_to_dlq(producer, msg, config.ORDERS_DLQ_TOPIC, GROUP, "validation", e)
                log.error("p%d off%d: invalid order, sent to DLQ", msg.partition(), msg.offset())
                consumer.store_offsets(message=msg)
                continue

            # 2. Transient errors: retry with backoff, then DLQ
            try:
                result = charge_with_retries(order)
            except bank.BankUnavailable as e:
                send_to_dlq(producer, msg, config.ORDERS_DLQ_TOPIC, GROUP, "retries_exhausted", e)
                log.error("%s: bank unavailable, sent to DLQ", order.order_id[:8])
                consumer.store_offsets(message=msg)
                continue

            # 3. Publish the outcome. A declined card is a normal business OUTCOME, not an error.
            producer.send_sync(
                config.PAYMENTS_TOPIC,
                order.order_id,
                result,
                headers={"event_type": result.event_type, "schema_version": "2"},
            )
            consumer.store_offsets(message=msg)
            log.info(
                "p%d off%d: %s %.2f %s -> %s",
                msg.partition(), msg.offset(), order.order_id[:8], order.total_cents / 100, order.currency,
                result.event_type,
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
