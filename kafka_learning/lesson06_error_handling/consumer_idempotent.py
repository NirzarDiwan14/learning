import sqlite3
import time
from datetime import UTC, datetime

import fake_bank
from confluent_kafka import Consumer, Producer
from models import PaymentRequest
from pydantic import ValidationError

TOPIC = "payment-requests"
DLQ_TOPIC = "payment-requests.dlq"
GROUP = "payment-processor"

MAX_ATTEMPTS = 4
BASE_BACKOFF_S = 0.5  # waits 0.5 s, 1 s, 2 s between attempts

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.offset.store": False,
        "session.timeout.ms": 6000,
    }
)
# Remembers which payments were already charged. Survives restarts, replays and rebalances.
db = sqlite3.connect("payments.db")
db.execute("CREATE TABLE IF NOT EXISTS charges (payment_id TEXT PRIMARY KEY, charged_at TEXT)")

dlq_producer = Producer({"bootstrap.servers": "localhost:9092", "enable.idempotence": True, "acks": "all"})


def send_to_dlq(msg, error_type: str, error: Exception) -> None:
    # Keep the ORIGINAL key, value and headers untouched, and add WHY and WHERE it failed
    headers = (msg.headers() or []) + [
        ("dlq.error.type", error_type),
        ("dlq.error.message", str(error)[:1000]),
        ("dlq.original.topic", msg.topic()),
        ("dlq.original.partition", str(msg.partition())),
        ("dlq.original.offset", str(msg.offset())),
        ("dlq.consumer.group", GROUP),
        ("dlq.failed.at", datetime.now(UTC).isoformat()),
    ]
    dlq_producer.produce(DLQ_TOPIC, key=msg.key(), value=msg.value(), headers=headers)
    # Wait for the DLQ write. Only after it's safely stored may we move past the message.
    if dlq_producer.flush(10) > 0:
        raise RuntimeError("Could not write to the DLQ, crashing instead of losing the message")
    print(f"   ☠️  sent to DLQ ({error_type})")


def process(msg) -> None:
    key = msg.key().decode() if msg.key() else None
    print(f"p{msg.partition()} off{msg.offset()} key={key}")

    # 1. Parse + validate. A bad message stays bad forever: retrying is pointless (PERMANENT error)
    try:
        payment = PaymentRequest.model_validate_json(msg.value())
    except ValidationError as e:
        print(f"   invalid message: {e.errors()[0]['msg']}")
        send_to_dlq(msg, "validation", e)
        return

    # 2. Idempotency check: a replay or rebalance may deliver the same payment again
    if db.execute("SELECT 1 FROM charges WHERE payment_id = ?", (payment.payment_id,)).fetchone():
        print(f"   ⏭️  {payment.payment_id} already charged, skipping")
        return

    # 3. Call the bank. Timeouts may go away: retry with exponential backoff (TRANSIENT error)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            fake_bank.charge(payment)
            # Record it right after charging. (Not atomic with the charge: see Q7.)
            with db:
                db.execute(
                    "INSERT INTO charges VALUES (?, ?)", (payment.payment_id, datetime.now(UTC).isoformat())
                )
            print(f"   ✅ charged {payment.amount_cents / 100:.2f} {payment.currency} (attempt {attempt})")
            return
        except fake_bank.TransientError as e:
            if attempt == MAX_ATTEMPTS:
                print(f"   attempt {attempt} failed: {e}, giving up")
                send_to_dlq(msg, "retries_exhausted", e)
                return
            delay = BASE_BACKOFF_S * 2 ** (attempt - 1)
            print(f"   attempt {attempt} failed: {e}, retrying in {delay}s")
            time.sleep(delay)


consumer.subscribe([TOPIC])
print("payment-processor is running...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        process(msg)
        # Charged OR safely in the DLQ: either way this message is handled
        consumer.store_offsets(message=msg)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    dlq_producer.flush(10)
    db.close()
