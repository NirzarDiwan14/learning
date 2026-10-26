import json

from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092", "enable.idempotence": True})


def payment(n, amount_cents=1000, currency="INR"):
    return json.dumps(
        {"payment_id": f"pay-{n}", "order_id": f"order-{n}", "amount_cents": amount_cents, "currency": currency}
    ).encode()


messages = [
    ("pay-1", payment(1)),                         # fine
    ("pay-2", payment(2)),                         # fine
    ("pay-3", payment(3)),                         # bank is flaky: fails twice, then works
    ("pay-4", payment(4)),                         # fine
    ("pay-5", b"{'payment_id': 'pay-5', broken"),  # not even JSON
    ("pay-6", payment(6)),                         # bank is flaky: fails twice, then works
    ("pay-7", payment(7)),                         # bank outage: always fails
    ("pay-8", payment(8, amount_cents=-500)),      # valid JSON, breaks a business rule
    ("pay-9", payment(9, currency="XYZ")),         # unknown currency
    ("pay-10", payment(10)),                       # fine
]

for key, value in messages:
    producer.produce("payment-requests", key=key.encode(), value=value)

producer.flush()
print(f"Sent {len(messages)} payment requests")
