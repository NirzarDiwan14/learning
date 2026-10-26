"""Checks the system's promises. Run it when lag.py shows LAG 0 everywhere."""

import sqlite3

import config

orders = sqlite3.connect(config.ORDERS_DB)
bank = sqlite3.connect(config.BANK_DB)

status = dict(orders.execute("SELECT status, COUNT(*) FROM orders GROUP BY status").fetchall())
requests = bank.execute("SELECT COUNT(*) FROM requests").fetchone()[0]
charges = dict(bank.execute("SELECT result, COUNT(*) FROM charges GROUP BY result").fetchall())
charged_keys = bank.execute("SELECT COUNT(DISTINCT idempotency_key) FROM charges").fetchone()[0]
total_charges = sum(charges.values())

print(f"orders by status (read model): {status}")
print(f"bank: {requests} charge requests -> {total_charges} charges {charges}")
print(f"      ({requests - total_charges} extra requests were retries/duplicates absorbed by the idempotency key)")
print()


def check(ok: bool, text: str):
    print(f"{'✅' if ok else '❌'} {text}")


check(charged_keys == total_charges, "no order was charged twice")
check(status.get("pending", 0) == 0, f"no order stuck in 'pending' (pending={status.get('pending', 0)})")
check(status.get("paid", 0) == charges.get("approved", 0), "every approved charge shows as 'paid', and vice versa")
check(
    status.get("payment_failed", 0) == charges.get("declined", 0),
    "every declined charge shows as 'payment_failed', and vice versa",
)
print("\n(pending > 0 is expected while orders are in flight or sitting in the DLQ, see dlq_replay.py)")
