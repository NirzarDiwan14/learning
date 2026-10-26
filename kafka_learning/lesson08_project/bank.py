"""A pretend external bank. It is NOT Kafka: think of it as an HTTP API of another company.

- Slow-ish: 10-50 ms per call.
- Unreliable: with probability BANK_FAILURE_RATE (default 0.2) a call times out, and HALF of those
  timeouts happen AFTER the money was already taken (the worst kind: you can't tell what happened).
- Supports an idempotency key: the same key never charges twice, it returns the first result again.
- Card "0000" is always declined.
"""

import os
import random
import sqlite3
import time
from datetime import UTC, datetime

import config

FAILURE_RATE = float(os.environ.get("BANK_FAILURE_RATE", "0.2"))


class BankUnavailable(Exception):
    """Timeout / 503. Transient: retrying later may work."""


def _connect():
    db = sqlite3.connect(config.BANK_DB, timeout=10, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE IF NOT EXISTS requests (id INTEGER PRIMARY KEY, idempotency_key TEXT, at TEXT)")
    db.execute(
        "CREATE TABLE IF NOT EXISTS charges ("
        " idempotency_key TEXT PRIMARY KEY, amount_cents INTEGER, result TEXT, at TEXT)"
    )
    return db


_db = _connect()


def charge(idempotency_key: str, amount_cents: int, card_last4: str) -> str:
    """Returns "approved" or "declined". Raises BankUnavailable."""
    time.sleep(random.uniform(0.01, 0.05))
    now = datetime.now(UTC).isoformat()
    _db.execute("INSERT INTO requests (idempotency_key, at) VALUES (?, ?)", (idempotency_key, now))

    roll = random.random()
    if roll < FAILURE_RATE / 2:
        raise BankUnavailable("bank timeout (nothing happened)")

    result = "declined" if card_last4 == "0000" else "approved"
    # Same key again: keep the FIRST result, never charge twice
    _db.execute(
        "INSERT OR IGNORE INTO charges VALUES (?, ?, ?, ?)", (idempotency_key, amount_cents, result, now)
    )
    result = _db.execute("SELECT result FROM charges WHERE idempotency_key = ?", (idempotency_key,)).fetchone()[0]

    if roll < FAILURE_RATE:
        raise BankUnavailable("bank timeout (but the charge went through!)")
    return result
