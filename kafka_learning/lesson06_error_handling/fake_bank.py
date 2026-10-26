import os
from collections import Counter

from models import PaymentRequest


class TransientError(Exception):
    """Temporary problem (timeout, 503...). Trying again later may work."""


# Flaky: fails the first 2 attempts, then works
FLAKY = {"pay-3", "pay-6"}
# Down: the bank can't process these at all while BANK_OUTAGE=1 (the default)
OUTAGE = {"pay-7"} if os.environ.get("BANK_OUTAGE", "1") == "1" else set()

_attempts = Counter()


def charge(payment: PaymentRequest) -> None:
    _attempts[payment.payment_id] += 1
    if payment.payment_id in OUTAGE:
        raise TransientError("bank timeout (outage)")
    if payment.payment_id in FLAKY and _attempts[payment.payment_id] <= 2:
        raise TransientError(f"bank timeout (attempt {_attempts[payment.payment_id]})")
