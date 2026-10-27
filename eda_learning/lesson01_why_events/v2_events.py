"""Version 2: the order service publishes an OrderPlaced event. Other services subscribe to it.

Flags (same as v1, plus two):
  --email-down       the email service raises an error
  --slow-analytics   the analytics service takes 2 s instead of 0.1 s
  --async            use AsyncEventBus (in-memory queue + background thread) instead of SyncEventBus
  --crash            the process dies right after the last order, before the bus has delivered everything
"""
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from event_bus import AsyncEventBus, SyncEventBus

EMAIL_DOWN = "--email-down" in sys.argv
SLOW_ANALYTICS = "--slow-analytics" in sys.argv
CRASH = "--crash" in sys.argv

bus = AsyncEventBus() if "--async" in sys.argv else SyncEventBus()
stock = {"keyboard": 5, "mouse": 0}


# ---- the event: a fact, in the past tense, that can't be changed ----

@dataclass(frozen=True)
class OrderPlaced:
    order_id: str
    customer: str
    item: str
    qty: int
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))


# ---- the order service: knows about inventory, but NOT about email or analytics ----

def reserve_stock(item, qty):
    if stock[item] < qty:
        raise ValueError(f"not enough {item} in stock")
    stock[item] -= qty
    print(f"    [inventory] reserved {qty} x {item}")


def place_order(order_id, customer, item, qty):
    reserve_stock(item, qty)  # still a direct call. Why? (Question 3)
    bus.publish(OrderPlaced(order_id, customer, item, qty))
    return "confirmed"


# ---- other services: each one only knows about the event ----

def send_confirmation_email(event: OrderPlaced):
    time.sleep(0.2)
    if EMAIL_DOWN:
        raise ConnectionError("SMTP server not reachable")
    print(f"    [email] sent confirmation for {event.order_id} to {event.customer}")


def record_analytics(event: OrderPlaced):
    time.sleep(2 if SLOW_ANALYTICS else 0.1)
    print(f"    [analytics] recorded {event.order_id}: {event.qty} x {event.item}")


bus.subscribe(OrderPlaced, send_confirmation_email)
bus.subscribe(OrderPlaced, record_analytics)


if __name__ == "__main__":
    print(f"using {type(bus).__name__}")
    orders = [
        ("order-1", "alice@example.com", "keyboard", 2),
        ("order-2", "bob@example.com", "mouse", 1),
        ("order-3", "carol@example.com", "keyboard", 1),
    ]
    for order_id, customer, item, qty in orders:
        print(f"\nplace_order({order_id}, {qty} x {item})")
        start = time.perf_counter()
        try:
            result = place_order(order_id, customer, item, qty)
        except Exception as e:
            result = f"FAILED: {e!r}"
        print(f"  -> {result}   (took {time.perf_counter() - start:.2f} s)")

    if CRASH:
        print("\n💥 process killed before the bus finished delivering")
        sys.stdout.flush()
        os._exit(1)  # like kill -9: no cleanup, nothing in memory survives

    bus.close()
    print(f"\nstock at the end: {stock}")
