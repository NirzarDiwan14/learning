"""Version 1: the order service calls every other service directly.

Flags:
  --email-down       the email service raises an error
  --slow-analytics   the analytics service takes 2 s instead of 0.1 s
"""
import sys
import time

EMAIL_DOWN = "--email-down" in sys.argv
SLOW_ANALYTICS = "--slow-analytics" in sys.argv

stock = {"keyboard": 5, "mouse": 0}


# ---- "other services" (imagine each one is a separate service behind an HTTP API) ----

def reserve_stock(item, qty):
    if stock[item] < qty:
        raise ValueError(f"not enough {item} in stock")
    stock[item] -= qty
    print(f"    [inventory] reserved {qty} x {item}")


def send_confirmation_email(customer, order_id):
    time.sleep(0.2)
    if EMAIL_DOWN:
        raise ConnectionError("SMTP server not reachable")
    print(f"    [email] sent confirmation for {order_id} to {customer}")


def record_analytics(order_id, item, qty):
    time.sleep(2 if SLOW_ANALYTICS else 0.1)
    print(f"    [analytics] recorded {order_id}: {qty} x {item}")

def award_reward_points(customer,qty):
    print(f"    [LOYALTY] [rewards] awarded {qty} points to {customer}")


# ---- the order service ----

def place_order(order_id, customer, item, qty):
    reserve_stock(item, qty)
    send_confirmation_email(customer, order_id)
    record_analytics(order_id, item, qty)
    award_reward_points(customer, qty)
    return "confirmed"


if __name__ == "__main__":
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

    print(f"\nstock at the end: {stock}")
