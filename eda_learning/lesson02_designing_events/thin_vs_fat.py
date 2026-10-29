"""Thin vs fat events: the same food-delivery flow, published both ways.

Two consumers:
  courier-app        prints a delivery label (needs the address)  on order.placed + address.changed
  restaurant-tablet  shows what to cook (needs the items)          on order.placed

The customer changes the address of o-2 0.1 s after placing it.

Flags:
  --mode thin|fat|both   which version to run (default: both, one after the other)
  --service-down         the orders service's GET /orders/{id} is unreachable
  --orders N             how many orders to place (default 3)
"""
import json
import sys
import time

from envelope import from_json, make_event, to_json
from event_bus import Bus

SERVICE_DOWN = "--service-down" in sys.argv


def arg(name, default):
    return sys.argv[sys.argv.index(name) + 1] if name in sys.argv else default


MODE = arg("--mode", "both")
N_ORDERS = int(arg("--orders", "3"))

PLACED = "com.fooddash.order.placed.v1"
ADDRESS_CHANGED = "com.fooddash.order.address_changed.v1"
SOURCE = "/services/orders"

start = time.perf_counter()


def log(msg):
    print(f"  {time.perf_counter() - start:5.2f}s {msg}")


# ---- the orders service: owns the data, publishes events, answers GET calls ----

class OrderService:
    def __init__(self, bus, fat):
        self.bus, self.fat = bus, fat
        self.orders = {}
        self.get_calls = 0

    def place_order(self, order_id, name, phone, address, items):
        self.orders[order_id] = {
            "order_id": order_id,
            "customer": {"name": name, "phone": phone},
            "delivery_address": address,
            "items": items,
            "total_cents": sum(i["qty"] * i["price_cents"] for i in items),
            "currency": "EUR",
        }
        data = dict(self.orders[order_id]) if self.fat else {"order_id": order_id}
        log(f"[orders] {order_id} placed, address: {address}")
        self.bus.publish(to_json(make_event(PLACED, SOURCE, order_id, data)))

    def change_address(self, order_id, new_address):
        self.orders[order_id]["delivery_address"] = new_address
        data = {"order_id": order_id, "new_address": new_address} if self.fat else {"order_id": order_id}
        log(f"[orders] {order_id} address changed to: {new_address}")
        self.bus.publish(to_json(make_event(ADDRESS_CHANGED, SOURCE, order_id, data)))

    def get_order(self, order_id):
        """GET /orders/{id}: what thin consumers must call. A network call takes time."""
        self.get_calls += 1
        time.sleep(0.3)
        if SERVICE_DOWN:
            raise ConnectionError("GET /orders/{id}: orders service unreachable")
        return json.loads(json.dumps(self.orders[order_id]))  # a copy of the CURRENT state


# ---- consumers ----

def make_thin_consumers(service):
    def courier_app(event_json):
        event = from_json(event_json)
        order = service.get_order(event["data"]["order_id"])  # call back for the details
        why = "new order" if event["type"] == PLACED else "address changed"
        log(f"[courier-app] LABEL {order['order_id']} ({why}) -> {order['delivery_address']}")

    def restaurant_tablet(event_json):
        event = from_json(event_json)
        order = service.get_order(event["data"]["order_id"])
        dishes = ", ".join(f"{i['qty']}x {i['dish']}" for i in order["items"])
        log(f"[restaurant-tablet] COOK {order['order_id']}: {dishes}")

    return courier_app, restaurant_tablet


def make_fat_consumers():
    addresses = {}  # the courier app's own local copy: no calls back to the orders service

    def courier_app(event_json):
        event = from_json(event_json)
        data = event["data"]
        if event["type"] == PLACED:
            addresses[data["order_id"]] = data["delivery_address"]
            why = "new order"
        else:
            addresses[data["order_id"]] = data["new_address"]
            why = "address changed"
        log(f"[courier-app] LABEL {data['order_id']} ({why}) -> {addresses[data['order_id']]}")

    def restaurant_tablet(event_json):
        data = from_json(event_json)["data"]
        dishes = ", ".join(f"{i['qty']}x {i['dish']}" for i in data["items"])
        log(f"[restaurant-tablet] COOK {data['order_id']}: {dishes}")

    return courier_app, restaurant_tablet


def run(mode):
    global start
    start = time.perf_counter()
    print(f"\n========== {mode.upper()} events ==========")
    log_path = f"event_log_{mode}.jsonl"
    bus = Bus(log_path)
    service = OrderService(bus, fat=(mode == "fat"))
    courier_app, restaurant_tablet = make_fat_consumers() if mode == "fat" else make_thin_consumers(service)
    bus.subscribe("courier-app", [PLACED, ADDRESS_CHANGED], courier_app)
    bus.subscribe("restaurant-tablet", [PLACED], restaurant_tablet)

    for n in range(1, N_ORDERS + 1):
        order_id = f"o-{n}"
        service.place_order(
            order_id, f"Customer {n}", f"+49 170 000000{n}", f"{n} Old Street",
            [{"dish": "pizza", "qty": n, "price_cents": 1100}, {"dish": "cola", "qty": 1, "price_cents": 250}],
        )
        if order_id == "o-2":
            time.sleep(0.1)
            service.change_address(order_id, "99 New Avenue")
        time.sleep(0.05)

    bus.close()
    with open(log_path) as f:
        lines = f.read().splitlines()
    print(f"  --- summary ({mode}) ---")
    print(f"  events published:               {len(lines)}")
    print(f"  avg event size:                 {sum(map(len, lines)) // len(lines)} bytes")
    print(f"  GET /orders calls by consumers: {service.get_calls}")
    print(f"  events in {log_path} containing a phone number: {sum('+49' in l for l in lines)}")


if __name__ == "__main__":
    for mode in (["thin", "fat"] if MODE == "both" else [MODE]):
        run(mode)
