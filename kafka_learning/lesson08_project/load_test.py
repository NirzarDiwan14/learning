import argparse
import json
import random
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

# Usage: python load_test.py -n 300 -c 20
parser = argparse.ArgumentParser()
parser.add_argument("-n", type=int, default=300, help="number of orders")
parser.add_argument("-c", type=int, default=20, help="concurrent clients")
args = parser.parse_args()


def place_order(i: int) -> int:
    body = {
        "customer_id": f"customer-{i % 50}",
        "items": [{"sku": f"sku-{random.randint(1, 20)}", "qty": random.randint(1, 3), "unit_price_cents": 1999}],
        # ~10% of customers use a card that the bank always declines
        "card_last4": "0000" if i % 10 == 0 else f"{random.randint(1000, 9999)}",
    }
    request = urllib.request.Request(
        "http://127.0.0.1:8000/orders",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status
    except urllib.error.HTTPError as e:
        return e.code
    except urllib.error.URLError:
        return 0  # API not reachable


start = time.perf_counter()
with ThreadPoolExecutor(max_workers=args.c) as pool:
    statuses = Counter(pool.map(place_order, range(args.n)))
elapsed = time.perf_counter() - start

print(f"{args.n} orders in {elapsed:.1f}s ({args.n / elapsed:.0f}/s) with {args.c} clients")
print(f"HTTP status codes: {dict(statuses)}   (0 = API not reachable)")
