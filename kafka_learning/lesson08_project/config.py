from pathlib import Path

BOOTSTRAP_SERVERS = "localhost:9092"

ORDERS_TOPIC = "shop-orders"  # OrderPlaced events, key = order_id
PAYMENTS_TOPIC = "shop-payments"  # PaymentResult events, key = order_id
ORDERS_DLQ_TOPIC = "shop-orders-dlq"  # orders the payment service could not handle

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
ORDERS_DB = DATA_DIR / "orders.db"  # read model, written by projector.py, read by api.py
BANK_DB = DATA_DIR / "bank.db"  # the fake bank's own records
