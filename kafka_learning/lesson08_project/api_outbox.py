"""Exercise C: the same API, with the OUTBOX pattern (Lesson 05, Q6).

POST /orders no longer talks to Kafka at all. It writes the event into a local SQLite table (the outbox)
and answers 201 as soon as that DB commit succeeds. A background relay thread publishes outbox rows
to Kafka and marks them as published.

  - Kafka down?  POST still works; rows wait in the outbox and go out when Kafka is back.
  - API killed?  Unpublished rows are still in the DB; the relay sends them after the restart.
  - Relay crashes between "Kafka acked" and "marked published"?  The row is sent AGAIN after the
    restart: the outbox is at-least-once. Fine, because every consumer downstream is idempotent,
    and the payload (incl. event_id) is byte-for-byte the same.

Run it INSTEAD of api.py:  python api_outbox.py
"""

import json
import logging
import sqlite3
import threading
from contextlib import asynccontextmanager, closing
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid4, uuid5

import config
from api import CreateOrderRequest, get_order
from confluent_kafka import Producer
from events import OrderPlaced
from fastapi import FastAPI, Header

# force=True: importing api.py above already configured logging with its own format
logging.basicConfig(level=logging.INFO, format="%(asctime)s [api-outbox] %(message)s", force=True)
log = logging.getLogger("api-outbox")

BATCH_SIZE = 500


def connect():
    db = sqlite3.connect(config.API_DB, timeout=10)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute(
        "CREATE TABLE IF NOT EXISTS outbox ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"  # publish order = insert order
        " order_id TEXT UNIQUE NOT NULL,"  # a retried request can't create a second row
        " topic TEXT NOT NULL, key TEXT NOT NULL, payload TEXT NOT NULL, headers TEXT NOT NULL,"
        " created_at TEXT NOT NULL, published_at TEXT)"
    )
    db.execute("CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL")
    return db


def _record_into(results: dict, row_id: int):
    """A delivery callback that stores the outcome of one outbox row."""

    def on_delivery(err, _msg):
        results[row_id] = err

    return on_delivery


class OutboxRelay(threading.Thread):
    """Moves rows from the outbox to Kafka, oldest first."""

    def __init__(self):
        super().__init__(daemon=True)
        self.stop = threading.Event()
        self.producer = Producer(
            {
                "bootstrap.servers": config.BOOTSTRAP_SERVERS,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 5,
                "delivery.timeout.ms": 10_000,
            }
        )

    def run(self):
        db = connect()  # SQLite connections are per thread
        while not self.stop.is_set():
            rows = db.execute(
                "SELECT id, topic, key, payload, headers FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT ?",
                (BATCH_SIZE,),
            ).fetchall()
            if not rows:
                self.stop.wait(0.2)
                continue

            results = {}  # row id -> delivery error (None = delivered)
            for row_id, topic, key, payload, headers in rows:
                self.producer.produce(
                    topic, key=key.encode(), value=payload.encode(), headers=json.loads(headers),
                    on_delivery=_record_into(results, row_id),
                )
            self.producer.flush(15)  # runs the callbacks

            # Mark only the LEADING run of delivered rows as published. If row 7 failed, rows 8+ are
            # resent with it next round, so a later event can never overtake an earlier one.
            done = []
            for row_id, *_ in rows:
                if row_id not in results or results[row_id] is not None:
                    break
                done.append(row_id)
            if done:
                with db:
                    db.executemany(
                        "UPDATE outbox SET published_at = ? WHERE id = ?",
                        [(datetime.now(UTC).isoformat(), row_id) for row_id in done],
                    )
            if len(done) < len(rows):
                error = results.get(rows[len(done)][0], "no delivery report")
                log.warning("published %d/%d outbox rows (%s), retrying in 1 s", len(done), len(rows), error)
                self.stop.wait(1)
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    connect().close()  # create the table before the first request
    relay = OutboxRelay()
    relay.start()
    yield
    relay.stop.set()
    relay.join()
    relay.producer.flush(10)


app = FastAPI(title="Shop API (outbox)", lifespan=lifespan)


@app.post("/orders", status_code=201)
def create_order(body: CreateOrderRequest, idempotency_key: str | None = Header(default=None)):
    if idempotency_key:
        order_id = str(uuid5(NAMESPACE_URL, f"order:{body.customer_id}:{idempotency_key}"))
    else:
        order_id = str(uuid4())

    event = OrderPlaced(
        order_id=order_id,
        customer_id=body.customer_id,
        items=body.items,
        total_cents=sum(i.qty * i.unit_price_cents for i in body.items),
        card_last4=body.card_last4,
        currency=body.currency,
    )
    headers = {"event_type": event.event_type, "schema_version": "2"}

    with closing(connect()) as db, db:
        cursor = db.execute(
            "INSERT OR IGNORE INTO outbox (order_id, topic, key, payload, headers, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (order_id, config.ORDERS_TOPIC, order_id, event.model_dump_json(), json.dumps(headers),
             datetime.now(UTC).isoformat()),
        )
    # rowcount 0 = this order_id is already in the outbox: the client retried. Nothing new is sent.
    return {"order_id": order_id, "status": "pending", "duplicate_request": cursor.rowcount == 0}


@app.get("/outbox")
def outbox_stats():
    """How far behind is the relay? (Like consumer lag, but for the API -> Kafka hop.)"""
    with closing(connect()) as db:
        pending, oldest = db.execute(
            "SELECT COUNT(*), MIN(created_at) FROM outbox WHERE published_at IS NULL"
        ).fetchone()
        published = db.execute("SELECT COUNT(*) FROM outbox WHERE published_at IS NOT NULL").fetchone()[0]
    return {"unpublished": pending, "oldest_unpublished_at": oldest, "published": published}


app.get("/orders/{order_id}")(get_order)  # same read model as api.py


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
