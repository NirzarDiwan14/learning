import logging
import sqlite3
from contextlib import asynccontextmanager, closing
from uuid import NAMESPACE_URL, uuid4, uuid5

import config
from confluent_kafka import KafkaException
from events import OrderItem, OrderPlaced
from fastapi import FastAPI, Header, HTTPException
from kafka_utils import EventProducer
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [api] %(message)s")
log = logging.getLogger("api")


# What CLIENTS send. Separate from the event: the server adds order_id, event_id, total, timestamps.
class CreateOrderRequest(BaseModel):
    customer_id: str
    items: list[OrderItem] = Field(min_length=1)
    card_last4: str = Field(pattern=r"^\d{4}$")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ONE producer for the whole app (Lesson 05, Q7). Give up on a delivery after 5 s.
    app.state.producer = EventProducer(delivery_timeout_ms=5_000)
    yield
    app.state.producer.close()


app = FastAPI(title="Shop API", lifespan=lifespan)


@app.post("/orders", status_code=201)
async def create_order(body: CreateOrderRequest, idempotency_key: str | None = Header(default=None)):
    # Same Idempotency-Key from the same customer -> same order_id -> the retry is harmless downstream
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
    )

    try:
        # Wait for Kafka's confirmation BEFORE answering: a 201 means the order is durably stored
        partition, offset = await app.state.producer.send_async(
            config.ORDERS_TOPIC,
            order_id,
            event,
            headers={"event_type": event.event_type, "schema_version": "1"},
            timeout=10,
        )
    except (BufferError, KafkaException, TimeoutError) as e:
        log.error("could not publish order %s: %s", order_id, e)
        raise HTTPException(status_code=503, detail="Could not place the order right now, please retry") from e

    return {"order_id": order_id, "status": "pending", "kafka": {"partition": partition, "offset": offset}}


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    row = None
    try:
        with closing(sqlite3.connect(f"file:{config.ORDERS_DB}?mode=ro", uri=True)) as db:
            db.row_factory = sqlite3.Row
            row = db.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()
    except sqlite3.OperationalError:  # the projector hasn't created the database yet
        pass
    if row is None:
        # Eventual consistency: the order may be in Kafka but not projected yet
        raise HTTPException(status_code=404, detail="Unknown order (or not processed yet, try again shortly)")
    return dict(row)


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
