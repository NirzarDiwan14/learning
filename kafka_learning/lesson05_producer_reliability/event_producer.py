
import logging
from contextlib import asynccontextmanager

from confluent_kafka import KafkaError, Producer
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# Pydantic Event Model
# --------------------------------------------------

class OrderEvent(BaseModel):
    order_id: str
    customer_id: str
    amount: float = Field(gt=0)
    status: str = "created"


# --------------------------------------------------
# Reusable Kafka Event Producer
# --------------------------------------------------

class EventProducer:
    def __init__(self, bootstrap_servers: str):
        self._failed_deliveries = 0

        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
            "compression.type": "snappy",
        })

    def send(
        self,
        topic: str,
        key: str,
        event: BaseModel,
        headers: dict | None = None,
    ) -> None:
        payload = event.model_dump_json().encode("utf-8")

        kafka_headers = (
            [(k, str(v).encode("utf-8")) for k, v in headers.items()]
            if headers
            else None
        )

        def delivery_callback(err, msg):
            if err is not None:
                self._failed_deliveries += 1
                logger.error(
                    "Delivery failed: topic=%s key=%s error=%s",
                    topic,
                    key,
                    err,
                )
            else:
                logger.info(
                    "Delivered: topic=%s key=%s partition=%s offset=%s",
                    msg.topic(),
                    key,
                    msg.partition(),
                    msg.offset(),
                )

        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=payload,
                    headers=kafka_headers,
                    callback=delivery_callback,
                )
                self._producer.poll(0)
                return

            except BufferError:
                logger.warning(
                    "Producer queue full; waiting for space: topic=%s key=%s",
                    topic,
                    key,
                )
                self._producer.poll(0.1)

    def close(self, timeout: float = 10) -> int:
        remaining = self._producer.flush(timeout)

        if remaining:
            logger.warning(
                "%d messages remain undelivered after flush",
                remaining,
            )
        else:
            logger.info("Kafka producer flushed successfully")

        return remaining


# --------------------------------------------------
# FastAPI Application Lifecycle
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "order-events"


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.event_producer = EventProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
    )

    logger.info("Kafka producer initialized")

    yield

    remaining = app.state.event_producer.close(timeout=10)
    logger.info("Shutdown complete; remaining=%d", remaining)


app = FastAPI(
    title="Order Event API",
    lifespan=lifespan,
)


# --------------------------------------------------
# API Endpoints
# --------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders", status_code=202)
def create_order(order: OrderEvent, request: Request):
    producer = request.app.state.event_producer
    

    try:
        producer.send(
            topic=KAFKA_TOPIC,
            key=order.order_id,
            event=order,
            headers={"event_type": "order"},
        )

    except Exception as exc:
        logger.exception(
            "Unable to enqueue order event: order_id=%s",
            order.order_id,
        )
        raise HTTPException(
            status_code=503,
            detail="Unable to enqueue order event",
        ) from exc

    return {
        "message": "Order event submitted to Kafka",
        "order_id": order.order_id,
        "topic": KAFKA_TOPIC,
    }


# --------------------------------------------------
# Run Directly
# --------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
    )
