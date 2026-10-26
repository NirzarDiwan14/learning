from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field


# The "contract" between producer and consumer.
# Kafka itself only stores bytes; this model decides what those bytes mean.
class OrderEvent(BaseModel):
    # Unique per event: the consumer can use it for idempotency (Lesson 03, Q5)
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: Literal["order_created", "order_paid", "order_shipped"]
    order_id: str
    user_id: str
    # Money as integer cents, never float (0.1 + 0.2 != 0.3)
    amount_cents: int
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
