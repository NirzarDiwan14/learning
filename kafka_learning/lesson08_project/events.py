from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field


def _new_id() -> str:
    return str(uuid4())


def _now() -> datetime:
    return datetime.now(UTC)


class OrderItem(BaseModel):
    sku: str
    qty: int = Field(gt=0)
    unit_price_cents: int = Field(gt=0)


class OrderPlaced(BaseModel):
    event_id: str = Field(default_factory=_new_id)
    event_type: Literal["order_placed"] = "order_placed"
    order_id: str
    customer_id: str
    items: list[OrderItem] = Field(min_length=1)
    total_cents: int = Field(gt=0)
    card_last4: str = Field(pattern=r"^\d{4}$")
    created_at: datetime = Field(default_factory=_now)


class PaymentResult(BaseModel):
    event_id: str = Field(default_factory=_new_id)
    event_type: Literal["payment_succeeded", "payment_failed"]
    order_id: str
    amount_cents: int
    reason: str | None = None  # e.g. "card_declined"
    created_at: datetime = Field(default_factory=_now)
