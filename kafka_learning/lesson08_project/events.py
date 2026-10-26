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
    # v2 (Exercise B): NEW field WITH a default -> compatible both ways:
    #  - old consumers ignore it (pydantic extra="ignore")
    #  - new consumers reading old v1 events get "INR"
    # Plain str, not Literal: if a future producer adds "GBP", consumers must not crash on it.
    # The API validates the allowed values at the edge instead.
    currency: str = "INR"


class PaymentResult(BaseModel):
    event_id: str = Field(default_factory=_new_id)
    event_type: Literal["payment_succeeded", "payment_failed"]
    order_id: str
    amount_cents: int
    reason: str | None = None  # e.g. "card_declined"
    created_at: datetime = Field(default_factory=_now)
    currency: str = "INR"  # v2


# Exercise A: produced by shipping_service.py for every successful payment
class ShipmentRequested(BaseModel):
    event_id: str = Field(default_factory=_new_id)
    event_type: Literal["shipment_requested"] = "shipment_requested"
    order_id: str
    created_at: datetime = Field(default_factory=_now)
