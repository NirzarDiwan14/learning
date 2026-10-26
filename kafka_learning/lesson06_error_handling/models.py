from typing import Literal

from pydantic import BaseModel, Field


class PaymentRequest(BaseModel):
    payment_id: str
    order_id: str
    amount_cents: int = Field(gt=0)  # business rule: must be positive
    currency: Literal["INR", "USD", "EUR"]
