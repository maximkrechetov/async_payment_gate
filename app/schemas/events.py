from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from app.db.models import Currency, PaymentStatus


class PaymentCreatedEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    payment_id: UUID
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    attempt: int = 1


class PaymentWebhookPayload(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    idempotency_key: str
    processed_at: datetime | None
