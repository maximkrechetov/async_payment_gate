import logging
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.topology import PAYMENTS_NEW_RK
from app.core.exceptions import NotFoundError
from app.db.models import Outbox, Payment
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas.events import PaymentCreatedEvent
from app.schemas.payments import PaymentCreate

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CreateResult:
    payment: Payment
    replayed: bool


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create(self, data: PaymentCreate, idempotency_key: str) -> CreateResult:
        payment = Payment(
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            meta=data.metadata,
            webhook_url=str(data.webhook_url),
            idempotency_key=idempotency_key,
        )
        self._payments.add(payment)

        try:
            await self._session.flush()
            event = PaymentCreatedEvent(payment_id=payment.id)
            self._outbox.add(
                Outbox(
                    event_type="payment.created",
                    routing_key=PAYMENTS_NEW_RK,
                    payment_id=payment.id,
                    payload=event.model_dump(mode="json"),
                )
            )
            await self._session.commit()
        except IntegrityError:
            await self._session.rollback()
            existing = await self._payments.get_by_idempotency_key(idempotency_key)
            if existing is None:
                raise
            logger.info("Idempotency race resolved: %s", idempotency_key)
            return CreateResult(payment=existing, replayed=True)

        return CreateResult(payment=payment, replayed=False)

    async def get(self, payment_id: UUID) -> Payment:
        payment = await self._payments.get(payment_id)
        if payment is None:
            raise NotFoundError(f"Payment {payment_id} not found")
        return payment
