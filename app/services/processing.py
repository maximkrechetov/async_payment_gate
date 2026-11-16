import logging
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Payment, PaymentStatus, WebhookStatus
from app.schemas.events import PaymentCreatedEvent
from app.services.gateway import PaymentGatewayEmulator
from app.services.webhook import WebhookNotifier

logger = logging.getLogger(__name__)


class PaymentProcessor:
    def __init__(
        self,
        session: AsyncSession,
        gateway: PaymentGatewayEmulator,
        notifier: WebhookNotifier,
    ) -> None:
        self._session = session
        self._gateway = gateway
        self._notifier = notifier

    async def handle(self, event: PaymentCreatedEvent) -> None:
        payment = await self._session.get(Payment, event.payment_id)
        if payment is None:
            logger.warning(f"Payment {event.payment_id} not found")
            return

        if payment.status is not PaymentStatus.PENDING:
            logger.info(f"Payment {payment.id} already processed ({payment.status}) — skip")
            return

        payment.attempts = event.attempt
        await self._session.commit()

        result = await self._gateway.charge(payment)

        payment.status = PaymentStatus.SUCCEEDED if result.succeeded else PaymentStatus.FAILED
        payment.error = None if result.succeeded else result.detail
        payment.processed_at = datetime.now(UTC)
        await self._session.commit()
        logger.info(f"Payment {payment.id}: {payment.status} ({event.attempt} attempt)")

        delivery = await self._notifier.send(payment)
        payment.webhook_status = WebhookStatus.DELIVERED if delivery.delivered else WebhookStatus.FAILED
        payment.webhook_attempts = delivery.attempts
        payment.webhook_error = delivery.error
        payment.webhook_delivered_at = delivery.delivered_at
        await self._session.commit()

    async def mark_technical_failure(self, event: PaymentCreatedEvent, exc: Exception) -> None:
        try:
            payment = await self._session.get(Payment, event.payment_id)
            if payment is not None and payment.status is PaymentStatus.PENDING:
                payment.status = PaymentStatus.FAILED
                payment.error = f"Processing failed after {event.attempt} attempts: {exc}"
                payment.processed_at = datetime.now(UTC)
            await self._session.commit()
        except Exception:
            logger.exception("Unexpected error, rollback transaction")
            await self._session.rollback()
