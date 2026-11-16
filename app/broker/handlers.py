import logging
from typing import Annotated

from faststream import Depends
from faststream.rabbit import RabbitRouter
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.core import broker
from app.broker.publisher import PaymentEventPublisher
from app.broker.topology import PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE
from app.core.config import settings
from app.db.session import get_session
from app.schemas.events import PaymentCreatedEvent
from app.services.gateway import get_gateway
from app.services.processing import PaymentProcessor
from app.services.webhook import get_webhook_notifier

logger = logging.getLogger(__name__)
router = RabbitRouter()
publisher = PaymentEventPublisher(broker)


@router.subscriber(PAYMENTS_NEW_QUEUE, exchange=PAYMENTS_EXCHANGE)
async def handle_payment_created(
    event: PaymentCreatedEvent,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> None:
    processor = PaymentProcessor(session, get_gateway(), get_webhook_notifier())
    try:
        await processor.handle(event)
    except Exception as exc:
        if event.attempt < settings.retry.max_attempts:
            await publisher.schedule_retry(event)
            return

        logger.error(
            f"Payment {event.payment_id}: all {settings.retry.max_attempts} attempts failed. Reason: {exc}",
        )
        await processor.mark_technical_failure(event, exc)
        raise
