import logging
from datetime import timedelta

from faststream.rabbit import RabbitBroker

from app.broker.topology import RETRY_EXCHANGE, RETRY_RK
from app.core.config import settings
from app.core.retry import exponential_delay
from app.schemas.events import PaymentCreatedEvent

logger = logging.getLogger(__name__)


class PaymentEventPublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def schedule_retry(self, event: PaymentCreatedEvent) -> None:
        delay = exponential_delay(event.attempt, settings.retry.base_delay)
        await self._broker.publish(
            event.model_copy(update={"attempt": event.attempt + 1}),
            exchange=RETRY_EXCHANGE,
            routing_key=RETRY_RK,
            persist=True,
            expiration=timedelta(seconds=delay),
            message_id=str(event.event_id),
        )
        logger.info(
            "Payment %s: retry %s/%s after %.1fs",
            event.payment_id,
            event.attempt + 1,
            settings.retry.max_attempts,
            delay,
        )
