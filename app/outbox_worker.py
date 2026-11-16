import asyncio
import logging
import signal
from contextlib import suppress
from datetime import UTC, datetime, timedelta

from app.broker.core import broker
from app.broker.topology import PAYMENTS_EXCHANGE, ensure_topology
from app.core.config import settings
from app.core.retry import exponential_delay
from app.db.models import OutboxStatus
from app.db.session import SessionFactory, engine
from app.repositories.outbox import OutboxRepository
from app.schemas.events import PaymentCreatedEvent

logger = logging.getLogger(__name__)


async def publish_batch() -> int:
    async with SessionFactory() as session, session.begin():
        rows = await OutboxRepository(session).lock_pending(settings.outbox.batch_size)
        for row in rows:
            try:
                event = PaymentCreatedEvent.model_validate(row.payload)
                await broker.publish(
                    event,
                    exchange=PAYMENTS_EXCHANGE,
                    routing_key=row.routing_key,
                    persist=True,
                    message_id=str(row.id),
                )
            except Exception as exc:
                delay = exponential_delay(row.attempts + 1, settings.outbox.base_delay, settings.outbox.max_delay)
                row.attempts += 1
                row.last_error = f"{type(exc).__name__}: {exc}"[:2000]
                row.available_at = datetime.now(UTC) + timedelta(seconds=delay)
                logger.warning("outbox %s: publish failed, retry after %.1fs", row.id, delay)
            else:
                row.status = OutboxStatus.PUBLISHED
                row.published_at = datetime.now(UTC)
                row.last_error = None
        return len(rows)


async def run(stop: asyncio.Event) -> None:
    logger.info(f"Outbox relay has started (batch={settings.outbox.batch_size})")
    while not stop.is_set():
        try:
            published = await publish_batch()
        except Exception:
            logger.exception("Outbox error")
            published = 0

        if published == 0:
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=settings.outbox.poll_interval)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await ensure_topology()
    try:
        async with broker:
            await run(stop)
    finally:
        await engine.dispose()
        logger.info("Outbox relay has stopped")


if __name__ == "__main__":
    asyncio.run(main())
