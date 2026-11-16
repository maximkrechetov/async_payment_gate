import logging

from faststream import FastStream

from app.broker.core import broker
from app.broker.handlers import router
from app.broker.topology import ensure_topology
from app.db.session import engine
from app.services.webhook import get_webhook_notifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

broker.include_router(router)
app = FastStream(broker)


@app.on_startup
async def _prepare() -> None:
    await ensure_topology()


@app.after_shutdown
async def _cleanup() -> None:
    await get_webhook_notifier().aclose()
    await engine.dispose()
