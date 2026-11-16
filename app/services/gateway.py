import asyncio
import logging
import random
from dataclasses import dataclass
from functools import lru_cache

from app.core.config import GatewaySettings, settings
from app.core.exceptions import GatewayUnavailable
from app.db.models import Payment

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ChargeResult:
    succeeded: bool
    detail: str


class PaymentGatewayEmulator:
    def __init__(self, cfg: GatewaySettings) -> None:
        self._cfg = cfg

    async def charge(self, payment: Payment) -> ChargeResult:
        await asyncio.sleep(random.uniform(self._cfg.min_delay, self._cfg.max_delay))

        force = (payment.meta or {}).get("force")
        if force == "crash":
            raise GatewayUnavailable("Gateway unavailable")
        if force == "decline":
            return ChargeResult(succeeded=False, detail="GatewayError (force=decline)")
        if force == "succeed":
            return ChargeResult(succeeded=True, detail="OK (force=succeed)")

        if random.random() < self._cfg.failure_rate:
            return ChargeResult(succeeded=False, detail="Insufficient funds")
        return ChargeResult(succeeded=True, detail="OK")


@lru_cache
def get_gateway() -> PaymentGatewayEmulator:
    return PaymentGatewayEmulator(settings.gateway)
