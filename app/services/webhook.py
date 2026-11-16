import hashlib
import hmac
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache

import httpx

from app.core.config import WebhookSettings, settings
from app.core.retry import exponential_delay
from app.db.models import Payment
from app.schemas.events import PaymentWebhookPayload

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DeliveryResult:
    delivered: bool
    attempts: int
    error: str | None
    delivered_at: datetime | None


class WebhookNotifier:
    def __init__(self, client: httpx.AsyncClient, cfg: WebhookSettings) -> None:
        self._client = client
        self._cfg = cfg

    async def send(self, payment: Payment) -> DeliveryResult:
        payload = PaymentWebhookPayload(
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            idempotency_key=payment.idempotency_key,
            processed_at=payment.processed_at,
        )
        body = json.dumps(payload.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":")).encode()
        signature = hmac.new(self._cfg.secret.encode(), body, hashlib.sha256).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "X-Signature": f"sha256={signature}",
            "X-Payment-Id": str(payment.id),
        }

        last_error: str | None = None
        for attempt in range(1, self._cfg.max_attempts + 1):
            try:
                response = await self._client.post(payment.webhook_url, content=body, headers=headers)
                response.raise_for_status()
            except httpx.HTTPError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "webhook %s: attempt %s/%s is failed  (%s)",
                    payment.id,
                    attempt,
                    self._cfg.max_attempts,
                    last_error,
                )
                if attempt < self._cfg.max_attempts:
                    await asyncio_sleep(exponential_delay(attempt, self._cfg.base_delay))
            else:
                return DeliveryResult(True, attempt, None, datetime.now(UTC))

        return DeliveryResult(False, self._cfg.max_attempts, last_error, None)

    async def aclose(self) -> None:
        await self._client.aclose()


async def asyncio_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


@lru_cache
def get_webhook_notifier() -> WebhookNotifier:
    client = httpx.AsyncClient(timeout=httpx.Timeout(settings.webhook.timeout))
    return WebhookNotifier(client, settings.webhook)
