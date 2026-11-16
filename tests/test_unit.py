from decimal import Decimal
from typing import cast

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.retry import exponential_delay
from app.db import models  # noqa: F401
from app.main import create_app
from app.schemas.events import PaymentCreatedEvent
from app.schemas.payments import PaymentCreate

BODY = {
    "amount": "10.5",
    "currency": "RUB",
    "metadata": {"a": 1, "b": 2},
    "webhook_url": "http://example.test/hook",
}


@pytest_asyncio.fixture
async def bare_client():
    async with AsyncClient(transport=ASGITransport(app=create_app()), base_url="http://test") as c:
        yield c


def test_amount_quantized() -> None:
    assert PaymentCreate(**BODY).amount == Decimal("10.50")


@pytest.mark.parametrize("amount", ["0", "-1", "1.005"])
def test_invalid_amount(amount: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**{**BODY, "amount": amount})


def test_invalid_currency() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**{**BODY, "currency": "GBP"})


def test_exponential_backoff() -> None:
    assert [exponential_delay(i, 2.0) for i in (1, 2, 3)] == [2.0, 4.0, 8.0]
    assert exponential_delay(10, 1.0, cap=60.0) == 60.0


async def test_api_key_required(bare_client: AsyncClient) -> None:
    assert (await bare_client.post("/api/v1/payments", json=BODY)).status_code == 401


async def test_idempotency_key_required(bare_client: AsyncClient) -> None:
    resp = await bare_client.post("/api/v1/payments", json=BODY, headers={"X-API-Key": settings.api_key})
    assert resp.status_code == 422


class _Boom:
    def __init__(self) -> None:
        self.marked = False

    async def handle(self, event):
        raise RuntimeError("technical failure")

    async def mark_technical_failure(self, event, exc):
        self.marked = True


@pytest.mark.parametrize(("attempt", "should_retry"), [(1, True), (2, True), (3, False)])
async def test_handler_retry_policy(monkeypatch, attempt: int, should_retry: bool) -> None:
    from app.broker import handlers

    boom, calls = _Boom(), []

    class _Publisher:
        async def schedule_retry(self, event):
            calls.append(event.attempt)

    monkeypatch.setattr(handlers, "PaymentProcessor", lambda *a, **kw: boom)
    monkeypatch.setattr(handlers, "publisher", _Publisher())
    event = PaymentCreatedEvent(payment_id=__import__("uuid").uuid4(), attempt=attempt)

    if should_retry:
        # сессия не используется: PaymentProcessor подменён заглушкой
        await handlers.handle_payment_created(event, session=cast(AsyncSession, None))
        assert calls == [attempt]
        assert not boom.marked
    else:
        with pytest.raises(RuntimeError):
            await handlers.handle_payment_created(event, session=cast(AsyncSession, None))
        assert calls == []
        assert boom.marked
