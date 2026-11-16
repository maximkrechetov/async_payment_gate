import hashlib
import hmac
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select, text

from app.core.config import GatewaySettings, WebhookSettings, settings
from app.core.exceptions import GatewayUnavailable
from app.db.models import Outbox, OutboxStatus, Payment, PaymentStatus, WebhookStatus
from app.schemas.events import PaymentCreatedEvent
from app.schemas.payments import PaymentCreate
from app.services.gateway import PaymentGatewayEmulator
from app.services.payments import PaymentService
from app.services.processing import PaymentProcessor
from app.services.webhook import WebhookNotifier

pytestmark = pytest.mark.integration

HEADERS = {"X-API-Key": settings.api_key}


def body(**kw) -> dict[str, Any]:
    return {
        "amount": "100.00",
        "currency": "RUB",
        "description": "test",
        "metadata": {},
        "webhook_url": "http://receiver.test/hook",
        **kw,
    }


def gateway() -> PaymentGatewayEmulator:
    return PaymentGatewayEmulator(GatewaySettings(min_delay=0.0, max_delay=0.0))


@asynccontextmanager
async def notifier(handler):
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        yield WebhookNotifier(client, WebhookSettings(base_delay=0.0, max_attempts=3, timeout=1.0))
    finally:
        await client.aclose()


def ok(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200)


async def make_payment(session, **kw) -> Payment:
    result = await PaymentService(session).create(PaymentCreate(**body(**kw)), str(uuid4()))
    return result.payment


async def test_create_202_and_outbox_row(api, session) -> None:
    resp = await api.post("/api/v1/payments", json=body(), headers={**HEADERS, "Idempotency-Key": "k1"})
    assert resp.status_code == 202
    payment_id = resp.json()["payment_id"]

    event = await session.scalar(select(Outbox).where(Outbox.payment_id == payment_id))
    assert event is not None
    assert event.status is OutboxStatus.PENDING
    assert event.payload["payment_id"] == payment_id


async def test_idempotent_replay(api) -> None:
    headers = {**HEADERS, "Idempotency-Key": "same"}
    first = await api.post("/api/v1/payments", json=body(), headers=headers)
    second = await api.post("/api/v1/payments", json=body(), headers=headers)
    assert first.json()["payment_id"] == second.json()["payment_id"]
    assert second.headers["X-Idempotent-Replay"] == "true"


async def test_get_payment(api) -> None:
    created = await api.post("/api/v1/payments", json=body(), headers={**HEADERS, "Idempotency-Key": "g1"})
    pid = created.json()["payment_id"]
    got = await api.get(f"/api/v1/payments/{pid}", headers=HEADERS)
    assert got.status_code == 200
    assert got.json()["amount"] == "100.00"
    assert got.json()["metadata"] == {}
    assert (await api.get(f"/api/v1/payments/{uuid4()}", headers=HEADERS)).status_code == 404


async def test_outbox_rollback_leaves_no_payment(session, monkeypatch) -> None:
    def boom(self, event):
        raise RuntimeError("outbox insert failed")

    monkeypatch.setattr("app.repositories.outbox.OutboxRepository.add", boom)
    with pytest.raises(RuntimeError):
        await PaymentService(session).create(PaymentCreate(**body()), "atomic")
    await session.rollback()

    assert await session.scalar(select(func.count()).select_from(Payment)) == 0
    assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_process_success(session) -> None:
    payment = await make_payment(session, metadata={"force": "succeed"})
    async with notifier(ok) as n:
        await PaymentProcessor(session, gateway(), n).handle(PaymentCreatedEvent(payment_id=payment.id))
    await session.refresh(payment)
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.webhook_status is WebhookStatus.DELIVERED
    assert payment.webhook_attempts == 1
    assert payment.processed_at is not None


async def test_process_decline_is_not_retried(session) -> None:
    payment = await make_payment(session, metadata={"force": "decline"})
    async with notifier(ok) as n:
        await PaymentProcessor(session, gateway(), n).handle(PaymentCreatedEvent(payment_id=payment.id))
    await session.refresh(payment)
    assert payment.status is PaymentStatus.FAILED
    assert payment.webhook_status is WebhookStatus.DELIVERED


async def test_process_technical_error_keeps_pending(session) -> None:
    payment = await make_payment(session, metadata={"force": "crash"})
    async with notifier(ok) as n:
        with pytest.raises(GatewayUnavailable):
            await PaymentProcessor(session, gateway(), n).handle(PaymentCreatedEvent(payment_id=payment.id))
    await session.refresh(payment)
    assert payment.status is PaymentStatus.PENDING


async def test_consumer_is_idempotent(session) -> None:
    payment = await make_payment(session, metadata={"force": "succeed"})
    calls = []

    async def counting_handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url)
        return httpx.Response(200)

    async with notifier(counting_handler) as n:
        processor = PaymentProcessor(session, gateway(), n)
        event = PaymentCreatedEvent(payment_id=payment.id)
        await processor.handle(event)
        await processor.handle(event)
    assert len(calls) == 1
    await session.refresh(payment)
    assert payment.attempts == 1


async def test_webhook_retries_then_delivers(session) -> None:
    payment = await make_payment(session, metadata={"force": "succeed"})
    attempts = []

    def flaky(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        return httpx.Response(500 if len(attempts) < 3 else 200)

    async with notifier(flaky) as n:
        await PaymentProcessor(session, gateway(), n).handle(PaymentCreatedEvent(payment_id=payment.id))
    await session.refresh(payment)
    assert len(attempts) == 3
    assert payment.webhook_status is WebhookStatus.DELIVERED
    assert payment.webhook_attempts == 3


async def test_webhook_failure_does_not_rollback_payment(session) -> None:
    payment = await make_payment(session, metadata={"force": "succeed"})
    async with notifier(lambda r: httpx.Response(500)) as n:
        await PaymentProcessor(session, gateway(), n).handle(PaymentCreatedEvent(payment_id=payment.id))
    await session.refresh(payment)
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.webhook_status is WebhookStatus.FAILED
    assert payment.webhook_error


async def test_webhook_signature(session) -> None:

    payment = await make_payment(session, metadata={"force": "succeed"})
    signature = ""
    raw_body = b""

    def capture(request: httpx.Request) -> httpx.Response:
        nonlocal signature, raw_body
        signature = request.headers["X-Signature"]
        raw_body = request.content
        return httpx.Response(200)

    async with notifier(capture) as n:
        await PaymentProcessor(session, gateway(), n).handle(PaymentCreatedEvent(payment_id=payment.id))

    expected = hmac.new(settings.webhook.secret.encode(), raw_body, hashlib.sha256).hexdigest()
    assert signature == f"sha256={expected}"


async def test_enum_schema_parity(session) -> None:
    from sqlalchemy.dialects.postgresql import ENUM as PgEnum

    from app.db.base import Base

    for table in Base.metadata.tables.values():
        for column in table.columns:
            if isinstance(column.type, PgEnum):
                udt = await session.scalar(
                    text("SELECT udt_name FROM information_schema.columns WHERE table_name = :t AND column_name = :c"),
                    {"t": table.name, "c": column.name},
                )
                assert udt == column.type.name, f"{table.name}.{column.name}: {udt}"
