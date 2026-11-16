import asyncio
import json
from datetime import timedelta
from uuid import uuid4

import aio_pika
import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy import select

from app.broker.topology import (
    DLQ,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_NEW_RK,
    RETRY_EXCHANGE,
    RETRY_RK,
)
from app.core.config import settings
from app.db.models import Outbox, OutboxStatus
from app.outbox_worker import publish_batch
from app.schemas.events import PaymentCreatedEvent
from app.schemas.payments import PaymentCreate
from app.services.payments import PaymentService

pytestmark = pytest.mark.integration


async def wait_message(channel, queue_name: str):
    queue = await channel.declare_queue(queue_name, durable=True, passive=True)
    future = asyncio.get_event_loop().create_future()

    async def on_message(message):
        if not future.done():
            future.set_result(message)

    consumer_tag = await queue.consume(on_message)
    try:
        return await future
    finally:
        await queue.cancel(consumer_tag)


@pytest.fixture
async def channel(rabbit):
    conn = await aio_pika.connect_robust(rabbit)
    try:
        yield await conn.channel()
    finally:
        await conn.close()


async def test_retry_flow(rabbit, channel) -> None:
    from uuid import uuid4

    event = PaymentCreatedEvent(payment_id=uuid4(), attempt=2)
    message = aio_pika.Message(
        body=event.model_dump_json().encode(),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        expiration=timedelta(seconds=settings.retry.base_delay),
        message_id=str(event.event_id),
    )
    exchange = await channel.declare_exchange(
        RETRY_EXCHANGE.name,
        type=aio_pika.ExchangeType.DIRECT,
        durable=True,
    )
    await exchange.publish(message, routing_key=RETRY_RK)

    async with asyncio.timeout(8):
        returned = await wait_message(channel, PAYMENTS_NEW_QUEUE.name)

    body = json.loads(returned.body)

    assert body["attempt"] == 2
    assert body["payment_id"] == str(event.payment_id)
    await returned.ack()


async def test_reject_lands_in_dlq(rabbit, channel) -> None:
    event = PaymentCreatedEvent(payment_id=uuid4(), attempt=3)
    exchange = await channel.get_exchange(PAYMENTS_EXCHANGE.name)
    await exchange.publish(
        aio_pika.Message(
            body=event.model_dump_json().encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=str(event.event_id),
        ),
        routing_key=PAYMENTS_NEW_RK,
    )

    async with asyncio.timeout(8):
        message = await wait_message(channel, PAYMENTS_NEW_QUEUE.name)
    await message.reject(requeue=False)

    async with asyncio.timeout(8):
        dead = await wait_message(channel, DLQ.name)
    assert json.loads(dead.body)["payment_id"] == json.loads(message.body)["payment_id"]
    await dead.ack()


async def test_outbox_relay_publishes_and_marks(rabbit, channel, session_factory, monkeypatch):
    async with session_factory() as session:
        result = await PaymentService(session).create(
            PaymentCreate(
                amount="10.00",
                currency="EUR",
                metadata={},
                webhook_url="http://receiver.test/hook",
            ),
            "relay-1",
        )
        payment_id = result.payment.id

    test_broker = RabbitBroker(rabbit)
    async with test_broker:
        monkeypatch.setattr("app.outbox_worker.broker", test_broker)
        from app.outbox_worker import publish_batch

        assert await publish_batch() == 1

    async with asyncio.timeout(8):
        message = await wait_message(channel, PAYMENTS_NEW_QUEUE.name)
    assert json.loads(message.body)["payment_id"] == str(payment_id)
    await message.ack()

    async with session_factory() as session:
        row = await session.scalar(select(Outbox).where(Outbox.payment_id == payment_id))
        assert row.status is OutboxStatus.PUBLISHED
        assert row.published_at is not None


async def test_relay_keeps_event_when_broker_down(rabbit, session_factory, monkeypatch) -> None:
    async with session_factory() as session:
        result = await PaymentService(session).create(
            PaymentCreate(
                amount="10.00",
                currency="USD",
                metadata={},
                webhook_url="http://receiver.test/hook",
            ),
            "relay-down",
        )
        payment_id = result.payment.id

    class Broken:
        async def publish(self, *a, **kw):
            raise ConnectionError("broker unavailable")

    monkeypatch.setattr("app.outbox_worker.broker", Broken())

    assert await publish_batch() == 1

    async with session_factory() as session:
        row = await session.scalar(select(Outbox).where(Outbox.payment_id == payment_id))
        assert row.status is OutboxStatus.PENDING
        assert row.attempts == 1
        assert row.last_error
