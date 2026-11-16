import asyncio
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from alembic.config import Config
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy import func, select, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer

from alembic import command
from app.core.config import settings
from app.db.models import (
    Currency,
    Outbox,
    OutboxStatus,
    Payment,
    PaymentStatus,
    WebhookStatus,
)
from app.db.session import get_session
from app.main import create_app

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[1]
API = f"{settings.api_prefix}/payments"


@pytest.fixture(scope="session")
def api_pg_dsn():
    keys = (
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB",
    )
    old_env = {key: os.environ.get(key) for key in keys}

    with PostgresContainer("postgres:18-alpine") as pg:
        url = make_url(pg.get_connection_url()).set(drivername="postgresql+asyncpg")

        host, port = url.host, url.port
        user, password, database = url.username, url.password, url.database
        assert host and port and user and password and database, "DSN is invalid"

        os.environ.update(
            POSTGRES_HOST=host,
            POSTGRES_PORT=str(port),
            POSTGRES_USER=user,
            POSTGRES_PASSWORD=password,
            POSTGRES_DB=database,
        )

        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("script_location", str(ROOT / "alembic"))
        command.upgrade(cfg, "head")

        try:
            yield url.render_as_string(hide_password=False)
        finally:
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


@pytest_asyncio.fixture
async def api_engine(api_pg_dsn):
    engine = create_async_engine(api_pg_dsn, pool_size=10, max_overflow=20)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def api_session_factory(api_engine):
    async with api_engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox CASCADE"))

    return async_sessionmaker(api_engine, expire_on_commit=False, autoflush=False)


@pytest_asyncio.fixture
async def api_session(api_session_factory):
    async with api_session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def api_client(api_session_factory):
    app = create_app()

    async def override_get_session():
        async with api_session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client


@pytest.fixture
def auth_headers() -> dict[str, str]:
    return {"X-API-Key": settings.api_key}


def valid_body(**overrides) -> dict[str, Any]:
    data = {
        "amount": "100.00",
        "currency": "RUB",
        "description": "Заказ №1",
        "metadata": {"order_id": 1},
        "webhook_url": "http://receiver.test/hook",
    }
    data.update(overrides)
    return data


def omit(*keys: str) -> dict[str, Any]:
    return {key: value for key, value in valid_body().items() if key not in keys}


def headers_with_key(auth_headers: dict[str, str], key: str) -> dict[str, str]:
    return {**auth_headers, "Idempotency-Key": key}


async def create_payment(
    client: AsyncClient,
    auth_headers: dict[str, Any],
    key: str = "key-1",
    **overrides,
) -> Response:
    return await client.post(
        API,
        json=valid_body(**overrides),
        headers=headers_with_key(auth_headers, key),
    )


async def counts(session) -> tuple[int, int]:
    payments = await session.scalar(select(func.count()).select_from(Payment))
    outbox = await session.scalar(select(func.count()).select_from(Outbox))
    return payments, outbox


@pytest.mark.parametrize("method", ["post", "get"])
async def test_missing_api_key(api_client, method):
    if method == "post":
        response = await api_client.post(
            API,
            json=valid_body(),
            headers={"Idempotency-Key": "no-api-key"},
        )
    else:
        response = await api_client.get(f"{API}/{uuid4()}")

    assert response.status_code == 401
    assert response.headers.get("WWW-Authenticate") == "APIKey"


async def test_wrong_api_key(api_client, auth_headers):
    bad_headers = {
        **auth_headers,
        "X-API-Key": "wrong-key",
        "Idempotency-Key": "wrong-key",
    }

    assert (await api_client.post(API, json=valid_body(), headers=bad_headers)).status_code == 401
    assert (await api_client.get(f"{API}/{uuid4()}", headers=bad_headers)).status_code == 401


async def test_missing_idempotency_key(api_client, auth_headers):
    response = await api_client.post(API, json=valid_body(), headers=auth_headers)
    assert response.status_code == 422


async def test_missing_body(api_client, auth_headers):
    response = await api_client.post(
        API,
        headers=headers_with_key(auth_headers, "missing-body"),
    )
    assert response.status_code == 422


@pytest.mark.parametrize("key", ["", "x" * 256])
async def test_invalid_idempotency_key(api_client, auth_headers, key):
    response = await api_client.post(
        API,
        json=valid_body(),
        headers=headers_with_key(auth_headers, key),
    )
    assert response.status_code == 422


async def test_max_length_idempotency_key(api_client, auth_headers):
    response = await api_client.post(
        API,
        json=valid_body(),
        headers=headers_with_key(auth_headers, "x" * 255),
    )
    assert response.status_code == 202


async def test_malformed_json(api_client, auth_headers):
    response = await api_client.post(
        API,
        content="{",
        headers={
            **auth_headers,
            "Idempotency-Key": "malformed",
            "Content-Type": "application/json",
        },
    )
    assert response.status_code in {400, 422}


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(omit("amount"), id="missing-amount"),
        pytest.param(omit("currency"), id="missing-currency"),
        pytest.param(omit("webhook_url"), id="missing-webhook-url"),
        pytest.param(valid_body(amount=None), id="null-amount"),
        pytest.param(valid_body(amount="0"), id="zero-amount"),
        pytest.param(valid_body(amount="-1.00"), id="negative-amount"),
        pytest.param(valid_body(amount="1.005"), id="too-many-decimal-places"),
        pytest.param(valid_body(amount="abc"), id="not-a-number"),
        pytest.param(valid_body(amount="123456789012345678.99"), id="too-many-digits"),
        pytest.param(valid_body(currency="GBP"), id="unsupported-currency"),
        pytest.param(valid_body(currency="rub"), id="lowercase-currency"),
        pytest.param(valid_body(currency=None), id="null-currency"),
        pytest.param(valid_body(webhook_url="not-a-url"), id="invalid-webhook-url"),
        pytest.param(valid_body(webhook_url="http://"), id="webhook-url-no-host"),
        pytest.param(valid_body(webhook_url=None), id="null-webhook-url"),
        pytest.param(valid_body(metadata=[]), id="metadata-list"),
        pytest.param(valid_body(metadata="text"), id="metadata-string"),
        pytest.param(valid_body(metadata=1), id="metadata-number"),
        pytest.param(valid_body(description="x" * 501), id="description-too-long"),
        pytest.param(valid_body(unknown="field"), id="extra-field"),
    ],
)
async def test_create_payment_validation(api_client, auth_headers, payload):
    response = await api_client.post(
        API,
        json=payload,
        headers=headers_with_key(auth_headers, f"invalid-{uuid4()}"),
    )
    assert response.status_code == 422


async def test_create_payment_full_contract(api_client, api_session, auth_headers):
    payload = valid_body(
        amount="199.90",
        currency="RUB",
        description="Заказ №1",
        metadata={"order_id": 1, "nested": {"items": [1, True, None, "текст"]}},
        webhook_url="http://receiver.test/hook?x=1&y=2",
    )

    response = await api_client.post(
        API,
        json=payload,
        headers=headers_with_key(auth_headers, "order-1"),
    )

    assert response.status_code == 202
    assert response.headers["content-type"].startswith("application/json")

    data = response.json()
    assert set(data) == {"payment_id", "status", "created_at"}
    assert data["status"] == "pending"

    payment_id = UUID(data["payment_id"])
    created_at = datetime.fromisoformat(data["created_at"].replace("Z", "+00:00"))
    assert created_at.tzinfo is not None

    payment = await api_session.get(Payment, payment_id)
    assert payment is not None
    assert payment.status is PaymentStatus.PENDING
    assert payment.amount == Decimal("199.90")
    assert payment.currency is Currency.RUB
    assert payment.description == "Заказ №1"
    assert payment.meta == payload["metadata"]
    assert payment.idempotency_key == "order-1"
    assert payment.webhook_url == payload["webhook_url"]
    assert payment.processed_at is None
    assert payment.webhook_status is WebhookStatus.PENDING
    assert payment.webhook_attempts == 0

    outbox = await api_session.scalar(select(Outbox).where(Outbox.payment_id == payment_id))
    assert outbox is not None
    assert outbox.status is OutboxStatus.PENDING
    assert outbox.event_type == "payment.created"
    assert outbox.routing_key == "payments.new"
    assert outbox.published_at is None
    assert outbox.payload["payment_id"] == str(payment_id)
    assert outbox.payload["attempt"] == 1
    UUID(outbox.payload["event_id"])


async def test_create_minimal_defaults(api_client, auth_headers):
    payload = {
        "amount": "10",
        "currency": "USD",
        "webhook_url": "http://receiver.test/hook",
    }

    response = await api_client.post(
        API,
        json=payload,
        headers=headers_with_key(auth_headers, "minimal"),
    )
    assert response.status_code == 202

    payment_id = response.json()["payment_id"]
    detail = await api_client.get(f"{API}/{payment_id}", headers=auth_headers)
    assert detail.status_code == 200

    body = detail.json()
    assert body["description"] is None
    assert body["metadata"] == {}
    assert body["amount"] == "10.00"


@pytest.mark.parametrize(
    ("amount", "expected"),
    [
        ("10.5", "10.50"),
        (10, "10.00"),
        ("9999999999999999.99", "9999999999999999.99"),
    ],
)
async def test_amount_round_trip(api_client, auth_headers, amount, expected):
    response = await create_payment(
        api_client,
        auth_headers,
        key=f"amount-{expected}",
        amount=amount,
    )
    assert response.status_code == 202

    payment_id = response.json()["payment_id"]
    detail = await api_client.get(f"{API}/{payment_id}", headers=auth_headers)
    assert detail.status_code == 200
    assert detail.json()["amount"] == expected


async def test_description_is_stripped(api_client, auth_headers):
    response = await create_payment(
        api_client,
        auth_headers,
        key="strip",
        description="  hello  ",
    )
    assert response.status_code == 202

    payment_id = response.json()["payment_id"]
    detail = await api_client.get(f"{API}/{payment_id}", headers=auth_headers)
    assert detail.json()["description"] == "hello"


async def test_idempotent_replay_same_body(api_client, api_session, auth_headers):
    key = "replay"

    first = await create_payment(api_client, auth_headers, key=key)
    second = await create_payment(api_client, auth_headers, key=key)

    assert first.status_code == second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]
    assert first.headers.get("X-Idempotent-Replay") is None
    assert second.headers.get("X-Idempotent-Replay") == "true"

    assert await counts(api_session) == (1, 1)


async def test_idempotent_replay_canonical_body(api_client, api_session, auth_headers):
    key = "canonical"

    first_payload = valid_body(
        amount="100.00",
        description=None,
        metadata={"a": 1, "b": 2},
    )
    second_payload = valid_body(
        amount="100.0",
        metadata={"b": 2, "a": 1},
    )
    second_payload.pop("description")

    first = await api_client.post(
        API,
        json=first_payload,
        headers=headers_with_key(auth_headers, key),
    )
    second = await api_client.post(
        API,
        json=second_payload,
        headers=headers_with_key(auth_headers, key),
    )

    assert first.status_code == second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]
    assert second.headers.get("X-Idempotent-Replay") == "true"
    assert await counts(api_session) == (1, 1)


async def test_replay_returns_current_status(api_client, api_session, auth_headers):
    key = "status-replay"

    created = await create_payment(api_client, auth_headers, key=key)
    payment_id = UUID(created.json()["payment_id"])

    await api_session.execute(
        update(Payment)
        .where(Payment.id == payment_id)
        .values(
            status=PaymentStatus.SUCCEEDED,
            processed_at=func.now(),
            webhook_status=WebhookStatus.DELIVERED,
            webhook_attempts=1,
        )
    )
    await api_session.commit()

    replay = await create_payment(api_client, auth_headers, key=key)
    assert replay.status_code == 202
    assert replay.json()["payment_id"] == str(payment_id)
    assert replay.json()["status"] == "succeeded"
    assert replay.headers.get("X-Idempotent-Replay") == "true"

    detail = await api_client.get(f"{API}/{payment_id}", headers=auth_headers)
    assert detail.status_code == 200
    assert detail.json()["status"] == "succeeded"
    assert detail.json()["processed_at"] is not None
    assert detail.json()["webhook_status"] == "delivered"


async def test_concurrent_same_key_creates_one_payment(api_client, api_session, auth_headers):
    key = "race-same"
    headers = headers_with_key(auth_headers, key)
    payload = valid_body()

    responses = await asyncio.gather(*(api_client.post(API, json=payload, headers=headers) for _ in range(5)))

    assert all(response.status_code == 202 for response in responses)
    assert len({response.json()["payment_id"] for response in responses}) == 1
    assert await counts(api_session) == (1, 1)


async def test_get_payment_detail(api_client, auth_headers):
    metadata = {"a": [1, 2, {"b": None}], "unicode": "текст"}

    created = await create_payment(
        api_client,
        auth_headers,
        key="get-detail",
        amount="12345.67",
        description="описание",
        metadata=metadata,
    )
    assert created.status_code == 202
    payment_id = created.json()["payment_id"]

    response = await api_client.get(f"{API}/{payment_id}", headers=auth_headers)
    assert response.status_code == 200

    detail = response.json()
    required_keys = {
        "id",
        "amount",
        "currency",
        "description",
        "metadata",
        "status",
        "idempotency_key",
        "webhook_url",
        "created_at",
        "processed_at",
        "error",
        "webhook_status",
        "webhook_attempts",
    }
    assert required_keys.issubset(detail)

    assert detail["id"] == payment_id
    assert detail["amount"] == "12345.67"
    assert detail["currency"] == "RUB"
    assert detail["description"] == "описание"
    assert detail["metadata"] == metadata
    assert detail["status"] == "pending"
    assert detail["idempotency_key"] == "get-detail"
    assert detail["webhook_url"] == "http://receiver.test/hook"
    assert detail["processed_at"] is None
    assert detail["error"] is None
    assert detail["webhook_status"] == "pending"
    assert detail["webhook_attempts"] == 0


async def test_get_not_found(api_client, auth_headers):
    response = await api_client.get(f"{API}/{uuid4()}", headers=auth_headers)
    assert response.status_code == 404


async def test_get_invalid_uuid(api_client, auth_headers):
    response = await api_client.get(f"{API}/not-a-uuid", headers=auth_headers)
    assert response.status_code == 422


async def test_get_reflects_failed_processing(api_client, api_session, auth_headers):
    created = await create_payment(api_client, auth_headers, key="failed-processing")
    payment_id = UUID(created.json()["payment_id"])

    await api_session.execute(
        update(Payment)
        .where(Payment.id == payment_id)
        .values(
            status=PaymentStatus.FAILED,
            error="Недостаточно средств",
            processed_at=func.now(),
            webhook_status=WebhookStatus.FAILED,
            webhook_attempts=3,
        )
    )
    await api_session.commit()

    response = await api_client.get(f"{API}/{payment_id}", headers=auth_headers)
    assert response.status_code == 200

    detail = response.json()
    assert detail["status"] == "failed"
    assert detail["error"] == "Недостаточно средств"
    assert detail["processed_at"] is not None
    assert detail["webhook_status"] == "failed"
    assert detail["webhook_attempts"] == 3


async def test_health_live_without_api_key(api_client):
    response = await api_client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_health_ready(api_client):
    response = await api_client.get("/health/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "db": "ok"}


async def test_openapi_contract(api_client):
    response = await api_client.get("/openapi.json")
    assert response.status_code == 200

    spec = response.json()
    schemes = spec.get("components", {}).get("securitySchemes", {}).values()
    assert any(
        scheme.get("type") == "apiKey" and scheme.get("name") == "X-API-Key" and scheme.get("in") == "header"
        for scheme in schemes
    )

    post = spec["paths"][API]["post"]
    parameters = post.get("parameters", [])
    assert any(
        parameter.get("name") == "Idempotency-Key"
        and parameter.get("in") == "header"
        and parameter.get("required") is True
        for parameter in parameters
    )
