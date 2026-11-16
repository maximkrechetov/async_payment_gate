import asyncio
import os
import time
from pathlib import Path
from urllib.parse import urlparse

import aio_pika
import pytest
import pytest_asyncio
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.core.container import DockerContainer
from testcontainers.postgres import PostgresContainer

from alembic import command
from app.broker.topology import DLQ, PAYMENTS_NEW_QUEUE, RETRY_QUEUE, ensure_topology
from app.core.config import settings
from app.db.session import get_session
from app.main import create_app

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def pg_dsn():
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
        yield url.render_as_string(hide_password=False)


def _wait_amqp(url: str, timeout: float = 90.0) -> None:
    async def probe() -> None:
        conn = await aio_pika.connect_robust(url, timeout=2)
        await conn.close()

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            asyncio.run(probe())
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("RabbitMQ не поднялся за отведённое время")


@pytest.fixture(scope="session")
def rabbit_url():
    container = (
        DockerContainer("rabbitmq:4-management-alpine")
        .with_exposed_ports(5672)
        .with_env("RABBITMQ_DEFAULT_USER", "app")
        .with_env("RABBITMQ_DEFAULT_PASS", "app")
    )
    container.start()
    try:
        url = f"amqp://app:app@{container.get_container_host_ip()}:{container.get_exposed_port(5672)}/"
        _wait_amqp(url)
        yield url
    finally:
        container.stop()


@pytest_asyncio.fixture
async def engine(pg_dsn):
    eng = create_async_engine(pg_dsn)
    try:
        yield eng
    finally:
        await eng.dispose()


@pytest_asyncio.fixture
async def session_factory(engine, monkeypatch):
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox CASCADE"))

    factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    monkeypatch.setattr("app.db.session.SessionFactory", factory)
    monkeypatch.setattr("app.outbox_worker.SessionFactory", factory)
    return factory


@pytest_asyncio.fixture
async def session(session_factory):
    async with session_factory() as s:
        yield s


@pytest_asyncio.fixture
async def api(session_factory):
    app = create_app()

    async def _override():
        async with session_factory() as s:
            yield s

    app.dependency_overrides[get_session] = _override
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest_asyncio.fixture
async def rabbit(rabbit_url, monkeypatch):

    parsed = urlparse(rabbit_url)
    monkeypatch.setattr(settings.rabbitmq, "host", parsed.hostname)
    monkeypatch.setattr(settings.rabbitmq, "port", int(parsed.port))
    monkeypatch.setattr(settings.rabbitmq, "user", parsed.username)
    monkeypatch.setattr(settings.rabbitmq, "password", parsed.password)
    monkeypatch.setattr(settings.rabbitmq, "vhost", "/")
    monkeypatch.setattr(settings.retry, "base_delay", 0.5)

    await ensure_topology()

    # Проверка через Management API (не через aio-pika)
    import httpx

    mgmt_url = f"http://{parsed.hostname}:15672/api/queues/{parsed.path.lstrip('/') or '%2F'}/{RETRY_QUEUE.name}"
    async with httpx.AsyncClient(auth=(parsed.username, parsed.password)) as client:
        resp = await client.get(mgmt_url)
        if resp.status_code == 200:
            args = resp.json().get("arguments", {})
            print("\n=== payments.retry arguments (from Management API) ===")
            print(f"DLX: {args.get('x-dead-letter-exchange')}")
            print(f"DLX-RK: {args.get('x-dead-letter-routing-key')}")
        else:
            print(f"\n=== payments.retry not found: {resp.status_code} ===")

    conn = await aio_pika.connect_robust(rabbit_url)
    try:
        channel = await conn.channel()
        for q in (PAYMENTS_NEW_QUEUE, RETRY_QUEUE, DLQ):
            declared = await channel.declare_queue(q.name, durable=True, passive=True)
            await declared.purge()
    finally:
        await conn.close()
    return rabbit_url


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    prefixes = (
        "POSTGRES_",
        "RABBITMQ_",
        "OUTBOX_",
        "RETRY_",
        "GATEWAY_",
        "WEBHOOK_",
        "APP_NAME",
        "DEBUG",
        "API_PREFIX",
        "API_KEY",
    )
    for key in list(os.environ):
        if any(key.startswith(prefix) for prefix in prefixes):
            monkeypatch.delenv(key, raising=False)
    yield
