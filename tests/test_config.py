from app.core.config import PostgresSettings, Settings, get_settings


def test_section_prefixes(monkeypatch) -> None:
    get_settings.cache_clear()
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_POOL_SIZE", "42")
    monkeypatch.setenv("RABBITMQ_USER", "payments")
    monkeypatch.setenv("RETRY_MAX_ATTEMPTS", "5")
    monkeypatch.setenv("WEBHOOK_SECRET", "s3cr3t")

    s = Settings()
    assert s.postgres.host == "db"
    assert s.postgres.pool_size == 42
    assert s.rabbitmq.user == "payments"
    assert s.retry.max_attempts == 5
    assert s.webhook.secret == "s3cr3t"
    assert s.postgres.dsn.startswith("postgresql+asyncpg://app:app@db:5432/")


def test_section_is_self_sufficient(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_DB", "isolated")
    assert PostgresSettings().db == "isolated"


def test_explicit_kwargs_beat_env(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "from-env")
    s = Settings(postgres=PostgresSettings(host="from-code"))
    assert s.postgres.host == "from-code"
