from functools import lru_cache
from urllib.parse import quote, quote_plus

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _config(prefix: str = "") -> SettingsConfigDict:
    return SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix=prefix,
        extra="ignore",
    )


class PostgresSettings(BaseSettings):
    model_config = _config("POSTGRES_")

    host: str = "localhost"
    port: int = 5432
    user: str = "app"
    password: str = "app"
    db: str = "payments"
    echo: bool = False
    pool_size: int = 10
    max_overflow: int = 20

    @property
    def dsn(self) -> str:
        return (
            f"postgresql+asyncpg://{quote_plus(self.user)}:{quote_plus(self.password)}"
            f"@{self.host}:{self.port}/{self.db}"
        )


class RabbitSettings(BaseSettings):
    model_config = _config("RABBITMQ_")

    host: str = "localhost"
    port: int = 5672
    user: str = "app"
    password: str = "app"
    vhost: str = "/"

    @property
    def url(self) -> str:
        return (
            f"amqp://{quote_plus(self.user)}:{quote_plus(self.password)}"
            f"@{self.host}:{self.port}/{quote(self.vhost, safe='')}"
        )


class OutboxSettings(BaseSettings):
    model_config = _config("OUTBOX_")

    poll_interval: float = 1.0
    batch_size: int = 50
    base_delay: float = 1.0
    max_delay: float = 60.0


class RetrySettings(BaseSettings):
    model_config = _config("RETRY_")

    max_attempts: int = 3
    base_delay: float = 2.0


class GatewaySettings(BaseSettings):
    model_config = _config("GATEWAY_")

    min_delay: float = 2.0
    max_delay: float = 5.0
    failure_rate: float = 0.1


class WebhookSettings(BaseSettings):
    model_config = _config("WEBHOOK_")

    timeout: float = 5.0
    max_attempts: int = 3
    base_delay: float = 1.0
    secret: str = "webhook-signing-secret"


class Settings(BaseSettings):
    model_config = _config()

    app_name: str = "payment-processing-service"
    debug: bool = False
    api_prefix: str = "/api/v1"
    api_key: str = "super-secret-key"

    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    rabbitmq: RabbitSettings = Field(default_factory=RabbitSettings)
    outbox: OutboxSettings = Field(default_factory=OutboxSettings)
    retry: RetrySettings = Field(default_factory=RetrySettings)
    gateway: GatewaySettings = Field(default_factory=GatewaySettings)
    webhook: WebhookSettings = Field(default_factory=WebhookSettings)


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
