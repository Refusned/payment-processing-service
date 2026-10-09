from typing import Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # allow_inf_nan: иначе "Infinity" в переменной окружения снимет любой таймаут.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", allow_inf_nan=False)

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:55432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:55672/"
    api_key: str = Field(min_length=1, pattern=r"^[\x21-\x7e]+$")
    # Ключ нужен на всех эндпоинтах, поэтому Swagger по умолчанию выключен.
    docs_enabled: bool = False

    db_pool_size: int = Field(default=10, ge=1)
    db_command_timeout: float = Field(default=30.0, gt=0)

    outbox_batch_size: int = Field(default=20, ge=1)
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    publish_timeout: float = Field(default=5.0, gt=0)

    consumer_prefetch: int = Field(default=10, ge=1)

    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)
    # Исход и задержка эмулятора из metadata платежа, только для тестов.
    gateway_test_hooks: bool = False

    webhook_connect_timeout: float = Field(default=2.0, gt=0)
    webhook_read_timeout: float = Field(default=5.0, gt=0)
    webhook_deadline: float = Field(default=10.0, gt=0)

    @model_validator(mode="after")
    def _check_gateway_delays(self) -> Self:
        if self.gateway_min_delay > self.gateway_max_delay:
            raise ValueError("gateway_min_delay must not exceed gateway_max_delay")
        return self


settings = Settings()  # type: ignore[call-arg]
