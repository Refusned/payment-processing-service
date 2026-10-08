from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str
    docs_enabled: bool = True

    db_pool_size: int = 10

    outbox_batch_size: int = 20
    outbox_poll_interval: float = 0.5
    publish_timeout: float = 5.0

    consumer_prefetch: int = 10

    gateway_min_delay: float = 2.0
    gateway_max_delay: float = 5.0
    gateway_success_rate: float = 0.9
    # Разрешает задавать исход и задержку эмулятора через metadata платежа.
    # Включается только в тестовом окружении.
    gateway_test_hooks: bool = False

    webhook_connect_timeout: float = 2.0
    webhook_read_timeout: float = 5.0
    # Общий лимит на запрос: read timeout сам по себе не ограничивает медленную отдачу ответа.
    webhook_deadline: float = 10.0


settings = Settings()  # type: ignore[call-arg]
