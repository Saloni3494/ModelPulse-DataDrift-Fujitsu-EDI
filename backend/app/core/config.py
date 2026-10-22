"""ModelPulse Application Configuration."""
from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment or .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application
    APP_NAME: str = "ModelPulse"
    APP_ENV: str = "development"
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    DEBUG: bool = True
    LOG_LEVEL: str = "INFO"

    # Database (PostgreSQL + TimescaleDB & Local Fallback)
    DATABASE_URL: str = (
         "postgresql+asyncpg://modelpulse:change_me@localhost:5432/modelpulse"
    )
    PERSISTENCE_TYPE: str = "timescaledb"  # 'in_memory', 'sqlite', 'timescaledb'
    SQLITE_DB_PATH: str = "data/modelpulse.db"

    # Kafka Streaming (Phase 5)
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_EVENTS: str = "events"
    KAFKA_TOPIC_DLQ: str = "events.dlq"
    KAFKA_TOPIC_RESULTS: str = "monitoring.results"
    KAFKA_CONSUMER_GROUP: str = "modelpulse-consumer-group"
    KAFKA_BATCH_INTERVAL_SECONDS: int = 300  # 5-minute micro-batches
    KAFKA_MAX_BATCH_SIZE: int = 1000
    KAFKA_MAX_RETRIES: int = 3
    KAFKA_RETRY_BACKOFF_MS: int = 500
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"
    KAFKA_ENABLE_AUTO_COMMIT: bool = False
    KAFKA_PRODUCER_ACKS: str = "all"  # 'all' (-1) awaits all in-sync broker replicas
    KAFKA_MODE: str = "memory"  # 'memory' (deterministic mock bus) or 'broker' (network broker)

    # Monitoring Windows & Detection
    MICROBATCH_MINUTES: int = 5
    MONITOR_CYCLE_MINUTES: int = 60
    MIN_SAMPLES_FOR_DETECTION: int = 500
    DRIFT_ENSEMBLE_K: int = 2
    DRIFT_ENSEMBLE_M: int = 3

    # Notifications
    SLACK_WEBHOOK_URL: Optional[str] = None
    SMTP_HOST: Optional[str] = None
    SMTP_PORT: Optional[int] = 587
    SMTP_USER: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    ALERT_RECIPIENT_EMAIL: Optional[str] = None


@lru_cache()
def get_settings() -> Settings:
    """Return cached application settings singleton."""
    return Settings()
