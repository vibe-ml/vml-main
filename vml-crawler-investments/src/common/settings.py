"""Investment news collection configuration."""

from datetime import date

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

SCHEMA = r"^[a-z][a-z0-9_]{0,62}$"


class Settings(BaseSettings):
    """Load settings from environment or a local .env file."""

    model_config = SettingsConfigDict(
        env_prefix="INVESTMENTS_", env_file=".env", extra="ignore"
    )
    database_url: SecretStr
    schema_investments: str = Field(default="investments", pattern=SCHEMA)
    alembic_name: str = Field(default="investments", pattern=r"^[a-z][a-z0-9_]{0,54}$")
    # Read-only search profile written by vml-crawler-social.
    social_schema: str = Field(default="social", pattern=SCHEMA)
    cadence: str = "0 4 * * *"
    collect_from: date = date(2020, 1, 1)
    # Source languages; GDELT matches English terms against translated articles.
    languages: tuple[str, ...] = ("english", "russian")
    # Single-word OpenAlex keywords ("Design", "Culture") match unrelated news.
    min_keyword_words: int = Field(default=2, ge=1)
    # GDELT asks for one request per five seconds but throttles bursts harder.
    request_interval: float = Field(default=10, ge=0)
    request_budget: int = Field(default=600, ge=0)
    window_cap: int = Field(default=250, ge=10, le=250)
    lease_seconds: int = Field(default=900, ge=1)
    max_retries: int = Field(default=3, ge=0, le=10)
    retry_seconds: float = Field(default=30, ge=0)
    timeout_seconds: float = Field(default=60, gt=0)

    @field_validator("database_url")
    @classmethod
    def postgres_only(cls, value: SecretStr) -> SecretStr:
        """Reject non-PostgreSQL persistence."""
        if make_url(value.get_secret_value()).drivername != "postgresql+psycopg":
            raise ValueError("Use a postgresql+psycopg database URL")
        return value
