"""Secret-bearing social collection configuration."""

from datetime import date

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

SCHEMA = r"^[a-z][a-z0-9_]{0,62}$"
PLATFORMS = ("hackernews", "stackexchange", "bluesky")


class Settings(BaseSettings):
    """Load collection settings from environment or a local .env file."""

    model_config = SettingsConfigDict(
        env_prefix="SOCIAL_", env_file=".env", extra="ignore"
    )
    database_url: SecretStr
    schema_social: str = Field(default="social", pattern=SCHEMA)
    alembic_name: str = Field(default="social", pattern=r"^[a-z][a-z0-9_]{0,54}$")
    # Read-only OpenAlex namespace written by vml-crawler-openalex.
    openalex_schema_raw: str = Field(default="raw", pattern=SCHEMA)
    cadence: str = "0 3 * * *"
    collect_from: date = date(2024, 1, 1)
    topic_ids: tuple[str, ...] = ()
    topic_limit: int = Field(default=100, ge=1)
    min_term_length: int = Field(default=4, ge=1)
    excluded_terms: tuple[str, ...] = ()
    platforms: tuple[str, ...] = PLATFORMS
    stackexchange_sites: tuple[str, ...] = (
        "stackoverflow",
        "ai",
        "datascience",
        "robotics",
        "electronics",
        "quantumcomputing",
        "engineering",
    )
    stackexchange_key: SecretStr | None = None
    bluesky_handle: str | None = None
    bluesky_app_password: SecretStr | None = None
    request_budget: int = Field(default=2000, ge=0)
    window_cap: int = Field(default=1000, ge=100, le=1000)
    lease_seconds: int = Field(default=900, ge=1)
    max_retries: int = Field(default=3, ge=0, le=10)
    retry_seconds: float = Field(default=2, ge=0)
    timeout_seconds: float = Field(default=30, gt=0)
    text_limit: int = Field(default=4000, ge=100)

    @field_validator("database_url")
    @classmethod
    def postgres_only(cls, value: SecretStr) -> SecretStr:
        """Reject non-PostgreSQL persistence."""
        if make_url(value.get_secret_value()).drivername != "postgresql+psycopg":
            raise ValueError("Use a postgresql+psycopg database URL")
        return value

    @field_validator("platforms")
    @classmethod
    def known_platforms(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Reject platforms without an adapter."""
        unknown = set(value) - set(PLATFORMS)
        if unknown:
            raise ValueError(f"Unknown platforms: {sorted(unknown)}")
        return value
