"""Secret-bearing ingestion configuration."""

from datetime import date
from pathlib import Path

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url


class Settings(BaseSettings):
    """Load collection settings from environment or a local .env file."""

    model_config = SettingsConfigDict(
        env_prefix="OPENALEX_", env_file=".env", extra="ignore"
    )
    database_url: SecretStr
    api_key: SecretStr | None = None
    schema_raw: str = Field(default="raw", pattern=r"^[a-z][a-z0-9_]{0,62}$")
    schema_tmd: str = Field(default="tmd", pattern=r"^[a-z][a-z0-9_]{0,62}$")
    alembic_name: str = Field(default="openalex", pattern=r"^[a-z][a-z0-9_]{0,54}$")
    collection_schema: str | None = Field(
        default=None, pattern=r"^[a-z][a-z0-9_]{0,62}$"
    )
    ingestion_cadence: str = Field(default="0 0 * * *")
    storage_root: Path = Path("data/openalex")
    publication_from: date = date(2026, 1, 1)
    publication_through: date | None = None
    domain_ids: tuple[str, ...] = ()
    field_ids: tuple[str, ...] = ()
    exclude_domain_ids: tuple[str, ...] = ()
    exclude_field_ids: tuple[str, ...] = ()
    page_size: int = Field(default=100, ge=1, le=100)

    @field_validator(
        "domain_ids",
        "field_ids",
        "exclude_domain_ids",
        "exclude_field_ids",
        mode="before",
    )
    @classmethod
    def parse_sequence_ids(cls, value: object) -> tuple[str, ...]:
        """Normalize sequence IDs from JSON strings or comma-separated strings."""
        if value is None:
            return ()
        if isinstance(value, str):
            value = value.strip()
            if value.startswith("[") and value.endswith("]"):
                import json

                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, list):
                        return tuple(str(x).strip() for x in parsed if str(x).strip())
                except ValueError, TypeError:
                    pass
            return tuple(x.strip() for x in value.split(",") if x.strip())
        if isinstance(value, (list, tuple, set)):
            return tuple(str(x).strip() for x in value if str(x).strip())
        return ()

    api_request_limit: int = Field(default=100, ge=0)
    api_lease_seconds: int = Field(default=300, ge=1)
    api_max_retries: int = Field(default=3, ge=0, le=10)
    api_retry_seconds: float = Field(default=1, ge=0)
    download_max_retries: int = Field(default=5, ge=0, le=20)
    download_retry_seconds: float = Field(default=2, ge=0)
    reserve_fraction: float = Field(default=0.1, ge=0, le=1)
    recent_fraction: float = Field(default=0.7, ge=0, le=1)
    timeout_seconds: float = Field(default=60, gt=0)

    @field_validator("database_url")
    @classmethod
    def postgres_only(cls, value: SecretStr) -> SecretStr:
        """Reject non-PostgreSQL persistence."""
        if make_url(value.get_secret_value()).drivername != "postgresql+psycopg":
            raise ValueError("Use a postgresql+psycopg database URL")
        return value

    @model_validator(mode="after")
    def sync_schemas(self) -> Settings:
        """Support collection_schema alias for backward compatibility."""
        if self.collection_schema:
            if self.collection_schema.startswith("raw_"):
                uid = self.collection_schema[4:]
                self.schema_raw = self.collection_schema
                self.schema_tmd = f"tmd_{uid}"
                if self.alembic_name == "openalex":
                    self.alembic_name = f"test_{uid}"
            elif self.collection_schema.startswith("test_taxonomy_"):
                uid = self.collection_schema.removeprefix("test_taxonomy_")
                self.schema_raw = f"raw_{uid}"
                self.schema_tmd = f"tmd_{uid}"
                if self.alembic_name == "openalex":
                    self.alembic_name = f"test_{uid}"
            else:
                self.schema_raw = f"{self.collection_schema}_raw"
                self.schema_tmd = f"{self.collection_schema}_tmd"
                if self.alembic_name == "openalex":
                    self.alembic_name = self.collection_schema
        else:
            self.collection_schema = self.schema_raw
        return self
