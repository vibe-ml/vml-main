"""Runtime configuration for the research agent and web UI."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Load Horizon settings from the environment or a local .env file."""

    model_config = SettingsConfigDict(env_prefix="HORIZON_", env_file=".env", extra="ignore")

    model: str = ""
    model_base_url: str | None = None
    model_api_key: SecretStr | None = None
    tavily_api_key: SecretStr | None = None
    host: str = "127.0.0.1"
    port: int = Field(default=8080, ge=1, le=65535)
    max_search_results: int = Field(default=5, ge=1, le=10)
    recursion_limit: int = Field(default=80, ge=10, le=200)
    database_url: SecretStr | None = None
    qdrant_url: str | None = None
    data_dir: Path = Field(default=Path("/data/demo"))


@lru_cache
def get_settings() -> Settings:
    """Return the process settings, cached after the first read."""
    return Settings()
