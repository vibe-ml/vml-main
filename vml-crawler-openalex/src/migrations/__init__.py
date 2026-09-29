"""Alembic configuration shared by ingestion jobs and operator commands."""

from pathlib import Path

from alembic.config import Config

from src.common.settings import Settings


def configuration(settings: Settings) -> Config:
    """Build Alembic configuration without placing credentials in its INI file."""
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    config.attributes["settings"] = settings
    return config
