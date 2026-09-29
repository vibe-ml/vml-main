"""SQLAlchemy declarative base for processor-owned tables."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Shared metadata for `pwf_` tables in the public schema."""
