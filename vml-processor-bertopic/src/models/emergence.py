"""Processor-owned emergence observation table."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, ClassVar

from sqlalchemy import DateTime, Float, ForeignKey, Integer, Text, Uuid, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class EmergenceObservation(Base):
    """One discovery topic's publication-share series within a topic run."""

    __tablename__ = "pwf_emergence_observations"
    __table_args__: ClassVar[dict[str, str]] = {
        "comment": "Publication-share series and emergence class per discovery topic",
    }

    topic_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pwf_topic_runs.run_id"),
        primary_key=True,
        comment="Topic run that this observation scores",
    )
    discovery_topic_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pwf_discovery_topics.discovery_topic_id"),
        primary_key=True,
        comment="Discovery topic whose share series is stored",
    )
    monthly_series: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB(),
        nullable=False,
        comment="Per-month topic_count, corpus_count, and share for the run interval",
    )
    monthly_share_change: Mapped[float | None] = mapped_column(
        Float(),
        nullable=True,
        comment="Geometric month-to-month share change; null when unset",
    )
    complete_years: Mapped[list[Any]] = mapped_column(
        JSONB(),
        nullable=False,
        comment="Complete calendar years used for annual figures",
    )
    mean_share: Mapped[float | None] = mapped_column(
        Float(),
        nullable=True,
        comment="Mean share over complete years; null under insufficient coverage",
    )
    annual_growth: Mapped[float | None] = mapped_column(
        Float(),
        nullable=True,
        comment="Annual share growth over complete years; null when unset",
    )
    valid_year_pair_count: Mapped[int | None] = mapped_column(
        Integer(),
        nullable=True,
        comment="Count of valid adjacent year pairs used for annual growth",
    )
    coverage_status: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Coverage class: insufficient_historical_coverage, newly_observed, or scored",
    )
    emergence_quadrant: Mapped[str | None] = mapped_column(
        Text(),
        nullable=True,
        comment="WISDOM quadrant when scored; null when coverage withholds it",
    )
    metric_version: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Formula version that produced this observation",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="When this observation row was inserted",
    )
