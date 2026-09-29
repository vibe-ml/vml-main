"""Processor-owned label and work-summary tables."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import ClassVar

from sqlalchemy import DateTime, ForeignKey, Integer, Text, Uuid, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class WorkSummary(Base):
    """One generated summary of a sampled work within a topic run."""

    __tablename__ = "pwf_work_summaries"
    __table_args__: ClassVar[dict[str, str]] = {
        "comment": "Per-work summaries that feed discovery topic headlines"
    }

    topic_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pwf_topic_runs.run_id"),
        primary_key=True,
        comment="Topic run that owns this summary",
    )
    discovery_topic_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pwf_discovery_topics.discovery_topic_id"),
        primary_key=True,
        comment="Discovery topic this work was sampled for",
    )
    work_id: Mapped[str] = mapped_column(
        Text(),
        primary_key=True,
        comment="OpenAlex work entity identifier that was summarized",
    )
    summary_text: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Generated English summary of the work title and abstract",
    )
    model: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Labeling model name that produced this summary",
    )
    model_revision: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Labeling model revision that produced this summary",
    )
    prompt_version: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Version of the summarization prompt constants used",
    )
    prompt_tokens: Mapped[int] = mapped_column(
        Integer(),
        nullable=False,
        comment="Prompt token count reported for this summary call",
    )
    completion_tokens: Mapped[int] = mapped_column(
        Integer(),
        nullable=False,
        comment="Completion token count reported for this summary call",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="When this summary row was inserted",
    )


class TopicLabel(Base):
    """One generated headline for a discovery topic within a topic run."""

    __tablename__ = "pwf_topic_labels"
    __table_args__: ClassVar[dict[str, str]] = {
        "comment": "Generated headlines and sampling metadata for discovery topics"
    }

    topic_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pwf_topic_runs.run_id"),
        primary_key=True,
        comment="Topic run that owns this label",
    )
    discovery_topic_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pwf_discovery_topics.discovery_topic_id"),
        primary_key=True,
        comment="Discovery topic this headline describes",
    )
    headline: Mapped[str | None] = mapped_column(
        Text(),
        nullable=True,
        comment="Generated English headline; null when labeling failed",
    )
    concatenated_summary_text: Mapped[str | None] = mapped_column(
        Text(),
        nullable=True,
        comment="Concatenated per-work summaries that fed the headline",
    )
    chunk_count: Mapped[int] = mapped_column(
        Integer(),
        nullable=False,
        comment="Number of summary chunks used; 1 for single-pass labeling",
    )
    sampling_method: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="How works were selected: representative_docs or all_members",
    )
    sampled_work_ids: Mapped[list[str]] = mapped_column(
        ARRAY(Text()),
        nullable=False,
        comment="OpenAlex work IDs whose summaries fed this headline",
    )
    sample_size: Mapped[int] = mapped_column(
        Integer(),
        nullable=False,
        comment="Number of works sampled for this label",
    )
    model: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Labeling model name that produced this headline",
    )
    model_revision: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Labeling model revision that produced this headline",
    )
    prompt_version: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Version of the labeling prompt constants used",
    )
    status: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Label outcome: succeeded or failed",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="When this label row was inserted",
    )
