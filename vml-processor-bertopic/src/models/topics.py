"""Processor-owned topic-run tables."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any, ClassVar

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Text,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class TopicRun(Base):
    """One discovery analysis of a fixed corpus."""

    __tablename__ = "pwf_topic_runs"
    __table_args__: ClassVar[dict[str, str]] = {
        "comment": "One discovery analysis of a fixed corpus"
    }

    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        comment="Stable identifier of this topic run",
    )
    status: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Run outcome: running, succeeded, or failed",
    )
    scope_ids: Mapped[list[str]] = mapped_column(
        ARRAY(Text()),
        nullable=False,
        comment="Crawler collection scopes used for the corpus",
    )
    work_types: Mapped[list[str]] = mapped_column(
        ARRAY(Text()),
        nullable=False,
        comment="OpenAlex work types kept in the corpus",
    )
    published_from: Mapped[date] = mapped_column(
        Date(),
        nullable=False,
        comment="Inclusive start of the publication interval",
    )
    published_to: Mapped[date] = mapped_column(
        Date(),
        nullable=False,
        comment="Inclusive end of the publication interval",
    )
    excluded_domain_ids: Mapped[list[str]] = mapped_column(
        ARRAY(Text()),
        nullable=False,
        comment="OpenAlex primary domain IDs excluded from the corpus",
    )
    embedding_model: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Embedding model name whose vectors were fitted",
    )
    embedding_model_revision: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Embedding model revision whose vectors were fitted",
    )
    min_cluster_size: Mapped[int] = mapped_column(
        Integer(),
        nullable=False,
        comment="HDBSCAN minimum cluster size used for the fit",
    )
    min_samples: Mapped[int | None] = mapped_column(
        Integer(),
        nullable=True,
        comment="HDBSCAN min_samples; null means library default",
    )
    umap_random_state: Mapped[int] = mapped_column(
        Integer(),
        nullable=False,
        comment="UMAP random_state pinning reproducibility",
    )
    umap_metric: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="UMAP distance metric used for the fit",
    )
    composition_hash_algorithm: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Hash algorithm used for composition_hash values",
    )
    composition_hash_encoding_version: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Version of the membership encoding fed to the hash",
    )
    work_count: Mapped[int] = mapped_column(
        Integer(),
        nullable=False,
        comment="Number of works in the fitted corpus",
    )
    topic_count: Mapped[int] = mapped_column(
        Integer(),
        nullable=False,
        comment="Number of non-outlier discovery topics found",
    )
    outlier_count: Mapped[int] = mapped_column(
        Integer(),
        nullable=False,
        comment="Number of works left unassigned as outliers",
    )
    outlier_rate: Mapped[float] = mapped_column(
        Float(),
        nullable=False,
        comment="outlier_count divided by work_count",
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="When the topic run started",
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When the topic run finished",
    )
    elapsed_seconds: Mapped[float | None] = mapped_column(
        Float(),
        nullable=True,
        comment="Wall-clock seconds from start to finish",
    )


class DiscoveryTopic(Base):
    """One discovery topic within a topic run."""

    __tablename__ = "pwf_discovery_topics"
    __table_args__: ClassVar[dict[str, str]] = {
        "comment": "Discovery topics found within a topic run",
    }

    discovery_topic_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        comment="Stable UUIDv4 identifier of this discovery topic",
    )
    topic_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pwf_topic_runs.run_id"),
        nullable=False,
        comment="Topic run that produced this discovery topic",
    )
    composition_hash: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="SHA-256 fingerprint of sorted member work IDs",
    )
    size: Mapped[int] = mapped_column(
        Integer(),
        nullable=False,
        comment="Number of member works in this discovery topic",
    )
    keywords: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB(),
        nullable=False,
        comment="c-TF-IDF keywords with weights as [{term, weight}]",
    )
    representative_work_ids: Mapped[list[str]] = mapped_column(
        ARRAY(Text()),
        nullable=False,
        comment="Centroid-nearest representative OpenAlex work IDs",
    )


class TopicRunMapping(Base):
    """BERTopic run-local integer ID mapped to a discovery topic UUID."""

    __tablename__ = "pwf_topic_run_mappings"
    __table_args__: ClassVar[dict[str, str]] = {
        "comment": "BERTopic integer IDs mapped to discovery topic UUIDs per run",
    }

    topic_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pwf_topic_runs.run_id"),
        primary_key=True,
        comment="Topic run that owns this mapping",
    )
    bertopic_topic_id: Mapped[int] = mapped_column(
        Integer(),
        primary_key=True,
        comment="BERTopic run-local topic integer identifier",
    )
    discovery_topic_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pwf_discovery_topics.discovery_topic_id"),
        nullable=False,
        comment="Persisted discovery topic UUID for this integer ID",
    )


class WorkTopicAssignment(Base):
    """One work's topic assignment within a topic run."""

    __tablename__ = "pwf_work_topic_assignments"
    __table_args__: ClassVar[dict[str, str]] = {
        "comment": "Per-work topic assignment or outlier flag within a run",
    }

    topic_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pwf_topic_runs.run_id"),
        primary_key=True,
        comment="Topic run that produced this assignment",
    )
    work_id: Mapped[str] = mapped_column(
        Text(),
        primary_key=True,
        comment="OpenAlex work entity identifier",
    )
    work_version_id: Mapped[str] = mapped_column(
        Text(),
        nullable=False,
        comment="Immutable work version consumed by the run",
    )
    discovery_topic_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pwf_discovery_topics.discovery_topic_id"),
        nullable=True,
        comment="Assigned discovery topic; null when outlier",
    )
    is_outlier: Mapped[bool] = mapped_column(
        Boolean(),
        nullable=False,
        comment="True when HDBSCAN left the work unassigned",
    )
    bertopic_topic_id: Mapped[int] = mapped_column(
        Integer(),
        nullable=False,
        comment="BERTopic run-local topic id; -1 for outliers",
    )
