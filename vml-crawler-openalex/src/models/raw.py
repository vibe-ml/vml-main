"""SQLAlchemy models for raw schema data and lineage."""

# ruff: noqa: RUF100, RUF012

import uuid
from datetime import datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class TaxonomyBundles(Base):
    """Canonical taxonomy tree and classification records."""

    __tablename__ = "openalex_taxonomy_bundles"
    __table_args__ = {
        "schema": "raw",
        "comment": "Canonical taxonomy tree and classification records",
    }

    id: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="Bundle identifier hash"
    )  # noqa: E501
    classification_hash: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Hash of classification tree"
    )  # noqa: E501
    canonicalization_version: Mapped[int] = mapped_column(
        sa.Integer(), nullable=False, comment="Version of canonicalization logic"
    )  # noqa: E501
    records: Mapped[dict[str, Any]] = mapped_column(
        postgresql.JSONB(),
        nullable=False,
        comment="Full classification hierarchy payload",
    )  # noqa: E501


class TaxonomyObservations(Base):
    """Sighting history per taxonomy bundle."""

    __tablename__ = "openalex_taxonomy_observations"
    __table_args__ = (
        sa.Index("openalex_taxonomy_observations_bundle", "bundle_id"),
        {"schema": "raw", "comment": "Sighting history per taxonomy bundle"},
    )

    sequence: Mapped[int] = mapped_column(
        sa.BigInteger(),
        sa.Identity(always=True),
        primary_key=True,
        comment="Monotonic observation sequence",
    )  # noqa: E501
    attempt_id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid(),
        sa.ForeignKey("tmd.openalex_taxonomy_runs.id"),
        nullable=False,
        unique=True,
        comment="Taxonomy run identifier",
    )  # noqa: E501
    bundle_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("raw.openalex_taxonomy_bundles.id"),
        nullable=False,
        comment="Observed taxonomy bundle identifier",
    )  # noqa: E501
    started_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, comment="Observation start time"
    )  # noqa: E501
    ended_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        comment="Observation completion time",
    )  # noqa: E501


class WorkVersions(Base):
    """Deduplicated work records and full entities."""

    __tablename__ = "openalex_work_versions"
    __table_args__ = (
        sa.UniqueConstraint(
            "entity_id",
            "canonicalization_version",
            "content_hash",
            name="openalex_work_versions_entity_version_hash_key",
        ),  # noqa: E501
        sa.Index(
            "openalex_work_versions_publication_date_idx",
            sa.text("(payload->>'publication_date')"),
        ),
        sa.Index(
            "openalex_work_versions_type_idx",
            sa.text("(payload->>'type')"),
        ),
        {"schema": "raw", "comment": "Deduplicated work records and full entities"},
    )

    id: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="Synthetic version identity"
    )  # noqa: E501
    entity_id: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="OpenAlex work entity identifier"
    )  # noqa: E501
    canonicalization_version: Mapped[int] = mapped_column(
        sa.Integer(), nullable=False, comment="Version of entity canonicalization"
    )  # noqa: E501
    content_hash: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="SHA-256 hash of canonicalized content"
    )  # noqa: E501
    payload: Mapped[dict[str, Any]] = mapped_column(
        postgresql.JSONB(), nullable=False, comment="Parsed work record entity payload"
    )  # noqa: E501


class WorkCurrent(Base):
    """Active reconciled version per work entity and collection scope."""

    __tablename__ = "openalex_work_current"
    __table_args__ = {
        "schema": "raw",
        "comment": "Active reconciled version per work entity and collection scope",
    }

    scope_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_scopes.id"),
        primary_key=True,
        comment="Collection scope identifier",
    )  # noqa: E501
    entity_id: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="OpenAlex work entity identifier"
    )  # noqa: E501
    version_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("raw.openalex_work_versions.id"),
        nullable=False,
        comment="Active work version identifier",
    )  # noqa: E501


class WorkObservations(Base):
    """Sighting history and disposition per work."""

    __tablename__ = "openalex_work_observations"
    __table_args__ = (
        sa.Index("openalex_work_observations_version", "version_id"),
        {"schema": "raw", "comment": "Sighting history and disposition per work"},
    )

    run_id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid(),
        sa.ForeignKey("tmd.openalex_work_runs.id"),
        primary_key=True,
        comment="Execution run identifier",
    )  # noqa: E501
    row_number: Mapped[int] = mapped_column(
        sa.Integer(), primary_key=True, comment="Position within source file or stream"
    )  # noqa: E501
    source_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_sources.id"),
        nullable=False,
        comment="Originating source receipt identifier",
    )  # noqa: E501
    version_id: Mapped[str | None] = mapped_column(
        sa.Text(),
        sa.ForeignKey("raw.openalex_work_versions.id"),
        nullable=True,
        comment="Resolved work version identifier",
    )  # noqa: E501
    disposition: Mapped[str] = mapped_column(
        sa.Text(),
        nullable=False,
        comment="Observation disposition (selected, quarantine, etc.)",
    )  # noqa: E501


class WorkProcessing(Base):
    """Transformation lineage connecting entity versions to derived batches."""

    __tablename__ = "openalex_work_processing"
    __table_args__ = {
        "schema": "raw",
        "comment": "Transformation lineage connecting entity versions to derived batches",
    }

    version_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("raw.openalex_work_versions.id"),
        primary_key=True,
        comment="Entity version identifier",
    )  # noqa: E501
    transformation: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="Transformation name"
    )  # noqa: E501
    dependency: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="Dependency hash or identifier"
    )  # noqa: E501
    batch_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_batches.id"),
        nullable=False,
        comment="Derived transform batch identifier",
    )  # noqa: E501
