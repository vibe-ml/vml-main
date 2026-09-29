"""SQLAlchemy models for tmd schema technical metadata and orchestration."""

# ruff: noqa: RUF100, RUF012

import uuid
from datetime import date, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class TaxonomyRuns(Base):
    """Taxonomy run lifecycle execution status and errors."""

    __tablename__ = "openalex_taxonomy_runs"
    __table_args__ = (
        sa.CheckConstraint(
            "status IN ('running', 'failed', 'complete')",
            name="openalex_taxonomy_runs_status_check",
        ),  # noqa: E501
        {
            "schema": "tmd",
            "comment": "Taxonomy run lifecycle execution status and errors",
        },
    )

    id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid(), primary_key=True, comment="Taxonomy run identifier"
    )  # noqa: E501
    started_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, comment="Run start timestamp"
    )  # noqa: E501
    ended_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=True,
        comment="Run completion or termination timestamp",
    )  # noqa: E501
    status: Mapped[str] = mapped_column(
        sa.Text(),
        nullable=False,
        comment="Execution status (running, failed, complete)",
    )  # noqa: E501
    error: Mapped[str | None] = mapped_column(
        sa.Text(), nullable=True, comment="Error message if run failed"
    )  # noqa: E501


class TaxonomyRawFiles(Base):
    """Storage metadata and checksums for raw taxonomy API responses."""

    __tablename__ = "openalex_taxonomy_raw_files"
    __table_args__ = (
        sa.Index("openalex_taxonomy_raw_files_attempt", "attempt_id"),
        {
            "schema": "tmd",
            "comment": "Storage metadata and checksums for raw taxonomy API responses",
        },
    )

    sequence: Mapped[int] = mapped_column(
        sa.BigInteger(),
        sa.Identity(always=True),
        primary_key=True,
        comment="Monotonic sequence number",
    )  # noqa: E501
    attempt_id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid(),
        sa.ForeignKey("tmd.openalex_taxonomy_runs.id"),
        nullable=False,
        comment="Originating taxonomy run identifier",
    )  # noqa: E501
    path: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, unique=True, comment="Storage path of raw payload"
    )  # noqa: E501
    source_locator: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Source URL or origin locator"
    )  # noqa: E501
    checksum: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Uncompressed content SHA-256 checksum"
    )  # noqa: E501
    compressed_checksum: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Compressed file SHA-256 checksum"
    )  # noqa: E501
    started_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, comment="Download start timestamp"
    )  # noqa: E501
    ended_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        comment="Download completion timestamp",
    )  # noqa: E501
    bytes: Mapped[int] = mapped_column(
        sa.BigInteger(), nullable=False, comment="Byte size of raw payload"
    )  # noqa: E501


class WorkScopes(Base):
    """Immutable ingestion query definitions and publication date boundaries."""

    __tablename__ = "openalex_work_scopes"
    __table_args__ = {
        "schema": "tmd",
        "comment": "Immutable ingestion query definitions and publication date boundaries",
    }

    id: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="Scope SHA-256 definition hash"
    )  # noqa: E501
    definition: Mapped[dict[str, Any]] = mapped_column(
        postgresql.JSONB(),
        nullable=False,
        comment="Immutable collection scope criteria JSON",
    )  # noqa: E501


class WorkScans(Base):
    """Resumable scan progress checkpoints, request keys, and next row cursors."""

    __tablename__ = "openalex_work_scans"
    __table_args__ = (
        sa.Index("openalex_work_scans_request", "request_key", "status"),
        {
            "schema": "tmd",
            "comment": "Resumable scan progress checkpoints, request keys, and next row cursors",
        },
    )

    id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid(), primary_key=True, comment="Scan session identifier"
    )  # noqa: E501
    scope_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_scopes.id"),
        nullable=False,
        comment="Associated collection scope identifier",
    )  # noqa: E501
    request_key: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Scan partition or request key"
    )  # noqa: E501
    source_request: Mapped[dict[str, Any]] = mapped_column(
        postgresql.JSONB(),
        nullable=False,
        comment="Serialized request context or arguments",
    )  # noqa: E501
    status: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Scan status (active, complete, failed)"
    )  # noqa: E501
    next_row: Mapped[int] = mapped_column(
        sa.BigInteger(),
        nullable=False,
        server_default="0",
        comment="Next record offset or cursor position",
    )  # noqa: E501


class WorkRuns(Base):
    """Work collection execution lifecycle (snapshot, bootstrap, API)."""

    __tablename__ = "openalex_work_runs"
    __table_args__ = {
        "schema": "tmd",
        "comment": "Work collection execution lifecycle (snapshot, bootstrap, API)",
    }

    id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid(), primary_key=True, comment="Work run identifier"
    )  # noqa: E501
    observed_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, comment="Observation timestamp"
    )  # noqa: E501
    lower_date: Mapped[date] = mapped_column(
        sa.Date(), nullable=False, comment="Lower publication date boundary"
    )  # noqa: E501
    upper_date: Mapped[date] = mapped_column(
        sa.Date(), nullable=False, comment="Upper publication date boundary"
    )  # noqa: E501
    status: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Run status (running, failed, complete)"
    )  # noqa: E501
    scope_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_scopes.id"),
        nullable=False,
        comment="Collection scope identifier",
    )  # noqa: E501
    scan_id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid(),
        sa.ForeignKey("tmd.openalex_work_scans.id"),
        nullable=False,
        comment="Scan checkpoint identifier",
    )  # noqa: E501


class WorkSources(Base):
    """Ingested snapshot Parquet files and raw API response file receipts."""

    __tablename__ = "openalex_work_sources"
    __table_args__ = {
        "schema": "tmd",
        "comment": "Ingested snapshot Parquet files and raw API response file receipts",
    }

    id: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="Source receipt identifier"
    )  # noqa: E501
    release: Mapped[str] = mapped_column(
        sa.Text(),
        nullable=False,
        comment="Snapshot release identifier or ingestion tag",
    )  # noqa: E501
    source: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Origin source type or URL"
    )  # noqa: E501
    path: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Local or object storage relative path"
    )  # noqa: E501
    sha256: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="SHA-256 checksum of source file"
    )  # noqa: E501
    bytes: Mapped[int] = mapped_column(
        sa.BigInteger(), nullable=False, comment="Source file byte size"
    )  # noqa: E501
    rows: Mapped[int] = mapped_column(
        sa.BigInteger(), nullable=False, comment="Total records parsed from source"
    )  # noqa: E501


class WorkReleases(Base):
    """Quarterly snapshot release manifest metadata and acquisition status."""

    __tablename__ = "openalex_work_releases"
    __table_args__ = {
        "schema": "tmd",
        "comment": "Quarterly snapshot release manifest metadata and acquisition status",
    }

    id: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="Snapshot release identifier"
    )  # noqa: E501
    release: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Canonical release name"
    )  # noqa: E501
    manifest_path: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Storage path for release manifest"
    )  # noqa: E501
    manifest_source: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Origin source URL of release manifest"
    )  # noqa: E501
    retrieved_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        comment="Manifest retrieval timestamp",
    )  # noqa: E501
    checked_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=True,
        comment="Release verification timestamp",
    )  # noqa: E501
    acquisition_status: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Release acquisition status"
    )  # noqa: E501


class WorkReleaseFiles(Base):
    """Snapshot file inventory manifests and download status."""

    __tablename__ = "openalex_work_release_files"
    __table_args__ = {
        "schema": "tmd",
        "comment": "Snapshot file inventory manifests and download status",
    }

    id: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="Release file identifier"
    )  # noqa: E501
    release_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_releases.id"),
        nullable=False,
        comment="Associated release identifier",
    )  # noqa: E501
    inventory: Mapped[dict[str, Any]] = mapped_column(
        postgresql.JSONB(), nullable=False, comment="Manifest file entry metadata"
    )  # noqa: E501
    source_id: Mapped[str | None] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_sources.id"),
        nullable=True,
        comment="Ingested source receipt identifier",
    )  # noqa: E501
    retrieved_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=True,
        comment="Download completion timestamp",
    )  # noqa: E501


class WorkBaselines(Base):
    """Scope-specific baseline selection configurations and status."""

    __tablename__ = "openalex_work_baselines"
    __table_args__ = (
        sa.UniqueConstraint(
            "release_id",
            "scope_id",
            name="openalex_work_baselines_release_id_scope_id_key",
        ),
        {
            "schema": "tmd",
            "comment": "Scope-specific baseline selection configurations and status",
        },
    )

    id: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="Baseline configuration identifier"
    )  # noqa: E501
    release_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_releases.id"),
        nullable=False,
        comment="Associated release identifier",
    )  # noqa: E501
    scope_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_scopes.id"),
        nullable=False,
        comment="Target collection scope identifier",
    )  # noqa: E501
    chunk_rows: Mapped[int] = mapped_column(
        sa.Integer(), nullable=False, comment="Number of rows per processing chunk"
    )  # noqa: E501
    selection_status: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Baseline processing status"
    )  # noqa: E501


class WorkBatches(Base):
    """Derived Parquet transform batch manifests linked to taxonomy bundles."""

    __tablename__ = "openalex_work_batches"
    __table_args__ = {
        "schema": "tmd",
        "comment": "Derived Parquet transform batch manifests linked to taxonomy bundles",
    }

    id: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="Derived batch identifier"
    )  # noqa: E501
    taxonomy_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("raw.openalex_taxonomy_bundles.id"),
        nullable=False,
        comment="Associated taxonomy bundle identifier",
    )  # noqa: E501
    manifest: Mapped[dict[str, Any]] = mapped_column(
        postgresql.JSONB(), nullable=False, comment="Batch manifest metadata"
    )  # noqa: E501


class WorkChunks(Base):
    """Row-chunk ranges for parallel bootstrap baseline processing."""

    __tablename__ = "openalex_work_chunks"
    __table_args__ = (
        sa.UniqueConstraint(
            "baseline_id",
            "file_id",
            "start_row",
            name="openalex_work_chunks_baseline_id_file_id_start_row_key",
        ),  # noqa: E501
        {
            "schema": "tmd",
            "comment": "Row-chunk ranges for parallel bootstrap baseline processing",
        },
    )

    id: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="Chunk processing identifier"
    )  # noqa: E501
    baseline_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_baselines.id"),
        nullable=False,
        comment="Associated baseline identifier",
    )  # noqa: E501
    file_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_release_files.id"),
        nullable=False,
        comment="Target release file identifier",
    )  # noqa: E501
    start_row: Mapped[int] = mapped_column(
        sa.BigInteger(), nullable=False, comment="Starting row offset in release file"
    )  # noqa: E501
    row_count: Mapped[int] = mapped_column(
        sa.Integer(), nullable=False, comment="Count of rows in this chunk"
    )  # noqa: E501
    batch_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_batches.id"),
        nullable=False,
        comment="Generated derived batch identifier",
    )  # noqa: E501


class WorkPartitions(Base):
    """Daily API refresh partition state machine, leases, and cursor tracking."""

    __tablename__ = "openalex_work_partitions"
    __table_args__ = (
        sa.UniqueConstraint(
            "scan_id",
            "publication_date",
            name="openalex_work_partitions_scan_id_publication_date_key",
        ),  # noqa: E501
        {
            "schema": "tmd",
            "comment": "Daily API refresh partition state machine, leases, and cursor tracking",
        },
    )

    id: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="Partition identifier"
    )  # noqa: E501
    scan_id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid(),
        sa.ForeignKey("tmd.openalex_work_scans.id"),
        nullable=False,
        comment="Associated scan identifier",
    )  # noqa: E501
    publication_date: Mapped[date] = mapped_column(
        sa.Date(), nullable=False, comment="Target partition publication date"
    )  # noqa: E501
    next_cursor: Mapped[str | None] = mapped_column(
        sa.Text(), nullable=True, comment="Resumable API cursor token"
    )  # noqa: E501
    status: Mapped[str] = mapped_column(
        sa.Text(),
        nullable=False,
        comment="Partition status (pending, leased, completed, failed)",
    )  # noqa: E501
    fencing_revision: Mapped[int] = mapped_column(
        sa.BigInteger(),
        nullable=False,
        server_default="0",
        comment="Fencing revision counter for distributed leases",
    )  # noqa: E501
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True), nullable=True, comment="Lease expiration timestamp"
    )  # noqa: E501
    record_count: Mapped[int] = mapped_column(
        sa.BigInteger(),
        nullable=False,
        server_default="0",
        comment="Ingested record count",
    )  # noqa: E501
    request_parameters: Mapped[dict[str, Any] | None] = mapped_column(
        postgresql.JSONB(), nullable=True, comment="API query parameters for partition"
    )  # noqa: E501
    request_fingerprint: Mapped[str | None] = mapped_column(
        sa.Text(), nullable=True, comment="Deterministic hash of query parameters"
    )  # noqa: E501
    generation: Mapped[int] = mapped_column(
        sa.Integer(),
        nullable=False,
        server_default="1",
        comment="Partition retry generation counter",
    )  # noqa: E501
    eligible_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=True,
        comment="Earliest eligibility for next attempt",
    )  # noqa: E501
    seen_cursors: Mapped[list[Any]] = mapped_column(
        postgresql.JSONB(),
        nullable=False,
        server_default="[]",
        comment="List of visited cursors in current generation",
    )  # noqa: E501
    received_count: Mapped[int] = mapped_column(
        sa.BigInteger(),
        nullable=False,
        server_default="0",
        comment="Records received in current generation",
    )  # noqa: E501
    expected_count: Mapped[int | None] = mapped_column(
        sa.BigInteger(), nullable=True, comment="Total expected records reported by API"
    )  # noqa: E501
    completed_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=True,
        comment="Partition completion timestamp",
    )  # noqa: E501


class WorkApiPages(Base):
    """Ingested API response page logs, cursors, timestamps, and staging paths."""

    __tablename__ = "openalex_work_api_pages"
    __table_args__ = {
        "schema": "tmd",
        "comment": "Ingested API response page logs, cursors, timestamps, and staging paths",
    }

    id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid(), primary_key=True, comment="API page log identifier"
    )  # noqa: E501
    partition_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_partitions.id"),
        nullable=False,
        comment="Associated partition identifier",
    )  # noqa: E501
    run_id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid(),
        sa.ForeignKey("tmd.openalex_work_runs.id"),
        nullable=False,
        comment="Associated work run identifier",
    )  # noqa: E501
    source_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_sources.id"),
        nullable=False,
        comment="Associated source receipt identifier",
    )  # noqa: E501
    generation: Mapped[int] = mapped_column(
        sa.Integer(),
        nullable=False,
        comment="Partition generation when page was fetched",
    )  # noqa: E501
    cursor: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="API request cursor for this page"
    )  # noqa: E501
    started_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        comment="Page request dispatch timestamp",
    )  # noqa: E501
    ended_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        comment="Page response received timestamp",
    )  # noqa: E501
    status_code: Mapped[int] = mapped_column(
        sa.Integer(), nullable=False, comment="HTTP response status code"
    )  # noqa: E501
    raw_path: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Storage path of raw API response JSON"
    )  # noqa: E501


class WorkApiAllowances(Base):
    """Daily API request quotas and reserved request counters."""

    __tablename__ = "openalex_work_api_allowances"
    __table_args__ = {
        "schema": "tmd",
        "comment": "Daily API request quotas and reserved request counters",
    }

    day: Mapped[date] = mapped_column(
        sa.Date(), primary_key=True, comment="Allowance calendar date"
    )  # noqa: E501
    reserved: Mapped[int] = mapped_column(
        sa.Integer(),
        nullable=False,
        server_default="0",
        comment="Total reserved API requests for day",
    )  # noqa: E501


class WorkBatchClaims(Base):
    """Distributed worker claims for derived batch transformation tasks."""

    __tablename__ = "openalex_work_batch_claims"
    __table_args__ = (
        sa.UniqueConstraint(
            "source_id",
            "scope_id",
            "transformation",
            "taxonomy_id",
            name="openalex_work_batch_claims_unique",
        ),  # noqa: E501
        {
            "schema": "tmd",
            "comment": "Distributed worker claims for derived batch transformation tasks",
        },
    )

    id: Mapped[str] = mapped_column(
        sa.Text(), primary_key=True, comment="Batch claim identifier"
    )  # noqa: E501
    source_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_sources.id"),
        nullable=False,
        comment="Source receipt identifier",
    )  # noqa: E501
    scope_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_scopes.id"),
        nullable=False,
        comment="Collection scope identifier",
    )  # noqa: E501
    transformation: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Transformation name"
    )  # noqa: E501
    dependency: Mapped[str] = mapped_column(
        sa.Text(), nullable=False, comment="Transformation dependency hash"
    )  # noqa: E501
    taxonomy_id: Mapped[str] = mapped_column(
        sa.Text(),
        sa.ForeignKey("raw.openalex_taxonomy_bundles.id"),
        nullable=False,
        comment="Associated taxonomy bundle identifier",
    )  # noqa: E501
    status: Mapped[str] = mapped_column(
        sa.Text(),
        nullable=False,
        comment="Claim status (pending, claimed, complete, failed)",
    )  # noqa: E501
    claimed_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True), nullable=True, comment="Worker claim timestamp"
    )  # noqa: E501
    batch_id: Mapped[str | None] = mapped_column(
        sa.Text(),
        sa.ForeignKey("tmd.openalex_work_batches.id"),
        nullable=True,
        comment="Derived batch identifier if completed",
    )  # noqa: E501
