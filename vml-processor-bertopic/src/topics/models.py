"""Topics configuration, results, and errors."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict, Field

COMPOSITION_HASH_ALGORITHM = "sha256"
COMPOSITION_HASH_ENCODING_VERSION = "sorted_work_ids_v1"
COMPOSITION_HASH_SEPARATOR = "\n"


class TopicConfig(BaseModel):
    """Clustering and embedding identity settings for one topic fit.

    Attributes:
        min_cluster_size: HDBSCAN minimum cluster size. Default 15 per the spec.
        min_samples: HDBSCAN min_samples; ``None`` leaves the library default.
        umap_random_state: UMAP seed for reproducible projections.
        umap_metric: UMAP distance metric.
        umap_n_neighbors: UMAP neighborhood size; lower for tiny pilot corpora.
        umap_n_components: UMAP output dimensionality.
        embedding_model: Embedding model name recorded on the run.
        embedding_model_revision: Embedding model revision recorded on the run.
    """

    model_config = ConfigDict(frozen=True)

    min_cluster_size: int = Field(default=15, ge=2)
    min_samples: int | None = Field(default=None, ge=1)
    umap_random_state: int = 42
    umap_metric: str = "cosine"
    umap_n_neighbors: int = Field(default=15, ge=2)
    umap_n_components: int = Field(default=2, ge=2)
    embedding_model: str
    embedding_model_revision: str


@dataclass(frozen=True)
class KeywordWeight:
    """One c-TF-IDF keyword with its weight."""

    term: str
    weight: float


@dataclass(frozen=True)
class PersistedDiscoveryTopic:
    """One discovery topic returned from a successful fit."""

    discovery_topic_id: uuid.UUID
    bertopic_topic_id: int
    composition_hash: str
    size: int
    keywords: tuple[KeywordWeight, ...]
    representative_work_ids: tuple[str, ...]


@dataclass(frozen=True)
class PersistedAssignment:
    """One work's assignment within a topic run."""

    work_id: str
    work_version_id: str
    discovery_topic_id: uuid.UUID | None
    is_outlier: bool
    bertopic_topic_id: int


@dataclass(frozen=True)
class TopicRunResult:
    """Persisted topic run returned by the topics stage."""

    run_id: uuid.UUID
    status: str
    work_count: int
    topic_count: int
    outlier_count: int
    outlier_rate: float
    composition_hash_algorithm: str
    composition_hash_encoding_version: str
    elapsed_seconds: float
    topics: tuple[PersistedDiscoveryTopic, ...]
    assignments: tuple[PersistedAssignment, ...]


class TopicFitError(RuntimeError):
    """The topics stage could not complete a usable fit."""
