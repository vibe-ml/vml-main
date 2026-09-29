"""Embeddings configuration, results, and ports."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field


class EmbeddingConfig(BaseModel):
    """Model, cache, and collection settings for the embeddings stage.

    Attributes:
        model: Embedding model name served by the configured HTTP endpoint.
        model_revision: Model revision pinned for the cache identity.
        collection: Qdrant collection that stores the vectors.
        input_format_version: Version of the title/abstract join format.
        vector_size: Dimensionality expected from the embedding model.
        context_window: Maximum input tokens the embedding model accepts.
        max_concurrency_per_endpoint: In-flight embed calls allowed per host.
        max_concurrency_overall: Cap on in-flight embed calls across all hosts.
        endpoint_failure_limit: Failures after retries before an endpoint is dropped.
        endpoint_max_retries: Transient retries per text before counting a failure.
        retry_backoff_seconds: Base delay before the first embedding retry; doubles each try.
    """

    model_config = ConfigDict(frozen=True)

    model: str = "microsoft/harrier-oss-v1-0.6b"
    model_revision: str = "main"
    collection: str = "openalex_tabstract"
    input_format_version: str = "title_newline_abstract_v1"
    vector_size: int = Field(default=1024, gt=0)
    context_window: int = Field(default=4096, ge=1)
    # Several in-flight requests per GPU host; overall cap still bounds fan-out.
    max_concurrency_per_endpoint: int = Field(default=4, ge=1)
    max_concurrency_overall: int = Field(default=6, ge=1)
    endpoint_failure_limit: int = Field(default=3, ge=1)
    endpoint_max_retries: int = Field(default=2, ge=0)
    retry_backoff_seconds: float = Field(default=0.5, ge=0.0)


@dataclass(frozen=True)
class VectorHandle:
    """Qdrant collection and point IDs aligned to a corpus manifest."""

    collection: str
    point_ids: tuple[str, ...]


@dataclass(frozen=True)
class EmbedCorpusResult:
    """Vector handle plus this-run embedding counters.

    Cache hits are not processed and are not saved. Works that exhaust retries
    are counted in ``embeddings_failed``, omitted from ``handle``, and do not
    abort the stage when at least one other work succeeded this run.

    Attributes:
        handle: Collection and point IDs for works that have vectors (cache hits
            plus this-run successes), in corpus order.
        embeddings_processed: Works sent to the embedding client this run.
        embeddings_succeeded: Works that received a vector this run.
        embeddings_failed: Works that exhausted retries this run and were skipped.
        embeddings_saved: Points upserted this run.
    """

    handle: VectorHandle
    embeddings_processed: int
    embeddings_succeeded: int
    embeddings_failed: int
    embeddings_saved: int


class EmbeddingError(RuntimeError):
    """The embeddings stage could not produce a usable vector handle."""


class VectorAlignmentError(EmbeddingError):
    """Vector count or order does not match the corpus manifest."""


class EmbeddingClient(Protocol):
    """Port that turns embedding texts into vectors."""

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Return one vector per input text, in the same order."""
