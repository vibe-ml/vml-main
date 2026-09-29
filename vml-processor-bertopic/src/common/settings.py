"""Processor settings read from the environment with `.env` overrides."""

from __future__ import annotations

import json
from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from src.corpus.models import CorpusConfig
from src.embeddings.models import EmbeddingConfig
from src.labels.models import LabelConfig
from src.labels.prompts import PROMPT_VERSION
from src.labels.tokenizer import FileTokenizer
from src.models.upstream import UPSTREAM_SCHEMA
from src.topics.models import TopicConfig


class Settings(BaseSettings):
    """Processor configuration.

    Variables use the `PWF_` prefix. Nested corpus fields use a double underscore, for
    example `PWF_CORPUS__SCOPE_IDS='["scope-a"]'`.
    """

    model_config = SettingsConfigDict(
        env_prefix="PWF_",
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore",
    )

    database_url: str
    upstream_schema: str = UPSTREAM_SCHEMA
    alembic_name: str = Field(default="bertopic", pattern=r"^[a-z][a-z0-9_]{0,54}$")
    corpus: CorpusConfig = Field(default_factory=CorpusConfig)

    qdrant_url: str = "http://127.0.0.1:6333"
    qdrant_collection: str = "openalex_tabstract"
    headline_collection: str = "discovery_headlines"
    embedding_model: str = "microsoft/harrier-oss-v1-0.6b"
    embedding_model_revision: str = "main"
    embedding_vector_size: int = 1024
    embedding_context_window: int = Field(default=4096, ge=1)
    embedder_api_urls: Annotated[tuple[str, ...], NoDecode] = ()
    embedding_max_concurrency_per_endpoint: int = 4
    embedding_max_concurrency_overall: int = 6
    embedding_endpoint_failure_limit: int = 3
    embedding_endpoint_max_retries: int = 2
    embedding_retry_backoff_seconds: float = Field(default=0.5, ge=0.0)
    topic_run_max_concurrent: int = Field(
        default=1,
        ge=1,
        description=(
            "Intended Dagster concurrency-pool limit for simultaneous topic fits "
            "(pool name topic_fit). Set the matching pool limit on the Dagster instance."
        ),
    )

    min_cluster_size: int = Field(default=15, ge=2)
    min_samples: int | None = Field(default=None, ge=1)
    umap_n_neighbors: int = Field(default=15, ge=2)

    labeling_api_url: str = "https://foundation-models.api.cloud.ru/v1"
    labeling_api_key: str = ""
    labeling_model: str = "Qwen/Qwen3.6-35B-A3B"
    labeling_model_revision: str = "main"
    labeling_sample_size: int = Field(default=15, ge=1, le=15)
    labeling_prompt_version: str = PROMPT_VERSION
    labeling_context_window: int = Field(default=32768, ge=1)
    labeling_output_reserve: int = Field(default=512, ge=1)
    labeling_max_retries: int = Field(default=2, ge=0)
    labeling_retry_backoff_seconds: float = Field(default=0.5, ge=0.0)
    labeling_max_concurrency: int = Field(default=4, ge=1)
    labeling_tokenizer_path: str | None = None

    @field_validator("embedder_api_urls", mode="before")
    @classmethod
    def _split_embedder_urls(cls, value: object) -> object:
        """Accept a comma-separated string from `.env` as well as a JSON list."""
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("["):
                return json.loads(stripped)
            return tuple(part.strip() for part in stripped.split(",") if part.strip())
        return value

    def embedding_config(self) -> EmbeddingConfig:
        """Build the embeddings-stage config from flat settings fields."""
        return EmbeddingConfig(
            model=self.embedding_model,
            model_revision=self.embedding_model_revision,
            collection=self.qdrant_collection,
            vector_size=self.embedding_vector_size,
            context_window=self.embedding_context_window,
            max_concurrency_per_endpoint=self.embedding_max_concurrency_per_endpoint,
            max_concurrency_overall=self.embedding_max_concurrency_overall,
            endpoint_failure_limit=self.embedding_endpoint_failure_limit,
            endpoint_max_retries=self.embedding_endpoint_max_retries,
            retry_backoff_seconds=self.embedding_retry_backoff_seconds,
        )

    def headline_embedding_config(self) -> EmbeddingConfig:
        """Build embedding config for discovery headlines (separate collection)."""
        return EmbeddingConfig(
            model=self.embedding_model,
            model_revision=self.embedding_model_revision,
            collection=self.headline_collection,
            vector_size=self.embedding_vector_size,
            context_window=self.embedding_context_window,
            max_concurrency_per_endpoint=self.embedding_max_concurrency_per_endpoint,
            max_concurrency_overall=self.embedding_max_concurrency_overall,
            endpoint_failure_limit=self.embedding_endpoint_failure_limit,
            endpoint_max_retries=self.embedding_endpoint_max_retries,
            retry_backoff_seconds=self.embedding_retry_backoff_seconds,
        )

    def topic_config(self) -> TopicConfig:
        """Build the topics-stage config from flat settings fields."""
        return TopicConfig(
            min_cluster_size=self.min_cluster_size,
            min_samples=self.min_samples,
            umap_n_neighbors=self.umap_n_neighbors,
            embedding_model=self.embedding_model,
            embedding_model_revision=self.embedding_model_revision,
        )

    def label_config(self) -> LabelConfig:
        """Build the labels-stage config from flat settings fields."""
        return LabelConfig(
            model=self.labeling_model,
            model_revision=self.labeling_model_revision,
            sample_size=self.labeling_sample_size,
            prompt_version=self.labeling_prompt_version,
            context_window=self.labeling_context_window,
            output_reserve=self.labeling_output_reserve,
            max_retries=self.labeling_max_retries,
            retry_backoff_seconds=self.labeling_retry_backoff_seconds,
            max_concurrency=self.labeling_max_concurrency,
        )

    def labeling_tokenizer(self) -> FileTokenizer:
        """Build the labeling tokenizer from a local tokenizer path.

        Uses the Hugging Face ``tokenizers`` library against
        ``labeling_tokenizer_path`` (a ``tokenizer.json`` file or its parent
        directory). Does not download ``labeling_model`` from Hugging Face;
        place the tokenizer files for that model id on disk first.
        """
        if not self.labeling_tokenizer_path:
            raise ValueError(
                "PWF_LABELING_TOKENIZER_PATH must point at local tokenizer files "
                f"for model {self.labeling_model!r}"
            )
        return FileTokenizer(self.labeling_tokenizer_path)
