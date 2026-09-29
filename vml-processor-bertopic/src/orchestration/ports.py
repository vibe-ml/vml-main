"""Injectable runtime ports for the Dagster assets."""

from __future__ import annotations

from dataclasses import dataclass

from qdrant_client import QdrantClient
from sqlalchemy import Engine

from src.common.settings import Settings
from src.embeddings.models import EmbeddingClient
from src.labels.models import LabelingClient, Tokenizer
from src.topics.fitter import TopicFitter


@dataclass
class PipelinePorts:
    """Collaborators the assets need; tests supply fakes instead of remote hosts."""

    settings: Settings
    engine: Engine
    qdrant: QdrantClient
    embedding_client: EmbeddingClient
    labeling_client: LabelingClient
    tokenizer: Tokenizer
    fitter: TopicFitter | None = None
