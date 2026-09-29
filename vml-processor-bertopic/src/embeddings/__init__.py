"""Embeddings stage: embed a frozen corpus and cache vectors in Qdrant."""

from src.embeddings.client import HttpxEmbeddingClient
from src.embeddings.embed import embed_corpus
from src.embeddings.models import (
    EmbedCorpusResult,
    EmbeddingClient,
    EmbeddingConfig,
    EmbeddingError,
    VectorAlignmentError,
    VectorHandle,
)
from src.embeddings.parallel import ParallelEmbeddingClient
from src.embeddings.vectors import (
    corpus_with_cached_vectors,
    load_embedding_matrix,
    load_vectors_by_work,
    vector_handle_for_corpus,
)

__all__ = [
    "EmbedCorpusResult",
    "EmbeddingClient",
    "EmbeddingConfig",
    "EmbeddingError",
    "HttpxEmbeddingClient",
    "ParallelEmbeddingClient",
    "VectorAlignmentError",
    "VectorHandle",
    "corpus_with_cached_vectors",
    "embed_corpus",
    "load_embedding_matrix",
    "load_vectors_by_work",
    "vector_handle_for_corpus",
]
