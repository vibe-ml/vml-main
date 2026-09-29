"""Load cached embedding matrices from Qdrant without calling embedding hosts."""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Sequence
from typing import Any

import numpy as np
from qdrant_client import QdrantClient

from src.common.log import get_logger
from src.corpus.models import Corpus, CorpusWork
from src.embeddings.embed import cache_point_id, embedding_text
from src.embeddings.models import (
    EmbeddingConfig,
    EmbeddingError,
    VectorAlignmentError,
    VectorHandle,
)

log = get_logger("embeddings")

# Match embed.py upsert/retrieve batch size: bulk retrieve of full corpora times out.
_RETRIEVE_BATCH_SIZE = 256


def vector_handle_for_corpus(corpus: Corpus, config: EmbeddingConfig) -> VectorHandle:
    """Return the deterministic Qdrant point IDs for `corpus` under `config`.

    Does not call an embedding host; IDs follow the same cache identity as
    ``embed_corpus``.
    """
    point_ids = tuple(_point_id(work, config) for work in corpus.works)
    return VectorHandle(collection=config.collection, point_ids=point_ids)


def corpus_with_cached_vectors(
    corpus: Corpus,
    config: EmbeddingConfig,
    qdrant: QdrantClient,
) -> tuple[Corpus, VectorHandle]:
    """Drop works whose cache points are missing from Qdrant.

    Keeps corpus order. Raises when every work is missing so topic fit cannot run.
    """
    if not corpus.works:
        return corpus, VectorHandle(collection=config.collection, point_ids=())

    handle = vector_handle_for_corpus(corpus, config)
    present: set[str] = set()
    ids = list(handle.point_ids)
    for start in range(0, len(ids), _RETRIEVE_BATCH_SIZE):
        chunk = ids[start : start + _RETRIEVE_BATCH_SIZE]
        points = qdrant.retrieve(
            collection_name=handle.collection,
            ids=chunk,
            with_payload=False,
            with_vectors=False,
        )
        present.update(str(point.id) for point in points)
    kept_works = tuple(
        work
        for work, point_id in zip(corpus.works, handle.point_ids, strict=True)
        if point_id in present
    )
    dropped = len(corpus.works) - len(kept_works)
    if dropped:
        log.warning(
            "topic_fit_dropping_works_without_vectors",
            selected=len(corpus.works),
            kept=len(kept_works),
            dropped=dropped,
            collection=config.collection,
        )
    if not kept_works:
        raise EmbeddingError(
            "no cached vectors available for topic fit after embedding failures"
        )
    filtered = Corpus(config=corpus.config, works=kept_works, coverage=corpus.coverage)
    return filtered, vector_handle_for_corpus(filtered, config)


def load_vectors_by_work(
    works: Sequence[CorpusWork],
    config: EmbeddingConfig,
    qdrant: QdrantClient,
) -> dict[str, np.ndarray]:
    """Retrieve dense vectors for works that have a cache point; skip missing ones.

    Does not raise when some points are absent. Callers log shortfalls.
    """
    if not works:
        return {}
    point_ids = tuple(_point_id(work, config) for work in works)
    by_id: dict[str, Any] = {}
    ids = list(dict.fromkeys(point_ids))
    for start in range(0, len(ids), _RETRIEVE_BATCH_SIZE):
        chunk = ids[start : start + _RETRIEVE_BATCH_SIZE]
        points = qdrant.retrieve(
            collection_name=config.collection,
            ids=chunk,
            with_payload=False,
            with_vectors=True,
        )
        by_id.update({str(point.id): point for point in points})

    found: dict[str, np.ndarray] = {}
    for work, point_id in zip(works, point_ids, strict=True):
        point = by_id.get(point_id)
        if point is None:
            continue
        vector = _as_dense_vector(point.vector)
        if vector is None:
            continue
        found[work.work_id] = np.asarray(vector, dtype=np.float64)
    return found


def load_embedding_matrix(
    handle: VectorHandle,
    qdrant: QdrantClient,
    *,
    works: Sequence[CorpusWork] | None = None,
) -> np.ndarray:
    """Retrieve vectors from Qdrant in ``handle.point_ids`` order.

    Args:
        handle: Collection and point IDs aligned to a corpus manifest.
        qdrant: Qdrant client pointing at the store that holds the vectors.
        works: Optional works used only to improve mismatch error messages.

    Returns:
        A 2-D float64 array with one row per point ID.

    Raises:
        VectorAlignmentError: A point is missing, has no vector, or count mismatches.
    """
    if not handle.point_ids:
        return np.zeros((0, 0), dtype=np.float64)

    by_id: dict[str, Any] = {}
    ids = list(handle.point_ids)
    for start in range(0, len(ids), _RETRIEVE_BATCH_SIZE):
        chunk = ids[start : start + _RETRIEVE_BATCH_SIZE]
        points = qdrant.retrieve(
            collection_name=handle.collection,
            ids=chunk,
            with_payload=True,
            with_vectors=True,
        )
        by_id.update({str(point.id): point for point in points})
    # One cache point can back several works. Compare unique ids, not row count.
    expected_ids = set(handle.point_ids)
    if len(by_id) != len(expected_ids):
        raise VectorAlignmentError(
            f"qdrant returned {len(by_id)} of {len(expected_ids)} expected points"
        )

    rows: list[list[float]] = []
    id_counts = Counter(handle.point_ids)
    for index, point_id in enumerate(handle.point_ids):
        point = by_id.get(point_id)
        if point is None:
            work_hint = ""
            if works is not None and index < len(works):
                work_hint = f" for work {works[index].work_id}"
            raise VectorAlignmentError(
                f"missing vector at manifest index {index}{work_hint}"
            )
        vector = _as_dense_vector(point.vector)
        if vector is None:
            raise VectorAlignmentError(f"point {point_id} has no dense vector")
        if works is not None and index < len(works):
            payload = point.payload or {}
            expected = works[index].work_id
            # Shared cache identities store one work_id; only check unique points.
            if id_counts[point_id] == 1 and payload.get("work_id") != expected:
                raise VectorAlignmentError(
                    f"point {point_id} at index {index} has work_id "
                    f"{payload.get('work_id')!r}, expected {expected!r}"
                )
        rows.append(vector)
    return np.asarray(rows, dtype=np.float64)


def _point_id(work: CorpusWork, config: EmbeddingConfig) -> str:
    text = embedding_text(work)
    text_hash = hashlib.sha256(text.encode()).hexdigest()
    return cache_point_id(
        text_hash=text_hash,
        model=config.model,
        model_revision=config.model_revision,
        input_format_version=config.input_format_version,
    )


def _as_dense_vector(raw: object) -> list[float] | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        # Named vectors: take the sole entry or the empty-name default.
        if "" in raw:
            raw = raw[""]
        elif len(raw) == 1:
            raw = next(iter(raw.values()))
        else:
            return None
    if isinstance(raw, np.ndarray):
        return [float(value) for value in raw.tolist()]
    if isinstance(raw, (list, tuple)):
        return [float(value) for value in raw]
    return None
