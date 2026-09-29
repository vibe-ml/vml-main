"""Embed a frozen corpus and cache the vectors in Qdrant."""

from __future__ import annotations

import hashlib
import time
import uuid
from collections import Counter
from collections.abc import Sequence
from typing import Any, NamedTuple

from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.models import Distance, PointStruct, VectorParams

from src.common.log import get_logger
from src.corpus.models import Corpus, CorpusWork
from src.embeddings.models import (
    EmbedCorpusResult,
    EmbeddingClient,
    EmbeddingConfig,
    EmbeddingError,
    VectorAlignmentError,
    VectorHandle,
)
from src.embeddings.parallel import ParallelEmbeddingClient

log = get_logger("embeddings")

# Stable namespace so the same cache identity always yields the same point UUID.
_POINT_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "vml-processor-bertopic/embeddings")
_EMBED_BATCH_SIZE = 64
_UPSERT_BATCH_SIZE = 256
# Qdrant retrieve of tens of thousands of points (esp. with vectors) times out as one call.
_RETRIEVE_BATCH_SIZE = 256


class _PlannedWork(NamedTuple):
    """One corpus work with its embedding text and cache identity."""

    work: CorpusWork
    text: str
    text_hash: str
    point_id: str


def embed_corpus(
    corpus: Corpus,
    config: EmbeddingConfig,
    embedding_client: EmbeddingClient,
    qdrant: QdrantClient,
) -> EmbedCorpusResult:
    """Embed each work in `corpus` and cache the vectors in Qdrant.

    Reuses an existing point when the cache identity matches. Calls `embedding_client`
    only for works still missing from the collection. Works that exhaust retries are
    skipped and counted rather than aborting the stage, as long as at least one
    vector is available afterward. Verifies vector count and order for kept works.

    Args:
        corpus: Frozen manifest of works to embed, ordered by work ID.
        config: Model, revision, collection, and vector size.
        embedding_client: Port that turns embedding text into vectors.
        qdrant: Qdrant client pointing at the target store.

    Returns:
        Vector handle for works that have vectors (cache hits plus this-run
        successes), plus this-run counters. Full cache hits leave processed,
        succeeded, failed, and saved at 0.

    Raises:
        EmbeddingError: The collection could not be prepared, upsert failed, or
            every missing work failed with no cache hits to fall back on.
        VectorAlignmentError: Returned vectors do not match the kept works.
    """
    started = time.monotonic()
    _ensure_collection(qdrant, config)

    planned = tuple(_plan_work(work, config) for work in corpus.works)
    existing = _retrieve_existing(
        qdrant, config.collection, [item.point_id for item in planned]
    )
    missing = [item for item in planned if item.point_id not in existing]

    succeeded_ids: set[str] = set()
    failed_count = 0
    succeeded_count = 0
    if missing:
        outcomes = _embed_allowing_failures(
            embedding_client, [item.text for item in missing]
        )
        points_by_id: dict[str, PointStruct] = {}
        for item, vector in zip(missing, outcomes, strict=True):
            if vector is None:
                failed_count += 1
                log.warning(
                    "embedding_work_skipped",
                    work_id=item.work.work_id,
                    point_id=item.point_id,
                )
                continue
            succeeded_count += 1
            succeeded_ids.add(item.point_id)
            points_by_id[item.point_id] = PointStruct(
                id=item.point_id,
                vector=vector,
                payload=_payload(item.work, config, item.text_hash),
            )
        if points_by_id:
            _upsert_batched(qdrant, config.collection, list(points_by_id.values()))
        if failed_count == len(missing) and not existing:
            raise EmbeddingError(
                "no usable embedding endpoint remains after failures"
            )

    kept = [
        item
        for item in planned
        if item.point_id in existing or item.point_id in succeeded_ids
    ]
    point_ids = tuple(item.point_id for item in kept)
    kept_works = tuple(item.work for item in kept)
    _verify_alignment(qdrant, config.collection, kept_works, point_ids)
    sent = len(missing)

    log.info(
        "corpus_embedded",
        collection=config.collection,
        selected=len(corpus.works),
        embedded=succeeded_count,
        embeddings_failed=failed_count,
        cache_hits=len(planned) - len(missing),
        model=config.model,
        model_revision=config.model_revision,
        elapsed_seconds=round(time.monotonic() - started, 3),
    )
    return EmbedCorpusResult(
        handle=VectorHandle(collection=config.collection, point_ids=point_ids),
        embeddings_processed=sent,
        embeddings_succeeded=succeeded_count,
        embeddings_failed=failed_count,
        embeddings_saved=succeeded_count,
    )


def embedding_text(work: CorpusWork) -> str:
    """Build the encoder input: title and abstract joined by a newline, or abstract alone."""
    if not work.title:
        return work.abstract
    return f"{work.title}\n{work.abstract}"


def cache_point_id(
    *,
    text_hash: str,
    model: str,
    model_revision: str,
    input_format_version: str,
) -> str:
    """Return the UUIDv5 point ID for a cache identity."""
    identity = f"{text_hash}|{model}|{model_revision}|{input_format_version}"
    return str(uuid.uuid5(_POINT_NAMESPACE, identity))


def _plan_work(work: CorpusWork, config: EmbeddingConfig) -> _PlannedWork:
    text = embedding_text(work)
    text_hash = _sha256(text)
    point_id = cache_point_id(
        text_hash=text_hash,
        model=config.model,
        model_revision=config.model_revision,
        input_format_version=config.input_format_version,
    )
    return _PlannedWork(work=work, text=text, text_hash=text_hash, point_id=point_id)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _payload(
    work: CorpusWork, config: EmbeddingConfig, text_hash: str
) -> dict[str, str | None]:
    return {
        "work_id": work.work_id,
        "work_version_id": work.work_version_id,
        "model": config.model,
        "model_revision": config.model_revision,
        "text_hash": text_hash,
        "publication_date": work.publication_date.isoformat(),
        "primary_domain": work.primary_domain_id,
        "language": work.language,
    }


def _ensure_collection(qdrant: QdrantClient, config: EmbeddingConfig) -> None:
    if qdrant.collection_exists(config.collection):
        return
    try:
        qdrant.create_collection(
            collection_name=config.collection,
            vectors_config=VectorParams(
                size=config.vector_size, distance=Distance.COSINE
            ),
        )
    except UnexpectedResponse as error:
        raise EmbeddingError(
            f"qdrant collection {config.collection!r} could not be created: {error}"
        ) from error


def _retrieve_existing(
    qdrant: QdrantClient, collection: str, point_ids: Sequence[str]
) -> set[str]:
    if not point_ids:
        return set()
    found: set[str] = set()
    ids = list(point_ids)
    for start in range(0, len(ids), _RETRIEVE_BATCH_SIZE):
        chunk = ids[start : start + _RETRIEVE_BATCH_SIZE]
        points = qdrant.retrieve(
            collection_name=collection,
            ids=chunk,
            with_payload=False,
            with_vectors=False,
        )
        found.update(str(point.id) for point in points)
    return found


def _embed_allowing_failures(
    embedding_client: EmbeddingClient, texts: Sequence[str]
) -> list[list[float] | None]:
    """Embed texts; return ``None`` for individuals that exhaust retries."""
    if isinstance(embedding_client, ParallelEmbeddingClient):
        return embedding_client.embed_allowing_failures(texts)

    outcomes: list[list[float] | None] = []
    for start in range(0, len(texts), _EMBED_BATCH_SIZE):
        chunk = list(texts[start : start + _EMBED_BATCH_SIZE])
        try:
            part = embedding_client.embed(chunk)
        except EmbeddingError:
            for text in chunk:
                try:
                    one = embedding_client.embed([text])
                except EmbeddingError:
                    outcomes.append(None)
                    continue
                if len(one) != 1:
                    raise VectorAlignmentError(
                        f"embedding client returned {len(one)} vectors for 1 text"
                    )
                outcomes.append(one[0])
            continue
        if len(part) != len(chunk):
            raise VectorAlignmentError(
                f"embedding client returned {len(part)} vectors for {len(chunk)} texts"
            )
        outcomes.extend(part)
    return outcomes


def _upsert_batched(
    qdrant: QdrantClient, collection: str, points: Sequence[PointStruct]
) -> None:
    try:
        for start in range(0, len(points), _UPSERT_BATCH_SIZE):
            qdrant.upsert(
                collection_name=collection,
                points=list(points[start : start + _UPSERT_BATCH_SIZE]),
            )
    except UnexpectedResponse as error:
        raise EmbeddingError(f"qdrant upsert failed: {error}") from error


def _verify_alignment(
    qdrant: QdrantClient,
    collection: str,
    works: Sequence[CorpusWork],
    point_ids: Sequence[str],
) -> None:
    if len(point_ids) != len(works):
        raise VectorAlignmentError(
            f"vector handle has {len(point_ids)} points for {len(works)} works"
        )
    if not point_ids:
        return
    id_counts = Counter(point_ids)
    unique_ids = list(id_counts)
    by_id: dict[str, Any] = {}
    for start in range(0, len(unique_ids), _RETRIEVE_BATCH_SIZE):
        chunk = unique_ids[start : start + _RETRIEVE_BATCH_SIZE]
        points = qdrant.retrieve(
            collection_name=collection,
            ids=chunk,
            with_payload=True,
            with_vectors=True,
        )
        by_id.update({str(point.id): point for point in points})
    if len(by_id) != len(id_counts):
        raise VectorAlignmentError(
            f"qdrant returned {len(by_id)} of {len(id_counts)} expected points"
        )
    for index, (work, point_id) in enumerate(zip(works, point_ids, strict=True)):
        point = by_id.get(point_id)
        if point is None:
            raise VectorAlignmentError(
                f"missing vector for work {work.work_id} at manifest index {index}"
            )
        if point.vector is None:
            raise VectorAlignmentError(
                f"point {point_id} for work {work.work_id} has no vector"
            )
        payload = point.payload or {}
        if payload.get("text_hash") != _sha256(embedding_text(work)):
            raise VectorAlignmentError(
                f"point {point_id} at index {index} has text_hash "
                f"{payload.get('text_hash')!r}, expected the work text"
            )
        # One stored work_id cannot name every work that shares a cache identity.
        if id_counts[point_id] == 1 and payload.get("work_id") != work.work_id:
            raise VectorAlignmentError(
                f"point {point_id} at index {index} has work_id "
                f"{payload.get('work_id')!r}, expected {work.work_id!r}"
            )
