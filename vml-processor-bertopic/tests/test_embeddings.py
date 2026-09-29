"""Embeddings stage behaviour, exercised through `embed_corpus` against local Qdrant."""

from __future__ import annotations

import hashlib
import threading
import time
import uuid
from collections.abc import Sequence
from datetime import date
from types import MappingProxyType

import pytest
from qdrant_client import QdrantClient
from structlog.testing import capture_logs

from src.corpus import Corpus, CorpusConfig, CorpusWork, CoverageReport
from src.embeddings import (
    EmbeddingConfig,
    EmbeddingError,
    ParallelEmbeddingClient,
    VectorHandle,
    embed_corpus,
)
from tests.conftest import TestSettings

PHYSICAL_SCIENCES = "https://openalex.org/domains/3"
VECTOR_SIZE = 8


class HashEmbeddingClient:
    """Deterministic fake: maps each text to a fixed vector derived from its hash."""

    def __init__(self, size: int = VECTOR_SIZE) -> None:
        self.size = size

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        return [_vector_for(text, self.size) for text in texts]


class RejectEmbeddingClient:
    """Fails if asked to embed, proving a cache hit issued no new requests."""

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        raise AssertionError(
            f"embedding endpoint must not be called; got {len(texts)} text(s)"
        )


class LatencyHashClient:
    """Hash fake with configurable per-call latency and received-text tracking."""

    def __init__(self, delay_seconds: float, size: int = VECTOR_SIZE) -> None:
        self.delay_seconds = delay_seconds
        self.size = size
        self.received: list[str] = []
        self._lock = threading.Lock()
        self._in_flight = 0
        self.max_in_flight = 0

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        with self._lock:
            self._in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self._in_flight)
            self.received.extend(texts)
        try:
            time.sleep(self.delay_seconds)
            return [_vector_for(text, self.size) for text in texts]
        finally:
            with self._lock:
                self._in_flight -= 1


class FlakyHashClient:
    """Fails a configured number of times, then returns hash vectors."""

    def __init__(
        self,
        *,
        failures_before_success: int,
        size: int = VECTOR_SIZE,
        always_fail: bool = False,
    ) -> None:
        self.failures_before_success = failures_before_success
        self.always_fail = always_fail
        self.size = size
        self.attempts = 0
        self._lock = threading.Lock()

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        with self._lock:
            self.attempts += 1
            attempt = self.attempts
        if self.always_fail or attempt <= self.failures_before_success:
            raise EmbeddingError(f"transient failure on attempt {attempt}")
        return [_vector_for(text, self.size) for text in texts]


class PoisonTextClient:
    """Fails only for texts that contain a marker; otherwise returns hash vectors."""

    def __init__(self, poison: str, size: int = VECTOR_SIZE) -> None:
        self.poison = poison
        self.size = size
        self.attempts = 0
        self._lock = threading.Lock()

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        with self._lock:
            self.attempts += 1
        if any(self.poison in text for text in texts):
            raise EmbeddingError(f"poison text rejected: {self.poison}")
        return [_vector_for(text, self.size) for text in texts]


def _vector_for(text: str, size: int) -> list[float]:
    digest = hashlib.sha256(text.encode()).digest()
    return [((digest[i % len(digest)] / 255.0) * 2.0) - 1.0 for i in range(size)]


def _work(
    number: int,
    *,
    title: str | None = None,
    abstract: str | None = None,
    version: str | None = None,
    published: date | None = None,
    domain: str | None = PHYSICAL_SCIENCES,
    language: str | None = "en",
) -> CorpusWork:
    return CorpusWork(
        work_id=f"https://openalex.org/W{number}",
        work_version_id=version or f"version-{number}",
        title=f"Work {number}" if title is None else title,
        abstract=abstract if abstract is not None else f"Abstract for work {number}.",
        publication_date=published or date(2026, 1, 15),
        primary_domain_id=domain,
        language=language,
    )


def _corpus(*works: CorpusWork) -> Corpus:
    ordered = tuple(sorted(works, key=lambda work: work.work_id))
    return Corpus(
        config=CorpusConfig(scope_ids=("scope-test",)),
        works=ordered,
        coverage=CoverageReport(
            considered=len(ordered),
            selected=len(ordered),
            excluded=MappingProxyType({}),
            unknown_domain=sum(work.unknown_domain for work in ordered),
            missing_title=sum(work.missing_title for work in ordered),
            conflicts=(),
        ),
    )


def _config(collection: str, **overrides: object) -> EmbeddingConfig:
    values: dict[str, object] = {
        "model": "microsoft/harrier-oss-v1-0.6b",
        "model_revision": "rev-test",
        "collection": collection,
        "vector_size": VECTOR_SIZE,
        "retry_backoff_seconds": 0.0,
    }
    values.update(overrides)
    return EmbeddingConfig(**values)  # type: ignore[arg-type]


@pytest.fixture
def qdrant_collection(test_settings: TestSettings):
    """Real local Qdrant client and a collection name unique to this test."""
    client = QdrantClient(url=test_settings.qdrant_url)
    collection = f"emb_test_{uuid.uuid4().hex}"
    try:
        yield client, collection
    finally:
        if client.collection_exists(collection):
            client.delete_collection(collection)
        client.close()


def test_every_work_gets_a_vector_in_qdrant(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    corpus = _corpus(_work(2), _work(1))
    config = _config(collection)
    client = HashEmbeddingClient()

    handle = embed_corpus(corpus, config, client, qdrant).handle

    assert isinstance(handle, VectorHandle)
    assert handle.collection == config.collection
    assert len(handle.point_ids) == len(corpus.works)
    points = qdrant.retrieve(
        collection_name=handle.collection,
        ids=list(handle.point_ids),
        with_vectors=True,
        with_payload=True,
    )
    by_id = {str(point.id): point for point in points}
    for work, point_id in zip(corpus.works, handle.point_ids, strict=True):
        point = by_id[point_id]
        assert point.vector is not None
        assert len(point.vector) == VECTOR_SIZE
        assert point.payload["work_id"] == work.work_id


def test_unchanged_rerun_issues_no_new_embedding_requests(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    corpus = _corpus(_work(1), _work(2))
    config = _config(collection)

    first = embed_corpus(corpus, config, HashEmbeddingClient(), qdrant).handle
    second = embed_corpus(corpus, config, RejectEmbeddingClient(), qdrant).handle

    assert second.point_ids == first.point_ids
    points = qdrant.retrieve(
        collection_name=second.collection,
        ids=list(second.point_ids),
        with_vectors=True,
    )
    assert len(points) == len(corpus.works)
    assert all(point.vector is not None for point in points)


def test_title_or_abstract_change_creates_new_point_leaving_old_intact(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    config = _config(collection)
    original = _corpus(_work(1, abstract="Original abstract."))
    first = embed_corpus(original, config, HashEmbeddingClient(), qdrant).handle
    old_point_id = first.point_ids[0]

    edited = _corpus(_work(1, abstract="Edited abstract.", version="version-1b"))
    second = embed_corpus(edited, config, HashEmbeddingClient(), qdrant).handle
    new_point_id = second.point_ids[0]

    assert new_point_id != old_point_id
    old_points = qdrant.retrieve(
        collection_name=collection, ids=[old_point_id], with_vectors=True
    )
    new_points = qdrant.retrieve(
        collection_name=collection, ids=[new_point_id], with_vectors=True
    )
    assert len(old_points) == 1
    assert old_points[0].vector is not None
    assert len(new_points) == 1
    assert new_points[0].vector is not None
    assert list(old_points[0].vector) != list(new_points[0].vector)


def test_metadata_only_change_reuses_cached_vector(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    config = _config(collection)
    original = _corpus(
        _work(
            1,
            version="version-a",
            published=date(2026, 1, 15),
            domain=PHYSICAL_SCIENCES,
            language="en",
        )
    )
    first = embed_corpus(original, config, HashEmbeddingClient(), qdrant).handle

    metadata_only = _corpus(
        _work(
            1,
            version="version-b",
            published=date(2026, 2, 1),
            domain="https://openalex.org/domains/4",
            language="de",
        )
    )
    second = embed_corpus(metadata_only, config, RejectEmbeddingClient(), qdrant).handle

    assert second.point_ids == first.point_ids
    points = qdrant.retrieve(
        collection_name=collection,
        ids=list(second.point_ids),
        with_vectors=True,
    )
    assert len(points) == 1
    assert points[0].vector is not None


def test_model_or_revision_change_produces_new_vectors(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    corpus = _corpus(_work(1))
    first = embed_corpus(
        corpus,
        _config(collection, model_revision="rev-a"),
        HashEmbeddingClient(),
        qdrant,
    ).handle
    second = embed_corpus(
        corpus,
        _config(collection, model_revision="rev-b"),
        HashEmbeddingClient(),
        qdrant,
    ).handle

    assert second.point_ids != first.point_ids
    for point_id in (*first.point_ids, *second.point_ids):
        points = qdrant.retrieve(
            collection_name=collection, ids=[point_id], with_vectors=True
        )
        assert len(points) == 1
        assert points[0].vector is not None


def test_stored_point_payload_carries_inspectable_metadata(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    work = _work(
        1,
        title="Inspectable title",
        abstract="Inspectable abstract.",
        version="version-payload",
        published=date(2025, 6, 1),
        domain=PHYSICAL_SCIENCES,
        language="fr",
    )
    config = _config(collection, model="test-model", model_revision="rev-payload")
    handle = embed_corpus(_corpus(work), config, HashEmbeddingClient(), qdrant).handle

    point = qdrant.retrieve(
        collection_name=handle.collection,
        ids=[handle.point_ids[0]],
        with_payload=True,
    )[0]
    text = f"{work.title}\n{work.abstract}"
    expected_hash = hashlib.sha256(text.encode()).hexdigest()
    assert point.payload == {
        "work_id": work.work_id,
        "work_version_id": "version-payload",
        "model": "test-model",
        "model_revision": "rev-payload",
        "text_hash": expected_hash,
        "publication_date": "2025-06-01",
        "primary_domain": PHYSICAL_SCIENCES,
        "language": "fr",
    }


def test_empty_title_embeds_abstract_alone(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    work = _work(1, title="", abstract="Abstract only text.")
    config = _config(collection)
    handle = embed_corpus(_corpus(work), config, HashEmbeddingClient(), qdrant).handle

    point = qdrant.retrieve(
        collection_name=handle.collection,
        ids=[handle.point_ids[0]],
        with_payload=True,
    )[0]
    assert (
        point.payload["text_hash"] == hashlib.sha256(b"Abstract only text.").hexdigest()
    )
    titled_hash = hashlib.sha256(b"\nAbstract only text.").hexdigest()
    assert point.payload["text_hash"] != titled_hash


def test_vector_count_mismatch_from_client_aborts(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection

    class ShortClient:
        def embed(self, texts: Sequence[str]) -> list[list[float]]:
            return [_vector_for(texts[0], VECTOR_SIZE)]

    with pytest.raises(EmbeddingError):
        embed_corpus(
            _corpus(_work(1), _work(2)),
            _config(collection),
            ShortClient(),
            qdrant,
        )


def test_misaligned_payload_aborts_rather_than_succeeding(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    corpus = _corpus(_work(1), _work(2))
    config = _config(collection)
    handle = embed_corpus(corpus, config, HashEmbeddingClient(), qdrant).handle

    qdrant.set_payload(
        collection_name=collection,
        payload={"work_id": "https://openalex.org/W-wrong"},
        points=[handle.point_ids[0]],
    )

    with pytest.raises(EmbeddingError):
        embed_corpus(corpus, config, RejectEmbeddingClient(), qdrant)


def _parallel(
    *endpoints: tuple[str, object],
    **kwargs: object,
) -> ParallelEmbeddingClient:
    kwargs.setdefault("retry_backoff_seconds", 0.0)
    return ParallelEmbeddingClient(endpoints, **kwargs)  # type: ignore[arg-type]


def test_three_endpoints_embed_concurrently(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    delay = 0.12
    clients = [LatencyHashClient(delay) for _ in range(3)]
    parallel = _parallel(
        ("ep-a", clients[0]),
        ("ep-b", clients[1]),
        ("ep-c", clients[2]),
        max_concurrency_per_endpoint=1,
        max_concurrency_overall=3,
    )
    corpus = _corpus(*(_work(i) for i in range(1, 10)))
    config = _config(collection)

    started = time.monotonic()
    handle = embed_corpus(corpus, config, parallel, qdrant).handle
    elapsed = time.monotonic() - started

    assert len(handle.point_ids) == 9
    # One worker sequential would need ~9 * delay; three workers finish near 3 * delay.
    assert elapsed < 6 * delay
    assert all(client.received for client in clients)


def test_slow_endpoint_receives_less_work(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    slow = LatencyHashClient(0.2)
    fast_a = LatencyHashClient(0.01)
    fast_b = LatencyHashClient(0.01)
    parallel = _parallel(
        ("slow", slow),
        ("fast-a", fast_a),
        ("fast-b", fast_b),
        max_concurrency_per_endpoint=1,
        max_concurrency_overall=3,
    )
    corpus = _corpus(*(_work(i) for i in range(1, 31)))
    config = _config(collection)

    started = time.monotonic()
    handle = embed_corpus(corpus, config, parallel, qdrant).handle
    elapsed = time.monotonic() - started

    assert len(handle.point_ids) == 30
    # Static thirds would give the slow host ~10 * 0.2s on the critical path.
    assert elapsed < 1.5
    assert len(slow.received) < len(fast_a.received)
    assert len(slow.received) < len(fast_b.received)


def test_failing_endpoint_is_dropped_and_stage_completes(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    broken = FlakyHashClient(failures_before_success=0, always_fail=True)
    healthy_a = HashEmbeddingClient()
    healthy_b = HashEmbeddingClient()
    parallel = _parallel(
        ("broken", broken),
        ("healthy-a", healthy_a),
        ("healthy-b", healthy_b),
        max_concurrency_per_endpoint=1,
        max_concurrency_overall=3,
        failure_limit=2,
        max_retries=0,
    )
    corpus = _corpus(*(_work(i) for i in range(1, 7)))
    config = _config(collection)

    with capture_logs() as captured:
        handle = embed_corpus(corpus, config, parallel, qdrant).handle

    assert len(handle.point_ids) == 6
    assert any(
        entry.get("event") == "embedding_endpoint_dropped"
        and entry.get("endpoint") == "broken"
        for entry in captured
    )


def test_all_endpoints_failing_aborts_with_clear_error(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    parallel = _parallel(
        ("a", FlakyHashClient(failures_before_success=0, always_fail=True)),
        ("b", FlakyHashClient(failures_before_success=0, always_fail=True)),
        ("c", FlakyHashClient(failures_before_success=0, always_fail=True)),
        failure_limit=1,
        max_retries=0,
    )
    with pytest.raises(EmbeddingError, match="no usable embedding endpoint"):
        embed_corpus(
            _corpus(_work(1), _work(2)),
            _config(collection),
            parallel,
            qdrant,
        )


def test_poison_text_is_skipped_without_aborting_siblings(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    """One permanently bad text is counted failed; siblings still embed."""
    qdrant, collection = qdrant_collection
    poison = "POISON_MARKER"
    parallel = _parallel(
        ("a", PoisonTextClient(poison)),
        ("b", PoisonTextClient(poison)),
        failure_limit=10,
        max_retries=0,
    )
    corpus = _corpus(
        _work(1),
        _work(2, title=f"{poison} bad", abstract="Rejected by every host."),
        _work(3),
    )
    result = embed_corpus(corpus, _config(collection), parallel, qdrant)

    assert result.embeddings_processed == 3
    assert result.embeddings_succeeded == 2
    assert result.embeddings_failed == 1
    assert result.embeddings_saved == 2
    assert len(result.handle.point_ids) == 2


def test_transient_failures_are_retried_before_drop(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    # Two failures then success: with max_retries=2 the endpoint stays usable.
    flaky = FlakyHashClient(failures_before_success=2)
    parallel = _parallel(
        ("flaky", flaky),
        max_concurrency_per_endpoint=1,
        failure_limit=1,
        max_retries=2,
    )
    handle = embed_corpus(
        _corpus(_work(1)),
        _config(collection),
        parallel,
        qdrant,
    ).handle
    assert len(handle.point_ids) == 1
    assert flaky.attempts == 3


def test_parallel_from_config_uses_retry_backoff_setting() -> None:
    config = EmbeddingConfig(
        max_concurrency_per_endpoint=2,
        max_concurrency_overall=4,
        retry_backoff_seconds=0.25,
    )
    client = ParallelEmbeddingClient.from_config(
        (("a", HashEmbeddingClient()),),
        config,
    )
    assert client._retry_backoff_seconds == 0.25
    assert client._max_concurrency_per_endpoint == 2
    assert client._max_concurrency_overall == 4


def test_parallel_resume_embeds_only_missing_works(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    config = _config(collection)
    embed_corpus(
        _corpus(_work(1), _work(2)),
        config,
        _parallel(("a", HashEmbeddingClient()), ("b", HashEmbeddingClient())),
        qdrant,
    )

    trackers = [LatencyHashClient(0.0), LatencyHashClient(0.0)]
    handle = embed_corpus(
        _corpus(_work(1), _work(2), _work(3)),
        config,
        _parallel(("a", trackers[0]), ("b", trackers[1])),
        qdrant,
    ).handle

    assert len(handle.point_ids) == 3
    received = trackers[0].received + trackers[1].received
    assert received == ["Work 3\nAbstract for work 3."]


def test_parallel_vectors_match_single_endpoint_content_and_order(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    corpus = _corpus(_work(3), _work(1), _work(2))
    single_collection = f"{collection}_single"
    parallel_collection = f"{collection}_parallel"
    try:
        single = embed_corpus(
            corpus,
            _config(single_collection),
            HashEmbeddingClient(),
            qdrant,
        ).handle
        parallel = embed_corpus(
            corpus,
            _config(parallel_collection),
            _parallel(
                ("a", LatencyHashClient(0.02)),
                ("b", LatencyHashClient(0.05)),
                ("c", LatencyHashClient(0.01)),
            ),
            qdrant,
        ).handle
        assert parallel.point_ids == single.point_ids
        single_points = qdrant.retrieve(
            collection_name=single.collection,
            ids=list(single.point_ids),
            with_vectors=True,
        )
        parallel_points = qdrant.retrieve(
            collection_name=parallel.collection,
            ids=list(parallel.point_ids),
            with_vectors=True,
        )
        single_by_id = {str(p.id): list(p.vector) for p in single_points}
        parallel_by_id = {str(p.id): list(p.vector) for p in parallel_points}
        for point_id in single.point_ids:
            assert parallel_by_id[point_id] == single_by_id[point_id]
    finally:
        for name in (single_collection, parallel_collection):
            if qdrant.collection_exists(name):
                qdrant.delete_collection(name)


def test_concurrency_limits_cap_in_flight_work(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    clients = [LatencyHashClient(0.05) for _ in range(3)]
    config = _config(
        collection,
        max_concurrency_per_endpoint=1,
        max_concurrency_overall=2,
    )
    parallel = ParallelEmbeddingClient.from_config(
        (
            ("a", clients[0]),
            ("b", clients[1]),
            ("c", clients[2]),
        ),
        config,
    )
    embed_corpus(
        _corpus(*(_work(i) for i in range(1, 13))),
        config,
        parallel,
        qdrant,
    )
    assert all(client.max_in_flight <= 1 for client in clients)
    assert parallel.max_in_flight <= 2


def test_payload_never_includes_endpoint_url_or_label(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    handle = embed_corpus(
        _corpus(_work(1)),
        _config(collection),
        _parallel(("http://secret-host:30004", HashEmbeddingClient())),
        qdrant,
    ).handle
    point = qdrant.retrieve(
        collection_name=handle.collection,
        ids=[handle.point_ids[0]],
        with_payload=True,
    )[0]
    assert point.payload is not None
    payload_text = str(point.payload)
    assert "secret-host" not in payload_text
    assert "30004" not in payload_text
    assert "endpoint" not in {key.lower() for key in point.payload}


def test_identical_text_shares_one_point_and_still_aligns(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    corpus = _corpus(
        _work(1, title="Same title", abstract="Same abstract."),
        _work(2, title="Same title", abstract="Same abstract."),
    )

    result = embed_corpus(corpus, _config(collection), HashEmbeddingClient(), qdrant)

    assert result.handle.point_ids[0] == result.handle.point_ids[1]
    assert result.embeddings_processed == 2
    assert result.embeddings_succeeded == 2
    assert result.embeddings_failed == 0
    assert result.embeddings_saved == 2
    points = qdrant.retrieve(
        collection_name=collection,
        ids=[result.handle.point_ids[0]],
        with_vectors=True,
    )
    assert len(points) == 1
    assert points[0].vector is not None


def test_full_miss_counts_every_work_processed_and_saved(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    corpus = _corpus(_work(1), _work(2))

    result = embed_corpus(corpus, _config(collection), HashEmbeddingClient(), qdrant)

    assert result.embeddings_processed == len(corpus.works)
    assert result.embeddings_succeeded == len(corpus.works)
    assert result.embeddings_failed == 0
    assert result.embeddings_saved == len(corpus.works)
    assert len(result.handle.point_ids) == len(corpus.works)


def test_cache_hit_counters_stay_zero(
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    qdrant, collection = qdrant_collection
    corpus = _corpus(_work(1), _work(2))
    config = _config(collection)
    embed_corpus(corpus, config, HashEmbeddingClient(), qdrant)

    result = embed_corpus(corpus, config, RejectEmbeddingClient(), qdrant)

    assert result.embeddings_processed == 0
    assert result.embeddings_succeeded == 0
    assert result.embeddings_failed == 0
    assert result.embeddings_saved == 0
    assert len(result.handle.point_ids) == len(corpus.works)
