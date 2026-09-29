"""Emergence stage behaviour, exercised through `observe_emergence` against `pwf_test`."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import date
from types import MappingProxyType

import pytest
from qdrant_client import QdrantClient
from sqlalchemy import Engine, insert, select, text

from src.corpus import Corpus, CorpusConfig, CorpusWork, CoverageReport
from src.embeddings import EmbeddingConfig
from src.emergence import EmergenceError, observe_emergence
from src.models.emergence import EmergenceObservation
from src.models.labels import TopicLabel
from src.models.topics import DiscoveryTopic, TopicRun, WorkTopicAssignment
from tests.conftest import TestSettings
from tests.test_embeddings import (
    HashEmbeddingClient,
    PoisonTextClient,
    RejectEmbeddingClient,
)

PHYSICAL_SCIENCES = "https://openalex.org/domains/3"
HEADLINE_VECTOR_SIZE = 8


def _work(
    n: int,
    *,
    published: date | None = None,
    title: str | None = None,
) -> CorpusWork:
    return CorpusWork(
        work_id=f"https://openalex.org/W{n:04d}",
        work_version_id=f"ver-{n:04d}",
        title=title if title is not None else f"Work {n}",
        abstract=f"Abstract for work {n}.",
        publication_date=published or date(2026, 1, 15),
        primary_domain_id=PHYSICAL_SCIENCES,
        language="en",
    )


def _corpus(
    works: Sequence[CorpusWork],
    *,
    published_from: date = date(2026, 1, 1),
    published_to: date = date(2026, 1, 31),
) -> Corpus:
    ordered = tuple(sorted(works, key=lambda work: work.work_id))
    return Corpus(
        config=CorpusConfig(
            scope_ids=("scope-a",),
            published_from=published_from,
            published_to=published_to,
        ),
        works=ordered,
        coverage=CoverageReport(
            considered=len(ordered),
            selected=len(ordered),
            excluded=MappingProxyType({}),
            unknown_domain=0,
            missing_title=0,
            conflicts=(),
        ),
    )


def _insert_topic_run(
    engine: Engine,
    *,
    run_id: uuid.UUID,
    status: str = "succeeded",
    published_from: date = date(2026, 1, 1),
    published_to: date = date(2026, 1, 31),
    work_count: int = 0,
    topic_count: int = 0,
    outlier_count: int = 0,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            insert(TopicRun.__table__).values(
                run_id=run_id,
                status=status,
                scope_ids=["scope-a"],
                work_types=["article"],
                published_from=published_from,
                published_to=published_to,
                excluded_domain_ids=[],
                embedding_model="test-encoder",
                embedding_model_revision="test-rev",
                min_cluster_size=2,
                min_samples=1,
                umap_random_state=42,
                umap_metric="cosine",
                composition_hash_algorithm="sha256",
                composition_hash_encoding_version="v1",
                work_count=work_count,
                topic_count=topic_count,
                outlier_count=outlier_count,
                outlier_rate=(outlier_count / work_count) if work_count else 0.0,
            )
        )


def _insert_discovery_topic(
    engine: Engine,
    *,
    topic_run_id: uuid.UUID,
    discovery_topic_id: uuid.UUID,
    size: int = 1,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            insert(DiscoveryTopic.__table__).values(
                discovery_topic_id=discovery_topic_id,
                topic_run_id=topic_run_id,
                composition_hash="abc",
                size=size,
                keywords=[{"term": "term", "weight": 1.0}],
                representative_work_ids=[],
            )
        )


def _insert_assignment(
    engine: Engine,
    *,
    topic_run_id: uuid.UUID,
    work: CorpusWork,
    discovery_topic_id: uuid.UUID | None,
    is_outlier: bool,
    bertopic_topic_id: int,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            insert(WorkTopicAssignment.__table__).values(
                topic_run_id=topic_run_id,
                work_id=work.work_id,
                work_version_id=work.work_version_id,
                discovery_topic_id=discovery_topic_id,
                is_outlier=is_outlier,
                bertopic_topic_id=bertopic_topic_id,
            )
        )


def _observation_rows(engine: Engine, topic_run_id: uuid.UUID):
    with engine.connect() as connection:
        return (
            connection.execute(
                select(EmergenceObservation.__table__).where(
                    EmergenceObservation.__table__.c.topic_run_id == topic_run_id
                )
            )
            .mappings()
            .all()
        )


def _insert_topic_label(
    engine: Engine,
    *,
    topic_run_id: uuid.UUID,
    discovery_topic_id: uuid.UUID,
    status: str = "succeeded",
    headline: str | None = "A rare growing theme",
    model: str = "test-label-model",
    model_revision: str = "label-rev-1",
    prompt_version: str = "prompt_v1",
) -> None:
    with engine.begin() as connection:
        connection.execute(
            insert(TopicLabel.__table__).values(
                topic_run_id=topic_run_id,
                discovery_topic_id=discovery_topic_id,
                headline=headline,
                concatenated_summary_text="summary",
                chunk_count=1,
                sampling_method="representative_docs",
                sampled_work_ids=[],
                sample_size=1,
                model=model,
                model_revision=model_revision,
                prompt_version=prompt_version,
                status=status,
            )
        )


def _headline_config(collection: str) -> EmbeddingConfig:
    return EmbeddingConfig(
        model="microsoft/harrier-oss-v1-0.6b",
        model_revision="rev-test",
        collection=collection,
        vector_size=HEADLINE_VECTOR_SIZE,
        retry_backoff_seconds=0.0,
    )


@pytest.fixture
def qdrant_collection(test_settings: TestSettings):
    """Real local Qdrant client and a collection name unique to this test."""
    client = QdrantClient(url=test_settings.qdrant_url)
    collection = f"headline_test_{uuid.uuid4().hex}"
    try:
        yield client, collection
    finally:
        if client.collection_exists(collection):
            client.delete_collection(collection)
        client.close()


def _seed_one_topic_run(
    engine: Engine,
) -> tuple[uuid.UUID, uuid.UUID, Corpus]:
    """One succeeded discovery topic with one member and one outlier."""
    run_id = uuid.uuid4()
    topic_a = uuid.uuid4()
    member = _work(1, published=date(2026, 1, 10))
    outlier = _work(2, published=date(2026, 1, 20))
    corpus = _corpus([member, outlier])
    _insert_topic_run(
        engine, run_id=run_id, work_count=2, topic_count=1, outlier_count=1
    )
    _insert_discovery_topic(
        engine, topic_run_id=run_id, discovery_topic_id=topic_a, size=1
    )
    _insert_assignment(
        engine,
        topic_run_id=run_id,
        work=member,
        discovery_topic_id=topic_a,
        is_outlier=False,
        bertopic_topic_id=0,
    )
    _insert_assignment(
        engine,
        topic_run_id=run_id,
        work=outlier,
        discovery_topic_id=None,
        is_outlier=True,
        bertopic_topic_id=-1,
    )
    return run_id, topic_a, corpus


def test_succeeded_headline_stores_one_point(
    processor_engine: Engine,
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    """A succeeded headline is stored as one point with the six payload fields."""
    qdrant, collection = qdrant_collection
    run_id, topic_a, corpus = _seed_one_topic_run(processor_engine)
    headline = "Rare growing research theme"
    _insert_topic_label(
        processor_engine,
        topic_run_id=run_id,
        discovery_topic_id=topic_a,
        headline=headline,
        model="label-model-a",
        model_revision="rev-a",
        prompt_version="pv1",
    )
    config = _headline_config(collection)

    result = observe_emergence(
        run_id,
        corpus,
        processor_engine,
        embedding_config=config,
        client=HashEmbeddingClient(size=HEADLINE_VECTOR_SIZE),
        qdrant=qdrant,
    )

    assert result.observation_count == 1
    assert result.headlines_processed == 1
    assert result.headlines_saved == 1
    assert result.headlines_cached == 0
    assert result.headlines_failed == 0
    points = qdrant.scroll(collection_name=collection, limit=10, with_payload=True)[0]
    assert len(points) == 1
    payload = points[0].payload or {}
    assert payload == {
        "discovery_topic_id": str(topic_a),
        "topic_run_id": str(run_id),
        "headline": headline,
        "labeling_model": "label-model-a",
        "labeling_revision": "rev-a",
        "prompt_version": "pv1",
    }
    assert "quadrant" not in payload
    assert "keywords" not in payload
    assert "emergence_quadrant" not in payload


def test_failed_and_missing_labels_produce_no_points(
    processor_engine: Engine,
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    """Failed and missing labels leave observations but no headline points."""
    qdrant, collection = qdrant_collection
    run_id = uuid.uuid4()
    topic_failed = uuid.uuid4()
    topic_missing = uuid.uuid4()
    member_a = _work(1, published=date(2026, 1, 10))
    member_b = _work(2, published=date(2026, 1, 12))
    corpus = _corpus([member_a, member_b])
    _insert_topic_run(
        processor_engine, run_id=run_id, work_count=2, topic_count=2, outlier_count=0
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_failed, size=1
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_missing, size=1
    )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=member_a,
        discovery_topic_id=topic_failed,
        is_outlier=False,
        bertopic_topic_id=0,
    )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=member_b,
        discovery_topic_id=topic_missing,
        is_outlier=False,
        bertopic_topic_id=1,
    )
    _insert_topic_label(
        processor_engine,
        topic_run_id=run_id,
        discovery_topic_id=topic_failed,
        status="failed",
        headline=None,
    )
    config = _headline_config(collection)

    result = observe_emergence(
        run_id,
        corpus,
        processor_engine,
        embedding_config=config,
        client=HashEmbeddingClient(size=HEADLINE_VECTOR_SIZE),
        qdrant=qdrant,
    )

    assert result.observation_count == 2
    assert len(_observation_rows(processor_engine, run_id)) == 2
    assert result.headlines_processed == 0
    assert result.headlines_saved == 0
    if qdrant.collection_exists(collection):
        points = qdrant.scroll(collection_name=collection, limit=10)[0]
        assert points == []
    else:
        assert result.headlines_cached == 0


def test_unchanged_headline_rerun_skips_embed(
    processor_engine: Engine,
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    """A second pass with an unchanged headline issues no embedding request."""
    qdrant, collection = qdrant_collection
    run_id, topic_a, corpus = _seed_one_topic_run(processor_engine)
    _insert_topic_label(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_a
    )
    config = _headline_config(collection)

    first = observe_emergence(
        run_id,
        corpus,
        processor_engine,
        embedding_config=config,
        client=HashEmbeddingClient(size=HEADLINE_VECTOR_SIZE),
        qdrant=qdrant,
    )
    points_after_first = qdrant.scroll(
        collection_name=collection, limit=10, with_payload=True
    )[0]
    assert first.headlines_saved == 1
    assert len(points_after_first) == 1
    first_point_id = points_after_first[0].id

    second = observe_emergence(
        run_id,
        corpus,
        processor_engine,
        embedding_config=config,
        client=RejectEmbeddingClient(),
        qdrant=qdrant,
    )

    assert second.headlines_cached == 1
    assert second.headlines_processed == 0
    assert second.headlines_saved == 0
    points_after_second = qdrant.scroll(
        collection_name=collection, limit=10, with_payload=True
    )[0]
    assert len(points_after_second) == 1
    assert points_after_second[0].id == first_point_id


def test_changed_headline_or_model_creates_new_point(
    processor_engine: Engine,
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    """Changed headline text or labeling model creates a new point; old stays."""
    qdrant, collection = qdrant_collection
    run_id, topic_a, corpus = _seed_one_topic_run(processor_engine)
    _insert_topic_label(
        processor_engine,
        topic_run_id=run_id,
        discovery_topic_id=topic_a,
        headline="Original headline",
        model="model-v1",
        model_revision="rev-1",
    )
    config = _headline_config(collection)

    observe_emergence(
        run_id,
        corpus,
        processor_engine,
        embedding_config=config,
        client=HashEmbeddingClient(size=HEADLINE_VECTOR_SIZE),
        qdrant=qdrant,
    )
    first_points = qdrant.scroll(
        collection_name=collection, limit=10, with_payload=True
    )[0]
    assert len(first_points) == 1
    first_id = first_points[0].id
    first_payload = first_points[0].payload or {}

    with processor_engine.begin() as connection:
        connection.execute(
            TopicLabel.__table__.update()
            .where(TopicLabel.__table__.c.topic_run_id == run_id)
            .where(TopicLabel.__table__.c.discovery_topic_id == topic_a)
            .values(headline="Edited headline text")
        )

    observe_emergence(
        run_id,
        corpus,
        processor_engine,
        embedding_config=config,
        client=HashEmbeddingClient(size=HEADLINE_VECTOR_SIZE),
        qdrant=qdrant,
    )
    after_text = qdrant.scroll(collection_name=collection, limit=10, with_payload=True)[
        0
    ]
    assert len(after_text) == 2
    assert qdrant.retrieve(collection_name=collection, ids=[first_id])[0].payload == (
        first_payload
    )

    with processor_engine.begin() as connection:
        connection.execute(
            TopicLabel.__table__.update()
            .where(TopicLabel.__table__.c.topic_run_id == run_id)
            .where(TopicLabel.__table__.c.discovery_topic_id == topic_a)
            .values(model="model-v2")
        )

    observe_emergence(
        run_id,
        corpus,
        processor_engine,
        embedding_config=config,
        client=HashEmbeddingClient(size=HEADLINE_VECTOR_SIZE),
        qdrant=qdrant,
    )
    after_model = qdrant.scroll(
        collection_name=collection, limit=10, with_payload=True
    )[0]
    assert len(after_model) == 3
    assert qdrant.retrieve(collection_name=collection, ids=[first_id])[0].payload == (
        first_payload
    )


def test_second_pass_embeds_late_label_keeps_created_at(
    processor_engine: Engine,
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    """First pass with no labels writes observations; later label embeds without rewrite."""
    qdrant, collection = qdrant_collection
    run_id, topic_a, corpus = _seed_one_topic_run(processor_engine)
    config = _headline_config(collection)

    first = observe_emergence(
        run_id,
        corpus,
        processor_engine,
        embedding_config=config,
        client=HashEmbeddingClient(size=HEADLINE_VECTOR_SIZE),
        qdrant=qdrant,
    )
    rows_after_first = _observation_rows(processor_engine, run_id)
    assert first.observation_count == 1
    assert first.headlines_saved == 0
    assert len(rows_after_first) == 1
    created_at = rows_after_first[0]["created_at"]
    if qdrant.collection_exists(collection):
        assert qdrant.scroll(collection_name=collection, limit=10)[0] == []

    _insert_topic_label(
        processor_engine,
        topic_run_id=run_id,
        discovery_topic_id=topic_a,
        headline="Late arriving headline",
    )

    second = observe_emergence(
        run_id,
        corpus,
        processor_engine,
        embedding_config=config,
        client=HashEmbeddingClient(size=HEADLINE_VECTOR_SIZE),
        qdrant=qdrant,
    )
    rows_after_second = _observation_rows(processor_engine, run_id)

    assert second.headlines_saved == 1
    assert len(rows_after_second) == 1
    assert rows_after_second[0]["created_at"] == created_at
    points = qdrant.scroll(collection_name=collection, limit=10, with_payload=True)[0]
    assert len(points) == 1
    assert points[0].payload["headline"] == "Late arriving headline"


def test_partial_headline_failure_continues_total_failure_raises(
    processor_engine: Engine,
    qdrant_collection: tuple[QdrantClient, str],
) -> None:
    """One of two headline failures stores the other; sole failure raises with row kept."""
    qdrant, collection = qdrant_collection
    run_id = uuid.uuid4()
    topic_ok = uuid.uuid4()
    topic_bad = uuid.uuid4()
    member_a = _work(1, published=date(2026, 1, 10))
    member_b = _work(2, published=date(2026, 1, 12))
    corpus = _corpus([member_a, member_b])
    _insert_topic_run(
        processor_engine, run_id=run_id, work_count=2, topic_count=2, outlier_count=0
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_ok, size=1
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_bad, size=1
    )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=member_a,
        discovery_topic_id=topic_ok,
        is_outlier=False,
        bertopic_topic_id=0,
    )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=member_b,
        discovery_topic_id=topic_bad,
        is_outlier=False,
        bertopic_topic_id=1,
    )
    _insert_topic_label(
        processor_engine,
        topic_run_id=run_id,
        discovery_topic_id=topic_ok,
        headline="Healthy headline",
    )
    _insert_topic_label(
        processor_engine,
        topic_run_id=run_id,
        discovery_topic_id=topic_bad,
        headline="POISON bad headline",
    )
    config = _headline_config(collection)

    result = observe_emergence(
        run_id,
        corpus,
        processor_engine,
        embedding_config=config,
        client=PoisonTextClient("POISON", size=HEADLINE_VECTOR_SIZE),
        qdrant=qdrant,
    )
    assert result.headlines_saved == 1
    assert result.headlines_failed == 1
    assert len(_observation_rows(processor_engine, run_id)) == 2
    points = qdrant.scroll(collection_name=collection, limit=10, with_payload=True)[0]
    assert len(points) == 1
    assert points[0].payload["headline"] == "Healthy headline"

    # Sole headline failure: observations stay, stage raises.
    run_id_only = uuid.uuid4()
    topic_only = uuid.uuid4()
    member_only = _work(3, published=date(2026, 1, 15))
    corpus_only = _corpus([member_only])
    _insert_topic_run(
        processor_engine,
        run_id=run_id_only,
        work_count=1,
        topic_count=1,
        outlier_count=0,
    )
    _insert_discovery_topic(
        processor_engine,
        topic_run_id=run_id_only,
        discovery_topic_id=topic_only,
        size=1,
    )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id_only,
        work=member_only,
        discovery_topic_id=topic_only,
        is_outlier=False,
        bertopic_topic_id=0,
    )
    _insert_topic_label(
        processor_engine,
        topic_run_id=run_id_only,
        discovery_topic_id=topic_only,
        headline="POISON only",
    )
    only_collection = f"{collection}_only"
    only_config = _headline_config(only_collection)
    try:
        with pytest.raises(EmergenceError, match="no usable embedding"):
            observe_emergence(
                run_id_only,
                corpus_only,
                processor_engine,
                embedding_config=only_config,
                client=PoisonTextClient("POISON", size=HEADLINE_VECTOR_SIZE),
                qdrant=qdrant,
            )
        assert len(_observation_rows(processor_engine, run_id_only)) == 1
    finally:
        if qdrant.collection_exists(only_collection):
            qdrant.delete_collection(only_collection)


def test_headline_collection_comes_from_config() -> None:
    """Headline collection default lives on Settings; tests pass config collection."""
    from src.common.settings import Settings

    settings = Settings(database_url="postgresql+psycopg://unused@localhost/unused")
    assert settings.headline_collection == "discovery_headlines"
    assert settings.headline_embedding_config().collection == "discovery_headlines"
    assert settings.embedding_config().collection == "openalex_tabstract"


def test_non_succeeded_run_is_refused(processor_engine: Engine) -> None:
    """A topic run that is not succeeded raises and writes no observation rows."""
    run_id = uuid.uuid4()
    _insert_topic_run(processor_engine, run_id=run_id, status="failed")
    corpus = _corpus([])

    with pytest.raises(EmergenceError, match="expected succeeded"):
        observe_emergence(run_id, corpus, processor_engine)

    assert _observation_rows(processor_engine, run_id) == []


def test_january_only_one_observation_per_discovery_topic(
    processor_engine: Engine,
) -> None:
    """A January-only succeeded run stores one observation per discovery topic.

    Topic -1 (outlier) has no observation. One member and one outlier in the
    same month yield share 0.5. A discovery topic with no members stores share 0.
    Coverage is insufficient historical coverage.
    """
    run_id = uuid.uuid4()
    topic_a = uuid.uuid4()
    topic_b = uuid.uuid4()
    member_a = _work(1, published=date(2026, 1, 10))
    outlier = _work(3, published=date(2026, 1, 20))
    corpus = _corpus([member_a, outlier])

    _insert_topic_run(
        processor_engine,
        run_id=run_id,
        work_count=2,
        topic_count=2,
        outlier_count=1,
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_a, size=1
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_b, size=0
    )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=member_a,
        discovery_topic_id=topic_a,
        is_outlier=False,
        bertopic_topic_id=0,
    )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=outlier,
        discovery_topic_id=None,
        is_outlier=True,
        bertopic_topic_id=-1,
    )

    result = observe_emergence(run_id, corpus, processor_engine)

    assert result.topic_run_id == run_id
    assert result.observation_count == 2
    rows = _observation_rows(processor_engine, run_id)
    assert len(rows) == 2
    assert {row["discovery_topic_id"] for row in rows} == {topic_a, topic_b}

    by_topic = {row["discovery_topic_id"]: row for row in rows}
    assert by_topic[topic_a]["monthly_series"] == [
        {
            "year_month": "2026-01",
            "topic_count": 1,
            "corpus_count": 2,
            "share": 0.5,
        }
    ]
    assert by_topic[topic_b]["monthly_series"] == [
        {
            "year_month": "2026-01",
            "topic_count": 0,
            "corpus_count": 2,
            "share": 0.0,
        }
    ]
    for row in rows:
        assert row["coverage_status"] == "insufficient_historical_coverage"
        assert row["mean_share"] is None
        assert row["annual_growth"] is None
        assert row["emergence_quadrant"] is None
        assert row["monthly_share_change"] is None
        assert row["valid_year_pair_count"] is None
        assert row["complete_years"] == []
        assert row["metric_version"] == "wisdom_tem_v1"


def test_two_complete_months_stores_monthly_share_change(
    processor_engine: Engine,
) -> None:
    """Two complete months with shares 0.25 then 0.5 store change exactly 1.0.

    January: 1 of 4 works in the topic. February: 1 of 2. The adjacent positive
    ratio is 2, so monthly share change is 1.0. Quadrant stays unset.
    """
    run_id = uuid.uuid4()
    topic_a = uuid.uuid4()
    jan_member = _work(1, published=date(2026, 1, 10))
    jan_other_a = _work(2, published=date(2026, 1, 12))
    jan_other_b = _work(3, published=date(2026, 1, 14))
    jan_other_c = _work(4, published=date(2026, 1, 16))
    feb_member = _work(5, published=date(2026, 2, 10))
    feb_other = _work(6, published=date(2026, 2, 20))
    corpus = _corpus(
        [jan_member, jan_other_a, jan_other_b, jan_other_c, feb_member, feb_other],
        published_from=date(2026, 1, 1),
        published_to=date(2026, 2, 28),
    )

    _insert_topic_run(
        processor_engine,
        run_id=run_id,
        published_from=date(2026, 1, 1),
        published_to=date(2026, 2, 28),
        work_count=6,
        topic_count=1,
        outlier_count=4,
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_a, size=2
    )
    for work in (jan_member, feb_member):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_a,
            is_outlier=False,
            bertopic_topic_id=0,
        )
    for work in (jan_other_a, jan_other_b, jan_other_c, feb_other):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=None,
            is_outlier=True,
            bertopic_topic_id=-1,
        )

    observe_emergence(run_id, corpus, processor_engine)

    row = _observation_rows(processor_engine, run_id)[0]
    assert row["monthly_series"] == [
        {
            "year_month": "2026-01",
            "topic_count": 1,
            "corpus_count": 4,
            "share": 0.25,
        },
        {
            "year_month": "2026-02",
            "topic_count": 1,
            "corpus_count": 2,
            "share": 0.5,
        },
    ]
    assert row["monthly_share_change"] == 1.0
    assert row["coverage_status"] == "insufficient_historical_coverage"
    assert row["emergence_quadrant"] is None
    assert row["mean_share"] is None
    assert row["annual_growth"] is None


def test_three_complete_months_uses_geometric_mean(
    processor_engine: Engine,
) -> None:
    """Three complete months with consecutive ratios 2 and 2 store change 1.0.

    Shares 0.25, 0.5, 1.0 show the geometric mean of both steps, not only the
    last ratio. Coverage stays insufficient; the quadrant stays unset.
    """
    run_id = uuid.uuid4()
    topic_a = uuid.uuid4()
    jan_member = _work(1, published=date(2026, 1, 10))
    jan_other_a = _work(2, published=date(2026, 1, 12))
    jan_other_b = _work(3, published=date(2026, 1, 14))
    jan_other_c = _work(4, published=date(2026, 1, 16))
    feb_member = _work(5, published=date(2026, 2, 10))
    feb_other = _work(6, published=date(2026, 2, 20))
    mar_member = _work(7, published=date(2026, 3, 10))
    corpus = _corpus(
        [
            jan_member,
            jan_other_a,
            jan_other_b,
            jan_other_c,
            feb_member,
            feb_other,
            mar_member,
        ],
        published_from=date(2026, 1, 1),
        published_to=date(2026, 3, 31),
    )

    _insert_topic_run(
        processor_engine,
        run_id=run_id,
        published_from=date(2026, 1, 1),
        published_to=date(2026, 3, 31),
        work_count=7,
        topic_count=1,
        outlier_count=4,
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_a, size=3
    )
    for work in (jan_member, feb_member, mar_member):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_a,
            is_outlier=False,
            bertopic_topic_id=0,
        )
    for work in (jan_other_a, jan_other_b, jan_other_c, feb_other):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=None,
            is_outlier=True,
            bertopic_topic_id=-1,
        )

    observe_emergence(run_id, corpus, processor_engine)

    row = _observation_rows(processor_engine, run_id)[0]
    assert row["monthly_series"] == [
        {
            "year_month": "2026-01",
            "topic_count": 1,
            "corpus_count": 4,
            "share": 0.25,
        },
        {
            "year_month": "2026-02",
            "topic_count": 1,
            "corpus_count": 2,
            "share": 0.5,
        },
        {
            "year_month": "2026-03",
            "topic_count": 1,
            "corpus_count": 1,
            "share": 1.0,
        },
    ]
    assert row["monthly_share_change"] == 1.0
    assert row["coverage_status"] == "insufficient_historical_coverage"
    assert row["emergence_quadrant"] is None


def test_zero_share_month_skips_adjacent_pairs(
    processor_engine: Engine,
) -> None:
    """Calendar-adjacent pairs that touch a zero share are skipped.

    January and March both have positive share, but February is zero, so
    January is not paired with March and monthly share change stays null.
    """
    run_id = uuid.uuid4()
    topic_a = uuid.uuid4()
    jan_member = _work(1, published=date(2026, 1, 10))
    feb_other = _work(2, published=date(2026, 2, 10))
    mar_member = _work(3, published=date(2026, 3, 10))
    corpus = _corpus(
        [jan_member, feb_other, mar_member],
        published_from=date(2026, 1, 1),
        published_to=date(2026, 3, 31),
    )

    _insert_topic_run(
        processor_engine,
        run_id=run_id,
        published_from=date(2026, 1, 1),
        published_to=date(2026, 3, 31),
        work_count=3,
        topic_count=1,
        outlier_count=1,
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_a, size=2
    )
    for work in (jan_member, mar_member):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_a,
            is_outlier=False,
            bertopic_topic_id=0,
        )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=feb_other,
        discovery_topic_id=None,
        is_outlier=True,
        bertopic_topic_id=-1,
    )

    observe_emergence(run_id, corpus, processor_engine)

    row = _observation_rows(processor_engine, run_id)[0]
    assert row["monthly_series"] == [
        {
            "year_month": "2026-01",
            "topic_count": 1,
            "corpus_count": 1,
            "share": 1.0,
        },
        {
            "year_month": "2026-02",
            "topic_count": 0,
            "corpus_count": 1,
            "share": 0.0,
        },
        {
            "year_month": "2026-03",
            "topic_count": 1,
            "corpus_count": 1,
            "share": 1.0,
        },
    ]
    assert row["monthly_share_change"] is None
    assert row["coverage_status"] == "insufficient_historical_coverage"
    assert row["emergence_quadrant"] is None


def test_incomplete_trailing_month_excluded_from_change(
    processor_engine: Engine,
) -> None:
    """An incomplete trailing month stays in the series and out of the change.

    January and February are complete with shares 0.25 then 0.5 (change 1.0).
    March ends mid-month with share 0.25; including it would make change 0.0.
    """
    run_id = uuid.uuid4()
    topic_a = uuid.uuid4()
    jan_member = _work(1, published=date(2026, 1, 10))
    jan_other_a = _work(2, published=date(2026, 1, 12))
    jan_other_b = _work(3, published=date(2026, 1, 14))
    jan_other_c = _work(4, published=date(2026, 1, 16))
    feb_member = _work(5, published=date(2026, 2, 10))
    feb_other = _work(6, published=date(2026, 2, 20))
    mar_member = _work(7, published=date(2026, 3, 5))
    mar_other_a = _work(8, published=date(2026, 3, 8))
    mar_other_b = _work(9, published=date(2026, 3, 10))
    mar_other_c = _work(10, published=date(2026, 3, 12))
    corpus = _corpus(
        [
            jan_member,
            jan_other_a,
            jan_other_b,
            jan_other_c,
            feb_member,
            feb_other,
            mar_member,
            mar_other_a,
            mar_other_b,
            mar_other_c,
        ],
        published_from=date(2026, 1, 1),
        published_to=date(2026, 3, 15),
    )

    _insert_topic_run(
        processor_engine,
        run_id=run_id,
        published_from=date(2026, 1, 1),
        published_to=date(2026, 3, 15),
        work_count=10,
        topic_count=1,
        outlier_count=7,
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_a, size=3
    )
    for work in (jan_member, feb_member, mar_member):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_a,
            is_outlier=False,
            bertopic_topic_id=0,
        )
    for work in (
        jan_other_a,
        jan_other_b,
        jan_other_c,
        feb_other,
        mar_other_a,
        mar_other_b,
        mar_other_c,
    ):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=None,
            is_outlier=True,
            bertopic_topic_id=-1,
        )

    observe_emergence(run_id, corpus, processor_engine)

    row = _observation_rows(processor_engine, run_id)[0]
    assert row["monthly_series"] == [
        {
            "year_month": "2026-01",
            "topic_count": 1,
            "corpus_count": 4,
            "share": 0.25,
        },
        {
            "year_month": "2026-02",
            "topic_count": 1,
            "corpus_count": 2,
            "share": 0.5,
        },
        {
            "year_month": "2026-03",
            "topic_count": 1,
            "corpus_count": 4,
            "share": 0.25,
        },
    ]
    assert row["monthly_share_change"] == 1.0
    assert row["coverage_status"] == "insufficient_historical_coverage"
    assert row["emergence_quadrant"] is None


def test_empty_month_stores_share_null(processor_engine: Engine) -> None:
    """A month inside the run interval with corpus count zero stores share null."""
    run_id = uuid.uuid4()
    topic_a = uuid.uuid4()
    member = _work(1, published=date(2026, 1, 10))
    corpus = _corpus(
        [member],
        published_from=date(2026, 1, 1),
        published_to=date(2026, 2, 28),
    )

    _insert_topic_run(
        processor_engine,
        run_id=run_id,
        published_from=date(2026, 1, 1),
        published_to=date(2026, 2, 28),
        work_count=1,
        topic_count=1,
        outlier_count=0,
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_a, size=1
    )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=member,
        discovery_topic_id=topic_a,
        is_outlier=False,
        bertopic_topic_id=0,
    )

    observe_emergence(run_id, corpus, processor_engine)

    row = _observation_rows(processor_engine, run_id)[0]
    assert row["monthly_series"] == [
        {
            "year_month": "2026-01",
            "topic_count": 1,
            "corpus_count": 1,
            "share": 1.0,
        },
        {
            "year_month": "2026-02",
            "topic_count": 0,
            "corpus_count": 0,
            "share": None,
        },
    ]
    assert row["monthly_share_change"] is None
    assert row["emergence_quadrant"] is None
    assert row["coverage_status"] == "insufficient_historical_coverage"


def test_missing_run_is_refused(processor_engine: Engine) -> None:
    """A topic run id with no row raises and writes no observation rows."""
    run_id = uuid.uuid4()
    corpus = _corpus([])

    with pytest.raises(EmergenceError, match="not found"):
        observe_emergence(run_id, corpus, processor_engine)

    assert _observation_rows(processor_engine, run_id) == []


def test_assignment_missing_from_corpus_fails(processor_engine: Engine) -> None:
    """An assignment whose work is absent from the corpus fails with no rows."""
    run_id = uuid.uuid4()
    topic_a = uuid.uuid4()
    assigned = _work(1, published=date(2026, 1, 10))
    corpus = _corpus([])  # assignment work not in corpus

    _insert_topic_run(
        processor_engine, run_id=run_id, work_count=1, topic_count=1, outlier_count=0
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_a, size=1
    )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=assigned,
        discovery_topic_id=topic_a,
        is_outlier=False,
        bertopic_topic_id=0,
    )

    with pytest.raises(EmergenceError, match="missing from corpus"):
        observe_emergence(run_id, corpus, processor_engine)

    assert _observation_rows(processor_engine, run_id) == []


def test_zero_discovery_topics_persists_zero_observations(
    processor_engine: Engine,
) -> None:
    """A succeeded run with zero discovery topics persists zero rows and succeeds."""
    run_id = uuid.uuid4()
    outlier = _work(1, published=date(2026, 1, 10))
    corpus = _corpus([outlier])

    _insert_topic_run(
        processor_engine, run_id=run_id, work_count=1, topic_count=0, outlier_count=1
    )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=outlier,
        discovery_topic_id=None,
        is_outlier=True,
        bertopic_topic_id=-1,
    )

    result = observe_emergence(run_id, corpus, processor_engine)

    assert result.observation_count == 0
    assert _observation_rows(processor_engine, run_id) == []


def test_second_pass_keeps_existing_rows(processor_engine: Engine) -> None:
    """A second pass keeps existing observation rows without rewrite or duplicates."""
    run_id = uuid.uuid4()
    topic_a = uuid.uuid4()
    member = _work(1, published=date(2026, 1, 10))
    outlier = _work(2, published=date(2026, 1, 20))
    corpus = _corpus([member, outlier])

    _insert_topic_run(
        processor_engine, run_id=run_id, work_count=2, topic_count=1, outlier_count=1
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_a, size=1
    )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=member,
        discovery_topic_id=topic_a,
        is_outlier=False,
        bertopic_topic_id=0,
    )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=outlier,
        discovery_topic_id=None,
        is_outlier=True,
        bertopic_topic_id=-1,
    )

    first = observe_emergence(run_id, corpus, processor_engine)
    rows_after_first = _observation_rows(processor_engine, run_id)
    assert first.observation_count == 1
    assert len(rows_after_first) == 1
    created_at = rows_after_first[0]["created_at"]

    second = observe_emergence(run_id, corpus, processor_engine)
    rows_after_second = _observation_rows(processor_engine, run_id)

    assert second.observation_count == 1
    assert len(rows_after_second) == 1
    assert rows_after_second[0]["created_at"] == created_at
    assert rows_after_second[0]["discovery_topic_id"] == topic_a
    assert (
        rows_after_second[0]["monthly_series"] == rows_after_first[0]["monthly_series"]
    )


def test_two_complete_years_partial_later_year_wisdom_weak(
    processor_engine: Engine,
) -> None:
    """Two complete years plus a partial later year score annual figures.

    2024 and 2025 are complete; January 2026 is partial and stays in the monthly
    series only. Topic A shares 0.25 then 0.5 (mean 0.375, growth 1.0). Topic B
    shares 0.5 then 0.5 (mean 0.5, growth 0.0). Median of means is 0.4375, so A
    is below-median growing wisdom_weak and coverage is scored.
    """
    run_id = uuid.uuid4()
    topic_a = uuid.uuid4()
    topic_b = uuid.uuid4()
    a_2024 = _work(1, published=date(2024, 6, 1))
    b_2024_a = _work(2, published=date(2024, 6, 2))
    b_2024_b = _work(3, published=date(2024, 6, 3))
    out_2024 = _work(4, published=date(2024, 6, 4))
    a_2025_a = _work(5, published=date(2025, 6, 1))
    a_2025_b = _work(6, published=date(2025, 6, 2))
    b_2025_a = _work(7, published=date(2025, 6, 3))
    b_2025_b = _work(8, published=date(2025, 6, 4))
    out_2026 = _work(9, published=date(2026, 1, 10))
    works = [
        a_2024,
        b_2024_a,
        b_2024_b,
        out_2024,
        a_2025_a,
        a_2025_b,
        b_2025_a,
        b_2025_b,
        out_2026,
    ]
    corpus = _corpus(
        works,
        published_from=date(2024, 1, 1),
        published_to=date(2026, 1, 15),
    )

    _insert_topic_run(
        processor_engine,
        run_id=run_id,
        published_from=date(2024, 1, 1),
        published_to=date(2026, 1, 15),
        work_count=9,
        topic_count=2,
        outlier_count=2,
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_a, size=3
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_b, size=4
    )
    for work in (a_2024, a_2025_a, a_2025_b):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_a,
            is_outlier=False,
            bertopic_topic_id=0,
        )
    for work in (b_2024_a, b_2024_b, b_2025_a, b_2025_b):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_b,
            is_outlier=False,
            bertopic_topic_id=1,
        )
    for work in (out_2024, out_2026):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=None,
            is_outlier=True,
            bertopic_topic_id=-1,
        )

    observe_emergence(run_id, corpus, processor_engine)

    rows = _observation_rows(processor_engine, run_id)
    by_topic = {row["discovery_topic_id"]: row for row in rows}
    row_a = by_topic[topic_a]
    year_months = [point["year_month"] for point in row_a["monthly_series"]]
    assert "2026-01" in year_months
    assert row_a["complete_years"] == [2024, 2025]
    assert row_a["mean_share"] == 0.375
    assert row_a["annual_growth"] == 1.0
    assert row_a["valid_year_pair_count"] == 1
    assert row_a["coverage_status"] == "scored"
    assert row_a["emergence_quadrant"] == "wisdom_weak"
    assert by_topic[topic_b]["complete_years"] == [2024, 2025]
    assert by_topic[topic_b]["coverage_status"] == "scored"


def test_mean_share_equal_median_is_high_share(
    processor_engine: Engine,
) -> None:
    """A topic whose mean_share equals the median counts as high share.

    Three topics with means 0.1875, 0.375, and 0.375 make the median 0.375.
    The middle topic grows from 0.25 to 0.5, so equality is high and positive
    growth yields wisdom_strong, not wisdom_weak or latent.
    """
    run_id = uuid.uuid4()
    topic_low = uuid.uuid4()
    topic_mid = uuid.uuid4()
    topic_high = uuid.uuid4()
    # 2024 N=8: low 1, mid 2, high 4, outlier 1
    low_2024 = _work(1, published=date(2024, 3, 1))
    mid_2024_a = _work(2, published=date(2024, 3, 2))
    mid_2024_b = _work(3, published=date(2024, 3, 3))
    high_2024_a = _work(4, published=date(2024, 3, 4))
    high_2024_b = _work(5, published=date(2024, 3, 5))
    high_2024_c = _work(6, published=date(2024, 3, 6))
    high_2024_d = _work(7, published=date(2024, 3, 7))
    out_2024 = _work(8, published=date(2024, 3, 8))
    # 2025 N=8: low 2, mid 4, high 2
    low_2025_a = _work(9, published=date(2025, 3, 1))
    low_2025_b = _work(10, published=date(2025, 3, 2))
    mid_2025_a = _work(11, published=date(2025, 3, 3))
    mid_2025_b = _work(12, published=date(2025, 3, 4))
    mid_2025_c = _work(13, published=date(2025, 3, 5))
    mid_2025_d = _work(14, published=date(2025, 3, 6))
    high_2025_a = _work(15, published=date(2025, 3, 7))
    high_2025_b = _work(16, published=date(2025, 3, 8))
    works = [
        low_2024,
        mid_2024_a,
        mid_2024_b,
        high_2024_a,
        high_2024_b,
        high_2024_c,
        high_2024_d,
        out_2024,
        low_2025_a,
        low_2025_b,
        mid_2025_a,
        mid_2025_b,
        mid_2025_c,
        mid_2025_d,
        high_2025_a,
        high_2025_b,
    ]
    corpus = _corpus(
        works,
        published_from=date(2024, 1, 1),
        published_to=date(2025, 12, 31),
    )

    _insert_topic_run(
        processor_engine,
        run_id=run_id,
        published_from=date(2024, 1, 1),
        published_to=date(2025, 12, 31),
        work_count=16,
        topic_count=3,
        outlier_count=1,
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_low, size=3
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_mid, size=6
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_high, size=6
    )
    for work in (low_2024, low_2025_a, low_2025_b):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_low,
            is_outlier=False,
            bertopic_topic_id=0,
        )
    for work in (
        mid_2024_a,
        mid_2024_b,
        mid_2025_a,
        mid_2025_b,
        mid_2025_c,
        mid_2025_d,
    ):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_mid,
            is_outlier=False,
            bertopic_topic_id=1,
        )
    for work in (
        high_2024_a,
        high_2024_b,
        high_2024_c,
        high_2024_d,
        high_2025_a,
        high_2025_b,
    ):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_high,
            is_outlier=False,
            bertopic_topic_id=2,
        )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=out_2024,
        discovery_topic_id=None,
        is_outlier=True,
        bertopic_topic_id=-1,
    )

    observe_emergence(run_id, corpus, processor_engine)

    by_topic = {
        row["discovery_topic_id"]: row
        for row in _observation_rows(processor_engine, run_id)
    }
    mid = by_topic[topic_mid]
    assert mid["mean_share"] == 0.375
    assert mid["annual_growth"] == 1.0
    assert mid["coverage_status"] == "scored"
    assert mid["emergence_quadrant"] == "wisdom_strong"
    assert mid["emergence_quadrant"] not in {"wisdom_weak", "latent"}


def test_quadrant_names_wisdom_strong_latent_nswk(
    processor_engine: Engine,
) -> None:
    """wisdom_strong, latent, and nswk each appear for a scored multi-year run.

    Means 0.25, 0.375, and 0.375 make the median 0.375. Flat low share is
    latent; declining high share is nswk; growing high share is wisdom_strong.
    """
    run_id = uuid.uuid4()
    topic_latent = uuid.uuid4()
    topic_nswk = uuid.uuid4()
    topic_strong = uuid.uuid4()
    # 2024 N=4: latent 1 (0.25), nswk 2 (0.5), strong 1 (0.25)
    latent_2024 = _work(1, published=date(2024, 5, 1))
    nswk_2024_a = _work(2, published=date(2024, 5, 2))
    nswk_2024_b = _work(3, published=date(2024, 5, 3))
    strong_2024 = _work(4, published=date(2024, 5, 4))
    # 2025 N=4: latent 1 (0.25), nswk 1 (0.25), strong 2 (0.5)
    latent_2025 = _work(5, published=date(2025, 5, 1))
    nswk_2025 = _work(6, published=date(2025, 5, 2))
    strong_2025_a = _work(7, published=date(2025, 5, 3))
    strong_2025_b = _work(8, published=date(2025, 5, 4))
    works = [
        latent_2024,
        nswk_2024_a,
        nswk_2024_b,
        strong_2024,
        latent_2025,
        nswk_2025,
        strong_2025_a,
        strong_2025_b,
    ]
    corpus = _corpus(
        works,
        published_from=date(2024, 1, 1),
        published_to=date(2025, 12, 31),
    )

    _insert_topic_run(
        processor_engine,
        run_id=run_id,
        published_from=date(2024, 1, 1),
        published_to=date(2025, 12, 31),
        work_count=8,
        topic_count=3,
        outlier_count=0,
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_latent, size=2
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_nswk, size=3
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_strong, size=3
    )
    for work in (latent_2024, latent_2025):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_latent,
            is_outlier=False,
            bertopic_topic_id=0,
        )
    for work in (nswk_2024_a, nswk_2024_b, nswk_2025):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_nswk,
            is_outlier=False,
            bertopic_topic_id=1,
        )
    for work in (strong_2024, strong_2025_a, strong_2025_b):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_strong,
            is_outlier=False,
            bertopic_topic_id=2,
        )

    observe_emergence(run_id, corpus, processor_engine)

    by_topic = {
        row["discovery_topic_id"]: row
        for row in _observation_rows(processor_engine, run_id)
    }
    assert by_topic[topic_latent]["mean_share"] == 0.25
    assert by_topic[topic_latent]["annual_growth"] == 0.0
    assert by_topic[topic_latent]["emergence_quadrant"] == "latent"
    assert by_topic[topic_nswk]["mean_share"] == 0.375
    assert by_topic[topic_nswk]["annual_growth"] == -0.5
    assert by_topic[topic_nswk]["emergence_quadrant"] == "nswk"
    assert by_topic[topic_strong]["mean_share"] == 0.375
    assert by_topic[topic_strong]["annual_growth"] == 1.0
    assert by_topic[topic_strong]["emergence_quadrant"] == "wisdom_strong"


def test_outlier_topic_excluded_from_median(
    processor_engine: Engine,
) -> None:
    """Topic -1 has no row and must not move the median cut.

    Discovery means 0.375 and 0.5 make the median 0.4375, so the growing 0.375
    topic is wisdom_weak. Folding a zero-share outlier bucket into the median
    would cut at 0.375 and flip that topic to high share.
    """
    run_id = uuid.uuid4()
    topic_a = uuid.uuid4()
    topic_b = uuid.uuid4()
    a_2024 = _work(1, published=date(2024, 7, 1))
    b_2024_a = _work(2, published=date(2024, 7, 2))
    b_2024_b = _work(3, published=date(2024, 7, 3))
    out_2024 = _work(4, published=date(2024, 7, 4))
    a_2025_a = _work(5, published=date(2025, 7, 1))
    a_2025_b = _work(6, published=date(2025, 7, 2))
    b_2025_a = _work(7, published=date(2025, 7, 3))
    b_2025_b = _work(8, published=date(2025, 7, 4))
    works = [
        a_2024,
        b_2024_a,
        b_2024_b,
        out_2024,
        a_2025_a,
        a_2025_b,
        b_2025_a,
        b_2025_b,
    ]
    corpus = _corpus(
        works,
        published_from=date(2024, 1, 1),
        published_to=date(2025, 12, 31),
    )

    _insert_topic_run(
        processor_engine,
        run_id=run_id,
        published_from=date(2024, 1, 1),
        published_to=date(2025, 12, 31),
        work_count=8,
        topic_count=2,
        outlier_count=1,
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_a, size=3
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_b, size=4
    )
    for work in (a_2024, a_2025_a, a_2025_b):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_a,
            is_outlier=False,
            bertopic_topic_id=0,
        )
    for work in (b_2024_a, b_2024_b, b_2025_a, b_2025_b):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=topic_b,
            is_outlier=False,
            bertopic_topic_id=1,
        )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=out_2024,
        discovery_topic_id=None,
        is_outlier=True,
        bertopic_topic_id=-1,
    )

    observe_emergence(run_id, corpus, processor_engine)

    rows = _observation_rows(processor_engine, run_id)
    assert len(rows) == 2
    assert {row["discovery_topic_id"] for row in rows} == {topic_a, topic_b}
    by_topic = {row["discovery_topic_id"]: row for row in rows}
    assert by_topic[topic_a]["mean_share"] == 0.375
    assert by_topic[topic_a]["emergence_quadrant"] == "wisdom_weak"
    assert by_topic[topic_b]["mean_share"] == 0.5
    assert by_topic[topic_b]["emergence_quadrant"] == "nswk"


def test_no_valid_year_pair_is_newly_observed(
    processor_engine: Engine,
) -> None:
    """No valid adjacent year pair stores growth -1 and newly_observed.

    Shares 0.5 then 0.0 leave no positive pair. The defined zero still enters
    mean_share (literal 0.25). Quadrant stays null.
    """
    run_id = uuid.uuid4()
    topic_a = uuid.uuid4()
    member_2024 = _work(1, published=date(2024, 8, 1))
    other_2024 = _work(2, published=date(2024, 8, 2))
    other_2025_a = _work(3, published=date(2025, 8, 1))
    other_2025_b = _work(4, published=date(2025, 8, 2))
    works = [member_2024, other_2024, other_2025_a, other_2025_b]
    corpus = _corpus(
        works,
        published_from=date(2024, 1, 1),
        published_to=date(2025, 12, 31),
    )

    _insert_topic_run(
        processor_engine,
        run_id=run_id,
        published_from=date(2024, 1, 1),
        published_to=date(2025, 12, 31),
        work_count=4,
        topic_count=1,
        outlier_count=3,
    )
    _insert_discovery_topic(
        processor_engine, topic_run_id=run_id, discovery_topic_id=topic_a, size=1
    )
    _insert_assignment(
        processor_engine,
        topic_run_id=run_id,
        work=member_2024,
        discovery_topic_id=topic_a,
        is_outlier=False,
        bertopic_topic_id=0,
    )
    for work in (other_2024, other_2025_a, other_2025_b):
        _insert_assignment(
            processor_engine,
            topic_run_id=run_id,
            work=work,
            discovery_topic_id=None,
            is_outlier=True,
            bertopic_topic_id=-1,
        )

    observe_emergence(run_id, corpus, processor_engine)

    row = _observation_rows(processor_engine, run_id)[0]
    assert row["complete_years"] == [2024, 2025]
    assert row["mean_share"] == 0.25
    assert row["annual_growth"] == -1
    assert row["valid_year_pair_count"] == 0
    assert row["coverage_status"] == "newly_observed"
    assert row["emergence_quadrant"] is None


def test_observation_table_and_columns_have_comments(processor_engine: Engine) -> None:
    """The emergence table and every column carry non-empty Postgres comments."""
    with processor_engine.connect() as connection:
        table_comment = connection.execute(
            text(
                """
                SELECT obj_description(c.oid)
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = 'pwf_emergence_observations'
                  AND n.nspname = 'public'
                """
            )
        ).scalar_one()
        assert table_comment
        assert table_comment.strip()

        column_comments = (
            connection.execute(
                text(
                    """
                    SELECT a.attname, col_description(a.attrelid, a.attnum)
                    FROM pg_attribute a
                    JOIN pg_class c ON c.oid = a.attrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = 'pwf_emergence_observations'
                      AND n.nspname = 'public'
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                    ORDER BY a.attnum
                    """
                )
            )
            .mappings()
            .all()
        )

    expected_columns = {
        "topic_run_id",
        "discovery_topic_id",
        "monthly_series",
        "monthly_share_change",
        "complete_years",
        "mean_share",
        "annual_growth",
        "valid_year_pair_count",
        "coverage_status",
        "emergence_quadrant",
        "metric_version",
        "created_at",
    }
    assert {row["attname"] for row in column_comments} == expected_columns
    for row in column_comments:
        assert row["col_description"], f"missing comment on {row['attname']}"
        assert row["col_description"].strip()
