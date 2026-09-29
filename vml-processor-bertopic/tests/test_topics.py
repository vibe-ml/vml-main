"""Topics stage behaviour, exercised through `fit_topics` against `pwf_test`."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import date
from types import MappingProxyType

import numpy as np
import pytest
from sqlalchemy import Engine, select

from src.corpus import Corpus, CorpusConfig, CorpusWork, CoverageReport
from src.models.topics import (
    DiscoveryTopic,
    TopicRun,
    TopicRunMapping,
    WorkTopicAssignment,
)
from src.topics import (
    TopicConfig,
    TopicFitError,
    TopicRunResult,
    composition_hash,
    fit_topics,
)
from src.topics.fitter import FitResult

PHYSICAL_SCIENCES = "https://openalex.org/domains/3"
HASH_SEP = "\n"


def _work(
    n: int,
    *,
    title: str,
    abstract: str,
) -> CorpusWork:
    work_id = f"https://openalex.org/W{n:04d}"
    return CorpusWork(
        work_id=work_id,
        work_version_id=f"ver-{n:04d}",
        title=title,
        abstract=abstract,
        publication_date=date(2024, 6, 1),
        primary_domain_id=PHYSICAL_SCIENCES,
        language="en",
    )


def _corpus(works: Sequence[CorpusWork]) -> Corpus:
    ordered = tuple(sorted(works, key=lambda work: work.work_id))
    return Corpus(
        config=CorpusConfig(
            scope_ids=("scope-a",),
            published_from=date(2024, 1, 1),
            published_to=date(2024, 12, 31),
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


def _planted_corpus_and_embeddings(
    *, include_outliers: bool = True
) -> tuple[Corpus, np.ndarray]:
    """Two tight orthogonal groups; vectors planted, not encoded."""
    quantum = [
        _work(
            i,
            title=f"quantum lattice superconductivity {i}",
            abstract="quantum lattice superconductivity copper oxide phase doping gap",
        )
        for i in range(1, 9)
    ]
    coral = [
        _work(
            i,
            title=f"coral reef bleaching {i}",
            abstract="coral reef bleaching ocean temperature stress algae recovery",
        )
        for i in range(9, 17)
    ]
    outliers = [
        _work(
            17,
            title="unrelated patent law survey",
            abstract="patent litigation doctrine statutory interpretation jurisdiction",
        ),
        _work(
            18,
            title="medieval manuscript paleography",
            abstract="codicology parchment ink pigment scriptorium paleography",
        ),
    ]
    works = [*quantum, *coral, *(outliers if include_outliers else ())]
    corpus = _corpus(works)
    quantum_ids = {work.work_id for work in quantum}
    coral_ids = {work.work_id for work in coral}
    generator = np.random.default_rng(42)
    centers = {
        "quantum": np.array([1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        "coral": np.array([0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        "outlier_a": np.array([-1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        "outlier_b": np.array([0.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    }
    outlier_ids = [work.work_id for work in outliers]
    vectors: list[np.ndarray] = []
    for work in corpus.works:
        if work.work_id in quantum_ids:
            center = centers["quantum"]
        elif work.work_id in coral_ids:
            center = centers["coral"]
        elif work.work_id == outlier_ids[0]:
            center = centers["outlier_a"]
        else:
            center = centers["outlier_b"]
        vectors.append(generator.normal(loc=center, scale=0.005, size=8))
    return corpus, np.vstack(vectors)


def _topic_config(**overrides: object) -> TopicConfig:
    values: dict[str, object] = {
        "min_cluster_size": 5,
        "min_samples": 1,
        "umap_n_neighbors": 3,
        "umap_n_components": 2,
        "embedding_model": "test-encoder",
        "embedding_model_revision": "test-rev",
    }
    values.update(overrides)
    return TopicConfig(**values)  # type: ignore[arg-type]


def _expected_hash(member_ids: Sequence[str]) -> str:
    """Independent digest of the documented composition-hash encoding."""
    payload = HASH_SEP.join(sorted(member_ids)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _membership(result: TopicRunResult) -> set[frozenset[str]]:
    return {
        frozenset(
            assignment.work_id
            for assignment in result.assignments
            if assignment.discovery_topic_id == topic.discovery_topic_id
        )
        for topic in result.topics
    }


@pytest.fixture(scope="session")
def _warm_bertopic() -> None:
    """Compile Numba/UMAP once so slow fits are not cold-start outliers."""
    corpus, embeddings = _planted_corpus_and_embeddings(include_outliers=False)
    documents = [
        f"{work.title}\n{work.abstract}" if work.title else work.abstract
        for work in corpus.works
    ]
    from src.topics.bertopic_fitter import BerTopicFitter

    BerTopicFitter(_topic_config()).fit_transform(documents, embeddings)


@pytest.mark.slow
def test_fit_topics_recovers_planted_clusters(
    processor_engine: Engine, _warm_bertopic: None
) -> None:
    """Planted groups become distinct discovery topics under a real BERTopic fit."""
    corpus, embeddings = _planted_corpus_and_embeddings(include_outliers=False)
    result = fit_topics(corpus, embeddings, _topic_config(), processor_engine)

    assert result.status == "succeeded"
    assert result.work_count == len(corpus.works)
    assert result.topic_count == 2
    assert result.outlier_count == 0

    quantum_ids = frozenset(f"https://openalex.org/W{i:04d}" for i in range(1, 9))
    coral_ids = frozenset(f"https://openalex.org/W{i:04d}" for i in range(9, 17))
    assert _membership(result) == {quantum_ids, coral_ids}

    runs = TopicRun.__table__
    with processor_engine.connect() as connection:
        run_row = (
            connection.execute(select(runs).where(runs.c.run_id == result.run_id))
            .mappings()
            .one()
        )
        assert run_row["status"] == "succeeded"
        topics = connection.execute(
            select(DiscoveryTopic.__table__).where(
                DiscoveryTopic.__table__.c.topic_run_id == result.run_id
            )
        ).all()
        assert len(topics) == 2
        mappings = connection.execute(
            select(TopicRunMapping.__table__).where(
                TopicRunMapping.__table__.c.topic_run_id == result.run_id
            )
        ).all()
        assert len(mappings) == 2
        assignments = connection.execute(
            select(WorkTopicAssignment.__table__).where(
                WorkTopicAssignment.__table__.c.topic_run_id == result.run_id
            )
        ).all()
        assert len(assignments) == 16


def test_outliers_persist_with_null_topic_and_flag(processor_engine: Engine) -> None:
    """HDBSCAN outliers are stored with a null discovery topic and outlier flag."""
    corpus, embeddings = _planted_corpus_and_embeddings(include_outliers=True)
    documents = tuple(
        f"{work.title}\n{work.abstract}" if work.title else work.abstract
        for work in corpus.works
    )
    # Topic 0 = quantum (8), topic 1 = coral (8), -1 = two planted outliers.
    topic_ids = tuple([0] * 8 + [1] * 8 + [-1, -1])

    class PlantedFitter:
        def fit_transform(
            self, docs: Sequence[str], embeddings: np.ndarray
        ) -> FitResult:
            assert list(docs) == list(documents)
            return FitResult(
                topic_ids=topic_ids,
                keywords_by_topic={
                    0: (("quantum", 0.5), ("lattice", 0.4)),
                    1: (("coral", 0.5), ("reef", 0.4)),
                },
                representative_docs_by_topic={
                    0: (documents[0],),
                    1: (documents[8],),
                },
            )

    result = fit_topics(
        corpus,
        embeddings,
        _topic_config(),
        processor_engine,
        fitter=PlantedFitter(),
    )

    assert result.topic_count == 2
    assert result.outlier_count == 2
    assert result.outlier_rate == pytest.approx(2 / 18)
    quantum_ids = frozenset(f"https://openalex.org/W{i:04d}" for i in range(1, 9))
    coral_ids = frozenset(f"https://openalex.org/W{i:04d}" for i in range(9, 17))
    assert _membership(result) == {quantum_ids, coral_ids}

    outlier_assignments = [a for a in result.assignments if a.is_outlier]
    assert len(outlier_assignments) == 2
    assert {a.work_id for a in outlier_assignments} == {
        "https://openalex.org/W0017",
        "https://openalex.org/W0018",
    }
    assert all(a.discovery_topic_id is None for a in outlier_assignments)
    assert all(a.bertopic_topic_id == -1 for a in outlier_assignments)

    with processor_engine.connect() as connection:
        run_row = (
            connection.execute(
                select(TopicRun.__table__).where(
                    TopicRun.__table__.c.run_id == result.run_id
                )
            )
            .mappings()
            .one()
        )
        assert run_row["outlier_count"] == 2
        assert run_row["outlier_rate"] == pytest.approx(2 / 18)
        null_assignments = (
            connection.execute(
                select(WorkTopicAssignment.__table__).where(
                    WorkTopicAssignment.__table__.c.topic_run_id == result.run_id,
                    WorkTopicAssignment.__table__.c.is_outlier.is_(True),
                )
            )
            .mappings()
            .all()
        )
        assert len(null_assignments) == 2
        assert all(row["discovery_topic_id"] is None for row in null_assignments)


@pytest.mark.slow
def test_fit_topics_persists_keywords_representatives_and_hash(
    processor_engine: Engine, _warm_bertopic: None
) -> None:
    """Each topic stores keywords, representative works, and a versioned hash."""
    corpus, embeddings = _planted_corpus_and_embeddings(include_outliers=False)
    result = fit_topics(corpus, embeddings, _topic_config(), processor_engine)

    assert result.composition_hash_algorithm == "sha256"
    assert result.composition_hash_encoding_version == "sorted_work_ids_v1"
    for topic in result.topics:
        members = [
            a.work_id
            for a in result.assignments
            if a.discovery_topic_id == topic.discovery_topic_id
        ]
        assert topic.composition_hash == _expected_hash(members)
        assert topic.size == len(members)
        assert topic.keywords
        assert all(kw.weight is not None for kw in topic.keywords)
        assert topic.representative_work_ids
        assert set(topic.representative_work_ids) <= set(members)


def test_composition_hash_stable_under_reorder_and_changes_with_membership() -> None:
    """Composition hash follows the documented sorted-membership encoding."""
    members = ("https://openalex.org/W0002", "https://openalex.org/W0001")
    first = composition_hash(members)
    second = composition_hash(tuple(reversed(members)))
    assert first == second
    # Independent literal for sorted W0001\\nW0002 under SHA-256 / utf-8.
    assert first == "3162e231853cbd12da6d98b4344e4435e5b5fea06354b9adf3f5c0257518c85a"
    changed = composition_hash((*members, "https://openalex.org/W0003"))
    assert changed != first


@pytest.mark.slow
def test_identical_corpus_and_seed_reproduce_assignments(
    processor_engine: Engine, _warm_bertopic: None
) -> None:
    """Two runs share topic membership and outlier sets; UUIDs may differ."""
    corpus, embeddings = _planted_corpus_and_embeddings(include_outliers=False)
    config = _topic_config()
    first = fit_topics(corpus, embeddings, config, processor_engine)
    second = fit_topics(corpus, embeddings, config, processor_engine)

    assert _membership(first) == _membership(second)
    assert {a.work_id for a in first.assignments if a.is_outlier} == {
        a.work_id for a in second.assignments if a.is_outlier
    }
    assert first.run_id != second.run_id


@pytest.mark.slow
def test_second_run_does_not_modify_first_run_rows(
    processor_engine: Engine, _warm_bertopic: None
) -> None:
    """Runs are append-only: earlier rows stay untouched after a later fit."""
    corpus, embeddings = _planted_corpus_and_embeddings(include_outliers=False)
    config = _topic_config()
    first = fit_topics(corpus, embeddings, config, processor_engine)

    table = DiscoveryTopic.__table__
    assignments = WorkTopicAssignment.__table__
    with processor_engine.connect() as connection:
        first_topics = connection.execute(
            select(table.c.discovery_topic_id, table.c.composition_hash).where(
                table.c.topic_run_id == first.run_id
            )
        ).all()
        first_assignment_count = connection.execute(
            select(assignments.c.work_id).where(
                assignments.c.topic_run_id == first.run_id
            )
        ).all()

    second = fit_topics(corpus, embeddings, config, processor_engine)
    assert second.run_id != first.run_id

    with processor_engine.connect() as connection:
        after_topics = connection.execute(
            select(table.c.discovery_topic_id, table.c.composition_hash).where(
                table.c.topic_run_id == first.run_id
            )
        ).all()
        after_assignment_count = connection.execute(
            select(assignments.c.work_id).where(
                assignments.c.topic_run_id == first.run_id
            )
        ).all()
        run_count = connection.execute(select(TopicRun.__table__.c.run_id)).all()

    assert after_topics == first_topics
    assert after_assignment_count == first_assignment_count
    assert len(run_count) >= 2


def test_raising_fit_marks_run_failed_without_partial_topics(
    processor_engine: Engine,
) -> None:
    """A failing fitter leaves a failed run and no topic or assignment rows."""
    corpus, embeddings = _planted_corpus_and_embeddings(include_outliers=False)

    class BoomFitter:
        def fit_transform(
            self, documents: Sequence[str], embeddings: np.ndarray
        ) -> FitResult:
            raise TopicFitError("synthetic fit failure")

    with pytest.raises(TopicFitError, match="synthetic fit failure"):
        fit_topics(
            corpus,
            embeddings,
            _topic_config(),
            processor_engine,
            fitter=BoomFitter(),
        )

    runs = TopicRun.__table__
    topics = DiscoveryTopic.__table__
    mappings = TopicRunMapping.__table__
    assignments = WorkTopicAssignment.__table__
    with processor_engine.connect() as connection:
        run_rows = connection.execute(select(runs)).mappings().all()
        assert len(run_rows) == 1
        assert run_rows[0]["status"] == "failed"
        run_id = run_rows[0]["run_id"]
        assert (
            connection.execute(
                select(topics).where(topics.c.topic_run_id == run_id)
            ).first()
            is None
        )
        assert (
            connection.execute(
                select(mappings).where(mappings.c.topic_run_id == run_id)
            ).first()
            is None
        )
        assert (
            connection.execute(
                select(assignments).where(assignments.c.topic_run_id == run_id)
            ).first()
            is None
        )


@pytest.mark.slow
def test_run_record_stores_filters_model_and_clustering(
    processor_engine: Engine, _warm_bertopic: None
) -> None:
    """The run row records corpus filters, embedding identity, and clustering knobs."""
    corpus, embeddings = _planted_corpus_and_embeddings(include_outliers=False)
    config = _topic_config(min_cluster_size=5, min_samples=1)
    result = fit_topics(corpus, embeddings, config, processor_engine)

    runs = TopicRun.__table__
    with processor_engine.connect() as connection:
        row = (
            connection.execute(select(runs).where(runs.c.run_id == result.run_id))
            .mappings()
            .one()
        )

    assert list(row["scope_ids"]) == ["scope-a"]
    assert row["published_from"] == date(2024, 1, 1)
    assert row["published_to"] == date(2024, 12, 31)
    assert row["embedding_model"] == "test-encoder"
    assert row["embedding_model_revision"] == "test-rev"
    assert row["min_cluster_size"] == 5
    assert row["min_samples"] == 1
    assert row["umap_random_state"] == 42
    assert row["umap_metric"] == "cosine"
    assert row["composition_hash_algorithm"] == "sha256"
    assert row["composition_hash_encoding_version"] == "sorted_work_ids_v1"
    assert row["work_count"] == 16
    assert row["topic_count"] == result.topic_count
    assert row["outlier_count"] == result.outlier_count
    assert row["status"] == "succeeded"
    assert row["elapsed_seconds"] is not None
    assert row["elapsed_seconds"] >= 0


def test_centroid_nearest_selects_min_fifteen_without_bertopic() -> None:
    """Centroid-nearest picks up to 15 members; outliers are not in the pool."""
    from src.topics.representatives import (
        centroid_nearest_indices,
        centroid_nearest_work_ids,
    )

    # Shared direction plus small noise: first 15 stay nearest the mean.
    rng = np.random.default_rng(0)
    base = np.ones(8, dtype=np.float64)
    embeddings = np.stack([base + 0.01 * rng.normal(size=8) for _ in range(20)], axis=0)
    # Push the last five far away.
    embeddings[15:] = -base

    indices = centroid_nearest_indices(embeddings, cap=15)
    assert len(indices) == 15
    assert set(indices).isdisjoint(range(15, 20))

    work_ids = tuple(f"w{i}" for i in range(20))
    vectors = {work_ids[i]: embeddings[i] for i in range(20)}
    # Omit two far works to simulate missing vectors; still fill to 15 from near ones.
    del vectors["w18"]
    del vectors["w19"]
    selected = centroid_nearest_work_ids(work_ids, vectors, cap=15)
    assert len(selected) == 15
    assert "w18" not in selected
    assert "w19" not in selected
    assert set(selected) <= set(work_ids[:18])


def test_planted_fitter_can_store_fifteen_centroid_docs(
    processor_engine: Engine,
) -> None:
    """Fit persistence stores the full representative list returned by the fitter."""
    corpus, embeddings = _planted_corpus_and_embeddings(include_outliers=False)
    documents = tuple(
        f"{work.title}\n{work.abstract}" if work.title else work.abstract
        for work in corpus.works
    )
    topic_ids = tuple([0] * 8 + [1] * 8)
    quantum_docs = documents[:8]
    coral_docs = documents[8:]

    class PlantedFitter:
        def fit_transform(
            self, docs: Sequence[str], embeddings: np.ndarray
        ) -> FitResult:
            return FitResult(
                topic_ids=topic_ids,
                keywords_by_topic={
                    0: (("quantum", 0.5),),
                    1: (("coral", 0.5),),
                },
                representative_docs_by_topic={
                    0: quantum_docs,
                    1: coral_docs,
                },
            )

    result = fit_topics(
        corpus,
        embeddings,
        _topic_config(),
        processor_engine,
        fitter=PlantedFitter(),
    )
    by_bertopic = {topic.bertopic_topic_id: topic for topic in result.topics}
    assert len(by_bertopic[0].representative_work_ids) == 8
    assert len(by_bertopic[1].representative_work_ids) == 8
    assert set(by_bertopic[0].representative_work_ids) == {
        work.work_id for work in corpus.works[:8]
    }
