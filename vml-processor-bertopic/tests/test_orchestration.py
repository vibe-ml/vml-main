"""Dagster job wiring: materialize discovery assets in-process with fakes."""

from __future__ import annotations

import uuid
from collections.abc import Iterator, Sequence
from datetime import date

import dagster as dg
import numpy as np
import pytest
from qdrant_client import QdrantClient
from sqlalchemy import Engine, func, select
from structlog.testing import capture_logs

from src.common.db import create_processor_engine
from src.common.settings import Settings
from src.corpus.models import CorpusConfig
from src.models.emergence import EmergenceObservation
from src.models.labels import TopicLabel, WorkSummary
from src.models.topics import TopicRun, WorkTopicAssignment
from src.orchestration.assets import (
    corpus_embeddings,
    discovery_assets,
    discovery_topics_job,
    selected_corpus,
    topic_emergence,
    topic_labels,
    topic_run,
)
from src.orchestration.ports import PipelinePorts
from src.topics.fitter import FitResult
from src.topics.models import TopicFitError
from tests.conftest import TestSettings, UpstreamFixture
from tests.test_corpus_selection import openalex_work
from tests.test_embeddings import HashEmbeddingClient, RejectEmbeddingClient
from tests.test_labels import FakeLabelingClient, WordCountTokenizer

SCOPE = "scope-orch"
PHYSICAL = "https://openalex.org/domains/3"
VECTOR_SIZE = 8

CORPUS_METADATA = ("works_selected",)
EMBEDDING_METADATA = (
    "embeddings_processed",
    "embeddings_succeeded",
    "embeddings_failed",
    "embeddings_saved",
    "embeddings_collection_total",
)
LABEL_METADATA = (
    "summaries_processed",
    "summaries_succeeded",
    "summaries_failed",
    "summaries_inserted",
    "summaries_total",
    "headlines_processed",
    "headlines_succeeded",
    "headlines_failed",
    "headlines_inserted",
    "headlines_total",
)
LABEL_THIS_RUN = (
    "summaries_processed",
    "summaries_succeeded",
    "summaries_failed",
    "summaries_inserted",
    "headlines_processed",
    "headlines_succeeded",
    "headlines_failed",
    "headlines_inserted",
)
EMERGENCE_METADATA = (
    "observation_count",
    "headlines_processed",
    "headlines_cached",
    "headlines_saved",
    "headlines_failed",
)
EMERGENCE_BOUNDARY_FIELDS = (
    "observation_count",
    "coverage_status",
    "headlines_processed",
    "headlines_cached",
    "headlines_saved",
    "headlines_failed",
    "elapsed_seconds",
)


class PlantedFitter:
    """Assigns the first half of works to topic 0 and the rest to topic 1."""

    def fit_transform(
        self, documents: Sequence[str], embeddings: np.ndarray
    ) -> FitResult:
        midpoint = max(1, len(documents) // 2)
        topic_ids = tuple(
            0 if index < midpoint else 1 for index in range(len(documents))
        )
        return FitResult(
            topic_ids=topic_ids,
            keywords_by_topic={
                0: (("alpha", 0.5), ("beta", 0.4)),
                1: (("gamma", 0.5), ("delta", 0.4)),
            },
            representative_docs_by_topic={
                0: (documents[0],),
                1: (documents[midpoint],),
            },
        )


def _seed_works(upstream: UpstreamFixture, count: int = 4) -> None:
    for number in range(1, count + 1):
        upstream.add_work(
            SCOPE,
            openalex_work(
                number,
                title=f"Theme work {number}",
                publication_date="2026-02-01",
                primary_topic={
                    "id": "https://openalex.org/T10028",
                    "domain": {
                        "id": PHYSICAL,
                        "display_name": "Physical Sciences",
                    },
                },
                abstract_inverted_index={
                    f"token{number}": [0],
                    "research": [1],
                    "theme.": [2],
                },
            ),
        )


def _settings(
    test_settings: TestSettings,
    upstream: UpstreamFixture,
    collection: str,
    *,
    headline_collection: str,
) -> Settings:
    return Settings(
        _env_file=None,
        database_url=test_settings.database_url,
        upstream_schema=upstream.schema,
        corpus=CorpusConfig(
            scope_ids=(SCOPE,),
            published_from=date(2026, 1, 1),
            published_to=date(2026, 12, 31),
        ),
        qdrant_url=test_settings.qdrant_url,
        qdrant_collection=collection,
        headline_collection=headline_collection,
        embedding_model="microsoft/harrier-oss-v1-0.6b",
        embedding_model_revision="orch-test",
        embedding_vector_size=VECTOR_SIZE,
        embedder_api_urls=("http://fake-embed.example",),
        embedding_max_concurrency_per_endpoint=1,
        embedding_max_concurrency_overall=2,
        min_cluster_size=2,
        umap_n_neighbors=2,
        labeling_api_url="http://fake-label.example",
        labeling_api_key="",
        labeling_model="fake-label-model",
        labeling_model_revision="main",
        labeling_sample_size=2,
        labeling_context_window=4096,
        labeling_output_reserve=32,
        topic_run_max_concurrent=1,
    )


def _ports(
    *,
    settings: Settings,
    engine: Engine,
    qdrant: QdrantClient,
    embedding_client: object,
    labeling_client: object,
    fitter: object | None = None,
) -> PipelinePorts:
    return PipelinePorts(
        settings=settings,
        engine=engine,
        qdrant=qdrant,
        embedding_client=embedding_client,  # type: ignore[arg-type]
        labeling_client=labeling_client,  # type: ignore[arg-type]
        tokenizer=WordCountTokenizer(),
        fitter=fitter if fitter is not None else PlantedFitter(),  # type: ignore[arg-type]
    )


@pytest.fixture
def orch_env(
    upstream: UpstreamFixture,
    processor_engine: Engine,
    test_settings: TestSettings,
) -> Iterator[tuple[Settings, Engine, QdrantClient]]:
    """Seeded upstream works, combined engine, and namespaced Qdrant collections."""
    _seed_works(upstream, count=4)
    qdrant = QdrantClient(url=test_settings.qdrant_url)
    collection = f"orch_test_{uuid.uuid4().hex}"
    headline_collection = f"orch_headlines_{uuid.uuid4().hex}"
    settings = _settings(
        test_settings, upstream, collection, headline_collection=headline_collection
    )
    engine = create_processor_engine(
        test_settings.database_url, upstream_schema=upstream.schema
    )
    # Ensure processor tables are clean for this test (processor_engine already truncated).
    _ = processor_engine
    try:
        yield settings, engine, qdrant
    finally:
        for name in (collection, headline_collection):
            if qdrant.collection_exists(name):
                qdrant.delete_collection(name)
        qdrant.close()
        engine.dispose()


def _metadata_ints(
    result: dg.ExecuteInProcessResult, asset: str, keys: Sequence[str]
) -> dict[str, int]:
    events = [
        event
        for event in result.get_asset_materialization_events()
        if event.asset_key.to_user_string() == asset
    ]
    assert len(events) == 1
    metadata = events[0].materialization.metadata
    assert set(keys) <= set(metadata)
    return {key: int(metadata[key].value) for key in keys}


def _assert_metadata_matches_boundary(
    result: dg.ExecuteInProcessResult,
    logs: Sequence[object],
    *,
    asset: str,
    stage: str,
    keys: Sequence[str],
) -> dict[str, int]:
    values = _metadata_ints(result, asset, keys)
    boundaries = [
        event
        for event in logs
        if isinstance(event, dict)
        and event.get("event") == "stage_boundary"
        and event.get("stage") == stage
    ]
    assert len(boundaries) == 1
    for key in keys:
        assert boundaries[0][key] == values[key]
    return values


def _table_count(engine: Engine, table: object) -> int:
    with engine.connect() as connection:
        return int(
            connection.execute(select(func.count()).select_from(table)).scalar_one()
        )


def test_discovery_job_materializes_assets_in_order(
    orch_env: tuple[Settings, Engine, QdrantClient],
) -> None:
    settings, engine, qdrant = orch_env
    headline_collection = settings.headline_collection
    ports = _ports(
        settings=settings,
        engine=engine,
        qdrant=qdrant,
        embedding_client=HashEmbeddingClient(size=VECTOR_SIZE),
        labeling_client=FakeLabelingClient(),
    )

    with capture_logs() as logs:
        result = dg.materialize(discovery_assets, resources={"ports": ports})

    assert result.success
    keys = [
        event.asset_key.to_user_string()
        for event in result.get_asset_materialization_events()
    ]
    assert keys == [
        "selected_corpus",
        "corpus_embeddings",
        "topic_run",
        "topic_labels",
        "topic_emergence",
    ]

    boundaries = [event for event in logs if event.get("event") == "stage_boundary"]
    assert [event["stage"] for event in boundaries] == [
        "corpus",
        "embeddings",
        "topics",
        "labels",
        "emergence",
    ]
    assert all(event.get("run_id") for event in boundaries)
    assert all("elapsed_seconds" in event for event in boundaries)

    corpus_meta = _assert_metadata_matches_boundary(
        result, logs, asset="selected_corpus", stage="corpus", keys=CORPUS_METADATA
    )
    embedding_meta = _assert_metadata_matches_boundary(
        result,
        logs,
        asset="corpus_embeddings",
        stage="embeddings",
        keys=EMBEDDING_METADATA,
    )
    label_meta = _assert_metadata_matches_boundary(
        result, logs, asset="topic_labels", stage="labels", keys=LABEL_METADATA
    )
    emergence_meta = _assert_metadata_matches_boundary(
        result,
        logs,
        asset="topic_emergence",
        stage="emergence",
        keys=EMERGENCE_METADATA,
    )
    assert corpus_meta["works_selected"] == 4
    assert embedding_meta["embeddings_processed"] == 4
    assert embedding_meta["embeddings_succeeded"] == 4
    assert embedding_meta["embeddings_failed"] == 0
    assert embedding_meta["embeddings_saved"] == 4
    assert embedding_meta["embeddings_collection_total"] == 4
    assert label_meta["summaries_processed"] == 4
    assert label_meta["summaries_succeeded"] == 4
    assert label_meta["summaries_failed"] == 0
    assert label_meta["summaries_inserted"] == 4
    assert label_meta["summaries_total"] == _table_count(engine, WorkSummary.__table__)
    assert label_meta["headlines_processed"] == 2
    assert label_meta["headlines_succeeded"] == 2
    assert label_meta["headlines_failed"] == 0
    assert label_meta["headlines_inserted"] == 2
    assert label_meta["headlines_total"] == _table_count(engine, TopicLabel.__table__)

    emergence_boundary = boundaries[-1]
    for field in EMERGENCE_BOUNDARY_FIELDS:
        assert field in emergence_boundary
    assert emergence_boundary["coverage_status"] == "insufficient_historical_coverage"
    assert emergence_meta["observation_count"] == 2
    assert emergence_meta["headlines_processed"] == 2
    assert emergence_meta["headlines_cached"] == 0
    assert emergence_meta["headlines_saved"] == 2
    assert emergence_meta["headlines_failed"] == 0

    with engine.connect() as connection:
        runs = connection.execute(select(TopicRun.__table__)).mappings().all()
        assert len(runs) == 1
        assert runs[0]["status"] == "succeeded"
        assert (
            len(
                connection.execute(select(WorkTopicAssignment.__table__))
                .mappings()
                .all()
            )
            == 4
        )
        observations = (
            connection.execute(select(EmergenceObservation.__table__)).mappings().all()
        )
        assert len(observations) == 2
        created_ats = {
            (row["topic_run_id"], row["discovery_topic_id"]): row["created_at"]
            for row in observations
        }

    assert qdrant.collection_exists(headline_collection)
    headline_points = qdrant.scroll(
        collection_name=headline_collection, limit=10, with_payload=True
    )[0]
    # FakeLabelingClient uses one fixed headline, so both topics share one point id.
    assert len(headline_points) == 1
    deleted_point_id = headline_points[0].id

    ports_reject = _ports(
        settings=settings,
        engine=engine,
        qdrant=qdrant,
        embedding_client=RejectEmbeddingClient(),
        labeling_client=FakeLabelingClient(),
    )
    with capture_logs() as adopt_logs:
        adopt_result = dg.materialize(
            discovery_assets, resources={"ports": ports_reject}
        )
    assert adopt_result.success
    adopt_meta = _assert_metadata_matches_boundary(
        adopt_result,
        adopt_logs,
        asset="topic_emergence",
        stage="emergence",
        keys=EMERGENCE_METADATA,
    )
    assert adopt_meta["headlines_processed"] == 0
    assert adopt_meta["headlines_cached"] == 2
    assert adopt_meta["headlines_saved"] == 0
    assert adopt_meta["headlines_failed"] == 0
    with engine.connect() as connection:
        after_adopt = (
            connection.execute(select(EmergenceObservation.__table__)).mappings().all()
        )
        assert {
            (row["topic_run_id"], row["discovery_topic_id"]): row["created_at"]
            for row in after_adopt
        } == created_ats

    qdrant.delete(
        collection_name=headline_collection,
        points_selector=[deleted_point_id],
        wait=True,
    )
    assert qdrant.scroll(collection_name=headline_collection, limit=10)[0] == []

    ports_fill = _ports(
        settings=settings,
        engine=engine,
        qdrant=qdrant,
        embedding_client=HashEmbeddingClient(size=VECTOR_SIZE),
        labeling_client=FakeLabelingClient(),
    )
    with capture_logs() as fill_logs:
        fill_result = dg.materialize(discovery_assets, resources={"ports": ports_fill})
    assert fill_result.success
    fill_meta = _assert_metadata_matches_boundary(
        fill_result,
        fill_logs,
        asset="topic_emergence",
        stage="emergence",
        keys=EMERGENCE_METADATA,
    )
    assert fill_meta["observation_count"] == 2
    # Both topics share the deleted point id, so both planned rows miss and re-embed.
    assert fill_meta["headlines_processed"] == 2
    assert fill_meta["headlines_cached"] == 0
    assert fill_meta["headlines_saved"] == 2
    assert fill_meta["headlines_failed"] == 0
    restored = qdrant.retrieve(
        collection_name=headline_collection, ids=[deleted_point_id]
    )
    assert len(restored) == 1
    with engine.connect() as connection:
        after_fill = (
            connection.execute(select(EmergenceObservation.__table__)).mappings().all()
        )
        assert {
            (row["topic_run_id"], row["discovery_topic_id"]): row["created_at"]
            for row in after_fill
        } == created_ats


def test_topic_rerun_adopts_succeeded_run_without_reembedding(
    orch_env: tuple[Settings, Engine, QdrantClient],
) -> None:
    settings, engine, qdrant = orch_env
    ports = _ports(
        settings=settings,
        engine=engine,
        qdrant=qdrant,
        embedding_client=HashEmbeddingClient(size=VECTOR_SIZE),
        labeling_client=FakeLabelingClient(),
    )
    assert dg.materialize(discovery_assets, resources={"ports": ports}).success
    with engine.connect() as connection:
        first_run_id = connection.execute(
            select(TopicRun.__table__.c.run_id)
        ).scalar_one()

    ports_reject = _ports(
        settings=settings,
        engine=engine,
        qdrant=qdrant,
        embedding_client=RejectEmbeddingClient(),
        labeling_client=FakeLabelingClient(),
    )
    assert dg.materialize(discovery_assets, resources={"ports": ports_reject}).success

    with engine.connect() as connection:
        run_ids = [
            row[0]
            for row in connection.execute(select(TopicRun.__table__.c.run_id)).all()
        ]
        assert run_ids == [first_run_id]
        assert (
            len(
                connection.execute(
                    select(WorkTopicAssignment.__table__.c.work_id)
                ).all()
            )
            == 4
        )


def test_clustering_change_creates_new_run_without_reembedding(
    orch_env: tuple[Settings, Engine, QdrantClient],
) -> None:
    settings, engine, qdrant = orch_env
    ports = _ports(
        settings=settings,
        engine=engine,
        qdrant=qdrant,
        embedding_client=HashEmbeddingClient(size=VECTOR_SIZE),
        labeling_client=FakeLabelingClient(),
    )
    assert dg.materialize(discovery_assets, resources={"ports": ports}).success

    changed = settings.model_copy(update={"min_cluster_size": 3})
    ports_changed = _ports(
        settings=changed,
        engine=engine,
        qdrant=qdrant,
        embedding_client=RejectEmbeddingClient(),
        labeling_client=FakeLabelingClient(),
    )
    result = dg.materialize(
        [selected_corpus, topic_run, topic_labels],
        resources={"ports": ports_changed},
    )
    assert result.success
    with engine.connect() as connection:
        runs = connection.execute(select(TopicRun.__table__)).mappings().all()
        assert len(runs) == 2
        assert {row["min_cluster_size"] for row in runs} == {2, 3}


def test_failed_topic_fit_fails_job_and_marks_run(
    orch_env: tuple[Settings, Engine, QdrantClient],
) -> None:
    settings, engine, qdrant = orch_env

    class BoomFitter:
        def fit_transform(
            self, documents: Sequence[str], embeddings: np.ndarray
        ) -> FitResult:
            raise TopicFitError("boom")

    ports = _ports(
        settings=settings,
        engine=engine,
        qdrant=qdrant,
        embedding_client=HashEmbeddingClient(size=VECTOR_SIZE),
        labeling_client=FakeLabelingClient(),
        fitter=BoomFitter(),
    )
    assert dg.materialize(
        [selected_corpus, corpus_embeddings], resources={"ports": ports}
    ).success

    result = dg.materialize(
        [selected_corpus, topic_run], resources={"ports": ports}, raise_on_error=False
    )
    assert not result.success
    with engine.connect() as connection:
        rows = connection.execute(select(TopicRun.__table__)).mappings().all()
        assert len(rows) == 1
        assert rows[0]["status"] == "failed"


def test_job_and_asset_names() -> None:
    assert discovery_topics_job.name == "discovery_topics_job"
    assert discovery_topics_job.tags.get("dagster/concurrency_key") == "topic_fit"
    assert {asset.key.to_user_string() for asset in discovery_assets} == {
        "selected_corpus",
        "corpus_embeddings",
        "topic_run",
        "topic_labels",
        "topic_emergence",
    }
    assert topic_run.node_def.pool == "topic_fit"  # type: ignore[attr-defined]
    assert topic_emergence.group_names_by_key[topic_emergence.key] == "discovery"


def test_label_adopt_reports_zero_this_run_and_store_totals(
    orch_env: tuple[Settings, Engine, QdrantClient],
) -> None:
    settings, engine, qdrant = orch_env
    ports = _ports(
        settings=settings,
        engine=engine,
        qdrant=qdrant,
        embedding_client=HashEmbeddingClient(size=VECTOR_SIZE),
        labeling_client=FakeLabelingClient(),
    )
    assert dg.materialize(discovery_assets, resources={"ports": ports}).success
    summary_total = _table_count(engine, WorkSummary.__table__)
    headline_total = _table_count(engine, TopicLabel.__table__)
    assert summary_total > 0
    assert headline_total > 0

    with capture_logs() as logs:
        result = dg.materialize(
            [selected_corpus, topic_labels], resources={"ports": ports}
        )

    assert result.success
    label_meta = _assert_metadata_matches_boundary(
        result, logs, asset="topic_labels", stage="labels", keys=LABEL_METADATA
    )
    assert all(label_meta[key] == 0 for key in LABEL_THIS_RUN)
    assert label_meta["summaries_total"] == summary_total
    assert label_meta["headlines_total"] == headline_total


def test_embedding_client_error_skips_embeddings_materialization(
    orch_env: tuple[Settings, Engine, QdrantClient],
) -> None:
    settings, engine, qdrant = orch_env
    ports = _ports(
        settings=settings,
        engine=engine,
        qdrant=qdrant,
        embedding_client=RejectEmbeddingClient(),
        labeling_client=FakeLabelingClient(),
    )
    result = dg.materialize(
        [selected_corpus, corpus_embeddings],
        resources={"ports": ports},
        raise_on_error=False,
    )
    assert not result.success
    keys = [
        event.asset_key.to_user_string()
        for event in result.get_asset_materialization_events()
    ]
    assert keys == ["selected_corpus"]


def test_failed_topic_label_still_materializes(
    orch_env: tuple[Settings, Engine, QdrantClient],
) -> None:
    settings, engine, qdrant = orch_env
    ports = _ports(
        settings=settings,
        engine=engine,
        qdrant=qdrant,
        embedding_client=HashEmbeddingClient(size=VECTOR_SIZE),
        labeling_client=FakeLabelingClient(fail_when_prompt_contains="Theme work 3"),
    )
    with capture_logs() as logs:
        result = dg.materialize(discovery_assets, resources={"ports": ports})

    assert result.success
    label_meta = _assert_metadata_matches_boundary(
        result, logs, asset="topic_labels", stage="labels", keys=LABEL_METADATA
    )
    assert label_meta["headlines_failed"] >= 1
    assert label_meta["headlines_inserted"] >= 1


def test_failed_label_is_retried_not_adopted(
    orch_env: tuple[Settings, Engine, QdrantClient],
) -> None:
    """Dagster does not adopt a full-sample failed label set; a later pass retries."""
    settings, engine, qdrant = orch_env
    ports_fail = _ports(
        settings=settings,
        engine=engine,
        qdrant=qdrant,
        embedding_client=HashEmbeddingClient(size=VECTOR_SIZE),
        labeling_client=FakeLabelingClient(fail_when_prompt_contains="Theme work 3"),
    )
    assert dg.materialize(discovery_assets, resources={"ports": ports_fail}).success
    with engine.connect() as connection:
        statuses = {
            row[0]
            for row in connection.execute(select(TopicLabel.__table__.c.status)).all()
        }
    assert "failed" in statuses

    ports_ok = _ports(
        settings=settings,
        engine=engine,
        qdrant=qdrant,
        embedding_client=HashEmbeddingClient(size=VECTOR_SIZE),
        labeling_client=FakeLabelingClient(),
    )
    with capture_logs() as logs:
        result = dg.materialize(
            [selected_corpus, topic_labels], resources={"ports": ports_ok}
        )

    assert result.success
    label_meta = _assert_metadata_matches_boundary(
        result, logs, asset="topic_labels", stage="labels", keys=LABEL_METADATA
    )
    assert label_meta["headlines_processed"] >= 1
    assert label_meta["headlines_succeeded"] >= 1
    assert label_meta["headlines_failed"] == 0
    boundaries = [
        event
        for event in logs
        if isinstance(event, dict)
        and event.get("event") == "stage_boundary"
        and event.get("stage") == "labels"
    ]
    assert len(boundaries) == 1
    assert boundaries[0].get("adopted") is False
    with engine.connect() as connection:
        rows = connection.execute(select(TopicLabel.__table__)).mappings().all()
        assert rows
        assert all(row["status"] == "succeeded" for row in rows)
        assert all(row["headline"] is not None for row in rows)
