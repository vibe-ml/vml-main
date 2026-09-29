"""Five Dagster assets and one job wiring the processor stages."""

import time
from collections.abc import Mapping
from typing import Any

import dagster as dg
import structlog
from dagster import AssetExecutionContext
from qdrant_client import QdrantClient
from sqlalchemy import Engine, func, select

from src.common.log import get_logger
from src.corpus import Corpus, select_corpus
from src.embeddings import embed_corpus
from src.embeddings.vectors import (
    corpus_with_cached_vectors,
    load_embedding_matrix,
)
from src.emergence import observe_emergence
from src.labels import label_topics
from src.labels.models import LabelRunResult
from src.models.labels import TopicLabel, WorkSummary
from src.orchestration.adopt import find_existing_labels, find_succeeded_topic_run
from src.orchestration.ports import PipelinePorts
from src.topics import fit_topics

log = get_logger("orchestration")

GROUP = "discovery"


def _bind_run(context: AssetExecutionContext) -> str:
    """Bind the Dagster run id into structlog context; return it for log fields."""
    run_id = context.run.run_id
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        dagster_run_id=run_id,
        asset_key=context.asset_key.to_user_string(),
    )
    return run_id


def _ports(context: AssetExecutionContext) -> PipelinePorts:
    return context.resources.ports


def _int_metadata(counts: Mapping[str, int]) -> dict[str, dg.MetadataValue]:
    """Wrap integer counters as Dagster metadata values."""
    return {key: dg.MetadataValue.int(value) for key, value in counts.items()}


def _collection_point_count(qdrant: QdrantClient, collection: str) -> int:
    """Return the collection point count after a successful embed.

    Uses ``points_count`` from ``get_collection``. When that value is missing,
    uses the client's exact count for the same collection.
    """
    info = qdrant.get_collection(collection)
    if info.points_count is not None:
        return int(info.points_count)
    return int(qdrant.count(collection_name=collection, exact=True).count)


def _table_count(engine: Engine, table: Any) -> int:
    """Return ``COUNT(*)`` for a processor table, including older topic runs."""
    with engine.connect() as connection:
        return int(
            connection.execute(select(func.count()).select_from(table)).scalar_one()
        )


def _label_metadata(result: LabelRunResult, engine: Engine) -> dict[str, int]:
    """Copy this-run label counters and query both store totals."""
    return {
        "summaries_processed": result.summaries_processed,
        "summaries_succeeded": result.summaries_succeeded,
        "summaries_failed": result.summaries_failed,
        "summaries_inserted": result.summaries_inserted,
        "summaries_total": _table_count(engine, WorkSummary.__table__),
        "headlines_processed": result.headlines_processed,
        "headlines_succeeded": result.headlines_succeeded,
        "headlines_failed": result.headlines_failed,
        "headlines_inserted": result.headlines_inserted,
        "headlines_total": _table_count(engine, TopicLabel.__table__),
    }


@dg.asset(
    group_name=GROUP,
    description="Frozen corpus selected from crawler current work versions.",
    required_resource_keys={"ports"},
)
def selected_corpus(context: AssetExecutionContext) -> dg.MaterializeResult:
    """Select the configured corpus; durable evidence stays in crawler tables."""
    run_id = _bind_run(context)
    ports = _ports(context)
    started = time.monotonic()
    corpus = select_corpus(ports.settings.corpus, ports.engine)
    elapsed = round(time.monotonic() - started, 3)
    counts = {"works_selected": len(corpus.works)}
    log.info(
        "stage_boundary",
        stage="corpus",
        run_id=run_id,
        selected=counts["works_selected"],
        considered=corpus.coverage.considered,
        elapsed_seconds=elapsed,
        **counts,
    )
    return dg.MaterializeResult(
        value=corpus,
        metadata=_int_metadata(counts),
    )


@dg.asset(
    ins={"corpus": dg.AssetIn(key=selected_corpus.key)},
    group_name=GROUP,
    description="Corpus vectors cached in Qdrant under deterministic point IDs.",
    required_resource_keys={"ports"},
)
def corpus_embeddings(
    context: AssetExecutionContext, corpus: Corpus
) -> dg.MaterializeResult:
    """Embed the frozen corpus; resumes from Qdrant cache hits without re-calling hosts."""
    run_id = _bind_run(context)
    ports = _ports(context)
    started = time.monotonic()
    config = ports.settings.embedding_config()
    embedded = embed_corpus(corpus, config, ports.embedding_client, ports.qdrant)
    handle = embedded.handle
    collection_total = _collection_point_count(ports.qdrant, handle.collection)
    elapsed = round(time.monotonic() - started, 3)
    counts = {
        "embeddings_processed": embedded.embeddings_processed,
        "embeddings_succeeded": embedded.embeddings_succeeded,
        "embeddings_failed": embedded.embeddings_failed,
        "embeddings_saved": embedded.embeddings_saved,
        "embeddings_collection_total": collection_total,
    }
    log.info(
        "stage_boundary",
        stage="embeddings",
        run_id=run_id,
        selected=len(corpus.works),
        point_count=len(handle.point_ids),
        collection=handle.collection,
        elapsed_seconds=elapsed,
        **counts,
    )
    return dg.MaterializeResult(
        value={
            "collection": handle.collection,
            "point_count": len(handle.point_ids),
        },
        metadata=_int_metadata(counts),
    )


@dg.asset(
    deps=[corpus_embeddings],
    ins={"corpus": dg.AssetIn(key=selected_corpus.key)},
    group_name=GROUP,
    description="Append-only topic run persisted in pwf_ topic tables.",
    required_resource_keys={"ports"},
    pool="topic_fit",
)
def topic_run(context: AssetExecutionContext, corpus: Corpus) -> dict[str, Any]:
    """Fit topics from Qdrant vectors, or adopt a succeeded run of the same identity."""
    run_id = _bind_run(context)
    ports = _ports(context)
    started = time.monotonic()
    embedding_config = ports.settings.embedding_config()
    topic_config = ports.settings.topic_config()

    existing = find_succeeded_topic_run(ports.engine, corpus, topic_config)
    if existing is not None:
        elapsed = round(time.monotonic() - started, 3)
        log.info(
            "stage_boundary",
            stage="topics",
            run_id=run_id,
            topic_run_id=str(existing.run_id),
            adopted=True,
            work_count=existing.work_count,
            topic_count=existing.topic_count,
            elapsed_seconds=elapsed,
        )
        return {
            "topic_run_id": str(existing.run_id),
            "status": existing.status,
            "adopted": True,
            "work_count": existing.work_count,
            "topic_count": existing.topic_count,
        }

    corpus, handle = corpus_with_cached_vectors(corpus, embedding_config, ports.qdrant)
    embeddings = load_embedding_matrix(handle, ports.qdrant, works=corpus.works)
    result = fit_topics(
        corpus,
        embeddings,
        topic_config,
        ports.engine,
        fitter=ports.fitter,
    )
    elapsed = round(time.monotonic() - started, 3)
    log.info(
        "stage_boundary",
        stage="topics",
        run_id=run_id,
        topic_run_id=str(result.run_id),
        adopted=False,
        work_count=result.work_count,
        topic_count=result.topic_count,
        status=result.status,
        elapsed_seconds=elapsed,
    )
    if result.status != "succeeded":
        raise dg.Failure(
            description=(
                f"topic run {result.run_id} finished with status {result.status!r}"
            )
        )
    return {
        "topic_run_id": str(result.run_id),
        "status": result.status,
        "adopted": False,
        "work_count": result.work_count,
        "topic_count": result.topic_count,
    }


@dg.asset(
    deps=[topic_run],
    ins={"corpus": dg.AssetIn(key=selected_corpus.key)},
    group_name=GROUP,
    description="Per-work summaries and discovery topic headlines in pwf_ label tables.",
    required_resource_keys={"ports"},
)
def topic_labels(
    context: AssetExecutionContext, corpus: Corpus
) -> dg.MaterializeResult:
    """Label the latest succeeded topic run for the current corpus/config identity."""
    run_id = _bind_run(context)
    ports = _ports(context)
    started = time.monotonic()
    topic_config = ports.settings.topic_config()
    run = find_succeeded_topic_run(ports.engine, corpus, topic_config)
    if run is None:
        raise dg.Failure(
            description=(
                "no succeeded topic run matches the current corpus/config identity"
            )
        )

    existing = find_existing_labels(
        ports.engine, run.run_id, expected_topic_count=run.topic_count
    )
    if existing is not None:
        elapsed = round(time.monotonic() - started, 3)
        counts = _label_metadata(existing, ports.engine)
        log.info(
            "stage_boundary",
            stage="labels",
            run_id=run_id,
            topic_run_id=str(run.run_id),
            adopted=True,
            label_count=len(existing.labels),
            summary_count=existing.summary_count,
            elapsed_seconds=elapsed,
            **counts,
        )
        return dg.MaterializeResult(
            value={
                "topic_run_id": str(run.run_id),
                "adopted": True,
                "label_count": len(existing.labels),
                "summary_count": existing.summary_count,
            },
            metadata=_int_metadata(counts),
        )

    result = label_topics(
        run.run_id,
        corpus,
        ports.settings.label_config(),
        ports.labeling_client,
        ports.engine,
        tokenizer=ports.tokenizer,
        qdrant=ports.qdrant,
        embedding_config=ports.settings.embedding_config(),
    )
    elapsed = round(time.monotonic() - started, 3)
    counts = _label_metadata(result, ports.engine)
    log.info(
        "stage_boundary",
        stage="labels",
        run_id=run_id,
        topic_run_id=str(result.topic_run_id),
        adopted=False,
        label_count=len(result.labels),
        summary_count=result.summary_count,
        elapsed_seconds=elapsed,
        **counts,
    )
    return dg.MaterializeResult(
        value={
            "topic_run_id": str(result.topic_run_id),
            "adopted": False,
            "label_count": len(result.labels),
            "summary_count": result.summary_count,
        },
        metadata=_int_metadata(counts),
    )


@dg.asset(
    deps=[topic_labels],
    ins={"corpus": dg.AssetIn(key=selected_corpus.key)},
    group_name=GROUP,
    description=(
        "Emergence observations and headline vectors for the latest succeeded topic run."
    ),
    required_resource_keys={"ports"},
)
def topic_emergence(
    context: AssetExecutionContext, corpus: Corpus
) -> dg.MaterializeResult:
    """Score emergence for the succeeded topic run and embed headline cache misses."""
    run_id = _bind_run(context)
    ports = _ports(context)
    started = time.monotonic()
    topic_config = ports.settings.topic_config()
    run = find_succeeded_topic_run(ports.engine, corpus, topic_config)
    if run is None:
        raise dg.Failure(
            description=(
                "no succeeded topic run matches the current corpus/config identity"
            )
        )

    result = observe_emergence(
        run.run_id,
        corpus,
        ports.engine,
        embedding_config=ports.settings.headline_embedding_config(),
        client=ports.embedding_client,
        qdrant=ports.qdrant,
    )
    elapsed = round(time.monotonic() - started, 3)
    counts = {
        "observation_count": result.observation_count,
        "headlines_processed": result.headlines_processed,
        "headlines_cached": result.headlines_cached,
        "headlines_saved": result.headlines_saved,
        "headlines_failed": result.headlines_failed,
    }
    log.info(
        "stage_boundary",
        stage="emergence",
        run_id=run_id,
        topic_run_id=str(run.run_id),
        coverage_status=result.coverage_status,
        elapsed_seconds=elapsed,
        **counts,
    )
    return dg.MaterializeResult(
        value={
            "topic_run_id": str(run.run_id),
            "observation_count": result.observation_count,
            "coverage_status": result.coverage_status,
        },
        metadata=_int_metadata(counts),
    )


discovery_assets = [
    selected_corpus,
    corpus_embeddings,
    topic_run,
    topic_labels,
    topic_emergence,
]

discovery_topics_job = dg.define_asset_job(
    name="discovery_topics_job",
    selection=discovery_assets,
    description=(
        "Select corpus, embed into Qdrant, fit discovery topics, label them, "
        "score emergence, and embed headlines. "
        "Topic and label assets can rematerialize against durable corpus vectors "
        "without calling embedding hosts for cache hits. "
        "Emergence adopts existing observation rows and embeds only headline cache misses."
    ),
    tags={"dagster/concurrency_key": "topic_fit"},
)
