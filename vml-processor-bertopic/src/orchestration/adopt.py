"""Adopt existing durable topic-run and label rows instead of duplicating them."""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence

from sqlalchemy import Connection, Engine, Select, func, select

from src.corpus.models import Corpus
from src.labels.models import LabelRunResult, PersistedTopicLabel
from src.models.labels import TopicLabel, WorkSummary
from src.models.topics import (
    DiscoveryTopic,
    TopicRun,
    TopicRunMapping,
    WorkTopicAssignment,
)
from src.topics.models import (
    KeywordWeight,
    PersistedAssignment,
    PersistedDiscoveryTopic,
    TopicConfig,
    TopicRunResult,
)


def topic_run_identity_clause(
    corpus: Corpus, config: TopicConfig
) -> tuple[object, ...]:
    """Return SQLAlchemy boolean clauses matching the corpus/config identity."""
    runs = TopicRun.__table__
    return (
        runs.c.status == "succeeded",
        runs.c.scope_ids == list(corpus.config.scope_ids),
        runs.c.work_types == list(corpus.config.work_types),
        runs.c.published_from == corpus.config.published_from,
        runs.c.published_to == corpus.config.published_to,
        runs.c.excluded_domain_ids == list(corpus.config.excluded_domain_ids),
        runs.c.embedding_model == config.embedding_model,
        runs.c.embedding_model_revision == config.embedding_model_revision,
        runs.c.min_cluster_size == config.min_cluster_size,
        runs.c.min_samples == config.min_samples,
        runs.c.umap_random_state == config.umap_random_state,
        runs.c.umap_metric == config.umap_metric,
        runs.c.work_count == len(corpus.works),
    )


def find_succeeded_topic_run(
    engine: Engine, corpus: Corpus, config: TopicConfig
) -> TopicRunResult | None:
    """Return the latest succeeded topic run for the corpus/config identity.

    Identity covers corpus filters, embedding model identity, clustering
    parameters recorded on ``pwf_topic_runs``, and work count. A clustering
    parameter change misses this lookup and callers create a new append-only
    run. Failed or still-running rows are never adopted.
    """
    runs = TopicRun.__table__
    query: Select[tuple] = (
        select(runs)
        .where(*topic_run_identity_clause(corpus, config))
        .order_by(runs.c.started_at.desc())
        .limit(1)
    )
    with engine.connect() as connection:
        row = connection.execute(query).mappings().first()
        if row is None:
            return None
        run_id: uuid.UUID = row["run_id"]
        topics = _load_topics(connection, run_id)
        assignments = _load_assignments(connection, run_id)
    return TopicRunResult(
        run_id=run_id,
        status=row["status"],
        work_count=row["work_count"],
        topic_count=row["topic_count"],
        outlier_count=row["outlier_count"],
        outlier_rate=row["outlier_rate"],
        composition_hash_algorithm=row["composition_hash_algorithm"],
        composition_hash_encoding_version=row["composition_hash_encoding_version"],
        elapsed_seconds=float(row["elapsed_seconds"] or 0.0),
        topics=topics,
        assignments=assignments,
    )


def find_existing_labels(
    engine: Engine, topic_run_id: uuid.UUID, *, expected_topic_count: int
) -> LabelRunResult | None:
    """Return labels when every discovery topic has a succeeded full-sample label.

    A label whose status is not ``succeeded``, or whose ``sample_size`` is below
    ``min(15, topic_size)``, is incomplete so a later labeling pass regenerates
    it. This-run summary and headline counters stay 0. Callers query store
    totals.
    """
    labels_table = TopicLabel.__table__
    topics_table = DiscoveryTopic.__table__
    with engine.connect() as connection:
        label_rows = (
            connection.execute(
                select(labels_table).where(labels_table.c.topic_run_id == topic_run_id)
            )
            .mappings()
            .all()
        )
        if expected_topic_count == 0:
            if label_rows:
                return None
            summary_count = _summary_count(connection, topic_run_id)
            return LabelRunResult(
                topic_run_id=topic_run_id,
                labels=(),
                summary_count=summary_count,
            )
        if len(label_rows) != expected_topic_count:
            return None
        topic_sizes = {
            row["discovery_topic_id"]: int(row["size"])
            for row in connection.execute(
                select(
                    topics_table.c.discovery_topic_id,
                    topics_table.c.size,
                ).where(topics_table.c.topic_run_id == topic_run_id)
            ).mappings()
        }
        for row in label_rows:
            if str(row["status"]) != "succeeded":
                return None
            topic_id = row["discovery_topic_id"]
            size = topic_sizes.get(topic_id)
            if size is None:
                return None
            if int(row["sample_size"]) < min(15, size):
                return None
        summary_count = _summary_count(connection, topic_run_id)
        labels = tuple(_label_from_row(row) for row in label_rows)
    return LabelRunResult(
        topic_run_id=topic_run_id,
        labels=labels,
        summary_count=summary_count,
    )


def _summary_count(connection: Connection, topic_run_id: uuid.UUID) -> int:
    summaries_table = WorkSummary.__table__
    return int(
        connection.execute(
            select(func.count())
            .select_from(summaries_table)
            .where(summaries_table.c.topic_run_id == topic_run_id)
        ).scalar_one()
    )


def _load_topics(
    connection: Connection, run_id: uuid.UUID
) -> tuple[PersistedDiscoveryTopic, ...]:
    topics_table = DiscoveryTopic.__table__
    rows = (
        connection.execute(
            select(topics_table).where(topics_table.c.topic_run_id == run_id)
        )
        .mappings()
        .all()
    )
    mappings_table = TopicRunMapping.__table__
    mapping_rows = (
        connection.execute(
            select(mappings_table).where(mappings_table.c.topic_run_id == run_id)
        )
        .mappings()
        .all()
    )
    bertopic_by_discovery = {
        row["discovery_topic_id"]: row["bertopic_topic_id"] for row in mapping_rows
    }
    topics: list[PersistedDiscoveryTopic] = []
    for row in rows:
        keywords_raw: Sequence[Mapping[str, object]] = row["keywords"] or ()
        keywords = tuple(
            KeywordWeight(term=str(item["term"]), weight=float(item["weight"]))  # type: ignore[arg-type]
            for item in keywords_raw
        )
        topics.append(
            PersistedDiscoveryTopic(
                discovery_topic_id=row["discovery_topic_id"],
                bertopic_topic_id=int(
                    bertopic_by_discovery.get(row["discovery_topic_id"], -1)
                ),
                composition_hash=row["composition_hash"],
                size=row["size"],
                keywords=keywords,
                representative_work_ids=tuple(row["representative_work_ids"] or ()),
            )
        )
    return tuple(topics)


def _load_assignments(
    connection: Connection, run_id: uuid.UUID
) -> tuple[PersistedAssignment, ...]:
    assignments = WorkTopicAssignment.__table__
    rows = (
        connection.execute(
            select(assignments).where(assignments.c.topic_run_id == run_id)
        )
        .mappings()
        .all()
    )
    return tuple(
        PersistedAssignment(
            work_id=row["work_id"],
            work_version_id=row["work_version_id"],
            discovery_topic_id=row["discovery_topic_id"],
            is_outlier=row["is_outlier"],
            bertopic_topic_id=row["bertopic_topic_id"],
        )
        for row in rows
    )


def _label_from_row(row: Mapping[str, object]) -> PersistedTopicLabel:
    sampled = row["sampled_work_ids"] or ()
    return PersistedTopicLabel(
        discovery_topic_id=row["discovery_topic_id"],  # type: ignore[arg-type]
        headline=row["headline"],  # type: ignore[arg-type]
        concatenated_summary_text=row["concatenated_summary_text"],  # type: ignore[arg-type]
        chunk_count=int(row["chunk_count"]),  # type: ignore[arg-type]
        sampling_method=str(row["sampling_method"]),
        sampled_work_ids=tuple(sampled),  # type: ignore[arg-type]
        sample_size=int(row["sample_size"]),  # type: ignore[arg-type]
        model=str(row["model"]),
        model_revision=str(row["model_revision"]),
        prompt_version=str(row["prompt_version"]),
        status=str(row["status"]),
    )
