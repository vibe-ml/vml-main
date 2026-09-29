"""Fit discovery topics over an embedded corpus and persist a topic run."""

from __future__ import annotations

import hashlib
import time
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

import numpy as np
from sqlalchemy import Engine, insert, update

from src.common.log import get_logger
from src.corpus.models import Corpus, CorpusWork
from src.models.topics import (
    DiscoveryTopic,
    TopicRun,
    TopicRunMapping,
    WorkTopicAssignment,
)
from src.topics.fitter import FitResult, TopicFitter
from src.topics.models import (
    COMPOSITION_HASH_ALGORITHM,
    COMPOSITION_HASH_ENCODING_VERSION,
    COMPOSITION_HASH_SEPARATOR,
    KeywordWeight,
    PersistedAssignment,
    PersistedDiscoveryTopic,
    TopicConfig,
    TopicFitError,
    TopicRunResult,
)

log = get_logger("topics")


def composition_hash(work_ids: Sequence[str]) -> str:
    """Return the SHA-256 composition hash for a topic's member work IDs.

    Members are sorted ascending and joined with ``COMPOSITION_HASH_SEPARATOR`` before
    hashing. The algorithm and encoding version are recorded on each topic run.
    """
    payload = COMPOSITION_HASH_SEPARATOR.join(sorted(work_ids)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def fit_topics(
    corpus: Corpus,
    embeddings: np.ndarray,
    config: TopicConfig,
    engine: Engine,
    *,
    fitter: TopicFitter | None = None,
) -> TopicRunResult:
    """Fit discovery topics for `corpus` and persist an append-only topic run.

    Args:
        corpus: Frozen manifest of works, ordered by work ID.
        embeddings: Precomputed vectors aligned to ``corpus.works``.
        config: Clustering parameters and embedding identity to record.
        engine: SQLAlchemy engine writing processor ``pwf_`` tables.
        fitter: Optional fit port; defaults to a real BERTopic fitter.

    Returns:
        The persisted run with discovery topics and work assignments.

    Raises:
        TopicFitError: The embedding matrix is misaligned or the fit raises.
        ValueError: ``embeddings`` is not a 2-D array matching the corpus length.
    """
    started = time.monotonic()
    started_at = datetime.now(UTC)
    run_id = uuid.uuid4()
    _validate_embeddings(corpus, embeddings)
    _insert_run_started(engine, run_id, corpus, config, started_at)

    if fitter is None:
        from src.topics.bertopic_fitter import BerTopicFitter

        active_fitter: TopicFitter = BerTopicFitter(config)
    else:
        active_fitter = fitter
    documents = tuple(_document_text(work) for work in corpus.works)

    try:
        fit = active_fitter.fit_transform(documents, embeddings)
    except Exception as error:
        elapsed = time.monotonic() - started
        _mark_run_failed(engine, run_id, elapsed)
        if isinstance(error, TopicFitError):
            raise
        raise TopicFitError(f"topic fit failed: {error}") from error

    if len(fit.topic_ids) != len(corpus.works):
        elapsed = time.monotonic() - started
        _mark_run_failed(engine, run_id, elapsed)
        raise TopicFitError(
            f"fitter returned {len(fit.topic_ids)} labels for {len(corpus.works)} works"
        )

    result = _persist_success(
        engine=engine,
        run_id=run_id,
        corpus=corpus,
        fit=fit,
        documents=documents,
        started=started,
    )
    log.info(
        "topics_fitted",
        run_id=str(run_id),
        work_count=result.work_count,
        topic_count=result.topic_count,
        outlier_count=result.outlier_count,
        outlier_rate=result.outlier_rate,
        elapsed_seconds=result.elapsed_seconds,
    )
    return result


def _document_text(work: CorpusWork) -> str:
    if not work.title:
        return work.abstract
    return f"{work.title}\n{work.abstract}"


def _validate_embeddings(corpus: Corpus, embeddings: np.ndarray) -> None:
    if embeddings.ndim != 2:
        raise ValueError(
            f"embeddings must be a 2-D array, got shape {embeddings.shape}"
        )
    if embeddings.shape[0] != len(corpus.works):
        raise ValueError(
            f"embeddings has {embeddings.shape[0]} rows for {len(corpus.works)} works"
        )


def _insert_run_started(
    engine: Engine,
    run_id: uuid.UUID,
    corpus: Corpus,
    config: TopicConfig,
    started_at: datetime,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            insert(TopicRun.__table__).values(
                run_id=run_id,
                status="running",
                scope_ids=list(corpus.config.scope_ids),
                work_types=list(corpus.config.work_types),
                published_from=corpus.config.published_from,
                published_to=corpus.config.published_to,
                excluded_domain_ids=list(corpus.config.excluded_domain_ids),
                embedding_model=config.embedding_model,
                embedding_model_revision=config.embedding_model_revision,
                min_cluster_size=config.min_cluster_size,
                min_samples=config.min_samples,
                umap_random_state=config.umap_random_state,
                umap_metric=config.umap_metric,
                composition_hash_algorithm=COMPOSITION_HASH_ALGORITHM,
                composition_hash_encoding_version=COMPOSITION_HASH_ENCODING_VERSION,
                work_count=len(corpus.works),
                topic_count=0,
                outlier_count=0,
                outlier_rate=0.0,
                started_at=started_at,
                finished_at=None,
                elapsed_seconds=None,
            )
        )


def _mark_run_failed(engine: Engine, run_id: uuid.UUID, elapsed: float) -> None:
    finished_at = datetime.now(UTC)
    with engine.begin() as connection:
        connection.execute(
            update(TopicRun.__table__)
            .where(TopicRun.__table__.c.run_id == run_id)
            .values(
                status="failed",
                finished_at=finished_at,
                elapsed_seconds=round(elapsed, 3),
                topic_count=0,
                outlier_count=0,
                outlier_rate=0.0,
            )
        )


def _persist_success(
    *,
    engine: Engine,
    run_id: uuid.UUID,
    corpus: Corpus,
    fit: FitResult,
    documents: Sequence[str],
    started: float,
) -> TopicRunResult:
    doc_to_work = {
        documents[index]: corpus.works[index] for index in range(len(documents))
    }
    members_by_topic: dict[int, list[str]] = {}
    for work, topic_id in zip(corpus.works, fit.topic_ids, strict=True):
        if topic_id == -1:
            continue
        members_by_topic.setdefault(topic_id, []).append(work.work_id)

    discovery_ids: dict[int, uuid.UUID] = {
        bertopic_id: uuid.uuid4() for bertopic_id in sorted(members_by_topic)
    }

    topics: list[PersistedDiscoveryTopic] = []
    topic_rows: list[dict] = []
    mapping_rows: list[dict] = []
    for bertopic_id, member_ids in sorted(members_by_topic.items()):
        discovery_topic_id = discovery_ids[bertopic_id]
        keywords = tuple(
            KeywordWeight(term=term, weight=weight)
            for term, weight in fit.keywords_by_topic.get(bertopic_id, ())
        )
        rep_docs = fit.representative_docs_by_topic.get(bertopic_id, ())
        representative_work_ids = tuple(
            dict.fromkeys(
                doc_to_work[doc].work_id for doc in rep_docs if doc in doc_to_work
            )
        )
        digest = composition_hash(member_ids)
        topics.append(
            PersistedDiscoveryTopic(
                discovery_topic_id=discovery_topic_id,
                bertopic_topic_id=bertopic_id,
                composition_hash=digest,
                size=len(member_ids),
                keywords=keywords,
                representative_work_ids=representative_work_ids,
            )
        )
        topic_rows.append(
            {
                "discovery_topic_id": discovery_topic_id,
                "topic_run_id": run_id,
                "composition_hash": digest,
                "size": len(member_ids),
                "keywords": [
                    {"term": keyword.term, "weight": keyword.weight}
                    for keyword in keywords
                ],
                "representative_work_ids": list(representative_work_ids),
            }
        )
        mapping_rows.append(
            {
                "topic_run_id": run_id,
                "bertopic_topic_id": bertopic_id,
                "discovery_topic_id": discovery_topic_id,
            }
        )

    assignments: list[PersistedAssignment] = []
    assignment_rows: list[dict] = []
    outlier_count = 0
    for work, topic_id in zip(corpus.works, fit.topic_ids, strict=True):
        is_outlier = topic_id == -1
        if is_outlier:
            outlier_count += 1
            discovery_topic_id = None
        else:
            discovery_topic_id = discovery_ids[topic_id]
        assignments.append(
            PersistedAssignment(
                work_id=work.work_id,
                work_version_id=work.work_version_id,
                discovery_topic_id=discovery_topic_id,
                is_outlier=is_outlier,
                bertopic_topic_id=topic_id,
            )
        )
        assignment_rows.append(
            {
                "topic_run_id": run_id,
                "work_id": work.work_id,
                "work_version_id": work.work_version_id,
                "discovery_topic_id": discovery_topic_id,
                "is_outlier": is_outlier,
                "bertopic_topic_id": topic_id,
            }
        )

    work_count = len(corpus.works)
    topic_count = len(topics)
    outlier_rate = (outlier_count / work_count) if work_count else 0.0
    elapsed = round(time.monotonic() - started, 3)
    finished_at = datetime.now(UTC)

    with engine.begin() as connection:
        if topic_rows:
            connection.execute(insert(DiscoveryTopic.__table__), topic_rows)
        if mapping_rows:
            connection.execute(insert(TopicRunMapping.__table__), mapping_rows)
        if assignment_rows:
            connection.execute(insert(WorkTopicAssignment.__table__), assignment_rows)
        connection.execute(
            update(TopicRun.__table__)
            .where(TopicRun.__table__.c.run_id == run_id)
            .values(
                status="succeeded",
                topic_count=topic_count,
                outlier_count=outlier_count,
                outlier_rate=outlier_rate,
                finished_at=finished_at,
                elapsed_seconds=elapsed,
            )
        )

    return TopicRunResult(
        run_id=run_id,
        status="succeeded",
        work_count=work_count,
        topic_count=topic_count,
        outlier_count=outlier_count,
        outlier_rate=outlier_rate,
        composition_hash_algorithm=COMPOSITION_HASH_ALGORITHM,
        composition_hash_encoding_version=COMPOSITION_HASH_ENCODING_VERSION,
        elapsed_seconds=elapsed,
        topics=tuple(topics),
        assignments=tuple(assignments),
    )
