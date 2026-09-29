"""Generate and persist discovery topic labels from a completed topic run."""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import numpy as np
from qdrant_client import QdrantClient
from sqlalchemy import Engine, insert, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError

from src.common.log import get_logger
from src.corpus.models import Corpus, CorpusWork
from src.embeddings.models import EmbeddingConfig
from src.embeddings.vectors import load_vectors_by_work
from src.labels.models import (
    GenerationResult,
    LabelConfig,
    LabelingClient,
    LabelingError,
    LabelRunResult,
    PersistedTopicLabel,
    Tokenizer,
)
from src.labels.prompts import (
    HEADLINE_PROMPT_TEMPLATE,
    SUMMARY_PROMPT_TEMPLATE,
)
from src.models.labels import TopicLabel, WorkSummary
from src.models.topics import DiscoveryTopic, TopicRun, WorkTopicAssignment
from src.topics.representatives import (
    DEFAULT_REPRESENTATIVE_CAP,
    centroid_nearest_work_ids,
)

log = get_logger("labels")

SAMPLING_REPRESENTATIVE = "representative_docs"
SAMPLING_ALL_MEMBERS = "all_members"
SINGLE_PASS_CHUNK_COUNT = 1


@dataclass
class _LabelCounters:
    """Mutable this-run counters accumulated while labeling one topic run."""

    summaries_processed: int = 0
    summaries_succeeded: int = 0
    summaries_failed: int = 0
    summaries_inserted: int = 0
    headlines_processed: int = 0
    headlines_succeeded: int = 0
    headlines_failed: int = 0
    headlines_inserted: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add_summary_processed(self, n: int = 1) -> None:
        with self._lock:
            self.summaries_processed += n

    def add_summary_succeeded(self, n: int = 1) -> None:
        with self._lock:
            self.summaries_succeeded += n

    def add_summary_failed(self, n: int = 1) -> None:
        with self._lock:
            self.summaries_failed += n

    def add_summary_inserted(self, n: int) -> None:
        with self._lock:
            self.summaries_inserted += n

    def add_headline_processed(self, n: int = 1) -> None:
        with self._lock:
            self.headlines_processed += n

    def add_headline_succeeded(self, n: int = 1) -> None:
        with self._lock:
            self.headlines_succeeded += n

    def add_headline_failed(self, n: int = 1) -> None:
        with self._lock:
            self.headlines_failed += n

    def add_headline_inserted(self, n: int = 1) -> None:
        with self._lock:
            self.headlines_inserted += n


def label_topics(
    topic_run_id: uuid.UUID,
    works: Corpus | Sequence[CorpusWork] | Mapping[str, CorpusWork],
    config: LabelConfig,
    client: LabelingClient,
    engine: Engine,
    *,
    tokenizer: Tokenizer,
    qdrant: QdrantClient | None = None,
    embedding_config: EmbeddingConfig | None = None,
    vectors_by_work: Mapping[str, np.ndarray] | None = None,
) -> LabelRunResult:
    """Label every discovery topic in a persisted run and return the rows.

    Args:
        topic_run_id: Completed topic run whose discovery topics are labeled.
        works: Corpus or work text source providing title and abstract by work ID.
        config: Model, sample size, and prompt version settings.
        client: Injected labeling port; tests supply a deterministic fake.
        engine: SQLAlchemy engine writing processor ``pwf_`` tables.
        tokenizer: Injected tokenizer port for context-window chunking.
        qdrant: Optional Qdrant client used to backfill short representative lists.
        embedding_config: Cache identity for Qdrant point lookup during backfill.
        vectors_by_work: Optional preloaded vectors keyed by work ID (tests).

    Returns:
        Persisted labels and the count of work summaries written.

    Raises:
        LabelingError: The topic run is missing or not in a succeeded state.
    """
    started = time.monotonic()
    work_by_id = _index_works(works)
    topics, members_by_topic = _load_run(engine, topic_run_id)
    if vectors_by_work is not None or (
        qdrant is not None and embedding_config is not None
    ):
        topics = _backfill_representative_work_ids(
            engine,
            topic_run_id=topic_run_id,
            topics=topics,
            members_by_topic=members_by_topic,
            work_by_id=work_by_id,
            sample_size=config.sample_size,
            qdrant=qdrant,
            embedding_config=embedding_config,
            vectors_by_work=vectors_by_work,
        )
    existing_labels = _existing_labels(engine, topic_run_id)
    generate_slots = threading.Semaphore(config.max_concurrency)

    pending: list[tuple[int, Mapping[str, object], tuple[str, ...], bool]] = []
    for index, topic in enumerate(topics):
        discovery_topic_id = topic["discovery_topic_id"]
        assert isinstance(discovery_topic_id, uuid.UUID)
        existing = existing_labels.get(discovery_topic_id)
        replace_label = False
        if existing is not None:
            if _should_skip_existing_label(existing, topic, config.sample_size):
                log.info(
                    "topic_label_skipped_existing",
                    topic_run_id=str(topic_run_id),
                    discovery_topic_id=str(discovery_topic_id),
                    sample_size=existing["sample_size"],
                )
                continue
            replace_label = True
            log.info(
                "topic_label_regenerating_short_sample",
                topic_run_id=str(topic_run_id),
                discovery_topic_id=str(discovery_topic_id),
                status=existing["status"],
                sample_size=existing["sample_size"],
                topic_size=int(topic["size"]),  # type: ignore[arg-type]
                representative_count=len(
                    _unique_ids(topic["representative_work_ids"])  # type: ignore[arg-type]
                ),
            )
        member_ids = members_by_topic.get(discovery_topic_id, ())
        pending.append((index, topic, member_ids, replace_label))

    counters = _LabelCounters()
    # Preserve discovery-topic order in the returned labels tuple.
    ordered: dict[int, tuple[PersistedTopicLabel, int]] = {}

    def _run_one(
        item: tuple[int, Mapping[str, object], tuple[str, ...], bool],
    ) -> tuple[int, PersistedTopicLabel, int]:
        index, topic, member_ids, replace_label = item
        persisted, wrote = _label_one_topic(
            engine=engine,
            topic_run_id=topic_run_id,
            topic=topic,
            member_ids=member_ids,
            work_by_id=work_by_id,
            config=config,
            client=client,
            tokenizer=tokenizer,
            counters=counters,
            generate_slots=generate_slots,
            replace_label=replace_label,
        )
        return index, persisted, wrote

    if pending:
        workers = min(config.max_concurrency, len(pending))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_run_one, item) for item in pending]
            for future in as_completed(futures):
                index, persisted, wrote = future.result()
                ordered[index] = (persisted, wrote)

    labels: list[PersistedTopicLabel] = []
    summary_count = 0
    for index in sorted(ordered):
        persisted, wrote = ordered[index]
        labels.append(persisted)
        summary_count += wrote

    elapsed = round(time.monotonic() - started, 3)
    log.info(
        "topics_labeled",
        topic_run_id=str(topic_run_id),
        topic_count=len(labels),
        summary_count=summary_count,
        failed_count=sum(1 for label in labels if label.status == "failed"),
        elapsed_seconds=elapsed,
    )
    return LabelRunResult(
        topic_run_id=topic_run_id,
        labels=tuple(labels),
        summary_count=summary_count,
        summaries_processed=counters.summaries_processed,
        summaries_succeeded=counters.summaries_succeeded,
        summaries_failed=counters.summaries_failed,
        summaries_inserted=counters.summaries_inserted,
        headlines_processed=counters.headlines_processed,
        headlines_succeeded=counters.headlines_succeeded,
        headlines_failed=counters.headlines_failed,
        headlines_inserted=counters.headlines_inserted,
    )


def _index_works(
    works: Corpus | Sequence[CorpusWork] | Mapping[str, CorpusWork],
) -> dict[str, CorpusWork]:
    if isinstance(works, Corpus):
        return {work.work_id: work for work in works.works}
    if isinstance(works, Mapping):
        return dict(works)
    return {work.work_id: work for work in works}


def _load_run(
    engine: Engine, topic_run_id: uuid.UUID
) -> tuple[list[dict], dict[uuid.UUID, tuple[str, ...]]]:
    with engine.connect() as connection:
        run = (
            connection.execute(
                select(TopicRun.__table__).where(
                    TopicRun.__table__.c.run_id == topic_run_id
                )
            )
            .mappings()
            .one_or_none()
        )
        if run is None:
            raise LabelingError(f"topic run {topic_run_id} not found")
        if run["status"] != "succeeded":
            raise LabelingError(
                f"topic run {topic_run_id} has status {run['status']!r}; expected succeeded"
            )
        topics = (
            connection.execute(
                select(DiscoveryTopic.__table__)
                .where(DiscoveryTopic.__table__.c.topic_run_id == topic_run_id)
                .order_by(DiscoveryTopic.__table__.c.discovery_topic_id)
            )
            .mappings()
            .all()
        )
        assignments = (
            connection.execute(
                select(WorkTopicAssignment.__table__).where(
                    WorkTopicAssignment.__table__.c.topic_run_id == topic_run_id,
                    WorkTopicAssignment.__table__.c.is_outlier.is_(False),
                )
            )
            .mappings()
            .all()
        )

    members_by_topic: dict[uuid.UUID, list[str]] = {}
    for row in assignments:
        topic_id = row["discovery_topic_id"]
        if topic_id is None:
            continue
        members_by_topic.setdefault(topic_id, []).append(row["work_id"])
    return (
        [dict(row) for row in topics],
        {key: tuple(values) for key, values in members_by_topic.items()},
    )


def _unique_ids(work_ids: Sequence[str]) -> tuple[str, ...]:
    """Drop repeated work ids, keeping the first occurrence."""
    return tuple(dict.fromkeys(work_ids))


def _existing_labels(engine: Engine, topic_run_id: uuid.UUID) -> dict[uuid.UUID, dict]:
    """Return discovery_topic_id -> label row mapping for this run."""
    table = TopicLabel.__table__
    with engine.connect() as connection:
        rows = (
            connection.execute(
                select(table).where(table.c.topic_run_id == topic_run_id)
            )
            .mappings()
            .all()
        )
    return {row["discovery_topic_id"]: dict(row) for row in rows}


def _target_sample_size(topic: Mapping[str, object], sample_size: int) -> int:
    """Return the agreed sample size: ``min(config, topic_size)``."""
    size = int(topic["size"])  # type: ignore[arg-type]
    return min(sample_size, size)


def _should_skip_existing_label(
    existing: Mapping[str, object],
    topic: Mapping[str, object],
    sample_size: int,
) -> bool:
    """Skip only a succeeded label whose sample is full or cannot grow yet.

    Rows with status other than ``succeeded`` are retried so a later pass can
    replace the failed headline. Undersampled succeeded rows still regenerate
    after representative backfill.
    """
    if existing.get("status") != "succeeded":
        return False
    stored = int(existing["sample_size"])  # type: ignore[arg-type]
    target = _target_sample_size(topic, sample_size)
    if stored >= target:
        return True
    representatives = _unique_ids(topic["representative_work_ids"])  # type: ignore[arg-type]
    # Undersampled and the topic still has only as many reps as already used.
    return len(representatives) <= stored


def _backfill_representative_work_ids(
    engine: Engine,
    *,
    topic_run_id: uuid.UUID,
    topics: list[dict],
    members_by_topic: Mapping[uuid.UUID, tuple[str, ...]],
    work_by_id: Mapping[str, CorpusWork],
    sample_size: int,
    qdrant: QdrantClient | None,
    embedding_config: EmbeddingConfig | None,
    vectors_by_work: Mapping[str, np.ndarray] | None,
) -> list[dict]:
    """Fill short ``representative_work_ids`` from available member vectors.

    Updates ``pwf_discovery_topics`` in place when centroid selection yields more
    representatives than currently stored. Missing vectors are skipped; the run
    continues with the nearest available members.
    """
    needed_work_ids: list[str] = []
    short_topics: list[tuple[int, dict, tuple[str, ...], int]] = []
    for index, topic in enumerate(topics):
        discovery_topic_id = topic["discovery_topic_id"]
        assert isinstance(discovery_topic_id, uuid.UUID)
        size = int(topic["size"])
        target = min(sample_size, size, DEFAULT_REPRESENTATIVE_CAP)
        current = _unique_ids(topic["representative_work_ids"] or ())
        if len(current) >= target:
            continue
        member_ids = members_by_topic.get(discovery_topic_id, ())
        short_topics.append((index, topic, member_ids, target))
        needed_work_ids.extend(member_ids)

    if not short_topics:
        return topics

    unique_needed = tuple(dict.fromkeys(needed_work_ids))
    if vectors_by_work is None:
        assert qdrant is not None and embedding_config is not None
        works_for_lookup = [
            work_by_id[work_id] for work_id in unique_needed if work_id in work_by_id
        ]
        vectors_by_work = load_vectors_by_work(
            works_for_lookup, embedding_config, qdrant
        )
    else:
        vectors_by_work = dict(vectors_by_work)

    updated_topics = [dict(topic) for topic in topics]
    table = DiscoveryTopic.__table__
    with engine.begin() as connection:
        for index, topic, member_ids, target in short_topics:
            discovery_topic_id = topic["discovery_topic_id"]
            assert isinstance(discovery_topic_id, uuid.UUID)
            current = _unique_ids(topic["representative_work_ids"] or ())
            selected = centroid_nearest_work_ids(
                member_ids, vectors_by_work, cap=target
            )
            missing = sum(1 for work_id in member_ids if work_id not in vectors_by_work)
            if missing:
                log.warning(
                    "representative_backfill_missing_vectors",
                    topic_run_id=str(topic_run_id),
                    discovery_topic_id=str(discovery_topic_id),
                    topic_size=int(topic["size"]),
                    missing_vectors=missing,
                    available_vectors=len(member_ids) - missing,
                    selected=len(selected),
                    target=target,
                )
            if len(selected) <= len(current):
                continue
            connection.execute(
                update(table)
                .where(
                    table.c.topic_run_id == topic_run_id,
                    table.c.discovery_topic_id == discovery_topic_id,
                )
                .values(representative_work_ids=list(selected))
            )
            updated_topics[index] = {
                **updated_topics[index],
                "representative_work_ids": list(selected),
            }
            log.info(
                "representative_work_ids_backfilled",
                topic_run_id=str(topic_run_id),
                discovery_topic_id=str(discovery_topic_id),
                previous=len(current),
                updated=len(selected),
                target=target,
            )
    return updated_topics


def _existing_summaries(
    engine: Engine,
    *,
    topic_run_id: uuid.UUID,
    discovery_topic_id: uuid.UUID,
    work_ids: Sequence[str],
) -> dict[str, str]:
    """Return work_id -> summary_text for rows already stored for this topic."""
    if not work_ids:
        return {}
    table = WorkSummary.__table__
    with engine.connect() as connection:
        rows = (
            connection.execute(
                select(table.c.work_id, table.c.summary_text).where(
                    table.c.topic_run_id == topic_run_id,
                    table.c.discovery_topic_id == discovery_topic_id,
                    table.c.work_id.in_(tuple(work_ids)),
                )
            )
            .mappings()
            .all()
        )
    return {row["work_id"]: row["summary_text"] for row in rows}


def _select_works(
    topic: Mapping[str, object],
    member_ids: Sequence[str],
    sample_size: int,
) -> tuple[tuple[str, ...], str]:
    size = int(topic["size"])  # type: ignore[arg-type]
    if size < 3:
        return _unique_ids(member_ids), SAMPLING_ALL_MEMBERS

    representatives = _unique_ids(topic["representative_work_ids"])  # type: ignore[arg-type]
    cap = min(sample_size, size)
    return representatives[:cap], SAMPLING_REPRESENTATIVE


def _label_one_topic(
    *,
    engine: Engine,
    topic_run_id: uuid.UUID,
    topic: Mapping[str, object],
    member_ids: Sequence[str],
    work_by_id: Mapping[str, CorpusWork],
    config: LabelConfig,
    client: LabelingClient,
    tokenizer: Tokenizer,
    counters: _LabelCounters,
    generate_slots: threading.Semaphore,
    replace_label: bool = False,
) -> tuple[PersistedTopicLabel, int]:
    discovery_topic_id = topic["discovery_topic_id"]  # type: ignore[assignment]
    assert isinstance(discovery_topic_id, uuid.UUID)
    sampled_ids, sampling_method = _select_works(topic, member_ids, config.sample_size)

    if len(sampled_ids) < config.sample_size:
        log.info(
            "label_sample_shortfall",
            topic_run_id=str(topic_run_id),
            discovery_topic_id=str(discovery_topic_id),
            available=len(sampled_ids),
            sample_size=config.sample_size,
            topic_size=int(topic["size"]),  # type: ignore[arg-type]
        )

    summary_rows: list[dict] = []
    summaries: list[str] = []
    concatenated: str | None = None
    chunk_count = SINGLE_PASS_CHUNK_COUNT
    try:
        _summarize_works(
            sampled_ids=sampled_ids,
            work_by_id=work_by_id,
            config=config,
            client=client,
            engine=engine,
            topic_run_id=topic_run_id,
            discovery_topic_id=discovery_topic_id,
            counters=counters,
            generate_slots=generate_slots,
            rows=summary_rows,
            summaries=summaries,
        )
        concatenated = "\n\n".join(summaries)
        counters.add_headline_processed()
        headline, chunk_count = _generate_headline(
            summaries=summaries,
            concatenated=concatenated,
            config=config,
            client=client,
            tokenizer=tokenizer,
            generate_slots=generate_slots,
        )
        persisted = PersistedTopicLabel(
            discovery_topic_id=discovery_topic_id,
            headline=headline,
            concatenated_summary_text=concatenated,
            chunk_count=chunk_count,
            sampling_method=sampling_method,
            sampled_work_ids=sampled_ids,
            sample_size=len(sampled_ids),
            model=config.model,
            model_revision=config.model_revision,
            prompt_version=config.prompt_version,
            status="succeeded",
        )
    except Exception as error:  # noqa: BLE001 — isolate labeling failures per topic
        if concatenated is None and summaries:
            concatenated = "\n\n".join(summaries)
        log.warning(
            "topic_label_failed",
            topic_run_id=str(topic_run_id),
            discovery_topic_id=str(discovery_topic_id),
            error=str(error),
        )
        failed = PersistedTopicLabel(
            discovery_topic_id=discovery_topic_id,
            headline=None,
            concatenated_summary_text=concatenated,
            chunk_count=chunk_count,
            sampling_method=sampling_method,
            sampled_work_ids=sampled_ids,
            sample_size=len(sampled_ids),
            model=config.model,
            model_revision=config.model_revision,
            prompt_version=config.prompt_version,
            status="failed",
        )
        wrote = _try_persist_topic_artifacts(
            engine,
            topic_run_id=topic_run_id,
            label=failed,
            summary_rows=summary_rows,
            replace_label=replace_label,
        )
        counters.add_summary_inserted(wrote)
        counters.add_headline_failed()
        if wrote >= 0 and _label_row_exists(engine, topic_run_id, discovery_topic_id):
            counters.add_headline_inserted()
        return failed, max(wrote, 0)

    wrote = _try_persist_topic_artifacts(
        engine,
        topic_run_id=topic_run_id,
        label=persisted,
        summary_rows=summary_rows,
        replace_label=replace_label,
    )
    if wrote < 0:
        # Persist failed after a successful generate; keep earlier topics intact.
        failed = PersistedTopicLabel(
            discovery_topic_id=discovery_topic_id,
            headline=None,
            concatenated_summary_text=concatenated,
            chunk_count=chunk_count,
            sampling_method=sampling_method,
            sampled_work_ids=sampled_ids,
            sample_size=len(sampled_ids),
            model=config.model,
            model_revision=config.model_revision,
            prompt_version=config.prompt_version,
            status="failed",
        )
        counters.add_headline_failed()
        return failed, 0

    counters.add_summary_inserted(wrote)
    counters.add_headline_succeeded()
    counters.add_headline_inserted()
    return persisted, wrote


def _label_row_exists(
    engine: Engine, topic_run_id: uuid.UUID, discovery_topic_id: uuid.UUID
) -> bool:
    table = TopicLabel.__table__
    with engine.connect() as connection:
        row = connection.execute(
            select(table.c.discovery_topic_id).where(
                table.c.topic_run_id == topic_run_id,
                table.c.discovery_topic_id == discovery_topic_id,
            )
        ).first()
    return row is not None


def _headline_fits(
    summaries_text: str, config: LabelConfig, tokenizer: Tokenizer
) -> bool:
    """Return True when prompt tokens plus the output reserve fit the window."""
    prompt = HEADLINE_PROMPT_TEMPLATE.format(summaries=summaries_text)
    return (
        tokenizer.count_tokens(prompt) + config.output_reserve <= config.context_window
    )


def _pack_summary_windows(
    summaries: Sequence[str], config: LabelConfig, tokenizer: Tokenizer
) -> tuple[str, ...]:
    """Split summaries into windows that each fit the token budget.

    Boundaries fall between whole per-work summaries, never inside one.
    """
    windows: list[list[str]] = []
    current: list[str] = []
    for summary in summaries:
        candidate = (*current, summary)
        candidate_text = "\n\n".join(candidate)
        if current and not _headline_fits(candidate_text, config, tokenizer):
            windows.append(current)
            current = [summary]
        else:
            current = list(candidate)
    if current:
        windows.append(current)
    return tuple("\n\n".join(window) for window in windows)


def _generate_with_retries(
    client: LabelingClient,
    prompt: str,
    *,
    max_retries: int,
    backoff_seconds: float,
    generate_slots: threading.Semaphore,
) -> GenerationResult:
    """Call ``client.generate`` with bounded exponential backoff retries."""
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            generate_slots.acquire()
            try:
                return client.generate(prompt)
            finally:
                generate_slots.release()
        except Exception as error:  # noqa: BLE001 — retry then let caller decide
            last_error = error
            if attempt < max_retries and backoff_seconds > 0:
                time.sleep(backoff_seconds * (2**attempt))
    assert last_error is not None
    raise last_error


def _generate_headline(
    *,
    summaries: Sequence[str],
    concatenated: str,
    config: LabelConfig,
    client: LabelingClient,
    tokenizer: Tokenizer,
    generate_slots: threading.Semaphore,
) -> tuple[str, int]:
    """Return the topic headline and how many summary windows produced it."""
    if _headline_fits(concatenated, config, tokenizer):
        prompt = HEADLINE_PROMPT_TEMPLATE.format(summaries=concatenated)
        return (
            _generate_with_retries(
                client,
                prompt,
                max_retries=config.max_retries,
                backoff_seconds=config.retry_backoff_seconds,
                generate_slots=generate_slots,
            ).text,
            SINGLE_PASS_CHUNK_COUNT,
        )

    windows = _pack_summary_windows(summaries, config, tokenizer)
    # One unsplittable window still uses a single generate; chunk_count stays 1.
    if len(windows) == 1:
        prompt = HEADLINE_PROMPT_TEMPLATE.format(summaries=windows[0])
        return (
            _generate_with_retries(
                client,
                prompt,
                max_retries=config.max_retries,
                backoff_seconds=config.retry_backoff_seconds,
                generate_slots=generate_slots,
            ).text,
            SINGLE_PASS_CHUNK_COUNT,
        )

    window_headlines = [
        _generate_with_retries(
            client,
            HEADLINE_PROMPT_TEMPLATE.format(summaries=window),
            max_retries=config.max_retries,
            backoff_seconds=config.retry_backoff_seconds,
            generate_slots=generate_slots,
        ).text
        for window in windows
    ]
    final_prompt = HEADLINE_PROMPT_TEMPLATE.format(
        summaries="\n\n".join(window_headlines)
    )
    return (
        _generate_with_retries(
            client,
            final_prompt,
            max_retries=config.max_retries,
            backoff_seconds=config.retry_backoff_seconds,
            generate_slots=generate_slots,
        ).text,
        len(windows),
    )


def _summarize_works(
    *,
    sampled_ids: Sequence[str],
    work_by_id: Mapping[str, CorpusWork],
    config: LabelConfig,
    client: LabelingClient,
    engine: Engine,
    topic_run_id: uuid.UUID,
    discovery_topic_id: uuid.UUID,
    counters: _LabelCounters,
    generate_slots: threading.Semaphore,
    rows: list[dict],
    summaries: list[str],
) -> None:
    """Summarize sampled works, recording each attempt on `counters`.

    Existing ``pwf_work_summaries`` rows are reused without a model call or
    insert. New calls run concurrently under ``generate_slots``; results are
    assembled in sample order. A call that still raises after retries skips that
    work so the topic can continue. Missing work text raises before any calls.
    """
    for work_id in sampled_ids:
        if work_by_id.get(work_id) is None:
            raise LabelingError(
                f"work {work_id} is sampled for topic {discovery_topic_id} but missing from text source"
            )

    cached = _existing_summaries(
        engine,
        topic_run_id=topic_run_id,
        discovery_topic_id=discovery_topic_id,
        work_ids=sampled_ids,
    )
    # Slot results by sample index so concatenation ignores completion order.
    texts: list[str | None] = [None] * len(sampled_ids)
    new_rows: list[dict | None] = [None] * len(sampled_ids)
    to_generate: list[tuple[int, str]] = []

    for index, work_id in enumerate(sampled_ids):
        if work_id in cached:
            texts[index] = cached[work_id]
            continue
        to_generate.append((index, work_id))

    def _summarize_one(
        index: int, work_id: str
    ) -> tuple[int, str, dict] | tuple[int, None, None]:
        work = work_by_id[work_id]
        prompt = SUMMARY_PROMPT_TEMPLATE.format(
            title=work.title or "",
            abstract=work.abstract,
        )
        counters.add_summary_processed()
        try:
            result = _generate_with_retries(
                client,
                prompt,
                max_retries=config.max_retries,
                backoff_seconds=config.retry_backoff_seconds,
                generate_slots=generate_slots,
            )
        except Exception as error:  # noqa: BLE001 — skip one work, continue topic
            counters.add_summary_failed()
            log.warning(
                "work_summary_skipped",
                topic_run_id=str(topic_run_id),
                discovery_topic_id=str(discovery_topic_id),
                work_id=work_id,
                error=str(error),
            )
            return index, None, None
        counters.add_summary_succeeded()
        row = {
            "topic_run_id": topic_run_id,
            "discovery_topic_id": discovery_topic_id,
            "work_id": work_id,
            "summary_text": result.text,
            "model": config.model,
            "model_revision": config.model_revision,
            "prompt_version": config.prompt_version,
            "prompt_tokens": result.prompt_tokens,
            "completion_tokens": result.completion_tokens,
        }
        return index, result.text, row

    if to_generate:
        workers = min(config.max_concurrency, len(to_generate))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(_summarize_one, index, work_id)
                for index, work_id in to_generate
            ]
            for future in as_completed(futures):
                index, text, row = future.result()
                if text is None:
                    continue
                texts[index] = text
                new_rows[index] = row

    for index, text in enumerate(texts):
        if text is None:
            continue
        summaries.append(text)
        row = new_rows[index]
        if row is not None:
            rows.append(row)

    if not summaries:
        raise LabelingError(f"all summary calls failed for topic {discovery_topic_id}")


def _try_persist_topic_artifacts(
    engine: Engine,
    *,
    topic_run_id: uuid.UUID,
    label: PersistedTopicLabel,
    summary_rows: Sequence[Mapping[str, object]],
    replace_label: bool = False,
) -> int:
    """Persist one topic's summaries and label; return inserted summary count.

    Returns ``-1`` when a database error prevents persistence so the caller can
    keep earlier committed topics and move on. Returns ``0`` when the label row
    already exists and ``replace_label`` is false (resume skip).
    """
    try:
        return _persist_topic_artifacts(
            engine,
            topic_run_id=topic_run_id,
            label=label,
            summary_rows=summary_rows,
            replace_label=replace_label,
        )
    except (IntegrityError, OperationalError, DBAPIError) as error:
        log.warning(
            "topic_label_persist_failed",
            topic_run_id=str(topic_run_id),
            discovery_topic_id=str(label.discovery_topic_id),
            error=str(error),
        )
        return -1


def _persist_topic_artifacts(
    engine: Engine,
    *,
    topic_run_id: uuid.UUID,
    label: PersistedTopicLabel,
    summary_rows: Sequence[Mapping[str, object]],
    replace_label: bool = False,
) -> int:
    """Insert missing summaries then insert or replace the label for one topic.

    Reuses existing summary rows by primary key. Skips the whole write when the
    label row already exists and ``replace_label`` is false so a resume never
    raises UniqueViolation. When replacing, updates the headline and sampling
    metadata in the same transaction as any new summary inserts.
    """
    table_summary = WorkSummary.__table__
    table_label = TopicLabel.__table__
    with engine.begin() as connection:
        existing_label = connection.execute(
            select(table_label.c.discovery_topic_id).where(
                table_label.c.topic_run_id == topic_run_id,
                table_label.c.discovery_topic_id == label.discovery_topic_id,
            )
        ).first()
        if existing_label is not None and not replace_label:
            return 0

        existing_work_ids: set[str] = set()
        if summary_rows:
            work_ids = tuple(row["work_id"] for row in summary_rows)
            existing_work_ids = {
                row[0]
                for row in connection.execute(
                    select(table_summary.c.work_id).where(
                        table_summary.c.topic_run_id == topic_run_id,
                        table_summary.c.discovery_topic_id == label.discovery_topic_id,
                        table_summary.c.work_id.in_(work_ids),
                    )
                )
            }
        new_rows = [
            dict(row) for row in summary_rows if row["work_id"] not in existing_work_ids
        ]
        if new_rows:
            connection.execute(insert(table_summary), new_rows)
        label_values = {
            "headline": label.headline,
            "concatenated_summary_text": label.concatenated_summary_text,
            "chunk_count": label.chunk_count,
            "sampling_method": label.sampling_method,
            "sampled_work_ids": list(label.sampled_work_ids),
            "sample_size": label.sample_size,
            "model": label.model,
            "model_revision": label.model_revision,
            "prompt_version": label.prompt_version,
            "status": label.status,
        }
        if existing_label is not None and replace_label:
            connection.execute(
                update(table_label)
                .where(
                    table_label.c.topic_run_id == topic_run_id,
                    table_label.c.discovery_topic_id == label.discovery_topic_id,
                )
                .values(**label_values)
            )
        else:
            connection.execute(
                insert(table_label).values(
                    topic_run_id=topic_run_id,
                    discovery_topic_id=label.discovery_topic_id,
                    **label_values,
                )
            )
    return len(new_rows)
