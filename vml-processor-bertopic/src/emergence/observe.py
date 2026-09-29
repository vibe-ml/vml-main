"""Score publication share for discovery topics in a completed topic run."""

from __future__ import annotations

import calendar
import hashlib
import math
import uuid
from collections import defaultdict
from datetime import date
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from sqlalchemy import Engine, insert, select

from src.common.log import get_logger
from src.corpus.models import Corpus
from src.embeddings.embed import (
    _embed_allowing_failures,
    _ensure_collection,
    _retrieve_existing,
    _upsert_batched,
    cache_point_id,
)
from src.embeddings.models import EmbeddingClient, EmbeddingConfig, EmbeddingError
from src.emergence.models import EmergenceError, EmergenceResult
from src.models.emergence import EmergenceObservation
from src.models.labels import TopicLabel
from src.models.topics import DiscoveryTopic, TopicRun, WorkTopicAssignment

METRIC_VERSION = "wisdom_tem_v1"
COVERAGE_INSUFFICIENT = "insufficient_historical_coverage"
COVERAGE_NEWLY_OBSERVED = "newly_observed"
COVERAGE_SCORED = "scored"
HEADLINE_INPUT_FORMAT_VERSION = "headline_v1"

log = get_logger("emergence")


def observe_emergence(
    topic_run_id: uuid.UUID,
    corpus: Corpus,
    engine: Engine,
    *,
    embedding_config: EmbeddingConfig | None = None,
    client: EmbeddingClient | None = None,
    qdrant: QdrantClient | None = None,
) -> EmergenceResult:
    """Persist emergence observations for every discovery topic in a run.

    When ``embedding_config``, ``client``, and ``qdrant`` are all provided,
    also embed succeeded headlines into the configured headline collection.
    Observation rows commit before embedding so an embed failure cannot roll
    back scores. A second pass keeps existing rows and still embeds cache misses.

    Args:
        topic_run_id: Succeeded topic run to score.
        corpus: Frozen corpus supplying publication dates by work ID.
        engine: SQLAlchemy engine writing processor ``pwf_`` tables.
        embedding_config: Optional headline embedding collection and vector size.
        client: Optional embedding client for headlines.
        qdrant: Optional Qdrant client for the headline collection.

    Returns:
        Counts of observations persisted and headline embedding outcomes.

    Raises:
        EmergenceError: The topic run is missing or not succeeded, an
            assignment's work is absent from the corpus, or every succeeded
            headline that should be embedded failed with none already stored.
    """
    works_by_id = {work.work_id: work for work in corpus.works}

    with engine.begin() as connection:
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
            raise EmergenceError(f"topic run {topic_run_id} not found")
        if run["status"] != "succeeded":
            raise EmergenceError(
                f"topic run {topic_run_id} has status {run['status']!r}; expected succeeded"
            )

        existing = connection.execute(
            select(EmergenceObservation.__table__.c.discovery_topic_id).where(
                EmergenceObservation.__table__.c.topic_run_id == topic_run_id
            )
        ).all()
        if existing:
            observation_count = len(existing)
        else:
            topics = (
                connection.execute(
                    select(DiscoveryTopic.__table__).where(
                        DiscoveryTopic.__table__.c.topic_run_id == topic_run_id
                    )
                )
                .mappings()
                .all()
            )
            assignments = (
                connection.execute(
                    select(WorkTopicAssignment.__table__).where(
                        WorkTopicAssignment.__table__.c.topic_run_id == topic_run_id
                    )
                )
                .mappings()
                .all()
            )

            for assignment in assignments:
                if assignment["work_id"] not in works_by_id:
                    raise EmergenceError(
                        f"assignment work {assignment['work_id']} missing from corpus"
                    )

            published_from = run["published_from"]
            published_to = run["published_to"]
            months = _months_in_interval(published_from, published_to)
            complete_months = {
                year_month
                for year_month in months
                if _is_complete_month(year_month, published_from, published_to)
            }
            complete_years = _complete_years_in_interval(published_from, published_to)
            corpus_count_by_month = _count_by_month(
                [work.publication_date for work in corpus.works]
            )
            corpus_count_by_year = _count_by_year(
                [work.publication_date for work in corpus.works]
            )
            # Topic member counts: only non-outlier assignments with a discovery topic.
            topic_month_counts: dict[uuid.UUID, dict[str, int]] = defaultdict(
                lambda: defaultdict(int)
            )
            topic_year_counts: dict[uuid.UUID, dict[int, int]] = defaultdict(
                lambda: defaultdict(int)
            )
            for assignment in assignments:
                if assignment["is_outlier"] or assignment["discovery_topic_id"] is None:
                    continue
                pub = works_by_id[assignment["work_id"]].publication_date
                topic_id = assignment["discovery_topic_id"]
                topic_month_counts[topic_id][_year_month(pub)] += 1
                topic_year_counts[topic_id][pub.year] += 1

            annual_ready = len(complete_years) >= 2
            draft_rows: list[dict[str, Any]] = []
            for topic in topics:
                topic_id = topic["discovery_topic_id"]
                series: list[dict[str, Any]] = []
                for year_month in months:
                    topic_count = topic_month_counts[topic_id].get(year_month, 0)
                    corpus_count = corpus_count_by_month.get(year_month, 0)
                    share = (topic_count / corpus_count) if corpus_count > 0 else None
                    series.append(
                        {
                            "year_month": year_month,
                            "topic_count": topic_count,
                            "corpus_count": corpus_count,
                            "share": share,
                        }
                    )
                row: dict[str, Any] = {
                    "topic_run_id": topic_run_id,
                    "discovery_topic_id": topic_id,
                    "monthly_series": series,
                    "monthly_share_change": _monthly_share_change(
                        series, complete_months
                    ),
                    "complete_years": [],
                    "mean_share": None,
                    "annual_growth": None,
                    "valid_year_pair_count": None,
                    "coverage_status": COVERAGE_INSUFFICIENT,
                    "emergence_quadrant": None,
                    "metric_version": METRIC_VERSION,
                }
                if annual_ready:
                    yearly_shares = _yearly_shares(
                        complete_years,
                        topic_year_counts[topic_id],
                        corpus_count_by_year,
                    )
                    mean_share = _mean_of_defined(yearly_shares)
                    annual_growth, pair_count = _annual_growth(
                        complete_years, yearly_shares
                    )
                    row["complete_years"] = list(complete_years)
                    row["mean_share"] = mean_share
                    row["annual_growth"] = annual_growth
                    row["valid_year_pair_count"] = pair_count
                    if pair_count == 0:
                        row["annual_growth"] = -1.0
                        row["coverage_status"] = COVERAGE_NEWLY_OBSERVED
                        row["emergence_quadrant"] = None
                    else:
                        row["coverage_status"] = COVERAGE_SCORED
                draft_rows.append(row)

            if annual_ready:
                _assign_emergence_quadrants(draft_rows)

            rows = draft_rows
            if rows:
                connection.execute(insert(EmergenceObservation.__table__), rows)
            observation_count = len(rows)

    headlines_processed = 0
    headlines_cached = 0
    headlines_saved = 0
    headlines_failed = 0
    if embedding_config is not None and client is not None and qdrant is not None:
        (
            headlines_processed,
            headlines_cached,
            headlines_saved,
            headlines_failed,
        ) = _embed_succeeded_headlines(
            topic_run_id,
            engine,
            embedding_config=embedding_config,
            client=client,
            qdrant=qdrant,
        )

    return EmergenceResult(
        topic_run_id=topic_run_id,
        observation_count=observation_count,
        headlines_processed=headlines_processed,
        headlines_cached=headlines_cached,
        headlines_saved=headlines_saved,
        headlines_failed=headlines_failed,
    )


def _embed_succeeded_headlines(
    topic_run_id: uuid.UUID,
    engine: Engine,
    *,
    embedding_config: EmbeddingConfig,
    client: EmbeddingClient,
    qdrant: QdrantClient,
) -> tuple[int, int, int, int]:
    """Embed succeeded headlines; return processed, cached, saved, failed counts."""
    with engine.connect() as connection:
        labels = (
            connection.execute(
                select(TopicLabel.__table__).where(
                    TopicLabel.__table__.c.topic_run_id == topic_run_id
                )
            )
            .mappings()
            .all()
        )

    planned: list[dict[str, Any]] = []
    for label in labels:
        if label["status"] != "succeeded" or label["headline"] is None:
            continue
        headline = label["headline"]
        text_hash = hashlib.sha256(headline.encode()).hexdigest()
        point_id = cache_point_id(
            text_hash=text_hash,
            model=label["model"],
            model_revision=label["model_revision"],
            input_format_version=HEADLINE_INPUT_FORMAT_VERSION,
        )
        planned.append(
            {
                "point_id": point_id,
                "headline": headline,
                "discovery_topic_id": label["discovery_topic_id"],
                "topic_run_id": label["topic_run_id"],
                "labeling_model": label["model"],
                "labeling_revision": label["model_revision"],
                "prompt_version": label["prompt_version"],
            }
        )

    if not planned:
        return _headline_embed_counts(topic_run_id, 0, 0, 0, 0)

    try:
        _ensure_collection(qdrant, embedding_config)
    except EmbeddingError as error:
        raise EmergenceError(str(error)) from error

    existing = _retrieve_existing(
        qdrant, embedding_config.collection, [item["point_id"] for item in planned]
    )
    missing = [item for item in planned if item["point_id"] not in existing]
    cached = len(planned) - len(missing)
    if not missing:
        return _headline_embed_counts(topic_run_id, 0, cached, 0, 0)

    outcomes = _embed_allowing_failures(client, [item["headline"] for item in missing])
    points: list[PointStruct] = []
    failed = 0
    for item, vector in zip(missing, outcomes, strict=True):
        if vector is None:
            failed += 1
            continue
        points.append(
            PointStruct(
                id=item["point_id"],
                vector=vector,
                payload={
                    "discovery_topic_id": str(item["discovery_topic_id"]),
                    "topic_run_id": str(item["topic_run_id"]),
                    "headline": item["headline"],
                    "labeling_model": item["labeling_model"],
                    "labeling_revision": item["labeling_revision"],
                    "prompt_version": item["prompt_version"],
                },
            )
        )

    if points:
        try:
            _upsert_batched(qdrant, embedding_config.collection, points)
        except EmbeddingError as error:
            raise EmergenceError(str(error)) from error

    saved = len(points)
    processed = len(missing)
    if failed == len(missing) and not existing:
        _headline_embed_counts(topic_run_id, processed, cached, saved, failed)
        raise EmergenceError(
            "no usable embedding endpoint remains after headline failures"
        )
    return _headline_embed_counts(topic_run_id, processed, cached, saved, failed)


def _headline_embed_counts(
    topic_run_id: uuid.UUID,
    processed: int,
    cached: int,
    saved: int,
    failed: int,
) -> tuple[int, int, int, int]:
    """Log headline embed counts and return them."""
    log.info(
        "headlines_embedded",
        topic_run_id=str(topic_run_id),
        headlines_processed=processed,
        headlines_cached=cached,
        headlines_saved=saved,
        headlines_failed=failed,
    )
    return processed, cached, saved, failed


def _year_month(publication_date: date) -> str:
    """Return the YYYY-MM key for a publication date."""
    return f"{publication_date.year:04d}-{publication_date.month:02d}"


def _months_in_interval(published_from: date, published_to: date) -> list[str]:
    """Return every YYYY-MM overlapping the inclusive publication interval."""
    months: list[str] = []
    year, month = published_from.year, published_from.month
    end_year, end_month = published_to.year, published_to.month
    while (year, month) <= (end_year, end_month):
        months.append(_year_month(date(year, month, 1)))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return months


def _parse_year_month(year_month: str) -> tuple[int, int]:
    """Return the calendar year and month from a YYYY-MM key."""
    year_text, month_text = year_month.split("-")
    return int(year_text), int(month_text)


def _is_complete_month(
    year_month: str, published_from: date, published_to: date
) -> bool:
    """Return True when the run interval covers the month's first through last day."""
    year, month = _parse_year_month(year_month)
    first = date(year, month, 1)
    last = date(year, month, calendar.monthrange(year, month)[1])
    return published_from <= first and published_to >= last


def _next_year_month(year_month: str) -> str:
    """Return the calendar-next YYYY-MM after ``year_month``."""
    year, month = _parse_year_month(year_month)
    if month == 12:
        return _year_month(date(year + 1, 1, 1))
    return _year_month(date(year, month + 1, 1))


def _monthly_share_change(
    series: list[dict[str, Any]],
    complete_months: set[str],
) -> float | None:
    """Geometric mean of adjacent complete-month positive share ratios, minus one.

    Consecutive means calendar-adjacent complete months. Pairs that touch a zero
    or undefined share are skipped. Returns None when no valid pair remains.
    """
    share_by_month = {row["year_month"]: row["share"] for row in series}
    log_ratios: list[float] = []
    for year_month in sorted(complete_months):
        next_month = _next_year_month(year_month)
        if next_month not in complete_months:
            continue
        current = share_by_month.get(year_month)
        following = share_by_month.get(next_month)
        if current is None or following is None or current <= 0 or following <= 0:
            continue
        log_ratios.append(math.log(following / current))
    if not log_ratios:
        return None
    return math.exp(sum(log_ratios) / len(log_ratios)) - 1


def _count_by_month(dates: list[date]) -> dict[str, int]:
    """Count how many dates fall in each YYYY-MM."""
    counts: dict[str, int] = defaultdict(int)
    for pub in dates:
        counts[_year_month(pub)] += 1
    return counts


def _count_by_year(dates: list[date]) -> dict[int, int]:
    """Count how many dates fall in each calendar year."""
    counts: dict[int, int] = defaultdict(int)
    for pub in dates:
        counts[pub.year] += 1
    return counts


def _is_complete_year(year: int, published_from: date, published_to: date) -> bool:
    """Return True when the run interval covers 1 January through 31 December."""
    return published_from <= date(year, 1, 1) and published_to >= date(year, 12, 31)


def _complete_years_in_interval(published_from: date, published_to: date) -> list[int]:
    """Return complete calendar years in the inclusive interval, ascending."""
    years: list[int] = []
    for year in range(published_from.year, published_to.year + 1):
        if _is_complete_year(year, published_from, published_to):
            years.append(year)
    return years


def _yearly_shares(
    complete_years: list[int],
    topic_year_counts: dict[int, int],
    corpus_count_by_year: dict[int, int],
) -> dict[int, float | None]:
    """Return per-year shares for complete years; None when corpus count is 0."""
    shares: dict[int, float | None] = {}
    for year in complete_years:
        corpus_count = corpus_count_by_year.get(year, 0)
        if corpus_count == 0:
            shares[year] = None
        else:
            shares[year] = topic_year_counts.get(year, 0) / corpus_count
    return shares


def _mean_of_defined(yearly_shares: dict[int, float | None]) -> float | None:
    """Arithmetic mean of defined yearly shares; None when none are defined."""
    values = [share for share in yearly_shares.values() if share is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _annual_growth(
    complete_years: list[int],
    yearly_shares: dict[int, float | None],
) -> tuple[float | None, int]:
    """Geometric mean of adjacent complete-year positive share ratios, minus one.

    Returns (growth, valid_pair_count). Growth is None when the pair set is empty;
    the caller stores -1 for newly observed topics.
    """
    year_set = set(complete_years)
    log_ratios: list[float] = []
    for year in complete_years:
        next_year = year + 1
        if next_year not in year_set:
            continue
        current = yearly_shares.get(year)
        following = yearly_shares.get(next_year)
        if current is None or following is None or current <= 0 or following <= 0:
            continue
        log_ratios.append(math.log(following / current))
    if not log_ratios:
        return None, 0
    return math.exp(sum(log_ratios) / len(log_ratios)) - 1, len(log_ratios)


def _ordinary_median(values: list[float]) -> float:
    """Ordinary statistical median of a non-empty value list."""
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _emergence_quadrant(mean_share: float, annual_growth: float, median: float) -> str:
    """Map mean share and annual growth onto a WISDOM emergence quadrant name."""
    high_share = mean_share >= median
    positive_growth = annual_growth > 0
    if not high_share and positive_growth:
        return "wisdom_weak"
    if high_share and positive_growth:
        return "wisdom_strong"
    if not high_share and not positive_growth:
        return "latent"
    return "nswk"


def _assign_emergence_quadrants(rows: list[dict[str, Any]]) -> None:
    """Set emergence_quadrant on scored rows from the median of scored mean shares.

    The cut is the ordinary median over scored discovery topics only. Newly
    observed topics and topic -1 do not move it. Only scored rows receive a
    quadrant.
    """
    means = [
        row["mean_share"]
        for row in rows
        if row["coverage_status"] == COVERAGE_SCORED and row["mean_share"] is not None
    ]
    if not means:
        return
    median = _ordinary_median(means)
    for row in rows:
        if row["coverage_status"] != COVERAGE_SCORED:
            continue
        mean_share = row["mean_share"]
        annual_growth = row["annual_growth"]
        if mean_share is None or annual_growth is None:
            continue
        row["emergence_quadrant"] = _emergence_quadrant(
            mean_share, annual_growth, median
        )
