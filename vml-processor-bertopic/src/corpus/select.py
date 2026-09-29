"""Select a frozen corpus from the crawler's current work versions."""

import json
import time
from collections import Counter
from collections.abc import Iterable, Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from datetime import date
from itertools import groupby
from operator import itemgetter
from types import MappingProxyType
from typing import Any

from sqlalchemy import Engine, RowMapping, Select, and_, func, or_, select
from sqlalchemy.exc import DBAPIError

from src.common.log import get_logger
from src.corpus.models import (
    Corpus,
    CorpusConfig,
    CorpusExclusionReason,
    CorpusSelectionError,
    CorpusWork,
    CoverageReport,
    EmptyCorpusError,
    VersionConflict,
)
from src.models.upstream import work_current, work_versions

log = get_logger("corpus")

_FETCH_SIZE = 2000


@dataclass(frozen=True)
class _KeptWork:
    """A work that passed every filter except the abstract check."""

    work_id: str
    version_id: str
    title: str
    publication_date: date
    domain_id: str | None
    language: str | None


def select_corpus(config: CorpusConfig, engine: Engine) -> Corpus:
    """Select the works matching `config` and freeze them as an ordered corpus.

    Reads the crawler's current work versions for the requested scopes in a read-only
    transaction and never writes upstream tables.

    Args:
        config: Scopes, filters, and publication interval of the request.
        engine: Engine that resolves the crawler's `raw` tables.

    Returns:
        The selected works ordered by work ID, with a coverage report accounting for every
        considered work.

    Raises:
        CorpusSelectionError: No scopes were requested, or the upstream tables could not be
            read.
        EmptyCorpusError: The request selected no works.
    """
    if not config.scope_ids:
        raise CorpusSelectionError("corpus request names no collection scopes")
    started = time.monotonic()
    try:
        with engine.connect() as connection:
            connection = connection.execution_options(
                postgresql_readonly=True, yield_per=_FETCH_SIZE
            )
            # Thin pass over work_current only: considered count and version conflicts.
            current = connection.execute(_current_works_query(config))
            try:
                considered, conflicts, conflict_work_ids = _collect_conflicts(
                    current.mappings()
                )
            finally:
                current.close()

            type_excluded, date_excluded = _type_date_exclusion_counts(
                connection, config
            )
            excluded: Counter[CorpusExclusionReason] = Counter()
            if conflicts:
                excluded[CorpusExclusionReason.VERSION_CONFLICT] = len(conflicts)
            if type_excluded:
                excluded[CorpusExclusionReason.WORK_TYPE] = type_excluded
            if date_excluded:
                excluded[CorpusExclusionReason.PUBLICATION_DATE] = date_excluded

            # Survivor stream: type and date predicates stay in SQL so expression indexes
            # can prune versions before title/language/domain TOAST is opened.
            result = connection.execute(
                _current_versions_query(config, conflict_work_ids)
            )
            try:
                pending, more_excluded = _partition(result.mappings(), config)
            finally:
                result.close()
            excluded.update(more_excluded)

            abstracts = _abstracts_for(
                connection, [item.version_id for item in pending]
            )
            works, coverage = _finish_coverage(
                pending, abstracts, excluded, conflicts, considered
            )
    except DBAPIError as error:
        raise CorpusSelectionError(
            f"upstream crawler tables could not be read: {error.orig}"
        ) from error

    if coverage.conflicts:
        log.warning(
            "corpus_version_conflicts",
            count=len(coverage.conflicts),
            work_ids=[conflict.work_id for conflict in coverage.conflicts],
        )
    log.info(
        "corpus_selected",
        scope_ids=list(config.scope_ids),
        considered=coverage.considered,
        selected=coverage.selected,
        excluded={str(reason): count for reason, count in coverage.excluded.items()},
        unknown_domain=coverage.unknown_domain,
        missing_title=coverage.missing_title,
        elapsed_seconds=round(time.monotonic() - started, 3),
    )
    if not works:
        raise EmptyCorpusError(coverage)
    return Corpus(config=config, works=works, coverage=coverage)


def _current_works_query(config: CorpusConfig) -> Select:
    """Build the thin query over scoped current rows (no version payloads)."""
    return (
        select(
            work_current.c.entity_id.label("work_id"),
            work_current.c.version_id,
        )
        .where(work_current.c.scope_id.in_(config.scope_ids))
        .order_by(work_current.c.entity_id.collate("C"), work_current.c.version_id)
    )


def _collect_conflicts(
    rows: Iterable[RowMapping],
) -> tuple[int, list[VersionConflict], frozenset[str]]:
    """Count considered works and collect multi-version conflicts from current rows."""
    considered = 0
    conflicts: list[VersionConflict] = []
    conflict_work_ids: set[str] = set()
    for work_id, group in groupby(rows, key=itemgetter("work_id")):
        considered += 1
        version_ids = tuple(sorted({row["version_id"] for row in group}))
        if len(version_ids) > 1:
            conflicts.append(VersionConflict(work_id=work_id, version_ids=version_ids))
            conflict_work_ids.add(work_id)
    return considered, conflicts, frozenset(conflict_work_ids)


def _unique_nonconflict_current(config: CorpusConfig) -> Any:
    """Subquery of one version per work that has a single current version across scopes."""
    return (
        select(
            work_current.c.entity_id.label("work_id"),
            func.min(work_current.c.version_id).label("version_id"),
        )
        .where(work_current.c.scope_id.in_(config.scope_ids))
        .group_by(work_current.c.entity_id)
        .having(func.count(func.distinct(work_current.c.version_id)) == 1)
        .subquery()
    )


def _type_date_exclusion_counts(
    connection: Any, config: CorpusConfig
) -> tuple[int, int]:
    """Count type and text-date rejects without selecting heavy payload fields."""
    unique_current = _unique_nonconflict_current(config)
    work_type = work_versions.c.payload["type"].astext
    pub_date = work_versions.c.payload["publication_date"].astext
    from_s = config.published_from.isoformat()
    to_s = config.published_to.isoformat()
    type_ok = work_type.in_(config.work_types)
    type_bad = or_(work_type.is_(None), ~type_ok)
    date_bad = or_(pub_date.is_(None), pub_date < from_s, pub_date > to_s)
    row = connection.execute(
        select(
            func.count().filter(type_bad).label("work_type"),
            func.count().filter(and_(type_ok, date_bad)).label("publication_date"),
        ).select_from(
            unique_current.join(
                work_versions, work_versions.c.id == unique_current.c.version_id
            )
        )
    ).one()
    return int(row.work_type), int(row.publication_date)


def _current_versions_query(
    config: CorpusConfig, conflict_work_ids: AbstractSet[str]
) -> Select:
    """Build the survivor query for filter columns of each scope's current work version.

    Type and publication-date predicates sit in SQL so Postgres can use the expression
    indexes on those JSON paths. Omits ``abstract_inverted_index``. That field is loaded
    later, and only for works that pass the type, date, and domain filters. Conflict works
    are excluded here; they are counted from the thin ``work_current`` pass.
    """
    payload = work_versions.c.payload
    work_type = payload["type"].astext
    pub_date = payload["publication_date"].astext
    from_s = config.published_from.isoformat()
    to_s = config.published_to.isoformat()
    stmt = (
        select(
            work_current.c.entity_id.label("work_id"),
            work_current.c.version_id,
            work_type.label("type"),
            pub_date.label("publication_date"),
            payload["title"].astext.label("title"),
            payload["language"].astext.label("language"),
            payload["primary_topic"]["domain"]["id"].astext.label("domain_id"),
        )
        .join(work_versions, work_versions.c.id == work_current.c.version_id)
        .where(work_current.c.scope_id.in_(config.scope_ids))
        .where(work_type.in_(config.work_types))
        .where(pub_date.is_not(None))
        .where(pub_date >= from_s)
        .where(pub_date <= to_s)
        # Byte-order collation keeps the manifest order independent of the database locale.
        .order_by(work_current.c.entity_id.collate("C"), work_current.c.version_id)
    )
    if conflict_work_ids:
        stmt = stmt.where(work_current.c.entity_id.notin_(conflict_work_ids))
    return stmt


def _partition(
    rows: Iterable[RowMapping], config: CorpusConfig
) -> tuple[list[_KeptWork], Counter[CorpusExclusionReason]]:
    """Apply domain and residual date checks on type+date SQL survivors."""
    pending: list[_KeptWork] = []
    excluded: Counter[CorpusExclusionReason] = Counter()
    # Rows arrive ordered by work ID then version ID, so each group holds one work's
    # rows across scopes, and a single-version group is resolved without choosing.
    for work_id, group in groupby(rows, key=itemgetter("work_id")):
        scope_rows = list(group)
        version_ids = {row["version_id"] for row in scope_rows}
        if len(version_ids) > 1:
            # Conflicts are counted from the thin work_current pass and excluded in SQL.
            continue
        row = scope_rows[0]
        reason = _exclude_before_abstract(row, config)
        if reason is not None:
            excluded[reason] += 1
            continue
        published = _parse_date(row["publication_date"])
        if published is None:
            excluded[CorpusExclusionReason.PUBLICATION_DATE] += 1
            continue
        pending.append(
            _KeptWork(
                work_id=work_id,
                version_id=row["version_id"],
                title=(row["title"] or "").strip(),
                publication_date=published,
                domain_id=row["domain_id"],
                language=row["language"],
            )
        )
    return pending, excluded


def _abstracts_for(connection: Any, version_ids: list[str]) -> dict[str, Any]:
    """Load inverted abstracts for the version ids that survived the cheap filters."""
    found: dict[str, Any] = {}
    if not version_ids:
        return found
    abstract = work_versions.c.payload["abstract_inverted_index"]
    for start in range(0, len(version_ids), _FETCH_SIZE):
        chunk = version_ids[start : start + _FETCH_SIZE]
        rows = connection.execute(
            select(
                work_versions.c.id,
                abstract.label("abstract_index"),
            ).where(work_versions.c.id.in_(chunk))
        ).mappings()
        for row in rows:
            found[str(row["id"])] = row["abstract_index"]
    return found


def _finish_coverage(
    pending: list[_KeptWork],
    abstracts: Mapping[str, Any],
    excluded: Counter[CorpusExclusionReason],
    conflicts: list[VersionConflict],
    considered: int,
) -> tuple[tuple[CorpusWork, ...], CoverageReport]:
    """Drop works with no usable abstract and build the coverage report."""
    works: list[CorpusWork] = []
    for item in pending:
        abstract = _reconstruct_abstract(abstracts.get(item.version_id))
        if abstract is None:
            excluded[CorpusExclusionReason.MISSING_ABSTRACT] += 1
            continue
        works.append(
            CorpusWork(
                work_id=item.work_id,
                work_version_id=item.version_id,
                title=item.title,
                abstract=abstract,
                publication_date=item.publication_date,
                primary_domain_id=item.domain_id,
                language=item.language,
            )
        )
    coverage = CoverageReport(
        considered=considered,
        selected=len(works),
        excluded=MappingProxyType(dict(excluded)),
        unknown_domain=sum(work.unknown_domain for work in works),
        missing_title=sum(work.missing_title for work in works),
        conflicts=tuple(conflicts),
    )
    return tuple(works), coverage


def _exclude_before_abstract(
    row: RowMapping, config: CorpusConfig
) -> CorpusExclusionReason | None:
    """Return the first failed filter before the abstract check, or None to keep going."""
    if row["type"] not in config.work_types:
        return CorpusExclusionReason.WORK_TYPE
    published = _parse_date(row["publication_date"])
    if (
        published is None
        or not config.published_from <= published <= config.published_to
    ):
        return CorpusExclusionReason.PUBLICATION_DATE
    if row["domain_id"] in config.excluded_domain_ids:
        return CorpusExclusionReason.EXCLUDED_DOMAIN
    return None


def _parse_date(value: str | None) -> date | None:
    """Parse an ISO publication date, or return None when absent or malformed."""
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _reconstruct_abstract(index: Any) -> str | None:
    """Rebuild abstract text from an OpenAlex inverted index, or None when unavailable."""
    # Snapshot-sourced payloads store the inverted index as a JSON-encoded string.
    if isinstance(index, str):
        try:
            index = json.loads(index)
        except json.JSONDecodeError:
            return None
    if not isinstance(index, dict):
        return None
    positioned: list[tuple[int, str]] = []
    for word, positions in index.items():
        if not isinstance(positions, list):
            return None
        for position in positions:
            # A malformed position makes the word order, and so the whole text, unreliable.
            if not isinstance(position, int) or isinstance(position, bool):
                return None
            positioned.append((position, word))
    positioned.sort()
    return " ".join(word for _, word in positioned).strip() or None
