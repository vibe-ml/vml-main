"""Corpus selection configuration, results, and errors."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator

SOCIAL_SCIENCES_DOMAIN_ID = "https://openalex.org/domains/2"


class CorpusConfig(BaseModel):
    """Filters and interval that define a corpus request.

    Attributes:
        scope_ids: Crawler collection scopes whose current works are considered.
        work_types: OpenAlex work types to keep.
        published_from: First publication date to keep, inclusive.
        published_to: Last publication date to keep, inclusive.
        excluded_domain_ids: OpenAlex primary domain IDs to drop. Works without a primary
            domain are never dropped by this rule.
    """

    model_config = ConfigDict(frozen=True)

    scope_ids: tuple[str, ...] = ()
    work_types: tuple[str, ...] = ("article", "preprint", "conference-paper")
    published_from: date = date(2024, 1, 1)
    published_to: date = date(2026, 9, 28)
    excluded_domain_ids: tuple[str, ...] = (SOCIAL_SCIENCES_DOMAIN_ID,)

    @model_validator(mode="after")
    def _check_interval(self) -> CorpusConfig:
        """Reject an interval whose start falls after its end."""
        if self.published_from > self.published_to:
            raise ValueError("published_from must not be after published_to")
        return self


class CorpusExclusionReason(StrEnum):
    """Why a considered work was left out, checked in declaration order."""

    VERSION_CONFLICT = "version_conflict"
    WORK_TYPE = "work_type"
    PUBLICATION_DATE = "publication_date"
    EXCLUDED_DOMAIN = "excluded_domain"
    MISSING_ABSTRACT = "missing_abstract"


@dataclass(frozen=True)
class CorpusWork:
    """One selected work, pinned to the immutable version it was read from."""

    work_id: str
    work_version_id: str
    title: str
    abstract: str
    publication_date: date
    primary_domain_id: str | None
    language: str | None

    @property
    def unknown_domain(self) -> bool:
        """Whether OpenAlex supplied no primary domain for this work."""
        return self.primary_domain_id is None

    @property
    def missing_title(self) -> bool:
        """Whether the work has no usable title."""
        return not self.title


@dataclass(frozen=True)
class VersionConflict:
    """A work whose current version differs between collection scopes."""

    work_id: str
    version_ids: tuple[str, ...]


@dataclass(frozen=True)
class CoverageReport:
    """Counts that account for every considered work.

    `considered` always equals `selected` plus the sum of `excluded`.
    """

    considered: int
    selected: int
    excluded: Mapping[CorpusExclusionReason, int]
    unknown_domain: int
    missing_title: int
    conflicts: tuple[VersionConflict, ...]

    def __getstate__(self) -> dict[str, Any]:
        """Pickle ``excluded`` as a plain dict. ``mappingproxy`` is not picklable."""
        return {
            "considered": self.considered,
            "selected": self.selected,
            "excluded": dict(self.excluded),
            "unknown_domain": self.unknown_domain,
            "missing_title": self.missing_title,
            "conflicts": self.conflicts,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore a pickled report, wrapping ``excluded`` again."""
        object.__setattr__(self, "considered", state["considered"])
        object.__setattr__(self, "selected", state["selected"])
        object.__setattr__(self, "excluded", MappingProxyType(state["excluded"]))
        object.__setattr__(self, "unknown_domain", state["unknown_domain"])
        object.__setattr__(self, "missing_title", state["missing_title"])
        object.__setattr__(self, "conflicts", state["conflicts"])


@dataclass(frozen=True)
class Corpus:
    """A frozen corpus manifest ordered by work ID, with its coverage report."""

    config: CorpusConfig
    works: tuple[CorpusWork, ...]
    coverage: CoverageReport


class CorpusSelectionError(RuntimeError):
    """Corpus selection could not produce a usable corpus."""


class EmptyCorpusError(CorpusSelectionError):
    """The request matched no works; the coverage report explains why."""

    def __init__(self, coverage: CoverageReport) -> None:
        super().__init__(
            f"corpus request selected no works out of {coverage.considered} considered: "
            f"{dict(coverage.excluded)}"
        )
        self.coverage = coverage
