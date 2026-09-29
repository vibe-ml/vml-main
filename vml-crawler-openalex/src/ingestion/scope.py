"""Immutable, evaluated collection policy shared by source adapters."""

import hashlib
import re
from datetime import date
from typing import Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationInfo,
    field_validator,
    model_validator,
)

from src.ingestion.taxonomy import canonical


class CollectionScope(BaseModel):
    """Freeze inclusive publication bounds for a collection scan."""

    model_config = ConfigDict(frozen=True)
    corpus: Literal["core"] = "core"
    publication_from: date = date(2026, 1, 1)
    publication_through: date
    domain_ids: tuple[str, ...] = ()
    field_ids: tuple[str, ...] = ()
    exclude_domain_ids: tuple[str, ...] = ()
    exclude_field_ids: tuple[str, ...] = ()
    matching: Literal["primary-topic-or-within-and-between-v1"] = (
        "primary-topic-or-within-and-between-v1"
    )

    @field_validator(
        "domain_ids",
        "field_ids",
        "exclude_domain_ids",
        "exclude_field_ids",
        mode="before",
    )
    @classmethod
    def normalized_ids(cls, value: object, info: ValidationInfo) -> tuple[str, ...]:
        """Accept numeric or typed OpenAlex IDs, rejecting filter injection."""
        if value is None:
            return ()
        if isinstance(value, str):
            value = [v.strip() for v in value.split(",") if v.strip()]
        if not isinstance(value, (list, tuple, set)):
            raise ValueError("Classification IDs must be a list")  # noqa: TRY004 - Pydantic validation error
        kind = "domains" if "domain" in str(info.field_name) else "fields"
        normalized = set()
        for item in value:
            if not isinstance(item, str):
                raise ValueError("Classification IDs must be strings")  # noqa: TRY004 - Pydantic validation error
            identifier = item.removeprefix(f"https://openalex.org/{kind}/")
            if not re.fullmatch(r"[1-9][0-9]*", identifier):
                raise ValueError("Invalid classification ID")
            normalized.add(identifier)
        return tuple(sorted(normalized))

    @model_validator(mode="after")
    def ordered_bounds(self) -> Self:
        """Reject empty publication intervals."""
        if self.publication_from > self.publication_through:
            raise ValueError("Publication bounds are reversed")
        return self

    @property
    def identity(self) -> str:
        """Identify the complete evaluated policy, independent of run metadata."""
        return hashlib.sha256(canonical(self.model_dump(mode="json"))).hexdigest()

    def disposition(self, record: dict) -> str:
        """Select valid core evidence and preserve invalid rows in quarantine."""
        value = record.get("publication_date")
        try:
            if not isinstance(value, str) or len(value) != 10:
                raise ValueError("Missing date")
            published = date.fromisoformat(value)
        except ValueError:
            return "quarantine:publication_date"
        if type(record.get("is_xpac")) is not bool:
            return "quarantine:corpus_flag"
        if record["is_xpac"]:
            return "excluded:expansion"
        if not self.publication_from <= published <= self.publication_through:
            return "excluded:date"
        if not isinstance(record.get("id"), str) or not record["id"]:
            return "quarantine:identity"
        primary = record.get("primary_topic")
        for kind, allowed in (("domain", self.domain_ids), ("field", self.field_ids)):
            if not allowed:
                continue
            classification = primary.get(kind) if isinstance(primary, dict) else None
            identifier = (
                classification.get("id") if isinstance(classification, dict) else None
            )
            if (
                not isinstance(identifier, str)
                or identifier.removeprefix(f"https://openalex.org/{kind}s/")
                not in allowed
            ):
                return "excluded:classification"
        for kind, excluded in (
            ("domain", self.exclude_domain_ids),
            ("field", self.exclude_field_ids),
        ):
            if not excluded:
                continue
            classification = primary.get(kind) if isinstance(primary, dict) else None
            identifier = (
                classification.get("id") if isinstance(classification, dict) else None
            )
            if (
                isinstance(identifier, str)
                and identifier.removeprefix(f"https://openalex.org/{kind}s/")
                in excluded
            ):
                return "excluded:classification"
        return "selected"

    def api_parameters(self) -> dict[str, str]:
        """Build primary-topic filters; adapters must validate responses locally.

        Contract: https://help.openalex.org/api/filtering/ and
        https://help.openalex.org/data/works/corpus/ (checked 2026-09-23).
        """
        filters = [
            f"from_publication_date:{self.publication_from.isoformat()}",
            f"to_publication_date:{self.publication_through.isoformat()}",
        ]
        for kind, identifiers in (
            ("domain", self.domain_ids),
            ("field", self.field_ids),
        ):
            if identifiers:
                filters.append(f"primary_topic.{kind}.id:" + "|".join(identifiers))
        for kind, identifiers in (
            ("domain", self.exclude_domain_ids),
            ("field", self.exclude_field_ids),
        ):
            for identifier in identifiers:
                filters.append(f"primary_topic.{kind}.id:!{identifier}")
        return {"corpus": self.corpus, "filter": ",".join(filters)}
