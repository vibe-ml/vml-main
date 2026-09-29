"""Taxonomy validation and versioned canonical identities."""

import hashlib
import json
import re
from decimal import Decimal
from typing import Any

KINDS = ("domains", "fields", "subfields", "topics")
PARENTS = {
    "fields": ("domain",),
    "subfields": ("domain", "field"),
    "topics": ("domain", "field", "subfield"),
}


def canonical(value: Any) -> bytes:
    """Encode sorted objects, normalized numbers, and ordered arrays (version 1)."""
    if isinstance(value, dict):
        return (
            b"{"
            + b",".join(
                canonical(key) + b":" + canonical(value[key]) for key in sorted(value)
            )
            + b"}"
        )
    if isinstance(value, list):
        return b"[" + b",".join(canonical(item) for item in value) + b"]"
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Non-finite JSON number")
        encoded = format(value, "f")
        if "." in encoded:
            encoded = encoded.rstrip("0").rstrip(".")
        return ("0" if value == 0 else encoded).encode()
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode()


def validate(records: dict[str, list[dict[str, Any]]]) -> None:
    """Require unique typed identities and a consistent, resolvable hierarchy."""
    lookup = {}
    for kind in KINDS:
        if not records[kind]:
            raise ValueError("Empty taxonomy endpoint")
        pattern = (
            r"https://openalex.org/T[1-9][0-9]*"
            if kind == "topics"
            else rf"https://openalex.org/{kind}/[1-9][0-9]*"
        )
        for record in records[kind]:
            identity = record.get("id")
            if (
                not isinstance(identity, str)
                or not re.fullmatch(pattern, identity)
                or identity in lookup
            ):
                raise ValueError("Invalid or duplicate typed taxonomy ID")
            if not isinstance(record.get("display_name"), str):
                raise TypeError("Missing taxonomy name")
            lookup[identity] = record
    for kind, parents in PARENTS.items():
        for record in records[kind]:
            for parent in parents:
                reference = record.get(parent)
                identity = reference.get("id") if isinstance(reference, dict) else None
                if (
                    not isinstance(identity, str)
                    or identity not in lookup
                    or not identity.startswith(f"https://openalex.org/{parent}s/")
                ):
                    raise ValueError("Unresolved taxonomy parent")
                for ancestor in PARENTS.get(parent + "s", ()):
                    if lookup[identity].get(ancestor, {}).get("id") != record.get(
                        ancestor, {}
                    ).get("id"):
                        raise ValueError("Inconsistent taxonomy ancestry")
    for rows in records.values():
        rows.sort(key=lambda record: record["id"])


def identities(records: dict) -> tuple[str, str]:
    """Hash complete records separately from classification inputs."""
    projection = {
        kind: [
            {
                key: record[key]
                for key in (
                    "id",
                    "display_name",
                    "description",
                    "keywords",
                    "display_name_alternatives",
                )
                if key in record
            }
            | {parent: record[parent]["id"] for parent in PARENTS.get(kind, ())}
            for record in rows
        ]
        for kind, rows in records.items()
    }
    return (
        hashlib.sha256(b"taxonomy-v1\n" + canonical(records)).hexdigest(),
        hashlib.sha256(b"taxonomy-v1\n" + canonical(projection)).hexdigest(),
    )
