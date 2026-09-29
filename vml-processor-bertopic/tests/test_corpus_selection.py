"""Corpus selection behaviour, exercised through `select_corpus` against real PostgreSQL."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from src.common.db import create_processor_engine
from src.corpus import (
    CorpusConfig,
    CorpusExclusionReason,
    CorpusSelectionError,
    EmptyCorpusError,
    VersionConflict,
    select_corpus,
)
from src.corpus import select as select_module
from tests.conftest import TestSettings, UpstreamFixture

SCOPE = "scope-2026"
PHYSICAL_SCIENCES = "https://openalex.org/domains/3"
SOCIAL_SCIENCES = "https://openalex.org/domains/2"


def openalex_work(number: int, **overrides: Any) -> dict[str, Any]:
    """Return an OpenAlex work payload that passes the default filters."""
    payload: dict[str, Any] = {
        "id": f"https://openalex.org/W{number}",
        "title": f"Work {number}",
        "display_name": f"Work {number}",
        "publication_date": "2026-01-15",
        "language": "en",
        "type": "article",
        "primary_topic": {
            "id": "https://openalex.org/T10028",
            "domain": {"id": PHYSICAL_SCIENCES, "display_name": "Physical Sciences"},
        },
        "abstract_inverted_index": {"Diffusion": [0], "models": [1], "scale.": [2]},
    }
    payload.update(overrides)
    return payload


def config(**overrides: Any) -> CorpusConfig:
    """Return a corpus request for the default test scope."""
    return CorpusConfig(scope_ids=(SCOPE,), **overrides)


def test_eligible_work_is_selected_with_its_version_and_text(
    upstream: UpstreamFixture,
) -> None:
    version_id = upstream.add_work(SCOPE, openalex_work(1, language="de"))

    corpus = select_corpus(config(), upstream.engine)

    assert len(corpus.works) == 1
    work = corpus.works[0]
    assert work.work_id == "https://openalex.org/W1"
    assert work.work_version_id == version_id
    assert work.title == "Work 1"
    assert work.abstract == "Diffusion models scale."
    assert work.publication_date == date(2026, 1, 15)
    assert work.primary_domain_id == PHYSICAL_SCIENCES
    assert work.language == "de"
    assert corpus.coverage.considered == 1
    assert corpus.coverage.selected == 1


def test_abstract_reconstructs_from_object_and_json_string_by_position(
    upstream: UpstreamFixture,
) -> None:
    index = {"the": [0, 3], "rise": [4], "of": [2], "Beyond": [1], "transformers.": [5]}
    object_payload = openalex_work(1, abstract_inverted_index=index)
    string_payload = openalex_work(
        2, abstract_inverted_index='{"b": [1], "a": [0], "c": [2]}'
    )
    object_version = upstream.add_work(SCOPE, object_payload)
    string_version = upstream.add_work(SCOPE, string_payload)

    corpus = select_corpus(config(), upstream.engine)

    assert [work.abstract for work in corpus.works] == [
        "the Beyond of the rise transformers.",
        "a b c",
    ]
    assert upstream.payload_of(object_version) == object_payload
    assert upstream.payload_of(string_version) == string_payload


def test_work_without_abstract_is_excluded_and_counted(
    upstream: UpstreamFixture,
) -> None:
    upstream.add_work(SCOPE, openalex_work(1))
    upstream.add_work(SCOPE, openalex_work(2, abstract_inverted_index=None))
    upstream.add_work(SCOPE, openalex_work(3, abstract_inverted_index={}))
    upstream.add_work(SCOPE, openalex_work(4, abstract_inverted_index="{}"))
    upstream.add_work(SCOPE, openalex_work(5, abstract_inverted_index="not json"))
    without_key = openalex_work(6)
    del without_key["abstract_inverted_index"]
    upstream.add_work(SCOPE, without_key)
    upstream.add_work(SCOPE, openalex_work(7, abstract_inverted_index={"word": 3}))
    upstream.add_work(SCOPE, openalex_work(8, abstract_inverted_index={"word": ["x"]}))

    corpus = select_corpus(config(), upstream.engine)

    assert [work.work_id for work in corpus.works] == ["https://openalex.org/W1"]
    assert corpus.coverage.considered == 8
    assert corpus.coverage.selected == 1
    assert corpus.coverage.excluded == {CorpusExclusionReason.MISSING_ABSTRACT: 7}


def test_date_excluded_work_does_not_load_its_abstract(
    upstream: UpstreamFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    upstream.add_work(SCOPE, openalex_work(1))
    upstream.add_work(
        SCOPE,
        openalex_work(
            2,
            publication_date="2020-01-01",
            abstract_inverted_index={"Secret": [0]},
        ),
    )
    seen: list[Any] = []
    real = select_module._reconstruct_abstract

    def spy(index: Any) -> str | None:
        seen.append(index)
        return real(index)

    monkeypatch.setattr(select_module, "_reconstruct_abstract", spy)

    corpus = select_corpus(config(), upstream.engine)

    assert [work.work_id for work in corpus.works] == ["https://openalex.org/W1"]
    assert corpus.coverage.excluded == {CorpusExclusionReason.PUBLICATION_DATE: 1}
    assert seen == [{"Diffusion": [0], "models": [1], "scale.": [2]}]


def test_default_publication_interval_matches_confirmed_coverage() -> None:
    """Default dates follow the interval confirmed against demo_vml coverage."""
    request = CorpusConfig()

    assert request.published_from == date(2024, 1, 1)
    assert request.published_to == date(2026, 9, 28)


def test_default_filters_keep_agreed_types_interval_and_domains(
    upstream: UpstreamFixture,
) -> None:
    upstream.add_work(SCOPE, openalex_work(1, type="article"))
    upstream.add_work(SCOPE, openalex_work(2, type="preprint"))
    upstream.add_work(SCOPE, openalex_work(3, type="conference-paper"))
    upstream.add_work(SCOPE, openalex_work(4, type="book-chapter"))
    upstream.add_work(SCOPE, openalex_work(5, type=None))
    upstream.add_work(SCOPE, openalex_work(6, publication_date="2024-01-01"))
    upstream.add_work(SCOPE, openalex_work(7, publication_date="2026-09-28"))
    upstream.add_work(SCOPE, openalex_work(8, publication_date="2023-12-31"))
    upstream.add_work(SCOPE, openalex_work(9, publication_date="2026-09-29"))
    upstream.add_work(SCOPE, openalex_work(10, publication_date=None))
    social = {"id": "https://openalex.org/T1", "domain": {"id": SOCIAL_SCIENCES}}
    upstream.add_work(SCOPE, openalex_work(11, primary_topic=social))

    corpus = select_corpus(CorpusConfig(scope_ids=(SCOPE,)), upstream.engine)

    assert [work.work_id for work in corpus.works] == [
        f"https://openalex.org/W{number}" for number in (1, 2, 3, 6, 7)
    ]
    assert corpus.coverage.excluded == {
        CorpusExclusionReason.WORK_TYPE: 2,
        CorpusExclusionReason.PUBLICATION_DATE: 3,
        CorpusExclusionReason.EXCLUDED_DOMAIN: 1,
    }


def test_filters_come_from_the_corpus_request(upstream: UpstreamFixture) -> None:
    health = {
        "id": "https://openalex.org/T2",
        "domain": {"id": "https://openalex.org/domains/4"},
    }
    upstream.add_work(
        SCOPE,
        openalex_work(
            1, type="book-chapter", publication_date="2024-06-01", primary_topic=health
        ),
    )
    social = {"id": "https://openalex.org/T1", "domain": {"id": SOCIAL_SCIENCES}}
    upstream.add_work(SCOPE, openalex_work(2, primary_topic=social))
    upstream.add_work(SCOPE, openalex_work(3))

    corpus = select_corpus(
        config(
            work_types=("book-chapter", "article"),
            published_from=date(2024, 1, 1),
            published_to=date(2026, 12, 31),
            excluded_domain_ids=(PHYSICAL_SCIENCES,),
        ),
        upstream.engine,
    )

    assert [work.work_id for work in corpus.works] == [
        "https://openalex.org/W1",
        "https://openalex.org/W2",
    ]
    assert corpus.coverage.excluded == {CorpusExclusionReason.EXCLUDED_DOMAIN: 1}


def test_unknown_domain_is_retained_marked_and_counted(
    upstream: UpstreamFixture,
) -> None:
    upstream.add_work(SCOPE, openalex_work(1, primary_topic=None))
    upstream.add_work(
        SCOPE, openalex_work(2, primary_topic={"id": "https://openalex.org/T1"})
    )
    upstream.add_work(SCOPE, openalex_work(3))

    corpus = select_corpus(config(), upstream.engine)

    assert [work.unknown_domain for work in corpus.works] == [True, True, False]
    assert corpus.coverage.selected == 3
    assert corpus.coverage.unknown_domain == 2
    assert corpus.coverage.excluded == {}


def test_empty_title_is_retained_marked_and_counted(upstream: UpstreamFixture) -> None:
    upstream.add_work(SCOPE, openalex_work(1, title=None))
    upstream.add_work(SCOPE, openalex_work(2, title="  "))
    upstream.add_work(SCOPE, openalex_work(3))

    corpus = select_corpus(config(), upstream.engine)

    assert [work.missing_title for work in corpus.works] == [True, True, False]
    assert [work.title for work in corpus.works] == ["", "", "Work 3"]
    assert corpus.coverage.selected == 3
    assert corpus.coverage.missing_title == 2


def test_work_in_overlapping_scopes_contributes_one_entry(
    upstream: UpstreamFixture,
) -> None:
    shared_version = upstream.add_work(SCOPE, openalex_work(1))
    upstream.set_current("scope-ai", "https://openalex.org/W1", shared_version)
    upstream.add_work("scope-ai", openalex_work(2))
    upstream.add_work("scope-unrequested", openalex_work(3))

    corpus = select_corpus(CorpusConfig(scope_ids=(SCOPE, "scope-ai")), upstream.engine)

    assert corpus.works[0].work_version_id == shared_version
    assert [work.work_id for work in corpus.works] == [
        "https://openalex.org/W1",
        "https://openalex.org/W2",
    ]
    assert corpus.coverage.considered == 2


def test_conflicting_current_versions_are_reported_not_resolved(
    upstream: UpstreamFixture,
) -> None:
    first = upstream.add_work(SCOPE, openalex_work(1, title="First wording"))
    second = upstream.add_version(
        "https://openalex.org/W1", openalex_work(1, title="Second wording")
    )
    upstream.set_current("scope-ai", "https://openalex.org/W1", second)
    upstream.add_work(SCOPE, openalex_work(2))

    corpus = select_corpus(CorpusConfig(scope_ids=(SCOPE, "scope-ai")), upstream.engine)

    assert [work.work_id for work in corpus.works] == ["https://openalex.org/W2"]
    assert corpus.coverage.conflicts == (
        VersionConflict(
            work_id="https://openalex.org/W1",
            version_ids=tuple(sorted((first, second))),
        ),
    )
    assert corpus.coverage.excluded == {CorpusExclusionReason.VERSION_CONFLICT: 1}
    assert corpus.coverage.considered == 2


def test_repeated_request_returns_identical_corpus_ordered_by_work_id(
    upstream: UpstreamFixture,
) -> None:
    for number in (30, 4, 100, 2, 1000):
        upstream.add_work(SCOPE, openalex_work(number))
    upstream.add_work("scope-ai", openalex_work(7))
    upstream.add_work(SCOPE, openalex_work(5, type="dataset"))
    upstream.add_work(SCOPE, openalex_work(6, abstract_inverted_index=None))
    request = CorpusConfig(scope_ids=("scope-ai", SCOPE))

    first = select_corpus(request, upstream.engine)
    second = select_corpus(request, upstream.engine)

    assert first == second
    assert [work.work_id for work in first.works] == [
        "https://openalex.org/W100",
        "https://openalex.org/W1000",
        "https://openalex.org/W2",
        "https://openalex.org/W30",
        "https://openalex.org/W4",
        "https://openalex.org/W7",
    ]
    coverage = first.coverage
    assert coverage.considered == coverage.selected + sum(coverage.excluded.values())
    assert coverage.selected == len(first.works) == 6


def test_request_matching_zero_works_fails_with_its_coverage(
    upstream: UpstreamFixture,
) -> None:
    upstream.add_work(SCOPE, openalex_work(1, type="dataset"))

    with pytest.raises(EmptyCorpusError) as raised:
        select_corpus(config(), upstream.engine)

    assert raised.value.coverage.considered == 1
    assert raised.value.coverage.excluded == {CorpusExclusionReason.WORK_TYPE: 1}


def test_request_without_scopes_fails(upstream: UpstreamFixture) -> None:
    upstream.add_work(SCOPE, openalex_work(1))

    with pytest.raises(CorpusSelectionError, match="scope"):
        select_corpus(CorpusConfig(), upstream.engine)


def test_missing_upstream_tables_fail_loudly(test_settings: TestSettings) -> None:
    engine = create_processor_engine(
        test_settings.database_url, upstream_schema="raw_missing_schema"
    )

    with pytest.raises(CorpusSelectionError, match="upstream"):
        select_corpus(config(), engine)


def test_unreachable_database_fails_loudly() -> None:
    engine = create_processor_engine("postgresql+psycopg://nobody@127.0.0.1:1/none")

    with pytest.raises(CorpusSelectionError, match="upstream"):
        select_corpus(config(), engine)
