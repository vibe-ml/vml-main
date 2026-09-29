"""Labels stage behaviour, exercised through `label_topics` against `pwf_test`."""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Sequence
from datetime import date
from types import MappingProxyType
from unittest.mock import patch

import numpy as np
from sqlalchemy import Engine, insert, select
from sqlalchemy.exc import IntegrityError, OperationalError
from structlog.testing import capture_logs

from src.corpus import Corpus, CorpusConfig, CorpusWork, CoverageReport
from src.labels import (
    GenerationResult,
    LabelConfig,
    LabelingError,
    label_topics,
)
from src.labels import label as label_mod
from src.models.labels import TopicLabel, WorkSummary
from src.models.topics import DiscoveryTopic
from src.topics import TopicConfig, TopicRunResult, fit_topics
from src.topics.fitter import FitResult

PHYSICAL_SCIENCES = "https://openalex.org/domains/3"
PROMPT_VERSION = "label_prompts_v1"
FIXED_HEADLINE = "HEADLINE(EN): Research theme overview"


class WordCountTokenizer:
    """Each whitespace-separated word is one token; character length is ignored.

    Many short words cost more than one long unbroken string of equal character length.
    """

    def count_tokens(self, text: str) -> int:
        return len(text.split())


def _tokenizer() -> WordCountTokenizer:
    return WordCountTokenizer()


def _work(
    n: int,
    *,
    title: str,
    abstract: str,
    language: str = "en",
) -> CorpusWork:
    work_id = f"https://openalex.org/W{n:04d}"
    return CorpusWork(
        work_id=work_id,
        work_version_id=f"ver-{n:04d}",
        title=title,
        abstract=abstract,
        publication_date=date(2024, 6, 1),
        primary_domain_id=PHYSICAL_SCIENCES,
        language=language,
    )


def _corpus(works: Sequence[CorpusWork]) -> Corpus:
    ordered = tuple(sorted(works, key=lambda work: work.work_id))
    return Corpus(
        config=CorpusConfig(
            scope_ids=("scope-a",),
            published_from=date(2024, 1, 1),
            published_to=date(2024, 12, 31),
        ),
        works=ordered,
        coverage=CoverageReport(
            considered=len(ordered),
            selected=len(ordered),
            excluded=MappingProxyType({}),
            unknown_domain=0,
            missing_title=0,
            conflicts=(),
        ),
    )


def _topic_config() -> TopicConfig:
    return TopicConfig(
        min_cluster_size=2,
        min_samples=1,
        umap_n_neighbors=2,
        embedding_model="test-encoder",
        embedding_model_revision="test-rev",
    )


def _label_config(**overrides: object) -> LabelConfig:
    values: dict[str, object] = {
        "model": "Qwen/Qwen3.6-35B-A3B",
        "model_revision": "test-rev",
        "sample_size": 15,
        "prompt_version": PROMPT_VERSION,
        "context_window": 4096,
        "output_reserve": 64,
        "max_retries": 0,
        "retry_backoff_seconds": 0.0,
        "max_concurrency": 4,
    }
    values.update(overrides)
    return LabelConfig(**values)  # type: ignore[arg-type]


class FakeLabelingClient:
    """Deterministic in-process fake; never calls a remote host."""

    def __init__(
        self,
        *,
        fail_when_prompt_contains: str | None = None,
        fail_summaries: bool = False,
        summary_by_title: dict[str, str] | None = None,
        headline: str = FIXED_HEADLINE,
        failures_before_success: int = 0,
        summary_delay_by_title: dict[str, float] | None = None,
    ) -> None:
        self.fail_when_prompt_contains = fail_when_prompt_contains
        self.fail_summaries = fail_summaries
        self.summary_by_title = summary_by_title
        self.headline = headline
        self.failures_before_success = failures_before_success
        self.summary_delay_by_title = summary_delay_by_title or {}
        self.attempts = 0
        self.prompts: list[str] = []
        self._lock = threading.Lock()

    def generate(self, prompt: str) -> GenerationResult:
        is_summary = "Write a concise English summary" in prompt
        title = ""
        if is_summary:
            title = prompt.split("Title:", 1)[-1].split("Abstract:", 1)[0].strip()
            delay = self.summary_delay_by_title.get(title, 0.0)
            if delay > 0:
                time.sleep(delay)
        with self._lock:
            self.prompts.append(prompt)
            self.attempts += 1
            attempt = self.attempts
        matches_failure = (
            self.fail_when_prompt_contains is not None
            and self.fail_when_prompt_contains in prompt
        )
        fails_this_call = matches_failure and (
            is_summary if self.fail_summaries else not is_summary
        )
        if fails_this_call:
            raise LabelingError("forced labeling failure")
        if attempt <= self.failures_before_success:
            raise LabelingError(f"transient labeling failure on attempt {attempt}")
        english_marker = "EN" if "English" in prompt else "NO-EN"
        if is_summary:
            if self.summary_by_title is not None and title in self.summary_by_title:
                text = self.summary_by_title[title]
            else:
                text = (
                    f"SUMMARY({english_marker}):{prompt.split('Title:', 1)[-1].strip()}"
                )
        else:
            text = (
                self.headline
                if english_marker == "EN"
                else self.headline.replace("HEADLINE(EN):", "HEADLINE(NO-EN):", 1)
            )
        return GenerationResult(
            text=text,
            prompt_tokens=len(prompt.split()),
            completion_tokens=len(text.split()),
        )


def _label(
    run_id: object,
    corpus: Corpus,
    client: FakeLabelingClient,
    engine: Engine,
    **config_overrides: object,
):
    return label_topics(
        run_id,  # type: ignore[arg-type]
        corpus,
        _label_config(**config_overrides),
        client,
        engine,
        tokenizer=_tokenizer(),
    )


def _persist_run(
    processor_engine: Engine,
    corpus: Corpus,
    *,
    topic_ids: Sequence[int],
    representatives: dict[int, Sequence[str]],
    keywords: dict[int, tuple[tuple[str, float], ...]] | None = None,
) -> TopicRunResult:
    """Persist a completed topic run via the public fit_topics path with a planted fitter."""
    documents = tuple(
        f"{work.title}\n{work.abstract}" if work.title else work.abstract
        for work in corpus.works
    )
    doc_by_work = {
        work.work_id: documents[index] for index, work in enumerate(corpus.works)
    }
    keywords_by_topic = keywords or {
        topic_id: (("term", 1.0),) for topic_id in set(topic_ids) if topic_id != -1
    }
    representative_docs = {
        topic_id: tuple(doc_by_work[work_id] for work_id in work_ids)
        for topic_id, work_ids in representatives.items()
    }
    embeddings = np.eye(len(corpus.works), 8)

    class PlantedFitter:
        def fit_transform(
            self, docs: Sequence[str], embeddings: np.ndarray
        ) -> FitResult:
            return FitResult(
                topic_ids=tuple(topic_ids),
                keywords_by_topic=keywords_by_topic,
                representative_docs_by_topic=representative_docs,
            )

    return fit_topics(
        corpus,
        embeddings,
        _topic_config(),
        processor_engine,
        fitter=PlantedFitter(),
    )


def test_two_work_topic_gets_summary_per_work_and_one_headline(
    processor_engine: Engine,
) -> None:
    """A two-work topic is labeled from both works with persisted summaries and headline."""
    works = (
        _work(
            1, title="Quantum dots", abstract="Semiconductor nanocrystals emit light."
        ),
        _work(
            2, title="Photon cascade", abstract="Multi-exciton recombination dynamics."
        ),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0),
        representatives={0: (works[0].work_id, works[1].work_id)},
    )
    client = FakeLabelingClient()

    result = _label(run.run_id, corpus, client, processor_engine)

    assert len(result.labels) == 1
    label = result.labels[0]
    assert label.status == "succeeded"
    assert label.headline == FIXED_HEADLINE
    assert label.sample_size == 2
    assert set(label.sampled_work_ids) == {works[0].work_id, works[1].work_id}
    assert label.chunk_count == 1
    assert label.prompt_version == PROMPT_VERSION
    assert label.sampling_method == "all_members"

    with processor_engine.connect() as connection:
        summaries = (
            connection.execute(
                select(WorkSummary.__table__).where(
                    WorkSummary.__table__.c.topic_run_id == run.run_id
                )
            )
            .mappings()
            .all()
        )
        assert len(summaries) == 2
        assert {row["work_id"] for row in summaries} == {
            works[0].work_id,
            works[1].work_id,
        }
        assert all(row["prompt_version"] == PROMPT_VERSION for row in summaries)
        assert all(row["summary_text"].startswith("SUMMARY(EN):") for row in summaries)
        assert all("secret" not in str(dict(row)).lower() for row in summaries)

        label_row = (
            connection.execute(
                select(TopicLabel.__table__).where(
                    TopicLabel.__table__.c.topic_run_id == run.run_id
                )
            )
            .mappings()
            .one()
        )
        assert label_row["headline"] == label.headline
        assert label_row["concatenated_summary_text"]
        assert label_row["chunk_count"] == 1
        assert label_row["sampling_method"] == "all_members"
        assert set(label_row["sampled_work_ids"]) == {
            works[0].work_id,
            works[1].work_id,
        }
        assert label_row["sample_size"] == 2
        assert label_row["model"] == "Qwen/Qwen3.6-35B-A3B"
        assert label_row["model_revision"] == "test-rev"
        assert label_row["prompt_version"] == PROMPT_VERSION
        assert label_row["status"] == "succeeded"
    assert result.summaries_processed == 2
    assert result.summaries_succeeded == 2
    assert result.summaries_failed == 0
    assert result.summaries_inserted == 2
    assert result.headlines_processed == 1
    assert result.headlines_succeeded == 1
    assert result.headlines_failed == 0
    assert result.headlines_inserted == 1


def test_representative_sampling_caps_at_fifteen(processor_engine: Engine) -> None:
    """Large topics sample representative works capped at 15, not every member."""
    works = tuple(
        _work(
            i,
            title=f"Topic work {i}",
            abstract=f"Body text for work {i} about catalysis.",
        )
        for i in range(1, 21)
    )
    corpus = _corpus(works)
    representative_ids = tuple(work.work_id for work in works[:15])
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=tuple([0] * 20),
        representatives={0: representative_ids},
    )

    result = _label(run.run_id, corpus, FakeLabelingClient(), processor_engine)

    assert len(result.labels) == 1
    label = result.labels[0]
    assert label.sampling_method == "representative_docs"
    assert label.sample_size == 15
    assert label.sampled_work_ids == representative_ids
    assert result.summary_count == 15
    assert works[-1].work_id not in label.sampled_work_ids
    assert label.chunk_count == 1


def test_fewer_than_fifteen_works_logs_shortfall(processor_engine: Engine) -> None:
    """A topic supplying fewer than 15 works logs the shortfall without failing."""
    works = tuple(
        _work(i, title=f"Small topic {i}", abstract=f"Abstract {i}")
        for i in range(1, 5)
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0, 0, 0),
        representatives={0: tuple(work.work_id for work in works)},
    )
    client = FakeLabelingClient()

    with capture_logs() as captured:
        result = _label(run.run_id, corpus, client, processor_engine)

    assert result.labels[0].status == "succeeded"
    assert result.labels[0].sample_size == 4
    assert result.labels[0].chunk_count == 1
    assert any(
        entry.get("event") == "label_sample_shortfall"
        and entry.get("available") == 4
        and entry.get("sample_size") == 15
        for entry in captured
    )


def test_outliers_are_not_labeled(processor_engine: Engine) -> None:
    """Outlier assignments are not discovery topics and receive no label row."""
    works = (
        _work(1, title="Cluster A1", abstract="Shared cluster text alpha."),
        _work(2, title="Cluster A2", abstract="Shared cluster text beta."),
        _work(3, title="Cluster A3", abstract="Shared cluster text gamma."),
        _work(4, title="Outlier alone", abstract="Unrelated legal doctrine."),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0, 0, -1),
        representatives={0: (works[0].work_id, works[1].work_id, works[2].work_id)},
    )

    result = _label(run.run_id, corpus, FakeLabelingClient(), processor_engine)

    assert len(result.labels) == 1
    assert result.labels[0].discovery_topic_id == run.topics[0].discovery_topic_id
    with processor_engine.connect() as connection:
        labels = connection.execute(
            select(TopicLabel.__table__).where(
                TopicLabel.__table__.c.topic_run_id == run.run_id
            )
        ).all()
        assert len(labels) == 1


def test_non_english_work_still_stores_english_template(
    processor_engine: Engine,
) -> None:
    """Prompts request English; the fake returns an English template for non-English input."""
    works = (
        _work(
            1,
            title="Fotosíntesis artificial",
            abstract="Catalizadores de óxido de titanio bajo luz visible.",
            language="es",
        ),
        _work(
            2,
            title="Celdas solares de perovskita",
            abstract="Estabilidad y eficiencia en dispositivos de película fina.",
            language="es",
        ),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0),
        representatives={0: (works[0].work_id, works[1].work_id)},
    )
    client = FakeLabelingClient()

    result = _label(run.run_id, corpus, client, processor_engine)

    assert any("English" in prompt for prompt in client.prompts)
    assert all(prompt.count("English") >= 1 for prompt in client.prompts)
    assert result.labels[0].headline == FIXED_HEADLINE
    with processor_engine.connect() as connection:
        summaries = (
            connection.execute(
                select(WorkSummary.__table__).where(
                    WorkSummary.__table__.c.topic_run_id == run.run_id
                )
            )
            .mappings()
            .all()
        )
        assert all(row["summary_text"].startswith("SUMMARY(EN):") for row in summaries)


def test_one_topic_failure_leaves_other_labels_intact(
    processor_engine: Engine,
) -> None:
    """A failure on one topic marks that label failed and keeps the other intact."""
    works = (
        _work(1, title="Good topic one", abstract="Stable catalysis pathway."),
        _work(2, title="Good topic two", abstract="Stable catalysis kinetics."),
        _work(3, title="Good topic three", abstract="Stable catalysis support."),
        _work(4, title="FAIL_MARKER boom", abstract="This topic will fail labeling."),
        _work(5, title="FAIL_MARKER more", abstract="Companion in the failing topic."),
        _work(
            6, title="FAIL_MARKER last", abstract="Third member of the failing topic."
        ),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0, 0, 1, 1, 1),
        representatives={
            0: (works[0].work_id, works[1].work_id, works[2].work_id),
            1: (works[3].work_id, works[4].work_id, works[5].work_id),
        },
    )
    client = FakeLabelingClient(fail_when_prompt_contains="FAIL_MARKER")

    result = _label(run.run_id, corpus, client, processor_engine)

    by_status = {label.status: label for label in result.labels}
    assert set(by_status) == {"succeeded", "failed"}
    assert by_status["succeeded"].headline is not None
    assert by_status["failed"].headline is None

    with processor_engine.connect() as connection:
        rows = (
            connection.execute(
                select(TopicLabel.__table__).where(
                    TopicLabel.__table__.c.topic_run_id == run.run_id
                )
            )
            .mappings()
            .all()
        )
        assert {row["status"] for row in rows} == {"succeeded", "failed"}
        succeeded = next(row for row in rows if row["status"] == "succeeded")
        failed = next(row for row in rows if row["status"] == "failed")
        assert succeeded["headline"]
        assert failed["headline"] is None
        assert failed["concatenated_summary_text"]
        summaries = connection.execute(
            select(WorkSummary.__table__).where(
                WorkSummary.__table__.c.topic_run_id == run.run_id
            )
        ).all()
        assert len(summaries) == 6
        failed_summaries = connection.execute(
            select(WorkSummary.__table__).where(
                WorkSummary.__table__.c.topic_run_id == run.run_id,
                WorkSummary.__table__.c.discovery_topic_id
                == by_status["failed"].discovery_topic_id,
            )
        ).all()
        assert len(failed_summaries) == 3
    assert result.summaries_processed == 6
    assert result.summaries_succeeded == 6
    assert result.summaries_failed == 0
    assert result.summaries_inserted == 6
    assert result.headlines_processed == 2
    assert result.headlines_succeeded == 1
    assert result.headlines_failed == 1
    assert result.headlines_inserted == 2


def test_summary_failure_skips_work_and_continues_topic(
    processor_engine: Engine,
) -> None:
    """A failed summary is skipped; remaining works still produce a headline."""
    works = (
        _work(1, title="Good topic one", abstract="Stable catalysis pathway."),
        _work(2, title="FAIL_MARKER boom", abstract="This summary call raises."),
        _work(3, title="Good topic three", abstract="Stable catalysis support."),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0, 0),
        representatives={
            0: (works[0].work_id, works[1].work_id, works[2].work_id),
        },
    )
    client = FakeLabelingClient(
        fail_when_prompt_contains="FAIL_MARKER",
        fail_summaries=True,
    )

    result = _label(run.run_id, corpus, client, processor_engine)

    assert len(result.labels) == 1
    assert result.labels[0].status == "succeeded"
    assert result.summaries_processed == 3
    assert result.summaries_succeeded == 2
    assert result.summaries_failed == 1
    assert result.summaries_inserted == 2
    assert result.headlines_processed == 1
    assert result.headlines_succeeded == 1
    assert result.headlines_failed == 0
    with processor_engine.connect() as connection:
        summaries = connection.execute(
            select(WorkSummary.__table__).where(
                WorkSummary.__table__.c.topic_run_id == run.run_id
            )
        ).all()
        assert len(summaries) == 2


def test_all_summaries_failing_marks_topic_failed(
    processor_engine: Engine,
) -> None:
    """When every summary fails after retries, the topic label is failed."""
    works = (
        _work(1, title="FAIL_MARKER one", abstract="a"),
        _work(2, title="FAIL_MARKER two", abstract="b"),
        _work(3, title="FAIL_MARKER three", abstract="c"),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0, 0),
        representatives={
            0: (works[0].work_id, works[1].work_id, works[2].work_id),
        },
    )
    client = FakeLabelingClient(
        fail_when_prompt_contains="FAIL_MARKER",
        fail_summaries=True,
    )

    result = _label(run.run_id, corpus, client, processor_engine)

    assert result.labels[0].status == "failed"
    assert result.summaries_processed == 3
    assert result.summaries_succeeded == 0
    assert result.summaries_failed == 3
    assert result.headlines_failed == 1


def test_labeling_retries_transient_failures(
    processor_engine: Engine,
) -> None:
    """Transient labeling errors are retried before succeeding."""
    works = (
        _work(1, title="Alpha", abstract="a"),
        _work(2, title="Beta", abstract="b"),
        _work(3, title="Gamma", abstract="c"),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0, 0),
        representatives={0: (works[0].work_id, works[1].work_id, works[2].work_id)},
    )
    client = FakeLabelingClient(failures_before_success=2)

    result = _label(
        run.run_id,
        corpus,
        client,
        processor_engine,
        max_retries=2,
        retry_backoff_seconds=0.0,
        max_concurrency=1,
    )

    assert result.labels[0].status == "succeeded"
    assert client.attempts >= 3
    assert result.summaries_failed == 0
    assert result.headlines_succeeded == 1


def _chunking_summaries() -> dict[str, str]:
    """Three many-word summaries that force tokenizer-based windowing.

    Each summary is 12 words. The empty headline prompt is 26 words. With
    context_window=50 and output_reserve=10, one summary fits (12+26+10=48) but
    two do not (24+26+10=60). Character length is short, so a char heuristic
    would wrongly keep them in one window against a large char budget.
    """
    return {
        "Alpha work": "alpha " * 12,
        "Beta work": "beta " * 12,
        "Gamma work": "gamma " * 12,
    }


def test_oversized_topic_chunks_and_persists_final_headline(
    processor_engine: Engine,
) -> None:
    """Summaries over the tokenizer window yield a multi-chunk final headline."""
    works = (
        _work(1, title="Alpha work", abstract="a"),
        _work(2, title="Beta work", abstract="b"),
        _work(3, title="Gamma work", abstract="c"),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0, 0),
        representatives={0: tuple(w.work_id for w in works)},
    )
    summaries = _chunking_summaries()
    client = FakeLabelingClient(summary_by_title=summaries)

    result = _label(
        run.run_id,
        corpus,
        client,
        processor_engine,
        context_window=50,
        output_reserve=10,
    )

    label = result.labels[0]
    assert label.status == "succeeded"
    assert label.chunk_count == 3
    assert label.headline == FIXED_HEADLINE
    assert len(label.headline) == len(FIXED_HEADLINE)

    with processor_engine.connect() as connection:
        row = (
            connection.execute(
                select(TopicLabel.__table__).where(
                    TopicLabel.__table__.c.topic_run_id == run.run_id
                )
            )
            .mappings()
            .one()
        )
        assert row["chunk_count"] == 3
        assert row["headline"] == FIXED_HEADLINE
        assert row["status"] == "succeeded"
        for summary in summaries.values():
            assert summary.strip() in row["concatenated_summary_text"]


def test_within_window_stays_single_pass_with_tokenizer(
    processor_engine: Engine,
) -> None:
    """Concatenated summaries that fit the token budget stay chunk_count=1."""
    works = (
        _work(1, title="Alpha work", abstract="a"),
        _work(2, title="Beta work", abstract="b"),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0),
        representatives={0: (works[0].work_id, works[1].work_id)},
    )
    # Few tokens each: together still under a generous word budget.
    client = FakeLabelingClient(
        summary_by_title={"Alpha work": "short alpha", "Beta work": "short beta"}
    )

    result = _label(
        run.run_id,
        corpus,
        client,
        processor_engine,
        context_window=50,
        output_reserve=10,
    )

    assert result.labels[0].chunk_count == 1
    assert result.labels[0].headline == FIXED_HEADLINE
    assert result.labels[0].status == "succeeded"


def test_token_budget_subtracts_prompt_and_output_reserve(
    processor_engine: Engine,
) -> None:
    """Summaries that fit alone still chunk when prompt plus output reserve do not."""
    works = (
        _work(1, title="Alpha work", abstract="a"),
        _work(2, title="Beta work", abstract="b"),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0),
        representatives={0: (works[0].work_id, works[1].work_id)},
    )
    # 12+12=24 summary words. Empty prompt=26. Reserve=10.
    # Full fit needs 24+26+10=60. Window 55 forces chunking even though 24 < 55.
    client = FakeLabelingClient(
        summary_by_title={
            "Alpha work": "alpha " * 12,
            "Beta work": "beta " * 12,
        }
    )

    result = _label(
        run.run_id,
        corpus,
        client,
        processor_engine,
        context_window=55,
        output_reserve=10,
    )

    assert result.labels[0].status == "succeeded"
    assert result.labels[0].chunk_count == 2
    assert result.labels[0].headline == FIXED_HEADLINE


def test_chunk_boundaries_fall_between_summaries(
    processor_engine: Engine,
) -> None:
    """Each window prompt contains whole summaries, never a truncated mid-summary."""
    works = (
        _work(1, title="Alpha work", abstract="a"),
        _work(2, title="Beta work", abstract="b"),
        _work(3, title="Gamma work", abstract="c"),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0, 0),
        representatives={0: tuple(w.work_id for w in works)},
    )
    summaries = _chunking_summaries()
    client = FakeLabelingClient(summary_by_title=summaries)

    _label(
        run.run_id,
        corpus,
        client,
        processor_engine,
        context_window=50,
        output_reserve=10,
    )

    headline_prompts = [
        prompt
        for prompt in client.prompts
        if prompt.startswith("Write one short English headline")
    ]
    # Three window headlines plus one final aggregation over those headlines.
    assert len(headline_prompts) == 4
    window_bodies = [
        prompt.split("Summaries:", 1)[-1].strip() for prompt in headline_prompts[:3]
    ]
    for body in window_bodies:
        matches = [text.strip() for text in summaries.values() if text.strip() in body]
        assert len(matches) == 1
        assert body == matches[0]
    final_body = headline_prompts[3].split("Summaries:", 1)[-1].strip()
    assert FIXED_HEADLINE in final_body


def test_long_characters_few_tokens_stay_single_pass(
    processor_engine: Engine,
) -> None:
    """Huge character length with few tokenizer tokens stays a single pass.

    A character-budget splitter would treat these as oversized; word counting
    sees one token per unbroken string.
    """
    works = (
        _work(1, title="Alpha work", abstract="a"),
        _work(2, title="Beta work", abstract="b"),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0),
        representatives={0: (works[0].work_id, works[1].work_id)},
    )
    long_a = "A" * 400
    long_b = "B" * 400
    client = FakeLabelingClient(
        summary_by_title={"Alpha work": long_a, "Beta work": long_b}
    )

    result = _label(
        run.run_id,
        corpus,
        client,
        processor_engine,
        context_window=50,
        output_reserve=10,
    )

    assert result.labels[0].chunk_count == 1
    assert result.labels[0].headline == FIXED_HEADLINE


def test_duplicate_representative_ids_are_sampled_once() -> None:
    """A repeated representative work id is summarized once, not inserted twice."""
    from src.labels.label import _select_works

    sampled, method = _select_works(
        {
            "size": 4,
            "representative_work_ids": ("w1", "w1", "w2", "w3"),
        },
        ("w1", "w2", "w3", "w4"),
        15,
    )

    assert method == "representative_docs"
    assert sampled == ("w1", "w2", "w3")


def test_second_label_pass_skips_topics_already_stored(
    processor_engine: Engine,
) -> None:
    """A rerun does not call the model again for topics that already have a label."""
    works = (
        _work(1, title="Kept one", abstract="First abstract."),
        _work(2, title="Kept two", abstract="Second abstract."),
        _work(3, title="Kept three", abstract="Third abstract."),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0, 0),
        representatives={0: (works[0].work_id, works[1].work_id, works[2].work_id)},
    )
    first_client = FakeLabelingClient()
    first = _label(run.run_id, corpus, first_client, processor_engine)
    assert len(first.labels) == 1
    assert first_client.attempts > 0

    second_client = FakeLabelingClient()
    second = _label(run.run_id, corpus, second_client, processor_engine)

    assert second.labels == ()
    assert second_client.attempts == 0
    with processor_engine.connect() as connection:
        labels = connection.execute(
            select(TopicLabel.__table__).where(
                TopicLabel.__table__.c.topic_run_id == run.run_id
            )
        ).all()
        summaries = connection.execute(
            select(WorkSummary.__table__).where(
                WorkSummary.__table__.c.topic_run_id == run.run_id
            )
        ).all()
    assert len(labels) == 1
    assert len(summaries) == 3


def test_rerun_reuses_existing_summary_rows_without_reinsert(
    processor_engine: Engine,
) -> None:
    """Summaries left without a label are reused; no duplicate insert or model call."""
    works = (
        _work(1, title="Alpha work", abstract="First abstract."),
        _work(2, title="Beta work", abstract="Second abstract."),
        _work(3, title="Gamma work", abstract="Third abstract."),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0, 0),
        representatives={0: (works[0].work_id, works[1].work_id, works[2].work_id)},
    )
    discovery_topic_id = run.topics[0].discovery_topic_id
    cached_text = "CACHED SUMMARY FOR ALPHA"
    with processor_engine.begin() as connection:
        connection.execute(
            insert(WorkSummary.__table__).values(
                topic_run_id=run.run_id,
                discovery_topic_id=discovery_topic_id,
                work_id=works[0].work_id,
                summary_text=cached_text,
                model="Qwen/Qwen3.6-35B-A3B",
                model_revision="test-rev",
                prompt_version=PROMPT_VERSION,
                prompt_tokens=1,
                completion_tokens=1,
            )
        )

    client = FakeLabelingClient()
    result = _label(run.run_id, corpus, client, processor_engine)

    assert result.labels[0].status == "succeeded"
    assert cached_text in (result.labels[0].concatenated_summary_text or "")
    summary_prompts = [
        prompt
        for prompt in client.prompts
        if "Write a concise English summary" in prompt
    ]
    assert len(summary_prompts) == 2
    assert all("Alpha work" not in prompt for prompt in summary_prompts)
    assert result.summaries_processed == 2
    assert result.summaries_inserted == 2

    with processor_engine.connect() as connection:
        summaries = (
            connection.execute(
                select(WorkSummary.__table__).where(
                    WorkSummary.__table__.c.topic_run_id == run.run_id
                )
            )
            .mappings()
            .all()
        )
        assert len(summaries) == 3
        by_work = {row["work_id"]: row["summary_text"] for row in summaries}
        assert by_work[works[0].work_id] == cached_text
        labels = connection.execute(
            select(TopicLabel.__table__).where(
                TopicLabel.__table__.c.topic_run_id == run.run_id
            )
        ).all()
        assert len(labels) == 1


def test_db_error_on_one_topic_keeps_earlier_committed_label(
    processor_engine: Engine,
) -> None:
    """UniqueViolation or disk-full on one topic does not roll back earlier topics."""
    works = (
        _work(1, title="Good topic one", abstract="Stable catalysis pathway."),
        _work(2, title="Good topic two", abstract="Stable catalysis kinetics."),
        _work(3, title="Good topic three", abstract="Stable catalysis support."),
        _work(4, title="Other topic one", abstract="Optical lattice dynamics."),
        _work(5, title="Other topic two", abstract="Optical lattice trapping."),
        _work(6, title="Other topic three", abstract="Optical lattice cooling."),
    )
    corpus = _corpus(works)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0, 0, 1, 1, 1),
        representatives={
            0: (works[0].work_id, works[1].work_id, works[2].work_id),
            1: (works[3].work_id, works[4].work_id, works[5].work_id),
        },
    )
    ordered_topic_ids = sorted(topic.discovery_topic_id for topic in run.topics)
    first_topic_id = ordered_topic_ids[0]
    persist_calls = {"n": 0}
    original = label_mod._persist_topic_artifacts

    def flaky_persist(
        engine: Engine,
        *,
        topic_run_id: uuid.UUID,
        label: object,
        summary_rows: Sequence[object],
        replace_label: bool = False,
    ) -> int:
        persist_calls["n"] += 1
        if persist_calls["n"] == 1:
            return original(
                engine,
                topic_run_id=topic_run_id,
                label=label,  # type: ignore[arg-type]
                summary_rows=summary_rows,  # type: ignore[arg-type]
                replace_label=replace_label,
            )
        raise OperationalError(
            "INSERT INTO pwf_topic_labels",
            {},
            Exception("psycopg.errors.DiskFull"),
        )

    client = FakeLabelingClient()
    with patch.object(label_mod, "_persist_topic_artifacts", side_effect=flaky_persist):
        result = _label(
            run.run_id,
            corpus,
            client,
            processor_engine,
            max_concurrency=1,
        )

    assert len(result.labels) == 2
    by_id = {label.discovery_topic_id: label for label in result.labels}
    assert by_id[first_topic_id].status == "succeeded"
    assert by_id[ordered_topic_ids[1]].status == "failed"

    with processor_engine.connect() as connection:
        rows = (
            connection.execute(
                select(TopicLabel.__table__).where(
                    TopicLabel.__table__.c.topic_run_id == run.run_id
                )
            )
            .mappings()
            .all()
        )
        assert len(rows) == 1
        assert rows[0]["discovery_topic_id"] == first_topic_id
        assert rows[0]["status"] == "succeeded"
        summaries = connection.execute(
            select(WorkSummary.__table__).where(
                WorkSummary.__table__.c.topic_run_id == run.run_id,
                WorkSummary.__table__.c.discovery_topic_id == first_topic_id,
            )
        ).all()
        assert len(summaries) == 3

    # UniqueViolation-style IntegrityError is also isolated per topic.
    persist_calls["n"] = 0

    def unique_violation_persist(
        engine: Engine,
        *,
        topic_run_id: uuid.UUID,
        label: object,
        summary_rows: Sequence[object],
        replace_label: bool = False,
    ) -> int:
        persist_calls["n"] += 1
        if persist_calls["n"] == 1:
            raise IntegrityError(
                "INSERT INTO pwf_work_summaries",
                {},
                Exception("UniqueViolation"),
            )
        return original(
            engine,
            topic_run_id=topic_run_id,
            label=label,  # type: ignore[arg-type]
            summary_rows=summary_rows,  # type: ignore[arg-type]
            replace_label=replace_label,
        )

    # Fresh run: first topic already labeled, only second remains; force IntegrityError
    # then confirm the earlier label row is still present.
    client2 = FakeLabelingClient()
    with patch.object(
        label_mod, "_persist_topic_artifacts", side_effect=unique_violation_persist
    ):
        second = _label(
            run.run_id,
            corpus,
            client2,
            processor_engine,
            max_concurrency=1,
        )
    assert len(second.labels) == 1
    assert second.labels[0].status == "failed"
    with processor_engine.connect() as connection:
        kept = (
            connection.execute(
                select(TopicLabel.__table__).where(
                    TopicLabel.__table__.c.topic_run_id == run.run_id,
                    TopicLabel.__table__.c.discovery_topic_id == first_topic_id,
                )
            )
            .mappings()
            .one()
        )
        assert kept["status"] == "succeeded"


def test_concurrent_summaries_concatenate_in_sample_order(
    processor_engine: Engine,
) -> None:
    """Concurrent summary generation still joins texts in sample order."""
    works = (
        _work(1, title="Alpha work", abstract="a"),
        _work(2, title="Beta work", abstract="b"),
        _work(3, title="Gamma work", abstract="c"),
    )
    corpus = _corpus(works)
    sample_order = (works[0].work_id, works[1].work_id, works[2].work_id)
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=(0, 0, 0),
        representatives={0: sample_order},
    )
    # Finish later samples first so completion order differs from sample order.
    client = FakeLabelingClient(
        summary_by_title={
            "Alpha work": "SUMMARY_ALPHA",
            "Beta work": "SUMMARY_BETA",
            "Gamma work": "SUMMARY_GAMMA",
        },
        summary_delay_by_title={
            "Alpha work": 0.05,
            "Beta work": 0.02,
            "Gamma work": 0.0,
        },
    )

    result = _label(
        run.run_id,
        corpus,
        client,
        processor_engine,
        max_concurrency=3,
    )

    assert result.labels[0].status == "succeeded"
    assert (
        result.labels[0].concatenated_summary_text
        == "SUMMARY_ALPHA\n\nSUMMARY_BETA\n\nSUMMARY_GAMMA"
    )
    assert result.labels[0].sampled_work_ids == sample_order


def test_backfill_extends_short_representatives_to_fifteen(
    processor_engine: Engine,
) -> None:
    """Short representative lists are expanded to min(15, size) via centroids."""
    works = tuple(
        _work(
            i,
            title=f"Topic work {i}",
            abstract=f"Body text for work {i} about catalysis.",
        )
        for i in range(1, 21)
    )
    corpus = _corpus(works)
    short_reps = tuple(work.work_id for work in works[:3])
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=tuple([0] * 20),
        representatives={0: short_reps},
    )
    # Plant orthogonal unit vectors; earlier works sit nearer a shared mean.
    vectors = {
        work.work_id: np.eye(20, dtype=np.float64)[index]
        for index, work in enumerate(works)
    }
    client = FakeLabelingClient()

    result = label_topics(
        run.run_id,
        corpus,
        _label_config(),
        client,
        processor_engine,
        tokenizer=_tokenizer(),
        vectors_by_work=vectors,
    )

    assert len(result.labels) == 1
    label = result.labels[0]
    assert label.sample_size == 15
    assert label.sampling_method == "representative_docs"
    assert len(label.sampled_work_ids) == 15
    with processor_engine.connect() as connection:
        topic_row = (
            connection.execute(
                select(DiscoveryTopic.__table__).where(
                    DiscoveryTopic.__table__.c.discovery_topic_id
                    == run.topics[0].discovery_topic_id
                )
            )
            .mappings()
            .one()
        )
        assert len(topic_row["representative_work_ids"]) == 15


def test_failed_full_sample_label_is_retried_on_rerun(
    processor_engine: Engine,
) -> None:
    """A failed full-sample label is replaced; existing summaries are reused."""
    works = tuple(
        _work(
            i,
            title=f"Topic work {i}",
            abstract=f"Body text for work {i} about catalysis.",
        )
        for i in range(1, 21)
    )
    corpus = _corpus(works)
    representative_ids = tuple(work.work_id for work in works[:15])
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=tuple([0] * 20),
        representatives={0: representative_ids},
    )
    first = _label(
        run.run_id,
        corpus,
        FakeLabelingClient(fail_when_prompt_contains="Topic work"),
        processor_engine,
    )
    assert len(first.labels) == 1
    assert first.labels[0].status == "failed"
    assert first.labels[0].sample_size == 15
    discovery_topic_id = first.labels[0].discovery_topic_id
    with processor_engine.connect() as connection:
        summary_count = len(
            connection.execute(
                select(WorkSummary.__table__).where(
                    WorkSummary.__table__.c.topic_run_id == run.run_id,
                    WorkSummary.__table__.c.discovery_topic_id == discovery_topic_id,
                )
            ).all()
        )
    assert summary_count == 15

    retry_headline = "HEADLINE(EN): Retried after failure"
    second = _label(
        run.run_id,
        corpus,
        FakeLabelingClient(headline=retry_headline),
        processor_engine,
    )

    assert len(second.labels) == 1
    assert second.labels[0].status == "succeeded"
    assert second.labels[0].headline == retry_headline
    assert second.labels[0].sample_size == 15
    assert second.summaries_inserted == 0
    assert second.summaries_processed == 0
    assert second.headlines_succeeded == 1
    with processor_engine.connect() as connection:
        label_rows = (
            connection.execute(
                select(TopicLabel.__table__).where(
                    TopicLabel.__table__.c.topic_run_id == run.run_id
                )
            )
            .mappings()
            .all()
        )
        assert len(label_rows) == 1
        assert label_rows[0]["status"] == "succeeded"
        assert label_rows[0]["headline"] == retry_headline
        summaries = connection.execute(
            select(WorkSummary.__table__).where(
                WorkSummary.__table__.c.topic_run_id == run.run_id,
                WorkSummary.__table__.c.discovery_topic_id == discovery_topic_id,
            )
        ).all()
        assert len(summaries) == summary_count


def test_full_sample_label_is_skipped_on_rerun(processor_engine: Engine) -> None:
    """A label already sampled at min(15, size) is not regenerated."""
    works = tuple(
        _work(
            i,
            title=f"Topic work {i}",
            abstract=f"Body text for work {i} about catalysis.",
        )
        for i in range(1, 21)
    )
    corpus = _corpus(works)
    representative_ids = tuple(work.work_id for work in works[:15])
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=tuple([0] * 20),
        representatives={0: representative_ids},
    )
    first_client = FakeLabelingClient()
    first = _label(run.run_id, corpus, first_client, processor_engine)
    assert first.labels[0].sample_size == 15
    first_headline = first.labels[0].headline
    first_attempts = first_client.attempts

    vectors = {
        work.work_id: np.eye(20, dtype=np.float64)[index]
        for index, work in enumerate(works)
    }
    second_client = FakeLabelingClient(headline="HEADLINE(EN): Should not appear")
    second = label_topics(
        run.run_id,
        corpus,
        _label_config(),
        second_client,
        processor_engine,
        tokenizer=_tokenizer(),
        vectors_by_work=vectors,
    )

    assert second.labels == ()
    assert second_client.attempts == 0
    with processor_engine.connect() as connection:
        label_row = (
            connection.execute(
                select(TopicLabel.__table__).where(
                    TopicLabel.__table__.c.topic_run_id == run.run_id
                )
            )
            .mappings()
            .one()
        )
        assert label_row["headline"] == first_headline
        assert label_row["sample_size"] == 15
    assert first_attempts > 0


def test_short_sample_label_is_regenerated_after_backfill(
    processor_engine: Engine,
) -> None:
    """An undersampled label is replaced once representatives grow past sample_size."""
    works = tuple(
        _work(
            i,
            title=f"Topic work {i}",
            abstract=f"Body text for work {i} about catalysis.",
        )
        for i in range(1, 17)
    )
    corpus = _corpus(works)
    short_reps = tuple(work.work_id for work in works[:3])
    run = _persist_run(
        processor_engine,
        corpus,
        topic_ids=tuple([0] * 16),
        representatives={0: short_reps},
    )
    # First pass without vectors keeps the planted 3-doc sample.
    first = _label(run.run_id, corpus, FakeLabelingClient(), processor_engine)
    assert first.labels[0].sample_size == 3
    old_headline = first.labels[0].headline
    discovery_topic_id = run.topics[0].discovery_topic_id

    vectors = {
        work.work_id: np.eye(16, dtype=np.float64)[index]
        for index, work in enumerate(works)
    }
    new_headline = "HEADLINE(EN): Regenerated full sample"
    second_client = FakeLabelingClient(headline=new_headline)
    second = label_topics(
        run.run_id,
        corpus,
        _label_config(),
        second_client,
        processor_engine,
        tokenizer=_tokenizer(),
        vectors_by_work=vectors,
    )

    assert len(second.labels) == 1
    assert second.labels[0].sample_size == 15
    assert second.labels[0].headline == new_headline
    assert second.labels[0].headline != old_headline
    # Three summaries reused; twelve new ones inserted.
    assert second.summaries_inserted == 12
    assert second_client.attempts > 0
    with processor_engine.connect() as connection:
        label_row = (
            connection.execute(
                select(TopicLabel.__table__).where(
                    TopicLabel.__table__.c.topic_run_id == run.run_id
                )
            )
            .mappings()
            .one()
        )
        assert label_row["sample_size"] == 15
        assert label_row["headline"] == new_headline
        assert len(label_row["sampled_work_ids"]) == 15
        summaries = connection.execute(
            select(WorkSummary.__table__).where(
                WorkSummary.__table__.c.topic_run_id == run.run_id,
                WorkSummary.__table__.c.discovery_topic_id == discovery_topic_id,
            )
        ).all()
        assert len(summaries) == 15
        topic_row = (
            connection.execute(
                select(DiscoveryTopic.__table__).where(
                    DiscoveryTopic.__table__.c.discovery_topic_id == discovery_topic_id
                )
            )
            .mappings()
            .one()
        )
        assert len(topic_row["representative_work_ids"]) == 15
