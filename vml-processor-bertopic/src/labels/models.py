"""Labels configuration, results, and ports."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from src.labels.prompts import PROMPT_VERSION


class LabelConfig(BaseModel):
    """Model and sampling settings for the labels stage.

    Attributes:
        model: Labeling model name used for summaries and headlines.
        model_revision: Model revision recorded on every artifact.
        sample_size: Maximum works sampled per topic (default 15, hard cap 15).
        prompt_version: Version string stored with every summary and label.
        context_window: Maximum prompt-plus-completion tokens for the model.
        output_reserve: Tokens reserved for the generated headline completion.
        max_retries: Transient retries per labeling call before counting a failure.
        retry_backoff_seconds: Base delay before the first labeling retry; doubles each try.
        max_concurrency: Cap on in-flight labeling generate calls and topics labeled at once.
    """

    model_config = ConfigDict(frozen=True)

    model: str = "Qwen/Qwen3.6-35B-A3B"
    model_revision: str = "main"
    sample_size: int = Field(default=15, ge=1, le=15)
    prompt_version: str = PROMPT_VERSION
    context_window: int = Field(default=32768, ge=1)
    output_reserve: int = Field(default=512, ge=1)
    max_retries: int = Field(default=2, ge=0)
    retry_backoff_seconds: float = Field(default=0.5, ge=0.0)
    max_concurrency: int = Field(default=4, ge=1)


@dataclass(frozen=True)
class GenerationResult:
    """Text and token counts returned by one labeling-model call."""

    text: str
    prompt_tokens: int
    completion_tokens: int


class LabelingClient(Protocol):
    """Port that turns a prompt into generated text."""

    def generate(self, prompt: str) -> GenerationResult:
        """Return generated text and token counts for `prompt`."""


class Tokenizer(Protocol):
    """Port that counts tokens the way the labeling model does."""

    def count_tokens(self, text: str) -> int:
        """Return the number of tokens in `text`."""


@dataclass(frozen=True)
class PersistedTopicLabel:
    """One discovery topic label returned from the labels stage."""

    discovery_topic_id: uuid.UUID
    headline: str | None
    concatenated_summary_text: str | None
    chunk_count: int
    sampling_method: str
    sampled_work_ids: tuple[str, ...]
    sample_size: int
    model: str
    model_revision: str
    prompt_version: str
    status: str


@dataclass(frozen=True)
class LabelRunResult:
    """Persisted labels and this-run counters for one topic run.

    ``summary_count`` is the stored summary count for this topic run. It is not
    ``summaries_inserted`` and it is not the table-wide total. Adopted label
    runs leave the eight this-run counters at 0.

    Attributes:
        summaries_processed: Summary model calls attempted this run.
        summaries_succeeded: Summary calls that returned text this run.
        summaries_failed: Summary calls that raised this run.
        summaries_inserted: ``pwf_work_summaries`` rows inserted this run.
        headlines_processed: Topics whose headline generation was attempted.
        headlines_succeeded: Topics persisted with status succeeded this run.
        headlines_failed: Topics persisted with status failed this run.
        headlines_inserted: ``pwf_topic_labels`` rows inserted this run.
    """

    topic_run_id: uuid.UUID
    labels: tuple[PersistedTopicLabel, ...]
    summary_count: int
    summaries_processed: int = 0
    summaries_succeeded: int = 0
    summaries_failed: int = 0
    summaries_inserted: int = 0
    headlines_processed: int = 0
    headlines_succeeded: int = 0
    headlines_failed: int = 0
    headlines_inserted: int = 0


class LabelingError(RuntimeError):
    """The labels stage could not generate a usable summary or headline."""
