"""Emergence configuration, results, and errors."""

from __future__ import annotations

import uuid
from dataclasses import dataclass


@dataclass(frozen=True)
class EmergenceResult:
    """Persisted emergence observations for one topic run.

    Attributes:
        topic_run_id: Topic run that was scored.
        observation_count: Observation rows present after this call.
        headlines_processed: Headlines sent to the embedding client this call, including failures.
        headlines_cached: Succeeded headlines already stored and not re-sent.
        headlines_saved: Headline points upserted this call.
        headlines_failed: Headlines skipped after embedding failure this call.
        coverage_status: Shared observation coverage, joined when a run mixes
            statuses, or ``none`` when the run has no observations.
    """

    topic_run_id: uuid.UUID
    observation_count: int
    headlines_processed: int = 0
    headlines_cached: int = 0
    headlines_saved: int = 0
    headlines_failed: int = 0
    coverage_status: str = "none"


class EmergenceError(RuntimeError):
    """The emergence stage could not score a usable topic run."""
