"""Port that runs a BERTopic-compatible fit over documents and embeddings."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

import numpy as np


@dataclass(frozen=True)
class FitResult:
    """Assignments and per-topic artefacts from one fit."""

    topic_ids: tuple[int, ...]
    keywords_by_topic: Mapping[int, tuple[tuple[str, float], ...]]
    representative_docs_by_topic: Mapping[int, tuple[str, ...]]


class TopicFitter(Protocol):
    """Seam for injecting a failing fit in tests without mocking BERTopic."""

    def fit_transform(
        self, documents: Sequence[str], embeddings: np.ndarray
    ) -> FitResult:
        """Fit topics over `documents` using precomputed `embeddings`."""
