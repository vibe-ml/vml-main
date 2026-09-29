"""Topics stage: fit discovery topics and persist an append-only topic run."""

from src.topics.fit import composition_hash, fit_topics
from src.topics.fitter import FitResult, TopicFitter
from src.topics.models import (
    COMPOSITION_HASH_ALGORITHM,
    COMPOSITION_HASH_ENCODING_VERSION,
    KeywordWeight,
    PersistedAssignment,
    PersistedDiscoveryTopic,
    TopicConfig,
    TopicFitError,
    TopicRunResult,
)
from src.topics.representatives import (
    DEFAULT_REPRESENTATIVE_CAP,
    centroid_nearest_indices,
    centroid_nearest_items,
    centroid_nearest_work_ids,
)

__all__ = [
    "COMPOSITION_HASH_ALGORITHM",
    "COMPOSITION_HASH_ENCODING_VERSION",
    "DEFAULT_REPRESENTATIVE_CAP",
    "FitResult",
    "KeywordWeight",
    "PersistedAssignment",
    "PersistedDiscoveryTopic",
    "TopicConfig",
    "TopicFitError",
    "TopicFitter",
    "TopicRunResult",
    "centroid_nearest_indices",
    "centroid_nearest_items",
    "centroid_nearest_work_ids",
    "composition_hash",
    "fit_topics",
]
