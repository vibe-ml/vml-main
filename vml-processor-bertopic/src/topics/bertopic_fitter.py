"""Default BERTopic fitter used by the topics stage."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
from bertopic import BERTopic
from hdbscan import HDBSCAN
from umap import UMAP

from src.topics.fitter import FitResult
from src.topics.models import TopicConfig
from src.topics.representatives import (
    DEFAULT_REPRESENTATIVE_CAP,
    centroid_nearest_items,
)


class BerTopicFitter:
    """Runs one BERTopic ``fit_transform`` with pinned UMAP and HDBSCAN settings."""

    def __init__(self, config: TopicConfig) -> None:
        self._config = config

    def fit_transform(
        self, documents: Sequence[str], embeddings: np.ndarray
    ) -> FitResult:
        """Fit discovery topics and return assignments, keywords, and representatives."""
        hdbscan_kwargs: dict[str, int | bool] = {
            "min_cluster_size": self._config.min_cluster_size,
            "prediction_data": True,
        }
        if self._config.min_samples is not None:
            hdbscan_kwargs["min_samples"] = self._config.min_samples

        model = BERTopic(
            umap_model=UMAP(
                n_neighbors=self._config.umap_n_neighbors,
                n_components=self._config.umap_n_components,
                min_dist=0.0,
                metric=self._config.umap_metric,
                random_state=self._config.umap_random_state,
                n_jobs=1,
            ),
            hdbscan_model=HDBSCAN(**hdbscan_kwargs),
            calculate_probabilities=False,
            verbose=False,
        )
        topics, _probabilities = model.fit_transform(
            list(documents), embeddings=embeddings
        )
        topic_ids = tuple(int(topic) for topic in topics)

        keywords_by_topic: dict[int, tuple[tuple[str, float], ...]] = {}
        representative_docs_by_topic: dict[int, tuple[str, ...]] = {}
        for bertopic_id in sorted({topic for topic in topic_ids if topic != -1}):
            raw_keywords = model.get_topic(bertopic_id) or []
            keywords_by_topic[bertopic_id] = tuple(
                (str(term), float(weight)) for term, weight in raw_keywords
            )
            member_indices = [
                index for index, topic in enumerate(topic_ids) if topic == bertopic_id
            ]
            member_docs = tuple(documents[index] for index in member_indices)
            member_embeddings = embeddings[np.asarray(member_indices, dtype=np.intp)]
            cap = min(DEFAULT_REPRESENTATIVE_CAP, len(member_indices))
            representative_docs_by_topic[bertopic_id] = centroid_nearest_items(
                member_docs, member_embeddings, cap=cap
            )

        return FitResult(
            topic_ids=topic_ids,
            keywords_by_topic=keywords_by_topic,
            representative_docs_by_topic=representative_docs_by_topic,
        )
