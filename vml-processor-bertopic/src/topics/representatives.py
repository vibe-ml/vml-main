"""Centroid-nearest representative document selection for discovery topics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np

DEFAULT_REPRESENTATIVE_CAP = 15


def centroid_nearest_indices(
    embeddings: np.ndarray,
    *,
    cap: int = DEFAULT_REPRESENTATIVE_CAP,
) -> tuple[int, ...]:
    """Return row indices of vectors nearest the L2-normalized mean centroid.

    Cosine distance ranks members: unit-normalize rows, take the mean as the
    centroid, re-normalize the centroid, then sort by descending cosine
    similarity. Ties keep the earlier row index.

    Args:
        embeddings: Member vectors shaped ``(n, d)``. Empty input yields ``()``.
        cap: Maximum number of indices to return.

    Returns:
        Up to ``min(cap, n)`` indices into ``embeddings``.
    """
    if embeddings.ndim != 2:
        raise ValueError(
            f"embeddings must be a 2-D array, got shape {embeddings.shape}"
        )
    n = embeddings.shape[0]
    if n == 0 or cap <= 0:
        return ()
    take = min(cap, n)
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms = np.maximum(norms, 1e-12)
    unit = embeddings / norms
    centroid = unit.mean(axis=0)
    centroid_norm = float(np.linalg.norm(centroid))
    if centroid_norm < 1e-12:
        return tuple(range(take))
    centroid = centroid / centroid_norm
    similarities = unit @ centroid
    # argsort ascending then reverse for descending; mergesort keeps ties stable.
    order = np.argsort(-similarities, kind="mergesort")
    return tuple(int(index) for index in order[:take])


def centroid_nearest_items(
    items: Sequence[str],
    embeddings: np.ndarray,
    *,
    cap: int = DEFAULT_REPRESENTATIVE_CAP,
) -> tuple[str, ...]:
    """Return up to ``cap`` items whose rows are nearest the member centroid."""
    if len(items) != embeddings.shape[0]:
        raise ValueError(
            f"items length {len(items)} does not match embeddings rows "
            f"{embeddings.shape[0]}"
        )
    return tuple(
        items[index] for index in centroid_nearest_indices(embeddings, cap=cap)
    )


def centroid_nearest_work_ids(
    work_ids: Sequence[str],
    vectors_by_work: Mapping[str, np.ndarray],
    *,
    cap: int = DEFAULT_REPRESENTATIVE_CAP,
) -> tuple[str, ...]:
    """Select centroid-nearest work IDs among those with available vectors.

    Work IDs missing from ``vectors_by_work`` are skipped. Order among the
    selected IDs follows ascending cosine distance to the centroid.
    """
    available: list[str] = []
    rows: list[np.ndarray] = []
    for work_id in work_ids:
        vector = vectors_by_work.get(work_id)
        if vector is None:
            continue
        available.append(work_id)
        rows.append(np.asarray(vector, dtype=np.float64))
    if not available:
        return ()
    matrix = np.stack(rows, axis=0)
    return centroid_nearest_items(available, matrix, cap=cap)
