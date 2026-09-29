"""Shared-queue fan-out across multiple embedding endpoints."""

from __future__ import annotations

import threading
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue

from src.common.log import get_logger
from src.embeddings.models import EmbeddingClient, EmbeddingConfig, EmbeddingError

log = get_logger("embeddings")

_QUEUE_POLL_SECONDS = 0.05


class ParallelEmbeddingClient:
    """Distribute embedding texts across endpoints from a shared work queue.

    Workers pull the next text when free, so a slow endpoint receives less work.
    An endpoint that exhausts its failure budget is dropped for the rest of the
    call. A single text is tried on each remaining endpoint at most once (with
    per-call retries); after that it is skipped rather than poisoning every
    host. The stage fails only when no endpoint remains usable and no text
    succeeded.
    """

    def __init__(
        self,
        endpoints: Sequence[tuple[str, EmbeddingClient]],
        *,
        max_concurrency_per_endpoint: int = 4,
        max_concurrency_overall: int | None = None,
        failure_limit: int = 3,
        max_retries: int = 2,
        retry_backoff_seconds: float = 0.5,
    ) -> None:
        if not endpoints:
            raise EmbeddingError(
                "parallel embedding client needs at least one endpoint"
            )
        labels = [label for label, _ in endpoints]
        if len(labels) != len(set(labels)):
            raise EmbeddingError("endpoint labels must be unique")
        if max_concurrency_per_endpoint < 1:
            raise EmbeddingError("max_concurrency_per_endpoint must be >= 1")
        if max_concurrency_overall is not None and max_concurrency_overall < 1:
            raise EmbeddingError("max_concurrency_overall must be >= 1")
        if failure_limit < 1:
            raise EmbeddingError("failure_limit must be >= 1")
        if max_retries < 0:
            raise EmbeddingError("max_retries must be >= 0")
        if retry_backoff_seconds < 0:
            raise EmbeddingError("retry_backoff_seconds must be >= 0")

        self._endpoints = tuple(endpoints)
        self._max_concurrency_per_endpoint = max_concurrency_per_endpoint
        overall = max_concurrency_overall
        if overall is None:
            overall = len(endpoints) * max_concurrency_per_endpoint
        self._max_concurrency_overall = overall
        self._failure_limit = failure_limit
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self.max_in_flight = 0

    @classmethod
    def from_config(
        cls,
        endpoints: Sequence[tuple[str, EmbeddingClient]],
        config: EmbeddingConfig,
    ) -> ParallelEmbeddingClient:
        """Build a fan-out client using concurrency and failover settings from `config`."""
        return cls(
            endpoints,
            max_concurrency_per_endpoint=config.max_concurrency_per_endpoint,
            max_concurrency_overall=config.max_concurrency_overall,
            failure_limit=config.endpoint_failure_limit,
            max_retries=config.endpoint_max_retries,
            retry_backoff_seconds=config.retry_backoff_seconds,
        )

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Return one vector per input text, in the same order.

        Raises EmbeddingError when any text is missing after the run.
        Prefer ``embed_allowing_failures`` when callers can skip failed texts.
        """
        outcomes = self.embed_allowing_failures(texts)
        missing = sum(1 for vector in outcomes if vector is None)
        if missing:
            raise EmbeddingError(
                f"{missing} texts failed after retries on all endpoints"
            )
        return [vector for vector in outcomes if vector is not None]

    def embed_allowing_failures(
        self, texts: Sequence[str]
    ) -> list[list[float] | None]:
        """Return one vector or ``None`` per input text, in the same order.

        ``None`` marks a text that exhausted every usable endpoint. Raises only
        when every endpoint is dropped before any text succeeds.
        """
        if not texts:
            return []

        pending: Queue[int] = Queue()
        for index in range(len(texts)):
            pending.put(index)

        results: list[list[float] | None] = [None] * len(texts)
        resolved: list[bool] = [False] * len(texts)
        state_lock = threading.Lock()
        failures = {label: 0 for label, _ in self._endpoints}
        dropped: set[str] = set()
        tried: dict[int, set[str]] = {index: set() for index in range(len(texts))}
        stop = threading.Event()
        abort_error: list[EmbeddingError] = []
        in_flight = 0
        succeeded = 0

        overall_slots = threading.Semaphore(self._max_concurrency_overall)
        endpoint_slots = {
            label: threading.Semaphore(self._max_concurrency_per_endpoint)
            for label, _ in self._endpoints
        }

        def mark_abort(message: str) -> None:
            if not abort_error:
                abort_error.append(EmbeddingError(message))
            stop.set()

        def usable_labels() -> list[str]:
            return [
                endpoint_label
                for endpoint_label, _ in self._endpoints
                if endpoint_label not in dropped
            ]

        def skip_text(index: int, *, reason: str) -> None:
            nonlocal succeeded
            if resolved[index]:
                return
            resolved[index] = True
            log.warning(
                "embedding_text_skipped",
                index=index,
                reason=reason,
                tried_endpoints=sorted(tried[index]),
            )

        def skip_unresolved(*, reason: str) -> None:
            for pending_index, done in enumerate(resolved):
                if not done:
                    skip_text(pending_index, reason=reason)
            stop.set()

        def worker(label: str, client: EmbeddingClient) -> None:
            nonlocal in_flight, succeeded
            while not stop.is_set():
                with state_lock:
                    if label in dropped:
                        return

                try:
                    index = pending.get(timeout=_QUEUE_POLL_SECONDS)
                except Empty:
                    with state_lock:
                        if all(resolved):
                            return
                        if not usable_labels():
                            if succeeded == 0:
                                mark_abort(
                                    "no usable embedding endpoint remains after failures"
                                )
                            else:
                                skip_unresolved(reason="no_usable_endpoint")
                            return
                    continue

                with state_lock:
                    if resolved[index]:
                        pending.task_done()
                        continue
                    if label in dropped:
                        pending.put(index)
                        pending.task_done()
                        return
                    if label in tried[index]:
                        hosts_left = [
                            endpoint_label
                            for endpoint_label in usable_labels()
                            if endpoint_label not in tried[index]
                        ]
                        if not hosts_left:
                            skip_text(index, reason="exhausted_endpoints")
                            pending.task_done()
                            if all(resolved):
                                stop.set()
                        else:
                            pending.put(index)
                            pending.task_done()
                        continue

                overall_slots.acquire()
                endpoint_slots[label].acquire()
                with state_lock:
                    if label in dropped or resolved[index] or label in tried[index]:
                        endpoint_slots[label].release()
                        overall_slots.release()
                        if not resolved[index]:
                            pending.put(index)
                        pending.task_done()
                        if label in dropped:
                            return
                        continue
                    tried[index].add(label)
                    in_flight += 1
                    self.max_in_flight = max(self.max_in_flight, in_flight)

                try:
                    vector = self._embed_with_retries(client, text=texts[index])
                except EmbeddingError:
                    with state_lock:
                        in_flight -= 1
                        failures[label] += 1
                        if (
                            failures[label] >= self._failure_limit
                            and label not in dropped
                        ):
                            dropped.add(label)
                            log.warning(
                                "embedding_endpoint_dropped",
                                endpoint=label,
                                failures=failures[label],
                                failure_limit=self._failure_limit,
                            )
                        hosts_left = [
                            endpoint_label
                            for endpoint_label in usable_labels()
                            if endpoint_label not in tried[index]
                        ]
                        all_down = not usable_labels()
                        text_exhausted = not hosts_left
                        if text_exhausted and not resolved[index]:
                            skip_text(index, reason="exhausted_endpoints")
                    endpoint_slots[label].release()
                    overall_slots.release()
                    pending.task_done()
                    if all_down and succeeded == 0:
                        mark_abort(
                            "no usable embedding endpoint remains after failures"
                        )
                        return
                    if all_down and succeeded > 0:
                        with state_lock:
                            skip_unresolved(reason="no_usable_endpoint")
                        return
                    if text_exhausted:
                        with state_lock:
                            if all(resolved):
                                stop.set()
                        if label in dropped:
                            return
                        continue
                    pending.put(index)
                    if label in dropped:
                        return
                    continue
                except BaseException as error:
                    with state_lock:
                        in_flight -= 1
                    endpoint_slots[label].release()
                    overall_slots.release()
                    pending.task_done()
                    mark_abort(f"embedding worker failed unexpectedly: {error}")
                    raise
                else:
                    results[index] = vector
                    with state_lock:
                        in_flight -= 1
                        if not resolved[index]:
                            resolved[index] = True
                            succeeded += 1
                        finished = all(resolved)
                    endpoint_slots[label].release()
                    overall_slots.release()
                    pending.task_done()
                    if finished:
                        stop.set()
                        return

        worker_count = len(self._endpoints) * self._max_concurrency_per_endpoint
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(worker, label, client)
                for label, client in self._endpoints
                for _ in range(self._max_concurrency_per_endpoint)
            ]
            for future in futures:
                future.result()

        if abort_error and succeeded == 0:
            raise abort_error[0]

        return results

    def _embed_with_retries(self, client: EmbeddingClient, *, text: str) -> list[float]:
        last_error: EmbeddingError | None = None
        for attempt in range(self._max_retries + 1):
            try:
                vectors = client.embed([text])
            except EmbeddingError as error:
                last_error = error
                if attempt < self._max_retries and self._retry_backoff_seconds > 0:
                    time.sleep(self._retry_backoff_seconds * (2**attempt))
                continue
            if len(vectors) != 1:
                last_error = EmbeddingError(
                    f"embedding endpoint returned {len(vectors)} vectors for 1 text"
                )
                if attempt < self._max_retries and self._retry_backoff_seconds > 0:
                    time.sleep(self._retry_backoff_seconds * (2**attempt))
                continue
            return vectors[0]
        assert last_error is not None
        raise last_error
