"""HTTP embedding client for a single configured endpoint."""

from __future__ import annotations

from collections.abc import Sequence
from types import TracebackType
from typing import Self

import httpx

from src.embeddings.models import EmbeddingError


class HttpxEmbeddingClient:
    """OpenAI-compatible embeddings client for one base URL.

    Combine several instances with `ParallelEmbeddingClient` for multi-host fan-out.
    """

    def __init__(self, base_url: str, model: str, *, timeout: float = 120.0) -> None:
        self._model = model
        self._client = httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout)

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Return one vector per input text, in the same order."""
        if not texts:
            return []
        try:
            response = self._client.post(
                "/v1/embeddings",
                json={"model": self._model, "input": list(texts)},
            )
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPError as error:
            raise EmbeddingError(f"embedding endpoint failed: {error}") from error

        try:
            rows = payload["data"]
            ordered = sorted(rows, key=lambda row: row["index"])
            vectors = [list(row["embedding"]) for row in ordered]
        except (KeyError, TypeError, ValueError) as error:
            raise EmbeddingError(
                f"embedding endpoint returned an unexpected payload: {error}"
            ) from error
        if len(vectors) != len(texts):
            raise EmbeddingError(
                f"embedding endpoint returned {len(vectors)} vectors for {len(texts)} texts"
            )
        return vectors

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
