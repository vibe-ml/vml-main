"""HTTP labeling client for the configured summarization endpoint."""

from __future__ import annotations

from types import TracebackType
from typing import Self

import httpx

from src.labels.models import GenerationResult, LabelingError


class HttpxLabelingClient:
    """OpenAI-compatible chat client for one base URL and API key.

    Credentials stay on the client instance and are never returned in results.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        *,
        timeout: float = 120.0,
    ) -> None:
        self._model = model
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers=headers,
            timeout=timeout,
        )

    def generate(self, prompt: str) -> GenerationResult:
        """Return generated text and token counts for `prompt`."""
        try:
            response = self._client.post(
                "/chat/completions",
                json={
                    "model": self._model,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPError as error:
            raise LabelingError(f"labeling endpoint failed: {error}") from error

        try:
            text = payload["choices"][0]["message"]["content"]
            usage = payload.get("usage") or {}
            prompt_tokens = int(usage.get("prompt_tokens", 0))
            completion_tokens = int(usage.get("completion_tokens", 0))
        except (KeyError, TypeError, ValueError, IndexError) as error:
            raise LabelingError(
                f"labeling endpoint returned an unexpected payload: {error}"
            ) from error
        if not isinstance(text, str) or not text.strip():
            raise LabelingError("labeling endpoint returned an empty completion")
        return GenerationResult(
            text=text.strip(),
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )

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
