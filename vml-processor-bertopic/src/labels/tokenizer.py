"""Local-file tokenizer adapter for labeling token budgets."""

from __future__ import annotations

from pathlib import Path

from src.labels.models import LabelingError


class FileTokenizer:
    """Count tokens with a Hugging Face `tokenizers` file from a local path.

    Loads `tokenizer.json` from a directory or a direct file path. Never
    downloads weights or tokenizer files from the network.
    """

    def __init__(self, path: str | Path) -> None:
        from tokenizers import Tokenizer as HuggingFaceTokenizer

        resolved = Path(path)
        tokenizer_json = resolved / "tokenizer.json" if resolved.is_dir() else resolved
        if not tokenizer_json.is_file():
            raise LabelingError(
                f"labeling tokenizer file not found at {tokenizer_json}"
            )
        self._tokenizer = HuggingFaceTokenizer.from_file(str(tokenizer_json))

    def count_tokens(self, text: str) -> int:
        """Return the number of tokens in `text`."""
        return len(self._tokenizer.encode(text).ids)
