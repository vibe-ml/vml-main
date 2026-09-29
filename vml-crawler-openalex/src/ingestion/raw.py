"""Durable, byte-preserving publication of API responses."""

import gzip
import hashlib
import os
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4


def sync_directory(path: Path) -> None:
    """Flush directory entries after creation or rename."""
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def durable_directory(path: Path) -> None:
    """Create directories and persist every new parent entry."""
    if not path.exists():
        durable_directory(path.parent)
        path.mkdir(exist_ok=True)
        sync_directory(path.parent)
    sync_directory(path)


def preserve(
    root: Path, body: bytes, failure: Callable[[str], None]
) -> dict[str, str | int]:
    """Validate and flush compressed original bytes before atomic publication."""
    staging = root / "staging"
    destination = root / "raw" / "api"
    durable_directory(staging)
    durable_directory(destination)
    name = uuid4().hex + ".json.gz"
    temporary = staging / name
    payload = gzip.compress(body, mtime=0)
    with temporary.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    if gzip.decompress(temporary.read_bytes()) != body:
        raise ValueError("Raw validation failed")
    failure("before_raw_publish")
    os.replace(temporary, destination / name)
    sync_directory(destination)
    sync_directory(staging)
    failure("after_raw_publish")
    return {
        "path": str((destination / name).relative_to(root)),
        "checksum": hashlib.sha256(body).hexdigest(),
        "compressed_checksum": hashlib.sha256(payload).hexdigest(),
        "bytes": len(body),
    }
