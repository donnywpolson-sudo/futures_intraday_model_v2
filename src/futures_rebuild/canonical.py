"""Canonical encoding and file integrity helpers."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path
from typing import Any

from .errors import ContractError, IntegrityError


def canonical_bytes(value: Any) -> bytes:
    """Return the one permitted JSON encoding for hashed artifacts."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def is_linklike(path: Path) -> bool:
    """Detect symbolic links and Windows reparse points (including junctions)."""

    info = path.lstat()
    file_attributes = getattr(info, "st_file_attributes", 0)
    reparse_point = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return path.is_symlink() or bool(file_attributes & reparse_point)


def assert_plain_file(path: Path, *, reject_hardlinks: bool = True) -> os.stat_result:
    if not path.exists():
        raise IntegrityError(f"required file does not exist: {path}")
    if is_linklike(path):
        raise ContractError(f"links and junctions are forbidden: {path}")
    info = path.stat()
    if not stat.S_ISREG(info.st_mode):
        raise ContractError(f"expected a regular file: {path}")
    if reject_hardlinks and info.st_nlink != 1:
        raise ContractError(f"hard-linked files are forbidden: {path}")
    return info


def assert_no_linklike_ancestors(path: Path) -> None:
    """Reject an existing symlink/junction anywhere in an absolute path."""

    absolute = path.absolute()
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current = current / part
        if current.exists() and is_linklike(current):
            raise ContractError(f"path crosses a link or junction: {current}")


def sha256_file(path: Path, *, reject_hardlinks: bool = True) -> str:
    assert_plain_file(path, reject_hardlinks=reject_hardlinks)
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def contained_path(root: Path, relative: str) -> Path:
    """Resolve a POSIX-like relative path beneath root without escaping it."""

    candidate_relative = Path(relative)
    if candidate_relative.is_absolute() or ".." in candidate_relative.parts:
        raise ContractError(f"path must be relative and contained: {relative}")
    root_resolved = root.resolve(strict=False)
    candidate = (root_resolved / candidate_relative).resolve(strict=False)
    try:
        candidate.relative_to(root_resolved)
    except ValueError as exc:
        raise ContractError(f"path escapes declared root: {relative}") from exc
    return candidate


def fsync_directory(path: Path) -> None:
    """Best-effort directory fsync; Windows does not expose it consistently."""

    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)
