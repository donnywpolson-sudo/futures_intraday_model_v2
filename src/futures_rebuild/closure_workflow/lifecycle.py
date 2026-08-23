"""Reusable assertions for generation plans whose outputs may later exist."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable, Mapping


def assert_generation_execution_lifecycle(
    *,
    repo: Path,
    declared_paths: Iterable[str],
    terminal_path: Path,
    expected_present: Mapping[str, str | None],
    expected_absent_after_execution: set[str],
) -> dict[str, object] | None:
    """Assert absence before execution or exact immutable receipts afterward."""

    declared = set(declared_paths)
    if not terminal_path.exists():
        for relative in declared:
            assert not (repo / relative).exists()
        return None

    assert declared == set(expected_present) | expected_absent_after_execution
    for relative in sorted(declared):
        path = repo / relative
        if relative in expected_absent_after_execution:
            assert not path.exists()
            continue
        assert path.exists()
        expected_sha256 = expected_present[relative]
        if expected_sha256 is not None:
            assert path.is_file()
            assert hashlib.sha256(path.read_bytes()).hexdigest() == expected_sha256
    return json.loads(terminal_path.read_text(encoding="utf-8"))
