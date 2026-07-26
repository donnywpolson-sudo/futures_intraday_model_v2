from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import IntegrityError
from futures_rebuild.legacy_guard import capture_legacy_baseline, verify_legacy_baseline


def _git(root: Path, *args: str) -> None:
    subprocess.run(("git", *args), cwd=root, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def _repo(tmp_path: Path) -> Path:
    root = tmp_path / "legacy"
    root.mkdir()
    _git(root, "init", "-b", "main")
    (root / "tracked.txt").write_text("tracked\n", encoding="utf-8")
    _git(root, "add", "tracked.txt")
    _git(root, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "baseline")
    (root / "untracked.txt").write_text("untracked\n", encoding="utf-8")
    return root


def test_verify_is_read_only_and_detects_content_mutation(tmp_path: Path) -> None:
    root = _repo(tmp_path)
    expected = capture_legacy_baseline(root)
    config = tmp_path / "baseline.json"
    config.write_bytes(canonical_bytes(expected) + b"\n")
    index = root / ".git" / "index"
    before = (sha256_file(index, reject_hardlinks=False), capture_legacy_baseline(root))
    assert verify_legacy_baseline(config) == expected
    assert (sha256_file(index, reject_hardlinks=False), capture_legacy_baseline(root)) == before
    (root / "untracked.txt").write_text("changed\n", encoding="utf-8")
    with pytest.raises(IntegrityError, match="legacy worktree changed"):
        verify_legacy_baseline(config)


def test_frozen_legacy_project_is_unchanged() -> None:
    config = Path(__file__).parents[1] / "configs" / "legacy_baseline.json"
    observed = verify_legacy_baseline(config)
    assert observed["head"] == "5929e9ec07f6815b149dbab97cbacf7fdbf7cb19"
    assert observed["status_count"] == 839
