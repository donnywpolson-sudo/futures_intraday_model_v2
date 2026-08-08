from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
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


def test_checked_in_legacy_baseline_is_self_validating_historical_evidence() -> None:
    config = Path(__file__).parents[1] / "configs" / "legacy_baseline.json"
    baseline = json.loads(config.read_text(encoding="utf-8"))
    core = {key: value for key, value in baseline.items() if key != "baseline_id"}
    assert sha256_json(core) == baseline["baseline_id"]
    assert baseline["head"] == "5929e9ec07f6815b149dbab97cbacf7fdbf7cb19"
    assert baseline["status_count"] == 839
