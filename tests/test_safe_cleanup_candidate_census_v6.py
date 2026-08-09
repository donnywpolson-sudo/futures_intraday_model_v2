from __future__ import annotations

import inspect
import subprocess

import pytest

from scripts import prepare_safe_cleanup_candidate_census_v6 as cleanup


pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def _head() -> str:
    return subprocess.check_output(
        ["git", "-C", str(cleanup.ROOT), "rev-parse", "HEAD"], text=True
    ).strip()


def test_cleanup_candidate_census_is_deterministic_metadata_only_and_no_mutation() -> None:
    output = cleanup.ROOT / cleanup.OUTPUT
    before = output.read_bytes()
    before_mtime = output.stat().st_mtime_ns
    first = cleanup.build_census(root=cleanup.ROOT, committed_head=_head())
    second = cleanup.build_census(root=cleanup.ROOT, committed_head=_head())
    assert first == second
    assert first["state"] == (
        "PREPARED_NO_MUTATION_SEPARATE_EXACT_CLEANUP_APPROVAL_REQUIRED"
    )
    assert first["cleanup_execution"] == {
        "performed": False,
        "files_deleted": 0,
        "directories_deleted": 0,
        "files_moved": 0,
        "data_changed": False,
        "active_catalog_changed": False,
    }
    assert first["payload_safety"] == {
        "dbn_or_parquet_payload_opened": False,
        "historical_rows_read": False,
        "year_2025_or_2026_payload_opened": False,
        "inventory_from_filesystem_metadata_only": True,
    }
    assert output.read_bytes() == before
    assert output.stat().st_mtime_ns == before_mtime


def test_cleanup_candidates_are_exact_ignored_caches_outside_protected_roots() -> None:
    census = cleanup.build_census(root=cleanup.ROOT, committed_head=_head())
    assert census["candidate_count"] == len(census["candidates"])
    for candidate in census["candidates"]:
        path = candidate["path"]
        assert candidate["tracked"] is False
        assert candidate["git_ignored"] is True
        assert candidate["classification"] == "REGENERABLE_IGNORED_CACHE_CANDIDATE"
        assert not path.startswith(("data/", "state/", "configs/"))
        assert candidate["proposed_action"] == (
            "DELETE_ONLY_AFTER_SEPARATE_EXACT_APPROVAL"
        )
    source = inspect.getsource(cleanup)
    assert "Remove-Item" not in source
    assert "shutil.rmtree" not in source
    assert ".unlink(" not in source
    assert "dbn" not in source.lower().split("def _inventory", 1)[1].split(
        "def _worktree_paths", 1
    )[0]
