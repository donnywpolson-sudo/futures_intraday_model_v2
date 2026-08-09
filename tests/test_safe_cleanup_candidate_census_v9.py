from __future__ import annotations

import inspect
import subprocess

import pytest

from scripts import prepare_safe_cleanup_candidate_census_v9 as cleanup


pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def _head() -> str:
    return subprocess.check_output(
        ["git", "-C", str(cleanup.ROOT), "rev-parse", "HEAD"], text=True
    ).strip()


def test_v9_census_is_deterministic_and_binds_superseded_v23() -> None:
    first = cleanup.build_census(root=cleanup.ROOT, committed_head=_head())
    second = cleanup.build_census(root=cleanup.ROOT, committed_head=_head())
    assert first == second
    assert first["schema_version"] == "safe_cleanup_candidate_census/9.0.0"
    assert first["state"] == (
        "PREPARED_NO_MUTATION_SEPARATE_EXACT_CLEANUP_APPROVAL_REQUIRED"
    )
    assert first["superseded_v23_preparation"] == {
        "report_path": cleanup.SUPERSESSION_REPORT.as_posix(),
        "state": "SUPERSEDED_PREPARATION_VOLATILE_CAPACITY_SNAPSHOT",
        "plan_audit_or_census_is_cleanup_candidate": False,
        "cleanup_mutation_authorized": False,
    }
    for path in (
        cleanup.SUPERSESSION_REPORT,
        cleanup.V23_PLAN,
        cleanup.V23_AUDIT,
        cleanup.V23_CENSUS,
    ):
        assert path.as_posix() in first["bindings"]
    assert all(
        path not in first["worktree_paths_preserved"]
        for path in cleanup.DECLARED_CREATE_ONLY_OUTPUT_STATUS_PATHS
    )


def test_v9_excludes_only_declared_outputs_and_has_no_mutation_surface() -> None:
    census = cleanup.build_census(root=cleanup.ROOT, committed_head=_head())
    exclusion = census["self_referential_output_exclusion"]
    assert exclusion["exact_status_paths"] == sorted(
        cleanup.DECLARED_CREATE_ONLY_OUTPUT_STATUS_PATHS
    )
    assert exclusion[
        "applies_only_to_create_only_v24_plan_audit_and_census_outputs"
    ] is True
    assert exclusion["excluded_path_is_cleanup_candidate"] is False
    source = inspect.getsource(cleanup)
    assert "Remove-Item" not in source
    assert "shutil.rmtree" not in source
    assert ".unlink(" not in source


def test_v9_post_generation_snapshot_removes_only_three_exact_outputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = cleanup.build_v8(root=cleanup.ROOT, committed_head=_head())
    baseline_core = dict(baseline)
    baseline_core["worktree_paths_preserved"] = sorted(
        {
            *baseline["worktree_paths_preserved"],
            *cleanup.DECLARED_CREATE_ONLY_OUTPUT_STATUS_PATHS,
            "UNRELATED_PRESERVED_PATH.txt",
        }
    )
    monkeypatch.setattr(cleanup, "build_v8", lambda **_kwargs: baseline_core)
    census = cleanup.build_census(root=cleanup.ROOT, committed_head=_head())
    assert "UNRELATED_PRESERVED_PATH.txt" in census["worktree_paths_preserved"]
    assert all(
        path not in census["worktree_paths_preserved"]
        for path in cleanup.DECLARED_CREATE_ONLY_OUTPUT_STATUS_PATHS
    )
