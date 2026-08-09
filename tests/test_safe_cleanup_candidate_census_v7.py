from __future__ import annotations

import inspect
import subprocess

import pytest

from scripts import prepare_safe_cleanup_candidate_census_v7 as cleanup


pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def _head() -> str:
    return subprocess.check_output(
        ["git", "-C", str(cleanup.ROOT), "rev-parse", "HEAD"], text=True
    ).strip()


def test_v7_cleanup_census_is_deterministic_and_preserves_v21_failure() -> None:
    first = cleanup.build_census(root=cleanup.ROOT, committed_head=_head())
    second = cleanup.build_census(root=cleanup.ROOT, committed_head=_head())
    assert first == second
    assert first["schema_version"] == "safe_cleanup_candidate_census/7.0.0"
    assert first["state"] == (
        "PREPARED_NO_MUTATION_SEPARATE_EXACT_CLEANUP_APPROVAL_REQUIRED"
    )
    assert first["preserved_acquisition_failure"] == {
        "report_path": cleanup.FAILURE_REPORT.as_posix(),
        "state": "SEALED_FAIL_CLOSED_RUNTIME_CEILING_NO_ACCEPTED_SOURCE",
        "staging_root_is_cleanup_candidate": False,
        "raw_or_sidecar_file_is_cleanup_candidate": False,
        "cleanup_mutation_authorized": False,
    }
    assert cleanup.FAILURE_REPORT.as_posix() in first["bindings"]
    assert cleanup.V21_PLAN.as_posix() in first["bindings"]
    assert cleanup.V21_AUDIT.as_posix() in first["bindings"]
    assert all(
        not candidate["path"].startswith(("data/", "state/", "configs/"))
        for candidate in first["candidates"]
    )


def test_v7_cleanup_preparation_has_no_mutation_surface() -> None:
    source = inspect.getsource(cleanup)
    assert "Remove-Item" not in source
    assert "shutil.rmtree" not in source
    assert ".unlink(" not in source
    assert "provider_acquisition_staging" not in source
