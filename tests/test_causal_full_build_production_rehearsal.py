from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.causal_observation_full_build import (
    validate_full_build_storage_floor,
)
from futures_rebuild.errors import ContractError, UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(os.name != "nt", reason="Windows production launcher rehearsal")
def test_real_launcher_runs_small_provider_free_production_rehearsal(
    tmp_path: Path,
) -> None:
    rehearsal_root = tmp_path / "rehearsal"
    rehearsal_root.mkdir()
    launcher = ROOT / "scripts/start_causal_full_build_v10_worker.ps1"
    powershell = Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    completed = subprocess.run(
        [
            str(powershell),
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(launcher),
            "-Rehearsal",
            "-RehearsalRoot",
            str(rehearsal_root),
            "-RehearsalPythonExecutable",
            sys.executable,
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
    )
    result = json.loads(completed.stdout.strip().splitlines()[-1])
    core = {key: value for key, value in result.items() if key != "rehearsal_id"}
    assert result["rehearsal_id"] == sha256_json(core)
    assert result["status"] == "PASS_CAUSAL_FULL_BUILD_PRODUCTION_REHEARSAL"
    assert result["real_launcher_sha256"] == sha256_file(launcher)
    assert result["parquet_path_length"] >= 265
    assert result["checkpoint_create_only_verified"] is True
    assert result["resource_ceiling_rejection_verified"] is True
    assert result["durable_host_heartbeat_terminal"] is True
    assert result["network_denied"] is True
    assert result["provider_calls"] == 0
    assert result["source_rows_read"] == 0
    assert result["receipt_issued"] is False
    assert result["receipt_consumed"] is False
    assert result["one_use_authority_consumed"] is False
    assert result["full_build_executed"] is False
    assert result["holdout_rows"] == 0
    assert result["forward_rows"] == 0
    assert result["publication_authorized"] is False
    assert result["activation_authorized"] is False
    assert result["scheduled_task_registered"] is False
    assert not (rehearsal_root / "state/authorization_uses").exists()
    script = launcher.read_text(encoding="utf-8")
    assert script.index("if ($Rehearsal)") < script.index("Get-ScheduledTask")


def test_storage_floor_shared_boundary_is_exact_and_fail_closed() -> None:
    assert (
        validate_full_build_storage_floor(
            free_bytes=100,
            maximum_peak_additional_bytes=40,
            minimum_free_after_peak_bytes=60,
        )
        == 60
    )
    with pytest.raises(UnauthorizedOperation, match="storage floor"):
        validate_full_build_storage_floor(
            free_bytes=100,
            maximum_peak_additional_bytes=41,
            minimum_free_after_peak_bytes=60,
        )
    with pytest.raises(ContractError, match="storage budget"):
        validate_full_build_storage_floor(
            free_bytes=True,
            maximum_peak_additional_bytes=0,
            minimum_free_after_peak_bytes=0,
        )
