from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from futures_rebuild.causal_full_build_durable_host import (
    DURABLE_HOST_ENVIRONMENT_KEY,
    DURABLE_HOST_EVIDENCE_ROOT,
    expected_durable_host_plan,
    inspect_durable_full_build_worker,
    run_durable_full_build_worker,
    validate_active_durable_host_evidence,
    validate_durable_host_environment,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]


def _repo(tmp_path: Path) -> Path:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "start_causal_full_build_v10_worker.ps1").write_text("# synthetic\n")
    return tmp_path


def _plan() -> dict[str, object]:
    return {
        "plan_id": "a" * 64,
        "target_market": "ES",
        "attempt_id": "f" * 64,
        "durable_host": expected_durable_host_plan("ES", "f" * 64),
    }


def _evidence(root: Path) -> Path:
    return root / DURABLE_HOST_EVIDENCE_ROOT / "ES" / ("f" * 64)


def test_v10_environment_fails_closed_outside_exact_scheduled_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    monkeypatch.delenv(DURABLE_HOST_ENVIRONMENT_KEY, raising=False)
    with pytest.raises(UnauthorizedOperation, match="scheduled task"):
        validate_durable_host_environment(root, _plan())
    monkeypatch.setenv(DURABLE_HOST_ENVIRONMENT_KEY, "wrong-task")
    with pytest.raises(UnauthorizedOperation, match="scheduled task"):
        validate_durable_host_environment(root, _plan())


def test_durable_worker_writes_start_heartbeat_logs_and_terminal_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    monkeypatch.setenv(DURABLE_HOST_ENVIRONMENT_KEY, _plan()["durable_host"]["task_name"])
    monkeypatch.setattr(
        "futures_rebuild.causal_full_build_durable_host.DURABLE_HOST_HEARTBEAT_INTERVAL_SECONDS",
        0.01,
    )
    plan = _plan()

    def operation() -> str:
        validate_active_durable_host_evidence(root, plan)
        return "done"

    assert run_durable_full_build_worker(
        repository_root=root, plan=plan, operation=operation
    ) == "done"
    evidence = _evidence(root)
    assert json.loads((evidence / "started.json").read_text())["status"] == "STARTED"
    assert json.loads((evidence / "heartbeat.json").read_text())["status"] == "TERMINAL"
    assert json.loads((evidence / "exit.json").read_text())["status"] == "PASS"
    assert (evidence / "stdout.log").is_file()
    assert (evidence / "stderr.log").is_file()
    with pytest.raises(IntegrityError, match="already exists"):
        run_durable_full_build_worker(
            repository_root=root, plan=_plan(), operation=lambda: None
        )


def test_durable_worker_records_caught_failure_before_reraising(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    monkeypatch.setenv(DURABLE_HOST_ENVIRONMENT_KEY, _plan()["durable_host"]["task_name"])
    monkeypatch.setattr(
        "futures_rebuild.causal_full_build_durable_host.DURABLE_HOST_HEARTBEAT_INTERVAL_SECONDS",
        0.01,
    )

    def fail() -> None:
        raise RuntimeError("synthetic failure")

    with pytest.raises(RuntimeError, match="synthetic failure"):
        run_durable_full_build_worker(repository_root=root, plan=_plan(), operation=fail)
    terminal = json.loads((_evidence(root) / "exit.json").read_text())
    assert terminal["status"] == "FAILED"
    assert terminal["error_type"] == "RuntimeError"


def test_heartbeat_terminal_failure_is_recorded_and_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    monkeypatch.setenv(DURABLE_HOST_ENVIRONMENT_KEY, _plan()["durable_host"]["task_name"])

    def fail_finish(_: object) -> None:
        raise OSError("synthetic heartbeat failure")

    monkeypatch.setattr(
        "futures_rebuild.causal_full_build_durable_host._Heartbeat.finish", fail_finish
    )
    with pytest.raises(OSError, match="synthetic heartbeat failure"):
        run_durable_full_build_worker(
            repository_root=root, plan=_plan(), operation=lambda: "built"
        )
    terminal = json.loads((_evidence(root) / "exit.json").read_text())
    assert terminal["status"] == "FAILED_HOST_EVIDENCE"
    assert terminal["heartbeat_error_type"] == "OSError"


def test_stale_heartbeat_and_dead_pid_classifies_abrupt_host_loss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    evidence = _evidence(root)
    evidence.mkdir(parents=True)
    (evidence / "started.json").write_text(json.dumps({"pid": 999_999_999}) + "\n")
    observed = datetime(2026, 8, 25, tzinfo=timezone.utc)
    (evidence / "heartbeat.json").write_text(
        json.dumps({"observed_at": observed.isoformat(), "pid": 999_999_999}) + "\n"
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_full_build_durable_host._local_process_alive",
        lambda _: False,
    )
    status = inspect_durable_full_build_worker(
        repository_root=root,
        plan=_plan(),
        now=observed + timedelta(hours=1, seconds=1),
    )
    assert status["status"] == "ABRUPT_TERMINATION_SUSPECTED"


def test_scheduler_launcher_is_nonoverwriting_and_not_restartable() -> None:
    text = (ROOT / "scripts/start_causal_full_build_v10_worker.ps1").read_text()
    assert "FIMV2-Causal-V10-{0}-{1}" in text
    assert "Get-ScheduledTask" in text
    assert "refusing to overwrite" in text
    assert "Register-ScheduledTask" in text
    assert "AddMinutes(2)" in text
    assert "Start-ScheduledTask" not in text
    assert "manual_start = $false" in text
    assert "REGISTERED_FOR_SERVICE_TRIGGER_AFTER_LAUNCHER_EXIT" in text
    assert "System32/WindowsPowerShell/v1.0/powershell.exe" in text
    assert "-WindowStyle Hidden" in text
    assert "-DontStopOnIdleEnd" in text
    assert "-DisallowHardTerminate" in text
    assert "Unregister-ScheduledTask" not in text
    assert "Start-Process" not in text
