from __future__ import annotations

import json
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.durable_windows_task_transport import (
    CANARY_APPROVAL_SCHEMA,
    CANARY_DURATION_SECONDS,
    CANARY_PLAN_SCHEMA,
    HEARTBEAT_INTERVAL_SECONDS,
    IMPLEMENTATION_PATH,
    LAUNCHER_PATH,
    JOB_HELPER_PATH,
    MAXIMUM_OUTPUT_BYTES,
    MAXIMUM_PROCESSES,
    OPERATION,
    RECONCILER_PATH,
    RUNNER_PATH,
    SCHEDULER_DURATION_SECONDS,
    TRANSPORT_SCHEMA,
    V10_APPROVAL_PATH,
    V10_INTERRUPTION_PATH,
    V10_LAUNCHER_PATH,
    V10_PLAN_PATH,
    V10_SUPERVISOR_PATH,
    WindowsKillOnCloseJob,
    _process_alive,
    approval_text,
    build_canary_plan,
    build_pending_canary_approval,
    build_v10_diagnosis,
    describe_canary,
    reconcile_postmortem,
    run_canary,
    validate_canary_plan,
    verify_canary_approval,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(payload) + b"\n")


def _copy_required_files(tmp_path: Path) -> None:
    source_root = Path(__file__).resolve().parents[1]
    for relative in (
        IMPLEMENTATION_PATH,
        LAUNCHER_PATH,
        JOB_HELPER_PATH,
        RECONCILER_PATH,
        RUNNER_PATH,
        V10_APPROVAL_PATH,
        V10_LAUNCHER_PATH,
        V10_PLAN_PATH,
        V10_SUPERVISOR_PATH,
    ):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((source_root / relative).read_bytes())
    interruption_core = {
        "active_root_absent": True,
        "interruption_id": (
            "fcfc02c592a5bd3d49e704dfb40868a3232ed5b80979e0d99743886f0e1ce26d"
        ),
        "status": "INTERRUPTED_FAIL_CLOSED",
        "terminal_evidence_absent": True,
    }
    _write_json(tmp_path / V10_INTERRUPTION_PATH, interruption_core)


def _bundle(tmp_path: Path) -> tuple[dict[str, object], dict[str, object]]:
    _copy_required_files(tmp_path)
    diagnosis = build_v10_diagnosis(tmp_path)
    diagnosis_path = (
        tmp_path
        / "manifests/active_data_view/execution_interruptions/"
        "full_certification_v10_transport_diagnosis.json"
    )
    _write_json(diagnosis_path, diagnosis)
    plan = build_canary_plan(tmp_path, diagnosis_path)
    approval = build_pending_canary_approval(plan)
    return plan, approval


def _approve(
    plan: dict[str, object], *, at: str = "2026-07-28T20:00:00Z"
) -> dict[str, object]:
    core = {
        "approved_at": at,
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_json(plan),
        "schema_version": CANARY_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "a" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def test_canary_plan_is_exact_bounded_nonrecurring_s4u(tmp_path: Path) -> None:
    plan, approval = _bundle(tmp_path)

    assert validate_canary_plan(tmp_path, plan) == plan
    assert plan["schema_version"] == CANARY_PLAN_SCHEMA
    assert plan["operation"] == OPERATION
    assert plan["transport"]["schema_version"] == TRANSPORT_SCHEMA
    assert plan["task"] == {
        "action": "WINDOWS_POWERSHELL_JOB_OWNER",
        "logon_type": "S4U",
        "multiple_instances": "IGNORE_NEW",
        "principal": "CURRENT_USER",
        "run_level": "LEAST_PRIVILEGE",
        "task_name": (
            "FIMV2-TransportCanary-" + plan["transport"]["transport_id"][:16]
        ),
        "triggers": [],
    }
    assert plan["limits"] == {
        "canary_duration_seconds": CANARY_DURATION_SECONDS,
        "heartbeat_interval_seconds": HEARTBEAT_INTERVAL_SECONDS,
        "maximum_output_bytes": MAXIMUM_OUTPUT_BYTES,
        "maximum_processes": MAXIMUM_PROCESSES,
        "maximum_retries": 0,
        "scheduler_duration_seconds": SCHEDULER_DURATION_SECONDS,
    }
    assert approval["status"] == "PENDING"
    assert approval["plan_id"] == plan["plan_id"]
    assert plan["plan_id"] == sha256_json(
        {key: value for key, value in plan.items() if key != "plan_id"}
    )


def test_canary_plan_rejects_transport_drift(tmp_path: Path) -> None:
    plan, _ = _bundle(tmp_path)
    (tmp_path / IMPLEMENTATION_PATH).write_text("drifted\n", encoding="utf-8")

    with pytest.raises(
        IntegrityError, match="transport canary plan differs from exact bindings"
    ):
        validate_canary_plan(tmp_path, plan)


def test_duplicate_start_evidence_is_rejected_before_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, pending = _bundle(tmp_path)
    plan_path = tmp_path / "canary-plan.json"
    approval_path = tmp_path / "canary-approval.json"
    _write_json(plan_path, plan)
    _write_json(approval_path, pending)
    start_path = tmp_path / plan["outputs"]["start_path"]
    _write_json(start_path, {"status": "already-exists"})
    monkeypatch.setattr(
        "futures_rebuild.durable_windows_task_transport."
        "require_locked_repository_environment",
        lambda _: "d" * 64,
    )

    with pytest.raises(
        IntegrityError, match="create-only canary output exists: start_path"
    ):
        describe_canary(
            repository_root=tmp_path,
            plan_path=plan_path,
            approval_path=approval_path,
            require_approved=False,
        )


def test_exact_approval_is_required(tmp_path: Path) -> None:
    plan, pending = _bundle(tmp_path)
    with pytest.raises(UnauthorizedOperation):
        verify_canary_approval(pending, plan)

    approved = _approve(plan)
    assert verify_canary_approval(approved, plan) == approved["approval_receipt_id"]

    approved["plan_sha256"] = "b" * 64
    with pytest.raises(UnauthorizedOperation):
        verify_canary_approval(approved, plan)


def test_approval_text_contains_every_bound_limit(tmp_path: Path) -> None:
    plan, _ = _bundle(tmp_path)
    text = approval_text(plan)
    assert str(plan["plan_id"]) in text
    assert sha256_json(plan) in text
    assert str(plan["transport"]["transport_id"]) in text
    assert str(plan["task"]["task_name"]) in text
    assert "AT MOST 3 PROCESSES, 120 CANARY SECONDS, 180 SCHEDULER SECONDS" in text
    assert "NO PROVIDER CALLS" in text
    assert "NO_RETRY" not in text


@pytest.mark.skipif(
    not hasattr(subprocess, "CREATE_NO_WINDOW"), reason="Windows-only job object"
)
def test_job_close_terminates_released_child() -> None:
    command = (
        str(Path(__file__).resolve().parents[1] / ".venv/Scripts/python.exe"),
        "-c",
        "import sys,time; sys.stdin.buffer.read(1); time.sleep(60)",
    )
    child = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    try:
        with WindowsKillOnCloseJob() as job:
            job.assign(child)
            assert child.stdin is not None
            child.stdin.write(b"G")
            child.stdin.flush()
            child.stdin.close()
            assert _process_alive(child.pid)
        child.wait(timeout=10)
        assert not _process_alive(child.pid)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)


@pytest.mark.skipif(
    not hasattr(subprocess, "CREATE_NO_WINDOW"), reason="Windows-only probe"
)
def test_probe_child_fails_closed_without_release() -> None:
    root = Path(__file__).resolve().parents[1]
    command = (
        str(root / ".venv/Scripts/python.exe"),
        "-m",
        "futures_rebuild.durable_windows_task_transport",
        "probe-child",
    )
    result = subprocess.run(
        command,
        input=b"",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
        check=False,
    )
    assert result.returncode == 2


def test_keyboard_interrupt_writes_fail_closed_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _ = _bundle(tmp_path)
    approval = _approve(plan)
    plan_path = tmp_path / "canary-plan.json"
    approval_path = tmp_path / "canary-approval.json"
    _write_json(plan_path, plan)
    _write_json(approval_path, approval)
    monkeypatch.setattr(
        "futures_rebuild.durable_windows_task_transport.describe_canary",
        lambda **_: {
            "approval_receipt_id": approval["approval_receipt_id"],
            "task_name": plan["task"]["task_name"],
            "transport_id": plan["transport"]["transport_id"],
        },
    )
    monkeypatch.setattr(
        "futures_rebuild.durable_windows_task_transport.exercise_containment",
        lambda: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    assert (
        run_canary(
            repository_root=tmp_path,
            plan_path=plan_path,
            approval_path=approval_path,
        )
        == 1
    )
    terminal = json.loads(
        (tmp_path / plan["outputs"]["terminal_path"]).read_bytes()
    )
    heartbeat = json.loads(
        (tmp_path / plan["outputs"]["heartbeat_path"]).read_bytes()
    )
    assert terminal["status"] == "INTERRUPTED_FAIL_CLOSED"
    assert terminal["failure_type"] == "KeyboardInterrupt"
    assert heartbeat["status"] == "INTERRUPTED_FAIL_CLOSED"
    assert heartbeat["canary_terminal_id"] == terminal["canary_terminal_id"]
    assert heartbeat["heartbeat_id"] == sha256_json(
        {
            key: value
            for key, value in heartbeat.items()
            if key != "heartbeat_id"
        }
    )


def test_launchers_encode_exact_s4u_and_reconciliation_contract() -> None:
    root = Path(__file__).resolve().parents[1]
    launcher = (root / LAUNCHER_PATH).read_text(encoding="utf-8")
    reconciler = (root / RECONCILER_PATH).read_text(encoding="utf-8")
    runner = (root / RUNNER_PATH).read_text(encoding="utf-8")
    job_helper = (root / JOB_HELPER_PATH).read_text(encoding="utf-8")
    assert "-LogonType S4U" in launcher
    assert "-RunLevel Limited" in launcher
    assert "-MultipleInstances IgnoreNew" in launcher
    assert "-DontStopOnIdleEnd" in launcher
    assert "New-ScheduledTask `\n    -Action" in launcher
    assert "New-ScheduledTaskTrigger" not in launcher
    assert "verify-approved" in reconciler
    assert "Start-ScheduledTask" not in reconciler
    assert "RunContained" in runner
    assert "ProbeKillOnClose" in runner
    assert runner.index("verify-approved") < runner.index("ProbeKillOnClose")
    assert "CREATE_SUSPENDED" in job_helper
    assert "JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE" in job_helper


def test_postmortem_never_infers_pass(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan, _ = _bundle(tmp_path)
    approval = _approve(plan)
    plan_path = tmp_path / "canary-plan.json"
    approval_path = tmp_path / "canary-approval.json"
    _write_json(plan_path, plan)
    _write_json(approval_path, approval)
    outputs = plan["outputs"]
    launch_core = {
        "approval_receipt_id": approval["approval_receipt_id"],
        "launcher_returned_at": "2026-07-28T20:00:01Z",
        "launch_requested_at": "2026-07-28T20:00:00Z",
        "plan_id": plan["plan_id"],
        "schema_version": "causal_active_transport_canary_launch/1.0.0",
        "status": "START_REQUESTED",
        "task_name": plan["task"]["task_name"],
        "task_xml_sha256": "c" * 64,
        "transport_id": plan["transport"]["transport_id"],
    }
    _write_json(
        tmp_path / outputs["launch_path"],
        {**launch_core, "launch_receipt_id": sha256_json(launch_core)},
    )
    heartbeat_core = {
        "elapsed_seconds": "5.000",
        "observed_at": "2026-07-28T20:00:05Z",
        "plan_id": plan["plan_id"],
        "process_id": 999_999,
        "schema_version": "causal_active_transport_canary_heartbeat/1.0.0",
        "sequence": 1,
        "status": "RUNNING",
        "transport_id": plan["transport"]["transport_id"],
    }
    _write_json(
        tmp_path / outputs["heartbeat_path"],
        {**heartbeat_core, "heartbeat_id": sha256_json(heartbeat_core)},
    )
    monkeypatch.setattr(
        "futures_rebuild.durable_windows_task_transport._process_alive",
        lambda _: False,
    )

    terminal = reconcile_postmortem(
        repository_root=tmp_path,
        plan_path=plan_path,
        approval_path=approval_path,
        scheduler_state="Ready",
        last_task_result=0,
        last_run_at="2026-07-28T20:00:00Z",
        next_run_absent=True,
        observed_at="2026-07-28T20:00:30Z",
    )

    assert terminal["status"] == "POSTMORTEM_INTERRUPTED_FAIL_CLOSED"
    assert terminal["postmortem"]["last_task_result"] == 0
    assert terminal["status"] != "PASS"


def test_postmortem_refuses_running_or_fresh_heartbeat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, _ = _bundle(tmp_path)
    approval = _approve(plan)
    plan_path = tmp_path / "canary-plan.json"
    approval_path = tmp_path / "canary-approval.json"
    _write_json(plan_path, plan)
    _write_json(approval_path, approval)
    outputs = plan["outputs"]
    heartbeat_time = datetime(2026, 7, 28, 20, 0, 5, tzinfo=UTC)
    heartbeat_core = {
        "elapsed_seconds": "5.000",
        "observed_at": heartbeat_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "plan_id": plan["plan_id"],
        "process_id": 999_999,
        "schema_version": "causal_active_transport_canary_heartbeat/1.0.0",
        "sequence": 1,
        "status": "RUNNING",
        "transport_id": plan["transport"]["transport_id"],
    }
    _write_json(
        tmp_path / outputs["heartbeat_path"],
        {**heartbeat_core, "heartbeat_id": sha256_json(heartbeat_core)},
    )
    monkeypatch.setattr(
        "futures_rebuild.durable_windows_task_transport._process_alive",
        lambda _: False,
    )

    with pytest.raises(IntegrityError, match="not terminal and inert"):
        reconcile_postmortem(
            repository_root=tmp_path,
            plan_path=plan_path,
            approval_path=approval_path,
            scheduler_state="Running",
            last_task_result=267_009,
            last_run_at="2026-07-28T20:00:00Z",
            next_run_absent=True,
            observed_at="2026-07-28T20:00:30Z",
        )

    with pytest.raises(IntegrityError, match="not yet conclusively stalled"):
        reconcile_postmortem(
            repository_root=tmp_path,
            plan_path=plan_path,
            approval_path=approval_path,
            scheduler_state="Ready",
            last_task_result=1,
            last_run_at="2026-07-28T20:00:00Z",
            next_run_absent=True,
            observed_at=(heartbeat_time + timedelta(seconds=10)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        )


def test_v10_diagnosis_binds_preserved_files(tmp_path: Path) -> None:
    _copy_required_files(tmp_path)
    diagnosis = build_v10_diagnosis(tmp_path)
    assert diagnosis["status"] == "DIAGNOSED_FAIL_CLOSED"
    assert diagnosis["diagnosis"]["direct_failure_code"] == "C000013A"
    assert diagnosis["diagnosis"]["signal_origin"] == "NOT_ESTABLISHED"
    assert (
        diagnosis["v10"]["supervisor_sha256"]
        == sha256_file(tmp_path / V10_SUPERVISOR_PATH)
    )
    assert diagnosis["diagnosis_id"] == sha256_json(
        {
            key: value
            for key, value in diagnosis.items()
            if key != "diagnosis_id"
        }
    )
