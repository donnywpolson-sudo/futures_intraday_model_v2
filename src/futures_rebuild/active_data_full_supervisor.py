"""Durably supervise one exact approval-bound full-certification process."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from .active_data_full_certification import _load_canonical
from .active_data_view import (
    ACTIVE_ROOT,
    verify_approval,
    verify_contract,
    verify_plan_bindings,
)
from .canonical import (
    assert_no_linklike_ancestors,
    canonical_bytes,
    fsync_directory,
    sha256_file,
    sha256_json,
)
from .errors import IntegrityError
from .runtime_environment import require_locked_repository_environment


SUPERVISION_SCHEMA = "causal_active_full_supervision/1.0.0"
TERMINAL_SCHEMA = "causal_active_full_supervisor_terminal/1.0.0"
SUPERVISOR_PATH = "src/futures_rebuild/active_data_full_supervisor.py"
SUPERVISOR_LAUNCHER_PATH = "scripts/start_active_data_full_supervisor.ps1"
HEARTBEAT_INTERVAL_SECONDS = 30
TERMINAL_GRACE_SECONDS = 300


def _utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_supervision_binding(
    *,
    repository_root: Path,
    interruption_id: str,
    attempt_number: int,
    maximum_duration_seconds: int,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    if (
        len(interruption_id) != 64
        or attempt_number < 2
        or maximum_duration_seconds <= 0
    ):
        raise IntegrityError("durable supervision basis is invalid")
    basis: dict[str, object] = {
        "attempt_number": attempt_number,
        "heartbeat_interval_seconds": HEARTBEAT_INTERVAL_SECONDS,
        "interruption_id": interruption_id,
        "launcher_sha256": sha256_file(root / SUPERVISOR_LAUNCHER_PATH),
        "maximum_duration_seconds": maximum_duration_seconds,
        "supervisor_sha256": sha256_file(root / SUPERVISOR_PATH),
        "terminal_grace_seconds": TERMINAL_GRACE_SECONDS,
    }
    supervision_id = sha256_json(basis)
    state_root = f"state/active_data_view_supervision/{supervision_id}"
    report_root = f"reports/active_data_view/supervision/{supervision_id}"
    return {
        **basis,
        "duplicate_start_policy": "FAIL_BEFORE_CHILD_PROCESS",
        "heartbeat_path": f"{state_root}/heartbeat.json",
        "scheduler_entry_lifecycle": (
            "NONRECURRING_RETAIN_INERT_UNTIL_SEPARATELY_REVIEWED_CLEANUP"
        ),
        "scheduler_execution_time_limit_seconds": (
            maximum_duration_seconds + TERMINAL_GRACE_SECONDS
        ),
        "schema_version": SUPERVISION_SCHEMA,
        "start_path": f"{state_root}/started.json",
        "stderr_path": f"{report_root}/stderr.log",
        "stdout_path": f"{report_root}/stdout.log",
        "supervision_id": supervision_id,
        "task_name": f"FIMV2-Stage6-{supervision_id[:16]}",
        "terminal_path": f"{report_root}/terminal.json",
        "temporary_heartbeat_path": f"{state_root}/heartbeat.tmp",
        "transport": "WINDOWS_TASK_SCHEDULER_MANUAL_ONE_SHOT",
    }


def validate_supervision_binding(
    repository_root: Path,
    plan: Mapping[str, object],
) -> dict[str, object]:
    attempt = plan.get("execution_attempt")
    limits = plan.get("limits")
    observed = plan.get("supervision")
    if (
        not isinstance(attempt, dict)
        or not isinstance(limits, dict)
        or not isinstance(observed, dict)
        or not isinstance(attempt.get("interruption_id"), str)
        or type(attempt.get("attempt_number")) is not int
        or type(limits.get("maximum_duration_seconds")) is not int
    ):
        raise IntegrityError("full-certification supervision is absent")
    expected = build_supervision_binding(
        repository_root=repository_root,
        interruption_id=str(attempt["interruption_id"]),
        attempt_number=int(attempt["attempt_number"]),
        maximum_duration_seconds=int(limits["maximum_duration_seconds"]),
    )
    if observed != expected:
        raise IntegrityError("full-certification supervision binding changed")
    return expected


def build_child_command(
    *,
    repository_root: Path,
    plan_path: Path,
    approval_path: Path,
) -> tuple[str, ...]:
    root = repository_root.resolve(strict=True)
    interpreter = root / ".venv" / "Scripts" / "python.exe"
    return (
        str(interpreter),
        "-m",
        "futures_rebuild.active_data_full_certification",
        "--repository-root",
        str(root),
        "--plan",
        str(plan_path.resolve(strict=True)),
        "--approval",
        str(approval_path.resolve(strict=True)),
    )


def _write_new(path: Path, payload: Mapping[str, object]) -> None:
    assert_no_linklike_ancestors(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_bytes(payload) + b"\n"
    with path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    fsync_directory(path.parent)


def _write_heartbeat(
    *,
    heartbeat_path: Path,
    temporary_path: Path,
    payload: Mapping[str, object],
) -> None:
    assert_no_linklike_ancestors(heartbeat_path)
    assert_no_linklike_ancestors(temporary_path)
    heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
    if temporary_path.exists():
        raise IntegrityError("stale supervisor heartbeat temporary exists")
    encoded = canonical_bytes(payload) + b"\n"
    with temporary_path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary_path, heartbeat_path)
    fsync_directory(heartbeat_path.parent)


def _supervision_paths(
    root: Path,
    supervision: Mapping[str, object],
) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for key in (
        "heartbeat_path",
        "start_path",
        "stderr_path",
        "stdout_path",
        "terminal_path",
        "temporary_heartbeat_path",
    ):
        value = supervision.get(key)
        if not isinstance(value, str):
            raise IntegrityError(f"supervisor {key} is absent")
        path = root / PurePosixPath(value)
        try:
            path.resolve(strict=False).relative_to(root)
        except ValueError as exc:
            raise IntegrityError(f"supervisor {key} is outside the repository") from exc
        paths[key] = path
    return paths


def describe_launch(
    *,
    repository_root: Path,
    plan_path: Path,
    approval_path: Path,
    require_approved: bool,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    require_locked_repository_environment(root)
    verify_contract(root)
    plan = _load_canonical(plan_path, "full certification plan")
    verify_plan_bindings(root, plan)
    supervision = validate_supervision_binding(root, plan)
    approval = _load_canonical(approval_path, "full certification approval")
    approval_receipt_id: str | None = None
    if require_approved:
        approval_receipt_id = verify_approval(
            approval,
            plan,
            expected_operation="CERTIFY_CAUSAL_ACTIVE_VIEW",
        )
    elif approval.get("plan_id") != plan.get("plan_id"):
        raise IntegrityError("pending approval does not bind the supervised plan")
    paths = _supervision_paths(root, supervision)
    if (root / ACTIVE_ROOT).exists():
        raise IntegrityError("durable supervision requires data/active to remain absent")
    for key in ("start_path", "stderr_path", "stdout_path", "terminal_path"):
        if paths[key].exists():
            raise IntegrityError(f"supervisor create-only output exists: {key}")
    if paths["temporary_heartbeat_path"].exists():
        raise IntegrityError("stale supervisor heartbeat temporary exists")
    command = build_child_command(
        repository_root=root,
        plan_path=plan_path,
        approval_path=approval_path,
    )
    return {
        "approval_receipt_id": approval_receipt_id,
        "child_command": list(command),
        "plan_id": plan["plan_id"],
        "scheduler_execution_time_limit_seconds": supervision[
            "scheduler_execution_time_limit_seconds"
        ],
        "supervision_id": supervision["supervision_id"],
        "task_name": supervision["task_name"],
    }


def _valid_full_report(
    *,
    repository_root: Path,
    plan: Mapping[str, object],
) -> str:
    scope_id = str(plan["certification_scope_id"])
    report_path = (
        repository_root
        / "reports"
        / "active_data_view"
        / "full"
        / scope_id
        / "full_certification_report.json"
    )
    report = _load_canonical(report_path, "full certification aggregate report")
    report_id = report.get("full_certification_report_id")
    core = {
        key: value
        for key, value in report.items()
        if key != "full_certification_report_id"
    }
    if (
        report.get("status") != "PASS"
        or report.get("plan_id") != plan.get("plan_id")
        or not isinstance(report_id, str)
        or report_id != sha256_json(core)
    ):
        raise IntegrityError("supervised full-certification report is invalid")
    return report_id


def run_supervised(
    *,
    repository_root: Path,
    plan_path: Path,
    approval_path: Path,
) -> int:
    root = repository_root.resolve(strict=True)
    launch = describe_launch(
        repository_root=root,
        plan_path=plan_path,
        approval_path=approval_path,
        require_approved=True,
    )
    plan = _load_canonical(plan_path, "full certification plan")
    supervision = validate_supervision_binding(root, plan)
    paths = _supervision_paths(root, supervision)
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    command = build_child_command(
        repository_root=root,
        plan_path=plan_path,
        approval_path=approval_path,
    )
    start_core: dict[str, object] = {
        "approval_receipt_id": launch["approval_receipt_id"],
        "child_command": list(command),
        "plan_id": plan["plan_id"],
        "schema_version": SUPERVISION_SCHEMA,
        "started_at": started_at,
        "status": "STARTED",
        "supervision_id": supervision["supervision_id"],
        "task_name": supervision["task_name"],
    }
    _write_new(
        paths["start_path"],
        {**start_core, "supervisor_start_id": sha256_json(start_core)},
    )
    sequence = 0
    timed_out = False
    child: subprocess.Popen[bytes] | None = None
    exit_code: int | None = None
    terminal_status = "FAILED_FAIL_CLOSED"
    full_report_id: str | None = None
    failure_type: str | None = None
    try:
        paths["stdout_path"].parent.mkdir(parents=True, exist_ok=True)
        with (
            paths["stdout_path"].open("xb") as stdout_handle,
            paths["stderr_path"].open("xb") as stderr_handle,
        ):
            child = subprocess.Popen(
                command,
                cwd=root,
                stdin=subprocess.DEVNULL,
                stdout=stdout_handle,
                stderr=stderr_handle,
                creationflags=(
                    subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
                ),
            )
            while True:
                exit_code = child.poll()
                elapsed = time.monotonic() - started_monotonic
                sequence += 1
                _write_heartbeat(
                    heartbeat_path=paths["heartbeat_path"],
                    temporary_path=paths["temporary_heartbeat_path"],
                    payload={
                        "child_process_id": child.pid,
                        "elapsed_seconds": format(elapsed, ".3f"),
                        "observed_at": _utc_now(),
                        "plan_id": plan["plan_id"],
                        "sequence": sequence,
                        "status": "RUNNING" if exit_code is None else "CHILD_EXITED",
                        "supervision_id": supervision["supervision_id"],
                    },
                )
                if exit_code is not None:
                    break
                if elapsed >= int(supervision["maximum_duration_seconds"]):
                    timed_out = True
                    child.terminate()
                    try:
                        exit_code = child.wait(timeout=60)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        exit_code = child.wait(timeout=30)
                    break
                time.sleep(int(supervision["heartbeat_interval_seconds"]))
        if timed_out:
            terminal_status = "TIMED_OUT_FAIL_CLOSED"
        elif exit_code == 0:
            full_report_id = _valid_full_report(repository_root=root, plan=plan)
            terminal_status = "PASS"
        else:
            terminal_status = "FAILED_FAIL_CLOSED"
    except Exception as exc:
        failure_type = type(exc).__name__
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                exit_code = child.wait(timeout=60)
            except subprocess.TimeoutExpired:
                child.kill()
                exit_code = child.wait(timeout=30)
        terminal_status = "SUPERVISOR_ERROR_FAIL_CLOSED"
    completed_at = _utc_now()
    elapsed = time.monotonic() - started_monotonic
    terminal_core: dict[str, object] = {
        "approval_receipt_id": launch["approval_receipt_id"],
        "child_exit_code": exit_code,
        "completed_at": completed_at,
        "elapsed_seconds": format(elapsed, ".3f"),
        "failure_type": failure_type,
        "full_certification_report_id": full_report_id,
        "plan_id": plan["plan_id"],
        "schema_version": TERMINAL_SCHEMA,
        "started_at": started_at,
        "status": terminal_status,
        "supervision_id": supervision["supervision_id"],
        "task_name": supervision["task_name"],
    }
    terminal = {
        **terminal_core,
        "supervisor_terminal_id": sha256_json(terminal_core),
    }
    _write_new(paths["terminal_path"], terminal)
    _write_heartbeat(
        heartbeat_path=paths["heartbeat_path"],
        temporary_path=paths["temporary_heartbeat_path"],
        payload={
            "child_process_id": child.pid if child is not None else None,
            "elapsed_seconds": format(elapsed, ".3f"),
            "observed_at": completed_at,
            "plan_id": plan["plan_id"],
            "sequence": sequence + 1,
            "status": terminal_status,
            "supervision_id": supervision["supervision_id"],
            "supervisor_terminal_id": terminal["supervisor_terminal_id"],
        },
    )
    return 0 if terminal_status == "PASS" else 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--approval", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--describe-plan", action="store_true")
    mode.add_argument("--preflight", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.repository_root.resolve(strict=True)
    resolve = lambda path: path if path.is_absolute() else root / path
    plan_path = resolve(args.plan)
    approval_path = resolve(args.approval)
    if args.describe_plan or args.preflight:
        result = describe_launch(
            repository_root=root,
            plan_path=plan_path,
            approval_path=approval_path,
            require_approved=args.preflight,
        )
        print(canonical_bytes(result).decode())
        return 0
    return run_supervised(
        repository_root=root,
        plan_path=plan_path,
        approval_path=approval_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
