"""Approval-bound, noninteractive Windows transport canary and reconciliation."""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import queue
import subprocess
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    fsync_directory,
    sha256_file,
    sha256_json,
)
from .errors import IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment


CANARY_PLAN_SCHEMA = "causal_active_transport_canary_plan/1.0.0"
CANARY_APPROVAL_SCHEMA = "causal_active_transport_canary_approval/1.0.0"
CANARY_START_SCHEMA = "causal_active_transport_canary_start/1.0.0"
CANARY_LAUNCH_SCHEMA = "causal_active_transport_canary_launch/1.0.0"
CANARY_HEARTBEAT_SCHEMA = "causal_active_transport_canary_heartbeat/1.0.0"
CANARY_CONTAINMENT_SCHEMA = "causal_active_transport_canary_containment/1.0.0"
CANARY_TERMINAL_SCHEMA = "causal_active_transport_canary_terminal/1.0.0"
CANARY_DIAGNOSIS_SCHEMA = "causal_active_transport_diagnosis/1.0.0"
TRANSPORT_SCHEMA = "durable_windows_task_transport/1.0.0"
OPERATION = "PROVE_CROSS_TASK_TRANSPORT_CANARY"
IMPLEMENTATION_PATH = "src/futures_rebuild/durable_windows_task_transport.py"
LAUNCHER_PATH = "scripts/start_active_data_transport_canary.ps1"
RECONCILER_PATH = "scripts/reconcile_active_data_transport_canary.ps1"
RUNNER_PATH = "scripts/run_active_data_transport_canary.ps1"
JOB_HELPER_PATH = "scripts/WindowsKillOnCloseProcess.cs"
V10_INTERRUPTION_PATH = (
    "manifests/active_data_view/execution_interruptions/"
    "full_certification_v10_supervisor_failed.json"
)
V10_PLAN_PATH = (
    "manifests/active_data_view/plans/"
    "causal_active_full_certification_plan_v10.json"
)
V10_APPROVAL_PATH = "configs/causal_active_full_certification_approval_v10.json"
V10_SUPERVISOR_PATH = "src/futures_rebuild/active_data_full_supervisor.py"
V10_LAUNCHER_PATH = "scripts/start_active_data_full_supervisor.ps1"
DEPENDENCY_RECEIPT_PATH = "configs/dependency_lock_receipt.json"
ACTIVE_ROOT = Path("data/active")
HEARTBEAT_INTERVAL_SECONDS = 5
CANARY_DURATION_SECONDS = 120
SCHEDULER_DURATION_SECONDS = 180
MAXIMUM_OUTPUT_BYTES = 1_048_576
MAXIMUM_PROCESSES = 3
PROBE_START_TIMEOUT_SECONDS = 15
PROBE_EXIT_TIMEOUT_SECONDS = 15
JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS = 9
STILL_ACTIVE = 259


def _utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(value: object, description: str) -> datetime:
    if not isinstance(value, str):
        raise IntegrityError(f"{description} is absent")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=UTC
        )
    except ValueError as exc:
        raise IntegrityError(f"{description} is not UTC seconds") from exc
    return parsed


def _load_canonical(path: Path, description: str) -> dict[str, object]:
    assert_plain_file(path)
    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid JSON") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def _contained(root: Path, relative: object, description: str) -> Path:
    if not isinstance(relative, str):
        raise IntegrityError(f"{description} is absent")
    posix = PurePosixPath(relative)
    if posix.is_absolute() or ".." in posix.parts:
        raise IntegrityError(f"{description} is not contained")
    target = root / posix
    try:
        target.resolve(strict=False).relative_to(root)
    except ValueError as exc:
        raise IntegrityError(f"{description} escapes the repository") from exc
    return target


def _write_new(path: Path, payload: Mapping[str, object]) -> None:
    assert_no_linklike_ancestors(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_bytes(payload) + b"\n"
    with path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    fsync_directory(path.parent)


def _write_new_or_exact(path: Path, payload: Mapping[str, object]) -> None:
    encoded = canonical_bytes(payload) + b"\n"
    if path.exists():
        assert_plain_file(path)
        if path.read_bytes() != encoded:
            raise IntegrityError(f"existing artifact differs: {path}")
        return
    _write_new(path, payload)


def _write_heartbeat(path: Path, temporary: Path, payload: Mapping[str, object]) -> None:
    assert_no_linklike_ancestors(path)
    assert_no_linklike_ancestors(temporary)
    path.parent.mkdir(parents=True, exist_ok=True)
    if temporary.exists():
        raise IntegrityError("stale canary heartbeat temporary exists")
    encoded = canonical_bytes(payload) + b"\n"
    with temporary.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    fsync_directory(path.parent)


def _identity(payload: Mapping[str, object], field: str) -> str:
    value = payload.get(field)
    core = {key: item for key, item in payload.items() if key != field}
    if not isinstance(value, str) or value != sha256_json(core):
        raise IntegrityError(f"{field} is invalid")
    return value


def _v10_binding(root: Path) -> dict[str, object]:
    interruption = _load_canonical(
        root / V10_INTERRUPTION_PATH, "v10 interruption"
    )
    if (
        interruption.get("status") != "INTERRUPTED_FAIL_CLOSED"
        or interruption.get("interruption_id")
        != "fcfc02c592a5bd3d49e704dfb40868a3232ed5b80979e0d99743886f0e1ce26d"
        or not interruption.get("active_root_absent")
        or not interruption.get("terminal_evidence_absent")
    ):
        raise IntegrityError("v10 interruption state differs")
    return {
        "approval_path": V10_APPROVAL_PATH,
        "approval_sha256": sha256_file(root / V10_APPROVAL_PATH),
        "interruption_id": interruption["interruption_id"],
        "interruption_path": V10_INTERRUPTION_PATH,
        "interruption_sha256": sha256_file(root / V10_INTERRUPTION_PATH),
        "launcher_path": V10_LAUNCHER_PATH,
        "launcher_sha256": sha256_file(root / V10_LAUNCHER_PATH),
        "plan_path": V10_PLAN_PATH,
        "plan_sha256": sha256_file(root / V10_PLAN_PATH),
        "supervisor_path": V10_SUPERVISOR_PATH,
        "supervisor_sha256": sha256_file(root / V10_SUPERVISOR_PATH),
        "task_name": "FIMV2-Stage6-d0d3f97e2cc23c91",
    }


def build_v10_diagnosis(repository_root: Path) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    v10 = _v10_binding(root)
    core: dict[str, object] = {
        "containment_defect": {
            "child_kill_on_parent_exit": False,
            "postmortem_terminalizer": False,
            "unconditional_child_cleanup": False,
        },
        "diagnosis": {
            "contributing_exposure": (
                "TASK_USED_INTERACTIVE_TOKEN_IN_EXISTING_USER_SESSION"
            ),
            "direct_failure": "STATUS_CONTROL_C_EXIT",
            "direct_failure_code": "C000013A",
            "signal_origin": "NOT_ESTABLISHED",
        },
        "does_not_authorize": [
            "ACTIVE_ROOT_MUTATION",
            "ARCHIVE_OR_DELETE",
            "CANARY_TASK_REGISTRATION_OR_START",
            "HOLDOUT_OR_FORWARD_PAYLOAD_ACCESS",
            "MODEL_FIT_OR_EVALUATION",
            "OUTCOME_LABEL_PREDICTION_ACCESS",
            "PROVIDER_CALL_OR_DOWNLOAD",
            "PUBLICATION",
            "RESTART_OR_RETRY",
            "STAGE6_SUCCESSOR_CREATION_OR_EXECUTION",
            "TRADING",
        ],
        "evidence_limitations": {
            "application_or_system_crash_event_found": False,
            "task_scheduler_operational_log_enabled": False,
        },
        "preservation_rule": (
            "PRESERVE_V10_EVIDENCE_AND_INERT_TASK_WITHOUT_RETRY_OR_CLEANUP"
        ),
        "schema_version": CANARY_DIAGNOSIS_SCHEMA,
        "status": "DIAGNOSED_FAIL_CLOSED",
        "v10": v10,
    }
    return {**core, "diagnosis_id": sha256_json(core)}


def build_canary_plan(
    repository_root: Path, diagnosis_path: Path
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    diagnosis = _load_canonical(diagnosis_path, "v10 transport diagnosis")
    diagnosis_id = _identity(diagnosis, "diagnosis_id")
    if diagnosis.get("status") != "DIAGNOSED_FAIL_CLOSED":
        raise IntegrityError("v10 diagnosis is not fail-closed")
    diagnosis_relative = diagnosis_path.resolve(strict=True).relative_to(root)
    transport_basis: dict[str, object] = {
        "diagnosis_id": diagnosis_id,
        "diagnosis_path": diagnosis_relative.as_posix(),
        "diagnosis_sha256": sha256_file(diagnosis_path),
        "implementation_path": IMPLEMENTATION_PATH,
        "implementation_sha256": sha256_file(root / IMPLEMENTATION_PATH),
        "launcher_path": LAUNCHER_PATH,
        "launcher_sha256": sha256_file(root / LAUNCHER_PATH),
        "job_helper_path": JOB_HELPER_PATH,
        "job_helper_sha256": sha256_file(root / JOB_HELPER_PATH),
        "reconciler_path": RECONCILER_PATH,
        "reconciler_sha256": sha256_file(root / RECONCILER_PATH),
        "runner_path": RUNNER_PATH,
        "runner_sha256": sha256_file(root / RUNNER_PATH),
        "schema_version": TRANSPORT_SCHEMA,
        "v10_interruption_id": (
            "fcfc02c592a5bd3d49e704dfb40868a3232ed5b80979e0d99743886f0e1ce26d"
        ),
    }
    transport_id = sha256_json(transport_basis)
    state_root = f"state/active_data_view_transport_canary/{transport_id}"
    report_root = f"reports/active_data_view/transport_canary/{transport_id}"
    core: dict[str, object] = {
        "forbidden_actions": [
            "ACTIVE_ROOT_MUTATION",
            "ARCHIVE_OR_DELETE",
            "CANDIDATE_CERTIFICATION",
            "HOLDOUT_OR_FORWARD_PAYLOAD_ACCESS",
            "MATERIALIZATION",
            "MODEL_FIT_OR_EVALUATION",
            "OUTCOME_LABEL_PREDICTION_ACCESS",
            "PROVIDER_CALL_OR_DOWNLOAD",
            "PUBLICATION",
            "SOURCE_PAYLOAD_ACCESS",
            "STAGE6_SUCCESSOR_CREATION_OR_EXECUTION",
            "TRADING",
        ],
        "limits": {
            "canary_duration_seconds": CANARY_DURATION_SECONDS,
            "heartbeat_interval_seconds": HEARTBEAT_INTERVAL_SECONDS,
            "maximum_output_bytes": MAXIMUM_OUTPUT_BYTES,
            "maximum_processes": MAXIMUM_PROCESSES,
            "maximum_retries": 0,
            "scheduler_duration_seconds": SCHEDULER_DURATION_SECONDS,
        },
        "operation": OPERATION,
        "outputs": {
            "containment_path": f"{report_root}/containment.json",
            "heartbeat_path": f"{state_root}/heartbeat.json",
            "launch_path": f"{report_root}/launch.json",
            "start_path": f"{state_root}/started.json",
            "temporary_heartbeat_path": f"{state_root}/heartbeat.tmp",
            "terminal_path": f"{report_root}/terminal.json",
        },
        "recovery": {
            "backup": "NOT_APPLICABLE_CREATE_ONLY_NO_OVERWRITE",
            "partial_outputs": "PRESERVE_NONRESUMABLE_FAILURE_EVIDENCE",
            "retry_policy": "NO_RETRY_NEW_PLAN_AND_APPROVAL_REQUIRED",
            "task_lifecycle": (
                "NONRECURRING_RETAIN_INERT_UNTIL_SEPARATELY_REVIEWED_CLEANUP"
            ),
        },
        "schema_version": CANARY_PLAN_SCHEMA,
        "task": {
            "action": "WINDOWS_POWERSHELL_JOB_OWNER",
            "logon_type": "S4U",
            "multiple_instances": "IGNORE_NEW",
            "principal": "CURRENT_USER",
            "run_level": "LEAST_PRIVILEGE",
            "task_name": f"FIMV2-TransportCanary-{transport_id[:16]}",
            "triggers": [],
        },
        "transport": {**transport_basis, "transport_id": transport_id},
    }
    return {**core, "plan_id": sha256_json(core)}


def build_pending_canary_approval(
    plan: Mapping[str, object],
) -> dict[str, object]:
    plan_id = _identity(plan, "plan_id")
    return {
        "approval_receipt_id": None,
        "approved_at": None,
        "operation": OPERATION,
        "plan_id": plan_id,
        "plan_sha256": sha256_json(plan),
        "schema_version": CANARY_APPROVAL_SCHEMA,
        "status": "PENDING",
        "user_authorization_id": None,
    }


def validate_canary_plan(
    repository_root: Path, plan: Mapping[str, object]
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    _identity(plan, "plan_id")
    transport = plan.get("transport")
    if not isinstance(transport, dict):
        raise IntegrityError("canary transport binding is absent")
    diagnosis_path = _contained(
        root, transport.get("diagnosis_path"), "diagnosis path"
    )
    expected = build_canary_plan(root, diagnosis_path)
    if plan != expected:
        raise IntegrityError("transport canary plan differs from exact bindings")
    if (root / ACTIVE_ROOT).exists():
        raise IntegrityError("transport canary requires data/active to remain absent")
    return expected


def verify_canary_approval(
    approval: Mapping[str, object], plan: Mapping[str, object]
) -> str:
    core_keys = {
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    core = {key: approval[key] for key in core_keys if key in approval}
    if (
        set(approval) != {*core_keys, "approval_receipt_id"}
        or approval.get("schema_version") != CANARY_APPROVAL_SCHEMA
        or approval.get("status") != "APPROVED"
        or approval.get("operation") != OPERATION
        or approval.get("plan_id") != plan.get("plan_id")
        or approval.get("plan_sha256") != sha256_json(plan)
        or approval.get("approval_receipt_id") != sha256_json(core)
    ):
        raise UnauthorizedOperation(
            "transport canary lacks exact hash-bound approval"
        )
    _parse_utc(approval.get("approved_at"), "canary approval time")
    authorization_id = approval.get("user_authorization_id")
    if (
        not isinstance(authorization_id, str)
        or len(authorization_id) != 64
        or any(character not in "0123456789abcdef" for character in authorization_id)
    ):
        raise UnauthorizedOperation("canary user authorization ID is invalid")
    return str(approval["approval_receipt_id"])


def _plan_paths(root: Path, plan: Mapping[str, object]) -> dict[str, Path]:
    outputs = plan.get("outputs")
    if not isinstance(outputs, dict):
        raise IntegrityError("canary outputs are absent")
    paths: dict[str, Path] = {}
    for key in (
        "containment_path",
        "heartbeat_path",
        "launch_path",
        "start_path",
        "temporary_heartbeat_path",
        "terminal_path",
    ):
        paths[key] = _contained(root, outputs.get(key), f"canary {key}")
    return paths


def describe_canary(
    *,
    repository_root: Path,
    plan_path: Path,
    approval_path: Path,
    require_approved: bool,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    dependency_receipt_id = require_locked_repository_environment(root)
    plan = _load_canonical(plan_path, "transport canary plan")
    validate_canary_plan(root, plan)
    approval = _load_canonical(approval_path, "transport canary approval")
    approval_receipt_id: str | None = None
    if require_approved:
        approval_receipt_id = verify_canary_approval(approval, plan)
    elif (
        approval.get("status") != "PENDING"
        or approval.get("plan_id") != plan.get("plan_id")
        or approval.get("plan_sha256") != sha256_json(plan)
    ):
        raise IntegrityError("pending canary approval differs from plan")
    paths = _plan_paths(root, plan)
    for key in (
        "containment_path",
        "heartbeat_path",
        "launch_path",
        "start_path",
        "terminal_path",
        "temporary_heartbeat_path",
    ):
        if paths[key].exists():
            raise IntegrityError(f"create-only canary output exists: {key}")
    task = plan["task"]
    transport = plan["transport"]
    limits = plan["limits"]
    return {
        "approval_receipt_id": approval_receipt_id,
        "dependency_lock_receipt_id": dependency_receipt_id,
        "plan_id": plan["plan_id"],
        "scheduler_duration_seconds": limits["scheduler_duration_seconds"],
        "task_name": task["task_name"],
        "transport_id": transport["transport_id"],
    }


def verify_approved_descriptor(
    *,
    repository_root: Path,
    plan_path: Path,
    approval_path: Path,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    dependency_receipt_id = require_locked_repository_environment(root)
    plan = _load_canonical(plan_path, "transport canary plan")
    validate_canary_plan(root, plan)
    approval = _load_canonical(approval_path, "transport canary approval")
    approval_receipt_id = verify_canary_approval(approval, plan)
    return {
        "approval_receipt_id": approval_receipt_id,
        "dependency_lock_receipt_id": dependency_receipt_id,
        "plan_id": plan["plan_id"],
        "scheduler_duration_seconds": plan["limits"][
            "scheduler_duration_seconds"
        ],
        "task_name": plan["task"]["task_name"],
        "transport_id": plan["transport"]["transport_id"],
    }


class _BasicLimitInformation(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_longlong),
        ("PerJobUserTimeLimit", ctypes.c_longlong),
        ("LimitFlags", ctypes.c_ulong),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", ctypes.c_ulong),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", ctypes.c_ulong),
        ("SchedulingClass", ctypes.c_ulong),
    ]


class _IoCounters(ctypes.Structure):
    _fields_ = [
        ("ReadOperationCount", ctypes.c_ulonglong),
        ("WriteOperationCount", ctypes.c_ulonglong),
        ("OtherOperationCount", ctypes.c_ulonglong),
        ("ReadTransferCount", ctypes.c_ulonglong),
        ("WriteTransferCount", ctypes.c_ulonglong),
        ("OtherTransferCount", ctypes.c_ulonglong),
    ]


class _ExtendedLimitInformation(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _BasicLimitInformation),
        ("IoInfo", _IoCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


class WindowsKillOnCloseJob:
    """Own a Windows Job Object that terminates its process tree on close."""

    def __init__(self) -> None:
        if os.name != "nt":
            raise IntegrityError("Windows job transport requires Windows")
        self._kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self._kernel32.CreateJobObjectW.argtypes = (ctypes.c_void_p, ctypes.c_wchar_p)
        self._kernel32.CreateJobObjectW.restype = ctypes.c_void_p
        self._kernel32.SetInformationJobObject.argtypes = (
            ctypes.c_void_p,
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_ulong,
        )
        self._kernel32.SetInformationJobObject.restype = ctypes.c_int
        self._kernel32.AssignProcessToJobObject.argtypes = (
            ctypes.c_void_p,
            ctypes.c_void_p,
        )
        self._kernel32.AssignProcessToJobObject.restype = ctypes.c_int
        self._kernel32.IsProcessInJob.argtypes = (
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_int),
        )
        self._kernel32.IsProcessInJob.restype = ctypes.c_int
        self._kernel32.TerminateJobObject.argtypes = (ctypes.c_void_p, ctypes.c_uint)
        self._kernel32.TerminateJobObject.restype = ctypes.c_int
        self._kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
        self._kernel32.CloseHandle.restype = ctypes.c_int
        handle = self._kernel32.CreateJobObjectW(None, None)
        if not handle:
            raise IntegrityError(
                f"CreateJobObjectW failed: {ctypes.get_last_error()}"
            )
        self._handle = handle
        information = _ExtendedLimitInformation()
        information.BasicLimitInformation.LimitFlags = (
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        )
        if not self._kernel32.SetInformationJobObject(
            self._handle,
            JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
            ctypes.byref(information),
            ctypes.sizeof(information),
        ):
            error = ctypes.get_last_error()
            self.close()
            raise IntegrityError(f"SetInformationJobObject failed: {error}")

    def assign(self, process: subprocess.Popen[bytes]) -> None:
        handle = ctypes.c_void_p(int(process._handle))  # type: ignore[attr-defined]
        if not self._kernel32.AssignProcessToJobObject(self._handle, handle):
            raise IntegrityError(
                f"AssignProcessToJobObject failed: {ctypes.get_last_error()}"
            )
        contained = ctypes.c_int()
        if not self._kernel32.IsProcessInJob(
            handle, self._handle, ctypes.byref(contained)
        ) or not contained.value:
            raise IntegrityError("child process is not in the kill-on-close job")

    def terminate(self, exit_code: int = 1) -> None:
        if self._handle and not self._kernel32.TerminateJobObject(
            self._handle, exit_code
        ):
            raise IntegrityError(
                f"TerminateJobObject failed: {ctypes.get_last_error()}"
            )

    def close(self) -> None:
        if getattr(self, "_handle", None):
            handle = self._handle
            self._handle = None
            if not self._kernel32.CloseHandle(handle):
                raise IntegrityError(
                    f"CloseHandle(job) failed: {ctypes.get_last_error()}"
                )

    def __enter__(self) -> WindowsKillOnCloseJob:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _process_alive(process_id: int) -> bool:
    if os.name != "nt":
        try:
            os.kill(process_id, 0)
        except OSError:
            return False
        return True
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = (ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong)
    open_process.restype = ctypes.c_void_p
    get_exit_code = kernel32.GetExitCodeProcess
    get_exit_code.argtypes = (ctypes.c_void_p, ctypes.POINTER(ctypes.c_ulong))
    get_exit_code.restype = ctypes.c_int
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (ctypes.c_void_p,)
    close_handle.restype = ctypes.c_int
    handle = open_process(0x1000, 0, process_id)
    if not handle:
        return False
    try:
        code = ctypes.c_ulong()
        return bool(get_exit_code(handle, ctypes.byref(code))) and code.value == STILL_ACTIVE
    finally:
        close_handle(handle)


def _probe_child() -> int:
    release = sys.stdin.buffer.read(1)
    if release != b"G":
        return 2
    while True:
        time.sleep(60)


def _probe_command(mode: str) -> tuple[tuple[str, ...], dict[str, str]]:
    root = Path(__file__).resolve().parents[2]
    executable = Path(getattr(sys, "_base_executable", sys.executable)).resolve(
        strict=True
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(root / "src")
    return (
        (
            str(executable),
            "-m",
            "futures_rebuild.durable_windows_task_transport",
            mode,
        ),
        environment,
    )


def _probe_owner() -> int:
    command, environment = _probe_command("probe-child")
    child: subprocess.Popen[bytes] | None = None
    job: WindowsKillOnCloseJob | None = None
    try:
        job = WindowsKillOnCloseJob()
        child = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=environment,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        job.assign(child)
        if child.stdin is None:
            raise IntegrityError("probe child release pipe is absent")
        child.stdin.write(b"G")
        child.stdin.flush()
        child.stdin.close()
        print(
            canonical_bytes(
                {
                    "child_process_id": child.pid,
                    "job_membership": "PASS",
                    "owner_process_id": os.getpid(),
                }
            ).decode(),
            flush=True,
        )
        while True:
            time.sleep(60)
    finally:
        if job is not None:
            try:
                job.terminate()
            except BaseException:
                pass
            try:
                job.close()
            except BaseException:
                pass
        if child is not None and child.poll() is None:
            child.kill()


def exercise_containment() -> dict[str, object]:
    command, environment = _probe_command("probe-owner")
    owner = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    line_queue: queue.Queue[bytes] = queue.Queue(maxsize=1)

    def _read_ready() -> None:
        if owner.stdout is not None:
            line_queue.put(owner.stdout.readline())

    reader = threading.Thread(target=_read_ready, daemon=True)
    reader.start()
    child_pid: int | None = None
    try:
        try:
            line = line_queue.get(timeout=PROBE_START_TIMEOUT_SECONDS)
        except queue.Empty as exc:
            raise IntegrityError("containment probe owner did not become ready") from exc
        try:
            ready = json.loads(line)
        except (UnicodeDecodeError, ValueError) as exc:
            raise IntegrityError("containment probe ready output is invalid") from exc
        if (
            not isinstance(ready, dict)
            or ready.get("job_membership") != "PASS"
            or type(ready.get("child_process_id")) is not int
            or ready.get("owner_process_id") != owner.pid
        ):
            raise IntegrityError("containment probe did not prove job membership")
        child_pid = int(ready["child_process_id"])
        if not _process_alive(child_pid):
            raise IntegrityError("contained probe child exited before owner termination")
        owner.terminate()
        owner.wait(timeout=PROBE_EXIT_TIMEOUT_SECONDS)
        deadline = time.monotonic() + PROBE_EXIT_TIMEOUT_SECONDS
        while _process_alive(child_pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        if _process_alive(child_pid):
            raise IntegrityError("kill-on-close left the probe child alive")
        core: dict[str, object] = {
            "child_process_id": child_pid,
            "job_limit": "JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE",
            "job_membership": "PASS",
            "owner_exit_code": owner.returncode,
            "owner_process_id": owner.pid,
            "schema_version": CANARY_CONTAINMENT_SCHEMA,
            "status": "PASS",
        }
        return {**core, "containment_result_id": sha256_json(core)}
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.wait(timeout=PROBE_EXIT_TIMEOUT_SECONDS)
        if child_pid is not None and _process_alive(child_pid):
            raise IntegrityError("containment probe cleanup left a child alive")


def _output_bytes(paths: Mapping[str, Path]) -> int:
    parents = {path.parent for path in paths.values()}
    files: set[Path] = set()
    for parent in parents:
        if parent.exists():
            files.update(path for path in parent.rglob("*") if path.is_file())
    return sum(path.stat().st_size for path in files)


def build_containment_result(
    *,
    owner_process_id: int,
    child_process_id: int,
    child_exit_code: int,
) -> dict[str, object]:
    if (
        owner_process_id <= 0
        or child_process_id <= 0
        or child_exit_code == STILL_ACTIVE
        or _process_alive(child_process_id)
    ):
        raise IntegrityError("runner containment evidence is invalid")
    core: dict[str, object] = {
        "child_exit_code": child_exit_code,
        "child_process_id": child_process_id,
        "job_limit": "JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE",
        "job_membership": "PASS",
        "owner_process_id": owner_process_id,
        "schema_version": CANARY_CONTAINMENT_SCHEMA,
        "status": "PASS",
    }
    return {**core, "containment_result_id": sha256_json(core)}


def run_canary(
    *,
    repository_root: Path,
    plan_path: Path,
    approval_path: Path,
    containment_result: Mapping[str, object] | None = None,
) -> int:
    root = repository_root.resolve(strict=True)
    launch = describe_canary(
        repository_root=root,
        plan_path=plan_path,
        approval_path=approval_path,
        require_approved=True,
    )
    plan = _load_canonical(plan_path, "transport canary plan")
    approval = _load_canonical(approval_path, "transport canary approval")
    approval_receipt_id = verify_canary_approval(approval, plan)
    paths = _plan_paths(root, plan)
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    start_core: dict[str, object] = {
        "approval_receipt_id": approval_receipt_id,
        "plan_id": plan["plan_id"],
        "process_id": os.getpid(),
        "schema_version": CANARY_START_SCHEMA,
        "started_at": started_at,
        "status": "STARTED",
        "task_name": launch["task_name"],
        "transport_id": launch["transport_id"],
    }
    _write_new(
        paths["start_path"],
        {**start_core, "canary_start_id": sha256_json(start_core)},
    )
    sequence = 0
    terminal_status = "FAILED_FAIL_CLOSED"
    failure_type: str | None = None
    containment_id: str | None = None
    caught: BaseException | None = None
    try:
        containment = (
            exercise_containment()
            if containment_result is None
            else dict(containment_result)
        )
        _identity(containment, "containment_result_id")
        if (
            containment.get("schema_version") != CANARY_CONTAINMENT_SCHEMA
            or containment.get("status") != "PASS"
            or containment.get("job_membership") != "PASS"
        ):
            raise IntegrityError("containment result is not a pass")
        containment_id = str(containment["containment_result_id"])
        _write_new(paths["containment_path"], containment)
        while True:
            elapsed = time.monotonic() - started_monotonic
            sequence += 1
            heartbeat_core: dict[str, object] = {
                "elapsed_seconds": format(elapsed, ".3f"),
                "observed_at": _utc_now(),
                "plan_id": plan["plan_id"],
                "process_id": os.getpid(),
                "schema_version": CANARY_HEARTBEAT_SCHEMA,
                "sequence": sequence,
                "status": "RUNNING",
                "transport_id": launch["transport_id"],
            }
            _write_heartbeat(
                paths["heartbeat_path"],
                paths["temporary_heartbeat_path"],
                {
                    **heartbeat_core,
                    "heartbeat_id": sha256_json(heartbeat_core),
                },
            )
            if _output_bytes(paths) > MAXIMUM_OUTPUT_BYTES:
                raise IntegrityError("transport canary output ceiling reached")
            if elapsed >= CANARY_DURATION_SECONDS:
                break
            time.sleep(HEARTBEAT_INTERVAL_SECONDS)
        terminal_status = "PASS"
    except BaseException as exc:
        caught = exc
        failure_type = type(exc).__name__
        terminal_status = "INTERRUPTED_FAIL_CLOSED"
    finally:
        completed_at = _utc_now()
        elapsed = time.monotonic() - started_monotonic
        terminal_core: dict[str, object] = {
            "approval_receipt_id": approval_receipt_id,
            "completed_at": completed_at,
            "containment_result_id": containment_id,
            "elapsed_seconds": format(elapsed, ".3f"),
            "failure_type": failure_type,
            "plan_id": plan["plan_id"],
            "schema_version": CANARY_TERMINAL_SCHEMA,
            "started_at": started_at,
            "status": terminal_status,
            "task_name": launch["task_name"],
            "transport_id": launch["transport_id"],
        }
        terminal = {
            **terminal_core,
            "canary_terminal_id": sha256_json(terminal_core),
        }
        if not paths["terminal_path"].exists():
            _write_new(paths["terminal_path"], terminal)
        sequence += 1
        heartbeat_core = {
            "canary_terminal_id": terminal["canary_terminal_id"],
            "elapsed_seconds": format(elapsed, ".3f"),
            "observed_at": completed_at,
            "plan_id": plan["plan_id"],
            "process_id": os.getpid(),
            "schema_version": CANARY_HEARTBEAT_SCHEMA,
            "sequence": sequence,
            "status": terminal_status,
            "transport_id": launch["transport_id"],
        }
        _write_heartbeat(
            paths["heartbeat_path"],
            paths["temporary_heartbeat_path"],
            {
                **heartbeat_core,
                "heartbeat_id": sha256_json(heartbeat_core),
            },
        )
    if caught is not None:
        return 1
    return 0 if terminal_status == "PASS" else 1


def record_launch(
    *,
    repository_root: Path,
    plan_path: Path,
    approval_path: Path,
    task_xml_sha256: str,
    launch_requested_at: str,
    launcher_returned_at: str,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    plan = _load_canonical(plan_path, "transport canary plan")
    validate_canary_plan(root, plan)
    approval = _load_canonical(approval_path, "transport canary approval")
    approval_receipt_id = verify_canary_approval(approval, plan)
    _parse_utc(launch_requested_at, "launch request time")
    _parse_utc(launcher_returned_at, "launcher return time")
    if (
        len(task_xml_sha256) != 64
        or any(character not in "0123456789abcdef" for character in task_xml_sha256)
    ):
        raise IntegrityError("task XML hash is invalid")
    task = plan["task"]
    transport = plan["transport"]
    core: dict[str, object] = {
        "approval_receipt_id": approval_receipt_id,
        "launcher_returned_at": launcher_returned_at,
        "launch_requested_at": launch_requested_at,
        "plan_id": plan["plan_id"],
        "schema_version": CANARY_LAUNCH_SCHEMA,
        "status": "START_REQUESTED",
        "task_name": task["task_name"],
        "task_xml_sha256": task_xml_sha256,
        "transport_id": transport["transport_id"],
    }
    result = {**core, "launch_receipt_id": sha256_json(core)}
    _write_new(_plan_paths(root, plan)["launch_path"], result)
    return result


def reconcile_postmortem(
    *,
    repository_root: Path,
    plan_path: Path,
    approval_path: Path,
    scheduler_state: str,
    last_task_result: int,
    last_run_at: str,
    next_run_absent: bool,
    observed_at: str | None = None,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    plan = _load_canonical(plan_path, "transport canary plan")
    validate_canary_plan(root, plan)
    approval = _load_canonical(approval_path, "transport canary approval")
    approval_receipt_id = verify_canary_approval(approval, plan)
    paths = _plan_paths(root, plan)
    if paths["terminal_path"].exists():
        terminal = _load_canonical(paths["terminal_path"], "canary terminal")
        _identity(terminal, "canary_terminal_id")
        return terminal
    if scheduler_state.upper() == "RUNNING" or not next_run_absent:
        raise IntegrityError("canary scheduler state is not terminal and inert")
    heartbeat = _load_canonical(paths["heartbeat_path"], "canary heartbeat")
    _identity(heartbeat, "heartbeat_id")
    process_id = heartbeat.get("process_id")
    if type(process_id) is not int or _process_alive(int(process_id)):
        raise IntegrityError("canary process remains active")
    observed = _utc_now() if observed_at is None else observed_at
    observed_dt = _parse_utc(observed, "postmortem observation time")
    heartbeat_dt = _parse_utc(heartbeat.get("observed_at"), "heartbeat time")
    if (observed_dt - heartbeat_dt).total_seconds() < HEARTBEAT_INTERVAL_SECONDS * 3:
        raise IntegrityError("canary heartbeat is not yet conclusively stalled")
    _parse_utc(last_run_at, "scheduler last-run time")
    launch_receipt_id: str | None = None
    if paths["launch_path"].exists():
        launch = _load_canonical(paths["launch_path"], "canary launch receipt")
        launch_receipt_id = _identity(launch, "launch_receipt_id")
    core: dict[str, object] = {
        "approval_receipt_id": approval_receipt_id,
        "completed_at": observed,
        "containment_result_id": None,
        "elapsed_seconds": None,
        "failure_type": "SCHEDULER_ACTION_EXIT_WITHOUT_TERMINAL",
        "last_heartbeat_id": heartbeat["heartbeat_id"],
        "launch_receipt_id": launch_receipt_id,
        "plan_id": plan["plan_id"],
        "postmortem": {
            "last_run_at": last_run_at,
            "last_task_result": last_task_result,
            "next_run_absent": next_run_absent,
            "scheduler_state": scheduler_state,
        },
        "schema_version": CANARY_TERMINAL_SCHEMA,
        "started_at": None,
        "status": "POSTMORTEM_INTERRUPTED_FAIL_CLOSED",
        "task_name": plan["task"]["task_name"],
        "transport_id": plan["transport"]["transport_id"],
    }
    terminal = {**core, "canary_terminal_id": sha256_json(core)}
    _write_new(paths["terminal_path"], terminal)
    return terminal


def approval_text(plan: Mapping[str, object]) -> str:
    transport = plan["transport"]
    task = plan["task"]
    limits = plan["limits"]
    return (
        f"APPROVE TRANSPORT CANARY PLAN {plan['plan_id']} WITH PLAN SHA256 "
        f"{sha256_json(plan)} TRANSPORT {transport['transport_id']} TASK "
        f"{task['task_name']} FOR AT MOST {limits['maximum_processes']} "
        f"PROCESSES, {limits['canary_duration_seconds']} CANARY SECONDS, "
        f"{limits['scheduler_duration_seconds']} SCHEDULER SECONDS, AND "
        f"{limits['maximum_output_bytes']} OUTPUT BYTES; PRESERVE V10 "
        f"INTERRUPTION {transport['v10_interruption_id']}; AUTHORIZE ONLY "
        "THE ONE-SHOT NONRECURRING S4U TASK REGISTRATION, HARMLESS "
        "CROSS-TASK TRANSPORT CANARY, CREATE-ONLY CANARY EVIDENCE, AND "
        "FAIL-CLOSED POSTMORTEM RECONCILIATION, RETAINING ALL INERT TASK "
        "ENTRIES FOR SEPARATE REVIEWED CLEANUP; NO PROVIDER CALLS, SOURCE "
        "PAYLOAD, HOLDOUT/FORWARD, OUTCOME, MODEL, CANDIDATE CERTIFICATION, "
        "DATA/ACTIVE MUTATION, MATERIALIZATION, PUBLICATION, TRADING, "
        "RETRY, DELETION, ARCHIVE, STAGE 6 SUCCESSOR, COMMIT, OR PUSH."
    )


def generate_bundle(
    *,
    repository_root: Path,
    diagnosis_output: Path,
    plan_output: Path,
    approval_output: Path,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    require_locked_repository_environment(root)
    if (root / ACTIVE_ROOT).exists():
        raise IntegrityError("bundle generation requires data/active to remain absent")
    diagnosis = build_v10_diagnosis(root)
    _write_new_or_exact(diagnosis_output, diagnosis)
    plan = build_canary_plan(root, diagnosis_output)
    approval = build_pending_canary_approval(plan)
    _write_new_or_exact(plan_output, plan)
    _write_new_or_exact(approval_output, approval)
    return {
        "approval_status": "PENDING",
        "approval_text": approval_text(plan),
        "diagnosis_id": diagnosis["diagnosis_id"],
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_json(plan),
        "task_name": plan["task"]["task_name"],
        "transport_id": plan["transport"]["transport_id"],
    }


def _resolve(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else root / path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate")
    generate.add_argument("--repository-root", type=Path, default=Path.cwd())
    generate.add_argument("--diagnosis-output", type=Path, required=True)
    generate.add_argument("--plan-output", type=Path, required=True)
    generate.add_argument("--approval-output", type=Path, required=True)

    for name in (
        "describe",
        "preflight",
        "verify-approved",
        "run",
        "record-launch",
        "reconcile",
    ):
        child = subparsers.add_parser(name)
        child.add_argument("--repository-root", type=Path, default=Path.cwd())
        child.add_argument("--plan", type=Path, required=True)
        child.add_argument("--approval", type=Path, required=True)
        if name == "record-launch":
            child.add_argument("--task-xml-sha256", required=True)
            child.add_argument("--launch-requested-at", required=True)
            child.add_argument("--launcher-returned-at", required=True)
        if name == "reconcile":
            child.add_argument("--scheduler-state", required=True)
            child.add_argument("--last-task-result", type=int, required=True)
            child.add_argument("--last-run-at", required=True)
            child.add_argument(
                "--next-run-absent", choices=("true", "false"), required=True
            )
        if name == "run":
            child.add_argument(
                "--containment-owner-process-id", type=int, required=True
            )
            child.add_argument(
                "--containment-child-process-id", type=int, required=True
            )
            child.add_argument(
                "--containment-child-exit-code", type=int, required=True
            )

    subparsers.add_parser("probe-owner")
    subparsers.add_parser("probe-child")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "probe-owner":
        return _probe_owner()
    if args.command == "probe-child":
        return _probe_child()
    root = args.repository_root.resolve(strict=True)
    if args.command == "generate":
        result = generate_bundle(
            repository_root=root,
            diagnosis_output=_resolve(root, args.diagnosis_output),
            plan_output=_resolve(root, args.plan_output),
            approval_output=_resolve(root, args.approval_output),
        )
    else:
        plan_path = _resolve(root, args.plan)
        approval_path = _resolve(root, args.approval)
        if args.command in {"describe", "preflight"}:
            result = describe_canary(
                repository_root=root,
                plan_path=plan_path,
                approval_path=approval_path,
                require_approved=args.command == "preflight",
            )
        elif args.command == "verify-approved":
            result = verify_approved_descriptor(
                repository_root=root,
                plan_path=plan_path,
                approval_path=approval_path,
            )
        elif args.command == "run":
            containment = build_containment_result(
                owner_process_id=args.containment_owner_process_id,
                child_process_id=args.containment_child_process_id,
                child_exit_code=args.containment_child_exit_code,
            )
            return run_canary(
                repository_root=root,
                plan_path=plan_path,
                approval_path=approval_path,
                containment_result=containment,
            )
        elif args.command == "record-launch":
            result = record_launch(
                repository_root=root,
                plan_path=plan_path,
                approval_path=approval_path,
                task_xml_sha256=args.task_xml_sha256,
                launch_requested_at=args.launch_requested_at,
                launcher_returned_at=args.launcher_returned_at,
            )
        else:
            result = reconcile_postmortem(
                repository_root=root,
                plan_path=plan_path,
                approval_path=approval_path,
                scheduler_state=args.scheduler_state,
                last_task_result=args.last_task_result,
                last_run_at=args.last_run_at,
                next_run_absent=args.next_run_absent == "true",
            )
    print(canonical_bytes(result).decode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
