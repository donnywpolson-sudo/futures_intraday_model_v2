"""Durable, auditable Windows host boundary for V10 causal market work."""

from __future__ import annotations

import contextlib
import json
import os
import sys
import threading
import time
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TypeVar

from .canonical import canonical_bytes, fsync_directory, sha256_json
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .locking import _local_process_alive


DURABLE_HOST_KIND = "WINDOWS_TASK_SCHEDULER_CURRENT_USER"
DURABLE_HOST_TASK_NAME_PREFIX = "FIMV2-Causal-V10"
DURABLE_HOST_LAUNCHER_PATH = "scripts/start_causal_full_build_v10_worker.ps1"
DURABLE_HOST_EVIDENCE_ROOT = (
    "state/causal_full_build_host/causal_observation_full_development_bounded_2025_v10"
)
DURABLE_HOST_ENVIRONMENT_KEY = "FUTURES_CAUSAL_FULL_BUILD_TASK_NAME"
DURABLE_HOST_HEARTBEAT_INTERVAL_SECONDS = 60
DURABLE_HOST_LIVE_MAX_AGE_SECONDS = 120
DURABLE_HOST_STALE_AFTER_SECONDS = 3_600
DURABLE_HOST_SCHEMA = "causal_full_build_durable_host/1.0.0"
DURABLE_HOST_STARTED_SCHEMA = "causal_full_build_host_started/1.0.0"
DURABLE_HOST_HEARTBEAT_SCHEMA = "causal_full_build_host_heartbeat/1.0.0"
DURABLE_HOST_EXIT_SCHEMA = "causal_full_build_host_exit/1.0.0"

T = TypeVar("T")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _host_io_path(path: Path) -> Path:
    """Return an extended-length absolute host-evidence path on Windows."""

    if os.name != "nt":
        return path
    value = str(path.resolve(strict=False))
    if value.startswith("\\\\?\\"):
        return Path(value)
    if value.startswith("\\\\"):
        return Path("\\\\?\\UNC\\" + value.lstrip("\\"))
    return Path("\\\\?\\" + value)


def _contained(root: Path, relative: object) -> Path:
    if type(relative) is not str or not relative:
        raise ContractError("durable-host evidence path is absent")
    value = Path(relative)
    if value.is_absolute() or ".." in value.parts or value.as_posix() != relative:
        raise ContractError("durable-host evidence path is not canonical and relative")
    candidate = (root / value).resolve()
    try:
        candidate.relative_to(root / "state/causal_full_build_host")
    except ValueError as exc:
        raise UnauthorizedOperation("durable-host evidence path is outside its state root") from exc
    return candidate


def expected_durable_host_plan(market: str, attempt_id: str) -> dict[str, object]:
    if not market or any(
        character not in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" for character in market
    ):
        raise ContractError("V10 durable-host market is invalid")
    if (
        len(attempt_id) != 64
        or attempt_id.lower() != attempt_id
        or any(character not in "0123456789abcdef" for character in attempt_id)
    ):
        raise ContractError("V10 durable-host attempt identity is invalid")
    task_name = f"{DURABLE_HOST_TASK_NAME_PREFIX}-{market}-{attempt_id[:8]}"
    return {
        "schema_version": DURABLE_HOST_SCHEMA,
        "kind": DURABLE_HOST_KIND,
        "task_name": task_name,
        "launcher_path": DURABLE_HOST_LAUNCHER_PATH,
        "evidence_path": f"{DURABLE_HOST_EVIDENCE_ROOT}/{market}/{attempt_id}",
        "heartbeat_interval_seconds": DURABLE_HOST_HEARTBEAT_INTERVAL_SECONDS,
        "stale_after_seconds": DURABLE_HOST_STALE_AFTER_SECONDS,
        "launch_mode": "SCHEDULED_SERVICE_TRIGGER_AFTER_LAUNCHER_EXIT",
        "minimum_trigger_delay_seconds": 120,
        "interactive_parent_independent": True,
        "task_overwrite_allowed": False,
        "automatic_restart_allowed": False,
    }


def validate_durable_host_plan(root: Path, plan: Mapping[str, object]) -> Path:
    durable = plan.get("durable_host")
    market = plan.get("target_market")
    attempt_id = plan.get("attempt_id")
    if (
        type(market) is not str
        or type(attempt_id) is not str
        or durable != expected_durable_host_plan(market, attempt_id)
    ):
        raise UnauthorizedOperation("V10 durable-host plan is absent or differs")
    launcher = root / DURABLE_HOST_LAUNCHER_PATH
    if not launcher.is_file():
        raise IntegrityError("V10 durable-host launcher is absent")
    return _contained(root, durable["evidence_path"])


def validate_durable_host_environment(root: Path, plan: Mapping[str, object]) -> Path:
    evidence = validate_durable_host_plan(root, plan)
    durable = plan["durable_host"]
    if (
        os.name != "nt"
        or os.environ.get(DURABLE_HOST_ENVIRONMENT_KEY) != durable["task_name"]
    ):
        raise UnauthorizedOperation(
            "V10 causal work must run inside its exact Windows scheduled task"
        )
    return evidence


def validate_active_durable_host_evidence(root: Path, plan: Mapping[str, object]) -> None:
    """Require the current scheduled worker's live, hash-bound heartbeat."""

    evidence = validate_durable_host_environment(root, plan)
    started_path = _host_io_path(evidence / "started.json")
    heartbeat_path = _host_io_path(evidence / "heartbeat.json")
    exit_path = _host_io_path(evidence / "exit.json")
    if (
        not started_path.is_file()
        or not heartbeat_path.is_file()
        or exit_path.exists()
    ):
        raise UnauthorizedOperation("V10 durable-host live evidence is absent or terminal")
    try:
        started = json.loads(started_path.read_text(encoding="utf-8"))
        heartbeat = json.loads(heartbeat_path.read_text(encoding="utf-8"))
        started_core = {key: value for key, value in started.items() if key != "started_id"}
        heartbeat_core = {
            key: value for key, value in heartbeat.items() if key != "heartbeat_id"
        }
        observed = datetime.fromisoformat(str(heartbeat["observed_at"]))
    except (OSError, UnicodeDecodeError, ValueError, KeyError, TypeError) as exc:
        raise IntegrityError("V10 durable-host live evidence is invalid") from exc
    current_pid = os.getpid()
    current_time = _utc_now()
    if (
        set(started)
        != {
            "schema_version",
            "status",
            "plan_id",
            "task_name",
            "pid",
            "parent_pid",
            "started_at",
            "interactive_parent_independent",
            "started_id",
        }
        or set(heartbeat)
        != {
            "schema_version",
            "status",
            "plan_id",
            "pid",
            "sequence",
            "observed_at",
            "heartbeat_id",
        }
        or started.get("schema_version") != DURABLE_HOST_STARTED_SCHEMA
        or started.get("status") != "STARTED"
        or started.get("plan_id") != plan.get("plan_id")
        or started.get("task_name") != plan["durable_host"]["task_name"]
        or started.get("pid") != current_pid
        or started.get("started_id") != sha256_json(started_core)
        or heartbeat.get("schema_version") != DURABLE_HOST_HEARTBEAT_SCHEMA
        or heartbeat.get("status") != "RUNNING"
        or heartbeat.get("plan_id") != plan.get("plan_id")
        or heartbeat.get("pid") != current_pid
        or heartbeat.get("heartbeat_id") != sha256_json(heartbeat_core)
        or observed.tzinfo is None
        or observed > current_time + timedelta(seconds=5)
        or current_time - observed
        > timedelta(seconds=DURABLE_HOST_LIVE_MAX_AGE_SECONDS)
    ):
        raise UnauthorizedOperation("V10 durable-host live evidence differs or is stale")


def _write_create_only(path: Path, value: Mapping[str, object]) -> None:
    io_path = _host_io_path(path)
    io_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        io_path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, canonical_bytes(dict(value)) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(io_path.parent)


def _write_atomic(path: Path, value: Mapping[str, object]) -> None:
    io_path = _host_io_path(path)
    temporary = io_path.with_name(f".{io_path.name}.{os.getpid()}.tmp")
    if temporary.exists():
        raise IntegrityError("durable-host heartbeat temporary path already exists")
    try:
        descriptor = os.open(
            temporary,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        )
        try:
            os.write(descriptor, canonical_bytes(dict(value)) + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        for attempt in range(20):
            try:
                os.replace(temporary, io_path)
                break
            except PermissionError:
                if attempt == 19:
                    raise
                time.sleep(0.01)
        fsync_directory(io_path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


class _Heartbeat:
    def __init__(self, path: Path, *, plan_id: str, interval_seconds: float) -> None:
        self.path = path
        self.plan_id = plan_id
        self.interval_seconds = interval_seconds
        self.stop = threading.Event()
        self.sequence = 0
        self.failure: BaseException | None = None
        self.thread = threading.Thread(target=self._run, name="causal-v10-heartbeat", daemon=True)

    def _write(self, status: str) -> None:
        self.sequence += 1
        core = {
            "schema_version": DURABLE_HOST_HEARTBEAT_SCHEMA,
            "status": status,
            "plan_id": self.plan_id,
            "pid": os.getpid(),
            "sequence": self.sequence,
            "observed_at": _utc_now().isoformat(),
        }
        _write_atomic(self.path, {**core, "heartbeat_id": sha256_json(core)})

    def _run(self) -> None:
        while not self.stop.wait(self.interval_seconds):
            try:
                self._write("RUNNING")
            except BaseException as exc:
                self.failure = exc
                self.stop.set()
                return

    def start(self) -> None:
        self._write("RUNNING")
        self.thread.start()

    def finish(self) -> None:
        self.stop.set()
        self.thread.join(timeout=max(5.0, self.interval_seconds + 1.0))
        if self.thread.is_alive():
            raise IntegrityError("V10 durable-host heartbeat thread did not stop")
        if self.failure is not None:
            raise IntegrityError("V10 durable-host heartbeat writer failed") from self.failure
        self._write("TERMINAL")


def run_durable_full_build_worker(
    *, repository_root: Path, plan: Mapping[str, object], operation: Callable[[], T]
) -> T:
    """Run one packet-bound operation with durable host evidence and logs."""

    root = repository_root.resolve(strict=True)
    evidence = validate_durable_host_environment(root, plan)
    evidence_io = _host_io_path(evidence)
    if evidence_io.exists():
        raise IntegrityError("V10 durable-host evidence path already exists")
    evidence_io.mkdir(parents=True)
    started_at = _utc_now()
    plan_id = str(plan.get("plan_id", ""))
    started_core = {
        "schema_version": DURABLE_HOST_STARTED_SCHEMA,
        "status": "STARTED",
        "plan_id": plan_id,
        "task_name": plan["durable_host"]["task_name"],
        "pid": os.getpid(),
        "parent_pid": os.getppid(),
        "started_at": started_at.isoformat(),
        "interactive_parent_independent": True,
    }
    _write_create_only(
        evidence / "started.json", {**started_core, "started_id": sha256_json(started_core)}
    )
    heartbeat = _Heartbeat(
        evidence / "heartbeat.json",
        plan_id=plan_id,
        interval_seconds=DURABLE_HOST_HEARTBEAT_INTERVAL_SECONDS,
    )
    heartbeat.start()
    status = "FAILED"
    error_type: str | None = None
    error_message: str | None = None
    operation_error: BaseException | None = None
    operation_traceback = None
    result: T | None = None
    try:
        stdout_path = _host_io_path(evidence / "stdout.log")
        stderr_path = _host_io_path(evidence / "stderr.log")
        with stdout_path.open("x", encoding="utf-8") as stdout, stderr_path.open(
            "x", encoding="utf-8"
        ) as stderr, contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(
            stderr
        ):
            result = operation()
            status = "PASS"
    except BaseException as exc:
        error_type = type(exc).__name__
        error_message = str(exc)
        operation_error = exc
        operation_traceback = sys.exc_info()[2]
    heartbeat_error: BaseException | None = None
    try:
        heartbeat.finish()
    except BaseException as exc:
        heartbeat_error = exc
        status = "FAILED_HOST_EVIDENCE"
        if error_type is None:
            error_type = type(exc).__name__
            error_message = str(exc)
    exit_core = {
        "schema_version": DURABLE_HOST_EXIT_SCHEMA,
        "status": status,
        "plan_id": plan_id,
        "task_name": plan["durable_host"]["task_name"],
        "pid": os.getpid(),
        "started_at": started_at.isoformat(),
        "finished_at": _utc_now().isoformat(),
        "error_type": error_type,
        "error_message": error_message,
        "heartbeat_error_type": (
            type(heartbeat_error).__name__ if heartbeat_error is not None else None
        ),
        "automatic_restart_authorized": False,
    }
    _write_create_only(
        evidence / "exit.json", {**exit_core, "exit_id": sha256_json(exit_core)}
    )
    if operation_error is not None:
        raise operation_error.with_traceback(operation_traceback)
    if heartbeat_error is not None:
        raise heartbeat_error
    return result  # type: ignore[return-value]


def inspect_durable_full_build_worker(
    *, repository_root: Path, plan: Mapping[str, object], now: datetime | None = None
) -> dict[str, object]:
    """Classify V10 without opening DBNs or output rows."""

    root = repository_root.resolve(strict=True)
    evidence = validate_durable_host_plan(root, plan)
    started_path = _host_io_path(evidence / "started.json")
    heartbeat_path = _host_io_path(evidence / "heartbeat.json")
    exit_path = _host_io_path(evidence / "exit.json")
    relative = evidence.relative_to(root).as_posix()
    if not started_path.is_file():
        return {"status": "NOT_STARTED", "evidence_path": relative}
    started = json.loads(started_path.read_text(encoding="utf-8"))
    if exit_path.is_file():
        terminal = json.loads(exit_path.read_text(encoding="utf-8"))
        return {
            "status": f"TERMINAL_{terminal['status']}",
            "pid": started["pid"],
            "evidence_path": relative,
        }
    if not heartbeat_path.is_file():
        return {
            "status": "STARTING_OR_UNRECORDED",
            "pid": started["pid"],
            "evidence_path": relative,
        }
    heartbeat = json.loads(heartbeat_path.read_text(encoding="utf-8"))
    observed = datetime.fromisoformat(str(heartbeat["observed_at"]))
    age_seconds = max(0.0, ((now or _utc_now()) - observed).total_seconds())
    alive = _local_process_alive(int(started["pid"]))
    if age_seconds >= DURABLE_HOST_STALE_AFTER_SECONDS:
        status = "STALLED_PROCESS_ALIVE" if alive is True else "ABRUPT_TERMINATION_SUSPECTED"
    else:
        status = "RUNNING" if alive is True else "PROCESS_LOSS_BEFORE_STALE_THRESHOLD"
    return {
        "status": status,
        "pid": started["pid"],
        "process_alive": alive,
        "heartbeat_age_seconds": age_seconds,
        "evidence_path": relative,
    }
