"""Explicit one-writer leases with inspectable recovery evidence."""

from __future__ import annotations

import json
import os
import re
import socket
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .canonical import canonical_bytes, fsync_directory
from .errors import ContractError, IntegrityError, LeaseBusy, LeaseOwnershipError


MINIMUM_STALE_AGE = timedelta(minutes=5)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _local_process_alive(pid: int) -> bool | None:
    """Return False only when the local OS proves that a PID is absent."""

    if pid == os.getpid():
        return True
    if os.name == "nt":
        import ctypes

        process_query_limited_information = 0x1000
        still_active = 259
        error_invalid_parameter = 87
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
        handle = open_process(
            process_query_limited_information, False, pid
        )
        if handle:
            try:
                exit_code = ctypes.c_ulong()
                if not get_exit_code(handle, ctypes.byref(exit_code)):
                    return None
                return exit_code.value == still_active
            finally:
                close_handle(handle)
        if ctypes.get_last_error() == error_invalid_parameter:
            return False
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except (PermissionError, OSError):
        return None
    return True


@dataclass(frozen=True)
class LeaseRecord:
    token: str
    pid: int
    host: str
    acquired_at: str

    def as_dict(self) -> dict[str, object]:
        return {
            "acquired_at": self.acquired_at,
            "host": self.host,
            "pid": self.pid,
            "token": self.token,
        }


class FileLease:
    """An exclusive-create lock that never silently steals a stale lease."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._now = _utc_now()
        self.record = LeaseRecord(
            token=uuid.uuid4().hex,
            pid=os.getpid(),
            host=socket.gethostname(),
            acquired_at=self._now.astimezone(timezone.utc).isoformat(),
        )
        self._owned = False

    def acquire(self) -> "FileLease":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(
                self.path,
                os.O_CREAT
                | os.O_EXCL
                | os.O_WRONLY
                | getattr(os, "O_BINARY", 0),
            )
        except FileExistsError as exc:
            raise LeaseBusy(f"writer lease already exists: {self.path}") from exc
        try:
            os.write(descriptor, canonical_bytes(self.record.as_dict()) + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        fsync_directory(self.path.parent)
        self._owned = True
        return self

    def release(self) -> None:
        if not self._owned:
            raise LeaseOwnershipError("lease is not owned by this instance")
        current = self.inspect(self.path)
        if current.token != self.record.token:
            raise LeaseOwnershipError("lease token changed; refusing to remove it")
        self.path.unlink()
        fsync_directory(self.path.parent)
        self._owned = False

    def __enter__(self) -> "FileLease":
        return self.acquire()

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()

    @staticmethod
    def inspect(path: Path) -> LeaseRecord:
        try:
            raw = path.read_bytes()
            payload = json.loads(raw.decode("utf-8"))
            if (
                not isinstance(payload, dict)
                or set(payload) != {"acquired_at", "host", "pid", "token"}
                or not isinstance(payload["token"], str)
                or re.fullmatch(r"[0-9a-f]{32}", payload["token"]) is None
                or isinstance(payload["pid"], bool)
                or not isinstance(payload["pid"], int)
                or not isinstance(payload["host"], str)
                or not isinstance(payload["acquired_at"], str)
                or not payload["token"]
                or payload["pid"] <= 0
                or not payload["host"]
                or raw != canonical_bytes(payload) + b"\n"
            ):
                raise ValueError("lease schema/types are not exact")
            acquired = datetime.fromisoformat(payload["acquired_at"])
            if acquired.tzinfo is None or acquired.utcoffset() != timedelta(0):
                raise ValueError("lease acquisition time is not UTC")
            return LeaseRecord(
                token=payload["token"],
                pid=payload["pid"],
                host=payload["host"],
                acquired_at=payload["acquired_at"],
            )
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise IntegrityError(f"invalid lease record: {path}") from exc

    @classmethod
    def quarantine_stale(
        cls,
        path: Path,
        recovery_dir: Path,
        *,
        older_than: timedelta,
        expected_token: str,
    ) -> Path:
        """Quarantine a reviewed stale lock; token knowledge prevents blind stealing."""

        if not isinstance(older_than, timedelta) or older_than < MINIMUM_STALE_AGE:
            raise ContractError(
                f"stale recovery threshold must be at least {MINIMUM_STALE_AGE}"
            )
        if (
            not isinstance(expected_token, str)
            or re.fullmatch(r"[0-9a-f]{32}", expected_token) is None
        ):
            raise ContractError(
                "stale recovery requires the exact reviewed UUID-hex token"
            )
        observed = cls.inspect(path)
        if observed.token != expected_token:
            raise LeaseOwnershipError("reviewed token no longer matches current lease")
        if observed.host != socket.gethostname():
            raise LeaseBusy(
                "lease owner host differs; dead ownership cannot be proved locally"
            )
        owner_alive = _local_process_alive(observed.pid)
        if owner_alive is not False:
            raise LeaseBusy(
                "lease owner is alive or its death cannot be proved"
            )
        acquired = datetime.fromisoformat(observed.acquired_at).astimezone(timezone.utc)
        current = _utc_now().astimezone(timezone.utc)
        if acquired > current:
            raise IntegrityError("lease acquisition time is in the future")
        if current - acquired <= older_than:
            raise LeaseBusy("lease is not old enough for explicit recovery")
        recovery_dir.mkdir(parents=True, exist_ok=True)
        destination = recovery_dir / f"{path.name}.{observed.token}.stale"
        if destination.exists():
            raise IntegrityError(f"recovery evidence already exists: {destination}")
        os.replace(path, destination)
        fsync_directory(path.parent)
        fsync_directory(recovery_dir)
        return destination
