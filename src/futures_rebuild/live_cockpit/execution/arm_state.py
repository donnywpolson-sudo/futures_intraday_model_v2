"""Explicit, time-limited, non-persistent execution arm state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import threading

from .domain import AccountBinding, ExecutionMode, aware
from .errors import ExecutionBlocked


@dataclass(frozen=True)
class ArmSnapshot:
    armed: bool
    expires_at: datetime | None
    account_binding_id: str | None
    mode: ExecutionMode
    reason: str


class ArmState:
    """Memory-only arm state; constructing or reconnecting always disarms."""

    def __init__(self, *, mode: ExecutionMode = ExecutionMode.OBSERVATION_ONLY) -> None:
        self._lock = threading.RLock()
        self._mode = mode
        self._expires_at: datetime | None = None
        self._binding_id: str | None = None
        self._reason = "APPLICATION_START"

    def snapshot(self, *, now: datetime | None = None) -> ArmSnapshot:
        instant = datetime.now(timezone.utc) if now is None else aware(now, name="now")
        with self._lock:
            if self._expires_at is not None and instant >= self._expires_at:
                self._expires_at = None
                self._binding_id = None
                self._reason = "ARM_EXPIRED"
            armed = self._expires_at is not None and self._binding_id is not None
            return ArmSnapshot(armed, self._expires_at, self._binding_id, self._mode, self._reason)

    def arm(
        self,
        *,
        binding: AccountBinding,
        confirmation: str,
        now: datetime,
        duration_seconds: int = 300,
        production_readiness: bool,
    ) -> ArmSnapshot:
        now = aware(now, name="now")
        if not production_readiness:
            raise ExecutionBlocked("production readiness is false")
        if duration_seconds < 30 or duration_seconds > 900:
            raise ValueError("arm duration must be between 30 and 900 seconds")
        expected = f"ARM {binding.account_spec} {binding.account_stage}"
        if confirmation != expected:
            raise ExecutionBlocked("typed arm confirmation does not match the exact account and stage")
        if self._mode not in {ExecutionMode.MFF_TRADOVATE_SIM_FUNDED, ExecutionMode.MFF_TRADOVATE_LIVE}:
            raise ExecutionBlocked("current execution mode cannot be armed")
        with self._lock:
            self._expires_at = now + timedelta(seconds=duration_seconds)
            self._binding_id = binding.binding_id
            self._reason = "EXPLICIT_OPERATOR_ACTION"
        return self.snapshot(now=now)

    def disarm(self, reason: str) -> ArmSnapshot:
        if not reason or len(reason) > 120:
            raise ValueError("disarm reason must be bounded")
        with self._lock:
            self._expires_at = None
            self._binding_id = None
            self._reason = reason
        return self.snapshot()

    def require_armed(self, *, binding: AccountBinding, now: datetime) -> None:
        snapshot = self.snapshot(now=now)
        if not snapshot.armed or snapshot.account_binding_id != binding.binding_id:
            raise ExecutionBlocked("execution is disarmed or bound to a different account")
