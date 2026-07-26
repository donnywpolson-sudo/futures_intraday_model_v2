"""One-shot, headless, bounded Databento verification for Futures Live Cockpit."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Mapping, TextIO

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json

from .feed import (
    DEFAULT_CONTINUOUS_SUFFIX,
    DEFAULT_DATASET,
    DEFAULT_HISTORICAL_SCHEMA,
    DEFAULT_SCHEMA,
    chart_market_universe,
)

from .credentials import CredentialLocatorError, resolve_cockpit_api_key_source
from .engine import LiveCockpitEngine, MAX_RENDER_HZ
from .approval import (
    LiveSmokeApprovalError,
    validate_live_smoke_plan,
    verify_live_smoke_approval,
)


SMOKE_DURATION_SECONDS = 120.0
SMOKE_MARKET = "ES"
INCONCLUSIVE_EXIT_CODE = 3
RESULT_SCHEMA = "futures_live_cockpit_smoke_result/1.0.0"


@dataclass(frozen=True)
class SmokeResult:
    status: str
    exit_code: int
    summary: dict[str, Any]


def _utc_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class _JsonlLog:
    def __init__(self, path: Path, *, secrets: list[str]) -> None:
        self.path = path
        self._secrets = [secret for secret in secrets if secret]
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._stream = path.open("x", encoding="utf-8", newline="\n")

    def sanitize(self, value: Any) -> Any:
        if isinstance(value, Mapping):
            clean: dict[str, Any] = {}
            for key, item in value.items():
                lowered = str(key).lower()
                if any(token in lowered for token in ("api_key", "credential", "secret", "token")):
                    clean[str(key)] = "[REDACTED]"
                else:
                    clean[str(key)] = self.sanitize(item)
            return clean
        if isinstance(value, (list, tuple)):
            return [self.sanitize(item) for item in value]
        if isinstance(value, str):
            clean_text = value
            for secret in self._secrets:
                clean_text = clean_text.replace(secret, "[REDACTED]")
            return clean_text
        if value is None or isinstance(value, (bool, int, float)):
            return value
        return self.sanitize(str(value))

    def write(self, kind: str, payload: Mapping[str, Any]) -> None:
        record = self.sanitize({"ts": _utc_text(), "kind": kind, "payload": payload})
        with self._lock:
            self._stream.write(json.dumps(record, sort_keys=True) + "\n")
            self._stream.flush()

    def close(self) -> None:
        with self._lock:
            if not self._stream.closed:
                self._stream.close()


def _temporary_log_path(
    *, env: Mapping[str, str], temp_root: Path | None = None
) -> Path:
    root = temp_root
    if root is None:
        configured = env.get("TEMP") or env.get("TMP")
        root = Path(configured) if configured else Path(tempfile.gettempdir())
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return root / "FuturesLiveCockpit" / f"live-smoke-{timestamp}.jsonl"


def _subscription_plan() -> dict[str, Any]:
    symbols = sorted(info.symbol for info in chart_market_universe())
    return {
        "dataset": DEFAULT_DATASET,
        "duration_seconds": int(SMOKE_DURATION_SECONDS),
        "overview": {
            "schema": DEFAULT_HISTORICAL_SCHEMA,
            "stype_in": "continuous",
            "symbols": [f"{symbol}{DEFAULT_CONTINUOUS_SUFFIX}" for symbol in symbols],
        },
        "focus": {
            "market": SMOKE_MARKET,
            "schema": DEFAULT_SCHEMA,
            "stype_in": "instrument_id",
        },
        "max_live_sessions": 2,
        "max_render_hz": MAX_RENDER_HZ,
        "historical_replay": False,
        "cache": False,
        "reconnect": False,
    }


def _verify_package_runtime(plan: Mapping[str, Any]) -> dict[str, Any]:
    scope = plan["scope"]
    frozen = bool(getattr(sys, "frozen", False))
    if frozen is not scope["runtime_frozen"]:
        raise LiveSmokeApprovalError(
            "provider-backed cockpit smoke requires the approved frozen runtime"
        )
    try:
        executable = Path(sys.executable).resolve(strict=True)
    except OSError as exc:
        raise LiveSmokeApprovalError(
            "approved cockpit executable is not readable"
        ) from exc
    executable_hash = sha256_file(executable)
    if executable_hash != scope["prepared_executable_sha256"]:
        raise LiveSmokeApprovalError(
            "provider-backed cockpit smoke runtime hash differs from the approved package"
        )
    return {
        "frozen": frozen,
        "executable_sha256": executable_hash,
    }


def run_smoke(
    *,
    env: Mapping[str, str] | None = None,
    db_module: ModuleType | None = None,
    duration_seconds: float = SMOKE_DURATION_SECONDS,
    temp_root: Path | None = None,
    poll_seconds: float = 0.05,
    approval_receipt_id: str | None = None,
) -> SmokeResult:
    """Run one bounded verification; duration override is test-only."""

    if db_module is None and approval_receipt_id is None:
        raise LiveSmokeApprovalError(
            "provider-backed cockpit smoke requires exact hash-bound approval"
        )
    active_env = dict(os.environ if env is None else env)
    resolution_env = None if env is None else active_env
    try:
        key_resolution = resolve_cockpit_api_key_source(resolution_env)
    except CredentialLocatorError:
        key_resolution = None
    secrets = [key_resolution.key] if key_resolution is not None else []
    log_path = _temporary_log_path(env=active_env, temp_root=temp_root)
    log = _JsonlLog(log_path, secrets=secrets)
    plan = _subscription_plan()
    state_lock = threading.RLock()
    stop_early = threading.Event()
    counters = {
        "overview_market_updates": 0,
        "focus_live_events": 0,
        "bar_updates": 0,
    }
    resolved_contract: str | None = None
    feed_errors: list[dict[str, Any]] = []

    def publish(message: dict[str, Any]) -> None:
        nonlocal resolved_contract
        payload = dict(message.get("payload", {}))
        message_type = str(message.get("type", "unknown"))
        with state_lock:
            if message_type == "market_status" and payload.get("state") == "LIVE":
                counters["overview_market_updates"] += 1
            elif message_type == "bar_update":
                counters["bar_updates"] += 1
            elif message_type == "chart_snapshot" and payload.get("market") == SMOKE_MARKET:
                contract = payload.get("contract")
                if contract:
                    resolved_contract = str(contract)
            elif (
                message_type == "feed_status"
                and payload.get("scope") == "focus"
                and payload.get("state") == "LIVE"
            ):
                counters["focus_live_events"] += 1
            if message_type == "feed_status" and payload.get("state") == "ERROR":
                feed_errors.append(payload)
                stop_early.set()
        log.write("ui_event", message)

    log.write("smoke_start", plan)
    engine = LiveCockpitEngine(
        cache_path=None,
        market=SMOKE_MARKET,
        timeframe="1m",
        env=resolution_env,
        db_module=db_module,
        history_enabled=False,
        cache_enabled=False,
        reconnect_enabled=False,
        fail_fast_provider_errors=True,
    )
    started = time.monotonic()
    runtime_exception: str | None = None
    metrics_before_stop: dict[str, Any] = {}
    metrics_after_stop: dict[str, Any] = {}
    try:
        engine.start(publish)
        deadline = started + max(0.0, duration_seconds)
        while time.monotonic() < deadline:
            if stop_early.is_set() or engine.runtime_metrics()["provider_failure"] is not None:
                break
            time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))
        metrics_before_stop = engine.runtime_metrics()
    except Exception as exc:
        runtime_exception = f"{type(exc).__name__}: {exc}"
        log.write("runtime_exception", {"message": runtime_exception})
    finally:
        engine.stop()
        metrics_after_stop = engine.runtime_metrics()

    elapsed_seconds = time.monotonic() - started
    with state_lock:
        observed = dict(counters)
        observed_errors = list(feed_errors)
        contract = resolved_contract

    reasons: list[str] = []
    provider_failure = metrics_after_stop.get("provider_failure")
    if runtime_exception:
        reasons.append(runtime_exception)
    if provider_failure:
        reasons.append(str(provider_failure.get("provider_name") or "provider failure"))
    if observed_errors:
        reasons.extend(str(item.get("message", "feed error")) for item in observed_errors)
    if metrics_after_stop.get("live_sessions_started") != 2:
        reasons.append(
            f"expected exactly 2 live sessions, started {metrics_after_stop.get('live_sessions_started')}"
        )
    if metrics_after_stop.get("max_live_sessions", 0) > 2:
        reasons.append("more than 2 live sessions were active")
    if metrics_after_stop.get("active_live_sessions") != 0:
        reasons.append("live sessions remained active after shutdown")
    if metrics_after_stop.get("history_requests") != 0:
        reasons.append("historical replay was requested")
    if metrics_after_stop.get("cache_reads") != 0 or metrics_after_stop.get("cache_writes") != 0:
        reasons.append("cache access occurred")
    if metrics_after_stop.get("shutdown_errors"):
        reasons.append("live session shutdown reported errors")
    if contract in (None, "", SMOKE_MARKET):
        reasons.append("ES did not resolve to an exact raw contract")

    missing_data = [name for name, count in observed.items() if count < 1]
    if reasons:
        status = "FAIL"
        exit_code = 1
    elif missing_data:
        status = "INCONCLUSIVE_NO_DATA"
        exit_code = INCONCLUSIVE_EXIT_CODE
    else:
        status = "PASS"
        exit_code = 0

    summary = log.sanitize(
        {
            "status": status,
            "dataset": DEFAULT_DATASET,
            "duration_budget_seconds": duration_seconds,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "market_count": len(plan["overview"]["symbols"]),
            "resolved_contract": contract,
            "observed": observed,
            "missing_data": missing_data,
            "reasons": list(dict.fromkeys(reasons)),
            "metrics_before_stop": metrics_before_stop,
            "metrics_after_stop": metrics_after_stop,
            "log_path": str(log_path),
            "log_retained": status != "PASS",
            "approval_receipt_id": approval_receipt_id,
            "runtime": {
                "frozen": bool(getattr(sys, "frozen", False)),
                "executable_sha256": sha256_file(Path(sys.executable).resolve()),
            },
        }
    )
    log.write("smoke_result", summary)
    log.close()
    if status == "PASS":
        try:
            log_path.unlink()
        except OSError as exc:
            summary["status"] = "FAIL"
            summary["reasons"] = [f"temporary PASS log could not be deleted: {exc}"]
            summary["log_retained"] = True
            return SmokeResult(status="FAIL", exit_code=1, summary=summary)
    return SmokeResult(status=status, exit_code=exit_code, summary=summary)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m live_cockpit.smoke",
        description="Run the fixed 120-second, two-session Databento cockpit smoke.",
    )
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--approval", type=Path, required=True)
    parser.add_argument("--result-output", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None, *, stdout: TextIO | None = sys.stdout) -> int:
    args = build_arg_parser().parse_args(argv)
    plan_path = args.plan.resolve(strict=True)
    approval_path = args.approval.resolve(strict=True)
    try:
        approval_id = verify_live_smoke_approval(
            plan_path=plan_path,
            approval_path=approval_path,
        )
        plan = validate_live_smoke_plan(
            json.loads(plan_path.read_text(encoding="utf-8"))
        )
        expected_relative = Path(str(plan["scope"]["result_output_relative"]))
        result_output = args.result_output.resolve(strict=False)
        expected_output = (Path.cwd().resolve() / expected_relative).resolve(
            strict=False
        )
        if result_output != expected_output:
            raise LiveSmokeApprovalError(
                "live-smoke result output differs from the approved relative path"
            )
        if result_output.exists():
            raise LiveSmokeApprovalError(
                "live-smoke result output already exists; no overwrite is allowed"
            )
        _verify_package_runtime(plan)
    except (LiveSmokeApprovalError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        if stdout is not None:
            print(f"BLOCKED: {exc}", file=stdout)
        return 2
    result = run_smoke(approval_receipt_id=approval_id)
    runtime = result.summary.get("runtime")
    runtime_matches = (
        type(runtime) is dict
        and runtime.get("frozen") is plan["scope"]["runtime_frozen"]
        and runtime.get("executable_sha256")
        == plan["scope"]["prepared_executable_sha256"]
    )
    if not runtime_matches:
        result.summary["status"] = "FAIL"
        result.summary["reasons"] = [
            *list(result.summary.get("reasons", [])),
            "runtime executable does not match the approved prepared package",
        ]
        result = SmokeResult(status="FAIL", exit_code=1, summary=result.summary)
    core = {
        "schema_version": RESULT_SCHEMA,
        "status": result.status,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "approval_receipt_id": approval_id,
        "completed_at": _utc_text(),
        "result_output_relative": expected_relative.as_posix(),
        "summary": result.summary,
    }
    receipt = {**core, "result_id": sha256_json(core)}
    result_output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with result_output.open("xb") as stream:
            stream.write(canonical_bytes(receipt) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
    except OSError as exc:
        if stdout is not None:
            print(f"FAIL: could not publish live-smoke result: {exc}", file=stdout)
        return 1
    if stdout is not None:
        print(json.dumps(receipt, sort_keys=True), file=stdout)
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
