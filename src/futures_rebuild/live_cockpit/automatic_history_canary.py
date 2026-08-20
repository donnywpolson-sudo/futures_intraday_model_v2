"""Isolated one-market validation of the automatic history-repair path."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
import threading
import time

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json

from .app import (
    CockpitController,
    _automatic_history_eligibility,
    default_history_update_policy,
    load_state,
    mutate_state,
    sanitize_history_update_policy,
)
from .credentials import resolve_cockpit_api_key_source
from .engine import (
    DEFAULT_DATASET,
    HISTORY_REQUEST_TIMEOUT_SECONDS,
    SYMBOL_REQUEST_TIMEOUT_SECONDS,
    DemoCockpitEngine,
    LiveCockpitEngine,
    _history_failure_details,
)
from .feed import chart_market_universe, import_databento


PLAN_SCHEMA = "live_cockpit_automatic_history_canary_plan/1.0.0"
TERMINAL_SCHEMA = "live_cockpit_automatic_history_canary_terminal/1.0.0"
OPERATION = "RUN_ISOLATED_AUTOMATIC_HISTORY_CANARY"
MARKET = "ES"
HISTORY_HOURS = 24
MAX_ESTIMATED_COST_USD = Decimal("0.05")
MAX_DURATION_SECONDS = 180
MAX_DATASET_RANGE_CALLS = 1
MAX_SYMBOLOGY_CALLS = 2
MAX_COST_CALLS = 1
MAX_TIMESERIES_DOWNLOADS = 1
RUN_ROOT_TEMPLATE = "reports/live_cockpit/automatic_history_canary/<PLAN_ID>"
STATE_PATH_TEMPLATE = f"{RUN_ROOT_TEMPLATE}/state/cockpit-state.json"
CACHE_PATH_TEMPLATE = f"{RUN_ROOT_TEMPLATE}/cache/bars.sqlite3"
TERMINAL_PATH_TEMPLATE = f"{RUN_ROOT_TEMPLATE}/terminal.json"
_INPUT_PATHS = (
    "src/futures_rebuild/live_cockpit/automatic_history_canary.py",
    "src/futures_rebuild/live_cockpit/app.py",
    "src/futures_rebuild/live_cockpit/cache.py",
    "src/futures_rebuild/live_cockpit/credentials.py",
    "src/futures_rebuild/live_cockpit/engine.py",
    "src/futures_rebuild/live_cockpit/feed.py",
    "src/futures_rebuild/live_cockpit/history.py",
    "src/futures_rebuild/live_cockpit/protocol.py",
    "configs/alpha_tiered.yaml",
)


class AutomaticHistoryCanaryError(RuntimeError):
    """The exact automatic-history canary contract is invalid or failed."""


def _git(root: Path, *args: str) -> str:
    import subprocess

    result = subprocess.run(
        ["git", "-C", str(root), *args],
        check=False,
        capture_output=True,
        text=True,
        shell=False,
    )
    if result.returncode:
        raise AutomaticHistoryCanaryError("repository identity is unavailable")
    return result.stdout.strip()


def _write_create_only(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(canonical_bytes(dict(value)) + b"\n")
    except FileExistsError as exc:
        raise AutomaticHistoryCanaryError(
            f"create-only output exists: {path.name}"
        ) from exc


def _relative_file(root: Path, path: Path, name: str) -> str:
    try:
        resolved = path.resolve(strict=True)
        relative = resolved.relative_to(root.resolve())
    except (OSError, ValueError) as exc:
        raise AutomaticHistoryCanaryError(
            f"{name} is unavailable or outside the repository"
        ) from exc
    if not resolved.is_file():
        raise AutomaticHistoryCanaryError(f"{name} is not a file")
    return relative.as_posix()


def _input_hashes(root: Path) -> list[dict[str, Any]]:
    return [
        {
            "path": relative,
            "bytes": (root / relative).stat().st_size,
            "sha256": sha256_file(root / relative),
        }
        for relative in _INPUT_PATHS
    ]


def build_plan(root: Path, *, candidate_executable: Path) -> dict[str, Any]:
    canonical_root = Path(_git(root, "rev-parse", "--show-toplevel")).resolve()
    if canonical_root != root.resolve():
        raise AutomaticHistoryCanaryError("repository root mismatch")
    candidate_relative = _relative_file(
        root,
        candidate_executable,
        "candidate executable",
    )
    body: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA,
        "operation": OPERATION,
        "basis": {
            "repository": str(root.resolve()),
            "branch": _git(root, "branch", "--show-current"),
            "head": _git(root, "rev-parse", "HEAD"),
            "candidate_executable": candidate_relative,
            "candidate_executable_sha256": sha256_file(candidate_executable),
        },
        "inputs": _input_hashes(root),
        "scope": {
            "dataset": DEFAULT_DATASET,
            "market": MARKET,
            "market_count": 1,
            "requested_hours": HISTORY_HOURS,
            "mode": "AUTO",
            "update_origin": "AUTO",
            "expected_terminal_state": "COMPLETE",
        },
        "limits": {
            "maximum_estimated_cost_usd": format(
                MAX_ESTIMATED_COST_USD,
                "f",
            ),
            "maximum_automatic_attempts": 1,
            "automatic_attempt_interval_hours": 24,
            "maximum_duration_seconds": MAX_DURATION_SECONDS,
            "maximum_dataset_range_calls": MAX_DATASET_RANGE_CALLS,
            "maximum_symbology_calls": MAX_SYMBOLOGY_CALLS,
            "maximum_cost_calls": MAX_COST_CALLS,
            "maximum_timeseries_downloads": MAX_TIMESERIES_DOWNLOADS,
            "maximum_live_clients": 0,
            "maximum_order_or_execution_calls": 0,
        },
        "paths": {
            "state": STATE_PATH_TEMPLATE,
            "cache": CACHE_PATH_TEMPLATE,
            "terminal": TERMINAL_PATH_TEMPLATE,
        },
        "preservation": {
            "production_state": "NO_ACCESS_NO_MUTATION",
            "production_cache": "NO_ACCESS_NO_MUTATION",
            "orders_and_execution": "DISABLED",
            "outputs": "CREATE_ONLY_RETAIN",
        },
        "execution_authorized": False,
    }
    body["plan_id"] = sha256_json(body)
    return body


def validate_plan(root: Path, plan: Mapping[str, Any]) -> dict[str, Any]:
    value = dict(plan)
    claimed = value.pop("plan_id", None)
    if value.get("schema_version") != PLAN_SCHEMA or value.get("operation") != OPERATION:
        raise AutomaticHistoryCanaryError("unsupported automatic canary plan")
    if claimed != sha256_json(value):
        raise AutomaticHistoryCanaryError("automatic canary plan hash mismatch")
    basis = value.get("basis")
    if not isinstance(basis, Mapping) or not isinstance(
        basis.get("candidate_executable"),
        str,
    ):
        raise AutomaticHistoryCanaryError("automatic canary basis is invalid")
    expected = build_plan(
        root,
        candidate_executable=root / str(basis["candidate_executable"]),
    )
    if dict(plan) != expected:
        raise AutomaticHistoryCanaryError(
            "automatic canary plan differs from current exact bindings"
        )
    return dict(plan)


def prepare_confirmation(
    root: Path,
    *,
    candidate_executable: Path,
    plan_root: Path,
) -> tuple[Path, dict[str, Any]]:
    plan = build_plan(root, candidate_executable=candidate_executable)
    plan_path = plan_root / f"{plan['plan_id']}.json"
    _write_create_only(plan_path, plan)
    return plan_path, {
        "status": "CONFIRMATION_REQUIRED",
        "operation": OPERATION,
        "scope": plan["scope"],
        "limits": plan["limits"],
        "outputs": plan["paths"],
        "preservation": plan["preservation"],
    }


class _BoundedMethod:
    def __init__(
        self,
        target: Callable[..., Any],
        *,
        counters: dict[str, int],
        name: str,
        limit: int,
    ) -> None:
        self._target = target
        self._counters = counters
        self._name = name
        self._limit = limit

    def __call__(self, **kwargs: Any) -> Any:
        count = self._counters[self._name] + 1
        if count > self._limit:
            raise AutomaticHistoryCanaryError(f"{self._name} call limit exceeded")
        self._counters[self._name] = count
        return self._target(**kwargs)


def _guard_historical(
    historical: object,
    counters: dict[str, int],
) -> object:
    metadata = getattr(historical, "metadata")
    symbology = getattr(historical, "symbology")
    timeseries = getattr(historical, "timeseries")
    return SimpleNamespace(
        metadata=SimpleNamespace(
            TIMEOUT=HISTORY_REQUEST_TIMEOUT_SECONDS,
            get_dataset_range=_BoundedMethod(
                getattr(metadata, "get_dataset_range"),
                counters=counters,
                name="dataset_range",
                limit=MAX_DATASET_RANGE_CALLS,
            ),
            get_cost=_BoundedMethod(
                getattr(metadata, "get_cost"),
                counters=counters,
                name="cost",
                limit=MAX_COST_CALLS,
            ),
        ),
        symbology=SimpleNamespace(
            TIMEOUT=SYMBOL_REQUEST_TIMEOUT_SECONDS,
            resolve=_BoundedMethod(
                getattr(symbology, "resolve"),
                counters=counters,
                name="symbology",
                limit=MAX_SYMBOLOGY_CALLS,
            ),
        ),
        timeseries=SimpleNamespace(
            TIMEOUT=HISTORY_REQUEST_TIMEOUT_SECONDS,
            get_range=_BoundedMethod(
                getattr(timeseries, "get_range"),
                counters=counters,
                name="timeseries_download",
                limit=MAX_TIMESERIES_DOWNLOADS,
            ),
        ),
    )


def _resolved_path(root: Path, template: str, plan_id: str) -> Path:
    path = (root / template.replace("<PLAN_ID>", plan_id)).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise AutomaticHistoryCanaryError("automatic canary path escaped root") from exc
    return path


def run_canary(
    root: Path,
    *,
    plan_path: Path,
    credential_locator: Path | None = None,
    credential_resolver: Callable[[], object | None] | None = None,
    db_module: ModuleType | object | None = None,
    poll_seconds: float = 0.05,
) -> dict[str, Any]:
    plan = validate_plan(root, json.loads(plan_path.read_text(encoding="utf-8")))
    plan_id = str(plan["plan_id"])
    run_root = _resolved_path(root, RUN_ROOT_TEMPLATE, plan_id)
    state_path = _resolved_path(root, STATE_PATH_TEMPLATE, plan_id)
    cache_path = _resolved_path(root, CACHE_PATH_TEMPLATE, plan_id)
    terminal_path = _resolved_path(root, TERMINAL_PATH_TEMPLATE, plan_id)
    if run_root.exists():
        raise AutomaticHistoryCanaryError("create-only canary output already exists")
    run_root.mkdir(parents=True, exist_ok=False)

    policy = default_history_update_policy()
    policy["mode"] = "AUTO"
    state_lock = threading.RLock()

    def initialize(state: dict[str, Any]) -> None:
        state.update(
            {
                "market": MARKET,
                "timeframe": "1m",
                "history_update_policy": policy,
            }
        )

    mutate_state(state_path, state_lock, initialize)
    counters = {
        "dataset_range": 0,
        "symbology": 0,
        "cost": 0,
        "timeseries_download": 0,
        "live_client": 0,
        "order_or_execution": 0,
    }
    started = time.monotonic()
    engine: LiveCockpitEngine | None = None
    terminal_state = "RUNNING"
    category: str | None = "UNAVAILABLE"
    estimate: Decimal | None = None
    selected_market_first = False
    update_origin: str | None = None
    cache_validated = False
    cache_bar_count = 0
    metrics: dict[str, Any] = {}
    try:
        resolution = (
            credential_resolver()
            if credential_resolver is not None
            else resolve_cockpit_api_key_source(
                None,
                locator_path=credential_locator,
            )
        )
        if resolution is None or not getattr(resolution, "key", None):
            category = "AUTHORIZATION"
            raise AutomaticHistoryCanaryError("credential is unavailable")
        module = db_module if db_module is not None else import_databento()
        historical = getattr(module, "Historical")(key=getattr(resolution, "key"))
        es = next(
            info for info in chart_market_universe() if info.symbol == MARKET
        )
        engine = LiveCockpitEngine(
            cache_path=cache_path,
            market=MARKET,
            timeframe="1m",
            history_hours=HISTORY_HOURS,
            reconnect_enabled=False,
            fail_fast_provider_errors=True,
            markets=(es,),
        )
        controller = CockpitController(engine, state_path=state_path)
        engine._historical = _guard_historical(historical, counters)
        engine._publish = controller.publish
        engine._ensure_history_worker()
        deadline = started + MAX_DURATION_SECONDS
        confirmation_payload: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            for message in controller.poll_events(200):
                if message.get("type") != "history_cache_status":
                    continue
                payload = message.get("payload")
                if not isinstance(payload, Mapping):
                    continue
                state = str(payload.get("state") or "").upper()
                if state == "CONFIRMATION_REQUIRED":
                    confirmation_payload = dict(payload)
                    try:
                        estimate = Decimal(str(payload.get("estimated_cost_usd")))
                    except (InvalidOperation, TypeError, ValueError):
                        estimate = None
                if state == "WARMING" and payload.get("active_market") == MARKET:
                    selected_market_first = True
                    update_origin = str(payload.get("update_origin") or "") or None
                if state in {"COMPLETE", "PARTIAL", "ERROR"}:
                    terminal_state = state
                    failure = payload.get("failure_category")
                    category = str(failure) if failure is not None else None
            if terminal_state in {"COMPLETE", "PARTIAL", "ERROR"}:
                break
            if confirmation_payload is not None and (
                estimate is None
                or not estimate.is_finite()
                or estimate < 0
                or estimate > MAX_ESTIMATED_COST_USD
            ):
                terminal_state = "REVIEW_REQUIRED"
                category = "COST_LIMIT"
                break
            time.sleep(poll_seconds)
        if terminal_state == "RUNNING" and time.monotonic() >= deadline:
            terminal_state = "ERROR"
            category = "TIMEOUT"
        metrics = engine.runtime_metrics()
        if terminal_state == "COMPLETE" and engine.cache is not None:
            cache_bar_count = engine.cache.count()
            binding = engine._market_bindings.get(MARKET)
            if binding is not None and confirmation_payload is not None:
                missing_start = confirmation_payload.get("missing_start")
                missing_end = confirmation_payload.get("missing_end")
                if isinstance(missing_start, int) and isinstance(missing_end, int):
                    cache_validated = not engine._missing_history(
                        binding=binding,
                        start=datetime.fromtimestamp(missing_start, tz=timezone.utc),
                        end=datetime.fromtimestamp(missing_end, tz=timezone.utc),
                    )
    except Exception as exc:
        terminal_state = "ERROR"
        if category in {None, "UNAVAILABLE"}:
            category = str(_history_failure_details(exc)["failure_category"])
    finally:
        if engine is not None:
            engine.stop()

    persisted = sanitize_history_update_policy(
        load_state(state_path).get("history_update_policy")
    )
    restarted = CockpitController(
        DemoCockpitEngine(market=MARKET, timeframe="1m"),
        state_path=state_path,
    )
    restart_policy = restarted._history_policy()
    recent_attempt_blocked = False
    if restart_policy.get("last_auto_attempt_at") is not None:
        eligible, reason, _ = _automatic_history_eligibility(
            restart_policy,
            {
                "state": "CONFIRMATION_REQUIRED",
                "estimated_cost_usd": "0.01",
                "plan_id": "restart-probe",
                "plan_fingerprint": "a" * 64,
                "estimate_expires_at": int(
                    (datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()
                ),
            },
            now=datetime.now(timezone.utc),
        )
        recent_attempt_blocked = not eligible and reason == "RECENT_ATTEMPT"

    reasons: list[str] = []
    if terminal_state != "COMPLETE":
        reasons.append("automatic update did not complete")
    if estimate is None or not estimate.is_finite() or estimate > MAX_ESTIMATED_COST_USD:
        reasons.append("estimate was not within the automatic cap")
    if counters["timeseries_download"] != 1:
        reasons.append("historical download count was not exactly one")
    if counters["live_client"] != 0 or counters["order_or_execution"] != 0:
        reasons.append("a forbidden client or execution path was used")
    if metrics.get("history_plan_confirmations") != 1:
        reasons.append("automatic plan confirmation count was not exactly one")
    if metrics.get("history_requests") != 1:
        reasons.append("history worker request count was not exactly one")
    if not selected_market_first or update_origin != "AUTO":
        reasons.append("selected-market-first automatic origin was not observed")
    if not cache_validated:
        reasons.append("isolated cache coverage validation failed")
    if (
        persisted.get("last_auto_outcome") != "COMPLETE"
        or persisted.get("mode") != "AUTO"
        or not recent_attempt_blocked
        or restart_policy != persisted
    ):
        reasons.append("automatic attempt state did not persist across restart")
    status = "PASS" if not reasons else "FAIL"
    body: dict[str, Any] = {
        "schema_version": TERMINAL_SCHEMA,
        "plan_id": plan_id,
        "status": status,
        "terminal_state": terminal_state,
        "diagnostic_category": category,
        "candidate_executable_sha256": plan["basis"][
            "candidate_executable_sha256"
        ],
        "source_revision": plan["basis"]["head"],
        "market": MARKET,
        "requested_hours": HISTORY_HOURS,
        "estimated_cost_usd": format(estimate, "f") if estimate is not None else None,
        "update_origin": update_origin,
        "selected_market_first": selected_market_first,
        "cache_validated": cache_validated,
        "cache_bar_count": cache_bar_count,
        "request_counts": counters,
        "history_plan_confirmations": metrics.get("history_plan_confirmations", 0),
        "last_auto_outcome": persisted.get("last_auto_outcome"),
        "last_auto_attempt_at": persisted.get("last_auto_attempt_at"),
        "restart_recent_attempt_blocked": recent_attempt_blocked,
        "reasons": reasons,
    }
    body["terminal_id"] = sha256_json(body)
    _write_create_only(terminal_path, body)
    return body
