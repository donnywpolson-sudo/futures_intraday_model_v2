"""Hash-bound metadata-only canary for live-cockpit history planning."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Callable, Mapping, Sequence

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json

from .credentials import resolve_cockpit_api_key_source
from .engine import (
    DEFAULT_DATASET,
    DEFAULT_HISTORICAL_SCHEMA,
    DEFAULT_HISTORY_HOURS,
    HISTORY_REQUEST_TIMEOUT_SECONDS,
    MAX_HISTORY_COST_ESTIMATE_REQUESTS,
    QUICK_CHART_MARKETS,
    SYMBOL_REQUEST_TIMEOUT_SECONDS,
    LiveCockpitEngine,
    _history_failure_details,
)
from .feed import import_databento


PLAN_SCHEMA = "live_cockpit_history_canary_plan/1.0.0"
APPROVAL_SCHEMA = "live_cockpit_history_canary_approval/1.0.0"  # historic reader
CONFIRMATION_SCHEMA = "live_cockpit_history_canary_confirmation/2.0.0"
TERMINAL_SCHEMA = "live_cockpit_history_canary_terminal/1.0.0"
OPERATION = "RUN_METADATA_ONLY_HISTORY_CANARY"
EXPECTED_MARKET_COUNT = 41
EXPECTED_HISTORY_MARKETS = QUICK_CHART_MARKETS
MAX_DATASET_RANGE_CALLS = 2
MAX_SYMBOLOGY_CALLS = 2
MAX_COST_CALLS = 8
MAX_DURATION_SECONDS = 360
RUN_ROOT_TEMPLATE = "reports/live_cockpit/history_canary/<PLAN_ID>"
CACHE_PATH_TEMPLATE = ".pytest_tmp/live_cockpit/history_canary/<PLAN_ID>/cache.sqlite3"
TERMINAL_PATH_TEMPLATE = f"{RUN_ROOT_TEMPLATE}/terminal.json"


class CanaryContractError(RuntimeError):
    """The hash-bound canary contract is absent, stale, or violated."""


def _write_create_only(path: Path, value: Mapping[str, Any]) -> None:
    data = canonical_bytes(dict(value)) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(data)
    except FileExistsError as exc:
        raise CanaryContractError(f"create-only output exists: {path.name}") from exc


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        check=False,
        capture_output=True,
        text=True,
        shell=False,
    )
    if result.returncode:
        raise CanaryContractError("repository identity is unavailable")
    return result.stdout.strip()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _input_hashes(root: Path) -> list[dict[str, str]]:
    paths = (
        "src/futures_rebuild/live_cockpit/history_canary.py",
        "src/futures_rebuild/live_cockpit/engine.py",
        "src/futures_rebuild/live_cockpit/protocol.py",
        "src/futures_rebuild/live_cockpit/credentials.py",
        "src/futures_rebuild/live_cockpit/cache.py",
        "src/futures_rebuild/live_cockpit/history.py",
        "src/futures_rebuild/live_cockpit/market_groups.py",
        "configs/source_contract.json",
    )
    return [{"path": path, "sha256": sha256_file(root / path)} for path in paths]


def _predecessor_binding(
    root: Path,
    predecessor_terminal: Path,
) -> dict[str, Any]:
    canonical_root = root.resolve()
    terminal_path = predecessor_terminal.resolve()
    try:
        relative_path = terminal_path.relative_to(canonical_root)
    except ValueError as exc:
        raise CanaryContractError(
            "predecessor terminal is outside the repository"
        ) from exc
    try:
        terminal = json.loads(terminal_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CanaryContractError("predecessor terminal is unavailable") from exc
    if not isinstance(terminal, Mapping):
        raise CanaryContractError("predecessor terminal is malformed")
    body = dict(terminal)
    terminal_id = body.pop("terminal_id", None)
    if (
        body.get("schema_version") != TERMINAL_SCHEMA
        or terminal_id != sha256_json(body)
        or body.get("terminal_state") != "ERROR"
        or body.get("diagnostic_category") != "TIMEOUT"
    ):
        raise CanaryContractError(
            "predecessor terminal is not a valid timeout failure"
        )
    counts = body.get("request_counts")
    if not isinstance(counts, Mapping) or any(
        counts.get(name) != expected
        for name, expected in {
            "dataset_range": 1,
            "symbology": 2,
            "cost_estimate": 0,
            "timeseries_download": 0,
            "live_client": 0,
            "production_cache_write": 0,
            "provider_failure_retry": 0,
        }.items()
    ):
        raise CanaryContractError("predecessor terminal request counts are invalid")
    return {
        "path": relative_path.as_posix(),
        "sha256": sha256_file(terminal_path),
        "terminal_id": terminal_id,
        "terminal_state": "ERROR",
        "diagnostic_category": "TIMEOUT",
    }


def build_plan(
    root: Path,
    *,
    predecessor_terminal: Path | None = None,
) -> dict[str, Any]:
    canonical_root = Path(_git(root, "rev-parse", "--show-toplevel")).resolve()
    if canonical_root != root.resolve():
        raise CanaryContractError("repository root mismatch")
    body: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA,
        "operation": OPERATION,
        "basis": {
            "repository": str(root.resolve()),
            "branch": _git(root, "branch", "--show-current"),
            "head": _git(root, "rev-parse", "HEAD"),
        },
        "inputs": _input_hashes(root),
        "scope": {
            "dataset": DEFAULT_DATASET,
            "schema": DEFAULT_HISTORICAL_SCHEMA,
            "market_count": EXPECTED_MARKET_COUNT,
            "history_market_count": len(EXPECTED_HISTORY_MARKETS),
            "history_markets": list(EXPECTED_HISTORY_MARKETS),
            "requested_hours": DEFAULT_HISTORY_HOURS,
            "expected_terminal_state": "CONFIRMATION_REQUIRED",
        },
        "limits": {
            "maximum_dataset_range_calls": MAX_DATASET_RANGE_CALLS,
            "maximum_symbology_calls": MAX_SYMBOLOGY_CALLS,
            "maximum_cost_estimate_calls": MAX_COST_CALLS,
            "maximum_duration_seconds": MAX_DURATION_SECONDS,
            "maximum_timeseries_downloads": 0,
            "maximum_live_clients": 0,
            "maximum_provider_failure_retries": 0,
            "maximum_production_cache_writes": 0,
        },
        "paths": {
            "cache": CACHE_PATH_TEMPLATE,
            "terminal": TERMINAL_PATH_TEMPLATE,
        },
        "preservation": {
            "production_cache": "NO_ACCESS_NO_MUTATION",
            "live_clients": "DO_NOT_START",
            "timeseries": "UNREACHABLE",
            "partial_outputs": "CREATE_ONLY_PRESERVE",
        },
    }
    if predecessor_terminal is not None:
        body["predecessor"] = _predecessor_binding(root, predecessor_terminal)
    body["plan_id"] = sha256_json(body)
    return body


def validate_plan(root: Path, plan: Mapping[str, Any]) -> dict[str, Any]:
    value = dict(plan)
    claimed = value.pop("plan_id", None)
    if value.get("schema_version") != PLAN_SCHEMA or value.get("operation") != OPERATION:
        raise CanaryContractError("unsupported canary plan")
    if claimed != sha256_json(value):
        raise CanaryContractError("canary plan content hash mismatch")
    predecessor = value.get("predecessor")
    predecessor_terminal: Path | None = None
    if predecessor is not None:
        if not isinstance(predecessor, Mapping) or not isinstance(
            predecessor.get("path"), str
        ):
            raise CanaryContractError("canary predecessor binding is malformed")
        predecessor_terminal = root / str(predecessor["path"])
    expected = build_plan(root, predecessor_terminal=predecessor_terminal)
    if dict(plan) != expected:
        raise CanaryContractError("canary plan differs from current exact bindings")
    return dict(plan)


def prepare_confirmation(
    root: Path,
    *,
    plan_root: Path,
    predecessor_terminal: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    plan = build_plan(root, predecessor_terminal=predecessor_terminal)
    plan_path = plan_root / f"{plan['plan_id']}.json"
    _write_create_only(plan_path, plan)
    confirmation = {
        "schema_version": CONFIRMATION_SCHEMA,
        "status": "CONFIRMATION_REQUIRED",
        "operation": OPERATION,
        "summary": "Run the bounded historical metadata canary without downloading timeseries data or writing production cache.",
        "limits": plan["limits"],
        "outputs": plan["paths"],
        "preservation": plan["preservation"],
    }
    return plan_path, confirmation


class _CountingMethod:
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
        next_count = self._counters[self._name] + 1
        if next_count > self._limit:
            raise CanaryContractError(f"{self._name} call limit exceeded")
        self._counters[self._name] = next_count
        return self._target(**kwargs)


class _BlockedTimeseries:
    TIMEOUT = HISTORY_REQUEST_TIMEOUT_SECONDS

    def __init__(self, counters: dict[str, int]) -> None:
        self._counters = counters

    def get_range(self, **_kwargs: Any) -> Any:
        self._counters["timeseries_download"] += 1
        raise CanaryContractError("timeseries download is forbidden")


def _guard_historical(historical: object, counters: dict[str, int]) -> object:
    metadata = getattr(historical, "metadata")
    symbology = getattr(historical, "symbology")
    setattr(metadata, "TIMEOUT", HISTORY_REQUEST_TIMEOUT_SECONDS)
    setattr(symbology, "TIMEOUT", SYMBOL_REQUEST_TIMEOUT_SECONDS)
    guarded_metadata = SimpleNamespace(
        TIMEOUT=HISTORY_REQUEST_TIMEOUT_SECONDS,
        get_dataset_range=_CountingMethod(
            getattr(metadata, "get_dataset_range"),
            counters=counters,
            name="dataset_range",
            limit=MAX_DATASET_RANGE_CALLS,
        ),
        get_cost=_CountingMethod(
            getattr(metadata, "get_cost"),
            counters=counters,
            name="cost_estimate",
            limit=MAX_COST_CALLS,
        ),
    )
    guarded_symbology = SimpleNamespace(
        TIMEOUT=SYMBOL_REQUEST_TIMEOUT_SECONDS,
        resolve=_CountingMethod(
            getattr(symbology, "resolve"),
            counters=counters,
            name="symbology",
            limit=MAX_SYMBOLOGY_CALLS,
        ),
    )
    return SimpleNamespace(
        metadata=guarded_metadata,
        symbology=guarded_symbology,
        timeseries=_BlockedTimeseries(counters),
    )


def _terminal(
    *,
    plan_id: str,
    requested_start: int | None,
    historical_end: int | None,
    counters: Mapping[str, int],
    estimated_cost_usd: float | None,
    terminal_state: str,
    diagnostic_category: str | None,
) -> dict[str, Any]:
    body = {
        "schema_version": TERMINAL_SCHEMA,
        "plan_id": plan_id,
        "requested_start": requested_start,
        "historical_end": historical_end,
        "request_counts": dict(counters),
        "estimated_cost_usd": estimated_cost_usd,
        "terminal_state": terminal_state,
        "diagnostic_category": diagnostic_category,
    }
    body["terminal_id"] = sha256_json(body)
    return body


def run_canary(
    root: Path,
    *,
    plan_path: Path,
    credential_resolver: Callable[[], object | None] | None = None,
    db_module: ModuleType | object | None = None,
) -> dict[str, Any]:
    plan = validate_plan(root, json.loads(plan_path.read_text(encoding="utf-8")))

    plan_id = str(plan["plan_id"])
    cache_path = root / CACHE_PATH_TEMPLATE.replace("<PLAN_ID>", plan_id)
    terminal_path = root / TERMINAL_PATH_TEMPLATE.replace("<PLAN_ID>", plan_id)
    run_root = terminal_path.parent
    if run_root.exists() or cache_path.parent.exists():
        raise CanaryContractError("create-only canary output already exists")
    run_root.mkdir(parents=True, exist_ok=False)
    cache_path.parent.mkdir(parents=True, exist_ok=False)

    counters = {
        "dataset_range": 0,
        "symbology": 0,
        "cost_estimate": 0,
        "timeseries_download": 0,
        "live_client": 0,
        "production_cache_write": 0,
        "provider_failure_retry": 0,
    }
    started = time.monotonic()
    engine: LiveCockpitEngine | None = None
    requested_start: int | None = None
    historical_end: int | None = None
    estimated_cost: float | None = None
    terminal_state = "ERROR"
    category: str | None = "UNAVAILABLE"
    events: list[dict[str, Any]] = []
    try:
        resolution = (
            credential_resolver()
            if credential_resolver is not None
            else resolve_cockpit_api_key_source(None)
        )
        if resolution is None or not getattr(resolution, "key", None):
            category = "AUTHORIZATION"
            raise CanaryContractError("credential is unavailable")
        module = db_module if db_module is not None else import_databento()
        historical = getattr(module, "Historical")(key=getattr(resolution, "key"))
        engine = LiveCockpitEngine(
            cache_path=cache_path,
            history_hours=DEFAULT_HISTORY_HOURS,
            reconnect_enabled=False,
        )
        if len(engine.markets) != EXPECTED_MARKET_COUNT:
            raise CanaryContractError("market count differs from canary scope")
        engine._historical = _guard_historical(historical, counters)
        engine._publish = events.append
        engine._prepare_history_plan()
        status = next(
            (
                item["payload"]
                for item in reversed(events)
                if item.get("type") == "history_cache_status"
            ),
            {},
        )
        terminal_state = str(status.get("state") or "ERROR")
        category_value = status.get("failure_category")
        category = str(category_value) if category_value is not None else None
        with engine._history_lock:
            history_plan = engine._history_plan
        if history_plan is not None:
            requested_start = int(history_plan.target_start.timestamp())
            historical_end = int(history_plan.target_end.timestamp())
            estimated_cost = float(history_plan.estimated_cost_usd)
            planned_markets = {
                binding.market
                for chunk in history_plan.chunks
                for binding in chunk.bindings
            }
            if planned_markets != set(EXPECTED_HISTORY_MARKETS):
                terminal_state = "ERROR"
                category = "UNAVAILABLE"
                raise CanaryContractError(
                    "canary plan does not cover the exact history market universe"
                )
        elif isinstance(status.get("diagnostic"), Mapping):
            requested_start = status["diagnostic"].get("requested_start")
            historical_end = status["diagnostic"].get("requested_end")
        if time.monotonic() - started > MAX_DURATION_SECONDS:
            terminal_state = "ERROR"
            category = "TIMEOUT"
        if terminal_state != "CONFIRMATION_REQUIRED":
            raise CanaryContractError("canary did not stop at confirmation")
        if (
            counters["dataset_range"] > MAX_DATASET_RANGE_CALLS
            or counters["symbology"] > MAX_SYMBOLOGY_CALLS
            or counters["cost_estimate"] > MAX_COST_CALLS
            or counters["timeseries_download"] != 0
            or counters["live_client"] != 0
            or counters["production_cache_write"] != 0
            or counters["provider_failure_retry"] != 0
        ):
            terminal_state = "ERROR"
            category = "UNAVAILABLE"
            raise CanaryContractError("canary request limits were violated")
    except Exception as exc:
        if category in {None, "UNAVAILABLE"}:
            category = str(_history_failure_details(exc)["failure_category"])
    finally:
        if engine is not None:
            engine.stop()

    terminal = _terminal(
        plan_id=plan_id,
        requested_start=requested_start,
        historical_end=historical_end,
        counters=counters,
        estimated_cost_usd=estimated_cost,
        terminal_state=terminal_state,
        diagnostic_category=category,
    )
    _write_create_only(terminal_path, terminal)
    return terminal


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="live-cockpit-history-canary")
    subparsers = parser.add_subparsers(dest="command", required=True)
    generate = subparsers.add_parser("generate")
    generate.add_argument("--plan-root", type=Path, required=True)
    generate.add_argument("--predecessor-terminal", type=Path)
    run = subparsers.add_parser("run")
    run.add_argument("--plan", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = _repo_root()
    if args.command == "generate":
        plan_path, confirmation = prepare_confirmation(
            root,
            plan_root=args.plan_root,
            predecessor_terminal=args.predecessor_terminal,
        )
        print(
            json.dumps(
                {
                    "plan": str(plan_path),
                    "confirmation": confirmation,
                }
            )
        )
        return 0
    terminal = run_canary(
        root,
        plan_path=args.plan,
    )
    print(json.dumps(terminal))
    return 0 if terminal["terminal_state"] == "CONFIRMATION_REQUIRED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
