"""Resumable four-worker Databento BBO-1s acquisition into sealed staging."""

from __future__ import annotations

import json
import os
import queue
import shutil
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, time as wall_time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Final
from zoneinfo import ZoneInfo

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .live_cockpit.databento_auth import resolve_databento_api_key
from .live_cockpit.single_instance import SingleInstance
from .runtime_environment import require_locked_repository_environment


PLAN_PATH: Final = Path("configs/l1_execution_quarantine_plan_v1.json")
OPERATION: Final = "ACQUIRE_L1_EXECUTION_BBO_1S_QUARANTINE_V1_ONCE"
STAGING_ROOT: Final = Path("state/provider_acquisition_staging/l1_execution_quarantine_v1")
RECORD_ROOT: Final = Path("state/provider_acquisitions/l1_execution_quarantine_v1")
EVENT_ROOT: Final = Path("state/provider_acquisition_events/l1_execution_quarantine_v1")
LOCK_PATH: Final = Path("state/locks/l1_execution_quarantine_v1.lock")
CREDENTIAL_SOURCE: Final = "file api.env"
DATASET: Final = "GLBX.MDP3"
SCHEMA: Final = "bbo-1s"
STYPE_IN: Final = "continuous"
STYPE_OUT: Final = "instrument_id"
SYMBOLS: Final = (
    "ES.v.0", "NQ.v.0", "CL.v.0", "GC.v.0", "ZN.v.0", "6E.v.0",
    "MES.v.0", "MES.v.1", "MCL.v.0", "MCL.v.1",
)
MAXIMUM_WORKERS: Final = 4
THROTTLED_WORKERS: Final = 2
MAXIMUM_RETRIES: Final = 3
MINIMUM_BILLABLE_BYTES: Final = 1_400_000_000
MINIMUM_NONEMPTY_REQUESTS: Final = 250
MAXIMUM_BILLABLE_BYTES: Final = 2_000_000_000
MAXIMUM_STAGED_BYTES: Final = 5_000_000_000
REQUIRED_FREE_BYTES: Final = 5_000_000_000
MAXIMUM_RUNTIME_SECONDS: Final = 21_600
MAXIMUM_METADATA_CALLS: Final = 524
MAXIMUM_METADATA_ATTEMPTS: Final = 2_096
MAXIMUM_TIMESERIES_ATTEMPTS: Final = 1_044


@dataclass(frozen=True)
class ProviderApis:
    get_dataset_range: Callable[..., object]
    resolve: Callable[..., object]
    get_cost: Callable[..., object]
    get_billable_size: Callable[..., object]
    get_range: Callable[..., object]


@dataclass(frozen=True)
class DailyRequest:
    trade_date: str
    start: str
    end: str
    request_id: str

    def provider_query(self, *, include_output_identity: bool = False) -> dict[str, object]:
        query: dict[str, object] = {
            "dataset": DATASET,
            "schema": SCHEMA,
            "stype_in": STYPE_IN,
            "symbols": list(SYMBOLS),
            "start": self.start,
            "end": self.end,
        }
        if include_output_identity:
            query["stype_out"] = STYPE_OUT
        return query

    def as_dict(self) -> dict[str, object]:
        return {
            "trade_date": self.trade_date,
            "request_id": self.request_id,
            "query": self.provider_query(include_output_identity=True),
        }


@dataclass(frozen=True)
class Preflight:
    billable_bytes: dict[str, int]
    total_billable_bytes: int
    empty_request_count: int
    metadata_calls: int
    metadata_attempts: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": "l1_execution_quarantine_preflight/1.0.0",
            "state": "PASS_ZERO_COST_CURRENT_ENTITLEMENT",
            "billable_bytes_by_request": self.billable_bytes,
            "total_billable_bytes": self.total_billable_bytes,
            "empty_request_count": self.empty_request_count,
            "metadata_calls": self.metadata_calls,
            "external_cost_usd": "0",
        }


@dataclass(frozen=True)
class WorkerResult:
    worker_index: int
    completed_request_ids: tuple[str, ...]
    timeseries_attempts: int
    failure_type: str | None
    failed_request_id: str | None


def build_file_provider_apis(
    *, root: Path, historical_factory: Callable[..., object] | None = None,
) -> ProviderApis:
    """Create one isolated SDK client without exposing credential content."""

    key = resolve_databento_api_key(key_files=(root / "api.env",))
    if not key:
        raise UnauthorizedOperation("the bound file api.env credential is unavailable")
    if historical_factory is None:
        from databento import Historical
        historical_factory = Historical
    client = historical_factory(key=key)
    metadata = getattr(client, "metadata", None)
    symbology = getattr(client, "symbology", None)
    timeseries = getattr(client, "timeseries", None)
    methods = {
        "get_dataset_range": getattr(metadata, "get_dataset_range", None),
        "resolve": getattr(symbology, "resolve", None),
        "get_cost": getattr(metadata, "get_cost", None),
        "get_billable_size": getattr(metadata, "get_billable_size", None),
        "get_range": getattr(timeseries, "get_range", None),
    }
    if not all(callable(method) for method in methods.values()):
        raise IntegrityError("Databento client lacks the required historical APIs")
    return ProviderApis(**methods)  # type: ignore[arg-type]


def _object(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{label} is invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{label} is not an object")
    return value


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH, "L1 execution quarantine plan")
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    limits = plan.get("limits")
    promotion = plan.get("canonical_promotion")
    remediations = plan.get("audit_remediations")
    forbidden = plan.get("forbidden")
    period = plan.get("period")
    symbol_roles = plan.get("symbols")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version") != "l1_execution_quarantine_plan/1.0.0"
        or plan.get("state")
        != "PREPARED_REQUIRES_SEPARATE_PROVIDER_ACQUISITION_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("dataset") != DATASET
        or plan.get("schema") != SCHEMA
        or plan.get("stype_in") != STYPE_IN
        or plan.get("stype_out") != STYPE_OUT
        or tuple(plan.get("request_symbol_order", ())) != SYMBOLS
        or plan.get("expected_billable_bytes") != 1_650_862_880
        or plan.get("implementation_sha256") != sha256_file(Path(__file__))
        or period
        != {
            "end_date_inclusive": "2026-08-31",
            "local_end_exclusive": "12:10:00",
            "local_start_inclusive": "09:55:00",
            "request_count": 261,
            "start_date_inclusive": "2025-09-01",
            "timezone": "America/Chicago",
            "weekdays_only": True,
        }
        or symbol_roles
        != {
            "auxiliary_no_current_alpha_consumer": ["ZN.v.0", "6E.v.0"],
            "current_alpha_authority": ["ES.v.0", "NQ.v.0", "CL.v.0", "GC.v.0"],
            "mapped_micro_roll_candidates": [
                "MES.v.0", "MES.v.1", "MCL.v.0", "MCL.v.1",
            ],
        }
        or not isinstance(limits, Mapping)
        or limits.get("maximum_parallel_downloads") != MAXIMUM_WORKERS
        or limits.get("throttled_parallel_downloads") != THROTTLED_WORKERS
        or limits.get("maximum_retries_per_request") != MAXIMUM_RETRIES
        or limits.get("minimum_billable_bytes") != MINIMUM_BILLABLE_BYTES
        or limits.get("minimum_nonempty_requests") != MINIMUM_NONEMPTY_REQUESTS
        or limits.get("maximum_billable_bytes") != MAXIMUM_BILLABLE_BYTES
        or limits.get("maximum_staged_bytes") != MAXIMUM_STAGED_BYTES
        or limits.get("required_free_disk_bytes") != REQUIRED_FREE_BYTES
        or limits.get("maximum_host_runtime_seconds") != MAXIMUM_RUNTIME_SECONDS
        or limits.get("maximum_metadata_calls") != MAXIMUM_METADATA_CALLS
        or limits.get("maximum_metadata_attempts") != MAXIMUM_METADATA_ATTEMPTS
        or limits.get("maximum_timeseries_attempts") != MAXIMUM_TIMESERIES_ATTEMPTS
        or not isinstance(promotion, Mapping)
        or promotion.get("state") != "DEFERRED_PROVIDER_FREE_NOT_PART_OF_ACQUISITION"
        or promotion.get("catalog_compatible_now") is not False
        or promotion.get("active_source_contract_change") is not False
        or promotion.get("alpha_source_allowlist_change") is not False
        or not isinstance(remediations, list)
        or len(remediations) != 7
        or {item.get("finding") for item in remediations if isinstance(item, Mapping)}
        != set(range(1, 8))
        or not isinstance(forbidden, Mapping)
        or set(forbidden)
        != {
            "active_data_mutation",
            "alpha_feature_or_target_use",
            "canonical_promotion_during_acquisition",
            "credential_log_stage_or_report",
            "dbn_row_decode",
            "holdout_or_forward_evaluation",
            "model_fit_or_prediction",
            "overwrite_existing_files",
            "trading_or_order_access",
        }
        or set(forbidden.values()) != {True}
    ):
        raise IntegrityError("L1 execution quarantine plan drifted")
    ledger = build_request_ledger(plan)
    if len(ledger) != plan["period"]["request_count"]:  # type: ignore[index]
        raise IntegrityError("L1 request count drifted")
    return plan


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_request_ledger(plan: Mapping[str, object]) -> tuple[DailyRequest, ...]:
    period = plan.get("period")
    if not isinstance(period, Mapping):
        raise IntegrityError("L1 period is invalid")
    try:
        first = date.fromisoformat(str(period["start_date_inclusive"]))
        last = date.fromisoformat(str(period["end_date_inclusive"]))
        start_clock = wall_time.fromisoformat(str(period["local_start_inclusive"]))
        end_clock = wall_time.fromisoformat(str(period["local_end_exclusive"]))
        zone = ZoneInfo(str(period["timezone"]))
    except (KeyError, ValueError) as exc:
        raise IntegrityError("L1 period fields are invalid") from exc
    if first > last or start_clock >= end_clock or period.get("weekdays_only") is not True:
        raise IntegrityError("L1 period bounds are invalid")
    requests: list[DailyRequest] = []
    current = first
    while current <= last:
        if current.weekday() < 5:
            start = _utc_text(datetime.combine(current, start_clock, zone))
            end = _utc_text(datetime.combine(current, end_clock, zone))
            core = {
                "dataset": DATASET,
                "schema": SCHEMA,
                "stype_in": STYPE_IN,
                "stype_out": STYPE_OUT,
                "symbols": list(SYMBOLS),
                "trade_date": current.isoformat(),
                "start": start,
                "end": end,
            }
            requests.append(DailyRequest(current.isoformat(), start, end, sha256_json(core)))
        current += timedelta(days=1)
    return tuple(requests)


def _parse_provider_time(value: object, label: str) -> datetime:
    if type(value) is not str:
        raise IntegrityError(f"provider {label} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IntegrityError(f"provider {label} is invalid") from exc
    if parsed.tzinfo is None:
        raise IntegrityError(f"provider {label} is not timezone-aware")
    return parsed.astimezone(timezone.utc)


def _validate_entitlement(value: object, ledger: Sequence[DailyRequest]) -> None:
    if not isinstance(value, Mapping):
        raise IntegrityError("provider dataset range is invalid")
    schema_ranges = value.get("schema")
    selected = schema_ranges.get(SCHEMA) if isinstance(schema_ranges, Mapping) else value
    if not isinstance(selected, Mapping):
        raise IntegrityError("provider bbo-1s range is absent")
    start = _parse_provider_time(selected.get("start"), "range start")
    end = _parse_provider_time(selected.get("end"), "range end")
    if start > _parse_provider_time(ledger[0].start, "request start") or end < _parse_provider_time(
        ledger[-1].end, "request end"
    ):
        raise UnauthorizedOperation("current bbo-1s entitlement does not cover the fixed ledger")


def _validate_symbology(value: object, plan: Mapping[str, object]) -> None:
    if not isinstance(value, Mapping):
        raise IntegrityError("provider symbology response is invalid")
    if value.get("partial") not in (None, [], (), False, 0) or value.get("not_found") not in (
        None, [], (),
    ):
        raise UnauthorizedOperation("continuous symbology is partial or unresolved")
    result = value.get("result")
    if not isinstance(result, Mapping) or set(result) != set(SYMBOLS):
        raise UnauthorizedOperation("continuous symbology does not resolve every symbol")
    period = plan["period"]  # type: ignore[index]
    start = date.fromisoformat(str(period["start_date_inclusive"]))
    end = date.fromisoformat(str(period["end_date_inclusive"])) + timedelta(days=1)
    if (
        value.get("stype_in") != STYPE_IN
        or value.get("stype_out") != STYPE_OUT
        or value.get("start_date") != period["start_date_inclusive"]
        or value.get("end_date") != end.isoformat()
    ):
        raise IntegrityError("continuous symbology response differs from the fixed period")
    for symbol in SYMBOLS:
        entries = result[symbol]
        if not isinstance(entries, list) or not entries:
            raise UnauthorizedOperation("continuous symbology has an empty mapping")
        intervals: list[tuple[date, date]] = []
        for entry in entries:
            if not isinstance(entry, Mapping) or not {"d0", "d1", "s"} <= set(entry):
                raise IntegrityError("continuous symbology interval is malformed")
            try:
                left = date.fromisoformat(str(entry["d0"]))
                right = date.fromisoformat(str(entry["d1"]))
            except ValueError as exc:
                raise IntegrityError("continuous symbology interval date is invalid") from exc
            if left >= right or str(entry["s"]) == "":
                raise IntegrityError("continuous symbology interval is invalid")
            if right > start and left < end:
                intervals.append((max(left, start), min(right, end)))
        cursor = start
        for left, right in sorted(intervals):
            if left > cursor:
                raise UnauthorizedOperation("continuous symbology contains a coverage gap")
            cursor = max(cursor, right)
        if cursor < end:
            raise UnauthorizedOperation("continuous symbology does not cover the fixed ledger")


def _zero_cost(value: object) -> None:
    if isinstance(value, bool):
        raise IntegrityError("provider cost response is invalid")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError("provider cost response is invalid") from exc
    if not amount.is_finite() or amount != 0:
        raise UnauthorizedOperation("fresh provider cost is not exactly zero")


def _remaining(deadline: float, clock: Callable[[], float]) -> float:
    remaining = deadline - clock()
    if remaining <= 0:
        raise UnauthorizedOperation("L1 acquisition runtime exhausted")
    return remaining


def _provider_call(
    method: Callable[..., object], *, deadline: float, clock: Callable[[], float],
    kwargs: Mapping[str, object],
) -> object:
    remaining = _remaining(deadline, clock)
    owner = getattr(method, "__self__", None)
    if owner is not None and hasattr(owner, "TIMEOUT"):
        try:
            owner.TIMEOUT = min(100.0, remaining)
        except (AttributeError, TypeError) as exc:
            raise IntegrityError("provider API timeout cannot be bounded") from exc
    value = method(**kwargs)
    _remaining(deadline, clock)
    return value


def _metadata_call(
    method: Callable[..., object], *, attempts: dict[str, int], deadline: float,
    clock: Callable[[], float], sleeper: Callable[[float], None],
    kwargs: Mapping[str, object],
) -> object:
    for retry in range(MAXIMUM_RETRIES + 1):
        attempts["count"] += 1
        if attempts["count"] > MAXIMUM_METADATA_ATTEMPTS:
            raise UnauthorizedOperation("L1 metadata-attempt ceiling exceeded")
        try:
            return _provider_call(method, deadline=deadline, clock=clock, kwargs=kwargs)
        except Exception as exc:
            if retry >= MAXIMUM_RETRIES or not _transient(exc):
                raise
            delay = _retry_after(exc, retry + 1)
            if delay >= _remaining(deadline, clock):
                raise UnauthorizedOperation("L1 runtime cannot accommodate metadata retry") from exc
            sleeper(delay)
    raise AssertionError("unreachable metadata retry state")


def run_preflight(
    *, plan: Mapping[str, object], ledger: Sequence[DailyRequest], apis: ProviderApis,
    deadline: float | None = None, clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> Preflight:
    deadline = clock() + MAXIMUM_RUNTIME_SECONDS if deadline is None else deadline
    attempts = {"count": 0}
    calls = 1
    _validate_entitlement(
        _metadata_call(
            apis.get_dataset_range, attempts=attempts, deadline=deadline, clock=clock,
            sleeper=sleeper, kwargs={"dataset": DATASET},
        ),
        ledger,
    )
    period = plan["period"]  # type: ignore[index]
    symbology_end = (
        date.fromisoformat(str(period["end_date_inclusive"])) + timedelta(days=1)
    ).isoformat()
    calls += 1
    _validate_symbology(
        _metadata_call(
            apis.resolve, attempts=attempts, deadline=deadline, clock=clock,
            sleeper=sleeper,
            kwargs={
                "dataset": DATASET,
                "symbols": list(SYMBOLS),
                "stype_in": STYPE_IN,
                "stype_out": STYPE_OUT,
                "start_date": period["start_date_inclusive"],
                "end_date": symbology_end,
            },
        ),
        plan,
    )
    sizes: dict[str, int] = {}
    for item in ledger:
        query = item.provider_query()
        _zero_cost(
            _metadata_call(
                apis.get_cost, attempts=attempts, deadline=deadline, clock=clock,
                sleeper=sleeper, kwargs=query,
            )
        )
        size = _metadata_call(
            apis.get_billable_size, attempts=attempts, deadline=deadline, clock=clock,
            sleeper=sleeper, kwargs=query,
        )
        calls += 2
        if type(size) is not int or size < 0:
            raise IntegrityError("provider billable-size response is invalid")
        sizes[item.request_id] = size
    total = sum(sizes.values())
    nonempty = sum(size > 0 for size in sizes.values())
    if (
        calls != MAXIMUM_METADATA_CALLS
        or not MINIMUM_BILLABLE_BYTES <= total <= MAXIMUM_BILLABLE_BYTES
        or nonempty < MINIMUM_NONEMPTY_REQUESTS
    ):
        raise UnauthorizedOperation("fresh metadata call, coverage, or billable-size bounds failed")
    return Preflight(sizes, total, len(sizes) - nonempty, calls, attempts["count"])


def _iso_to_ns(value: str) -> int:
    parsed = _parse_provider_time(value, "DBN timestamp")
    return int(parsed.timestamp()) * 1_000_000_000


def verify_store_metadata(*, store: object, request: DailyRequest) -> None:
    metadata = getattr(store, "metadata", None)
    if (
        metadata is None
        or getattr(metadata, "dataset", None) != DATASET
        or str(getattr(metadata, "schema", "")) != SCHEMA
        or str(getattr(metadata, "stype_in", "")) != STYPE_IN
        or str(getattr(metadata, "stype_out", "")) != STYPE_OUT
        or getattr(metadata, "start", None) != _iso_to_ns(request.start)
        or getattr(metadata, "end", None) != _iso_to_ns(request.end)
        or sorted(str(item) for item in getattr(metadata, "symbols", ())) != sorted(SYMBOLS)
    ):
        raise IntegrityError("downloaded DBN metadata differs from its daily request")


def _paths(attempt: Path, request: DailyRequest) -> tuple[Path, Path]:
    year = request.trade_date[:4]
    return (
        attempt / "raw" / SCHEMA / year / f"{request.trade_date}.dbn.zst",
        attempt / "sidecars" / SCHEMA / year / f"{request.trade_date}.json",
    )


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp.{uuid.uuid4().hex[:16]}")
    with temporary.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _sidecar(
    *, root: Path, attempt: Path, plan: Mapping[str, object], request: DailyRequest,
    billable_bytes: int, raw: Path | None,
) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": "l1_execution_quarantine_sidecar/1.0.0",
        "state": (
            "PROVIDER_EMPTY_SESSION_NO_DBN"
            if raw is None
            else "SEALED_PROVIDER_CUSTODY_NOT_ROW_QUALITY_CERTIFIED"
        ),
        "plan_id": plan["plan_id"],
        "request": request.as_dict(),
        "billable_bytes": billable_bytes,
        "raw_relative_path": None if raw is None else raw.relative_to(root).as_posix(),
        "raw_size": None if raw is None else raw.stat().st_size,
        "raw_sha256": None if raw is None else sha256_file(raw),
        "dbn_rows_decoded": 0,
        "row_quality_certified": False,
        "alpha_source_authorized": False,
        "canonical_promotion_authorized": False,
    }
    return {**core, "manifest_id": sha256_json(core)}


def _verify_sidecar(
    *, root: Path, attempt: Path, plan: Mapping[str, object], request: DailyRequest,
    billable_bytes: int,
) -> dict[str, object] | None:
    raw, sidecar_path = _paths(attempt, request)
    if not raw.exists() and not sidecar_path.exists():
        return None
    if not sidecar_path.is_file() or sidecar_path.is_symlink():
        raise IntegrityError("staged request has an incomplete or linked sidecar")
    if billable_bytes and (not raw.is_file() or raw.is_symlink()):
        raise IntegrityError("staged request has an incomplete or linked DBN")
    sidecar = _object(sidecar_path, "L1 request sidecar")
    core = dict(sidecar)
    manifest_id = core.pop("manifest_id", None)
    if manifest_id != sha256_json(core):
        raise IntegrityError("L1 request sidecar hash is invalid")
    expected = _sidecar(
        root=root, attempt=attempt, plan=plan, request=request,
        billable_bytes=billable_bytes, raw=raw if billable_bytes else None,
    )
    if sidecar != expected or (billable_bytes == 0 and raw.exists()):
        raise IntegrityError("staged request differs from the current fixed ledger")
    return sidecar


def _status_code(exc: BaseException) -> int | None:
    direct = getattr(exc, "status_code", None)
    response = getattr(exc, "response", None)
    nested = getattr(response, "status_code", None)
    return direct if type(direct) is int else nested if type(nested) is int else None


def _transient(exc: BaseException) -> bool:
    status = _status_code(exc)
    return status == 429 or (status is not None and 500 <= status <= 599) or type(exc).__name__ in {
        "ConnectionError", "ConnectTimeout", "ReadTimeout", "Timeout", "TimeoutError",
        "ProtocolError",
    }


def _retry_after(exc: BaseException, retry_number: int) -> float:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None) or getattr(exc, "headers", None)
    value = headers.get("Retry-After") if isinstance(headers, Mapping) else None
    if value is not None:
        try:
            return min(300.0, max(0.0, float(value)))
        except (TypeError, ValueError):
            try:
                target = parsedate_to_datetime(str(value)).astimezone(timezone.utc)
                return min(300.0, max(0.0, (target - datetime.now(timezone.utc)).total_seconds()))
            except (TypeError, ValueError):
                pass
    return float(2 ** (retry_number - 1))


def _register_bytes(
    path: Path, *, total: dict[str, int], lock: threading.Lock,
) -> None:
    if not path.is_file():
        return
    size = path.stat().st_size
    with lock:
        total["bytes"] += size
        if total["bytes"] > MAXIMUM_STAGED_BYTES:
            raise UnauthorizedOperation("L1 staged-byte ceiling exceeded")


def _download_worker(
    *, index: int, work: queue.Queue[DailyRequest], root: Path, attempt: Path,
    plan: Mapping[str, object], preflight: Preflight,
    provider_factory: Callable[[], ProviderApis], stop: threading.Event,
    throttled: threading.Event, total: dict[str, int], total_lock: threading.Lock,
    call_state: dict[str, int], call_lock: threading.Lock,
    sleeper: Callable[[float], None], deadline: float, clock: Callable[[], float],
) -> WorkerResult:
    completed: list[str] = []
    failed_request_id: str | None = None
    attempts = 0
    try:
        _remaining(deadline, clock)
        apis = provider_factory()
        _remaining(deadline, clock)
        while not stop.is_set():
            _remaining(deadline, clock)
            if throttled.is_set() and index >= THROTTLED_WORKERS:
                break
            try:
                request = work.get_nowait()
            except queue.Empty:
                break
            failed_request_id = request.request_id
            for retry in range(MAXIMUM_RETRIES + 1):
                registered = False
                partial = _paths(attempt, request)[0].with_name(
                    f"{request.trade_date}.dbn.zst.partial.{uuid.uuid4().hex[:16]}"
                )
                partial.parent.mkdir(parents=True, exist_ok=True)
                try:
                    with call_lock:
                        call_state["attempts"] += 1
                        if call_state["attempts"] > MAXIMUM_TIMESERIES_ATTEMPTS:
                            raise UnauthorizedOperation("L1 timeseries call ceiling exceeded")
                    attempts += 1
                    store = _provider_call(
                        apis.get_range, deadline=deadline, clock=clock,
                        kwargs={
                            **request.provider_query(include_output_identity=True),
                            "path": str(partial),
                        },
                    )
                    if not partial.is_file() or partial.stat().st_size <= 0:
                        raise IntegrityError("provider did not create a nonempty DBN file")
                    verify_store_metadata(store=store, request=request)
                    del store  # release Databento's file handle before Windows finalization
                    _register_bytes(partial, total=total, lock=total_lock)
                    registered = True
                    raw, sidecar_path = _paths(attempt, request)
                    if raw.exists() or sidecar_path.exists():
                        raise IntegrityError("create-only L1 destination appeared during download")
                    sidecar = _sidecar(
                        root=root, attempt=attempt, plan=plan, request=request,
                        billable_bytes=preflight.billable_bytes[request.request_id], raw=partial,
                    )
                    sidecar["raw_relative_path"] = raw.relative_to(root).as_posix()
                    sidecar_core = dict(sidecar)
                    sidecar_core.pop("manifest_id")
                    sidecar["manifest_id"] = sha256_json(sidecar_core)
                    sidecar_partial = sidecar_path.with_name(
                        f"{sidecar_path.name}.partial.{uuid.uuid4().hex[:16]}"
                    )
                    sidecar_partial.parent.mkdir(parents=True, exist_ok=True)
                    with sidecar_partial.open("xb") as stream:
                        stream.write(canonical_bytes(sidecar) + b"\n")
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.link(partial, raw)
                    try:
                        os.link(sidecar_partial, sidecar_path)
                    except Exception:
                        raw.unlink(missing_ok=True)
                        raise
                    partial.unlink(missing_ok=True)
                    sidecar_partial.unlink(missing_ok=True)
                    completed.append(request.request_id)
                    failed_request_id = None
                    break
                except Exception as exc:
                    if partial.exists() and not registered:
                        _register_bytes(partial, total=total, lock=total_lock)
                    if _status_code(exc) == 429:
                        throttled.set()
                    if retry >= MAXIMUM_RETRIES or not _transient(exc):
                        raise
                    delay = _retry_after(exc, retry + 1)
                    if delay >= _remaining(deadline, clock):
                        raise UnauthorizedOperation(
                            "L1 runtime cannot accommodate timeseries retry"
                        ) from exc
                    sleeper(delay)
            work.task_done()
    except Exception as exc:
        stop.set()
        return WorkerResult(index, tuple(completed), attempts, type(exc).__name__, failed_request_id)
    return WorkerResult(index, tuple(completed), attempts, None, None)


def download_missing(
    *, root: Path, attempt: Path, plan: Mapping[str, object],
    requests: Sequence[DailyRequest], preflight: Preflight,
    provider_factory: Callable[[], ProviderApis], existing_staged_bytes: int = 0,
    sleeper: Callable[[float], None] = time.sleep,
    deadline: float | None = None, clock: Callable[[], float] = time.monotonic,
) -> tuple[WorkerResult, ...]:
    """Download each missing request once, with bounded retries owned by one worker."""

    deadline = clock() + MAXIMUM_RUNTIME_SECONDS if deadline is None else deadline
    work: queue.Queue[DailyRequest] = queue.Queue()
    for request in requests:
        work.put(request)
    stop = threading.Event()
    throttled = threading.Event()
    total = {"bytes": existing_staged_bytes}
    total_lock = threading.Lock()
    call_state = {"attempts": 0}
    call_lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=MAXIMUM_WORKERS, thread_name_prefix="l1-bbo1s") as pool:
        futures = [
            pool.submit(
                _download_worker,
                index=index, work=work, root=root, attempt=attempt, plan=plan,
                preflight=preflight, provider_factory=provider_factory, stop=stop,
                throttled=throttled, total=total, total_lock=total_lock,
                call_state=call_state, call_lock=call_lock, sleeper=sleeper,
                deadline=deadline, clock=clock,
            )
            for index in range(MAXIMUM_WORKERS)
        ]
        results = tuple(future.result() for future in futures)
    failures = [result for result in results if result.failure_type is not None]
    if failures:
        error = IntegrityError(f"L1 download worker failed closed: {failures[0].failure_type}")
        error.failed_request_id = failures[0].failed_request_id  # type: ignore[attr-defined]
        raise error
    if not work.empty():
        raise IntegrityError("L1 worker pool stopped before completing its queue")
    return results


def _tree_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    pending = [path]
    while pending:
        current = pending.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                if entry.is_symlink():
                    raise IntegrityError("L1 staging contains a symbolic link")
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    total += Path(entry.path).stat().st_size
                else:
                    raise IntegrityError("L1 staging contains an unsupported object")
    return total


def _required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "plan_id": str(plan["plan_id"]),
        "implementation_sha256": str(plan["implementation_sha256"]),
        "provider": "Databento",
        "dataset": DATASET,
        "schema": SCHEMA,
        "symbols": ",".join(SYMBOLS),
        "period": "2025-09-01/2026-08-31",
        "request_count": "261",
        "maximum_parallel_downloads": str(MAXIMUM_WORKERS),
        "maximum_external_cost_usd": "0",
        "minimum_billable_bytes": str(MINIMUM_BILLABLE_BYTES),
        "minimum_nonempty_requests": str(MINIMUM_NONEMPTY_REQUESTS),
        "maximum_billable_bytes": str(MAXIMUM_BILLABLE_BYTES),
        "maximum_staged_bytes": str(MAXIMUM_STAGED_BYTES),
        "staging_only": "true",
        "row_decode": "false",
        "alpha_source_activation": "false",
        "canonical_promotion": "false",
        "credential_source": CREDENTIAL_SOURCE,
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return _required_scope(root=root, plan=plan)


def _write_or_verify(path: Path, payload: Mapping[str, object], label: str) -> None:
    expected = canonical_bytes(payload) + b"\n"
    if path.exists():
        if not path.is_file() or path.is_symlink() or path.read_bytes() != expected:
            raise IntegrityError(f"existing {label} differs")
        return
    _atomic_json(path, payload)


def _write_failure_receipt(
    *, root: Path, attempt: Path, plan: Mapping[str, object],
    authorization: OperationReceipt, claim: Path, exc: Exception,
) -> None:
    sidecar_root = attempt / "sidecars" / SCHEMA
    completed = sum(
        path.is_file() and not path.is_symlink()
        for path in sidecar_root.glob("*/*.json")
    )
    core = {
        "schema_version": "l1_execution_quarantine_failure/1.0.0",
        "state": "FAILED_CLOSED_RESUMABLE",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "implementation_sha256": plan["implementation_sha256"],
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_claim_sha256": sha256_file(claim),
        "failure_type": type(exc).__name__,
        "failed_request_id": getattr(exc, "failed_request_id", None),
        "completed_sidecar_count": completed,
        "automatic_resume_supported": True,
        "canonical_promotion_performed": False,
        "row_quality_certified": False,
    }
    payload = {**core, "failure_id": sha256_json(core)}
    _atomic_json(attempt / "failures" / f"{uuid.uuid4().hex[:16]}.json", payload)


def execute_authorized_acquisition(
    *, root: Path, authorization: OperationReceipt,
    provider_factory: Callable[[], ProviderApis], credential_source: str,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    """Acquire only sealed DBN custody; this function cannot promote or decode it."""

    started = clock()
    deadline = started + MAXIMUM_RUNTIME_SECONDS
    root = root.resolve(strict=True)
    plan = load_plan(root=root)
    ledger = build_request_ledger(plan)
    if credential_source != CREDENTIAL_SOURCE:
        raise UnauthorizedOperation("L1 credential source is not bound")
    environment_check(root)
    free = getattr(disk_usage(root), "free", None)
    if type(free) is not int or free < REQUIRED_FREE_BYTES:
        raise UnauthorizedOperation("insufficient free space for L1 acquisition")
    boundary = RepoBoundary(root)
    scope = _required_scope(root=root, plan=plan)
    attempt = root / STAGING_ROOT / authorization.receipt_id
    lock_path = root / LOCK_PATH
    boundary.assert_active_path(
        lock_path.absolute(), purpose="L1 acquisition process lock", subtree="state/locks"
    )
    instance = SingleInstance(lock_path)
    if not instance.acquire():
        raise UnauthorizedOperation("another L1 acquisition or resume is already running")
    try:
        return _execute_locked(
            root=root, authorization=authorization, provider_factory=provider_factory,
            sleeper=sleeper, clock=clock, deadline=deadline, boundary=boundary,
            plan=plan, ledger=ledger, attempt=attempt, scope=scope,
        )
    finally:
        instance.release()


def _execute_locked(
    *, root: Path, authorization: OperationReceipt,
    provider_factory: Callable[[], ProviderApis], sleeper: Callable[[float], None],
    clock: Callable[[], float], deadline: float, boundary: RepoBoundary,
    plan: Mapping[str, object], ledger: Sequence[DailyRequest], attempt: Path,
    scope: Mapping[str, str],
) -> dict[str, object]:
    use_path = root / "state" / "authorization_uses" / f"{authorization.receipt_id}.json"
    claim = (
        authorization.assert_consumed(
            boundary, operation=OPERATION,
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            required_scope=scope,
        )
        if use_path.exists()
        else authorization.consume(
            boundary, operation=OPERATION,
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            required_scope=scope,
        )
    )
    boundary.assert_active_path(
        attempt.absolute(), purpose="L1 execution quarantine attempt",
        subtree=STAGING_ROOT.as_posix(),
    )
    attempt.mkdir(parents=True, exist_ok=True)
    request_plan = {
        "schema_version": "l1_execution_quarantine_request_ledger/1.0.0",
        "plan_id": plan["plan_id"],
        "requests": [item.as_dict() for item in ledger],
        "request_set_id": sha256_json([item.as_dict() for item in ledger]),
    }
    try:
        _write_or_verify(attempt / "request_plan.json", request_plan, "L1 request ledger")
        _remaining(deadline, clock)
        apis = provider_factory()
        _remaining(deadline, clock)
        preflight = run_preflight(
            plan=plan, ledger=ledger, apis=apis, deadline=deadline, clock=clock,
            sleeper=sleeper,
        )
        _write_or_verify(attempt / "preflight.json", preflight.as_dict(), "L1 preflight")
        missing_positive: list[DailyRequest] = []
        for request in ledger:
            _remaining(deadline, clock)
            billable = preflight.billable_bytes[request.request_id]
            if _verify_sidecar(
                root=root, attempt=attempt, plan=plan, request=request,
                billable_bytes=billable,
            ) is None:
                if billable == 0:
                    _, sidecar_path = _paths(attempt, request)
                    _atomic_json(
                        sidecar_path,
                        _sidecar(
                            root=root, attempt=attempt, plan=plan, request=request,
                            billable_bytes=0, raw=None,
                        ),
                    )
                else:
                    missing_positive.append(request)
        existing_size = _tree_size(attempt)
        if existing_size > MAXIMUM_STAGED_BYTES:
            raise UnauthorizedOperation("existing L1 attempt exceeds its staged-byte ceiling")
        if missing_positive:
            download_missing(
                root=root, attempt=attempt, plan=plan, requests=missing_positive,
                preflight=preflight, provider_factory=provider_factory,
                existing_staged_bytes=existing_size, sleeper=sleeper,
                deadline=deadline, clock=clock,
            )
        staged = []
        for request in ledger:
            _remaining(deadline, clock)
            staged.append(_verify_sidecar(
                root=root, attempt=attempt, plan=plan, request=request,
                billable_bytes=preflight.billable_bytes[request.request_id],
            ))
        if any(item is None for item in staged):
            raise IntegrityError("L1 acquisition is incomplete")
        staged_size = _tree_size(attempt)
        if staged_size > MAXIMUM_STAGED_BYTES:
            raise UnauthorizedOperation("completed L1 attempt exceeds its staged-byte ceiling")
        manifest_core = {
            "schema_version": "l1_execution_quarantine_manifest/1.0.0",
            "state": "SEALED_PROVIDER_CUSTODY_NOT_ROW_QUALITY_CERTIFIED",
            "plan_id": plan["plan_id"],
            "plan_sha256": sha256_file(root / PLAN_PATH),
            "implementation_sha256": plan["implementation_sha256"],
            "authorization_receipt_id": authorization.receipt_id,
            "authorization_claim_sha256": sha256_file(claim),
            "request_count": len(ledger),
            "nonempty_request_count": len(ledger) - preflight.empty_request_count,
            "empty_request_count": preflight.empty_request_count,
            "fresh_total_billable_bytes": preflight.total_billable_bytes,
            "staged_files": staged,
            "staged_files_id": sha256_json(staged),
            "maximum_parallel_downloads": MAXIMUM_WORKERS,
            "throttled_parallel_downloads": THROTTLED_WORKERS,
            "dbn_rows_decoded": 0,
            "row_quality_certified": False,
            "alpha_source_authorized": False,
            "active_source_contract_changed": False,
            "canonical_promotion_performed": False,
        }
        manifest = {**manifest_core, "manifest_id": sha256_json(manifest_core)}
        _write_or_verify(attempt / "manifest.json", manifest, "L1 terminal manifest")
        record = {
            "schema_version": "l1_execution_quarantine_acquisition/1.0.0",
            "state": "PUBLISHED_SEALED_PROVIDER_CUSTODY_ONLY",
            "record_id": manifest["manifest_id"],
            "manifest_path": (attempt / "manifest.json").relative_to(root).as_posix(),
            "manifest_sha256": sha256_file(attempt / "manifest.json"),
            "plan_id": plan["plan_id"],
            "canonical_promotion_performed": False,
            "row_quality_certified": False,
        }
        event = {
            "schema_version": "l1_execution_quarantine_event/1.0.0",
            "event_type": "SEALED_CUSTODY_ACQUIRED",
            "record_id": record["record_id"],
            "authorization_receipt_id": authorization.receipt_id,
        }
        record_path = root / RECORD_ROOT / f"{record['record_id']}.json"
        event_path = root / EVENT_ROOT / f"{record['record_id']}.json"
        boundary.assert_active_path(
            record_path.absolute(), purpose="L1 acquisition record",
            subtree=RECORD_ROOT.as_posix(),
        )
        boundary.assert_active_path(
            event_path.absolute(), purpose="L1 acquisition event",
            subtree=EVENT_ROOT.as_posix(),
        )
        _write_or_verify(record_path, record, "L1 acquisition record")
        _write_or_verify(event_path, event, "L1 acquisition event")
        return {
            "record_id": record["record_id"],
            "attempt_path": attempt.relative_to(root).as_posix(),
            "request_count": len(ledger),
            "nonempty_request_count": len(ledger) - preflight.empty_request_count,
            "fresh_total_billable_bytes": preflight.total_billable_bytes,
            "maximum_parallel_downloads": MAXIMUM_WORKERS,
            "row_quality_certified": False,
            "canonical_promotion_performed": False,
        }
    except Exception as exc:
        _write_failure_receipt(
            root=root, attempt=attempt, plan=plan, authorization=authorization,
            claim=claim, exc=exc,
        )
        raise


def execute_receipt_file_acquisition(
    *, root: Path, authorization_path: Path,
    provider_factory: Callable[[], ProviderApis] | None = None,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    """Run or resume from an already-issued canonical external receipt."""

    root = root.resolve(strict=True)
    receipt_path = authorization_path if authorization_path.is_absolute() else root / authorization_path
    receipt = OperationReceipt.load_external(receipt_path.resolve(strict=True), RepoBoundary(root))
    factory = provider_factory or (lambda: build_file_provider_apis(root=root))
    return execute_authorized_acquisition(
        root=root, authorization=receipt, provider_factory=factory,
        credential_source=CREDENTIAL_SOURCE, disk_usage=disk_usage,
        environment_check=environment_check, sleeper=sleeper, clock=clock,
    )


__all__ = [
    "CREDENTIAL_SOURCE", "DailyRequest", "MAXIMUM_WORKERS", "OPERATION",
    "PLAN_PATH", "Preflight", "ProviderApis", "STAGING_ROOT", "SYMBOLS",
    "build_file_provider_apis", "build_request_ledger", "download_missing",
    "execute_authorized_acquisition", "execute_receipt_file_acquisition",
    "load_plan", "required_scope",
    "run_preflight", "verify_store_metadata",
]
