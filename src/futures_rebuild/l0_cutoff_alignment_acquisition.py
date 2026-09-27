"""Resumable four-worker L0 cutoff-alignment acquisition into sealed staging."""

from __future__ import annotations

import json
import os
import queue
import re
import shutil
import threading
import time
import uuid
import warnings
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Final

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .live_cockpit.databento_auth import resolve_databento_api_key
from .live_cockpit.single_instance import SingleInstance
from .runtime_environment import require_locked_repository_environment


PLAN_PATH: Final = Path("configs/l0_cutoff_alignment_acquisition_v1.json")
SOURCE_CONTRACT_PATH: Final = Path("configs/source_contract.json")
OPERATION: Final = "ACQUIRE_L0_AUG31_CUTOFF_ALIGNMENT_QUARANTINE_V1_ONCE"
STAGING_ROOT: Final = Path("state/provider_acquisition_staging/l0_cutoff_alignment_v1")
RECORD_ROOT: Final = Path("state/provider_acquisitions/l0_cutoff_alignment_v1")
EVENT_ROOT: Final = Path("state/provider_acquisition_events/l0_cutoff_alignment_v1")
LOCK_PATH: Final = Path("state/locks/l0_cutoff_alignment_v1.lock")
CREDENTIAL_SOURCE: Final = "file api.env"
DATASET: Final = "GLBX.MDP3"
START_INCLUSIVE: Final = "2026-07-14T00:00:00Z"
END_EXCLUSIVE: Final = "2026-09-01T00:00:00Z"
INTERVAL_NAME: Final = "2026-07-14_2026-09-01"
STYPE_OUT: Final = "instrument_id"
STANDARD_ROOTS: Final = (
    "ES", "NQ", "RTY", "YM", "CL", "NG", "RB", "HO", "GC", "SI", "HG",
    "PL", "SR3", "SR1", "ZQ", "TN", "ZT", "ZF", "ZN", "ZB", "UB", "6A",
    "6B", "6C", "6E", "6J", "6M", "6N", "6S", "ZC", "ZS", "ZL", "ZM",
    "ZW", "KE", "LE", "HE", "GF", "BTC", "ETH", "PA",
)
MICRO_ROOTS: Final = (
    "M2K", "M6A", "M6B", "M6E", "MBT", "MCD", "MCL", "MES", "MET",
    "MGC", "MHG", "MJY", "MNG", "MNQ", "MSF", "MYM", "SIL",
)
ROOTS: Final = STANDARD_ROOTS + MICRO_ROOTS
FAMILIES: Final = (
    "definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s", "ohlcv-1h",
    "ohlcv-1d",
)
FAMILY_DIRECTORIES: Final = {
    family: family.replace("-", "_") for family in FAMILIES
}
MAXIMUM_WORKERS: Final = 4
MAXIMUM_CONCURRENT_OHLCV_1S: Final = 2
MAXIMUM_RETRIES: Final = 3
MAXIMUM_BILLABLE_BYTES: Final = 5_000_000_000
MAXIMUM_STAGED_BYTES: Final = 2_000_000_000
REQUIRED_FREE_BYTES: Final = 5_000_000_000
MAXIMUM_RUNTIME_SECONDS: Final = 43_200
MAXIMUM_METADATA_CALLS: Final = 815
MAXIMUM_METADATA_ATTEMPTS: Final = 3_260
MAXIMUM_TIMESERIES_ATTEMPTS: Final = 1_624


@dataclass(frozen=True)
class ProviderApis:
    get_dataset_range: Callable[..., object]
    resolve: Callable[..., object]
    get_cost: Callable[..., object]
    get_billable_size: Callable[..., object]
    get_range: Callable[..., object]


@dataclass(frozen=True)
class L0Request:
    market: str
    family: str
    directory: str
    symbol: str
    stype_in: str
    request_id: str

    def provider_query(self, *, include_output_identity: bool = False) -> dict[str, object]:
        query: dict[str, object] = {
            "dataset": DATASET,
            "schema": self.family,
            "stype_in": self.stype_in,
            "symbols": [self.symbol],
            "start": START_INCLUSIVE,
            "end": END_EXCLUSIVE,
        }
        if include_output_identity:
            query["stype_out"] = STYPE_OUT
        return query

    def as_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "family": self.family,
            "directory": self.directory,
            "request_id": self.request_id,
            "query": self.provider_query(include_output_identity=True),
        }


@dataclass(frozen=True)
class Preflight:
    billable_bytes: dict[str, int]
    total_billable_bytes: int
    empty_request_count: int
    metadata_calls: int
    metadata_attempts: int

    def as_dict(self) -> dict[str, object]:
        core: dict[str, object] = {
            "schema_version": "l0_cutoff_alignment_preflight/1.0.0",
            "state": "PASS_ZERO_COST_CURRENT_ENTITLEMENT",
            "billable_bytes_by_request": self.billable_bytes,
            "total_billable_bytes": self.total_billable_bytes,
            "empty_request_count": self.empty_request_count,
            "metadata_calls": self.metadata_calls,
            "metadata_attempts": self.metadata_attempts,
            "external_cost_usd": "0",
        }
        return {**core, "preflight_id": sha256_json(core)}


@dataclass(frozen=True)
class WorkerResult:
    worker_index: int
    completed_request_ids: tuple[str, ...]
    timeseries_attempts: int
    failure_type: str | None
    failed_request_id: str | None


class ProviderWarningCollector:
    """Capture sanitized provider warnings emitted by concurrent SDK calls."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._local = threading.local()
        self._records: dict[tuple[str, str, tuple[str, ...]], int] = {}
        self._prior_showwarning: Callable[..., object] | None = None
        self._prior_filters: list[tuple[object, ...]] | None = None

    def __enter__(self) -> ProviderWarningCollector:
        self._prior_showwarning = warnings.showwarning
        self._prior_filters = list(warnings.filters)

        def capture(
            message: Warning | str,
            category: type[Warning],
            _filename: str,
            _lineno: int,
            file: object | None = None,
            line: str | None = None,
        ) -> None:
            request_id = getattr(self._local, "request_id", None)
            if request_id is None:
                assert self._prior_showwarning is not None
                self._prior_showwarning(message, category, _filename, _lineno, file, line)
                return
            if issubclass(category, (ResourceWarning, DeprecationWarning, PendingDeprecationWarning)):
                return
            dates = tuple(sorted(set(re.findall(r"20\d{2}-\d{2}-\d{2}", str(message)))))
            key = (str(request_id), category.__name__, dates)
            with self._lock:
                self._records[key] = self._records.get(key, 0) + 1

        # ponytail: process-global warning capture is safe under this operation's
        # exclusive lock; use SDK-native warning callbacks if concurrent provider
        # operations inside one process are ever required.
        warnings.simplefilter("always")
        warnings.showwarning = capture
        return self

    def __exit__(self, *_args: object) -> None:
        assert self._prior_showwarning is not None
        assert self._prior_filters is not None
        warnings.showwarning = self._prior_showwarning
        warnings.filters[:] = self._prior_filters

    def set_request(self, request_id: str) -> None:
        self._local.request_id = request_id

    def clear_request(self) -> None:
        self._local.request_id = None

    def for_request(self, request_id: str) -> list[dict[str, object]]:
        with self._lock:
            records = [
                {
                    "category": category,
                    "dates": list(dates),
                    "count": count,
                }
                for (selected, category, dates), count in self._records.items()
                if selected == request_id
            ]
        return sorted(records, key=lambda item: (str(item["category"]), item["dates"]))


def build_file_provider_apis(
    *, root: Path, historical_factory: Callable[..., object] | None = None,
) -> ProviderApis:
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


def _source_binding(root: Path) -> dict[str, object]:
    path = root / SOURCE_CONTRACT_PATH
    source = _object(path, "source contract")
    inventory = source.get("complete_inventory")
    universe = source.get("universe")
    if not isinstance(inventory, Mapping) or not isinstance(universe, Mapping):
        raise IntegrityError("source contract custody binding is invalid")
    return {
        "source_contract_path": SOURCE_CONTRACT_PATH.as_posix(),
        "source_contract_sha256": sha256_file(path),
        "source_contract_id": source.get("contract_id"),
        "complete_inventory_path": inventory.get("path"),
        "complete_inventory_sha256": inventory.get("sha256"),
        "complete_inventory_content_sha256": inventory.get("content_inventory_sha256"),
        "complete_inventory_id": inventory.get("inventory_id"),
        "complete_inventory_file_count": inventory.get("file_count"),
        "standard_roots": list(universe.get("standard_roots", ())),
        "micro_roots": list(universe.get("deferred_micro_roots", ())),
    }


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH, "L0 cutoff-alignment plan")
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    limits = plan.get("limits")
    forbidden = plan.get("forbidden")
    families = plan.get("families")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version") != "l0_cutoff_alignment_plan/1.0.0"
        or plan.get("state") != "PREPARED_REQUIRES_SEPARATE_PROVIDER_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("dataset") != DATASET
        or plan.get("interval")
        != {"start_inclusive_utc": START_INCLUSIVE, "end_exclusive_utc": END_EXCLUSIVE}
        or tuple(plan.get("standard_roots", ())) != STANDARD_ROOTS
        or tuple(plan.get("micro_roots", ())) != MICRO_ROOTS
        or tuple(plan.get("request_root_order", ())) != ROOTS
        or tuple(item.get("schema") for item in families or () if isinstance(item, Mapping))
        != FAMILIES
        or plan.get("request_count") != len(ROOTS) * len(FAMILIES)
        or plan.get("implementation_sha256") != sha256_file(Path(__file__))
        or plan.get("source_binding") != _source_binding(root)
        or limits
        != {
            "maximum_billable_bytes": MAXIMUM_BILLABLE_BYTES,
            "maximum_concurrent_ohlcv_1s": MAXIMUM_CONCURRENT_OHLCV_1S,
            "maximum_host_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_metadata_attempts": MAXIMUM_METADATA_ATTEMPTS,
            "maximum_metadata_calls": MAXIMUM_METADATA_CALLS,
            "maximum_parallel_downloads": MAXIMUM_WORKERS,
            "maximum_retries_per_request": MAXIMUM_RETRIES,
            "maximum_staged_bytes": MAXIMUM_STAGED_BYTES,
            "maximum_timeseries_attempts": MAXIMUM_TIMESERIES_ATTEMPTS,
            "required_free_disk_bytes": REQUIRED_FREE_BYTES,
        }
        or not isinstance(forbidden, Mapping)
        or set(forbidden)
        != {
            "active_data_mutation",
            "alpha_feature_or_target_use",
            "canonical_promotion",
            "credential_log_stage_or_report",
            "data_dbn_write",
            "dbn_row_decode",
            "holdout_or_forward_row_read",
            "model_fit_prediction_or_evaluation",
            "overwrite_existing_files",
            "trading_or_order_access",
        }
        or set(forbidden.values()) != {True}
    ):
        raise IntegrityError("L0 cutoff-alignment plan drifted")
    expected_families = [
        {
            "directory": FAMILY_DIRECTORIES[family],
            "schema": family,
            "stype_in": "parent" if family == "definition" else "continuous",
            "symbol_template": "{root}.FUT" if family == "definition" else "{root}.v.0",
        }
        for family in FAMILIES
    ]
    if families != expected_families:
        raise IntegrityError("L0 family query contract drifted")
    if len(build_request_ledger(plan)) != plan["request_count"]:
        raise IntegrityError("L0 request count drifted")
    return plan


def build_request_ledger(plan: Mapping[str, object]) -> tuple[L0Request, ...]:
    requests: list[L0Request] = []
    family_specs = plan.get("families")
    if not isinstance(family_specs, list):
        raise IntegrityError("L0 family plan is invalid")
    for market in plan.get("request_root_order", ()):  # type: ignore[assignment]
        if not isinstance(market, str):
            raise IntegrityError("L0 market is invalid")
        for spec in family_specs:
            if not isinstance(spec, Mapping):
                raise IntegrityError("L0 family specification is invalid")
            family = str(spec["schema"])
            stype_in = str(spec["stype_in"])
            symbol = str(spec["symbol_template"]).format(root=market)
            core = {
                "dataset": DATASET,
                "schema": family,
                "stype_in": stype_in,
                "stype_out": STYPE_OUT,
                "symbols": [symbol],
                "start": START_INCLUSIVE,
                "end": END_EXCLUSIVE,
                "market": market,
            }
            requests.append(
                L0Request(
                    market=market,
                    family=family,
                    directory=str(spec["directory"]),
                    symbol=symbol,
                    stype_in=stype_in,
                    request_id=sha256_json(core),
                )
            )
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


def _validate_entitlement(value: object) -> None:
    if not isinstance(value, Mapping):
        raise IntegrityError("provider dataset range is invalid")
    ranges = value.get("schema")
    if not isinstance(ranges, Mapping):
        raise IntegrityError("provider schema ranges are absent")
    requested_start = _parse_provider_time(START_INCLUSIVE, "request start")
    requested_end = _parse_provider_time(END_EXCLUSIVE, "request end")
    for family in FAMILIES:
        selected = ranges.get(family)
        if not isinstance(selected, Mapping):
            raise UnauthorizedOperation(f"current {family} entitlement is absent")
        if (
            _parse_provider_time(selected.get("start"), "range start") > requested_start
            or _parse_provider_time(selected.get("end"), "range end") < requested_end
        ):
            raise UnauthorizedOperation(f"current {family} entitlement does not cover the interval")


def _validate_symbology(
    value: object, *, stype_in: str, symbols: Sequence[str],
) -> None:
    if not isinstance(value, Mapping):
        raise IntegrityError("provider symbology response is invalid")
    partial = value.get("partial")
    not_found = value.get("not_found")
    if not isinstance(partial, list) or not isinstance(not_found, list):
        raise IntegrityError(f"{stype_in} symbology resolution state is invalid")
    if not_found or (stype_in == "continuous" and partial):
        raise UnauthorizedOperation(f"{stype_in} symbology is partial or unresolved")
    result = value.get("result")
    if (
        not isinstance(result, Mapping)
        or value.get("status") != (1 if stype_in == "parent" and partial else 0)
        or set(value.get("symbols", ())) != set(symbols)
    ):
        raise UnauthorizedOperation(f"{stype_in} symbology does not resolve every symbol")
    if (
        value.get("stype_in") != stype_in
        or value.get("stype_out") != STYPE_OUT
        or value.get("start_date") != "2026-07-14"
        or value.get("end_date") != "2026-09-01"
    ):
        raise IntegrityError(f"{stype_in} symbology response differs from the fixed interval")
    start = _parse_provider_time(START_INCLUSIVE, "symbology start").date()
    end = _parse_provider_time(END_EXCLUSIVE, "symbology end").date()
    for symbol in symbols:
        keys = (
            [symbol]
            if stype_in == "continuous" and symbol in result
            else [key for key in result if str(key).startswith(symbol.removesuffix(".FUT"))]
        )
        entries = []
        for key in keys:
            selected = result[key]
            if not isinstance(selected, list):
                raise IntegrityError(f"{stype_in} symbology mapping is malformed")
            entries.extend(selected)
        if not entries:
            raise UnauthorizedOperation(f"{stype_in} symbology has an empty mapping")
        intervals = []
        for entry in entries:
            if not isinstance(entry, Mapping) or not {"d0", "d1", "s"} <= set(entry):
                raise IntegrityError(f"{stype_in} symbology interval is malformed")
            try:
                left = datetime.fromisoformat(str(entry["d0"])).date()
                right = datetime.fromisoformat(str(entry["d1"])).date()
            except ValueError as exc:
                raise IntegrityError(f"{stype_in} symbology interval date is invalid") from exc
            if left >= right or str(entry["s"]) == "":
                raise IntegrityError(f"{stype_in} symbology interval is invalid")
            if right > start and left < end:
                intervals.append((max(left, start), min(right, end)))
        cursor = start
        for left, right in sorted(intervals):
            if left > cursor:
                raise UnauthorizedOperation(f"{stype_in} symbology contains a coverage gap")
            cursor = max(cursor, right)
        if cursor < end:
            raise UnauthorizedOperation(f"{stype_in} symbology does not cover the interval")


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
        raise UnauthorizedOperation("L0 cutoff-alignment runtime exhausted")
    return remaining


def _provider_call(
    method: Callable[..., object], *, deadline: float, clock: Callable[[], float],
    kwargs: Mapping[str, object],
) -> object:
    remaining = _remaining(deadline, clock)
    owner = getattr(method, "__self__", None)
    if owner is not None and hasattr(owner, "TIMEOUT"):
        try:
            owner.TIMEOUT = min(900.0, remaining)
        except (AttributeError, TypeError) as exc:
            raise IntegrityError("provider API timeout cannot be bounded") from exc
    value = method(**kwargs)
    _remaining(deadline, clock)
    return value


def _status_code(exc: BaseException) -> int | None:
    direct = getattr(exc, "status_code", None)
    response = getattr(exc, "response", None)
    nested = getattr(response, "status_code", None)
    return direct if type(direct) is int else nested if type(nested) is int else None


def _transient(exc: BaseException) -> bool:
    status = _status_code(exc)
    return (
        status == 429
        or (status is not None and 500 <= status <= 599)
        or type(exc).__name__ in {
            "ConnectionError", "ConnectTimeout", "ReadTimeout", "Timeout", "TimeoutError",
            "ProtocolError",
        }
        or (
            type(exc).__name__ == "BentoError"
            and str(exc).startswith("Error streaming response:")
        )
    )


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


def _metadata_call(
    method: Callable[..., object], *, attempts: dict[str, int], deadline: float,
    clock: Callable[[], float], sleeper: Callable[[float], None],
    kwargs: Mapping[str, object],
) -> object:
    for retry in range(MAXIMUM_RETRIES + 1):
        attempts["count"] += 1
        if attempts["count"] > MAXIMUM_METADATA_ATTEMPTS:
            raise UnauthorizedOperation("L0 metadata-attempt ceiling exceeded")
        try:
            return _provider_call(method, deadline=deadline, clock=clock, kwargs=kwargs)
        except Exception as exc:
            if retry >= MAXIMUM_RETRIES or not _transient(exc):
                raise
            delay = _retry_after(exc, retry + 1)
            if delay >= _remaining(deadline, clock):
                raise UnauthorizedOperation("L0 runtime cannot accommodate metadata retry") from exc
            sleeper(delay)
    raise AssertionError("unreachable metadata retry state")


def run_preflight(
    *, plan: Mapping[str, object], ledger: Sequence[L0Request], apis: ProviderApis,
    deadline: float | None = None, clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> Preflight:
    del plan
    deadline = clock() + MAXIMUM_RUNTIME_SECONDS if deadline is None else deadline
    attempts = {"count": 0}
    calls = 1
    _validate_entitlement(
        _metadata_call(
            apis.get_dataset_range, attempts=attempts, deadline=deadline, clock=clock,
            sleeper=sleeper, kwargs={"dataset": DATASET},
        )
    )
    for stype_in, symbols in (
        ("parent", [f"{root}.FUT" for root in ROOTS]),
        ("continuous", [f"{root}.v.0" for root in ROOTS]),
    ):
        calls += 1
        _validate_symbology(
            _metadata_call(
                apis.resolve, attempts=attempts, deadline=deadline, clock=clock,
                sleeper=sleeper,
                kwargs={
                    "dataset": DATASET,
                    "symbols": symbols,
                    "stype_in": stype_in,
                    "stype_out": STYPE_OUT,
                    "start_date": "2026-07-14",
                    "end_date": "2026-09-01",
                },
            ),
            stype_in=stype_in,
            symbols=symbols,
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
        if item.family.startswith("ohlcv-") and size == 0:
            raise UnauthorizedOperation("an OHLCV request has no billable coverage")
        sizes[item.request_id] = size
    total = sum(sizes.values())
    if calls != MAXIMUM_METADATA_CALLS or total <= 0 or total > MAXIMUM_BILLABLE_BYTES:
        raise UnauthorizedOperation("fresh metadata call or billable-size bounds failed")
    return Preflight(sizes, total, sum(size == 0 for size in sizes.values()), calls, attempts["count"])


def _iso_to_ns(value: str) -> int:
    return int(_parse_provider_time(value, "DBN timestamp").timestamp()) * 1_000_000_000


def verify_store_metadata(*, store: object, request: L0Request) -> None:
    metadata = getattr(store, "metadata", None)
    if (
        metadata is None
        or getattr(metadata, "dataset", None) != DATASET
        or str(getattr(metadata, "schema", "")) != request.family
        or str(getattr(metadata, "stype_in", "")) != request.stype_in
        or str(getattr(metadata, "stype_out", "")) != STYPE_OUT
        or getattr(metadata, "start", None) != _iso_to_ns(START_INCLUSIVE)
        or getattr(metadata, "end", None) != _iso_to_ns(END_EXCLUSIVE)
        or [str(item) for item in getattr(metadata, "symbols", ())] != [request.symbol]
    ):
        raise IntegrityError("downloaded DBN metadata differs from its L0 request")


def _paths(attempt: Path, request: L0Request) -> tuple[Path, Path]:
    raw = (
        attempt / "raw" / request.directory / request.market / "2026"
        / f"{INTERVAL_NAME}.dbn.zst"
    )
    return raw, raw.with_name(f"{raw.name}.manifest.json")


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp.{uuid.uuid4().hex[:12]}")
    with temporary.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _sidecar(
    *, root: Path, plan: Mapping[str, object], request: L0Request,
    billable_bytes: int, raw: Path | None, raw_destination: Path,
    provider_warnings: Sequence[Mapping[str, object]] = (),
) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": "l0_cutoff_alignment_sidecar/1.0.0",
        "state": (
            "PROVIDER_EMPTY_INTERVAL_NO_DBN"
            if raw is None
            else "SEALED_PROVIDER_CUSTODY_NOT_ROW_QUALITY_CERTIFIED"
        ),
        "plan_id": plan["plan_id"],
        "request": request.as_dict(),
        "billable_bytes": billable_bytes,
        "raw_relative_path": None if raw is None else raw_destination.relative_to(root).as_posix(),
        "raw_size": None if raw is None else raw.stat().st_size,
        "raw_sha256": None if raw is None else sha256_file(raw),
        "provider_warnings": list(provider_warnings),
        "dbn_rows_decoded": 0,
        "row_quality_certified": False,
        "alpha_source_authorized": False,
        "active_source_contract_changed": False,
        "canonical_promotion_performed": False,
        "holdout_or_forward_rows_read": False,
    }
    return {**core, "manifest_id": sha256_json(core)}


def _verify_sidecar(
    *, root: Path, attempt: Path, plan: Mapping[str, object], request: L0Request,
    billable_bytes: int,
) -> dict[str, object] | None:
    raw, sidecar_path = _paths(attempt, request)
    if not raw.exists() and not sidecar_path.exists():
        return None
    if not sidecar_path.is_file() or sidecar_path.is_symlink():
        raise IntegrityError("staged L0 request has an incomplete or linked sidecar")
    if billable_bytes and (not raw.is_file() or raw.is_symlink()):
        raise IntegrityError("staged L0 request has an incomplete or linked DBN")
    sidecar = _object(sidecar_path, "L0 request sidecar")
    core = dict(sidecar)
    manifest_id = core.pop("manifest_id", None)
    if manifest_id != sha256_json(core):
        raise IntegrityError("L0 request sidecar hash is invalid")
    provider_warnings = sidecar.get("provider_warnings")
    if not isinstance(provider_warnings, list):
        raise IntegrityError("L0 request warning summary is invalid")
    expected = _sidecar(
        root=root,
        plan=plan,
        request=request,
        billable_bytes=billable_bytes,
        raw=raw if billable_bytes else None,
        raw_destination=raw,
        provider_warnings=provider_warnings,
    )
    if sidecar != expected or (billable_bytes == 0 and raw.exists()):
        raise IntegrityError("staged L0 request differs from the current fixed ledger")
    return sidecar


def _register_bytes(path: Path, *, total: dict[str, int], lock: threading.Lock) -> None:
    if not path.is_file():
        return
    with lock:
        total["bytes"] += path.stat().st_size
        if total["bytes"] > MAXIMUM_STAGED_BYTES:
            raise UnauthorizedOperation("L0 staged-byte ceiling exceeded")


def _download_worker(
    *, index: int, work: queue.Queue[L0Request], root: Path, attempt: Path,
    plan: Mapping[str, object], preflight: Preflight,
    provider_factory: Callable[[], ProviderApis], stop: threading.Event,
    one_second_slots: threading.BoundedSemaphore, total: dict[str, int],
    total_lock: threading.Lock, call_state: dict[str, int], call_lock: threading.Lock,
    warnings_collector: ProviderWarningCollector, sleeper: Callable[[float], None],
    deadline: float, clock: Callable[[], float],
) -> WorkerResult:
    completed: list[str] = []
    failed_request_id: str | None = None
    attempts = 0
    try:
        _remaining(deadline, clock)
        apis = provider_factory()
        _remaining(deadline, clock)
        while not stop.is_set():
            try:
                request = work.get_nowait()
            except queue.Empty:
                break
            failed_request_id = request.request_id
            acquired = False
            try:
                if request.family == "ohlcv-1s":
                    one_second_slots.acquire()
                    acquired = True
                for retry in range(MAXIMUM_RETRIES + 1):
                    registered = False
                    raw, sidecar_path = _paths(attempt, request)
                    partial = raw.with_name(f"{raw.name}.p.{uuid.uuid4().hex[:8]}")
                    partial.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        with call_lock:
                            call_state["attempts"] += 1
                            if call_state["attempts"] > MAXIMUM_TIMESERIES_ATTEMPTS:
                                raise UnauthorizedOperation("L0 timeseries-attempt ceiling exceeded")
                        attempts += 1
                        warnings_collector.set_request(request.request_id)
                        try:
                            store = _provider_call(
                                apis.get_range,
                                deadline=deadline,
                                clock=clock,
                                kwargs={
                                    **request.provider_query(include_output_identity=True),
                                    "path": str(partial),
                                },
                            )
                        finally:
                            warnings_collector.clear_request()
                        if not partial.is_file() or partial.stat().st_size <= 0:
                            raise IntegrityError("provider did not create a nonempty DBN file")
                        verify_store_metadata(store=store, request=request)
                        del store  # release Databento's file handle before Windows finalization
                        _register_bytes(partial, total=total, lock=total_lock)
                        registered = True
                        if raw.exists() or sidecar_path.exists():
                            raise IntegrityError("create-only L0 destination appeared during download")
                        sidecar = _sidecar(
                            root=root,
                            plan=plan,
                            request=request,
                            billable_bytes=preflight.billable_bytes[request.request_id],
                            raw=partial,
                            raw_destination=raw,
                            provider_warnings=warnings_collector.for_request(request.request_id),
                        )
                        sidecar_partial = sidecar_path.with_name(
                            f"{sidecar_path.name}.p.{uuid.uuid4().hex[:8]}"
                        )
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
                        if retry >= MAXIMUM_RETRIES or not _transient(exc):
                            raise
                        delay = _retry_after(exc, retry + 1)
                        if delay >= _remaining(deadline, clock):
                            raise UnauthorizedOperation(
                                "L0 runtime cannot accommodate timeseries retry"
                            ) from exc
                        sleeper(delay)
            finally:
                if acquired:
                    one_second_slots.release()
                work.task_done()
    except Exception as exc:
        stop.set()
        return WorkerResult(index, tuple(completed), attempts, type(exc).__name__, failed_request_id)
    return WorkerResult(index, tuple(completed), attempts, None, None)


def download_missing(
    *, root: Path, attempt: Path, plan: Mapping[str, object],
    requests: Sequence[L0Request], preflight: Preflight,
    provider_factory: Callable[[], ProviderApis], warnings_collector: ProviderWarningCollector,
    existing_staged_bytes: int = 0, sleeper: Callable[[float], None] = time.sleep,
    deadline: float | None = None, clock: Callable[[], float] = time.monotonic,
) -> tuple[WorkerResult, ...]:
    deadline = clock() + MAXIMUM_RUNTIME_SECONDS if deadline is None else deadline
    work: queue.Queue[L0Request] = queue.Queue()
    for request in requests:
        work.put(request)
    stop = threading.Event()
    total = {"bytes": existing_staged_bytes}
    total_lock = threading.Lock()
    call_state = {"attempts": 0}
    call_lock = threading.Lock()
    one_second_slots = threading.BoundedSemaphore(MAXIMUM_CONCURRENT_OHLCV_1S)
    with ThreadPoolExecutor(max_workers=MAXIMUM_WORKERS, thread_name_prefix="l0-cutoff") as pool:
        futures = [
            pool.submit(
                _download_worker,
                index=index,
                work=work,
                root=root,
                attempt=attempt,
                plan=plan,
                preflight=preflight,
                provider_factory=provider_factory,
                stop=stop,
                one_second_slots=one_second_slots,
                total=total,
                total_lock=total_lock,
                call_state=call_state,
                call_lock=call_lock,
                warnings_collector=warnings_collector,
                sleeper=sleeper,
                deadline=deadline,
                clock=clock,
            )
            for index in range(MAXIMUM_WORKERS)
        ]
        results = tuple(future.result() for future in futures)
    failures = [result for result in results if result.failure_type is not None]
    if failures:
        error = IntegrityError(f"L0 download worker failed closed: {failures[0].failure_type}")
        error.failed_request_id = failures[0].failed_request_id  # type: ignore[attr-defined]
        raise error
    if not work.empty():
        raise IntegrityError("L0 worker pool stopped before completing its queue")
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
                    raise IntegrityError("L0 staging contains a symbolic link")
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    total += Path(entry.path).stat().st_size
                else:
                    raise IntegrityError("L0 staging contains an unsupported object")
    return total


def _sealed_custody_size(attempt: Path) -> int:
    total = _tree_size(attempt / "raw")
    for name in ("request_plan.json", "preflight.json"):
        path = attempt / name
        if not path.is_file() or path.is_symlink():
            raise IntegrityError("L0 sealed custody metadata is missing or linked")
        total += path.stat().st_size
    return total


def verify_predecessor_custody(
    *, root: Path, plan: Mapping[str, object], ledger: Sequence[L0Request],
) -> str:
    binding = plan["source_binding"]
    if not isinstance(binding, Mapping):
        raise IntegrityError("L0 source binding is invalid")
    inventory_path = root / str(binding["complete_inventory_path"])
    if sha256_file(inventory_path) != binding["complete_inventory_sha256"]:
        raise IntegrityError("complete source inventory hash drifted")
    inventory = _object(inventory_path, "complete source inventory")
    if (
        inventory.get("inventory_id") != binding["complete_inventory_id"]
        or inventory.get("content_inventory_sha256")
        != binding["complete_inventory_content_sha256"]
        or inventory.get("file_count") != binding["complete_inventory_file_count"]
    ):
        raise IntegrityError("complete source inventory identity drifted")
    entries = inventory.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("complete source inventory entries are invalid")
    by_path = {
        str(item.get("path")): item for item in entries if isinstance(item, Mapping)
    }
    verified: list[dict[str, object]] = []
    for request in ledger:
        prior = Path("data/dbn") / request.directory / request.market / "2026" / (
            "2026-01-01_2026-07-14.dbn.zst"
        )
        prior_sidecar = prior.with_name(f"{prior.name}.manifest.json")
        for expected_path, kind in ((prior, "DBN"), (prior_sidecar, "SIDECAR")):
            entry = by_path.get(expected_path.as_posix())
            actual = root / expected_path
            if (
                not isinstance(entry, Mapping)
                or entry.get("kind") != kind
                or entry.get("market") != request.market
                or entry.get("family") != request.directory
                or entry.get("interval_start_inclusive") != "2026-01-01"
                or entry.get("interval_end_exclusive") != "2026-07-14"
                or not actual.is_file()
                or actual.is_symlink()
                or actual.stat().st_size != entry.get("size_bytes")
                or sha256_file(actual) != entry.get("sha256")
            ):
                raise IntegrityError("existing July 14 predecessor custody differs")
            verified.append(
                {
                    "path": expected_path.as_posix(),
                    "sha256": entry["sha256"],
                    "size_bytes": entry["size_bytes"],
                }
            )
        destination = (
            root / "data" / "dbn" / request.directory / request.market / "2026"
            / f"{INTERVAL_NAME}.dbn.zst"
        )
        if destination.exists() or destination.with_name(f"{destination.name}.manifest.json").exists():
            raise IntegrityError("canonical L0 delta destination already exists")
    return sha256_json(verified)


def _required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    binding = plan["source_binding"]
    assert isinstance(binding, Mapping)
    return {
        "plan_id": str(plan["plan_id"]),
        "implementation_sha256": str(plan["implementation_sha256"]),
        "source_contract_sha256": str(binding["source_contract_sha256"]),
        "complete_inventory_id": str(binding["complete_inventory_id"]),
        "provider": "Databento",
        "dataset": DATASET,
        "families": ",".join(FAMILIES),
        "root_count": str(len(ROOTS)),
        "request_count": str(len(ROOTS) * len(FAMILIES)),
        "interval": f"{START_INCLUSIVE}/{END_EXCLUSIVE}",
        "maximum_parallel_downloads": str(MAXIMUM_WORKERS),
        "maximum_concurrent_ohlcv_1s": str(MAXIMUM_CONCURRENT_OHLCV_1S),
        "maximum_external_cost_usd": "0",
        "maximum_billable_bytes": str(MAXIMUM_BILLABLE_BYTES),
        "maximum_staged_bytes": str(MAXIMUM_STAGED_BYTES),
        "staging_only": "true",
        "row_decode": "false",
        "holdout_or_forward_row_read": "false",
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


def _load_preflight(path: Path) -> Preflight:
    value = _object(path, "L0 preflight")
    core = dict(value)
    preflight_id = core.pop("preflight_id", None)
    sizes = core.get("billable_bytes_by_request")
    if (
        preflight_id != sha256_json(core)
        or not isinstance(sizes, dict)
        or any(type(key) is not str for key in sizes)
        or any(type(item) is not int or item < 0 for item in sizes.values())
        or core.get("state") != "PASS_ZERO_COST_CURRENT_ENTITLEMENT"
        or core.get("external_cost_usd") != "0"
        or core.get("total_billable_bytes") != sum(sizes.values())
        or core.get("empty_request_count") != sum(item == 0 for item in sizes.values())
        or core.get("metadata_calls") != MAXIMUM_METADATA_CALLS
        or type(core.get("metadata_attempts")) is not int
        or not MAXIMUM_METADATA_CALLS <= core["metadata_attempts"] <= MAXIMUM_METADATA_ATTEMPTS
        or not 0 < core["total_billable_bytes"] <= MAXIMUM_BILLABLE_BYTES
    ):
        raise IntegrityError("L0 preflight receipt is invalid")
    return Preflight(
        billable_bytes=sizes,
        total_billable_bytes=int(core["total_billable_bytes"]),
        empty_request_count=int(core["empty_request_count"]),
        metadata_calls=int(core["metadata_calls"]),
        metadata_attempts=int(core["metadata_attempts"]),
    )


def _write_failure_receipt(
    *, root: Path, attempt: Path, plan: Mapping[str, object],
    authorization: OperationReceipt, claim: Path, exc: Exception,
) -> None:
    completed = sum(1 for path in (attempt / "raw").glob("**/*.manifest.json") if path.is_file())
    core = {
        "schema_version": "l0_cutoff_alignment_failure/1.0.0",
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
        "holdout_or_forward_rows_read": False,
    }
    payload = {**core, "failure_id": sha256_json(core)}
    _atomic_json(attempt / "failures" / f"{uuid.uuid4().hex[:12]}.json", payload)


def execute_authorized_acquisition(
    *, root: Path, authorization: OperationReceipt,
    provider_factory: Callable[[], ProviderApis], credential_source: str,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
    predecessor_check: Callable[..., str] = verify_predecessor_custody,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    started = clock()
    deadline = started + MAXIMUM_RUNTIME_SECONDS
    root = root.resolve(strict=True)
    plan = load_plan(root=root)
    ledger = build_request_ledger(plan)
    if credential_source != CREDENTIAL_SOURCE:
        raise UnauthorizedOperation("L0 credential source is not bound")
    environment_check(root)
    free = getattr(disk_usage(root), "free", None)
    if type(free) is not int or free < REQUIRED_FREE_BYTES:
        raise UnauthorizedOperation("insufficient free space for L0 acquisition")
    predecessor_set_id = predecessor_check(root=root, plan=plan, ledger=ledger)
    boundary = RepoBoundary(root)
    scope = _required_scope(root=root, plan=plan)
    attempt = root / STAGING_ROOT / authorization.receipt_id
    lock_path = root / LOCK_PATH
    boundary.assert_active_path(
        lock_path.absolute(), purpose="L0 acquisition process lock", subtree="state/locks"
    )
    instance = SingleInstance(lock_path)
    if not instance.acquire():
        raise UnauthorizedOperation("another L0 acquisition or resume is already running")
    try:
        return _execute_locked(
            root=root,
            authorization=authorization,
            provider_factory=provider_factory,
            sleeper=sleeper,
            clock=clock,
            deadline=deadline,
            boundary=boundary,
            plan=plan,
            ledger=ledger,
            attempt=attempt,
            scope=scope,
            predecessor_set_id=predecessor_set_id,
        )
    finally:
        instance.release()


def _execute_locked(
    *, root: Path, authorization: OperationReceipt,
    provider_factory: Callable[[], ProviderApis], sleeper: Callable[[float], None],
    clock: Callable[[], float], deadline: float, boundary: RepoBoundary,
    plan: Mapping[str, object], ledger: Sequence[L0Request], attempt: Path,
    scope: Mapping[str, str], predecessor_set_id: str,
) -> dict[str, object]:
    use_path = root / "state" / "authorization_uses" / f"{authorization.receipt_id}.json"
    claim = (
        authorization.assert_consumed(
            boundary,
            operation=OPERATION,
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            required_scope=scope,
        )
        if use_path.exists()
        else authorization.consume(
            boundary,
            operation=OPERATION,
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            required_scope=scope,
        )
    )
    boundary.assert_active_path(
        attempt.absolute(), purpose="L0 cutoff-alignment attempt", subtree=STAGING_ROOT.as_posix()
    )
    attempt.mkdir(parents=True, exist_ok=True)
    request_plan = {
        "schema_version": "l0_cutoff_alignment_request_ledger/1.0.0",
        "plan_id": plan["plan_id"],
        "predecessor_set_id": predecessor_set_id,
        "requests": [item.as_dict() for item in ledger],
        "request_set_id": sha256_json([item.as_dict() for item in ledger]),
    }
    try:
        _write_or_verify(attempt / "request_plan.json", request_plan, "L0 request ledger")
        apis = provider_factory()
        preflight = run_preflight(
            plan=plan,
            ledger=ledger,
            apis=apis,
            deadline=deadline,
            clock=clock,
            sleeper=sleeper,
        )
        _write_or_verify(attempt / "preflight.json", preflight.as_dict(), "L0 preflight")
        missing_positive: list[L0Request] = []
        for request in ledger:
            billable = preflight.billable_bytes[request.request_id]
            if _verify_sidecar(
                root=root,
                attempt=attempt,
                plan=plan,
                request=request,
                billable_bytes=billable,
            ) is None:
                if billable == 0:
                    raw, sidecar_path = _paths(attempt, request)
                    _atomic_json(
                        sidecar_path,
                        _sidecar(
                            root=root,
                            plan=plan,
                            request=request,
                            billable_bytes=0,
                            raw=None,
                            raw_destination=raw,
                        ),
                    )
                else:
                    missing_positive.append(request)
        existing_size = _tree_size(attempt)
        if existing_size > MAXIMUM_STAGED_BYTES:
            raise UnauthorizedOperation("existing L0 attempt exceeds its staged-byte ceiling")
        if missing_positive:
            with ProviderWarningCollector() as warning_collector:
                download_missing(
                    root=root,
                    attempt=attempt,
                    plan=plan,
                    requests=missing_positive,
                    preflight=preflight,
                    provider_factory=provider_factory,
                    warnings_collector=warning_collector,
                    existing_staged_bytes=existing_size,
                    sleeper=sleeper,
                    deadline=deadline,
                    clock=clock,
                )
        staged = []
        for request in ledger:
            sidecar = _verify_sidecar(
                root=root,
                attempt=attempt,
                plan=plan,
                request=request,
                billable_bytes=preflight.billable_bytes[request.request_id],
            )
            if sidecar is None:
                raise IntegrityError("L0 acquisition is incomplete")
            staged.append(sidecar)
        attempt_size = _tree_size(attempt)
        if attempt_size > MAXIMUM_STAGED_BYTES:
            raise UnauthorizedOperation("completed L0 attempt exceeds its staged-byte ceiling")
        sealed_custody_size = _sealed_custody_size(attempt)
        provider_warnings = [
            {
                "request_id": item["request"]["request_id"],  # type: ignore[index]
                **warning,
            }
            for item in staged
            for warning in item["provider_warnings"]  # type: ignore[index]
        ]
        stored_raw_bytes = sum(int(item["raw_size"] or 0) for item in staged)
        manifest_core = {
            "schema_version": "l0_cutoff_alignment_manifest/1.0.0",
            "state": "SEALED_PROVIDER_CUSTODY_NOT_ROW_QUALITY_CERTIFIED",
            "plan_id": plan["plan_id"],
            "plan_sha256": sha256_file(root / PLAN_PATH),
            "implementation_sha256": plan["implementation_sha256"],
            "source_binding": plan["source_binding"],
            "predecessor_set_id": predecessor_set_id,
            "authorization_receipt_id": authorization.receipt_id,
            "authorization_claim_sha256": sha256_file(claim),
            "request_count": len(ledger),
            "nonempty_request_count": len(ledger) - preflight.empty_request_count,
            "empty_request_count": preflight.empty_request_count,
            "external_cost_usd": "0",
            "fresh_total_billable_bytes": preflight.total_billable_bytes,
            "stored_raw_bytes": stored_raw_bytes,
            "attempt_tree_bytes": sealed_custody_size,
            "staged_files": staged,
            "staged_files_id": sha256_json(staged),
            "provider_warnings": provider_warnings,
            "maximum_parallel_downloads": MAXIMUM_WORKERS,
            "maximum_concurrent_ohlcv_1s": MAXIMUM_CONCURRENT_OHLCV_1S,
            "dbn_rows_decoded": 0,
            "row_quality_certified": False,
            "alpha_source_authorized": False,
            "active_source_contract_changed": False,
            "canonical_promotion_performed": False,
            "holdout_or_forward_rows_read": False,
        }
        manifest = {**manifest_core, "manifest_id": sha256_json(manifest_core)}
        _write_or_verify(attempt / "manifest.json", manifest, "L0 terminal manifest")
        record = {
            "schema_version": "l0_cutoff_alignment_acquisition/1.0.0",
            "state": "PUBLISHED_SEALED_PROVIDER_CUSTODY_ONLY",
            "record_id": manifest["manifest_id"],
            "manifest_path": (attempt / "manifest.json").relative_to(root).as_posix(),
            "manifest_sha256": sha256_file(attempt / "manifest.json"),
            "plan_id": plan["plan_id"],
            "canonical_promotion_performed": False,
            "row_quality_certified": False,
            "holdout_or_forward_rows_read": False,
        }
        event = {
            "schema_version": "l0_cutoff_alignment_event/1.0.0",
            "event_type": "SEALED_CUSTODY_ACQUIRED",
            "record_id": record["record_id"],
            "authorization_receipt_id": authorization.receipt_id,
        }
        record_path = root / RECORD_ROOT / f"{record['record_id']}.json"
        event_path = root / EVENT_ROOT / f"{record['record_id']}.json"
        boundary.assert_active_path(
            record_path.absolute(), purpose="L0 acquisition record", subtree=RECORD_ROOT.as_posix()
        )
        boundary.assert_active_path(
            event_path.absolute(), purpose="L0 acquisition event", subtree=EVENT_ROOT.as_posix()
        )
        _write_or_verify(record_path, record, "L0 acquisition record")
        _write_or_verify(event_path, event, "L0 acquisition event")
        return {
            "record_id": record["record_id"],
            "attempt_path": attempt.relative_to(root).as_posix(),
            "request_count": len(ledger),
            "nonempty_request_count": len(ledger) - preflight.empty_request_count,
            "fresh_total_billable_bytes": preflight.total_billable_bytes,
            "stored_raw_bytes": stored_raw_bytes,
            "provider_warning_count": sum(int(item["count"]) for item in provider_warnings),
            "maximum_parallel_downloads": MAXIMUM_WORKERS,
            "row_quality_certified": False,
            "canonical_promotion_performed": False,
        }
    except Exception as exc:
        _write_failure_receipt(
            root=root,
            attempt=attempt,
            plan=plan,
            authorization=authorization,
            claim=claim,
            exc=exc,
        )
        raise


def verify_completed_acquisition(
    *, root: Path, authorization_receipt_id: str,
    predecessor_check: Callable[..., str] = verify_predecessor_custody,
) -> dict[str, object]:
    root = root.resolve(strict=True)
    plan = load_plan(root=root)
    ledger = build_request_ledger(plan)
    predecessor_set_id = predecessor_check(root=root, plan=plan, ledger=ledger)
    attempt = root / STAGING_ROOT / authorization_receipt_id
    preflight = _load_preflight(attempt / "preflight.json")
    if set(preflight.billable_bytes) != {item.request_id for item in ledger}:
        raise IntegrityError("sealed L0 preflight request set differs")
    if any(
        request.family.startswith("ohlcv-")
        and preflight.billable_bytes[request.request_id] == 0
        for request in ledger
    ):
        raise IntegrityError("sealed L0 preflight has empty OHLCV coverage")
    staged = []
    for request in ledger:
        sidecar = _verify_sidecar(
            root=root,
            attempt=attempt,
            plan=plan,
            request=request,
            billable_bytes=preflight.billable_bytes[request.request_id],
        )
        if sidecar is None:
            raise IntegrityError("sealed L0 acquisition is incomplete")
        staged.append(sidecar)
    manifest = _object(attempt / "manifest.json", "L0 terminal manifest")
    core = dict(manifest)
    manifest_id = core.pop("manifest_id", None)
    provider_warnings = [
        {
            "request_id": item["request"]["request_id"],  # type: ignore[index]
            **warning,
        }
        for item in staged
        for warning in item["provider_warnings"]  # type: ignore[index]
    ]
    stored_raw_bytes = sum(int(item["raw_size"] or 0) for item in staged)
    claim_path = root / "state" / "authorization_uses" / f"{authorization_receipt_id}.json"
    if (
        manifest_id != sha256_json(core)
        or manifest.get("state") != "SEALED_PROVIDER_CUSTODY_NOT_ROW_QUALITY_CERTIFIED"
        or manifest.get("plan_id") != plan["plan_id"]
        or manifest.get("plan_sha256") != sha256_file(root / PLAN_PATH)
        or manifest.get("implementation_sha256") != plan["implementation_sha256"]
        or manifest.get("source_binding") != plan["source_binding"]
        or manifest.get("predecessor_set_id") != predecessor_set_id
        or manifest.get("authorization_receipt_id") != authorization_receipt_id
        or not claim_path.is_file()
        or claim_path.is_symlink()
        or manifest.get("authorization_claim_sha256") != sha256_file(claim_path)
        or manifest.get("request_count") != len(ledger)
        or manifest.get("nonempty_request_count")
        != len(ledger) - preflight.empty_request_count
        or manifest.get("empty_request_count") != preflight.empty_request_count
        or manifest.get("external_cost_usd") != "0"
        or manifest.get("fresh_total_billable_bytes") != preflight.total_billable_bytes
        or manifest.get("stored_raw_bytes") != stored_raw_bytes
        or manifest.get("attempt_tree_bytes") != _sealed_custody_size(attempt)
        or manifest.get("staged_files") != staged
        or manifest.get("staged_files_id") != sha256_json(staged)
        or manifest.get("provider_warnings") != provider_warnings
        or manifest.get("maximum_parallel_downloads") != MAXIMUM_WORKERS
        or manifest.get("maximum_concurrent_ohlcv_1s") != MAXIMUM_CONCURRENT_OHLCV_1S
        or manifest.get("dbn_rows_decoded") != 0
        or manifest.get("canonical_promotion_performed") is not False
        or manifest.get("active_source_contract_changed") is not False
        or manifest.get("alpha_source_authorized") is not False
        or manifest.get("row_quality_certified") is not False
        or manifest.get("holdout_or_forward_rows_read") is not False
    ):
        raise IntegrityError("sealed L0 terminal manifest differs")
    record = {
        "schema_version": "l0_cutoff_alignment_acquisition/1.0.0",
        "state": "PUBLISHED_SEALED_PROVIDER_CUSTODY_ONLY",
        "record_id": manifest_id,
        "manifest_path": (attempt / "manifest.json").relative_to(root).as_posix(),
        "manifest_sha256": sha256_file(attempt / "manifest.json"),
        "plan_id": plan["plan_id"],
        "canonical_promotion_performed": False,
        "row_quality_certified": False,
        "holdout_or_forward_rows_read": False,
    }
    event = {
        "schema_version": "l0_cutoff_alignment_event/1.0.0",
        "event_type": "SEALED_CUSTODY_ACQUIRED",
        "record_id": manifest_id,
        "authorization_receipt_id": authorization_receipt_id,
    }
    if (
        _object(root / RECORD_ROOT / f"{manifest_id}.json", "L0 acquisition record")
        != record
        or _object(root / EVENT_ROOT / f"{manifest_id}.json", "L0 acquisition event")
        != event
    ):
        raise IntegrityError("sealed L0 acquisition record or event differs")
    return {
        "manifest_id": manifest_id,
        "request_count": len(ledger),
        "nonempty_request_count": manifest["nonempty_request_count"],
        "empty_request_count": manifest["empty_request_count"],
        "stored_raw_bytes": manifest["stored_raw_bytes"],
        "provider_warning_count": sum(
            int(item["count"]) for item in manifest["provider_warnings"]  # type: ignore[index]
        ),
        "row_quality_certified": False,
        "canonical_promotion_performed": False,
    }


def execute_receipt_file_acquisition(
    *, root: Path, authorization_path: Path,
    provider_factory: Callable[[], ProviderApis] | None = None,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
    predecessor_check: Callable[..., str] = verify_predecessor_custody,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    root = root.resolve(strict=True)
    receipt_path = authorization_path if authorization_path.is_absolute() else root / authorization_path
    receipt = OperationReceipt.load_external(receipt_path.resolve(strict=True), RepoBoundary(root))
    factory = provider_factory or (lambda: build_file_provider_apis(root=root))
    return execute_authorized_acquisition(
        root=root,
        authorization=receipt,
        provider_factory=factory,
        credential_source=CREDENTIAL_SOURCE,
        disk_usage=disk_usage,
        environment_check=environment_check,
        predecessor_check=predecessor_check,
        sleeper=sleeper,
        clock=clock,
    )


__all__ = [
    "CREDENTIAL_SOURCE",
    "FAMILIES",
    "L0Request",
    "MAXIMUM_WORKERS",
    "MICRO_ROOTS",
    "OPERATION",
    "PLAN_PATH",
    "Preflight",
    "ProviderApis",
    "ROOTS",
    "STAGING_ROOT",
    "STANDARD_ROOTS",
    "build_file_provider_apis",
    "build_request_ledger",
    "download_missing",
    "execute_authorized_acquisition",
    "execute_receipt_file_acquisition",
    "load_plan",
    "required_scope",
    "run_preflight",
    "verify_completed_acquisition",
    "verify_predecessor_custody",
    "verify_store_metadata",
]
