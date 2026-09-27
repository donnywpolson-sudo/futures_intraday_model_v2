"""Resumable zero-dollar TBBO acquisition into sealed vault staging."""

from __future__ import annotations

from .custody_successor import resolve_snapshot, binding_matches

import gc
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
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Final

import databento

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (
    io_path,
    is_linklike,
    sha256_file,
    sha256_json,
    write_new_canonical_authority,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .live_cockpit.databento_auth import resolve_databento_api_key
from .live_cockpit.single_instance import SingleInstance
from .runtime_environment import require_locked_repository_environment
from .source_symbology import build_query_contract, require_query_contract


PLAN_PATH: Final = Path("configs/tbbo_16_market_acquisition_plan_v1.json")
OPERATION: Final = "ACQUIRE_TBBO_16_MARKET_RECENT_YEAR_V1_ONCE"
PUBLICATION_OPERATION: Final = "PUBLISH_TBBO_16_MARKET_SEALED_CUSTODY_V1"
STAGING_ROOT: Final = Path("state/provider_acquisition_staging/tbbo_16_market_v1")
RECORD_ROOT: Final = Path("state/provider_acquisitions/tbbo_16_market_v1")
EVENT_ROOT: Final = Path("state/provider_acquisition_events/tbbo_16_market_v1")
LOCK_PATH: Final = Path("state/locks/tbbo_16_market_acquisition_v1.lock")
CREDENTIAL_SOURCE: Final = "file api.env"
DATASET: Final = "GLBX.MDP3"
SCHEMA: Final = "tbbo"
STYPE_IN: Final = "continuous"
STYPE_OUT: Final = "instrument_id"
MARKETS: Final = (
    "ES", "NQ", "CL", "GC", "NG", "RB", "HG", "SR3", "ZN", "ZB",
    "6E", "6J", "ZC", "ZS", "LE", "HE",
)
START: Final = "2025-09-01T00:00:00Z"
END: Final = "2026-09-01T00:00:00Z"
MAXIMUM_WORKERS: Final = 4
THROTTLED_WORKERS: Final = 2
MAXIMUM_RETRIES: Final = 3
MAXIMUM_BILLABLE_BYTES: Final = 40_000_000_000
MAXIMUM_STAGED_BYTES: Final = 40_000_000_000
REQUIRED_FREE_BYTES: Final = 45_000_000_000
MAXIMUM_RUNTIME_SECONDS: Final = 21_600
MAXIMUM_METADATA_CALLS: Final = 386
MAXIMUM_METADATA_ATTEMPTS: Final = 1_544
MAXIMUM_TIMESERIES_ATTEMPTS: Final = 768
REQUEST_COUNT: Final = 192
PROTECTED_ACTIVE_PATHS: Final = (
    "data/dbn",
    "configs/active_dbn_congruence_release_v1.json",
    "configs/source_contract.json",
    "data/active/catalog.json",
)


@dataclass(frozen=True)
class ProviderApis:
    get_dataset_range: Callable[..., object]
    resolve: Callable[..., object]
    get_cost: Callable[..., object]
    get_billable_size: Callable[..., object]
    get_range: Callable[..., object]


@dataclass(frozen=True)
class MonthlyRequest:
    market: str
    start: str
    end: str
    start_date: str
    end_date: str
    query_contract: dict[str, object]
    request_id: str

    @property
    def filename(self) -> str:
        return f"{self.start_date}_{self.end_date}.dbn.zst"

    @property
    def snapshot_relative_path(self) -> str:
        return f"dbn/tbbo/{self.market}/{self.start_date[:4]}/{self.filename}"

    def provider_query(self, *, output_identity: bool = False) -> dict[str, object]:
        query: dict[str, object] = {
            "dataset": DATASET,
            "schema": SCHEMA,
            "stype_in": STYPE_IN,
            "symbols": [f"{self.market}.v.0"],
            "start": self.start,
            "end": self.end,
        }
        if output_identity:
            query["stype_out"] = STYPE_OUT
        return query

    def as_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "request_id": self.request_id,
            "snapshot_relative_path": self.snapshot_relative_path,
            "query_contract": self.query_contract,
            "stype_out": STYPE_OUT,
        }


@dataclass(frozen=True)
class Preflight:
    billable_bytes: dict[str, int]
    total_billable_bytes: int
    metadata_calls: int
    metadata_attempts: int

    def as_dict(self) -> dict[str, object]:
        core = {
            "schema_version": "tbbo_16_market_preflight/1.0.0",
            "state": "PASS_ZERO_COST_COMPLETE_ENTITLEMENT",
            "billable_bytes_by_request": self.billable_bytes,
            "total_billable_bytes": self.total_billable_bytes,
            "metadata_calls": self.metadata_calls,
            "metadata_attempts": self.metadata_attempts,
            "external_cost_usd": "0",
        }
        return {**core, "preflight_id": sha256_json(core)}


@dataclass(frozen=True)
class WorkerResult:
    completed_request_ids: tuple[str, ...]
    attempts: int
    failure_type: str | None = None
    failed_request_id: str | None = None


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


def json_object(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(io_path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{label} is invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{label} is not an object")
    return value


def _expected_plan_shape() -> dict[str, object]:
    return {
        "dataset": DATASET,
        "schema": SCHEMA,
        "stype_in": STYPE_IN,
        "stype_out": STYPE_OUT,
        "markets": list(MARKETS),
        "operation": OPERATION,
    }


def load_plan(*, root: Path) -> dict[str, object]:
    plan = json_object(root / PLAN_PATH, "TBBO acquisition plan")
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    limits = plan.get("limits")
    period = plan.get("period")
    publication = plan.get("publication")
    forbidden = plan.get("forbidden")
    expected_limits = {
        "maximum_billable_bytes": MAXIMUM_BILLABLE_BYTES,
        "maximum_host_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
        "maximum_metadata_attempts": MAXIMUM_METADATA_ATTEMPTS,
        "maximum_metadata_calls": MAXIMUM_METADATA_CALLS,
        "maximum_parallel_downloads": MAXIMUM_WORKERS,
        "maximum_retries_per_request": MAXIMUM_RETRIES,
        "maximum_staged_bytes": MAXIMUM_STAGED_BYTES,
        "maximum_timeseries_attempts": MAXIMUM_TIMESERIES_ATTEMPTS,
        "minimum_nonempty_requests": REQUEST_COUNT,
        "required_free_disk_bytes": REQUIRED_FREE_BYTES,
        "throttled_parallel_downloads": THROTTLED_WORKERS,
    }
    expected_period = {
        "end_exclusive": END,
        "partition": "calendar_month",
        "request_count": REQUEST_COUNT,
        "start_inclusive": START,
    }
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version") != "tbbo_16_market_acquisition_plan/1.0.0"
        or plan.get("state") != "PREPARED_REQUIRES_BOUNDED_PROVIDER_ACQUISITION"
        or any(plan.get(key) != value for key, value in _expected_plan_shape().items())
        or plan.get("credential_source") != CREDENTIAL_SOURCE
        or plan.get("expected_billable_bytes_informational") != 28_934_630_640
        or limits != expected_limits
        or period != expected_period
        or not isinstance(publication, Mapping)
        or publication.get("operation") != PUBLICATION_OPERATION
        or publication.get("vault_root") != "data/vault/source_snapshots"
        or publication.get("registry_root") != "state/tbbo_custody_registry"
        or not isinstance(forbidden, Mapping)
        or set(forbidden.values()) != {True}
        or not binding_matches(root, Path(__file__), plan.get("implementation_sha256"))
        or plan.get("source_symbology_sha256")
        != sha256_file(root / "src/futures_rebuild/source_symbology.py")
    ):
        raise IntegrityError("TBBO acquisition plan drifted")
    if len(build_request_ledger(plan)) != REQUEST_COUNT:
        raise IntegrityError("TBBO request count drifted")
    return plan


def _utc(value: date) -> str:
    return f"{value.isoformat()}T00:00:00Z"


def _next_month(value: date) -> date:
    return date(value.year + int(value.month == 12), 1 if value.month == 12 else value.month + 1, 1)


def build_request_ledger(plan: Mapping[str, object]) -> tuple[MonthlyRequest, ...]:
    period = plan.get("period")
    if not isinstance(period, Mapping):
        raise IntegrityError("TBBO period is invalid")
    try:
        first = date.fromisoformat(str(period["start_inclusive"])[:10])
        end = date.fromisoformat(str(period["end_exclusive"])[:10])
    except (KeyError, ValueError) as exc:
        raise IntegrityError("TBBO period bounds are invalid") from exc
    if first.day != 1 or end.day != 1 or first >= end:
        raise IntegrityError("TBBO monthly bounds are invalid")
    requests: list[MonthlyRequest] = []
    for market in MARKETS:
        current = first
        while current < end:
            following = _next_month(current)
            contract = build_query_contract(
                schema=SCHEMA,
                market=market,
                start=_utc(current),
                end=_utc(following),
                stype_in=STYPE_IN,
                symbols=[f"{market}.v.0"],
            )
            core = {"query_contract": contract, "stype_out": STYPE_OUT}
            requests.append(MonthlyRequest(
                market=market,
                start=_utc(current),
                end=_utc(following),
                start_date=current.isoformat(),
                end_date=following.isoformat(),
                query_contract=contract,
                request_id=sha256_json(core),
            ))
            current = following
    return tuple(requests)


def _provider_time(value: object, label: str) -> datetime:
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
    selected = ranges.get(SCHEMA) if isinstance(ranges, Mapping) else value
    if not isinstance(selected, Mapping):
        raise UnauthorizedOperation("provider TBBO entitlement range is absent")
    if (
        _provider_time(selected.get("start"), "range start") > _provider_time(START, "request start")
        or _provider_time(selected.get("end"), "range end") < _provider_time(END, "request end")
    ):
        raise UnauthorizedOperation("current TBBO entitlement does not cover the fixed ledger")


def _validate_symbology(value: object) -> None:
    if not isinstance(value, Mapping):
        raise IntegrityError("provider symbology response is invalid")
    if value.get("partial") not in (None, [], (), False, 0) or value.get("not_found") not in (None, [], ()):
        raise UnauthorizedOperation("continuous symbology is partial or unresolved")
    symbols = [f"{market}.v.0" for market in MARKETS]
    result = value.get("result")
    if (
        value.get("stype_in") != STYPE_IN
        or value.get("stype_out") != STYPE_OUT
        or value.get("start_date") != START[:10]
        or value.get("end_date") != END[:10]
        or not isinstance(result, Mapping)
        or set(result) != set(symbols)
    ):
        raise UnauthorizedOperation("continuous symbology does not resolve the exact fixed scope")
    requested_start = date.fromisoformat(START[:10])
    requested_end = date.fromisoformat(END[:10])
    for symbol in symbols:
        raw_intervals = result[symbol]
        if not isinstance(raw_intervals, list) or not raw_intervals:
            raise UnauthorizedOperation("continuous symbology has an empty mapping")
        intervals: list[tuple[date, date]] = []
        for raw in raw_intervals:
            if not isinstance(raw, Mapping) or not {"d0", "d1", "s"} <= set(raw):
                raise IntegrityError("continuous symbology interval is malformed")
            left = date.fromisoformat(str(raw["d0"]))
            right = date.fromisoformat(str(raw["d1"]))
            if left >= right or not str(raw["s"]):
                raise IntegrityError("continuous symbology interval is invalid")
            if right > requested_start and left < requested_end:
                intervals.append((max(left, requested_start), min(right, requested_end)))
        cursor = requested_start
        for left, right in sorted(intervals):
            if left > cursor:
                raise UnauthorizedOperation("continuous symbology contains a coverage gap")
            cursor = max(cursor, right)
        if cursor < requested_end:
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
        raise UnauthorizedOperation("TBBO acquisition runtime exhausted")
    return remaining


def _status_code(exc: BaseException) -> int | None:
    direct = getattr(exc, "status_code", None)
    response = getattr(exc, "response", None)
    nested = getattr(response, "status_code", None)
    return direct if type(direct) is int else nested if type(nested) is int else None


def _transient(exc: BaseException) -> bool:
    status = _status_code(exc)
    return status == 429 or (status is not None and 500 <= status <= 599) or type(exc).__name__ in {
        "ConnectionError", "ConnectTimeout", "ReadTimeout", "Timeout", "TimeoutError", "ProtocolError",
    }


def _retry_after(exc: BaseException, retry: int) -> float:
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
    return float(2 ** (retry - 1))


def _provider_call(
    method: Callable[..., object], *, deadline: float, clock: Callable[[], float],
    kwargs: Mapping[str, object],
) -> object:
    remaining = _remaining(deadline, clock)
    owner = getattr(method, "__self__", None)
    if owner is not None and hasattr(owner, "TIMEOUT"):
        owner.TIMEOUT = min(100.0, remaining)
    value = method(**kwargs)
    _remaining(deadline, clock)
    return value


def _metadata_call(
    method: Callable[..., object], *, attempts: dict[str, int], deadline: float,
    clock: Callable[[], float], sleeper: Callable[[float], None], kwargs: Mapping[str, object],
) -> object:
    for retry in range(MAXIMUM_RETRIES + 1):
        attempts["count"] += 1
        if attempts["count"] > MAXIMUM_METADATA_ATTEMPTS:
            raise UnauthorizedOperation("TBBO metadata-attempt ceiling exceeded")
        try:
            return _provider_call(method, deadline=deadline, clock=clock, kwargs=kwargs)
        except Exception as exc:
            if retry >= MAXIMUM_RETRIES or not _transient(exc):
                raise
            delay = _retry_after(exc, retry + 1)
            if delay >= _remaining(deadline, clock):
                raise UnauthorizedOperation("TBBO runtime cannot accommodate metadata retry") from exc
            sleeper(delay)
    raise AssertionError("unreachable")


def run_preflight(
    *, plan: Mapping[str, object], ledger: Sequence[MonthlyRequest], apis: ProviderApis,
    deadline: float | None = None, clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> Preflight:
    del plan
    deadline = clock() + MAXIMUM_RUNTIME_SECONDS if deadline is None else deadline
    attempts = {"count": 0}
    _validate_entitlement(_metadata_call(
        apis.get_dataset_range, attempts=attempts, deadline=deadline, clock=clock,
        sleeper=sleeper, kwargs={"dataset": DATASET},
    ))
    _validate_symbology(_metadata_call(
        apis.resolve, attempts=attempts, deadline=deadline, clock=clock, sleeper=sleeper,
        kwargs={
            "dataset": DATASET,
            "symbols": [f"{market}.v.0" for market in MARKETS],
            "stype_in": STYPE_IN,
            "stype_out": STYPE_OUT,
            "start_date": START[:10],
            "end_date": END[:10],
        },
    ))
    sizes: dict[str, int] = {}
    for request in ledger:
        query = request.provider_query()
        _zero_cost(_metadata_call(
            apis.get_cost, attempts=attempts, deadline=deadline, clock=clock,
            sleeper=sleeper, kwargs=query,
        ))
        size = _metadata_call(
            apis.get_billable_size, attempts=attempts, deadline=deadline, clock=clock,
            sleeper=sleeper, kwargs=query,
        )
        if type(size) is not int or size <= 0:
            raise UnauthorizedOperation("every TBBO request must have positive billable size")
        sizes[request.request_id] = size
    total = sum(sizes.values())
    calls = 2 + 2 * len(ledger)
    if calls != MAXIMUM_METADATA_CALLS or len(sizes) != REQUEST_COUNT or total > MAXIMUM_BILLABLE_BYTES:
        raise UnauthorizedOperation("fresh TBBO metadata scope or 40 GB billable ceiling failed")
    return Preflight(sizes, total, calls, attempts["count"])


def _metadata_dict(metadata: object) -> dict[str, object]:
    return {
        "dataset": getattr(metadata, "dataset", None),
        "schema": str(getattr(metadata, "schema", "")),
        "stype_in": str(getattr(metadata, "stype_in", "")),
        "stype_out": str(getattr(metadata, "stype_out", "")),
        "start": getattr(metadata, "start", None),
        "end": getattr(metadata, "end", None),
        "symbols": [str(item) for item in getattr(metadata, "symbols", ())],
        "ts_out": getattr(metadata, "ts_out", None),
        "limit": getattr(metadata, "limit", None),
    }


def _iso_ns(value: str) -> int:
    return int(_provider_time(value, "DBN timestamp").timestamp()) * 1_000_000_000


def validate_metadata(metadata: object, request: MonthlyRequest) -> dict[str, object]:
    observed = _metadata_dict(metadata)
    expected = {
        "dataset": DATASET,
        "schema": SCHEMA,
        "stype_in": STYPE_IN,
        "stype_out": STYPE_OUT,
        "start": _iso_ns(request.start),
        "end": _iso_ns(request.end),
        "symbols": [f"{request.market}.v.0"],
        "ts_out": False,
        "limit": None,
    }
    if observed != expected:
        raise IntegrityError("downloaded DBN header metadata differs from its request")
    return observed


def load_header(path: Path) -> object:
    store = databento.DBNStore.from_file(io_path(path))
    try:
        return store.metadata
    finally:
        del store
        gc.collect()


def request_paths(snapshot: Path, request: MonthlyRequest) -> tuple[Path, Path]:
    dbn = snapshot / request.snapshot_relative_path
    return dbn, Path(f"{dbn}.manifest.json")


def atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    write_new_canonical_authority(path, dict(payload))


def write_or_verify(path: Path, payload: Mapping[str, object], label: str) -> None:
    try:
        if io_path(path).exists() and is_linklike(path):
            raise IntegrityError(f"existing {label} is link-like")
        atomic_json(path, payload)
    except IntegrityError as exc:
        raise IntegrityError(f"existing {label} differs") from exc


def sidecar_for(
    *, plan: Mapping[str, object], request: MonthlyRequest, dbn_path: Path,
    billable_bytes: int, metadata: object,
) -> dict[str, object]:
    header = validate_metadata(metadata, request)
    core = {
        "schema_version": "tbbo_sealed_custody_sidecar/1.0.0",
        "state": "SEALED_PROVIDER_CUSTODY_NOT_ROW_QUALITY_CERTIFIED",
        "plan_id": plan["plan_id"],
        "request": request.as_dict(),
        "billable_bytes_at_download": billable_bytes,
        "snapshot_relative_path": request.snapshot_relative_path,
        "file_size_bytes": io_path(dbn_path).stat().st_size,
        "file_sha256": sha256_file(dbn_path),
        "dbn_header": header,
        "dbn_rows_decoded": 0,
        "row_quality_certified": False,
        "research_source_authorized": False,
    }
    return {**core, "manifest_id": sha256_json(core)}


def verify_sidecar(
    *, plan: Mapping[str, object], snapshot: Path, request: MonthlyRequest,
    metadata_loader: Callable[[Path], object] = load_header,
) -> dict[str, object] | None:
    dbn, sidecar_path = request_paths(snapshot, request)
    if not io_path(dbn).exists() and not io_path(sidecar_path).exists():
        return None
    if (
        is_linklike(dbn) or not io_path(dbn).is_file()
        or is_linklike(sidecar_path) or not io_path(sidecar_path).is_file()
    ):
        raise IntegrityError("staged TBBO request is incomplete or link-like")
    sidecar = json_object(sidecar_path, "TBBO sidecar")
    core = dict(sidecar)
    manifest_id = core.pop("manifest_id", None)
    if manifest_id != sha256_json(core):
        raise IntegrityError("TBBO sidecar identity is invalid")
    if (
        sidecar.get("plan_id") != plan["plan_id"]
        or sidecar.get("request") != request.as_dict()
        or sidecar.get("snapshot_relative_path") != request.snapshot_relative_path
        or sidecar.get("file_size_bytes") != io_path(dbn).stat().st_size
        or sidecar.get("file_sha256") != sha256_file(dbn)
        or sidecar.get("dbn_rows_decoded") != 0
        or sidecar.get("row_quality_certified") is not False
        or sidecar.get("research_source_authorized") is not False
        or sidecar.get("dbn_header") != validate_metadata(metadata_loader(dbn), request)
    ):
        raise IntegrityError("staged TBBO request differs from its fixed contract")
    return sidecar


def plain_tree_files(root: Path, *, reject_hardlinks: bool) -> tuple[Path, ...]:
    if not io_path(root).is_dir() or is_linklike(root):
        raise IntegrityError(f"expected a plain directory: {root}")
    files: list[Path] = []
    pending = [Path()]
    while pending:
        relative = pending.pop()
        directory = root / relative
        try:
            entries = sorted(os.scandir(io_path(directory)), key=lambda item: item.name)
        except OSError as exc:
            raise IntegrityError(f"directory inventory is unreadable: {directory}") from exc
        for entry in entries:
            child_relative = relative / entry.name
            child = root / child_relative
            if is_linklike(child):
                raise IntegrityError(f"tree contains a link: {child}")
            if entry.is_dir(follow_symlinks=False):
                pending.append(child_relative)
            elif entry.is_file(follow_symlinks=False):
                if reject_hardlinks and io_path(child).stat().st_nlink != 1:
                    from .custody_successor import registered_hardlink
                    if not registered_hardlink(child):
                        raise IntegrityError(f"tree contains a hard link: {child}")
                files.append(child)
            else:
                raise IntegrityError(f"tree contains an unsupported entry: {child}")
    return tuple(sorted(files, key=lambda item: item.relative_to(root).as_posix()))


def tree_size(path: Path) -> int:
    if not io_path(path).exists():
        return 0
    return sum(
        io_path(child).stat().st_size
        for child in plain_tree_files(path, reject_hardlinks=False)
    )


def protected_active_state(root: Path) -> dict[str, object]:
    """Hash active source bytes without opening or decoding DBN records."""

    bindings: list[dict[str, object]] = []
    for relative in PROTECTED_ACTIVE_PATHS:
        path = root / relative
        if io_path(path).is_file():
            bindings.append({
                "path": relative,
                "kind": "file",
                "file_count": 1,
                "total_bytes": io_path(path).stat().st_size,
                "sha256": sha256_file(path, reject_hardlinks=False),
            })
            continue
        if not io_path(path).is_dir() or is_linklike(path):
            raise IntegrityError(f"protected active source is invalid: {relative}")
        files: list[dict[str, object]] = []
        for child in plain_tree_files(path, reject_hardlinks=False):
            files.append({
                "path": child.relative_to(path).as_posix(),
                "size": io_path(child).stat().st_size,
                "sha256": sha256_file(child, reject_hardlinks=False),
            })
        bindings.append({
            "path": relative,
            "kind": "tree",
            "file_count": len(files),
            "total_bytes": sum(int(item["size"]) for item in files),
            "sha256": sha256_json(files),
        })
    core = {"paths": bindings, "dbn_rows_decoded": 0}
    return {**core, "state_id": sha256_json(core)}


def _download_worker(
    *, index: int, work: queue.Queue[MonthlyRequest], snapshot: Path,
    plan: Mapping[str, object], preflight: Preflight,
    provider_factory: Callable[[], ProviderApis], stop: threading.Event,
    throttled: threading.Event, call_state: dict[str, int], call_lock: threading.Lock,
    staged_state: dict[str, int], staged_lock: threading.Lock,
    sleeper: Callable[[float], None], deadline: float, clock: Callable[[], float],
) -> WorkerResult:
    completed: list[str] = []
    attempts = 0
    failed_request: str | None = None
    try:
        apis = provider_factory()
        while not stop.is_set():
            _remaining(deadline, clock)
            if throttled.is_set() and index >= THROTTLED_WORKERS:
                break
            try:
                request = work.get_nowait()
            except queue.Empty:
                break
            failed_request = request.request_id
            final, sidecar_path = request_paths(snapshot, request)
            io_path(final.parent).mkdir(parents=True, exist_ok=True)
            for retry in range(MAXIMUM_RETRIES + 1):
                partial = final.with_name(f"{final.name}.partial.{uuid.uuid4().hex[:16]}")
                registered = False
                try:
                    with call_lock:
                        call_state["attempts"] += 1
                        if call_state["attempts"] > MAXIMUM_TIMESERIES_ATTEMPTS:
                            raise UnauthorizedOperation("TBBO timeseries-attempt ceiling exceeded")
                    attempts += 1
                    store = _provider_call(
                        apis.get_range, deadline=deadline, clock=clock,
                        kwargs={
                            **request.provider_query(output_identity=True),
                            "path": str(io_path(partial)),
                        },
                    )
                    if not io_path(partial).is_file() or io_path(partial).stat().st_size <= 0:
                        raise IntegrityError("provider did not create a nonempty DBN file")
                    with staged_lock:
                        staged_state["bytes"] += io_path(partial).stat().st_size
                        registered = True
                        if staged_state["bytes"] > MAXIMUM_STAGED_BYTES:
                            raise UnauthorizedOperation("TBBO 40 GB staging ceiling exceeded")
                    metadata = getattr(store, "metadata", None)
                    validate_metadata(metadata, request)
                    del store
                    gc.collect()
                    if io_path(final).exists() or io_path(sidecar_path).exists():
                        raise IntegrityError("create-only TBBO destination appeared during download")
                    sidecar = sidecar_for(
                        plan=plan, request=request, dbn_path=partial,
                        billable_bytes=preflight.billable_bytes[request.request_id], metadata=metadata,
                    )
                    os.replace(io_path(partial), io_path(final))
                    try:
                        atomic_json(sidecar_path, sidecar)
                    except Exception:
                        os.replace(io_path(final), io_path(partial))
                        raise
                    completed.append(request.request_id)
                    failed_request = None
                    break
                except Exception as exc:
                    if io_path(partial).exists() and not registered:
                        with staged_lock:
                            staged_state["bytes"] += io_path(partial).stat().st_size
                            registered = True
                            if staged_state["bytes"] > MAXIMUM_STAGED_BYTES:
                                raise UnauthorizedOperation(
                                    "TBBO 40 GB staging ceiling exceeded"
                                ) from exc
                    if _status_code(exc) == 429:
                        throttled.set()
                    if retry >= MAXIMUM_RETRIES or not _transient(exc):
                        raise
                    delay = _retry_after(exc, retry + 1)
                    if delay >= _remaining(deadline, clock):
                        raise UnauthorizedOperation("TBBO runtime cannot accommodate retry") from exc
                    sleeper(delay)
            work.task_done()
    except Exception as exc:
        stop.set()
        return WorkerResult(tuple(completed), attempts, type(exc).__name__, failed_request)
    return WorkerResult(tuple(completed), attempts)


def download_missing(
    *, attempt: Path, snapshot: Path, plan: Mapping[str, object],
    requests: Sequence[MonthlyRequest], preflight: Preflight,
    provider_factory: Callable[[], ProviderApis], deadline: float,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[WorkerResult, ...]:
    work: queue.Queue[MonthlyRequest] = queue.Queue()
    for request in requests:
        dbn, _ = request_paths(snapshot, request)
        io_path(dbn.parent).mkdir(parents=True, exist_ok=True)
        work.put(request)
    stop = threading.Event()
    throttled = threading.Event()
    call_state = {"attempts": 0}
    call_lock = threading.Lock()
    staged_state = {"bytes": tree_size(attempt)}
    staged_lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=MAXIMUM_WORKERS) as pool:
        futures = [
            pool.submit(
                _download_worker, index=index, work=work, snapshot=snapshot, plan=plan,
                preflight=preflight, provider_factory=provider_factory, stop=stop,
                throttled=throttled, call_state=call_state, call_lock=call_lock,
                staged_state=staged_state, staged_lock=staged_lock,
                sleeper=sleeper, deadline=deadline, clock=clock,
            )
            for index in range(MAXIMUM_WORKERS)
        ]
        results = tuple(future.result() for future in futures)
    failures = [result for result in results if result.failure_type]
    if failures:
        failure = failures[0]
        raise IntegrityError(
            f"TBBO acquisition failed closed at {failure.failed_request_id}: {failure.failure_type}"
        )
    if tree_size(attempt) > MAXIMUM_STAGED_BYTES:
        raise UnauthorizedOperation("TBBO 40 GB staging ceiling exceeded")
    return results


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "dataset": DATASET,
        "schema": SCHEMA,
        "symbols": ",".join(f"{market}.v.0" for market in MARKETS),
        "period": f"{START}/{END}",
        "request_count": str(REQUEST_COUNT),
        "maximum_parallel_downloads": str(MAXIMUM_WORKERS),
        "maximum_external_cost_usd": "0",
        "maximum_billable_bytes": str(MAXIMUM_BILLABLE_BYTES),
        "maximum_staged_bytes": str(MAXIMUM_STAGED_BYTES),
        "staging_root": STAGING_ROOT.as_posix(),
        "row_decode": "false",
        "publication": "false",
        "credential_source": CREDENTIAL_SOURCE,
        "implementation_sha256": str(plan["implementation_sha256"]),
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def execute_authorized_acquisition(
    *, root: Path, authorization: OperationReceipt,
    provider_factory: Callable[[], ProviderApis] | None = None,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
    metadata_loader: Callable[[Path], object] = load_header,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    root = root.resolve(strict=True)
    plan = load_plan(root=root)
    ledger = build_request_ledger(plan)
    environment_check(root)
    free = getattr(disk_usage(root), "free", None)
    if type(free) is not int or free < REQUIRED_FREE_BYTES:
        raise UnauthorizedOperation("insufficient free space for TBBO acquisition")
    boundary = RepoBoundary(root)
    scope = required_scope(root=root, plan=plan)
    attempt = root / STAGING_ROOT / authorization.receipt_id
    snapshot = attempt / "snapshot"
    boundary.assert_active_path(attempt.absolute(), purpose="TBBO acquisition staging", subtree=STAGING_ROOT.as_posix())
    instance = SingleInstance(root / LOCK_PATH)
    if not instance.acquire():
        raise UnauthorizedOperation("another TBBO acquisition is running")
    started = clock()
    deadline = started + MAXIMUM_RUNTIME_SECONDS
    try:
        use_path = root / "state/authorization_uses" / f"{authorization.receipt_id}.json"
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
        attempt.mkdir(parents=True, exist_ok=True)
        active_baseline = protected_active_state(root)
        request_plan = {
            "schema_version": "tbbo_16_market_request_ledger/1.0.0",
            "plan_id": plan["plan_id"],
            "requests": [request.as_dict() for request in ledger],
            "request_set_id": sha256_json([request.as_dict() for request in ledger]),
            "protected_active_baseline": active_baseline,
        }
        write_or_verify(attempt / "request_plan.json", request_plan, "TBBO request ledger")
        existing_manifest_path = attempt / "manifest.json"
        if io_path(existing_manifest_path).exists():
            existing = json_object(existing_manifest_path, "TBBO acquisition manifest")
            existing_core = dict(existing)
            existing_id = existing_core.pop("manifest_id", None)
            if (
                existing_id != sha256_json(existing_core)
                or existing.get("plan_id") != plan["plan_id"]
                or existing.get("authorization_receipt_id") != authorization.receipt_id
                or existing.get("protected_active_baseline") != active_baseline
                or existing.get("request_count") != REQUEST_COUNT
                or existing.get("dbn_rows_decoded") != 0
            ):
                raise IntegrityError("completed TBBO acquisition manifest differs")
            if any(
                verify_sidecar(
                    plan=plan, snapshot=snapshot, request=request,
                    metadata_loader=metadata_loader,
                ) is None
                for request in ledger
            ):
                raise IntegrityError("completed TBBO acquisition is missing a request")
            record = json_object(
                root / RECORD_ROOT / f"{existing_id}.json", "TBBO acquisition record",
            )
            if (
                record.get("record_id") != existing_id
                or record.get("manifest_sha256") != sha256_file(existing_manifest_path)
                or record.get("attempt_path") != attempt.relative_to(root).as_posix()
                or type(record.get("staged_bytes")) is not int
            ):
                raise IntegrityError("completed TBBO acquisition record differs")
            return {
                "record_id": existing_id,
                "attempt_path": record["attempt_path"],
                "request_count": REQUEST_COUNT,
                "fresh_total_billable_bytes": existing["fresh_total_billable_bytes"],
                "staged_bytes": record["staged_bytes"],
                "dbn_rows_decoded": 0,
                "canonical_publication_performed": False,
            }
        factory = provider_factory or (lambda: build_file_provider_apis(root=root))
        preflight = run_preflight(
            plan=plan, ledger=ledger, apis=factory(), deadline=deadline,
            clock=clock, sleeper=sleeper,
        )
        preflight_payload = preflight.as_dict()
        preflight_path = attempt / "preflights" / f"{preflight_payload['preflight_id']}.json"
        write_or_verify(preflight_path, preflight_payload, "TBBO preflight")
        missing: list[MonthlyRequest] = []
        for request in ledger:
            if verify_sidecar(
                plan=plan, snapshot=snapshot, request=request,
                metadata_loader=metadata_loader,
            ) is None:
                missing.append(request)
        if tree_size(attempt) > MAXIMUM_STAGED_BYTES:
            raise UnauthorizedOperation("existing TBBO staging exceeds 40 GB")
        if missing:
            remaining_billable = sum(
                preflight.billable_bytes[request.request_id] for request in missing
            )
            if tree_size(attempt) + remaining_billable > MAXIMUM_STAGED_BYTES:
                raise UnauthorizedOperation("projected TBBO staging exceeds 40 GB")
            download_missing(
                attempt=attempt, snapshot=snapshot, plan=plan, requests=missing,
                preflight=preflight, provider_factory=factory, deadline=deadline,
                sleeper=sleeper, clock=clock,
            )
        sidecars = [
            verify_sidecar(
                plan=plan, snapshot=snapshot, request=request,
                metadata_loader=metadata_loader,
            )
            for request in ledger
        ]
        if any(sidecar is None for sidecar in sidecars):
            raise IntegrityError("TBBO acquisition is incomplete")
        staged_bytes = tree_size(attempt)
        if staged_bytes > MAXIMUM_STAGED_BYTES:
            raise UnauthorizedOperation("completed TBBO staging exceeds 40 GB")
        manifest_core = {
            "schema_version": "tbbo_16_market_acquisition_manifest/1.0.0",
            "state": "SEALED_PROVIDER_CUSTODY_NOT_ROW_QUALITY_CERTIFIED",
            "plan_id": plan["plan_id"],
            "plan_sha256": sha256_file(root / PLAN_PATH),
            "implementation_sha256": plan["implementation_sha256"],
            "authorization_receipt_id": authorization.receipt_id,
            "authorization_claim_sha256": sha256_file(claim),
            "preflight_path": preflight_path.relative_to(root).as_posix(),
            "preflight_sha256": sha256_file(preflight_path),
            "request_count": REQUEST_COUNT,
            "fresh_total_billable_bytes": preflight.total_billable_bytes,
            "staged_bytes_before_manifest": staged_bytes,
            "staged_files": sidecars,
            "staged_files_id": sha256_json(sidecars),
            "dbn_rows_decoded": 0,
            "row_quality_certified": False,
            "research_source_authorized": False,
            "canonical_publication_performed": False,
            "protected_active_baseline": active_baseline,
        }
        manifest = {**manifest_core, "manifest_id": sha256_json(manifest_core)}
        write_or_verify(attempt / "manifest.json", manifest, "TBBO acquisition manifest")
        completed_staged_bytes = tree_size(attempt)
        if completed_staged_bytes > MAXIMUM_STAGED_BYTES:
            raise UnauthorizedOperation("completed TBBO attempt exceeds 40 GB")
        record = {
            "schema_version": "tbbo_16_market_acquisition_record/1.0.0",
            "state": "SEALED_PROVIDER_CUSTODY_AWAITING_VAULT_PUBLICATION",
            "record_id": manifest["manifest_id"],
            "attempt_path": attempt.relative_to(root).as_posix(),
            "manifest_path": (attempt / "manifest.json").relative_to(root).as_posix(),
            "manifest_sha256": sha256_file(attempt / "manifest.json"),
            "plan_id": plan["plan_id"],
            "dbn_rows_decoded": 0,
            "staged_bytes": completed_staged_bytes,
        }
        event = {
            "schema_version": "tbbo_16_market_acquisition_event/1.0.0",
            "event_type": "SEALED_TBBO_CUSTODY_ACQUIRED",
            "record_id": record["record_id"],
            "authorization_receipt_id": authorization.receipt_id,
        }
        write_or_verify(root / RECORD_ROOT / f"{record['record_id']}.json", record, "TBBO acquisition record")
        write_or_verify(root / EVENT_ROOT / f"{record['record_id']}.json", event, "TBBO acquisition event")
        return {
            "record_id": record["record_id"],
            "attempt_path": record["attempt_path"],
            "request_count": REQUEST_COUNT,
            "fresh_total_billable_bytes": preflight.total_billable_bytes,
            "staged_bytes": completed_staged_bytes,
            "dbn_rows_decoded": 0,
            "canonical_publication_performed": False,
        }
    except Exception as exc:
        if attempt.exists():
            failure_core = {
                "schema_version": "tbbo_16_market_acquisition_failure/1.0.0",
                "state": "FAILED_CLOSED_RESUMABLE_PARTIALS_PRESERVED",
                "plan_id": plan["plan_id"],
                "authorization_receipt_id": authorization.receipt_id,
                "exception_type": type(exc).__name__,
                "staged_bytes": tree_size(attempt),
                "dbn_rows_decoded": 0,
                "automatic_cleanup_performed": False,
            }
            failure = {**failure_core, "failure_id": sha256_json(failure_core)}
            write_or_verify(
                attempt / "failures" / f"{failure['failure_id']}.json", failure,
                "TBBO acquisition failure",
            )
        raise
    finally:
        instance.release()


__all__ = [
    "MARKETS", "OPERATION", "PLAN_PATH", "PUBLICATION_OPERATION", "ProviderApis",
    "build_file_provider_apis", "build_request_ledger", "execute_authorized_acquisition",
    "load_header", "load_plan", "plain_tree_files", "protected_active_state", "required_scope",
    "run_preflight", "verify_sidecar",
]
