"""Resumable market-partitioned BBO-1s acquisition into sealed staging."""

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
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Final

from . import l1_execution_quarantine_acquisition as predecessor
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, is_linklike, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .research_gateway_policy import require_current_real_history_operation


PLAN_PATH: Final = Path("configs/l1_execution_bbo_1s_market_partition_plan_v1.json")
IMPLEMENTATION_PATH: Final = Path(
    "src/futures_rebuild/l1_execution_bbo_1s_market_partition.py"
)
OPERATION: Final = "ACQUIRE_L1_EXECUTION_BBO_1S_MARKET_PARTITION_V1_ONCE"
STAGING_ROOT: Final = Path(
    "state/provider_acquisition_staging/l1_bbo1s_mp_v1"
)
RECORD_ROOT: Final = Path(
    "state/provider_acquisitions/l1_execution_bbo_1s_market_partition_v1"
)
EVENT_ROOT: Final = Path(
    "state/provider_acquisition_events/l1_execution_bbo_1s_market_partition_v1"
)
LOCK_PATH: Final = Path("state/locks/l1_execution_bbo_1s_market_partition_v1.lock")
CREDENTIAL_SOURCE: Final = predecessor.CREDENTIAL_SOURCE
DATASET: Final = "GLBX.MDP3"
SCHEMA: Final = "bbo-1s"
STYPE_IN: Final = "continuous"
STYPE_OUT: Final = "instrument_id"
SIDECAR_SCHEMA: Final = "l1_execution_bbo_1s_market_partition_sidecar/1.0.0"
MARKET_SYMBOLS: Final = {
    "ES": ("ES.v.0",),
    "NQ": ("NQ.v.0",),
    "CL": ("CL.v.0",),
    "GC": ("GC.v.0",),
    "ZN": ("ZN.v.0",),
    "6E": ("6E.v.0",),
    "MES": ("MES.v.0", "MES.v.1"),
    "MCL": ("MCL.v.0", "MCL.v.1"),
    "NG": ("NG.v.0",),
    "RB": ("RB.v.0",),
    "HG": ("HG.v.0",),
    "SR3": ("SR3.v.0",),
    "ZB": ("ZB.v.0",),
    "6J": ("6J.v.0",),
    "ZC": ("ZC.v.0",),
    "ZS": ("ZS.v.0",),
    "LE": ("LE.v.0",),
    "HE": ("HE.v.0",),
}
SYMBOLS: Final = tuple(symbol for symbols in MARKET_SYMBOLS.values() for symbol in symbols)
DEGRADED_DATES: Final = (
    "2025-09-17",
    "2025-09-24",
    "2025-11-28",
    "2026-03-16",
    "2026-04-10",
    "2026-07-30",
)
PREDECESSOR_EMPTY_DATES: Final = ("2025-12-25", "2026-01-01")
REQUEST_COUNT: Final = 4_698
MINIMUM_NONEMPTY_REQUESTS: Final = 4_500
MINIMUM_NONEMPTY_REQUESTS_PER_MARKET: Final = 250
MINIMUM_BILLABLE_BYTES: Final = 1_400_000_000
MAXIMUM_BILLABLE_BYTES: Final = 10_000_000_000
MAXIMUM_METADATA_CALLS: Final = 9_398
MAXIMUM_METADATA_ATTEMPTS: Final = 37_592
MAXIMUM_TIMESERIES_ATTEMPTS: Final = 18_792
MAXIMUM_RETRIES: Final = 3
MAXIMUM_PREFLIGHT_WORKERS: Final = 6
MAXIMUM_WORKERS: Final = 4
THROTTLED_WORKERS: Final = 2
MAXIMUM_RUNTIME_SECONDS: Final = 21_600
MAXIMUM_STAGED_BYTES: Final = 5_000_000_000
REQUIRED_FREE_BYTES: Final = 5_000_000_000


@dataclass(frozen=True)
class MarketRequest:
    market: str
    trade_date: str
    start: str
    end: str
    symbols: tuple[str, ...]
    request_id: str

    def provider_query(self, *, include_output_identity: bool = False) -> dict[str, object]:
        query: dict[str, object] = {
            "dataset": DATASET,
            "schema": SCHEMA,
            "stype_in": STYPE_IN,
            "symbols": list(self.symbols),
            "start": self.start,
            "end": self.end,
        }
        if include_output_identity:
            query["stype_out"] = STYPE_OUT
        return query

    def query(self) -> dict[str, object]:
        return self.provider_query(include_output_identity=True)

    def as_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "trade_date": self.trade_date,
            "request_id": self.request_id,
            "query": self.query(),
        }


@dataclass(frozen=True)
class Preflight:
    billable_bytes: dict[str, int]
    total_billable_bytes: int
    empty_request_count: int
    metadata_calls: int
    metadata_attempts: int

    def as_dict(self) -> dict[str, object]:
        core = {
            "schema_version": "l1_execution_bbo_1s_market_partition_preflight/1.0.0",
            "state": "PASS_ZERO_COST_CURRENT_ENTITLEMENT_BOUNDED_COVERAGE",
            "billable_bytes_by_request": self.billable_bytes,
            "total_billable_bytes": self.total_billable_bytes,
            "empty_request_count": self.empty_request_count,
            "metadata_calls": self.metadata_calls,
            "metadata_attempts": self.metadata_attempts,
            "maximum_parallel_preflight_requests": MAXIMUM_PREFLIGHT_WORKERS,
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


def _object(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{label} is invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{label} is not an object")
    return value


def _request_core(
    *, market: str, trade_date: str, start: str, end: str, symbols: Sequence[str],
) -> dict[str, object]:
    return {
        "market": market,
        "trade_date": trade_date,
        "start": start,
        "end": end,
        "symbols": list(symbols),
        "dataset": DATASET,
        "schema": SCHEMA,
        "stype_in": STYPE_IN,
        "stype_out": STYPE_OUT,
    }


def build_request_ledger(predecessor_plan: Mapping[str, object]) -> tuple[MarketRequest, ...]:
    requests: list[MarketRequest] = []
    for daily in predecessor.build_request_ledger(predecessor_plan):
        for market, symbols in MARKET_SYMBOLS.items():
            core = _request_core(
                market=market,
                trade_date=daily.trade_date,
                start=daily.start,
                end=daily.end,
                symbols=symbols,
            )
            requests.append(
                MarketRequest(
                    market=market,
                    trade_date=daily.trade_date,
                    start=daily.start,
                    end=daily.end,
                    symbols=symbols,
                    request_id=sha256_json(core),
                )
            )
    return tuple(requests)


def canonical_paths(request: MarketRequest) -> tuple[Path, Path]:
    raw = (
        Path("data/dbn/bbo_1s")
        / request.market
        / request.trade_date[:4]
        / f"{request.trade_date}.dbn.zst"
    )
    return raw, raw.with_name(f"{raw.name}.manifest.json")


def _expected_predecessor(
    root: Path, predecessor_plan: Mapping[str, object],
) -> dict[str, object]:
    plan_sha256 = sha256_file(root / predecessor.PLAN_PATH)
    manifest_path = (
        root
        / "state/provider_acquisition_staging/l1_execution_quarantine_v1"
        / "2a5621a9a895512cbd499f7a034217f255e3d509bbb0f86efff977e86b675730"
        / "manifest.json"
    )
    manifest = _object(manifest_path, "sealed L1 predecessor manifest")
    manifest_core = dict(manifest)
    manifest_id = manifest_core.pop("manifest_id", None)
    record_path = (
        root
        / "state/provider_acquisitions/l1_execution_quarantine_v1"
        / "15dc5ccf4fdfcf4f48d586086f8fad2c2dee36f40f53e7763e47102b042edd83.json"
    )
    record = _object(record_path, "sealed L1 predecessor acquisition record")
    manifest_sha256 = sha256_file(manifest_path)
    staged = manifest.get("staged_files")
    empty_dates = (
        sorted(
            str(item["request"]["trade_date"])
            for item in staged
            if isinstance(item, Mapping) and item.get("raw_relative_path") is None
        )
        if isinstance(staged, list)
        else []
    )
    if (
        manifest_id != sha256_json(manifest_core)
        or manifest.get("plan_id") != predecessor_plan["plan_id"]
        or manifest.get("plan_sha256") != plan_sha256
        or manifest.get("request_count") != 261
        or manifest.get("nonempty_request_count") != 259
        or manifest.get("empty_request_count") != 2
        or manifest.get("dbn_rows_decoded") != 0
        or manifest.get("row_quality_certified") is not False
        or manifest.get("canonical_promotion_performed") is not False
        or empty_dates != list(PREDECESSOR_EMPTY_DATES)
        or record
        != {
            "schema_version": "l1_execution_quarantine_acquisition/1.0.0",
            "state": "PUBLISHED_SEALED_PROVIDER_CUSTODY_ONLY",
            "record_id": manifest_id,
            "manifest_path": manifest_path.relative_to(root).as_posix(),
            "manifest_sha256": manifest_sha256,
            "plan_id": predecessor_plan["plan_id"],
            "canonical_promotion_performed": False,
            "row_quality_certified": False,
        }
    ):
        raise IntegrityError("sealed L1 predecessor evidence differs")
    return {
        "acquisition_plan_path": predecessor.PLAN_PATH.as_posix(),
        "acquisition_plan_id": predecessor_plan["plan_id"],
        "acquisition_plan_sha256": plan_sha256,
        "acquisition_manifest_path": manifest_path.relative_to(root).as_posix(),
        "acquisition_manifest_id": manifest_id,
        "acquisition_manifest_sha256": manifest_sha256,
        "acquisition_record_path": record_path.relative_to(root).as_posix(),
        "acquisition_record_sha256": sha256_file(record_path),
    }


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH, "L1 BBO-1s market-partition plan")
    predecessor_plan = predecessor.load_plan(root=root)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version")
        != "l1_execution_bbo_1s_market_partition_plan/1.0.0"
        or plan.get("state") != "PREPARED_REQUIRES_SEPARATE_PROVIDER_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("implementation")
        != {
            "path": IMPLEMENTATION_PATH.as_posix(),
            "sha256": sha256_file(Path(__file__)),
        }
        or plan.get("predecessor") != _expected_predecessor(root, predecessor_plan)
        or plan.get("period") != predecessor_plan["period"]
        or plan.get("market_symbol_groups")
        != {market: list(symbols) for market, symbols in MARKET_SYMBOLS.items()}
        or plan.get("provider_degraded_dates")
        != {
            "dates": list(DEGRADED_DATES),
            "evidence_artifact_present": False,
            "evidence_basis": "USER_REPORTED_DATABENTO_WARNING_FROM_COMPLETED_ACQUISITION",
            "required_disposition": "PROVIDER_DEGRADED_QUARANTINED",
            "scope_without_narrower_authoritative_evidence": "ALL_EIGHTEEN_MARKETS",
        }
        or plan.get("zero_billable_session_policy")
        != {
            "predecessor_combined_request_dates": list(PREDECESSOR_EMPTY_DATES),
            "fresh_zero_billable_disposition": (
                "PROVIDER_ZERO_BILLABLE_NO_DBN_NOT_ROW_QUALITY_CERTIFIED"
            ),
            "market_schedule_certified": False,
        }
        or plan.get("layout")
        != {
            "canonical_dbn_template": "data/dbn/bbo_1s/{market}/{year}/{trade_date}.dbn.zst",
            "canonical_sidecar_template": "data/dbn/bbo_1s/{market}/{year}/{trade_date}.dbn.zst.manifest.json",
            "combined_file_promotion_forbidden": True,
            "provider_native_market_partition_required": True,
            "trade_date_basis": "AMERICA_CHICAGO_LOCAL_DATE",
        }
        or plan.get("staging")
        != {
            "candidate_template": (
                "state/provider_acquisition_staging/"
                "l1_bbo1s_mp_v1/{receipt_id}/{market}/{year}/{trade_date}.dbn.zst"
            ),
            "create_only": True,
            "preserve_failed_partials": True,
            "canonical_publication": False,
        }
        or plan.get("limits")
        != {
            "maximum_billable_bytes": MAXIMUM_BILLABLE_BYTES,
            "maximum_host_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_metadata_attempts": MAXIMUM_METADATA_ATTEMPTS,
            "maximum_metadata_calls": MAXIMUM_METADATA_CALLS,
            "maximum_parallel_preflight_requests": MAXIMUM_PREFLIGHT_WORKERS,
            "maximum_parallel_downloads": MAXIMUM_WORKERS,
            "maximum_retries_per_request": MAXIMUM_RETRIES,
            "maximum_staged_bytes": MAXIMUM_STAGED_BYTES,
            "maximum_timeseries_attempts": MAXIMUM_TIMESERIES_ATTEMPTS,
            "minimum_billable_bytes": MINIMUM_BILLABLE_BYTES,
            "minimum_nonempty_requests": MINIMUM_NONEMPTY_REQUESTS,
            "minimum_nonempty_requests_per_market": MINIMUM_NONEMPTY_REQUESTS_PER_MARKET,
            "request_count": REQUEST_COUNT,
            "required_free_disk_bytes": REQUIRED_FREE_BYTES,
            "throttled_parallel_downloads": THROTTLED_WORKERS,
        }
        or plan.get("authority")
        != {
            "active_source_contract_change": False,
            "alpha_source_allowlist_change": False,
            "canonical_publication": False,
            "dbn_row_read": False,
            "provider_access": False,
            "trading_or_order_access": False,
        }
    ):
        raise IntegrityError("L1 BBO-1s market-partition plan drifted")
    ledger = build_request_ledger(predecessor_plan)
    if (
        len(ledger) != REQUEST_COUNT
        or len({item.request_id for item in ledger}) != REQUEST_COUNT
        or len(SYMBOLS) != 20
    ):
        raise IntegrityError("L1 BBO-1s market-partition request ledger drifted")
    require_current_real_history_operation(OPERATION, {})
    return plan


def build_file_provider_apis(*, root: Path) -> predecessor.ProviderApis:
    return predecessor.build_file_provider_apis(root=root)


def _metadata_call(
    method: Callable[..., object], *, attempts: dict[str, int], deadline: float,
    clock: Callable[[], float], sleeper: Callable[[float], None],
    kwargs: Mapping[str, object],
) -> object:
    for retry in range(MAXIMUM_RETRIES + 1):
        attempts["count"] += 1
        if attempts["count"] > MAXIMUM_METADATA_ATTEMPTS:
            raise UnauthorizedOperation("market-partition metadata-attempt ceiling exceeded")
        try:
            return predecessor._provider_call(
                method, deadline=deadline, clock=clock, kwargs=kwargs,
            )
        except Exception as exc:
            if retry >= MAXIMUM_RETRIES or not predecessor._transient(exc):
                raise
            delay = predecessor._retry_after(exc, retry + 1)
            if delay >= predecessor._remaining(deadline, clock):
                raise UnauthorizedOperation(
                    "market-partition runtime cannot accommodate metadata retry"
                ) from exc
            sleeper(delay)
    raise AssertionError("unreachable metadata retry state")


def _validate_symbology(value: object, plan: Mapping[str, object]) -> None:
    if not isinstance(value, Mapping):
        raise IntegrityError("provider symbology response is invalid")
    if value.get("partial") not in (None, [], (), False, 0) or value.get(
        "not_found"
    ) not in (None, [], ()):
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


def run_preflight(
    *, plan: Mapping[str, object], ledger: Sequence[MarketRequest],
    apis: predecessor.ProviderApis, deadline: float | None = None,
    provider_factory: Callable[[], predecessor.ProviderApis] | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> Preflight:
    if len(ledger) != REQUEST_COUNT or len({item.request_id for item in ledger}) != REQUEST_COUNT:
        raise IntegrityError("market-partition preflight ledger is not exact")
    deadline = clock() + MAXIMUM_RUNTIME_SECONDS if deadline is None else deadline
    attempts = {"count": 0}
    predecessor._validate_entitlement(
        _metadata_call(
            apis.get_dataset_range,
            attempts=attempts,
            deadline=deadline,
            clock=clock,
            sleeper=sleeper,
            kwargs={"dataset": DATASET},
        ),
        ledger,  # type: ignore[arg-type]
    )
    period = plan["period"]  # type: ignore[index]
    _validate_symbology(
        _metadata_call(
            apis.resolve,
            attempts=attempts,
            deadline=deadline,
            clock=clock,
            sleeper=sleeper,
            kwargs={
                "dataset": DATASET,
                "symbols": list(SYMBOLS),
                "stype_in": STYPE_IN,
                "stype_out": STYPE_OUT,
                "start_date": period["start_date_inclusive"],
                "end_date": "2026-09-01",
            },
        ),
        plan,
    )
    stop = threading.Event()

    def measure(worker_index: int) -> tuple[dict[str, int], int]:
        worker_apis = (
            apis
            if worker_index == 0 or provider_factory is None
            else provider_factory()
        )
        worker_attempts = {"count": 0}
        worker_sizes: dict[str, int] = {}
        try:
            for request in ledger[worker_index::MAXIMUM_PREFLIGHT_WORKERS]:
                if stop.is_set():
                    break
                query = request.provider_query()
                predecessor._zero_cost(
                    _metadata_call(
                        worker_apis.get_cost,
                        attempts=worker_attempts,
                        deadline=deadline,
                        clock=clock,
                        sleeper=sleeper,
                        kwargs=query,
                    )
                )
                size = _metadata_call(
                    worker_apis.get_billable_size,
                    attempts=worker_attempts,
                    deadline=deadline,
                    clock=clock,
                    sleeper=sleeper,
                    kwargs=query,
                )
                if type(size) is not int or size < 0:
                    raise IntegrityError("provider billable-size response is invalid")
                worker_sizes[request.request_id] = size
        except BaseException:
            stop.set()
            raise
        return worker_sizes, worker_attempts["count"]

    with ThreadPoolExecutor(
        max_workers=MAXIMUM_PREFLIGHT_WORKERS,
        thread_name_prefix="l1-bbo1s-preflight",
    ) as pool:
        futures = [
            pool.submit(measure, index)
            for index in range(MAXIMUM_PREFLIGHT_WORKERS)
        ]
        results = [future.result() for future in futures]
    sizes = {
        request_id: size
        for batch, _ in results
        for request_id, size in batch.items()
    }
    attempts["count"] += sum(count for _, count in results)
    if len(sizes) != len(ledger):
        raise IntegrityError("market-partition preflight worker pool was incomplete")
    observed_empty_ids = {request_id for request_id, size in sizes.items() if size == 0}
    total = sum(sizes.values())
    nonempty = len(sizes) - len(observed_empty_ids)
    nonempty_by_market = {
        market: sum(
            sizes[request.request_id] > 0
            for request in ledger
            if request.market == market
        )
        for market in MARKET_SYMBOLS
    }
    if (
        2 + 2 * len(ledger) != MAXIMUM_METADATA_CALLS
        or nonempty < MINIMUM_NONEMPTY_REQUESTS
        or min(nonempty_by_market.values()) < MINIMUM_NONEMPTY_REQUESTS_PER_MARKET
        or not MINIMUM_BILLABLE_BYTES <= total <= MAXIMUM_BILLABLE_BYTES
    ):
        raise UnauthorizedOperation(
            "fresh market-partition cost, coverage, or billable-size bounds failed"
        )
    return Preflight(
        sizes,
        total,
        len(observed_empty_ids),
        MAXIMUM_METADATA_CALLS,
        attempts["count"],
    )


def verify_store_metadata(*, store: object, request: MarketRequest) -> None:
    metadata = getattr(store, "metadata", None)
    if (
        metadata is None
        or getattr(metadata, "dataset", None) != DATASET
        or str(getattr(metadata, "schema", "")) != SCHEMA
        or str(getattr(metadata, "stype_in", "")) != STYPE_IN
        or str(getattr(metadata, "stype_out", "")) != STYPE_OUT
        or getattr(metadata, "start", None) != predecessor._iso_to_ns(request.start)
        or getattr(metadata, "end", None) != predecessor._iso_to_ns(request.end)
        or sorted(str(item) for item in getattr(metadata, "symbols", ()))
        != sorted(request.symbols)
    ):
        raise IntegrityError("downloaded market-partition DBN metadata differs")


def _paths(attempt: Path, request: MarketRequest) -> tuple[Path, Path]:
    raw = attempt / request.market / request.trade_date[:4] / f"{request.trade_date}.dbn.zst"
    return raw, raw.with_name(f"{raw.name}.manifest.json")


def _sidecar_core(
    *, request: MarketRequest, plan_id: str, predecessor_manifest_id: str,
    billable_bytes: int, dbn_path: Path | None,
) -> dict[str, object]:
    degraded = request.trade_date in DEGRADED_DATES
    if dbn_path is None:
        state = "PROVIDER_ZERO_BILLABLE_NO_DBN_NOT_ROW_QUALITY_CERTIFIED"
    elif degraded:
        state = "PROVIDER_DEGRADED_QUARANTINED"
    else:
        state = "SEALED_MARKET_PARTITION_NOT_ROW_QUALITY_CERTIFIED"
    logical_path, _ = canonical_paths(request)
    return {
        "schema_version": SIDECAR_SCHEMA,
        "state": state,
        "disposition": state,
        "plan_id": plan_id,
        "dataset": DATASET,
        "schema": SCHEMA,
        "market": request.market,
        "trade_date": request.trade_date,
        "logical_path": logical_path.as_posix(),
        "request_id": request.request_id,
        "query": request.query(),
        "billable_bytes": billable_bytes,
        "file_size_bytes": None if dbn_path is None else dbn_path.stat().st_size,
        "file_sha256": None if dbn_path is None else sha256_file(dbn_path),
        "predecessor_manifest_id": predecessor_manifest_id,
        "provider_condition": (
            "ZERO_BILLABLE_NO_DBN"
            if dbn_path is None
            else "DEGRADED" if degraded else "NOT_REPORTED_DEGRADED"
        ),
        "quality_certificate_id": None,
        "dbn_rows_decoded": 0,
        "row_quality_certified": False,
        "canonical_promotion_authorized": False,
        "alpha_source_authorized": False,
    }


def build_candidate_sidecar(
    *, request: MarketRequest, dbn_path: Path, plan_id: str,
    predecessor_manifest_id: str, billable_bytes: int,
) -> dict[str, object]:
    if (
        billable_bytes <= 0
        or not dbn_path.is_file()
        or is_linklike(dbn_path)
    ):
        raise IntegrityError("BBO-1s candidate DBN is invalid")
    core = _sidecar_core(
        request=request,
        plan_id=plan_id,
        predecessor_manifest_id=predecessor_manifest_id,
        billable_bytes=billable_bytes,
        dbn_path=dbn_path,
    )
    return {**core, "sidecar_id": sha256_json(core)}


def build_empty_sidecar(
    *, request: MarketRequest, plan_id: str, predecessor_manifest_id: str,
) -> dict[str, object]:
    core = _sidecar_core(
        request=request,
        plan_id=plan_id,
        predecessor_manifest_id=predecessor_manifest_id,
        billable_bytes=0,
        dbn_path=None,
    )
    return {**core, "sidecar_id": sha256_json(core)}


def _load_metadata(path: Path) -> object:
    from databento import DBNStore

    return DBNStore.from_file(path).metadata


def validate_candidate_pair(
    *, request: MarketRequest, dbn_path: Path, sidecar_path: Path, plan_id: str,
    predecessor_manifest_id: str, billable_bytes: int,
    metadata_loader: Callable[[Path], object] | None = None,
) -> dict[str, object]:
    if sidecar_path != dbn_path.with_name(f"{dbn_path.name}.manifest.json"):
        raise IntegrityError("BBO-1s candidate sidecar is not adjacent")
    if not sidecar_path.is_file() or is_linklike(sidecar_path):
        raise IntegrityError("BBO-1s candidate sidecar is absent or link-like")
    sidecar = _object(sidecar_path, "BBO-1s candidate sidecar")
    expected = build_candidate_sidecar(
        request=request,
        dbn_path=dbn_path,
        plan_id=plan_id,
        predecessor_manifest_id=predecessor_manifest_id,
        billable_bytes=billable_bytes,
    )
    if sidecar != expected or sidecar_path.read_bytes() != canonical_bytes(sidecar) + b"\n":
        raise IntegrityError("BBO-1s candidate sidecar differs")
    metadata = (metadata_loader or _load_metadata)(dbn_path)
    verify_store_metadata(store=SimpleNamespace(metadata=metadata), request=request)
    return sidecar


def _verify_staged_sidecar(
    *, attempt: Path, plan: Mapping[str, object], request: MarketRequest,
    billable_bytes: int, metadata_loader: Callable[[Path], object] | None = None,
) -> dict[str, object] | None:
    raw, sidecar_path = _paths(attempt, request)
    if not raw.exists() and not sidecar_path.exists():
        return None
    if not sidecar_path.is_file() or is_linklike(sidecar_path):
        raise IntegrityError("staged market request has an incomplete or linked sidecar")
    predecessor_manifest_id = str(plan["predecessor"]["acquisition_manifest_id"])
    if billable_bytes == 0:
        if raw.exists():
            raise IntegrityError("empty market request unexpectedly has a DBN")
        sidecar = _object(sidecar_path, "empty BBO-1s candidate sidecar")
        expected = build_empty_sidecar(
            request=request,
            plan_id=str(plan["plan_id"]),
            predecessor_manifest_id=predecessor_manifest_id,
        )
        if sidecar != expected or sidecar_path.read_bytes() != canonical_bytes(sidecar) + b"\n":
            raise IntegrityError("empty BBO-1s candidate sidecar differs")
        return sidecar
    if not raw.is_file() or is_linklike(raw):
        raise IntegrityError("staged market request has an incomplete or linked DBN")
    return validate_candidate_pair(
        request=request,
        dbn_path=raw,
        sidecar_path=sidecar_path,
        plan_id=str(plan["plan_id"]),
        predecessor_manifest_id=predecessor_manifest_id,
        billable_bytes=billable_bytes,
        metadata_loader=metadata_loader,
    )


def synthetic_metadata(request: MarketRequest) -> SimpleNamespace:
    """Return exact metadata for synthetic tests without reading a DBN."""

    return SimpleNamespace(
        dataset=DATASET,
        schema=SCHEMA,
        stype_in=STYPE_IN,
        stype_out=STYPE_OUT,
        symbols=list(request.symbols),
        start=predecessor._iso_to_ns(request.start),
        end=predecessor._iso_to_ns(request.end),
    )


def _register_bytes(path: Path, *, total: dict[str, int], lock: threading.Lock) -> None:
    if not path.is_file():
        return
    with lock:
        total["bytes"] += path.stat().st_size
        if total["bytes"] > MAXIMUM_STAGED_BYTES:
            raise UnauthorizedOperation("market-partition staged-byte ceiling exceeded")


def _download_worker(
    *, index: int, work: queue.Queue[MarketRequest], attempt: Path,
    plan: Mapping[str, object], preflight: Preflight,
    provider_factory: Callable[[], predecessor.ProviderApis], stop: threading.Event,
    throttled: threading.Event, total: dict[str, int], total_lock: threading.Lock,
    call_state: dict[str, int], call_lock: threading.Lock,
    sleeper: Callable[[float], None], deadline: float, clock: Callable[[], float],
) -> WorkerResult:
    completed: list[str] = []
    failed_request_id: str | None = None
    attempts = 0
    try:
        predecessor._remaining(deadline, clock)
        apis = provider_factory()
        predecessor._remaining(deadline, clock)
        while not stop.is_set():
            predecessor._remaining(deadline, clock)
            if throttled.is_set() and index >= THROTTLED_WORKERS:
                break
            try:
                request = work.get_nowait()
            except queue.Empty:
                break
            failed_request_id = request.request_id
            for retry in range(MAXIMUM_RETRIES + 1):
                registered = False
                raw, sidecar_path = _paths(attempt, request)
                partial = raw.with_name(f"{raw.name}.partial.{uuid.uuid4().hex[:16]}")
                partial.parent.mkdir(parents=True, exist_ok=True)
                try:
                    with call_lock:
                        call_state["attempts"] += 1
                        if call_state["attempts"] > MAXIMUM_TIMESERIES_ATTEMPTS:
                            raise UnauthorizedOperation(
                                "market-partition timeseries call ceiling exceeded"
                            )
                    attempts += 1
                    store = predecessor._provider_call(
                        apis.get_range,
                        deadline=deadline,
                        clock=clock,
                        kwargs={
                            **request.provider_query(include_output_identity=True),
                            "path": str(partial),
                        },
                    )
                    if not partial.is_file() or partial.stat().st_size <= 0:
                        raise IntegrityError("provider did not create a nonempty DBN file")
                    verify_store_metadata(store=store, request=request)
                    del store
                    _register_bytes(partial, total=total, lock=total_lock)
                    registered = True
                    if raw.exists() or sidecar_path.exists():
                        raise IntegrityError(
                            "create-only market-partition destination appeared during download"
                        )
                    sidecar = build_candidate_sidecar(
                        request=request,
                        dbn_path=partial,
                        plan_id=str(plan["plan_id"]),
                        predecessor_manifest_id=str(
                            plan["predecessor"]["acquisition_manifest_id"]
                        ),
                        billable_bytes=preflight.billable_bytes[request.request_id],
                    )
                    sidecar_partial = sidecar_path.with_name(
                        f"{sidecar_path.name}.partial.{uuid.uuid4().hex[:16]}"
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
                    if predecessor._status_code(exc) == 429:
                        throttled.set()
                    if retry >= MAXIMUM_RETRIES or not predecessor._transient(exc):
                        raise
                    delay = predecessor._retry_after(exc, retry + 1)
                    if delay >= predecessor._remaining(deadline, clock):
                        raise UnauthorizedOperation(
                            "market-partition runtime cannot accommodate timeseries retry"
                        ) from exc
                    sleeper(delay)
            work.task_done()
    except Exception as exc:
        stop.set()
        return WorkerResult(index, tuple(completed), attempts, type(exc).__name__, failed_request_id)
    return WorkerResult(index, tuple(completed), attempts, None, None)


def download_missing(
    *, attempt: Path, plan: Mapping[str, object], requests: Sequence[MarketRequest],
    preflight: Preflight,
    provider_factory: Callable[[], predecessor.ProviderApis],
    existing_staged_bytes: int = 0,
    sleeper: Callable[[float], None] = time.sleep,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[WorkerResult, ...]:
    deadline = clock() + MAXIMUM_RUNTIME_SECONDS if deadline is None else deadline
    work: queue.Queue[MarketRequest] = queue.Queue()
    for request in requests:
        work.put(request)
    stop = threading.Event()
    throttled = threading.Event()
    total = {"bytes": existing_staged_bytes}
    total_lock = threading.Lock()
    call_state = {"attempts": 0}
    call_lock = threading.Lock()
    with ThreadPoolExecutor(
        max_workers=MAXIMUM_WORKERS, thread_name_prefix="l1-bbo1s-market",
    ) as pool:
        futures = [
            pool.submit(
                _download_worker,
                index=index,
                work=work,
                attempt=attempt,
                plan=plan,
                preflight=preflight,
                provider_factory=provider_factory,
                stop=stop,
                throttled=throttled,
                total=total,
                total_lock=total_lock,
                call_state=call_state,
                call_lock=call_lock,
                sleeper=sleeper,
                deadline=deadline,
                clock=clock,
            )
            for index in range(MAXIMUM_WORKERS)
        ]
        results = tuple(future.result() for future in futures)
    failures = [result for result in results if result.failure_type is not None]
    if failures:
        error = IntegrityError(
            f"market-partition download worker failed closed: {failures[0].failure_type}"
        )
        error.failed_request_id = failures[0].failed_request_id  # type: ignore[attr-defined]
        raise error
    if not work.empty():
        raise IntegrityError("market-partition worker pool stopped before completing its queue")
    return results


def _required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "plan_id": str(plan["plan_id"]),
        "implementation_sha256": str(plan["implementation"]["sha256"]),
        "predecessor_manifest_id": str(plan["predecessor"]["acquisition_manifest_id"]),
        "provider": "Databento",
        "dataset": DATASET,
        "schema": SCHEMA,
        "markets": ",".join(MARKET_SYMBOLS),
        "period": "2025-09-01/2026-08-31",
        "request_count": str(REQUEST_COUNT),
        "minimum_nonempty_requests": str(MINIMUM_NONEMPTY_REQUESTS),
        "minimum_nonempty_requests_per_market": str(
            MINIMUM_NONEMPTY_REQUESTS_PER_MARKET
        ),
        "maximum_parallel_downloads": str(MAXIMUM_WORKERS),
        "maximum_parallel_preflight_requests": str(MAXIMUM_PREFLIGHT_WORKERS),
        "maximum_external_cost_usd": "0",
        "minimum_billable_bytes": str(MINIMUM_BILLABLE_BYTES),
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


def _write_failure_receipt(
    *, root: Path, attempt: Path, plan: Mapping[str, object],
    authorization: OperationReceipt, claim: Path, exc: Exception,
) -> None:
    completed = sum(
        path.is_file() and not is_linklike(path)
        for path in attempt.glob("*/*/*.dbn.zst.manifest.json")
    )
    core = {
        "schema_version": "l1_execution_bbo_1s_market_partition_failure/1.0.0",
        "state": "FAILED_CLOSED_RESUMABLE",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "implementation_sha256": plan["implementation"]["sha256"],
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
    predecessor._atomic_json(
        attempt / "failures" / f"{uuid.uuid4().hex[:16]}.json", payload,
    )


def execute_authorized_acquisition(
    *, root: Path, authorization: OperationReceipt,
    provider_factory: Callable[[], predecessor.ProviderApis], credential_source: str,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = predecessor.require_locked_repository_environment,
    metadata_loader: Callable[[Path], object] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    """Acquire provider-native market files into sealed staging only."""

    started = clock()
    deadline = started + MAXIMUM_RUNTIME_SECONDS
    root = root.resolve(strict=True)
    plan = load_plan(root=root)
    ledger = build_request_ledger(predecessor.load_plan(root=root))
    if credential_source != CREDENTIAL_SOURCE:
        raise UnauthorizedOperation("market-partition credential source is not bound")
    environment_check(root)
    free = getattr(disk_usage(root), "free", None)
    if type(free) is not int or free < REQUIRED_FREE_BYTES:
        raise UnauthorizedOperation("insufficient free space for market-partition acquisition")
    boundary = RepoBoundary(root)
    scope = _required_scope(root=root, plan=plan)
    attempt = root / STAGING_ROOT / authorization.receipt_id
    lock_path = root / LOCK_PATH
    boundary.assert_active_path(
        lock_path.absolute(),
        purpose="market-partition acquisition process lock",
        subtree="state/locks",
    )
    instance = predecessor.SingleInstance(lock_path)
    if not instance.acquire():
        raise UnauthorizedOperation("another market-partition acquisition is already running")
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
            metadata_loader=metadata_loader,
        )
    finally:
        instance.release()


def _execute_locked(
    *, root: Path, authorization: OperationReceipt,
    provider_factory: Callable[[], predecessor.ProviderApis],
    sleeper: Callable[[float], None], clock: Callable[[], float], deadline: float,
    boundary: RepoBoundary, plan: Mapping[str, object],
    ledger: Sequence[MarketRequest], attempt: Path, scope: Mapping[str, str],
    metadata_loader: Callable[[Path], object] | None,
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
        attempt.absolute(),
        purpose="L1 market-partition acquisition attempt",
        subtree=STAGING_ROOT.as_posix(),
    )
    attempt.mkdir(parents=True, exist_ok=True)
    request_plan = {
        "schema_version": "l1_execution_bbo_1s_market_partition_request_ledger/1.0.0",
        "plan_id": plan["plan_id"],
        "requests": [item.as_dict() for item in ledger],
        "request_set_id": sha256_json([item.as_dict() for item in ledger]),
    }
    try:
        predecessor._write_or_verify(
            attempt / "request_plan.json", request_plan, "market-partition request ledger",
        )
        predecessor._remaining(deadline, clock)
        preflight = run_preflight(
            plan=plan,
            ledger=ledger,
            apis=provider_factory(),
            provider_factory=provider_factory,
            deadline=deadline,
            clock=clock,
            sleeper=sleeper,
        )
        predecessor._write_or_verify(
            attempt / "preflight.json", preflight.as_dict(), "market-partition preflight",
        )
        missing_positive: list[MarketRequest] = []
        for request in ledger:
            predecessor._remaining(deadline, clock)
            billable = preflight.billable_bytes[request.request_id]
            if _verify_staged_sidecar(
                attempt=attempt,
                plan=plan,
                request=request,
                billable_bytes=billable,
                metadata_loader=metadata_loader,
            ) is None:
                if billable == 0:
                    _, sidecar_path = _paths(attempt, request)
                    predecessor._atomic_json(
                        sidecar_path,
                        build_empty_sidecar(
                            request=request,
                            plan_id=str(plan["plan_id"]),
                            predecessor_manifest_id=str(
                                plan["predecessor"]["acquisition_manifest_id"]
                            ),
                        ),
                    )
                else:
                    missing_positive.append(request)
        existing_size = predecessor._tree_size(attempt)
        if existing_size > MAXIMUM_STAGED_BYTES:
            raise UnauthorizedOperation(
                "existing market-partition attempt exceeds its staged-byte ceiling"
            )
        if missing_positive:
            download_missing(
                attempt=attempt,
                plan=plan,
                requests=missing_positive,
                preflight=preflight,
                provider_factory=provider_factory,
                existing_staged_bytes=existing_size,
                sleeper=sleeper,
                deadline=deadline,
                clock=clock,
            )
        staged = [
            _verify_staged_sidecar(
                attempt=attempt,
                plan=plan,
                request=request,
                billable_bytes=preflight.billable_bytes[request.request_id],
                metadata_loader=metadata_loader,
            )
            for request in ledger
        ]
        if any(item is None for item in staged):
            raise IntegrityError("market-partition acquisition is incomplete")
        staged_size = predecessor._tree_size(attempt)
        if staged_size > MAXIMUM_STAGED_BYTES:
            raise UnauthorizedOperation(
                "completed market-partition attempt exceeds its staged-byte ceiling"
            )
        manifest_core = {
            "schema_version": "l1_execution_bbo_1s_market_partition_manifest/1.0.0",
            "state": "SEALED_MARKET_PARTITION_CUSTODY_NOT_ROW_QUALITY_CERTIFIED",
            "plan_id": plan["plan_id"],
            "plan_sha256": sha256_file(root / PLAN_PATH),
            "implementation_sha256": plan["implementation"]["sha256"],
            "predecessor_manifest_id": plan["predecessor"]["acquisition_manifest_id"],
            "authorization_receipt_id": authorization.receipt_id,
            "authorization_claim_sha256": sha256_file(claim),
            "request_count": len(ledger),
            "nonempty_request_count": len(ledger) - preflight.empty_request_count,
            "empty_request_count": preflight.empty_request_count,
            "provider_degraded_request_count": len(DEGRADED_DATES) * len(MARKET_SYMBOLS),
            "fresh_total_billable_bytes": preflight.total_billable_bytes,
            "staged_files": staged,
            "staged_files_id": sha256_json(staged),
            "maximum_parallel_downloads": MAXIMUM_WORKERS,
            "maximum_parallel_preflight_requests": MAXIMUM_PREFLIGHT_WORKERS,
            "throttled_parallel_downloads": THROTTLED_WORKERS,
            "dbn_rows_decoded": 0,
            "row_quality_certified": False,
            "execution_eligible_request_count": 0,
            "alpha_source_authorized": False,
            "active_source_contract_changed": False,
            "canonical_promotion_performed": False,
        }
        manifest = {**manifest_core, "manifest_id": sha256_json(manifest_core)}
        predecessor._write_or_verify(
            attempt / "manifest.json", manifest, "market-partition terminal manifest",
        )
        record = {
            "schema_version": "l1_execution_bbo_1s_market_partition_acquisition/1.0.0",
            "state": "PUBLISHED_SEALED_MARKET_PARTITION_CUSTODY_ONLY",
            "record_id": manifest["manifest_id"],
            "manifest_path": (attempt / "manifest.json").relative_to(root).as_posix(),
            "manifest_sha256": sha256_file(attempt / "manifest.json"),
            "plan_id": plan["plan_id"],
            "canonical_promotion_performed": False,
            "row_quality_certified": False,
        }
        event = {
            "schema_version": "l1_execution_bbo_1s_market_partition_event/1.0.0",
            "event_type": "SEALED_MARKET_PARTITION_CUSTODY_ACQUIRED",
            "record_id": record["record_id"],
            "authorization_receipt_id": authorization.receipt_id,
        }
        record_path = root / RECORD_ROOT / f"{record['record_id']}.json"
        event_path = root / EVENT_ROOT / f"{record['record_id']}.json"
        boundary.assert_active_path(
            record_path.absolute(),
            purpose="market-partition acquisition record",
            subtree=RECORD_ROOT.as_posix(),
        )
        boundary.assert_active_path(
            event_path.absolute(),
            purpose="market-partition acquisition event",
            subtree=EVENT_ROOT.as_posix(),
        )
        predecessor._write_or_verify(record_path, record, "market-partition acquisition record")
        predecessor._write_or_verify(event_path, event, "market-partition acquisition event")
        return {
            "record_id": record["record_id"],
            "attempt_path": attempt.relative_to(root).as_posix(),
            "request_count": len(ledger),
            "nonempty_request_count": len(ledger) - preflight.empty_request_count,
            "empty_request_count": preflight.empty_request_count,
            "provider_degraded_request_count": len(DEGRADED_DATES) * len(MARKET_SYMBOLS),
            "fresh_total_billable_bytes": preflight.total_billable_bytes,
            "maximum_parallel_downloads": MAXIMUM_WORKERS,
            "maximum_parallel_preflight_requests": MAXIMUM_PREFLIGHT_WORKERS,
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


def execute_receipt_file_acquisition(
    *, root: Path, authorization_path: Path,
    provider_factory: Callable[[], predecessor.ProviderApis] | None = None,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = predecessor.require_locked_repository_environment,
    metadata_loader: Callable[[Path], object] | None = None,
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
        metadata_loader=metadata_loader,
        sleeper=sleeper,
        clock=clock,
    )


__all__ = [
    "CREDENTIAL_SOURCE",
    "DEGRADED_DATES",
    "PREDECESSOR_EMPTY_DATES",
    "MARKET_SYMBOLS",
    "MAXIMUM_PREFLIGHT_WORKERS",
    "MAXIMUM_WORKERS",
    "MarketRequest",
    "OPERATION",
    "PLAN_PATH",
    "Preflight",
    "STAGING_ROOT",
    "build_candidate_sidecar",
    "build_empty_sidecar",
    "build_file_provider_apis",
    "build_request_ledger",
    "canonical_paths",
    "download_missing",
    "execute_authorized_acquisition",
    "execute_receipt_file_acquisition",
    "load_plan",
    "required_scope",
    "run_preflight",
    "synthetic_metadata",
    "validate_candidate_pair",
    "verify_store_metadata",
]
