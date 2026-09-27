"""Prepare and approval-gate quality certification of sealed TBBO custody."""

from __future__ import annotations

from .custody_successor import resolve_snapshot, binding_matches

import gc
import hashlib
import os
import shutil
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, time as wall_time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final
from zoneinfo import ZoneInfo

import databento
import numpy as np

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (
    canonical_bytes,
    io_path,
    is_linklike,
    sha256_file,
    sha256_json,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .dbn_catalog import (
    _canonical_metadata_mappings,
    _symbology_resolution_disposition,
)
from .foundation.decoder import (
    DEFINITION_DTYPE_V1,
    DEFINITION_DTYPE_V3,
    STATUS_DTYPE,
    SUPPORTED_DATABENTO_VERSION,
)
from .live_cockpit.databento_auth import resolve_databento_api_key
from .live_cockpit.single_instance import SingleInstance
from .runtime_environment import require_locked_repository_environment
from .source_symbology import build_query_contract, require_query_contract
from .tbbo_16_market_acquisition import (
    END as TBBO_END,
    MARKETS,
    PLAN_PATH as ACQUISITION_PLAN_PATH,
    START as TBBO_START,
    ProviderApis,
    _provider_call,
    _retry_after,
    _status_code,
    _transient,
    build_request_ledger,
    json_object,
    load_header,
    load_plan as load_acquisition_plan,
    plain_tree_files,
    protected_active_state,
    verify_sidecar,
    write_or_verify,
)


PLAN_PATH: Final = Path("configs/tbbo_16_market_quality_certification_plan_v1.json")
PREPARE_OPERATION: Final = "PREPARE_TBBO_16_MARKET_ROW_QUALITY_V1"
SUPPLEMENT_OPERATION: Final = (
    "ACQUIRE_AND_SEAL_TBBO_16_MARKET_DEFINITION_STATUS_SUPPLEMENT_V1_ONCE"
)
CERTIFY_OPERATION: Final = "CERTIFY_TBBO_16_MARKET_ROW_QUALITY_V1_ONCE"
TBBO_SNAPSHOT_ID: Final = (
    "bb6daab99dbd203775d558da22a69f9e8ad6c63a1114de57a1bc4f8d5c44cb31"
)
TBBO_REGISTRY: Final = Path("state/tbbo_custody_registry") / TBBO_SNAPSHOT_ID / "snapshot.json"
VAULT_ROOT: Final = Path("data/vault/source_snapshots")
PREPARATION_ROOT: Final = Path("state/unpublished_evidence/tbbo_16_market_quality_v1")
SUPPLEMENT_STAGING_ROOT: Final = Path(
    "state/provider_acquisition_staging/tbbo_definition_status_supplement_v1"
)
SUPPLEMENT_REGISTRY_ROOT: Final = Path("state/tbbo_quality_supplement_registry")
CERTIFICATION_REGISTRY_ROOT: Final = Path("state/tbbo_quality_certification_registry")
STATUS_SESSION_DIAGNOSTIC_PATH: Final = Path(
    "state/tbbo_status_session_diagnostics/"
    "df129698337dabe6d1fc0a0969aabc5acb90e0d26061613e0e26a0bf3bb44e9d.json"
)
SUPPLEMENT_LOCK: Final = Path("state/locks/tbbo_quality_supplement_v1.lock")
CERTIFICATION_LOCK: Final = Path("state/locks/tbbo_quality_certification_v1.lock")
HISTORICAL_POLICY_PATH: Final = Path("configs/historical_observability_policy.json")
SUPPORT_ARCHIVE_ROOT: Final = Path(
    "state/dbn_congruence_release_archive/"
    "4a2dc40618448fd9ef3a5fc415be2a3f2fde043d1027aa056d138bf9d309cd5f/"
    "l02026merge_108ea2d2bcbdf6f905f01468d48ea200/retired_2026_shards"
)
DATASET: Final = "GLBX.MDP3"
STYPE_OUT: Final = "instrument_id"
SUPPLEMENT_START: Final = "2026-07-14T00:00:00Z"
SUPPLEMENT_END: Final = "2026-09-01T00:00:00Z"
SUPPLEMENT_REQUEST_COUNT: Final = 32
MAXIMUM_SUPPLEMENT_BYTES: Final = 650_000_000
REQUIRED_SUPPLEMENT_FREE_BYTES: Final = 1_000_000_000
MAXIMUM_BATCH_ROWS: Final = 1_000_000
MAXIMUM_WORKERS: Final = 4
MAXIMUM_RUNTIME_SECONDS: Final = 21_600
MAXIMUM_OUTPUT_BYTES: Final = 64 * 1024 * 1024
MAXIMUM_RETRIES: Final = 3
UINT64_NULL: Final = 2**64 - 1
UTC_DAY_NS: Final = 86_400_000_000_000
F_BAD_TS_RECV: Final = 8
F_MAYBE_BAD_BOOK: Final = 4
DEGRADED_DATES: Final = (
    "2025-09-17",
    "2025-09-24",
    "2025-11-28",
    "2026-01-31",
    "2026-03-15",
    "2026-03-16",
    "2026-03-21",
    "2026-04-10",
    "2026-05-24",
    "2026-07-30",
    "2026-08-29",
)
BOUNDARY_SESSIONS: Final = ("2025-09-01", "2026-09-01")
MARKET_QUARANTINED_DATES: Final = {"SR3": ("2026-05-17",)}
MARKET_QUARANTINE_DEPENDENCY_POLICY: Final = (
    "EXCLUDE_UTC_DATE_AND_EVERY_DEPENDENT_SESSION_FEATURE_WINDOW"
)
STATUS_MIDNIGHT_SNAPSHOT_POLICY: Final = (
    "CARRY_FORWARD_STATE_NOT_NEW_SESSION_EVIDENCE"
)
STATUS_ONLY_SESSION_POLICY: Final = (
    "REPORT_MISSING_NOT_INFER_CLOSED_NON_BLOCKING_NO_SYNTHETIC_ROWS"
)
IDENTICAL_TBBO_ROW_POLICY: Final = (
    "RETAIN_REPORT_NON_BLOCKING_NO_DEDUPLICATION"
)
TBBO_NUMPY_DTYPE: Final = np.dtype(databento.MBP1Msg._dtypes)
TBBO_DTYPE: Final = TBBO_NUMPY_DTYPE.names
SUPPORT_ACTIVE_PATHS: Final = tuple(
    (
        Path("data/dbn")
        if year == "2025"
        else SUPPORT_ARCHIVE_ROOT / "data/dbn"
    ) / schema / market / year / filename
    for market in MARKETS
    for schema in ("definition", "status")
    for year, filename in (
        ("2025", "2025-01-01_2026-01-01.dbn.zst"),
        ("2026", "2026-01-01_2026-07-14.dbn.zst"),
    )
)


@dataclass(frozen=True)
class SupplementRequest:
    market: str
    schema: str
    stype_in: str
    symbol: str
    query_contract: dict[str, object]
    request_id: str

    @property
    def snapshot_relative_path(self) -> str:
        return (
            f"dbn/{self.schema}/{self.market}/2026/"
            "2026-07-14_2026-09-01.dbn.zst"
        )

    def provider_query(self, *, output_identity: bool = False) -> dict[str, object]:
        query: dict[str, object] = {
            "dataset": DATASET,
            "schema": self.schema,
            "stype_in": self.stype_in,
            "symbols": [self.symbol],
            "start": SUPPLEMENT_START,
            "end": SUPPLEMENT_END,
        }
        if output_identity:
            query["stype_out"] = STYPE_OUT
        return query

    def as_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "query_contract": self.query_contract,
            "request_id": self.request_id,
            "schema": self.schema,
            "snapshot_relative_path": self.snapshot_relative_path,
            "stype_out": STYPE_OUT,
        }


@dataclass(frozen=True)
class SupplementProviderApis:
    get_dataset_condition: Callable[..., object]
    get_dataset_range: Callable[..., object]
    resolve: Callable[..., object]
    get_cost: Callable[..., object]
    get_billable_size: Callable[..., object]
    get_range: Callable[..., object]


def _expected_forbidden() -> dict[str, bool]:
    return {
        "active_catalog_or_source_mutation": True,
        "alpha_feature_target_model_or_evaluation": True,
        "credential_log_stage_or_report": True,
        "decoded_dataset_creation": True,
        "provider_access_during_preparation": True,
        "row_decode_during_preparation": True,
        "trading_or_order_access": True,
    }


def load_plan(*, root: Path) -> dict[str, object]:
    plan = json_object(root / PLAN_PATH, "TBBO quality-certification plan")
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    supplement = plan.get("supplement")
    quality = plan.get("quality_scan")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version") != "tbbo_quality_certification_plan/1.0.0"
        or plan.get("state") != "PREPARATION_ONLY_NO_ROWS_DECODED"
        or plan.get("operations") != {
            "prepare": PREPARE_OPERATION,
            "supplement": SUPPLEMENT_OPERATION,
            "certify": CERTIFY_OPERATION,
        }
        or plan.get("tbbo_snapshot_id") != TBBO_SNAPSHOT_ID
        or tuple(plan.get("markets", ())) != MARKETS
        or tuple(plan.get("degraded_utc_dates", ())) != DEGRADED_DATES
        or tuple(plan.get("boundary_sessions", ())) != BOUNDARY_SESSIONS
        or plan.get("market_quarantined_utc_dates")
        != {key: list(value) for key, value in MARKET_QUARANTINED_DATES.items()}
        or plan.get("market_quarantine_dependency_policy")
        != MARKET_QUARANTINE_DEPENDENCY_POLICY
        or plan.get("status_utc_midnight_snapshot_policy")
        != STATUS_MIDNIGHT_SNAPSHOT_POLICY
        or plan.get("status_only_session_policy") != STATUS_ONLY_SESSION_POLICY
        or plan.get("identical_tbbo_row_policy") != IDENTICAL_TBBO_ROW_POLICY
        or plan.get("historical_session_policy")
        != "DATABENTO_EMPIRICAL_OBSERVABILITY_17_00_AMERICA_CHICAGO_ROLL_NOT_OFFICIAL_CME_CALENDAR"
        or plan.get("forbidden") != _expected_forbidden()
        or not isinstance(supplement, Mapping)
        or supplement.get("dataset") != DATASET
        or supplement.get("start_inclusive") != SUPPLEMENT_START
        or supplement.get("end_exclusive") != SUPPLEMENT_END
        or supplement.get("request_count") != SUPPLEMENT_REQUEST_COUNT
        or supplement.get("maximum_billable_bytes") != MAXIMUM_SUPPLEMENT_BYTES
        or supplement.get("maximum_staged_bytes") != MAXIMUM_SUPPLEMENT_BYTES
        or supplement.get("required_free_disk_bytes") != REQUIRED_SUPPLEMENT_FREE_BYTES
        or supplement.get("maximum_external_cost_usd") != "0"
        or not isinstance(quality, Mapping)
        or quality.get("maximum_batch_rows") != MAXIMUM_BATCH_ROWS
        or quality.get("maximum_workers") != MAXIMUM_WORKERS
        or quality.get("maximum_runtime_seconds") != MAXIMUM_RUNTIME_SECONDS
        or quality.get("maximum_output_bytes") != MAXIMUM_OUTPUT_BYTES
        or not binding_matches(root, Path(__file__), plan.get("implementation_sha256"))
        or plan.get("acquisition_plan_sha256") != sha256_file(root / ACQUISITION_PLAN_PATH)
        or plan.get("historical_policy_sha256") != sha256_file(root / HISTORICAL_POLICY_PATH)
        or plan.get("tbbo_registry_sha256") != sha256_file(root / TBBO_REGISTRY)
        or plan.get("status_session_diagnostic_sha256")
        != sha256_file(root / STATUS_SESSION_DIAGNOSTIC_PATH)
    ):
        raise IntegrityError("TBBO quality-certification plan drifted")
    if len(build_supplement_ledger(plan)) != SUPPLEMENT_REQUEST_COUNT:
        raise IntegrityError("TBBO supplement request count drifted")
    return plan


def build_supplement_ledger(
    plan: Mapping[str, object],
) -> tuple[SupplementRequest, ...]:
    supplement = plan.get("supplement")
    if not isinstance(supplement, Mapping):
        raise IntegrityError("TBBO supplement plan is invalid")
    requests: list[SupplementRequest] = []
    for market in MARKETS:
        for schema, stype_in, symbol in (
            ("definition", "parent", f"{market}.FUT"),
            ("status", "continuous", f"{market}.v.0"),
        ):
            contract = build_query_contract(
                schema=schema,
                market=market,
                start=SUPPLEMENT_START,
                end=SUPPLEMENT_END,
                stype_in=stype_in,
                symbols=[symbol],
            )
            request_id = sha256_json({"query_contract": contract, "stype_out": STYPE_OUT})
            requests.append(SupplementRequest(
                market=market,
                schema=schema,
                stype_in=stype_in,
                symbol=symbol,
                query_contract=contract,
                request_id=request_id,
            ))
    return tuple(requests)


def _iso_ns(value: str) -> int:
    return int(datetime.fromisoformat(value.removesuffix("Z") + "+00:00").timestamp()) * 1_000_000_000


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


def _metadata_mappings(metadata: object) -> object:
    return getattr(metadata, "mappings", None)


def _definition_mapping_id(metadata: object, *, symbol: str) -> str:
    mapping_symbols, canonical = _canonical_metadata_mappings(
        _metadata_mappings(metadata)
    )
    disposition = _symbology_resolution_disposition(
        query_stype_in="parent",
        query_symbols=[symbol],
        partial_symbols=getattr(metadata, "partial", ()),
        not_found_symbols=getattr(metadata, "not_found", ()),
        mapping_symbols=mapping_symbols,
    )
    if not canonical:
        raise IntegrityError("definition DBN header has no instrument mappings")
    return sha256_json({"disposition": disposition, "mappings": canonical})


def _mapping_intervals(
    metadata: object,
    *,
    symbol: str,
    start_date: str,
    end_date: str,
) -> tuple[tuple[str, str, int], ...]:
    if list(getattr(metadata, "partial", ())) or list(getattr(metadata, "not_found", ())):
        raise IntegrityError("DBN header contains partial or unresolved symbology")
    mappings = _metadata_mappings(metadata)
    if not isinstance(mappings, Mapping) or set(mappings) != {symbol}:
        raise IntegrityError("DBN header mapping identity differs")
    raw_intervals = mappings[symbol]
    if not isinstance(raw_intervals, Sequence) or isinstance(raw_intervals, (str, bytes)):
        raise IntegrityError("DBN header mapping intervals are invalid")
    requested_start = date.fromisoformat(start_date)
    requested_end = date.fromisoformat(end_date)
    intervals: list[tuple[str, str, int]] = []
    for raw in raw_intervals:
        if not isinstance(raw, Mapping):
            raise IntegrityError("DBN header mapping interval is invalid")
        try:
            raw_start = date.fromisoformat(str(raw["start_date"]))
            raw_end = date.fromisoformat(str(raw["end_date"]))
            instrument_id = int(raw["symbol"])
            if raw_start >= raw_end or instrument_id <= 0:
                raise ValueError
        except (KeyError, TypeError, ValueError) as exc:
            raise IntegrityError("DBN header mapping interval is invalid") from exc
        if raw_start < requested_end and raw_end > requested_start:
            intervals.append((
                max(raw_start, requested_start).isoformat(),
                min(raw_end, requested_end).isoformat(),
                instrument_id,
            ))
    if (
        not intervals
        or intervals != sorted(intervals)
        or intervals[0][0] != start_date
        or intervals[-1][1] != end_date
        or any(left[1] != right[0] for left, right in zip(intervals, intervals[1:]))
    ):
        raise IntegrityError("DBN header mapping intervals are not gapless")
    return tuple(intervals)


def validate_supplement_header(
    metadata: object, request: SupplementRequest,
) -> dict[str, object]:
    observed = _metadata_dict(metadata)
    expected = {
        "dataset": DATASET,
        "schema": request.schema,
        "stype_in": request.stype_in,
        "stype_out": STYPE_OUT,
        "start": _iso_ns(SUPPLEMENT_START),
        "end": _iso_ns(SUPPLEMENT_END),
        "symbols": [request.symbol],
        "ts_out": False,
        "limit": None,
    }
    if observed != expected or require_query_contract(request.query_contract) != request.query_contract:
        raise IntegrityError("supplement DBN header differs from its exact request")
    mapping_id = (
        _definition_mapping_id(metadata, symbol=request.symbol)
        if request.schema == "definition"
        else sha256_json(_mapping_intervals(
            metadata,
            symbol=request.symbol,
            start_date=SUPPLEMENT_START[:10],
            end_date=SUPPLEMENT_END[:10],
        ))
    )
    return {**observed, "mapping_id": mapping_id}


def _tbbo_registry(root: Path) -> dict[str, object]:
    registry = json_object(root / TBBO_REGISTRY, "TBBO custody registry")
    if (
        registry.get("snapshot_id") != TBBO_SNAPSHOT_ID
        or registry.get("state")
        != "SEALED_CANONICAL_CUSTODY_NOT_ROW_QUALITY_CERTIFIED_NOT_RESEARCH_ELIGIBLE"
        or registry.get("dbn_file_count") != 192
        or registry.get("sidecar_file_count") != 192
        or registry.get("dbn_rows_decoded") != 0
        or registry.get("row_quality_certified") is not False
        or registry.get("research_eligible") is not False
    ):
        raise IntegrityError("TBBO custody registry is not the required sealed snapshot")
    return registry


def verify_tbbo_custody(
    *, root: Path, header_loader: Callable[[Path], object] = load_header,
) -> dict[str, object]:
    registry = _tbbo_registry(root)
    acquisition_plan = load_acquisition_plan(root=root)
    ledger = build_request_ledger(acquisition_plan)
    snapshot = resolve_snapshot(root, str(registry["vault_snapshot_path"]))
    if snapshot != resolve_snapshot(root, VAULT_ROOT / TBBO_SNAPSHOT_ID):
        raise IntegrityError("TBBO vault path differs from its content address")
    raw_entries = registry.get("entries")
    if not isinstance(raw_entries, list) or len(raw_entries) != 192:
        raise IntegrityError("TBBO registry entries are invalid")
    entries_by_path = {
        str(item.get("dbn", {}).get("path")): item
        for item in raw_entries
        if isinstance(item, Mapping) and isinstance(item.get("dbn"), Mapping)
    }
    expected_files: set[str] = set()
    header_ids: list[dict[str, str]] = []
    for request in ledger:
        sidecar = verify_sidecar(
            plan=acquisition_plan,
            snapshot=snapshot,
            request=request,
            metadata_loader=header_loader,
        )
        entry = entries_by_path.get(request.snapshot_relative_path)
        if sidecar is None or not isinstance(entry, Mapping):
            raise IntegrityError("TBBO custody has a request gap")
        dbn = snapshot / request.snapshot_relative_path
        sidecar_path = Path(f"{dbn}.manifest.json")
        dbn_entry = entry.get("dbn")
        sidecar_entry = entry.get("sidecar")
        if (
            not isinstance(dbn_entry, Mapping)
            or not isinstance(sidecar_entry, Mapping)
            or dbn_entry.get("size") != io_path(dbn).stat().st_size
            or dbn_entry.get("sha256") != sha256_file(dbn)
            or sidecar_entry.get("size") != io_path(sidecar_path).stat().st_size
            or sidecar_entry.get("sha256") != sha256_file(sidecar_path)
            or entry.get("query") != request.as_dict()
        ):
            raise IntegrityError("TBBO registry entry differs from vault custody")
        metadata = header_loader(dbn)
        intervals = _mapping_intervals(
            metadata,
            symbol=f"{request.market}.v.0",
            start_date=request.start_date,
            end_date=request.end_date,
        )
        header_ids.append({
            "path": request.snapshot_relative_path,
            "mapping_id": sha256_json(intervals),
        })
        expected_files.update((
            request.snapshot_relative_path,
            sidecar_path.relative_to(snapshot).as_posix(),
        ))
    observed = {
        path.relative_to(snapshot).as_posix()
        for path in plain_tree_files(snapshot, reject_hardlinks=True)
    }
    if observed != expected_files:
        raise IntegrityError("TBBO vault inventory is not exactly 192 DBNs and sidecars")
    return {
        "snapshot_id": TBBO_SNAPSHOT_ID,
        "registry_path": TBBO_REGISTRY.as_posix(),
        "registry_sha256": sha256_file(root / TBBO_REGISTRY),
        "vault_snapshot_path": snapshot.relative_to(root).as_posix(),
        "request_set_id": registry["request_set_id"],
        "dbn_file_count": 192,
        "sidecar_file_count": 192,
        "total_payload_bytes": registry["total_payload_bytes"],
        "header_set_id": sha256_json(header_ids),
        "dbn_rows_decoded": 0,
    }


def _active_support_bindings(
    *, root: Path, header_loader: Callable[[Path], object] = load_header,
) -> list[dict[str, object]]:
    bindings: list[dict[str, object]] = []
    for relative in SUPPORT_ACTIVE_PATHS:
        path = root / relative
        sidecar = Path(f"{path}.manifest.json")
        if (
            not io_path(path).is_file()
            or is_linklike(path)
            or not io_path(sidecar).is_file()
            or is_linklike(sidecar)
        ):
            raise IntegrityError("active definition/status support custody is incomplete")
        metadata = header_loader(path)
        market = relative.parts[-3]
        schema = relative.parts[-4]
        start, end = relative.name.removesuffix(".dbn.zst").split("_")
        symbol = f"{market}.FUT" if schema == "definition" else f"{market}.v.0"
        stype = "parent" if schema == "definition" else "continuous"
        expected = {
            "dataset": DATASET,
            "schema": schema,
            "stype_in": stype,
            "stype_out": STYPE_OUT,
            "start": _iso_ns(f"{start}T00:00:00Z"),
            "end": _iso_ns(f"{end}T00:00:00Z"),
            "symbols": [symbol],
            "ts_out": False,
            "limit": None,
        }
        if _metadata_dict(metadata) != expected:
            raise IntegrityError("active definition/status DBN header differs")
        mapping_id = (
            _definition_mapping_id(metadata, symbol=symbol)
            if schema == "definition"
            else sha256_json(_mapping_intervals(
                metadata, symbol=symbol, start_date=start, end_date=end,
            ))
        )
        bindings.append({
            "path": relative.as_posix(),
            "sha256": sha256_file(path),
            "size": io_path(path).stat().st_size,
            "sidecar_path": sidecar.relative_to(root).as_posix(),
            "sidecar_sha256": sha256_file(sidecar),
            "schema": schema,
            "market": market,
            "mapping_id": mapping_id,
            "custody_class": (
                "active_canonical"
                if relative.parts[0] == "data"
                else "retired_shard_archive"
            ),
        })
    return bindings


def _supplement_registry(
    *, root: Path, snapshot_id: str, header_loader: Callable[[Path], object] = load_header,
) -> dict[str, object]:
    path = root / SUPPLEMENT_REGISTRY_ROOT / snapshot_id / "snapshot.json"
    registry = json_object(path, "TBBO definition/status supplement registry")
    if (
        registry.get("snapshot_id") != snapshot_id
        or registry.get("state") != "SEALED_DEFINITION_STATUS_SUPPLEMENT_NOT_ROW_CERTIFIED"
        or registry.get("dbn_file_count") != SUPPLEMENT_REQUEST_COUNT
        or registry.get("sidecar_file_count") != SUPPLEMENT_REQUEST_COUNT
        or registry.get("dbn_rows_decoded") != 0
    ):
        raise IntegrityError("TBBO definition/status supplement registry is invalid")
    plan = load_plan(root=root)
    ledger = build_supplement_ledger(plan)
    snapshot = resolve_snapshot(root, VAULT_ROOT / snapshot_id)
    entries = registry.get("entries")
    if not isinstance(entries, list) or len(entries) != SUPPLEMENT_REQUEST_COUNT:
        raise IntegrityError("TBBO supplement inventory is invalid")
    by_path = {
        str(item.get("dbn", {}).get("path")): item
        for item in entries
        if isinstance(item, Mapping) and isinstance(item.get("dbn"), Mapping)
    }
    expected: set[str] = set()
    for request in ledger:
        entry = by_path.get(request.snapshot_relative_path)
        dbn = snapshot / request.snapshot_relative_path
        sidecar = Path(f"{dbn}.manifest.json")
        if (
            not isinstance(entry, Mapping)
            or not isinstance(entry.get("dbn"), Mapping)
            or not isinstance(entry.get("sidecar"), Mapping)
            or entry["dbn"].get("size") != io_path(dbn).stat().st_size
            or entry["dbn"].get("sha256") != sha256_file(dbn)
            or entry["sidecar"].get("size") != io_path(sidecar).stat().st_size
            or entry["sidecar"].get("sha256") != sha256_file(sidecar)
            or entry.get("request") != request.as_dict()
        ):
            raise IntegrityError("TBBO supplement has a request gap")
        manifest = json_object(sidecar, "TBBO supplement sidecar")
        core = dict(manifest)
        manifest_id = core.pop("manifest_id", None)
        if (
            manifest_id != sha256_json(core)
            or manifest.get("request") != request.as_dict()
            or manifest.get("file_size_bytes") != io_path(dbn).stat().st_size
            or manifest.get("file_sha256") != sha256_file(dbn)
            or manifest.get("dbn_rows_decoded") != 0
            or manifest.get("dbn_header") != validate_supplement_header(
                header_loader(dbn), request,
            )
        ):
            raise IntegrityError("TBBO supplement sidecar differs")
        expected.update((
            request.snapshot_relative_path,
            sidecar.relative_to(snapshot).as_posix(),
        ))
    observed = {
        item.relative_to(snapshot).as_posix()
        for item in plain_tree_files(snapshot, reject_hardlinks=True)
    }
    if observed != expected:
        raise IntegrityError("TBBO supplement vault inventory differs")
    return {
        "snapshot_id": snapshot_id,
        "registry_path": path.relative_to(root).as_posix(),
        "registry_sha256": sha256_file(path),
        "vault_snapshot_path": snapshot.relative_to(root).as_posix(),
        "provider_conditions": registry["provider_conditions"],
        "entries": entries,
        "dbn_rows_decoded": 0,
    }


def prepare_quality_certification(
    *,
    root: Path,
    supplement_snapshot_id: str | None = None,
    header_loader: Callable[[Path], object] = load_header,
) -> dict[str, object]:
    """Bind immutable custody using only hashes, sidecars, and DBN headers."""

    root = root.resolve(strict=True)
    plan = load_plan(root=root)
    custody = verify_tbbo_custody(root=root, header_loader=header_loader)
    active_support = _active_support_bindings(root=root, header_loader=header_loader)
    active_baseline = protected_active_state(root)
    supplement = (
        {"state": "REQUIRED_NOT_PRESENT", "snapshot_id": None, "dbn_rows_decoded": 0}
        if supplement_snapshot_id is None
        else {
            "state": "SEALED_HEADER_VERIFIED",
            **_supplement_registry(
                root=root,
                snapshot_id=supplement_snapshot_id,
                header_loader=header_loader,
            ),
        }
    )
    state = (
        "PREPARED_REQUIRES_DEFINITION_STATUS_SUPPLEMENT"
        if supplement_snapshot_id is None
        else "PREPARED_REQUIRES_SEPARATE_PROTECTED_ROW_SCAN_APPROVAL"
    )
    ledger = build_supplement_ledger(plan)
    core: dict[str, object] = {
        "schema_version": "tbbo_quality_preparation/1.0.0",
        "state": state,
        "operation": PREPARE_OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "implementation_sha256": plan["implementation_sha256"],
        "tbbo_custody": custody,
        "active_definition_status_bindings": active_support,
        "active_definition_status_binding_id": sha256_json(active_support),
        "supplement_request_set_id": sha256_json([item.as_dict() for item in ledger]),
        "supplement": supplement,
        "degraded_utc_dates": list(DEGRADED_DATES),
        "boundary_sessions": list(BOUNDARY_SESSIONS),
        "market_quarantined_utc_dates": {
            key: list(value) for key, value in MARKET_QUARANTINED_DATES.items()
        },
        "market_quarantine_dependency_policy": MARKET_QUARANTINE_DEPENDENCY_POLICY,
        "status_utc_midnight_snapshot_policy": STATUS_MIDNIGHT_SNAPSHOT_POLICY,
        "status_only_session_policy": STATUS_ONLY_SESSION_POLICY,
        "status_session_diagnostic": {
            "path": STATUS_SESSION_DIAGNOSTIC_PATH.as_posix(),
            "sha256": sha256_file(root / STATUS_SESSION_DIAGNOSTIC_PATH),
        },
        "identical_tbbo_row_policy": IDENTICAL_TBBO_ROW_POLICY,
        "historical_observability_policy": {
            "path": HISTORICAL_POLICY_PATH.as_posix(),
            "sha256": sha256_file(root / HISTORICAL_POLICY_PATH),
            "claim": "NOT_OFFICIAL_HISTORICAL_CME_SESSION_AUTHORITY",
        },
        "protected_active_baseline": active_baseline,
        "provider_calls": 0,
        "dbn_rows_decoded": 0,
        "decoded_datasets_created": 0,
        "active_catalog_mutated": False,
        "research_authorized": False,
    }
    preparation_id = sha256_json(core)
    preparation = {
        **core,
        "preparation_id": preparation_id,
        "preparation_path": (
            PREPARATION_ROOT / preparation_id / "preparation.json"
        ).as_posix(),
    }
    write_or_verify(
        root / str(preparation["preparation_path"]),
        preparation,
        "TBBO quality preparation",
    )
    return preparation


def build_file_supplement_provider_apis(
    *, root: Path, historical_factory: Callable[..., object] | None = None,
) -> SupplementProviderApis:
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
        "get_dataset_condition": getattr(metadata, "get_dataset_condition", None),
        "get_dataset_range": getattr(metadata, "get_dataset_range", None),
        "resolve": getattr(symbology, "resolve", None),
        "get_cost": getattr(metadata, "get_cost", None),
        "get_billable_size": getattr(metadata, "get_billable_size", None),
        "get_range": getattr(timeseries, "get_range", None),
    }
    if not all(callable(value) for value in methods.values()):
        raise IntegrityError("Databento client lacks supplement APIs")
    return SupplementProviderApis(**methods)  # type: ignore[arg-type]


def _provider_datetime(value: object, label: str) -> datetime:
    if type(value) is not str:
        raise IntegrityError(f"provider {label} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IntegrityError(f"provider {label} is invalid") from exc
    if parsed.tzinfo is None:
        raise IntegrityError(f"provider {label} is not timezone-aware")
    return parsed.astimezone(timezone.utc)


def _call_with_retry(
    method: Callable[..., object],
    *,
    kwargs: Mapping[str, object],
    deadline: float,
    clock: Callable[[], float],
    sleeper: Callable[[float], None],
) -> object:
    for retry in range(MAXIMUM_RETRIES + 1):
        try:
            return _provider_call(method, deadline=deadline, clock=clock, kwargs=kwargs)
        except Exception as exc:
            if retry >= MAXIMUM_RETRIES or not _retryable_provider_failure(exc):
                raise
            delay = _retry_after(exc, retry + 1)
            if clock() + delay >= deadline:
                raise UnauthorizedOperation("supplement runtime cannot accommodate retry") from exc
            sleeper(delay)
    raise AssertionError("unreachable")


def _retryable_provider_failure(exc: BaseException) -> bool:
    return _transient(exc) or (
        type(exc).__name__ == "BentoError"
        and str(exc).startswith("Error streaming response:")
    )


def _validate_supplement_entitlement(value: object) -> None:
    if not isinstance(value, Mapping):
        raise IntegrityError("provider dataset range is invalid")
    ranges = value.get("schema")
    for schema in ("definition", "status"):
        selected = ranges.get(schema) if isinstance(ranges, Mapping) else value
        if not isinstance(selected, Mapping):
            raise UnauthorizedOperation(f"provider {schema} entitlement range is absent")
        if (
            _provider_datetime(selected.get("start"), "range start")
            > _provider_datetime(SUPPLEMENT_START, "request start")
            or _provider_datetime(selected.get("end"), "range end")
            < _provider_datetime(SUPPLEMENT_END, "request end")
        ):
            raise UnauthorizedOperation("provider entitlement does not cover supplement")


def _validate_resolve_response(
    value: object, *, stype_in: str, symbols: Sequence[str], parent: bool,
) -> None:
    if not isinstance(value, Mapping):
        raise IntegrityError("provider symbology response is invalid")
    partial = value.get("partial") or []
    not_found = value.get("not_found") or []
    result = value.get("result")
    if (
        value.get("stype_in") != stype_in
        or value.get("stype_out") != STYPE_OUT
        or value.get("start_date") != SUPPLEMENT_START[:10]
        or value.get("end_date") != SUPPLEMENT_END[:10]
        or not isinstance(partial, Sequence)
        or isinstance(partial, (str, bytes))
        or not isinstance(not_found, Sequence)
        or isinstance(not_found, (str, bytes))
        or set(symbols) & (set(str(item) for item in partial) | set(str(item) for item in not_found))
        or not isinstance(result, Mapping)
        or not result
    ):
        raise UnauthorizedOperation("supplement symbology is partial or unresolved")
    if parent:
        for raw_intervals in result.values():
            if not isinstance(raw_intervals, list) or not raw_intervals:
                raise UnauthorizedOperation("parent symbology has an empty mapping")
            for raw in raw_intervals:
                if not isinstance(raw, Mapping) or not {"d0", "d1", "s"} <= set(raw):
                    raise IntegrityError("parent symbology interval is malformed")
        return
    if set(result) != set(symbols):
        raise UnauthorizedOperation("continuous symbology scope differs")
    requested_start = date.fromisoformat(SUPPLEMENT_START[:10])
    requested_end = date.fromisoformat(SUPPLEMENT_END[:10])
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
            if left < requested_end and right > requested_start:
                intervals.append((max(left, requested_start), min(right, requested_end)))
        cursor = requested_start
        for left, right in sorted(intervals):
            if left > cursor:
                raise UnauthorizedOperation("continuous symbology contains a gap")
            cursor = max(cursor, right)
        if cursor < requested_end:
            raise UnauthorizedOperation("continuous symbology does not cover supplement")


def _zero_cost(value: object) -> None:
    if isinstance(value, bool):
        raise IntegrityError("provider cost response is invalid")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError("provider cost response is invalid") from exc
    if not amount.is_finite() or amount != 0:
        raise UnauthorizedOperation("fresh provider cost is not exactly zero")


def _plain_provider(value: object) -> object:
    if value is None or type(value) in {bool, int, float, str}:
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _plain_provider(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_plain_provider(item) for item in value]
    return str(value)


def _condition_date(item: Mapping[str, object]) -> str | None:
    preferred = [item.get(key) for key in ("date", "start_date", "day")]
    values = preferred + list(item.values())
    for value in values:
        if type(value) is str:
            try:
                return date.fromisoformat(value[:10]).isoformat()
            except ValueError:
                continue
    return None


def normalize_provider_conditions(value: object) -> dict[str, object]:
    if not isinstance(value, list):
        raise IntegrityError("provider dataset conditions are invalid")
    raw = _plain_provider(value)
    degraded = set(DEGRADED_DATES)
    missing_or_pending: set[str] = set()
    for item in value:
        if not isinstance(item, Mapping):
            raise IntegrityError("provider dataset condition row is invalid")
        statuses = {
            str(field).casefold()
            for field in item.values()
            if str(field).casefold() in {"degraded", "missing", "pending"}
        }
        if not statuses:
            continue
        condition_date = _condition_date(item)
        if condition_date is None:
            raise IntegrityError("provider condition lacks an exact date")
        if "degraded" in statuses:
            degraded.add(condition_date)
        if statuses & {"missing", "pending"}:
            missing_or_pending.add(condition_date)
    if missing_or_pending:
        raise UnauthorizedOperation("provider reports missing or pending supplement dates")
    core = {
        "dataset": DATASET,
        "known_degraded_dates_are_a_floor": True,
        "degraded_utc_dates": sorted(degraded),
        "missing_or_pending_dates": [],
        "raw_response": raw,
    }
    return {**core, "provider_conditions_id": sha256_json(core)}


def preflight_supplement(
    *,
    plan: Mapping[str, object],
    ledger: Sequence[SupplementRequest],
    apis: SupplementProviderApis,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    del plan
    deadline = clock() + MAXIMUM_RUNTIME_SECONDS if deadline is None else deadline
    _validate_supplement_entitlement(_call_with_retry(
        apis.get_dataset_range,
        kwargs={"dataset": DATASET},
        deadline=deadline,
        clock=clock,
        sleeper=sleeper,
    ))
    for stype, symbols, parent in (
        ("parent", [f"{market}.FUT" for market in MARKETS], True),
        ("continuous", [f"{market}.v.0" for market in MARKETS], False),
    ):
        _validate_resolve_response(
            _call_with_retry(
                apis.resolve,
                kwargs={
                    "dataset": DATASET,
                    "symbols": symbols,
                    "stype_in": stype,
                    "stype_out": STYPE_OUT,
                    "start_date": SUPPLEMENT_START[:10],
                    "end_date": SUPPLEMENT_END[:10],
                },
                deadline=deadline,
                clock=clock,
                sleeper=sleeper,
            ),
            stype_in=stype,
            symbols=symbols,
            parent=parent,
        )
    conditions = normalize_provider_conditions(_call_with_retry(
        apis.get_dataset_condition,
        kwargs={
            "dataset": DATASET,
            "start_date": "2025-09-01",
            "end_date": "2026-08-31",
        },
        deadline=deadline,
        clock=clock,
        sleeper=sleeper,
    ))
    sizes: dict[str, int] = {}
    for request in ledger:
        query = request.provider_query()
        _zero_cost(_call_with_retry(
            apis.get_cost,
            kwargs=query,
            deadline=deadline,
            clock=clock,
            sleeper=sleeper,
        ))
        size = _call_with_retry(
            apis.get_billable_size,
            kwargs=query,
            deadline=deadline,
            clock=clock,
            sleeper=sleeper,
        )
        if type(size) is not int or size <= 0:
            raise UnauthorizedOperation("every supplement request must have positive billable size")
        sizes[request.request_id] = size
    total = sum(sizes.values())
    if len(sizes) != SUPPLEMENT_REQUEST_COUNT or total > MAXIMUM_SUPPLEMENT_BYTES:
        raise UnauthorizedOperation("supplement billable ceiling failed")
    core = {
        "schema_version": "tbbo_definition_status_supplement_preflight/1.0.0",
        "state": "PASS_ZERO_COST_COMPLETE_MAPPING",
        "billable_bytes_by_request": sizes,
        "total_billable_bytes": total,
        "provider_conditions": conditions,
        "external_cost_usd": "0",
    }
    return {**core, "preflight_id": sha256_json(core)}


def required_supplement_scope(
    *, root: Path, plan: Mapping[str, object],
) -> dict[str, str]:
    ledger = build_supplement_ledger(plan)
    return {
        "dataset": DATASET,
        "schemas": "definition,status",
        "markets": ",".join(MARKETS),
        "period": f"{SUPPLEMENT_START}/{SUPPLEMENT_END}",
        "request_count": str(SUPPLEMENT_REQUEST_COUNT),
        "request_set_id": sha256_json([item.as_dict() for item in ledger]),
        "maximum_external_cost_usd": "0",
        "maximum_billable_bytes": str(MAXIMUM_SUPPLEMENT_BYTES),
        "maximum_staged_bytes": str(MAXIMUM_SUPPLEMENT_BYTES),
        "row_decode": "false",
        "vault_publication": "true",
        "active_source_mutation": "false",
        "implementation_sha256": str(plan["implementation_sha256"]),
        "approval_command": SUPPLEMENT_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _supplement_paths(
    snapshot: Path, request: SupplementRequest,
) -> tuple[Path, Path]:
    dbn = snapshot / request.snapshot_relative_path
    return dbn, Path(f"{dbn}.manifest.json")


def _supplement_sidecar(
    *,
    plan: Mapping[str, object],
    request: SupplementRequest,
    dbn: Path,
    billable_bytes: int,
    metadata: object,
) -> dict[str, object]:
    core = {
        "schema_version": "tbbo_definition_status_supplement_sidecar/1.0.0",
        "state": "SEALED_SUPPLEMENT_CUSTODY_NOT_ROW_CERTIFIED",
        "plan_id": plan["plan_id"],
        "request": request.as_dict(),
        "billable_bytes_at_download": billable_bytes,
        "file_size_bytes": io_path(dbn).stat().st_size,
        "file_sha256": sha256_file(dbn),
        "dbn_header": validate_supplement_header(metadata, request),
        "dbn_rows_decoded": 0,
        "research_source_authorized": False,
    }
    return {**core, "manifest_id": sha256_json(core)}


def _download_supplement_request(
    *,
    method: Callable[..., object],
    query: Mapping[str, object],
    final: Path,
    attempt: Path,
    deadline: float,
    clock: Callable[[], float],
    sleeper: Callable[[float], None],
) -> tuple[object, Path]:
    for retry in range(MAXIMUM_RETRIES + 1):
        partial = final.with_name(f"{final.name}.partial.{uuid.uuid4().hex[:16]}")
        try:
            store = _provider_call(
                method,
                deadline=deadline,
                clock=clock,
                kwargs={**query, "path": str(io_path(partial))},
            )
            if not io_path(partial).is_file() or io_path(partial).stat().st_size <= 0:
                raise IntegrityError("provider did not create a nonempty supplement DBN")
            if _tree_bytes(attempt) > MAXIMUM_SUPPLEMENT_BYTES:
                raise UnauthorizedOperation("supplement staging ceiling exceeded")
            return store, partial
        except Exception as exc:
            if io_path(partial).is_file():
                failed = attempt / "failed_partials" / partial.name
                io_path(failed.parent).mkdir(parents=True, exist_ok=True)
                os.replace(io_path(partial), io_path(failed))
            if _tree_bytes(attempt) > MAXIMUM_SUPPLEMENT_BYTES:
                raise UnauthorizedOperation("supplement staging ceiling exceeded") from exc
            if retry >= MAXIMUM_RETRIES or not _retryable_provider_failure(exc):
                raise
            delay = _retry_after(exc, retry + 1)
            if clock() + delay >= deadline:
                raise UnauthorizedOperation(
                    "supplement runtime cannot accommodate retry"
                ) from exc
            sleeper(delay)
    raise AssertionError("unreachable")


def _tree_bytes(path: Path) -> int:
    if not io_path(path).exists():
        return 0
    return sum(
        io_path(item).stat().st_size
        for item in plain_tree_files(path, reject_hardlinks=False)
    )


def _same_volume(source: Path, destination_parent: Path) -> None:
    if io_path(source).stat().st_dev != io_path(destination_parent).stat().st_dev:
        raise IntegrityError("supplement publication requires one volume")


def execute_authorized_supplement(
    *,
    root: Path,
    authorization: OperationReceipt,
    provider_factory: Callable[[], SupplementProviderApis] | None = None,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
    header_loader: Callable[[Path], object] = load_header,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    """Acquire and atomically seal the separate 32-file supplement."""

    root = root.resolve(strict=True)
    plan = load_plan(root=root)
    environment_check(root)
    free = getattr(disk_usage(root), "free", None)
    if type(free) is not int or free < REQUIRED_SUPPLEMENT_FREE_BYTES:
        raise UnauthorizedOperation("insufficient free space for supplement")
    boundary = RepoBoundary(root)
    scope = required_supplement_scope(root=root, plan=plan)
    attempt = root / SUPPLEMENT_STAGING_ROOT / authorization.receipt_id
    snapshot = attempt / "snapshot"
    lock = SingleInstance(root / SUPPLEMENT_LOCK)
    if not lock.acquire():
        raise UnauthorizedOperation("another supplement acquisition is running")
    moved_to: Path | None = None
    try:
        use_path = root / "state/authorization_uses" / f"{authorization.receipt_id}.json"
        if use_path.exists():
            authorization.assert_consumed(
                boundary,
                operation=SUPPLEMENT_OPERATION,
                classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
                required_scope=scope,
            )
        else:
            authorization.consume(
                boundary,
                operation=SUPPLEMENT_OPERATION,
                classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
                required_scope=scope,
            )
        attempt.mkdir(parents=True, exist_ok=True)
        active_baseline = protected_active_state(root)
        ledger = build_supplement_ledger(plan)
        deadline = clock() + MAXIMUM_RUNTIME_SECONDS
        factory = provider_factory or (
            lambda: build_file_supplement_provider_apis(root=root)
        )
        apis = factory()
        preflight = preflight_supplement(
            plan=plan,
            ledger=ledger,
            apis=apis,
            deadline=deadline,
            clock=clock,
            sleeper=sleeper,
        )
        write_or_verify(attempt / "preflight.json", preflight, "supplement preflight")
        for request in ledger:
            final, sidecar_path = _supplement_paths(snapshot, request)
            if io_path(final).exists() or io_path(sidecar_path).exists():
                manifest = json_object(sidecar_path, "supplement sidecar")
                core = dict(manifest)
                manifest_id = core.pop("manifest_id", None)
                if (
                    manifest_id != sha256_json(core)
                    or manifest.get("request") != request.as_dict()
                    or manifest.get("file_sha256") != sha256_file(final)
                    or manifest.get("dbn_header")
                    != validate_supplement_header(header_loader(final), request)
                ):
                    raise IntegrityError("existing supplement request differs")
                continue
            io_path(final.parent).mkdir(parents=True, exist_ok=True)
            try:
                store, partial = _download_supplement_request(
                    method=apis.get_range,
                    query=request.provider_query(output_identity=True),
                    final=final,
                    attempt=attempt,
                    deadline=deadline,
                    clock=clock,
                    sleeper=sleeper,
                )
                metadata = getattr(store, "metadata", None)
                validate_supplement_header(metadata, request)
                del store
                gc.collect()
                sidecar = _supplement_sidecar(
                    plan=plan,
                    request=request,
                    dbn=partial,
                    billable_bytes=int(preflight["billable_bytes_by_request"][request.request_id]),
                    metadata=metadata,
                )
                if _tree_bytes(attempt) > MAXIMUM_SUPPLEMENT_BYTES:
                    raise UnauthorizedOperation("supplement staging ceiling exceeded")
                os.replace(io_path(partial), io_path(final))
                try:
                    write_or_verify(sidecar_path, sidecar, "supplement sidecar")
                except Exception:
                    os.replace(io_path(final), io_path(partial))
                    raise
            except Exception:
                # Failed partials remain in staging for diagnosis and bounded resume.
                raise
        entries: list[dict[str, object]] = []
        expected: set[str] = set()
        for request in ledger:
            dbn, sidecar = _supplement_paths(snapshot, request)
            manifest = json_object(sidecar, "supplement sidecar")
            validate_supplement_header(header_loader(dbn), request)
            expected.update((
                request.snapshot_relative_path,
                sidecar.relative_to(snapshot).as_posix(),
            ))
            entries.append({
                "request": request.as_dict(),
                "dbn": {
                    "path": request.snapshot_relative_path,
                    "size": io_path(dbn).stat().st_size,
                    "sha256": sha256_file(dbn),
                },
                "sidecar": {
                    "path": sidecar.relative_to(snapshot).as_posix(),
                    "size": io_path(sidecar).stat().st_size,
                    "sha256": sha256_file(sidecar),
                },
                "manifest_id": manifest["manifest_id"],
            })
        observed = {
            item.relative_to(snapshot).as_posix()
            for item in plain_tree_files(snapshot, reject_hardlinks=True)
        }
        if observed != expected or _tree_bytes(attempt) > MAXIMUM_SUPPLEMENT_BYTES:
            raise IntegrityError("completed supplement staging inventory differs")
        snapshot_core = {
            "schema_version": "tbbo_definition_status_supplement_identity/1.0.0",
            "state": "SEALED_DEFINITION_STATUS_SUPPLEMENT_NOT_ROW_CERTIFIED",
            "plan_id": plan["plan_id"],
            "request_set_id": sha256_json([item.as_dict() for item in ledger]),
            "provider_conditions": preflight["provider_conditions"],
            "entries": entries,
            "dbn_file_count": SUPPLEMENT_REQUEST_COUNT,
            "sidecar_file_count": SUPPLEMENT_REQUEST_COUNT,
            "dbn_rows_decoded": 0,
            "row_quality_certified": False,
            "research_eligible": False,
            "protected_active_baseline": active_baseline,
        }
        snapshot_id = sha256_json(snapshot_core)
        destination = boundary.assert_snapshot_path(root / VAULT_ROOT / snapshot_id)
        registry_path = root / SUPPLEMENT_REGISTRY_ROOT / snapshot_id / "snapshot.json"
        if io_path(destination).exists() or io_path(registry_path).exists():
            raise IntegrityError("supplement publication destination already exists")
        io_path(destination.parent).mkdir(parents=True, exist_ok=True)
        _same_volume(snapshot, destination.parent)
        os.replace(io_path(snapshot), io_path(destination))
        moved_to = destination
        registry = {
            **snapshot_core,
            "snapshot_id": snapshot_id,
            "publication_authorization_receipt_id": authorization.receipt_id,
            "vault_snapshot_path": destination.relative_to(root).as_posix(),
            "registry_path": registry_path.relative_to(root).as_posix(),
        }
        try:
            if protected_active_state(root) != active_baseline:
                raise IntegrityError("protected active sources changed during supplement")
            write_or_verify(registry_path, registry, "supplement registry")
            _supplement_registry(
                root=root,
                snapshot_id=snapshot_id,
                header_loader=header_loader,
            )
        except Exception:
            if io_path(destination).exists() and not io_path(snapshot).exists():
                io_path(snapshot.parent).mkdir(parents=True, exist_ok=True)
                os.replace(io_path(destination), io_path(snapshot))
                moved_to = None
            raise
        return {
            "snapshot_id": snapshot_id,
            "state": registry["state"],
            "vault_snapshot_path": registry["vault_snapshot_path"],
            "registry_path": registry["registry_path"],
            "dbn_file_count": SUPPLEMENT_REQUEST_COUNT,
            "sidecar_file_count": SUPPLEMENT_REQUEST_COUNT,
            "dbn_rows_decoded": 0,
        }
    finally:
        unsafe = moved_to is not None and not io_path(
            root / SUPPLEMENT_REGISTRY_ROOT / moved_to.name / "snapshot.json"
        ).exists()
        lock.release()
        if unsafe:
            raise IntegrityError("supplement publication failed without rollback")


def _trade_session(timestamp_ns: int) -> str:
    seconds = timestamp_ns // 1_000_000_000
    local = datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone(
        ZoneInfo("America/Chicago")
    )
    trade_date = local.date() + (
        timedelta(days=1) if local.time() >= wall_time(17) else timedelta()
    )
    return trade_date.isoformat()


def _excluded_sessions(degraded_dates: Sequence[str]) -> frozenset[str]:
    sessions = set(BOUNDARY_SESSIONS)
    for text in degraded_dates:
        value = date.fromisoformat(text)
        start = datetime.combine(value, wall_time.min, timezone.utc)
        end = start + timedelta(days=1) - timedelta(microseconds=1)
        sessions.add(_trade_session(int(start.timestamp()) * 1_000_000_000))
        sessions.add(_trade_session(int(end.timestamp()) * 1_000_000_000))
    return frozenset(sessions)


def _market_quarantined_dates(
    market: str, degraded_dates: Sequence[str],
) -> tuple[str, ...]:
    return tuple(sorted(set(degraded_dates) | set(MARKET_QUARANTINED_DATES.get(market, ()))))


def _mapping_for_date(
    intervals: Sequence[tuple[str, str, int]], utc_date: str,
) -> int | None:
    for start, end, instrument_id in intervals:
        if start <= utc_date < end:
            return instrument_id
    return None


def _default_chunk_loader(path: Path) -> Iterable[np.ndarray]:
    if databento.__version__ != SUPPORTED_DATABENTO_VERSION:
        raise ContractError("quality scan requires the pinned offline Databento decoder")
    store = databento.DBNStore.from_file(io_path(path))
    decoded = store.to_ndarray(count=MAXIMUM_BATCH_ROWS)
    chunks = (decoded,) if isinstance(decoded, np.ndarray) else decoded
    try:
        yield from chunks
    except (NotImplementedError, TypeError, ValueError) as exc:
        raise IntegrityError("offline DBN decoding failed") from exc
    finally:
        del decoded
        del store
        gc.collect()


def _verify_chunk(
    chunk: object,
    expected: set[tuple[str, ...]],
    label: str,
    *,
    exact_dtype: np.dtype[object] | None = None,
) -> np.ndarray:
    if (
        not isinstance(chunk, np.ndarray)
        or chunk.dtype.names not in expected
        or (exact_dtype is not None and chunk.dtype != exact_dtype)
    ):
        raise IntegrityError(f"decoded {label} dtype differs from the pinned schema")
    return chunk


def _source_order(
    chunk: np.ndarray, *, prior: int | None, label: str,
) -> int | None:
    if not len(chunk):
        return prior
    times = chunk["ts_recv"]
    if (
        np.any(times == 0)
        or np.any(times == UINT64_NULL)
        or (prior is not None and int(times[0]) < prior)
        or np.any(times[1:] < times[:-1])
    ):
        raise IntegrityError(f"decoded {label} is not receive-time ordered")
    return int(times[-1])


def _char(value: object) -> str:
    if isinstance(value, np.bytes_):
        value = bytes(value)
    if isinstance(value, bytes):
        return value.rstrip(b"\x00").decode("ascii", errors="strict")
    return str(value)


def _bound_file(
    path: Path,
    *,
    expected_sha256: str,
    chunk_loader: Callable[[Path], Iterable[np.ndarray]],
) -> Iterable[np.ndarray]:
    if sha256_file(path) != expected_sha256:
        raise IntegrityError("quality source hash differs before decode")
    try:
        yield from chunk_loader(path)
    finally:
        if sha256_file(path) != expected_sha256:
            raise IntegrityError("quality source hash differs after decode")


def _scan_definitions(
    sources: Sequence[tuple[Path, str]],
    *,
    chunk_loader: Callable[[Path], Iterable[np.ndarray]],
    deadline: float,
    clock: Callable[[], float],
) -> dict[int, list[tuple[int, int, int]]]:
    definitions: dict[int, list[tuple[int, int, int]]] = {}
    prior: int | None = None
    for path, expected_sha in sources:
        for raw_chunk in _bound_file(
            path, expected_sha256=expected_sha, chunk_loader=chunk_loader,
        ):
            if clock() > deadline:
                raise UnauthorizedOperation("quality scan runtime exhausted")
            chunk = _verify_chunk(
                raw_chunk,
                {DEFINITION_DTYPE_V1, DEFINITION_DTYPE_V3},
                "definition",
            )
            prior = _source_order(chunk, prior=prior, label="definition")
            if len(chunk) and (
                np.any(chunk["ts_event"] == 0)
                or np.any(chunk["ts_event"] == UINT64_NULL)
            ):
                raise IntegrityError("definition contains an undefined event timestamp")
            for row in chunk:
                instrument_id = int(row["instrument_id"])
                if instrument_id <= 0:
                    raise IntegrityError("definition contains an invalid instrument ID")
                definitions.setdefault(instrument_id, []).append((
                    int(row["ts_recv"]),
                    int(row["activation"]),
                    int(row["expiration"]),
                ))
    for values in definitions.values():
        values.sort()
    return definitions


def _continuous_intervals(
    *, path: Path, market: str, header_loader: Callable[[Path], object],
) -> tuple[tuple[str, str, int], ...]:
    start, end = path.name.removesuffix(".dbn.zst").split("_")
    return _mapping_intervals(
        header_loader(path),
        symbol=f"{market}.v.0",
        start_date=start,
        end_date=end,
    )


def _scan_status(
    sources: Sequence[tuple[Path, str]],
    *,
    market: str,
    degraded_dates: Sequence[str],
    chunk_loader: Callable[[Path], Iterable[np.ndarray]],
    header_loader: Callable[[Path], object],
    deadline: float,
    clock: Callable[[], float],
) -> tuple[set[str], dict[int, tuple[int, int]], tuple[tuple[str, str, int], ...]]:
    excluded = _excluded_sessions(degraded_dates)
    sessions: set[str] = set()
    first_use: dict[int, tuple[int, int]] = {}
    all_intervals: list[tuple[str, str, int]] = []
    prior: int | None = None
    tbbo_start_ns = _iso_ns(TBBO_START)
    tbbo_end_ns = _iso_ns(TBBO_END)
    for path, expected_sha in sources:
        intervals = _continuous_intervals(
            path=path, market=market, header_loader=header_loader,
        )
        all_intervals.extend(intervals)
        for raw_chunk in _bound_file(
            path, expected_sha256=expected_sha, chunk_loader=chunk_loader,
        ):
            if clock() > deadline:
                raise UnauthorizedOperation("quality scan runtime exhausted")
            chunk = _verify_chunk(raw_chunk, {STATUS_DTYPE}, "status")
            prior = _source_order(chunk, prior=prior, label="status")
            if len(chunk) and (
                np.any(chunk["ts_event"] == 0)
                or np.any(chunk["ts_event"] == UINT64_NULL)
            ):
                raise IntegrityError("status contains an undefined event timestamp")
            for row in chunk:
                received = int(row["ts_recv"])
                event = int(row["ts_event"])
                instrument_id = int(row["instrument_id"])
                utc_date = datetime.fromtimestamp(
                    received // 1_000_000_000, tz=timezone.utc,
                ).date().isoformat()
                expected_id = _mapping_for_date(intervals, utc_date)
                session = _trade_session(received)
                eligible = (
                    tbbo_start_ns <= received < tbbo_end_ns
                    and session not in excluded
                )
                carry_forward = event < received - received % UTC_DAY_NS
                if expected_id != instrument_id and eligible:
                    raise IntegrityError("eligible status instrument mapping differs")
                if eligible:
                    previous = first_use.get(instrument_id)
                    candidate = (received, event)
                    if previous is None or candidate < previous:
                        first_use[instrument_id] = candidate
                    if not carry_forward and (
                        _char(row["is_trading"]) == "Y"
                        or _char(row["is_quoting"]) == "Y"
                    ):
                        sessions.add(session)
    return sessions, first_use, tuple(sorted(all_intervals))


@dataclass
class _TbboScanState:
    prior_ts_recv: int | None = None
    duplicate_timestamp: int | None = None
    raw_hashes_at_timestamp: set[bytes] | None = None
    total_rows: int = 0
    eligible_rows: int = 0
    quarantined_rows: int = 0
    duplicate_rows: int = 0
    quarantined_duplicate_rows: int = 0
    bad_flag_rows: int = 0
    quarantined_bad_flag_rows: int = 0
    mapping_mismatches: int = 0
    quarantined_mapping_mismatches: int = 0
    duplicate_dates: dict[str, int] | None = None
    bad_flag_dates: dict[str, int] | None = None
    mapping_mismatch_dates: dict[str, int] | None = None
    sessions: set[str] | None = None
    first_use: dict[int, tuple[int, int]] | None = None

    def __post_init__(self) -> None:
        if self.raw_hashes_at_timestamp is None:
            self.raw_hashes_at_timestamp = set()
        if self.sessions is None:
            self.sessions = set()
        if self.first_use is None:
            self.first_use = {}
        if self.duplicate_dates is None:
            self.duplicate_dates = {}
        if self.bad_flag_dates is None:
            self.bad_flag_dates = {}
        if self.mapping_mismatch_dates is None:
            self.mapping_mismatch_dates = {}


def _scan_tbbo_chunks(
    chunks: Iterable[np.ndarray],
    *,
    state: _TbboScanState,
    start_ns: int,
    end_ns: int,
    tbbo_intervals: Sequence[tuple[str, str, int]],
    status_intervals: Sequence[tuple[str, str, int]],
    degraded_dates: Sequence[str],
    deadline: float,
    clock: Callable[[], float],
) -> int:
    excluded = _excluded_sessions(degraded_dates)
    file_rows = 0
    assert state.raw_hashes_at_timestamp is not None
    assert state.sessions is not None
    assert state.first_use is not None
    assert state.duplicate_dates is not None
    assert state.bad_flag_dates is not None
    assert state.mapping_mismatch_dates is not None
    for raw_chunk in chunks:
        if clock() > deadline:
            raise UnauthorizedOperation("quality scan runtime exhausted")
        chunk = _verify_chunk(
            raw_chunk,
            {TBBO_DTYPE},
            "TBBO",
            exact_dtype=TBBO_NUMPY_DTYPE,
        )
        if len(chunk) and (
            np.any(chunk["ts_event"] == 0)
            or np.any(chunk["ts_event"] == UINT64_NULL)
            or np.any(chunk["ts_recv"] == 0)
            or np.any(chunk["ts_recv"] == UINT64_NULL)
            or np.any(chunk["ts_recv"] < start_ns)
            or np.any(chunk["ts_recv"] >= end_ns)
            or (
                state.prior_ts_recv is not None
                and int(chunk["ts_recv"][0]) < state.prior_ts_recv
            )
            or np.any(chunk["ts_recv"][1:] < chunk["ts_recv"][:-1])
        ):
            raise IntegrityError("TBBO timestamp or monthly interval integrity failed")
        for row in chunk:
            received = int(row["ts_recv"])
            event = int(row["ts_event"])
            instrument_id = int(row["instrument_id"])
            if instrument_id <= 0:
                raise IntegrityError("TBBO contains an invalid instrument ID")
            utc_date = datetime.fromtimestamp(
                received // 1_000_000_000, tz=timezone.utc,
            ).date().isoformat()
            session = _trade_session(received)
            eligible = session not in excluded
            if state.duplicate_timestamp != received:
                state.duplicate_timestamp = received
                state.raw_hashes_at_timestamp.clear()
            raw_hash = hashlib.sha256(row.tobytes()).digest()
            duplicate = raw_hash in state.raw_hashes_at_timestamp
            state.raw_hashes_at_timestamp.add(raw_hash)
            expected_tbbo = _mapping_for_date(tbbo_intervals, utc_date)
            expected_status = _mapping_for_date(status_intervals, utc_date)
            mapping_bad = expected_tbbo != instrument_id or expected_status != instrument_id
            bad_flag = bool(int(row["flags"]) & (F_BAD_TS_RECV | F_MAYBE_BAD_BOOK))
            if eligible:
                state.eligible_rows += 1
                state.sessions.add(session)
                if duplicate:
                    state.duplicate_rows += 1
                    state.duplicate_dates[utc_date] = (
                        state.duplicate_dates.get(utc_date, 0) + 1
                    )
                if mapping_bad:
                    state.mapping_mismatches += 1
                    state.mapping_mismatch_dates[utc_date] = (
                        state.mapping_mismatch_dates.get(utc_date, 0) + 1
                    )
                if bad_flag:
                    state.bad_flag_rows += 1
                    state.bad_flag_dates[utc_date] = (
                        state.bad_flag_dates.get(utc_date, 0) + 1
                    )
                previous = state.first_use.get(instrument_id)
                candidate = (received, event)
                if previous is None or candidate < previous:
                    state.first_use[instrument_id] = candidate
            else:
                state.quarantined_rows += 1
                state.quarantined_duplicate_rows += int(duplicate)
                state.quarantined_mapping_mismatches += int(mapping_bad)
                state.quarantined_bad_flag_rows += int(bad_flag)
            state.total_rows += 1
            file_rows += 1
            state.prior_ts_recv = received
    return file_rows


def _definition_covers(
    definitions: Mapping[int, Sequence[tuple[int, int, int]]],
    first_use: Mapping[int, tuple[int, int]],
) -> bool:
    for instrument_id, (received, event) in first_use.items():
        candidates = definitions.get(instrument_id, ())
        if not any(
            definition_received <= received
            and (activation in {0, UINT64_NULL} or activation <= event)
            and (expiration in {0, UINT64_NULL} or event < expiration)
            for definition_received, activation, expiration in candidates
        ):
            return False
    return True


def _dated_counts(values: Mapping[str, int]) -> list[dict[str, object]]:
    return [{"date": key, "count": values[key]} for key in sorted(values)]


def _support_sources(
    preparation: Mapping[str, object],
    *,
    root: Path,
    market: str,
    schema: str,
) -> list[tuple[Path, str]]:
    active = preparation.get("active_definition_status_bindings")
    supplement = preparation.get("supplement")
    if not isinstance(active, list) or not isinstance(supplement, Mapping):
        raise IntegrityError("quality preparation support bindings are invalid")
    result = [
        (root / str(item["path"]), str(item["sha256"]))
        for item in active
        if isinstance(item, Mapping)
        and item.get("market") == market
        and item.get("schema") == schema
    ]
    entries = supplement.get("entries")
    vault = supplement.get("vault_snapshot_path")
    if not isinstance(entries, list) or type(vault) is not str:
        raise IntegrityError("quality preparation lacks the sealed supplement")
    for item in entries:
        if (
            isinstance(item, Mapping)
            and isinstance(item.get("request"), Mapping)
            and item["request"].get("market") == market
            and item["request"].get("schema") == schema
            and isinstance(item.get("dbn"), Mapping)
        ):
            result.append((
                resolve_snapshot(root, vault) / str(item["dbn"]["path"]),
                str(item["dbn"]["sha256"]),
            ))
    if len(result) != 3:
        raise IntegrityError("quality support source count differs")
    return sorted(result, key=lambda item: item[0].name)


def _require_monthly_sequence(requests: Sequence[object]) -> None:
    bounds = [
        (str(getattr(item, "start_date")), str(getattr(item, "end_date")))
        for item in requests
    ]
    if (
        len(bounds) != 12
        or bounds[0][0] != "2025-09-01"
        or bounds[-1][1] != "2026-09-01"
        or any(start >= end for start, end in bounds)
        or any(left[1] != right[0] for left, right in zip(bounds, bounds[1:]))
    ):
        raise IntegrityError("TBBO monthly intervals contain a gap or overlap")


def _market_quality_disposition(
    *,
    monthly: Sequence[Mapping[str, object]],
    state: _TbboScanState,
    expected_sessions: set[str],
    definitions: Mapping[int, Sequence[tuple[int, int, int]]],
) -> dict[str, object]:
    assert state.sessions is not None and state.first_use is not None
    assert state.duplicate_dates is not None
    assert state.bad_flag_dates is not None
    assert state.mapping_mismatch_dates is not None
    missing_sessions = sorted(expected_sessions - state.sessions)
    definition_coverage_passed = _definition_covers(definitions, state.first_use)
    return {
        "quality_passed": not (
        len(monthly) != 12
        or state.bad_flag_rows
        or state.mapping_mismatches
        or not definition_coverage_passed
        ),
        "missing_empirical_sessions": missing_sessions,
        "missing_empirical_sessions_blocking": False,
        "status_only_session_policy": STATUS_ONLY_SESSION_POLICY,
        "definition_coverage_passed": definition_coverage_passed,
    }


def _scan_market(
    *,
    root: Path,
    plan: Mapping[str, object],
    preparation: Mapping[str, object],
    market: str,
    degraded_dates: Sequence[str],
    chunk_loader: Callable[[Path], Iterable[np.ndarray]],
    header_loader: Callable[[Path], object],
    deadline: float,
    clock: Callable[[], float],
) -> dict[str, object]:
    quarantined_dates = _market_quarantined_dates(market, degraded_dates)
    definitions = _scan_definitions(
        _support_sources(preparation, root=root, market=market, schema="definition"),
        chunk_loader=chunk_loader,
        deadline=deadline,
        clock=clock,
    )
    expected_sessions, first_use, status_intervals = _scan_status(
        _support_sources(preparation, root=root, market=market, schema="status"),
        market=market,
        degraded_dates=quarantined_dates,
        chunk_loader=chunk_loader,
        header_loader=header_loader,
        deadline=deadline,
        clock=clock,
    )
    acquisition_plan = load_acquisition_plan(root=root)
    registry = _tbbo_registry(root)
    entries = registry["entries"]
    assert isinstance(entries, list)
    by_path = {
        str(item["dbn"]["path"]): item
        for item in entries
        if isinstance(item, Mapping) and isinstance(item.get("dbn"), Mapping)
    }
    snapshot = resolve_snapshot(root, str(registry["vault_snapshot_path"]))
    state = _TbboScanState(first_use=dict(first_use))
    monthly: list[dict[str, object]] = []
    requests = tuple(
        item for item in build_request_ledger(acquisition_plan) if item.market == market
    )
    _require_monthly_sequence(requests)
    for request in requests:
        entry = by_path.get(request.snapshot_relative_path)
        if not isinstance(entry, Mapping) or not isinstance(entry.get("dbn"), Mapping):
            raise IntegrityError("TBBO registry request is absent during quality scan")
        path = snapshot / request.snapshot_relative_path
        metadata = header_loader(path)
        intervals = _mapping_intervals(
            metadata,
            symbol=f"{market}.v.0",
            start_date=request.start_date,
            end_date=request.end_date,
        )
        rows = _scan_tbbo_chunks(
            _bound_file(
                path,
                expected_sha256=str(entry["dbn"]["sha256"]),
                chunk_loader=chunk_loader,
            ),
            state=state,
            start_ns=_iso_ns(request.start),
            end_ns=_iso_ns(request.end),
            tbbo_intervals=intervals,
            status_intervals=status_intervals,
            degraded_dates=quarantined_dates,
            deadline=deadline,
            clock=clock,
        )
        if rows <= 0:
            raise IntegrityError("TBBO monthly file is empty")
        monthly.append({
            "interval": f"{request.start_date}/{request.end_date}",
            "row_count": rows,
        })
    assert state.sessions is not None and state.first_use is not None
    disposition = _market_quality_disposition(
        monthly=monthly,
        state=state,
        expected_sessions=expected_sessions,
        definitions=definitions,
    )
    return {
        "market": market,
        "monthly": monthly,
        "total_rows": state.total_rows,
        "eligible_rows": state.eligible_rows,
        "quarantined_rows": state.quarantined_rows,
        "eligible_identical_rows_retained": state.duplicate_rows,
        "eligible_bad_flag_rows": state.bad_flag_rows,
        "eligible_mapping_mismatches": state.mapping_mismatches,
        "eligible_identical_row_dates": _dated_counts(state.duplicate_dates),
        "eligible_bad_flag_dates": _dated_counts(state.bad_flag_dates),
        "eligible_mapping_mismatch_dates": _dated_counts(
            state.mapping_mismatch_dates
        ),
        "quarantined_identical_rows_retained": state.quarantined_duplicate_rows,
        "quarantined_bad_flag_rows": state.quarantined_bad_flag_rows,
        "quarantined_mapping_mismatches": state.quarantined_mapping_mismatches,
        "identical_rows_deduplicated": 0,
        "identical_rows_blocking": False,
        "market_quarantined_utc_dates": list(
            MARKET_QUARANTINED_DATES.get(market, ())
        ),
        "excluded_empirical_sessions": sorted(_excluded_sessions(quarantined_dates)),
        "empirical_expected_session_count": len(expected_sessions),
        "empirical_observed_session_count": len(state.sessions),
        "defined_eligible_instrument_count": len(state.first_use),
        **disposition,
    }


def load_preparation(*, root: Path, preparation_id: str) -> dict[str, object]:
    path = root / PREPARATION_ROOT / preparation_id / "preparation.json"
    preparation = json_object(path, "TBBO quality preparation")
    core = dict(preparation)
    observed_id = core.pop("preparation_id", None)
    core.pop("preparation_path", None)
    if (
        observed_id != preparation_id
        or observed_id != sha256_json(core)
        or preparation.get("preparation_path") != path.relative_to(root).as_posix()
        or preparation.get("operation") != PREPARE_OPERATION
        or preparation.get("dbn_rows_decoded") != 0
        or preparation.get("provider_calls") != 0
    ):
        raise IntegrityError("TBBO quality preparation identity is invalid")
    return preparation


def required_quality_scope(
    *, root: Path, plan: Mapping[str, object], preparation: Mapping[str, object],
) -> dict[str, str]:
    supplement = preparation.get("supplement")
    if (
        preparation.get("state")
        != "PREPARED_REQUIRES_SEPARATE_PROTECTED_ROW_SCAN_APPROVAL"
        or not isinstance(supplement, Mapping)
        or supplement.get("state") != "SEALED_HEADER_VERIFIED"
        or type(supplement.get("snapshot_id")) is not str
    ):
        raise UnauthorizedOperation("quality preparation lacks the sealed supplement")
    preparation_path = root / str(preparation["preparation_path"])
    return {
        "tbbo_snapshot_id": TBBO_SNAPSHOT_ID,
        "supplement_snapshot_id": str(supplement["snapshot_id"]),
        "markets": ",".join(MARKETS),
        "period": "2025-09-01T00:00:00Z/2026-09-01T00:00:00Z",
        "preparation_id": str(preparation["preparation_id"]),
        "preparation_sha256": sha256_file(preparation_path),
        "maximum_batch_rows": str(MAXIMUM_BATCH_ROWS),
        "maximum_workers": str(MAXIMUM_WORKERS),
        "maximum_runtime_seconds": str(MAXIMUM_RUNTIME_SECONDS),
        "decoded_dataset_creation": "false",
        "price_size_side_feature_return_output": "false",
        "active_source_mutation": "false",
        "provider_calls": "0",
        "status_utc_midnight_snapshot_session_evidence": "false",
        "status_only_sessions": "report_missing_non_blocking_no_synthetic_rows",
        "status_session_diagnostic_sha256": sha256_file(
            root / STATUS_SESSION_DIAGNOSTIC_PATH
        ),
        "identical_tbbo_rows": "retain_report_non_blocking_no_deduplication",
        "market_quarantines": ",".join(
            f"{market}:{date}"
            for market, dates in MARKET_QUARANTINED_DATES.items()
            for date in dates
        ),
        "implementation_sha256": str(plan["implementation_sha256"]),
        "approval_command": CERTIFY_OPERATION,
        "approval_plan_id": str(preparation["preparation_id"]),
        "approval_plan_sha256": sha256_file(preparation_path),
    }


def execute_authorized_quality_scan(
    *,
    root: Path,
    preparation_id: str,
    authorization: OperationReceipt,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
    chunk_loader: Callable[[Path], Iterable[np.ndarray]] = _default_chunk_loader,
    header_loader: Callable[[Path], object] = load_header,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, object]:
    """Run the one approved, aggregate-only protected quality scan."""

    root = root.resolve(strict=True)
    plan = load_plan(root=root)
    preparation = load_preparation(root=root, preparation_id=preparation_id)
    supplement = preparation.get("supplement")
    if not isinstance(supplement, Mapping) or type(supplement.get("snapshot_id")) is not str:
        raise UnauthorizedOperation("quality preparation has no supplement binding")
    refreshed = prepare_quality_certification(
        root=root,
        supplement_snapshot_id=str(supplement["snapshot_id"]),
        header_loader=header_loader,
    )
    if refreshed["preparation_id"] != preparation_id:
        raise IntegrityError("quality preparation drifted before protected row access")
    environment_check(root)
    boundary = RepoBoundary(root)
    scope = required_quality_scope(root=root, plan=plan, preparation=preparation)
    lock = SingleInstance(root / CERTIFICATION_LOCK)
    if not lock.acquire():
        raise UnauthorizedOperation("another TBBO quality scan is running")
    attempt = root / PREPARATION_ROOT / preparation_id / "attempts" / authorization.receipt_id
    results: list[dict[str, object]] = []
    try:
        authorization.consume(
            boundary,
            operation=CERTIFY_OPERATION,
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            required_scope=scope,
        )
        attempt.mkdir(parents=True, exist_ok=True)
        conditions = supplement.get("provider_conditions")
        degraded = (
            conditions.get("degraded_utc_dates")
            if isinstance(conditions, Mapping)
            else None
        )
        if (
            not isinstance(degraded, list)
            or not set(DEGRADED_DATES).issubset(set(str(item) for item in degraded))
        ):
            raise IntegrityError("quality preparation lost mandatory degraded dates")
        degraded_dates = tuple(sorted(str(item) for item in degraded))
        deadline = clock() + MAXIMUM_RUNTIME_SECONDS
        with ThreadPoolExecutor(max_workers=MAXIMUM_WORKERS) as pool:
            futures = {
                market: pool.submit(
                    _scan_market,
                    root=root,
                    plan=plan,
                    preparation=preparation,
                    market=market,
                    degraded_dates=degraded_dates,
                    chunk_loader=chunk_loader,
                    header_loader=header_loader,
                    deadline=deadline,
                    clock=clock,
                )
                for market in MARKETS
            }
            results = [futures[market].result() for market in MARKETS]
        if clock() > deadline:
            raise UnauthorizedOperation("quality scan runtime exhausted")
        if protected_active_state(root) != preparation["protected_active_baseline"]:
            raise IntegrityError("protected active sources changed during quality scan")
        if any(item.get("quality_passed") is not True for item in results):
            raise IntegrityError("eligible TBBO quality gate failed")
        certificate_core: dict[str, object] = {
            "schema_version": "tbbo_quality_certificate/1.0.0",
            "state": "CERTIFIED_ROW_QUALITY_WITH_MANDATORY_EXCLUSIONS_NOT_CATALOG_ADMITTED",
            "operation": CERTIFY_OPERATION,
            "plan_id": plan["plan_id"],
            "plan_sha256": sha256_file(root / PLAN_PATH),
            "implementation_sha256": plan["implementation_sha256"],
            "preparation_id": preparation_id,
            "preparation_sha256": sha256_file(
                root / str(preparation["preparation_path"])
            ),
            "authorization_receipt_id": authorization.receipt_id,
            "tbbo_snapshot_id": TBBO_SNAPSHOT_ID,
            "supplement_snapshot_id": supplement["snapshot_id"],
            "markets": list(MARKETS),
            "degraded_utc_dates": list(degraded_dates),
            "excluded_empirical_sessions": sorted(_excluded_sessions(degraded_dates)),
            "boundary_sessions": list(BOUNDARY_SESSIONS),
            "market_quarantined_utc_dates": {
                key: list(value) for key, value in MARKET_QUARANTINED_DATES.items()
            },
            "market_quarantine_dependency_policy": MARKET_QUARANTINE_DEPENDENCY_POLICY,
            "status_utc_midnight_snapshot_policy": STATUS_MIDNIGHT_SNAPSHOT_POLICY,
            "status_only_session_policy": STATUS_ONLY_SESSION_POLICY,
            "status_session_diagnostic": {
                "path": STATUS_SESSION_DIAGNOSTIC_PATH.as_posix(),
                "sha256": sha256_file(root / STATUS_SESSION_DIAGNOSTIC_PATH),
            },
            "identical_tbbo_row_policy": IDENTICAL_TBBO_ROW_POLICY,
            "market_results": results,
            "total_rows_scanned": sum(int(item["total_rows"]) for item in results),
            "eligible_rows": sum(int(item["eligible_rows"]) for item in results),
            "quarantined_rows": sum(int(item["quarantined_rows"]) for item in results),
            "dbn_files_scanned": 192 + 3 * 2 * len(MARKETS),
            "maximum_batch_rows": MAXIMUM_BATCH_ROWS,
            "maximum_workers": MAXIMUM_WORKERS,
            "decoded_datasets_created": 0,
            "provider_calls": 0,
            "price_size_side_feature_return_output": False,
            "active_catalog_mutated": False,
            "research_catalog_admitted": False,
            "alpha_research_authorized": False,
        }
        certificate_id = sha256_json(certificate_core)
        certificate = {**certificate_core, "certificate_id": certificate_id}
        if len(canonical_bytes(certificate)) > MAXIMUM_OUTPUT_BYTES:
            raise UnauthorizedOperation("quality certificate output ceiling exceeded")
        certificate_path = (
            root / CERTIFICATION_REGISTRY_ROOT / certificate_id / "certificate.json"
        )
        write_or_verify(certificate_path, certificate, "TBBO quality certificate")
        return {
            "certificate_id": certificate_id,
            "certificate_path": certificate_path.relative_to(root).as_posix(),
            "state": certificate["state"],
            "total_rows_scanned": certificate["total_rows_scanned"],
            "dbn_files_scanned": certificate["dbn_files_scanned"],
            "decoded_datasets_created": 0,
            "research_catalog_admitted": False,
        }
    except Exception as exc:
        if attempt.exists():
            failure_core = {
                "schema_version": "tbbo_quality_failure/1.0.0",
                "state": "QUALITY_CERTIFICATION_FAILED_NOT_CATALOG_ELIGIBLE",
                "plan_id": plan["plan_id"],
                "preparation_id": preparation_id,
                "authorization_receipt_id": authorization.receipt_id,
                "exception_type": type(exc).__name__,
                "raw_exception_message_recorded": False,
                "decoded_datasets_created": 0,
                "provider_calls": 0,
                "active_catalog_mutated": False,
            }
            if results:
                failure_core.update({
                    "disposition": "ELIGIBLE_ROW_QUALITY_GATE_FAILED",
                    "market_results": results,
                    "total_rows_scanned": sum(
                        int(item["total_rows"]) for item in results
                    ),
                    "eligible_rows": sum(int(item["eligible_rows"]) for item in results),
                    "quarantined_rows": sum(
                        int(item["quarantined_rows"]) for item in results
                    ),
                })
            failure = {**failure_core, "failure_id": sha256_json(failure_core)}
            write_or_verify(
                attempt / f"failure_{failure['failure_id']}.json",
                failure,
                "TBBO quality failure",
            )
        raise
    finally:
        lock.release()


__all__ = [
    "CERTIFY_OPERATION",
    "DEGRADED_DATES",
    "PLAN_PATH",
    "PREPARE_OPERATION",
    "SUPPLEMENT_OPERATION",
    "TBBO_SNAPSHOT_ID",
    "build_file_supplement_provider_apis",
    "build_supplement_ledger",
    "execute_authorized_quality_scan",
    "execute_authorized_supplement",
    "load_plan",
    "normalize_provider_conditions",
    "prepare_quality_certification",
    "preflight_supplement",
    "required_quality_scope",
    "required_supplement_scope",
]
