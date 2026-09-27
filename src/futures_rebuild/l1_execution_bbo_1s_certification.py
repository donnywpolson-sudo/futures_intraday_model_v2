"""Provider-free certification and canonical publication of sealed BBO-1s custody."""

from __future__ import annotations

import gc
import json
import os
import shutil
from collections import Counter
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import databento
import numpy as np

from . import l1_execution_bbo_1s_market_partition as acquisition
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, io_path, is_linklike, sha256_file, sha256_json
from .data_layout import (
    DataFileEntry,
    DataReleaseManifest,
    PhasePublisher,
    verify_data_tree_closure,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation.decoder import SUPPORTED_DATABENTO_VERSION
from .live_cockpit.single_instance import SingleInstance
from .tbbo_16_market_acquisition import json_object, plain_tree_files, write_or_verify


PLAN_PATH: Final = Path("configs/l1_execution_bbo_1s_certification_plan_v1.json")
ROW_OPERATION: Final = "CERTIFY_L1_EXECUTION_BBO_1S_MARKET_PARTITION_V1_ONCE"
PUBLICATION_OPERATION: Final = "PUBLISH_RELEASE"
AUDIT_ROOT: Final = Path("state/l1_bbo_1s_custody_audit_registry")
CERTIFICATE_ROOT: Final = Path("state/l1_bbo_1s_quality_certification_registry")
CATALOG_ROOT: Final = Path("state/l1_bbo_1s_eligibility_catalog_registry")
PREPARATION_ROOT: Final = Path("state/l1_bbo_1s_publication_preparations")
ROW_LOCK: Final = Path("state/locks/l1_bbo_1s_quality_certification_v1.lock")
PUBLICATION_LOCK: Final = Path("state/locks/l1_bbo_1s_canonical_publication_v1.lock")
MAXIMUM_WORKERS: Final = 4
MAXIMUM_COMPRESSED_FILE_BYTES: Final = 1_000_000
MINIMUM_FREE_BYTES: Final = 5_000_000_000
UINT32_NULL: Final = 2**32 - 1
UINT64_NULL: Final = 2**64 - 1
F_BAD_TS_RECV: Final = 8
F_MAYBE_BAD_BOOK: Final = 4
BBO_DTYPE: Final = np.dtype(databento.BBOMsg._dtypes)
BBO_RTYPE: Final = int(databento.RType.BBO_1S)
DEFECT_COUNT_FIELDS: Final = (
    "duplicate_record_identities",
    "invalid_bid_sentinel_or_size_rows",
    "invalid_ask_sentinel_or_size_rows",
    "crossed_book_rows",
    "bad_timestamp_or_book_flag_rows",
    "unobserved_mapped_instrument_count",
)


@dataclass(frozen=True)
class SourceBinding:
    plan: dict[str, object]
    acquisition_plan: dict[str, object]
    record: dict[str, object]
    manifest: dict[str, object]
    attempt: Path
    requests: tuple[acquisition.MarketRequest, ...]


def load_plan(*, root: Path) -> dict[str, object]:
    plan = json_object(root / PLAN_PATH, "BBO-1s certification plan")
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    implementation = plan.get("implementation")
    acquisition_binding = plan.get("acquisition")
    if (
        plan_id != sha256_json(core)
        or plan.get("operation") != ROW_OPERATION
        or plan.get("maximum_workers") != MAXIMUM_WORKERS
        or plan.get("minimum_free_bytes") != MINIMUM_FREE_BYTES
        or not isinstance(implementation, Mapping)
        or implementation.get("path") != Path(__file__).relative_to(root).as_posix()
        or implementation.get("sha256") != sha256_file(Path(__file__))
        or not isinstance(acquisition_binding, Mapping)
        or acquisition_binding.get("plan_id")
        != acquisition.load_plan(root=root)["plan_id"]
        or tuple(plan.get("degraded_dates", ())) != acquisition.DEGRADED_DATES
    ):
        raise ContractError("BBO-1s certification plan drifted")
    return plan


def _source_binding(*, root: Path) -> SourceBinding:
    plan = load_plan(root=root)
    acquisition_plan = acquisition.load_plan(root=root)
    binding = plan["acquisition"]
    assert isinstance(binding, Mapping)
    record_path = root / str(binding["record_path"])
    record = json_object(record_path, "BBO-1s acquisition record")
    record_id = record.get("record_id")
    if (
        record_id != binding.get("record_id")
        or sha256_file(record_path) != binding.get("record_sha256")
        or record.get("state") != "PUBLISHED_SEALED_MARKET_PARTITION_CUSTODY_ONLY"
        or record.get("canonical_promotion_performed") is not False
        or record.get("row_quality_certified") is not False
    ):
        raise IntegrityError("BBO-1s acquisition record binding differs")
    manifest_path = root / str(record["manifest_path"])
    manifest = json_object(manifest_path, "BBO-1s acquisition manifest")
    manifest_core = dict(manifest)
    manifest_id = manifest_core.pop("manifest_id", None)
    if (
        manifest_id != record_id
        or manifest_id != sha256_json(manifest_core)
        or sha256_file(manifest_path) != record.get("manifest_sha256")
        or manifest.get("state")
        != "SEALED_MARKET_PARTITION_CUSTODY_NOT_ROW_QUALITY_CERTIFIED"
        or manifest.get("canonical_promotion_performed") is not False
        or manifest.get("dbn_rows_decoded") != 0
    ):
        raise IntegrityError("BBO-1s acquisition manifest binding differs")
    attempt = manifest_path.parent
    expected_attempt = root / str(binding["attempt_path"])
    predecessor = json_object(
        root / acquisition.predecessor.PLAN_PATH, "BBO-1s predecessor plan"
    )
    requests = acquisition.build_request_ledger(predecessor)
    if attempt != expected_attempt or len(requests) != 4_698:
        raise IntegrityError("BBO-1s acquisition attempt binding differs")
    return SourceBinding(
        plan, acquisition_plan, record, manifest, attempt, requests
    )


def _canonical_json(path: Path, label: str) -> dict[str, object]:
    value = json_object(path, label)
    if io_path(path).read_bytes() != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"{label} is not canonical JSON")
    return value


def verify_custody(*, root: Path) -> dict[str, object]:
    """Hash and inventory every sealed DBN and sidecar without decoding rows."""

    source = _source_binding(root=root)
    request_plan = _canonical_json(source.attempt / "request_plan.json", "request plan")
    preflight = _canonical_json(source.attempt / "preflight.json", "preflight")
    manifest_path = source.attempt / "manifest.json"
    manifest = _canonical_json(manifest_path, "acquisition manifest")
    request_core = dict(request_plan)
    request_set_id = request_core.pop("request_set_id", None)
    preflight_core = dict(preflight)
    preflight_id = preflight_core.pop("preflight_id", None)
    staged = manifest.get("staged_files")
    if (
        request_set_id != sha256_json(request_plan.get("requests"))
        or request_set_id != sha256_json([item.as_dict() for item in source.requests])
        or preflight_id != sha256_json(preflight_core)
        or not isinstance(staged, list)
        or len(staged) != 4_698
        or manifest.get("staged_files_id") != sha256_json(staged)
        or preflight.get("metadata_calls") != 9_398
        or preflight.get("metadata_attempts") != 9_398
        or preflight.get("external_cost_usd") != "0"
    ):
        raise IntegrityError("BBO-1s sealed control identities differ")
    by_request = {
        str(item.get("request_id")): item
        for item in staged
        if isinstance(item, Mapping)
    }
    expected_files = {"request_plan.json", "preflight.json", "manifest.json"}
    inventory: list[dict[str, object]] = [
        {
            "path": path.name,
            "size": io_path(path).stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in (
            source.attempt / "request_plan.json",
            source.attempt / "preflight.json",
            manifest_path,
        )
    ]
    dbn_count = 0
    sidecar_count = 0
    dbn_bytes = 0
    for request in source.requests:
        item = by_request.get(request.request_id)
        if not isinstance(item, Mapping):
            raise IntegrityError("BBO-1s manifest request is absent")
        dbn, sidecar_path = acquisition._paths(source.attempt, request)
        sidecar = _canonical_json(sidecar_path, "BBO-1s staged sidecar")
        sidecar_core = dict(sidecar)
        sidecar_id = sidecar_core.pop("sidecar_id", None)
        if sidecar != item or sidecar_id != sha256_json(sidecar_core):
            raise IntegrityError("BBO-1s staged sidecar differs from its manifest")
        sidecar_relative = sidecar_path.relative_to(source.attempt).as_posix()
        expected_files.add(sidecar_relative)
        sidecar_count += 1
        inventory.append({
            "path": sidecar_relative,
            "size": io_path(sidecar_path).stat().st_size,
            "sha256": sha256_file(sidecar_path),
        })
        file_size = item.get("file_size_bytes")
        file_hash = item.get("file_sha256")
        if item.get("billable_bytes") == 0:
            if io_path(dbn).exists() or file_size is not None or file_hash is not None:
                raise IntegrityError("zero-billable request unexpectedly owns a DBN")
            continue
        if (
            type(file_size) is not int
            or file_size <= 0
            or file_size > MAXIMUM_COMPRESSED_FILE_BYTES
            or type(file_hash) is not str
            or not io_path(dbn).is_file()
            or is_linklike(dbn)
            or io_path(dbn).stat().st_nlink != 1
            or io_path(dbn).stat().st_size != file_size
            or sha256_file(dbn) != file_hash
        ):
            raise IntegrityError("BBO-1s staged DBN bytes differ")
        dbn_relative = dbn.relative_to(source.attempt).as_posix()
        expected_files.add(dbn_relative)
        inventory.append({"path": dbn_relative, "size": file_size, "sha256": file_hash})
        dbn_count += 1
        dbn_bytes += file_size
    observed = {
        path.relative_to(source.attempt).as_posix()
        for path in plain_tree_files(source.attempt, reject_hardlinks=True)
    }
    conditions = Counter(str(item["provider_condition"]) for item in staged)
    if (
        observed != expected_files
        or dbn_count != 4_625
        or sidecar_count != 4_698
        or conditions
        != Counter({"NOT_REPORTED_DEGRADED": 4_517, "DEGRADED": 108,
                    "ZERO_BILLABLE_NO_DBN": 73})
    ):
        raise IntegrityError("BBO-1s sealed inventory closure failed")
    audit_core = {
        "schema_version": "l1_bbo_1s_custody_audit/1.0.0",
        "state": "INDEPENDENT_FULL_FILE_HASH_AUDIT_PASS",
        "plan_id": source.plan["plan_id"],
        "acquisition_record_id": source.record["record_id"],
        "acquisition_manifest_sha256": source.record["manifest_sha256"],
        "request_set_id": request_set_id,
        "preflight_id": preflight_id,
        "inventory_id": sha256_json(sorted(inventory, key=lambda item: str(item["path"]))),
        "file_count": len(inventory),
        "dbn_file_count": dbn_count,
        "sidecar_file_count": sidecar_count,
        "dbn_bytes": dbn_bytes,
        "provider_condition_counts": dict(sorted(conditions.items())),
        "partials": 0,
        "failures": 0,
        "dbn_rows_decoded": 0,
        "canonical_publication_performed": False,
    }
    audit_id = sha256_json(audit_core)
    audit = {**audit_core, "audit_id": audit_id}
    write_or_verify(root / AUDIT_ROOT / audit_id / "audit.json", audit, "BBO-1s custody audit")
    return audit


def required_row_scope(*, root: Path, audit: Mapping[str, object]) -> dict[str, str]:
    plan = load_plan(root=root)
    plan_sha256 = sha256_file(root / PLAN_PATH)
    return {
        "plan_id": str(plan["plan_id"]),
        "plan_sha256": plan_sha256,
        "custody_audit_id": str(audit["audit_id"]),
        "dbn_file_count": "4625",
        "maximum_workers": str(MAXIMUM_WORKERS),
        "provider_calls": "0",
        "decoded_dataset_creation": "false",
        "alpha_activation": "false",
        "approval_command": ROW_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": plan_sha256,
    }


def _mapping_ids(metadata: object, request: acquisition.MarketRequest) -> set[int]:
    partial = getattr(metadata, "partial", None)
    not_found = getattr(metadata, "not_found", None)
    mappings = getattr(metadata, "mappings", None)
    if partial or not_found or not isinstance(mappings, Mapping) or set(mappings) != set(request.symbols):
        raise IntegrityError("BBO-1s metadata mappings are partial or unresolved")
    target = request.trade_date
    result: set[int] = set()
    for symbol in request.symbols:
        intervals = mappings.get(symbol)
        if not isinstance(intervals, list) or not intervals:
            raise IntegrityError("BBO-1s metadata mapping is empty")
        covered = False
        for interval in intervals:
            if not isinstance(interval, Mapping):
                raise IntegrityError("BBO-1s metadata mapping is malformed")
            start = str(interval.get("start_date"))
            end = str(interval.get("end_date"))
            mapped = str(interval.get("symbol", ""))
            if start <= target < end:
                if not mapped.isdigit() or int(mapped) <= 0:
                    raise IntegrityError("BBO-1s mapped instrument ID is invalid")
                result.add(int(mapped))
                covered = True
        if not covered:
            raise IntegrityError("BBO-1s metadata mapping does not cover its request")
    return result


def _semantic_counts(rows: np.ndarray) -> dict[str, int]:
    identity = rows[["publisher_id", "instrument_id", "ts_event", "ts_recv", "sequence", "rtype"]]
    bid_undef = rows["bid_px_00"] == databento.UNDEF_PRICE
    ask_undef = rows["ask_px_00"] == databento.UNDEF_PRICE
    bid_size = rows["bid_sz_00"]
    ask_size = rows["ask_sz_00"]
    bid_count = rows["bid_ct_00"]
    ask_count = rows["ask_ct_00"]
    invalid_bid = (bid_undef & ((bid_size != 0) | (bid_count != 0))) | (
        ~bid_undef
        & ((bid_size == 0) | (bid_size == UINT32_NULL) | (bid_count == 0) | (bid_count == UINT32_NULL))
    )
    invalid_ask = (ask_undef & ((ask_size != 0) | (ask_count != 0))) | (
        ~ask_undef
        & ((ask_size == 0) | (ask_size == UINT32_NULL) | (ask_count == 0) | (ask_count == UINT32_NULL))
    )
    crossed = (~bid_undef) & (~ask_undef) & (rows["bid_px_00"] > rows["ask_px_00"])
    bad_flags = (rows["flags"] & (F_BAD_TS_RECV | F_MAYBE_BAD_BOOK)) != 0
    return {
        "duplicate_record_identities": len(identity) - len(np.unique(identity)),
        "invalid_bid_sentinel_or_size_rows": int(np.count_nonzero(invalid_bid)),
        "invalid_ask_sentinel_or_size_rows": int(np.count_nonzero(invalid_ask)),
        "crossed_book_rows": int(np.count_nonzero(crossed)),
        "bad_timestamp_or_book_flag_rows": int(np.count_nonzero(bad_flags)),
    }


def _scan_one(
    source: SourceBinding,
    request: acquisition.MarketRequest,
    item: Mapping[str, object],
) -> dict[str, object]:
    dbn, _ = acquisition._paths(source.attempt, request)
    expected_hash = str(item["file_sha256"])
    if sha256_file(dbn) != expected_hash:
        raise IntegrityError("BBO-1s source hash differs before row scan")
    store = databento.DBNStore.from_file(io_path(dbn))
    acquisition.verify_store_metadata(store=store, request=request)
    mapped_ids = _mapping_ids(store.metadata, request)
    try:
        rows = store.to_ndarray()
    except (NotImplementedError, TypeError, ValueError) as exc:
        raise IntegrityError("offline BBO-1s DBN decoding failed") from exc
    finally:
        del store
    if not isinstance(rows, np.ndarray) or rows.dtype != BBO_DTYPE or not len(rows):
        raise IntegrityError("decoded BBO-1s rows differ from the pinned schema")
    start_ns = acquisition.predecessor._iso_to_ns(request.start)
    end_ns = acquisition.predecessor._iso_to_ns(request.end)
    times = rows["ts_recv"]
    instruments = rows["instrument_id"]
    if (
        np.any(rows["rtype"] != BBO_RTYPE)
        or np.any(rows["publisher_id"] == 0)
        or np.any(instruments == 0)
        or np.any(rows["ts_event"] == 0)
        or np.any(times == 0)
        or np.any(times == UINT64_NULL)
        or np.any(times < start_ns)
        or np.any(times >= end_ns)
        or np.any(times[1:] < times[:-1])
        or np.any(times % 1_000_000_000 != 0)
        or not set(int(value) for value in np.unique(instruments)) <= mapped_ids
    ):
        raise IntegrityError("BBO-1s structural row gate failed")
    observed_ids = set(int(value) for value in np.unique(instruments))
    unique_times = np.unique(times)
    gaps = np.diff(unique_times)
    semantic_counts = _semantic_counts(rows)
    semantic_counts["unobserved_mapped_instrument_count"] = len(mapped_ids - observed_ids)
    result = {
        "request_id": request.request_id,
        "market": request.market,
        "trade_date": request.trade_date,
        "provider_condition": item["provider_condition"],
        "row_count": len(rows),
        "instrument_count": len(mapped_ids),
        "first_ts_recv": int(times[0]),
        "last_ts_recv": int(times[-1]),
        "observed_second_count": len(unique_times),
        "gap_over_one_second_count": int(np.count_nonzero(gaps > 1_000_000_000)),
        "maximum_gap_seconds": 0 if not len(gaps) else int(np.max(gaps) // 1_000_000_000),
        "first_observation_lag_seconds": int((int(times[0]) - start_ns) // 1_000_000_000),
        "terminal_observation_lag_seconds": int((end_ns - int(times[-1])) // 1_000_000_000),
        "undefined_last_trade_timestamp_rows": int(
            np.count_nonzero(rows["ts_event"] == UINT64_NULL)
        ),
        **semantic_counts,
    }
    del rows
    gc.collect()
    if sha256_file(dbn) != expected_hash:
        raise IntegrityError("BBO-1s source hash differs after row scan")
    return result


def _scan_all(source: SourceBinding) -> list[dict[str, object]]:
    if databento.__version__ != SUPPORTED_DATABENTO_VERSION:
        raise ContractError("BBO-1s certification requires the pinned Databento decoder")
    staged = source.manifest["staged_files"]
    assert isinstance(staged, list)
    by_request = {
        str(item["request_id"]): item
        for item in staged
        if isinstance(item, Mapping) and item.get("billable_bytes") != 0
    }
    requests = [item for item in source.requests if item.request_id in by_request]
    with ThreadPoolExecutor(max_workers=MAXIMUM_WORKERS) as pool:
        futures = {
            request.request_id: pool.submit(_scan_one, source, request, by_request[request.request_id])
            for request in requests
        }
        return [futures[request.request_id].result() for request in requests]


def certify_rows(
    *, root: Path, audit: Mapping[str, object], authorization: OperationReceipt
) -> dict[str, object]:
    source = _source_binding(root=root)
    boundary = RepoBoundary(root)
    scope = required_row_scope(root=root, audit=audit)
    lock = SingleInstance(root / ROW_LOCK)
    if not lock.acquire():
        raise UnauthorizedOperation("another BBO-1s certification is running")
    try:
        authorization.consume(
            boundary,
            operation=ROW_OPERATION,
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            required_scope=scope,
        )
        first = _scan_all(source)
        second = _scan_all(source)
        if first != second or len(first) != 4_625:
            raise IntegrityError("independent BBO-1s scan reproduction failed")
        by_condition = Counter(str(item["provider_condition"]) for item in first)
        semantic_totals = {
            key: sum(int(item[key]) for item in first)
            for key in DEFECT_COUNT_FIELDS
        }
        certificate_core = {
            "schema_version": "l1_bbo_1s_quality_certificate/1.0.0",
            "state": "CERTIFIED_COMPLETE_SCAN_WITH_MANDATORY_EXCLUSIONS",
            "plan_id": source.plan["plan_id"],
            "plan_sha256": sha256_file(root / PLAN_PATH),
            "implementation_sha256": source.plan["implementation"]["sha256"],
            "authorization_receipt_id": authorization.receipt_id,
            "custody_audit_id": audit["audit_id"],
            "acquisition_record_id": source.record["record_id"],
            "dbn_files_scanned": len(first),
            "complete_scan_repetitions": 2,
            "total_rows_scanned_per_repetition": sum(int(item["row_count"]) for item in first),
            "provider_condition_counts": dict(sorted(by_condition.items())),
            "semantic_totals": semantic_totals,
            "undefined_last_trade_timestamp_rows": sum(
                int(item["undefined_last_trade_timestamp_rows"]) for item in first
            ),
            "semantic_defect_partition_count": sum(
                any(int(item[key]) for key in semantic_totals) for item in first
            ),
            "market_results_id": sha256_json(first),
            "market_date_results": first,
            "degraded_dates": list(acquisition.DEGRADED_DATES),
            "degraded_disposition": "PROVIDER_DEGRADED_EXCLUDED_NO_AUTHORITATIVE_WAIVER",
            "zero_billable_disposition": "PROVIDER_ZERO_BILLABLE_EXCLUDED_NO_DBN",
            "coverage_basis": "FIXED_REQUEST_INTERVAL_OBSERVED_BBO_SECONDS_GAPS_MEASURED_NO_SYNTHETIC_FILL",
            "decoded_datasets_created": 0,
            "provider_calls": 0,
            "alpha_source_authorized": False,
        }
        certificate_id = sha256_json(certificate_core)
        certificate = {**certificate_core, "certificate_id": certificate_id}
        write_or_verify(
            root / CERTIFICATE_ROOT / certificate_id / "certificate.json",
            certificate,
            "BBO-1s quality certificate",
        )
        return certificate
    finally:
        lock.release()


def build_eligibility_catalog(
    *, root: Path, certificate: Mapping[str, object]
) -> dict[str, object]:
    source = _source_binding(root=root)
    results = certificate.get("market_date_results")
    if not isinstance(results, list):
        raise IntegrityError("BBO-1s certificate lacks scan results")
    scans = {str(item["request_id"]): item for item in results if isinstance(item, Mapping)}
    entries: list[dict[str, object]] = []
    for item in source.manifest["staged_files"]:
        assert isinstance(item, Mapping)
        request_id = str(item["request_id"])
        condition = str(item["provider_condition"])
        scan = scans.get(request_id)
        semantic_defect = isinstance(scan, Mapping) and any(
            int(scan[key])
            for key in DEFECT_COUNT_FIELDS
        )
        eligible = condition == "NOT_REPORTED_DEGRADED" and not semantic_defect
        if condition == "DEGRADED":
            disposition = "PROVIDER_DEGRADED_EXCLUDED_NO_AUTHORITATIVE_WAIVER"
        elif condition == "ZERO_BILLABLE_NO_DBN":
            disposition = "PROVIDER_ZERO_BILLABLE_EXCLUDED_NO_DBN"
        elif semantic_defect:
            disposition = "ROW_QUALITY_DEFECT_EXCLUDED_NO_WAIVER"
        else:
            disposition = "CERTIFIED_SOURCE_ELIGIBLE_NOT_ALPHA_ACTIVE"
        if eligible and request_id not in scans:
            raise IntegrityError("eligible BBO-1s request lacks a scan result")
        entries.append({
            "request_id": request_id,
            "market": item["market"],
            "trade_date": item["trade_date"],
            "logical_path": item["logical_path"],
            "provider_condition": condition,
            "disposition": disposition,
            "structural_scan_completed": request_id in scans,
            "row_quality_certified": eligible,
            "source_eligible": eligible,
            "alpha_source_authorized": False,
        })
    counts = Counter(str(item["disposition"]) for item in entries)
    if (
        counts["PROVIDER_DEGRADED_EXCLUDED_NO_AUTHORITATIVE_WAIVER"] != 108
        or counts["PROVIDER_ZERO_BILLABLE_EXCLUDED_NO_DBN"] != 73
        or sum(counts.values()) != 4_698
    ):
        raise IntegrityError("BBO-1s eligibility adjudication counts differ")
    eligible_count = counts["CERTIFIED_SOURCE_ELIGIBLE_NOT_ALPHA_ACTIVE"]
    core = {
        "schema_version": "l1_bbo_1s_eligibility_catalog/1.0.0",
        "state": "SEALED_SOURCE_ELIGIBILITY_CATALOG_NOT_ALPHA_ACTIVE",
        "plan_id": source.plan["plan_id"],
        "quality_certificate_id": certificate["certificate_id"],
        "acquisition_record_id": source.record["record_id"],
        "entries": entries,
        "entry_count": len(entries),
        "eligible_count": eligible_count,
        "excluded_count": len(entries) - eligible_count,
        "disposition_counts": dict(sorted(counts.items())),
        "alpha_source_authorized": False,
    }
    catalog_id = sha256_json(core)
    catalog = {**core, "catalog_id": catalog_id}
    write_or_verify(
        root / CATALOG_ROOT / catalog_id / "catalog.json",
        catalog,
        "BBO-1s eligibility catalog",
    )
    return catalog


def _canonical_sidecar(
    *, source_item: Mapping[str, object], eligibility_entry: Mapping[str, object],
    certificate_id: str, catalog_id: str
) -> dict[str, object]:
    condition = str(source_item["provider_condition"])
    eligible = eligibility_entry.get("source_eligible") is True
    disposition = str(eligibility_entry["disposition"])
    core = {
        "schema_version": "l1_bbo_1s_canonical_sidecar/1.0.0",
        "state": "PUBLISHED_CANONICAL_CUSTODY",
        "market": source_item["market"],
        "trade_date": source_item["trade_date"],
        "logical_path": source_item["logical_path"],
        "request_id": source_item["request_id"],
        "source_sidecar_id": source_item["sidecar_id"],
        "source_plan_id": source_item["plan_id"],
        "source_dbn_size_bytes": source_item["file_size_bytes"],
        "source_dbn_sha256": source_item["file_sha256"],
        "provider_condition": condition,
        "disposition": disposition,
        "quality_certificate_id": certificate_id,
        "eligibility_catalog_id": catalog_id,
        "structural_scan_completed": condition != "ZERO_BILLABLE_NO_DBN",
        "row_quality_certified": eligible,
        "source_eligible": eligible,
        "alpha_source_authorized": False,
    }
    return {**core, "sidecar_id": sha256_json(core)}


def _copy_create_only(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with io_path(source).open("rb") as reader, io_path(destination).open("xb") as writer:
        shutil.copyfileobj(reader, writer, length=1_048_576)
        writer.flush()
        os.fsync(writer.fileno())
    if io_path(destination).stat().st_nlink != 1 or sha256_file(destination) != sha256_file(source):
        raise IntegrityError("BBO-1s publication staging copy differs")


def prepare_publication(
    *, root: Path, certificate: Mapping[str, object], catalog: Mapping[str, object],
    authorization: OperationReceipt,
) -> dict[str, object]:
    source = _source_binding(root=root)
    if shutil.disk_usage(root).free < MINIMUM_FREE_BYTES:
        raise UnauthorizedOperation("BBO-1s publication free-space floor failed")
    boundary = RepoBoundary(root)
    publisher = PhasePublisher(
        boundary=boundary, operation_receipt=authorization, lock_path=root / PUBLICATION_LOCK
    )
    stage = publisher.create_stage("l1_bbo_1s_canonical")
    logical_paths: dict[str, str] = {}
    catalog_entries = {
        str(item["request_id"]): item
        for item in catalog["entries"]
        if isinstance(item, Mapping)
    }
    for item in source.manifest["staged_files"]:
        assert isinstance(item, Mapping)
        logical_dbn = str(item["logical_path"])
        stage_dbn = stage / logical_dbn.removeprefix("data/dbn/")
        if item["billable_bytes"] != 0:
            request = next(
                request for request in source.requests if request.request_id == item["request_id"]
            )
            source_dbn, _ = acquisition._paths(source.attempt, request)
            _copy_create_only(source_dbn, stage_dbn)
            logical_paths[stage_dbn.relative_to(stage).as_posix()] = logical_dbn
        sidecar = _canonical_sidecar(
            source_item=item,
            eligibility_entry=catalog_entries[str(item["request_id"])],
            certificate_id=str(certificate["certificate_id"]),
            catalog_id=str(catalog["catalog_id"]),
        )
        stage_sidecar = Path(f"{stage_dbn}.manifest.json")
        stage_sidecar.parent.mkdir(parents=True, exist_ok=True)
        with io_path(stage_sidecar).open("xb") as stream:
            stream.write(canonical_bytes(sidecar) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        logical_paths[stage_sidecar.relative_to(stage).as_posix()] = f"{logical_dbn}.manifest.json"
    manifest = DataReleaseManifest.build(
        stage,
        phase="dbn",
        release_kind="l1_bbo_1s_market_partition_canonical_custody",
        schema_version="1.0.0",
        logical_paths=logical_paths,
        source_release_ids=(
            str(source.record["record_id"]),
            str(certificate["certificate_id"]),
            str(catalog["catalog_id"]),
        ),
        embedded_documents={
            "quality_certificate": {
                "id": certificate["certificate_id"],
                "sha256": sha256_file(
                    root / CERTIFICATE_ROOT / str(certificate["certificate_id"]) / "certificate.json"
                ),
            },
            "eligibility_catalog": {
                "id": catalog["catalog_id"],
                "sha256": sha256_file(
                    root / CATALOG_ROOT / str(catalog["catalog_id"]) / "catalog.json"
                ),
            },
        },
        metadata={
            "acquisition_record_id": source.record["record_id"],
            "dbn_file_count": 4_625,
            "sidecar_file_count": 4_698,
            "eligible_count": catalog["eligible_count"],
            "provider_degraded_excluded_count": 108,
            "zero_billable_excluded_count": 73,
            "row_quality_defect_excluded_count": catalog["disposition_counts"].get(
                "ROW_QUALITY_DEFECT_EXCLUDED_NO_WAIVER", 0
            ),
            "alpha_source_authorized": False,
        },
    )
    preparation = {
        "schema_version": "l1_bbo_1s_publication_preparation/1.0.0",
        "state": "PREPARED_FOR_MANIFEST_LAST_PUBLICATION",
        "plan_id": source.plan["plan_id"],
        "release_id": manifest.release_id,
        "stage_path": stage.relative_to(root).as_posix(),
        "manifest": manifest.as_dict(),
        "logical_paths": logical_paths,
        "quality_certificate_id": certificate["certificate_id"],
        "eligibility_catalog_id": catalog["catalog_id"],
        "source_acquisition_record_id": source.record["record_id"],
        "file_count": len(manifest.files),
    }
    write_or_verify(
        root / PREPARATION_ROOT / manifest.release_id / "preparation.json",
        preparation,
        "BBO-1s publication preparation",
    )
    return preparation


def required_publication_scope(
    *, root: Path, preparation: Mapping[str, object]
) -> dict[str, str]:
    plan = load_plan(root=root)
    plan_sha256 = sha256_file(root / PLAN_PATH)
    return {
        "plan_id": str(preparation["plan_id"]),
        "quality_certificate_id": str(preparation["quality_certificate_id"]),
        "eligibility_catalog_id": str(preparation["eligibility_catalog_id"]),
        "dbn_file_count": "4625",
        "sidecar_file_count": "4698",
        "canonical_root": "data/dbn/bbo_1s",
        "source_staging_preserved": "true",
        "alpha_activation": "false",
        "approval_command": PUBLICATION_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": plan_sha256,
    }


def publish_prepared(
    *, root: Path, preparation: Mapping[str, object], authorization: OperationReceipt
) -> dict[str, object]:
    boundary = RepoBoundary(root)
    scope = required_publication_scope(root=root, preparation=preparation)
    authorization.consume(
        boundary,
        operation=PUBLICATION_OPERATION,
        classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        required_scope=scope,
    )
    manifest_payload = preparation["manifest"]
    if not isinstance(manifest_payload, Mapping):
        raise IntegrityError("BBO-1s publication preparation lacks its manifest")
    manifest = DataReleaseManifest(
        release_id=str(manifest_payload["release_id"]),
        phase=str(manifest_payload["phase"]),
        release_kind=str(manifest_payload["release_kind"]),
        schema_version=str(manifest_payload["schema_version"]),
        source_release_ids=tuple(str(item) for item in manifest_payload["source_release_ids"]),
        files=tuple(
            DataFileEntry(
                str(item["logical_path"]), int(item["size"]), str(item["sha256"])
            )
            for item in manifest_payload["files"]
        ),
        embedded_documents=dict(manifest_payload["embedded_documents"]),
        metadata=dict(manifest_payload["metadata"]),
        layout_version=str(manifest_payload["layout_version"]),
        manifest_version=str(manifest_payload["manifest_version"]),
    )
    publisher = PhasePublisher(
        boundary=boundary, operation_receipt=authorization, lock_path=root / PUBLICATION_LOCK
    )
    stage = root / str(preparation["stage_path"])
    relative_to_logical = dict(preparation["logical_paths"])
    manifest_path = publisher.publish(
        stage,
        manifest,
        staged_paths={logical: relative for relative, logical in relative_to_logical.items()},
    )
    closure = verify_data_tree_closure(boundary)
    return {
        "release_id": manifest.release_id,
        "manifest_path": manifest_path.relative_to(root).as_posix(),
        "canonical_root": "data/dbn/bbo_1s",
        "dbn_file_count": 4_625,
        "sidecar_file_count": 4_698,
        "eligible_count": manifest.metadata["eligible_count"],
        "excluded_degraded_count": 108,
        "excluded_zero_billable_count": 73,
        "excluded_row_quality_defect_count": manifest.metadata[
            "row_quality_defect_excluded_count"
        ],
        "source_staging_preserved": True,
        "alpha_source_authorized": False,
        "data_tree_closure": closure,
    }


__all__ = [
    "PLAN_PATH",
    "PUBLICATION_OPERATION",
    "ROW_OPERATION",
    "build_eligibility_catalog",
    "certify_rows",
    "load_plan",
    "prepare_publication",
    "publish_prepared",
    "required_publication_scope",
    "required_row_scope",
    "verify_custody",
]
