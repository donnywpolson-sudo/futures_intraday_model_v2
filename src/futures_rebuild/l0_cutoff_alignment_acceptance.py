"""Provider-free structural audit and conditional canonical custody publication."""

from __future__ import annotations

import gc
import json
import os
import shutil
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Final
from zoneinfo import ZoneInfo

import databento

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (
    canonical_authority_file_bytes,
    canonical_bytes,
    io_path,
    iter_plain_files,
    read_canonical_authority,
    sha256_bytes,
    sha256_file,
    sha256_json,
    write_new_canonical_authority,
)
from .causal_source_closure import validate_source_closure
from .dbn_catalog import (
    FULL_SCAN_CHUNK_RECORDS,
    RECEIVE_TIME_COVERAGE_SCHEMAS,
    SUPPORTED_DATABENTO_VERSION,
    _iter_arrays,
    _normalize_schema,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation_operation_firewall import issue_current_source_closure_context
from .l0_cutoff_alignment_acquisition import STANDARD_ROOTS, verify_completed_acquisition
from .live_cockpit.single_instance import SingleInstance
from .runtime_environment import require_locked_repository_environment


ACQUISITION_RECEIPT_ID: Final = (
    "35f6f066e581f451981ace3fb745e33113b018c5e0040e4404fcd2694ab45d13"
)
ACQUISITION_ROOT: Final = Path(
    "state/provider_acquisition_staging/l0_cutoff_alignment_v1"
) / ACQUISITION_RECEIPT_ID
ACQUISITION_PLAN: Final = Path("configs/l0_cutoff_alignment_acquisition_v1.json")
ACTIVE_POINTER: Final = Path("configs/active_dbn_congruence_release_v1.json")
ACTIVE_SOURCE_CONTRACT: Final = Path("configs/source_contract.json")
PREDECESSOR_SOURCE_CONTRACT: Final = Path("configs/source_contract_v4.json")
SUCCESSOR_SOURCE_CONTRACT: Final = Path("configs/source_contract_v5.json")
ACTIVE_ALPHA_POINTER: Final = Path("configs/active_alpha_research_ladder.json")
ACCEPTANCE_ROOT: Final = Path("state/l0_cutoff_alignment_acceptance_v1")
PUBLICATION_REPORT_ROOT: Final = Path("reports/l0_cutoff_alignment_publication_v1")
PUBLICATION_STAGE_ROOT: Final = Path(
    "state/data_publication_staging/l0_cutoff_alignment_v1"
)
PUBLICATION_LOCK: Final = Path("state/locks/l0_cutoff_alignment_publication_v1.lock")
DBN_ROOT: Final = Path("data/dbn")
AUDIT_OPERATION: Final = "AUDIT_L0_AUG31_CUTOFF_ALIGNMENT_ROWS_V1_ONCE"
PUBLICATION_OPERATION: Final = "PUBLISH_L0_AUG31_CUTOFF_ALIGNMENT_CUSTODY_V1_ONCE"
START: Final = "2026-07-14T00:00:00Z"
END: Final = "2026-09-01T00:00:00Z"
START_NS: Final = 1_783_987_200_000_000_000
END_NS: Final = 1_788_220_800_000_000_000
WARNING_DATES: Final = ("2026-07-30", "2026-08-29")
OHLCV_FAMILIES: Final = ("ohlcv-1s", "ohlcv-1m", "ohlcv-1h", "ohlcv-1d")
PROJECT_ZONE: Final = ZoneInfo("America/Chicago")


def _json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(io_path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise IntegrityError(f"{label} is not readable JSON") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{label} must be an object")
    return value


def _relative(root: Path, path: Path) -> str:
    try:
        return path.resolve(strict=False).relative_to(root).as_posix()
    except ValueError as exc:
        raise ContractError(f"path escapes repository: {path}") from exc


def _implementation_sha(root: Path) -> str:
    return sha256_file(root / Path(__file__).resolve().relative_to(root))


def _timestamp_text(value: int) -> str:
    seconds, nanos = divmod(value, 1_000_000_000)
    prefix = datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{prefix}.{nanos:09d}Z"


def _utc_date(value: int) -> str:
    return datetime.fromtimestamp(value // 1_000_000_000, timezone.utc).date().isoformat()


def _project_trade_date(value: int) -> str:
    instant = datetime.fromtimestamp(value // 1_000_000_000, timezone.utc).astimezone(
        PROJECT_ZONE
    )
    day = instant.date() + timedelta(days=1) if instant.time() >= time(17) else instant.date()
    return day.isoformat()


def _warning_count(manifest: Mapping[str, Any]) -> int:
    warnings = manifest.get("provider_warnings")
    if not isinstance(warnings, list):
        raise IntegrityError("acquisition warning inventory is absent")
    total = 0
    for item in warnings:
        if not isinstance(item, Mapping) or type(item.get("count")) is not int:
            raise IntegrityError("acquisition warning entry is invalid")
        total += int(item["count"])
    return total


def _partial_inventory(root: Path) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    raw = root / ACQUISITION_ROOT / "raw"
    for path in iter_plain_files(raw):
        if ".dbn.zst.p." not in path.name:
            continue
        result.append(
            {
                "path": _relative(root, path),
                "sha256": sha256_file(path),
                "size_bytes": path.stat().st_size,
            }
        )
    return sorted(result, key=lambda item: item["path"])


def _request_entries(root: Path, manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    staged = manifest.get("staged_files")
    if not isinstance(staged, list) or len(staged) != 406:
        raise IntegrityError("sealed acquisition does not contain 406 staged files")
    entries: list[dict[str, Any]] = []
    for item in staged:
        if not isinstance(item, Mapping) or not isinstance(item.get("request"), Mapping):
            raise IntegrityError("staged file entry is invalid")
        request = dict(item["request"])
        query = request.get("query")
        if not isinstance(query, Mapping):
            raise IntegrityError("staged request query is absent")
        raw = root / str(item["raw_relative_path"])
        sidecar = Path(f"{raw}.manifest.json")
        family = str(request["family"])
        directory = str(request["directory"])
        market = str(request["market"])
        destination = DBN_ROOT / directory / market / "2026" / raw.name
        if raw.stat().st_size != item.get("raw_size") or sha256_file(raw) != item.get(
            "raw_sha256"
        ):
            raise IntegrityError(f"staged DBN differs from manifest: {raw}")
        entries.append(
            {
                "destination_dbn": destination.as_posix(),
                "destination_sidecar": f"{destination.as_posix()}.manifest.json",
                "family": family,
                "market": market,
                "query": dict(query),
                "request_id": str(request["request_id"]),
                "staging_dbn": _relative(root, raw),
                "staging_dbn_sha256": str(item["raw_sha256"]),
                "staging_dbn_size": int(item["raw_size"]),
                "staging_sidecar": _relative(root, sidecar),
                "staging_sidecar_sha256": sha256_file(sidecar),
                "staging_sidecar_size": sidecar.stat().st_size,
            }
        )
    entries.sort(key=lambda item: item["request_id"])
    destinations = [item["destination_dbn"] for item in entries]
    if len(destinations) != len(set(destinations)):
        raise IntegrityError("canonical destination ledger is not unique")
    conflicts = [path for path in destinations if (root / path).exists()]
    if conflicts:
        raise IntegrityError(f"canonical destination already exists: {conflicts[0]}")
    return entries


def build_plan(root: Path) -> dict[str, Any]:
    """Freeze the exact provider-free audit plan without opening DBN rows."""

    root = root.resolve(strict=True)
    require_locked_repository_environment(root)
    verification = verify_completed_acquisition(
        root=root, authorization_receipt_id=ACQUISITION_RECEIPT_ID
    )
    if verification.get("request_count") != 406:
        raise IntegrityError("provider-free acquisition verifier did not confirm 406 requests")
    manifest_path = root / ACQUISITION_ROOT / "manifest.json"
    manifest = _json(manifest_path, "sealed acquisition manifest")
    pointer_path = root / ACTIVE_POINTER
    contract_path = root / ACTIVE_SOURCE_CONTRACT
    contract = _json(contract_path, "active source contract")
    inventory_path = root / str(contract["complete_inventory"]["path"])
    if (
        contract["complete_inventory"].get("file_count") != 11_474
        or contract["active_canonical_source"].get("complete_root_file_count") != 11_474
        or contract_path.read_bytes() != (root / PREDECESSOR_SOURCE_CONTRACT).read_bytes()
    ):
        raise IntegrityError("active canonical predecessor is not the exact v4 custody state")
    entries = _request_entries(root, manifest)
    partials = _partial_inventory(root)
    if (
        manifest.get("authorization_receipt_id") != ACQUISITION_RECEIPT_ID
        or manifest.get("state") != "SEALED_PROVIDER_CUSTODY_NOT_ROW_QUALITY_CERTIFIED"
        or manifest.get("nonempty_request_count") != 406
        or manifest.get("empty_request_count") != 0
        or len(manifest.get("provider_warnings", ())) != 406
        or _warning_count(manifest) != 431
        or len(partials) != 25
    ):
        raise IntegrityError("sealed acquisition counts differ from the approved audit scope")
    core = {
        "schema_version": "l0_cutoff_alignment_acceptance_plan/1.0.0",
        "state": "PREPARED_PROVIDER_FREE_FORWARD_ROW_AUDIT_AND_CONDITIONAL_PUBLICATION",
        "receipt_id": ACQUISITION_RECEIPT_ID,
        "interval": {"start_inclusive_utc": START, "end_exclusive_utc": END},
        "bindings": {
            "acquisition_plan_path": ACQUISITION_PLAN.as_posix(),
            "acquisition_plan_sha256": sha256_file(root / ACQUISITION_PLAN),
            "acquisition_manifest_path": _relative(root, manifest_path),
            "acquisition_manifest_sha256": sha256_file(manifest_path),
            "active_pointer_path": ACTIVE_POINTER.as_posix(),
            "active_pointer_sha256": sha256_file(pointer_path),
            "active_release_id": str(contract["active_canonical_source"]["release_id"]),
            "source_contract_path": ACTIVE_SOURCE_CONTRACT.as_posix(),
            "source_contract_sha256": sha256_file(contract_path),
            "source_contract_id": str(contract["contract_id"]),
            "predecessor_source_contract_path": PREDECESSOR_SOURCE_CONTRACT.as_posix(),
            "predecessor_source_contract_sha256": sha256_file(
                root / PREDECESSOR_SOURCE_CONTRACT
            ),
            "complete_inventory_path": _relative(root, inventory_path),
            "complete_inventory_sha256": sha256_file(inventory_path),
            "acquisition_staging_summary": _inventory_summary(
                _inventory_rows(root, root / ACQUISITION_ROOT)
            ),
            "data_active_summary": _inventory_summary(
                _inventory_rows(root, root / "data/active")
            ),
            "active_alpha_pointer_path": ACTIVE_ALPHA_POINTER.as_posix(),
            "active_alpha_pointer_sha256": sha256_file(root / ACTIVE_ALPHA_POINTER),
            "implementation_sha256": _implementation_sha(root),
        },
        "counts": {
            "requests": 406,
            "markets": 58,
            "families": 7,
            "final_warning_records": 406,
            "preserved_retry_partials": 25,
            "historical_warning_occurrences_reconciled": 431,
            "predecessor_files": 11_474,
            "publication_dbns": 406,
            "publication_sidecars": 406,
            "successor_files": 12_286,
        },
        "requests": entries,
        "retry_partials": partials,
        "authority": {
            "provider_access": False,
            "credential_access": False,
            "forward_row_structural_audit": True,
            "price_or_volume_persistence": False,
            "feature_or_return_construction": False,
            "model_fit_prediction_or_evaluation": False,
            "research_activation": False,
            "trading_or_order_access": False,
            "conditional_canonical_custody_publication": True,
        },
    }
    plan = {**core, "plan_id": sha256_json(core)}
    path = root / ACCEPTANCE_ROOT / str(plan["plan_id"]) / "plan.json"
    write_new_canonical_authority(path, plan)
    return {**plan, "plan_path": _relative(root, path)}


def _load_plan(root: Path, plan_path: Path) -> dict[str, Any]:
    plan = read_canonical_authority(plan_path)
    plan_id = plan.get("plan_id")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    if plan_id != sha256_json(core):
        raise IntegrityError("acceptance plan identity differs")
    bindings = plan.get("bindings")
    if not isinstance(bindings, Mapping) or bindings.get(
        "implementation_sha256"
    ) != _implementation_sha(root):
        raise IntegrityError("acceptance implementation drifted")
    return plan


def audit_scope(plan: Mapping[str, Any], plan_sha256: str) -> dict[str, str]:
    base = {
        "receipt_id": str(plan["receipt_id"]),
        "request_count": str(plan["counts"]["requests"]),
        "interval": f"{START}/{END}",
        "provider_access": "false",
        "credential_access": "false",
        "holdout_or_forward_row_read": "true",
        "structural_aggregates_only": "true",
        "model_fit_prediction_or_evaluation": "false",
        "research_activation": "false",
        "trading_or_order_access": "false",
        "canonical_mutation_during_audit": "false",
    }
    return {
        **base,
        "approval_command": AUDIT_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": plan_sha256,
    }


def _scan_dbn(path: Path, entry: Mapping[str, Any]) -> dict[str, Any]:
    if databento.__version__ != SUPPORTED_DATABENTO_VERSION:
        raise ContractError("offline Databento decoder version differs")
    store = databento.DBNStore.from_file(path)
    metadata = store.metadata
    query = entry["query"]
    schema = _normalize_schema(metadata.schema)
    expected_schema = str(query["schema"])
    if (
        str(metadata.dataset) != "GLBX.MDP3"
        or schema != expected_schema
        or _normalize_schema(store.schema) != expected_schema
        or str(metadata.stype_in) != str(query["stype_in"])
        or str(metadata.stype_out) != "instrument_id"
        or list(metadata.symbols) != list(query["symbols"])
        or int(metadata.start) != START_NS
        or int(metadata.end) != END_NS
    ):
        raise IntegrityError(f"DBN metadata differs from request: {path}")
    coverage_field = "ts_recv" if schema in RECEIVE_TIME_COVERAGE_SCHEMAS else "ts_event"
    count = 0
    first: int | None = None
    last: int | None = None
    prior: int | None = None
    utc_counts: Counter[str] = Counter()
    trade_counts: Counter[str] = Counter()
    try:
        for array in _iter_arrays(store, scan_to_end=True, sample_records=1):
            names = set(getattr(getattr(array, "dtype", None), "names", None) or ())
            required = {"publisher_id", "instrument_id", "ts_event", coverage_field}
            if not required <= names:
                raise IntegrityError(f"DBN record schema lacks required fields: {path}")
            for record in array:
                if (
                    int(record["publisher_id"]) <= 0
                    or int(record["instrument_id"]) <= 0
                    or int(record["ts_event"]) <= 0
                ):
                    raise IntegrityError(f"DBN record identity is invalid: {path}")
                value = int(record[coverage_field])
                if not START_NS <= value < END_NS:
                    raise IntegrityError(f"DBN timestamp lies outside the declared interval: {path}")
                if prior is not None and value < prior:
                    raise IntegrityError(f"DBN coverage timestamps are not nondecreasing: {path}")
                first = value if first is None else first
                last = value
                prior = value
                count += 1
                utc_counts[_utc_date(value)] += 1
                if schema in OHLCV_FAMILIES:
                    trade_counts[_project_trade_date(value)] += 1
    except (NotImplementedError, TypeError, ValueError) as exc:
        raise IntegrityError(f"DBN cannot be decoded to EOF: {path}") from exc
    finally:
        del store
        gc.collect()
    if count <= 0 or first is None or last is None:
        raise IntegrityError(f"DBN contains no decodable records: {path}")
    return {
        "coverage_timestamp_field": coverage_field,
        "first_timestamp_utc": _timestamp_text(first),
        "last_timestamp_utc": _timestamp_text(last),
        "record_count": count,
        "schema": schema,
        "utc_date_counts": dict(sorted(utc_counts.items())),
        "project_trade_date_counts": dict(sorted(trade_counts.items())),
    }


def _validate_ohlcv_dates(results: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_market: dict[str, dict[str, set[str]]] = {}
    for result in results:
        family = str(result["family"])
        if family not in OHLCV_FAMILIES:
            continue
        dates = set(result["utc_date_counts"])
        by_market.setdefault(str(result["market"]), {})[family] = dates
    mismatches: list[dict[str, Any]] = []
    for market, families in sorted(by_market.items()):
        if set(families) != set(OHLCV_FAMILIES):
            mismatches.append({"market": market, "reason": "missing_resolution"})
            continue
        union = set().union(*families.values())
        if any(values != union for values in families.values()):
            mismatches.append(
                {
                    "market": market,
                    "reason": "utc_observation_date_set_mismatch",
                    "missing_by_family": {
                        family: sorted(union - values)
                        for family, values in sorted(families.items())
                        if values != union
                    },
                }
            )
    return mismatches


def _write_audit_failure(
    root: Path, plan: Mapping[str, Any], error: BaseException, completed: int
) -> None:
    payload = {
        "schema_version": "l0_cutoff_alignment_row_audit_failure/1.0.0",
        "status": "FAIL_CLOSED_NO_CANONICAL_MUTATION",
        "plan_id": plan["plan_id"],
        "completed_request_count": completed,
        "error_type": type(error).__name__,
        "provider_calls": 0,
        "credential_reads": 0,
        "canonical_mutation": False,
    }
    path = root / ACCEPTANCE_ROOT / str(plan["plan_id"]) / "audit_failure.json"
    write_new_canonical_authority(path, payload)


def run_row_coverage_audit(
    root: Path, plan_path: Path, authorization: OperationReceipt
) -> dict[str, Any]:
    """Consume bounded authority and scan all 406 staged DBNs to EOF."""

    root = root.resolve(strict=True)
    plan = _load_plan(root, plan_path.resolve(strict=True))
    plan_sha = sha256_file(plan_path)
    scope = audit_scope(plan, plan_sha)
    boundary = RepoBoundary(root)
    output = root / ACCEPTANCE_ROOT / str(plan["plan_id"]) / "coverage_audit.json"
    if output.exists():
        authorization.assert_consumed(
            boundary,
            operation=AUDIT_OPERATION,
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            required_scope=scope,
        )
        return read_canonical_authority(output)
    current = plan["bindings"]
    for key, relative in (
        ("acquisition_manifest_sha256", ACQUISITION_ROOT / "manifest.json"),
        ("active_pointer_sha256", ACTIVE_POINTER),
        ("source_contract_sha256", ACTIVE_SOURCE_CONTRACT),
        ("complete_inventory_sha256", Path(str(current["complete_inventory_path"]))),
    ):
        if sha256_file(root / relative) != current[key]:
            raise IntegrityError(f"audit predecessor binding drifted: {key}")
    verify_completed_acquisition(
        root=root, authorization_receipt_id=ACQUISITION_RECEIPT_ID
    )
    if _inventory_summary(_inventory_rows(root, root / ACQUISITION_ROOT)) != current[
        "acquisition_staging_summary"
    ]:
        raise IntegrityError("sealed acquisition staging changed before row audit")
    authorization.consume(
        boundary,
        operation=AUDIT_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    results: list[dict[str, Any]] = []
    try:
        for entry in plan["requests"]:
            path = root / str(entry["staging_dbn"])
            if path.stat().st_size != entry["staging_dbn_size"] or sha256_file(path) != entry[
                "staging_dbn_sha256"
            ]:
                raise IntegrityError(f"staged DBN drifted before row audit: {path}")
            scanned = _scan_dbn(path, entry)
            results.append(
                {
                    "family": entry["family"],
                    "market": entry["market"],
                    "request_id": entry["request_id"],
                    "staging_dbn_sha256": entry["staging_dbn_sha256"],
                    **scanned,
                    "warning_date_counts": {
                        day: {
                            "utc": scanned["utc_date_counts"].get(day, 0),
                            "project_trade_date": scanned["project_trade_date_counts"].get(
                                day, 0
                            ),
                        }
                        for day in WARNING_DATES
                    },
                }
            )
        scan_core = {
            "schema_version": "l0_cutoff_alignment_structural_scan/1.0.0",
            "status": "COMPLETE_PENDING_CROSS_RESOLUTION_ACCEPTANCE",
            "plan_id": plan["plan_id"],
            "request_count": len(results),
            "results": results,
            "cross_resolution_date_equality_basis": "UTC_OBSERVATION_DATE",
            "project_trading_date_counts_diagnostic_only": True,
            "provider_calls": 0,
            "credential_reads": 0,
            "canonical_mutation": False,
        }
        scan = {**scan_core, "scan_id": sha256_json(scan_core)}
        write_new_canonical_authority(output.with_name("structural_scan.json"), scan)
        mismatches = _validate_ohlcv_dates(results)
        if mismatches:
            write_new_canonical_authority(
                output.with_name("coverage_mismatch.json"),
                {
                    "schema_version": "l0_cutoff_alignment_coverage_mismatch/1.0.0",
                    "status": "FAIL_CLOSED_NO_CANONICAL_MUTATION",
                    "plan_id": plan["plan_id"],
                    "scan_id": scan["scan_id"],
                    "mismatches": mismatches,
                    "canonical_mutation": False,
                },
            )
            raise IntegrityError("OHLCV UTC observation-date coverage differs by resolution")
        if sha256_file(root / ACQUISITION_ROOT / "manifest.json") != current[
            "acquisition_manifest_sha256"
        ]:
            raise IntegrityError("sealed acquisition manifest changed during row audit")
        core = {
            "schema_version": "l0_cutoff_alignment_row_coverage_audit/1.0.0",
            "status": "PASS_STRUCTURAL_ROW_COVERAGE_PROVIDER_COMPLETENESS_NOT_CERTIFIED",
            "plan_id": plan["plan_id"],
            "plan_sha256": plan_sha,
            "authorization_receipt_id": authorization.receipt_id,
            "request_count": len(results),
            "total_record_count": sum(int(item["record_count"]) for item in results),
            "results": results,
            "warning_reconciliation": {
                "final_file_warning_records": 406,
                "preserved_retry_partials": 25,
                "historical_warning_occurrences_reconciled": 431,
                "warning_dates": list(WARNING_DATES),
            },
            "cross_resolution_date_equality_basis": "UTC_OBSERVATION_DATE",
            "project_trading_date_counts_diagnostic_only": True,
            "structural_row_coverage_certified": True,
            "provider_completeness_certified": False,
            "row_quality_certified": False,
            "holdout_or_forward_rows_read": True,
            "price_or_volume_values_persisted": False,
            "features_returns_models_or_evaluations_created": False,
            "canonical_mutation": False,
            "provider_calls": 0,
            "credential_reads": 0,
            "research_activation": False,
            "trading_or_order_access": False,
        }
        audit = {**core, "audit_id": sha256_json(core)}
        write_new_canonical_authority(output, audit)
        return audit
    except Exception as exc:
        _write_audit_failure(root, plan, exc, len(results))
        raise


def _copy_plain(source: Path, destination: Path) -> None:
    if destination.exists():
        raise IntegrityError(f"publication candidate destination exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with io_path(source).open("rb") as reader, io_path(destination).open("xb") as writer:
        shutil.copyfileobj(reader, writer, length=1_048_576)
        writer.flush()
        os.fsync(writer.fileno())
    if destination.stat().st_nlink != 1 or sha256_file(destination) != sha256_file(source):
        raise IntegrityError("publication candidate is not an exact plain-file copy")


def _inventory_rows(root: Path, path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for directory, directories, filenames in os.walk(path):
        directories.sort()
        for filename in sorted(filenames):
            item = Path(directory) / filename
            rows.append(
                {
                    "relative_path": item.relative_to(path).as_posix(),
                    "sha256": sha256_file(item, reject_hardlinks=False),
                    "size_bytes": item.stat().st_size,
                }
            )
    return sorted(rows, key=lambda item: item["relative_path"])


def _inventory_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    canonical = [
        {
            "relative_path": str(item["relative_path"]),
            "size_bytes": int(item["size_bytes"]),
            "sha256": str(item["sha256"]),
        }
        for item in rows
    ]
    canonical.sort(key=lambda item: item["relative_path"])
    return {
        "file_count": len(canonical),
        "total_bytes": sum(int(item["size_bytes"]) for item in canonical),
        "inventory_sha256": sha256_json(canonical),
    }


def _canonical_sidecar(
    entry: Mapping[str, Any], result: Mapping[str, Any], audit: Mapping[str, Any], audit_sha: str
) -> dict[str, Any]:
    return {
        "schema_version": "l0_cutoff_alignment_canonical_sidecar/1.0.0",
        "activation_status": "CANONICAL_CUSTODY_NOT_RESEARCH_ADMITTED",
        "canonical_dbn": {
            "copy_semantics": "EXACT_PLAIN_FILE_COPY_FROM_SEALED_STAGING",
            "project_relative_path": entry["destination_dbn"],
            "sha256": entry["staging_dbn_sha256"],
            "size_bytes": entry["staging_dbn_size"],
        },
        "coverage_interval": {
            "start_inclusive_utc": START,
            "end_exclusive_utc": END,
        },
        "dataset": "GLBX.MDP3",
        "family": entry["family"],
        "market": entry["market"],
        "year": 2026,
        "query": entry["query"],
        "structural_row_coverage": {
            "audit_id": audit["audit_id"],
            "audit_sha256": audit_sha,
            "first_timestamp_utc": result["first_timestamp_utc"],
            "last_timestamp_utc": result["last_timestamp_utc"],
            "record_count": result["record_count"],
            "provider_completeness_certified": False,
        },
        "input_provenance": {
            "acquisition_receipt_id": ACQUISITION_RECEIPT_ID,
            "staging_dbn_path": entry["staging_dbn"],
            "staging_dbn_sha256": entry["staging_dbn_sha256"],
            "staging_sidecar_path": entry["staging_sidecar"],
            "staging_sidecar_sha256": entry["staging_sidecar_sha256"],
        },
        "authority": {
            "admitted_standard_foundation": False,
            "research_activation": False,
            "holdout_or_forward_selection": False,
            "model_or_evaluation": False,
            "trading_or_order_access": False,
        },
    }


def _new_inventory_entries(
    plan: Mapping[str, Any], audit: Mapping[str, Any], payload: Path
) -> list[dict[str, Any]]:
    standard_roots = set(STANDARD_ROOTS)
    entries: list[dict[str, Any]] = []
    for item in plan["requests"]:
        family = str(item["destination_dbn"]).split("/")[2]
        lane = "STANDARD_41" if item["market"] in standard_roots else "DEFERRED_MICRO_17"
        for kind, relative in (
            ("DBN", str(item["destination_dbn"])),
            ("SIDECAR", str(item["destination_sidecar"])),
        ):
            candidate = payload / Path(relative).relative_to(DBN_ROOT)
            entries.append(
                {
                    "admitted_standard_foundation": False,
                    "family": family,
                    "identity_basis": (
                        "ADJACENT_CANONICAL_SIDECAR_DECLARATION"
                        if kind == "DBN"
                        else "SIDECAR_BYTES_HASHED_METADATA_ONLY"
                    ),
                    "interval_end_exclusive": "2026-09-01",
                    "interval_start_inclusive": "2026-07-14",
                    "kind": kind,
                    "lane": lane,
                    "market": item["market"],
                    "path": relative,
                    "sha256": sha256_file(candidate),
                    "sidecar_schema_version": "l0_cutoff_alignment_canonical_sidecar/1.0.0",
                    "size_bytes": candidate.stat().st_size,
                    "year": 2026,
                }
            )
    return sorted(entries, key=lambda item: item["path"])


def _require_publication_space(root: Path) -> None:
    if shutil.disk_usage(root).free < 1_000_000_000:
        raise IntegrityError("less than one gigabyte is free for publication")


def prepare_publication_candidate(
    root: Path, plan_path: Path, audit_path: Path
) -> dict[str, Any]:
    """Build an isolated canonical successor after a passing structural audit."""

    root = root.resolve(strict=True)
    plan = _load_plan(root, plan_path.resolve(strict=True))
    audit = read_canonical_authority(audit_path.resolve(strict=True))
    audit_sha = sha256_file(audit_path)
    if (
        audit.get("status")
        != "PASS_STRUCTURAL_ROW_COVERAGE_PROVIDER_COMPLETENESS_NOT_CERTIFIED"
        or audit.get("plan_id") != plan["plan_id"]
        or audit.get("request_count") != 406
        or audit.get("structural_row_coverage_certified") is not True
        or audit.get("provider_completeness_certified") is not False
    ):
        raise UnauthorizedOperation("publication requires the exact passing row audit")
    bindings = plan["bindings"]
    for key, relative in (
        ("active_pointer_sha256", ACTIVE_POINTER),
        ("source_contract_sha256", ACTIVE_SOURCE_CONTRACT),
        ("complete_inventory_sha256", Path(str(bindings["complete_inventory_path"]))),
    ):
        if sha256_file(root / relative) != bindings[key]:
            raise IntegrityError(f"publication predecessor drifted: {key}")
    basis = {
        "plan_id": plan["plan_id"],
        "audit_id": audit["audit_id"],
        "audit_sha256": audit_sha,
        "active_pointer_sha256": bindings["active_pointer_sha256"],
        "source_contract_sha256": bindings["source_contract_sha256"],
        "implementation_sha256": bindings["implementation_sha256"],
    }
    run_id = "l0cutpub_" + sha256_json(basis)[:32]
    report = root / PUBLICATION_REPORT_ROOT / run_id
    payload = root / PUBLICATION_STAGE_ROOT / run_id / "candidate_additions/data/dbn"
    plan_output = report / "publication_plan.json"
    if plan_output.exists():
        publication = _load_publication_plan(root, plan_output)
        return {**publication, "publication_plan_path": _relative(root, plan_output)}
    if report.exists() or payload.exists():
        raise IntegrityError("partial publication candidate already exists")
    report.mkdir(parents=True, exist_ok=False)
    result_by_id = {str(item["request_id"]): item for item in audit["results"]}
    try:
        _require_publication_space(root)
        for entry in plan["requests"]:
            if (root / str(entry["destination_dbn"])).exists() or (
                root / str(entry["destination_sidecar"])
            ).exists():
                raise IntegrityError("create-only canonical destination appeared")
        for entry in plan["requests"]:
            source = root / str(entry["staging_dbn"])
            destination = payload / Path(str(entry["destination_dbn"])).relative_to(DBN_ROOT)
            _copy_plain(source, destination)
            sidecar = _canonical_sidecar(
                entry, result_by_id[str(entry["request_id"])], audit, audit_sha
            )
            write_new_canonical_authority(Path(f"{destination}.manifest.json"), sidecar)
        candidate_rows = _inventory_rows(root, payload)
        candidate_summary = _inventory_summary(candidate_rows)
        if candidate_summary["file_count"] != 812:
            raise IntegrityError("publication candidate does not contain 812 files")
        prior_contract = _json(root / ACTIVE_SOURCE_CONTRACT, "active source contract")
        prior_inventory_path = root / str(prior_contract["complete_inventory"]["path"])
        prior_inventory = _json(prior_inventory_path, "predecessor complete inventory")
        prior_entries = prior_inventory.get("entries")
        if not isinstance(prior_entries, list) or len(prior_entries) != 11_474:
            raise IntegrityError("predecessor inventory does not contain 11,474 files")
        additions = _new_inventory_entries(
            {**plan, "plan_path": _relative(root, plan_path)}, audit, payload
        )
        all_entries = sorted([*map(dict, prior_entries), *additions], key=lambda item: item["path"])
        if len(all_entries) != 12_286 or len({item["path"] for item in all_entries}) != 12_286:
            raise IntegrityError("successor inventory path set differs")
        content_rows = [
            {
                "relative_path": str(item["path"])[len("data/dbn/") :],
                "size_bytes": int(item["size_bytes"]),
                "sha256": str(item["sha256"]),
            }
            for item in all_entries
        ]
        shadow = {
            "file_count": len(content_rows),
            "total_bytes": sum(int(item["size_bytes"]) for item in content_rows),
            "inventory_sha256": sha256_json(content_rows),
        }
        release_core = {
            "schema_version": "l0_cutoff_alignment_canonical_successor_core/1.0.0",
            "scope": "ADD_406_AUG31_CUTOFF_ALIGNMENT_DBN_PAIRS_AS_NON_RESEARCH_CUSTODY",
            "physical_publication_successor_to": bindings["active_release_id"],
            "acquisition_receipt_id": ACQUISITION_RECEIPT_ID,
            "audit_id": audit["audit_id"],
            "audit_sha256": audit_sha,
            "added_dbn_count": 406,
            "added_sidecar_count": 406,
            "successor_inventory_sha256": sha256_json(all_entries),
            "complete_shadow_tree": shadow,
            "research_admitted_addition_count": 0,
        }
        release_id = sha256_json(release_core)
        release = {
            "schema_version": "l0_cutoff_alignment_canonical_successor_release/1.0.0",
            "release_id": release_id,
            "release_status": "CERTIFIED_NON_ACTIVE_REQUIRES_BOUND_PUBLICATION",
            "publication_authorized": False,
            "activation_authorized": False,
            "release_core": release_core,
            "complete_shadow_tree": shadow,
            "successor_inventory": all_entries,
        }
        release_path = report / "successor_release_manifest.json"
        write_new_canonical_authority(release_path, release)
        inventory_core = {
            "schema_version": "canonical_dbn_complete_inventory/1.0.0",
            "status": "PASS_COMPLETE_IMMUTABLE_CANONICAL_SOURCE_SUCCESSOR",
            "active_release_id": release_id,
            "content_inventory_sha256": shadow["inventory_sha256"],
            "entries": all_entries,
            "file_count": shadow["file_count"],
            "total_bytes": shadow["total_bytes"],
        }
        inventory = {**inventory_core, "inventory_id": sha256_json(inventory_core)}
        inventory_path = report / "canonical_inventory.json"
        write_new_canonical_authority(inventory_path, inventory)
        wrapper = {
            "schema_version": "canonical_data_dbn_release_wrapper/2.0.0",
            "status": "IMMUTABLE_SUCCESSOR_WRAPPER",
            "release_id": release_id,
            "release_manifest_path": _relative(root, release_path),
            "release_manifest_sha256": sha256_file(release_path),
        }
        wrapper_path = report / "active_wrapper_v2.json"
        write_new_canonical_authority(wrapper_path, wrapper)
        final_wrapper = (
            Path("state/dbn_congruence_release_registry")
            / release_id
            / "active_wrapper_v2.json"
        )
        pointer_core = {
            "activated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "canonical_artifact_root": DBN_ROOT.as_posix(),
            "physical_publication_successor_to": bindings["active_release_id"],
            "release_id": release_id,
            "release_manifest_path": _relative(root, release_path),
            "release_manifest_sha256": sha256_file(release_path),
            "schema_version": "canonical_data_dbn_active_pointer/2.0.0",
            "scope": release_core["scope"],
            "status": "ACTIVE",
            "wrapper_path": final_wrapper.as_posix(),
            "wrapper_sha256": sha256_file(wrapper_path),
        }
        pointer = {**pointer_core, "pointer_id": sha256_json(pointer_core)}
        pointer_path = report / "active_pointer.json"
        write_new_canonical_authority(pointer_path, pointer)
        final_inventory = (
            Path("state/source_contract_registry") / release_id / "canonical_inventory.json"
        )
        successor_contract = dict(prior_contract)
        successor_contract["active_canonical_source"] = {
            "canonical_root": DBN_ROOT.as_posix(),
            "complete_root_file_count": shadow["file_count"],
            "complete_root_total_bytes": shadow["total_bytes"],
            "pointer_id": pointer["pointer_id"],
            "pointer_path": ACTIVE_POINTER.as_posix(),
            "pointer_sha256": sha256_file(pointer_path),
            "release_id": release_id,
            "release_manifest_path": _relative(root, release_path),
            "release_manifest_sha256": sha256_file(release_path),
        }
        successor_contract["complete_inventory"] = {
            "content_inventory_sha256": shadow["inventory_sha256"],
            "file_count": shadow["file_count"],
            "inventory_id": inventory["inventory_id"],
            "path": final_inventory.as_posix(),
            "sha256": sha256_file(inventory_path),
            "size_bytes": inventory_path.stat().st_size,
            "total_bytes": shadow["total_bytes"],
        }
        counts = dict(prior_contract["source_family_dbn_counts"])
        for family in ("definition", "status", "statistics", "ohlcv_1m", "ohlcv_1s", "ohlcv_1h", "ohlcv_1d"):
            counts[family] = int(counts[family]) + 58
        successor_contract["source_family_dbn_counts"] = counts
        successor_contract.pop("contract_id", None)
        successor_contract["contract_id"] = sha256_json(successor_contract)
        source_path = report / "source_contract_v5.json"
        write_new_canonical_authority(source_path, successor_contract)
        rollback = {
            "schema_version": "l0_cutoff_alignment_publication_rollback/1.0.0",
            "prior_pointer_sha256": bindings["active_pointer_sha256"],
            "prior_source_contract_sha256": bindings["source_contract_sha256"],
            "added_paths": [
                path
                for item in plan["requests"]
                for path in (item["destination_dbn"], item["destination_sidecar"])
            ],
            "failure_policy": "RESTORE_POINTER_AND_SOURCE_ALIAS_THEN_MOVE_ONLY_NEW_FILES_BACK_TO_STAGING",
            "predecessor_files_preserved": 11_474,
        }
        rollback_path = report / "rollback_plan.json"
        write_new_canonical_authority(rollback_path, rollback)
        publication_core = {
            "schema_version": "l0_cutoff_alignment_publication_plan/1.0.0",
            "state": "CERTIFIED_CONDITIONAL_PUBLICATION_AUTHORIZED_BY_CURRENT_USER_REQUEST",
            "run_id": run_id,
            "plan_id": plan["plan_id"],
            "audit_id": audit["audit_id"],
            "audit_sha256": audit_sha,
            "prior_active_pointer_sha256": bindings["active_pointer_sha256"],
            "prior_source_contract_sha256": bindings["source_contract_sha256"],
            "predecessor_source_contract_sha256": bindings[
                "predecessor_source_contract_sha256"
            ],
            "successor_release_id": release_id,
            "candidate_payload_root": _relative(root, payload),
            "candidate_summary": candidate_summary,
            "successor_summary": shadow,
            "release_manifest_path": _relative(root, release_path),
            "release_manifest_sha256": sha256_file(release_path),
            "inventory_candidate_path": _relative(root, inventory_path),
            "inventory_candidate_sha256": sha256_file(inventory_path),
            "wrapper_candidate_path": _relative(root, wrapper_path),
            "wrapper_candidate_sha256": sha256_file(wrapper_path),
            "pointer_candidate_path": _relative(root, pointer_path),
            "pointer_candidate_sha256": sha256_file(pointer_path),
            "source_contract_candidate_path": _relative(root, source_path),
            "source_contract_candidate_sha256": sha256_file(source_path),
            "rollback_plan_path": _relative(root, rollback_path),
            "rollback_plan_sha256": sha256_file(rollback_path),
            "targets": [
                {
                    "candidate_dbn": _relative(
                        root,
                        payload / Path(str(item["destination_dbn"])).relative_to(DBN_ROOT),
                    ),
                    "candidate_sidecar": _relative(
                        root,
                        payload / Path(str(item["destination_sidecar"])).relative_to(DBN_ROOT),
                    ),
                    "destination_dbn": item["destination_dbn"],
                    "destination_sidecar": item["destination_sidecar"],
                    "dbn_sha256": item["staging_dbn_sha256"],
                    "sidecar_sha256": sha256_file(
                        payload
                        / Path(str(item["destination_sidecar"])).relative_to(DBN_ROOT)
                    ),
                }
                for item in plan["requests"]
            ],
            "authority": {
                "provider_access": False,
                "credential_access": False,
                "research_activation": False,
                "holdout_or_forward_selection": False,
                "model_fit_prediction_or_evaluation": False,
                "trading_or_order_access": False,
                "canonical_custody_publication": True,
            },
        }
        publication = {**publication_core, "publication_plan_id": sha256_json(publication_core)}
        write_new_canonical_authority(plan_output, publication)
        return {**publication, "publication_plan_path": _relative(root, plan_output)}
    except Exception as exc:
        failure = report / "candidate_failure.json"
        if not failure.exists():
            write_new_canonical_authority(
                failure,
                {
                    "schema_version": "l0_cutoff_alignment_candidate_failure/1.0.0",
                    "status": "FAIL_CLOSED_NO_CANONICAL_MUTATION",
                    "run_id": run_id,
                    "error_type": type(exc).__name__,
                    "canonical_mutation": False,
                },
            )
        raise


def publication_scope(plan: Mapping[str, Any], plan_sha256: str) -> dict[str, str]:
    base = {
        "run_id": str(plan["run_id"]),
        "successor_release_id": str(plan["successor_release_id"]),
        "added_dbn_count": "406",
        "added_sidecar_count": "406",
        "successor_file_count": "12286",
        "provider_access": "false",
        "credential_access": "false",
        "research_activation": "false",
        "holdout_or_forward_selection": "false",
        "model_fit_prediction_or_evaluation": "false",
        "trading_or_order_access": "false",
        "canonical_custody_publication": "true",
    }
    return {
        **base,
        "approval_command": PUBLICATION_OPERATION,
        "approval_plan_id": str(plan["publication_plan_id"]),
        "approval_plan_sha256": plan_sha256,
    }


def _atomic_replace(path: Path, data: bytes, suffix: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{suffix}.tmp")
    if temporary.exists():
        raise IntegrityError(f"atomic replacement temporary exists: {temporary}")
    with io_path(temporary).open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(io_path(temporary), io_path(path))


def _write_authority_exact_or_new(path: Path, payload: Mapping[str, Any]) -> None:
    expected = canonical_authority_file_bytes(payload)
    if path.exists():
        if path.read_bytes() != expected:
            raise IntegrityError(f"existing authority bytes differ: {path}")
        return
    write_new_canonical_authority(path, payload)


def _load_publication_plan(
    root: Path, path: Path, *, candidate_required: bool = True
) -> dict[str, Any]:
    plan = read_canonical_authority(path)
    identity = plan.get("publication_plan_id")
    core = {key: value for key, value in plan.items() if key != "publication_plan_id"}
    if identity != sha256_json(core):
        raise IntegrityError("publication plan identity differs")
    for path_key, hash_key in (
        ("release_manifest_path", "release_manifest_sha256"),
        ("inventory_candidate_path", "inventory_candidate_sha256"),
        ("wrapper_candidate_path", "wrapper_candidate_sha256"),
        ("pointer_candidate_path", "pointer_candidate_sha256"),
        ("source_contract_candidate_path", "source_contract_candidate_sha256"),
        ("rollback_plan_path", "rollback_plan_sha256"),
    ):
        if sha256_file(root / str(plan[path_key])) != plan[hash_key]:
            raise IntegrityError(f"publication plan binding differs: {path_key}")
    if candidate_required:
        candidate_rows = _inventory_rows(root, root / str(plan["candidate_payload_root"]))
        if _inventory_summary(candidate_rows) != plan["candidate_summary"]:
            raise IntegrityError("publication candidate payload differs")
    return plan


def publish_authorized(
    root: Path,
    publication_plan_path: Path,
    authorization: OperationReceipt,
    *,
    failpoint: str | None = None,
) -> dict[str, Any]:
    """Install the exact certified custody successor with rollback."""

    root = root.resolve(strict=True)
    plan = _load_publication_plan(
        root, publication_plan_path.resolve(strict=True), candidate_required=False
    )
    scope = publication_scope(plan, sha256_file(publication_plan_path))
    boundary = RepoBoundary(root)
    pointer_path = root / ACTIVE_POINTER
    source_path = root / ACTIVE_SOURCE_CONTRACT
    if _json(pointer_path, "active pointer").get("release_id") == plan[
        "successor_release_id"
    ]:
        authorization.assert_consumed(
            boundary,
            operation=PUBLICATION_OPERATION,
            classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            required_scope=scope,
        )
        return {
            "status": "NO_ACTION_ALREADY_ACTIVE_SAME_RELEASE",
            "verification": verify_publication(root, publication_plan_path),
        }
    lock = SingleInstance(root / PUBLICATION_LOCK)
    if not lock.acquire():
        raise IntegrityError("L0 cutoff-alignment publication is already running")
    installed: list[tuple[Path, Path]] = []
    prior_pointer = pointer_path.read_bytes()
    prior_source = source_path.read_bytes()
    pointer_replaced = False
    source_replaced = False
    v5_created = False
    try:
        if (
            sha256_bytes(prior_pointer) != plan["prior_active_pointer_sha256"]
            or sha256_bytes(prior_source) != plan["prior_source_contract_sha256"]
            or sha256_file(root / PREDECESSOR_SOURCE_CONTRACT)
            != plan["predecessor_source_contract_sha256"]
        ):
            raise IntegrityError("publication predecessor control files drifted")
        for target in plan["targets"]:
            source_dbn = root / str(target["candidate_dbn"])
            source_sidecar = root / str(target["candidate_sidecar"])
            destination_dbn = root / str(target["destination_dbn"])
            destination_sidecar = root / str(target["destination_sidecar"])
            if destination_dbn.exists() or destination_sidecar.exists():
                raise IntegrityError("create-only canonical destination appeared")
            if (
                sha256_file(source_dbn) != target["dbn_sha256"]
                or source_dbn.stat().st_dev != destination_dbn.parent.stat().st_dev
            ):
                raise IntegrityError("publication candidate DBN differs or is on another volume")
            if sha256_file(source_sidecar) != target["sidecar_sha256"]:
                raise IntegrityError("publication candidate sidecar differs")
        authorization.consume(
            boundary,
            operation=PUBLICATION_OPERATION,
            classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            required_scope=scope,
        )
        archive = (
            root
            / "state/dbn_congruence_release_archive"
            / str(_json(pointer_path, "active pointer")["release_id"])
            / str(plan["run_id"])
        )
        archive.mkdir(parents=True, exist_ok=True)
        for archive_path, expected in (
            (archive / "prior_active_pointer.json", prior_pointer),
            (archive / "prior_source_contract.json", prior_source),
        ):
            if archive_path.exists() and archive_path.read_bytes() != expected:
                raise IntegrityError("publication rollback archive differs")
            if not archive_path.exists():
                archive_path.write_bytes(expected)
        if failpoint == "before_install":
            raise IntegrityError("injected failure before install")
        for index, target in enumerate(plan["targets"], start=1):
            for source_key, destination_key in (
                ("candidate_dbn", "destination_dbn"),
                ("candidate_sidecar", "destination_sidecar"),
            ):
                incoming = root / str(target[source_key])
                outgoing = root / str(target[destination_key])
                outgoing.parent.mkdir(parents=True, exist_ok=True)
                os.replace(io_path(incoming), io_path(outgoing))
                installed.append((outgoing, incoming))
            if failpoint == f"after_target_{index}":
                raise IntegrityError(f"injected failure after target {index}")
        release_id = str(plan["successor_release_id"])
        wrapper_destination = (
            root
            / "state/dbn_congruence_release_registry"
            / release_id
            / "active_wrapper_v2.json"
        )
        inventory_destination = (
            root
            / "state/source_contract_registry"
            / release_id
            / "canonical_inventory.json"
        )
        _write_authority_exact_or_new(
            wrapper_destination,
            read_canonical_authority(root / str(plan["wrapper_candidate_path"])),
        )
        _write_authority_exact_or_new(
            inventory_destination,
            read_canonical_authority(root / str(plan["inventory_candidate_path"])),
        )
        successor_source = (root / str(plan["source_contract_candidate_path"])).read_bytes()
        successor_pointer = (root / str(plan["pointer_candidate_path"])).read_bytes()
        if (root / SUCCESSOR_SOURCE_CONTRACT).exists():
            if (root / SUCCESSOR_SOURCE_CONTRACT).read_bytes() != successor_source:
                raise IntegrityError("source_contract_v5 already exists with different bytes")
        else:
            (root / SUCCESSOR_SOURCE_CONTRACT).write_bytes(successor_source)
            v5_created = True
        _atomic_replace(source_path, successor_source, str(plan["run_id"]))
        source_replaced = True
        if failpoint == "after_source_replace":
            raise IntegrityError("injected failure after source replace")
        if failpoint == "before_pointer_replace":
            raise IntegrityError("injected failure before pointer replace")
        _atomic_replace(pointer_path, successor_pointer, str(plan["run_id"]))
        pointer_replaced = True
        if failpoint == "after_pointer_replace":
            raise IntegrityError("injected failure after pointer replace")
        verification = verify_publication(root, publication_plan_path)
        receipt = {
            "schema_version": "l0_cutoff_alignment_publication_receipt/1.0.0",
            "status": "ACTIVATED_CANONICAL_CUSTODY_AND_VERIFIED",
            "run_id": plan["run_id"],
            "successor_release_id": release_id,
            "authorization_receipt_id": authorization.receipt_id,
            "installed_dbns": 406,
            "installed_sidecars": 406,
            "provider_calls": 0,
            "credential_reads": 0,
            "research_activation": False,
            "holdout_or_forward_selection": False,
            "model_fit_prediction_or_evaluation": False,
            "trading_or_order_access": False,
            "verification": verification,
        }
        receipt_path = publication_plan_path.parent / "publication_receipt.json"
        write_new_canonical_authority(receipt_path, receipt)
        return receipt
    except Exception:
        if pointer_replaced:
            _atomic_replace(pointer_path, prior_pointer, str(plan["run_id"]) + ".rollback")
        if source_replaced:
            _atomic_replace(source_path, prior_source, str(plan["run_id"]) + ".rollback")
        if v5_created:
            (root / SUCCESSOR_SOURCE_CONTRACT).unlink(missing_ok=True)
        for outgoing, incoming in reversed(installed):
            incoming.parent.mkdir(parents=True, exist_ok=True)
            if outgoing.exists():
                os.replace(io_path(outgoing), io_path(incoming))
        if (
            sha256_file(pointer_path) != plan["prior_active_pointer_sha256"]
            or sha256_file(source_path) != plan["prior_source_contract_sha256"]
        ):
            raise IntegrityError("publication rollback did not restore predecessor controls")
        failure = publication_plan_path.parent / "publication_failure.json"
        if not failure.exists():
            write_new_canonical_authority(
                failure,
                {
                    "schema_version": "l0_cutoff_alignment_publication_failure/1.0.0",
                    "status": "ROLLED_BACK_NO_ACTIVE_CANONICAL_MUTATION",
                    "run_id": plan["run_id"],
                    "failpoint": failpoint,
                },
            )
        raise
    finally:
        lock.release()


def verify_publication(root: Path, publication_plan_path: Path) -> dict[str, Any]:
    """Verify active controls, the exact 12,286-file inventory, and research exclusion."""

    root = root.resolve(strict=True)
    plan = _load_publication_plan(
        root, publication_plan_path.resolve(strict=True), candidate_required=False
    )
    pointer_candidate = root / str(plan["pointer_candidate_path"])
    source_candidate = root / str(plan["source_contract_candidate_path"])
    if (
        (root / ACTIVE_POINTER).read_bytes() != pointer_candidate.read_bytes()
        or (root / ACTIVE_SOURCE_CONTRACT).read_bytes() != source_candidate.read_bytes()
        or (root / SUCCESSOR_SOURCE_CONTRACT).read_bytes() != source_candidate.read_bytes()
        or sha256_file(root / PREDECESSOR_SOURCE_CONTRACT)
        != plan["predecessor_source_contract_sha256"]
    ):
        raise IntegrityError("active successor control files differ")
    contract = _json(root / ACTIVE_SOURCE_CONTRACT, "successor source contract")
    predecessor = _json(root / PREDECESSOR_SOURCE_CONTRACT, "predecessor source contract")
    if (
        contract.get("selection_policy") != predecessor.get("selection_policy")
        or contract.get("universe") != predecessor.get("universe")
        or contract.get("authority") != predecessor.get("authority")
    ):
        raise IntegrityError("successor changed research selection or authority")
    inventory_path = root / str(contract["complete_inventory"]["path"])
    inventory = _json(inventory_path, "active successor inventory")
    entries = inventory.get("entries")
    if not isinstance(entries, list) or len(entries) != 12_286:
        raise IntegrityError("active successor inventory count differs")
    new_entries = [
        item
        for item in entries
        if item.get("interval_start_inclusive") == "2026-07-14"
        and item.get("interval_end_exclusive") == "2026-09-01"
    ]
    if len(new_entries) != 812 or any(
        item.get("admitted_standard_foundation") is not False for item in new_entries
    ):
        raise IntegrityError("new canonical custody is research-admitted or incomplete")
    if any((root / str(item["path"])).stat().st_nlink != 1 for item in new_entries):
        raise IntegrityError("new canonical custody is not stored as plain files")
    observed = _inventory_rows(root, root / DBN_ROOT)
    expected = [
        {
            "relative_path": str(item["path"])[len("data/dbn/") :],
            "size_bytes": int(item["size_bytes"]),
            "sha256": str(item["sha256"]),
        }
        for item in entries
    ]
    if observed != expected:
        raise IntegrityError("complete canonical root differs from successor inventory")
    context = issue_current_source_closure_context(
        root, contract_path=root / ACTIVE_SOURCE_CONTRACT
    )
    closure = validate_source_closure(
        root,
        operation_context=context,
        contract_path=root / ACTIVE_SOURCE_CONTRACT,
    )
    acceptance_plan = root / ACCEPTANCE_ROOT / str(plan["plan_id"]) / "plan.json"
    acceptance = read_canonical_authority(acceptance_plan)
    for request in acceptance["requests"]:
        staged = root / str(request["staging_dbn"])
        if (
            staged.stat().st_size != request["staging_dbn_size"]
            or sha256_file(staged) != request["staging_dbn_sha256"]
        ):
            raise IntegrityError("sealed staging changed after canonical publication")
    bindings = acceptance["bindings"]
    if (
        _inventory_summary(_inventory_rows(root, root / ACQUISITION_ROOT))
        != bindings["acquisition_staging_summary"]
        or _inventory_summary(_inventory_rows(root, root / "data/active"))
        != bindings["data_active_summary"]
        or sha256_file(root / ACTIVE_ALPHA_POINTER)
        != bindings["active_alpha_pointer_sha256"]
    ):
        raise IntegrityError("sealed staging or research authority changed")
    return {
        "status": "PASS_ACTIVE_CANONICAL_CUSTODY_SUCCESSOR",
        "release_id": plan["successor_release_id"],
        "canonical_file_count": len(observed),
        "canonical_total_bytes": sum(item["size_bytes"] for item in observed),
        "new_dbn_count": 406,
        "new_sidecar_count": 406,
        "predecessor_file_count": 11_474,
        "new_research_admitted_count": 0,
        "source_closure": closure,
        "sealed_staging_unchanged": True,
        "provider_calls": 0,
        "credential_reads": 0,
        "research_activation": False,
        "holdout_or_forward_selection": False,
        "model_fit_prediction_or_evaluation": False,
        "trading_or_order_access": False,
    }
