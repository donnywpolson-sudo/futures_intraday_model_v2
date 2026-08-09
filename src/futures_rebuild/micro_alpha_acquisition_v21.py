"""V21-bound annual Apex micro Phase 1A inactive-custody acquisition.

The passing metadata report and every predecessor it binds remain immutable.
This successor freezes and can stream only the exact annual DBN requests from
that report.  It never imports a DBN decoder or iterates downloaded records.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .micro_alpha_acquisition import (
    DownloadProviderApis,
    build_file_download_provider_apis,
)
from .micro_alpha_databento_preflight import CREDENTIAL_SOURCE
from .micro_alpha_databento_preflight_v21 import (
    PLAN_PATH as PREFLIGHT_PLAN_PATH,
    REPORT_PATH as PREFLIGHT_REPORT_PATH,
)
from .micro_alpha_pipeline import (
    CURRENT_ACQUISITION_MARKETS,
    DATASET,
    SCHEMAS,
    annual_market_year_intervals,
    build_product_reference_requirements,
    phase1a_paths,
    validate_annual_market_year_interval,
    validate_phase1a_request,
    validate_product_effective_date,
)
from .runtime_environment import require_locked_repository_environment


PLAN_PATH: Final = Path(
    "configs/apex_micro_tier01_phase1a_acquisition_plan_v21.json"
)
AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v21/audit.json"
)
OPERATION: Final = "ACQUIRE_APEX_MICRO_TIER01_RAW_DBN_INACTIVE_CUSTODY_V21_ONCE"
STAGING_ROOT: Final = Path(
    "state/provider_acquisition_staging/apex_micro_tier01_v21"
)
PREFLIGHT_PLAN_ID: Final = (
    "2f3aca8a4775dfc3a10b29a5854b655ef04f5339a1c52e87297e8ebad227124c"
)
PREFLIGHT_PLAN_SHA256: Final = (
    "34f83ec5ae8bb7da819e174703b2dacba77fa5dde5eea2ecf3448025d8516c8d"
)
PREFLIGHT_REPORT_ID: Final = (
    "fc5ec7340c25617a3158ff7b19f555ba961586de6bd7b19dc55895e15c70516e"
)
PREFLIGHT_REPORT_SHA256: Final = (
    "5a9c68a00431ba3e1c16cad44f9e65afe19933cb2db2f2f1c584e21a5f17cd68"
)
PREFLIGHT_AUTHORIZATION_RECEIPT_ID: Final = (
    "bf720c94e7307379dbbf4bce5e482c5e3f452d2718009d1d26422fbd6256cc40"
)
PREFLIGHT_AUTHORIZATION_PATH: Final = Path("state/authorization_uses") / (
    f"{PREFLIGHT_AUTHORIZATION_RECEIPT_ID}.json"
)
PREFLIGHT_AUTHORIZATION_SHA256: Final = (
    "0b78b66394396ee1846235fca0abb4f11a1c3dc91a5ad86216d86c8d9fc31465"
)
STANDARD_TOPOLOGY_PATH: Final = Path(
    "state/unpublished_evidence/standard_data_topology_source_safe_audit/report.json"
)
CLEANUP_CENSUS_PATH: Final = Path(
    "state/unpublished_evidence/safe_cleanup_candidate_census_v6/census.json"
)
MAXIMUM_RUNTIME_SECONDS: Final = 7200
MAXIMUM_PER_DOWNLOAD_SECONDS: Final = 900
MAXIMUM_RETRIES: Final = 0
MAXIMUM_DBN_FILES: Final = 160
MAXIMUM_SIDECARS: Final = 160
MAXIMUM_PARALLEL_DOWNLOADS: Final = 2
MAXIMUM_PROVIDER_CLIENTS: Final = 1 + MAXIMUM_PARALLEL_DOWNLOADS
MAXIMUM_TOTAL_ACQUISITION_BYTES: Final = 128 * 1024**3
DISK_SAFETY_BYTES: Final = 1024**3


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{description} is not an object")
    return value


def _git_head(root: Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    head = completed.stdout.strip()
    if len(head) != 40:
        raise IntegrityError("committed implementation HEAD is invalid")
    return head


def _prelaunch_records(product_dates: Mapping[str, object]) -> dict[str, list[dict[str, object]]]:
    records: dict[str, list[dict[str, object]]] = {}
    for market in CURRENT_ACQUISITION_MARKETS:
        effective = str(product_dates[market])
        market_records: list[dict[str, object]] = []
        for year in range(2018, int(effective[:4]) + 1):
            start = f"{year:04d}-01-01"
            end = min(f"{year + 1:04d}-01-01", effective)
            if start < end:
                market_records.append(
                    {
                        "year": year,
                        "start": start,
                        "end_exclusive": end,
                        "disposition": "PRODUCT_PRELAUNCH_NO_DBN_FABRICATED",
                    }
                )
        records[market] = market_records
    return records


def _validate_preflight_evidence(*, root: Path) -> tuple[dict[str, object], dict[str, object]]:
    """Verify the sealed v21 plan, PASS report, and consumed authorization."""

    plan_path = root / PREFLIGHT_PLAN_PATH
    report_path = root / PREFLIGHT_REPORT_PATH
    authorization_path = root / PREFLIGHT_AUTHORIZATION_PATH
    if (
        sha256_file(plan_path) != PREFLIGHT_PLAN_SHA256
        or sha256_file(report_path) != PREFLIGHT_REPORT_SHA256
        or sha256_file(authorization_path) != PREFLIGHT_AUTHORIZATION_SHA256
    ):
        raise IntegrityError("sealed v21 metadata evidence bytes drifted")
    plan = _object(plan_path, "v21 metadata plan")
    report = _object(report_path, "v21 metadata report")
    authorization = _object(authorization_path, "v21 metadata authorization use")
    report_core = dict(report)
    report_id = report_core.pop("report_id", None)
    requests = plan.get("requests")
    cumulative = report.get("cumulative_metadata")
    product_dates = report.get("product_effective_dates")
    annual_count = report.get("annual_market_schema_request_count")
    if (
        plan.get("plan_id") != PREFLIGHT_PLAN_ID
        or report_id != PREFLIGHT_REPORT_ID
        or report_id != sha256_json(report_core)
        or report.get("state") != "PASS_METADATA_ONLY"
        or report.get("plan_id") != PREFLIGHT_PLAN_ID
        or report.get("plan_sha256") != PREFLIGHT_PLAN_SHA256
        or report.get("authorization_receipt_id")
        != PREFLIGHT_AUTHORIZATION_RECEIPT_ID
        or authorization.get("receipt_id") != PREFLIGHT_AUTHORIZATION_RECEIPT_ID
        or authorization.get("operation")
        != "PREFLIGHT_APEX_MICRO_TIER01_DATABENTO_METADATA_ONCE"
        or not isinstance(requests, list)
        or len(requests) != 20
        or annual_count != MAXIMUM_DBN_FILES
        or report.get("maximum_annual_market_schema_requests") != MAXIMUM_DBN_FILES
        or report.get("request_definition_count") != 20
        or report.get("provider_call_counts")
        != {"get_billable_size": MAXIMUM_DBN_FILES, "get_cost": 20}
        or report.get("provider_call_total") != 180
        or report.get("maximum_external_cost_usd") != "0"
        or report.get("external_cost_incurred_usd") != "0"
        or report.get("automatic_retries") != 0
        or report.get("timeseries_download_calls") != 0
        or report.get("historical_rows_read") is not False
        or report.get("dbn_files_created") != 0
        or report.get("credential_content_recorded") is not False
        or report.get("destination_conflict_count") != 0
        or report.get("catalog_activated") is not False
        or report.get("published") is not False
        or report.get("registered") is not False
        or report.get("trading") is not False
        or report.get("file_partition")
        != "ONE_DBN_AND_ADJACENT_SIDECAR_PER_MARKET_SCHEMA_CALENDAR_YEAR"
        or not isinstance(cumulative, Mapping)
        or cumulative.get("dataset_entitlement") != "PASS_FROM_SEALED_V19"
        or cumulative.get("required_schema_entitlement") != "PASS_FROM_SEALED_V19"
        or cumulative.get("dataset_range") != "PASS_FROM_SEALED_V19"
        or cumulative.get("symbology_availability_and_continuity")
        != "PASS_FROM_SEALED_V19"
        or not isinstance(product_dates, Mapping)
        or set(product_dates) != set(CURRENT_ACQUISITION_MARKETS)
    ):
        raise UnauthorizedOperation("passing sealed v21 metadata evidence is absent or drifted")
    end_exclusive = cumulative.get("latest_complete_end_exclusive")
    provider_start = cumulative.get("provider_dataset_start_date")
    schema_starts = cumulative.get("provider_schema_start_dates")
    if (
        type(end_exclusive) is not str
        or len(end_exclusive) != 10
        or type(provider_start) is not str
        or len(provider_start) != 10
        or not isinstance(schema_starts, Mapping)
        or set(schema_starts) != set(SCHEMAS)
        or any(type(value) is not str or len(value) != 10 for value in schema_starts.values())
    ):
        raise IntegrityError("sealed v21 range evidence is incomplete")
    for effective in product_dates.values():
        validate_product_effective_date(str(effective))
    if report.get("prelaunch_dispositions") != _prelaunch_records(product_dates):
        raise IntegrityError("sealed v21 prelaunch dispositions drifted")

    request_by_id: dict[object, Mapping[str, object]] = {}
    for request in requests:
        if not isinstance(request, Mapping):
            raise IntegrityError("v21 request definition is malformed")
        core = {key: value for key, value in request.items() if key != "request_id"}
        validate_phase1a_request(core)
        request_id = request.get("request_id")
        if request_id != sha256_json(core) or request_id in request_by_id:
            raise IntegrityError("v21 request identity drifted")
        request_by_id[request_id] = request
    if len(request_by_id) != 20:
        raise IntegrityError("v21 request identities are incomplete")

    cost_proofs = report.get("full_range_zero_cost_proofs")
    if not isinstance(cost_proofs, list) or len(cost_proofs) != 20:
        raise IntegrityError("v21 full-range zero-cost proofs are incomplete")
    observed_cost_ids: set[object] = set()
    for proof in cost_proofs:
        if not isinstance(proof, Mapping):
            raise IntegrityError("v21 cost proof is malformed")
        request_id = proof.get("request_definition_id")
        request = request_by_id.get(request_id)
        market = str(request.get("market")) if request else ""
        if (
            request is None
            or request_id in observed_cost_ids
            or proof.get("market") != market
            or proof.get("schema") != request.get("schema")
            or proof.get("acquisition_start")
            != max("2018-01-01", str(product_dates[market]))
            or proof.get("end_exclusive") != end_exclusive
            or proof.get("estimated_cost_usd") != "0"
            or proof.get("annual_subset_dominance") is not True
        ):
            raise IntegrityError("v21 full-range zero-cost proof drifted")
        observed_cost_ids.add(request_id)
    if observed_cost_ids != set(request_by_id):
        raise IntegrityError("v21 cost-proof request census drifted")

    expected_intervals: dict[tuple[object, str, str], Mapping[str, object]] = {}
    for request_id, request in request_by_id.items():
        market = str(request["market"])
        for annual in annual_market_year_intervals(
            start=max("2018-01-01", str(product_dates[market])),
            end_exclusive=end_exclusive,
        ):
            expected_intervals[
                (request_id, str(annual["start"]), str(annual["end_exclusive"]))
            ] = annual
    estimates = report.get("request_estimates")
    if not isinstance(estimates, list) or len(estimates) != MAXIMUM_DBN_FILES:
        raise IntegrityError("v21 annual byte estimates are incomplete")
    observed_intervals: set[tuple[object, str, str]] = set()
    observed_acquisition_ids: set[object] = set()
    observed_destinations: set[str] = set()
    total_estimated = 0
    for estimate in estimates:
        if not isinstance(estimate, Mapping):
            raise IntegrityError("v21 annual estimate is malformed")
        estimate_core = dict(estimate)
        acquisition_id = estimate_core.pop("acquisition_request_id", None)
        if acquisition_id != sha256_json(estimate_core):
            raise IntegrityError("v21 annual estimate identity drifted")
        request_id = estimate.get("request_definition_id")
        request = request_by_id.get(request_id)
        key = (
            request_id,
            str(estimate.get("acquisition_start")),
            str(estimate.get("end_exclusive")),
        )
        annual = expected_intervals.get(key)
        size = estimate.get("estimated_bytes")
        if (
            request is None
            or annual is None
            or key in observed_intervals
            or acquisition_id in observed_acquisition_ids
            or type(size) is not int
            or size <= 0
            or estimate.get("estimated_cost_usd")
            != "0_FROM_FULL_RANGE_DOMINANCE"
            or estimate.get("exact_annual_requote_required_before_download") is not True
            or estimate.get("market") != request.get("market")
            or estimate.get("schema") != request.get("schema")
            or estimate.get("year") != annual.get("year")
            or estimate.get("partial_launch_year")
            != annual.get("partial_launch_year")
            or estimate.get("partial_latest_year")
            != annual.get("partial_latest_year")
        ):
            raise IntegrityError("v21 annual estimate scope or economics drifted")
        market = str(request["market"])
        expected_paths = phase1a_paths(
            market=market,
            schema=str(request["schema"]),
            year=int(annual["year"]),
            interval=str(annual["interval"]),
        )
        if (
            estimate.get("product_effective_date") != product_dates[market]
            or estimate.get("dbn_destination") != expected_paths["dbn"]
            or estimate.get("sidecar_destination") != expected_paths["sidecar"]
        ):
            raise IntegrityError("v21 annual estimate routing drifted")
        observed_intervals.add(key)
        observed_acquisition_ids.add(acquisition_id)
        for destination in expected_paths.values():
            if destination in observed_destinations:
                raise IntegrityError("v21 annual destinations collide")
            observed_destinations.add(destination)
        total_estimated += size
    byte_ceiling = total_estimated + max(total_estimated // 10, 1024**2)
    if (
        observed_intervals != set(expected_intervals)
        or len(observed_destinations) != 2 * MAXIMUM_DBN_FILES
        or report.get("total_estimated_bytes") != total_estimated
        or report.get("total_acquisition_byte_ceiling") != byte_ceiling
        or report.get("fixed_maximum_total_acquisition_bytes")
        != MAXIMUM_TOTAL_ACQUISITION_BYTES
        or report.get("disk_required_free_bytes") != byte_ceiling + DISK_SAFETY_BYTES
        or type(report.get("disk_free_bytes_observed")) is not int
        or report.get("disk_free_bytes_observed", 0)
        < report.get("disk_required_free_bytes", 1)
    ):
        raise IntegrityError("v21 byte or disk evidence drifted")
    return plan, report


def _exact_query(
    request: Mapping[str, object], estimate: Mapping[str, object]
) -> dict[str, object]:
    return {
        "dataset": request["dataset"],
        "schema": request["schema"],
        "stype_in": request["stype_in"],
        "stype_out": request["stype_out"],
        "symbols": request["symbols"],
        "start": estimate["acquisition_start"],
        "end": estimate["end_exclusive"],
    }


def build_acquisition_plan(
    *, root: Path, committed_head: str, require_destination_absence: bool = True
) -> dict[str, object]:
    """Freeze the exact 160 annual DBNs from the passing v21 report."""

    root = root.resolve(strict=True)
    if committed_head != _git_head(root):
        raise IntegrityError("acquisition plan must bind the live committed implementation HEAD")
    preflight_plan, report = _validate_preflight_evidence(root=root)
    requests = preflight_plan["requests"]
    request_by_id = {
        item["request_id"]: item for item in requests if isinstance(item, Mapping)
    }
    frozen: list[dict[str, object]] = []
    destinations: list[str] = []
    for estimate in report["request_estimates"]:
        if not isinstance(estimate, Mapping):
            raise IntegrityError("v21 annual estimate is malformed")
        request = request_by_id.get(estimate.get("request_definition_id"))
        if not isinstance(request, Mapping):
            raise IntegrityError("v21 request definition is absent for annual estimate")
        query = _exact_query(request, estimate)
        size = estimate.get("estimated_bytes")
        if type(size) is not int or size <= 0:
            raise IntegrityError("v21 annual byte estimate is invalid")
        request_ceiling = size + max(size // 10, 1024**2)
        dbn = str(estimate["dbn_destination"])
        sidecar = str(estimate["sidecar_destination"])
        if not dbn.startswith("data/dbn/") or sidecar != dbn + ".manifest.json":
            raise IntegrityError("acquisition destination shape drifted")
        if query["schema"] in {"ohlcv-1m", "ohlcv-1s"} and "/ohlcv_" not in dbn:
            raise IntegrityError("hyphen-versus-underscore destination mismatch")
        destinations.extend((dbn, sidecar))
        frozen.append(
            {
                "request_id": estimate["acquisition_request_id"],
                "request_definition_id": request["request_id"],
                "market": request["market"],
                "schema": request["schema"],
                "year": estimate["year"],
                "partial_launch_year": estimate["partial_launch_year"],
                "partial_latest_year": estimate["partial_latest_year"],
                "query": query,
                "wire_format": {
                    "encoding": "dbn",
                    "compression": "zstd",
                    "contract": "LOCKED_DATABENTO_GET_RANGE_ALWAYS_DBN_ZSTD",
                },
                "metadata_estimated_cost_usd": "0_FROM_FULL_RANGE_DOMINANCE",
                "fresh_exact_cost_requote_required_before_download": True,
                "estimated_bytes": size,
                "request_byte_ceiling": request_ceiling,
                "dbn_destination": dbn,
                "sidecar_destination": sidecar,
            }
        )
    if len(frozen) != MAXIMUM_DBN_FILES or len(set(destinations)) != 320:
        raise IntegrityError("acquisition request or destination count drifted")
    if require_destination_absence and any((root / path).exists() for path in destinations):
        raise IntegrityError("acquisition destination already exists")
    total_ceiling = report.get("total_acquisition_byte_ceiling")
    required_free = report.get("disk_required_free_bytes")
    if (
        type(total_ceiling) is not int
        or not (0 < total_ceiling <= MAXIMUM_TOTAL_ACQUISITION_BYTES)
        or required_free != total_ceiling + DISK_SAFETY_BYTES
    ):
        raise IntegrityError("v21 acquisition byte ceiling is invalid")
    prelaunch = [
        {
            "market": market,
            **record,
            "applies_to_schemas": list(SCHEMAS),
            "dbn_fabricated": False,
        }
        for market in CURRENT_ACQUISITION_MARKETS
        for record in report["prelaunch_dispositions"][market]
    ]
    implementation_paths = (
        "configs/dependency_lock_receipt.json",
        "scripts/prepare_apex_micro_phase1a_acquisition_v21.py",
        "scripts/prepare_safe_cleanup_candidate_census_v6.py",
        "src/futures_rebuild/boundary.py",
        "src/futures_rebuild/canonical.py",
        "src/futures_rebuild/live_cockpit/databento_auth.py",
        "src/futures_rebuild/micro_alpha_acquisition.py",
        "src/futures_rebuild/micro_alpha_acquisition_v21.py",
        "src/futures_rebuild/micro_alpha_pipeline.py",
        "src/futures_rebuild/research_gateway_policy.py",
        "src/futures_rebuild/runtime_environment.py",
    )
    product_requirements = build_product_reference_requirements()
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1a_acquisition_plan/21.0.0",
        "state": "PREPARED_REQUIRES_SEPARATE_EXACT_DOWNLOAD_APPROVAL",
        "operation": OPERATION,
        "committed_implementation_head": committed_head,
        "lane_id": "apex_integer_micro_21",
        "dataset": DATASET,
        "standard_plan_requirement": "DATABENTO_STANDARD",
        "markets": list(CURRENT_ACQUISITION_MARKETS),
        "schemas": list(SCHEMAS),
        "latest_complete_end_exclusive": report["cumulative_metadata"][
            "latest_complete_end_exclusive"
        ],
        "product_effective_dates": report["product_effective_dates"],
        "preflight_evidence": {
            "plan_path": PREFLIGHT_PLAN_PATH.as_posix(),
            "plan_id": PREFLIGHT_PLAN_ID,
            "plan_sha256": PREFLIGHT_PLAN_SHA256,
            "report_path": PREFLIGHT_REPORT_PATH.as_posix(),
            "report_id": PREFLIGHT_REPORT_ID,
            "report_sha256": PREFLIGHT_REPORT_SHA256,
            "authorization_use_path": PREFLIGHT_AUTHORIZATION_PATH.as_posix(),
            "authorization_receipt_id": PREFLIGHT_AUTHORIZATION_RECEIPT_ID,
            "authorization_use_sha256": PREFLIGHT_AUTHORIZATION_SHA256,
        },
        "product_reference_requirements_id": product_requirements["requirements_id"],
        "product_reference_requirements_sha256": sha256_file(
            root / "configs/apex_micro_product_reference_requirements.json"
        ),
        "implementation_hashes": {
            path: sha256_file(root / path) for path in implementation_paths
        },
        "file_partition": (
            "ONE_DBN_AND_ADJACENT_SIDECAR_PER_MARKET_SCHEMA_CALENDAR_YEAR"
        ),
        "requests": frozen,
        "prelaunch_coverage": prelaunch,
        "limits": {
            "exact_request_count": MAXIMUM_DBN_FILES,
            "maximum_provider_calls": 2 * MAXIMUM_DBN_FILES,
            "maximum_dbn_files": MAXIMUM_DBN_FILES,
            "maximum_sidecars": MAXIMUM_SIDECARS,
            "maximum_total_bytes": total_ceiling,
            "required_free_disk_bytes": required_free,
            "maximum_external_cost_usd": "0",
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_per_download_seconds": MAXIMUM_PER_DOWNLOAD_SECONDS,
            "maximum_attempts": 1,
            "maximum_retries": MAXIMUM_RETRIES,
            "maximum_parallel_downloads": MAXIMUM_PARALLEL_DOWNLOADS,
            "maximum_provider_clients": MAXIMUM_PROVIDER_CLIENTS,
        },
        "credential_source": {
            "path": "api.env",
            "binding": "PATH_ONLY_CONTENT_NEVER_REPORTED",
        },
        "custody": {
            "inactive_staging_first": True,
            "create_only_destinations": True,
            "adjacent_immutable_sidecar": True,
            "sha256_and_byte_count_verified_before_finalization": True,
            "terminal_record_written_last": True,
            "failed_or_partial_attempt_preserved_inactive": True,
            "resume_or_overwrite": False,
            "parallelism": "TWO_ISOLATED_DOWNLOAD_CLIENTS_MAXIMUM",
            "stop_scheduling_after_first_failure": True,
            "already_running_second_request_may_finish_to_inactive_staging": True,
        },
        "holdout_and_forward": {
            "year_2025": "INACTIVE_RAW_CUSTODY_DECODING_BLOCKED_SHARED_HOLDOUT",
            "year_2026": "INACTIVE_RAW_CUSTODY_PRE_FREEZE_DECODING_BLOCKED",
            "forward_boundary": "MECHANISM_FREEZE_TIMESTAMP_NOT_CALENDAR_YEAR",
            "payload_open_during_acquisition_verification": False,
        },
        "forbidden": {
            "dbn_row_decode": True,
            "raw_values_in_reports": True,
            "credential_log_stage_or_report": True,
            "publication": True,
            "catalog_or_pointer_activation": True,
            "registration": True,
            "model_fit_prediction_or_evaluation": True,
            "trading": True,
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def write_acquisition_plan_create_only(*, root: Path, committed_head: str) -> dict[str, object]:
    plan = build_acquisition_plan(root=root, committed_head=committed_head)
    path = root / PLAN_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(plan) + b"\n")
    return plan


def load_acquisition_plan(
    *, root: Path, require_destination_absence: bool = True
) -> dict[str, object]:
    root = root.resolve(strict=True)
    plan = _object(root / PLAN_PATH, "v21 Apex micro acquisition plan")
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    if (
        plan_id != sha256_json(core)
        or plan.get("state")
        != "PREPARED_REQUIRES_SEPARATE_EXACT_DOWNLOAD_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("markets") != list(CURRENT_ACQUISITION_MARKETS)
        or plan.get("schemas") != list(SCHEMAS)
        or plan.get("committed_implementation_head") != _git_head(root)
    ):
        raise UnauthorizedOperation("v21 Apex micro acquisition plan is absent or drifted")
    expected = build_acquisition_plan(
        root=root,
        committed_head=str(plan["committed_implementation_head"]),
        require_destination_absence=require_destination_absence,
    )
    if plan != expected:
        raise IntegrityError("v21 acquisition plan does not reconstruct exactly")
    requests = plan.get("requests")
    limits = plan.get("limits")
    if (
        not isinstance(requests, list)
        or len(requests) != MAXIMUM_DBN_FILES
        or not isinstance(limits, Mapping)
        or limits.get("exact_request_count") != MAXIMUM_DBN_FILES
        or limits.get("maximum_dbn_files") != MAXIMUM_DBN_FILES
        or limits.get("maximum_sidecars") != MAXIMUM_SIDECARS
        or limits.get("maximum_provider_calls") != 320
        or limits.get("maximum_parallel_downloads") != 2
        or limits.get("maximum_provider_clients") != 3
        or limits.get("maximum_external_cost_usd") != "0"
        or limits.get("maximum_attempts") != 1
        or limits.get("maximum_retries") != 0
    ):
        raise IntegrityError("v21 acquisition plan limits drifted")
    destinations: list[str] = []
    for item in requests:
        if not isinstance(item, Mapping):
            raise IntegrityError("v21 frozen acquisition request is malformed")
        query = item.get("query")
        if not isinstance(query, Mapping) or set(query) != {
            "dataset",
            "schema",
            "stype_in",
            "stype_out",
            "symbols",
            "start",
            "end",
        }:
            raise IntegrityError("v21 frozen provider query is malformed")
        destination_parts = str(item["dbn_destination"]).split("/")
        if len(destination_parts) != 6:
            raise IntegrityError("v21 frozen destination depth drifted")
        market = destination_parts[3]
        year = int(destination_parts[4])
        schema = str(query["schema"])
        expected_stype = "parent" if schema == "definition" else "continuous"
        expected_symbol = f"{market}.FUT" if schema == "definition" else f"{market}.v.0"
        validate_annual_market_year_interval(
            year=year, interval=f'{query["start"]}_{query["end"]}'
        )
        if (
            query["dataset"] != DATASET
            or schema not in SCHEMAS
            or query["stype_in"] != expected_stype
            or query["stype_out"] != "instrument_id"
            or query["symbols"] != [expected_symbol]
            or query["start"] < "2018-01-01"
            or query["start"] >= query["end"]
            or item.get("market") != market
            or item.get("year") != year
            or item.get("wire_format")
            != {
                "encoding": "dbn",
                "compression": "zstd",
                "contract": "LOCKED_DATABENTO_GET_RANGE_ALWAYS_DBN_ZSTD",
            }
            or item.get("fresh_exact_cost_requote_required_before_download") is not True
            or item.get("sidecar_destination")
            != str(item["dbn_destination"]) + ".manifest.json"
        ):
            raise IntegrityError("v21 frozen acquisition request scope drifted")
        destinations.extend(
            (str(item["dbn_destination"]), str(item["sidecar_destination"]))
        )
    if len(set(destinations)) != 320:
        raise IntegrityError("v21 frozen acquisition destinations collide")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["limits"]
    return {
        "plan_id": str(plan["plan_id"]),
        "committed_implementation_head": str(plan["committed_implementation_head"]),
        "preflight_report_id": PREFLIGHT_REPORT_ID,
        "markets": ",".join(CURRENT_ACQUISITION_MARKETS),
        "schemas": ",".join(SCHEMAS),
        "request_count": str(limits["exact_request_count"]),
        "maximum_dbn_files": str(limits["maximum_dbn_files"]),
        "maximum_sidecars": str(limits["maximum_sidecars"]),
        "maximum_provider_calls": str(limits["maximum_provider_calls"]),
        "maximum_total_bytes": str(limits["maximum_total_bytes"]),
        "required_free_disk_bytes": str(limits["required_free_disk_bytes"]),
        "maximum_runtime_seconds": str(limits["maximum_runtime_seconds"]),
        "maximum_per_download_seconds": str(limits["maximum_per_download_seconds"]),
        "maximum_parallel_downloads": str(limits["maximum_parallel_downloads"]),
        "maximum_provider_clients": str(limits["maximum_provider_clients"]),
        "maximum_external_cost_usd": "0",
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "credential_source": CREDENTIAL_SOURCE,
        "destination_root": "data/dbn",
        "inactive_custody": "true",
        "dbn_row_decode": "false",
        "publication": "false",
        "catalog_activation": "false",
        "registration": "false",
        "trading": "false",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _zero_cost(value: object) -> None:
    if isinstance(value, bool):
        raise IntegrityError("fresh acquisition cost is invalid")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError("fresh acquisition cost is invalid") from exc
    if not amount.is_finite() or amount != 0:
        raise UnauthorizedOperation("fresh acquisition cost is unexpectedly nonzero")


def _metadata_query(query: Mapping[str, object]) -> dict[str, object]:
    return {
        key: query[key]
        for key in ("dataset", "schema", "stype_in", "symbols", "start", "end")
    }


def _set_timeout(function: Callable[..., object], seconds: float) -> None:
    owner = getattr(function, "__self__", None)
    if owner is not None and hasattr(owner, "TIMEOUT"):
        setattr(owner, "TIMEOUT", max(1.0, seconds))


def _write_terminal(path: Path, core: Mapping[str, object]) -> dict[str, object]:
    terminal = {**core, "terminal_id": sha256_json(core)}
    with path.open("xb") as stream:
        stream.write(canonical_bytes(terminal) + b"\n")
    return terminal


def _mark_read_only(path: Path) -> None:
    path.chmod(stat.S_IREAD)


@dataclass(frozen=True)
class _DownloadWorkerResult:
    records: tuple[dict[str, object], ...]
    get_range_calls: int
    provider_client_created: bool
    failure_type: str | None
    failed_request_id: str | None


def _download_worker(
    *,
    root: Path,
    downloads: Path,
    plan_id: str,
    items: tuple[Mapping[str, object], ...],
    provider_factory: Callable[[], DownloadProviderApis],
    stop_event: threading.Event,
    total_state: dict[str, int],
    total_lock: threading.Lock,
    maximum_total_bytes: int,
    started: float,
    clock: Callable[[], float],
) -> _DownloadWorkerResult:
    """Download one deterministic queue with one isolated client and no decode."""

    records: list[dict[str, object]] = []
    calls = 0
    failed_request_id: str | None = None
    try:
        apis = provider_factory()
    except Exception as exc:
        stop_event.set()
        return _DownloadWorkerResult((), 0, False, type(exc).__name__, None)
    try:
        for item in items:
            if stop_event.is_set():
                break
            elapsed = clock() - started
            remaining = MAXIMUM_RUNTIME_SECONDS - elapsed
            if remaining <= 0:
                raise UnauthorizedOperation("acquisition runtime ceiling reached")
            request_id = str(item["request_id"])
            failed_request_id = request_id
            partial = downloads / f"{request_id[:16]}.dbn.zst.partial"
            if partial.exists():
                raise IntegrityError("partial staging destination already exists")
            _set_timeout(
                apis.get_range,
                min(float(MAXIMUM_PER_DOWNLOAD_SECONDS), float(remaining)),
            )
            calls += 1
            apis.get_range(**item["query"], path=str(partial))
            if not partial.is_file():
                raise IntegrityError("provider did not create the bound staging file")
            size = partial.stat().st_size
            if size <= 0 or size > item["request_byte_ceiling"]:
                raise UnauthorizedOperation(
                    "downloaded file exceeds its byte ceiling or is empty"
                )
            digest = sha256_file(partial)
            with total_lock:
                proposed = total_state["bytes"] + size
                if proposed > maximum_total_bytes:
                    raise UnauthorizedOperation(
                        "downloaded files exceed the total byte ceiling"
                    )
                total_state["bytes"] = proposed
            sidecar_staging = downloads / f"{request_id[:16]}.manifest.json.partial"
            exact_authorized_query = {
                **dict(item["query"]),
                "encoding": item["wire_format"]["encoding"],
                "compression": item["wire_format"]["compression"],
            }
            sidecar_core = {
                "schema_version": "apex_micro_inactive_dbn_manifest/21.0.0",
                "state": "INACTIVE_CUSTODY_NOT_A_RESEARCH_SOURCE",
                "plan_id": plan_id,
                "request_id": request_id,
                "exact_authorized_query": exact_authorized_query,
                "wire_format_contract": item["wire_format"]["contract"],
                "metadata_estimated_cost_usd": item[
                    "metadata_estimated_cost_usd"
                ],
                "fresh_exact_cost_requote_usd": "0",
                "external_cost_incurred_usd": "0",
                "byte_count": size,
                "sha256": digest,
                "dbn_rows_decoded": 0,
                "payload_opened_for_row_access": False,
                "catalog_activation": False,
            }
            with sidecar_staging.open("xb") as stream:
                stream.write(
                    canonical_bytes(
                        {**sidecar_core, "manifest_id": sha256_json(sidecar_core)}
                    )
                    + b"\n"
                )
            records.append(
                {
                    "request_id": request_id,
                    "staging_dbn": partial.relative_to(root).as_posix(),
                    "staging_sidecar": sidecar_staging.relative_to(root).as_posix(),
                    "dbn_destination": item["dbn_destination"],
                    "sidecar_destination": item["sidecar_destination"],
                    "byte_count": size,
                    "sha256": digest,
                }
            )
            failed_request_id = None
    except Exception as exc:
        stop_event.set()
        return _DownloadWorkerResult(
            tuple(records), calls, True, type(exc).__name__, failed_request_id
        )
    return _DownloadWorkerResult(tuple(records), calls, True, None, None)


def execute_authorized_acquisition(
    *,
    root: Path,
    authorization: OperationReceipt,
    provider_factory: Callable[[], DownloadProviderApis],
    credential_source: str,
    clock: Callable[[], float] = time.monotonic,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
    mark_immutable: Callable[[Path], None] = _mark_read_only,
    link_file: Callable[[Path, Path], None] = os.link,
) -> dict[str, object]:
    """Execute one exact annual acquisition into inactive immutable custody."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_acquisition_plan(root=root)
    if credential_source != CREDENTIAL_SOURCE:
        raise UnauthorizedOperation("acquisition credential source is not bound")
    environment_check(root)
    destinations = [
        root / str(item[key])
        for item in plan["requests"]
        for key in ("dbn_destination", "sidecar_destination")
    ]
    if any(path.exists() for path in destinations):
        raise IntegrityError("create-only acquisition destination already exists")
    usage = disk_usage(root)
    free = getattr(usage, "free", None)
    if type(free) is not int or free < plan["limits"]["required_free_disk_bytes"]:
        raise UnauthorizedOperation("insufficient disk capacity for acquisition")
    claim = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    attempt = root / STAGING_ROOT / authorization.receipt_id[:16]
    boundary.assert_active_path(
        attempt.absolute(),
        purpose="Apex micro v21 acquisition staging",
        subtree=STAGING_ROOT.as_posix(),
    )
    attempt.mkdir(parents=True, exist_ok=False)
    terminal_path = attempt / "terminal.json"
    started = clock()
    staged: list[dict[str, object]] = []
    finalized: list[dict[str, object]] = []
    finalization_attempts: list[dict[str, object]] = []
    exact_count = int(plan["limits"]["exact_request_count"])
    provider_calls = {"get_cost": 0, "get_range": 0}
    provider_client_count = 0
    worker_failures: list[dict[str, object]] = []
    downloads: Path | None = None
    failure_stage = "PROVIDER_FACTORY"
    base: dict[str, object] = {
        "schema_version": "apex_micro_phase1a_acquisition_terminal/21.0.0",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_claim_sha256": sha256_file(claim),
        "credential_source": CREDENTIAL_SOURCE,
        "credential_content_recorded": False,
        "maximum_external_cost_usd": "0",
        "external_cost_incurred_usd": "0",
        "automatic_retries": 0,
        "maximum_parallel_downloads": MAXIMUM_PARALLEL_DOWNLOADS,
        "maximum_provider_clients": MAXIMUM_PROVIDER_CLIENTS,
        "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
        "maximum_per_download_seconds": MAXIMUM_PER_DOWNLOAD_SECONDS,
        "dbn_rows_decoded": 0,
        "payloads_opened_for_row_access": 0,
        "year_2025_or_2026_payloads_opened": 0,
        "raw_values_reported": False,
        "catalog_or_pointer_activated": False,
        "published": False,
        "registered": False,
        "model_fit_prediction_or_evaluation": False,
        "trading": False,
    }
    try:
        apis = provider_factory()
        provider_client_count = 1
        failure_stage = "FRESH_EXACT_ZERO_COST_CENSUS"
        for item in plan["requests"]:
            remaining = MAXIMUM_RUNTIME_SECONDS - (clock() - started)
            if remaining <= 0:
                raise UnauthorizedOperation(
                    "acquisition runtime ceiling reached before download"
                )
            _set_timeout(apis.get_cost, min(90.0, float(remaining)))
            provider_calls["get_cost"] += 1
            _zero_cost(apis.get_cost(**_metadata_query(item["query"])))
        downloads = attempt / "downloads"
        downloads.mkdir()
        failure_stage = "DOWNLOAD_TO_INACTIVE_STAGING"
        worker_count = min(MAXIMUM_PARALLEL_DOWNLOADS, exact_count)
        queues = tuple(
            tuple(plan["requests"][worker_index::worker_count])
            for worker_index in range(worker_count)
        )
        stop_event = threading.Event()
        total_state = {"bytes": 0}
        total_lock = threading.Lock()
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="apex-micro-v21-dbn",
        ) as executor:
            futures = [
                executor.submit(
                    _download_worker,
                    root=root,
                    downloads=downloads,
                    plan_id=str(plan["plan_id"]),
                    items=queue,
                    provider_factory=provider_factory,
                    stop_event=stop_event,
                    total_state=total_state,
                    total_lock=total_lock,
                    maximum_total_bytes=int(plan["limits"]["maximum_total_bytes"]),
                    started=started,
                    clock=clock,
                )
                for queue in queues
            ]
            results = [future.result() for future in futures]
        provider_calls["get_range"] = sum(
            result.get_range_calls for result in results
        )
        provider_client_count += sum(
            int(result.provider_client_created) for result in results
        )
        staged = [record for result in results for record in result.records]
        request_order = {
            str(item["request_id"]): index
            for index, item in enumerate(plan["requests"])
        }
        staged.sort(key=lambda item: request_order[str(item["request_id"])])
        worker_failures = [
            {
                "worker_index": index,
                "exception_type": result.failure_type,
                "failed_request_id": result.failed_request_id,
            }
            for index, result in enumerate(results)
            if result.failure_type is not None
        ]
        if worker_failures:
            raise IntegrityError("bounded parallel download worker failed")
        if (
            provider_calls != {"get_cost": exact_count, "get_range": exact_count}
            or len(staged) != exact_count
            or provider_client_count != MAXIMUM_PROVIDER_CLIENTS
        ):
            raise IntegrityError("successful acquisition call or file count drifted")
        failure_stage = "FINAL_DESTINATION_RECHECK"
        if any(path.exists() for path in destinations):
            raise IntegrityError("destination appeared before finalization")
        failure_stage = "CREATE_ONLY_FINALIZATION"
        for item in staged:
            source_dbn = root / str(item["staging_dbn"])
            source_sidecar = root / str(item["staging_sidecar"])
            final_dbn = root / str(item["dbn_destination"])
            final_sidecar = root / str(item["sidecar_destination"])
            final_dbn.parent.mkdir(parents=True, exist_ok=True)
            finalization_record = {
                "request_id": item["request_id"],
                "dbn_destination": item["dbn_destination"],
                "sidecar_destination": item["sidecar_destination"],
                "dbn_link_created": False,
                "sidecar_link_created": False,
                "staging_sources_removed": False,
                "hash_reverified": False,
                "marked_immutable": False,
            }
            finalization_attempts.append(finalization_record)
            link_file(source_dbn, final_dbn)
            finalization_record["dbn_link_created"] = True
            link_file(source_sidecar, final_sidecar)
            finalization_record["sidecar_link_created"] = True
            source_dbn.unlink()
            source_sidecar.unlink()
            finalization_record["staging_sources_removed"] = True
            if (
                final_dbn.stat().st_size != item["byte_count"]
                or sha256_file(final_dbn) != item["sha256"]
            ):
                raise IntegrityError("final DBN differs from verified staging")
            sidecar = _object(final_sidecar, "final inactive sidecar")
            sidecar_core = dict(sidecar)
            sidecar_id = sidecar_core.pop("manifest_id", None)
            if sidecar_id != sha256_json(sidecar_core):
                raise IntegrityError("final sidecar identity differs")
            finalization_record["hash_reverified"] = True
            mark_immutable(final_dbn)
            mark_immutable(final_sidecar)
            finalization_record["marked_immutable"] = True
            finalized.append(
                {
                    "request_id": item["request_id"],
                    "dbn_destination": item["dbn_destination"],
                    "sidecar_destination": item["sidecar_destination"],
                    "byte_count": item["byte_count"],
                    "sha256": item["sha256"],
                }
            )
        core = {
            **base,
            "state": "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY",
            "provider_call_counts": provider_calls,
            "provider_client_count": provider_client_count,
            "download_worker_count": worker_count,
            "accepted_dbn_count": exact_count,
            "accepted_sidecar_count": exact_count,
            "total_bytes": sum(int(item["byte_count"]) for item in staged),
            "accepted_files": finalized,
            "prelaunch_coverage": plan["prelaunch_coverage"],
            "terminal_written_last": True,
        }
    except Exception as exc:
        core = {
            **base,
            "state": "FAILURE_INACTIVE_EVIDENCE_PRESERVED",
            "failure_code": "ACQUISITION_FAIL_CLOSED",
            "failure_stage": failure_stage,
            "exception_type": type(exc).__name__,
            "provider_call_counts": provider_calls,
            "provider_client_count": provider_client_count,
            "download_worker_failures": worker_failures,
            "accepted_dbn_count": 0,
            "accepted_sidecar_count": 0,
            "staged_complete_pairs": staged,
            "completed_finalized_pairs": finalized,
            "finalization_attempts": finalization_attempts,
            "staging_file_census": (
                sorted(
                    path.relative_to(root).as_posix()
                    for path in downloads.iterdir()
                    if path.is_file()
                )
                if downloads is not None and downloads.exists()
                else []
            ),
            "terminal_written_last": True,
        }
    return _write_terminal(terminal_path, core)


def verify_completed_acquisition(*, root: Path, terminal_path: Path) -> dict[str, object]:
    """Verify hashes and sidecars without opening DBN payloads."""

    root = root.resolve(strict=True)
    terminal_abs = terminal_path if terminal_path.is_absolute() else root / terminal_path
    terminal = _object(terminal_abs, "v21 Apex micro acquisition terminal")
    terminal_core = dict(terminal)
    terminal_id = terminal_core.pop("terminal_id", None)
    plan = load_acquisition_plan(root=root, require_destination_absence=False)
    exact_count = int(plan["limits"]["exact_request_count"])
    if (
        terminal_id != sha256_json(terminal_core)
        or terminal.get("state") != "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY"
        or terminal.get("plan_id") != plan["plan_id"]
        or terminal.get("plan_sha256") != sha256_file(root / PLAN_PATH)
        or terminal.get("provider_call_counts")
        != {"get_cost": exact_count, "get_range": exact_count}
        or terminal.get("provider_client_count") != MAXIMUM_PROVIDER_CLIENTS
        or terminal.get("download_worker_count") != MAXIMUM_PARALLEL_DOWNLOADS
        or terminal.get("accepted_dbn_count") != exact_count
        or terminal.get("accepted_sidecar_count") != exact_count
        or terminal.get("external_cost_incurred_usd") != "0"
        or terminal.get("automatic_retries") != 0
        or terminal.get("credential_content_recorded") is not False
        or terminal.get("dbn_rows_decoded") != 0
        or terminal.get("payloads_opened_for_row_access") != 0
        or terminal.get("year_2025_or_2026_payloads_opened") != 0
        or terminal.get("raw_values_reported") is not False
        or terminal.get("catalog_or_pointer_activated") is not False
        or terminal.get("published") is not False
        or terminal.get("registered") is not False
        or terminal.get("model_fit_prediction_or_evaluation") is not False
        or terminal.get("trading") is not False
        or terminal.get("terminal_written_last") is not True
    ):
        raise IntegrityError("v21 acquisition terminal is not an accepted success")
    accepted = terminal.get("accepted_files")
    if not isinstance(accepted, list) or len(accepted) != exact_count:
        raise IntegrityError("v21 terminal file ledger is incomplete")
    planned = {str(item["request_id"]): item for item in plan["requests"]}
    observed: set[str] = set()
    total_bytes = 0
    for item in accepted:
        if not isinstance(item, Mapping):
            raise IntegrityError("v21 accepted file ledger is malformed")
        request_id = str(item.get("request_id"))
        request = planned.get(request_id)
        if request is None or request_id in observed:
            raise IntegrityError("v21 accepted request identity is invalid")
        observed.add(request_id)
        dbn = root / str(item["dbn_destination"])
        sidecar_path = root / str(item["sidecar_destination"])
        size = item.get("byte_count")
        digest = item.get("sha256")
        if (
            item.get("dbn_destination") != request["dbn_destination"]
            or item.get("sidecar_destination") != request["sidecar_destination"]
            or type(size) is not int
            or size <= 0
            or type(digest) is not str
            or len(digest) != 64
            or dbn.stat().st_size != size
            or sha256_file(dbn) != digest
        ):
            raise IntegrityError("v21 DBN byte count, hash, or destination differs")
        sidecar = _object(sidecar_path, "v21 inactive DBN sidecar")
        sidecar_core = dict(sidecar)
        manifest_id = sidecar_core.pop("manifest_id", None)
        exact_query = {
            **dict(request["query"]),
            "encoding": request["wire_format"]["encoding"],
            "compression": request["wire_format"]["compression"],
        }
        if (
            manifest_id != sha256_json(sidecar_core)
            or sidecar.get("state") != "INACTIVE_CUSTODY_NOT_A_RESEARCH_SOURCE"
            or sidecar.get("plan_id") != plan["plan_id"]
            or sidecar.get("request_id") != request_id
            or sidecar.get("exact_authorized_query") != exact_query
            or sidecar.get("fresh_exact_cost_requote_usd") != "0"
            or sidecar.get("external_cost_incurred_usd") != "0"
            or sidecar.get("byte_count") != size
            or sidecar.get("sha256") != digest
            or sidecar.get("dbn_rows_decoded") != 0
            or sidecar.get("payload_opened_for_row_access") is not False
            or sidecar.get("catalog_activation") is not False
        ):
            raise IntegrityError("v21 sidecar differs from the exact accepted query")
        total_bytes += size
    if (
        observed != set(planned)
        or terminal.get("total_bytes") != total_bytes
        or total_bytes > plan["limits"]["maximum_total_bytes"]
        or (root / "configs/active_micro_alpha_research_ladder.json").exists()
        or (root / "data/active/catalogs/apex_micro.json").exists()
    ):
        raise IntegrityError("v21 inactive custody reconciliation is incomplete")
    return {
        "status": "PASS_INACTIVE_CUSTODY_NO_ROW_DECODE",
        "terminal_id": terminal_id,
        "dbn_count": exact_count,
        "sidecar_count": exact_count,
        "total_bytes": total_bytes,
    }


def build_plan_audit(
    *,
    root: Path,
    fresh_standard_topology_report: Mapping[str, object],
    fresh_cleanup_census: Mapping[str, object],
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
) -> dict[str, object]:
    """Independently audit a frozen plan without reading any data payload."""

    root = root.resolve(strict=True)
    plan = load_acquisition_plan(root=root)
    persisted_topology = _object(root / STANDARD_TOPOLOGY_PATH, "standard topology report")
    if dict(fresh_standard_topology_report) != persisted_topology:
        raise IntegrityError("fresh standard topology reconstruction drifted")
    persisted_cleanup = _object(root / CLEANUP_CENSUS_PATH, "cleanup candidate census")
    if dict(fresh_cleanup_census) != persisted_cleanup:
        raise IntegrityError("fresh cleanup candidate census drifted")
    topology_core = dict(persisted_topology)
    topology_id = topology_core.pop("report_id", None)
    if (
        topology_id != sha256_json(topology_core)
        or persisted_topology.get("state")
        != "PASS_SOURCE_SAFE_PROVENANCE_METADATA_ONLY"
        or persisted_topology.get("payload_safety", {}).get("historical_rows_read") != 0
        or persisted_topology.get("payload_safety", {}).get(
            "year_2025_or_2026_payload_opened"
        )
        is not False
    ):
        raise IntegrityError("standard topology source-safe evidence drifted")
    cleanup_core = dict(persisted_cleanup)
    cleanup_id = cleanup_core.pop("census_id", None)
    if (
        cleanup_id != sha256_json(cleanup_core)
        or persisted_cleanup.get("state")
        != "PREPARED_NO_MUTATION_SEPARATE_EXACT_CLEANUP_APPROVAL_REQUIRED"
        or persisted_cleanup.get("committed_head") != _git_head(root)
        or persisted_cleanup.get("cleanup_execution", {}).get("performed") is not False
        or persisted_cleanup.get("cleanup_execution", {}).get("data_changed") is not False
        or persisted_cleanup.get("payload_safety", {}).get("historical_rows_read") is not False
        or persisted_cleanup.get("payload_safety", {}).get(
            "year_2025_or_2026_payload_opened"
        )
        is not False
    ):
        raise IntegrityError("cleanup candidate census safety drifted")
    usage = disk_usage(root)
    free = getattr(usage, "free", None)
    required = plan["limits"]["required_free_disk_bytes"]
    if type(free) is not int or free < required:
        raise UnauthorizedOperation("insufficient disk for frozen acquisition plan")
    destinations = [
        str(item[key])
        for item in plan["requests"]
        for key in ("dbn_destination", "sidecar_destination")
    ]
    conflicts = [path for path in destinations if (root / path).exists()]
    if conflicts:
        raise UnauthorizedOperation("frozen acquisition destination conflict appeared")
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1a_acquisition_plan_audit/21.0.0",
        "state": "PASS_EXACT_DOWNLOAD_APPROVAL_PREPARATION_ONLY",
        "observed_head": _git_head(root),
        "plan_path": PLAN_PATH.as_posix(),
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "preflight_report_id": PREFLIGHT_REPORT_ID,
        "preflight_report_sha256": PREFLIGHT_REPORT_SHA256,
        "standard_topology": {
            "path": STANDARD_TOPOLOGY_PATH.as_posix(),
            "report_id": topology_id,
            "sha256": sha256_file(root / STANDARD_TOPOLOGY_PATH),
            "fresh_reconstruction_match": True,
            "payload_rows_read": 0,
        },
        "cleanup_governance": {
            "path": CLEANUP_CENSUS_PATH.as_posix(),
            "census_id": cleanup_id,
            "sha256": sha256_file(root / CLEANUP_CENSUS_PATH),
            "candidate_count": persisted_cleanup["candidate_count"],
            "cleanup_performed": False,
            "data_or_active_catalog_changed": False,
            "separate_exact_cleanup_approval_required": True,
        },
        "scope": {
            "markets": list(CURRENT_ACQUISITION_MARKETS),
            "schemas": list(SCHEMAS),
            "exact_requests": len(plan["requests"]),
            "dbn_files": plan["limits"]["maximum_dbn_files"],
            "adjacent_sidecars": plan["limits"]["maximum_sidecars"],
            "destination_paths": len(destinations),
            "destination_conflicts": 0,
            "prelaunch_records": len(plan["prelaunch_coverage"]),
            "higher_tier_markets": [],
            "forbidden_schemas": [],
        },
        "capacity": {
            "maximum_total_bytes": plan["limits"]["maximum_total_bytes"],
            "required_free_disk_bytes": required,
            "observed_free_disk_bytes": free,
            "fits_disk": True,
        },
        "execution": {
            "maximum_provider_calls": plan["limits"]["maximum_provider_calls"],
            "maximum_parallel_downloads": plan["limits"][
                "maximum_parallel_downloads"
            ],
            "maximum_provider_clients": plan["limits"]["maximum_provider_clients"],
            "maximum_runtime_seconds": plan["limits"]["maximum_runtime_seconds"],
            "maximum_per_download_seconds": plan["limits"][
                "maximum_per_download_seconds"
            ],
            "maximum_external_cost_usd": "0",
            "maximum_attempts": 1,
            "maximum_retries": 0,
        },
        "safety": {
            "dbn_download_performed": False,
            "historical_rows_read": False,
            "year_2025_or_2026_payload_opened": False,
            "catalog_or_pointer_activated": False,
            "publication_registration_evaluation_or_trading": False,
            "cleanup_mutation_performed": False,
            "deterministic_reconstruction": True,
        },
    }
    return {**core, "audit_id": sha256_json(core)}


def write_plan_audit_create_only(
    *,
    root: Path,
    fresh_standard_topology_report: Mapping[str, object],
    fresh_cleanup_census: Mapping[str, object],
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
) -> dict[str, object]:
    audit = build_plan_audit(
        root=root,
        fresh_standard_topology_report=fresh_standard_topology_report,
        fresh_cleanup_census=fresh_cleanup_census,
        disk_usage=disk_usage,
    )
    path = root / AUDIT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(audit) + b"\n")
    return audit


__all__ = [
    "AUDIT_PATH",
    "CLEANUP_CENSUS_PATH",
    "CREDENTIAL_SOURCE",
    "DownloadProviderApis",
    "OPERATION",
    "PLAN_PATH",
    "build_acquisition_plan",
    "build_file_download_provider_apis",
    "build_plan_audit",
    "execute_authorized_acquisition",
    "load_acquisition_plan",
    "required_scope",
    "verify_completed_acquisition",
    "write_acquisition_plan_create_only",
    "write_plan_audit_create_only",
]
