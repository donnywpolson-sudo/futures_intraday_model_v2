"""Create-only inactive-custody acquisition for Apex micro Phase 1A.

This module can stream only the exact frozen Databento DBN requests to files.
It never iterates, converts, decodes, or otherwise opens DBN records.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .live_cockpit.databento_auth import resolve_databento_api_key
from .micro_alpha_databento_preflight import (
    CREDENTIAL_SOURCE,
    MAXIMUM_TOTAL_ACQUISITION_BYTES,
    PLAN_PATH as PREFLIGHT_PLAN_PATH,
    REPORT_PATH as PREFLIGHT_REPORT_PATH,
)
from .micro_alpha_pipeline import (
    CURRENT_ACQUISITION_MARKETS,
    DATASET,
    SCHEMAS,
    build_product_reference_requirements,
    phase1a_paths,
    validate_phase1a_request,
    validate_product_effective_date,
)
from .runtime_environment import require_locked_repository_environment


PLAN_PATH: Final = Path("configs/apex_micro_tier01_phase1a_acquisition_plan.json")
OPERATION: Final = "ACQUIRE_APEX_MICRO_TIER01_RAW_DBN_INACTIVE_CUSTODY_ONCE"
STAGING_ROOT: Final = Path("state/provider_acquisition_staging/apex_micro_tier01")
MAXIMUM_RUNTIME_SECONDS: Final = 7200
MAXIMUM_RETRIES: Final = 0
MAXIMUM_DBN_FILES: Final = 20
MAXIMUM_SIDECARS: Final = 20
DISK_SAFETY_BYTES: Final = 1024**3


@dataclass(frozen=True)
class DownloadProviderApis:
    get_cost: Callable[..., object]
    get_range: Callable[..., object]


def build_file_download_provider_apis(
    *, root: Path, historical_factory: Callable[..., object] | None = None,
) -> DownloadProviderApis:
    """Read the approved file credential and expose only cost and download APIs."""

    key = resolve_databento_api_key(key_files=(root / "api.env",))
    if not key:
        raise UnauthorizedOperation("the bound file api.env credential is unavailable")
    if historical_factory is None:
        from databento import Historical
        historical_factory = Historical
    client = historical_factory(key=key)
    metadata = getattr(client, "metadata", None)
    timeseries = getattr(client, "timeseries", None)
    get_cost = getattr(metadata, "get_cost", None)
    get_range = getattr(timeseries, "get_range", None)
    if not callable(get_cost) or not callable(get_range):
        raise IntegrityError("Databento client lacks the exact acquisition APIs")
    return DownloadProviderApis(get_cost=get_cost, get_range=get_range)


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
        check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    head = completed.stdout.strip()
    if len(head) != 40:
        raise IntegrityError("committed implementation HEAD is invalid")
    return head


def _validate_preflight_report(*, root: Path) -> dict[str, object]:
    report_path = root / PREFLIGHT_REPORT_PATH
    report = _object(report_path, "Apex micro metadata preflight report")
    core = dict(report)
    report_id = core.pop("report_id", None)
    preflight_plan = _object(root / PREFLIGHT_PLAN_PATH, "metadata preflight plan")
    if (
        report_id != sha256_json(core)
        or report.get("state") != "PASS_METADATA_ONLY"
        or report.get("plan_id") != preflight_plan.get("plan_id")
        or report.get("plan_sha256") != sha256_file(root / PREFLIGHT_PLAN_PATH)
        or report.get("request_definition_count") != 20
        or report.get("provider_call_total") != 51
        or report.get("maximum_external_cost_usd") != "0"
        or report.get("external_cost_incurred_usd") != "0"
        or report.get("timeseries_download_calls") != 0
        or report.get("historical_rows_read") is not False
        or report.get("dbn_files_created") != 0
        or report.get("destination_conflict_count") != 0
        or report.get("catalog_activated") is not False
        or report.get("provider_call_counts") != {
            "get_billable_size": 20,
            "get_cost": 20,
            "get_dataset_range": 1,
            "list_datasets": 1,
            "list_schemas": 1,
            "resolve": 8,
        }
    ):
        raise UnauthorizedOperation("passing Apex micro metadata preflight is absent or drifted")
    estimates = report.get("request_estimates")
    if not isinstance(estimates, list) or len(estimates) != 20:
        raise IntegrityError("metadata preflight request estimates are incomplete")
    requests = preflight_plan.get("requests")
    product_dates = report.get("product_effective_dates")
    end = report.get("latest_complete_end_exclusive")
    symbology = report.get("symbology_summaries")
    if (
        not isinstance(requests, list) or len(requests) != 20
        or not isinstance(product_dates, Mapping)
        or set(product_dates) != set(CURRENT_ACQUISITION_MARKETS)
        or type(end) is not str or len(end) != 10
        or not isinstance(symbology, Mapping)
        or set(symbology) != set(CURRENT_ACQUISITION_MARKETS)
    ):
        raise IntegrityError("metadata preflight scope summary is incomplete")
    for market, effective in product_dates.items():
        validate_product_effective_date(effective)
        market_summary = symbology.get(market)
        if not isinstance(market_summary, Mapping) or set(market_summary) != {"parent", "continuous"}:
            raise IntegrityError("metadata symbology summary is incomplete")
        for stype in ("parent", "continuous"):
            summary = market_summary.get(stype)
            if (
                not isinstance(summary, Mapping)
                or set(summary) != {"first_effective_date", "mapping_interval_count", "mapping_sha256"}
                or summary.get("first_effective_date") != effective
                or type(summary.get("mapping_interval_count")) is not int
                or summary.get("mapping_interval_count", 0) <= 0
                or type(summary.get("mapping_sha256")) is not str
                or len(str(summary.get("mapping_sha256"))) != 64
            ):
                raise IntegrityError("metadata symbology summary drifted")
    request_by_id = {
        item.get("request_id"): item for item in requests if isinstance(item, Mapping)
    }
    if len(request_by_id) != 20:
        raise IntegrityError("metadata request identities are not unique")
    observed_ids: set[object] = set()
    total_estimated = 0
    for estimate in estimates:
        if not isinstance(estimate, Mapping) or set(estimate) != {
            "request_id", "estimated_bytes", "estimated_cost_usd",
            "product_effective_date", "acquisition_start", "end_exclusive",
            "dbn_destination", "sidecar_destination",
        }:
            raise IntegrityError("metadata request estimate fields drifted")
        request_id = estimate.get("request_id")
        request = request_by_id.get(request_id)
        if request is None or request_id in observed_ids:
            raise IntegrityError("metadata estimate identity is missing or duplicated")
        observed_ids.add(request_id)
        market = str(request["market"])
        effective = str(product_dates[market])
        start = max("2018-01-01", effective)
        size = estimate.get("estimated_bytes")
        if (
            type(size) is not int or size < 0
            or estimate.get("estimated_cost_usd") != "0"
            or estimate.get("product_effective_date") != effective
            or estimate.get("acquisition_start") != start
            or estimate.get("end_exclusive") != end
        ):
            raise IntegrityError("metadata estimate date, cost, or bytes drifted")
        expected_paths = phase1a_paths(
            market=market, schema=str(request["schema"]), year=int(start[:4]),
            interval=f"{start}_{end}",
        )
        if (
            estimate.get("dbn_destination") != expected_paths["dbn"]
            or estimate.get("sidecar_destination") != expected_paths["sidecar"]
        ):
            raise IntegrityError("metadata estimate destination drifted")
        total_estimated += size
    byte_ceiling = total_estimated + max(total_estimated // 10, 1024**2)
    if (
        observed_ids != set(request_by_id)
        or report.get("total_estimated_bytes") != total_estimated
        or report.get("total_acquisition_byte_ceiling") != byte_ceiling
        or report.get("fixed_maximum_total_acquisition_bytes") != MAXIMUM_TOTAL_ACQUISITION_BYTES
        or report.get("disk_required_free_bytes") != byte_ceiling + DISK_SAFETY_BYTES
        or type(report.get("disk_free_bytes_observed")) is not int
        or report.get("disk_free_bytes_observed", 0) < report.get("disk_required_free_bytes", 1)
    ):
        raise IntegrityError("metadata byte or disk reconciliation drifted")
    return report


def _exact_query(
    request: Mapping[str, object], estimate: Mapping[str, object],
) -> dict[str, object]:
    validate_phase1a_request({key: value for key, value in request.items() if key != "request_id"})
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
    *, root: Path, committed_head: str, require_destination_absence: bool = True,
) -> dict[str, object]:
    """Freeze the exact 20 DBNs only after a passing metadata preflight."""

    root = root.resolve(strict=True)
    if committed_head != _git_head(root):
        raise IntegrityError("acquisition plan must bind the live committed implementation HEAD")
    report = _validate_preflight_report(root=root)
    preflight_plan = _object(root / PREFLIGHT_PLAN_PATH, "metadata preflight plan")
    requests = preflight_plan.get("requests")
    estimates = report["request_estimates"]
    if not isinstance(requests, list) or len(requests) != 20:
        raise IntegrityError("metadata preflight request definitions are incomplete")
    estimate_by_id = {
        item.get("request_id"): item for item in estimates if isinstance(item, Mapping)
    }
    frozen: list[dict[str, object]] = []
    prelaunch: list[dict[str, object]] = []
    destination_paths: list[str] = []
    for request in requests:
        if not isinstance(request, Mapping):
            raise IntegrityError("metadata preflight request is malformed")
        estimate = estimate_by_id.get(request.get("request_id"))
        if not isinstance(estimate, Mapping):
            raise IntegrityError("metadata estimate is missing for a request")
        query = _exact_query(request, estimate)
        estimated_bytes = estimate.get("estimated_bytes")
        if type(estimated_bytes) is not int or estimated_bytes < 0:
            raise IntegrityError("metadata byte estimate is invalid")
        request_ceiling = estimated_bytes + max(estimated_bytes // 10, 1024**2)
        dbn = str(estimate.get("dbn_destination"))
        sidecar = str(estimate.get("sidecar_destination"))
        if not dbn.startswith("data/dbn/") or sidecar != dbn + ".manifest.json":
            raise IntegrityError("acquisition destination shape drifted")
        if "ohlcv-" in dbn or "ohlcv_" not in dbn and query["schema"] in {"ohlcv-1m", "ohlcv-1s"}:
            raise IntegrityError("hyphen-versus-underscore destination mismatch")
        destination_paths.extend((dbn, sidecar))
        frozen.append({
            "request_id": request["request_id"],
            "query": query,
            "estimated_cost_usd": "0",
            "estimated_bytes": estimated_bytes,
            "request_byte_ceiling": request_ceiling,
            "dbn_destination": dbn,
            "sidecar_destination": sidecar,
        })
        if query["start"] > "2018-01-01":
            prelaunch.append({
                "market": request["market"],
                "schema": request["schema"],
                "start": "2018-01-01",
                "end_exclusive": query["start"],
                "disposition": "PRODUCT_NOT_YET_EFFECTIVE_NO_EMPTY_DBN",
            })
    if len(frozen) != 20 or len(set(destination_paths)) != 40:
        raise IntegrityError("acquisition request or destination count drifted")
    if require_destination_absence and any((root / path).exists() for path in destination_paths):
        raise IntegrityError("acquisition destination already exists")
    report_ceiling = report.get("total_acquisition_byte_ceiling")
    if type(report_ceiling) is not int or not (0 < report_ceiling <= MAXIMUM_TOTAL_ACQUISITION_BYTES):
        raise IntegrityError("metadata report byte ceiling is invalid")
    implementation_paths = (
        "src/futures_rebuild/micro_alpha_pipeline.py",
        "src/futures_rebuild/micro_alpha_databento_preflight.py",
        "src/futures_rebuild/micro_alpha_acquisition.py",
        "src/futures_rebuild/alpha_research_architecture.py",
    )
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1a_acquisition_plan/1.0.0",
        "state": "PREPARED_REQUIRES_SEPARATE_DOWNLOAD_APPROVAL",
        "operation": OPERATION,
        "committed_implementation_head": committed_head,
        "lane_id": "apex_integer_micro_11",
        "dataset": DATASET,
        "markets": list(CURRENT_ACQUISITION_MARKETS),
        "schemas": list(SCHEMAS),
        "preflight_report": {
            "path": PREFLIGHT_REPORT_PATH.as_posix(),
            "report_id": report["report_id"],
            "sha256": sha256_file(root / PREFLIGHT_REPORT_PATH),
        },
        "preflight_plan_sha256": sha256_file(root / PREFLIGHT_PLAN_PATH),
        "product_reference_requirements_id": build_product_reference_requirements()["requirements_id"],
        "product_reference_requirements_sha256": sha256_file(
            root / "configs/apex_micro_product_reference_requirements.json"
        ),
        "implementation_hashes": {
            path: sha256_file(root / path) for path in implementation_paths
        },
        "requests": frozen,
        "prelaunch_coverage": prelaunch,
        "limits": {
            "exact_request_count": 20,
            "maximum_provider_calls": 40,
            "maximum_dbn_files": MAXIMUM_DBN_FILES,
            "maximum_sidecars": MAXIMUM_SIDECARS,
            "maximum_total_bytes": report_ceiling,
            "required_free_disk_bytes": report_ceiling + DISK_SAFETY_BYTES,
            "maximum_external_cost_usd": "0",
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_attempts": 1,
            "maximum_retries": MAXIMUM_RETRIES,
        },
        "credential_source": {"path": "api.env", "binding": "PATH_ONLY_CONTENT_NEVER_REPORTED"},
        "custody": {
            "inactive_staging_first": True,
            "create_only_destinations": True,
            "adjacent_immutable_sidecar": True,
            "terminal_record_written_last": True,
            "failed_or_partial_attempt_preserved_inactive": True,
            "resume_or_overwrite": False,
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
    *, root: Path, require_destination_absence: bool = True,
) -> dict[str, object]:
    plan = _object(root / PLAN_PATH, "Apex micro acquisition plan")
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    if (
        plan_id != sha256_json(core)
        or plan.get("state") != "PREPARED_REQUIRES_SEPARATE_DOWNLOAD_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("markets") != list(CURRENT_ACQUISITION_MARKETS)
        or plan.get("schemas") != list(SCHEMAS)
        or plan.get("committed_implementation_head") != _git_head(root)
    ):
        raise UnauthorizedOperation("Apex micro acquisition plan is absent or drifted")
    expected = build_acquisition_plan(
        root=root, committed_head=str(plan["committed_implementation_head"]),
        require_destination_absence=require_destination_absence,
    )
    if plan != expected:
        raise IntegrityError("acquisition plan does not reconstruct exactly")
    requests = plan.get("requests")
    if not isinstance(requests, list) or len(requests) != 20:
        raise IntegrityError("acquisition plan must freeze exactly 20 requests")
    destinations: list[str] = []
    for item in requests:
        if not isinstance(item, Mapping) or set(item.get("query", {})) != {
            "dataset", "schema", "stype_in", "stype_out", "symbols", "start", "end",
        }:
            raise IntegrityError("frozen acquisition request is malformed")
        query = item["query"]
        if (
            query["dataset"] != DATASET
            or query["schema"] not in SCHEMAS
            or query["stype_out"] != "instrument_id"
            or query["start"] < "2018-01-01"
            or query["start"] >= query["end"]
        ):
            raise IntegrityError("frozen acquisition query is outside scope")
        expected_stype = "parent" if query["schema"] == "definition" else "continuous"
        market = str(item["dbn_destination"]).split("/")[3]
        expected_symbol = f"{market}.FUT" if expected_stype == "parent" else f"{market}.v.0"
        if query["stype_in"] != expected_stype or query["symbols"] != [expected_symbol]:
            raise IntegrityError("frozen acquisition symbology drifted")
        destinations.extend((str(item["dbn_destination"]), str(item["sidecar_destination"])))
    if len(set(destinations)) != 40:
        raise IntegrityError("frozen acquisition destinations collide")
    return plan


def verify_completed_acquisition(*, root: Path, terminal_path: Path) -> dict[str, object]:
    """Reconcile the terminal and every DBN/sidecar pair without decoding rows."""

    root = root.resolve(strict=True)
    terminal_abs = terminal_path if terminal_path.is_absolute() else root / terminal_path
    terminal = _object(terminal_abs, "Apex micro acquisition terminal")
    terminal_core = dict(terminal)
    terminal_id = terminal_core.pop("terminal_id", None)
    plan = load_acquisition_plan(root=root, require_destination_absence=False)
    if (
        terminal_id != sha256_json(terminal_core)
        or terminal.get("state") != "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY"
        or terminal.get("plan_id") != plan["plan_id"]
        or terminal.get("plan_sha256") != sha256_file(root / PLAN_PATH)
        or terminal.get("provider_call_counts") != {"get_cost": 20, "get_range": 20}
        or terminal.get("accepted_dbn_count") != 20
        or terminal.get("accepted_sidecar_count") != 20
        or terminal.get("external_cost_incurred_usd") != "0"
        or terminal.get("automatic_retries") != 0
        or terminal.get("credential_content_recorded") is not False
        or terminal.get("dbn_rows_decoded") != 0
        or terminal.get("raw_values_reported") is not False
        or terminal.get("catalog_or_pointer_activated") is not False
        or terminal.get("published") is not False
        or terminal.get("registered") is not False
        or terminal.get("model_fit_prediction_or_evaluation") is not False
        or terminal.get("trading") is not False
        or terminal.get("terminal_written_last") is not True
    ):
        raise IntegrityError("Apex micro acquisition terminal is not an accepted success")
    accepted = terminal.get("accepted_files")
    if not isinstance(accepted, list) or len(accepted) != 20:
        raise IntegrityError("Apex micro acquisition terminal file ledger is incomplete")
    planned = {str(item["request_id"]): item for item in plan["requests"]}
    observed: set[str] = set()
    total_bytes = 0
    for item in accepted:
        if not isinstance(item, Mapping):
            raise IntegrityError("Apex micro accepted file ledger is malformed")
        request_id = str(item.get("request_id"))
        request = planned.get(request_id)
        if request is None or request_id in observed:
            raise IntegrityError("Apex micro accepted request identity is invalid")
        observed.add(request_id)
        if (
            item.get("dbn_destination") != request["dbn_destination"]
            or item.get("sidecar_destination") != request["sidecar_destination"]
        ):
            raise IntegrityError("Apex micro accepted destination differs from plan")
        dbn = root / str(item["dbn_destination"])
        sidecar_path = root / str(item["sidecar_destination"])
        size = item.get("byte_count")
        digest = item.get("sha256")
        if (
            type(size) is not int or size <= 0
            or type(digest) is not str or len(digest) != 64
            or dbn.stat().st_size != size
            or sha256_file(dbn) != digest
        ):
            raise IntegrityError("Apex micro DBN byte count or SHA-256 differs")
        sidecar = _object(sidecar_path, "Apex micro DBN sidecar")
        sidecar_core = dict(sidecar)
        manifest_id = sidecar_core.pop("manifest_id", None)
        if (
            manifest_id != sha256_json(sidecar_core)
            or sidecar.get("state") != "INACTIVE_CUSTODY_NOT_A_RESEARCH_SOURCE"
            or sidecar.get("plan_id") != plan["plan_id"]
            or sidecar.get("request_id") != request_id
            or sidecar.get("exact_authorized_query") != request["query"]
            or sidecar.get("external_cost_incurred_usd") != "0"
            or sidecar.get("byte_count") != size
            or sidecar.get("sha256") != digest
            or sidecar.get("dbn_rows_decoded") != 0
            or sidecar.get("payload_opened_for_row_access") is not False
            or sidecar.get("catalog_activation") is not False
        ):
            raise IntegrityError("Apex micro DBN sidecar differs from the accepted query")
        total_bytes += size
    if (
        observed != set(planned)
        or terminal.get("total_bytes") != total_bytes
        or (root / "configs/active_micro_alpha_research_ladder.json").exists()
        or (root / "data/active/catalogs/apex_micro.json").exists()
    ):
        raise IntegrityError("Apex micro custody reconciliation is incomplete or activated")
    return {
        "status": "PASS_INACTIVE_CUSTODY_NO_ROW_DECODE",
        "terminal_id": terminal_id,
        "dbn_count": 20,
        "sidecar_count": 20,
        "total_bytes": total_bytes,
    }


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["limits"]
    return {
        "plan_id": str(plan["plan_id"]),
        "committed_implementation_head": str(plan["committed_implementation_head"]),
        "markets": ",".join(CURRENT_ACQUISITION_MARKETS),
        "schemas": ",".join(SCHEMAS),
        "request_count": str(limits["exact_request_count"]),
        "maximum_dbn_files": str(limits["maximum_dbn_files"]),
        "maximum_sidecars": str(limits["maximum_sidecars"]),
        "maximum_total_bytes": str(limits["maximum_total_bytes"]),
        "maximum_runtime_seconds": str(limits["maximum_runtime_seconds"]),
        "maximum_external_cost_usd": "0",
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "credential_source": CREDENTIAL_SOURCE,
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
    return {key: query[key] for key in ("dataset", "schema", "stype_in", "symbols", "start", "end")}


def _write_terminal(path: Path, core: Mapping[str, object]) -> dict[str, object]:
    terminal = {**core, "terminal_id": sha256_json(core)}
    with path.open("xb") as stream:
        stream.write(canonical_bytes(terminal) + b"\n")
    return terminal


def _mark_read_only(path: Path) -> None:
    path.chmod(stat.S_IREAD)


def execute_authorized_acquisition(
    *, root: Path, authorization: OperationReceipt,
    provider_factory: Callable[[], DownloadProviderApis], credential_source: str,
    clock: Callable[[], float] = time.monotonic,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
    mark_immutable: Callable[[Path], None] = _mark_read_only,
) -> dict[str, object]:
    """Stream the exact plan to inactive custody with no row decoding."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_acquisition_plan(root=root)
    if credential_source != CREDENTIAL_SOURCE:
        raise UnauthorizedOperation("acquisition credential source is not bound")
    environment_check(root)
    destinations = [
        root / str(item[key]) for item in plan["requests"]
        for key in ("dbn_destination", "sidecar_destination")
    ]
    if any(path.exists() for path in destinations):
        raise IntegrityError("create-only acquisition destination already exists")
    usage = disk_usage(root)
    free = getattr(usage, "free", None)
    if type(free) is not int or free < plan["limits"]["required_free_disk_bytes"]:
        raise UnauthorizedOperation("insufficient disk capacity for acquisition")
    claim = authorization.consume(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    attempt = root / STAGING_ROOT / authorization.receipt_id[:16]
    boundary.assert_active_path(
        attempt.absolute(), purpose="Apex micro acquisition staging",
        subtree=STAGING_ROOT.as_posix(),
    )
    attempt.mkdir(parents=True, exist_ok=False)
    terminal_path = attempt / "terminal.json"
    started = clock()
    staged: list[dict[str, object]] = []
    provider_calls = {"get_cost": 0, "get_range": 0}
    failure_stage = "PROVIDER_FACTORY"
    base: dict[str, object] = {
        "schema_version": "apex_micro_phase1a_acquisition_terminal/1.0.0",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_claim_sha256": sha256_file(claim),
        "credential_source": CREDENTIAL_SOURCE,
        "credential_content_recorded": False,
        "maximum_external_cost_usd": "0",
        "external_cost_incurred_usd": "0",
        "automatic_retries": 0,
        "dbn_rows_decoded": 0,
        "raw_values_reported": False,
        "catalog_or_pointer_activated": False,
        "published": False,
        "registered": False,
        "model_fit_prediction_or_evaluation": False,
        "trading": False,
    }
    try:
        apis = provider_factory()
        failure_stage = "FRESH_ZERO_COST_CENSUS"
        for item in plan["requests"]:
            if clock() - started >= MAXIMUM_RUNTIME_SECONDS:
                raise UnauthorizedOperation("acquisition runtime ceiling reached before download")
            provider_calls["get_cost"] += 1
            _zero_cost(apis.get_cost(**_metadata_query(item["query"])))
        downloads = attempt / "downloads"
        downloads.mkdir()
        total_bytes = 0
        failure_stage = "DOWNLOAD_TO_INACTIVE_STAGING"
        for item in plan["requests"]:
            if clock() - started >= MAXIMUM_RUNTIME_SECONDS:
                raise UnauthorizedOperation("acquisition runtime ceiling reached")
            request_id = str(item["request_id"])
            partial = downloads / f"{request_id[:16]}.dbn.zst.partial"
            if partial.exists():
                raise IntegrityError("partial staging destination already exists")
            provider_calls["get_range"] += 1
            apis.get_range(**item["query"], path=str(partial))
            if not partial.is_file():
                raise IntegrityError("provider did not create the bound staging file")
            size = partial.stat().st_size
            if size <= 0 or size > item["request_byte_ceiling"]:
                raise UnauthorizedOperation("downloaded file exceeds its byte ceiling or is empty")
            total_bytes += size
            if total_bytes > plan["limits"]["maximum_total_bytes"]:
                raise UnauthorizedOperation("downloaded files exceed the total byte ceiling")
            digest = sha256_file(partial)
            sidecar_staging = downloads / f"{request_id[:16]}.manifest.json.partial"
            sidecar_core = {
                "schema_version": "apex_micro_inactive_dbn_manifest/1.0.0",
                "state": "INACTIVE_CUSTODY_NOT_A_RESEARCH_SOURCE",
                "plan_id": plan["plan_id"],
                "request_id": request_id,
                "exact_authorized_query": item["query"],
                "estimated_cost_usd": "0",
                "external_cost_incurred_usd": "0",
                "byte_count": size,
                "sha256": digest,
                "dbn_rows_decoded": 0,
                "payload_opened_for_row_access": False,
                "catalog_activation": False,
            }
            with sidecar_staging.open("xb") as stream:
                stream.write(canonical_bytes({
                    **sidecar_core, "manifest_id": sha256_json(sidecar_core),
                }) + b"\n")
            staged.append({
                "request_id": request_id,
                "staging_dbn": partial.relative_to(root).as_posix(),
                "staging_sidecar": sidecar_staging.relative_to(root).as_posix(),
                "dbn_destination": item["dbn_destination"],
                "sidecar_destination": item["sidecar_destination"],
                "byte_count": size,
                "sha256": digest,
            })
        if provider_calls != {"get_cost": 20, "get_range": 20} or len(staged) != 20:
            raise IntegrityError("successful acquisition call or file count drifted")
        failure_stage = "FINAL_DESTINATION_RECHECK"
        if any(path.exists() for path in destinations):
            raise IntegrityError("destination appeared after download and before finalization")
        failure_stage = "CREATE_ONLY_FINALIZATION"
        for item in staged:
            source_dbn = root / str(item["staging_dbn"])
            source_sidecar = root / str(item["staging_sidecar"])
            final_dbn = root / str(item["dbn_destination"])
            final_sidecar = root / str(item["sidecar_destination"])
            final_dbn.parent.mkdir(parents=True, exist_ok=True)
            os.link(source_dbn, final_dbn)
            os.link(source_sidecar, final_sidecar)
            source_dbn.unlink()
            source_sidecar.unlink()
            if final_dbn.stat().st_size != item["byte_count"] or sha256_file(final_dbn) != item["sha256"]:
                raise IntegrityError("final DBN differs from verified inactive staging")
            mark_immutable(final_dbn)
            mark_immutable(final_sidecar)
        core = {
            **base,
            "state": "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY",
            "provider_call_counts": provider_calls,
            "accepted_dbn_count": 20,
            "accepted_sidecar_count": 20,
            "total_bytes": sum(int(item["byte_count"]) for item in staged),
            "accepted_files": staged,
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
            "accepted_dbn_count": 0,
            "accepted_sidecar_count": 0,
            "staged_or_partially_finalized_evidence": staged,
            "terminal_written_last": True,
        }
    return _write_terminal(terminal_path, core)
