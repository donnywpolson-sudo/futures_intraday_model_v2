"""Immutable v4 metadata-only successor after the v2 Databento timeout.

The v2 plan, executor, consumed authorization, and fail-closed report remain
untouched.  This successor exposes the same six metadata/symbology methods and
no timeseries or batch-download capability.  Its only operational correction
is a separately bounded 30-second SDK timeout for the observed metadata-call
latency, under a 300-second total runtime ceiling.
"""

from __future__ import annotations

import json
import shutil
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .live_cockpit.databento_auth import resolve_databento_api_key
from .micro_alpha_databento_preflight import (
    CREDENTIAL_SOURCE,
    DISK_SAFETY_BYTES,
    MAXIMUM_PROVIDER_CALLS,
    MAXIMUM_RETRIES,
    MAXIMUM_TOTAL_ACQUISITION_BYTES,
    MetadataProviderApis,
    OBSOLETE_PLAN_ID,
    OBSOLETE_PLAN_SHA256,
    OPERATION,
    PLAN_PATH as PREDECESSOR_PLAN_PATH,
    REFERENCE_PATH,
    REPORT_PATH as PREDECESSOR_REPORT_PATH,
    SUPERSESSION_PATH,
    _decimal_zero,
    _latest_complete_end,
    _provider_query,
    _request,
    _symbology_summary,
    _write_report_create_only,
)
from .micro_alpha_pipeline import (
    CURRENT_ACQUISITION_MARKETS,
    DATASET,
    SCHEMAS,
    build_product_reference_requirements,
    phase1a_paths,
    validate_phase1a_request,
)
from .runtime_environment import require_locked_repository_environment


PLAN_PATH: Final = Path(
    "configs/apex_micro_tier01_databento_metadata_preflight_v4.json"
)
REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_metadata_preflight_v4/report.json"
)
SUPERSEDED_LOCAL_PLAN_PATH: Final = Path(
    "configs/apex_micro_tier01_databento_metadata_preflight_v3.json"
)
SUPERSEDED_LOCAL_PLAN_ID: Final = (
    "48c6f177a892190e77d82c21445641ff68588528ae6ab32268989fd5df530e6e"
)
SUPERSEDED_LOCAL_PLAN_SHA256: Final = (
    "f74c5d50b3440d5ea20f3a36e4269e01e1275e657431f814f5d2394f17345c43"
)
LOCAL_SUPERSESSION_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_metadata_preflight_v3_supersession.json"
)
PREDECESSOR_AUTHORIZATION_RECEIPT_ID: Final = (
    "83f1fdb2ed7a542bf8571edb83b3927d0ce0d4cfedad97ce6aa6b2087171b660"
)
PREDECESSOR_AUTHORIZATION_PATH: Final = Path(
    "state/authorization_uses"
) / f"{PREDECESSOR_AUTHORIZATION_RECEIPT_ID}.json"
PREDECESSOR_PLAN_ID: Final = (
    "8a0e472d83f54f8508efef18cd9adc9273cd4bc54c3b806564651ed3a09b73cd"
)
PREDECESSOR_PLAN_SHA256: Final = (
    "9de92213328d7c32c8fb0c5681f05372800b54ffc3243905782eec44c4b91bc7"
)
PREDECESSOR_REPORT_ID: Final = (
    "524624df5b4a476abb81e2bb27b817a8ed6b8acd2ad6e9d3db9e03c8cf25ff97"
)
PREDECESSOR_REPORT_SHA256: Final = (
    "9fbe8b8002cb2a224a50335b6e9c8b8f0779f5e03b5f84042d5d7dc5c58abb59"
)
PREDECESSOR_AUTHORIZATION_SHA256: Final = (
    "01b2d79c4b8cd3d960cbf0ffcdb0827fd868561e6fcb9e2fdfe91ad0c432fb10"
)
SCHEMA: Final = "apex_micro_databento_metadata_preflight/4.0.0"
REPORT_SCHEMA: Final = "apex_micro_databento_metadata_preflight_report/4.0.0"
MAXIMUM_RUNTIME_SECONDS: Final = 300
PER_CALL_TIMEOUT_SECONDS: Final = 30


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{description} is not an object")
    return value


def load_predecessor_failure(*, root: Path) -> dict[str, object]:
    """Verify the exact v2 timeout evidence before constructing the successor."""

    plan_path = root / PREDECESSOR_PLAN_PATH
    report_path = root / PREDECESSOR_REPORT_PATH
    authorization_path = root / PREDECESSOR_AUTHORIZATION_PATH
    if (
        sha256_file(plan_path) != PREDECESSOR_PLAN_SHA256
        or sha256_file(report_path) != PREDECESSOR_REPORT_SHA256
        or sha256_file(authorization_path) != PREDECESSOR_AUTHORIZATION_SHA256
    ):
        raise IntegrityError("v2 metadata preflight evidence was not preserved byte-for-byte")
    plan = _object(plan_path, "v2 metadata preflight plan")
    report = _object(report_path, "v2 metadata preflight report")
    report_core = dict(report)
    report_id = report_core.pop("report_id", None)
    if (
        plan.get("plan_id") != PREDECESSOR_PLAN_ID
        or report_id != PREDECESSOR_REPORT_ID
        or report_id != sha256_json(report_core)
        or report.get("plan_id") != PREDECESSOR_PLAN_ID
        or report.get("plan_sha256") != PREDECESSOR_PLAN_SHA256
        or report.get("authorization_receipt_id")
        != PREDECESSOR_AUTHORIZATION_RECEIPT_ID
        or report.get("authorization_claim_sha256")
        != PREDECESSOR_AUTHORIZATION_SHA256
        or report.get("state") != "FAIL_CLOSED_METADATA_ONLY"
        or report.get("failure_code") != "METADATA_PREFLIGHT_FAIL_CLOSED"
        or report.get("exception_type") != "ReadTimeout"
        or report.get("provider_call_counts")
        != {"list_datasets": 1, "list_schemas": 1}
        or report.get("provider_call_total") != 2
        or report.get("maximum_external_cost_usd") != "0"
        or report.get("external_cost_incurred_usd") != "0"
        or report.get("automatic_retries") != 0
        or report.get("timeseries_download_calls") != 0
        or report.get("historical_rows_read") is not False
        or report.get("dbn_files_created") != 0
    ):
        raise IntegrityError("v2 metadata preflight timeout evidence drifted")
    return report


def load_superseded_local_preparation(*, root: Path) -> dict[str, object]:
    """Verify the unexecuted v3 plan and its immutable supersession record."""

    plan_path = root / SUPERSEDED_LOCAL_PLAN_PATH
    classification_path = root / LOCAL_SUPERSESSION_PATH
    if sha256_file(plan_path) != SUPERSEDED_LOCAL_PLAN_SHA256:
        raise IntegrityError("v3 local preflight preparation was not preserved byte-for-byte")
    plan = _object(plan_path, "v3 local preflight preparation")
    classification = _object(
        classification_path, "v3 local preflight supersession classification"
    )
    if (
        plan.get("plan_id") != SUPERSEDED_LOCAL_PLAN_ID
        or classification.get("classification")
        != "SUPERSEDED_LOCAL_PREPARATION"
        or classification.get("reason")
        != "SELF_HASH_DRIFT_BEFORE_STAGING_OR_EXECUTION"
        or classification.get("plan_path") != SUPERSEDED_LOCAL_PLAN_PATH.as_posix()
        or classification.get("plan_id") != SUPERSEDED_LOCAL_PLAN_ID
        or classification.get("plan_sha256") != SUPERSEDED_LOCAL_PLAN_SHA256
        or classification.get("provider_access_performed") is not False
        or classification.get("authorization_consumed") is not False
        or classification.get("report_created") is not False
        or classification.get("execution_forbidden") is not True
    ):
        raise IntegrityError("v3 local preflight supersession classification drifted")
    return classification


def build_file_metadata_provider_apis(
    *, root: Path, historical_factory: Callable[..., object] | None = None,
) -> MetadataProviderApis:
    """Read only file api.env and expose no download-capable SDK surface."""

    key = resolve_databento_api_key(key_files=(root / "api.env",))
    if not key:
        raise UnauthorizedOperation("the bound file api.env credential is unavailable")
    if historical_factory is None:
        from databento import Historical

        historical_factory = Historical
    client = historical_factory(key=key)
    metadata = getattr(client, "metadata", None)
    symbology = getattr(client, "symbology", None)
    methods = {
        "list_datasets": getattr(metadata, "list_datasets", None),
        "list_schemas": getattr(metadata, "list_schemas", None),
        "get_dataset_range": getattr(metadata, "get_dataset_range", None),
        "resolve": getattr(symbology, "resolve", None),
        "get_cost": getattr(metadata, "get_cost", None),
        "get_billable_size": getattr(metadata, "get_billable_size", None),
    }
    if not all(callable(value) for value in methods.values()):
        raise IntegrityError("Databento historical client lacks required metadata APIs")
    for api in (metadata, symbology):
        if hasattr(api, "TIMEOUT"):
            setattr(api, "TIMEOUT", PER_CALL_TIMEOUT_SECONDS)
    return MetadataProviderApis(**methods)  # type: ignore[arg-type]


def build_plan(*, root: Path) -> dict[str, object]:
    load_predecessor_failure(root=root)
    load_superseded_local_preparation(root=root)
    implementation_paths = (
        "src/futures_rebuild/micro_alpha_pipeline.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v4.py",
        "src/futures_rebuild/micro_alpha_acquisition.py",
        "src/futures_rebuild/alpha_research_architecture.py",
    )
    references = build_product_reference_requirements()
    if _object(root / REFERENCE_PATH, "Apex micro product references") != references:
        raise IntegrityError("Apex micro product reference artifact drifted")
    requests = [
        _request(market=market, schema=schema)
        for market in CURRENT_ACQUISITION_MARKETS
        for schema in SCHEMAS
    ]
    core: dict[str, object] = {
        "schema_version": SCHEMA,
        "classification": "PREPARED_METADATA_ONLY_PROVIDER_APPROVAL_REQUIRED",
        "state": "PREPARED_NOT_EXECUTED",
        "operation": OPERATION,
        "lane_id": "apex_integer_micro_11",
        "dataset": DATASET,
        "standard_plan_requirement": "DATABENTO_STANDARD",
        "predecessor_execution": {
            "plan_path": PREDECESSOR_PLAN_PATH.as_posix(),
            "plan_id": PREDECESSOR_PLAN_ID,
            "plan_sha256": PREDECESSOR_PLAN_SHA256,
            "report_path": PREDECESSOR_REPORT_PATH.as_posix(),
            "report_id": PREDECESSOR_REPORT_ID,
            "report_sha256": PREDECESSOR_REPORT_SHA256,
            "authorization_use_path": PREDECESSOR_AUTHORIZATION_PATH.as_posix(),
            "authorization_receipt_id": PREDECESSOR_AUTHORIZATION_RECEIPT_ID,
            "authorization_use_sha256": PREDECESSOR_AUTHORIZATION_SHA256,
            "state": "FAIL_CLOSED_METADATA_ONLY",
            "failure_code": "METADATA_PREFLIGHT_FAIL_CLOSED",
            "exception_type": "ReadTimeout",
            "provider_call_total": 2,
            "automatic_retries": 0,
            "preservation": "IMMUTABLE_NO_OVERWRITE_DELETE_OR_RELABEL",
        },
        "superseded_local_preparation": {
            "classification_path": LOCAL_SUPERSESSION_PATH.as_posix(),
            "plan_path": SUPERSEDED_LOCAL_PLAN_PATH.as_posix(),
            "plan_id": SUPERSEDED_LOCAL_PLAN_ID,
            "plan_sha256": SUPERSEDED_LOCAL_PLAN_SHA256,
            "reason": "SELF_HASH_DRIFT_BEFORE_STAGING_OR_EXECUTION",
            "execution_forbidden": True,
        },
        "correction": {
            "reason": "OBSERVED_LIST_SCHEMAS_READ_TIMEOUT_AT_TEN_SECONDS",
            "provider_sdk_default_timeout_seconds": 100,
            "predecessor_per_call_timeout_seconds": 10,
            "successor_per_call_timeout_seconds": PER_CALL_TIMEOUT_SECONDS,
            "scope_change": "TIMEOUT_ONLY_NO_MARKET_SCHEMA_OR_ENDPOINT_CHANGE",
        },
        "obsolete_plan": {
            "plan_id": OBSOLETE_PLAN_ID,
            "sha256": OBSOLETE_PLAN_SHA256,
            "execution_forbidden": True,
        },
        "plan_bindings": {
            **{path: sha256_file(root / path) for path in implementation_paths},
            REFERENCE_PATH.as_posix(): sha256_file(root / REFERENCE_PATH),
            SUPERSESSION_PATH.as_posix(): sha256_file(root / SUPERSESSION_PATH),
            PREDECESSOR_PLAN_PATH.as_posix(): PREDECESSOR_PLAN_SHA256,
            PREDECESSOR_REPORT_PATH.as_posix(): PREDECESSOR_REPORT_SHA256,
            PREDECESSOR_AUTHORIZATION_PATH.as_posix(): PREDECESSOR_AUTHORIZATION_SHA256,
            SUPERSEDED_LOCAL_PLAN_PATH.as_posix(): SUPERSEDED_LOCAL_PLAN_SHA256,
            LOCAL_SUPERSESSION_PATH.as_posix(): sha256_file(
                root / LOCAL_SUPERSESSION_PATH
            ),
        },
        "requests": requests,
        "limits": {
            "exact_request_definitions": 20,
            "exact_provider_call_ceiling": MAXIMUM_PROVIDER_CALLS,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "per_call_timeout_seconds": PER_CALL_TIMEOUT_SECONDS,
            "maximum_external_cost_usd": "0",
            "maximum_retries": MAXIMUM_RETRIES,
            "maximum_total_acquisition_bytes": MAXIMUM_TOTAL_ACQUISITION_BYTES,
            "disk_safety_bytes": DISK_SAFETY_BYTES,
            "maximum_phase1a_dbn_files": 20,
            "maximum_phase1a_sidecars": 20,
        },
        "checks": {
            "dataset_entitlement": True,
            "exact_schema_entitlement": True,
            "parent_and_continuous_symbology": True,
            "product_effective_dates": True,
            "latest_complete_end_exclusive": True,
            "estimated_bytes_per_request": True,
            "total_byte_ceiling": True,
            "disk_capacity": True,
            "destination_conflict_census": True,
        },
        "credential_source": {
            "path": "api.env",
            "binding": "PATH_ONLY_CONTENT_NEVER_REPORTED",
        },
        "output": {
            "path": REPORT_PATH.as_posix(),
            "create_only": True,
            "price_free": True,
        },
        "forbidden": {
            "timeseries_download": True,
            "batch_download": True,
            "data_dbn_write": True,
            "dbn_decode": True,
            "historical_row_read": True,
            "raw_values_in_report": True,
            "credential_log_stage_or_report": True,
            "publication": True,
            "catalog_activation": True,
            "registration": True,
            "trading": True,
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_plan(plan: Mapping[str, object], *, root: Path) -> dict[str, object]:
    expected = build_plan(root=root)
    if dict(plan) != expected:
        raise IntegrityError("Apex micro metadata preflight successor drifted")
    requests = plan.get("requests")
    if not isinstance(requests, list) or len(requests) != 20:
        raise IntegrityError("Apex micro preflight must contain exactly 20 requests")
    for request in requests:
        if not isinstance(request, Mapping):
            raise IntegrityError("Apex micro preflight request is malformed")
        core = dict(request)
        request_id = core.pop("request_id", None)
        if request_id != sha256_json(core):
            raise IntegrityError("Apex micro preflight request identity drifted")
        validate_phase1a_request(core)
    return dict(plan)


def load_plan(*, root: Path) -> dict[str, object]:
    return validate_plan(_object(root / PLAN_PATH, "v3 metadata preflight plan"), root=root)


def _required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "plan_id": str(plan["plan_id"]),
        "predecessor_report_id": PREDECESSOR_REPORT_ID,
        "request_definitions": "20",
        "markets": ",".join(CURRENT_ACQUISITION_MARKETS),
        "schemas": ",".join(SCHEMAS),
        "provider": "Databento",
        "dataset": DATASET,
        "provider_call_ceiling": str(MAXIMUM_PROVIDER_CALLS),
        "maximum_runtime_seconds": str(MAXIMUM_RUNTIME_SECONDS),
        "per_call_timeout_seconds": str(PER_CALL_TIMEOUT_SECONDS),
        "maximum_external_cost_usd": "0",
        "maximum_retries": "0",
        "credential_source": CREDENTIAL_SOURCE,
        "report_path": REPORT_PATH.as_posix(),
        "timeseries_download": "false",
        "historical_row_read": "false",
        "data_dbn_write": "false",
        "publication": "false",
        "catalog_activation": "false",
        "registration": "false",
        "trading": "false",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return _required_scope(root=root, plan=plan)


@dataclass
class _CallBudget:
    clock: Callable[[], float]

    def __post_init__(self) -> None:
        self.started = self.clock()
        self.counts: dict[str, int] = {}

    def call(self, name: str, function: Callable[..., object], **kwargs: object) -> object:
        if sum(self.counts.values()) >= MAXIMUM_PROVIDER_CALLS:
            raise UnauthorizedOperation("metadata preflight provider call ceiling reached")
        if self.clock() - self.started >= MAXIMUM_RUNTIME_SECONDS:
            raise UnauthorizedOperation("metadata preflight runtime ceiling reached")
        self.counts[name] = self.counts.get(name, 0) + 1
        return function(**kwargs)


def execute_preflight(
    *,
    root: Path,
    authorization: OperationReceipt,
    provider_factory: Callable[[], MetadataProviderApis],
    credential_source: str,
    clock: Callable[[], float] = time.monotonic,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
) -> dict[str, object]:
    """Consume one successor authorization and run only metadata calls."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_plan(root=root)
    report_path = root / REPORT_PATH
    boundary.assert_active_path(
        report_path.absolute(),
        purpose="Apex micro successor metadata report",
        subtree="state/unpublished_evidence/apex_micro_metadata_preflight_v4",
    )
    if report_path.exists():
        raise IntegrityError("Apex micro successor metadata report is create-only")
    if credential_source != CREDENTIAL_SOURCE:
        raise UnauthorizedOperation("metadata preflight credential source is not bound")
    environment_check(root)
    claim = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=_required_scope(root=root, plan=plan),
    )
    budget = _CallBudget(clock=clock)
    base: dict[str, object] = {
        "schema_version": REPORT_SCHEMA,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "predecessor_report_id": PREDECESSOR_REPORT_ID,
        "predecessor_report_sha256": PREDECESSOR_REPORT_SHA256,
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_claim_sha256": sha256_file(claim),
        "price_free": True,
        "credential_source": CREDENTIAL_SOURCE,
        "credential_content_recorded": False,
        "timeseries_download_calls": 0,
        "historical_rows_read": False,
        "dbn_files_created": 0,
        "catalog_activated": False,
        "published": False,
        "registered": False,
        "trading": False,
        "maximum_external_cost_usd": "0",
        "external_cost_incurred_usd": "0",
        "automatic_retries": 0,
    }
    try:
        apis = provider_factory()
        datasets = budget.call("list_datasets", apis.list_datasets)
        if not isinstance(datasets, list) or not all(type(item) is str for item in datasets):
            raise IntegrityError("provider dataset entitlement response is invalid")
        if DATASET not in datasets:
            raise UnauthorizedOperation("GLBX.MDP3 is not entitled")
        schemas = budget.call("list_schemas", apis.list_schemas, dataset=DATASET)
        if not isinstance(schemas, list) or not all(type(item) is str for item in schemas):
            raise IntegrityError("provider schema entitlement response is invalid")
        if not set(SCHEMAS).issubset(schemas):
            raise UnauthorizedOperation(
                "a required Databento Standard historical schema is unavailable"
            )
        dataset_range = budget.call(
            "get_dataset_range", apis.get_dataset_range, dataset=DATASET
        )
        end = _latest_complete_end(dataset_range)

        symbology: dict[str, dict[str, dict[str, object]]] = {}
        for market in CURRENT_ACQUISITION_MARKETS:
            symbology[market] = {}
            for stype_in, symbol in (
                ("parent", f"{market}.FUT"),
                ("continuous", f"{market}.v.0"),
            ):
                raw = budget.call(
                    "resolve",
                    apis.resolve,
                    dataset=DATASET,
                    symbols=[symbol],
                    stype_in=stype_in,
                    stype_out="instrument_id",
                    start_date="2000-01-01",
                    end_date=end,
                )
                symbology[market][stype_in] = _symbology_summary(
                    raw, symbol=symbol, stype_in=stype_in, end=end
                )
            if (
                symbology[market]["parent"]["first_effective_date"]
                != symbology[market]["continuous"]["first_effective_date"]
            ):
                raise IntegrityError(
                    "parent and continuous product effective dates disagree"
                )

        estimates: list[dict[str, object]] = []
        total_estimated = 0
        destinations: list[str] = []
        conflicts: list[str] = []
        for request in plan["requests"]:
            query = _provider_query(request, end=end)
            _decimal_zero(budget.call("get_cost", apis.get_cost, **query))
            size = budget.call(
                "get_billable_size", apis.get_billable_size, **query
            )
            if type(size) is not int or size < 0:
                raise IntegrityError("provider billable-size response is invalid")
            total_estimated += size
            market = str(request["market"])
            effective = str(symbology[market]["parent"]["first_effective_date"])
            query_start = max("2018-01-01", effective)
            interval = f"{query_start}_{end}"
            paths = phase1a_paths(
                market=market,
                schema=str(request["schema"]),
                year=int(query_start[:4]),
                interval=interval,
            )
            for destination in paths.values():
                destinations.append(destination)
                if (root / destination).exists():
                    conflicts.append(destination)
            estimates.append(
                {
                    "request_id": request["request_id"],
                    "estimated_bytes": size,
                    "estimated_cost_usd": "0",
                    "product_effective_date": effective,
                    "acquisition_start": query_start,
                    "end_exclusive": end,
                    "dbn_destination": paths["dbn"],
                    "sidecar_destination": paths["sidecar"],
                }
            )
        if sum(budget.counts.values()) != MAXIMUM_PROVIDER_CALLS:
            raise IntegrityError("successful metadata preflight call count drifted")
        if len(set(destinations)) != 40:
            raise IntegrityError("metadata preflight destinations collide internally")
        byte_ceiling = total_estimated + max(total_estimated // 10, 1024**2)
        if byte_ceiling > MAXIMUM_TOTAL_ACQUISITION_BYTES:
            raise UnauthorizedOperation("estimated acquisition exceeds the fixed byte ceiling")
        usage = disk_usage(root)
        free = getattr(usage, "free", None)
        if type(free) is not int or free < 0:
            raise IntegrityError("disk-capacity response is invalid")
        required_free = byte_ceiling + DISK_SAFETY_BYTES
        if free < required_free:
            raise UnauthorizedOperation("insufficient disk capacity for bounded acquisition")
        if conflicts:
            raise UnauthorizedOperation("an exact Phase 1A destination already exists")
        product_dates = {
            market: symbology[market]["parent"]["first_effective_date"]
            for market in CURRENT_ACQUISITION_MARKETS
        }
        core = {
            **base,
            "state": "PASS_METADATA_ONLY",
            "provider_call_counts": dict(sorted(budget.counts.items())),
            "provider_call_total": sum(budget.counts.values()),
            "dataset_entitlement": "PASS",
            "standard_plan_schema_entitlement": "PASS_ZERO_COST_EACH_REQUEST",
            "latest_complete_end_exclusive": end,
            "product_effective_dates": product_dates,
            "symbology_summaries": symbology,
            "request_estimates": estimates,
            "total_estimated_bytes": total_estimated,
            "total_acquisition_byte_ceiling": byte_ceiling,
            "fixed_maximum_total_acquisition_bytes": MAXIMUM_TOTAL_ACQUISITION_BYTES,
            "disk_free_bytes_observed": free,
            "disk_required_free_bytes": required_free,
            "destination_conflict_count": 0,
            "request_definition_count": 20,
        }
    except Exception as exc:
        exception_type = type(exc).__name__
        failure_code = (
            "PROVIDER_TIMEOUT"
            if "timeout" in exception_type.lower()
            else "UNEXPECTED_NONZERO_COST"
            if "nonzero cost" in str(exc)
            else "INSUFFICIENT_DISK"
            if "disk capacity" in str(exc)
            else "DESTINATION_CONFLICT"
            if "destination already exists" in str(exc)
            else "METADATA_PREFLIGHT_FAIL_CLOSED"
        )
        core = {
            **base,
            "state": "FAIL_CLOSED_METADATA_ONLY",
            "failure_code": failure_code,
            "provider_call_counts": dict(sorted(budget.counts.items())),
            "provider_call_total": sum(budget.counts.values()),
            "exception_type": exception_type,
        }
    return _write_report_create_only(report_path, core)
