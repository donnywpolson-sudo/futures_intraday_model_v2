"""Opaque-single-partial-safe Apex micro metadata preflight successor.

The executed v9 plan, consumed authorization, and fail-closed report remain
immutable. V10 treats the content of one discovery-only ``partial`` entry as
opaque and validates only its exact cardinality under separately exact
single-symbol request, echo, and result-key bindings. It derives the first
mapping date, then re-resolves both parent and continuous symbology from that
date with empty ``partial`` and ``not_found`` lists required. Dataset-start
ambiguity and every later gap still fail closed. No download-capable API is
exposed to this executor.
"""

from __future__ import annotations

import shutil
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .micro_alpha_databento_preflight import (
    CREDENTIAL_SOURCE,
    DISK_SAFETY_BYTES,
    MAXIMUM_RETRIES,
    MAXIMUM_TOTAL_ACQUISITION_BYTES,
    MetadataProviderApis,
    OBSOLETE_PLAN_ID,
    OBSOLETE_PLAN_SHA256,
    OPERATION,
    REFERENCE_PATH,
    SUPERSESSION_PATH,
    _decimal_zero,
    _provider_query,
    _request,
    _write_report_create_only,
)
from .micro_alpha_databento_preflight_v5 import build_file_metadata_provider_apis
from .micro_alpha_databento_preflight_v8 import (
    MAXIMUM_ANNUAL_REQUESTS,
    MAXIMUM_RUNTIME_SECONDS,
    PER_CALL_TIMEOUT_SECONDS,
    _dataset_bounds,
    _object,
)
from .micro_alpha_databento_preflight_v9 import (
    PLAN_PATH as PREDECESSOR_PLAN_PATH,
    REPORT_PATH as PREDECESSOR_REPORT_PATH,
)
from .micro_alpha_pipeline import (
    CURRENT_ACQUISITION_MARKETS,
    DATASET,
    SCHEMAS,
    annual_market_year_intervals,
    build_product_reference_requirements,
    phase1a_paths,
    validate_phase1a_request,
)
from .runtime_environment import require_locked_repository_environment


PLAN_PATH: Final = Path(
    "configs/apex_micro_tier01_databento_metadata_preflight_v10.json"
)
REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_metadata_preflight_v10/report.json"
)
PREDECESSOR_PLAN_ID: Final = (
    "522341da19e8f07c79e9aed02a19e743ee6ef8d6dee1adfcb333e851f5b00c39"
)
PREDECESSOR_PLAN_SHA256: Final = (
    "3cd8afab45ef40026d4c63b34bac95a7d2ef0d432c8cacd7be366cc346c97d3c"
)
PREDECESSOR_REPORT_ID: Final = (
    "058579916d5c20cd0438894af4641dc404f3b5e8af7e7d723cc524b4020197c0"
)
PREDECESSOR_REPORT_SHA256: Final = (
    "feb7d21ef6c8161163b5420e7ff7da49c6896e382f852118b12818ad7f56d4e5"
)
PREDECESSOR_AUTHORIZATION_RECEIPT_ID: Final = (
    "018cdc1bd9f706f60388d91b06bfd38bbb405258a9523a18285ca33d295c1a26"
)
PREDECESSOR_AUTHORIZATION_PATH: Final = Path("state/authorization_uses") / (
    f"{PREDECESSOR_AUTHORIZATION_RECEIPT_ID}.json"
)
PREDECESSOR_AUTHORIZATION_SHA256: Final = (
    "88857111b87688a041dc7a27dc8a6a4238f989272e89a1b70c1f5ec6ec54c60a"
)
SCHEMA: Final = "apex_micro_databento_metadata_preflight/10.0.0"
REPORT_SCHEMA: Final = "apex_micro_databento_metadata_preflight_report/10.0.0"
MAXIMUM_PROVIDER_CALLS: Final = 375
_SYMBOLOGY_KEYS: Final = {
    "result",
    "symbols",
    "stype_in",
    "stype_out",
    "start_date",
    "end_date",
    "partial",
    "not_found",
    "message",
    "status",
}


def load_predecessor_failure(*, root: Path) -> dict[str, object]:
    """Verify the exact v9 failure and its one-use authorization evidence."""

    plan_path = root / PREDECESSOR_PLAN_PATH
    report_path = root / PREDECESSOR_REPORT_PATH
    authorization_path = root / PREDECESSOR_AUTHORIZATION_PATH
    if (
        sha256_file(plan_path) != PREDECESSOR_PLAN_SHA256
        or sha256_file(report_path) != PREDECESSOR_REPORT_SHA256
        or sha256_file(authorization_path) != PREDECESSOR_AUTHORIZATION_SHA256
    ):
        raise IntegrityError("v9 metadata preflight evidence was not preserved byte-for-byte")
    plan = _object(plan_path, "v9 metadata preflight plan")
    plan_core = dict(plan)
    plan_id = plan_core.pop("plan_id", None)
    report = _object(report_path, "v9 metadata preflight report")
    report_core = dict(report)
    report_id = report_core.pop("report_id", None)
    if (
        plan_id != PREDECESSOR_PLAN_ID
        or plan_id != sha256_json(plan_core)
        or report_id != PREDECESSOR_REPORT_ID
        or report_id != sha256_json(report_core)
        or report.get("plan_id") != PREDECESSOR_PLAN_ID
        or report.get("plan_sha256") != PREDECESSOR_PLAN_SHA256
        or report.get("authorization_receipt_id")
        != PREDECESSOR_AUTHORIZATION_RECEIPT_ID
        or report.get("authorization_claim_sha256")
        != PREDECESSOR_AUTHORIZATION_SHA256
        or report.get("state") != "FAIL_CLOSED_METADATA_ONLY"
        or report.get("failure_code") != "PARTIAL_OR_NOT_FOUND_SYMBOLOGY"
        or report.get("exception_type") != "UnauthorizedOperation"
        or report.get("failed_provider_operation") != "resolve"
        or report.get("failed_provider_call_ordinal") != 4
        or report.get("failed_request_context")
        != {
            "market": "MES",
            "stage": "discovery",
            "stype_in": "parent",
            "symbol": "MES.FUT",
        }
        or report.get("failed_validation_field") != "partial"
        or report.get("provider_call_counts")
        != {
            "get_dataset_range": 1,
            "list_datasets": 1,
            "list_schemas": 1,
            "resolve": 1,
        }
        or report.get("provider_call_total") != 4
        or report.get("provider_http_status") is not None
        or report.get("provider_error_message_recorded") is not False
        or report.get("maximum_external_cost_usd") != "0"
        or report.get("external_cost_incurred_usd") != "0"
        or report.get("automatic_retries") != 0
        or report.get("timeseries_download_calls") != 0
        or report.get("historical_rows_read") is not False
        or report.get("dbn_files_created") != 0
        or report.get("credential_content_recorded") is not False
    ):
        raise IntegrityError("v9 metadata preflight failure evidence drifted")
    return report


def build_plan(*, root: Path) -> dict[str, object]:
    failure = load_predecessor_failure(root=root)
    predecessor_plan = _object(
        root / PREDECESSOR_PLAN_PATH, "v9 metadata preflight plan"
    )
    implementation_paths = (
        "configs/dependency_lock_receipt.json",
        "src/futures_rebuild/boundary.py",
        "src/futures_rebuild/canonical.py",
        "src/futures_rebuild/errors.py",
        "src/futures_rebuild/live_cockpit/databento_auth.py",
        "src/futures_rebuild/micro_alpha_databento_preflight.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v5.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v6.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v7.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v8.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v9.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v10.py",
        "src/futures_rebuild/micro_alpha_pipeline.py",
        "src/futures_rebuild/micro_alpha_acquisition.py",
        "src/futures_rebuild/alpha_research_architecture.py",
        "src/futures_rebuild/runtime_environment.py",
    )
    references = build_product_reference_requirements()
    if _object(root / REFERENCE_PATH, "Apex micro product references") != references:
        raise IntegrityError("Apex micro product reference artifact drifted")
    requests = [
        _request(market=market, schema=schema)
        for market in CURRENT_ACQUISITION_MARKETS
        for schema in SCHEMAS
    ]
    if requests != predecessor_plan.get("requests"):
        raise IntegrityError("v10 market/schema request definitions drifted from v9")
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
            "state": failure["state"],
            "failure_code": failure["failure_code"],
            "exception_type": failure["exception_type"],
            "provider_call_total": 4,
            "failed_call_inference": {
                "basis": (
                    "SEALED_V9_EXACT_SINGLE_SYMBOL_REQUEST_ECHO_AND_RESULT_KEY_"
                    "PLUS_NONEMPTY_PARTIAL_STRING_LIST"
                ),
                "call_ordinal": 4,
                "operation": "resolve",
                "market": "MES",
                "symbol": "MES.FUT",
                "stype_in": "parent",
                "local_defect": (
                    "OPAQUE_PROVIDER_PARTIAL_CONTENT_COMPARED_TO_REQUESTED_"
                    "SYMBOL_BEFORE_CARDINALITY_PROOF"
                ),
                "affected_field_disposition": (
                    "V9_SANITIZED_PARTIAL_FIELD_NAME_ONLY_CONTENT_NOT_RECORDED"
                ),
                "inference_state": (
                    "SINGLE_OPAQUE_ENTRY_CARDINALITY_REQUIRES_INDEPENDENT_EXACT_"
                    "REQUEST_ECHO_RESULT_BINDINGS"
                ),
                "provider_message_recorded": False,
            },
            "external_cost_incurred_usd": "0",
            "automatic_retries": 0,
            "preservation": "IMMUTABLE_NO_OVERWRITE_DELETE_OR_RELABEL",
        },
        "correction": {
            "reason": "PROVIDER_PARTIAL_ENTRY_CONTENT_IS_OPAQUE",
            "discovery_parent_partial_validation": (
                "EMPTY_OR_EXACT_ONE_OPAQUE_STRING_UNDER_EXACT_SINGLE_SYMBOL_"
                "REQUEST_ECHO_AND_RESULT_KEY"
            ),
            "discovery_partial_content_recording": "FORBIDDEN",
            "verification_partial_validation": "EXACT_EMPTY_STRING_LIST_REQUIRED",
            "not_found_validation": "ALWAYS_EXACT_EMPTY_STRING_LIST_REQUIRED",
            "two_stage_verification": "DISCOVER_FIRST_MAPPING_THEN_REQUERY_PARENT_AND_CONTINUOUS",
            "dataset_start_effective_date": "FAIL_CLOSED_EXACT_PRODUCT_DATE_UNRESOLVED",
            "field_specific_failure_diagnostics": "ORDERED_SANITIZED_FIELD_NAME_ONLY_NO_VALUE",
            "provider_error_recording": (
                "HTTP_STATUS_AND_PRICE_FREE_CALL_CONTEXT_ONLY_NO_MESSAGE_OR_CREDENTIAL"
            ),
            "file_partition": (
                "ONE_DBN_AND_ADJACENT_SIDECAR_PER_MARKET_SCHEMA_CALENDAR_YEAR"
            ),
            "scope_change": "NONE_SAME_CALL_CEILING_MARKETS_SCHEMAS_AND_ENDPOINTS",
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
        },
        "requests": requests,
        "limits": {
            "exact_request_definitions": 20,
            "exact_provider_call_ceiling": MAXIMUM_PROVIDER_CALLS,
            "maximum_annual_market_schema_requests": MAXIMUM_ANNUAL_REQUESTS,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "per_call_timeout_seconds": PER_CALL_TIMEOUT_SECONDS,
            "maximum_external_cost_usd": "0",
            "maximum_retries": MAXIMUM_RETRIES,
            "maximum_total_acquisition_bytes": MAXIMUM_TOTAL_ACQUISITION_BYTES,
            "disk_safety_bytes": DISK_SAFETY_BYTES,
            "maximum_phase1a_dbn_files": MAXIMUM_ANNUAL_REQUESTS,
            "maximum_phase1a_sidecars": MAXIMUM_ANNUAL_REQUESTS,
        },
        "checks": {
            "dataset_entitlement": True,
            "exact_schema_entitlement": True,
            "provider_range_bounded_symbology": True,
            "parent_and_continuous_symbology": True,
            "post_effective_symbology_status_exact_empty_list_shape": True,
            "prelaunch_discovery_single_opaque_partial_cardinality_only": True,
            "prelaunch_discovery_exact_single_symbol_echo_and_result_key": True,
            "post_effective_parent_and_continuous_empty_status": True,
            "symbology_success_message_bounded_allowlist": True,
            "symbology_echo_field_specific_fail_closed": True,
            "exact_product_effective_dates_or_fail_closed": True,
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
        raise IntegrityError("Apex micro metadata preflight v10 drifted")
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
    return validate_plan(_object(root / PLAN_PATH, "v10 metadata preflight plan"), root=root)


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "plan_id": str(plan["plan_id"]),
        "predecessor_report_id": PREDECESSOR_REPORT_ID,
        "request_definitions": "20",
        "maximum_annual_market_schema_requests": str(MAXIMUM_ANNUAL_REQUESTS),
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


@dataclass
class _CallBudget:
    clock: Callable[[], float]

    def __post_init__(self) -> None:
        self.started = self.clock()
        self.counts: dict[str, int] = {}
        self.last_name: str | None = None
        self.last_context: dict[str, str] = {}

    def call(
        self,
        name: str,
        function: Callable[..., object],
        *,
        context: Mapping[str, str] | None = None,
        **kwargs: object,
    ) -> object:
        elapsed = self.clock() - self.started
        remaining = MAXIMUM_RUNTIME_SECONDS - elapsed
        if sum(self.counts.values()) >= MAXIMUM_PROVIDER_CALLS:
            raise UnauthorizedOperation("metadata preflight provider call ceiling reached")
        if remaining <= 0:
            raise UnauthorizedOperation("metadata preflight runtime ceiling reached")
        owner = getattr(function, "__self__", None)
        if owner is not None and hasattr(owner, "TIMEOUT"):
            setattr(owner, "TIMEOUT", min(float(PER_CALL_TIMEOUT_SECONDS), remaining))
        self.last_name = name
        self.last_context = dict(sorted((context or {}).items()))
        self.counts[name] = self.counts.get(name, 0) + 1
        value = function(**kwargs)
        if self.clock() - self.started > MAXIMUM_RUNTIME_SECONDS:
            raise UnauthorizedOperation("metadata preflight runtime ceiling reached")
        return value


def _symbol_status(
    value: object,
    *,
    name: str,
    allow_single_symbol_partial: bool,
) -> int:
    if type(value) is not list or any(type(item) is not str for item in value):
        raise IntegrityError(f"provider symbology {name} field is not an exact string list")
    if not value:
        return 0
    if name == "partial" and allow_single_symbol_partial and len(value) == 1:
        return 1
    raise UnauthorizedOperation(f"provider symbology returned nonempty {name} symbols")


def _symbology_summary(
    value: object,
    *,
    symbol: str,
    stype_in: str,
    query_start: str,
    end: str,
    allow_single_symbol_partial: bool = False,
) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != _SYMBOLOGY_KEYS:
        raise IntegrityError("provider symbology returned unexpected fields")
    expected_echoes: tuple[tuple[str, object], ...] = (
        ("stype_in", stype_in),
        ("stype_out", "instrument_id"),
        ("symbols", [symbol]),
        ("start_date", query_start),
        ("end_date", end),
    )
    for field, expected in expected_echoes:
        if value.get(field) != expected:
            raise IntegrityError(f"provider symbology {field} echo drifted")
    partial_count = _symbol_status(
        value.get("partial"),
        name="partial",
        allow_single_symbol_partial=allow_single_symbol_partial,
    )
    not_found_count = _symbol_status(
        value.get("not_found"),
        name="not_found",
        allow_single_symbol_partial=False,
    )
    if type(value.get("status")) is not int or value.get("status") != 0:
        raise IntegrityError("provider symbology status echo drifted")
    if type(value.get("message")) is not str or value.get("message") not in {"", "OK"}:
        raise IntegrityError("provider symbology success message echo drifted")
    result = value.get("result")
    if not isinstance(result, Mapping) or set(result) != {symbol}:
        raise IntegrityError("provider symbology result is incomplete")
    entries = result.get(symbol)
    if not isinstance(entries, list) or not entries:
        raise IntegrityError("provider symbology has no instrument identity mapping")
    dates: list[str] = []
    for entry in entries:
        if not isinstance(entry, Mapping) or set(entry) != {"d0", "d1", "s"}:
            raise IntegrityError("provider symbology interval has unexpected fields")
        if type(entry.get("s")) not in {str, int} or str(entry.get("s")) == "":
            raise IntegrityError("provider symbology instrument identity is absent")
        d0, d1 = entry.get("d0"), entry.get("d1")
        if type(d0) is not str or type(d1) is not str or len(d0) != 10 or len(d1) != 10:
            raise IntegrityError("provider symbology interval date is invalid")
        if d0 < query_start or d0 >= d1:
            raise IntegrityError("provider symbology interval is outside the bound query")
        dates.append(d0)
    first = min(dates)
    if allow_single_symbol_partial and (partial_count == 1) != (first > query_start):
        raise IntegrityError("provider symbology prelaunch partial disposition drifted")
    if not allow_single_symbol_partial and first != query_start:
        raise IntegrityError("provider symbology post-effective coverage start drifted")
    disposition = (
        "ACTIVE_AT_PROVIDER_DATASET_START_EXACT_PRODUCT_EFFECTIVE_DATE_UNRESOLVED"
        if allow_single_symbol_partial and first == query_start
        else "PROVIDER_PRELAUNCH_PARTIAL_FIRST_MAPPING_DATE"
        if allow_single_symbol_partial
        else "POST_EFFECTIVE_FULL_COVERAGE_VERIFIED"
    )
    return {
        "first_effective_date": first,
        "effective_date_disposition": disposition,
        "query_start_date": query_start,
        "mapping_interval_count": len(entries),
        "mapping_sha256": sha256_json(value),
        "partial_count": partial_count,
        "not_found_count": not_found_count,
    }


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
    """Consume one v10 authorization and run metadata-only operations."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_plan(root=root)
    report_path = root / REPORT_PATH
    boundary.assert_active_path(
        report_path.absolute(),
        purpose="Apex micro v10 metadata report",
        subtree="state/unpublished_evidence/apex_micro_metadata_preflight_v10",
    )
    if report_path.exists():
        raise IntegrityError("Apex micro v10 metadata report is create-only")
    if credential_source != CREDENTIAL_SOURCE:
        raise UnauthorizedOperation("metadata preflight credential source is not bound")
    environment_check(root)
    claim = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
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
    observed: dict[str, object] = {}
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
        bounds = _dataset_bounds(dataset_range)
        symbology_start = str(bounds["dataset_start"])
        end = str(bounds["latest_complete_end_exclusive"])
        observed.update(
            {
                "provider_dataset_start_date": symbology_start,
                "provider_schema_start_dates": bounds["schema_starts"],
                "latest_complete_end_exclusive": end,
            }
        )

        symbology: dict[str, dict[str, dict[str, object]]] = {}
        for market in CURRENT_ACQUISITION_MARKETS:
            parent_symbol = f"{market}.FUT"
            discovery_raw = budget.call(
                "resolve",
                apis.resolve,
                context={
                    "market": market,
                    "stage": "discovery",
                    "stype_in": "parent",
                    "symbol": parent_symbol,
                },
                dataset=DATASET,
                symbols=[parent_symbol],
                stype_in="parent",
                stype_out="instrument_id",
                start_date=symbology_start,
                end_date=end,
            )
            discovery = _symbology_summary(
                discovery_raw,
                symbol=parent_symbol,
                stype_in="parent",
                query_start=symbology_start,
                end=end,
                allow_single_symbol_partial=True,
            )
            effective = str(discovery["first_effective_date"])
            symbology[market] = {"discovery_parent": discovery}
            for stype_in, symbol in (
                ("parent", parent_symbol),
                ("continuous", f"{market}.v.0"),
            ):
                raw = budget.call(
                    "resolve",
                    apis.resolve,
                    context={
                        "market": market,
                        "stage": "post_effective_verification",
                        "stype_in": stype_in,
                        "symbol": symbol,
                    },
                    dataset=DATASET,
                    symbols=[symbol],
                    stype_in=stype_in,
                    stype_out="instrument_id",
                    start_date=effective,
                    end_date=end,
                )
                symbology[market][stype_in] = _symbology_summary(
                    raw,
                    symbol=symbol,
                    stype_in=stype_in,
                    query_start=effective,
                    end=end,
                )
        observed["symbology_summaries"] = symbology
        unresolved = sorted(
            market
            for market in CURRENT_ACQUISITION_MARKETS
            if symbology[market]["discovery_parent"]["effective_date_disposition"]
            != "PROVIDER_PRELAUNCH_PARTIAL_FIRST_MAPPING_DATE"
        )
        if unresolved:
            observed["unresolved_product_effective_date_markets"] = unresolved
            raise UnauthorizedOperation(
                "exact product effective date is unresolved before provider dataset start"
            )

        annual_by_market = {
            market: annual_market_year_intervals(
                start=max(
                    "2018-01-01",
                    str(symbology[market]["discovery_parent"]["first_effective_date"]),
                ),
                end_exclusive=end,
            )
            for market in CURRENT_ACQUISITION_MARKETS
        }
        annual_request_count = sum(
            len(annual_by_market[str(request["market"])]) for request in plan["requests"]
        )
        if not (1 <= annual_request_count <= MAXIMUM_ANNUAL_REQUESTS):
            raise UnauthorizedOperation("annual market/schema request ceiling exceeded")

        estimates: list[dict[str, object]] = []
        total_estimated = 0
        destinations: list[str] = []
        conflicts: list[str] = []
        for request in plan["requests"]:
            market = str(request["market"])
            effective = str(
                symbology[market]["discovery_parent"]["first_effective_date"]
            )
            for annual in annual_by_market[market]:
                query = {
                    **_provider_query(request, end=str(annual["end_exclusive"])),
                    "start": annual["start"],
                }
                cost = budget.call(
                    "get_cost",
                    apis.get_cost,
                    context={
                        "market": market,
                        "schema": str(request["schema"]),
                        "year": str(annual["year"]),
                    },
                    **query,
                )
                _decimal_zero(cost)
                size = budget.call(
                    "get_billable_size",
                    apis.get_billable_size,
                    context={
                        "market": market,
                        "schema": str(request["schema"]),
                        "year": str(annual["year"]),
                    },
                    **query,
                )
                if type(size) is not int or size < 0:
                    raise IntegrityError("provider billable-size response is invalid")
                total_estimated += size
                paths = phase1a_paths(
                    market=market,
                    schema=str(request["schema"]),
                    year=int(annual["year"]),
                    interval=str(annual["interval"]),
                )
                for destination in paths.values():
                    destinations.append(destination)
                    if (root / destination).exists():
                        conflicts.append(destination)
                estimate_core: dict[str, object] = {
                    "request_definition_id": request["request_id"],
                    "market": market,
                    "schema": request["schema"],
                    "year": annual["year"],
                    "estimated_bytes": size,
                    "estimated_cost_usd": "0",
                    "product_effective_date": effective,
                    "acquisition_start": annual["start"],
                    "end_exclusive": annual["end_exclusive"],
                    "partial_launch_year": annual["partial_launch_year"],
                    "partial_latest_year": annual["partial_latest_year"],
                    "dbn_destination": paths["dbn"],
                    "sidecar_destination": paths["sidecar"],
                }
                estimates.append(
                    {**estimate_core, "acquisition_request_id": sha256_json(estimate_core)}
                )
        expected_successful_calls = 15 + (2 * annual_request_count)
        if sum(budget.counts.values()) != expected_successful_calls:
            raise IntegrityError("successful metadata preflight call count drifted")
        if len(set(destinations)) != 2 * annual_request_count:
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
            market: symbology[market]["discovery_parent"]["first_effective_date"]
            for market in CURRENT_ACQUISITION_MARKETS
        }
        core = {
            **base,
            **observed,
            "state": "PASS_METADATA_ONLY",
            "provider_call_counts": dict(sorted(budget.counts.items())),
            "provider_call_total": sum(budget.counts.values()),
            "dataset_entitlement": "PASS",
            "standard_plan_schema_entitlement": "PASS_ZERO_COST_EACH_REQUEST",
            "product_effective_dates": product_dates,
            "request_estimates": estimates,
            "annual_market_schema_request_count": annual_request_count,
            "maximum_annual_market_schema_requests": MAXIMUM_ANNUAL_REQUESTS,
            "total_estimated_bytes": total_estimated,
            "total_acquisition_byte_ceiling": byte_ceiling,
            "fixed_maximum_total_acquisition_bytes": MAXIMUM_TOTAL_ACQUISITION_BYTES,
            "disk_free_bytes_observed": free,
            "disk_required_free_bytes": required_free,
            "destination_conflict_count": 0,
            "request_definition_count": 20,
            "file_partition": (
                "ONE_DBN_AND_ADJACENT_SIDECAR_PER_MARKET_SCHEMA_CALENDAR_YEAR"
            ),
        }
    except Exception as exc:
        exception_type = type(exc).__name__
        text = str(exc)
        validation_field = next(
            (
                field
                for field in (
                    "partial",
                    "not_found",
                    "stype_in",
                    "stype_out",
                    "symbols",
                    "start_date",
                    "end_date",
                    "status",
                    "message",
                    "result",
                    "interval",
                )
                if field in text
            ),
            None,
        )
        failure_code = (
            "PRODUCT_EFFECTIVE_DATE_UNRESOLVED_PRE_DATASET"
            if "product effective date is unresolved" in text
            else "PROVIDER_HTTP_CLIENT_ERROR"
            if exception_type == "BentoClientError"
            else "RUNTIME_CEILING"
            if "runtime ceiling" in text
            else "PROVIDER_TIMEOUT"
            if "timeout" in exception_type.lower()
            else "UNEXPECTED_NONZERO_COST"
            if "nonzero cost" in text
            else "INSUFFICIENT_DISK"
            if "disk capacity" in text
            else "DESTINATION_CONFLICT"
            if "destination already exists" in text
            else "PARTIAL_OR_NOT_FOUND_SYMBOLOGY"
            if "nonempty partial" in text or "nonempty not_found" in text
            else "SYMBOL_STATUS_SHAPE_DRIFT"
            if "exact string list" in text
            else "SYMBOL_SUCCESS_MESSAGE_DRIFT"
            if "success message echo" in text
            else "SYMBOL_ECHO_FIELD_DRIFT"
            if "echo drifted" in text
            else "PRELAUNCH_PARTIAL_DISPOSITION_DRIFT"
            if "prelaunch partial disposition" in text
            else "POST_EFFECTIVE_COVERAGE_DRIFT"
            if "post-effective coverage start" in text
            else "METADATA_PREFLIGHT_FAIL_CLOSED"
        )
        core = {
            **base,
            **observed,
            "state": "FAIL_CLOSED_METADATA_ONLY",
            "failure_code": failure_code,
            "provider_call_counts": dict(sorted(budget.counts.items())),
            "provider_call_total": sum(budget.counts.values()),
            "failed_provider_operation": budget.last_name,
            "failed_provider_call_ordinal": sum(budget.counts.values()),
            "failed_request_context": budget.last_context,
            "failed_validation_field": validation_field,
            "provider_http_status": (
                getattr(exc, "http_status")
                if type(getattr(exc, "http_status", None)) is int
                else None
            ),
            "provider_error_message_recorded": False,
            "exception_type": exception_type,
        }
    return _write_report_create_only(report_path, core)
