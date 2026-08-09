"""Bounded interval-overlap-safe Apex micro metadata preflight successor.

The executed v15 plan, consumed authorization, and fail-closed report remain
immutable. V15 proved that the MES parent mapping satisfied its gap-free gate,
then rejected the MES continuous mapping at its strict interval-bound check.
V16 keeps exact request echoes and every result-shape, identity, cost, disk,
and destination gate. A returned mapping interval must overlap the exact query
range; only then is it clipped to that range for the gap-free coverage proof.
Wholly out-of-range, malformed, duplicate, unrelated, or gapped mappings still
fail closed. No mapping values, DBN rows, or download-capable API are exposed.
"""

from __future__ import annotations

import copy
import shutil
import time
from collections.abc import Callable, Mapping
from datetime import date
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
    OPERATION,
    _decimal_zero,
    _provider_query,
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
from .micro_alpha_databento_preflight_v11 import (
    MAXIMUM_OPAQUE_PARTIAL_ENTRIES,
    MAXIMUM_PROVIDER_CALLS,
    _CallBudget,
    _SYMBOLOGY_KEYS,
    _symbol_status,
)
from .micro_alpha_databento_preflight_v15 import (
    MAXIMUM_MESSAGE_STRING_LENGTH,
    MAXIMUM_RESULT_GROUP_KEY_LENGTH,
    MAXIMUM_RESULT_GROUPS,
    MAXIMUM_RESULT_INTERVALS,
    MAXIMUM_STATUS_INTEGER_MAGNITUDE,
    MAXIMUM_STATUS_STRING_LENGTH,
    PLAN_PATH as PREDECESSOR_PLAN_PATH,
    REPORT_PATH as PREDECESSOR_REPORT_PATH,
    _opaque_application_status,
    _opaque_success_message,
    load_plan as load_v15_plan,
)
from .micro_alpha_pipeline import (
    CURRENT_ACQUISITION_MARKETS,
    DATASET,
    SCHEMAS,
    annual_market_year_intervals,
    phase1a_paths,
    validate_phase1a_request,
)
from .runtime_environment import require_locked_repository_environment


PLAN_PATH: Final = Path(
    "configs/apex_micro_tier01_databento_metadata_preflight_v16.json"
)
REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_metadata_preflight_v16/report.json"
)
PREDECESSOR_PLAN_ID: Final = (
    "dea10c74f91db26efe19f27e1d21ff20f23899be60caca36fb0e383578b573c8"
)
PREDECESSOR_PLAN_SHA256: Final = (
    "89271c552e0c0cc8addead957cc596120fb8ac28027e4c7ad5c99977128fe9ec"
)
PREDECESSOR_REPORT_ID: Final = (
    "34bf7c0d5c5c6d0d5f3de61619887a276329ef8ede84069c4914550911438b33"
)
PREDECESSOR_REPORT_SHA256: Final = (
    "89c83e250826c17162525d963051772d0e84aa0c09e5496eab6e83f81740596d"
)
PREDECESSOR_AUTHORIZATION_RECEIPT_ID: Final = (
    "139de825bdaf165885238e8b37a0720a0bb0e772c428cd0b7751d0848431553f"
)
PREDECESSOR_AUTHORIZATION_PATH: Final = Path("state/authorization_uses") / (
    f"{PREDECESSOR_AUTHORIZATION_RECEIPT_ID}.json"
)
PREDECESSOR_AUTHORIZATION_SHA256: Final = (
    "d28d911fcaa8e20ae37387c1ebf612cefb632780f2dde585012d2dc39d09e19c"
)
SCHEMA: Final = "apex_micro_databento_metadata_preflight/16.0.0"
REPORT_SCHEMA: Final = "apex_micro_databento_metadata_preflight_report/16.0.0"


def load_predecessor_failure(*, root: Path) -> dict[str, object]:
    """Verify the exact v15 plan, failure report, and authorization evidence."""

    plan = load_v15_plan(root=root)
    report_path = root / PREDECESSOR_REPORT_PATH
    authorization_path = root / PREDECESSOR_AUTHORIZATION_PATH
    if (
        plan.get("plan_id") != PREDECESSOR_PLAN_ID
        or sha256_file(root / PREDECESSOR_PLAN_PATH) != PREDECESSOR_PLAN_SHA256
        or sha256_file(report_path) != PREDECESSOR_REPORT_SHA256
        or sha256_file(authorization_path) != PREDECESSOR_AUTHORIZATION_SHA256
    ):
        raise IntegrityError("v15 predecessor evidence drifted")
    report = _object(report_path, "v15 metadata preflight failure report")
    expected_context = {
        "market": "MES",
        "stage": "post_effective_verification",
        "stype_in": "continuous",
        "symbol": "MES.v.0",
    }
    if (
        report.get("report_id") != PREDECESSOR_REPORT_ID
        or report.get("state") != "FAIL_CLOSED_METADATA_ONLY"
        or report.get("failure_code") != "METADATA_PREFLIGHT_FAIL_CLOSED"
        or report.get("exception_type") != "IntegrityError"
        or report.get("failed_provider_call_ordinal") != 6
        or report.get("failed_provider_operation") != "resolve"
        or report.get("failed_request_context") != expected_context
        or report.get("failed_validation_field") != "interval"
        or report.get("provider_call_total") != 6
        or report.get("external_cost_incurred_usd") != "0"
        or report.get("automatic_retries") != 0
        or report.get("timeseries_download_calls") != 0
        or report.get("historical_rows_read") is not False
        or report.get("dbn_files_created") != 0
        or report.get("credential_content_recorded") is not False
        or report.get("authorization_receipt_id")
        != PREDECESSOR_AUTHORIZATION_RECEIPT_ID
    ):
        raise IntegrityError("v15 predecessor failure classification drifted")
    return report


def build_plan(*, root: Path) -> dict[str, object]:
    """Build the exact v16 successor without changing market/schema scope."""

    failure = load_predecessor_failure(root=root)
    predecessor = load_v15_plan(root=root)
    core = copy.deepcopy(predecessor)
    core.pop("plan_id", None)
    core.update(
        {
            "schema_version": SCHEMA,
            "classification": "PREPARED_METADATA_ONLY_PROVIDER_APPROVAL_REQUIRED",
            "state": "PREPARED_NOT_EXECUTED",
            "lane_id": "apex_integer_micro_16",
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
                "exception_type": "IntegrityError",
                "provider_call_total": 6,
                "failed_provider_operation": "resolve",
                "failed_provider_call_ordinal": 6,
                "failed_request_context": failure["failed_request_context"],
                "failed_validation_field": "interval",
                "automatic_retries": 0,
                "preservation": "IMMUTABLE_NO_OVERWRITE_DELETE_OR_RELABEL",
            },
            "correction": {
                **dict(predecessor["correction"]),
                "reason": "V15_CONTINUOUS_MAPPING_FAILED_STRICT_INTERVAL_BOUND_GATE",
                "interval_entry_fields": "EXACT_D0_D1_S_ONLY",
                "interval_raw_range": "VALID_ISO_POSITIVE_AND_OVERLAPS_EXACT_QUERY",
                "interval_coverage_range": "CLIPPED_TO_EXACT_QUERY_ONLY_AFTER_VALIDATION",
                "post_effective_interval_coverage": (
                    "GAP_FREE_UNION_OF_CLIPPED_OVERLAPPING_INTERVALS"
                ),
                "wholly_outside_query_interval": "FAIL_CLOSED",
                "duplicate_or_malformed_interval": "FAIL_CLOSED",
                "interval_values_recorded": False,
                "scope_change": "NONE_SAME_CALL_CEILING_MARKETS_SCHEMAS_AND_ENDPOINTS",
            },
            "output": {
                "path": REPORT_PATH.as_posix(),
                "create_only": True,
                "price_free": True,
            },
        }
    )
    bindings = dict(core["plan_bindings"])
    bindings.update(
        {
            PREDECESSOR_PLAN_PATH.as_posix(): PREDECESSOR_PLAN_SHA256,
            PREDECESSOR_REPORT_PATH.as_posix(): PREDECESSOR_REPORT_SHA256,
            PREDECESSOR_AUTHORIZATION_PATH.as_posix(): (
                PREDECESSOR_AUTHORIZATION_SHA256
            ),
            "src/futures_rebuild/micro_alpha_databento_preflight_v15.py": (
                sha256_file(
                    root / "src/futures_rebuild/micro_alpha_databento_preflight_v15.py"
                )
            ),
            "src/futures_rebuild/micro_alpha_databento_preflight_v16.py": (
                sha256_file(
                    root / "src/futures_rebuild/micro_alpha_databento_preflight_v16.py"
                )
            ),
        }
    )
    core["plan_bindings"] = bindings
    checks = dict(core["checks"])
    checks.update(
        {
            "exact_interval_entry_fields": True,
            "positive_iso_interval_range": True,
            "interval_overlap_with_exact_query": True,
            "coverage_only_clipping_to_exact_query": True,
            "post_effective_gap_free_clipped_interval_union": True,
            "interval_values_recording_forbidden": True,
            "field_specific_interval_failure_diagnostics": True,
        }
    )
    core["checks"] = checks
    return {**core, "plan_id": sha256_json(core)}


def validate_plan(plan: Mapping[str, object], *, root: Path) -> dict[str, object]:
    expected = build_plan(root=root)
    if dict(plan) != expected:
        raise IntegrityError("Apex micro metadata preflight v16 plan drifted")
    requests = plan.get("requests")
    if not isinstance(requests, list) or len(requests) != 20:
        raise IntegrityError("Apex micro preflight must contain exactly 20 requests")
    for request in requests:
        if not isinstance(request, Mapping):
            raise IntegrityError("Apex micro preflight request is malformed")
        request_core = dict(request)
        request_id = request_core.pop("request_id", None)
        if request_id != sha256_json(request_core):
            raise IntegrityError("Apex micro preflight request identity drifted")
        validate_phase1a_request(request_core)
    return dict(plan)


def load_plan(*, root: Path) -> dict[str, object]:
    return validate_plan(_object(root / PLAN_PATH, "v16 metadata preflight plan"), root=root)


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


def _symbology_summary(
    value: object,
    *,
    symbol: str,
    stype_in: str,
    query_start: str,
    end: str,
    allow_discovery_partial: bool = False,
    allow_bounded_partial: bool = False,
) -> dict[str, object]:
    """Validate mappings and clip only overlapping intervals for coverage."""

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
    partial_present = _symbol_status(
        value.get("partial"),
        name="partial",
        allow_discovery_partial=(allow_discovery_partial or allow_bounded_partial),
    )
    not_found_present = _symbol_status(
        value.get("not_found"),
        name="not_found",
        allow_discovery_partial=False,
    )
    status_shape = _opaque_application_status(value.get("status"))
    message_shape = _opaque_success_message(value.get("message"))
    result = value.get("result")
    if not isinstance(result, Mapping) or not result:
        raise IntegrityError("provider symbology result groups are absent")
    if len(result) > MAXIMUM_RESULT_GROUPS:
        raise IntegrityError("provider symbology result group ceiling exceeded")
    market_root = symbol.split(".", 1)[0]
    normalized_starts: list[str] = []
    coverage_intervals: list[tuple[str, str]] = []
    seen_intervals: set[tuple[str, str, str, str]] = set()
    requested_group_present = False
    left_clipped = 0
    right_clipped = 0
    for group_key, entries in result.items():
        if (
            type(group_key) is not str
            or not group_key
            or len(group_key) > MAXIMUM_RESULT_GROUP_KEY_LENGTH
        ):
            raise IntegrityError("provider symbology result group key is unbounded")
        if group_key != symbol and not group_key.startswith(market_root):
            raise IntegrityError("provider symbology result group root is unrelated")
        requested_group_present = requested_group_present or group_key == symbol
        if not isinstance(entries, list) or not entries:
            raise IntegrityError("provider symbology result group has no mappings")
        if len(seen_intervals) + len(entries) > MAXIMUM_RESULT_INTERVALS:
            raise IntegrityError("provider symbology result interval ceiling exceeded")
        for entry in entries:
            if not isinstance(entry, Mapping) or set(entry) != {"d0", "d1", "s"}:
                raise IntegrityError("provider symbology interval field shape drifted")
            identity = entry.get("s")
            if type(identity) is int:
                valid_identity = 0 < identity < 2**64
            elif type(identity) is str:
                valid_identity = identity.isdecimal() and 0 < int(identity) < 2**64
            else:
                valid_identity = False
            if not valid_identity:
                raise IntegrityError("provider symbology instrument identity is invalid")
            d0, d1 = entry.get("d0"), entry.get("d1")
            if type(d0) is not str or type(d1) is not str:
                raise IntegrityError("provider symbology interval date shape drifted")
            try:
                date.fromisoformat(d0)
                date.fromisoformat(d1)
            except ValueError as exc:
                raise IntegrityError("provider symbology interval date shape drifted") from exc
            if d0 >= d1:
                raise IntegrityError("provider symbology interval range is nonpositive")
            if d1 <= query_start or d0 >= end:
                raise IntegrityError("provider symbology interval is wholly outside query")
            identity_text = str(identity)
            interval_key = (group_key, d0, d1, identity_text)
            if interval_key in seen_intervals:
                raise IntegrityError("provider symbology interval is duplicated")
            seen_intervals.add(interval_key)
            clipped_start = max(d0, query_start)
            clipped_end = min(d1, end)
            left_clipped += int(d0 < query_start)
            right_clipped += int(d1 > end)
            normalized_starts.append(clipped_start)
            coverage_intervals.append((clipped_start, clipped_end))
    first = min(normalized_starts)
    if allow_discovery_partial and partial_present != (first > query_start):
        raise IntegrityError("provider symbology prelaunch partial disposition drifted")
    if not allow_discovery_partial and first != query_start:
        raise IntegrityError("provider symbology post-effective coverage start drifted")
    post_effective_gap_free = False
    if not allow_discovery_partial:
        cursor = query_start
        for interval_start, interval_end in sorted(coverage_intervals):
            if interval_start > cursor:
                raise IntegrityError("provider symbology post-effective coverage gap")
            if interval_end > cursor:
                cursor = interval_end
        if cursor != end:
            raise IntegrityError("provider symbology post-effective coverage gap")
        post_effective_gap_free = True
    disposition = (
        "ACTIVE_AT_PROVIDER_DATASET_START_EXACT_PRODUCT_EFFECTIVE_DATE_UNRESOLVED"
        if allow_discovery_partial and first == query_start
        else "PROVIDER_PRELAUNCH_PARTIAL_FIRST_MAPPING_DATE"
        if allow_discovery_partial
        else "POST_EFFECTIVE_FULL_COVERAGE_VERIFIED"
    )
    return {
        "first_effective_date": first,
        "effective_date_disposition": disposition,
        "query_start_date": query_start,
        "result_group_count": len(result),
        "requested_result_group_present": requested_group_present,
        "result_group_keys_recorded": False,
        "mapping_interval_count": len(seen_intervals),
        "instrument_identity_values_recorded": False,
        "mapping_sha256": sha256_json(value),
        "partial_present": partial_present,
        "partial_content_or_exact_count_recorded": False,
        "not_found_present": not_found_present,
        "post_effective_gap_free_coverage": post_effective_gap_free,
        "interval_boundary_policy": (
            "OVERLAP_REQUIRED_AND_CLIPPED_TO_EXACT_QUERY_FOR_COVERAGE_ONLY"
        ),
        "left_boundary_clipped_interval_count": left_clipped,
        "right_boundary_clipped_interval_count": right_clipped,
        "raw_interval_values_recorded": False,
        "application_status_shape": status_shape,
        "application_status_value_recorded": False,
        "success_message_shape": message_shape,
        "success_message_value_recorded": False,
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
    """Consume one v16 authorization and run metadata-only operations."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_plan(root=root)
    report_path = root / REPORT_PATH
    boundary.assert_active_path(
        report_path.absolute(),
        purpose="Apex micro v16 metadata report",
        subtree="state/unpublished_evidence/apex_micro_metadata_preflight_v16",
    )
    if report_path.exists():
        raise IntegrityError("Apex micro v16 metadata report is create-only")
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
                allow_discovery_partial=True,
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
                    allow_bounded_partial=True,
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
            else "RESULT_GROUP_ROOT_DRIFT"
            if "result group root" in text
            else "RESULT_GROUP_CEILING"
            if "result group ceiling" in text or "result interval ceiling" in text
            else "RESULT_GROUP_SHAPE_DRIFT"
            if "result group" in text or "instrument identity" in text
            else "INTERVAL_FIELD_SHAPE_DRIFT"
            if "interval field shape" in text
            else "INTERVAL_DATE_SHAPE_DRIFT"
            if "interval date shape" in text
            else "INTERVAL_NONPOSITIVE_RANGE"
            if "interval range is nonpositive" in text
            else "INTERVAL_OUTSIDE_QUERY"
            if "interval is wholly outside query" in text
            else "INTERVAL_DUPLICATE"
            if "interval is duplicated" in text
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
            else "OPAQUE_PARTIAL_ENTRY_CEILING"
            if "partial entry ceiling" in text
            else "PARTIAL_OR_NOT_FOUND_SYMBOLOGY"
            if "nonempty partial" in text or "nonempty not_found" in text
            else "OPAQUE_APPLICATION_STATUS_SHAPE"
            if "application status" in text
            else "SYMBOL_STATUS_SHAPE_DRIFT"
            if "exact string list" in text
            else "OPAQUE_SUCCESS_MESSAGE_SHAPE"
            if "success message" in text
            else "SYMBOL_ECHO_FIELD_DRIFT"
            if "echo drifted" in text
            else "PRELAUNCH_PARTIAL_DISPOSITION_DRIFT"
            if "prelaunch partial disposition" in text
            else "POST_EFFECTIVE_COVERAGE_DRIFT"
            if "post-effective coverage start" in text
            or "post-effective coverage gap" in text
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


__all__ = [
    "MAXIMUM_ANNUAL_REQUESTS",
    "MAXIMUM_OPAQUE_PARTIAL_ENTRIES",
    "MAXIMUM_PROVIDER_CALLS",
    "MAXIMUM_STATUS_INTEGER_MAGNITUDE",
    "MAXIMUM_STATUS_STRING_LENGTH",
    "MAXIMUM_MESSAGE_STRING_LENGTH",
    "PLAN_PATH",
    "PREDECESSOR_AUTHORIZATION_PATH",
    "PREDECESSOR_REPORT_ID",
    "PREDECESSOR_REPORT_PATH",
    "REPORT_PATH",
    "_symbology_summary",
    "build_file_metadata_provider_apis",
    "build_plan",
    "execute_preflight",
    "load_plan",
    "load_predecessor_failure",
    "required_scope",
    "validate_plan",
]
