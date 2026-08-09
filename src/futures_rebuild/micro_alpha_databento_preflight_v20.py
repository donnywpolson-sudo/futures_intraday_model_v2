"""Official-launch-date-safe cumulative Apex micro metadata preflight.

V19 authenticated the Databento Standard entitlement, required schemas,
dataset range, parent-family mappings, and continuous-roll coverage before it
failed closed because a provider mapping boundary was not an exchange launch
date.  V20 preserves that immutable evidence, binds separate CME Group launch
date reports, and performs only the missing annual cost and billable-size
queries.  It exposes no download, DBN, decode, or row-reading surface.
"""

from __future__ import annotations

import copy
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
from .micro_alpha_databento_preflight_v8 import _object
from .micro_alpha_databento_preflight_v19 import (
    PLAN_PATH as PREDECESSOR_PLAN_PATH,
    REPORT_PATH as PREDECESSOR_REPORT_PATH,
    load_plan as load_v19_plan,
)
from .micro_alpha_pipeline import (
    CURRENT_ACQUISITION_MARKETS,
    DATASET,
    SCHEMAS,
    annual_market_year_intervals,
    phase1a_paths,
    validate_phase1a_request,
)
from .micro_alpha_product_effective_dates import (
    M6E_REPORT_PATH,
    REMAINING_REPORT_PATH,
    load_official_product_effective_dates,
)
from .runtime_environment import require_locked_repository_environment


PLAN_PATH: Final = Path(
    "configs/apex_micro_tier01_databento_metadata_preflight_v20.json"
)
REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_metadata_preflight_v20/report.json"
)
PREDECESSOR_PLAN_ID: Final = (
    "b8da9346200f457c47cff75330b01c64615e9b1f62c0272ab3a00409098788c8"
)
PREDECESSOR_PLAN_SHA256: Final = (
    "f024d33293b864350d002df5281a5c13da9ce4e1e7557e02e7c1d0b32604c75f"
)
PREDECESSOR_REPORT_ID: Final = (
    "2c08f95147d2b0f75cb0d357c182ab11ab597f326e3de7d8f087a587570cee98"
)
PREDECESSOR_REPORT_SHA256: Final = (
    "49adb236d5b61e6a259f8f1191d74c153fffe87880eeacc7ea97ea1a4f60fbfe"
)
PREDECESSOR_AUTHORIZATION_RECEIPT_ID: Final = (
    "2e3e8240d3ae52c4582a6b24b7f302fde45f8fd43389da2fb79b832b49898568"
)
PREDECESSOR_AUTHORIZATION_PATH: Final = Path("state/authorization_uses") / (
    f"{PREDECESSOR_AUTHORIZATION_RECEIPT_ID}.json"
)
PREDECESSOR_AUTHORIZATION_SHA256: Final = (
    "06e6ce4e3c84a34a87480a3a0920ec6ebf32af6a43a9919f20c449ca20247e5e"
)
M6E_REPORT_ID: Final = (
    "c061f4ff78fd6bc408ae237b69ab0e6898c0d3b5a2419955ab4f27278b32b54c"
)
M6E_REPORT_SHA256: Final = (
    "3df830da2f8977e3510ad8ba1ab5d31270ca5d646e20623d3d40634d64c89de1"
)
REMAINING_REPORT_ID: Final = (
    "f1e17dcf1703b2e1b5525d350f51f11508875a8b68c4350182ee2ebb48befbb3"
)
REMAINING_REPORT_SHA256: Final = (
    "3f691be6a07ea141505060f26b65523c1562267e25919cfc4442c2db8c2dc2e2"
)
SCHEMA: Final = "apex_micro_databento_metadata_preflight/20.0.0"
REPORT_SCHEMA: Final = "apex_micro_databento_metadata_preflight_report/20.0.0"
MAXIMUM_ANNUAL_REQUESTS: Final = 160
MAXIMUM_PROVIDER_CALLS: Final = 2 * MAXIMUM_ANNUAL_REQUESTS
MAXIMUM_RUNTIME_SECONDS: Final = 300
PER_CALL_TIMEOUT_SECONDS: Final = 30
PHASE1A_START: Final = "2018-01-01"


def _verified_self_hash(value: Mapping[str, object], *, name: str) -> None:
    core = dict(value)
    identity = core.pop("report_id", None)
    if type(identity) is not str or identity != sha256_json(core):
        raise IntegrityError(f"{name} identity drifted")


def load_predecessor_metadata(*, root: Path) -> dict[str, object]:
    """Validate the immutable v19 metadata result used cumulatively by v20."""

    plan = load_v19_plan(root=root)
    report_path = root / PREDECESSOR_REPORT_PATH
    authorization_path = root / PREDECESSOR_AUTHORIZATION_PATH
    if (
        plan.get("plan_id") != PREDECESSOR_PLAN_ID
        or sha256_file(root / PREDECESSOR_PLAN_PATH) != PREDECESSOR_PLAN_SHA256
        or sha256_file(report_path) != PREDECESSOR_REPORT_SHA256
        or sha256_file(authorization_path) != PREDECESSOR_AUTHORIZATION_SHA256
    ):
        raise IntegrityError("v19 metadata evidence was not preserved byte-for-byte")
    report = _object(report_path, "v19 metadata preflight report")
    _verified_self_hash(report, name="v19 metadata preflight report")
    expected_counts = {
        "get_dataset_range": 1,
        "list_datasets": 1,
        "list_schemas": 1,
        "resolve": 12,
    }
    if (
        report.get("report_id") != PREDECESSOR_REPORT_ID
        or report.get("state") != "FAIL_CLOSED_METADATA_ONLY"
        or report.get("failure_code")
        != "PRODUCT_EFFECTIVE_DATE_UNRESOLVED_PRE_DATASET"
        or report.get("provider_call_counts") != expected_counts
        or report.get("provider_call_total") != 15
        or report.get("failed_provider_operation") != "resolve"
        or report.get("failed_provider_call_ordinal") != 15
        or report.get("external_cost_incurred_usd") != "0"
        or report.get("automatic_retries") != 0
        or report.get("timeseries_download_calls") != 0
        or report.get("historical_rows_read") is not False
        or report.get("dbn_files_created") != 0
        or report.get("credential_content_recorded") is not False
        or report.get("authorization_receipt_id")
        != PREDECESSOR_AUTHORIZATION_RECEIPT_ID
        or report.get("provider_dataset_start_date") != "2010-06-06"
        or report.get("latest_complete_end_exclusive") != "2026-08-09"
        or report.get("unresolved_product_effective_date_markets") != ["M6E"]
    ):
        raise IntegrityError("v19 cumulative metadata classification drifted")
    schema_starts = report.get("provider_schema_start_dates")
    if not isinstance(schema_starts, Mapping) or set(schema_starts) != set(SCHEMAS):
        raise IntegrityError("v19 required schema range evidence drifted")
    symbology = report.get("symbology_summaries")
    if not isinstance(symbology, Mapping) or set(symbology) != set(
        CURRENT_ACQUISITION_MARKETS
    ):
        raise IntegrityError("v19 symbology market evidence drifted")
    for market in CURRENT_ACQUISITION_MARKETS:
        summaries = symbology.get(market)
        if not isinstance(summaries, Mapping) or set(summaries) != {
            "discovery_parent",
            "parent",
            "continuous",
        }:
            raise IntegrityError(f"v19 {market} symbology evidence drifted")
        parent = summaries.get("parent")
        continuous = summaries.get("continuous")
        discovery = summaries.get("discovery_parent")
        if (
            not isinstance(parent, Mapping)
            or not isinstance(continuous, Mapping)
            or not isinstance(discovery, Mapping)
            or parent.get("post_effective_boundary_coverage") is not True
            or parent.get("roll_continuity_claimed") is not False
            or continuous.get("post_effective_gap_free_coverage") is not True
            or continuous.get("roll_continuity_claimed") is not True
            or discovery.get("raw_interval_values_recorded") is not False
            or parent.get("raw_interval_values_recorded") is not False
            or continuous.get("raw_interval_values_recorded") is not False
        ):
            raise IntegrityError(f"v19 {market} coverage evidence drifted")
    return report


def _prelaunch_dispositions(*, effective: str) -> list[dict[str, object]]:
    if effective <= PHASE1A_START:
        return []
    records: list[dict[str, object]] = []
    cursor = date.fromisoformat(PHASE1A_START)
    stop = date.fromisoformat(effective)
    while cursor < stop:
        year_end = date(cursor.year + 1, 1, 1)
        end = min(year_end, stop)
        records.append(
            {
                "year": cursor.year,
                "start": cursor.isoformat(),
                "end_exclusive": end.isoformat(),
                "disposition": "PRODUCT_PRELAUNCH_NO_DBN_FABRICATED",
            }
        )
        cursor = end
    return records


def _annual_scope(
    *, product_dates: Mapping[str, str], end_exclusive: str
) -> tuple[dict[str, list[dict[str, object]]], int]:
    annual = {
        market: annual_market_year_intervals(
            start=max(PHASE1A_START, product_dates[market]),
            end_exclusive=end_exclusive,
        )
        for market in CURRENT_ACQUISITION_MARKETS
    }
    count = len(SCHEMAS) * sum(len(intervals) for intervals in annual.values())
    if count != MAXIMUM_ANNUAL_REQUESTS:
        raise IntegrityError("official-date annual request count drifted")
    return annual, count


def _validate_acquisition_start_coverage(
    *, predecessor: Mapping[str, object], product_dates: Mapping[str, str]
) -> None:
    """Require sealed provider coverage to envelop every acquisition start."""

    symbology = predecessor.get("symbology_summaries")
    if not isinstance(symbology, Mapping):
        raise IntegrityError("v19 symbology coverage evidence is absent")
    for market in CURRENT_ACQUISITION_MARKETS:
        acquisition_start = max(PHASE1A_START, product_dates[market])
        summaries = symbology.get(market)
        if not isinstance(summaries, Mapping):
            raise IntegrityError(f"v19 {market} coverage evidence is absent")
        for role in ("parent", "continuous"):
            summary = summaries.get(role)
            query_start = (
                summary.get("query_start_date")
                if isinstance(summary, Mapping)
                else None
            )
            if type(query_start) is not str or query_start > acquisition_start:
                raise IntegrityError(
                    f"v19 {market} {role} coverage does not envelop acquisition start"
                )


def build_plan(*, root: Path) -> dict[str, object]:
    predecessor_report = load_predecessor_metadata(root=root)
    product_dates = load_official_product_effective_dates(root=root)
    if (
        sha256_file(root / M6E_REPORT_PATH) != M6E_REPORT_SHA256
        or sha256_file(root / REMAINING_REPORT_PATH) != REMAINING_REPORT_SHA256
    ):
        raise IntegrityError("official CME product-date report bytes drifted")
    predecessor = load_v19_plan(root=root)
    end_exclusive = str(predecessor_report["latest_complete_end_exclusive"])
    _validate_acquisition_start_coverage(
        predecessor=predecessor_report, product_dates=product_dates
    )
    annual, annual_count = _annual_scope(
        product_dates=product_dates, end_exclusive=end_exclusive
    )
    core = copy.deepcopy(predecessor)
    core.pop("plan_id", None)
    core.update(
        {
            "schema_version": SCHEMA,
            "classification": "PREPARED_METADATA_ONLY_PROVIDER_APPROVAL_REQUIRED",
            "state": "PREPARED_NOT_EXECUTED",
            "lane_id": "apex_integer_micro_20",
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
                "failure_code": "PRODUCT_EFFECTIVE_DATE_UNRESOLVED_PRE_DATASET",
                "provider_call_total": 15,
                "cumulatively_reused_passed_operations": [
                    "dataset_entitlement",
                    "required_schema_entitlement",
                    "dataset_range",
                    "parent_family_symbology",
                    "continuous_roll_symbology",
                ],
                "preservation": "IMMUTABLE_NO_OVERWRITE_DELETE_OR_RELABEL",
            },
            "official_product_effective_date_sources": {
                "M6E": {
                    "path": M6E_REPORT_PATH.as_posix(),
                    "report_id": M6E_REPORT_ID,
                    "sha256": M6E_REPORT_SHA256,
                },
                "MES_MCL_MGC": {
                    "path": REMAINING_REPORT_PATH.as_posix(),
                    "report_id": REMAINING_REPORT_ID,
                    "sha256": REMAINING_REPORT_SHA256,
                },
                "product_effective_dates": product_dates,
                "semantic_rule": (
                    "CME_LAUNCH_DATE_CONTROLS_PRELAUNCH_DATABENTO_MAPPING_ONLY_"
                    "PROVES_AVAILABILITY_AND_CONTINUITY"
                ),
            },
            "annual_scope": {
                "latest_complete_end_exclusive": end_exclusive,
                "market_interval_counts": {
                    market: len(intervals) for market, intervals in annual.items()
                },
                "exact_market_schema_requests": annual_count,
                "prelaunch_dispositions": {
                    market: _prelaunch_dispositions(effective=product_dates[market])
                    for market in CURRENT_ACQUISITION_MARKETS
                },
            },
            "provider_operations": {
                "list_datasets": 0,
                "list_schemas": 0,
                "get_dataset_range": 0,
                "resolve": 0,
                "get_cost": annual_count,
                "get_billable_size": annual_count,
                "timeseries_download": 0,
            },
            "correction": {
                "reason": "DATABENTO_MAPPING_BOUNDARY_IS_NOT_EXCHANGE_LAUNCH_DATE",
                "exchange_launch_date_source": "SEALED_CME_GROUP_PRIMARY_EVIDENCE",
                "databento_mapping_role": "AVAILABILITY_AND_CONTINUITY_ONLY",
                "passed_v19_metadata_requeried": False,
                "remaining_operations": "ANNUAL_COST_AND_BILLABLE_SIZE_ONLY",
                "scope_change": "NONE_SAME_MARKETS_SCHEMAS_AND_ANNUAL_LAYOUT",
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
            M6E_REPORT_PATH.as_posix(): M6E_REPORT_SHA256,
            REMAINING_REPORT_PATH.as_posix(): REMAINING_REPORT_SHA256,
            "src/futures_rebuild/micro_alpha_product_effective_dates.py": (
                sha256_file(
                    root / "src/futures_rebuild/micro_alpha_product_effective_dates.py"
                )
            ),
            "src/futures_rebuild/micro_alpha_databento_preflight_v19.py": (
                sha256_file(
                    root / "src/futures_rebuild/micro_alpha_databento_preflight_v19.py"
                )
            ),
            "src/futures_rebuild/micro_alpha_databento_preflight_v20.py": (
                sha256_file(
                    root / "src/futures_rebuild/micro_alpha_databento_preflight_v20.py"
                )
            ),
        }
    )
    core["plan_bindings"] = bindings
    limits = dict(core["limits"])
    limits.update(
        {
            "exact_provider_call_ceiling": MAXIMUM_PROVIDER_CALLS,
            "maximum_annual_market_schema_requests": MAXIMUM_ANNUAL_REQUESTS,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "per_call_timeout_seconds": PER_CALL_TIMEOUT_SECONDS,
            "maximum_phase1a_dbn_files": MAXIMUM_ANNUAL_REQUESTS,
            "maximum_phase1a_sidecars": MAXIMUM_ANNUAL_REQUESTS,
        }
    )
    core["limits"] = limits
    checks = dict(core["checks"])
    checks.update(
        {
            "official_cme_product_effective_dates": True,
            "databento_mapping_not_used_as_exchange_launch_date": True,
            "sealed_v19_entitlement_range_and_symbology_reused": True,
            "sealed_v19_coverage_envelops_each_official_date_acquisition_start": True,
            "only_missing_cost_and_billable_size_operations_reachable": True,
            "explicit_prelaunch_no_dbn_dispositions": True,
        }
    )
    core["checks"] = checks
    return {**core, "plan_id": sha256_json(core)}


def validate_plan(plan: Mapping[str, object], *, root: Path) -> dict[str, object]:
    expected = build_plan(root=root)
    if dict(plan) != expected:
        raise IntegrityError("Apex micro metadata preflight v20 plan drifted")
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
    return validate_plan(_object(root / PLAN_PATH, "v20 metadata preflight plan"), root=root)


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "plan_id": str(plan["plan_id"]),
        "predecessor_report_id": PREDECESSOR_REPORT_ID,
        "official_m6e_report_id": M6E_REPORT_ID,
        "official_remaining_report_id": REMAINING_REPORT_ID,
        "request_definitions": "20",
        "exact_annual_market_schema_requests": str(MAXIMUM_ANNUAL_REQUESTS),
        "markets": ",".join(CURRENT_ACQUISITION_MARKETS),
        "schemas": ",".join(SCHEMAS),
        "provider": "Databento",
        "dataset": DATASET,
        "provider_operations": "get_cost,get_billable_size",
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
        context: Mapping[str, str],
        **kwargs: object,
    ) -> object:
        remaining = MAXIMUM_RUNTIME_SECONDS - (self.clock() - self.started)
        if sum(self.counts.values()) >= MAXIMUM_PROVIDER_CALLS:
            raise UnauthorizedOperation("metadata preflight provider call ceiling reached")
        if remaining <= 0:
            raise UnauthorizedOperation("metadata preflight runtime ceiling reached")
        owner = getattr(function, "__self__", None)
        if owner is not None and hasattr(owner, "TIMEOUT"):
            setattr(owner, "TIMEOUT", min(float(PER_CALL_TIMEOUT_SECONDS), remaining))
        self.last_name = name
        self.last_context = dict(sorted(context.items()))
        self.counts[name] = self.counts.get(name, 0) + 1
        value = function(**kwargs)
        if self.clock() - self.started > MAXIMUM_RUNTIME_SECONDS:
            raise UnauthorizedOperation("metadata preflight runtime ceiling reached")
        return value


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
    """Consume one v20 authorization and run 320 cost/size calls only."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_plan(root=root)
    report_path = root / REPORT_PATH
    boundary.assert_active_path(
        report_path.absolute(),
        purpose="Apex micro v20 metadata report",
        subtree="state/unpublished_evidence/apex_micro_metadata_preflight_v20",
    )
    if report_path.exists():
        raise IntegrityError("Apex micro v20 metadata report is create-only")
    if credential_source != CREDENTIAL_SOURCE:
        raise UnauthorizedOperation("metadata preflight credential source is not bound")
    environment_check(root)
    predecessor = load_predecessor_metadata(root=root)
    product_dates = load_official_product_effective_dates(root=root)
    end_exclusive = str(predecessor["latest_complete_end_exclusive"])
    _validate_acquisition_start_coverage(
        predecessor=predecessor, product_dates=product_dates
    )
    annual_by_market, annual_request_count = _annual_scope(
        product_dates=product_dates, end_exclusive=end_exclusive
    )
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
        "official_m6e_report_id": M6E_REPORT_ID,
        "official_m6e_report_sha256": M6E_REPORT_SHA256,
        "official_remaining_report_id": REMAINING_REPORT_ID,
        "official_remaining_report_sha256": REMAINING_REPORT_SHA256,
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
        "cumulative_metadata": {
            "dataset_entitlement": "PASS_FROM_SEALED_V19",
            "required_schema_entitlement": "PASS_FROM_SEALED_V19",
            "dataset_range": "PASS_FROM_SEALED_V19",
            "symbology_availability_and_continuity": "PASS_FROM_SEALED_V19",
            "provider_dataset_start_date": predecessor["provider_dataset_start_date"],
            "provider_schema_start_dates": predecessor["provider_schema_start_dates"],
            "latest_complete_end_exclusive": end_exclusive,
        },
        "product_effective_dates": product_dates,
        "prelaunch_dispositions": {
            market: _prelaunch_dispositions(effective=product_dates[market])
            for market in CURRENT_ACQUISITION_MARKETS
        },
    }
    try:
        apis = provider_factory()
        estimates: list[dict[str, object]] = []
        total_estimated = 0
        destinations: list[str] = []
        conflicts: list[str] = []
        for request in plan["requests"]:
            market = str(request["market"])
            for annual in annual_by_market[market]:
                query = {
                    **_provider_query(request, end=str(annual["end_exclusive"])),
                    "start": annual["start"],
                }
                context = {
                    "market": market,
                    "schema": str(request["schema"]),
                    "year": str(annual["year"]),
                }
                cost = budget.call(
                    "get_cost", apis.get_cost, context=context, **query
                )
                _decimal_zero(cost)
                size = budget.call(
                    "get_billable_size",
                    apis.get_billable_size,
                    context=context,
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
                    "product_effective_date": product_dates[market],
                    "acquisition_start": annual["start"],
                    "end_exclusive": annual["end_exclusive"],
                    "partial_launch_year": annual["partial_launch_year"],
                    "partial_latest_year": annual["partial_latest_year"],
                    "dbn_destination": paths["dbn"],
                    "sidecar_destination": paths["sidecar"],
                }
                estimates.append(
                    {
                        **estimate_core,
                        "acquisition_request_id": sha256_json(estimate_core),
                    }
                )
        if sum(budget.counts.values()) != MAXIMUM_PROVIDER_CALLS:
            raise IntegrityError("successful metadata preflight call count drifted")
        if len(estimates) != annual_request_count:
            raise IntegrityError("annual estimate count drifted")
        if len(set(destinations)) != 2 * annual_request_count:
            raise IntegrityError("metadata preflight destinations collide internally")
        byte_ceiling = total_estimated + max(total_estimated // 10, 1024**2)
        if byte_ceiling > MAXIMUM_TOTAL_ACQUISITION_BYTES:
            raise UnauthorizedOperation(
                "estimated acquisition exceeds the fixed byte ceiling"
            )
        usage = disk_usage(root)
        free = getattr(usage, "free", None)
        if type(free) is not int or free < 0:
            raise IntegrityError("disk-capacity response is invalid")
        required_free = byte_ceiling + DISK_SAFETY_BYTES
        if free < required_free:
            raise UnauthorizedOperation(
                "insufficient disk capacity for bounded acquisition"
            )
        if conflicts:
            raise UnauthorizedOperation("an exact Phase 1A destination already exists")
        core = {
            **base,
            "state": "PASS_METADATA_ONLY",
            "provider_call_counts": dict(sorted(budget.counts.items())),
            "provider_call_total": sum(budget.counts.values()),
            "request_estimates": estimates,
            "annual_market_schema_request_count": annual_request_count,
            "maximum_annual_market_schema_requests": MAXIMUM_ANNUAL_REQUESTS,
            "total_estimated_bytes": total_estimated,
            "total_acquisition_byte_ceiling": byte_ceiling,
            "fixed_maximum_total_acquisition_bytes": (
                MAXIMUM_TOTAL_ACQUISITION_BYTES
            ),
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
        message = str(exc)
        failure_code = (
            "UNEXPECTED_NONZERO_COST"
            if "nonzero cost" in message
            else "BILLABLE_SIZE_RESPONSE_DRIFT"
            if "billable-size" in message
            else "INSUFFICIENT_DISK"
            if "disk capacity" in message
            else "DESTINATION_CONFLICT"
            if "destination already exists" in message
            else "RUNTIME_CEILING"
            if "runtime ceiling" in message
            else "PROVIDER_TIMEOUT"
            if "timeout" in exception_type.lower()
            else "METADATA_PREFLIGHT_FAIL_CLOSED"
        )
        core = {
            **base,
            "state": "FAIL_CLOSED_METADATA_ONLY",
            "failure_code": failure_code,
            "provider_call_counts": dict(sorted(budget.counts.items())),
            "provider_call_total": sum(budget.counts.values()),
            "failed_provider_operation": budget.last_name,
            "failed_provider_call_ordinal": sum(budget.counts.values()),
            "failed_request_context": budget.last_context,
            "provider_error_message_recorded": False,
            "exception_type": exception_type,
        }
    return _write_report_create_only(report_path, core)


__all__ = [
    "MAXIMUM_ANNUAL_REQUESTS",
    "MAXIMUM_PROVIDER_CALLS",
    "PLAN_PATH",
    "REPORT_PATH",
    "build_file_metadata_provider_apis",
    "build_plan",
    "execute_preflight",
    "load_plan",
    "load_predecessor_metadata",
    "required_scope",
    "validate_plan",
]
