"""Dataset-range-safe Apex micro metadata preflight successor.

The executed v5 plan, consumed authorization, and fail-closed report remain
immutable.  V6 corrects the rejected ``2000-01-01`` symbology search by using
the provider-confirmed dataset start.  If that start truncates a product's
actual effective date, the run fails explicitly instead of inventing a date.
No download-capable API is exposed to this executor.
"""

from __future__ import annotations

import json
import shutil
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
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
from .micro_alpha_databento_preflight_v5 import (
    PLAN_PATH as PREDECESSOR_PLAN_PATH,
    REPORT_PATH as PREDECESSOR_REPORT_PATH,
    build_file_metadata_provider_apis,
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
    "configs/apex_micro_tier01_databento_metadata_preflight_v6.json"
)
REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_metadata_preflight_v6/report.json"
)
PREDECESSOR_PLAN_ID: Final = (
    "b1031c54f50f603a832eda086b8df70cb241f6e51476586413ebf468b52f2df0"
)
PREDECESSOR_PLAN_SHA256: Final = (
    "5c8d027c900bbb0bb553c0e9a556b46bb51ec4158259f12d68e592f671af72a5"
)
PREDECESSOR_REPORT_ID: Final = (
    "eff60da8e541691d3275dc438343c24dffc210fe21c27aa7863cf85826dcfb31"
)
PREDECESSOR_REPORT_SHA256: Final = (
    "d9c71c93f11924f023d67d0fb0d49e04a28e06025a3deae5a6aea3b72d0f8799"
)
PREDECESSOR_AUTHORIZATION_RECEIPT_ID: Final = (
    "28f1b90cdaa59d9089841bf1df95ab113a5c00dcf2131f3e832bb66d0762e596"
)
PREDECESSOR_AUTHORIZATION_PATH: Final = Path("state/authorization_uses") / (
    f"{PREDECESSOR_AUTHORIZATION_RECEIPT_ID}.json"
)
PREDECESSOR_AUTHORIZATION_SHA256: Final = (
    "4081e343cec6f1aa87009e75601730f53fdec882ae8f0ee592adafe17eefe516"
)
SCHEMA: Final = "apex_micro_databento_metadata_preflight/6.0.0"
REPORT_SCHEMA: Final = "apex_micro_databento_metadata_preflight_report/6.0.0"
MAXIMUM_ANNUAL_REQUESTS: Final = 180
MAXIMUM_PROVIDER_CALLS: Final = 11 + (2 * MAXIMUM_ANNUAL_REQUESTS)
MAXIMUM_RUNTIME_SECONDS: Final = 300
PER_CALL_TIMEOUT_SECONDS: Final = 30
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


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{description} is not an object")
    return value


def load_predecessor_failure(*, root: Path) -> dict[str, object]:
    """Verify the exact v5 failure and its one-use authorization evidence."""

    plan_path = root / PREDECESSOR_PLAN_PATH
    report_path = root / PREDECESSOR_REPORT_PATH
    authorization_path = root / PREDECESSOR_AUTHORIZATION_PATH
    if (
        sha256_file(plan_path) != PREDECESSOR_PLAN_SHA256
        or sha256_file(report_path) != PREDECESSOR_REPORT_SHA256
        or sha256_file(authorization_path) != PREDECESSOR_AUTHORIZATION_SHA256
    ):
        raise IntegrityError("v5 metadata preflight evidence was not preserved byte-for-byte")
    plan = _object(plan_path, "v5 metadata preflight plan")
    plan_core = dict(plan)
    plan_id = plan_core.pop("plan_id", None)
    report = _object(report_path, "v5 metadata preflight report")
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
        or report.get("failure_code") != "METADATA_PREFLIGHT_FAIL_CLOSED"
        or report.get("exception_type") != "BentoClientError"
        or report.get("provider_call_counts")
        != {
            "get_dataset_range": 1,
            "list_datasets": 1,
            "list_schemas": 1,
            "resolve": 1,
        }
        or report.get("provider_call_total") != 4
        or report.get("maximum_external_cost_usd") != "0"
        or report.get("external_cost_incurred_usd") != "0"
        or report.get("automatic_retries") != 0
        or report.get("timeseries_download_calls") != 0
        or report.get("historical_rows_read") is not False
        or report.get("dbn_files_created") != 0
        or report.get("credential_content_recorded") is not False
    ):
        raise IntegrityError("v5 metadata preflight failure evidence drifted")
    return report


def build_plan(*, root: Path) -> dict[str, object]:
    failure = load_predecessor_failure(root=root)
    predecessor_plan = _object(
        root / PREDECESSOR_PLAN_PATH, "v5 metadata preflight plan"
    )
    implementation_paths = (
        "configs/dependency_lock_receipt.json",
        "src/futures_rebuild/boundary.py",
        "src/futures_rebuild/canonical.py",
        "src/futures_rebuild/errors.py",
        "src/futures_rebuild/live_cockpit/databento_auth.py",
        "src/futures_rebuild/micro_alpha_databento_preflight.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v5.py",
        "src/futures_rebuild/micro_alpha_pipeline.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v6.py",
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
        raise IntegrityError("v6 market/schema request definitions drifted from v5")
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
                "basis": "DETERMINISTIC_EXECUTOR_ORDER_AND_SEALED_CALL_COUNTS",
                "call_ordinal": 4,
                "operation": "resolve",
                "market": "MES",
                "symbol": "MES.FUT",
                "stype_in": "parent",
                "rejected_start_date": "2000-01-01",
                "provider_message_recorded": False,
            },
            "external_cost_incurred_usd": "0",
            "automatic_retries": 0,
            "preservation": "IMMUTABLE_NO_OVERWRITE_DELETE_OR_RELABEL",
        },
        "correction": {
            "reason": "SYMBOLOGY_START_MUST_RESPECT_PROVIDER_DATASET_RANGE",
            "predecessor_symbology_start": "2000-01-01",
            "successor_symbology_start_rule": "PROVIDER_CONFIRMED_DATASET_START_DATE",
            "pre_dataset_product_effective_date_disposition": (
                "FAIL_CLOSED_EXACT_PRODUCT_EFFECTIVE_DATE_UNRESOLVED"
            ),
            "provider_error_recording": (
                "HTTP_STATUS_AND_PRICE_FREE_CALL_CONTEXT_ONLY_NO_MESSAGE_OR_CREDENTIAL"
            ),
            "file_partition": (
                "ONE_DBN_AND_ADJACENT_SIDECAR_PER_MARKET_SCHEMA_CALENDAR_YEAR"
            ),
            "scope_change": "QUERY_RANGE_CORRECTION_ONLY_NO_MARKET_SCHEMA_OR_DATA_ENDPOINT",
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
        raise IntegrityError("Apex micro metadata preflight v6 drifted")
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
    return validate_plan(_object(root / PLAN_PATH, "v6 metadata preflight plan"), root=root)


def _required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
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


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return _required_scope(root=root, plan=plan)


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


def _aware_range(value: object, *, description: str) -> tuple[date, date]:
    if not isinstance(value, Mapping) or set(value) != {"start", "end"}:
        raise IntegrityError(f"provider {description} range returned unexpected fields")
    parsed: dict[str, datetime] = {}
    for key in ("start", "end"):
        raw = value.get(key)
        if type(raw) is not str:
            raise IntegrityError(f"provider {description} range is invalid")
        try:
            stamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise IntegrityError(f"provider {description} range is invalid") from exc
        if stamp.tzinfo is None:
            raise IntegrityError(f"provider {description} range must be timezone-aware")
        parsed[key] = stamp.astimezone(timezone.utc)
    if parsed["start"] >= parsed["end"]:
        raise IntegrityError(f"provider {description} range is not positive")
    return parsed["start"].date(), parsed["end"].date()


def _dataset_bounds(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != {"start", "end", "schema"}:
        raise IntegrityError("provider dataset range returned unexpected fields")
    dataset_start, dataset_end = _aware_range(
        {"start": value.get("start"), "end": value.get("end")},
        description="dataset",
    )
    schema_ranges = value.get("schema")
    if not isinstance(schema_ranges, Mapping) or not set(SCHEMAS).issubset(schema_ranges):
        raise IntegrityError("provider dataset range lacks a required schema range")
    starts: dict[str, str] = {}
    ends = [dataset_end]
    for schema in SCHEMAS:
        schema_start, schema_end = _aware_range(
            schema_ranges[schema], description=f"schema {schema}"
        )
        starts[schema] = schema_start.isoformat()
        ends.append(schema_end)
    latest_complete = min(ends)
    if latest_complete <= date(2018, 1, 1):
        raise IntegrityError("provider dataset range does not cover the acquisition start")
    return {
        "dataset_start": dataset_start.isoformat(),
        "schema_starts": dict(sorted(starts.items())),
        "latest_complete_end_exclusive": latest_complete.isoformat(),
    }


def _symbology_summary(
    value: object,
    *,
    symbol: str,
    stype_in: str,
    query_start: str,
    end: str,
) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != _SYMBOLOGY_KEYS:
        raise IntegrityError("provider symbology returned unexpected fields")
    if (
        value.get("stype_in") != stype_in
        or value.get("stype_out") != "instrument_id"
        or value.get("symbols") != [symbol]
        or value.get("start_date") != query_start
        or value.get("end_date") != end
        or value.get("partial") not in {False, 0}
        or value.get("not_found") not in (None, (), [])
        or value.get("message") != ""
        or value.get("status") != 0
    ):
        raise IntegrityError("provider symbology response drifted")
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
    disposition = (
        "ACTIVE_AT_PROVIDER_DATASET_START_EXACT_PRODUCT_EFFECTIVE_DATE_UNRESOLVED"
        if first == query_start
        else "PROVIDER_MAPPING_FIRST_EFFECTIVE_DATE"
    )
    return {
        "first_effective_date": first,
        "effective_date_disposition": disposition,
        "query_start_date": query_start,
        "mapping_interval_count": len(entries),
        "mapping_sha256": sha256_json(value),
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
    """Consume one v6 authorization and run metadata-only operations."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_plan(root=root)
    report_path = root / REPORT_PATH
    boundary.assert_active_path(
        report_path.absolute(),
        purpose="Apex micro v6 metadata report",
        subtree="state/unpublished_evidence/apex_micro_metadata_preflight_v6",
    )
    if report_path.exists():
        raise IntegrityError("Apex micro v6 metadata report is create-only")
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
    observed: dict[str, object] = {}
    try:
        apis = provider_factory()
        datasets = budget.call("list_datasets", apis.list_datasets)
        if not isinstance(datasets, list) or not all(type(item) is str for item in datasets):
            raise IntegrityError("provider dataset entitlement response is invalid")
        if DATASET not in datasets:
            raise UnauthorizedOperation("GLBX.MDP3 is not entitled")
        schemas = budget.call(
            "list_schemas", apis.list_schemas, dataset=DATASET
        )
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
            symbology[market] = {}
            for stype_in, symbol in (
                ("parent", f"{market}.FUT"),
                ("continuous", f"{market}.v.0"),
            ):
                raw = budget.call(
                    "resolve",
                    apis.resolve,
                    context={
                        "market": market,
                        "stype_in": stype_in,
                        "symbol": symbol,
                    },
                    dataset=DATASET,
                    symbols=[symbol],
                    stype_in=stype_in,
                    stype_out="instrument_id",
                    start_date=symbology_start,
                    end_date=end,
                )
                symbology[market][stype_in] = _symbology_summary(
                    raw,
                    symbol=symbol,
                    stype_in=stype_in,
                    query_start=symbology_start,
                    end=end,
                )
            if (
                symbology[market]["parent"]["first_effective_date"]
                != symbology[market]["continuous"]["first_effective_date"]
            ):
                raise IntegrityError(
                    "parent and continuous product effective dates disagree"
                )
        observed["symbology_summaries"] = symbology
        unresolved = sorted(
            market
            for market in CURRENT_ACQUISITION_MARKETS
            if symbology[market]["parent"]["effective_date_disposition"]
            != "PROVIDER_MAPPING_FIRST_EFFECTIVE_DATE"
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
                    str(symbology[market]["parent"]["first_effective_date"]),
                ),
                end_exclusive=end,
            )
            for market in CURRENT_ACQUISITION_MARKETS
        }
        annual_request_count = sum(
            len(annual_by_market[str(request["market"])])
            for request in plan["requests"]
        )
        if not (1 <= annual_request_count <= MAXIMUM_ANNUAL_REQUESTS):
            raise UnauthorizedOperation("annual market/schema request ceiling exceeded")

        estimates: list[dict[str, object]] = []
        total_estimated = 0
        destinations: list[str] = []
        conflicts: list[str] = []
        for request in plan["requests"]:
            market = str(request["market"])
            effective = str(symbology[market]["parent"]["first_effective_date"])
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
        expected_successful_calls = 11 + (2 * annual_request_count)
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
            market: symbology[market]["parent"]["first_effective_date"]
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
            "provider_http_status": (
                getattr(exc, "http_status")
                if type(getattr(exc, "http_status", None)) is int
                else None
            ),
            "provider_error_message_recorded": False,
            "exception_type": exception_type,
        }
    return _write_report_create_only(report_path, core)
