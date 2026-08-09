"""Bounded parallel successor to the consumed v20 metadata preflight.

V20 preserved all prior entitlement, range, symbology, and official launch-date
evidence but timed out during the annual billable-size census. V21 keeps the
300-second total ceiling, raises the per-call bound for heavy annual 1-second
estimates, and uses six isolated metadata clients. It verifies zero cost once
over each complete market/schema acquisition range; that zero-cost superset
dominates its annual subsets, which the downloader must still requote exactly
before acquisition. Every annual interval retains its own byte estimate.
"""

from __future__ import annotations

import copy
import queue
import shutil
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
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
    OPERATION,
    _decimal_zero,
    _provider_query,
    _write_report_create_only,
)
from .micro_alpha_databento_preflight_v8 import _object
from .micro_alpha_databento_preflight_v20 import (
    M6E_REPORT_ID,
    M6E_REPORT_SHA256,
    PHASE1A_START,
    REMAINING_REPORT_ID,
    REMAINING_REPORT_SHA256,
    PLAN_PATH as PREDECESSOR_PLAN_PATH,
    REPORT_PATH as PREDECESSOR_REPORT_PATH,
    _annual_scope,
    _prelaunch_dispositions,
    _validate_acquisition_start_coverage,
    build_file_metadata_provider_apis,
    load_plan as load_v20_plan,
    load_predecessor_metadata,
)
from .micro_alpha_pipeline import (
    CURRENT_ACQUISITION_MARKETS,
    DATASET,
    SCHEMAS,
    phase1a_paths,
    validate_phase1a_request,
)
from .micro_alpha_product_effective_dates import load_official_product_effective_dates
from .runtime_environment import require_locked_repository_environment


PLAN_PATH: Final = Path(
    "configs/apex_micro_tier01_databento_metadata_preflight_v21.json"
)
REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_metadata_preflight_v21/report.json"
)
PREDECESSOR_PLAN_ID: Final = (
    "d9721cf0b2ff78ffddbfd24afd0867f8572a6b222e1ad8b21407a9885928b5f6"
)
PREDECESSOR_PLAN_SHA256: Final = (
    "7e0dcdcc85345f863b0d825d9278cd2d2df8c4163cd647f01b597c3460bc0211"
)
PREDECESSOR_REPORT_ID: Final = (
    "4f6513f9dc65590a542bc2c59ceaab5bb2a1e53fcf9ca2b15ee875113fe15478"
)
PREDECESSOR_REPORT_SHA256: Final = (
    "932373921efa17a8b1eb4653f8786dcc684cdcfbecaa51fc1be7379e5ba47981"
)
PREDECESSOR_AUTHORIZATION_RECEIPT_ID: Final = (
    "82f2a9b5794be8365c84d9c1f2fb1f5a8bcfb5a4a9e47b9198e458ee04dce509"
)
PREDECESSOR_AUTHORIZATION_PATH: Final = Path("state/authorization_uses") / (
    f"{PREDECESSOR_AUTHORIZATION_RECEIPT_ID}.json"
)
PREDECESSOR_AUTHORIZATION_SHA256: Final = (
    "2a545c88d0dfa28a1f977f54ccff8f2d5900f1945c45eadb41a90c2c36334c13"
)
SCHEMA: Final = "apex_micro_databento_metadata_preflight/21.0.0"
REPORT_SCHEMA: Final = "apex_micro_databento_metadata_preflight_report/21.0.0"
MAXIMUM_ANNUAL_REQUESTS: Final = 160
FULL_RANGE_COST_REQUESTS: Final = 20
MAXIMUM_PROVIDER_CALLS: Final = FULL_RANGE_COST_REQUESTS + MAXIMUM_ANNUAL_REQUESTS
MAXIMUM_RUNTIME_SECONDS: Final = 300
PER_CALL_TIMEOUT_SECONDS: Final = 90
MAXIMUM_PROVIDER_CLIENTS: Final = 6


def load_predecessor_failure(*, root: Path) -> dict[str, object]:
    """Verify the exact v20 plan, report, and consumed authorization."""

    plan = load_v20_plan(root=root)
    report_path = root / PREDECESSOR_REPORT_PATH
    authorization_path = root / PREDECESSOR_AUTHORIZATION_PATH
    if (
        plan.get("plan_id") != PREDECESSOR_PLAN_ID
        or sha256_file(root / PREDECESSOR_PLAN_PATH) != PREDECESSOR_PLAN_SHA256
        or sha256_file(report_path) != PREDECESSOR_REPORT_SHA256
        or sha256_file(authorization_path) != PREDECESSOR_AUTHORIZATION_SHA256
    ):
        raise IntegrityError("v20 metadata evidence was not preserved byte-for-byte")
    report = _object(report_path, "v20 metadata preflight report")
    core = dict(report)
    report_id = core.pop("report_id", None)
    expected_context = {"market": "MES", "schema": "ohlcv-1s", "year": "2020"}
    if (
        report_id != PREDECESSOR_REPORT_ID
        or report_id != sha256_json(core)
        or report.get("state") != "FAIL_CLOSED_METADATA_ONLY"
        or report.get("failure_code") != "PROVIDER_TIMEOUT"
        or report.get("exception_type") != "ReadTimeout"
        or report.get("failed_provider_operation") != "get_billable_size"
        or report.get("failed_provider_call_ordinal") != 68
        or report.get("failed_request_context") != expected_context
        or report.get("provider_call_counts")
        != {"get_billable_size": 34, "get_cost": 34}
        or report.get("provider_call_total") != 68
        or report.get("provider_error_message_recorded") is not False
        or report.get("external_cost_incurred_usd") != "0"
        or report.get("automatic_retries") != 0
        or report.get("timeseries_download_calls") != 0
        or report.get("historical_rows_read") is not False
        or report.get("dbn_files_created") != 0
        or report.get("credential_content_recorded") is not False
        or report.get("authorization_receipt_id")
        != PREDECESSOR_AUTHORIZATION_RECEIPT_ID
    ):
        raise IntegrityError("v20 timeout evidence classification drifted")
    return report


def build_plan(*, root: Path) -> dict[str, object]:
    failure = load_predecessor_failure(root=root)
    predecessor = load_v20_plan(root=root)
    inherited = load_predecessor_metadata(root=root)
    product_dates = load_official_product_effective_dates(root=root)
    end_exclusive = str(inherited["latest_complete_end_exclusive"])
    _validate_acquisition_start_coverage(
        predecessor=inherited, product_dates=product_dates
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
            "lane_id": "apex_integer_micro_21",
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
                "failure_code": "PROVIDER_TIMEOUT",
                "exception_type": "ReadTimeout",
                "provider_call_total": 68,
                "failed_provider_operation": "get_billable_size",
                "failed_provider_call_ordinal": 68,
                "failed_request_context": failure["failed_request_context"],
                "preservation": "IMMUTABLE_NO_OVERWRITE_DELETE_OR_RELABEL",
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
                "get_cost_full_acquisition_range": FULL_RANGE_COST_REQUESTS,
                "get_billable_size_annual": annual_count,
                "timeseries_download": 0,
            },
            "concurrency": {
                "maximum_isolated_metadata_clients": MAXIMUM_PROVIDER_CLIENTS,
                "shared_sdk_client_forbidden": True,
                "one_worker_per_client": True,
                "stop_scheduling_after_first_failure": True,
                "already_running_calls_may_finish_metadata_only": True,
                "automatic_retry": False,
            },
            "cost_dominance": {
                "full_range_request_count": FULL_RANGE_COST_REQUESTS,
                "full_range_zero_cost_required": True,
                "annual_subsets_share_exact_market_schema_symbol_and_symbology": True,
                "nonnegative_full_range_zero_cost_dominates_annual_subsets": True,
                "exact_annual_requote_required_immediately_before_download": True,
                "acquisition_cost_ceiling_usd": "0",
            },
            "correction": {
                "reason": "V20_ANNUAL_BILLABLE_SIZE_READ_TIMEOUT",
                "previous_per_call_timeout_seconds": 30,
                "per_call_timeout_seconds": PER_CALL_TIMEOUT_SECONDS,
                "bounded_isolated_metadata_clients": MAXIMUM_PROVIDER_CLIENTS,
                "annual_byte_estimates_preserved": annual_count,
                "annual_duplicate_cost_calls_replaced_by_full_range_dominance": True,
                "scope_change": "NONE_SAME_MARKETS_SCHEMAS_INTERVALS_AND_OUTPUT_CLASS",
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
            "src/futures_rebuild/micro_alpha_databento_preflight_v20.py": (
                sha256_file(
                    root / "src/futures_rebuild/micro_alpha_databento_preflight_v20.py"
                )
            ),
            "src/futures_rebuild/micro_alpha_databento_preflight_v21.py": (
                sha256_file(
                    root / "src/futures_rebuild/micro_alpha_databento_preflight_v21.py"
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
            "maximum_provider_clients": MAXIMUM_PROVIDER_CLIENTS,
            "maximum_phase1a_dbn_files": MAXIMUM_ANNUAL_REQUESTS,
            "maximum_phase1a_sidecars": MAXIMUM_ANNUAL_REQUESTS,
        }
    )
    core["limits"] = limits
    checks = dict(core["checks"])
    checks.update(
        {
            "full_range_zero_cost_dominates_annual_subsets": True,
            "exact_annual_download_requote_still_required": True,
            "one_billable_size_estimate_per_annual_request": True,
            "isolated_bounded_metadata_clients": True,
            "stop_scheduling_after_first_failure": True,
        }
    )
    core["checks"] = checks
    return {**core, "plan_id": sha256_json(core)}


def validate_plan(plan: Mapping[str, object], *, root: Path) -> dict[str, object]:
    expected = build_plan(root=root)
    if dict(plan) != expected:
        raise IntegrityError("Apex micro metadata preflight v21 plan drifted")
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
    return validate_plan(_object(root / PLAN_PATH, "v21 metadata preflight plan"), root=root)


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "plan_id": str(plan["plan_id"]),
        "predecessor_report_id": PREDECESSOR_REPORT_ID,
        "official_m6e_report_id": M6E_REPORT_ID,
        "official_remaining_report_id": REMAINING_REPORT_ID,
        "request_definitions": "20",
        "exact_annual_market_schema_requests": str(MAXIMUM_ANNUAL_REQUESTS),
        "full_range_cost_requests": str(FULL_RANGE_COST_REQUESTS),
        "annual_billable_size_requests": str(MAXIMUM_ANNUAL_REQUESTS),
        "markets": ",".join(CURRENT_ACQUISITION_MARKETS),
        "schemas": ",".join(SCHEMAS),
        "provider": "Databento",
        "dataset": DATASET,
        "provider_operations": "get_cost,get_billable_size",
        "provider_call_ceiling": str(MAXIMUM_PROVIDER_CALLS),
        "maximum_provider_clients": str(MAXIMUM_PROVIDER_CLIENTS),
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


@dataclass(frozen=True)
class _Task:
    index: int
    context: dict[str, str]
    query: dict[str, object]


@dataclass
class _ConcurrentCallBudget:
    clock: Callable[[], float]

    def __post_init__(self) -> None:
        self.started = self.clock()
        self.lock = threading.Lock()
        self.counts: dict[str, int] = {}
        self.ordinal = 0
        self.failure_name: str | None = None
        self.failure_context: dict[str, str] = {}
        self.failure_ordinal: int | None = None

    def call(
        self,
        *,
        name: str,
        function: Callable[..., object],
        context: Mapping[str, str],
        query: Mapping[str, object],
        validator: Callable[[object], object],
    ) -> object:
        with self.lock:
            remaining = MAXIMUM_RUNTIME_SECONDS - (self.clock() - self.started)
            if self.ordinal >= MAXIMUM_PROVIDER_CALLS:
                raise UnauthorizedOperation(
                    "metadata preflight provider call ceiling reached"
                )
            if remaining <= 0:
                raise UnauthorizedOperation("metadata preflight runtime ceiling reached")
            self.ordinal += 1
            ordinal = self.ordinal
            self.counts[name] = self.counts.get(name, 0) + 1
        owner = getattr(function, "__self__", None)
        if owner is not None and hasattr(owner, "TIMEOUT"):
            setattr(owner, "TIMEOUT", min(float(PER_CALL_TIMEOUT_SECONDS), remaining))
        try:
            value = validator(function(**query))
            if self.clock() - self.started > MAXIMUM_RUNTIME_SECONDS:
                raise UnauthorizedOperation("metadata preflight runtime ceiling reached")
            return value
        except Exception:
            with self.lock:
                if self.failure_ordinal is None:
                    self.failure_name = name
                    self.failure_context = dict(sorted(context.items()))
                    self.failure_ordinal = ordinal
            raise


def _zero_cost(value: object) -> str:
    _decimal_zero(value)
    return "0"


def _billable_size(value: object) -> int:
    if type(value) is not int or value < 0:
        raise IntegrityError("provider billable-size response is invalid")
    return value


def _parallel_calls(
    *,
    tasks: Sequence[_Task],
    clients: Sequence[MetadataProviderApis],
    operation: str,
    budget: _ConcurrentCallBudget,
    validator: Callable[[object], object],
) -> dict[int, object]:
    pending: queue.Queue[_Task] = queue.Queue()
    for task in tasks:
        pending.put(task)
    stop = threading.Event()
    results: dict[int, object] = {}
    results_lock = threading.Lock()
    failure_lock = threading.Lock()
    failures: list[Exception] = []

    def worker(client: MetadataProviderApis) -> None:
        function = getattr(client, operation)
        while not stop.is_set():
            try:
                task = pending.get_nowait()
            except queue.Empty:
                return
            if stop.is_set():
                return
            try:
                value = budget.call(
                    name=operation,
                    function=function,
                    context=task.context,
                    query=task.query,
                    validator=validator,
                )
            except Exception as exc:
                with failure_lock:
                    if not failures:
                        failures.append(exc)
                        stop.set()
                return
            with results_lock:
                results[task.index] = value

    with ThreadPoolExecutor(max_workers=len(clients)) as executor:
        futures = [executor.submit(worker, client) for client in clients]
        for future in futures:
            future.result()
    if failures:
        raise failures[0]
    if len(results) != len(tasks):
        raise IntegrityError("parallel metadata task completion count drifted")
    return results


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
    """Consume one v21 authorization and run bounded parallel metadata only."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_plan(root=root)
    report_path = root / REPORT_PATH
    boundary.assert_active_path(
        report_path.absolute(),
        purpose="Apex micro v21 metadata report",
        subtree="state/unpublished_evidence/apex_micro_metadata_preflight_v21",
    )
    if report_path.exists():
        raise IntegrityError("Apex micro v21 metadata report is create-only")
    if credential_source != CREDENTIAL_SOURCE:
        raise UnauthorizedOperation("metadata preflight credential source is not bound")
    environment_check(root)
    load_predecessor_failure(root=root)
    inherited = load_predecessor_metadata(root=root)
    product_dates = load_official_product_effective_dates(root=root)
    end_exclusive = str(inherited["latest_complete_end_exclusive"])
    _validate_acquisition_start_coverage(
        predecessor=inherited, product_dates=product_dates
    )
    annual_by_market, annual_count = _annual_scope(
        product_dates=product_dates, end_exclusive=end_exclusive
    )
    claim = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    budget = _ConcurrentCallBudget(clock=clock)
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
        "maximum_isolated_metadata_clients": MAXIMUM_PROVIDER_CLIENTS,
        "cumulative_metadata": {
            "dataset_entitlement": "PASS_FROM_SEALED_V19",
            "required_schema_entitlement": "PASS_FROM_SEALED_V19",
            "dataset_range": "PASS_FROM_SEALED_V19",
            "symbology_availability_and_continuity": "PASS_FROM_SEALED_V19",
            "provider_dataset_start_date": inherited["provider_dataset_start_date"],
            "provider_schema_start_dates": inherited["provider_schema_start_dates"],
            "latest_complete_end_exclusive": end_exclusive,
        },
        "product_effective_dates": product_dates,
        "prelaunch_dispositions": {
            market: _prelaunch_dispositions(effective=product_dates[market])
            for market in CURRENT_ACQUISITION_MARKETS
        },
    }
    try:
        clients = tuple(provider_factory() for _ in range(MAXIMUM_PROVIDER_CLIENTS))
        cost_tasks: list[_Task] = []
        size_tasks: list[_Task] = []
        size_records: dict[int, tuple[Mapping[str, object], dict[str, object]]] = {}
        size_index = 0
        for request_index, request in enumerate(plan["requests"]):
            market = str(request["market"])
            acquisition_start = max(PHASE1A_START, product_dates[market])
            full_query = {
                **_provider_query(request, end=end_exclusive),
                "start": acquisition_start,
            }
            cost_tasks.append(
                _Task(
                    index=request_index,
                    context={
                        "market": market,
                        "schema": str(request["schema"]),
                        "scope": "full_acquisition_range",
                    },
                    query=full_query,
                )
            )
            for annual in annual_by_market[market]:
                annual_query = {
                    **_provider_query(request, end=str(annual["end_exclusive"])),
                    "start": annual["start"],
                }
                size_tasks.append(
                    _Task(
                        index=size_index,
                        context={
                            "market": market,
                            "schema": str(request["schema"]),
                            "year": str(annual["year"]),
                        },
                        query=annual_query,
                    )
                )
                size_records[size_index] = (request, annual)
                size_index += 1
        if len(cost_tasks) != FULL_RANGE_COST_REQUESTS or len(size_tasks) != annual_count:
            raise IntegrityError("v21 metadata task construction drifted")
        cost_results = _parallel_calls(
            tasks=cost_tasks,
            clients=clients,
            operation="get_cost",
            budget=budget,
            validator=_zero_cost,
        )
        size_results = _parallel_calls(
            tasks=size_tasks,
            clients=clients,
            operation="get_billable_size",
            budget=budget,
            validator=_billable_size,
        )
        if budget.counts != {
            "get_cost": FULL_RANGE_COST_REQUESTS,
            "get_billable_size": annual_count,
        }:
            raise IntegrityError("successful metadata preflight call count drifted")
        cost_proofs = []
        for index, request in enumerate(plan["requests"]):
            market = str(request["market"])
            cost_proofs.append(
                {
                    "request_definition_id": request["request_id"],
                    "market": market,
                    "schema": request["schema"],
                    "acquisition_start": max(PHASE1A_START, product_dates[market]),
                    "end_exclusive": end_exclusive,
                    "estimated_cost_usd": cost_results[index],
                    "annual_subset_dominance": True,
                }
            )
        estimates: list[dict[str, object]] = []
        total_estimated = 0
        destinations: list[str] = []
        conflicts: list[str] = []
        for index in range(annual_count):
            request, annual = size_records[index]
            market = str(request["market"])
            size = int(size_results[index])
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
                "estimated_cost_usd": "0_FROM_FULL_RANGE_DOMINANCE",
                "exact_annual_requote_required_before_download": True,
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
        if len(set(destinations)) != 2 * annual_count:
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
            "full_range_zero_cost_proofs": cost_proofs,
            "request_estimates": estimates,
            "annual_market_schema_request_count": annual_count,
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
            "failed_provider_operation": budget.failure_name,
            "failed_provider_call_ordinal": budget.failure_ordinal,
            "failed_request_context": budget.failure_context,
            "provider_error_message_recorded": False,
            "exception_type": exception_type,
        }
    return _write_report_create_only(report_path, core)


__all__ = [
    "FULL_RANGE_COST_REQUESTS",
    "MAXIMUM_ANNUAL_REQUESTS",
    "MAXIMUM_PROVIDER_CALLS",
    "MAXIMUM_PROVIDER_CLIENTS",
    "PLAN_PATH",
    "REPORT_PATH",
    "build_file_metadata_provider_apis",
    "build_plan",
    "execute_preflight",
    "load_plan",
    "load_predecessor_failure",
    "required_scope",
    "validate_plan",
]
