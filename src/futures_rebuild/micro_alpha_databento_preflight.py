"""Single-use, metadata-only Databento preflight for Apex micro Tier 0/1.

The executor deliberately receives a capability containing metadata and
symbology methods only.  A timeseries or batch download method is never placed
on the capability and no code in this module opens or decodes DBN data.
"""

from __future__ import annotations

import json
import shutil
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .live_cockpit.databento_auth import resolve_databento_api_key
from .micro_alpha_pipeline import (
    CURRENT_ACQUISITION_MARKETS,
    DATASET,
    SCHEMAS,
    build_product_reference_requirements,
    phase1a_paths,
    validate_phase1a_request,
)
from .runtime_environment import require_locked_repository_environment


OBSOLETE_PLAN_PATH: Final = Path("configs/apex_micro_tier01_databento_preflight_plan.json")
OBSOLETE_PLAN_ID: Final = "c9bf6a86a9ca501cc4682ed10e63bf8cc984bfd27c3c44d35097e0aeeeba2ecc"
OBSOLETE_PLAN_SHA256: Final = "f09dfbf555b5b9178d248d60d5471cc38294dbcb9ea019c811513529f34595f4"
PLAN_PATH: Final = Path("configs/apex_micro_tier01_databento_metadata_preflight_v2.json")
REFERENCE_PATH: Final = Path("configs/apex_micro_product_reference_requirements.json")
SUPERSESSION_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_preparation_supersessions/"
    "micro_tier1_scope_reconciliation.json"
)
REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_metadata_preflight_v2/report.json"
)
SCHEMA: Final = "apex_micro_databento_metadata_preflight/2.0.0"
OPERATION: Final = "PREFLIGHT_APEX_MICRO_TIER01_DATABENTO_METADATA_ONCE"
CREDENTIAL_SOURCE: Final = "file api.env"
MAXIMUM_PROVIDER_CALLS: Final = 51
MAXIMUM_RUNTIME_SECONDS: Final = 240
PER_CALL_TIMEOUT_SECONDS: Final = 10
MAXIMUM_TOTAL_ACQUISITION_BYTES: Final = 128 * 1024**3
DISK_SAFETY_BYTES: Final = 1024**3
MAXIMUM_RETRIES: Final = 0


@dataclass(frozen=True)
class MetadataProviderApis:
    list_datasets: Callable[..., object]
    list_schemas: Callable[..., object]
    get_dataset_range: Callable[..., object]
    resolve: Callable[..., object]
    get_cost: Callable[..., object]
    get_billable_size: Callable[..., object]


def build_file_metadata_provider_apis(
    *, root: Path, historical_factory: Callable[..., object] | None = None,
) -> MetadataProviderApis:
    """Read only the bound file credential and expose no download capability."""

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


def _request(*, market: str, schema: str) -> dict[str, object]:
    stype = "parent" if schema == "definition" else "continuous"
    symbol = f"{market}.FUT" if schema == "definition" else f"{market}.v.0"
    core: dict[str, object] = {
        "dataset": DATASET,
        "market": market,
        "schema": schema,
        "stype_in": stype,
        "stype_out": "instrument_id",
        "symbols": [symbol],
        "start": "2018-01-01",
        "end_rule": "LATEST_COMPLETE_DAY_END_EXCLUSIVE_PROVIDER_CONFIRMED",
        "maximum_cost_usd": 0,
    }
    return {**core, "request_id": sha256_json(core)}


def load_obsolete_plan(*, root: Path) -> dict[str, object]:
    """Verify the preserved obsolete bytes while keeping execution unreachable."""

    path = root / OBSOLETE_PLAN_PATH
    if sha256_file(path) != OBSOLETE_PLAN_SHA256:
        raise IntegrityError("obsolete Apex micro preflight was not preserved byte-for-byte")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("obsolete Apex micro preflight is invalid") from exc
    if not isinstance(value, dict) or value.get("plan_id") != OBSOLETE_PLAN_ID:
        raise IntegrityError("obsolete Apex micro preflight identity drifted")
    return value


def build_plan(*, root: Path) -> dict[str, object]:
    load_obsolete_plan(root=root)
    implementation_paths = (
        "src/futures_rebuild/micro_alpha_pipeline.py",
        "src/futures_rebuild/micro_alpha_databento_preflight.py",
        "src/futures_rebuild/micro_alpha_acquisition.py",
        "src/futures_rebuild/alpha_research_architecture.py",
    )
    bindings = {path: sha256_file(root / path) for path in implementation_paths}
    references = build_product_reference_requirements()
    if json.loads((root / REFERENCE_PATH).read_text(encoding="utf-8")) != references:
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
        "obsolete_plan": {
            "plan_id": OBSOLETE_PLAN_ID,
            "sha256": OBSOLETE_PLAN_SHA256,
            "classification": "SUPERSEDED_PREPARATION — MICRO_TIER1_SCOPE_RECONCILIATION",
            "execution_forbidden": True,
        },
        "plan_bindings": {
            **bindings,
            REFERENCE_PATH.as_posix(): sha256_file(root / REFERENCE_PATH),
            SUPERSESSION_PATH.as_posix(): sha256_file(root / SUPERSESSION_PATH),
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
        "credential_source": {"path": "api.env", "binding": "PATH_ONLY_CONTENT_NEVER_REPORTED"},
        "output": {"path": REPORT_PATH.as_posix(), "create_only": True, "price_free": True},
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
    try:
        value = json.loads((root / PLAN_PATH).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("Apex micro metadata preflight plan is invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError("Apex micro metadata preflight plan is not an object")
    return validate_plan(value, root=root)


def _required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "plan_id": str(plan["plan_id"]),
        "request_definitions": "20",
        "markets": ",".join(CURRENT_ACQUISITION_MARKETS),
        "schemas": ",".join(SCHEMAS),
        "provider": "Databento",
        "dataset": DATASET,
        "provider_call_ceiling": str(MAXIMUM_PROVIDER_CALLS),
        "maximum_runtime_seconds": str(MAXIMUM_RUNTIME_SECONDS),
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
    """Expose the exact scope for the task agent that records user approval."""

    return _required_scope(root=root, plan=plan)


def _latest_complete_end(value: object) -> str:
    if not isinstance(value, Mapping) or set(value) != {"start", "end"}:
        raise IntegrityError("provider dataset range returned unexpected fields")
    end = value.get("end")
    if type(end) is not str or len(end) < 10:
        raise IntegrityError("provider dataset end is invalid")
    try:
        observed = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IntegrityError("provider dataset end is invalid") from exc
    if observed.tzinfo is None:
        raise IntegrityError("provider dataset end must be timezone-aware")
    candidate = observed.astimezone(timezone.utc).date()
    if candidate <= date(2018, 1, 1):
        raise IntegrityError("provider dataset end does not cover the acquisition start")
    return candidate.isoformat()


_SYMBOLOGY_KEYS = {
    "result", "symbols", "stype_in", "stype_out", "start_date", "end_date",
    "partial", "not_found", "message", "status",
}


def _symbology_summary(
    value: object, *, symbol: str, stype_in: str, end: str,
) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != _SYMBOLOGY_KEYS:
        raise IntegrityError("provider symbology returned unexpected fields")
    if (
        value.get("stype_in") != stype_in
        or value.get("stype_out") != "instrument_id"
        or value.get("start_date") != "2000-01-01"
        or value.get("end_date") != end
        or value.get("partial") not in {False, 0}
        or value.get("not_found") not in (None, (), [])
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
        if d0 >= d1:
            raise IntegrityError("provider symbology interval is not positive")
        dates.append(d0)
    return {
        "first_effective_date": min(dates),
        "mapping_interval_count": len(entries),
        "mapping_sha256": sha256_json(value),
    }


def _decimal_zero(value: object) -> None:
    if isinstance(value, bool):
        raise IntegrityError("provider cost response is invalid")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError("provider cost response is invalid") from exc
    if not amount.is_finite() or amount != 0:
        raise UnauthorizedOperation("provider returned an unexpected nonzero cost")


class _CallBudget:
    def __init__(self, *, clock: Callable[[], float]) -> None:
        self.clock = clock
        self.started = clock()
        self.counts: dict[str, int] = {}

    def call(self, name: str, function: Callable[..., object], **kwargs: object) -> object:
        if sum(self.counts.values()) >= MAXIMUM_PROVIDER_CALLS:
            raise UnauthorizedOperation("metadata preflight provider call ceiling reached")
        if self.clock() - self.started >= MAXIMUM_RUNTIME_SECONDS:
            raise UnauthorizedOperation("metadata preflight runtime ceiling reached")
        self.counts[name] = self.counts.get(name, 0) + 1
        return function(**kwargs)


def _provider_query(request: Mapping[str, object], *, end: str) -> dict[str, object]:
    return {
        "dataset": request["dataset"],
        "schema": request["schema"],
        "stype_in": request["stype_in"],
        "symbols": request["symbols"],
        "start": request["start"],
        "end": end,
    }


def _write_report_create_only(path: Path, core: Mapping[str, object]) -> dict[str, object]:
    report = {**core, "report_id": sha256_json(core)}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(report) + b"\n")
    return report


def execute_preflight(
    *, root: Path, authorization: OperationReceipt,
    provider_factory: Callable[[], MetadataProviderApis],
    credential_source: str, clock: Callable[[], float] = time.monotonic,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
) -> dict[str, object]:
    """Consume one authorization and run only the exact 51 metadata calls."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_plan(root=root)
    report_path = root / REPORT_PATH
    boundary.assert_active_path(
        report_path.absolute(), purpose="Apex micro metadata report",
        subtree="state/unpublished_evidence/apex_micro_metadata_preflight_v2",
    )
    if report_path.exists():
        raise IntegrityError("Apex micro metadata report is create-only")
    if credential_source != CREDENTIAL_SOURCE:
        raise UnauthorizedOperation("metadata preflight credential source is not bound")
    environment_check(root)
    claim = authorization.consume(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=_required_scope(root=root, plan=plan),
    )
    budget = _CallBudget(clock=clock)
    base: dict[str, object] = {
        "schema_version": "apex_micro_databento_metadata_preflight_report/2.0.0",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
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
            raise UnauthorizedOperation("a required Databento Standard historical schema is unavailable")
        dataset_range = budget.call("get_dataset_range", apis.get_dataset_range, dataset=DATASET)
        end = _latest_complete_end(dataset_range)

        symbology: dict[str, dict[str, dict[str, object]]] = {}
        for market in CURRENT_ACQUISITION_MARKETS:
            symbology[market] = {}
            for stype_in, symbol in (("parent", f"{market}.FUT"), ("continuous", f"{market}.v.0")):
                raw = budget.call(
                    "resolve", apis.resolve, dataset=DATASET, symbols=[symbol],
                    stype_in=stype_in, stype_out="instrument_id",
                    start_date="2000-01-01", end_date=end,
                )
                symbology[market][stype_in] = _symbology_summary(
                    raw, symbol=symbol, stype_in=stype_in, end=end,
                )
            parent_date = symbology[market]["parent"]["first_effective_date"]
            continuous_date = symbology[market]["continuous"]["first_effective_date"]
            if parent_date != continuous_date:
                raise IntegrityError("parent and continuous product effective dates disagree")

        estimates: list[dict[str, object]] = []
        total_estimated = 0
        destinations: list[str] = []
        conflicts: list[str] = []
        for request in plan["requests"]:
            query = _provider_query(request, end=end)
            _decimal_zero(budget.call("get_cost", apis.get_cost, **query))
            size = budget.call("get_billable_size", apis.get_billable_size, **query)
            if type(size) is not int or size < 0:
                raise IntegrityError("provider billable-size response is invalid")
            total_estimated += size
            market = str(request["market"])
            effective = str(symbology[market]["parent"]["first_effective_date"])
            query_start = max("2018-01-01", effective)
            interval = f"{query_start}_{end}"
            paths = phase1a_paths(
                market=market, schema=str(request["schema"]),
                year=int(query_start[:4]), interval=interval,
            )
            for destination in paths.values():
                destinations.append(destination)
                if (root / destination).exists():
                    conflicts.append(destination)
            estimates.append({
                "request_id": request["request_id"],
                "estimated_bytes": size,
                "estimated_cost_usd": "0",
                "product_effective_date": effective,
                "acquisition_start": query_start,
                "end_exclusive": end,
                "dbn_destination": paths["dbn"],
                "sidecar_destination": paths["sidecar"],
            })
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
        failure_code = (
            "UNEXPECTED_NONZERO_COST" if "nonzero cost" in str(exc)
            else "INSUFFICIENT_DISK" if "disk capacity" in str(exc)
            else "DESTINATION_CONFLICT" if "destination already exists" in str(exc)
            else "METADATA_PREFLIGHT_FAIL_CLOSED"
        )
        core = {
            **base,
            "state": "FAIL_CLOSED_METADATA_ONLY",
            "failure_code": failure_code,
            "provider_call_counts": dict(sorted(budget.counts.items())),
            "provider_call_total": sum(budget.counts.values()),
            "exception_type": type(exc).__name__,
        }
    return _write_report_create_only(report_path, core)


def execute_obsolete_preflight(*args: object, **kwargs: object) -> None:
    raise UnauthorizedOperation(
        "obsolete Apex micro preflight is SUPERSEDED_PREPARATION and cannot execute"
    )
