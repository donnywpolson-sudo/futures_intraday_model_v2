"""Receipt-gated staging acquisition for frozen BBO recovery windows.

The operation downloads the exact 30 already-quoted DBN requests into a
create-only staging subtree, verifies metadata and bytes, and publishes a
hash-bound acquisition receipt.  Staged bytes are not an active research
source and cannot alter the frozen protocol or trial lifecycle.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .live_cockpit.databento_auth import resolve_databento_api_key
from .runtime_environment import require_locked_repository_environment
from .tier1_frozen_quote_recovery_cost import (
    CREDENTIAL_SOURCE,
    DATASET,
    DIAGNOSTIC_RECORD_ID,
    DIAGNOSTIC_RECORD_SHA256,
    MAXIMUM_PROVIDER_CALLS,
    PROTOCOL_ID,
    PROTOCOL_PATH,
    PROTOCOL_SHA256,
    SCHEMA,
    STYPE_IN,
    STYPE_OUT,
    QuoteCostQuery,
    build_quote_cost_queries,
    load_diagnostic_record,
)


PLAN_PATH = Path("configs/tier1_frozen_quote_recovery_acquisition_plan.json")
COST_RECORD_PATH = Path(
    "state/provider_quotes/tier1_frozen_bbo_recovery_cost/"
    "6226b40be917d4d407b40c78569c1250394326ad0294aca285812772c1e3dc07.json"
)
COST_RECORD_ID = COST_RECORD_PATH.stem
COST_RECORD_SHA256 = "b4b01b70003a48c8c32b401b8286177ab93f1e9d340753df9bafe5b1792c2534"
OPERATION = "ACQUIRE_FROZEN_TIER1_BBO_1S_RECOVERY_STAGING_AND_PUBLISH"
STAGING_ROOT = Path("state/provider_acquisition_staging/tier1_frozen_bbo_recovery")
RECORD_ROOT = Path("state/provider_acquisitions/tier1_frozen_bbo_recovery")
EVENT_ROOT = Path("state/provider_acquisition_events/tier1_frozen_bbo_recovery")
MAXIMUM_HOST_RUNTIME_SECONDS = 600
MAXIMUM_EXTERNAL_COST_USD = Decimal("1.30")


@dataclass(frozen=True)
class FileCredentialProviderApis:
    get_cost: Callable[..., object]
    get_range: Callable[..., object]


def build_file_credential_provider_apis(
    *, root: Path, historical_factory: Callable[..., object] | None = None,
) -> FileCredentialProviderApis:
    """Build Databento APIs from only the repository api.env file."""

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
        raise IntegrityError("Databento historical client lacks required metadata or timeseries API")
    return FileCredentialProviderApis(get_cost=get_cost, get_range=get_range)


def build_file_credential_get_range(
    *, root: Path, historical_factory: Callable[..., object] | None = None,
) -> Callable[..., object]:
    """Compatibility wrapper returning the file-bound row reader."""

    return build_file_credential_provider_apis(
        root=root, historical_factory=historical_factory,
    ).get_range


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid BBO acquisition artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("BBO acquisition artifact is not an object")
    return value


def load_cost_record(*, root: Path) -> dict[str, object]:
    path = root / COST_RECORD_PATH
    if sha256_file(path) != COST_RECORD_SHA256:
        raise IntegrityError("BBO recovery cost record changed")
    record = _object(path)
    if (
        record.get("record_id") != COST_RECORD_ID
        or record.get("state") != "PUBLISHED_PROVIDER_QUOTE_ONLY"
        or record.get("query_count") != MAXIMUM_PROVIDER_CALLS
        or record.get("diagnostic_record_id") != DIAGNOSTIC_RECORD_ID
        or record.get("diagnostic_record_sha256") != DIAGNOSTIC_RECORD_SHA256
        or record.get("credential_source") != CREDENTIAL_SOURCE
        or record.get("metadata_cost_calls_only") is not True
        or record.get("historical_rows_read") is not False
        or record.get("market_rows_downloaded") is not False
        or record.get("external_cost_incurred_usd") != "0"
    ):
        raise IntegrityError("BBO recovery cost record is not the accepted quote")
    return record


def _iso_to_ns(value: str) -> int:
    if not isinstance(value, str) or not value.endswith("Z") or "." not in value:
        raise IntegrityError("BBO query timestamp is invalid")
    whole, fraction = value[:-1].split(".", 1)
    if len(fraction) != 9 or not fraction.isdigit():
        raise IntegrityError("BBO query timestamp precision changed")
    parsed = datetime.strptime(whole, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    seconds = (parsed - epoch).days * 86_400 + (parsed - epoch).seconds
    return seconds * 1_000_000_000 + int(fraction)


def validate_cost_record_queries(
    *, cost_record: Mapping[str, object], queries: Sequence[QuoteCostQuery],
) -> dict[str, Decimal]:
    estimates = cost_record.get("query_estimates")
    if not isinstance(estimates, list) or len(estimates) != len(queries):
        raise IntegrityError("BBO cost estimate ledger is incomplete")
    expected_ids = {item.query_id for item in queries}
    output: dict[str, Decimal] = {}
    for item in estimates:
        if not isinstance(item, Mapping) or item.get("provider_row_downloaded") is not False:
            raise IntegrityError("BBO cost estimate entry is invalid")
        query_id = str(item.get("query_id"))
        try:
            cost = Decimal(str(item.get("estimated_data_cost_usd")))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise IntegrityError("BBO cost estimate is invalid") from exc
        if query_id in output or query_id not in expected_ids or not cost.is_finite() or cost < 0:
            raise IntegrityError("BBO cost estimate identity or amount is invalid")
        output[query_id] = cost
    total = sum(output.values(), Decimal("0"))
    if (
        set(output) != expected_ids
        or format(total, "f") != cost_record.get("total_estimated_data_cost_usd")
        or total > MAXIMUM_EXTERNAL_COST_USD
    ):
        raise IntegrityError("BBO cost estimate exceeds or differs from the frozen quote")
    return output


def validate_fresh_cost_quote(
    *, queries: Sequence[QuoteCostQuery], get_cost: Callable[..., object],
    started_at: float | None = None, clock: Callable[[], float] = time.monotonic,
) -> tuple[dict[str, Decimal], tuple[dict[str, object], ...]]:
    """Requote every request and fail before download if the hard cap drifted."""

    if len(queries) != MAXIMUM_PROVIDER_CALLS:
        raise IntegrityError("fresh BBO cost call count changed")
    start = clock() if started_at is None else started_at
    raw_estimates: list[dict[str, object]] = []
    for query in queries:
        if clock() - start >= MAXIMUM_HOST_RUNTIME_SECONDS:
            raise IntegrityError("BBO acquisition exceeded its frozen host runtime")
        value = get_cost(**query.provider_kwargs())
        try:
            cost = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise IntegrityError("fresh BBO cost estimate is invalid") from exc
        if not cost.is_finite() or cost < 0:
            raise IntegrityError("fresh BBO cost estimate is invalid")
        raw_estimates.append({
            "query_id": query.query_id,
            "estimated_data_cost_usd": format(cost, "f"),
            "provider_row_downloaded": False,
        })
    estimates = tuple(raw_estimates)
    expected_ids = {item.query_id for item in queries}
    costs: dict[str, Decimal] = {}
    for item in estimates:
        query_id = str(item.get("query_id"))
        try:
            cost = Decimal(str(item.get("estimated_data_cost_usd")))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise IntegrityError("fresh BBO cost estimate is invalid") from exc
        if (
            query_id in costs or query_id not in expected_ids
            or not cost.is_finite() or cost < 0
            or item.get("provider_row_downloaded") is not False
        ):
            raise IntegrityError("fresh BBO cost estimate ledger is invalid")
        costs[query_id] = cost
    if set(costs) != expected_ids or sum(costs.values(), Decimal("0")) > MAXIMUM_EXTERNAL_COST_USD:
        raise UnauthorizedOperation("fresh BBO quote exceeds the authorized cost ceiling")
    return costs, estimates


def provider_request(query: QuoteCostQuery) -> dict[str, object]:
    return {**query.provider_kwargs(), "stype_out": STYPE_OUT}


def verify_store_metadata(*, store: object, query: QuoteCostQuery) -> None:
    metadata = getattr(store, "metadata", None)
    if (
        metadata is None
        or getattr(metadata, "dataset", None) != DATASET
        or str(getattr(metadata, "schema", "")) != SCHEMA
        or str(getattr(metadata, "stype_in", "")) != STYPE_IN
        or str(getattr(metadata, "stype_out", "")) != STYPE_OUT
        or getattr(metadata, "ts_out", None) is not False
        or getattr(metadata, "limit", None) is not None
        or getattr(metadata, "start", None) != _iso_to_ns(query.start)
        or getattr(metadata, "end", None) != _iso_to_ns(query.end)
        or sorted(str(item) for item in getattr(metadata, "symbols", ()))
        != sorted(query.symbols)
    ):
        raise IntegrityError("downloaded BBO metadata differs from its frozen query")


@dataclass(frozen=True)
class StagedBboFile:
    query_id: str
    relative_path: str
    sha256: str
    size: int
    sidecar_relative_path: str
    sidecar_sha256: str
    sidecar_size: int
    estimated_data_cost_usd: str

    def as_dict(self) -> dict[str, object]:
        return {
            "query_id": self.query_id, "relative_path": self.relative_path,
            "sha256": self.sha256, "size": self.size,
            "sidecar_relative_path": self.sidecar_relative_path,
            "sidecar_sha256": self.sidecar_sha256,
            "sidecar_size": self.sidecar_size,
            "estimated_data_cost_usd": self.estimated_data_cost_usd,
        }


def stage_query_store(
    *, root: Path, attempt_root: Path, query: QuoteCostQuery,
    store: object, estimated_cost: Decimal,
) -> StagedBboFile:
    verify_store_metadata(store=store, query=query)
    path = attempt_root / f"{query.query_id}.dbn.zst"
    RepoBoundary(root).assert_active_path(
        path.absolute(), purpose="BBO acquisition staging file",
        subtree=STAGING_ROOT.as_posix(),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    writer = getattr(store, "to_file", None)
    if not callable(writer):
        raise IntegrityError("provider BBO store cannot preserve DBN bytes")
    writer(path, mode="x", compression="zstd")
    if not path.is_file() or path.is_symlink() or path.stat().st_size <= 0:
        raise IntegrityError("staged BBO DBN file is absent or invalid")
    file_sha256 = sha256_file(path)
    file_size = path.stat().st_size
    sidecar = path.with_name(path.name + ".manifest.json")
    sidecar_core = {
        "schema_version": "tier1_frozen_bbo_recovery_staging_manifest/1.0.0",
        "state": "STAGED_NOT_ACTIVE_RESEARCH_DATA",
        "query": query.as_dict(),
        "query_id": query.query_id,
        "relative_path": path.relative_to(root).as_posix(),
        "sha256": file_sha256,
        "size": file_size,
        "estimated_data_cost_usd": format(estimated_cost, "f"),
        "cost_record_id": COST_RECORD_ID,
        "cost_record_sha256": COST_RECORD_SHA256,
        "diagnostic_record_id": DIAGNOSTIC_RECORD_ID,
        "diagnostic_record_sha256": DIAGNOSTIC_RECORD_SHA256,
        "protocol_id": PROTOCOL_ID,
        "protocol_sha256": PROTOCOL_SHA256,
        "successor_source_activated": False,
    }
    boundary = RepoBoundary(root)
    boundary.assert_active_path(
        sidecar.absolute(), purpose="BBO acquisition staging manifest",
        subtree=STAGING_ROOT.as_posix(),
    )
    if sidecar.exists():
        raise IntegrityError("staged BBO sidecar already exists")
    with sidecar.open("xb") as stream:
        stream.write(canonical_bytes({
            **sidecar_core, "manifest_id": sha256_json(sidecar_core),
        }) + b"\n")
    return StagedBboFile(
        query_id=query.query_id,
        relative_path=path.relative_to(root).as_posix(),
        sha256=file_sha256, size=file_size,
        sidecar_relative_path=sidecar.relative_to(root).as_posix(),
        sidecar_sha256=sha256_file(sidecar), sidecar_size=sidecar.stat().st_size,
        estimated_data_cost_usd=format(estimated_cost, "f"),
    )


def load_acquisition_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    cost = load_cost_record(root=root)
    queries = build_quote_cost_queries(
        diagnostic_record=load_diagnostic_record(root=root),
    )
    validate_cost_record_queries(cost_record=cost, queries=queries)
    forbidden = plan.get("forbidden_actions")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version") != "tier1_frozen_quote_recovery_acquisition_plan/1.0.0"
        or plan.get("state") != "PREPARED_REQUIRES_SEPARATE_PAID_ACQUISITION_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("cost_record_id") != COST_RECORD_ID
        or plan.get("cost_record_sha256") != COST_RECORD_SHA256
        or plan.get("protocol_id") != PROTOCOL_ID
        or plan.get("protocol_sha256") != PROTOCOL_SHA256
        or plan.get("credential_source") != CREDENTIAL_SOURCE
        or plan.get("query_count") != MAXIMUM_PROVIDER_CALLS
        or plan.get("query_set_id") != sha256_json([item.as_dict() for item in queries])
        or plan.get("maximum_external_cost_usd") != format(MAXIMUM_EXTERNAL_COST_USD, "f")
        or plan.get("fresh_metadata_cost_calls_before_download") != MAXIMUM_PROVIDER_CALLS
        or plan.get("fresh_quote_must_not_exceed_maximum_before_any_download") is not True
        or plan.get("maximum_host_runtime_seconds") != MAXIMUM_HOST_RUNTIME_SECONDS
        or plan.get("implementation_sha256") != sha256_file(Path(__file__))
        or sha256_file(root / PROTOCOL_PATH) != PROTOCOL_SHA256
        or not isinstance(forbidden, dict) or not forbidden
        or not all(value is True for value in forbidden.values())
    ):
        raise UnauthorizedOperation("BBO acquisition plan is absent or drifted")
    return plan


def _required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "cost_record_id": COST_RECORD_ID, "cost_record_sha256": COST_RECORD_SHA256,
        "query_count": str(MAXIMUM_PROVIDER_CALLS), "query_set_id": str(plan["query_set_id"]),
        "credential_source": CREDENTIAL_SOURCE,
        "provider": "Databento", "dataset": DATASET, "schema": SCHEMA,
        "market_row_download": "true", "staging_only": "true",
        "fresh_metadata_cost_calls_before_download": str(MAXIMUM_PROVIDER_CALLS),
        "maximum_external_cost_usd": format(MAXIMUM_EXTERNAL_COST_USD, "f"),
        "successor_source_activation": "false", "active_data_mutation": "false",
        "protocol_change": "false", "model_fit": "false",
        "prediction_generation": "false", "historical_evaluation": "false",
        "trial_registration_or_retirement": "false", "holdout_or_forward_access": "false",
        "staging_git_index": "false", "commit": "false", "push": "false", "trading": "false",
        "publication_root": RECORD_ROOT.as_posix(),
        "approval_command": OPERATION, "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def execute_authorized_acquisition(
    *, root: Path, authorization: OperationReceipt,
    get_cost: Callable[..., object], get_range: Callable[..., object],
    credential_source: str,
) -> dict[str, object]:
    started = time.monotonic()
    boundary = RepoBoundary(root)
    plan = load_acquisition_plan(root=root)
    if credential_source != CREDENTIAL_SOURCE:
        raise UnauthorizedOperation("BBO acquisition credential source is not bound")
    require_locked_repository_environment(root)
    claim = authorization.consume(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=_required_scope(root=root, plan=plan),
    )
    cost_record = load_cost_record(root=root)
    queries = build_quote_cost_queries(
        diagnostic_record=load_diagnostic_record(root=root),
    )
    validate_cost_record_queries(cost_record=cost_record, queries=queries)
    costs, fresh_estimates = validate_fresh_cost_quote(
        queries=queries, get_cost=get_cost, started_at=started,
    )
    fresh_total = sum(costs.values(), Decimal("0"))
    attempt_root = root / STAGING_ROOT / authorization.receipt_id
    if attempt_root.exists():
        raise IntegrityError("BBO acquisition attempt path already exists")
    staged: list[StagedBboFile] = []
    for query in queries:
        if time.monotonic() - started >= MAXIMUM_HOST_RUNTIME_SECONDS:
            raise IntegrityError("BBO acquisition exceeded its frozen host runtime")
        store = get_range(**provider_request(query))
        staged.append(stage_query_store(
            root=root, attempt_root=attempt_root, query=query,
            store=store, estimated_cost=costs[query.query_id],
        ))
    core = {
        "schema_version": "tier1_frozen_quote_recovery_acquisition/1.0.0",
        "state": "PREPARED_CREATE_ONLY", "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_claim_sha256": sha256_file(claim),
        "cost_record_id": COST_RECORD_ID, "cost_record_sha256": COST_RECORD_SHA256,
        "protocol_id": PROTOCOL_ID, "protocol_sha256": PROTOCOL_SHA256,
        "query_count": len(queries),
        "query_set_id": sha256_json([item.as_dict() for item in queries]),
        "staged_files": [item.as_dict() for item in staged],
        "staged_files_id": sha256_json([item.as_dict() for item in staged]),
        "prior_total_estimated_data_cost_usd": cost_record["total_estimated_data_cost_usd"],
        "fresh_query_estimates": list(fresh_estimates),
        "fresh_total_estimated_data_cost_usd": format(fresh_total, "f"),
        "fresh_metadata_cost_calls_before_download": len(fresh_estimates),
        "maximum_authorized_external_cost_usd": format(MAXIMUM_EXTERNAL_COST_USD, "f"),
        "credential_source": CREDENTIAL_SOURCE,
        "provider": "Databento", "dataset": DATASET, "schema": SCHEMA,
        "market_rows_downloaded": True, "staging_only": True,
        "successor_source_activated": False, "active_data_mutation": False,
        "protocol_changed": False, "model_fit": False,
        "prediction_generation": False, "historical_evaluation": False,
        "trial_registration_or_retirement": False,
        "holdout_or_forward_access": False, "trading": False,
    }
    record_id = sha256_json(core)
    record = root / RECORD_ROOT / f"{record_id}.json"
    event = root / EVENT_ROOT / f"{record_id}.json"
    boundary.assert_active_path(record.absolute(), purpose="BBO acquisition record", subtree=RECORD_ROOT.as_posix())
    boundary.assert_active_path(event.absolute(), purpose="BBO acquisition event", subtree=EVENT_ROOT.as_posix())
    if record.exists() or event.exists():
        raise IntegrityError("BBO acquisition publication is create-only")
    record.parent.mkdir(parents=True, exist_ok=True)
    event.parent.mkdir(parents=True, exist_ok=True)
    with record.open("xb") as stream:
        stream.write(canonical_bytes({
            **core, "state": "PUBLISHED_STAGING_ONLY", "record_id": record_id,
        }) + b"\n")
    with event.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_frozen_quote_recovery_acquisition_event/1.0.0",
            "event_type": "PUBLISHED", "record_id": record_id,
            "authorization_receipt_id": authorization.receipt_id,
        }) + b"\n")
    return {
        "record_id": record_id, "record_path": record.relative_to(root).as_posix(),
        "event_path": event.relative_to(root).as_posix(),
        "authorization_claim_path": claim.relative_to(root).as_posix(),
        "staged_file_count": len(staged),
        "fresh_total_estimated_data_cost_usd": format(fresh_total, "f"),
    }
