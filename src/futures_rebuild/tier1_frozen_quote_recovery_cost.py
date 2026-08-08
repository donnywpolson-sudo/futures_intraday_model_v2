"""Bounded provider-cost inquiry for unresolved frozen execution gaps.

The inquiry asks only what one-second top-of-book data would cost for the 33
entry/liquidation windows that could not be recovered from accepted local
trade-bar diagnostics.  It performs no market-row download and cannot adopt a
source, change the protocol, or register/evaluate a trial.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment
from .tier1_bracket_v5 import NS_PER_MINUTE
from .tier1_frozen_diagnostic_recovery import (
    ENTRY_DELAY_MINUTES,
    MAXIMUM_HOLD_MINUTES,
    MAXIMUM_LIQUIDATION_DELAY_MINUTES,
    PROTOCOL_ID,
    PROTOCOL_PATH,
    PROTOCOL_SHA256,
)
from .tier1_preexecution_recovery_feasibility import DBN_RELEASE_ID


PLAN_PATH = Path("configs/tier1_frozen_quote_recovery_cost_plan.json")
DIAGNOSTIC_RECORD_PATH = Path(
    "state/source_quality/tier1_frozen_diagnostic_recovery/"
    "43afeb164e576bbbe6e343cd1356a16773240cfd22ce3fa2673ca5f8e78b0cc9.json"
)
DIAGNOSTIC_RECORD_ID = DIAGNOSTIC_RECORD_PATH.stem
DIAGNOSTIC_RECORD_SHA256 = (
    "bfbbdc9c86115628c88959ba139bb97033554364d5a219daa90d5a5db90bdefd"
)
OPERATION = "QUOTE_FROZEN_TIER1_BBO_1S_RECOVERY_COST_AND_PUBLISH"
RECORD_ROOT = Path("state/provider_quotes/tier1_frozen_bbo_recovery_cost")
EVENT_ROOT = Path("state/provider_quote_events/tier1_frozen_bbo_recovery_cost")
DATASET = "GLBX.MDP3"
SCHEMA = "bbo-1s"
STYPE_IN = "continuous"
STYPE_OUT = "instrument_id"
QUOTE_STALENESS_SECONDS = 10
ENTRY_POST_ARRIVAL_SECONDS = 10
MAXIMUM_PROVIDER_CALLS = 30
MAXIMUM_HOST_RUNTIME_SECONDS = 300
CREDENTIAL_SOURCE = "file api.env"


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid quote-recovery artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("quote-recovery artifact is not an object")
    return value


def _iso_ns(value: int) -> str:
    if type(value) is not int or value < 0:
        raise IntegrityError("quote-recovery timestamp is invalid")
    seconds, nanos = divmod(value, 1_000_000_000)
    observed = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return observed.strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}Z"


@dataclass(frozen=True)
class QuoteCostQuery:
    query_id: str
    start: str
    end: str
    symbols: tuple[str, ...]
    opportunity_ids: tuple[str, ...]
    categories: tuple[str, ...]

    def provider_kwargs(self) -> dict[str, object]:
        return {
            "dataset": DATASET,
            "schema": SCHEMA,
            "stype_in": STYPE_IN,
            "symbols": list(self.symbols),
            "start": self.start,
            "end": self.end,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            "query_id": self.query_id,
            **self.provider_kwargs(),
            "stype_out": STYPE_OUT,
            "opportunity_ids": list(self.opportunity_ids),
            "categories": list(self.categories),
        }


def load_diagnostic_record(*, root: Path) -> dict[str, object]:
    path = root / DIAGNOSTIC_RECORD_PATH
    if sha256_file(path) != DIAGNOSTIC_RECORD_SHA256:
        raise IntegrityError("diagnostic recovery record changed")
    record = _object(path)
    if (
        record.get("record_id") != DIAGNOSTIC_RECORD_ID
        or record.get("state") != "PUBLISHED_SOURCE_QUALITY_ONLY"
        or record.get("target_count") != 34
        or record.get("disposition_counts") != {
            "DIAGNOSTIC_RECOVERY_CANDIDATE": 1,
            "NOT_OBSERVED_IN_BOUND_DIAGNOSTIC_SOURCES": 33,
        }
        or record.get("prices_reported") is not False
        or record.get("protocol_changed") is not False
        or record.get("historical_evaluation") is not False
    ):
        raise IntegrityError("diagnostic recovery record is not the accepted source-only result")
    return record


def build_quote_cost_queries(
    *, diagnostic_record: Mapping[str, object],
) -> tuple[QuoteCostQuery, ...]:
    recovery = diagnostic_record.get("recovery_map")
    if not isinstance(recovery, list):
        raise IntegrityError("diagnostic recovery map is absent")
    unresolved = [
        item for item in recovery
        if isinstance(item, Mapping)
        and item.get("disposition") == "NOT_OBSERVED_IN_BOUND_DIAGNOSTIC_SOURCES"
    ]
    if len(unresolved) != 33:
        raise IntegrityError("quote-recovery target count changed")
    second = 1_000_000_000
    grouped: dict[tuple[int, int], list[Mapping[str, object]]] = {}
    for item in unresolved:
        category = item.get("category")
        decision = item.get("decision_at_ns")
        market = item.get("market")
        year = item.get("year")
        if (
            category not in {"ENTRY", "LIQUIDATION"}
            or type(decision) is not int
            or market not in {"6E", "CL", "ES", "ZN"}
            or type(year) is not int or year not in range(2018, 2023)
        ):
            raise IntegrityError("quote-recovery target is outside the frozen scope")
        entry_at = decision + ENTRY_DELAY_MINUTES * NS_PER_MINUTE
        if category == "ENTRY":
            start = entry_at - QUOTE_STALENESS_SECONDS * second
            end = entry_at + (ENTRY_POST_ARRIVAL_SECONDS + 1) * second
        else:
            timeout = entry_at + MAXIMUM_HOLD_MINUTES * NS_PER_MINUTE
            deadline = timeout + MAXIMUM_LIQUIDATION_DELAY_MINUTES * NS_PER_MINUTE
            start = timeout - QUOTE_STALENESS_SECONDS * second
            end = deadline + second
        grouped.setdefault((start, end), []).append(item)
    output: list[QuoteCostQuery] = []
    for (start, end), items in sorted(grouped.items()):
        symbols = tuple(sorted({f"{item['market']}.v.0" for item in items}))
        opportunity_ids = tuple(sorted(str(item["opportunity_id"]) for item in items))
        categories = tuple(sorted({str(item["category"]) for item in items}))
        core = {
            "dataset": DATASET, "schema": SCHEMA,
            "stype_in": STYPE_IN, "stype_out": STYPE_OUT,
            "symbols": list(symbols), "start": _iso_ns(start), "end": _iso_ns(end),
            "opportunity_ids": list(opportunity_ids), "categories": list(categories),
        }
        output.append(QuoteCostQuery(
            query_id=sha256_json(core), start=str(core["start"]), end=str(core["end"]),
            symbols=symbols, opportunity_ids=opportunity_ids, categories=categories,
        ))
    if (
        len(output) != MAXIMUM_PROVIDER_CALLS
        or len({item.query_id for item in output}) != len(output)
        or len({opportunity for item in output for opportunity in item.opportunity_ids}) != 33
    ):
        raise IntegrityError("quote-recovery query grouping changed")
    return tuple(output)


def _decimal_cost(value: object) -> Decimal:
    if isinstance(value, bool):
        raise IntegrityError("provider cost estimate is invalid")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError("provider cost estimate is invalid") from exc
    if not result.is_finite() or result < 0:
        raise IntegrityError("provider cost estimate is invalid")
    return result


def quote_costs(
    *, queries: Sequence[QuoteCostQuery],
    get_cost: Callable[..., object],
) -> tuple[dict[str, object], ...]:
    """Make exactly one metadata-only cost call for every frozen query."""

    if len(queries) != MAXIMUM_PROVIDER_CALLS:
        raise IntegrityError("provider cost call count changed")
    output: list[dict[str, object]] = []
    for query in queries:
        cost = _decimal_cost(get_cost(**query.provider_kwargs()))
        output.append({
            "query_id": query.query_id,
            "estimated_data_cost_usd": format(cost, "f"),
            "provider_row_downloaded": False,
        })
    return tuple(output)


def load_quote_recovery_cost_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    diagnostic = load_diagnostic_record(root=root)
    queries = build_quote_cost_queries(diagnostic_record=diagnostic)
    forbidden = plan.get("forbidden_actions")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version") != "tier1_frozen_quote_recovery_cost_plan/1.0.0"
        or plan.get("state") != "PREPARED_REQUIRES_SEPARATE_PROVIDER_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("diagnostic_record_id") != DIAGNOSTIC_RECORD_ID
        or plan.get("diagnostic_record_sha256") != DIAGNOSTIC_RECORD_SHA256
        or plan.get("protocol_id") != PROTOCOL_ID
        or plan.get("protocol_sha256") != PROTOCOL_SHA256
        or plan.get("accepted_dbn_release_id") != DBN_RELEASE_ID
        or plan.get("query_count") != MAXIMUM_PROVIDER_CALLS
        or plan.get("query_set_id") != sha256_json([item.as_dict() for item in queries])
        or plan.get("schema") != SCHEMA
        or plan.get("credential_source") != CREDENTIAL_SOURCE
        or plan.get("maximum_host_runtime_seconds") != MAXIMUM_HOST_RUNTIME_SECONDS
        or plan.get("maximum_external_cost_usd") != "0"
        or plan.get("implementation_sha256") != sha256_file(Path(__file__))
        or sha256_file(root / PROTOCOL_PATH) != PROTOCOL_SHA256
        or not isinstance(forbidden, dict) or not forbidden
        or not all(value is True for value in forbidden.values())
    ):
        raise UnauthorizedOperation("quote-recovery cost plan is absent or drifted")
    return plan


def _required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "diagnostic_record_id": DIAGNOSTIC_RECORD_ID,
        "diagnostic_record_sha256": DIAGNOSTIC_RECORD_SHA256,
        "query_count": str(MAXIMUM_PROVIDER_CALLS),
        "query_set_id": str(plan["query_set_id"]),
        "provider": "Databento", "dataset": DATASET, "schema": SCHEMA,
        "credential_source": CREDENTIAL_SOURCE,
        "metadata_cost_calls_only": "true", "maximum_external_cost_usd": "0",
        "historical_row_read": "false", "market_row_download": "false",
        "successor_data_creation": "false", "active_data_mutation": "false",
        "protocol_change": "false", "model_fit": "false",
        "prediction_generation": "false", "historical_evaluation": "false",
        "trial_registration_or_retirement": "false",
        "holdout_or_forward_access": "false", "staging": "false",
        "commit": "false", "push": "false", "trading": "false",
        "publication_root": RECORD_ROOT.as_posix(),
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def execute_authorized_cost_quote(
    *, root: Path, authorization: OperationReceipt,
    get_cost: Callable[..., object], credential_source: str,
) -> dict[str, object]:
    boundary = RepoBoundary(root)
    plan = load_quote_recovery_cost_plan(root=root)
    if credential_source != CREDENTIAL_SOURCE:
        raise UnauthorizedOperation("quote-recovery credential source is not the bound file source")
    require_locked_repository_environment(root)
    claim = authorization.consume(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=_required_scope(root=root, plan=plan),
    )
    queries = build_quote_cost_queries(
        diagnostic_record=load_diagnostic_record(root=root),
    )
    estimates = quote_costs(queries=queries, get_cost=get_cost)
    total = sum(
        (_decimal_cost(item["estimated_data_cost_usd"]) for item in estimates),
        Decimal("0"),
    )
    core = {
        "schema_version": "tier1_frozen_quote_recovery_cost/1.0.0",
        "state": "PREPARED_CREATE_ONLY",
        "plan_id": plan["plan_id"], "plan_sha256": sha256_file(root / PLAN_PATH),
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_claim_sha256": sha256_file(claim),
        "diagnostic_record_id": DIAGNOSTIC_RECORD_ID,
        "diagnostic_record_sha256": DIAGNOSTIC_RECORD_SHA256,
        "protocol_id": PROTOCOL_ID, "protocol_sha256": PROTOCOL_SHA256,
        "query_count": len(queries),
        "query_set_id": sha256_json([item.as_dict() for item in queries]),
        "query_estimates": list(estimates),
        "total_estimated_data_cost_usd": format(total, "f"),
        "provider": "Databento", "dataset": DATASET, "schema": SCHEMA,
        "credential_source": CREDENTIAL_SOURCE,
        "metadata_cost_calls_only": True, "external_cost_incurred_usd": "0",
        "historical_rows_read": False, "market_rows_downloaded": False,
        "successor_data_created": False, "active_data_mutation": False,
        "protocol_changed": False, "model_fit": False,
        "prediction_generation": False, "historical_evaluation": False,
        "trial_registration_or_retirement": False,
        "holdout_or_forward_access": False, "trading": False,
    }
    record_id = sha256_json(core)
    record = root / RECORD_ROOT / f"{record_id}.json"
    event = root / EVENT_ROOT / f"{record_id}.json"
    boundary.assert_active_path(
        record.absolute(), purpose="quote recovery cost record",
        subtree=RECORD_ROOT.as_posix(),
    )
    boundary.assert_active_path(
        event.absolute(), purpose="quote recovery cost event",
        subtree=EVENT_ROOT.as_posix(),
    )
    if record.exists() or event.exists():
        raise IntegrityError("quote-recovery cost publication is create-only")
    record.parent.mkdir(parents=True, exist_ok=True)
    event.parent.mkdir(parents=True, exist_ok=True)
    with record.open("xb") as stream:
        stream.write(canonical_bytes({
            **core, "state": "PUBLISHED_PROVIDER_QUOTE_ONLY", "record_id": record_id,
        }) + b"\n")
    with event.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_frozen_quote_recovery_cost_event/1.0.0",
            "event_type": "PUBLISHED", "record_id": record_id,
            "diagnostic_record_id": DIAGNOSTIC_RECORD_ID,
            "authorization_receipt_id": authorization.receipt_id,
        }) + b"\n")
    return {
        "record_id": record_id,
        "record_path": record.relative_to(root).as_posix(),
        "event_path": event.relative_to(root).as_posix(),
        "authorization_claim_path": claim.relative_to(root).as_posix(),
        "query_count": len(queries),
        "total_estimated_data_cost_usd": format(total, "f"),
    }
