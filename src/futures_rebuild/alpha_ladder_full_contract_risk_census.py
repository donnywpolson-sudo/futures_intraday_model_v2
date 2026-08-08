"""Return-free 41-market census of one-full-contract planned loss.

Preparation reads only immutable metadata. Historical Parquet is opened only by
``execute_once`` after its exact single-use authorization has been consumed.
The census emits causal stop geometry and dollar risk, never OHLC prices,
returns, labels, predictions, fitted models, or a selected account risk unit.
"""

from __future__ import annotations

import json
import math
import multiprocessing
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, InvalidOperation
from pathlib import Path
from time import monotonic
from zoneinfo import ZoneInfo

from .active_data_view import resolve
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .cash_open_source_compatibility import source_row_from_mapping
from .errors import IntegrityError, UnauthorizedOperation
from .research_gateway_policy import ALPHA_LADDER_READINESS_CENSUS_OPERATION


CT = ZoneInfo("America/Chicago")
YEARS = (2018, 2019, 2020, 2021, 2022)
CHECKPOINT = "10:00"
DECISION_LATENCY_SECONDS = 5
STOP_ATR_MULTIPLE = Decimal("1.5")
FEATURE_BAR_COUNT = 21
TRUE_RANGE_COUNT = 20
TRAINING_SESSIONS = 504
EMBARGO_SESSIONS = 1
EVALUATION_SESSIONS = 63
OUTER_FOLDS = 8
PURGE_MINUTES = 40
MINIMUM_TRAINING_RISK_FEASIBLE = 252
MINIMUM_EVALUATION_RISK_FEASIBLE = 32
PILOT_MINIMUM_TRADES = 8
OPPORTUNITY_BUFFER_MULTIPLE = 4
RISK_LEVELS_USD = (
    "250", "500", "750", "1000", "1500", "2000", "3000", "5000", "10000", "25000",
)
PERCENTILES = (0, 25, 50, 75, 90, 95, 99, 100)

PLAN_PATH = Path("configs/alpha_ladder_full_contract_risk_census_plan.json")
RUNNER_PATH = Path("scripts/run_alpha_ladder_full_contract_risk_census.py")
OUTPUT_ROOT = Path("state/unpublished_evidence/alpha_ladder_full_contract_risk_census")
ACTIVE_LADDER_POINTER = Path("configs/active_alpha_research_ladder.json")
ACTIVE_CALENDAR_POINTER = Path("configs/active_cash_open_impulse_historical_calendar.json")
ACTIVE_CATALOG_PATH = Path("data/active/catalog.json")
ECONOMICS_RULES_PATH = Path("configs/contract_economics_rules.json")
FAILED_MECHANISM_ID = "cfefe8ce78e46d1e6a68184cbebdf4f4fe6d46169dc7bbfcfcd501c595563dc3"
FAILED_MECHANISM_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_full_regular_source_observable_successor/"
    f"{FAILED_MECHANISM_ID}/mechanism.json"
)
FAILED_CLOSURE_ID = "6b9ab13e0f1400af9f3dc5abce99d9afe9540bd439bbc38685a04d62ccef44c7"
FAILED_CLOSURE_PATH = Path(
    "state/trial_registry/alpha_ladder_es_pilot_terminal_closure/"
    f"{FAILED_CLOSURE_ID}.json"
)

REQUIRED_COLUMNS = frozenset(
    {
        "actual_identity_hash",
        "disposition",
        "event_at_ns",
        "exchange_session_date",
        "source_row_sha256",
        "open_nano",
        "high_nano",
        "low_nano",
        "close_nano",
        "volume",
        "tick_size",
        "tick_value",
    }
)


@dataclass(frozen=True)
class RiskBar:
    event_at: datetime
    available_at: datetime
    identity: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    tick_size: Decimal
    tick_value: Decimal


def _read_canonical(path: Path, *, name: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"{name} is unreadable") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{name} is not canonical JSON")
    return payload


def _read_json(path: Path, *, name: str) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"{name} is unreadable") from exc
    if not isinstance(payload, dict):
        raise IntegrityError(f"{name} is not a JSON object")
    return payload


def _decimal(value: object, *, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError(f"{name} is not decimal") from exc
    if not result.is_finite() or result <= 0:
        raise IntegrityError(f"{name} must be positive")
    return result


def _active_inputs(root: Path) -> tuple[dict[str, object], ...]:
    ladder_pointer = _read_canonical(root / ACTIVE_LADDER_POINTER, name="active Alpha ladder")
    contract_path = root / str(ladder_pointer.get("contract_path"))
    profile_path = root / str(ladder_pointer.get("profile_path"))
    if (
        sha256_file(contract_path) != ladder_pointer.get("contract_sha256")
        or sha256_file(profile_path) != ladder_pointer.get("profile_sha256")
    ):
        raise IntegrityError("active Alpha ladder bindings drifted")
    contract = _read_canonical(contract_path, name="Alpha ladder universe contract")
    calendar_pointer = _read_canonical(root / ACTIVE_CALENDAR_POINTER, name="active calendar")
    calendar_path = root / str(calendar_pointer.get("calendar_path"))
    if sha256_file(calendar_path) != calendar_pointer.get("calendar_sha256"):
        raise IntegrityError("active calendar binding drifted")
    calendar = _read_canonical(calendar_path, name="active calendar registry")
    catalog = _read_canonical(root / ACTIVE_CATALOG_PATH, name="active catalog")
    rules = _read_json(root / ECONOMICS_RULES_PATH, name="contract economics rules")
    mechanism = _read_canonical(root / FAILED_MECHANISM_PATH, name="failed mechanism")
    closure = _read_canonical(root / FAILED_CLOSURE_PATH, name="failed pilot closure")
    if (
        mechanism.get("mechanism_id") != FAILED_MECHANISM_ID
        or closure.get("closure_id") != FAILED_CLOSURE_ID
        or closure.get("mechanism_id") != FAILED_MECHANISM_ID
        or closure.get("economic_result") != "FAIL"
        or closure.get("attempt_consumed") is not True
    ):
        raise IntegrityError("failed pilot preservation binding is invalid")
    return ladder_pointer, contract, calendar_pointer, calendar, catalog, rules, mechanism, closure


def _exact_markets(contract: Mapping[str, object]) -> tuple[str, ...]:
    try:
        markets = tuple(str(item) for item in contract["stages"]["tier_3"]["markets"])
    except (KeyError, TypeError) as exc:
        raise IntegrityError("Alpha ladder lacks the Tier 3 market universe") from exc
    if len(markets) != 41 or len(set(markets)) != 41:
        raise IntegrityError("full-contract census requires exactly 41 markets")
    return markets


def _economics_by_market(rules: Mapping[str, object], markets: Sequence[str]) -> dict[str, object]:
    raw = rules.get("rules")
    if not isinstance(raw, list):
        raise IntegrityError("contract economics rules are absent")
    parsed = {
        str(item["market"]): {
            "point_value": str(item["point_value"]),
            "expected_unit_qty": str(item["expected_unit_qty"]),
            "quote_convention": str(item["quote_convention"]),
            "source_ids": list(item["source_ids"]),
        }
        for item in raw
        if isinstance(item, dict)
    }
    if set(parsed) != set(markets):
        raise IntegrityError("contract economics rules do not match the 41 markets")
    for market, item in parsed.items():
        _decimal(item["point_value"], name=f"{market} point value")
        _decimal(item["expected_unit_qty"], name=f"{market} unit quantity")
    return {market: parsed[market] for market in markets}


def _catalog_inventory(
    catalog: Mapping[str, object], markets: Sequence[str]
) -> tuple[list[dict[str, object]], dict[str, str]]:
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are absent")
    by_key = {
        (str(item["market"]), int(item["year"])): item
        for item in entries
        if isinstance(item, dict) and item.get("market") in markets and item.get("year") in YEARS
    }
    inventory: list[dict[str, object]] = []
    sources: dict[str, str] = {}
    for market in markets:
        for year in YEARS:
            item = by_key.get((market, year))
            if item is None:
                inventory.append({"market": market, "year": year, "disposition": "ABSENT"})
                continue
            record: dict[str, object] = {
                "market": market,
                "year": year,
                "disposition": str(item.get("disposition")),
            }
            if item.get("disposition") == "RESEARCH_READY_CAUSAL_PRICE":
                path = str(item.get("parquet_path"))
                digest = str(item.get("parquet_sha256"))
                if not path.startswith("data/active/") or len(digest) != 64:
                    raise IntegrityError("active catalog price binding is invalid")
                record.update({"path": path, "sha256": digest})
                sources[path] = digest
            else:
                record["reason"] = str(item.get("reason"))
            inventory.append(record)
    if len(inventory) != 205 or len(sources) != 198:
        raise IntegrityError("2018-2022 catalog inventory differs from the expected 41-market state")
    return inventory, dict(sorted(sources.items()))


def build_plan(*, root: Path) -> dict[str, object]:
    (
        ladder_pointer,
        contract,
        calendar_pointer,
        calendar,
        catalog,
        rules,
        mechanism,
        _closure,
    ) = _active_inputs(root)
    markets = _exact_markets(contract)
    economics = _economics_by_market(rules, markets)
    inventory, source_bindings = _catalog_inventory(catalog, markets)
    schedule_families = calendar.get("market_to_schedule_family")
    if not isinstance(schedule_families, dict) or set(schedule_families) != set(markets):
        raise IntegrityError("calendar schedule families do not match the 41 markets")
    product_intervals = calendar.get("product_effective_intervals")
    if not isinstance(product_intervals, dict) or set(product_intervals) != set(markets):
        raise IntegrityError("calendar product intervals do not match the 41 markets")
    costs = mechanism.get("costs")
    if not isinstance(costs, dict):
        raise IntegrityError("failed mechanism costs are absent")
    adverse = costs.get("round_trip_adverse_ticks")
    if (
        costs.get("round_trip_fee_usd") != "10.00"
        or not isinstance(adverse, dict)
        or set(adverse) != {"base", "stress", "extreme"}
        or any(set(values) != set(markets) for values in adverse.values() if isinstance(values, dict))
        or any(not isinstance(values, dict) for values in adverse.values())
    ):
        raise IntegrityError("locked 41-market cost schedule is invalid")
    stages = contract.get("stages")
    assert isinstance(stages, dict)
    tier_0 = stages["tier_0"]
    assert isinstance(tier_0, dict)
    tier_sets = {
        # Schema v2 makes the pilot an internal Tier 0 gate.  Keep the historic
        # key in census evidence so already-produced reports remain comparable.
        "pilot": list(tier_0["markets"]),
        "tier_1": list(stages["tier_1"]["markets"]),
        "tier_2": list(stages["tier_2"]["markets"]),
        "tier_3": list(stages["tier_3"]["markets"]),
        "traditional": list(stages["tier_3"]["traditional_markets"]),
        "satellite": list(stages["tier_3"]["satellite_markets"]),
    }
    bindings = {
        ACTIVE_LADDER_POINTER.as_posix(): sha256_file(root / ACTIVE_LADDER_POINTER),
        str(ladder_pointer["contract_path"]): str(ladder_pointer["contract_sha256"]),
        str(ladder_pointer["profile_path"]): str(ladder_pointer["profile_sha256"]),
        ACTIVE_CALENDAR_POINTER.as_posix(): sha256_file(root / ACTIVE_CALENDAR_POINTER),
        str(calendar_pointer["calendar_path"]): str(calendar_pointer["calendar_sha256"]),
        ACTIVE_CATALOG_PATH.as_posix(): sha256_file(root / ACTIVE_CATALOG_PATH),
        ECONOMICS_RULES_PATH.as_posix(): sha256_file(root / ECONOMICS_RULES_PATH),
        FAILED_MECHANISM_PATH.as_posix(): sha256_file(root / FAILED_MECHANISM_PATH),
        FAILED_CLOSURE_PATH.as_posix(): sha256_file(root / FAILED_CLOSURE_PATH),
        "src/futures_rebuild/active_data_view.py": sha256_file(root / "src/futures_rebuild/active_data_view.py"),
        "src/futures_rebuild/research_gateway_policy.py": sha256_file(root / "src/futures_rebuild/research_gateway_policy.py"),
        "src/futures_rebuild/alpha_ladder_full_contract_risk_census.py": sha256_file(Path(__file__)),
        RUNNER_PATH.as_posix(): sha256_file(root / RUNNER_PATH),
    }
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_full_contract_risk_census_plan/1.0.0",
        "state": "PREPARED_NOT_EXECUTED",
        "operation": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "purpose": "RETURN_FREE_FULL_CONTRACT_ACCOUNT_RISK_FEASIBILITY",
        "failed_mechanism_id": FAILED_MECHANISM_ID,
        "failed_pilot_closure_id": FAILED_CLOSURE_ID,
        "markets": list(markets),
        "years": list(YEARS),
        "checkpoint": CHECKPOINT,
        "tier_sets": tier_sets,
        "product_groups": {market: str(schedule_families[market]) for market in markets},
        "product_effective_intervals": {market: product_intervals[market] for market in markets},
        "full_contract_economics": economics,
        "catalog_inventory": inventory,
        "source_bindings": source_bindings,
        "stop_geometry": {
            "atr_period": TRUE_RANGE_COUNT,
            "atr_multiple": str(STOP_ATR_MULTIPLE),
            "feature_bar_count": FEATURE_BAR_COUNT,
            "rounding": "CEILING_TO_FULL_TICK",
            "contracts": 1,
            "micros": False,
            "fractional_contracts": False,
            "proxy_fills": False,
        },
        "locked_costs": {
            "round_trip_fee_usd": costs["round_trip_fee_usd"],
            "round_trip_adverse_ticks": adverse,
            "primary_feasibility_scenario": "stress",
        },
        "diagnostic_risk_levels_usd": list(RISK_LEVELS_USD),
        "percentiles": list(PERCENTILES),
        "feasibility_gate": {
            "checkpoint_accounting_percent": 100,
            "minimum_training_risk_feasible_sessions_per_fold": MINIMUM_TRAINING_RISK_FEASIBLE,
            "minimum_evaluation_risk_feasible_sessions_per_fold": MINIMUM_EVALUATION_RISK_FEASIBLE,
            "pilot_minimum_trades": PILOT_MINIMUM_TRADES,
            "pilot_opportunity_buffer_multiple": OPPORTUNITY_BUFFER_MULTIPLE,
            "product_pre_effective_sessions": "EXPLICIT_NOT_APPLICABLE_ACCOUNTED",
            "missing_source_or_economics": "FAIL_CLOSED",
            "registration_authorized_by_this_census": False,
            "complete_execution_path_readiness_required_separately": True,
        },
        "folds": {
            "construction": "FULL_REGULAR_PRODUCT_EFFECTIVE_SOURCE_OBSERVABLE_CALENDAR_BEFORE_ROWS",
            "initial_training_sessions": TRAINING_SESSIONS,
            "embargo_sessions": EMBARGO_SESSIONS,
            "evaluation_sessions": EVALUATION_SESSIONS,
            "outer_folds": OUTER_FOLDS,
            "purge_minutes": PURGE_MINUTES,
        },
        "account_risk_decision": {
            "selected_r_usd": None,
            "automatic_selection_forbidden": True,
            "owner_attestation_required": True,
            "trade_limit": "1R",
            "daily_loss_limit": "2R",
            "continuous_drawdown_limit": "6R",
            "next_state": "AWAITING_OWNER_ACCOUNT_R_AFTER_CENSUS",
        },
        "authority": {
            "historical_row_read": True,
            "price_values_emitted": False,
            "planned_loss_usd_emitted": True,
            "returns_or_trade_pnl": False,
            "model_fit": False,
            "prediction_generation": False,
            "performance_evaluation": False,
            "mechanism_creation": False,
            "registration": False,
            "publication": False,
            "provider_network_credentials": False,
            "year_2025_access": False,
            "trading": False,
        },
        "execution_limits": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_workers": 4,
            "worker_deadline_seconds": 3300,
            "maximum_runtime_seconds": 3600,
            "maximum_external_cost_usd": "0",
            "windows_host_required": True,
        },
        "output_root": OUTPUT_ROOT.as_posix(),
        "bindings": dict(sorted(bindings.items())),
    }
    return {**core, "plan_id": sha256_json(core)}


def _validate_plan_semantics(plan: Mapping[str, object]) -> None:
    stop = plan.get("stop_geometry")
    account = plan.get("account_risk_decision")
    authority = plan.get("authority")
    limits = plan.get("execution_limits")
    gate = plan.get("feasibility_gate")
    if (
        plan.get("state") != "PREPARED_NOT_EXECUTED"
        or plan.get("operation") != ALPHA_LADDER_READINESS_CENSUS_OPERATION
        or plan.get("markets") is None
        or len(plan["markets"]) != 41
        or plan.get("years") != list(YEARS)
        or plan.get("checkpoint") != CHECKPOINT
        or not isinstance(stop, Mapping)
        or stop.get("contracts") != 1
        or stop.get("micros") is not False
        or stop.get("fractional_contracts") is not False
        or stop.get("proxy_fills") is not False
        or stop.get("atr_multiple") != "1.5"
        or not isinstance(account, Mapping)
        or account.get("selected_r_usd") is not None
        or account.get("automatic_selection_forbidden") is not True
        or account.get("trade_limit") != "1R"
        or account.get("daily_loss_limit") != "2R"
        or account.get("continuous_drawdown_limit") != "6R"
        or not isinstance(gate, Mapping)
        or gate.get("minimum_training_risk_feasible_sessions_per_fold") != 252
        or gate.get("minimum_evaluation_risk_feasible_sessions_per_fold") != 32
        or gate.get("pilot_opportunity_buffer_multiple") != 4
        or not isinstance(authority, Mapping)
        or authority.get("returns_or_trade_pnl") is not False
        or authority.get("model_fit") is not False
        or authority.get("year_2025_access") is not False
        or authority.get("mechanism_creation") is not False
        or not isinstance(limits, Mapping)
        or limits.get("maximum_attempts") != 1
        or limits.get("maximum_retries") != 0
        or limits.get("maximum_workers") != 4
        or limits.get("maximum_runtime_seconds") != 3600
        or limits.get("maximum_external_cost_usd") != "0"
    ):
        raise IntegrityError("full-contract risk census plan semantics drifted")


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="full-contract risk census plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    if plan.get("plan_id") != sha256_json(core):
        raise IntegrityError("full-contract risk census plan identity drifted")
    _validate_plan_semantics(plan)
    bindings = plan.get("bindings")
    if not isinstance(bindings, Mapping) or any(
        sha256_file(root / str(path)) != digest for path, digest in bindings.items()
    ):
        raise IntegrityError("full-contract risk census non-price binding drifted")
    current = build_plan(root=root)
    if current != plan:
        raise IntegrityError("full-contract risk census active metadata drifted")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    sources = plan["source_bindings"]
    limits = plan["execution_limits"]
    assert isinstance(sources, Mapping) and isinstance(limits, Mapping)
    return {
        "purpose": "RETURN_FREE_41_MARKET_FULL_CONTRACT_RISK_FEASIBILITY",
        "period": "2018,2019,2020,2021,2022",
        "market_count": "41",
        "bound_source_file_count": str(len(sources)),
        "checkpoint": CHECKPOINT,
        "contracts_per_opportunity": "1_FULL_CONTRACT",
        "micros_or_fractional_contracts": "false",
        "stop": "1.5_ATR20_CEILING_FULL_TICK",
        "output_root": str(plan["output_root"]),
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "maximum_workers": "4",
        "worker_deadline_seconds": str(limits["worker_deadline_seconds"]),
        "maximum_runtime_seconds": str(limits["maximum_runtime_seconds"]),
        "maximum_external_cost_usd": "0",
        "price_values_emitted": "false",
        "planned_loss_usd_emitted": "true",
        "returns_or_trade_pnl": "false",
        "model_fit": "false",
        "prediction_generation": "false",
        "performance_evaluation": "false",
        "mechanism_creation": "false",
        "registration": "false",
        "publication": "false",
        "provider_network_credentials": "false",
        "holdout_2025_access": "false",
        "trading": "false",
        "approval_command": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def build_account_risk_policy(*, r_usd: str, owner_attestation: str) -> dict[str, object]:
    """Build the later 1R/2R/6R policy without consulting census passage."""

    risk = _decimal(r_usd, name="owner account R")
    if owner_attestation != "OWNER_ACCOUNT_TOLERANCE_NOT_SELECTED_TO_PASS_CENSUS":
        raise IntegrityError("account R requires the explicit owner-tolerance attestation")
    core = {
        "schema_version": "alpha_ladder_full_contract_account_risk/1.0.0",
        "instrument": "ONE_FULL_CONTRACT",
        "r_usd": str(risk),
        "maximum_planned_loss_usd": str(risk),
        "daily_loss_limit_usd": str(risk * 2),
        "continuous_drawdown_limit_usd": str(risk * 6),
        "owner_attestation": owner_attestation,
        "fixed_across_alpha_ladder": True,
        "automatic_historical_coverage_selection": False,
    }
    return {**core, "policy_id": sha256_json(core)}


def _feature_clock(event_at_ns: int) -> bool:
    clock = datetime.fromtimestamp(event_at_ns / 1_000_000_000, timezone.utc).astimezone(CT).time()
    return time(9, 30) <= clock < time(10, 0)


def _read_market(task: tuple[str, tuple[tuple[int, str], ...], str]) -> tuple[object, ...]:
    import pyarrow.parquet as pq

    market, sources, point_value_raw = task
    point_value = Decimal(point_value_raw)
    bars: dict[str, list[RiskBar]] = {}
    issues: dict[str, set[str]] = {}
    audits: dict[str, object] = {}
    for year, raw_path in sources:
        path = Path(raw_path)
        parquet = pq.ParquetFile(path)
        missing = sorted(REQUIRED_COLUMNS - set(parquet.schema_arrow.names))
        if missing:
            audits[f"{market}/{year}"] = {
                "source_path": path.as_posix(),
                "source_sha256": sha256_file(path),
                "schema_missing": missing,
                "total_rows_scanned": 0,
                "feature_window_rows_retained": 0,
            }
            continue
        total = retained = sessionless = nonexecutable = 0
        for batch in parquet.iter_batches(batch_size=65_536, columns=sorted(REQUIRED_COLUMNS)):
            columns = batch.to_pydict()
            for index in range(batch.num_rows):
                total += 1
                event_ns = columns["event_at_ns"][index]
                if type(event_ns) is not int or not _feature_clock(event_ns):
                    continue
                raw = {name: values[index] for name, values in columns.items()}
                session = raw.get("exchange_session_date")
                if not isinstance(session, str):
                    sessionless += 1
                    continue
                try:
                    normalized = source_row_from_mapping(market=market, row=raw)
                except (IntegrityError, UnauthorizedOperation):
                    issues.setdefault(session, set()).add("INVALID_CAUSAL_IDENTITY_FIELDS")
                    continue
                if not normalized.executable:
                    nonexecutable += 1
                    continue
                try:
                    tick_size = Decimal(str(raw["tick_size"]))
                    tick_value = Decimal(str(raw["tick_value"]))
                    if tick_size <= 0 or tick_value <= 0 or tick_size * point_value != tick_value:
                        raise ValueError
                    event = datetime.fromtimestamp(event_ns / 1_000_000_000, timezone.utc).astimezone(CT)
                    available = datetime.fromtimestamp(
                        normalized.available_at_ns / 1_000_000_000, timezone.utc
                    ).astimezone(CT)
                    bar = RiskBar(
                        event_at=event,
                        available_at=available,
                        identity=str(normalized.actual_identity_hash),
                        open=Decimal(raw["open_nano"]) / Decimal(1_000_000_000),
                        high=Decimal(raw["high_nano"]) / Decimal(1_000_000_000),
                        low=Decimal(raw["low_nano"]) / Decimal(1_000_000_000),
                        close=Decimal(raw["close_nano"]) / Decimal(1_000_000_000),
                        volume=Decimal(str(raw["volume"])),
                        tick_size=tick_size,
                        tick_value=tick_value,
                    )
                except (InvalidOperation, TypeError, ValueError):
                    issues.setdefault(session, set()).add("INVALID_FIELDS_OR_ECONOMICS")
                    continue
                bars.setdefault(session, []).append(bar)
                retained += 1
        audits[f"{market}/{year}"] = {
            "source_path": path.as_posix(),
            "source_sha256": sha256_file(path),
            "schema_missing": [],
            "total_rows_scanned": total,
            "feature_window_rows_retained": retained,
            "sessionless_feature_window_rows": sessionless,
            "nonexecutable_feature_window_rows": nonexecutable,
        }
    ordered = {key: tuple(sorted(value, key=lambda item: item.event_at)) for key, value in bars.items()}
    return market, ordered, {key: tuple(sorted(value)) for key, value in issues.items()}, audits


def classify_feature_risk(
    *, session: str, bars: Sequence[RiskBar], point_value: Decimal,
    fee_usd: Decimal, cost_ticks: Mapping[str, int], risk_levels: Sequence[Decimal],
) -> dict[str, object]:
    checkpoint = datetime.combine(date.fromisoformat(session), time(10, 0), CT)
    decision = checkpoint + timedelta(seconds=DECISION_LATENCY_SECONDS)
    ordered = tuple(sorted(bars, key=lambda item: item.event_at))
    if len({bar.event_at for bar in ordered}) != len(ordered):
        return {"disposition": "FEATURE_INCOMPLETE_DUPLICATE_TIMESTAMP"}
    causal = tuple(
        bar for bar in ordered
        if time(9, 30) <= bar.event_at.time() < time(10, 0) and bar.available_at <= decision
    )
    if len(causal) < FEATURE_BAR_COUNT:
        return {"disposition": "FEATURE_INCOMPLETE_MISSING_OR_LATE_BARS"}
    feature = causal[-FEATURE_BAR_COUNT:]
    if (
        len({bar.identity for bar in feature}) != 1
        or len({(bar.tick_size, bar.tick_value) for bar in feature}) != 1
        or any(current.event_at - previous.event_at != timedelta(minutes=1)
               for previous, current in zip(feature, feature[1:]))
    ):
        return {"disposition": "FEATURE_INCOMPLETE_IDENTITY_ECONOMICS_OR_TIME"}
    if any(
        bar.open <= 0 or bar.high <= 0 or bar.low <= 0 or bar.close <= 0
        or bar.volume < 0 or bar.high < bar.low
        or not bar.low <= bar.open <= bar.high or not bar.low <= bar.close <= bar.high
        or bar.tick_size * point_value != bar.tick_value
        for bar in feature
    ):
        return {"disposition": "ECONOMICS_OR_FEATURE_FIELDS_INVALID"}
    true_ranges = [
        max(current.high - current.low, abs(current.high - previous.close), abs(current.low - previous.close))
        for previous, current in zip(feature, feature[1:])
    ]
    atr20 = sum(true_ranges, Decimal(0)) / Decimal(TRUE_RANGE_COUNT)
    tick_size = feature[-1].tick_size
    tick_value = feature[-1].tick_value
    stop_ticks = int((STOP_ATR_MULTIPLE * atr20 / tick_size).to_integral_value(rounding=ROUND_CEILING))
    if stop_ticks <= 0:
        return {"disposition": "STOP_GEOMETRY_INVALID"}
    planned = {
        scenario: Decimal(stop_ticks) * tick_value + fee_usd + Decimal(ticks) * tick_value
        for scenario, ticks in cost_ticks.items()
    }
    stress = planned["stress"]
    return {
        "disposition": "RISK_MEASURABLE",
        "stop_ticks": stop_ticks,
        "tick_size": str(tick_size),
        "tick_value": str(tick_value),
        "point_value": str(point_value),
        "planned_loss_usd": {key: str(value) for key, value in sorted(planned.items())},
        "risk_feasible_by_level": {str(level): stress <= level for level in risk_levels},
    }


def _effective(session: str, intervals: Sequence[Mapping[str, object]]) -> bool:
    instant = datetime.combine(date.fromisoformat(session), time(10, 0), CT).astimezone(timezone.utc)
    ns = int(instant.timestamp() * 1_000_000_000)
    return any(int(item["activation_ns"]) <= ns < int(item["expiration_ns_exclusive"]) for item in intervals)


def _folds(sessions: Sequence[str]) -> tuple[dict[str, object], ...]:
    ordered = tuple(sessions)
    if ordered != tuple(sorted(set(ordered))):
        raise IntegrityError("risk census fold sessions are not unique and chronological")
    required = TRAINING_SESSIONS + EMBARGO_SESSIONS + OUTER_FOLDS * EVALUATION_SESSIONS
    if len(ordered) < required:
        return ()
    result = []
    for index in range(OUTER_FOLDS):
        fit_count = TRAINING_SESSIONS + index * EVALUATION_SESSIONS
        evaluation_start = fit_count + EMBARGO_SESSIONS
        result.append({
            "fold_id": f"fold-{index}",
            "training_sessions": list(ordered[:fit_count]),
            "embargo_sessions": list(ordered[fit_count:evaluation_start]),
            "evaluation_sessions": list(ordered[evaluation_start:evaluation_start + EVALUATION_SESSIONS]),
            "purge_minutes": PURGE_MINUTES,
        })
    return tuple(result)


def _nearest_rank(values: Sequence[Decimal], percentile: int) -> str | None:
    if not values:
        return None
    ordered = sorted(values)
    if percentile == 0:
        return str(ordered[0])
    index = max(0, math.ceil(percentile / 100 * len(ordered)) - 1)
    return str(ordered[index])


def summarize_records(records: Sequence[Mapping[str, object]], risk_levels: Sequence[Decimal]) -> dict[str, object]:
    measurable = [item for item in records if item.get("disposition") == "RISK_MEASURABLE"]
    expected = [
        item for item in records
        if item.get("calendar_eligible") is True and item.get("source_eligible") is True
    ]
    planned = {
        scenario: [Decimal(str(item["planned_loss_usd"][scenario])) for item in measurable]
        for scenario in ("base", "stress", "extreme")
    }
    return {
        "checkpoint_count": len(records),
        "expected_risk_measurable_count": len(expected),
        "risk_measurable_count": len(measurable),
        "disposition_counts": dict(sorted(Counter(str(item["disposition"]) for item in records).items())),
        "planned_loss_usd_percentiles": {
            scenario: {str(p): _nearest_rank(values, p) for p in PERCENTILES}
            for scenario, values in planned.items()
        },
        "stress_coverage_curve": {
            str(level): {
                "risk_feasible_count": sum(value <= level for value in planned["stress"]),
                "percent_of_measurable": (
                    str(Decimal(sum(value <= level for value in planned["stress"])) * 100 / Decimal(len(measurable)))
                    if measurable else None
                ),
                "percent_of_expected": (
                    str(Decimal(sum(value <= level for value in planned["stress"])) * 100 / Decimal(len(expected)))
                    if expected else None
                ),
            }
            for level in risk_levels
        },
    }


def _group_summary(
    records: Sequence[Mapping[str, object]], key, risk_levels: Sequence[Decimal]
) -> dict[str, object]:
    groups: dict[str, list[Mapping[str, object]]] = {}
    for item in records:
        groups.setdefault(str(key(item)), []).append(item)
    return {name: summarize_records(items, risk_levels) for name, items in sorted(groups.items())}


def execute_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> dict[str, object]:
    started = monotonic()
    plan = load_plan(root=root)
    if os.name != "nt" or multiprocessing.current_process().name != "MainProcess":
        raise UnauthorizedOperation("full-contract risk census requires the Windows main process")
    output_root = root / str(plan["output_root"])
    boundary.assert_active_path(output_root, purpose="unpublished full-contract risk census", subtree="state/unpublished_evidence")
    if output_root.exists():
        raise UnauthorizedOperation("full-contract risk census output already exists")
    use_path = receipt.consume(
        boundary,
        operation=ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    source_bindings = plan["source_bindings"]
    assert isinstance(source_bindings, Mapping)
    for path, digest in source_bindings.items():
        if sha256_file(root / str(path)) != digest:
            raise IntegrityError(f"bound source changed: {path}")
    inventory = plan["catalog_inventory"]
    economics = plan["full_contract_economics"]
    assert isinstance(inventory, list) and isinstance(economics, Mapping)
    source_by_market: dict[str, list[tuple[int, str]]] = {str(m): [] for m in plan["markets"]}
    catalog_state: dict[tuple[str, int], str] = {}
    for item in inventory:
        assert isinstance(item, Mapping)
        key = (str(item["market"]), int(item["year"]))
        catalog_state[key] = str(item["disposition"])
        if item["disposition"] == "RESEARCH_READY_CAUSAL_PRICE":
            resolved = resolve(repository_root=root, market=key[0], year=key[1], purpose="SELECTION")
            if resolved.relative_to(root).as_posix() != item["path"]:
                raise IntegrityError("active resolver differs from census plan")
            source_by_market[key[0]].append((key[1], str(resolved)))
    tasks = [
        (market, tuple(source_by_market[market]), str(economics[market]["point_value"]))
        for market in plan["markets"]
    ]
    limits = plan["execution_limits"]
    assert isinstance(limits, Mapping)
    pool = multiprocessing.get_context("spawn").Pool(processes=int(limits["maximum_workers"]))
    try:
        worker_results = pool.map_async(_read_market, tasks, chunksize=1).get(
            timeout=int(limits["worker_deadline_seconds"])
        )
        pool.close()
        pool.join()
    except BaseException:
        pool.terminate()
        pool.join()
        raise
    observed = {str(item[0]): item[1:] for item in worker_results}
    calendar_pointer = _read_canonical(root / ACTIVE_CALENDAR_POINTER, name="active calendar")
    calendar = _read_canonical(root / str(calendar_pointer["calendar_path"]), name="active calendar registry")
    calendar_rows = calendar.get("calendar_rows")
    source_observability = calendar.get("source_observability_records")
    if not isinstance(calendar_rows, list) or not isinstance(source_observability, list):
        raise IntegrityError("calendar checkpoint accounting is absent")
    source_unobservable = {
        (str(item["market"]), str(item["trade_date"]), str(item["checkpoint"]))
        for item in source_observability if isinstance(item, dict)
    }
    risk_levels = tuple(Decimal(item) for item in plan["diagnostic_risk_levels_usd"])
    fee = Decimal(str(plan["locked_costs"]["round_trip_fee_usd"]))
    cost_schedule = plan["locked_costs"]["round_trip_adverse_ticks"]
    product_intervals = plan["product_effective_intervals"]
    groups = plan["product_groups"]
    tier_sets = {name: set(values) for name, values in plan["tier_sets"].items()}
    records: list[dict[str, object]] = []
    folds_by_market: dict[str, list[dict[str, object]]] = {}
    source_audits: dict[str, object] = {}
    for market in plan["markets"]:
        bars_by_session, issues_by_session, audits = observed[str(market)]
        source_audits.update(audits)
        market_rows = [item for item in calendar_rows if item.get("market") == market and str(item.get("trade_date", ""))[:4] in {str(y) for y in YEARS}]
        eligible_sessions: list[str] = []
        market_records: dict[str, dict[str, object]] = {}
        for calendar_row in market_rows:
            session = str(calendar_row["trade_date"])
            year = int(session[:4])
            open_state = calendar_row["checkpoint_open"].get(CHECKPOINT) is True
            disposition = str(calendar_row["disposition"].get(CHECKPOINT))
            effective = _effective(session, product_intervals[market])
            catalog_ready = catalog_state.get((str(market), year)) == "RESEARCH_READY_CAUSAL_PRICE"
            explicit_unobservable = (str(market), session, CHECKPOINT) in source_unobservable
            calendar_eligible = open_state and disposition == "REGULAR_WEEKDAY_REFERENCE_RULE" and effective
            source_eligible = catalog_ready and not explicit_unobservable
            base = {
                "market": market,
                "trade_date": session,
                "year": year,
                "checkpoint": CHECKPOINT,
                "product_group": groups[market],
                "subgroup": "satellite" if market in tier_sets["satellite"] else "traditional",
                "alpha_tiers": [name for name in ("pilot", "tier_1", "tier_2", "tier_3") if market in tier_sets[name]],
                "calendar_eligible": calendar_eligible,
                "source_eligible": source_eligible,
            }
            if not effective:
                result = {"disposition": "PRODUCT_NOT_EFFECTIVE"}
            elif not open_state:
                result = {"disposition": "CALENDAR_CLOSED"}
            elif disposition != "REGULAR_WEEKDAY_REFERENCE_RULE":
                result = {"disposition": "HOLIDAY_MODIFIED"}
            elif explicit_unobservable:
                result = {"disposition": "SOURCE_UNOBSERVABLE"}
            elif not catalog_ready:
                result = {"disposition": "CATALOG_SOURCE_UNAVAILABLE"}
            elif issues_by_session.get(session):
                result = {"disposition": "SOURCE_OR_ECONOMICS_INVALID", "issue_codes": list(issues_by_session[session])}
            else:
                result = classify_feature_risk(
                    session=session,
                    bars=bars_by_session.get(session, ()),
                    point_value=Decimal(str(economics[market]["point_value"])),
                    fee_usd=fee,
                    cost_ticks={scenario: int(values[market]) for scenario, values in cost_schedule.items()},
                    risk_levels=risk_levels,
                )
            record = {**base, **result}
            records.append(record)
            market_records[session] = record
            if calendar_eligible and source_eligible:
                eligible_sessions.append(session)
        market_folds = _folds(eligible_sessions)
        fold_results: list[dict[str, object]] = []
        for fold in market_folds:
            training = [market_records[item] for item in fold["training_sessions"]]
            evaluation = [market_records[item] for item in fold["evaluation_sessions"]]
            fold_results.append({
                "fold_id": fold["fold_id"],
                "training": summarize_records(training, risk_levels),
                "evaluation": summarize_records(evaluation, risk_levels),
                "embargo_sessions": len(fold["embargo_sessions"]),
                "purge_minutes": fold["purge_minutes"],
            })
        folds_by_market[str(market)] = fold_results
    if len(records) != 74866:
        raise IntegrityError(f"checkpoint accounting expected 74866 records, found {len(records)}")
    if monotonic() - started > int(limits["maximum_runtime_seconds"]):
        raise UnauthorizedOperation("full-contract risk census exceeded total runtime")
    summaries = {
        "overall": summarize_records(records, risk_levels),
        "by_market": _group_summary(records, lambda item: item["market"], risk_levels),
        "by_product_group": _group_summary(records, lambda item: item["product_group"], risk_levels),
        "by_market_year": _group_summary(records, lambda item: f"{item['market']}/{item['year']}", risk_levels),
        "by_subgroup": _group_summary(records, lambda item: item["subgroup"], risk_levels),
        "by_alpha_tier": {
            tier: summarize_records([item for item in records if tier in item["alpha_tiers"]], risk_levels)
            for tier in ("pilot", "tier_1", "tier_2", "tier_3")
        },
    }
    core = {
        "schema_version": "alpha_ladder_full_contract_risk_census_report/1.0.0",
        "state": "SEALED_UNPUBLISHED_RETURN_FREE_RISK_FEASIBILITY",
        "plan_id": plan["plan_id"],
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "decision": "AWAITING_OWNER_ACCOUNT_R_NOT_AUTOMATICALLY_SELECTED",
        "checkpoint_accounting_count": len(records),
        "summaries": summaries,
        "fold_results_by_market": folds_by_market,
        "source_audits": dict(sorted(source_audits.items())),
        "authority": plan["authority"],
    }
    report = {**core, "report_id": sha256_json(core)}
    destination = output_root / str(report["report_id"])
    destination.mkdir(parents=True, exist_ok=False)
    outputs = {
        "checkpoint_accounting.json": {
            "schema_version": "alpha_ladder_full_contract_checkpoint_accounting/1.0.0",
            "plan_id": plan["plan_id"],
            "records": records,
        },
        "risk_feasibility_report.json": report,
    }
    for name, payload in outputs.items():
        with (destination / name).open("xb") as stream:
            stream.write(canonical_bytes(payload) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
    return report
