"""Synthetic-first mechanics and governance for the post-audit Tier 1 trial.

This module deliberately has no historical-row or publication entry point.  It
implements the causality, coverage, execution, risk, metric, and ordered-decision
contracts that must be proven on synthetic fixtures before a new trial may be
registered.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_CEILING
from pathlib import Path
from statistics import mean, stdev
from typing import Iterable, Mapping, Sequence

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


CONTRACT_PATH = Path("configs/tier1_bracket_post_audit_successor_v3.json")
CLOSURE_PATH = Path("configs/tier1_bracket_invalid_trial_closure_preparation_v2.json")
INVALID_TRIAL_ID = "aab8134537f5f6efa9d9ced5603adb89212d43e67282ab5a2ab7e3adb3fd011c"
INVALID_TRIAL_REGISTRY = Path(
    "state/trial_registry/tier1_bracket_successor_v2"
) / f"{INVALID_TRIAL_ID}.json"
INVALID_EVALUATION_EVENT = Path(
    "state/trial_events/tier1_bracket_successor_v2_evaluation"
) / "65baef1fc6360a0f17ea67ac62ed343896301980320bbaa0ec70210cf92cd74b.json"
INVALID_EVALUATION_MANIFEST = Path(
    "manifests/data_releases/evaluations"
) / "45054d8dce43317a746154fdb6d0a2346b4cc92a178d3824184a7d9b25e69038.json"
POST_AUDIT_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_post_audit_v3")
POST_AUDIT_EVENT_ROOT = Path("state/trial_events/tier1_bracket_post_audit_v3")
MARKETS = ("ES", "CL", "ZN", "6E")
CHECKPOINTS = ("08:30", "10:30", "13:30")
TERMINAL_DISPOSITIONS = {
    "PREDICTION_PRODUCED",
    "INSUFFICIENT_CAUSAL_HISTORY",
    "MISSING_OR_AMBIGUOUS_MARKET_IDENTITY",
    "MISSING_SCHEDULE_OR_ROLL_STATE",
    "MISSING_CAUSAL_ENTRY",
    "MISSING_PRICE_PATH",
    "RISK_CAP_REJECTION",
    "TRAINING_OR_PREDICTION_INELIGIBLE",
    "HURDLE_FAILURE",
    "CROSS_MARKET_RANKING_LOSS",
    "OVERLAP_ABSTENTION",
    "DAILY_STOP_ABSTENTION",
    "ENTRY_CAP_ABSTENTION",
    "DRAWDOWN_ABSTENTION",
    "ADMITTED_TRADE",
}
PRE_PREDICTION = {
    "INSUFFICIENT_CAUSAL_HISTORY",
    "MISSING_OR_AMBIGUOUS_MARKET_IDENTITY",
    "MISSING_SCHEDULE_OR_ROLL_STATE",
    "TRAINING_OR_PREDICTION_INELIGIBLE",
}
POST_PREDICTION = TERMINAL_DISPOSITIONS - PRE_PREDICTION - {"PREDICTION_PRODUCED"}


def _json_object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"cannot read {path.name}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{path.name} must contain one object")
    return value


def load_post_audit_contract(*, root: Path) -> dict[str, object]:
    payload = _json_object(root / CONTRACT_PATH)
    encoded = canonical_bytes(payload).decode("utf-8")
    risk = payload.get("risk")
    costs = payload.get("costs")
    inference = payload.get("inference")
    if (
        payload.get("schema_version") != "tier1_bracket_post_audit_successor/3.0.0"
        or payload.get("state") != "PREPARED_NOT_REGISTERED"
        or payload.get("supersedes_invalid_trial_id") != INVALID_TRIAL_ID
        or payload.get("classification") != "POST_AUDIT_NON_PRISTINE_HISTORICAL_SCREEN_ONLY"
        or "apex" in encoded.lower()
        or not isinstance(risk, dict)
        or risk.get("profile_id") != "RESEARCH_ACCOUNT_100K_V1"
        or Decimal(str(risk.get("starting_capital_usd"))) != Decimal("100000")
        or Decimal(str(risk.get("maximum_planned_initial_loss_usd"))) != Decimal("1000")
        or Decimal(str(risk.get("daily_loss_threshold_usd"))) != Decimal("1000")
        or Decimal(str(risk.get("continuous_drawdown_threshold_usd"))) != Decimal("5000")
        or not isinstance(costs, dict)
        or costs.get("label") != "PROVIDER_NEUTRAL_PROVISIONAL_RESEARCH_COSTS"
        or Decimal(str(costs.get("fee_per_side_usd"))) != Decimal("5.00")
        or not isinstance(inference, dict)
        or Decimal(str(inference.get("portfolio_mees_usd_per_complete_session"))) != Decimal("20")
        or inference.get("stationary_bootstrap_resamples") != 10000
        or inference.get("minimum_complete_clusters") != 30
        or payload.get("ordered_outcomes") != [
            "INVALID", "INCONCLUSIVE_DATA_OR_POWER", "FAIL_NO_EDGE",
            "FAIL_NOT_ECONOMIC", "INCONCLUSIVE_EFFECT",
            "FAIL_MULTIPLICITY_OR_CONTROL", "PASS_HISTORICAL_SCREEN",
        ]
    ):
        raise IntegrityError("post-audit successor contract is invalid")
    return payload


def load_invalid_closure_preparation(*, root: Path) -> dict[str, object]:
    payload = _json_object(root / CLOSURE_PATH)
    if (
        payload.get("schema_version") != "tier1_bracket_invalid_trial_closure_preparation/2.0.0"
        or payload.get("state") != "PREPARED_NOT_PUBLISHED_NOT_ACTIVE"
        or payload.get("disposition") != "INVALID_VOID_CAUSAL_TIMING_FILL_AND_COVERAGE_DEFECTS"
        or payload.get("trial_id") != INVALID_TRIAL_ID
        or payload.get("preserve_existing_artifacts_byte_for_byte") is not True
        or payload.get("publication_authorized") is not False
        or payload.get("activation_authorized") is not False
    ):
        raise IntegrityError("invalid-trial closure preparation is invalid")
    bindings = payload.get("preserved_bindings")
    if not isinstance(bindings, dict):
        raise IntegrityError("invalid-trial closure lacks preservation bindings")
    for path_key, hash_key in (
        ("trial_registry_path", "trial_registry_sha256"),
        ("evaluation_event_path", "evaluation_event_sha256"),
        ("evaluation_manifest_path", "evaluation_manifest_sha256"),
    ):
        relative = bindings.get(path_key)
        expected = bindings.get(hash_key)
        if not isinstance(relative, str) or expected != sha256_file(root / relative):
            raise IntegrityError("invalid-trial preserved artifact changed")
    return payload


@dataclass(frozen=True)
class PreparedPostAuditRegistration:
    trial_id: str
    canonical_payload: Mapping[str, object]
    confirmation_required: Mapping[str, object]


def prepare_post_audit_registration(*, root: Path) -> PreparedPostAuditRegistration:
    """Bind code/config metadata without opening a market row or writing a registry."""

    contract = load_post_audit_contract(root=root)
    closure = load_invalid_closure_preparation(root=root)
    implementation = Path("src/futures_rebuild/tier1_bracket_post_audit.py")
    historical_gates = Path("src/futures_rebuild/historical_capability.py")
    invalid_registry = _json_object(root / INVALID_TRIAL_REGISTRY)
    if invalid_registry.get("trial_id") != INVALID_TRIAL_ID:
        raise IntegrityError("post-audit successor does not bind the invalid trial")
    source_pairs = invalid_registry.get("source_pairs")
    if not isinstance(source_pairs, list) or len(source_pairs) != 20:
        raise IntegrityError("invalid trial lacks its 20 source metadata bindings")
    source_bindings = []
    for item in source_pairs:
        if not isinstance(item, dict):
            raise IntegrityError("invalid trial source binding is malformed")
        market, year, source_hash = (
            item.get("market"), item.get("year"), item.get("source_parquet_sha256")
        )
        if (
            market not in MARKETS
            or type(year) is not int
            or year not in range(2018, 2023)
            or not isinstance(source_hash, str)
            or len(source_hash) != 64
        ):
            raise IntegrityError("invalid trial source binding is outside scope")
        source_bindings.append(
            {"market": market, "year": year, "source_parquet_sha256": source_hash}
        )
    core: dict[str, object] = {
        "schema_version": "tier1_bracket_post_audit_registration/3.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": "POST_AUDIT_NON_PRISTINE_HISTORICAL_SCREEN_ONLY",
        "contract": contract,
        "bindings": {
            CONTRACT_PATH.as_posix(): sha256_file(root / CONTRACT_PATH),
            CLOSURE_PATH.as_posix(): sha256_file(root / CLOSURE_PATH),
            implementation.as_posix(): sha256_file(root / implementation),
            historical_gates.as_posix(): sha256_file(root / historical_gates),
            INVALID_TRIAL_REGISTRY.as_posix(): sha256_file(root / INVALID_TRIAL_REGISTRY),
            INVALID_EVALUATION_EVENT.as_posix(): sha256_file(root / INVALID_EVALUATION_EVENT),
            INVALID_EVALUATION_MANIFEST.as_posix(): sha256_file(
                root / INVALID_EVALUATION_MANIFEST
            ),
        },
        "prior_invalid_trial_id": closure["trial_id"],
        "source_bindings": sorted(
            source_bindings, key=lambda item: (str(item["market"]), int(item["year"]))
        ),
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "holdout_or_forward_access": False,
        "provider_access": False,
        "trading": False,
    }
    trial_id = sha256_json(core)
    return PreparedPostAuditRegistration(
        trial_id,
        core,
        {
            "operation": "publish one create-only post-audit trial declaration",
            "scope": (
                "metadata and hashes only; no historical rows, models, "
                "predictions, or evaluation"
            ),
            "cost": "$0 and no provider access",
            "preservation": (
                "all prior trial, evaluation, diagnosis, and closure bytes "
                "remain unchanged"
            ),
        },
    )


def _validate_prepared_registration(prepared: PreparedPostAuditRegistration) -> None:
    payload = prepared.canonical_payload
    if (
        prepared.trial_id != sha256_json(payload)
        or payload.get("schema_version")
        != "tier1_bracket_post_audit_registration/3.0.0"
        or payload.get("state") != "PREPARED_REQUIRES_PUBLICATION_APPROVAL"
        or payload.get("source_row_access") is not False
        or payload.get("holdout_or_forward_access") is not False
        or payload.get("provider_access") is not False
        or payload.get("trading") is not False
    ):
        raise IntegrityError("post-audit registration preparation is inconsistent")


def persist_post_audit_registration(
    *, root: Path, prepared: PreparedPostAuditRegistration,
) -> dict[str, str]:
    """Create the exact declaration and event; callers enforce external approval."""

    _validate_prepared_registration(prepared)
    registry = root / POST_AUDIT_REGISTRY_ROOT / f"{prepared.trial_id}.json"
    event = root / POST_AUDIT_EVENT_ROOT / f"{prepared.trial_id}.json"
    if registry.exists() or event.exists():
        raise IntegrityError("post-audit registration already exists")
    registry.parent.mkdir(parents=True, exist_ok=True)
    event.parent.mkdir(parents=True, exist_ok=True)
    registry_payload = {
        **dict(prepared.canonical_payload),
        "state": "REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
        "trial_id": prepared.trial_id,
    }
    event_payload = {
        "schema_version": "tier1_bracket_post_audit_event/3.0.0",
        "event_type": "DECLARED",
        "trial_id": prepared.trial_id,
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "holdout_or_forward_access": False,
    }
    try:
        with registry.open("xb") as stream:
            stream.write(canonical_bytes(registry_payload) + b"\n")
        with event.open("xb") as stream:
            stream.write(canonical_bytes(event_payload) + b"\n")
    except FileExistsError as exc:
        raise IntegrityError("post-audit registration raced another writer") from exc
    return {
        "trial_id": prepared.trial_id,
        "registry_path": registry.relative_to(root).as_posix(),
        "event_path": event.relative_to(root).as_posix(),
    }


def verify_post_audit_registration(
    *, root: Path, prepared: PreparedPostAuditRegistration,
) -> dict[str, str]:
    _validate_prepared_registration(prepared)
    registry = root / POST_AUDIT_REGISTRY_ROOT / f"{prepared.trial_id}.json"
    event = root / POST_AUDIT_EVENT_ROOT / f"{prepared.trial_id}.json"
    registry_payload = _json_object(registry)
    event_payload = _json_object(event)
    expected_registry = {
        **dict(prepared.canonical_payload),
        "state": "REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
        "trial_id": prepared.trial_id,
    }
    expected_event = {
        "schema_version": "tier1_bracket_post_audit_event/3.0.0",
        "event_type": "DECLARED",
        "trial_id": prepared.trial_id,
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "holdout_or_forward_access": False,
    }
    if registry_payload != expected_registry or event_payload != expected_event:
        raise IntegrityError("post-audit registration bytes are inconsistent")
    return {
        "trial_id": prepared.trial_id,
        "registry_sha256": sha256_file(registry),
        "event_sha256": sha256_file(event),
    }


def assert_allowed_research_year(year: int) -> None:
    """Reject holdout/forward references before a caller can construct a path."""

    if type(year) is not int or year not in range(2018, 2023):
        if year == 2025:
            raise UnauthorizedOperation("2025 holdout access is forbidden")
        raise IntegrityError("year is outside the registered discovery scope")


@dataclass(frozen=True)
class CausalBar:
    event_at_ns: int
    bar_end_at_ns: int
    available_at_ns: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    executable: bool = True

    def validate(self) -> None:
        prices = (self.open_price, self.high_price, self.low_price, self.close_price)
        if (
            any(
                type(value) is not int
                for value in (
                    self.event_at_ns,
                    self.bar_end_at_ns,
                    self.available_at_ns,
                )
            )
            or not self.event_at_ns < self.bar_end_at_ns <= self.available_at_ns
            or any(not value.is_finite() or value <= 0 for value in prices)
            or self.low_price > min(self.open_price, self.close_price)
            or self.high_price < max(self.open_price, self.close_price)
            or self.low_price > self.high_price
        ):
            raise IntegrityError("causal bar is invalid")


def latest_causal_feature_bar(*, bars: Sequence[CausalBar], decision_at_ns: int) -> CausalBar:
    if type(decision_at_ns) is not int:
        raise IntegrityError("decision time is invalid")
    for bar in bars:
        bar.validate()
    eligible = [
        bar for bar in bars
        if bar.bar_end_at_ns <= decision_at_ns and bar.available_at_ns <= decision_at_ns
    ]
    if not eligible:
        raise IntegrityError("no completed feature bar was available by decision")
    return max(
        eligible,
        key=lambda item: (
            item.available_at_ns,
            item.bar_end_at_ns,
            item.event_at_ns,
        ),
    )


def first_causal_entry_bar(
    *, bars: Sequence[CausalBar], decision_at_ns: int, one_bar_delay_ns: int,
) -> CausalBar:
    if type(one_bar_delay_ns) is not int or one_bar_delay_ns <= 0:
        raise IntegrityError("entry delay is invalid")
    earliest = decision_at_ns + one_bar_delay_ns
    for bar in bars:
        bar.validate()
    eligible = [bar for bar in bars if bar.event_at_ns >= earliest and bar.executable]
    if not eligible:
        raise IntegrityError("no causal executable entry exists")
    selected = min(eligible, key=lambda item: item.event_at_ns)
    if not decision_at_ns < selected.event_at_ns:
        raise IntegrityError("entry does not follow decision")
    return selected


@dataclass(frozen=True)
class OpportunityRecord:
    opportunity_id: str
    market: str
    exchange_session_date: str
    checkpoint: str
    decision_at_ns: int
    terminal_disposition: str
    prediction_produced: bool
    feature_event_at_ns: int | None = None
    feature_available_at_ns: int | None = None
    order_submitted_at_ns: int | None = None
    fill_at_ns: int | None = None
    outcome_coverage: str = "NOT_APPLICABLE"

    def validate(self) -> None:
        if (
            not self.opportunity_id
            or self.market not in MARKETS
            or self.checkpoint not in CHECKPOINTS
            or not self.exchange_session_date
            or type(self.decision_at_ns) is not int
            or self.terminal_disposition not in TERMINAL_DISPOSITIONS
            or type(self.prediction_produced) is not bool
            or (self.terminal_disposition in PRE_PREDICTION and self.prediction_produced)
            or (self.terminal_disposition in POST_PREDICTION and not self.prediction_produced)
            or (self.terminal_disposition == "PREDICTION_PRODUCED" and not self.prediction_produced)
        ):
            raise IntegrityError("opportunity record is inconsistent")
        if self.prediction_produced and (
            type(self.feature_event_at_ns) is not int
            or type(self.feature_available_at_ns) is not int
            or self.feature_event_at_ns > self.feature_available_at_ns
            or self.feature_available_at_ns > self.decision_at_ns
        ):
            raise IntegrityError("prediction lacks a causal feature timestamp")
        admitted = self.terminal_disposition == "ADMITTED_TRADE"
        if admitted and (
            type(self.order_submitted_at_ns) is not int
            or type(self.fill_at_ns) is not int
            or not self.decision_at_ns
            < self.order_submitted_at_ns
            <= self.fill_at_ns
            or self.outcome_coverage != "COMPLETE"
        ):
            raise IntegrityError("admitted trade lacks causal order/fill coverage")


def causality_certificate(records: Sequence[OpportunityRecord]) -> dict[str, object]:
    for record in records:
        record.validate()
    predicted = [record for record in records if record.prediction_produced]
    admitted = [record for record in records if record.terminal_disposition == "ADMITTED_TRADE"]
    core: dict[str, object] = {
        "schema_version": "tier1_bracket_causality_certificate/1.0.0",
        "record_count": len(records),
        "prediction_count": len(predicted),
        "admitted_trade_count": len(admitted),
        "all_features_available_by_decision": True,
        "all_orders_and_fills_after_decision": True,
        "opportunity_ids": sorted(record.opportunity_id for record in records),
    }
    return {**core, "certificate_id": sha256_json(core)}


def reconcile_opportunity_ledger(
    *, expected_ids: Sequence[str], records: Sequence[OpportunityRecord],
) -> dict[str, int]:
    if len(set(expected_ids)) != len(expected_ids) or any(not item for item in expected_ids):
        raise IntegrityError("expected opportunity IDs are invalid")
    for record in records:
        record.validate()
    record_ids = [record.opportunity_id for record in records]
    if len(set(record_ids)) != len(record_ids) or set(record_ids) != set(expected_ids):
        raise IntegrityError("opportunity ledger does not exactly match its expected universe")
    predictions = sum(record.prediction_produced for record in records)
    pre = sum(record.terminal_disposition in PRE_PREDICTION for record in records)
    if len(records) != predictions + pre:
        raise IntegrityError("prediction/pre-prediction funnel does not reconcile")
    post = sum(record.terminal_disposition in POST_PREDICTION for record in records)
    unresolved = sum(record.terminal_disposition == "PREDICTION_PRODUCED" for record in records)
    if predictions != post + unresolved:
        raise IntegrityError("post-prediction funnel does not reconcile")
    return {
        "expected_opportunities": len(expected_ids),
        "predictions": predictions,
        "pre_prediction_abstentions": pre,
        "post_prediction_terminal_rows": post,
        "predictions_awaiting_terminal_decision": unresolved,
    }


def cost_ticks(*, contract: Mapping[str, object], scenario: str, market: str) -> int:
    costs = contract.get("costs")
    grid = costs.get("round_trip_adverse_execution_ticks") if isinstance(costs, dict) else None
    scenario_grid = grid.get(scenario) if isinstance(grid, dict) else None
    ticks = scenario_grid.get(market) if isinstance(scenario_grid, dict) else None
    if type(ticks) is not int or ticks < 0:
        raise IntegrityError("cost scenario is incomplete or invalid")
    return ticks


def round_up_ticks(*, distance: Decimal, tick_size: Decimal) -> int:
    if not distance.is_finite() or not tick_size.is_finite() or distance <= 0 or tick_size <= 0:
        raise IntegrityError("tick rounding inputs are invalid")
    return int((distance / tick_size).to_integral_value(rounding=ROUND_CEILING))


def planned_initial_loss_usd(
    *, atr: Decimal, tick_size: Decimal, tick_value: Decimal,
    round_trip_cost_ticks: int, fee_per_side_usd: Decimal,
) -> Decimal:
    stop_ticks = round_up_ticks(distance=Decimal("1.5") * atr, tick_size=tick_size)
    return (
        Decimal(stop_ticks + round_trip_cost_ticks) * tick_value
        + Decimal("2") * fee_per_side_usd
    )


@dataclass(frozen=True)
class BracketFill:
    entry_at_ns: int
    exit_at_ns: int
    entry_price: Decimal
    exit_price: Decimal
    stop_price: Decimal
    target_price: Decimal
    reason: str
    gross_pnl_usd: Decimal
    costs_usd: Decimal
    net_pnl_usd: Decimal
    planned_initial_loss_usd: Decimal


def simulate_bracket_fill(
    *, direction: str, decision_at_ns: int, entry_bar: CausalBar,
    path_bars: Sequence[CausalBar], atr: Decimal, tick_size: Decimal,
    tick_value: Decimal, point_value: Decimal, fee_per_side_usd: Decimal,
    round_trip_cost_ticks: int, maximum_planned_loss_usd: Decimal = Decimal("1000"),
    maximum_hold_ns: int = 3_600_000_000_000,
    session_liquidation_at_ns: int | None = None,
) -> BracketFill:
    if (
        direction not in {"long", "short"}
        or entry_bar.event_at_ns <= decision_at_ns
        or type(maximum_hold_ns) is not int
        or maximum_hold_ns <= 0
    ):
        raise IntegrityError("bracket direction or entry timing is invalid")
    entry_bar.validate()
    for bar in path_bars:
        bar.validate()
    if any(bar.event_at_ns < entry_bar.event_at_ns for bar in path_bars):
        raise IntegrityError("outcome path begins before entry")
    planned = planned_initial_loss_usd(
        atr=atr, tick_size=tick_size, tick_value=tick_value,
        round_trip_cost_ticks=round_trip_cost_ticks,
        fee_per_side_usd=fee_per_side_usd,
    )
    if planned > maximum_planned_loss_usd:
        raise IntegrityError("planned initial loss exceeds the research cap")
    half_ticks = Decimal(round_trip_cost_ticks) / Decimal("2")
    sign = Decimal("1") if direction == "long" else Decimal("-1")
    entry = entry_bar.open_price + sign * half_ticks * tick_size
    stop_ticks = round_up_ticks(distance=Decimal("1.5") * atr, tick_size=tick_size)
    stop = entry - sign * Decimal(stop_ticks) * tick_size
    # Net target is at least 2R after the full declared fee and execution allowance.
    gross_target_usd = Decimal("2") * planned + Decimal("2") * fee_per_side_usd
    target_ticks = int((gross_target_usd / tick_value).to_integral_value(rounding=ROUND_CEILING))
    target = entry + sign * Decimal(target_ticks) * tick_size
    exit_price: Decimal | None = None
    exit_at = 0
    reason = ""
    timeout_at = entry_bar.event_at_ns + maximum_hold_ns
    liquidation_at = (
        min(timeout_at, session_liquidation_at_ns)
        if isinstance(session_liquidation_at_ns, int)
        else timeout_at
    )
    for bar in sorted(path_bars, key=lambda item: item.event_at_ns):
        if not bar.executable:
            continue
        if bar.event_at_ns >= liquidation_at:
            exit_price = bar.open_price - sign * half_ticks * tick_size
            reason = (
                "SESSION_LIQUIDATION"
                if isinstance(session_liquidation_at_ns, int)
                and session_liquidation_at_ns <= timeout_at
                else "TIMEOUT"
            )
            exit_at = bar.event_at_ns
            break
        if direction == "long":
            if bar.open_price <= stop:
                exit_price, reason = bar.open_price - half_ticks * tick_size, "STOP_GAP"
            elif bar.low_price <= stop:
                exit_price, reason = stop - half_ticks * tick_size, "STOP"
            elif bar.high_price >= target:
                exit_price, reason = target - half_ticks * tick_size, "TARGET"
        else:
            if bar.open_price >= stop:
                exit_price, reason = bar.open_price + half_ticks * tick_size, "STOP_GAP"
            elif bar.high_price >= stop:
                exit_price, reason = stop + half_ticks * tick_size, "STOP"
            elif bar.low_price <= target:
                exit_price, reason = target + half_ticks * tick_size, "TARGET"
        if exit_price is not None:
            exit_at = bar.event_at_ns
            break
    if exit_price is None:
        raise IntegrityError("outcome path ends before a causal executable exit")
    fill_to_fill = sign * (exit_price - entry) * point_value
    fees = Decimal("2") * fee_per_side_usd
    net = fill_to_fill - fees
    total_costs = fees + Decimal(round_trip_cost_ticks) * tick_value
    # Slippage is embedded in the adverse fill prices.  Reconstruct the
    # comparable zero-cost gross so gross - declared costs == net exactly.
    gross = net + total_costs
    return BracketFill(
        entry_bar.event_at_ns, exit_at, entry, exit_price, stop, target,
        reason, gross, total_costs, net, planned,
    )


@dataclass(frozen=True)
class ResearchRiskAccount:
    realized_pnl_usd: Decimal = Decimal("0")
    open_unrealized_pnl_usd: Decimal = Decimal("0")
    peak_equity_usd: Decimal = Decimal("100000")
    session_start_equity_usd: Decimal = Decimal("100000")
    entry_blocked_for_session: bool = False
    permanently_halted: bool = False
    liquidation_required: bool = False

    @property
    def equity_usd(self) -> Decimal:
        return Decimal("100000") + self.realized_pnl_usd + self.open_unrealized_pnl_usd

    def mark(self, *, unrealized_pnl_usd: Decimal) -> "ResearchRiskAccount":
        if not unrealized_pnl_usd.is_finite():
            raise IntegrityError("risk mark is invalid")
        marked = replace(self, open_unrealized_pnl_usd=unrealized_pnl_usd)
        peak = max(marked.peak_equity_usd, marked.equity_usd)
        daily_loss = marked.session_start_equity_usd - marked.equity_usd
        drawdown = peak - marked.equity_usd
        return replace(
            marked,
            peak_equity_usd=peak,
            entry_blocked_for_session=(
                marked.entry_blocked_for_session or daily_loss >= Decimal("1000")
            ),
            permanently_halted=marked.permanently_halted or drawdown >= Decimal("5000"),
            liquidation_required=(
                marked.open_unrealized_pnl_usd != 0
                and (daily_loss >= Decimal("1000") or drawdown >= Decimal("5000"))
            ),
        )

    def close(self, *, net_pnl_usd: Decimal) -> "ResearchRiskAccount":
        if not net_pnl_usd.is_finite():
            raise IntegrityError("closed P&L is invalid")
        closed = replace(
            self,
            realized_pnl_usd=self.realized_pnl_usd + net_pnl_usd,
            open_unrealized_pnl_usd=Decimal("0"),
            liquidation_required=False,
        )
        return closed.mark(unrealized_pnl_usd=Decimal("0"))

    def reset_session(self) -> "ResearchRiskAccount":
        if self.open_unrealized_pnl_usd != 0:
            raise IntegrityError("cannot reset a session with an open position")
        return replace(
            self,
            session_start_equity_usd=self.equity_usd,
            entry_blocked_for_session=False,
        )


@dataclass(frozen=True)
class SessionObservation:
    session_id: str
    net_pnl_usd: Decimal | None
    complete: bool
    absolute_position_changes: Decimal = Decimal("0")


@dataclass(frozen=True)
class BaselineRun:
    baseline_id: str
    signal_state_id: str
    scheduler_state_id: str
    opportunity_ids: tuple[str, ...]
    fills: tuple[BracketFill, ...] = ()


def validate_independent_baselines(
    *, expected_opportunity_ids: Sequence[str], runs: Sequence[BaselineRun],
) -> None:
    required = {
        "flat_no_trade",
        "fold_local_unconditional_return_by_market_session",
        "previous_bar_sign_momentum",
        "previous_bar_sign_reversal",
        "risk_matched_always_long_intraday",
        "equal_risk_version_of_candidate_signal",
    }
    if {run.baseline_id for run in runs} != required:
        raise IntegrityError("required independent baselines are incomplete")
    if len({run.signal_state_id for run in runs}) != len(runs) or len(
        {run.scheduler_state_id for run in runs}
    ) != len(runs):
        raise IntegrityError("baselines reused signal or scheduling state")
    expected = tuple(sorted(expected_opportunity_ids))
    for run in runs:
        if tuple(sorted(run.opportunity_ids)) != expected:
            raise IntegrityError("baseline does not own the complete opportunity universe")
        if run.baseline_id == "flat_no_trade" and run.fills:
            raise IntegrityError("flat baseline must not trade")


def account_metrics(*, sessions: Sequence[SessionObservation]) -> dict[str, object]:
    if not sessions or len({item.session_id for item in sessions}) != len(sessions):
        raise IntegrityError("session observations are empty or duplicated")
    if any(
        (item.complete and item.net_pnl_usd is None)
        or (not item.complete and item.net_pnl_usd is not None)
        for item in sessions
    ):
        raise IntegrityError("complete and missing sessions are inconsistent")
    complete = [item for item in sessions if item.complete]
    if not complete:
        raise IntegrityError("no complete sessions exist")
    pnls = [item.net_pnl_usd for item in complete]
    assert all(item is not None for item in pnls)
    values = [item for item in pnls if item is not None]
    returns = [float(item / Decimal("100000")) for item in values]
    daily_mean = mean(returns)
    daily_std = stdev(returns) if len(returns) >= 2 else 0.0
    downside = math.sqrt(mean([min(item, 0.0) ** 2 for item in returns]))
    sharpe = None if daily_std == 0.0 else math.sqrt(252.0) * daily_mean / daily_std
    sortino = None if downside == 0.0 else math.sqrt(252.0) * daily_mean / downside
    equity = peak = Decimal("0")
    drawdown = Decimal("0")
    for pnl in values:
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return {
        "complete_sessions": len(complete),
        "incomplete_sessions": len(sessions) - len(complete),
        "net_pnl_usd": str(sum(values, Decimal("0"))),
        "annualized_daily_sharpe": sharpe,
        "annualized_daily_sortino": sortino,
        "maximum_drawdown_usd": str(drawdown),
        "turnover_contract_equivalents": str(
            sum(
                (item.absolute_position_changes for item in complete),
                Decimal("0"),
            )
        ),
    }


@dataclass(frozen=True)
class GateEvidence:
    invalid: bool = False
    complete_clusters: int = 0
    power: Decimal = Decimal("0")
    every_required_sleeve_powered: bool = False
    confidence_lower_usd: Decimal = Decimal("0")
    confidence_upper_usd: Decimal = Decimal("0")
    mees_usd: Decimal = Decimal("20")
    dsr_probability: Decimal = Decimal("0")
    romano_wolf_passed: bool = False
    controls_passed: bool = False
    stress_and_baselines_passed: bool = False
    distribution_gate_passed: bool = False
    drawdown_gate_passed: bool = False


def classify_historical_screen(evidence: GateEvidence) -> str:
    if evidence.invalid:
        return "INVALID"
    if (
        evidence.complete_clusters < 30
        or evidence.power < Decimal("0.80")
        or not evidence.every_required_sleeve_powered
    ):
        return "INCONCLUSIVE_DATA_OR_POWER"
    if evidence.confidence_upper_usd <= 0:
        return "FAIL_NO_EDGE"
    if evidence.confidence_upper_usd <= evidence.mees_usd:
        return "FAIL_NOT_ECONOMIC"
    if evidence.confidence_lower_usd <= evidence.mees_usd:
        return "INCONCLUSIVE_EFFECT"
    if not (
        evidence.dsr_probability >= Decimal("0.95")
        and evidence.romano_wolf_passed
        and evidence.controls_passed
        and evidence.stress_and_baselines_passed
        and evidence.distribution_gate_passed
        and evidence.drawdown_gate_passed
    ):
        return "FAIL_MULTIPLICITY_OR_CONTROL"
    return "PASS_HISTORICAL_SCREEN"
