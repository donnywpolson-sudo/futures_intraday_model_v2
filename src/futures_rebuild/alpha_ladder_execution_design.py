"""Pure validation for the pre-result Alpha-ladder execution design."""

from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


CONTRACT_PATH = Path("configs/alpha_ladder_execution_design_contract_v1.json")
SCHEMA_VERSION = "alpha_ladder_execution_design_contract/1.0.0"
MACRO_CANDIDATES = ("ZN", "6E")
ELIGIBLE = "EXECUTION_ELIGIBLE"
INELIGIBLE = "EXECUTION_INELIGIBLE"
UNKNOWN = "UNKNOWN_FAIL_CLOSED"
NO_ELIGIBLE = "NO_ELIGIBLE_MACRO_DIVERSIFIER"
EVIDENCE_FIELDS = (
    "target_horizon_movement_ticks", "conservative_round_trip_friction_ticks",
    "movement_to_cost_ratio", "active_minute_coverage",
    "zero_volume_minute_fraction", "missingness_and_continuity", "roll_behavior",
)
FORBIDDEN_RESULT_TOKENS = (
    "alpha", "backtest", "feature", "forward", "holdout", "model", "pnl",
    "prediction", "profit", "return", "sharpe", "strategy", "target_hit",
    "tier_result", "wfa",
)


def _decimal(value: object, label: str) -> Decimal:
    if isinstance(value, bool):
        raise IntegrityError(f"{label} must be numeric")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError(f"{label} must be numeric") from exc
    if not number.is_finite():
        raise IntegrityError(f"{label} must be finite")
    return number


def _contains_forbidden_result_key(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).strip().lower().replace("-", "_").replace(" ", "_")
            if any(token in normalized for token in FORBIDDEN_RESULT_TOKENS):
                return True
            if _contains_forbidden_result_key(item):
                return True
    elif isinstance(value, (list, tuple)):
        return any(_contains_forbidden_result_key(item) for item in value)
    return False


def validate_execution_design_contract(payload: Mapping[str, object]) -> dict[str, object]:
    """Validate the immutable design without reading result-bearing data."""

    core = dict(payload)
    contract_id = core.pop("contract_id", None)
    if contract_id != sha256_json(core):
        raise IntegrityError("execution-design contract identity mismatch")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise IntegrityError("execution-design schema version mismatch")
    state = payload.get("state")
    if not isinstance(state, Mapping) or state != {
        "activation": "SEPARATE_APPROVAL_REQUIRED",
        "macro_selection": "PENDING_PRE_RESULT_EXECUTION_GATE",
        "mechanism": "NOT_STARTED",
        "row_read_authorization": "NOT_ISSUED",
        "status": "PREPARED_NOT_ACTIVE_APPROVAL_REQUIRED",
    }:
        raise UnauthorizedOperation("execution design is not preparation-only")
    authority = payload.get("authority")
    if not isinstance(authority, Mapping) or not authority or any(authority.values()):
        raise UnauthorizedOperation("prepared execution design grants authority")
    selector = payload.get("macro_selector")
    if not isinstance(selector, Mapping) or tuple(selector.get("candidates", ())) != MACRO_CANDIDATES:
        raise IntegrityError("macro candidates must be exactly ZN and 6E")
    if selector.get("result_evidence_forbidden") is not True:
        raise UnauthorizedOperation("selector must forbid result evidence")
    if selector.get("both_eligible_tie_breaker") != {
        "exact_tie": "FROZEN_CANDIDATE_ORDER_ZN_THEN_6E",
        "id": "MAX_MOVEMENT_TO_COST_RATIO_THEN_ZN_6E_ORDER",
        "primary": "HIGHEST_MOVEMENT_TO_COST_RATIO",
    }:
        raise IntegrityError("macro tie-breaker is not exactly frozen")
    gates = payload.get("execution_proxy_gates")
    if not isinstance(gates, Mapping) or tuple(gates.get("evidence_fields", ())) != EVIDENCE_FIELDS:
        raise IntegrityError("execution-proxy evidence fields differ from the frozen set")
    if _contains_forbidden_result_key(gates.get("evidence_fields")):
        raise UnauthorizedOperation("execution-proxy gates name result-bearing evidence")
    movement = gates.get("movement_proxy_anchor_contract")
    if not isinstance(movement, Mapping) or movement != {
        "aggregation": "UNWEIGHTED_MEDIAN_ACROSS_ALL_COMPLETE_ELIGIBLE_ENTRY_ANCHORS_IN_THE_BOUND_DEVELOPMENT_PERIOD",
        "direction_or_strategy_signal": "NONE",
        "entry_anchor": "OPEN_OF_THE_FIRST_COMPLETE_ONE_MINUTE_BAR_STARTING_STRICTLY_AFTER_THE_DECISION_TIMESTAMP",
        "exit_anchor": "OPEN_OF_THE_COMPLETE_ONE_MINUTE_BAR_STARTING_EXACTLY_30_CLOCK_MINUTES_AFTER_ENTRY",
        "missing_entry_exit_or_tick_metadata": "ANCHOR_UNUSABLE_AND_COUNTED_IN_COVERAGE_OR_QUALITY_GATES_NEVER_IMPUTED",
        "price_change": "ABSOLUTE_EXIT_OPEN_MINUS_ENTRY_OPEN",
        "result_metrics_used": False,
        "tick_conversion": "DIVIDE_BY_POINT_IN_TIME_TICK_SIZE_BOUND_TO_THE_SAME_ACTUAL_CONTRACT_IDENTITY",
        "tie_break_dimension": "HIGHEST_MOVEMENT_TO_COST_RATIO",
    }:
        raise IntegrityError("typical-movement proxy is not exactly frozen")
    row_authorization = payload.get("future_row_read_authorization")
    if not isinstance(row_authorization, Mapping):
        raise IntegrityError("future row-read authorization template is missing")
    if (
        row_authorization.get("status") != "NOT_ISSUED"
        or row_authorization.get("authorization_id") is not None
        or row_authorization.get("maximum_uses") != 1
        or row_authorization.get("uses_consumed") != 0
        or tuple(row_authorization.get("markets", ())) != MACRO_CANDIDATES
    ):
        raise UnauthorizedOperation("row-read authorization must remain unissued and ZN/6E-only")
    if _contains_forbidden_result_key(row_authorization.get("allowed_fields", ())):
        raise UnauthorizedOperation("row-read template permits result-bearing fields")
    horizon = payload.get("horizon")
    decision = payload.get("decision_timestamp")
    session = payload.get("session_policy")
    position = payload.get("position_size")
    costs = payload.get("cost_model")
    if not all(isinstance(item, Mapping) for item in (horizon, decision, session, position, costs)):
        raise IntegrityError("one or more frozen design sections are missing")
    if horizon.get("holding_horizon_minutes") != 30 or horizon.get("maximum_holding_minutes") != 30:
        raise IntegrityError("first mechanism must use the frozen 30-minute clock horizon")
    if horizon.get("overnight_holding_allowed") is not False:
        raise UnauthorizedOperation("overnight holding is forbidden")
    if decision.get("decision_timestamp") != "bar_start + 65 seconds":
        raise IntegrityError("decision timestamp differs from causal one-minute authority")
    if (
        session.get("timezone") != "America/Chicago"
        or session.get("flat_by_local") != "15:00:00"
        or session.get("feature_bar_start_window_local")
        != "DISCRETE_MINUTE_STARTS_08:34:00_THROUGH_14:23:00_INCLUSIVE"
        or session.get("decision_window_local")
        != "DISCRETE_TIMESTAMPS_08:35:05_THROUGH_14:24:05_INCLUSIVE"
        or session.get("entry_bar_start_window_local")
        != "DISCRETE_MINUTE_STARTS_08:36:00_THROUGH_14:25:00_INCLUSIVE"
    ):
        raise IntegrityError("session policy differs from the frozen development window")
    if position.get("instrument") != "ONE_BOUND_STANDARD_CONTRACT" or position.get("optimization") is not False:
        raise UnauthorizedOperation("position sizing is not the frozen one-contract policy")
    if costs.get("fee_round_trip_usd") != "10.00" or costs.get("may_be_reduced_after_result_access") is not False:
        raise IntegrityError("cost policy differs from the frozen conservative model")
    return dict(payload)


def load_execution_design_contract(root: Path) -> dict[str, object]:
    """Load canonical contract bytes and authenticate every reused authority."""

    path = root / CONTRACT_PATH
    raw = path.read_bytes()
    payload = json.loads(raw)
    if raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError("execution-design contract is not canonically encoded")
    validated = validate_execution_design_contract(payload)
    reused = validated["reused_surfaces"]
    assert isinstance(reused, Mapping)
    for binding in reused.values():
        assert isinstance(binding, Mapping)
        for key, expected in binding.items():
            if key == "path":
                if sha256_file(root / str(expected), reject_hardlinks=False) != binding.get("sha256"):
                    raise IntegrityError(f"reused surface hash mismatch: {expected}")
            elif key.endswith("_path"):
                prefix = key[:-5]
                if sha256_file(root / str(expected), reject_hardlinks=False) != binding.get(f"{prefix}_sha256"):
                    raise IntegrityError(f"reused surface hash mismatch: {expected}")
    future = validated["future_row_read_authorization"]
    assert isinstance(future, Mapping)
    for name in ("canonical_source", "causal_observation"):
        binding = future[name]
        assert isinstance(binding, Mapping)
        if sha256_file(root / str(binding["path"]), reject_hardlinks=False) != binding["sha256"]:
            raise IntegrityError(f"{name} authority hash mismatch")
    return validated


def evaluate_execution_proxy(
    contract: Mapping[str, object], market: str, evidence: Mapping[str, object]
) -> dict[str, object]:
    """Evaluate one market using execution evidence only; never infer missing data."""

    validate_execution_design_contract(contract)
    if market not in MACRO_CANDIDATES:
        raise UnauthorizedOperation("execution proxy may evaluate only ZN or 6E")
    if _contains_forbidden_result_key(evidence):
        raise UnauthorizedOperation("result-bearing evidence cannot enter the selector")
    if set(evidence) - set(EVIDENCE_FIELDS):
        raise UnauthorizedOperation("selector evidence contains an unapproved field")
    if any(field not in evidence or evidence[field] is None for field in EVIDENCE_FIELDS):
        return {"market": market, "status": UNKNOWN, "failed_gates": ["MISSING_EVIDENCE"]}
    coverage = evidence["active_minute_coverage"]
    continuity = evidence["missingness_and_continuity"]
    roll = evidence["roll_behavior"]
    friction = evidence["conservative_round_trip_friction_ticks"]
    if not all(isinstance(item, Mapping) for item in (coverage, continuity, roll, friction)):
        return {"market": market, "status": UNKNOWN, "failed_gates": ["MALFORMED_EVIDENCE"]}
    thresholds = contract["execution_proxy_gates"]["thresholds"]  # type: ignore[index]
    adverse = contract["cost_model"]["adverse_ticks_round_trip"][market]  # type: ignore[index]
    failed: list[str] = []
    try:
        movement = _decimal(evidence["target_horizon_movement_ticks"], "movement ticks")
        friction_value = _decimal(friction.get("value"), "friction ticks")
        tick_value = _decimal(friction.get("tick_value_usd"), "tick value")
        declared_ratio = _decimal(evidence["movement_to_cost_ratio"], "movement/cost ratio")
        calculated_friction = Decimal(adverse) + Decimal("10.00") / tick_value
        calculated_ratio = movement / friction_value
        if friction_value <= 0 or tick_value <= 0:
            raise IntegrityError("friction and tick value must be positive")
        if abs(friction_value - calculated_friction) > Decimal("0.000001"):
            raise IntegrityError("friction evidence does not match the frozen cost model")
        if abs(declared_ratio - calculated_ratio) > Decimal("0.000001"):
            raise IntegrityError("movement-to-cost ratio is inconsistent")
        checks = (
            (int(coverage.get("usable_sessions", -1)) >= thresholds["minimum_usable_sessions"], "USABLE_SESSIONS"),
            (int(coverage.get("usable_observations", -1)) >= thresholds["minimum_usable_observations"], "USABLE_OBSERVATIONS"),
            (_decimal(coverage.get("ratio"), "coverage ratio") >= _decimal(thresholds["active_minute_coverage_ratio_minimum"], "coverage threshold"), "ACTIVE_MINUTE_COVERAGE"),
            (_decimal(evidence["zero_volume_minute_fraction"], "zero-volume fraction") <= _decimal(thresholds["zero_volume_minute_fraction_maximum"], "zero-volume threshold"), "ZERO_VOLUME"),
            (_decimal(continuity.get("unexpected_gap_or_stale_fraction"), "gap fraction") <= _decimal(thresholds["unexpected_gap_or_stale_fraction_maximum"], "gap threshold"), "GAP_OR_STALENESS"),
            (_decimal(continuity.get("point_in_time_metadata_coverage_ratio"), "metadata coverage") == _decimal(thresholds["metadata_coverage_ratio_required"], "metadata threshold"), "METADATA_COVERAGE"),
            (int(continuity.get("quality_error_count", -1)) <= thresholds["quality_error_count_maximum"], "QUALITY_ERRORS"),
            (int(roll.get("identity_violation_count", -1)) <= thresholds["roll_identity_violation_count_maximum"], "ROLL_IDENTITY"),
            (declared_ratio >= _decimal(thresholds["minimum_movement_to_cost_ratio"], "ratio threshold"), "MOVEMENT_TO_COST"),
        )
    except (IntegrityError, TypeError, ValueError, ZeroDivisionError):
        return {"market": market, "status": UNKNOWN, "failed_gates": ["MALFORMED_EVIDENCE"]}
    failed.extend(label for passed, label in checks if not passed)
    return {"market": market, "status": INELIGIBLE if failed else ELIGIBLE, "failed_gates": failed, "movement_to_cost_ratio": str(declared_ratio)}


def select_macro_execution_candidate(
    contract: Mapping[str, object], evidence_by_market: Mapping[str, Mapping[str, object]]
) -> dict[str, object]:
    """Apply the frozen ZN/6E rule without publishing a selection."""

    if tuple(evidence_by_market) != MACRO_CANDIDATES:
        raise IntegrityError("selector input must be ordered exactly ZN then 6E")
    results = {market: evaluate_execution_proxy(contract, market, evidence_by_market[market]) for market in MACRO_CANDIDATES}
    eligible = [market for market in MACRO_CANDIDATES if results[market]["status"] == ELIGIBLE]
    if len(eligible) == 1:
        outcome, selected = "SELECT_THE_ONLY_EXECUTION_ELIGIBLE_CANDIDATE", eligible[0]
    elif len(eligible) == 2:
        selected = max(eligible, key=lambda market: (Decimal(str(results[market]["movement_to_cost_ratio"])), -MACRO_CANDIDATES.index(market)))
        outcome = "SELECT_BY_FROZEN_TIE_BREAKER"
    elif all(results[market]["status"] == INELIGIBLE for market in MACRO_CANDIDATES):
        outcome, selected = NO_ELIGIBLE, None
    else:
        outcome, selected = UNKNOWN, None
    return {"outcome": outcome, "selected_market": selected, "candidate_results": results}
