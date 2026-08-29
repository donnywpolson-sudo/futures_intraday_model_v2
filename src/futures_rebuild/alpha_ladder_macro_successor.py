"""Bias-preserving five-candidate macro selector successor."""

from __future__ import annotations

import json
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


CLOSURE_PATH = Path("state/alpha_ladder_macro_selection_closure/zn_6e_terminal_v1.json")
CONTRACT_PATH = Path("configs/alpha_ladder_macro_successor_contract_v1.json")
CANDIDATES = ("SR3", "ZN", "ZB", "6E", "6J")
CARRIED_CANDIDATES = ("ZN", "6E")
NEW_CANDIDATES = ("SR3", "ZB", "6J")
ELIGIBLE = "EXECUTION_ELIGIBLE"
INELIGIBLE = "EXECUTION_INELIGIBLE"
UNKNOWN = "UNKNOWN_FAIL_CLOSED"
NO_ELIGIBLE = "NO_ELIGIBLE_MACRO_DIVERSIFIER"
EVIDENCE_FIELDS = (
    "target_horizon_movement_ticks",
    "conservative_round_trip_friction_ticks",
    "movement_to_cost_ratio",
    "active_minute_coverage",
    "zero_volume_minute_fraction",
    "missingness_and_continuity",
    "roll_behavior",
)
FORBIDDEN_RESULT_TOKENS = (
    "alpha", "feature", "hit_rate", "holdout", "pnl", "prediction",
    "profit", "sharpe", "tier_result", "wfa",
)


def _read_canonical(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"canonical input is unreadable: {path}") from exc
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"input is not canonical JSON: {path}")
    return value


def _relative(root: Path, value: object) -> Path:
    if type(value) is not str or not value:
        raise IntegrityError("bound path is absent")
    relative = PurePosixPath(value)
    if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != value:
        raise IntegrityError("bound path is not repository-relative")
    path = root.joinpath(*relative.parts).resolve(strict=False)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise UnauthorizedOperation("bound path escapes repository") from exc
    return path


def load_predecessor_closure(repository_root: Path) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    closure = _read_canonical(root / CLOSURE_PATH)
    core = {key: value for key, value in closure.items() if key != "closure_id"}
    bindings = closure.get("artifact_bindings")
    if (
        closure.get("schema_version") != "alpha_ladder_macro_predecessor_closure/1.0.0"
        or closure.get("state") != "CLOSED_NO_ELIGIBLE_MACRO_DIVERSIFIER"
        or closure.get("outcome") != NO_ELIGIBLE
        or closure.get("selected_market") is not None
        or closure.get("retry_allowed") is not False
        or closure.get("closure_id") != sha256_json(core)
        or not isinstance(bindings, list)
        or not bindings
    ):
        raise IntegrityError("predecessor closure is invalid")
    for binding in bindings:
        if not isinstance(binding, Mapping):
            raise IntegrityError("predecessor binding is invalid")
        path = _relative(root, binding.get("path"))
        if not path.is_file() or sha256_file(path, reject_hardlinks=False) != binding.get("sha256"):
            raise IntegrityError(f"predecessor binding drifted: {binding.get('path')}")
    return closure


def load_successor_contract(repository_root: Path) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    contract = _read_canonical(root / CONTRACT_PATH)
    core = {key: value for key, value in contract.items() if key != "contract_id"}
    closure = load_predecessor_closure(root)
    closure_binding = contract.get("predecessor_closure")
    if (
        contract.get("schema_version") != "alpha_ladder_macro_successor_contract/1.0.0"
        or contract.get("state") != "FROZEN_PRE_RESULT_PREPARATION_ONLY"
        or contract.get("contract_id") != sha256_json(core)
        or contract.get("candidate_order") != list(CANDIDATES)
        or contract.get("carried_candidates") != list(CARRIED_CANDIDATES)
        or contract.get("new_candidates") != list(NEW_CANDIDATES)
        or contract.get("phase2_priority") != list(NEW_CANDIDATES)
        or contract.get("authority") != {
            "catalog_activation": False,
            "feature_or_outcome_construction": False,
            "final_252_or_forward_access": False,
            "macro_selection": False,
            "market_row_read": False,
            "model_fit_or_prediction": False,
            "provider_or_network": False,
            "trading": False,
            "wfa_or_alpha_evaluation": False,
        }
        or not isinstance(closure_binding, Mapping)
        or closure_binding.get("path") != CLOSURE_PATH.as_posix()
        or closure_binding.get("closure_id") != closure.get("closure_id")
        or closure_binding.get("sha256") != sha256_file(root / CLOSURE_PATH, reject_hardlinks=False)
    ):
        raise IntegrityError("macro successor contract is invalid")
    return contract


def _contains_forbidden(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(
            any(token in str(key).lower() for token in FORBIDDEN_RESULT_TOKENS)
            or _contains_forbidden(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_forbidden(item) for item in value)
    return False


def _decimal(value: object) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise IntegrityError("numeric selector evidence is invalid") from exc
    if not result.is_finite():
        raise IntegrityError("numeric selector evidence is not finite")
    return result


def evaluate_new_candidate(
    contract: Mapping[str, object], market: str, evidence: Mapping[str, object]
) -> dict[str, object]:
    if market not in NEW_CANDIDATES:
        raise UnauthorizedOperation("successor may read only SR3, ZB, or 6J")
    if set(evidence) != set(EVIDENCE_FIELDS) or _contains_forbidden(evidence):
        raise UnauthorizedOperation("successor evidence is missing, extra, or result-bearing")
    coverage = evidence["active_minute_coverage"]
    continuity = evidence["missingness_and_continuity"]
    roll = evidence["roll_behavior"]
    friction = evidence["conservative_round_trip_friction_ticks"]
    if not all(isinstance(item, Mapping) for item in (coverage, continuity, roll, friction)):
        return {"market": market, "status": UNKNOWN, "failed_gates": ["MALFORMED_EVIDENCE"]}
    thresholds = contract["execution_proxy_gates"]["thresholds"]  # type: ignore[index]
    adverse = contract["cost_model"]["adverse_ticks_round_trip"][market]  # type: ignore[index]
    try:
        movement = _decimal(evidence["target_horizon_movement_ticks"])
        friction_value = _decimal(friction.get("value"))
        tick_value = _decimal(friction.get("tick_value_usd"))
        declared_ratio = _decimal(evidence["movement_to_cost_ratio"])
        calculated_friction = Decimal(str(adverse)) + Decimal("10.00") / tick_value
        calculated_ratio = movement / friction_value
        if friction_value <= 0 or tick_value <= 0:
            raise IntegrityError("friction and tick value must be positive")
        if abs(friction_value - calculated_friction) > Decimal("0.000001"):
            raise IntegrityError("friction evidence differs from the frozen cost rule")
        if abs(declared_ratio - calculated_ratio) > Decimal("0.000001"):
            raise IntegrityError("movement-to-cost evidence is inconsistent")
        checks = (
            (int(coverage.get("usable_sessions", -1)) >= thresholds["minimum_usable_sessions"], "USABLE_SESSIONS"),
            (int(coverage.get("usable_observations", -1)) >= thresholds["minimum_usable_observations"], "USABLE_OBSERVATIONS"),
            (_decimal(coverage.get("ratio")) >= _decimal(thresholds["active_minute_coverage_ratio_minimum"]), "ACTIVE_MINUTE_COVERAGE"),
            (_decimal(evidence["zero_volume_minute_fraction"]) <= _decimal(thresholds["zero_volume_minute_fraction_maximum"]), "ZERO_VOLUME"),
            (_decimal(continuity.get("unexpected_gap_or_stale_fraction")) <= _decimal(thresholds["unexpected_gap_or_stale_fraction_maximum"]), "GAP_OR_STALENESS"),
            (_decimal(continuity.get("point_in_time_metadata_coverage_ratio")) == _decimal(thresholds["metadata_coverage_ratio_required"]), "METADATA_COVERAGE"),
            (int(continuity.get("quality_error_count", -1)) <= thresholds["quality_error_count_maximum"], "QUALITY_ERRORS"),
            (int(roll.get("identity_violation_count", -1)) <= thresholds["roll_identity_violation_count_maximum"], "ROLL_IDENTITY"),
            (declared_ratio >= _decimal(thresholds["minimum_movement_to_cost_ratio"]), "MOVEMENT_TO_COST"),
        )
    except (IntegrityError, TypeError, ValueError, ZeroDivisionError):
        return {"market": market, "status": UNKNOWN, "failed_gates": ["MALFORMED_EVIDENCE"]}
    failed = [name for passed, name in checks if not passed]
    return {
        "market": market,
        "status": INELIGIBLE if failed else ELIGIBLE,
        "failed_gates": failed,
        "movement_to_cost_ratio": str(declared_ratio),
    }


def select_successor_macro(
    contract: Mapping[str, object], new_evidence: Mapping[str, Mapping[str, object]]
) -> dict[str, object]:
    if tuple(new_evidence) != NEW_CANDIDATES:
        raise IntegrityError("new candidate evidence must be ordered SR3, ZB, 6J")
    carried = contract.get("carried_candidate_results")
    if not isinstance(carried, Mapping) or set(carried) != set(CARRIED_CANDIDATES):
        raise IntegrityError("carried ZN/6E evidence is invalid")
    fresh = {
        market: evaluate_new_candidate(contract, market, new_evidence[market])
        for market in NEW_CANDIDATES
    }
    results = {
        market: dict(carried[market]) if market in CARRIED_CANDIDATES else fresh[market]
        for market in CANDIDATES
    }
    if any(result.get("status") not in {ELIGIBLE, INELIGIBLE} for result in results.values()):
        return {"outcome": UNKNOWN, "selected_market": None, "candidate_results": results}
    eligible = [market for market in CANDIDATES if results[market]["status"] == ELIGIBLE]
    if not eligible:
        return {"outcome": NO_ELIGIBLE, "selected_market": None, "candidate_results": results}
    selected = max(
        eligible,
        key=lambda market: (
            _decimal(results[market]["movement_to_cost_ratio"]),
            -CANDIDATES.index(market),
        ),
    )
    return {
        "outcome": "SELECT_HIGHEST_MOVEMENT_TO_COST_RATIO",
        "selected_market": selected,
        "candidate_results": results,
    }
