"""Prepare a counted Alpha successor after V3 source incompatibility.

The successor changes execution semantics, so it restarts at synthetic Tier 0.
It uses resting limit orders whose fills can be proven from later reported bars.
No source compatibility, alpha, or profitability claim is made until a new
row-certified census passes every locked market and fold.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections import Counter
from collections.abc import Mapping
from pathlib import Path

from .alpha_ladder_frozen_mechanism import validate_frozen_mechanism
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


PREDECESSOR_ID = "186d8a103a581ae8c27fc531e0a556070991c9d2f87bbe5d62c1478867b5de3f"
PREDECESSOR_SHA256 = "1b0fa1d2beb1b463ec5c37f1341cca348a7ce1fee6d9dbae6074603b5ec37798"
PREDECESSOR_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_frozen_mechanism/"
    f"{PREDECESSOR_ID}/mechanism.json"
)
V3_REPORT_ID = "2cff813d498f24401bf600bf1087938f2a456d1235d31647cf63eac7dc0eb367"
V3_ROOT = Path("state/unpublished_evidence/alpha_ladder_combined_readiness_v3")
V3_BINDINGS = {
    (V3_ROOT / "pilot_fold_selection.json").as_posix():
        "6f47255d6900025d0d93f7e3d02fadca211541790988f703c58f7b2a83c16a95",
    (V3_ROOT / "pilot_readiness_certificate.json").as_posix():
        "ba0d2c65ac1dcacd0a88cf6c2d76fd4308eca7e043dbe9915e1a15aa1a8b5eb9",
    (V3_ROOT / "pilot_session_manifest.json").as_posix():
        "5e4d0fe5cf7411fee1376795c5a2306db29b68fbe832b4a51a3ce63dff2d4b3d",
    (V3_ROOT / "readiness_report.json").as_posix():
        "eb8508e258e46b26ae4e4b43b9c3228713f510c2b95ea3b41a0be5f320129277",
    (V3_ROOT / "tier1_readiness_certificate.json").as_posix():
        "59fc47cbf2e20e512effaff21cf726e861ad475c7829f89404978a43a730a775",
    (V3_ROOT / "tier1_session_manifest.json").as_posix():
        "b6b6a2522df945705c06ef2d213ef002c12ed804980b91cc70b36ca335e38c1b",
    "state/authorization_uses/8ac6621de3fbb10674ed0ac05e805ddaaa07197e2ddf8bc444bab0d4f96acfe0.json":
        "619d29ba935531d428baddf35b35b10bb4591dc0c768d641e9b28f7be80644ca",
}
REJECTION_ROOT = Path(
    "state/unpublished_evidence/alpha_ladder_v3_source_incompatibility_rejection"
)
SUCCESSOR_ROOT = Path(
    "state/unpublished_evidence/alpha_ladder_source_compatible_successor"
)
MODULE_PATH = Path("src/futures_rebuild/alpha_ladder_source_compatible_successor.py")
PREPARE_SCRIPT_PATH = Path("scripts/prepare_alpha_ladder_source_compatible_successor.py")

RETAINED_FIELDS = (
    "ladder_binding", "checkpoint", "features", "transformations", "model_family",
    "model_parameters", "ranking", "costs", "stop", "sizing", "baselines",
    "fold_construction", "metrics", "statistics", "promotion_gates", "research_only",
    "live_readiness",
)


def _load(path: Path) -> dict[str, object]:
    raw = path.read_bytes()
    if not raw.endswith(b"\n") or raw[:-1].endswith(b"\n"):
        raise IntegrityError(f"artifact is not canonical single-line JSON: {path}")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise IntegrityError(f"artifact is not a JSON object: {path}")
    return value


def _verify_bindings(root: Path) -> None:
    if sha256_file(root / PREDECESSOR_PATH) != PREDECESSOR_SHA256:
        raise IntegrityError("predecessor Alpha mechanism changed")
    for relative, expected in V3_BINDINGS.items():
        if sha256_file(root / relative) != expected:
            raise IntegrityError(f"Alpha V3 evidence changed: {relative}")


def build_rejection(*, root: Path) -> dict[str, object]:
    _verify_bindings(root)
    report = _load(root / V3_ROOT / "readiness_report.json")
    pilot = _load(root / V3_ROOT / "pilot_readiness_certificate.json")
    tier1 = _load(root / V3_ROOT / "tier1_readiness_certificate.json")
    if (
        report.get("report_id") != V3_REPORT_ID
        or report.get("pilot_decision") != "PASS"
        or report.get("tier1_decision") != "FAIL"
        or report.get("combined_registration_ready") is not False
        or pilot.get("overall_decision") != "PASS"
        or tier1.get("overall_decision") != "FAIL"
    ):
        raise IntegrityError("Alpha V3 terminal disposition changed")
    path_failures: Counter[tuple[str, str]] = Counter()
    failed_cells = []
    for item in tier1["fold_market_results"]:
        if item["status"] != "FAIL":
            continue
        failed_cells.append(f"{item['market']}/{item['fold_id']}")
        for reason, count in item["exclusion_reasons"].items():
            if reason.startswith("EVALUATION__TRIGGERED_ORDER_"):
                path_failures[(str(item["market"]), str(reason))] += int(count)
    if sum(path_failures.values()) != 7:
        raise IntegrityError("Alpha V3 execution-gap count no longer reconciles")
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_source_incompatibility_rejection/1.0.0",
        "state": "PREPARED_UNPUBLISHED_PRE_REGISTRATION_REJECTION",
        "classification": "CONCLUSIVE_PRE_REGISTRATION_SOURCE_INCOMPATIBILITY",
        "mechanism_id": PREDECESSOR_ID,
        "mechanism_path": PREDECESSOR_PATH.as_posix(),
        "mechanism_sha256": PREDECESSOR_SHA256,
        "v3_report_id": V3_REPORT_ID,
        "v3_report_path": (V3_ROOT / "readiness_report.json").as_posix(),
        "v3_report_sha256": V3_BINDINGS[(V3_ROOT / "readiness_report.json").as_posix()],
        "pilot_readiness": "PASS",
        "tier1_readiness": "FAIL",
        "combined_registration_ready": False,
        "failed_fold_market_cells": sorted(failed_cells),
        "triggered_execution_gaps": [
            {"market": market, "disposition": reason, "count": count}
            for (market, reason), count in sorted(path_failures.items())
        ],
        "total_triggered_execution_gaps": 7,
        "economic_result": "NOT_PRODUCED",
        "strategy_failure": False,
        "registration_allowed": False,
        "trial_execution_allowed": False,
        "incremental_retry_allowed": False,
        "preservation": "MECHANISM_AND_ALL_V3_BYTES_REMAIN_UNCHANGED",
        "bindings": dict(sorted({PREDECESSOR_PATH.as_posix(): PREDECESSOR_SHA256, **V3_BINDINGS}.items())),
    }
    return {**core, "rejection_id": sha256_json(core)}


def rejection_path(rejection: Mapping[str, object]) -> Path:
    return REJECTION_ROOT / str(rejection["rejection_id"]) / "rejection.json"


def build_successor(*, root: Path, rejection: Mapping[str, object]) -> dict[str, object]:
    predecessor = _load(root / PREDECESSOR_PATH)
    validate_frozen_mechanism(predecessor)
    if rejection.get("classification") != "CONCLUSIVE_PRE_REGISTRATION_SOURCE_INCOMPATIBILITY":
        raise IntegrityError("successor lacks the conclusive V3 rejection")
    core = copy.deepcopy({key: value for key, value in predecessor.items() if key != "mechanism_id"})
    core.update({
        "schema_version": "alpha_ladder_source_compatible_successor/1.0.0",
        "state": "PREPARED_UNPUBLISHED_UNREGISTERED_TIER0_RESTART_REQUIRED",
        "classification": "NEW_COUNTED_ALPHA_MECHANISM_SOURCE_COMPATIBILITY_UNPROVEN",
        "restart_stage": "tier_0",
        "predecessor": {
            "mechanism_id": PREDECESSOR_ID,
            "path": PREDECESSOR_PATH.as_posix(),
            "sha256": PREDECESSOR_SHA256,
            "rejection_id": rejection["rejection_id"],
            "rejection_path": rejection_path(rejection).as_posix(),
        },
        "entry_rules": {
            "decision_time": "10:00:05_AMERICA_CHICAGO",
            "intent_trigger": "FIRST_REPORTED_BAR_AVAILABLE_AFTER_DECISION_WITHIN_120_SECONDS",
            "no_trigger": "EXPLICIT_NO_TRADE_TIMEOUT",
            "order_type": "RESTING_LIMIT_ONE_STANDARD_CONTRACT",
            "limit_price": "TRIGGER_REPORTED_CLOSE",
            "order_time": "TRIGGER_AVAILABLE_AT",
            "entry_resolution_window_minutes": 5,
            "verified_fill": (
                "FIRST_LATER_REPORTED_BAR_WHOSE_INTERVAL_STARTS_AFTER_ORDER_TIME_AND_WHOSE_"
                "RANGE_PENETRATES_THE_RESTING_LIMIT_BY_AT_LEAST_ONE_TICK_FILL_EXACTLY_AT_LIMIT"
            ),
            "unfilled_limit": "EXPLICIT_CANCELLED_NO_TRADE_TIMEOUT",
            "same_bar_entry_and_stop": "CONSERVATIVE_ENTRY_THEN_STOP_ORDERING",
            "same_actual_contract_identity_required": True,
            "runner_up_substitution": False,
        },
        "exit_rules": {
            "protective_stop": "UNCHANGED_1_5_ATR20_FULL_TICK_STOP_ACTIVE_FROM_ENTRY",
            "scheduled_exit_intent": "ENTRY_FILL_TIME_PLUS_30_MINUTES",
            "exit_anchor": (
                "FIRST_REPORTED_BAR_CLOSE_AVAILABLE_AT_OR_AFTER_SCHEDULED_EXIT_INTENT"
            ),
            "exit_order_time": "EXIT_ANCHOR_AVAILABLE_AT",
            "order_type": "DIRECTIONAL_PRICE_PROTECTED_RESTING_LIMIT",
            "limit_offset": "LOCKED_STRESS_ADVERSE_TICKS_FROM_EXIT_ANCHOR_CLOSE",
            "exit_resolution_window_minutes": 15,
            "verified_exit": (
                "FIRST_LATER_REPORTED_BAR_WHOSE_INTERVAL_STARTS_AFTER_EXIT_ORDER_TIME_AND_"
                "WHOSE_RANGE_PENETRATES_THE_EXIT_LIMIT_BY_AT_LEAST_ONE_TICK_FILL_EXACTLY_AT_LIMIT"
            ),
            "protective_stop_precedence": True,
            "unresolved_exit_after_filled_entry": "MANDATORY_EXECUTION_PATH_FAILURE",
            "same_actual_contract_identity_required": True,
        },
        "source_compatibility_gate": {
            "status": "UNPROVEN_REQUIRES_NEW_ROW_CERTIFIED_CENSUS",
            "required_markets": ["ES", "CL", "ZN", "6E"],
            "candidate_checkpoint_accounting_percent": 100,
            "active_baseline_checkpoint_accounting_percent": 100,
            "filled_entry_to_verified_exit_percent": 100,
            "unfilled_entry_limits_are_explicit_no_trade": True,
            "future_path_used_for_admission": False,
            "candidate_schedule_reuse_by_baselines": False,
            "registration_before_pass": False,
            "pilot_execution_before_pass": False,
        },
        "outcome_access": {
            "returns_read": False, "predictions_generated": False,
            "models_fit": False, "economic_evaluation": False, "year_2025_access": False,
        },
        "authority": {
            "historical_rows": False, "registration": False, "execution": False,
            "publication": False, "holdout_2025": False,
            "provider_network_credentials": False, "trading": False,
        },
        "source_design_binding": {
            "classification": "CAUSAL_REPORTED_BAR_RESTING_LIMIT_SUCCESSOR",
            "predecessor_mechanism_id": PREDECESSOR_ID,
            "v3_rejection_id": rejection["rejection_id"],
            "no_fill_invention": True,
            "no_future_complete_path_filter": True,
            "new_100_percent_census_required": True,
        },
        "bindings": dict(sorted({
            PREDECESSOR_PATH.as_posix(): PREDECESSOR_SHA256,
            rejection_path(rejection).as_posix(): hashlib.sha256(
                canonical_bytes(rejection) + b"\n"
            ).hexdigest(),
            MODULE_PATH.as_posix(): sha256_file(root / MODULE_PATH),
            PREPARE_SCRIPT_PATH.as_posix(): sha256_file(root / PREPARE_SCRIPT_PATH),
            **V3_BINDINGS,
        }.items())),
    })
    return {**core, "mechanism_id": sha256_json(core)}


def successor_path(mechanism: Mapping[str, object]) -> Path:
    return SUCCESSOR_ROOT / str(mechanism["mechanism_id"]) / "mechanism.json"


def validate_successor(
    mechanism: Mapping[str, object], *, predecessor: Mapping[str, object],
    rejection: Mapping[str, object],
) -> dict[str, object]:
    core = {key: value for key, value in mechanism.items() if key != "mechanism_id"}
    if mechanism.get("mechanism_id") != sha256_json(core):
        raise IntegrityError("successor mechanism identity changed")
    for field in RETAINED_FIELDS:
        if mechanism.get(field) != predecessor.get(field):
            raise UnauthorizedOperation(f"successor changed retained field: {field}")
    gate = mechanism.get("source_compatibility_gate")
    entry = mechanism.get("entry_rules")
    exit_rules = mechanism.get("exit_rules")
    authority = mechanism.get("authority")
    outcomes = mechanism.get("outcome_access")
    if (
        mechanism.get("restart_stage") != "tier_0"
        or mechanism.get("classification")
        != "NEW_COUNTED_ALPHA_MECHANISM_SOURCE_COMPATIBILITY_UNPROVEN"
        or not isinstance(gate, Mapping) or not isinstance(entry, Mapping)
        or not isinstance(exit_rules, Mapping) or not isinstance(authority, Mapping)
        or not isinstance(outcomes, Mapping)
        or gate.get("filled_entry_to_verified_exit_percent") != 100
        or gate.get("candidate_checkpoint_accounting_percent") != 100
        or gate.get("active_baseline_checkpoint_accounting_percent") != 100
        or gate.get("required_markets") != ["ES", "CL", "ZN", "6E"]
        or gate.get("registration_before_pass") is not False
        or gate.get("pilot_execution_before_pass") is not False
        or gate.get("future_path_used_for_admission") is not False
        or gate.get("candidate_schedule_reuse_by_baselines") is not False
        or entry.get("order_type") != "RESTING_LIMIT_ONE_STANDARD_CONTRACT"
        or entry.get("limit_price") != "TRIGGER_REPORTED_CLOSE"
        or entry.get("entry_resolution_window_minutes") != 5
        or "PENETRATES_THE_RESTING_LIMIT_BY_AT_LEAST_ONE_TICK"
        not in str(entry.get("verified_fill"))
        or entry.get("unfilled_limit") != "EXPLICIT_CANCELLED_NO_TRADE_TIMEOUT"
        or exit_rules.get("order_type") != "DIRECTIONAL_PRICE_PROTECTED_RESTING_LIMIT"
        or exit_rules.get("limit_offset")
        != "LOCKED_STRESS_ADVERSE_TICKS_FROM_EXIT_ANCHOR_CLOSE"
        or exit_rules.get("exit_resolution_window_minutes") != 15
        or "PENETRATES_THE_EXIT_LIMIT_BY_AT_LEAST_ONE_TICK"
        not in str(exit_rules.get("verified_exit"))
        or exit_rules.get("unresolved_exit_after_filled_entry")
        != "MANDATORY_EXECUTION_PATH_FAILURE"
        or any(value is not False for value in authority.values())
        or any(value is not False for value in outcomes.values())
        or mechanism.get("predecessor", {}).get("rejection_id") != rejection.get("rejection_id")
    ):
        raise IntegrityError("successor fail-closed semantics are incomplete")
    return dict(mechanism)
