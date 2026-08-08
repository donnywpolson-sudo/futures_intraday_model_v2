"""Close the resting-exit mechanism and prepare its counted successor."""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from .alpha_ladder_limit_readiness import LimitBar
from .alpha_ladder_source_compatible_successor import validate_successor as validate_predecessor
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


PREDECESSOR_ID = "767ecf3987d816c2f657fbf030da25bf72511275812d6664aa6bd56faf7f3660"
PREDECESSOR_SHA256 = "c29e2a13639a289440065b6d07b064e4cd2334c80b42ba8de205d3dc5cba5e1c"
PREDECESSOR_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_source_compatible_successor/"
    f"{PREDECESSOR_ID}/mechanism.json"
)
PREDECESSOR_TIER0_CERTIFICATE = PREDECESSOR_PATH.with_name("tier0_certificate.json")
PREDECESSOR_TIER0_DECISION = PREDECESSOR_PATH.with_name("tier0_decision.json")
V5_REPORT_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_limit_readiness_v5/readiness_report.json"
)
V5_REPORT_ID = "2207e76d6e0d2255dec67349385d68d5c72d212c3a9021ca69dd3ebbdef5488b"
V5_SELECTION_PATH = V5_REPORT_PATH.with_name("pilot_fold_selection.json")
V5_SELECTION_ID = "87cef6ab6640c546252c92dda2c9a9ec0e8c902264ebc2e1da835d24770e0b47"
DIAGNOSTIC_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_es_training_diagnostic/diagnostic_report.json"
)
DIAGNOSTIC_ID = "e6d97759ecbfcce6ea9b6f0bc6b6c20af67b5a370bcb94913237923e12cd19ff"
DIAGNOSTIC_PLAN_PATH = Path("configs/alpha_ladder_es_training_diagnostic_plan.json")
DIAGNOSTIC_AUTHORIZATION_PATH = Path(
    "state/authorization_uses/3cc8658ec9e0fc850611211b3a95c8c30cdcca5aff763643a835b24ae7c7bb22.json"
)
MODULE_PATH = Path("src/futures_rebuild/alpha_ladder_reported_trade_exit_successor.py")
PREPARE_SCRIPT_PATH = Path("scripts/prepare_alpha_ladder_reported_trade_exit_successor.py")
CLOSURE_ROOT = Path("state/unpublished_evidence/alpha_ladder_limit_exit_closure")
SUCCESSOR_ROOT = Path("state/unpublished_evidence/alpha_ladder_reported_trade_exit_successor")
SUPERSEDED_PREPARATION_ID = (
    "0122bb6afb32ae0acc760dd0beb675b473d69af8c9b5ea736cad20d63c2616c8"
)
SUPERSEDED_PREPARATION_SHA256 = (
    "e4d296ff2b992a757bf4f912f1be97384d585a57170b7f4ee20cd4451d3e9461"
)
SUPERSEDED_PREPARATION_PATH = (
    SUCCESSOR_ROOT / SUPERSEDED_PREPARATION_ID / "mechanism.json"
)


@dataclass(frozen=True)
class ReportedTradeExit:
    complete: bool
    disposition: str
    order_time: datetime
    evidence_bar: LimitBar | None
    fill_time: datetime | None
    fill_price: Decimal | None


def classify_reported_trade_exit(
    *, bars: Sequence[LimitBar], scheduled_exit_intent: datetime,
    identity: str, resolution_minutes: int = 15,
) -> ReportedTradeExit:
    """Find the first later reported trade bar without a price-return condition."""

    if (
        resolution_minutes != 15
        or not identity
        or scheduled_exit_intent.tzinfo is None
        or scheduled_exit_intent.utcoffset() is None
    ):
        raise IntegrityError("reported-trade exit left its locked semantics")
    order_time = scheduled_exit_intent + timedelta(seconds=5)
    candidates = tuple(sorted((bar for bar in bars
                               if order_time < bar.event_at
                               <= order_time + timedelta(minutes=resolution_minutes)),
                              key=lambda bar: bar.event_at))
    if not candidates:
        return ReportedTradeExit(False, "REPORTED_TRADE_EXIT_EVIDENCE_MISSING",
                                 order_time, None, None, None)
    first = candidates[0]
    first_timestamp = tuple(bar for bar in candidates if bar.event_at == first.event_at)
    if len(first_timestamp) != 1:
        return ReportedTradeExit(False, "REPORTED_TRADE_EXIT_EVIDENCE_AMBIGUOUS",
                                 order_time, None, None, None)
    if first.identity != identity:
        return ReportedTradeExit(False, "REPORTED_TRADE_EXIT_IDENTITY_CHANGING",
                                 order_time, first, None, None)
    if (
        first.event_at.tzinfo is None
        or first.event_at.utcoffset() is None
        or first.available_at.tzinfo is None
        or first.available_at.utcoffset() is None
        or first.available_at < first.event_at + timedelta(minutes=1, seconds=5)
        or first.open <= 0
        or first.high <= 0
        or first.low <= 0
        or first.close <= 0
        or first.tick_size <= 0
        or first.tick_value <= 0
        or first.volume <= 0
        or first.high < first.low
        or not first.low <= first.open <= first.high
        or not first.low <= first.close <= first.high
    ):
        return ReportedTradeExit(False, "REPORTED_TRADE_EXIT_EVIDENCE_INVALID",
                                 order_time, first, None, None)
    return ReportedTradeExit(True, "VERIFIED_CAUSAL_REPORTED_TRADE_EXIT_PROXY",
                             order_time, first, first.event_at, first.open)


def _load(path: Path) -> dict[str, object]:
    raw = path.read_bytes()
    if not raw.endswith(b"\n") or raw[:-1].endswith(b"\n"):
        raise IntegrityError(f"artifact is not canonical single-line JSON: {path}")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise IntegrityError(f"artifact is not an object: {path}")
    return value


def _load_predecessor(*, root: Path) -> dict[str, object]:
    predecessor = _load(root / PREDECESSOR_PATH)
    old_predecessor = _load(root / str(predecessor["predecessor"]["path"]))
    rejection = _load(root / str(predecessor["predecessor"]["rejection_path"]))
    validate_predecessor(predecessor, predecessor=old_predecessor, rejection=rejection)
    if sha256_file(root / PREDECESSOR_PATH) != PREDECESSOR_SHA256:
        raise IntegrityError("resting-exit predecessor changed")
    return predecessor


def build_closure(*, root: Path) -> dict[str, object]:
    _load_predecessor(root=root)
    v5 = _load(root / V5_REPORT_PATH)
    selection = _load(root / V5_SELECTION_PATH)
    diagnostic = _load(root / DIAGNOSTIC_PATH)
    if (
        v5.get("report_id") != V5_REPORT_ID
        or v5.get("state") != "SEALED_UNPUBLISHED_NO_EXECUTABLE_PILOT_FOLD"
        or selection.get("selection_id") != V5_SELECTION_ID
        or selection.get("decision") != "NO_EXECUTABLE_PILOT_FOLD"
        or diagnostic.get("report_id") != DIAGNOSTIC_ID
        or diagnostic.get("eligible_session_count") != 1291
        or diagnostic.get("terminal_session_count") != 1291
        or diagnostic.get("session_canonical_failure_counts") != {
            "EXPLICIT_CAUSAL_FEATURE_ABSTENTION": 2,
            "LONG__base__VERIFIED_EXIT_MISSING": 18,
            "SHORT__base__VERIFIED_EXIT_MISSING": 11,
        }
        or diagnostic["window_summary"].get("maximum_complete_training_sessions") != 495
        or diagnostic["window_summary"].get("minimum_training_shortfall_sessions") != 9
    ):
        raise IntegrityError("resting-exit closure evidence changed")
    bindings = {
        PREDECESSOR_PATH.as_posix(): PREDECESSOR_SHA256,
        PREDECESSOR_TIER0_CERTIFICATE.as_posix(): sha256_file(root / PREDECESSOR_TIER0_CERTIFICATE),
        PREDECESSOR_TIER0_DECISION.as_posix(): sha256_file(root / PREDECESSOR_TIER0_DECISION),
        V5_REPORT_PATH.as_posix(): sha256_file(root / V5_REPORT_PATH),
        V5_SELECTION_PATH.as_posix(): sha256_file(root / V5_SELECTION_PATH),
        DIAGNOSTIC_PATH.as_posix(): sha256_file(root / DIAGNOSTIC_PATH),
        DIAGNOSTIC_PLAN_PATH.as_posix(): sha256_file(root / DIAGNOSTIC_PLAN_PATH),
        DIAGNOSTIC_AUTHORIZATION_PATH.as_posix(): sha256_file(root / DIAGNOSTIC_AUTHORIZATION_PATH),
    }
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_pre_registration_closure/1.0.0",
        "state": "PREPARED_UNPUBLISHED_TERMINAL_CLOSURE",
        "classification": "PRE_REGISTRATION_SOURCE_INCOMPATIBLE_UNRESOLVED_EXIT_PATHS",
        "display_classification": "PRE_REGISTRATION_SOURCE_INCOMPATIBLE — UNRESOLVED EXIT PATHS",
        "mechanism_id": PREDECESSOR_ID,
        "mechanism_path": PREDECESSOR_PATH.as_posix(),
        "mechanism_sha256": PREDECESSOR_SHA256,
        "tier0_status": "PASS_SYNTHETIC_ONLY",
        "pilot_registration_status": "FORBIDDEN",
        "historical_execution_status": "READINESS_ONLY_NO_ECONOMIC_EXECUTION",
        "economic_result": "NOT_PRODUCED",
        "strategy_failure": False,
        "profitability_conclusion": False,
        "source_compatibility_conclusion": "FAIL",
        "eligible_es_sessions": 1291,
        "incomplete_es_sessions": 31,
        "unresolved_base_exit_sessions": 29,
        "feature_gap_sessions": 2,
        "best_504_session_window_complete_sessions": 495,
        "minimum_training_shortfall_sessions": 9,
        "incremental_retry_allowed": False,
        "parameter_rescue_allowed": False,
        "new_counted_mechanism_required": True,
        "preservation": "MECHANISM_TIER0_V5_DIAGNOSTIC_AND_AUTHORIZATION_BYTES_UNCHANGED",
        "publication_authorized": False,
        "activation_authorized": False,
        "bindings": dict(sorted(bindings.items())),
    }
    return {**core, "closure_id": sha256_json(core)}


def closure_path(closure: Mapping[str, object]) -> Path:
    return CLOSURE_ROOT / str(closure["closure_id"]) / "closure.json"


def validate_closure(closure: Mapping[str, object], *, root: Path) -> dict[str, object]:
    core = {key: value for key, value in closure.items() if key != "closure_id"}
    if (
        closure.get("closure_id") != sha256_json(core)
        or closure.get("classification")
        != "PRE_REGISTRATION_SOURCE_INCOMPATIBLE_UNRESOLVED_EXIT_PATHS"
        or closure.get("strategy_failure") is not False
        or closure.get("economic_result") != "NOT_PRODUCED"
        or closure.get("pilot_registration_status") != "FORBIDDEN"
        or closure.get("incremental_retry_allowed") is not False
        or closure.get("publication_authorized") is not False
        or closure.get("activation_authorized") is not False
    ):
        raise IntegrityError("resting-exit closure is invalid")
    bindings = closure.get("bindings")
    if not isinstance(bindings, Mapping) or any(
        sha256_file(root / str(path)) != digest for path, digest in bindings.items()
    ):
        raise IntegrityError("resting-exit closure binding changed")
    return dict(closure)


def build_successor(*, root: Path, closure: Mapping[str, object]) -> dict[str, object]:
    predecessor = _load_predecessor(root=root)
    validate_closure(closure, root=root)
    core = copy.deepcopy({key: value for key, value in predecessor.items()
                          if key != "mechanism_id"})
    core.update({
        "schema_version": "alpha_ladder_reported_trade_exit_successor/1.0.0",
        "state": "PREPARED_UNPUBLISHED_UNREGISTERED_TIER0_RESTART_REQUIRED",
        "classification": "NEW_COUNTED_ALPHA_MECHANISM_SOURCE_COMPATIBILITY_UNPROVEN",
        "restart_stage": "tier_0",
        "predecessor": {
            "mechanism_id": PREDECESSOR_ID,
            "path": PREDECESSOR_PATH.as_posix(),
            "sha256": PREDECESSOR_SHA256,
            "closure_id": closure["closure_id"],
            "closure_path": closure_path(closure).as_posix(),
        },
        "exit_rules": {
            "protective_stop": "UNCHANGED_1_5_ATR20_FULL_TICK_STOP_ACTIVE_FROM_ENTRY",
            "scheduled_exit_intent": "ENTRY_FILL_TIME_PLUS_30_MINUTES",
            "exit_order_time": "SCHEDULED_EXIT_INTENT_PLUS_5_SECONDS_LOCKED_LATENCY",
            "order_type": "RESEARCH_MARKET_ORDER_WITH_CAUSAL_REPORTED_TRADE_PROXY",
            "exit_resolution_window_minutes": 15,
            "verified_exit": (
                "FIRST_LATER_REPORTED_BAR_WHOSE_INTERVAL_STARTS_AFTER_EXIT_ORDER_TIME_"
                "FILL_PROXY_AT_REPORTED_BAR_OPEN_BEFORE_SEPARATE_LOCKED_ADVERSE_COSTS"
            ),
            "price_return_condition": None,
            "exact_quote_or_queue_claim": False,
            "simulation_only_fill_proxy": True,
            "protective_stop_precedence": True,
            "unresolved_exit_after_filled_entry": "MANDATORY_EXECUTION_PATH_FAILURE",
            "same_actual_contract_identity_required": True,
        },
        "source_compatibility_gate": {
            **predecessor["source_compatibility_gate"],
            "status": "UNPROVEN_REQUIRES_NEW_100_PERCENT_ROW_CERTIFIED_CENSUS",
            "scheduled_exit_requires_price_return": False,
            "reported_trade_exit_proxy_percent": 100,
            "previous_diagnostic_outcomes_used_for_parameter_selection": False,
        },
        "outcome_access": {
            "returns_read": False, "predictions_generated": False,
            "models_fit": False, "economic_evaluation": False,
            "year_2025_access": False,
        },
        "authority": {
            "historical_rows": False, "registration": False, "execution": False,
            "publication": False, "holdout_2025": False,
            "provider_network_credentials": False, "trading": False,
        },
        "source_design_binding": {
            "classification": "CAUSAL_REPORTED_TRADE_SCHEDULED_EXIT_SUCCESSOR",
            "predecessor_mechanism_id": PREDECESSOR_ID,
            "predecessor_closure_id": closure["closure_id"],
            "only_semantic_change": "SCHEDULED_EXIT_FILL_PROXY",
            "no_price_return_condition": True,
            "no_future_complete_path_filter": True,
            "new_100_percent_census_required": True,
            "no_return_or_economic_outcome_used": True,
            "superseded_unpublished_preparation": {
                "mechanism_id": SUPERSEDED_PREPARATION_ID,
                "path": SUPERSEDED_PREPARATION_PATH.as_posix(),
                "sha256": SUPERSEDED_PREPARATION_SHA256,
                "reason": "TRANSITION_BINDINGS_NOT_ENFORCED_BY_INITIAL_VALIDATOR",
            },
        },
        "bindings": dict(sorted({
            PREDECESSOR_PATH.as_posix(): PREDECESSOR_SHA256,
            closure_path(closure).as_posix(): hashlib.sha256(
                canonical_bytes(closure) + b"\n").hexdigest(),
            MODULE_PATH.as_posix(): sha256_file(root / MODULE_PATH),
            PREPARE_SCRIPT_PATH.as_posix(): sha256_file(root / PREPARE_SCRIPT_PATH),
            DIAGNOSTIC_PATH.as_posix(): sha256_file(root / DIAGNOSTIC_PATH),
            SUPERSEDED_PREPARATION_PATH.as_posix(): SUPERSEDED_PREPARATION_SHA256,
        }.items())),
    })
    return {**core, "mechanism_id": sha256_json(core)}


def successor_path(mechanism: Mapping[str, object]) -> Path:
    return SUCCESSOR_ROOT / str(mechanism["mechanism_id"]) / "mechanism.json"


def validate_successor(
    mechanism: Mapping[str, object], *, predecessor: Mapping[str, object],
    closure: Mapping[str, object], root: Path,
) -> dict[str, object]:
    core = {key: value for key, value in mechanism.items() if key != "mechanism_id"}
    mutable = {"schema_version", "state", "classification", "restart_stage",
               "predecessor", "exit_rules", "source_compatibility_gate",
               "outcome_access", "authority", "source_design_binding", "bindings"}
    retained = set(predecessor) - {"mechanism_id"} - mutable
    if mechanism.get("mechanism_id") != sha256_json(core):
        raise IntegrityError("reported-trade successor identity changed")
    for field in retained:
        if mechanism.get(field) != predecessor.get(field):
            raise UnauthorizedOperation(f"reported-trade successor changed retained field: {field}")
    exit_rules = mechanism.get("exit_rules")
    gate = mechanism.get("source_compatibility_gate")
    predecessor_binding = mechanism.get("predecessor")
    design = mechanism.get("source_design_binding")
    bindings = mechanism.get("bindings")
    expected_predecessor_binding = {
        "mechanism_id": PREDECESSOR_ID,
        "path": PREDECESSOR_PATH.as_posix(),
        "sha256": PREDECESSOR_SHA256,
        "closure_id": closure["closure_id"],
        "closure_path": closure_path(closure).as_posix(),
    }
    expected_bindings = {
        PREDECESSOR_PATH.as_posix(): PREDECESSOR_SHA256,
        closure_path(closure).as_posix(): hashlib.sha256(
            canonical_bytes(closure) + b"\n").hexdigest(),
        MODULE_PATH.as_posix(): sha256_file(root / MODULE_PATH),
        PREPARE_SCRIPT_PATH.as_posix(): sha256_file(root / PREPARE_SCRIPT_PATH),
        DIAGNOSTIC_PATH.as_posix(): sha256_file(root / DIAGNOSTIC_PATH),
        SUPERSEDED_PREPARATION_PATH.as_posix(): SUPERSEDED_PREPARATION_SHA256,
    }
    if (
        mechanism.get("state")
        != "PREPARED_UNPUBLISHED_UNREGISTERED_TIER0_RESTART_REQUIRED"
        or mechanism.get("classification")
        != "NEW_COUNTED_ALPHA_MECHANISM_SOURCE_COMPATIBILITY_UNPROVEN"
        or mechanism.get("restart_stage") != "tier_0"
        or not isinstance(exit_rules, Mapping) or not isinstance(gate, Mapping)
        or not isinstance(predecessor_binding, Mapping)
        or dict(predecessor_binding) != expected_predecessor_binding
        or not isinstance(design, Mapping)
        or design.get("only_semantic_change") != "SCHEDULED_EXIT_FILL_PROXY"
        or design.get("no_price_return_condition") is not True
        or design.get("new_100_percent_census_required") is not True
        or design.get("no_return_or_economic_outcome_used") is not True
        or exit_rules.get("price_return_condition") is not None
        or exit_rules.get("order_type")
        != "RESEARCH_MARKET_ORDER_WITH_CAUSAL_REPORTED_TRADE_PROXY"
        or exit_rules.get("simulation_only_fill_proxy") is not True
        or exit_rules.get("exact_quote_or_queue_claim") is not False
        or exit_rules.get("exit_resolution_window_minutes") != 15
        or gate.get("filled_entry_to_verified_exit_percent") != 100
        or gate.get("candidate_checkpoint_accounting_percent") != 100
        or gate.get("active_baseline_checkpoint_accounting_percent") != 100
        or gate.get("registration_before_pass") is not False
        or gate.get("pilot_execution_before_pass") is not False
        or gate.get("scheduled_exit_requires_price_return") is not False
        or gate.get("reported_trade_exit_proxy_percent") != 100
        or gate.get("previous_diagnostic_outcomes_used_for_parameter_selection") is not False
        or mechanism.get("outcome_access") != {
            "returns_read": False, "predictions_generated": False,
            "models_fit": False, "economic_evaluation": False,
            "year_2025_access": False,
        }
        or mechanism.get("authority") != {
            "historical_rows": False, "registration": False, "execution": False,
            "publication": False, "holdout_2025": False,
            "provider_network_credentials": False, "trading": False,
        }
        or not isinstance(bindings, Mapping)
        or dict(bindings) != dict(sorted(expected_bindings.items()))
    ):
        raise IntegrityError("reported-trade successor is not fail closed")
    validate_closure(closure, root=root)
    return dict(mechanism)
