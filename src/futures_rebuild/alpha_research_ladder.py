"""Fail-closed contracts for the staged Alpha research ladder.

The ladder governs evidence reuse and stage progression.  It does not fit a
model, read market rows, evaluate returns, publish research, or grant access.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

from .canonical import canonical_bytes, contained_path, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


LEGACY_CONTRACT_SCHEMA = "alpha_research_ladder_contract/1.0.0"
CONTRACT_SCHEMA = "alpha_research_ladder_contract/2.0.0"
QUALIFICATION_CONTRACT_SCHEMA = "alpha_research_ladder_contract/4.0.0"
FINAL_252_CONTRACT_SCHEMA = "alpha_research_ladder_contract/5.0.0"
HYPOTHESIS_CONDITIONAL_CONTRACT_SCHEMA = "alpha_research_ladder_contract/6.0.0"
BIAS_GATE_CONTRACT_SCHEMA = "alpha_research_ladder_contract/7.0.0"
BIAS_REMEDIATION_CONTRACT_SCHEMA = "alpha_research_ladder_contract/8.0.0"
CANONICAL_BIAS_REMEDIATION_CONTRACT_SCHEMA = "alpha_research_ladder_contract/9.0.0"
LEGACY_PROFILE_SCHEMA = "alpha_research_ladder_profile/1.0.0"
PROFILE_SCHEMA = "alpha_research_ladder_profile/2.0.0"
QUALIFICATION_PROFILE_SCHEMA = "alpha_research_ladder_profile/4.0.0"
FINAL_252_PROFILE_SCHEMA = "alpha_research_ladder_profile/5.0.0"
HYPOTHESIS_CONDITIONAL_PROFILE_SCHEMA = "alpha_research_ladder_profile/6.0.0"
BIAS_GATE_PROFILE_SCHEMA = "alpha_research_ladder_profile/7.0.0"
BIAS_REMEDIATION_PROFILE_SCHEMA = "alpha_research_ladder_profile/8.0.0"
CANONICAL_BIAS_REMEDIATION_PROFILE_SCHEMA = "alpha_research_ladder_profile/9.0.0"
POINTER_SCHEMA = "active_alpha_research_ladder/1.0.0"
SESSION_MANIFEST_SCHEMA = "alpha_ladder_session_manifest/1.0.0"
DECISION_SCHEMA = "alpha_ladder_stage_decision/1.0.0"

ACTIVE_POINTER_PATH = Path("configs/active_alpha_research_ladder.json")
# ``pilot`` remains an operational gate identifier for immutable registrations
# and evidence.  It is no longer a user-visible ladder level in schema v2.
STAGES = ("pilot", "tier_1", "tier_2", "tier_3", "holdout", "forward")
LEVELS = ("tier_0", "tier_1", "tier_2", "tier_3", "holdout", "forward")
PREDECESSOR = {
    "pilot": "tier_0",
    "tier_1": "pilot",
    "tier_2": "tier_1",
    "tier_3": "tier_2",
    "holdout": "tier_3",
    "forward": "holdout",
}

CORE = ("ES", "CL", "ZN", "6E")
TIER1_FIXED_MARKETS = ("NQ", "CL", "GC")
MACRO_CANDIDATES = ("ZN", "6E")
BALANCED = (
    "ES", "NQ", "CL", "NG", "RB", "GC", "HG", "SR3",
    "ZN", "ZB", "6E", "6J", "ZC", "ZS", "LE", "HE",
)
TRADITIONAL = (
    "ES", "NQ", "RTY", "YM", "CL", "NG", "RB", "HO", "GC", "SI",
    "HG", "PL", "SR3", "SR1", "ZQ", "TN", "ZT", "ZF", "ZN", "ZB",
    "UB", "6A", "6B", "6C", "6E", "6J", "6M", "6N", "6S", "ZC",
    "ZS", "ZL", "ZM", "ZW", "KE", "LE", "HE", "GF",
)
SATELLITE = ("BTC", "ETH", "PA")
ALL_APPROVED = (*TRADITIONAL, *SATELLITE)
DEFERRED_MICROS = (
    "MES", "MCL", "MGC", "M6E", "MNQ", "MYM", "M2K", "M6A", "SIL",
    "MBT", "MET", "M6B", "MJY", "MCD", "MSF", "MNG", "MHG",
)

ALPHA_STATUSES = ("NOT_EVALUATED", "PASS", "FAIL", "INSUFFICIENT_EVIDENCE")
EXECUTION_PROXY_STATUSES = ALPHA_STATUSES
LIVE_EXECUTION_STATUSES = ("UNCLASSIFIED", "APPROVED", "REJECTED")
MACRO_SELECTION_STATUSES = (
    "PENDING_PRE_RESULT_EXECUTION_GATE", "SELECTED", "NO_ELIGIBLE_MACRO_DIVERSIFIER",
)
ALLOWED_MACRO_EVIDENCE_FIELDS = (
    "target_horizon_movement_ticks", "conservative_round_trip_friction_ticks",
    "movement_to_cost_ratio", "active_minute_coverage",
    "zero_volume_minute_fraction", "missingness_and_continuity",
    "time_of_day_stability", "roll_behavior", "spread_depth_cost_evidence",
)
FORBIDDEN_MACRO_EVIDENCE_FIELDS = (
    "pnl", "profit_and_loss", "sharpe", "prediction_accuracy", "hit_rate",
    "target_hit_rate", "feature_importance", "wfa", "tier_result",
    "holdout_result", "forward_result",
)

FROZEN_MECHANISM_FIELDS = (
    "features", "transformations", "model_family", "model_parameters",
    "checkpoint", "entry_rules", "ranking", "costs", "stop", "sizing",
    "baselines", "fold_construction", "metrics", "promotion_gates",
)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise IntegrityError(f"{name} must be a mapping")
    return value


def _strings(value: object, name: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or (not value and not allow_empty)
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise IntegrityError(f"{name} must be a string sequence")
    return tuple(value)


def _digest(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise IntegrityError(f"{name} must be a SHA-256 digest")
    return value


def _identity(payload: Mapping[str, object], key: str, schema: str) -> None:
    core = dict(payload)
    identity = core.pop(key, None)
    if core.get("schema_version") != schema or identity != sha256_json(core):
        raise IntegrityError(f"{key} is invalid")


def _build_legacy_contract(
    *, predecessor_path: str, predecessor_sha256: str,
) -> dict[str, object]:
    """Rebuild the immutable schema-v1 contract for historical validation."""

    _digest(predecessor_sha256, "predecessor SHA-256")
    core: dict[str, object] = {
        "schema_version": LEGACY_CONTRACT_SCHEMA,
        "classification": "PREPARED_NON_AUTHORIZING_SUCCESSOR",
        "state": "PREPARED_NOT_PUBLISHED_NOT_ACTIVE",
        "publication_layout": {
            "contract_path_template": (
                "state/alpha_ladder_registry/{contract_id}/universe_contract.json"
            ),
            "profile_path_template": (
                "state/alpha_ladder_registry/{contract_id}/alpha_tiered.yaml"
            ),
            "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
            "active_pointer_written_last": True,
        },
        "predecessor": {
            "path": predecessor_path,
            "sha256": predecessor_sha256,
            "preserved_byte_for_byte": True,
        },
        "authority": {
            "historical_rows": False,
            "registration": False,
            "execution": False,
            "holdout_2025": False,
            "provider_network_credentials": False,
            "trading": False,
        },
        "stages": {
            "tier_0": {
                "role": "SYNTHETIC_ENGINEERING_ONLY",
                "markets": ["ES"],
                "historical_years": [],
                "alpha_evidence": False,
            },
            "pilot": {
                "role": "GO_NO_GO_SCREEN_ONLY",
                "markets": ["ES"],
                "training_sessions": 504,
                "evaluation_sessions": 63,
                "fold_selection": "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD",
                "purge_and_embargo_required": True,
                "exact_session_ids_frozen_before_outcomes": True,
                "alpha_confirmation": False,
            },
            "tier_1": {
                "role": "FIRST_FORMAL_MULTI_MARKET_CONFIRMATION",
                "markets": list(CORE),
                "pilot_evaluation_sessions_excluded_for_every_market": True,
            },
            "tier_2": {
                "role": "FROZEN_BALANCED_REPLICATION",
                "markets": list(BALANCED),
                "report_core_and_additions_separately": True,
            },
            "tier_3": {
                "role": "FULL_UNIVERSE_REPLICATION",
                "markets": list(ALL_APPROVED),
                "traditional_markets": list(TRADITIONAL),
                "satellite_markets": list(SATELLITE),
                "traditional_must_pass_independently": True,
                "satellite_can_rescue_traditional_failure": False,
            },
            "holdout": {
                "role": "ONE_PROJECT_LEVEL_FINAL_HOLDOUT",
                "years": [2025],
                "maximum_accesses": 1,
                "terminal_tier": "tier_3",
            },
            "forward": {
                "role": "MONITORING_ONLY",
                "period": "2026_ONWARD",
                "can_rescue_failure": False,
            },
        },
        "transition_order": [
            "tier_0", "pilot", "tier_1", "tier_2", "tier_3", "holdout", "forward",
        ],
        "frozen_mechanism_fields": list(FROZEN_MECHANISM_FIELDS),
        "semantic_change": "NEW_COUNTED_MECHANISM_RESTARTS_AT_PILOT",
        "failed_higher_tier": "NO_FALLBACK_SCOPE_UNLESS_PREDECLARED_BEFORE_OUTCOMES",
        "missing_or_ambiguous_evidence": "FAIL_CLOSED",
    }
    return {**core, "contract_id": sha256_json(core)}


def build_contract(
    *, predecessor_path: str, predecessor_sha256: str,
    predecessor_contract_id: str,
) -> dict[str, object]:
    """Build the Tier-0-unified, inactive authoritative successor semantics."""

    _digest(predecessor_sha256, "predecessor SHA-256")
    _digest(predecessor_contract_id, "predecessor contract identity")
    core: dict[str, object] = {
        "schema_version": CONTRACT_SCHEMA,
        "classification": "PREPARED_NON_AUTHORIZING_SUCCESSOR",
        "state": "ACTIVE_ONLY_WHEN_REFERENCED_BY_VALID_POINTER",
        "publication_layout": {
            "contract_path_template": (
                "state/alpha_ladder_registry/{contract_id}/universe_contract.json"
            ),
            "profile_path_template": (
                "state/alpha_ladder_registry/{contract_id}/alpha_tiered.yaml"
            ),
            "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
            "active_pointer_written_last": True,
        },
        "predecessor": {
            "path": predecessor_path,
            "sha256": predecessor_sha256,
            "contract_id": predecessor_contract_id,
            "preserved_byte_for_byte": True,
        },
        "authority": {
            "historical_rows": False,
            "registration": False,
            "execution": False,
            "holdout_2025": False,
            "provider_network_credentials": False,
            "trading": False,
        },
        "stages": {
            "tier_0": {
                "role": "ENGINEERING_AND_ES_QUALIFICATION",
                "markets": ["ES"],
                "pass_requires_all_gates": [
                    "synthetic_engineering", "es_pilot",
                ],
                "alpha_confirmation": False,
                "gates": {
                    "synthetic_engineering": {
                        "role": "SYNTHETIC_ENGINEERING_ONLY",
                        "data": "SYNTHETIC_ONLY",
                        "historical_years": [],
                        "alpha_evidence": False,
                    },
                    "es_pilot": {
                        "role": "GO_NO_GO_SCREEN_ONLY",
                        "data": "ROW_CERTIFIED_REAL_HISTORY",
                        "training_sessions": 504,
                        "evaluation_sessions": 63,
                        "fold_selection": "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD",
                        "purge_and_embargo_required": True,
                        "exact_session_ids_frozen_before_outcomes": True,
                        "alpha_confirmation": False,
                    },
                },
            },
            "tier_1": {
                "role": "FIRST_FORMAL_MULTI_MARKET_CONFIRMATION",
                "markets": list(CORE),
                "tier_0_pilot_evaluation_sessions_excluded_for_every_market": True,
            },
            "tier_2": {
                "role": "FROZEN_BALANCED_REPLICATION",
                "markets": list(BALANCED),
                "report_core_and_additions_separately": True,
            },
            "tier_3": {
                "role": "FULL_UNIVERSE_REPLICATION",
                "markets": list(ALL_APPROVED),
                "traditional_markets": list(TRADITIONAL),
                "satellite_markets": list(SATELLITE),
                "traditional_must_pass_independently": True,
                "satellite_can_rescue_traditional_failure": False,
            },
            "holdout": {
                "role": "ONE_PROJECT_LEVEL_FINAL_HOLDOUT",
                "years": [2025],
                "maximum_accesses": 1,
                "terminal_tier": "tier_3",
            },
            "forward": {
                "role": "MONITORING_ONLY",
                "period": "2026_ONWARD",
                "can_rescue_failure": False,
            },
        },
        "transition_order": list(LEVELS),
        "tier_0_gate_order": ["synthetic_engineering", "es_pilot"],
        "operational_gate_identifiers": {
            "tier_0.synthetic_engineering": "tier_0",
            "tier_0.es_pilot": "pilot",
        },
        "gate_authority_separation_required": True,
        "frozen_mechanism_fields": list(FROZEN_MECHANISM_FIELDS),
        "semantic_change": "PILOT_LEVEL_MERGED_INTO_TIER_0_WITH_SEPARATE_GATES",
        "new_counted_mechanism_restart": "TIER_0_SYNTHETIC_ENGINEERING_GATE",
        "failed_tier_0_gate": "TIER_0_FAIL_FOR_EXACT_FROZEN_MECHANISM",
        "failed_higher_tier": "NO_FALLBACK_SCOPE_UNLESS_PREDECLARED_BEFORE_OUTCOMES",
        "missing_or_ambiguous_evidence": "FAIL_CLOSED",
    }
    return {**core, "contract_id": sha256_json(core)}


def build_qualification_successor_contract(
    *, predecessor_path: str, predecessor_sha256: str,
    predecessor_contract_id: str, predecessor_profile_id: str,
    predecessor_pointer_id: str, prior_failure_closure_id: str,
    prior_failed_mechanism_id: str,
    final_252_binding: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build the future-mechanism qualification successor without granting access."""

    for value, name in (
        (predecessor_sha256, "predecessor SHA-256"),
        (predecessor_contract_id, "predecessor contract identity"),
        (predecessor_profile_id, "predecessor profile identity"),
        (predecessor_pointer_id, "predecessor pointer identity"),
        (prior_failure_closure_id, "prior failure closure identity"),
        (prior_failed_mechanism_id, "prior failed mechanism identity"),
    ):
        _digest(value, name)
    final_stage = {
        "role": "FUTURE_FINAL_SEALED_EVALUATION_GATE",
        "status": (
            "SEALED_AUTHORITY_RESOLVED_ACCESS_FORBIDDEN"
            if final_252_binding is not None
            else "BLOCKED_FINAL_252_AUTHORITY_UNRESOLVED"
        ),
        "access_allowed": False,
        "manifest_binding": (
            dict(final_252_binding) if final_252_binding is not None else None
        ),
        "pointer_binding": None,
        "historical_summary_is_authority": False,
    }
    core: dict[str, object] = {
        "schema_version": (
            FINAL_252_CONTRACT_SCHEMA
            if final_252_binding is not None
            else QUALIFICATION_CONTRACT_SCHEMA
        ),
        "classification": "PRE_RESEARCH_DESIGN_PRIOR",
        "state": "ACTIVE_ONLY_WHEN_REFERENCED_BY_VALID_POINTER",
        "applies_to": "FUTURE_COUNTED_MECHANISMS_ONLY",
        "publication_layout": {
            "contract_path_template": (
                "state/alpha_ladder_registry/{contract_id}/universe_contract.json"
            ),
            "profile_path_template": (
                "state/alpha_ladder_registry/{contract_id}/alpha_tiered.yaml"
            ),
            "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
            "active_pointer_written_last": True,
        },
        "predecessor": {
            "path": predecessor_path,
            "sha256": predecessor_sha256,
            "contract_id": predecessor_contract_id,
            "profile_id": predecessor_profile_id,
            "pointer_id": predecessor_pointer_id,
            "preserved_byte_for_byte": True,
        },
        "authority": {
            "historical_rows": False,
            "registration": False,
            "execution": False,
            "final_252": False,
            "holdout_or_forward_values": False,
            "provider_network_credentials": False,
            "trading": False,
        },
        "stages": {
            "synthetic_engineering": {
                "role": "SYNTHETIC_ENGINEERING_ONLY",
                "alpha_claim": False,
                "real_history_evaluation": False,
                "mechanism_freeze_required_before_next_stage": True,
            },
            "tier_0": {
                "role": "ES_MANDATORY_CONTROLLED_REAL_HISTORY_PILOT",
                "evaluation_pack": ["ES"],
                "mandatory_pass": ["ES"],
                "failure_action": "CLOSE_MECHANISM",
                "rescue_markets": [],
                "nq_rescue_forbidden": True,
                "training_sessions": 504,
                "evaluation_sessions": 63,
                "fold_selection": "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD",
                "purge_and_embargo_required": True,
            },
            "tier_1": {
                "role": "FROZEN_INDEPENDENT_FAMILY_REPLICATION",
                "incremental_evaluation_pack": list(TIER1_FIXED_MARKETS),
                "macro_slot": {
                    "candidates": list(MACRO_CANDIDATES),
                    "selected_root": None,
                    "selection_status": "PENDING_PRE_RESULT_EXECUTION_GATE",
                },
                "cumulative_prior_result_reuse": ["ES"],
                "rerun_prior_tier_markets": False,
                "frozen_mechanism_required": True,
                "tuning_between_markets_forbidden": True,
                "aggregate_scores_authoritative": False,
            },
            "tier_2": {
                "role": "FROZEN_BALANCED_REPLICATION",
                "evaluation_pack": list(BALANCED),
                "reuse_certified_prior_tier_results": True,
                "report_per_market_and_family": True,
                "promotion_thresholds": "PRESERVE_PREDECESSOR_APPROVED_THRESHOLDS",
            },
            "tier_3": {
                "role": "FULL_STANDARD_UNIVERSE_REPLICATION",
                "evaluation_pack": list(ALL_APPROVED),
                "standard_markets": list(ALL_APPROVED),
                "traditional_markets": list(TRADITIONAL),
                "satellite_markets": list(SATELLITE),
                "deferred_micros": list(DEFERRED_MICROS),
                "traditional_must_pass_independently": True,
                "satellite_can_rescue_traditional_failure": False,
                "micros_can_rescue_failure": False,
                "micros_create_holdout": False,
            },
            "final_historical_evaluation": final_stage,
            "forward_monitoring": {
                "role": "MONITORING_ONLY_AFTER_ALL_SCIENTIFIC_AND_OPERATIONAL_GATES",
                "requires_alpha_and_live_execution_eligibility": True,
                "can_rescue_failure": False,
            },
        },
        "transition_order": [
            "synthetic_engineering", "tier_0", "tier_1", "tier_2", "tier_3",
            "final_historical_evaluation", "forward_monitoring",
        ],
        "operational_gate_identifiers": {
            "synthetic_engineering": "tier_0",
            "tier_0": "pilot",
            "tier_1": "tier_1",
            "tier_2": "tier_2",
            "tier_3": "tier_3",
            "final_historical_evaluation": "holdout",
            "forward_monitoring": "forward",
        },
        "family_validation": {
            "equity": {
                "anchor": "ES", "correlated_extension": "NQ",
                "maximum_independent_credit": 1, "nq_cannot_rescue_es": True,
            },
            "energy": {"anchor": "CL", "maximum_independent_credit": 1},
            "metals": {"anchor": "GC", "maximum_independent_credit": 1},
            "macro": {
                "candidates": list(MACRO_CANDIDATES), "selected_root": None,
                "selection_status": "PENDING_PRE_RESULT_EXECUTION_GATE",
                "maximum_independent_credit": 1,
            },
        },
        "per_market_qualification": {
            "alpha_status": list(ALPHA_STATUSES),
            "execution_proxy_status": list(EXECUTION_PROXY_STATUSES),
            "live_execution_status": list(LIVE_EXECUTION_STATUSES),
            "default_live_execution_status": "UNCLASSIFIED",
            "deployment_candidate_rule": (
                "alpha_status == PASS AND live_execution_status == APPROVED"
            ),
            "ohlcv_execution_proxy_does_not_certify": [
                "bid_ask_spread", "book_depth", "queue_position", "market_impact",
                "live_fill_quality",
            ],
        },
        "macro_selection_contract": {
            "candidates": list(MACRO_CANDIDATES),
            "allowed_evidence_fields": list(ALLOWED_MACRO_EVIDENCE_FIELDS),
            "forbidden_evidence_fields": list(FORBIDDEN_MACRO_EVIDENCE_FIELDS),
            "selection_must_precede_strategy_results": True,
            "both_pass_requires_pre_frozen_tie_breaker": True,
            "neither_pass_result": "NO_ELIGIBLE_MACRO_DIVERSIFIER",
            "silent_substitution_forbidden": True,
        },
        "mechanism_freeze": {
            "same_identity_required_through": ["tier_0", "tier_1", "tier_2", "tier_3"],
            "material_fields": [
                "features", "target", "horizon", "decision_timing", "entry_timing",
                "model_family", "thresholds", "costs", "sizing", "stops_risk_limits",
                "promotion_rules",
            ],
            "between_market_tuning_forbidden": True,
            "material_change_action": "NEW_COUNTED_MECHANISM_RESTARTS_AT_SYNTHETIC_ENGINEERING",
        },
        "prior_failed_mechanism": {
            "closure_id": prior_failure_closure_id,
            "mechanism_id": prior_failed_mechanism_id,
            "status": "CLOSED_NONRETRYABLE",
            "retroactive_reinterpretation": False,
        },
        "missing_or_ambiguous_evidence": "FAIL_CLOSED",
    }
    return {**core, "contract_id": sha256_json(core)}


def build_hypothesis_conditional_successor_contract(
    *, predecessor_path: str, predecessor_sha256: str,
    predecessor_contract_id: str, predecessor_profile_id: str,
    predecessor_pointer_id: str, prior_failure_closure_id: str,
    prior_failed_mechanism_id: str, final_252_binding: Mapping[str, object],
) -> dict[str, object]:
    """Make ladder gates conditional on the preregistered hypothesis scope."""

    final_252_binding = _mapping(final_252_binding, "final 252 manifest binding")
    prior = build_qualification_successor_contract(
        predecessor_path=predecessor_path,
        predecessor_sha256=predecessor_sha256,
        predecessor_contract_id=predecessor_contract_id,
        predecessor_profile_id=predecessor_profile_id,
        predecessor_pointer_id=predecessor_pointer_id,
        prior_failure_closure_id=prior_failure_closure_id,
        prior_failed_mechanism_id=prior_failed_mechanism_id,
        final_252_binding=final_252_binding,
    )
    core = {key: value for key, value in prior.items() if key != "contract_id"}
    core["schema_version"] = HYPOTHESIS_CONDITIONAL_CONTRACT_SCHEMA
    core["hypothesis_scope_policy"] = {
        "freeze_before": "HISTORICAL_OUTCOME_OR_OUTER_EVALUATION_ACCESS",
        "required_dimensions": [
            "MARKETS", "MARKET_FAMILIES", "ASSET_CLASSES", "SESSIONS",
        ],
        "scientific_replication_rule": "ONLY_PREREGISTERED_PREDICTED_SCOPE",
        "broad_generality_tiers": ["tier_2", "tier_3"],
        "missing_or_ambiguous_scope": "FAIL_CLOSED",
    }
    stages = core["stages"]
    assert isinstance(stages, dict)
    stages["tier_0"]["scientific_gate_condition"] = (
        "PREREGISTERED_PREDICTED_SCOPE_INCLUDES_ES"
    )
    stages["tier_0"]["es_outside_predicted_scope_result_use"] = (
        "ENGINEERING_ONLY_NOT_ALPHA_GATE"
    )
    stages["tier_1"]["applicability"] = (
        "ONLY_PREREGISTERED_PREDICTED_MARKETS_AND_FAMILIES"
    )
    for stage in ("tier_2", "tier_3"):
        stages[stage]["applicability"] = (
            "REQUIRED_ONLY_FOR_PREREGISTERED_BROAD_GENERALITY"
        )
    stages["final_historical_evaluation"]["predecessor_requirement"] = (
        "UNCHANGED_MECHANISM_PASSES_EVERY_APPLICABLE_SCOPE_CONDITIONAL_TIER"
    )
    return {**core, "contract_id": sha256_json(core)}


def build_bias_gate_successor_contract(
    *, predecessor_path: str, predecessor_sha256: str,
    predecessor_contract_id: str, predecessor_profile_id: str,
    predecessor_pointer_id: str, prior_failure_closure_id: str,
    prior_failed_mechanism_id: str, final_252_binding: Mapping[str, object],
    semantic_predecessor: Mapping[str, object],
) -> dict[str, object]:
    """Add the inactive Stage 2.5, challenge, and refit topology."""

    prior = build_hypothesis_conditional_successor_contract(
        predecessor_path=predecessor_path,
        predecessor_sha256=predecessor_sha256,
        predecessor_contract_id=predecessor_contract_id,
        predecessor_profile_id=predecessor_profile_id,
        predecessor_pointer_id=predecessor_pointer_id,
        prior_failure_closure_id=prior_failure_closure_id,
        prior_failed_mechanism_id=prior_failed_mechanism_id,
        final_252_binding=final_252_binding,
    )
    semantic_predecessor = _mapping(semantic_predecessor, "semantic predecessor")
    for key in ("path", "sha256", "contract_id", "profile_id", "pointer_id"):
        if not isinstance(semantic_predecessor.get(key), str) or not semantic_predecessor[key]:
            raise IntegrityError(f"semantic predecessor {key} is absent")
    _digest(semantic_predecessor["sha256"], "semantic predecessor SHA-256")
    for key in ("contract_id", "profile_id", "pointer_id"):
        _digest(semantic_predecessor[key], f"semantic predecessor {key}")

    core = {key: value for key, value in prior.items() if key != "contract_id"}
    core["schema_version"] = BIAS_GATE_CONTRACT_SCHEMA
    core["semantic_predecessor"] = dict(semantic_predecessor)
    core["causal_search_access_contract"] = {
        "required_schema": "causal_search_access_contract/1.0.0",
        "required_before": "OUTCOME_TARGET_FEATURE_OR_WFA_ROW_ACCESS",
        "registration_and_execution_binding_required": True,
        "material_change_action": "NEW_COUNTED_MECHANISM_RESTARTS_AT_STAGE_2_5",
    }
    stages = core["stages"]
    assert isinstance(stages, dict)
    stages["recent_regime_challenge"] = {
        "role": "ONE_SHOT_SCOPE_WIDE_RECENT_REGIME_CHALLENGE",
        "start_inclusive": "2023-01-01T00:00:00Z",
        "end_exclusive": "2025-07-13T22:00:00Z",
        "after": "LAST_APPLICABLE_SCOPE_MATCHED_REPLICATION_STAGE",
        "maximum_attempts": 1,
        "same_frozen_candidate_required": True,
        "failure_action": "CLOSE_MECHANISM_NO_RETRY",
    }
    stages["deterministic_refit"] = {
        "role": "FROZEN_ALGORITHM_REFIT_WITHOUT_EVALUATION",
        "training_start_inclusive": "2018-01-01T00:00:00Z",
        "training_end_exclusive": "2025-07-13T22:00:00Z",
        "fit_parameters_only": True,
        "performance_evaluation_allowed": False,
        "required_predecessor": "recent_regime_challenge",
    }
    stages["final_historical_evaluation"]["predecessor_requirement"] = (
        "DETERMINISTIC_REFIT_COMPLETE_NO_EVALUATION"
    )
    mechanism_freeze = core["mechanism_freeze"]
    assert isinstance(mechanism_freeze, dict)
    mechanism_freeze["same_identity_required_through"] = [
        "tier_0", "tier_1", "tier_2", "tier_3", "recent_regime_challenge",
    ]
    core["transition_order"] = [
        "synthetic_engineering", "tier_0", "tier_1", "tier_2", "tier_3",
        "recent_regime_challenge", "deterministic_refit",
        "final_historical_evaluation", "forward_monitoring",
    ]
    return {**core, "contract_id": sha256_json(core)}


def build_bias_remediation_successor_contract(
    *, predecessor_path: str, predecessor_sha256: str,
    predecessor_contract_id: str, predecessor_profile_id: str,
    predecessor_pointer_id: str, prior_failure_closure_id: str,
    prior_failed_mechanism_id: str, final_252_binding: Mapping[str, object],
    semantic_predecessor: Mapping[str, object],
) -> dict[str, object]:
    """Close Stage 3+, challenge, refit, claim, and forward prerequisites."""

    prior = build_bias_gate_successor_contract(
        predecessor_path=predecessor_path,
        predecessor_sha256=predecessor_sha256,
        predecessor_contract_id=predecessor_contract_id,
        predecessor_profile_id=predecessor_profile_id,
        predecessor_pointer_id=predecessor_pointer_id,
        prior_failure_closure_id=prior_failure_closure_id,
        prior_failed_mechanism_id=prior_failed_mechanism_id,
        final_252_binding=final_252_binding,
        semantic_predecessor=semantic_predecessor,
    )
    core = {key: value for key, value in prior.items() if key != "contract_id"}
    core["schema_version"] = BIAS_REMEDIATION_CONTRACT_SCHEMA
    core["causal_search_access_contract"] = {
        "required_schema": "causal_search_access_contract/2.0.0",
        "required_before": "ANY_STAGE_3_PLUS_RESULT_BEARING_ROW_OPENER",
        "registration_and_execution_binding_required": True,
        "pre_evaluation_declaration_required": True,
        "legacy_active_action": "BLOCKED_STAGE_2_5_ACTIVE_AUTHORITY_REQUIRED",
        "material_change_action": "NEW_COUNTED_MECHANISM_RESTARTS_AT_STAGE_2_5_INHERIT_HISTORY",
    }
    core["macro_disposition"] = "NO_ELIGIBLE_MACRO_DIVERSIFIER"
    family_validation = core["family_validation"]
    assert isinstance(family_validation, dict)
    macro = family_validation["macro"]
    assert isinstance(macro, dict)
    macro["selection_status"] = "NO_ELIGIBLE_MACRO_DIVERSIFIER"
    macro["selected_root"] = None
    stages = core["stages"]
    assert isinstance(stages, dict)
    tier_1 = stages["tier_1"]
    assert isinstance(tier_1, dict)
    macro_slot = tier_1["macro_slot"]
    assert isinstance(macro_slot, dict)
    macro_slot["selection_status"] = "NO_ELIGIBLE_MACRO_DIVERSIFIER"
    macro_slot["selected_root"] = None
    mechanism_freeze = core["mechanism_freeze"]
    assert isinstance(mechanism_freeze, dict)
    mechanism_freeze["material_change_action"] = (
        "NEW_COUNTED_MECHANISM_RESTARTS_AT_STAGE_2_5_INHERIT_HISTORY"
    )
    core["governance_gates"] = {
        "pre_evaluation_declaration": "pre_evaluation_declaration/1.0.0",
        "challenge_claim": "recent_regime_program_claim/1.0.0",
        "refit_contract": "deterministic_refit_contract/1.0.0",
        "refit_output": "deterministic_refit_parameter_output/1.0.0",
        "claim_ceiling": "ROBUST_PREDICTIVE_ASSOCIATION",
        "causal_claim_action": "BLOCKED_CAUSAL_IDENTIFICATION_CONTRACT_REQUIRED",
        "forward_protocol": "forward_monitoring_protocol/1.0.0",
        "missing_forward_protocol_action": "BLOCKED_FORWARD_MONITORING_PROTOCOL_NOT_FROZEN",
    }
    return {**core, "contract_id": sha256_json(core)}


def build_canonical_bias_remediation_successor_contract(
    predecessor: Mapping[str, object], *, predecessor_path: str,
    predecessor_sha256: str, stage_2_5_binding: Mapping[str, object],
) -> dict[str, object]:
    """Rebind the validated v8 semantics to canonical Stage 2.5 authority."""

    if predecessor.get("schema_version") != BIAS_REMEDIATION_CONTRACT_SCHEMA:
        raise IntegrityError("canonical bias successor requires a schema-v8 predecessor")
    binding = _mapping(stage_2_5_binding, "Stage 2.5 binding")
    if set(binding) != {"contract_id", "path", "sha256", "schema_version"}:
        raise IntegrityError("Stage 2.5 binding keys are not exact")
    core = {key: value for key, value in predecessor.items() if key != "contract_id"}
    core["schema_version"] = CANONICAL_BIAS_REMEDIATION_CONTRACT_SCHEMA
    core["canonical_predecessor"] = {
        "contract_id": predecessor["contract_id"], "path": predecessor_path,
        "sha256": predecessor_sha256,
        "supersession_reason": "NONCANONICAL_TERMINAL_NEWLINE",
    }
    gate = dict(_mapping(core["causal_search_access_contract"], "Stage 2.5 gate"))
    gate["required_schema"] = str(binding["schema_version"])
    gate["contract_binding"] = dict(binding)
    core["causal_search_access_contract"] = gate
    return {**core, "contract_id": sha256_json(core)}


def validate_contract(payload: Mapping[str, object]) -> dict[str, object]:
    schema = payload.get("schema_version")
    if schema not in {
        LEGACY_CONTRACT_SCHEMA, CONTRACT_SCHEMA, QUALIFICATION_CONTRACT_SCHEMA,
        FINAL_252_CONTRACT_SCHEMA, HYPOTHESIS_CONDITIONAL_CONTRACT_SCHEMA,
        BIAS_GATE_CONTRACT_SCHEMA, BIAS_REMEDIATION_CONTRACT_SCHEMA,
        CANONICAL_BIAS_REMEDIATION_CONTRACT_SCHEMA,
    }:
        raise IntegrityError("unsupported Alpha ladder contract schema")
    assert isinstance(schema, str)
    _identity(payload, "contract_id", schema)
    stages = _mapping(payload.get("stages"), "stages")
    authority = _mapping(payload.get("authority"), "authority")
    if any(value is not False for value in authority.values()):
        raise IntegrityError("ladder contract cannot grant authority")
    predecessor = _mapping(payload.get("predecessor"), "predecessor")
    if schema == LEGACY_CONTRACT_SCHEMA:
        expected = _build_legacy_contract(
            predecessor_path=str(predecessor.get("path", "")),
            predecessor_sha256=str(predecessor.get("sha256", "")),
        )
        expected_stages = {"tier_0", *STAGES}
    elif schema == CONTRACT_SCHEMA:
        expected = build_contract(
            predecessor_path=str(predecessor.get("path", "")),
            predecessor_sha256=str(predecessor.get("sha256", "")),
            predecessor_contract_id=str(predecessor.get("contract_id", "")),
        )
        expected_stages = set(LEVELS)
    elif schema in {QUALIFICATION_CONTRACT_SCHEMA, FINAL_252_CONTRACT_SCHEMA}:
        prior_failure = _mapping(
            payload.get("prior_failed_mechanism"), "prior failed mechanism",
        )
        expected = build_qualification_successor_contract(
            predecessor_path=str(predecessor.get("path", "")),
            predecessor_sha256=str(predecessor.get("sha256", "")),
            predecessor_contract_id=str(predecessor.get("contract_id", "")),
            predecessor_profile_id=str(predecessor.get("profile_id", "")),
            predecessor_pointer_id=str(predecessor.get("pointer_id", "")),
            prior_failure_closure_id=str(prior_failure.get("closure_id", "")),
            prior_failed_mechanism_id=str(prior_failure.get("mechanism_id", "")),
            final_252_binding=(
                _mapping(
                    stages.get("final_historical_evaluation"),
                    "final historical evaluation",
                ).get("manifest_binding")
                if schema == FINAL_252_CONTRACT_SCHEMA
                else None
            ),
        )
        expected_stages = {
            "synthetic_engineering", "tier_0", "tier_1", "tier_2", "tier_3",
            "final_historical_evaluation", "forward_monitoring",
        }
    elif schema == HYPOTHESIS_CONDITIONAL_CONTRACT_SCHEMA:
        prior_failure = _mapping(
            payload.get("prior_failed_mechanism"), "prior failed mechanism",
        )
        expected = build_hypothesis_conditional_successor_contract(
            predecessor_path=str(predecessor.get("path", "")),
            predecessor_sha256=str(predecessor.get("sha256", "")),
            predecessor_contract_id=str(predecessor.get("contract_id", "")),
            predecessor_profile_id=str(predecessor.get("profile_id", "")),
            predecessor_pointer_id=str(predecessor.get("pointer_id", "")),
            prior_failure_closure_id=str(prior_failure.get("closure_id", "")),
            prior_failed_mechanism_id=str(prior_failure.get("mechanism_id", "")),
            final_252_binding=_mapping(
                _mapping(
                    stages.get("final_historical_evaluation"),
                    "final historical evaluation",
                ).get("manifest_binding"),
                "final 252 manifest binding",
            ),
        )
        expected_stages = {
            "synthetic_engineering", "tier_0", "tier_1", "tier_2", "tier_3",
            "final_historical_evaluation", "forward_monitoring",
        }
    elif schema == BIAS_GATE_CONTRACT_SCHEMA:
        prior_failure = _mapping(
            payload.get("prior_failed_mechanism"), "prior failed mechanism",
        )
        expected = build_bias_gate_successor_contract(
            predecessor_path=str(predecessor.get("path", "")),
            predecessor_sha256=str(predecessor.get("sha256", "")),
            predecessor_contract_id=str(predecessor.get("contract_id", "")),
            predecessor_profile_id=str(predecessor.get("profile_id", "")),
            predecessor_pointer_id=str(predecessor.get("pointer_id", "")),
            prior_failure_closure_id=str(prior_failure.get("closure_id", "")),
            prior_failed_mechanism_id=str(prior_failure.get("mechanism_id", "")),
            final_252_binding=_mapping(
                _mapping(
                    stages.get("final_historical_evaluation"),
                    "final historical evaluation",
                ).get("manifest_binding"),
                "final 252 manifest binding",
            ),
            semantic_predecessor=_mapping(
                payload.get("semantic_predecessor"), "semantic predecessor",
            ),
        )
        expected_stages = {
            "synthetic_engineering", "tier_0", "tier_1", "tier_2", "tier_3",
            "recent_regime_challenge", "deterministic_refit",
            "final_historical_evaluation", "forward_monitoring",
        }
    elif schema == BIAS_REMEDIATION_CONTRACT_SCHEMA:
        prior_failure = _mapping(
            payload.get("prior_failed_mechanism"), "prior failed mechanism",
        )
        expected = build_bias_remediation_successor_contract(
            predecessor_path=str(predecessor.get("path", "")),
            predecessor_sha256=str(predecessor.get("sha256", "")),
            predecessor_contract_id=str(predecessor.get("contract_id", "")),
            predecessor_profile_id=str(predecessor.get("profile_id", "")),
            predecessor_pointer_id=str(predecessor.get("pointer_id", "")),
            prior_failure_closure_id=str(prior_failure.get("closure_id", "")),
            prior_failed_mechanism_id=str(prior_failure.get("mechanism_id", "")),
            final_252_binding=_mapping(
                _mapping(stages.get("final_historical_evaluation"), "final historical evaluation").get("manifest_binding"),
                "final 252 manifest binding",
            ),
            semantic_predecessor=_mapping(payload.get("semantic_predecessor"), "semantic predecessor"),
        )
        expected_stages = {
            "synthetic_engineering", "tier_0", "tier_1", "tier_2", "tier_3",
            "recent_regime_challenge", "deterministic_refit",
            "final_historical_evaluation", "forward_monitoring",
        }
    else:
        canonical_predecessor = _mapping(payload.get("canonical_predecessor"), "canonical predecessor")
        gate = _mapping(payload.get("causal_search_access_contract"), "Stage 2.5 gate")
        binding = _mapping(gate.get("contract_binding"), "Stage 2.5 binding")
        prior_core = {
            key: value for key, value in payload.items()
            if key not in {"contract_id", "canonical_predecessor"}
        }
        prior_core["schema_version"] = BIAS_REMEDIATION_CONTRACT_SCHEMA
        prior_gate = dict(gate)
        prior_gate.pop("contract_binding", None)
        prior_gate["required_schema"] = "causal_search_access_contract/2.0.0"
        prior_core["causal_search_access_contract"] = prior_gate
        prior = {**prior_core, "contract_id": sha256_json(prior_core)}
        if prior["contract_id"] != canonical_predecessor.get("contract_id"):
            raise IntegrityError("canonical ladder predecessor identity changed")
        validate_contract(prior)
        expected = build_canonical_bias_remediation_successor_contract(
            prior,
            predecessor_path=str(canonical_predecessor.get("path", "")),
            predecessor_sha256=str(canonical_predecessor.get("sha256", "")),
            stage_2_5_binding=binding,
        )
        expected_stages = {
            "synthetic_engineering", "tier_0", "tier_1", "tier_2", "tier_3",
            "recent_regime_challenge", "deterministic_refit",
            "final_historical_evaluation", "forward_monitoring",
        }
    if dict(payload) != expected:
        raise IntegrityError("ladder contract semantics drifted")
    if set(stages) != expected_stages:
        raise IntegrityError("ladder stage topology drifted")
    return dict(payload)


def _build_legacy_profile(
    *, contract_path: str, contract_sha256: str, contract_id: str,
) -> dict[str, object]:
    _digest(contract_sha256, "contract SHA-256")
    _digest(contract_id, "contract identity")
    core: dict[str, object] = {
        "schema_version": LEGACY_PROFILE_SCHEMA,
        "classification": "PREPARED_NON_AUTHORIZING_OPERATIONAL_VIEW",
        "state": "PREPARED_NOT_ACTIVE",
        "contract_binding": {
            "path": contract_path,
            "sha256": contract_sha256,
            "contract_id": contract_id,
        },
        "market_sets": {
            "core": list(CORE),
            "balanced": list(BALANCED),
            "traditional": list(TRADITIONAL),
            "satellite": list(SATELLITE),
            "all_approved": list(ALL_APPROVED),
        },
        "profiles": {
            "tier_0": {"markets": ["ES"], "data": "SYNTHETIC_ONLY"},
            "pilot": {
                "markets": ["ES"],
                "training_sessions": 504,
                "evaluation_sessions": 63,
                "result_use": "ADVANCE_OR_REJECT_EXACT_FROZEN_MECHANISM_ONLY",
            },
            "tier_1": {"market_set": "core"},
            "tier_2": {"market_set": "balanced"},
            "tier_3": {
                "market_set": "all_approved",
                "mandatory_subgroups": ["traditional", "satellite"],
                "traditional_must_pass_independently": True,
                "satellite_can_rescue_traditional_failure": False,
            },
            "holdout": {"years": [2025], "maximum_accesses": 1},
            "forward": {"period": "2026_ONWARD", "monitoring_only": True},
        },
        "authority": {
            "historical_rows": False,
            "registration": False,
            "execution": False,
            "holdout_2025": False,
            "provider_network_credentials": False,
            "trading": False,
        },
    }
    return {**core, "profile_id": sha256_json(core)}


def build_profile(
    *, contract_path: str, contract_sha256: str, contract_id: str,
) -> dict[str, object]:
    _digest(contract_sha256, "contract SHA-256")
    _digest(contract_id, "contract identity")
    core: dict[str, object] = {
        "schema_version": PROFILE_SCHEMA,
        "classification": "PREPARED_NON_AUTHORIZING_OPERATIONAL_VIEW",
        "state": "ACTIVE_ONLY_WHEN_REFERENCED_BY_VALID_POINTER",
        "contract_binding": {
            "path": contract_path,
            "sha256": contract_sha256,
            "contract_id": contract_id,
        },
        "market_sets": {
            "core": list(CORE),
            "balanced": list(BALANCED),
            "traditional": list(TRADITIONAL),
            "satellite": list(SATELLITE),
            "all_approved": list(ALL_APPROVED),
        },
        "profiles": {
            "tier_0": {
                "markets": ["ES"],
                "pass_requires_all_gates": [
                    "synthetic_engineering", "es_pilot",
                ],
                "alpha_confirmation": False,
                "gates": {
                    "synthetic_engineering": {
                        "data": "SYNTHETIC_ONLY",
                        "result_use": "MECHANICS_VALIDATION_ONLY",
                    },
                    "es_pilot": {
                        "data": "ROW_CERTIFIED_REAL_HISTORY",
                        "training_sessions": 504,
                        "evaluation_sessions": 63,
                        "result_use": "ADVANCE_OR_REJECT_EXACT_FROZEN_MECHANISM_ONLY",
                    },
                },
            },
            "tier_1": {"market_set": "core"},
            "tier_2": {"market_set": "balanced"},
            "tier_3": {
                "market_set": "all_approved",
                "mandatory_subgroups": ["traditional", "satellite"],
                "traditional_must_pass_independently": True,
                "satellite_can_rescue_traditional_failure": False,
            },
            "holdout": {"years": [2025], "maximum_accesses": 1},
            "forward": {"period": "2026_ONWARD", "monitoring_only": True},
        },
        "operational_gate_identifiers": {
            "tier_0.synthetic_engineering": "tier_0",
            "tier_0.es_pilot": "pilot",
        },
        "authority": {
            "historical_rows": False,
            "registration": False,
            "execution": False,
            "holdout_2025": False,
            "provider_network_credentials": False,
            "trading": False,
        },
    }
    return {**core, "profile_id": sha256_json(core)}


def build_qualification_successor_profile(
    *, contract_path: str, contract_sha256: str, contract_id: str,
    final_252_binding: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build the operational view for qualification-contract schema v4."""

    _digest(contract_sha256, "contract SHA-256")
    _digest(contract_id, "contract identity")
    core: dict[str, object] = {
        "schema_version": (
            FINAL_252_PROFILE_SCHEMA
            if final_252_binding is not None
            else QUALIFICATION_PROFILE_SCHEMA
        ),
        "classification": "PRE_RESEARCH_DESIGN_PRIOR",
        "state": "ACTIVE_ONLY_WHEN_REFERENCED_BY_VALID_POINTER",
        "applies_to": "FUTURE_COUNTED_MECHANISMS_ONLY",
        "contract_binding": {
            "path": contract_path,
            "sha256": contract_sha256,
            "contract_id": contract_id,
        },
        "market_sets": {
            "tier_0": ["ES"],
            "tier_1_fixed": list(TIER1_FIXED_MARKETS),
            "macro_candidates": list(MACRO_CANDIDATES),
            "balanced": list(BALANCED),
            "traditional": list(TRADITIONAL),
            "satellite": list(SATELLITE),
            "standard": list(ALL_APPROVED),
            "deferred_micros": list(DEFERRED_MICROS),
        },
        "profiles": {
            "synthetic_engineering": {
                "data": "SYNTHETIC_ONLY",
                "alpha_claim": False,
                "mechanism_freeze_required_before_next_stage": True,
            },
            "tier_0": {
                "evaluation_pack": ["ES"],
                "mandatory_pass": ["ES"],
                "failure_action": "CLOSE_MECHANISM",
                "nq_rescue_forbidden": True,
            },
            "tier_1": {
                "incremental_market_set": "tier_1_fixed",
                "macro_candidates_market_set": "macro_candidates",
                "macro_selection_status": "PENDING_PRE_RESULT_EXECUTION_GATE",
                "cumulative_prior_result_reuse": ["ES"],
                "same_frozen_mechanism_required": True,
                "between_market_tuning_forbidden": True,
            },
            "tier_2": {
                "market_set": "balanced",
                "preserve_predecessor_promotion_thresholds": True,
                "report_per_market_and_family": True,
            },
            "tier_3": {
                "market_set": "standard",
                "mandatory_subgroups": ["traditional", "satellite"],
                "traditional_must_pass_independently": True,
                "satellite_can_rescue_traditional_failure": False,
                "deferred_micros_can_rescue_failure": False,
                "deferred_micros_create_holdout": False,
            },
            "final_historical_evaluation": {
                "status": (
                    "SEALED_AUTHORITY_RESOLVED_ACCESS_FORBIDDEN"
                    if final_252_binding is not None
                    else "BLOCKED_FINAL_252_AUTHORITY_UNRESOLVED"
                ),
                "access_allowed": False,
                **(
                    {"manifest_binding": dict(final_252_binding)}
                    if final_252_binding is not None
                    else {}
                ),
            },
            "forward_monitoring": {
                "requires_alpha_and_live_execution_eligibility": True,
                "monitoring_only": True,
            },
        },
        "family_validation": {
            "equity": {
                "anchor": "ES", "correlated_extension": "NQ",
                "maximum_independent_credit": 1, "nq_cannot_rescue_es": True,
            },
            "energy": {"anchor": "CL", "maximum_independent_credit": 1},
            "metals": {"anchor": "GC", "maximum_independent_credit": 1},
            "macro": {
                "candidates": list(MACRO_CANDIDATES), "selected_root": None,
                "selection_status": "PENDING_PRE_RESULT_EXECUTION_GATE",
                "maximum_independent_credit": 1,
            },
        },
        "per_market_qualification": {
            "default_alpha_status": "NOT_EVALUATED",
            "default_execution_proxy_status": "NOT_EVALUATED",
            "default_live_execution_status": "UNCLASSIFIED",
            "deployment_candidate_rule": (
                "alpha_status == PASS AND live_execution_status == APPROVED"
            ),
        },
        "operational_gate_identifiers": {
            "synthetic_engineering": "tier_0",
            "tier_0": "pilot",
            "tier_1": "tier_1",
            "tier_2": "tier_2",
            "tier_3": "tier_3",
            "final_historical_evaluation": "holdout",
            "forward_monitoring": "forward",
        },
        "authority": {
            "historical_rows": False,
            "registration": False,
            "execution": False,
            "final_252": False,
            "holdout_or_forward_values": False,
            "provider_network_credentials": False,
            "trading": False,
        },
    }
    return {**core, "profile_id": sha256_json(core)}


def build_hypothesis_conditional_successor_profile(
    *, contract_path: str, contract_sha256: str, contract_id: str,
    final_252_binding: Mapping[str, object],
) -> dict[str, object]:
    """Build the scope-conditional operational view for schema v6."""

    final_252_binding = _mapping(final_252_binding, "final 252 manifest binding")
    prior = build_qualification_successor_profile(
        contract_path=contract_path,
        contract_sha256=contract_sha256,
        contract_id=contract_id,
        final_252_binding=final_252_binding,
    )
    core = {key: value for key, value in prior.items() if key != "profile_id"}
    core["schema_version"] = HYPOTHESIS_CONDITIONAL_PROFILE_SCHEMA
    core["hypothesis_scope_policy"] = {
        "freeze_before": "HISTORICAL_OUTCOME_OR_OUTER_EVALUATION_ACCESS",
        "required_dimensions": [
            "MARKETS", "MARKET_FAMILIES", "ASSET_CLASSES", "SESSIONS",
        ],
        "scientific_replication_rule": "ONLY_PREREGISTERED_PREDICTED_SCOPE",
        "broad_generality_tiers": ["tier_2", "tier_3"],
        "missing_or_ambiguous_scope": "FAIL_CLOSED",
    }
    profiles = core["profiles"]
    assert isinstance(profiles, dict)
    profiles["tier_0"]["scientific_gate_condition"] = (
        "PREREGISTERED_PREDICTED_SCOPE_INCLUDES_ES"
    )
    profiles["tier_0"]["es_outside_predicted_scope_result_use"] = (
        "ENGINEERING_ONLY_NOT_ALPHA_GATE"
    )
    profiles["tier_1"]["applicability"] = (
        "ONLY_PREREGISTERED_PREDICTED_MARKETS_AND_FAMILIES"
    )
    for stage in ("tier_2", "tier_3"):
        profiles[stage]["applicability"] = (
            "REQUIRED_ONLY_FOR_PREREGISTERED_BROAD_GENERALITY"
        )
    profiles["final_historical_evaluation"]["predecessor_requirement"] = (
        "UNCHANGED_MECHANISM_PASSES_EVERY_APPLICABLE_SCOPE_CONDITIONAL_TIER"
    )
    return {**core, "profile_id": sha256_json(core)}


def build_bias_gate_successor_profile(
    *, contract_path: str, contract_sha256: str, contract_id: str,
    final_252_binding: Mapping[str, object],
) -> dict[str, object]:
    """Build the inactive operational view for the Stage 2.5 successor."""

    prior = build_hypothesis_conditional_successor_profile(
        contract_path=contract_path,
        contract_sha256=contract_sha256,
        contract_id=contract_id,
        final_252_binding=final_252_binding,
    )
    core = {key: value for key, value in prior.items() if key != "profile_id"}
    core["schema_version"] = BIAS_GATE_PROFILE_SCHEMA
    core["causal_search_access_contract"] = {
        "required_schema": "causal_search_access_contract/1.0.0",
        "required_before": "OUTCOME_TARGET_FEATURE_OR_WFA_ROW_ACCESS",
        "registration_and_execution_binding_required": True,
    }
    profiles = core["profiles"]
    assert isinstance(profiles, dict)
    profiles["recent_regime_challenge"] = {
        "maximum_attempts": 1,
        "period": "2023-01-01T00:00:00Z_TO_2025-07-13T22:00:00Z",
        "same_frozen_candidate_required": True,
        "failure_action": "CLOSE_MECHANISM_NO_RETRY",
    }
    profiles["deterministic_refit"] = {
        "period": "2018-01-01T00:00:00Z_TO_2025-07-13T22:00:00Z",
        "fit_parameters_only": True,
        "performance_evaluation_allowed": False,
        "required_status": "REFIT_COMPLETE_NO_EVALUATION",
    }
    profiles["final_historical_evaluation"]["predecessor_requirement"] = (
        "DETERMINISTIC_REFIT_COMPLETE_NO_EVALUATION"
    )
    core["operational_gate_identifiers"] = {
        **core["operational_gate_identifiers"],
        "recent_regime_challenge": "challenge",
        "deterministic_refit": "refit",
    }
    return {**core, "profile_id": sha256_json(core)}


def build_bias_remediation_successor_profile(
    *, contract_path: str, contract_sha256: str, contract_id: str,
    final_252_binding: Mapping[str, object],
) -> dict[str, object]:
    prior = build_bias_gate_successor_profile(
        contract_path=contract_path,
        contract_sha256=contract_sha256,
        contract_id=contract_id,
        final_252_binding=final_252_binding,
    )
    core = {key: value for key, value in prior.items() if key != "profile_id"}
    core["schema_version"] = BIAS_REMEDIATION_PROFILE_SCHEMA
    core["causal_search_access_contract"] = {
        "required_schema": "causal_search_access_contract/2.0.0",
        "required_before": "ANY_STAGE_3_PLUS_RESULT_BEARING_ROW_OPENER",
        "registration_and_execution_binding_required": True,
        "pre_evaluation_declaration_required": True,
    }
    core["macro_disposition"] = "NO_ELIGIBLE_MACRO_DIVERSIFIER"
    family_validation = core["family_validation"]
    assert isinstance(family_validation, dict)
    macro = family_validation["macro"]
    assert isinstance(macro, dict)
    macro["selection_status"] = "NO_ELIGIBLE_MACRO_DIVERSIFIER"
    macro["selected_root"] = None
    profiles = core["profiles"]
    assert isinstance(profiles, dict)
    tier_1 = profiles["tier_1"]
    assert isinstance(tier_1, dict)
    tier_1["macro_selection_status"] = "NO_ELIGIBLE_MACRO_DIVERSIFIER"
    core["governance_gates"] = {
        "challenge": "recent_regime_program_claim/1.0.0",
        "refit": "deterministic_refit_contract/1.0.0",
        "claim_ceiling": "ROBUST_PREDICTIVE_ASSOCIATION",
        "forward": "forward_monitoring_protocol/1.0.0",
    }
    return {**core, "profile_id": sha256_json(core)}


def build_canonical_bias_remediation_successor_profile(
    predecessor: Mapping[str, object], *, predecessor_path: str,
    predecessor_sha256: str, contract_path: str, contract_sha256: str,
    contract_id: str, stage_2_5_binding: Mapping[str, object],
) -> dict[str, object]:
    if predecessor.get("schema_version") != BIAS_REMEDIATION_PROFILE_SCHEMA:
        raise IntegrityError("canonical bias profile requires a schema-v8 predecessor")
    core = {key: value for key, value in predecessor.items() if key != "profile_id"}
    core["schema_version"] = CANONICAL_BIAS_REMEDIATION_PROFILE_SCHEMA
    core["canonical_predecessor"] = {
        "profile_id": predecessor["profile_id"], "path": predecessor_path,
        "sha256": predecessor_sha256,
        "supersession_reason": "NONCANONICAL_TERMINAL_NEWLINE",
    }
    core["contract_binding"] = {
        "path": contract_path, "sha256": contract_sha256, "contract_id": contract_id,
    }
    gate = dict(_mapping(core["causal_search_access_contract"], "Stage 2.5 gate"))
    gate["required_schema"] = str(stage_2_5_binding["schema_version"])
    gate["contract_binding"] = dict(stage_2_5_binding)
    core["causal_search_access_contract"] = gate
    return {**core, "profile_id": sha256_json(core)}


def validate_profile(
    payload: Mapping[str, object], *, root: Path,
    prepared_contract_path: Path | None = None,
) -> dict[str, object]:
    schema = payload.get("schema_version")
    if schema not in {
        LEGACY_PROFILE_SCHEMA, PROFILE_SCHEMA, QUALIFICATION_PROFILE_SCHEMA,
        FINAL_252_PROFILE_SCHEMA, HYPOTHESIS_CONDITIONAL_PROFILE_SCHEMA,
        BIAS_GATE_PROFILE_SCHEMA, BIAS_REMEDIATION_PROFILE_SCHEMA,
        CANONICAL_BIAS_REMEDIATION_PROFILE_SCHEMA,
    }:
        raise IntegrityError("unsupported Alpha ladder profile schema")
    assert isinstance(schema, str)
    _identity(payload, "profile_id", schema)
    binding = _mapping(payload.get("contract_binding"), "contract binding")
    contract_path = (
        prepared_contract_path.resolve(strict=False)
        if prepared_contract_path is not None
        else contained_path(root, str(binding.get("path", "")))
    )
    if sha256_file(contract_path) != _digest(binding.get("sha256"), "contract SHA-256"):
        raise IntegrityError("profile contract binding changed")
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    if not isinstance(contract, dict):
        raise IntegrityError("profile contract is malformed")
    validate_contract(contract)
    expected_contract_schema = {
        LEGACY_PROFILE_SCHEMA: LEGACY_CONTRACT_SCHEMA,
        PROFILE_SCHEMA: CONTRACT_SCHEMA,
        QUALIFICATION_PROFILE_SCHEMA: QUALIFICATION_CONTRACT_SCHEMA,
        FINAL_252_PROFILE_SCHEMA: FINAL_252_CONTRACT_SCHEMA,
        HYPOTHESIS_CONDITIONAL_PROFILE_SCHEMA: HYPOTHESIS_CONDITIONAL_CONTRACT_SCHEMA,
        BIAS_GATE_PROFILE_SCHEMA: BIAS_GATE_CONTRACT_SCHEMA,
        BIAS_REMEDIATION_PROFILE_SCHEMA: BIAS_REMEDIATION_CONTRACT_SCHEMA,
        CANONICAL_BIAS_REMEDIATION_PROFILE_SCHEMA: CANONICAL_BIAS_REMEDIATION_CONTRACT_SCHEMA,
    }[schema]
    if contract.get("schema_version") != expected_contract_schema:
        raise IntegrityError("ladder profile and contract schema generations differ")
    if binding.get("contract_id") != contract.get("contract_id"):
        raise IntegrityError("profile contract identity changed")
    if schema == CANONICAL_BIAS_REMEDIATION_PROFILE_SCHEMA:
        predecessor = _mapping(payload.get("canonical_predecessor"), "canonical predecessor")
        gate = _mapping(payload.get("causal_search_access_contract"), "Stage 2.5 gate")
        prior_core = {
            key: value for key, value in payload.items()
            if key not in {"profile_id", "canonical_predecessor"}
        }
        prior_core["schema_version"] = BIAS_REMEDIATION_PROFILE_SCHEMA
        prior_gate = dict(gate)
        prior_gate.pop("contract_binding", None)
        prior_gate["required_schema"] = "causal_search_access_contract/2.0.0"
        prior_core["causal_search_access_contract"] = prior_gate
        old_binding = dict(_mapping(prior_core["contract_binding"], "old contract binding"))
        contract_predecessor = _mapping(contract.get("canonical_predecessor"), "contract predecessor")
        old_binding.update({
            "path": contract_predecessor.get("path"),
            "sha256": contract_predecessor.get("sha256"),
            "contract_id": contract_predecessor.get("contract_id"),
        })
        prior_core["contract_binding"] = old_binding
        prior = {**prior_core, "profile_id": sha256_json(prior_core)}
        if prior["profile_id"] != predecessor.get("profile_id"):
            raise IntegrityError("canonical profile predecessor identity changed")
        expected = build_canonical_bias_remediation_successor_profile(
            prior,
            predecessor_path=str(predecessor.get("path", "")),
            predecessor_sha256=str(predecessor.get("sha256", "")),
            contract_path=str(binding.get("path", "")),
            contract_sha256=str(binding.get("sha256", "")),
            contract_id=str(binding.get("contract_id", "")),
            stage_2_5_binding=_mapping(gate.get("contract_binding"), "Stage 2.5 binding"),
        )
        if dict(payload) != expected:
            raise IntegrityError("ladder profile semantics drifted")
        return dict(payload)
    builder = {
        LEGACY_PROFILE_SCHEMA: _build_legacy_profile,
        PROFILE_SCHEMA: build_profile,
        QUALIFICATION_PROFILE_SCHEMA: build_qualification_successor_profile,
        FINAL_252_PROFILE_SCHEMA: build_qualification_successor_profile,
        HYPOTHESIS_CONDITIONAL_PROFILE_SCHEMA: build_hypothesis_conditional_successor_profile,
        BIAS_GATE_PROFILE_SCHEMA: build_bias_gate_successor_profile,
        BIAS_REMEDIATION_PROFILE_SCHEMA: build_bias_remediation_successor_profile,
    }[schema]
    builder_kwargs: dict[str, object] = {
        "contract_path": str(binding.get("path")),
        "contract_sha256": str(binding.get("sha256")),
        "contract_id": str(binding.get("contract_id")),
    }
    if schema in {
        FINAL_252_PROFILE_SCHEMA,
        HYPOTHESIS_CONDITIONAL_PROFILE_SCHEMA,
        BIAS_GATE_PROFILE_SCHEMA,
        BIAS_REMEDIATION_PROFILE_SCHEMA,
    }:
        builder_kwargs["final_252_binding"] = _mapping(
            _mapping(contract.get("stages"), "stages").get(
                "final_historical_evaluation"
            ),
            "final historical evaluation",
        ).get("manifest_binding")
    expected = builder(
        **builder_kwargs,
    )
    if dict(payload) != expected:
        raise IntegrityError("ladder profile semantics drifted")
    return dict(payload)


def _load_json(root: Path, relative: str, expected_sha256: str) -> dict[str, object]:
    path = contained_path(root, relative)
    if sha256_file(path) != _digest(expected_sha256, f"{relative} SHA-256"):
        raise IntegrityError(f"bound artifact changed: {relative}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise IntegrityError(f"bound artifact is invalid: {relative}") from exc
    if not isinstance(payload, dict) or path.read_bytes() != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"bound artifact is not canonical: {relative}")
    return payload


def load_active_ladder(root: Path) -> tuple[dict[str, object], dict[str, object]]:
    pointer_path = root / ACTIVE_POINTER_PATH
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise UnauthorizedOperation("no active Alpha research ladder") from exc
    if not isinstance(pointer, dict):
        raise IntegrityError("active Alpha ladder pointer is malformed")
    _identity(pointer, "pointer_id", POINTER_SCHEMA)
    contract = _load_json(
        root, str(pointer.get("contract_path", "")), str(pointer.get("contract_sha256", "")),
    )
    profile_path = contained_path(root, str(pointer.get("profile_path", "")))
    if sha256_file(profile_path) != _digest(pointer.get("profile_sha256"), "profile SHA-256"):
        raise IntegrityError("active Alpha ladder profile binding changed")
    try:
        profile = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise IntegrityError("active Alpha ladder profile is malformed") from exc
    if not isinstance(profile, dict):
        raise IntegrityError("active Alpha ladder profile is malformed")
    validate_contract(contract)
    validate_profile(profile, root=root)
    if (
        pointer.get("contract_id") != contract.get("contract_id")
        or pointer.get("profile_id") != profile.get("profile_id")
    ):
        raise IntegrityError("active Alpha ladder pointer identity drifted")
    return contract, profile


def load_registered_ladder(
    root: Path, *, contract_id: str, profile_id: str,
) -> tuple[dict[str, object], dict[str, object]]:
    """Load one immutable ladder generation without making it active authority."""

    contract_id = _digest(contract_id, "registered contract identity")
    profile_id = _digest(profile_id, "registered profile identity")
    registry = Path("state/alpha_ladder_registry") / contract_id
    contract_path = contained_path(root, (registry / "universe_contract.json").as_posix())
    profile_path = contained_path(root, (registry / "alpha_tiered.yaml").as_posix())
    try:
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        profile = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError, yaml.YAMLError) as exc:
        raise IntegrityError("registered Alpha ladder generation is unreadable") from exc
    if (
        not isinstance(contract, dict)
        or contract_path.read_bytes() != canonical_bytes(contract) + b"\n"
        or not isinstance(profile, dict)
    ):
        raise IntegrityError("registered Alpha ladder generation is malformed")
    validate_contract(contract)
    validate_profile(profile, root=root)
    if (
        contract.get("contract_id") != contract_id
        or profile.get("profile_id") != profile_id
    ):
        raise IntegrityError("registered Alpha ladder generation identity drifted")
    return contract, profile


def build_active_pointer(
    *, contract_path: str, contract_sha256: str, contract_id: str,
    profile_path: str, profile_sha256: str, profile_id: str,
) -> dict[str, object]:
    core = {
        "schema_version": POINTER_SCHEMA,
        "contract_path": contract_path,
        "contract_sha256": contract_sha256,
        "contract_id": contract_id,
        "profile_path": profile_path,
        "profile_sha256": profile_sha256,
        "profile_id": profile_id,
    }
    return {**core, "pointer_id": sha256_json(core)}


def _contains_forbidden_macro_evidence(value: object) -> bool:
    """Return whether a selector payload names strategy-result evidence."""

    forbidden = set(FORBIDDEN_MACRO_EVIDENCE_FIELDS)

    def normalized(item: object) -> str:
        return str(item).strip().lower().replace("-", "_").replace(" ", "_")

    if isinstance(value, Mapping):
        return any(
            any(token in normalized(key) for token in forbidden)
            or _contains_forbidden_macro_evidence(item)
            for key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return any(_contains_forbidden_macro_evidence(item) for item in value)
    if isinstance(value, str):
        return any(token in normalized(value) for token in forbidden)
    return False


def validate_macro_selection(payload: Mapping[str, object]) -> dict[str, object]:
    """Validate the small pre-result ZN/6E selector boundary."""

    if tuple(payload.get("candidates", ())) != MACRO_CANDIDATES:
        raise IntegrityError("macro candidates must be exactly ZN and 6E")
    status = str(payload.get("selection_status", ""))
    if status not in MACRO_SELECTION_STATUSES:
        raise IntegrityError("macro selection status is invalid")
    selected = payload.get("selected_root")
    evidence_fields = _strings(
        payload.get("evidence_fields", ()), "macro selector evidence fields",
        allow_empty=True,
    )
    if any(field not in ALLOWED_MACRO_EVIDENCE_FIELDS for field in evidence_fields):
        raise UnauthorizedOperation("macro selector uses non-approved evidence")
    if _contains_forbidden_macro_evidence(payload.get("evidence", {})):
        raise UnauthorizedOperation("strategy-result evidence cannot select the macro market")
    if _contains_forbidden_macro_evidence(evidence_fields):
        raise UnauthorizedOperation("strategy-result evidence cannot select the macro market")
    results = _mapping(payload.get("candidate_gate_results", {}), "macro gate results")
    if set(results) != set(MACRO_CANDIDATES) or any(
        result not in {"NOT_EVALUATED", "PASS", "FAIL"} for result in results.values()
    ):
        raise IntegrityError("macro gate results must cover exactly ZN and 6E")
    passing = tuple(market for market in MACRO_CANDIDATES if results[market] == "PASS")
    if status == "PENDING_PRE_RESULT_EXECUTION_GATE":
        if selected is not None or passing:
            raise IntegrityError("pending macro selection cannot select or pass a candidate")
    elif status == "NO_ELIGIBLE_MACRO_DIVERSIFIER":
        if selected is not None or passing or set(results.values()) != {"FAIL"}:
            raise IntegrityError("no-eligible macro result requires both candidates to fail")
    else:
        if selected not in MACRO_CANDIDATES or selected not in passing:
            raise IntegrityError("selected macro must be exactly one passing candidate")
        if len(passing) == 2 and (
            payload.get("tie_breaker_frozen_before_row_access") is not True
            or not isinstance(payload.get("tie_breaker_id"), str)
            or not payload.get("tie_breaker_id")
        ):
            raise UnauthorizedOperation(
                "two passing macro candidates require a pre-frozen tie-breaker"
            )
    return dict(payload)


def qualify_market(
    *, alpha_status: str = "NOT_EVALUATED",
    execution_proxy_status: str = "NOT_EVALUATED",
    live_execution_status: str = "UNCLASSIFIED",
) -> dict[str, object]:
    """Keep scientific, proxy, live-execution, and deployment status separate."""

    if alpha_status not in ALPHA_STATUSES:
        raise IntegrityError("invalid per-market alpha status")
    if execution_proxy_status not in EXECUTION_PROXY_STATUSES:
        raise IntegrityError("invalid execution-proxy status")
    if live_execution_status not in LIVE_EXECUTION_STATUSES:
        raise IntegrityError("invalid live-execution status")
    return {
        "alpha_status": alpha_status,
        "execution_proxy_status": execution_proxy_status,
        "live_execution_status": live_execution_status,
        "alpha_eligible": alpha_status == "PASS",
        "execution_proxy_eligible": execution_proxy_status == "PASS",
        "live_execution_eligible": live_execution_status == "APPROVED",
        "deployment_candidate": (
            alpha_status == "PASS" and live_execution_status == "APPROVED"
        ),
    }


def qualify_tier1(
    *, es_tier0_result: Mapping[str, object],
    market_results: Mapping[str, object], selected_macro: str,
    frozen_mechanism_sha256: str, no_between_market_tuning: bool,
) -> dict[str, object]:
    """Qualify the frozen Tier-1 pack without pooled-score rescue."""

    _digest(frozen_mechanism_sha256, "frozen mechanism SHA-256")
    if selected_macro not in MACRO_CANDIDATES:
        raise UnauthorizedOperation("Tier 1 requires one selected ZN-or-6E macro market")
    required = (*TIER1_FIXED_MARKETS, selected_macro)
    if not set(market_results) <= set(required):
        raise IntegrityError("Tier 1 results include a market outside the frozen pack")
    complete = set(market_results) == set(required)
    normalized: dict[str, dict[str, object]] = {}
    for market in required:
        if market in market_results:
            result = _mapping(market_results[market], f"{market} result")
            qualification = qualify_market(
                alpha_status=str(result.get("alpha_status", "NOT_EVALUATED")),
                execution_proxy_status=str(
                    result.get("execution_proxy_status", "NOT_EVALUATED")
                ),
                live_execution_status=str(result.get("live_execution_status", "UNCLASSIFIED")),
            )
            normalized[market] = {
                **qualification,
                "mechanism_sha256": str(result.get("mechanism_sha256", "")),
                "tuning_after_freeze": result.get("tuning_after_freeze", False),
            }
    es = qualify_market(
        alpha_status=str(es_tier0_result.get("alpha_status", "NOT_EVALUATED")),
        execution_proxy_status=str(
            es_tier0_result.get("execution_proxy_status", "NOT_EVALUATED")
        ),
        live_execution_status=str(
            es_tier0_result.get("live_execution_status", "UNCLASSIFIED")
        ),
    )
    es_mechanism = str(es_tier0_result.get("mechanism_sha256", ""))
    same_mechanism = complete and es_mechanism == frozen_mechanism_sha256 and all(
        result["mechanism_sha256"] == frozen_mechanism_sha256
        for result in normalized.values()
    )
    tuning_clear = (
        no_between_market_tuning
        and es_tier0_result.get("tuning_after_freeze", False) is False
        and complete
        and all(result["tuning_after_freeze"] is False for result in normalized.values())
    )
    required_pass = complete and all(
        normalized[market]["alpha_status"] == "PASS"
        for market in ("CL", "GC", selected_macro)
    )
    promoted = (
        complete and es["alpha_status"] == "PASS" and required_pass
        and same_mechanism and tuning_clear
    )
    reasons: list[str] = []
    if not complete:
        reasons.append("TIER_1_REQUIRED_RESULTS_INCOMPLETE")
    if es["alpha_status"] != "PASS":
        reasons.append("ES_TIER_0_NOT_PASS")
    if complete:
        for market in ("CL", "GC", selected_macro):
            if normalized[market]["alpha_status"] != "PASS":
                reasons.append(f"REQUIRED_MARKET_{market}_NOT_PASS")
    if not same_mechanism:
        reasons.append("FROZEN_MECHANISM_IDENTITY_MISMATCH")
    if not tuning_clear:
        reasons.append("BETWEEN_MARKET_TUNING_DETECTED")
    family_results = {
        "equity": {
            "status": "PASS" if es["alpha_status"] == "PASS" else "FAIL",
            "independent_credit": 1 if es["alpha_status"] == "PASS" else 0,
            "maximum_independent_credit": 1,
            "anchor": "ES",
            "correlated_extension": "NQ",
        },
        "energy": {
            "status": normalized.get("CL", {}).get("alpha_status", "NOT_EVALUATED"),
            "independent_credit": int(
                normalized.get("CL", {}).get("alpha_status") == "PASS"
            ),
        },
        "metals": {
            "status": normalized.get("GC", {}).get("alpha_status", "NOT_EVALUATED"),
            "independent_credit": int(
                normalized.get("GC", {}).get("alpha_status") == "PASS"
            ),
        },
        "macro": {
            "status": normalized.get(selected_macro, {}).get(
                "alpha_status", "NOT_EVALUATED"
            ),
            "selected_root": selected_macro,
            "independent_credit": int(
                normalized.get(selected_macro, {}).get("alpha_status") == "PASS"
            ),
        },
    }
    return {
        "scientific_promotion": promoted,
        "required_results_exist": complete,
        "same_frozen_mechanism_identity": same_mechanism,
        "no_between_market_tuning": tuning_clear,
        "selected_macro": selected_macro,
        "es_tier0_result": es,
        "market_results": normalized,
        "family_results": family_results,
        "nq_additional_equity_family_credit": 0,
        "nq_result_required_and_individually_visible": True,
        "required_failure_reasons": reasons,
        "aggregate_score_authoritative": False,
    }


def validate_stage_decision(
    payload: Mapping[str, object], *, contract_id: str, mechanism_sha256: str,
    expected_stage: str, root: Path | None = None,
) -> dict[str, object]:
    _identity(payload, "decision_id", DECISION_SCHEMA)
    if (
        payload.get("contract_id") != contract_id
        or payload.get("mechanism_sha256") != mechanism_sha256
        or payload.get("stage") != expected_stage
        or payload.get("decision") != "PASS"
    ):
        raise UnauthorizedOperation("required predecessor stage did not pass")
    qualification_contract = False
    if root is not None:
        active_contract, _ = load_active_ladder(root)
        qualification_contract = (
            active_contract.get("contract_id") == contract_id
            and active_contract.get("schema_version") in {
                QUALIFICATION_CONTRACT_SCHEMA, FINAL_252_CONTRACT_SCHEMA,
                BIAS_GATE_CONTRACT_SCHEMA, BIAS_REMEDIATION_CONTRACT_SCHEMA,
                CANONICAL_BIAS_REMEDIATION_CONTRACT_SCHEMA,
            }
        )
    if qualification_contract and expected_stage in {"holdout", "forward"}:
        raise UnauthorizedOperation(
            "Final-252 access requires separate authorization and is forbidden"
            if active_contract.get("schema_version") == FINAL_252_CONTRACT_SCHEMA
            else "Final-252 authority is unresolved and access is forbidden"
        )
    if expected_stage == "challenge" and (
        payload.get("attempt_count") != 1
        or payload.get("same_frozen_candidate") is not True
        or payload.get("tuning_design_change_or_retry") is not False
    ):
        raise UnauthorizedOperation("recent-regime challenge is not one-shot and frozen")
    if expected_stage == "refit" and (
        payload.get("fit_parameters_only") is not True
        or payload.get("algorithm_changed") is not False
        or payload.get("performance_evaluation") is not False
        or payload.get("status") != "REFIT_COMPLETE_NO_EVALUATION"
    ):
        raise UnauthorizedOperation("deterministic refit performed evaluation or changed the algorithm")
    if expected_stage == "tier_0":
        if root is None:
            raise IntegrityError("Tier 0 decision validation requires repository context")
        certificate_path = str(payload.get("synthetic_certificate_path", ""))
        certificate_sha = str(payload.get("synthetic_certificate_sha256", ""))
        certificate = _load_json(root, certificate_path, certificate_sha)
        from .alpha_ladder_frozen_mechanism import validate_tier0_certificate
        validate_tier0_certificate(
            certificate, contract_id=contract_id, mechanism_sha256=mechanism_sha256,
        )
    elif (
        qualification_contract
        and active_contract.get("schema_version") in {
            BIAS_GATE_CONTRACT_SCHEMA, BIAS_REMEDIATION_CONTRACT_SCHEMA,
            CANONICAL_BIAS_REMEDIATION_CONTRACT_SCHEMA,
        }
        and expected_stage == "tier_1"
    ):
        evidence = _mapping(payload.get("promotion_evidence"), "promotion evidence")
        markets = _strings(payload.get("scope_markets"), "scope-matched Tier 1 markets")
        from .alpha_ladder_frozen_mechanism import validate_promotion_evidence
        validate_promotion_evidence(evidence, stage=expected_stage, markets=markets)
    elif qualification_contract and expected_stage == "tier_1":
        evidence = _mapping(payload.get("promotion_evidence"), "promotion evidence")
        es_result = _mapping(evidence.get("es_tier0_result"), "ES Tier-0 result")
        market_results = _mapping(evidence.get("market_results"), "Tier-1 market results")
        report = qualify_tier1(
            es_tier0_result=es_result,
            market_results=market_results,
            selected_macro=str(evidence.get("selected_macro", "")),
            frozen_mechanism_sha256=mechanism_sha256,
            no_between_market_tuning=(
                evidence.get("no_between_market_tuning") is True
            ),
        )
        if report["scientific_promotion"] is not True:
            raise UnauthorizedOperation("Tier 1 independent-family qualification did not pass")
    elif expected_stage in {"pilot", "tier_1", "tier_2", "tier_3"}:
        evidence = _mapping(payload.get("promotion_evidence"), "promotion evidence")
        from .alpha_ladder_frozen_mechanism import validate_promotion_evidence
        stage_markets = {
            "pilot": ("ES",), "tier_1": CORE, "tier_2": BALANCED,
            "tier_3": TRADITIONAL,
        }[expected_stage]
        validate_promotion_evidence(evidence, stage=expected_stage, markets=stage_markets)
    if expected_stage == "tier_3":
        subgroup = _mapping(payload.get("subgroup_decisions"), "Tier 3 subgroup decisions")
        if (
            subgroup.get("traditional") != "PASS"
            or subgroup.get("combined") != "PASS"
            or subgroup.get("satellite_can_rescue_traditional_failure") is not False
        ):
            raise UnauthorizedOperation("Tier 3 traditional subgroup did not pass independently")
    return dict(payload)


def validate_session_manifest(
    payload: Mapping[str, object], *, contract_id: str, mechanism_sha256: str,
    stage: str, markets: Sequence[str], pilot_evaluation_sha256: str | None = None,
) -> dict[str, object]:
    _identity(payload, "manifest_id", SESSION_MANIFEST_SCHEMA)
    if (
        payload.get("contract_id") != contract_id
        or payload.get("mechanism_sha256") != mechanism_sha256
        or payload.get("stage") != stage
    ):
        raise IntegrityError("session manifest identity is mismatched")
    if stage == "pilot":
        training = _strings(payload.get("training_session_ids"), "pilot training sessions")
        evaluation = _strings(payload.get("evaluation_session_ids"), "pilot evaluation sessions")
        if (
            len(training) != 504
            or len(evaluation) != 63
            or len(set((*training, *evaluation))) != 567
            or tuple(sorted((*training, *evaluation))) != (*training, *evaluation)
            or payload.get("markets") != ["ES"]
            or payload.get("fold_ordinal") != 0
            or payload.get("selection_rule")
            != "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD"
            or payload.get("purge_applied") is not True
            or payload.get("embargo_applied") is not True
        ):
            raise UnauthorizedOperation("pilot fold is not the locked 504/63 chronological fold")
    elif stage == "refit":
        by_market = _mapping(payload.get("training_session_ids_by_market"), "refit sessions")
        if set(by_market) != set(markets) or payload.get("performance_evaluation") is not False:
            raise UnauthorizedOperation("refit manifest is not fit-only for the exact scope")
        for market, values in by_market.items():
            sessions = _strings(values, f"{market} refit sessions")
            if len(set(sessions)) != len(sessions) or tuple(sorted(sessions)) != sessions:
                raise IntegrityError("refit sessions are not unique and chronological")
    else:
        by_market = _mapping(payload.get("evaluation_session_ids_by_market"), "stage sessions")
        if set(by_market) != set(markets):
            raise IntegrityError("stage session manifest does not cover its exact markets")
        pilot_ids = _strings(payload.get("excluded_pilot_evaluation_session_ids"), "pilot exclusions")
        if (
            len(pilot_ids) != 63
            or len(set(pilot_ids)) != 63
            or tuple(sorted(pilot_ids)) != pilot_ids
            or sha256_json(list(pilot_ids)) != pilot_evaluation_sha256
        ):
            raise IntegrityError("stage session manifest lost the pilot exclusion binding")
        for market, values in by_market.items():
            sessions = _strings(values, f"{market} evaluation sessions")
            if len(set(sessions)) != len(sessions) or tuple(sorted(sessions)) != sessions:
                raise IntegrityError("stage evaluation sessions are not unique and chronological")
            if set(sessions) & set(pilot_ids):
                raise UnauthorizedOperation("pilot evaluation sessions were reused")
    return dict(payload)


def validate_stage_registration(
    registration: Mapping[str, object], *, certificate: Mapping[str, object], root: Path,
) -> dict[str, str]:
    contract, profile = load_active_ladder(root)
    binding = _mapping(registration.get("alpha_ladder_binding"), "Alpha ladder binding")
    stage = str(binding.get("stage", ""))
    bias_gate = contract.get("schema_version") in {
        BIAS_GATE_CONTRACT_SCHEMA, BIAS_REMEDIATION_CONTRACT_SCHEMA,
        CANONICAL_BIAS_REMEDIATION_CONTRACT_SCHEMA,
    }
    allowed_stages = (*STAGES, "challenge", "refit") if bias_gate else STAGES
    if stage not in allowed_stages:
        raise UnauthorizedOperation("real-history registration has no current Alpha ladder stage")
    if stage == "tier_0":
        raise UnauthorizedOperation("Tier 0 is synthetic-only")
    if (
        binding.get("contract_id") != contract.get("contract_id")
        or binding.get("profile_id") != profile.get("profile_id")
    ):
        raise IntegrityError("registration is not bound to the active Alpha ladder")
    qualification_contract = contract.get("schema_version") in {
        QUALIFICATION_CONTRACT_SCHEMA, FINAL_252_CONTRACT_SCHEMA,
        BIAS_GATE_CONTRACT_SCHEMA, BIAS_REMEDIATION_CONTRACT_SCHEMA,
        CANONICAL_BIAS_REMEDIATION_CONTRACT_SCHEMA,
    }
    if qualification_contract and stage in {"holdout", "forward"}:
        raise UnauthorizedOperation(
            "Final-252 access requires separate authorization and is forbidden"
            if contract.get("schema_version") == FINAL_252_CONTRACT_SCHEMA
            else "Final-252 authority is unresolved and access is forbidden"
        )

    causal_contract: Mapping[str, object] | None = None
    selected_macro: str | None = None
    tier1_markets = CORE
    if bias_gate:
        from .causal_search_access_contract import load_registration_contract
        causal_contract = load_registration_contract(root, registration, required=True)
        assert causal_contract is not None
        causal_scope = _mapping(causal_contract.get("scope"), "causal/search/access scope")
        tier1_markets = _strings(
            causal_scope.get("tier_1_incremental_markets"), "Tier 1 incremental markets",
        )
    elif qualification_contract:
        selector_path = str(binding.get("macro_selection_path", ""))
        selector_sha = str(binding.get("macro_selection_sha256", ""))
        if not selector_path or not selector_sha:
            raise UnauthorizedOperation(
                "Tier 1 macro selection is unresolved; real-history execution is blocked"
            )
        selector = _load_json(root, selector_path, selector_sha)
        validate_macro_selection(selector)
        if selector.get("selection_status") != "SELECTED":
            raise UnauthorizedOperation(
                "Tier 1 macro selection has no eligible selected market"
            )
        selected_macro = str(selector.get("selected_root"))
        tier1_markets = (*TIER1_FIXED_MARKETS, selected_macro)

    mechanism = _digest(binding.get("mechanism_sha256"), "mechanism SHA-256")
    mechanism_path = str(binding.get("mechanism_path", ""))
    frozen_mechanism = _load_json(root, mechanism_path, mechanism)
    from .alpha_ladder_frozen_mechanism import validate_frozen_mechanism
    validate_frozen_mechanism(frozen_mechanism)
    mechanism_id = frozen_mechanism.get("mechanism_id")
    ladder_binding = _mapping(
        frozen_mechanism.get("ladder_binding"), "frozen mechanism ladder binding",
    )
    if (
        registration.get("protocol_id") != mechanism_id
        or binding.get("mechanism_id") != mechanism_id
        or ladder_binding.get("contract_id") != contract.get("contract_id")
        or ladder_binding.get("profile_id") != profile.get("profile_id")
    ):
        raise IntegrityError("registration frozen mechanism binding changed")

    expected_markets_by_stage = {
        "pilot": ("ES",), "tier_1": tier1_markets, "tier_2": BALANCED,
        "tier_3": ALL_APPROVED, "holdout": ALL_APPROVED, "forward": ALL_APPROVED,
    }
    if bias_gate:
        assert causal_contract is not None
        causal_scope = _mapping(causal_contract.get("scope"), "causal/search/access scope")
        protected_markets = _strings(
            causal_scope.get("predicted_effect_markets"), "predicted-effect markets",
        )
        expected_markets_by_stage.update({
            "challenge": protected_markets,
            "refit": protected_markets,
            "holdout": protected_markets,
            "forward": protected_markets,
        })
    expected_markets = expected_markets_by_stage[stage]
    requirements = _mapping(certificate.get("requirements"), "readiness requirements")
    if certificate.get("protocol_id") != mechanism_id:
        raise UnauthorizedOperation("row certificate covers a different frozen mechanism")
    if tuple(requirements.get("required_markets", ())) != expected_markets:
        raise UnauthorizedOperation("row certificate does not cover the exact ladder market set")
    if stage == "pilot" and (
        requirements.get("minimum_training_sessions") != 504
        or requirements.get("minimum_evaluation_sessions") != 63
        or requirements.get("minimum_purge_minutes", 0) <= 0
        or requirements.get("minimum_embargo_sessions", 0) <= 0
    ):
        raise UnauthorizedOperation("pilot readiness does not certify the locked fold")

    predecessor_path = str(binding.get("predecessor_decision_path", ""))
    predecessor_sha = str(binding.get("predecessor_decision_sha256", ""))
    predecessor = _load_json(root, predecessor_path, predecessor_sha)
    predecessor_stage = PREDECESSOR.get(stage)
    if bias_gate:
        assert causal_contract is not None
        causal_scope = _mapping(causal_contract.get("scope"), "causal/search/access scope")
        predecessor_stage = {
            "challenge": str(causal_scope.get("replication_terminal_stage", "")),
            "refit": "challenge",
            "holdout": "refit",
            "forward": "holdout",
        }.get(stage, predecessor_stage)
    if not predecessor_stage:
        raise UnauthorizedOperation("stage predecessor is undefined")
    validate_stage_decision(
        predecessor, contract_id=str(contract["contract_id"]),
        mechanism_sha256=mechanism, expected_stage=predecessor_stage, root=root,
    )

    manifest_path = str(binding.get("session_manifest_path", ""))
    manifest_sha = str(binding.get("session_manifest_sha256", ""))
    manifest = _load_json(root, manifest_path, manifest_sha)
    source_bindings = _mapping(certificate.get("source_bindings"), "certificate source bindings")
    if source_bindings.get(manifest_path) != manifest_sha:
        raise IntegrityError("row certificate does not bind the exact stage sessions")
    pilot_eval_sha = binding.get("pilot_evaluation_session_ids_sha256")
    if stage != "pilot":
        pilot_eval_sha = _digest(pilot_eval_sha, "pilot evaluation session identity")
    validate_session_manifest(
        manifest, contract_id=str(contract["contract_id"]),
        mechanism_sha256=mechanism, stage=stage, markets=expected_markets,
        pilot_evaluation_sha256=pilot_eval_sha if isinstance(pilot_eval_sha, str) else None,
    )
    if stage == "pilot":
        evaluation = manifest["evaluation_session_ids"]
        assert isinstance(evaluation, list)
        pilot_eval_sha = sha256_json(evaluation)
        pilot_only_deferred = (
            causal_contract is not None
            and causal_contract.get("schema_version")
            == "causal_search_access_contract/2.3.0"
        )
        if pilot_only_deferred:
            if binding.get("next_stage_status") != (
                "BLOCKED_UNTIL_SEPARATELY_AUTHORIZED_ROW_CERTIFIED_READINESS"
            ):
                raise UnauthorizedOperation(
                    "multi-family pilot must leave every later stage blocked"
                )
            return {
                "alpha_ladder_contract_id": str(contract["contract_id"]),
                "alpha_ladder_profile_id": str(profile["profile_id"]),
                "alpha_ladder_stage": stage,
                "mechanism_sha256": mechanism,
                "predecessor_decision_sha256": predecessor_sha,
                "session_manifest_sha256": manifest_sha,
                "pilot_evaluation_session_ids_sha256": str(pilot_eval_sha),
            }
        next_path = str(binding.get("tier_1_readiness_evidence_path", ""))
        next_manifest_path = str(binding.get("tier_1_session_manifest_path", ""))
        if not next_path or not next_manifest_path:
            raise UnauthorizedOperation(
                "pilot registration requires executable four-market Tier 1 readiness"
            )
        next_sha = _digest(
            binding.get("tier_1_readiness_evidence_sha256"),
            "Tier 1 readiness evidence SHA-256",
        )
        next_manifest_sha = _digest(
            binding.get("tier_1_session_manifest_sha256"),
            "Tier 1 session manifest SHA-256",
        )
        next_certificate = _load_json(root, next_path, next_sha)
        next_manifest = _load_json(root, next_manifest_path, next_manifest_sha)
        from .preexecution_fold_certification import require_registration_ready
        require_registration_ready(next_certificate, root=root)
        next_requirements = _mapping(
            next_certificate.get("requirements"), "Tier 1 readiness requirements",
        )
        next_sources = _mapping(
            next_certificate.get("source_bindings"), "Tier 1 readiness source bindings",
        )
        if (
            next_certificate.get("protocol_id") != mechanism_id
            or tuple(next_requirements.get("required_markets", ())) != tier1_markets
            or next_sources.get(next_manifest_path) != next_manifest_sha
        ):
            raise UnauthorizedOperation(
                "pilot registration requires executable four-market Tier 1 readiness"
            )
        validate_session_manifest(
            next_manifest, contract_id=str(contract["contract_id"]),
            mechanism_sha256=mechanism, stage="tier_1", markets=tier1_markets,
            pilot_evaluation_sha256=str(pilot_eval_sha),
        )

    if stage == "tier_2":
        reporting = _mapping(registration.get("reporting"), "Tier 2 reporting")
        prior_markets = (
            ("ES", *tier1_markets) if qualification_contract else CORE
        )
        if reporting != {
            "core_markets": list(prior_markets),
            "addition_markets": [
                market for market in BALANCED if market not in prior_markets
            ],
            "report_separately": True,
        }:
            raise UnauthorizedOperation("Tier 2 core/addition reporting is incomplete")
    if stage == "tier_3":
        reporting = _mapping(registration.get("reporting"), "Tier 3 reporting")
        if reporting != {
            "traditional_markets": list(TRADITIONAL),
            "satellite_markets": list(SATELLITE),
            "combined_markets": list(ALL_APPROVED),
            "traditional_must_pass_independently": True,
            "satellite_can_rescue_traditional_failure": False,
        }:
            raise UnauthorizedOperation("Tier 3 mandatory 38/3 reporting is incomplete")

    return {
        "alpha_ladder_contract_id": str(contract["contract_id"]),
        "alpha_ladder_profile_id": str(profile["profile_id"]),
        "alpha_ladder_stage": stage,
        "mechanism_sha256": mechanism,
        "predecessor_decision_sha256": predecessor_sha,
        "session_manifest_sha256": manifest_sha,
        "pilot_evaluation_session_ids_sha256": str(pilot_eval_sha),
    }
