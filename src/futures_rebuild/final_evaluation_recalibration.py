"""Pure builders and validators for the non-active final-evaluation successor.

The module does not read market rows, evaluate a mechanism, publish artifacts,
or mutate active pointers.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from .alpha_research_ladder import ALL_APPROVED, BALANCED, CORE, SATELLITE, TRADITIONAL
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError


ATTESTATION_SCHEMA = "final_252_human_use_attestation/1.0.0"
CLASSIFICATION_SCHEMA = "final_252_contamination_classification/1.0.0"
CONTRACT_SCHEMA = "alpha_research_ladder_contract/3.0.0"
PROFILE_SCHEMA = "alpha_research_ladder_profile/3.0.0"
ACTIVATION_SCHEMA = "final_252_pipeline_activation_packet/1.0.0"
MANIFEST_SCHEMA = "purpose_limited_final_evaluation_session_manifest/1.0.0"


def _identified(core: Mapping[str, object], field: str) -> dict[str, object]:
    return {**core, field: sha256_json(core)}


def _identity(payload: Mapping[str, object], field: str, schema: str) -> None:
    core = dict(payload)
    recorded = core.pop(field, None)
    if core.get("schema_version") != schema or recorded != sha256_json(core):
        raise IntegrityError(f"invalid {field}")


def build_human_attestation(
    *, manifest_id: str, ordered_session_sha256: str, machine_audit_id: str,
) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": ATTESTATION_SCHEMA,
        "attestation_type": "EXPLICIT_HUMAN_USE_ATTESTATION",
        "manifest_id": manifest_id,
        "ordered_session_sha256": ordered_session_sha256,
        "machine_audit_id": machine_audit_id,
        "trade_date_bounds_inclusive": ["2025-07-14", "2026-07-13"],
        "selection_use": False,
        "attested_non_use_categories": [
            "FEATURE_OR_LABEL_DESIGN", "WFA_OR_OOS_EVALUATION", "MODEL_FIT_OR_COMPARISON",
            "THRESHOLDS", "STOPS", "POSITION_SIZING", "TRANSACTION_COST_ASSUMPTIONS",
            "HYPERPARAMETERS", "PROMOTION_RULES", "RETURNS_PNL_STRATEGY_METRICS",
            "PREDICTION_QUALITY", "ECONOMIC_RESULTS", "INFORMAL_OR_FORMAL_BACKTEST_SELECTION",
        ],
        "permitted_prior_activity_classification": "STRUCTURAL_OR_CUSTODY_ONLY",
        "permitted_prior_activities": [
            "DATA_ACQUISITION", "MIGRATION", "STORAGE", "COVERAGE_VERIFICATION",
            "SCHEMA_AND_TIMESTAMP_VALIDATION", "INTEGRITY_CHECKS", "CANONICAL_PUBLICATION",
            "CALENDAR_AND_SESSION_CONSTRUCTION",
        ],
        "authority": {
            "holdout_value_access": False, "evaluation": False, "row_read": False,
            "publication": False, "active_pointer": False, "provider": False,
            "trading": False, "git": False,
        },
    }
    return _identified(core, "attestation_id")


def build_contamination_classification(
    *, manifest: Mapping[str, object], certificate_id: str,
    machine_audit: Mapping[str, object], attestation: Mapping[str, object],
) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": CLASSIFICATION_SCHEMA,
        "manifest_id": manifest["manifest_id"],
        "ordered_session_sha256": manifest["ordered_session_sha256"],
        "independent_certificate_id": certificate_id,
        "machine_audit_id": machine_audit["audit_id"],
        "machine_exact_session_overlap_count": len(machine_audit["exact_session_overlap_records"]),
        "machine_metadata_parse_failure_count": len(machine_audit["metadata_parse_failures"]),
        "human_attestation_id": attestation["attestation_id"],
        "classification": "RESEARCH_SELECTION_PRISTINE",
        "nomenclature": "Final Sealed 252-Session Holdout",
        "holdout_access_count": 0,
        "holdout_remains_sealed": True,
        "development_end_exclusive": manifest["development_end_exclusive"],
        "forward_start": manifest["forward_start"],
        "limitations": [
            "CLASSIFICATION_DOES_NOT_AUTHORIZE_VALUE_ACCESS_OR_EVALUATION",
            "STRUCTURAL_AND_CUSTODY_ACTIVITY_IS_NOT_RESEARCH_SELECTION_ACCESS",
            "ANY_FUTURE_HOLDOUT_ACCESS_REQUIRES_SEPARATE_EXPLICIT_AUTHORITY",
        ],
    }
    return _identified(core, "classification_id")


def build_contract(
    *, predecessor: Mapping[str, object], final_binding: Mapping[str, object],
    failed_mechanism: Mapping[str, object],
) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": CONTRACT_SCHEMA,
        "classification": "PREPARED_NON_AUTHORIZING_SUCCESSOR",
        "state": "ACTIVE_ONLY_WHEN_REFERENCED_BY_VALID_POINTER",
        "publication_layout": {
            "contract_path_template": "state/alpha_ladder_registry/{contract_id}/universe_contract.json",
            "profile_path_template": "state/alpha_ladder_registry/{contract_id}/alpha_tiered.yaml",
            "active_pointer_path": "configs/active_alpha_research_ladder.json",
            "active_pointer_written_last": True,
        },
        "predecessor": dict(predecessor),
        "authority": {
            "historical_rows": False, "registration": False, "execution": False,
            "holdout_access": False, "provider_network_credentials": False,
            "publication": False, "active_pointer": False, "trading": False,
        },
        "stages": {
            "tier_0": {
                "role": "ENGINEERING_AND_ES_QUALIFICATION", "markets": ["ES"],
                "pass_requires_all_gates": ["synthetic_engineering", "es_pilot"],
                "gates": {
                    "synthetic_engineering": {"data": "SYNTHETIC_ONLY", "alpha_evidence": False},
                    "es_pilot": {
                        "data": "ROW_CERTIFIED_REAL_HISTORY", "training_sessions": 504,
                        "evaluation_sessions": 63, "alpha_confirmation": False,
                        "exact_session_ids_frozen_before_outcomes": True,
                    },
                },
            },
            "tier_1": {"role": "FOUR_MARKET_CONFIRMATION", "markets": list(CORE), "tier_0_pilot_sessions_excluded": True},
            "tier_2": {"role": "BALANCED_16_MARKET_REPLICATION", "markets": list(BALANCED)},
            "tier_3": {
                "role": "FULL_41_MARKET_REPLICATION", "markets": list(ALL_APPROVED),
                "traditional_markets": list(TRADITIONAL), "satellite_markets": list(SATELLITE),
                "traditional_must_pass_independently": True,
                "satellite_can_rescue_traditional_failure": False,
            },
            "final_evaluation": {
                "role": "ONE_PROJECT_LEVEL_FINAL_SEALED_252_SESSION_HOLDOUT",
                "terminal_tier": "tier_3", "maximum_accesses": 1,
                "market_specific_window": False, "micro_window": None,
                "binding": dict(final_binding),
            },
            "forward": {
                "role": "POST_CUTOFF_FORWARD_MONITORING", "start": "2026-07-14T00:00:00Z",
                "monitoring_only": True, "can_rescue_failure": False,
            },
        },
        "transition_order": ["tier_0", "tier_1", "tier_2", "tier_3", "final_evaluation", "forward"],
        "tier_0_gate_order": ["synthetic_engineering", "es_pilot"],
        "operational_gate_identifiers": {"tier_0.synthetic_engineering": "tier_0", "tier_0.es_pilot": "pilot"},
        "standard_market_count": 41,
        "deferred_micro_market_count": 17,
        "deferred_micros": ["MES", "MCL", "MGC", "M6E", "MNQ", "MYM", "M2K", "M6A", "SIL", "MBT", "MET", "M6B", "MJY", "MCD", "MSF", "MNG", "MHG"],
        "deferred_micro_universe_id": "b0e8f6dba737a24ccde56f845307196f69bd4e8143ed30f3742ad3f8fed0e318",
        "failed_mechanism": dict(failed_mechanism),
        "new_counted_mechanism": "NOT_STARTED_RESTART_AT_TIER_0_SYNTHETIC_ENGINEERING",
        "complete_historical_session_index": "UNRESOLVED_2023_2024_NOT_REQUIRED_BY_PURPOSE_LIMITED_FINAL_SEQUENCE",
        "missing_or_ambiguous_evidence": "FAIL_CLOSED",
    }
    return _identified(core, "contract_id")


def build_profile(
    *, contract_path: str, contract_sha256: str, contract_id: str,
    final_binding: Mapping[str, object],
) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": PROFILE_SCHEMA,
        "classification": "PREPARED_NON_AUTHORIZING_OPERATIONAL_VIEW",
        "state": "ACTIVE_ONLY_WHEN_REFERENCED_BY_VALID_POINTER",
        "contract_binding": {"path": contract_path, "sha256": contract_sha256, "contract_id": contract_id},
        "market_sets": {
            "tier_0": ["ES"], "tier_1": list(CORE), "tier_2": list(BALANCED),
            "tier_3_traditional": list(TRADITIONAL), "tier_3_satellite": list(SATELLITE),
            "tier_3_all": list(ALL_APPROVED),
        },
        "transition_order": ["tier_0", "tier_1", "tier_2", "tier_3", "final_evaluation", "forward"],
        "final_evaluation": dict(final_binding),
        "authority": {
            "historical_rows": False, "registration": False, "execution": False,
            "holdout_access": False, "provider_network_credentials": False,
            "publication": False, "active_pointer": False, "trading": False,
        },
    }
    return _identified(core, "profile_id")


def validate_manifest(payload: Mapping[str, object]) -> dict[str, object]:
    _identity(payload, "manifest_id", MANIFEST_SCHEMA)
    sessions = payload.get("sessions")
    if not isinstance(sessions, list) or len(sessions) != 252:
        raise IntegrityError("final evaluation manifest must have exactly 252 sessions")
    ids = [item.get("deterministic_session_id") for item in sessions if isinstance(item, Mapping)]
    if len(ids) != 252 or len(set(ids)) != 252 or ids != sorted(ids):
        raise IntegrityError("final evaluation session identities are invalid")
    if payload.get("ordered_session_sha256") != sha256_json(ids):
        raise IntegrityError("final evaluation ordered-session identity changed")
    if payload.get("purpose") != "FINAL_PROJECT_LEVEL_EVALUATION_SEQUENCE_ONLY":
        raise IntegrityError("manifest exceeded its purpose")
    if any(payload.get("authority", {}).values()):
        raise IntegrityError("manifest cannot grant authority")
    return dict(payload)


def validate_contract(payload: Mapping[str, object]) -> dict[str, object]:
    _identity(payload, "contract_id", CONTRACT_SCHEMA)
    if payload.get("transition_order") != ["tier_0", "tier_1", "tier_2", "tier_3", "final_evaluation", "forward"]:
        raise IntegrityError("successor transition order changed")
    if any(payload.get("authority", {}).values()):
        raise IntegrityError("successor contract cannot grant authority")
    stages = payload.get("stages", {})
    if not isinstance(stages, Mapping) or stages.get("tier_1", {}).get("markets") != list(CORE):
        raise IntegrityError("Tier 1 membership changed")
    if stages.get("tier_2", {}).get("markets") != list(BALANCED):
        raise IntegrityError("Tier 2 membership changed")
    tier3 = stages.get("tier_3", {})
    if tier3.get("traditional_markets") != list(TRADITIONAL) or tier3.get("satellite_markets") != list(SATELLITE):
        raise IntegrityError("Tier 3 membership changed")
    return dict(payload)


def validate_profile(payload: Mapping[str, object], *, contract: Mapping[str, object], contract_path: Path) -> dict[str, object]:
    _identity(payload, "profile_id", PROFILE_SCHEMA)
    binding = payload.get("contract_binding", {})
    if not isinstance(binding, Mapping) or binding.get("contract_id") != contract.get("contract_id") or binding.get("sha256") != sha256_file(contract_path):
        raise IntegrityError("profile contract binding changed")
    if any(payload.get("authority", {}).values()):
        raise IntegrityError("successor profile cannot grant authority")
    return dict(payload)


def require_canonical_file(path: Path, payload: Mapping[str, object]) -> None:
    if path.read_bytes() != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"non-canonical artifact: {path}")
