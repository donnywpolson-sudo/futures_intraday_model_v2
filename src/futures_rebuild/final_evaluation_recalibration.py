"""Pure builders and validators for the non-active final-evaluation successor.

The module does not read market rows, evaluate a mechanism, publish artifacts,
or mutate active pointers.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import date, timedelta
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
BOUNDED_FACTS_SCHEMA = "final_252_bounded_official_facts/1.0.0"
CERTIFICATE_SCHEMA = "final_252_independent_certificate/1.0.0"
AUDIT_SCHEMA = "final_252_machine_contamination_audit/1.0.0"
DECLARED_MANIFEST_ID = "0ff48f99d8b6d3a262ddf0a060bea8e733fc95aa7c4b4d43f19a0f78b107d4d1"
DECLARED_MANIFEST_SHA256 = "ddaae9ea273208e840c4a5851d534218fe7b009edd58a42a2ae08b8fdfb44cdd"
DECLARED_ORDERED_SESSION_SHA256 = "fa05d8b7df4df05c1eef6a3266df3d2cb2d70dfe21e1161c327188022a201171"
FIRST_TRADE_DATE = "2025-07-14"
LAST_TRADE_DATE = "2026-07-13"
EXCLUDED_TRADE_DATES = (
    "2025-09-01", "2025-11-27", "2025-12-25", "2026-01-01",
    "2026-01-19", "2026-02-16", "2026-05-25", "2026-06-19",
    "2026-07-03",
)


def build_bounded_official_facts(
    *, source_documents: list[Mapping[str, object]],
) -> dict[str, object]:
    if len(source_documents) != 3 or any(
        item.get("publisher") != "CME_GROUP"
        or not str(item.get("url", "")).startswith("https://www.cmegroup.com/")
        or not item.get("document_id")
        for item in source_documents
    ):
        raise IntegrityError("exact public CME structural sources are required")
    source_bundle_id = sha256_json(source_documents)
    semantic_core = {
        "bounds_inclusive": [FIRST_TRADE_DATE, LAST_TRADE_DATE],
        "construction": "ISO_WEEKDAYS_MINUS_EXACT_CME_NON_TRADE_DATES_PLUS_EXPLICIT_CME_TRADE_DATE_EXCEPTIONS",
        "fallback_dates": False,
        "general_calendar_authority": False,
        "inferred_dates": False,
        "market_specific_availability_deferred_fail_closed": True,
        "session_semantics": "PROJECT_LEVEL_CME_TRADE_DATE_ONLY",
    }
    semantic_contract_id = sha256_json(semantic_core)
    core: dict[str, object] = {
        "schema_version": BOUNDED_FACTS_SCHEMA,
        "source_documents": [dict(item) for item in source_documents],
        "source_bundle_id": source_bundle_id,
        "semantic_contract": semantic_core,
        "semantic_contract_id": semantic_contract_id,
        "excluded_trade_dates": list(EXCLUDED_TRADE_DATES),
        "explicit_trade_date_exceptions": ["2026-04-03"],
        "source_fact": (
            "CME observes the nine excluded dates as non-trade dates in the bounded "
            "interval; CME's 2026 Good Friday advisory explicitly transmits trades "
            "with trade date 2026-04-03."
        ),
        "authority": {
            "row_read": False, "evaluation": False, "holdout_value_access": False,
            "forward_value_access": False, "provider": False, "publication": False,
        },
    }
    return validate_bounded_official_facts(
        _identified(core, "bounded_source_facts_id")
    )


def trade_dates_from_bounded_facts(
    facts: Mapping[str, object],
) -> list[str]:
    validate_bounded_official_facts(facts)
    excluded = set(facts["excluded_trade_dates"])
    current = date.fromisoformat(FIRST_TRADE_DATE)
    last = date.fromisoformat(LAST_TRADE_DATE)
    values: list[str] = []
    while current <= last:
        value = current.isoformat()
        if current.weekday() < 5 and value not in excluded:
            values.append(value)
        current += timedelta(days=1)
    if len(values) != 252 or sha256_json(values) != DECLARED_ORDERED_SESSION_SHA256:
        raise IntegrityError("bounded CME facts do not reproduce Final-252")
    return values


def validate_bounded_official_facts(
    payload: Mapping[str, object],
) -> dict[str, object]:
    _identity(payload, "bounded_source_facts_id", BOUNDED_FACTS_SCHEMA)
    semantic = payload.get("semantic_contract")
    sources = payload.get("source_documents")
    if (
        not isinstance(semantic, Mapping)
        or semantic.get("fallback_dates") is not False
        or semantic.get("inferred_dates") is not False
        or semantic.get("general_calendar_authority") is not False
        or payload.get("semantic_contract_id") != sha256_json(semantic)
        or not isinstance(sources, list)
        or payload.get("source_bundle_id") != sha256_json(sources)
        or payload.get("excluded_trade_dates") != list(EXCLUDED_TRADE_DATES)
        or payload.get("explicit_trade_date_exceptions") != ["2026-04-03"]
        or any(payload.get("authority", {}).values())
    ):
        raise IntegrityError("bounded official facts are invalid")
    return dict(payload)


def build_independent_certificate(
    *, manifest: Mapping[str, object], facts: Mapping[str, object],
) -> dict[str, object]:
    validate_manifest(manifest)
    validate_bounded_official_facts(facts)
    if (
        manifest["ordered_session_sha256"] != DECLARED_ORDERED_SESSION_SHA256
        or manifest["bindings"]["bounded_source_facts_id"]
        != facts["bounded_source_facts_id"]
        or manifest["bindings"]["source_bundle_id"] != facts["source_bundle_id"]
        or manifest["bindings"]["semantic_contract_id"]
        != facts["semantic_contract_id"]
    ):
        raise IntegrityError("certificate inputs are not transitively bound")
    core: dict[str, object] = {
        "schema_version": CERTIFICATE_SCHEMA,
        "status": "CERTIFIED_NON_ACTIVE",
        "manifest_id": manifest["manifest_id"],
        "ordered_session_sha256": manifest["ordered_session_sha256"],
        "bounded_source_facts_id": facts["bounded_source_facts_id"],
        "source_bundle_id": facts["source_bundle_id"],
        "semantic_contract_id": facts["semantic_contract_id"],
        "checks": {
            "exactly_252_unique_ordered_sessions": True,
            "exact_bounds": True,
            "declared_ordered_session_identity_reproduced": True,
            "fallback_or_inferred_dates_absent": True,
            "general_calendar_overclaim_absent": True,
            "market_values_opened": False,
        },
        "authority": {
            "row_read": False, "evaluation": False, "holdout_value_access": False,
            "forward_value_access": False, "publication": False, "active_pointer": False,
        },
    }
    return _identified(core, "certificate_id")


def validate_independent_certificate(
    payload: Mapping[str, object], *, manifest: Mapping[str, object],
    facts: Mapping[str, object],
) -> dict[str, object]:
    expected = build_independent_certificate(manifest=manifest, facts=facts)
    if dict(payload) != expected:
        raise IntegrityError("independent certificate is invalid")
    return dict(payload)


def build_machine_contamination_audit(
    *, manifest: Mapping[str, object], scanned_paths: list[str],
    scanned_git_commits: list[str], exact_session_overlap_records: list[object],
    metadata_parse_failures: list[object],
) -> dict[str, object]:
    validate_manifest(manifest)
    core: dict[str, object] = {
        "schema_version": AUDIT_SCHEMA,
        "manifest_id": manifest["manifest_id"],
        "ordered_session_sha256": manifest["ordered_session_sha256"],
        "scopes": ["REACHABLE_GIT_HISTORY", "WORKTREES", "REPORTS", "STATE_METADATA", "AUTHORIZATION_RECORDS"],
        "scanned_paths": sorted(scanned_paths),
        "scanned_git_commits": sorted(scanned_git_commits),
        "exact_session_overlap_records": exact_session_overlap_records,
        "metadata_parse_failures": metadata_parse_failures,
        "machine_evidence_result": (
            "NO_RESEARCH_SELECTION_ACCESS_PROVEN"
            if not exact_session_overlap_records and not metadata_parse_failures
            else "AMBIGUOUS_OR_CONTAMINATED"
        ),
        "authority": {"row_read": False, "evaluation": False, "holdout_value_access": False},
    }
    return _identified(core, "audit_id")


def build_manifest(
    *, trade_dates: list[str], bindings: Mapping[str, object],
) -> dict[str, object]:
    sessions = [
        {"deterministic_session_id": value, "trade_date": value}
        for value in trade_dates
    ]
    core: dict[str, object] = {
        "schema_version": MANIFEST_SCHEMA,
        "purpose": "FINAL_PROJECT_LEVEL_EVALUATION_SEQUENCE_ONLY",
        "session_semantics": "PROJECT_LEVEL_CME_TRADE_DATE_ONLY",
        "market_specific_window": False,
        "sessions": sessions,
        "ordered_session_sha256": sha256_json(trade_dates),
        "development_end_exclusive": "2025-07-13T22:00:00Z",
        "forward_start": "2026-07-14T00:00:00Z",
        "bindings": dict(bindings),
        "authority": {
            "row_read": False, "evaluation": False, "holdout_value_access": False,
            "forward_value_access": False, "provider": False, "publication": False,
            "active_pointer": False, "trading": False,
        },
    }
    manifest = _identified(core, "manifest_id")
    return validate_manifest(manifest)


def restoration_disposition(payload: Mapping[str, object]) -> str:
    canonical_sha256 = hashlib.sha256(canonical_bytes(payload) + b"\n").hexdigest()
    matches = (
        payload.get("manifest_id") == DECLARED_MANIFEST_ID,
        canonical_sha256 == DECLARED_MANIFEST_SHA256,
        payload.get("ordered_session_sha256") == DECLARED_ORDERED_SESSION_SHA256,
    )
    if all(matches):
        return "EXACT_DECLARED_AUTHORITY_RESTORATION"
    return "NEW_SUCCESSOR_AUTHORITY_REQUIRED"


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
        "explicit_non_use_confirmed": True,
        "attestation_basis": "CURRENT_USER_FINAL_252_REMEDIATION_CONFIRMATION",
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
    if any(
        item.get("trade_date") != item.get("deterministic_session_id")
        for item in sessions if isinstance(item, Mapping)
    ):
        raise IntegrityError("final evaluation trade dates are invalid")
    try:
        parsed = [date.fromisoformat(str(value)) for value in ids]
    except ValueError as exc:
        raise IntegrityError("final evaluation trade dates are invalid") from exc
    if (
        parsed[0].isoformat() != FIRST_TRADE_DATE
        or parsed[-1].isoformat() != LAST_TRADE_DATE
    ):
        raise IntegrityError("final evaluation trade-date bounds changed")
    if payload.get("ordered_session_sha256") != sha256_json(ids):
        raise IntegrityError("final evaluation ordered-session identity changed")
    if payload.get("purpose") != "FINAL_PROJECT_LEVEL_EVALUATION_SEQUENCE_ONLY":
        raise IntegrityError("manifest exceeded its purpose")
    if payload.get("manifest_id") != DECLARED_MANIFEST_ID and (
        payload.get("session_semantics") != "PROJECT_LEVEL_CME_TRADE_DATE_ONLY"
        or payload.get("market_specific_window") is not False
    ):
        raise IntegrityError("manifest session semantics changed")
    bindings = payload.get("bindings")
    if not isinstance(bindings, Mapping) or any(
        not isinstance(bindings.get(key), str)
        or len(str(bindings[key])) != 64
        or any(character not in "0123456789abcdef" for character in str(bindings[key]))
        for key in ("bounded_source_facts_id", "source_bundle_id", "semantic_contract_id")
    ):
        raise IntegrityError("manifest bindings are invalid")
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
