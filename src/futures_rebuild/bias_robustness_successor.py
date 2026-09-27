"""Fail-closed governance primitives for the inactive post-Stage-2.5 successor.

This module validates declarations and protected-stage receipts.  It never opens
market rows, fits a model, or grants authority.
"""

from __future__ import annotations

import json
from copy import deepcopy
from collections.abc import Mapping, Sequence
from pathlib import Path

from .canonical import (
    canonical_authority_file_bytes, canonical_bytes, contained_path, sha256_file,
    sha256_json,
)
from .errors import IntegrityError, UnauthorizedOperation


FAILED_CONTRACT_SCHEMA = "causal_search_access_contract/2.0.0"
CONTRACT_SCHEMA = "causal_search_access_contract/2.1.0"
FIXED_HOUR_CONTRACT_SCHEMA = "causal_search_access_contract/2.2.0"
MULTIFAMILY_CONTRACT_SCHEMA = "causal_search_access_contract/2.3.0"
GLOBEX_CONTRACT_SCHEMA = "causal_search_access_contract/2.4.0"
GLOBEX_CONTRACT_SCHEMA_V2 = "causal_search_access_contract/2.5.0"
GLOBEX_V1_PATH = Path(
    "state/causal_search_access_contract_registry/"
    "6bbfcbefd92930d9c2bc1858335e90be2d00ba0bb2458f222f413f660c3b4b0d/contract.json"
)
LADDER_SCHEMA = "alpha_research_ladder_contract/9.0.0"
PROFILE_SCHEMA = "alpha_research_ladder_profile/9.0.0"
ACTIVE_HISTORICAL_SCHEMA = "active_alpha_historical_research_design/3.0.0"
ACTIVE_EXECUTION_SCHEMA = "active_alpha_ladder_execution_design/2.0.0"
DECLARATION_SCHEMA = "pre_evaluation_declaration/1.0.0"
ROW_REQUEST_SCHEMA = "stage3_result_bearing_access_request/1.0.0"
CHALLENGE_CLAIM_SCHEMA = "recent_regime_program_claim/1.0.0"
REFIT_CONTRACT_SCHEMA = "deterministic_refit_contract/1.0.0"
REFIT_OUTPUT_SCHEMA = "deterministic_refit_parameter_output/1.0.0"
FORWARD_PROTOCOL_SCHEMA = "forward_monitoring_protocol/1.0.0"
PREDICTIVE_CLAIM = "ROBUST_PREDICTIVE_ASSOCIATION"
CAUSAL_CLAIM = "CAUSALLY_SUPPORTED_MECHANISM"
DEVELOPMENT = ("2018-01-01T00:00:00Z", "2023-01-01T00:00:00Z")
CHALLENGE = ("2023-01-01T00:00:00Z", "2025-07-13T22:00:00Z")
REFIT = ("2018-01-01T00:00:00Z", "2025-07-13T22:00:00Z")
FINAL_252 = ("2025-07-14", "2026-07-13")
DECLARATION_ROOT = Path("state/pre_evaluation_declaration_registry")
CHALLENGE_MARKER_ROOT = Path("state/recent_regime_program_claims")
REFIT_MARKER_ROOT = Path("state/deterministic_refit_claims")
ALLOWED_PURPOSES = frozenset({
    "OUTCOME_OR_TARGET_CONSTRUCTION", "TARGET_COMPARISON", "FEATURE_CONSTRUCTION",
    "FEATURE_COMPARISON", "WFA", "FITTING", "MODEL_COMPARISON", "PREDICTION",
    "STRATEGY_EVALUATION", "ALPHA_EVALUATION", "CAUSAL_RESULT_EVALUATION",
    "ROBUSTNESS_EVALUATION", "REPLICATION", "RECENT_REGIME_CHALLENGE",
    "DETERMINISTIC_REFIT", "FINAL_252", "FORWARD_MONITORING",
})
ALLOWED_STAGES = frozenset({"3", "4", "5", "6", "7", "8", "9", "10", "10.5", "10.6", "11", "12"})
COUNTED_STATES = frozenset({
    "EVALUATION_CLAIMED", "EVALUATED_PASS", "EVALUATED_FAIL",
    "ABORTED_AFTER_ACCESS_COUNTED", "UNRESOLVED",
})
TERMINAL_STATES = frozenset({
    "EVALUATED_PASS", "EVALUATED_FAIL", "ABORTED_AFTER_ACCESS_COUNTED",
    "ABANDONED_BEFORE_ACCESS_NOT_COUNTED",
})


def _object(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise IntegrityError(f"{name} must be an object")
    return value


def _exact(value: object, keys: set[str], name: str) -> Mapping[str, object]:
    item = _object(value, name)
    if set(item) != keys:
        raise IntegrityError(f"{name} keys are not exact")
    return item


def _strings(value: object, name: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    if (not isinstance(value, Sequence) or isinstance(value, (str, bytes))
            or (not value and not allow_empty)
            or any(not isinstance(item, str) or not item for item in value)):
        raise IntegrityError(f"{name} must be a string sequence")
    return tuple(value)


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise IntegrityError(f"{name} must be a SHA-256 digest")
    return value


def _identity(payload: Mapping[str, object], field: str, schema: str) -> None:
    core = {key: value for key, value in payload.items() if key != field}
    if payload.get("schema_version") != schema or payload.get(field) != sha256_json(core):
        raise IntegrityError(f"{schema} identity is invalid")


def _canonical_json(root: Path, binding: Mapping[str, object], name: str) -> dict[str, object]:
    binding = _exact(binding, {"contract_id", "path", "sha256"}, f"{name} binding")
    path = contained_path(root, str(binding["path"]))
    if sha256_file(path) != _digest(binding["sha256"], f"{name} SHA-256"):
        raise IntegrityError(f"{name} binding changed")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise IntegrityError(f"{name} is unreadable") from exc
    if not isinstance(payload, dict) or path.read_bytes() != canonical_authority_file_bytes(payload):
        raise IntegrityError(f"{name} is not canonical")
    if payload.get("contract_id") != binding["contract_id"]:
        raise IntegrityError(f"{name} internal identity changed")
    return payload


def validate_contract(payload: Mapping[str, object], *, root: Path) -> dict[str, object]:
    """Validate the exact-key Stage 2.5 successor contract."""

    if payload.get("schema_version") in {GLOBEX_CONTRACT_SCHEMA, GLOBEX_CONTRACT_SCHEMA_V2}:
        predecessor = _canonical_json(
            root, _object(payload.get("predecessor"), "predecessor"),
            "Stage 2.5 predecessor",
        )
        validate_contract(predecessor, root=root)
        if payload.get("schema_version") == GLOBEX_CONTRACT_SCHEMA:
            path = root / GLOBEX_V1_PATH
            expected = json.loads(path.read_bytes())
            if path.read_bytes() != canonical_bytes(expected) + b"\n":
                raise IntegrityError("historical all-Globex Stage 2.5 contract is not canonical")
        else:
            from .fixed_hour_globex_pipeline import frozen_stage_2_5_contract
            expected = frozen_stage_2_5_contract(root)
        if dict(payload) != expected:
            raise IntegrityError("all-Globex Stage 2.5 successor drifted")
        return expected

    if payload.get("schema_version") == MULTIFAMILY_CONTRACT_SCHEMA:
        predecessor = _canonical_json(
            root, _object(payload.get("predecessor"), "predecessor"),
            "Stage 2.5 predecessor",
        )
        validate_contract(predecessor, root=root)
        protocol = _canonical_json(
            root, _object(payload.get("protocol_binding"), "protocol binding"),
            "multi-family protocol",
        )
        from .alpha_ladder_frozen_mechanism import validate_frozen_mechanism
        protocol_core = {key: value for key, value in protocol.items() if key != "contract_id"}
        mechanism = {**protocol_core, "mechanism_id": protocol.get("contract_id")}
        if (
            protocol.get("schema_version")
            != "fixed_hour_multifamily_frozen_mechanism/1.0.0"
            or protocol.get("contract_id") != sha256_json(protocol_core)
        ):
            raise IntegrityError("multi-family protocol identity or design drifted")
        validate_frozen_mechanism(mechanism)
        expected = build_fixed_hour_multifamily_successor_contract(
            root=root,
            predecessor_binding=dict(payload["predecessor"]),
            protocol_binding=dict(payload["protocol_binding"]),
        )
        if dict(payload) != expected:
            raise IntegrityError("multi-family Stage 2.5 successor drifted")
        return expected

    if payload.get("schema_version") == FIXED_HOUR_CONTRACT_SCHEMA:
        predecessor = _canonical_json(
            root, _object(payload.get("predecessor"), "predecessor"),
            "Stage 2.5 predecessor",
        )
        validate_contract(predecessor, root=root)
        protocol = _canonical_json(
            root, _object(payload.get("protocol_binding"), "protocol binding"),
            "fixed-hour protocol",
        )
        from .fixed_hour_ridge_discovery import enumerate_candidates
        protocol_core = {key: value for key, value in protocol.items() if key != "contract_id"}
        sources = _object(protocol.get("source_contract"), "fixed-hour sources")
        search = _object(protocol.get("search"), "fixed-hour search")
        if (
            protocol.get("schema_version")
            != "fixed_hour_ridge_discovery_contract/1.0.0"
            or protocol.get("contract_id") != sha256_json(protocol_core)
            or search.get("candidates") != enumerate_candidates()
            or search.get("candidate_budget") != 9
            or any(item.get("holding_horizon_minutes") != 60
                   for item in search.get("candidates", ()))
            or sources.get("foundation")
            != "ACTIVE_CAUSALLY_GATED_NORMALIZED_ONE_MINUTE_OHLCV_ONLY"
            or sources.get("market_year_file_count") != 20
            or {item.get("market") for item in sources.get("sources", ())}
            != {"ES", "NQ", "CL", "GC"}
            or not {
                "RAW_L0_OR_DBN_ROWS", "SEALED_SOURCE_ROWS", "2023_PLUS",
                "FINAL_252_HOLDOUT", "FORWARD_DATA",
            } <= set(sources.get("rejected", ()))
        ):
            raise IntegrityError("fixed-hour protocol semantics drifted")
        expected = build_fixed_hour_successor_contract(
            root=root,
            predecessor_binding=dict(payload["predecessor"]),
            protocol_binding=dict(payload["protocol_binding"]),
        )
        if dict(payload) != expected:
            raise IntegrityError("fixed-hour Stage 2.5 successor drifted")
        return expected

    keys = {
        "schema_version", "state", "authority", "predecessor", "protocol_binding",
        "question", "timing", "scope", "search", "macro_disposition", "stage_order",
        "reset", "evidence_classes", "challenge", "refit", "claim_gate",
        "forward_gate", "contract_id",
    }
    schema = payload.get("schema_version")
    if schema == CONTRACT_SCHEMA:
        keys.add("supersession")
    elif schema != FAILED_CONTRACT_SCHEMA:
        raise IntegrityError("unsupported Stage 2.5 successor schema")
    _exact(payload, keys, "Stage 2.5 successor")
    assert isinstance(schema, str)
    _identity(payload, "contract_id", schema)
    if payload["state"] != "PREPARED_NOT_ACTIVE":
        raise IntegrityError("Stage 2.5 successor must remain inactive")
    authority = _exact(payload["authority"], {
        "development_rows", "challenge_rows", "final_252_or_forward",
        "registration_or_execution", "provider_or_network", "trading",
    }, "authority")
    if any(value is not False for value in authority.values()):
        raise IntegrityError("Stage 2.5 successor cannot grant authority")
    predecessor = _exact(payload["predecessor"], {"contract_id", "path", "sha256"}, "predecessor")
    if schema == CONTRACT_SCHEMA:
        predecessor_path = contained_path(root, str(predecessor["path"]))
        if sha256_file(predecessor_path) != _digest(predecessor["sha256"], "predecessor SHA-256"):
            raise IntegrityError("Stage 2.5 predecessor binding changed")
        try:
            predecessor_payload = json.loads(predecessor_path.read_bytes())
        except (OSError, UnicodeError, ValueError) as exc:
            raise IntegrityError("Stage 2.5 predecessor is unreadable") from exc
        if not isinstance(predecessor_payload, dict) or predecessor_payload.get("contract_id") != predecessor["contract_id"] or predecessor_payload.get("schema_version") != FAILED_CONTRACT_SCHEMA:
            raise IntegrityError("Stage 2.5 predecessor identity changed")
        supersession = _exact(payload["supersession"], {
            "predecessor_bundle_id", "reason", "preservation_status",
            "scientific_reinterpretation",
        }, "supersession")
        _digest(supersession["predecessor_bundle_id"], "predecessor bundle identity")
        if supersession != {
            "predecessor_bundle_id": supersession["predecessor_bundle_id"],
            "reason": "NONCANONICAL_TERMINAL_NEWLINE",
            "preservation_status": "PRESERVED_IMMUTABLE_INACTIVE_EVIDENCE",
            "scientific_reinterpretation": False,
        }:
            raise IntegrityError("Stage 2.5 supersession lineage is invalid")
    else:
        _canonical_json(root, predecessor, "Stage 2.5 predecessor")
    protocol = _canonical_json(root, _object(payload["protocol_binding"], "protocol binding"), "bounded protocol")
    if protocol.get("schema_version") != "bounded_alpha_discovery_contract/1.4.0":
        raise IntegrityError("bounded protocol schema is not the frozen search authority")
    question = _exact(payload["question"], {
        "causal_question", "estimand", "mechanism_family", "claim_domain",
        "claim_ceiling", "requested_claim_label",
    }, "question")
    if question["claim_ceiling"] != PREDICTIVE_CLAIM or question["requested_claim_label"] != PREDICTIVE_CLAIM:
        raise IntegrityError("claim exceeds the predictive ceiling")
    timing = _exact(payload["timing"], {
        "exposure_available_before", "outcome_starts_after", "outcome_matures_by",
        "development_start_inclusive", "development_end_exclusive",
        "challenge_start_inclusive", "challenge_end_exclusive",
        "refit_start_inclusive", "refit_end_exclusive", "final_252_first_session",
        "final_252_last_session", "forward_start",
    }, "timing")
    if (timing["development_start_inclusive"], timing["development_end_exclusive"]) != DEVELOPMENT or (
        timing["challenge_start_inclusive"], timing["challenge_end_exclusive"]
    ) != CHALLENGE or (timing["refit_start_inclusive"], timing["refit_end_exclusive"]) != REFIT or (
        timing["final_252_first_session"], timing["final_252_last_session"]
    ) != FINAL_252 or timing["forward_start"] != "2026-07-14T00:00:00Z":
        raise IntegrityError("Stage 2.5 time boundaries drifted")
    scope = _exact(payload["scope"], {
        "broad_generality_claim", "development_markets", "predicted_effect_markets",
        "required_market_families", "tier_1_incremental_markets",
        "last_applicable_replication_stage", "initial_target_family",
    }, "scope")
    markets = _strings(scope["development_markets"], "development markets")
    predicted = _strings(scope["predicted_effect_markets"], "predicted markets")
    if scope["broad_generality_claim"] is not False or markets != ("ES", "NQ", "CL", "GC") or predicted != markets:
        raise IntegrityError("narrow claim scope is inconsistent")
    if _strings(scope["tier_1_incremental_markets"], "Tier 1 markets") != ("NQ", "CL", "GC") or scope[
        "last_applicable_replication_stage"
    ] != "tier_1":
        raise IntegrityError("narrow claim terminal replication stage is inconsistent")
    if _object(scope["required_market_families"], "market families") != {
        "EQUITY_INDEX": ["ES", "NQ"], "ENERGY": ["CL"], "METALS": ["GC"],
    }:
        raise IntegrityError("market-family scope is inconsistent")
    search = _exact(payload["search"], {
        "trial_family", "candidate_budget", "evaluated_trial_definition",
        "legacy_attempts_charged", "adaptive_candidate_creation", "candidate_retry",
        "declaration_schema", "scope_narrowing_action",
    }, "search")
    if search["candidate_budget"] != 27 or search["legacy_attempts_charged"] != 105 or search[
        "declaration_schema"
    ] != DECLARATION_SCHEMA or search["adaptive_candidate_creation"] is not False or search["candidate_retry"] is not False:
        raise IntegrityError("search accounting drifted")
    macro = _exact(payload["macro_disposition"], {"state", "evidence_path", "evidence_sha256", "silent_substitution"}, "macro disposition")
    if macro["state"] != "NO_ELIGIBLE_MACRO_DIVERSIFIER" or macro["silent_substitution"] is not False:
        raise IntegrityError("macro disposition is unresolved")
    evidence_path = contained_path(root, str(macro["evidence_path"]))
    if sha256_file(evidence_path) != _digest(macro["evidence_sha256"], "macro evidence SHA-256"):
        raise IntegrityError("macro disposition evidence changed")
    if tuple(payload["stage_order"]) != (
        "2.5", "3", "4", "5", "6", "SYNTHETIC_ENGINEERING", "ES_PILOT",
        "SCOPE_MATCHED_REPLICATION", "10.5", "10.6", "11", "12",
    ):
        raise IntegrityError("successor stage order drifted")
    reset = _exact(payload["reset"], {"stage_2_5_material_change", "post_stage_6_material_change", "immaterial_correction"}, "reset")
    if reset != {
        "stage_2_5_material_change": "NEW_STAGE_2_5_CONTRACT_INHERIT_HISTORY",
        "post_stage_6_material_change": "NEW_COUNTED_MECHANISM_RESTART_STAGE_2_5_INHERIT_HISTORY",
        "immaterial_correction": "NO_PERFORMANCE_DIRECTION_ALLOWED_EXACT_SUCCESSOR_REQUIRED",
    }:
        raise IntegrityError("reset semantics conflict")
    _strings(payload["evidence_classes"], "evidence classes")
    challenge = _exact(payload["challenge"], {"schema", "scope", "interval", "session_manifest_status", "one_shot", "ancestor_exposure_inherited", "failure_action"}, "challenge")
    if challenge["schema"] != CHALLENGE_CLAIM_SCHEMA or challenge["interval"] != list(CHALLENGE) or challenge["session_manifest_status"] != "NOT_FROZEN_BLOCKED" or challenge["one_shot"] is not True or challenge["ancestor_exposure_inherited"] is not True:
        raise IntegrityError("challenge custody is not fail closed")
    refit = _exact(payload["refit"], {"contract_schema", "output_schema", "interval", "instance_status", "required_predecessor", "performance_evaluation"}, "refit")
    if refit != {"contract_schema": REFIT_CONTRACT_SCHEMA, "output_schema": REFIT_OUTPUT_SCHEMA, "interval": list(REFIT), "instance_status": "NOT_FROZEN_BLOCKED", "required_predecessor": "PASSING_SCOPE_WIDE_CHALLENGE", "performance_evaluation": False}:
        raise IntegrityError("refit prerequisite is not closed")
    claim_gate = _exact(payload["claim_gate"], {"current_ceiling", "causal_contract_status", "causal_claim_action"}, "claim gate")
    if claim_gate != {"current_ceiling": PREDICTIVE_CLAIM, "causal_contract_status": "ABSENT", "causal_claim_action": "BLOCKED_CAUSAL_IDENTIFICATION_CONTRACT_REQUIRED"}:
        raise IntegrityError("causal claim gate drifted")
    forward = _exact(payload["forward_gate"], {"protocol_schema", "instance_status", "missing_action"}, "forward gate")
    if forward != {"protocol_schema": FORWARD_PROTOCOL_SCHEMA, "instance_status": "NOT_FROZEN_BLOCKED", "missing_action": "BLOCKED_FORWARD_MONITORING_PROTOCOL_NOT_FROZEN"}:
        raise IntegrityError("forward prerequisite drifted")
    return dict(payload)


def build_fixed_hour_successor_contract(
    *, root: Path, predecessor_binding: Mapping[str, object],
    protocol_binding: Mapping[str, object],
) -> dict[str, object]:
    """Replace the 27-way horizon search with the exact nine-way 60m search."""

    predecessor = _canonical_json(
        root, predecessor_binding, "Stage 2.5 predecessor",
    )
    if predecessor.get("schema_version") != CONTRACT_SCHEMA:
        raise IntegrityError("fixed-hour successor requires the active 2.1 predecessor")
    validate_contract(predecessor, root=root)
    protocol = _canonical_json(root, protocol_binding, "fixed-hour protocol")
    protocol_core = {key: value for key, value in protocol.items() if key != "contract_id"}
    if (
        protocol.get("schema_version")
        != "fixed_hour_ridge_discovery_contract/1.0.0"
        or protocol.get("contract_id") != sha256_json(protocol_core)
    ):
        raise IntegrityError("fixed-hour protocol identity is invalid")
    core = deepcopy({
        key: value for key, value in predecessor.items()
        if key not in {"contract_id", "supersession"}
    })
    core.update({
        "schema_version": FIXED_HOUR_CONTRACT_SCHEMA,
        "predecessor": dict(predecessor_binding),
        "protocol_binding": dict(protocol_binding),
        "question": {
            **dict(_object(predecessor["question"], "question")),
            "estimand": "POINT_IN_TIME_ASSOCIATION_WITH_SUBSEQUENT_60_MINUTE_STRESS_NET_RETURN",
            "mechanism_family": "FIXED_10_00_05_CAUSAL_OHLCV_TWO_TARGET_RIDGE",
        },
        "search": {
            **dict(_object(predecessor["search"], "search")),
            "candidate_budget": 9,
            "evaluated_trial_definition": "ONE_NESTED_NINE_CONFIGURATION_60_MINUTE_MECHANISM_CHARGED_NINE_ALTERNATIVES_UPFRONT",
            "legacy_attempts_charged": 169,
        },
        "supersession": {
            "predecessor_bundle_id": str(predecessor["contract_id"]),
            "reason": "EXACT_NINE_CANDIDATE_FIXED_60_MINUTE_SUCCESSOR",
            "preservation_status": "PRESERVED_IMMUTABLE_INACTIVE_EVIDENCE",
            "scientific_reinterpretation": False,
        },
    })
    return {**core, "contract_id": sha256_json(core)}


def build_fixed_hour_multifamily_successor_contract(
    *, root: Path, predecessor_binding: Mapping[str, object],
    protocol_binding: Mapping[str, object],
) -> dict[str, object]:
    """Create the counted three-penalty multi-family successor."""

    predecessor = _canonical_json(
        root, predecessor_binding, "Stage 2.5 predecessor",
    )
    if predecessor.get("schema_version") not in {
        FIXED_HOUR_CONTRACT_SCHEMA, MULTIFAMILY_CONTRACT_SCHEMA,
    }:
        raise IntegrityError("multi-family successor requires the active fixed-hour predecessor")
    validate_contract(predecessor, root=root)
    protocol = _canonical_json(root, protocol_binding, "multi-family protocol")
    protocol_core = {key: value for key, value in protocol.items() if key != "contract_id"}
    if (
        protocol.get("schema_version")
        != "fixed_hour_multifamily_frozen_mechanism/1.0.0"
        or protocol.get("contract_id") != sha256_json(protocol_core)
    ):
        raise IntegrityError("multi-family protocol identity is invalid")
    if predecessor.get("schema_version") == MULTIFAMILY_CONTRACT_SCHEMA:
        predecessor_protocol = _canonical_json(
            root, _object(predecessor.get("protocol_binding"), "predecessor protocol"),
            "predecessor multi-family protocol",
        )
        if predecessor_protocol.get("design") != protocol.get("design"):
            raise IntegrityError("result-neutral successor changed the frozen research design")
    correction_only = predecessor.get("schema_version") == MULTIFAMILY_CONTRACT_SCHEMA
    core = deepcopy({
        key: value for key, value in predecessor.items()
        if key not in {"contract_id", "supersession"}
    })
    core.update({
        "schema_version": MULTIFAMILY_CONTRACT_SCHEMA,
        "predecessor": dict(predecessor_binding),
        "protocol_binding": dict(protocol_binding),
        "question": {
            **dict(_object(predecessor["question"], "question")),
            "estimand": "POINT_IN_TIME_ASSOCIATION_WITH_SUBSEQUENT_60_MINUTE_SIGNED_GROSS_RETURN_CLEARING_STRESS_COST",
            "mechanism_family": "FIXED_10_00_05_CAUSAL_OHLCV_SIGNED_TARGET_MULTIFAMILY_RIDGE",
        },
        "search": {
            **dict(_object(predecessor["search"], "search")),
            "trial_family": "fixed_hour_multifamily_signed_target_ridge_60m",
            "candidate_budget": 3,
            "evaluated_trial_definition": "ONE_NESTED_ALL_18_FEATURE_THREE_PENALTY_60_MINUTE_MECHANISM_CHARGED_THREE_ALTERNATIVES_UPFRONT",
            "legacy_attempts_charged": 172,
        },
        "supersession": {
            "predecessor_bundle_id": str(predecessor["contract_id"]),
            "reason": (
                "RESULT_NEUTRAL_CATALOG_SIZE_ASSERTION_CORRECTION_BEFORE_ROW_DECODING"
                if correction_only
                else "CORRECTED_ALL_18_FEATURE_SIGNED_TARGET_THREE_PENALTY_SUCCESSOR"
            ),
            "preservation_status": "PRESERVED_IMMUTABLE_INACTIVE_EVIDENCE",
            "scientific_reinterpretation": False,
        },
    })
    return {**core, "contract_id": sha256_json(core)}


def build_active_historical_design(
    predecessor: Mapping[str, object], *, predecessor_sha256: str,
    source_design: Mapping[str, object], source_path: str, source_sha256: str,
    ladder_contract_id: str, ladder_profile_id: str,
) -> dict[str, object]:
    core = deepcopy({key: value for key, value in predecessor.items() if key != "pointer_id"})
    core.update({
        "schema_version": ACTIVE_HISTORICAL_SCHEMA,
        "predecessor_pointer": {"path": "configs/active_alpha_historical_research_design.json", "sha256": predecessor_sha256, "pointer_id": predecessor["pointer_id"]},
        "source_design_binding": {"path": source_path, "sha256": source_sha256, "design_id": source_design["design_id"]},
        "stage_2_5_binding": {key: source_design["stage_2_5_binding"][key] for key in ("contract_id", "path", "sha256")},
        "ladder_contract_id": ladder_contract_id,
        "ladder_profile_id": ladder_profile_id,
    })
    return {**core, "pointer_id": sha256_json(core)}


def build_active_execution_design(
    predecessor: Mapping[str, object], *, predecessor_sha256: str,
    source_design: Mapping[str, object], source_path: str, source_sha256: str,
    ladder_contract_id: str, ladder_profile_id: str,
) -> dict[str, object]:
    core = deepcopy({key: value for key, value in predecessor.items() if key != "pointer_id"})
    core.update({
        "schema_version": ACTIVE_EXECUTION_SCHEMA,
        "predecessor_pointer": {"path": "configs/active_alpha_ladder_execution_design.json", "sha256": predecessor_sha256, "pointer_id": predecessor["pointer_id"]},
        "source_execution_design_binding": {"path": source_path, "sha256": source_sha256, "execution_design_id": source_design["execution_design_id"]},
        "stage_2_5_contract_id": source_design["stage_2_5_contract_id"],
        "ladder_contract_id": ladder_contract_id,
        "ladder_profile_id": ladder_profile_id,
    })
    return {**core, "pointer_id": sha256_json(core)}


def _validate_active_pointer(payload: Mapping[str, object], schema: str, target: str) -> None:
    _identity(payload, "pointer_id", schema)
    if payload.get("target_path") != target:
        raise IntegrityError("active pointer target changed")


def validate_active_designs(
    historical: Mapping[str, object], execution: Mapping[str, object], *, root: Path,
    ladder_contract_id: str, ladder_profile_id: str,
) -> dict[str, object]:
    _validate_active_pointer(historical, ACTIVE_HISTORICAL_SCHEMA, "configs/active_alpha_historical_research_design.json")
    _validate_active_pointer(execution, ACTIVE_EXECUTION_SCHEMA, "configs/active_alpha_ladder_execution_design.json")
    binding = _exact(historical.get("stage_2_5_binding"), {"contract_id", "path", "sha256"}, "active Stage 2.5 binding")
    stage = _canonical_json(root, binding, "active Stage 2.5 contract")
    validate_contract(stage, root=root)
    if historical.get("ladder_contract_id") != ladder_contract_id or historical.get("ladder_profile_id") != ladder_profile_id or execution.get("ladder_contract_id") != ladder_contract_id or execution.get("ladder_profile_id") != ladder_profile_id or execution.get("stage_2_5_contract_id") != stage["contract_id"]:
        raise IntegrityError("active design bindings changed")
    for payload, key, field, schema in (
        (historical, "source_design_binding", "design_id", "inactive_alpha_historical_research_design_successor/2.0.0"),
        (execution, "source_execution_design_binding", "execution_design_id", "inactive_alpha_execution_design_successor/2.0.0"),
    ):
        source = _exact(payload.get(key), {"path", "sha256", field}, key)
        path = contained_path(root, str(source["path"]))
        if sha256_file(path) != source["sha256"]:
            raise IntegrityError(f"{key} changed")
        value = json.loads(path.read_bytes())
        if not isinstance(value, dict) or path.read_bytes() != canonical_authority_file_bytes(value) or value.get(field) != source[field] or value.get("schema_version") != schema:
            raise IntegrityError(f"{key} is invalid")
    return {"historical": dict(historical), "execution": dict(execution), "stage_2_5_binding": dict(binding)}


def validate_declaration(payload: Mapping[str, object]) -> dict[str, object]:
    keys = {
        "schema_version", "stage_2_5_contract_id", "protocol_id", "trial_family",
        "candidate_id", "candidate_budget_slot", "predecessor_declaration_id",
        "pipeline_stage", "operation",
        "target_family", "target", "horizon", "feature_pack", "model_family",
        "hyperparameter", "construction_variant", "markets", "start_inclusive",
        "end_exclusive", "purpose", "metric_family", "benchmark", "execution_version",
        "code_config_sha256", "catalog_sha256", "creation_evidence", "state",
        "declaration_id",
    }
    _exact(payload, keys, "pre-evaluation declaration")
    _identity(payload, "declaration_id", DECLARATION_SCHEMA)
    _digest(payload["stage_2_5_contract_id"], "Stage 2.5 identity")
    _digest(payload["protocol_id"], "protocol identity")
    _digest(payload["candidate_id"], "candidate identity")
    _digest(payload["code_config_sha256"], "code/config identity")
    _digest(payload["catalog_sha256"], "catalog identity")
    _strings(payload["markets"], "declaration markets")
    if type(payload["candidate_budget_slot"]) is not int or not 0 <= int(payload["candidate_budget_slot"]) < 27:
        raise IntegrityError("candidate budget slot is invalid")
    if payload["purpose"] not in ALLOWED_PURPOSES or payload["state"] not in {"DECLARED_UNEVALUATED", "ABANDONED_BEFORE_ACCESS_NOT_COUNTED"}:
        raise IntegrityError("declaration state or purpose is invalid")
    if payload["pipeline_stage"] not in ALLOWED_STAGES or not isinstance(payload["operation"], str) or not payload["operation"]:
        raise IntegrityError("declaration stage or operation is invalid")
    if not isinstance(payload["start_inclusive"], str) or not isinstance(payload["end_exclusive"], str) or payload["start_inclusive"] >= payload["end_exclusive"]:
        raise IntegrityError("declaration interval is invalid")
    predecessor = payload["predecessor_declaration_id"]
    if predecessor is not None:
        _digest(predecessor, "predecessor declaration identity")
    return dict(payload)


def load_declaration(root: Path, binding: Mapping[str, object]) -> dict[str, object]:
    binding = _exact(binding, {"declaration_id", "path", "sha256"}, "declaration binding")
    path = contained_path(root, str(binding["path"]))
    if sha256_file(path) != _digest(binding["sha256"], "declaration SHA-256"):
        raise IntegrityError("pre-evaluation declaration binding changed")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or path.read_bytes() != canonical_bytes(payload) + b"\n":
        raise IntegrityError("pre-evaluation declaration is not canonical")
    validate_declaration(payload)
    if payload["declaration_id"] != binding["declaration_id"]:
        raise IntegrityError("pre-evaluation declaration identity changed")
    return payload


def require_stage3_active_authority(root: Path) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    from .alpha_research_ladder import load_active_ladder
    contract, profile = load_active_ladder(root)
    if contract.get("schema_version") != LADDER_SCHEMA or profile.get("schema_version") != PROFILE_SCHEMA:
        raise UnauthorizedOperation("BLOCKED_STAGE_2_5_ACTIVE_AUTHORITY_REQUIRED")
    try:
        historical = json.loads((root / "configs/active_alpha_historical_research_design.json").read_text(encoding="utf-8"))
        execution = json.loads((root / "configs/active_alpha_ladder_execution_design.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise UnauthorizedOperation("BLOCKED_STAGE_2_5_ACTIVE_AUTHORITY_REQUIRED") from exc
    if historical.get("schema_version") != ACTIVE_HISTORICAL_SCHEMA or execution.get("schema_version") != ACTIVE_EXECUTION_SCHEMA:
        raise UnauthorizedOperation("BLOCKED_STAGE_2_5_ACTIVE_AUTHORITY_REQUIRED")
    try:
        active = validate_active_designs(
            historical, execution, root=root,
            ladder_contract_id=str(contract["contract_id"]),
            ladder_profile_id=str(profile["profile_id"]),
        )
    except (IntegrityError, OSError, UnicodeError, ValueError) as exc:
        raise UnauthorizedOperation("BLOCKED_STAGE_2_5_ACTIVE_AUTHORITY_REQUIRED") from exc
    binding = active["stage_2_5_binding"]
    if not isinstance(binding, dict):
        raise UnauthorizedOperation("BLOCKED_STAGE_2_5_ACTIVE_AUTHORITY_REQUIRED")
    return contract, profile, dict(binding)


def validate_row_request(root: Path, request: Mapping[str, object]) -> dict[str, object]:
    """Validate all metadata before a Stage 3+ caller may create a row iterator."""

    _exact(request, {"schema_version", "stage", "purpose", "operation", "contract_binding", "declaration_binding", "markets", "start_inclusive", "end_exclusive", "catalog_binding", "request_id"}, "row request")
    _identity(request, "request_id", ROW_REQUEST_SCHEMA)
    _ladder, _profile, active_binding = require_stage3_active_authority(root)
    if request["stage"] not in ALLOWED_STAGES or request["purpose"] not in ALLOWED_PURPOSES:
        raise UnauthorizedOperation("Stage 3+ purpose is not declared")
    contract = _canonical_json(root, _object(request["contract_binding"], "contract binding"), "Stage 2.5 contract")
    validate_contract(contract, root=root)
    if dict(request["contract_binding"]) != active_binding:
        raise UnauthorizedOperation("Stage 3+ request substituted its active Stage 2.5 contract")
    declaration = load_declaration(root, _object(request["declaration_binding"], "declaration binding"))
    markets = _strings(request["markets"], "requested markets")
    if tuple(declaration["markets"]) != markets or declaration["pipeline_stage"] != request["stage"] or declaration["operation"] != request["operation"] or declaration["purpose"] != request["purpose"] or declaration["start_inclusive"] != request["start_inclusive"] or declaration["end_exclusive"] != request["end_exclusive"]:
        raise UnauthorizedOperation("Stage 3+ request differs from its declaration")
    scope = _object(contract["scope"], "contract scope")
    if not set(markets) <= set(scope["development_markets"]):
        raise UnauthorizedOperation("Stage 3+ request leaves the contract market scope")
    catalog = _exact(request["catalog_binding"], {"path", "sha256"}, "catalog binding")
    if sha256_file(contained_path(root, str(catalog["path"]))) != _digest(catalog["sha256"], "catalog SHA-256") or declaration["catalog_sha256"] != catalog["sha256"]:
        raise IntegrityError("Stage 3+ catalog authority changed")
    return dict(request)


def require_claim_label(label: str, identification_contract: Mapping[str, object] | None = None) -> None:
    if label == PREDICTIVE_CLAIM:
        return
    if label != CAUSAL_CLAIM or identification_contract is None:
        raise UnauthorizedOperation("BLOCKED_CAUSAL_IDENTIFICATION_CONTRACT_REQUIRED")
    raise UnauthorizedOperation("BLOCKED_CAUSAL_IDENTIFICATION_CONTRACT_REQUIRED")


def validate_challenge_claim(
    payload: Mapping[str, object], *, root: Path | None = None,
) -> dict[str, object]:
    keys = {"schema_version", "program_id", "period_id", "trial_family", "stage_2_5_contract_id", "candidate_lineage", "claim_scope_sha256", "replication_decision_sha256", "session_manifest_path", "session_manifest_sha256", "ordered_session_sha256", "markets", "start_inclusive", "end_exclusive", "metrics_sha256", "acceptance_rules_sha256", "report_schema_sha256", "failure_action", "claim_id"}
    _exact(payload, keys, "challenge claim")
    _identity(payload, "claim_id", CHALLENGE_CLAIM_SCHEMA)
    for key in ("program_id", "period_id", "stage_2_5_contract_id", "claim_scope_sha256", "replication_decision_sha256", "session_manifest_sha256", "ordered_session_sha256", "metrics_sha256", "acceptance_rules_sha256", "report_schema_sha256"):
        _digest(payload[key], key)
    lineage = _strings(payload["candidate_lineage"], "candidate lineage")
    if len(set(lineage)) != len(lineage) or (payload["start_inclusive"], payload["end_exclusive"]) != CHALLENGE:
        raise UnauthorizedOperation("challenge period or lineage is invalid")
    if _strings(payload["markets"], "challenge markets") != ("ES", "NQ", "CL", "GC"):
        raise UnauthorizedOperation("challenge must consume the complete declared scope")
    if root is not None:
        manifest_path = contained_path(root, str(payload["session_manifest_path"]))
        if sha256_file(manifest_path) != payload["session_manifest_sha256"]:
            raise IntegrityError("challenge session manifest changed")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict) or manifest.get("start_inclusive") != CHALLENGE[0] or manifest.get("end_exclusive") != CHALLENGE[1] or manifest.get("ordered_session_sha256") != payload["ordered_session_sha256"]:
            raise UnauthorizedOperation("challenge session authority is cross-period")
    return dict(payload)


def claim_challenge_exposure(root: Path, payload: Mapping[str, object]) -> Path:
    validate_challenge_claim(payload, root=root)
    lineage = _strings(payload["candidate_lineage"], "candidate lineage")
    scope_key = sha256_json({"program_id": payload["program_id"], "period_id": payload["period_id"], "trial_family": payload["trial_family"]})
    for marker in (root / CHALLENGE_MARKER_ROOT).glob("*/*.json") if (root / CHALLENGE_MARKER_ROOT).exists() else ():
        prior = json.loads(marker.read_text(encoding="utf-8"))
        if prior.get("scope_key") == scope_key or set(prior.get("candidate_lineage", ())) & set(lineage):
            raise UnauthorizedOperation("BLOCKED_CHALLENGE_PERIOD_ALREADY_EXPOSED")
    path = root / CHALLENGE_MARKER_ROOT / scope_key / "claim.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    marker = {"schema_version": "recent_regime_program_exposure/1.0.0", "scope_key": scope_key, "claim_id": payload["claim_id"], "candidate_lineage": list(lineage), "state": "CLAIMED_ONCE_NO_RETRY"}
    try:
        with path.open("xb") as stream:
            stream.write(canonical_authority_file_bytes(marker))
    except FileExistsError as exc:
        raise UnauthorizedOperation("BLOCKED_CHALLENGE_PERIOD_ALREADY_EXPOSED") from exc
    return path


_REFIT_FORBIDDEN = frozenset({"metric", "metrics", "pnl", "sharpe", "return", "returns", "prediction", "predictions", "hit_rate", "accuracy", "feature_importance", "comparison", "validation_score", "challenge_summary", "holdout_summary"})


def _reject_refit_results(value: object) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = str(key).lower()
            if normalized in _REFIT_FORBIDDEN or any(word in normalized for word in _REFIT_FORBIDDEN):
                raise UnauthorizedOperation("refit output contains result-bearing fields")
            _reject_refit_results(nested)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for nested in value:
            _reject_refit_results(nested)


def validate_refit_contract(
    payload: Mapping[str, object], *, root: Path | None = None,
) -> dict[str, object]:
    keys = {"schema_version", "stage_2_5_contract_id", "candidate_sha256", "challenge_decision_sha256", "challenge_claim_sha256", "algorithm_sha256", "code_sha256", "config_sha256", "target_sha256", "features_sha256", "transformations_sha256", "model_family", "hyperparameters_sha256", "objective", "fitting_procedure", "convergence_policy", "deterministic_setting", "permitted_parameter_names", "markets", "session_manifest_path", "session_manifest_sha256", "start_inclusive", "end_exclusive", "execution_design_sha256", "output_schema", "contract_id"}
    _exact(payload, keys, "refit contract")
    _identity(payload, "contract_id", REFIT_CONTRACT_SCHEMA)
    for key in keys - {"schema_version", "model_family", "objective", "fitting_procedure", "convergence_policy", "deterministic_setting", "permitted_parameter_names", "markets", "session_manifest_path", "start_inclusive", "end_exclusive", "output_schema", "contract_id"}:
        _digest(payload[key], key)
    names = _strings(payload["permitted_parameter_names"], "refit parameter names")
    if len(set(names)) != len(names) or tuple(sorted(names)) != names or (payload["start_inclusive"], payload["end_exclusive"]) != REFIT or payload["output_schema"] != REFIT_OUTPUT_SCHEMA:
        raise UnauthorizedOperation("refit contract is not deterministic and interval-bound")
    _strings(payload["markets"], "refit markets")
    if root is not None:
        path = contained_path(root, str(payload["session_manifest_path"]))
        if sha256_file(path) != payload["session_manifest_sha256"]:
            raise IntegrityError("refit session manifest changed")
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict) or manifest.get("start_inclusive") != REFIT[0] or manifest.get("end_exclusive") != REFIT[1]:
            raise UnauthorizedOperation("refit session authority is cross-period")
    return dict(payload)


def validate_refit_output(payload: Mapping[str, object], contract: Mapping[str, object]) -> dict[str, object]:
    validate_refit_contract(contract)
    keys = {"schema_version", "refit_contract_id", "algorithm_sha256", "session_manifest_sha256", "environment_sha256", "parameters", "status", "output_id"}
    _exact(payload, keys, "refit output")
    _identity(payload, "output_id", REFIT_OUTPUT_SCHEMA)
    parameters = _exact(payload["parameters"], set(contract["permitted_parameter_names"]), "refit parameters")
    if payload["refit_contract_id"] != contract["contract_id"] or payload["algorithm_sha256"] != contract["algorithm_sha256"] or payload["session_manifest_sha256"] != contract["session_manifest_sha256"] or payload["status"] != "REFIT_COMPLETE_NO_EVALUATION":
        raise UnauthorizedOperation("refit output changed frozen identities")
    if any(not isinstance(value, (int, float)) or isinstance(value, bool) for value in parameters.values()):
        raise IntegrityError("refit parameter values must be numeric")
    _digest(payload["environment_sha256"], "refit environment identity")
    _reject_refit_results(payload)
    return dict(payload)


def claim_refit_output(root: Path, payload: Mapping[str, object], contract: Mapping[str, object]) -> Path:
    """Atomically accept at most one parameter-only artifact per frozen refit."""

    validate_refit_output(payload, contract)
    path = root / REFIT_MARKER_ROOT / str(contract["contract_id"]) / "claim.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    marker = {"schema_version": "deterministic_refit_claim/1.0.0", "contract_id": contract["contract_id"], "output_id": payload["output_id"], "state": "CLAIMED_ONCE_NO_COMPARISON"}
    try:
        with path.open("xb") as stream:
            stream.write(canonical_authority_file_bytes(marker))
    except FileExistsError as exc:
        raise UnauthorizedOperation("BLOCKED_REFIT_ALREADY_CLAIMED") from exc
    return path


def validate_forward_protocol(payload: Mapping[str, object]) -> dict[str, object]:
    keys = {"schema_version", "forward_start", "candidate_sha256", "refit_output_sha256", "code_config_sha256", "markets", "execution_version", "cost_version", "review_cadence", "minimum_calendar_duration", "minimum_independent_decisions", "reporting_dates", "allowed_interim_diagnostics", "prohibited_interim_success_declarations", "alert_rules", "abstention_kill_rules", "drift_definitions", "execution_discrepancy_treatment", "operational_failure_treatment", "change_taxonomy", "material_change_reset", "immaterial_fix_rules", "version_succession", "evidence_label_consequences", "backfill_is_forward_evidence", "optional_stopping_can_create_success", "final_252_history_rewrite_allowed", "protocol_id"}
    _exact(payload, keys, "forward-monitoring protocol")
    _identity(payload, "protocol_id", FORWARD_PROTOCOL_SCHEMA)
    for key in ("candidate_sha256", "refit_output_sha256", "code_config_sha256"):
        _digest(payload[key], key)
    if payload["forward_start"] != "2026-07-14T00:00:00Z" or type(payload["minimum_independent_decisions"]) is not int or payload["minimum_independent_decisions"] <= 0:
        raise IntegrityError("forward protocol boundary or minimum evidence is invalid")
    if payload["backfill_is_forward_evidence"] is not False or payload["optional_stopping_can_create_success"] is not False or payload["final_252_history_rewrite_allowed"] is not False:
        raise UnauthorizedOperation("forward protocol permits evidence rewriting")
    for key in ("execution_version", "cost_version", "review_cadence", "minimum_calendar_duration", "execution_discrepancy_treatment", "operational_failure_treatment", "change_taxonomy", "material_change_reset", "immaterial_fix_rules", "version_succession", "evidence_label_consequences"):
        if not isinstance(payload[key], str) or not payload[key]:
            raise IntegrityError(f"forward protocol {key} is absent")
    for key in ("markets", "reporting_dates", "allowed_interim_diagnostics", "alert_rules", "abstention_kill_rules", "drift_definitions"):
        _strings(payload[key], f"forward {key}")
    if payload["prohibited_interim_success_declarations"] is not True:
        raise UnauthorizedOperation("forward protocol permits interim success declarations")
    return dict(payload)


def require_forward_protocol(payload: Mapping[str, object] | None) -> dict[str, object]:
    if payload is None:
        raise UnauthorizedOperation("BLOCKED_FORWARD_MONITORING_PROTOCOL_NOT_FROZEN")
    return validate_forward_protocol(payload)


def governance_ledgers(root: Path) -> dict[str, object]:
    """Derive deterministic non-authorizing views from future declarations and evidence."""

    declarations: dict[str, tuple[Path, dict[str, object]]] = {}
    if (root / DECLARATION_ROOT).exists():
        for path in sorted((root / DECLARATION_ROOT).glob("*/*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise IntegrityError("invalid pre-evaluation declaration")
            validate_declaration(payload)
            declarations[str(payload["declaration_id"])] = (path, payload)
    evidence: dict[str, list[tuple[Path, dict[str, object]]]] = {key: [] for key in declarations}
    unresolved: list[dict[str, object]] = []
    roots = (Path("state/trial_registry"), Path("state/authorization_uses"), CHALLENGE_MARKER_ROOT)
    for relative in roots:
        if not (root / relative).exists():
            continue
        for path in sorted((root / relative).glob("**/*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                continue
            declaration_id = payload.get("pre_evaluation_declaration_id")
            if not isinstance(declaration_id, str):
                if "evaluation_state" in payload:
                    unresolved.append({"reason": "MISSING_DECLARATION", "source_path": path.relative_to(root).as_posix(), "source_sha256": sha256_file(path)})
                continue
            if declaration_id not in declarations:
                unresolved.append({"reason": "MISSING_DECLARATION", "source_path": path.relative_to(root).as_posix(), "source_sha256": sha256_file(path)})
            else:
                evidence[declaration_id].append((path, payload))
    search_rows = []
    exposure_rows = []
    claim_rows = []
    evaluated = 0
    for declaration_id in sorted(declarations):
        path, declaration = declarations[declaration_id]
        records = evidence[declaration_id]
        predecessor_id = declaration["predecessor_declaration_id"]
        if predecessor_id is not None and predecessor_id not in declarations:
            unresolved.append({"reason": "MISSING_PREDECESSOR_DECLARATION", "declaration_id": declaration_id, "source_path": path.relative_to(root).as_posix(), "source_sha256": sha256_file(path)})
        states = [str(item.get("evaluation_state", item.get("state", "UNRESOLVED"))) for _, item in records]
        state = states[-1] if states else str(declaration["state"])
        if records and state not in TERMINAL_STATES | {"EVALUATION_CLAIMED", "UNRESOLVED"}:
            state = "UNRESOLVED"
        if state in {"EVALUATION_CLAIMED", "UNRESOLVED"}:
            unresolved.append({"reason": "MISSING_TERMINAL_RESULT", "declaration_id": declaration_id, "source_path": path.relative_to(root).as_posix(), "source_sha256": sha256_file(path)})
        if state in COUNTED_STATES:
            evaluated += 1
        source = path.relative_to(root).as_posix()
        row = {"declaration_id": declaration_id, "candidate_id": declaration["candidate_id"], "budget_slot": declaration["candidate_budget_slot"], "state": state, "predecessor_declaration_id": declaration["predecessor_declaration_id"], "source_path": source, "source_sha256": sha256_file(path)}
        search_rows.append(row)
        exposure_rows.append({**row, "markets": declaration["markets"], "start_inclusive": declaration["start_inclusive"], "end_exclusive": declaration["end_exclusive"], "purpose": declaration["purpose"], "access_record_count": len(records)})
        claim_rows.append({**row, "claim_label": PREDICTIVE_CLAIM, "claim_ceiling": PREDICTIVE_CLAIM, "evidence_class": "DEVELOPMENT_EVIDENCE" if records else "DECLARED", "challenge_status": "NOT_STARTED", "refit_status": "NOT_STARTED", "holdout_status": "SEALED_UNREAD", "forward_status": "NOT_STARTED", "predecessor_evidence": predecessor_id})
    legacy_files = sorted((root / "state/trial_registry").glob("*/*.json")) if (root / "state/trial_registry").exists() else []
    search = {"trial_family": "bounded_alpha_discovery_point_in_time_intraday_state", "candidate_budget": 27, "declarations": search_rows, "current_family_evaluated_count": evaluated, "current_family_remaining_budget": max(0, 27 - evaluated), "legacy_aggregate_burden": 105, "combined_information_family_burden": 105 + evaluated, "legacy_registry_file_count": len(legacy_files), "legacy_registry_inventory_sha256": sha256_json([{"path": path.relative_to(root).as_posix(), "sha256": sha256_file(path)} for path in legacy_files])}
    core = {"schema_version": "research_governance_ledgers/2.0.0", "research_search": search, "protected_period_exposure": exposure_rows, "claim_evidence_status": claim_rows, "unresolved": unresolved, "unresolved_count": len(unresolved), "authority": "DERIVED_READ_ONLY_NOT_AUTHORITY"}
    return {**core, "ledger_id": sha256_json(core)}
