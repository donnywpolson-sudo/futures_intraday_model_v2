"""Register and execute one bounded ES pilot for the frozen multi-family model."""

from __future__ import annotations

import json
import math
import os
from copy import deepcopy
from pathlib import Path
from statistics import median
from typing import Mapping, Sequence

import numpy as np
import pyarrow.parquet as pq

from .alpha_ladder_frozen_mechanism import (
    MANDATORY_BASELINES,
    build_tier0_certificate,
    build_tier0_decision,
    validate_frozen_mechanism,
)
from .alpha_research_ladder import ALL_APPROVED, SESSION_MANIFEST_SCHEMA, load_active_ladder
from .bias_robustness_successor import (
    FIXED_HOUR_CONTRACT_SCHEMA,
    MULTIFAMILY_CONTRACT_SCHEMA,
    build_active_execution_design,
    build_active_historical_design,
    build_fixed_hour_multifamily_successor_contract,
    require_stage3_active_authority,
    validate_active_designs,
    validate_contract as validate_stage_2_5_contract,
    validate_declaration,
    validate_row_request,
)
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .causal_observation_parquet import SCHEMAS
from .certified_research_gateway import CertifiedResearchGateway
from .errors import IntegrityError, UnauthorizedOperation
from .fixed_hour_multifamily_discovery import (
    FEATURE_FAMILIES,
    FEATURE_NAMES,
    INNER_MINIMUM_TRADES,
    PENALTIES,
    PILOT_MINIMUM_TRADES,
    SignedTargetRidge,
    frozen_design,
    prepare_session,
    score_predictions,
    select_penalty,
)
from .fixed_hour_ridge_audit_successor import (
    execution_audit_coordinates,
    purged_embargoed_inner_folds,
    purged_embargoed_outer_pilot,
)
from .fixed_hour_ridge_discovery import NANO, checkpoint_ns
from .fixed_hour_ridge_readiness import BATCH_ROWS, READ_COLUMNS, _valid_row, load_fixed_hour_schedule
from .preexecution_fold_certification import ROW_CERTIFIED, build_fold_readiness_certificate
from .research_gateway_policy import CERTIFIED_TRIAL_EXECUTION_OPERATION, scientific_governance_binding


TRIAL_FAMILY = "fixed_hour_multifamily_signed_target_ridge_60m"
PROTOCOL_PATH = Path("configs/fixed_hour_multifamily_discovery_contract_result_neutral_v2.json")
READINESS_PATH = Path(
    "state/fixed_hour_multifamily_readiness/"
    "a3f02ab717379bad3542b66bf9a8bba727ba75699c30b20f0a77464d889554e1/"
    "readiness.json"
)
ROOT_STATE = Path("state/fixed_hour_multifamily_governed")
ACTIVE_HISTORICAL = Path("configs/active_alpha_historical_research_design.json")
ACTIVE_EXECUTION = Path("configs/active_alpha_ladder_execution_design.json")
CATALOG = Path("data/causally_gated_normalized/_control/catalog.json")
APPROVED_YEARS = (2018, 2019, 2020)
APPROVED_ROWS = 1_048_132
APPROVED_BYTES = 88_028_968
SCENARIOS = ("base", "stress", "extreme")
COST_TICKS = {"base": 2, "stress": 4, "extreme": 8}


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"unreadable multi-family artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"multi-family artifact is not an object: {path}")
    return value


def _artifact(core: Mapping[str, object], identity: str) -> dict[str, object]:
    return {**dict(core), identity: sha256_json(dict(core))}


def _write_or_verify(path: Path, payload: Mapping[str, object]) -> None:
    raw = canonical_bytes(dict(payload)) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != raw:
            raise IntegrityError(f"immutable multi-family artifact differs: {path}")
        return
    descriptor = os.open(
        path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, raw)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _replace(path: Path, payload: Mapping[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.multifamily.tmp")
    temporary.write_bytes(canonical_bytes(dict(payload)) + b"\n")
    os.replace(temporary, path)


def _binding(root: Path, path: Path, identity: str, value: str) -> dict[str, str]:
    return {identity: value, "path": path.as_posix(), "sha256": sha256_file(root / path)}


def candidate_ids() -> list[str]:
    return [
        sha256_json({"full_feature_library": list(FEATURE_NAMES), "ridge_penalty": penalty})
        for penalty in PENALTIES
    ]


def protocol_core(root: Path) -> dict[str, object]:
    ladder, profile = load_active_ladder(root)
    code_paths = (
        Path("src/futures_rebuild/fixed_hour_multifamily_discovery.py"),
        Path("src/futures_rebuild/fixed_hour_multifamily_readiness.py"),
        Path("src/futures_rebuild/fixed_hour_multifamily_governed.py"),
        Path("src/futures_rebuild/fixed_hour_ridge_audit_successor.py"),
        Path("src/futures_rebuild/bias_robustness_successor.py"),
        Path("src/futures_rebuild/causal_search_access_contract.py"),
        Path("src/futures_rebuild/alpha_research_ladder.py"),
    )
    return {
        "schema_version": "fixed_hour_multifamily_frozen_mechanism/1.0.0",
        "state": "FROZEN_UNPUBLISHED_UNREGISTERED",
        "classification": "COUNTED_THREE_ALTERNATIVE_ALL_18_FEATURE_SIGNED_TARGET_RIDGE",
        "research_only": True,
        "ladder_binding": {
            "contract_id": ladder["contract_id"],
            "profile_id": profile["profile_id"],
        },
        "design": frozen_design(),
        "search": {
            "candidate_ids": candidate_ids(),
            "alternative_count_charged_upfront": 3,
            "prior_related_candidate_count": 9,
            "related_candidate_count_after_registration": 12,
            "selection": "FOUR_PURGED_EMBARGOED_TRAINING_ONLY_INNER_FOLDS",
            "outer_result_tuning": False,
        },
        "ranking": {
            "canonical_market_order": list(ALL_APPROVED),
            "restricted_to_current_stage_markets": True,
            "one_intent_per_market_session": True,
        },
        "promotion_gates": {
            "common": {
                "stress_net_pnl_positive": True,
                "beat_all_six_mandatory_baselines": True,
            },
            "pilot": {
                "minimum_trades": PILOT_MINIMUM_TRADES,
                "median_stress_net_mfe_mae_gt": 1.0,
                "largest_positive_trade_contribution_lte": 0.20,
            },
            "tier_1": {"blocked_pending_separate_row_readiness_and_authorization": True},
            "tier_2": {"not_applicable_to_narrow_scope": True},
            "tier_3": {"not_applicable_to_narrow_scope": True},
        },
        "code_bindings": {path.as_posix(): sha256_file(root / path) for path in code_paths},
        "readiness_binding": {
            "readiness_id": READINESS_PATH.parent.name,
            "path": READINESS_PATH.as_posix(),
            "sha256": sha256_file(root / READINESS_PATH),
        },
        "authority": {
            "historical_rows": False,
            "registration": False,
            "execution": False,
            "holdout_2025": False,
            "provider_network_credentials": False,
            "publication": False,
            "trading": False,
        },
    }


def load_protocol(root: Path) -> dict[str, object]:
    protocol = _read_json(root / PROTOCOL_PATH)
    core = {key: value for key, value in protocol.items() if key != "contract_id"}
    if core != protocol_core(root) or protocol.get("contract_id") != sha256_json(core):
        raise IntegrityError("multi-family protocol does not match the frozen implementation")
    validate_frozen_mechanism({**core, "mechanism_id": protocol["contract_id"]})
    return protocol


def build_mechanism(root: Path, protocol: Mapping[str, object]) -> tuple[dict[str, object], Path]:
    core = {key: value for key, value in protocol.items() if key != "contract_id"}
    mechanism = {**core, "mechanism_id": protocol["contract_id"]}
    validate_frozen_mechanism(mechanism)
    path = ROOT_STATE / str(mechanism["mechanism_id"]) / "mechanism.json"
    _write_or_verify(root / path, mechanism)
    return mechanism, path


def activate_successor(
    root: Path, protocol: Mapping[str, object],
) -> dict[str, object]:
    ladder, profile = load_active_ladder(root)
    historical = _read_json(root / ACTIVE_HISTORICAL)
    execution = _read_json(root / ACTIVE_EXECUTION)
    _old_ladder, _old_profile, active_binding = require_stage3_active_authority(root)
    active_contract = _read_json(root / str(active_binding["path"]))
    protocol_binding = _binding(
        root, PROTOCOL_PATH, "contract_id", str(protocol["contract_id"]),
    )
    if active_contract.get("schema_version") == MULTIFAMILY_CONTRACT_SCHEMA:
        validate_stage_2_5_contract(active_contract, root=root)
        if active_contract.get("protocol_binding") == protocol_binding:
            return {"binding": dict(active_binding), "activation": "ALREADY_ACTIVE"}
    elif active_contract.get("schema_version") != FIXED_HOUR_CONTRACT_SCHEMA:
        raise UnauthorizedOperation("multi-family successor has an unexpected predecessor")

    successor = build_fixed_hour_multifamily_successor_contract(
        root=root,
        predecessor_binding=dict(active_binding),
        protocol_binding=protocol_binding,
    )
    successor_path = Path("state/causal_search_access_contract_registry") / str(
        successor["contract_id"]
    ) / "contract.json"
    _write_or_verify(root / successor_path, successor)
    successor_binding = _binding(
        root, successor_path, "contract_id", str(successor["contract_id"]),
    )
    validate_stage_2_5_contract(successor, root=root)

    old_h_binding = historical.get("source_design_binding")
    old_e_binding = execution.get("source_execution_design_binding")
    if not isinstance(old_h_binding, Mapping) or not isinstance(old_e_binding, Mapping):
        raise IntegrityError("active source-design lineage is absent")
    old_h_source = _read_json(root / str(old_h_binding["path"]))
    old_e_source = _read_json(root / str(old_e_binding["path"]))
    h_core = deepcopy({key: value for key, value in old_h_source.items() if key != "design_id"})
    h_core.update({
        "predecessor": dict(old_h_binding),
        "stage_2_5_binding": {
            **successor_binding,
            "schema_version": MULTIFAMILY_CONTRACT_SCHEMA,
        },
        "supersession_reason": "CORRECTED_ALL_18_FEATURE_SIGNED_TARGET_SUCCESSOR",
    })
    h_source = {**h_core, "design_id": sha256_json(h_core)}
    e_core = deepcopy({
        key: value for key, value in old_e_source.items() if key != "execution_design_id"
    })
    e_core.update({
        "predecessor": dict(old_e_binding),
        "stage_2_5_binding": {
            **successor_binding,
            "schema_version": MULTIFAMILY_CONTRACT_SCHEMA,
        },
        "stage_2_5_contract_id": successor["contract_id"],
        "supersession_reason": "CORRECTED_ALL_18_FEATURE_SIGNED_TARGET_SUCCESSOR",
    })
    e_source = {**e_core, "execution_design_id": sha256_json(e_core)}
    source_root = Path("state/bias_robustness_successor_registry") / str(h_source["design_id"])
    h_source_path = source_root / "historical_design.json"
    e_source_path = source_root / "execution_design.json"
    _write_or_verify(root / h_source_path, h_source)
    _write_or_verify(root / e_source_path, e_source)

    next_historical = build_active_historical_design(
        historical,
        predecessor_sha256=sha256_file(root / ACTIVE_HISTORICAL),
        source_design=h_source,
        source_path=h_source_path.as_posix(),
        source_sha256=sha256_file(root / h_source_path),
        ladder_contract_id=str(ladder["contract_id"]),
        ladder_profile_id=str(profile["profile_id"]),
    )
    next_execution = build_active_execution_design(
        execution,
        predecessor_sha256=sha256_file(root / ACTIVE_EXECUTION),
        source_design=e_source,
        source_path=e_source_path.as_posix(),
        source_sha256=sha256_file(root / e_source_path),
        ladder_contract_id=str(ladder["contract_id"]),
        ladder_profile_id=str(profile["profile_id"]),
    )
    validate_active_designs(
        next_historical,
        next_execution,
        root=root,
        ladder_contract_id=str(ladder["contract_id"]),
        ladder_profile_id=str(profile["profile_id"]),
    )
    rollback = _artifact({
        "schema_version": "fixed_hour_multifamily_activation_rollback/1.0.0",
        "state": "PRESERVED_PREDECESSOR_BYTES",
        "historical": {
            "path": ACTIVE_HISTORICAL.as_posix(),
            "sha256": sha256_file(root / ACTIVE_HISTORICAL),
            "payload": historical,
        },
        "execution": {
            "path": ACTIVE_EXECUTION.as_posix(),
            "sha256": sha256_file(root / ACTIVE_EXECUTION),
            "payload": execution,
        },
        "successor_contract_id": successor["contract_id"],
    }, "rollback_id")
    rollback_path = ROOT_STATE / "activation" / str(rollback["rollback_id"]) / "rollback.json"
    _write_or_verify(root / rollback_path, rollback)
    try:
        _replace(root / ACTIVE_HISTORICAL, next_historical)
        _replace(root / ACTIVE_EXECUTION, next_execution)
        _ladder, _profile, observed = require_stage3_active_authority(root)
        if observed != successor_binding:
            raise IntegrityError("multi-family Stage 2.5 activation did not become authoritative")
    except BaseException:
        _replace(root / ACTIVE_HISTORICAL, historical)
        _replace(root / ACTIVE_EXECUTION, execution)
        raise
    return {
        "binding": successor_binding,
        "activation": "ACTIVATED",
        "rollback_path": rollback_path.as_posix(),
    }


def _build_tier0(
    root: Path, mechanism: Mapping[str, object], mechanism_path: Path,
) -> Path:
    ladder, profile = load_active_ladder(root)
    mechanism_sha = sha256_file(root / mechanism_path)
    nodes = (
        "tests/test_fixed_hour_multifamily_discovery.py::test_multifamily_mechanism_is_causal_bounded_and_training_only",
        "tests/test_fixed_hour_multifamily_governed.py::test_synthetic_pilot_metrics_charge_cost_once_and_count_zeros",
    )
    certificate = build_tier0_certificate(
        contract_id=str(ladder["contract_id"]),
        profile_id=str(profile["profile_id"]),
        mechanism_id=str(mechanism["mechanism_id"]),
        mechanism_sha256=mechanism_sha,
        test_node_ids=nodes,
        passed_test_count=len(nodes),
    )
    base = ROOT_STATE / str(mechanism["mechanism_id"])
    certificate_path = base / "tier0_certificate.json"
    _write_or_verify(root / certificate_path, certificate)
    decision = build_tier0_decision(
        contract_id=str(ladder["contract_id"]),
        mechanism_sha256=mechanism_sha,
        synthetic_certificate_path=certificate_path.as_posix(),
        synthetic_certificate_sha256=sha256_file(root / certificate_path),
    )
    decision_path = base / "tier0_decision.json"
    _write_or_verify(root / decision_path, decision)
    return decision_path


def _session_date(session_id: str) -> str:
    value = session_id.removeprefix("PROJECT-")
    if len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise IntegrityError("unexpected project session identity")
    return value


def _ready_session_ids(root: Path, readiness: Mapping[str, object]) -> list[str]:
    schedule = load_fixed_hour_schedule(root, "ES")["sessions"]
    if not isinstance(schedule, Mapping):
        raise IntegrityError("ES schedule is malformed")
    coverage = readiness.get("coverage")
    if not isinstance(coverage, Mapping):
        raise IntegrityError("multi-family readiness coverage is absent")
    nonready = coverage.get("nonready_sessions")
    if not isinstance(nonready, list):
        raise IntegrityError("multi-family readiness non-ready ledger is absent")
    blocked_dates = {
        str(item["trade_date"])
        for item in nonready if isinstance(item, Mapping) and "trade_date" in item
    }
    ready = [
        f"PROJECT-{trade_date}"
        for trade_date, state in sorted(schedule.items())
        if state == "AUTHORITATIVE_OPEN" and trade_date not in blocked_dates
    ]
    if len(ready) != coverage.get("mechanics_ready_sessions"):
        raise IntegrityError("readiness session identities do not reproduce")
    return ready


def _session_manifest(
    contract_id: str, mechanism_sha: str, training: Sequence[str], evaluation: Sequence[str],
) -> dict[str, object]:
    core = {
        "schema_version": SESSION_MANIFEST_SCHEMA,
        "contract_id": contract_id,
        "mechanism_sha256": mechanism_sha,
        "stage": "pilot",
        "markets": ["ES"],
        "fold_ordinal": 0,
        "selection_rule": "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD",
        "training_session_ids": list(training),
        "evaluation_session_ids": list(evaluation),
        "purge_applied": True,
        "embargo_applied": True,
    }
    return {**core, "manifest_id": sha256_json(core)}


def _year_breakdown(
    training: Sequence[str], evaluation: Sequence[str],
) -> dict[str, dict[str, object]]:
    years = sorted({_session_date(item)[:4] for item in (*training, *evaluation)})
    return {
        year: {
            "expected_training_sessions": sum(_session_date(item).startswith(year) for item in training),
            "complete_training_sessions": sum(_session_date(item).startswith(year) for item in training),
            "expected_evaluation_sessions": sum(_session_date(item).startswith(year) for item in evaluation),
            "feature_complete_evaluation_sessions": sum(_session_date(item).startswith(year) for item in evaluation),
            "terminal_evaluation_sessions": sum(_session_date(item).startswith(year) for item in evaluation),
            "execution_path_complete_evaluation_sessions": sum(_session_date(item).startswith(year) for item in evaluation),
            "exclusion_reasons": {},
        }
        for year in years
    }


def _fold_evidence(training: Sequence[str], evaluation: Sequence[str]) -> dict[str, object]:
    count = len(evaluation)
    dispositions = {
        scenario: {
            "feasible_sessions": count,
            "risk_abstention_sessions": 0,
            "unresolved_sessions": 0,
        }
        for scenario in SCENARIOS
    }
    baselines = {}
    for name in MANDATORY_BASELINES:
        flat = name == "flat_no_trade"
        selected = 0 if flat else count
        baselines[name] = {
            "expected_sessions": count,
            "terminal_sessions": count,
            "selected_sessions": selected,
            "selected_path_complete_sessions": selected,
            "scenario_risk_dispositions": {
                scenario: {
                    "feasible_sessions": selected,
                    "risk_abstention_sessions": 0,
                    "unresolved_sessions": 0,
                }
                for scenario in SCENARIOS
            },
            "schedule_independently_derived": True,
            "flat_no_trade": flat,
        }
    return {
        "fold_id": "OUTER_00",
        "market": "ES",
        "role": "OUTER",
        "counts": {
            "expected_training_sessions": len(training),
            "complete_training_sessions": len(training),
            "feature_complete_training_sessions": len(training),
            "transformation_ready_training_sessions": len(training),
            "expected_evaluation_sessions": count,
            "feature_complete_evaluation_sessions": count,
            "terminal_evaluation_sessions": count,
            "execution_path_complete_evaluation_sessions": count,
            "candidate_selected_sessions": count,
            "candidate_selected_path_complete_sessions": count,
            "scenario_risk_dispositions": dispositions,
            "purge_minutes": 120,
            "embargo_sessions": 1,
        },
        "checks": {
            "chronological_order": max(training) < min(evaluation),
            "purge_applied": True,
            "embargo_applied": True,
            "training_only_transformation": True,
            "contract_identity_discontinuities_terminalized": True,
            "roll_discontinuities_terminalized": True,
            "all_incomplete_sessions_terminalized": True,
            "complete_required_metrics": True,
            "promotion_path_computable": True,
        },
        "baseline_universe_readiness": baselines,
        "exclusion_reasons": {},
        "market_year_breakdown": _year_breakdown(training, evaluation),
    }


def build_readiness_artifacts(
    root: Path,
    mechanism: Mapping[str, object],
    mechanism_path: Path,
    stage_binding: Mapping[str, str],
) -> dict[str, object]:
    readiness = _read_json(root / READINESS_PATH)
    readiness_core = {key: value for key, value in readiness.items() if key != "readiness_id"}
    if (
        readiness.get("readiness_id") != sha256_json(readiness_core)
        or readiness.get("state") != "PASS_READINESS_ONLY_MODEL_EXECUTION_NOT_AUTHORIZED"
        or readiness.get("design_id") != frozen_design()["design_id"]
    ):
        raise IntegrityError("approved multi-family readiness evidence is invalid")
    ready = _ready_session_ids(root, readiness)
    training, embargo, evaluation = ready[:504], ready[504:505], ready[505:568]
    fold_readiness = readiness.get("fold_readiness")
    if not isinstance(fold_readiness, Mapping) or not isinstance(fold_readiness.get("outer"), Mapping):
        raise IntegrityError("approved multi-family fold readiness is absent")
    outer = fold_readiness["outer"]
    if (
        sha256_json(training) != outer.get("training_ids_sha256")
        or sha256_json(embargo) != outer.get("embargo_ids_sha256")
        or sha256_json(evaluation) != outer.get("evaluation_ids_sha256")
    ):
        raise IntegrityError("approved outer pilot identities changed")

    ladder, _profile = load_active_ladder(root)
    mechanism_sha = sha256_file(root / mechanism_path)
    manifest = _session_manifest(
        str(ladder["contract_id"]), mechanism_sha, training, evaluation,
    )
    base = ROOT_STATE / str(mechanism["mechanism_id"])
    manifest_path = base / "pilot_sessions.json"
    _write_or_verify(root / manifest_path, manifest)

    observed = readiness.get("observed_access")
    bindings = readiness.get("bindings")
    if not isinstance(observed, Mapping) or not isinstance(bindings, Mapping):
        raise IntegrityError("readiness source bindings are absent")
    sources = observed.get("sources")
    if not isinstance(sources, list):
        raise IntegrityError("readiness source ledger is absent")
    source_bindings = {
        str(item[path_key]): str(item[hash_key])
        for item in sources if isinstance(item, Mapping)
        for path_key, hash_key in (
            ("parquet_path", "parquet_sha256"),
            ("sidecar_path", "sidecar_sha256"),
        )
    }
    for value in bindings.values():
        if not isinstance(value, Mapping):
            raise IntegrityError("readiness implementation binding is malformed")
        source_bindings[str(value["path"])] = str(value["sha256"])
    source_bindings.update({
        READINESS_PATH.as_posix(): sha256_file(root / READINESS_PATH),
        PROTOCOL_PATH.as_posix(): sha256_file(root / PROTOCOL_PATH),
        mechanism_path.as_posix(): mechanism_sha,
        str(stage_binding["path"]): str(stage_binding["sha256"]),
        manifest_path.as_posix(): sha256_file(root / manifest_path),
    })
    certificate = build_fold_readiness_certificate(
        trial_family=TRIAL_FAMILY,
        protocol_id=str(mechanism["mechanism_id"]),
        source_bindings=source_bindings,
        fold_evidence=(_fold_evidence(training, evaluation),),
        required_markets=("ES",),
        required_baselines=MANDATORY_BASELINES,
        required_cost_scenarios=SCENARIOS,
        required_outer_fold_ids=("OUTER_00",),
        required_nested_fold_ids=(),
        expected_outer_folds=1,
        expected_nested_folds=0,
        minimum_training_sessions=504,
        minimum_evaluation_sessions=63,
        minimum_purge_minutes=120,
        minimum_embargo_sessions=1,
        evidence_class=ROW_CERTIFIED,
        historical_rows_opened=True,
    )
    certificate_path = base / "pilot_readiness.json"
    _write_or_verify(root / certificate_path, certificate)
    return {
        "certificate": certificate,
        "certificate_path": certificate_path,
        "manifest": manifest,
        "manifest_path": manifest_path,
        "training": training,
        "embargo": embargo,
        "evaluation": evaluation,
        "readiness": readiness,
    }


def _declaration_and_request(
    root: Path,
    mechanism: Mapping[str, object],
    stage_binding: Mapping[str, str],
) -> tuple[dict[str, object], dict[str, object], Path]:
    code_config = {
        path.as_posix(): sha256_file(root / path)
        for path in (
            PROTOCOL_PATH,
            Path("src/futures_rebuild/fixed_hour_multifamily_discovery.py"),
            Path("src/futures_rebuild/fixed_hour_multifamily_governed.py"),
        )
    }
    superseded = _superseded_registration(root, str(mechanism["mechanism_id"]))
    predecessor_declaration_id = None
    if superseded is not None:
        binding = superseded.get("pre_evaluation_declaration_binding")
        if isinstance(binding, Mapping):
            predecessor_declaration_id = binding.get("declaration_id")
    core = {
        "schema_version": "pre_evaluation_declaration/1.0.0",
        "stage_2_5_contract_id": stage_binding["contract_id"],
        "protocol_id": mechanism["mechanism_id"],
        "trial_family": TRIAL_FAMILY,
        "candidate_id": sha256_json(candidate_ids()),
        "candidate_budget_slot": 9,
        "predecessor_declaration_id": predecessor_declaration_id,
        "pipeline_stage": "5",
        "operation": CERTIFIED_TRIAL_EXECUTION_OPERATION,
        "target_family": "SIGNED_GROSS_60_MINUTE_R_WITH_STRESS_COST_HURDLE",
        "target": "SIGNED_GROSS_60_MINUTE_R",
        "horizon": "60_MINUTES",
        "feature_pack": "ALL_18_FROZEN_CAUSAL_OHLCV_FEATURES",
        "model_family": "SINGLE_SIGNED_TARGET_RIDGE",
        "hyperparameter": "NESTED_SELECTION_PENALTIES_0_1_1_10",
        "construction_variant": "EXACT_THREE_ALTERNATIVES_CHARGED_UPFRONT",
        "markets": ["ES"],
        "start_inclusive": "2018-01-01T00:00:00Z",
        "end_exclusive": "2021-01-01T00:00:00Z",
        "purpose": "WFA",
        "metric_family": "STRESS_NET_SESSION_PNL_MFE_MAE_CONCENTRATION_AND_BASELINES",
        "benchmark": "ZERO_AND_SIX_MANDATORY_BASELINES",
        "execution_version": "fixed_hour_multifamily_governed/1.0.0",
        "code_config_sha256": sha256_json(code_config),
        "catalog_sha256": sha256_file(root / CATALOG),
        "creation_evidence": "USER_APPROVED_ONE_COUNTED_THREE_ALTERNATIVE_ES_PILOT",
        "state": "DECLARED_UNEVALUATED",
    }
    declaration = {**core, "declaration_id": sha256_json(core)}
    validate_declaration(declaration)
    declaration_path = Path("state/pre_evaluation_declaration_registry") / str(
        declaration["declaration_id"]
    ) / "declaration.json"
    _write_or_verify(root / declaration_path, declaration)
    request_core = {
        "schema_version": "stage3_result_bearing_access_request/1.0.0",
        "stage": "5",
        "purpose": "WFA",
        "operation": CERTIFIED_TRIAL_EXECUTION_OPERATION,
        "contract_binding": dict(stage_binding),
        "declaration_binding": {
            "declaration_id": declaration["declaration_id"],
            "path": declaration_path.as_posix(),
            "sha256": sha256_file(root / declaration_path),
        },
        "markets": ["ES"],
        "start_inclusive": "2018-01-01T00:00:00Z",
        "end_exclusive": "2021-01-01T00:00:00Z",
        "catalog_binding": {
            "path": CATALOG.as_posix(),
            "sha256": sha256_file(root / CATALOG),
        },
    }
    request = {**request_core, "request_id": sha256_json(request_core)}
    validate_row_request(root, request)
    return declaration, request, declaration_path


def register(
    root: Path,
    boundary: RepoBoundary,
    mechanism: Mapping[str, object],
    mechanism_path: Path,
    tier0_decision_path: Path,
    stage_binding: Mapping[str, str],
    readiness: Mapping[str, object],
) -> tuple[dict[str, object], Path, str]:
    declaration, request, declaration_path = _declaration_and_request(
        root, mechanism, stage_binding,
    )
    ladder, profile = load_active_ladder(root)
    certificate_path = readiness["certificate_path"]
    manifest_path = readiness["manifest_path"]
    certificate = readiness["certificate"]
    if not isinstance(certificate_path, Path) or not isinstance(manifest_path, Path) or not isinstance(certificate, Mapping):
        raise IntegrityError("multi-family readiness artifact types are invalid")
    ladder_binding = {
        "contract_id": ladder["contract_id"],
        "profile_id": profile["profile_id"],
        "stage": "pilot",
        "mechanism_sha256": sha256_file(root / mechanism_path),
        "mechanism_path": mechanism_path.as_posix(),
        "mechanism_id": mechanism["mechanism_id"],
        "predecessor_decision_path": tier0_decision_path.as_posix(),
        "predecessor_decision_sha256": sha256_file(root / tier0_decision_path),
        "session_manifest_path": manifest_path.as_posix(),
        "session_manifest_sha256": sha256_file(root / manifest_path),
        "next_stage_status": "BLOCKED_UNTIL_SEPARATELY_AUTHORIZED_ROW_CERTIFIED_READINESS",
    }
    superseded = _superseded_registration(root, str(mechanism["mechanism_id"]))
    core = {
        "schema_version": "future_trial_registration/1.0.0",
        "trial_family": TRIAL_FAMILY,
        "protocol_id": mechanism["mechanism_id"],
        "candidate_ids": candidate_ids(),
        "candidate_alternatives_charged_upfront": 3,
        "related_candidate_count_after_registration": 12,
        "fold_readiness_binding": {
            "evidence_path": certificate_path.as_posix(),
            "evidence_sha256": sha256_file(root / certificate_path),
            "certificate_id": certificate["certificate_id"],
        },
        "scientific_governance_binding": scientific_governance_binding(root),
        "alpha_ladder_binding": ladder_binding,
        "causal_search_access_binding": dict(stage_binding),
        "pre_evaluation_declaration_binding": {
            "declaration_id": declaration["declaration_id"],
            "path": declaration_path.as_posix(),
            "sha256": sha256_file(root / declaration_path),
        },
        "stage3_access_request": request,
    }
    if superseded is not None:
        core["result_free_mechanical_abort_predecessor_trial_id"] = superseded["trial_id"]
    registration = {**core, "trial_id": sha256_json(core)}
    path = Path("state/trial_registry/fixed_hour_multifamily") / f"{registration['trial_id']}.json"
    result = CertifiedResearchGateway(root=root, boundary=boundary).register_trial(
        registration_path=root / path,
        registration=registration,
        readiness_evidence_path=root / certificate_path,
    )
    return registration, path, str(result["registration_sha256"])


def _approved_sources(root: Path) -> list[dict[str, object]]:
    entries = _read_json(root / CATALOG).get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are absent")
    sources = sorted(
        (
            item for item in entries
            if isinstance(item, dict) and item.get("market") == "ES"
            and item.get("year") in APPROVED_YEARS
            and item.get("selection_eligible") is True
        ),
        key=lambda item: int(item["year"]),
    )
    expected_paths = [
        f"data/active/causally_gated_normalized/ES/{year}/{year}.parquet"
        for year in APPROVED_YEARS
    ]
    if (
        len(sources) != 3
        or [item.get("parquet_path") for item in sources] != expected_paths
        or sum(int(item.get("row_count", -1)) for item in sources) != APPROVED_ROWS
        or sum((root / str(item["parquet_path"])).stat().st_size for item in sources) != APPROVED_BYTES
    ):
        raise IntegrityError("active catalog differs from the approved ES pilot scope")
    return sources


def _superseded_registration(
    root: Path, current_mechanism_id: str,
) -> dict[str, object] | None:
    registry = root / "state/trial_registry/fixed_hour_multifamily"
    if not registry.exists():
        return None
    matches = []
    for path in sorted(registry.glob("*.json")):
        registration = _read_json(path)
        binding = registration.get("alpha_ladder_binding")
        if (
            registration.get("trial_family") == TRIAL_FAMILY
            and isinstance(binding, Mapping)
            and binding.get("mechanism_id") != current_mechanism_id
        ):
            matches.append(registration)
    if len(matches) > 1:
        raise IntegrityError("more than one result-free multi-family predecessor exists")
    return matches[0] if matches else None


def _prepared_session(
    rows: Sequence[Mapping[str, object]], schedule_state: str,
) -> dict[str, object]:
    session_id = str(rows[0]["project_session_id"])
    trade_date = str(rows[0]["project_trade_date"])
    for row in rows:
        _valid_row(row, session_id, "ES")
    if schedule_state != "AUTHORITATIVE_OPEN":
        raise IntegrityError("a locked pilot session is not authoritatively open")
    decision = checkpoint_ns(trade_date, str(rows[0]["project_timezone"]))
    tick_size = int(rows[0]["min_price_increment_nano"]) / NANO
    multiplier = int(rows[0]["multiplier_nano"]) / NANO
    tick_value = tick_size * multiplier
    costs = {name: 10.0 + ticks * tick_value for name, ticks in COST_TICKS.items()}
    prepared = prepare_session(rows, decision, stress_cost_usd=costs["stress"])
    if prepared.get("status") != "SCORABLE":
        raise IntegrityError(f"locked pilot session became {prepared.get('status')}")
    feature_audit = prepared["feature_audit"]
    outcome = prepared["outcome"]
    if not isinstance(feature_audit, Mapping) or not isinstance(outcome, Mapping):
        raise IntegrityError("prepared pilot session lacks audit evidence")
    audit = execution_audit_coordinates(feature_audit, outcome)
    initial_risk = costs["stress"] / float(prepared["stress_cost_r"])
    signed_gross_usd = float(prepared["signed_gross_r"]) * initial_risk
    entry = int(audit["entry_open_nano"]) / NANO
    maximum = int(audit["maximum_high_nano"]) / NANO
    minimum = int(audit["minimum_low_nano"]) / NANO
    excursions = {
        "long": {
            "mfe_usd": (maximum - entry) * multiplier,
            "mae_usd": (entry - minimum) * multiplier,
        },
        "short": {
            "mfe_usd": (entry - minimum) * multiplier,
            "mae_usd": (maximum - entry) * multiplier,
        },
    }
    return {
        "session_id": session_id,
        "trade_date": trade_date,
        "actual_contract": feature_audit["actual_contract"],
        "features": dict(prepared["features"]),
        "signed_gross_r": float(prepared["signed_gross_r"]),
        "stress_cost_r": float(prepared["stress_cost_r"]),
        "initial_risk_usd": initial_risk,
        "signed_gross_pnl_usd": signed_gross_usd,
        "round_trip_cost_usd": costs,
        "stress_net_mfe_mae": dict(prepared["stress_net_mfe_mae"]),
        "excursions": excursions,
        "execution": audit,
        "label_start_ns": int(outcome["entry"]["bar_start_ns"]),
        "label_end_ns": int(outcome["exit"]["bar_start_ns"]),
    }


def read_pilot_rows(
    root: Path, training: Sequence[str], embargo: Sequence[str], evaluation: Sequence[str],
) -> tuple[dict[str, dict[str, object]], dict[str, object]]:
    required_order = [*training, *embargo, *evaluation]
    required = set(required_order)
    schedule = load_fixed_hour_schedule(root, "ES")["sessions"]
    if not isinstance(schedule, Mapping):
        raise IntegrityError("ES schedule is malformed")
    prepared: dict[str, dict[str, object]] = {}
    observed_rows = observed_bytes = 0
    source_reads = []
    current_id: str | None = None
    current_rows: list[dict[str, object]] = []
    last_start: int | None = None

    def finish() -> None:
        nonlocal current_rows
        if current_id in required:
            if not current_rows:
                raise IntegrityError("locked pilot session has no rows")
            trade_date = str(current_rows[0]["project_trade_date"])
            state = schedule.get(trade_date)
            if not isinstance(state, str):
                raise IntegrityError("locked pilot session lacks calendar authority")
            prepared[str(current_id)] = _prepared_session(current_rows, state)
        current_rows = []

    for source in _approved_sources(root):
        relative = str(source["parquet_path"])
        sidecar_relative = str(source["sidecar_path"])
        path = root / relative
        sidecar_path = root / sidecar_relative
        sidecar = _read_json(sidecar_path)
        entry = sidecar.get("entry_binding")
        if (
            not isinstance(entry, Mapping)
            or sha256_file(path) != source.get("parquet_sha256")
            or sha256_file(sidecar_path) != source.get("sidecar_sha256")
            or entry.get("parquet_path") != relative
            or entry.get("parquet_sha256") != source.get("parquet_sha256")
            or entry.get("row_count") != source.get("row_count")
            or entry.get("selection_eligible") is not True
        ):
            raise IntegrityError("approved ES source or manifest binding changed")
        parquet = pq.ParquetFile(path)
        if (
            parquet.schema_arrow.names != SCHEMAS["observations"].names
            or parquet.metadata.num_rows != int(source["row_count"])
            or path.stat().st_size <= 0
        ):
            raise IntegrityError("approved ES Parquet schema or volume changed")
        file_rows = 0
        for batch in parquet.iter_batches(batch_size=BATCH_ROWS, columns=list(READ_COLUMNS)):
            file_rows += batch.num_rows
            for row in batch.to_pylist():
                start = int(row["bar_start_ns"])
                if last_start is not None and start <= last_start:
                    raise IntegrityError("approved ES files are not globally chronological")
                last_start = start
                session_id = str(row["project_session_id"])
                if current_id is None:
                    current_id = session_id
                if session_id != current_id:
                    finish()
                    current_id = session_id
                if session_id in required:
                    current_rows.append(row)
        if file_rows != int(source["row_count"]):
            raise IntegrityError("approved ES row count changed during replay")
        size = path.stat().st_size
        observed_rows += file_rows
        observed_bytes += size
        source_reads.append({
            "year": source["year"],
            "parquet_path": relative,
            "sidecar_path": sidecar_relative,
            "rows_read": file_rows,
            "physical_bytes": size,
            "parquet_sha256": source["parquet_sha256"],
            "sidecar_sha256": source["sidecar_sha256"],
        })
    finish()
    if (
        observed_rows != APPROVED_ROWS
        or observed_bytes != APPROVED_BYTES
        or list(prepared) != required_order
    ):
        raise IntegrityError("bounded ES replay scope or session order changed")
    outer = purged_embargoed_outer_pilot(prepared, required_order)
    inner = purged_embargoed_inner_folds(prepared, outer["training_session_ids"])
    if (
        outer["training_session_ids"] != list(training)
        or outer["embargo_session_ids"] != list(embargo)
        or outer["evaluation_session_ids"] != list(evaluation)
        or len(inner) != 4
    ):
        raise IntegrityError("row-replayed fold isolation differs from readiness")
    return prepared, {
        "rows_read": observed_rows,
        "physical_bytes": observed_bytes,
        "source_files": source_reads,
        "outer": outer,
        "inner": inner,
    }


def _matrix(rows: Sequence[Mapping[str, object]]) -> np.ndarray:
    return np.asarray(
        [[float(row["features"][name]) for name in FEATURE_NAMES] for row in rows],
        dtype=np.float64,
    )


def _sharpe(values: Sequence[float]) -> float | None:
    array = np.asarray(values, dtype=np.float64)
    standard_deviation = float(np.std(array, ddof=0))
    if standard_deviation == 0.0:
        return 0.0 if float(np.mean(array)) == 0.0 else None
    return float(np.mean(array)) * math.sqrt(252.0) / standard_deviation


def strategy_metrics(values: Sequence[float], intents: Sequence[int]) -> dict[str, object]:
    if len(values) != len(intents) or not values:
        raise IntegrityError("strategy metric arrays are invalid")
    cumulative = peak = maximum_drawdown = 0.0
    trade_values = []
    for value, intent in zip(values, intents, strict=True):
        cumulative += float(value)
        peak = max(peak, cumulative)
        maximum_drawdown = max(maximum_drawdown, peak - cumulative)
        if intent:
            trade_values.append(float(value))
    absolute = np.abs(np.asarray(trade_values, dtype=np.float64))
    effective = (
        float(np.sum(absolute) ** 2 / np.sum(absolute ** 2))
        if len(absolute) and float(np.sum(absolute ** 2)) > 0 else 0.0
    )
    return {
        "net_pnl_usd": float(sum(values)),
        "mean_session_pnl_usd": float(np.mean(values)),
        "session_standard_deviation_usd": float(np.std(values, ddof=0)),
        "session_sharpe_252_including_abstention_zeros": _sharpe(values),
        "maximum_drawdown_usd": maximum_drawdown,
        "trade_count": len(trade_values),
        "winning_trade_fraction": (
            sum(value > 0 for value in trade_values) / len(trade_values)
            if trade_values else None
        ),
        "effective_trade_sample_size_from_absolute_pnl": effective,
    }


def _directions_and_values(
    rows: Sequence[Mapping[str, object]], directions: Sequence[int], scenario: str,
) -> list[float]:
    return [
        0.0 if direction == 0 else (
            direction * float(row["signed_gross_pnl_usd"])
            - float(row["round_trip_cost_usd"][scenario])
        )
        for row, direction in zip(rows, directions, strict=True)
    ]


def _ledger_record(
    row: Mapping[str, object], prediction: float, intent: int,
) -> dict[str, object]:
    direction = "long" if intent == 1 else "short" if intent == -1 else None
    net_pnl = {
        scenario: (
            0.0 if intent == 0 else (
                intent * float(row["signed_gross_pnl_usd"])
                - float(row["round_trip_cost_usd"][scenario])
            )
        )
        for scenario in SCENARIOS
    }
    return {
        "session_id": row["session_id"],
        "trade_date": row["trade_date"],
        "actual_contract": row["actual_contract"],
        "predicted_signed_gross_r": prediction,
        "stress_cost_hurdle_r": row["stress_cost_r"],
        "intent": intent,
        "direction": direction,
        "disposition": (
            "TRADE" if intent else "ABS_PREDICTED_GROSS_R_NOT_ABOVE_STRESS_COST_R"
        ),
        "realized_signed_gross_r": row["signed_gross_r"],
        "initial_risk_usd": row["initial_risk_usd"],
        "gross_pnl_usd": (
            0.0 if intent == 0 else intent * float(row["signed_gross_pnl_usd"])
        ),
        "round_trip_cost_usd": dict(row["round_trip_cost_usd"]),
        "net_pnl_usd": net_pnl,
        "stress_net_mfe_mae": (
            None if direction is None else row["stress_net_mfe_mae"][direction]
        ),
        "excursions": (
            None if direction is None else dict(row["excursions"][direction])
        ),
        "execution": dict(row["execution"]),
    }


def evaluate_pilot(
    ready: Mapping[str, Mapping[str, object]],
    training_ids: Sequence[str],
    evaluation_ids: Sequence[str],
    inner_folds: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], list[dict[str, object]]]:
    training = [ready[item] for item in training_ids]
    evaluation = [ready[item] for item in evaluation_ids]
    folds = []
    for fold in inner_folds:
        fold_training = [ready[item] for item in fold["training_session_ids"]]
        validation = [ready[item] for item in fold["validation_session_ids"]]
        folds.append((
            _matrix(fold_training),
            np.asarray([row["signed_gross_r"] for row in fold_training], dtype=np.float64),
            _matrix(validation),
            np.asarray([row["signed_gross_r"] for row in validation], dtype=np.float64),
            np.asarray([row["stress_cost_r"] for row in validation], dtype=np.float64),
        ))
    try:
        selection = select_penalty(folds)
    except IntegrityError as exc:
        ledger = [{
            "session_id": row["session_id"],
            "trade_date": row["trade_date"],
            "actual_contract": row["actual_contract"],
            "predicted_signed_gross_r": None,
            "intent": 0,
            "direction": None,
            "disposition": "OUTER_NOT_EVALUATED_INNER_SELECTION_GATE_FAILED",
            "execution": dict(row["execution"]),
        } for row in evaluation]
        return {
            "pilot_decision": "FAIL",
            "failed_pilot_gates": ["NO_INNER_ACTIVITY_AND_POSITIVE_NET_ELIGIBLE_CANDIDATE"],
            "selection_failure": str(exc),
            "evaluated_sessions": 0,
            "outer_outcomes_not_scored": True,
            "candidate_alternatives_charged_upfront": 3,
            "related_candidate_count": 12,
        }, ledger

    penalty = float(selection["selected_ridge_penalty"])
    model = SignedTargetRidge(penalty).fit(
        _matrix(training),
        np.asarray([row["signed_gross_r"] for row in training], dtype=np.float64),
    )
    predictions = model.predict(_matrix(evaluation))
    gross_r = np.asarray([row["signed_gross_r"] for row in evaluation], dtype=np.float64)
    stress_cost_r = np.asarray([row["stress_cost_r"] for row in evaluation], dtype=np.float64)
    intents_array, _stress_net_r = score_predictions(predictions, gross_r, stress_cost_r)
    intents = [int(value) for value in intents_array]
    ledger = [
        _ledger_record(row, float(prediction), intent)
        for row, prediction, intent in zip(evaluation, predictions, intents, strict=True)
    ]

    unconditional = 1 if float(np.mean([row["signed_gross_r"] for row in training])) >= 0 else -1
    baseline_directions = {
        "flat_no_trade": [0] * len(evaluation),
        "fold_local_unconditional_direction": [unconditional] * len(evaluation),
        "previous_reported_bar_sign_momentum": [
            1 if float(row["features"]["log_return_1"]) > 0
            else -1 if float(row["features"]["log_return_1"]) < 0 else 0
            for row in evaluation
        ],
        "previous_reported_bar_sign_reversal": [
            -1 if float(row["features"]["log_return_1"]) > 0
            else 1 if float(row["features"]["log_return_1"]) < 0 else 0
            for row in evaluation
        ],
        "risk_matched_always_long": [1] * len(evaluation),
        "risk_matched_always_short": [-1] * len(evaluation),
    }
    strategies: dict[str, object] = {}
    for name, directions in {"candidate": intents, **baseline_directions}.items():
        strategies[name] = {
            scenario: strategy_metrics(
                _directions_and_values(evaluation, directions, scenario), directions,
            )
            for scenario in SCENARIOS
        }

    candidate_stress = strategies["candidate"]["stress"]
    stress_values = _directions_and_values(evaluation, intents, "stress")
    positive_values = [value for value in stress_values if value > 0]
    largest_positive_share = (
        max(positive_values) / sum(positive_values) if positive_values else None
    )
    ratios = [
        float(row["stress_net_mfe_mae"]["long" if intent == 1 else "short"])
        for row, intent in zip(evaluation, intents, strict=True) if intent
    ]
    median_ratio = median(ratios) if ratios else None
    baseline_stress = {
        name: float(strategies[name]["stress"]["net_pnl_usd"])
        for name in MANDATORY_BASELINES
    }
    failed = []
    if float(candidate_stress["net_pnl_usd"]) <= 0:
        failed.append("STRESS_NET_PNL_NOT_POSITIVE")
    if int(candidate_stress["trade_count"]) < PILOT_MINIMUM_TRADES:
        failed.append("FEWER_THAN_20_TRADES")
    if median_ratio is None or median_ratio <= 1.0:
        failed.append("MEDIAN_STRESS_NET_MFE_MAE_NOT_GT_1")
    if largest_positive_share is None or largest_positive_share > 0.20:
        failed.append("POSITIVE_PNL_CONCENTRATION_GT_20_PERCENT")
    if any(float(candidate_stress["net_pnl_usd"]) <= value for value in baseline_stress.values()):
        failed.append("DID_NOT_BEAT_EVERY_MANDATORY_BASELINE")

    coefficients = [
        {
            "feature": name,
            "family": next(family for family, names in FEATURE_FAMILIES.items() if name in names),
            "standardized_coefficient": float(model.coefficients[index]),
        }
        for index, name in enumerate(FEATURE_NAMES)
    ]
    coefficients.sort(key=lambda item: -abs(float(item["standardized_coefficient"])))
    return {
        "pilot_decision": "PASS" if not failed else "FAIL",
        "failed_pilot_gates": failed,
        "selected_candidate_id": selection["selected_candidate_id"],
        "selected_ridge_penalty": penalty,
        "inner_selection": selection,
        "strategies": strategies,
        "baseline_stress_net_pnl_usd": baseline_stress,
        "median_stress_net_mfe_mae": median_ratio,
        "largest_positive_trade_contribution": largest_positive_share,
        "final_training_standardized_coefficients_report_only": coefficients,
        "evaluated_sessions": len(evaluation),
        "outer_folds_executed": 1,
        "remaining_frozen_outer_folds_not_executed": 7,
        "candidate_alternatives_charged_upfront": 3,
        "related_candidate_count": 12,
        "formal_significance_claim": False,
        "result_informed_feature_changes_allowed": False,
    }, ledger


def execute_once(root: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    protocol = load_protocol(root)
    mechanism, mechanism_path = build_mechanism(root, protocol)
    existing_result = ROOT_STATE / str(mechanism["mechanism_id"]) / "es_pilot_result.json"
    if (root / existing_result).exists():
        official = _read_json(root / existing_result)
        result = official.get("result")
        if not isinstance(result, Mapping):
            raise IntegrityError("existing multi-family pilot result is invalid")
        return {
            "status": "ALREADY_COMPLETED_UNPUBLISHED_ES_PILOT",
            "mechanism_id": mechanism["mechanism_id"],
            "result_path": existing_result.as_posix(),
            "result_id": official["result_id"],
            "pilot_decision": result["pilot_decision"],
        }

    activation = activate_successor(root, protocol)
    stage_binding = activation["binding"]
    if not isinstance(stage_binding, Mapping):
        raise IntegrityError("multi-family activation binding is absent")
    tier0_decision_path = _build_tier0(root, mechanism, mechanism_path)
    readiness = build_readiness_artifacts(root, mechanism, mechanism_path, stage_binding)
    superseded = _superseded_registration(root, str(mechanism["mechanism_id"]))
    if superseded is not None:
        old_binding = superseded.get("alpha_ladder_binding")
        if not isinstance(old_binding, Mapping):
            raise IntegrityError("result-free predecessor mechanism binding is absent")
        abort = _artifact({
            "schema_version": "fixed_hour_multifamily_mechanical_abort/1.0.0",
            "state": "ABORTED_BEFORE_PARQUET_ROW_DECODING_NO_MODEL_FIT_NO_RESULT",
            "trial_id": superseded["trial_id"],
            "mechanism_id": old_binding["mechanism_id"],
            "reason": "CATALOG_ENTRY_HAS_NO_PHYSICAL_BYTES_FIELD",
            "parquet_hash_bytes_read": True,
            "parquet_rows_decoded": 0,
            "feature_or_outcome_values_materialized": False,
            "model_fit_or_prediction": False,
            "performance_result_observed": False,
            "candidate_set_changed": False,
            "replacement_is_result_neutral_exact_successor": True,
        }, "abort_id")
        abort_path = ROOT_STATE / str(old_binding["mechanism_id"]) / "mechanical_abort.json"
        _write_or_verify(root / abort_path, abort)
    registration, registration_path, registration_sha = register(
        root,
        boundary,
        mechanism,
        mechanism_path,
        tier0_decision_path,
        stage_binding,
        readiness,
    )
    gateway = CertifiedResearchGateway(root=root, boundary=boundary)
    extras = {
        "approved_market": "ES",
        "approved_years": "2018,2019,2020",
        "approved_es_rows": str(APPROVED_ROWS),
        "approved_es_physical_bytes": str(APPROVED_BYTES),
        "outer_folds_executed": "1",
        "remaining_outer_folds_not_executed": "7",
        "new_candidate_alternatives_charged_upfront": "3",
        "prior_related_candidate_count": "9",
        "related_candidate_count_after_registration": "12",
        "tier1_or_other_market_row_access": "false",
        "2021_plus_holdout_or_forward_access": "false",
        "provider_access": "false",
        "publication": "false",
        "trading": "false",
    }
    scope = gateway.execution_scope(
        registration_path=root / registration_path,
        expected_registration_sha256=registration_sha,
        additional_scope=extras,
    )
    plan = _artifact({
        "schema_version": "fixed_hour_multifamily_es_pilot_execution_plan/1.0.0",
        "state": "USER_APPROVED_BOUNDED_ES_PILOT",
        "operation": CERTIFIED_TRIAL_EXECUTION_OPERATION,
        "trial_id": registration["trial_id"],
        "registration_path": registration_path.as_posix(),
        "registration_sha256": registration_sha,
        "scope": scope,
    }, "plan_id")
    plan_path = ROOT_STATE / str(mechanism["mechanism_id"]) / "execution_plan.json"
    _write_or_verify(root / plan_path, plan)
    plan_sha = sha256_file(root / plan_path)
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=plan_sha,
        approval_line=(
            f"APPROVE {CERTIFIED_TRIAL_EXECUTION_OPERATION} PLAN "
            f"{plan['plan_id']} SHA256 {plan_sha}"
        ),
    )
    use_path = gateway.claim_historical_execution(
        registration_path=root / registration_path,
        expected_registration_sha256=registration_sha,
        receipt=receipt,
        additional_scope=extras,
    )

    prepared, access = read_pilot_rows(
        root,
        readiness["training"],
        readiness["embargo"],
        readiness["evaluation"],
    )
    result, ledger = evaluate_pilot(
        prepared,
        readiness["training"],
        readiness["evaluation"],
        access["inner"],
    )
    ledger_artifact = _artifact({
        "schema_version": "fixed_hour_multifamily_es_pilot_session_ledger/1.0.0",
        "trial_id": registration["trial_id"],
        "mechanism_id": mechanism["mechanism_id"],
        "session_count": len(ledger),
        "records": ledger,
    }, "ledger_id")
    ledger_path = ROOT_STATE / str(mechanism["mechanism_id"]) / "es_pilot_session_ledger.json"
    _write_or_verify(root / ledger_path, ledger_artifact)
    disposition = (
        "PILOT_PASS_LATER_STAGES_BLOCKED"
        if result["pilot_decision"] == "PASS"
        else "CLOSED_NONRETRYABLE"
    )
    official = _artifact({
        "schema_version": "fixed_hour_multifamily_es_pilot_result/1.0.0",
        "state": "SEALED_UNPUBLISHED_ES_PILOT_COMPLETE",
        "mechanism_disposition": disposition,
        "result_free_mechanical_abort_predecessor_trial_id": (
            None if superseded is None else superseded["trial_id"]
        ),
        "trial_id": registration["trial_id"],
        "mechanism_id": mechanism["mechanism_id"],
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "session_ledger_binding": {
            "path": ledger_path.as_posix(),
            "sha256": sha256_file(root / ledger_path),
            "ledger_id": ledger_artifact["ledger_id"],
        },
        "observed_access": access,
        "result": result,
        "tier1_or_other_market_evaluation": False,
        "2021_plus_holdout_or_forward_access": False,
        "provider_or_network": False,
        "publication": False,
        "trading": False,
    }, "result_id")
    _write_or_verify(root / existing_result, official)
    return {
        "status": "COMPLETED_UNPUBLISHED_ES_PILOT",
        "activation": activation["activation"],
        "stage_2_5_contract_id": stage_binding["contract_id"],
        "mechanism_id": mechanism["mechanism_id"],
        "trial_id": registration["trial_id"],
        "result_path": existing_result.as_posix(),
        "result_id": official["result_id"],
        "ledger_path": ledger_path.as_posix(),
        "pilot_decision": result["pilot_decision"],
        "mechanism_disposition": disposition,
        "selected_ridge_penalty": result.get("selected_ridge_penalty"),
        "candidate_stress": result.get("strategies", {}).get("candidate", {}).get("stress"),
        "failed_pilot_gates": result.get("failed_pilot_gates"),
    }


if __name__ == "__main__":
    print(json.dumps(execute_once(Path.cwd()), sort_keys=True, separators=(",", ":")))
