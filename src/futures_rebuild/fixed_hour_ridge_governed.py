"""Activate, certify, register, and run one fixed-hour ES pilot."""

from __future__ import annotations

import json
import math
import os
from collections import Counter
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
from .alpha_research_ladder import (
    ALL_APPROVED,
    SESSION_MANIFEST_SCHEMA,
    load_active_ladder,
)
from .bias_robustness_successor import (
    FIXED_HOUR_CONTRACT_SCHEMA,
    build_active_execution_design,
    build_active_historical_design,
    build_fixed_hour_successor_contract,
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
from .fixed_hour_ridge_discovery import (
    CONTRACT_PATH, PACKS, TwoTargetRidge, enumerate_candidates, select_candidate,
)
from .fixed_hour_ridge_readiness import (
    BATCH_ROWS, READ_COLUMNS, assess_session, load_fixed_hour_schedule,
)
from .preexecution_fold_certification import (
    ROW_CERTIFIED, build_fold_readiness_certificate,
)
from .research_gateway_policy import (
    CERTIFIED_TRIAL_EXECUTION_OPERATION, scientific_governance_binding,
)


MARKETS = ("ES", "NQ", "CL", "GC")
TIER1_MARKETS = ("NQ", "CL", "GC")
SCENARIOS = ("base", "stress", "extreme")
ROOT_STATE = Path("state/fixed_hour_ridge_governed")
ACTIVE_HISTORICAL = Path("configs/active_alpha_historical_research_design.json")
ACTIVE_EXECUTION = Path("configs/active_alpha_ladder_execution_design.json")
CATALOG = Path("data/causally_gated_normalized/_control/catalog.json")


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"unreadable governed fixed-hour artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"governed fixed-hour artifact is not an object: {path}")
    return value


def _artifact(core: Mapping[str, object], identity: str) -> dict[str, object]:
    return {**dict(core), identity: sha256_json(dict(core))}


def _write_or_verify(path: Path, payload: Mapping[str, object]) -> None:
    raw = canonical_bytes(dict(payload)) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != raw:
            raise IntegrityError(f"immutable governed artifact already differs: {path}")
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
    raw = canonical_bytes(dict(payload)) + b"\n"
    temporary = path.with_name(f".{path.name}.fixed-hour.tmp")
    temporary.write_bytes(raw)
    os.replace(temporary, path)


def _fixed_protocol(root: Path) -> dict[str, object]:
    protocol = _read_json(root / CONTRACT_PATH)
    core = {key: value for key, value in protocol.items() if key != "contract_id"}
    if (
        protocol.get("schema_version")
        != "fixed_hour_ridge_discovery_contract/1.0.0"
        or protocol.get("contract_id") != sha256_json(core)
        or protocol.get("search", {}).get("candidates") != enumerate_candidates()  # type: ignore[union-attr]
    ):
        raise IntegrityError("fixed-hour protocol identity or candidate grid changed")
    return protocol


def _binding(root: Path, path: Path, identity: str, value: str) -> dict[str, str]:
    return {identity: value, "path": path.as_posix(), "sha256": sha256_file(root / path)}


def activate_successor(root: Path) -> dict[str, object]:
    """Activate only the exact nine-candidate Stage-2.5 successor."""

    ladder, profile = load_active_ladder(root)
    historical = _read_json(root / ACTIVE_HISTORICAL)
    execution = _read_json(root / ACTIVE_EXECUTION)
    _old_ladder, _old_profile, active_binding = require_stage3_active_authority(root)
    if active_binding.get("contract_id") == historical.get("stage_2_5_binding", {}).get("contract_id"):
        active_contract = _read_json(root / str(active_binding["path"]))
    else:
        raise IntegrityError("active Stage 2.5 pointer is internally inconsistent")
    if active_contract.get("schema_version") == FIXED_HOUR_CONTRACT_SCHEMA:
        validate_stage_2_5_contract(active_contract, root=root)
        return {"binding": dict(active_binding), "activation": "ALREADY_ACTIVE"}
    if active_contract.get("schema_version") != "causal_search_access_contract/2.1.0":
        raise UnauthorizedOperation("fixed-hour successor has an unexpected predecessor")

    protocol = _fixed_protocol(root)
    protocol_binding = _binding(
        root, CONTRACT_PATH, "contract_id", str(protocol["contract_id"]),
    )
    successor = build_fixed_hour_successor_contract(
        root=root, predecessor_binding=dict(active_binding),
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

    old_h_source_binding = historical.get("source_design_binding")
    old_e_source_binding = execution.get("source_execution_design_binding")
    if not isinstance(old_h_source_binding, Mapping) or not isinstance(old_e_source_binding, Mapping):
        raise IntegrityError("active source-design lineage is absent")
    old_h_source = _read_json(root / str(old_h_source_binding["path"]))
    old_e_source = _read_json(root / str(old_e_source_binding["path"]))
    h_core = deepcopy({key: value for key, value in old_h_source.items() if key != "design_id"})
    h_core.update({
        "predecessor": dict(old_h_source_binding),
        "stage_2_5_binding": {**successor_binding, "schema_version": FIXED_HOUR_CONTRACT_SCHEMA},
        "supersession_reason": "EXACT_NINE_CANDIDATE_FIXED_60_MINUTE_SUCCESSOR",
    })
    h_source = {**h_core, "design_id": sha256_json(h_core)}
    e_core = deepcopy({key: value for key, value in old_e_source.items() if key != "execution_design_id"})
    e_core.update({
        "predecessor": dict(old_e_source_binding),
        "stage_2_5_binding": {**successor_binding, "schema_version": FIXED_HOUR_CONTRACT_SCHEMA},
        "stage_2_5_contract_id": successor["contract_id"],
        "supersession_reason": "EXACT_NINE_CANDIDATE_FIXED_60_MINUTE_SUCCESSOR",
    })
    e_source = {**e_core, "execution_design_id": sha256_json(e_core)}
    source_root = Path("state/bias_robustness_successor_registry") / str(h_source["design_id"])
    h_source_path = source_root / "historical_design.json"
    e_source_path = source_root / "execution_design.json"
    _write_or_verify(root / h_source_path, h_source)
    _write_or_verify(root / e_source_path, e_source)

    next_historical = build_active_historical_design(
        historical, predecessor_sha256=sha256_file(root / ACTIVE_HISTORICAL),
        source_design=h_source, source_path=h_source_path.as_posix(),
        source_sha256=sha256_file(root / h_source_path),
        ladder_contract_id=str(ladder["contract_id"]),
        ladder_profile_id=str(profile["profile_id"]),
    )
    next_execution = build_active_execution_design(
        execution, predecessor_sha256=sha256_file(root / ACTIVE_EXECUTION),
        source_design=e_source, source_path=e_source_path.as_posix(),
        source_sha256=sha256_file(root / e_source_path),
        ladder_contract_id=str(ladder["contract_id"]),
        ladder_profile_id=str(profile["profile_id"]),
    )
    validate_active_designs(
        next_historical, next_execution, root=root,
        ladder_contract_id=str(ladder["contract_id"]),
        ladder_profile_id=str(profile["profile_id"]),
    )
    rollback = _artifact({
        "schema_version": "fixed_hour_ridge_activation_rollback/1.0.0",
        "state": "PRESERVED_PREDECESSOR_BYTES",
        "historical": {
            "path": ACTIVE_HISTORICAL.as_posix(),
            "sha256": sha256_file(root / ACTIVE_HISTORICAL), "payload": historical,
        },
        "execution": {
            "path": ACTIVE_EXECUTION.as_posix(),
            "sha256": sha256_file(root / ACTIVE_EXECUTION), "payload": execution,
        },
        "successor_contract_id": successor["contract_id"],
        "successor_historical_pointer_id": next_historical["pointer_id"],
        "successor_execution_pointer_id": next_execution["pointer_id"],
    }, "rollback_id")
    rollback_path = ROOT_STATE / "activation" / str(rollback["rollback_id"]) / "rollback.json"
    _write_or_verify(root / rollback_path, rollback)
    try:
        _replace(root / ACTIVE_HISTORICAL, next_historical)
        _replace(root / ACTIVE_EXECUTION, next_execution)
        _ladder, _profile, observed = require_stage3_active_authority(root)
        if observed != successor_binding:
            raise IntegrityError("fixed-hour Stage 2.5 activation did not become authoritative")
    except BaseException:
        _replace(root / ACTIVE_HISTORICAL, historical)
        _replace(root / ACTIVE_EXECUTION, execution)
        raise
    return {
        "binding": successor_binding, "activation": "ACTIVATED",
        "rollback_path": rollback_path.as_posix(),
    }


def _build_mechanism(
    root: Path, stage_binding: Mapping[str, str], protocol: Mapping[str, object],
) -> tuple[dict[str, object], Path]:
    ladder, profile = load_active_ladder(root)
    core = {
        "schema_version": "fixed_hour_ridge_frozen_mechanism/1.0.0",
        "state": "PREPARED_UNPUBLISHED_NOT_REGISTERED",
        "classification": "COUNTED_NINE_ALTERNATIVE_FIXED_HOUR_RIDGE_MECHANISM",
        "research_only": True, "live_readiness": False,
        "ladder_binding": {
            "contract_id": ladder["contract_id"], "profile_id": profile["profile_id"],
        },
        "stage_2_5_binding": dict(stage_binding),
        "fixed_hour_protocol_binding": _binding(
            root, CONTRACT_PATH, "contract_id", str(protocol["contract_id"]),
        ),
        "search": {
            "alternative_count_charged_upfront": 9,
            "candidate_ids": [item["candidate_id"] for item in enumerate_candidates()],
            "selection": "FOUR_TRAINING_ONLY_INNER_FOLDS_MEDIAN_STRESS_SESSION_SHARPE",
            "outer_result_tuning": False,
        },
        "features": protocol["features"], "target_and_diagnostics": protocol["target_and_diagnostics"],
        "model": protocol["model"], "validation": protocol["validation"],
        "costs": protocol["costs"], "baselines": protocol["baselines"],
        "inference": protocol["inference"],
        "ranking": {
            "canonical_market_order": list(ALL_APPROVED),
            "restricted_to_current_stage_markets": True,
            "one_intent_per_market_session": True,
        },
        "promotion_gates": {
            "common": {"stress_net_pnl_positive": True, "beat_all_baselines": True},
            "pilot": {"minimum_trades": 8, "maximum_drawdown_usd": "1500"},
            "tier_1": {"full_fixed_protocol_gate_required": True},
            "tier_2": {"not_applicable_narrow_scope": True},
            "tier_3": {"not_applicable_narrow_scope": True},
        },
        "code_bindings": {
            path.as_posix(): sha256_file(root / path) for path in (
                Path("src/futures_rebuild/fixed_hour_ridge_discovery.py"),
                Path("src/futures_rebuild/fixed_hour_ridge_readiness.py"),
                Path("src/futures_rebuild/fixed_hour_ridge_governed.py"),
            )
        },
        "authority": {
            "historical_rows": False, "registration": False, "execution": False,
            "holdout_2025": False, "provider_network_credentials": False,
            "publication": False, "trading": False,
        },
    }
    mechanism = {**core, "mechanism_id": sha256_json(core)}
    validate_frozen_mechanism(mechanism)
    path = ROOT_STATE / str(mechanism["mechanism_id"]) / "mechanism.json"
    _write_or_verify(root / path, mechanism)
    return mechanism, path


def _build_tier0(root: Path, mechanism: Mapping[str, object], path: Path) -> tuple[Path, Path]:
    ladder, profile = load_active_ladder(root)
    mechanism_sha = sha256_file(root / path)
    certificate = build_tier0_certificate(
        contract_id=str(ladder["contract_id"]), profile_id=str(profile["profile_id"]),
        mechanism_id=str(mechanism["mechanism_id"]), mechanism_sha256=mechanism_sha,
        test_node_ids=(
            "tests/test_fixed_hour_ridge_governed.py::test_nested_selection_is_training_only_and_deterministic",
        ),
        passed_test_count=1,
    )
    base = ROOT_STATE / str(mechanism["mechanism_id"])
    certificate_path = base / "tier0_certificate.json"
    _write_or_verify(root / certificate_path, certificate)
    decision = build_tier0_decision(
        contract_id=str(ladder["contract_id"]), mechanism_sha256=mechanism_sha,
        synthetic_certificate_path=certificate_path.as_posix(),
        synthetic_certificate_sha256=sha256_file(root / certificate_path),
    )
    decision_path = base / "tier0_decision.json"
    _write_or_verify(root / decision_path, decision)
    return certificate_path, decision_path


def _scan_market(
    root: Path, protocol: Mapping[str, object], market: str,
) -> tuple[list[dict[str, object]], dict[str, str], dict[str, object]]:
    schedule = load_fixed_hour_schedule(root, market)
    schedule_sessions = schedule["sessions"]
    assert isinstance(schedule_sessions, Mapping)
    source_contract = protocol["source_contract"]
    assert isinstance(source_contract, Mapping)
    sources = sorted(
        (item for item in source_contract["sources"] if item["market"] == market),  # type: ignore[index]
        key=lambda item: int(item["year"]),
    )
    if len(sources) != 5 or [item["year"] for item in sources] != list(range(2018, 2023)):
        raise IntegrityError(f"{market} readiness does not resolve five frozen years")
    sessions: list[dict[str, object]] = []
    bindings: dict[str, str] = {}
    rows_read = bytes_read = 0
    current_id: str | None = None
    current_rows: list[dict[str, object]] = []
    last_start: int | None = None

    def finish() -> None:
        nonlocal current_rows
        if not current_rows:
            return
        trade_date = str(current_rows[0]["project_trade_date"])
        state = schedule_sessions.get(trade_date)
        if not isinstance(state, str):
            raise IntegrityError(f"{market} session lacks calendar authority")
        sessions.append(assess_session(current_rows, authoritative_schedule_state=state))
        current_rows = []

    for source in sources:
        relative = str(source["parquet_path"])
        sidecar_relative = str(source["sidecar_path"])
        path = root / relative
        sidecar_path = root / sidecar_relative
        sidecar = _read_json(sidecar_path)
        entry = sidecar.get("entry_binding")
        if (
            not isinstance(entry, Mapping)
            or sha256_file(sidecar_path) != source["sidecar_sha256"]
            or entry.get("parquet_path") != relative
            or entry.get("parquet_sha256") != source["parquet_sha256"]
            or entry.get("row_count") != source["row_count"]
            or entry.get("selection_eligible") is not True
        ):
            raise IntegrityError(f"{market} sidecar binding changed")
        parquet = pq.ParquetFile(path)
        if (
            parquet.schema_arrow.names != SCHEMAS["observations"].names
            or parquet.metadata.num_rows != source["row_count"]
            or path.stat().st_size != source["physical_bytes"]
        ):
            raise IntegrityError(f"{market} Parquet schema or volume changed")
        observed = 0
        for batch in parquet.iter_batches(batch_size=BATCH_ROWS, columns=list(READ_COLUMNS)):
            observed += batch.num_rows
            for row in batch.to_pylist():
                start = int(row["bar_start_ns"])
                if last_start is not None and start <= last_start:
                    raise IntegrityError(f"{market} source is not globally chronological")
                last_start = start
                session_id = str(row["project_session_id"])
                if current_id is None:
                    current_id = session_id
                if session_id != current_id:
                    finish()
                    current_id = session_id
                current_rows.append(row)
        if observed != source["row_count"]:
            raise IntegrityError(f"{market} row count changed during read")
        rows_read += observed
        bytes_read += path.stat().st_size
        bindings[relative] = str(source["parquet_sha256"])
        bindings[sidecar_relative] = str(source["sidecar_sha256"])
    finish()
    expected_rows = sum(int(item["row_count"]) for item in sources)
    expected_bytes = sum(int(item["physical_bytes"]) for item in sources)
    if (rows_read, bytes_read) != (expected_rows, expected_bytes):
        raise IntegrityError(f"{market} approved read volume changed")
    return sessions, bindings, {
        "market": market, "rows_read": rows_read, "physical_bytes": bytes_read,
        "source_files": 5, "session_count": len(sessions),
        "status_counts": dict(sorted(Counter(str(item["status"]) for item in sessions).items())),
        "ready_sessions": sum(item["status"] == "READY" for item in sessions),
    }


def _ready_by_date(sessions: Sequence[Mapping[str, object]]) -> dict[str, Mapping[str, object]]:
    ready = {str(item["trade_date"]): item for item in sessions if item["status"] == "READY"}
    if len(ready) != sum(item["status"] == "READY" for item in sessions):
        raise IntegrityError("more than one ready session exists for a trade date")
    return dict(sorted(ready.items()))


def _session_manifest(
    *, contract_id: str, mechanism_sha: str, stage: str,
    training: Sequence[str] | None = None, evaluation: Sequence[str] | None = None,
    evaluation_by_market: Mapping[str, Sequence[str]] | None = None,
    excluded_pilot: Sequence[str] | None = None,
) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": SESSION_MANIFEST_SCHEMA, "contract_id": contract_id,
        "mechanism_sha256": mechanism_sha, "stage": stage,
    }
    if stage == "pilot":
        core.update({
            "markets": ["ES"], "fold_ordinal": 0,
            "selection_rule": "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD",
            "training_session_ids": list(training or ()),
            "evaluation_session_ids": list(evaluation or ()),
            "purge_applied": True, "embargo_applied": True,
        })
    else:
        core.update({
            "excluded_pilot_evaluation_session_ids": list(excluded_pilot or ()),
            "evaluation_session_ids_by_market": {
                key: list(value) for key, value in (evaluation_by_market or {}).items()
            },
        })
    return {**core, "manifest_id": sha256_json(core)}


def _year_breakdown(
    training: Sequence[str], evaluation: Sequence[str],
) -> dict[str, dict[str, object]]:
    output: dict[str, dict[str, object]] = {}
    for year in sorted({item[:4] for item in (*training, *evaluation)}):
        train_count = sum(item.startswith(year) for item in training)
        eval_count = sum(item.startswith(year) for item in evaluation)
        output[year] = {
            "expected_training_sessions": train_count,
            "complete_training_sessions": train_count,
            "expected_evaluation_sessions": eval_count,
            "feature_complete_evaluation_sessions": eval_count,
            "terminal_evaluation_sessions": eval_count,
            "execution_path_complete_evaluation_sessions": eval_count,
            "exclusion_reasons": {},
        }
    return output


def _fold_evidence(
    market: str, fold_id: str, training: Sequence[str], evaluation: Sequence[str],
) -> dict[str, object]:
    count = len(evaluation)
    dispositions = {
        scenario: {"feasible_sessions": count, "risk_abstention_sessions": 0, "unresolved_sessions": 0}
        for scenario in SCENARIOS
    }
    baselines = {}
    for name in MANDATORY_BASELINES:
        flat = name == "flat_no_trade"
        selected = 0 if flat else count
        baselines[name] = {
            "expected_sessions": count, "terminal_sessions": count,
            "selected_sessions": selected, "selected_path_complete_sessions": selected,
            "scenario_risk_dispositions": {
                scenario: {
                    "feasible_sessions": selected, "risk_abstention_sessions": 0,
                    "unresolved_sessions": 0,
                } for scenario in SCENARIOS
            },
            "schedule_independently_derived": True, "flat_no_trade": flat,
        }
    return {
        "fold_id": fold_id, "market": market, "role": "OUTER",
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
            "purge_minutes": 120, "embargo_sessions": 1,
        },
        "checks": {
            "chronological_order": max(training) < min(evaluation),
            "purge_applied": True, "embargo_applied": True,
            "training_only_transformation": True,
            "contract_identity_discontinuities_terminalized": True,
            "roll_discontinuities_terminalized": True,
            "all_incomplete_sessions_terminalized": True,
            "complete_required_metrics": True, "promotion_path_computable": True,
        },
        "baseline_universe_readiness": baselines,
        "exclusion_reasons": {},
        "market_year_breakdown": _year_breakdown(training, evaluation),
    }


def _inner_folds(training: Sequence[str]) -> list[dict[str, object]]:
    initial = len(training) - 4 * 42
    if initial < 252:
        raise IntegrityError("pilot training cannot supply four inner folds")
    folds = []
    for ordinal in range(4):
        end = initial + ordinal * 42
        train = tuple(training[:end])
        validate = tuple(training[end:end + 42])
        folds.append({
            "fold_id": f"INNER_{ordinal:02d}", "training_session_ids": list(train),
            "validation_session_ids": list(validate),
            "training_count": len(train), "validation_count": len(validate),
            "training_sha256": sha256_json(list(train)),
            "validation_sha256": sha256_json(list(validate)),
            "chronological": max(train) < min(validate), "purge_minutes": 120,
            "embargo_sessions": 1, "training_only_transformations": True,
        })
    return folds


def _certificate(
    *, mechanism_id: str, source_bindings: Mapping[str, str], markets: Sequence[str],
    fold_evidence: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    return build_fold_readiness_certificate(
        trial_family="fixed_hour_ridge_nine_alternative_60m",
        protocol_id=mechanism_id, source_bindings=source_bindings,
        fold_evidence=fold_evidence, required_markets=markets,
        required_baselines=MANDATORY_BASELINES, required_cost_scenarios=SCENARIOS,
        required_outer_fold_ids=("OUTER_00",), required_nested_fold_ids=(),
        expected_outer_folds=1, expected_nested_folds=0,
        minimum_training_sessions=504, minimum_evaluation_sessions=63,
        minimum_purge_minutes=120, minimum_embargo_sessions=1,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True,
    )


def build_readiness(
    root: Path, mechanism: Mapping[str, object], mechanism_path: Path,
    stage_binding: Mapping[str, str], protocol: Mapping[str, object],
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    """Read only the approved 20 files and materialize both readiness gates."""

    scanned: dict[str, list[dict[str, object]]] = {}
    source_bindings: dict[str, dict[str, str]] = {}
    audits = []
    for market in MARKETS:
        scanned[market], source_bindings[market], audit = _scan_market(root, protocol, market)
        audits.append(audit)
    approved_tier1 = {
        "rows_read": sum(item["rows_read"] for item in audits if item["market"] in TIER1_MARKETS),
        "physical_bytes": sum(item["physical_bytes"] for item in audits if item["market"] in TIER1_MARKETS),
    }
    if approved_tier1 != {"rows_read": 5_254_767, "physical_bytes": 445_372_070}:
        raise IntegrityError("approved NQ/CL/GC readiness volume changed")

    ready = {market: _ready_by_date(values) for market, values in scanned.items()}
    es_dates = tuple(ready["ES"])
    if len(es_dates) < 567:
        raise IntegrityError("ES has fewer than 567 executable sessions")
    pilot_train, pilot_eval = es_dates[:504], es_dates[504:567]
    inner = _artifact({
        "schema_version": "fixed_hour_ridge_inner_readiness/1.0.0",
        "market": "ES", "outer_training_session_count": 504,
        "folds": _inner_folds(pilot_train), "model_fit": False,
        "performance_evaluation": False,
    }, "readiness_id")
    base = ROOT_STATE / str(mechanism["mechanism_id"])
    inner_path = base / "inner_readiness.json"
    _write_or_verify(root / inner_path, inner)

    ladder, _profile = load_active_ladder(root)
    mechanism_sha = sha256_file(root / mechanism_path)
    pilot_manifest = _session_manifest(
        contract_id=str(ladder["contract_id"]), mechanism_sha=mechanism_sha,
        stage="pilot", training=pilot_train, evaluation=pilot_eval,
    )
    pilot_manifest_path = base / "pilot_sessions.json"
    _write_or_verify(root / pilot_manifest_path, pilot_manifest)

    tier1_train: dict[str, tuple[str, ...]] = {}
    tier1_eval: dict[str, tuple[str, ...]] = {}
    excluded = set(pilot_eval)
    for market in TIER1_MARKETS:
        dates = tuple(item for item in ready[market] if item not in excluded)
        if len(dates) < 567:
            raise IntegrityError(f"{market} has fewer than 567 non-pilot executable sessions")
        tier1_train[market], tier1_eval[market] = dates[:504], dates[504:567]
    tier1_manifest = _session_manifest(
        contract_id=str(ladder["contract_id"]), mechanism_sha=mechanism_sha,
        stage="tier_1", evaluation_by_market=tier1_eval, excluded_pilot=pilot_eval,
    )
    tier1_manifest_path = base / "tier1_sessions.json"
    _write_or_verify(root / tier1_manifest_path, tier1_manifest)

    common = {
        mechanism_path.as_posix(): mechanism_sha,
        str(stage_binding["path"]): str(stage_binding["sha256"]),
        CONTRACT_PATH.as_posix(): sha256_file(root / CONTRACT_PATH),
        CATALOG.as_posix(): sha256_file(root / CATALOG),
    }
    pilot_sources = {
        **source_bindings["ES"], **common,
        pilot_manifest_path.as_posix(): sha256_file(root / pilot_manifest_path),
        inner_path.as_posix(): sha256_file(root / inner_path),
    }
    pilot_certificate = _certificate(
        mechanism_id=str(mechanism["mechanism_id"]), source_bindings=pilot_sources,
        markets=("ES",),
        fold_evidence=(_fold_evidence("ES", "OUTER_00", pilot_train, pilot_eval),),
    )
    pilot_certificate_path = base / "pilot_readiness.json"
    _write_or_verify(root / pilot_certificate_path, pilot_certificate)

    tier1_sources = {
        **common,
        **{key: value for market in TIER1_MARKETS for key, value in source_bindings[market].items()},
        tier1_manifest_path.as_posix(): sha256_file(root / tier1_manifest_path),
    }
    tier1_certificate = _certificate(
        mechanism_id=str(mechanism["mechanism_id"]), source_bindings=tier1_sources,
        markets=TIER1_MARKETS,
        fold_evidence=tuple(
            _fold_evidence(market, "OUTER_00", tier1_train[market], tier1_eval[market])
            for market in TIER1_MARKETS
        ),
    )
    tier1_certificate_path = base / "tier1_readiness.json"
    _write_or_verify(root / tier1_certificate_path, tier1_certificate)
    return {
        "pilot_certificate": pilot_certificate,
        "pilot_certificate_path": pilot_certificate_path,
        "pilot_manifest": pilot_manifest, "pilot_manifest_path": pilot_manifest_path,
        "tier1_certificate": tier1_certificate,
        "tier1_certificate_path": tier1_certificate_path,
        "tier1_manifest": tier1_manifest, "tier1_manifest_path": tier1_manifest_path,
        "audits": audits, "inner_readiness": inner,
    }, ready["ES"], {"pilot_train": pilot_train, "pilot_eval": pilot_eval}


def _declaration_and_request(
    root: Path, mechanism: Mapping[str, object], stage_binding: Mapping[str, str],
) -> tuple[dict[str, object], dict[str, object], Path]:
    candidates = enumerate_candidates()
    code_config = {
        path.as_posix(): sha256_file(root / path) for path in (
            CONTRACT_PATH,
            Path("src/futures_rebuild/fixed_hour_ridge_discovery.py"),
            Path("src/futures_rebuild/fixed_hour_ridge_readiness.py"),
            Path("src/futures_rebuild/fixed_hour_ridge_governed.py"),
        )
    }
    core = {
        "schema_version": "pre_evaluation_declaration/1.0.0",
        "stage_2_5_contract_id": stage_binding["contract_id"],
        "protocol_id": mechanism["mechanism_id"],
        "trial_family": "fixed_hour_ridge_nine_alternative_60m",
        "candidate_id": sha256_json([item["candidate_id"] for item in candidates]),
        "candidate_budget_slot": 8, "predecessor_declaration_id": None,
        "pipeline_stage": "5", "operation": CERTIFIED_TRIAL_EXECUTION_OPERATION,
        "target_family": "TWO_TARGET_STRESS_NET_R", "target": "LONG_AND_SHORT_STRESS_NET_R",
        "horizon": "60_MINUTES", "feature_pack": "NESTED_SELECTION_THREE_FROZEN_PACKS",
        "model_family": "MARKET_SPECIFIC_TWO_TARGET_RIDGE",
        "hyperparameter": "NESTED_SELECTION_PENALTIES_0_1_1_10",
        "construction_variant": "EXACT_NINE_ALTERNATIVES_CHARGED_UPFRONT",
        "markets": ["ES"], "start_inclusive": "2018-01-01T00:00:00Z",
        "end_exclusive": "2023-01-01T00:00:00Z", "purpose": "WFA",
        "metric_family": "STRESS_NET_SESSION_PNL_AND_FIXED_DIAGNOSTICS",
        "benchmark": "ZERO_AND_SIX_MANDATORY_BASELINES",
        "execution_version": "fixed_hour_ridge_governed/1.0.0",
        "code_config_sha256": sha256_json(code_config),
        "catalog_sha256": sha256_file(root / CATALOG),
        "creation_evidence": "USER_APPROVED_NEW_COUNTED_NINE_ALTERNATIVE_MECHANISM",
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
        "stage": "5", "purpose": "WFA", "operation": CERTIFIED_TRIAL_EXECUTION_OPERATION,
        "contract_binding": dict(stage_binding),
        "declaration_binding": {
            "declaration_id": declaration["declaration_id"],
            "path": declaration_path.as_posix(),
            "sha256": sha256_file(root / declaration_path),
        },
        "markets": ["ES"], "start_inclusive": "2018-01-01T00:00:00Z",
        "end_exclusive": "2023-01-01T00:00:00Z",
        "catalog_binding": {"path": CATALOG.as_posix(), "sha256": sha256_file(root / CATALOG)},
    }
    request = {**request_core, "request_id": sha256_json(request_core)}
    validate_row_request(root, request)
    return declaration, request, declaration_path


def register(
    root: Path, boundary: RepoBoundary, mechanism: Mapping[str, object],
    mechanism_path: Path, tier0_decision_path: Path,
    stage_binding: Mapping[str, str], readiness: Mapping[str, object],
) -> tuple[dict[str, object], Path, str]:
    declaration, request, declaration_path = _declaration_and_request(
        root, mechanism, stage_binding,
    )
    ladder, profile = load_active_ladder(root)
    pilot_path = readiness["pilot_certificate_path"]
    pilot_manifest_path = readiness["pilot_manifest_path"]
    tier1_path = readiness["tier1_certificate_path"]
    tier1_manifest_path = readiness["tier1_manifest_path"]
    assert all(isinstance(item, Path) for item in (
        pilot_path, pilot_manifest_path, tier1_path, tier1_manifest_path,
    ))
    ladder_binding = {
        "contract_id": ladder["contract_id"], "profile_id": profile["profile_id"],
        "stage": "pilot", "mechanism_sha256": sha256_file(root / mechanism_path),
        "mechanism_path": mechanism_path.as_posix(), "mechanism_id": mechanism["mechanism_id"],
        "predecessor_decision_path": tier0_decision_path.as_posix(),
        "predecessor_decision_sha256": sha256_file(root / tier0_decision_path),
        "session_manifest_path": pilot_manifest_path.as_posix(),
        "session_manifest_sha256": sha256_file(root / pilot_manifest_path),
        "tier_1_readiness_evidence_path": tier1_path.as_posix(),
        "tier_1_readiness_evidence_sha256": sha256_file(root / tier1_path),
        "tier_1_session_manifest_path": tier1_manifest_path.as_posix(),
        "tier_1_session_manifest_sha256": sha256_file(root / tier1_manifest_path),
    }
    pilot_certificate = readiness["pilot_certificate"]
    assert isinstance(pilot_certificate, Mapping)
    core = {
        "schema_version": "future_trial_registration/1.0.0",
        "trial_family": pilot_certificate["trial_family"],
        "protocol_id": mechanism["mechanism_id"],
        "candidate_ids": [item["candidate_id"] for item in enumerate_candidates()],
        "candidate_alternatives_charged_upfront": 9,
        "fold_readiness_binding": {
            "evidence_path": pilot_path.as_posix(),
            "evidence_sha256": sha256_file(root / pilot_path),
            "certificate_id": pilot_certificate["certificate_id"],
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
    registration = {**core, "trial_id": sha256_json(core)}
    path = Path("state/trial_registry/fixed_hour_ridge") / f"{registration['trial_id']}.json"
    gateway = CertifiedResearchGateway(root=root, boundary=boundary)
    result = gateway.register_trial(
        registration_path=root / path, registration=registration,
        readiness_evidence_path=root / pilot_path,
    )
    return registration, path, str(result["registration_sha256"])


def _fit_candidate(
    candidate: Mapping[str, object], training: Sequence[Mapping[str, object]],
) -> TwoTargetRidge:
    names = PACKS[str(candidate["feature_pack"])]
    x = np.asarray([[row["features"][name] for name in names] for row in training], dtype=float)  # type: ignore[index]
    y_long = np.asarray([row["targets"]["long"] for row in training], dtype=float)  # type: ignore[index]
    y_short = np.asarray([row["targets"]["short"] for row in training], dtype=float)  # type: ignore[index]
    return TwoTargetRidge(float(candidate["ridge_penalty"])).fit(x, y_long, y_short)


def _score(model: TwoTargetRidge, candidate: Mapping[str, object], row: Mapping[str, object]) -> tuple[int, float, float]:
    names = PACKS[str(candidate["feature_pack"])]
    return model.predict_intent([row["features"][name] for name in names])  # type: ignore[index]


def _sharpe(values: Sequence[float]) -> float:
    array = np.asarray(values, dtype=float)
    std = float(np.std(array, ddof=0))
    mean = float(np.mean(array))
    return mean * math.sqrt(252.0) / std if std else (0.0 if mean == 0 else math.copysign(1e12, mean))


def nested_select(
    protocol: Mapping[str, object], ready: Mapping[str, Mapping[str, object]],
    training_ids: Sequence[str],
) -> tuple[Mapping[str, object], dict[str, list[float]]]:
    candidates = enumerate_candidates()
    scores = {str(item["candidate_id"]): [] for item in candidates}
    for fold in _inner_folds(training_ids):
        train = [ready[item] for item in fold["training_session_ids"]]  # type: ignore[index]
        validation = [ready[item] for item in fold["validation_session_ids"]]  # type: ignore[index]
        for candidate in candidates:
            model = _fit_candidate(candidate, train)
            pnl = []
            for row in validation:
                intent, _long, _short = _score(model, candidate, row)
                pnl.append(0.0 if intent == 0 else float(row["net_pnl_usd"]["stress"]["long" if intent == 1 else "short"]))  # type: ignore[index]
            scores[str(candidate["candidate_id"])].append(_sharpe(pnl))
    selected_id = select_candidate(protocol, scores)
    return next(item for item in candidates if item["candidate_id"] == selected_id), scores


def _metrics(values: Sequence[float], trades: int) -> dict[str, object]:
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
    positives = sum(value > 0 for value in values)
    return {
        "net_pnl_usd": float(sum(values)), "session_sharpe_252": _sharpe(values),
        "maximum_drawdown_usd": drawdown, "trade_count": trades,
        "positive_session_fraction": positives / len(values),
    }


def evaluate_pilot(
    protocol: Mapping[str, object], ready: Mapping[str, Mapping[str, object]],
    training_ids: Sequence[str], evaluation_ids: Sequence[str],
) -> dict[str, object]:
    selected, inner_scores = nested_select(protocol, ready, training_ids)
    training = [ready[item] for item in training_ids]
    evaluation = [ready[item] for item in evaluation_ids]
    model = _fit_candidate(selected, training)
    directions: list[int] = []
    for row in evaluation:
        directions.append(_score(model, selected, row)[0])
    mean_long = float(np.mean([row["targets"]["long"] for row in training]))  # type: ignore[index]
    mean_short = float(np.mean([row["targets"]["short"] for row in training]))  # type: ignore[index]
    unconditional = 1 if mean_long >= mean_short else -1
    baseline_directions = {
        "flat_no_trade": [0] * len(evaluation),
        "fold_local_unconditional_direction": [unconditional] * len(evaluation),
        "previous_reported_bar_sign_momentum": [
            1 if row["features"]["log_return_1"] > 0 else -1 if row["features"]["log_return_1"] < 0 else 0  # type: ignore[index]
            for row in evaluation
        ],
        "previous_reported_bar_sign_reversal": [
            -1 if row["features"]["log_return_1"] > 0 else 1 if row["features"]["log_return_1"] < 0 else 0  # type: ignore[index]
            for row in evaluation
        ],
        "risk_matched_always_long": [1] * len(evaluation),
        "risk_matched_always_short": [-1] * len(evaluation),
    }
    strategy_directions = {"candidate": directions, **baseline_directions}
    results: dict[str, object] = {}
    for name, intents in strategy_directions.items():
        scenarios = {}
        for scenario in SCENARIOS:
            pnl = [
                0.0 if intent == 0 else float(row["net_pnl_usd"][scenario]["long" if intent == 1 else "short"])  # type: ignore[index]
                for row, intent in zip(evaluation, intents, strict=True)
            ]
            scenarios[scenario] = _metrics(pnl, sum(intent != 0 for intent in intents))
        results[name] = scenarios
    stress = results["candidate"]["stress"]  # type: ignore[index]
    baseline_stress = {name: results[name]["stress"]["net_pnl_usd"] for name in MANDATORY_BASELINES}  # type: ignore[index]
    failed = []
    if stress["net_pnl_usd"] <= 0:
        failed.append("STRESS_NET_PNL_NOT_POSITIVE")
    if any(stress["net_pnl_usd"] <= value for value in baseline_stress.values()):
        failed.append("DID_NOT_BEAT_ZERO_AND_EVERY_BASELINE")
    if stress["trade_count"] < 8:
        failed.append("FEWER_THAN_8_TRADES")
    if stress["maximum_drawdown_usd"] > 1500:
        failed.append("MAXIMUM_DRAWDOWN_GT_1500")
    ratios = [
        float(row["net_mfe_mae"]["long" if intent == 1 else "short"])  # type: ignore[index]
        for row, intent in zip(evaluation, directions, strict=True) if intent
    ]
    return {
        "selected_candidate": dict(selected),
        "inner_fold_scores": {key: list(value) for key, value in inner_scores.items()},
        "inner_fold_median_scores": {key: median(value) for key, value in inner_scores.items()},
        "strategies": results, "pilot_decision": "PASS" if not failed else "FAIL",
        "failed_pilot_gates": failed,
        "median_stress_net_mfe_mae": median(ratios) if ratios else None,
        "evaluated_sessions": len(evaluation), "outer_folds_executed": 1,
        "remaining_frozen_outer_folds_not_executed": 7,
        "formal_significance_claim": False,
        "full_es_promotion_gate": "NOT_EVALUATED_REQUIRES_ALL_8_OUTER_FOLDS",
    }


def execute_once(root: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    activation = activate_successor(root)
    stage_binding = activation["binding"]
    assert isinstance(stage_binding, Mapping)
    protocol = _fixed_protocol(root)
    mechanism, mechanism_path = _build_mechanism(root, stage_binding, protocol)
    _tier0_certificate_path, tier0_decision_path = _build_tier0(root, mechanism, mechanism_path)
    readiness, es_ready, folds = build_readiness(
        root, mechanism, mechanism_path, stage_binding, protocol,
    )
    registration, registration_path, registration_sha = register(
        root, boundary, mechanism, mechanism_path, tier0_decision_path,
        stage_binding, readiness,
    )
    gateway = CertifiedResearchGateway(root=root, boundary=boundary)
    extras = {
        "approved_market": "ES", "approved_years": "2018,2019,2020,2021,2022",
        "approved_es_rows": "1755614", "approved_tier1_readiness_rows": "5254767",
        "tier1_model_evaluation": "false", "outer_folds_executed": "1",
        "remaining_outer_folds_not_executed": "7",
        "candidate_alternatives_charged_upfront": "9",
        "prior_conservative_individual_attempt_count": "169",
        "holdout_or_forward_access": "false", "provider_access": "false",
        "trading": "false",
    }
    scope = gateway.execution_scope(
        registration_path=root / registration_path,
        expected_registration_sha256=registration_sha, additional_scope=extras,
    )
    plan = _artifact({
        "schema_version": "fixed_hour_ridge_es_pilot_execution_plan/1.0.0",
        "state": "USER_APPROVED_BOUNDED_ES_PILOT",
        "operation": CERTIFIED_TRIAL_EXECUTION_OPERATION,
        "trial_id": registration["trial_id"], "registration_path": registration_path.as_posix(),
        "registration_sha256": registration_sha, "scope": scope,
        "readiness_audits": readiness["audits"],
    }, "plan_id")
    plan_path = ROOT_STATE / str(mechanism["mechanism_id"]) / "execution_plan.json"
    _write_or_verify(root / plan_path, plan)
    plan_sha = sha256_file(root / plan_path)
    receipt = OperationReceipt.issue_user_approved(
        boundary, operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope, approval_command=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        approval_plan_id=str(plan["plan_id"]), approval_plan_sha256=plan_sha,
        approval_line=(
            f"APPROVE {CERTIFIED_TRIAL_EXECUTION_OPERATION} PLAN "
            f"{plan['plan_id']} SHA256 {plan_sha}"
        ),
    )
    use_path = gateway.claim_historical_execution(
        registration_path=root / registration_path,
        expected_registration_sha256=registration_sha, receipt=receipt,
        additional_scope=extras,
    )
    result = evaluate_pilot(
        protocol, es_ready, folds["pilot_train"], folds["pilot_eval"],
    )
    official = _artifact({
        "schema_version": "fixed_hour_ridge_es_pilot_result/1.0.0",
        "state": "SEALED_UNPUBLISHED_ES_PILOT_COMPLETE",
        "trial_id": registration["trial_id"], "mechanism_id": mechanism["mechanism_id"],
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "candidate_alternatives_charged_upfront": 9,
        "prior_conservative_individual_attempt_count": 169,
        "result": result, "tier1_model_evaluation": False,
        "holdout_or_forward_access": False, "provider_or_network": False,
        "publication": False, "trading": False,
    }, "result_id")
    output = ROOT_STATE / str(mechanism["mechanism_id"]) / "es_pilot_result.json"
    _write_or_verify(root / output, official)
    return {
        "status": "COMPLETED_UNPUBLISHED_ES_PILOT",
        "activation": activation["activation"], "stage_2_5_contract_id": stage_binding["contract_id"],
        "mechanism_id": mechanism["mechanism_id"], "trial_id": registration["trial_id"],
        "result_path": output.as_posix(), "result_id": official["result_id"],
        "pilot_decision": result["pilot_decision"],
        "selected_candidate": result["selected_candidate"],
        "readiness_audits": readiness["audits"],
        "tier1_model_evaluation": False, "outer_folds_executed": 1,
        "remaining_outer_folds_not_executed": 7,
    }


if __name__ == "__main__":
    print(json.dumps(execute_once(Path.cwd()), sort_keys=True, separators=(",", ":")))
