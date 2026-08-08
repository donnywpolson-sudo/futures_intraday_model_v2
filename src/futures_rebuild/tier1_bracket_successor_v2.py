"""Prepare-only governance for the failed bracket trial and its v2 successor.

The builders read metadata and hashes only.  Registry/event writes are exposed
as create-only functions for an explicitly approved orchestration step; this
module never opens market rows, fits a model, evaluates, or touches the holdout.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Mapping

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError


FAILED_TRIAL_ID = "035955798cd0176732365b9706487ee3bfa6b1a4afa3d0047eeb1ee60744d3ba"
FAILED_TRIAL_REGISTRY = Path(
    "state/trial_registry/tier1_bracket_prediction"
) / f"current-{FAILED_TRIAL_ID}.json"
CLOSURE_PREPARATION = Path("configs/tier1_bracket_failed_trial_closure_preparation.json")
SUCCESSOR_CONTRACT = Path("configs/tier1_bracket_successor_v2.json")
CLOSURE_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_closure")
CLOSURE_EVENT_ROOT = Path("state/trial_events/tier1_bracket_closure")
SUCCESSOR_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_successor_v2")
SUCCESSOR_EVENT_ROOT = Path("state/trial_events/tier1_bracket_successor_v2")


@dataclass(frozen=True)
class PreparedRecord:
    record_id: str
    payload: Mapping[str, object]


def _load(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"cannot read {path.name}") from exc
    if not isinstance(payload, dict):
        raise IntegrityError(f"{path.name} must contain one JSON object")
    return payload


def load_failed_trial_closure_preparation(*, root: Path) -> dict[str, object]:
    payload = _load(root / CLOSURE_PREPARATION)
    bindings = payload.get("preserved_bindings")
    failure = payload.get("genuine_failure")
    defect = payload.get("evaluator_defect")
    if (
        payload.get("schema_version") != "tier1_bracket_failed_trial_closure_preparation/1.0.0"
        or payload.get("state") != "PREPARED_NOT_PUBLISHED_NOT_ACTIVE"
        or payload.get("disposition") != "FAILED_NO_RESCUE"
        or payload.get("trial_id") != FAILED_TRIAL_ID
        or payload.get("publication_authorized") is not False
        or payload.get("activation_authorized") is not False
        or not isinstance(bindings, dict)
        or not isinstance(failure, dict)
        or not isinstance(defect, dict)
        or failure.get("base_net_pnl_usd") != "-1509.400000"
        or defect.get("flat_no_trade_was_charged_candidate_costs") is not True
        or defect.get("active_baselines_reused_candidate_admissions") is not True
    ):
        raise IntegrityError("failed bracket trial closure preparation is invalid")
    registry_hash = bindings.get("trial_registry_sha256")
    if registry_hash != sha256_file(root / FAILED_TRIAL_REGISTRY):
        raise IntegrityError("failed trial registry changed after closure preparation")
    evaluation_id = bindings.get("evaluation_release_id")
    evaluation_hash = bindings.get("evaluation_manifest_sha256")
    if not isinstance(evaluation_id, str) or evaluation_hash != sha256_file(
        root / "manifests/data_releases/evaluations" / f"{evaluation_id}.json"
    ):
        raise IntegrityError("failed trial evaluation binding changed after closure preparation")
    diagnoses = bindings.get("diagnoses")
    if not isinstance(diagnoses, list) or len(diagnoses) != 2:
        raise IntegrityError("failed trial closure must preserve both local diagnoses")
    for item in diagnoses:
        if not isinstance(item, dict):
            raise IntegrityError("failed trial diagnosis binding is invalid")
        diagnosis_id = item.get("diagnosis_id")
        expected = item.get("sha256")
        if not isinstance(diagnosis_id, str) or expected != sha256_file(
            root / "reports/tier1_bracket_diagnosis" / diagnosis_id / "diagnosis.json"
        ):
            raise IntegrityError("failed trial diagnosis changed after closure preparation")
    return payload


def load_successor_v2_contract(*, root: Path) -> dict[str, object]:
    payload = _load(root / SUCCESSOR_CONTRACT)
    transformation = payload.get("transformation")
    model = payload.get("model")
    entry = payload.get("entry_policy")
    promotion = payload.get("promotion_gate")
    reporting = payload.get("reporting")
    forbidden = payload.get("forbidden_without_separate_approval")
    if (
        payload.get("schema_version") != "tier1_bracket_successor_v2_contract/1.0.0"
        or payload.get("state") != "PREPARED_NOT_REGISTERED"
        or payload.get("supersedes_failed_trial_id") != FAILED_TRIAL_ID
        or payload.get("locked_untouched_holdout") != "2025"
        or payload.get("failed_trial_use") != "DIAGNOSIS_ONLY_NO_PARAMETER_SELECTION"
        or not isinstance(transformation, dict)
        or transformation.get("family") != "MARKET_AND_OUTER_FOLD_TRAINING_ONLY_STANDARDIZATION"
        or transformation.get("test_or_holdout_fit") is not False
        or not isinstance(model, dict)
        or model.get("family") != "MARKET_SPECIFIC_TWO_TARGET_RIDGE"
        or model.get("hyperparameter_search") is not False
        or not isinstance(entry, dict)
        or entry.get("decision_checkpoints") != [
            "08:30 America/Chicago", "10:30 America/Chicago", "13:30 America/Chicago"
        ]
        or Decimal(str(entry.get("minimum_selected_predicted_net_r"))) != Decimal("0.25")
        or not isinstance(promotion, dict)
        or promotion.get("required_cost_scenario") != "stress"
        or promotion.get("minimum_positive_independent_portfolio_years_of_3") != 2
        or promotion.get("minimum_positive_market_years_of_12") != 6
        or promotion.get("maximum_continuous_drawdown_usd") != "1500"
        or not isinstance(reporting, dict)
        or set(reporting.values()) != {True}
        or not isinstance(forbidden, list)
        or "trial_registry_write" not in forbidden
        or "holdout_or_forward_access" not in forbidden
    ):
        raise IntegrityError("Tier 1 bracket successor v2 contract is invalid")
    return payload


def prepare_failed_trial_closure(*, root: Path) -> PreparedRecord:
    preparation = load_failed_trial_closure_preparation(root=root)
    core = {
        **preparation,
        "schema_version": "tier1_bracket_failed_trial_closure/1.0.0",
        "state": "PREPARED_FOR_CLOSURE_CONFIRMATION",
        "publication_authorized": False,
        "activation_authorized": False,
    }
    return PreparedRecord(sha256_json(core), core)


def prepare_successor_v2_registration(*, root: Path) -> PreparedRecord:
    contract = load_successor_v2_contract(root=root)
    failed_registry = _load(root / FAILED_TRIAL_REGISTRY)
    if failed_registry.get("trial_id") != FAILED_TRIAL_ID:
        raise IntegrityError("successor does not bind the current failed bracket trial")
    source_pairs = failed_registry.get("source_pairs")
    if not isinstance(source_pairs, list) or len(source_pairs) != 20:
        raise IntegrityError("successor registration requires the preserved 20 source-pair bindings")
    core = {
        "schema_version": "tier1_bracket_successor_v2_registration/1.0.0",
        "state": "REGISTERED_BEFORE_SOURCE_ROW_OR_OUTCOME_ACCESS",
        "research_only": True,
        "live_readiness": False,
        "contract": contract,
        "contract_sha256": sha256_file(root / SUCCESSOR_CONTRACT),
        "failed_trial_id": FAILED_TRIAL_ID,
        "failed_trial_closure_preparation_sha256": sha256_file(root / CLOSURE_PREPARATION),
        "source_pairs": source_pairs,
        "phase8_index_release_id": failed_registry.get("phase8_index_release_id"),
        "phase8_audit_release_id": failed_registry.get("phase8_audit_release_id"),
        "phase8_evaluation_config_sha256": failed_registry.get("phase8_evaluation_config_sha256"),
        "risk_profile_sha256": failed_registry.get("risk_profile_sha256"),
        "rulebook_sha256": failed_registry.get("rulebook_sha256"),
        "evaluator_schema_version": "tier1_bracket_evaluation/2.0.0",
        "evaluator_source_sha256": sha256_file(root / "src/futures_rebuild/tier1_bracket_evaluation.py"),
        "pipeline_outputs": {
            "features": "NOT_CREATED_REQUIRES_SEPARATE_REAL_DATA_APPROVAL",
            "labels": "NOT_CREATED_REQUIRES_SEPARATE_REAL_DATA_APPROVAL",
            "models": "NOT_FIT_REQUIRES_SEPARATE_REAL_DATA_APPROVAL",
            "predictions": "NOT_CREATED_REQUIRES_SEPARATE_REAL_DATA_APPROVAL",
            "evaluation": "NOT_RUN_REQUIRES_SEPARATE_REAL_DATA_APPROVAL",
        },
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "economics_evaluation": False,
        "holdout_or_forward_access": False,
        "provider_access": False,
        "trading": False,
    }
    required_hashes = (
        core["phase8_index_release_id"], core["phase8_audit_release_id"],
        core["phase8_evaluation_config_sha256"], core["risk_profile_sha256"], core["rulebook_sha256"],
    )
    if not all(isinstance(value, str) and len(value) == 64 for value in required_hashes):
        raise IntegrityError("successor registration provenance is incomplete")
    return PreparedRecord(sha256_json(core), core)


def _persist_create_only(*, path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
    try:
        os.write(descriptor, canonical_bytes(dict(payload)) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_prepared_closure(prepared: PreparedRecord) -> None:
    if (
        prepared.record_id != sha256_json(prepared.payload)
        or prepared.payload.get("schema_version") != "tier1_bracket_failed_trial_closure/1.0.0"
        or prepared.payload.get("state") != "PREPARED_FOR_CLOSURE_CONFIRMATION"
        or prepared.payload.get("trial_id") != FAILED_TRIAL_ID
        or prepared.payload.get("publication_authorized") is not False
        or prepared.payload.get("activation_authorized") is not False
    ):
        raise IntegrityError("approved failed-trial closure preparation is inconsistent")


def _validate_prepared_successor(prepared: PreparedRecord) -> None:
    if (
        prepared.record_id != sha256_json(prepared.payload)
        or prepared.payload.get("schema_version") != "tier1_bracket_successor_v2_registration/1.0.0"
        or prepared.payload.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_OR_OUTCOME_ACCESS"
        or prepared.payload.get("failed_trial_id") != FAILED_TRIAL_ID
        or prepared.payload.get("source_row_access") is not False
        or prepared.payload.get("model_fit") is not False
        or prepared.payload.get("prediction_generation") is not False
        or prepared.payload.get("economics_evaluation") is not False
        or prepared.payload.get("holdout_or_forward_access") is not False
        or prepared.payload.get("provider_access") is not False
        or prepared.payload.get("trading") is not False
    ):
        raise IntegrityError("approved Tier 1 successor preparation is inconsistent")


def persist_failed_trial_closure(*, root: Path, prepared: PreparedRecord) -> dict[str, str]:
    """Create the approved closure registry and event without changing prior artifacts."""
    _validate_prepared_closure(prepared)
    registry = root / CLOSURE_REGISTRY_ROOT / f"{prepared.record_id}.json"
    event = root / CLOSURE_EVENT_ROOT / f"{prepared.record_id}.json"
    if registry.exists() or event.exists():
        raise IntegrityError("failed trial closure already exists")
    closed_at = datetime.now(timezone.utc).isoformat()
    _persist_create_only(path=registry, payload={**prepared.payload, "state": "CLOSED_FAILED_NO_RESCUE", "publication_authorized": True, "activation_authorized": True, "closure_id": prepared.record_id, "closed_at_utc": closed_at})
    _persist_create_only(path=event, payload={"event_type": "CLOSED_FAILED_NO_RESCUE", "closure_id": prepared.record_id, "trial_id": FAILED_TRIAL_ID, "closed_at_utc": closed_at})
    return {"closure_id": prepared.record_id, "registry_path": registry.relative_to(root).as_posix(), "event_path": event.relative_to(root).as_posix()}


def persist_successor_v2_registration(*, root: Path, prepared: PreparedRecord) -> dict[str, str]:
    """Create the approved successor registry and declaration event only."""
    _validate_prepared_successor(prepared)
    registry = root / SUCCESSOR_REGISTRY_ROOT / f"{prepared.record_id}.json"
    event = root / SUCCESSOR_EVENT_ROOT / f"{prepared.record_id}.json"
    if registry.exists() or event.exists():
        raise IntegrityError("Tier 1 bracket successor v2 registration already exists")
    registered_at = datetime.now(timezone.utc).isoformat()
    _persist_create_only(path=registry, payload={**prepared.payload, "trial_id": prepared.record_id, "registered_at_utc": registered_at})
    _persist_create_only(path=event, payload={"event_type": "DECLARED", "trial_id": prepared.record_id, "registered_at_utc": registered_at, "source_row_access": False, "model_fit": False, "prediction_generation": False, "economics_evaluation": False})
    return {"trial_id": prepared.record_id, "registry_path": registry.relative_to(root).as_posix(), "event_path": event.relative_to(root).as_posix()}


def verify_failed_trial_closure(
    *, root: Path, closure_id: str, prepared: PreparedRecord | None = None,
) -> dict[str, str]:
    prepared = prepare_failed_trial_closure(root=root) if prepared is None else prepared
    _validate_prepared_closure(prepared)
    if closure_id != prepared.record_id:
        raise IntegrityError("failed trial closure ID differs from its current preparation")
    registry_path = root / CLOSURE_REGISTRY_ROOT / f"{closure_id}.json"
    event_path = root / CLOSURE_EVENT_ROOT / f"{closure_id}.json"
    registry, event = _load(registry_path), _load(event_path)
    closed_at = registry.get("closed_at_utc")
    expected_registry = {
        **prepared.payload,
        "state": "CLOSED_FAILED_NO_RESCUE",
        "publication_authorized": True,
        "activation_authorized": True,
        "closure_id": closure_id,
        "closed_at_utc": closed_at,
    }
    expected_event = {
        "event_type": "CLOSED_FAILED_NO_RESCUE",
        "closure_id": closure_id,
        "trial_id": FAILED_TRIAL_ID,
        "closed_at_utc": closed_at,
    }
    if not isinstance(closed_at, str) or registry != expected_registry or event != expected_event:
        raise IntegrityError("failed trial closure registry or event is inconsistent")
    return {
        "closure_id": closure_id,
        "registry_sha256": sha256_file(registry_path),
        "event_sha256": sha256_file(event_path),
    }


def verify_successor_v2_registration(
    *, root: Path, trial_id: str, prepared: PreparedRecord | None = None,
) -> dict[str, str]:
    prepared = prepare_successor_v2_registration(root=root) if prepared is None else prepared
    _validate_prepared_successor(prepared)
    if trial_id != prepared.record_id:
        raise IntegrityError("successor trial ID differs from its current preparation")
    registry_path = root / SUCCESSOR_REGISTRY_ROOT / f"{trial_id}.json"
    event_path = root / SUCCESSOR_EVENT_ROOT / f"{trial_id}.json"
    registry, event = _load(registry_path), _load(event_path)
    registered_at = registry.get("registered_at_utc")
    expected_registry = {**prepared.payload, "trial_id": trial_id, "registered_at_utc": registered_at}
    expected_event = {
        "event_type": "DECLARED",
        "trial_id": trial_id,
        "registered_at_utc": registered_at,
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "economics_evaluation": False,
    }
    if not isinstance(registered_at, str) or registry != expected_registry or event != expected_event:
        raise IntegrityError("successor trial registry or event is inconsistent")
    return {
        "trial_id": trial_id,
        "registry_sha256": sha256_file(registry_path),
        "event_sha256": sha256_file(event_path),
    }
