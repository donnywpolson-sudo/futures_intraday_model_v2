"""Create-only V9 retirement and V10 registration preparation."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError
from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v9 as v9
from .tier1_bracket_v10_decision_validity import load_decision_validity_contract_v10


V9_TRIAL_ID = v9.V9_TRIAL_ID if hasattr(v9, "V9_TRIAL_ID") else "fed4cc30c3f01e4f5b15eacfecdc50fe3a45bf671c0306d568f013f02c91dcd8"
V9_REGISTRY = Path("state/trial_registry/tier1_bracket_successor_v9") / f"{V9_TRIAL_ID}.json"
V9_EVENT = Path("state/trial_events/tier1_bracket_successor_v9") / f"{V9_TRIAL_ID}.json"
V9_RETIREMENT_PREPARATION = Path("configs/tier1_bracket_v9_retirement_preparation.json")
V9_EXECUTION_PLAN = Path("configs/tier1_bracket_successor_v9_historical_execution_plan.json")
V9_EXECUTION_CLAIM = Path("state/authorization_uses/547dea184fc11eb5500167a51c684484c474f7f3e42301320f7cacd6521fe40b.json")
V9_CENSUS_PLAN = Path("configs/tier1_bracket_v9_dependency_window_census_plan.json")
V9_CENSUS_RUNNER = Path("scripts/run_tier1_bracket_v9_dependency_window_census.py")
V9_CENSUS_CLAIM = Path("state/authorization_uses/ae57c50a0bf0ad272fa7b8b855a16866c7edb746a7ab6ebd344a19f89eb03c58.json")
V9_RETIREMENT_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_v9_retirement")
V9_RETIREMENT_EVENT_ROOT = Path("state/trial_events/tier1_bracket_v9_retirement")
V10_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_successor_v10")
V10_EVENT_ROOT = Path("state/trial_events/tier1_bracket_successor_v10")


def _load(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid V10 lifecycle artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"V10 lifecycle artifact is not an object: {path.as_posix()}")
    return value


@dataclass(frozen=True)
class PreparedV9RetirementV10:
    record_id: str
    canonical_payload: Mapping[str, object]


@dataclass(frozen=True)
class PreparedV10Registration:
    trial_id: str
    canonical_payload: Mapping[str, object]


def prepare_v9_retirement_v10(*, root: Path) -> PreparedV9RetirementV10:
    preparation = _load(root / V9_RETIREMENT_PREPARATION)
    registry = _load(root / V9_REGISTRY)
    event = _load(root / V9_EVENT)
    bindings = registry.get("bindings")
    if (
        preparation.get("trial_id") != V9_TRIAL_ID
        or preparation.get("disposition")
        != "INVALID_SOURCE_SCOPE_AND_FUTURE_OUTCOME_CONDITIONED_DECISION_PIPELINE"
        or preparation.get("historical_source_rows_opened") is not True
        or preparation.get("model_fit") is not True
        or preparation.get("predictions_generated") is not True
        or preparation.get("historical_evaluation_executed") is not True
        or preparation.get("historical_results_published") is not False
        or preparation.get("dependency_window_census_executed") is not True
        or preparation.get("dependency_window_census_published") is not False
        or preparation.get("holdout_or_forward_access") is not False
        or registry.get("trial_id") != V9_TRIAL_ID
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or event.get("trial_id") != V9_TRIAL_ID
        or not isinstance(bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("V9 retirement preparation or registered bytes are invalid")
    preserved = dict(bindings)
    for path in (
        V9_REGISTRY, V9_EVENT, V9_EXECUTION_PLAN, V9_EXECUTION_CLAIM,
        V9_CENSUS_PLAN, V9_CENSUS_RUNNER, V9_CENSUS_CLAIM,
    ):
        preserved[path.as_posix()] = sha256_file(root / path)
    core = {**preparation, "preserved_v9_sha256": dict(sorted(preserved.items()))}
    return PreparedV9RetirementV10(sha256_json(core), core)


def prepare_v10_registration(*, root: Path) -> PreparedV10Registration:
    _, delta = load_decision_validity_contract_v10(root=root)
    retirement = prepare_v9_retirement_v10(root=root)
    registry = _load(root / V9_REGISTRY)
    prior_bindings = registry.get("bindings")
    sources = registry.get("source_bindings")
    if not isinstance(prior_bindings, dict) or not isinstance(sources, list):
        raise IntegrityError("V9 lineage is incomplete for V10 registration")
    bindings = dict(prior_bindings)
    new_paths = (
        V9_RETIREMENT_PREPARATION,
        Path("configs/tier1_bracket_successor_v10.json"),
        Path("src/futures_rebuild/tier1_bracket_v10.py"),
        Path("src/futures_rebuild/tier1_bracket_v10_decision_validity.py"),
        Path("src/futures_rebuild/tier1_bracket_v10_pipeline.py"),
        Path("src/futures_rebuild/tier1_bracket_v10_execution.py"),
        Path("src/futures_rebuild/tier1_bracket_v10_registration.py"),
        Path("tests/test_tier1_bracket_v10.py"),
        Path("tests/test_tier1_bracket_v10_decision_validity.py"),
        Path("tests/test_tier1_bracket_v10_pipeline.py"),
        Path("tests/test_tier1_bracket_v10_execution.py"),
        Path("tests/test_tier1_bracket_v10_registration.py"),
        Path("reports/tier1_bracket_v10_prepublication_control_audit.md"),
        V9_EXECUTION_PLAN, V9_EXECUTION_CLAIM,
        V9_CENSUS_PLAN, V9_CENSUS_RUNNER, V9_CENSUS_CLAIM,
        V9_REGISTRY, V9_EVENT,
    )
    bindings.update({path.as_posix(): sha256_file(root / path) for path in new_paths})
    source_binding_id = v5.source_binding_id_from_metadata_v5(sources)
    core = {
        "schema_version": "tier1_bracket_successor_v10_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": delta["classification"],
        "supersedes_v9_trial_id": V9_TRIAL_ID,
        "v9_retirement_record_id": retirement.record_id,
        "change_scope": (
            "CHECKPOINT_SCOPED_SOURCE_CONTINUITY_CAUSAL_PREFIX_OUTCOMES_"
            "AND_DECISION_TIME_CROSSFIT_AND_RANKING_VALIDITY_ONLY"
        ),
        "bindings": dict(sorted(bindings.items())),
        "calendar_release_id": registry["calendar_release_id"],
        "dependency_lock_receipt_id": registry["dependency_lock_receipt_id"],
        "source_bindings": sorted(
            (dict(item) for item in sources),
            key=lambda item: (str(item["market"]), int(item["year"])),
        ),
        "source_binding_id": source_binding_id,
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "publication": False,
        "holdout_or_forward_access": False,
        "provider_access": False,
        "trading": False,
    }
    return PreparedV10Registration(sha256_json(core), core)


def persist_v9_retirement_v10(
    *, root: Path, prepared: PreparedV9RetirementV10,
) -> dict[str, str]:
    if prepared.record_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V9 retirement identity is invalid")
    preserved = prepared.canonical_payload.get("preserved_v9_sha256")
    if not isinstance(preserved, dict) or any(
        sha256_file(root / path) != digest for path, digest in preserved.items()
    ):
        raise IntegrityError("preserved V9 bytes changed after retirement preparation")
    registry = V9_RETIREMENT_REGISTRY_ROOT / f"{prepared.record_id}.json"
    event = V9_RETIREMENT_EVENT_ROOT / f"{prepared.record_id}.json"
    if (root / registry).exists() or (root / event).exists():
        raise IntegrityError("V9 retirement publication is create-only")
    (root / registry).parent.mkdir(parents=True, exist_ok=True)
    (root / event).parent.mkdir(parents=True, exist_ok=True)
    with (root / registry).open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.canonical_payload,
            "state": "RETIRED_INVALID_AFTER_UNPUBLISHED_HISTORICAL_EXECUTION",
        }) + b"\n")
    with (root / event).open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_bracket_v9_retirement_event/1.0.0",
            "event_type": "RETIRED", "trial_id": V9_TRIAL_ID,
            "record_id": prepared.record_id,
        }) + b"\n")
    return {"record_id": prepared.record_id, "registry_path": registry.as_posix(), "event_path": event.as_posix()}


def persist_v10_registration(
    *, root: Path, prepared: PreparedV10Registration,
) -> dict[str, str]:
    if prepared.trial_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V10 trial identity is invalid")
    bindings = prepared.canonical_payload.get("bindings")
    if not isinstance(bindings, dict) or any(
        sha256_file(root / path) != digest for path, digest in bindings.items()
    ):
        raise IntegrityError("V10 registration binding changed after preparation")
    retirement_id = prepared.canonical_payload.get("v9_retirement_record_id")
    retirement_path = root / V9_RETIREMENT_REGISTRY_ROOT / f"{retirement_id}.json"
    retirement = _load(retirement_path)
    if (
        retirement.get("state") != "RETIRED_INVALID_AFTER_UNPUBLISHED_HISTORICAL_EXECUTION"
        or sha256_json({
            **retirement, "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        }) != retirement_id
    ):
        raise IntegrityError("published V9 retirement is absent or inconsistent")
    registry = V10_REGISTRY_ROOT / f"{prepared.trial_id}.json"
    event = V10_EVENT_ROOT / f"{prepared.trial_id}.json"
    if (root / registry).exists() or (root / event).exists():
        raise IntegrityError("V10 registration publication is create-only")
    (root / registry).parent.mkdir(parents=True, exist_ok=True)
    (root / event).parent.mkdir(parents=True, exist_ok=True)
    with (root / registry).open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.canonical_payload,
            "state": "REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
            "trial_id": prepared.trial_id,
        }) + b"\n")
    with (root / event).open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_bracket_successor_v10_event/1.0.0",
            "event_type": "DECLARED", "trial_id": prepared.trial_id,
            "source_row_access": False, "model_fit": False,
            "prediction_generation": False, "historical_evaluation": False,
            "holdout_or_forward_access": False,
        }) + b"\n")
    return {"trial_id": prepared.trial_id, "registry_path": registry.as_posix(), "event_path": event.as_posix()}
