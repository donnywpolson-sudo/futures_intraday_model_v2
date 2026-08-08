"""Create-only V11 invalid retirement and V12 registration controls."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError
from . import tier1_bracket_v5 as v5
from .tier1_bracket_v12 import V11_TRIAL_ID, load_v12_contract


V11_REGISTRY = Path("state/trial_registry/tier1_bracket_successor_v11") / f"{V11_TRIAL_ID}.json"
V11_EVENT = Path("state/trial_events/tier1_bracket_successor_v11") / f"{V11_TRIAL_ID}.json"
V11_EXECUTION_PLAN = Path("configs/tier1_bracket_successor_v11_historical_execution_plan.json")
V11_RUNNER = Path("scripts/run_tier1_bracket_v11_historical_execution.py")
V11_RETIREMENT_PREPARATION = Path("configs/tier1_bracket_v11_retirement_preparation.json")
V11_AUTHORIZATION_RECEIPT_ID = "17b5b9b6f8c20adcb1b0773257b10e67c86fda52929fbd327de199a75fa6661b"
V11_AUTHORIZATION_CLAIM = Path("state/authorization_uses") / f"{V11_AUTHORIZATION_RECEIPT_ID}.json"
V11_RETIREMENT_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_v11_retirement")
V11_RETIREMENT_EVENT_ROOT = Path("state/trial_events/tier1_bracket_v11_retirement")
V12_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_successor_v12")
V12_EVENT_ROOT = Path("state/trial_events/tier1_bracket_successor_v12")


def _load(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid V12 lifecycle artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("V12 lifecycle artifact is not an object")
    return value


@dataclass(frozen=True)
class PreparedV11RetirementV12:
    record_id: str
    canonical_payload: Mapping[str, object]


@dataclass(frozen=True)
class PreparedV12Registration:
    trial_id: str
    canonical_payload: Mapping[str, object]


def prepare_v11_retirement_v12(*, root: Path) -> PreparedV11RetirementV12:
    preparation = _load(root / V11_RETIREMENT_PREPARATION)
    registry = _load(root / V11_REGISTRY)
    event = _load(root / V11_EVENT)
    claim = _load(root / V11_AUTHORIZATION_CLAIM)
    bindings = registry.get("bindings")
    if (
        preparation.get("trial_id") != V11_TRIAL_ID
        or preparation.get("disposition")
        != "INVALID_POST_EXECUTION_FLAT_BASELINE_FEATURE_COVERAGE_DEPENDENCY"
        or preparation.get("historical_source_rows_opened") is not True
        or preparation.get("historical_evaluation_executed") is not True
        or preparation.get("historical_results_published") is not False
        or preparation.get("performance_interpretation")
        != "INVALID_NOT_A_STRATEGY_VERDICT"
        or registry.get("trial_id") != V11_TRIAL_ID
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or event.get("source_row_access") is not False
        or claim.get("receipt_id") != V11_AUTHORIZATION_RECEIPT_ID
        or claim.get("trial_id") != V11_TRIAL_ID
        or claim.get("holdout_or_forward_access") is not False
        or claim.get("provider_access") is not False
        or claim.get("publication") is not False
        or not isinstance(bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
        or (root / "state/tier1_bracket_successor_v11_unpublished").exists()
    ):
        raise IntegrityError("V11 invalid-retirement evidence or preserved bytes are invalid")
    preserved = dict(bindings)
    for path in (
        V11_REGISTRY, V11_EVENT, V11_EXECUTION_PLAN, V11_RUNNER,
        V11_AUTHORIZATION_CLAIM,
    ):
        preserved[path.as_posix()] = sha256_file(root / path)
    core = {**preparation, "preserved_v11_sha256": dict(sorted(preserved.items()))}
    return PreparedV11RetirementV12(sha256_json(core), core)


def prepare_v12_registration(*, root: Path) -> PreparedV12Registration:
    _, delta = load_v12_contract(root=root)
    retirement = prepare_v11_retirement_v12(root=root)
    registry = _load(root / V11_REGISTRY)
    prior = registry.get("bindings")
    sources = registry.get("source_bindings")
    if not isinstance(prior, dict) or not isinstance(sources, list):
        raise IntegrityError("V11 lineage is incomplete for V12 registration")
    bindings = dict(prior)
    new_paths = (
        V11_RETIREMENT_PREPARATION, V11_EXECUTION_PLAN, V11_RUNNER,
        V11_AUTHORIZATION_CLAIM,
        Path("configs/tier1_bracket_successor_v12.json"),
        Path("configs/tier1_bracket_v12_local_source_alternative_census_plan.json"),
        Path("scripts/run_v12_local_source_alternative_census.py"),
        Path("src/futures_rebuild/tier1_bracket_v12.py"),
        Path("src/futures_rebuild/tier1_bracket_v12_pipeline.py"),
        Path("src/futures_rebuild/tier1_bracket_v12_execution.py"),
        Path("src/futures_rebuild/tier1_bracket_v12_registration.py"),
        Path("tests/test_tier1_bracket_v12.py"),
        Path("tests/test_tier1_bracket_v12_execution.py"),
        Path("tests/test_tier1_bracket_v12_registration.py"),
        Path("tests/test_tier1_bracket_v12_source_census.py"),
        V11_REGISTRY, V11_EVENT,
    )
    bindings.update({path.as_posix(): sha256_file(root / path) for path in new_paths})
    source_binding_id = v5.source_binding_id_from_metadata_v5(sources)
    core = {
        "schema_version": "tier1_bracket_successor_v12_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": delta["classification"],
        "supersedes_v11_trial_id": V11_TRIAL_ID,
        "v11_retirement_record_id": retirement.record_id,
        "change_scope": "TRUE_ZERO_FLAT_BASELINE_COVERAGE_INDEPENDENCE_ONLY",
        "bindings": dict(sorted(bindings.items())),
        "calendar_release_id": registry["calendar_release_id"],
        "dependency_lock_receipt_id": registry["dependency_lock_receipt_id"],
        "source_bindings": sorted(
            (dict(item) for item in sources),
            key=lambda item: (str(item["market"]), int(item["year"])),
        ),
        "source_binding_id": source_binding_id,
        "source_row_access": False, "model_fit": False,
        "prediction_generation": False, "historical_evaluation": False,
        "publication": False, "holdout_or_forward_access": False,
        "provider_access": False, "trading": False,
    }
    return PreparedV12Registration(sha256_json(core), core)


def persist_v11_retirement_v12(
    *, root: Path, prepared: PreparedV11RetirementV12,
) -> dict[str, str]:
    if prepared.record_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V11 retirement identity is invalid")
    preserved = prepared.canonical_payload.get("preserved_v11_sha256")
    if not isinstance(preserved, dict) or any(
        sha256_file(root / path) != digest for path, digest in preserved.items()
    ):
        raise IntegrityError("preserved V11 bytes changed after retirement preparation")
    registry = V11_RETIREMENT_REGISTRY_ROOT / f"{prepared.record_id}.json"
    event = V11_RETIREMENT_EVENT_ROOT / f"{prepared.record_id}.json"
    if (root / registry).exists() or (root / event).exists():
        raise IntegrityError("V11 retirement publication is create-only")
    (root / registry).parent.mkdir(parents=True, exist_ok=True)
    (root / event).parent.mkdir(parents=True, exist_ok=True)
    with (root / registry).open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.canonical_payload,
            "state": "RETIRED_INVALID_AFTER_UNPUBLISHED_EXECUTION",
        }) + b"\n")
    with (root / event).open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_bracket_v11_retirement_event/1.0.0",
            "event_type": "RETIRED", "trial_id": V11_TRIAL_ID,
            "record_id": prepared.record_id,
        }) + b"\n")
    return {
        "record_id": prepared.record_id,
        "registry_path": registry.as_posix(), "event_path": event.as_posix(),
    }


def persist_v12_registration(
    *, root: Path, prepared: PreparedV12Registration,
) -> dict[str, str]:
    if prepared.trial_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V12 trial identity is invalid")
    bindings = prepared.canonical_payload.get("bindings")
    if not isinstance(bindings, dict) or any(
        sha256_file(root / path) != digest for path, digest in bindings.items()
    ):
        raise IntegrityError("V12 registration binding changed after preparation")
    retirement_id = prepared.canonical_payload.get("v11_retirement_record_id")
    retirement = _load(
        root / V11_RETIREMENT_REGISTRY_ROOT / f"{retirement_id}.json"
    )
    if (
        retirement.get("state") != "RETIRED_INVALID_AFTER_UNPUBLISHED_EXECUTION"
        or sha256_json({
            **retirement, "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        }) != retirement_id
    ):
        raise IntegrityError("published V11 retirement is absent or inconsistent")
    registry = V12_REGISTRY_ROOT / f"{prepared.trial_id}.json"
    event = V12_EVENT_ROOT / f"{prepared.trial_id}.json"
    if (root / registry).exists() or (root / event).exists():
        raise IntegrityError("V12 registration publication is create-only")
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
            "schema_version": "tier1_bracket_successor_v12_event/1.0.0",
            "event_type": "DECLARED", "trial_id": prepared.trial_id,
            "source_row_access": False, "model_fit": False,
            "prediction_generation": False, "historical_evaluation": False,
            "holdout_or_forward_access": False,
        }) + b"\n")
    return {
        "trial_id": prepared.trial_id,
        "registry_path": registry.as_posix(), "event_path": event.as_posix(),
    }
