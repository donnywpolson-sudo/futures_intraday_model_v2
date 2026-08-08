"""Create-only V10 pre-data retirement and V11 registration controls."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError
from . import tier1_bracket_v5 as v5
from .tier1_bracket_v11 import V10_TRIAL_ID, load_v11_contract


V10_REGISTRY = Path("state/trial_registry/tier1_bracket_successor_v10") / f"{V10_TRIAL_ID}.json"
V10_EVENT = Path("state/trial_events/tier1_bracket_successor_v10") / f"{V10_TRIAL_ID}.json"
V10_EXECUTION_PLAN = Path("configs/tier1_bracket_successor_v10_historical_execution_plan.json")
V10_RETIREMENT_PREPARATION = Path("configs/tier1_bracket_v10_retirement_preparation.json")
V10_RETIREMENT_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_v10_retirement")
V10_RETIREMENT_EVENT_ROOT = Path("state/trial_events/tier1_bracket_v10_retirement")
V11_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_successor_v11")
V11_EVENT_ROOT = Path("state/trial_events/tier1_bracket_successor_v11")
RETIRED_PREPUBLICATION_TEST = "tests/test_tier1_bracket_v10_registration.py"


def _load(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid V11 lifecycle artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("V11 lifecycle artifact is not an object")
    return value


@dataclass(frozen=True)
class PreparedV10RetirementV11:
    record_id: str
    canonical_payload: Mapping[str, object]


@dataclass(frozen=True)
class PreparedV11Registration:
    trial_id: str
    canonical_payload: Mapping[str, object]


def prepare_v10_retirement_v11(*, root: Path) -> PreparedV10RetirementV11:
    preparation = _load(root / V10_RETIREMENT_PREPARATION)
    registry = _load(root / V10_REGISTRY)
    event = _load(root / V10_EVENT)
    bindings = registry.get("bindings")
    if (
        preparation.get("trial_id") != V10_TRIAL_ID
        or preparation.get("disposition")
        != "INVALID_PRE_DATA_SHARED_CANDIDATE_BASELINE_OPPORTUNITY_UNIVERSE"
        or preparation.get("historical_source_rows_opened") is not False
        or preparation.get("model_fit") is not False
        or preparation.get("predictions_generated") is not False
        or preparation.get("historical_evaluation_executed") is not False
        or registry.get("trial_id") != V10_TRIAL_ID
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or event.get("trial_id") != V10_TRIAL_ID
        or event.get("source_row_access") is not False
        or not isinstance(bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
        or (root / "state/tier1_bracket_successor_v10_unpublished").exists()
    ):
        raise IntegrityError("V10 pre-data retirement evidence or registered bytes are invalid")
    preserved = dict(bindings)
    for path in (V10_REGISTRY, V10_EVENT, V10_EXECUTION_PLAN):
        preserved[path.as_posix()] = sha256_file(root / path)
    core = {**preparation, "preserved_v10_sha256": dict(sorted(preserved.items()))}
    return PreparedV10RetirementV11(sha256_json(core), core)


def prepare_v11_registration(*, root: Path) -> PreparedV11Registration:
    _, delta = load_v11_contract(root=root)
    retirement = prepare_v10_retirement_v11(root=root)
    registry = _load(root / V10_REGISTRY)
    prior = registry.get("bindings")
    sources = registry.get("source_bindings")
    if not isinstance(prior, dict) or not isinstance(sources, list):
        raise IntegrityError("V10 lineage is incomplete for V11 registration")
    bindings = dict(prior)
    retired_test_hash = bindings.pop(RETIRED_PREPUBLICATION_TEST, None)
    if not v5._hex64(retired_test_hash):
        raise IntegrityError("V10 prepublication-only test binding is absent")
    new_paths = (
        V10_RETIREMENT_PREPARATION,
        Path("configs/tier1_bracket_successor_v11.json"),
        Path("src/futures_rebuild/tier1_bracket_v11.py"),
        Path("src/futures_rebuild/tier1_bracket_v11_pipeline.py"),
        Path("src/futures_rebuild/tier1_bracket_v11_execution.py"),
        Path("src/futures_rebuild/tier1_bracket_v11_registration.py"),
        Path("tests/test_tier1_bracket_v11.py"),
        Path("tests/test_tier1_bracket_v11_execution.py"),
        Path("tests/test_tier1_bracket_v11_registration.py"),
        Path("reports/tier1_bracket_v11_prepublication_completion_audit.md"),
        V10_EXECUTION_PLAN, V10_REGISTRY, V10_EVENT,
    )
    bindings.update({path.as_posix(): sha256_file(root / path) for path in new_paths})
    source_binding_id = v5.source_binding_id_from_metadata_v5(sources)
    core = {
        "schema_version": "tier1_bracket_successor_v11_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": delta["classification"],
        "supersedes_v10_trial_id": V10_TRIAL_ID,
        "v10_retirement_record_id": retirement.record_id,
        "change_scope": "INDEPENDENT_CAUSAL_OPPORTUNITY_UNIVERSE_AND_COVERAGE_FOR_EACH_REQUIRED_BASELINE_ONLY",
        "retired_prepublication_only_binding": {
            "path": RETIRED_PREPUBLICATION_TEST, "sha256": retired_test_hash,
            "reason": "ASSERTED_REGISTRY_ABSENCE_AND_BECAME_HISTORICAL_AFTER_CREATE_ONLY_PUBLICATION",
        },
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
    return PreparedV11Registration(sha256_json(core), core)


def persist_v10_retirement_v11(
    *, root: Path, prepared: PreparedV10RetirementV11,
) -> dict[str, str]:
    if prepared.record_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V10 retirement identity is invalid")
    preserved = prepared.canonical_payload.get("preserved_v10_sha256")
    if not isinstance(preserved, dict) or any(
        sha256_file(root / path) != digest for path, digest in preserved.items()
    ):
        raise IntegrityError("preserved V10 bytes changed after retirement preparation")
    registry = V10_RETIREMENT_REGISTRY_ROOT / f"{prepared.record_id}.json"
    event = V10_RETIREMENT_EVENT_ROOT / f"{prepared.record_id}.json"
    if (root / registry).exists() or (root / event).exists():
        raise IntegrityError("V10 retirement publication is create-only")
    (root / registry).parent.mkdir(parents=True, exist_ok=True)
    (root / event).parent.mkdir(parents=True, exist_ok=True)
    with (root / registry).open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.canonical_payload, "state": "RETIRED_INVALID_BEFORE_SOURCE_ACCESS",
        }) + b"\n")
    with (root / event).open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_bracket_v10_retirement_event/1.0.0",
            "event_type": "RETIRED", "trial_id": V10_TRIAL_ID,
            "record_id": prepared.record_id,
        }) + b"\n")
    return {"record_id": prepared.record_id, "registry_path": registry.as_posix(), "event_path": event.as_posix()}


def persist_v11_registration(
    *, root: Path, prepared: PreparedV11Registration,
) -> dict[str, str]:
    if prepared.trial_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V11 trial identity is invalid")
    bindings = prepared.canonical_payload.get("bindings")
    if not isinstance(bindings, dict) or any(
        sha256_file(root / path) != digest for path, digest in bindings.items()
    ):
        raise IntegrityError("V11 registration binding changed after preparation")
    retirement_id = prepared.canonical_payload.get("v10_retirement_record_id")
    retirement = _load(
        root / V10_RETIREMENT_REGISTRY_ROOT / f"{retirement_id}.json"
    )
    if (
        retirement.get("state") != "RETIRED_INVALID_BEFORE_SOURCE_ACCESS"
        or sha256_json({
            **retirement, "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        }) != retirement_id
    ):
        raise IntegrityError("published V10 retirement is absent or inconsistent")
    registry = V11_REGISTRY_ROOT / f"{prepared.trial_id}.json"
    event = V11_EVENT_ROOT / f"{prepared.trial_id}.json"
    if (root / registry).exists() or (root / event).exists():
        raise IntegrityError("V11 registration publication is create-only")
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
            "schema_version": "tier1_bracket_successor_v11_event/1.0.0",
            "event_type": "DECLARED", "trial_id": prepared.trial_id,
            "source_row_access": False, "model_fit": False,
            "prediction_generation": False, "historical_evaluation": False,
            "holdout_or_forward_access": False,
        }) + b"\n")
    return {"trial_id": prepared.trial_id, "registry_path": registry.as_posix(), "event_path": event.as_posix()}
