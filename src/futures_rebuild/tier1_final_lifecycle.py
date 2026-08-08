"""Prepare the invalid closure and one final Tier 1 registration.

Preparation is read-only.  Publication is deliberately a separate function so
calling the preparation path cannot retire a trial or replace the active
pointer.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError
from .tier1_final_protocol import (
    CLOSURE_PATH, PROTOCOL_PATH, load_final_trial_protocol,
    load_invalid_closure_preparation,
)


PREDECESSOR_TRIAL_ID = "221ddd3dd8816970794cff86ad1b119bfaac1b3f5678647dc8fc0bcc990ab76e"
PREDECESSOR_POINTER_SHA256 = (
    "38963664d5e1c04ed4e37aa4d4931b4d40f47b031ed0b995abb2aa950d19644f"
)
ACTIVE_POINTER_PATH = Path("configs/active_tier1_trial.json")
EXECUTION_PLAN_PATH = Path("configs/tier1_final_historical_execution_plan.json")
SYNTHETIC_VERIFICATION_PATH = Path("configs/tier1_final_synthetic_verification.json")
PREPARED_CERTIFICATE_PATH = Path("configs/tier1_final_preexecution_certificate.json")
RETIREMENT_ROOT = Path("state/trial_registry/tier1_standard_only_invalid_retirement")
RETIREMENT_EVENT_ROOT = Path("state/trial_events/tier1_standard_only_invalid_retirement")
TRIAL_ROOT = Path("state/trial_registry/tier1_final_trial")
TRIAL_EVENT_ROOT = Path("state/trial_events/tier1_final_trial")
CERTIFICATE_ROOT = Path("state/preexecution_certificates/tier1_final_trial")

GATES = (
    "PREDECESSOR_PRESERVED_AND_INVALID_CLOSURE_BOUND",
    "RESEARCH_PARAMETERS_INHERITED_WITHOUT_OUTCOME_TUNING",
    "SCENARIO_SPECIFIC_RISK_ABSTENTIONS",
    "CONCLUSIVE_REJECTION_FOR_FULLY_OBSERVED_FAILED_MANDATORY_GATE",
    "MISSING_SELECTED_CANDIDATE_PATH_REMAINS_INCONCLUSIVE",
    "INDEPENDENT_BASELINE_UNIVERSES_SCHEDULES_COSTS_AND_PATHS",
    "COMPLETE_SYNTHETIC_SOURCE_TO_TERMINAL_PIPELINE",
    "DURABLE_CREATE_ONLY_UNPUBLISHED_EVIDENCE",
    "EXACT_SINGLE_USE_EXECUTION_AUTHORIZATION",
    "LOCKED_COST_RISK_STATISTICS_AND_PROMOTION_GATES",
    "ONE_IMMUTABLE_SOURCE_SET_AND_ONE_ACTIVE_POINTER",
    "HOLDOUT_2025_PROVIDER_CREDENTIAL_AND_TRADING_FAIL_CLOSED",
)


@dataclass(frozen=True)
class PreparedFinalLifecycle:
    retirement_id: str
    retirement: Mapping[str, object]
    trial_id: str
    trial: Mapping[str, object]
    pointer_id: str
    pointer: Mapping[str, object]
    certificate_id: str
    certificate: Mapping[str, object]


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid final lifecycle artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("final lifecycle artifact is not an object")
    return value


def load_final_synthetic_verification(*, root: Path) -> dict[str, object]:
    verification = _object(root / SYNTHETIC_VERIFICATION_PATH)
    core = dict(verification)
    verification_id = core.pop("verification_id", None)
    prefixes = (
        "test_tier1_bracket", "test_tier1_frozen", "test_tier1_preexecution",
        "test_tier1_standard", "test_tier1_final",
    )
    paths = sorted(
        path for path in (root / "tests").glob("test_tier1_*.py")
        if path.name.startswith(prefixes)
    )
    tree_id = sha256_json({
        "files": [
            {"path": path.relative_to(root).as_posix(), "sha256": sha256_file(path)}
            for path in paths
        ]
    })
    results = verification.get("applicable_results")
    if (
        verification_id != sha256_json(core)
        or verification.get("schema_version")
        != "tier1_final_synthetic_verification/1.0.0"
        or verification.get("state") != "PREPARED_NOT_PUBLISHED"
        or verification.get("test_file_count") != len(paths)
        or verification.get("test_tree_id") != tree_id
        or verification.get("conftest_sha256") != sha256_file(root / "tests/conftest.py")
        or verification.get("runner_sha256")
        != sha256_file(root / "scripts/run_windows_host_root_pytest.ps1")
        or not isinstance(results, Mapping)
        or results.get("failed") != 0
        or not isinstance(results.get("passed"), int)
        or int(results["passed"]) < 274
        or results.get("expected_xfailed") != 1
        or any(verification.get(field) is not True for field in (
            "scenario_specific_risk_abstention_tested",
            "conclusive_rejection_lattice_tested",
            "missing_candidate_path_inconclusive_tested",
            "durable_unpublished_bundle_tested",
            "complete_synthetic_source_to_terminal_pipeline_tested",
            "single_use_authorization_tested",
        ))
        or any(verification.get(field) is not False for field in (
            "historical_rows_opened", "provider_or_network_access", "credential_access",
            "holdout_2025_access", "real_model_fit", "historical_evaluation",
            "trial_registration", "publication", "trading",
        ))
    ):
        raise IntegrityError("final synthetic verification is incomplete or drifted")
    return verification


def _bindings(root: Path) -> dict[str, str]:
    paths = (
        CLOSURE_PATH, PROTOCOL_PATH, EXECUTION_PLAN_PATH,
        SYNTHETIC_VERIFICATION_PATH,
        Path("configs/tier1_standard_only_trial_protocol.json"),
        Path("src/futures_rebuild/tier1_final_decision_validity.py"),
        Path("src/futures_rebuild/tier1_final_execution.py"),
        Path("src/futures_rebuild/tier1_final_lifecycle.py"),
        Path("src/futures_rebuild/tier1_final_pipeline.py"),
        Path("src/futures_rebuild/tier1_final_protocol.py"),
        Path("src/futures_rebuild/tier1_final_unpublished_evidence.py"),
        Path("src/futures_rebuild/tier1_frozen_successor_source_semantics.py"),
        Path("src/futures_rebuild/tier1_standard_only_execution.py"),
        Path("scripts/run_tier1_final_historical_execution.py"),
        Path("scripts/run_windows_host_root_pytest.ps1"),
        Path("tests/conftest.py"),
        Path("tests/test_tier1_final_decision_validity.py"),
        Path("configs/dependency_lock_receipt.json"),
    )
    return {path.as_posix(): sha256_file(root / path) for path in paths}


def prepare_final_lifecycle(*, root: Path) -> PreparedFinalLifecycle:
    closure = load_invalid_closure_preparation(root=root)
    protocol = load_final_trial_protocol(root=root)
    verification = load_final_synthetic_verification(root=root)
    pointer = _object(root / ACTIVE_POINTER_PATH)
    if (
        closure.get("trial_id") != PREDECESSOR_TRIAL_ID
        or pointer.get("trial_id") != PREDECESSOR_TRIAL_ID
        or pointer.get("state") != "ACTIVE_REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or sha256_file(root / ACTIVE_POINTER_PATH) != PREDECESSOR_POINTER_SHA256
    ):
        raise IntegrityError("predecessor is not the preserved active trial")
    preserved = closure.get("preserved_bindings")
    source = protocol.get("lineage")
    if not isinstance(preserved, Mapping) or not isinstance(source, Mapping):
        raise IntegrityError("final lineage is incomplete")

    retirement = {
        **{key: value for key, value in closure.items() if key != "record_id"},
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "closure_preparation_id": closure["record_id"],
    }
    retirement_id = sha256_json(retirement)
    bindings = _bindings(root)
    trial = {
        "schema_version": "tier1_final_trial_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": protocol["classification"],
        "protocol_id": protocol["protocol_id"],
        "supersedes_invalid_trial_id": PREDECESSOR_TRIAL_ID,
        "invalid_retirement_id": retirement_id,
        "selected_sources_id": "f61f34df0b9d8cf7b344016ce3df8bb76abeb890558740460f60d51e5ca37bde",
        "calendar_release_id": "038940d82031f31e2c66ed37186e98a6ee6cff3e7248f634f2c7a8e94ea6ecf3",
        "bindings": dict(sorted(bindings.items())),
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "unpublished_evidence_staging": False,
        "publication": False,
        "holdout_or_forward_access": False,
        "provider_or_network_access": False,
        "credential_access": False,
        "trading": False,
    }
    trial_id = sha256_json(trial)
    certificate_path = (CERTIFICATE_ROOT / f"{trial_id}.json").as_posix()
    new_pointer = {
        "schema_version": "active_tier1_trial/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "trial_id": trial_id,
        "trial_registry_path": (TRIAL_ROOT / f"{trial_id}.json").as_posix(),
        "preexecution_certificate_path": certificate_path,
        "protocol_id": protocol["protocol_id"],
        "holdout_or_forward_access": False,
    }
    pointer_id = sha256_json(new_pointer)
    certificate = {
        "schema_version": "tier1_final_preexecution_certificate/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "overall_decision": "PASS",
        "trial_id": trial_id,
        "active_pointer_id": pointer_id,
        "protocol_id": protocol["protocol_id"],
        "invalid_retirement_id": retirement_id,
        "synthetic_verification_id": verification["verification_id"],
        "gates": [
            {"gate": gate, "status": "PASS", "evidence": "HASH_BOUND_SYNTHETIC_AND_STATIC_CONTROL"}
            for gate in GATES
        ],
        "bindings": dict(sorted(bindings.items())),
        "continuous_drawdown_limit_usd": "1500",
        "stress_costs_required_for_promotion": True,
        "durable_unpublished_evidence_required_before_terminal_report": True,
        "separate_single_use_historical_execution_authorization_required": True,
        "historical_rows_opened": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "holdout_2025_touched": False,
        "provider_or_network_access": False,
        "credential_access": False,
        "trading": False,
    }
    certificate_id = sha256_json(certificate)
    return PreparedFinalLifecycle(
        retirement_id, retirement, trial_id, trial, pointer_id, new_pointer,
        certificate_id, certificate,
    )


def load_prepared_final_certificate(*, root: Path) -> dict[str, object]:
    prepared = prepare_final_lifecycle(root=root)
    expected = {**prepared.certificate, "certificate_id": prepared.certificate_id}
    certificate = _object(root / PREPARED_CERTIFICATE_PATH)
    if certificate != expected:
        raise IntegrityError("prepared final preexecution certificate drifted")
    return certificate


def persist_final_lifecycle(
    *, root: Path, prepared: PreparedFinalLifecycle,
) -> dict[str, str]:
    """Publish only after separate approval; replace the active pointer last."""

    prepared_certificate = load_prepared_final_certificate(root=root)
    if (
        prepared.retirement_id != sha256_json(prepared.retirement)
        or prepared.trial_id != sha256_json(prepared.trial)
        or prepared.pointer_id != sha256_json(prepared.pointer)
        or prepared.certificate_id != sha256_json(prepared.certificate)
        or prepared.trial.get("invalid_retirement_id") != prepared.retirement_id
        or prepared.pointer.get("trial_id") != prepared.trial_id
        or prepared.certificate.get("trial_id") != prepared.trial_id
        or prepared.certificate.get("active_pointer_id") != prepared.pointer_id
        or prepared_certificate.get("certificate_id") != prepared.certificate_id
    ):
        raise IntegrityError("final lifecycle identities are invalid")
    bindings = prepared.trial.get("bindings")
    if not isinstance(bindings, Mapping) or any(
        sha256_file(root / str(path)) != digest for path, digest in bindings.items()
    ):
        raise IntegrityError("final registration bindings drifted")
    if sha256_file(root / ACTIVE_POINTER_PATH) != PREDECESSOR_POINTER_SHA256:
        raise IntegrityError("active pointer changed before compare-and-swap")

    retirement_path = RETIREMENT_ROOT / f"{prepared.retirement_id}.json"
    retirement_event = RETIREMENT_EVENT_ROOT / f"{prepared.retirement_id}.json"
    trial_path = TRIAL_ROOT / f"{prepared.trial_id}.json"
    trial_event = TRIAL_EVENT_ROOT / f"{prepared.trial_id}.json"
    certificate_path = CERTIFICATE_ROOT / f"{prepared.trial_id}.json"
    destinations = (retirement_path, retirement_event, trial_path, trial_event, certificate_path)
    if any((root / path).exists() for path in destinations):
        raise IntegrityError("final lifecycle immutable publication paths already exist")
    for path in destinations:
        (root / path).parent.mkdir(parents=True, exist_ok=True)
    payloads = (
        (retirement_path, {**prepared.retirement, "state": "RETIRED_INVALID_AFTER_UNPUBLISHED_EXECUTION", "record_id": prepared.retirement_id}),
        (retirement_event, {"schema_version": "tier1_standard_only_invalid_retirement_event/1.0.0", "event_type": "RETIRED_INVALID", "trial_id": PREDECESSOR_TRIAL_ID, "record_id": prepared.retirement_id}),
        (trial_path, {**prepared.trial, "state": "REGISTERED_BEFORE_SOURCE_ROW_ACCESS", "trial_id": prepared.trial_id}),
        (trial_event, {"schema_version": "tier1_final_trial_event/1.0.0", "event_type": "DECLARED", "trial_id": prepared.trial_id, "source_row_access": False, "model_fit": False, "prediction_generation": False, "historical_evaluation": False, "holdout_or_forward_access": False}),
        (certificate_path, {**prepared.certificate, "state": "PUBLISHED_PREEXECUTION_PASS", "certificate_id": prepared.certificate_id}),
    )
    for path, payload in payloads:
        with (root / path).open("xb") as stream:
            stream.write(canonical_bytes(payload) + b"\n")
    temporary_pointer = root / ACTIVE_POINTER_PATH.with_suffix(".json.final-new")
    with temporary_pointer.open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.pointer,
            "state": "ACTIVE_REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
            "pointer_id": prepared.pointer_id,
        }) + b"\n")
    if sha256_file(root / ACTIVE_POINTER_PATH) != PREDECESSOR_POINTER_SHA256:
        raise IntegrityError("active pointer changed during publication")
    os.replace(temporary_pointer, root / ACTIVE_POINTER_PATH)
    return {
        "retirement_id": prepared.retirement_id,
        "trial_id": prepared.trial_id,
        "certificate_id": prepared.certificate_id,
        "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
    }
