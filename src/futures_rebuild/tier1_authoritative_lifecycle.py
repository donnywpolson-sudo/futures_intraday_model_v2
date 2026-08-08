"""Prepare one corrected unversioned lifecycle without activating it."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError
from .tier1_authoritative_execution import (
    ACTIVE_POINTER_PATH, CERTIFICATE_ROOT, PLAN_PATH, REGISTRY_ROOT,
    load_authoritative_execution_plan, load_authoritative_registered_context,
    validate_registered_context_documents,
)
from .tier1_authoritative_protocol import (
    CLOSURE_PATH, FAILED_RETIREMENT_ROOT, FAILED_TRIAL_ID, PROTOCOL_PATH,
    load_authoritative_protocol, load_failed_final_closure_preparation,
)


EXPECTED_PREPUBLICATION_POINTER_SHA256 = (
    "38963664d5e1c04ed4e37aa4d4931b4d40f47b031ed0b995abb2aa950d19644f"
)
EXPECTED_PREPUBLICATION_TRIAL_ID = (
    "221ddd3dd8816970794cff86ad1b119bfaac1b3f5678647dc8fc0bcc990ab76e"
)
SYNTHETIC_VERIFICATION_PATH = Path(
    "configs/tier1_authoritative_synthetic_verification.json"
)
PREPARED_CERTIFICATE_PATH = Path(
    "configs/tier1_authoritative_preexecution_certificate.json"
)
FAILED_RETIREMENT_EVENT_ROOT = Path(
    "state/trial_events/tier1_final_pointer_binding_invalid_retirement"
)
TRIAL_EVENT_ROOT = Path("state/trial_events/tier1_authoritative_trial")

GATES = (
    "VERSION_RECONCILIATION_AND_SUCCESSOR_FREEZE",
    "ALL_PRIOR_REGISTERED_BYTES_PRESERVED",
    "FAILED_FINAL_TRIAL_INVALID_PRE_DATA_RETIREMENT_BOUND",
    "RESEARCH_PARAMETERS_UNCHANGED_AND_OUTCOME_UNTUNED",
    "MUTABLE_ACTIVE_POINTER_EXCLUDED_FROM_PROTOCOL_BINDINGS",
    "PRE_AND_POST_PUBLICATION_REGISTERED_CONTEXT_VALIDATED",
    "SCENARIO_SPECIFIC_RISK_ABSTENTIONS",
    "CONCLUSIVE_REJECTION_FOR_FULLY_OBSERVED_FAILED_MANDATORY_GATE",
    "INDEPENDENT_BASELINE_UNIVERSES_SCHEDULES_COSTS_AND_PATHS",
    "COMPLETE_SYNTHETIC_SOURCE_TO_TERMINAL_PIPELINE",
    "SOURCE_COVERAGE_AND_IMMUTABLE_SOURCE_SET_BOUND",
    "DURABLE_UNPUBLISHED_EVIDENCE_AND_SINGLE_USE_AUTHORIZATION",
    "LOCKED_COST_RISK_STATISTICS_AND_1500_DRAWDOWN_GATE",
    "HOLDOUT_2025_PROVIDER_CREDENTIAL_AND_TRADING_FAIL_CLOSED",
)


@dataclass(frozen=True)
class PreparedAuthoritativeLifecycle:
    trial_id: str
    trial: Mapping[str, object]
    pointer_id: str
    pointer: Mapping[str, object]
    certificate_id: str
    certificate: Mapping[str, object]
    failed_retirement_id: str
    failed_retirement: Mapping[str, object]


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid authoritative lifecycle artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("authoritative lifecycle artifact is not an object")
    return value


def load_authoritative_synthetic_verification(*, root: Path) -> dict[str, object]:
    verification = _object(root / SYNTHETIC_VERIFICATION_PATH)
    core = dict(verification)
    verification_id = core.pop("verification_id", None)
    prefixes = (
        "test_tier1_bracket", "test_tier1_frozen", "test_tier1_preexecution",
        "test_tier1_standard", "test_tier1_final", "test_tier1_authoritative",
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
        != "tier1_authoritative_synthetic_verification/1.0.0"
        or verification.get("state") != "PREPARED_NOT_PUBLISHED"
        or verification.get("test_file_count") != len(paths)
        or verification.get("test_tree_id") != tree_id
        or verification.get("conftest_sha256") != sha256_file(root / "tests/conftest.py")
        or verification.get("runner_sha256")
        != sha256_file(root / "scripts/run_windows_host_root_pytest.ps1")
        or not isinstance(results, Mapping)
        or results.get("failed") != 0
        or results.get("passed") != 283
        or results.get("deselected") != 1004
        or results.get("expected_xfailed") != 1
        or any(verification.get(field) is not True for field in (
            "all_v4_through_v12_controls_included",
            "complete_synthetic_source_to_terminal_pipeline_tested",
            "scenario_specific_risk_abstention_tested",
            "conclusive_rejection_lattice_tested",
            "independent_baseline_paths_tested",
            "mutable_pointer_excluded_from_protocol_tested",
            "prepublication_context_tested", "postpublication_context_tested",
            "failed_activation_rollback_tested",
            "durable_unpublished_bundle_tested", "single_use_authorization_tested",
        ))
        or any(verification.get(field) is not False for field in (
            "historical_rows_opened", "provider_or_network_access", "credential_access",
            "holdout_2025_access", "real_model_fit", "historical_evaluation",
            "trial_registration", "publication", "trading",
        ))
    ):
        raise IntegrityError("authoritative synthetic verification is incomplete or drifted")
    return verification


def _bindings(root: Path) -> dict[str, str]:
    paths = (
        CLOSURE_PATH, PROTOCOL_PATH, PLAN_PATH, SYNTHETIC_VERIFICATION_PATH,
        Path("configs/tier1_bracket_version_freeze_reconciliation.json"),
        Path("configs/tier1_bracket_preexecution_validity_certificate.json"),
        Path("configs/tier1_standard_only_trial_protocol.json"),
        Path("state/preexecution_certificates/tier1_standard_only_trial/221ddd3dd8816970794cff86ad1b119bfaac1b3f5678647dc8fc0bcc990ab76e.json"),
        Path("configs/dependency_lock_receipt.json"),
        Path("state/source_quality/tier1_preexecution_source_certification/7a7db45fb4e1a2e3825969e99781fd6f0d02b4dad7a7376b3f0163a0bb41cda5.json"),
        Path("state/source_quality/tier1_frozen_source_adequacy/b3d8efbb010631922a944f13aff2de77e20d6775a2d98e5333994eca33cb5fbf.json"),
        Path("src/futures_rebuild/tier1_authoritative_protocol.py"),
        Path("src/futures_rebuild/tier1_authoritative_execution.py"),
        Path("src/futures_rebuild/tier1_authoritative_lifecycle.py"),
        Path("src/futures_rebuild/tier1_final_decision_validity.py"),
        Path("src/futures_rebuild/tier1_final_pipeline.py"),
        Path("src/futures_rebuild/tier1_final_unpublished_evidence.py"),
        Path("src/futures_rebuild/tier1_frozen_successor_source_semantics.py"),
        Path("src/futures_rebuild/tier1_standard_only_execution.py"),
        Path("scripts/run_tier1_authoritative_historical_execution.py"),
        Path("scripts/run_windows_host_root_pytest.ps1"),
        Path("tests/conftest.py"),
        Path("tests/test_tier1_authoritative_lifecycle.py"),
    )
    return {path.as_posix(): sha256_file(root / path) for path in paths}


def build_authoritative_lifecycle_payloads(
    *, protocol: Mapping[str, object], closure: Mapping[str, object],
    verification: Mapping[str, object], bindings: Mapping[str, str],
) -> PreparedAuthoritativeLifecycle:
    if (
        protocol.get("state") != "PREPARED_NOT_REGISTERED"
        or closure.get("state") != "PREPARED_NOT_PUBLISHED"
        or closure.get("trial_id") != FAILED_TRIAL_ID
        or verification.get("state") != "PREPARED_NOT_PUBLISHED"
        or not bindings or "configs/active_tier1_trial.json" in bindings
    ):
        raise IntegrityError("authoritative lifecycle inputs are incomplete")
    retirement_id = str(closure["record_id"])
    failed_retirement = {
        **{key: value for key, value in closure.items() if key != "record_id"},
        "state": "PREPARED_NOT_PUBLISHED",
    }
    trial = {
        "schema_version": "tier1_authoritative_trial_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": protocol["classification"],
        "protocol_id": protocol["protocol_id"],
        "supersedes_invalid_trial_id": FAILED_TRIAL_ID,
        "invalid_retirement_id": retirement_id,
        "selected_sources_id": "f61f34df0b9d8cf7b344016ce3df8bb76abeb890558740460f60d51e5ca37bde",
        "calendar_release_id": "038940d82031f31e2c66ed37186e98a6ee6cff3e7248f634f2c7a8e94ea6ecf3",
        "bindings": dict(sorted(bindings.items())),
        "source_row_access": False, "model_fit": False,
        "prediction_generation": False, "historical_evaluation": False,
        "unpublished_evidence_staging": False, "publication": False,
        "holdout_or_forward_access": False,
        "provider_or_network_access": False, "credential_access": False,
        "trading": False,
    }
    trial_id = sha256_json(trial)
    pointer = {
        "schema_version": "active_tier1_trial/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "trial_id": trial_id,
        "trial_registry_path": (REGISTRY_ROOT / f"{trial_id}.json").as_posix(),
        "preexecution_certificate_path": (
            CERTIFICATE_ROOT / f"{trial_id}.json"
        ).as_posix(),
        "protocol_id": protocol["protocol_id"],
        "holdout_or_forward_access": False,
    }
    pointer_id = sha256_json(pointer)
    certificate = {
        "schema_version": "tier1_authoritative_preexecution_certificate/1.0.0",
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
        "historical_rows_opened": False, "model_fit": False,
        "prediction_generation": False, "historical_evaluation": False,
        "holdout_2025_touched": False, "provider_or_network_access": False,
        "credential_access": False, "trading": False,
    }
    certificate_id = sha256_json(certificate)
    return PreparedAuthoritativeLifecycle(
        trial_id, trial, pointer_id, pointer, certificate_id, certificate,
        retirement_id, failed_retirement,
    )


def published_documents(
    prepared: PreparedAuthoritativeLifecycle,
) -> dict[str, dict[str, object]]:
    return {
        "failed_retirement": {
            **prepared.failed_retirement,
            "state": "RETIRED_INVALID_PRE_DATA",
            "record_id": prepared.failed_retirement_id,
        },
        "registry": {
            **prepared.trial,
            "state": "REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
            "trial_id": prepared.trial_id,
        },
        "pointer": {
            **prepared.pointer,
            "state": "ACTIVE_REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
            "pointer_id": prepared.pointer_id,
        },
        "certificate": {
            **prepared.certificate,
            "state": "PUBLISHED_PREEXECUTION_PASS",
            "certificate_id": prepared.certificate_id,
        },
    }


def prepare_authoritative_lifecycle(*, root: Path) -> PreparedAuthoritativeLifecycle:
    protocol = load_authoritative_protocol(root=root)
    closure = load_failed_final_closure_preparation(root=root)
    verification = load_authoritative_synthetic_verification(root=root)
    plan = load_authoritative_execution_plan(root=root)
    pointer = _object(root / ACTIVE_POINTER_PATH)
    if (
        pointer.get("trial_id") != EXPECTED_PREPUBLICATION_TRIAL_ID
        or sha256_file(root / ACTIVE_POINTER_PATH)
        != EXPECTED_PREPUBLICATION_POINTER_SHA256
        or plan.get("protocol_id") != protocol.get("protocol_id")
    ):
        raise IntegrityError("authoritative prepublication pointer condition failed")
    prepared = build_authoritative_lifecycle_payloads(
        protocol=protocol, closure=closure, verification=verification,
        bindings=_bindings(root),
    )
    documents = published_documents(prepared)
    validate_registered_context_documents(
        plan=plan, pointer=documents["pointer"], registry=documents["registry"],
        certificate=documents["certificate"],
        failed_retirement=documents["failed_retirement"],
    )
    return prepared


def load_prepared_authoritative_certificate(*, root: Path) -> dict[str, object]:
    prepared = prepare_authoritative_lifecycle(root=root)
    expected = {**prepared.certificate, "certificate_id": prepared.certificate_id}
    certificate = _object(root / PREPARED_CERTIFICATE_PATH)
    if certificate != expected:
        raise IntegrityError("prepared authoritative certificate drifted")
    return certificate


def replace_pointer_with_rollback(
    *, pointer_path: Path, new_bytes: bytes, expected_old_sha256: str,
    postcheck: Callable[[], object],
) -> None:
    """Replace one pointer and restore its exact bytes if validation fails."""

    if sha256_file(pointer_path) != expected_old_sha256:
        raise IntegrityError("active pointer changed before compare-and-swap")
    old_bytes = pointer_path.read_bytes()
    replacement = pointer_path.with_suffix(pointer_path.suffix + ".authoritative-new")
    with replacement.open("xb") as stream:
        stream.write(new_bytes)
    if sha256_file(pointer_path) != expected_old_sha256:
        raise IntegrityError("active pointer changed during compare-and-swap")
    os.replace(replacement, pointer_path)
    try:
        postcheck()
    except Exception:
        rollback = pointer_path.with_suffix(pointer_path.suffix + ".authoritative-rollback")
        with rollback.open("xb") as stream:
            stream.write(old_bytes)
        os.replace(rollback, pointer_path)
        raise


def persist_authoritative_lifecycle(
    *, root: Path, prepared: PreparedAuthoritativeLifecycle,
) -> dict[str, str]:
    """Publish only after separate approval, with pointer replacement last."""

    expected = prepare_authoritative_lifecycle(root=root)
    if prepared != expected:
        raise IntegrityError("authoritative lifecycle changed after preparation")
    load_prepared_authoritative_certificate(root=root)
    documents = published_documents(prepared)
    plan = load_authoritative_execution_plan(root=root)
    validate_registered_context_documents(
        plan=plan, pointer=documents["pointer"], registry=documents["registry"],
        certificate=documents["certificate"],
        failed_retirement=documents["failed_retirement"],
    )
    retirement_path = FAILED_RETIREMENT_ROOT / f"{prepared.failed_retirement_id}.json"
    retirement_event = FAILED_RETIREMENT_EVENT_ROOT / f"{prepared.failed_retirement_id}.json"
    registry_path = REGISTRY_ROOT / f"{prepared.trial_id}.json"
    trial_event = TRIAL_EVENT_ROOT / f"{prepared.trial_id}.json"
    certificate_path = CERTIFICATE_ROOT / f"{prepared.trial_id}.json"
    destinations = (
        retirement_path, retirement_event, registry_path, trial_event, certificate_path,
    )
    if any((root / path).exists() for path in destinations):
        raise IntegrityError("authoritative immutable publication path already exists")
    if sha256_file(root / ACTIVE_POINTER_PATH) != EXPECTED_PREPUBLICATION_POINTER_SHA256:
        raise IntegrityError("active pointer changed before publication")
    payloads = (
        (retirement_path, documents["failed_retirement"]),
        (retirement_event, {
            "schema_version": "tier1_final_pointer_binding_invalid_retirement_event/1.0.0",
            "event_type": "RETIRED_INVALID_PRE_DATA", "trial_id": FAILED_TRIAL_ID,
            "record_id": prepared.failed_retirement_id,
        }),
        (registry_path, documents["registry"]),
        (trial_event, {
            "schema_version": "tier1_authoritative_trial_event/1.0.0",
            "event_type": "DECLARED", "trial_id": prepared.trial_id,
            "source_row_access": False, "model_fit": False,
            "prediction_generation": False, "historical_evaluation": False,
            "holdout_or_forward_access": False,
        }),
        (certificate_path, documents["certificate"]),
    )
    for path, payload in payloads:
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        with (root / path).open("xb") as stream:
            stream.write(canonical_bytes(payload) + b"\n")
    replace_pointer_with_rollback(
        pointer_path=root / ACTIVE_POINTER_PATH,
        new_bytes=canonical_bytes(documents["pointer"]) + b"\n",
        expected_old_sha256=EXPECTED_PREPUBLICATION_POINTER_SHA256,
        postcheck=lambda: load_authoritative_registered_context(root=root, plan=plan),
    )
    return {
        "retirement_id": prepared.failed_retirement_id,
        "trial_id": prepared.trial_id,
        "certificate_id": prepared.certificate_id,
        "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
    }
