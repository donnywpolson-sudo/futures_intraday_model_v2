"""Additive, transition-stable lifecycle for the authoritative Tier 1 trial."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError
from .tier1_authoritative_execution import (
    ACTIVE_POINTER_PATH, CERTIFICATE_ROOT, PLAN_PATH, REGISTRY_ROOT,
    load_authoritative_execution_plan, load_authoritative_registered_context,
    validate_registered_context_documents,
)
from .tier1_authoritative_lifecycle import (
    EXPECTED_PREPUBLICATION_POINTER_SHA256, EXPECTED_PREPUBLICATION_TRIAL_ID,
    FAILED_RETIREMENT_EVENT_ROOT, GATES, TRIAL_EVENT_ROOT,
    PreparedAuthoritativeLifecycle, build_authoritative_lifecycle_payloads,
    published_documents, replace_pointer_with_rollback,
)
from .tier1_authoritative_protocol import (
    CLOSURE_PATH, FAILED_RETIREMENT_ROOT, FAILED_TRIAL_ID, PROTOCOL_PATH,
    load_authoritative_protocol, load_failed_final_closure_preparation,
)


INVALID_PREPARATION_PATH = Path(
    "configs/tier1_authoritative_unpublished_certification_invalid_461cffa1.json"
)
SYNTHETIC_VERIFICATION_PATH = Path(
    "configs/tier1_authoritative_stable_synthetic_verification.json"
)
PREPARED_CERTIFICATE_PATH = Path(
    "configs/tier1_authoritative_stable_preexecution_certificate.json"
)


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid stable authoritative artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("stable authoritative artifact is not an object")
    return value


def load_invalid_unpublished_preparation(*, root: Path) -> dict[str, object]:
    invalid = _object(root / INVALID_PREPARATION_PATH)
    core = dict(invalid)
    record_id = core.pop("record_id", None)
    finding = invalid.get("finding")
    lifecycle = invalid.get("lifecycle_status")
    bindings = invalid.get("preserved_bindings")
    boundary = invalid.get("replacement_boundary")
    if (
        record_id != sha256_json(core)
        or invalid.get("schema_version")
        != "tier1_authoritative_unpublished_certification_invalid/1.0.0"
        or invalid.get("state") != "PREPARED_NOT_PUBLISHED"
        or invalid.get("prepared_trial_id")
        != "461cffa140e546b57c91609a8cea18e4b7506b517c75010b320aa59295264167"
        or invalid.get("disposition")
        != "UNPUBLISHED_CERTIFICATION_INVALID_TRANSITION_TESTS_CALLED_PREPUBLICATION_LOADER_AFTER_ACTIVATION"
        or not isinstance(finding, Mapping)
        or finding.get("research_parameter_defect") is not False
        or not isinstance(lifecycle, Mapping)
        or any(lifecycle.get(field) is not False for field in (
            "published", "registered", "activated", "historical_rows_opened",
            "authorization_claim_created", "model_fit", "predictions_generated",
            "historical_evaluation", "holdout_2025_touched",
            "provider_or_network_access", "credential_access", "trading",
        ))
        or not isinstance(bindings, Mapping) or not bindings
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
        or not isinstance(boundary, Mapping)
        or any(boundary.get(field) is not False for field in (
            "research_parameters_may_change", "publication_authorized",
            "registration_authorized", "activation_authorized", "execution_authorized",
        ))
    ):
        raise IntegrityError("invalid unpublished authoritative preparation drifted")
    return invalid


def load_stable_synthetic_verification(*, root: Path) -> dict[str, object]:
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
    selection = verification.get("selection")
    if (
        verification_id != sha256_json(core)
        or verification.get("schema_version")
        != "tier1_authoritative_stable_synthetic_verification/1.0.0"
        or verification.get("state") != "PREPARED_NOT_PUBLISHED"
        or verification.get("test_file_count") != len(paths)
        or verification.get("test_tree_id") != tree_id
        or verification.get("conftest_sha256") != sha256_file(root / "tests/conftest.py")
        or verification.get("runner_sha256")
        != sha256_file(root / "scripts/run_windows_host_root_pytest.ps1")
        or not isinstance(selection, Mapping)
        or selection.get("ignored_invalid_test_file")
        != "tests/test_tier1_authoritative_lifecycle.py"
        or not isinstance(results, Mapping)
        or results.get("passed") != 283
        or results.get("failed") != 0
        or results.get("deselected") != 1004
        or results.get("expected_xfailed") != 1
        or any(verification.get(field) is not True for field in (
            "all_v4_through_v12_controls_included",
            "complete_synthetic_source_to_terminal_pipeline_tested",
            "scenario_specific_risk_abstention_tested",
            "conclusive_rejection_lattice_tested",
            "independent_baseline_paths_tested",
            "invalid_preparation_preserved_tested",
            "every_selected_lifecycle_test_transition_stable",
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
        raise IntegrityError("stable authoritative synthetic verification drifted")
    return verification


def _bindings(root: Path) -> dict[str, str]:
    paths = (
        INVALID_PREPARATION_PATH, CLOSURE_PATH, PROTOCOL_PATH, PLAN_PATH,
        SYNTHETIC_VERIFICATION_PATH,
        Path("configs/tier1_bracket_version_freeze_reconciliation.json"),
        Path("configs/tier1_bracket_preexecution_validity_certificate.json"),
        Path("configs/tier1_standard_only_trial_protocol.json"),
        Path("configs/dependency_lock_receipt.json"),
        Path("state/preexecution_certificates/tier1_standard_only_trial/221ddd3dd8816970794cff86ad1b119bfaac1b3f5678647dc8fc0bcc990ab76e.json"),
        Path("state/source_quality/tier1_preexecution_source_certification/7a7db45fb4e1a2e3825969e99781fd6f0d02b4dad7a7376b3f0163a0bb41cda5.json"),
        Path("state/source_quality/tier1_frozen_source_adequacy/b3d8efbb010631922a944f13aff2de77e20d6775a2d98e5333994eca33cb5fbf.json"),
        Path("src/futures_rebuild/tier1_authoritative_protocol.py"),
        Path("src/futures_rebuild/tier1_authoritative_execution.py"),
        Path("src/futures_rebuild/tier1_authoritative_lifecycle.py"),
        Path("src/futures_rebuild/tier1_authoritative_stable_lifecycle.py"),
        Path("src/futures_rebuild/tier1_final_decision_validity.py"),
        Path("src/futures_rebuild/tier1_final_pipeline.py"),
        Path("src/futures_rebuild/tier1_final_unpublished_evidence.py"),
        Path("src/futures_rebuild/tier1_frozen_successor_source_semantics.py"),
        Path("src/futures_rebuild/tier1_standard_only_execution.py"),
        Path("scripts/run_tier1_authoritative_historical_execution.py"),
        Path("scripts/run_windows_host_root_pytest.ps1"),
        Path("tests/conftest.py"),
        Path("tests/test_tier1_authoritative_stable_lifecycle.py"),
    )
    return {path.as_posix(): sha256_file(root / path) for path in paths}


def prepare_stable_authoritative_lifecycle(
    *, root: Path,
) -> PreparedAuthoritativeLifecycle:
    load_invalid_unpublished_preparation(root=root)
    protocol = load_authoritative_protocol(root=root)
    closure = load_failed_final_closure_preparation(root=root)
    verification = load_stable_synthetic_verification(root=root)
    plan = load_authoritative_execution_plan(root=root)
    pointer = _object(root / ACTIVE_POINTER_PATH)
    if (
        pointer.get("trial_id") != EXPECTED_PREPUBLICATION_TRIAL_ID
        or sha256_file(root / ACTIVE_POINTER_PATH)
        != EXPECTED_PREPUBLICATION_POINTER_SHA256
        or plan.get("protocol_id") != protocol.get("protocol_id")
    ):
        raise IntegrityError("stable authoritative prepublication pointer condition failed")
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


def transition_documents(
    *, root: Path,
) -> tuple[dict[str, object], dict[str, dict[str, object]]]:
    """Return valid documents in either prepublication or activated state."""

    plan = load_authoritative_execution_plan(root=root)
    registries = sorted((root / REGISTRY_ROOT).glob("*.json"))
    if registries:
        trial_id, pointer, registry, certificate = (
            load_authoritative_registered_context(root=root, plan=plan)
        )
        retirement_id = registry.get("invalid_retirement_id")
        if not isinstance(retirement_id, str):
            raise IntegrityError("stable registered retirement identity is absent")
        failed_retirement = _object(
            root / FAILED_RETIREMENT_ROOT / f"{retirement_id}.json"
        )
        documents = {
            "pointer": pointer, "registry": registry,
            "certificate": certificate, "failed_retirement": failed_retirement,
        }
        validate_registered_context_documents(plan=plan, **documents)
        if trial_id != registry.get("trial_id"):
            raise IntegrityError("stable registered trial identity is inconsistent")
        return plan, documents
    prepared = prepare_stable_authoritative_lifecycle(root=root)
    documents = published_documents(prepared)
    validate_registered_context_documents(plan=plan, **documents)
    return plan, documents


def load_prepared_stable_certificate(*, root: Path) -> dict[str, object]:
    prepared = prepare_stable_authoritative_lifecycle(root=root)
    expected = {**prepared.certificate, "certificate_id": prepared.certificate_id}
    certificate = _object(root / PREPARED_CERTIFICATE_PATH)
    if certificate != expected:
        raise IntegrityError("prepared stable authoritative certificate drifted")
    return certificate


def persist_stable_authoritative_lifecycle(
    *, root: Path, prepared: PreparedAuthoritativeLifecycle,
) -> dict[str, str]:
    """Publish only after separate approval; validate after pointer replacement."""

    if prepared != prepare_stable_authoritative_lifecycle(root=root):
        raise IntegrityError("stable authoritative lifecycle changed after preparation")
    load_prepared_stable_certificate(root=root)
    documents = published_documents(prepared)
    plan = load_authoritative_execution_plan(root=root)
    validate_registered_context_documents(plan=plan, **documents)
    retirement_path = FAILED_RETIREMENT_ROOT / f"{prepared.failed_retirement_id}.json"
    retirement_event = FAILED_RETIREMENT_EVENT_ROOT / f"{prepared.failed_retirement_id}.json"
    registry_path = REGISTRY_ROOT / f"{prepared.trial_id}.json"
    trial_event = TRIAL_EVENT_ROOT / f"{prepared.trial_id}.json"
    certificate_path = CERTIFICATE_ROOT / f"{prepared.trial_id}.json"
    destinations = (
        retirement_path, retirement_event, registry_path, trial_event, certificate_path,
    )
    if any((root / path).exists() for path in destinations):
        raise IntegrityError("stable authoritative publication path already exists")
    if sha256_file(root / ACTIVE_POINTER_PATH) != EXPECTED_PREPUBLICATION_POINTER_SHA256:
        raise IntegrityError("active pointer changed before stable publication")
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
