"""Terminal authoritative lifecycle with pointer-neutral certification tests."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError
from .tier1_authoritative_execution import (
    ACTIVE_POINTER_PATH,
    CERTIFICATE_ROOT,
    PLAN_PATH,
    REGISTRY_ROOT,
    load_authoritative_execution_plan,
)
from .tier1_authoritative_lifecycle import (
    PreparedAuthoritativeLifecycle,
    published_documents,
    replace_pointer_with_rollback,
)
from .tier1_authoritative_protocol import load_authoritative_protocol
from .tier1_authoritative_terminal_execution import (
    INVALID_DISPOSITION,
    INVALID_RETIREMENT_ROOT,
    INVALID_TRIAL_ID,
    load_terminal_registered_context,
    validate_terminal_registered_context_documents,
)


EXPECTED_PREPUBLICATION_POINTER_SHA256 = (
    "38963664d5e1c04ed4e37aa4d4931b4d40f47b031ed0b995abb2aa950d19644f"
)
EXPECTED_PREPUBLICATION_TRIAL_ID = (
    "221ddd3dd8816970794cff86ad1b119bfaac1b3f5678647dc8fc0bcc990ab76e"
)
INVALID_PREPARATION_PATH = Path(
    "configs/tier1_authoritative_140f_invalid_retirement_preparation.json"
)
SYNTHETIC_VERIFICATION_PATH = Path(
    "configs/tier1_authoritative_terminal_synthetic_verification.json"
)
PREPARED_CERTIFICATE_PATH = Path(
    "configs/tier1_authoritative_terminal_preexecution_certificate.json"
)
INVALID_RETIREMENT_EVENT_ROOT = Path(
    "state/trial_events/tier1_authoritative_postcheck_invalid_retirement"
)
TRIAL_EVENT_ROOT = Path("state/trial_events/tier1_authoritative_trial")
PUBLISHED_REGISTRY_PATH = REGISTRY_ROOT / f"{INVALID_TRIAL_ID}.json"
PUBLISHED_EVENT_PATH = TRIAL_EVENT_ROOT / f"{INVALID_TRIAL_ID}.json"
PUBLISHED_CERTIFICATE_PATH = CERTIFICATE_ROOT / f"{INVALID_TRIAL_ID}.json"

INVALID_TEST_FILES = (
    "tests/test_tier1_authoritative_lifecycle.py",
    "tests/test_tier1_authoritative_stable_lifecycle.py",
    "tests/test_tier1_authoritative_certified_lifecycle.py",
)
SUPERSEDED_NODE_IDS = (
    "tests/test_tier1_bracket_v10_registration.py::test_v9_retirement_and_v10_registration_are_prepared_create_only",
    "tests/test_tier1_final_decision_validity.py::test_final_lifecycle_is_prepared_without_publication_or_pointer_mutation",
    "tests/test_tier1_final_decision_validity.py::test_final_protocol_preserves_parameters_and_closes_predecessor_only",
    "tests/test_tier1_final_decision_validity.py::test_final_execution_plan_requires_registration_and_durable_staging",
)
GATES = (
    "VERSION_RECONCILIATION_AND_SUCCESSOR_FREEZE",
    "ALL_PRIOR_REGISTERED_BYTES_PRESERVED",
    "PUBLISHED_140F_INVALID_PRE_DATA_RETIREMENT_BOUND",
    "RESEARCH_PARAMETERS_UNCHANGED_AND_OUTCOME_UNTUNED",
    "MUTABLE_ACTIVE_POINTER_EXCLUDED_FROM_PROTOCOL_BINDINGS",
    "POINTER_NEUTRAL_IDENTICAL_PRE_AND_POST_ACTIVATION_SUITE",
    "SCENARIO_SPECIFIC_RISK_ABSTENTIONS",
    "CONCLUSIVE_REJECTION_FOR_FULLY_OBSERVED_FAILED_MANDATORY_GATE",
    "INDEPENDENT_BASELINE_UNIVERSES_SCHEDULES_COSTS_AND_PATHS",
    "COMPLETE_SYNTHETIC_SOURCE_TO_TERMINAL_PIPELINE",
    "SOURCE_COVERAGE_AND_IMMUTABLE_SOURCE_SET_BOUND",
    "DURABLE_UNPUBLISHED_EVIDENCE_AND_SINGLE_USE_AUTHORIZATION",
    "LOCKED_COST_RISK_STATISTICS_AND_1500_DRAWDOWN_GATE",
    "HOLDOUT_2025_PROVIDER_CREDENTIAL_AND_TRADING_FAIL_CLOSED",
)


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid terminal lifecycle artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("terminal lifecycle artifact is not an object")
    return value


def load_140f_invalid_retirement_preparation(*, root: Path) -> dict[str, object]:
    preparation = _object(root / INVALID_PREPARATION_PATH)
    core = dict(preparation)
    record_id = core.pop("record_id", None)
    bindings = preparation.get("preserved_bindings")
    expected_paths = {
        PUBLISHED_REGISTRY_PATH.as_posix(),
        PUBLISHED_EVENT_PATH.as_posix(),
        PUBLISHED_CERTIFICATE_PATH.as_posix(),
    }
    if (
        record_id != sha256_json(core)
        or preparation.get("schema_version")
        != "tier1_authoritative_postcheck_invalid_retirement/1.0.0"
        or preparation.get("state") != "PREPARED_NOT_PUBLISHED"
        or preparation.get("trial_id") != INVALID_TRIAL_ID
        or preparation.get("disposition") != INVALID_DISPOSITION
        or preparation.get("research_parameter_defect") is not False
        or any(
            preparation.get(field) is not False
            for field in (
                "historical_rows_opened",
                "model_fit",
                "predictions_generated",
                "historical_evaluation",
                "holdout_2025_touched",
                "provider_or_network_access",
                "credential_access",
                "trading",
            )
        )
        or not isinstance(bindings, Mapping)
        or set(bindings) != expected_paths
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("140f invalid-retirement preparation drifted")
    return preparation


def _tier1_test_paths(root: Path) -> list[Path]:
    prefixes = (
        "test_tier1_bracket",
        "test_tier1_frozen",
        "test_tier1_preexecution",
        "test_tier1_standard",
        "test_tier1_final",
        "test_tier1_authoritative",
    )
    return sorted(
        path
        for path in (root / "tests").glob("test_tier1_*.py")
        if path.name.startswith(prefixes)
    )


def load_terminal_synthetic_verification(*, root: Path) -> dict[str, object]:
    verification = _object(root / SYNTHETIC_VERIFICATION_PATH)
    core = dict(verification)
    verification_id = core.pop("verification_id", None)
    paths = _tier1_test_paths(root)
    tree_id = sha256_json(
        {
            "files": [
                {"path": path.relative_to(root).as_posix(), "sha256": sha256_file(path)}
                for path in paths
            ]
        }
    )
    selection = verification.get("selection")
    results = verification.get("applicable_results")
    replacement = verification.get("replacement_control_map")
    if (
        verification_id != sha256_json(core)
        or verification.get("schema_version")
        != "tier1_authoritative_terminal_synthetic_verification/1.0.0"
        or verification.get("state") != "PREPARED_NOT_PUBLISHED"
        or verification.get("test_file_count") != len(paths)
        or verification.get("test_tree_id") != tree_id
        or verification.get("conftest_sha256") != sha256_file(root / "tests/conftest.py")
        or verification.get("runner_sha256")
        != sha256_file(root / "scripts/run_windows_host_root_pytest.ps1")
        or not isinstance(selection, Mapping)
        or selection.get("ignored_invalid_test_files") != list(INVALID_TEST_FILES)
        or selection.get("deselected_historical_assertions")
        != list(SUPERSEDED_NODE_IDS)
        or selection.get("same_arguments_required_pre_and_post_activation") is not True
        or selection.get("selected_tests_may_call_prepublication_builder") is not False
        or not isinstance(results, Mapping)
        or not isinstance(results.get("passed"), int)
        or results.get("passed", 0) < 283
        or results.get("failed") != 0
        or results.get("expected_xfailed") != 1
        or not isinstance(replacement, Mapping)
        or set(replacement) != set(INVALID_TEST_FILES)
        or any(not isinstance(value, list) or len(value) < 12 for value in replacement.values())
        or any(
            verification.get(field) is not True
            for field in (
                "all_v4_through_v12_controls_included",
                "complete_synthetic_source_to_terminal_pipeline_tested",
                "scenario_specific_risk_abstention_tested",
                "conclusive_rejection_lattice_tested",
                "independent_baseline_paths_tested",
                "synthetic_active_documents_tested",
                "live_pointer_selected_context_tested",
                "prepublication_builder_call_forbidden_in_selected_tests",
                "failed_activation_rollback_tested",
                "durable_unpublished_bundle_tested",
                "single_use_authorization_tested",
            )
        )
        or any(
            verification.get(field) is not False
            for field in (
                "historical_rows_opened",
                "provider_or_network_access",
                "credential_access",
                "holdout_2025_access",
                "real_model_fit",
                "historical_evaluation",
                "trial_registration",
                "publication",
                "trading",
            )
        )
    ):
        raise IntegrityError("terminal synthetic verification is incomplete or drifted")
    return verification


def _bindings(root: Path) -> dict[str, str]:
    invalid_registry = _object(root / PUBLISHED_REGISTRY_PATH)
    inherited = invalid_registry.get("bindings")
    if not isinstance(inherited, Mapping) or not inherited:
        raise IntegrityError("published 140f registry bindings are unavailable")
    bindings = {str(path): str(digest) for path, digest in inherited.items()}
    additions = (
        INVALID_PREPARATION_PATH,
        SYNTHETIC_VERIFICATION_PATH,
        PUBLISHED_REGISTRY_PATH,
        PUBLISHED_EVENT_PATH,
        PUBLISHED_CERTIFICATE_PATH,
        Path("src/futures_rebuild/tier1_authoritative_terminal_execution.py"),
        Path("src/futures_rebuild/tier1_authoritative_terminal_lifecycle.py"),
        Path("scripts/run_tier1_authoritative_terminal_historical_execution.py"),
        Path("scripts/publish_tier1_authoritative_terminal_lifecycle.py"),
        Path("tests/test_tier1_authoritative_terminal_lifecycle.py"),
    )
    bindings.update({path.as_posix(): sha256_file(root / path) for path in additions})
    if "configs/active_tier1_trial.json" in bindings:
        raise IntegrityError("mutable active pointer entered terminal bindings")
    if any(sha256_file(root / path) != digest for path, digest in bindings.items()):
        raise IntegrityError("terminal binding drifted")
    return dict(sorted(bindings.items()))


def build_terminal_lifecycle_payloads(
    *,
    protocol: Mapping[str, object],
    invalid_preparation: Mapping[str, object],
    verification: Mapping[str, object],
    bindings: Mapping[str, str],
) -> PreparedAuthoritativeLifecycle:
    if (
        protocol.get("state") != "PREPARED_NOT_REGISTERED"
        or invalid_preparation.get("state") != "PREPARED_NOT_PUBLISHED"
        or invalid_preparation.get("trial_id") != INVALID_TRIAL_ID
        or verification.get("state") != "PREPARED_NOT_PUBLISHED"
        or not bindings
        or "configs/active_tier1_trial.json" in bindings
    ):
        raise IntegrityError("terminal lifecycle inputs are incomplete")
    retirement_id = str(invalid_preparation["record_id"])
    retirement = {
        **{key: value for key, value in invalid_preparation.items() if key != "record_id"},
        "state": "PREPARED_NOT_PUBLISHED",
    }
    trial = {
        "schema_version": "tier1_authoritative_trial_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": protocol["classification"],
        "protocol_id": protocol["protocol_id"],
        "supersedes_invalid_trial_id": INVALID_TRIAL_ID,
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
    pointer = {
        "schema_version": "active_tier1_trial/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "trial_id": trial_id,
        "trial_registry_path": (REGISTRY_ROOT / f"{trial_id}.json").as_posix(),
        "preexecution_certificate_path": (CERTIFICATE_ROOT / f"{trial_id}.json").as_posix(),
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
    return PreparedAuthoritativeLifecycle(
        trial_id,
        trial,
        pointer_id,
        pointer,
        certificate_id,
        certificate,
        retirement_id,
        retirement,
    )


def _build_from_immutable_inputs(*, root: Path) -> PreparedAuthoritativeLifecycle:
    return build_terminal_lifecycle_payloads(
        protocol=load_authoritative_protocol(root=root),
        invalid_preparation=load_140f_invalid_retirement_preparation(root=root),
        verification=load_terminal_synthetic_verification(root=root),
        bindings=_bindings(root),
    )


def synthetic_active_documents(
    *, root: Path
) -> tuple[dict[str, object], dict[str, dict[str, object]]]:
    """Build and validate active-form documents without consulting the pointer."""

    plan = load_authoritative_execution_plan(root=root)
    documents = published_documents(_build_from_immutable_inputs(root=root))
    validate_terminal_registered_context_documents(
        plan=plan,
        pointer=documents["pointer"],
        registry=documents["registry"],
        certificate=documents["certificate"],
        invalid_retirement=documents["failed_retirement"],
    )
    return plan, documents


def prepare_terminal_authoritative_lifecycle(
    *, root: Path
) -> PreparedAuthoritativeLifecycle:
    pointer = _object(root / ACTIVE_POINTER_PATH)
    if (
        pointer.get("trial_id") != EXPECTED_PREPUBLICATION_TRIAL_ID
        or sha256_file(root / ACTIVE_POINTER_PATH) != EXPECTED_PREPUBLICATION_POINTER_SHA256
    ):
        raise IntegrityError("terminal prepublication pointer condition failed")
    prepared = _build_from_immutable_inputs(root=root)
    synthetic_active_documents(root=root)
    return prepared


def transition_documents(
    *, root: Path
) -> tuple[dict[str, object], dict[str, dict[str, object]]]:
    """Return valid documents selected only by the active pointer identity."""

    plan = load_authoritative_execution_plan(root=root)
    pointer = _object(root / ACTIVE_POINTER_PATH)
    if pointer.get("trial_id") == EXPECTED_PREPUBLICATION_TRIAL_ID:
        return synthetic_active_documents(root=root)
    trial_id, active, registry, certificate = load_terminal_registered_context(
        root=root, plan=plan
    )
    retirement_id = registry["invalid_retirement_id"]
    retirement = _object(root / INVALID_RETIREMENT_ROOT / f"{retirement_id}.json")
    if trial_id != registry.get("trial_id"):
        raise IntegrityError("terminal transition identity is inconsistent")
    return plan, {
        "pointer": active,
        "registry": registry,
        "certificate": certificate,
        "failed_retirement": retirement,
    }


def load_prepared_terminal_certificate(*, root: Path) -> dict[str, object]:
    prepared = prepare_terminal_authoritative_lifecycle(root=root)
    expected = {**prepared.certificate, "certificate_id": prepared.certificate_id}
    certificate = _object(root / PREPARED_CERTIFICATE_PATH)
    if certificate != expected:
        raise IntegrityError("prepared terminal certificate drifted")
    return certificate


def persist_terminal_authoritative_lifecycle(
    *,
    root: Path,
    prepared: PreparedAuthoritativeLifecycle,
    post_activation_check: Callable[[], object],
) -> dict[str, str]:
    if prepared != prepare_terminal_authoritative_lifecycle(root=root):
        raise IntegrityError("terminal lifecycle changed after preparation")
    load_prepared_terminal_certificate(root=root)
    documents = published_documents(prepared)
    plan = load_authoritative_execution_plan(root=root)
    validate_terminal_registered_context_documents(
        plan=plan,
        pointer=documents["pointer"],
        registry=documents["registry"],
        certificate=documents["certificate"],
        invalid_retirement=documents["failed_retirement"],
    )
    payloads = (
        (
            INVALID_RETIREMENT_ROOT / f"{prepared.failed_retirement_id}.json",
            documents["failed_retirement"],
        ),
        (
            INVALID_RETIREMENT_EVENT_ROOT / f"{prepared.failed_retirement_id}.json",
            {
                "schema_version": "tier1_authoritative_postcheck_invalid_retirement_event/1.0.0",
                "event_type": "RETIRED_INVALID_PRE_DATA",
                "trial_id": INVALID_TRIAL_ID,
                "record_id": prepared.failed_retirement_id,
            },
        ),
        (REGISTRY_ROOT / f"{prepared.trial_id}.json", documents["registry"]),
        (
            TRIAL_EVENT_ROOT / f"{prepared.trial_id}.json",
            {
                "schema_version": "tier1_authoritative_trial_event/1.0.0",
                "event_type": "DECLARED",
                "trial_id": prepared.trial_id,
                "source_row_access": False,
                "model_fit": False,
                "prediction_generation": False,
                "historical_evaluation": False,
                "holdout_or_forward_access": False,
            },
        ),
        (CERTIFICATE_ROOT / f"{prepared.trial_id}.json", documents["certificate"]),
    )
    if any((root / path).exists() for path, _ in payloads):
        raise IntegrityError("terminal publication destination already exists")
    if sha256_file(root / ACTIVE_POINTER_PATH) != EXPECTED_PREPUBLICATION_POINTER_SHA256:
        raise IntegrityError("active pointer changed before terminal publication")
    for path, payload in payloads:
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        with (root / path).open("xb") as stream:
            stream.write(canonical_bytes(payload) + b"\n")

    def validate_after_activation() -> None:
        load_terminal_registered_context(root=root, plan=plan)
        post_activation_check()

    replace_pointer_with_rollback(
        pointer_path=root / ACTIVE_POINTER_PATH,
        new_bytes=canonical_bytes(documents["pointer"]) + b"\n",
        expected_old_sha256=EXPECTED_PREPUBLICATION_POINTER_SHA256,
        postcheck=validate_after_activation,
    )
    return {
        "retirement_id": prepared.failed_retirement_id,
        "trial_id": prepared.trial_id,
        "certificate_id": prepared.certificate_id,
        "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
    }
