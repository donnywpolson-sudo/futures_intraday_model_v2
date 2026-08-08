"""Prepare, but never publish, the Standard-only Tier 1 trial lifecycle."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError
from .tier1_standard_only_protocol import (
    PROTOCOL_PATH,
    PUBLICATION_ROOT,
    build_source_policy_correction_record,
    load_standard_only_protocol,
)


SYNTHETIC_VERIFICATION_PATH = Path(
    "configs/tier1_standard_only_synthetic_verification.json"
)
PREPARED_CERTIFICATE_PATH = Path(
    "configs/tier1_standard_only_preexecution_certificate.json"
)
INHERITED_SYNTHETIC_VERIFICATION_PATH = Path(
    "configs/tier1_frozen_synthetic_verification.json"
)
MECHANICS_CERTIFICATE_PATH = Path(
    "configs/tier1_bracket_preexecution_validity_certificate.json"
)
RECONCILIATION_PATH = Path("configs/tier1_bracket_version_freeze_reconciliation.json")
TRIAL_REGISTRY_ROOT = Path("state/trial_registry/tier1_standard_only_trial")
TRIAL_EVENT_ROOT = Path("state/trial_events/tier1_standard_only_trial")
CERTIFICATE_ROOT = Path("state/preexecution_certificates/tier1_standard_only_trial")
ACTIVE_POINTER_PATH = Path("configs/active_tier1_trial.json")

FOUNDATIONAL_GATES = (
    "VERSION_LINEAGE_AND_FREEZE",
    "ALL_V4_THROUGH_V12_DEFECTS_HAVE_ADVERSARIAL_TESTS",
    "COMPLETE_SYNTHETIC_SOURCE_TO_TERMINAL_PIPELINE",
    "INDEPENDENT_BASELINE_UNIVERSES_SCHEDULES_COSTS_AND_ACCOUNT_PATHS",
    "PREDICTION_ELIGIBILITY_IS_OUTCOME_INDEPENDENT",
    "COST_RISK_STATISTICS_AND_PROTOCOL_ALIGNMENT",
    "EVIDENCE_RUNTIME_AND_SINGLE_USE_AUTHORIZATION",
    "HOLDOUT_2025_FAILS_CLOSED_BEFORE_OPEN",
)
REPLACED_PENDING_GATES = (
    "ONE_IMMUTABLE_SOURCE_SET_BOUND",
    "SOURCE_COVERAGE_SUFFICIENT_FOR_EVERY_REQUIRED_DEPENDENCY",
    "ONE_AUTHORITATIVE_EXECUTION_READY_TRIAL_POINTER",
)


@dataclass(frozen=True)
class PreparedStandardOnlyLifecycle:
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
        raise IntegrityError(f"invalid Standard-only lifecycle artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("Standard-only lifecycle artifact is not an object")
    return value


def load_standard_only_synthetic_verification(*, root: Path) -> dict[str, object]:
    verification = _object(root / SYNTHETIC_VERIFICATION_PATH)
    core = dict(verification)
    verification_id = core.pop("verification_id", None)
    prefixes = (
        "test_tier1_bracket", "test_tier1_frozen",
        "test_tier1_preexecution", "test_tier1_standard",
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
        != "tier1_standard_only_synthetic_verification/1.0.0"
        or verification.get("state") != "PREPARED_NOT_PUBLISHED"
        or verification.get("test_file_count") != len(paths)
        or verification.get("test_tree_id") != tree_id
        or verification.get("conftest_sha256")
        != sha256_file(root / "tests/conftest.py")
        or verification.get("windows_host_root_runner_sha256")
        != sha256_file(root / "scripts/run_windows_host_root_pytest.ps1")
        or not isinstance(results, Mapping)
        or results.get("passed") != 266
        or results.get("failed") != 0
        or results.get("deselected") != 23
        or results.get("expected_xfailed") != 1
        or not isinstance(selection, Mapping)
        or selection.get("marker") != "high_risk"
        or not isinstance(selection.get("deselected_historical_assertion"), str)
        or not isinstance(selection.get("expected_xfail"), str)
        or sha256_file(root / INHERITED_SYNTHETIC_VERIFICATION_PATH)
        != verification.get("inherited_verification_sha256")
        or any(verification.get(field) is not True for field in (
            "all_v4_through_v12_defect_controls_included",
            "complete_synthetic_source_to_terminal_pipeline",
            "independent_baseline_universes_and_account_paths",
            "prediction_eligibility_outcome_independent",
            "selected_missing_path_forces_inconclusive",
            "missing_path_zero_return_imputation_forbidden",
            "runner_up_substitution_forbidden",
            "same_rule_for_candidate_and_active_baselines",
            "cost_risk_statistics_evidence_runtime_and_authorization_aligned",
            "holdout_2025_fails_closed_before_open",
        ))
        or any(verification.get(field) is not False for field in (
            "historical_source_rows_opened", "provider_or_network_access",
            "credential_access", "holdout_or_forward_access",
            "model_fit_on_real_data", "historical_performance_evaluation",
            "trial_registration", "trading",
        ))
    ):
        raise IntegrityError("Standard-only synthetic verification is incomplete or drifted")
    return verification


def _validate_foundational_certificate(mechanics: Mapping[str, object]) -> None:
    gates = mechanics.get("gates")
    runtime = mechanics.get("locked_runtime")
    if not isinstance(gates, list) or not isinstance(runtime, Mapping):
        raise IntegrityError("foundational mechanics certificate is incomplete")
    statuses = {
        str(item.get("gate")): item.get("status")
        for item in gates if isinstance(item, Mapping)
    }
    if (
        mechanics.get("overall_decision") != "NOT_READY"
        or any(statuses.get(gate) != "PASS" for gate in FOUNDATIONAL_GATES)
        or any(statuses.get(gate) != "FAIL" for gate in REPLACED_PENDING_GATES)
        or runtime.get("dependency_lock_path") != "configs/dependency_lock_receipt.json"
        or runtime.get("dependency_lock_sha256")
        != "e6302e16d30d114b0c3140f2b075fd6850558ce4ea245475d2687b8531db1726"
        or mechanics.get("holdout_2025_touched") is not False
        or mechanics.get("provider_access") is not False
        or mechanics.get("trading") is not False
    ):
        raise IntegrityError("foundational mechanics certificate no longer supports replacement")


def build_standard_only_lifecycle_payloads(
    *, protocol: Mapping[str, object], correction: Mapping[str, object],
    synthetic: Mapping[str, object], mechanics: Mapping[str, object],
    reconciliation: Mapping[str, object], bindings: Mapping[str, str],
) -> PreparedStandardOnlyLifecycle:
    source = protocol.get("source")
    policy = correction.get("correction")
    scope = correction.get("publication_scope")
    results = synthetic.get("applicable_results")
    _validate_foundational_certificate(mechanics)
    if (
        protocol.get("state") != "PUBLISHED_PRE_REGISTRATION_PROTOCOL_ONLY"
        or not isinstance(source, Mapping)
        or source.get("historical_l1_bbo_dependency") is not False
        or correction.get("state")
        != "PUBLISHED_PRE_REGISTRATION_SOURCE_POLICY_CORRECTION_ONLY"
        or not isinstance(policy, Mapping)
        or policy.get("all_checkpoints_retained_with_terminal_source_status") is not True
        or policy.get("missing_prices_or_fills_invented") is not False
        or policy.get("missing_execution_return_imputed_as_zero") is not False
        or policy.get("runner_up_substitution_after_missing_selected_path") is not False
        or policy.get("selected_missing_path_result") != "INCONCLUSIVE_DATA_OR_COVERAGE"
        or policy.get("promotion_possible_with_selected_missing_path") is not False
        or not isinstance(scope, Mapping) or any(scope.values())
        or synthetic.get("state") != "PREPARED_NOT_PUBLISHED"
        or not isinstance(results, Mapping)
        or results.get("failed") != 0
        or not isinstance(results.get("passed"), int)
        or int(results.get("passed", 0)) < 29
        or synthetic.get("selected_missing_path_forces_inconclusive") is not True
        or synthetic.get("missing_path_zero_return_imputation_forbidden") is not True
        or synthetic.get("runner_up_substitution_forbidden") is not True
        or reconciliation.get("authoritative_current_trial") is not None
        or reconciliation.get("current_state") != "NO_EXECUTION_READY_TIER1_TRIAL"
        or reconciliation.get("version_creation_frozen") is not True
        or len(reconciliation.get("versions", [])) != 13
        or not bindings
    ):
        raise IntegrityError("Standard-only lifecycle cannot be prepared before every gate passes")
    protocol_id = protocol.get("protocol_id")
    correction_id = correction.get("record_id")
    verification_id = synthetic.get("verification_id")
    if not all(
        isinstance(value, str) and len(value) == 64
        for value in (protocol_id, correction_id, verification_id)
    ):
        raise IntegrityError("Standard-only lifecycle evidence identity is invalid")
    trial_core = {
        "schema_version": "tier1_standard_only_trial_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": protocol["classification"],
        "protocol_id": protocol_id,
        "source_policy_correction_id": correction_id,
        "selected_sources_id": source["selected_sources_id"],
        "calendar_release_id": source["calendar_release_id"],
        "bindings": dict(sorted(bindings.items())),
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "holdout_or_forward_access": False,
        "provider_access": False,
        "publication": False,
        "trading": False,
    }
    trial_id = sha256_json(trial_core)
    certificate_path = (CERTIFICATE_ROOT / f"{trial_id}.json").as_posix()
    pointer_core = {
        "schema_version": "active_tier1_trial/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "trial_id": trial_id,
        "trial_registry_path": (TRIAL_REGISTRY_ROOT / f"{trial_id}.json").as_posix(),
        "preexecution_certificate_path": certificate_path,
        "protocol_id": protocol_id,
        "source_policy_correction_id": correction_id,
        "holdout_or_forward_access": False,
    }
    pointer_id = sha256_json(pointer_core)
    certificate_core = {
        "schema_version": "tier1_standard_only_preexecution_certificate/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "overall_decision": "PASS",
        "trial_id": trial_id,
        "active_pointer_id": pointer_id,
        "protocol_id": protocol_id,
        "source_policy_correction_id": correction_id,
        "synthetic_verification_id": verification_id,
        "gates": [
            *({
                "gate": gate,
                "status": "PASS",
                "evidence": "INHERITED_HASH_BOUND_FOUNDATIONAL_CERTIFICATE_AND_CURRENT_SYNTHETIC_RERUN",
            } for gate in FOUNDATIONAL_GATES),
            {
                "gate": "ONE_IMMUTABLE_SOURCE_SET_BOUND",
                "status": "PASS",
                "evidence": "20_HASH_BOUND_SELECTED_MARKET_YEAR_RELEASES_PLUS_REGISTERED_CALENDAR",
            },
            {
                "gate": "SOURCE_COVERAGE_SUFFICIENT_FOR_EVERY_REQUIRED_DEPENDENCY",
                "status": "PASS",
                "evidence": "ALL_CHECKPOINTS_TERMINAL_FEATURE_GATES_PASS_AND_ANY_SELECTED_MISSING_PATH_FORCES_INCONCLUSIVE",
            },
            {
                "gate": "ONE_AUTHORITATIVE_EXECUTION_READY_TRIAL_POINTER",
                "status": "PASS",
                "evidence": "THIS_CERTIFICATE_BINDS_THE_SINGLE_CREATE_ONLY_POINTER_PUBLISHED_AT_REGISTRATION",
            },
        ],
        "source_coverage": {
            "calendar_open_checkpoints": 15343,
            "complete_feature_windows": 15288,
            "feature_complete_execution_complete": 15254,
            "feature_complete_execution_unavailable": 34,
            "overall_feature_gate": "PASS",
            "every_market_year_feature_gate": "PASS",
            "every_market_fold_role_feature_and_30_session_gate": "PASS",
            "terminal_execution_source_status_gate": "PASS",
            "selected_execution_path_gate": "REQUIRES_100_PERCENT_OR_EVALUATION_IS_INCONCLUSIVE",
        },
        "bindings": dict(sorted(bindings.items())),
        "one_immutable_source_set_bound": True,
        "one_authoritative_active_trial_pointer_bound": True,
        "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
        "cost_risk_statistics_evidence_runtime_and_authorization_aligned": True,
        "continuous_drawdown_limit_usd": "1500",
        "stress_costs_required_for_promotion": True,
        "separate_single_use_historical_execution_authorization_required": True,
        "holdout_2025_touched": False,
        "provider_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "trading": False,
    }
    certificate_id = sha256_json(certificate_core)
    return PreparedStandardOnlyLifecycle(
        trial_id, trial_core, pointer_id, pointer_core,
        certificate_id, certificate_core,
    )


def prepare_standard_only_lifecycle(*, root: Path) -> PreparedStandardOnlyLifecycle:
    protocol = load_standard_only_protocol(root=root)
    correction_expected = build_source_policy_correction_record(root=root)
    correction_path = (
        root / PUBLICATION_ROOT / f"{correction_expected['record_id']}.json"
    )
    correction = _object(correction_path)
    if correction != correction_expected:
        raise IntegrityError("published Standard-only correction drifted")
    synthetic = load_standard_only_synthetic_verification(root=root)
    mechanics = _object(root / MECHANICS_CERTIFICATE_PATH)
    reconciliation = _object(root / RECONCILIATION_PATH)
    bindings = {
        path.as_posix(): sha256_file(root / path)
        for path in (
            PROTOCOL_PATH,
            correction_path.relative_to(root),
            SYNTHETIC_VERIFICATION_PATH,
            MECHANICS_CERTIFICATE_PATH,
            RECONCILIATION_PATH,
            Path("src/futures_rebuild/tier1_frozen_successor_source_semantics.py"),
            Path("src/futures_rebuild/tier1_frozen_trial_pipeline.py"),
            Path("src/futures_rebuild/tier1_standard_only_protocol.py"),
            Path("src/futures_rebuild/tier1_standard_only_execution.py"),
            Path("src/futures_rebuild/tier1_standard_only_lifecycle.py"),
            Path("configs/tier1_standard_only_historical_execution_plan.json"),
            Path("scripts/run_tier1_standard_only_historical_execution.py"),
        )
    }
    return build_standard_only_lifecycle_payloads(
        protocol=protocol, correction=correction, synthetic=synthetic,
        mechanics=mechanics, reconciliation=reconciliation, bindings=bindings,
    )


def persist_standard_only_lifecycle(
    *, root: Path, prepared: PreparedStandardOnlyLifecycle,
) -> dict[str, str]:
    """Create one registry, event, certificate, and pointer; never overwrite."""

    if (
        prepared.trial_id != sha256_json(prepared.trial)
        or prepared.pointer_id != sha256_json(prepared.pointer)
        or prepared.certificate_id != sha256_json(prepared.certificate)
        or prepared.pointer.get("trial_id") != prepared.trial_id
        or prepared.certificate.get("trial_id") != prepared.trial_id
        or prepared.certificate.get("active_pointer_id") != prepared.pointer_id
        or prepared.pointer.get("protocol_id") != prepared.trial.get("protocol_id")
        or prepared.certificate.get("protocol_id") != prepared.trial.get("protocol_id")
        or prepared.pointer.get("source_policy_correction_id")
        != prepared.trial.get("source_policy_correction_id")
        or prepared.certificate.get("source_policy_correction_id")
        != prepared.trial.get("source_policy_correction_id")
        or prepared.trial.get("publication") is not False
        or prepared.certificate.get("overall_decision") != "PASS"
        or {
            item.get("gate") for item in prepared.certificate.get("gates", [])
            if isinstance(item, Mapping) and item.get("status") == "PASS"
        } != set(FOUNDATIONAL_GATES + REPLACED_PENDING_GATES)
    ):
        raise IntegrityError("Standard-only lifecycle identities are invalid")
    bindings = prepared.trial.get("bindings")
    if not isinstance(bindings, Mapping) or any(
        sha256_file(root / str(path)) != digest for path, digest in bindings.items()
    ):
        raise IntegrityError("Standard-only lifecycle binding changed after preparation")
    registry = TRIAL_REGISTRY_ROOT / f"{prepared.trial_id}.json"
    event = TRIAL_EVENT_ROOT / f"{prepared.trial_id}.json"
    certificate = CERTIFICATE_ROOT / f"{prepared.trial_id}.json"
    destinations = (registry, event, certificate, ACTIVE_POINTER_PATH)
    if any((root / path).exists() for path in destinations):
        raise IntegrityError("Standard-only lifecycle publication is create-only")
    for path in destinations:
        (root / path).parent.mkdir(parents=True, exist_ok=True)
    with (root / registry).open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.trial,
            "state": "REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
            "trial_id": prepared.trial_id,
        }) + b"\n")
    with (root / event).open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_standard_only_trial_event/1.0.0",
            "event_type": "DECLARED",
            "trial_id": prepared.trial_id,
            "source_row_access": False,
            "model_fit": False,
            "prediction_generation": False,
            "historical_evaluation": False,
            "holdout_or_forward_access": False,
        }) + b"\n")
    with (root / certificate).open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.certificate,
            "state": "PUBLISHED_PREEXECUTION_PASS",
            "certificate_id": prepared.certificate_id,
        }) + b"\n")
    with (root / ACTIVE_POINTER_PATH).open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.pointer,
            "state": "ACTIVE_REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
            "pointer_id": prepared.pointer_id,
        }) + b"\n")
    return {
        "trial_id": prepared.trial_id,
        "registry_path": registry.as_posix(),
        "event_path": event.as_posix(),
        "certificate_path": certificate.as_posix(),
        "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
    }


def load_prepared_standard_only_certificate(*, root: Path) -> dict[str, object]:
    prepared = prepare_standard_only_lifecycle(root=root)
    expected = {**prepared.certificate, "certificate_id": prepared.certificate_id}
    certificate = _object(root / PREPARED_CERTIFICATE_PATH)
    if certificate != expected:
        raise IntegrityError("prepared Standard-only preexecution certificate drifted")
    return certificate
