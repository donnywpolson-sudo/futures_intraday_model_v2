"""Transition-stable protocol validation for the authoritative Tier 1 trial."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError
from .tier1_standard_only_protocol import load_standard_only_protocol


FAILED_TRIAL_ID = "72566d6f80073a2c592f7b13436c98a1bb956e6b135bac297aa8f5636fb09541"
PROTOCOL_PATH = Path("configs/tier1_authoritative_trial_protocol.json")
CLOSURE_PATH = Path(
    "configs/tier1_final_pointer_binding_invalid_closure_preparation.json"
)
FAILED_RETIREMENT_ROOT = Path(
    "state/trial_registry/tier1_final_pointer_binding_invalid_retirement"
)
STANDARD_RETIREMENT_PATH = Path(
    "state/trial_registry/tier1_standard_only_invalid_retirement/"
    "20ea50ded16c0a0999bdccf171a577ba5cd4b651eb06ade74bc104dc60522647.json"
)


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid authoritative Tier 1 artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("authoritative Tier 1 artifact is not an object")
    return value


def load_failed_final_closure_preparation(*, root: Path) -> dict[str, object]:
    closure = _object(root / CLOSURE_PATH)
    core = dict(closure)
    record_id = core.pop("record_id", None)
    defect = closure.get("defect")
    execution = closure.get("execution_status")
    rollback = closure.get("activation_rollback")
    boundary = closure.get("research_use_boundary")
    bindings = closure.get("preserved_immutable_bindings")
    if (
        record_id != sha256_json(core)
        or closure.get("schema_version")
        != "tier1_final_pointer_binding_invalid_closure_preparation/1.0.0"
        or closure.get("state") != "PREPARED_NOT_PUBLISHED"
        or closure.get("trial_id") != FAILED_TRIAL_ID
        or closure.get("disposition")
        != "INVALID_PRE_DATA_LIFECYCLE_POINTER_BINDING_DEFECT"
        or not isinstance(defect, Mapping)
        or defect.get("finding") != "PROTOCOL_BOUND_MUTABLE_PREDECESSOR_ACTIVE_POINTER_PATH"
        or not isinstance(execution, Mapping)
        or any(execution.get(field) is not False for field in (
            "historical_rows_opened", "authorization_claim_created", "model_fit",
            "predictions_generated", "historical_evaluation",
            "unpublished_evidence_bundle_created", "holdout_2025_touched",
            "provider_or_network_access", "credential_access", "trading",
        ))
        or not isinstance(rollback, Mapping)
        or rollback.get("performed_fail_closed") is not True
        or rollback.get("mutable_pointer_is_not_a_preservation_binding") is not True
        or not isinstance(boundary, Mapping)
        or boundary.get("failed_trial_may_be_executed") is not False
        or boundary.get("failed_trial_may_be_repaired_in_place") is not False
        or boundary.get("research_parameters_may_change") is not False
        or not isinstance(bindings, Mapping) or not bindings
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
        or "configs/active_tier1_trial.json" in bindings
    ):
        raise IntegrityError("failed final-trial closure preparation drifted")
    return closure


def _validate_optional_published_retirement(
    *, root: Path, closure: Mapping[str, object],
) -> None:
    record_id = str(closure["record_id"])
    path = root / FAILED_RETIREMENT_ROOT / f"{record_id}.json"
    if not path.exists():
        return
    published = _object(path)
    normalized = dict(published)
    normalized.pop("record_id", None)
    normalized["state"] = "PREPARED_NOT_PUBLISHED"
    if (
        published.get("state") != "RETIRED_INVALID_PRE_DATA"
        or published.get("record_id") != record_id
        or published.get("trial_id") != FAILED_TRIAL_ID
        or sha256_json(normalized) != record_id
    ):
        raise IntegrityError("published failed-final retirement is inconsistent")


def load_authoritative_protocol(*, root: Path) -> dict[str, object]:
    predecessor = load_standard_only_protocol(root=root)
    closure = load_failed_final_closure_preparation(root=root)
    _validate_optional_published_retirement(root=root, closure=closure)
    protocol = _object(root / PROTOCOL_PATH)
    core = dict(protocol)
    protocol_id = core.pop("protocol_id", None)
    lineage = protocol.get("lineage")
    inherited = protocol.get("inherited_research_specification")
    lifecycle = protocol.get("lifecycle_validity")
    decision = protocol.get("decision_validity")
    evidence = protocol.get("durable_unpublished_evidence")
    authority = protocol.get("execution_authority")
    bindings = protocol.get("bindings")
    if (
        protocol_id != sha256_json(core)
        or protocol.get("schema_version") != "tier1_authoritative_trial_protocol/1.0.0"
        or protocol.get("state") != "PREPARED_NOT_REGISTERED"
        or protocol.get("classification")
        != "ONE_AUTHORITATIVE_UNVERSIONED_PREREGISTERED_TIER1_HISTORICAL_SCREEN"
        or not isinstance(lineage, Mapping)
        or lineage.get("failed_trial_id") != FAILED_TRIAL_ID
        or lineage.get("failed_trial_closure_preparation_id") != closure.get("record_id")
        or lineage.get("research_predecessor_protocol_id") != predecessor.get("protocol_id")
        or lineage.get("new_numbered_version_created") is not False
        or lineage.get("further_successor_creation_frozen") is not True
        or not isinstance(inherited, Mapping)
        or inherited.get("parameter_changes") != []
        or not isinstance(lifecycle, Mapping)
        or lifecycle.get("mutable_active_pointer_is_protocol_binding") is not False
        or lifecycle.get("predecessor_preservation_source")
        != "IMMUTABLE_RETIREMENT_AND_REGISTRATION_RECORDS_ONLY"
        or lifecycle.get("active_pointer_validation_scope")
        != "REGISTERED_EXECUTION_CONTEXT_ONLY"
        or not isinstance(decision, Mapping)
        or decision.get("scenario_risk_cap_failure")
        != "RISK_CAP_REJECTION_POLICY_ABSTENTION_NOT_MISSING_PRICE_PATH"
        or decision.get("fully_observed_failed_mandatory_candidate_gate")
        != "CONCLUSIVE_REJECTION"
        or not isinstance(evidence, Mapping)
        or evidence.get("required_before_terminal_result") is not True
        or evidence.get("publication") is not False
        or not isinstance(authority, Mapping)
        or any(authority.get(field) is not False for field in (
            "historical_row_access_authorized", "model_fit_prediction_evaluation_authorized",
            "unpublished_evidence_staging_authorized", "provider_or_network_access",
            "credential_access", "holdout_or_forward_access", "publication",
            "active_data_mutation", "stage_commit_push", "trading",
        ))
        or not isinstance(bindings, Mapping) or not bindings
        or "configs/active_tier1_trial.json" in bindings
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
        or sha256_file(root / STANDARD_RETIREMENT_PATH)
        != lineage.get("standard_only_invalid_retirement_sha256")
    ):
        raise IntegrityError("authoritative Tier 1 protocol is incomplete or drifted")
    return protocol


def load_authoritative_effective_contract(*, root: Path) -> dict[str, object]:
    """Return unchanged research parameters after lifecycle validation."""

    load_authoritative_protocol(root=root)
    return load_standard_only_protocol(root=root)
