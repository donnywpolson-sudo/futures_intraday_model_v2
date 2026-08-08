"""Validation for the final unversioned Tier 1 protocol and invalid closure."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError
from .tier1_standard_only_protocol import load_standard_only_protocol


PROTOCOL_PATH = Path("configs/tier1_final_trial_protocol.json")
CLOSURE_PATH = Path("configs/tier1_standard_only_invalid_closure_preparation.json")


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid final Tier 1 artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("final Tier 1 artifact is not an object")
    return value


def load_invalid_closure_preparation(*, root: Path) -> dict[str, object]:
    closure = _object(root / CLOSURE_PATH)
    core = dict(closure)
    record_id = core.pop("record_id", None)
    observations = closure.get("unpublished_observations")
    boundary = closure.get("research_use_boundary")
    bindings = closure.get("preserved_bindings")
    if (
        record_id != sha256_json(core)
        or closure.get("schema_version")
        != "tier1_standard_only_invalid_closure_preparation/1.0.0"
        or closure.get("state") != "PREPARED_NOT_PUBLISHED"
        or closure.get("disposition")
        != "INVALID_CERTIFICATION_AND_COMPLETENESS_SEMANTICS_DEFECTS"
        or closure.get("official_execution_classification")
        != "INCONCLUSIVE_DATA_OR_COVERAGE"
        or not isinstance(observations, Mapping)
        or observations.get("performance_inference_executed") is not False
        or observations.get("complete_evidence_bundle_persisted") is not False
        or not isinstance(boundary, Mapping)
        or boundary.get("observations_are_diagnosis_only") is not True
        or boundary.get("same_trial_may_be_tuned_or_reinterpreted") is not False
        or any(boundary.get(field) is not False for field in (
            "closure_publication_authorized", "replacement_registration_authorized",
            "historical_rerun_authorized",
        ))
        or not isinstance(bindings, Mapping) or not bindings
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("invalid-closure preparation is incomplete or drifted")
    return closure


def load_final_trial_protocol(*, root: Path) -> dict[str, object]:
    predecessor = load_standard_only_protocol(root=root)
    closure = load_invalid_closure_preparation(root=root)
    protocol = _object(root / PROTOCOL_PATH)
    core = dict(protocol)
    protocol_id = core.pop("protocol_id", None)
    lineage = protocol.get("lineage")
    inherited = protocol.get("inherited_research_specification")
    corrections = protocol.get("decision_validity_corrections")
    staging = protocol.get("durable_unpublished_evidence")
    authority = protocol.get("execution_authority")
    bindings = protocol.get("bindings")
    if (
        protocol_id != sha256_json(core)
        or protocol.get("schema_version") != "tier1_final_trial_protocol/1.0.0"
        or protocol.get("state") != "PREPARED_NOT_REGISTERED"
        or protocol.get("classification")
        != "ONE_FINAL_UNVERSIONED_PREREGISTERED_TIER1_HISTORICAL_SCREEN"
        or not isinstance(lineage, Mapping)
        or lineage.get("predecessor_trial_id") != closure.get("trial_id")
        or lineage.get("predecessor_protocol_id") != predecessor.get("protocol_id")
        or lineage.get("invalid_closure_preparation_id") != closure.get("record_id")
        or lineage.get("predecessor_outcomes_used_for_parameter_selection") is not False
        or lineage.get("predecessor_outcomes_used_for_decision_validity_diagnosis_only") is not True
        or lineage.get("new_numbered_version_created") is not False
        or lineage.get("further_successor_creation_frozen") is not True
        or not isinstance(inherited, Mapping)
        or inherited.get("parameter_changes") != []
        or not isinstance(corrections, Mapping)
        or corrections.get("scenario_risk_cap_failure")
        != "RISK_CAP_REJECTION_POLICY_ABSTENTION_NOT_MISSING_PRICE_PATH"
        or corrections.get("missing_data_may_create_promotion") is not False
        or corrections.get("missing_data_may_create_rejection") is not False
        or not isinstance(staging, Mapping)
        or staging.get("required") is not True
        or staging.get("publication") is not False
        or staging.get("process_success_forbidden_before_bundle_verifies") is not True
        or not isinstance(authority, Mapping)
        or authority.get("maximum_host_runtime_seconds") != 900
        or any(authority.get(field) is not False for field in (
            "historical_row_access_authorized", "model_fit_prediction_evaluation_authorized",
            "unpublished_evidence_staging_authorized", "provider_or_network_access",
            "credential_access", "holdout_or_forward_access", "publication",
            "active_data_mutation", "stage_commit_push", "trading",
        ))
        or not isinstance(bindings, Mapping) or not bindings
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("final Tier 1 protocol is incomplete or drifted")
    return protocol


def load_final_effective_contract(*, root: Path) -> dict[str, object]:
    """Return the unchanged research parameters after validating final corrections."""

    load_final_trial_protocol(root=root)
    return load_standard_only_protocol(root=root)
