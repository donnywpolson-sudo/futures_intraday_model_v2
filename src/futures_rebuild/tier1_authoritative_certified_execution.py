"""Execution boundary for the final transition-stable authoritative trial."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

from . import tier1_bracket_v5 as v5
from .boundary import OperationReceipt, RepoBoundary
from .canonical import sha256_file
from .errors import IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment
from .tier1_authoritative_execution import (
    ACTIVE_POINTER_PATH,
    CERTIFICATE_ROOT,
    OPERATION,
    OUTPUT_ROOT,
    PLAN_PATH,
    REGISTRY_ROOT,
    AuthoritativeExecutionResult,
    _prepared_identity,
    claim_authoritative_execution,
    load_authoritative_execution_plan,
)
from .tier1_authoritative_protocol import load_authoritative_effective_contract
from .tier1_bracket_v10_execution import V10ExecutionResult, _evidence_payloads_v10
from .tier1_final_pipeline import run_final_trial_pipeline
from .tier1_final_unpublished_evidence import (
    stage_unpublished_evidence,
    verify_unpublished_evidence,
)
from .tier1_standard_only_execution import resolve_authorized_source_streams


INVALID_TRIAL_ID = (
    "692a52d60cddec45fdbf9b95ffc4a2cb301739d7b701a16d5f1a085b0aa74ad7"
)
INVALID_RETIREMENT_ROOT = Path(
    "state/trial_registry/tier1_authoritative_postactivation_invalid_retirement"
)
INVALID_DISPOSITION = (
    "INVALID_PRE_DATA_POSTACTIVATION_CERTIFICATE_SELECTION_AND_TRANSITION_STATE_DEFECTS"
)


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid certified authoritative artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("certified authoritative artifact is not an object")
    return value


def validate_certified_registered_context_documents(
    *,
    plan: Mapping[str, object],
    pointer: Mapping[str, object],
    registry: Mapping[str, object],
    certificate: Mapping[str, object],
    invalid_retirement: Mapping[str, object],
) -> str:
    """Validate the successor without depending on a mutable predecessor pointer."""

    trial_id = registry.get("trial_id")
    retirement_id = invalid_retirement.get("record_id")
    gates = certificate.get("gates")
    if (
        not isinstance(trial_id, str)
        or len(trial_id) != 64
        or _prepared_identity(
            registry,
            identity_field="trial_id",
            prepared_state="PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        )
        != trial_id
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or registry.get("protocol_id") != plan.get("protocol_id")
        or registry.get("selected_sources_id") != plan.get("selected_sources_id")
        or registry.get("calendar_release_id") != plan.get("calendar_release_id")
        or registry.get("supersedes_invalid_trial_id") != INVALID_TRIAL_ID
        or registry.get("invalid_retirement_id") != retirement_id
        or any(
            registry.get(field) is not False
            for field in (
                "source_row_access",
                "model_fit",
                "prediction_generation",
                "historical_evaluation",
                "unpublished_evidence_staging",
                "publication",
                "holdout_or_forward_access",
                "provider_or_network_access",
                "credential_access",
                "trading",
            )
        )
        or _prepared_identity(
            pointer,
            identity_field="pointer_id",
            prepared_state="PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        )
        != pointer.get("pointer_id")
        or pointer.get("state") != "ACTIVE_REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or pointer.get("trial_id") != trial_id
        or pointer.get("protocol_id") != registry.get("protocol_id")
        or pointer.get("trial_registry_path")
        != (REGISTRY_ROOT / f"{trial_id}.json").as_posix()
        or pointer.get("preexecution_certificate_path")
        != (CERTIFICATE_ROOT / f"{trial_id}.json").as_posix()
        or pointer.get("holdout_or_forward_access") is not False
        or _prepared_identity(
            certificate,
            identity_field="certificate_id",
            prepared_state="PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        )
        != certificate.get("certificate_id")
        or certificate.get("state") != "PUBLISHED_PREEXECUTION_PASS"
        or certificate.get("overall_decision") != "PASS"
        or certificate.get("trial_id") != trial_id
        or certificate.get("active_pointer_id") != pointer.get("pointer_id")
        or certificate.get("invalid_retirement_id") != retirement_id
        or not isinstance(gates, list)
        or len(gates) != 14
        or any(
            not isinstance(gate, Mapping) or gate.get("status") != "PASS"
            for gate in gates
        )
        or not isinstance(retirement_id, str)
        or len(retirement_id) != 64
        or _prepared_identity(
            invalid_retirement,
            identity_field="record_id",
            prepared_state="PREPARED_NOT_PUBLISHED",
        )
        != retirement_id
        or invalid_retirement.get("state") != "RETIRED_INVALID_PRE_DATA"
        or invalid_retirement.get("trial_id") != INVALID_TRIAL_ID
        or invalid_retirement.get("disposition") != INVALID_DISPOSITION
        or invalid_retirement.get("research_parameter_defect") is not False
        or invalid_retirement.get("historical_rows_opened") is not False
    ):
        raise UnauthorizedOperation("certified authoritative context is inconsistent")
    return trial_id


def load_certified_registered_context(
    *, root: Path, plan: Mapping[str, object]
) -> tuple[str, dict[str, object], dict[str, object], dict[str, object]]:
    pointer = _object(root / ACTIVE_POINTER_PATH)
    trial_id = pointer.get("trial_id")
    if not isinstance(trial_id, str) or len(trial_id) != 64:
        raise UnauthorizedOperation("certified authoritative active pointer is unavailable")
    registry = _object(root / REGISTRY_ROOT / f"{trial_id}.json")
    certificate = _object(root / CERTIFICATE_ROOT / f"{trial_id}.json")
    retirement_id = registry.get("invalid_retirement_id")
    if not isinstance(retirement_id, str) or len(retirement_id) != 64:
        raise UnauthorizedOperation("certified invalid-retirement identity is absent")
    retirement = _object(root / INVALID_RETIREMENT_ROOT / f"{retirement_id}.json")
    validate_certified_registered_context_documents(
        plan=plan,
        pointer=pointer,
        registry=registry,
        certificate=certificate,
        invalid_retirement=retirement,
    )
    bindings = registry.get("bindings")
    if (
        not isinstance(bindings, Mapping)
        or bindings.get(PLAN_PATH.as_posix()) != sha256_file(root / PLAN_PATH)
        or "configs/active_tier1_trial.json" in bindings
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise UnauthorizedOperation("certified authoritative bindings drifted")
    return trial_id, pointer, registry, certificate


def execute_authorized_certified_authoritative(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt
) -> AuthoritativeExecutionResult:
    """Execute only after the separately approved exact single-use receipt."""

    plan = load_authoritative_execution_plan(root=root)
    plan = {**plan, "plan_sha256": sha256_file(root / PLAN_PATH)}
    trial_id, _, registry, _ = load_certified_registered_context(root=root, plan=plan)
    require_locked_repository_environment(root)
    output_root = root / OUTPUT_ROOT
    claim = claim_authoritative_execution(
        root=root,
        boundary=boundary,
        receipt=receipt,
        trial_id=trial_id,
        plan=plan,
        output_root=output_root,
    )
    streams, audits = resolve_authorized_source_streams(
        root=root,
        boundary=boundary,
        selected_sources_id=str(registry["selected_sources_id"]),
    )
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(registry["calendar_release_id"]),
    )
    runtime = dict(v5.prepare_runtime_receipt_v5(root=root, trial_id=trial_id))
    runtime.pop("runtime_receipt_id", None)
    runtime.update(
        {
            "schema_version": "tier1_authoritative_runtime_receipt/1.0.0",
            "authorization_receipt_id": receipt.receipt_id,
            "authorization_claim_path": claim.relative_to(root).as_posix(),
            "authorization_claim_sha256": sha256_file(claim),
            "execution_plan_id": plan["plan_id"],
            "execution_plan_sha256": plan["plan_sha256"],
        }
    )
    from .canonical import sha256_json

    runtime["runtime_receipt_id"] = sha256_json(runtime)
    result = run_final_trial_pipeline(
        streams=streams,
        census=v5.build_expected_census_from_calendar(sessions=sessions),
        contract=load_authoritative_effective_contract(root=root),
        trial_id=trial_id,
        runtime_receipt=runtime,
    )
    audit_payload = {
        f"{market}/{year}": audit.as_dict()
        for (market, year), audit in sorted(audits.items())
    }
    wrapped = V10ExecutionResult(result, audit_payload)
    bundle = stage_unpublished_evidence(
        root=root,
        boundary=boundary,
        output_root=output_root,
        trial_id=trial_id,
        authorization_receipt_id=receipt.receipt_id,
        payloads=_evidence_payloads_v10(wrapped),
    )
    verify_unpublished_evidence(root=root, bundle_path=Path(bundle["bundle_path"]))
    return AuthoritativeExecutionResult(result, audit_payload, claim, bundle)
