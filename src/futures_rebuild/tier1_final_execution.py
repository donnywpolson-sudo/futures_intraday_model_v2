"""Authorization, execution, and durable unpublished staging for the final trial."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from . import tier1_bracket_v5 as v5
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment
from .tier1_bracket_v10_execution import V10ExecutionResult, _evidence_payloads_v10
from .tier1_final_pipeline import run_final_trial_pipeline
from .tier1_final_protocol import load_final_effective_contract, load_final_trial_protocol
from .tier1_final_unpublished_evidence import (
    stage_unpublished_evidence, verify_unpublished_evidence,
)
from .tier1_standard_only_execution import resolve_authorized_source_streams


PLAN_PATH = Path("configs/tier1_final_historical_execution_plan.json")
ACTIVE_POINTER_PATH = Path("configs/active_tier1_trial.json")
REGISTRY_ROOT = Path("state/trial_registry/tier1_final_trial")
CERTIFICATE_ROOT = Path("state/preexecution_certificates/tier1_final_trial")
OUTPUT_ROOT = Path("state/tier1_final_unpublished_evidence")
OPERATION = "EXECUTE_FINAL_TIER1_HISTORICAL_SCREEN_AND_STAGE_UNPUBLISHED_EVIDENCE"


@dataclass(frozen=True)
class FinalExecutionResult:
    result: v5.V5PipelineResult
    source_integrity_audit: Mapping[str, Mapping[str, object]]
    authorization_claim_path: Path
    unpublished_bundle: Mapping[str, str]


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid final execution artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("final execution artifact is not an object")
    return value


def load_final_execution_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    protocol = load_final_trial_protocol(root=root)
    predecessor = load_final_effective_contract(root=root)
    source = predecessor.get("source")
    forbidden = plan.get("forbidden_actions")
    scope = plan.get("source_scope")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version")
        != "tier1_final_historical_execution_plan/1.0.0"
        or plan.get("state") != "PREPARED_REQUIRES_SEPARATE_EXECUTION_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("protocol_id") != protocol.get("protocol_id")
        or not isinstance(source, Mapping)
        or plan.get("selected_sources_id") != source.get("selected_sources_id")
        or plan.get("calendar_release_id") != source.get("calendar_release_id")
        or plan.get("execution_mode")
        != "CREATE_ONLY_SEALED_UNPUBLISHED_EVIDENCE_AND_TERMINAL_SUMMARY"
        or plan.get("maximum_host_runtime_seconds") != 900
        or plan.get("estimated_external_cost_usd") != "0"
        or plan.get("output_root") != OUTPUT_ROOT.as_posix()
        or plan.get("success_requires_verified_unpublished_bundle") is not True
        or not isinstance(scope, Mapping)
        or scope.get("markets") != ["6E", "CL", "ES", "ZN"]
        or scope.get("years") != [2018, 2019, 2020, 2021, 2022]
        or scope.get("selected_release_count") != 20
        or not isinstance(forbidden, Mapping) or not forbidden
        or not all(value is True for value in forbidden.values())
    ):
        raise UnauthorizedOperation("final historical execution plan drifted")
    return plan


def load_final_registered_context(
    *, root: Path, plan: Mapping[str, object],
) -> tuple[str, dict[str, object], dict[str, object], dict[str, object]]:
    pointer = _object(root / ACTIVE_POINTER_PATH)
    trial_id = pointer.get("trial_id")
    if not isinstance(trial_id, str) or len(trial_id) != 64:
        raise UnauthorizedOperation("final active pointer is unavailable")
    registry_path = root / REGISTRY_ROOT / f"{trial_id}.json"
    certificate_path = root / CERTIFICATE_ROOT / f"{trial_id}.json"
    registry = _object(registry_path)
    certificate = _object(certificate_path)
    bindings = registry.get("bindings")
    if (
        pointer.get("state") != "ACTIVE_REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or pointer.get("trial_registry_path") != registry_path.relative_to(root).as_posix()
        or pointer.get("preexecution_certificate_path")
        != certificate_path.relative_to(root).as_posix()
        or registry.get("trial_id") != trial_id
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or registry.get("source_row_access") is not False
        or registry.get("historical_evaluation") is not False
        or registry.get("holdout_or_forward_access") is not False
        or registry.get("protocol_id") != plan.get("protocol_id")
        or registry.get("selected_sources_id") != plan.get("selected_sources_id")
        or registry.get("calendar_release_id") != plan.get("calendar_release_id")
        or certificate.get("trial_id") != trial_id
        or certificate.get("state") != "PUBLISHED_PREEXECUTION_PASS"
        or certificate.get("overall_decision") != "PASS"
        or certificate.get("active_pointer_id") != pointer.get("pointer_id")
        or not isinstance(bindings, Mapping)
        or bindings.get(PLAN_PATH.as_posix()) != sha256_file(root / PLAN_PATH)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise UnauthorizedOperation("final registered execution context drifted")
    return trial_id, pointer, registry, certificate


def _required_scope(
    *, trial_id: str, plan: Mapping[str, object], output_root: Path,
) -> dict[str, str]:
    return {
        "trial_id": trial_id,
        "selected_sources_id": str(plan["selected_sources_id"]),
        "output_root": output_root.as_posix(),
        "unpublished_evidence_staging": "true",
        "holdout_or_forward_access": "false",
        "provider_access": "false",
        "publication": "false",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": str(plan["plan_sha256"]),
    }


def claim_final_execution(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, plan: Mapping[str, object], output_root: Path,
) -> Path:
    boundary.assert_active_path(output_root.absolute(), purpose="final unpublished output root")
    receipt.verify(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
    )
    if (
        dict(receipt.scope) != _required_scope(
            trial_id=trial_id, plan=plan, output_root=output_root,
        )
        or not receipt.single_use or not receipt.externally_authorized
    ):
        raise UnauthorizedOperation("final execution requires an exact single-use receipt")
    claim = root / "state/authorization_uses" / f"{receipt.receipt_id}.json"
    boundary.assert_active_path(
        claim.absolute(), purpose="final execution authorization use",
        subtree="state/authorization_uses",
    )
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        with claim.open("xb") as stream:
            stream.write(canonical_bytes({
                "schema_version": "tier1_final_execution_authorization_use/1.0.0",
                "receipt_id": receipt.receipt_id,
                "trial_id": trial_id,
                "selected_sources_id": plan["selected_sources_id"],
                "approval_plan_id": plan["plan_id"],
                "approval_plan_sha256": plan["plan_sha256"],
                "unpublished_evidence_staging": True,
                "holdout_or_forward_access": False,
                "provider_access": False,
                "publication": False,
            }) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("final execution receipt was already consumed") from exc
    return claim


def execute_authorized_final(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> FinalExecutionResult:
    plan = load_final_execution_plan(root=root)
    plan = {**plan, "plan_sha256": sha256_file(root / PLAN_PATH)}
    trial_id, _, registry, _ = load_final_registered_context(root=root, plan=plan)
    require_locked_repository_environment(root)
    output_root = root / OUTPUT_ROOT
    claim = claim_final_execution(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        plan=plan, output_root=output_root,
    )
    streams, audits = resolve_authorized_source_streams(
        root=root, boundary=boundary,
        selected_sources_id=str(registry["selected_sources_id"]),
    )
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(registry["calendar_release_id"]),
    )
    runtime = dict(v5.prepare_runtime_receipt_v5(root=root, trial_id=trial_id))
    runtime.pop("runtime_receipt_id", None)
    runtime.update({
        "schema_version": "tier1_final_runtime_receipt/1.0.0",
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_claim_path": claim.relative_to(root).as_posix(),
        "authorization_claim_sha256": sha256_file(claim),
        "execution_plan_id": plan["plan_id"],
        "execution_plan_sha256": plan["plan_sha256"],
    })
    runtime["runtime_receipt_id"] = sha256_json(runtime)
    result = run_final_trial_pipeline(
        streams=streams,
        census=v5.build_expected_census_from_calendar(sessions=sessions),
        contract=load_final_effective_contract(root=root),
        trial_id=trial_id, runtime_receipt=runtime,
    )
    audit_payload = {
        f"{market}/{year}": audit.as_dict()
        for (market, year), audit in sorted(audits.items())
    }
    wrapped = V10ExecutionResult(result, audit_payload)
    bundle = stage_unpublished_evidence(
        root=root, boundary=boundary, output_root=output_root,
        trial_id=trial_id, authorization_receipt_id=receipt.receipt_id,
        payloads=_evidence_payloads_v10(wrapped),
    )
    verify_unpublished_evidence(
        root=root, bundle_path=Path(bundle["bundle_path"]),
    )
    return FinalExecutionResult(result, audit_payload, claim, bundle)
