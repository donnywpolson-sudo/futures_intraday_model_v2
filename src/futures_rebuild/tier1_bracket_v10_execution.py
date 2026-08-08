"""Authorization, execution, and immutable evidence controls for V10."""

from __future__ import annotations

import json
import tempfile
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment
from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v6 as v6
from . import tier1_bracket_v10 as v10
from .tier1_bracket_v10_decision_validity import load_decision_validity_contract_v10
from .tier1_bracket_v10_pipeline import run_v10_pipeline


V10_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_successor_v10")
EXECUTION_OPERATION = "EXECUTE_TIER1_BRACKET_SUCCESSOR_V10_HISTORICAL_SCREEN"
PUBLICATION_OPERATION = "PUBLISH_TIER1_BRACKET_SUCCESSOR_V10_EVIDENCE"


def _load(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid V10 execution artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"V10 execution artifact is not an object: {path.as_posix()}")
    return value


def verify_historical_operation_receipt_v10(
    *, boundary: RepoBoundary, receipt: OperationReceipt, trial_id: str,
    source_binding_id: str, output_root: Path, plan_id: str, plan_sha256: str,
) -> str:
    if not all(v5._hex64(value) for value in (trial_id, source_binding_id, plan_id, plan_sha256)):
        raise UnauthorizedOperation("V10 historical receipt scope is invalid")
    boundary.assert_active_path(output_root.absolute(), purpose="V10 historical output root")
    required = {
        "trial_id": trial_id,
        "source_binding_id": source_binding_id,
        "output_root": output_root.as_posix(),
        "holdout_or_forward_access": "false",
        "provider_access": "false",
        "publication": "false",
        "approval_command": EXECUTION_OPERATION,
        "approval_plan_id": plan_id,
        "approval_plan_sha256": plan_sha256,
    }
    receipt.verify(
        boundary, operation=EXECUTION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
    )
    if dict(receipt.scope) != required or not receipt.single_use or not receipt.externally_authorized:
        raise UnauthorizedOperation("V10 execution requires an exact single-use external receipt")
    return receipt.receipt_id


def claim_historical_operation_receipt_v10(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_binding_id: str, output_root: Path,
    plan_id: str, plan_sha256: str,
) -> Path:
    receipt_id = verify_historical_operation_receipt_v10(
        boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_binding_id=source_binding_id, output_root=output_root,
        plan_id=plan_id, plan_sha256=plan_sha256,
    )
    claim = root / "state/authorization_uses" / f"{receipt_id}.json"
    boundary.assert_active_path(
        claim.absolute(), purpose="V10 authorization use",
        subtree="state/authorization_uses",
    )
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        with claim.open("xb") as stream:
            stream.write(canonical_bytes({
                "schema_version": "tier1_bracket_v10_authorization_use/1.0.0",
                "receipt_id": receipt_id, "trial_id": trial_id,
                "source_binding_id": source_binding_id,
                "output_root": output_root.as_posix(),
                "approval_plan_id": plan_id,
                "approval_plan_sha256": plan_sha256,
                "holdout_or_forward_access": False,
                "provider_access": False, "publication": False,
            }) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("V10 historical receipt was already consumed") from exc
    return claim


def authorized_source_streams_v10(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path], output_root: Path,
    plan_id: str, plan_sha256: str,
) -> tuple[
    Mapping[tuple[str, int], Iterator[v5.V5SourceRecord]],
    Mapping[tuple[str, int], v10.SourceIntegrityAuditV10], Path,
]:
    if any(year == 2025 for _, year in source_paths):
        raise UnauthorizedOperation("2025 holdout path is rejected before registry or file open")
    registry = _load(root / V10_REGISTRY_ROOT / f"{trial_id}.json")
    bindings = registry.get("bindings")
    if (
        registry.get("trial_id") != trial_id
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or registry.get("holdout_or_forward_access") is not False
        or not isinstance(bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
    ):
        raise UnauthorizedOperation("registered V10 declaration is unavailable or drifted")
    require_locked_repository_environment(root)
    raw = registry.get("source_bindings")
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise IntegrityError("registered V10 source bindings are absent")
    binding_id = v5.source_binding_id_from_metadata_v5(raw)
    expected = {
        (str(item["market"]), int(item["year"])): str(item["source_parquet_sha256"])
        for item in raw
    }
    if registry.get("source_binding_id") != binding_id or set(source_paths) != set(expected):
        raise IntegrityError("V10 source binding or path map is inconsistent")
    claim = claim_historical_operation_receipt_v10(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_binding_id=binding_id, output_root=output_root,
        plan_id=plan_id, plan_sha256=plan_sha256,
    )
    for key, path in source_paths.items():
        if key[1] == 2025:
            raise UnauthorizedOperation("2025 holdout path is rejected before open")
        if sha256_file(path) != expected[key]:
            raise IntegrityError("V10 source bytes differ from registration")
    audits = {key: v10.SourceIntegrityAuditV10(key[0]) for key in sorted(source_paths)}
    streams = {
        key: v10.iter_source_records_from_parquet_v10(
            market=key[0], path=source_paths[key], audit=audits[key],
        )
        for key in sorted(source_paths)
    }
    return streams, audits, claim


@dataclass(frozen=True)
class V10ExecutionResult:
    base: v5.V5PipelineResult
    source_integrity_audit: Mapping[str, Mapping[str, object]]


def execute_authorized_v10(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path], output_root: Path,
    plan_id: str, plan_sha256: str,
) -> V10ExecutionResult:
    """Execute in memory only; evidence publication is a separate receipt."""

    streams, audits, claim = authorized_source_streams_v10(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_paths=source_paths, output_root=output_root,
        plan_id=plan_id, plan_sha256=plan_sha256,
    )
    registry = _load(root / V10_REGISTRY_ROOT / f"{trial_id}.json")
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(registry["calendar_release_id"]),
    )
    inherited, _ = load_decision_validity_contract_v10(root=root)
    runtime = dict(v5.prepare_runtime_receipt_v5(root=root, trial_id=trial_id))
    runtime.pop("runtime_receipt_id", None)
    runtime.update({
        "schema_version": "tier1_bracket_successor_v10_runtime_receipt/1.0.0",
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_claim_path": claim.relative_to(root).as_posix(),
        "authorization_claim_sha256": sha256_file(claim),
        "execution_plan_id": plan_id,
        "execution_plan_sha256": plan_sha256,
    })
    runtime["runtime_receipt_id"] = sha256_json(runtime)
    base = run_v10_pipeline(
        streams=streams,
        census=v5.build_expected_census_from_calendar(sessions=sessions),
        contract=inherited, trial_id=trial_id, runtime_receipt=runtime,
    )
    audit_payload = {
        f"{market}/{year}": audit.as_dict()
        for (market, year), audit in sorted(audits.items())
    }
    return V10ExecutionResult(base, audit_payload)


def _evidence_payloads_v10(result: V10ExecutionResult) -> dict[str, object]:
    base = result.base.evidence
    raw = {
        "model": base.model,
        "predictions": base.predictions,
        "opportunity_ledger": base.opportunity_ledger,
        "fills": base.fills,
        "continuous_equity_marks": base.continuous_equity_marks,
        "segmented_metrics": base.segmented_metrics,
        "inference": base.inference,
        "decision": base.decision,
        "runtime_receipt": base.runtime_receipt,
        "source_integrity_audit": result.source_integrity_audit,
    }
    safe = v5._json_safe(raw)
    if not isinstance(safe, dict):
        raise IntegrityError("V10 evidence cannot be canonicalized")
    return safe


def build_evidence_manifest_v10(
    *, trial_id: str, result: V10ExecutionResult,
) -> dict[str, object]:
    if not v5._hex64(trial_id):
        raise IntegrityError("V10 evidence trial identity is invalid")
    base = result.base.evidence
    runtime = base.runtime_receipt
    if (
        not base.predictions or not base.opportunity_ledger
        or not isinstance(runtime, Mapping)
        or not v5._hex64(runtime.get("runtime_receipt_id"))
        or not v5._hex64(runtime.get("dependency_lock_receipt_id"))
        or not v5._hex64(runtime.get("authorization_receipt_id"))
        or not v5._hex64(runtime.get("authorization_claim_sha256"))
        or not v5._hex64(runtime.get("execution_plan_id"))
        or not v5._hex64(runtime.get("execution_plan_sha256"))
    ):
        raise IntegrityError("V10 evidence lacks predictions, ledger, runtime, or authority")
    payloads = _evidence_payloads_v10(result)
    files = {
        f"{name}.json": sha256_bytes(canonical_bytes({"payload": payload}) + b"\n")
        for name, payload in sorted(payloads.items())
    }
    core = {
        "schema_version": "tier1_bracket_successor_v10_evidence_manifest/1.0.0",
        "trial_id": trial_id, "files": files,
    }
    return {**core, "manifest_id": sha256_json(core)}


def _claim_publication_receipt_v10(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, manifest_id: str, output_root: Path,
    plan_id: str, plan_sha256: str,
) -> Path:
    boundary.assert_active_path(output_root.absolute(), purpose="V10 evidence output root")
    required = {
        "trial_id": trial_id, "manifest_id": manifest_id,
        "output_root": output_root.as_posix(), "publication": "true",
        "holdout_or_forward_access": "false", "provider_access": "false",
        "approval_command": PUBLICATION_OPERATION,
        "approval_plan_id": plan_id, "approval_plan_sha256": plan_sha256,
    }
    receipt.verify(
        boundary, operation=PUBLICATION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
    )
    if (
        dict(receipt.scope) != required or not receipt.single_use
        or not receipt.externally_authorized
    ):
        raise UnauthorizedOperation("V10 publication requires an exact single-use receipt")
    claim = root / "state/authorization_uses" / f"{receipt.receipt_id}.json"
    boundary.assert_active_path(
        claim.absolute(), purpose="V10 evidence publication authorization use",
        subtree="state/authorization_uses",
    )
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        with claim.open("xb") as stream:
            stream.write(canonical_bytes({
                "schema_version": "tier1_bracket_v10_publication_authorization_use/1.0.0",
                "receipt_id": receipt.receipt_id, "trial_id": trial_id,
                "manifest_id": manifest_id, "output_root": output_root.as_posix(),
                "approval_plan_id": plan_id, "approval_plan_sha256": plan_sha256,
                "publication": True, "holdout_or_forward_access": False,
                "provider_access": False,
            }) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("V10 publication receipt was already consumed") from exc
    return claim


def persist_evidence_bundle_v10(
    *, root: Path, boundary: RepoBoundary, output_root: Path, trial_id: str,
    result: V10ExecutionResult, receipt: OperationReceipt,
    plan_id: str, plan_sha256: str,
) -> dict[str, str]:
    manifest = build_evidence_manifest_v10(trial_id=trial_id, result=result)
    _claim_publication_receipt_v10(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        manifest_id=str(manifest["manifest_id"]), output_root=output_root,
        plan_id=plan_id, plan_sha256=plan_sha256,
    )
    destination = output_root / trial_id / str(manifest["manifest_id"])
    if destination.exists():
        raise IntegrityError("V10 evidence publication is create-only")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(
        prefix=f".staging-{str(manifest['manifest_id'])[:12]}-",
        dir=destination.parent,
    ))
    payloads = _evidence_payloads_v10(result)
    for filename, expected_hash in manifest["files"].items():
        name = filename.removesuffix(".json")
        path = staging / filename
        with path.open("xb") as stream:
            stream.write(canonical_bytes({"payload": payloads[name]}) + b"\n")
        if sha256_file(path) != expected_hash:
            raise IntegrityError("persisted V10 evidence hash mismatch")
    with (staging / "manifest.json").open("xb") as stream:
        stream.write(canonical_bytes(manifest) + b"\n")
    if destination.exists():
        raise IntegrityError("V10 evidence destination appeared during publication")
    staging.replace(destination)
    final_manifest = destination / "manifest.json"
    return {
        "manifest_id": str(manifest["manifest_id"]),
        "manifest_path": final_manifest.as_posix(),
        "manifest_sha256": sha256_file(final_manifest),
    }
