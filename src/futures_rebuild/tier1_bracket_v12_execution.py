"""Exact authorization and immutable evidence controls for V12."""

from __future__ import annotations

import json
import tempfile
from collections.abc import Iterator, Mapping
from pathlib import Path

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment
from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v10 as v10
from .tier1_bracket_v10_execution import V10ExecutionResult, _evidence_payloads_v10
from .tier1_bracket_v12 import load_v12_contract
from .tier1_bracket_v12_pipeline import run_v12_pipeline


V12_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_successor_v12")
EXECUTION_OPERATION_V12 = "EXECUTE_TIER1_BRACKET_SUCCESSOR_V12_HISTORICAL_SCREEN"
PUBLICATION_OPERATION_V12 = "PUBLISH_TIER1_BRACKET_SUCCESSOR_V12_EVIDENCE"


def _load(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid V12 execution artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("V12 execution artifact is not an object")
    return value


def _verify_receipt_v12(
    *, boundary: RepoBoundary, receipt: OperationReceipt, operation: str,
    required: Mapping[str, str],
) -> str:
    receipt.verify(
        boundary, operation=operation,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
    )
    if (
        dict(receipt.scope) != dict(required) or not receipt.single_use
        or not receipt.externally_authorized
    ):
        raise UnauthorizedOperation("V12 operation requires an exact single-use receipt")
    return receipt.receipt_id


def claim_historical_operation_receipt_v12(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_binding_id: str, output_root: Path,
    plan_id: str, plan_sha256: str,
) -> Path:
    if not all(
        v5._hex64(value)
        for value in (trial_id, source_binding_id, plan_id, plan_sha256)
    ):
        raise UnauthorizedOperation("V12 historical receipt scope is invalid")
    boundary.assert_active_path(output_root.absolute(), purpose="V12 historical output root")
    required = {
        "trial_id": trial_id, "source_binding_id": source_binding_id,
        "output_root": output_root.as_posix(),
        "holdout_or_forward_access": "false", "provider_access": "false",
        "publication": "false", "approval_command": EXECUTION_OPERATION_V12,
        "approval_plan_id": plan_id, "approval_plan_sha256": plan_sha256,
    }
    receipt_id = _verify_receipt_v12(
        boundary=boundary, receipt=receipt, operation=EXECUTION_OPERATION_V12,
        required=required,
    )
    claim = root / "state/authorization_uses" / f"{receipt_id}.json"
    boundary.assert_active_path(
        claim.absolute(), purpose="V12 authorization use",
        subtree="state/authorization_uses",
    )
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        with claim.open("xb") as stream:
            stream.write(canonical_bytes({
                "schema_version": "tier1_bracket_v12_authorization_use/1.0.0",
                "receipt_id": receipt_id, "trial_id": trial_id,
                "source_binding_id": source_binding_id,
                "output_root": output_root.as_posix(),
                "approval_plan_id": plan_id,
                "approval_plan_sha256": plan_sha256,
                "holdout_or_forward_access": False, "provider_access": False,
                "publication": False,
            }) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("V12 historical receipt was already consumed") from exc
    return claim


def authorized_source_streams_v12(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path], output_root: Path,
    plan_id: str, plan_sha256: str,
) -> tuple[
    Mapping[tuple[str, int], Iterator[v5.V5SourceRecord]],
    Mapping[tuple[str, int], v10.SourceIntegrityAuditV10], Path,
]:
    if any(year == 2025 for _, year in source_paths):
        raise UnauthorizedOperation("2025 holdout path is rejected before registry or file open")
    registry = _load(root / V12_REGISTRY_ROOT / f"{trial_id}.json")
    bindings = registry.get("bindings")
    if (
        registry.get("trial_id") != trial_id
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or registry.get("holdout_or_forward_access") is not False
        or not isinstance(bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
    ):
        raise UnauthorizedOperation("registered V12 declaration is unavailable or drifted")
    require_locked_repository_environment(root)
    raw = registry.get("source_bindings")
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise IntegrityError("registered V12 source bindings are absent")
    binding_id = v5.source_binding_id_from_metadata_v5(raw)
    expected = {
        (str(item["market"]), int(item["year"])): str(item["source_parquet_sha256"])
        for item in raw
    }
    if registry.get("source_binding_id") != binding_id or set(source_paths) != set(expected):
        raise IntegrityError("V12 source binding or path map is inconsistent")
    claim = claim_historical_operation_receipt_v12(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_binding_id=binding_id, output_root=output_root,
        plan_id=plan_id, plan_sha256=plan_sha256,
    )
    for key, path in source_paths.items():
        if sha256_file(path) != expected[key]:
            raise IntegrityError("V12 source bytes differ from registration")
    audits = {key: v10.SourceIntegrityAuditV10(key[0]) for key in sorted(source_paths)}
    streams = {
        key: v10.iter_source_records_from_parquet_v10(
            market=key[0], path=source_paths[key], audit=audits[key],
        )
        for key in sorted(source_paths)
    }
    return streams, audits, claim


def execute_authorized_v12(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path], output_root: Path,
    plan_id: str, plan_sha256: str,
) -> V10ExecutionResult:
    streams, audits, claim = authorized_source_streams_v12(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_paths=source_paths, output_root=output_root,
        plan_id=plan_id, plan_sha256=plan_sha256,
    )
    registry = _load(root / V12_REGISTRY_ROOT / f"{trial_id}.json")
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(registry["calendar_release_id"]),
    )
    inherited, _ = load_v12_contract(root=root)
    runtime = dict(v5.prepare_runtime_receipt_v5(root=root, trial_id=trial_id))
    runtime.pop("runtime_receipt_id", None)
    runtime.update({
        "schema_version": "tier1_bracket_successor_v12_runtime_receipt/1.0.0",
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_claim_path": claim.relative_to(root).as_posix(),
        "authorization_claim_sha256": sha256_file(claim),
        "execution_plan_id": plan_id, "execution_plan_sha256": plan_sha256,
    })
    runtime["runtime_receipt_id"] = sha256_json(runtime)
    base = run_v12_pipeline(
        streams=streams,
        census=v5.build_expected_census_from_calendar(sessions=sessions),
        contract=inherited, trial_id=trial_id, runtime_receipt=runtime,
    )
    return V10ExecutionResult(base, {
        f"{market}/{year}": audit.as_dict()
        for (market, year), audit in sorted(audits.items())
    })


def build_evidence_manifest_v12(
    *, trial_id: str, result: V10ExecutionResult,
) -> dict[str, object]:
    runtime = result.base.evidence.runtime_receipt
    if (
        not v5._hex64(trial_id) or not result.base.evidence.predictions
        or not result.base.evidence.opportunity_ledger
        or not isinstance(runtime, Mapping)
        or any(not v5._hex64(runtime.get(key)) for key in (
            "runtime_receipt_id", "dependency_lock_receipt_id",
            "authorization_receipt_id", "authorization_claim_sha256",
            "execution_plan_id", "execution_plan_sha256",
        ))
    ):
        raise IntegrityError("V12 evidence lacks identity, runtime, or authorization")
    payloads = _evidence_payloads_v10(result)
    files = {
        f"{name}.json": sha256_bytes(canonical_bytes({"payload": payload}) + b"\n")
        for name, payload in sorted(payloads.items())
    }
    core = {
        "schema_version": "tier1_bracket_successor_v12_evidence_manifest/1.0.0",
        "trial_id": trial_id, "files": files,
    }
    return {**core, "manifest_id": sha256_json(core)}


def persist_evidence_bundle_v12(
    *, root: Path, boundary: RepoBoundary, output_root: Path, trial_id: str,
    result: V10ExecutionResult, receipt: OperationReceipt,
    plan_id: str, plan_sha256: str,
) -> dict[str, str]:
    manifest = build_evidence_manifest_v12(trial_id=trial_id, result=result)
    boundary.assert_active_path(output_root.absolute(), purpose="V12 evidence output root")
    required = {
        "trial_id": trial_id, "manifest_id": str(manifest["manifest_id"]),
        "output_root": output_root.as_posix(), "publication": "true",
        "holdout_or_forward_access": "false", "provider_access": "false",
        "approval_command": PUBLICATION_OPERATION_V12,
        "approval_plan_id": plan_id, "approval_plan_sha256": plan_sha256,
    }
    receipt_id = _verify_receipt_v12(
        boundary=boundary, receipt=receipt, operation=PUBLICATION_OPERATION_V12,
        required=required,
    )
    claim = root / "state/authorization_uses" / f"{receipt_id}.json"
    boundary.assert_active_path(
        claim.absolute(), purpose="V12 publication authorization use",
        subtree="state/authorization_uses",
    )
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        with claim.open("xb") as stream:
            stream.write(canonical_bytes({
                "schema_version": "tier1_bracket_v12_publication_authorization_use/1.0.0",
                "receipt_id": receipt_id, "trial_id": trial_id,
                "manifest_id": manifest["manifest_id"],
                "output_root": output_root.as_posix(), "publication": True,
                "approval_plan_id": plan_id,
                "approval_plan_sha256": plan_sha256,
                "holdout_or_forward_access": False, "provider_access": False,
            }) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("V12 publication receipt was already consumed") from exc
    destination = output_root / trial_id / str(manifest["manifest_id"])
    if destination.exists():
        raise IntegrityError("V12 evidence publication is create-only")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(
        prefix=f".staging-{str(manifest['manifest_id'])[:12]}-",
        dir=destination.parent,
    ))
    payloads = _evidence_payloads_v10(result)
    for filename, expected_hash in manifest["files"].items():
        path = staging / filename
        with path.open("xb") as stream:
            stream.write(canonical_bytes({
                "payload": payloads[filename.removesuffix(".json")]
            }) + b"\n")
        if sha256_file(path) != expected_hash:
            raise IntegrityError("persisted V12 evidence hash mismatch")
    with (staging / "manifest.json").open("xb") as stream:
        stream.write(canonical_bytes(manifest) + b"\n")
    if destination.exists():
        raise IntegrityError("V12 evidence destination appeared during publication")
    staging.replace(destination)
    final = destination / "manifest.json"
    return {
        "manifest_id": str(manifest["manifest_id"]),
        "manifest_path": final.as_posix(), "manifest_sha256": sha256_file(final),
    }
