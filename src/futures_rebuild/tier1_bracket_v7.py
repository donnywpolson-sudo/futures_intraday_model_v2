"""Governance-only successor to the pre-data V6 lifecycle-test defect.

V7 changes no research behavior. It preserves V6's source-integrity adapter
and the inherited V5 strategy, then binds the pytest lifecycle controller and
a state-aware successor test before any V7 historical source access.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path

from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v6 as v6
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment
from .tier1_bracket_v5 import V5SourceRecord, _hex64


V6_TRIAL_ID = "c92c5a6ecfd96a00d0cf89aa02319878b479dad6c6e21b703e54bd55943a8608"
V6_REGISTRY = Path("state/trial_registry/tier1_bracket_successor_v6") / f"{V6_TRIAL_ID}.json"
V6_EVENT = Path("state/trial_events/tier1_bracket_successor_v6") / f"{V6_TRIAL_ID}.json"
V6_RETIREMENT_PREPARATION = Path("configs/tier1_bracket_v6_retirement_preparation.json")
V7_CONTRACT = Path("configs/tier1_bracket_successor_v7.json")
V6_RETIREMENT_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_v6_retirement")
V6_RETIREMENT_EVENT_ROOT = Path("state/trial_events/tier1_bracket_v6_retirement")
V7_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_successor_v7")
V7_EVENT_ROOT = Path("state/trial_events/tier1_bracket_successor_v7")


def _load(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid V7 JSON artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"V7 artifact is not an object: {path.as_posix()}")
    return value


def load_v7_contract(*, root: Path) -> tuple[dict[str, object], dict[str, object]]:
    delta = _load(root / V7_CONTRACT)
    inherited_path = delta.get("inherited_v6_contract_path")
    inherited_hash = delta.get("inherited_v6_contract_sha256")
    governance = delta.get("governance_successor")
    authority = delta.get("authority")
    if (
        delta.get("schema_version") != "tier1_bracket_successor_v7_contract/1.0.0"
        or delta.get("state") != "PREPARED_NOT_REGISTERED"
        or delta.get("supersedes_v6_trial_id") != V6_TRIAL_ID
        or inherited_path != "configs/tier1_bracket_successor_v6.json"
        or not _hex64(inherited_hash)
        or sha256_file(root / str(inherited_path)) != inherited_hash
        or not isinstance(governance, dict)
        or governance.get("test_runner_binding") != "tests/conftest.py_REQUIRED"
        or not isinstance(authority, dict)
        or authority.get("holdout_or_forward_access") is not False
        or authority.get("publication_requires_separate_approval") is not True
    ):
        raise IntegrityError("V7 governance successor contract is incomplete or drifted")
    inherited, _ = v6.load_v6_contract(root=root)
    return inherited, delta


@dataclass(frozen=True)
class PreparedV6RetirementV7:
    record_id: str
    canonical_payload: Mapping[str, object]


@dataclass(frozen=True)
class PreparedV7Registration:
    trial_id: str
    canonical_payload: Mapping[str, object]


def prepare_v6_retirement_v7(*, root: Path) -> PreparedV6RetirementV7:
    preparation = _load(root / V6_RETIREMENT_PREPARATION)
    registry = _load(root / V6_REGISTRY)
    event = _load(root / V6_EVENT)
    bindings = registry.get("bindings")
    if (
        preparation.get("trial_id") != V6_TRIAL_ID
        or preparation.get("disposition")
        != "INVALID_PRE_DATA_TEST_LIFECYCLE_AND_RUNNER_BINDING_DEFECT"
        or preparation.get("historical_source_rows_opened") is not False
        or registry.get("trial_id") != V6_TRIAL_ID
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or event.get("trial_id") != V6_TRIAL_ID
        or not isinstance(bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("V6 retirement preparation or registered bytes are invalid")
    preserved = dict(bindings)
    for path in (V6_REGISTRY, V6_EVENT):
        preserved[path.as_posix()] = sha256_file(root / path)
    core = {
        **preparation,
        "preserved_v6_sha256": dict(sorted(preserved.items())),
    }
    return PreparedV6RetirementV7(sha256_json(core), core)


def prepare_v7_registration(*, root: Path) -> PreparedV7Registration:
    _, delta = load_v7_contract(root=root)
    retirement = prepare_v6_retirement_v7(root=root)
    registry = _load(root / V6_REGISTRY)
    prior_bindings = registry.get("bindings")
    sources = registry.get("source_bindings")
    if not isinstance(prior_bindings, dict) or not isinstance(sources, list):
        raise IntegrityError("V6 lineage is incomplete for V7 registration")
    bindings = dict(prior_bindings)
    new_paths = (
        V6_RETIREMENT_PREPARATION,
        V7_CONTRACT,
        Path("src/futures_rebuild/tier1_bracket_v7.py"),
        Path("tests/test_tier1_bracket_v7.py"),
        Path("tests/conftest.py"),
        V6_REGISTRY,
        V6_EVENT,
    )
    bindings.update({path.as_posix(): sha256_file(root / path) for path in new_paths})
    source_binding_id = v5.source_binding_id_from_metadata_v5(sources)
    core = {
        "schema_version": "tier1_bracket_successor_v7_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": delta["classification"],
        "supersedes_v6_trial_id": V6_TRIAL_ID,
        "v6_retirement_record_id": retirement.record_id,
        "change_scope": "TEST_LIFECYCLE_AND_RUNNER_BINDING_ONLY",
        "inherited_v6_contract_sha256": delta["inherited_v6_contract_sha256"],
        "bindings": bindings,
        "calendar_release_id": registry["calendar_release_id"],
        "dependency_lock_receipt_id": registry["dependency_lock_receipt_id"],
        "source_bindings": sorted(
            (dict(item) for item in sources),
            key=lambda item: (str(item["market"]), int(item["year"])),
        ),
        "source_binding_id": source_binding_id,
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "publication": False,
        "holdout_or_forward_access": False,
        "provider_access": False,
        "trading": False,
    }
    return PreparedV7Registration(sha256_json(core), core)


def persist_v6_retirement_v7(
    *, root: Path, prepared: PreparedV6RetirementV7,
) -> dict[str, str]:
    if prepared.record_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V6 retirement identity is invalid")
    preserved = prepared.canonical_payload.get("preserved_v6_sha256")
    if not isinstance(preserved, dict) or any(
        sha256_file(root / path) != digest for path, digest in preserved.items()
    ):
        raise IntegrityError("preserved V6 bytes changed after retirement preparation")
    registry = V6_RETIREMENT_REGISTRY_ROOT / f"{prepared.record_id}.json"
    event = V6_RETIREMENT_EVENT_ROOT / f"{prepared.record_id}.json"
    registry_path, event_path = root / registry, root / event
    if registry_path.exists() or event_path.exists():
        raise IntegrityError("V6 retirement publication is create-only")
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    event_path.parent.mkdir(parents=True, exist_ok=True)
    with registry_path.open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.canonical_payload,
            "state": "RETIRED_INVALID_BEFORE_SOURCE_ACCESS",
        }) + b"\n")
    with event_path.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_bracket_v6_retirement_event/1.0.0",
            "event_type": "RETIRED",
            "trial_id": V6_TRIAL_ID,
            "record_id": prepared.record_id,
        }) + b"\n")
    return {
        "record_id": prepared.record_id,
        "registry_path": registry.as_posix(),
        "event_path": event.as_posix(),
    }


def persist_v7_registration(
    *, root: Path, prepared: PreparedV7Registration,
) -> dict[str, str]:
    if prepared.trial_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V7 trial identity is invalid")
    bindings = prepared.canonical_payload.get("bindings")
    if not isinstance(bindings, dict) or any(
        sha256_file(root / path) != digest for path, digest in bindings.items()
    ):
        raise IntegrityError("V7 registration binding changed after preparation")
    retirement_id = prepared.canonical_payload.get("v6_retirement_record_id")
    if not _hex64(retirement_id):
        raise IntegrityError("V7 registration lacks a V6 retirement identity")
    retirement = _load(root / V6_RETIREMENT_REGISTRY_ROOT / f"{retirement_id}.json")
    if (
        retirement.get("state") != "RETIRED_INVALID_BEFORE_SOURCE_ACCESS"
        or sha256_json({
            **retirement,
            "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        }) != retirement_id
    ):
        raise IntegrityError("published V6 retirement is absent or inconsistent")
    registry = V7_REGISTRY_ROOT / f"{prepared.trial_id}.json"
    event = V7_EVENT_ROOT / f"{prepared.trial_id}.json"
    registry_path, event_path = root / registry, root / event
    if registry_path.exists() or event_path.exists():
        raise IntegrityError("V7 registration publication is create-only")
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    event_path.parent.mkdir(parents=True, exist_ok=True)
    with registry_path.open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.canonical_payload,
            "state": "REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
            "trial_id": prepared.trial_id,
        }) + b"\n")
    with event_path.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_bracket_successor_v7_event/1.0.0",
            "event_type": "DECLARED",
            "trial_id": prepared.trial_id,
            "source_row_access": False,
            "model_fit": False,
            "prediction_generation": False,
            "historical_evaluation": False,
            "holdout_or_forward_access": False,
        }) + b"\n")
    return {
        "trial_id": prepared.trial_id,
        "registry_path": registry.as_posix(),
        "event_path": event.as_posix(),
    }


def verify_historical_operation_receipt_v7(
    *, boundary: RepoBoundary, receipt: OperationReceipt, trial_id: str,
    source_binding_id: str, output_root: Path,
) -> str:
    if not _hex64(trial_id) or not _hex64(source_binding_id):
        raise UnauthorizedOperation("V7 historical receipt scope is invalid")
    boundary.assert_active_path(output_root.absolute(), purpose="V7 historical output root")
    required = {
        "trial_id": trial_id,
        "source_binding_id": source_binding_id,
        "output_root": output_root.as_posix(),
        "holdout_or_forward_access": "false",
        "provider_access": "false",
        "publication": "false",
    }
    receipt.verify(
        boundary,
        operation="EXECUTE_TIER1_BRACKET_SUCCESSOR_V7_HISTORICAL_SCREEN",
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
    )
    observed = dict(receipt.scope)
    approval = {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    if set(observed) != set(required) | approval or any(
        observed.get(key) != value for key, value in required.items()
    ):
        raise UnauthorizedOperation("V7 receipt does not grant the exact historical scope")
    if not receipt.single_use or not receipt.externally_authorized:
        raise UnauthorizedOperation("V7 execution requires single-use external authority")
    return receipt.receipt_id


def claim_historical_operation_receipt_v7(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_binding_id: str, output_root: Path,
) -> Path:
    receipt_id = verify_historical_operation_receipt_v7(
        boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_binding_id=source_binding_id, output_root=output_root,
    )
    claim = root / "state/authorization_uses" / f"{receipt_id}.json"
    boundary.assert_active_path(
        claim.absolute(), purpose="V7 authorization use",
        subtree="state/authorization_uses",
    )
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        with claim.open("xb") as stream:
            stream.write(canonical_bytes({
                "schema_version": "tier1_bracket_v7_authorization_use/1.0.0",
                "receipt_id": receipt_id,
                "trial_id": trial_id,
                "source_binding_id": source_binding_id,
                "output_root": output_root.as_posix(),
                "holdout_or_forward_access": False,
            }) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("V7 historical receipt was already consumed") from exc
    return claim


def authorized_source_streams_v7(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path],
    output_root: Path,
) -> tuple[
    Mapping[tuple[str, int], Iterator[V5SourceRecord]],
    Mapping[tuple[str, int], v6.SourceIntegrityAuditV6],
]:
    if any(year == 2025 for _, year in source_paths):
        raise UnauthorizedOperation("2025 holdout path is rejected before open")
    registry = _load(root / V7_REGISTRY_ROOT / f"{trial_id}.json")
    bindings = registry.get("bindings")
    if (
        registry.get("trial_id") != trial_id
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or registry.get("holdout_or_forward_access") is not False
        or not isinstance(bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
    ):
        raise UnauthorizedOperation("registered V7 declaration is unavailable or drifted")
    require_locked_repository_environment(root)
    raw = registry.get("source_bindings")
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise IntegrityError("registered V7 source bindings are absent")
    binding_id = v5.source_binding_id_from_metadata_v5(raw)
    expected = {
        (str(item["market"]), int(item["year"])): str(item["source_parquet_sha256"])
        for item in raw
    }
    if registry.get("source_binding_id") != binding_id or set(source_paths) != set(expected):
        raise IntegrityError("V7 source binding or path map is inconsistent")
    claim_historical_operation_receipt_v7(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_binding_id=binding_id, output_root=output_root,
    )
    for key, path in source_paths.items():
        if sha256_file(path) != expected[key]:
            raise IntegrityError("V7 source bytes differ from registration")
    audits = {key: v6.SourceIntegrityAuditV6(key[0]) for key in sorted(source_paths)}
    streams = {
        key: v6.iter_source_records_from_parquet_v6(
            market=key[0], path=source_paths[key], audit=audits[key],
        )
        for key in sorted(source_paths)
    }
    return streams, audits


def execute_authorized_v7(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path],
    output_root: Path,
) -> v6.V6PipelineResult:
    streams, audits = authorized_source_streams_v7(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_paths=source_paths, output_root=output_root,
    )
    registry = _load(root / V7_REGISTRY_ROOT / f"{trial_id}.json")
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(registry["calendar_release_id"]),
    )
    inherited, _ = load_v7_contract(root=root)
    base = v5.run_v5_pipeline(
        streams=streams,
        census=v5.build_expected_census_from_calendar(sessions=sessions),
        contract=inherited,
        trial_id=trial_id,
        runtime_receipt=v5.prepare_runtime_receipt_v5(root=root, trial_id=trial_id),
    )
    audit_payload = {
        f"{market}/{year}": audit.as_dict()
        for (market, year), audit in sorted(audits.items())
    }
    return v6.V6PipelineResult(base, audit_payload)


def build_evidence_manifest_v7(
    *, trial_id: str, result: v6.V6PipelineResult,
) -> dict[str, object]:
    payloads = v6._evidence_payloads_v6(result)
    if not result.base.evidence.predictions or not result.base.evidence.opportunity_ledger:
        raise IntegrityError("V7 evidence lacks frozen predictions or opportunity rows")
    files = {
        f"{name}.json": sha256_bytes(canonical_bytes({"payload": payload}) + b"\n")
        for name, payload in sorted(payloads.items())
    }
    core = {
        "schema_version": "tier1_bracket_successor_v7_evidence_manifest/1.0.0",
        "trial_id": trial_id,
        "files": files,
    }
    return {**core, "manifest_id": sha256_json(core)}


def persist_evidence_bundle_v7(
    *, boundary: RepoBoundary, output_root: Path, trial_id: str,
    result: v6.V6PipelineResult,
) -> dict[str, str]:
    """Publish all V7 evidence create-only after separate approval."""

    manifest = build_evidence_manifest_v7(trial_id=trial_id, result=result)
    boundary.assert_active_path(
        output_root.absolute(), purpose="V7 evidence output root"
    )
    destination = output_root / trial_id / str(manifest["manifest_id"])
    if destination.exists():
        raise IntegrityError("V7 evidence publication is create-only")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(
        prefix=f".staging-{manifest['manifest_id']}-",
        dir=destination.parent,
    ))
    payloads = v6._evidence_payloads_v6(result)
    for filename, expected_hash in manifest["files"].items():
        name = filename.removesuffix(".json")
        path = staging / filename
        with path.open("xb") as stream:
            stream.write(canonical_bytes({"payload": payloads[name]}) + b"\n")
        if sha256_file(path) != expected_hash:
            raise IntegrityError("persisted V7 evidence hash mismatch")
    manifest_path = staging / "manifest.json"
    with manifest_path.open("xb") as stream:
        stream.write(canonical_bytes(manifest) + b"\n")
    if destination.exists():
        raise IntegrityError("V7 evidence destination appeared during publication")
    staging.replace(destination)
    final_manifest = destination / "manifest.json"
    return {
        "manifest_id": str(manifest["manifest_id"]),
        "manifest_path": final_manifest.as_posix(),
        "manifest_sha256": sha256_file(final_manifest),
    }
