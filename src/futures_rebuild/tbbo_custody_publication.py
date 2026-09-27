"""Provider-free certification and atomic publication of sealed TBBO custody."""

from __future__ import annotations

from .custody_successor import resolve_snapshot, binding_matches

import os
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Final

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import io_path, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .live_cockpit.single_instance import SingleInstance
from .runtime_environment import require_locked_repository_environment
from .tbbo_16_market_acquisition import (
    PLAN_PATH,
    PUBLICATION_OPERATION,
    RECORD_ROOT,
    REQUEST_COUNT,
    build_request_ledger,
    json_object,
    load_header,
    load_plan,
    plain_tree_files,
    protected_active_state,
    request_paths,
    verify_sidecar,
    write_or_verify,
)


OPERATION: Final = PUBLICATION_OPERATION
PREPARATION_ROOT: Final = Path("state/tbbo_custody_preparations")
REGISTRY_ROOT: Final = Path("state/tbbo_custody_registry")
VAULT_ROOT: Final = Path("data/vault/source_snapshots")
LOCK_PATH: Final = Path("state/locks/tbbo_custody_publication_v1.lock")
TERMINAL_STATE: Final = (
    "SEALED_CANONICAL_CUSTODY_NOT_ROW_QUALITY_CERTIFIED_NOT_RESEARCH_ELIGIBLE"
)


def _inventory_tree(snapshot: Path) -> list[str]:
    return [
        path.relative_to(snapshot).as_posix()
        for path in plain_tree_files(snapshot, reject_hardlinks=True)
    ]


def _load_acquisition(root: Path, record_id: str) -> tuple[dict[str, object], dict[str, object], Path]:
    record_path = root / RECORD_ROOT / f"{record_id}.json"
    record = json_object(record_path, "TBBO acquisition record")
    if (
        record.get("record_id") != record_id
        or record.get("state") != "SEALED_PROVIDER_CUSTODY_AWAITING_VAULT_PUBLICATION"
        or record.get("dbn_rows_decoded") != 0
    ):
        raise IntegrityError("TBBO acquisition record is invalid")
    manifest_path = root / str(record.get("manifest_path", ""))
    manifest = json_object(manifest_path, "TBBO acquisition manifest")
    manifest_core = dict(manifest)
    manifest_id = manifest_core.pop("manifest_id", None)
    if (
        manifest_id != record_id
        or manifest_id != sha256_json(manifest_core)
        or record.get("manifest_sha256") != sha256_file(manifest_path)
        or manifest.get("dbn_rows_decoded") != 0
        or manifest.get("request_count") != REQUEST_COUNT
        or manifest.get("canonical_publication_performed") is not False
    ):
        raise IntegrityError("TBBO acquisition manifest is invalid")
    attempt = root / str(record.get("attempt_path", ""))
    if manifest_path != attempt / "manifest.json":
        raise IntegrityError("TBBO acquisition record paths differ")
    return record, manifest, attempt


def certify_staged_snapshot(
    *, root: Path, record_id: str,
    metadata_loader: Callable[[Path], object] = load_header,
) -> dict[str, object]:
    plan = load_plan(root=root)
    record, manifest, attempt = _load_acquisition(root, record_id)
    snapshot = attempt / "snapshot"
    ledger = build_request_ledger(plan)
    expected: set[str] = set()
    entries: list[dict[str, object]] = []
    for request in ledger:
        dbn, sidecar_path = request_paths(snapshot, request)
        sidecar = verify_sidecar(
            plan=plan, snapshot=snapshot, request=request,
            metadata_loader=metadata_loader,
        )
        if sidecar is None:
            raise IntegrityError("staged TBBO snapshot has a request gap")
        dbn_relative = dbn.relative_to(snapshot).as_posix()
        sidecar_relative = sidecar_path.relative_to(snapshot).as_posix()
        expected.update((dbn_relative, sidecar_relative))
        entries.append({
            "query": request.as_dict(),
            "dbn": {
                "path": dbn_relative,
                "size": io_path(dbn).stat().st_size,
                "sha256": sha256_file(dbn),
            },
            "sidecar": {
                "path": sidecar_relative,
                "size": io_path(sidecar_path).stat().st_size,
                "sha256": sha256_file(sidecar_path),
            },
        })
    observed = _inventory_tree(snapshot)
    if len(entries) != REQUEST_COUNT or observed != sorted(expected):
        raise IntegrityError("staged TBBO snapshot inventory is not exactly 192 pairs")
    baseline = manifest.get("protected_active_baseline")
    if not isinstance(baseline, Mapping):
        raise IntegrityError("protected active-source baseline is absent")
    core = {
        "schema_version": "tbbo_custody_snapshot_identity/1.0.0",
        "state": TERMINAL_STATE,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "acquisition_record_id": record_id,
        "acquisition_manifest_sha256": record["manifest_sha256"],
        "request_set_id": sha256_json([request.as_dict() for request in ledger]),
        "entries": entries,
        "entry_count": len(entries),
        "dbn_file_count": REQUEST_COUNT,
        "sidecar_file_count": REQUEST_COUNT,
        "total_payload_bytes": sum(int(item["dbn"]["size"]) for item in entries),
        "total_snapshot_bytes": sum(
            int(item[kind]["size"]) for item in entries for kind in ("dbn", "sidecar")
        ),
        "dbn_rows_decoded": 0,
        "row_quality_certified": False,
        "research_eligible": False,
        "definition_data_known_end_exclusive": "2026-07-14",
        "protected_active_baseline": baseline,
    }
    snapshot_id = sha256_json(core)
    preparation = {
        **core,
        "snapshot_id": snapshot_id,
        "staging_snapshot_path": snapshot.relative_to(root).as_posix(),
        "vault_snapshot_path": (VAULT_ROOT / snapshot_id).as_posix(),
        "registry_path": (REGISTRY_ROOT / snapshot_id / "snapshot.json").as_posix(),
        "publication_operation": OPERATION,
    }
    write_or_verify(
        root / PREPARATION_ROOT / f"{snapshot_id}.json", preparation,
        "TBBO custody preparation",
    )
    return preparation


def required_scope(*, root: Path, preparation: Mapping[str, object]) -> dict[str, str]:
    snapshot_id = str(preparation["snapshot_id"])
    preparation_path = root / PREPARATION_ROOT / f"{snapshot_id}.json"
    return {
        "snapshot_id": snapshot_id,
        "acquisition_record_id": str(preparation["acquisition_record_id"]),
        "preparation_path": preparation_path.relative_to(root).as_posix(),
        "preparation_sha256": sha256_file(preparation_path),
        "staging_snapshot_path": str(preparation["staging_snapshot_path"]),
        "vault_snapshot_path": str(preparation["vault_snapshot_path"]),
        "dbn_file_count": str(REQUEST_COUNT),
        "sidecar_file_count": str(REQUEST_COUNT),
        "row_decode": "false",
        "active_source_mutation": "false",
        "approval_command": OPERATION,
        "approval_plan_id": snapshot_id,
        "approval_plan_sha256": sha256_file(preparation_path),
    }


def _verify_published(root: Path, preparation: Mapping[str, object]) -> None:
    snapshot = resolve_snapshot(root, str(preparation["vault_snapshot_path"]))
    if _inventory_tree(snapshot) != sorted(
        str(item[kind]["path"])
        for item in preparation["entries"]
        for kind in ("dbn", "sidecar")
    ):
        raise IntegrityError("published TBBO snapshot inventory differs")
    for item in preparation["entries"]:
        for kind in ("dbn", "sidecar"):
            expected = item[kind]
            path = snapshot / str(expected["path"])
            if io_path(path).stat().st_size != expected["size"] or sha256_file(path) != expected["sha256"]:
                raise IntegrityError("published TBBO snapshot bytes differ")


def _require_same_volume(source: Path, destination_parent: Path) -> None:
    if io_path(source).stat().st_dev != io_path(destination_parent).stat().st_dev:
        raise IntegrityError("TBBO custody publication requires one volume")


def publish_authorized_snapshot(
    *, root: Path, record_id: str, authorization: OperationReceipt,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
    metadata_loader: Callable[[Path], object] = load_header,
) -> dict[str, object]:
    root = root.resolve(strict=True)
    plan = load_plan(root=root)
    publication = plan["publication"]
    if (
        not isinstance(publication, Mapping)
        or not binding_matches(root, Path(__file__), publication.get("publication_implementation_sha256"))
    ):
        raise IntegrityError("TBBO publication implementation drifted")
    environment_check(root)
    preparation = certify_staged_snapshot(
        root=root, record_id=record_id, metadata_loader=metadata_loader,
    )
    scope = required_scope(root=root, preparation=preparation)
    boundary = RepoBoundary(root)
    staging = root / str(preparation["staging_snapshot_path"])
    destination = boundary.assert_snapshot_path(resolve_snapshot(root, str(preparation["vault_snapshot_path"])))
    registry = root / str(preparation["registry_path"])
    lock = SingleInstance(root / LOCK_PATH)
    if not lock.acquire():
        raise UnauthorizedOperation("another TBBO custody publication is running")
    moved = False
    try:
        authorization.consume(
            boundary, operation=OPERATION,
            classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            required_scope=scope,
        )
        if io_path(destination).exists() or io_path(registry).exists():
            raise IntegrityError("TBBO custody publication destination already exists")
        io_path(destination.parent).mkdir(parents=True, exist_ok=True)
        _require_same_volume(staging, destination.parent)
        os.replace(io_path(staging), io_path(destination))
        moved = True
        try:
            _verify_published(root, preparation)
            if preparation["protected_active_baseline"] != protected_active_state(root):
                raise IntegrityError("protected active sources changed during publication")
            registry_payload = {
                **preparation,
                "schema_version": "tbbo_custody_registry_record/1.0.0",
                "state": TERMINAL_STATE,
                "publication_authorization_receipt_id": authorization.receipt_id,
                "staging_payload_bytes_retained": False,
            }
            write_or_verify(registry, registry_payload, "TBBO custody registry record")
        except Exception:
            if io_path(destination).exists() and not io_path(staging).exists():
                io_path(staging.parent).mkdir(parents=True, exist_ok=True)
                os.replace(io_path(destination), io_path(staging))
                moved = False
            raise
        return {
            "snapshot_id": preparation["snapshot_id"],
            "state": TERMINAL_STATE,
            "vault_snapshot_path": preparation["vault_snapshot_path"],
            "registry_path": preparation["registry_path"],
            "dbn_file_count": REQUEST_COUNT,
            "sidecar_file_count": REQUEST_COUNT,
            "dbn_rows_decoded": 0,
            "staging_payload_bytes_retained": False,
        }
    finally:
        unsafe = moved and not io_path(registry).exists()
        lock.release()
        if unsafe:
            raise IntegrityError("TBBO publication failed without rollback")


__all__ = [
    "OPERATION", "TERMINAL_STATE", "certify_staged_snapshot",
    "publish_authorized_snapshot", "required_scope",
]
