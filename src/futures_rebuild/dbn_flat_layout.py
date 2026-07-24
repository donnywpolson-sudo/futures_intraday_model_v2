"""Approval-gated copy of one verified DBN release into its flat physical layout."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (
    assert_plain_file,
    canonical_bytes,
    fsync_directory,
    sha256_file,
    sha256_json,
)
from .data_layout import (
    DataFileEntry,
    DataReleaseManifest,
    manifest_relative_path,
    verify_data_release_manifest,
    verify_layout_contract,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .locking import FileLease


PLAN_VERSION = "1.0.0"
APPROVAL_VERSION = "1.0.0"
RECEIPT_VERSION = "1.0.0"
OPERATION = "COPY_CANONICAL_DBN_TO_FLAT_LAYOUT_V1"
CANONICAL_RELEASE_ID = "9e5a9f2a405e50b0cda6702b67506b0951b057500781d37c45171da3967e9b51"
EXPECTED_FILES = 8_040
EXPECTED_BYTES = 25_007_876_004
PLAN_PATH = Path("configs/dbn_flat_layout_migration_plan.json")
APPROVAL_PATH = Path("configs/dbn_flat_layout_migration_approval.json")
RECEIPT_ROOT = Path("manifests/data_layout_transitions")
LOCK_PATH = Path("state/locks/dbn-flat-layout.lock")
QUARANTINE_ROOT = Path("state/quarantine/dbn_flat_layout")
IMPLEMENTATION_PATHS = (
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/dbn_flat_layout.py",
    "src/futures_rebuild/locking.py",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class CopyBinding:
    entry: DataFileEntry
    source: Path
    destination: Path


def _canonical_json(payload: Mapping[str, object]) -> bytes:
    return canonical_bytes(dict(payload)) + b"\n"


def _load_json(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise ContractError(f"invalid JSON: {path}") from exc
    if not isinstance(payload, dict) or raw != _canonical_json(payload):
        raise ContractError(f"JSON is not a canonical object: {path}")
    return payload


def _manifest_path(boundary: RepoBoundary, release_id: str) -> Path:
    return boundary.active_root / manifest_relative_path("dbn", release_id)


def _bindings(
    manifest: DataReleaseManifest, boundary: RepoBoundary
) -> tuple[CopyBinding, ...]:
    bindings: list[CopyBinding] = []
    for entry in manifest.files:
        source = boundary.active_root / manifest.retained_release_id_relative_path(entry)
        destination = boundary.active_root / manifest.physical_relative_path(entry)
        boundary.assert_active_path(
            source, purpose="retained DBN release source", subtree="data/dbn"
        )
        boundary.assert_active_path(
            destination, purpose="flat DBN destination", subtree="data/dbn"
        )
        if source == destination:
            raise IntegrityError("flat DBN source and destination are identical")
        bindings.append(CopyBinding(entry, source, destination))
    if len({item.destination for item in bindings}) != len(bindings):
        raise IntegrityError("flat DBN destinations collide")
    return tuple(bindings)


def _verify_file(path: Path, entry: DataFileEntry, *, label: str) -> None:
    assert_plain_file(path)
    if path.stat().st_size != entry.size or sha256_file(path) != entry.sha256:
        raise IntegrityError(f"{label} differs from the canonical DBN manifest")


def build_scope(
    *, repository_root: Path, release_id: str | None = None
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(active_root=root)
    release_id = CANONICAL_RELEASE_ID if release_id is None else release_id
    if release_id != CANONICAL_RELEASE_ID:
        raise UnauthorizedOperation("flat migration is bound to the canonical DBN release")
    contract_path = root / "configs" / "data_layout_contract.json"
    contract = verify_layout_contract(contract_path)
    if (
        contract.get("physical_path_templates", {}).get("dbn")
        != "data/dbn/{family}/{market}/{year}/{filename}"
        or contract.get("retained_release_id_copy_phases") not in ([], ["dbn"])
    ):
        raise IntegrityError("layout contract does not declare the controlled DBN transition")
    manifest_path = _manifest_path(boundary, release_id)
    manifest = verify_data_release_manifest(manifest_path, boundary, verify_files=False)
    if manifest.phase != "dbn" or manifest.release_id != release_id:
        raise IntegrityError("canonical DBN manifest identity is invalid")
    if len(manifest.files) != EXPECTED_FILES or sum(item.size for item in manifest.files) != EXPECTED_BYTES:
        raise IntegrityError("canonical DBN totals differ from the frozen migration scope")
    bindings = _bindings(manifest, boundary)
    for binding in bindings:
        _verify_file(binding.source, binding.entry, label="retained DBN source")
        if binding.destination.exists():
            _verify_file(binding.destination, binding.entry, label="flat DBN destination")
    implementation = [
        {"path": relative, "sha256": sha256_file(root / relative)}
        for relative in IMPLEMENTATION_PATHS
    ]
    inventory = [item.as_dict() for item in manifest.files]
    return {
        "destination_layout": "data/dbn/{family}/{market}/{year}/{filename}",
        "expected_total_bytes": EXPECTED_BYTES,
        "expected_total_files": EXPECTED_FILES,
        "implementation_files": implementation,
        "implementation_sha256": sha256_json(implementation),
        "inventory_sha256": sha256_json(inventory),
        "layout_contract_sha256": sha256_file(contract_path),
        "legacy_layout_preserved": True,
        "operation": OPERATION,
        "repository_root": str(root),
        "source_layout": "data/dbn/{family}/{market}/{year}/{release-id}/{filename}",
        "source_manifest_path": manifest_path.relative_to(root).as_posix(),
        "source_manifest_sha256": sha256_file(manifest_path),
        "source_release_id": release_id,
    }


def build_plan(scope: Mapping[str, object]) -> dict[str, object]:
    core = {"migration_plan_version": PLAN_VERSION, "scope": dict(scope)}
    return {**core, "migration_plan_id": sha256_json(core)}


def build_approval_draft(plan: Mapping[str, object]) -> dict[str, object]:
    return {
        "approval_receipt_id": None,
        "approval_version": APPROVAL_VERSION,
        "approved_at": None,
        "migration_plan_id": plan.get("migration_plan_id"),
        "operation": OPERATION,
        "scope": plan.get("scope"),
        "status": "PENDING_APPROVAL",
        "user_authorization_id": None,
    }


def verify_approval(approval: Mapping[str, object], plan: Mapping[str, object]) -> str:
    approved_at = approval.get("approved_at")
    user_authorization_id = approval.get("user_authorization_id")
    core = {
        "approval_version": APPROVAL_VERSION,
        "approved_at": approved_at,
        "migration_plan_id": plan.get("migration_plan_id"),
        "operation": OPERATION,
        "scope": plan.get("scope"),
        "status": "APPROVED",
        "user_authorization_id": user_authorization_id,
    }
    expected = {**core, "approval_receipt_id": approval.get("approval_receipt_id")}
    if (
        dict(approval) != expected
        or type(approved_at) is not str
        or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", approved_at) is None
        or type(user_authorization_id) is not str
        or _SHA256.fullmatch(user_authorization_id) is None
        or approval.get("approval_receipt_id") != sha256_json(core)
    ):
        raise UnauthorizedOperation("flat DBN migration lacks exact hash-bound approval")
    return str(approval["approval_receipt_id"])


def _quarantine(boundary: RepoBoundary, path: Path) -> Path:
    root = boundary.active_root / QUARANTINE_ROOT
    root.mkdir(parents=True, exist_ok=True)
    destination = root / f"{uuid.uuid4().hex}-{path.name}"
    os.replace(path, destination)
    fsync_directory(root)
    return destination


def _copy_one(binding: CopyBinding, boundary: RepoBoundary) -> None:
    target = binding.destination
    target.parent.mkdir(parents=True, exist_ok=True)
    prefix = f".{target.name}.flat-migration-"
    for temporary in sorted(target.parent.glob(f"{prefix}*.tmp")):
        try:
            _verify_file(temporary, binding.entry, label="recoverable flat DBN temporary")
        except (ContractError, IntegrityError):
            _quarantine(boundary, temporary)
            continue
        if target.exists():
            _verify_file(target, binding.entry, label="flat DBN destination")
            temporary.unlink()
        else:
            os.replace(temporary, target)
        fsync_directory(target.parent)
    if target.exists():
        _verify_file(target, binding.entry, label="flat DBN destination")
        return
    temporary = target.parent / f"{prefix}{uuid.uuid4().hex}.tmp"
    try:
        shutil.copyfile(binding.source, temporary)
        _verify_file(temporary, binding.entry, label="staged flat DBN copy")
        with temporary.open("r+b") as handle:
            os.fsync(handle.fileno())
        if target.exists():
            _verify_file(target, binding.entry, label="flat DBN destination")
            temporary.unlink()
        else:
            os.replace(temporary, target)
        fsync_directory(target.parent)
    except BaseException:
        if temporary.exists():
            _quarantine(boundary, temporary)
        raise


def _receipt_path(boundary: RepoBoundary, plan_id: str) -> Path:
    if _SHA256.fullmatch(plan_id) is None:
        raise ContractError("migration plan ID is invalid")
    return boundary.active_root / RECEIPT_ROOT / f"{plan_id}.json"


def _write_receipt(path: Path, payload: Mapping[str, object]) -> None:
    encoded = _canonical_json(payload)
    if path.exists():
        if path.read_bytes() != encoded:
            raise IntegrityError("existing flat migration receipt conflicts")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    descriptor = os.open(
        temporary,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    fsync_directory(path.parent)


def execute(
    *,
    repository_root: Path,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(active_root=root)
    approval_id = verify_approval(approval, plan)
    live_scope = build_scope(repository_root=root)
    if plan.get("scope") != live_scope:
        raise UnauthorizedOperation("flat DBN migration plan differs from live inputs")
    plan_id = plan.get("migration_plan_id")
    if type(plan_id) is not str or plan_id != sha256_json(
        {"migration_plan_version": PLAN_VERSION, "scope": live_scope}
    ):
        raise UnauthorizedOperation("flat DBN migration plan identity is invalid")
    operation = OperationReceipt.issue_local(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={"migration_plan_id": plan_id, "source_release_id": CANONICAL_RELEASE_ID},
    )
    operation.verify(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
    )
    manifest = verify_data_release_manifest(
        _manifest_path(boundary, CANONICAL_RELEASE_ID), boundary, verify_files=False
    )
    with FileLease(boundary.active_root / LOCK_PATH):
        for binding in _bindings(manifest, boundary):
            _copy_one(binding, boundary)
        verify_data_release_manifest(
            _manifest_path(boundary, CANONICAL_RELEASE_ID), boundary, verify_files=True
        )
        for binding in _bindings(manifest, boundary):
            _verify_file(binding.source, binding.entry, label="retained DBN source")
        receipt_path = _receipt_path(boundary, plan_id)
        if receipt_path.exists():
            receipt = _load_json(receipt_path)
            verify_receipt(repository_root=root, receipt=receipt, plan=plan, approval=approval)
            return receipt
        core = {
            "approval_receipt_id": approval_id,
            "completed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "destination_layout": live_scope["destination_layout"],
            "inventory_sha256": live_scope["inventory_sha256"],
            "legacy_layout_preserved": True,
            "migration_plan_id": plan_id,
            "receipt_version": RECEIPT_VERSION,
            "source_manifest_sha256": live_scope["source_manifest_sha256"],
            "source_release_id": CANONICAL_RELEASE_ID,
            "status": "COMPLETE_VERIFIED_COPY_ONLY",
            "total_bytes": EXPECTED_BYTES,
            "total_files": EXPECTED_FILES,
        }
        receipt = {**core, "receipt_id": sha256_json(core)}
        _write_receipt(receipt_path, receipt)
        return receipt


def verify_receipt(
    *,
    repository_root: Path,
    receipt: Mapping[str, object],
    plan: Mapping[str, object],
    approval: Mapping[str, object],
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(active_root=root)
    approval_id = verify_approval(approval, plan)
    plan_id = plan.get("migration_plan_id")
    core = {key: value for key, value in receipt.items() if key != "receipt_id"}
    if (
        set(receipt)
        != {
            "approval_receipt_id",
            "completed_at",
            "destination_layout",
            "inventory_sha256",
            "legacy_layout_preserved",
            "migration_plan_id",
            "receipt_id",
            "receipt_version",
            "source_manifest_sha256",
            "source_release_id",
            "status",
            "total_bytes",
            "total_files",
        }
        or receipt.get("approval_receipt_id") != approval_id
        or receipt.get("migration_plan_id") != plan_id
        or receipt.get("receipt_version") != RECEIPT_VERSION
        or receipt.get("source_release_id") != CANONICAL_RELEASE_ID
        or receipt.get("status") != "COMPLETE_VERIFIED_COPY_ONLY"
        or receipt.get("legacy_layout_preserved") is not True
        or receipt.get("total_files") != EXPECTED_FILES
        or receipt.get("total_bytes") != EXPECTED_BYTES
        or receipt.get("receipt_id") != sha256_json(core)
    ):
        raise IntegrityError("flat DBN migration receipt is invalid")
    live_scope = build_scope(repository_root=root)
    if plan.get("scope") != live_scope:
        raise IntegrityError("flat DBN migration receipt differs from live inputs")
    manifest = verify_data_release_manifest(
        _manifest_path(boundary, CANONICAL_RELEASE_ID), boundary
    )
    for binding in _bindings(manifest, boundary):
        _verify_file(binding.source, binding.entry, label="retained DBN source")
    return dict(receipt)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    for command in ("plan", "execute", "verify"):
        item = subcommands.add_parser(command)
        item.add_argument("--repository-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.repository_root.resolve(strict=True)
    if args.command == "plan":
        plan = build_plan(build_scope(repository_root=root))
        print(json.dumps({"approval_draft": build_approval_draft(plan), "plan": plan}, sort_keys=True))
        return 0
    plan = _load_json(root / PLAN_PATH)
    approval = _load_json(root / APPROVAL_PATH)
    if args.command == "execute":
        receipt = execute(repository_root=root, plan=plan, approval=approval)
    else:
        plan_id = plan.get("migration_plan_id")
        if type(plan_id) is not str:
            raise ContractError("migration plan ID is invalid")
        receipt = verify_receipt(
            repository_root=root,
            receipt=_load_json(_receipt_path(RepoBoundary(active_root=root), plan_id)),
            plan=plan,
            approval=approval,
        )
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
