"""Approval-gated removal of verified retained DBN release-ID copies."""

from __future__ import annotations

import argparse
import json
import os
import re
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
    is_linklike,
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
INTENT_VERSION = "1.0.0"
RECEIPT_VERSION = "1.0.0"
OPERATION = "DELETE_VERIFIED_RETAINED_DBN_RELEASE_ID_COPIES_V1"
CANONICAL_RELEASE_ID = "9e5a9f2a405e50b0cda6702b67506b0951b057500781d37c45171da3967e9b51"
EXPECTED_DBN_FILES = 4_020
EXPECTED_SIDECAR_FILES = 4_020
EXPECTED_FILES = EXPECTED_DBN_FILES + EXPECTED_SIDECAR_FILES
EXPECTED_BYTES = 25_007_876_004
EXPECTED_RELEASE_DIRECTORIES = 3_777
PLAN_PATH = Path("configs/dbn_flat_layout_cutover_plan.json")
APPROVAL_PATH = Path("configs/dbn_flat_layout_cutover_approval.json")
FLAT_MIGRATION_RECEIPT_PATH = Path(
    "manifests/data_layout_transitions/"
    "a41d87b4732537326388ec9838e6b7f3303d6f01753a73b5e4add117f758fefa.json"
)
RECEIPT_ROOT = Path("manifests/data_layout_cutovers")
STATE_ROOT = Path("state/data_cutover/dbn_flat")
LOCK_PATH = Path("state/locks/dbn-flat-cutover.lock")
IMPLEMENTATION_PATHS = (
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/dbn_flat_cutover.py",
    "src/futures_rebuild/locking.py",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class CutoverBinding:
    entry: DataFileEntry
    flat: Path
    retained: Path


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


def _write_new_or_exact(path: Path, payload: Mapping[str, object]) -> None:
    encoded = _canonical_json(payload)
    if path.exists():
        assert_plain_file(path)
        if path.read_bytes() != encoded:
            raise IntegrityError(f"existing cutover artifact conflicts: {path}")
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


def _verify_file(path: Path, entry: DataFileEntry, *, label: str) -> None:
    assert_plain_file(path)
    if path.stat().st_size != entry.size or sha256_file(path) != entry.sha256:
        raise IntegrityError(f"{label} differs from the canonical DBN manifest")


def _manifest(boundary: RepoBoundary) -> tuple[Path, DataReleaseManifest]:
    path = boundary.active_root / manifest_relative_path("dbn", CANONICAL_RELEASE_ID)
    manifest = verify_data_release_manifest(path, boundary, verify_files=False)
    if manifest.phase != "dbn" or manifest.release_id != CANONICAL_RELEASE_ID:
        raise IntegrityError("canonical DBN manifest identity is invalid")
    return path, manifest


def _bindings(
    manifest: DataReleaseManifest, boundary: RepoBoundary
) -> tuple[CutoverBinding, ...]:
    bindings: list[CutoverBinding] = []
    for entry in manifest.files:
        logical = Path(entry.logical_path)
        flat = boundary.active_root / logical
        retained = flat.parent / CANONICAL_RELEASE_ID / flat.name
        boundary.assert_active_path(flat, purpose="flat DBN file", subtree="data/dbn")
        boundary.assert_active_path(
            retained, purpose="retained DBN file", subtree="data/dbn"
        )
        bindings.append(CutoverBinding(entry, flat, retained))
    if len({item.flat for item in bindings}) != len(bindings) or len(
        {item.retained for item in bindings}
    ) != len(bindings):
        raise IntegrityError("DBN cutover paths collide")
    return tuple(bindings)


def _verify_pairing(manifest: DataReleaseManifest) -> None:
    dbn = {
        entry.logical_path
        for entry in manifest.files
        if not entry.logical_path.endswith(".manifest.json")
    }
    sidecars = {
        entry.logical_path.removesuffix(".manifest.json")
        for entry in manifest.files
        if entry.logical_path.endswith(".manifest.json")
    }
    if (
        len(dbn) != EXPECTED_DBN_FILES
        or len(sidecars) != EXPECTED_SIDECAR_FILES
        or dbn != sidecars
    ):
        raise IntegrityError("DBN files and JSON sidecars are not exactly paired")


def _verify_flat_migration_receipt(
    root: Path, *, inventory_sha256: str
) -> tuple[Path, dict[str, object]]:
    path = root / FLAT_MIGRATION_RECEIPT_PATH
    receipt = _load_json(path)
    core = {key: value for key, value in receipt.items() if key != "receipt_id"}
    if (
        receipt.get("status") != "COMPLETE_VERIFIED_COPY_ONLY"
        or receipt.get("source_release_id") != CANONICAL_RELEASE_ID
        or receipt.get("inventory_sha256") != inventory_sha256
        or receipt.get("legacy_layout_preserved") is not True
        or receipt.get("total_files") != EXPECTED_FILES
        or receipt.get("total_bytes") != EXPECTED_BYTES
        or receipt.get("receipt_id") != sha256_json(core)
    ):
        raise IntegrityError("flat DBN migration receipt is invalid")
    return path, receipt


def _observed_dbn_tree(root: Path) -> tuple[set[Path], set[Path], set[Path]]:
    flat_files: set[Path] = set()
    retained_files: set[Path] = set()
    release_directories: set[Path] = set()
    data_root = root / "data" / "dbn"
    for path in data_root.rglob("*"):
        if is_linklike(path):
            raise IntegrityError(f"DBN tree contains a link-like path: {path}")
        if path.is_dir() and _SHA256.fullmatch(path.name):
            release_directories.add(path)
        elif path.is_file():
            if _SHA256.fullmatch(path.parent.name):
                retained_files.add(path)
            else:
                flat_files.add(path)
    return flat_files, retained_files, release_directories


def _verify_live_state(
    boundary: RepoBoundary, *, require_complete_retained: bool
) -> tuple[Path, DataReleaseManifest, tuple[CutoverBinding, ...], str]:
    manifest_path, manifest = _manifest(boundary)
    if len(manifest.files) != EXPECTED_FILES or sum(
        entry.size for entry in manifest.files
    ) != EXPECTED_BYTES:
        raise IntegrityError("canonical DBN totals differ from the cutover contract")
    _verify_pairing(manifest)
    bindings = _bindings(manifest, boundary)
    expected_flat = {item.flat for item in bindings}
    expected_retained = {item.retained for item in bindings}
    expected_directories = {item.retained.parent for item in bindings}
    observed_flat, observed_retained, observed_directories = _observed_dbn_tree(
        boundary.active_root
    )
    if observed_flat != expected_flat:
        raise IntegrityError("flat DBN tree has missing or unexpected files")
    if require_complete_retained:
        if observed_retained != expected_retained:
            raise IntegrityError("retained DBN tree has missing or unexpected files")
        if observed_directories != expected_directories:
            raise IntegrityError("retained DBN release directory census differs")
    elif (
        not observed_retained.issubset(expected_retained)
        or not observed_directories.issubset(expected_directories)
    ):
        raise IntegrityError("resumable retained DBN state is outside the approved intent")
    if len(expected_directories) != EXPECTED_RELEASE_DIRECTORIES:
        raise IntegrityError("retained DBN directory count differs from the frozen scope")
    for item in bindings:
        _verify_file(item.flat, item.entry, label="flat DBN file")
        if item.retained.exists():
            _verify_file(item.retained, item.entry, label="retained DBN file")
        elif require_complete_retained:
            raise IntegrityError("retained DBN file is missing before cutover intent")
    inventory_sha256 = sha256_json([item.entry.as_dict() for item in bindings])
    return manifest_path, manifest, bindings, inventory_sha256


def build_scope(*, repository_root: Path) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(active_root=root)
    contract_path = root / "configs" / "data_layout_contract.json"
    contract = verify_layout_contract(contract_path)
    if (
        contract.get("physical_path_templates", {}).get("dbn")
        != "data/dbn/{family}/{market}/{year}/{filename}"
        or contract.get("retained_release_id_copy_phases") not in ([], ["dbn"])
    ):
        raise IntegrityError("DBN cutover requires the active transitional layout contract")
    manifest_path, _, _, inventory_sha256 = _verify_live_state(
        boundary, require_complete_retained=True
    )
    flat_receipt_path, flat_receipt = _verify_flat_migration_receipt(
        root, inventory_sha256=inventory_sha256
    )
    implementation = [
        {"path": relative, "sha256": sha256_file(root / relative)}
        for relative in IMPLEMENTATION_PATHS
    ]
    return {
        "delete_layout": "data/dbn/{family}/{market}/{year}/{release-id}/{filename}",
        "destination_layout": "data/dbn/{family}/{market}/{year}/{filename}",
        "expected_dbn_files": EXPECTED_DBN_FILES,
        "expected_release_directories": EXPECTED_RELEASE_DIRECTORIES,
        "expected_sidecar_files": EXPECTED_SIDECAR_FILES,
        "expected_total_bytes": EXPECTED_BYTES,
        "expected_total_files": EXPECTED_FILES,
        "flat_migration_receipt_id": flat_receipt["receipt_id"],
        "flat_migration_receipt_path": flat_receipt_path.relative_to(root).as_posix(),
        "flat_migration_receipt_sha256": sha256_file(flat_receipt_path),
        "implementation_files": implementation,
        "implementation_sha256": sha256_json(implementation),
        "inventory_sha256": inventory_sha256,
        "layout_contract_sha256": sha256_file(contract_path),
        "operation": OPERATION,
        "preserve_external_archive": True,
        "preserve_flat_files": True,
        "preserve_vault": True,
        "repository_root": str(root),
        "source_manifest_path": manifest_path.relative_to(root).as_posix(),
        "source_manifest_sha256": sha256_file(manifest_path),
        "source_release_id": CANONICAL_RELEASE_ID,
    }


def build_plan(scope: Mapping[str, object]) -> dict[str, object]:
    core = {"cutover_plan_version": PLAN_VERSION, "scope": dict(scope)}
    return {**core, "cutover_plan_id": sha256_json(core)}


def build_approval_draft(plan: Mapping[str, object]) -> dict[str, object]:
    return {
        "approval_receipt_id": None,
        "approval_version": APPROVAL_VERSION,
        "approved_at": None,
        "cutover_plan_id": plan.get("cutover_plan_id"),
        "operation": OPERATION,
        "scope": plan.get("scope"),
        "status": "PENDING_APPROVAL",
        "user_authorization_id": None,
    }


def verify_approval(approval: Mapping[str, object], plan: Mapping[str, object]) -> str:
    core = {
        "approval_version": APPROVAL_VERSION,
        "approved_at": approval.get("approved_at"),
        "cutover_plan_id": plan.get("cutover_plan_id"),
        "operation": OPERATION,
        "scope": plan.get("scope"),
        "status": "APPROVED",
        "user_authorization_id": approval.get("user_authorization_id"),
    }
    expected = {**core, "approval_receipt_id": approval.get("approval_receipt_id")}
    if (
        dict(approval) != expected
        or type(core["approved_at"]) is not str
        or re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", core["approved_at"]
        )
        is None
        or type(core["user_authorization_id"]) is not str
        or _SHA256.fullmatch(core["user_authorization_id"]) is None
        or approval.get("approval_receipt_id") != sha256_json(core)
    ):
        raise UnauthorizedOperation("DBN cutover lacks exact hash-bound approval")
    return str(approval["approval_receipt_id"])


def _plan_identity(plan: Mapping[str, object]) -> str:
    core = {"cutover_plan_version": PLAN_VERSION, "scope": plan.get("scope")}
    plan_id = plan.get("cutover_plan_id")
    if type(plan_id) is not str or plan_id != sha256_json(core):
        raise UnauthorizedOperation("DBN cutover plan identity is invalid")
    return plan_id


def _intent_path(boundary: RepoBoundary, plan_id: str) -> Path:
    return boundary.active_root / STATE_ROOT / plan_id / "intent.json"


def _receipt_path(boundary: RepoBoundary, plan_id: str) -> Path:
    return boundary.active_root / RECEIPT_ROOT / f"{plan_id}.json"


def _intent(
    *, plan: Mapping[str, object], approval_id: str, state: str = "PREPARED"
) -> dict[str, object]:
    core = {
        "approval_receipt_id": approval_id,
        "cutover_plan_id": plan["cutover_plan_id"],
        "intent_version": INTENT_VERSION,
        "scope": plan["scope"],
        "state": state,
    }
    return {**core, "intent_id": sha256_json(core)}


def _verify_intent(
    payload: Mapping[str, object], *, plan: Mapping[str, object], approval_id: str
) -> None:
    expected = _intent(plan=plan, approval_id=approval_id)
    if dict(payload) != expected:
        raise IntegrityError("DBN cutover intent is invalid or conflicts")


def _delete_retained(path: Path) -> None:
    path.unlink()
    fsync_directory(path.parent)


def _verify_receipt_payload(
    receipt: Mapping[str, object], *, plan: Mapping[str, object], approval_id: str
) -> None:
    core = {key: value for key, value in receipt.items() if key != "receipt_id"}
    if (
        set(receipt)
        != {
            "approval_receipt_id",
            "completed_at",
            "cutover_plan_id",
            "deleted_bytes",
            "deleted_files",
            "deleted_release_directories",
            "flat_migration_receipt_sha256",
            "inventory_sha256",
            "receipt_id",
            "receipt_version",
            "source_manifest_sha256",
            "source_release_id",
            "status",
        }
        or receipt.get("approval_receipt_id") != approval_id
        or receipt.get("cutover_plan_id") != plan.get("cutover_plan_id")
        or receipt.get("deleted_bytes") != EXPECTED_BYTES
        or receipt.get("deleted_files") != EXPECTED_FILES
        or receipt.get("deleted_release_directories")
        != EXPECTED_RELEASE_DIRECTORIES
        or receipt.get("inventory_sha256")
        != dict(plan.get("scope", {})).get("inventory_sha256")
        or receipt.get("receipt_version") != RECEIPT_VERSION
        or receipt.get("source_release_id") != CANONICAL_RELEASE_ID
        or receipt.get("status") != "COMPLETE_VERIFIED_DESTRUCTIVE_CUTOVER"
        or receipt.get("receipt_id") != sha256_json(core)
    ):
        raise IntegrityError("DBN cutover receipt is invalid")


def verify_post_cutover(
    *,
    repository_root: Path,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
    receipt: Mapping[str, object],
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(active_root=root)
    plan_id = _plan_identity(plan)
    approval_id = verify_approval(approval, plan)
    _verify_receipt_payload(receipt, plan=plan, approval_id=approval_id)
    manifest_path, manifest = _manifest(boundary)
    scope = dict(plan["scope"])
    if sha256_file(manifest_path) != scope.get("source_manifest_sha256"):
        raise IntegrityError("canonical DBN manifest changed after cutover")
    _verify_pairing(manifest)
    bindings = _bindings(manifest, boundary)
    expected_flat = {item.flat for item in bindings}
    observed_flat, observed_retained, observed_directories = _observed_dbn_tree(root)
    if (
        observed_flat != expected_flat
        or observed_retained
        or observed_directories
        or len(bindings) != EXPECTED_FILES
    ):
        raise IntegrityError("DBN tree is not flat-only after cutover")
    for item in bindings:
        _verify_file(item.flat, item.entry, label="post-cutover flat DBN file")
    receipt_path = _receipt_path(boundary, plan_id)
    if not receipt_path.is_file() or _load_json(receipt_path) != dict(receipt):
        raise IntegrityError("DBN cutover receipt path or bytes differ")
    return dict(receipt)


def execute(
    *,
    repository_root: Path,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(active_root=root)
    plan_id = _plan_identity(plan)
    approval_id = verify_approval(approval, plan)
    receipt_path = _receipt_path(boundary, plan_id)
    if receipt_path.exists():
        return verify_post_cutover(
            repository_root=root,
            plan=plan,
            approval=approval,
            receipt=_load_json(receipt_path),
        )
    active_locks = sorted(
        path.name
        for path in (root / "state" / "locks").glob("*.lock")
        if path.is_file() and path != root / LOCK_PATH
    )
    if active_locks:
        raise UnauthorizedOperation(
            "DBN cutover found active writer locks: " + ", ".join(active_locks)
        )
    operation = OperationReceipt.issue_local(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={"cutover_plan_id": plan_id, "source_release_id": CANONICAL_RELEASE_ID},
    )
    operation.verify(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
    )
    with FileLease(root / LOCK_PATH):
        intent_path = _intent_path(boundary, plan_id)
        if intent_path.exists():
            _verify_intent(
                _load_json(intent_path), plan=plan, approval_id=approval_id
            )
            _, manifest, bindings, _ = _verify_live_state(
                boundary, require_complete_retained=False
            )
        else:
            live_scope = build_scope(repository_root=root)
            if plan.get("scope") != live_scope:
                raise UnauthorizedOperation("DBN cutover plan differs from live inputs")
            _, manifest, bindings, _ = _verify_live_state(
                boundary, require_complete_retained=True
            )
            _write_new_or_exact(
                intent_path, _intent(plan=plan, approval_id=approval_id)
            )
        for item in bindings:
            _verify_file(item.flat, item.entry, label="flat DBN file before deletion")
            if item.retained.exists():
                _verify_file(
                    item.retained, item.entry, label="retained DBN file before deletion"
                )
                _delete_retained(item.retained)
        for directory in sorted(
            {item.retained.parent for item in bindings},
            key=lambda value: value.as_posix(),
        ):
            if directory.exists():
                if any(directory.iterdir()):
                    raise IntegrityError(
                        f"retained DBN directory is not empty after exact deletion: {directory}"
                    )
                directory.rmdir()
                fsync_directory(directory.parent)
        observed_flat, observed_retained, observed_directories = _observed_dbn_tree(root)
        if (
            observed_flat != {item.flat for item in bindings}
            or observed_retained
            or observed_directories
        ):
            raise IntegrityError("DBN tree did not reach the exact flat-only state")
        manifest_path, _ = _manifest(boundary)
        scope = dict(plan["scope"])
        core = {
            "approval_receipt_id": approval_id,
            "completed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "cutover_plan_id": plan_id,
            "deleted_bytes": EXPECTED_BYTES,
            "deleted_files": EXPECTED_FILES,
            "deleted_release_directories": EXPECTED_RELEASE_DIRECTORIES,
            "flat_migration_receipt_sha256": scope["flat_migration_receipt_sha256"],
            "inventory_sha256": scope["inventory_sha256"],
            "receipt_version": RECEIPT_VERSION,
            "source_manifest_sha256": sha256_file(manifest_path),
            "source_release_id": CANONICAL_RELEASE_ID,
            "status": "COMPLETE_VERIFIED_DESTRUCTIVE_CUTOVER",
        }
        receipt = {**core, "receipt_id": sha256_json(core)}
        _write_new_or_exact(receipt_path, receipt)
        _write_new_or_exact(
            intent_path.parent / "completion.json",
            {
                "cutover_plan_id": plan_id,
                "receipt_id": receipt["receipt_id"],
                "receipt_sha256": sha256_file(receipt_path),
                "state": "COMPLETE",
            },
        )
        return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("plan", "execute", "verify"):
        item = commands.add_parser(command)
        item.add_argument("--repository-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.repository_root.resolve(strict=True)
    if args.command == "plan":
        plan = build_plan(build_scope(repository_root=root))
        print(
            json.dumps(
                {"approval_draft": build_approval_draft(plan), "plan": plan},
                sort_keys=True,
            )
        )
        return 0
    plan = _load_json(root / PLAN_PATH)
    approval = _load_json(root / APPROVAL_PATH)
    if args.command == "execute":
        receipt = execute(repository_root=root, plan=plan, approval=approval)
    else:
        plan_id = _plan_identity(plan)
        receipt = verify_post_cutover(
            repository_root=root,
            plan=plan,
            approval=approval,
            receipt=_load_json(_receipt_path(RepoBoundary(active_root=root), plan_id)),
        )
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
