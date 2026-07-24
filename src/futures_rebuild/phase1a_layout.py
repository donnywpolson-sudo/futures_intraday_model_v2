"""Approval-gated copy of verified DBNs into the phase-first layout."""

from __future__ import annotations

import json
import re
import shutil
import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
    verify_data_release_manifest,
    verify_layout_contract,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .migration import verify_published_source_snapshot


MIGRATION_PLAN_VERSION = "1.0.0"
MIGRATION_APPROVAL_VERSION = "1.0.0"
MIGRATION_OPERATION = "COPY_VERIFIED_DBN_TO_PHASE_FIRST_LAYOUT_V2"
DBN_RELEASE_KIND = "futures_phase1a_verified_dbn"
DBN_SCHEMA_VERSION = "1.0.0"
EXPECTED_DBN_FILES = 4_020
EXPECTED_SIDECAR_FILES = 4_020
EXPECTED_TOTAL_FILES = EXPECTED_DBN_FILES + EXPECTED_SIDECAR_FILES
EXPECTED_TOTAL_BYTES = 25_007_876_004
_DBN_PATH = re.compile(
    r"^dbn/(?P<family>definition|ohlcv_1d|ohlcv_1h|ohlcv_1m|ohlcv_1s|statistics|status|trades)/"
    r"(?P<market>[0-9A-Z]{2,3})/(?P<year>\d{4})/(?P<filename>[^/]+)$"
)
IMPLEMENTATION_PATHS = (
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/locking.py",
    "src/futures_rebuild/migration.py",
    "src/futures_rebuild/phase1a_layout.py",
)
PLAN_PATH = Path("configs/data_layout_migration_plan.json")
APPROVAL_PATH = Path("configs/data_layout_migration_approval.json")


@dataclass(frozen=True)
class DbnSourceFile:
    source_relative_path: str
    logical_path: str
    size: int
    sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "logical_path": self.logical_path,
            "sha256": self.sha256,
            "size": self.size,
            "source_relative_path": self.source_relative_path,
        }


def dbn_files_from_snapshot_receipt(
    receipt: Mapping[str, object],
) -> tuple[DbnSourceFile, ...]:
    raw_files = receipt.get("files")
    if not isinstance(raw_files, list):
        raise IntegrityError("source snapshot receipt has no exact file index")
    files: list[DbnSourceFile] = []
    for raw in raw_files:
        if not isinstance(raw, dict) or set(raw) != {"path", "sha256", "size"}:
            raise IntegrityError("source snapshot file record is invalid")
        source_path = raw["path"]
        if not isinstance(source_path, str) or not source_path.startswith("dbn/"):
            continue
        match = _DBN_PATH.fullmatch(source_path)
        if match is None:
            raise IntegrityError(f"DBN source path is outside the phase layout: {source_path}")
        size = raw["size"]
        sha256 = raw["sha256"]
        if (
            isinstance(size, bool)
            or type(size) is not int
            or size < 0
            or type(sha256) is not str
            or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
        ):
            raise IntegrityError("DBN source file size or hash is invalid")
        files.append(
            DbnSourceFile(
                source_relative_path=source_path,
                logical_path=f"data/{source_path}",
                size=size,
                sha256=sha256,
            )
        )
    files.sort(key=lambda item: item.source_relative_path)
    if len(files) != EXPECTED_TOTAL_FILES:
        raise IntegrityError("DBN source file count differs from the frozen inventory")
    dbn_count = sum(not item.source_relative_path.endswith(".manifest.json") for item in files)
    sidecar_count = len(files) - dbn_count
    total_bytes = sum(item.size for item in files)
    if (
        dbn_count != EXPECTED_DBN_FILES
        or sidecar_count != EXPECTED_SIDECAR_FILES
        or total_bytes != EXPECTED_TOTAL_BYTES
    ):
        raise IntegrityError("DBN source inventory totals differ from the frozen contract")
    return tuple(files)


def build_migration_scope(
    *,
    snapshot_root: Path,
    repository_root: Path,
    layout_contract_path: Path,
    archive_manifest_path: Path,
) -> dict[str, object]:
    repository_root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(active_root=repository_root)
    snapshot_root = boundary.assert_snapshot_path(snapshot_root)
    expected_contract = repository_root / "configs" / "data_layout_contract.json"
    if layout_contract_path.resolve(strict=True) != expected_contract.resolve(strict=True):
        raise IntegrityError("layout migration contract path is not the tracked contract")
    verify_layout_contract(layout_contract_path)
    receipt_path = snapshot_root / "SOURCE_SNAPSHOT_RECEIPT.json"
    receipt = verify_published_source_snapshot(snapshot_root)
    files = dbn_files_from_snapshot_receipt(receipt)
    inventory = [item.as_dict() for item in files]
    archive_manifest = verify_data_release_manifest(
        archive_manifest_path, boundary, verify_files=False
    )
    archive_receipt = archive_manifest.embedded_documents.get("archive_receipt")
    if (
        archive_manifest.phase != "migration"
        or archive_manifest.release_kind != "futures_layout_v1_vault_archive_receipt"
        or not isinstance(archive_receipt, dict)
        or archive_receipt.get("status") != "COMPLETE_VERIFIED_COPY_ONLY"
        or any(
            type(archive_receipt.get(name)) is not str
            or re.fullmatch(r"[0-9a-f]{64}", archive_receipt[name]) is None
            for name in ("archive_receipt_id", "tree_sha256")
        )
        or archive_receipt.get("source_root")
        != str((repository_root / "data" / "vault").resolve(strict=True))
        or type(archive_receipt.get("archive_root")) is not str
        or type(archive_receipt.get("total_bytes")) is not int
        or archive_receipt["total_bytes"] < EXPECTED_TOTAL_BYTES
        or type(archive_receipt.get("total_files")) is not int
        or archive_receipt["total_files"] < EXPECTED_TOTAL_FILES
    ):
        raise IntegrityError("layout migration requires a verified vault archive receipt")
    archive_root = Path(archive_receipt["archive_root"]).resolve(strict=True)
    try:
        archive_root.relative_to(repository_root)
    except ValueError:
        pass
    else:
        raise IntegrityError("vault archive must be external to the active repository")
    archive_relative = archive_manifest_path.resolve(strict=True).relative_to(
        repository_root.resolve(strict=True)
    ).as_posix()
    implementation_inventory = [
        {
            "path": relative,
            "sha256": sha256_file(repository_root / relative),
        }
        for relative in IMPLEMENTATION_PATHS
    ]
    return {
        "archive_manifest_path": archive_relative,
        "archive_manifest_sha256": sha256_file(archive_manifest_path),
        "archive_receipt_id": archive_receipt.get("archive_receipt_id"),
        "archive_release_id": archive_manifest.release_id,
        "archive_tree_sha256": archive_receipt.get("tree_sha256"),
        "destination_layout": (
            "data/dbn/{family}/{market}/{year}/{release-id}/{original-filename}"
        ),
        "expected_dbn_files": EXPECTED_DBN_FILES,
        "expected_sidecar_files": EXPECTED_SIDECAR_FILES,
        "expected_total_bytes": EXPECTED_TOTAL_BYTES,
        "expected_total_files": EXPECTED_TOTAL_FILES,
        "maximum_total_bytes": EXPECTED_TOTAL_BYTES,
        "maximum_total_files": EXPECTED_TOTAL_FILES,
        "implementation_files": implementation_inventory,
        "implementation_sha256": sha256_json(implementation_inventory),
        "inventory_sha256": sha256_json(inventory),
        "layout_contract_sha256": sha256_file(layout_contract_path),
        "operation": MIGRATION_OPERATION,
        "repository_root": str(repository_root.resolve(strict=True)),
        "source_receipt_sha256": sha256_file(receipt_path),
        "source_snapshot_id": receipt["source_snapshot_id"],
        "source_snapshot_root": str(snapshot_root.resolve(strict=True)),
        "staging_root": "state/data_publication_staging",
    }


def build_migration_plan(scope: Mapping[str, object]) -> dict[str, object]:
    core = {
        "migration_plan_version": MIGRATION_PLAN_VERSION,
        "scope": dict(scope),
    }
    return {**core, "migration_plan_id": sha256_json(core)}


def build_approval_draft(plan: Mapping[str, object]) -> dict[str, object]:
    return {
        "approval_receipt_id": None,
        "approval_version": MIGRATION_APPROVAL_VERSION,
        "approved_at": None,
        "migration_plan_id": plan["migration_plan_id"],
        "operation": MIGRATION_OPERATION,
        "scope": plan["scope"],
        "status": "PENDING_APPROVAL",
        "user_authorization_id": None,
    }


def verify_approval(
    approval: Mapping[str, object], plan: Mapping[str, object]
) -> str:
    approved_at = approval.get("approved_at")
    user_authorization_id = approval.get("user_authorization_id")
    core = {
        "approval_version": MIGRATION_APPROVAL_VERSION,
        "approved_at": approved_at,
        "migration_plan_id": plan.get("migration_plan_id"),
        "operation": MIGRATION_OPERATION,
        "scope": plan.get("scope"),
        "status": "APPROVED",
        "user_authorization_id": user_authorization_id,
    }
    if (
        dict(approval) != {**core, "approval_receipt_id": approval.get("approval_receipt_id")}
        or approval.get("status") != "APPROVED"
        or type(approved_at) is not str
        or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", approved_at) is None
        or type(user_authorization_id) is not str
        or re.fullmatch(r"[0-9a-f]{64}", user_authorization_id) is None
        or approval.get("approval_receipt_id") != sha256_json(core)
    ):
        raise UnauthorizedOperation("layout migration lacks an exact hash-bound approval")
    return str(approval["approval_receipt_id"])


def execute_dbn_layout_copy(
    *,
    snapshot_root: Path,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
    boundary: RepoBoundary,
    operation_receipt: OperationReceipt,
) -> DataReleaseReceipt:
    lock_root = boundary.active_root / "state" / "locks"
    active_locks = sorted(path.name for path in lock_root.glob("*.lock") if path.is_file())
    if active_locks:
        raise UnauthorizedOperation(
            "layout migration coordination gate found active lock files: "
            + ", ".join(active_locks)
        )
    approval_id = verify_approval(approval, plan)
    live_scope = build_migration_scope(
        snapshot_root=snapshot_root,
        repository_root=boundary.active_root,
        layout_contract_path=boundary.active_root / "configs" / "data_layout_contract.json",
        archive_manifest_path=(
            boundary.active_root
            / str(dict(plan.get("scope", {})).get("archive_manifest_path", ""))
        ),
    )
    if plan.get("scope") != live_scope:
        raise UnauthorizedOperation("layout migration plan differs from live immutable inputs")
    receipt = verify_published_source_snapshot(snapshot_root)
    files = dbn_files_from_snapshot_receipt(receipt)
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_receipt,
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )
    stage = publisher.create_stage("phase1a_dbn")
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    for item in files:
        source = snapshot_root / item.source_relative_path
        relative = item.source_relative_path.removeprefix("dbn/")
        target = stage / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        if target.stat().st_size != item.size or sha256_file(target) != item.sha256:
            raise IntegrityError("staged DBN differs from its verified source")
        logical_paths[relative] = item.logical_path
        staged_paths[item.logical_path] = relative
    manifest = DataReleaseManifest.build(
        stage,
        phase="dbn",
        release_kind=DBN_RELEASE_KIND,
        schema_version=DBN_SCHEMA_VERSION,
        logical_paths=logical_paths,
        embedded_documents={
            "phase1a_receipt": {
                "approval_receipt_id": approval_id,
                "source_snapshot_id": receipt["source_snapshot_id"],
                "status": "COMPLETE_VERIFIED_IMMUTABLE",
                "total_bytes": EXPECTED_TOTAL_BYTES,
                "total_files": EXPECTED_TOTAL_FILES,
            }
        },
        metadata={
            "approval_receipt_id": approval_id,
            "inventory_sha256": live_scope["inventory_sha256"],
            "source_snapshot_id": receipt["source_snapshot_id"],
        },
    )
    manifest_path = publisher.publish(
        stage, manifest, staged_paths=staged_paths
    )
    return DataReleaseReceipt.from_manifest(manifest_path, boundary)


def canonical_json(payload: Mapping[str, object]) -> bytes:
    return canonical_bytes(dict(payload)) + b"\n"


def load_json(path: Path) -> dict[str, object]:
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ContractError(f"invalid JSON: {path}") from exc
    if not isinstance(payload, dict) or raw != canonical_json(payload):
        raise ContractError(f"JSON is not a canonical object: {path}")
    return payload


def _write_new_or_exact(path: Path, payload: Mapping[str, object]) -> None:
    encoded = canonical_json(payload)
    if path.exists():
        if path.read_bytes() != encoded:
            raise IntegrityError(f"existing file conflicts with generated content: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encoded)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    plan = subcommands.add_parser("plan", help="generate an exact pending approval")
    plan.add_argument("--repository-root", type=Path, required=True)
    plan.add_argument("--source-snapshot-root", type=Path, required=True)
    plan.add_argument("--plan", type=Path, required=True)
    plan.add_argument("--approval", type=Path, required=True)
    plan.add_argument("--archive-manifest", type=Path, required=True)
    execute = subcommands.add_parser("execute", help="copy after exact approval")
    execute.add_argument("--repository-root", type=Path, required=True)
    execute.add_argument("--source-snapshot-root", type=Path, required=True)
    execute.add_argument("--plan", type=Path, required=True)
    execute.add_argument("--approval", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.repository_root.resolve(strict=True)
    boundary = RepoBoundary(active_root=root)
    plan_path = args.plan.resolve(strict=False)
    approval_path = args.approval.resolve(strict=False)
    if (
        plan_path != (root / PLAN_PATH).resolve(strict=False)
        or approval_path != (root / APPROVAL_PATH).resolve(strict=False)
    ):
        raise UnauthorizedOperation("layout migration artifacts must use their tracked config paths")
    if args.command == "plan":
        scope = build_migration_scope(
            snapshot_root=args.source_snapshot_root,
            repository_root=root,
            layout_contract_path=root / "configs" / "data_layout_contract.json",
            archive_manifest_path=args.archive_manifest,
        )
        plan = build_migration_plan(scope)
        approval = build_approval_draft(plan)
        _write_new_or_exact(plan_path, plan)
        _write_new_or_exact(approval_path, approval)
        print(json.dumps({"migration_plan_id": plan["migration_plan_id"], "status": "PENDING_APPROVAL"}, sort_keys=True))
        return 0
    plan = load_json(plan_path)
    approval = load_json(approval_path)
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={
            "migration_plan_id": str(plan.get("migration_plan_id")),
            "phase": "dbn",
        },
    )
    receipt = execute_dbn_layout_copy(
        snapshot_root=args.source_snapshot_root,
        plan=plan,
        approval=approval,
        boundary=boundary,
        operation_receipt=operation,
    )
    print(json.dumps(receipt.as_dict(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
