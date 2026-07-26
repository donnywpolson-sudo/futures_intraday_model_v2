"""Subprocess worker used to prove resumability across uncatchable exits."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import futures_rebuild.migration as migration
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)


def main() -> int:
    if len(sys.argv) != 5:
        raise SystemExit("manifest approval inventory_sha256 kill_phase required")
    # This helper runs outside pytest's monkeypatch process and exercises only
    # synthetic crash/recovery mechanics in a temporary repository.
    migration._validate_controlled_rebuild_authorization = (
        lambda: migration.CONTROLLED_REBUILD_AUTHORIZATION_ID
    )
    manifest_path = Path(sys.argv[1])
    approval_path = Path(sys.argv[2])
    approved_inventory = sys.argv[3]
    kill_phase = sys.argv[4]
    manifest, manifest_hash = migration.load_manifest(manifest_path)
    approval = migration.read_migration_approval(approval_path)
    boundary = RepoBoundary(
        Path(str(manifest["destination_root"])).parents[3].resolve(),
        (Path(str(manifest["source_root"])).resolve(),),
        (),
    )
    receipt = OperationReceipt.issue_local(
        boundary,
        operation="COPY_SOURCE_SNAPSHOT",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
        scope=migration.migration_authorization_scope(
            manifest, manifest_hash, approved_inventory, approval
        ),
    )
    if kill_phase == "mid_copy":
        original_copyfile = migration.shutil.copyfile

        def kill_after_temp_copy(source: object, destination: object) -> object:
            result = original_copyfile(source, destination)
            os._exit(91)
            return result

        migration.shutil.copyfile = kill_after_temp_copy
    elif kill_phase == "pre_rename":
        original_rename = migration.os.rename

        def kill_before_publication(source: object, destination: object) -> object:
            if Path(source) == Path(str(manifest["destination_root"])):
                os._exit(92)
            return original_rename(source, destination)

        migration.os.rename = kill_before_publication
    elif kill_phase == "post_rename":
        original_checkpoint = migration._write_checkpoint

        def kill_before_published_checkpoint(path: Path, payload: dict[str, object]) -> None:
            if payload.get("status") == "PUBLISHED":
                os._exit(93)
            original_checkpoint(path, payload)

        migration._write_checkpoint = kill_before_published_checkpoint
    else:
        raise SystemExit(f"unknown kill phase: {kill_phase}")
    migration.guarded_copy(
        manifest,
        manifest_hash,
        manifest_hash,
        approved_inventory,
        migration_approval=approval,
        boundary=boundary,
        operation_receipt=receipt,
    )
    raise SystemExit("worker unexpectedly completed")


if __name__ == "__main__":
    raise SystemExit(main())
