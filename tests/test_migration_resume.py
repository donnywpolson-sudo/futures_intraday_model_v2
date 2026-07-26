import json
import os
from pathlib import Path
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import (
    ContractError,
    IntegrityError,
    LeaseBusy,
    UnauthorizedOperation,
)
import futures_rebuild.locking as locking_module
from futures_rebuild.locking import FileLease
import futures_rebuild.migration as migration_module
from futures_rebuild.migration import (
    MIGRATION_APPROVAL_DRAFT_OPERATION,
    MIGRATION_APPROVAL_DRAFT_STATUS,
    MIGRATION_INVENTORY_REVIEW_OPERATION,
    MIGRATION_INVENTORY_REVIEW_STATUS,
    MIGRATION_STALE_LOCK_RECOVERY_OPERATION,
    MigrationApproval,
    approval_payload_for_review,
    guarded_copy,
    inventory,
    load_detailed_inventory_review,
    load_manifest,
    migration_approval_draft_scope,
    migration_authorization_scope,
    migration_inventory_review_scope,
    migration_recovery_scope,
    recover_stale_migration_lock,
    verify_published_source_snapshot,
    write_detailed_inventory_review,
    write_migration_approval_draft,
)


@pytest.fixture(autouse=True)
def _synthetic_migration_authorization(monkeypatch) -> None:
    """Keep synthetic copy mechanics independent of the Desktop checkout path."""

    monkeypatch.setattr(
        migration_module,
        "_validate_controlled_rebuild_authorization",
        lambda: migration_module.CONTROLLED_REBUILD_AUTHORIZATION_ID,
    )


def _manifest(tmp_path: Path, *, authorized: bool, direct_only: bool = False) -> Path:
    source = tmp_path / "legacy"
    active = tmp_path / "new"
    destination = active / "data" / "vault" / ".staging" / "import"
    source.mkdir(parents=True, exist_ok=True)
    entry = {
        "family": "raw" if direct_only else "dbn",
        "source": "raw" if direct_only else "dbn",
        "destination": "comparison/raw" if direct_only else "dbn",
        "disposition": "comparison" if direct_only else "authoritative",
    }
    if direct_only:
        entry.update({
            "include_regex": r"^[^_/][^/]*/\d{4}\.parquet$",
            "exclude_regexes": [r"(^|/)_refresh_"],
            "expected_files": 1,
            "expected_excluded_files": 1,
        })
    payload = {
        "migration_id": "synthetic_resume",
        "source_root": str(source.resolve()),
        "destination_root": str(destination.resolve()),
        "publication_root": str((active / "data" / "vault" / "source_snapshots").resolve()),
        "state_root": str((active / "state" / "migrations").resolve()),
        "lock_path": str((active / "state" / "locks" / "migration.lock").resolve()),
        "copy_authorized": authorized,
        "policy": {
            "operation": "copy_only",
            "overwrite": False,
            "follow_links": False,
            "require_source_stable_during_copy": True,
            "verify_destination_sha256": True,
        },
        "entries": [entry],
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _authority(manifest, digest, approved_inventory, approval):
    boundary = RepoBoundary(
        Path(str(manifest["destination_root"])).parents[3].resolve(),
        (Path(str(manifest["source_root"])).resolve(),),
        (),
    )
    receipt = OperationReceipt.issue_local(
        boundary,
        operation="COPY_SOURCE_SNAPSHOT",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
        scope=migration_authorization_scope(
            manifest, digest, approved_inventory, approval
        ),
    )
    return boundary, receipt


def _approval(manifest, digest) -> tuple[dict[str, object], MigrationApproval]:
    source_inventory = inventory(manifest, digest, detailed=True)
    approval = MigrationApproval.from_dict(
        approval_payload_for_review(
            manifest_hash=digest,
            source_inventory=source_inventory,
            approved_at="2026-07-15T00:00:00Z",
        )
    )
    return source_inventory, approval


def _copy(manifest, digest, approved_inventory):
    source_inventory = inventory(manifest, digest)
    approval = MigrationApproval.from_dict(
        approval_payload_for_review(
            manifest_hash=digest,
            source_inventory=source_inventory,
            approved_at="2026-07-15T00:00:00Z",
        )
    )
    boundary, receipt = _authority(
        manifest, digest, approved_inventory, approval
    )
    return guarded_copy(
        manifest,
        digest,
        digest,
        approved_inventory,
        migration_approval=approval,
        boundary=boundary,
        operation_receipt=receipt,
    )


def test_direct_market_year_policy_excludes_refresh_tree(tmp_path) -> None:
    path = _manifest(tmp_path, authorized=False, direct_only=True)
    source = tmp_path / "legacy" / "raw"
    (source / "ES").mkdir(parents=True)
    (source / "ES" / "2026.parquet").write_bytes(b"canonical")
    refresh = source / "_refresh_backups" / "run" / "ES"
    refresh.mkdir(parents=True)
    (refresh / "2026.parquet").write_bytes(b"refresh")
    manifest, digest = load_manifest(path)
    plan = inventory(manifest, digest)
    detailed_plan = inventory(manifest, digest, detailed=True)
    family = plan["families"][0]
    assert family["file_count"] == 1
    assert family["excluded_files"] == 1
    assert "files" not in family
    assert detailed_plan["inventory_sha256"] == plan["inventory_sha256"]
    assert len(detailed_plan["families"][0]["files"]) == 1


def test_copy_checkpoint_resume_and_idempotent_destination_verification(tmp_path) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    (source / "sample.dbn.zst").write_bytes(b"immutable")
    manifest, digest = load_manifest(path)
    approved_inventory = inventory(manifest, digest)["inventory_sha256"]
    first = _copy(manifest, digest, approved_inventory)
    assert first["copied_files"] == 1 and first["verified_files"] == 0
    second = _copy(manifest, digest, approved_inventory)
    assert second["copied_files"] == 0 and second["verified_files"] == 1
    checkpoint = json.loads(Path(str(second["checkpoint"])).read_text(encoding="utf-8"))
    assert checkpoint["status"] == "PUBLISHED" and checkpoint["completed_files"] == 1
    publication = Path(str(first["publication_path"]))
    assert publication.name == first["source_snapshot_id"]
    assert (publication / "SOURCE_SNAPSHOT_RECEIPT.json").exists()
    assert not (tmp_path / "new" / "stage").exists()


def test_copy_rejects_unapproved_inventory_without_writing(tmp_path) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    (source / "sample.dbn.zst").write_bytes(b"immutable")
    manifest, digest = load_manifest(path)
    with pytest.raises(Exception, match="inventory hash"):
        _copy(manifest, digest, "0" * 64)
    assert not (tmp_path / "new" / "stage").exists()


def test_crash_after_receipt_write_resumes_publication(tmp_path, monkeypatch) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    (source / "sample.dbn.zst").write_bytes(b"immutable")
    manifest, digest = load_manifest(path)
    approved_inventory = inventory(manifest, digest)["inventory_sha256"]
    original_rename = migration_module.os.rename
    raised = False

    def fail_publication_once(source_path, target_path):
        nonlocal raised
        if Path(source_path) == Path(str(manifest["destination_root"])) and not raised:
            raised = True
            raise OSError("synthetic crash before publication rename")
        return original_rename(source_path, target_path)

    monkeypatch.setattr(migration_module.os, "rename", fail_publication_once)
    with pytest.raises(OSError, match="synthetic crash"):
        _copy(manifest, digest, approved_inventory)
    stage = Path(str(manifest["destination_root"]))
    assert (stage / "SOURCE_SNAPSHOT_RECEIPT.json").is_file()
    monkeypatch.setattr(migration_module.os, "rename", original_rename)
    resumed = _copy(manifest, digest, approved_inventory)
    assert resumed["status"] == "PUBLISHED" and not stage.exists()


def test_crash_inside_uncheckpointed_batch_verifies_and_resumes(tmp_path, monkeypatch) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    for index in range(3):
        (source / f"{index}.dbn.zst").write_bytes(f"immutable-{index}".encode())
    manifest, digest = load_manifest(path)
    approved_inventory = inventory(manifest, digest)["inventory_sha256"]
    monkeypatch.setattr(migration_module, "CHECKPOINT_BATCH_FILES", 25)
    original_copyfile = migration_module.shutil.copyfile
    calls = 0

    def crash_on_second_copy(source_path, target_path):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("synthetic batch crash")
        return original_copyfile(source_path, target_path)

    monkeypatch.setattr(migration_module.shutil, "copyfile", crash_on_second_copy)
    with pytest.raises(OSError, match="batch crash"):
        _copy(manifest, digest, approved_inventory)
    monkeypatch.setattr(migration_module.shutil, "copyfile", original_copyfile)
    resumed = _copy(manifest, digest, approved_inventory)
    assert resumed["status"] == "PUBLISHED"


def test_crash_after_publication_rename_before_checkpoint_resumes(
    tmp_path, monkeypatch
) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    (source / "sample.dbn.zst").write_bytes(b"immutable")
    manifest, digest = load_manifest(path)
    approved_inventory = inventory(manifest, digest)["inventory_sha256"]
    original = migration_module._write_checkpoint
    raised = False

    def crash_on_published_checkpoint(checkpoint_path, payload):
        nonlocal raised
        if payload.get("status") == "PUBLISHED" and not raised:
            raised = True
            raise OSError("synthetic post-rename crash")
        return original(checkpoint_path, payload)

    monkeypatch.setattr(migration_module, "_write_checkpoint", crash_on_published_checkpoint)
    with pytest.raises(OSError, match="post-rename crash"):
        _copy(manifest, digest, approved_inventory)
    assert not Path(str(manifest["destination_root"])).exists()
    monkeypatch.setattr(migration_module, "_write_checkpoint", original)
    resumed = _copy(manifest, digest, approved_inventory)
    assert resumed["status"] == "PUBLISHED"


def test_pinned_evidence_size_and_hash_fail_closed(tmp_path) -> None:
    path = _manifest(tmp_path, authorized=False)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    (source / "artifact.exe").write_bytes(b"artifact")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["entries"][0]["expected_files"] = 1
    payload["entries"][0]["expected_bytes"] = len(b"artifact")
    payload["entries"][0]["expected_sha256"] = "0" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")
    manifest, digest = load_manifest(path)
    with pytest.raises(IntegrityError):
        inventory(manifest, digest)


@pytest.mark.parametrize(
    "mutation",
    (
        "extra",
        "receipt_version",
        "status",
        "files",
        "files_index_sha256",
        "approval_id",
        "inventory_sha256",
        "manifest_sha256",
        "migration_implementation_sha256",
        "total_files",
        "total_bytes",
        "user_authorization_id",
    ),
)
def test_snapshot_receipt_exact_schema_and_all_semantics_are_identity_bound(
    tmp_path, mutation
) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    (source / "sample.dbn.zst").write_bytes(b"immutable")
    manifest, digest = load_manifest(path)
    approved_inventory = inventory(manifest, digest)["inventory_sha256"]
    copied = _copy(manifest, digest, approved_inventory)
    publication = Path(str(copied["publication_path"]))
    assert verify_published_source_snapshot(publication)["source_snapshot_id"] == publication.name
    receipt_path = publication / "SOURCE_SNAPSHOT_RECEIPT.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if mutation == "extra":
        receipt["unbound_role_override"] = "AUTHORITATIVE"
    elif mutation in {"receipt_version", "status"}:
        receipt[mutation] = "WRONG"
    elif mutation == "files":
        receipt["files"][0]["size"] += 1
    elif mutation in {"total_files", "total_bytes"}:
        receipt[mutation] += 1
    else:
        receipt[mutation] = "0" * 64
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(IntegrityError):
        verify_published_source_snapshot(publication)


def _replace_entries(path: Path, entries: list[dict[str, object]]) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["entries"] = entries
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_windows_normalized_destination_collision_fails_before_hashing(
    tmp_path, monkeypatch
) -> None:
    path = _manifest(tmp_path, authorized=False)
    first = tmp_path / "legacy" / "first" / "one.bin"
    second = tmp_path / "legacy" / "second" / "two.bin"
    first.parent.mkdir(parents=True)
    second.parent.mkdir(parents=True)
    first.write_bytes(b"one")
    second.write_bytes(b"two")
    _replace_entries(
        path,
        [
            {
                "family": "first",
                "kind": "file",
                "source": "first/one.bin",
                "destination": "dbn/Foo.dbn.zst",
                "disposition": "authoritative",
            },
            {
                "family": "second",
                "kind": "file",
                "source": "second/two.bin",
                "destination": "DBN/foo.dbn.zst",
                "disposition": "authoritative",
            },
        ],
    )
    manifest, digest = load_manifest(path)
    calls = 0

    def unexpected_hash(_: Path) -> str:
        nonlocal calls
        calls += 1
        raise AssertionError("content hashing must follow namespace validation")

    monkeypatch.setattr(migration_module, "sha256_file", unexpected_hash)
    with pytest.raises(IntegrityError, match="Windows-normalized"):
        inventory(manifest, digest)
    assert calls == 0


@pytest.mark.parametrize(
    "destination",
    (
        "dbn/AUX.txt",
        "dbn/name.",
        "dbn/name ",
        "dbn/file:stream",
        "source_snapshot_receipt.json",
    ),
)
def test_windows_unsafe_destination_fails_before_hashing(
    tmp_path, monkeypatch, destination
) -> None:
    path = _manifest(tmp_path, authorized=False)
    source = tmp_path / "legacy" / "evidence.bin"
    source.write_bytes(b"evidence")
    _replace_entries(
        path,
        [
            {
                "family": "evidence",
                "kind": "file",
                "source": "evidence.bin",
                "destination": destination,
                "disposition": "authoritative",
            }
        ],
    )
    manifest, digest = load_manifest(path)
    calls = 0

    def unexpected_hash(_: Path) -> str:
        nonlocal calls
        calls += 1
        raise AssertionError("content hashing must follow namespace validation")

    monkeypatch.setattr(migration_module, "sha256_file", unexpected_hash)
    with pytest.raises((ContractError, IntegrityError)):
        inventory(manifest, digest)
    assert calls == 0


@pytest.mark.parametrize("extra_kind", ("file", "directory"))
def test_existing_stage_rejects_every_unplanned_path_before_copy(
    tmp_path, extra_kind
) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    (source / "sample.dbn.zst").write_bytes(b"immutable")
    manifest, digest = load_manifest(path)
    source_inventory, approval = _approval(manifest, digest)
    approved_inventory = str(source_inventory["inventory_sha256"])
    stage = Path(str(manifest["destination_root"]))
    stage.mkdir(parents=True)
    extra = stage / "unplanned"
    if extra_kind == "file":
        extra.write_bytes(b"not allowlisted")
    else:
        extra.mkdir()
    boundary, receipt = _authority(
        manifest, digest, approved_inventory, approval
    )
    with pytest.raises(IntegrityError, match="unexpected files or directories"):
        guarded_copy(
            manifest,
            digest,
            digest,
            approved_inventory,
            migration_approval=approval,
            boundary=boundary,
            operation_receipt=receipt,
        )
    assert not (stage / "dbn" / "sample.dbn.zst").exists()


def test_final_source_rehash_detects_mutation_before_publication(
    tmp_path, monkeypatch
) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    source_file = source / "sample.dbn.zst"
    source_file.write_bytes(b"immutable")
    manifest, digest = load_manifest(path)
    source_inventory, approval = _approval(manifest, digest)
    approved_inventory = str(source_inventory["inventory_sha256"])
    boundary, receipt = _authority(
        manifest, digest, approved_inventory, approval
    )
    original = migration_module._verify_final_source_inventory

    def mutate_then_verify(
        frozen_manifest: dict[str, object],
        frozen_hash: str,
        expected_hash: str,
    ) -> None:
        source_file.write_bytes(b"mutated!!")
        original(frozen_manifest, frozen_hash, expected_hash)

    monkeypatch.setattr(
        migration_module, "_verify_final_source_inventory", mutate_then_verify
    )
    with pytest.raises(IntegrityError, match="changed after copy and before publication"):
        guarded_copy(
            manifest,
            digest,
            digest,
            approved_inventory,
            migration_approval=approval,
            boundary=boundary,
            operation_receipt=receipt,
        )
    assert Path(str(manifest["destination_root"])).exists()
    publication = Path(str(manifest["publication_root"]))
    assert not publication.exists() or not any(publication.iterdir())


def _recovery_receipt(
    manifest: dict[str, object],
    digest: str,
    inventory_hash: str,
    approval: MigrationApproval,
    token: str,
) -> tuple[RepoBoundary, OperationReceipt]:
    boundary = RepoBoundary(
        Path(str(manifest["destination_root"])).parents[3].resolve(),
        (Path(str(manifest["source_root"])).resolve(),),
        (),
    )
    receipt = OperationReceipt.issue_local(
        boundary,
        operation=MIGRATION_STALE_LOCK_RECOVERY_OPERATION,
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
        scope=migration_recovery_scope(
            manifest, digest, inventory_hash, approval, token
        ),
    )
    return boundary, receipt


@pytest.mark.parametrize(
    ("kill_phase", "return_code"),
    (("mid_copy", 91), ("pre_rename", 92), ("post_rename", 93)),
)
def test_os_exit_kill_windows_require_dead_owner_recovery_then_resume(
    tmp_path, monkeypatch, kill_phase, return_code
) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    (source / "sample.dbn.zst").write_bytes(b"immutable")
    manifest, digest = load_manifest(path)
    source_inventory, approval = _approval(manifest, digest)
    inventory_hash = str(source_inventory["inventory_sha256"])
    approval_path = tmp_path / "approval.json"
    approval_path.write_text(json.dumps(asdict(approval)), encoding="utf-8")
    repository_root = Path(__file__).resolve().parents[1]
    worker = repository_root / "tests" / "helpers" / "hard_kill_migration_worker.py"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(repository_root / "src")
    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            str(worker),
            str(path),
            str(approval_path),
            inventory_hash,
            kill_phase,
        ],
        cwd=repository_root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == return_code, completed.stderr
    lock_path = Path(str(manifest["lock_path"]))
    dead_lease = FileLease.inspect(lock_path)
    original_now = locking_module._utc_now
    monkeypatch.setattr(
        locking_module,
        "_utc_now",
        lambda: original_now() + timedelta(minutes=6),
    )
    boundary, recovery_receipt = _recovery_receipt(
        manifest, digest, inventory_hash, approval, dead_lease.token
    )
    recovered = recover_stale_migration_lock(
        manifest,
        digest,
        inventory_hash,
        migration_approval=approval,
        expected_token=dead_lease.token,
        boundary=boundary,
        operation_receipt=recovery_receipt,
    )
    assert recovered["status"] == "DEAD_OWNER_LEASE_QUARANTINED"
    assert not lock_path.exists()
    evidence = Path(str(recovered["recovery_evidence"]))
    recovery_record = Path(str(recovered["recovery_receipt"]))
    assert evidence.exists() and recovery_record.exists()
    assert FileLease.inspect(evidence) == dead_lease
    record = json.loads(recovery_record.read_text(encoding="utf-8"))
    assert record["stale_lock_token"] == dead_lease.token
    monkeypatch.setattr(locking_module, "_utc_now", original_now)
    resumed_boundary, resume_receipt = _authority(
        manifest, digest, inventory_hash, approval
    )
    resumed = guarded_copy(
        manifest,
        digest,
        digest,
        inventory_hash,
        migration_approval=approval,
        boundary=resumed_boundary,
        operation_receipt=resume_receipt,
    )
    assert resumed["status"] == "PUBLISHED"
    assert verify_published_source_snapshot(
        Path(str(resumed["publication_path"]))
    )["source_snapshot_id"] == resumed["source_snapshot_id"]


def test_stale_lock_recovery_refuses_a_live_owner(tmp_path) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    (source / "sample.dbn.zst").write_bytes(b"immutable")
    manifest, digest = load_manifest(path)
    source_inventory, approval = _approval(manifest, digest)
    inventory_hash = str(source_inventory["inventory_sha256"])
    lock_path = Path(str(manifest["lock_path"]))
    with FileLease(lock_path) as live:
        boundary, recovery_receipt = _recovery_receipt(
            manifest, digest, inventory_hash, approval, live.record.token
        )
        with pytest.raises(LeaseBusy, match="alive or its death cannot be proved"):
            recover_stale_migration_lock(
                manifest,
                digest,
                inventory_hash,
                migration_approval=approval,
                expected_token=live.record.token,
                boundary=boundary,
                operation_receipt=recovery_receipt,
            )
        assert lock_path.exists()


def test_stale_lock_recovery_requires_exact_token_bound_authority(tmp_path) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    (source / "sample.dbn.zst").write_bytes(b"immutable")
    manifest, digest = load_manifest(path)
    source_inventory, approval = _approval(manifest, digest)
    inventory_hash = str(source_inventory["inventory_sha256"])
    lock_path = Path(str(manifest["lock_path"]))
    with FileLease(lock_path) as live:
        boundary, wrong_receipt = _recovery_receipt(
            manifest, digest, inventory_hash, approval, "0" * 32
        )
        with pytest.raises(UnauthorizedOperation, match="scope"):
            recover_stale_migration_lock(
                manifest,
                digest,
                inventory_hash,
                migration_approval=approval,
                expected_token=live.record.token,
                boundary=boundary,
                operation_receipt=wrong_receipt,
            )
        assert lock_path.exists()


def test_review_outputs_are_canonical_contained_no_overwrite_and_non_authorizing(
    tmp_path,
) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    (source / "sample.dbn.zst").write_bytes(b"immutable")
    manifest, digest = load_manifest(path)
    boundary = RepoBoundary(
        Path(str(manifest["destination_root"])).parents[3].resolve(),
        (Path(str(manifest["source_root"])).resolve(),),
        (),
    )
    output = boundary.active_root / "state" / "migrations" / "inventory_review.json"
    review_receipt = OperationReceipt.issue_local(
        boundary,
        operation=MIGRATION_INVENTORY_REVIEW_OPERATION,
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
        scope=migration_inventory_review_scope(manifest, digest, output),
    )
    summary = write_detailed_inventory_review(
        manifest,
        digest,
        output,
        boundary=boundary,
        operation_receipt=review_receipt,
    )
    raw = output.read_bytes()
    review = json.loads(raw)
    assert raw == canonical_bytes(review) + b"\n"
    assert summary["status"] == MIGRATION_INVENTORY_REVIEW_STATUS
    assert review["execution_authorized"] is False
    assert review["inventory"]["families"][0]["files"]
    original_hash = sha256_file(output)
    with pytest.raises(IntegrityError, match="already exists"):
        write_detailed_inventory_review(
            manifest,
            digest,
            output,
            boundary=boundary,
            operation_receipt=review_receipt,
        )
    assert sha256_file(output) == original_hash
    loaded = load_detailed_inventory_review(
        manifest, digest, output, boundary=boundary
    )
    draft_output = (
        boundary.active_root / "state" / "migrations" / "approval_draft.json"
    )
    approved_at = "2026-07-15T00:00:00Z"
    draft_receipt = OperationReceipt.issue_local(
        boundary,
        operation=MIGRATION_APPROVAL_DRAFT_OPERATION,
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
        scope=migration_approval_draft_scope(
            manifest,
            digest,
            str(loaded["artifact_id"]),
            approved_at,
            draft_output,
        ),
    )
    draft_summary = write_migration_approval_draft(
        manifest,
        digest,
        output,
        approved_at,
        draft_output,
        boundary=boundary,
        operation_receipt=draft_receipt,
    )
    draft_raw = draft_output.read_bytes()
    draft = json.loads(draft_raw)
    assert draft_raw == canonical_bytes(draft) + b"\n"
    assert draft_summary["status"] == MIGRATION_APPROVAL_DRAFT_STATUS
    assert draft["execution_authorized"] is False
    assert draft["status"] == MIGRATION_APPROVAL_DRAFT_STATUS
    assert draft["proposed_tracked_artifact"]["execution_authorized"] is True
    assert not (boundary.active_root / "configs").exists()


def test_review_output_rejects_escape_before_inventory_hashing(
    tmp_path, monkeypatch
) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    (source / "sample.dbn.zst").write_bytes(b"immutable")
    manifest, digest = load_manifest(path)
    boundary = RepoBoundary(
        Path(str(manifest["destination_root"])).parents[3].resolve(),
        (Path(str(manifest["source_root"])).resolve(),),
        (),
    )
    output = boundary.active_root / "state" / "outside.json"
    receipt = OperationReceipt.issue_local(
        boundary,
        operation=MIGRATION_INVENTORY_REVIEW_OPERATION,
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
        scope=migration_inventory_review_scope(manifest, digest, output),
    )
    calls = 0

    def unexpected_inventory(*args: object, **kwargs: object) -> dict[str, object]:
        nonlocal calls
        calls += 1
        raise AssertionError("inventory must follow output containment")

    monkeypatch.setattr(migration_module, "inventory", unexpected_inventory)
    with pytest.raises(UnauthorizedOperation):
        write_detailed_inventory_review(
            manifest,
            digest,
            output,
            boundary=boundary,
            operation_receipt=receipt,
        )
    assert calls == 0 and not output.exists()


def test_cli_writes_review_and_approval_draft_without_dumping_file_index(
    tmp_path, capsys
) -> None:
    path = _manifest(tmp_path, authorized=True)
    source = tmp_path / "legacy" / "dbn"
    source.mkdir(parents=True)
    (source / "sample.dbn.zst").write_bytes(b"immutable")
    active = tmp_path / "new"
    review_path = active / "state" / "migrations" / "review.json"
    assert migration_module.main(
        [
            "--manifest",
            str(path),
            "--active-root",
            str(active),
            "--legacy-root",
            str(tmp_path / "legacy"),
            "--detailed-inventory-output",
            str(review_path),
        ]
    ) == 0
    review_summary = json.loads(capsys.readouterr().out)
    assert review_summary["execution_authorized"] is False
    assert "families" not in review_summary and "inventory" not in review_summary
    draft_path = active / "state" / "migrations" / "approval_draft.json"
    assert migration_module.main(
        [
            "--manifest",
            str(path),
            "--active-root",
            str(active),
            "--legacy-root",
            str(tmp_path / "legacy"),
            "--approval-draft-output",
            str(draft_path),
            "--inventory-review",
            str(review_path),
            "--approved-at",
            "2026-07-15T00:00:00Z",
        ]
    ) == 0
    draft_summary = json.loads(capsys.readouterr().out)
    assert draft_summary["execution_authorized"] is False
    assert "proposed_tracked_artifact" not in draft_summary
    assert json.loads(draft_path.read_text(encoding="utf-8"))[
        "proposed_tracked_artifact"
    ]["execution_authorized"] is True
