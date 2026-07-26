from pathlib import Path

import pytest

import futures_rebuild.phase1a_layout as phase1a
from futures_rebuild.canonical import sha256_bytes, sha256_json
from futures_rebuild.data_layout import DataReleaseManifest, PhasePublisher
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


def _publisher(boundary, operation_factory) -> PhasePublisher:
    return PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )


def _approved(plan):
    core = {
        "approval_version": phase1a.MIGRATION_APPROVAL_VERSION,
        "approved_at": "2026-07-22T12:00:00Z",
        "migration_plan_id": plan["migration_plan_id"],
        "operation": phase1a.MIGRATION_OPERATION,
        "scope": plan["scope"],
        "status": "APPROVED",
        "user_authorization_id": "f" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def test_dbn_inventory_rejects_non_phase_path(monkeypatch) -> None:
    monkeypatch.setattr(phase1a, "EXPECTED_DBN_FILES", 1)
    monkeypatch.setattr(phase1a, "EXPECTED_SIDECAR_FILES", 1)
    monkeypatch.setattr(phase1a, "EXPECTED_TOTAL_FILES", 2)
    monkeypatch.setattr(phase1a, "EXPECTED_TOTAL_BYTES", 2)
    receipt = {
        "files": [
            {"path": "dbn/ohlcv_1m/es/2024/a.dbn.zst", "sha256": "0" * 64, "size": 1},
            {"path": "dbn/ohlcv_1m/es/2024/a.dbn.zst.manifest.json", "sha256": "1" * 64, "size": 1},
        ]
    }
    with pytest.raises(IntegrityError, match="outside the phase layout"):
        phase1a.dbn_files_from_snapshot_receipt(receipt)


def test_dbn_copy_is_exactly_approval_gated_and_manifest_addressed(
    boundary, operation_factory, monkeypatch
) -> None:
    monkeypatch.setattr(phase1a, "EXPECTED_DBN_FILES", 1)
    monkeypatch.setattr(phase1a, "EXPECTED_SIDECAR_FILES", 1)
    monkeypatch.setattr(phase1a, "EXPECTED_TOTAL_FILES", 2)
    dbn = b"d"
    sidecar = b"s"
    monkeypatch.setattr(phase1a, "EXPECTED_TOTAL_BYTES", len(dbn) + len(sidecar))

    for relative in phase1a.IMPLEMENTATION_PATHS:
        path = boundary.active_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative, encoding="utf-8")
    layout_contract = boundary.active_root / "configs" / "data_layout_contract.json"
    layout_contract.write_bytes(
        (Path(__file__).parents[1] / "configs" / "data_layout_contract.json").read_bytes()
    )

    snapshot_id = "a" * 64
    snapshot = (
        boundary.active_root
        / "data"
        / "vault"
        / "source_snapshots"
        / snapshot_id
    )
    source_dbn = snapshot / "dbn" / "ohlcv_1m" / "ES" / "2024" / "a.dbn.zst"
    source_dbn.parent.mkdir(parents=True)
    source_dbn.write_bytes(dbn)
    source_sidecar = source_dbn.with_name("a.dbn.zst.manifest.json")
    source_sidecar.write_bytes(sidecar)
    (snapshot / "SOURCE_SNAPSHOT_RECEIPT.json").write_text("receipt", encoding="utf-8")
    snapshot_receipt = {
        "source_snapshot_id": snapshot_id,
        "files": [
            {
                "path": "dbn/ohlcv_1m/ES/2024/a.dbn.zst",
                "sha256": sha256_bytes(dbn),
                "size": len(dbn),
            },
            {
                "path": "dbn/ohlcv_1m/ES/2024/a.dbn.zst.manifest.json",
                "sha256": sha256_bytes(sidecar),
                "size": len(sidecar),
            },
        ],
    }
    monkeypatch.setattr(
        phase1a, "verify_published_source_snapshot", lambda _path: snapshot_receipt
    )

    publisher = _publisher(boundary, operation_factory)
    external_archive = boundary.active_root.parent / "external-archive"
    external_archive.mkdir()
    archive_stage = publisher.create_stage("archive")
    archive = DataReleaseManifest.build(
        archive_stage,
        phase="migration",
        release_kind="futures_layout_v1_vault_archive_receipt",
        schema_version="1.0.0",
        embedded_documents={
            "archive_receipt": {
                "archive_receipt_id": "b" * 64,
                "archive_root": str(external_archive.resolve()),
                "source_root": str((boundary.active_root / "data" / "vault").resolve()),
                "status": "COMPLETE_VERIFIED_COPY_ONLY",
                "total_bytes": len(dbn) + len(sidecar),
                "total_files": 2,
                "tree_sha256": "c" * 64,
            }
        },
    )
    archive_path = publisher.publish(archive_stage, archive)
    scope = phase1a.build_migration_scope(
        snapshot_root=snapshot,
        repository_root=boundary.active_root,
        layout_contract_path=layout_contract,
        archive_manifest_path=archive_path,
    )
    plan = phase1a.build_migration_plan(scope)
    pending = phase1a.build_approval_draft(plan)
    with pytest.raises(UnauthorizedOperation, match="approval"):
        phase1a.execute_dbn_layout_copy(
            snapshot_root=snapshot,
            plan=plan,
            approval=pending,
            boundary=boundary,
            operation_receipt=operation_factory("PUBLISH_RELEASE"),
        )

    receipt = phase1a.execute_dbn_layout_copy(
        snapshot_root=snapshot,
        plan=plan,
        approval=_approved(plan),
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
    )

    manifest = receipt.verify(boundary)
    assert len(manifest.files) == 2
    assert receipt.resolve_file(
        "data/dbn/ohlcv_1m/ES/2024/a.dbn.zst", boundary
    ).read_bytes() == dbn


def test_approval_rejects_scope_change() -> None:
    plan = phase1a.build_migration_plan({"source_snapshot_id": "a" * 64})
    approval = _approved(plan)
    changed = phase1a.build_migration_plan({"source_snapshot_id": "b" * 64})
    with pytest.raises(UnauthorizedOperation, match="approval"):
        phase1a.verify_approval(approval, changed)


def test_dbn_copy_refuses_active_publication_lock(boundary, operation_factory) -> None:
    lock = boundary.active_root / "state" / "locks" / "foundation-build.lock"
    lock.parent.mkdir(parents=True)
    lock.write_text("active", encoding="utf-8")
    with pytest.raises(UnauthorizedOperation, match="coordination gate"):
        phase1a.execute_dbn_layout_copy(
            snapshot_root=boundary.active_root,
            plan={},
            approval={},
            boundary=boundary,
            operation_receipt=operation_factory("PUBLISH_RELEASE"),
        )
