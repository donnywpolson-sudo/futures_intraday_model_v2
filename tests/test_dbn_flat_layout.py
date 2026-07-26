from pathlib import Path

import pytest

import futures_rebuild.dbn_flat_layout as flat
from futures_rebuild.canonical import canonical_bytes, sha256_bytes, sha256_json
from futures_rebuild.data_layout import (
    DataReleaseManifest,
    manifest_relative_path,
    verify_data_tree_closure,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


def _fixture_release(boundary, monkeypatch):
    contract_source = Path(__file__).parents[1] / "configs" / "data_layout_contract.json"
    contract_target = boundary.active_root / "configs" / "data_layout_contract.json"
    contract_target.parent.mkdir(parents=True, exist_ok=True)
    contract_target.write_bytes(contract_source.read_bytes())

    stage = boundary.active_root / "state" / "data_publication_staging" / "fixture"
    stage.mkdir(parents=True)
    files = {
        "a.dbn.zst": b"dbn",
        "a.dbn.zst.manifest.json": b"sidecar",
    }
    for name, payload in files.items():
        (stage / name).write_bytes(payload)
    manifest = DataReleaseManifest.build(
        stage,
        phase="dbn",
        release_kind="verified_dbn",
        schema_version="1.0.0",
        logical_paths={
            name: f"data/dbn/ohlcv_1m/ES/2024/{name}" for name in files
        },
    )
    manifest_path = boundary.active_root / manifest_relative_path("dbn", manifest.release_id)
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_bytes(canonical_bytes(manifest.as_dict()) + b"\n")
    for entry in manifest.files:
        source = boundary.active_root / manifest.retained_release_id_relative_path(entry)
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(files[Path(entry.logical_path).name])
    for path in stage.iterdir():
        path.unlink()
    stage.rmdir()

    monkeypatch.setattr(flat, "CANONICAL_RELEASE_ID", manifest.release_id)
    monkeypatch.setattr(flat, "EXPECTED_FILES", len(files))
    monkeypatch.setattr(flat, "EXPECTED_BYTES", sum(map(len, files.values())))
    monkeypatch.setattr(flat, "IMPLEMENTATION_PATHS", ())
    return manifest, manifest_path


def _approval(plan):
    authorization = sha256_bytes(
        b"Proceed with the controlled flat-layout migration for the canonical DBN release; do not delete the old layout yet."
    )
    core = {
        "approval_version": flat.APPROVAL_VERSION,
        "approved_at": "2026-07-22T12:00:00Z",
        "migration_plan_id": plan["migration_plan_id"],
        "operation": flat.OPERATION,
        "scope": plan["scope"],
        "status": "APPROVED",
        "user_authorization_id": authorization,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def test_flat_copy_is_hash_bound_idempotent_and_preserves_release_layout(
    boundary, monkeypatch
) -> None:
    manifest, _ = _fixture_release(boundary, monkeypatch)
    plan = flat.build_plan(flat.build_scope(repository_root=boundary.active_root))
    approval = _approval(plan)

    first = flat.execute(
        repository_root=boundary.active_root, plan=plan, approval=approval
    )
    second = flat.execute(
        repository_root=boundary.active_root, plan=plan, approval=approval
    )

    assert first == second
    assert first["status"] == "COMPLETE_VERIFIED_COPY_ONLY"
    assert first["legacy_layout_preserved"] is True
    for entry in manifest.files:
        destination = boundary.active_root / manifest.physical_relative_path(entry)
        source = boundary.active_root / manifest.retained_release_id_relative_path(entry)
        assert destination.read_bytes() == source.read_bytes()
        assert destination.parent.name == "2024"
        assert source.parent.name == manifest.release_id
    with pytest.raises(IntegrityError, match="orphaned"):
        verify_data_tree_closure(boundary)


def test_flat_copy_rejects_destination_collision(boundary, monkeypatch) -> None:
    manifest, _ = _fixture_release(boundary, monkeypatch)
    destination = boundary.active_root / manifest.physical_relative_path(manifest.files[0])
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(b"different")

    with pytest.raises(IntegrityError, match="flat DBN destination"):
        flat.build_scope(repository_root=boundary.active_root)


def test_flat_copy_rejects_pending_or_mutated_approval(boundary, monkeypatch) -> None:
    _fixture_release(boundary, monkeypatch)
    plan = flat.build_plan(flat.build_scope(repository_root=boundary.active_root))
    approval = flat.build_approval_draft(plan)

    with pytest.raises(UnauthorizedOperation, match="hash-bound approval"):
        flat.execute(repository_root=boundary.active_root, plan=plan, approval=approval)
