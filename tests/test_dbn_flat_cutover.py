import os
from pathlib import Path

import pytest

import futures_rebuild.dbn_flat_cutover as cutover
from futures_rebuild.canonical import canonical_bytes, sha256_bytes, sha256_file, sha256_json
from futures_rebuild.data_layout import DataReleaseManifest, manifest_relative_path
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(payload) + b"\n")


def _fixture(boundary, monkeypatch):
    contract_source = Path(__file__).parents[1] / "configs" / "data_layout_contract.json"
    contract_target = boundary.active_root / "configs" / "data_layout_contract.json"
    contract_target.parent.mkdir(parents=True, exist_ok=True)
    contract_target.write_bytes(contract_source.read_bytes())

    stage = boundary.active_root / "state" / "data_publication_staging" / "cutover"
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
    _write_json(manifest_path, manifest.as_dict())
    for entry in manifest.files:
        flat = boundary.active_root / entry.logical_path
        flat.parent.mkdir(parents=True, exist_ok=True)
        flat.write_bytes(files[flat.name])
        retained = flat.parent / manifest.release_id / flat.name
        retained.parent.mkdir(parents=True, exist_ok=True)
        retained.write_bytes(files[flat.name])
    for path in stage.iterdir():
        path.unlink()
    stage.rmdir()

    monkeypatch.setattr(cutover, "CANONICAL_RELEASE_ID", manifest.release_id)
    monkeypatch.setattr(cutover, "EXPECTED_DBN_FILES", 1)
    monkeypatch.setattr(cutover, "EXPECTED_SIDECAR_FILES", 1)
    monkeypatch.setattr(cutover, "EXPECTED_FILES", 2)
    monkeypatch.setattr(cutover, "EXPECTED_BYTES", sum(map(len, files.values())))
    monkeypatch.setattr(cutover, "EXPECTED_RELEASE_DIRECTORIES", 1)
    monkeypatch.setattr(cutover, "IMPLEMENTATION_PATHS", ())
    receipt_relative = Path("manifests/data_layout_transitions/test-flat.json")
    monkeypatch.setattr(cutover, "FLAT_MIGRATION_RECEIPT_PATH", receipt_relative)
    inventory_sha256 = sha256_json([entry.as_dict() for entry in manifest.files])
    flat_core = {
        "approval_receipt_id": "a" * 64,
        "completed_at": "2026-07-23T00:00:00Z",
        "destination_layout": "data/dbn/{family}/{market}/{year}/{filename}",
        "inventory_sha256": inventory_sha256,
        "legacy_layout_preserved": True,
        "migration_plan_id": "b" * 64,
        "receipt_version": "1.0.0",
        "source_manifest_sha256": sha256_file(manifest_path),
        "source_release_id": manifest.release_id,
        "status": "COMPLETE_VERIFIED_COPY_ONLY",
        "total_bytes": cutover.EXPECTED_BYTES,
        "total_files": cutover.EXPECTED_FILES,
    }
    _write_json(
        boundary.active_root / receipt_relative,
        {**flat_core, "receipt_id": sha256_json(flat_core)},
    )
    return manifest


def _approval(plan):
    core = {
        "approval_version": cutover.APPROVAL_VERSION,
        "approved_at": "2026-07-23T01:00:00Z",
        "cutover_plan_id": plan["cutover_plan_id"],
        "operation": cutover.OPERATION,
        "scope": plan["scope"],
        "status": "APPROVED",
        "user_authorization_id": sha256_bytes(b"delete exact retained DBN copies"),
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def _plan(boundary):
    return cutover.build_plan(
        cutover.build_scope(repository_root=boundary.active_root)
    )


def test_cutover_deletes_only_retained_tree_and_is_idempotent(
    boundary, monkeypatch
) -> None:
    manifest = _fixture(boundary, monkeypatch)
    plan = _plan(boundary)
    approval = _approval(plan)

    first = cutover.execute(
        repository_root=boundary.active_root, plan=plan, approval=approval
    )
    second = cutover.execute(
        repository_root=boundary.active_root, plan=plan, approval=approval
    )

    assert first == second
    assert first["status"] == "COMPLETE_VERIFIED_DESTRUCTIVE_CUTOVER"
    for entry in manifest.files:
        flat = boundary.active_root / entry.logical_path
        assert flat.is_file()
        assert not (flat.parent / manifest.release_id).exists()


@pytest.mark.parametrize("defect", ["missing_flat", "extra_retained", "bad_retained"])
def test_cutover_preflight_rejects_incomplete_extra_or_changed_bytes(
    boundary, monkeypatch, defect
) -> None:
    manifest = _fixture(boundary, monkeypatch)
    entry = manifest.files[0]
    flat = boundary.active_root / entry.logical_path
    retained = flat.parent / manifest.release_id / flat.name
    if defect == "missing_flat":
        flat.unlink()
    elif defect == "extra_retained":
        (retained.parent / "extra.dbn.zst").write_bytes(b"extra")
    else:
        retained.write_bytes(b"changed")

    with pytest.raises((ContractError, IntegrityError)):
        cutover.build_scope(repository_root=boundary.active_root)


def test_cutover_rejects_wrong_release_directory_and_hardlink(
    boundary, monkeypatch
) -> None:
    manifest = _fixture(boundary, monkeypatch)
    entry = manifest.files[0]
    flat = boundary.active_root / entry.logical_path
    wrong = flat.parent / ("f" * 64)
    wrong.mkdir()
    with pytest.raises(IntegrityError, match="directory census"):
        cutover.build_scope(repository_root=boundary.active_root)
    wrong.rmdir()

    original = boundary.active_root / "state" / "hardlink-source"
    original.parent.mkdir(parents=True, exist_ok=True)
    flat.replace(original)
    os.link(original, flat)
    with pytest.raises(ContractError, match="hard-linked"):
        cutover.build_scope(repository_root=boundary.active_root)


def test_cutover_rejects_pending_approval(boundary, monkeypatch) -> None:
    _fixture(boundary, monkeypatch)
    plan = _plan(boundary)

    with pytest.raises(UnauthorizedOperation, match="hash-bound approval"):
        cutover.execute(
            repository_root=boundary.active_root,
            plan=plan,
            approval=cutover.build_approval_draft(plan),
        )


def test_cutover_resumes_only_after_durable_intent(boundary, monkeypatch) -> None:
    manifest = _fixture(boundary, monkeypatch)
    plan = _plan(boundary)
    approval = _approval(plan)
    original_delete = cutover._delete_retained
    calls = 0

    def interrupted(path):
        nonlocal calls
        original_delete(path)
        calls += 1
        if calls == 1:
            raise RuntimeError("simulated interruption")

    monkeypatch.setattr(cutover, "_delete_retained", interrupted)
    with pytest.raises(RuntimeError, match="simulated interruption"):
        cutover.execute(
            repository_root=boundary.active_root, plan=plan, approval=approval
        )
    intent = (
        boundary.active_root
        / cutover.STATE_ROOT
        / plan["cutover_plan_id"]
        / "intent.json"
    )
    assert intent.is_file()
    remaining = [
        path
        for path in (boundary.active_root / "data" / "dbn").rglob("*")
        if path.is_file() and path.parent.name == manifest.release_id
    ]
    assert len(remaining) == cutover.EXPECTED_FILES - 1

    monkeypatch.setattr(cutover, "_delete_retained", original_delete)
    receipt = cutover.execute(
        repository_root=boundary.active_root, plan=plan, approval=approval
    )
    assert receipt["deleted_files"] == cutover.EXPECTED_FILES
    assert not any(
        path.is_dir() and path.name == manifest.release_id
        for path in (boundary.active_root / "data" / "dbn").rglob("*")
    )
