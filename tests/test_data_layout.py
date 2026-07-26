import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_bytes
from futures_rebuild.data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
    verify_data_release_manifest,
    verify_data_tree_closure,
    verify_layout_contract,
)
from futures_rebuild.errors import ContractError, IntegrityError
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.release import AtomicPublisher


def _publisher(boundary, operation_factory) -> PhasePublisher:
    return PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )


def test_phase_publication_is_manifest_addressed_and_tamper_evident(
    boundary, operation_factory
) -> None:
    publisher = _publisher(boundary, operation_factory)
    stage = publisher.create_stage("raw")
    (stage / "bars.parquet").write_bytes(b"bars")
    (stage / "definitions.parquet").write_bytes(b"definitions")
    manifest = DataReleaseManifest.build(
        stage,
        phase="raw",
        release_kind="futures_phase1b_actual_raw_interval",
        schema_version="3.0.0",
        logical_paths={
            "bars.parquet": "data/raw/ES/2024/2024-01-01_2024-02-01/bars.parquet",
            "definitions.parquet": (
                "data/raw/ES/2024/2024-01-01_2024-02-01/definitions.parquet"
            ),
        },
        embedded_documents={"interval_receipt": {"rows": 1}},
    )
    manifest_path = publisher.publish(stage, manifest)
    assert manifest_path == (
        boundary.active_root
        / "manifests"
        / "data_releases"
        / "raw"
        / f"{manifest.release_id}.json"
    )
    bars = (
        boundary.active_root
        / "data"
        / "raw"
        / "ES"
        / "2024"
        / "2024-01-01_2024-02-01"
        / manifest.release_id
        / "bars.parquet"
    )
    assert bars.read_bytes() == b"bars"
    receipt = DataReleaseReceipt.from_manifest(manifest_path, boundary)
    assert receipt.resolve_file(
        "data/raw/ES/2024/2024-01-01_2024-02-01/bars.parquet", boundary
    ) == bars
    assert verify_data_tree_closure(boundary) == {"data_files": 2, "manifests": 1}
    bars.write_bytes(b"tamper")
    with pytest.raises(IntegrityError, match="failed verification"):
        receipt.verify(boundary)


def test_phase_publication_is_idempotent_and_preserves_unrelated_stage(
    boundary, operation_factory
) -> None:
    publisher = _publisher(boundary, operation_factory)

    def build():
        stage = publisher.create_stage("same")
        (stage / "rows.jsonl").write_bytes(b"{}\n")
        manifest = DataReleaseManifest.build(
            stage,
            phase="features",
            release_kind="feature_release",
            schema_version="3.0.0",
            logical_paths={
                "rows.jsonl": (
                    "data/features/spec/ES/2024/2024-01-01_2024-02-01/rows.jsonl"
                )
            },
        )
        return stage, manifest

    first, first_manifest = build()
    target = publisher.publish(first, first_manifest)
    unrelated = publisher.create_stage("unrelated")
    (unrelated / "keep.txt").write_text("keep", encoding="utf-8")
    duplicate, duplicate_manifest = build()
    assert publisher.publish(duplicate, duplicate_manifest) == target
    assert not duplicate.exists()
    assert (unrelated / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_manifest_only_release_embeds_control_document(boundary) -> None:
    stage = boundary.active_root / "state" / "data_publication_staging" / "control"
    stage.mkdir(parents=True)
    manifest = DataReleaseManifest.build(
        stage,
        phase="foundation",
        release_kind="futures_mechanical_foundation_set",
        schema_version="3.0.0",
        embedded_documents={"foundation_set": {"status": "COMPLETE"}},
    )
    output = (
        boundary.active_root
        / "manifests"
        / "data_releases"
        / "foundation"
        / f"{manifest.release_id}.json"
    )
    output.parent.mkdir(parents=True)
    output.write_bytes(canonical_bytes(manifest.as_dict()) + b"\n")
    observed = verify_data_release_manifest(output, boundary)
    assert observed.embedded_documents["foundation_set"] == {"status": "COMPLETE"}


def test_layout_rejects_wrong_phase_paths_or_unmapped_stage_files(boundary) -> None:
    stage = boundary.active_root / "state" / "data_publication_staging" / "bad"
    stage.mkdir(parents=True)
    (stage / "rows").write_bytes(b"x")
    with pytest.raises(ContractError, match="declared phase"):
        DataReleaseManifest.build(
            stage,
            phase="raw",
            release_kind="raw",
            schema_version="1",
            logical_paths={"rows": "data/features/rows"},
        )
    with pytest.raises(ContractError, match="exact staged file set"):
        DataReleaseManifest.build(
            stage,
            phase="raw",
            release_kind="raw",
            schema_version="1",
            logical_paths={
                "rows": "data/raw/ES/2024/interval/rows",
                "missing": "data/raw/ES/2024/interval/missing",
            },
        )


def test_manifest_rejects_noncanonical_or_extra_fields(
    boundary, operation_factory
) -> None:
    publisher = _publisher(boundary, operation_factory)
    stage = publisher.create_stage("schema")
    (stage / "rows").write_bytes(b"x")
    manifest = DataReleaseManifest.build(
        stage,
        phase="raw",
        release_kind="raw",
        schema_version="1",
        logical_paths={"rows": "data/raw/ES/2024/interval/rows"},
    )
    path = publisher.publish(stage, manifest)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["ignored"] = True
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(IntegrityError, match="canonical|schema"):
        verify_data_release_manifest(path, boundary)


def test_closure_rejects_orphan_data_file(boundary) -> None:
    orphan = boundary.active_root / "data" / "raw" / "ES" / "2024" / "orphan"
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"orphan")
    with pytest.raises(IntegrityError, match="orphaned"):
        verify_data_tree_closure(boundary)


def test_layout_contract_matches_implementation(boundary) -> None:
    source = Path(__file__).parents[1] / "configs" / "data_layout_contract.json"
    target = boundary.active_root / "configs" / "data_layout_contract.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(source.read_bytes())
    assert verify_layout_contract(target)["layout_version"] == "2.0.0"


def test_dbn_manifest_resolves_flat_and_closure_rejects_historical_copy(
    boundary, operation_factory
) -> None:
    publisher = _publisher(boundary, operation_factory)
    stage = publisher.create_stage("dbn")
    (stage / "a.dbn.zst").write_bytes(b"dbn")
    manifest = DataReleaseManifest.build(
        stage,
        phase="dbn",
        release_kind="verified_dbn",
        schema_version="1.0.0",
        logical_paths={
            "a.dbn.zst": "data/dbn/ohlcv_1m/ES/2024/a.dbn.zst"
        },
    )
    manifest_path = publisher.publish(stage, manifest)
    entry = manifest.files[0]
    flat = boundary.active_root / manifest.physical_relative_path(entry)
    retained = boundary.active_root / manifest.retained_release_id_relative_path(entry)
    receipt = DataReleaseReceipt.from_manifest(manifest_path, boundary)

    assert receipt.resolve_file(entry.logical_path, boundary) == flat
    assert flat.parent.name == "2024"
    assert retained.parent.name == manifest.release_id
    assert verify_data_tree_closure(boundary) == {"data_files": 1, "manifests": 1}

    retained.parent.mkdir(parents=True)
    retained.write_bytes(flat.read_bytes())
    with pytest.raises(IntegrityError, match="orphaned"):
        verify_data_tree_closure(boundary)


def test_publication_recovers_after_data_promotion_before_manifest(
    boundary, operation_factory
) -> None:
    publisher = _publisher(boundary, operation_factory)
    stage = publisher.create_stage("recover")
    (stage / "rows.jsonl").write_bytes(b"{}\n")
    logical = "data/raw/ES/2024/2024-01-01_2024-02-01/rows.jsonl"
    manifest = DataReleaseManifest.build(
        stage,
        phase="raw",
        release_kind="raw",
        schema_version="1",
        logical_paths={"rows.jsonl": logical},
    )
    staged_paths = {logical: "rows.jsonl"}
    publisher._write_intent(stage, manifest, staged_paths)
    target = boundary.active_root / manifest.physical_relative_path(manifest.files[0])
    target.parent.mkdir(parents=True)
    (stage / "rows.jsonl").replace(target)

    manifest_path = publisher.recover_stage(stage)

    assert not stage.exists()
    assert verify_data_release_manifest(manifest_path, boundary) == manifest


def test_publication_rejects_extra_file_added_after_manifest(
    boundary, operation_factory
) -> None:
    publisher = _publisher(boundary, operation_factory)
    stage = publisher.create_stage("extra")
    (stage / "rows").write_bytes(b"x")
    manifest = DataReleaseManifest.build(
        stage,
        phase="raw",
        release_kind="raw",
        schema_version="1",
        logical_paths={"rows": "data/raw/ES/2024/interval/rows"},
    )
    (stage / "extra").write_bytes(b"unexpected")
    with pytest.raises(IntegrityError, match="unmanifested"):
        publisher.publish(stage, manifest)


def test_layout_contract_disables_legacy_vault_publisher(
    boundary, operation_factory
) -> None:
    contract = Path(__file__).parents[1] / "configs" / "data_layout_contract.json"
    (boundary.active_root / "configs" / "data_layout_contract.json").write_bytes(
        contract.read_bytes()
    )
    with pytest.raises(UnauthorizedOperation, match="layout-v1"):
        AtomicPublisher(
            boundary.active_root / "data" / "vault" / ".staging" / "releases",
            boundary.active_root / "data" / "vault" / "releases",
            boundary.active_root / "state" / "locks" / "release.lock",
            boundary=boundary,
            operation_receipt=operation_factory("PUBLISH_RELEASE"),
        )
