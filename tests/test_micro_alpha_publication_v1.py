from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from futures_rebuild import micro_alpha_publication as publication
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.data_layout import DataFileEntry, DataReleaseManifest
from futures_rebuild.errors import IntegrityError


def _manifest(payload: Path) -> DataReleaseManifest:
    entry = DataFileEntry(
        logical_path="data/raw/M6E/2018/2018-01-01_2019-01-01/bars.parquet",
        size=payload.stat().st_size,
        sha256=sha256_file(payload),
    )
    core = {
        "embedded_documents": {},
        "files": [entry.as_dict()],
        "layout_version": "2.0.0",
        "manifest_version": "2.0.0",
        "metadata": {"lane_id": "apex_integer_micro_11"},
        "phase": "raw",
        "release_kind": "synthetic_micro_publication_test",
        "schema_version": "synthetic/1.0.0",
        "source_release_ids": ["a" * 64],
    }
    return DataReleaseManifest(
        release_id=sha256_json(core),
        phase="raw",
        release_kind="synthetic_micro_publication_test",
        schema_version="synthetic/1.0.0",
        source_release_ids=("a" * 64,),
        files=(entry,),
        embedded_documents={},
        metadata={"lane_id": "apex_integer_micro_11"},
    )


def test_prepare_cli_has_no_execution_surface() -> None:
    from scripts import prepare_apex_micro_publication_v1 as prepare

    source = inspect.getsource(prepare)
    assert "execute_once" not in source
    assert '"execute"' not in source
    assert '"preview-plan"' in source
    assert '"write-audit"' in source


@pytest.mark.parametrize(
    ("staged", "logical"),
    [
        (
            "state/data_publication_staging/lane/hash/data/raw/M6E/2018/"
            "2018-01-01_2019-01-01/0123456789abcdef01234567/bars.parquet",
            "data/raw/M6E/2018/2018-01-01_2019-01-01/bars.parquet",
        ),
        (
            "state/data_publication_staging/lane/hash/data/causally_gated_normalized/"
            "MES/2019/2019-05-05_2020-01-01/abcdef0123456789abcdef01/bars.parquet",
            "data/causally_gated_normalized/MES/2019/"
            "2019-05-05_2020-01-01/bars.parquet",
        ),
    ],
)
def test_staging_alias_is_removed_without_changing_logical_identity(
    staged: str, logical: str,
) -> None:
    assert publication.logical_path_from_inactive(staged) == logical


def test_invalid_staging_alias_fails_closed() -> None:
    with pytest.raises(IntegrityError, match="96-bit"):
        publication.logical_path_from_inactive(
            "state/data_publication_staging/x/data/raw/M6E/2018/interval/not-an-alias/bars.parquet"
        )


def test_publish_one_preserves_source_and_uses_manifested_full_release_id(
    tmp_path: Path,
) -> None:
    root = tmp_path.resolve()
    (root / "configs").mkdir()
    source = root / "state/inactive/source.parquet"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"synthetic-not-parquet-row-data")
    manifest = _manifest(source)
    item = {
        "logical_path": manifest.files[0].logical_path,
        "manifest": manifest.as_dict(),
        "manifest_path": publication.manifest_relative_path(
            manifest.phase, manifest.release_id
        ).as_posix(),
        "physical_path": manifest.physical_relative_path(manifest.files[0]).as_posix(),
        "role": "PHASE1B",
        "source_bytes": source.stat().st_size,
        "source_path": source.relative_to(root).as_posix(),
        "source_sha256": sha256_file(source),
    }
    boundary = RepoBoundary(root)
    receipt = OperationReceipt.issue_local(
        boundary,
        operation=publication.OPERATION,
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={"test": "synthetic"},
    )
    result = publication._publish_one(
        root=root,
        boundary=boundary,
        authorization=receipt,
        plan={"scope_id": "b" * 64},
        item=item,
        ordinal=0,
    )
    target = root / result["target_path"]
    assert source.read_bytes() == b"synthetic-not-parquet-row-data"
    assert target.read_bytes() == source.read_bytes()
    assert manifest.release_id in target.parts
    assert (root / result["manifest_path"]).is_file()


def test_activation_rollback_moves_only_exact_new_artifacts(tmp_path: Path) -> None:
    root = tmp_path.resolve()
    catalog = {"schema_version": "x", "catalog_id": "c" * 64}
    pointer = {"schema_version": "y", "pointer_id": "d" * 64}
    catalog_path = root / publication.ACTIVE_MICRO_CATALOG_PATH
    pointer_path = root / publication.MICRO_POINTER_PATH
    publication._write_create_only(catalog_path, catalog)
    publication._write_create_only(pointer_path, pointer)
    moved = publication._quarantine_activation(
        root=root,
        plan={"failed_activation_root": "state/failed/test"},
        catalog=catalog,
        pointer=pointer,
    )
    assert not catalog_path.exists()
    assert not pointer_path.exists()
    assert sorted(moved) == [
        "state/failed/test/active_catalog.json",
        "state/failed/test/active_pointer.json",
    ]


def test_active_verification_fails_if_standard_catalog_changes(tmp_path: Path) -> None:
    root = tmp_path.resolve()
    standard = root / publication.STANDARD_ACTIVE_CATALOG_PATH
    standard.parent.mkdir(parents=True)
    standard.write_bytes(b"standard-catalog\n")
    catalog_core = {"schema_version": "synthetic", "state": "ACTIVE"}
    catalog = {**catalog_core, "catalog_id": sha256_json(catalog_core)}
    catalog_path = root / publication.ACTIVE_MICRO_CATALOG_PATH
    publication._write_create_only(catalog_path, catalog)
    pointer_core = {
        "catalog_sha256": sha256_file(catalog_path),
        "schema_version": "synthetic",
        "state": "ACTIVE",
    }
    pointer = {**pointer_core, "pointer_id": sha256_json(pointer_core)}
    publication._write_create_only(root / publication.MICRO_POINTER_PATH, pointer)
    plan = {
        "catalog": catalog,
        "pointer": pointer,
        "standard_active_catalog_sha256": sha256_file(standard),
    }
    assert publication.verify_active(root=root, plan=plan)["catalog_id"] == catalog["catalog_id"]
    standard.write_bytes(b"changed-standard-catalog\n")
    with pytest.raises(IntegrityError, match="verification failed"):
        publication.verify_active(root=root, plan=plan)


def test_authority_is_consumed_before_payload_hash_or_copy() -> None:
    source = inspect.getsource(publication.execute_once)
    assert source.index("authorization.verify(") < source.index("_publish_one(")
    assert source.index("authorization.consume(") < source.index("_publish_one(")
    module = inspect.getsource(publication)
    assert "pyarrow" not in module
    assert '"year_2025_or_2026_payloads_opened": 0' in source
    assert '"standard_active_catalog_mutated": False' in source


def test_pointer_is_written_after_all_releases_and_catalog() -> None:
    source = inspect.getsource(publication.execute_once)
    publish_at = source.index("_publish_one(")
    catalog_at = source.index("_write_create_only(root / ACTIVE_MICRO_CATALOG_PATH")
    pointer_at = source.index("_write_create_only(root / MICRO_POINTER_PATH")
    verify_at = source.index("verify_active(")
    assert publish_at < catalog_at < pointer_at < verify_at
