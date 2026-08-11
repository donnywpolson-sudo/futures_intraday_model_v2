import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.micro_futures_catalog_migration import (
    GENERIC_CATALOG_PATH,
    GENERIC_EVIDENCE_PARENT,
    GENERIC_FAILURE_PARENT,
    GENERIC_POINTER_PATH,
    GENERIC_PUBLICATION_LOCK,
    LEGACY_CATALOG_PATH,
    LEGACY_POINTER_PATH,
    LEGACY_REPORT_PATH,
    LEGACY_TERMINAL_PATH,
    PLAN_PATH,
    build_plan,
    check_plan,
    write_plan_create_only,
)


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(value) + b"\n")


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "migration"
    catalog_core = {
        "schema_version": "legacy_provider_catalog/1.0.0",
        "lane_id": "legacy_provider_lane",
        "state": "ACTIVE_CERTIFIED_SOURCE_ONLY",
        "entries": [{"immutable_release_id": "release-1"}],
    }
    catalog = {**catalog_core, "catalog_id": sha256_json(catalog_core)}
    _write(root / LEGACY_CATALOG_PATH, catalog)
    pointer_core = {
        "schema_version": "legacy_pointer/1.0.0",
        "lane_id": "legacy_provider_lane",
        "catalog_id": catalog["catalog_id"],
        "catalog_path": LEGACY_CATALOG_PATH.as_posix(),
        "catalog_sha256": sha256_file(root / LEGACY_CATALOG_PATH),
        "state": "ACTIVE_SOURCE_CATALOG_MECHANISM_NOT_FROZEN",
    }
    pointer = {**pointer_core, "pointer_id": sha256_json(pointer_core)}
    _write(root / LEGACY_POINTER_PATH, pointer)
    _write(
        root / LEGACY_TERMINAL_PATH,
        {
            "state": "SUCCESS_PUBLISHED_ACTIVE_MICRO_SOURCE_CATALOG",
            "terminal_id": "terminal-1",
            "standard_active_catalog_mutated": False,
            "year_2025_or_2026_payloads_opened": 0,
            "terminal_written_last": True,
        },
    )
    _write(
        root / LEGACY_REPORT_PATH,
        {"state": "PUBLISHED_AND_ACTIVE_SOURCE_CATALOG_ONLY"},
    )
    return root


def test_plan_proposes_only_generic_successor_paths(tmp_path: Path) -> None:
    root = _root(tmp_path)
    plan = build_plan(root=root)
    successor = plan["proposed_successor"]
    assert successor["catalog_path"] == GENERIC_CATALOG_PATH.as_posix()
    assert successor["pointer_path"] == GENERIC_POINTER_PATH.as_posix()
    assert successor["publication_lock"] == GENERIC_PUBLICATION_LOCK.as_posix()
    assert successor["failure_parent"] == GENERIC_FAILURE_PARENT.as_posix()
    assert successor["evidence_parent"] == GENERIC_EVIDENCE_PARENT.as_posix()
    assert "apex" not in " ".join(
        (
            successor["catalog_path"],
            successor["pointer_path"],
            successor["publication_lock"],
            successor["failure_parent"],
            successor["evidence_parent"],
        )
    ).lower()
    assert plan["authority"]["active_catalog_write"] is False
    assert plan["authority"]["active_pointer_write"] is False
    assert plan["authority"]["historical_row_read"] is False
    assert not (root / GENERIC_CATALOG_PATH).exists()
    assert not (root / GENERIC_POINTER_PATH).exists()


def test_plan_preserves_legacy_provenance_and_recomputes_ids(tmp_path: Path) -> None:
    root = _root(tmp_path)
    plan = build_plan(root=root)
    catalog = plan["proposed_successor"]["catalog"]
    pointer = plan["proposed_successor"]["pointer"]
    assert catalog["schema_version"] == "micro_futures_active_catalog/1.0.0"
    assert catalog["source_lane_id"] == "legacy_provider_lane"
    assert catalog["legacy_source"]["preserved_unchanged"] is True
    assert pointer["schema_version"] == "active_micro_futures_research_ladder/1.0.0"
    assert pointer["legacy_source_pointer"]["preserved_unchanged"] is True
    assert plan["plan_id"] == sha256_json(
        {key: value for key, value in plan.items() if key != "plan_id"}
    )


def test_plan_write_is_create_only_and_reconstructable(tmp_path: Path) -> None:
    root = _root(tmp_path)
    written = write_plan_create_only(root=root)
    assert json.loads((root / PLAN_PATH).read_text(encoding="utf-8")) == written
    assert check_plan(root=root) == written
    with pytest.raises(FileExistsError):
        write_plan_create_only(root=root)
