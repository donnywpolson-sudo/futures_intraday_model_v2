from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.data_layout import DataFileEntry, DataReleaseManifest
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.successor_migration import (
    APPROVAL_SCHEMA,
    OPERATION,
    _contract_templates,
    _inventory_entries,
    _successor_manifest,
    approval_draft,
    approval_payload,
    verify_approval,
)


def _inventory() -> dict:
    records = []
    for index in range(471):
        market = f"X{index:03d}"
        base = f"data/dbn/definition/{market}/2020/{index:04d}.dbn.zst"
        records.append(
            {
                "destination_path": base,
                "source_path": base,
                "sidecar_path": f"{base}.manifest.json",
                "dbn_bytes": 10 + index,
                "dbn_sha256": f"{index + 1:064x}",
                "sidecar_bytes": 20 + index,
                "sidecar_sha256": f"{index + 1000:064x}",
            }
        )
    return {"inventory_id": "f" * 64, "records": records}


def _parent() -> DataReleaseManifest:
    files = tuple(
        DataFileEntry(
            f"data/dbn/status/P{i:04d}/2020/{i:04d}.dbn.zst",
            i + 1,
            f"{i + 2000:064x}",
        )
        for i in range(8040)
    )
    core = {
        "embedded_documents": {},
        "files": [item.as_dict() for item in files],
        "layout_version": "2.0.0",
        "manifest_version": "2.0.0",
        "metadata": {},
        "phase": "dbn",
        "release_kind": "futures_phase1a_verified_dbn",
        "schema_version": "1.0.0",
        "source_release_ids": [],
    }
    return DataReleaseManifest(
        release_id=sha256_json(core),
        phase="dbn",
        release_kind="futures_phase1a_verified_dbn",
        schema_version="1.0.0",
        source_release_ids=(),
        files=files,
        embedded_documents={},
        metadata={},
    )


def test_candidate_inventory_becomes_exact_unique_942_file_closure() -> None:
    entries = _inventory_entries(_inventory())
    assert len(entries) == 942
    assert len({item.logical_path for item in entries}) == 942
    with pytest.raises(Exception, match="942"):
        _inventory_entries({"records": _inventory()["records"][:-1]})


def test_approval_is_hash_bound_and_pending_never_authorizes() -> None:
    plan = {"plan_id": "a" * 64}
    with pytest.raises(UnauthorizedOperation):
        verify_approval(
            approval_draft(plan), plan=plan, plan_sha256="b" * 64
        )
    approved = approval_payload(
        plan=plan,
        plan_sha256="b" * 64,
        approved_at="2026-07-24T20:00:00Z",
        user_authorization_id="c" * 64,
    )
    assert approved["schema_version"] == APPROVAL_SCHEMA
    assert approved["operation"] == OPERATION
    assert (
        verify_approval(approved, plan=plan, plan_sha256="b" * 64)
        == approved["approval_receipt_id"]
    )
    tampered = dict(approved)
    tampered["plan_sha256"] = "d" * 64
    with pytest.raises(UnauthorizedOperation):
        verify_approval(tampered, plan=plan, plan_sha256="b" * 64)


def test_successor_manifest_is_parent_plus_candidate_and_non_alpha() -> None:
    parent = _parent()
    successor = _successor_manifest(
        parent=parent, inventory=_inventory(), approval_id="e" * 64
    )
    assert len(successor.files) == 8982
    assert successor.source_release_ids == (parent.release_id,)
    assert (
        successor.embedded_documents["phase1a_receipt"]["status"]
        == "COMPLETE_VERIFIED_IMMUTABLE_SUCCESSOR"
    )


def test_contract_templates_bind_release_and_approval_placeholders() -> None:
    templates = _contract_templates(
        {
            "canonical_dbn_release": {},
            "vault_expectations": {},
            "legacy_repository": "C:/retired-a",
        },
        {"status": "PENDING_APPROVAL", "approval_receipt_id": None},
    )
    assert set(templates) == {
        "source_contract_template_sha256",
        "research_universe_template_sha256",
    }
    assert all(len(value) == 64 for value in templates.values())
    other_legacy_path = _contract_templates(
        {
            "canonical_dbn_release": {},
            "vault_expectations": {},
            "legacy_repository": "D:/retired-b",
        },
        {"status": "PENDING_APPROVAL", "approval_receipt_id": None},
    )
    assert (
        templates["source_contract_template_sha256"]
        == other_legacy_path["source_contract_template_sha256"]
    )
