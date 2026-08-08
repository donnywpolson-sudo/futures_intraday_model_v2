from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_bracket_v7 import (
    V6_EVENT,
    V6_REGISTRY,
    V6_TRIAL_ID,
    authorized_source_streams_v7,
    build_evidence_manifest_v7,
    load_v7_contract,
    prepare_v6_retirement_v7,
    prepare_v7_registration,
    persist_evidence_bundle_v7,
)
from futures_rebuild.tier1_bracket_v5 import EvidenceArtifactsV5
from futures_rebuild.tier1_bracket_v6 import V6PipelineResult


ROOT = Path(__file__).resolve().parents[1]


def test_v7_is_governance_only_and_binds_the_test_runner() -> None:
    inherited, delta = load_v7_contract(root=ROOT)
    retirement = prepare_v6_retirement_v7(root=ROOT)
    registration = prepare_v7_registration(root=ROOT)
    assert inherited["risk"]["continuous_drawdown_threshold_usd"] == "1500"
    assert delta["governance_successor"]["test_runner_binding"] == "tests/conftest.py_REQUIRED"
    assert registration.canonical_payload["change_scope"] == "TEST_LIFECYCLE_AND_RUNNER_BINDING_ONLY"
    assert "tests/conftest.py" in registration.canonical_payload["bindings"]
    assert registration.canonical_payload["v6_retirement_record_id"] == retirement.record_id
    assert len(registration.canonical_payload["source_bindings"]) == 20


def test_v7_lifecycle_is_valid_before_and_after_create_only_publication() -> None:
    retirement = prepare_v6_retirement_v7(root=ROOT)
    registration = prepare_v7_registration(root=ROOT)
    retirement_path = (
        ROOT / "state/trial_registry/tier1_bracket_v6_retirement"
        / f"{retirement.record_id}.json"
    )
    registration_path = (
        ROOT / "state/trial_registry/tier1_bracket_successor_v7"
        / f"{registration.trial_id}.json"
    )
    assert retirement_path.exists() == registration_path.exists()
    if retirement_path.exists():
        assert '"state":"RETIRED_INVALID_BEFORE_SOURCE_ACCESS"' in retirement_path.read_text()
        assert '"state":"REGISTERED_BEFORE_SOURCE_ROW_ACCESS"' in registration_path.read_text()


def test_v6_registered_bytes_remain_preserved() -> None:
    retirement = prepare_v6_retirement_v7(root=ROOT)
    preserved = retirement.canonical_payload["preserved_v6_sha256"]
    assert V6_REGISTRY.as_posix() in preserved
    assert V6_EVENT.as_posix() in preserved
    assert retirement.canonical_payload["trial_id"] == V6_TRIAL_ID


def test_v7_rejects_2025_before_registry_or_file_open(tmp_path: Path) -> None:
    boundary = RepoBoundary(tmp_path)
    receipt = OperationReceipt.issue_local(
        boundary,
        operation="synthetic",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    with pytest.raises(UnauthorizedOperation, match="2025"):
        authorized_source_streams_v7(
            root=tmp_path, boundary=boundary, receipt=receipt,
            trial_id="f" * 64,
            source_paths={("ES", 2025): tmp_path / "must-not-open.parquet"},
            output_root=tmp_path / "output",
        )


def test_v7_evidence_manifest_is_versioned_complete_and_create_only(tmp_path: Path) -> None:
    evidence = EvidenceArtifactsV5(
        model={"coefficient": Decimal("1.25")},
        predictions=({"opportunity_id": "p", "score": Decimal("0.3")},),
        opportunity_ledger=({"opportunity_id": "p", "terminal": "ADMITTED_TRADE"},),
        fills=({"opportunity_id": "p", "net": Decimal("2.50")},),
        continuous_equity_marks=({"equity": Decimal("100002.50")},),
        segmented_metrics={"folds": []},
        inference={"status": "OK"},
        decision={"classification": "FAIL_NO_EDGE"},
        runtime_receipt={"runtime_receipt_id": "a" * 64},
    )
    result = V6PipelineResult(
        base=SimpleNamespace(evidence=evidence),
        source_integrity_audit={"ES/2020": {"nontradable_rows": 1}},
    )
    trial_id = "f" * 64
    manifest = build_evidence_manifest_v7(trial_id=trial_id, result=result)
    assert manifest["schema_version"] == "tier1_bracket_successor_v7_evidence_manifest/1.0.0"
    assert len(manifest["files"]) == 10
    output = tmp_path / "evidence"
    published = persist_evidence_bundle_v7(
        boundary=RepoBoundary(tmp_path), output_root=output,
        trial_id=trial_id, result=result,
    )
    assert Path(published["manifest_path"]).exists()
    with pytest.raises(IntegrityError, match="create-only"):
        persist_evidence_bundle_v7(
            boundary=RepoBoundary(tmp_path), output_root=output,
            trial_id=trial_id, result=result,
        )
