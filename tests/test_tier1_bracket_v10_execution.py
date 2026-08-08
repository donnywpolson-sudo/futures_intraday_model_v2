from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild.boundary import OperationClassification, OperationReceipt, RepoBoundary
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.tier1_bracket_v5 import EvidenceArtifactsV5
from futures_rebuild.tier1_bracket_v10_execution import (
    EXECUTION_OPERATION, V10ExecutionResult, authorized_source_streams_v10,
    build_evidence_manifest_v10, claim_historical_operation_receipt_v10,
)


def _execution_receipt(
    *, boundary: RepoBoundary, trial_id: str, source_binding_id: str,
    output_root: Path, plan_id: str, plan_sha: str,
) -> OperationReceipt:
    scope = {
        "trial_id": trial_id, "source_binding_id": source_binding_id,
        "output_root": output_root.as_posix(),
        "holdout_or_forward_access": "false", "provider_access": "false",
        "publication": "false",
    }
    return OperationReceipt.issue_user_approved(
        boundary, operation=EXECUTION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope, approval_command=EXECUTION_OPERATION,
        approval_plan_id=plan_id, approval_plan_sha256=plan_sha,
        approval_line=f"APPROVE {EXECUTION_OPERATION} PLAN {plan_id} SHA256 {plan_sha}",
    )


def test_v10_historical_authorization_is_exact_durable_and_single_use(tmp_path: Path) -> None:
    boundary = RepoBoundary(tmp_path)
    trial_id, source_id, plan_id, plan_sha = ("a" * 64, "b" * 64, "c" * 64, "d" * 64)
    output = tmp_path / "unpublished"
    receipt = _execution_receipt(
        boundary=boundary, trial_id=trial_id, source_binding_id=source_id,
        output_root=output, plan_id=plan_id, plan_sha=plan_sha,
    )
    claim = claim_historical_operation_receipt_v10(
        root=tmp_path, boundary=boundary, receipt=receipt,
        trial_id=trial_id, source_binding_id=source_id, output_root=output,
        plan_id=plan_id, plan_sha256=plan_sha,
    )
    assert claim.exists()
    with pytest.raises(UnauthorizedOperation, match="already consumed"):
        claim_historical_operation_receipt_v10(
            root=tmp_path, boundary=boundary, receipt=receipt,
            trial_id=trial_id, source_binding_id=source_id, output_root=output,
            plan_id=plan_id, plan_sha256=plan_sha,
        )


def test_v10_rejects_2025_before_registry_or_file_open(tmp_path: Path) -> None:
    boundary = RepoBoundary(tmp_path)
    receipt = OperationReceipt.issue_local(
        boundary, operation="synthetic",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    with pytest.raises(UnauthorizedOperation, match="2025"):
        authorized_source_streams_v10(
            root=tmp_path, boundary=boundary, receipt=receipt,
            trial_id="a" * 64,
            source_paths={("ES", 2025): tmp_path / "must-not-open.parquet"},
            output_root=tmp_path / "output", plan_id="c" * 64,
            plan_sha256="d" * 64,
        )


def test_v10_manifest_binds_full_evidence_runtime_source_and_authority() -> None:
    runtime = {
        "runtime_receipt_id": "1" * 64,
        "dependency_lock_receipt_id": "2" * 64,
        "authorization_receipt_id": "3" * 64,
        "authorization_claim_sha256": "4" * 64,
        "execution_plan_id": "5" * 64,
        "execution_plan_sha256": "6" * 64,
    }
    evidence = EvidenceArtifactsV5(
        model={"id": "model"}, predictions=({"opportunity_id": "p"},),
        opportunity_ledger=({"opportunity_id": "p", "terminal": "HURDLE_FAILURE"},),
        fills=(), continuous_equity_marks=(), segmented_metrics={},
        inference={"status": "PASS"}, decision={"classification": "FAIL_NO_EDGE"},
        runtime_receipt=runtime,
    )
    result = V10ExecutionResult(
        SimpleNamespace(evidence=evidence),  # type: ignore[arg-type]
        {"ES/2020": {"nontradable_rows": 1}},
    )
    manifest = build_evidence_manifest_v10(trial_id="f" * 64, result=result)
    assert set(manifest["files"]) == {
        "continuous_equity_marks.json", "decision.json", "fills.json",
        "inference.json", "model.json", "opportunity_ledger.json",
        "predictions.json", "runtime_receipt.json", "segmented_metrics.json",
        "source_integrity_audit.json",
    }
    assert len(manifest["manifest_id"]) == 64
