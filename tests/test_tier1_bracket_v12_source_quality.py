from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_bracket_v12_source_quality import (
    COUNT_FIELDS,
    MARKETS,
    PLAN_ID,
    PUBLISH_OPERATION,
    SELECTION_RULE,
    YEARS,
    persist_v12_source_quality_record,
    prepare_v12_selected_source_quality_record,
    prepare_v12_source_quality_record,
)


def _candidate(seed: int, *, both: int) -> dict[str, object]:
    expected = 100
    counts = {name: 0 for name in COUNT_FIELDS}
    counts.update({
        "expected_open_checkpoints": expected,
        "complete_feature_windows": 90,
        "incomplete_feature_windows": 10,
        "complete_execution_windows": 92,
        "incomplete_execution_windows": 8,
        "complete_both_windows": both,
    })
    return {
        "release_id": f"{seed:064x}",
        "payload_sha256": f"{seed + 1000:064x}",
        "dependency_windows": counts,
        "source_integrity": {"total_rows": 1000},
    }


def _output() -> dict[str, object]:
    candidates: dict[str, list[dict[str, object]]] = {}
    selected: dict[str, dict[str, object]] = {}
    seed = 1
    for market in MARKETS:
        for year in YEARS:
            key = f"{market}/{year}"
            size = 4 if key == "6E/2018" else 3
            values = [_candidate(seed + offset, both=80 + offset) for offset in range(size)]
            seed += size
            candidates[key] = values
            selected[key] = values[-1]
    return {
        "status": "COMPLETED_IN_MEMORY_UNPUBLISHED_COVERAGE_COUNTS_ONLY",
        "plan_id": PLAN_ID,
        "authorization_claim_sha256": "a" * 64,
        "selection_rule": SELECTION_RULE,
        "selected": selected,
        "all_candidates": candidates,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "publication": False,
        "holdout_or_forward_access": False,
        "provider_access": False,
    }


def _authorization(root: Path, record_id: str, *, historical_row_read: str = "false") -> tuple[OperationReceipt, str, str]:
    plan_id, plan_sha = "c" * 64, "d" * 64
    scope = {
        "record_id": record_id,
        "census_plan_id": PLAN_ID,
        "publication": "true",
        "historical_row_read": historical_row_read,
        "model_fit": "false",
        "prediction_generation": "false",
        "historical_evaluation": "false",
        "holdout_or_forward_access": "false",
        "provider_access": "false",
        "active_data_mutation": "false",
        "staging": "false",
        "commit": "false",
        "push": "false",
        "trading": "false",
    }
    approval = f"APPROVE {PUBLISH_OPERATION} PLAN {plan_id} SHA256 {plan_sha}"
    return OperationReceipt.issue_user_approved(
        RepoBoundary(root),
        operation=PUBLISH_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=PUBLISH_OPERATION,
        approval_plan_id=plan_id,
        approval_plan_sha256=plan_sha,
        approval_line=approval,
    ), plan_id, plan_sha


def test_prepare_freezes_all_61_candidates_and_predeclared_winners() -> None:
    prepared = prepare_v12_source_quality_record(
        census_output=_output(), plan_sha256="b" * 64,
    )
    assert prepared.canonical_payload["candidate_release_count"] == 61
    assert prepared.canonical_payload["market_year_pair_count"] == 20
    assert prepared.canonical_payload["research_actions"]["historical_evaluation"] is False


def test_prepare_rejects_a_winner_that_does_not_follow_the_rule() -> None:
    output = _output()
    output["selected"]["ZN/2018"] = output["all_candidates"]["ZN/2018"][0]
    with pytest.raises(IntegrityError, match="preregistered rule"):
        prepare_v12_source_quality_record(census_output=output, plan_sha256="b" * 64)


def test_selected_snapshot_is_explicitly_narrow_and_hash_bound() -> None:
    output = _output()
    selected = {
        key: {
            "release_id": item["release_id"],
            "payload_sha256": item["payload_sha256"],
            "dependency_windows": item["dependency_windows"],
        }
        for key, item in output["selected"].items()
    }
    prepared = prepare_v12_selected_source_quality_record(
        selected=selected,
        plan_sha256="b" * 64,
        census_authorization_claim_sha256="a" * 64,
        candidate_manifest_catalog_id="e" * 64,
    )
    assert prepared.canonical_payload["candidate_release_count_assessed"] == 61
    assert prepared.canonical_payload["losing_candidate_counts_retained"] is False
    assert len(prepared.canonical_payload["selected"]) == 20


def test_publication_requires_exact_claim_and_is_create_only(tmp_path: Path) -> None:
    prepared = prepare_v12_source_quality_record(
        census_output=_output(), plan_sha256="b" * 64,
    )
    authorization, plan_id, plan_sha = _authorization(tmp_path, prepared.record_id)
    result = persist_v12_source_quality_record(
        root=tmp_path, prepared=prepared, authorization=authorization,
        approval_plan_id=plan_id, approval_plan_sha256=plan_sha,
    )
    assert (tmp_path / result["registry_path"]).is_file()
    with pytest.raises(UnauthorizedOperation, match="already used"):
        persist_v12_source_quality_record(
            root=tmp_path, prepared=prepared, authorization=authorization,
            approval_plan_id=plan_id, approval_plan_sha256=plan_sha,
        )


def test_publication_rejects_scope_expansion(tmp_path: Path) -> None:
    prepared = prepare_v12_source_quality_record(
        census_output=_output(), plan_sha256="b" * 64,
    )
    authorization, plan_id, plan_sha = _authorization(
        tmp_path, prepared.record_id, historical_row_read="true",
    )
    with pytest.raises(UnauthorizedOperation, match="exact required scope"):
        persist_v12_source_quality_record(
            root=tmp_path, prepared=prepared, authorization=authorization,
            approval_plan_id=plan_id, approval_plan_sha256=plan_sha,
        )
