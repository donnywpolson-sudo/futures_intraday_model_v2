from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.overnight_inventory_reversal_historical_execution import (
    OPERATION,
    execute_authorized_once,
    load_historical_execution_plan,
    required_scope,
)


ROOT = Path(__file__).resolve().parents[1]


def test_execution_plan_is_exactly_one_2018_2022_attempt() -> None:
    plan = load_historical_execution_plan(root=ROOT)
    scope = required_scope(root=ROOT, plan=plan)

    assert plan["period"] == [2018, 2019, 2020, 2021, 2022]
    assert plan["markets"] == ["ES", "CL", "ZN", "6E"]
    assert plan["input_market_year_pairs"] == 20
    assert plan["limits"]["maximum_real_history_attempts"] == 1
    assert plan["limits"]["maximum_retries"] == 0
    assert plan["preservation"]["holdout_2025_untouched"] is True
    assert scope["holdout_2025_access"] == "false"
    assert scope["provider_or_network_access"] == "false"


def test_non_external_receipt_fails_before_anchor_or_output() -> None:
    boundary = RepoBoundary(ROOT)
    receipt = OperationReceipt.issue_local(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={},
    )
    plan = load_historical_execution_plan(root=ROOT)
    output_root = ROOT / str(plan["output_root"])
    before_outputs = {
        path.name: sha256_file(path) for path in output_root.glob("*.json")
    }
    before_anchors = set(
        (ROOT / "state/trial_events/overnight_inventory_reversal").glob(
            f"{plan['trial_id']}_pre_outcome_anchor_*.json"
        )
    )

    with pytest.raises(UnauthorizedOperation):
        execute_authorized_once(root=ROOT, boundary=boundary, receipt=receipt)

    assert {
        path.name: sha256_file(path) for path in output_root.glob("*.json")
    } == before_outputs
    assert set(
        (ROOT / "state/trial_events/overnight_inventory_reversal").glob(
            f"{plan['trial_id']}_pre_outcome_anchor_*.json"
        )
    ) == before_anchors


def test_consumed_attempt_has_terminal_inconclusive_closure_and_no_retry() -> None:
    plan = load_historical_execution_plan(root=ROOT)
    output_root = ROOT / str(plan["output_root"])
    closure_path = output_root / "terminal_closure.json"
    import json

    closure = json.loads(closure_path.read_text(encoding="utf-8"))
    core = dict(closure)
    report_id = core.pop("report_id")
    assert report_id == sha256_json(core)
    assert closure["period"] == [2018, 2019, 2020, 2021, 2022]
    assert closure["failure_code"] == "INSUFFICIENT_COMPLETE_TRAINING_SESSIONS"
    assert closure["terminal_state"] == "CLOSED"
    assert closure["decision"] == "INCONCLUSIVE_DATA_OR_COVERAGE"
    assert closure["attempts_consumed"] == closure["maximum_attempts"] == 1
    assert closure["retry_authorized"] is False
    assert closure["holdout_2025_touched"] is False
    assert not (output_root / "outer_evaluation.json").exists()
