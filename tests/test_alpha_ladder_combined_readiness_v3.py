from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_combined_readiness_v3 import (
    EVALUATION_SESSIONS,
    PURGE_MINUTES,
    TRAINING_SESSIONS,
    _outer_folds,
    _rolling_fold,
    build_plan,
    select_earliest_executable_pilot,
    validate_selection,
)
from futures_rebuild.errors import IntegrityError


def _sessions(count: int = 1100) -> tuple[str, ...]:
    return tuple(f"{index:04d}" for index in range(count))


def test_rolling_pilot_uses_exact_locked_504_1_63_and_40() -> None:
    fold = _rolling_fold(_sessions(), 7)
    assert len(fold["training_sessions"]) == TRAINING_SESSIONS == 504
    assert len(fold["embargo_sessions"]) == 1
    assert len(fold["evaluation_sessions"]) == EVALUATION_SESSIONS == 63
    assert fold["purge_minutes"] == PURGE_MINUTES == 40
    assert fold["calendar_start_offset"] == 7


def test_tier1_folds_expand_without_reintroducing_31_minute_purge() -> None:
    folds = _outer_folds(_sessions())
    assert len(folds) == 8
    assert [len(item["training_sessions"]) for item in folds] == [
        504, 567, 630, 693, 756, 819, 882, 945,
    ]
    assert {item["purge_minutes"] for item in folds} == {40}
    assert {len(item["evaluation_sessions"]) for item in folds} == {63}


def test_pilot_selection_chooses_first_readiness_pass_without_returns(monkeypatch) -> None:
    import futures_rebuild.alpha_ladder_combined_readiness_v3 as module

    monkeypatch.setattr(
        module, "_candidate_failed_gates",
        lambda evidence: () if evidence["offset"] == 2 else ("SOURCE_GAP",),
    )

    def evidence_builder(**kwargs):
        return {"offset": kwargs["fold"]["calendar_start_offset"]}

    fold, evidence, selection = select_earliest_executable_pilot(
        sessions=_sessions(570), rows_by_session={}, risk_by_session={},
        evidence_builder=evidence_builder,
    )
    assert fold is not None and evidence == {"offset": 2}
    assert fold["calendar_start_offset"] == 2
    assert selection["candidates_examined"] == 3
    assert [item["status"] for item in selection["candidate_results"]] == [
        "FAIL", "FAIL", "PASS",
    ]
    assert selection["selection_inputs"] == "SOURCE_READINESS_ONLY_NO_RETURNS"
    assert "returns" not in inspect.signature(select_earliest_executable_pilot).parameters
    validate_selection(selection, selected_fold=fold)


def test_selection_rejects_skipping_an_earlier_passing_candidate() -> None:
    fold = _rolling_fold(_sessions(570), 1)
    selection = {
        "selection_rule": "EARLIEST_ROW_EXECUTABLE_ROLLING_504_1_63",
        "selection_inputs": "SOURCE_READINESS_ONLY_NO_RETURNS",
        "selected_calendar_start_offset": 1,
        "candidate_results": [
            {"calendar_start_offset": 0, "status": "PASS"},
            {"calendar_start_offset": 1, "status": "PASS"},
        ],
    }
    with pytest.raises(IntegrityError, match="earliest passing"):
        validate_selection(selection, selected_fold=fold)


def test_no_executable_fold_fails_closed(monkeypatch) -> None:
    import futures_rebuild.alpha_ladder_combined_readiness_v3 as module

    monkeypatch.setattr(module, "_candidate_failed_gates", lambda _evidence: ("SOURCE_GAP",))
    fold, evidence, selection = select_earliest_executable_pilot(
        sessions=_sessions(569), rows_by_session={}, risk_by_session={},
        evidence_builder=lambda **kwargs: {"offset": kwargs["fold"]["calendar_start_offset"]},
    )
    assert fold is None and evidence is None
    assert selection["decision"] == "NO_EXECUTABLE_PILOT_FOLD"
    validate_selection(selection, selected_fold=None)


def test_out_of_range_rolling_fold_fails_closed() -> None:
    with pytest.raises(IntegrityError, match="outside"):
        _rolling_fold(_sessions(567), 0)


def test_successor_plan_preserves_v2_and_denies_economic_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import futures_rebuild.alpha_ladder_combined_readiness as predecessor_module
    import futures_rebuild.alpha_ladder_combined_readiness_v3 as module

    root = tmp_path
    runner = root / "synthetic_runner.py"
    runner.write_text("# synthetic runner\n", encoding="utf-8")
    authority = {
        "historical_row_read": True,
        "returns": False,
        "model_fit": False,
        "prediction_generation": False,
        "performance_evaluation": False,
        "registration": False,
        "trial_execution": False,
        "publication": False,
        "provider_network_credentials": False,
        "year_2025_access": False,
        "active_data_mutation": False,
        "trading": False,
    }
    monkeypatch.setattr(module, "PREDECESSOR_BINDINGS", {})
    monkeypatch.setattr(module, "RUNNER_PATH", Path("synthetic_runner.py"))
    monkeypatch.setattr(
        predecessor_module,
        "build_plan",
        lambda **_kwargs: {
            "plan_id": "0" * 64,
            "schema_version": "synthetic-predecessor/1.0.0",
            "operation": "SYNTHETIC_READINESS",
            "mechanism_id": "1" * 64,
            "bindings": {},
            "execution_limits": {
                "worker_deadline_seconds": 3300,
                "maximum_runtime_seconds": 3600,
            },
            "authority": authority,
        },
    )
    plan = build_plan(root=root)
    assert plan["supersedes_report_id"] == (
        "261eeba727ce682c61a096f4f18201ae2403c6ccebd1fe087e291996908b01ba"
    )
    assert plan["pilot"]["purge_minutes"] == 40
    assert plan["tier_1"]["purge_minutes"] == 40
    assert plan["authority"] == authority
