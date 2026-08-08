from __future__ import annotations

from futures_rebuild import alpha_ladder_limit_readiness as v1
from futures_rebuild import alpha_ladder_limit_readiness_v4 as v4
from futures_rebuild.alpha_ladder_combined_readiness_v3 import (
    select_earliest_executable_pilot,
)
from futures_rebuild.alpha_ladder_limit_readiness_v2 import _fold_evidence
from datetime import date, timedelta
from tests.test_alpha_ladder_limit_readiness_v2 import _bars


def test_v4_runs_complete_nested_adapter_chain_without_recursion(monkeypatch, tmp_path) -> None:
    plan_path = tmp_path / "plan.json"; plan_path.write_text("{}", encoding="utf-8")
    plan = {"plan_id": "1" * 64, "mechanism_id": "2" * 64,
            "execution_limits": {"worker_deadline_seconds": 3300,
                                 "maximum_runtime_seconds": 3600}}
    monkeypatch.setattr(v4, "load_plan", lambda *, root: plan)
    monkeypatch.setattr(v4, "PLAN_PATH", plan_path)
    observed = {}

    def fake_v1_execute_once(*, root, boundary, receipt):
        observed["scope"] = v1.required_scope(root=root, plan=plan)
        fold = {"fold_id": "fold-0", "training_sessions": ["2020-01-02"],
                "evaluation_sessions": ["2020-01-02"], "purge_minutes": 40,
                "embargo_sessions": ["2020-01-01"]}
        observed["evidence"] = v1._fold_evidence(
            market="ES", fold=fold,
            rows_by_session={"2020-01-02": _bars(),
                             "__cost_ticks__": {"base": 2, "stress": 4, "extreme": 8}},
            risk_by_session={},
        )
        return {"ok": True}

    monkeypatch.setattr(v1, "execute_once", fake_v1_execute_once)
    assert v4.execute_once(root=tmp_path, boundary=object(), receipt=object()) == {"ok": True}
    assert observed["scope"]["output_root"] == v4.OUTPUT_ROOT.as_posix()
    assert observed["scope"]["approval_plan_id"] == "1" * 64
    assert all(set(item) == {"expected_sessions", "terminal_sessions", "selected_sessions",
                             "selected_path_complete_sessions", "scenario_risk_dispositions",
                             "schedule_independently_derived", "flat_no_trade"}
               for item in observed["evidence"]["baseline_universe_readiness"].values())


def test_full_504_1_63_pilot_selection_and_certificate_path_is_synthetic_green() -> None:
    dates = tuple(date(2018, 1, 1) + timedelta(days=index) for index in range(568))
    sessions = tuple(item.isoformat() for item in dates)
    rows = {session: _bars(session_date) for session, session_date in zip(sessions, dates)}
    rows["__cost_ticks__"] = {"base": 2, "stress": 4, "extreme": 8}
    fold, evidence, selection = select_earliest_executable_pilot(
        sessions=sessions, rows_by_session=rows, risk_by_session={},
        evidence_builder=_fold_evidence,
    )
    assert fold is not None and evidence is not None
    assert selection["decision"] == "EARLIEST_EXECUTABLE_PILOT_SELECTED"
    assert selection["selected_calendar_start_offset"] == 0
    assert len(fold["training_sessions"]) == 504
    assert len(fold["evaluation_sessions"]) == 63
