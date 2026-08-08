from __future__ import annotations

from futures_rebuild import alpha_ladder_limit_readiness as v1
from futures_rebuild import alpha_ladder_limit_readiness_v2 as v2
from futures_rebuild import alpha_ladder_limit_readiness_v3 as v3
from tests.test_alpha_ladder_limit_readiness_v2 import _bars


def test_v3_execution_adapter_uses_exact_plan_and_scope_without_recursion(
    monkeypatch, tmp_path,
) -> None:
    plan_path = tmp_path / "plan.json"
    plan_path.write_text("{}", encoding="utf-8")
    plan = {"plan_id": "1" * 64, "mechanism_id": "2" * 64,
            "execution_limits": {"worker_deadline_seconds": 3300,
                                 "maximum_runtime_seconds": 3600}}
    monkeypatch.setattr(v3, "load_plan", lambda *, root: plan)
    monkeypatch.setattr(v3, "PLAN_PATH", plan_path)
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
    assert v3.execute_once(root=tmp_path, boundary=object(), receipt=object()) == {"ok": True}
    assert observed["scope"]["approval_plan_id"] == "1" * 64
    assert observed["scope"]["output_root"] == v3.OUTPUT_ROOT.as_posix()
    assert all("readiness_universe" not in item
               for item in observed["evidence"]["baseline_universe_readiness"].values())
