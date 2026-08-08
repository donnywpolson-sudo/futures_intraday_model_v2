from __future__ import annotations

import copy
from pathlib import Path

import pytest

import futures_rebuild.alpha_ladder_full_regular_readiness as readiness
from futures_rebuild.alpha_ladder_full_regular_source_observable_successor import (
    CALENDAR_CLOSED,
    ELIGIBLE,
    HOLIDAY_ABSTENTION,
    SOURCE_ABSTENTION,
)
from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]


def test_plan_binds_exact_mechanism_tier0_ladder_calendar_and_twenty_sources() -> None:
    plan = readiness.build_plan(root=ROOT)
    assert plan["mechanism_id"] == "cfefe8ce78e46d1e6a68184cbebdf4f4fe6d46169dc7bbfcfcd501c595563dc3"
    assert plan["contract_id"] == "d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18"
    assert plan["calendar_id"] == "ddbe0c706d6568d8d7ddefd830677d73978b428d8a99925290310224f673a7f9"
    assert len(plan["protected_source_paths"]) == 20
    assert all(path.endswith(".parquet") for path in plan["protected_source_paths"])
    assert plan["execution_limits"]["maximum_attempts"] == 1
    assert plan["execution_limits"]["maximum_retries"] == 0


def test_plan_construction_never_hashes_protected_payloads(monkeypatch) -> None:
    original = readiness.sha256_file

    def guarded(path: Path) -> str:
        if path.suffix == ".parquet":
            raise AssertionError("plan construction hashed a protected payload")
        return original(path)

    monkeypatch.setattr(readiness, "sha256_file", guarded)
    readiness.build_plan(root=ROOT)


def test_full_regular_eligibility_precedes_folds_and_accounts_for_every_row() -> None:
    _pointer, calendar = readiness.base._active_calendar(ROOT)
    eligible, accounting = readiness._eligible_and_accounting(calendar)
    assert {market: len(sessions) for market, sessions in eligible.items()} == {
        "ES": 1248, "CL": 1248, "ZN": 1247, "6E": 1249,
    }
    assert len(accounting["inventory"]) == 7304
    dispositions = {item["disposition"] for item in accounting["inventory"]}
    assert dispositions == {CALENDAR_CLOSED, HOLIDAY_ABSTENTION, SOURCE_ABSTENTION, ELIGIBLE}


def test_plan_fails_closed_on_any_semantic_drift() -> None:
    plan = readiness.build_plan(root=ROOT)
    changed = copy.deepcopy(plan)
    changed["coverage"]["filled_entry_verified_exit_percent"] = 99
    with pytest.raises(IntegrityError, match="drifted"):
        readiness.validate_plan(changed, root=ROOT)


def test_execution_consumes_authority_before_protected_verification(monkeypatch, tmp_path: Path) -> None:
    plan = readiness.build_plan(root=ROOT)
    calls: list[bool] = []

    def fake_load(*, root: Path, verify_protected: bool = False):
        calls.append(verify_protected)
        if verify_protected:
            raise AssertionError("protected verification happened before authority")
        return plan

    class StopAtConsume:
        receipt_id = "synthetic"

        def consume(self, *args, **kwargs):
            raise RuntimeError("authority boundary reached")

    monkeypatch.setattr(readiness, "load_plan", fake_load)
    monkeypatch.setattr(readiness, "required_scope", lambda **_kwargs: {})
    monkeypatch.setattr(readiness, "OUTPUT_ROOT", Path("never-created-readiness-output"))
    with pytest.raises(RuntimeError, match="authority boundary reached"):
        readiness.execute_once(
            root=ROOT, boundary=RepoBoundary(active_root=ROOT), receipt=StopAtConsume(),
        )
    assert calls == [False]
    assert not (ROOT / "never-created-readiness-output").exists()


def test_required_authority_cannot_fit_predict_evaluate_register_or_access_2025() -> None:
    plan = readiness.build_plan(root=ROOT)
    assert plan["authority"]["historical_row_read"] is True
    for key in (
        "returns", "model_fit", "prediction_generation", "performance_evaluation",
        "registration", "trial_execution", "publication",
        "provider_network_credentials", "year_2025_access", "active_data_mutation",
        "trading",
    ):
        assert plan["authority"][key] is False
