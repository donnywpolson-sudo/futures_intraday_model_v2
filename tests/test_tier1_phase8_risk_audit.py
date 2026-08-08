from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_phase8_evaluation_config import load_tier1_phase8_evaluation_config
from futures_rebuild.tier1_phase8_evaluator import Phase8SyntheticTrade
from futures_rebuild.tier1_phase8_risk_audit import (
    MINUTE_NS,
    Phase8RiskAuditPath,
    audit_tier1_phase8_risk_synthetic,
    main,
    run_default_tier1_risk_realism_audit,
    write_local_risk_audit_report,
)


ROOT = Path(__file__).parents[1]


def _path(
    *,
    scenario: str = "path",
    market: str = "ES",
    session: int = 1,
    entry: int = 0,
    exit: int | None = MINUTE_NS,
    planned: str = "250",
    worst_open: str = "-25",
    gross: str | None = "500",
    reason: str | None = "target",
    tick_value: str = "12.50",
    extra_ticks: int = 1,
    complete: bool = True,
    fresh: bool = True,
    minutes_to_roll: int = 120,
) -> Phase8RiskAuditPath:
    return Phase8RiskAuditPath(
        scenario, market, session, entry, exit, Decimal(planned), Decimal(worst_open),
        None if gross is None else Decimal(gross), Decimal(tick_value), reason,
        extra_ticks, complete, fresh, minutes_to_roll,
    )


def test_cost_scenarios_apply_one_to_four_extra_ticks() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    paths = tuple(
        _path(scenario=f"ES_{ticks}", session=ticks, entry=ticks * 2 * MINUTE_NS, exit=(ticks * 2 + 1) * MINUTE_NS, extra_ticks=ticks)
        for ticks in range(1, 5)
    )
    result = audit_tier1_phase8_risk_synthetic(paths=paths, evaluation_config=config)

    base = result.scenarios["base"].path_results
    stress = result.scenarios["stress"].path_results
    extreme = result.scenarios["extreme"].path_results
    assert [item.configured_cost_usd for item in base] == sorted(item.configured_cost_usd for item in base)
    assert all(stress[index].configured_cost_usd > base[index].configured_cost_usd for index in range(4))
    assert all(extreme[index].configured_cost_usd > stress[index].configured_cost_usd for index in range(4))


def test_stale_missing_late_and_over_risk_paths_are_rejected() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    result = audit_tier1_phase8_risk_synthetic(
        paths=(
            _path(scenario="over_risk", planned="251"),
            _path(scenario="missing", market="CL", complete=False, exit=None, gross=None, reason=None),
            _path(scenario="stale", market="ZN", fresh=False),
            _path(scenario="late", market="6E", minutes_to_roll=59),
        ),
        evaluation_config=config,
    )

    assert all(item.rejected_trade_count == 4 for item in result.scenarios.values())
    assert all(item.path_results[0].disposition == "REJECTED_ADMISSION_OR_SOURCE" for item in result.scenarios.values())


def test_daily_and_total_drawdown_breaches_force_flatten_and_block_later_entries() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    daily = (
        _path(scenario="loss_one", gross="-230", worst_open="-260", reason="stop"),
        _path(scenario="loss_two", entry=2 * MINUTE_NS, exit=3 * MINUTE_NS, gross="-230", worst_open="-260", reason="stop"),
        _path(scenario="after_daily", entry=4 * MINUTE_NS, exit=5 * MINUTE_NS),
    )
    result = audit_tier1_phase8_risk_synthetic(paths=daily, evaluation_config=config)
    assert result.scenarios["base"].forced_flatten_count >= 1
    assert result.scenarios["base"].skipped_trade_count == 1

    report = run_default_tier1_risk_realism_audit(evaluation_config=config).report()
    assert report["scenarios"]["extreme"]["drawdown_stop_triggered"] is True
    assert report["scenarios"]["extreme"]["forced_flatten_count"] >= 1


def test_report_discloses_hold_buckets_and_non_live_boundary() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    report = run_default_tier1_risk_realism_audit(evaluation_config=config).report()

    assert report["kind"] == "LOCAL_SYNTHETIC_ONLY_NOT_LIVE_VALIDATION"
    assert report["live_realism_claim_supported"] is False
    assert "margin_and_buying_power" in report["blocked_live_assumptions"]
    assert "configured_base_stress_extreme_cost_math" in report["supported_simulation_claims"]
    assert "31_60_minutes" in report["scenarios"]["base"]["hold_time_buckets"]
    assert "safety_exit" in report["scenarios"]["base"]["hold_time_buckets"]
    assert any(item["scenario_id"] == "slippage_6E_4_ticks" for item in report["scenarios"]["extreme"]["path_results"])


def test_preclosed_evaluator_trade_is_rejected_as_intratrade_audit_input() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    old = Phase8SyntheticTrade("ES", 2018, 1, 1, Decimal("250"), Decimal("1"), Decimal("12.5"), {})

    with pytest.raises(IntegrityError, match="intratrade path"):
        audit_tier1_phase8_risk_synthetic(paths=(old,), evaluation_config=config)  # type: ignore[arg-type]


def test_local_report_writer_and_cli_create_no_release(tmp_path) -> None:
    output = tmp_path / "risk-audit.json"
    written = write_local_risk_audit_report(root=ROOT, output=output)
    assert written == output
    payload = output.read_text(encoding="utf-8")
    assert "LOCAL_SYNTHETIC_ONLY_NOT_LIVE_VALIDATION" in payload

    cli_output = tmp_path / "risk-audit-cli.json"
    assert main(["--root", str(ROOT), "--output", str(cli_output)]) == 0
    assert cli_output.is_file()
