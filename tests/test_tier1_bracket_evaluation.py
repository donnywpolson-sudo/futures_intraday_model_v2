from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_bracket_evaluation import (
    ALL_BASELINES,
    _BaselinePath,
    _Candidate,
    _evaluate_strategy,
    _report_payload,
)
from futures_rebuild.tier1_phase8_evaluation_config import load_tier1_phase8_evaluation_config


ROOT = Path(__file__).parents[1]


def _candidate(
    *, market: str = "ES", year: int = 2020, entry: int = 10, exit_at: int = 20,
    gross: str = "100", direction: str = "long", session: str = "2020-01-02",
    fold: int = 0, baseline_exit_at: int | None = None, candidate_active: bool = True,
) -> _Candidate:
    risk = Decimal("200")
    baseline_exit = exit_at if baseline_exit_at is None else baseline_exit_at
    long = _BaselinePath("long", baseline_exit, risk, Decimal(gross))
    short = _BaselinePath("short", exit_at, risk, Decimal(gross))
    selected = long if direction == "long" else short
    paths = {
        "fold_local_unconditional_return_by_market_session": long,
        "previous_bar_sign_momentum": long,
        "previous_bar_sign_reversal": short,
        "risk_matched_always_long_intraday": long,
        "equal_risk_version_of_candidate_signal": selected if candidate_active else None,
    }
    return _Candidate(
        key=f"{market}-{year}-{entry}", market=market, year=year, session=session,
        entry_at_ns=entry, exit_at_ns=exit_at, direction=direction, score=0.8,
        risk=risk, candidate_gross=Decimal(gross),
        baselines={name: path.gross_pnl_usd if path is not None else Decimal("0") for name, path in paths.items()},
        tick_value=Decimal("12.50"), outer_fold=fold, exit_reason="MAX_HOLD",
        baseline_paths=paths, candidate_active=candidate_active,
    )


def test_flat_no_trade_has_zero_turnover_cost_and_pnl() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    result = _evaluate_strategy(
        candidates=(_candidate(),), evaluation_config=config, scenario="base", strategy="flat_no_trade",
    )

    assert result["metrics"]["net_pnl_usd"] == "0"
    assert result["metrics"]["turnover_contract_equivalents"] == 0
    assert result["scheduler"]["admitted_count"] == 0


def test_baseline_uses_its_own_exit_and_scheduler_path() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    candidates = (
        _candidate(entry=10, exit_at=30, direction="short", baseline_exit_at=15),
        _candidate(entry=20, exit_at=25, direction="short", baseline_exit_at=25),
    )

    candidate = _evaluate_strategy(
        candidates=candidates, evaluation_config=config, scenario="base", strategy="candidate",
    )
    baseline = _evaluate_strategy(
        candidates=candidates, evaluation_config=config, scenario="base",
        strategy="risk_matched_always_long_intraday",
    )

    assert candidate["scheduler"]["admitted_count"] == 1
    assert candidate["scheduler"]["overlap_abstentions"] == 1
    assert candidate["by_market_year"]["ES/2020"]["net_directional_contract_equivalents"] == -1
    assert baseline["scheduler"]["admitted_count"] == 2
    assert baseline["scheduler"]["overlap_abstentions"] == 0


def test_missing_baseline_path_fails_closed() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    original = _candidate()
    invalid = _Candidate(**{**original.__dict__, "baseline_paths": {}})

    with pytest.raises(IntegrityError, match="lacks independent path"):
        _evaluate_strategy(
            candidates=(invalid,), evaluation_config=config, scenario="base",
            strategy="previous_bar_sign_momentum",
        )


def test_active_baseline_keeps_its_own_entry_when_candidate_is_neutral() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    opportunity = _candidate(candidate_active=False)

    candidate = _evaluate_strategy(
        candidates=(opportunity,), evaluation_config=config, scenario="base", strategy="candidate",
    )
    always_long = _evaluate_strategy(
        candidates=(opportunity,), evaluation_config=config, scenario="base",
        strategy="risk_matched_always_long_intraday",
    )
    identical = _evaluate_strategy(
        candidates=(opportunity,), evaluation_config=config, scenario="base",
        strategy="equal_risk_version_of_candidate_signal",
    )

    assert candidate["scheduler"]["admitted_count"] == 0
    assert candidate["scheduler"]["neutral_abstentions"] == 1
    assert always_long["scheduler"]["admitted_count"] == 1
    assert always_long["scheduler"]["neutral_abstentions"] == 0
    assert identical["scheduler"]["admitted_count"] == 0
    assert identical["scheduler"]["neutral_abstentions"] == 1


def test_independent_market_year_diagnostics_reset_earlier_drawdown() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    candidates = []
    entry = 100
    for year in (2020, 2021, 2022):
        for market in ("ES", "CL", "ZN", "6E"):
            gross = "-1600" if (market, year) == ("ES", 2020) else "100"
            candidates.append(_candidate(
                market=market, year=year, entry=entry, exit_at=entry + 5, gross=gross,
                session=f"{year}-01-{entry}", fold=year - 2020,
            ))
            entry += 10

    model, risk = _report_payload(
        candidates=tuple(candidates), evaluation_config=config,
        trial_id="synthetic-trial", prediction_index_id="synthetic-index",
    )
    base = model["cost_scenarios"]["base"]

    assert model["schema_version"] == "tier1_bracket_evaluation/2.0.0"
    assert base["baseline_net_pnl_usd"]["flat_no_trade"] == "0"
    assert base["by_market_year"]["ES/2021"]["observation_count"] == 0
    assert base["independent_market_year"]["ES/2021"]["candidate"]["metrics"]["observation_count"] == 1
    assert base["continuous_account"]["strategies"]["candidate"]["scheduler"]["drawdown_stop_abstentions"] > 0
    assert risk["cost_scenarios"]["base"]["independent_market_year"]["ES/2021"]["metrics"]["observation_count"] == 1
    assert set(base["continuous_account"]["strategies"]) == {"candidate", *ALL_BASELINES}
