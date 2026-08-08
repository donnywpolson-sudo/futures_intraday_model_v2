from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from futures_rebuild.tier1_bracket_post_audit import CausalBar
from futures_rebuild.tier1_bracket_v4 import BracketFill, ExpectedCheckpoint, FrozenPrediction, MarketSpec
from futures_rebuild.tier1_bracket_v5 import (
    CensusCheckpoint, MaterializedRowV5, NS_PER_MINUTE, OpportunityRecordV5,
    V5SourceRecord, load_v5_contract,
)
from futures_rebuild.tier1_bracket_v10_decision_validity import (
    DirectionalOutcomeResolutionV10,
    attach_causal_outcomes_v10,
    evaluate_crossfit_decision_availability_v10,
    evaluate_selected_path_coverage_v10,
    plan_strategy_rank_before_outcome_v10,
    prepare_crossfit_prediction_rows_v10,
    load_decision_validity_contract_v10,
    materialize_checkpoint_scoped_rows_v10,
    resolve_directional_outcomes_v10,
)


ROOT = Path(__file__).resolve().parents[1]
D = Decimal


def _bar(event: int, *, high: str = "100.25", low: str = "99.75") -> CausalBar:
    return CausalBar(
        event, event + NS_PER_MINUTE, event + NS_PER_MINUTE + 5_000_000_000,
        D("100"), D(high), D(low), D("100"), True,
    )


def _row(market: str, index: int, *, risk_eligible: bool = True, features: bool = True):
    session = "2019-07-01"
    opportunity_id = f"{market}-{index}"
    expected = ExpectedCheckpoint(opportunity_id, market, 2019, session, "08:30", 10**18 + index)
    ledger = OpportunityRecordV5(
        opportunity_id, market, session, "08:30", expected.decision_at_ns,
        "TRAINING_OR_PREDICTION_INELIGIBLE", False,
        expected.decision_at_ns - 2 * NS_PER_MINUTE,
        expected.decision_at_ns - NS_PER_MINUTE,
        outcome_coverage=("MISSING" if risk_eligible else "NOT_APPLICABLE_RISK_INELIGIBLE"),
    )
    return MaterializedRowV5(
        expected, ledger, ({"x": 1.0} if features else None), D("1"), "a" * 64,
        None, (), MarketSpec(D("0.25"), D("12.5"), D("50")), risk_eligible,
    )


def _prediction(opportunity_id: str, market: str, score: float) -> FrozenPrediction:
    return FrozenPrediction(
        opportunity_id, market, 2020, "2020-01-02", "08:30", 0,
        score, -score, "long", score, "long", 0.1, 0.01,
    )


def _fill() -> BracketFill:
    return BracketFill(
        120, 180, D("100"), D("101"), D("99"), D("101"), "TARGET",
        D("60"), D("10"), D("50"), D("100"),
    )


def _source(event: int, index: int) -> V5SourceRecord:
    spec = MarketSpec(D("0.25"), D("12.5"), D("50"))
    return V5SourceRecord(
        "ES", "2020-01-02", "ELIGIBLE", _bar(event), 10.0,
        "b" * 64, f"{index:064x}", spec,
    )


def test_crossfit_prediction_eligibility_ignores_future_outcomes_and_risk_abstention() -> None:
    rows = tuple(_row(market, 0, risk_eligible=(market != "ES")) for market in ("ES", "CL", "ZN", "6E"))
    prepared = prepare_crossfit_prediction_rows_v10(
        rows=rows, owner_sessions=("2019-07-01",),
    )
    assert all(row.ledger.prediction_produced for row in prepared)
    assert all(row.outcomes is None for row in prepared)
    evidence = evaluate_crossfit_decision_availability_v10(
        rows=prepared, owner_sessions=("2019-07-01",), unavailable_ids=(),
    )
    assert evidence["status"] == "PASS"
    assert evidence["decision_feature_eligible_opportunities"] == 4
    assert evidence["model_available_opportunities"] == 4
    assert evidence["risk_cap_policy_abstentions"] == 1


def test_decision_validity_contract_is_prepared_and_complete() -> None:
    inherited, contract = load_decision_validity_contract_v10(root=ROOT)
    assert inherited["strategy"]["minimum_predicted_net_r_after_stress_costs"] == "0.25"
    assert contract["state"] == "PREPARED_NOT_REGISTERED"
    assert contract["decision_validity_successor"]["missing_selected_outcome"].startswith("NO_RUNNER_UP")


def test_missing_feature_is_data_loss_but_missing_outcome_is_not_prediction_censoring() -> None:
    rows = [_row(market, 0) for market in ("ES", "CL", "ZN", "6E")]
    rows[0] = _row("ES", 0, features=False)
    prepared = prepare_crossfit_prediction_rows_v10(
        rows=tuple(rows), owner_sessions=("2019-07-01",),
    )
    assert sum(row.ledger.prediction_produced for row in prepared) == 3
    evidence = evaluate_crossfit_decision_availability_v10(
        rows=prepared, owner_sessions=("2019-07-01",), unavailable_ids=(),
    )
    assert evidence["status"] == "INCONCLUSIVE_DATA_OR_POWER"
    assert evidence["market_decision_feature_eligibility_rates"]["ES"] == 0.0


def test_gap_after_causal_exit_is_irrelevant_but_gap_before_exit_is_unresolved() -> None:
    decision = 1_600_000_000_000_000_000
    contract = load_v5_contract(root=ROOT)
    spec = MarketSpec(D("0.25"), D("12.5"), D("50"))
    early_terminal = resolve_directional_outcomes_v10(
        path_bars=(_bar(decision + NS_PER_MINUTE, high="110", low="90"),),
        decision_at_ns=decision, atr=D("0.5"), spec=spec,
        contract=contract, market="ES",
    )
    assert early_terminal.first_unresolved_event_at_ns == decision + 2 * NS_PER_MINUTE
    assert len(early_terminal.fills) == 6

    duplicate_after_exit = resolve_directional_outcomes_v10(
        path_bars=(
            _bar(decision + NS_PER_MINUTE, high="110", low="90"),
            _bar(decision + 2 * NS_PER_MINUTE),
            _bar(decision + 2 * NS_PER_MINUTE),
        ),
        decision_at_ns=decision, atr=D("0.5"), spec=spec,
        contract=contract, market="ES",
    )
    assert duplicate_after_exit.first_unresolved_event_at_ns == decision + 2 * NS_PER_MINUTE
    assert len(duplicate_after_exit.fills) == 6

    duplicate_entry = resolve_directional_outcomes_v10(
        path_bars=(
            _bar(decision + NS_PER_MINUTE),
            _bar(decision + NS_PER_MINUTE),
        ),
        decision_at_ns=decision, atr=D("0.5"), spec=spec,
        contract=contract, market="ES",
    )
    assert not duplicate_entry.fills

    unresolved = resolve_directional_outcomes_v10(
        path_bars=(_bar(decision + NS_PER_MINUTE),),
        decision_at_ns=decision, atr=D("2"), spec=spec,
        contract=contract, market="ES",
    )
    assert unresolved.first_unresolved_event_at_ns == decision + 2 * NS_PER_MINUTE
    assert not unresolved.fills

    row = _row("ES", 0)
    row = replace(
        row, atr=D("0.5"), execution_path=(_bar(row.expected.decision_at_ns + NS_PER_MINUTE, high="110", low="90"),),
    )
    attached, resolutions = attach_causal_outcomes_v10(rows=(row,), contract=contract)
    assert attached[0].outcomes is not None
    assert attached[0].ledger.outcome_coverage == "COMPLETE"
    assert len(resolutions[row.expected.opportunity_id].fills) == 6


def test_ranking_occurs_before_outcome_lookup_and_never_substitutes_runner_up() -> None:
    rows = (
        replace(_row("ES", 0), expected=ExpectedCheckpoint("ES", "ES", 2020, "2020-01-02", "08:30", 100)),
        replace(_row("CL", 0), expected=ExpectedCheckpoint("CL", "CL", 2020, "2020-01-02", "08:30", 100)),
    )
    predictions = (_prediction("ES", "ES", 0.6), _prediction("CL", "CL", 0.5))
    resolution = DirectionalOutcomeResolutionV10(
        {("stress", "long"): _fill()}, (_bar(NS_PER_MINUTE),), None,
    )
    plan = plan_strategy_rank_before_outcome_v10(
        strategy="candidate", predictions=predictions, rows=rows,
        scenario="stress", resolutions={"CL": resolution},
    )
    assert not plan.trades
    assert plan.preliminary_terminals == {
        "CL": "CROSS_MARKET_RANKING_LOSS", "ES": "MISSING_PRICE_PATH",
    }
    coverage = evaluate_selected_path_coverage_v10(plans={"candidate": plan})
    assert coverage["status"] == "INCONCLUSIVE_DATA_OR_COVERAGE"
    assert coverage["selected_intents_missing_causal_outcome"] == 1


def test_duplicate_timestamp_is_scoped_to_exact_checkpoint_dependencies() -> None:
    decision = 1_600_020_000_000_000_000
    expected = ExpectedCheckpoint("scope", "ES", 2020, "2020-01-02", "08:30", decision)
    census = (CensusCheckpoint(expected, True, "c" * 64),)
    events = [
        decision + offset * NS_PER_MINUTE
        for offset in range(-62, -1)
    ] + [
        decision + offset * NS_PER_MINUTE for offset in range(1, 62)
    ]
    source = [_source(event, index + 1) for index, event in enumerate(events)]
    outside = _source(decision - 200 * NS_PER_MINUTE, 500)
    outside_duplicate = _source(decision - 200 * NS_PER_MINUTE, 501)
    rows = materialize_checkpoint_scoped_rows_v10(
        source_rows=(*source, outside, outside_duplicate), census=census,
        market_specs={"ES": MarketSpec(D("0.25"), D("12.5"), D("50"))},
        contract=load_v5_contract(root=ROOT),
        prediction_scope_sessions=("2020-01-02",),
    )
    assert rows[0].ledger.terminal_disposition != "MISSING_OR_AMBIGUOUS_MARKET_IDENTITY"

    inside_duplicate = _source(decision - 20 * NS_PER_MINUTE, 502)
    affected = materialize_checkpoint_scoped_rows_v10(
        source_rows=(*source, inside_duplicate), census=census,
        market_specs={"ES": MarketSpec(D("0.25"), D("12.5"), D("50"))},
        contract=load_v5_contract(root=ROOT),
        prediction_scope_sessions=("2020-01-02",),
    )
    assert affected[0].ledger.terminal_disposition == "MISSING_OR_AMBIGUOUS_MARKET_IDENTITY"
    assert not affected[0].ledger.prediction_produced
