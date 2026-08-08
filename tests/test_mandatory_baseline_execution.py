from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from futures_rebuild import tier1_bracket_v5 as v5
from futures_rebuild.canonical import sha256_json
from futures_rebuild.tier1_bracket_post_audit import CausalBar
from futures_rebuild.tier1_bracket_v4 import (
    ExpectedCheckpoint, FrozenPrediction, MarketSpec,
)
from futures_rebuild.tier1_final_decision_validity import plan_final_strategy
from futures_rebuild.tier1_mandatory_baseline_execution import (
    evaluate_mandatory_baseline_pipeline,
    materialize_mandatory_baseline_rows,
)
from futures_rebuild.tier1_standard_only_protocol import load_standard_only_protocol
from tests.test_tier1_frozen_source_adequacy import _checkpoint
from tests.test_tier1_frozen_source_semantics import _rows


ROOT = Path(__file__).resolve().parents[1]


def _contract() -> dict[str, object]:
    return deepcopy(load_standard_only_protocol(root=ROOT))


def _prediction(opportunity_id: str) -> FrozenPrediction:
    return FrozenPrediction(
        opportunity_id, "ES", 2020, "2020-01-02", "08:30", 0,
        0.5, -0.1, "long", 0.5, "long", 0.2, 0.01,
    )


def test_stress_abstention_preserves_base_baseline_execution() -> None:
    contract = _contract()
    costs = contract["costs"]  # type: ignore[index]
    costs["round_trip_adverse_execution_ticks"]["stress"]["ES"] = 100
    offsets = [
        *[offset for offset in range(-64, -1) if offset not in {-30, -20}],
        1, 2, 5, 20, 40, 61,
    ]
    source = _rows(offsets)
    rows, resolutions = materialize_mandatory_baseline_rows(
        source_rows=source,
        census=(_checkpoint(),),
        market_specs={"ES": source[0].market_spec},
        contract=contract,
        prediction_scope_sessions=("2020-01-02",),
    )
    row = rows[0]
    prediction = _prediction(row.expected.opportunity_id)
    assert row.risk_eligible is False
    assert resolutions[row.expected.opportunity_id].fill(
        scenario="base", direction="long",
    ) is not None

    base = plan_final_strategy(
        strategy="candidate", predictions=(prediction,), rows=rows,
        scenario="base", resolutions=resolutions, contract=contract,
    )
    assert len(base.trades) == 1
    assert "MISSING_PRICE_PATH" not in base.preliminary_terminals.values()

    stress = plan_final_strategy(
        strategy="candidate", predictions=(prediction,), rows=rows,
        scenario="stress", resolutions=resolutions, contract=contract,
    )
    assert stress.trades == ()
    assert stress.preliminary_terminals[prediction.opportunity_id] == (
        "RISK_CAP_REJECTION"
    )


def _complete_synthetic_inputs() -> tuple[
    dict[tuple[str, int], tuple[v5.V5SourceRecord, ...]],
    tuple[v5.CensusCheckpoint, ...],
]:
    chicago = ZoneInfo("America/Chicago")
    training_dates = [
        date(2018, 1, 2) + timedelta(days=index) for index in range(30)
    ] + [
        date(2019, 1, 2) + timedelta(days=index) for index in range(30)
    ]
    evaluation_dates = [
        date(year, 1, 2) + timedelta(days=index)
        for year in (2020, 2021, 2022) for index in range(10)
    ]
    sessions = training_dates + evaluation_dates
    census: list[v5.CensusCheckpoint] = []
    streams: dict[tuple[str, int], list[v5.V5SourceRecord]] = {
        (market, year): []
        for market in v5.MARKETS for year in range(2018, 2023)
    }
    spec = MarketSpec(Decimal("0.25"), Decimal("12.50"), Decimal("50"))
    ordinal = 0
    for session_index, session_date in enumerate(sessions):
        session = session_date.isoformat()
        decisions = {
            checkpoint: int(datetime.combine(
                session_date,
                time(*[int(value) for value in checkpoint.split(":")]),
                chicago,
            ).timestamp() * 1_000_000_000)
            for checkpoint in v5.CHECKPOINTS
        }
        for market_index, market in enumerate(v5.MARKETS):
            for checkpoint in v5.CHECKPOINTS:
                decision = decisions[checkpoint]
                core = {
                    "market": market, "session": session,
                    "checkpoint": checkpoint, "decision": decision,
                }
                census.append(v5.CensusCheckpoint(
                    ExpectedCheckpoint(
                        sha256_json(core), market, session_date.year,
                        session, checkpoint, decision,
                    ),
                    True, "c" * 64,
                ))
            first = decisions["08:30"] - 70 * v5.NS_PER_MINUTE
            last = decisions["13:30"] + 61 * v5.NS_PER_MINUTE
            event = first
            while event <= last:
                ordinal += 1
                minute_index = (event - first) // v5.NS_PER_MINUTE
                center = (
                    Decimal("100") + Decimal(market_index * 5)
                    + Decimal(session_index % 7) / Decimal("10")
                    + Decimal(minute_index % 23) / Decimal("100")
                )
                close = center + Decimal((minute_index % 3) - 1) / Decimal("100")
                streams[(market, session_date.year)].append(v5.V5SourceRecord(
                    market, session, "ELIGIBLE",
                    CausalBar(
                        event, event + v5.NS_PER_MINUTE,
                        event + v5.NS_PER_MINUTE + 5_000_000_000,
                        center, max(center, close) + Decimal("0.25"),
                        min(center, close) - Decimal("0.25"), close, True,
                    ),
                    float(100 + ordinal % 37), "d" * 64,
                    f"{ordinal:064x}", spec,
                ))
                event += v5.NS_PER_MINUTE
    return (
        {key: tuple(value) for key, value in streams.items()},
        tuple(census),
    )


def test_complete_mandatory_baselines_reach_performance_decision() -> None:
    streams, census = _complete_synthetic_inputs()
    result = evaluate_mandatory_baseline_pipeline(
        streams=streams, census=census, contract=_contract(), trial_id="e" * 64,
    )
    for coverage in (
        result.selected_path_coverage,
        result.nested_selected_path_coverage,
    ):
        assert all(
            scenario["passed"] is True
            and all(missing == 0 for missing in scenario["by_strategy"].values())
            for scenario in coverage.values()
        )
    assert result.outer_completeness["passed"] is True
    assert result.nested_completeness["passed"] is True
    assert result.complete_decision_reached is True
    assert result.decision["classification"] != "INCONCLUSIVE_DATA_OR_COVERAGE"
