from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from futures_rebuild.alpha_ladder_combined_readiness import (
    PriceBar, _risk_dispositions, _session_results,
)
from futures_rebuild.research_gateway_policy import (
    ALPHA_LADDER_READINESS_CENSUS_OPERATION,
    require_current_real_history_operation,
)


CT = ZoneInfo("America/Chicago")


def _bars(*, tick_value: str = "12.5", future_last: bool = False) -> list[PriceBar]:
    start = datetime(2020, 1, 2, 9, 30, tzinfo=CT)
    result = []
    for index in range(30):
        event = start + timedelta(minutes=index)
        available = event + timedelta(seconds=65)
        if future_last and index >= 28:
            available = datetime(2020, 1, 2, 10, 0, 6, tzinfo=CT)
        result.append(PriceBar(
            event_at_ns=int(event.timestamp() * 1_000_000_000),
            available_at_ns=int(available.timestamp() * 1_000_000_000),
            identity="ES-contract", high=Decimal("101"), low=Decimal("100"),
            close=Decimal("100.5"), volume=Decimal("10"), tick_size=Decimal("0.25"),
            tick_value=Decimal(tick_value),
        ))
    return result


def test_risk_disposition_is_scenario_specific_and_terminal() -> None:
    result = _risk_dispositions(_bars(), {"base": 2, "stress": 4, "extreme": 8})
    assert result == {"base": "FEASIBLE", "stress": "FEASIBLE", "extreme": "FEASIBLE"}
    rejected = _risk_dispositions(
        _bars(tick_value="50"), {"base": 2, "stress": 4, "extreme": 8},
    )
    assert rejected == {
        "base": "RISK_ABSTENTION", "stress": "RISK_ABSTENTION",
        "extreme": "RISK_ABSTENTION",
    }


def test_feature_bar_unavailable_at_decision_fails_closed() -> None:
    assert _risk_dispositions(
        _bars(future_last=True), {"base": 2, "stress": 4, "extreme": 8},
    ) is None


def test_feature_abstention_is_terminal_but_not_training_complete() -> None:
    result = _session_results(("2020-01-02",), {}, {})
    assert result[0][1].disposition == "EXPLICIT_CAUSAL_FEATURE_ABSTENTION"
    assert result[0][2] is False


def test_combined_readiness_is_preparatory_not_trial_execution() -> None:
    require_current_real_history_operation(ALPHA_LADDER_READINESS_CENSUS_OPERATION, {})
