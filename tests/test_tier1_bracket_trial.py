from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_bracket_trial import (
    BracketBar,
    BracketExecutionProtocol,
    bracket_trial_metadata,
    build_directional_bracket_outcome,
    load_registered_tier1_bracket_trial,
    load_tier1_bracket_trial_contract,
    require_baseline_execution_parity,
    wilder_atr_nano,
)


MINUTE = 60_000_000_000


def _bars(*, count: int = 22, last_open: int = 100, last_high: int = 105, last_low: int = 95, identity: str = "a" ) -> list[BracketBar]:
    rows = [
        BracketBar(index * MINUTE, 100, 105, 95, 100, "2021-01-04", identity)
        for index in range(count)
    ]
    rows[-1] = BracketBar((count - 1) * MINUTE, last_open, last_high, last_low, last_open, "2021-01-04", identity)
    return rows


def _outcome(bars: list[BracketBar], *, direction: str = "long", tick_value: str = "1"):
    return build_directional_bracket_outcome(
        bars=bars,
        decision_index=20,
        direction=direction,  # type: ignore[arg-type]
        tick_size_nano=1,
        tick_value_usd=Decimal(tick_value),
        stress_round_trip_cost_usd=Decimal("0"),
    )


def test_wilder_atr_uses_only_completed_contiguous_same_identity_bars() -> None:
    bars = _bars()
    assert wilder_atr_nano(bars=bars, decision_index=20) == Decimal("10")
    bars[10] = BracketBar(10 * MINUTE, 100, 105, 95, 100, "2021-01-04", "other")
    assert wilder_atr_nano(bars=bars, decision_index=20) is None


def test_target_is_directional_net_two_r_and_unlock_is_conservative_maximum() -> None:
    outcome = _outcome(_bars(last_high=130, last_low=95))

    assert outcome.status == "MATURED"
    assert outcome.exit_reason == "TARGET"
    assert outcome.planned_all_in_risk_usd == Decimal("15")
    assert outcome.realized_net_r == Decimal("2")
    assert outcome.label_unlock_at_ns == 80 * MINUTE
    assert outcome.exit_at_ns == 22 * MINUTE

    short = _outcome(_bars(last_high=105, last_low=70), direction="short")
    assert short.exit_reason == "TARGET"
    assert short.realized_net_r == Decimal("2")


def test_target_remains_net_two_r_after_the_locked_stress_costs() -> None:
    outcome = build_directional_bracket_outcome(
        bars=_bars(last_high=140, last_low=95),
        decision_index=20,
        direction="long",
        tick_size_nano=1,
        tick_value_usd=Decimal("1"),
        stress_round_trip_cost_usd=Decimal("2"),
    )

    # ATR is 10, so the 1.5-ATR stop costs 15 plus 2 in round-trip costs.
    # The target must therefore net 34 / 17 = 2R, rather than merely gross 2R.
    assert outcome.planned_all_in_risk_usd == Decimal("17")
    assert outcome.exit_reason == "TARGET"
    assert outcome.realized_net_r == Decimal("2")


def test_same_bar_collision_is_stop_first_and_gap_can_exceed_planned_risk() -> None:
    collision = _outcome(_bars(last_high=130, last_low=85))
    gap = _outcome(_bars(count=23, last_open=70, last_high=75, last_low=65))

    assert collision.exit_reason == "STOP_FIRST_COLLISION"
    assert collision.realized_net_r == Decimal("-1")
    assert gap.exit_reason == "STOP_GAP"
    assert gap.realized_net_r < Decimal("-1")


def test_risk_cap_and_missing_or_roll_entry_abstain() -> None:
    assert _outcome(_bars(last_high=130), tick_value="20").status == "ABSTAIN_INITIAL_RISK_CAP"
    bars = _bars(identity="a")
    bars[21] = BracketBar(21 * MINUTE, 100, 105, 95, 100, "2021-01-04", "b")
    assert _outcome(bars).status == "ABSTAIN_MISSING_OR_ROLL_ENTRY"


def test_session_roll_missing_and_max_hold_exits_fail_closed_or_close_safely() -> None:
    session_end = _bars(count=23)
    session_end[22] = BracketBar(22 * MINUTE, 100, 105, 95, 100, "2021-01-05", "a")
    assert _outcome(session_end).exit_reason == "SESSION_END"

    rolled = _bars(count=23)
    rolled[22] = BracketBar(22 * MINUTE, 100, 105, 95, 100, "2021-01-04", "b")
    assert _outcome(rolled).exit_reason == "ROLL_BOUNDARY"

    missing = _bars(count=23)
    missing[22] = BracketBar(23 * MINUTE, 100, 105, 95, 100, "2021-01-04", "a")
    assert _outcome(missing).status == "ABSTAIN_MISSING_SOURCE"

    held = _outcome(_bars(count=81))
    assert held.exit_reason == "MAX_HOLD"


def test_protocol_parity_and_metadata_reject_live_claims() -> None:
    protocol = BracketExecutionProtocol()
    require_baseline_execution_parity(candidate=protocol, baseline=protocol)
    with pytest.raises(IntegrityError, match="baseline execution protocol"):
        require_baseline_execution_parity(candidate=protocol, baseline=BracketExecutionProtocol(cost_scenario="base"))

    metadata = bracket_trial_metadata(protocol=protocol)
    assert metadata["live_readiness"] is False
    assert "apex_readiness" in metadata["forbidden_claims"]
    assert metadata["locked_untouched_holdout"] == "2025"


def test_trial_contract_locks_discovery_scope_and_holdout() -> None:
    contract = load_tier1_bracket_trial_contract(root=Path(__file__).parents[1])
    assert contract["discovery_period"] == "2018-2022"
    assert contract["locked_untouched_holdout"] == "2025"


def test_registered_trial_state_is_read_from_its_registry_not_template() -> None:
    state = load_registered_tier1_bracket_trial(root=Path(__file__).parents[1])

    assert state is not None
    assert state["trial_id"] == "035955798cd0176732365b9706487ee3bfa6b1a4afa3d0047eeb1ee60744d3ba"
    assert state["registration_state"] == "CURRENT_REGISTERED_BEFORE_BRACKET_SOURCE_ROW_OPEN"
    assert state["template_state"] == "LOCAL_IMPLEMENTATION_ONLY_NOT_REGISTERED"
