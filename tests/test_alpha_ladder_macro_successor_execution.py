from __future__ import annotations

from datetime import datetime, time
from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_macro_successor import load_successor_contract
from futures_rebuild.alpha_ladder_macro_successor_execution import aggregate_candidate
from futures_rebuild.alpha_ladder_zn_6e_execution_proxy import CHICAGO
from futures_rebuild.causal_nine_market_successor import _protected_sr3_entries


pytestmark = pytest.mark.current
ROOT = Path(__file__).resolve().parents[1]


def _bundle(date_text: str) -> dict[str, list[dict[str, object]]]:
    midnight = datetime.combine(datetime.fromisoformat(date_text).date(), time(), CHICAGO)
    observations = []
    missingness = []
    quality = []
    roll = []
    tick = 2_500_000
    for ordinal, minute in enumerate(range(8 * 60 + 35, 15 * 60)):
        local = midnight.replace(hour=minute // 60, minute=minute % 60)
        start = int(local.timestamp() * 1_000_000_000)
        end = start + 60_000_000_000
        observations.append({
            "actual_contract": "SR3Z20", "available_at_ns": end,
            "bar_end_ns": end, "bar_start_ns": start, "currency": "USD",
            "decision_eligible_at_ns": end, "definition_event_at_ns": start,
            "definition_received_at_ns": start, "expiration_ns": 2**64 - 1,
            "instrument_id": 1, "listing_activation_ns": 0, "market": "SR3",
            "min_price_increment_nano": tick, "multiplier_nano": 2_500_000_000_000,
            "open_nano": 95_000_000_000 + ordinal * tick,
            "project_timezone": "America/Chicago", "project_trade_date": date_text,
            "publisher_id": 1, "raw_symbol": "SR3Z20", "volume": 1,
        })
        missingness.append({
            "observation_ordinal": ordinal, "state": "OBSERVED_VALID", "market": "SR3",
            "interval_start_ns": start, "interval_end_ns": end,
        })
        quality.append({
            "observation_ordinal": ordinal, "ohlc_valid": True, "volume_valid": True,
            "timestamp_order_valid": True, "duplicate_state": "UNIQUE",
        })
        roll.append({"observation_ordinal": ordinal, "roll_flag": False})
    return {"observations": observations, "missingness": missingness, "quality": quality, "roll": roll}


def test_aggregate_candidate_uses_the_frozen_horizon_and_economics() -> None:
    contract = load_successor_contract(ROOT)
    evidence = aggregate_candidate(
        contract, "SR3", ["2020-01-02"], [_bundle("2020-01-02")],
        {"point_value": Decimal("2500"), "expected_unit_qty": Decimal("2500")},
    )
    assert evidence["target_horizon_movement_ticks"] == "30"
    assert evidence["conservative_round_trip_friction_ticks"] == {
        "value": "5.6", "tick_value_usd": "6.2500",
    }
    assert evidence["active_minute_coverage"] == {
        "usable_sessions": 1, "usable_observations": 385, "ratio": "1",
    }
    assert evidence["missingness_and_continuity"]["point_in_time_metadata_coverage_ratio"] == "1"


def test_aggregate_candidate_fails_closed_on_economics_drift() -> None:
    contract = load_successor_contract(ROOT)
    evidence = aggregate_candidate(
        contract, "SR3", ["2020-01-02"], [_bundle("2020-01-02")],
        {"point_value": Decimal("2500"), "expected_unit_qty": Decimal("1")},
    )
    assert set(evidence.values()) == {None}


def test_sr3_prelisting_years_are_explicitly_inaccessible() -> None:
    entries = _protected_sr3_entries("a" * 64)
    assert [entry.year for entry in entries] == list(range(2010, 2018))
    assert all(entry.disposition == "QUARANTINED_NOT_MATERIALIZED" for entry in entries)
    assert all(entry.reason == "PRODUCT_NOT_LISTED_NO_ROWS" for entry in entries)
    assert all(entry.selection_eligible is False and not entry.permitted_uses for entry in entries)
