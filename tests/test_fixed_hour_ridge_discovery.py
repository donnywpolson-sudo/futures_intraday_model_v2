import json
from pathlib import Path

import pytest

from futures_rebuild.fixed_hour_ridge_discovery import (
    CONTRACT_PATH, NANO, _synthetic_row, build_contract, build_synthetic_certificate,
    checkpoint_ns, validate_contract,
)
from futures_rebuild.fixed_hour_ridge_readiness import (
    assess_session,
    load_fixed_hour_schedule,
)


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.current


def test_fixed_hour_ridge_freeze_and_synthetic_mechanics() -> None:
    contract = build_contract(ROOT)
    assert validate_contract(ROOT, contract) == contract
    assert contract["search"]["candidate_budget"] == 9
    assert {item["holding_horizon_minutes"] for item in contract["search"]["candidates"]} == {60}
    assert contract["research_family_accounting"] == {
        "prior_conservative_individual_count": 169,
        "new_candidate_alternatives_if_evaluated": 9,
        "conservative_cumulative_individual_count_if_evaluated": 178,
        "exact_ledger_reconciliation_required_before_registration": True,
    }
    assert not any(contract["authority"].values())

    published = json.loads((ROOT / CONTRACT_PATH).read_bytes())
    assert validate_contract(ROOT, published) == published
    certificate = build_synthetic_certificate(ROOT, published)
    assert certificate["state"] == "PASS_SYNTHETIC_MECHANICS_ONLY_NO_ROW_AUTHORITY"
    assert all(certificate["checks"].values())
    assert certificate["authority"] == {
        "historical_row_read": False,
        "trial_execution": False,
    }

    schedule = load_fixed_hour_schedule(ROOT)
    assert schedule["dependency_window"] == {
        "start": "10:00:00",
        "decision": "10:00:05",
        "entry": "10:01:00",
        "exit": "11:01:00",
        "timezone": "America/Chicago",
    }
    assert schedule["session_state_counts"] == {
        "AUTHORITATIVE_CALENDAR_INELIGIBLE": 536,
        "AUTHORITATIVE_OPEN": 1289,
        "AUTHORITATIVE_SOURCE_UNOBSERVABLE": 1,
    }
    schedule_sessions = schedule["sessions"]
    assert schedule_sessions["2018-12-05"] == "AUTHORITATIVE_CALENDAR_INELIGIBLE"
    assert schedule_sessions["2020-06-30"] == "AUTHORITATIVE_SOURCE_UNOBSERVABLE"
    assert schedule_sessions["2022-01-10"] == "AUTHORITATIVE_OPEN"

    decision = checkpoint_ns("2022-01-10")
    grouping_start = decision - 12 * 60 * 60 * NANO
    grouping_end = decision + 6 * 60 * 60 * NANO
    start = decision - 21 * 60 * NANO - 5 * NANO
    rows = [
        _synthetic_row(start + index * 60 * NANO, 100 + index / 10, 100 + index)
        for index in range(21)
    ]
    entry_start = decision + 55 * NANO
    rows.extend(
        _synthetic_row(entry_start + index * 60 * NANO, 102 + index / 100, 200 + index)
        for index in range(61)
    )
    for row in rows:
        row.update({
            "market": "ES",
            "source_cadence": "1m",
            "min_price_increment_nano": int(0.25 * NANO),
            "multiplier_nano": 50 * NANO,
            "project_session_id": "ES|2022-01-10",
            "project_trade_date": "2022-01-10",
            "project_grouping_start_ns": grouping_start,
            "project_grouping_end_ns": grouping_end,
            "official_schedule_state": "REGULAR",
        })
    ordered_rows = sorted(rows, key=lambda row: row["bar_start_ns"])
    assert assess_session(
        ordered_rows,
        authoritative_schedule_state="AUTHORITATIVE_CALENDAR_INELIGIBLE",
    )["status"] == "CALENDAR_INELIGIBLE"
    assert assess_session(
        ordered_rows,
        authoritative_schedule_state="AUTHORITATIVE_SOURCE_UNOBSERVABLE",
    )["status"] == "SOURCE_UNOBSERVABLE"
    assessed = assess_session(
        ordered_rows,
        authoritative_schedule_state=schedule_sessions["2022-01-10"],
    )
    assert assessed["status"] == "READY"
    assert assessed["feature_ready"] is True
    assert assessed["mechanics_ready"] is True
