from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from futures_rebuild.alpha_ladder_execution_design import (
    ELIGIBLE,
    UNKNOWN,
    load_execution_design_contract,
    select_macro_execution_candidate,
)
from futures_rebuild.alpha_ladder_zn_6e_execution_proxy import (
    ACTIVE_MACRO_PATH,
    OPERATION,
    PLAN_PATH,
    SCHEDULE_SHA256,
    _shared_dates,
    aggregate_execution_proxy,
    execute_authorized_execution_proxy,
    prepare_execution_proxy_plan,
)
from futures_rebuild.boundary import OperationClassification, OperationReceipt, RepoBoundary
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.research_gateway_policy import PREPARATORY_REAL_HISTORY_OPERATIONS


ROOT = Path(__file__).resolve().parents[1]
CHICAGO = ZoneInfo("America/Chicago")
pytestmark = pytest.mark.current


def _contract():
    contract = deepcopy(load_execution_design_contract(ROOT))
    gates = contract["execution_proxy_gates"]["thresholds"]
    gates.update({
        "minimum_usable_sessions": 1,
        "minimum_usable_observations": 385,
        "minimum_movement_to_cost_ratio": "0.1",
    })
    core = {key: value for key, value in contract.items() if key != "contract_id"}
    contract["contract_id"] = sha256_json(core)
    return contract


def _bundle(
    market="ZN", *, missing_minute=None, duplicate=False, invalid_metadata=False,
    late=False, quality_error=False, zero_volume=False, roll=False,
    identity_transition=False,
):
    observations, missingness, rolls, quality = [], [], [], []
    base = datetime(2019, 1, 2, 8, 35, tzinfo=CHICAGO)
    tick = 15_625_000 if market == "ZN" else 50_000
    multiplier = 100_000_000_000_000 if market == "ZN" else 125_000_000_000_000
    contract = "ZNH9" if market == "ZN" else "6EH9"
    for ordinal in range(385):
        if ordinal == missing_minute:
            continue
        start = int((base + timedelta(minutes=ordinal)).astimezone(timezone.utc).timestamp() * 1_000_000_000)
        row = {
            "market": market,
            "project_trade_date": "2019-01-02",
            "project_timezone": "America/Chicago",
            "bar_start_ns": start,
            "bar_end_ns": start + 60_000_000_000,
            "available_at_ns": start + (66 if late and ordinal == 0 else 65) * 1_000_000_000,
            "decision_eligible_at_ns": start + 65_000_000_000,
            "instrument_id": 1,
            "publisher_id": 1,
            "actual_contract": contract + "X" if identity_transition and ordinal >= 100 else contract,
            "raw_symbol": contract,
            "definition_event_at_ns": start - 1,
            "definition_received_at_ns": start - 1,
            "listing_activation_ns": start - 1,
            "expiration_ns": start + 10**15,
            "currency": "USD",
            "min_price_increment_nano": tick,
            "multiplier_nano": multiplier,
            "open_nano": 100_000_000_000 + ordinal * tick,
            "volume": 0 if zero_volume else 10,
        }
        if invalid_metadata and ordinal == 0:
            row["expiration_ns"] = start
        output_ordinal = len(observations)
        observations.append(row)
        missingness.append({
            "observation_ordinal": output_ordinal,
            "market": market,
            "interval_start_ns": start,
            "interval_end_ns": start + 60_000_000_000,
            "state": "OBSERVED_VALID",
        })
        rolls.append({"observation_ordinal": output_ordinal, "roll_flag": roll and ordinal == 10})
        quality.append({
            "observation_ordinal": output_ordinal,
            "ohlc_valid": not (quality_error and ordinal == 0),
            "volume_valid": True,
            "timestamp_order_valid": True,
            "duplicate_state": "UNIQUE",
            "quality_flags": ["MULTIPLIER_PROVIDER_DEFINITION_CROSSCHECK_MATCH"],
        })
    if duplicate:
        observations.append(dict(observations[0]))
        i = len(observations) - 1
        missingness.append({**missingness[0], "observation_ordinal": i})
        rolls.append({"observation_ordinal": i, "roll_flag": False})
        quality.append({**quality[0], "observation_ordinal": i})
    return {"observations": observations, "missingness": missingness, "roll": rolls, "quality": quality}


def test_calendar_and_metadata_only_plan_are_exact():
    dates = _shared_dates()
    assert len(dates) == 1251
    assert dates[0] == "2018-01-02" and dates[-1] == "2022-12-30"
    assert "2018-12-05" not in dates
    assert sha256_json(list(dates)) == SCHEDULE_SHA256
    before_macro = (ROOT / ACTIVE_MACRO_PATH).exists()
    plan = prepare_execution_proxy_plan(ROOT)
    assert plan["state"] == "PREPARED_REQUIRES_EXACT_APPROVAL"
    assert plan["limits"] == {"market_year_partitions": 10, "files": 480, "observation_rows": 4_000_000, "bytes": 512 * 1024 * 1024}
    assert len(plan["bindings"]["files"]) == 480
    assert plan["authority"] == {
        "authorization_receipt_issued": False,
        "market_row_read": False,
        "macro_selection": False,
        "research_results": False,
        "provider_or_network": False,
        "final_252_or_forward": False,
    }
    assert (ROOT / ACTIVE_MACRO_PATH).exists() is before_macro
    assert OPERATION in PREPARATORY_REAL_HISTORY_OPERATIONS


def test_synthetic_aggregate_is_deterministic_and_selector_passes():
    contract = _contract()
    zn = aggregate_execution_proxy(contract, "ZN", ["2019-01-02"], [_bundle()])
    again = aggregate_execution_proxy(contract, "ZN", ["2019-01-02"], [_bundle()])
    assert zn == again
    assert zn["target_horizon_movement_ticks"] == "30"
    assert zn["conservative_round_trip_friction_ticks"] == {"value": "4.64", "tick_value_usd": "15.625000"}
    six_e = aggregate_execution_proxy(contract, "6E", ["2019-01-02"], [_bundle("6E")])
    result = select_macro_execution_candidate(contract, {"ZN": zn, "6E": six_e})
    assert result["candidate_results"]["ZN"]["status"] == ELIGIBLE
    assert result["candidate_results"]["6E"]["status"] == ELIGIBLE
    assert result["selected_market"] == "ZN"  # exact ratios tie; frozen order wins


@pytest.mark.parametrize("change", [
    {"missing_minute": 0},
    {"duplicate": True},
    {"invalid_metadata": True},
    {"late": True},
    {"quality_error": True},
    {"zero_volume": True},
    {"roll": True},
    {"identity_transition": True},
])
def test_synthetic_bad_or_excluded_evidence_fails_closed(change):
    contract = _contract()
    evidence = aggregate_execution_proxy(contract, "ZN", ["2019-01-02"], [_bundle(**change)])
    result = select_macro_execution_candidate(contract, {"ZN": evidence, "6E": evidence})
    assert result["selected_market"] is None
    assert result["outcome"] == UNKNOWN or all(
        item["status"] != ELIGIBLE for item in result["candidate_results"].values()
    )


def test_scope_and_authorization_fail_before_parquet(monkeypatch):
    contract = _contract()
    with pytest.raises(UnauthorizedOperation):
        aggregate_execution_proxy(contract, "ES", ["2019-01-02"], [])
    with pytest.raises(UnauthorizedOperation):
        aggregate_execution_proxy(contract, "ZN", ["2023-01-03"], [])
    receipt = OperationReceipt.issue_local(
        RepoBoundary(ROOT),
        operation=OPERATION,
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    monkeypatch.setattr(
        "futures_rebuild.alpha_ladder_zn_6e_execution_proxy._read_bound_bundles",
        lambda *_: pytest.fail("Parquet accessed before authorization"),
    )
    with pytest.raises(UnauthorizedOperation):
        execute_authorized_execution_proxy(ROOT, receipt)
    assert (ROOT / PLAN_PATH).exists()


def test_exact_external_receipt_is_single_use(boundary):
    plan_id, plan_sha = "a" * 64, "b" * 64
    approval = f"APPROVE {OPERATION} PLAN {plan_id} SHA256 {plan_sha}"
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={"bounded_test": "synthetic_only"},
        approval_command=OPERATION,
        approval_plan_id=plan_id,
        approval_plan_sha256=plan_sha,
        approval_line=approval,
    )
    scope = {
        "bounded_test": "synthetic_only",
        "approval_command": OPERATION,
        "approval_plan_id": plan_id,
        "approval_plan_sha256": plan_sha,
    }
    receipt.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    with pytest.raises(UnauthorizedOperation, match="already used"):
        receipt.consume(
            boundary,
            operation=OPERATION,
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            required_scope=scope,
        )
