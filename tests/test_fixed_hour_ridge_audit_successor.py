from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.fixed_hour_ridge_audit_successor import (
    INNER_MINIMUM_TRADES,
    execution_audit_coordinates,
    purged_embargoed_inner_folds,
    purged_embargoed_outer_pilot,
    select_activity_eligible_candidate,
    session_ledger_record,
    strict_feature_values,
    strict_outcome_path,
)
from futures_rebuild.fixed_hour_ridge_discovery import NANO, _synthetic_row, checkpoint_ns


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.current


def test_audit_successor_fails_closed_and_preserves_session_zero_accounting() -> None:
    decision = checkpoint_ns("2022-01-10")
    feature_start = decision - 20 * 60 * NANO - 5 * NANO
    feature_rows = [
        _synthetic_row(feature_start + index * 60 * NANO, 100 + index / 10, 100 + index)
        for index in range(20)
    ]
    features, feature_audit = strict_feature_values(feature_rows, decision)
    assert len(features) == 8 and feature_audit["bar_count"] == 20

    gapped = [dict(row) for row in feature_rows]
    gapped[10]["bar_start_ns"] += 60 * NANO
    gapped[10]["bar_end_ns"] += 60 * NANO
    with pytest.raises(IntegrityError, match="consecutive 20 minutes"):
        strict_feature_values(gapped, decision)
    mixed = [dict(row) for row in feature_rows]
    mixed[0]["actual_contract"] = "ESM2"
    with pytest.raises(IntegrityError, match="actual-contract"):
        strict_feature_values(mixed, decision)

    entry = (decision // (60 * NANO) + 1) * 60 * NANO
    complete = [
        _synthetic_row(entry + index * 60 * NANO, 102 + index / 100, 200 + index)
        for index in range(61)
    ]
    outcome = strict_outcome_path(complete, decision, expected_contract="ESH2")
    assert outcome["score_status"] == "SCORABLE"
    assert outcome["coverage"]["observed_path_bars"] == 60
    sparse = strict_outcome_path([complete[0], complete[-1]], decision, expected_contract="ESH2")
    assert sparse["score_status"] == "INDETERMINATE_SPARSE_OHLCV_PATH"

    ids = [f"D{index:03d}" for index in range(504)]
    ready = {
        session_id: {
            "label_start_ns": index * 24 * 60 * 60 * NANO + 10 * 60 * 60 * NANO,
            "label_end_ns": index * 24 * 60 * 60 * NANO + 11 * 60 * 60 * NANO,
        }
        for index, session_id in enumerate(ids)
    }
    folds = purged_embargoed_inner_folds(ready, ids)
    assert len(folds) == 4
    assert all(len(fold["embargo_session_ids"]) == 1 for fold in folds)
    assert all(
        not set(fold["training_session_ids"]) & set(fold["validation_session_ids"])
        for fold in folds
    )
    outer = purged_embargoed_outer_pilot(ready | {
        f"D{index:03d}": {
            "label_start_ns": index * 24 * 60 * 60 * NANO + 10 * 60 * 60 * NANO,
            "label_end_ns": index * 24 * 60 * 60 * NANO + 11 * 60 * 60 * NANO,
        }
        for index in range(504, 568)
    }, [f"D{index:03d}" for index in range(568)])
    assert len(outer["training_session_ids"]) == 504
    assert len(outer["embargo_session_ids"]) == 1
    assert len(outer["evaluation_session_ids"]) == 63

    order = ("inactive", "active")
    selected = select_activity_eligible_candidate(
        order,
        scores={"inactive": (0.0, 0.0, 0.0, 0.0), "active": (0.1, 0.1, 0.1, 0.1)},
        trade_counts={
            "inactive": (0, 0, 0, 0),
            "active": (INNER_MINIMUM_TRADES,) * 4,
        },
    )
    assert selected == "active"
    with pytest.raises(IntegrityError, match="no candidate"):
        select_activity_eligible_candidate(
            order,
            scores={"inactive": (0.0,) * 4, "active": (-1.0,) * 4},
            trade_counts={"inactive": (0,) * 4, "active": (INNER_MINIMUM_TRADES,) * 4},
        )

    execution_audit = execution_audit_coordinates(feature_audit, outcome)
    auditable_row = {
        "session_id": "ES|2022-01-10",
        "trade_date": "2022-01-10",
        "execution_audit": execution_audit,
        "gross_pnl_usd": {"long": 100.0, "short": -100.0},
        "net_pnl_usd": {
            "base": {"long": 65.0, "short": -135.0},
            "stress": {"long": 40.0, "short": -160.0},
            "extreme": {"long": -10.0, "short": -210.0},
        },
        "net_mfe_mae": {"long": 2.0, "short": 0.1},
        "round_trip_cost_usd": {"base": 35.0, "stress": 60.0, "extreme": 110.0},
    }
    ledger = session_ledger_record(
        auditable_row,
        intent=0,
        predicted_long_r=-0.1,
        predicted_short_r=-0.2,
    )
    assert ledger["disposition"] == "BEST_PREDICTED_STRESS_NET_R_NOT_POSITIVE"
    assert ledger["net_pnl_usd"] == {"base": 0.0, "stress": 0.0, "extreme": 0.0}
    trade = session_ledger_record(
        auditable_row,
        intent=1,
        predicted_long_r=0.3,
        predicted_short_r=-0.4,
    )
    assert trade["disposition"] == "TRADE"
    assert trade["gross_pnl_usd"] == 100.0
    assert trade["net_pnl_usd"]["stress"] == 40.0
