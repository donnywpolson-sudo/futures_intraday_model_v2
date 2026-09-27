from pathlib import Path

from futures_rebuild.es_micro_exhaustion_preparation import (
    build_program,
    signal_direction,
)
from futures_rebuild.es_micro_exhaustion_execution import _screen_summary


ROOT = Path(__file__).resolve().parents[1]


def test_micro_exhaustion_is_one_causal_candidate_with_no_authority() -> None:
    assert signal_direction(2.1, -0.001, 1.1) == -1
    assert signal_direction(-2.1, 0.001, 1.1) == 1
    assert signal_direction(2.1, 0.001, 1.1) == 0
    assert signal_direction(1.9, -0.001, 1.1) == 0
    assert signal_direction(2.1, -0.001, 0.9) == 0

    program = build_program(ROOT)
    assert program["candidate_count"] == 1
    assert program["timing"]["entry_offset_from_trigger_bars"] == 2
    assert program["timing"]["feature_label_overlap"] is False
    assert program["scope"]["full_session_excluded_if_actual_contract_changes"] is True
    assert program["research_family_accounting"]["conservative_cumulative_individual_count_if_evaluated"] == 169
    assert program["expected_volume"] == {
        "partitions": 5,
        "catalog_rows": 1_755_614,
        "physical_bytes": 147_844_292,
    }
    assert not any(program["authority"].values())

    trades = [
        {
            "session_date": f"{2018 + index % 3}-06-01",
            "gross_pnl_usd": 30.0,
            "net_pnl_usd": {"2.50": 27.5, "5.00": 25.0, "10.00": 20.0},
            "net_mfe_mae_at_5_usd": 2.0,
        }
        for index in range(60)
    ]
    screen = _screen_summary(trades, {"evaluated_session_count": 60})
    assert screen["passed"] is True
    assert screen["decision"] == "PREPARE_CONDITIONAL_WFA_ONLY_NO_AUTOMATIC_AUTHORITY"
