import json
from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_phase8_preparation import (
    AUDIT_RELEASE_ID,
    INDEX_RELEASE_ID,
    TRIAL_ID,
    prepare_tier1_phase8,
)


ROOT = Path(__file__).parents[1]


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _synthetic_phase8_root(tmp_path: Path, *, active_profile: str) -> Path:
    root = tmp_path / "synthetic-phase8"
    _write_json(
        root / "state/trial_registry/phase6_prediction_only" / f"{TRIAL_ID}.json",
        {"state": "REGISTERED_BEFORE_OUTCOME_OPEN", "input_pairs": [{}] * 20},
    )
    _write_json(
        root / "manifests/data_releases/reference" / f"{INDEX_RELEASE_ID}.json",
        {"release_kind": "phase8_actual_contract_economics_index"},
    )
    _write_json(
        root / "manifests/data_releases/reference" / f"{AUDIT_RELEASE_ID}.json",
        {"metadata": {"status": "PASSED"}},
    )
    config_dir = root / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "contract_economics_rules.json").write_text(
        "synthetic economics rules\n", encoding="utf-8"
    )
    profile_id = "apex_eod_performance_50k"
    profile = {
        "active_profile_id": active_profile,
        "profiles": {
            profile_id: {
                "project_limits": {
                    "maximum_standard_contract_equivalents": 1,
                    "daily_stop_loss_usd": 500,
                    "maximum_total_drawdown_usd": 1500,
                    "maximum_initial_risk_usd": 250,
                    "maximum_entries_per_session": 3,
                    "external_drawdown_reserve_usd": 500,
                },
                "firm_limits": {"drawdown": {"maximum_usd": 2000}},
            }
        },
    }
    _write_json(config_dir / "prop_firm_risk_profile.json", profile)
    evaluation = {
        "markets": ["ES", "CL", "ZN", "6E"],
        "period": "2018-2022",
        "risk_profile_id": profile_id,
        "costs": {
            "assumption_status": "PROVISIONAL_APEX_TRADOVATE_ZN_FEE",
            "evaluation_result_label": "PROVISIONAL_EXECUTION_COSTS",
            "exact_apex_live_costs_verified": False,
            "execution_connection": "Tradovate",
            "official_source": "synthetic",
            "base": {
                market: {
                    "round_trip_slippage_ticks_per_contract": 2,
                    **(
                        {
                            "fee_status": "PROVISIONAL_CONSERVATIVE_PRE_ACCOUNT_ASSUMPTION",
                            "all_in_fee_per_side_usd": "2.50",
                        }
                        if market == "ZN"
                        else {}
                    ),
                }
                for market in ("ES", "CL", "ZN", "6E")
            },
        },
        "delay": {},
        "position_sizing": {
            "method": "atr_bracket_fixed_one_contract",
            "risk_per_new_position_usd": 250,
            "maximum_open_risk_usd": 250,
            "maximum_standard_contract_equivalents": 1,
            "maximum_entries_per_session": 3,
            "admission_cost_scenario": "stress",
        },
        "margin": {
            "treatment": "NOT_USED_FOR_FIXED_RISK_RESEARCH_EVALUATION",
            "live_buying_power_validation": False,
            "source": "outside_this_historical_research_lane",
        },
        "concentration_limits": {
            "daily_stop_loss_usd": 500,
            "maximum_total_drawdown_usd": 1500,
            "external_firm_drawdown_usd": 2000,
            "external_drawdown_reserve_usd": 500,
        },
        "bracket_exit_policy": {
            "atr_method": "wilder",
            "atr_lookback_completed_bars": 20,
            "atr_multiple": "1.5",
            "profit_target_net_r_multiple": "2",
            "maximum_hold_minutes": 60,
            "session_roll": "17:00 America/Chicago",
            "session_end_fill": "last_verified_eligible_bar_before_roll",
            "intrabar_stop_target_collision": "stop_first",
            "entry_blackout_minutes_before_session_roll": 60,
            "missing_or_roll_behavior": "abstain_or_flatten_never_invent_price",
        },
        "baselines": [
            "flat_no_trade",
            "fold_local_unconditional_return_by_market_session",
            "previous_bar_sign_momentum",
            "previous_bar_sign_reversal",
            "risk_matched_always_long_intraday",
            "equal_risk_version_of_candidate_signal",
        ],
        "pass_fail_metrics": {
            "required_cost_scenario_for_promotion": "stress",
            "must_beat_after_costs": [
                "flat_no_trade",
                "fold_local_unconditional_return_by_market_session",
                "previous_bar_sign_momentum",
                "previous_bar_sign_reversal",
                "risk_matched_always_long_intraday",
            ],
            "equivalence_checks": ["equal_risk_version_of_candidate_signal"],
        },
    }
    _write_json(config_dir / "tier1_phase8_evaluation.json", evaluation)
    return root


def test_preparation_pins_the_active_apex_risk_profile(
    local_evidence_root: Path,
) -> None:
    prepared = prepare_tier1_phase8(root=local_evidence_root)

    assert prepared.risk_profile_id == "apex_eod_performance_50k"
    assert len(prepared.risk_profile_hash) == 64
    assert len(prepared.evaluation_config_hash) == 64
    assert prepared.evaluation_ready
    assert prepared.declaration()["permitted_result_label"] == "PROVISIONAL_EXECUTION_COSTS"


def test_provisional_schedule_cannot_be_labeled_exact_apex_live_costs(
    tmp_path: Path,
) -> None:
    prepared = prepare_tier1_phase8(
        root=_synthetic_phase8_root(
            tmp_path, active_profile="apex_eod_performance_50k"
        )
    )

    assert prepared.declaration()["exact_apex_live_costs_verified"] is False


def test_preparation_rejects_an_unknown_active_risk_profile(tmp_path) -> None:
    with pytest.raises(IntegrityError, match="active prop-firm risk profile"):
        prepare_tier1_phase8(
            root=_synthetic_phase8_root(tmp_path, active_profile="missing")
        )
