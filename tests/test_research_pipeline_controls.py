from __future__ import annotations

import numpy as np
import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.historical_builder import build_synthetic_research_run
from futures_rebuild.historical_engine_contracts import (
    HistoricalResearchDataset,
    LinearCandidate,
    synthetic_research_fixture,
)
from futures_rebuild.historical_splitter import split_synthetic_research_run
from futures_rebuild.l1_execution_cost_validation import (
    causal_l1_round_trip_cost,
    validate_causal_l1_cost,
)
from futures_rebuild.research import (
    ResearchContractError,
    SessionWindow,
    TemporalSamples,
    make_synthetic_permit,
    nested_chronological_splits,
)
from futures_rebuild.research.feature_foundation import (
    build_feature_foundation_run_audit,
    optional_feature_jaccard,
    validate_feature_foundation_contract,
)
from futures_rebuild.research.power import (
    build_final_252_power_plan,
    validate_final_252_power_plan,
)
from futures_rebuild.research.scope_inference import joint_scope_test
from futures_rebuild.schemas import OutcomeStatus


pytestmark = pytest.mark.current


def _identified(core: dict[str, object]) -> dict[str, object]:
    return {**core, "contract_id": sha256_json(core)}


def _feature_contract() -> dict[str, object]:
    return _identified({
        "schema_version": "feature_foundation_contract/1.1.0",
        "state": "PREPARED_NOT_ACTIVE",
        "authority": {
            "real_history": False,
            "holdout": False,
            "provider": False,
            "activation": False,
            "trading": False,
        },
        "outcome_foundation_contract_id": "a" * 64,
        "stage_order": [
            "BASELINE_FEATURES",
            "FEATURE_DISCOVERY_EXPANSION",
            "FEATURE_SELECTION",
            "FREEZE_FEATURE_PROCEDURE",
        ],
        "core_library": {"PRICE_PATH": ["signal"]},
        "expanded_feature_names": ["noise", "signal_noise_interaction"],
        "feature_definitions": {
            "signal": "CAUSAL_SIGNAL_T",
            "noise": "CAUSAL_CONTROL_T",
            "signal_noise_interaction": "SIGNAL_T_TIMES_CONTROL_T",
        },
        "expansion_grammar": {
            "operators": ["PRODUCT"],
            "lags": [],
            "windows": [],
            "interactions": True,
            "maximum_expanded_features": 2,
        },
        "feature_discovery": {
            "fit_scope": "INNER_TRAIN_ONLY",
            "attempt_budget": 2,
            "ranking": "RMS_ABS_TRAIN_ONLY_CORRELATION_ACROSS_THREE_TARGETS",
            "output": "ORDERED_EXPANSION_CANDIDATES",
        },
        "candidate_budget": 2,
        "candidates": [
            {
                "candidate_id": "signal-ridge",
                "feature_names": ["signal"],
                "hyperparameters": {"ridge": 0.1},
                "hyperparameters_sha256": sha256_json({"ridge": 0.1}),
            },
            {
                "candidate_id": "expanded-ridge",
                "feature_names": ["signal", "noise", "signal_noise_interaction"],
                "hyperparameters": {"ridge": 1.0},
                "hyperparameters_sha256": sha256_json({"ridge": 1.0}),
            },
        ],
        "selector": {
            "fit_scope": "INNER_TRAIN_ONLY",
            "score_direction": "MINIMIZE",
            "tie_break": "CANDIDATE_ID_ASCENDING",
        },
        "stability": {
            "metric": "MINIMUM_SIGN_AGREEMENT",
            "threshold": 0.75,
            "feature_names": ["signal"],
            "failure_action": "FAIL_PROMOTION",
        },
        "outer_results_may_change_features": False,
    })


def _historical_results():
    n = 170
    time = np.arange(n, dtype=np.float64)
    signal = np.sin(time * 0.173) + 0.2 * np.cos(time * 0.037)
    noise = np.cos(time * 0.311)
    features = np.column_stack((signal, noise, signal * noise)).astype(np.float64)
    labels = (0.8 * signal + 0.02 * np.sin(time * 0.91)).astype(np.float64)
    resolved = np.ones(n, dtype=np.bool_)
    fixture = synthetic_research_fixture(features, labels, resolved)
    temporal = TemporalSamples(
        np.arange(n, dtype=np.int64),
        np.arange(n, dtype=np.int64) + 1,
        np.arange(n, dtype=np.int64) + 2,
        np.arange(n, dtype=np.int64) + 2,
    )
    dataset = HistoricalResearchDataset(
        sample_ids=tuple(f"sample-{index:04d}" for index in range(n)),
        feature_names=("signal", "noise", "signal_noise_interaction"),
        features=features,
        labels=labels,
        outcome_statuses=tuple(OutcomeStatus.MATURED.value for _ in range(n)),
        temporal=temporal,
        permit=make_synthetic_permit(fixture, generator_id="feature-foundation-test", seed=9),
    )
    folds = nested_chronological_splits(
        temporal,
        (SessionWindow(100, 120), SessionWindow(130, 150)),
        (
            (SessionWindow(50, 60), SessionWindow(75, 85)),
            (SessionWindow(70, 80), SessionWindow(105, 115)),
        ),
        session_embargo=1,
        minimum_fit_samples=20,
        minimum_audit_samples=8,
    )
    split = split_synthetic_research_run(dataset, folds)
    candidates = (
        LinearCandidate("signal-ridge", ("signal",), 0.1),
        LinearCandidate(
            "expanded-ridge", ("signal", "noise", "signal_noise_interaction"), 1.0
        ),
    )
    return build_synthetic_research_run(split.builder_packets, candidates)


def test_feature_contract_and_fold_audits_bind_the_complete_search() -> None:
    contract = _feature_contract()
    assert validate_feature_foundation_contract(contract) == contract
    audit = build_feature_foundation_run_audit(contract, _historical_results())
    assert audit["state"] == "PASS"
    assert audit["stability"]["metric"] == "MINIMUM_SIGN_AGREEMENT"
    assert audit["stability"]["score"] >= 0.75
    for fold in audit["fold_manifests"]:
        assert set(fold["selection_scores"]) == {"signal-ridge", "expanded-ridge"}
        assert all(
            entry["fit_sample_ids"] and entry["audit_sample_ids"]
            for entries in fold["inner_fit_audits"].values()
            for entry in entries
        )
        assert fold["outer_fit_audit"]["fit_sample_ids"]

    changed = dict(contract)
    changed["candidate_budget"] = 3
    with pytest.raises(ResearchContractError, match="contract_id|budget"):
        validate_feature_foundation_contract(changed)


def test_optional_jaccard_excludes_baselines_and_handles_baseline_only() -> None:
    selected = [
        ("base_a", "base_b", "extra_a", "extra_b"),
        ("base_a", "base_b", "extra_c", "extra_d"),
    ]
    assert optional_feature_jaccard(selected, ("extra_a", "extra_b", "extra_c", "extra_d")) == 0.0
    assert optional_feature_jaccard([("base_a",), ("base_a",)], ("extra_a",)) is None


def test_joint_scope_test_uses_one_complete_common_session_matrix() -> None:
    rng = np.random.default_rng(18)
    shared = rng.normal(scale=0.08, size=120)
    values = np.column_stack((0.5 + shared, 0.45 + shared, 0.4 + shared))
    sessions = tuple(f"2024-{index:03d}" for index in range(120))
    result = joint_scope_test(
        values,
        session_ids=sessions,
        market_ids=("ES", "CL", "GC"),
        required_positive_markets=("ES", "CL", "GC"),
        hac_lag=2,
        mean_block_length=8.0,
        n_resamples=99,
        seed=7,
        alpha=0.05,
        minimum_effect=0.1,
    )
    assert result["passed"] is True
    assert result["same_date_dependence_preserved"] is True
    assert result["market_count_is_independence_count"] is False
    with pytest.raises(ResearchContractError, match="chronological"):
        joint_scope_test(
            values,
            session_ids=tuple(reversed(sessions)),
            market_ids=("ES", "CL", "GC"),
            required_positive_markets=("ES", "CL", "GC"),
            hac_lag=2,
            mean_block_length=8.0,
            n_resamples=20,
            seed=7,
            alpha=0.05,
            minimum_effect=0.1,
        )


def test_final_252_power_plan_is_bound_and_underpower_fails_closed() -> None:
    values = 0.02 * np.sin(np.arange(500, dtype=np.float64))
    bindings = {
        "mechanism_sha256": "a" * 64,
        "config_sha256": "b" * 64,
        "session_manifest_id": "c" * 64,
        "session_manifest_sha256": "d" * 64,
        "ordered_session_sha256": "e" * 64,
    }
    plan = build_final_252_power_plan(
        values,
        estimand="MEAN_STRESS_NET_PNL_USD_PER_SESSION",
        **bindings,
        hac_lag=4,
        alpha=0.05,
        target_power=0.8,
        alternative="greater",
        economic_mean_hurdle=0.01,
    )
    assert plan["planned_evaluation_observations"] == 252
    assert validate_final_252_power_plan(plan, **bindings) == plan
    with pytest.raises(ResearchContractError, match="binding"):
        validate_final_252_power_plan(
            plan, **{**bindings, "mechanism_sha256": "f" * 64}
        )
    underpowered = build_final_252_power_plan(
        values,
        estimand="MEAN_STRESS_NET_PNL_USD_PER_SESSION",
        **bindings,
        hac_lag=4,
        alpha=0.05,
        target_power=0.8,
        alternative="greater",
        economic_mean_hurdle=0.00001,
    )
    assert underpowered["status"] == "INCONCLUSIVE_DATA_OR_POWER"
    with pytest.raises(ResearchContractError, match="INCONCLUSIVE_DATA_OR_POWER"):
        validate_final_252_power_plan(underpowered, **bindings)


def test_l1_cost_is_causal_and_does_not_claim_impact() -> None:
    result = causal_l1_round_trip_cost(
        source_certification_id="a" * 64,
        entry_decision_ns=1_000,
        entry_quote_available_ns=990,
        entry_bid=100.00,
        entry_ask=100.25,
        exit_decision_ns=2_000,
        exit_quote_available_ns=1_980,
        exit_bid=101.00,
        exit_ask=101.50,
        tick_size=0.25,
        tick_value_usd=12.50,
        round_trip_fee_usd=5.00,
        adverse_selection_ticks=1.0,
        maximum_quote_age_ns=100,
    )
    assert validate_causal_l1_cost(result) == result
    assert result["estimated_round_trip_cost_usd"] == 36.25
    assert result["market_impact_status"] == "NOT_IDENTIFIED_FROM_L1_BBO"
    tampered = dict(result)
    tampered["estimated_round_trip_cost_usd"] = 1.0
    with pytest.raises(IntegrityError, match="invalid"):
        validate_causal_l1_cost(tampered)
    with pytest.raises(IntegrityError, match="future-known or stale"):
        causal_l1_round_trip_cost(
            source_certification_id="a" * 64,
            entry_decision_ns=1_000,
            entry_quote_available_ns=1_001,
            entry_bid=100.00,
            entry_ask=100.25,
            exit_decision_ns=2_000,
            exit_quote_available_ns=1_980,
            exit_bid=101.00,
            exit_ask=101.50,
            tick_size=0.25,
            tick_value_usd=12.50,
            round_trip_fee_usd=5.00,
            adverse_selection_ticks=1.0,
            maximum_quote_age_ns=100,
        )
