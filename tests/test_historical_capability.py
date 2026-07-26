from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.historical_capability import (
    DerivedGatePolicy,
    _historical_authorization_scope,
    derive_gate_evidence_from_returns,
    load_historical_capability_config,
    verify_production_capability_closure,
)
from futures_rebuild.research.controls import NegativeControlOutcome
from futures_rebuild.research.contracts import ResearchContractError


REPO = Path(__file__).resolve().parents[1]


def _inputs():
    rng = np.random.Generator(np.random.PCG64(812))
    sessions = 80
    strategies = 10
    raw = rng.normal(0.0, 0.004, size=(sessions, strategies)).astype(np.float64)
    raw[:, 3] += 0.004
    costs = {
        "zero": raw,
        "base": raw - 0.0002,
        "stress": raw - 0.0004,
        "extreme": raw - 0.0008,
    }
    selected = costs["stress"][:, 3]
    selected_sharpe = float(np.mean(selected) / np.std(selected, ddof=1))
    trial_sharpes = np.linspace(-0.5, selected_sharpe - 0.01, 11, dtype=np.float64)
    trial_sharpes = np.concatenate((trial_sharpes, np.asarray([selected_sharpe])))
    return costs, trial_sharpes


def _policy() -> DerivedGatePolicy:
    return DerivedGatePolicy(
        hac_lag=1,
        mean_block_length=5.0,
        bootstrap_resamples=31,
        seed=77,
        alpha=0.05,
        confidence_level=0.95,
        minimum_effect=0.0001,
        dsr_probability_minimum=0.5,
        pbo_conservative_maximum=1.0 - 1e-12,
        cscv_blocks=8,
        target_power=0.8,
    )


def test_capability_closure_is_exact_and_non_authorizing() -> None:
    config = load_historical_capability_config(REPO)
    closure = verify_production_capability_closure(REPO)
    assert config["status"] == "IMPLEMENTED_EXECUTION_DISABLED"
    assert closure["status"] == "PRODUCTION_SHAPED_EXECUTION_DISABLED"
    assert closure["execution_authorized"] is False
    assert closure["alpha_evidence"] is False
    assert closure["candidate_eligible"] is False
    assert any(
        item["path"] == "src/futures_rebuild/source_symbology.py"
        for item in closure["component_files"]
    )


def test_real_history_scope_binds_blueprint_and_query_manifest() -> None:
    scope = _historical_authorization_scope(
        foundation_release_id="a" * 64,
        foundation_research_blueprint_id="b" * 64,
        query_manifest_id="c" * 64,
        trial_charter_id="d" * 64,
    )
    assert scope == {
        "foundation_release_id": "a" * 64,
        "foundation_research_blueprint_id": "b" * 64,
        "query_manifest_id": "c" * 64,
        "trial_charter_id": "d" * 64,
    }


def test_gate_evidence_is_derived_from_return_rows_and_never_claims_alpha() -> None:
    costs, trial_sharpes = _inputs()
    evidence = derive_gate_evidence_from_returns(
        scenario_strategy_returns=costs,
        strategy_ids=tuple(f"S{index}" for index in range(10)),
        selected_strategy_index=3,
        trial_sharpes=trial_sharpes,
        selected_trial_index=len(trial_sharpes) - 1,
        training_differentials=np.tile(
            np.asarray([-0.01, 0.01], dtype=np.float64), 80
        ),
        negative_controls=(
            NegativeControlOutcome("label-shift", True, False),
            NegativeControlOutcome("future-canary", True, False),
        ),
        policy=_policy(),
    ).as_dict()
    assert evidence["status"] == "DERIVED_FROM_ALIGNED_RETURNS_NO_ALPHA_AUTHORITY"
    assert evidence["alpha_evidence"] is False
    assert evidence["candidate_eligible"] is False
    assert set(evidence["derived_gates"]) == {
        "confidence_lower_bound",
        "cost_monotonicity",
        "deflated_sharpe",
        "mean_after_stress_costs",
        "negative_controls",
        "pbo",
        "power",
        "romano_wolf",
    }


def test_cost_or_real_history_authority_cannot_be_faked() -> None:
    costs, trial_sharpes = _inputs()
    costs["stress"] = costs["base"] + 0.001
    kwargs = dict(
        scenario_strategy_returns=costs,
        strategy_ids=tuple(f"S{index}" for index in range(10)),
        selected_strategy_index=3,
        trial_sharpes=trial_sharpes,
        selected_trial_index=len(trial_sharpes) - 1,
        training_differentials=np.tile(
            np.asarray([-0.01, 0.01], dtype=np.float64), 80
        ),
        negative_controls=(NegativeControlOutcome("control", True, False),),
        policy=_policy(),
    )
    with pytest.raises(ResearchContractError, match="monotonically"):
        derive_gate_evidence_from_returns(**kwargs)
    costs, trial_sharpes = _inputs()
    kwargs["scenario_strategy_returns"] = costs
    kwargs["trial_sharpes"] = trial_sharpes
    with pytest.raises(UnauthorizedOperation, match="consumed authority"):
        derive_gate_evidence_from_returns(
            **kwargs, source_kind="EXTERNALLY_AUTHORIZED_REAL_HISTORY"
        )
