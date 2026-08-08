from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from futures_rebuild.tier1_bracket_post_audit import CausalBar
from futures_rebuild.tier1_bracket_v4 import ExpectedCheckpoint, FEATURE_NAMES, MarketSpec
from futures_rebuild.tier1_bracket_v5 import (
    CHECKPOINTS, MARKETS, CrossfitEvidenceBundleV5, CoverageEvidence,
    MaterializedRowV5, NS_PER_MINUTE, OpportunityRecordV5, load_v5_contract,
)
from futures_rebuild.tier1_bracket_v10_decision_validity import attach_causal_outcomes_v10
from futures_rebuild.tier1_bracket_v10_pipeline import (
    CrossfitEvidenceV10, build_nested_crossfit_evidence_v10, derive_v10_decision,
)


ROOT = Path(__file__).resolve().parents[1]
D = Decimal


def _rows_for_crossfit() -> tuple[MaterializedRowV5, ...]:
    rows: list[MaterializedRowV5] = []
    origin = date(2018, 1, 2)
    base_ns = 1_514_908_800_000_000_000
    for session_index in range(60):
        session = (origin + timedelta(days=session_index)).isoformat()
        for checkpoint_index, checkpoint in enumerate(CHECKPOINTS):
            decision = base_ns + session_index * 86_400_000_000_000 + checkpoint_index * 7_200_000_000_000
            for market in MARKETS:
                opportunity_id = f"{market}/{session}/{checkpoint}"
                expected = ExpectedCheckpoint(
                    opportunity_id, market, 2018, session, checkpoint, decision,
                )
                ledger = OpportunityRecordV5(
                    opportunity_id, market, session, checkpoint, decision,
                    "TRAINING_OR_PREDICTION_INELIGIBLE", False,
                    decision - 2 * NS_PER_MINUTE, decision - NS_PER_MINUTE,
                    outcome_coverage="MISSING",
                )
                path = tuple(
                    CausalBar(
                        decision + offset * NS_PER_MINUTE,
                        decision + (offset + 1) * NS_PER_MINUTE,
                        decision + (offset + 1) * NS_PER_MINUTE + 5_000_000_000,
                        D("100"), D("100.25"), D("99.75"), D("100"), True,
                    )
                    for offset in range(1, 62)
                )
                rows.append(MaterializedRowV5(
                    expected, ledger, {name: 0.0 for name in FEATURE_NAMES},
                    D("0.25"), "a" * 64, None, path,
                    MarketSpec(D("0.25"), D("12.5"), D("50")), True,
                ))
    return tuple(rows)


def test_nested_crossfit_uses_all_feature_valid_decisions_and_complete_policy_paths() -> None:
    upgraded, resolutions = attach_causal_outcomes_v10(
        rows=_rows_for_crossfit(), contract=load_v5_contract(root=ROOT),
    )
    evidence = build_nested_crossfit_evidence_v10(
        rows=upgraded, resolutions=resolutions,
    )
    assert evidence.decision_availability["status"] == "PASS"
    assert evidence.decision_availability["overall_decision_feature_eligibility_rate"] == 1.0
    assert evidence.evaluation_completeness["status"] == "PASS"
    assert evidence.evaluation_completeness["selected_intents_missing_causal_outcome"] == 0
    assert len(evidence.base.sessions) == len(evidence.base.fold_ids) == 30


def test_incomplete_policy_path_blocks_performance_inference_before_it_runs() -> None:
    by_market_year = {
        f"{market}/{year}": 100
        for market in MARKETS for year in (2020, 2021, 2022)
    }
    coverage = CoverageEvidence(
        1200, 1200, 1200, 1200, 1200,
        by_market_year, by_market_year,
    )
    decision = derive_v10_decision(
        evaluation={}, evaluation_sessions=(), coverage=coverage,
        baseline_coverage={"status": "PASS", "passed": True},
        crossfit=CrossfitEvidenceV10(
            CrossfitEvidenceBundleV5((), (), {}, {}),
            {"status": "PASS", "passed": True}, {},
            {"status": "PASS", "passed": True},
        ),
        evaluation_completeness={
            "status": "INCONCLUSIVE_DATA_OR_COVERAGE", "passed": False,
        },
        seed=1,
    )
    assert decision["classification"] == "INCONCLUSIVE_DATA_OR_COVERAGE"
    assert decision["inference_executed"] is False
