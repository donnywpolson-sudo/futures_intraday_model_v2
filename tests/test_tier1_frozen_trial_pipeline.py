from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from futures_rebuild.tier1_bracket_post_audit import CausalBar
from futures_rebuild.tier1_bracket_v4 import ExpectedCheckpoint, MarketSpec
from futures_rebuild.tier1_bracket_v5 import (
    CHECKPOINTS, MARKETS, CensusCheckpoint, NS_PER_MINUTE, V5SourceRecord,
    load_v5_contract,
)
from futures_rebuild.tier1_bracket_v12_pipeline import build_nested_crossfit_evidence_v12
from futures_rebuild.tier1_frozen_trial_pipeline import materialize_reported_bar_rows
from futures_rebuild.tier1_frozen_trial_protocol import (
    load_frozen_synthetic_verification,
    load_frozen_trial_protocol,
)
from tests.test_tier1_frozen_source_adequacy import _checkpoint
from tests.test_tier1_frozen_source_semantics import _rows


ROOT = Path(__file__).resolve().parents[1]


def _materialize(offsets: list[int]):
    return materialize_reported_bar_rows(
        source_rows=_rows(offsets), census=(_checkpoint(),),
        market_specs={"ES": _rows(offsets)[0].market_spec},
        contract=load_v5_contract(root=ROOT),
        prediction_scope_sessions=("2020-01-02",),
    )


def test_sparse_reported_bars_materialize_a_causal_prediction_and_outcomes() -> None:
    offsets = [
        *[offset for offset in range(-64, -1) if offset not in {-30, -20}],
        1, 2, 5, 20, 40, 61,
    ]
    rows, resolutions = _materialize(offsets)
    assert len(rows) == 1
    row = rows[0]
    assert row.features is not None
    assert row.ledger.prediction_produced is True
    assert row.ledger.feature_available_at_ns <= row.expected.decision_at_ns
    assert row.outcomes is not None
    assert row.ledger.outcome_coverage in {
        "COMPLETE", "STRESS_COMPLETE_PARTIAL_DIAGNOSTICS",
    }
    assert "stress" in row.outcomes
    assert row.expected.opportunity_id in resolutions
    observed = {bar.event_at_ns for bar in row.execution_path}
    assert len(observed) == 6


def test_future_path_never_changes_feature_or_prediction_eligibility() -> None:
    feature_offsets = [
        offset for offset in range(-64, -1) if offset not in {-30, -20}
    ]
    complete, _ = _materialize([*feature_offsets, 1, 2, 20, 40, 61])
    unavailable, resolutions = _materialize(feature_offsets)
    assert complete[0].features == unavailable[0].features
    assert complete[0].source_row_sha256 == unavailable[0].source_row_sha256
    assert complete[0].ledger.prediction_produced is True
    assert unavailable[0].ledger.prediction_produced is True
    assert unavailable[0].ledger.outcome_coverage == "MISSING"
    assert unavailable[0].outcomes is None
    assert resolutions == {}


def test_missing_exact_entry_is_not_replaced_by_a_later_bar() -> None:
    feature_offsets = [
        offset for offset in range(-64, -1) if offset not in {-30, -20}
    ]
    rows, resolutions = _materialize([*feature_offsets, 2, 20, 40, 61])
    assert rows[0].ledger.prediction_produced is True
    assert rows[0].ledger.outcome_coverage == "MISSING"
    assert rows[0].execution_path == ()
    assert resolutions == {}


def test_missing_feature_history_is_retained_as_causal_abstention() -> None:
    rows, resolutions = _materialize([-3, -2, 1, 61])
    assert len(rows) == 1
    assert rows[0].ledger.terminal_disposition == "INSUFFICIENT_CAUSAL_HISTORY"
    assert rows[0].ledger.prediction_produced is False
    assert resolutions == {}


def test_reported_bar_source_adapter_completes_the_synthetic_crossfit_pipeline() -> None:
    contract = load_v5_contract(root=ROOT)
    spec = MarketSpec(Decimal("0.25"), Decimal("12.50"), Decimal("50"))
    origin = date(2018, 1, 2)
    base_ns = 1_514_908_800_000_000_000
    all_rows = []
    all_resolutions = {}
    ordinal = 0
    for session_index in range(60):
        session = (origin + timedelta(days=session_index)).isoformat()
        decisions = [
            base_ns + session_index * 86_400_000_000_000
            + checkpoint_index * 120 * NS_PER_MINUTE
            for checkpoint_index in range(3)
        ]
        for market_index, market in enumerate(MARKETS):
            checkpoints = tuple(
                CensusCheckpoint(
                    ExpectedCheckpoint(
                        f"{market}/{session}/{checkpoint}", market, 2018,
                        session, checkpoint, decisions[index],
                    ),
                    True, "c" * 64,
                )
                for index, checkpoint in enumerate(CHECKPOINTS)
            )
            protected = {
                decision + offset * NS_PER_MINUTE
                for decision in decisions for offset in (1, 61)
            }
            start = decisions[0] - 64 * NS_PER_MINUTE
            stop = decisions[-1] + 61 * NS_PER_MINUTE
            source = []
            event = start
            while event <= stop:
                minute_index = (event - start) // NS_PER_MINUTE
                if minute_index % 37 or event in protected:
                    ordinal += 1
                    center = Decimal("100") + Decimal(
                        (minute_index + session_index + market_index) % 17
                    ) / Decimal("100")
                    bar = CausalBar(
                        event, event + NS_PER_MINUTE,
                        event + NS_PER_MINUTE + 5_000_000_000,
                        center, center + Decimal("0.25"),
                        center - Decimal("0.25"), center + Decimal("0.01"), True,
                    )
                    source.append(V5SourceRecord(
                        market, session, "ELIGIBLE", bar,
                        float(100 + ordinal % 29), "b" * 64,
                        f"{ordinal:064x}", spec,
                    ))
                event += NS_PER_MINUTE
            rows, resolutions = materialize_reported_bar_rows(
                source_rows=tuple(source), census=checkpoints,
                market_specs={market: spec}, contract=contract,
                prediction_scope_sessions=(),
            )
            all_rows.extend(rows)
            all_resolutions.update(resolutions)
    evidence = build_nested_crossfit_evidence_v12(
        rows=tuple(all_rows), resolutions=all_resolutions,
    )
    assert evidence.controls.decision_availability["status"] == "PASS"
    assert evidence.controls.evaluation_completeness["status"] == "PASS"
    assert evidence.baseline_coverage["status"] == "PASS"


def test_frozen_promotion_uses_aggregate_evidence_not_impossible_all_sleeve_gate(
    monkeypatch,
) -> None:
    from futures_rebuild import tier1_frozen_trial_pipeline as pipeline

    monkeypatch.setattr(pipeline, "derive_v10_decision", lambda **_: {
        "schema_version": "old", "decision_id": "a" * 64,
        "inference_executed": True,
        "classification": "FAIL_MULTIPLICITY_OR_CONTROL",
        "candidate_effect_classification": "PASS_EFFECT_GATE",
        "stress_and_baselines_passed": True,
        "distribution_passed": True, "drawdown_passed": True,
        "sleeves": {"ES/08:30/long": {"passed": False}},
    })
    result = pipeline.derive_frozen_trial_decision(
        evaluation={}, evaluation_sessions=(), coverage=None,  # type: ignore[arg-type]
        baseline_coverage={}, crossfit=object(),
        evaluation_completeness={}, seed=1,
    )
    assert result["classification"] == "PASS_HISTORICAL_SCREEN"
    assert result["sleeve_tests_role"].startswith("DIAGNOSTIC_ONLY")
    assert len(result["decision_id"]) == 64


def test_one_unversioned_protocol_is_hash_bound_but_not_registered() -> None:
    protocol = load_frozen_trial_protocol(root=ROOT)
    assert protocol["protocol_id"] == (
        "d647438200d54b60f9c7ddb69117adcd0abc23050b971dae542cda3fbdc21867"
    )
    assert protocol["state"] == "PREPARED_NOT_REGISTERED_SOURCE_ADEQUACY_PENDING"
    assert protocol["lineage"]["new_numbered_successor_creation"] is False
    assert protocol["source"]["source_adequacy_record_id"] is None
    assert protocol["position_and_risk"]["instrument_size"] == (
        "ONE_BOUND_STANDARD_CONTRACT_NO_FRACTIONAL_OR_MICRO_PROXY"
    )


def test_synthetic_verification_binds_the_complete_applicable_test_tree(
    local_evidence_root: Path,
) -> None:
    verification = load_frozen_synthetic_verification(root=local_evidence_root)
    assert verification["applicable_results"]["passed"] == 262
    assert verification["applicable_results"]["failed"] == 0
    assert verification["historical_source_rows_opened"] is False
