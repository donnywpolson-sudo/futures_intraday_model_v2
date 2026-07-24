from dataclasses import replace
from datetime import timedelta

import pytest

from futures_rebuild.errors import ContractError, IntegrityError
from futures_rebuild.clock import SyntheticClock
from futures_rebuild.data_layout import MANIFEST_ROOT, PhasePublisher
from futures_rebuild.ledger import LedgerHeadContract, PredictionLedger
from futures_rebuild.schemas import (
    FeatureLineage,
    FeatureRow,
    OutcomeCoverageReport,
    OutcomeRow,
    OutcomeStatus,
    PredictionRow,
    prediction_id_for,
)
from futures_rebuild.time_contracts import AvailabilityBasis


def _receipts(release_factory):
    _, raw = release_factory(
        release_kind="futures_phase2_causal_interval",
        filename="rows.bin",
        content=b"raw",
    )
    _, features = release_factory(
        release_kind="feature_release",
        filename="rows.bin",
        content=b"features",
    )
    return raw, features


def feature_row(
    contract,
    decision,
    values,
    *,
    release_factory,
    boundary,
    available=None,
    declared_available=None,
    complete=True,
    allowed_override=None,
    segment_override=None,
):
    available = available or decision
    raw, features = _receipts(release_factory)
    lineage = {
        name: FeatureLineage(
            raw.release_id,
            available,
            "1" * 64,
            AvailabilityBasis.DERIVED_FROM_VERIFIED_UPSTREAM,
            "2" * 64,
            decision,
            segment_override or contract.contract_segment_hash,
        )
        for name in values
    }
    return FeatureRow(
        actual=contract,
        bar_event_at=decision - timedelta(minutes=1),
        decision_at=decision,
        available_at_max=declared_available or available,
        source_release_id=features.release_id,
        allowed_upstream_release_ids=allowed_override or (raw.release_id,),
        verified_release_receipts=tuple(sorted((raw, features), key=lambda item: item.release_id)),
        boundary=boundary,
        values=values,
        lineage=lineage,
        inputs_complete=complete,
        planned_entry_at=decision + timedelta(minutes=1),
        label_unlock_at=decision + timedelta(days=1),
    )


def _prediction(contract, decision, feature_row_id):
    entry = decision + timedelta(minutes=1)
    unlock = decision + timedelta(days=1)
    prediction_id = prediction_id_for(
        bundle_id="b" * 64,
        actual=contract,
        decision_at=decision,
        recorded_at=decision,
        source_release_id="a" * 64,
        source_release_receipt_id="c" * 64,
        economics_record_id="e" * 64,
        feature_row_id=feature_row_id,
        planned_entry_at=entry,
        label_unlock_at=unlock,
    )
    return PredictionRow(
        prediction_id,
        "b" * 64,
        contract,
        decision,
        decision,
        "a" * 64,
        "c" * 64,
        "e" * 64,
        feature_row_id,
        entry,
        unlock,
        False,
        (),
        0.0,
        0.4,
        0.4,
        0.2,
        0.1,
    )


def _prediction_ledger(boundary, operation_factory, decision):
    receipt = operation_factory("APPEND_PREDICTION")
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )
    return PredictionLedger(
        boundary.active_root / MANIFEST_ROOT / "predictions",
        boundary.active_root / "state" / "locks" / "ledger.lock",
        boundary.active_root / "state" / "anchors",
        boundary.active_root / "state" / "ledger_heads" / "head.json",
        publisher=publisher,
        max_append_delay=timedelta(minutes=5),
        clock=SyntheticClock(boundary, receipt, decision),
        boundary=boundary,
        operation_receipt=receipt,
    )


def test_feature_row_rejects_future_poison(contract, decision, release_factory, boundary) -> None:
    with pytest.raises(ContractError):
        feature_row(
            contract, decision, {"future_return": 0.1},
            release_factory=release_factory, boundary=boundary,
        )
    with pytest.raises(ContractError):
        feature_row(
            contract, decision, {"past_return": 0.1},
            available=decision + timedelta(seconds=1),
            release_factory=release_factory, boundary=boundary,
        )


def test_past_return_is_valid_feature(contract, decision, release_factory, boundary) -> None:
    row = feature_row(
        contract, decision, {"past_return": 0.1},
        release_factory=release_factory, boundary=boundary,
    )
    assert row.values["past_return"] == 0.1


def test_feature_row_rejects_release_role_aliasing(
    contract, decision, release_factory, boundary
) -> None:
    _, causal = release_factory(
        release_kind="futures_phase2_causal_interval",
        filename="rows.bin",
        content=b"causal",
    )
    _, features = release_factory(
        release_kind="feature_release",
        filename="rows.bin",
        content=b"features",
    )
    _, outcomes = release_factory(
        release_kind="historical_outcomes",
        filename="rows.bin",
        content=b"outcomes",
    )

    def build(source, upstream):
        return FeatureRow(
            actual=contract,
            bar_event_at=decision - timedelta(minutes=1),
            decision_at=decision,
            available_at_max=decision,
            source_release_id=source.release_id,
            allowed_upstream_release_ids=(upstream.release_id,),
            verified_release_receipts=tuple(
                sorted((source, upstream), key=lambda item: item.release_id)
            ),
            boundary=boundary,
            values={"past_return": 0.1},
            lineage={
                "past_return": FeatureLineage(
                    upstream.release_id,
                    decision,
                    "1" * 64,
                    AvailabilityBasis.DERIVED_FROM_VERIFIED_UPSTREAM,
                    "2" * 64,
                    decision,
                    contract.contract_segment_hash,
                )
            },
            inputs_complete=True,
            planned_entry_at=decision + timedelta(minutes=1),
            label_unlock_at=decision + timedelta(days=1),
        )

    with pytest.raises(ContractError, match="feature-release role"):
        build(outcomes, causal)
    with pytest.raises(ContractError, match="causal-bar release role"):
        build(features, outcomes)


def test_feature_row_rejects_mismatched_max_and_nonallowed_lineage(
    contract, decision, release_factory, boundary
) -> None:
    with pytest.raises(ContractError, match="computed maximum"):
        feature_row(
            contract, decision, {"past_return": 0.1},
            available=decision - timedelta(seconds=2),
            declared_available=decision - timedelta(seconds=1),
            release_factory=release_factory, boundary=boundary,
        )
    with pytest.raises(ContractError, match="non-allowlisted"):
        feature_row(
            contract, decision, {"past_return": 0.1},
            allowed_override=("f" * 64,),
            release_factory=release_factory, boundary=boundary,
        )


def test_feature_row_rejects_runtime_non_numeric_and_roll(
    contract, decision, release_factory, boundary
) -> None:
    with pytest.raises(ContractError):
        feature_row(
            contract, decision, {"bad": "1.0"},
            release_factory=release_factory, boundary=boundary,
        )
    with pytest.raises(ContractError):
        feature_row(
            contract, decision, {"bars_until_roll": 2},
            release_factory=release_factory, boundary=boundary,
        )
    with pytest.raises(ContractError, match="instrument_id boundary"):
        feature_row(
            contract, decision, {"past_return": 0.1},
            segment_override="f" * 64,
            release_factory=release_factory, boundary=boundary,
        )


def test_prediction_rejects_nonfinite_uncertainty(contract, decision) -> None:
    with pytest.raises(ContractError):
        PredictionRow(
            prediction_id="p" * 64,
            bundle_id="b" * 64,
            actual=contract,
            decision_at=decision,
            recorded_at=decision,
            source_release_id="r" * 64,
            source_release_receipt_id="s" * 64,
            economics_record_id="e" * 64,
            feature_row_id="f" * 64,
            planned_entry_at=decision + timedelta(minutes=1),
            label_unlock_at=decision + timedelta(days=1),
            abstained=False,
            abstention_reasons=(),
            expected_return=0.0,
            probability_up=0.4,
            probability_down=0.4,
            probability_neutral=0.2,
            uncertainty=float("nan"),
        )


def test_unresolved_outcome_cannot_smuggle_return(contract, decision) -> None:
    with pytest.raises(ContractError):
        OutcomeRow(
            "9" * 64,
            contract,
            decision,
            decision + timedelta(days=1),
            decision + timedelta(days=1),
            "a" * 64,
            (contract.contract_segment_hash,),
            True,
            OutcomeStatus.HALTED,
            0.2,
        )


def test_future_contract_change_stays_unresolved_in_coverage(
    contract, decision, boundary, operation_factory
) -> None:
    prediction = _prediction(contract, decision, "f" * 64)
    ledger = _prediction_ledger(boundary, operation_factory, decision)
    ledger.append(prediction, expected_head=LedgerHeadContract.genesis())
    unresolved = OutcomeRow(
        prediction.prediction_id,
        contract,
        decision,
        decision + timedelta(days=1),
        decision + timedelta(days=1),
        "a" * 64,
        (contract.contract_segment_hash, "f" * 64),
        True,
        OutcomeStatus.ROLL_UNRESOLVED,
        None,
    )
    report = OutcomeCoverageReport(ledger.issue_census(), (unresolved,), ledger)
    assert report.denominator_count == 1
    assert report.resolved_count == 0
    assert report.unresolved_count == 1
    with pytest.raises(ContractError, match="cross-contract outcome"):
        OutcomeRow(
            "9" * 64,
            contract,
            decision,
            decision + timedelta(days=1),
            decision + timedelta(days=1),
            "a" * 64,
            (contract.contract_segment_hash, "f" * 64),
            True,
            OutcomeStatus.MATURED,
            0.1,
        )
    with pytest.raises(ContractError, match="coverage denominator"):
        replace(unresolved, included_in_coverage_denominator=False)


def test_outcome_coverage_rejects_silent_prediction_deletion(
    contract, decision, boundary, operation_factory
) -> None:
    first = _prediction(contract, decision, "f" * 64)
    second = _prediction(contract, decision, "d" * 64)
    ledger = _prediction_ledger(boundary, operation_factory, decision)
    first_append = ledger.append(first, expected_head=LedgerHeadContract.genesis())
    ledger.append(second, expected_head=first_append.head)
    unresolved = OutcomeRow(
        first.prediction_id,
        contract,
        decision,
        decision + timedelta(days=1),
        decision + timedelta(days=1),
        "a" * 64,
        (contract.contract_segment_hash,),
        True,
        OutcomeStatus.MISSING_SOURCE,
        None,
    )
    with pytest.raises(ContractError, match="exactly one row"):
        OutcomeCoverageReport(ledger.issue_census(), (unresolved,), ledger)


def test_prediction_census_detects_forgery_staleness_and_joint_deletion(
    contract, decision, boundary, operation_factory
) -> None:
    first = _prediction(contract, decision, "f" * 64)
    second = _prediction(contract, decision, "d" * 64)
    ledger = _prediction_ledger(boundary, operation_factory, decision)
    first_append = ledger.append(first, expected_head=LedgerHeadContract.genesis())
    census = ledger.issue_census()
    with pytest.raises(IntegrityError, match="forged|stale|truncated"):
        replace(census, prediction_ids=("0" * 64,)).verify(ledger)
    ledger.append(second, expected_head=first_append.head)
    with pytest.raises(IntegrityError, match="forged|stale|truncated"):
        census.verify(ledger)

    for root in (ledger.prediction_manifest_root, ledger.anchor_root):
        for path in root.glob("*.json"):
            path.unlink()
    ledger.persistent_head_path.unlink()
    with pytest.raises((ContractError, IntegrityError)):
        census.verify(ledger)
