import json
from dataclasses import replace
from datetime import timedelta

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.clock import SyntheticClock
from futures_rebuild.data_layout import MANIFEST_ROOT, PhasePublisher
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.ledger import LedgerHeadContract, PredictionLedger
from futures_rebuild.migration import (
    MigrationApproval,
    approval_payload_for_review,
    guarded_copy,
    inventory,
    load_manifest,
    migration_authorization_scope,
)
from futures_rebuild.schemas import PredictionRow, prediction_id_for


def prediction(contract, decision, *, economics="e" * 64):
    entry = decision + timedelta(minutes=10)
    label_unlock = decision + timedelta(days=1)
    prediction_id = prediction_id_for(
        bundle_id="b" * 64,
        actual=contract,
        decision_at=decision,
        recorded_at=decision,
        source_release_id="a" * 64,
        source_release_receipt_id="c" * 64,
        economics_record_id=economics,
        feature_row_id="f" * 64,
        planned_entry_at=entry,
        label_unlock_at=label_unlock,
    )
    return PredictionRow(
        prediction_id=prediction_id,
        bundle_id="b" * 64,
        actual=contract,
        decision_at=decision,
        recorded_at=decision,
        source_release_id="a" * 64,
        source_release_receipt_id="c" * 64,
        economics_record_id=economics,
        feature_row_id="f" * 64,
        planned_entry_at=entry,
        label_unlock_at=label_unlock,
        abstained=False,
        abstention_reasons=(),
        expected_return=0.0,
        probability_up=0.4,
        probability_down=0.4,
        probability_neutral=0.2,
        uncertainty=0.1,
    )


def ledger(boundary, operation_factory, decision):
    receipt = operation_factory("APPEND_PREDICTION")
    clock = SyntheticClock(boundary, receipt, decision)
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )
    target = PredictionLedger(
        boundary.active_root / MANIFEST_ROOT / "predictions",
        boundary.active_root / "state" / "locks" / "ledger.lock",
        boundary.active_root / "state" / "anchors",
        boundary.active_root / "state" / "ledger_heads" / "head.json",
        publisher=publisher,
        max_append_delay=timedelta(minutes=5),
        clock=clock,
        boundary=boundary,
        operation_receipt=receipt,
    )
    return target, clock


def test_prediction_ledger_is_anchored_idempotent_and_tamper_evident(
    boundary, operation_factory, contract, decision
) -> None:
    target_ledger, clock = ledger(boundary, operation_factory, decision)
    item = prediction(contract, decision)
    first = target_ledger.append(item, expected_head=LedgerHeadContract.genesis())
    assert len(target_ledger.verify()) == 1
    clock.set(decision + timedelta(minutes=20))
    retry = target_ledger.append(item, expected_head=LedgerHeadContract.genesis())
    assert retry.idempotent_retry and retry.head == first.head
    wrong_prior = replace(LedgerHeadContract.genesis(), anchor_hash="0" * 64)
    with pytest.raises(IntegrityError):
        target_ledger.append(item, expected_head=wrong_prior)
    payload = json.loads(first.path.read_text(encoding="utf-8"))
    payload["prediction"]["expected_return"] = 99
    first.path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(IntegrityError):
        target_ledger.verify()


def test_historical_post_outcome_insertion_into_empty_ledger_fails(
    boundary, operation_factory, contract, decision
) -> None:
    target_ledger, clock = ledger(boundary, operation_factory, decision)
    clock.set(decision + timedelta(days=2))
    with pytest.raises(IntegrityError, match="timing window"):
        target_ledger.append(
            prediction(contract, decision), expected_head=LedgerHeadContract.genesis()
        )


def test_prediction_ledger_rejects_noncanonical_authorization_scope(
    boundary, operation_factory, contract, decision
) -> None:
    receipt = operation_factory(
        "APPEND_PREDICTION", scope={"unexpected": "scope"}
    )
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )
    target_ledger = PredictionLedger(
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
    with pytest.raises(UnauthorizedOperation, match="exact required scope"):
        target_ledger.append(
            prediction(contract, decision),
            expected_head=LedgerHeadContract.genesis(),
        )


def test_prediction_record_crash_before_anchor_recovers_after_window(
    boundary, operation_factory, contract, decision, monkeypatch
) -> None:
    target_ledger, clock = ledger(boundary, operation_factory, decision)
    item = prediction(contract, decision)
    original = target_ledger._write_anchor

    def crash(record, previous, receipt):
        raise OSError("synthetic anchor crash")

    monkeypatch.setattr(target_ledger, "_write_anchor", crash)
    with pytest.raises(OSError, match="anchor crash"):
        target_ledger.append(item, expected_head=LedgerHeadContract.genesis())
    monkeypatch.setattr(target_ledger, "_write_anchor", original)
    clock.set(decision + timedelta(minutes=20))
    recovered = target_ledger.append(item, expected_head=LedgerHeadContract.genesis())
    assert recovered.idempotent_retry and len(target_ledger.verify()) == 1


def test_crash_after_anchor_before_head_recovers_only_exact_retry(
    boundary, operation_factory, contract, decision, monkeypatch
) -> None:
    target_ledger, clock = ledger(boundary, operation_factory, decision)
    item = prediction(contract, decision)
    original = target_ledger._write_persistent_head

    def crash(head):
        raise OSError("synthetic head crash")

    monkeypatch.setattr(target_ledger, "_write_persistent_head", crash)
    with pytest.raises(OSError, match="head crash"):
        target_ledger.append(item, expected_head=LedgerHeadContract.genesis())
    monkeypatch.setattr(target_ledger, "_write_persistent_head", original)
    clock.set(decision + timedelta(minutes=20))
    recovered = target_ledger.append(item, expected_head=LedgerHeadContract.genesis())
    assert recovered.idempotent_retry and len(target_ledger.verify()) == 1


def test_persistent_head_detects_tail_deletion_and_backwards_clock(
    boundary, operation_factory, contract, decision
) -> None:
    target_ledger, clock = ledger(boundary, operation_factory, decision)
    result = target_ledger.append(
        prediction(contract, decision), expected_head=LedgerHeadContract.genesis()
    )
    clock.set(decision - timedelta(seconds=1))
    with pytest.raises(IntegrityError, match="backwards"):
        target_ledger.append(
            replace(prediction(contract, decision), prediction_id="0" * 64),
            expected_head=result.head,
        )
    next((boundary.active_root / "state" / "anchors").glob("*.json")).unlink()
    result.path.unlink()
    with pytest.raises(IntegrityError, match="persistent external head|differs"):
        target_ledger.verify()


def test_missing_persistent_head_without_crash_intent_is_not_recoverable(
    boundary, operation_factory, contract, decision
) -> None:
    target_ledger, _ = ledger(boundary, operation_factory, decision)
    item = prediction(contract, decision)
    target_ledger.append(item, expected_head=LedgerHeadContract.genesis())
    target_ledger.persistent_head_path.unlink()
    with pytest.raises(IntegrityError, match="persistent external ledger head is missing"):
        target_ledger.append(item, expected_head=LedgerHeadContract.genesis())


def test_crash_after_head_before_intent_clear_recovers_exactly(
    boundary, operation_factory, contract, decision, monkeypatch
) -> None:
    target_ledger, clock = ledger(boundary, operation_factory, decision)
    item = prediction(contract, decision)
    original = target_ledger._clear_intent

    def crash():
        raise OSError("synthetic intent-clear crash")

    monkeypatch.setattr(target_ledger, "_clear_intent", crash)
    with pytest.raises(OSError, match="intent-clear crash"):
        target_ledger.append(item, expected_head=LedgerHeadContract.genesis())
    monkeypatch.setattr(target_ledger, "_clear_intent", original)
    clock.set(decision + timedelta(minutes=20))
    recovered = target_ledger.append(item, expected_head=LedgerHeadContract.genesis())
    assert recovered.idempotent_retry and len(target_ledger.verify()) == 1


def test_crash_recovery_rejects_different_prediction_payload(
    boundary, operation_factory, contract, decision, monkeypatch
) -> None:
    target_ledger, _ = ledger(boundary, operation_factory, decision)
    original = target_ledger._write_anchor

    def crash(record, previous, receipt):
        raise OSError("synthetic anchor crash")

    monkeypatch.setattr(target_ledger, "_write_anchor", crash)
    with pytest.raises(OSError):
        target_ledger.append(
            prediction(contract, decision, economics="1" * 64),
            expected_head=LedgerHeadContract.genesis(),
        )
    monkeypatch.setattr(target_ledger, "_write_anchor", original)
    with pytest.raises(IntegrityError, match="intent does not match"):
        target_ledger.append(
            prediction(contract, decision, economics="2" * 64),
            expected_head=LedgerHeadContract.genesis(),
        )


def test_economics_change_changes_prediction_identity_and_old_id_conflicts(
    contract, decision
) -> None:
    first = prediction(contract, decision, economics="1" * 64)
    second = prediction(contract, decision, economics="2" * 64)
    assert first.prediction_id != second.prediction_id
    with pytest.raises(IntegrityError):
        # Altered economics with the old identity is rejected before deduplication.
        from futures_rebuild.ledger import _prediction_from_payload, _prediction_payload

        payload = _prediction_payload(replace(first, economics_record_id="2" * 64))
        _prediction_from_payload(payload)


def test_prediction_ledger_parser_rejects_boolean_integer_alias(
    contract, decision
) -> None:
    from futures_rebuild.ledger import _prediction_from_payload, _prediction_payload

    payload = _prediction_payload(prediction(contract, decision))
    actual = payload["actual_contract"]
    assert isinstance(actual, dict)
    actual["publisher_id"] = True
    with pytest.raises(IntegrityError, match="field types"):
        _prediction_from_payload(payload)


def test_migration_inventory_hashes_allowlisted_files_and_copy_is_blocked(tmp_path) -> None:
    source = tmp_path / "legacy"
    destination = tmp_path / "new" / "stage"
    (source / "data" / "dbn").mkdir(parents=True)
    (source / "data" / "dbn" / "sample.dbn.zst").write_bytes(b"provider-bytes")
    manifest_path = tmp_path / "migration.json"
    manifest_path.write_text(json.dumps({
        "migration_id": "synthetic_inventory",
        "source_root": str(source.resolve()),
        "destination_root": str(destination.resolve()),
        "copy_authorized": False,
        "policy": {
            "operation": "copy_only",
            "overwrite": False,
            "follow_links": False,
            "require_source_stable_during_copy": True,
            "verify_destination_sha256": True,
        },
        "entries": [{"family": "dbn", "source": "data/dbn", "destination": "dbn", "disposition": "validate_then_promote"}],
    }), encoding="utf-8")
    manifest, digest = load_manifest(manifest_path)
    result = inventory(manifest, digest)
    approval = MigrationApproval.from_dict(
        approval_payload_for_review(
            manifest_hash=digest,
            source_inventory=result,
            approved_at="2026-07-15T00:00:00Z",
        )
    )
    assert result["total_files"] == 1 and result["total_bytes"] == len(b"provider-bytes")
    assert not destination.exists()
    boundary = RepoBoundary(destination.parent.resolve(), (source.resolve(),), ())
    receipt = OperationReceipt.issue_local(
        boundary,
        operation="COPY_SOURCE_SNAPSHOT",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope=migration_authorization_scope(
            manifest, digest, result["inventory_sha256"], approval
        ),
    )
    with pytest.raises(UnauthorizedOperation):
        guarded_copy(
            manifest,
            digest,
            digest,
            result["inventory_sha256"],
            migration_approval=approval,
            boundary=boundary,
            operation_receipt=receipt,
        )
