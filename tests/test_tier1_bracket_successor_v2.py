import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_bracket_successor_v2 import (
    CLOSURE_EVENT_ROOT,
    CLOSURE_REGISTRY_ROOT,
    SUCCESSOR_EVENT_ROOT,
    SUCCESSOR_REGISTRY_ROOT,
    PreparedRecord,
    load_failed_trial_closure_preparation,
    load_successor_v2_contract,
    persist_failed_trial_closure,
    persist_successor_v2_registration,
    prepare_failed_trial_closure,
    prepare_successor_v2_registration,
    verify_failed_trial_closure,
    verify_successor_v2_registration,
)


ROOT = Path(__file__).parents[1]


def test_closure_preparation_preserves_bound_artifacts_and_is_registered() -> None:
    preparation = load_failed_trial_closure_preparation(root=ROOT)
    prepared = prepare_failed_trial_closure(root=ROOT)

    assert preparation["state"] == "PREPARED_NOT_PUBLISHED_NOT_ACTIVE"
    assert prepared.payload["state"] == "PREPARED_FOR_CLOSURE_CONFIRMATION"
    assert prepared.payload["publication_authorized"] is False
    assert prepared.payload["activation_authorized"] is False
    assert prepared.record_id == sha256_json(prepared.payload)
    registry_path = ROOT / CLOSURE_REGISTRY_ROOT / f"{prepared.record_id}.json"
    assert registry_path.exists()
    verification = verify_failed_trial_closure(
        root=ROOT,
        closure_id=prepared.record_id,
    )
    assert verification["closure_id"] == prepared.record_id


def test_successor_contract_is_decision_complete_and_registered() -> None:
    contract = load_successor_v2_contract(root=ROOT)
    prepared = prepare_successor_v2_registration(root=ROOT)

    assert contract["entry_policy"]["minimum_selected_predicted_net_r"] == "0.25"
    assert contract["model"]["family"] == "MARKET_SPECIFIC_TWO_TARGET_RIDGE"
    assert contract["promotion_gate"]["minimum_positive_market_years_of_12"] == 6
    assert prepared.payload["state"] == "REGISTERED_BEFORE_SOURCE_ROW_OR_OUTCOME_ACCESS"
    assert prepared.payload["source_row_access"] is False
    assert prepared.payload["model_fit"] is False
    assert prepared.payload["economics_evaluation"] is False
    assert len(prepared.payload["source_pairs"]) == 20
    assert prepared.record_id == sha256_json(prepared.payload)
    registry_path = ROOT / SUCCESSOR_REGISTRY_ROOT / f"{prepared.record_id}.json"
    assert registry_path.exists()
    verification = verify_successor_v2_registration(
        root=ROOT,
        trial_id=prepared.record_id,
    )
    assert verification["trial_id"] == prepared.record_id


def test_approved_persistence_is_create_only_and_keeps_events_non_executing(tmp_path: Path) -> None:
    closure = prepare_failed_trial_closure(root=ROOT)
    successor = prepare_successor_v2_registration(root=ROOT)

    closed = persist_failed_trial_closure(root=tmp_path, prepared=closure)
    registered = persist_successor_v2_registration(root=tmp_path, prepared=successor)

    closure_event = json.loads((tmp_path / closed["event_path"]).read_text(encoding="utf-8"))
    successor_event = json.loads((tmp_path / registered["event_path"]).read_text(encoding="utf-8"))
    assert closure_event["event_type"] == "CLOSED_FAILED_NO_RESCUE"
    assert successor_event["event_type"] == "DECLARED"
    assert successor_event["source_row_access"] is False
    assert successor_event["model_fit"] is False
    assert successor_event["economics_evaluation"] is False
    assert verify_failed_trial_closure(root=tmp_path, closure_id=closure.record_id, prepared=closure)["closure_id"] == closure.record_id
    assert verify_successor_v2_registration(root=tmp_path, trial_id=successor.record_id, prepared=successor)["trial_id"] == successor.record_id

    with pytest.raises(IntegrityError, match="already exists"):
        persist_successor_v2_registration(root=tmp_path, prepared=successor)

    assert (tmp_path / CLOSURE_REGISTRY_ROOT / f"{closure.record_id}.json").exists()
    assert (tmp_path / CLOSURE_EVENT_ROOT / f"{closure.record_id}.json").exists()
    assert (tmp_path / SUCCESSOR_REGISTRY_ROOT / f"{successor.record_id}.json").exists()
    assert (tmp_path / SUCCESSOR_EVENT_ROOT / f"{successor.record_id}.json").exists()


def test_persistence_rejects_a_tampered_preparation(tmp_path: Path) -> None:
    prepared = prepare_successor_v2_registration(root=ROOT)
    tampered = PreparedRecord(prepared.record_id, {**prepared.payload, "source_row_access": True})

    with pytest.raises(IntegrityError, match="preparation is inconsistent"):
        persist_successor_v2_registration(root=tmp_path, prepared=tampered)
