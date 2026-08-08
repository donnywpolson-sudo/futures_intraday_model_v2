from __future__ import annotations

from pathlib import Path

from futures_rebuild.canonical import sha256_json
from futures_rebuild.tier1_bracket_v11_registration import (
    V10_RETIREMENT_EVENT_ROOT, V10_RETIREMENT_REGISTRY_ROOT,
    V11_EVENT_ROOT, V11_REGISTRY_ROOT,
    prepare_v10_retirement_v11, prepare_v11_registration,
)


ROOT = Path(__file__).resolve().parents[1]


def test_v10_retirement_and_v11_registration_are_canonical_and_create_only() -> None:
    retirement = prepare_v10_retirement_v11(root=ROOT)
    registration = prepare_v11_registration(root=ROOT)
    assert retirement.record_id == sha256_json(retirement.canonical_payload)
    assert registration.trial_id == sha256_json(registration.canonical_payload)
    assert registration.canonical_payload["v10_retirement_record_id"] == retirement.record_id
    assert registration.canonical_payload["source_row_access"] is False
    assert registration.canonical_payload["model_fit"] is False
    retirement_registry = ROOT / V10_RETIREMENT_REGISTRY_ROOT / f"{retirement.record_id}.json"
    retirement_event = ROOT / V10_RETIREMENT_EVENT_ROOT / f"{retirement.record_id}.json"
    registration_registry = ROOT / V11_REGISTRY_ROOT / f"{registration.trial_id}.json"
    registration_event = ROOT / V11_EVENT_ROOT / f"{registration.trial_id}.json"
    assert retirement_registry.exists() == retirement_event.exists()
    assert registration_registry.exists() == registration_event.exists()
