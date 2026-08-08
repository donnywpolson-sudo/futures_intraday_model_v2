from __future__ import annotations

from pathlib import Path

from futures_rebuild.canonical import sha256_json
from futures_rebuild.tier1_bracket_v10_registration import (
    V10_EVENT_ROOT, V10_REGISTRY_ROOT,
    prepare_v9_retirement_v10, prepare_v10_registration,
)


ROOT = Path(__file__).resolve().parents[1]


def test_v9_retirement_and_v10_registration_are_prepared_create_only() -> None:
    retirement = prepare_v9_retirement_v10(root=ROOT)
    registration = prepare_v10_registration(root=ROOT)
    assert retirement.record_id == sha256_json(retirement.canonical_payload)
    assert registration.trial_id == sha256_json(registration.canonical_payload)
    assert registration.canonical_payload["v9_retirement_record_id"] == retirement.record_id
    assert registration.canonical_payload["source_row_access"] is False
    assert registration.canonical_payload["model_fit"] is False
    assert not (ROOT / V10_REGISTRY_ROOT / f"{registration.trial_id}.json").exists()
    assert not (ROOT / V10_EVENT_ROOT / f"{registration.trial_id}.json").exists()
