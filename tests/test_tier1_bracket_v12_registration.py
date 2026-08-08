from __future__ import annotations

from pathlib import Path

from futures_rebuild.canonical import sha256_json
from futures_rebuild.tier1_bracket_v12_registration import (
    V11_RETIREMENT_EVENT_ROOT, V11_RETIREMENT_REGISTRY_ROOT,
    V12_EVENT_ROOT, V12_REGISTRY_ROOT,
    prepare_v11_retirement_v12, prepare_v12_registration,
)


ROOT = Path(__file__).resolve().parents[1]


def test_v11_retirement_and_v12_registration_are_canonical_and_create_only() -> None:
    retirement = prepare_v11_retirement_v12(root=ROOT)
    registration = prepare_v12_registration(root=ROOT)
    assert retirement.record_id == sha256_json(retirement.canonical_payload)
    assert registration.trial_id == sha256_json(registration.canonical_payload)
    assert registration.canonical_payload["v11_retirement_record_id"] == retirement.record_id
    assert registration.canonical_payload["source_row_access"] is False
    assert registration.canonical_payload["model_fit"] is False
    rr = ROOT / V11_RETIREMENT_REGISTRY_ROOT / f"{retirement.record_id}.json"
    re = ROOT / V11_RETIREMENT_EVENT_ROOT / f"{retirement.record_id}.json"
    vr = ROOT / V12_REGISTRY_ROOT / f"{registration.trial_id}.json"
    ve = ROOT / V12_EVENT_ROOT / f"{registration.trial_id}.json"
    assert rr.exists() == re.exists()
    assert vr.exists() == ve.exists()
