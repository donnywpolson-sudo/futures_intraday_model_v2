from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RECONCILIATION = ROOT / "configs/tier1_bracket_version_freeze_reconciliation.json"


def test_version_freeze_has_one_disposition_per_v2_through_v14() -> None:
    payload = json.loads(RECONCILIATION.read_text(encoding="utf-8"))
    versions = payload["versions"]
    assert [item["version"] for item in versions] == [f"V{i}" for i in range(2, 15)]
    assert len({item["version"] for item in versions}) == 13
    assert all(item["disposition"] for item in versions)
    assert payload["version_creation_frozen"] is True
    assert payload["authoritative_current_trial"] is None
    assert payload["current_state"] == "NO_EXECUTION_READY_TIER1_TRIAL"


def test_reconciliation_evidence_matches_each_existing_artifact() -> None:
    payload = json.loads(RECONCILIATION.read_text(encoding="utf-8"))
    for item in payload["versions"]:
        evidence = item["evidence"]
        if evidence is None:
            assert item["version"] in {"V13", "V14"}
            continue
        artifact = json.loads((ROOT / evidence).read_text(encoding="utf-8"))
        assert artifact.get("disposition", item["disposition"]) == item["disposition"]
        if item["trial_id"] is not None:
            assert artifact.get("trial_id") == item["trial_id"]


def test_freeze_forbids_every_high_risk_action_until_certification() -> None:
    payload = json.loads(RECONCILIATION.read_text(encoding="utf-8"))
    assert payload["execution_freeze"]
    assert set(payload["execution_freeze"].values()) == {False}
    assert payload["certification_target"] == "UNVERSIONED_FROZEN_SUCCESSOR_SNAPSHOT"
