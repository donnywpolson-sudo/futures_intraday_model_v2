from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.canonical import sha256_json
from futures_rebuild.tier1_phase8_readiness import (
    _validate_index_metadata,
    audit_tier1_phase8_bracket_readiness,
    audit_tier1_phase8_readiness,
)


ROOT = Path(__file__).parents[1]


def test_readiness_audit_blocks_obsolete_five_minute_predictions(
    local_evidence_root: Path,
) -> None:
    report = audit_tier1_phase8_readiness(root=local_evidence_root).report()

    assert report["status"] == "BLOCKED_NEW_BRACKET_TRIAL_NOT_REGISTERED"
    assert "five-minute" in report["blocker"]
    assert report["result_label"] == "PROVISIONAL_EXECUTION_COSTS"
    assert report["market_data_read"] is False
    assert report["release_publication"] is False


def test_bracket_readiness_reports_registered_trial_and_next_boundary(
    local_evidence_root: Path,
) -> None:
    report = audit_tier1_phase8_bracket_readiness(root=local_evidence_root).report()

    assert report["status"] == "REGISTERED_BRACKET_TRIAL_AWAITING_SEPARATE_REAL_DATA_APPROVAL"
    assert report["risk_audit_policy_controls_pass"] is True
    assert report["old_five_minute_predictions_blocked"] is True
    assert report["registered_trial_id"] == "035955798cd0176732365b9706487ee3bfa6b1a4afa3d0047eeb1ee60744d3ba"
    assert report["registered_trial_state"] == "CURRENT_REGISTERED_BEFORE_BRACKET_SOURCE_ROW_OPEN"
    assert report["market_data_read"] is False
    assert report["release_publication"] is False
    assert report["live_realism_claim_supported"] is False


def test_readiness_rejects_index_that_does_not_bind_the_audit() -> None:
    with pytest.raises(IntegrityError, match="current audited selection"):
        _validate_index_metadata(
            {
                "release_kind": "phase8_actual_contract_economics_index",
                "metadata": {"interval_count": 677, "market_year_count": 644, "rulebook_hash": "a" * 64},
                "source_release_ids": [],
            },
            rulebook_hash="a" * 64,
            audit_release_id="b" * 64,
        )


def test_canonical_rulebook_hash_ignores_json_formatting() -> None:
    value = {"rules": [{"market": "ES", "point_value": "50"}], "version": "1"}

    assert sha256_json(value) == sha256_json({"version": "1", "rules": [{"point_value": "50", "market": "ES"}]})
