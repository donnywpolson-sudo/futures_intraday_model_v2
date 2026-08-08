from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CERTIFICATE = ROOT / "configs/tier1_bracket_preexecution_validity_certificate.json"


def _certificate() -> dict[str, object]:
    return json.loads(CERTIFICATE.read_text(encoding="utf-8"))


def test_certificate_is_honestly_not_ready_until_sources_and_pointer_pass() -> None:
    payload = _certificate()
    gates = {item["gate"]: item for item in payload["gates"]}
    assert payload["overall_decision"] == "NOT_READY"
    assert gates["ONE_IMMUTABLE_SOURCE_SET_BOUND"]["status"] == "FAIL"
    assert gates["SOURCE_COVERAGE_SUFFICIENT_FOR_EVERY_REQUIRED_DEPENDENCY"]["status"] == "FAIL"
    assert gates["ONE_AUTHORITATIVE_EXECUTION_READY_TRIAL_POINTER"]["status"] == "FAIL"
    assert {item["status"] for item in payload["gates"]} == {"PASS", "FAIL"}


def test_every_passing_gate_has_existing_executable_or_governance_evidence() -> None:
    payload = _certificate()
    for gate in payload["gates"]:
        if gate["status"] != "PASS":
            continue
        assert gate["evidence"]
        for reference in gate["evidence"]:
            relative = reference.split("::", 1)[0]
            assert (ROOT / relative).is_file(), reference


def test_certificate_binds_freeze_and_locked_runtime_without_authority_expansion() -> None:
    payload = _certificate()
    freeze = ROOT / payload["version_freeze_reconciliation_path"]
    lock = ROOT / payload["locked_runtime"]["dependency_lock_path"]
    import hashlib
    assert hashlib.sha256(freeze.read_bytes()).hexdigest() == payload["version_freeze_reconciliation_sha256"]
    assert hashlib.sha256(lock.read_bytes()).hexdigest() == payload["locked_runtime"]["dependency_lock_sha256"]
    assert payload["synthetic_verification"]["historical_price_rows_opened"] is False
    assert payload["holdout_2025_touched"] is False
    assert payload["provider_access"] is False
    assert payload["trading"] is False


def test_post_source_successor_preserves_prior_and_stays_not_ready() -> None:
    successor = json.loads((ROOT / "configs/tier1_bracket_preexecution_validity_certificate_post_source.json").read_text(encoding="utf-8"))
    import hashlib
    prior = ROOT / successor["supersedes_certificate_path"]
    assert hashlib.sha256(prior.read_bytes()).hexdigest() == successor["supersedes_certificate_sha256"]
    assert successor["overall_decision"] == "NOT_READY"
    assert successor["source_set_binding"]["status"] == "PASS"
    assert successor["source_coverage"]["status"] == "FAIL"
    assert successor["source_coverage"]["market_years_failed"] == 19
    assert successor["authoritative_trial_pointer"]["trial_id"] is None
