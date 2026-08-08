from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_reported_trade_exit_tier0 import (
    MECHANISM_ID,
    MECHANISM_PATH,
    REQUIRED_TEST_FILES,
    TIER0_CERTIFICATE_PATH,
    TIER0_DECISION_PATH,
    build_certificate,
    build_decision,
    validate_certificate,
    validate_live_evidence,
    validate_mechanism_context,
)
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]


def _synthetic_full_lane_nodes() -> tuple[str, ...]:
    files = sorted(REQUIRED_TEST_FILES)
    return tuple(sorted(
        f"{files[index % len(files)]}::test_synthetic_lane_{index:03d}"
        for index in range(100)
    ))


def test_mechanism_context_binds_published_closure_and_has_no_authority() -> None:
    contract, profile = validate_mechanism_context(root=ROOT)
    mechanism = json.loads((ROOT / MECHANISM_PATH).read_text(encoding="utf-8"))
    assert contract["contract_id"] == mechanism["ladder_binding"]["contract_id"]
    assert profile["profile_id"] == mechanism["ladder_binding"]["profile_id"]
    assert mechanism["mechanism_id"] == MECHANISM_ID
    assert set(mechanism["authority"].values()) == {False}
    assert set(mechanism["outcome_access"].values()) == {False}


def test_certificate_is_synthetic_only_and_hash_binds_the_complete_lane() -> None:
    certificate = build_certificate(
        root=ROOT, collected_test_nodes=_synthetic_full_lane_nodes(),
    )
    validate_certificate(certificate, root=ROOT)
    assert certificate["decision"] == "PASS"
    assert certificate["historical_rows_opened"] is False
    assert certificate["alpha_evidence"] is False
    assert certificate["profitability_claim"] is False
    assert certificate["source_compatibility_claim"] is False
    assert certificate["passed_test_count"] == 100
    assert set(REQUIRED_TEST_FILES) <= {
        node.split("::", 1)[0] for node in certificate["test_node_ids"]
    }


def test_certificate_rejects_a_changed_suite_binding() -> None:
    certificate = build_certificate(
        root=ROOT, collected_test_nodes=_synthetic_full_lane_nodes(),
    )
    changed = copy.deepcopy(certificate)
    first = next(iter(changed["suite_bindings"]))
    changed["suite_bindings"][first] = "0" * 64
    core = {key: value for key, value in changed.items() if key != "certificate_id"}
    changed["certificate_id"] = sha256_json(core)
    with pytest.raises(IntegrityError):
        validate_certificate(changed, root=ROOT)


def test_decision_is_bound_to_the_exact_certificate() -> None:
    certificate = build_certificate(
        root=ROOT, collected_test_nodes=_synthetic_full_lane_nodes(),
    )
    decision = build_decision(root=ROOT, certificate=certificate)
    assert decision["stage"] == "tier_0"
    assert decision["decision"] == "PASS"
    assert decision["synthetic_certificate_path"] == TIER0_CERTIFICATE_PATH.as_posix()


def test_live_evidence_is_transition_stable_when_present(
    local_evidence_root: Path,
) -> None:
    certificate_exists = (local_evidence_root / TIER0_CERTIFICATE_PATH).exists()
    decision_exists = (local_evidence_root / TIER0_DECISION_PATH).exists()
    assert certificate_exists == decision_exists
    if certificate_exists:
        result = validate_live_evidence(root=local_evidence_root)
        assert result["mechanism_id"] == MECHANISM_ID
        assert int(result["passed_test_count"]) >= 100


def test_tier0_decision_without_certificate_fails_closed(tmp_path: Path) -> None:
    decision = tmp_path / TIER0_DECISION_PATH
    decision.parent.mkdir(parents=True, exist_ok=True)
    decision.write_text("{}\n", encoding="utf-8")
    with pytest.raises(IntegrityError, match="complete pair"):
        validate_live_evidence(root=tmp_path)
