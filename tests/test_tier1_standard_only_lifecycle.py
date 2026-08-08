from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_standard_only_lifecycle import (
    FOUNDATIONAL_GATES,
    REPLACED_PENDING_GATES,
    build_standard_only_lifecycle_payloads,
    load_prepared_standard_only_certificate,
    persist_standard_only_lifecycle,
)


ROOT = Path(__file__).resolve().parents[1]


def _inputs():
    protocol = {
        "state": "PUBLISHED_PRE_REGISTRATION_PROTOCOL_ONLY",
        "classification": "ONE_UNVERSIONED_PREREGISTERED_TIER1_HISTORICAL_SCREEN_STANDARD_ONLY",
        "protocol_id": "a" * 64,
        "source": {
            "historical_l1_bbo_dependency": False,
            "selected_sources_id": "b" * 64,
            "calendar_release_id": "c" * 64,
        },
    }
    correction = {
        "state": "PUBLISHED_PRE_REGISTRATION_SOURCE_POLICY_CORRECTION_ONLY",
        "record_id": "d" * 64,
        "correction": {
            "all_checkpoints_retained_with_terminal_source_status": True,
            "missing_prices_or_fills_invented": False,
            "missing_execution_return_imputed_as_zero": False,
            "runner_up_substitution_after_missing_selected_path": False,
            "selected_missing_path_result": "INCONCLUSIVE_DATA_OR_COVERAGE",
            "promotion_possible_with_selected_missing_path": False,
        },
        "publication_scope": {
            "trial_registration": False,
            "historical_rows_read": False,
        },
    }
    synthetic = {
        "state": "PREPARED_NOT_PUBLISHED",
        "verification_id": "e" * 64,
        "applicable_results": {"passed": 29, "failed": 0},
        "selected_missing_path_forces_inconclusive": True,
        "missing_path_zero_return_imputation_forbidden": True,
        "runner_up_substitution_forbidden": True,
    }
    mechanics = {
        "overall_decision": "NOT_READY",
        "locked_runtime": {
            "dependency_lock_path": "configs/dependency_lock_receipt.json",
            "dependency_lock_sha256": "e6302e16d30d114b0c3140f2b075fd6850558ce4ea245475d2687b8531db1726",
        },
        "gates": [
            *({"gate": gate, "status": "PASS"} for gate in FOUNDATIONAL_GATES),
            *({"gate": gate, "status": "FAIL"} for gate in REPLACED_PENDING_GATES),
        ],
        "holdout_2025_touched": False,
        "provider_access": False,
        "trading": False,
    }
    reconciliation = {
        "authoritative_current_trial": None,
        "current_state": "NO_EXECUTION_READY_TIER1_TRIAL",
        "version_creation_frozen": True,
        "versions": [{} for _ in range(13)],
    }
    return protocol, correction, synthetic, mechanics, reconciliation


def test_standard_only_lifecycle_prepares_consistent_unpublished_payloads():
    protocol, correction, synthetic, mechanics, reconciliation = _inputs()
    prepared = build_standard_only_lifecycle_payloads(
        protocol=protocol, correction=correction, synthetic=synthetic,
        mechanics=mechanics, reconciliation=reconciliation,
        bindings={"bound": "f" * 64},
    )
    assert prepared.trial_id == sha256_json(prepared.trial)
    assert prepared.pointer_id == sha256_json(prepared.pointer)
    assert prepared.certificate_id == sha256_json(prepared.certificate)
    assert prepared.certificate["overall_decision"] == "PASS"
    assert prepared.certificate["source_coverage"]["selected_execution_path_gate"] == (
        "REQUIRES_100_PERCENT_OR_EVALUATION_IS_INCONCLUSIVE"
    )
    assert prepared.trial["publication"] is False


@pytest.mark.parametrize(
    "fault",
    [
        "invented_fill", "zero_imputation", "runner_up", "promotion",
        "existing_trial", "foundational_gate", "version_inventory",
    ],
)
def test_standard_only_lifecycle_fails_closed_on_policy_drift(fault):
    protocol, correction, synthetic, mechanics, reconciliation = _inputs()
    correction = deepcopy(correction)
    if fault == "invented_fill":
        correction["correction"]["missing_prices_or_fills_invented"] = True
    elif fault == "zero_imputation":
        correction["correction"]["missing_execution_return_imputed_as_zero"] = True
    elif fault == "runner_up":
        correction["correction"]["runner_up_substitution_after_missing_selected_path"] = True
    elif fault == "promotion":
        correction["correction"]["promotion_possible_with_selected_missing_path"] = True
    elif fault == "existing_trial":
        reconciliation["authoritative_current_trial"] = "existing"
    elif fault == "foundational_gate":
        mechanics["gates"][0]["status"] = "FAIL"
    else:
        reconciliation["versions"] = reconciliation["versions"][:-1]
    with pytest.raises(IntegrityError, match="before every gate|foundational mechanics"):
        build_standard_only_lifecycle_payloads(
            protocol=protocol, correction=correction, synthetic=synthetic,
            mechanics=mechanics, reconciliation=reconciliation,
            bindings={"bound": "f" * 64},
        )


def test_standard_only_lifecycle_publication_is_create_only(tmp_path):
    protocol, correction, synthetic, mechanics, reconciliation = _inputs()
    bound = tmp_path / "bound.txt"
    bound.write_text("bound\n", encoding="utf-8")
    prepared = build_standard_only_lifecycle_payloads(
        protocol=protocol, correction=correction, synthetic=synthetic,
        mechanics=mechanics, reconciliation=reconciliation,
        bindings={"bound.txt": sha256_file(bound)},
    )
    published = persist_standard_only_lifecycle(root=tmp_path, prepared=prepared)
    assert set(published) == {
        "trial_id", "registry_path", "event_path",
        "certificate_path", "active_pointer_path",
    }
    assert all((tmp_path / published[key]).exists() for key in (
        "registry_path", "event_path", "certificate_path", "active_pointer_path",
    ))
    with pytest.raises(IntegrityError, match="create-only"):
        persist_standard_only_lifecycle(root=tmp_path, prepared=prepared)


def test_prepared_standard_only_certificate_matches_deterministic_lifecycle():
    certificate = load_prepared_standard_only_certificate(root=ROOT)
    assert certificate["overall_decision"] == "PASS"
    assert len(certificate["gates"]) == 11
    assert {gate["status"] for gate in certificate["gates"]} == {"PASS"}


def test_registration_publication_rejects_cross_identity_drift(tmp_path):
    protocol, correction, synthetic, mechanics, reconciliation = _inputs()
    bound = tmp_path / "bound.txt"
    bound.write_text("bound\n", encoding="utf-8")
    prepared = build_standard_only_lifecycle_payloads(
        protocol=protocol, correction=correction, synthetic=synthetic,
        mechanics=mechanics, reconciliation=reconciliation,
        bindings={"bound.txt": sha256_file(bound)},
    )
    pointer = dict(prepared.pointer)
    pointer["trial_id"] = "0" * 64
    drifted = replace(
        prepared,
        pointer=pointer,
        pointer_id=sha256_json(pointer),
    )
    with pytest.raises(IntegrityError, match="identities"):
        persist_standard_only_lifecycle(root=tmp_path, prepared=drifted)
