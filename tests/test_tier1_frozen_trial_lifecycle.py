from __future__ import annotations

from copy import deepcopy

import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_frozen_trial_lifecycle import (
    build_frozen_lifecycle_payloads,
)


MECHANICS_GATES = (
    "VERSION_LINEAGE_AND_FREEZE",
    "ALL_V4_THROUGH_V12_DEFECTS_HAVE_ADVERSARIAL_TESTS",
    "COMPLETE_SYNTHETIC_SOURCE_TO_TERMINAL_PIPELINE",
    "INDEPENDENT_BASELINE_UNIVERSES_SCHEDULES_COSTS_AND_ACCOUNT_PATHS",
    "PREDICTION_ELIGIBILITY_IS_OUTCOME_INDEPENDENT",
    "COST_RISK_STATISTICS_AND_PROTOCOL_ALIGNMENT",
    "EVIDENCE_RUNTIME_AND_SINGLE_USE_AUTHORIZATION",
    "HOLDOUT_2025_FAILS_CLOSED_BEFORE_OPEN",
)


def _inputs():
    protocol = {
        "state": "PREPARED_NOT_REGISTERED_SOURCE_ADEQUACY_PENDING",
        "classification": "ONE_UNVERSIONED_PREREGISTERED_TIER1_HISTORICAL_SCREEN",
        "protocol_id": "a" * 64,
        "source": {
            "selected_sources_id": "b" * 64,
            "calendar_release_id": "c" * 64,
        },
    }
    synthetic = {
        "verification_id": "d" * 64,
        "applicable_results": {"failed": 0},
    }
    mechanics = {
        "holdout_2025_touched": False,
        "gates": [{"gate": gate, "status": "PASS"} for gate in MECHANICS_GATES],
    }
    reconciliation = {"authoritative_current_trial": None}
    source = {
        "state": "PUBLISHED_SOURCE_QUALITY_ONLY",
        "record_id": "e" * 64,
        "selected_sources_id": "b" * 64,
        "calendar_release_id": "c" * 64,
        "adjudication": {
            "decision": "PASS",
            "checks": {
                "coverage": True,
                "every_feature_complete_checkpoint_has_complete_execution_path": True,
            },
        },
        "holdout_or_forward_access": False,
        "historical_evaluation": False,
    }
    return protocol, synthetic, mechanics, reconciliation, source


def test_lifecycle_prepares_one_consistent_trial_pointer_and_certificate() -> None:
    protocol, synthetic, mechanics, reconciliation, source = _inputs()
    prepared = build_frozen_lifecycle_payloads(
        protocol=protocol, synthetic=synthetic, mechanics=mechanics,
        reconciliation=reconciliation, source_adequacy=source,
        bindings={"bound": "f" * 64},
    )
    assert prepared.trial_id == sha256_json(prepared.trial)
    assert prepared.pointer_id == sha256_json(prepared.pointer)
    assert prepared.certificate_id == sha256_json(prepared.certificate)
    assert prepared.pointer["trial_id"] == prepared.trial_id
    assert prepared.certificate["trial_id"] == prepared.trial_id
    assert prepared.certificate["active_pointer_id"] == prepared.pointer_id
    assert prepared.certificate["overall_decision"] == "PASS"
    assert prepared.trial["source_row_access"] is False


@pytest.mark.parametrize("fault", ["source_fail", "execution_gap", "existing_pointer"])
def test_lifecycle_fails_before_publication_on_any_unresolved_gate(fault: str) -> None:
    protocol, synthetic, mechanics, reconciliation, source = _inputs()
    source = deepcopy(source)
    if fault == "source_fail":
        source["adjudication"]["decision"] = "FAIL"
    elif fault == "execution_gap":
        source["adjudication"]["checks"][
            "every_feature_complete_checkpoint_has_complete_execution_path"
        ] = False
    else:
        reconciliation["authoritative_current_trial"] = "old"
    with pytest.raises(IntegrityError, match="before every gate"):
        build_frozen_lifecycle_payloads(
            protocol=protocol, synthetic=synthetic, mechanics=mechanics,
            reconciliation=reconciliation, source_adequacy=source,
            bindings={"bound": "f" * 64},
        )
