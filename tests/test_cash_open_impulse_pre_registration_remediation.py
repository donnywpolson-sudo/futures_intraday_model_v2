from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from futures_rebuild.cash_open_impulse_pre_registration_remediation import (
    CORRECTED_DEPENDENT_REASON,
    FORENSIC_EVIDENCE_SHA256,
    build_checkpoint_eligible_outer_folds,
    census_active_catalog_metadata,
    correct_dependent_timing_failures,
    verify_preserved_forensic_evidence,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]


def _sessions(count: int = 1100) -> list[str]:
    result = []
    value = date(2018, 1, 2)
    while len(result) < count:
        if value.weekday() < 5:
            result.append(value.isoformat())
        value += timedelta(days=1)
    return result


def test_missing_feature_relabels_dependent_entry_failure() -> None:
    corrected = correct_dependent_timing_failures({
        "failures": [
            {"role": "FEATURE", "reason": "MISSING_MINUTE", "clock_chicago": "08:59"},
            {"role": "EXECUTION", "reason": "ENTRY_NOT_AFTER_DECISION"},
        ]
    })
    reasons = [item["reason"] for item in corrected["failures"]]
    assert "ENTRY_NOT_AFTER_DECISION" not in reasons
    assert CORRECTED_DEPENDENT_REASON in reasons
    assert corrected["dependent_timing_label_corrected"] is True


def test_genuine_entry_order_failure_is_not_erased() -> None:
    corrected = correct_dependent_timing_failures({
        "failures": [{"role": "EXECUTION", "reason": "ENTRY_NOT_AFTER_DECISION"}]
    })
    assert corrected["failures"] == [
        {"role": "EXECUTION", "reason": "ENTRY_NOT_AFTER_DECISION"}
    ]
    assert corrected["dependent_timing_label_corrected"] is False


def test_folds_use_eligible_sessions_and_retain_63_tests() -> None:
    sessions = _sessions()
    good_friday = "2021-04-02"
    if good_friday not in sessions:
        sessions.append(good_friday)
        sessions.sort()
    eligible = [item for item in sessions if item != good_friday]
    result = build_checkpoint_eligible_outer_folds(
        eligible_sessions_by_market={market: eligible for market in ("ES", "CL", "ZN", "6E")},
        required_markets=("ES", "CL", "ZN", "6E"),
    )
    assert len(result["folds"]) == 8
    assert all(item["test_session_count"] == 63 for item in result["folds"])
    assert all(good_friday not in item["embargo_sessions"] for item in result["folds"])


def test_one_market_calendar_gap_is_not_hidden_by_aggregate() -> None:
    sessions = _sessions(1010)
    with pytest.raises(IntegrityError, match="cannot support locked folds"):
        build_checkpoint_eligible_outer_folds(
            eligible_sessions_by_market={
                "ES": sessions,
                "CL": sessions,
                "ZN": sessions[:900],
                "6E": sessions,
            },
            required_markets=("ES", "CL", "ZN", "6E"),
        )


def test_catalog_inventory_terminalizes_absent_and_quarantined_pairs(
    local_evidence_root: Path,
) -> None:
    values = census_active_catalog_metadata(
        root=local_evidence_root,
        markets=("ETH", "KE", "ES"),
        years=(2018, 2019, 2021),
    )
    by_key = {(item.market, item.year): item for item in values}
    assert by_key[("ETH", 2018)].disposition == "ABSENT_FROM_ACTIVE_CATALOG"
    assert by_key[("KE", 2019)].disposition == "QUARANTINED_NOT_MATERIALIZED"
    assert by_key[("ES", 2018)].disposition == "RESOLVABLE_FROM_ACTIVE_CATALOG"


def test_completed_forensic_evidence_remains_exact() -> None:
    assert sha256_file(ROOT / (
        "state/unpublished_evidence/cash_open_impulse_dependency_forensics_v2/"
        "fc59dd719820964ffc0d270307f62588acd4f1ca51ef982a4436fbc969d5c04a/"
        "dependency_forensics.json"
    )) == FORENSIC_EVIDENCE_SHA256
    assert verify_preserved_forensic_evidence(ROOT) == (
        "b3d8f168068b9cba3c955bbee9a4167bbc2e65b143924d0dd0003ed6cd6f199b"
    )


def test_plan_validator_will_reject_execution_enabled_preparation(tmp_path) -> None:
    from futures_rebuild.cash_open_impulse_pre_registration_remediation import (
        validate_41_market_plan,
    )

    bound = tmp_path / "bound.txt"
    bound.write_text("bound", encoding="utf-8")
    core = {
        "execution_allowed": True,
        "historical_row_read_allowed": False,
        "calendar_coverage_gate": "FAIL_CLOSED_37_MARKETS_UNVERIFIED",
        "bindings": {"bound.txt": sha256_file(bound)},
    }
    plan = {**core, "plan_id": sha256_json(core)}
    path = tmp_path / "plan.json"
    path.write_bytes(canonical_bytes(plan) + b"\n")
    with pytest.raises(UnauthorizedOperation):
        validate_41_market_plan(tmp_path, Path("plan.json"))


def test_prepared_41_market_plan_is_exact_and_fail_closed(
    local_evidence_root: Path,
) -> None:
    from futures_rebuild.cash_open_impulse_pre_registration_remediation import (
        validate_41_market_plan,
    )

    plan = validate_41_market_plan(
        local_evidence_root,
        Path("configs/cash_open_impulse_41_market_source_compatibility_census_plan.json"),
    )
    assert len(plan["markets"]) == 41
    assert plan["expected_market_year_pairs"] == 205
    assert plan["source_dispositions"] == {
        "absent": 3,
        "quarantined": 4,
        "resolvable": 198,
    }
    assert len(plan["calendar_verified_markets"]) == 4
    assert len(plan["calendar_unverified_markets"]) == 37
    assert plan["execution_allowed"] is False
    assert plan["historical_row_read_allowed"] is False
    assert plan["source_resolution"]["resolver"] == (
        "futures_rebuild.active_data_view.resolve"
    )
    assert plan["source_resolution"]["fallback_allowed"] is False
