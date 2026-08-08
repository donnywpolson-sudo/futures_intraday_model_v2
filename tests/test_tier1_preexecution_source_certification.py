from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.canonical import sha256_json
from futures_rebuild.tier1_preexecution_source_certification import (
    SELECTION_RULE,
    load_source_certification_plan,
    select_and_certify_sources,
)


ROOT = Path(__file__).resolve().parents[1]


def _candidate(seed: int, *, expected: int = 10, feature: int = 10, execution: int = 10, both: int = 10) -> dict[str, object]:
    return {
        "release_id": f"{seed:064x}",
        "payload_sha256": f"{seed + 1000:064x}",
        "dependency_windows": {
            "expected_open_checkpoints": expected,
            "missing_source_sessions": 0,
            "ambiguous_source_sessions": 0,
            "complete_feature_windows": feature,
            "incomplete_feature_windows": expected - feature,
            "complete_execution_windows": execution,
            "incomplete_execution_windows": expected - execution,
            "complete_both_windows": both,
        },
        "source_integrity": {"total_rows": 100},
    }


def _candidates() -> dict[str, list[dict[str, object]]]:
    result: dict[str, list[dict[str, object]]] = {}
    seed = 1
    for market in ("6E", "CL", "ES", "ZN"):
        for year in range(2018, 2023):
            key = f"{market}/{year}"
            size = 4 if key == "6E/2018" else 3
            values = [_candidate(seed + offset) for offset in range(size)]
            seed += size
            result[key] = values
    return result


def test_complete_selected_sources_pass_every_dependency_gate() -> None:
    result = select_and_certify_sources(_candidates())
    assert result["decision"] == "PASS"
    assert result["selection_rule"] == SELECTION_RULE
    assert result["candidate_release_count_assessed"] == 61
    assert all(item["status"] == "PASS" for item in result["market_year_gates"].values())


def test_real_operation_plan_is_hash_bound_and_forbids_research_actions(
    local_evidence_root: Path,
) -> None:
    plan = load_source_certification_plan(root=local_evidence_root)
    assert plan["candidate_release_count"] == 61
    assert plan["maximum_host_runtime_seconds"] == 900
    assert set(plan["forbidden_actions"].values()) == {True}


def test_one_missing_selected_execution_window_fails_whole_certificate() -> None:
    candidates = _candidates()
    for item in candidates["ZN/2018"]:
        item["dependency_windows"].update({
            "complete_execution_windows": 9,
            "incomplete_execution_windows": 1,
            "complete_both_windows": 9,
        })
    result = select_and_certify_sources(candidates)
    assert result["decision"] == "FAIL"
    assert result["market_year_gates"]["ZN/2018"]["checks"]["all_execution_windows_complete"] is False


def test_ranking_is_coverage_only_and_scope_must_be_complete() -> None:
    candidates = _candidates()
    weaker = deepcopy(candidates["ES/2022"][0])
    weaker["release_id"] = "f" * 64
    weaker["dependency_windows"].update({
        "complete_feature_windows": 9,
        "incomplete_feature_windows": 1,
        "complete_execution_windows": 9,
        "incomplete_execution_windows": 1,
        "complete_both_windows": 9,
    })
    candidates["ES/2022"].append(weaker)
    with pytest.raises(IntegrityError, match="all 61"):
        select_and_certify_sources(candidates)
    del candidates["ZN/2022"]
    with pytest.raises(IntegrityError, match="scope is incomplete"):
        select_and_certify_sources(candidates)


def test_published_source_certificate_is_canonical_and_research_free() -> None:
    path = ROOT / (
        "state/source_quality/tier1_preexecution_source_certification/"
        "7a7db45fb4e1a2e3825969e99781fd6f0d02b4dad7a7376b3f0163a0bb41cda5.json"
    )
    payload = __import__("json").loads(path.read_text(encoding="utf-8"))
    record_id = payload.pop("record_id")
    payload["state"] = "PREPARED_CREATE_ONLY"
    assert sha256_json(payload) == record_id
    assert record_id == path.stem
    assert payload["certification"]["candidate_release_count_assessed"] == 61
    assert len(payload["certification"]["selected"]) == 20
    assert payload["certification"]["decision"] == "FAIL"
    assert payload["model_fit"] is False
    assert payload["prediction_generation"] is False
    assert payload["historical_evaluation"] is False
    assert payload["holdout_or_forward_access"] is False
