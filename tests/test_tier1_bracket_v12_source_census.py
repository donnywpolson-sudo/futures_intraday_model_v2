from __future__ import annotations

from scripts.run_v12_local_source_alternative_census import _catalog, _load_plan
import scripts.run_v12_local_source_alternative_census as census_module


def test_v12_source_census_catalog_and_selection_are_frozen(
    local_evidence_root, monkeypatch,
) -> None:
    monkeypatch.setattr(census_module, "ROOT", local_evidence_root)
    monkeypatch.setattr(
        census_module,
        "PLAN_PATH",
        local_evidence_root
        / "configs/tier1_bracket_v12_local_source_alternative_census_plan.json",
    )
    monkeypatch.setattr(
        census_module,
        "MANIFEST_ROOT",
        local_evidence_root / "manifests/data_releases/causally_gated_normalized",
    )
    plan = _load_plan()
    catalog = _catalog()
    assert len(catalog) == plan["candidate_release_count"] == 61
    pairs = {(item["market"], item["year"]) for item in catalog}
    assert pairs == {
        (market, year)
        for market in ("ES", "CL", "ZN", "6E")
        for year in range(2018, 2023)
    }
    assert all(item["year"] != 2025 for item in catalog)
    assert plan["selection_rule"] == (
        "MAXIMIZE_COMPLETE_BOTH_WINDOWS_THEN_COMPLETE_EXECUTION_WINDOWS_"
        "THEN_COMPLETE_FEATURE_WINDOWS_THEN_MINIMIZE_MISSING_SESSIONS_"
        "THEN_AMBIGUOUS_SESSIONS_THEN_LEXICOGRAPHIC_RELEASE_ID"
    )
    assert plan["forbidden_actions"]["historical_performance_evaluation"] is True
    assert plan["forbidden_actions"]["provider_or_network_access"] is True
