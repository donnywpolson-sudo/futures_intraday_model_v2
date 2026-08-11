from pathlib import Path
from types import SimpleNamespace
import inspect

from tests.conftest import (
    LEGACY_RESEARCH_TEST_FILES,
    LEGACY_RESEARCH_TEST_NODES,
    LOCAL_EVIDENCE_TEST_FILES,
    LOCAL_EVIDENCE_TEST_NODES,
    _lane_for,
    pytest_collection_modifyitems,
)


def test_all_collected_tests_have_one_workflow_lane(pytestconfig) -> None:
    """Lane selection is deterministic and leaves no historic test in default work."""

    root = Path(pytestconfig.rootpath)
    test_paths = sorted((root / "tests").rglob("test_*.py"))
    assert test_paths
    for path in test_paths:
        relative = path.relative_to(root).as_posix()
        assert relative.startswith("tests/")


def test_default_lane_is_current(pytestconfig) -> None:
    assert pytestconfig.option.markexpr == "current"


def test_collection_preserves_explicit_lane_markers_and_uses_effective_lane_filter() -> None:
    source = inspect.getsource(pytest_collection_modifyitems)
    assert "own_markers[:]" not in source
    assert "_effective_workflow_lane" in source
    assert '"local_evidence": 3' in source


def test_superseded_research_snapshots_are_explicit_not_pattern_accidents() -> None:
    assert {
        "test_tier1_authoritative_certified_lifecycle.py",
        "test_tier1_final_decision_validity.py",
        "test_tier1_bracket_v4.py",
        "test_trial_bundle_inference.py",
    } <= LEGACY_RESEARCH_TEST_FILES
    assert (
        "tests/test_overnight_inventory_reversal_preexecution_census_v2.py::"
        "test_parallel_successor_plan_is_hash_bound_and_preserves_consumed_attempt"
    ) in LEGACY_RESEARCH_TEST_NODES
    assert "test_certified_research_gateway.py" not in LEGACY_RESEARCH_TEST_FILES


def test_machine_local_evidence_has_an_explicit_fail_closed_lane() -> None:
    assert "test_tier1_economics_only.py" in LOCAL_EVIDENCE_TEST_FILES
    assert len(LOCAL_EVIDENCE_TEST_NODES) == 28
    assert (
        "tests/test_phase8_economics_index.py::"
        "test_live_foundation_selection_is_explicit_and_complete"
    ) in LOCAL_EVIDENCE_TEST_NODES
    assert all(node.startswith("tests/") for node in LOCAL_EVIDENCE_TEST_NODES)
    assert (
        "tests/test_generic_naming_policy.py::"
        "test_legacy_lineage_bindings_still_match_exact_bytes"
    ) in LOCAL_EVIDENCE_TEST_NODES
    assert (
        "tests/test_operational_documents.py::"
        "test_handoff_describes_the_active_alpha_ladder_and_next_boundary"
    ) in LOCAL_EVIDENCE_TEST_NODES


def test_clean_export_routes_ignored_and_exact_evidence_out_of_source_safe_lanes(
    tmp_path: Path,
) -> None:
    ignored = tmp_path / "tests" / "test_tier1_economics_only.py"
    exact = tmp_path / "tests" / "test_phase8_economics_index.py"
    mechanics = tmp_path / "tests" / "test_phase8_economics_index.py"
    assert _lane_for(
        SimpleNamespace(
            path=ignored,
            nodeid="tests/test_tier1_economics_only.py::test_ignored_economics",
        )
    ) == "local_evidence"
    assert _lane_for(
        SimpleNamespace(
            path=exact,
            nodeid=(
                "tests/test_phase8_economics_index.py::"
                "test_live_foundation_selection_is_explicit_and_complete"
            ),
        )
    ) == "local_evidence"
    assert _lane_for(
        SimpleNamespace(
            path=mechanics,
            nodeid=(
                "tests/test_phase8_economics_index.py::"
                "test_selection_rejects_duplicate_causal_receipt"
            ),
        )
    ) == "high_risk"
