from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path

from futures_rebuild.overnight_inventory_reversal_preregistration import (
    CORRECTION_PATH,
    PREREGISTRATION_PATH,
    load_overnight_inventory_reversal_preregistration,
    load_preoutcome_correction,
    prepare_corrected_registration_documents,
    prepare_registration_documents,
    registration_paths,
)


ROOT = Path(__file__).resolve().parents[1]


def test_materially_new_hypothesis_is_complete_and_outcome_locked() -> None:
    protocol = load_overnight_inventory_reversal_preregistration(root=ROOT)
    novelty = protocol["material_novelty"]
    exposure = protocol["prior_evidence_exposure"]
    authority = protocol["authority"]

    assert protocol["hypothesis_id"] == "overnight_inventory_reversal_cash_open"
    assert novelty["mechanism"] == (
        "OVERNIGHT_INVENTORY_DISPLACEMENT_MEAN_REVERSION"
    )
    assert novelty["incremental_rescue"] is False
    assert exposure["new_hypothesis_evaluation_outcomes_computed_or_examined"] is False
    assert exposure["historical_period_claimed_pristine"] is False
    assert all(value is False for value in authority.values())


def test_fixed_rule_has_no_model_search_or_incremental_rescue_surface() -> None:
    protocol = load_overnight_inventory_reversal_preregistration(root=ROOT)
    feature = protocol["feature_policy"]
    execution = protocol["target_and_execution_policy"]
    closure = protocol["closure_policy"]

    assert feature["standardized_displacement_threshold"] == "1.5"
    assert feature["threshold_search"] is False
    assert feature["feature_selection"] is False
    assert feature["additional_features"] == []
    assert execution["maximum_hold_minutes"] == 60
    assert execution["profit_target"] is False
    assert closure["v15_v16_style_rescue_trials"] is False
    assert set(closure["forbidden_incremental_successors"]) == {
        "THRESHOLD_CHANGE", "HORIZON_CHANGE", "MARKET_SUBSET_SELECTION",
        "NEW_CONFIRMATION_FILTER", "SIZING_CHANGE", "COST_CHANGE",
        "STOP_OR_EXIT_CHANGE", "METRIC_OR_BASELINE_CHANGE",
    }


def test_registered_documents_are_deterministic_and_match_immutable_records() -> None:
    registered_at = datetime(
        2026, 8, 6, 17, 9, 39, 686203, tzinfo=timezone.utc,
    )
    first = prepare_registration_documents(
        root=ROOT, registered_at_utc=registered_at,
    )
    second = prepare_registration_documents(
        root=ROOT, registered_at_utc=registered_at,
    )
    assert first == second
    registration, event = first
    assert registration["state"] == (
        "REGISTERED_BEFORE_NEW_HYPOTHESIS_OUTCOME_ACCESS"
    )
    assert registration["counted_trial_number_floor"] == 106
    assert registration["trusted_external_pre_outcome_anchor"] is False
    assert registration["evaluation_authority"] is False
    assert event["event_type"] == "DECLARED"
    assert event["evaluation"] is False
    registry_path, event_path = registration_paths(registration["trial_id"])
    assert load_json(ROOT / registry_path) == registration
    assert load_json(ROOT / event_path) == event


def test_preregistration_module_has_no_evaluator_or_data_reader_import() -> None:
    module_path = ROOT / (
        "src/futures_rebuild/overnight_inventory_reversal_preregistration.py"
    )
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
            imported.update(alias.name for alias in node.names)
    assert not any(
        fragment in name
        for name in imported
        for fragment in (
            "pyarrow", "numpy", "pandas", "active_phase3_outcomes",
            "active_phase4_features", "tier1_final_pipeline",
        )
    )
    assert (ROOT / PREREGISTRATION_PATH).is_file()


def test_preoutcome_correction_is_causal_complete_and_not_outcome_informed() -> None:
    correction = load_preoutcome_correction(root=ROOT)
    causal = correction["causal_feature_correction"]
    inference = correction["inference_clarifications"]

    assert causal["decision_at_chicago"] == "08:30:05"
    assert correction["outcome_informed"] is False
    assert correction["source_rows_opened"] is False
    assert correction["outcome_rows_opened"] is False
    assert inference["hac_lag_sessions"] == 5
    assert inference["stationary_bootstrap_resamples"] == 10000
    assert inference["stationary_bootstrap_seed"] == 20260806
    assert (ROOT / CORRECTION_PATH).is_file()


def test_corrected_registration_matches_immutable_local_records() -> None:
    registered_at = datetime(
        2026, 8, 6, 17, 27, 2, 791224, tzinfo=timezone.utc,
    )
    first = prepare_corrected_registration_documents(
        root=ROOT, registered_at_utc=registered_at,
    )
    second = prepare_corrected_registration_documents(
        root=ROOT, registered_at_utc=registered_at,
    )
    assert first == second
    registration, event = first
    assert registration["state"] == (
        "REGISTERED_CORRECTED_BEFORE_SOURCE_OR_OUTCOME_ACCESS"
    )
    assert registration["counted_trial_number_floor"] == 106
    assert registration["evaluation_authority"] is False
    assert event["event_type"] == "PRE_OUTCOME_CORRECTED_DECLARATION"
    registry_path, event_path = registration_paths(registration["trial_id"])
    assert load_json(ROOT / registry_path) == registration
    assert load_json(ROOT / event_path) == event


def load_json(path: Path) -> object:
    import json

    return json.loads(path.read_text(encoding="utf-8"))
