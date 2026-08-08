from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs" / "research_readiness_contract.json"
DOC_PATH = ROOT / "docs" / "HISTORICAL_RESEARCH_HARNESS.md"


def _contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def test_controlled_rebuild_authority_and_repo_independence_are_explicit() -> None:
    contract = _contract()
    authorization = contract["authorization"]
    assert authorization["this_contract_grants_execution_authority"] is False
    assert set(authorization["current_controlled_rebuild_receipt_allows"]) == {
        "approved_hash_copy",
        "non_alpha_data_validation",
        "synthetic_fixture_model_fitting",
        "synthetic_fixture_wfa",
    }
    assert set(authorization["hard_pauses_requiring_new_user_authorization"]) == {
        "paid_databento_download",
        "real_history_hypothesis_or_wfa_execution",
        "candidate_sealing",
        "destructive_cutover",
        "external_push",
        "trading",
        "legacy_repository_write",
    }
    assert authorization["legacy_repositories_read_only"] is True
    assert authorization["user_approval_receipt_required"] is True
    security = authorization["user_approval_security"]
    assert security["windows_signing_key_required"] is False
    assert security["cryptographic_signature_required"] is False
    assert security["approval_line_stored"] is False
    assert security["approval_line_sha256_stored"] is True
    assert security["exact_plan_scope_and_single_use_required"] is True
    assert authorization["migration_approval_is_candidate_or_history_authorization"] is False
    independence = contract["independence"]
    assert independence["shared_mutable_data_paths"] is False
    assert independence["shared_trial_artifact_or_state_paths"] is False
    assert independence["no_cross_import_test_required"] is True
    assert independence["no_cross_write_test_required"] is True


def test_trial_firewall_holdout_and_source_roles_fail_closed() -> None:
    contract = _contract()
    ledger = contract["trial_ledger"]
    assert ledger["append_contract"]["whole_chain_rewrite_allowed"] is False
    assert ledger["pre_outcome_anchor"]["required"] is True
    assert ledger["legacy_trial_census"]["unresolved_status"] == (
        "INVALID_TRIAL_CENSUS_UNRESOLVED"
    )
    assert ledger["legacy_trial_census"]["exact_resolution_claim_allowed_for_current_legacy_evidence"] is False
    assert ledger["legacy_trial_census"]["executable_conservative_status"] == (
        "CONSERVATIVE_PENALTY_PREREGISTERED"
    )
    assert ledger["family_assignment_must_predate_outcome_access"] is True
    holdout = contract["nested_wfa"]["final_holdout"]
    assert holdout["outer_oos_role"] == "screen_only"
    assert holdout["one_time_access"] is True
    assert holdout["pooled_for_selection"] is False
    assert holdout["retune_rescue_retry_or_reuse_allowed"] is False
    assert holdout["pass_authorizes_candidate_sealing"] is False
    sources = contract["source_roles"]
    assert sources["authoritative_history"] == "approved_hashed_local_dbn_vault"
    assert sources["legacy_raw"] == "comparison_only"
    assert sources["legacy_causally_gated_normalized"] == "comparison_only"
    assert sources["comparison_sources_may_be_promoted"] is False


def test_chronology_multiplicity_pbo_and_mees_gate_are_binding() -> None:
    contract = _contract()
    assert contract["sample_contract"]["required_time_order"] == (
        "feature_window_start <= feature_available_at <= decision_at "
        "< intended_entry_at = label_start_at < label_end_at = intended_exit_at"
    )
    sample = contract["sample_contract"]
    assert sample["ohlcv_provider_ts_recv_exists"] is False
    assert sample["continuous_selection_rule"] == "V_PREVIOUS_DAY_VOLUME_RANK_0"
    assert sample["contract_segment_key_includes_utc_or_session_date"] is False
    assert sample["missing_outcomes_remain_in_coverage_denominator"] is True
    multiple = contract["multiple_testing"]
    assert "every_negative_control" in multiple["family_scope"]
    assert multiple["outcome_informed_metric_substitution_allowed"] is False
    pbo = multiple["pbo"]
    assert pbo["tie_rule"] == "deterministic_midrank"
    assert pbo["not_applicable_receives_positive_credit"] is False
    assert pbo["diagnostic_may_replace_chronological_wfa"] is False
    assert contract["power"]["design_alternative_strictly_greater_than_mees_required"]
    gate = contract["binding_gate"]
    assert gate["decision_order"][-1] == "PASS_HISTORICAL_SCREEN"
    screen = gate["PASS_HISTORICAL_SCREEN"]
    assert screen["multiplicity_adjusted_one_sided_95pct_lower_bound_gt_mees"]
    assert screen["candidate_sealed"] is False


def test_document_does_not_convert_a_screen_into_authority() -> None:
    text = DOC_PATH.read_text(encoding="utf-8")
    assert "A historical pass does not seal a candidate." in text
    assert "all writes to the legacy repository remain hard-paused" in text
    assert "Outer OOS is a screen, not the final holdout." in text
