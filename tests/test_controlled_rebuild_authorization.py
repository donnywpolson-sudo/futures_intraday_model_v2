import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import UnauthorizedOperation
import futures_rebuild.migration as migration_module
from futures_rebuild.migration import (
    AUTHORIZED_MIGRATION_MANIFEST_SHA256,
    AUTHORIZED_MIGRATION_SOURCE_SCOPE_SHA256,
    CONTROLLED_REBUILD_AUTHORIZATION_ID,
    MIGRATION_APPROVAL_COMPLETE,
    MigrationApproval,
    migration_implementation_sha256,
    migration_source_scope,
)


def test_controlled_rebuild_authorization_is_exact_and_non_alpha() -> None:
    path = Path(__file__).resolve().parents[1] / "configs" / "controlled_rebuild_authorization.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    authorization_id = payload.pop("authorization_id")
    assert sha256_json(payload) == authorization_id == (
        "4977eb04c13f92045a3c020b9a8c9f691e21df583a81d8cc5040a13104cb8793"
    )
    assert payload["data_reuse_policy"] == {
        "blanket_redownload_allowed": False,
        "copy_mode": "HASH_VERIFIED_COPY_NOT_MOVE",
        "links_allowed": False,
        "legacy_bytes_remain_unchanged": True,
    }
    assert "real_history_hypothesis_or_wfa_execution" in payload["hard_pauses"]
    assert "paid_databento_download" in payload["hard_pauses"]
    assert "candidate_sealing" in payload["hard_pauses"]
    assert "hash_copy_approved_legacy_data" in payload["allowed_actions"]
    assert "bounded_free_alpaca_qualification" not in payload["allowed_actions"]


def test_controlled_copy_manifest_changes_no_source_or_role_boundary() -> None:
    root = Path(__file__).resolve().parents[1]
    dry = json.loads((root / "configs" / "migration_manifest.json").read_text(encoding="utf-8"))
    approved = json.loads(
        (root / "configs" / "migration_manifest_authorized.json").read_text(
            encoding="utf-8"
        )
    )
    assert dry["copy_authorized"] is False
    assert approved["copy_authorized"] is True
    assert approved["migration_id"] == "futures_v2_m1_controlled_source_copy"
    assert approved["destination_root"].endswith(r"data\vault\.staging\controlled_import")
    for key in (
        "manifest_version",
        "source_root",
        "publication_root",
        "state_root",
        "lock_path",
        "authoritative_group",
        "policy",
        "entries",
    ):
        assert approved[key] == dry[key]


def test_legacy_trial_census_evidence_boundary_is_exact_and_evidence_only() -> None:
    root = Path(__file__).resolve().parents[1]
    payload = json.loads(
        (root / "configs" / "migration_manifest_authorized.json").read_text(
            encoding="utf-8"
        )
    )
    expected_sources = {
        "docs/opening_range_acceptance_continuation_30m_v1_failure_analysis.md",
        "docs/opening_range_acceptance_continuation_30m_v1_failure_autopsy.md",
        "manifests/feature_hypotheses/registry.json",
        "manifests/feature_hypotheses/trial_statuses.jsonl",
        "manifests/target_hypotheses/registry.json",
        "manifests/target_hypotheses/trial_statuses.jsonl",
        "reports/experiments/ledger.jsonl",
        "reports/master_audit/master_audit_canonical_trial_search_append_only_mutation_execution_20260710/master_audit_canonical_trial_search_append_only_mutation_execution.json",
        "reports/master_audit/master_audit_canonical_trial_search_append_only_mutation_package_20260710/master_audit_canonical_trial_search_append_only_mutation_package.json",
        "reports/master_audit/master_audit_post_mutation_trial_search_completeness_reconciliation_20260710/master_audit_post_mutation_trial_search_completeness_reconciliation.json",
        "reports/master_audit/master_audit_post_recompute_statistical_validity_reconciliation_20260710/master_audit_post_recompute_statistical_validity_reconciliation.json",
        "reports/master_audit/master_audit_statistical_validity_closeout_boundary_decision_20260710/master_audit_statistical_validity_closeout_boundary_decision.json",
        "reports/master_audit/master_audit_trial_ledger_overfit_accounting_intake_20260710/master_audit_trial_ledger_overfit_accounting_intake.json",
        "reports/master_audit/master_audit_trial_ledger_search_path_reconciliation_20260710/master_audit_trial_ledger_search_path_reconciliation.json",
        "reports/master_audit/master_audit_trial_search_ledger_backfill_decision_20260710/master_audit_trial_search_ledger_backfill_decision.json",
        "reports/master_audit/master_audit_trial_search_ledger_schema_remediation_plan_20260710/master_audit_trial_search_ledger_schema_remediation_plan.json",
        "reports/master_audit/master_audit_unrecovered_source_family_metadata_remediation_20260710/master_audit_unrecovered_source_family_metadata_remediation.json",
        "reports/model_trust_audit/alpha_evidence_completion_closeout_20260709T035929Z/alpha_evidence_completion_closeout.json",
        "reports/model_trust_audit/alpha_evidence_gap_matrix_20260709T034109Z/alpha_evidence_gap_matrix.json",
        "reports/model_trust_audit/alpha_evidence_gap_matrix_20260709T034313Z/alpha_evidence_gap_matrix.json",
        "reports/statistical_validity/tier1_core_phase6_full_predictions_20260706/statistical_validity_summary.json",
        "reports/wfa_distributional/distributional_30m_probability_magnitude_v1_5929e9e/predictions_manifest.json",
        "reports/prediction_audit/distributional_30m_probability_magnitude_v1_5929e9e/distributional_prediction_audit.json",
        "reports/model_selection/distributional_30m_probability_magnitude_v1_5929e9e/distributional_alpha_evaluation.json",
    }
    evidence = [
        entry
        for entry in payload["entries"]
        if entry["disposition"] == "legacy_trial_census_evidence_only"
    ]
    assert {entry["source"] for entry in evidence} == expected_sources
    assert len(evidence) == 24
    assert sum(entry["expected_bytes"] for entry in evidence) == 1_515_908
    assert all(entry["kind"] == "file" for entry in evidence)
    assert all(entry["expected_files"] == 1 for entry in evidence)
    assert all(
        entry["destination"]
        == (
            "evidence/legacy_research/by_family/"
            f"{entry['family']}{Path(entry['source']).suffix}"
        )
        for entry in evidence
    )
    publication_root = Path(str(payload["publication_root"])) / ("a" * 64)
    assert max(
        len(str(publication_root / Path(entry["destination"])))
        for entry in evidence
    ) < 240


def test_authorized_migration_manifest_and_source_scope_are_exactly_pinned() -> None:
    root = Path(__file__).resolve().parents[1]
    path = root / "configs" / "migration_manifest_authorized.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert sha256_json(payload) == AUTHORIZED_MIGRATION_MANIFEST_SHA256 == (
        "57bcac635731c0bf85a0c0cad5c810cf9173b68813d501b0a94ce15a6ddacda7"
    )
    assert sha256_json(
        migration_source_scope(payload)
    ) == AUTHORIZED_MIGRATION_SOURCE_SCOPE_SHA256 == (
        "6f9c47cb65e9b2198163de8c9f069b25f8b07ca276d9fb07cccdee9dc0c371cb"
    )
    migration_module._validate_authorized_repository_manifest(
        payload, AUTHORIZED_MIGRATION_MANIFEST_SHA256
    )


def test_checked_in_migration_approval_is_exact_reviewed_hash_copy_only() -> None:
    root = Path(__file__).resolve().parents[1]
    path = root / "configs" / "migration_approval_authorized.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    artifact_id = payload.pop("artifact_id")
    assert artifact_id == sha256_json(payload) == (
        "3ec5584cd3273caa11938321c83fea0da573ae88e1db384154ad199687195584"
    )
    assert payload["status"] == MIGRATION_APPROVAL_COMPLETE
    assert payload["execution_authorized"] is True
    assert payload["required_next_step"] is None
    approval = MigrationApproval.from_dict(payload["approval"])
    assert approval.approval_id == (
        "186ee07ee57c578043dfc346a76f6ef4b5ab703958ff9f94e69b00c40b1b7abd"
    )
    assert approval.manifest_sha256 == AUTHORIZED_MIGRATION_MANIFEST_SHA256
    assert approval.inventory_sha256 == (
        "1243eacf39007042126237913fe17070d445712cbc4c1147d11fcae4bc598e96"
    )
    assert approval.migration_implementation_sha256 != (
        migration_implementation_sha256()
    )
    assert approval.total_files == 9_122
    assert approval.total_bytes == 33_313_291_079
    assert approval.user_authorization_id == CONTROLLED_REBUILD_AUTHORIZATION_ID
    with pytest.raises(UnauthorizedOperation, match="historical"):
        migration_module._load_checked_in_migration_approval()
