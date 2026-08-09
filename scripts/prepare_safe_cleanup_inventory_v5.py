"""Prepare a stable no-mutation cleanup policy and deferred-census boundary."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path("state/unpublished_evidence/safe_cleanup_preparation_v5/plan.json")
PREDECESSOR_OUTPUT = Path(
    "state/unpublished_evidence/safe_cleanup_preparation_v4/plan.json"
)
PREDECESSOR_PLAN_ID = (
    "811e3d6d9eb35cf24e49a9e215f6d4550cdfb69792a46bf3cf5cb52244c48b72"
)
PREDECESSOR_PLAN_SHA256 = (
    "2e488ad54e5e9300918918cb83f99512e768d8537f8cb2beb7337e61c0f81b84"
)
PROTECTED_PATHS = (
    "configs/active_alpha_research_ladder.json",
    "data/active/catalog.json",
    "data/active/causally_gated_normalized",
    "data/dbn",
    "data/raw",
    "data/causally_gated_normalized",
    "data/vault",
    "manifests",
    "state/authorization_uses",
    "state/unpublished_evidence",
)
MANUAL_REVIEW_PATHS = (
    "data/vault/.staging",
    (
        "data/vault/source_snapshots/"
        "6dc18d3104e37cb1bd65e5387b7a7a92f851d0e3ea571baa3157349872f5a872/"
        "comparison_only"
    ),
    (
        "data/vault/source_snapshots/"
        "6dc18d3104e37cb1bd65e5387b7a7a92f851d0e3ea571baa3157349872f5a872/"
        "evidence/legacy_research"
    ),
)


def _git_value(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def _load_predecessor(root: Path) -> dict[str, object]:
    path = root / PREDECESSOR_OUTPUT
    if sha256_file(path) != PREDECESSOR_PLAN_SHA256:
        raise IntegrityError("cleanup v4 preparation was not preserved byte-for-byte")
    try:
        plan = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("cleanup v4 preparation is invalid") from exc
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    if (
        plan_id != PREDECESSOR_PLAN_ID
        or plan_id != sha256_json(core)
        or plan.get("cleanup_execution", {}).get("performed") is not False
        or plan.get("observed_head")
        != "558ee0943a06a89699a888d35f329bbdc17099fc"
    ):
        raise IntegrityError("cleanup v4 preparation identity drifted")
    return plan


def build_plan(*, root: Path = ROOT) -> dict[str, object]:
    root = root.resolve(strict=True)
    predecessor = _load_predecessor(root)
    topology_report = root / (
        "state/unpublished_evidence/standard_data_topology_source_safe_audit/"
        "report.json"
    )
    if not topology_report.is_file():
        raise IntegrityError("source-safe standard topology report is absent")
    core: dict[str, object] = {
        "schema_version": "safe_data_project_cleanup_preparation/5.0.0",
        "state": "PREPARED_NO_MUTATION_EXACT_CLEANUP_CENSUS_AND_APPROVAL_REQUIRED",
        "repository_root": str(root),
        "branch": _git_value(root, "branch", "--show-current"),
        "head_binding": {
            "prepared_head_recorded": False,
            "reason": (
                "ONGOING_AUTHORIZED_COMMITS_MUST_NOT_SELF_INVALIDATE_PREPARE_ONLY_POLICY"
            ),
            "exact_execution_head_required_after_candidate_census": True,
        },
        "superseded_predecessor": {
            "path": PREDECESSOR_OUTPUT.as_posix(),
            "plan_id": PREDECESSOR_PLAN_ID,
            "sha256": PREDECESSOR_PLAN_SHA256,
            "observed_head": predecessor["observed_head"],
            "reason": "DYNAMIC_PREPARED_HEAD_BECAME_STALE_AFTER_APPROVED_COMMIT",
            "execution_forbidden": True,
        },
        "control_rationale": {
            "concrete_risk_prevented": (
                "DELETING_ACTIVE_CATALOG_FILES_OR_PINNED_RELEASE_HISTORY_BREAKS_"
                "PROVENANCE_HASHES_AND_RESEARCH_GATE_CLOSURE"
            ),
            "decision_improved": (
                "EXACTLY_WHICH_PATHS_MUST_BE_PRESERVED_REVIEWED_OR_MAY_BE_CLEANED"
            ),
            "why_simple_name_rules_are_insufficient": (
                "ACTIVE_FLAT_VIEWS_CONTENT_ADDRESSED_HISTORY_STAGING_SNAPSHOTS_AND_"
                "REGENERABLE_CACHES_HAVE_SIMILAR_OR_GENERIC_FOLDER_NAMES"
            ),
        },
        "authoritative_resolution": {
            "standard_lane": "data/active/catalog.json",
            "standard_active_root": "data/active/causally_gated_normalized",
            "phase2_release_history_root": "data/causally_gated_normalized",
            "micro_lane": "NO_ACTIVE_POINTER_OR_CATALOG",
            "directory_presence_alone_grants_research_use": False,
        },
        "protected_paths": [
            {
                "path": path,
                "classification": (
                    "AUTHORITATIVE_ACTIVE_CATALOG_SELECTED_VIEW"
                    if path.startswith("data/active")
                    or path == "configs/active_alpha_research_ladder.json"
                    else "IMMUTABLE_PROVENANCE_OR_RELEASE_HISTORY"
                ),
                "proposed_action": "PRESERVE_NO_MOVE_DELETE_OR_RENAME",
                "inventory": "DEFERRED_TO_EXACT_PRE_CLEANUP_CENSUS",
            }
            for path in PROTECTED_PATHS
        ],
        "manual_review_preserve_paths": [
            {
                "path": path,
                "classification": "UNRESOLVED_OR_SNAPSHOT_EVIDENCE_PRESERVE",
                "proposed_action": "NO_ACTION_UNTIL_DEPENDENCY_CLOSURE_PROVEN",
                "inventory": "DEFERRED_TO_EXACT_PRE_CLEANUP_CENSUS",
            }
            for path in MANUAL_REVIEW_PATHS
        ],
        "candidate_policy": {
            "frozen_candidates": [],
            "candidate_count": 0,
            "data_path_candidates_allowed": False,
            "only_regenerable_project_caches_may_be_considered": True,
            "candidate_roots": [
                ".pytest_cache",
                ".pytest_tmp",
                "scripts/**/__pycache__",
                "src/**/__pycache__",
                "tests/**/__pycache__",
                "manifests/workflow/**/__pycache__",
            ],
            "exact_literal_paths_must_be_frozen_after_all_prior_writes": True,
        },
        "worktree_preservation_policy": {
            "all_modified_untracked_and_staged_paths_preserved_by_default": True,
            "exact_census_required_immediately_before_cleanup": True,
            "cleanup_preparation_does_not_preapprove_any_worktree_path": True,
        },
        "cleanup_execution": {
            "performed": False,
            "files_deleted": 0,
            "directories_deleted": 0,
            "files_moved": 0,
            "active_data_changed": False,
            "raw_data_changed": False,
            "manifests_changed": False,
        },
        "required_before_any_cleanup": [
            "PASSING_MICRO_METADATA_PREFLIGHT_OR_EXPLICIT_ABANDONMENT_DECISION",
            "EXACT_LITERAL_CANDIDATE_PATH_CENSUS_AFTER_ALL_PRIOR_WRITES",
            "PROOF_NO_CATALOG_MANIFEST_RECEIPT_PLAN_OR_SOURCE_BINDING",
            "RECOVERABLE_ARCHIVE_OR_REGENERATION_PROOF",
            "EXACT_EXECUTION_HEAD_AND_WORKTREE_CENSUS",
            "SEPARATE_EXACT_CLEANUP_APPROVAL",
            "POST_CLEANUP_CATALOG_MANIFEST_AND_TEST_REVALIDATION",
            "MICRO_ACQUISITION_DESTINATION_AND_DISK_RECENSUS",
        ],
        "recommended_sequence": [
            "SECURE_CURRENT_MICRO_V6_IMPLEMENTATION",
            "RUN_SEPARATELY_APPROVED_V6_METADATA_PREFLIGHT",
            "RECONSTRUCT_EXACT_CLEANUP_CANDIDATE_CENSUS_IF_USEFUL",
            "REQUEST_SEPARATE_EXACT_CLEANUP_APPROVAL",
            "VERIFY_NO_ACTIVE_OR_IMMUTABLE_RELEASE_MUTATION",
            "FREEZE_FINAL_MICRO_ACQUISITION_PLAN_AFTER_PATH_AND_DISK_STATE_IS_STABLE",
        ],
        "bindings": {
            "data/active/catalog.json": sha256_file(root / "data/active/catalog.json"),
            "configs/active_alpha_research_ladder.json": sha256_file(
                root / "configs/active_alpha_research_ladder.json"
            ),
            topology_report.relative_to(root).as_posix(): sha256_file(topology_report),
            PREDECESSOR_OUTPUT.as_posix(): PREDECESSOR_PLAN_SHA256,
        },
        "payload_safety": {
            "dbn_or_parquet_payload_opened": False,
            "historical_rows_read": False,
            "year_2025_or_2026_payload_opened": False,
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def main() -> int:
    plan = build_plan()
    output = ROOT / OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(plan) + b"\n"
    if output.exists():
        if output.read_bytes() != raw:
            raise RuntimeError("existing cleanup v5 preparation differs")
    else:
        with output.open("xb") as stream:
            stream.write(raw)
    print(json.dumps({"plan_id": plan["plan_id"], "state": plan["state"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
