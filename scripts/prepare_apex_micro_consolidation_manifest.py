"""Create the exact pre-staging consolidation manifest without staging files."""

from __future__ import annotations

import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/apex_micro_phase1a_consolidation_manifest/manifest.json"
)

CATEGORIES = {
    "a_pre_existing_full_contract_and_apex_work": (
        "configs/alpha_ladder_full_contract_risk_census_plan.json",
        "configs/apex_tradovate_50k_eod_risk_policy.json",
        "reports/alpha_ladder_full_contract_risk_framework.md",
        "reports/alpha_ladder_tier0_unification.md",
        "reports/apex_tradovate_50k_eod_risk_policy.md",
        "scripts/prepare_alpha_ladder_full_contract_risk_census_plan.py",
        "scripts/run_alpha_ladder_full_contract_risk_census.py",
        "src/futures_rebuild/alpha_ladder_full_contract_risk_census.py",
        "src/futures_rebuild/apex_tradovate_eod_risk.py",
        "state/authorization_uses/74405ebf4d8dee969371d0f094d03c03009b8f44aae6678d5a9b61b2b55aa6f7.json",
        "state/unpublished_evidence/alpha_ladder_full_contract_risk_census/c82f91b564ad4772c338b2fafbf87627aeb07f61695acc31080f9ed8486b902d/checkpoint_accounting.json",
        "state/unpublished_evidence/alpha_ladder_full_contract_risk_census/c82f91b564ad4772c338b2fafbf87627aeb07f61695acc31080f9ed8486b902d/risk_feasibility_report.json",
        "tests/test_alpha_ladder_full_contract_risk_census.py",
        "tests/test_apex_tradovate_eod_risk.py",
    ),
    "b_standard_ladder_corrections": (
        "configs/active_alpha_research_ladder.json",
        "configs/alpha_tiered.yaml",
        "scripts/prepare_alpha_research_ladder.py",
        "src/futures_rebuild/alpha_ladder_full_regular_tier0.py",
        "src/futures_rebuild/alpha_ladder_reported_trade_exit_tier0.py",
        "src/futures_rebuild/alpha_research_ladder.py",
        "state/alpha_ladder_registry/53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815/alpha_tiered.yaml",
        "state/alpha_ladder_registry/53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815/universe_contract.json",
        "state/unpublished_evidence/alpha_research_ladder_preparation/53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815/alpha_tiered.yaml",
        "state/unpublished_evidence/alpha_research_ladder_preparation/53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815/es_2018_alpha_profile_preparation.yaml",
        "state/unpublished_evidence/alpha_research_ladder_preparation/53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815/invalid_preparations.json",
        "state/unpublished_evidence/alpha_research_ladder_preparation/53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815/universe_contract.json",
        "tests/conftest.py",
        "tests/test_alpha_ladder_es_pilot_execution_transition.py",
        "tests/test_alpha_ladder_full_regular_readiness.py",
        "tests/test_alpha_ladder_full_regular_readiness_v2.py",
        "tests/test_alpha_research_ladder.py",
        "tests/test_preexecution_fold_certification.py",
        "tests/test_profiles_and_pipeline.py",
    ),
    "c_superseded_micro_preparation": (
        "configs/apex_micro_tier01_databento_preflight_plan.json",
        "state/unpublished_evidence/alpha_research_architecture/architecture.json",
        "state/unpublished_evidence/apex_micro_ladder_preparation/febccafd953e8bd7323930ae7beb8d381242e5adb31300795f31e5ce092245ab/alpha_tiered.json",
        "state/unpublished_evidence/apex_micro_ladder_preparation/febccafd953e8bd7323930ae7beb8d381242e5adb31300795f31e5ce092245ab/prepared_active_pointer.json",
        "state/unpublished_evidence/apex_micro_ladder_preparation/febccafd953e8bd7323930ae7beb8d381242e5adb31300795f31e5ce092245ab/universe_contract.json",
        "state/unpublished_evidence/apex_micro_preparation_supersessions/micro_tier1_scope_reconciliation.json",
    ),
    "d_corrected_micro_architecture": (
        "configs/apex_micro_product_reference_requirements.json",
        "src/futures_rebuild/alpha_research_architecture.py",
        "src/futures_rebuild/micro_alpha_pipeline.py",
        "state/unpublished_evidence/alpha_research_architecture_v2/b13f545895ca965244fecd23fd75254f6d5632dd05d3f2866686db0e0eb06f56/architecture.json",
        "state/unpublished_evidence/apex_micro_ladder_preparation_v2/234eccff53c6620f2f54e73c88165574531f434b441ae808dd36c2f75d1927c8/alpha_tiered.json",
        "state/unpublished_evidence/apex_micro_ladder_preparation_v2/234eccff53c6620f2f54e73c88165574531f434b441ae808dd36c2f75d1927c8/prepared_active_pointer.json",
        "state/unpublished_evidence/apex_micro_ladder_preparation_v2/234eccff53c6620f2f54e73c88165574531f434b441ae808dd36c2f75d1927c8/universe_contract.json",
    ),
    "e_phase1a_acquisition_implementation": (
        "src/futures_rebuild/micro_alpha_acquisition.py",
        "src/futures_rebuild/research_gateway_policy.py",
    ),
    "f_metadata_preflight": (
        "configs/apex_micro_tier01_databento_metadata_preflight_v2.json",
        "src/futures_rebuild/micro_alpha_databento_preflight.py",
    ),
    "g_tests_and_documentation": (
        "PIPELINE_FOLDER_MAP.md",
        "PROJECT_OUTLINE.md",
        "scripts/prepare_apex_micro_consolidation_manifest.py",
        "scripts/prepare_apex_micro_infrastructure.py",
        "tests/test_micro_alpha_acquisition.py",
        "tests/test_micro_alpha_databento_preflight.py",
        "tests/test_micro_alpha_pipeline.py",
        "tests/test_operational_documents.py",
    ),
    "h_unrelated_preserved_work": (
        "CODEX_HANDOFF.md",
        "CURRENT_WORKFLOW.md",
    ),
}


def _status_paths() -> set[str]:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "status", "--porcelain=v1", "-z", "--untracked-files=all"],
        check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    records = completed.stdout.decode("utf-8", "strict").split("\0")
    output: set[str] = set()
    for record in records:
        if not record:
            continue
        path = record[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        output.add(path.replace("\\", "/"))
    return output


def _git_value(*args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), *args], check=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    return completed.stdout.strip()


def main() -> int:
    categorized = {path for paths in CATEGORIES.values() for path in paths}
    if len(categorized) != sum(len(paths) for paths in CATEGORIES.values()):
        raise RuntimeError("consolidation categories contain a duplicate path")
    observed = _status_paths() - {OUTPUT.as_posix()}
    if observed != categorized:
        missing = sorted(observed - categorized)
        stale = sorted(categorized - observed)
        raise RuntimeError(f"consolidation census drifted missing={missing} stale={stale}")
    records = {
        category: [
            {
                "path": path,
                "sha256": sha256_file(ROOT / path),
                "recommended_for_exact_stage": not category.startswith("h_"),
            }
            for path in paths
        ]
        for category, paths in CATEGORIES.items()
    }
    recommended = sorted(
        path for category, paths in CATEGORIES.items()
        if not category.startswith("h_") for path in paths
    )
    recommended.append(OUTPUT.as_posix())
    core = {
        "schema_version": "apex_micro_phase1a_consolidation_manifest/1.0.0",
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git_value("branch", "--show-current"),
        "observed_head": _git_value("rev-parse", "HEAD"),
        "category_records": records,
        "recommended_exact_stage_paths": sorted(recommended),
        "preserved_unstaged_paths": list(CATEGORIES["h_unrelated_preserved_work"]),
        "staging_performed": False,
        "commit_performed": False,
        "push_performed": False,
        "provider_access_performed": False,
        "dbn_download_performed": False,
        "historical_rows_read": False,
    }
    manifest = {**core, "manifest_id": sha256_json(core)}
    output = ROOT / OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(manifest) + b"\n"
    if output.exists():
        if output.read_bytes() != raw:
            raise RuntimeError("existing consolidation manifest differs")
    else:
        with output.open("xb") as stream:
            stream.write(raw)
    print(manifest["manifest_id"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
