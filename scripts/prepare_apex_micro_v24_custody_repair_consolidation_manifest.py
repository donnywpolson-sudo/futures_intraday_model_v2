from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
for candidate in (ROOT, ROOT / "src"):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json  # noqa: E402
from futures_rebuild.micro_alpha_custody_repair_v1 import (  # noqa: E402
    FAILURE_REPORT_PATH,
)


OUTPUT = Path(
    "state/unpublished_evidence/apex_micro_v24_custody_repair_consolidation_manifest_v2/manifest.json"
)
RECOMMENDED = (
    "PIPELINE_FOLDER_MAP.md",
    "PROJECT_OUTLINE.md",
    "configs/apex_micro_tier01_phase1a_acquisition_plan_v24.json",
    "scripts/prepare_apex_micro_v24_custody_repair_consolidation_manifest.py",
    "scripts/prepare_apex_micro_v24_custody_repair_v1.py",
    "src/futures_rebuild/micro_alpha_acquisition_v24.py",
    "src/futures_rebuild/micro_alpha_custody_repair_v1.py",
    "src/futures_rebuild/research_gateway_policy.py",
    "state/authorization_uses/eaee71b9128cf8e65b8f733f359e48ec39349046809ec18c8eefecd60398f1b8.json",
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v24/audit.json",
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_v24_verification_failure/report.json",
    "state/unpublished_evidence/apex_micro_v24_custody_repair_consolidation_manifest/manifest.json",
    "state/unpublished_evidence/apex_micro_v24_custody_repair_consolidation_manifest_v2/manifest.json",
    "state/unpublished_evidence/safe_cleanup_candidate_census_v9/census.json",
    "tests/test_micro_alpha_acquisition_v21.py",
    "tests/test_micro_alpha_acquisition_v24.py",
    "tests/test_micro_alpha_custody_repair_v1.py",
)


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()


def build_manifest() -> dict[str, object]:
    failure = json.loads((ROOT / FAILURE_REPORT_PATH).read_text(encoding="utf-8"))
    output_posix = OUTPUT.as_posix()
    records = []
    for relative in RECOMMENDED:
        records.append(
            {
                "path": relative,
                "recommended_for_exact_stage": True,
                "sha256": (
                    "SELF_HASHED_AT_WRITE"
                    if relative == output_posix
                    else sha256_file(ROOT / relative)
                ),
            }
        )
    preserved = []
    for relative in ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md"):
        preserved.append(
            {
                "path": relative,
                "recommended_for_exact_stage": False,
                "sha256": sha256_file(ROOT / relative),
            }
        )
    core = {
        "schema_version": "apex_micro_v24_custody_repair_consolidation_manifest/2.0.0",
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream_head": _git("rev-parse", "origin/main"),
        "v24_execution": {
            "plan_id": failure["v24_plan_id"],
            "terminal_id": failure["v24_terminal_id"],
            "verification_failure_report_id": failure["report_id"],
            "accepted_dbn_count": 160,
            "accepted_sidecar_count": 160,
            "total_dbn_bytes": 1_849_575_228,
            "external_cost_usd": "0",
            "automatic_retries": 0,
            "authorization_consumed": True,
        },
        "repair_preparation": {
            "exact_staging_alias_removals": 320,
            "provider_calls_permitted": 0,
            "dbn_row_decode_permitted": False,
            "cleanup_mutation_performed": False,
            "repair_plan_written": False,
            "repair_plan_requires_new_committed_head": True,
            "repair_authority_present": False,
            "predecessor_manifest_id": "a290bf6b11ceedf0e9c3ddd9e4325c3c482381e2899ff37fe1e5964d1f5b2f07",
            "predecessor_manifest_state": "SUPERSEDED_BY_LIVE_DESTINATION_AWARE_TEST_UPDATE",
        },
        "recommended_exact_stage_path_count": len(RECOMMENDED),
        "recommended_exact_stage_paths": list(RECOMMENDED),
        "records": records,
        "preserved_unstaged_paths": ["CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md"],
        "preserved_records": preserved,
        "authority_and_effects": {
            "provider_access_after_v24": False,
            "repair_alias_removed": 0,
            "historical_rows_read": False,
            "year_2025_or_2026_payload_opened_for_row_access": False,
            "catalog_or_pointer_activated": False,
            "publication_registration_evaluation_or_trading": False,
            "staging_performed": False,
            "commit_performed": False,
            "push_performed": False,
        },
    }
    return {**core, "manifest_id": sha256_json(core)}


def main() -> int:
    manifest = build_manifest()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with (ROOT / OUTPUT).open("xb") as stream:
        stream.write(canonical_bytes(manifest) + b"\n")
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
