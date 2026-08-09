"""Freeze a reconstruction-stable cleanup census for the v24 successor."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json

if __package__:
    from scripts.prepare_safe_cleanup_candidate_census_v8 import (
        build_census as build_v8,
    )
else:
    from prepare_safe_cleanup_candidate_census_v8 import build_census as build_v8


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/safe_cleanup_candidate_census_v9/census.json"
)
SUPERSESSION_REPORT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1a_acquisition_v23_supersession/report.json"
)
V23_PLAN = Path("configs/apex_micro_tier01_phase1a_acquisition_plan_v23.json")
V23_AUDIT = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v23/audit.json"
)
V23_CENSUS = Path(
    "state/unpublished_evidence/safe_cleanup_candidate_census_v8/census.json"
)
DECLARED_CREATE_ONLY_OUTPUT_STATUS_PATHS = frozenset(
    {
        "configs/apex_micro_tier01_phase1a_acquisition_plan_v24.json",
        "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v24/",
        "state/unpublished_evidence/safe_cleanup_candidate_census_v9/",
    }
)


def _head(root: Path) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
    ).strip()


def build_census(*, root: Path = ROOT, committed_head: str) -> dict[str, object]:
    predecessor = build_v8(root=root, committed_head=committed_head)
    core = dict(predecessor)
    core.pop("census_id", None)
    core["schema_version"] = "safe_cleanup_candidate_census/9.0.0"
    observed = list(core["worktree_paths_preserved"])
    core["worktree_paths_preserved"] = [
        path
        for path in observed
        if path not in DECLARED_CREATE_ONLY_OUTPUT_STATUS_PATHS
    ]
    core["self_referential_output_exclusion"] = {
        "exact_status_paths": sorted(DECLARED_CREATE_ONLY_OUTPUT_STATUS_PATHS),
        "applies_only_to_create_only_v24_plan_audit_and_census_outputs": True,
        "excluded_path_is_cleanup_candidate": False,
        "excluded_path_is_ignored_or_unbound": False,
        "separate_bindings_required": True,
    }
    bindings = dict(core["bindings"])
    bindings.update(
        {
            SUPERSESSION_REPORT.as_posix(): sha256_file(
                root / SUPERSESSION_REPORT
            ),
            V23_PLAN.as_posix(): sha256_file(root / V23_PLAN),
            V23_AUDIT.as_posix(): sha256_file(root / V23_AUDIT),
            V23_CENSUS.as_posix(): sha256_file(root / V23_CENSUS),
        }
    )
    core["bindings"] = dict(sorted(bindings.items()))
    core["superseded_v23_preparation"] = {
        "report_path": SUPERSESSION_REPORT.as_posix(),
        "state": "SUPERSEDED_PREPARATION_VOLATILE_CAPACITY_SNAPSHOT",
        "plan_audit_or_census_is_cleanup_candidate": False,
        "cleanup_mutation_authorized": False,
    }
    return {**core, "census_id": sha256_json(core)}


def write_census_create_only(
    *, root: Path = ROOT, committed_head: str
) -> dict[str, object]:
    census = build_census(root=root, committed_head=committed_head)
    output = root / OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as stream:
        stream.write(canonical_bytes(census) + b"\n")
    return census


def main() -> int:
    census = write_census_create_only(root=ROOT, committed_head=_head(ROOT))
    print(
        json.dumps(
            {
                "census_id": census["census_id"],
                "candidate_count": census["candidate_count"],
                "state": census["state"],
                "sha256": sha256_file(ROOT / OUTPUT),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
