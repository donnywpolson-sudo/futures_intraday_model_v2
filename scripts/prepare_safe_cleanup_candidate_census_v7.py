"""Freeze cleanup candidates after the preserved v21 acquisition failure."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
if __package__:
    from scripts.prepare_safe_cleanup_candidate_census_v6 import (
        build_census as build_v6,
    )
else:
    from prepare_safe_cleanup_candidate_census_v6 import build_census as build_v6


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/safe_cleanup_candidate_census_v7/census.json"
)
FAILURE_REPORT = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_v21_failure/report.json"
)
V21_PLAN = Path("configs/apex_micro_tier01_phase1a_acquisition_plan_v21.json")
V21_AUDIT = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v21/audit.json"
)


def _head(root: Path) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
    ).strip()


def build_census(*, root: Path = ROOT, committed_head: str) -> dict[str, object]:
    predecessor = build_v6(root=root, committed_head=committed_head)
    core = dict(predecessor)
    core.pop("census_id", None)
    core["schema_version"] = "safe_cleanup_candidate_census/7.0.0"
    bindings = dict(core["bindings"])
    bindings.update(
        {
            FAILURE_REPORT.as_posix(): sha256_file(root / FAILURE_REPORT),
            V21_PLAN.as_posix(): sha256_file(root / V21_PLAN),
            V21_AUDIT.as_posix(): sha256_file(root / V21_AUDIT),
        }
    )
    core["bindings"] = dict(sorted(bindings.items()))
    core["preserved_acquisition_failure"] = {
        "report_path": FAILURE_REPORT.as_posix(),
        "state": "SEALED_FAIL_CLOSED_RUNTIME_CEILING_NO_ACCEPTED_SOURCE",
        "staging_root_is_cleanup_candidate": False,
        "raw_or_sidecar_file_is_cleanup_candidate": False,
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
