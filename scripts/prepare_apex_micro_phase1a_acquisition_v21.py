"""Create the exact v21 annual acquisition plan and source-safe audit."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import sha256_file
from futures_rebuild.micro_alpha_acquisition_v21 import (
    AUDIT_PATH,
    PLAN_PATH,
    write_acquisition_plan_create_only,
    write_plan_audit_create_only,
)
if __package__:
    from scripts.audit_standard_data_topology_source_safe import build_report
    from scripts.prepare_safe_cleanup_candidate_census_v6 import (
        OUTPUT as CLEANUP_CENSUS_PATH,
        write_census_create_only,
    )
else:
    from audit_standard_data_topology_source_safe import build_report
    from prepare_safe_cleanup_candidate_census_v6 import (
        OUTPUT as CLEANUP_CENSUS_PATH,
        write_census_create_only,
    )


ROOT = Path(__file__).resolve().parents[1]


def _head() -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def main() -> int:
    if any(
        (ROOT / path).exists()
        for path in (CLEANUP_CENSUS_PATH, PLAN_PATH, AUDIT_PATH)
    ):
        raise RuntimeError("v21 cleanup census, acquisition plan, or audit output exists")
    head = _head()
    fresh_topology = build_report(root=ROOT)
    cleanup_census = write_census_create_only(root=ROOT, committed_head=head)
    plan = write_acquisition_plan_create_only(root=ROOT, committed_head=head)
    audit = write_plan_audit_create_only(
        root=ROOT,
        fresh_standard_topology_report=fresh_topology,
        fresh_cleanup_census=cleanup_census,
    )
    print(
        json.dumps(
            {
                "plan_id": plan["plan_id"],
                "plan_sha256": sha256_file(ROOT / PLAN_PATH),
                "audit_id": audit["audit_id"],
                "audit_sha256": sha256_file(ROOT / AUDIT_PATH),
                "cleanup_census_id": cleanup_census["census_id"],
                "cleanup_candidate_count": cleanup_census["candidate_count"],
                "request_count": plan["limits"]["exact_request_count"],
                "state": audit["state"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
