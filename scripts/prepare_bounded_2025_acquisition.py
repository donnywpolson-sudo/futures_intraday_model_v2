"""Prepare the bounded-2025 acquisition plan without provider or DBN access."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from futures_rebuild.bounded_2025_acquisition import PLAN_PATH, build_acquisition_plan
from futures_rebuild.canonical import canonical_bytes, sha256_file


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    output = (args.output or (root / PLAN_PATH)).resolve()
    plan = build_acquisition_plan(root=root)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(canonical_bytes(plan) + b"\n")
    print(
        json.dumps(
            {
                "plan_id": plan["plan_id"],
                "plan_sha256": sha256_file(output),
                "path": str(output),
                "requests": plan["counts"]["requests"],
                "maximum_parallel_downloads": plan["worker_contract"][
                    "maximum_parallel_downloads"
                ],
                "provider_calls": 0,
                "dbn_payload_files_opened": 0,
                "status": "PREPARED_IMPLEMENTATION_COMMIT_AND_NETWORK_APPROVAL_REQUIRED",
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
