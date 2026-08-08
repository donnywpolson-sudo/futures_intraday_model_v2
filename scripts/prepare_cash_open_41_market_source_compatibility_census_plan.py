"""Prepare the executable census plan only after the grid calendar is active."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.cash_open_source_compatibility_census import PLAN_PATH, build_census_plan


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    plan = build_census_plan(root=ROOT)
    path = ROOT / PLAN_PATH
    with path.open("xb") as stream:
        stream.write(canonical_bytes(plan) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({"plan_id": plan["plan_id"], "path": PLAN_PATH.as_posix(), "sha256": sha256_file(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
