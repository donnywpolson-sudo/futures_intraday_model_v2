"""Create only the immutable corrected Alpha readiness census plan."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.alpha_ladder_combined_readiness_v3 import PLAN_PATH, build_plan
from futures_rebuild.canonical import canonical_bytes, sha256_file


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    plan = build_plan(root=ROOT)
    path = ROOT / PLAN_PATH
    with path.open("xb") as stream:
        stream.write(canonical_bytes(plan) + b"\n")
        stream.flush(); os.fsync(stream.fileno())
    print(json.dumps({
        "plan_id": plan["plan_id"], "path": PLAN_PATH.as_posix(),
        "sha256": sha256_file(path),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
