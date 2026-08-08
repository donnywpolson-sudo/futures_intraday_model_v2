"""Prepare only the fully adapter-tested V4 readiness census plan."""

import json
import os
from pathlib import Path

from futures_rebuild.alpha_ladder_limit_readiness_v4 import PLAN_PATH, build_plan, load_plan
from futures_rebuild.canonical import canonical_bytes, sha256_file

ROOT = Path(__file__).resolve().parents[1]
if __name__ == "__main__":
    path = ROOT / PLAN_PATH
    if path.exists():
        raise FileExistsError("limit-readiness V4 plan already exists")
    payload = build_plan(root=ROOT)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n"); stream.flush(); os.fsync(stream.fileno())
    loaded = load_plan(root=ROOT)
    print(json.dumps({"plan_id": loaded["plan_id"], "path": PLAN_PATH.as_posix(),
                      "sha256": sha256_file(path), "state": loaded["state"]}, sort_keys=True))
