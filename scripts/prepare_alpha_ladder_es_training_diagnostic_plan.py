"""Prepare only the hash-bound ES training diagnostic plan."""

import json
import os
from pathlib import Path
from futures_rebuild.alpha_ladder_es_training_diagnostic import PLAN_PATH, build_plan, load_plan
from futures_rebuild.canonical import canonical_bytes, sha256_file

ROOT = Path(__file__).resolve().parents[1]
if __name__ == "__main__":
    path = ROOT / PLAN_PATH
    if path.exists():
        raise FileExistsError("ES diagnostic plan already exists")
    payload = build_plan(root=ROOT)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n"); stream.flush(); os.fsync(stream.fileno())
    loaded = load_plan(root=ROOT)
    print(json.dumps({"plan_id": loaded["plan_id"], "path": PLAN_PATH.as_posix(),
                      "sha256": sha256_file(path), "state": loaded["state"]}, sort_keys=True))
