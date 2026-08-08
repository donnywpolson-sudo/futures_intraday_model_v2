"""Create only the immutable eight-session provenance audit plan."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.alpha_ladder_missing_session_provenance import (
    PLAN_PATH,
    build_plan,
    load_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    path = ROOT / PLAN_PATH
    if path.exists():
        raise FileExistsError("missing-session provenance plan already exists")
    payload = build_plan(root=ROOT)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, canonical_bytes(payload) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    plan = load_plan(root=ROOT)
    print(
        json.dumps(
            {
                "path": PLAN_PATH.as_posix(),
                "plan_id": plan["plan_id"],
                "sha256": sha256_file(path),
                "state": plan["state"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
