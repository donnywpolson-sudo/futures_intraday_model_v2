"""Prepare the immutable, non-authorizing ES pilot economic-execution plan."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.alpha_ladder_es_pilot_execution import (
    PLAN_PATH,
    build_plan,
    load_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import UnauthorizedOperation


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    path = root / PLAN_PATH
    if path.exists():
        raise UnauthorizedOperation("ES pilot execution plan already exists")
    plan = build_plan(root=root)
    raw = canonical_bytes(plan) + b"\n"
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, raw)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    loaded = load_plan(root=root, verify_protected=False)
    print(
        json.dumps(
            {
                "plan_id": loaded["plan_id"],
                "plan_path": PLAN_PATH.as_posix(),
                "plan_sha256": sha256_file(path),
                "state": loaded["state"],
                "historical_rows_opened": False,
                "execution_authorized": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
