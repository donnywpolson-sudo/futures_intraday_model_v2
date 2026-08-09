"""Prepare the immutable prelaunch-discovery-safe Apex micro preflight v9."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.micro_alpha_databento_preflight_v9 import (
    PLAN_PATH,
    PREDECESSOR_REPORT_ID,
    PREDECESSOR_REPORT_PATH,
    build_plan,
)


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    plan = build_plan(root=ROOT)
    path = ROOT / PLAN_PATH
    raw = canonical_bytes(plan) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != raw:
            raise RuntimeError(f"prepared successor differs: {path}")
    else:
        with path.open("xb") as stream:
            stream.write(raw)
    print(
        json.dumps(
            {
                "plan_id": plan["plan_id"],
                "plan_path": PLAN_PATH.as_posix(),
                "plan_sha256": sha256_file(path),
                "predecessor_report_id": PREDECESSOR_REPORT_ID,
                "predecessor_report_path": PREDECESSOR_REPORT_PATH.as_posix(),
                "state": plan["state"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
