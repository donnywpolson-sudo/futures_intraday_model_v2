"""Create or verify the immutable Apex micro metadata preflight v20 plan."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.micro_alpha_databento_preflight_v20 import PLAN_PATH, build_plan


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    plan = build_plan(root=ROOT)
    path = ROOT / PLAN_PATH
    raw = canonical_bytes(plan) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != raw:
            raise SystemExit(f"refusing to overwrite drifted v20 plan: {path}")
    else:
        with path.open("xb") as stream:
            stream.write(raw)
    print(
        json.dumps(
            {
                "plan_id": plan["plan_id"],
                "plan_path": PLAN_PATH.as_posix(),
                "plan_sha256": sha256_file(path),
                "state": plan["state"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
