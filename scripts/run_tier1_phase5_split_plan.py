from __future__ import annotations

from pathlib import Path

from futures_rebuild.active_phase5_splits import build_tier1_phase5_split_plan
from futures_rebuild.boundary import RepoBoundary


def main() -> int:
    print("starting Tier 1 Phase 5 split-plan scan", flush=True)
    result = build_tier1_phase5_split_plan(
        boundary=RepoBoundary(active_root=Path.cwd()),
        progress=lambda event: print(event, flush=True),
    )
    print(f"completed {result['plan_id']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
