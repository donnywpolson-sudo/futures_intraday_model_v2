"""Prepare, but never publish or activate, the four-checkpoint calendar."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.cash_open_calendar_grid_successor import build_grid_successor


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = Path(
    "state/unpublished_evidence/cash_open_impulse_41_market_calendar_grid_successor"
)


def main() -> int:
    payload = build_grid_successor(root=ROOT)
    target = OUTPUT_ROOT / str(payload["calendar_id"]) / "historical_calendar_successor.json"
    path = ROOT / target
    path.parent.mkdir(parents=True, exist_ok=False)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    print(
        json.dumps(
            {
                "calendar_id": payload["calendar_id"],
                "decision": payload["decision"],
                "output_path": target.as_posix(),
                "output_sha256": sha256_file(path),
                "row_count": len(payload["calendar_rows"]),
                "unresolved_reference_count": payload["unresolved_reference_count"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
