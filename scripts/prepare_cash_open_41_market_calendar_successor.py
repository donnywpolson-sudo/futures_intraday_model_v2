"""Prepare, but never publish or activate, the reference-only calendar successor."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.cme_calendar_successor import build_successor


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path("state/unpublished_evidence/cash_open_impulse_41_market_calendar_successor_preparation")


def main() -> int:
    payload = build_successor(root=ROOT)
    target = ROOT / OUTPUT / str(payload["calendar_id"]) / "historical_calendar_successor.json"
    target.parent.mkdir(parents=True, exist_ok=False)
    with target.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({
        "calendar_id": payload["calendar_id"], "decision": payload["decision"],
        "output_path": target.relative_to(ROOT).as_posix(), "output_sha256": sha256_file(target),
        "row_count": len(payload["calendar_rows"]),
        "unresolved_reference_count": payload["unresolved_reference_count"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
