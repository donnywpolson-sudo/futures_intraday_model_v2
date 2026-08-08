"""Prepare, but do not publish or activate, the Alpha calendar successor."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.alpha_ladder_calendar_observability_successor import (
    persist_preparation,
)
from futures_rebuild.canonical import sha256_file


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    relative = persist_preparation(root=ROOT)
    payload = json.loads((ROOT / relative).read_text(encoding="utf-8"))
    print(
        json.dumps(
            {
                "calendar_id": payload["calendar_id"],
                "calendar_sha256": sha256_file(ROOT / relative),
                "path": relative.as_posix(),
                "status": payload["status"],
                "mechanism_registered": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
