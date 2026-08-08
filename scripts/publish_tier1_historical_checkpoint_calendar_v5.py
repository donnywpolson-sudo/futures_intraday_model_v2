"""Publish the approved CME 2018-2022 V5 checkpoint-calendar successor."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.historical_checkpoint_calendar import publish_successor


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    loaded = publish_successor(root=root)
    print(
        json.dumps(
            {
                "calendar_release_id": loaded.calendar_receipt.release_id,
                "capture_release_id": loaded.capture_receipt.release_id,
                "index_release_id": loaded.index_receipt.release_id,
                "market_date_count": len(loaded.sessions),
                "status": "LOCAL_IMMUTABLE_SUCCESSOR_CREATED_NOT_A_TRIAL_PUBLICATION",
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
