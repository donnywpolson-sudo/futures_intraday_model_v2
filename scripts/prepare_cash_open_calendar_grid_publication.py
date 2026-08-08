"""Describe the grid publication; activation requires separate approval."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.cash_open_calendar_grid_publication import publication_documents


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    documents = publication_documents(root=ROOT)
    print(
        json.dumps(
            {
                "status": "PREPARED_SEPARATE_PUBLICATION_ACTIVATION_APPROVAL_REQUIRED",
                "calendar_id": documents["pointer"]["calendar_id"],
                "registration_id": documents["registration"]["registration_id"],
                "event_id": documents["event"]["event_id"],
                "pointer_id": documents["pointer"]["pointer_id"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
