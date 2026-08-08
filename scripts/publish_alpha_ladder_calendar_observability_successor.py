"""Publish and activate the approved Alpha calendar-observability successor."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.alpha_ladder_calendar_observability_publication import (
    persist_publication,
)


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    print(json.dumps(persist_publication(root=ROOT), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
