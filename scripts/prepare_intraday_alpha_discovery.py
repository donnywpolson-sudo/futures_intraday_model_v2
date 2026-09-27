"""Prepare the immutable intraday research successor without activating it."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.intraday_alpha_discovery import prepare


ROOT = Path(__file__).resolve().parents[1]


if __name__ == "__main__":
    print(json.dumps(prepare(ROOT), sort_keys=True, separators=(",", ":")))
