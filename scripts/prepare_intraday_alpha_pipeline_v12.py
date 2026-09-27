"""Prepare the bounded ES-to-MES rescue without reading rows or activating it."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.intraday_alpha_pipeline_v12 import prepare_es_flow_screen


ROOT = Path(__file__).resolve().parents[1]


if __name__ == "__main__":
    print(json.dumps(prepare_es_flow_screen(ROOT), sort_keys=True, separators=(",", ":")))
