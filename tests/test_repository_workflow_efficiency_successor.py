from __future__ import annotations

import json
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
PLAN_ID = "48bf1b39919df0a5566658076f3fd3eda464184440cb46da8e610d27462e39f0"
PLAN_PATH = REPO / "manifests" / "workflow" / "closure" / "plans" / f"{PLAN_ID}.json"
SUPERSESSION_PATH = REPO / "manifests" / "workflow" / "closure" / "supersessions" / "4b5e111f0e872e1d607b252eff55494b5e1a40b9e3f7ac5c41231bd240ef4e4b.json"


def test_historic_snapshot_and_supersession_evidence_remain_readable() -> None:
    plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
    supersession = json.loads(SUPERSESSION_PATH.read_text(encoding="utf-8"))
    assert plan["plan_id"] == PLAN_ID
    assert plan["snapshot_reused"] is True
    assert supersession["predecessor"]["approval_consumed"] is False
    assert supersession["successor"]["plan_id"] == PLAN_ID
