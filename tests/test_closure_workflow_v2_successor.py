from __future__ import annotations

import hashlib
import json
from pathlib import Path

from futures_rebuild.canonical import sha256_json


REPO = Path(__file__).resolve().parents[1]
PLAN_ID = "bf33315d37e1cb040b33db475a110036739a1fd1726d15fc8bf106d576fe024b"
PLAN_SHA256 = "9085c75dfb7288500261c702623451ab43224285ae28df6f35adeefd6996caad"
PLAN_PATH = REPO / "manifests" / "workflow" / "closure" / "plans" / f"{PLAN_ID}.json"
APPROVAL_PATH = REPO / "manifests" / "workflow" / "closure" / "approvals" / f"{PLAN_ID}.json"


def test_historic_closure_plan_and_approval_remain_immutable_evidence() -> None:
    plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
    approval = json.loads(APPROVAL_PATH.read_text(encoding="utf-8"))
    assert plan["plan_id"] == PLAN_ID
    assert sha256_json({key: value for key, value in plan.items() if key != "plan_id"}) == PLAN_ID
    assert hashlib.sha256(PLAN_PATH.read_bytes()).hexdigest() == PLAN_SHA256
    assert approval["plan_sha256"] == PLAN_SHA256
    assert approval["status"] == "WAITING_FOR_EXACT_USER_APPROVAL"
