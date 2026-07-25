from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.live_cockpit.approval import (
    APPROVAL_SCHEMA,
    OPERATION,
    LiveSmokeApprovalError,
    validate_live_smoke_plan,
    verify_live_smoke_approval,
)
from futures_rebuild.live_cockpit.smoke import main as smoke_main
from futures_rebuild.live_cockpit.smoke import run_smoke


ROOT = Path(__file__).resolve().parents[2]
PLAN_PATH = ROOT / "configs" / "live_cockpit_smoke_plan.json"
PENDING_PATH = ROOT / "configs" / "live_cockpit_smoke_approval.json"


def _approved_receipt(tmp_path: Path) -> Path:
    plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
    core = {
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(PLAN_PATH),
        "approved_at": "2026-07-25T00:00:00Z",
        "user_authorization_id": "1" * 64,
    }
    payload = {**core, "approval_receipt_id": sha256_json(core)}
    path = tmp_path / "approved-live-smoke.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def test_checked_in_plan_is_exactly_41_market_and_bounded() -> None:
    plan = validate_live_smoke_plan(
        json.loads(PLAN_PATH.read_text(encoding="utf-8"))
    )
    assert len(plan["scope"]["overview_markets"]) == 41
    assert plan["scope"]["focus_market"] == "ES"
    assert plan["scope"]["duration_seconds"] == 120
    assert plan["scope"]["maximum_live_sessions"] == 2
    assert plan["execution_authorized"] is False


def test_pending_receipt_blocks_before_provider_execution() -> None:
    assert (
        smoke_main(
            ["--plan", str(PLAN_PATH), "--approval", str(PENDING_PATH)],
            stdout=None,
        )
        == 2
    )


def test_exact_core_hash_receipt_is_accepted(tmp_path: Path) -> None:
    approval_path = _approved_receipt(tmp_path)
    receipt_id = verify_live_smoke_approval(
        plan_path=PLAN_PATH, approval_path=approval_path
    )
    assert receipt_id == json.loads(
        approval_path.read_text(encoding="utf-8")
    )["approval_receipt_id"]


def test_plan_market_drift_is_rejected(tmp_path: Path) -> None:
    plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
    plan["scope"]["overview_markets"][-1] = "XX"
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    plan["plan_id"] = sha256_json(core)
    with pytest.raises(LiveSmokeApprovalError, match="scope is broader"):
        validate_live_smoke_plan(plan)


def test_direct_provider_smoke_requires_verified_receipt() -> None:
    with pytest.raises(LiveSmokeApprovalError, match="requires exact"):
        run_smoke(env={"DATABENTO_API_KEY": "never-used"})


def test_receipt_cannot_survive_plan_tamper(tmp_path: Path) -> None:
    approval_path = _approved_receipt(tmp_path)
    plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
    plan["scope"]["duration_seconds"] = 121
    tampered_plan = tmp_path / "tampered-plan.json"
    tampered_plan.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    with pytest.raises(LiveSmokeApprovalError):
        verify_live_smoke_approval(
            plan_path=tampered_plan, approval_path=approval_path
        )
