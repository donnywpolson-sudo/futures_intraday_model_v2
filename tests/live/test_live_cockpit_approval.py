from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.live_cockpit.approval import (
    APPROVAL_SCHEMA,
    OPERATION,
    PREDECESSOR_ATTEMPT,
    RESULT_OUTPUT_RELATIVE,
    LiveSmokeApprovalError,
    build_live_smoke_plan,
    validate_live_smoke_plan,
    verify_live_smoke_approval,
)
import futures_rebuild.live_cockpit.smoke as cockpit_smoke
from futures_rebuild.live_cockpit.smoke import SmokeResult
from futures_rebuild.live_cockpit.smoke import main as smoke_main
from futures_rebuild.live_cockpit.smoke import run_smoke


def _plan_path(tmp_path: Path, executable_hash: str = "a" * 64) -> Path:
    path = tmp_path / "live-smoke-plan.json"
    path.write_bytes(
        canonical_bytes(build_live_smoke_plan(executable_hash)) + b"\n"
    )
    return path


def _approved_receipt(tmp_path: Path, plan_path: Path) -> Path:
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    core = {
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "approved_at": "2026-07-25T00:00:00Z",
        "user_authorization_id": "1" * 64,
    }
    payload = {**core, "approval_receipt_id": sha256_json(core)}
    path = tmp_path / "approved-live-smoke.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _pending_receipt(tmp_path: Path, plan_path: Path) -> Path:
    payload = {
        "schema_version": APPROVAL_SCHEMA,
        "status": "PENDING_APPROVAL",
        "operation": OPERATION,
        "plan_id": json.loads(plan_path.read_text(encoding="utf-8"))["plan_id"],
        "plan_sha256": None,
        "approved_at": None,
        "user_authorization_id": None,
        "approval_receipt_id": None,
    }
    path = tmp_path / "pending-live-smoke.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def test_generated_plan_is_exactly_41_market_and_bounded() -> None:
    plan = validate_live_smoke_plan(build_live_smoke_plan("a" * 64))
    assert len(plan["scope"]["overview_markets"]) == 41
    assert plan["scope"]["focus_market"] == "ES"
    assert plan["scope"]["required_focus_market_calendar_state"] == "OPEN"
    assert plan["scope"]["minimum_open_window_seconds"] == 180
    assert plan["scope"]["duration_seconds"] == 120
    assert plan["scope"]["maximum_live_sessions"] == 2
    assert plan["scope"]["runtime_frozen"] is True
    assert (
        plan["scope"]["result_output_relative"]
        == RESULT_OUTPUT_RELATIVE
    )
    assert plan["predecessor_attempt"] == PREDECESSOR_ATTEMPT
    assert plan["execution_authorized"] is False


def test_pending_receipt_blocks_before_provider_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_path = _plan_path(tmp_path)
    approval_path = _pending_receipt(tmp_path, plan_path)
    result_path = (
        tmp_path
        / "reports"
        / "live_cockpit"
        / "bounded_live_smoke_result_attempt_7.json"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cockpit_smoke,
        "run_smoke",
        lambda **_kwargs: pytest.fail("provider smoke was called"),
    )
    assert (
        smoke_main(
            [
                "--plan",
                str(plan_path),
                "--approval",
                str(approval_path),
                "--result-output",
                str(result_path),
            ],
            stdout=None,
        )
        == 2
    )
    assert not result_path.exists()


def test_source_runtime_blocks_before_provider_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_path = _plan_path(tmp_path)
    approval_path = _approved_receipt(tmp_path, plan_path)
    result_path = (
        tmp_path
        / "reports"
        / "live_cockpit"
        / "bounded_live_smoke_result_attempt_7.json"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cockpit_smoke,
        "run_smoke",
        lambda **_kwargs: pytest.fail("provider smoke was called"),
    )
    assert (
        smoke_main(
            [
                "--plan",
                str(plan_path),
                "--approval",
                str(approval_path),
                "--result-output",
                str(result_path),
            ],
            stdout=None,
        )
        == 2
    )
    assert not result_path.exists()


def test_exact_core_hash_receipt_is_accepted(tmp_path: Path) -> None:
    plan_path = _plan_path(tmp_path)
    approval_path = _approved_receipt(tmp_path, plan_path)
    receipt_id = verify_live_smoke_approval(
        plan_path=plan_path, approval_path=approval_path
    )
    assert receipt_id == json.loads(
        approval_path.read_text(encoding="utf-8")
    )["approval_receipt_id"]


def test_plan_market_drift_is_rejected(tmp_path: Path) -> None:
    plan = build_live_smoke_plan("a" * 64)
    plan["scope"]["overview_markets"][-1] = "XX"
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    plan["plan_id"] = sha256_json(core)
    with pytest.raises(LiveSmokeApprovalError, match="identity is invalid"):
        validate_live_smoke_plan(plan)


def test_plan_predecessor_drift_is_rejected() -> None:
    plan = build_live_smoke_plan("a" * 64)
    plan["predecessor_attempt"]["disposition"] = "PASS"
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    plan["plan_id"] = sha256_json(core)
    with pytest.raises(LiveSmokeApprovalError, match="identity is invalid"):
        validate_live_smoke_plan(plan)


def test_direct_provider_smoke_requires_verified_receipt() -> None:
    with pytest.raises(LiveSmokeApprovalError, match="requires exact"):
        run_smoke(env={"DATABENTO_API_KEY": "never-used"})


def test_receipt_cannot_survive_plan_tamper(tmp_path: Path) -> None:
    plan_path = _plan_path(tmp_path)
    approval_path = _approved_receipt(tmp_path, plan_path)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["scope"]["duration_seconds"] = 121
    tampered_plan = tmp_path / "tampered-plan.json"
    tampered_plan.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    with pytest.raises(LiveSmokeApprovalError):
        verify_live_smoke_approval(
            plan_path=tampered_plan, approval_path=approval_path
        )


def test_success_result_is_hash_bound_create_only_and_package_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_path = _plan_path(tmp_path)
    approval_path = _approved_receipt(tmp_path, plan_path)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    result_path = (
        tmp_path
        / "reports"
        / "live_cockpit"
        / "bounded_live_smoke_result_attempt_7.json"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cockpit_smoke,
        "_verify_package_runtime",
        lambda _plan: {
            "frozen": True,
            "executable_sha256": plan["scope"]["prepared_executable_sha256"],
        },
    )
    monkeypatch.setattr(
        cockpit_smoke,
        "run_smoke",
        lambda **_kwargs: SmokeResult(
            status="PASS",
            exit_code=0,
            summary={
                "status": "PASS",
                "reasons": [],
                "runtime": {
                    "frozen": True,
                    "executable_sha256": plan["scope"][
                        "prepared_executable_sha256"
                    ],
                },
            },
        ),
    )

    args = [
        "--plan",
        str(plan_path),
        "--approval",
        str(approval_path),
        "--result-output",
        str(result_path),
    ]
    assert smoke_main(args, stdout=None) == 0
    receipt = json.loads(result_path.read_text(encoding="utf-8"))
    core = {key: value for key, value in receipt.items() if key != "result_id"}
    assert receipt["status"] == "PASS"
    assert receipt["result_id"] == sha256_json(core)
    assert receipt["summary"]["runtime"]["frozen"] is True
    assert smoke_main(args, stdout=None) == 2
