from __future__ import annotations

import json
from io import StringIO
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
import futures_rebuild.live_cockpit.app as cockpit_app
import futures_rebuild.live_cockpit.smoke as cockpit_smoke
from futures_rebuild.live_cockpit.smoke import main as smoke_main
from futures_rebuild.live_cockpit.smoke import run_smoke


def _plan_path(tmp_path: Path, executable_hash: str = "a" * 64) -> Path:
    path = tmp_path / "live-smoke-plan.json"
    path.write_bytes(
        canonical_bytes(build_live_smoke_plan(executable_hash)) + b"\n"
    )
    return path


def _approved_receipt(tmp_path: Path, plan_path: Path) -> tuple[Path, Path]:
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    credential_locator = tmp_path / "credential-source.json"
    credential_locator.write_text("{}", encoding="utf-8")
    core = {
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "approved_at": "2026-07-25T00:00:00Z",
        "user_authorization_id": "1" * 64,
        "credential_locator_path": str(credential_locator.resolve()),
        "credential_locator_sha256": sha256_file(credential_locator),
    }
    payload = {**core, "approval_receipt_id": sha256_json(core)}
    path = tmp_path / "approved-live-smoke.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path, credential_locator


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


def test_prepare_only_cli_rejects_pending_execution_arguments_before_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_path = _plan_path(tmp_path)
    approval_path = _pending_receipt(tmp_path, plan_path)
    result_path = (
        tmp_path
        / "reports"
        / "live_cockpit"
        / "bounded_live_smoke_result_attempt_8.json"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cockpit_smoke,
        "run_smoke",
        lambda **_kwargs: pytest.fail("provider smoke was called"),
    )
    with pytest.raises(SystemExit) as caught:
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
    assert caught.value.code == 2
    assert not result_path.exists()


def test_prepare_only_cli_emits_confirmation_without_provider_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        cockpit_smoke,
        "run_smoke",
        lambda **_kwargs: pytest.fail("provider smoke was called"),
    )
    output = StringIO()

    assert smoke_main(["--prepare"], stdout=output) == 0

    prepared = json.loads(output.getvalue())
    assert prepared["status"] == "CONFIRMATION_REQUIRED"
    assert prepared["operation"] == "cockpit live smoke"


def test_exact_core_hash_receipt_is_accepted(tmp_path: Path) -> None:
    plan_path = _plan_path(tmp_path)
    approval_path, credential_locator = _approved_receipt(tmp_path, plan_path)
    receipt_id = verify_live_smoke_approval(
        plan_path=plan_path,
        approval_path=approval_path,
        credential_locator=credential_locator,
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


def test_approval_receipt_cannot_be_reused_with_another_locator(
    tmp_path: Path,
) -> None:
    plan_path = _plan_path(tmp_path)
    approval_path, _credential_locator = _approved_receipt(tmp_path, plan_path)
    other_locator = tmp_path / "other-credential-source.json"
    other_locator.write_text("{}", encoding="utf-8")

    with pytest.raises(LiveSmokeApprovalError, match="lacks exact hash-bound approval"):
        verify_live_smoke_approval(
            plan_path=plan_path,
            approval_path=approval_path,
            credential_locator=other_locator,
        )


def test_approval_receipt_cannot_survive_locator_drift(tmp_path: Path) -> None:
    plan_path = _plan_path(tmp_path)
    approval_path, credential_locator = _approved_receipt(tmp_path, plan_path)
    credential_locator.write_text('{"changed":true}', encoding="utf-8")

    with pytest.raises(LiveSmokeApprovalError, match="lacks exact hash-bound approval"):
        verify_live_smoke_approval(
            plan_path=plan_path,
            approval_path=approval_path,
            credential_locator=credential_locator,
        )


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
    approval_path, credential_locator = _approved_receipt(tmp_path, plan_path)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["scope"]["duration_seconds"] = 121
    tampered_plan = tmp_path / "tampered-plan.json"
    tampered_plan.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    with pytest.raises(LiveSmokeApprovalError):
        verify_live_smoke_approval(
            plan_path=tampered_plan,
            approval_path=approval_path,
            credential_locator=credential_locator,
        )


def test_prepare_only_cli_rejects_approved_execution_receipt_and_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan_path = _plan_path(tmp_path)
    approval_path, _credential_locator = _approved_receipt(tmp_path, plan_path)
    result_path = (
        tmp_path
        / "reports"
        / "live_cockpit"
        / "bounded_live_smoke_result_attempt_8.json"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cockpit_smoke,
        "run_smoke",
        lambda **_kwargs: pytest.fail("provider smoke was called"),
    )

    args = [
        "--plan",
        str(plan_path),
        "--approval",
        str(approval_path),
        "--result-output",
        str(result_path),
    ]
    with pytest.raises(SystemExit) as caught:
        smoke_main(args, stdout=None)
    assert caught.value.code == 2
    assert not result_path.exists()


def test_frozen_smoke_entrypoint_requires_all_task_paths_before_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cockpit_smoke,
        "execute_approved_smoke",
        lambda **_kwargs: pytest.fail("provider smoke was called"),
    )

    with pytest.raises(
        SystemExit,
        match="requires plan, approval, credential locator, and result paths",
    ):
        cockpit_app.main(["--run-approved-live-smoke"])


def test_frozen_smoke_entrypoint_delegates_only_complete_task_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Path]] = []
    monkeypatch.setattr(
        cockpit_smoke,
        "execute_approved_smoke",
        lambda **kwargs: calls.append(kwargs) or 3,
    )
    plan = tmp_path / "plan.json"
    approval = tmp_path / "approval.json"
    locator = tmp_path / "credential-source.json"
    result = tmp_path / "result.json"

    assert cockpit_app.main(
        [
            "--run-approved-live-smoke",
            "--smoke-plan",
            str(plan),
            "--smoke-approval",
            str(approval),
            "--smoke-credential-locator",
            str(locator),
            "--smoke-result-output",
            str(result),
        ]
    ) == 3
    assert calls == [
        {
            "plan_path": plan,
            "approval_path": approval,
            "credential_locator": locator,
            "result_output": result,
        }
    ]
