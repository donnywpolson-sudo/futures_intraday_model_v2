from __future__ import annotations

import base64

import pytest

import futures_rebuild.calendar_archived_cme_gap_discovery_recovery as module
from futures_rebuild.calendar_archived_cme_gap_discovery import _cdx_url
from futures_rebuild.calendar_archived_cme_gap_discovery_recovery import (
    APPROVAL_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    build_recovery_plan,
    execute_recovery,
    validate_recovery_approval,
    validate_recovery_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_bytes, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


def _request(ordinal: int) -> dict[str, object]:
    original = (
        "https://www.cmegroup.com/tools-information/"
        f"holiday-calendar/files/missing-{ordinal:02d}.pdf"
    )
    return {
        "accept": "application/json, text/plain",
        "cdx_url": _cdx_url(original),
        "cme_request_id": f"cme-request-{ordinal}",
        "expected_content_types": [
            "application/json",
            "text/json",
            "text/plain",
        ],
        "ordinal": ordinal,
        "original_cme_url": original,
        "request_id": f"archived-cme-gap-{ordinal:04d}",
        "source_ordinal": ordinal,
    }


def _response(ordinal: int, *, reused: bool = False) -> dict[str, object]:
    body = b"[]" if ordinal != 1 else b'[["timestamp"]]'
    return {
        "acquisition": (
            "REUSED_HASH_VERIFIED_RAW_DIAGNOSTIC"
            if reused
            else "NETWORK_CAPTURED"
        ),
        "body_base64": base64.b64encode(body).decode("ascii"),
        "body_sha256": sha256_bytes(body),
        "content_type": "application/json",
        "ordinal": ordinal,
        "original_cme_url": _request(ordinal)["original_cme_url"],
        "request_id": _request(ordinal)["request_id"],
        "size": len(body),
        "snapshot_count": 0,
        "snapshots": [],
        "status_code": 200,
    }


def _authority(reused: dict[str, object]) -> dict[str, object]:
    digest = "a" * 64
    return {
        "diagnostic_approval_path": "configs/diagnostic-approval.json",
        "diagnostic_approval_receipt_id": digest,
        "diagnostic_approval_sha256": digest,
        "diagnostic_plan_id": digest,
        "diagnostic_plan_path": "reports/exchange_calendar/diagnostic-plan.json",
        "diagnostic_plan_sha256": digest,
        "diagnostic_result_id": digest,
        "diagnostic_result_path": (
            "reports/exchange_calendar/diagnostic-result.json"
        ),
        "diagnostic_result_sha256": digest,
        "failure_assessment_path": "reports/exchange_calendar/failure.json",
        "failure_assessment_sha256": digest,
        "predecessor_approval_path": "configs/predecessor-approval.json",
        "predecessor_approval_receipt_id": digest,
        "predecessor_approval_sha256": digest,
        "predecessor_plan_id": digest,
        "predecessor_plan_path": (
            "reports/exchange_calendar/predecessor-plan.json"
        ),
        "predecessor_plan_sha256": digest,
        "reused_request_id": _request(1)["request_id"],
        "reused_response_sha256": sha256_json(reused),
    }


def _implementation() -> dict[str, str]:
    return {relative: "b" * 64 for relative in IMPLEMENTATION_PATHS}


def _plan() -> dict[str, object]:
    reused = _response(1, reused=True)
    return build_recovery_plan(
        authority=_authority(reused),
        remaining_requests=[_request(item) for item in range(2, 60)],
        reused_response=reused,
        implementation_sha256=_implementation(),
    )


def _approval(plan: dict[str, object], plan_sha256: str) -> dict[str, object]:
    core: dict[str, object] = {
        "approved_at": "2026-07-27T23:59:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "c" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def test_recovery_plan_reuses_one_and_requests_only_remaining_58() -> None:
    plan = _plan()
    assert validate_recovery_plan(plan) == plan
    assert plan["execution_authorized"] is False
    assert plan["scope"]["reused_response_count"] == 1
    assert plan["scope"]["max_network_requests"] == 58
    assert plan["scope"]["reused_response"]["ordinal"] == 1
    assert [item["ordinal"] for item in plan["scope"]["requests"]] == list(
        range(2, 60)
    )

    tampered = dict(plan)
    tampered["execution_authorized"] = True
    with pytest.raises(IntegrityError):
        validate_recovery_plan(tampered)


def test_recovery_approval_requires_exact_plan_and_self_hash() -> None:
    plan = _plan()
    plan_sha256 = sha256_json({"plan": "file"})
    approval = _approval(plan, plan_sha256)
    assert (
        validate_recovery_approval(
            approval,
            plan=plan,
            plan_sha256=plan_sha256,
        )
        == approval
    )
    wrong = dict(approval)
    wrong["approval_receipt_id"] = "d" * 64
    with pytest.raises(IntegrityError):
        validate_recovery_approval(
            wrong,
            plan=plan,
            plan_sha256=plan_sha256,
        )
    with pytest.raises(UnauthorizedOperation):
        validate_recovery_approval(
            approval,
            plan=plan,
            plan_sha256="e" * 64,
        )


def test_recovery_execute_combines_reused_and_network_responses(
    boundary,
    monkeypatch,
) -> None:
    plan = _plan()
    plan_path = (
        boundary.active_root
        / "reports/exchange_calendar/recovery-plan.json"
    )
    approval_path = (
        boundary.active_root / "configs/recovery-approval.json"
    )
    output_path = (
        boundary.active_root
        / (
            "reports/exchange_calendar/"
            f"archived_cme_holiday_gap_index_recovery_result_"
            f"{str(plan['plan_id'])[:8]}.json"
        )
    )
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    approval_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, sha256_bytes(plan_path.read_bytes()))
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")

    monkeypatch.setattr(
        module,
        "_reconstruct_plan",
        lambda **_kwargs: plan,
    )
    monkeypatch.setattr(
        module,
        "implementation_hashes",
        lambda _root: _implementation(),
    )
    monkeypatch.setattr(
        module,
        "_fetch",
        lambda request: {
            key: value
            for key, value in _response(int(request["ordinal"])).items()
            if key != "acquisition"
        },
    )

    result = execute_recovery(
        plan_path=plan_path,
        approval_path=approval_path,
        output_path=output_path,
        boundary=boundary,
    )

    assert result["network_request_count"] == 58
    assert result["reused_response_count"] == 1
    assert result["response_count"] == 59
    assert result["responses"][0]["acquisition"] == (
        "REUSED_HASH_VERIFIED_RAW_DIAGNOSTIC"
    )
    assert result["responses"][1]["acquisition"] == "NETWORK_CAPTURED"
