from pathlib import Path

import pytest

import futures_rebuild.calendar_holiday_schedule_capture as module
from futures_rebuild.calendar_holiday_schedule_capture import (
    APPROVAL_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    HolidayScheduleCaptureError,
    build_holiday_schedule_capture_plan,
    capture_holiday_schedules,
    load_holiday_schedule_capture,
    validate_holiday_schedule_capture_approval,
    validate_holiday_schedule_capture_plan,
)
from futures_rebuild.calendar_notice_attachment_capture import (
    NoticeAttachmentRequestError,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.data_layout import PhasePublisher
from futures_rebuild.errors import ContractError


def _authority():
    return {
        "assessment_id": "a" * 64,
        "assessment_path": "reports/exchange_calendar/assessment.json",
        "assessment_sha256": "b" * 64,
        "candidate_set_id": "c" * 64,
        "source_capture_id": "d" * 64,
        "source_manifest_path": (
            "manifests/data_releases/reference/" + "e" * 64 + ".json"
        ),
        "source_manifest_sha256": "f" * 64,
        "source_release_id": "e" * 64,
    }


def _candidate(ordinal, extension=".pdf"):
    return {
        "evidence_kinds": ["PDF_ANNOTATION_URI"],
        "extension": extension,
        "ordinal": ordinal,
        "source_ordinals": [ordinal + 100],
        "source_request_ids": [f"attachment-{ordinal + 100:04d}"],
        "url": (
            "https://www.cmegroup.com/tools-information/"
            "holiday-calendar/files/"
            f"{ordinal:04d}-schedule{extension}"
        ),
    }


def _implementation():
    return {relative: "1" * 64 for relative in IMPLEMENTATION_PATHS}


def _approval(plan, plan_sha256):
    core = {
        "approved_at": "2026-07-27T22:00:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "2" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def test_holiday_schedule_plan_binds_exact_requests(monkeypatch):
    monkeypatch.setattr(module, "MAX_REQUESTS", 3)
    monkeypatch.setattr(module, "MAX_TOTAL_BYTES", 3 * 16_777_216)
    candidates = [
        _candidate(1),
        _candidate(2, ".xls"),
        _candidate(3, ".xlsx"),
    ]
    plan = build_holiday_schedule_capture_plan(
        authority=_authority(),
        candidates=candidates,
        implementation_sha256=_implementation(),
    )

    assert validate_holiday_schedule_capture_plan(plan) == plan
    scope = plan["scope"]
    assert scope["max_requests"] == 3
    assert scope["continue_conditions"] == [
        "EXACT_HTTP_404_FOR_LISTED_URL"
    ]
    assert [item["extension"] for item in scope["requests"]] == [
        ".pdf",
        ".xls",
        ".xlsx",
    ]
    assert "PARSE_OR_ACCEPT_HISTORICAL_CALENDAR" in (
        scope["forbidden_actions"]
    )


def test_holiday_schedule_plan_rejects_noncanonical_url(monkeypatch):
    monkeypatch.setattr(module, "MAX_REQUESTS", 1)
    candidate = _candidate(1)
    candidate["url"] += "?download=1"
    with pytest.raises(ContractError, match="candidate URL is invalid"):
        build_holiday_schedule_capture_plan(
            authority=_authority(),
            candidates=[candidate],
            implementation_sha256=_implementation(),
        )


def test_holiday_schedule_approval_is_exact(monkeypatch):
    monkeypatch.setattr(module, "MAX_REQUESTS", 1)
    monkeypatch.setattr(module, "MAX_TOTAL_BYTES", 16_777_216)
    plan = build_holiday_schedule_capture_plan(
        authority=_authority(),
        candidates=[_candidate(1)],
        implementation_sha256=_implementation(),
    )
    approval = _approval(plan, "3" * 64)
    assert (
        validate_holiday_schedule_capture_approval(
            approval,
            plan=plan,
            plan_sha256="3" * 64,
        )
        == approval["approval_receipt_id"]
    )
    with pytest.raises(HolidayScheduleCaptureError):
        validate_holiday_schedule_capture_approval(
            approval,
            plan=plan,
            plan_sha256="4" * 64,
        )


def test_capture_publishes_payloads_and_exact_404_exclusion(
    boundary,
    operation_factory,
    monkeypatch,
):
    monkeypatch.setattr(module, "MAX_REQUESTS", 3)
    monkeypatch.setattr(module, "MAX_TOTAL_BYTES", 3 * 16_777_216)
    candidates = [
        _candidate(1),
        _candidate(2, ".xls"),
        _candidate(3, ".xlsx"),
    ]
    implementation = _implementation()
    authority = _authority()
    plan = build_holiday_schedule_capture_plan(
        authority=authority,
        candidates=candidates,
        implementation_sha256=implementation,
    )
    plan_path = boundary.active_root / "reports" / "plan.json"
    plan_path.parent.mkdir(parents=True)
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(
        plan,
        module.sha256_file(plan_path),
    )
    approval_path = boundary.active_root / "configs" / "approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")

    bodies = {
        1: b"%PDF-synthetic",
        3: b"PK\x03\x04synthetic",
    }

    def fake_fetch(spec, *, allowed):
        assert spec["url"] in allowed
        ordinal = int(spec["ordinal"])
        if ordinal == 2:
            raise NoticeAttachmentRequestError(
                "not found",
                failure_code="HTTP_STATUS_REJECTED",
                safe_details={
                    "content_type": "text/html",
                    "http_status": 404,
                },
            )
        return (
            bodies[ordinal],
            "application/octet-stream",
            {"content-type": "application/octet-stream"},
            "2026-07-27T22:00:01Z",
        )

    monkeypatch.setattr(module, "implementation_hashes", lambda _root: implementation)
    monkeypatch.setattr(
        module,
        "holiday_schedule_authority",
        lambda **_kwargs: (authority, candidates),
    )
    monkeypatch.setattr(module, "_fetch", fake_fetch)
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=(
            boundary.active_root
            / "state"
            / "locks"
            / "data-publication.lock"
        ),
    )

    receipt = capture_holiday_schedules(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=publisher,
    )
    capture = load_holiday_schedule_capture(
        receipt,
        boundary=boundary,
        verify_files=True,
    )

    assert capture["network_request_count"] == 3
    assert capture["response_count"] == 2
    assert capture["exclusion_count"] == 1
    assert capture["exclusions"][0]["ordinal"] == 2
    assert capture["unresolved_candidate_count"] == 0


def test_capture_stops_on_non_404_http_failure(
    boundary,
    operation_factory,
    monkeypatch,
):
    monkeypatch.setattr(module, "MAX_REQUESTS", 1)
    monkeypatch.setattr(module, "MAX_TOTAL_BYTES", 16_777_216)
    candidates = [_candidate(1)]
    implementation = _implementation()
    authority = _authority()
    plan = build_holiday_schedule_capture_plan(
        authority=authority,
        candidates=candidates,
        implementation_sha256=implementation,
    )
    plan_path = boundary.active_root / "reports" / "plan.json"
    plan_path.parent.mkdir(parents=True)
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, module.sha256_file(plan_path))
    approval_path = boundary.active_root / "configs" / "approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")

    def failed_fetch(_spec, *, allowed):
        assert allowed
        raise NoticeAttachmentRequestError(
            "server error",
            failure_code="HTTP_STATUS_REJECTED",
            safe_details={
                "content_type": "text/html",
                "http_status": 500,
            },
        )

    monkeypatch.setattr(module, "implementation_hashes", lambda _root: implementation)
    monkeypatch.setattr(
        module,
        "holiday_schedule_authority",
        lambda **_kwargs: (authority, candidates),
    )
    monkeypatch.setattr(module, "_fetch", failed_fetch)
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=(
            boundary.active_root
            / "state"
            / "locks"
            / "data-publication.lock"
        ),
    )

    with pytest.raises(
        HolidayScheduleCaptureError,
        match="stopped on failure",
    ):
        capture_holiday_schedules(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=publisher,
        )
    failure = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / f"cme_historical_holiday_schedule_capture_failure_{plan['plan_id'][:8]}.json"
    )
    assert failure.is_file()
