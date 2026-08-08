import hashlib
from pathlib import Path

import pytest

import futures_rebuild.calendar_notice_attachment_capture as capture_module
import futures_rebuild.calendar_notice_attachment_diagnostic as diagnostic_module
from futures_rebuild.calendar_notice_attachment_capture import (
    APPROVAL_SCHEMA as PREDECESSOR_APPROVAL_SCHEMA,
)
from futures_rebuild.calendar_notice_attachment_capture import (
    IMPLEMENTATION_PATHS as PREDECESSOR_IMPLEMENTATION_PATHS,
)
from futures_rebuild.calendar_notice_attachment_capture import (
    OPERATION as PREDECESSOR_OPERATION,
)
from futures_rebuild.calendar_notice_attachment_capture import (
    PREDECESSOR_FAILURE_SCHEMA,
    build_attachment_capture_plan,
)
from futures_rebuild.calendar_notice_attachment_diagnostic import (
    APPROVAL_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    NoticeAttachmentDiagnosticError,
    build_diagnostic_plan,
    preserved_failure_authority,
    run_diagnostic,
    validate_diagnostic_approval,
    validate_diagnostic_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json


def _predecessor_authority() -> dict[str, object]:
    return {
        "assessment_id": "a" * 64,
        "assessment_path": "reports/exchange_calendar/assessment.json",
        "assessment_sha256": "b" * 64,
        "union_capture_id": "c" * 64,
        "union_manifest_path": (
            "manifests/data_releases/reference/union.json"
        ),
        "union_manifest_sha256": "d" * 64,
        "union_release_id": "e" * 64,
    }


def _candidate(ordinal: int) -> dict[str, object]:
    return {
        "discovery_reasons": ["NOTICE_PATH"],
        "extension": ".pdf",
        "link_texts": ["Full text"],
        "source_notice_request_ids": [f"notice-{ordinal}"],
        "source_notice_urls": [
            "https://www.cmegroup.com/notices/test/"
            f"notice-{ordinal}.html"
        ],
        "source_titles": [f"Notice {ordinal}"],
        "url": (
            "https://www.cmegroup.com/notices/test/"
            f"attachment-{ordinal}.pdf"
        ),
    }


def _bind_small_counts(monkeypatch) -> None:
    monkeypatch.setattr(capture_module, "MAX_REQUESTS", 2)
    monkeypatch.setattr(
        diagnostic_module,
        "EXPECTED_PREDECESSOR_REQUESTS",
        2,
    )
    monkeypatch.setattr(
        diagnostic_module,
        "EXPECTED_ATTEMPTED_REQUESTS",
        2,
    )
    monkeypatch.setattr(
        diagnostic_module,
        "EXPECTED_PRESERVED_RESPONSES",
        1,
    )
    monkeypatch.setattr(
        diagnostic_module,
        "EXPECTED_FAILED_ORDINAL",
        2,
    )
    monkeypatch.setattr(
        diagnostic_module,
        "EXPECTED_REMAINING_AFTER_DIAGNOSTIC",
        0,
    )


def _predecessor_evidence(boundary, monkeypatch):
    _bind_small_counts(monkeypatch)
    candidates = [_candidate(1), _candidate(2)]
    implementation = {
        relative: "f" * 64
        for relative in PREDECESSOR_IMPLEMENTATION_PATHS
    }
    plan = build_attachment_capture_plan(
        authority=_predecessor_authority(),
        candidates=candidates,
        implementation_sha256=implementation,
    )
    plan_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "predecessor-plan.json"
    )
    plan_path.parent.mkdir(parents=True)
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    plan_sha256 = hashlib.sha256(plan_path.read_bytes()).hexdigest()
    approval_core = {
        "approved_at": "2026-07-27T20:00:00Z",
        "operation": PREDECESSOR_OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": PREDECESSOR_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "1" * 64,
    }
    approval = {
        **approval_core,
        "approval_receipt_id": sha256_json(approval_core),
    }
    approval_path = (
        boundary.active_root
        / "configs"
        / "predecessor-approval.json"
    )
    approval_path.parent.mkdir(parents=True, exist_ok=True)
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    stage = (
        boundary.active_root
        / "state"
        / "data_publication_staging"
        / "predecessor"
    )
    stage.mkdir(parents=True)
    first = plan["scope"]["requests"][0]
    body = b"%PDF-synthetic-preserved\n"
    physical = stage / Path(first["logical_path"]).name
    physical.write_bytes(body)
    response = {
        "content_type": "application/pdf",
        "discovery_reasons": first["discovery_reasons"],
        "extension": ".pdf",
        "logical_path": first["logical_path"],
        "ordinal": 1,
        "received_at_utc": "2026-07-27T20:01:00Z",
        "request_id": first["request_id"],
        "request_kind": first["request_kind"],
        "safe_headers": {"content-type": "application/pdf"},
        "sha256": hashlib.sha256(body).hexdigest(),
        "size": len(body),
        "source_notice_request_ids": first[
            "source_notice_request_ids"
        ],
        "source_titles": first["source_titles"],
        "status_code": 200,
        "url": first["url"],
    }
    failed = plan["scope"]["requests"][1]
    failure_core = {
        "approval_receipt_id": approval["approval_receipt_id"],
        "elapsed_milliseconds": 123,
        "failed_requests": [
            {
                "error_class": "NoticeAttachmentCaptureError",
                "request_id": failed["request_id"],
                "url": failed["url"],
            }
        ],
        "network_requests_attempted": 2,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "publication_occurred": False,
        "responses_preserved": [response],
        "responses_preserved_count": 1,
        "retries_performed": 0,
        "schema_version": PREDECESSOR_FAILURE_SCHEMA,
        "stage_relative_path": stage.relative_to(
            boundary.active_root
        ).as_posix(),
        "status": "STOPPED",
    }
    failure = {
        **failure_core,
        "failure_id": sha256_json(failure_core),
    }
    failure_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "predecessor-failure.json"
    )
    failure_path.write_bytes(canonical_bytes(failure) + b"\n")
    return plan_path, approval_path, failure_path


def test_preserved_authority_rehashes_exact_stage(
    boundary,
    monkeypatch,
) -> None:
    plan_path, approval_path, failure_path = _predecessor_evidence(
        boundary,
        monkeypatch,
    )

    authority, failed_request, descriptors = (
        preserved_failure_authority(
            predecessor_plan_path=plan_path,
            predecessor_approval_path=approval_path,
            failure_report_path=failure_path,
            boundary=boundary,
        )
    )

    assert authority["preserved_response_count"] == 1
    assert authority["preserved_total_bytes"] == 25
    assert authority["preserved_response_set_id"] == sha256_json(
        descriptors
    )
    assert failed_request["ordinal"] == 2
    assert authority["failed_url"] == failed_request["url"]


def test_diagnostic_plan_is_one_request_and_non_authorizing(
    boundary,
    monkeypatch,
) -> None:
    plan_path, approval_path, failure_path = _predecessor_evidence(
        boundary,
        monkeypatch,
    )
    authority, failed_request, _descriptors = (
        preserved_failure_authority(
            predecessor_plan_path=plan_path,
            predecessor_approval_path=approval_path,
            failure_report_path=failure_path,
            boundary=boundary,
        )
    )
    implementation = {
        relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
    }

    plan = build_diagnostic_plan(
        authority=authority,
        failed_request=failed_request,
        implementation_sha256=implementation,
    )

    assert validate_diagnostic_plan(plan) == plan
    assert plan["execution_authorized"] is False
    assert plan["scope"]["max_requests"] == 1
    assert plan["scope"]["workers"] == 1
    assert plan["scope"]["request"]["url"] == failed_request["url"]
    assert "REQUEST_ANY_OF_THE_REMAINING_783_ATTACHMENTS" in (
        plan["scope"]["forbidden_actions"]
    )


def test_diagnostic_execution_preserves_valid_payload_once(
    boundary,
    monkeypatch,
) -> None:
    plan_path, predecessor_approval, failure_path = (
        _predecessor_evidence(boundary, monkeypatch)
    )
    authority, failed_request, descriptors = (
        preserved_failure_authority(
            predecessor_plan_path=plan_path,
            predecessor_approval_path=predecessor_approval,
            failure_report_path=failure_path,
            boundary=boundary,
        )
    )
    implementation = {
        relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
    }
    plan = build_diagnostic_plan(
        authority=authority,
        failed_request=failed_request,
        implementation_sha256=implementation,
    )
    diagnostic_plan_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "diagnostic-plan.json"
    )
    diagnostic_plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval_core = {
        "approved_at": "2026-07-27T20:05:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": hashlib.sha256(
            diagnostic_plan_path.read_bytes()
        ).hexdigest(),
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "2" * 64,
    }
    approval = {
        **approval_core,
        "approval_receipt_id": sha256_json(approval_core),
    }
    diagnostic_approval = (
        boundary.active_root
        / "configs"
        / "diagnostic-approval.json"
    )
    diagnostic_approval.write_bytes(
        canonical_bytes(approval) + b"\n"
    )
    monkeypatch.setattr(
        diagnostic_module,
        "implementation_hashes",
        lambda _root: implementation,
    )
    monkeypatch.setattr(
        diagnostic_module,
        "preserved_failure_authority",
        lambda **_kwargs: (authority, failed_request, descriptors),
    )
    body = b"%PDF-diagnostic\n"
    monkeypatch.setattr(
        diagnostic_module,
        "_fetch",
        lambda *_args, **_kwargs: (
            body,
            "application/pdf",
            {"content-type": "application/pdf"},
            "2026-07-27T20:06:00Z",
        ),
    )

    result = run_diagnostic(
        plan_path=diagnostic_plan_path,
        approval_path=diagnostic_approval,
        predecessor_plan_path=plan_path,
        predecessor_approval_path=predecessor_approval,
        failure_report_path=failure_path,
        boundary=boundary,
    )

    assert result["status"] == "DIAGNOSTIC_COMPLETED"
    assert result["network_request_count"] == 1
    assert result["classification"]["failure_code"] == (
        "HTTP_200_EXPECTED_MIME_PAYLOAD_PRESERVED"
    )
    payload_path = (
        boundary.active_root
        / result["payload"]["stage_relative_path"]
    )
    assert payload_path.read_bytes() == body
    with pytest.raises(
        NoticeAttachmentDiagnosticError,
        match="already has an outcome",
    ):
        run_diagnostic(
            plan_path=diagnostic_plan_path,
            approval_path=diagnostic_approval,
            predecessor_plan_path=plan_path,
            predecessor_approval_path=predecessor_approval,
            failure_report_path=failure_path,
            boundary=boundary,
        )


def test_diagnostic_approval_is_exact_hash_bound(monkeypatch) -> None:
    _bind_small_counts(monkeypatch)
    authority = {
        "failed_ordinal": 2,
        "failed_request_id": "attachment-2",
        "failed_url": (
            "https://www.cmegroup.com/notices/test/attachment-2.pdf"
        ),
        "failure_id": "a" * 64,
        "failure_report_path": "reports/exchange_calendar/failure.json",
        "failure_report_sha256": "b" * 64,
        "predecessor_approval_path": "configs/approval.json",
        "predecessor_approval_receipt_id": "c" * 64,
        "predecessor_approval_sha256": "d" * 64,
        "predecessor_plan_id": "e" * 64,
        "predecessor_plan_path": "reports/exchange_calendar/plan.json",
        "predecessor_plan_sha256": "f" * 64,
        "preserved_response_count": 1,
        "preserved_response_set_id": "1" * 64,
        "preserved_stage_relative_path": (
            "state/data_publication_staging/stage"
        ),
        "preserved_total_bytes": 10,
        "remaining_request_count_after_diagnostic": 0,
    }
    failed = {
        "expected_content_types": [
            "application/octet-stream",
            "application/pdf",
        ],
        "extension": ".pdf",
        "ordinal": 2,
        "request_id": "attachment-2",
        "url": authority["failed_url"],
    }
    plan = build_diagnostic_plan(
        authority=authority,
        failed_request=failed,
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    core = {
        "approved_at": "2026-07-27T20:05:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": "3" * 64,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "4" * 64,
    }
    approval = {**core, "approval_receipt_id": sha256_json(core)}

    assert validate_diagnostic_approval(
        approval,
        plan=plan,
        plan_sha256="3" * 64,
    ) == approval["approval_receipt_id"]
