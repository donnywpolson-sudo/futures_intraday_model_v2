import hashlib
from pathlib import Path

import futures_rebuild.calendar_notice_attachment_capture as capture_module
import futures_rebuild.calendar_notice_attachment_diagnostic as diagnostic_module
import futures_rebuild.calendar_notice_attachment_recovery as recovery_module
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
    build_attachment_capture_plan,
)
from futures_rebuild.calendar_notice_attachment_diagnostic import (
    APPROVAL_SCHEMA as DIAGNOSTIC_APPROVAL_SCHEMA,
)
from futures_rebuild.calendar_notice_attachment_diagnostic import (
    IMPLEMENTATION_PATHS as DIAGNOSTIC_IMPLEMENTATION_PATHS,
)
from futures_rebuild.calendar_notice_attachment_diagnostic import (
    OPERATION as DIAGNOSTIC_OPERATION,
)
from futures_rebuild.calendar_notice_attachment_diagnostic import (
    PREDECESSOR_FAILURE_SCHEMA,
    RESULT_SCHEMA as DIAGNOSTIC_RESULT_SCHEMA,
    build_diagnostic_plan,
    preserved_failure_authority,
)
from futures_rebuild.calendar_notice_attachment_recovery import (
    APPROVAL_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    build_recovery_plan,
    capture_attachment_recovery,
    load_attachment_recovery_capture,
    recovery_authority,
    validate_recovery_approval,
    validate_recovery_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.data_layout import PhasePublisher


def _authority() -> dict[str, object]:
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
    monkeypatch.setattr(capture_module, "MAX_REQUESTS", 3)
    monkeypatch.setattr(
        diagnostic_module,
        "EXPECTED_PREDECESSOR_REQUESTS",
        3,
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
        1,
    )
    monkeypatch.setattr(recovery_module, "TOTAL_CANDIDATES", 3)
    monkeypatch.setattr(recovery_module, "REUSED_RESPONSES", 1)
    monkeypatch.setattr(recovery_module, "EXCLUDED_RESPONSES", 1)
    monkeypatch.setattr(recovery_module, "NETWORK_REQUESTS", 1)
    monkeypatch.setattr(recovery_module, "TOTAL_PAYLOADS", 2)
    monkeypatch.setattr(recovery_module, "MAX_NETWORK_BYTES", 1_024)
    monkeypatch.setattr(recovery_module, "MAX_TOTAL_BYTES", 1_024)
    monkeypatch.setattr(recovery_module, "MAX_RESPONSE_BYTES", 512)


def _evidence(boundary, monkeypatch):
    _bind_small_counts(monkeypatch)
    candidates = [_candidate(index) for index in range(1, 4)]
    predecessor_implementation = {
        relative: "1" * 64
        for relative in PREDECESSOR_IMPLEMENTATION_PATHS
    }
    predecessor_plan = build_attachment_capture_plan(
        authority=_authority(),
        candidates=candidates,
        implementation_sha256=predecessor_implementation,
    )
    predecessor_plan_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "predecessor-plan.json"
    )
    predecessor_plan_path.parent.mkdir(parents=True)
    predecessor_plan_path.write_bytes(
        canonical_bytes(predecessor_plan) + b"\n"
    )
    predecessor_plan_sha = hashlib.sha256(
        predecessor_plan_path.read_bytes()
    ).hexdigest()
    predecessor_approval_core = {
        "approved_at": "2026-07-27T20:00:00Z",
        "operation": PREDECESSOR_OPERATION,
        "plan_id": predecessor_plan["plan_id"],
        "plan_sha256": predecessor_plan_sha,
        "schema_version": PREDECESSOR_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "2" * 64,
    }
    predecessor_approval = {
        **predecessor_approval_core,
        "approval_receipt_id": sha256_json(
            predecessor_approval_core
        ),
    }
    predecessor_approval_path = (
        boundary.active_root
        / "configs"
        / "predecessor-approval.json"
    )
    predecessor_approval_path.parent.mkdir(parents=True, exist_ok=True)
    predecessor_approval_path.write_bytes(
        canonical_bytes(predecessor_approval) + b"\n"
    )
    stage = (
        boundary.active_root
        / "state"
        / "data_publication_staging"
        / "predecessor"
    )
    stage.mkdir(parents=True)
    first = predecessor_plan["scope"]["requests"][0]
    body = b"%PDF-preserved\n"
    physical = stage / Path(first["logical_path"]).name
    physical.write_bytes(body)
    response = {
        "content_type": "application/pdf",
        "discovery_reasons": first["discovery_reasons"],
        "extension": first["extension"],
        "logical_path": first["logical_path"],
        "ordinal": first["ordinal"],
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
    failed = predecessor_plan["scope"]["requests"][1]
    failure_core = {
        "approval_receipt_id": predecessor_approval[
            "approval_receipt_id"
        ],
        "elapsed_milliseconds": 10,
        "failed_requests": [
            {
                "error_class": "NoticeAttachmentCaptureError",
                "request_id": failed["request_id"],
                "url": failed["url"],
            }
        ],
        "network_requests_attempted": 2,
        "plan_id": predecessor_plan["plan_id"],
        "plan_sha256": predecessor_plan_sha,
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
        / "failure.json"
    )
    failure_path.write_bytes(canonical_bytes(failure) + b"\n")
    preserved_authority, failed_request, _descriptors = (
        preserved_failure_authority(
            predecessor_plan_path=predecessor_plan_path,
            predecessor_approval_path=predecessor_approval_path,
            failure_report_path=failure_path,
            boundary=boundary,
        )
    )
    diagnostic_plan = build_diagnostic_plan(
        authority=preserved_authority,
        failed_request=failed_request,
        implementation_sha256={
            relative: "3" * 64
            for relative in DIAGNOSTIC_IMPLEMENTATION_PATHS
        },
    )
    diagnostic_plan_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "diagnostic-plan.json"
    )
    diagnostic_plan_path.write_bytes(
        canonical_bytes(diagnostic_plan) + b"\n"
    )
    diagnostic_plan_sha = hashlib.sha256(
        diagnostic_plan_path.read_bytes()
    ).hexdigest()
    diagnostic_approval_core = {
        "approved_at": "2026-07-27T20:02:00Z",
        "operation": DIAGNOSTIC_OPERATION,
        "plan_id": diagnostic_plan["plan_id"],
        "plan_sha256": diagnostic_plan_sha,
        "schema_version": DIAGNOSTIC_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "4" * 64,
    }
    diagnostic_approval = {
        **diagnostic_approval_core,
        "approval_receipt_id": sha256_json(diagnostic_approval_core),
    }
    diagnostic_approval_path = (
        boundary.active_root
        / "configs"
        / "diagnostic-approval.json"
    )
    diagnostic_approval_path.write_bytes(
        canonical_bytes(diagnostic_approval) + b"\n"
    )
    diagnostic_scope = diagnostic_plan["scope"]
    diagnostic_core = {
        "approval_receipt_id": diagnostic_approval[
            "approval_receipt_id"
        ],
        "authority": preserved_authority,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": diagnostic_scope[
                "max_duration_seconds"
            ],
            "max_requests": diagnostic_scope["max_requests"],
            "max_response_bytes": diagnostic_scope[
                "max_response_bytes"
            ],
            "request_timeout_seconds": diagnostic_scope[
                "request_timeout_seconds"
            ],
            "retries": 0,
            "workers": 1,
        },
        "classification": {
            "error_class": "NoticeAttachmentRequestError",
            "failure_code": "HTTP_STATUS_REJECTED",
            "safe_details": {
                "content_type": "text/html",
                "http_status": 404,
            },
        },
        "diagnosed_at_utc": "2026-07-27T20:03:00Z",
        "elapsed_milliseconds": 20,
        "network_request_count": 1,
        "operation": DIAGNOSTIC_OPERATION,
        "payload": None,
        "plan_id": diagnostic_plan["plan_id"],
        "request": diagnostic_scope["request"],
        "schema_version": DIAGNOSTIC_RESULT_SCHEMA,
        "status": "DIAGNOSTIC_COMPLETED",
    }
    diagnostic_result = {
        **diagnostic_core,
        "diagnostic_id": sha256_json(diagnostic_core),
    }
    diagnostic_result_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "diagnostic-result.json"
    )
    diagnostic_result_path.write_bytes(
        canonical_bytes(diagnostic_result) + b"\n"
    )
    return {
        "diagnostic_approval": diagnostic_approval_path,
        "diagnostic_plan": diagnostic_plan_path,
        "diagnostic_result": diagnostic_result_path,
        "failure": failure_path,
        "predecessor_approval": predecessor_approval_path,
        "predecessor_plan": predecessor_plan_path,
    }


def _recovery_inputs(boundary, monkeypatch):
    evidence = _evidence(boundary, monkeypatch)
    authority, remaining, descriptors, exclusion = recovery_authority(
        predecessor_plan_path=evidence["predecessor_plan"],
        predecessor_approval_path=evidence["predecessor_approval"],
        failure_report_path=evidence["failure"],
        diagnostic_plan_path=evidence["diagnostic_plan"],
        diagnostic_approval_path=evidence["diagnostic_approval"],
        diagnostic_result_path=evidence["diagnostic_result"],
        boundary=boundary,
    )
    implementation = {
        relative: "5" * 64 for relative in IMPLEMENTATION_PATHS
    }
    return (
        evidence,
        authority,
        remaining,
        descriptors,
        exclusion,
        implementation,
    )


def test_recovery_plan_reuses_excludes_and_requests_exact_remainder(
    boundary,
    monkeypatch,
) -> None:
    (
        _evidence_paths,
        authority,
        remaining,
        descriptors,
        exclusion,
        implementation,
    ) = _recovery_inputs(boundary, monkeypatch)

    plan = build_recovery_plan(
        authority=authority,
        remaining_requests=remaining,
        exclusion=exclusion,
        implementation_sha256=implementation,
    )

    assert validate_recovery_plan(plan) == plan
    assert plan["execution_authorized"] is False
    assert plan["scope"]["max_network_requests"] == 1
    assert plan["scope"]["reused_response_count"] == 1
    assert len(descriptors) == 1
    assert exclusion["status"] == "EXCLUDED_AUTHORITATIVE_HTTP_404"
    assert remaining[0]["ordinal"] == 3
    assert exclusion["url"] not in {
        item["url"] for item in plan["scope"]["requests"]
    }


def test_recovery_approval_and_capture_are_exact_and_fail_closed(
    boundary,
    operation_factory,
    monkeypatch,
) -> None:
    (
        _evidence_paths,
        authority,
        remaining,
        _descriptors,
        exclusion,
        implementation,
    ) = _recovery_inputs(boundary, monkeypatch)
    plan = build_recovery_plan(
        authority=authority,
        remaining_requests=remaining,
        exclusion=exclusion,
        implementation_sha256=implementation,
    )
    plan_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "recovery-plan.json"
    )
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    plan_sha = hashlib.sha256(plan_path.read_bytes()).hexdigest()
    approval_core = {
        "approved_at": "2026-07-27T20:04:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "6" * 64,
    }
    approval = {
        **approval_core,
        "approval_receipt_id": sha256_json(approval_core),
    }
    approval_path = (
        boundary.active_root
        / "configs"
        / "recovery-approval.json"
    )
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")

    assert validate_recovery_approval(
        approval,
        plan=plan,
        plan_sha256=plan_sha,
    ) == approval["approval_receipt_id"]
    monkeypatch.setattr(
        recovery_module,
        "implementation_hashes",
        lambda _root: implementation,
    )
    monkeypatch.setattr(
        recovery_module,
        "fetch_attachment",
        lambda spec, **_kwargs: (
            b"%PDF-network\n",
            "application/pdf",
            {"content-type": "application/pdf"},
            "2026-07-27T20:05:00Z",
        ),
    )
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

    receipt = capture_attachment_recovery(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=publisher,
    )
    capture = load_attachment_recovery_capture(
        receipt,
        boundary=boundary,
    )

    assert capture["network_request_count"] == 1
    assert capture["reused_response_count"] == 1
    assert capture["excluded_response_count"] == 1
    assert [item["ordinal"] for item in capture["responses"]] == [1, 3]
    assert capture["excluded_requests"][0]["ordinal"] == 2
