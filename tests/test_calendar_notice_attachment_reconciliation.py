import hashlib
from pathlib import Path

import pytest

import futures_rebuild.calendar_notice_attachment_reconciliation as module
from futures_rebuild.calendar_notice_attachment_capture import (
    NoticeAttachmentRequestError,
)
from futures_rebuild.calendar_notice_attachment_reconciliation import (
    APPROVAL_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    _network_exclusion,
    build_reconciliation_plan,
    capture_attachment_reconciliation,
    load_attachment_reconciliation_capture,
    validate_reconciliation_approval,
    validate_reconciliation_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.data_layout import PhasePublisher


def _patch_counts(monkeypatch) -> None:
    monkeypatch.setattr(module, "TOTAL_CANDIDATES", 6)
    monkeypatch.setattr(module, "REUSED_RESPONSES", 2)
    monkeypatch.setattr(module, "KNOWN_EXCLUSIONS", 2)
    monkeypatch.setattr(module, "NETWORK_REQUESTS", 2)
    monkeypatch.setattr(module, "FIRST_NETWORK_ORDINAL", 5)
    monkeypatch.setattr(module, "KNOWN_EXCLUSION_ORDINALS", (2, 3))
    monkeypatch.setattr(module, "RECOVERY_COMPLETED_ORDINAL", 4)
    monkeypatch.setattr(module, "MAX_RESPONSE_BYTES", 512)
    monkeypatch.setattr(module, "MAX_NETWORK_BYTES", 1_024)
    monkeypatch.setattr(module, "MAX_TOTAL_BYTES", 1_024)


def _request(ordinal: int) -> dict[str, object]:
    request_id = f"attachment-{ordinal:04d}"
    return {
        "accept": "application/octet-stream, application/pdf",
        "discovery_reasons": ["NOTICE_PATH"],
        "expected_content_types": [
            "application/octet-stream",
            "application/pdf",
        ],
        "extension": ".pdf",
        "link_texts": ["Full text"],
        "logical_path": (
            "data/reference/exchange_calendars/"
            f"{request_id}.pdf"
        ),
        "ordinal": ordinal,
        "request_id": request_id,
        "request_kind": "HISTORICAL_NOTICE_ATTACHMENT_CAPTURE",
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


def _exclusion(ordinal: int) -> dict[str, object]:
    request = _request(ordinal)
    return {
        "classification": {
            "error_class": "NoticeAttachmentRequestError",
            "failure_code": "HTTP_STATUS_REJECTED",
            "safe_details": {
                "content_type": "text/html",
                "http_status": 404,
            },
        },
        "evidence_id": f"{ordinal}" * 64,
        "evidence_kind": (
            "DIAGNOSTIC_RESULT"
            if ordinal == 2
            else "RECOVERY_FAILURE"
        ),
        "evidence_path": f"reports/evidence-{ordinal}.json",
        "evidence_sha256": f"{ordinal + 2}" * 64,
        "ordinal": ordinal,
        "request_id": request["request_id"],
        "status": "EXCLUDED_AUTHORITATIVE_HTTP_404",
        "url": request["url"],
    }


def _authority(
    *,
    exclusions: list[dict[str, object]],
    preserved_total_bytes: int,
) -> dict[str, object]:
    return {
        "first_remaining_request_id": _request(5)["request_id"],
        "known_exclusion_count": 2,
        "known_exclusion_set_id": sha256_json(exclusions),
        "last_remaining_request_id": _request(6)["request_id"],
        "predecessor_approval_path": "configs/predecessor-approval.json",
        "predecessor_approval_receipt_id": "a" * 64,
        "predecessor_approval_sha256": "b" * 64,
        "predecessor_failure_id": "c" * 64,
        "predecessor_failure_path": "reports/predecessor-failure.json",
        "predecessor_failure_sha256": "d" * 64,
        "predecessor_plan_id": "e" * 64,
        "predecessor_plan_path": "reports/predecessor-plan.json",
        "predecessor_plan_sha256": "f" * 64,
        "preserved_response_count": 2,
        "preserved_response_set_id": "1" * 64,
        "preserved_stage_relative_path": (
            "state/data_publication_staging/predecessor"
        ),
        "preserved_total_bytes": preserved_total_bytes,
        "remaining_request_count": 2,
        "source_union_release_id": "2" * 64,
    }


def _implementation() -> dict[str, str]:
    return {relative: "9" * 64 for relative in IMPLEMENTATION_PATHS}


def test_reconciliation_plan_binds_one_get_per_remaining_url(
    monkeypatch,
) -> None:
    _patch_counts(monkeypatch)
    exclusions = [_exclusion(2), _exclusion(3)]
    requests = [_request(5), _request(6)]
    plan = build_reconciliation_plan(
        authority=_authority(
            exclusions=exclusions,
            preserved_total_bytes=20,
        ),
        remaining_requests=requests,
        known_exclusions=exclusions,
        implementation_sha256=_implementation(),
    )

    assert validate_reconciliation_plan(plan) == plan
    assert plan["execution_authorized"] is False
    assert plan["scope"]["max_network_requests"] == 2
    assert plan["scope"]["retries"] == 0
    assert plan["scope"]["allow_redirects"] is False
    assert plan["scope"]["allow_http_404_exclusion_and_continue"] is True
    assert {item["url"] for item in exclusions}.isdisjoint(
        {item["url"] for item in plan["scope"]["requests"]}
    )


@pytest.mark.parametrize("status", [401, 403, 410, 429, 500, 503])
def test_only_exact_http_404_is_a_continuable_exclusion(
    status,
) -> None:
    request = _request(5)
    not_found = NoticeAttachmentRequestError(
        "not found",
        failure_code="HTTP_STATUS_REJECTED",
        safe_details={
            "content_type": "text/html",
            "http_status": 404,
        },
    )
    other = NoticeAttachmentRequestError(
        "other status",
        failure_code="HTTP_STATUS_REJECTED",
        safe_details={
            "content_type": "text/html",
            "http_status": status,
        },
    )

    exclusion = _network_exclusion(spec=request, exc=not_found)

    assert exclusion is not None
    assert exclusion["ordinal"] == 5
    assert exclusion["status"] == "EXCLUDED_AUTHORITATIVE_HTTP_404"
    assert _network_exclusion(spec=request, exc=other) is None


def test_reconciliation_continues_on_404_and_publishes_complete_union(
    boundary,
    operation_factory,
    monkeypatch,
) -> None:
    _patch_counts(monkeypatch)
    source_stage = (
        boundary.active_root
        / "state"
        / "data_publication_staging"
        / "predecessor"
    )
    source_stage.mkdir(parents=True)
    reused: list[dict[str, object]] = []
    descriptors: list[dict[str, object]] = []
    total = 0
    for ordinal in (1, 4):
        request = _request(ordinal)
        body = f"%PDF-reused-{ordinal}\n".encode("utf-8")
        name = Path(str(request["logical_path"])).name
        physical = source_stage / name
        physical.write_bytes(body)
        digest = hashlib.sha256(body).hexdigest()
        total += len(body)
        reused.append(
            {
                "acquisition": "NETWORK",
                "content_type": "application/pdf",
                "discovery_reasons": request["discovery_reasons"],
                "extension": ".pdf",
                "logical_path": request["logical_path"],
                "ordinal": ordinal,
                "received_at_utc": "2026-07-27T21:00:00Z",
                "request_id": request["request_id"],
                "request_kind": request["request_kind"],
                "safe_headers": {
                    "content-type": "application/pdf"
                },
                "sha256": digest,
                "size": len(body),
                "source_notice_request_ids": request[
                    "source_notice_request_ids"
                ],
                "source_titles": request["source_titles"],
                "status_code": 200,
                "url": request["url"],
            }
        )
        descriptors.append(
            {
                "content_type": "application/pdf",
                "logical_path": request["logical_path"],
                "ordinal": ordinal,
                "request_id": request["request_id"],
                "sha256": digest,
                "size": len(body),
                "url": request["url"],
            }
        )
    exclusions = [_exclusion(2), _exclusion(3)]
    authority = _authority(
        exclusions=exclusions,
        preserved_total_bytes=total,
    )
    requests = [_request(5), _request(6)]
    implementation = _implementation()
    plan = build_reconciliation_plan(
        authority=authority,
        remaining_requests=requests,
        known_exclusions=exclusions,
        implementation_sha256=implementation,
    )
    plan_path = boundary.active_root / "reports" / "plan.json"
    plan_path.parent.mkdir(parents=True)
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval_core = {
        "approved_at": "2026-07-27T21:01:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": hashlib.sha256(
            plan_path.read_bytes()
        ).hexdigest(),
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "8" * 64,
    }
    approval = {
        **approval_core,
        "approval_receipt_id": sha256_json(approval_core),
    }
    approval_path = boundary.active_root / "configs" / "approval.json"
    approval_path.parent.mkdir(parents=True, exist_ok=True)
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    predecessor_failure_path = (
        boundary.active_root / authority["predecessor_failure_path"]
    )
    predecessor_failure_path.parent.mkdir(parents=True, exist_ok=True)
    predecessor_failure_path.write_bytes(
        canonical_bytes({"responses_preserved": reused}) + b"\n"
    )
    monkeypatch.setattr(
        module,
        "implementation_hashes",
        lambda _root: implementation,
    )
    monkeypatch.setattr(
        module,
        "reconciliation_authority",
        lambda **_kwargs: (
            authority,
            requests,
            descriptors,
            exclusions,
        ),
    )

    def fake_fetch(spec, **_kwargs):
        if spec["ordinal"] == 5:
            raise NoticeAttachmentRequestError(
                "not found",
                failure_code="HTTP_STATUS_REJECTED",
                safe_details={
                    "content_type": "text/html",
                    "http_status": 404,
                },
            )
        return (
            b"%PDF-network-6\n",
            "application/pdf",
            {"content-type": "application/pdf"},
            "2026-07-27T21:02:00Z",
        )

    monkeypatch.setattr(module, "fetch_attachment", fake_fetch)
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

    assert validate_reconciliation_approval(
        approval,
        plan=plan,
        plan_sha256=approval_core["plan_sha256"],
    ) == approval["approval_receipt_id"]
    receipt = capture_attachment_reconciliation(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=publisher,
    )
    capture = load_attachment_reconciliation_capture(
        receipt,
        boundary=boundary,
    )

    assert capture["network_request_count"] == 2
    assert capture["resolved_candidate_count"] == 6
    assert capture["unresolved_candidate_count"] == 0
    assert [item["ordinal"] for item in capture["responses"]] == [1, 4, 6]
    assert [item["ordinal"] for item in capture["exclusions"]] == [2, 3, 5]
