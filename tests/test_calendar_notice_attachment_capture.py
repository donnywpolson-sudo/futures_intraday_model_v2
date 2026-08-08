import hashlib
import urllib.error
from email.message import Message
from pathlib import Path

import pytest

import futures_rebuild.calendar_notice_attachment_capture as capture_module
from futures_rebuild.calendar_notice_attachment_capture import (
    APPROVAL_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    NoticeAttachmentCaptureError,
    NoticeAttachmentRequestError,
    _fetch,
    _failure_evidence,
    build_attachment_capture_plan,
    capture_attachments,
    load_attachment_capture,
    validate_attachment_capture_approval,
    validate_attachment_capture_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.data_layout import PhasePublisher
from futures_rebuild.errors import ContractError


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


def _candidate(ordinal: int, extension: str = ".pdf") -> dict[str, object]:
    url = (
        "https://www.cmegroup.com/notices/clearing/2020/01/"
        f"attachment-{ordinal}{extension}"
    )
    return {
        "discovery_reasons": ["NOTICE_PATH"],
        "extension": extension,
        "link_texts": ["Full text"],
        "source_notice_request_ids": [f"notice-{ordinal}"],
        "source_notice_urls": [
            "https://www.cmegroup.com/notices/clearing/2020/01/"
            f"notice-{ordinal}.html"
        ],
        "source_titles": [f"Notice {ordinal}"],
        "url": url,
    }


def _implementation() -> dict[str, str]:
    return {relative: "f" * 64 for relative in IMPLEMENTATION_PATHS}


def test_attachment_plan_binds_exact_sorted_requests(monkeypatch) -> None:
    monkeypatch.setattr(capture_module, "MAX_REQUESTS", 2)
    candidates = [_candidate(1), _candidate(2, ".xls")]

    plan = build_attachment_capture_plan(
        authority=_authority(),
        candidates=candidates,
        implementation_sha256=_implementation(),
    )

    assert validate_attachment_capture_plan(plan) == plan
    assert plan["execution_authorized"] is False
    scope = plan["scope"]
    assert scope["max_requests"] == 2
    assert scope["workers"] == 2
    assert [item["url"] for item in scope["requests"]] == [
        item["url"] for item in candidates
    ]
    assert scope["requests"][0]["logical_path"].endswith(".pdf")
    assert scope["requests"][1]["logical_path"].endswith(".xls")
    assert "PARSE_OR_ACCEPT_HISTORICAL_CALENDAR" in (
        scope["forbidden_actions"]
    )


def test_attachment_plan_rejects_non_cme_url(monkeypatch) -> None:
    monkeypatch.setattr(capture_module, "MAX_REQUESTS", 1)
    candidate = _candidate(1)
    candidate["url"] = "https://example.com/attachment.pdf"

    with pytest.raises(ContractError, match="candidate URL is invalid"):
        build_attachment_capture_plan(
            authority=_authority(),
            candidates=[candidate],
            implementation_sha256=_implementation(),
        )


def test_attachment_approval_requires_exact_plan_hash(monkeypatch) -> None:
    monkeypatch.setattr(capture_module, "MAX_REQUESTS", 1)
    plan = build_attachment_capture_plan(
        authority=_authority(),
        candidates=[_candidate(1)],
        implementation_sha256=_implementation(),
    )
    core = {
        "approved_at": "2026-07-27T20:00:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": "1" * 64,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "2" * 64,
    }
    approval = {**core, "approval_receipt_id": sha256_json(core)}

    assert (
        validate_attachment_capture_approval(
            approval,
            plan=plan,
            plan_sha256="1" * 64,
        )
        == approval["approval_receipt_id"]
    )
    with pytest.raises(
        NoticeAttachmentCaptureError,
        match="exact hash-bound approval",
    ):
        validate_attachment_capture_approval(
            approval,
            plan=plan,
            plan_sha256="3" * 64,
        )


def test_fetch_rejects_html_for_pdf(monkeypatch) -> None:
    candidate = _candidate(1)
    request_id = (
        "attachment-0001-"
        + hashlib.sha256(
            str(candidate["url"]).encode("utf-8")
        ).hexdigest()[:12]
    )
    spec = {
        "accept": "application/pdf",
        "expected_content_types": ["application/pdf"],
        "request_id": request_id,
        "url": candidate["url"],
    }

    class FakeResponse:
        status = 200

        def __init__(self) -> None:
            self.headers = Message()
            self.headers["Content-Type"] = "text/html"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return candidate["url"]

    class FakeOpener:
        def open(self, _request, timeout):
            assert timeout == capture_module.REQUEST_TIMEOUT_SECONDS
            return FakeResponse()

    monkeypatch.setattr(
        capture_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
    )

    with pytest.raises(
        NoticeAttachmentRequestError,
        match="content type is unexpected",
    ) as raised:
        _fetch(spec, allowed={str(candidate["url"])})
    assert raised.value.failure_code == "MIME_TYPE_REJECTED"
    assert raised.value.safe_details == {
        "content_type": "text/html",
        "expected_content_types": ["application/pdf"],
    }
    assert _failure_evidence(raised.value, spec=spec) == {
        "error_class": "NoticeAttachmentRequestError",
        "failure_code": "MIME_TYPE_REJECTED",
        "request_id": request_id,
        "safe_details": {
            "content_type": "text/html",
            "expected_content_types": ["application/pdf"],
        },
        "url": candidate["url"],
    }


def test_fetch_classifies_http_without_body_or_exception_text(
    monkeypatch,
) -> None:
    candidate = _candidate(1)
    spec = {
        "accept": "application/pdf",
        "expected_content_types": ["application/pdf"],
        "request_id": "attachment-1",
        "url": candidate["url"],
    }
    headers = Message()
    headers["Content-Type"] = "text/html; charset=UTF-8"

    class FakeOpener:
        def open(self, request, timeout):
            raise urllib.error.HTTPError(
                request.full_url,
                404,
                "sensitive upstream diagnostic text",
                headers,
                None,
            )

    monkeypatch.setattr(
        capture_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
    )

    with pytest.raises(NoticeAttachmentRequestError) as raised:
        _fetch(spec, allowed={str(candidate["url"])})

    assert raised.value.failure_code == "HTTP_STATUS_REJECTED"
    assert raised.value.safe_details == {
        "content_type": "text/html",
        "http_status": 404,
    }
    assert "sensitive" not in str(raised.value.evidence())


def test_fetch_classifies_network_reason_by_type_only(monkeypatch) -> None:
    candidate = _candidate(1)
    spec = {
        "accept": "application/pdf",
        "expected_content_types": ["application/pdf"],
        "request_id": "attachment-1",
        "url": candidate["url"],
    }

    class SensitiveReason(Exception):
        pass

    class FakeOpener:
        def open(self, _request, timeout):
            raise urllib.error.URLError(
                SensitiveReason("do not persist this text")
            )

    monkeypatch.setattr(
        capture_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
    )

    with pytest.raises(NoticeAttachmentRequestError) as raised:
        _fetch(spec, allowed={str(candidate["url"])})

    assert raised.value.failure_code == "NETWORK_ERROR"
    assert raised.value.safe_details == {
        "reason_class": "SensitiveReason"
    }
    assert "do not persist" not in str(raised.value.evidence())


def test_fetch_classifies_wrapped_timeout_without_message(
    monkeypatch,
) -> None:
    candidate = _candidate(1)
    spec = {
        "accept": "application/pdf",
        "expected_content_types": ["application/pdf"],
        "request_id": "attachment-1",
        "url": candidate["url"],
    }

    class FakeOpener:
        def open(self, _request, timeout):
            raise urllib.error.URLError(
                TimeoutError("do not persist timeout endpoint text")
            )

    monkeypatch.setattr(
        capture_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
    )

    with pytest.raises(NoticeAttachmentRequestError) as raised:
        _fetch(spec, allowed={str(candidate["url"])})

    assert raised.value.failure_code == "REQUEST_TIMEOUT"
    assert raised.value.safe_details == {}
    assert "do not persist" not in str(raised.value.evidence())


def test_capture_publishes_only_exact_approved_attachments(
    boundary,
    operation_factory,
    monkeypatch,
) -> None:
    monkeypatch.setattr(capture_module, "MAX_REQUESTS", 2)
    monkeypatch.setattr(capture_module, "MAX_TOTAL_BYTES", 1_024)
    monkeypatch.setattr(capture_module, "MAX_RESPONSE_BYTES", 512)
    candidates = [_candidate(1), _candidate(2, ".xls")]
    authority = _authority()
    implementation = _implementation()
    plan = build_attachment_capture_plan(
        authority=authority,
        candidates=candidates,
        implementation_sha256=implementation,
    )
    plan_path = boundary.active_root / "reports" / "plan.json"
    plan_path.parent.mkdir(parents=True)
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    plan_sha256 = hashlib.sha256(plan_path.read_bytes()).hexdigest()
    approval_core = {
        "approved_at": "2026-07-27T20:00:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "2" * 64,
    }
    approval = {
        **approval_core,
        "approval_receipt_id": sha256_json(approval_core),
    }
    approval_path = boundary.active_root / "configs" / "approval.json"
    approval_path.parent.mkdir(parents=True, exist_ok=True)
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    opened: list[str] = []

    class FakeResponse:
        status = 200

        def __init__(self, url: str) -> None:
            self.url = url
            self.headers = Message()
            self.headers["Content-Type"] = (
                "application/vnd.ms-excel"
                if url.endswith(".xls")
                else "application/pdf"
            )

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return self.url

        def read(self, maximum):
            assert maximum == 513
            return f"body:{self.url}".encode("utf-8")

    class FakeOpener:
        def open(self, request, timeout):
            assert timeout == capture_module.REQUEST_TIMEOUT_SECONDS
            opened.append(request.full_url)
            return FakeResponse(request.full_url)

    monkeypatch.setattr(
        capture_module,
        "implementation_hashes",
        lambda _root: implementation,
    )
    monkeypatch.setattr(
        capture_module,
        "attachment_authority",
        lambda **_kwargs: (authority, candidates),
    )
    monkeypatch.setattr(
        capture_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
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

    receipt = capture_attachments(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=publisher,
    )
    capture = load_attachment_capture(receipt, boundary=boundary)

    assert opened == [item["url"] for item in candidates]
    assert capture["network_request_count"] == 2
    assert [item["extension"] for item in capture["responses"]] == [
        ".pdf",
        ".xls",
    ]
