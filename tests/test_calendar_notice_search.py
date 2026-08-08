import hashlib
import json
import shutil
from email.message import Message
from pathlib import Path

import pytest

import futures_rebuild.calendar_notice_search as search_module
from futures_rebuild.calendar_notice_search import (
    APPROVAL_SCHEMA,
    CAPABILITY_URL,
    CAPTURE_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    RELEASE_KIND,
    NoticeSearchCaptureError,
    build_notice_endpoint_assessment,
    build_notice_search_capability_plan,
    capture_notice_search_capability,
    derive_notice_endpoint_evidence,
    implementation_hashes,
    load_notice_search_capability_capture,
    notice_search_authority,
    validate_notice_search_capability_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.data_layout import PhasePublisher
from futures_rebuild.errors import IntegrityError


REPO = Path(__file__).resolve().parents[1]
NOTICES_RELEASE_ID = (
    "1e417664f71bbc8197fd8918f5bdff2061e1849f26f95f5684df2e0e78b6a88b"
)
CLIENT_RELEASE_ID = (
    "2d8b7a83b64352457180d6c1c4cdc0c6285dafe8b70e99ba99b5cd1b7781691f"
)
COMMON_RELEASE_ID = (
    "7c71bbfe451b7f9c6029c9557d2a26d7411fc50209ae86a633374349f00f6516"
)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _publisher(boundary, operation_factory) -> PhasePublisher:
    return PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root
        / "state"
        / "locks"
        / "data-publication.lock",
    )


def _copy_file(active: Path, relative: str) -> None:
    source = REPO / relative
    target = active / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def _copy_release(active: Path, release_id: str, filenames: list[str]) -> Path:
    manifest = (
        f"manifests/data_releases/reference/{release_id}.json"
    )
    _copy_file(active, manifest)
    for filename in filenames:
        _copy_file(
            active,
            f"data/reference/exchange_calendars/{release_id}/{filename}",
        )
    return active / manifest


def _authority_evidence(boundary) -> tuple[Path, Path, Path, Path]:
    notices = _copy_release(
        boundary.active_root,
        NOTICES_RELEASE_ID,
        ["001-notices-page.html"],
    )
    client = _copy_release(
        boundary.active_root,
        CLIENT_RELEASE_ID,
        [
            "001-dynamic-alert-list.js",
            "002-search-sort-filter-dynamic.js",
        ],
    )
    common = _copy_release(
        boundary.active_root,
        COMMON_RELEASE_ID,
        ["trading-hours-common.js"],
    )
    evidence = derive_notice_endpoint_evidence(
        notices_manifest_path=notices,
        client_manifest_path=client,
        common_manifest_path=common,
        boundary=boundary,
    )
    assessment = build_notice_endpoint_assessment(evidence)
    assessment_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "notice-endpoint-assessment.json"
    )
    assessment_path.parent.mkdir(parents=True)
    assessment_path.write_bytes(canonical_bytes(assessment) + b"\n")
    return notices, client, common, assessment_path


def _authority(boundary) -> dict[str, object]:
    notices, client, common, assessment = _authority_evidence(boundary)
    return notice_search_authority(
        notices_manifest_path=notices,
        client_manifest_path=client,
        common_manifest_path=common,
        assessment_path=assessment,
        boundary=boundary,
    )


def _copy_implementation_closure(active: Path) -> None:
    for relative in IMPLEMENTATION_PATHS:
        _copy_file(active, relative)


def _approval(plan: dict[str, object], plan_sha256: str) -> dict[str, object]:
    core = {
        "approved_at": "2026-07-27T13:00:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "e" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def test_notice_endpoint_and_capability_plan_are_offline_exact(
    boundary,
) -> None:
    notices, client, common, assessment_path = _authority_evidence(boundary)
    assessment = json.loads(assessment_path.read_text(encoding="utf-8"))
    assert assessment["capability_url"] == CAPABILITY_URL
    assert assessment["facade_module_id"] == 26088
    assert assessment["service_module_id"] == 20237
    plan = build_notice_search_capability_plan(
        authority=notice_search_authority(
            notices_manifest_path=notices,
            client_manifest_path=client,
            common_manifest_path=common,
            assessment_path=assessment_path,
            boundary=boundary,
        ),
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    assert validate_notice_search_capability_plan(plan) == plan
    scope = plan["scope"]
    assert isinstance(scope, dict)
    assert scope["max_requests"] == 1
    assert scope["request"]["url"] == CAPABILITY_URL  # type: ignore[index]
    drifted = json.loads(json.dumps(plan))
    drifted["scope"]["request"]["url"] = "https://www.cmegroup.com/notices.html"
    with pytest.raises(IntegrityError):
        validate_notice_search_capability_plan(drifted)


def test_notice_capability_rejects_bad_approval_before_network(
    boundary, operation_factory, monkeypatch
) -> None:
    plan = build_notice_search_capability_plan(
        authority=_authority(boundary),
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    plan_path = boundary.active_root / "reports" / "notice-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, "0" * 64)
    approval_path = (
        boundary.active_root / "configs" / "notice-approval.json"
    )
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    monkeypatch.setattr(
        search_module.urllib.request,
        "build_opener",
        lambda *_args: pytest.fail("network opener constructed before approval"),
    )
    with pytest.raises(
        NoticeSearchCaptureError, match="exact hash-bound approval"
    ):
        capture_notice_search_capability(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=_publisher(boundary, operation_factory),
        )


def test_notice_capability_publishes_only_exact_response(
    boundary, operation_factory, monkeypatch
) -> None:
    authority = _authority(boundary)
    _copy_implementation_closure(boundary.active_root)
    plan = build_notice_search_capability_plan(
        authority=authority,
        implementation_sha256=implementation_hashes(boundary.active_root),
    )
    plan_path = boundary.active_root / "reports" / "notice-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, _sha256_bytes(plan_path.read_bytes()))
    approval_path = (
        boundary.active_root / "configs" / "notice-approval.json"
    )
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    body = (
        b'{"currentPage":0,"facets":[],"results":[],'
        b'"totalPages":42,"totalResults":417}\n'
    )
    opened: list[str] = []

    class FakeResponse:
        status = 200

        def __init__(self) -> None:
            self.headers = Message()
            self.headers["Content-Type"] = (
                "application/json; charset=UTF-8"
            )

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return CAPABILITY_URL

        def read(self, maximum):
            assert maximum == 8_388_609
            return body

    class FakeOpener:
        def open(self, request, timeout):
            assert timeout == 30
            opened.append(request.full_url)
            return FakeResponse()

    monkeypatch.setattr(
        search_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
    )
    receipt = capture_notice_search_capability(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    payload = load_notice_search_capability_capture(
        receipt, boundary=boundary
    )
    assert opened == [CAPABILITY_URL]
    assert receipt.release_kind == RELEASE_KIND
    assert receipt.schema_version == CAPTURE_SCHEMA
    assert payload["request_count"] == 1
    assert (
        receipt.resolve_unique_filename(
            "001-historical-notice-search-capability.json", boundary
        ).read_bytes()
        == body
    )
