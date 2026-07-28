import hashlib
import json
import shutil
from email.message import Message
from pathlib import Path

import pytest

import futures_rebuild.calendar_notice_pagination as pagination_module
from futures_rebuild.calendar_notice_pagination import (
    APPROVAL_SCHEMA,
    CAPTURE_SCHEMA,
    IMPLEMENTATION_PATHS,
    NETWORK_REQUESTS,
    OPERATION,
    RELEASE_KIND,
    TOTAL_RESPONSES,
    NoticePaginationCaptureError,
    build_pagination_plan,
    build_semantic_assessment,
    capture_pagination,
    implementation_hashes,
    load_pagination_capture,
    pagination_authority,
    validate_pagination_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.data_layout import PhasePublisher
from futures_rebuild.errors import IntegrityError


REPO = Path(__file__).resolve().parents[1]
SEMANTIC_RELEASE_ID = (
    "52266170f15dd70b8e21e2e408a8a8e6d3d82628bf1fc4b9b68ee1f22dbabeea"
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


def _authority_evidence(boundary) -> tuple[Path, Path, dict[str, object]]:
    manifest_relative = (
        "manifests/data_releases/reference/"
        f"{SEMANTIC_RELEASE_ID}.json"
    )
    _copy_file(boundary.active_root, manifest_relative)
    for filename in ("001-holiday.json", "002-trading-hours.json"):
        _copy_file(
            boundary.active_root,
            "data/reference/exchange_calendars/"
            f"{SEMANTIC_RELEASE_ID}/{filename}",
        )
    manifest_path = boundary.active_root / manifest_relative
    assessment = build_semantic_assessment(
        semantic_manifest_path=manifest_path,
        boundary=boundary,
    )
    assessment_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "semantic-assessment.json"
    )
    assessment_path.parent.mkdir(parents=True)
    assessment_path.write_bytes(canonical_bytes(assessment) + b"\n")
    return manifest_path, assessment_path, assessment


def _authority(boundary) -> tuple[dict[str, object], dict[str, object]]:
    manifest, assessment_path, assessment = _authority_evidence(boundary)
    return (
        pagination_authority(
            semantic_manifest_path=manifest,
            assessment_path=assessment_path,
            boundary=boundary,
        ),
        assessment,
    )


def _copy_implementation_closure(active: Path) -> None:
    for relative in IMPLEMENTATION_PATHS:
        _copy_file(active, relative)


def _approval(plan: dict[str, object], plan_sha256: str) -> dict[str, object]:
    core = {
        "approved_at": "2026-07-27T13:45:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "e" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def test_semantic_assessment_and_pagination_plan_are_exact(
    boundary,
) -> None:
    authority, assessment = _authority(boundary)
    assert assessment["network_request_count"] == 46
    assert assessment["reused_response_count"] == 2
    plan = build_pagination_plan(
        authority=authority,
        assessment=assessment,
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    assert validate_pagination_plan(plan) == plan
    scope = plan["scope"]
    assert isinstance(scope, dict)
    assert scope["max_requests"] == NETWORK_REQUESTS
    assert len(scope["requests"]) == NETWORK_REQUESTS
    assert len(scope["reuse_responses"]) == 2
    assert scope["requests"][0]["page"] == 1  # type: ignore[index]
    assert scope["requests"][-1]["page"] == 30  # type: ignore[index]
    drifted = json.loads(json.dumps(plan))
    drifted["scope"]["requests"][0]["page"] = 0
    with pytest.raises(IntegrityError):
        validate_pagination_plan(drifted)


def test_pagination_rejects_bad_approval_before_network(
    boundary, operation_factory, monkeypatch
) -> None:
    authority, assessment = _authority(boundary)
    plan = build_pagination_plan(
        authority=authority,
        assessment=assessment,
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    plan_path = boundary.active_root / "reports" / "pagination-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, "0" * 64)
    approval_path = (
        boundary.active_root / "configs" / "pagination-approval.json"
    )
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    monkeypatch.setattr(
        pagination_module.urllib.request,
        "build_opener",
        lambda *_args: pytest.fail("network opener constructed before approval"),
    )
    with pytest.raises(
        NoticePaginationCaptureError, match="exact hash-bound approval"
    ):
        capture_pagination(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=_publisher(boundary, operation_factory),
        )


def test_pagination_reuses_two_and_requests_only_remaining_pages(
    boundary, operation_factory, monkeypatch
) -> None:
    authority, assessment = _authority(boundary)
    _copy_implementation_closure(boundary.active_root)
    plan = build_pagination_plan(
        authority=authority,
        assessment=assessment,
        implementation_sha256=implementation_hashes(boundary.active_root),
    )
    plan_path = boundary.active_root / "reports" / "pagination-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, _sha256_bytes(plan_path.read_bytes()))
    approval_path = (
        boundary.active_root / "configs" / "pagination-approval.json"
    )
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    opened: list[str] = []

    class FakeResponse:
        status = 200

        def __init__(self, url: str) -> None:
            self.url = url
            self.headers = Message()
            self.headers["Content-Type"] = (
                "application/json; charset=UTF-8"
            )

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return self.url

        def read(self, maximum):
            assert maximum <= 67_108_865
            return b'{"results":[]}\n'

    class FakeOpener:
        def open(self, request, timeout):
            assert timeout == 30
            opened.append(request.full_url)
            return FakeResponse(request.full_url)

    monkeypatch.setattr(
        pagination_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
    )
    receipt = capture_pagination(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    payload = load_pagination_capture(receipt, boundary=boundary)
    assert len(opened) == NETWORK_REQUESTS
    assert opened == [
        str(item["url"]) for item in plan["scope"]["requests"]  # type: ignore[index]
    ]
    assert receipt.release_kind == RELEASE_KIND
    assert receipt.schema_version == CAPTURE_SCHEMA
    assert payload["total_response_count"] == TOTAL_RESPONSES
    assert payload["reused_response_count"] == 2
    assert payload["network_request_count"] == NETWORK_REQUESTS
