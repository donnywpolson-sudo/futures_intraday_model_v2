import hashlib
import json
import shutil
from email.message import Message
from pathlib import Path

import pytest

import futures_rebuild.calendar_notice_metadata as metadata_module
from futures_rebuild.calendar_notice_metadata import (
    APPROVAL_SCHEMA,
    CAPTURE_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    RELEASE_KIND,
    SEARCH_REQUESTS,
    NoticeMetadataCaptureError,
    build_capability_assessment,
    build_metadata_discovery_plan,
    capture_metadata_discovery,
    implementation_hashes,
    load_metadata_discovery_capture,
    metadata_authority,
    validate_metadata_discovery_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.data_layout import PhasePublisher
from futures_rebuild.errors import IntegrityError


REPO = Path(__file__).resolve().parents[1]
CAPABILITY_RELEASE_ID = (
    "be9e64789d486b846fcc07c36083bddbdab00461d62974fbd93fe040369547b2"
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


def _authority_evidence(boundary) -> tuple[Path, Path]:
    manifest_relative = (
        "manifests/data_releases/reference/"
        f"{CAPABILITY_RELEASE_ID}.json"
    )
    data_relative = (
        "data/reference/exchange_calendars/"
        f"{CAPABILITY_RELEASE_ID}/"
        "001-historical-notice-search-capability.json"
    )
    _copy_file(boundary.active_root, manifest_relative)
    _copy_file(boundary.active_root, data_relative)
    manifest_path = boundary.active_root / manifest_relative
    assessment = build_capability_assessment(
        capability_manifest_path=manifest_path,
        boundary=boundary,
    )
    assessment_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "capability-assessment.json"
    )
    assessment_path.parent.mkdir(parents=True)
    assessment_path.write_bytes(canonical_bytes(assessment) + b"\n")
    return manifest_path, assessment_path


def _authority(boundary) -> dict[str, object]:
    manifest_path, assessment_path = _authority_evidence(boundary)
    return metadata_authority(
        capability_manifest_path=manifest_path,
        assessment_path=assessment_path,
        boundary=boundary,
    )


def _copy_implementation_closure(active: Path) -> None:
    for relative in IMPLEMENTATION_PATHS:
        _copy_file(active, relative)


def _approval(plan: dict[str, object], plan_sha256: str) -> dict[str, object]:
    core = {
        "approved_at": "2026-07-27T13:30:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "e" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def test_capability_assessment_and_metadata_plan_are_exact(
    boundary,
) -> None:
    manifest_path, assessment_path = _authority_evidence(boundary)
    assessment = json.loads(assessment_path.read_text(encoding="utf-8"))
    assert assessment["total_pages"] == 643
    assert assessment["total_results"] == 19_269
    plan = build_metadata_discovery_plan(
        authority=metadata_authority(
            capability_manifest_path=manifest_path,
            assessment_path=assessment_path,
            boundary=boundary,
        ),
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    assert validate_metadata_discovery_plan(plan) == plan
    scope = plan["scope"]
    assert isinstance(scope, dict)
    assert scope["max_requests"] == 2
    assert [item["url"] for item in scope["requests"]] == [  # type: ignore[index]
        item["url"] for item in SEARCH_REQUESTS
    ]
    drifted = json.loads(json.dumps(plan))
    drifted["scope"]["requests"][0]["query"] = "hours"
    with pytest.raises(IntegrityError):
        validate_metadata_discovery_plan(drifted)


def test_metadata_capture_rejects_bad_approval_before_network(
    boundary, operation_factory, monkeypatch
) -> None:
    plan = build_metadata_discovery_plan(
        authority=_authority(boundary),
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    plan_path = boundary.active_root / "reports" / "metadata-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, "0" * 64)
    approval_path = boundary.active_root / "configs" / "metadata-approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    monkeypatch.setattr(
        metadata_module.urllib.request,
        "build_opener",
        lambda *_args: pytest.fail("network opener constructed before approval"),
    )
    with pytest.raises(
        NoticeMetadataCaptureError, match="exact hash-bound approval"
    ):
        capture_metadata_discovery(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=_publisher(boundary, operation_factory),
        )


def test_metadata_capture_publishes_only_two_exact_queries(
    boundary, operation_factory, monkeypatch
) -> None:
    authority = _authority(boundary)
    _copy_implementation_closure(boundary.active_root)
    plan = build_metadata_discovery_plan(
        authority=authority,
        implementation_sha256=implementation_hashes(boundary.active_root),
    )
    plan_path = boundary.active_root / "reports" / "metadata-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, _sha256_bytes(plan_path.read_bytes()))
    approval_path = boundary.active_root / "configs" / "metadata-approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    bodies = {
        str(SEARCH_REQUESTS[0]["url"]): (
            b'{"currentPage":0,"facets":[],"hasMore":true,'
            b'"results":[],"totalPages":3,"totalResults":70}\n'
        ),
        str(SEARCH_REQUESTS[1]["url"]): (
            b'{"currentPage":0,"facets":[],"hasMore":true,'
            b'"results":[],"totalPages":2,"totalResults":45}\n'
        ),
    }
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
            assert maximum <= 16_777_217
            return bodies[self.url]

    class FakeOpener:
        def open(self, request, timeout):
            assert timeout == 30
            opened.append(request.full_url)
            return FakeResponse(request.full_url)

    monkeypatch.setattr(
        metadata_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
    )
    receipt = capture_metadata_discovery(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    payload = load_metadata_discovery_capture(receipt, boundary=boundary)
    assert opened == [str(item["url"]) for item in SEARCH_REQUESTS]
    assert receipt.release_kind == RELEASE_KIND
    assert receipt.schema_version == CAPTURE_SCHEMA
    assert payload["request_count"] == 2
    for ordinal, request in enumerate(SEARCH_REQUESTS, start=1):
        filename = f"{ordinal:03d}-{request['request_id']}.json"
        assert (
            receipt.resolve_unique_filename(filename, boundary).read_bytes()
            == bodies[str(request["url"])]
        )
