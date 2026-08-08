import hashlib
import json
import shutil
from email.message import Message
from pathlib import Path

import pytest

import futures_rebuild.calendar_notice_union as union_module
from futures_rebuild.calendar_notice_union import (
    APPROVAL_SCHEMA,
    ASSESSMENT_SCHEMA,
    CAPTURE_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    RELEASE_KIND,
    NoticeUnionCaptureError,
    build_probe_assessment,
    build_union_plan,
    capture_union,
    implementation_hashes,
    load_union_capture,
    union_authority,
    validate_union_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.data_layout import PhasePublisher
from futures_rebuild.errors import IntegrityError


REPO = Path(__file__).resolve().parents[1]
PROBE_RELEASE_ID = (
    "7b4952c94b6f0284d380e66d173222e13e6716c392a7222f704c0ee4c727de0b"
)
INDEX_RELATIVE = (
    "reports/exchange_calendar/"
    "cme_historical_notice_metadata_index_2fecac57.json"
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


def _probe_evidence(boundary) -> tuple[Path, Path]:
    manifest_relative = (
        "manifests/data_releases/reference/"
        f"{PROBE_RELEASE_ID}.json"
    )
    manifest = json.loads((REPO / manifest_relative).read_text(encoding="utf-8"))
    _copy_file(boundary.active_root, manifest_relative)
    for item in manifest["files"]:
        filename = Path(item["logical_path"]).name
        _copy_file(
            boundary.active_root,
            "data/reference/exchange_calendars/"
            f"{PROBE_RELEASE_ID}/{filename}",
        )
    _copy_file(boundary.active_root, INDEX_RELATIVE)
    return (
        boundary.active_root / manifest_relative,
        boundary.active_root / INDEX_RELATIVE,
    )


def _assessment_evidence(
    boundary,
) -> tuple[Path, Path, Path, dict[str, object]]:
    probe_manifest, index_path = _probe_evidence(boundary)
    assessment = build_probe_assessment(
        probe_manifest_path=probe_manifest,
        index_path=index_path,
        boundary=boundary,
    )
    assessment_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "probe-assessment.json"
    )
    assessment_path.write_bytes(canonical_bytes(assessment) + b"\n")
    return probe_manifest, index_path, assessment_path, assessment


def _copy_implementation_closure(active: Path) -> None:
    for relative in IMPLEMENTATION_PATHS:
        _copy_file(active, relative)


def _approval(plan: dict[str, object], plan_sha256: str) -> dict[str, object]:
    core = {
        "approved_at": "2026-07-27T14:30:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "e" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def _synthetic_candidates(count: int) -> list[dict[str, object]]:
    return [
        {
            "queries": ["holiday"],
            "relative_url": f"/notices/test/{ordinal:04d}.html",
            "title": f"Notice {ordinal:04d}",
            "url": (
                "https://www.cmegroup.com/notices/test/"
                f"{ordinal:04d}.html"
            ),
        }
        for ordinal in range(1, count + 1)
    ]


def _synthetic_authority() -> dict[str, object]:
    return {
        "assessment_id": "a" * 64,
        "assessment_path": "reports/exchange_calendar/assessment.json",
        "assessment_sha256": "b" * 64,
        "index_id": "c" * 64,
        "index_path": "reports/exchange_calendar/index.json",
        "index_sha256": "d" * 64,
        "pagination_release_id": "e" * 64,
        "probe_capture_id": "f" * 64,
        "probe_manifest_path": (
            "manifests/data_releases/reference/probe.json"
        ),
        "probe_manifest_sha256": "1" * 64,
        "probe_release_id": "2" * 64,
    }


def test_probe_assessment_and_complete_union_plan_are_exact(boundary) -> None:
    probe_manifest, index_path, assessment_path, assessment = (
        _assessment_evidence(boundary)
    )
    assert assessment["schema_version"] == ASSESSMENT_SCHEMA
    assert assessment["required_document_url_count"] == 1273
    assert [item["document_shape"] for item in assessment["documents"]] == [
        "INLINE_SCHEDULE_TEXT_WITH_PDF_MIRROR",
        "ATTACHMENT_BACKED_SCHEDULE",
    ]
    authority, index = union_authority(
        probe_manifest_path=probe_manifest,
        index_path=index_path,
        assessment_path=assessment_path,
        boundary=boundary,
    )
    plan = build_union_plan(
        authority=authority,
        candidates=index["candidates"],  # type: ignore[arg-type]
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    assert validate_union_plan(plan) == plan
    scope = plan["scope"]
    assert isinstance(scope, dict)
    assert scope["max_requests"] == 1273
    assert scope["workers"] == 2
    assert len(scope["requests"]) == 1273
    drifted = json.loads(json.dumps(plan))
    drifted["scope"]["requests"][0]["url"] = (
        "https://www.cmegroup.com/notices/test/drift.html"
    )
    with pytest.raises(IntegrityError):
        validate_union_plan(drifted)


def test_union_rejects_bad_approval_before_network(
    boundary, operation_factory, monkeypatch
) -> None:
    candidates = _synthetic_candidates(1273)
    plan = build_union_plan(
        authority=_synthetic_authority(),
        candidates=candidates,
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    plan_path = boundary.active_root / "reports" / "union-plan.json"
    plan_path.parent.mkdir(parents=True)
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, "0" * 64)
    approval_path = boundary.active_root / "configs" / "union-approval.json"
    approval_path.parent.mkdir(parents=True, exist_ok=True)
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    monkeypatch.setattr(
        union_module.urllib.request,
        "build_opener",
        lambda *_args: pytest.fail("network opener constructed before approval"),
    )
    with pytest.raises(
        NoticeUnionCaptureError, match="exact hash-bound approval"
    ):
        capture_union(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=_publisher(boundary, operation_factory),
        )


def test_union_publishes_only_exact_allowlisted_pages(
    boundary, operation_factory, monkeypatch
) -> None:
    monkeypatch.setattr(union_module, "MAX_REQUESTS", 3)
    candidates = _synthetic_candidates(3)
    authority = _synthetic_authority()
    _copy_implementation_closure(boundary.active_root)
    plan = build_union_plan(
        authority=authority,
        candidates=candidates,
        implementation_sha256=implementation_hashes(boundary.active_root),
    )
    plan_path = boundary.active_root / "reports" / "union-plan.json"
    plan_path.parent.mkdir(parents=True)
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, _sha256_bytes(plan_path.read_bytes()))
    approval_path = boundary.active_root / "configs" / "union-approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    monkeypatch.setattr(
        union_module,
        "union_authority",
        lambda **_kwargs: (authority, {"candidates": candidates}),
    )
    opened: list[str] = []

    class FakeResponse:
        status = 200

        def __init__(self, url: str) -> None:
            self.url = url
            self.headers = Message()
            self.headers["Content-Type"] = "text/html; charset=UTF-8"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return self.url

        def read(self, maximum):
            assert maximum == 1_048_577
            return f"<html>{self.url}</html>\n".encode()

    class FakeOpener:
        def open(self, request, timeout):
            assert timeout == 30
            opened.append(request.full_url)
            return FakeResponse(request.full_url)

    monkeypatch.setattr(
        union_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
    )
    receipt = capture_union(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    payload = load_union_capture(receipt, boundary=boundary)
    assert sorted(opened) == sorted(item["url"] for item in candidates)
    assert receipt.release_kind == RELEASE_KIND
    assert receipt.schema_version == CAPTURE_SCHEMA
    assert payload["network_request_count"] == 3
    assert len(payload["responses"]) == 3


def test_existing_failure_consumes_union_approval_before_network(
    boundary, operation_factory, monkeypatch
) -> None:
    monkeypatch.setattr(union_module, "MAX_REQUESTS", 3)
    candidates = _synthetic_candidates(3)
    authority = _synthetic_authority()
    _copy_implementation_closure(boundary.active_root)
    plan = build_union_plan(
        authority=authority,
        candidates=candidates,
        implementation_sha256=implementation_hashes(boundary.active_root),
    )
    plan_path = boundary.active_root / "reports" / "union-plan.json"
    plan_path.parent.mkdir(parents=True)
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, _sha256_bytes(plan_path.read_bytes()))
    approval_path = boundary.active_root / "configs" / "union-approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    failure = union_module._failure_path(  # noqa: SLF001
        boundary.active_root, str(plan["plan_id"])
    )
    failure.parent.mkdir(parents=True, exist_ok=True)
    failure.write_bytes(b"{}\n")
    monkeypatch.setattr(
        union_module,
        "union_authority",
        lambda **_kwargs: (authority, {"candidates": candidates}),
    )
    monkeypatch.setattr(
        union_module.urllib.request,
        "build_opener",
        lambda *_args: pytest.fail("network opener constructed after outcome"),
    )
    with pytest.raises(
        NoticeUnionCaptureError, match="already has a durable outcome"
    ):
        capture_union(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=_publisher(boundary, operation_factory),
        )
