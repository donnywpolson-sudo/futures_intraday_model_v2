import hashlib
import json
import shutil
from email.message import Message
from pathlib import Path

import pytest

import futures_rebuild.calendar_notice_documents as documents_module
from futures_rebuild.calendar_notice_documents import (
    APPROVAL_SCHEMA,
    CAPTURE_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    PROBE_REQUESTS,
    RELEASE_KIND,
    NoticeDocumentCaptureError,
    build_document_probe_plan,
    build_metadata_index,
    capture_document_probe,
    document_probe_authority,
    implementation_hashes,
    load_document_probe_capture,
    validate_document_probe_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.data_layout import PhasePublisher
from futures_rebuild.errors import IntegrityError


REPO = Path(__file__).resolve().parents[1]
PAGINATION_RELEASE_ID = (
    "2fecac572adc8c3aceedc5f2e90a688b8443353fe90b12e6501d10df0e94f090"
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


def _pagination_evidence(boundary) -> Path:
    manifest_relative = (
        "manifests/data_releases/reference/"
        f"{PAGINATION_RELEASE_ID}.json"
    )
    manifest = json.loads((REPO / manifest_relative).read_text(encoding="utf-8"))
    _copy_file(boundary.active_root, manifest_relative)
    for item in manifest["files"]:
        filename = Path(item["logical_path"]).name
        _copy_file(
            boundary.active_root,
            "data/reference/exchange_calendars/"
            f"{PAGINATION_RELEASE_ID}/{filename}",
        )
    return boundary.active_root / manifest_relative


def _index_evidence(boundary) -> tuple[Path, Path, dict[str, object]]:
    manifest_path = _pagination_evidence(boundary)
    index = build_metadata_index(
        pagination_manifest_path=manifest_path,
        boundary=boundary,
    )
    index_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "notice-metadata-index.json"
    )
    index_path.parent.mkdir(parents=True)
    index_path.write_bytes(canonical_bytes(index) + b"\n")
    return manifest_path, index_path, index


def _authority_from_index(
    manifest_path: Path,
    index_path: Path,
    index: dict[str, object],
) -> dict[str, object]:
    return {
        "index_id": index["index_id"],
        "index_path": index_path.relative_to(index_path.parents[2]).as_posix(),
        "index_sha256": sha256_file(index_path),
        "pagination_capture_id": index["pagination_capture_id"],
        "pagination_manifest_path": index["pagination_manifest_path"],
        "pagination_manifest_sha256": sha256_file(manifest_path),
        "pagination_release_id": index["pagination_release_id"],
    }


def _copy_implementation_closure(active: Path) -> None:
    for relative in IMPLEMENTATION_PATHS:
        _copy_file(active, relative)


def _approval(plan: dict[str, object], plan_sha256: str) -> dict[str, object]:
    core = {
        "approved_at": "2026-07-27T14:00:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "e" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def test_complete_metadata_index_and_document_plan_are_exact(
    boundary,
) -> None:
    manifest_path, index_path, index = _index_evidence(boundary)
    assert index["unique_url_count"] == 1273
    assert index["overlap_url_count"] == 159
    assert index["query_summaries"] == {
        "holiday": {
            "page_count": 17,
            "result_count": 510,
            "total_pages": 17,
            "total_results": 510,
        },
        "trading hours": {
            "page_count": 31,
            "result_count": 924,
            "total_pages": 31,
            "total_results": 924,
        },
    }
    authority = document_probe_authority(
        pagination_manifest_path=manifest_path,
        index_path=index_path,
        boundary=boundary,
    )
    plan = build_document_probe_plan(
        authority=authority,
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    assert validate_document_probe_plan(plan) == plan
    scope = plan["scope"]
    assert isinstance(scope, dict)
    assert scope["max_requests"] == 2
    assert [item["url"] for item in scope["requests"]] == [  # type: ignore[index]
        item["url"] for item in PROBE_REQUESTS
    ]
    drifted = json.loads(json.dumps(plan))
    drifted["scope"]["requests"][0]["url"] = str(PROBE_REQUESTS[1]["url"])
    with pytest.raises(IntegrityError):
        validate_document_probe_plan(drifted)


def test_document_probe_rejects_bad_approval_before_network(
    boundary, operation_factory, monkeypatch
) -> None:
    plan = build_document_probe_plan(
        authority={
            "index_id": "a" * 64,
            "index_path": "reports/exchange_calendar/index.json",
            "index_sha256": "b" * 64,
            "pagination_capture_id": "c" * 64,
            "pagination_manifest_path": (
                "manifests/data_releases/reference/release.json"
            ),
            "pagination_manifest_sha256": "d" * 64,
            "pagination_release_id": "e" * 64,
        },
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    plan_path = boundary.active_root / "reports" / "document-plan.json"
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, "0" * 64)
    approval_path = boundary.active_root / "configs" / "document-approval.json"
    approval_path.parent.mkdir(parents=True, exist_ok=True)
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    monkeypatch.setattr(
        documents_module.urllib.request,
        "build_opener",
        lambda *_args: pytest.fail("network opener constructed before approval"),
    )
    with pytest.raises(
        NoticeDocumentCaptureError, match="exact hash-bound approval"
    ):
        capture_document_probe(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=_publisher(boundary, operation_factory),
        )


def test_document_probe_publishes_only_two_exact_html_pages(
    boundary, operation_factory, monkeypatch
) -> None:
    manifest_path, index_path, index = _index_evidence(boundary)
    authority = _authority_from_index(manifest_path, index_path, index)
    _copy_implementation_closure(boundary.active_root)
    plan = build_document_probe_plan(
        authority=authority,
        implementation_sha256=implementation_hashes(boundary.active_root),
    )
    plan_path = boundary.active_root / "reports" / "document-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, _sha256_bytes(plan_path.read_bytes()))
    approval_path = boundary.active_root / "configs" / "document-approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    bodies = {
        str(PROBE_REQUESTS[0]["url"]): b"<html>modern notice</html>\n",
        str(PROBE_REQUESTS[1]["url"]): b"<html>legacy notice</html>\n",
    }
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
            assert maximum <= 16_777_217
            return bodies[self.url]

    class FakeOpener:
        def open(self, request, timeout):
            assert timeout == 30
            opened.append(request.full_url)
            return FakeResponse(request.full_url)

    monkeypatch.setattr(
        documents_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
    )
    monkeypatch.setattr(
        documents_module,
        "document_probe_authority",
        lambda **_kwargs: authority,
    )
    receipt = capture_document_probe(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    payload = load_document_probe_capture(receipt, boundary=boundary)
    assert opened == [str(item["url"]) for item in PROBE_REQUESTS]
    assert receipt.release_kind == RELEASE_KIND
    assert receipt.schema_version == CAPTURE_SCHEMA
    assert payload["request_count"] == 2
    for ordinal, request in enumerate(PROBE_REQUESTS, start=1):
        filename = f"{ordinal:03d}-{request['request_id']}.html"
        assert (
            receipt.resolve_unique_filename(filename, boundary).read_bytes()
            == bodies[str(request["url"])]
        )
