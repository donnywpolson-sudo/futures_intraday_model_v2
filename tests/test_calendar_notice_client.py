import hashlib
import json
import shutil
from email.message import Message
from pathlib import Path

import pytest

import futures_rebuild.calendar_notice_client as client_module
from futures_rebuild.calendar_notice_client import (
    APPROVAL_SCHEMA,
    CAPTURE_SCHEMA,
    CLIENT_ASSETS,
    IMPLEMENTATION_PATHS,
    OPERATION,
    RELEASE_KIND,
    NoticeClientCaptureError,
    build_notice_client_plan,
    capture_notice_client_contract,
    implementation_hashes,
    load_notice_client_capture,
    notice_client_authority,
    validate_notice_client_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.data_layout import DataReleaseReceipt, PhasePublisher
from futures_rebuild.errors import IntegrityError


REPO = Path(__file__).resolve().parents[1]
NOTICES_RELEASE_ID = (
    "1e417664f71bbc8197fd8918f5bdff2061e1849f26f95f5684df2e0e78b6a88b"
)
ARCHIVE_RELEASE_ID = (
    "10e1c3c567a6c960ba419be112d9448dcd0f92412434528d78a2bd06826c0248"
)
ASSESSMENT_RELATIVE = (
    "reports/exchange_calendar/"
    "cme_historical_advisory_archive_assessment_10e1c3c5.json"
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


def _copy_authority_evidence(boundary) -> tuple[Path, Path, Path]:
    active = boundary.active_root
    for release_id, filename in (
        (NOTICES_RELEASE_ID, "001-notices-page.html"),
        (ARCHIVE_RELEASE_ID, "001-advisory-archive.html"),
    ):
        _copy_file(
            active,
            f"manifests/data_releases/reference/{release_id}.json",
        )
        _copy_file(
            active,
            "data/reference/exchange_calendars/"
            f"{release_id}/{filename}",
        )
    _copy_file(active, ASSESSMENT_RELATIVE)
    notices_manifest = (
        active
        / "manifests"
        / "data_releases"
        / "reference"
        / f"{NOTICES_RELEASE_ID}.json"
    )
    archive_manifest = (
        active
        / "manifests"
        / "data_releases"
        / "reference"
        / f"{ARCHIVE_RELEASE_ID}.json"
    )
    archive_receipt = DataReleaseReceipt.from_manifest(
        archive_manifest, boundary
    )
    assessment_path = active / ASSESSMENT_RELATIVE
    assessment = json.loads(assessment_path.read_text(encoding="utf-8"))
    assessment["archive_receipt_id"] = archive_receipt.receipt_id
    assessment.pop("assessment_id")
    assessment["assessment_id"] = sha256_json(assessment)
    assessment_path.write_bytes(canonical_bytes(assessment) + b"\n")
    return (
        notices_manifest,
        archive_manifest,
        assessment_path,
    )


def _copy_implementation_closure(active: Path) -> None:
    for relative in IMPLEMENTATION_PATHS:
        _copy_file(active, relative)


def _authority(boundary) -> dict[str, object]:
    notices, archive, assessment = _copy_authority_evidence(boundary)
    return notice_client_authority(
        notices_manifest_path=notices,
        archive_manifest_path=archive,
        assessment_path=assessment,
        boundary=boundary,
    )


def _approval(plan: dict[str, object], plan_sha256: str) -> dict[str, object]:
    core = {
        "approved_at": "2026-07-27T03:30:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "e" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def test_notice_client_plan_is_offline_exact_and_hash_bound(
    boundary,
) -> None:
    authority = _authority(boundary)
    assert authority["client_assets"] == [dict(item) for item in CLIENT_ASSETS]
    plan = build_notice_client_plan(
        authority=authority,
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    assert validate_notice_client_plan(plan) == plan
    scope = plan["scope"]
    assert isinstance(scope, dict)
    assert scope["max_requests"] == 2
    assert scope["workers"] == 1
    assert scope["retries"] == 0
    assert [item["url"] for item in scope["requests"]] == [  # type: ignore[index]
        item["url"] for item in CLIENT_ASSETS
    ]
    drifted = json.loads(json.dumps(plan))
    drifted["scope"]["max_requests"] = 3
    with pytest.raises(IntegrityError):
        validate_notice_client_plan(drifted)


def test_notice_client_capture_rejects_bad_approval_before_network(
    boundary, operation_factory, monkeypatch
) -> None:
    plan = build_notice_client_plan(
        authority=_authority(boundary),
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    plan_path = boundary.active_root / "reports" / "notice-client-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, "0" * 64)
    approval_path = (
        boundary.active_root / "configs" / "notice-client-approval.json"
    )
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    monkeypatch.setattr(
        client_module.urllib.request,
        "build_opener",
        lambda *_args: pytest.fail("network opener constructed before approval"),
    )
    with pytest.raises(
        NoticeClientCaptureError, match="exact hash-bound approval"
    ):
        capture_notice_client_contract(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=_publisher(boundary, operation_factory),
        )


def test_notice_client_capture_publishes_only_exact_two_assets(
    boundary, operation_factory, monkeypatch
) -> None:
    authority = _authority(boundary)
    _copy_implementation_closure(boundary.active_root)
    plan = build_notice_client_plan(
        authority=authority,
        implementation_sha256=implementation_hashes(boundary.active_root),
    )
    plan_path = boundary.active_root / "reports" / "notice-client-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, _sha256_bytes(plan_path.read_bytes()))
    approval_path = (
        boundary.active_root / "configs" / "notice-client-approval.json"
    )
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    bodies = {
        str(CLIENT_ASSETS[0]["url"]): b"window.dynamicAlertList = {};\n",
        str(CLIENT_ASSETS[1]["url"]): b"window.searchSortFilter = {};\n",
    }
    content_types = {
        str(CLIENT_ASSETS[0]["url"]): "application/javascript",
        str(CLIENT_ASSETS[1]["url"]): "text/javascript",
    }
    opened: list[str] = []

    class FakeResponse:
        status = 200

        def __init__(self, url: str) -> None:
            self.url = url
            self.headers = Message()
            self.headers["Content-Type"] = (
                f"{content_types[url]}; charset=UTF-8"
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
        client_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
    )
    receipt = capture_notice_client_contract(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    payload = load_notice_client_capture(receipt, boundary=boundary)
    assert opened == [str(item["url"]) for item in CLIENT_ASSETS]
    assert receipt.release_kind == RELEASE_KIND
    assert receipt.schema_version == CAPTURE_SCHEMA
    assert payload["request_count"] == 2
    assert payload["total_bytes"] == sum(map(len, bodies.values()))
    for ordinal, asset in enumerate(CLIENT_ASSETS, start=1):
        filename = f"{ordinal:03d}-{asset['request_id']}.js"
        assert (
            receipt.resolve_unique_filename(filename, boundary).read_bytes()
            == bodies[str(asset["url"])]
        )
