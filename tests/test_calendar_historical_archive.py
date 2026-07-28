import hashlib
import json
from email.message import Message
from pathlib import Path

import pytest

import futures_rebuild.calendar_historical_archive as archive_module
from futures_rebuild.calendar_historical_archive import (
    APPROVAL_SCHEMA,
    ARCHIVE_URL,
    CAPTURE_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    RELEASE_KIND,
    HistoricalArchiveCaptureError,
    build_historical_archive_plan,
    capture_historical_archive_landing,
    historical_archive_authority,
    implementation_hashes,
    load_historical_archive_landing_capture,
    validate_historical_archive_plan,
)
from futures_rebuild.calendar_historical_discovery import (
    CAPTURE_SCHEMA as SOURCE_CAPTURE_SCHEMA,
    NOTICES_URL,
    OPERATION as SOURCE_OPERATION,
    RELEASE_KIND as SOURCE_RELEASE_KIND,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
)
from futures_rebuild.errors import IntegrityError


REPO = Path(__file__).resolve().parents[1]


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _publisher(boundary, operation_factory) -> PhasePublisher:
    return PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )


def _publish_notices_source(boundary, operation_factory):
    publisher = _publisher(boundary, operation_factory)
    stage = publisher.create_stage("notices_source")
    body = (
        b'<html><a href="/tools-information/advisory-archive.html">'
        b"Advisory archive</a></html>\n"
    )
    staged_name = "001-notices-page.html"
    logical_path = (
        "data/reference/exchange_calendars/001-notices-page.html"
    )
    (stage / staged_name).write_bytes(body)
    captured_at = "2026-07-27T03:01:49Z"
    response = {
        "content_type": "text/html",
        "logical_path": logical_path,
        "received_at_utc": captured_at,
        "request_id": "notices-page",
        "request_kind": "HISTORICAL_ARCHIVE_SOURCE_DISCOVERY",
        "safe_headers": {"content-type": "text/html"},
        "sha256": _sha256_bytes(body),
        "size": len(body),
        "status_code": 200,
        "url": NOTICES_URL,
    }
    source_authority = {
        "probe_release_id": "a" * 64,
        "probe_result_id": "b" * 64,
    }
    core = {
        "approval_receipt_id": "c" * 64,
        "authority": source_authority,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": 30,
            "max_requests": 1,
            "max_total_bytes": 8_388_608,
            "retries": 0,
            "workers": 1,
        },
        "capture_approval": {},
        "captured_at_utc": captured_at,
        "elapsed_milliseconds": 1,
        "operation": SOURCE_OPERATION,
        "plan_id": "d" * 64,
        "request_count": 1,
        "response": response,
        "schema_version": SOURCE_CAPTURE_SCHEMA,
        "total_bytes": len(body),
    }
    capture = {**core, "capture_id": sha256_json(core)}
    manifest = DataReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=SOURCE_RELEASE_KIND,
        schema_version=SOURCE_CAPTURE_SCHEMA,
        logical_paths={staged_name: logical_path},
        source_release_ids=("a" * 64,),
        embedded_documents={"capture_receipt.json": capture},
        metadata={
            "approval_receipt_id": "c" * 64,
            "capture_id": capture["capture_id"],
            "captured_at_utc": captured_at,
            "plan_id": "d" * 64,
            "probe_result_id": "b" * 64,
        },
    )
    manifest_path = publisher.publish(
        stage,
        manifest,
        staged_paths={logical_path: staged_name},
    )
    receipt = DataReleaseReceipt.from_manifest(manifest_path, boundary)
    candidate_core = {
        "capture_id": capture["capture_id"],
        "capture_manifest_sha256": receipt.manifest_sha256,
        "capture_receipt_id": receipt.receipt_id,
        "capture_release_id": receipt.release_id,
        "classification": (
            "AUTHORITATIVE_CME_ARCHIVE_LANDING_REFERENCE_DISCOVERED"
        ),
        "discovered_candidates": [
            {
                "evidence_kind": "ANCHOR_HREF",
                "role": "HISTORICAL_ADVISORY_ARCHIVE_LANDING",
                "url": ARCHIVE_URL,
            }
        ],
        "forbidden_interpretations": [],
        "next_authority": (
            "HASH_BOUND_CME_ADVISORY_ARCHIVE_LANDING_CAPTURE_"
            "APPROVAL_REQUIRED"
        ),
        "plan_id": "d" * 64,
        "probe_result_id": "b" * 64,
        "required_coverage_end_trade_date": "2026-07-13",
        "required_coverage_start_trade_date": "2010-06-06",
        "response_sha256": _sha256_bytes(body),
        "schema_version": "cme_historical_archive_source_candidates/1.0.0",
        "status": "ONE_EXACT_ARCHIVE_LANDING_CANDIDATE",
    }
    candidates = {
        **candidate_core,
        "result_id": sha256_json(candidate_core),
    }
    result_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "archive-candidates.json"
    )
    result_path.parent.mkdir(parents=True)
    result_path.write_bytes(canonical_bytes(candidates) + b"\n")
    return manifest_path, result_path


def _copy_implementation_closure(active: Path) -> None:
    for relative in IMPLEMENTATION_PATHS:
        source = REPO / relative
        target = active / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())


def _approval(plan: dict[str, object], plan_sha256: str) -> dict[str, object]:
    core = {
        "approved_at": "2026-07-27T03:10:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "e" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def test_archive_landing_plan_is_offline_exact_and_hash_bound(
    boundary, operation_factory
) -> None:
    manifest_path, result_path = _publish_notices_source(
        boundary, operation_factory
    )
    authority = historical_archive_authority(
        notices_manifest_path=manifest_path,
        candidate_result_path=result_path,
        boundary=boundary,
    )
    assert authority["archive_url"] == ARCHIVE_URL
    plan = build_historical_archive_plan(
        authority=authority,
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    assert validate_historical_archive_plan(plan) == plan
    assert plan["scope"]["max_requests"] == 1  # type: ignore[index]
    assert plan["scope"]["request"]["url"] == ARCHIVE_URL  # type: ignore[index]
    drifted = json.loads(json.dumps(plan))
    drifted["scope"]["request"]["url"] = NOTICES_URL  # type: ignore[index]
    with pytest.raises(IntegrityError):
        validate_historical_archive_plan(drifted)


def test_archive_landing_capture_rejects_bad_approval_before_network(
    boundary, operation_factory, monkeypatch
) -> None:
    manifest_path, result_path = _publish_notices_source(
        boundary, operation_factory
    )
    plan = build_historical_archive_plan(
        authority=historical_archive_authority(
            notices_manifest_path=manifest_path,
            candidate_result_path=result_path,
            boundary=boundary,
        ),
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    plan_path = boundary.active_root / "reports" / "archive-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, "0" * 64)
    approval_path = boundary.active_root / "configs" / "archive-approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    monkeypatch.setattr(
        archive_module.urllib.request,
        "build_opener",
        lambda *_args: pytest.fail("network opener constructed before approval"),
    )
    with pytest.raises(
        HistoricalArchiveCaptureError, match="exact hash-bound approval"
    ):
        capture_historical_archive_landing(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=_publisher(boundary, operation_factory),
        )


def test_archive_landing_capture_publishes_only_exact_page(
    boundary, operation_factory, monkeypatch
) -> None:
    manifest_path, result_path = _publish_notices_source(
        boundary, operation_factory
    )
    _copy_implementation_closure(boundary.active_root)
    authority = historical_archive_authority(
        notices_manifest_path=manifest_path,
        candidate_result_path=result_path,
        boundary=boundary,
    )
    plan = build_historical_archive_plan(
        authority=authority,
        implementation_sha256=implementation_hashes(boundary.active_root),
    )
    plan_path = boundary.active_root / "reports" / "archive-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, _sha256_bytes(plan_path.read_bytes()))
    approval_path = boundary.active_root / "configs" / "archive-approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    body = b"<html><p>Advisory archive structure</p></html>\n"
    opened: list[str] = []

    class FakeResponse:
        status = 200

        def __init__(self) -> None:
            self.headers = Message()
            self.headers["Content-Type"] = "text/html; charset=UTF-8"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return ARCHIVE_URL

        def read(self, maximum):
            assert maximum == 8_388_609
            return body

    class FakeOpener:
        def open(self, request, timeout):
            assert timeout == 30
            opened.append(request.full_url)
            return FakeResponse()

    monkeypatch.setattr(
        archive_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
    )
    receipt = capture_historical_archive_landing(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    payload = load_historical_archive_landing_capture(
        receipt, boundary=boundary
    )
    assert opened == [ARCHIVE_URL]
    assert receipt.release_kind == RELEASE_KIND
    assert receipt.schema_version == CAPTURE_SCHEMA
    assert payload["request_count"] == 1
    assert receipt.resolve_unique_filename(
        "001-advisory-archive.html", boundary
    ).read_bytes() == body
