import hashlib
import json
from email.message import Message
from pathlib import Path

import pytest

import futures_rebuild.calendar_historical_discovery as discovery_module
from futures_rebuild.calendar_cli import CAPTURE_APPROVAL_SCHEMA, CAPTURE_OPERATION
from futures_rebuild.calendar_historical_discovery import (
    APPROVAL_SCHEMA,
    CAPTURE_SCHEMA,
    IMPLEMENTATION_PATHS,
    NOTICES_URL,
    OPERATION,
    RELEASE_KIND,
    HistoricalSourceDiscoveryError,
    build_historical_source_discovery_plan,
    capture_historical_source_discovery,
    historical_source_authority,
    implementation_hashes,
    load_historical_source_discovery_capture,
    validate_historical_source_discovery_approval,
    validate_historical_source_discovery_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
)
from futures_rebuild.exchange_calendar import (
    CAPTURE_RELEASE_KIND,
    CAPTURE_SCHEMA_VERSION,
    PARSER_VERSION,
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


def _publish_probe(boundary, operation_factory):
    publisher = _publisher(boundary, operation_factory)
    stage = publisher.create_stage("historical_probe")
    landing = (
        b'<html><a href="/notices.html">Notices</a>'
        b'<div data-links="[{&quot;linkUrl&quot;:&quot;/notices.html&quot;}]">'
        b"</div></html>\n"
    )
    filters = b'{"filters":[]}\n'
    schedule = (
        b'{"products":[],"props":{"hasEvents":false,"total":41}}\n'
    )
    files = {
        "landing.html": landing,
        "filters.json": filters,
        "schedule.json": schedule,
    }
    logical_paths = {
        name: f"data/reference/exchange_calendars/{name}"
        for name in files
    }
    for name, content in files.items():
        (stage / name).write_bytes(content)
    received = "2026-07-27T02:42:23Z"
    responses = [
        {
            "content_type": "text/html",
            "logical_path": logical_paths["landing.html"],
            "received_at_utc": received,
            "request_id": "landing-page",
            "request_kind": "LANDING_PAGE",
            "safe_headers": {"content-type": "text/html"},
            "sha256": _sha256_bytes(landing),
            "size": len(landing),
            "status_code": 200,
            "url": "https://www.cmegroup.com/trading-hours.html",
        },
        {
            "content_type": "application/json",
            "logical_path": logical_paths["filters.json"],
            "received_at_utc": received,
            "request_id": "filters",
            "request_kind": "FILTERS",
            "safe_headers": {"content-type": "application/json"},
            "sha256": _sha256_bytes(filters),
            "size": len(filters),
            "status_code": 200,
            "url": (
                "https://www.cmegroup.com/services/"
                "trading-hours-filters?isProtected"
            ),
        },
        {
            "content_type": "application/json",
            "logical_path": logical_paths["schedule.json"],
            "received_at_utc": received,
            "request_id": "schedule-001-p1",
            "request_kind": "SCHEDULE",
            "safe_headers": {"content-type": "application/json"},
            "sha256": _sha256_bytes(schedule),
            "size": len(schedule),
            "status_code": 200,
            "url": (
                "https://www.cmegroup.com/services/"
                "trading-hours-by-product?isProtected&pageNumber=1"
            ),
        },
    ]
    approval_core = {
        "approved_at": received,
        "operation": CAPTURE_OPERATION,
        "plan_id": "a" * 64,
        "plan_sha256": "b" * 64,
        "schema_version": CAPTURE_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "c" * 64,
    }
    capture_approval = {
        **approval_core,
        "approval_receipt_id": sha256_json(approval_core),
    }
    capture_core = {
        "approval_receipt_id": capture_approval["approval_receipt_id"],
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": 900,
            "max_requests": 40,
            "max_total_bytes": 268_435_456,
            "retries": 0,
            "workers": 1,
        },
        "capture_approval": capture_approval,
        "coverage_end_trade_date": "2010-01-04",
        "coverage_start_trade_date": "2010-01-04",
        "elapsed_milliseconds": 1,
        "mode": "STEADY_STATE",
        "parser_version": PARSER_VERSION,
        "plan_id": "a" * 64,
        "predecessor_capture_release_id": "d" * 64,
        "request_count": 3,
        "responses": responses,
        "retrieved_at_utc": received,
        "schema_version": CAPTURE_SCHEMA_VERSION,
        "total_bytes": sum(len(value) for value in files.values()),
    }
    capture = {**capture_core, "capture_id": sha256_json(capture_core)}
    manifest = DataReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=CAPTURE_RELEASE_KIND,
        schema_version=CAPTURE_SCHEMA_VERSION,
        logical_paths=logical_paths,
        source_release_ids=("d" * 64,),
        embedded_documents={"capture_receipt.json": capture},
        metadata={
            "approval_receipt_id": capture_approval["approval_receipt_id"],
            "capture_id": capture["capture_id"],
            "coverage_end_trade_date": "2010-01-04",
            "coverage_start_trade_date": "2010-01-04",
            "parser_version": PARSER_VERSION,
            "plan_id": "a" * 64,
            "retrieved_at_utc": received,
        },
    )
    manifest_path = publisher.publish(
        stage,
        manifest,
        staged_paths={logical_paths[name]: name for name in files},
    )
    receipt = DataReleaseReceipt.from_manifest(manifest_path, boundary)
    result_core = {
        "approval_receipt_id": capture_approval["approval_receipt_id"],
        "capture_id": capture["capture_id"],
        "capture_manifest_sha256": receipt.manifest_sha256,
        "capture_release_id": receipt.release_id,
        "classification": "NEGATIVE_CAPABILITY_EVIDENCE",
        "conclusion": (
            "CURRENT_TRADING_HOURS_ENDPOINT_DID_NOT_RETURN_"
            "AUTHORITATIVE_HISTORICAL_SESSION_EVENTS"
        ),
        "evidence": {
            "event_dates": ["2010-01-03", "2010-01-04", "2010-01-05"],
            "long_lived_market_samples": {},
            "product_count": 41,
            "props_has_events": False,
            "schedule_event_count": 0,
            "schedule_response_sha256": _sha256_bytes(schedule),
            "schedule_shell_count": 123,
        },
        "forbidden_interpretations": [],
        "foundation_impact": {
            "active_calendar_covered_interval_count": 0,
            "foundation_interval_count": 683,
            "required_coverage_end_trade_date": "2026-07-13",
            "required_coverage_start_trade_date": "2010-06-06",
            "status": "BLOCKED_MISSING_HISTORICAL_CME_BYTES",
        },
        "next_authority": (
            "AUTHORITATIVE_CME_HISTORICAL_SOURCE_OR_HASH_BOUND_"
            "ARCHIVE_DISCOVERY_APPROVAL_REQUIRED"
        ),
        "plan_id": "a" * 64,
        "plan_sha256": "b" * 64,
        "probe_date": "2010-01-04",
        "request_count": 3,
        "schema_version": (
            "cme_historical_schedule_capability_probe_result/1.0.0"
        ),
        "status": "PROBE_COMPLETED_NO_HISTORICAL_EVENTS",
    }
    result = {**result_core, "result_id": sha256_json(result_core)}
    result_path = (
        boundary.active_root
        / "reports"
        / "exchange_calendar"
        / "probe-result.json"
    )
    result_path.parent.mkdir(parents=True)
    result_path.write_bytes(canonical_bytes(result) + b"\n")
    return manifest_path, result_path


def _copy_implementation_closure(active: Path) -> None:
    for relative in IMPLEMENTATION_PATHS:
        source = REPO / relative
        target = active / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())


def _approval(plan: dict[str, object], plan_sha256: str) -> dict[str, object]:
    core = {
        "approved_at": "2026-07-27T03:00:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "e" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def test_historical_source_plan_is_offline_exact_and_hash_bound(
    boundary, operation_factory
) -> None:
    manifest_path, result_path = _publish_probe(boundary, operation_factory)
    authority = historical_source_authority(
        probe_manifest_path=manifest_path,
        probe_result_path=result_path,
        boundary=boundary,
    )
    assert authority["discovered_link_url"] == NOTICES_URL
    implementation = {
        relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
    }
    plan = build_historical_source_discovery_plan(
        authority=authority,
        implementation_sha256=implementation,
    )
    assert validate_historical_source_discovery_plan(plan) == plan
    assert plan["scope"]["max_requests"] == 1  # type: ignore[index]
    assert plan["scope"]["request"]["url"] == NOTICES_URL  # type: ignore[index]
    drifted = json.loads(json.dumps(plan))
    drifted["scope"]["request"]["url"] = "https://www.cmegroup.com/"  # type: ignore[index]
    with pytest.raises(IntegrityError):
        validate_historical_source_discovery_plan(drifted)


def test_historical_source_capture_requires_exact_approval_before_network(
    boundary, operation_factory, monkeypatch
) -> None:
    manifest_path, result_path = _publish_probe(boundary, operation_factory)
    plan = build_historical_source_discovery_plan(
        authority=historical_source_authority(
            probe_manifest_path=manifest_path,
            probe_result_path=result_path,
            boundary=boundary,
        ),
        implementation_sha256={
            relative: "f" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    plan_path = boundary.active_root / "reports" / "plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, "0" * 64)
    approval_path = boundary.active_root / "configs" / "approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    monkeypatch.setattr(
        discovery_module.urllib.request,
        "build_opener",
        lambda *_args: pytest.fail("network opener constructed before approval"),
    )
    with pytest.raises(
        HistoricalSourceDiscoveryError, match="exact hash-bound approval"
    ):
        capture_historical_source_discovery(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=_publisher(boundary, operation_factory),
        )


def test_historical_source_capture_publishes_only_the_exact_notices_page(
    boundary, operation_factory, monkeypatch
) -> None:
    manifest_path, result_path = _publish_probe(boundary, operation_factory)
    _copy_implementation_closure(boundary.active_root)
    authority = historical_source_authority(
        probe_manifest_path=manifest_path,
        probe_result_path=result_path,
        boundary=boundary,
    )
    plan = build_historical_source_discovery_plan(
        authority=authority,
        implementation_sha256=implementation_hashes(boundary.active_root),
    )
    plan_path = boundary.active_root / "reports" / "discovery-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval = _approval(plan, _sha256_bytes(plan_path.read_bytes()))
    approval_path = boundary.active_root / "configs" / "discovery-approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    body = b"<html><p>Historical notices</p></html>\n"
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
            return NOTICES_URL

        def read(self, maximum):
            assert maximum == 8_388_609
            return body

    class FakeOpener:
        def open(self, request, timeout):
            assert timeout == 30
            opened.append(request.full_url)
            return FakeResponse()

    monkeypatch.setattr(
        discovery_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
    )
    receipt = capture_historical_source_discovery(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    payload = load_historical_source_discovery_capture(
        receipt, boundary=boundary
    )
    assert opened == [NOTICES_URL]
    assert receipt.release_kind == RELEASE_KIND
    assert receipt.schema_version == CAPTURE_SCHEMA
    assert payload["request_count"] == 1
    assert receipt.resolve_unique_filename(
        "001-notices-page.html", boundary
    ).read_bytes() == body


def test_historical_source_approval_rejects_plan_hash_drift() -> None:
    authority = {
        "discovered_link_url": NOTICES_URL,
        "landing_logical_path": "data/reference/exchange_calendars/landing.html",
        "landing_response_sha256": "a" * 64,
        "landing_response_size": 1,
        "landing_source_url": "https://www.cmegroup.com/trading-hours.html",
        "probe_capture_id": "b" * 64,
        "probe_manifest_path": "manifests/data_releases/reference/x.json",
        "probe_manifest_sha256": "c" * 64,
        "probe_receipt_id": "d" * 64,
        "probe_release_id": "e" * 64,
        "probe_result_id": "f" * 64,
        "probe_result_path": "reports/exchange_calendar/result.json",
        "probe_result_sha256": "0" * 64,
    }
    plan = build_historical_source_discovery_plan(
        authority=authority,
        implementation_sha256={
            relative: "1" * 64 for relative in IMPLEMENTATION_PATHS
        },
    )
    approval = _approval(plan, "2" * 64)
    with pytest.raises(
        HistoricalSourceDiscoveryError, match="exact hash-bound approval"
    ):
        validate_historical_source_discovery_approval(
            approval, plan=plan, plan_sha256="3" * 64
        )
