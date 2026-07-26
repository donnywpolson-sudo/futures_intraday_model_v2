import json
from datetime import date, datetime, timezone

import pytest

import futures_rebuild.calendar_cli as calendar_cli_module
from futures_rebuild.calendar_cli import (
    CAPTURE_APPROVAL_SCHEMA,
    CAPTURE_OPERATION,
    build_capture_plan,
    capture_cme_calendar,
    validate_capture_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
)
from futures_rebuild.errors import ContractError, IntegrityError
from futures_rebuild.exchange_calendar import (
    ACTIVATION_APPROVAL_SCHEMA,
    CAPTURE_RELEASE_KIND,
    CAPTURE_SCHEMA_VERSION,
    CALENDAR_RELEASE_KIND,
    CME_TIMEZONE,
    CME_VENUE,
    MAPPING_APPROVAL_SCHEMA,
    PARSER_VERSION,
    SERVICE_RESPONSE_SCHEMA,
    VerifiedExchangeCalendar,
    coverage_matches_active_index,
    diff_exchange_calendars,
    generate_mapping_candidates,
    load_calendar_index,
    load_cme_capture,
    load_foundation_calendar_coverage,
    publish_calendar_index,
    publish_foundation_calendar_coverage,
    publish_verified_exchange_calendar,
    verify_calendar_freshness,
)


UTC = timezone.utc


def _publisher(boundary, operation_factory) -> PhasePublisher:
    return PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "calendar.lock",
    )


def _capture(
    boundary,
    operation_factory,
    *,
    response: dict,
    start: str,
    end: str,
    retrieved_at_utc: str = "2026-07-25T12:00:00Z",
) -> DataReleaseReceipt:
    publisher = _publisher(boundary, operation_factory)
    stage = publisher.create_stage("calendar_capture")
    source = stage / "schedule.json"
    source.write_bytes(canonical_bytes(response) + b"\n")
    logical = "data/reference/exchange_calendars/schedule.json"
    response_receipt = {
        "content_type": "application/json",
        "logical_path": logical,
        "received_at_utc": retrieved_at_utc,
        "request_id": "schedule-001-p1",
        "request_kind": "SCHEDULE",
        "safe_headers": {"content-type": "application/json"},
        "sha256": __import__("hashlib").sha256(source.read_bytes()).hexdigest(),
        "size": source.stat().st_size,
        "status_code": 200,
        "url": "https://www.cmegroup.com/services/trading-hours-by-product?pageNumber=1",
    }
    core = {
        "approval_receipt_id": "a" * 64,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": 900,
            "max_requests": 40,
            "max_total_bytes": 268_435_456,
            "retries": 0,
            "workers": 1,
        },
        "coverage_end_trade_date": end,
        "coverage_start_trade_date": start,
        "elapsed_milliseconds": 0,
        "mode": "STEADY_STATE",
        "parser_version": PARSER_VERSION,
        "plan_id": "b" * 64,
        "predecessor_capture_release_id": None,
        "request_count": 1,
        "responses": [response_receipt],
        "retrieved_at_utc": retrieved_at_utc,
        "schema_version": CAPTURE_SCHEMA_VERSION,
        "total_bytes": source.stat().st_size,
    }
    capture_payload = {**core, "capture_id": sha256_json(core)}
    manifest = DataReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=CAPTURE_RELEASE_KIND,
        schema_version=CAPTURE_SCHEMA_VERSION,
        logical_paths={"schedule.json": logical},
        embedded_documents={"capture_receipt.json": capture_payload},
        metadata={
            "approval_receipt_id": "a" * 64,
            "capture_id": capture_payload["capture_id"],
            "coverage_end_trade_date": end,
            "coverage_start_trade_date": start,
            "parser_version": PARSER_VERSION,
            "plan_id": "b" * 64,
            "retrieved_at_utc": retrieved_at_utc,
        },
    )
    manifest_path = publisher.publish(
        stage, manifest, staged_paths={logical: "schedule.json"}
    )
    receipt = DataReleaseReceipt.from_manifest(manifest_path, boundary)
    load_cme_capture(receipt, boundary=boundary)
    return receipt


def _mapping(capture_release_id: str, mappings: list[dict[str, str]]) -> dict:
    core = {
        "approved_at": "2026-07-25T12:01:00Z",
        "capture_release_id": capture_release_id,
        "mappings": mappings,
        "operation": "APPROVE_CME_PRODUCT_MAPPING",
        "schema_version": MAPPING_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "c" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def _activation(
    *,
    calendar_release_id: str,
    predecessor_index_release_id: str | None,
    diff_report_id: str,
) -> dict:
    core = {
        "approved_at": "2026-07-25T12:02:00Z",
        "candidate_calendar_release_id": calendar_release_id,
        "diff_report_id": diff_report_id,
        "operation": "ACTIVATE_CME_EXCHANGE_CALENDAR",
        "predecessor_index_release_id": predecessor_index_release_id,
        "schema_version": ACTIVATION_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "d" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def _service_response(
    sessions: list[dict],
    *,
    code: str = "138",
    state_override: str | None = None,
) -> dict:
    if state_override is not None:
        sessions = json.loads(json.dumps(sessions))
        sessions[0]["intervals"][0]["state"] = state_override
    return {
        "holiday_notices": [],
        "pagination": {"page_number": 1, "total_pages": 1},
        "products": [
            {
                "cme_product_code": code,
                "cme_product_name": "E-mini S&P 500",
                "product_group": "Equity Index",
                "product_type": "Futures",
                "sessions": sessions,
                "venue": CME_VENUE,
            }
        ],
        "schema_version": SERVICE_RESPONSE_SCHEMA,
        "timezone": CME_TIMEZONE,
    }


def _regular_session(trade_date: str) -> dict:
    current = date.fromisoformat(trade_date)
    previous = current.fromordinal(current.toordinal() - 1)
    return {
        "intervals": [
            {
                "ends_at_local": f"{trade_date}T16:00:00",
                "starts_at_local": f"{previous.isoformat()}T17:00:00",
                "state": "OPEN",
            },
            {
                "ends_at_local": f"{trade_date}T17:00:00",
                "starts_at_local": f"{trade_date}T16:00:00",
                "state": "CLOSED",
            },
        ],
        "trade_date": trade_date,
    }


def _publish_calendar(
    boundary,
    operation_factory,
    *,
    sessions: list[dict],
    expected_markets: tuple[str, ...] = ("ES",),
    mappings: list[dict[str, str]] | None = None,
    retrieved_at_utc: str = "2026-07-25T12:00:00Z",
    predecessor_calendar_receipt: DataReleaseReceipt | None = None,
) -> tuple[DataReleaseReceipt, VerifiedExchangeCalendar]:
    response = _service_response(sessions)
    capture = _capture(
        boundary,
        operation_factory,
        response=response,
        start=sessions[0]["trade_date"],
        end=sessions[-1]["trade_date"],
        retrieved_at_utc=retrieved_at_utc,
    )
    approval = _mapping(
        capture.release_id,
        mappings
        or [{"cme_product_code": "138", "market": expected_markets[0]}],
    )
    calendar_receipt = publish_verified_exchange_calendar(
        capture_receipt=capture,
        mapping_approval=approval,
        expected_markets=expected_markets,
        publisher=_publisher(boundary, operation_factory),
        predecessor_calendar_receipt=predecessor_calendar_receipt,
    )
    assert calendar_receipt.release_kind == CALENDAR_RELEASE_KIND
    calendar = VerifiedExchangeCalendar.from_release(
        calendar_receipt,
        boundary=boundary,
        expected_markets=expected_markets,
    )
    return calendar_receipt, calendar


def test_capture_plan_is_exact_and_bounded() -> None:
    plan = build_capture_plan(
        mode="bootstrap",
        coverage_start=date(2026, 7, 25),
        coverage_end=date(2026, 10, 22),
    )
    assert validate_capture_plan(plan) == plan
    scope = plan["scope"]
    assert scope["max_requests"] == 96
    assert len(scope["requests"]) <= 96
    assert scope["workers"] == 1
    assert scope["retries"] == 0
    assert scope["allow_redirects"] is False


def test_capture_enforces_content_types_and_sanitizes_response_headers(
    boundary, operation_factory, monkeypatch, tmp_path
) -> None:
    plan = build_capture_plan(
        mode="bootstrap",
        coverage_start=date(2026, 7, 25),
        coverage_end=date(2026, 7, 25),
    )
    plan_path = tmp_path / "plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval_core = {
        "approved_at": "2026-07-25T12:00:00Z",
        "operation": CAPTURE_OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "schema_version": CAPTURE_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "1" * 64,
    }
    approval = {
        **approval_core,
        "approval_receipt_id": sha256_json(approval_core),
    }
    approval_path = tmp_path / "approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    schedule = canonical_bytes(
        _service_response([_regular_session("2026-07-25")])
    ) + b"\n"

    class Headers(dict):
        def get_content_type(self) -> str:
            return str(self["content-type"])

    class Response:
        status = 200

        def __init__(self, url: str, content_type: str, body: bytes) -> None:
            self._url = url
            self.headers = Headers(
                {
                    "content-type": content_type,
                    "etag": '"fixture"',
                    "set-cookie": "must-not-be-retained",
                }
            )
            self._body = body

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def geturl(self) -> str:
            return self._url

        def read(self, maximum: int) -> bytes:
            return self._body[:maximum]

    class Opener:
        def open(self, request, timeout: int):
            assert timeout == 30
            url = request.full_url
            if url.endswith("trading-hours.html"):
                return Response(url, "text/html", b"<html>fixture</html>")
            if "trading-hours-filters" in url:
                return Response(url, "application/json", b"{}\n")
            return Response(url, "application/json", schedule)

    monkeypatch.setattr(
        calendar_cli_module.urllib.request,
        "build_opener",
        lambda *_handlers: Opener(),
    )
    receipt = capture_cme_calendar(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    capture = load_cme_capture(receipt, boundary=boundary)
    assert capture["request_count"] == 3
    assert capture["bounds"]["max_requests"] == 96
    assert all(
        "set-cookie" not in response["safe_headers"]
        for response in capture["responses"]
    )


def test_steady_state_plan_requires_exact_41_product_ids() -> None:
    with pytest.raises(ContractError, match="exactly 41"):
        build_capture_plan(
            mode="steady-state",
            coverage_start=date(2026, 7, 25),
            coverage_end=date(2026, 7, 27),
            product_ids=("138",),
        )


def test_calendar_regular_early_close_pause_and_full_closure(
    boundary, operation_factory
) -> None:
    sessions = [
        _regular_session("2026-07-06"),
        {
            "intervals": [
                {
                    "ends_at_local": "2026-07-07T12:00:00",
                    "starts_at_local": "2026-07-06T17:00:00",
                    "state": "OPEN",
                },
                {
                    "ends_at_local": "2026-07-07T17:00:00",
                    "starts_at_local": "2026-07-07T12:00:00",
                    "state": "CLOSED",
                },
            ],
            "trade_date": "2026-07-07",
        },
        {
            "intervals": [
                {
                    "ends_at_local": "2026-07-08T12:00:00",
                    "starts_at_local": "2026-07-07T17:00:00",
                    "state": "OPEN",
                },
                {
                    "ends_at_local": "2026-07-08T13:00:00",
                    "starts_at_local": "2026-07-08T12:00:00",
                    "state": "PAUSED",
                },
                {
                    "ends_at_local": "2026-07-08T17:00:00",
                    "starts_at_local": "2026-07-08T13:00:00",
                    "state": "OPEN",
                },
            ],
            "trade_date": "2026-07-08",
        },
        {
            "intervals": [
                {
                    "ends_at_local": "2026-07-09T17:00:00",
                    "starts_at_local": "2026-07-08T17:00:00",
                    "state": "CLOSED",
                }
            ],
            "trade_date": "2026-07-09",
        },
    ]
    _, calendar = _publish_calendar(
        boundary, operation_factory, sessions=sessions
    )
    assert (
        calendar.state_at(
            "ES",
            datetime(2026, 7, 7, 18, tzinfo=UTC),
            trade_date=date(2026, 7, 7),
        )
        == "CLOSED"
    )
    assert (
        calendar.state_at(
            "ES",
            datetime(2026, 7, 8, 17, 30, tzinfo=UTC),
            trade_date=date(2026, 7, 8),
        )
        == "PAUSED"
    )
    assert calendar.trading_intervals("ES", date(2026, 7, 9)) == ()


def test_dst_session_uses_zoneinfo_and_has_exact_23_hour_window(
    boundary, operation_factory
) -> None:
    session = {
        "intervals": [
            {
                "ends_at_local": "2026-03-08T17:00:00",
                "starts_at_local": "2026-03-07T17:00:00",
                "state": "CLOSED",
            }
        ],
        "trade_date": "2026-03-08",
    }
    _, calendar = _publish_calendar(
        boundary, operation_factory, sessions=[session]
    )
    interval = calendar.sessions[("ES", date(2026, 3, 8))].intervals[0]
    assert interval.ends_at_utc - interval.starts_at_utc == pytest.approx(
        __import__("datetime").timedelta(hours=23)
    )


def test_unknown_state_and_ambiguous_mapping_fail_closed(
    boundary, operation_factory
) -> None:
    response = _service_response(
        [_regular_session("2026-07-06")], state_override="AUCTION"
    )
    capture = _capture(
        boundary,
        operation_factory,
        response=response,
        start="2026-07-06",
        end="2026-07-06",
    )
    with pytest.raises(IntegrityError, match="unsupported"):
        publish_verified_exchange_calendar(
            capture_receipt=capture,
            mapping_approval=_mapping(
                capture.release_id,
                [{"cme_product_code": "138", "market": "ES"}],
            ),
            expected_markets=("ES",),
            publisher=_publisher(boundary, operation_factory),
        )
    valid_capture = _capture(
        boundary,
        operation_factory,
        response=_service_response([_regular_session("2026-07-06")]),
        start="2026-07-06",
        end="2026-07-06",
    )
    with pytest.raises(IntegrityError, match="incomplete or ambiguous"):
        publish_verified_exchange_calendar(
            capture_receipt=valid_capture,
            mapping_approval=_mapping(
                valid_capture.release_id,
                [
                    {"cme_product_code": "138", "market": "ES"},
                    {"cme_product_code": "138", "market": "NQ"},
                ],
            ),
            expected_markets=("ES", "NQ"),
            publisher=_publisher(boundary, operation_factory),
        )


def test_mapping_candidates_are_hash_bound_and_pagination_overflow_fails_closed(
    boundary, operation_factory
) -> None:
    response = _service_response([_regular_session("2026-07-06")])
    capture = _capture(
        boundary,
        operation_factory,
        response=response,
        start="2026-07-06",
        end="2026-07-06",
    )
    candidates = generate_mapping_candidates(
        capture,
        boundary=boundary,
        expected_markets=("ES",),
    )
    assert candidates["capture_release_id"] == capture.release_id
    assert candidates["status"] == "REVIEW_REQUIRED_NO_MAPPING_AUTHORITY"
    assert candidates["mapping_candidates_id"] == sha256_json(
        {
            key: value
            for key, value in candidates.items()
            if key != "mapping_candidates_id"
        }
    )

    overflow = json.loads(json.dumps(response))
    overflow["pagination"]["total_pages"] = 2
    overflow_capture = _capture(
        boundary,
        operation_factory,
        response=overflow,
        start="2026-07-06",
        end="2026-07-06",
    )
    with pytest.raises(IntegrityError, match="pagination overflowed"):
        generate_mapping_candidates(
            overflow_capture,
            boundary=boundary,
            expected_markets=("ES",),
        )


def test_index_coverage_and_freshness_are_dependency_closed(
    boundary, operation_factory
) -> None:
    calendar_receipt, calendar = _publish_calendar(
        boundary,
        operation_factory,
        sessions=[_regular_session("2026-07-25")],
    )
    diff = diff_exchange_calendars(None, calendar)
    index_receipt = publish_calendar_index(
        candidate_calendar_receipt=calendar_receipt,
        activation_approval=_activation(
            calendar_release_id=calendar_receipt.release_id,
            predecessor_index_release_id=None,
            diff_report_id=diff["diff_report_id"],
        ),
        publisher=_publisher(boundary, operation_factory),
        expected_markets=("ES",),
    )
    index = load_calendar_index(
        index_receipt, boundary=boundary, expected_markets=("ES",)
    )
    freshness = verify_calendar_freshness(
        index,
        expected_markets=("ES",),
        now=datetime(2026, 7, 25, 13, tzinfo=UTC),
        minimum_forward_days=1,
    )
    assert freshness["status"] == "CURRENT"
    interval = {
        "end": "2026-07-26",
        "interval_key": "ES:2026-07-25:2026-07-26",
        "market": "ES",
        "start": "2026-07-25",
    }
    coverage_receipt = publish_foundation_calendar_coverage(
        index_receipt=index_receipt,
        intervals=[interval],
        publisher=_publisher(boundary, operation_factory),
        expected_markets=("ES",),
    )
    coverage = load_foundation_calendar_coverage(
        coverage_receipt,
        boundary=boundary,
        expected_markets=("ES",),
        expected_intervals=[interval],
    )
    assert coverage.calendar_for("ES", date(2026, 7, 25)).calendar_id == (
        calendar.calendar_id
    )


def test_calendar_correction_invalidates_only_intersecting_foundation_dates(
    boundary, operation_factory
) -> None:
    base_sessions = [
        _regular_session("2026-07-25"),
        _regular_session("2026-07-26"),
    ]
    base_receipt, base = _publish_calendar(
        boundary,
        operation_factory,
        sessions=base_sessions,
    )
    base_diff = diff_exchange_calendars(None, base)
    base_index_receipt = publish_calendar_index(
        candidate_calendar_receipt=base_receipt,
        activation_approval=_activation(
            calendar_release_id=base_receipt.release_id,
            predecessor_index_release_id=None,
            diff_report_id=str(base_diff["diff_report_id"]),
        ),
        publisher=_publisher(boundary, operation_factory),
        expected_markets=("ES",),
    )
    coverage_receipt = publish_foundation_calendar_coverage(
        index_receipt=base_index_receipt,
        intervals=[
            {
                "end": "2026-07-26",
                "interval_key": "ES:2026-07-25:2026-07-26",
                "market": "ES",
                "start": "2026-07-25",
            }
        ],
        publisher=_publisher(boundary, operation_factory),
        expected_markets=("ES",),
    )
    coverage = load_foundation_calendar_coverage(
        coverage_receipt,
        boundary=boundary,
        expected_markets=("ES",),
    )

    future_change = json.loads(json.dumps(base_sessions))
    future_change[1]["intervals"] = [
        {
            "ends_at_local": "2026-07-26T12:00:00",
            "starts_at_local": "2026-07-25T17:00:00",
            "state": "OPEN",
        },
        {
            "ends_at_local": "2026-07-26T17:00:00",
            "starts_at_local": "2026-07-26T12:00:00",
            "state": "CLOSED",
        },
    ]
    future_receipt, future_calendar = _publish_calendar(
        boundary,
        operation_factory,
        sessions=future_change,
        predecessor_calendar_receipt=base_receipt,
    )
    future_diff = diff_exchange_calendars(base, future_calendar)
    future_index_receipt = publish_calendar_index(
        candidate_calendar_receipt=future_receipt,
        activation_approval=_activation(
            calendar_release_id=future_receipt.release_id,
            predecessor_index_release_id=base_index_receipt.release_id,
            diff_report_id=str(future_diff["diff_report_id"]),
        ),
        publisher=_publisher(boundary, operation_factory),
        expected_markets=("ES",),
        predecessor_index_receipt=base_index_receipt,
    )
    future_index = load_calendar_index(
        future_index_receipt,
        boundary=boundary,
        expected_markets=("ES",),
    )
    assert coverage_matches_active_index(coverage, future_index)

    intersecting_change = json.loads(json.dumps(future_change))
    intersecting_change[0]["intervals"] = [
        {
            "ends_at_local": "2026-07-25T11:00:00",
            "starts_at_local": "2026-07-24T17:00:00",
            "state": "OPEN",
        },
        {
            "ends_at_local": "2026-07-25T17:00:00",
            "starts_at_local": "2026-07-25T11:00:00",
            "state": "CLOSED",
        },
    ]
    changed_receipt, changed_calendar = _publish_calendar(
        boundary,
        operation_factory,
        sessions=intersecting_change,
        predecessor_calendar_receipt=future_receipt,
    )
    changed_diff = diff_exchange_calendars(future_calendar, changed_calendar)
    changed_index_receipt = publish_calendar_index(
        candidate_calendar_receipt=changed_receipt,
        activation_approval=_activation(
            calendar_release_id=changed_receipt.release_id,
            predecessor_index_release_id=future_index_receipt.release_id,
            diff_report_id=str(changed_diff["diff_report_id"]),
        ),
        publisher=_publisher(boundary, operation_factory),
        expected_markets=("ES",),
        predecessor_index_receipt=future_index_receipt,
    )
    changed_index = load_calendar_index(
        changed_index_receipt,
        boundary=boundary,
        expected_markets=("ES",),
    )
    assert not coverage_matches_active_index(coverage, changed_index)
