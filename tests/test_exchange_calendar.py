import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import futures_rebuild.calendar_cli as calendar_cli_module
import futures_rebuild.exchange_calendar as exchange_calendar_module
from futures_rebuild.calendar_cli import (
    CAPTURE_APPROVAL_SCHEMA,
    CAPTURE_FORBIDDEN_ACTIONS,
    CAPTURE_IMPLEMENTATION_PATHS,
    CAPTURE_OPERATION,
    CAPTURE_OUTPUT_PATHS,
    CAPTURE_STOP_CONDITIONS,
    CLIENT_CONTRACT_APPROVAL_SCHEMA,
    CLIENT_CONTRACT_FORBIDDEN_ACTIONS,
    CLIENT_CONTRACT_MAX_BYTES,
    CLIENT_CONTRACT_OPERATION,
    CLIENT_CONTRACT_STOP_CONDITIONS,
    CLIENT_DEPENDENCY_MAX_BYTES,
    CLIENT_DEPENDENCY_APPROVAL_SCHEMA,
    CLIENT_DEPENDENCY_OPERATION,
    NONEMPTY_DISCOVERY_GROUP_BY_MARKET,
    NONEMPTY_DISCOVERY_OPERATION,
    SCHEDULE_RECOVERY_PLAN_SCHEMA,
    _schedule_recovery_requests,
    _client_contract_authority,
    _client_dependency_authority,
    build_calendar_activation_plan,
    build_capture_plan,
    build_client_contract_plan,
    build_client_dependency_plan,
    build_nonempty_discovery_plan,
    capture_cme_calendar,
    capture_schedule_coverage_recovery,
    capture_client_contract,
    capture_client_dependency,
    load_client_contract_capture,
    load_client_dependency_capture,
    parse_client_contract_candidates,
    parse_client_dependency_candidates,
    validate_capture_plan,
    validate_calendar_activation_plan,
    validate_client_contract_plan,
    validate_client_dependency_plan,
    validate_nonempty_discovery_plan,
    validate_schedule_coverage_recovery_approval,
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
    SCHEDULE_RECOVERY_APPROVAL_SCHEMA,
    SCHEDULE_RECOVERY_CAPTURE_SCHEMA_VERSION,
    SCHEDULE_RECOVERY_OPERATION,
    SERVICE_RESPONSE_SCHEMA,
    VerifiedExchangeCalendar,
    approved_research_markets,
    coverage_matches_active_index,
    diff_exchange_calendars,
    generate_mapping_candidates,
    load_calendar_index,
    load_cme_capture,
    load_exchange_calendar_policy,
    load_foundation_calendar_coverage,
    publish_calendar_index,
    publish_foundation_calendar_coverage,
    publish_verified_exchange_calendar,
    verify_calendar_freshness,
)


UTC = timezone.utc
REPO = Path(__file__).resolve().parents[1]


def test_exchange_calendar_policy_separates_future_and_history_roles() -> None:
    policy = load_exchange_calendar_policy(
        REPO / "configs" / "exchange_calendar_policy.json"
    )
    assert policy["contract_version"] == "2.0.0"
    assert (
        policy["historical_backfill_policy"]
        == "DBN_EMPIRICAL_OBSERVABILITY_NO_OFFICIAL_CALENDAR_CLAIM"
    )
    assert (
        policy["roles"]["current_forward"]
        == "AUTHORITATIVE_CME_CALENDAR_FOR_COCKPIT_AND_FORWARD_SCHEDULING"
    )


def _implementation_hashes() -> dict[str, str]:
    return {
        relative: sha256_file(REPO / relative)
        for relative in CAPTURE_IMPLEMENTATION_PATHS
    }


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
    landing_bytes: bytes = b"<html>synthetic CME landing page</html>\n",
    predecessor_capture_release_id: str | None = None,
) -> DataReleaseReceipt:
    publisher = _publisher(boundary, operation_factory)
    stage = publisher.create_stage("calendar_capture")
    filters = {
        "error": None,
        "filters": {
            "assetClasses": [],
            "checkboxOptions": [],
            "dateTime": retrieved_at_utc,
            "exchanges": [],
            "holidays": [],
        },
    }
    files = {
        "landing.html": landing_bytes,
        "filters.json": canonical_bytes(filters) + b"\n",
        "schedule.json": canonical_bytes(response) + b"\n",
    }
    for name, content in files.items():
        (stage / name).write_bytes(content)
    logical_paths = {
        name: f"data/reference/exchange_calendars/{name}"
        for name in files
    }
    responses = [
        {
            "content_type": "text/html",
            "logical_path": logical_paths["landing.html"],
            "received_at_utc": retrieved_at_utc,
            "request_id": "landing-page",
            "request_kind": "LANDING_PAGE",
            "safe_headers": {"content-type": "text/html"},
            "sha256": __import__("hashlib").sha256(
                files["landing.html"]
            ).hexdigest(),
            "size": len(files["landing.html"]),
            "status_code": 200,
            "url": "https://www.cmegroup.com/trading-hours.html",
        },
        {
            "content_type": "application/json",
            "logical_path": logical_paths["filters.json"],
            "received_at_utc": retrieved_at_utc,
            "request_id": "filters",
            "request_kind": "FILTERS",
            "safe_headers": {"content-type": "application/json"},
            "sha256": __import__("hashlib").sha256(
                files["filters.json"]
            ).hexdigest(),
            "size": len(files["filters.json"]),
            "status_code": 200,
            "url": (
                "https://www.cmegroup.com/services/"
                "trading-hours-filters?isProtected"
            ),
        },
        {
            "content_type": "application/json",
            "logical_path": logical_paths["schedule.json"],
            "received_at_utc": retrieved_at_utc,
            "request_id": "schedule-001-p1",
            "request_kind": "SCHEDULE",
            "safe_headers": {"content-type": "application/json"},
            "sha256": __import__("hashlib").sha256(
                files["schedule.json"]
            ).hexdigest(),
            "size": len(files["schedule.json"]),
            "status_code": 200,
            "url": (
                "https://www.cmegroup.com/services/"
                "trading-hours-by-product?isProtected&pageNumber=1"
            ),
        },
    ]
    capture_approval_core = {
        "approved_at": retrieved_at_utc,
        "operation": CAPTURE_OPERATION,
        "plan_id": "b" * 64,
        "plan_sha256": "e" * 64,
        "schema_version": CAPTURE_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "f" * 64,
    }
    capture_approval = {
        **capture_approval_core,
        "approval_receipt_id": sha256_json(capture_approval_core),
    }
    core = {
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
        "coverage_end_trade_date": end,
        "coverage_start_trade_date": start,
        "elapsed_milliseconds": 0,
        "mode": "STEADY_STATE",
        "parser_version": PARSER_VERSION,
        "plan_id": "b" * 64,
        "predecessor_capture_release_id": predecessor_capture_release_id,
        "request_count": len(responses),
        "responses": responses,
        "retrieved_at_utc": retrieved_at_utc,
        "schema_version": CAPTURE_SCHEMA_VERSION,
        "total_bytes": sum(len(content) for content in files.values()),
    }
    capture_payload = {**core, "capture_id": sha256_json(core)}
    manifest = DataReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=CAPTURE_RELEASE_KIND,
        schema_version=CAPTURE_SCHEMA_VERSION,
        logical_paths=logical_paths,
        source_release_ids=(
            (predecessor_capture_release_id,)
            if predecessor_capture_release_id is not None
            else ()
        ),
        embedded_documents={"capture_receipt.json": capture_payload},
        metadata={
            "approval_receipt_id": capture_approval["approval_receipt_id"],
            "capture_id": capture_payload["capture_id"],
            "coverage_end_trade_date": end,
            "coverage_start_trade_date": start,
            "parser_version": PARSER_VERSION,
            "plan_id": "b" * 64,
            "retrieved_at_utc": retrieved_at_utc,
        },
    )
    manifest_path = publisher.publish(
        stage,
        manifest,
        staged_paths={
            logical_paths[name]: name
            for name in files
        },
    )
    receipt = DataReleaseReceipt.from_manifest(manifest_path, boundary)
    load_cme_capture(receipt, boundary=boundary)
    return receipt


def _mapping(
    capture_release_id: str,
    mappings: list[dict[str, str]],
    *,
    approved_at_utc: str = "2026-07-25T12:01:00Z",
) -> dict:
    core = {
        "approved_at": approved_at_utc,
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
    approved_at_utc: str = "2026-07-25T12:02:00Z",
) -> dict:
    core = {
        "approved_at": approved_at_utc,
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
    holiday_notices: list[dict[str, str]] | None = None,
) -> dict:
    if state_override is not None:
        sessions = json.loads(json.dumps(sessions))
        sessions[0]["intervals"][0]["state"] = state_override
    return {
        "holiday_notices": holiday_notices or [],
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
    holiday_notices: list[dict[str, str]] | None = None,
) -> tuple[DataReleaseReceipt, VerifiedExchangeCalendar]:
    response = _service_response(
        sessions,
        holiday_notices=holiday_notices,
    )
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
        approved_at_utc=(
            datetime.fromisoformat(
                retrieved_at_utc.replace("Z", "+00:00")
            )
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        ),
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
        implementation_sha256=_implementation_hashes(),
    )
    assert validate_capture_plan(plan) == plan
    scope = plan["scope"]
    assert scope["max_requests"] == 96
    assert len(scope["requests"]) <= 96
    assert scope["workers"] == 1
    assert scope["retries"] == 0
    assert scope["allow_redirects"] is False
    assert scope["implementation_sha256"] == _implementation_hashes()
    assert scope["output_paths"] == CAPTURE_OUTPUT_PATHS
    assert scope["forbidden_actions"] == list(CAPTURE_FORBIDDEN_ACTIONS)
    assert scope["stop_conditions"] == list(CAPTURE_STOP_CONDITIONS)


def test_product_discovery_plan_is_one_date_and_omits_product_ids() -> None:
    plan = build_capture_plan(
        mode="product-discovery",
        coverage_start=date(2026, 7, 26),
        coverage_end=date(2026, 7, 26),
        implementation_sha256=_implementation_hashes(),
    )
    scope = plan["scope"]
    assert scope["mode"] == "PRODUCT_DISCOVERY"
    assert scope["max_requests"] == 3
    assert len(scope["requests"]) == 3
    schedule_url = scope["requests"][-1]["url"]
    assert "id=" not in schedule_url
    with pytest.raises(ContractError, match="exactly one date"):
        build_capture_plan(
            mode="product-discovery",
            coverage_start=date(2026, 7, 26),
            coverage_end=date(2026, 7, 27),
            implementation_sha256=_implementation_hashes(),
        )


def test_steady_state_plan_pins_41_products_and_90_day_horizon() -> None:
    product_ids = tuple(sorted(str(value) for value in range(1, 42)))
    plan = build_capture_plan(
        mode="steady-state",
        coverage_start=date(2026, 7, 26),
        coverage_end=date(2026, 10, 23),
        implementation_sha256=_implementation_hashes(),
        product_ids=product_ids,
        predecessor_capture_release_id="a" * 64,
    )
    assert validate_capture_plan(plan) == plan
    scope = plan["scope"]
    assert scope["mode"] == "STEADY_STATE"
    assert scope["product_ids"] == list(product_ids)
    assert scope["predecessor_capture_release_id"] == "a" * 64
    assert scope["max_requests"] == 40
    assert len(scope["requests"]) == 32
    assert all(
        "id=" in request["url"]
        for request in scope["requests"]
        if request["request_kind"] == "SCHEDULE"
    )


def test_schedule_recovery_requests_only_cover_the_missing_range() -> None:
    product_ids = tuple(sorted(str(value) for value in range(1, 42)))
    requests = _schedule_recovery_requests(
        coverage_start=date(2026, 10, 24),
        coverage_end=date(2027, 1, 1),
        product_ids=product_ids,
    )

    assert len(requests) == 24
    assert requests[0]["core_start_trade_date"] == "2026-10-24"
    assert requests[-1]["core_end_trade_date"] == "2027-01-01"
    assert all(
        request["request_kind"] == "SCHEDULE"
        and "trading-hours.html" not in request["url"]
        and "trading-hours-filters" not in request["url"]
        and "id=" in request["url"]
        for request in requests
    )
    for previous, current in zip(requests, requests[1:]):
        assert (
            date.fromisoformat(current["core_start_trade_date"])
            == date.fromisoformat(previous["core_end_trade_date"])
            + timedelta(days=1)
        )


def test_schedule_recovery_approval_is_exact_and_hash_bound() -> None:
    plan = {
        "plan_id": "a" * 64,
        "schema_version": SCHEDULE_RECOVERY_PLAN_SCHEMA,
    }
    core = {
        "approved_at": "2026-07-27T01:30:00Z",
        "operation": SCHEDULE_RECOVERY_OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": "b" * 64,
        "schema_version": SCHEDULE_RECOVERY_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "c" * 64,
    }
    approval = {**core, "approval_receipt_id": sha256_json(core)}

    assert validate_schedule_coverage_recovery_approval(
        approval, plan=plan, plan_sha256="b" * 64
    ) == approval["approval_receipt_id"]
    drifted = dict(approval)
    drifted["plan_sha256"] = "d" * 64
    with pytest.raises(
        calendar_cli_module.CalendarCaptureError,
        match="lacks exact hash-bound approval",
    ):
        validate_schedule_coverage_recovery_approval(
            drifted, plan=plan, plan_sha256="b" * 64
        )


def test_capture_enforces_content_types_and_sanitizes_response_headers(
    boundary, operation_factory, monkeypatch, tmp_path
) -> None:
    plan = build_capture_plan(
        mode="bootstrap",
        coverage_start=date(2026, 7, 25),
        coverage_end=date(2026, 7, 25),
        implementation_sha256=_implementation_hashes(),
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
    monkeypatch.setattr(
        calendar_cli_module,
        "_capture_implementation_hashes",
        lambda _root: dict(plan["scope"]["implementation_sha256"]),
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


def test_schedule_recovery_reuses_source_and_only_networks_missing_windows(
    boundary, operation_factory, monkeypatch, tmp_path
) -> None:
    product_ids = tuple(sorted(str(value) for value in range(1, 42)))
    implementation = _implementation_hashes()
    source_plan = build_capture_plan(
        mode="steady-state",
        coverage_start=date(2026, 7, 26),
        coverage_end=date(2026, 10, 23),
        implementation_sha256=implementation,
        product_ids=product_ids,
        predecessor_capture_release_id="a" * 64,
    )
    source_plan_path = tmp_path / "source-plan.json"
    source_plan_path.write_bytes(canonical_bytes(source_plan) + b"\n")
    source_approval_core = {
        "approved_at": "2026-07-25T12:00:00Z",
        "operation": CAPTURE_OPERATION,
        "plan_id": source_plan["plan_id"],
        "plan_sha256": sha256_file(source_plan_path),
        "schema_version": CAPTURE_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "1" * 64,
    }
    source_approval = {
        **source_approval_core,
        "approval_receipt_id": sha256_json(source_approval_core),
    }
    source_approval_path = tmp_path / "source-approval.json"
    source_approval_path.write_bytes(
        canonical_bytes(source_approval) + b"\n"
    )
    schedule = canonical_bytes(
        _service_response([_regular_session("2026-07-25")])
    ) + b"\n"

    class Headers(dict):
        def get_content_type(self) -> str:
            return str(self["content-type"])

    class Response:
        status = 200

        def __init__(
            self, url: str, content_type: str, body: bytes
        ) -> None:
            self._url = url
            self.headers = Headers({"content-type": content_type})
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
    monkeypatch.setattr(
        calendar_cli_module,
        "_capture_implementation_hashes",
        lambda _root: implementation,
    )
    source_receipt = capture_cme_calendar(
        plan_path=source_plan_path,
        approval_path=source_approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    source_capture = load_cme_capture(
        source_receipt, boundary=boundary
    )
    reused = []
    for ordinal, response in enumerate(
        source_capture["responses"], start=1
    ):
        source = source_receipt.resolve_file(
            response["logical_path"], boundary
        )
        reused.append(
            {
                "ordinal": ordinal,
                "response": response,
                "source_relative_path": source.relative_to(
                    boundary.active_root
                ).as_posix(),
            }
        )
    network_requests = _schedule_recovery_requests(
        coverage_start=date(2026, 10, 24),
        coverage_end=date(2027, 1, 1),
        product_ids=product_ids,
    )
    recovery_scope = {
        "active_pointer_path": "configs/active_exchange_calendar.json",
        "authority": {
            "mapping_capture_release_id": "a" * 64,
            "source_capture_release_id": source_receipt.release_id,
        },
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": 900,
            "max_network_requests": 24,
            "max_output_responses": 56,
            "max_total_bytes": 268_435_456,
            "request_timeout_seconds": 30,
            "retries": 0,
            "workers": 1,
        },
        "coverage_end_trade_date": "2027-01-01",
        "coverage_start_trade_date": "2026-07-26",
        "implementation_sha256": implementation,
        "network_requests": network_requests,
        "reused_responses": reused,
    }
    recovery_core = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": SCHEDULE_RECOVERY_OPERATION,
        "schema_version": SCHEDULE_RECOVERY_PLAN_SCHEMA,
        "scope": recovery_scope,
    }
    recovery_plan = {
        **recovery_core,
        "plan_id": sha256_json(recovery_core),
    }
    recovery_plan_path = tmp_path / "recovery-plan.json"
    recovery_plan_path.write_bytes(
        canonical_bytes(recovery_plan) + b"\n"
    )
    recovery_approval_core = {
        "approved_at": "2026-07-27T01:30:00Z",
        "operation": SCHEDULE_RECOVERY_OPERATION,
        "plan_id": recovery_plan["plan_id"],
        "plan_sha256": sha256_file(recovery_plan_path),
        "schema_version": SCHEDULE_RECOVERY_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "2" * 64,
    }
    recovery_approval = {
        **recovery_approval_core,
        "approval_receipt_id": sha256_json(
            recovery_approval_core
        ),
    }
    recovery_approval_path = tmp_path / "recovery-approval.json"
    recovery_approval_path.write_bytes(
        canonical_bytes(recovery_approval) + b"\n"
    )
    monkeypatch.setattr(
        calendar_cli_module,
        "validate_schedule_coverage_recovery_plan",
        lambda payload, boundary: dict(recovery_plan),
    )

    recovery_receipt = capture_schedule_coverage_recovery(
        plan_path=recovery_plan_path,
        approval_path=recovery_approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    recovery = load_cme_capture(
        recovery_receipt, boundary=boundary
    )

    assert recovery_receipt.schema_version == (
        SCHEDULE_RECOVERY_CAPTURE_SCHEMA_VERSION
    )
    assert recovery["reused_response_count"] == 32
    assert recovery["network_request_count"] == 24
    assert recovery["request_count"] == 56
    assert recovery["responses"][:32] == source_capture["responses"]
    assert all(
        response["request_kind"] == "SCHEDULE"
        for response in recovery["responses"][32:]
    )


def test_steady_state_plan_requires_exact_41_product_ids() -> None:
    with pytest.raises(ContractError, match="exactly 41"):
        build_capture_plan(
            mode="steady-state",
            coverage_start=date(2026, 7, 25),
            coverage_end=date(2026, 7, 27),
            implementation_sha256=_implementation_hashes(),
            product_ids=("138",),
        )


def test_capture_rejects_implementation_drift_before_network(
    boundary, operation_factory, monkeypatch, tmp_path
) -> None:
    plan = build_capture_plan(
        mode="bootstrap",
        coverage_start=date(2026, 7, 25),
        coverage_end=date(2026, 7, 25),
        implementation_sha256=_implementation_hashes(),
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
    drifted = dict(plan["scope"]["implementation_sha256"])
    drifted["src/futures_rebuild/calendar_cli.py"] = "0" * 64
    monkeypatch.setattr(
        calendar_cli_module,
        "_capture_implementation_hashes",
        lambda _root: drifted,
    )
    network_attempted = False

    def _unexpected_network(*_args, **_kwargs):
        nonlocal network_attempted
        network_attempted = True
        raise AssertionError("network must not be reached")

    monkeypatch.setattr(
        calendar_cli_module.urllib.request,
        "build_opener",
        _unexpected_network,
    )
    with pytest.raises(
        calendar_cli_module.CalendarCaptureError,
        match="implementation hashes",
    ):
        capture_cme_calendar(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=_publisher(boundary, operation_factory),
        )
    assert network_attempted is False


def test_client_contract_plan_is_bound_to_accepted_landing_evidence(
    boundary, operation_factory
) -> None:
    asset_path = (
        "/etc.clientlibs/cmegroupaem/clientlibs/"
        "trading-hours.0123456789abcdef0123456789abcdef.js"
    )
    landing = (
        f'<html><script src="{asset_path}"></script></html>\n'.encode()
    )
    source_capture = _capture(
        boundary,
        operation_factory,
        response=_service_response([_regular_session("2026-07-25")]),
        start="2026-07-25",
        end="2026-07-25",
        landing_bytes=landing,
    )
    authority = _client_contract_authority(
        boundary.active_root / source_capture.manifest_path,
        boundary=boundary,
    )
    plan = build_client_contract_plan(
        authority=authority,
        implementation_sha256=_implementation_hashes(),
    )
    assert validate_client_contract_plan(plan) == plan
    scope = plan["scope"]
    assert scope["authority"] == authority
    assert scope["bounds"] == {
        "allow_redirects": False,
        "max_duration_seconds": 30,
        "max_requests": 1,
        "max_total_bytes": CLIENT_CONTRACT_MAX_BYTES,
        "request_timeout_seconds": 30,
        "retries": 0,
        "workers": 1,
    }
    assert scope["requests"] == [
        {
            "accept": "application/javascript,text/javascript;q=0.9",
            "request_id": "trading-hours-client-contract",
            "request_kind": "CLIENT_CONTRACT",
            "url": f"https://www.cmegroup.com{asset_path}",
        }
    ]
    assert scope["forbidden_actions"] == list(
        CLIENT_CONTRACT_FORBIDDEN_ACTIONS
    )
    assert scope["stop_conditions"] == list(CLIENT_CONTRACT_STOP_CONDITIONS)


def test_client_dependency_plan_is_single_request_and_non_authorizing() -> None:
    authority = {
        "client_asset_sha256": "1" * 64,
        "client_candidates_id": "2" * 64,
        "client_capture_manifest_path": (
            "manifests/data_releases/reference/" + "3" * 64 + ".json"
        ),
        "client_capture_manifest_sha256": "4" * 64,
        "client_capture_release_id": "3" * 64,
        "dependency_asset_url": (
            "https://www.cmegroup.com/etc.clientlibs/cmegroupaem/clientlibs/"
            "common.0123456789abcdef0123456789abcdef.js"
        ),
        "dependency_chunk_ids": ["4598"],
        "landing_capture_release_id": "5" * 64,
        "service_module_ids": ["26088"],
    }
    plan = build_client_dependency_plan(
        authority=authority,
        implementation_sha256=_implementation_hashes(),
    )
    assert validate_client_dependency_plan(plan) == plan
    assert plan["operation"] == CLIENT_DEPENDENCY_OPERATION
    assert plan["execution_authorized"] is False
    assert plan["scope"]["bounds"] == {
        "allow_redirects": False,
        "max_duration_seconds": 30,
        "max_requests": 1,
        "max_total_bytes": CLIENT_DEPENDENCY_MAX_BYTES,
        "request_timeout_seconds": 30,
        "retries": 0,
        "workers": 1,
    }
    assert plan["scope"]["requests"] == [
        {
            "accept": "application/javascript,text/javascript;q=0.9",
            "request_id": "trading-hours-common-dependency",
            "request_kind": "CLIENT_DEPENDENCY",
            "url": authority["dependency_asset_url"],
        }
    ]


def test_nonempty_discovery_plan_has_exact_41_market_requests() -> None:
    authority = {
        "dependency_asset_sha256": "1" * 64,
        "dependency_candidates_id": "2" * 64,
        "dependency_capture_manifest_path": (
            "manifests/data_releases/reference/" + "3" * 64 + ".json"
        ),
        "dependency_capture_manifest_sha256": "4" * 64,
        "dependency_capture_release_id": "3" * 64,
        "endpoint": "/services/trading-hours-by-product",
        "endpoint_module_id": "23031",
    }
    markets = tuple(sorted(NONEMPTY_DISCOVERY_GROUP_BY_MARKET))
    plan = build_nonempty_discovery_plan(
        authority=authority,
        coverage_date=date(2026, 7, 26),
        expected_markets=markets,
        implementation_sha256=_implementation_hashes(),
        universe_contract_sha256="5" * 64,
    )
    assert validate_nonempty_discovery_plan(plan) == plan
    assert plan["operation"] == NONEMPTY_DISCOVERY_OPERATION
    requests = plan["scope"]["requests"]
    assert len(requests) == 41
    assert {item["market"] for item in requests} == set(markets)
    assert all(
        "id=" in item["url"]
        and "searchString=" in item["url"]
        and item["request_kind"] == "NONEMPTY_PRODUCT_DISCOVERY"
        for item in requests
    )
    assert plan["scope"]["bounds"]["retries"] == 0
    assert plan["scope"]["bounds"]["workers"] == 1


def test_semantic_search_recovery_changes_only_pl_query() -> None:
    markets = (
        "PL",
        "RB",
        "RTY",
        "SI",
        "SR1",
        "SR3",
        "TN",
        "UB",
        "YM",
        "ZB",
        "ZF",
        "ZL",
        "ZM",
        "ZQ",
        "ZS",
        "ZT",
        "ZW",
    )
    predecessor = [
        {
            "accept": "application/json",
            "market": market,
            "request_id": f"search-{market.lower()}",
            "request_kind": "SEARCH_PRODUCT_DISCOVERY",
            "url": (
                "https://www.cmegroup.com/services/"
                "trading-hours-by-product?pageNumber=1&pageSize=999"
                f"&cleared=Futures&searchString={market}&sortAsc=true"
                "&fromEventDate=2026-07-25&toEventDate=2026-07-27"
            ),
        }
        for market in markets
    ]
    successor, override = (
        calendar_cli_module._semantic_search_successor_requests(predecessor)
    )
    calendar_cli_module._validate_semantic_search_requests(
        successor, predecessor_requests=predecessor
    )
    assert successor[0] == {
        **predecessor[0],
        "url": predecessor[0]["url"].replace(
            "searchString=PL", "searchString=Platinum"
        ),
    }
    assert successor[1:] == predecessor[1:]
    assert override["request_ordinal"] == 17
    assert override["predecessor_value"] == "PL"
    assert override["successor_value"] == "Platinum"
    assert override["predecessor_request_sha256"] == sha256_json(
        predecessor[0]
    )
    assert override["successor_request_sha256"] == sha256_json(successor[0])

    drifted = [dict(item) for item in successor]
    drifted[1]["url"] = drifted[1]["url"].replace(
        "searchString=RB", "searchString=RBOB"
    )
    with pytest.raises(IntegrityError, match="request set drifted"):
        calendar_cli_module._validate_semantic_search_requests(
            drifted, predecessor_requests=predecessor
        )


def test_semantic_search_recovery_rejects_ambiguous_predecessor() -> None:
    request = {
        "accept": "application/json",
        "market": "PL",
        "request_id": "search-pl",
        "request_kind": "SEARCH_PRODUCT_DISCOVERY",
        "url": (
            "https://www.cmegroup.com/services/trading-hours-by-product"
            "?id=437&searchString=PL"
        ),
    }
    with pytest.raises(IntegrityError, match="predecessor PL request"):
        calendar_cli_module._semantic_search_successor_requests(
            [request] * 17
        )


def test_exact_futures_product_mapping_is_fail_closed() -> None:
    exact = {
        "foi": "Futures",
        "globex": "PL",
        "id": 446,
        "name": "Platinum Futures",
        "prodGroup": "PL",
    }
    payload = {
        "products": [
            {
                "foi": "Futures",
                "globex": "PLT",
                "id": 999,
                "name": "Unrelated Futures",
                "prodGroup": "PX",
            },
            exact,
        ],
        "props": {},
    }
    assert calendar_cli_module._exact_futures_product(
        payload, market="PL"
    ) == exact

    duplicate = json.loads(json.dumps(payload))
    duplicate["products"].append(dict(exact))
    with pytest.raises(IntegrityError, match="incomplete or ambiguous"):
        calendar_cli_module._exact_futures_product(
            duplicate, market="PL"
        )
    with pytest.raises(IntegrityError, match="service schema"):
        calendar_cli_module._exact_futures_product(
            {"products": []}, market="PL"
        )


def test_client_contract_capture_and_offline_parser_are_fail_closed(
    boundary, operation_factory, monkeypatch, tmp_path
) -> None:
    asset_path = (
        "/etc.clientlibs/cmegroupaem/clientlibs/"
        "trading-hours.0123456789abcdef0123456789abcdef.js"
    )
    source_capture = _capture(
        boundary,
        operation_factory,
        response=_service_response([_regular_session("2026-07-25")]),
        start="2026-07-25",
        end="2026-07-25",
        landing_bytes=(
            (
                f'<script src="https://www.cmegroup.com{asset_path}"></script>'
                '<script src="/etc.clientlibs/cmegroupaem/clientlibs/'
                'common.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.js"></script>\n'
            ).encode()
        ),
    )
    plan = build_client_contract_plan(
        authority=_client_contract_authority(
            boundary.active_root / source_capture.manifest_path,
            boundary=boundary,
        ),
        implementation_sha256=_implementation_hashes(),
    )
    plan_path = tmp_path / "client-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval_core = {
        "approved_at": "2026-07-25T12:00:00Z",
        "operation": CLIENT_CONTRACT_OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "schema_version": CLIENT_CONTRACT_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "2" * 64,
    }
    approval = {
        **approval_core,
        "approval_receipt_id": sha256_json(approval_core),
    }
    approval_path = tmp_path / "client-approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    javascript = (
        b"var y=r(26088);y.getTradingHoursData();"
        b'e.O(0,[4598]);const keys=["id","pageNumber","pageSize","search"];'
    )

    class Headers(dict):
        def get_content_type(self) -> str:
            return str(self["content-type"])

    class Response:
        status = 200
        headers = Headers(
            {
                "content-type": "application/javascript",
                "etag": '"fixture"',
                "set-cookie": "must-not-be-retained",
            }
        )

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def geturl(self) -> str:
            return f"https://www.cmegroup.com{asset_path}"

        def read(self, maximum: int) -> bytes:
            return javascript[:maximum]

    class Opener:
        def open(self, request, timeout: int):
            assert request.full_url == f"https://www.cmegroup.com{asset_path}"
            assert timeout == 30
            return Response()

    monkeypatch.setattr(
        calendar_cli_module.urllib.request,
        "build_opener",
        lambda *_handlers: Opener(),
    )
    monkeypatch.setattr(
        calendar_cli_module,
        "_capture_implementation_hashes",
        lambda _root: dict(plan["scope"]["implementation_sha256"]),
    )
    receipt = capture_client_contract(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    capture = load_client_contract_capture(receipt, boundary=boundary)
    assert capture["request_count"] == 1
    assert "set-cookie" not in capture["response"]["safe_headers"]
    candidates = parse_client_contract_candidates(receipt, boundary=boundary)
    assert candidates["status"] == "DEPENDENCY_CAPTURE_REQUIRED"
    assert candidates["endpoint_candidates"] == []
    assert {
        item["module_id"] for item in candidates["service_module_candidates"]
    } == {"26088"}
    assert {
        item["chunk_id"] for item in candidates["dependency_chunk_candidates"]
    } == {"4598"}
    assert {
        item["value"] for item in candidates["query_key_candidates"]
    } == {"id", "pageNumber", "pageSize", "search"}
    assert all(
        type(item["byte_offset"]) is int
        and len(item["literal_sha256"]) == 64
        for item in candidates["service_module_candidates"]
    )
    dependency_plan = build_client_dependency_plan(
        authority=_client_dependency_authority(
            boundary.active_root / receipt.manifest_path,
            boundary=boundary,
        ),
        implementation_sha256=_implementation_hashes(),
    )
    dependency_plan_path = tmp_path / "dependency-plan.json"
    dependency_plan_path.write_bytes(
        canonical_bytes(dependency_plan) + b"\n"
    )
    dependency_approval_core = {
        "approved_at": "2026-07-25T12:01:00Z",
        "operation": CLIENT_DEPENDENCY_OPERATION,
        "plan_id": dependency_plan["plan_id"],
        "plan_sha256": sha256_file(dependency_plan_path),
        "schema_version": CLIENT_DEPENDENCY_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "4" * 64,
    }
    dependency_approval = {
        **dependency_approval_core,
        "approval_receipt_id": sha256_json(dependency_approval_core),
    }
    dependency_approval_path = tmp_path / "dependency-approval.json"
    dependency_approval_path.write_bytes(
        canonical_bytes(dependency_approval) + b"\n"
    )
    dependency_javascript = (
        b'({23031:(e,t,r)=>{getTradingHoursData;'
        b'b="/services/trading-hours-by-product?";'
        b'"id=".concat(t,"&pageNumber=");'
        b'"&pageSize=";"&exch=";"&cleared=";"&group=";"&subGroup=";'
        b'"&searchString=".concat(encodeURIComponent(u));'
        b'"&sortField=";"&sortAsc=";'
        b'"&fromEventDate=".concat(p);'
        b'"&toEventDate=".concat(h)},78355:(e)=>{}})'
    )

    class DependencyResponse(Response):
        def __init__(self) -> None:
            self.headers = Headers(
                {
                    "content-type": "application/javascript",
                    "etag": '"dependency-fixture"',
                    "set-cookie": "must-not-be-retained",
                }
            )

        def geturl(self) -> str:
            return str(
                dependency_plan["scope"]["requests"][0]["url"]
            )

        def read(self, maximum: int) -> bytes:
            return dependency_javascript[:maximum]

    class DependencyOpener:
        def open(self, request, timeout: int):
            assert request.full_url == DependencyResponse().geturl()
            assert timeout == 30
            return DependencyResponse()

    monkeypatch.setattr(
        calendar_cli_module.urllib.request,
        "build_opener",
        lambda *_handlers: DependencyOpener(),
    )
    monkeypatch.setattr(
        calendar_cli_module,
        "_capture_implementation_hashes",
        lambda _root: dict(
            dependency_plan["scope"]["implementation_sha256"]
        ),
    )
    dependency_receipt = capture_client_dependency(
        plan_path=dependency_plan_path,
        approval_path=dependency_approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    dependency_capture = load_client_dependency_capture(
        dependency_receipt, boundary=boundary
    )
    assert dependency_capture["response"]["request_kind"] == "CLIENT_DEPENDENCY"
    endpoint_candidates = parse_client_dependency_candidates(
        dependency_receipt, boundary=boundary
    )
    assert (
        endpoint_candidates["status"]
        == "NONEMPTY_ID_DISCOVERY_PLAN_READY"
    )
    assert endpoint_candidates["endpoint"] == (
        "/services/trading-hours-by-product"
    )
    assert endpoint_candidates["query_keys"][0:3] == [
        "id",
        "pageNumber",
        "pageSize",
    ]


def test_client_contract_rejects_landing_authority_drift_before_network(
    boundary, operation_factory, monkeypatch, tmp_path
) -> None:
    asset_path = (
        "/etc.clientlibs/cmegroupaem/clientlibs/"
        "trading-hours.0123456789abcdef0123456789abcdef.js"
    )
    source_capture = _capture(
        boundary,
        operation_factory,
        response=_service_response([_regular_session("2026-07-25")]),
        start="2026-07-25",
        end="2026-07-25",
        landing_bytes=f'<script src="{asset_path}"></script>\n'.encode(),
    )
    authority = _client_contract_authority(
        boundary.active_root / source_capture.manifest_path,
        boundary=boundary,
    )
    authority["asset_url"] = (
        "https://www.cmegroup.com/etc.clientlibs/cmegroupaem/clientlibs/"
        "trading-hours.ffffffffffffffffffffffffffffffff.js"
    )
    plan = build_client_contract_plan(
        authority=authority,
        implementation_sha256=_implementation_hashes(),
    )
    plan_path = tmp_path / "drift-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval_core = {
        "approved_at": "2026-07-25T12:00:00Z",
        "operation": CLIENT_CONTRACT_OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "schema_version": CLIENT_CONTRACT_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "3" * 64,
    }
    approval = {
        **approval_core,
        "approval_receipt_id": sha256_json(approval_core),
    }
    approval_path = tmp_path / "drift-approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    monkeypatch.setattr(
        calendar_cli_module,
        "_capture_implementation_hashes",
        lambda _root: dict(plan["scope"]["implementation_sha256"]),
    )
    network_attempted = False

    def _unexpected_network(*_args, **_kwargs):
        nonlocal network_attempted
        network_attempted = True
        raise AssertionError("network must not be reached")

    monkeypatch.setattr(
        calendar_cli_module.urllib.request,
        "build_opener",
        _unexpected_network,
    )
    with pytest.raises(
        calendar_cli_module.CalendarCaptureError,
        match="authority differs",
    ):
        capture_client_contract(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=_publisher(boundary, operation_factory),
        )
    assert network_attempted is False


def test_schedule_page_parser_accepts_only_exact_bare_protection_flag() -> None:
    assert (
        exchange_calendar_module._response_page_number(
            "https://www.cmegroup.com/services/trading-hours-by-product"
            "?isProtected&pageNumber=1"
        )
        == 1
    )
    with pytest.raises(IntegrityError, match="query is malformed"):
        exchange_calendar_module._response_page_number(
            "https://www.cmegroup.com/services/trading-hours-by-product"
            "?unexpected&pageNumber=1"
        )
    with pytest.raises(IntegrityError, match="protection flag"):
        exchange_calendar_module._response_page_number(
            "https://www.cmegroup.com/services/trading-hours-by-product"
            "?isProtected=true&pageNumber=1"
        )


def test_native_cme_response_is_strictly_adapted_to_calendar_intervals() -> None:
    products: dict[str, dict[str, object]] = {}
    notices: set[tuple[str, str]] = set()
    native = {
        "products": [
            {
                "foi": "Futures",
                "globex": "ES",
                "id": 133,
                "name": "E-mini S&P 500 Futures",
                "prodGroup": "ES",
                "tradingHours": {
                    "eventCount": 2,
                    "schedules": [
                        {
                            "eventDate": "2026-07-25",
                            "events": [],
                            "groupCode": "ES",
                        },
                        {
                            "eventDate": "2026-07-26",
                            "events": [
                                {
                                    "eventTime": "17:00",
                                    "marketEventType": "open",
                                    "tradingDate": "2026-07-27",
                                }
                            ],
                            "groupCode": "ES",
                        },
                        {
                            "eventDate": "2026-07-27",
                            "events": [
                                {
                                    "eventTime": "16:00",
                                    "marketEventType": "closed",
                                    "tradingDate": "2026-07-27",
                                },
                                {
                                    "eventTime": "16:45",
                                    "marketEventType": "preopen",
                                    "tradingDate": "2026-07-28",
                                },
                                {
                                    "eventTime": "17:00",
                                    "marketEventType": "open",
                                    "tradingDate": "2026-07-28",
                                },
                            ],
                            "groupCode": "ES",
                        },
                    ],
                },
                "url": (
                    "/markets/equities/sp/"
                    "e-mini-sandp500_contract_specifications.html"
                ),
            }
        ],
        "props": {
            "hasEvents": True,
            "pageNumber": 1,
            "pageSize": 500,
            "pageTotal": 1,
            "sortAsc": "true",
            "total": 1,
        },
    }
    exchange_calendar_module._merge_service_response(
        native,
        products_by_code=products,
        holiday_notices=notices,
        expected_page_number=1,
        maximum_page_number=1,
    )
    intervals = exchange_calendar_module._product_intervals_for_date(
        products["133"], trade_date=date(2026, 7, 27)
    )
    assert intervals == [
        {
            "ends_at_local": "2026-07-27T16:00:00",
            "starts_at_local": "2026-07-26T17:00:00",
            "state": "OPEN",
        },
        {
            "ends_at_local": "2026-07-27T16:45:00",
            "starts_at_local": "2026-07-27T16:00:00",
            "state": "CLOSED",
        },
        {
            "ends_at_local": "2026-07-27T17:00:00",
            "starts_at_local": "2026-07-27T16:45:00",
            "state": "PREOPEN",
        },
    ]
    exchange_calendar_module._merge_filters_response(
        {
            "error": None,
            "filters": {
                "assetClasses": [],
                "checkboxOptions": [],
                "dateTime": "2026-07-26T22:38:20.499Z",
                "exchanges": [],
                "holidays": [
                    {
                        "date": "2026-11-26",
                        "endPeriod": "2026-11-27",
                        "holiday": "Thanksgiving",
                        "startPeriod": "2026-11-25",
                    }
                ],
            },
        },
        holiday_notices=notices,
    )
    assert notices == {("2026-11-26", "Thanksgiving")}


def test_verified_calendar_covers_exact_41_market_universe(
    boundary, operation_factory
) -> None:
    markets = approved_research_markets(
        REPO / "configs" / "research_universe_contract.json"
    )
    assert len(markets) == 41
    session = _regular_session("2026-07-25")
    products = []
    mappings = []
    for ordinal, market in enumerate(markets, start=1000):
        code = str(ordinal)
        products.append(
            {
                "cme_product_code": code,
                "cme_product_name": f"Synthetic {market} Futures",
                "product_group": "Synthetic Fixture",
                "product_type": "Futures",
                "sessions": [session],
                "venue": CME_VENUE,
            }
        )
        mappings.append({"cme_product_code": code, "market": market})
    response = {
        "holiday_notices": [],
        "pagination": {"page_number": 1, "total_pages": 1},
        "products": products,
        "schema_version": SERVICE_RESPONSE_SCHEMA,
        "timezone": CME_TIMEZONE,
    }
    capture = _capture(
        boundary,
        operation_factory,
        response=response,
        start="2026-07-25",
        end="2026-07-25",
    )
    calendar_receipt = publish_verified_exchange_calendar(
        capture_receipt=capture,
        mapping_approval=_mapping(capture.release_id, mappings),
        expected_markets=markets,
        publisher=_publisher(boundary, operation_factory),
    )
    calendar = VerifiedExchangeCalendar.from_release(
        calendar_receipt,
        boundary=boundary,
        expected_markets=markets,
    )
    assert tuple(calendar.products) == markets


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


def test_mapping_approval_may_bind_direct_capture_lineage_only(
    boundary, operation_factory
) -> None:
    mapping_source_release_id = "a" * 64
    capture = _capture(
        boundary,
        operation_factory,
        response=_service_response([_regular_session("2026-07-06")]),
        start="2026-07-06",
        end="2026-07-06",
        predecessor_capture_release_id=mapping_source_release_id,
    )
    receipt = publish_verified_exchange_calendar(
        capture_receipt=capture,
        mapping_approval=_mapping(
            mapping_source_release_id,
            [{"cme_product_code": "138", "market": "ES"}],
        ),
        expected_markets=("ES",),
        publisher=_publisher(boundary, operation_factory),
    )
    VerifiedExchangeCalendar.from_release(
        receipt, boundary=boundary, expected_markets=("ES",)
    )

    unrelated = _mapping(
        "b" * 64,
        [{"cme_product_code": "138", "market": "ES"}],
    )
    with pytest.raises(IntegrityError, match="direct capture lineage"):
        publish_verified_exchange_calendar(
            capture_receipt=capture,
            mapping_approval=unrelated,
            expected_markets=("ES",),
            publisher=_publisher(boundary, operation_factory),
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
        freshness_at=datetime(2026, 7, 25, 13, tzinfo=UTC),
        minimum_forward_days=1,
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


def test_calendar_activation_plan_binds_candidate_diff_and_contracts(
    boundary, operation_factory
) -> None:
    calendar_receipt, calendar = _publish_calendar(
        boundary,
        operation_factory,
        sessions=[_regular_session("2026-07-25")],
    )
    diff = diff_exchange_calendars(None, calendar)
    diff_path = boundary.active_root / "reports" / "calendar_diff.json"
    diff_path.parent.mkdir(parents=True, exist_ok=True)
    diff_path.write_bytes(canonical_bytes(diff) + b"\n")
    plan = build_calendar_activation_plan(
        candidate_calendar_manifest_path=(
            boundary.active_root / calendar_receipt.manifest_path
        ),
        diff_report_path=diff_path,
        boundary=boundary,
        expected_markets=("ES",),
        implementation_sha256=_implementation_hashes(),
        policy_sha256="a" * 64,
        universe_contract_sha256="b" * 64,
    )

    assert validate_calendar_activation_plan(
        plan, boundary=boundary
    ) == plan
    assert plan["scope"]["candidate_calendar_release_id"] == (
        calendar_receipt.release_id
    )
    assert plan["scope"]["diff_report_id"] == diff["diff_report_id"]
    assert plan["scope"]["predecessor_index_release_id"] is None

    drifted = dict(diff)
    drifted["status"] = "DRIFTED"
    diff_path.write_bytes(canonical_bytes(drifted) + b"\n")
    with pytest.raises(IntegrityError, match="diff report drifted"):
        validate_calendar_activation_plan(plan, boundary=boundary)


def test_freshness_requires_72_hour_holiday_revalidation_and_far_notice_coverage(
    boundary, operation_factory
) -> None:
    stale_receipt, stale_calendar = _publish_calendar(
        boundary,
        operation_factory,
        sessions=[_regular_session("2026-07-25")],
        retrieved_at_utc="2026-07-22T11:00:00Z",
        holiday_notices=[
            {"name": "Synthetic Holiday", "trade_date": "2026-07-25"}
        ],
    )
    stale_diff = diff_exchange_calendars(None, stale_calendar)
    with pytest.raises(IntegrityError, match="revalidated"):
        publish_calendar_index(
            candidate_calendar_receipt=stale_receipt,
            activation_approval=_activation(
                calendar_release_id=stale_receipt.release_id,
                predecessor_index_release_id=None,
                diff_report_id=str(stale_diff["diff_report_id"]),
            ),
            publisher=_publisher(boundary, operation_factory),
            expected_markets=("ES",),
            freshness_at=datetime(2026, 7, 25, 13, tzinfo=UTC),
            minimum_forward_days=1,
        )

    far_receipt, far_calendar = _publish_calendar(
        boundary,
        operation_factory,
        sessions=[_regular_session("2026-07-25")],
        holiday_notices=[
            {"name": "Far Holiday", "trade_date": "2026-12-25"}
        ],
    )
    far_diff = diff_exchange_calendars(None, far_calendar)
    before = tuple(
        (boundary.active_root / "manifests" / "data_releases" / "controls")
        .glob("*.json")
    )
    with pytest.raises(IntegrityError, match="coverage is incomplete"):
        publish_calendar_index(
            candidate_calendar_receipt=far_receipt,
            activation_approval=_activation(
                calendar_release_id=far_receipt.release_id,
                predecessor_index_release_id=None,
                diff_report_id=str(far_diff["diff_report_id"]),
            ),
            publisher=_publisher(boundary, operation_factory),
            expected_markets=("ES",),
            freshness_at=datetime(2026, 7, 25, 13, tzinfo=UTC),
            minimum_forward_days=1,
        )
    after = tuple(
        (boundary.active_root / "manifests" / "data_releases" / "controls")
        .glob("*.json")
    )
    assert after == before


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
        freshness_at=datetime(2026, 7, 25, 13, tzinfo=UTC),
        minimum_forward_days=2,
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
        freshness_at=datetime(2026, 7, 25, 13, tzinfo=UTC),
        minimum_forward_days=2,
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
        freshness_at=datetime(2026, 7, 25, 13, tzinfo=UTC),
        minimum_forward_days=2,
        predecessor_index_receipt=future_index_receipt,
    )
    changed_index = load_calendar_index(
        changed_index_receipt,
        boundary=boundary,
        expected_markets=("ES",),
    )
    assert not coverage_matches_active_index(coverage, changed_index)
