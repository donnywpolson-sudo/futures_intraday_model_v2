"""Plan, capture, parse, diff, activate, and verify CME exchange calendars."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import sys
import time as monotonic_time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .calendar_historical_archive import (
    build_historical_archive_plan,
    capture_historical_archive_landing,
    historical_archive_authority,
    implementation_hashes as historical_archive_implementation_hashes,
)
from .calendar_historical_discovery import (
    build_historical_source_discovery_plan,
    capture_historical_source_discovery,
    historical_source_authority,
    implementation_hashes as historical_source_implementation_hashes,
)
from .calendar_holiday_schedule_discovery import (
    build_holiday_schedule_discovery,
)
from .calendar_holiday_schedule_capture import (
    build_holiday_schedule_capture_plan,
    capture_holiday_schedules,
    holiday_schedule_authority,
    implementation_hashes as holiday_schedule_implementation_hashes,
)
from .calendar_notice_client import (
    build_notice_client_plan,
    capture_notice_client_contract,
    implementation_hashes as notice_client_implementation_hashes,
    notice_client_authority,
)
from .calendar_notice_search import (
    build_notice_endpoint_assessment,
    build_notice_search_capability_plan,
    capture_notice_search_capability,
    derive_notice_endpoint_evidence,
    implementation_hashes as notice_search_implementation_hashes,
    notice_search_authority,
)
from .calendar_notice_metadata import (
    build_capability_assessment,
    build_metadata_discovery_plan,
    capture_metadata_discovery,
    implementation_hashes as notice_metadata_implementation_hashes,
    metadata_authority,
)
from .calendar_notice_pagination import (
    build_pagination_plan,
    build_semantic_assessment,
    capture_pagination,
    implementation_hashes as notice_pagination_implementation_hashes,
    pagination_authority,
)
from .calendar_notice_documents import (
    build_document_probe_plan,
    build_metadata_index,
    capture_document_probe,
    document_probe_authority,
    implementation_hashes as notice_document_implementation_hashes,
)
from .calendar_notice_union import (
    build_probe_assessment,
    build_union_plan,
    capture_union,
    implementation_hashes as notice_union_implementation_hashes,
    union_authority,
)
from .calendar_notice_union_recovery import (
    build_recovery_plan as build_notice_union_recovery_plan,
)
from .calendar_notice_union_recovery import (
    capture_recovery_union,
)
from .calendar_notice_union_recovery import (
    implementation_hashes as notice_union_recovery_implementation_hashes,
)
from .calendar_notice_union_recovery import (
    recovery_authority as notice_union_recovery_authority,
)
from .calendar_notice_attachments import build_attachment_assessment
from .calendar_notice_attachment_capture import (
    attachment_authority as notice_attachment_authority,
    build_attachment_capture_plan,
    capture_attachments,
    implementation_hashes as notice_attachment_implementation_hashes,
)
from .calendar_notice_attachment_diagnostic import (
    build_diagnostic_plan as build_notice_attachment_diagnostic_plan,
    implementation_hashes as notice_attachment_diagnostic_implementation_hashes,
    preserved_failure_authority,
    run_diagnostic as run_notice_attachment_diagnostic,
)
from .calendar_notice_attachment_recovery import (
    build_recovery_plan as build_notice_attachment_recovery_plan,
    capture_attachment_recovery,
    implementation_hashes as notice_attachment_recovery_implementation_hashes,
    recovery_authority as notice_attachment_recovery_authority,
)
from .calendar_notice_attachment_reconciliation import (
    build_reconciliation_plan as build_notice_attachment_reconciliation_plan,
    capture_attachment_reconciliation,
    implementation_hashes as notice_attachment_reconciliation_hashes,
    reconciliation_authority as notice_attachment_reconciliation_authority,
)
from .calendar_notice_attachment_reconciliation_recovery import (
    build_interruption_evidence as build_notice_attachment_interruption_evidence,
    build_recovery_plan as build_notice_attachment_reconciliation_recovery_plan,
    capture_reconciliation_recovery,
    implementation_hashes as notice_attachment_reconciliation_recovery_hashes,
    recovery_authority as notice_attachment_reconciliation_recovery_authority,
)
from .canonical import canonical_bytes, fsync_directory, sha256_file, sha256_json
from .data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .exchange_calendar import (
    ACTIVE_POINTER_SCHEMA,
    CAPTURE_APPROVAL_SCHEMA,
    CAPTURE_OPERATION,
    CAPTURE_RELEASE_KIND,
    CAPTURE_SCHEMA_VERSION,
    CME_TIMEZONE,
    MAPPING_APPROVAL_SCHEMA,
    PARSER_VERSION,
    SCHEDULE_RECOVERY_APPROVAL_SCHEMA,
    SCHEDULE_RECOVERY_CAPTURE_SCHEMA_VERSION,
    SCHEDULE_RECOVERY_OPERATION,
    VerifiedExchangeCalendar,
    active_pointer_payload,
    approved_research_markets,
    diff_exchange_calendars,
    generate_mapping_candidates,
    load_active_calendar_index,
    load_calendar_index,
    load_cme_capture,
    load_exchange_calendar_policy,
    load_exchange_calendar_policy,
    publish_calendar_index,
    publish_verified_exchange_calendar,
    validate_mapping_approval,
    verify_calendar_freshness,
)
from .source_contract import legacy_roots_from_contract


CAPTURE_PLAN_SCHEMA = "cme_calendar_capture_plan/1.1.0"
CLIENT_CONTRACT_PLAN_SCHEMA = "cme_calendar_client_contract_plan/1.0.0"
CLIENT_CONTRACT_APPROVAL_SCHEMA = "cme_calendar_client_contract_approval/1.0.0"
CLIENT_CONTRACT_CAPTURE_SCHEMA = "cme_calendar_client_contract_capture/1.0.0"
CLIENT_CONTRACT_CANDIDATES_SCHEMA = "cme_calendar_client_contract_candidates/1.0.0"
CLIENT_CONTRACT_OPERATION = (
    "CAPTURE_BOUNDED_PUBLIC_CME_TRADING_HOURS_CLIENT_CONTRACT"
)
CLIENT_CONTRACT_RELEASE_KIND = "cme_trading_hours_client_contract_capture"
CLIENT_CONTRACT_ASSET_PREFIX = (
    "/etc.clientlibs/cmegroupaem/clientlibs/trading-hours."
)
CLIENT_CONTRACT_ASSET_PATTERN = re.compile(
    r"^/etc\.clientlibs/cmegroupaem/clientlibs/"
    r"trading-hours\.[0-9a-f]{32}\.js$"
)
CLIENT_COMMON_ASSET_PATTERN = re.compile(
    r"^/etc\.clientlibs/cmegroupaem/clientlibs/"
    r"common\.[0-9a-f]{32}\.js$"
)
CLIENT_CONTRACT_MAX_BYTES = 8_388_608
CLIENT_CONTRACT_MAX_DURATION_SECONDS = 30
CLIENT_DEPENDENCY_PLAN_SCHEMA = "cme_calendar_client_dependency_plan/1.0.0"
CLIENT_DEPENDENCY_APPROVAL_SCHEMA = (
    "cme_calendar_client_dependency_approval/1.0.0"
)
CLIENT_DEPENDENCY_CAPTURE_SCHEMA = (
    "cme_calendar_client_dependency_capture/1.0.0"
)
CLIENT_DEPENDENCY_CANDIDATES_SCHEMA = (
    "cme_calendar_client_dependency_candidates/1.0.0"
)
CLIENT_DEPENDENCY_OPERATION = (
    "CAPTURE_BOUNDED_PUBLIC_CME_TRADING_HOURS_CLIENT_DEPENDENCY"
)
CLIENT_DEPENDENCY_RELEASE_KIND = (
    "cme_trading_hours_client_dependency_capture"
)
CLIENT_DEPENDENCY_MAX_BYTES = 16_777_216
CLIENT_DEPENDENCY_ENDPOINT_SCHEMA = (
    "cme_calendar_client_dependency_endpoints/1.0.0"
)
NONEMPTY_DISCOVERY_PLAN_SCHEMA = (
    "cme_calendar_nonempty_product_discovery_plan/1.0.0"
)
NONEMPTY_DISCOVERY_APPROVAL_SCHEMA = (
    "cme_calendar_nonempty_product_discovery_approval/1.0.0"
)
NONEMPTY_DISCOVERY_CAPTURE_SCHEMA = (
    "cme_calendar_nonempty_product_discovery_capture/1.0.0"
)
NONEMPTY_DISCOVERY_CANDIDATES_SCHEMA = (
    "cme_calendar_nonempty_product_discovery_candidates/1.0.0"
)
NONEMPTY_DISCOVERY_OPERATION = (
    "CAPTURE_BOUNDED_PUBLIC_CME_NONEMPTY_PRODUCT_DISCOVERY"
)
NONEMPTY_DISCOVERY_RELEASE_KIND = "cme_nonempty_product_discovery_capture"
NONEMPTY_DISCOVERY_GROUP_BY_MARKET = {
    **{market: "133" for market in ("ES", "NQ", "RTY", "YM")},
    **{market: "425" for market in ("CL", "HO", "NG", "RB")},
    **{market: "437" for market in ("GC", "HG", "PA", "PL", "SI")},
    **{
        market: "316"
        for market in ("SR1", "SR3", "TN", "UB", "ZF", "ZN", "ZQ", "ZT", "ZB")
    },
    **{
        market: "58"
        for market in ("6A", "6B", "6C", "6E", "6J", "6M", "6N", "6S")
    },
    **{market: "300" for market in ("KE", "ZC", "ZL", "ZM", "ZS", "ZW")},
    **{market: "22" for market in ("GF", "HE", "LE")},
    **{market: "8478" for market in ("BTC", "ETH")},
}
SEARCH_DISCOVERY_PLAN_SCHEMA = "cme_calendar_search_product_discovery_plan/1.0.0"
SEARCH_DISCOVERY_APPROVAL_SCHEMA = (
    "cme_calendar_search_product_discovery_approval/1.0.0"
)
SEARCH_DISCOVERY_CAPTURE_SCHEMA = (
    "cme_calendar_search_product_discovery_capture/1.0.0"
)
SEARCH_DISCOVERY_OPERATION = (
    "CAPTURE_BOUNDED_PUBLIC_CME_SEARCH_PRODUCT_DISCOVERY"
)
SEARCH_DISCOVERY_RELEASE_KIND = "cme_search_product_discovery_capture"
SEARCH_RECOVERY_PLAN_SCHEMA = (
    "cme_calendar_search_product_discovery_recovery_plan/1.0.0"
)
SEARCH_RECOVERY_APPROVAL_SCHEMA = (
    "cme_calendar_search_product_discovery_recovery_approval/1.0.0"
)
SEARCH_RECOVERY_CAPTURE_SCHEMA = (
    "cme_calendar_search_product_discovery_capture/1.1.0"
)
SEARCH_RECOVERY_OPERATION = (
    "RECOVER_BOUNDED_PUBLIC_CME_SEARCH_PRODUCT_DISCOVERY"
)
SEMANTIC_SEARCH_RECOVERY_PLAN_SCHEMA = (
    "cme_calendar_search_product_discovery_semantic_recovery_plan/1.0.0"
)
SEMANTIC_SEARCH_RECOVERY_APPROVAL_SCHEMA = (
    "cme_calendar_search_product_discovery_semantic_recovery_approval/1.0.0"
)
SEMANTIC_SEARCH_RECOVERY_CAPTURE_SCHEMA = (
    "cme_calendar_search_product_discovery_capture/1.2.0"
)
SEMANTIC_SEARCH_RECOVERY_OPERATION = (
    "RECOVER_BOUNDED_PUBLIC_CME_SEMANTIC_SEARCH_PRODUCT_DISCOVERY"
)
COMPLETE_PRODUCT_MAPPING_CANDIDATES_SCHEMA = (
    "cme_calendar_complete_product_mapping_candidates/1.0.0"
)
CALENDAR_ACTIVATION_PLAN_SCHEMA = "cme_calendar_activation_plan/1.0.0"
CALENDAR_ACTIVATION_OPERATION = "ACTIVATE_CME_EXCHANGE_CALENDAR"
SCHEDULE_RECOVERY_PLAN_SCHEMA = (
    "cme_calendar_schedule_coverage_recovery_plan/1.0.0"
)
CAPTURE_GROUP_IDS = (
    "22",
    "58",
    "133",
    "300",
    "316",
    "425",
    "437",
    "5201",
    "8478",
    "10191",
)
CAPTURE_IMPLEMENTATION_PATHS = (
    "configs/data_layout_contract.json",
    "configs/exchange_calendar_policy.json",
    "configs/research_universe_contract.json",
    "configs/source_contract.json",
    "pyproject.toml",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/calendar_cli.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/exchange_calendar.py",
    "src/futures_rebuild/source_contract.py",
)
CAPTURE_OUTPUT_PATHS = {
    "data_template": (
        "data/reference/exchange_calendars/"
        "{release_id}/{ordinal}-{request_id}.{extension}"
    ),
    "manifest_template": "manifests/data_releases/reference/{release_id}.json",
    "publication_lock": "state/locks/data-publication.lock",
    "staging_root": "state/data_publication_staging",
}
CAPTURE_FORBIDDEN_ACTIONS = (
    "ACTIVATE_CALENDAR",
    "CALL_DATABENTO_OR_ANY_NON_CME_PROVIDER",
    "CREATE_LINK_OR_MUTABLE_EXTERNAL_REFERENCE",
    "DELETE_OR_OVERWRITE_ACCEPTED_RELEASE",
    "PARSE_OR_ACCEPT_PRODUCT_MAPPING",
    "REBUILD_FOUNDATION",
    "RETRY_OR_REDIRECT_REQUEST",
    "USE_CREDENTIAL_OR_SECRET",
)
CAPTURE_STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "BYTE_REQUEST_OR_DURATION_CEILING_REACHED",
    "CONTENT_TYPE_OR_HTTP_STATUS_MISMATCH",
    "IMPLEMENTATION_HASH_DRIFT",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
CLIENT_CONTRACT_FORBIDDEN_ACTIONS = (
    "ACTIVATE_CALENDAR",
    "CALL_DATABENTO_OR_ANY_NON_CME_PROVIDER",
    "CAPTURE_PRODUCT_LOOKUP_OR_SCHEDULE",
    "CREATE_LINK_OR_MUTABLE_EXTERNAL_REFERENCE",
    "DELETE_OR_OVERWRITE_ACCEPTED_RELEASE",
    "EVALUATE_OR_EXECUTE_CAPTURED_JAVASCRIPT",
    "PARSE_OR_ACCEPT_PRODUCT_MAPPING",
    "REBUILD_FOUNDATION",
    "RETRY_OR_REDIRECT_REQUEST",
    "USE_CREDENTIAL_OR_SECRET",
)
CLIENT_CONTRACT_STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "ASSET_REFERENCE_DIFFERS_FROM_ACCEPTED_LANDING_RELEASE",
    "BYTE_OR_DURATION_CEILING_REACHED",
    "CONTENT_TYPE_OR_HTTP_STATUS_MISMATCH",
    "IMPLEMENTATION_HASH_DRIFT",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
LANDING_PAGE_URL = "https://www.cmegroup.com/trading-hours.html"
FILTERS_URL = "https://www.cmegroup.com/services/trading-hours-filters?isProtected"
SCHEDULE_URL = "https://www.cmegroup.com/services/trading-hours-by-product"
_SAFE_HEADERS = frozenset(
    {"cache-control", "content-type", "date", "etag", "last-modified"}
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class CalendarCaptureError(UnauthorizedOperation):
    """Raised before or during a bounded public CME capture."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise CalendarCaptureError("CME capture rejected an HTTP redirect")


def _canonical_object(path: Path, *, description: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is not readable JSON") from exc
    if (
        not isinstance(payload, dict)
        or raw != canonical_bytes(payload) + b"\n"
    ):
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def _publisher(boundary: RepoBoundary, *, scope: Mapping[str, str]) -> AtomicPublisher:
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope=scope,
    )
    return AtomicPublisher(
        boundary=boundary,
        operation_receipt=operation,
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )


def _boundary(repository_root: Path, source_contract_path: Path) -> RepoBoundary:
    payload = json.loads(source_contract_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ContractError("source contract must be an object")
    boundary = RepoBoundary(
        Path(str(payload["active_repository"])),
        legacy_roots=legacy_roots_from_contract(payload),
        foreign_roots=(
            Path.home() / "Desktop" / "US_stocks_swing_model",
            Path.home() / "Desktop" / "US_stocks_swing_model_v2",
        ),
    )
    boundary.assert_active_root(repository_root)
    boundary.assert_active_path(
        source_contract_path, purpose="source contract", subtree="configs"
    )
    return boundary


def _capture_implementation_hashes(repository_root: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for relative in CAPTURE_IMPLEMENTATION_PATHS:
        path = repository_root / relative
        if not path.is_file():
            raise IntegrityError(
                f"CME capture implementation input is missing: {relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _three_day_windows(start: date, end: date) -> list[tuple[date, date, date, date]]:
    result: list[tuple[date, date, date, date]] = []
    current = start
    while current <= end:
        core_end = min(end, current + timedelta(days=2))
        result.append(
            (
                current,
                core_end,
                current - timedelta(days=1),
                core_end + timedelta(days=1),
            )
        )
        current = core_end + timedelta(days=1)
    return result


def build_capture_plan(
    *,
    mode: str,
    coverage_start: date,
    coverage_end: date,
    implementation_sha256: Mapping[str, str],
    product_ids: Sequence[str] = (),
    predecessor_capture_release_id: str | None = None,
) -> dict[str, object]:
    normalized_mode = mode.upper().replace("-", "_")
    if normalized_mode not in {
        "BOOTSTRAP",
        "PRODUCT_DISCOVERY",
        "STEADY_STATE",
    }:
        raise ContractError("calendar capture mode is invalid")
    normalized_products = tuple(sorted(set(product_ids)))
    if normalized_products != tuple(product_ids) or any(
        not item or not item.isdigit() for item in normalized_products
    ):
        raise ContractError("CME product IDs must be unique sorted decimal identifiers")
    if normalized_mode in {"BOOTSTRAP", "PRODUCT_DISCOVERY"} and normalized_products:
        raise ContractError("bootstrap/discovery capture cannot pin product IDs")
    if normalized_mode == "STEADY_STATE" and len(normalized_products) != 41:
        raise ContractError("steady-state capture requires exactly 41 CME product IDs")
    if normalized_mode == "PRODUCT_DISCOVERY" and coverage_start != coverage_end:
        raise ContractError("product-discovery capture must cover exactly one date")
    if coverage_start > coverage_end:
        raise ContractError("calendar capture coverage range is invalid")
    normalized_implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(normalized_implementation) != CAPTURE_IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in normalized_implementation.values()
        )
    ):
        raise ContractError("calendar capture implementation hashes are invalid")
    requests: list[dict[str, object]] = [
        {
            "accept": "text/html",
            "request_id": "landing-page",
            "request_kind": "LANDING_PAGE",
            "url": LANDING_PAGE_URL,
        },
        {
            "accept": "application/json",
            "request_id": "filters",
            "request_kind": "FILTERS",
            "url": FILTERS_URL,
        },
    ]
    page_numbers = (1,)
    identifiers = (
        CAPTURE_GROUP_IDS
        if normalized_mode == "BOOTSTRAP"
        else normalized_products
    )
    for window_number, (core_start, core_end, source_start, source_end) in enumerate(
        _three_day_windows(coverage_start, coverage_end), start=1
    ):
        for page_number in page_numbers:
            query_fields = {
                "cleared": "Futures",
                "fromEventDate": source_start.isoformat(),
                "isProtected": "",
                "pageNumber": page_number,
                "pageSize": 999,
                "sortAsc": "true",
                "toEventDate": source_end.isoformat(),
            }
            if identifiers:
                query_fields["id"] = ",".join(identifiers)
            query = urllib.parse.urlencode(query_fields).replace(
                "isProtected=", "isProtected"
            )
            requests.append(
                {
                    "accept": "application/json",
                    "core_end_trade_date": core_end.isoformat(),
                    "core_start_trade_date": core_start.isoformat(),
                    "request_id": f"schedule-{window_number:03d}-p{page_number}",
                    "request_kind": "SCHEDULE",
                    "url": f"{SCHEDULE_URL}?{query}",
                }
            )
    maximum_requests = {
        "BOOTSTRAP": 96,
        "PRODUCT_DISCOVERY": 3,
        "STEADY_STATE": 40,
    }[normalized_mode]
    if len(requests) > maximum_requests:
        raise ContractError("calendar capture plan exceeds its request ceiling")
    scope: dict[str, object] = {
        "allow_redirects": False,
        "coverage_end_trade_date": coverage_end.isoformat(),
        "coverage_start_trade_date": coverage_start.isoformat(),
        "forbidden_actions": list(CAPTURE_FORBIDDEN_ACTIONS),
        "implementation_sha256": normalized_implementation,
        "max_duration_seconds": 900,
        "max_requests": maximum_requests,
        "max_total_bytes": 268_435_456,
        "mode": normalized_mode,
        "output_paths": dict(CAPTURE_OUTPUT_PATHS),
        "predecessor_capture_release_id": predecessor_capture_release_id,
        "product_ids": list(normalized_products),
        "requests": requests,
        "retries": 0,
        "stop_conditions": list(CAPTURE_STOP_CONDITIONS),
        "workers": 1,
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": CAPTURE_OPERATION,
        "schema_version": CAPTURE_PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_capture_plan(payload: Mapping[str, object]) -> dict[str, object]:
    if set(payload) != {
        "classification",
        "execution_authorized",
        "operation",
        "plan_id",
        "schema_version",
        "scope",
    }:
        raise IntegrityError("CME capture plan schema is invalid")
    core = {key: payload[key] for key in payload if key != "plan_id"}
    scope = payload.get("scope")
    if (
        payload.get("schema_version") != CAPTURE_PLAN_SCHEMA
        or payload.get("classification") != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload.get("execution_authorized") is not False
        or payload.get("operation") != CAPTURE_OPERATION
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
        or set(scope)
        != {
            "allow_redirects",
            "coverage_end_trade_date",
            "coverage_start_trade_date",
            "forbidden_actions",
            "implementation_sha256",
            "max_duration_seconds",
            "max_requests",
            "max_total_bytes",
            "mode",
            "output_paths",
            "predecessor_capture_release_id",
            "product_ids",
            "requests",
            "retries",
            "stop_conditions",
            "workers",
        }
    ):
        raise IntegrityError("CME capture plan identity is invalid")
    expected = build_capture_plan(
        mode=str(scope["mode"]),
        coverage_start=date.fromisoformat(str(scope["coverage_start_trade_date"])),
        coverage_end=date.fromisoformat(str(scope["coverage_end_trade_date"])),
        implementation_sha256=(
            scope["implementation_sha256"]
            if isinstance(scope["implementation_sha256"], dict)
            else {}
        ),
        product_ids=tuple(scope["product_ids"]) if isinstance(scope["product_ids"], list) else (),
        predecessor_capture_release_id=(
            str(scope["predecessor_capture_release_id"])
            if scope["predecessor_capture_release_id"] is not None
            else None
        ),
    )
    if dict(payload) != expected:
        raise IntegrityError("CME capture plan differs from the bounded implementation")
    return dict(payload)


def validate_capture_approval(
    approval: Mapping[str, object], *, plan: Mapping[str, object], plan_sha256: str
) -> str:
    core_keys = {
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    if set(approval) != {*core_keys, "approval_receipt_id"}:
        raise CalendarCaptureError("CME capture approval schema is invalid")
    core = {key: approval[key] for key in core_keys}
    if (
        approval.get("schema_version") != CAPTURE_APPROVAL_SCHEMA
        or approval.get("status") != "APPROVED"
        or approval.get("operation") != CAPTURE_OPERATION
        or approval.get("plan_id") != plan["plan_id"]
        or approval.get("plan_sha256") != plan_sha256
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"])) is None
        or approval.get("approval_receipt_id") != sha256_json(core)
    ):
        raise CalendarCaptureError(
            "CME capture lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _client_contract_authority(
    landing_manifest_path: Path, *, boundary: RepoBoundary
) -> dict[str, object]:
    receipt = _receipt_from_manifest(landing_manifest_path, boundary=boundary)
    from .exchange_calendar import load_cme_capture

    capture = load_cme_capture(receipt, boundary=boundary)
    responses = capture["responses"]
    assert isinstance(responses, list)
    landing = [
        item
        for item in responses
        if isinstance(item, dict) and item.get("request_kind") == "LANDING_PAGE"
    ]
    if len(landing) != 1:
        raise IntegrityError("accepted CME capture has no unique landing-page response")
    response = landing[0]
    logical_path = response.get("logical_path")
    if type(logical_path) is not str:
        raise IntegrityError("accepted CME landing-page path is invalid")
    landing_path = receipt.resolve_file(logical_path, boundary)
    if (
        landing_path.stat().st_size != response.get("size")
        or sha256_file(landing_path) != response.get("sha256")
    ):
        raise IntegrityError("accepted CME landing-page evidence changed")
    try:
        source = landing_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise IntegrityError("accepted CME landing page is not readable UTF-8") from exc
    script_sources = [
        html.unescape(match.group("src"))
        for match in re.finditer(
            r"""<script\b[^>]*\bsrc\s*=\s*["'](?P<src>[^"']+)["'][^>]*>""",
            source,
            flags=re.IGNORECASE,
        )
    ]
    asset_paths: list[str] = []
    for value in script_sources:
        parsed = urllib.parse.urlparse(value)
        if parsed.scheme or parsed.netloc:
            if (
                parsed.scheme != "https"
                or parsed.hostname != "www.cmegroup.com"
                or parsed.username is not None
                or parsed.password is not None
            ):
                continue
        if (
            not parsed.query
            and not parsed.fragment
            and CLIENT_CONTRACT_ASSET_PATTERN.fullmatch(parsed.path) is not None
        ):
            asset_paths.append(parsed.path)
    if len(asset_paths) != 1:
        raise IntegrityError(
            "accepted CME landing page has no unique trading-hours client asset"
        )
    manifest_relative = landing_manifest_path.resolve(strict=True).relative_to(
        boundary.active_root
    ).as_posix()
    return {
        "asset_url": f"https://www.cmegroup.com{asset_paths[0]}",
        "landing_capture_release_id": receipt.release_id,
        "landing_logical_path": logical_path,
        "landing_manifest_path": manifest_relative,
        "landing_manifest_sha256": sha256_file(landing_manifest_path),
        "landing_sha256": str(response["sha256"]),
        "landing_size": int(response["size"]),
    }


def build_client_contract_plan(
    *,
    authority: Mapping[str, object],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    expected_authority = {
        "asset_url",
        "landing_capture_release_id",
        "landing_logical_path",
        "landing_manifest_path",
        "landing_manifest_sha256",
        "landing_sha256",
        "landing_size",
    }
    normalized_authority = dict(authority)
    asset_url = normalized_authority.get("asset_url")
    parsed_asset = urllib.parse.urlparse(str(asset_url))
    if (
        set(normalized_authority) != expected_authority
        or type(asset_url) is not str
        or parsed_asset.scheme != "https"
        or parsed_asset.hostname != "www.cmegroup.com"
        or parsed_asset.username is not None
        or parsed_asset.password is not None
        or parsed_asset.query
        or parsed_asset.fragment
        or CLIENT_CONTRACT_ASSET_PATTERN.fullmatch(parsed_asset.path) is None
        or type(normalized_authority["landing_capture_release_id"]) is not str
        or _SHA256.fullmatch(
            str(normalized_authority["landing_capture_release_id"])
        )
        is None
        or type(normalized_authority["landing_manifest_sha256"]) is not str
        or _SHA256.fullmatch(str(normalized_authority["landing_manifest_sha256"]))
        is None
        or type(normalized_authority["landing_sha256"]) is not str
        or _SHA256.fullmatch(str(normalized_authority["landing_sha256"])) is None
        or type(normalized_authority["landing_size"]) is not int
        or int(normalized_authority["landing_size"]) <= 0
        or type(normalized_authority["landing_manifest_path"]) is not str
        or not str(normalized_authority["landing_manifest_path"]).startswith(
            "manifests/data_releases/reference/"
        )
        or type(normalized_authority["landing_logical_path"]) is not str
        or not str(normalized_authority["landing_logical_path"]).startswith(
            "data/reference/exchange_calendars/"
        )
    ):
        raise ContractError("CME client-contract authority is invalid")
    normalized_implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(normalized_implementation) != CAPTURE_IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in normalized_implementation.values()
        )
    ):
        raise ContractError("CME client-contract implementation hashes are invalid")
    release_prefix = str(
        normalized_authority["landing_capture_release_id"]
    )[:8]
    scope: dict[str, object] = {
        "authority": normalized_authority,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": CLIENT_CONTRACT_MAX_DURATION_SECONDS,
            "max_requests": 1,
            "max_total_bytes": CLIENT_CONTRACT_MAX_BYTES,
            "request_timeout_seconds": CLIENT_CONTRACT_MAX_DURATION_SECONDS,
            "retries": 0,
            "workers": 1,
        },
        "forbidden_actions": list(CLIENT_CONTRACT_FORBIDDEN_ACTIONS),
        "implementation_sha256": normalized_implementation,
        "output_paths": {
            "data_template": (
                "data/reference/exchange_calendars/"
                "{release_id}/trading-hours-client.js"
            ),
            "failure_report": (
                "reports/exchange_calendar/"
                f"cme_trading_hours_client_contract_capture_failure_{release_prefix}.json"
            ),
            "manifest_template": (
                "manifests/data_releases/reference/{release_id}.json"
            ),
            "publication_lock": "state/locks/data-publication.lock",
            "staging_root": "state/data_publication_staging",
        },
        "requests": [
            {
                "accept": "application/javascript,text/javascript;q=0.9",
                "request_id": "trading-hours-client-contract",
                "request_kind": "CLIENT_CONTRACT",
                "url": asset_url,
            }
        ],
        "stop_conditions": list(CLIENT_CONTRACT_STOP_CONDITIONS),
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": CLIENT_CONTRACT_OPERATION,
        "schema_version": CLIENT_CONTRACT_PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_client_contract_plan(
    payload: Mapping[str, object],
) -> dict[str, object]:
    if set(payload) != {
        "classification",
        "execution_authorized",
        "operation",
        "plan_id",
        "schema_version",
        "scope",
    }:
        raise IntegrityError("CME client-contract plan schema is invalid")
    scope = payload.get("scope")
    core = {key: payload[key] for key in payload if key != "plan_id"}
    if (
        payload.get("schema_version") != CLIENT_CONTRACT_PLAN_SCHEMA
        or payload.get("classification") != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload.get("execution_authorized") is not False
        or payload.get("operation") != CLIENT_CONTRACT_OPERATION
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
        or set(scope)
        != {
            "authority",
            "bounds",
            "forbidden_actions",
            "implementation_sha256",
            "output_paths",
            "requests",
            "stop_conditions",
        }
        or not isinstance(scope.get("authority"), dict)
        or not isinstance(scope.get("implementation_sha256"), dict)
    ):
        raise IntegrityError("CME client-contract plan identity is invalid")
    expected = build_client_contract_plan(
        authority=scope["authority"],
        implementation_sha256=scope["implementation_sha256"],
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME client-contract plan differs from the bounded implementation"
        )
    return dict(payload)


def validate_client_contract_approval(
    approval: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> str:
    core_keys = {
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    if set(approval) != {*core_keys, "approval_receipt_id"}:
        raise CalendarCaptureError(
            "CME client-contract capture approval schema is invalid"
        )
    core = {key: approval[key] for key in core_keys}
    if (
        approval.get("schema_version") != CLIENT_CONTRACT_APPROVAL_SCHEMA
        or approval.get("status") != "APPROVED"
        or approval.get("operation") != CLIENT_CONTRACT_OPERATION
        or approval.get("plan_id") != plan["plan_id"]
        or approval.get("plan_sha256") != plan_sha256
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"])) is None
        or approval.get("approval_receipt_id") != sha256_json(core)
    ):
        raise CalendarCaptureError(
            "CME client-contract capture lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _safe_client_contract_url(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or CLIENT_CONTRACT_ASSET_PATTERN.fullmatch(parsed.path) is None
    ):
        raise CalendarCaptureError(
            "CME client-contract URL is outside the exact allowlist"
        )


def _safe_url(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or parsed.path
        not in {
            "/trading-hours.html",
            "/services/trading-hours-filters",
            "/services/trading-hours-by-product",
        }
    ):
        raise CalendarCaptureError("CME capture URL is outside the exact allowlist")


def capture_cme_calendar(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    plan = validate_capture_plan(
        _canonical_object(plan_path, description="CME capture plan")
    )
    approval = _canonical_object(
        approval_path, description="CME capture approval"
    )
    approval_id = validate_capture_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != _capture_implementation_hashes(
        publisher.boundary.active_root
    ):
        raise CalendarCaptureError(
            "CME capture implementation hashes differ from the approved plan"
        )
    requests = scope["requests"]
    assert isinstance(requests, list)
    if len(requests) > int(scope["max_requests"]):
        raise CalendarCaptureError("CME capture request ceiling is exceeded")
    stage = publisher.create_stage("cme_calendar_capture")
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    retrieved_at = datetime.now(timezone.utc).replace(microsecond=0)
    total_bytes = 0
    responses: list[dict[str, object]] = []
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    for ordinal, request_spec in enumerate(requests, start=1):
        if not isinstance(request_spec, dict):
            raise CalendarCaptureError("CME capture request is invalid")
        if monotonic_time.monotonic() - started > int(
            scope["max_duration_seconds"]
        ):
            raise CalendarCaptureError("CME capture duration ceiling is exceeded")
        url = str(request_spec["url"])
        _safe_url(url)
        request = urllib.request.Request(
            url,
            headers={
                "Accept": str(request_spec["accept"]),
                "User-Agent": "futures-intraday-model-v2-calendar/1.0",
            },
            method="GET",
        )
        try:
            with opener.open(request, timeout=30) as response:
                if response.status != 200 or response.geturl() != url:
                    raise CalendarCaptureError("CME capture response is not exact HTTP 200")
                content_type = response.headers.get_content_type()
                expected_content_type = str(request_spec["accept"])
                if content_type != expected_content_type:
                    raise CalendarCaptureError(
                        "CME capture response content type is unexpected"
                    )
                remaining = int(scope["max_total_bytes"]) - total_bytes
                body = response.read(remaining + 1)
                if len(body) > remaining:
                    raise CalendarCaptureError("CME capture byte ceiling is exceeded")
                safe_headers = {
                    key.lower(): value
                    for key, value in response.headers.items()
                    if key.lower() in _SAFE_HEADERS
                }
        except CalendarCaptureError:
            raise
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            raise CalendarCaptureError(
                f"CME capture request {ordinal} failed before publication"
            ) from exc
        total_bytes += len(body)
        request_id = str(request_spec["request_id"])
        suffix = "html" if content_type == "text/html" else "json"
        staged_name = f"{ordinal:03d}-{request_id}.{suffix}"
        staged = stage / staged_name
        staged.write_bytes(body)
        logical = f"data/reference/exchange_calendars/{staged_name}"
        logical_paths[staged_name] = logical
        staged_paths[logical] = staged_name
        received_at = datetime.now(timezone.utc).replace(microsecond=0)
        responses.append(
            {
                "content_type": content_type,
                "logical_path": logical,
                "received_at_utc": received_at.isoformat().replace("+00:00", "Z"),
                "request_id": request_id,
                "request_kind": request_spec["request_kind"],
                "safe_headers": dict(sorted(safe_headers.items())),
                "sha256": sha256_file(staged),
                "size": len(body),
                "status_code": 200,
                "url": url,
            }
        )
    elapsed_milliseconds = int(
        (monotonic_time.monotonic() - started) * 1000
    )
    if elapsed_milliseconds > int(scope["max_duration_seconds"]) * 1000:
        raise CalendarCaptureError("CME capture duration ceiling is exceeded")
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "bounds": {
            "allow_redirects": scope["allow_redirects"],
            "max_duration_seconds": scope["max_duration_seconds"],
            "max_requests": scope["max_requests"],
            "max_total_bytes": scope["max_total_bytes"],
            "retries": scope["retries"],
            "workers": scope["workers"],
        },
        "capture_approval": dict(approval),
        "coverage_end_trade_date": scope["coverage_end_trade_date"],
        "coverage_start_trade_date": scope["coverage_start_trade_date"],
        "elapsed_milliseconds": elapsed_milliseconds,
        "mode": scope["mode"],
        "parser_version": PARSER_VERSION,
        "plan_id": plan["plan_id"],
        "predecessor_capture_release_id": scope[
            "predecessor_capture_release_id"
        ],
        "request_count": len(responses),
        "responses": responses,
        "retrieved_at_utc": retrieved_at.isoformat().replace("+00:00", "Z"),
        "schema_version": CAPTURE_SCHEMA_VERSION,
        "total_bytes": total_bytes,
    }
    capture_receipt = {**core, "capture_id": sha256_json(core)}
    predecessor = scope["predecessor_capture_release_id"]
    release = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=CAPTURE_RELEASE_KIND,
        schema_version=CAPTURE_SCHEMA_VERSION,
        logical_paths=logical_paths,
        source_release_ids=(str(predecessor),) if predecessor is not None else (),
        embedded_documents={"capture_receipt.json": capture_receipt},
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture_receipt["capture_id"],
            "coverage_end_trade_date": scope["coverage_end_trade_date"],
            "coverage_start_trade_date": scope["coverage_start_trade_date"],
            "parser_version": PARSER_VERSION,
            "plan_id": plan["plan_id"],
            "retrieved_at_utc": core["retrieved_at_utc"],
        },
    )
    manifest_path = publisher.publish(stage, release, staged_paths=staged_paths)
    receipt = VerifiedReleaseReceipt.from_manifest(manifest_path, publisher.boundary)
    from .exchange_calendar import load_cme_capture

    load_cme_capture(receipt, boundary=publisher.boundary)
    return receipt


def capture_client_contract(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    plan = validate_client_contract_plan(
        _canonical_object(plan_path, description="CME client-contract capture plan")
    )
    approval = _canonical_object(
        approval_path, description="CME client-contract capture approval"
    )
    approval_id = validate_client_contract_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != _capture_implementation_hashes(
        publisher.boundary.active_root
    ):
        raise CalendarCaptureError(
            "CME client-contract implementation hashes differ from the approved plan"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    current_authority = _client_contract_authority(
        publisher.boundary.active_root / str(authority["landing_manifest_path"]),
        boundary=publisher.boundary,
    )
    if current_authority != authority:
        raise CalendarCaptureError(
            "CME client-contract authority differs from the accepted landing release"
        )
    requests = scope["requests"]
    bounds = scope["bounds"]
    assert isinstance(requests, list)
    assert isinstance(bounds, dict)
    if len(requests) != 1 or int(bounds["max_requests"]) != 1:
        raise CalendarCaptureError("CME client-contract request ceiling is invalid")
    request_spec = requests[0]
    assert isinstance(request_spec, dict)
    url = str(request_spec["url"])
    _safe_client_contract_url(url)
    stage = publisher.create_stage("cme_client_contract_capture")
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    retrieved_at = datetime.now(timezone.utc).replace(microsecond=0)
    request = urllib.request.Request(
        url,
        headers={
            "Accept": str(request_spec["accept"]),
            "User-Agent": "futures-intraday-model-v2-calendar/1.0",
        },
        method="GET",
    )
    try:
        with opener.open(
            request, timeout=int(bounds["request_timeout_seconds"])
        ) as response:
            if response.status != 200 or response.geturl() != url:
                raise CalendarCaptureError(
                    "CME client-contract response is not exact HTTP 200"
                )
            content_type = response.headers.get_content_type()
            if content_type not in {"application/javascript", "text/javascript"}:
                raise CalendarCaptureError(
                    "CME client-contract response content type is unexpected"
                )
            body = response.read(int(bounds["max_total_bytes"]) + 1)
            if len(body) > int(bounds["max_total_bytes"]):
                raise CalendarCaptureError(
                    "CME client-contract byte ceiling is exceeded"
                )
            safe_headers = {
                key.lower(): value
                for key, value in response.headers.items()
                if key.lower() in _SAFE_HEADERS
            }
    except CalendarCaptureError:
        raise
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise CalendarCaptureError(
            "CME client-contract request failed before publication"
        ) from exc
    elapsed_milliseconds = int(
        (monotonic_time.monotonic() - started) * 1000
    )
    if elapsed_milliseconds > int(bounds["max_duration_seconds"]) * 1000:
        raise CalendarCaptureError(
            "CME client-contract duration ceiling is exceeded"
        )
    staged_name = "trading-hours-client.js"
    staged = stage / staged_name
    staged.write_bytes(body)
    logical_path = "data/reference/exchange_calendars/trading-hours-client.js"
    received_at = datetime.now(timezone.utc).replace(microsecond=0)
    response_record: dict[str, object] = {
        "content_type": content_type,
        "logical_path": logical_path,
        "received_at_utc": received_at.isoformat().replace("+00:00", "Z"),
        "request_id": request_spec["request_id"],
        "request_kind": request_spec["request_kind"],
        "safe_headers": dict(sorted(safe_headers.items())),
        "sha256": sha256_file(staged),
        "size": len(body),
        "status_code": 200,
        "url": url,
    }
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "authority": authority,
        "bounds": bounds,
        "capture_approval": dict(approval),
        "elapsed_milliseconds": elapsed_milliseconds,
        "plan_id": plan["plan_id"],
        "request_count": 1,
        "response": response_record,
        "retrieved_at_utc": retrieved_at.isoformat().replace("+00:00", "Z"),
        "schema_version": CLIENT_CONTRACT_CAPTURE_SCHEMA,
        "total_bytes": len(body),
    }
    capture_receipt = {**core, "capture_id": sha256_json(core)}
    release = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=CLIENT_CONTRACT_RELEASE_KIND,
        schema_version=CLIENT_CONTRACT_CAPTURE_SCHEMA,
        logical_paths={staged_name: logical_path},
        source_release_ids=(str(authority["landing_capture_release_id"]),),
        embedded_documents={
            "client_contract_capture_receipt.json": capture_receipt
        },
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture_receipt["capture_id"],
            "landing_capture_release_id": authority[
                "landing_capture_release_id"
            ],
            "plan_id": plan["plan_id"],
            "retrieved_at_utc": core["retrieved_at_utc"],
        },
    )
    manifest_path = publisher.publish(
        stage, release, staged_paths={logical_path: staged_name}
    )
    receipt = VerifiedReleaseReceipt.from_manifest(
        manifest_path, publisher.boundary
    )
    load_client_contract_capture(receipt, boundary=publisher.boundary)
    return receipt


def load_client_contract_capture(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "reference"
        or manifest.release_kind != CLIENT_CONTRACT_RELEASE_KIND
        or manifest.schema_version != CLIENT_CONTRACT_CAPTURE_SCHEMA
        or set(manifest.embedded_documents)
        != {"client_contract_capture_receipt.json"}
        or set(manifest.metadata)
        != {
            "approval_receipt_id",
            "capture_id",
            "landing_capture_release_id",
            "plan_id",
            "retrieved_at_utc",
        }
        or len(manifest.files) != 1
        or len(manifest.source_release_ids) != 1
    ):
        raise IntegrityError("CME client-contract capture release is invalid")
    raw = receipt.embedded_document(
        "client_contract_capture_receipt.json", boundary
    )
    if not isinstance(raw, dict):
        raise IntegrityError("CME client-contract capture receipt is invalid")
    payload = dict(raw)
    expected = {
        "approval_receipt_id",
        "authority",
        "bounds",
        "capture_approval",
        "capture_id",
        "elapsed_milliseconds",
        "plan_id",
        "request_count",
        "response",
        "retrieved_at_utc",
        "schema_version",
        "total_bytes",
    }
    if set(payload) != expected:
        raise IntegrityError("CME client-contract capture receipt schema is invalid")
    capture_id = payload.pop("capture_id", None)
    authority = payload.get("authority")
    bounds = payload.get("bounds")
    approval = payload.get("capture_approval")
    response = payload.get("response")
    entry = manifest.files[0]
    if (
        capture_id != sha256_json(payload)
        or capture_id != manifest.metadata["capture_id"]
        or payload.get("schema_version") != CLIENT_CONTRACT_CAPTURE_SCHEMA
        or payload.get("request_count") != 1
        or type(payload.get("total_bytes")) is not int
        or not 0 < int(payload["total_bytes"]) <= CLIENT_CONTRACT_MAX_BYTES
        or type(payload.get("elapsed_milliseconds")) is not int
        or not 0
        <= int(payload["elapsed_milliseconds"])
        <= CLIENT_CONTRACT_MAX_DURATION_SECONDS * 1000
        or not isinstance(authority, dict)
        or not isinstance(bounds, dict)
        or bounds
        != {
            "allow_redirects": False,
            "max_duration_seconds": CLIENT_CONTRACT_MAX_DURATION_SECONDS,
            "max_requests": 1,
            "max_total_bytes": CLIENT_CONTRACT_MAX_BYTES,
            "request_timeout_seconds": CLIENT_CONTRACT_MAX_DURATION_SECONDS,
            "retries": 0,
            "workers": 1,
        }
        or not isinstance(approval, dict)
        or not isinstance(response, dict)
        or set(response)
        != {
            "content_type",
            "logical_path",
            "received_at_utc",
            "request_id",
            "request_kind",
            "safe_headers",
            "sha256",
            "size",
            "status_code",
            "url",
        }
        or response.get("content_type")
        not in {"application/javascript", "text/javascript"}
        or response.get("logical_path") != entry.logical_path
        or response.get("request_id") != "trading-hours-client-contract"
        or response.get("request_kind") != "CLIENT_CONTRACT"
        or response.get("sha256") != entry.sha256
        or response.get("size") != entry.size
        or response.get("size") != payload["total_bytes"]
        or response.get("status_code") != 200
        or type(response.get("url")) is not str
        or response.get("url") != authority.get("asset_url")
        or type(response.get("safe_headers")) is not dict
        or any(
            key not in _SAFE_HEADERS or type(value) is not str
            for key, value in response["safe_headers"].items()
        )
        or manifest.source_release_ids
        != (str(authority.get("landing_capture_release_id")),)
        or manifest.metadata
        != {
            "approval_receipt_id": payload["approval_receipt_id"],
            "capture_id": capture_id,
            "landing_capture_release_id": authority.get(
                "landing_capture_release_id"
            ),
            "plan_id": payload["plan_id"],
            "retrieved_at_utc": payload["retrieved_at_utc"],
        }
    ):
        raise IntegrityError("CME client-contract capture identity is invalid")
    landing_manifest_path = authority.get("landing_manifest_path")
    if type(landing_manifest_path) is not str or _client_contract_authority(
        boundary.active_root / landing_manifest_path,
        boundary=boundary,
    ) != authority:
        raise IntegrityError(
            "CME client-contract capture landing authority is invalid"
        )
    _safe_client_contract_url(str(response["url"]))
    validate_client_contract_approval(
        approval,
        plan={
            "plan_id": payload["plan_id"],
        },
        plan_sha256=str(approval.get("plan_sha256")),
    )
    payload["capture_id"] = capture_id
    return payload


def parse_client_contract_candidates(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    capture = load_client_contract_capture(receipt, boundary=boundary)
    response = capture["response"]
    assert isinstance(response, dict)
    path = receipt.resolve_file(str(response["logical_path"]), boundary)
    raw = path.read_bytes()
    try:
        source = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise IntegrityError("CME client contract is not readable UTF-8") from exc
    literal_pattern = re.compile(
        r"""(?P<quote>["'])(?P<value>(?:\\.|(?!\1).){1,512})(?P=quote)"""
    )
    endpoint_values: dict[tuple[str, int], dict[str, object]] = {}
    query_values: dict[tuple[str, int], dict[str, object]] = {}
    query_allowlist = {
        "cleared",
        "fromEventDate",
        "id",
        "pageNumber",
        "pageSize",
        "product",
        "productId",
        "query",
        "search",
        "sortAsc",
        "symbol",
        "term",
        "toEventDate",
    }
    for match in literal_pattern.finditer(source):
        value = match.group("value")
        if "\\" in value:
            continue
        byte_offset = len(source[: match.start("value")].encode("utf-8"))
        evidence = {
            "byte_offset": byte_offset,
            "literal_sha256": hashlib.sha256(value.encode("utf-8")).hexdigest(),
            "value": value,
        }
        lowered = value.lower()
        parsed = urllib.parse.urlparse(value)
        path_value = parsed.path
        if (
            ("trading-hours" in lowered or "product" in lowered)
            and (
                value.startswith("/services/")
                or (
                    parsed.scheme == "https"
                    and parsed.hostname == "www.cmegroup.com"
                    and path_value.startswith("/services/")
                )
            )
        ):
            endpoint_values[(value, byte_offset)] = evidence
        if value in query_allowlist:
            query_values[(value, byte_offset)] = evidence
    endpoints = [
        endpoint_values[key]
        for key in sorted(endpoint_values, key=lambda item: (item[0], item[1]))
    ]
    query_keys = [
        query_values[key]
        for key in sorted(query_values, key=lambda item: (item[0], item[1]))
    ]
    service_modules: list[dict[str, object]] = []
    for call in re.finditer(
        r"(?P<binding>[A-Za-z_$][A-Za-z0-9_$]*)"
        r"\.(?:getTradingHoursData|getTradingHoursFilters)\b",
        source,
    ):
        binding = re.escape(call.group("binding"))
        imports = list(
            re.finditer(
                rf"{binding}=r\((?P<module>\d+)\)",
                source[: call.start()],
            )
        )
        if not imports or call.start() - imports[-1].start() > 12_000:
            continue
        match = imports[-1]
        value = match.group("module")
        byte_offset = len(source[: match.start("module")].encode("utf-8"))
        service_modules.append(
            {
                "byte_offset": byte_offset,
                "literal_sha256": hashlib.sha256(
                    value.encode("utf-8")
                ).hexdigest(),
                "module_id": value,
            }
        )
    service_modules = sorted(
        {
            (item["module_id"], item["byte_offset"]): item
            for item in service_modules
        }.values(),
        key=lambda item: (item["module_id"], item["byte_offset"]),
    )
    dependency_chunks: list[dict[str, object]] = []
    for match in re.finditer(r"\.O\(0,\[(?P<chunks>[0-9,]+)\]", source):
        for value in match.group("chunks").split(","):
            byte_offset = len(
                source[: match.start("chunks")].encode("utf-8")
            ) + match.group("chunks").index(value)
            dependency_chunks.append(
                {
                    "byte_offset": byte_offset,
                    "literal_sha256": hashlib.sha256(
                        value.encode("utf-8")
                    ).hexdigest(),
                    "chunk_id": value,
                }
            )
    dependency_chunks = sorted(
        {
            (item["chunk_id"], item["byte_offset"]): item
            for item in dependency_chunks
        }.values(),
        key=lambda item: (item["chunk_id"], item["byte_offset"]),
    )
    authority = capture["authority"]
    assert isinstance(authority, dict)
    landing_path = boundary.active_root / str(authority["landing_manifest_path"])
    landing_authority = _client_contract_authority(
        landing_path, boundary=boundary
    )
    landing_receipt = _receipt_from_manifest(landing_path, boundary=boundary)
    landing_file = landing_receipt.resolve_file(
        str(landing_authority["landing_logical_path"]), boundary
    )
    landing_source = landing_file.read_text(encoding="utf-8")
    dependency_assets: list[dict[str, object]] = []
    for match in re.finditer(
        r"""<script\b[^>]*\bsrc\s*=\s*["'](?P<src>[^"']+)["'][^>]*>""",
        landing_source,
        flags=re.IGNORECASE,
    ):
        value = html.unescape(match.group("src"))
        parsed = urllib.parse.urlparse(value)
        if CLIENT_COMMON_ASSET_PATTERN.fullmatch(parsed.path) is None:
            continue
        if parsed.scheme or parsed.netloc:
            if (
                parsed.scheme != "https"
                or parsed.hostname != "www.cmegroup.com"
                or parsed.username is not None
                or parsed.password is not None
            ):
                continue
        if parsed.query or parsed.fragment:
            continue
        byte_offset = len(
            landing_source[: match.start("src")].encode("utf-8")
        )
        asset_url = f"https://www.cmegroup.com{parsed.path}"
        dependency_assets.append(
            {
                "asset_url": asset_url,
                "byte_offset": byte_offset,
                "literal_sha256": hashlib.sha256(
                    asset_url.encode("utf-8")
                ).hexdigest(),
            }
        )
    if not endpoints and (
        not service_modules or not dependency_chunks or len(dependency_assets) != 1
    ):
        raise IntegrityError(
            "CME client contract lacks a closed endpoint or dependency route"
        )
    core: dict[str, object] = {
        "asset_sha256": response["sha256"],
        "capture_release_id": receipt.release_id,
        "dependency_asset_candidates": dependency_assets,
        "dependency_chunk_candidates": dependency_chunks,
        "endpoint_candidates": endpoints,
        "query_key_candidates": query_keys,
        "service_module_candidates": service_modules,
        "schema_version": CLIENT_CONTRACT_CANDIDATES_SCHEMA,
        "status": (
            "ENDPOINT_REVIEW_REQUIRED_NO_NETWORK_AUTHORITY"
            if endpoints
            else "DEPENDENCY_CAPTURE_REQUIRED"
        ),
    }
    return {**core, "candidates_id": sha256_json(core)}


def _client_dependency_authority(
    client_manifest_path: Path, *, boundary: RepoBoundary
) -> dict[str, object]:
    receipt = _receipt_from_manifest(client_manifest_path, boundary=boundary)
    capture = load_client_contract_capture(receipt, boundary=boundary)
    candidates = parse_client_contract_candidates(receipt, boundary=boundary)
    assets = candidates["dependency_asset_candidates"]
    chunks = candidates["dependency_chunk_candidates"]
    modules = candidates["service_module_candidates"]
    if (
        candidates["status"] != "DEPENDENCY_CAPTURE_REQUIRED"
        or not isinstance(assets, list)
        or len(assets) != 1
        or not isinstance(chunks, list)
        or not chunks
        or not isinstance(modules, list)
        or not modules
    ):
        raise IntegrityError(
            "CME client dependency evidence is incomplete or ambiguous"
        )
    asset = assets[0]
    assert isinstance(asset, dict)
    chunk_ids = sorted({str(item["chunk_id"]) for item in chunks})
    module_ids = sorted({str(item["module_id"]) for item in modules})
    authority = capture["authority"]
    assert isinstance(authority, dict)
    return {
        "client_asset_sha256": capture["response"]["sha256"],  # type: ignore[index]
        "client_candidates_id": candidates["candidates_id"],
        "client_capture_manifest_path": (
            client_manifest_path.resolve(strict=True)
            .relative_to(boundary.active_root)
            .as_posix()
        ),
        "client_capture_manifest_sha256": sha256_file(client_manifest_path),
        "client_capture_release_id": receipt.release_id,
        "dependency_asset_url": asset["asset_url"],
        "dependency_chunk_ids": chunk_ids,
        "landing_capture_release_id": authority["landing_capture_release_id"],
        "service_module_ids": module_ids,
    }


def build_client_dependency_plan(
    *,
    authority: Mapping[str, object],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    normalized = dict(authority)
    expected = {
        "client_asset_sha256",
        "client_candidates_id",
        "client_capture_manifest_path",
        "client_capture_manifest_sha256",
        "client_capture_release_id",
        "dependency_asset_url",
        "dependency_chunk_ids",
        "landing_capture_release_id",
        "service_module_ids",
    }
    asset_url = normalized.get("dependency_asset_url")
    parsed = urllib.parse.urlparse(str(asset_url))
    if (
        set(normalized) != expected
        or type(asset_url) is not str
        or parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or CLIENT_COMMON_ASSET_PATTERN.fullmatch(parsed.path) is None
        or any(
            type(normalized[key]) is not str
            or _SHA256.fullmatch(str(normalized[key])) is None
            for key in (
                "client_asset_sha256",
                "client_candidates_id",
                "client_capture_manifest_sha256",
                "client_capture_release_id",
                "landing_capture_release_id",
            )
        )
        or type(normalized["client_capture_manifest_path"]) is not str
        or not str(normalized["client_capture_manifest_path"]).startswith(
            "manifests/data_releases/reference/"
        )
        or not isinstance(normalized["dependency_chunk_ids"], list)
        or not normalized["dependency_chunk_ids"]
        or normalized["dependency_chunk_ids"]
        != sorted(set(normalized["dependency_chunk_ids"]))
        or not isinstance(normalized["service_module_ids"], list)
        or not normalized["service_module_ids"]
        or normalized["service_module_ids"]
        != sorted(set(normalized["service_module_ids"]))
        or any(
            type(value) is not str or not value.isdigit()
            for key in ("dependency_chunk_ids", "service_module_ids")
            for value in normalized[key]
        )
    ):
        raise ContractError("CME client-dependency authority is invalid")
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != CAPTURE_IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
    ):
        raise ContractError("CME client-dependency implementation hashes are invalid")
    scope: dict[str, object] = {
        "authority": normalized,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": CLIENT_CONTRACT_MAX_DURATION_SECONDS,
            "max_requests": 1,
            "max_total_bytes": CLIENT_DEPENDENCY_MAX_BYTES,
            "request_timeout_seconds": CLIENT_CONTRACT_MAX_DURATION_SECONDS,
            "retries": 0,
            "workers": 1,
        },
        "forbidden_actions": list(CLIENT_CONTRACT_FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "output_paths": {
            "data_template": (
                "data/reference/exchange_calendars/"
                "{release_id}/trading-hours-common.js"
            ),
            "manifest_template": (
                "manifests/data_releases/reference/{release_id}.json"
            ),
            "publication_lock": "state/locks/data-publication.lock",
            "staging_root": "state/data_publication_staging",
        },
        "requests": [
            {
                "accept": "application/javascript,text/javascript;q=0.9",
                "request_id": "trading-hours-common-dependency",
                "request_kind": "CLIENT_DEPENDENCY",
                "url": asset_url,
            }
        ],
        "stop_conditions": list(CLIENT_CONTRACT_STOP_CONDITIONS),
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": CLIENT_DEPENDENCY_OPERATION,
        "schema_version": CLIENT_DEPENDENCY_PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_client_dependency_plan(
    payload: Mapping[str, object],
) -> dict[str, object]:
    if set(payload) != {
        "classification",
        "execution_authorized",
        "operation",
        "plan_id",
        "schema_version",
        "scope",
    }:
        raise IntegrityError("CME client-dependency plan schema is invalid")
    scope = payload.get("scope")
    core = {key: payload[key] for key in payload if key != "plan_id"}
    if (
        payload.get("schema_version") != CLIENT_DEPENDENCY_PLAN_SCHEMA
        or payload.get("classification") != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload.get("execution_authorized") is not False
        or payload.get("operation") != CLIENT_DEPENDENCY_OPERATION
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
        or not isinstance(scope.get("authority"), dict)
        or not isinstance(scope.get("implementation_sha256"), dict)
    ):
        raise IntegrityError("CME client-dependency plan identity is invalid")
    expected = build_client_dependency_plan(
        authority=scope["authority"],
        implementation_sha256=scope["implementation_sha256"],
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME client-dependency plan differs from the bounded implementation"
        )
    return dict(payload)


def validate_client_dependency_approval(
    approval: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> str:
    core_keys = {
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    if set(approval) != {*core_keys, "approval_receipt_id"}:
        raise CalendarCaptureError(
            "CME client-dependency approval schema is invalid"
        )
    core = {key: approval[key] for key in core_keys}
    if (
        approval.get("schema_version") != CLIENT_DEPENDENCY_APPROVAL_SCHEMA
        or approval.get("status") != "APPROVED"
        or approval.get("operation") != CLIENT_DEPENDENCY_OPERATION
        or approval.get("plan_id") != plan["plan_id"]
        or approval.get("plan_sha256") != plan_sha256
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"])) is None
        or approval.get("approval_receipt_id") != sha256_json(core)
    ):
        raise CalendarCaptureError(
            "CME client-dependency lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def capture_client_dependency(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    plan = validate_client_dependency_plan(
        _canonical_object(plan_path, description="CME client-dependency plan")
    )
    approval = _canonical_object(
        approval_path, description="CME client-dependency approval"
    )
    approval_id = validate_client_dependency_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != _capture_implementation_hashes(
        publisher.boundary.active_root
    ):
        raise CalendarCaptureError(
            "CME client-dependency implementation hashes differ from the plan"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    current = _client_dependency_authority(
        publisher.boundary.active_root
        / str(authority["client_capture_manifest_path"]),
        boundary=publisher.boundary,
    )
    if current != authority:
        raise CalendarCaptureError("CME client-dependency authority changed")
    bounds = scope["bounds"]
    requests = scope["requests"]
    assert isinstance(bounds, dict)
    assert isinstance(requests, list)
    request_spec = requests[0]
    assert isinstance(request_spec, dict)
    url = str(request_spec["url"])
    parsed = urllib.parse.urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.query
        or parsed.fragment
        or CLIENT_COMMON_ASSET_PATTERN.fullmatch(parsed.path) is None
    ):
        raise CalendarCaptureError("CME client-dependency URL is not exact")
    stage = publisher.create_stage("cme_client_dependency_capture")
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    retrieved_at = datetime.now(timezone.utc).replace(microsecond=0)
    request = urllib.request.Request(
        url,
        headers={
            "Accept": str(request_spec["accept"]),
            "User-Agent": "futures-intraday-model-v2-calendar/1.0",
        },
        method="GET",
    )
    try:
        with opener.open(request, timeout=30) as response:
            if response.status != 200 or response.geturl() != url:
                raise CalendarCaptureError(
                    "CME client-dependency response is not exact HTTP 200"
                )
            content_type = response.headers.get_content_type()
            if content_type not in {"application/javascript", "text/javascript"}:
                raise CalendarCaptureError(
                    "CME client-dependency content type is unexpected"
                )
            body = response.read(CLIENT_DEPENDENCY_MAX_BYTES + 1)
            if len(body) > CLIENT_DEPENDENCY_MAX_BYTES:
                raise CalendarCaptureError(
                    "CME client-dependency byte ceiling is exceeded"
                )
            safe_headers = {
                key.lower(): value
                for key, value in response.headers.items()
                if key.lower() in _SAFE_HEADERS
            }
    except CalendarCaptureError:
        raise
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise CalendarCaptureError(
            "CME client-dependency request failed before publication"
        ) from exc
    elapsed = int((monotonic_time.monotonic() - started) * 1000)
    if elapsed > CLIENT_CONTRACT_MAX_DURATION_SECONDS * 1000:
        raise CalendarCaptureError(
            "CME client-dependency duration ceiling is exceeded"
        )
    staged_name = "trading-hours-common.js"
    staged = stage / staged_name
    staged.write_bytes(body)
    logical = "data/reference/exchange_calendars/trading-hours-common.js"
    response_record = {
        "content_type": content_type,
        "logical_path": logical,
        "received_at_utc": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "request_id": request_spec["request_id"],
        "request_kind": request_spec["request_kind"],
        "safe_headers": dict(sorted(safe_headers.items())),
        "sha256": sha256_file(staged),
        "size": len(body),
        "status_code": 200,
        "url": url,
    }
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "authority": authority,
        "bounds": bounds,
        "capture_approval": dict(approval),
        "elapsed_milliseconds": elapsed,
        "plan_id": plan["plan_id"],
        "response": response_record,
        "retrieved_at_utc": retrieved_at.isoformat().replace("+00:00", "Z"),
        "schema_version": CLIENT_DEPENDENCY_CAPTURE_SCHEMA,
    }
    capture = {**core, "capture_id": sha256_json(core)}
    release = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=CLIENT_DEPENDENCY_RELEASE_KIND,
        schema_version=CLIENT_DEPENDENCY_CAPTURE_SCHEMA,
        logical_paths={staged_name: logical},
        source_release_ids=(str(authority["client_capture_release_id"]),),
        embedded_documents={"client_dependency_capture_receipt.json": capture},
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture["capture_id"],
            "client_capture_release_id": authority["client_capture_release_id"],
            "plan_id": plan["plan_id"],
            "retrieved_at_utc": core["retrieved_at_utc"],
        },
    )
    manifest_path = publisher.publish(
        stage, release, staged_paths={logical: staged_name}
    )
    receipt = VerifiedReleaseReceipt.from_manifest(
        manifest_path, publisher.boundary
    )
    load_client_dependency_capture(receipt, boundary=publisher.boundary)
    return receipt


def load_client_dependency_capture(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "reference"
        or manifest.release_kind != CLIENT_DEPENDENCY_RELEASE_KIND
        or manifest.schema_version != CLIENT_DEPENDENCY_CAPTURE_SCHEMA
        or set(manifest.embedded_documents)
        != {"client_dependency_capture_receipt.json"}
        or len(manifest.files) != 1
    ):
        raise IntegrityError("CME client-dependency release is invalid")
    raw = receipt.embedded_document(
        "client_dependency_capture_receipt.json", boundary
    )
    if not isinstance(raw, dict):
        raise IntegrityError("CME client-dependency receipt is invalid")
    payload = dict(raw)
    capture_id = payload.pop("capture_id", None)
    response = payload.get("response")
    authority = payload.get("authority")
    if (
        capture_id != sha256_json(payload)
        or capture_id != manifest.metadata.get("capture_id")
        or payload.get("schema_version") != CLIENT_DEPENDENCY_CAPTURE_SCHEMA
        or not isinstance(response, dict)
        or not isinstance(authority, dict)
        or response.get("logical_path") != manifest.files[0].logical_path
        or response.get("sha256") != manifest.files[0].sha256
        or response.get("size") != manifest.files[0].size
        or response.get("status_code") != 200
        or response.get("request_kind") != "CLIENT_DEPENDENCY"
        or response.get("url") != authority.get("dependency_asset_url")
        or manifest.source_release_ids
        != (str(authority.get("client_capture_release_id")),)
        or _client_dependency_authority(
            boundary.active_root
            / str(authority.get("client_capture_manifest_path")),
            boundary=boundary,
        )
        != authority
    ):
        raise IntegrityError("CME client-dependency identity is invalid")
    payload["capture_id"] = capture_id
    return payload


def parse_client_dependency_candidates(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    capture = load_client_dependency_capture(receipt, boundary=boundary)
    response = capture["response"]
    assert isinstance(response, dict)
    source = receipt.resolve_file(
        str(response["logical_path"]), boundary
    ).read_text(encoding="utf-8")
    module_start = source.find("23031:")
    if module_start < 0:
        raise IntegrityError("CME dependency lacks trading-hours service module")
    next_module = re.search(r"\},\d+:\(", source[module_start + 6 :])
    if next_module is None:
        raise IntegrityError("CME trading-hours service module boundary is absent")
    module_end = module_start + 6 + next_module.start()
    module = source[module_start:module_end]
    required_fragments = (
        "getTradingHoursData",
        "/services/trading-hours-by-product?",
        '"id=".concat(t,"&pageNumber="',
        '"&searchString=".concat(encodeURIComponent(u))',
        '"&fromEventDate=".concat(p)',
        '"&toEventDate=".concat(h)',
    )
    if any(fragment not in module for fragment in required_fragments):
        raise IntegrityError(
            "CME trading-hours endpoint construction is unexpected"
        )
    endpoint = "/services/trading-hours-by-product"
    query_keys = (
        "id",
        "pageNumber",
        "pageSize",
        "exch",
        "cleared",
        "group",
        "subGroup",
        "searchString",
        "sortField",
        "sortAsc",
        "fromEventDate",
        "toEventDate",
    )
    evidence = []
    for value in (endpoint, *query_keys):
        offset = module.find(value)
        if offset < 0:
            raise IntegrityError(
                "CME trading-hours query contract is incomplete"
            )
        absolute = module_start + offset
        evidence.append(
            {
                "byte_offset": len(source[:absolute].encode("utf-8")),
                "literal_sha256": hashlib.sha256(
                    value.encode("utf-8")
                ).hexdigest(),
                "value": value,
            }
        )
    core: dict[str, object] = {
        "argument_order": list(query_keys) + ["download"],
        "asset_sha256": response["sha256"],
        "capture_release_id": receipt.release_id,
        "endpoint": endpoint,
        "evidence": evidence,
        "module_id": "23031",
        "query_keys": list(query_keys),
        "schema_version": CLIENT_DEPENDENCY_ENDPOINT_SCHEMA,
        "status": "NONEMPTY_ID_DISCOVERY_PLAN_READY",
    }
    return {**core, "candidates_id": sha256_json(core)}


def _nonempty_discovery_authority(
    dependency_manifest_path: Path, *, boundary: RepoBoundary
) -> dict[str, object]:
    receipt = _receipt_from_manifest(dependency_manifest_path, boundary=boundary)
    capture = load_client_dependency_capture(receipt, boundary=boundary)
    candidates = parse_client_dependency_candidates(receipt, boundary=boundary)
    return {
        "dependency_asset_sha256": capture["response"]["sha256"],  # type: ignore[index]
        "dependency_candidates_id": candidates["candidates_id"],
        "dependency_capture_manifest_path": (
            dependency_manifest_path.resolve(strict=True)
            .relative_to(boundary.active_root)
            .as_posix()
        ),
        "dependency_capture_manifest_sha256": sha256_file(
            dependency_manifest_path
        ),
        "dependency_capture_release_id": receipt.release_id,
        "endpoint": candidates["endpoint"],
        "endpoint_module_id": candidates["module_id"],
    }


def build_nonempty_discovery_plan(
    *,
    authority: Mapping[str, object],
    coverage_date: date,
    expected_markets: Sequence[str],
    implementation_sha256: Mapping[str, str],
    universe_contract_sha256: str,
) -> dict[str, object]:
    markets = tuple(sorted(set(expected_markets)))
    if tuple(expected_markets) != markets or set(markets) != set(
        NONEMPTY_DISCOVERY_GROUP_BY_MARKET
    ):
        raise ContractError(
            "nonempty discovery requires the exact 41-market universe"
        )
    normalized_authority = dict(authority)
    if (
        set(normalized_authority)
        != {
            "dependency_asset_sha256",
            "dependency_candidates_id",
            "dependency_capture_manifest_path",
            "dependency_capture_manifest_sha256",
            "dependency_capture_release_id",
            "endpoint",
            "endpoint_module_id",
        }
        or normalized_authority.get("endpoint")
        != "/services/trading-hours-by-product"
        or normalized_authority.get("endpoint_module_id") != "23031"
        or any(
            type(normalized_authority[key]) is not str
            or _SHA256.fullmatch(str(normalized_authority[key])) is None
            for key in (
                "dependency_asset_sha256",
                "dependency_candidates_id",
                "dependency_capture_manifest_sha256",
                "dependency_capture_release_id",
            )
        )
        or _SHA256.fullmatch(universe_contract_sha256) is None
    ):
        raise ContractError("nonempty discovery authority is invalid")
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != CAPTURE_IMPLEMENTATION_PATHS
        or any(_SHA256.fullmatch(value) is None for value in implementation.values())
    ):
        raise ContractError("nonempty discovery implementation hashes are invalid")
    source_start = coverage_date - timedelta(days=1)
    source_end = coverage_date + timedelta(days=1)
    requests = []
    for market in markets:
        query = urllib.parse.urlencode(
            {
                "id": NONEMPTY_DISCOVERY_GROUP_BY_MARKET[market],
                "pageNumber": 1,
                "pageSize": 999,
                "cleared": "Futures",
                "searchString": market,
                "sortAsc": "true",
                "fromEventDate": source_start.isoformat(),
                "toEventDate": source_end.isoformat(),
            }
        )
        requests.append(
            {
                "accept": "application/json",
                "market": market,
                "request_id": f"discover-{market.lower()}",
                "request_kind": "NONEMPTY_PRODUCT_DISCOVERY",
                "url": f"https://www.cmegroup.com/services/trading-hours-by-product?{query}",
            }
        )
    scope: dict[str, object] = {
        "authority": normalized_authority,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": 900,
            "max_requests": 41,
            "max_total_bytes": 67_108_864,
            "request_timeout_seconds": 30,
            "retries": 0,
            "workers": 1,
        },
        "coverage_date": coverage_date.isoformat(),
        "forbidden_actions": list(CAPTURE_FORBIDDEN_ACTIONS),
        "group_id_by_market": dict(
            sorted(NONEMPTY_DISCOVERY_GROUP_BY_MARKET.items())
        ),
        "implementation_sha256": implementation,
        "output_paths": dict(CAPTURE_OUTPUT_PATHS),
        "requests": requests,
        "stop_conditions": list(CAPTURE_STOP_CONDITIONS),
        "universe_contract_sha256": universe_contract_sha256,
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": NONEMPTY_DISCOVERY_OPERATION,
        "schema_version": NONEMPTY_DISCOVERY_PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_nonempty_discovery_plan(
    payload: Mapping[str, object],
) -> dict[str, object]:
    scope = payload.get("scope")
    core = {key: payload[key] for key in payload if key != "plan_id"}
    if (
        set(payload)
        != {
            "classification",
            "execution_authorized",
            "operation",
            "plan_id",
            "schema_version",
            "scope",
        }
        or payload.get("schema_version") != NONEMPTY_DISCOVERY_PLAN_SCHEMA
        or payload.get("operation") != NONEMPTY_DISCOVERY_OPERATION
        or payload.get("execution_authorized") is not False
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
    ):
        raise IntegrityError("nonempty discovery plan identity is invalid")
    expected = build_nonempty_discovery_plan(
        authority=scope["authority"],  # type: ignore[arg-type]
        coverage_date=date.fromisoformat(str(scope["coverage_date"])),
        expected_markets=tuple(sorted(scope["group_id_by_market"])),  # type: ignore[arg-type]
        implementation_sha256=scope["implementation_sha256"],  # type: ignore[arg-type]
        universe_contract_sha256=str(scope["universe_contract_sha256"]),
    )
    if dict(payload) != expected:
        raise IntegrityError("nonempty discovery plan is not reproducible")
    return dict(payload)


def validate_nonempty_discovery_approval(
    approval: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> str:
    core_keys = {
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    core = {key: approval[key] for key in core_keys if key in approval}
    if (
        set(approval) != {*core_keys, "approval_receipt_id"}
        or approval.get("schema_version") != NONEMPTY_DISCOVERY_APPROVAL_SCHEMA
        or approval.get("operation") != NONEMPTY_DISCOVERY_OPERATION
        or approval.get("status") != "APPROVED"
        or approval.get("plan_id") != plan["plan_id"]
        or approval.get("plan_sha256") != plan_sha256
        or approval.get("approval_receipt_id") != sha256_json(core)
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"])) is None
    ):
        raise CalendarCaptureError(
            "nonempty discovery lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def capture_nonempty_discovery(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    plan = validate_nonempty_discovery_plan(
        _canonical_object(plan_path, description="nonempty discovery plan")
    )
    approval = _canonical_object(
        approval_path, description="nonempty discovery approval"
    )
    approval_id = validate_nonempty_discovery_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != _capture_implementation_hashes(
        publisher.boundary.active_root
    ):
        raise CalendarCaptureError(
            "nonempty discovery implementation hashes changed"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    if _nonempty_discovery_authority(
        publisher.boundary.active_root
        / str(authority["dependency_capture_manifest_path"]),
        boundary=publisher.boundary,
    ) != authority:
        raise CalendarCaptureError("nonempty discovery authority changed")
    if sha256_file(
        publisher.boundary.active_root
        / "configs"
        / "research_universe_contract.json"
    ) != scope["universe_contract_sha256"]:
        raise CalendarCaptureError("nonempty discovery universe contract changed")
    stage = publisher.create_stage("cme_nonempty_product_discovery")
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    total = 0
    responses = []
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    for ordinal, spec in enumerate(scope["requests"], start=1):  # type: ignore[union-attr]
        assert isinstance(spec, dict)
        url = str(spec["url"])
        _safe_url(url)
        if "id=" not in url or "searchString=" not in url:
            raise CalendarCaptureError("nonempty discovery request is not nonempty")
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "futures-intraday-model-v2-calendar/1.0",
            },
            method="GET",
        )
        try:
            with opener.open(request, timeout=30) as response:
                if (
                    response.status != 200
                    or response.geturl() != url
                    or response.headers.get_content_type() != "application/json"
                ):
                    raise CalendarCaptureError(
                        "nonempty discovery response contract failed"
                    )
                remaining = 67_108_864 - total
                body = response.read(remaining + 1)
                if len(body) > remaining:
                    raise CalendarCaptureError(
                        "nonempty discovery byte ceiling exceeded"
                    )
        except CalendarCaptureError:
            raise
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            raise CalendarCaptureError(
                f"nonempty discovery request {ordinal} failed"
            ) from exc
        total += len(body)
        market = str(spec["market"])
        name = f"{ordinal:03d}-{market}.json"
        staged = stage / name
        staged.write_bytes(body)
        logical = f"data/reference/exchange_calendars/{name}"
        logical_paths[name] = logical
        staged_paths[logical] = name
        responses.append(
            {
                "logical_path": logical,
                "market": market,
                "request_id": spec["request_id"],
                "sha256": sha256_file(staged),
                "size": len(body),
                "status_code": 200,
                "url": url,
            }
        )
    elapsed = int((monotonic_time.monotonic() - started) * 1000)
    if elapsed > 900_000:
        raise CalendarCaptureError("nonempty discovery duration ceiling exceeded")
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "authority": authority,
        "capture_approval": dict(approval),
        "elapsed_milliseconds": elapsed,
        "plan_id": plan["plan_id"],
        "responses": responses,
        "schema_version": NONEMPTY_DISCOVERY_CAPTURE_SCHEMA,
        "total_bytes": total,
    }
    capture = {**core, "capture_id": sha256_json(core)}
    release = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=NONEMPTY_DISCOVERY_RELEASE_KIND,
        schema_version=NONEMPTY_DISCOVERY_CAPTURE_SCHEMA,
        logical_paths=logical_paths,
        source_release_ids=(str(authority["dependency_capture_release_id"]),),
        embedded_documents={"nonempty_discovery_capture.json": capture},
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture["capture_id"],
            "plan_id": plan["plan_id"],
        },
    )
    manifest_path = publisher.publish(stage, release, staged_paths=staged_paths)
    return VerifiedReleaseReceipt.from_manifest(
        manifest_path, publisher.boundary
    )


def load_nonempty_discovery_capture(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "reference"
        or manifest.release_kind != NONEMPTY_DISCOVERY_RELEASE_KIND
        or manifest.schema_version != NONEMPTY_DISCOVERY_CAPTURE_SCHEMA
        or set(manifest.embedded_documents)
        != {"nonempty_discovery_capture.json"}
        or len(manifest.files) != 41
    ):
        raise IntegrityError("nonempty discovery release contract is invalid")
    raw = receipt.embedded_document(
        "nonempty_discovery_capture.json", boundary
    )
    if not isinstance(raw, dict):
        raise IntegrityError("nonempty discovery capture receipt is invalid")
    payload = dict(raw)
    capture_id = payload.pop("capture_id", None)
    responses = payload.get("responses")
    authority = payload.get("authority")
    if (
        capture_id != sha256_json(payload)
        or capture_id != manifest.metadata.get("capture_id")
        or payload.get("schema_version") != NONEMPTY_DISCOVERY_CAPTURE_SCHEMA
        or not isinstance(responses, list)
        or len(responses) != 41
        or not isinstance(authority, dict)
        or manifest.source_release_ids
        != (str(authority.get("dependency_capture_release_id")),)
    ):
        raise IntegrityError("nonempty discovery capture identity is invalid")
    entries = {entry.logical_path: entry for entry in manifest.files}
    markets = []
    for response in responses:
        if (
            not isinstance(response, dict)
            or set(response)
            != {
                "logical_path",
                "market",
                "request_id",
                "sha256",
                "size",
                "status_code",
                "url",
            }
            or response.get("logical_path") not in entries
            or response.get("status_code") != 200
            or response.get("sha256")
            != entries[str(response["logical_path"])].sha256
            or response.get("size")
            != entries[str(response["logical_path"])].size
            or "id=" not in str(response.get("url"))
            or "searchString=" not in str(response.get("url"))
        ):
            raise IntegrityError("nonempty discovery response closure is invalid")
        markets.append(str(response["market"]))
    if tuple(markets) != tuple(sorted(NONEMPTY_DISCOVERY_GROUP_BY_MARKET)):
        raise IntegrityError("nonempty discovery market census is invalid")
    payload["capture_id"] = capture_id
    return payload


def generate_nonempty_discovery_candidates(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    capture = load_nonempty_discovery_capture(receipt, boundary=boundary)
    resolved = []
    missing = []
    for response in capture["responses"]:  # type: ignore[union-attr]
        assert isinstance(response, dict)
        market = str(response["market"])
        path = receipt.resolve_file(str(response["logical_path"]), boundary)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise IntegrityError(
                "nonempty discovery response JSON is invalid"
            ) from exc
        if (
            not isinstance(payload, dict)
            or set(payload) != {"products", "props"}
            or not isinstance(payload["products"], list)
            or not isinstance(payload["props"], dict)
        ):
            raise IntegrityError("nonempty discovery service schema is invalid")
        exact = [
            item
            for item in payload["products"]
            if isinstance(item, dict)
            and str(item.get("globex", "")).upper() == market
            and type(item.get("id")) is int
        ]
        if len(exact) == 1:
            item = exact[0]
            resolved.append(
                {
                    "cme_product_id": str(item["id"]),
                    "globex": str(item["globex"]),
                    "market": market,
                    "name": str(item.get("name", "")),
                    "response_sha256": response["sha256"],
                }
            )
        elif not exact and not payload["products"]:
            missing.append(market)
        else:
            raise IntegrityError(
                f"nonempty discovery result is ambiguous for {market}"
            )
    core: dict[str, object] = {
        "capture_release_id": receipt.release_id,
        "missing_markets": missing,
        "resolved": resolved,
        "schema_version": NONEMPTY_DISCOVERY_CANDIDATES_SCHEMA,
        "status": "INCOMPLETE_REQUIRES_BOUNDED_SEARCH_SUCCESSOR",
    }
    return {**core, "candidates_id": sha256_json(core)}


def build_search_discovery_plan(
    *,
    predecessor_receipt: VerifiedReleaseReceipt,
    predecessor_candidates: Mapping[str, object],
    dependency_authority: Mapping[str, object],
    coverage_date: date,
    implementation_sha256: Mapping[str, str],
    universe_contract_sha256: str,
) -> dict[str, object]:
    missing = predecessor_candidates.get("missing_markets")
    if (
        predecessor_candidates.get("status")
        != "INCOMPLETE_REQUIRES_BOUNDED_SEARCH_SUCCESSOR"
        or not isinstance(missing, list)
        or len(missing) != 33
        or missing != sorted(missing)
        or any(market not in NONEMPTY_DISCOVERY_GROUP_BY_MARKET for market in missing)
        or dependency_authority.get("endpoint")
        != "/services/trading-hours-by-product"
    ):
        raise ContractError("search discovery predecessor evidence is invalid")
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != CAPTURE_IMPLEMENTATION_PATHS
        or _SHA256.fullmatch(universe_contract_sha256) is None
    ):
        raise ContractError("search discovery implementation authority is invalid")
    source_start = coverage_date - timedelta(days=1)
    source_end = coverage_date + timedelta(days=1)
    requests = []
    for market in missing:
        query = urllib.parse.urlencode(
            {
                "pageNumber": 1,
                "pageSize": 999,
                "cleared": "Futures",
                "searchString": market,
                "sortAsc": "true",
                "fromEventDate": source_start.isoformat(),
                "toEventDate": source_end.isoformat(),
            }
        )
        requests.append(
            {
                "accept": "application/json",
                "market": market,
                "request_id": f"search-{market.lower()}",
                "request_kind": "SEARCH_PRODUCT_DISCOVERY",
                "url": f"https://www.cmegroup.com/services/trading-hours-by-product?{query}",
            }
        )
    authority = {
        "dependency_authority": dict(dependency_authority),
        "predecessor_candidates_id": predecessor_candidates["candidates_id"],
        "predecessor_capture_manifest_path": predecessor_receipt.manifest_path,
        "predecessor_capture_manifest_sha256": predecessor_receipt.manifest_sha256,
        "predecessor_capture_release_id": predecessor_receipt.release_id,
    }
    scope: dict[str, object] = {
        "authority": authority,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": 900,
            "max_requests": 33,
            "max_total_bytes": 67_108_864,
            "request_timeout_seconds": 30,
            "retries": 0,
            "workers": 1,
        },
        "coverage_date": coverage_date.isoformat(),
        "forbidden_actions": list(CAPTURE_FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "output_paths": dict(CAPTURE_OUTPUT_PATHS),
        "requests": requests,
        "stop_conditions": list(CAPTURE_STOP_CONDITIONS),
        "universe_contract_sha256": universe_contract_sha256,
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": SEARCH_DISCOVERY_OPERATION,
        "schema_version": SEARCH_DISCOVERY_PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_search_discovery_plan(
    payload: Mapping[str, object], *, boundary: RepoBoundary
) -> dict[str, object]:
    scope = payload.get("scope")
    core = {key: payload[key] for key in payload if key != "plan_id"}
    if (
        set(payload)
        != {
            "classification",
            "execution_authorized",
            "operation",
            "plan_id",
            "schema_version",
            "scope",
        }
        or payload.get("schema_version") != SEARCH_DISCOVERY_PLAN_SCHEMA
        or payload.get("operation") != SEARCH_DISCOVERY_OPERATION
        or payload.get("execution_authorized") is not False
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
        or not isinstance(scope.get("authority"), dict)
    ):
        raise IntegrityError("search discovery plan identity is invalid")
    authority = scope["authority"]
    assert isinstance(authority, dict)
    predecessor = _receipt_from_manifest(
        boundary.active_root / str(authority["predecessor_capture_manifest_path"]),
        boundary=boundary,
    )
    candidates = generate_nonempty_discovery_candidates(
        predecessor, boundary=boundary
    )
    dependency = authority["dependency_authority"]
    assert isinstance(dependency, dict)
    expected = build_search_discovery_plan(
        predecessor_receipt=predecessor,
        predecessor_candidates=candidates,
        dependency_authority=dependency,
        coverage_date=date.fromisoformat(str(scope["coverage_date"])),
        implementation_sha256=scope["implementation_sha256"],  # type: ignore[arg-type]
        universe_contract_sha256=str(scope["universe_contract_sha256"]),
    )
    if dict(payload) != expected:
        raise IntegrityError("search discovery plan is not reproducible")
    return dict(payload)


def validate_search_discovery_approval(
    approval: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> str:
    core_keys = {
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    core = {key: approval[key] for key in core_keys if key in approval}
    if (
        set(approval) != {*core_keys, "approval_receipt_id"}
        or approval.get("schema_version") != SEARCH_DISCOVERY_APPROVAL_SCHEMA
        or approval.get("operation") != SEARCH_DISCOVERY_OPERATION
        or approval.get("status") != "APPROVED"
        or approval.get("plan_id") != plan["plan_id"]
        or approval.get("plan_sha256") != plan_sha256
        or approval.get("approval_receipt_id") != sha256_json(core)
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"])) is None
    ):
        raise CalendarCaptureError(
            "search discovery lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def capture_search_discovery(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    plan = validate_search_discovery_plan(
        _canonical_object(plan_path, description="search discovery plan"),
        boundary=publisher.boundary,
    )
    approval = _canonical_object(
        approval_path, description="search discovery approval"
    )
    approval_id = validate_search_discovery_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != _capture_implementation_hashes(
        publisher.boundary.active_root
    ):
        raise CalendarCaptureError("search discovery implementation changed")
    stage = publisher.create_stage("cme_search_product_discovery")
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    total = 0
    responses = []
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    for ordinal, spec in enumerate(scope["requests"], start=1):  # type: ignore[union-attr]
        assert isinstance(spec, dict)
        url = str(spec["url"])
        _safe_url(url)
        if "id=" in url or "searchString=" not in url:
            raise CalendarCaptureError("search discovery request is not exact")
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "futures-intraday-model-v2-calendar/1.0",
            },
            method="GET",
        )
        try:
            with opener.open(request, timeout=30) as response:
                if (
                    response.status != 200
                    or response.geturl() != url
                    or response.headers.get_content_type() != "application/json"
                ):
                    raise CalendarCaptureError(
                        "search discovery response contract failed"
                    )
                body = response.read(67_108_864 - total + 1)
                if total + len(body) > 67_108_864:
                    raise CalendarCaptureError(
                        "search discovery byte ceiling exceeded"
                    )
        except CalendarCaptureError:
            raise
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            raise CalendarCaptureError(
                f"search discovery request {ordinal} failed"
            ) from exc
        total += len(body)
        market = str(spec["market"])
        name = f"{ordinal:03d}-{market}.json"
        staged = stage / name
        staged.write_bytes(body)
        logical = f"data/reference/exchange_calendars/{name}"
        logical_paths[name] = logical
        staged_paths[logical] = name
        responses.append(
            {
                "logical_path": logical,
                "market": market,
                "request_id": spec["request_id"],
                "sha256": sha256_file(staged),
                "size": len(body),
                "status_code": 200,
                "url": url,
            }
        )
    elapsed = int((monotonic_time.monotonic() - started) * 1000)
    if elapsed > 900_000:
        raise CalendarCaptureError("search discovery duration ceiling exceeded")
    authority = scope["authority"]
    assert isinstance(authority, dict)
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "authority": authority,
        "capture_approval": dict(approval),
        "elapsed_milliseconds": elapsed,
        "plan_id": plan["plan_id"],
        "responses": responses,
        "schema_version": SEARCH_DISCOVERY_CAPTURE_SCHEMA,
        "total_bytes": total,
    }
    capture = {**core, "capture_id": sha256_json(core)}
    release = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=SEARCH_DISCOVERY_RELEASE_KIND,
        schema_version=SEARCH_DISCOVERY_CAPTURE_SCHEMA,
        logical_paths=logical_paths,
        source_release_ids=(str(authority["predecessor_capture_release_id"]),),
        embedded_documents={"search_discovery_capture.json": capture},
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture["capture_id"],
            "plan_id": plan["plan_id"],
        },
    )
    manifest_path = publisher.publish(stage, release, staged_paths=staged_paths)
    return VerifiedReleaseReceipt.from_manifest(
        manifest_path, publisher.boundary
    )


def build_search_recovery_plan(
    *,
    failed_plan_path: Path,
    failure_report_path: Path,
    boundary: RepoBoundary,
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    failed_plan = validate_search_discovery_plan(
        _canonical_object(failed_plan_path, description="failed search plan"),
        boundary=boundary,
    )
    failure = _canonical_object(
        failure_report_path, description="search failure report"
    )
    if (
        failure.get("schema_version")
        != "cme_calendar_search_discovery_failure/1.0.0"
        or failure.get("status") != "STOPPED"
        or failure.get("plan_id") != failed_plan["plan_id"]
        or failure.get("plan_sha256") != sha256_file(failed_plan_path)
        or failure.get("publication_occurred") is not False
        or failure.get("retries_performed") != 0
        or failure.get("network_requests_attempted") != 17
        or failure.get("responses_preserved_count") != 16
    ):
        raise IntegrityError("search recovery failure evidence is invalid")
    stage_relative = str(failure["stage_relative_path"])
    stage = boundary.assert_active_path(
        boundary.active_root / stage_relative,
        purpose="failed search capture staging",
        subtree="state/data_publication_staging",
    )
    scope = failed_plan["scope"]
    assert isinstance(scope, dict)
    requests = scope["requests"]
    assert isinstance(requests, list)
    reused = []
    for ordinal, spec in enumerate(requests[:16], start=1):
        assert isinstance(spec, dict)
        market = str(spec["market"])
        name = f"{ordinal:03d}-{market}.json"
        source = boundary.assert_active_path(
            stage / name,
            purpose="preserved search response",
            subtree="state/data_publication_staging",
        )
        if not source.is_file():
            raise IntegrityError("preserved search response is missing")
        reused.append(
            {
                "market": market,
                "ordinal": ordinal,
                "request_id": spec["request_id"],
                "sha256": sha256_file(source),
                "size": source.stat().st_size,
                "source_relative_path": source.relative_to(
                    boundary.active_root
                ).as_posix(),
                "url": spec["url"],
            }
        )
    remaining = requests[16:]
    failed_request = failure["failed_request"]
    if (
        not isinstance(failed_request, dict)
        or failed_request.get("request_ordinal") != 17
        or failed_request.get("request_id") != remaining[0]["request_id"]  # type: ignore[index]
        or len(remaining) != 17
    ):
        raise IntegrityError("search recovery request boundary is invalid")
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != CAPTURE_IMPLEMENTATION_PATHS
        or any(_SHA256.fullmatch(value) is None for value in implementation.values())
    ):
        raise ContractError("search recovery implementation hashes are invalid")
    recovery_scope: dict[str, object] = {
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": 900,
            "max_network_requests": 17,
            "max_total_bytes": 67_108_864,
            "request_timeout_seconds": 30,
            "retries": 0,
            "workers": 1,
        },
        "failure_report_path": failure_report_path.resolve(strict=True)
        .relative_to(boundary.active_root)
        .as_posix(),
        "failure_report_sha256": sha256_file(failure_report_path),
        "failed_plan_id": failed_plan["plan_id"],
        "failed_plan_path": failed_plan_path.resolve(strict=True)
        .relative_to(boundary.active_root)
        .as_posix(),
        "failed_plan_sha256": sha256_file(failed_plan_path),
        "forbidden_actions": list(CAPTURE_FORBIDDEN_ACTIONS)
        + ["MODIFY_OR_DELETE_PRESERVED_FAILED_ATTEMPT"],
        "implementation_sha256": implementation,
        "network_requests": remaining,
        "output_paths": dict(CAPTURE_OUTPUT_PATHS),
        "reused_responses": reused,
        "source_release_id": scope["authority"][  # type: ignore[index]
            "predecessor_capture_release_id"
        ],
        "stop_conditions": list(CAPTURE_STOP_CONDITIONS)
        + ["PRESERVED_RESPONSE_DRIFT"],
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": SEARCH_RECOVERY_OPERATION,
        "schema_version": SEARCH_RECOVERY_PLAN_SCHEMA,
        "scope": recovery_scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_search_recovery_plan(
    payload: Mapping[str, object], *, boundary: RepoBoundary
) -> dict[str, object]:
    scope = payload.get("scope")
    core = {key: payload[key] for key in payload if key != "plan_id"}
    if (
        set(payload)
        != {
            "classification",
            "execution_authorized",
            "operation",
            "plan_id",
            "schema_version",
            "scope",
        }
        or payload.get("schema_version") != SEARCH_RECOVERY_PLAN_SCHEMA
        or payload.get("operation") != SEARCH_RECOVERY_OPERATION
        or payload.get("execution_authorized") is not False
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
        or not isinstance(scope.get("implementation_sha256"), dict)
    ):
        raise IntegrityError("search recovery plan identity is invalid")
    expected = build_search_recovery_plan(
        failed_plan_path=boundary.active_root / str(scope["failed_plan_path"]),
        failure_report_path=boundary.active_root
        / str(scope["failure_report_path"]),
        boundary=boundary,
        implementation_sha256=scope["implementation_sha256"],
    )
    if dict(payload) != expected:
        raise IntegrityError("search recovery plan is not reproducible")
    return dict(payload)


def validate_search_recovery_approval(
    approval: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> str:
    core_keys = {
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    core = {key: approval[key] for key in core_keys if key in approval}
    if (
        set(approval) != {*core_keys, "approval_receipt_id"}
        or approval.get("schema_version") != SEARCH_RECOVERY_APPROVAL_SCHEMA
        or approval.get("operation") != SEARCH_RECOVERY_OPERATION
        or approval.get("status") != "APPROVED"
        or approval.get("plan_id") != plan["plan_id"]
        or approval.get("plan_sha256") != plan_sha256
        or approval.get("approval_receipt_id") != sha256_json(core)
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"])) is None
    ):
        raise CalendarCaptureError(
            "search recovery lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def capture_search_recovery(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    plan = validate_search_recovery_plan(
        _canonical_object(plan_path, description="search recovery plan"),
        boundary=publisher.boundary,
    )
    approval = _canonical_object(
        approval_path, description="search recovery approval"
    )
    approval_id = validate_search_recovery_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != _capture_implementation_hashes(
        publisher.boundary.active_root
    ):
        raise CalendarCaptureError("search recovery implementation changed")
    stage = publisher.create_stage("cme_search_product_discovery_recovery")
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    responses = []
    total = 0
    for item in scope["reused_responses"]:  # type: ignore[union-attr]
        assert isinstance(item, dict)
        source = publisher.boundary.assert_active_path(
            publisher.boundary.active_root / str(item["source_relative_path"]),
            purpose="reused failed-attempt response",
            subtree="state/data_publication_staging",
        )
        if (
            not source.is_file()
            or source.stat().st_size != item["size"]
            or sha256_file(source) != item["sha256"]
        ):
            raise CalendarCaptureError("preserved response drifted before recovery")
        name = f"{int(item['ordinal']):03d}-{item['market']}.json"
        destination = stage / name
        destination.write_bytes(source.read_bytes())
        logical = f"data/reference/exchange_calendars/{name}"
        logical_paths[name] = logical
        staged_paths[logical] = name
        total += int(item["size"])
        responses.append(
            {
                "logical_path": logical,
                "market": item["market"],
                "request_id": item["request_id"],
                "sha256": item["sha256"],
                "size": item["size"],
                "source": "REUSED_HASH_BOUND_FAILED_ATTEMPT",
                "status_code": 200,
                "url": item["url"],
            }
        )
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    for ordinal, spec in enumerate(scope["network_requests"], start=17):  # type: ignore[union-attr]
        assert isinstance(spec, dict)
        url = str(spec["url"])
        _safe_url(url)
        if "id=" in url or "searchString=" not in url:
            raise CalendarCaptureError("search recovery request is not exact")
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "futures-intraday-model-v2-calendar/1.0",
            },
            method="GET",
        )
        try:
            with opener.open(request, timeout=30) as response:
                if (
                    response.status != 200
                    or response.geturl() != url
                    or response.headers.get_content_type() != "application/json"
                ):
                    raise CalendarCaptureError(
                        "search recovery response contract failed"
                    )
                body = response.read(67_108_864 - total + 1)
                if total + len(body) > 67_108_864:
                    raise CalendarCaptureError(
                        "search recovery byte ceiling exceeded"
                    )
        except CalendarCaptureError:
            raise
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            raise CalendarCaptureError(
                f"search recovery request {ordinal} failed"
            ) from exc
        total += len(body)
        market = str(spec["market"])
        name = f"{ordinal:03d}-{market}.json"
        destination = stage / name
        destination.write_bytes(body)
        logical = f"data/reference/exchange_calendars/{name}"
        logical_paths[name] = logical
        staged_paths[logical] = name
        responses.append(
            {
                "logical_path": logical,
                "market": market,
                "request_id": spec["request_id"],
                "sha256": sha256_file(destination),
                "size": len(body),
                "source": "NETWORK_RECOVERY_SUCCESSOR",
                "status_code": 200,
                "url": url,
            }
        )
    elapsed = int((monotonic_time.monotonic() - started) * 1000)
    if elapsed > 900_000 or len(responses) != 33:
        raise CalendarCaptureError("search recovery completion bound failed")
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "elapsed_milliseconds": elapsed,
        "failed_plan_id": scope["failed_plan_id"],
        "failure_report_sha256": scope["failure_report_sha256"],
        "network_request_count": 17,
        "plan_id": plan["plan_id"],
        "responses": responses,
        "reused_response_count": 16,
        "schema_version": SEARCH_RECOVERY_CAPTURE_SCHEMA,
        "total_bytes": total,
    }
    capture = {**core, "capture_id": sha256_json(core)}
    release = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=SEARCH_DISCOVERY_RELEASE_KIND,
        schema_version=SEARCH_RECOVERY_CAPTURE_SCHEMA,
        logical_paths=logical_paths,
        source_release_ids=(str(scope["source_release_id"]),),
        embedded_documents={"search_recovery_capture.json": capture},
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture["capture_id"],
            "failed_plan_id": scope["failed_plan_id"],
            "plan_id": plan["plan_id"],
        },
    )
    manifest_path = publisher.publish(stage, release, staged_paths=staged_paths)
    return VerifiedReleaseReceipt.from_manifest(
        manifest_path, publisher.boundary
    )


def _semantic_search_successor_requests(
    predecessor_requests: Sequence[object],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    if len(predecessor_requests) != 17:
        raise IntegrityError("semantic recovery requires 17 predecessor requests")
    original = predecessor_requests[0]
    if not isinstance(original, dict):
        raise IntegrityError("semantic recovery PL request is invalid")
    original_url = str(original.get("url", ""))
    expected_fragment = "searchString=PL"
    if (
        original.get("market") != "PL"
        or original.get("request_id") != "search-pl"
        or original.get("request_kind") != "SEARCH_PRODUCT_DISCOVERY"
        or original.get("accept") != "application/json"
        or original_url.count(expected_fragment) != 1
        or "id=" in original_url
    ):
        raise IntegrityError("semantic recovery predecessor PL request is invalid")
    successor_url = original_url.replace(
        expected_fragment, "searchString=Platinum", 1
    )
    successor = dict(original)
    successor["url"] = successor_url
    requests = [successor]
    for item in predecessor_requests[1:]:
        if not isinstance(item, dict):
            raise IntegrityError("semantic recovery predecessor request is invalid")
        requests.append(dict(item))
    if requests[1:] != list(predecessor_requests[1:]):
        raise IntegrityError("semantic recovery changed an original market request")
    override: dict[str, object] = {
        "market": "PL",
        "parameter": "searchString",
        "predecessor_request_sha256": sha256_json(original),
        "predecessor_value": "PL",
        "request_ordinal": 17,
        "successor_request_sha256": sha256_json(successor),
        "successor_value": "Platinum",
    }
    return requests, override


def _validate_semantic_search_requests(
    requests: Sequence[object],
    *,
    predecessor_requests: Sequence[object],
) -> None:
    expected, _ = _semantic_search_successor_requests(predecessor_requests)
    if list(requests) != expected:
        raise IntegrityError("semantic recovery request set drifted")
    for offset, item in enumerate(requests):
        if not isinstance(item, dict):
            raise IntegrityError("semantic recovery request is invalid")
        url = str(item.get("url", ""))
        _safe_url(url)
        if "id=" in url or "searchString=" not in url:
            raise IntegrityError("semantic recovery request is not exact")
        query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(url).query,
            keep_blank_values=True,
            strict_parsing=True,
        )
        expected_value = "Platinum" if offset == 0 else str(item["market"])
        if query.get("searchString") != [expected_value]:
            raise IntegrityError("semantic recovery searchString is invalid")


def build_semantic_search_recovery_plan(
    *,
    recovery_plan_path: Path,
    recovery_failure_report_path: Path,
    boundary: RepoBoundary,
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    recovery_plan = validate_search_recovery_plan(
        _canonical_object(
            recovery_plan_path, description="failed search recovery plan"
        ),
        boundary=boundary,
    )
    failure = _canonical_object(
        recovery_failure_report_path,
        description="search recovery failure report",
    )
    recovery_scope = recovery_plan["scope"]
    assert isinstance(recovery_scope, dict)
    predecessor_requests = recovery_scope["network_requests"]
    if not isinstance(predecessor_requests, list):
        raise IntegrityError("search recovery request evidence is invalid")
    failed_request = failure.get("failed_request")
    if (
        failure.get("schema_version")
        != "cme_calendar_search_discovery_recovery_failure/1.0.0"
        or failure.get("status") != "STOPPED"
        or failure.get("stop_reason")
        != "REPEATED_CME_HTTP_502_FOR_PL_SEARCHSTRING"
        or failure.get("plan_id") != recovery_plan["plan_id"]
        or failure.get("plan_sha256") != sha256_file(recovery_plan_path)
        or failure.get("publication_occurred") is not False
        or failure.get("retries_performed") != 0
        or failure.get("network_requests_attempted") != 1
        or failure.get("responses_reused_count") != 16
        or failure.get("failed_request_prior_attempt_count") != 1
        or not isinstance(failed_request, dict)
        or failed_request.get("http_status") != 502
        or failed_request.get("market") != "PL"
        or failed_request.get("request_ordinal") != 17
        or failed_request.get("request_id")
        != predecessor_requests[0]["request_id"]  # type: ignore[index]
        or failed_request.get("url")
        != predecessor_requests[0]["url"]  # type: ignore[index]
    ):
        raise IntegrityError("semantic recovery failure evidence is invalid")
    stage_relative = str(failure["stage_relative_path"])
    stage = boundary.assert_active_path(
        boundary.active_root / stage_relative,
        purpose="failed semantic-search recovery staging",
        subtree="state/data_publication_staging",
    )
    predecessor_reused = recovery_scope["reused_responses"]
    if not isinstance(predecessor_reused, list) or len(predecessor_reused) != 16:
        raise IntegrityError("semantic recovery reused-response evidence is invalid")
    expected_names = {
        f"{int(item['ordinal']):03d}-{item['market']}.json"
        for item in predecessor_reused
        if isinstance(item, dict)
    }
    if len(expected_names) != 16 or {
        item.name for item in stage.iterdir() if item.is_file()
    } != expected_names:
        raise IntegrityError("semantic recovery staging inventory is invalid")
    reused: list[dict[str, object]] = []
    for expected in predecessor_reused:
        if not isinstance(expected, dict):
            raise IntegrityError("semantic recovery reused response is invalid")
        name = f"{int(expected['ordinal']):03d}-{expected['market']}.json"
        source = boundary.assert_active_path(
            stage / name,
            purpose="preserved semantic-search response",
            subtree="state/data_publication_staging",
        )
        if (
            not source.is_file()
            or source.stat().st_size != expected["size"]
            or sha256_file(source) != expected["sha256"]
        ):
            raise IntegrityError("semantic recovery preserved response drifted")
        reused.append(
            {
                "market": expected["market"],
                "ordinal": expected["ordinal"],
                "request_id": expected["request_id"],
                "sha256": expected["sha256"],
                "size": expected["size"],
                "source_relative_path": source.relative_to(
                    boundary.active_root
                ).as_posix(),
                "url": expected["url"],
            }
        )
    network_requests, semantic_override = _semantic_search_successor_requests(
        predecessor_requests
    )
    _validate_semantic_search_requests(
        network_requests, predecessor_requests=predecessor_requests
    )
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != CAPTURE_IMPLEMENTATION_PATHS
        or any(
            _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
    ):
        raise ContractError(
            "semantic search recovery implementation hashes are invalid"
        )
    scope: dict[str, object] = {
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": 900,
            "max_network_requests": 17,
            "max_total_bytes": 67_108_864,
            "request_timeout_seconds": 30,
            "retries": 0,
            "workers": 1,
        },
        "failed_attempt_count": 2,
        "first_failure_report_path": recovery_scope["failure_report_path"],
        "first_failure_report_sha256": recovery_scope[
            "failure_report_sha256"
        ],
        "first_failed_plan_id": recovery_scope["failed_plan_id"],
        "first_failed_plan_path": recovery_scope["failed_plan_path"],
        "first_failed_plan_sha256": recovery_scope["failed_plan_sha256"],
        "forbidden_actions": list(CAPTURE_FORBIDDEN_ACTIONS)
        + [
            "CHANGE_ANY_REQUEST_OTHER_THAN_PL_SEARCHSTRING",
            "MODIFY_OR_DELETE_PRESERVED_FAILED_ATTEMPT",
        ],
        "implementation_sha256": implementation,
        "network_requests": network_requests,
        "original_market_search_count": 16,
        "output_paths": dict(CAPTURE_OUTPUT_PATHS),
        "recovery_failure_report_path": recovery_failure_report_path.resolve(
            strict=True
        )
        .relative_to(boundary.active_root)
        .as_posix(),
        "recovery_failure_report_sha256": sha256_file(
            recovery_failure_report_path
        ),
        "recovery_plan_id": recovery_plan["plan_id"],
        "recovery_plan_path": recovery_plan_path.resolve(strict=True)
        .relative_to(boundary.active_root)
        .as_posix(),
        "recovery_plan_sha256": sha256_file(recovery_plan_path),
        "reused_response_count": 16,
        "reused_responses": reused,
        "semantic_override": semantic_override,
        "source_release_id": recovery_scope["source_release_id"],
        "stop_conditions": list(CAPTURE_STOP_CONDITIONS)
        + [
            "PRESERVED_RESPONSE_DRIFT",
            "SEMANTIC_OR_ORIGINAL_REQUEST_DRIFT",
        ],
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": SEMANTIC_SEARCH_RECOVERY_OPERATION,
        "schema_version": SEMANTIC_SEARCH_RECOVERY_PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_semantic_search_recovery_plan(
    payload: Mapping[str, object], *, boundary: RepoBoundary
) -> dict[str, object]:
    scope = payload.get("scope")
    core = {key: payload[key] for key in payload if key != "plan_id"}
    if (
        set(payload)
        != {
            "classification",
            "execution_authorized",
            "operation",
            "plan_id",
            "schema_version",
            "scope",
        }
        or payload.get("schema_version")
        != SEMANTIC_SEARCH_RECOVERY_PLAN_SCHEMA
        or payload.get("operation") != SEMANTIC_SEARCH_RECOVERY_OPERATION
        or payload.get("classification")
        != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload.get("execution_authorized") is not False
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
        or not isinstance(scope.get("implementation_sha256"), dict)
    ):
        raise IntegrityError("semantic search recovery plan identity is invalid")
    expected = build_semantic_search_recovery_plan(
        recovery_plan_path=boundary.active_root
        / str(scope["recovery_plan_path"]),
        recovery_failure_report_path=boundary.active_root
        / str(scope["recovery_failure_report_path"]),
        boundary=boundary,
        implementation_sha256=scope["implementation_sha256"],
    )
    if dict(payload) != expected:
        raise IntegrityError("semantic search recovery plan is not reproducible")
    return dict(payload)


def validate_semantic_search_recovery_approval(
    approval: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> str:
    core_keys = {
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    core = {key: approval[key] for key in core_keys if key in approval}
    if (
        set(approval) != {*core_keys, "approval_receipt_id"}
        or approval.get("schema_version")
        != SEMANTIC_SEARCH_RECOVERY_APPROVAL_SCHEMA
        or approval.get("operation") != SEMANTIC_SEARCH_RECOVERY_OPERATION
        or approval.get("status") != "APPROVED"
        or approval.get("plan_id") != plan["plan_id"]
        or approval.get("plan_sha256") != plan_sha256
        or approval.get("approval_receipt_id") != sha256_json(core)
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"])) is None
    ):
        raise CalendarCaptureError(
            "semantic search recovery lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def capture_semantic_search_recovery(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    plan = validate_semantic_search_recovery_plan(
        _canonical_object(
            plan_path, description="semantic search recovery plan"
        ),
        boundary=publisher.boundary,
    )
    approval = _canonical_object(
        approval_path, description="semantic search recovery approval"
    )
    approval_id = validate_semantic_search_recovery_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != _capture_implementation_hashes(
        publisher.boundary.active_root
    ):
        raise CalendarCaptureError(
            "semantic search recovery implementation changed"
        )
    recovery_plan = validate_search_recovery_plan(
        _canonical_object(
            publisher.boundary.active_root
            / str(scope["recovery_plan_path"]),
            description="semantic recovery predecessor plan",
        ),
        boundary=publisher.boundary,
    )
    predecessor_scope = recovery_plan["scope"]
    assert isinstance(predecessor_scope, dict)
    predecessor_requests = predecessor_scope["network_requests"]
    assert isinstance(predecessor_requests, list)
    requests = scope["network_requests"]
    assert isinstance(requests, list)
    _validate_semantic_search_requests(
        requests, predecessor_requests=predecessor_requests
    )
    sources: list[tuple[dict[str, object], Path]] = []
    for item in scope["reused_responses"]:  # type: ignore[union-attr]
        assert isinstance(item, dict)
        source = publisher.boundary.assert_active_path(
            publisher.boundary.active_root
            / str(item["source_relative_path"]),
            purpose="reused semantic-search response",
            subtree="state/data_publication_staging",
        )
        if (
            not source.is_file()
            or source.stat().st_size != item["size"]
            or sha256_file(source) != item["sha256"]
        ):
            raise CalendarCaptureError(
                "preserved response drifted before semantic recovery"
            )
        sources.append((item, source))
    if len(sources) != 16:
        raise CalendarCaptureError(
            "semantic recovery reused-response count drifted"
        )
    stage = publisher.create_stage(
        "cme_search_product_discovery_semantic_recovery"
    )
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    responses = []
    total = 0
    for item, source in sources:
        name = f"{int(item['ordinal']):03d}-{item['market']}.json"
        destination = stage / name
        destination.write_bytes(source.read_bytes())
        if (
            destination.stat().st_size != item["size"]
            or sha256_file(destination) != item["sha256"]
        ):
            raise CalendarCaptureError("semantic recovery copy verification failed")
        logical = f"data/reference/exchange_calendars/{name}"
        logical_paths[name] = logical
        staged_paths[logical] = name
        total += int(item["size"])
        responses.append(
            {
                "logical_path": logical,
                "market": item["market"],
                "request_id": item["request_id"],
                "sha256": item["sha256"],
                "size": item["size"],
                "source": "REUSED_HASH_BOUND_RECOVERY_ATTEMPT",
                "status_code": 200,
                "url": item["url"],
            }
        )
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    for ordinal, spec in enumerate(requests, start=17):
        assert isinstance(spec, dict)
        url = str(spec["url"])
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "futures-intraday-model-v2-calendar/1.0",
            },
            method="GET",
        )
        try:
            with opener.open(request, timeout=30) as response:
                if (
                    response.status != 200
                    or response.geturl() != url
                    or response.headers.get_content_type()
                    != "application/json"
                ):
                    raise CalendarCaptureError(
                        "semantic search recovery response contract failed"
                    )
                body = response.read(67_108_864 - total + 1)
                if total + len(body) > 67_108_864:
                    raise CalendarCaptureError(
                        "semantic search recovery byte ceiling exceeded"
                    )
        except CalendarCaptureError:
            raise
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
        ) as exc:
            raise CalendarCaptureError(
                f"semantic search recovery request {ordinal} failed"
            ) from exc
        total += len(body)
        market = str(spec["market"])
        name = f"{ordinal:03d}-{market}.json"
        destination = stage / name
        destination.write_bytes(body)
        logical = f"data/reference/exchange_calendars/{name}"
        logical_paths[name] = logical
        staged_paths[logical] = name
        responses.append(
            {
                "logical_path": logical,
                "market": market,
                "request_id": spec["request_id"],
                "sha256": sha256_file(destination),
                "size": len(body),
                "source": (
                    "NETWORK_SEMANTIC_QUERY_SUCCESSOR"
                    if ordinal == 17
                    else "NETWORK_ORIGINAL_MARKET_SEARCH"
                ),
                "status_code": 200,
                "url": url,
            }
        )
    elapsed = int((monotonic_time.monotonic() - started) * 1000)
    if elapsed > 900_000 or len(responses) != 33:
        raise CalendarCaptureError(
            "semantic search recovery completion bound failed"
        )
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "elapsed_milliseconds": elapsed,
        "network_request_count": 17,
        "original_market_search_count": 16,
        "plan_id": plan["plan_id"],
        "recovery_failure_report_sha256": scope[
            "recovery_failure_report_sha256"
        ],
        "recovery_plan_id": scope["recovery_plan_id"],
        "responses": responses,
        "reused_response_count": 16,
        "schema_version": SEMANTIC_SEARCH_RECOVERY_CAPTURE_SCHEMA,
        "semantic_override": scope["semantic_override"],
        "total_bytes": total,
    }
    capture = {**core, "capture_id": sha256_json(core)}
    release = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=SEARCH_DISCOVERY_RELEASE_KIND,
        schema_version=SEMANTIC_SEARCH_RECOVERY_CAPTURE_SCHEMA,
        logical_paths=logical_paths,
        source_release_ids=(str(scope["source_release_id"]),),
        embedded_documents={
            "semantic_search_recovery_capture.json": capture
        },
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture["capture_id"],
            "recovery_plan_id": scope["recovery_plan_id"],
            "plan_id": plan["plan_id"],
        },
    )
    manifest_path = publisher.publish(
        stage, release, staged_paths=staged_paths
    )
    return VerifiedReleaseReceipt.from_manifest(
        manifest_path, publisher.boundary
    )


def load_semantic_search_recovery_capture(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "reference"
        or manifest.release_kind != SEARCH_DISCOVERY_RELEASE_KIND
        or manifest.schema_version != SEMANTIC_SEARCH_RECOVERY_CAPTURE_SCHEMA
        or set(manifest.embedded_documents)
        != {"semantic_search_recovery_capture.json"}
        or len(manifest.files) != 33
        or len(manifest.source_release_ids) != 1
    ):
        raise IntegrityError(
            "semantic search recovery release contract is invalid"
        )
    raw = receipt.embedded_document(
        "semantic_search_recovery_capture.json", boundary
    )
    if not isinstance(raw, dict):
        raise IntegrityError(
            "semantic search recovery capture receipt is invalid"
        )
    payload = dict(raw)
    capture_id = payload.pop("capture_id", None)
    responses = payload.get("responses")
    if (
        set(raw)
        != {
            "approval_receipt_id",
            "capture_id",
            "elapsed_milliseconds",
            "network_request_count",
            "original_market_search_count",
            "plan_id",
            "recovery_failure_report_sha256",
            "recovery_plan_id",
            "responses",
            "reused_response_count",
            "schema_version",
            "semantic_override",
            "total_bytes",
        }
        or capture_id != sha256_json(payload)
        or capture_id != manifest.metadata.get("capture_id")
        or payload.get("schema_version")
        != SEMANTIC_SEARCH_RECOVERY_CAPTURE_SCHEMA
        or payload.get("approval_receipt_id")
        != manifest.metadata.get("approval_receipt_id")
        or payload.get("plan_id") != manifest.metadata.get("plan_id")
        or payload.get("recovery_plan_id")
        != manifest.metadata.get("recovery_plan_id")
        or payload.get("network_request_count") != 17
        or payload.get("original_market_search_count") != 16
        or payload.get("reused_response_count") != 16
        or not isinstance(responses, list)
        or len(responses) != 33
    ):
        raise IntegrityError(
            "semantic search recovery capture identity is invalid"
        )
    entries = {entry.logical_path: entry for entry in manifest.files}
    markets = []
    for ordinal, response in enumerate(responses, start=1):
        if not isinstance(response, dict):
            raise IntegrityError(
                "semantic search recovery response is invalid"
            )
        source = (
            "REUSED_HASH_BOUND_RECOVERY_ATTEMPT"
            if ordinal <= 16
            else (
                "NETWORK_SEMANTIC_QUERY_SUCCESSOR"
                if ordinal == 17
                else "NETWORK_ORIGINAL_MARKET_SEARCH"
            )
        )
        logical = str(response.get("logical_path", ""))
        if (
            set(response)
            != {
                "logical_path",
                "market",
                "request_id",
                "sha256",
                "size",
                "source",
                "status_code",
                "url",
            }
            or logical not in entries
            or response.get("status_code") != 200
            or response.get("source") != source
            or response.get("sha256") != entries[logical].sha256
            or response.get("size") != entries[logical].size
            or "id=" in str(response.get("url"))
            or "searchString=" not in str(response.get("url"))
        ):
            raise IntegrityError(
                "semantic search recovery response closure is invalid"
            )
        markets.append(str(response["market"]))
    if len(set(markets)) != 33 or markets != sorted(markets):
        raise IntegrityError(
            "semantic search recovery market census is invalid"
        )
    override = payload.get("semantic_override")
    if (
        not isinstance(override, dict)
        or override.get("market") != "PL"
        or override.get("parameter") != "searchString"
        or override.get("predecessor_value") != "PL"
        or override.get("successor_value") != "Platinum"
        or override.get("request_ordinal") != 17
    ):
        raise IntegrityError(
            "semantic search recovery override is invalid"
        )
    payload["capture_id"] = capture_id
    payload["source_release_id"] = manifest.source_release_ids[0]
    return payload


def _exact_futures_product(
    payload: object, *, market: str
) -> dict[str, object]:
    if (
        not isinstance(payload, dict)
        or set(payload) != {"products", "props"}
        or not isinstance(payload["products"], list)
        or not isinstance(payload["props"], dict)
    ):
        raise IntegrityError("CME product-discovery service schema is invalid")
    exact = [
        item
        for item in payload["products"]
        if isinstance(item, dict)
        and str(item.get("globex", "")).upper() == market
        and item.get("foi") == "Futures"
        and type(item.get("id")) is int
        and type(item.get("name")) is str
        and bool(item.get("name"))
        and type(item.get("prodGroup")) is str
        and bool(item.get("prodGroup"))
    ]
    if len(exact) != 1:
        raise IntegrityError(
            f"CME product mapping is incomplete or ambiguous for {market}"
        )
    return dict(exact[0])


def generate_complete_product_mapping_candidates(
    *,
    predecessor_receipt: VerifiedReleaseReceipt,
    predecessor_candidates: Mapping[str, object],
    search_receipt: VerifiedReleaseReceipt,
    boundary: RepoBoundary,
    expected_markets: Sequence[str],
) -> dict[str, object]:
    generated_predecessor = generate_nonempty_discovery_candidates(
        predecessor_receipt, boundary=boundary
    )
    if dict(predecessor_candidates) != generated_predecessor:
        raise IntegrityError(
            "complete mapping predecessor candidates drifted"
        )
    search_capture = load_semantic_search_recovery_capture(
        search_receipt, boundary=boundary
    )
    if search_capture["source_release_id"] != predecessor_receipt.release_id:
        raise IntegrityError(
            "complete mapping capture lineage is invalid"
        )
    expected = tuple(sorted(expected_markets))
    if (
        tuple(expected_markets) != expected
        or len(expected) != 41
        or tuple(generated_predecessor["missing_markets"])
        != tuple(
            response["market"]
            for response in search_capture["responses"]  # type: ignore[union-attr]
        )
    ):
        raise IntegrityError(
            "complete mapping market census is invalid"
        )
    predecessor_capture = load_nonempty_discovery_capture(
        predecessor_receipt, boundary=boundary
    )
    resolved_markets = {
        str(item["market"])
        for item in generated_predecessor["resolved"]  # type: ignore[union-attr]
    }
    response_by_market: dict[
        str, tuple[VerifiedReleaseReceipt, dict[str, object]]
    ] = {}
    for response in predecessor_capture["responses"]:  # type: ignore[union-attr]
        assert isinstance(response, dict)
        market = str(response["market"])
        if market in resolved_markets:
            response_by_market[market] = (
                predecessor_receipt,
                response,
            )
    for response in search_capture["responses"]:  # type: ignore[union-attr]
        assert isinstance(response, dict)
        market = str(response["market"])
        if market in response_by_market:
            raise IntegrityError(
                "complete mapping has duplicate market evidence"
            )
        response_by_market[market] = (search_receipt, response)
    if tuple(sorted(response_by_market)) != expected:
        raise IntegrityError(
            "complete mapping evidence is incomplete"
        )
    mappings = []
    product_codes = set()
    for market in expected:
        source_receipt, response = response_by_market[market]
        path = source_receipt.resolve_file(
            str(response["logical_path"]), boundary
        )
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise IntegrityError(
                "complete mapping response JSON is invalid"
            ) from exc
        product = _exact_futures_product(raw, market=market)
        code = str(product["id"])
        if code in product_codes:
            raise IntegrityError(
                "complete mapping product ID is ambiguous"
            )
        product_codes.add(code)
        mappings.append(
            {
                "cme_product_code": code,
                "cme_product_name": str(product["name"]),
                "globex": str(product["globex"]),
                "market": market,
                "match_rule": "EXACT_GLOBEX_AND_FUTURES",
                "product_group": str(product["prodGroup"]),
                "product_type": "FUTURES",
                "response_logical_path": response["logical_path"],
                "response_sha256": response["sha256"],
                "source_capture_release_id": source_receipt.release_id,
                "venue": "CME Globex",
            }
        )
    core: dict[str, object] = {
        "capture_release_id": search_receipt.release_id,
        "expected_markets": list(expected),
        "mappings": mappings,
        "predecessor_candidates_id": generated_predecessor["candidates_id"],
        "schema_version": COMPLETE_PRODUCT_MAPPING_CANDIDATES_SCHEMA,
        "source_capture_release_ids": [
            predecessor_receipt.release_id,
            search_receipt.release_id,
        ],
        "status": "REVIEW_REQUIRED_NO_MAPPING_AUTHORITY",
    }
    return {**core, "mapping_candidates_id": sha256_json(core)}


def _schedule_recovery_requests(
    *,
    coverage_start: date,
    coverage_end: date,
    product_ids: Sequence[str],
) -> list[dict[str, object]]:
    requests = []
    for window_number, (
        core_start,
        core_end,
        source_start,
        source_end,
    ) in enumerate(
        _three_day_windows(coverage_start, coverage_end),
        start=1,
    ):
        query = urllib.parse.urlencode(
            {
                "cleared": "Futures",
                "fromEventDate": source_start.isoformat(),
                "isProtected": "",
                "pageNumber": 1,
                "pageSize": 999,
                "sortAsc": "true",
                "toEventDate": source_end.isoformat(),
                "id": ",".join(product_ids),
            }
        ).replace("isProtected=", "isProtected")
        requests.append(
            {
                "accept": "application/json",
                "core_end_trade_date": core_end.isoformat(),
                "core_start_trade_date": core_start.isoformat(),
                "request_id": (
                    f"schedule-recovery-{window_number:03d}-p1"
                ),
                "request_kind": "SCHEDULE",
                "url": f"{SCHEDULE_URL}?{query}",
            }
        )
    return requests


def build_schedule_coverage_recovery_plan(
    *,
    source_capture_manifest_path: Path,
    mapping_approval_path: Path,
    candidate_calendar_manifest_path: Path,
    failed_index_manifest_path: Path,
    boundary: RepoBoundary,
    expected_markets: Sequence[str],
    implementation_sha256: Mapping[str, str],
    universe_contract_sha256: str,
    active_pointer_path: Path | None = None,
) -> dict[str, object]:
    source_receipt = _receipt_from_manifest(
        source_capture_manifest_path, boundary=boundary
    )
    source_capture = load_cme_capture(
        source_receipt, boundary=boundary
    )
    source_manifest = source_receipt.verify(boundary)
    if (
        source_receipt.schema_version != CAPTURE_SCHEMA_VERSION
        or source_capture.get("mode") != "STEADY_STATE"
        or source_capture.get("request_count") != 32
        or source_capture.get("coverage_start_trade_date")
        != "2026-07-26"
        or source_capture.get("coverage_end_trade_date")
        != "2026-10-23"
    ):
        raise IntegrityError(
            "schedule recovery source capture is not the accepted predecessor"
        )
    mapping_approval = _canonical_object(
        mapping_approval_path, description="accepted CME product mapping"
    )
    mapping_capture_release_id = str(
        mapping_approval.get("capture_release_id", "")
    )
    mappings = validate_mapping_approval(
        mapping_approval,
        capture_release_id=mapping_capture_release_id,
        expected_markets=expected_markets,
    )
    if (
        mapping_approval.get("schema_version") != MAPPING_APPROVAL_SCHEMA
        or mapping_capture_release_id not in source_manifest.source_release_ids
    ):
        raise IntegrityError(
            "schedule recovery mapping is outside source lineage"
        )
    candidate_receipt = _receipt_from_manifest(
        candidate_calendar_manifest_path, boundary=boundary
    )
    candidate = VerifiedExchangeCalendar.from_release(
        candidate_receipt,
        boundary=boundary,
        expected_markets=expected_markets,
    )
    if (
        candidate.source_capture_receipt.release_id
        != source_receipt.release_id
        or candidate.coverage_start
        != date.fromisoformat(
            str(source_capture["coverage_start_trade_date"])
        )
        or candidate.coverage_end
        != date.fromisoformat(
            str(source_capture["coverage_end_trade_date"])
        )
    ):
        raise IntegrityError(
            "schedule recovery candidate does not bind the source capture"
        )
    failed_index_receipt = _receipt_from_manifest(
        failed_index_manifest_path, boundary=boundary
    )
    failed_index = load_calendar_index(
        failed_index_receipt,
        boundary=boundary,
        expected_markets=expected_markets,
    )
    failed_index_document = failed_index_receipt.embedded_document(
        "exchange_calendar_index.json", boundary
    )
    if not isinstance(failed_index_document, dict):
        raise IntegrityError(
            "schedule recovery failed-index evidence is invalid"
        )
    failed_approval = failed_index_document.get("activation_approval")
    if (
        candidate.receipt.release_id
        not in failed_index.calendar_by_release_id
        or not isinstance(failed_approval, dict)
        or failed_approval.get("candidate_calendar_release_id")
        != candidate.receipt.release_id
        or failed_approval.get("approval_receipt_id")
        != failed_index_document.get("activation_approval_receipt_id")
    ):
        raise IntegrityError(
            "schedule recovery failed activation is not exact"
        )
    pointer = (
        active_pointer_path
        if active_pointer_path is not None
        else boundary.active_root
        / "configs"
        / "active_exchange_calendar.json"
    )
    pointer = boundary.assert_active_path(
        pointer,
        purpose="active exchange calendar pointer",
        subtree="configs",
    )
    if pointer.exists():
        raise IntegrityError(
            "schedule recovery requires the failed activation to remain inactive"
        )
    far_notices = sorted(
        {
            date.fromisoformat(str(notice["trade_date"]))
            for notice in candidate.holiday_notices
            if date.fromisoformat(str(notice["trade_date"]))
            > candidate.coverage_end
        }
    )
    if (
        far_notices
        != [
            date(2026, 11, 26),
            date(2026, 12, 25),
            date(2027, 1, 1),
        ]
    ):
        raise IntegrityError(
            "schedule recovery missing-date authority changed"
        )
    recovery_start = candidate.coverage_end + timedelta(days=1)
    recovery_end = far_notices[-1]
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != CAPTURE_IMPLEMENTATION_PATHS
        or any(
            type(value) is not str
            or _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
        or _SHA256.fullmatch(universe_contract_sha256) is None
    ):
        raise ContractError(
            "schedule recovery implementation authority is invalid"
        )
    reused_responses = []
    responses = source_capture.get("responses")
    if not isinstance(responses, list):
        raise IntegrityError(
            "schedule recovery source response census is invalid"
        )
    for ordinal, response in enumerate(responses, start=1):
        if not isinstance(response, dict):
            raise IntegrityError(
                "schedule recovery source response is invalid"
            )
        logical_path = str(response["logical_path"])
        source = source_receipt.resolve_file(logical_path, boundary)
        if (
            source.stat().st_size != response["size"]
            or sha256_file(source) != response["sha256"]
        ):
            raise IntegrityError(
                "schedule recovery source response drifted"
            )
        reused_responses.append(
            {
                "ordinal": ordinal,
                "response": dict(response),
                "source_relative_path": source.relative_to(
                    boundary.active_root
                ).as_posix(),
            }
        )
    product_ids = tuple(
        sorted(str(item["cme_product_code"]) for item in mappings)
    )
    network_requests = _schedule_recovery_requests(
        coverage_start=recovery_start,
        coverage_end=recovery_end,
        product_ids=product_ids,
    )
    if len(reused_responses) != 32 or len(network_requests) != 24:
        raise IntegrityError(
            "schedule recovery request census is not exact"
        )
    authority: dict[str, object] = {
        "candidate_calendar_manifest_path": (
            candidate_calendar_manifest_path.resolve(strict=True)
            .relative_to(boundary.active_root)
            .as_posix()
        ),
        "candidate_calendar_manifest_sha256": sha256_file(
            candidate_calendar_manifest_path
        ),
        "candidate_calendar_release_id": candidate_receipt.release_id,
        "failed_activation_approval_receipt_id": failed_approval[
            "approval_receipt_id"
        ],
        "failed_index_manifest_path": (
            failed_index_manifest_path.resolve(strict=True)
            .relative_to(boundary.active_root)
            .as_posix()
        ),
        "failed_index_manifest_sha256": sha256_file(
            failed_index_manifest_path
        ),
        "failed_index_release_id": failed_index_receipt.release_id,
        "mapping_approval_path": (
            mapping_approval_path.resolve(strict=True)
            .relative_to(boundary.active_root)
            .as_posix()
        ),
        "mapping_approval_receipt_id": mapping_approval[
            "approval_receipt_id"
        ],
        "mapping_approval_sha256": sha256_file(mapping_approval_path),
        "mapping_capture_release_id": mapping_capture_release_id,
        "source_capture_id": source_capture["capture_id"],
        "source_capture_manifest_path": (
            source_capture_manifest_path.resolve(strict=True)
            .relative_to(boundary.active_root)
            .as_posix()
        ),
        "source_capture_manifest_sha256": sha256_file(
            source_capture_manifest_path
        ),
        "source_capture_release_id": source_receipt.release_id,
    }
    bounds: dict[str, object] = {
        "allow_redirects": False,
        "max_duration_seconds": 900,
        "max_network_requests": 24,
        "max_output_responses": 56,
        "max_total_bytes": 268_435_456,
        "request_timeout_seconds": 30,
        "retries": 0,
        "workers": 1,
    }
    scope: dict[str, object] = {
        "active_pointer_path": pointer.relative_to(
            boundary.active_root
        ).as_posix(),
        "authority": authority,
        "bounds": bounds,
        "coverage_end_trade_date": recovery_end.isoformat(),
        "coverage_start_trade_date": str(
            source_capture["coverage_start_trade_date"]
        ),
        "forbidden_actions": list(CAPTURE_FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "missing_schedule_end_trade_date": recovery_end.isoformat(),
        "missing_schedule_start_trade_date": recovery_start.isoformat(),
        "network_requests": network_requests,
        "output_paths": dict(CAPTURE_OUTPUT_PATHS),
        "reused_responses": reused_responses,
        "stop_conditions": list(CAPTURE_STOP_CONDITIONS)
        + [
            "ACTIVE_POINTER_APPEARED",
            "FAILED_ACTIVATION_OR_HOLIDAY_AUTHORITY_DRIFT",
            "REUSED_ACCEPTED_RESPONSE_DRIFT",
        ],
        "universe_contract_sha256": universe_contract_sha256,
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": SCHEDULE_RECOVERY_OPERATION,
        "schema_version": SCHEDULE_RECOVERY_PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_schedule_coverage_recovery_plan(
    payload: Mapping[str, object], *, boundary: RepoBoundary
) -> dict[str, object]:
    scope = payload.get("scope")
    core = {key: payload[key] for key in payload if key != "plan_id"}
    if (
        set(payload)
        != {
            "classification",
            "execution_authorized",
            "operation",
            "plan_id",
            "schema_version",
            "scope",
        }
        or payload.get("schema_version")
        != SCHEDULE_RECOVERY_PLAN_SCHEMA
        or payload.get("operation") != SCHEDULE_RECOVERY_OPERATION
        or payload.get("classification")
        != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload.get("execution_authorized") is not False
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
        or not isinstance(scope.get("authority"), dict)
        or not isinstance(scope.get("implementation_sha256"), dict)
        or not isinstance(scope.get("reused_responses"), list)
        or not isinstance(scope.get("network_requests"), list)
    ):
        raise IntegrityError(
            "schedule recovery plan identity is invalid"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    expected = build_schedule_coverage_recovery_plan(
        source_capture_manifest_path=boundary.active_root
        / str(authority["source_capture_manifest_path"]),
        mapping_approval_path=boundary.active_root
        / str(authority["mapping_approval_path"]),
        candidate_calendar_manifest_path=boundary.active_root
        / str(authority["candidate_calendar_manifest_path"]),
        failed_index_manifest_path=boundary.active_root
        / str(authority["failed_index_manifest_path"]),
        boundary=boundary,
        expected_markets=approved_research_markets(
            boundary.active_root
            / "configs"
            / "research_universe_contract.json"
        ),
        implementation_sha256=scope["implementation_sha256"],
        universe_contract_sha256=str(
            scope["universe_contract_sha256"]
        ),
        active_pointer_path=boundary.active_root
        / str(scope["active_pointer_path"]),
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "schedule recovery plan is not reproducible"
        )
    return dict(payload)


def validate_schedule_coverage_recovery_approval(
    approval: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> str:
    core_keys = {
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    core = {key: approval[key] for key in core_keys if key in approval}
    if (
        set(approval) != {*core_keys, "approval_receipt_id"}
        or approval.get("schema_version")
        != SCHEDULE_RECOVERY_APPROVAL_SCHEMA
        or approval.get("operation") != SCHEDULE_RECOVERY_OPERATION
        or approval.get("status") != "APPROVED"
        or approval.get("plan_id") != plan["plan_id"]
        or approval.get("plan_sha256") != plan_sha256
        or approval.get("approval_receipt_id") != sha256_json(core)
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"]))
        is None
    ):
        raise CalendarCaptureError(
            "schedule recovery lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def capture_schedule_coverage_recovery(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    plan = validate_schedule_coverage_recovery_plan(
        _canonical_object(
            plan_path, description="schedule recovery plan"
        ),
        boundary=publisher.boundary,
    )
    approval = _canonical_object(
        approval_path, description="schedule recovery approval"
    )
    approval_id = validate_schedule_coverage_recovery_approval(
        approval,
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != _capture_implementation_hashes(
        publisher.boundary.active_root
    ):
        raise CalendarCaptureError(
            "schedule recovery implementation changed"
        )
    if (
        publisher.boundary.active_root
        / str(scope["active_pointer_path"])
    ).exists():
        raise CalendarCaptureError(
            "schedule recovery active-pointer state changed"
        )
    stage = publisher.create_stage("cme_calendar_schedule_recovery")
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    responses: list[dict[str, object]] = []
    total_bytes = 0
    for item in scope["reused_responses"]:  # type: ignore[union-attr]
        assert isinstance(item, dict)
        response = item["response"]
        assert isinstance(response, dict)
        source = publisher.boundary.assert_active_path(
            publisher.boundary.active_root
            / str(item["source_relative_path"]),
            purpose="reused accepted schedule response",
            subtree="data/reference/exchange_calendars",
        )
        if (
            not source.is_file()
            or source.stat().st_size != response["size"]
            or sha256_file(source) != response["sha256"]
        ):
            raise CalendarCaptureError(
                "accepted schedule response drifted before recovery"
            )
        name = Path(str(response["logical_path"])).name
        destination = stage / name
        destination.write_bytes(source.read_bytes())
        logical = str(response["logical_path"])
        logical_paths[name] = logical
        staged_paths[logical] = name
        responses.append(dict(response))
        total_bytes += int(response["size"])
    bounds = scope["bounds"]
    assert isinstance(bounds, dict)
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    retrieved_at = datetime.now(timezone.utc).replace(microsecond=0)
    for ordinal, spec in enumerate(
        scope["network_requests"],  # type: ignore[union-attr]
        start=len(responses) + 1,
    ):
        assert isinstance(spec, dict)
        if (
            monotonic_time.monotonic() - started
            > int(bounds["max_duration_seconds"])
        ):
            raise CalendarCaptureError(
                "schedule recovery duration ceiling is exceeded"
            )
        url = str(spec["url"])
        _safe_url(url)
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": (
                    "futures-intraday-model-v2-calendar/1.0"
                ),
            },
            method="GET",
        )
        try:
            with opener.open(
                request,
                timeout=int(bounds["request_timeout_seconds"]),
            ) as response:
                if (
                    response.status != 200
                    or response.geturl() != url
                    or response.headers.get_content_type()
                    != "application/json"
                ):
                    raise CalendarCaptureError(
                        "schedule recovery response contract failed"
                    )
                remaining = (
                    int(bounds["max_total_bytes"]) - total_bytes
                )
                body = response.read(remaining + 1)
                if len(body) > remaining:
                    raise CalendarCaptureError(
                        "schedule recovery byte ceiling exceeded"
                    )
                safe_headers = {
                    key.lower(): value
                    for key, value in response.headers.items()
                    if key.lower() in _SAFE_HEADERS
                }
        except CalendarCaptureError:
            raise
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
        ) as exc:
            raise CalendarCaptureError(
                f"schedule recovery request {ordinal} failed"
            ) from exc
        total_bytes += len(body)
        request_id = str(spec["request_id"])
        name = f"{ordinal:03d}-{request_id}.json"
        destination = stage / name
        destination.write_bytes(body)
        logical = f"data/reference/exchange_calendars/{name}"
        logical_paths[name] = logical
        staged_paths[logical] = name
        received_at = datetime.now(timezone.utc).replace(microsecond=0)
        responses.append(
            {
                "content_type": "application/json",
                "logical_path": logical,
                "received_at_utc": received_at.isoformat().replace(
                    "+00:00", "Z"
                ),
                "request_id": request_id,
                "request_kind": "SCHEDULE",
                "safe_headers": dict(sorted(safe_headers.items())),
                "sha256": sha256_file(destination),
                "size": len(body),
                "status_code": 200,
                "url": url,
            }
        )
    elapsed_milliseconds = int(
        (monotonic_time.monotonic() - started) * 1000
    )
    if (
        elapsed_milliseconds
        > int(bounds["max_duration_seconds"]) * 1000
        or len(responses) != int(bounds["max_output_responses"])
        or len(scope["network_requests"])  # type: ignore[arg-type]
        != int(bounds["max_network_requests"])
    ):
        raise CalendarCaptureError(
            "schedule recovery completion bound failed"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    receipt_bounds = {
        key: bounds[key]
        for key in (
            "allow_redirects",
            "max_duration_seconds",
            "max_network_requests",
            "max_output_responses",
            "max_total_bytes",
            "retries",
            "workers",
        )
    }
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "bounds": receipt_bounds,
        "capture_approval": dict(approval),
        "coverage_end_trade_date": scope["coverage_end_trade_date"],
        "coverage_start_trade_date": scope[
            "coverage_start_trade_date"
        ],
        "elapsed_milliseconds": elapsed_milliseconds,
        "mapping_capture_release_id": authority[
            "mapping_capture_release_id"
        ],
        "mode": "SCHEDULE_COVERAGE_RECOVERY",
        "network_request_count": len(scope["network_requests"]),  # type: ignore[arg-type]
        "parser_version": PARSER_VERSION,
        "plan_id": plan["plan_id"],
        "predecessor_capture_release_id": authority[
            "source_capture_release_id"
        ],
        "request_count": len(responses),
        "responses": responses,
        "retrieved_at_utc": retrieved_at.isoformat().replace(
            "+00:00", "Z"
        ),
        "reused_response_count": len(scope["reused_responses"]),  # type: ignore[arg-type]
        "schema_version": SCHEDULE_RECOVERY_CAPTURE_SCHEMA_VERSION,
        "total_bytes": total_bytes,
    }
    capture_receipt = {**core, "capture_id": sha256_json(core)}
    source_release_ids = tuple(
        sorted(
            (
                str(authority["source_capture_release_id"]),
                str(authority["mapping_capture_release_id"]),
            )
        )
    )
    release = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=CAPTURE_RELEASE_KIND,
        schema_version=SCHEDULE_RECOVERY_CAPTURE_SCHEMA_VERSION,
        logical_paths=logical_paths,
        source_release_ids=source_release_ids,
        embedded_documents={
            "capture_receipt.json": capture_receipt
        },
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture_receipt["capture_id"],
            "coverage_end_trade_date": scope[
                "coverage_end_trade_date"
            ],
            "coverage_start_trade_date": scope[
                "coverage_start_trade_date"
            ],
            "parser_version": PARSER_VERSION,
            "plan_id": plan["plan_id"],
            "retrieved_at_utc": core["retrieved_at_utc"],
        },
    )
    manifest_path = publisher.publish(
        stage, release, staged_paths=staged_paths
    )
    receipt = VerifiedReleaseReceipt.from_manifest(
        manifest_path, publisher.boundary
    )
    load_cme_capture(receipt, boundary=publisher.boundary)
    return receipt


def build_calendar_activation_plan(
    *,
    candidate_calendar_manifest_path: Path,
    diff_report_path: Path,
    boundary: RepoBoundary,
    expected_markets: Sequence[str],
    implementation_sha256: Mapping[str, str],
    policy_sha256: str,
    universe_contract_sha256: str,
    predecessor_index_manifest_path: Path | None = None,
    active_pointer_path: Path | None = None,
) -> dict[str, object]:
    candidate_receipt = _receipt_from_manifest(
        candidate_calendar_manifest_path, boundary=boundary
    )
    candidate = VerifiedExchangeCalendar.from_release(
        candidate_receipt,
        boundary=boundary,
        expected_markets=expected_markets,
    )
    predecessor_index = (
        load_calendar_index(
            _receipt_from_manifest(
                predecessor_index_manifest_path, boundary=boundary
            ),
            boundary=boundary,
            expected_markets=expected_markets,
        )
        if predecessor_index_manifest_path is not None
        else None
    )
    predecessor_calendar = None
    if candidate.predecessor_calendar_release_id is not None:
        if predecessor_index is None:
            raise IntegrityError(
                "activation plan lacks the candidate calendar predecessor"
            )
        predecessor_calendar = predecessor_index.calendar_by_release_id.get(
            candidate.predecessor_calendar_release_id
        )
        if predecessor_calendar is None:
            raise IntegrityError(
                "activation plan predecessor calendar is not active"
            )
    elif predecessor_index is not None:
        raise IntegrityError(
            "initial calendar candidate cannot replace an existing index"
        )
    diff = _canonical_object(
        diff_report_path, description="calendar activation diff report"
    )
    expected_diff = diff_exchange_calendars(
        predecessor_calendar, candidate
    )
    if diff != expected_diff:
        raise IntegrityError("calendar activation diff report drifted")
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != CAPTURE_IMPLEMENTATION_PATHS
        or any(
            _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
        or _SHA256.fullmatch(policy_sha256) is None
        or _SHA256.fullmatch(universe_contract_sha256) is None
    ):
        raise ContractError(
            "calendar activation implementation authority is invalid"
        )
    markets = tuple(sorted(expected_markets))
    if (
        not markets
        or tuple(expected_markets) != markets
        or len(markets) != len(set(markets))
        or any(
            type(market) is not str or not market
            for market in markets
        )
    ):
        raise ContractError(
            "calendar activation market authority is invalid"
        )
    pointer = (
        active_pointer_path
        if active_pointer_path is not None
        else boundary.active_root
        / "configs"
        / "active_exchange_calendar.json"
    )
    pointer = boundary.assert_active_path(
        pointer,
        purpose="active exchange calendar pointer",
        subtree="configs",
    )
    if predecessor_index is None and pointer.exists():
        raise IntegrityError(
            "initial calendar activation conflicts with an active pointer"
        )
    if predecessor_index is not None and not pointer.is_file():
        raise IntegrityError(
            "calendar successor activation lacks its active pointer"
        )
    candidate_manifest = candidate_receipt.verify(boundary)
    scope: dict[str, object] = {
        "active_pointer_path": pointer.relative_to(
            boundary.active_root
        ).as_posix(),
        "candidate_calendar_id": candidate.calendar_id,
        "candidate_calendar_manifest_path": (
            candidate_calendar_manifest_path.resolve(strict=True)
            .relative_to(boundary.active_root)
            .as_posix()
        ),
        "candidate_calendar_manifest_sha256": sha256_file(
            candidate_calendar_manifest_path
        ),
        "candidate_calendar_release_id": candidate_receipt.release_id,
        "coverage_end_trade_date": candidate.coverage_end.isoformat(),
        "coverage_start_trade_date": candidate.coverage_start.isoformat(),
        "diff_report_id": diff["diff_report_id"],
        "diff_report_path": diff_report_path.resolve(strict=True)
        .relative_to(boundary.active_root)
        .as_posix(),
        "diff_report_sha256": sha256_file(diff_report_path),
        "expected_markets": list(markets),
        "forbidden_actions": [
            "CALL_ANY_PROVIDER",
            "DELETE_OR_OVERWRITE_ACCEPTED_RELEASE",
            "REBUILD_FOUNDATION",
            "TRADE_OR_PLACE_ORDER",
        ],
        "implementation_sha256": implementation,
        "mapping_approval_receipt_id": candidate_manifest.metadata[
            "mapping_approval_receipt_id"
        ],
        "output_paths": {
            "active_pointer": pointer.relative_to(
                boundary.active_root
            ).as_posix(),
            "index_manifest_template": (
                "manifests/data_releases/controls/{release_id}.json"
            ),
            "publication_lock": "state/locks/data-publication.lock",
        },
        "policy_sha256": policy_sha256,
        "predecessor_index_manifest_path": (
            predecessor_index_manifest_path.resolve(strict=True)
            .relative_to(boundary.active_root)
            .as_posix()
            if predecessor_index_manifest_path is not None
            else None
        ),
        "predecessor_index_release_id": (
            predecessor_index.receipt.release_id
            if predecessor_index is not None
            else None
        ),
        "source_capture_release_id": (
            candidate.source_capture_receipt.release_id
        ),
        "stop_conditions": [
            "ACTIVATION_APPROVAL_MISMATCH",
            "ACTIVE_POINTER_STATE_DRIFT",
            "CANDIDATE_CALENDAR_OR_DIFF_DRIFT",
            "FRESHNESS_OR_COVERAGE_FAILURE",
            "IMPLEMENTATION_OR_CONTRACT_HASH_DRIFT",
            "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
        ],
        "universe_contract_sha256": universe_contract_sha256,
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": CALENDAR_ACTIVATION_OPERATION,
        "schema_version": CALENDAR_ACTIVATION_PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_calendar_activation_plan(
    payload: Mapping[str, object], *, boundary: RepoBoundary
) -> dict[str, object]:
    scope = payload.get("scope")
    core = {key: payload[key] for key in payload if key != "plan_id"}
    if (
        set(payload)
        != {
            "classification",
            "execution_authorized",
            "operation",
            "plan_id",
            "schema_version",
            "scope",
        }
        or payload.get("schema_version") != CALENDAR_ACTIVATION_PLAN_SCHEMA
        or payload.get("operation") != CALENDAR_ACTIVATION_OPERATION
        or payload.get("classification")
        != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload.get("execution_authorized") is not False
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
        or not isinstance(scope.get("implementation_sha256"), dict)
        or not isinstance(scope.get("expected_markets"), list)
    ):
        raise IntegrityError("calendar activation plan identity is invalid")
    predecessor_path = scope.get("predecessor_index_manifest_path")
    expected = build_calendar_activation_plan(
        candidate_calendar_manifest_path=boundary.active_root
        / str(scope["candidate_calendar_manifest_path"]),
        diff_report_path=boundary.active_root
        / str(scope["diff_report_path"]),
        boundary=boundary,
        expected_markets=tuple(scope["expected_markets"]),
        implementation_sha256=scope["implementation_sha256"],
        policy_sha256=str(scope["policy_sha256"]),
        universe_contract_sha256=str(
            scope["universe_contract_sha256"]
        ),
        predecessor_index_manifest_path=(
            boundary.active_root / str(predecessor_path)
            if predecessor_path is not None
            else None
        ),
        active_pointer_path=boundary.active_root
        / str(scope["active_pointer_path"]),
    )
    if dict(payload) != expected:
        raise IntegrityError("calendar activation plan is not reproducible")
    return dict(payload)


def _receipt_from_manifest(
    path: Path, *, boundary: RepoBoundary
) -> VerifiedReleaseReceipt:
    return VerifiedReleaseReceipt.from_manifest(path, boundary)


def _write_canonical(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_bytes(dict(payload)) + b"\n"
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        remaining = memoryview(encoded)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError("canonical file write made no progress")
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _replace_active_pointer(
    path: Path, payload: Mapping[str, object], *, boundary: RepoBoundary
) -> None:
    target = boundary.assert_active_path(
        path, purpose="active calendar pointer", subtree="configs"
    )
    temporary = target.with_name(f".{target.name}.new")
    boundary.assert_active_path(
        temporary, purpose="active calendar pointer staging", subtree="configs"
    )
    if temporary.exists():
        raise IntegrityError("active calendar pointer staging path already exists")
    _write_canonical(temporary, payload)
    os.replace(temporary, target)
    fsync_directory(target.parent)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--source-contract", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan")
    plan.add_argument(
        "--mode",
        choices=("bootstrap", "product-discovery", "steady-state"),
        required=True,
    )
    plan.add_argument("--coverage-start", type=date.fromisoformat, required=True)
    plan.add_argument("--coverage-end", type=date.fromisoformat, required=True)
    plan.add_argument("--product-id", action="append", default=[])
    plan.add_argument("--predecessor-capture-release-id")
    plan.add_argument("--output", type=Path, required=True)

    historical_source_plan = commands.add_parser(
        "historical-source-discovery-plan"
    )
    historical_source_plan.add_argument(
        "--probe-manifest", type=Path, required=True
    )
    historical_source_plan.add_argument(
        "--probe-result", type=Path, required=True
    )
    historical_source_plan.add_argument("--output", type=Path, required=True)

    historical_source_capture = commands.add_parser(
        "historical-source-discovery-capture"
    )
    historical_source_capture.add_argument("--plan", type=Path, required=True)
    historical_source_capture.add_argument(
        "--approval", type=Path, required=True
    )
    historical_source_capture.add_argument("--execute", action="store_true")

    historical_archive_plan = commands.add_parser(
        "historical-archive-landing-plan"
    )
    historical_archive_plan.add_argument(
        "--notices-manifest", type=Path, required=True
    )
    historical_archive_plan.add_argument(
        "--candidate-result", type=Path, required=True
    )
    historical_archive_plan.add_argument("--output", type=Path, required=True)

    historical_archive_capture = commands.add_parser(
        "historical-archive-landing-capture"
    )
    historical_archive_capture.add_argument("--plan", type=Path, required=True)
    historical_archive_capture.add_argument(
        "--approval", type=Path, required=True
    )
    historical_archive_capture.add_argument("--execute", action="store_true")

    notice_client_plan = commands.add_parser("notice-search-client-plan")
    notice_client_plan.add_argument(
        "--notices-manifest", type=Path, required=True
    )
    notice_client_plan.add_argument(
        "--archive-manifest", type=Path, required=True
    )
    notice_client_plan.add_argument(
        "--archive-assessment", type=Path, required=True
    )
    notice_client_plan.add_argument("--output", type=Path, required=True)

    notice_client_capture = commands.add_parser(
        "notice-search-client-capture"
    )
    notice_client_capture.add_argument("--plan", type=Path, required=True)
    notice_client_capture.add_argument(
        "--approval", type=Path, required=True
    )
    notice_client_capture.add_argument("--execute", action="store_true")

    notice_endpoint_assess = commands.add_parser(
        "historical-notice-endpoint-assess"
    )
    notice_endpoint_assess.add_argument(
        "--notices-manifest", type=Path, required=True
    )
    notice_endpoint_assess.add_argument(
        "--client-manifest", type=Path, required=True
    )
    notice_endpoint_assess.add_argument(
        "--common-manifest", type=Path, required=True
    )
    notice_endpoint_assess.add_argument("--output", type=Path, required=True)

    notice_search_plan = commands.add_parser(
        "historical-notice-capability-plan"
    )
    notice_search_plan.add_argument(
        "--notices-manifest", type=Path, required=True
    )
    notice_search_plan.add_argument(
        "--client-manifest", type=Path, required=True
    )
    notice_search_plan.add_argument(
        "--common-manifest", type=Path, required=True
    )
    notice_search_plan.add_argument(
        "--endpoint-assessment", type=Path, required=True
    )
    notice_search_plan.add_argument("--output", type=Path, required=True)

    notice_search_capture = commands.add_parser(
        "historical-notice-capability-capture"
    )
    notice_search_capture.add_argument("--plan", type=Path, required=True)
    notice_search_capture.add_argument(
        "--approval", type=Path, required=True
    )
    notice_search_capture.add_argument("--execute", action="store_true")

    notice_capability_assess = commands.add_parser(
        "historical-notice-capability-assess"
    )
    notice_capability_assess.add_argument(
        "--capability-manifest", type=Path, required=True
    )
    notice_capability_assess.add_argument("--output", type=Path, required=True)

    notice_metadata_plan = commands.add_parser(
        "historical-notice-metadata-plan"
    )
    notice_metadata_plan.add_argument(
        "--capability-manifest", type=Path, required=True
    )
    notice_metadata_plan.add_argument(
        "--capability-assessment", type=Path, required=True
    )
    notice_metadata_plan.add_argument("--output", type=Path, required=True)

    notice_metadata_capture = commands.add_parser(
        "historical-notice-metadata-capture"
    )
    notice_metadata_capture.add_argument("--plan", type=Path, required=True)
    notice_metadata_capture.add_argument(
        "--approval", type=Path, required=True
    )
    notice_metadata_capture.add_argument("--execute", action="store_true")

    notice_semantic_assess = commands.add_parser(
        "historical-notice-semantic-assess"
    )
    notice_semantic_assess.add_argument(
        "--semantic-manifest", type=Path, required=True
    )
    notice_semantic_assess.add_argument("--output", type=Path, required=True)

    notice_pagination_plan = commands.add_parser(
        "historical-notice-pagination-plan"
    )
    notice_pagination_plan.add_argument(
        "--semantic-manifest", type=Path, required=True
    )
    notice_pagination_plan.add_argument(
        "--semantic-assessment", type=Path, required=True
    )
    notice_pagination_plan.add_argument("--output", type=Path, required=True)

    notice_pagination_capture = commands.add_parser(
        "historical-notice-pagination-capture"
    )
    notice_pagination_capture.add_argument("--plan", type=Path, required=True)
    notice_pagination_capture.add_argument(
        "--approval", type=Path, required=True
    )
    notice_pagination_capture.add_argument("--execute", action="store_true")

    notice_metadata_index = commands.add_parser(
        "historical-notice-metadata-index"
    )
    notice_metadata_index.add_argument(
        "--pagination-manifest", type=Path, required=True
    )
    notice_metadata_index.add_argument("--output", type=Path, required=True)

    notice_document_plan = commands.add_parser(
        "historical-notice-document-probe-plan"
    )
    notice_document_plan.add_argument(
        "--pagination-manifest", type=Path, required=True
    )
    notice_document_plan.add_argument(
        "--metadata-index", type=Path, required=True
    )
    notice_document_plan.add_argument("--output", type=Path, required=True)

    notice_document_capture = commands.add_parser(
        "historical-notice-document-probe-capture"
    )
    notice_document_capture.add_argument("--plan", type=Path, required=True)
    notice_document_capture.add_argument(
        "--approval", type=Path, required=True
    )
    notice_document_capture.add_argument("--execute", action="store_true")

    notice_document_assess = commands.add_parser(
        "historical-notice-document-probe-assess"
    )
    notice_document_assess.add_argument(
        "--probe-manifest", type=Path, required=True
    )
    notice_document_assess.add_argument(
        "--metadata-index", type=Path, required=True
    )
    notice_document_assess.add_argument("--output", type=Path, required=True)

    notice_union_plan = commands.add_parser(
        "historical-notice-document-union-plan"
    )
    notice_union_plan.add_argument(
        "--probe-manifest", type=Path, required=True
    )
    notice_union_plan.add_argument(
        "--metadata-index", type=Path, required=True
    )
    notice_union_plan.add_argument(
        "--probe-assessment", type=Path, required=True
    )
    notice_union_plan.add_argument("--output", type=Path, required=True)

    notice_union_capture = commands.add_parser(
        "historical-notice-document-union-capture"
    )
    notice_union_capture.add_argument("--plan", type=Path, required=True)
    notice_union_capture.add_argument("--approval", type=Path, required=True)
    notice_union_capture.add_argument("--execute", action="store_true")

    notice_union_recovery_plan = commands.add_parser(
        "historical-notice-document-union-recovery-plan"
    )
    notice_union_recovery_plan.add_argument(
        "--predecessor-plan", type=Path, required=True
    )
    notice_union_recovery_plan.add_argument(
        "--failure-report", type=Path, required=True
    )
    notice_union_recovery_plan.add_argument(
        "--output", type=Path, required=True
    )

    notice_union_recovery_capture = commands.add_parser(
        "historical-notice-document-union-recovery-capture"
    )
    notice_union_recovery_capture.add_argument(
        "--plan", type=Path, required=True
    )
    notice_union_recovery_capture.add_argument(
        "--approval", type=Path, required=True
    )
    notice_union_recovery_capture.add_argument(
        "--execute", action="store_true"
    )

    notice_attachment_assess = commands.add_parser(
        "historical-notice-attachment-assess"
    )
    notice_attachment_assess.add_argument(
        "--union-manifest", type=Path, required=True
    )
    notice_attachment_assess.add_argument(
        "--output", type=Path, required=True
    )

    notice_attachment_plan = commands.add_parser(
        "historical-notice-attachment-plan"
    )
    notice_attachment_plan.add_argument(
        "--assessment", type=Path, required=True
    )
    notice_attachment_plan.add_argument(
        "--union-manifest", type=Path, required=True
    )
    notice_attachment_plan.add_argument(
        "--output", type=Path, required=True
    )

    notice_attachment_capture = commands.add_parser(
        "historical-notice-attachment-capture"
    )
    notice_attachment_capture.add_argument(
        "--plan", type=Path, required=True
    )
    notice_attachment_capture.add_argument(
        "--approval", type=Path, required=True
    )
    notice_attachment_capture.add_argument(
        "--execute", action="store_true"
    )

    notice_attachment_diagnostic_plan = commands.add_parser(
        "historical-notice-attachment-diagnostic-plan"
    )
    notice_attachment_diagnostic_plan.add_argument(
        "--predecessor-plan", type=Path, required=True
    )
    notice_attachment_diagnostic_plan.add_argument(
        "--predecessor-approval", type=Path, required=True
    )
    notice_attachment_diagnostic_plan.add_argument(
        "--failure-report", type=Path, required=True
    )
    notice_attachment_diagnostic_plan.add_argument(
        "--output", type=Path, required=True
    )

    notice_attachment_diagnostic = commands.add_parser(
        "historical-notice-attachment-diagnostic"
    )
    notice_attachment_diagnostic.add_argument(
        "--plan", type=Path, required=True
    )
    notice_attachment_diagnostic.add_argument(
        "--approval", type=Path, required=True
    )
    notice_attachment_diagnostic.add_argument(
        "--predecessor-plan", type=Path, required=True
    )
    notice_attachment_diagnostic.add_argument(
        "--predecessor-approval", type=Path, required=True
    )
    notice_attachment_diagnostic.add_argument(
        "--failure-report", type=Path, required=True
    )
    notice_attachment_diagnostic.add_argument(
        "--execute", action="store_true"
    )

    notice_attachment_recovery_plan = commands.add_parser(
        "historical-notice-attachment-recovery-plan"
    )
    notice_attachment_recovery_plan.add_argument(
        "--predecessor-plan", type=Path, required=True
    )
    notice_attachment_recovery_plan.add_argument(
        "--predecessor-approval", type=Path, required=True
    )
    notice_attachment_recovery_plan.add_argument(
        "--failure-report", type=Path, required=True
    )
    notice_attachment_recovery_plan.add_argument(
        "--diagnostic-plan", type=Path, required=True
    )
    notice_attachment_recovery_plan.add_argument(
        "--diagnostic-approval", type=Path, required=True
    )
    notice_attachment_recovery_plan.add_argument(
        "--diagnostic-result", type=Path, required=True
    )
    notice_attachment_recovery_plan.add_argument(
        "--output", type=Path, required=True
    )

    notice_attachment_recovery_capture = commands.add_parser(
        "historical-notice-attachment-recovery-capture"
    )
    notice_attachment_recovery_capture.add_argument(
        "--plan", type=Path, required=True
    )
    notice_attachment_recovery_capture.add_argument(
        "--approval", type=Path, required=True
    )
    notice_attachment_recovery_capture.add_argument(
        "--execute", action="store_true"
    )

    notice_attachment_reconciliation_plan = commands.add_parser(
        "historical-notice-attachment-reconciliation-plan"
    )
    notice_attachment_reconciliation_plan.add_argument(
        "--predecessor-plan", type=Path, required=True
    )
    notice_attachment_reconciliation_plan.add_argument(
        "--predecessor-approval", type=Path, required=True
    )
    notice_attachment_reconciliation_plan.add_argument(
        "--predecessor-failure", type=Path, required=True
    )
    notice_attachment_reconciliation_plan.add_argument(
        "--output", type=Path, required=True
    )

    notice_attachment_reconciliation_capture = commands.add_parser(
        "historical-notice-attachment-reconciliation-capture"
    )
    notice_attachment_reconciliation_capture.add_argument(
        "--plan", type=Path, required=True
    )
    notice_attachment_reconciliation_capture.add_argument(
        "--approval", type=Path, required=True
    )
    notice_attachment_reconciliation_capture.add_argument(
        "--execute", action="store_true"
    )

    notice_attachment_reconciliation_interruption = commands.add_parser(
        "historical-notice-attachment-reconciliation-interruption"
    )
    notice_attachment_reconciliation_interruption.add_argument(
        "--plan", type=Path, required=True
    )
    notice_attachment_reconciliation_interruption.add_argument(
        "--approval", type=Path, required=True
    )
    notice_attachment_reconciliation_interruption.add_argument(
        "--stage", type=Path, required=True
    )
    notice_attachment_reconciliation_interruption.add_argument(
        "--observed-at-utc", required=True
    )
    notice_attachment_reconciliation_interruption.add_argument(
        "--wrapper-exit-code", type=int, required=True
    )
    notice_attachment_reconciliation_interruption.add_argument(
        "--wrapper-timeout-seconds", type=int, required=True
    )
    notice_attachment_reconciliation_interruption.add_argument(
        "--output", type=Path, required=True
    )

    notice_attachment_reconciliation_recovery_plan = commands.add_parser(
        "historical-notice-attachment-reconciliation-recovery-plan"
    )
    notice_attachment_reconciliation_recovery_plan.add_argument(
        "--interruption", type=Path, required=True
    )
    notice_attachment_reconciliation_recovery_plan.add_argument(
        "--output", type=Path, required=True
    )

    notice_attachment_reconciliation_recovery_capture = commands.add_parser(
        "historical-notice-attachment-reconciliation-recovery-capture"
    )
    notice_attachment_reconciliation_recovery_capture.add_argument(
        "--plan", type=Path, required=True
    )
    notice_attachment_reconciliation_recovery_capture.add_argument(
        "--approval", type=Path, required=True
    )
    notice_attachment_reconciliation_recovery_capture.add_argument(
        "--execute", action="store_true"
    )

    holiday_schedule_discover = commands.add_parser(
        "historical-holiday-schedule-discover"
    )
    holiday_schedule_discover.add_argument(
        "--source-manifest", type=Path, required=True
    )
    holiday_schedule_discover.add_argument(
        "--output", type=Path, required=True
    )

    holiday_schedule_plan = commands.add_parser(
        "historical-holiday-schedule-plan"
    )
    holiday_schedule_plan.add_argument(
        "--assessment", type=Path, required=True
    )
    holiday_schedule_plan.add_argument(
        "--output", type=Path, required=True
    )

    holiday_schedule_capture = commands.add_parser(
        "historical-holiday-schedule-capture"
    )
    holiday_schedule_capture.add_argument(
        "--plan", type=Path, required=True
    )
    holiday_schedule_capture.add_argument(
        "--approval", type=Path, required=True
    )
    holiday_schedule_capture.add_argument(
        "--execute", action="store_true"
    )

    client_plan = commands.add_parser("client-contract-plan")
    client_plan.add_argument("--landing-manifest", type=Path, required=True)
    client_plan.add_argument("--output", type=Path, required=True)

    client_capture = commands.add_parser("client-contract-capture")
    client_capture.add_argument("--plan", type=Path, required=True)
    client_capture.add_argument("--approval", type=Path, required=True)
    client_capture.add_argument("--execute", action="store_true")

    client_parse = commands.add_parser("client-contract-parse")
    client_parse.add_argument("--capture-manifest", type=Path, required=True)
    client_parse.add_argument("--output", type=Path, required=True)

    dependency_plan = commands.add_parser("client-dependency-plan")
    dependency_plan.add_argument("--client-manifest", type=Path, required=True)
    dependency_plan.add_argument("--output", type=Path, required=True)

    dependency_capture = commands.add_parser("client-dependency-capture")
    dependency_capture.add_argument("--plan", type=Path, required=True)
    dependency_capture.add_argument("--approval", type=Path, required=True)
    dependency_capture.add_argument("--execute", action="store_true")

    dependency_parse = commands.add_parser("client-dependency-parse")
    dependency_parse.add_argument("--capture-manifest", type=Path, required=True)
    dependency_parse.add_argument("--output", type=Path, required=True)

    discovery_plan = commands.add_parser("nonempty-discovery-plan")
    discovery_plan.add_argument("--dependency-manifest", type=Path, required=True)
    discovery_plan.add_argument("--coverage-date", type=date.fromisoformat, required=True)
    discovery_plan.add_argument("--output", type=Path, required=True)

    discovery_capture = commands.add_parser("nonempty-discovery-capture")
    discovery_capture.add_argument("--plan", type=Path, required=True)
    discovery_capture.add_argument("--approval", type=Path, required=True)
    discovery_capture.add_argument("--execute", action="store_true")

    discovery_parse = commands.add_parser("nonempty-discovery-parse")
    discovery_parse.add_argument("--capture-manifest", type=Path, required=True)
    discovery_parse.add_argument("--output", type=Path, required=True)

    search_plan = commands.add_parser("search-discovery-plan")
    search_plan.add_argument("--predecessor-manifest", type=Path, required=True)
    search_plan.add_argument("--dependency-manifest", type=Path, required=True)
    search_plan.add_argument("--coverage-date", type=date.fromisoformat, required=True)
    search_plan.add_argument("--output", type=Path, required=True)

    search_capture = commands.add_parser("search-discovery-capture")
    search_capture.add_argument("--plan", type=Path, required=True)
    search_capture.add_argument("--approval", type=Path, required=True)
    search_capture.add_argument("--execute", action="store_true")

    recovery_plan = commands.add_parser("search-recovery-plan")
    recovery_plan.add_argument("--failed-plan", type=Path, required=True)
    recovery_plan.add_argument("--failure-report", type=Path, required=True)
    recovery_plan.add_argument("--output", type=Path, required=True)

    recovery_capture = commands.add_parser("search-recovery-capture")
    recovery_capture.add_argument("--plan", type=Path, required=True)
    recovery_capture.add_argument("--approval", type=Path, required=True)
    recovery_capture.add_argument("--execute", action="store_true")

    semantic_recovery_plan = commands.add_parser(
        "semantic-search-recovery-plan"
    )
    semantic_recovery_plan.add_argument(
        "--recovery-plan", type=Path, required=True
    )
    semantic_recovery_plan.add_argument(
        "--recovery-failure-report", type=Path, required=True
    )
    semantic_recovery_plan.add_argument("--output", type=Path, required=True)

    semantic_recovery_capture = commands.add_parser(
        "semantic-search-recovery-capture"
    )
    semantic_recovery_capture.add_argument("--plan", type=Path, required=True)
    semantic_recovery_capture.add_argument(
        "--approval", type=Path, required=True
    )
    semantic_recovery_capture.add_argument("--execute", action="store_true")

    complete_mapping = commands.add_parser("complete-mapping-candidates")
    complete_mapping.add_argument(
        "--predecessor-manifest", type=Path, required=True
    )
    complete_mapping.add_argument(
        "--predecessor-candidates", type=Path, required=True
    )
    complete_mapping.add_argument(
        "--search-manifest", type=Path, required=True
    )
    complete_mapping.add_argument("--output", type=Path, required=True)

    capture = commands.add_parser("capture")
    capture.add_argument("--plan", type=Path, required=True)
    capture.add_argument("--approval", type=Path, required=True)
    capture.add_argument("--execute", action="store_true")

    parse = commands.add_parser("parse")
    parse.add_argument("--capture-manifest", type=Path, required=True)
    parse.add_argument("--mapping-approval", type=Path)
    parse.add_argument("--mapping-candidates-output", type=Path)
    parse.add_argument("--predecessor-calendar-manifest", type=Path)
    parse.add_argument("--execute", action="store_true")

    diff = commands.add_parser("diff")
    diff.add_argument("--candidate-calendar-manifest", type=Path, required=True)
    diff.add_argument("--predecessor-calendar-manifest", type=Path)
    diff.add_argument("--output", type=Path)

    schedule_recovery_plan = commands.add_parser(
        "schedule-recovery-plan"
    )
    schedule_recovery_plan.add_argument(
        "--source-capture-manifest", type=Path, required=True
    )
    schedule_recovery_plan.add_argument(
        "--mapping-approval", type=Path, required=True
    )
    schedule_recovery_plan.add_argument(
        "--candidate-calendar-manifest", type=Path, required=True
    )
    schedule_recovery_plan.add_argument(
        "--failed-index-manifest", type=Path, required=True
    )
    schedule_recovery_plan.add_argument(
        "--active-pointer",
        type=Path,
        default=Path("configs/active_exchange_calendar.json"),
    )
    schedule_recovery_plan.add_argument(
        "--output", type=Path, required=True
    )

    schedule_recovery_capture = commands.add_parser(
        "schedule-recovery-capture"
    )
    schedule_recovery_capture.add_argument(
        "--plan", type=Path, required=True
    )
    schedule_recovery_capture.add_argument(
        "--approval", type=Path, required=True
    )
    schedule_recovery_capture.add_argument(
        "--execute", action="store_true"
    )

    activation_plan = commands.add_parser("activation-plan")
    activation_plan.add_argument(
        "--candidate-calendar-manifest", type=Path, required=True
    )
    activation_plan.add_argument("--diff-report", type=Path, required=True)
    activation_plan.add_argument("--predecessor-index-manifest", type=Path)
    activation_plan.add_argument(
        "--active-pointer",
        type=Path,
        default=Path("configs/active_exchange_calendar.json"),
    )
    activation_plan.add_argument("--output", type=Path, required=True)

    activate = commands.add_parser("activate")
    activate.add_argument("--activation-plan", type=Path, required=True)
    activate.add_argument("--candidate-calendar-manifest", type=Path, required=True)
    activate.add_argument("--predecessor-index-manifest", type=Path)
    activate.add_argument("--activation-approval", type=Path, required=True)
    activate.add_argument(
        "--active-pointer",
        type=Path,
        default=Path("configs/active_exchange_calendar.json"),
    )
    activate.add_argument("--execute", action="store_true")

    verify = commands.add_parser("verify")
    verify.add_argument(
        "--active-pointer",
        type=Path,
        default=Path("configs/active_exchange_calendar.json"),
    )
    verify.add_argument("--at-utc")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    repository_root = args.repository_root.resolve(strict=True)
    source_contract = args.source_contract.resolve(strict=True)
    boundary = _boundary(repository_root, source_contract)
    load_exchange_calendar_policy(
        boundary.active_root / "configs" / "exchange_calendar_policy.json"
    )
    markets = approved_research_markets(
        boundary.active_root / "configs" / "research_universe_contract.json"
    )
    if args.command == "plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output, purpose="CME capture plan", subtree="reports"
        )
        payload = build_capture_plan(
            mode=args.mode,
            coverage_start=args.coverage_start,
            coverage_end=args.coverage_end,
            implementation_sha256=_capture_implementation_hashes(
                boundary.active_root
            ),
            product_ids=tuple(args.product_id),
            predecessor_capture_release_id=args.predecessor_capture_release_id,
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-source-discovery-plan":
        probe_manifest = args.probe_manifest.resolve(strict=True)
        probe_result = args.probe_result.resolve(strict=True)
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical-source discovery plan",
            subtree="reports",
        )
        payload = build_historical_source_discovery_plan(
            authority=historical_source_authority(
                probe_manifest_path=probe_manifest,
                probe_result_path=probe_result,
                boundary=boundary,
            ),
            implementation_sha256=historical_source_implementation_hashes(
                boundary.active_root
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-source-discovery-capture":
        if not args.execute:
            parser.error(
                "CME historical-source discovery capture requires explicit --execute"
            )
        receipt = capture_historical_source_discovery(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "historical-archive-landing-plan":
        notices_manifest = args.notices_manifest.resolve(strict=True)
        candidate_result = args.candidate_result.resolve(strict=True)
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical advisory-archive landing plan",
            subtree="reports",
        )
        payload = build_historical_archive_plan(
            authority=historical_archive_authority(
                notices_manifest_path=notices_manifest,
                candidate_result_path=candidate_result,
                boundary=boundary,
            ),
            implementation_sha256=historical_archive_implementation_hashes(
                boundary.active_root
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-archive-landing-capture":
        if not args.execute:
            parser.error(
                "CME historical advisory-archive landing capture "
                "requires explicit --execute"
            )
        receipt = capture_historical_archive_landing(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "notice-search-client-plan":
        notices_manifest = args.notices_manifest.resolve(strict=True)
        archive_manifest = args.archive_manifest.resolve(strict=True)
        archive_assessment = args.archive_assessment.resolve(strict=True)
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME Notices search client capture plan",
            subtree="reports",
        )
        payload = build_notice_client_plan(
            authority=notice_client_authority(
                notices_manifest_path=notices_manifest,
                archive_manifest_path=archive_manifest,
                assessment_path=archive_assessment,
                boundary=boundary,
            ),
            implementation_sha256=notice_client_implementation_hashes(
                boundary.active_root
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "notice-search-client-capture":
        if not args.execute:
            parser.error(
                "CME Notices search client capture requires explicit --execute"
            )
        receipt = capture_notice_client_contract(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "historical-notice-endpoint-assess":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical Notices endpoint assessment",
            subtree="reports",
        )
        payload = build_notice_endpoint_assessment(
            derive_notice_endpoint_evidence(
                notices_manifest_path=args.notices_manifest.resolve(
                    strict=True
                ),
                client_manifest_path=args.client_manifest.resolve(strict=True),
                common_manifest_path=args.common_manifest.resolve(strict=True),
                boundary=boundary,
            )
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-notice-capability-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical Notices capability plan",
            subtree="reports",
        )
        payload = build_notice_search_capability_plan(
            authority=notice_search_authority(
                notices_manifest_path=args.notices_manifest.resolve(
                    strict=True
                ),
                client_manifest_path=args.client_manifest.resolve(strict=True),
                common_manifest_path=args.common_manifest.resolve(strict=True),
                assessment_path=args.endpoint_assessment.resolve(strict=True),
                boundary=boundary,
            ),
            implementation_sha256=notice_search_implementation_hashes(
                boundary.active_root
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-notice-capability-capture":
        if not args.execute:
            parser.error(
                "CME historical Notices capability capture requires "
                "explicit --execute"
            )
        receipt = capture_notice_search_capability(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "historical-notice-capability-assess":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical Notices capability assessment",
            subtree="reports",
        )
        payload = build_capability_assessment(
            capability_manifest_path=args.capability_manifest.resolve(
                strict=True
            ),
            boundary=boundary,
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-notice-metadata-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical Notices metadata discovery plan",
            subtree="reports",
        )
        payload = build_metadata_discovery_plan(
            authority=metadata_authority(
                capability_manifest_path=args.capability_manifest.resolve(
                    strict=True
                ),
                assessment_path=args.capability_assessment.resolve(strict=True),
                boundary=boundary,
            ),
            implementation_sha256=notice_metadata_implementation_hashes(
                boundary.active_root
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-notice-metadata-capture":
        if not args.execute:
            parser.error(
                "CME historical Notices metadata capture requires "
                "explicit --execute"
            )
        receipt = capture_metadata_discovery(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "historical-notice-semantic-assess":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical Notices semantic assessment",
            subtree="reports",
        )
        payload = build_semantic_assessment(
            semantic_manifest_path=args.semantic_manifest.resolve(strict=True),
            boundary=boundary,
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-notice-pagination-plan":
        assessment_path = args.semantic_assessment.resolve(strict=True)
        assessment = _canonical_object(
            assessment_path,
            description="CME historical Notices semantic assessment",
        )
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical Notices pagination plan",
            subtree="reports",
        )
        payload = build_pagination_plan(
            authority=pagination_authority(
                semantic_manifest_path=args.semantic_manifest.resolve(
                    strict=True
                ),
                assessment_path=assessment_path,
                boundary=boundary,
            ),
            assessment=assessment,
            implementation_sha256=notice_pagination_implementation_hashes(
                boundary.active_root
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-notice-pagination-capture":
        if not args.execute:
            parser.error(
                "CME historical Notices pagination capture requires "
                "explicit --execute"
            )
        receipt = capture_pagination(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "historical-notice-metadata-index":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical notice metadata index",
            subtree="reports",
        )
        payload = build_metadata_index(
            pagination_manifest_path=args.pagination_manifest.resolve(
                strict=True
            ),
            boundary=boundary,
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-notice-document-probe-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical notice-document probe plan",
            subtree="reports",
        )
        payload = build_document_probe_plan(
            authority=document_probe_authority(
                pagination_manifest_path=args.pagination_manifest.resolve(
                    strict=True
                ),
                index_path=args.metadata_index.resolve(strict=True),
                boundary=boundary,
            ),
            implementation_sha256=notice_document_implementation_hashes(
                boundary.active_root
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-notice-document-probe-capture":
        if not args.execute:
            parser.error(
                "CME historical notice-document probe requires "
                "explicit --execute"
            )
        receipt = capture_document_probe(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "historical-notice-document-probe-assess":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical notice-document probe assessment",
            subtree="reports",
        )
        payload = build_probe_assessment(
            probe_manifest_path=args.probe_manifest.resolve(strict=True),
            index_path=args.metadata_index.resolve(strict=True),
            boundary=boundary,
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-notice-document-union-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical notice-document union plan",
            subtree="reports",
        )
        authority, index = union_authority(
            probe_manifest_path=args.probe_manifest.resolve(strict=True),
            index_path=args.metadata_index.resolve(strict=True),
            assessment_path=args.probe_assessment.resolve(strict=True),
            boundary=boundary,
        )
        candidates = index.get("candidates")
        if not isinstance(candidates, list):
            raise IntegrityError(
                "CME historical notice metadata candidates are absent"
            )
        payload = build_union_plan(
            authority=authority,
            candidates=candidates,
            implementation_sha256=notice_union_implementation_hashes(
                boundary.active_root
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-notice-document-union-capture":
        if not args.execute:
            parser.error(
                "CME historical notice-document union requires "
                "explicit --execute"
            )
        receipt = capture_union(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "historical-notice-document-union-recovery-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical notice-document union recovery plan",
            subtree="reports",
        )
        authority, _predecessor, _failure, remaining = (
            notice_union_recovery_authority(
                predecessor_plan_path=args.predecessor_plan.resolve(
                    strict=True
                ),
                failure_report_path=args.failure_report.resolve(strict=True),
                boundary=boundary,
            )
        )
        payload = build_notice_union_recovery_plan(
            authority=authority,
            remaining_requests=remaining,
            implementation_sha256=(
                notice_union_recovery_implementation_hashes(
                    boundary.active_root
                )
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "historical-notice-document-union-recovery-capture":
        if not args.execute:
            parser.error(
                "CME historical notice-document union recovery requires "
                "explicit --execute"
            )
        receipt = capture_recovery_union(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "historical-notice-attachment-assess":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical notice attachment assessment",
            subtree="reports",
        )
        payload = build_attachment_assessment(
            union_manifest_path=args.union_manifest.resolve(strict=True),
            boundary=boundary,
        )
        _write_canonical(output, payload)
        sys.stdout.buffer.write(canonical_bytes(payload) + b"\n")
        return 0
    if args.command == "historical-notice-attachment-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical notice attachment capture plan",
            subtree="reports",
        )
        authority, candidates = notice_attachment_authority(
            assessment_path=args.assessment.resolve(strict=True),
            union_manifest_path=args.union_manifest.resolve(strict=True),
            boundary=boundary,
        )
        payload = build_attachment_capture_plan(
            authority=authority,
            candidates=candidates,
            implementation_sha256=(
                notice_attachment_implementation_hashes(
                    boundary.active_root
                )
            ),
        )
        _write_canonical(output, payload)
        sys.stdout.buffer.write(canonical_bytes(payload) + b"\n")
        return 0
    if args.command == "historical-notice-attachment-capture":
        if not args.execute:
            parser.error(
                "CME historical notice-attachment capture requires "
                "explicit --execute"
            )
        receipt = capture_attachments(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "historical-notice-attachment-diagnostic-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical notice attachment diagnostic plan",
            subtree="reports",
        )
        authority, failed_request, _descriptors = (
            preserved_failure_authority(
                predecessor_plan_path=args.predecessor_plan.resolve(
                    strict=True
                ),
                predecessor_approval_path=(
                    args.predecessor_approval.resolve(strict=True)
                ),
                failure_report_path=args.failure_report.resolve(
                    strict=True
                ),
                boundary=boundary,
            )
        )
        payload = build_notice_attachment_diagnostic_plan(
            authority=authority,
            failed_request=failed_request,
            implementation_sha256=(
                notice_attachment_diagnostic_implementation_hashes(
                    boundary.active_root
                )
            ),
        )
        _write_canonical(output, payload)
        sys.stdout.buffer.write(canonical_bytes(payload) + b"\n")
        return 0
    if args.command == "historical-notice-attachment-diagnostic":
        if not args.execute:
            parser.error(
                "CME historical notice attachment diagnostic requires "
                "explicit --execute"
            )
        payload = run_notice_attachment_diagnostic(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            predecessor_plan_path=args.predecessor_plan.resolve(
                strict=True
            ),
            predecessor_approval_path=(
                args.predecessor_approval.resolve(strict=True)
            ),
            failure_report_path=args.failure_report.resolve(strict=True),
            boundary=boundary,
        )
        sys.stdout.buffer.write(canonical_bytes(payload) + b"\n")
        return 0
    if args.command == "historical-notice-attachment-recovery-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical notice attachment recovery plan",
            subtree="reports",
        )
        authority, remaining, _descriptors, exclusion = (
            notice_attachment_recovery_authority(
                predecessor_plan_path=args.predecessor_plan.resolve(
                    strict=True
                ),
                predecessor_approval_path=(
                    args.predecessor_approval.resolve(strict=True)
                ),
                failure_report_path=args.failure_report.resolve(
                    strict=True
                ),
                diagnostic_plan_path=args.diagnostic_plan.resolve(
                    strict=True
                ),
                diagnostic_approval_path=(
                    args.diagnostic_approval.resolve(strict=True)
                ),
                diagnostic_result_path=args.diagnostic_result.resolve(
                    strict=True
                ),
                boundary=boundary,
            )
        )
        payload = build_notice_attachment_recovery_plan(
            authority=authority,
            remaining_requests=remaining,
            exclusion=exclusion,
            implementation_sha256=(
                notice_attachment_recovery_implementation_hashes(
                    boundary.active_root
                )
            ),
        )
        _write_canonical(output, payload)
        sys.stdout.buffer.write(canonical_bytes(payload) + b"\n")
        return 0
    if args.command == "historical-notice-attachment-recovery-capture":
        if not args.execute:
            parser.error(
                "CME historical notice attachment recovery requires "
                "explicit --execute"
            )
        receipt = capture_attachment_recovery(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "historical-notice-attachment-reconciliation-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical notice attachment reconciliation plan",
            subtree="reports",
        )
        authority, remaining, _descriptors, exclusions = (
            notice_attachment_reconciliation_authority(
                predecessor_plan_path=args.predecessor_plan.resolve(
                    strict=True
                ),
                predecessor_approval_path=(
                    args.predecessor_approval.resolve(strict=True)
                ),
                predecessor_failure_path=(
                    args.predecessor_failure.resolve(strict=True)
                ),
                boundary=boundary,
            )
        )
        payload = build_notice_attachment_reconciliation_plan(
            authority=authority,
            remaining_requests=remaining,
            known_exclusions=exclusions,
            implementation_sha256=(
                notice_attachment_reconciliation_hashes(
                    boundary.active_root
                )
            ),
        )
        _write_canonical(output, payload)
        sys.stdout.buffer.write(canonical_bytes(payload) + b"\n")
        return 0
    if args.command == "historical-notice-attachment-reconciliation-capture":
        if not args.execute:
            parser.error(
                "CME historical notice attachment reconciliation requires "
                "explicit --execute"
            )
        receipt = capture_attachment_reconciliation(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if (
        args.command
        == "historical-notice-attachment-reconciliation-interruption"
    ):
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME reconciliation interruption evidence",
            subtree="reports/exchange_calendar",
        )
        payload = build_notice_attachment_interruption_evidence(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            stage_path=args.stage.resolve(strict=True),
            observed_at_utc=args.observed_at_utc,
            wrapper_exit_code=args.wrapper_exit_code,
            wrapper_timeout_seconds=args.wrapper_timeout_seconds,
            boundary=boundary,
        )
        _write_canonical(output, payload)
        sys.stdout.buffer.write(canonical_bytes(payload) + b"\n")
        return 0
    if (
        args.command
        == "historical-notice-attachment-reconciliation-recovery-plan"
    ):
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME reconciliation-recovery plan",
            subtree="reports/exchange_calendar",
        )
        authority, remaining, _descriptors, exclusions, possible = (
            notice_attachment_reconciliation_recovery_authority(
                interruption_path=args.interruption.resolve(strict=True),
                boundary=boundary,
            )
        )
        payload = build_notice_attachment_reconciliation_recovery_plan(
            authority=authority,
            remaining_requests=remaining,
            known_exclusions=exclusions,
            possibly_in_flight_requests=possible,
            implementation_sha256=(
                notice_attachment_reconciliation_recovery_hashes(
                    boundary.active_root
                )
            ),
        )
        _write_canonical(output, payload)
        sys.stdout.buffer.write(canonical_bytes(payload) + b"\n")
        return 0
    if (
        args.command
        == "historical-notice-attachment-reconciliation-recovery-capture"
    ):
        if not args.execute:
            parser.error(
                "CME historical notice attachment reconciliation recovery "
                "requires explicit --execute"
            )
        receipt = capture_reconciliation_recovery(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "historical-holiday-schedule-discover":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical holiday-schedule discovery",
            subtree="reports/exchange_calendar",
        )
        payload = build_holiday_schedule_discovery(
            source_manifest_path=args.source_manifest.resolve(strict=True),
            boundary=boundary,
        )
        _write_canonical(output, payload)
        sys.stdout.buffer.write(canonical_bytes(payload) + b"\n")
        return 0
    if args.command == "historical-holiday-schedule-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME historical holiday-schedule capture plan",
            subtree="reports/exchange_calendar",
        )
        authority, candidates = holiday_schedule_authority(
            assessment_path=args.assessment.resolve(strict=True),
            boundary=boundary,
        )
        payload = build_holiday_schedule_capture_plan(
            authority=authority,
            candidates=candidates,
            implementation_sha256=holiday_schedule_implementation_hashes(
                boundary.active_root
            ),
        )
        _write_canonical(output, payload)
        sys.stdout.buffer.write(canonical_bytes(payload) + b"\n")
        return 0
    if args.command == "historical-holiday-schedule-capture":
        if not args.execute:
            raise UnauthorizedOperation(
                "CME historical holiday-schedule capture requires explicit "
                "--execute"
            )
        receipt = capture_holiday_schedules(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "client-contract-plan":
        landing_manifest = args.landing_manifest.resolve(strict=True)
        boundary.assert_active_path(
            landing_manifest,
            purpose="accepted CME landing capture manifest",
            subtree="manifests/data_releases/reference",
        )
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output, purpose="CME client-contract capture plan", subtree="reports"
        )
        payload = build_client_contract_plan(
            authority=_client_contract_authority(
                landing_manifest, boundary=boundary
            ),
            implementation_sha256=_capture_implementation_hashes(
                boundary.active_root
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "client-contract-capture":
        if not args.execute:
            parser.error("CME client-contract capture requires explicit --execute")
        publisher = _publisher(
            boundary,
            scope={
                "approval_path": str(args.approval),
                "capture_plan_path": str(args.plan),
            },
        )
        receipt = capture_client_contract(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=publisher,
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "client-contract-parse":
        capture = _receipt_from_manifest(
            args.capture_manifest.resolve(strict=True), boundary=boundary
        )
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME client-contract endpoint candidates",
            subtree="reports",
        )
        candidates = parse_client_contract_candidates(
            capture, boundary=boundary
        )
        _write_canonical(output, candidates)
        print(canonical_bytes(candidates).decode("utf-8"))
        return 0
    if args.command == "client-dependency-plan":
        client_manifest = args.client_manifest.resolve(strict=True)
        boundary.assert_active_path(
            client_manifest,
            purpose="accepted CME client-contract manifest",
            subtree="manifests/data_releases/reference",
        )
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output, purpose="CME client-dependency plan", subtree="reports"
        )
        payload = build_client_dependency_plan(
            authority=_client_dependency_authority(
                client_manifest, boundary=boundary
            ),
            implementation_sha256=_capture_implementation_hashes(
                boundary.active_root
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "client-dependency-capture":
        if not args.execute:
            parser.error("CME client-dependency capture requires explicit --execute")
        publisher = _publisher(
            boundary,
            scope={
                "approval_path": str(args.approval),
                "capture_plan_path": str(args.plan),
            },
        )
        receipt = capture_client_dependency(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=publisher,
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "client-dependency-parse":
        capture = _receipt_from_manifest(
            args.capture_manifest.resolve(strict=True), boundary=boundary
        )
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME client-dependency endpoint candidates",
            subtree="reports",
        )
        candidates = parse_client_dependency_candidates(
            capture, boundary=boundary
        )
        _write_canonical(output, candidates)
        print(canonical_bytes(candidates).decode("utf-8"))
        return 0
    if args.command == "nonempty-discovery-plan":
        dependency_manifest = args.dependency_manifest.resolve(strict=True)
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output, purpose="CME nonempty discovery plan", subtree="reports"
        )
        payload = build_nonempty_discovery_plan(
            authority=_nonempty_discovery_authority(
                dependency_manifest, boundary=boundary
            ),
            coverage_date=args.coverage_date,
            expected_markets=markets,
            implementation_sha256=_capture_implementation_hashes(
                boundary.active_root
            ),
            universe_contract_sha256=sha256_file(
                boundary.active_root
                / "configs"
                / "research_universe_contract.json"
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "nonempty-discovery-capture":
        if not args.execute:
            parser.error("nonempty discovery capture requires explicit --execute")
        receipt = capture_nonempty_discovery(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "nonempty-discovery-parse":
        capture = _receipt_from_manifest(
            args.capture_manifest.resolve(strict=True), boundary=boundary
        )
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME nonempty discovery candidates",
            subtree="reports",
        )
        candidates = generate_nonempty_discovery_candidates(
            capture, boundary=boundary
        )
        _write_canonical(output, candidates)
        print(canonical_bytes(candidates).decode("utf-8"))
        return 0
    if args.command == "search-discovery-plan":
        predecessor = _receipt_from_manifest(
            args.predecessor_manifest.resolve(strict=True), boundary=boundary
        )
        candidates = generate_nonempty_discovery_candidates(
            predecessor, boundary=boundary
        )
        dependency_manifest = args.dependency_manifest.resolve(strict=True)
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output, purpose="CME search discovery plan", subtree="reports"
        )
        payload = build_search_discovery_plan(
            predecessor_receipt=predecessor,
            predecessor_candidates=candidates,
            dependency_authority=_nonempty_discovery_authority(
                dependency_manifest, boundary=boundary
            ),
            coverage_date=args.coverage_date,
            implementation_sha256=_capture_implementation_hashes(
                boundary.active_root
            ),
            universe_contract_sha256=sha256_file(
                boundary.active_root
                / "configs"
                / "research_universe_contract.json"
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "search-discovery-capture":
        if not args.execute:
            parser.error("search discovery capture requires explicit --execute")
        receipt = capture_search_discovery(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "search-recovery-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output, purpose="CME search recovery plan", subtree="reports"
        )
        payload = build_search_recovery_plan(
            failed_plan_path=args.failed_plan.resolve(strict=True),
            failure_report_path=args.failure_report.resolve(strict=True),
            boundary=boundary,
            implementation_sha256=_capture_implementation_hashes(
                boundary.active_root
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "search-recovery-capture":
        if not args.execute:
            parser.error("search recovery capture requires explicit --execute")
        receipt = capture_search_recovery(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "semantic-search-recovery-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME semantic search recovery plan",
            subtree="reports",
        )
        payload = build_semantic_search_recovery_plan(
            recovery_plan_path=args.recovery_plan.resolve(strict=True),
            recovery_failure_report_path=args.recovery_failure_report.resolve(
                strict=True
            ),
            boundary=boundary,
            implementation_sha256=_capture_implementation_hashes(
                boundary.active_root
            ),
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "semantic-search-recovery-capture":
        if not args.execute:
            parser.error(
                "semantic search recovery capture requires explicit --execute"
            )
        receipt = capture_semantic_search_recovery(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "capture_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "complete-mapping-candidates":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="complete CME product mapping candidates",
            subtree="reports",
        )
        payload = generate_complete_product_mapping_candidates(
            predecessor_receipt=_receipt_from_manifest(
                args.predecessor_manifest.resolve(strict=True),
                boundary=boundary,
            ),
            predecessor_candidates=_canonical_object(
                args.predecessor_candidates.resolve(strict=True),
                description="predecessor CME product candidates",
            ),
            search_receipt=_receipt_from_manifest(
                args.search_manifest.resolve(strict=True),
                boundary=boundary,
            ),
            boundary=boundary,
            expected_markets=markets,
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "capture":
        if not args.execute:
            parser.error("CME capture requires explicit --execute")
        publisher = _publisher(
            boundary,
            scope={
                "approval_path": str(args.approval),
                "capture_plan_path": str(args.plan),
            },
        )
        receipt = capture_cme_calendar(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=publisher,
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "parse":
        capture = _receipt_from_manifest(
            args.capture_manifest.resolve(strict=True), boundary=boundary
        )
        if args.mapping_approval is None:
            if args.execute:
                parser.error(
                    "mapping-candidate generation is offline and does not use --execute"
                )
            if args.mapping_candidates_output is None:
                parser.error(
                    "parse without an approval requires --mapping-candidates-output"
                )
            candidates = generate_mapping_candidates(
                capture,
                boundary=boundary,
                expected_markets=markets,
            )
            output = (
                args.mapping_candidates_output
                if args.mapping_candidates_output.is_absolute()
                else boundary.active_root / args.mapping_candidates_output
            )
            boundary.assert_active_path(
                output,
                purpose="CME product mapping candidates",
                subtree="reports",
            )
            _write_canonical(output, candidates)
            print(canonical_bytes(candidates).decode("utf-8"))
            return 0
        if not args.execute:
            parser.error("calendar parsing publication requires explicit --execute")
        predecessor = (
            _receipt_from_manifest(
                args.predecessor_calendar_manifest.resolve(strict=True),
                boundary=boundary,
            )
            if args.predecessor_calendar_manifest is not None
            else None
        )
        mapping = _canonical_object(
            args.mapping_approval.resolve(strict=True),
            description="CME product mapping approval",
        )
        publisher = _publisher(
            boundary,
            scope={
                "capture_release_id": capture.release_id,
                "mapping_approval_receipt_id": str(mapping.get("approval_receipt_id")),
            },
        )
        receipt = publish_verified_exchange_calendar(
            capture_receipt=capture,
            mapping_approval=mapping,
            expected_markets=markets,
            publisher=publisher,
            predecessor_calendar_receipt=predecessor,
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "diff":
        candidate_receipt = _receipt_from_manifest(
            args.candidate_calendar_manifest.resolve(strict=True),
            boundary=boundary,
        )
        candidate = VerifiedExchangeCalendar.from_release(
            candidate_receipt, boundary=boundary, expected_markets=markets
        )
        predecessor = None
        if args.predecessor_calendar_manifest is not None:
            predecessor_receipt = _receipt_from_manifest(
                args.predecessor_calendar_manifest.resolve(strict=True),
                boundary=boundary,
            )
            predecessor = VerifiedExchangeCalendar.from_release(
                predecessor_receipt, boundary=boundary, expected_markets=markets
            )
        payload = diff_exchange_calendars(predecessor, candidate)
        if args.output is not None:
            output = (
                args.output
                if args.output.is_absolute()
                else boundary.active_root / args.output
            )
            boundary.assert_active_path(
                output, purpose="calendar diff report", subtree="reports"
            )
            _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "schedule-recovery-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="CME schedule recovery plan",
            subtree="reports",
        )
        pointer = (
            args.active_pointer
            if args.active_pointer.is_absolute()
            else boundary.active_root / args.active_pointer
        )
        payload = build_schedule_coverage_recovery_plan(
            source_capture_manifest_path=(
                args.source_capture_manifest.resolve(strict=True)
            ),
            mapping_approval_path=(
                args.mapping_approval.resolve(strict=True)
            ),
            candidate_calendar_manifest_path=(
                args.candidate_calendar_manifest.resolve(strict=True)
            ),
            failed_index_manifest_path=(
                args.failed_index_manifest.resolve(strict=True)
            ),
            boundary=boundary,
            expected_markets=markets,
            implementation_sha256=_capture_implementation_hashes(
                boundary.active_root
            ),
            universe_contract_sha256=sha256_file(
                boundary.active_root
                / "configs"
                / "research_universe_contract.json"
            ),
            active_pointer_path=pointer,
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "schedule-recovery-capture":
        if not args.execute:
            parser.error(
                "schedule recovery capture requires explicit --execute"
            )
        receipt = capture_schedule_coverage_recovery(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=_publisher(
                boundary,
                scope={
                    "approval_path": str(args.approval),
                    "schedule_recovery_plan_path": str(args.plan),
                },
            ),
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "activation-plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="calendar activation plan",
            subtree="reports",
        )
        pointer = (
            args.active_pointer
            if args.active_pointer.is_absolute()
            else boundary.active_root / args.active_pointer
        )
        payload = build_calendar_activation_plan(
            candidate_calendar_manifest_path=(
                args.candidate_calendar_manifest.resolve(strict=True)
            ),
            diff_report_path=args.diff_report.resolve(strict=True),
            boundary=boundary,
            expected_markets=markets,
            implementation_sha256=_capture_implementation_hashes(
                boundary.active_root
            ),
            policy_sha256=sha256_file(
                boundary.active_root
                / "configs"
                / "exchange_calendar_policy.json"
            ),
            universe_contract_sha256=sha256_file(
                boundary.active_root
                / "configs"
                / "research_universe_contract.json"
            ),
            predecessor_index_manifest_path=(
                args.predecessor_index_manifest.resolve(strict=True)
                if args.predecessor_index_manifest is not None
                else None
            ),
            active_pointer_path=pointer,
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "activate":
        if not args.execute:
            parser.error("calendar activation requires explicit --execute")
        activation_plan = validate_calendar_activation_plan(
            _canonical_object(
                args.activation_plan.resolve(strict=True),
                description="calendar activation plan",
            ),
            boundary=boundary,
        )
        activation_scope = activation_plan["scope"]
        assert isinstance(activation_scope, dict)
        if (
            activation_scope["implementation_sha256"]
            != _capture_implementation_hashes(boundary.active_root)
            or activation_scope["policy_sha256"]
            != sha256_file(
                boundary.active_root
                / "configs"
                / "exchange_calendar_policy.json"
            )
            or activation_scope["universe_contract_sha256"]
            != sha256_file(
                boundary.active_root
                / "configs"
                / "research_universe_contract.json"
            )
        ):
            raise IntegrityError(
                "calendar activation plan implementation drifted"
            )
        candidate = _receipt_from_manifest(
            args.candidate_calendar_manifest.resolve(strict=True),
            boundary=boundary,
        )
        predecessor = (
            _receipt_from_manifest(
                args.predecessor_index_manifest.resolve(strict=True),
                boundary=boundary,
            )
            if args.predecessor_index_manifest is not None
            else None
        )
        approval = _canonical_object(
            args.activation_approval.resolve(strict=True),
            description="calendar activation approval",
        )
        candidate_relative = (
            args.candidate_calendar_manifest.resolve(strict=True)
            .relative_to(boundary.active_root)
            .as_posix()
        )
        predecessor_relative = (
            args.predecessor_index_manifest.resolve(strict=True)
            .relative_to(boundary.active_root)
            .as_posix()
            if args.predecessor_index_manifest is not None
            else None
        )
        pointer_path = (
            args.active_pointer
            if args.active_pointer.is_absolute()
            else boundary.active_root / args.active_pointer
        )
        if (
            activation_scope["candidate_calendar_manifest_path"]
            != candidate_relative
            or activation_scope["predecessor_index_manifest_path"]
            != predecessor_relative
            or activation_scope["active_pointer_path"]
            != pointer_path.relative_to(boundary.active_root).as_posix()
            or approval.get("candidate_calendar_release_id")
            != activation_scope["candidate_calendar_release_id"]
            or approval.get("predecessor_index_release_id")
            != activation_scope["predecessor_index_release_id"]
            or approval.get("diff_report_id")
            != activation_scope["diff_report_id"]
        ):
            raise IntegrityError(
                "calendar activation execution differs from its plan"
            )
        publisher = _publisher(
            boundary,
            scope={
                "activation_approval_receipt_id": str(
                    approval.get("approval_receipt_id")
                ),
                "candidate_calendar_release_id": candidate.release_id,
            },
        )
        index_receipt = publish_calendar_index(
            candidate_calendar_receipt=candidate,
            activation_approval=approval,
            publisher=publisher,
            expected_markets=markets,
            freshness_at=datetime.now(timezone.utc),
            predecessor_index_receipt=predecessor,
        )
        index = load_calendar_index(
            index_receipt, boundary=boundary, expected_markets=markets
        )
        verify_calendar_freshness(
            index,
            expected_markets=markets,
            now=datetime.now(timezone.utc),
        )
        pointer = active_pointer_payload(
            index_receipt,
            activation_approval_receipt_id=str(
                approval["approval_receipt_id"]
            ),
            activated_at_utc=str(approval["approved_at"]),
        )
        _replace_active_pointer(pointer_path, pointer, boundary=boundary)
        load_active_calendar_index(
            boundary=boundary, expected_markets=markets, path=pointer_path
        )
        print(
            canonical_bytes(
                {
                    "calendar_index_receipt": index.receipt.as_dict(),
                    "pointer": pointer,
                }
            ).decode("utf-8")
        )
        return 0
    if args.command == "verify":
        pointer_path = (
            args.active_pointer
            if args.active_pointer.is_absolute()
            else boundary.active_root / args.active_pointer
        )
        index = load_active_calendar_index(
            boundary=boundary, expected_markets=markets, path=pointer_path
        )
        now = (
            datetime.fromisoformat(args.at_utc.replace("Z", "+00:00"))
            if args.at_utc
            else datetime.now(timezone.utc)
        )
        result = verify_calendar_freshness(
            index, expected_markets=markets, now=now
        )
        print(canonical_bytes(result).decode("utf-8"))
        return 0
    raise AssertionError("unreachable calendar command")


if __name__ == "__main__":
    raise SystemExit(main())
