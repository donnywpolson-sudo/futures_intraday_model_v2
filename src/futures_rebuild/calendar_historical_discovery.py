"""Approval-gated discovery of authoritative CME historical-calendar sources."""

from __future__ import annotations

import html
import json
import re
import time as monotonic_time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .exchange_calendar import load_cme_capture


PLAN_SCHEMA = "cme_historical_archive_source_discovery_plan/1.0.0"
APPROVAL_SCHEMA = "cme_historical_archive_source_discovery_approval/1.0.0"
CAPTURE_SCHEMA = "cme_historical_archive_source_discovery_capture/1.0.0"
OPERATION = "CAPTURE_BOUNDED_PUBLIC_CME_HISTORICAL_ARCHIVE_SOURCE_DISCOVERY"
RELEASE_KIND = "cme_historical_archive_source_discovery_capture"
NOTICES_URL = "https://www.cmegroup.com/notices.html"
TRADING_HOURS_URL = "https://www.cmegroup.com/trading-hours.html"
MAX_REQUESTS = 1
MAX_TOTAL_BYTES = 8_388_608
MAX_DURATION_SECONDS = 30
IMPLEMENTATION_PATHS = (
    "configs/data_layout_contract.json",
    "configs/exchange_calendar_policy.json",
    "configs/source_contract.json",
    "pyproject.toml",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/calendar_cli.py",
    "src/futures_rebuild/calendar_historical_discovery.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/exchange_calendar.py",
    "src/futures_rebuild/source_contract.py",
)
OUTPUT_PATHS = {
    "data_template": (
        "data/reference/exchange_calendars/"
        "{release_id}/001-notices-page.html"
    ),
    "manifest_template": "manifests/data_releases/reference/{release_id}.json",
    "publication_lock": "state/locks/data-publication.lock",
    "staging_root": "state/data_publication_staging",
}
FORBIDDEN_ACTIONS = (
    "ACTIVATE_CALENDAR",
    "CALL_DATABENTO_OR_ANY_NON_CME_PROVIDER",
    "CREATE_LINK_OR_MUTABLE_EXTERNAL_REFERENCE",
    "DELETE_OR_OVERWRITE_ACCEPTED_RELEASE",
    "DOWNLOAD_ARCHIVE_ATTACHMENT_OR_NOTICE_DOCUMENT",
    "FOLLOW_OR_REQUEST_ANY_DISCOVERED_LINK",
    "PARSE_OR_ACCEPT_HISTORICAL_CALENDAR",
    "REBUILD_FOUNDATION",
    "REQUEST_CME_PRODUCT_OR_TRADING_HOURS_SERVICE",
    "RETRY_OR_REDIRECT_REQUEST",
    "USE_CREDENTIAL_OR_SECRET",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "BYTE_OR_DURATION_CEILING_REACHED",
    "CAPTURED_PROBE_OR_LINK_AUTHORITY_DRIFT",
    "CONTENT_TYPE_OR_HTTP_STATUS_MISMATCH",
    "IMPLEMENTATION_HASH_DRIFT",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
_AUTHORITY_KEYS = {
    "discovered_link_url",
    "landing_logical_path",
    "landing_response_sha256",
    "landing_response_size",
    "landing_source_url",
    "probe_capture_id",
    "probe_manifest_path",
    "probe_manifest_sha256",
    "probe_receipt_id",
    "probe_release_id",
    "probe_result_id",
    "probe_result_path",
    "probe_result_sha256",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_SAFE_HEADERS = frozenset(
    {"cache-control", "content-type", "date", "etag", "last-modified"}
)


class HistoricalSourceDiscoveryError(UnauthorizedOperation):
    """Raised before or during the one-request discovery capture."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise HistoricalSourceDiscoveryError(
            "CME historical-source discovery rejected an HTTP redirect"
        )


def _canonical_object(path: Path, *, description: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is not readable JSON") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def implementation_hashes(repository_root: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for relative in IMPLEMENTATION_PATHS:
        path = repository_root / relative
        if not path.is_file():
            raise IntegrityError(
                f"CME historical-source implementation input is missing: {relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _validate_probe_result(
    payload: Mapping[str, object],
    *,
    receipt: DataReleaseReceipt,
    manifest_sha256: str,
) -> str:
    result_id = payload.get("result_id")
    core = {key: value for key, value in payload.items() if key != "result_id"}
    evidence = payload.get("evidence")
    impact = payload.get("foundation_impact")
    if (
        type(result_id) is not str
        or _SHA256.fullmatch(result_id) is None
        or result_id != sha256_json(core)
        or payload.get("schema_version")
        != "cme_historical_schedule_capability_probe_result/1.0.0"
        or payload.get("status") != "PROBE_COMPLETED_NO_HISTORICAL_EVENTS"
        or payload.get("capture_release_id") != receipt.release_id
        or payload.get("capture_manifest_sha256") != manifest_sha256
        or not isinstance(evidence, dict)
        or evidence.get("product_count") != 41
        or evidence.get("schedule_event_count") != 0
        or not isinstance(impact, dict)
        or impact.get("status") != "BLOCKED_MISSING_HISTORICAL_CME_BYTES"
        or payload.get("next_authority")
        != (
            "AUTHORITATIVE_CME_HISTORICAL_SOURCE_OR_HASH_BOUND_"
            "ARCHIVE_DISCOVERY_APPROVAL_REQUIRED"
        )
    ):
        raise IntegrityError(
            "historical schedule capability-probe result is invalid"
        )
    return result_id


def _notices_link_from_landing(source: str) -> str:
    decoded = html.unescape(source)
    references = re.findall(
        r"""\bhref\s*=\s*["']([^"']+)["']""",
        decoded,
        flags=re.IGNORECASE,
    )
    references.extend(
        re.findall(r'''"linkUrl"\s*:\s*"([^"]+)"''', decoded)
    )
    normalized: set[str] = set()
    for value in references:
        candidate = urllib.parse.urljoin(TRADING_HOURS_URL, value)
        parsed = urllib.parse.urlparse(candidate)
        if (
            parsed.scheme == "https"
            and parsed.hostname == "www.cmegroup.com"
            and parsed.username is None
            and parsed.password is None
            and not parsed.query
            and not parsed.fragment
            and parsed.path == "/notices.html"
        ):
            normalized.add(candidate)
    if normalized != {NOTICES_URL}:
        raise IntegrityError(
            "accepted CME landing page does not establish the exact notices link"
        )
    return NOTICES_URL


def historical_source_authority(
    *,
    probe_manifest_path: Path,
    probe_result_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    manifest_path = boundary.assert_active_path(
        probe_manifest_path,
        purpose="historical capability-probe manifest",
        subtree="manifests/data_releases/reference",
    )
    result_path = boundary.assert_active_path(
        probe_result_path,
        purpose="historical capability-probe result",
        subtree="reports/exchange_calendar",
    )
    receipt = DataReleaseReceipt.from_manifest(manifest_path, boundary)
    capture = load_cme_capture(receipt, boundary=boundary)
    manifest_sha256 = sha256_file(manifest_path)
    result = _canonical_object(
        result_path, description="historical capability-probe result"
    )
    result_id = _validate_probe_result(
        result, receipt=receipt, manifest_sha256=manifest_sha256
    )
    responses = capture.get("responses")
    if not isinstance(responses, list):
        raise IntegrityError(
            "historical capability probe response index is invalid"
        )
    landing = [
        item
        for item in responses
        if isinstance(item, dict)
        and item.get("request_kind") == "LANDING_PAGE"
        and item.get("url") == TRADING_HOURS_URL
    ]
    if len(landing) != 1:
        raise IntegrityError(
            "historical capability probe has no unique CME landing response"
        )
    response = landing[0]
    logical_path = response.get("logical_path")
    if (
        type(logical_path) is not str
        or type(response.get("sha256")) is not str
        or _SHA256.fullmatch(str(response["sha256"])) is None
        or type(response.get("size")) is not int
    ):
        raise IntegrityError("historical capability-probe landing response is invalid")
    landing_path = receipt.resolve_file(logical_path, boundary)
    if (
        landing_path.stat().st_size != response["size"]
        or sha256_file(landing_path) != response["sha256"]
    ):
        raise IntegrityError("historical capability-probe landing bytes changed")
    try:
        landing_source = landing_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise IntegrityError(
            "historical capability-probe landing page is not readable UTF-8"
        ) from exc
    discovered = _notices_link_from_landing(landing_source)
    return {
        "discovered_link_url": discovered,
        "landing_logical_path": logical_path,
        "landing_response_sha256": response["sha256"],
        "landing_response_size": response["size"],
        "landing_source_url": TRADING_HOURS_URL,
        "probe_capture_id": capture["capture_id"],
        "probe_manifest_path": manifest_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "probe_manifest_sha256": manifest_sha256,
        "probe_receipt_id": receipt.receipt_id,
        "probe_release_id": receipt.release_id,
        "probe_result_id": result_id,
        "probe_result_path": result_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "probe_result_sha256": sha256_file(result_path),
    }


def _validate_authority_shape(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError("historical-source authority schema is invalid")
    for key in (
        "landing_response_sha256",
        "probe_capture_id",
        "probe_manifest_sha256",
        "probe_receipt_id",
        "probe_release_id",
        "probe_result_id",
        "probe_result_sha256",
    ):
        value = authority.get(key)
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise ContractError("historical-source authority hash is invalid")
    if (
        authority.get("discovered_link_url") != NOTICES_URL
        or authority.get("landing_source_url") != TRADING_HOURS_URL
        or type(authority.get("landing_logical_path")) is not str
        or type(authority.get("probe_manifest_path")) is not str
        or type(authority.get("probe_result_path")) is not str
        or type(authority.get("landing_response_size")) is not int
        or int(authority["landing_response_size"]) <= 0
    ):
        raise ContractError("historical-source authority value is invalid")


def build_historical_source_discovery_plan(
    *,
    authority: Mapping[str, object],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    _validate_authority_shape(authority)
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
    ):
        raise ContractError(
            "historical-source discovery implementation hashes are invalid"
        )
    scope: dict[str, object] = {
        "allow_redirects": False,
        "authority": dict(authority),
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "max_duration_seconds": MAX_DURATION_SECONDS,
        "max_requests": MAX_REQUESTS,
        "max_total_bytes": MAX_TOTAL_BYTES,
        "output_paths": dict(OUTPUT_PATHS),
        "purpose": (
            "DISCOVER_CME_HOSTED_HISTORICAL_TRADING_HOURS_"
            "ARCHIVE_REFERENCES_ONLY"
        ),
        "request": {
            "accept": "text/html",
            "request_id": "notices-page",
            "request_kind": "HISTORICAL_ARCHIVE_SOURCE_DISCOVERY",
            "url": NOTICES_URL,
        },
        "required_coverage_end_trade_date": "2026-07-13",
        "required_coverage_start_trade_date": "2010-06-06",
        "retries": 0,
        "stop_conditions": list(STOP_CONDITIONS),
        "workers": 1,
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": OPERATION,
        "schema_version": PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_historical_source_discovery_plan(
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
        raise IntegrityError("historical-source discovery plan schema is invalid")
    core = {key: value for key, value in payload.items() if key != "plan_id"}
    scope = payload.get("scope")
    if (
        payload.get("schema_version") != PLAN_SCHEMA
        or payload.get("classification") != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload.get("execution_authorized") is not False
        or payload.get("operation") != OPERATION
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
    ):
        raise IntegrityError("historical-source discovery plan identity is invalid")
    authority = scope.get("authority")
    implementation = scope.get("implementation_sha256")
    if not isinstance(authority, dict) or not isinstance(implementation, dict):
        raise IntegrityError("historical-source discovery scope is invalid")
    expected = build_historical_source_discovery_plan(
        authority=authority,
        implementation_sha256=implementation,
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "historical-source discovery plan differs from bounded implementation"
        )
    return dict(payload)


def validate_historical_source_discovery_approval(
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
        or approval.get("schema_version") != APPROVAL_SCHEMA
        or approval.get("operation") != OPERATION
        or approval.get("status") != "APPROVED"
        or approval.get("plan_id") != plan["plan_id"]
        or approval.get("plan_sha256") != plan_sha256
        or approval.get("approval_receipt_id") != sha256_json(core)
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"])) is None
    ):
        raise HistoricalSourceDiscoveryError(
            "historical-source discovery lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _safe_url(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    if (
        url != NOTICES_URL
        or parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != "/notices.html"
        or parsed.query
        or parsed.fragment
    ):
        raise HistoricalSourceDiscoveryError(
            "historical-source discovery URL is outside the exact allowlist"
        )


def capture_historical_source_discovery(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_historical_source_discovery_plan(
        _canonical_object(plan_path, description="historical-source discovery plan")
    )
    approval = _canonical_object(
        approval_path, description="historical-source discovery approval"
    )
    approval_id = validate_historical_source_discovery_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != implementation_hashes(
        publisher.boundary.active_root
    ):
        raise HistoricalSourceDiscoveryError(
            "historical-source discovery implementation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived_authority = historical_source_authority(
        probe_manifest_path=publisher.boundary.active_root
        / str(authority["probe_manifest_path"]),
        probe_result_path=publisher.boundary.active_root
        / str(authority["probe_result_path"]),
        boundary=publisher.boundary,
    )
    if authority != derived_authority:
        raise HistoricalSourceDiscoveryError(
            "historical-source discovery authority changed"
        )
    request_spec = scope["request"]
    assert isinstance(request_spec, dict)
    url = str(request_spec["url"])
    _safe_url(url)
    stage = publisher.create_stage("cme_historical_source_discovery")
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    request = urllib.request.Request(
        url,
        headers={
            "Accept": str(request_spec["accept"]),
            "User-Agent": "futures-intraday-model-v2-calendar/1.0",
        },
        method="GET",
    )
    try:
        with opener.open(request, timeout=MAX_DURATION_SECONDS) as response:
            if response.status != 200 or response.geturl() != url:
                raise HistoricalSourceDiscoveryError(
                    "historical-source discovery response is not exact HTTP 200"
                )
            content_type = response.headers.get_content_type()
            if content_type != "text/html":
                raise HistoricalSourceDiscoveryError(
                    "historical-source discovery content type is unexpected"
                )
            body = response.read(MAX_TOTAL_BYTES + 1)
            if len(body) > MAX_TOTAL_BYTES:
                raise HistoricalSourceDiscoveryError(
                    "historical-source discovery byte ceiling is exceeded"
                )
            safe_headers = {
                key.lower(): value
                for key, value in response.headers.items()
                if key.lower() in _SAFE_HEADERS
            }
    except HistoricalSourceDiscoveryError:
        raise
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise HistoricalSourceDiscoveryError(
            "historical-source discovery request failed before publication"
        ) from exc
    elapsed_milliseconds = int(
        (monotonic_time.monotonic() - started) * 1000
    )
    if elapsed_milliseconds > MAX_DURATION_SECONDS * 1000:
        raise HistoricalSourceDiscoveryError(
            "historical-source discovery duration ceiling is exceeded"
        )
    staged_name = "001-notices-page.html"
    staged = stage / staged_name
    staged.write_bytes(body)
    logical_path = (
        "data/reference/exchange_calendars/001-notices-page.html"
    )
    response_record = {
        "content_type": "text/html",
        "logical_path": logical_path,
        "received_at_utc": captured_at.isoformat().replace("+00:00", "Z"),
        "request_id": "notices-page",
        "request_kind": "HISTORICAL_ARCHIVE_SOURCE_DISCOVERY",
        "safe_headers": dict(sorted(safe_headers.items())),
        "sha256": sha256_file(staged),
        "size": len(body),
        "status_code": 200,
        "url": url,
    }
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "authority": authority,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": MAX_DURATION_SECONDS,
            "max_requests": MAX_REQUESTS,
            "max_total_bytes": MAX_TOTAL_BYTES,
            "retries": 0,
            "workers": 1,
        },
        "capture_approval": dict(approval),
        "captured_at_utc": captured_at.isoformat().replace("+00:00", "Z"),
        "elapsed_milliseconds": elapsed_milliseconds,
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "request_count": 1,
        "response": response_record,
        "schema_version": CAPTURE_SCHEMA,
        "total_bytes": len(body),
    }
    capture_receipt = {**core, "capture_id": sha256_json(core)}
    manifest = DataReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=RELEASE_KIND,
        schema_version=CAPTURE_SCHEMA,
        logical_paths={staged_name: logical_path},
        source_release_ids=(str(authority["probe_release_id"]),),
        embedded_documents={"capture_receipt.json": capture_receipt},
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture_receipt["capture_id"],
            "captured_at_utc": core["captured_at_utc"],
            "plan_id": plan["plan_id"],
            "probe_result_id": authority["probe_result_id"],
        },
    )
    manifest_path = publisher.publish(
        stage,
        manifest,
        staged_paths={logical_path: staged_name},
    )
    receipt = DataReleaseReceipt.from_manifest(manifest_path, publisher.boundary)
    load_historical_source_discovery_capture(
        receipt, boundary=publisher.boundary
    )
    return receipt


def load_historical_source_discovery_capture(
    receipt: DataReleaseReceipt,
    *,
    boundary: RepoBoundary,
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "reference"
        or manifest.release_kind != RELEASE_KIND
        or manifest.schema_version != CAPTURE_SCHEMA
        or len(manifest.files) != 1
        or set(manifest.embedded_documents) != {"capture_receipt.json"}
        or set(manifest.metadata)
        != {
            "approval_receipt_id",
            "capture_id",
            "captured_at_utc",
            "plan_id",
            "probe_result_id",
        }
    ):
        raise IntegrityError(
            "historical-source discovery release contract is invalid"
        )
    raw = receipt.embedded_document("capture_receipt.json", boundary)
    if not isinstance(raw, dict):
        raise IntegrityError(
            "historical-source discovery capture receipt is invalid"
        )
    payload = dict(raw)
    expected = {
        "approval_receipt_id",
        "authority",
        "bounds",
        "capture_approval",
        "capture_id",
        "captured_at_utc",
        "elapsed_milliseconds",
        "operation",
        "plan_id",
        "request_count",
        "response",
        "schema_version",
        "total_bytes",
    }
    capture_id = payload.pop("capture_id", None)
    authority = payload.get("authority")
    response = payload.get("response")
    if (
        set(raw) != expected
        or type(capture_id) is not str
        or capture_id != sha256_json(payload)
        or payload.get("schema_version") != CAPTURE_SCHEMA
        or payload.get("operation") != OPERATION
        or payload.get("request_count") != 1
        or not isinstance(authority, dict)
        or not isinstance(response, dict)
        or response.get("url") != NOTICES_URL
        or response.get("request_kind")
        != "HISTORICAL_ARCHIVE_SOURCE_DISCOVERY"
        or response.get("content_type") != "text/html"
        or response.get("status_code") != 200
        or manifest.source_release_ids
        != (str(authority.get("probe_release_id")),)
        or manifest.metadata.get("capture_id") != capture_id
        or manifest.metadata.get("approval_receipt_id")
        != payload.get("approval_receipt_id")
    ):
        raise IntegrityError(
            "historical-source discovery capture contract is invalid"
        )
    logical_path = response.get("logical_path")
    if type(logical_path) is not str:
        raise IntegrityError(
            "historical-source discovery response path is invalid"
        )
    physical = receipt.resolve_file(logical_path, boundary)
    if (
        physical.stat().st_size != response.get("size")
        or sha256_file(physical) != response.get("sha256")
    ):
        raise IntegrityError(
            "historical-source discovery response bytes changed"
        )
    return dict(raw)
