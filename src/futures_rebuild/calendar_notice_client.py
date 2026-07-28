"""Approval-gated capture of CME Notices search client assets."""

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
from .calendar_historical_archive import (
    load_historical_archive_landing_capture,
)
from .calendar_historical_discovery import (
    load_historical_source_discovery_capture,
)
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import DataReleaseManifest, DataReleaseReceipt, PhasePublisher
from .errors import ContractError, IntegrityError, UnauthorizedOperation


PLAN_SCHEMA = "cme_notice_search_client_contract_plan/1.0.0"
APPROVAL_SCHEMA = "cme_notice_search_client_contract_approval/1.0.0"
CAPTURE_SCHEMA = "cme_notice_search_client_contract_capture/1.0.0"
OPERATION = "CAPTURE_BOUNDED_PUBLIC_CME_NOTICE_SEARCH_CLIENT_CONTRACT"
RELEASE_KIND = "cme_notice_search_client_contract_capture"
NOTICES_URL = "https://www.cmegroup.com/notices.html"
CLIENT_ASSETS = (
    {
        "request_id": "dynamic-alert-list",
        "url": (
            "https://www.cmegroup.com/etc.clientlibs/cmegroupaem/clientlibs/"
            "dynamic-alert-list.c1ba229f7ecc4ec3703f66aaafe1f3cc.js"
        ),
    },
    {
        "request_id": "search-sort-filter-dynamic",
        "url": (
            "https://www.cmegroup.com/etc.clientlibs/cmegroupaem/clientlibs/"
            "search-sort-filter-dynamic.5f3753aa099ec25dcf789f94113b3f4e.js"
        ),
    },
)
MAX_REQUESTS = 2
MAX_TOTAL_BYTES = 16_777_216
MAX_DURATION_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 30
IMPLEMENTATION_PATHS = (
    "configs/data_layout_contract.json",
    "configs/exchange_calendar_policy.json",
    "configs/source_contract.json",
    "pyproject.toml",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/calendar_cli.py",
    "src/futures_rebuild/calendar_historical_archive.py",
    "src/futures_rebuild/calendar_historical_discovery.py",
    "src/futures_rebuild/calendar_notice_client.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/exchange_calendar.py",
    "src/futures_rebuild/source_contract.py",
)
OUTPUT_PATHS = {
    "data_template": (
        "data/reference/exchange_calendars/{release_id}/"
        "{ordinal:03d}-{request_id}.js"
    ),
    "manifest_template": "manifests/data_releases/reference/{release_id}.json",
    "publication_lock": "state/locks/data-publication.lock",
    "staging_root": "state/data_publication_staging",
}
FORBIDDEN_ACTIONS = (
    "ACTIVATE_CALENDAR",
    "CALL_CME_NOTICE_SEARCH_OR_ANY_OTHER_ENDPOINT",
    "CALL_DATABENTO_OR_ANY_NON_CME_PROVIDER",
    "CREATE_LINK_OR_MUTABLE_EXTERNAL_REFERENCE",
    "DELETE_OR_OVERWRITE_ACCEPTED_RELEASE",
    "DOWNLOAD_ARCHIVE_ATTACHMENT_OR_NOTICE_DOCUMENT",
    "EVALUATE_OR_EXECUTE_CAPTURED_JAVASCRIPT",
    "PARSE_OR_ACCEPT_HISTORICAL_CALENDAR",
    "REBUILD_FOUNDATION",
    "RETRY_OR_REDIRECT_REQUEST",
    "USE_CREDENTIAL_OR_SECRET",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "ASSET_REFERENCE_OR_SOURCE_EVIDENCE_DRIFT",
    "BYTE_OR_DURATION_CEILING_REACHED",
    "CONTENT_TYPE_OR_HTTP_STATUS_MISMATCH",
    "IMPLEMENTATION_HASH_DRIFT",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
_AUTHORITY_KEYS = {
    "archive_capture_id",
    "archive_manifest_path",
    "archive_manifest_sha256",
    "archive_receipt_id",
    "archive_release_id",
    "archive_response_sha256",
    "assessment_id",
    "assessment_path",
    "assessment_sha256",
    "client_assets",
    "notices_capture_id",
    "notices_manifest_path",
    "notices_manifest_sha256",
    "notices_receipt_id",
    "notices_release_id",
    "notices_response_logical_path",
    "notices_response_sha256",
}
_ASSESSMENT_SCHEMA = "cme_historical_advisory_archive_assessment/1.0.0"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_CLIENT_PATTERN = re.compile(
    r"""(?:https://www\.cmegroup\.com)?"""
    r"""(/etc\.clientlibs/cmegroupaem/clientlibs/"""
    r"""(?:dynamic-alert-list|search-sort-filter-dynamic)"""
    r"""\.[0-9a-f]{32}\.js)"""
)
_SAFE_HEADERS = frozenset(
    {"cache-control", "content-type", "date", "etag", "last-modified"}
)


class NoticeClientCaptureError(UnauthorizedOperation):
    """Raised before or during the bounded client-asset capture."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise NoticeClientCaptureError(
            "CME Notices client capture rejected an HTTP redirect"
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
                f"CME Notices client implementation input is missing: {relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _response(
    capture: Mapping[str, object], *, description: str
) -> dict[str, object]:
    response = capture.get("response")
    if not isinstance(response, dict):
        raise IntegrityError(f"{description} response is invalid")
    return response


def _assets_from_notices(source: str) -> list[dict[str, str]]:
    decoded = html.unescape(source)
    urls = {
        urllib.parse.urljoin(NOTICES_URL, match)
        for match in _CLIENT_PATTERN.findall(decoded)
    }
    expected = {str(item["url"]) for item in CLIENT_ASSETS}
    if urls != expected:
        raise IntegrityError(
            "accepted CME Notices page does not establish the exact client assets"
        )
    return [dict(item) for item in CLIENT_ASSETS]


def _validate_assessment(
    payload: Mapping[str, object],
    *,
    notices_receipt: DataReleaseReceipt,
    notices_capture: Mapping[str, object],
    notices_manifest_sha256: str,
    archive_receipt: DataReleaseReceipt,
    archive_capture: Mapping[str, object],
    archive_manifest_sha256: str,
    assets: list[dict[str, str]],
) -> str:
    assessment_id = payload.get("assessment_id")
    core = {key: value for key, value in payload.items() if key != "assessment_id"}
    notices_response = _response(
        notices_capture, description="CME Notices discovery capture"
    )
    archive_response = _response(
        archive_capture, description="CME archive-landing capture"
    )
    if (
        type(assessment_id) is not str
        or _SHA256.fullmatch(assessment_id) is None
        or assessment_id != sha256_json(core)
        or payload.get("schema_version") != _ASSESSMENT_SCHEMA
        or payload.get("status")
        != "REQUIRED_RANGE_MUST_USE_CURRENT_CME_NOTICES_SEARCH_CONTRACT"
        or payload.get("classification")
        != "STATIC_ARCHIVE_OUTSIDE_REQUIRED_RANGE_CURRENT_NOTICE_CLIENT_DISCOVERED"
        or payload.get("next_authority")
        != "HASH_BOUND_CME_NOTICE_SEARCH_CLIENT_CONTRACT_CAPTURE_APPROVAL_REQUIRED"
        or payload.get("required_coverage_start_trade_date") != "2010-06-06"
        or payload.get("required_coverage_end_trade_date") != "2026-07-13"
        or payload.get("static_archive_latest_year") != 2008
        or type(payload.get("static_archive_zip_count")) is not int
        or payload.get("notices_release_id") != notices_receipt.release_id
        or payload.get("notices_response_sha256")
        != notices_response.get("sha256")
        or payload.get("archive_release_id") != archive_receipt.release_id
        or payload.get("archive_manifest_sha256") != archive_manifest_sha256
        or payload.get("archive_receipt_id") != archive_receipt.receipt_id
        or payload.get("archive_capture_id") != archive_capture.get("capture_id")
        or payload.get("archive_response_sha256")
        != archive_response.get("sha256")
        or payload.get("current_notice_client_assets") != assets
        or notices_manifest_sha256 != notices_receipt.manifest_sha256
    ):
        raise IntegrityError(
            "CME historical advisory archive assessment is invalid"
        )
    return assessment_id


def notice_client_authority(
    *,
    notices_manifest_path: Path,
    archive_manifest_path: Path,
    assessment_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    notices_path = boundary.assert_active_path(
        notices_manifest_path,
        purpose="CME Notices discovery manifest",
        subtree="manifests/data_releases/reference",
    )
    archive_path = boundary.assert_active_path(
        archive_manifest_path,
        purpose="CME archive-landing manifest",
        subtree="manifests/data_releases/reference",
    )
    report_path = boundary.assert_active_path(
        assessment_path,
        purpose="CME advisory archive assessment",
        subtree="reports/exchange_calendar",
    )
    notices_receipt = DataReleaseReceipt.from_manifest(notices_path, boundary)
    archive_receipt = DataReleaseReceipt.from_manifest(archive_path, boundary)
    archive_manifest = archive_receipt.verify(boundary)
    notices_capture = load_historical_source_discovery_capture(
        notices_receipt, boundary=boundary
    )
    archive_capture = load_historical_archive_landing_capture(
        archive_receipt, boundary=boundary
    )
    notices_response = _response(
        notices_capture, description="CME Notices discovery capture"
    )
    archive_response = _response(
        archive_capture, description="CME archive-landing capture"
    )
    logical_path = notices_response.get("logical_path")
    if (
        type(logical_path) is not str
        or notices_response.get("url") != NOTICES_URL
        or archive_manifest.source_release_ids != (notices_receipt.release_id,)
    ):
        raise IntegrityError("CME Notices/archive release lineage is invalid")
    notices_file = notices_receipt.resolve_file(logical_path, boundary)
    try:
        notices_source = notices_file.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise IntegrityError("CME Notices page is not readable UTF-8") from exc
    assets = _assets_from_notices(notices_source)
    assessment = _canonical_object(
        report_path, description="CME advisory archive assessment"
    )
    notices_manifest_sha256 = sha256_file(notices_path)
    archive_manifest_sha256 = sha256_file(archive_path)
    assessment_id = _validate_assessment(
        assessment,
        notices_receipt=notices_receipt,
        notices_capture=notices_capture,
        notices_manifest_sha256=notices_manifest_sha256,
        archive_receipt=archive_receipt,
        archive_capture=archive_capture,
        archive_manifest_sha256=archive_manifest_sha256,
        assets=assets,
    )
    return {
        "archive_capture_id": archive_capture["capture_id"],
        "archive_manifest_path": archive_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "archive_manifest_sha256": archive_manifest_sha256,
        "archive_receipt_id": archive_receipt.receipt_id,
        "archive_release_id": archive_receipt.release_id,
        "archive_response_sha256": archive_response["sha256"],
        "assessment_id": assessment_id,
        "assessment_path": report_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "assessment_sha256": sha256_file(report_path),
        "client_assets": assets,
        "notices_capture_id": notices_capture["capture_id"],
        "notices_manifest_path": notices_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "notices_manifest_sha256": notices_manifest_sha256,
        "notices_receipt_id": notices_receipt.receipt_id,
        "notices_release_id": notices_receipt.release_id,
        "notices_response_logical_path": logical_path,
        "notices_response_sha256": notices_response["sha256"],
    }


def _validate_authority_shape(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError("CME Notices client authority schema is invalid")
    for key in (
        "archive_capture_id",
        "archive_manifest_sha256",
        "archive_receipt_id",
        "archive_release_id",
        "archive_response_sha256",
        "assessment_id",
        "assessment_sha256",
        "notices_capture_id",
        "notices_manifest_sha256",
        "notices_receipt_id",
        "notices_release_id",
        "notices_response_sha256",
    ):
        value = authority.get(key)
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise ContractError("CME Notices client authority hash is invalid")
    if (
        authority.get("client_assets")
        != [dict(item) for item in CLIENT_ASSETS]
        or any(
            type(authority.get(key)) is not str
            for key in (
                "archive_manifest_path",
                "assessment_path",
                "notices_manifest_path",
                "notices_response_logical_path",
            )
        )
    ):
        raise ContractError("CME Notices client authority value is invalid")


def build_notice_client_plan(
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
            "CME Notices client implementation hashes are invalid"
        )
    requests = [
        {
            "accept": "application/javascript,text/javascript;q=0.9",
            "request_id": item["request_id"],
            "request_kind": "NOTICE_SEARCH_CLIENT_CONTRACT",
            "url": item["url"],
        }
        for item in CLIENT_ASSETS
    ]
    scope: dict[str, object] = {
        "allow_redirects": False,
        "authority": dict(authority),
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "max_duration_seconds": MAX_DURATION_SECONDS,
        "max_requests": MAX_REQUESTS,
        "max_total_bytes": MAX_TOTAL_BYTES,
        "output_paths": dict(OUTPUT_PATHS),
        "purpose": "DISCOVER_CME_NOTICE_SEARCH_CLIENT_CONTRACT_ONLY",
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "requests": requests,
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


def validate_notice_client_plan(
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
        raise IntegrityError("CME Notices client plan schema is invalid")
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
        raise IntegrityError("CME Notices client plan identity is invalid")
    authority = scope.get("authority")
    implementation = scope.get("implementation_sha256")
    if not isinstance(authority, dict) or not isinstance(implementation, dict):
        raise IntegrityError("CME Notices client plan scope is invalid")
    expected = build_notice_client_plan(
        authority=authority,
        implementation_sha256=implementation,
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME Notices client plan differs from bounded implementation"
        )
    return dict(payload)


def validate_notice_client_approval(
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
        raise NoticeClientCaptureError(
            "CME Notices client capture lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _safe_url(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    expected = {str(item["url"]) for item in CLIENT_ASSETS}
    if (
        url not in expected
        or parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise NoticeClientCaptureError(
            "CME Notices client URL is outside the exact allowlist"
        )


def capture_notice_client_contract(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_notice_client_plan(
        _canonical_object(plan_path, description="CME Notices client plan")
    )
    approval = _canonical_object(
        approval_path, description="CME Notices client approval"
    )
    approval_id = validate_notice_client_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != implementation_hashes(
        publisher.boundary.active_root
    ):
        raise NoticeClientCaptureError(
            "CME Notices client implementation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived_authority = notice_client_authority(
        notices_manifest_path=publisher.boundary.active_root
        / str(authority["notices_manifest_path"]),
        archive_manifest_path=publisher.boundary.active_root
        / str(authority["archive_manifest_path"]),
        assessment_path=publisher.boundary.active_root
        / str(authority["assessment_path"]),
        boundary=publisher.boundary,
    )
    if authority != derived_authority:
        raise NoticeClientCaptureError("CME Notices client authority changed")
    request_specs = scope["requests"]
    assert isinstance(request_specs, list)
    if len(request_specs) != MAX_REQUESTS:
        raise NoticeClientCaptureError(
            "CME Notices client request ceiling is invalid"
        )
    for request_spec in request_specs:
        assert isinstance(request_spec, dict)
        _safe_url(str(request_spec["url"]))
    stage = publisher.create_stage("cme_notice_search_client")
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    responses: list[dict[str, object]] = []
    staged_paths: dict[str, str] = {}
    logical_paths: dict[str, str] = {}
    total_bytes = 0
    for ordinal, request_spec in enumerate(request_specs, start=1):
        request_id = str(request_spec["request_id"])
        url = str(request_spec["url"])
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/javascript,text/javascript;q=0.9",
                "User-Agent": "futures-intraday-model-v2-calendar/1.0",
            },
            method="GET",
        )
        try:
            with opener.open(
                request, timeout=REQUEST_TIMEOUT_SECONDS
            ) as response:
                if response.status != 200 or response.geturl() != url:
                    raise NoticeClientCaptureError(
                        "CME Notices client response is not exact HTTP 200"
                    )
                content_type = response.headers.get_content_type()
                if content_type not in {
                    "application/javascript",
                    "text/javascript",
                }:
                    raise NoticeClientCaptureError(
                        "CME Notices client response content type is unexpected"
                    )
                remaining = MAX_TOTAL_BYTES - total_bytes
                body = response.read(remaining + 1)
                if len(body) > remaining:
                    raise NoticeClientCaptureError(
                        "CME Notices client byte ceiling is exceeded"
                    )
                safe_headers = {
                    key.lower(): value
                    for key, value in response.headers.items()
                    if key.lower() in _SAFE_HEADERS
                }
        except NoticeClientCaptureError:
            raise
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
        ) as exc:
            raise NoticeClientCaptureError(
                "CME Notices client request failed before publication"
            ) from exc
        total_bytes += len(body)
        staged_name = f"{ordinal:03d}-{request_id}.js"
        staged = stage / staged_name
        staged.write_bytes(body)
        logical_path = (
            f"data/reference/exchange_calendars/{ordinal:03d}-"
            f"{request_id}.js"
        )
        logical_paths[staged_name] = logical_path
        staged_paths[logical_path] = staged_name
        responses.append(
            {
                "content_type": content_type,
                "logical_path": logical_path,
                "received_at_utc": captured_at.isoformat().replace(
                    "+00:00", "Z"
                ),
                "request_id": request_id,
                "request_kind": "NOTICE_SEARCH_CLIENT_CONTRACT",
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
    if elapsed_milliseconds > MAX_DURATION_SECONDS * 1000:
        raise NoticeClientCaptureError(
            "CME Notices client duration ceiling is exceeded"
        )
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "authority": authority,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": MAX_DURATION_SECONDS,
            "max_requests": MAX_REQUESTS,
            "max_total_bytes": MAX_TOTAL_BYTES,
            "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "retries": 0,
            "workers": 1,
        },
        "capture_approval": dict(approval),
        "captured_at_utc": captured_at.isoformat().replace("+00:00", "Z"),
        "elapsed_milliseconds": elapsed_milliseconds,
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "request_count": MAX_REQUESTS,
        "responses": responses,
        "schema_version": CAPTURE_SCHEMA,
        "total_bytes": total_bytes,
    }
    capture_receipt = {**core, "capture_id": sha256_json(core)}
    manifest = DataReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=RELEASE_KIND,
        schema_version=CAPTURE_SCHEMA,
        logical_paths=logical_paths,
        source_release_ids=(
            str(authority["notices_release_id"]),
            str(authority["archive_release_id"]),
        ),
        embedded_documents={"capture_receipt.json": capture_receipt},
        metadata={
            "approval_receipt_id": approval_id,
            "assessment_id": authority["assessment_id"],
            "capture_id": capture_receipt["capture_id"],
            "captured_at_utc": core["captured_at_utc"],
            "plan_id": plan["plan_id"],
        },
    )
    manifest_path = publisher.publish(
        stage, manifest, staged_paths=staged_paths
    )
    receipt = DataReleaseReceipt.from_manifest(
        manifest_path, publisher.boundary
    )
    load_notice_client_capture(receipt, boundary=publisher.boundary)
    return receipt


def load_notice_client_capture(
    receipt: DataReleaseReceipt,
    *,
    boundary: RepoBoundary,
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "reference"
        or manifest.release_kind != RELEASE_KIND
        or manifest.schema_version != CAPTURE_SCHEMA
        or len(manifest.files) != MAX_REQUESTS
        or set(manifest.embedded_documents) != {"capture_receipt.json"}
        or set(manifest.metadata)
        != {
            "approval_receipt_id",
            "assessment_id",
            "capture_id",
            "captured_at_utc",
            "plan_id",
        }
    ):
        raise IntegrityError("CME Notices client release contract is invalid")
    raw = receipt.embedded_document("capture_receipt.json", boundary)
    if not isinstance(raw, dict):
        raise IntegrityError("CME Notices client capture receipt is invalid")
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
        "responses",
        "schema_version",
        "total_bytes",
    }
    capture_id = payload.pop("capture_id", None)
    authority = payload.get("authority")
    responses = payload.get("responses")
    if (
        set(raw) != expected
        or type(capture_id) is not str
        or capture_id != sha256_json(payload)
        or payload.get("schema_version") != CAPTURE_SCHEMA
        or payload.get("operation") != OPERATION
        or payload.get("request_count") != MAX_REQUESTS
        or not isinstance(authority, dict)
        or not isinstance(responses, list)
        or len(responses) != MAX_REQUESTS
        or manifest.source_release_ids
        != tuple(
            sorted(
                (
                    str(authority.get("notices_release_id")),
                    str(authority.get("archive_release_id")),
                )
            )
        )
        or manifest.metadata.get("capture_id") != capture_id
        or manifest.metadata.get("approval_receipt_id")
        != payload.get("approval_receipt_id")
    ):
        raise IntegrityError("CME Notices client capture contract is invalid")
    expected_assets = list(CLIENT_ASSETS)
    total_bytes = 0
    for response, asset in zip(responses, expected_assets, strict=True):
        if (
            not isinstance(response, dict)
            or response.get("url") != asset["url"]
            or response.get("request_id") != asset["request_id"]
            or response.get("request_kind")
            != "NOTICE_SEARCH_CLIENT_CONTRACT"
            or response.get("content_type")
            not in {"application/javascript", "text/javascript"}
            or response.get("status_code") != 200
            or type(response.get("logical_path")) is not str
        ):
            raise IntegrityError(
                "CME Notices client response contract is invalid"
            )
        physical = receipt.resolve_file(str(response["logical_path"]), boundary)
        if (
            physical.stat().st_size != response.get("size")
            or sha256_file(physical) != response.get("sha256")
        ):
            raise IntegrityError("CME Notices client response bytes changed")
        total_bytes += physical.stat().st_size
    if total_bytes != payload.get("total_bytes"):
        raise IntegrityError("CME Notices client total byte count is invalid")
    return dict(raw)
