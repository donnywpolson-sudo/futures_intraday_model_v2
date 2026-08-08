"""Approval-gated capture of CME's historical advisory-archive landing page."""

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
from .calendar_historical_discovery import (
    load_historical_source_discovery_capture,
)
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation


PLAN_SCHEMA = "cme_historical_advisory_archive_landing_plan/1.0.0"
APPROVAL_SCHEMA = "cme_historical_advisory_archive_landing_approval/1.0.0"
CAPTURE_SCHEMA = "cme_historical_advisory_archive_landing_capture/1.0.0"
OPERATION = "CAPTURE_BOUNDED_PUBLIC_CME_HISTORICAL_ADVISORY_ARCHIVE_LANDING"
RELEASE_KIND = "cme_historical_advisory_archive_landing_capture"
ARCHIVE_URL = "https://www.cmegroup.com/tools-information/advisory-archive.html"
NOTICES_URL = "https://www.cmegroup.com/notices.html"
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
    "src/futures_rebuild/calendar_historical_archive.py",
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
        "{release_id}/001-advisory-archive.html"
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
    "FOLLOW_OR_REQUEST_ANY_ARCHIVE_ENTRY_OR_DISCOVERED_LINK",
    "PARSE_OR_ACCEPT_HISTORICAL_CALENDAR",
    "REBUILD_FOUNDATION",
    "REQUEST_CME_PRODUCT_OR_TRADING_HOURS_SERVICE",
    "RETRY_OR_REDIRECT_REQUEST",
    "USE_CREDENTIAL_OR_SECRET",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "ARCHIVE_LINK_OR_SOURCE_CAPTURE_DRIFT",
    "BYTE_OR_DURATION_CEILING_REACHED",
    "CONTENT_TYPE_OR_HTTP_STATUS_MISMATCH",
    "IMPLEMENTATION_HASH_DRIFT",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
_AUTHORITY_KEYS = {
    "archive_url",
    "candidate_result_id",
    "candidate_result_path",
    "candidate_result_sha256",
    "notices_capture_id",
    "notices_manifest_path",
    "notices_manifest_sha256",
    "notices_receipt_id",
    "notices_release_id",
    "notices_response_logical_path",
    "notices_response_sha256",
    "notices_source_url",
    "probe_result_id",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_SAFE_HEADERS = frozenset(
    {"cache-control", "content-type", "date", "etag", "last-modified"}
)


class HistoricalArchiveCaptureError(UnauthorizedOperation):
    """Raised before or during the one-request archive-landing capture."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise HistoricalArchiveCaptureError(
            "CME advisory-archive capture rejected an HTTP redirect"
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
                f"CME archive-landing implementation input is missing: {relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _archive_link_from_notices(source: str) -> str:
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
        candidate = urllib.parse.urljoin(NOTICES_URL, value)
        parsed = urllib.parse.urlparse(candidate)
        if (
            parsed.scheme == "https"
            and parsed.hostname == "www.cmegroup.com"
            and parsed.username is None
            and parsed.password is None
            and not parsed.query
            and not parsed.fragment
            and parsed.path == "/tools-information/advisory-archive.html"
        ):
            normalized.add(candidate)
    if normalized != {ARCHIVE_URL}:
        raise IntegrityError(
            "accepted CME notices page does not establish the exact archive link"
        )
    return ARCHIVE_URL


def _validate_candidates(
    payload: Mapping[str, object],
    *,
    receipt: DataReleaseReceipt,
    manifest_sha256: str,
    capture: Mapping[str, object],
) -> str:
    result_id = payload.get("result_id")
    core = {key: value for key, value in payload.items() if key != "result_id"}
    candidates = payload.get("discovered_candidates")
    if (
        type(result_id) is not str
        or _SHA256.fullmatch(result_id) is None
        or result_id != sha256_json(core)
        or payload.get("schema_version")
        != "cme_historical_archive_source_candidates/1.0.0"
        or payload.get("status") != "ONE_EXACT_ARCHIVE_LANDING_CANDIDATE"
        or payload.get("capture_release_id") != receipt.release_id
        or payload.get("capture_manifest_sha256") != manifest_sha256
        or payload.get("capture_receipt_id") != receipt.receipt_id
        or payload.get("capture_id") != capture.get("capture_id")
        or payload.get("next_authority")
        != "HASH_BOUND_CME_ADVISORY_ARCHIVE_LANDING_CAPTURE_APPROVAL_REQUIRED"
        or candidates
        != [
            {
                "evidence_kind": "ANCHOR_HREF",
                "role": "HISTORICAL_ADVISORY_ARCHIVE_LANDING",
                "url": ARCHIVE_URL,
            }
        ]
    ):
        raise IntegrityError("CME historical-source candidate result is invalid")
    return result_id


def historical_archive_authority(
    *,
    notices_manifest_path: Path,
    candidate_result_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    manifest_path = boundary.assert_active_path(
        notices_manifest_path,
        purpose="CME notices discovery manifest",
        subtree="manifests/data_releases/reference",
    )
    result_path = boundary.assert_active_path(
        candidate_result_path,
        purpose="CME historical-source candidates",
        subtree="reports/exchange_calendar",
    )
    receipt = DataReleaseReceipt.from_manifest(manifest_path, boundary)
    capture = load_historical_source_discovery_capture(
        receipt, boundary=boundary
    )
    manifest_sha256 = sha256_file(manifest_path)
    candidates = _canonical_object(
        result_path, description="CME historical-source candidates"
    )
    candidate_result_id = _validate_candidates(
        candidates,
        receipt=receipt,
        manifest_sha256=manifest_sha256,
        capture=capture,
    )
    response = capture.get("response")
    authority = capture.get("authority")
    if not isinstance(response, dict) or not isinstance(authority, dict):
        raise IntegrityError("CME notices discovery capture is invalid")
    logical_path = response.get("logical_path")
    if (
        response.get("url") != NOTICES_URL
        or type(logical_path) is not str
        or type(response.get("sha256")) is not str
        or _SHA256.fullmatch(str(response["sha256"])) is None
    ):
        raise IntegrityError("CME notices discovery response is invalid")
    notices_path = receipt.resolve_file(logical_path, boundary)
    try:
        source = notices_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise IntegrityError("CME notices page is not readable UTF-8") from exc
    archive_url = _archive_link_from_notices(source)
    if candidates.get("response_sha256") != response["sha256"]:
        raise IntegrityError(
            "CME historical-source candidates bind another response"
        )
    return {
        "archive_url": archive_url,
        "candidate_result_id": candidate_result_id,
        "candidate_result_path": result_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "candidate_result_sha256": sha256_file(result_path),
        "notices_capture_id": capture["capture_id"],
        "notices_manifest_path": manifest_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "notices_manifest_sha256": manifest_sha256,
        "notices_receipt_id": receipt.receipt_id,
        "notices_release_id": receipt.release_id,
        "notices_response_logical_path": logical_path,
        "notices_response_sha256": response["sha256"],
        "notices_source_url": NOTICES_URL,
        "probe_result_id": authority["probe_result_id"],
    }


def _validate_authority_shape(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError("CME archive-landing authority schema is invalid")
    for key in (
        "candidate_result_id",
        "candidate_result_sha256",
        "notices_capture_id",
        "notices_manifest_sha256",
        "notices_receipt_id",
        "notices_release_id",
        "notices_response_sha256",
        "probe_result_id",
    ):
        value = authority.get(key)
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise ContractError("CME archive-landing authority hash is invalid")
    if (
        authority.get("archive_url") != ARCHIVE_URL
        or authority.get("notices_source_url") != NOTICES_URL
        or type(authority.get("candidate_result_path")) is not str
        or type(authority.get("notices_manifest_path")) is not str
        or type(authority.get("notices_response_logical_path")) is not str
    ):
        raise ContractError("CME archive-landing authority value is invalid")


def build_historical_archive_plan(
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
            "CME archive-landing implementation hashes are invalid"
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
            "DISCOVER_CME_HISTORICAL_ADVISORY_ARCHIVE_STRUCTURE_ONLY"
        ),
        "request": {
            "accept": "text/html",
            "request_id": "advisory-archive",
            "request_kind": "HISTORICAL_ADVISORY_ARCHIVE_LANDING",
            "url": ARCHIVE_URL,
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


def validate_historical_archive_plan(
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
        raise IntegrityError("CME archive-landing plan schema is invalid")
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
        raise IntegrityError("CME archive-landing plan identity is invalid")
    authority = scope.get("authority")
    implementation = scope.get("implementation_sha256")
    if not isinstance(authority, dict) or not isinstance(implementation, dict):
        raise IntegrityError("CME archive-landing plan scope is invalid")
    expected = build_historical_archive_plan(
        authority=authority,
        implementation_sha256=implementation,
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME archive-landing plan differs from bounded implementation"
        )
    return dict(payload)


def validate_historical_archive_approval(
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
        raise HistoricalArchiveCaptureError(
            "CME archive-landing capture lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _safe_url(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    if (
        url != ARCHIVE_URL
        or parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != "/tools-information/advisory-archive.html"
        or parsed.query
        or parsed.fragment
    ):
        raise HistoricalArchiveCaptureError(
            "CME archive-landing URL is outside the exact allowlist"
        )


def capture_historical_archive_landing(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_historical_archive_plan(
        _canonical_object(plan_path, description="CME archive-landing plan")
    )
    approval = _canonical_object(
        approval_path, description="CME archive-landing approval"
    )
    approval_id = validate_historical_archive_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != implementation_hashes(
        publisher.boundary.active_root
    ):
        raise HistoricalArchiveCaptureError(
            "CME archive-landing implementation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived_authority = historical_archive_authority(
        notices_manifest_path=publisher.boundary.active_root
        / str(authority["notices_manifest_path"]),
        candidate_result_path=publisher.boundary.active_root
        / str(authority["candidate_result_path"]),
        boundary=publisher.boundary,
    )
    if authority != derived_authority:
        raise HistoricalArchiveCaptureError(
            "CME archive-landing authority changed"
        )
    request_spec = scope["request"]
    assert isinstance(request_spec, dict)
    url = str(request_spec["url"])
    _safe_url(url)
    stage = publisher.create_stage("cme_historical_archive_landing")
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "text/html",
            "User-Agent": "futures-intraday-model-v2-calendar/1.0",
        },
        method="GET",
    )
    try:
        with opener.open(request, timeout=MAX_DURATION_SECONDS) as response:
            if response.status != 200 or response.geturl() != url:
                raise HistoricalArchiveCaptureError(
                    "CME archive-landing response is not exact HTTP 200"
                )
            if response.headers.get_content_type() != "text/html":
                raise HistoricalArchiveCaptureError(
                    "CME archive-landing content type is unexpected"
                )
            body = response.read(MAX_TOTAL_BYTES + 1)
            if len(body) > MAX_TOTAL_BYTES:
                raise HistoricalArchiveCaptureError(
                    "CME archive-landing byte ceiling is exceeded"
                )
            safe_headers = {
                key.lower(): value
                for key, value in response.headers.items()
                if key.lower() in _SAFE_HEADERS
            }
    except HistoricalArchiveCaptureError:
        raise
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise HistoricalArchiveCaptureError(
            "CME archive-landing request failed before publication"
        ) from exc
    elapsed_milliseconds = int(
        (monotonic_time.monotonic() - started) * 1000
    )
    if elapsed_milliseconds > MAX_DURATION_SECONDS * 1000:
        raise HistoricalArchiveCaptureError(
            "CME archive-landing duration ceiling is exceeded"
        )
    staged_name = "001-advisory-archive.html"
    staged = stage / staged_name
    staged.write_bytes(body)
    logical_path = (
        "data/reference/exchange_calendars/001-advisory-archive.html"
    )
    response_record = {
        "content_type": "text/html",
        "logical_path": logical_path,
        "received_at_utc": captured_at.isoformat().replace("+00:00", "Z"),
        "request_id": "advisory-archive",
        "request_kind": "HISTORICAL_ADVISORY_ARCHIVE_LANDING",
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
        source_release_ids=(str(authority["notices_release_id"]),),
        embedded_documents={"capture_receipt.json": capture_receipt},
        metadata={
            "approval_receipt_id": approval_id,
            "candidate_result_id": authority["candidate_result_id"],
            "capture_id": capture_receipt["capture_id"],
            "captured_at_utc": core["captured_at_utc"],
            "plan_id": plan["plan_id"],
        },
    )
    manifest_path = publisher.publish(
        stage,
        manifest,
        staged_paths={logical_path: staged_name},
    )
    receipt = DataReleaseReceipt.from_manifest(manifest_path, publisher.boundary)
    load_historical_archive_landing_capture(
        receipt, boundary=publisher.boundary
    )
    return receipt


def load_historical_archive_landing_capture(
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
            "candidate_result_id",
            "capture_id",
            "captured_at_utc",
            "plan_id",
        }
    ):
        raise IntegrityError("CME archive-landing release contract is invalid")
    raw = receipt.embedded_document("capture_receipt.json", boundary)
    if not isinstance(raw, dict):
        raise IntegrityError("CME archive-landing capture receipt is invalid")
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
        or response.get("url") != ARCHIVE_URL
        or response.get("request_kind")
        != "HISTORICAL_ADVISORY_ARCHIVE_LANDING"
        or response.get("content_type") != "text/html"
        or response.get("status_code") != 200
        or manifest.source_release_ids
        != (str(authority.get("notices_release_id")),)
        or manifest.metadata.get("capture_id") != capture_id
        or manifest.metadata.get("approval_receipt_id")
        != payload.get("approval_receipt_id")
    ):
        raise IntegrityError("CME archive-landing capture contract is invalid")
    logical_path = response.get("logical_path")
    if type(logical_path) is not str:
        raise IntegrityError("CME archive-landing response path is invalid")
    physical = receipt.resolve_file(logical_path, boundary)
    if (
        physical.stat().st_size != response.get("size")
        or sha256_file(physical) != response.get("sha256")
    ):
        raise IntegrityError("CME archive-landing response bytes changed")
    return dict(raw)
