"""Offline notice assessment and bounded complete notice-document capture."""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import time as monotonic_time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import RepoBoundary
from .calendar_notice_documents import load_document_probe_capture
from .canonical import canonical_bytes, fsync_directory, sha256_file, sha256_json
from .data_layout import DataReleaseManifest, DataReleaseReceipt, PhasePublisher
from .errors import ContractError, IntegrityError, UnauthorizedOperation


ASSESSMENT_SCHEMA = "cme_historical_notice_document_probe_assessment/1.0.0"
PLAN_SCHEMA = "cme_historical_notice_document_union_plan/1.0.0"
APPROVAL_SCHEMA = "cme_historical_notice_document_union_approval/1.0.0"
CAPTURE_SCHEMA = "cme_historical_notice_document_union_capture/1.0.0"
FAILURE_SCHEMA = "cme_historical_notice_document_union_failure/1.0.0"
OPERATION = "CAPTURE_BOUNDED_PUBLIC_CME_HISTORICAL_NOTICE_DOCUMENT_UNION"
RELEASE_KIND = "cme_historical_notice_document_union_capture"
MAX_REQUESTS = 1_273
MAX_RESPONSE_BYTES = 1_048_576
MAX_TOTAL_BYTES = MAX_REQUESTS * MAX_RESPONSE_BYTES
MAX_DURATION_SECONDS = 5_400
REQUEST_TIMEOUT_SECONDS = 30
WORKERS = 2
IMPLEMENTATION_PATHS = (
    "configs/data_layout_contract.json",
    "configs/exchange_calendar_policy.json",
    "configs/source_contract.json",
    "pyproject.toml",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/calendar_cli.py",
    "src/futures_rebuild/calendar_notice_documents.py",
    "src/futures_rebuild/calendar_notice_union.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/source_contract.py",
)
FORBIDDEN_ACTIONS = (
    "ACTIVATE_CALENDAR",
    "CALL_ANOTHER_CME_ENDPOINT_OR_UNLISTED_NOTICE",
    "CALL_DATABENTO_OR_ANY_NON_CME_PROVIDER",
    "CREATE_LINK_OR_MUTABLE_EXTERNAL_REFERENCE",
    "DELETE_OR_OVERWRITE_ACCEPTED_RELEASE",
    "DOWNLOAD_LINKED_ATTACHMENT_OR_PDF",
    "EXECUTE_PAGE_SCRIPT_OR_FOLLOW_DISCOVERED_LINK",
    "PARSE_OR_ACCEPT_HISTORICAL_CALENDAR",
    "REBUILD_FOUNDATION",
    "RETRY_OR_REDIRECT_REQUEST",
    "USE_CREDENTIAL_OR_SECRET",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "BYTE_OR_DURATION_CEILING_REACHED",
    "CONTENT_TYPE_OR_HTTP_STATUS_MISMATCH",
    "IMPLEMENTATION_HASH_DRIFT",
    "NOTICE_INDEX_OR_PROBE_ASSESSMENT_DRIFT",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PRIOR_PLAN_OUTCOME_EXISTS",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
_AUTHORITY_KEYS = {
    "assessment_id",
    "assessment_path",
    "assessment_sha256",
    "index_id",
    "index_path",
    "index_sha256",
    "pagination_release_id",
    "probe_capture_id",
    "probe_manifest_path",
    "probe_manifest_sha256",
    "probe_release_id",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_SAFE_HEADERS = frozenset(
    {"cache-control", "content-type", "date", "etag", "last-modified"}
)
_MODERN_PDF = (
    "https://www.cmegroup.com/notices/clearing/2018/12/Chadv18-474.pdf"
)
_LEGACY_PDF = (
    "https://www.cmegroup.com/tools-information/lookups/advisories/"
    "clearing/files/Chadv15-075aa.pdf"
)


class NoticeUnionCaptureError(UnauthorizedOperation):
    """Raised before or during the bounded complete notice-page capture."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise NoticeUnionCaptureError(
            "CME notice-document union rejected an HTTP redirect"
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
                f"CME notice-union implementation input is missing: {relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _plain_text(fragment: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", fragment)
    return " ".join(html.unescape(without_tags).split())


def _notice_structure(
    *,
    request_id: str,
    url: str,
    body: bytes,
) -> dict[str, object]:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise IntegrityError("CME notice probe HTML is not UTF-8") from exc
    title_match = re.search(
        r"<title>(.*?)</title>", text, flags=re.IGNORECASE | re.DOTALL
    )
    heading_match = re.search(
        r'<li class="cmeListTitle[^"]*".*?<h1>(.*?)</h1>',
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    content_match = re.search(
        r'<li class="cmeAdvisoryContent[^"]*">(.*?)'
        r'<div class="cmeAttachmentField',
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if title_match is None or heading_match is None or content_match is None:
        raise IntegrityError("CME notice advisory structure is absent")
    content = content_match.group(1)
    hrefs = sorted(
        {
            urllib.parse.urljoin(url, html.unescape(match))
            for match in re.findall(
                r'href="([^"]+)"', content, flags=re.IGNORECASE
            )
            if match.lower().endswith(".pdf")
        }
    )
    plain = _plain_text(content)
    if request_id == "national-day-mourning-2018":
        if (
            "Trading hours for CME Group U.S.-based equity products"
            not in plain
            or hrefs != [_MODERN_PDF]
        ):
            raise IntegrityError("modern CME notice probe structure drifted")
        shape = "INLINE_SCHEDULE_TEXT_WITH_PDF_MIRROR"
    elif request_id == "good-friday-2015":
        if (
            "holiday processing schedule" not in plain
            or "attached" not in plain
            or hrefs != [_LEGACY_PDF]
        ):
            raise IntegrityError("legacy CME notice probe structure drifted")
        shape = "ATTACHMENT_BACKED_SCHEDULE"
    else:
        raise IntegrityError("unexpected CME notice probe identity")
    return {
        "attachment_urls": hrefs,
        "document_shape": shape,
        "heading": _plain_text(heading_match.group(1)),
        "request_id": request_id,
        "title": _plain_text(title_match.group(1)),
        "url": url,
    }


def build_probe_assessment(
    *,
    probe_manifest_path: Path,
    index_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    manifest_path = boundary.assert_active_path(
        probe_manifest_path,
        purpose="CME notice-document probe manifest",
        subtree="manifests/data_releases/reference",
    )
    report_path = boundary.assert_active_path(
        index_path,
        purpose="CME historical notice metadata index",
        subtree="reports/exchange_calendar",
    )
    index = _canonical_object(
        report_path, description="CME historical notice metadata index"
    )
    if (
        index.get("schema_version")
        != "cme_historical_notice_metadata_index/1.0.0"
        or index.get("unique_url_count") != MAX_REQUESTS
        or index.get("overlap_url_count") != 159
        or type(index.get("index_id")) is not str
    ):
        raise IntegrityError("CME notice metadata index is invalid")
    receipt = DataReleaseReceipt.from_manifest(manifest_path, boundary)
    capture = load_document_probe_capture(receipt, boundary=boundary)
    responses = capture.get("responses")
    if not isinstance(responses, list) or len(responses) != 2:
        raise IntegrityError("CME notice-document probe responses are invalid")
    documents: list[dict[str, object]] = []
    for response in responses:
        if not isinstance(response, dict):
            raise IntegrityError("CME notice-document probe response is invalid")
        physical = receipt.resolve_file(str(response["logical_path"]), boundary)
        documents.append(
            _notice_structure(
                request_id=str(response["request_id"]),
                url=str(response["url"]),
                body=physical.read_bytes(),
            )
        )
    if [item["request_id"] for item in documents] != [
        "national-day-mourning-2018",
        "good-friday-2015",
    ]:
        raise IntegrityError("CME notice-document probe ordering drifted")
    core: dict[str, object] = {
        "attachment_discovery_status": (
            "DEFER_UNTIL_COMPLETE_HTML_UNION_IS_CAPTURED"
        ),
        "classification": "NOTICE_HTML_CAN_BE_INLINE_OR_ATTACHMENT_BACKED",
        "documents": documents,
        "forbidden_interpretations": [
            "TITLES_CANNOT_EXCLUDE_BODY_ONLY_QUERY_MATCHES",
            "PROBE_ATTACHMENTS_DO_NOT_DEFINE_THE_COMPLETE_ATTACHMENT_SET",
            "NOTICE_HTML_OR_PDF_BYTES_ARE_NOT_YET_ACCEPTED_CALENDAR_SEGMENTS",
        ],
        "index_id": index["index_id"],
        "index_path": report_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "index_sha256": sha256_file(report_path),
        "next_authority": (
            "HASH_BOUND_CME_HISTORICAL_NOTICE_DOCUMENT_UNION_"
            "CAPTURE_APPROVAL_REQUIRED"
        ),
        "pagination_release_id": index["pagination_release_id"],
        "probe_capture_id": capture["capture_id"],
        "probe_manifest_path": manifest_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "probe_manifest_sha256": sha256_file(manifest_path),
        "probe_release_id": receipt.release_id,
        "required_document_url_count": MAX_REQUESTS,
        "schema_version": ASSESSMENT_SCHEMA,
        "status": "COMPLETE_HTML_UNION_CAPTURE_READY",
    }
    return {**core, "assessment_id": sha256_json(core)}


def union_authority(
    *,
    probe_manifest_path: Path,
    index_path: Path,
    assessment_path: Path,
    boundary: RepoBoundary,
) -> tuple[dict[str, object], dict[str, object]]:
    report_path = boundary.assert_active_path(
        assessment_path,
        purpose="CME notice-document probe assessment",
        subtree="reports/exchange_calendar",
    )
    expected = build_probe_assessment(
        probe_manifest_path=probe_manifest_path,
        index_path=index_path,
        boundary=boundary,
    )
    assessment = _canonical_object(
        report_path, description="CME notice-document probe assessment"
    )
    if assessment != expected:
        raise IntegrityError("CME notice-document probe assessment is invalid")
    index = _canonical_object(
        index_path, description="CME historical notice metadata index"
    )
    authority = {
        "assessment_id": assessment["assessment_id"],
        "assessment_path": report_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "assessment_sha256": sha256_file(report_path),
        "index_id": assessment["index_id"],
        "index_path": assessment["index_path"],
        "index_sha256": assessment["index_sha256"],
        "pagination_release_id": assessment["pagination_release_id"],
        "probe_capture_id": assessment["probe_capture_id"],
        "probe_manifest_path": assessment["probe_manifest_path"],
        "probe_manifest_sha256": assessment["probe_manifest_sha256"],
        "probe_release_id": assessment["probe_release_id"],
    }
    return authority, index


def _validate_authority(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError("CME notice-union authority schema is invalid")
    for key in _AUTHORITY_KEYS - {
        "assessment_path",
        "index_path",
        "probe_manifest_path",
    }:
        value = authority.get(key)
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise ContractError("CME notice-union authority hash is invalid")
    for key in ("assessment_path", "index_path", "probe_manifest_path"):
        if type(authority.get(key)) is not str:
            raise ContractError("CME notice-union authority path is invalid")


def _requests(candidates: Sequence[object]) -> list[dict[str, object]]:
    if len(candidates) != MAX_REQUESTS:
        raise ContractError("CME notice-union candidate count is invalid")
    requests: list[dict[str, object]] = []
    previous_url = ""
    for ordinal, candidate in enumerate(candidates, start=1):
        if not isinstance(candidate, dict):
            raise ContractError("CME notice-union candidate is invalid")
        url = candidate.get("url")
        queries = candidate.get("queries")
        if (
            type(url) is not str
            or url <= previous_url
            or not isinstance(queries, list)
            or not queries
            or any(type(item) is not str for item in queries)
            or type(candidate.get("relative_url")) is not str
            or type(candidate.get("title")) is not str
        ):
            raise ContractError("CME notice-union candidate ordering is invalid")
        previous_url = url
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
        request_id = f"notice-{ordinal:04d}-{digest}"
        requests.append(
            {
                "accept": "text/html",
                "matched_queries": list(queries),
                "metadata_title": candidate["title"],
                "ordinal": ordinal,
                "request_id": request_id,
                "request_kind": "HISTORICAL_NOTICE_DOCUMENT_UNION",
                "url": url,
            }
        )
    return requests


def build_union_plan(
    *,
    authority: Mapping[str, object],
    candidates: Sequence[object],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    _validate_authority(authority)
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
    ):
        raise ContractError("CME notice-union implementation hashes are invalid")
    requests = _requests(candidates)
    scope: dict[str, object] = {
        "allow_redirects": False,
        "authority": dict(authority),
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "max_duration_seconds": MAX_DURATION_SECONDS,
        "max_requests": MAX_REQUESTS,
        "max_response_bytes": MAX_RESPONSE_BYTES,
        "max_total_bytes": MAX_TOTAL_BYTES,
        "output_paths": {
            "data_template": (
                "data/reference/exchange_calendars/{release_id}/"
                "{request_id}.html"
            ),
            "failure_report": (
                "reports/exchange_calendar/"
                "cme_historical_notice_document_union_capture_failure_"
                "{plan_prefix}.json"
            ),
            "manifest_template": (
                "manifests/data_releases/reference/{release_id}.json"
            ),
            "publication_lock": "state/locks/data-publication.lock",
            "staging_root": "state/data_publication_staging",
        },
        "purpose": (
            "CAPTURE_COMPLETE_QUERY_MATCHED_NOTICE_HTML_UNION_BEFORE_"
            "ATTACHMENT_DISCOVERY"
        ),
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "requests": requests,
        "retries": 0,
        "stop_conditions": list(STOP_CONDITIONS),
        "workers": WORKERS,
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": OPERATION,
        "schema_version": PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_union_plan(payload: Mapping[str, object]) -> dict[str, object]:
    if set(payload) != {
        "classification",
        "execution_authorized",
        "operation",
        "plan_id",
        "schema_version",
        "scope",
    }:
        raise IntegrityError("CME notice-union plan schema is invalid")
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
        raise IntegrityError("CME notice-union plan identity is invalid")
    authority = scope.get("authority")
    implementation = scope.get("implementation_sha256")
    requests = scope.get("requests")
    if (
        not isinstance(authority, dict)
        or not isinstance(implementation, dict)
        or not isinstance(requests, list)
    ):
        raise IntegrityError("CME notice-union plan scope is invalid")
    candidates = [
        {
            "queries": request["matched_queries"],
            "relative_url": urllib.parse.urlparse(str(request["url"])).path,
            "title": request["metadata_title"],
            "url": request["url"],
        }
        for request in requests
        if isinstance(request, dict)
    ]
    if len(candidates) != len(requests):
        raise IntegrityError("CME notice-union request schema is invalid")
    expected = build_union_plan(
        authority=authority,
        candidates=candidates,
        implementation_sha256=implementation,
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME notice-union plan differs from bounded implementation"
        )
    return dict(payload)


def validate_union_approval(
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
        raise NoticeUnionCaptureError(
            "CME notice-union capture lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _safe_url(url: str, *, allowed: set[str]) -> None:
    parsed = urllib.parse.urlparse(url)
    if (
        url not in allowed
        or parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise NoticeUnionCaptureError(
            "CME notice-union URL is outside the exact allowlist"
        )


def _failure_path(root: Path, plan_id: str) -> Path:
    return (
        root
        / "reports"
        / "exchange_calendar"
        / (
            "cme_historical_notice_document_union_capture_failure_"
            f"{plan_id[:8]}.json"
        )
    )


def _existing_release_for_plan(root: Path, plan_id: str) -> Path | None:
    manifests = root / "manifests" / "data_releases" / "reference"
    if not manifests.is_dir():
        return None
    for path in sorted(manifests.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, ValueError):
            continue
        if (
            isinstance(payload, dict)
            and payload.get("release_kind") == RELEASE_KIND
            and isinstance(payload.get("metadata"), dict)
            and payload["metadata"].get("plan_id") == plan_id
        ):
            return path
    return None


def _write_create_only(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, canonical_bytes(dict(payload)) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(path.parent)


def _fetch(
    spec: Mapping[str, object],
    *,
    allowed: set[str],
) -> tuple[bytes, dict[str, str], str]:
    url = str(spec["url"])
    _safe_url(url, allowed=allowed)
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "text/html",
            "User-Agent": "futures-intraday-model-v2-calendar/1.0",
        },
        method="GET",
    )
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        with opener.open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            if response.status != 200 or response.geturl() != url:
                raise NoticeUnionCaptureError(
                    "CME notice-union response is not exact HTTP 200"
                )
            if response.headers.get_content_type() != "text/html":
                raise NoticeUnionCaptureError(
                    "CME notice-union content type is unexpected"
                )
            body = response.read(MAX_RESPONSE_BYTES + 1)
            if len(body) > MAX_RESPONSE_BYTES:
                raise NoticeUnionCaptureError(
                    "CME notice-union response byte ceiling is exceeded"
                )
            safe_headers = {
                key.lower(): value
                for key, value in response.headers.items()
                if key.lower() in _SAFE_HEADERS
            }
    except NoticeUnionCaptureError:
        raise
    except (
        urllib.error.HTTPError,
        urllib.error.URLError,
        TimeoutError,
    ) as exc:
        raise NoticeUnionCaptureError(
            "CME notice-union request failed"
        ) from exc
    received = datetime.now(timezone.utc).replace(microsecond=0)
    return (
        body,
        dict(sorted(safe_headers.items())),
        received.isoformat().replace("+00:00", "Z"),
    )


def _failure_report(
    *,
    plan: Mapping[str, object],
    plan_path: Path,
    approval_id: str,
    stage: Path,
    attempted: int,
    responses: Sequence[Mapping[str, object]],
    failures: Sequence[Mapping[str, object]],
    elapsed_milliseconds: int,
    boundary: RepoBoundary,
) -> dict[str, object]:
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "elapsed_milliseconds": elapsed_milliseconds,
        "failed_requests": list(failures),
        "network_requests_attempted": attempted,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "publication_occurred": False,
        "responses_preserved": list(responses),
        "responses_preserved_count": len(responses),
        "retries_performed": 0,
        "schema_version": FAILURE_SCHEMA,
        "stage_relative_path": stage.relative_to(
            boundary.active_root
        ).as_posix(),
        "status": "STOPPED",
    }
    return {**core, "failure_id": sha256_json(core)}


def capture_union(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_union_plan(
        _canonical_object(plan_path, description="CME notice-union plan")
    )
    approval = _canonical_object(
        approval_path, description="CME notice-union approval"
    )
    approval_id = validate_union_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    root = publisher.boundary.active_root
    if scope["implementation_sha256"] != implementation_hashes(root):
        raise NoticeUnionCaptureError(
            "CME notice-union implementation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived, index = union_authority(
        probe_manifest_path=root / str(authority["probe_manifest_path"]),
        index_path=root / str(authority["index_path"]),
        assessment_path=root / str(authority["assessment_path"]),
        boundary=publisher.boundary,
    )
    if authority != derived:
        raise NoticeUnionCaptureError("CME notice-union authority changed")
    candidates = index.get("candidates")
    if not isinstance(candidates, list):
        raise NoticeUnionCaptureError("CME notice-union candidates are absent")
    expected = build_union_plan(
        authority=derived,
        candidates=candidates,
        implementation_sha256=implementation_hashes(root),
    )
    if plan != expected:
        raise NoticeUnionCaptureError("CME notice-union requests drifted")
    failure_path = _failure_path(root, str(plan["plan_id"]))
    prior_release = _existing_release_for_plan(root, str(plan["plan_id"]))
    if failure_path.exists() or prior_release is not None:
        raise NoticeUnionCaptureError(
            "CME notice-union approval already has a durable outcome"
        )
    requests = scope["requests"]
    assert isinstance(requests, list)
    allowed = {str(item["url"]) for item in requests if isinstance(item, dict)}
    if len(allowed) != MAX_REQUESTS:
        raise NoticeUnionCaptureError("CME notice-union allowlist is invalid")
    stage = publisher.create_stage("cme_notice_document_union")
    started = monotonic_time.monotonic()
    responses: list[dict[str, object]] = []
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    total_bytes = 0
    attempted = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        for offset in range(0, len(requests), WORKERS):
            elapsed = int((monotonic_time.monotonic() - started) * 1000)
            if elapsed >= MAX_DURATION_SECONDS * 1000:
                failure = _failure_report(
                    plan=plan,
                    plan_path=plan_path,
                    approval_id=approval_id,
                    stage=stage,
                    attempted=attempted,
                    responses=responses,
                    failures=[
                        {
                            "error_class": "DURATION_CEILING_REACHED",
                            "request_id": requests[offset]["request_id"],  # type: ignore[index]
                        }
                    ],
                    elapsed_milliseconds=elapsed,
                    boundary=publisher.boundary,
                )
                _write_create_only(failure_path, failure)
                raise NoticeUnionCaptureError(
                    "CME notice-union duration ceiling is reached"
                )
            batch = requests[offset : offset + WORKERS]
            futures = [
                (
                    spec,
                    executor.submit(_fetch, spec, allowed=allowed),
                )
                for spec in batch
                if isinstance(spec, dict)
            ]
            attempted += len(futures)
            completed: list[
                tuple[Mapping[str, object], bytes, dict[str, str], str]
            ] = []
            failures: list[dict[str, object]] = []
            for spec, future in futures:
                try:
                    body, safe_headers, received_at = future.result()
                    completed.append(
                        (spec, body, safe_headers, received_at)
                    )
                except Exception as exc:
                    failures.append(
                        {
                            "error_class": type(exc).__name__,
                            "request_id": spec["request_id"],
                            "url": spec["url"],
                        }
                    )
            for spec, body, safe_headers, received_at in sorted(
                completed, key=lambda item: int(item[0]["ordinal"])
            ):
                request_id = str(spec["request_id"])
                name = f"{request_id}.html"
                staged = stage / name
                staged.write_bytes(body)
                logical = f"data/reference/exchange_calendars/{name}"
                logical_paths[name] = logical
                staged_paths[logical] = name
                total_bytes += len(body)
                responses.append(
                    {
                        "content_type": "text/html",
                        "logical_path": logical,
                        "matched_queries": spec["matched_queries"],
                        "metadata_title": spec["metadata_title"],
                        "ordinal": spec["ordinal"],
                        "received_at_utc": received_at,
                        "request_id": request_id,
                        "request_kind": (
                            "HISTORICAL_NOTICE_DOCUMENT_UNION"
                        ),
                        "safe_headers": safe_headers,
                        "sha256": sha256_file(staged),
                        "size": len(body),
                        "status_code": 200,
                        "url": spec["url"],
                    }
                )
            if failures:
                elapsed = int(
                    (monotonic_time.monotonic() - started) * 1000
                )
                failure = _failure_report(
                    plan=plan,
                    plan_path=plan_path,
                    approval_id=approval_id,
                    stage=stage,
                    attempted=attempted,
                    responses=responses,
                    failures=failures,
                    elapsed_milliseconds=elapsed,
                    boundary=publisher.boundary,
                )
                _write_create_only(failure_path, failure)
                raise NoticeUnionCaptureError(
                    "CME notice-union stopped on a request failure"
                )
    elapsed = int((monotonic_time.monotonic() - started) * 1000)
    if (
        elapsed > MAX_DURATION_SECONDS * 1000
        or attempted != MAX_REQUESTS
        or len(responses) != MAX_REQUESTS
        or total_bytes > MAX_TOTAL_BYTES
    ):
        failure = _failure_report(
            plan=plan,
            plan_path=plan_path,
            approval_id=approval_id,
            stage=stage,
            attempted=attempted,
            responses=responses,
            failures=[{"error_class": "FINAL_COMPLETION_BOUND_FAILED"}],
            elapsed_milliseconds=elapsed,
            boundary=publisher.boundary,
        )
        _write_create_only(failure_path, failure)
        raise NoticeUnionCaptureError(
            "CME notice-union final completion bound failed"
        )
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "authority": authority,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": MAX_DURATION_SECONDS,
            "max_requests": MAX_REQUESTS,
            "max_response_bytes": MAX_RESPONSE_BYTES,
            "max_total_bytes": MAX_TOTAL_BYTES,
            "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "retries": 0,
            "workers": WORKERS,
        },
        "capture_approval": dict(approval),
        "elapsed_milliseconds": elapsed,
        "network_request_count": MAX_REQUESTS,
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "responses": responses,
        "schema_version": CAPTURE_SCHEMA,
        "total_bytes": total_bytes,
    }
    capture = {**core, "capture_id": sha256_json(core)}
    manifest = DataReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=RELEASE_KIND,
        schema_version=CAPTURE_SCHEMA,
        logical_paths=logical_paths,
        source_release_ids=tuple(
            sorted(
                {
                    str(authority["pagination_release_id"]),
                    str(authority["probe_release_id"]),
                }
            )
        ),
        embedded_documents={"capture_receipt.json": capture},
        metadata={
            "approval_receipt_id": approval_id,
            "assessment_id": authority["assessment_id"],
            "capture_id": capture["capture_id"],
            "index_id": authority["index_id"],
            "plan_id": plan["plan_id"],
        },
    )
    manifest_path = publisher.publish(
        stage, manifest, staged_paths=staged_paths
    )
    receipt = DataReleaseReceipt.from_manifest(
        manifest_path, publisher.boundary
    )
    load_union_capture(receipt, boundary=publisher.boundary)
    return receipt


def load_union_capture(
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
    ):
        raise IntegrityError("CME notice-union release is invalid")
    raw = receipt.embedded_document("capture_receipt.json", boundary)
    if not isinstance(raw, dict):
        raise IntegrityError("CME notice-union capture receipt is invalid")
    core = dict(raw)
    capture_id = core.pop("capture_id", None)
    responses = raw.get("responses")
    if (
        type(capture_id) is not str
        or capture_id != sha256_json(core)
        or raw.get("schema_version") != CAPTURE_SCHEMA
        or raw.get("operation") != OPERATION
        or raw.get("network_request_count") != MAX_REQUESTS
        or not isinstance(responses, list)
        or len(responses) != MAX_REQUESTS
        or manifest.metadata.get("capture_id") != capture_id
    ):
        raise IntegrityError("CME notice-union capture contract is invalid")
    total = 0
    previous_ordinal = 0
    urls: set[str] = set()
    for response in responses:
        if (
            not isinstance(response, dict)
            or response.get("ordinal") != previous_ordinal + 1
            or response.get("content_type") != "text/html"
            or response.get("status_code") != 200
            or type(response.get("logical_path")) is not str
            or type(response.get("url")) is not str
            or response["url"] in urls
        ):
            raise IntegrityError("CME notice-union response contract is invalid")
        previous_ordinal += 1
        urls.add(str(response["url"]))
        physical = receipt.resolve_file(str(response["logical_path"]), boundary)
        if (
            physical.stat().st_size != response.get("size")
            or sha256_file(physical) != response.get("sha256")
        ):
            raise IntegrityError("CME notice-union response bytes changed")
        total += physical.stat().st_size
    if total != raw.get("total_bytes"):
        raise IntegrityError("CME notice-union total bytes are invalid")
    return dict(raw)
