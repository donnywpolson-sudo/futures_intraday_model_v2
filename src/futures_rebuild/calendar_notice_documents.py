"""Offline notice index and bounded CME notice-document structure probe."""

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
from .calendar_notice_pagination import load_pagination_capture
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import DataReleaseManifest, DataReleaseReceipt, PhasePublisher
from .errors import ContractError, IntegrityError, UnauthorizedOperation


INDEX_SCHEMA = "cme_historical_notice_metadata_index/1.0.0"
PLAN_SCHEMA = "cme_historical_notice_document_probe_plan/1.0.0"
APPROVAL_SCHEMA = "cme_historical_notice_document_probe_approval/1.0.0"
CAPTURE_SCHEMA = "cme_historical_notice_document_probe_capture/1.0.0"
OPERATION = "CAPTURE_BOUNDED_PUBLIC_CME_HISTORICAL_NOTICE_DOCUMENT_PROBE"
RELEASE_KIND = "cme_historical_notice_document_probe_capture"
PROBE_REQUESTS = (
    {
        "evidence_role": "MODERN_EXCEPTIONAL_CLOSURE_NOTICE",
        "request_id": "national-day-mourning-2018",
        "url": (
            "https://www.cmegroup.com/notices/clearing/2018/12/"
            "Chadv18-474.html"
        ),
    },
    {
        "evidence_role": "LEGACY_HOLIDAY_SCHEDULE_NOTICE",
        "request_id": "good-friday-2015",
        "url": (
            "https://www.cmegroup.com/tools-information/lookups/advisories/"
            "clearing/Chadv15-075.html"
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
    "src/futures_rebuild/calendar_notice_client.py",
    "src/futures_rebuild/calendar_notice_documents.py",
    "src/futures_rebuild/calendar_notice_metadata.py",
    "src/futures_rebuild/calendar_notice_pagination.py",
    "src/futures_rebuild/calendar_notice_search.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/exchange_calendar.py",
    "src/futures_rebuild/source_contract.py",
)
OUTPUT_PATHS = {
    "data_template": (
        "data/reference/exchange_calendars/{release_id}/"
        "{ordinal:03d}-{request_id}.html"
    ),
    "manifest_template": "manifests/data_releases/reference/{release_id}.json",
    "publication_lock": "state/locks/data-publication.lock",
    "staging_root": "state/data_publication_staging",
}
FORBIDDEN_ACTIONS = (
    "ACTIVATE_CALENDAR",
    "CALL_ANOTHER_CME_ENDPOINT_OR_NOTICE",
    "CALL_DATABENTO_OR_ANY_NON_CME_PROVIDER",
    "CREATE_LINK_OR_MUTABLE_EXTERNAL_REFERENCE",
    "DELETE_OR_OVERWRITE_ACCEPTED_RELEASE",
    "DOWNLOAD_LINKED_ATTACHMENT_OR_PDF",
    "FOLLOW_ANY_DISCOVERED_LINK",
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
    "METADATA_INDEX_OR_NOTICE_IDENTITY_DRIFT",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
_AUTHORITY_KEYS = {
    "index_id",
    "index_path",
    "index_sha256",
    "pagination_capture_id",
    "pagination_manifest_path",
    "pagination_manifest_sha256",
    "pagination_release_id",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_SAFE_HEADERS = frozenset(
    {"cache-control", "content-type", "date", "etag", "last-modified"}
)


class NoticeDocumentCaptureError(UnauthorizedOperation):
    """Raised before or during the two-page notice structure probe."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise NoticeDocumentCaptureError(
            "CME notice-document probe rejected an HTTP redirect"
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
                f"CME notice-document implementation input is missing: {relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _payload(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("CME notice metadata page is not JSON") from exc
    if not isinstance(payload, dict):
        raise IntegrityError("CME notice metadata page is not an object")
    return payload


def _title(text: str) -> str:
    decoded = html.unescape(text)
    match = re.search(
        r'<a[^>]+href="[^"]+"[^>]*>(.*?)<i class="icon">',
        decoded,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        raise IntegrityError("CME notice metadata title is absent")
    title = re.sub(r"<[^>]+>", " ", match.group(1))
    normalized = " ".join(html.unescape(title).split())
    if not normalized:
        raise IntegrityError("CME notice metadata title is empty")
    return normalized


def build_metadata_index(
    *,
    pagination_manifest_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    manifest_path = boundary.assert_active_path(
        pagination_manifest_path,
        purpose="CME historical Notices pagination manifest",
        subtree="manifests/data_releases/reference",
    )
    receipt = DataReleaseReceipt.from_manifest(manifest_path, boundary)
    capture = load_pagination_capture(receipt, boundary=boundary)
    responses = capture.get("responses")
    if not isinstance(responses, list):
        raise IntegrityError("CME notice pagination responses are invalid")
    records: dict[str, dict[str, object]] = {}
    query_pages: dict[str, dict[str, int]] = {}
    for response in responses:
        if not isinstance(response, dict):
            raise IntegrityError("CME notice pagination response is invalid")
        payload = _payload(
            receipt.resolve_file(str(response["logical_path"]), boundary)
        )
        query = str(response["query"])
        page = int(response["page"])
        results = payload.get("results")
        if (
            payload.get("currentPage") != page
            or not isinstance(results, list)
            or type(payload.get("totalPages")) is not int
            or type(payload.get("totalResults")) is not int
        ):
            raise IntegrityError("CME notice pagination payload drifted")
        summary = query_pages.setdefault(
            query,
            {
                "page_count": 0,
                "result_count": 0,
                "total_pages": int(payload["totalPages"]),
                "total_results": int(payload["totalResults"]),
            },
        )
        if (
            summary["total_pages"] != payload["totalPages"]
            or summary["total_results"] != payload["totalResults"]
        ):
            raise IntegrityError("CME notice pagination totals drifted")
        summary["page_count"] += 1
        summary["result_count"] += len(results)
        for item in results:
            if (
                not isinstance(item, dict)
                or set(item) != {"text", "url"}
                or type(item["text"]) is not str
                or type(item["url"]) is not str
            ):
                raise IntegrityError("CME notice metadata record is invalid")
            relative_url = str(item["url"])
            absolute_url = urllib.parse.urljoin(
                "https://www.cmegroup.com/notices.html", relative_url
            )
            parsed = urllib.parse.urlparse(absolute_url)
            if (
                parsed.scheme != "https"
                or parsed.hostname != "www.cmegroup.com"
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
            ):
                raise IntegrityError("CME notice metadata URL is unsafe")
            title = _title(str(item["text"]))
            record = records.setdefault(
                absolute_url,
                {
                    "queries": set(),
                    "relative_url": relative_url,
                    "title": title,
                    "url": absolute_url,
                },
            )
            if record["title"] != title or record["relative_url"] != relative_url:
                raise IntegrityError("CME notice metadata identity drifted")
            queries = record["queries"]
            assert isinstance(queries, set)
            queries.add(query)
    if query_pages != {
        "holiday": {
            "page_count": 17,
            "result_count": 510,
            "total_pages": 17,
            "total_results": 510,
        },
        "trading hours": {
            "page_count": 31,
            "result_count": 924,
            "total_pages": 31,
            "total_results": 924,
        },
    }:
        raise IntegrityError("CME notice metadata coverage is incomplete")
    candidates = [
        {
            **{key: value for key, value in record.items() if key != "queries"},
            "queries": sorted(record["queries"]),  # type: ignore[arg-type]
        }
        for record in records.values()
    ]
    candidates.sort(key=lambda item: str(item["url"]))
    if len(candidates) != 1273:
        raise IntegrityError("CME notice metadata union count changed")
    urls = {str(item["url"]) for item in candidates}
    if any(str(item["url"]) not in urls for item in PROBE_REQUESTS):
        raise IntegrityError("CME notice structure-probe candidate is absent")
    overlap_count = sum(len(item["queries"]) == 2 for item in candidates)
    if overlap_count != 159:
        raise IntegrityError("CME notice metadata overlap count changed")
    core: dict[str, object] = {
        "candidates": candidates,
        "classification": "COMPLETE_QUERY_MATCHED_NOTICE_METADATA_INDEX",
        "forbidden_interpretations": [
            "TITLE_ONLY_FILTERING_CANNOT_PROVE BODY_MATCHES_IRRELEVANT",
            "NOTICE_METADATA_DOES_NOT_AUTHORIZE_NOTICE_PAGE_OR_ATTACHMENT_FETCH",
            "QUERY_MATCHED_NOTICE_PAGES_ARE_NOT_SESSION_SEGMENT_EVIDENCE",
        ],
        "next_authority": (
            "HASH_BOUND_CME_HISTORICAL_NOTICE_DOCUMENT_PROBE_"
            "CAPTURE_APPROVAL_REQUIRED"
        ),
        "overlap_url_count": overlap_count,
        "pagination_capture_id": capture["capture_id"],
        "pagination_manifest_path": manifest_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "pagination_manifest_sha256": sha256_file(manifest_path),
        "pagination_release_id": receipt.release_id,
        "probe_requests": [dict(item) for item in PROBE_REQUESTS],
        "query_summaries": query_pages,
        "schema_version": INDEX_SCHEMA,
        "status": "TWO_EXACT_NOTICE_STRUCTURE_PROBES_READY",
        "unique_url_count": len(candidates),
    }
    return {**core, "index_id": sha256_json(core)}


def document_probe_authority(
    *,
    pagination_manifest_path: Path,
    index_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    report_path = boundary.assert_active_path(
        index_path,
        purpose="CME historical notice metadata index",
        subtree="reports/exchange_calendar",
    )
    expected = build_metadata_index(
        pagination_manifest_path=pagination_manifest_path,
        boundary=boundary,
    )
    index = _canonical_object(
        report_path, description="CME historical notice metadata index"
    )
    if index != expected:
        raise IntegrityError("CME historical notice metadata index is invalid")
    return {
        "index_id": index["index_id"],
        "index_path": report_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "index_sha256": sha256_file(report_path),
        "pagination_capture_id": index["pagination_capture_id"],
        "pagination_manifest_path": index["pagination_manifest_path"],
        "pagination_manifest_sha256": index["pagination_manifest_sha256"],
        "pagination_release_id": index["pagination_release_id"],
    }


def _validate_authority_shape(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError("CME notice-document authority schema is invalid")
    for key in _AUTHORITY_KEYS - {"index_path", "pagination_manifest_path"}:
        value = authority.get(key)
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise ContractError("CME notice-document authority hash is invalid")
    for key in ("index_path", "pagination_manifest_path"):
        if type(authority.get(key)) is not str:
            raise ContractError("CME notice-document authority path is invalid")


def build_document_probe_plan(
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
            "CME notice-document implementation hashes are invalid"
        )
    requests = [
        {
            "accept": "text/html",
            "evidence_role": item["evidence_role"],
            "request_id": item["request_id"],
            "request_kind": "HISTORICAL_NOTICE_DOCUMENT_STRUCTURE_PROBE",
            "url": item["url"],
        }
        for item in PROBE_REQUESTS
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
        "purpose": "DISCOVER_MODERN_AND_LEGACY_NOTICE_DOCUMENT_STRUCTURE_ONLY",
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


def validate_document_probe_plan(
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
        raise IntegrityError("CME notice-document plan schema is invalid")
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
        raise IntegrityError("CME notice-document plan identity is invalid")
    authority = scope.get("authority")
    implementation = scope.get("implementation_sha256")
    if not isinstance(authority, dict) or not isinstance(implementation, dict):
        raise IntegrityError("CME notice-document plan scope is invalid")
    expected = build_document_probe_plan(
        authority=authority, implementation_sha256=implementation
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME notice-document plan differs from bounded implementation"
        )
    return dict(payload)


def validate_document_probe_approval(
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
        raise NoticeDocumentCaptureError(
            "CME notice-document probe lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _safe_url(url: str) -> None:
    expected = {str(item["url"]) for item in PROBE_REQUESTS}
    parsed = urllib.parse.urlparse(url)
    if (
        url not in expected
        or parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise NoticeDocumentCaptureError(
            "CME notice-document URL is outside the exact allowlist"
        )


def capture_document_probe(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_document_probe_plan(
        _canonical_object(plan_path, description="CME notice-document plan")
    )
    approval = _canonical_object(
        approval_path, description="CME notice-document approval"
    )
    approval_id = validate_document_probe_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != implementation_hashes(
        publisher.boundary.active_root
    ):
        raise NoticeDocumentCaptureError(
            "CME notice-document implementation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived = document_probe_authority(
        pagination_manifest_path=publisher.boundary.active_root
        / str(authority["pagination_manifest_path"]),
        index_path=publisher.boundary.active_root
        / str(authority["index_path"]),
        boundary=publisher.boundary,
    )
    if authority != derived:
        raise NoticeDocumentCaptureError(
            "CME notice-document authority changed"
        )
    requests = scope["requests"]
    assert isinstance(requests, list)
    stage = publisher.create_stage("cme_notice_document_probe")
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    responses: list[dict[str, object]] = []
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    total_bytes = 0
    for ordinal, spec in enumerate(requests, start=1):
        assert isinstance(spec, dict)
        url = str(spec["url"])
        _safe_url(url)
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "text/html",
                "User-Agent": "futures-intraday-model-v2-calendar/1.0",
            },
            method="GET",
        )
        try:
            with opener.open(
                request, timeout=REQUEST_TIMEOUT_SECONDS
            ) as response:
                if response.status != 200 or response.geturl() != url:
                    raise NoticeDocumentCaptureError(
                        "CME notice-document response is not exact HTTP 200"
                    )
                if response.headers.get_content_type() != "text/html":
                    raise NoticeDocumentCaptureError(
                        "CME notice-document content type is unexpected"
                    )
                remaining = MAX_TOTAL_BYTES - total_bytes
                body = response.read(remaining + 1)
                if len(body) > remaining:
                    raise NoticeDocumentCaptureError(
                        "CME notice-document byte ceiling is exceeded"
                    )
                safe_headers = {
                    key.lower(): value
                    for key, value in response.headers.items()
                    if key.lower() in _SAFE_HEADERS
                }
        except NoticeDocumentCaptureError:
            raise
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
        ) as exc:
            raise NoticeDocumentCaptureError(
                "CME notice-document request failed before publication"
            ) from exc
        total_bytes += len(body)
        request_id = str(spec["request_id"])
        staged_name = f"{ordinal:03d}-{request_id}.html"
        staged = stage / staged_name
        staged.write_bytes(body)
        logical_path = f"data/reference/exchange_calendars/{staged_name}"
        logical_paths[staged_name] = logical_path
        staged_paths[logical_path] = staged_name
        responses.append(
            {
                "content_type": "text/html",
                "evidence_role": spec["evidence_role"],
                "logical_path": logical_path,
                "received_at_utc": captured_at.isoformat().replace(
                    "+00:00", "Z"
                ),
                "request_id": request_id,
                "request_kind": "HISTORICAL_NOTICE_DOCUMENT_STRUCTURE_PROBE",
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
        raise NoticeDocumentCaptureError(
            "CME notice-document duration ceiling is exceeded"
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
        source_release_ids=(str(authority["pagination_release_id"]),),
        embedded_documents={"capture_receipt.json": capture_receipt},
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture_receipt["capture_id"],
            "captured_at_utc": core["captured_at_utc"],
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
    load_document_probe_capture(receipt, boundary=publisher.boundary)
    return receipt


def load_document_probe_capture(
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
        raise IntegrityError("CME notice-document release is invalid")
    raw = receipt.embedded_document("capture_receipt.json", boundary)
    if not isinstance(raw, dict):
        raise IntegrityError("CME notice-document receipt is invalid")
    core = dict(raw)
    capture_id = core.pop("capture_id", None)
    authority = raw.get("authority")
    responses = raw.get("responses")
    if (
        type(capture_id) is not str
        or capture_id != sha256_json(core)
        or raw.get("schema_version") != CAPTURE_SCHEMA
        or raw.get("operation") != OPERATION
        or raw.get("request_count") != MAX_REQUESTS
        or not isinstance(authority, dict)
        or not isinstance(responses, list)
        or len(responses) != MAX_REQUESTS
        or manifest.source_release_ids
        != (str(authority.get("pagination_release_id")),)
        or manifest.metadata.get("capture_id") != capture_id
    ):
        raise IntegrityError("CME notice-document contract is invalid")
    total_bytes = 0
    for response, expected in zip(responses, PROBE_REQUESTS, strict=True):
        if (
            not isinstance(response, dict)
            or response.get("url") != expected["url"]
            or response.get("request_id") != expected["request_id"]
            or response.get("evidence_role") != expected["evidence_role"]
            or response.get("content_type") != "text/html"
            or response.get("status_code") != 200
            or type(response.get("logical_path")) is not str
        ):
            raise IntegrityError(
                "CME notice-document response contract is invalid"
            )
        physical = receipt.resolve_file(str(response["logical_path"]), boundary)
        if (
            physical.stat().st_size != response.get("size")
            or sha256_file(physical) != response.get("sha256")
        ):
            raise IntegrityError("CME notice-document response bytes changed")
        total_bytes += physical.stat().st_size
    if total_bytes != raw.get("total_bytes"):
        raise IntegrityError("CME notice-document total bytes are invalid")
    return dict(raw)
