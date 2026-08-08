"""Hash-bound pagination of relevant CME historical Notices metadata."""

from __future__ import annotations

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
from .calendar_notice_metadata import (
    SEARCH_REQUESTS,
    load_metadata_discovery_capture,
)
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import DataReleaseManifest, DataReleaseReceipt, PhasePublisher
from .errors import ContractError, IntegrityError, UnauthorizedOperation


ASSESSMENT_SCHEMA = "cme_historical_notice_semantic_assessment/1.0.0"
PLAN_SCHEMA = "cme_historical_notice_pagination_plan/1.0.0"
APPROVAL_SCHEMA = "cme_historical_notice_pagination_approval/1.0.0"
CAPTURE_SCHEMA = "cme_historical_notice_pagination_capture/1.0.0"
OPERATION = "CAPTURE_BOUNDED_PUBLIC_CME_HISTORICAL_NOTICE_PAGINATION"
RELEASE_KIND = "cme_historical_notice_pagination_capture"
EXPECTED_COUNTS = {
    "holiday": {"total_pages": 17, "total_results": 510},
    "trading-hours": {"total_pages": 31, "total_results": 924},
}
REUSED_REQUESTS = 2
NETWORK_REQUESTS = 46
TOTAL_RESPONSES = REUSED_REQUESTS + NETWORK_REQUESTS
MAX_TOTAL_BYTES = 67_108_864
MAX_DURATION_SECONDS = 600
REQUEST_TIMEOUT_SECONDS = 30
IMPLEMENTATION_PATHS = (
    "configs/data_layout_contract.json",
    "configs/exchange_calendar_policy.json",
    "configs/source_contract.json",
    "pyproject.toml",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/calendar_cli.py",
    "src/futures_rebuild/calendar_notice_client.py",
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
        "{request_id}-page-{page:03d}.json"
    ),
    "manifest_template": "manifests/data_releases/reference/{release_id}.json",
    "publication_lock": "state/locks/data-publication.lock",
    "staging_root": "state/data_publication_staging",
}
FORBIDDEN_ACTIONS = (
    "ACTIVATE_CALENDAR",
    "CALL_ANOTHER_CME_ENDPOINT_OR_QUERY",
    "CALL_DATABENTO_OR_ANY_NON_CME_PROVIDER",
    "CREATE_LINK_OR_MUTABLE_EXTERNAL_REFERENCE",
    "DELETE_OR_OVERWRITE_ACCEPTED_RELEASE",
    "DOWNLOAD_NOTICE_DOCUMENT_OR_ARCHIVE_ATTACHMENT",
    "FOLLOW_RESULT_LINK",
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
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PAGE_IDENTITY_OR_SEMANTIC_EVIDENCE_DRIFT",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
    "REUSED_PAGE_ZERO_HASH_OR_SIZE_MISMATCH",
)
_AUTHORITY_KEYS = {
    "assessment_id",
    "assessment_path",
    "assessment_sha256",
    "semantic_capture_id",
    "semantic_manifest_path",
    "semantic_manifest_sha256",
    "semantic_release_id",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_SAFE_HEADERS = frozenset(
    {"cache-control", "content-type", "date", "etag", "last-modified"}
)


class NoticePaginationCaptureError(UnauthorizedOperation):
    """Raised before or during bounded historical Notices pagination."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise NoticePaginationCaptureError(
            "CME historical Notices pagination rejected a redirect"
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
                f"CME Notices pagination implementation input is missing: {relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _payload(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("CME Notices semantic response is not JSON") from exc
    if not isinstance(payload, dict):
        raise IntegrityError("CME Notices semantic response is not an object")
    return payload


def _page_url(base_url: str, page: int) -> str:
    marker = ".ssfajax.0."
    if marker not in base_url:
        raise IntegrityError("CME Notices semantic URL lacks page marker")
    return base_url.replace(marker, f".ssfajax.{page}.", 1)


def build_semantic_assessment(
    *,
    semantic_manifest_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    manifest_path = boundary.assert_active_path(
        semantic_manifest_path,
        purpose="CME historical Notices semantic manifest",
        subtree="manifests/data_releases/reference",
    )
    receipt = DataReleaseReceipt.from_manifest(manifest_path, boundary)
    capture = load_metadata_discovery_capture(receipt, boundary=boundary)
    responses = capture.get("responses")
    if not isinstance(responses, list) or len(responses) != REUSED_REQUESTS:
        raise IntegrityError("CME Notices semantic capture is invalid")
    page_zero: list[dict[str, object]] = []
    for response, expected in zip(responses, SEARCH_REQUESTS, strict=True):
        if not isinstance(response, dict) or type(
            response.get("logical_path")
        ) is not str:
            raise IntegrityError("CME Notices semantic response is invalid")
        request_id = str(expected["request_id"])
        payload = _payload(
            receipt.resolve_file(str(response["logical_path"]), boundary)
        )
        expected_count = EXPECTED_COUNTS[request_id]
        results = payload.get("results")
        facets = payload.get("facets")
        if (
            set(payload)
            != {
                "currentPage",
                "facets",
                "hasMore",
                "results",
                "totalPages",
                "totalResults",
            }
            or payload.get("currentPage") != 0
            or payload.get("hasMore") is not True
            or payload.get("totalPages") != expected_count["total_pages"]
            or payload.get("totalResults") != expected_count["total_results"]
            or not isinstance(results, list)
            or len(results) != 30
            or not isinstance(facets, list)
        ):
            raise IntegrityError(
                "CME Notices semantic pagination counts changed"
            )
        page_zero.append(
            {
                "logical_path": response["logical_path"],
                "page": 0,
                "page_result_count": len(results),
                "query": expected["query"],
                "request_id": request_id,
                "sha256": response["sha256"],
                "size": response["size"],
                "total_pages": payload["totalPages"],
                "total_results": payload["totalResults"],
                "url": response["url"],
            }
        )
    core: dict[str, object] = {
        "classification": "FINITE_RELEVANT_NOTICE_METADATA_PAGINATION_READY",
        "forbidden_interpretations": [
            "METADATA_PAGINATION_DOES_NOT_AUTHORIZE_NOTICE_DOCUMENT_DOWNLOAD",
            "QUERY_UNION_MAY_OVERLAP_AND_MUST_BE_DEDUPLICATED_OFFLINE",
            "NOTICE_METADATA_IS_NOT_EXCHANGE_SESSION_SEGMENT_EVIDENCE",
        ],
        "network_request_count": NETWORK_REQUESTS,
        "next_authority": (
            "HASH_BOUND_CME_HISTORICAL_NOTICE_PAGINATION_CAPTURE_"
            "APPROVAL_REQUIRED"
        ),
        "page_zero_responses": page_zero,
        "reused_response_count": REUSED_REQUESTS,
        "schema_version": ASSESSMENT_SCHEMA,
        "semantic_capture_id": capture["capture_id"],
        "semantic_manifest_path": manifest_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "semantic_manifest_sha256": sha256_file(manifest_path),
        "semantic_release_id": receipt.release_id,
        "status": "EXACT_46_REQUEST_REMAINDER_READY",
        "total_response_count": TOTAL_RESPONSES,
    }
    return {**core, "assessment_id": sha256_json(core)}


def pagination_authority(
    *,
    semantic_manifest_path: Path,
    assessment_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    report_path = boundary.assert_active_path(
        assessment_path,
        purpose="CME historical Notices semantic assessment",
        subtree="reports/exchange_calendar",
    )
    expected = build_semantic_assessment(
        semantic_manifest_path=semantic_manifest_path,
        boundary=boundary,
    )
    assessment = _canonical_object(
        report_path, description="CME historical Notices semantic assessment"
    )
    if assessment != expected:
        raise IntegrityError(
            "CME historical Notices semantic assessment is invalid"
        )
    return {
        "assessment_id": assessment["assessment_id"],
        "assessment_path": report_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "assessment_sha256": sha256_file(report_path),
        "semantic_capture_id": assessment["semantic_capture_id"],
        "semantic_manifest_path": assessment["semantic_manifest_path"],
        "semantic_manifest_sha256": assessment["semantic_manifest_sha256"],
        "semantic_release_id": assessment["semantic_release_id"],
    }


def _validate_authority_shape(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError("CME Notices pagination authority schema is invalid")
    for key in _AUTHORITY_KEYS - {
        "assessment_path",
        "semantic_manifest_path",
    }:
        value = authority.get(key)
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise ContractError(
                "CME Notices pagination authority hash is invalid"
            )
    for key in ("assessment_path", "semantic_manifest_path"):
        if type(authority.get(key)) is not str:
            raise ContractError(
                "CME Notices pagination authority path is invalid"
            )


def _request_specs() -> list[dict[str, object]]:
    requests: list[dict[str, object]] = []
    for base, count in zip(
        SEARCH_REQUESTS,
        (EXPECTED_COUNTS["holiday"], EXPECTED_COUNTS["trading-hours"]),
        strict=True,
    ):
        for page in range(1, count["total_pages"]):
            requests.append(
                {
                    "accept": "application/json",
                    "page": page,
                    "query": base["query"],
                    "request_id": base["request_id"],
                    "request_kind": "HISTORICAL_NOTICE_PAGINATION",
                    "url": _page_url(str(base["url"]), page),
                }
            )
    if len(requests) != NETWORK_REQUESTS:
        raise ContractError("CME Notices pagination request count is invalid")
    return requests


def build_pagination_plan(
    *,
    authority: Mapping[str, object],
    assessment: Mapping[str, object],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    _validate_authority_shape(authority)
    if (
        assessment.get("assessment_id") != authority["assessment_id"]
        or assessment.get("network_request_count") != NETWORK_REQUESTS
        or assessment.get("reused_response_count") != REUSED_REQUESTS
        or assessment.get("total_response_count") != TOTAL_RESPONSES
        or not isinstance(assessment.get("page_zero_responses"), list)
    ):
        raise ContractError("CME Notices pagination assessment is invalid")
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
    ):
        raise ContractError(
            "CME Notices pagination implementation hashes are invalid"
        )
    scope: dict[str, object] = {
        "allow_redirects": False,
        "authority": dict(authority),
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "max_duration_seconds": MAX_DURATION_SECONDS,
        "max_requests": NETWORK_REQUESTS,
        "max_total_bytes": MAX_TOTAL_BYTES,
        "output_paths": dict(OUTPUT_PATHS),
        "purpose": "CAPTURE_COMPLETE_RELEVANT_NOTICE_METADATA_ONLY",
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "requests": _request_specs(),
        "retries": 0,
        "reuse_responses": assessment["page_zero_responses"],
        "stop_conditions": list(STOP_CONDITIONS),
        "total_response_count": TOTAL_RESPONSES,
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


def validate_pagination_plan(
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
        raise IntegrityError("CME Notices pagination plan schema is invalid")
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
        raise IntegrityError("CME Notices pagination plan identity is invalid")
    authority = scope.get("authority")
    implementation = scope.get("implementation_sha256")
    reuse = scope.get("reuse_responses")
    if (
        not isinstance(authority, dict)
        or not isinstance(implementation, dict)
        or not isinstance(reuse, list)
    ):
        raise IntegrityError("CME Notices pagination plan scope is invalid")
    assessment = {
        "assessment_id": authority.get("assessment_id"),
        "network_request_count": NETWORK_REQUESTS,
        "page_zero_responses": reuse,
        "reused_response_count": REUSED_REQUESTS,
        "total_response_count": TOTAL_RESPONSES,
    }
    expected = build_pagination_plan(
        authority=authority,
        assessment=assessment,
        implementation_sha256=implementation,
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME Notices pagination plan differs from bounded implementation"
        )
    return dict(payload)


def validate_pagination_approval(
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
        raise NoticePaginationCaptureError(
            "CME Notices pagination lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _safe_url(url: str) -> None:
    if url not in {str(item["url"]) for item in _request_specs()}:
        raise NoticePaginationCaptureError(
            "CME Notices pagination URL is outside the exact allowlist"
        )
    parsed = urllib.parse.urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise NoticePaginationCaptureError(
            "CME Notices pagination URL is unsafe"
        )


def _stage_response(
    *,
    stage: Path,
    request_id: str,
    page: int,
    body: bytes,
) -> tuple[str, str, Path]:
    staged_name = f"{request_id}-page-{page:03d}.json"
    staged = stage / staged_name
    staged.write_bytes(body)
    logical_path = f"data/reference/exchange_calendars/{staged_name}"
    return staged_name, logical_path, staged


def capture_pagination(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_pagination_plan(
        _canonical_object(plan_path, description="CME Notices pagination plan")
    )
    approval = _canonical_object(
        approval_path, description="CME Notices pagination approval"
    )
    approval_id = validate_pagination_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != implementation_hashes(
        publisher.boundary.active_root
    ):
        raise NoticePaginationCaptureError(
            "CME Notices pagination implementation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    semantic_manifest = (
        publisher.boundary.active_root
        / str(authority["semantic_manifest_path"])
    )
    derived = pagination_authority(
        semantic_manifest_path=semantic_manifest,
        assessment_path=publisher.boundary.active_root
        / str(authority["assessment_path"]),
        boundary=publisher.boundary,
    )
    if authority != derived:
        raise NoticePaginationCaptureError(
            "CME Notices pagination authority changed"
        )
    semantic_receipt = DataReleaseReceipt.from_manifest(
        semantic_manifest, publisher.boundary
    )
    reuse = scope["reuse_responses"]
    requests = scope["requests"]
    assert isinstance(reuse, list) and isinstance(requests, list)
    stage = publisher.create_stage("cme_notice_pagination")
    responses: list[dict[str, object]] = []
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    total_bytes = 0
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    for item in reuse:
        assert isinstance(item, dict)
        source = semantic_receipt.resolve_file(
            str(item["logical_path"]), publisher.boundary
        )
        if (
            source.stat().st_size != item["size"]
            or sha256_file(source) != item["sha256"]
        ):
            raise NoticePaginationCaptureError(
                "CME Notices reused page zero changed"
            )
        body = source.read_bytes()
        request_id = str(item["request_id"])
        staged_name, logical_path, staged = _stage_response(
            stage=stage,
            request_id=request_id,
            page=0,
            body=body,
        )
        logical_paths[staged_name] = logical_path
        staged_paths[logical_path] = staged_name
        total_bytes += len(body)
        responses.append(
            {
                "content_type": "application/json",
                "logical_path": logical_path,
                "page": 0,
                "query": item["query"],
                "request_id": request_id,
                "request_kind": "HISTORICAL_NOTICE_PAGINATION",
                "sha256": sha256_file(staged),
                "size": len(body),
                "source": "REUSED_IMMUTABLE",
                "source_logical_path": item["logical_path"],
                "source_release_id": semantic_receipt.release_id,
                "url": item["url"],
            }
        )
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    for spec in requests:
        assert isinstance(spec, dict)
        url = str(spec["url"])
        _safe_url(url)
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "futures-intraday-model-v2-calendar/1.0",
            },
            method="GET",
        )
        try:
            with opener.open(
                request, timeout=REQUEST_TIMEOUT_SECONDS
            ) as response:
                if response.status != 200 or response.geturl() != url:
                    raise NoticePaginationCaptureError(
                        "CME Notices pagination response is not exact HTTP 200"
                    )
                if response.headers.get_content_type() != "application/json":
                    raise NoticePaginationCaptureError(
                        "CME Notices pagination content type is unexpected"
                    )
                remaining = MAX_TOTAL_BYTES - total_bytes
                body = response.read(remaining + 1)
                if len(body) > remaining:
                    raise NoticePaginationCaptureError(
                        "CME Notices pagination byte ceiling is exceeded"
                    )
                safe_headers = {
                    key.lower(): value
                    for key, value in response.headers.items()
                    if key.lower() in _SAFE_HEADERS
                }
        except NoticePaginationCaptureError:
            raise
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
        ) as exc:
            raise NoticePaginationCaptureError(
                "CME Notices pagination request failed before publication"
            ) from exc
        request_id = str(spec["request_id"])
        page = int(spec["page"])
        staged_name, logical_path, staged = _stage_response(
            stage=stage,
            request_id=request_id,
            page=page,
            body=body,
        )
        logical_paths[staged_name] = logical_path
        staged_paths[logical_path] = staged_name
        total_bytes += len(body)
        responses.append(
            {
                "content_type": "application/json",
                "logical_path": logical_path,
                "page": page,
                "query": spec["query"],
                "received_at_utc": captured_at.isoformat().replace(
                    "+00:00", "Z"
                ),
                "request_id": request_id,
                "request_kind": "HISTORICAL_NOTICE_PAGINATION",
                "safe_headers": dict(sorted(safe_headers.items())),
                "sha256": sha256_file(staged),
                "size": len(body),
                "source": "NETWORK",
                "status_code": 200,
                "url": url,
            }
        )
    elapsed_milliseconds = int(
        (monotonic_time.monotonic() - started) * 1000
    )
    if elapsed_milliseconds > MAX_DURATION_SECONDS * 1000:
        raise NoticePaginationCaptureError(
            "CME Notices pagination duration ceiling is exceeded"
        )
    responses.sort(key=lambda item: (str(item["request_id"]), int(item["page"])))
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "authority": authority,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": MAX_DURATION_SECONDS,
            "max_requests": NETWORK_REQUESTS,
            "max_total_bytes": MAX_TOTAL_BYTES,
            "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "retries": 0,
            "workers": 1,
        },
        "capture_approval": dict(approval),
        "captured_at_utc": captured_at.isoformat().replace("+00:00", "Z"),
        "elapsed_milliseconds": elapsed_milliseconds,
        "network_request_count": NETWORK_REQUESTS,
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "responses": responses,
        "reused_response_count": REUSED_REQUESTS,
        "schema_version": CAPTURE_SCHEMA,
        "total_bytes": total_bytes,
        "total_response_count": TOTAL_RESPONSES,
    }
    capture_receipt = {**core, "capture_id": sha256_json(core)}
    manifest = DataReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=RELEASE_KIND,
        schema_version=CAPTURE_SCHEMA,
        logical_paths=logical_paths,
        source_release_ids=(semantic_receipt.release_id,),
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
    load_pagination_capture(receipt, boundary=publisher.boundary)
    return receipt


def load_pagination_capture(
    receipt: DataReleaseReceipt,
    *,
    boundary: RepoBoundary,
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "reference"
        or manifest.release_kind != RELEASE_KIND
        or manifest.schema_version != CAPTURE_SCHEMA
        or len(manifest.files) != TOTAL_RESPONSES
        or set(manifest.embedded_documents) != {"capture_receipt.json"}
    ):
        raise IntegrityError("CME Notices pagination release is invalid")
    raw = receipt.embedded_document("capture_receipt.json", boundary)
    if not isinstance(raw, dict):
        raise IntegrityError("CME Notices pagination receipt is invalid")
    core = dict(raw)
    capture_id = core.pop("capture_id", None)
    authority = raw.get("authority")
    responses = raw.get("responses")
    if (
        type(capture_id) is not str
        or capture_id != sha256_json(core)
        or raw.get("schema_version") != CAPTURE_SCHEMA
        or raw.get("operation") != OPERATION
        or raw.get("network_request_count") != NETWORK_REQUESTS
        or raw.get("reused_response_count") != REUSED_REQUESTS
        or raw.get("total_response_count") != TOTAL_RESPONSES
        or not isinstance(authority, dict)
        or not isinstance(responses, list)
        or len(responses) != TOTAL_RESPONSES
        or manifest.source_release_ids
        != (str(authority.get("semantic_release_id")),)
        or manifest.metadata.get("capture_id") != capture_id
    ):
        raise IntegrityError("CME Notices pagination contract is invalid")
    total_bytes = 0
    identities: set[tuple[str, int]] = set()
    for response in responses:
        if (
            not isinstance(response, dict)
            or response.get("request_kind") != "HISTORICAL_NOTICE_PAGINATION"
            or response.get("content_type") != "application/json"
            or type(response.get("request_id")) is not str
            or type(response.get("page")) is not int
            or response.get("source") not in {"NETWORK", "REUSED_IMMUTABLE"}
            or type(response.get("logical_path")) is not str
        ):
            raise IntegrityError(
                "CME Notices pagination response contract is invalid"
            )
        identity = (str(response["request_id"]), int(response["page"]))
        if identity in identities:
            raise IntegrityError("CME Notices pagination page is duplicated")
        identities.add(identity)
        physical = receipt.resolve_file(str(response["logical_path"]), boundary)
        if (
            physical.stat().st_size != response.get("size")
            or sha256_file(physical) != response.get("sha256")
        ):
            raise IntegrityError("CME Notices pagination response bytes changed")
        total_bytes += physical.stat().st_size
    expected_identities = {
        ("holiday", page) for page in range(17)
    } | {("trading-hours", page) for page in range(31)}
    if identities != expected_identities or total_bytes != raw.get("total_bytes"):
        raise IntegrityError("CME Notices pagination coverage is invalid")
    return dict(raw)
