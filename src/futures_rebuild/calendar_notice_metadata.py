"""Bounded semantic discovery over CME historical Notices metadata."""

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
from .calendar_notice_search import (
    CAPABILITY_URL,
    load_notice_search_capability_capture,
)
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import DataReleaseManifest, DataReleaseReceipt, PhasePublisher
from .errors import ContractError, IntegrityError, UnauthorizedOperation


ASSESSMENT_SCHEMA = "cme_historical_notice_capability_assessment/1.0.0"
PLAN_SCHEMA = "cme_historical_notice_semantic_discovery_plan/1.0.0"
APPROVAL_SCHEMA = "cme_historical_notice_semantic_discovery_approval/1.0.0"
CAPTURE_SCHEMA = "cme_historical_notice_semantic_discovery_capture/1.0.0"
OPERATION = "CAPTURE_BOUNDED_PUBLIC_CME_HISTORICAL_NOTICE_SEMANTIC_DISCOVERY"
RELEASE_KIND = "cme_historical_notice_semantic_discovery_capture"
SEARCH_REQUESTS = (
    {
        "query": "holiday",
        "request_id": "holiday",
        "url": f"{CAPABILITY_URL}?search=holiday",
    },
    {
        "query": "trading hours",
        "request_id": "trading-hours",
        "url": f"{CAPABILITY_URL}?search=trading%20hours",
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
    "src/futures_rebuild/calendar_notice_metadata.py",
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
        "{ordinal:03d}-{request_id}.json"
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
    "FOLLOW_PAGINATION_OR_RESULT_LINK",
    "PARSE_OR_ACCEPT_HISTORICAL_CALENDAR",
    "REBUILD_FOUNDATION",
    "RETRY_OR_REDIRECT_REQUEST",
    "USE_CREDENTIAL_OR_SECRET",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "BYTE_OR_DURATION_CEILING_REACHED",
    "CAPABILITY_EVIDENCE_OR_QUERY_DRIFT",
    "CONTENT_TYPE_OR_HTTP_STATUS_MISMATCH",
    "IMPLEMENTATION_HASH_DRIFT",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
_AUTHORITY_KEYS = {
    "assessment_id",
    "assessment_path",
    "assessment_sha256",
    "capability_capture_id",
    "capability_manifest_path",
    "capability_manifest_sha256",
    "capability_release_id",
    "capability_response_sha256",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_SAFE_HEADERS = frozenset(
    {"cache-control", "content-type", "date", "etag", "last-modified"}
)


class NoticeMetadataCaptureError(UnauthorizedOperation):
    """Raised before or during bounded semantic metadata discovery."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise NoticeMetadataCaptureError(
            "CME historical Notices metadata capture rejected a redirect"
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
                f"CME Notices metadata implementation input is missing: {relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _capability_payload(
    receipt: DataReleaseReceipt,
    *,
    boundary: RepoBoundary,
) -> tuple[dict[str, object], dict[str, object]]:
    capture = load_notice_search_capability_capture(receipt, boundary=boundary)
    response = capture.get("response")
    if not isinstance(response, dict) or type(
        response.get("logical_path")
    ) is not str:
        raise IntegrityError("CME historical Notices capability is invalid")
    physical = receipt.resolve_file(str(response["logical_path"]), boundary)
    try:
        payload = json.loads(physical.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(
            "CME historical Notices capability response is not JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise IntegrityError(
            "CME historical Notices capability response is not an object"
        )
    return capture, payload


def build_capability_assessment(
    *,
    capability_manifest_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    manifest_path = boundary.assert_active_path(
        capability_manifest_path,
        purpose="CME historical Notices capability manifest",
        subtree="manifests/data_releases/reference",
    )
    receipt = DataReleaseReceipt.from_manifest(manifest_path, boundary)
    capture, payload = _capability_payload(receipt, boundary=boundary)
    response = capture["response"]
    assert isinstance(response, dict)
    expected_keys = {
        "currentPage",
        "facets",
        "hasMore",
        "results",
        "totalPages",
        "totalResults",
    }
    results = payload.get("results")
    facets = payload.get("facets")
    if (
        set(payload) != expected_keys
        or payload.get("currentPage") != 0
        or payload.get("hasMore") is not True
        or type(payload.get("totalPages")) is not int
        or int(payload["totalPages"]) <= 1
        or type(payload.get("totalResults")) is not int
        or int(payload["totalResults"]) <= 0
        or not isinstance(results, list)
        or len(results) == 0
        or not isinstance(facets, list)
        or any(
            not isinstance(item, dict)
            or set(item) != {"text", "url"}
            or type(item["text"]) is not str
            or type(item["url"]) is not str
            for item in results
        )
        or any(
            not isinstance(item, dict)
            or set(item) != {"count", "tagId"}
            or type(item["count"]) is not int
            or type(item["tagId"]) is not str
            for item in facets
        )
    ):
        raise IntegrityError(
            "CME historical Notices capability payload is unexpected"
        )
    core: dict[str, object] = {
        "capability_capture_id": capture["capture_id"],
        "capability_manifest_path": manifest_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "capability_manifest_sha256": sha256_file(manifest_path),
        "capability_release_id": receipt.release_id,
        "capability_response_sha256": response["sha256"],
        "classification": "FINITE_BUT_BROAD_HISTORICAL_NOTICE_RESULT_SET",
        "current_page": payload["currentPage"],
        "facet_count": len(facets),
        "forbidden_interpretations": [
            "CAPABILITY_RESULTS_DO_NOT_AUTHORIZE_PAGINATION_OR_RESULT_LINKS",
            "NOTICE_METADATA_IS_NOT_EXCHANGE_SESSION_SEGMENT_EVIDENCE",
            "SEMANTIC_QUERY_COUNTS_MUST_BE_CAPTURED_BEFORE_BROAD_DOWNLOAD",
        ],
        "has_more": payload["hasMore"],
        "next_authority": (
            "HASH_BOUND_CME_HISTORICAL_NOTICE_SEMANTIC_DISCOVERY_"
            "CAPTURE_APPROVAL_REQUIRED"
        ),
        "proposed_queries": [dict(item) for item in SEARCH_REQUESTS],
        "result_count_on_page": len(results),
        "schema_version": ASSESSMENT_SCHEMA,
        "status": "TWO_EXACT_PAGE_ZERO_SEMANTIC_QUERIES_READY",
        "total_pages": payload["totalPages"],
        "total_results": payload["totalResults"],
    }
    return {**core, "assessment_id": sha256_json(core)}


def metadata_authority(
    *,
    capability_manifest_path: Path,
    assessment_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    report_path = boundary.assert_active_path(
        assessment_path,
        purpose="CME historical Notices capability assessment",
        subtree="reports/exchange_calendar",
    )
    expected = build_capability_assessment(
        capability_manifest_path=capability_manifest_path,
        boundary=boundary,
    )
    assessment = _canonical_object(
        report_path, description="CME historical Notices capability assessment"
    )
    if assessment != expected:
        raise IntegrityError(
            "CME historical Notices capability assessment is invalid"
        )
    return {
        "assessment_id": assessment["assessment_id"],
        "assessment_path": report_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "assessment_sha256": sha256_file(report_path),
        "capability_capture_id": assessment["capability_capture_id"],
        "capability_manifest_path": assessment["capability_manifest_path"],
        "capability_manifest_sha256": assessment[
            "capability_manifest_sha256"
        ],
        "capability_release_id": assessment["capability_release_id"],
        "capability_response_sha256": assessment[
            "capability_response_sha256"
        ],
    }


def _validate_authority_shape(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError("CME Notices metadata authority schema is invalid")
    for key in _AUTHORITY_KEYS - {
        "assessment_path",
        "capability_manifest_path",
    }:
        value = authority.get(key)
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise ContractError("CME Notices metadata authority hash is invalid")
    for key in ("assessment_path", "capability_manifest_path"):
        if type(authority.get(key)) is not str:
            raise ContractError("CME Notices metadata authority path is invalid")


def build_metadata_discovery_plan(
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
            "CME Notices metadata implementation hashes are invalid"
        )
    requests = [
        {
            "accept": "application/json",
            "query": item["query"],
            "request_id": item["request_id"],
            "request_kind": "HISTORICAL_NOTICE_SEMANTIC_DISCOVERY",
            "url": item["url"],
        }
        for item in SEARCH_REQUESTS
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
        "purpose": "DISCOVER_RELEVANT_HISTORICAL_NOTICE_RESULT_COUNTS_ONLY",
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


def validate_metadata_discovery_plan(
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
        raise IntegrityError("CME Notices metadata plan schema is invalid")
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
        raise IntegrityError("CME Notices metadata plan identity is invalid")
    authority = scope.get("authority")
    implementation = scope.get("implementation_sha256")
    if not isinstance(authority, dict) or not isinstance(implementation, dict):
        raise IntegrityError("CME Notices metadata plan scope is invalid")
    expected = build_metadata_discovery_plan(
        authority=authority,
        implementation_sha256=implementation,
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME Notices metadata plan differs from bounded implementation"
        )
    return dict(payload)


def validate_metadata_discovery_approval(
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
        raise NoticeMetadataCaptureError(
            "CME Notices metadata capture lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _safe_url(url: str) -> None:
    expected = {str(item["url"]) for item in SEARCH_REQUESTS}
    parsed = urllib.parse.urlparse(url)
    if (
        url not in expected
        or parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != urllib.parse.urlparse(CAPABILITY_URL).path
        or parsed.fragment
    ):
        raise NoticeMetadataCaptureError(
            "CME Notices metadata URL is outside the exact allowlist"
        )


def capture_metadata_discovery(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_metadata_discovery_plan(
        _canonical_object(plan_path, description="CME Notices metadata plan")
    )
    approval = _canonical_object(
        approval_path, description="CME Notices metadata approval"
    )
    approval_id = validate_metadata_discovery_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != implementation_hashes(
        publisher.boundary.active_root
    ):
        raise NoticeMetadataCaptureError(
            "CME Notices metadata implementation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived = metadata_authority(
        capability_manifest_path=publisher.boundary.active_root
        / str(authority["capability_manifest_path"]),
        assessment_path=publisher.boundary.active_root
        / str(authority["assessment_path"]),
        boundary=publisher.boundary,
    )
    if authority != derived:
        raise NoticeMetadataCaptureError(
            "CME Notices metadata authority changed"
        )
    request_specs = scope["requests"]
    assert isinstance(request_specs, list)
    for spec in request_specs:
        assert isinstance(spec, dict)
        _safe_url(str(spec["url"]))
    stage = publisher.create_stage("cme_notice_semantic_discovery")
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    captured_at = datetime.now(timezone.utc).replace(microsecond=0)
    responses: list[dict[str, object]] = []
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    total_bytes = 0
    for ordinal, spec in enumerate(request_specs, start=1):
        request_id = str(spec["request_id"])
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
            with opener.open(
                request, timeout=REQUEST_TIMEOUT_SECONDS
            ) as response:
                if response.status != 200 or response.geturl() != url:
                    raise NoticeMetadataCaptureError(
                        "CME Notices metadata response is not exact HTTP 200"
                    )
                if response.headers.get_content_type() != "application/json":
                    raise NoticeMetadataCaptureError(
                        "CME Notices metadata content type is unexpected"
                    )
                remaining = MAX_TOTAL_BYTES - total_bytes
                body = response.read(remaining + 1)
                if len(body) > remaining:
                    raise NoticeMetadataCaptureError(
                        "CME Notices metadata byte ceiling is exceeded"
                    )
                safe_headers = {
                    key.lower(): value
                    for key, value in response.headers.items()
                    if key.lower() in _SAFE_HEADERS
                }
        except NoticeMetadataCaptureError:
            raise
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
        ) as exc:
            raise NoticeMetadataCaptureError(
                "CME Notices metadata request failed before publication"
            ) from exc
        total_bytes += len(body)
        staged_name = f"{ordinal:03d}-{request_id}.json"
        staged = stage / staged_name
        staged.write_bytes(body)
        logical_path = (
            f"data/reference/exchange_calendars/{ordinal:03d}-"
            f"{request_id}.json"
        )
        logical_paths[staged_name] = logical_path
        staged_paths[logical_path] = staged_name
        responses.append(
            {
                "content_type": "application/json",
                "logical_path": logical_path,
                "query": spec["query"],
                "received_at_utc": captured_at.isoformat().replace(
                    "+00:00", "Z"
                ),
                "request_id": request_id,
                "request_kind": "HISTORICAL_NOTICE_SEMANTIC_DISCOVERY",
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
        raise NoticeMetadataCaptureError(
            "CME Notices metadata duration ceiling is exceeded"
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
        source_release_ids=(str(authority["capability_release_id"]),),
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
    load_metadata_discovery_capture(receipt, boundary=publisher.boundary)
    return receipt


def load_metadata_discovery_capture(
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
        raise IntegrityError("CME Notices metadata release is invalid")
    raw = receipt.embedded_document("capture_receipt.json", boundary)
    if not isinstance(raw, dict):
        raise IntegrityError("CME Notices metadata receipt is invalid")
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
        != (str(authority.get("capability_release_id")),)
        or manifest.metadata.get("capture_id") != capture_id
    ):
        raise IntegrityError("CME Notices metadata contract is invalid")
    total_bytes = 0
    for response, expected in zip(responses, SEARCH_REQUESTS, strict=True):
        if (
            not isinstance(response, dict)
            or response.get("url") != expected["url"]
            or response.get("query") != expected["query"]
            or response.get("request_id") != expected["request_id"]
            or response.get("content_type") != "application/json"
            or response.get("status_code") != 200
            or type(response.get("logical_path")) is not str
        ):
            raise IntegrityError(
                "CME Notices metadata response contract is invalid"
            )
        physical = receipt.resolve_file(str(response["logical_path"]), boundary)
        if (
            physical.stat().st_size != response.get("size")
            or sha256_file(physical) != response.get("sha256")
        ):
            raise IntegrityError("CME Notices metadata response bytes changed")
        total_bytes += physical.stat().st_size
    if total_bytes != raw.get("total_bytes"):
        raise IntegrityError("CME Notices metadata total bytes are invalid")
    return dict(raw)
