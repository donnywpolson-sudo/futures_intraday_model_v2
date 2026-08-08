"""Hash-bound capture of the exact CME notice attachments found offline."""

from __future__ import annotations

import hashlib
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
from .calendar_notice_attachments import (
    ASSESSMENT_SCHEMA,
    build_attachment_assessment,
)
from .canonical import canonical_bytes, fsync_directory, sha256_file, sha256_json
from .data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
    verify_data_release_manifest,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation


PLAN_SCHEMA = "cme_historical_notice_attachment_capture_plan/1.0.0"
APPROVAL_SCHEMA = "cme_historical_notice_attachment_capture_approval/1.0.0"
CAPTURE_SCHEMA = "cme_historical_notice_attachment_capture/1.0.0"
PREDECESSOR_FAILURE_SCHEMA = (
    "cme_historical_notice_attachment_capture_failure/1.0.0"
)
FAILURE_SCHEMA = "cme_historical_notice_attachment_capture_failure/1.1.0"
OPERATION = "CAPTURE_BOUNDED_PUBLIC_CME_HISTORICAL_NOTICE_ATTACHMENTS"
RELEASE_KIND = "cme_historical_notice_attachment_capture"
MAX_REQUESTS = 797
MAX_RESPONSE_BYTES = 16_777_216
MAX_TOTAL_BYTES = 4_294_967_296
MAX_DURATION_SECONDS = 5_400
REQUEST_TIMEOUT_SECONDS = 45
WORKERS = 2
VERIFY_WORKERS = 16
IMPLEMENTATION_PATHS = (
    "configs/data_layout_contract.json",
    "configs/exchange_calendar_policy.json",
    "configs/source_contract.json",
    "pyproject.toml",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/calendar_cli.py",
    "src/futures_rebuild/calendar_notice_attachment_capture.py",
    "src/futures_rebuild/calendar_notice_attachments.py",
    "src/futures_rebuild/calendar_notice_union_recovery.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/source_contract.py",
)
FORBIDDEN_ACTIONS = (
    "ACTIVATE_CALENDAR",
    "CALL_ANOTHER_CME_ENDPOINT_OR_UNLISTED_ATTACHMENT",
    "CALL_DATABENTO_OR_ANY_NON_CME_PROVIDER",
    "CREATE_LINK_OR_MUTABLE_EXTERNAL_REFERENCE",
    "DELETE_OR_OVERWRITE_ACCEPTED_RELEASE",
    "EXECUTE_DOCUMENT_CONTENT_OR_FOLLOW_EMBEDDED_LINK",
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
    "OFFLINE_ASSESSMENT_OR_UNION_RELEASE_DRIFT",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PRIOR_PLAN_OUTCOME_EXISTS",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_SAFE_HEADERS = frozenset(
    {
        "cache-control",
        "content-disposition",
        "content-length",
        "content-type",
        "date",
        "etag",
        "last-modified",
    }
)
_CONTENT_TYPES = {
    ".csv": (
        "application/csv",
        "application/octet-stream",
        "application/vnd.ms-excel",
        "text/csv",
        "text/plain",
    ),
    ".pdf": (
        "application/octet-stream",
        "application/pdf",
    ),
    ".xls": (
        "application/octet-stream",
        "application/vnd.ms-excel",
    ),
}
_AUTHORITY_KEYS = {
    "assessment_id",
    "assessment_path",
    "assessment_sha256",
    "union_capture_id",
    "union_manifest_path",
    "union_manifest_sha256",
    "union_release_id",
}


class NoticeAttachmentCaptureError(UnauthorizedOperation):
    """Raised before or during the exact CME attachment capture."""


class NoticeAttachmentRequestError(NoticeAttachmentCaptureError):
    """A request failure with only bounded, non-sensitive evidence."""

    def __init__(
        self,
        message: str,
        *,
        failure_code: str,
        safe_details: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.failure_code = failure_code
        self.safe_details = dict(sorted((safe_details or {}).items()))

    def evidence(self) -> dict[str, object]:
        return {
            "error_class": type(self).__name__,
            "failure_code": self.failure_code,
            "safe_details": self.safe_details,
        }


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise NoticeAttachmentRequestError(
            "CME notice-attachment capture rejected an HTTP redirect",
            failure_code="HTTP_REDIRECT_REJECTED",
            safe_details={"http_status": int(code)},
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
                "CME notice-attachment implementation input is missing: "
                f"{relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def attachment_authority(
    *,
    assessment_path: Path,
    union_manifest_path: Path,
    boundary: RepoBoundary,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    boundary.assert_active_path(
        assessment_path,
        purpose="CME notice attachment assessment",
        subtree="reports/exchange_calendar",
    )
    boundary.assert_active_path(
        union_manifest_path,
        purpose="accepted CME notice union manifest",
        subtree="manifests/data_releases/reference",
    )
    assessment = _canonical_object(
        assessment_path,
        description="CME notice attachment assessment",
    )
    rebuilt = build_attachment_assessment(
        union_manifest_path=union_manifest_path,
        boundary=boundary,
    )
    core = dict(assessment)
    assessment_id = core.pop("assessment_id", None)
    candidates = assessment.get("attachment_candidates")
    if (
        assessment != rebuilt
        or assessment.get("schema_version") != ASSESSMENT_SCHEMA
        or assessment.get("status")
        != "COMPLETE_OFFLINE_ATTACHMENT_DISCOVERY"
        or type(assessment_id) is not str
        or assessment_id != sha256_json(core)
        or assessment.get("attachment_candidate_count") != MAX_REQUESTS
        or not isinstance(candidates, list)
        or len(candidates) != MAX_REQUESTS
        or assessment.get("union_manifest_path")
        != union_manifest_path.relative_to(
            boundary.active_root
        ).as_posix()
        or assessment.get("union_manifest_sha256")
        != sha256_file(union_manifest_path)
    ):
        raise IntegrityError(
            "CME notice attachment assessment authority is invalid"
        )
    authority = {
        "assessment_id": assessment_id,
        "assessment_path": assessment_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "assessment_sha256": sha256_file(assessment_path),
        "union_capture_id": assessment["union_capture_id"],
        "union_manifest_path": assessment["union_manifest_path"],
        "union_manifest_sha256": assessment["union_manifest_sha256"],
        "union_release_id": assessment["union_release_id"],
    }
    _validate_authority(authority)
    return authority, [dict(item) for item in candidates if isinstance(item, dict)]


def _validate_authority(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError(
            "CME notice-attachment authority schema is invalid"
        )
    for key, value in authority.items():
        if key.endswith("_id") or key.endswith("_sha256"):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise ContractError(
                    "CME notice-attachment authority hash is invalid"
                )
        elif type(value) is not str or not value:
            raise ContractError(
                "CME notice-attachment authority path is invalid"
            )


def _requests(
    candidates: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    if len(candidates) != MAX_REQUESTS:
        raise ContractError(
            "CME notice-attachment candidate count is invalid"
        )
    requests: list[dict[str, object]] = []
    previous_url = ""
    for ordinal, candidate in enumerate(candidates, start=1):
        url = candidate.get("url")
        extension = candidate.get("extension")
        reasons = candidate.get("discovery_reasons")
        link_texts = candidate.get("link_texts")
        request_ids = candidate.get("source_notice_request_ids")
        notice_urls = candidate.get("source_notice_urls")
        titles = candidate.get("source_titles")
        if (
            type(url) is not str
            or url <= previous_url
            or type(extension) is not str
            or extension not in _CONTENT_TYPES
            or not isinstance(reasons, list)
            or not reasons
            or any(type(item) is not str for item in reasons)
            or not isinstance(link_texts, list)
            or any(type(item) is not str for item in link_texts)
            or not isinstance(request_ids, list)
            or not request_ids
            or any(type(item) is not str for item in request_ids)
            or not isinstance(notice_urls, list)
            or not notice_urls
            or any(type(item) is not str for item in notice_urls)
            or not isinstance(titles, list)
            or not titles
            or any(type(item) is not str for item in titles)
        ):
            raise ContractError(
                "CME notice-attachment candidate ordering is invalid"
            )
        previous_url = url
        parsed = urllib.parse.urlparse(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "www.cmegroup.com"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or Path(parsed.path).suffix.lower() != extension
        ):
            raise ContractError(
                "CME notice-attachment candidate URL is invalid"
            )
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
        request_id = f"attachment-{ordinal:04d}-{digest}"
        requests.append(
            {
                "accept": ", ".join(_CONTENT_TYPES[extension]),
                "discovery_reasons": list(reasons),
                "expected_content_types": list(
                    _CONTENT_TYPES[extension]
                ),
                "extension": extension,
                "link_texts": list(link_texts),
                "logical_path": (
                    "data/reference/exchange_calendars/"
                    f"{request_id}{extension}"
                ),
                "ordinal": ordinal,
                "request_id": request_id,
                "request_kind": (
                    "HISTORICAL_NOTICE_ATTACHMENT_CAPTURE"
                ),
                "source_notice_request_ids": list(request_ids),
                "source_notice_urls": list(notice_urls),
                "source_titles": list(titles),
                "url": url,
            }
        )
    return requests


def build_attachment_capture_plan(
    *,
    authority: Mapping[str, object],
    candidates: Sequence[Mapping[str, object]],
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
        raise ContractError(
            "CME notice-attachment implementation hashes are invalid"
        )
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
                "{request_id}{extension}"
            ),
            "failure_report": (
                "reports/exchange_calendar/"
                "cme_historical_notice_attachment_capture_failure_"
                "{plan_prefix}.json"
            ),
            "manifest_template": (
                "manifests/data_releases/reference/{release_id}.json"
            ),
            "publication_lock": "state/locks/data-publication.lock",
            "staging_root": "state/data_publication_staging",
        },
        "purpose": (
            "CAPTURE_EXACT_OFFLINE_DISCOVERED_CME_NOTICE_ATTACHMENTS_"
            "BEFORE_ANY_CALENDAR_PARSE"
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


def validate_attachment_capture_plan(
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
        raise IntegrityError(
            "CME notice-attachment plan schema is invalid"
        )
    core = {key: value for key, value in payload.items() if key != "plan_id"}
    scope = payload.get("scope")
    if (
        payload.get("schema_version") != PLAN_SCHEMA
        or payload.get("classification")
        != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload.get("execution_authorized") is not False
        or payload.get("operation") != OPERATION
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
    ):
        raise IntegrityError(
            "CME notice-attachment plan identity is invalid"
        )
    authority = scope.get("authority")
    implementation = scope.get("implementation_sha256")
    requests = scope.get("requests")
    if (
        not isinstance(authority, dict)
        or not isinstance(implementation, dict)
        or not isinstance(requests, list)
    ):
        raise IntegrityError(
            "CME notice-attachment plan scope is invalid"
        )
    candidates = []
    for request in requests:
        if not isinstance(request, dict):
            raise IntegrityError(
                "CME notice-attachment request schema is invalid"
            )
        candidates.append(
            {
                "discovery_reasons": request.get("discovery_reasons"),
                "extension": request.get("extension"),
                "link_texts": request.get("link_texts"),
                "source_notice_request_ids": request.get(
                    "source_notice_request_ids"
                ),
                "source_notice_urls": request.get("source_notice_urls"),
                "source_titles": request.get("source_titles"),
                "url": request.get("url"),
            }
        )
    expected = build_attachment_capture_plan(
        authority=authority,
        candidates=candidates,
        implementation_sha256=implementation,
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME notice-attachment plan differs from bounded implementation"
        )
    return dict(payload)


def validate_attachment_capture_approval(
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
        raise NoticeAttachmentCaptureError(
            "CME notice-attachment capture lacks exact hash-bound approval"
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
        raise NoticeAttachmentCaptureError(
            "CME notice-attachment URL is outside the exact allowlist"
        )


def _failure_path(root: Path, plan_id: str) -> Path:
    return (
        root
        / "reports"
        / "exchange_calendar"
        / (
            "cme_historical_notice_attachment_capture_failure_"
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
) -> tuple[bytes, str, dict[str, str], str]:
    url = str(spec["url"])
    _safe_url(url, allowed=allowed)
    request = urllib.request.Request(
        url,
        headers={
            "Accept": str(spec["accept"]),
            "User-Agent": "futures-intraday-model-v2-calendar/1.0",
        },
        method="GET",
    )
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        with opener.open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            if response.status != 200:
                raise NoticeAttachmentRequestError(
                    "CME notice-attachment response is not HTTP 200",
                    failure_code="HTTP_STATUS_REJECTED",
                    safe_details={"http_status": int(response.status)},
                )
            if response.geturl() != url:
                raise NoticeAttachmentRequestError(
                    "CME notice-attachment response URL changed",
                    failure_code="RESPONSE_URL_MISMATCH",
                    safe_details={
                        "response_url_sha256": hashlib.sha256(
                            str(response.geturl()).encode("utf-8")
                        ).hexdigest()
                    },
                )
            content_type = response.headers.get_content_type().lower()
            expected = spec.get("expected_content_types")
            if (
                not isinstance(expected, list)
                or content_type not in expected
            ):
                raise NoticeAttachmentRequestError(
                    "CME notice-attachment content type is unexpected",
                    failure_code="MIME_TYPE_REJECTED",
                    safe_details={
                        "content_type": content_type,
                        "expected_content_types": (
                            sorted(str(item) for item in expected)
                            if isinstance(expected, list)
                            else []
                        ),
                    },
                )
            body = response.read(MAX_RESPONSE_BYTES + 1)
            if len(body) > MAX_RESPONSE_BYTES:
                raise NoticeAttachmentRequestError(
                    "CME notice-attachment response byte ceiling is exceeded",
                    failure_code="RESPONSE_BYTE_CEILING_REACHED",
                    safe_details={
                        "observed_size_lower_bound": len(body),
                    },
                )
            safe_headers = {
                key.lower(): value
                for key, value in response.headers.items()
                if key.lower() in _SAFE_HEADERS
            }
    except NoticeAttachmentRequestError:
        raise
    except urllib.error.HTTPError as exc:
        content_type = (
            exc.headers.get_content_type().lower()
            if exc.headers is not None
            else "UNAVAILABLE"
        )
        raise NoticeAttachmentRequestError(
            "CME notice-attachment HTTP request was rejected",
            failure_code="HTTP_STATUS_REJECTED",
            safe_details={
                "content_type": content_type,
                "http_status": int(exc.code),
            },
        ) from exc
    except TimeoutError as exc:
        raise NoticeAttachmentRequestError(
            "CME notice-attachment request timed out",
            failure_code="REQUEST_TIMEOUT",
            safe_details={},
        ) from exc
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", None)
        if isinstance(reason, TimeoutError):
            raise NoticeAttachmentRequestError(
                "CME notice-attachment request timed out",
                failure_code="REQUEST_TIMEOUT",
                safe_details={},
            ) from exc
        raise NoticeAttachmentRequestError(
            "CME notice-attachment network request failed",
            failure_code="NETWORK_ERROR",
            safe_details={
                "reason_class": (
                    type(reason).__name__
                    if reason is not None
                    else "UNAVAILABLE"
                )
            },
        ) from exc
    received = datetime.now(timezone.utc).replace(microsecond=0)
    return (
        body,
        content_type,
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


def _failure_evidence(
    exc: Exception,
    *,
    spec: Mapping[str, object],
) -> dict[str, object]:
    if isinstance(exc, NoticeAttachmentRequestError):
        evidence = exc.evidence()
    else:
        evidence = {
            "error_class": type(exc).__name__,
            "failure_code": "UNCLASSIFIED_INTERNAL_FAILURE",
            "safe_details": {},
        }
    return {
        **evidence,
        "request_id": spec["request_id"],
        "url": spec["url"],
    }


def capture_attachments(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_attachment_capture_plan(
        _canonical_object(
            plan_path,
            description="CME notice-attachment plan",
        )
    )
    approval = _canonical_object(
        approval_path,
        description="CME notice-attachment approval",
    )
    approval_id = validate_attachment_capture_approval(
        approval,
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    root = publisher.boundary.active_root
    if scope["implementation_sha256"] != implementation_hashes(root):
        raise NoticeAttachmentCaptureError(
            "CME notice-attachment implementation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived, candidates = attachment_authority(
        assessment_path=root / str(authority["assessment_path"]),
        union_manifest_path=root / str(authority["union_manifest_path"]),
        boundary=publisher.boundary,
    )
    if authority != derived:
        raise NoticeAttachmentCaptureError(
            "CME notice-attachment authority changed"
        )
    expected = build_attachment_capture_plan(
        authority=derived,
        candidates=candidates,
        implementation_sha256=implementation_hashes(root),
    )
    if plan != expected:
        raise NoticeAttachmentCaptureError(
            "CME notice-attachment requests drifted"
        )
    failure_path = _failure_path(root, str(plan["plan_id"]))
    prior_release = _existing_release_for_plan(root, str(plan["plan_id"]))
    if failure_path.exists() or prior_release is not None:
        raise NoticeAttachmentCaptureError(
            "CME notice-attachment approval already has a durable outcome"
        )
    requests = scope["requests"]
    assert isinstance(requests, list)
    allowed = {
        str(item["url"]) for item in requests if isinstance(item, dict)
    }
    if len(allowed) != MAX_REQUESTS:
        raise NoticeAttachmentCaptureError(
            "CME notice-attachment allowlist is invalid"
        )
    stage = publisher.create_stage("cme_notice_attachment_capture")
    started = monotonic_time.monotonic()
    responses: list[dict[str, object]] = []
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    total_bytes = 0
    attempted = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        for offset in range(0, len(requests), WORKERS):
            elapsed = int(
                (monotonic_time.monotonic() - started) * 1000
            )
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
                            "failure_code": "DURATION_CEILING_REACHED",
                            "request_id": requests[offset]["request_id"],  # type: ignore[index]
                            "safe_details": {},
                        }
                    ],
                    elapsed_milliseconds=elapsed,
                    boundary=publisher.boundary,
                )
                _write_create_only(failure_path, failure)
                raise NoticeAttachmentCaptureError(
                    "CME notice-attachment duration ceiling is reached"
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
                tuple[
                    Mapping[str, object],
                    bytes,
                    str,
                    dict[str, str],
                    str,
                ]
            ] = []
            failures: list[dict[str, object]] = []
            for spec, future in futures:
                try:
                    body, content_type, safe_headers, received_at = (
                        future.result()
                    )
                    completed.append(
                        (
                            spec,
                            body,
                            content_type,
                            safe_headers,
                            received_at,
                        )
                    )
                except Exception as exc:
                    failures.append(_failure_evidence(exc, spec=spec))
            for spec, body, content_type, safe_headers, received_at in sorted(
                completed,
                key=lambda item: int(item[0]["ordinal"]),
            ):
                request_id = str(spec["request_id"])
                extension = str(spec["extension"])
                name = f"{request_id}{extension}"
                staged = stage / name
                staged.write_bytes(body)
                logical = str(spec["logical_path"])
                logical_paths[name] = logical
                staged_paths[logical] = name
                total_bytes += len(body)
                responses.append(
                    {
                        "content_type": content_type,
                        "discovery_reasons": spec[
                            "discovery_reasons"
                        ],
                        "extension": extension,
                        "logical_path": logical,
                        "ordinal": spec["ordinal"],
                        "received_at_utc": received_at,
                        "request_id": request_id,
                        "request_kind": spec["request_kind"],
                        "safe_headers": safe_headers,
                        "sha256": sha256_file(staged),
                        "size": len(body),
                        "source_notice_request_ids": spec[
                            "source_notice_request_ids"
                        ],
                        "source_titles": spec["source_titles"],
                        "status_code": 200,
                        "url": spec["url"],
                    }
                )
            if total_bytes > MAX_TOTAL_BYTES:
                failures.append(
                    {
                        "error_class": "TOTAL_BYTE_CEILING_REACHED",
                        "failure_code": "TOTAL_BYTE_CEILING_REACHED",
                        "request_id": (
                            completed[-1][0]["request_id"]
                            if completed
                            else batch[0]["request_id"]  # type: ignore[index]
                        ),
                        "safe_details": {},
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
                raise NoticeAttachmentCaptureError(
                    "CME notice-attachment capture stopped on failure"
                )
    responses.sort(key=lambda item: int(item["ordinal"]))
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
            failures=[
                {
                    "error_class": "FINAL_COMPLETION_BOUND_FAILED",
                    "failure_code": "FINAL_COMPLETION_BOUND_FAILED",
                    "safe_details": {},
                }
            ],
            elapsed_milliseconds=elapsed,
            boundary=publisher.boundary,
        )
        _write_create_only(failure_path, failure)
        raise NoticeAttachmentCaptureError(
            "CME notice-attachment final completion bound failed"
        )
    capture_core: dict[str, object] = {
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
    capture = {
        **capture_core,
        "capture_id": sha256_json(capture_core),
    }
    manifest = DataReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=RELEASE_KIND,
        schema_version=CAPTURE_SCHEMA,
        logical_paths=logical_paths,
        source_release_ids=(str(authority["union_release_id"]),),
        embedded_documents={"capture_receipt.json": capture},
        metadata={
            "approval_receipt_id": approval_id,
            "assessment_id": authority["assessment_id"],
            "capture_id": capture["capture_id"],
            "plan_id": plan["plan_id"],
        },
    )
    manifest_path = publisher.publish(
        stage,
        manifest,
        staged_paths=staged_paths,
    )
    receipt = DataReleaseReceipt.from_manifest(
        manifest_path,
        publisher.boundary,
    )
    load_attachment_capture(receipt, boundary=publisher.boundary)
    return receipt


def load_attachment_capture(
    receipt: DataReleaseReceipt,
    *,
    boundary: RepoBoundary,
) -> dict[str, object]:
    DataReleaseReceipt.from_dict(receipt.as_dict())
    if receipt.repository_id != boundary.repository_id:
        raise IntegrityError(
            "CME notice-attachment receipt belongs to another repository"
        )
    manifest_path = boundary.active_root / receipt.manifest_path
    manifest = verify_data_release_manifest(
        manifest_path,
        boundary,
        verify_files=False,
    )
    if (
        receipt.phase != "reference"
        or manifest.release_id != receipt.release_id
        or manifest.release_kind != RELEASE_KIND
        or manifest.release_kind != receipt.release_kind
        or manifest.schema_version != CAPTURE_SCHEMA
        or manifest.schema_version != receipt.schema_version
        or sha256_file(manifest_path) != receipt.manifest_sha256
        or len(manifest.files) != MAX_REQUESTS
        or set(manifest.embedded_documents) != {"capture_receipt.json"}
    ):
        raise IntegrityError(
            "CME notice-attachment release is invalid"
        )
    raw = manifest.embedded_documents["capture_receipt.json"]
    if not isinstance(raw, dict):
        raise IntegrityError(
            "CME notice-attachment receipt is invalid"
        )
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
        raise IntegrityError(
            "CME notice-attachment contract is invalid"
        )
    entries = {entry.logical_path: entry for entry in manifest.files}
    files = []
    total = 0
    urls: set[str] = set()
    previous_ordinal = 0
    for response in responses:
        if (
            not isinstance(response, dict)
            or response.get("ordinal") != previous_ordinal + 1
            or response.get("status_code") != 200
            or type(response.get("logical_path")) is not str
            or type(response.get("url")) is not str
            or response["url"] in urls
        ):
            raise IntegrityError(
                "CME notice-attachment response contract is invalid"
            )
        previous_ordinal += 1
        urls.add(str(response["url"]))
        entry = entries.get(str(response["logical_path"]))
        if (
            entry is None
            or entry.size != response.get("size")
            or entry.sha256 != response.get("sha256")
        ):
            raise IntegrityError(
                "CME notice-attachment response bytes changed"
            )
        physical = (
            boundary.active_root
            / manifest.physical_relative_path(entry)
        )
        files.append((entry, physical))
        total += entry.size
    with ThreadPoolExecutor(max_workers=VERIFY_WORKERS) as executor:
        futures = [
            (
                entry,
                executor.submit(
                    lambda path: (
                        path.stat().st_size,
                        sha256_file(path),
                    ),
                    physical,
                ),
            )
            for entry, physical in files
        ]
        for entry, future in futures:
            size, digest = future.result()
            if size != entry.size or digest != entry.sha256:
                raise IntegrityError(
                    "CME notice-attachment manifested bytes changed"
                )
    if total != raw.get("total_bytes"):
        raise IntegrityError(
            "CME notice-attachment total bytes are invalid"
        )
    return dict(raw)
