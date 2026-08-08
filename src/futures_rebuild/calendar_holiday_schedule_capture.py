"""Bounded capture of exact CME holiday-schedule files discovered offline."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time as monotonic_time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import RepoBoundary
from .calendar_holiday_schedule_discovery import (
    ASSESSMENT_SCHEMA,
    validate_holiday_schedule_discovery,
)
from .calendar_notice_attachment_capture import (
    NoticeAttachmentRequestError,
    _fetch,
)
from .canonical import canonical_bytes, fsync_directory, sha256_file, sha256_json
from .data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
    verify_data_release_manifest,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation


PLAN_SCHEMA = "cme_historical_holiday_schedule_capture_plan/1.0.0"
APPROVAL_SCHEMA = "cme_historical_holiday_schedule_capture_approval/1.0.0"
CAPTURE_SCHEMA = "cme_historical_holiday_schedule_capture/1.0.0"
FAILURE_SCHEMA = "cme_historical_holiday_schedule_capture_failure/1.0.0"
OPERATION = "CAPTURE_BOUNDED_PUBLIC_CME_HISTORICAL_HOLIDAY_SCHEDULE_FILES"
RELEASE_KIND = "cme_historical_holiday_schedule_capture"
MAX_REQUESTS = 64
MAX_RESPONSE_BYTES = 16_777_216
MAX_TOTAL_BYTES = MAX_REQUESTS * MAX_RESPONSE_BYTES
MAX_DURATION_SECONDS = 900
REQUEST_TIMEOUT_SECONDS = 45
WORKERS = 2
IMPLEMENTATION_PATHS = (
    "configs/data_layout_contract.json",
    "configs/exchange_calendar_policy.json",
    "configs/source_contract.json",
    "pyproject.toml",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/calendar_cli.py",
    "src/futures_rebuild/calendar_holiday_schedule_capture.py",
    "src/futures_rebuild/calendar_holiday_schedule_discovery.py",
    "src/futures_rebuild/calendar_notice_attachment_capture.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/source_contract.py",
)
FORBIDDEN_ACTIONS = (
    "ACTIVATE_CALENDAR",
    "CALL_ANOTHER_CME_ENDPOINT_OR_UNLISTED_FILE",
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
    "CONTENT_SIGNATURE_OR_MIME_MISMATCH",
    "IMPLEMENTATION_HASH_DRIFT",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "NON_404_HTTP_FAILURE",
    "OFFLINE_DISCOVERY_OR_SOURCE_RELEASE_DRIFT",
    "PRIOR_PLAN_OUTCOME_EXISTS",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
CONTINUE_CONDITIONS = ("EXACT_HTTP_404_FOR_LISTED_URL",)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_CONTENT_TYPES = {
    ".pdf": ("application/octet-stream", "application/pdf"),
    ".xls": ("application/octet-stream", "application/vnd.ms-excel"),
    ".xlsx": (
        "application/octet-stream",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/zip",
    ),
}
_SIGNATURES = {
    ".pdf": (b"%PDF-",),
    ".xls": (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1",),
    ".xlsx": (b"PK\x03\x04",),
}
_AUTHORITY_KEYS = {
    "assessment_id",
    "assessment_path",
    "assessment_sha256",
    "candidate_set_id",
    "source_capture_id",
    "source_manifest_path",
    "source_manifest_sha256",
    "source_release_id",
}


class HolidayScheduleCaptureError(UnauthorizedOperation):
    """Raised before or during this exact CME authority class."""


def _canonical_object(path: Path, *, description: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is not readable JSON") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


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


def implementation_hashes(repository_root: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for relative in IMPLEMENTATION_PATHS:
        path = repository_root / relative
        if not path.is_file():
            raise IntegrityError(
                "CME holiday-schedule implementation input is missing: "
                f"{relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def holiday_schedule_authority(
    *,
    assessment_path: Path,
    boundary: RepoBoundary,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    boundary.assert_active_path(
        assessment_path,
        purpose="CME historical holiday-schedule discovery",
        subtree="reports/exchange_calendar",
    )
    assessment = validate_holiday_schedule_discovery(
        _canonical_object(
            assessment_path,
            description="CME historical holiday-schedule discovery",
        )
    )
    source_manifest_path = (
        boundary.active_root / str(assessment["source_manifest_path"])
    )
    boundary.assert_active_path(
        source_manifest_path,
        purpose="accepted CME notice-attachment source manifest",
        subtree="manifests/data_releases/reference",
    )
    source_manifest = verify_data_release_manifest(
        source_manifest_path,
        boundary,
        verify_files=True,
    )
    candidates = assessment.get("candidates")
    if (
        assessment.get("schema_version") != ASSESSMENT_SCHEMA
        or assessment.get("candidate_count") != MAX_REQUESTS
        or not isinstance(candidates, list)
        or len(candidates) != MAX_REQUESTS
        or sha256_file(source_manifest_path)
        != assessment["source_manifest_sha256"]
        or source_manifest.release_id != assessment["source_release_id"]
        or source_manifest.metadata.get("capture_id")
        != assessment["source_capture_id"]
    ):
        raise IntegrityError(
            "CME historical holiday-schedule authority is invalid"
        )
    authority = {
        "assessment_id": assessment["assessment_id"],
        "assessment_path": assessment_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "assessment_sha256": sha256_file(assessment_path),
        "candidate_set_id": assessment["candidate_set_id"],
        "source_capture_id": assessment["source_capture_id"],
        "source_manifest_path": assessment["source_manifest_path"],
        "source_manifest_sha256": assessment["source_manifest_sha256"],
        "source_release_id": assessment["source_release_id"],
    }
    _validate_authority(authority)
    return authority, [dict(item) for item in candidates if isinstance(item, dict)]


def _validate_authority(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError("CME holiday-schedule authority schema is invalid")
    for key, value in authority.items():
        if key.endswith("_id") or key.endswith("_sha256"):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise ContractError(
                    "CME holiday-schedule authority hash is invalid"
                )
        elif type(value) is not str or not value:
            raise ContractError(
                "CME holiday-schedule authority path is invalid"
            )


def _requests(
    candidates: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    if len(candidates) != MAX_REQUESTS:
        raise ContractError("CME holiday-schedule candidate count is invalid")
    requests: list[dict[str, object]] = []
    previous_url = ""
    for ordinal, candidate in enumerate(candidates, start=1):
        url = candidate.get("url")
        extension = candidate.get("extension")
        source_ordinals = candidate.get("source_ordinals")
        source_request_ids = candidate.get("source_request_ids")
        evidence_kinds = candidate.get("evidence_kinds")
        if (
            type(url) is not str
            or url <= previous_url
            or type(extension) is not str
            or extension not in _CONTENT_TYPES
            or candidate.get("ordinal") != ordinal
            or not isinstance(source_ordinals, list)
            or not source_ordinals
            or any(type(item) is not int for item in source_ordinals)
            or not isinstance(source_request_ids, list)
            or not source_request_ids
            or any(type(item) is not str for item in source_request_ids)
            or not isinstance(evidence_kinds, list)
            or not evidence_kinds
            or any(type(item) is not str for item in evidence_kinds)
        ):
            raise ContractError(
                "CME holiday-schedule candidate ordering is invalid"
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
            or not parsed.path.startswith(
                "/tools-information/holiday-calendar/files/"
            )
            or Path(parsed.path).suffix.lower() != extension
        ):
            raise ContractError("CME holiday-schedule candidate URL is invalid")
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
        request_id = f"holiday-schedule-{ordinal:04d}-{digest}"
        requests.append(
            {
                "accept": ", ".join(_CONTENT_TYPES[extension]),
                "evidence_kinds": list(evidence_kinds),
                "expected_content_types": list(_CONTENT_TYPES[extension]),
                "extension": extension,
                "logical_path": (
                    "data/reference/exchange_calendars/"
                    f"{request_id}{extension}"
                ),
                "ordinal": ordinal,
                "request_id": request_id,
                "request_kind": "HISTORICAL_HOLIDAY_SCHEDULE_CAPTURE",
                "source_notice_attachment_ordinals": list(source_ordinals),
                "source_notice_attachment_request_ids": list(
                    source_request_ids
                ),
                "url": url,
            }
        )
    return requests


def build_holiday_schedule_capture_plan(
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
            "CME holiday-schedule implementation hashes are invalid"
        )
    requests = _requests(candidates)
    scope: dict[str, object] = {
        "allow_redirects": False,
        "authority": dict(authority),
        "continue_conditions": list(CONTINUE_CONDITIONS),
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
                "cme_historical_holiday_schedule_capture_failure_"
                "{plan_prefix}.json"
            ),
            "manifest_template": (
                "manifests/data_releases/reference/{release_id}.json"
            ),
            "publication_lock": "state/locks/data-publication.lock",
            "staging_root": "state/data_publication_staging",
        },
        "purpose": (
            "CAPTURE_EXACT_CME_HOLIDAY_SCHEDULE_FILES_DISCOVERED_"
            "OFFLINE_BEFORE_ANY_PARSE"
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


def validate_holiday_schedule_capture_plan(
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
            "CME holiday-schedule capture plan schema is invalid"
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
            "CME holiday-schedule capture plan identity is invalid"
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
            "CME holiday-schedule capture plan scope is invalid"
        )
    candidates = []
    for request in requests:
        if not isinstance(request, dict):
            raise IntegrityError(
                "CME holiday-schedule request schema is invalid"
            )
        candidates.append(
            {
                "evidence_kinds": request.get("evidence_kinds"),
                "extension": request.get("extension"),
                "ordinal": request.get("ordinal"),
                "source_ordinals": request.get(
                    "source_notice_attachment_ordinals"
                ),
                "source_request_ids": request.get(
                    "source_notice_attachment_request_ids"
                ),
                "url": request.get("url"),
            }
        )
    expected = build_holiday_schedule_capture_plan(
        authority=authority,
        candidates=candidates,
        implementation_sha256=implementation,
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME holiday-schedule capture plan differs from implementation"
        )
    return dict(payload)


def validate_holiday_schedule_capture_approval(
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
        raise HolidayScheduleCaptureError(
            "CME holiday-schedule capture lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _signature_valid(body: bytes, extension: str) -> bool:
    return any(body.startswith(prefix) for prefix in _SIGNATURES[extension])


def _failure_path(root: Path, plan_id: str) -> Path:
    return (
        root
        / "reports"
        / "exchange_calendar"
        / (
            "cme_historical_holiday_schedule_capture_failure_"
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


def _is_exact_404(failure: Mapping[str, object]) -> bool:
    details = failure.get("safe_details")
    return (
        failure.get("failure_code") == "HTTP_STATUS_REJECTED"
        and isinstance(details, dict)
        and details.get("http_status") == 404
    )


def _failure_report(
    *,
    plan: Mapping[str, object],
    plan_path: Path,
    approval_id: str,
    stage: Path,
    attempted: int,
    responses: Sequence[Mapping[str, object]],
    exclusions: Sequence[Mapping[str, object]],
    failures: Sequence[Mapping[str, object]],
    elapsed_milliseconds: int,
    boundary: RepoBoundary,
) -> dict[str, object]:
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "elapsed_milliseconds": elapsed_milliseconds,
        "excluded_requests": list(exclusions),
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


def capture_holiday_schedules(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_holiday_schedule_capture_plan(
        _canonical_object(
            plan_path,
            description="CME holiday-schedule capture plan",
        )
    )
    approval = _canonical_object(
        approval_path,
        description="CME holiday-schedule capture approval",
    )
    approval_id = validate_holiday_schedule_capture_approval(
        approval,
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    root = publisher.boundary.active_root
    if scope["implementation_sha256"] != implementation_hashes(root):
        raise HolidayScheduleCaptureError(
            "CME holiday-schedule implementation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived, candidates = holiday_schedule_authority(
        assessment_path=root / str(authority["assessment_path"]),
        boundary=publisher.boundary,
    )
    if authority != derived:
        raise HolidayScheduleCaptureError(
            "CME holiday-schedule authority changed"
        )
    expected = build_holiday_schedule_capture_plan(
        authority=derived,
        candidates=candidates,
        implementation_sha256=implementation_hashes(root),
    )
    if plan != expected:
        raise HolidayScheduleCaptureError(
            "CME holiday-schedule requests drifted"
        )
    failure_path = _failure_path(root, str(plan["plan_id"]))
    if (
        failure_path.exists()
        or _existing_release_for_plan(root, str(plan["plan_id"])) is not None
    ):
        raise HolidayScheduleCaptureError(
            "CME holiday-schedule approval already has a durable outcome"
        )
    requests = scope["requests"]
    assert isinstance(requests, list)
    allowed = {
        str(item["url"]) for item in requests if isinstance(item, dict)
    }
    if len(allowed) != MAX_REQUESTS:
        raise HolidayScheduleCaptureError(
            "CME holiday-schedule allowlist is invalid"
        )
    stage = publisher.create_stage("cme_holiday_schedule_capture")
    started = monotonic_time.monotonic()
    responses: list[dict[str, object]] = []
    exclusions: list[dict[str, object]] = []
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    total_bytes = 0
    attempted = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        for offset in range(0, len(requests), WORKERS):
            elapsed = int((monotonic_time.monotonic() - started) * 1000)
            if elapsed >= MAX_DURATION_SECONDS * 1000:
                failures = [
                    {
                        "error_class": "DURATION_CEILING_REACHED",
                        "failure_code": "DURATION_CEILING_REACHED",
                        "request_id": requests[offset]["request_id"],  # type: ignore[index]
                        "safe_details": {},
                    }
                ]
            else:
                failures = []
            if failures:
                report = _failure_report(
                    plan=plan,
                    plan_path=plan_path,
                    approval_id=approval_id,
                    stage=stage,
                    attempted=attempted,
                    responses=responses,
                    exclusions=exclusions,
                    failures=failures,
                    elapsed_milliseconds=elapsed,
                    boundary=publisher.boundary,
                )
                _write_create_only(failure_path, report)
                raise HolidayScheduleCaptureError(
                    "CME holiday-schedule duration ceiling is reached"
                )
            batch = requests[offset : offset + WORKERS]
            futures = [
                (spec, executor.submit(_fetch, spec, allowed=allowed))
                for spec in batch
                if isinstance(spec, dict)
            ]
            attempted += len(futures)
            completed = []
            batch_failures = []
            for spec, future in futures:
                try:
                    body, content_type, safe_headers, received_at = (
                        future.result()
                    )
                    if not _signature_valid(body, str(spec["extension"])):
                        raise NoticeAttachmentRequestError(
                            "CME holiday-schedule content signature is "
                            "invalid",
                            failure_code="CONTENT_SIGNATURE_REJECTED",
                            safe_details={
                                "extension": str(spec["extension"]),
                            },
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
                    failure = _failure_evidence(exc, spec=spec)
                    if _is_exact_404(failure):
                        exclusions.append(
                            {
                                "exclusion_code": "HTTP_404_NOT_FOUND",
                                "http_status": 404,
                                "ordinal": spec["ordinal"],
                                "request_id": spec["request_id"],
                                "url": spec["url"],
                            }
                        )
                    else:
                        batch_failures.append(failure)
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
                        "extension": extension,
                        "logical_path": logical,
                        "ordinal": spec["ordinal"],
                        "received_at_utc": received_at,
                        "request_id": request_id,
                        "request_kind": spec["request_kind"],
                        "safe_headers": safe_headers,
                        "sha256": sha256_file(staged),
                        "size": len(body),
                        "status_code": 200,
                        "url": spec["url"],
                    }
                )
            if total_bytes > MAX_TOTAL_BYTES:
                batch_failures.append(
                    {
                        "error_class": "TOTAL_BYTE_CEILING_REACHED",
                        "failure_code": "TOTAL_BYTE_CEILING_REACHED",
                        "request_id": batch[0]["request_id"],  # type: ignore[index]
                        "safe_details": {},
                    }
                )
            if batch_failures:
                elapsed = int((monotonic_time.monotonic() - started) * 1000)
                report = _failure_report(
                    plan=plan,
                    plan_path=plan_path,
                    approval_id=approval_id,
                    stage=stage,
                    attempted=attempted,
                    responses=responses,
                    exclusions=exclusions,
                    failures=batch_failures,
                    elapsed_milliseconds=elapsed,
                    boundary=publisher.boundary,
                )
                _write_create_only(failure_path, report)
                raise HolidayScheduleCaptureError(
                    "CME holiday-schedule capture stopped on failure"
                )
    responses.sort(key=lambda item: int(item["ordinal"]))
    exclusions.sort(key=lambda item: int(item["ordinal"]))
    elapsed = int((monotonic_time.monotonic() - started) * 1000)
    if (
        elapsed > MAX_DURATION_SECONDS * 1000
        or attempted != MAX_REQUESTS
        or len(responses) + len(exclusions) != MAX_REQUESTS
        or not responses
        or total_bytes > MAX_TOTAL_BYTES
    ):
        report = _failure_report(
            plan=plan,
            plan_path=plan_path,
            approval_id=approval_id,
            stage=stage,
            attempted=attempted,
            responses=responses,
            exclusions=exclusions,
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
        _write_create_only(failure_path, report)
        raise HolidayScheduleCaptureError(
            "CME holiday-schedule final completion bound failed"
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
        "exclusion_count": len(exclusions),
        "exclusions": exclusions,
        "network_request_count": MAX_REQUESTS,
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "response_count": len(responses),
        "responses": responses,
        "schema_version": CAPTURE_SCHEMA,
        "total_bytes": total_bytes,
        "unresolved_candidate_count": 0,
    }
    capture = {**capture_core, "capture_id": sha256_json(capture_core)}
    manifest = DataReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=RELEASE_KIND,
        schema_version=CAPTURE_SCHEMA,
        logical_paths=logical_paths,
        source_release_ids=(str(authority["source_release_id"]),),
        embedded_documents={"capture_receipt.json": capture},
        metadata={
            "approval_receipt_id": approval_id,
            "assessment_id": authority["assessment_id"],
            "capture_id": capture["capture_id"],
            "plan_id": plan["plan_id"],
        },
    )
    manifest_path = publisher.publish(stage, manifest, staged_paths=staged_paths)
    receipt = DataReleaseReceipt.from_manifest(
        manifest_path,
        publisher.boundary,
    )
    load_holiday_schedule_capture(
        receipt,
        boundary=publisher.boundary,
        verify_files=True,
    )
    return receipt


def load_holiday_schedule_capture(
    receipt: DataReleaseReceipt,
    *,
    boundary: RepoBoundary,
    verify_files: bool = False,
) -> dict[str, object]:
    DataReleaseReceipt.from_dict(receipt.as_dict())
    if receipt.repository_id != boundary.repository_id:
        raise IntegrityError(
            "CME holiday-schedule receipt belongs to another repository"
        )
    manifest_path = boundary.active_root / receipt.manifest_path
    manifest = verify_data_release_manifest(
        manifest_path,
        boundary,
        verify_files=verify_files,
    )
    raw = manifest.embedded_documents.get("capture_receipt.json")
    if (
        receipt.phase != "reference"
        or manifest.release_id != receipt.release_id
        or manifest.release_kind != RELEASE_KIND
        or manifest.release_kind != receipt.release_kind
        or manifest.schema_version != CAPTURE_SCHEMA
        or manifest.schema_version != receipt.schema_version
        or sha256_file(manifest_path) != receipt.manifest_sha256
        or not isinstance(raw, dict)
        or raw.get("schema_version") != CAPTURE_SCHEMA
        or raw.get("capture_id")
        != sha256_json(
            {key: value for key, value in raw.items() if key != "capture_id"}
        )
        or raw.get("network_request_count") != MAX_REQUESTS
        or raw.get("unresolved_candidate_count") != 0
        or raw.get("response_count") != len(manifest.files)
        or raw.get("exclusion_count")
        != MAX_REQUESTS - len(manifest.files)
        or raw.get("response_count", 0) <= 0
    ):
        raise IntegrityError("CME holiday-schedule release is invalid")
    if verify_files:
        release_root = (
            boundary.active_root
            / "data"
            / "reference"
            / "exchange_calendars"
            / manifest.release_id
        )
        responses = raw.get("responses")
        if not isinstance(responses, list):
            raise IntegrityError(
                "CME holiday-schedule response list is invalid"
            )
        for response in responses:
            if not isinstance(response, dict):
                raise IntegrityError(
                    "CME holiday-schedule response is invalid"
                )
            path = release_root / Path(str(response["logical_path"])).name
            if (
                not path.is_file()
                or sha256_file(path) != response.get("sha256")
                or path.stat().st_size != response.get("size")
                or not _signature_valid(
                    path.read_bytes()[:8],
                    str(response["extension"]),
                )
            ):
                raise IntegrityError(
                    "CME holiday-schedule payload verification failed"
                )
    return dict(raw)
