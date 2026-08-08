"""Single-request diagnostic successor for a stopped attachment capture."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time as monotonic_time
import urllib.parse
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import RepoBoundary
from .calendar_notice_attachment_capture import (
    APPROVAL_SCHEMA as PREDECESSOR_APPROVAL_SCHEMA,
)
from .calendar_notice_attachment_capture import (
    OPERATION as PREDECESSOR_OPERATION,
)
from .calendar_notice_attachment_capture import (
    PREDECESSOR_FAILURE_SCHEMA,
    NoticeAttachmentRequestError,
    _fetch,
    validate_attachment_capture_approval,
    validate_attachment_capture_plan,
)
from .canonical import canonical_bytes, fsync_directory, sha256_file, sha256_json
from .errors import ContractError, IntegrityError, UnauthorizedOperation


PLAN_SCHEMA = "cme_historical_notice_attachment_diagnostic_plan/1.0.0"
APPROVAL_SCHEMA = "cme_historical_notice_attachment_diagnostic_approval/1.0.0"
RESULT_SCHEMA = "cme_historical_notice_attachment_diagnostic_result/1.0.0"
OPERATION = "DIAGNOSE_BOUNDED_PUBLIC_CME_HISTORICAL_NOTICE_ATTACHMENT"
EXPECTED_PREDECESSOR_REQUESTS = 797
EXPECTED_ATTEMPTED_REQUESTS = 14
EXPECTED_PRESERVED_RESPONSES = 13
EXPECTED_FAILED_ORDINAL = 14
EXPECTED_REMAINING_AFTER_DIAGNOSTIC = 783
MAX_REQUESTS = 1
MAX_RESPONSE_BYTES = 16_777_216
MAX_DURATION_SECONDS = 120
REQUEST_TIMEOUT_SECONDS = 45
WORKERS = 1
VERIFY_WORKERS = 8
IMPLEMENTATION_PATHS = (
    "configs/data_layout_contract.json",
    "configs/exchange_calendar_policy.json",
    "configs/source_contract.json",
    "pyproject.toml",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/calendar_cli.py",
    "src/futures_rebuild/calendar_notice_attachment_capture.py",
    "src/futures_rebuild/calendar_notice_attachment_diagnostic.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/source_contract.py",
)
FORBIDDEN_ACTIONS = (
    "ACTIVATE_CALENDAR",
    "CALL_ANOTHER_CME_ENDPOINT_OR_ATTACHMENT",
    "CALL_DATABENTO_OR_ANY_NON_CME_PROVIDER",
    "DELETE_EDIT_MOVE_OR_OVERWRITE_PREDECESSOR_EVIDENCE",
    "EXECUTE_DOCUMENT_CONTENT_OR_FOLLOW_EMBEDDED_LINK",
    "PARSE_OR_ACCEPT_HISTORICAL_CALENDAR",
    "PUBLISH_ATTACHMENT_RELEASE",
    "REBUILD_FOUNDATION",
    "REQUEST_ANY_OF_THE_REMAINING_783_ATTACHMENTS",
    "RETRY_OR_REDIRECT_REQUEST",
    "USE_CREDENTIAL_OR_SECRET",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "BYTE_OR_DURATION_CEILING_REACHED",
    "IMPLEMENTATION_HASH_DRIFT",
    "PREDECESSOR_FAILURE_OR_STAGE_DRIFT",
    "PRIOR_DIAGNOSTIC_OUTCOME_EXISTS",
    "UNLISTED_URL_OR_REDIRECT",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_AUTHORITY_KEYS = {
    "failed_ordinal",
    "failed_request_id",
    "failed_url",
    "failure_id",
    "failure_report_path",
    "failure_report_sha256",
    "predecessor_approval_path",
    "predecessor_approval_receipt_id",
    "predecessor_approval_sha256",
    "predecessor_plan_id",
    "predecessor_plan_path",
    "predecessor_plan_sha256",
    "preserved_response_count",
    "preserved_response_set_id",
    "preserved_stage_relative_path",
    "preserved_total_bytes",
    "remaining_request_count_after_diagnostic",
}
_AUTHORITY_HASH_KEYS = {
    "failure_id",
    "failure_report_sha256",
    "predecessor_approval_receipt_id",
    "predecessor_approval_sha256",
    "predecessor_plan_id",
    "predecessor_plan_sha256",
    "preserved_response_set_id",
}
_AUTHORITY_PATH_KEYS = {
    "failure_report_path",
    "predecessor_approval_path",
    "predecessor_plan_path",
    "preserved_stage_relative_path",
}
_AUTHORITY_TEXT_KEYS = {
    "failed_request_id",
    "failed_url",
}


class NoticeAttachmentDiagnosticError(UnauthorizedOperation):
    """Raised before an unauthorized or stale diagnostic operation."""


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
                "CME attachment diagnostic implementation input is missing: "
                f"{relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _validate_authority(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError(
            "CME attachment diagnostic authority schema is invalid"
        )
    for key, value in authority.items():
        if key in _AUTHORITY_HASH_KEYS:
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise ContractError(
                    "CME attachment diagnostic authority hash is invalid"
                )
        elif key in _AUTHORITY_PATH_KEYS:
            if type(value) is not str or not value:
                raise ContractError(
                    "CME attachment diagnostic authority path is invalid"
                )
        elif key in _AUTHORITY_TEXT_KEYS:
            if type(value) is not str or not value:
                raise ContractError(
                    "CME attachment diagnostic authority text is invalid"
                )
        elif type(value) is not int or value < 0:
            raise ContractError(
                "CME attachment diagnostic authority count is invalid"
            )


def _validate_predecessor_failure(
    failure: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    plan_path: Path,
    approval: Mapping[str, object],
    boundary: RepoBoundary,
) -> tuple[Path, list[dict[str, object]], dict[str, object]]:
    expected_keys = {
        "approval_receipt_id",
        "elapsed_milliseconds",
        "failed_requests",
        "failure_id",
        "network_requests_attempted",
        "plan_id",
        "plan_sha256",
        "publication_occurred",
        "responses_preserved",
        "responses_preserved_count",
        "retries_performed",
        "schema_version",
        "stage_relative_path",
        "status",
    }
    core = dict(failure)
    failure_id = core.pop("failure_id", None)
    responses = failure.get("responses_preserved")
    failed = failure.get("failed_requests")
    scope = plan.get("scope")
    if (
        set(failure) != expected_keys
        or type(failure_id) is not str
        or failure_id != sha256_json(core)
        or failure.get("schema_version") != PREDECESSOR_FAILURE_SCHEMA
        or failure.get("status") != "STOPPED"
        or failure.get("plan_id") != plan.get("plan_id")
        or failure.get("plan_sha256") != sha256_file(plan_path)
        or failure.get("approval_receipt_id")
        != approval.get("approval_receipt_id")
        or failure.get("publication_occurred") is not False
        or failure.get("retries_performed") != 0
        or failure.get("network_requests_attempted")
        != EXPECTED_ATTEMPTED_REQUESTS
        or failure.get("responses_preserved_count")
        != EXPECTED_PRESERVED_RESPONSES
        or not isinstance(responses, list)
        or len(responses) != EXPECTED_PRESERVED_RESPONSES
        or not isinstance(failed, list)
        or len(failed) != 1
        or not isinstance(failed[0], dict)
        or set(failed[0]) != {"error_class", "request_id", "url"}
        or not isinstance(scope, dict)
    ):
        raise IntegrityError(
            "CME attachment predecessor failure contract is invalid"
        )
    requests = scope.get("requests")
    if (
        not isinstance(requests, list)
        or len(requests) != EXPECTED_PREDECESSOR_REQUESTS
    ):
        raise IntegrityError(
            "CME attachment predecessor request set is invalid"
        )
    failed_request = requests[EXPECTED_FAILED_ORDINAL - 1]
    if (
        not isinstance(failed_request, dict)
        or failed_request.get("ordinal") != EXPECTED_FAILED_ORDINAL
        or failed[0].get("request_id")
        != failed_request.get("request_id")
        or failed[0].get("url") != failed_request.get("url")
    ):
        raise IntegrityError(
            "CME attachment predecessor failed item is invalid"
        )
    stage_relative = failure.get("stage_relative_path")
    if (
        type(stage_relative) is not str
        or not stage_relative.startswith("state/data_publication_staging/")
    ):
        raise IntegrityError(
            "CME attachment predecessor stage path is invalid"
        )
    stage = boundary.assert_active_path(
        boundary.active_root / stage_relative,
        purpose="CME attachment predecessor stage",
        subtree="state/data_publication_staging",
    )
    if not stage.is_dir() or stage.is_symlink():
        raise IntegrityError(
            "CME attachment predecessor stage is unavailable"
        )
    files = sorted(path for path in stage.iterdir() if path.is_file())
    if (
        len(files) != EXPECTED_PRESERVED_RESPONSES
        or any(path.is_symlink() for path in files)
    ):
        raise IntegrityError(
            "CME attachment predecessor stage file set is invalid"
        )
    descriptors: list[dict[str, object]] = []
    files_by_name = {path.name: path for path in files}
    verification: list[
        tuple[dict[str, object], Path, Future[tuple[int, str]]]
    ] = []
    with ThreadPoolExecutor(max_workers=VERIFY_WORKERS) as executor:
        for ordinal, response in enumerate(responses, start=1):
            spec = requests[ordinal - 1]
            if (
                not isinstance(response, dict)
                or not isinstance(spec, dict)
                or response.get("ordinal") != ordinal
                or response.get("request_id") != spec.get("request_id")
                or response.get("url") != spec.get("url")
                or response.get("logical_path") != spec.get("logical_path")
                or response.get("status_code") != 200
                or response.get("content_type")
                not in spec.get("expected_content_types", [])
                or type(response.get("sha256")) is not str
                or _SHA256.fullmatch(str(response["sha256"])) is None
                or type(response.get("size")) is not int
                or int(response["size"]) < 0
            ):
                raise IntegrityError(
                    "CME attachment preserved response contract is invalid"
                )
            name = Path(str(response["logical_path"])).name
            physical = files_by_name.get(name)
            if physical is None:
                raise IntegrityError(
                    "CME attachment preserved file is absent"
                )
            future = executor.submit(
                lambda path: (
                    path.stat().st_size,
                    sha256_file(path),
                ),
                physical,
            )
            verification.append((response, physical, future))
        for response, physical, future in verification:
            size, digest = future.result()
            if size != response["size"] or digest != response["sha256"]:
                raise IntegrityError(
                    "CME attachment preserved bytes changed"
                )
            descriptors.append(
                {
                    "content_type": response["content_type"],
                    "logical_path": response["logical_path"],
                    "ordinal": response["ordinal"],
                    "request_id": response["request_id"],
                    "sha256": digest,
                    "size": size,
                    "url": response["url"],
                }
            )
    descriptors.sort(key=lambda item: int(item["ordinal"]))
    return stage, descriptors, dict(failed_request)


def preserved_failure_authority(
    *,
    predecessor_plan_path: Path,
    predecessor_approval_path: Path,
    failure_report_path: Path,
    boundary: RepoBoundary,
) -> tuple[
    dict[str, object],
    dict[str, object],
    list[dict[str, object]],
]:
    for path, purpose, subtree in (
        (
            predecessor_plan_path,
            "CME attachment predecessor plan",
            "reports/exchange_calendar",
        ),
        (
            predecessor_approval_path,
            "CME attachment predecessor approval",
            "configs",
        ),
        (
            failure_report_path,
            "CME attachment predecessor failure",
            "reports/exchange_calendar",
        ),
    ):
        boundary.assert_active_path(
            path,
            purpose=purpose,
            subtree=subtree,
        )
    plan = validate_attachment_capture_plan(
        _canonical_object(
            predecessor_plan_path,
            description="CME attachment predecessor plan",
        )
    )
    if (
        plan.get("operation") != PREDECESSOR_OPERATION
        or len(plan.get("scope", {}).get("requests", []))  # type: ignore[union-attr]
        != EXPECTED_PREDECESSOR_REQUESTS
    ):
        raise IntegrityError(
            "CME attachment predecessor plan authority is invalid"
        )
    approval = _canonical_object(
        predecessor_approval_path,
        description="CME attachment predecessor approval",
    )
    if approval.get("schema_version") != PREDECESSOR_APPROVAL_SCHEMA:
        raise IntegrityError(
            "CME attachment predecessor approval schema is invalid"
        )
    approval_id = validate_attachment_capture_approval(
        approval,
        plan=plan,
        plan_sha256=sha256_file(predecessor_plan_path),
    )
    failure = _canonical_object(
        failure_report_path,
        description="CME attachment predecessor failure",
    )
    stage, descriptors, failed_request = _validate_predecessor_failure(
        failure,
        plan=plan,
        plan_path=predecessor_plan_path,
        approval=approval,
        boundary=boundary,
    )
    total = sum(int(item["size"]) for item in descriptors)
    authority: dict[str, object] = {
        "failed_ordinal": failed_request["ordinal"],
        "failed_request_id": failed_request["request_id"],
        "failed_url": failed_request["url"],
        "failure_id": failure["failure_id"],
        "failure_report_path": failure_report_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "failure_report_sha256": sha256_file(failure_report_path),
        "predecessor_approval_path": predecessor_approval_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "predecessor_approval_receipt_id": approval_id,
        "predecessor_approval_sha256": sha256_file(
            predecessor_approval_path
        ),
        "predecessor_plan_id": plan["plan_id"],
        "predecessor_plan_path": predecessor_plan_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "predecessor_plan_sha256": sha256_file(predecessor_plan_path),
        "preserved_response_count": len(descriptors),
        "preserved_response_set_id": sha256_json(descriptors),
        "preserved_stage_relative_path": stage.relative_to(
            boundary.active_root
        ).as_posix(),
        "preserved_total_bytes": total,
        "remaining_request_count_after_diagnostic": (
            EXPECTED_REMAINING_AFTER_DIAGNOSTIC
        ),
    }
    _validate_authority(authority)
    return authority, failed_request, descriptors


def _diagnostic_request(
    failed_request: Mapping[str, object],
) -> dict[str, object]:
    url = failed_request.get("url")
    request_id = failed_request.get("request_id")
    ordinal = failed_request.get("ordinal")
    extension = failed_request.get("extension")
    expected = failed_request.get("expected_content_types")
    if (
        type(url) is not str
        or type(request_id) is not str
        or type(ordinal) is not int
        or extension != ".pdf"
        or not isinstance(expected, list)
        or sorted(expected)
        != ["application/octet-stream", "application/pdf"]
    ):
        raise ContractError(
            "CME attachment diagnostic request source is invalid"
        )
    parsed = urllib.parse.urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.lower().endswith(".pdf")
    ):
        raise ContractError(
            "CME attachment diagnostic URL is invalid"
        )
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
    return {
        "accept": "application/octet-stream, application/pdf",
        "diagnostic_request_id": f"diagnostic-{digest}",
        "expected_content_types": sorted(expected),
        "extension": ".pdf",
        "predecessor_ordinal": ordinal,
        "predecessor_request_id": request_id,
        "request_id": request_id,
        "request_kind": "HISTORICAL_NOTICE_ATTACHMENT_DIAGNOSTIC",
        "url": url,
    }


def build_diagnostic_plan(
    *,
    authority: Mapping[str, object],
    failed_request: Mapping[str, object],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    _validate_authority(authority)
    request = _diagnostic_request(failed_request)
    if (
        request["predecessor_ordinal"] != authority["failed_ordinal"]
        or request["predecessor_request_id"]
        != authority["failed_request_id"]
        or request["url"] != authority["failed_url"]
    ):
        raise ContractError(
            "CME attachment diagnostic request differs from authority"
        )
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
    ):
        raise ContractError(
            "CME attachment diagnostic implementation hashes are invalid"
        )
    scope: dict[str, object] = {
        "allow_redirects": False,
        "authority": dict(authority),
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "max_duration_seconds": MAX_DURATION_SECONDS,
        "max_requests": MAX_REQUESTS,
        "max_response_bytes": MAX_RESPONSE_BYTES,
        "output_paths": {
            "payload_stage": (
                "state/data_publication_staging/"
                "cme_notice_attachment_diagnostic-{plan_prefix}"
            ),
            "result": (
                "reports/exchange_calendar/"
                "cme_historical_notice_attachment_diagnostic_result_"
                "{plan_prefix}.json"
            ),
        },
        "purpose": (
            "CLASSIFY_ONE_FAILED_CME_ATTACHMENT_AS_HTTP_MIME_NETWORK_"
            "TIMEOUT_REDIRECT_OR_VALID_PAYLOAD"
        ),
        "request": request,
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
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


def validate_diagnostic_plan(
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
            "CME attachment diagnostic plan schema is invalid"
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
            "CME attachment diagnostic plan identity is invalid"
        )
    authority = scope.get("authority")
    implementation = scope.get("implementation_sha256")
    request = scope.get("request")
    if (
        not isinstance(authority, dict)
        or not isinstance(implementation, dict)
        or not isinstance(request, dict)
    ):
        raise IntegrityError(
            "CME attachment diagnostic plan scope is invalid"
        )
    failed_request = {
        "expected_content_types": request.get(
            "expected_content_types"
        ),
        "extension": request.get("extension"),
        "ordinal": request.get("predecessor_ordinal"),
        "request_id": request.get("predecessor_request_id"),
        "url": request.get("url"),
    }
    expected = build_diagnostic_plan(
        authority=authority,
        failed_request=failed_request,
        implementation_sha256=implementation,
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME attachment diagnostic plan differs from implementation"
        )
    return dict(payload)


def validate_diagnostic_approval(
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
        raise NoticeAttachmentDiagnosticError(
            "CME attachment diagnostic lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _write_create_only(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(path.parent)


def _result_path(root: Path, plan_id: str) -> Path:
    return (
        root
        / "reports"
        / "exchange_calendar"
        / (
            "cme_historical_notice_attachment_diagnostic_result_"
            f"{plan_id[:8]}.json"
        )
    )


def _stage_path(root: Path, plan_id: str) -> Path:
    return (
        root
        / "state"
        / "data_publication_staging"
        / f"cme_notice_attachment_diagnostic-{plan_id[:8]}"
    )


def run_diagnostic(
    *,
    plan_path: Path,
    approval_path: Path,
    predecessor_plan_path: Path,
    predecessor_approval_path: Path,
    failure_report_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    plan = validate_diagnostic_plan(
        _canonical_object(
            plan_path,
            description="CME attachment diagnostic plan",
        )
    )
    approval = _canonical_object(
        approval_path,
        description="CME attachment diagnostic approval",
    )
    approval_id = validate_diagnostic_approval(
        approval,
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    if scope["implementation_sha256"] != implementation_hashes(
        boundary.active_root
    ):
        raise NoticeAttachmentDiagnosticError(
            "CME attachment diagnostic implementation hashes drifted"
        )
    authority, failed_request, _descriptors = preserved_failure_authority(
        predecessor_plan_path=predecessor_plan_path,
        predecessor_approval_path=predecessor_approval_path,
        failure_report_path=failure_report_path,
        boundary=boundary,
    )
    if authority != scope["authority"]:
        raise NoticeAttachmentDiagnosticError(
            "CME attachment diagnostic authority changed"
        )
    expected = build_diagnostic_plan(
        authority=authority,
        failed_request=failed_request,
        implementation_sha256=implementation_hashes(
            boundary.active_root
        ),
    )
    if plan != expected:
        raise NoticeAttachmentDiagnosticError(
            "CME attachment diagnostic request drifted"
        )
    result_path = _result_path(
        boundary.active_root,
        str(plan["plan_id"]),
    )
    stage_path = _stage_path(
        boundary.active_root,
        str(plan["plan_id"]),
    )
    if result_path.exists() or stage_path.exists():
        raise NoticeAttachmentDiagnosticError(
            "CME attachment diagnostic already has an outcome"
        )
    request = scope["request"]
    assert isinstance(request, dict)
    started = monotonic_time.monotonic()
    payload: dict[str, object] | None = None
    try:
        body, content_type, safe_headers, received_at = _fetch(
            request,
            allowed={str(request["url"])},
        )
        stage_path.parent.mkdir(parents=True, exist_ok=True)
        stage_path.mkdir()
        physical = stage_path / (
            f"{request['predecessor_request_id']}.pdf"
        )
        _write_create_only(physical, body)
        payload = {
            "content_type": content_type,
            "received_at_utc": received_at,
            "safe_headers": safe_headers,
            "sha256": sha256_file(physical),
            "size": physical.stat().st_size,
            "stage_relative_path": physical.relative_to(
                boundary.active_root
            ).as_posix(),
        }
        classification = {
            "error_class": None,
            "failure_code": "HTTP_200_EXPECTED_MIME_PAYLOAD_PRESERVED",
            "safe_details": {
                "content_type": content_type,
                "http_status": 200,
            },
        }
    except NoticeAttachmentRequestError as exc:
        classification = exc.evidence()
    except Exception as exc:
        classification = {
            "error_class": type(exc).__name__,
            "failure_code": "UNCLASSIFIED_INTERNAL_FAILURE",
            "safe_details": {},
        }
    elapsed = int((monotonic_time.monotonic() - started) * 1000)
    if elapsed > MAX_DURATION_SECONDS * 1000:
        classification = {
            "error_class": "DURATION_CEILING_REACHED",
            "failure_code": "DURATION_CEILING_REACHED",
            "safe_details": {},
        }
    result_core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "authority": authority,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": MAX_DURATION_SECONDS,
            "max_requests": MAX_REQUESTS,
            "max_response_bytes": MAX_RESPONSE_BYTES,
            "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "retries": 0,
            "workers": WORKERS,
        },
        "classification": classification,
        "diagnosed_at_utc": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "elapsed_milliseconds": elapsed,
        "network_request_count": 1,
        "operation": OPERATION,
        "payload": payload,
        "plan_id": plan["plan_id"],
        "request": request,
        "schema_version": RESULT_SCHEMA,
        "status": "DIAGNOSTIC_COMPLETED",
    }
    result = {
        **result_core,
        "diagnostic_id": sha256_json(result_core),
    }
    _write_create_only(
        result_path,
        canonical_bytes(result) + b"\n",
    )
    return result
