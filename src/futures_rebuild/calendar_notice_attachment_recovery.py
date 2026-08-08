"""Hash-bound recovery of the remaining CME historical notice attachments."""

from __future__ import annotations

import json
import os
import re
import shutil
import time as monotonic_time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import RepoBoundary
from .calendar_notice_attachment_capture import (
    RELEASE_KIND,
    _failure_evidence as attachment_failure_evidence,
    _fetch as fetch_attachment,
    validate_attachment_capture_plan,
)
from .calendar_notice_attachment_diagnostic import (
    OPERATION as DIAGNOSTIC_OPERATION,
    RESULT_SCHEMA as DIAGNOSTIC_RESULT_SCHEMA,
    preserved_failure_authority,
    validate_diagnostic_approval,
    validate_diagnostic_plan,
)
from .canonical import canonical_bytes, fsync_directory, sha256_file, sha256_json
from .data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
    verify_data_release_manifest,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation


PLAN_SCHEMA = "cme_historical_notice_attachment_recovery_plan/1.0.0"
APPROVAL_SCHEMA = "cme_historical_notice_attachment_recovery_approval/1.0.0"
CAPTURE_SCHEMA = "cme_historical_notice_attachment_capture/2.0.0"
FAILURE_SCHEMA = "cme_historical_notice_attachment_recovery_failure/1.0.0"
OPERATION = (
    "CAPTURE_BOUNDED_PUBLIC_CME_HISTORICAL_NOTICE_ATTACHMENT_RECOVERY"
)
TOTAL_CANDIDATES = 797
REUSED_RESPONSES = 13
EXCLUDED_RESPONSES = 1
NETWORK_REQUESTS = 783
TOTAL_PAYLOADS = REUSED_RESPONSES + NETWORK_REQUESTS
MAX_RESPONSE_BYTES = 16_777_216
MAX_NETWORK_BYTES = 4_294_967_296
MAX_TOTAL_BYTES = 4_294_967_296
MAX_DURATION_SECONDS = 5_400
REQUEST_TIMEOUT_SECONDS = 45
WORKERS = 2
REUSE_VERIFICATION_WORKERS = 16
IMPLEMENTATION_PATHS = (
    "configs/data_layout_contract.json",
    "configs/exchange_calendar_policy.json",
    "configs/source_contract.json",
    "pyproject.toml",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/calendar_cli.py",
    "src/futures_rebuild/calendar_notice_attachment_capture.py",
    "src/futures_rebuild/calendar_notice_attachment_diagnostic.py",
    "src/futures_rebuild/calendar_notice_attachment_recovery.py",
    "src/futures_rebuild/calendar_notice_attachments.py",
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
    "DELETE_EDIT_MOVE_OR_OVERWRITE_PREDECESSOR_EVIDENCE",
    "EXECUTE_DOCUMENT_CONTENT_OR_FOLLOW_EMBEDDED_LINK",
    "PARSE_OR_ACCEPT_HISTORICAL_CALENDAR",
    "REBUILD_FOUNDATION",
    "REQUEST_EXCLUDED_HTTP_404_ATTACHMENT",
    "RETRY_OR_REDIRECT_REQUEST",
    "USE_CREDENTIAL_OR_SECRET",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "BYTE_OR_DURATION_CEILING_REACHED",
    "CONTENT_TYPE_OR_HTTP_STATUS_MISMATCH",
    "DIAGNOSTIC_OR_PREDECESSOR_EVIDENCE_DRIFT",
    "IMPLEMENTATION_HASH_DRIFT",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PRIOR_PLAN_OUTCOME_EXISTS",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_REQUEST_KEYS = {
    "accept",
    "discovery_reasons",
    "expected_content_types",
    "extension",
    "link_texts",
    "logical_path",
    "ordinal",
    "request_id",
    "request_kind",
    "source_notice_request_ids",
    "source_notice_urls",
    "source_titles",
    "url",
}
_AUTHORITY_HASH_KEYS = {
    "diagnostic_approval_receipt_id",
    "diagnostic_approval_sha256",
    "diagnostic_id",
    "diagnostic_plan_id",
    "diagnostic_plan_sha256",
    "diagnostic_result_sha256",
    "failure_id",
    "failure_report_sha256",
    "predecessor_approval_receipt_id",
    "predecessor_approval_sha256",
    "predecessor_plan_id",
    "predecessor_plan_sha256",
    "preserved_response_set_id",
    "source_union_release_id",
}
_AUTHORITY_PATH_KEYS = {
    "diagnostic_approval_path",
    "diagnostic_plan_path",
    "diagnostic_result_path",
    "failure_report_path",
    "predecessor_approval_path",
    "predecessor_plan_path",
    "preserved_stage_relative_path",
}
_AUTHORITY_TEXT_KEYS = {
    "diagnostic_failure_code",
    "excluded_request_id",
    "excluded_url",
    "first_remaining_request_id",
    "last_remaining_request_id",
}
_AUTHORITY_COUNT_KEYS = {
    "diagnostic_http_status",
    "excluded_response_count",
    "preserved_response_count",
    "preserved_total_bytes",
    "remaining_request_count",
}
_AUTHORITY_KEYS = (
    _AUTHORITY_HASH_KEYS
    | _AUTHORITY_PATH_KEYS
    | _AUTHORITY_TEXT_KEYS
    | _AUTHORITY_COUNT_KEYS
)


class NoticeAttachmentRecoveryError(UnauthorizedOperation):
    """Raised before or during the bounded attachment recovery."""


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
                "CME attachment-recovery implementation input is missing: "
                f"{relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _validate_authority(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError(
            "CME attachment-recovery authority schema is invalid"
        )
    for key in _AUTHORITY_HASH_KEYS:
        value = authority.get(key)
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise ContractError(
                "CME attachment-recovery authority hash is invalid"
            )
    for key in _AUTHORITY_PATH_KEYS | _AUTHORITY_TEXT_KEYS:
        if type(authority.get(key)) is not str or not authority[key]:
            raise ContractError(
                "CME attachment-recovery authority text is invalid"
            )
    for key in _AUTHORITY_COUNT_KEYS:
        if type(authority.get(key)) is not int or int(authority[key]) < 0:
            raise ContractError(
                "CME attachment-recovery authority count is invalid"
            )
    if (
        authority["diagnostic_failure_code"] != "HTTP_STATUS_REJECTED"
        or authority["diagnostic_http_status"] != 404
        or authority["excluded_response_count"] != EXCLUDED_RESPONSES
        or authority["preserved_response_count"] != REUSED_RESPONSES
        or authority["remaining_request_count"] != NETWORK_REQUESTS
        or int(authority["preserved_total_bytes"]) < REUSED_RESPONSES
        or int(authority["preserved_total_bytes"]) > MAX_TOTAL_BYTES
    ):
        raise ContractError(
            "CME attachment-recovery authority bounds are invalid"
        )


def _validate_diagnostic_result(
    *,
    plan: Mapping[str, object],
    plan_path: Path,
    approval: Mapping[str, object],
    approval_path: Path,
    result: Mapping[str, object],
) -> dict[str, object]:
    approval_id = validate_diagnostic_approval(
        approval,
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    core = dict(result)
    diagnostic_id = core.pop("diagnostic_id", None)
    expected_keys = {
        "approval_receipt_id",
        "authority",
        "bounds",
        "classification",
        "diagnosed_at_utc",
        "diagnostic_id",
        "elapsed_milliseconds",
        "network_request_count",
        "operation",
        "payload",
        "plan_id",
        "request",
        "schema_version",
        "status",
    }
    scope = plan.get("scope")
    if not isinstance(scope, dict):
        raise IntegrityError(
            "CME attachment diagnostic plan scope is invalid"
        )
    expected_bounds = {
        "allow_redirects": scope["allow_redirects"],
        "max_duration_seconds": scope["max_duration_seconds"],
        "max_requests": scope["max_requests"],
        "max_response_bytes": scope["max_response_bytes"],
        "request_timeout_seconds": scope["request_timeout_seconds"],
        "retries": scope["retries"],
        "workers": scope["workers"],
    }
    classification = result.get("classification")
    if (
        set(result) != expected_keys
        or type(diagnostic_id) is not str
        or diagnostic_id != sha256_json(core)
        or result.get("schema_version") != DIAGNOSTIC_RESULT_SCHEMA
        or result.get("status") != "DIAGNOSTIC_COMPLETED"
        or result.get("operation") != DIAGNOSTIC_OPERATION
        or result.get("plan_id") != plan.get("plan_id")
        or result.get("approval_receipt_id") != approval_id
        or result.get("authority") != scope.get("authority")
        or result.get("request") != scope.get("request")
        or result.get("bounds") != expected_bounds
        or result.get("network_request_count") != 1
        or type(result.get("elapsed_milliseconds")) is not int
        or int(result["elapsed_milliseconds"]) < 0
        or int(result["elapsed_milliseconds"])
        > int(scope["max_duration_seconds"]) * 1_000
        or type(result.get("diagnosed_at_utc")) is not str
        or _UTC_SECOND.fullmatch(str(result["diagnosed_at_utc"])) is None
        or result.get("payload") is not None
        or not isinstance(classification, dict)
        or classification
        != {
            "error_class": "NoticeAttachmentRequestError",
            "failure_code": "HTTP_STATUS_REJECTED",
            "safe_details": {
                "content_type": "text/html",
                "http_status": 404,
            },
        }
    ):
        raise IntegrityError(
            "CME attachment diagnostic result is not an exact HTTP 404"
        )
    return dict(result)


def _validate_remaining_requests(
    requests: Sequence[object],
) -> list[dict[str, object]]:
    if len(requests) != NETWORK_REQUESTS:
        raise ContractError(
            "CME attachment-recovery request count is invalid"
        )
    normalized: list[dict[str, object]] = []
    previous_url = ""
    start = REUSED_RESPONSES + EXCLUDED_RESPONSES + 1
    for ordinal, request in enumerate(requests, start=start):
        if (
            not isinstance(request, dict)
            or set(request) != _REQUEST_KEYS
            or request.get("ordinal") != ordinal
            or type(request.get("request_id")) is not str
            or type(request.get("url")) is not str
            or str(request["url"]) <= previous_url
            or request.get("request_kind")
            != "HISTORICAL_NOTICE_ATTACHMENT_CAPTURE"
            or type(request.get("extension")) is not str
            or request.get("extension") not in {".csv", ".pdf", ".xls"}
            or type(request.get("logical_path")) is not str
            or Path(str(request["logical_path"])).name
            != f"{request['request_id']}{request['extension']}"
            or not isinstance(request.get("expected_content_types"), list)
            or not request["expected_content_types"]
        ):
            raise ContractError(
                "CME attachment-recovery request schema is invalid"
            )
        parsed = urllib.parse.urlparse(str(request["url"]))
        if (
            parsed.scheme != "https"
            or parsed.hostname != "www.cmegroup.com"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ContractError(
                "CME attachment-recovery request URL is invalid"
            )
        for key in (
            "discovery_reasons",
            "link_texts",
            "source_notice_request_ids",
            "source_notice_urls",
            "source_titles",
        ):
            if (
                not isinstance(request.get(key), list)
                or any(type(item) is not str for item in request[key])
            ):
                raise ContractError(
                    "CME attachment-recovery request metadata is invalid"
                )
        previous_url = str(request["url"])
        normalized.append(dict(request))
    return normalized


def _validate_exclusion(exclusion: Mapping[str, object]) -> None:
    expected_keys = {
        "classification",
        "diagnostic_id",
        "diagnostic_result_path",
        "diagnostic_result_sha256",
        "ordinal",
        "request_id",
        "status",
        "url",
    }
    if (
        set(exclusion) != expected_keys
        or exclusion.get("ordinal") != REUSED_RESPONSES + 1
        or type(exclusion.get("diagnostic_id")) is not str
        or _SHA256.fullmatch(str(exclusion["diagnostic_id"])) is None
        or type(exclusion.get("diagnostic_result_sha256")) is not str
        or _SHA256.fullmatch(str(exclusion["diagnostic_result_sha256"]))
        is None
        or type(exclusion.get("diagnostic_result_path")) is not str
        or not exclusion["diagnostic_result_path"]
        or type(exclusion.get("request_id")) is not str
        or type(exclusion.get("url")) is not str
        or exclusion.get("status") != "EXCLUDED_AUTHORITATIVE_HTTP_404"
        or exclusion.get("classification")
        != {
            "error_class": "NoticeAttachmentRequestError",
            "failure_code": "HTTP_STATUS_REJECTED",
            "safe_details": {
                "content_type": "text/html",
                "http_status": 404,
            },
        }
    ):
        raise ContractError(
            "CME attachment-recovery exclusion is invalid"
        )


def recovery_authority(
    *,
    predecessor_plan_path: Path,
    predecessor_approval_path: Path,
    failure_report_path: Path,
    diagnostic_plan_path: Path,
    diagnostic_approval_path: Path,
    diagnostic_result_path: Path,
    boundary: RepoBoundary,
) -> tuple[
    dict[str, object],
    list[dict[str, object]],
    list[dict[str, object]],
    dict[str, object],
]:
    for path, purpose, subtree in (
        (
            diagnostic_plan_path,
            "CME attachment diagnostic plan",
            "reports/exchange_calendar",
        ),
        (
            diagnostic_approval_path,
            "CME attachment diagnostic approval",
            "configs",
        ),
        (
            diagnostic_result_path,
            "CME attachment diagnostic result",
            "reports/exchange_calendar",
        ),
    ):
        boundary.assert_active_path(path, purpose=purpose, subtree=subtree)
    predecessor_authority, failed_request, descriptors = (
        preserved_failure_authority(
            predecessor_plan_path=predecessor_plan_path,
            predecessor_approval_path=predecessor_approval_path,
            failure_report_path=failure_report_path,
            boundary=boundary,
        )
    )
    predecessor_plan = validate_attachment_capture_plan(
        _canonical_object(
            predecessor_plan_path,
            description="CME attachment predecessor plan",
        )
    )
    diagnostic_plan = validate_diagnostic_plan(
        _canonical_object(
            diagnostic_plan_path,
            description="CME attachment diagnostic plan",
        )
    )
    diagnostic_scope = diagnostic_plan["scope"]
    assert isinstance(diagnostic_scope, dict)
    diagnostic_request = diagnostic_scope["request"]
    if (
        diagnostic_scope.get("authority") != predecessor_authority
        or not isinstance(diagnostic_request, dict)
        or diagnostic_request.get("predecessor_request_id")
        != failed_request.get("request_id")
        or diagnostic_request.get("url") != failed_request.get("url")
    ):
        raise IntegrityError(
            "CME attachment diagnostic authority changed"
        )
    diagnostic_approval = _canonical_object(
        diagnostic_approval_path,
        description="CME attachment diagnostic approval",
    )
    diagnostic_result = _validate_diagnostic_result(
        plan=diagnostic_plan,
        plan_path=diagnostic_plan_path,
        approval=diagnostic_approval,
        approval_path=diagnostic_approval_path,
        result=_canonical_object(
            diagnostic_result_path,
            description="CME attachment diagnostic result",
        ),
    )
    predecessor_scope = predecessor_plan["scope"]
    assert isinstance(predecessor_scope, dict)
    predecessor_requests = predecessor_scope["requests"]
    predecessor_source = predecessor_scope["authority"]
    if (
        not isinstance(predecessor_requests, list)
        or len(predecessor_requests) != TOTAL_CANDIDATES
        or not isinstance(predecessor_source, dict)
    ):
        raise IntegrityError(
            "CME attachment predecessor request set changed"
        )
    remaining = [
        dict(item)
        for item in predecessor_requests[
            REUSED_RESPONSES + EXCLUDED_RESPONSES :
        ]
        if isinstance(item, dict)
    ]
    remaining = _validate_remaining_requests(remaining)
    classification = diagnostic_result["classification"]
    assert isinstance(classification, dict)
    exclusion = {
        "classification": dict(classification),
        "diagnostic_id": diagnostic_result["diagnostic_id"],
        "diagnostic_result_path": diagnostic_result_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "diagnostic_result_sha256": sha256_file(diagnostic_result_path),
        "ordinal": failed_request["ordinal"],
        "request_id": failed_request["request_id"],
        "status": "EXCLUDED_AUTHORITATIVE_HTTP_404",
        "url": failed_request["url"],
    }
    _validate_exclusion(exclusion)
    authority: dict[str, object] = {
        "diagnostic_approval_path": diagnostic_approval_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "diagnostic_approval_receipt_id": diagnostic_result[
            "approval_receipt_id"
        ],
        "diagnostic_approval_sha256": sha256_file(
            diagnostic_approval_path
        ),
        "diagnostic_failure_code": classification["failure_code"],
        "diagnostic_http_status": classification["safe_details"][  # type: ignore[index]
            "http_status"
        ],
        "diagnostic_id": diagnostic_result["diagnostic_id"],
        "diagnostic_plan_id": diagnostic_plan["plan_id"],
        "diagnostic_plan_path": diagnostic_plan_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "diagnostic_plan_sha256": sha256_file(diagnostic_plan_path),
        "diagnostic_result_path": exclusion["diagnostic_result_path"],
        "diagnostic_result_sha256": exclusion[
            "diagnostic_result_sha256"
        ],
        "excluded_request_id": exclusion["request_id"],
        "excluded_response_count": EXCLUDED_RESPONSES,
        "excluded_url": exclusion["url"],
        "failure_id": predecessor_authority["failure_id"],
        "failure_report_path": predecessor_authority[
            "failure_report_path"
        ],
        "failure_report_sha256": predecessor_authority[
            "failure_report_sha256"
        ],
        "first_remaining_request_id": remaining[0]["request_id"],
        "last_remaining_request_id": remaining[-1]["request_id"],
        "predecessor_approval_path": predecessor_authority[
            "predecessor_approval_path"
        ],
        "predecessor_approval_receipt_id": predecessor_authority[
            "predecessor_approval_receipt_id"
        ],
        "predecessor_approval_sha256": predecessor_authority[
            "predecessor_approval_sha256"
        ],
        "predecessor_plan_id": predecessor_authority[
            "predecessor_plan_id"
        ],
        "predecessor_plan_path": predecessor_authority[
            "predecessor_plan_path"
        ],
        "predecessor_plan_sha256": predecessor_authority[
            "predecessor_plan_sha256"
        ],
        "preserved_response_count": predecessor_authority[
            "preserved_response_count"
        ],
        "preserved_response_set_id": predecessor_authority[
            "preserved_response_set_id"
        ],
        "preserved_stage_relative_path": predecessor_authority[
            "preserved_stage_relative_path"
        ],
        "preserved_total_bytes": predecessor_authority[
            "preserved_total_bytes"
        ],
        "remaining_request_count": len(remaining),
        "source_union_release_id": predecessor_source[
            "union_release_id"
        ],
    }
    _validate_authority(authority)
    return authority, remaining, descriptors, exclusion


def build_recovery_plan(
    *,
    authority: Mapping[str, object],
    remaining_requests: Sequence[object],
    exclusion: Mapping[str, object],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    _validate_authority(authority)
    requests = _validate_remaining_requests(remaining_requests)
    _validate_exclusion(exclusion)
    if (
        exclusion["diagnostic_id"] != authority["diagnostic_id"]
        or exclusion["diagnostic_result_sha256"]
        != authority["diagnostic_result_sha256"]
        or exclusion["request_id"] != authority["excluded_request_id"]
        or exclusion["url"] != authority["excluded_url"]
    ):
        raise ContractError(
            "CME attachment-recovery exclusion differs from authority"
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
            "CME attachment-recovery implementation hashes are invalid"
        )
    scope: dict[str, object] = {
        "allow_redirects": False,
        "authority": dict(authority),
        "excluded_request": dict(exclusion),
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "max_duration_seconds": MAX_DURATION_SECONDS,
        "max_network_bytes": MAX_NETWORK_BYTES,
        "max_network_requests": NETWORK_REQUESTS,
        "max_response_bytes": MAX_RESPONSE_BYTES,
        "max_total_bytes": MAX_TOTAL_BYTES,
        "output_paths": {
            "data_template": (
                "data/reference/exchange_calendars/{release_id}/"
                "{request_id}{extension}"
            ),
            "failure_report": (
                "reports/exchange_calendar/"
                "cme_historical_notice_attachment_recovery_failure_"
                "{plan_prefix}.json"
            ),
            "manifest_template": (
                "manifests/data_releases/reference/{release_id}.json"
            ),
            "publication_lock": "state/locks/data-publication.lock",
            "staging_root": "state/data_publication_staging",
        },
        "purpose": (
            "REUSE_13_HASH_VERIFIED_ATTACHMENTS_EXCLUDE_ONE_DIAGNOSED_"
            "HTTP_404_AND_CAPTURE_ONLY_THE_REMAINING_783_ATTACHMENTS"
        ),
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "requests": requests,
        "retries": 0,
        "reused_response_count": REUSED_RESPONSES,
        "reuse_verification_workers": REUSE_VERIFICATION_WORKERS,
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


def validate_recovery_plan(
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
            "CME attachment-recovery plan schema is invalid"
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
        or not isinstance(scope.get("authority"), dict)
        or not isinstance(scope.get("requests"), list)
        or not isinstance(scope.get("excluded_request"), dict)
        or not isinstance(scope.get("implementation_sha256"), dict)
    ):
        raise IntegrityError(
            "CME attachment-recovery plan identity is invalid"
        )
    expected = build_recovery_plan(
        authority=scope["authority"],
        remaining_requests=scope["requests"],
        exclusion=scope["excluded_request"],
        implementation_sha256=scope["implementation_sha256"],
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME attachment-recovery plan differs from implementation"
        )
    return dict(payload)


def validate_recovery_approval(
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
        raise NoticeAttachmentRecoveryError(
            "CME attachment recovery lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _failure_path(root: Path, plan_id: str) -> Path:
    return (
        root
        / "reports"
        / "exchange_calendar"
        / (
            "cme_historical_notice_attachment_recovery_failure_"
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
            and payload.get("schema_version") == CAPTURE_SCHEMA
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


def _recovery_failure_report(
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
        "excluded_response_count": EXCLUDED_RESPONSES,
        "failed_requests": list(failures),
        "network_requests_attempted": attempted,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "publication_occurred": False,
        "responses_preserved": list(responses),
        "responses_preserved_count": len(responses),
        "retries_performed": 0,
        "reused_response_count": REUSED_RESPONSES,
        "schema_version": FAILURE_SCHEMA,
        "stage_relative_path": stage.relative_to(
            boundary.active_root
        ).as_posix(),
        "status": "STOPPED",
    }
    return {**core, "failure_id": sha256_json(core)}


def capture_attachment_recovery(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_recovery_plan(
        _canonical_object(
            plan_path,
            description="CME attachment-recovery plan",
        )
    )
    approval = _canonical_object(
        approval_path,
        description="CME attachment-recovery approval",
    )
    approval_id = validate_recovery_approval(
        approval,
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    root = publisher.boundary.active_root
    if scope["implementation_sha256"] != implementation_hashes(root):
        raise NoticeAttachmentRecoveryError(
            "CME attachment-recovery implementation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived, remaining, _descriptors, exclusion = recovery_authority(
        predecessor_plan_path=root
        / str(authority["predecessor_plan_path"]),
        predecessor_approval_path=root
        / str(authority["predecessor_approval_path"]),
        failure_report_path=root / str(authority["failure_report_path"]),
        diagnostic_plan_path=root
        / str(authority["diagnostic_plan_path"]),
        diagnostic_approval_path=root
        / str(authority["diagnostic_approval_path"]),
        diagnostic_result_path=root
        / str(authority["diagnostic_result_path"]),
        boundary=publisher.boundary,
    )
    expected = build_recovery_plan(
        authority=derived,
        remaining_requests=remaining,
        exclusion=exclusion,
        implementation_sha256=implementation_hashes(root),
    )
    if authority != derived or plan != expected:
        raise NoticeAttachmentRecoveryError(
            "CME attachment-recovery evidence changed"
        )
    failure_path = _failure_path(root, str(plan["plan_id"]))
    prior_release = _existing_release_for_plan(root, str(plan["plan_id"]))
    if failure_path.exists() or prior_release is not None:
        raise NoticeAttachmentRecoveryError(
            "CME attachment-recovery approval already has an outcome"
        )
    predecessor_failure = _canonical_object(
        root / str(authority["failure_report_path"]),
        description="CME attachment predecessor failure",
    )
    reused_raw = predecessor_failure["responses_preserved"]
    assert isinstance(reused_raw, list)
    source_stage = root / str(authority["preserved_stage_relative_path"])
    requests = scope["requests"]
    assert isinstance(requests, list)
    allowed = {
        str(item["url"]) for item in requests if isinstance(item, dict)
    }
    if (
        len(allowed) != NETWORK_REQUESTS
        or str(authority["excluded_url"]) in allowed
    ):
        raise NoticeAttachmentRecoveryError(
            "CME attachment-recovery allowlist is invalid"
        )
    stage = publisher.create_stage("cme_notice_attachment_recovery")
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    responses: list[dict[str, object]] = []
    total_bytes = 0
    for raw in reused_raw:
        assert isinstance(raw, dict)
        name = Path(str(raw["logical_path"])).name
        source = source_stage / name
        target = stage / name
        shutil.copyfile(source, target)
        if (
            target.stat().st_size != raw["size"]
            or sha256_file(target) != raw["sha256"]
        ):
            raise NoticeAttachmentRecoveryError(
                "CME attachment-recovery copy verification failed"
            )
        logical = str(raw["logical_path"])
        logical_paths[name] = logical
        staged_paths[logical] = name
        total_bytes += target.stat().st_size
        responses.append(
            {**dict(raw), "acquisition": "REUSED_HASH_VERIFIED_STAGE"}
        )
    if (
        len(responses) != REUSED_RESPONSES
        or total_bytes != authority["preserved_total_bytes"]
    ):
        raise NoticeAttachmentRecoveryError(
            "CME attachment-recovery reuse set changed"
        )
    started = monotonic_time.monotonic()
    network_bytes = 0
    attempted = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        for offset in range(0, len(requests), WORKERS):
            elapsed = int(
                (monotonic_time.monotonic() - started) * 1_000
            )
            if elapsed >= MAX_DURATION_SECONDS * 1_000:
                failure = _recovery_failure_report(
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
                raise NoticeAttachmentRecoveryError(
                    "CME attachment-recovery duration ceiling reached"
                )
            batch = requests[offset : offset + WORKERS]
            futures = [
                (
                    spec,
                    executor.submit(
                        fetch_attachment,
                        spec,
                        allowed=allowed,
                    ),
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
                    failures.append(
                        attachment_failure_evidence(exc, spec=spec)
                    )
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
                network_bytes += len(body)
                total_bytes += len(body)
                responses.append(
                    {
                        "acquisition": "NETWORK",
                        "content_type": content_type,
                        "discovery_reasons": spec["discovery_reasons"],
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
            if network_bytes > MAX_NETWORK_BYTES or total_bytes > MAX_TOTAL_BYTES:
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
                    (monotonic_time.monotonic() - started) * 1_000
                )
                failure = _recovery_failure_report(
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
                raise NoticeAttachmentRecoveryError(
                    "CME attachment recovery stopped on request failure"
                )
    responses.sort(key=lambda item: int(item["ordinal"]))
    elapsed = int((monotonic_time.monotonic() - started) * 1_000)
    if (
        elapsed > MAX_DURATION_SECONDS * 1_000
        or attempted != NETWORK_REQUESTS
        or len(responses) != TOTAL_PAYLOADS
        or network_bytes > MAX_NETWORK_BYTES
        or total_bytes > MAX_TOTAL_BYTES
    ):
        failure = _recovery_failure_report(
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
        raise NoticeAttachmentRecoveryError(
            "CME attachment-recovery final completion bound failed"
        )
    capture_core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "authority": authority,
        "bounds": {
            "allow_redirects": False,
            "max_duration_seconds": MAX_DURATION_SECONDS,
            "max_network_bytes": MAX_NETWORK_BYTES,
            "max_network_requests": NETWORK_REQUESTS,
            "max_response_bytes": MAX_RESPONSE_BYTES,
            "max_total_bytes": MAX_TOTAL_BYTES,
            "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "retries": 0,
            "reuse_verification_workers": REUSE_VERIFICATION_WORKERS,
            "workers": WORKERS,
        },
        "capture_approval": dict(approval),
        "elapsed_milliseconds": elapsed,
        "excluded_requests": [scope["excluded_request"]],
        "excluded_response_count": EXCLUDED_RESPONSES,
        "network_bytes": network_bytes,
        "network_request_count": NETWORK_REQUESTS,
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "responses": responses,
        "reused_response_count": REUSED_RESPONSES,
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
        source_release_ids=(str(authority["source_union_release_id"]),),
        embedded_documents={"capture_receipt.json": capture},
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture["capture_id"],
            "diagnostic_id": authority["diagnostic_id"],
            "excluded_response_count": EXCLUDED_RESPONSES,
            "plan_id": plan["plan_id"],
            "predecessor_failure_id": authority["failure_id"],
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
    load_attachment_recovery_capture(
        receipt,
        boundary=publisher.boundary,
    )
    return receipt


def load_attachment_recovery_capture(
    receipt: DataReleaseReceipt,
    *,
    boundary: RepoBoundary,
    verify_payload_files: bool = True,
) -> dict[str, object]:
    DataReleaseReceipt.from_dict(receipt.as_dict())
    if receipt.repository_id != boundary.repository_id:
        raise IntegrityError(
            "CME attachment-recovery receipt belongs to another repository"
        )
    manifest_path = boundary.active_root / receipt.manifest_path
    manifest = verify_data_release_manifest(
        manifest_path,
        boundary,
        verify_files=verify_payload_files,
    )
    if (
        receipt.phase != "reference"
        or manifest.release_id != receipt.release_id
        or manifest.release_kind != RELEASE_KIND
        or manifest.release_kind != receipt.release_kind
        or manifest.schema_version != CAPTURE_SCHEMA
        or manifest.schema_version != receipt.schema_version
        or sha256_file(manifest_path) != receipt.manifest_sha256
        or len(manifest.files) != TOTAL_PAYLOADS
        or set(manifest.embedded_documents) != {"capture_receipt.json"}
    ):
        raise IntegrityError(
            "CME attachment-recovery release is invalid"
        )
    raw = manifest.embedded_documents["capture_receipt.json"]
    if not isinstance(raw, dict):
        raise IntegrityError(
            "CME attachment-recovery receipt is invalid"
        )
    core = dict(raw)
    capture_id = core.pop("capture_id", None)
    responses = raw.get("responses")
    exclusions = raw.get("excluded_requests")
    authority = raw.get("authority")
    if (
        type(capture_id) is not str
        or capture_id != sha256_json(core)
        or raw.get("schema_version") != CAPTURE_SCHEMA
        or raw.get("operation") != OPERATION
        or raw.get("network_request_count") != NETWORK_REQUESTS
        or raw.get("reused_response_count") != REUSED_RESPONSES
        or raw.get("excluded_response_count") != EXCLUDED_RESPONSES
        or not isinstance(responses, list)
        or len(responses) != TOTAL_PAYLOADS
        or not isinstance(exclusions, list)
        or len(exclusions) != EXCLUDED_RESPONSES
        or not isinstance(authority, dict)
        or manifest.metadata.get("capture_id") != capture_id
        or manifest.metadata.get("diagnostic_id")
        != authority.get("diagnostic_id")
    ):
        raise IntegrityError(
            "CME attachment-recovery receipt contract is invalid"
        )
    _validate_exclusion(exclusions[0])
    ordinals = [item.get("ordinal") for item in responses if isinstance(item, dict)]
    if (
        len(ordinals) != TOTAL_PAYLOADS
        or len(set(ordinals)) != TOTAL_PAYLOADS
        or REUSED_RESPONSES + 1 in ordinals
    ):
        raise IntegrityError(
            "CME attachment-recovery response set is invalid"
        )
    return dict(raw)
