"""Availability-tolerant reconciliation of historical CME attachments."""

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
    NoticeAttachmentRequestError,
    _failure_evidence as attachment_failure_evidence,
    _fetch as fetch_attachment,
    validate_attachment_capture_plan,
)
from .calendar_notice_attachment_recovery import (
    FAILURE_SCHEMA as PREDECESSOR_FAILURE_SCHEMA,
    OPERATION as PREDECESSOR_OPERATION,
    recovery_authority as predecessor_recovery_authority,
    validate_recovery_approval as validate_predecessor_approval,
    validate_recovery_plan as validate_predecessor_plan,
)
from .canonical import canonical_bytes, fsync_directory, sha256_file, sha256_json
from .data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
    verify_data_release_manifest,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation


PLAN_SCHEMA = "cme_historical_notice_attachment_reconciliation_plan/1.0.0"
APPROVAL_SCHEMA = (
    "cme_historical_notice_attachment_reconciliation_approval/1.0.0"
)
CAPTURE_SCHEMA = "cme_historical_notice_attachment_capture/3.0.0"
FAILURE_SCHEMA = (
    "cme_historical_notice_attachment_reconciliation_failure/1.0.0"
)
OPERATION = (
    "RECONCILE_BOUNDED_PUBLIC_CME_HISTORICAL_NOTICE_ATTACHMENTS"
)
TOTAL_CANDIDATES = 797
REUSED_RESPONSES = 14
KNOWN_EXCLUSIONS = 2
NETWORK_REQUESTS = 781
FIRST_NETWORK_ORDINAL = 17
KNOWN_EXCLUSION_ORDINALS = (14, 15)
RECOVERY_COMPLETED_ORDINAL = 16
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
    "src/futures_rebuild/calendar_notice_attachment_reconciliation.py",
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
    "REQUEST_KNOWN_EXCLUDED_ATTACHMENT",
    "RETRY_OR_REDIRECT_REQUEST",
    "USE_CREDENTIAL_OR_SECRET",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "BYTE_OR_DURATION_CEILING_REACHED",
    "HTTP_STATUS_OTHER_THAN_200_OR_404",
    "IMPLEMENTATION_HASH_DRIFT",
    "MIME_OR_RESPONSE_URL_MISMATCH",
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PREDECESSOR_FAILURE_OR_STAGE_DRIFT",
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
    "known_exclusion_set_id",
    "predecessor_approval_receipt_id",
    "predecessor_approval_sha256",
    "predecessor_failure_id",
    "predecessor_failure_sha256",
    "predecessor_plan_id",
    "predecessor_plan_sha256",
    "preserved_response_set_id",
    "source_union_release_id",
}
_AUTHORITY_PATH_KEYS = {
    "predecessor_approval_path",
    "predecessor_failure_path",
    "predecessor_plan_path",
    "preserved_stage_relative_path",
}
_AUTHORITY_TEXT_KEYS = {
    "first_remaining_request_id",
    "last_remaining_request_id",
}
_AUTHORITY_COUNT_KEYS = {
    "known_exclusion_count",
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


class NoticeAttachmentReconciliationError(UnauthorizedOperation):
    """Raised before or during bounded attachment reconciliation."""


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
                "CME attachment-reconciliation implementation input is "
                f"missing: {relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _validate_authority(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError(
            "CME attachment-reconciliation authority schema is invalid"
        )
    for key in _AUTHORITY_HASH_KEYS:
        value = authority.get(key)
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise ContractError(
                "CME attachment-reconciliation authority hash is invalid"
            )
    for key in _AUTHORITY_PATH_KEYS | _AUTHORITY_TEXT_KEYS:
        if type(authority.get(key)) is not str or not authority[key]:
            raise ContractError(
                "CME attachment-reconciliation authority text is invalid"
            )
    for key in _AUTHORITY_COUNT_KEYS:
        if type(authority.get(key)) is not int or int(authority[key]) < 0:
            raise ContractError(
                "CME attachment-reconciliation authority count is invalid"
            )
    if (
        authority["known_exclusion_count"] != KNOWN_EXCLUSIONS
        or authority["preserved_response_count"] != REUSED_RESPONSES
        or authority["remaining_request_count"] != NETWORK_REQUESTS
        or int(authority["preserved_total_bytes"]) < REUSED_RESPONSES
        or int(authority["preserved_total_bytes"]) > MAX_TOTAL_BYTES
    ):
        raise ContractError(
            "CME attachment-reconciliation authority bounds are invalid"
        )


def _validate_request(
    request: Mapping[str, object],
    *,
    expected_ordinal: int,
) -> dict[str, object]:
    if (
        set(request) != _REQUEST_KEYS
        or request.get("ordinal") != expected_ordinal
        or type(request.get("request_id")) is not str
        or type(request.get("url")) is not str
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
            "CME attachment-reconciliation request schema is invalid"
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
            "CME attachment-reconciliation request URL is invalid"
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
                "CME attachment-reconciliation request metadata is invalid"
            )
    return dict(request)


def _validate_remaining_requests(
    requests: Sequence[object],
) -> list[dict[str, object]]:
    if len(requests) != NETWORK_REQUESTS:
        raise ContractError(
            "CME attachment-reconciliation request count is invalid"
        )
    normalized: list[dict[str, object]] = []
    previous_url = ""
    for ordinal, raw in enumerate(
        requests,
        start=FIRST_NETWORK_ORDINAL,
    ):
        if not isinstance(raw, dict):
            raise ContractError(
                "CME attachment-reconciliation request is invalid"
            )
        request = _validate_request(raw, expected_ordinal=ordinal)
        if str(request["url"]) <= previous_url:
            raise ContractError(
                "CME attachment-reconciliation ordering is invalid"
            )
        previous_url = str(request["url"])
        normalized.append(request)
    return normalized


def _validate_exclusion(
    exclusion: Mapping[str, object],
) -> dict[str, object]:
    expected_keys = {
        "classification",
        "evidence_id",
        "evidence_kind",
        "evidence_path",
        "evidence_sha256",
        "ordinal",
        "request_id",
        "status",
        "url",
    }
    classification = exclusion.get("classification")
    if (
        set(exclusion) != expected_keys
        or type(exclusion.get("evidence_id")) is not str
        or _SHA256.fullmatch(str(exclusion["evidence_id"])) is None
        or exclusion.get("evidence_kind")
        not in {
            "DIAGNOSTIC_RESULT",
            "RECONCILIATION_NETWORK_404",
            "RECOVERY_FAILURE",
        }
        or type(exclusion.get("evidence_path")) is not str
        or not exclusion["evidence_path"]
        or type(exclusion.get("evidence_sha256")) is not str
        or _SHA256.fullmatch(str(exclusion["evidence_sha256"])) is None
        or type(exclusion.get("ordinal")) is not int
        or type(exclusion.get("request_id")) is not str
        or type(exclusion.get("url")) is not str
        or exclusion.get("status") != "EXCLUDED_AUTHORITATIVE_HTTP_404"
        or not isinstance(classification, dict)
        or classification.get("failure_code") != "HTTP_STATUS_REJECTED"
        or not isinstance(classification.get("safe_details"), dict)
        or classification["safe_details"].get("http_status") != 404
    ):
        raise ContractError(
            "CME attachment-reconciliation exclusion is invalid"
        )
    return dict(exclusion)


def _validate_known_exclusions(
    exclusions: Sequence[object],
) -> list[dict[str, object]]:
    if len(exclusions) != KNOWN_EXCLUSIONS:
        raise ContractError(
            "CME attachment-reconciliation exclusion count is invalid"
        )
    normalized = [
        _validate_exclusion(item)
        for item in exclusions
        if isinstance(item, dict)
    ]
    if (
        len(normalized) != KNOWN_EXCLUSIONS
        or [item["ordinal"] for item in normalized]
        != list(KNOWN_EXCLUSION_ORDINALS)
        or len({item["url"] for item in normalized}) != KNOWN_EXCLUSIONS
    ):
        raise ContractError(
            "CME attachment-reconciliation exclusion set is invalid"
        )
    return normalized


def _validate_predecessor_failure(
    *,
    plan: Mapping[str, object],
    plan_path: Path,
    approval: Mapping[str, object],
    failure: Mapping[str, object],
    failure_path: Path,
    boundary: RepoBoundary,
) -> tuple[
    Path,
    list[dict[str, object]],
    int,
    dict[str, object],
]:
    core = dict(failure)
    failure_id = core.pop("failure_id", None)
    expected_keys = {
        "approval_receipt_id",
        "elapsed_milliseconds",
        "excluded_response_count",
        "failed_requests",
        "failure_id",
        "network_requests_attempted",
        "plan_id",
        "plan_sha256",
        "publication_occurred",
        "responses_preserved",
        "responses_preserved_count",
        "retries_performed",
        "reused_response_count",
        "schema_version",
        "stage_relative_path",
        "status",
    }
    scope = plan.get("scope")
    responses = failure.get("responses_preserved")
    failed = failure.get("failed_requests")
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
        or failure.get("network_requests_attempted") != WORKERS
        or failure.get("responses_preserved_count") != REUSED_RESPONSES
        or failure.get("reused_response_count") != REUSED_RESPONSES - 1
        or failure.get("excluded_response_count") != 1
        or not isinstance(responses, list)
        or len(responses) != REUSED_RESPONSES
        or not isinstance(failed, list)
        or len(failed) != 1
        or not isinstance(failed[0], dict)
        or failed[0].get("failure_code") != "HTTP_STATUS_REJECTED"
        or not isinstance(failed[0].get("safe_details"), dict)
        or failed[0]["safe_details"].get("http_status") != 404
        or not isinstance(scope, dict)
    ):
        raise IntegrityError(
            "CME attachment-reconciliation predecessor failure is invalid"
        )
    requests = scope.get("requests")
    if not isinstance(requests, list) or len(requests) < WORKERS:
        raise IntegrityError(
            "CME attachment-reconciliation predecessor requests are invalid"
        )
    failed_request = requests[0]
    completed_request = requests[1]
    if (
        not isinstance(failed_request, dict)
        or not isinstance(completed_request, dict)
        or failed[0].get("request_id")
        != failed_request.get("request_id")
        or failed[0].get("url") != failed_request.get("url")
        or failed_request.get("ordinal") != KNOWN_EXCLUSION_ORDINALS[-1]
        or completed_request.get("ordinal") != RECOVERY_COMPLETED_ORDINAL
    ):
        raise IntegrityError(
            "CME attachment-reconciliation stop position is invalid"
        )
    stage_relative = failure.get("stage_relative_path")
    if (
        type(stage_relative) is not str
        or not stage_relative.startswith("state/data_publication_staging/")
    ):
        raise IntegrityError(
            "CME attachment-reconciliation stage path is invalid"
        )
    stage = boundary.assert_active_path(
        boundary.active_root / stage_relative,
        purpose="CME attachment-reconciliation predecessor stage",
        subtree="state/data_publication_staging",
    )
    if not stage.is_dir() or stage.is_symlink():
        raise IntegrityError(
            "CME attachment-reconciliation predecessor stage is absent"
        )
    files = sorted(path for path in stage.iterdir() if path.is_file())
    if (
        len(files) != REUSED_RESPONSES
        or any(path.is_symlink() for path in files)
    ):
        raise IntegrityError(
            "CME attachment-reconciliation predecessor files are invalid"
        )
    predecessor_authority = scope.get("authority")
    if not isinstance(predecessor_authority, dict):
        raise IntegrityError(
            "CME attachment-reconciliation predecessor authority is invalid"
        )
    original_plan_path = boundary.active_root / str(
        predecessor_authority["predecessor_plan_path"]
    )
    original_plan = validate_attachment_capture_plan(
        _canonical_object(
            original_plan_path,
            description="CME original attachment plan",
        )
    )
    original_scope = original_plan["scope"]
    assert isinstance(original_scope, dict)
    original_requests = original_scope["requests"]
    assert isinstance(original_requests, list)
    request_by_ordinal = {
        int(item["ordinal"]): item
        for item in original_requests
        if isinstance(item, dict)
    }
    expected_ordinals = {
        *range(1, REUSED_RESPONSES),
        RECOVERY_COMPLETED_ORDINAL,
    }
    descriptors: list[dict[str, object]] = []
    expected_names: set[str] = set()
    total = 0
    files_by_name = {path.name: path for path in files}
    verification = []
    with ThreadPoolExecutor(
        max_workers=REUSE_VERIFICATION_WORKERS
    ) as executor:
        for response in responses:
            if not isinstance(response, dict):
                raise IntegrityError(
                    "CME attachment-reconciliation response is invalid"
                )
            ordinal = response.get("ordinal")
            spec = (
                request_by_ordinal.get(int(ordinal))
                if type(ordinal) is int
                else None
            )
            logical = response.get("logical_path")
            if (
                spec is None
                or ordinal not in expected_ordinals
                or response.get("request_id") != spec.get("request_id")
                or response.get("url") != spec.get("url")
                or response.get("status_code") != 200
                or response.get("content_type")
                not in spec.get("expected_content_types", [])
                or type(logical) is not str
                or type(response.get("sha256")) is not str
                or _SHA256.fullmatch(str(response["sha256"])) is None
                or type(response.get("size")) is not int
                or int(response["size"]) < 1
                or int(response["size"]) > MAX_RESPONSE_BYTES
            ):
                raise IntegrityError(
                    "CME attachment-reconciliation response changed"
                )
            name = Path(logical).name
            source = files_by_name.get(name)
            if source is None:
                raise IntegrityError(
                    "CME attachment-reconciliation payload is absent"
                )
            expected_names.add(name)
            verification.append(
                (
                    response,
                    executor.submit(
                        lambda path: (
                            path.stat().st_size,
                            sha256_file(path),
                        ),
                        source,
                    ),
                )
            )
        for response, future in verification:
            size, digest = future.result()
            if size != response["size"] or digest != response["sha256"]:
                raise IntegrityError(
                    "CME attachment-reconciliation bytes changed"
                )
            total += size
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
    if {path.name for path in files} != expected_names:
        raise IntegrityError(
            "CME attachment-reconciliation stage has unexpected files"
        )
    descriptors.sort(key=lambda item: int(item["ordinal"]))
    return stage, descriptors, total, dict(failed_request)


def reconciliation_authority(
    *,
    predecessor_plan_path: Path,
    predecessor_approval_path: Path,
    predecessor_failure_path: Path,
    boundary: RepoBoundary,
) -> tuple[
    dict[str, object],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    for path, purpose, subtree in (
        (
            predecessor_plan_path,
            "CME attachment-recovery predecessor plan",
            "reports/exchange_calendar",
        ),
        (
            predecessor_approval_path,
            "CME attachment-recovery predecessor approval",
            "configs",
        ),
        (
            predecessor_failure_path,
            "CME attachment-recovery predecessor failure",
            "reports/exchange_calendar",
        ),
    ):
        boundary.assert_active_path(path, purpose=purpose, subtree=subtree)
    plan = validate_predecessor_plan(
        _canonical_object(
            predecessor_plan_path,
            description="CME attachment-recovery predecessor plan",
        )
    )
    if plan.get("operation") != PREDECESSOR_OPERATION:
        raise IntegrityError(
            "CME attachment-recovery predecessor operation is invalid"
        )
    approval = _canonical_object(
        predecessor_approval_path,
        description="CME attachment-recovery predecessor approval",
    )
    approval_id = validate_predecessor_approval(
        approval,
        plan=plan,
        plan_sha256=sha256_file(predecessor_plan_path),
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    old_authority = scope["authority"]
    assert isinstance(old_authority, dict)
    derived, old_remaining, _old_descriptors, old_exclusion = (
        predecessor_recovery_authority(
            predecessor_plan_path=boundary.active_root
            / str(old_authority["predecessor_plan_path"]),
            predecessor_approval_path=boundary.active_root
            / str(old_authority["predecessor_approval_path"]),
            failure_report_path=boundary.active_root
            / str(old_authority["failure_report_path"]),
            diagnostic_plan_path=boundary.active_root
            / str(old_authority["diagnostic_plan_path"]),
            diagnostic_approval_path=boundary.active_root
            / str(old_authority["diagnostic_approval_path"]),
            diagnostic_result_path=boundary.active_root
            / str(old_authority["diagnostic_result_path"]),
            boundary=boundary,
        )
    )
    if (
        old_authority != derived
        or scope["requests"] != old_remaining
        or scope["excluded_request"] != old_exclusion
    ):
        raise IntegrityError(
            "CME attachment-recovery predecessor evidence changed"
        )
    failure = _canonical_object(
        predecessor_failure_path,
        description="CME attachment-recovery predecessor failure",
    )
    stage, descriptors, total, failed_request = (
        _validate_predecessor_failure(
            plan=plan,
            plan_path=predecessor_plan_path,
            approval=approval,
            failure=failure,
            failure_path=predecessor_failure_path,
            boundary=boundary,
        )
    )
    remaining = [
        dict(item)
        for item in old_remaining[WORKERS:]
        if isinstance(item, dict)
    ]
    remaining = _validate_remaining_requests(remaining)
    first_exclusion = {
        "classification": old_exclusion["classification"],
        "evidence_id": old_exclusion["diagnostic_id"],
        "evidence_kind": "DIAGNOSTIC_RESULT",
        "evidence_path": old_exclusion["diagnostic_result_path"],
        "evidence_sha256": old_exclusion[
            "diagnostic_result_sha256"
        ],
        "ordinal": old_exclusion["ordinal"],
        "request_id": old_exclusion["request_id"],
        "status": "EXCLUDED_AUTHORITATIVE_HTTP_404",
        "url": old_exclusion["url"],
    }
    second_exclusion = {
        "classification": {
            "error_class": failure["failed_requests"][0]["error_class"],
            "failure_code": failure["failed_requests"][0]["failure_code"],
            "safe_details": failure["failed_requests"][0]["safe_details"],
        },
        "evidence_id": failure["failure_id"],
        "evidence_kind": "RECOVERY_FAILURE",
        "evidence_path": predecessor_failure_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "evidence_sha256": sha256_file(predecessor_failure_path),
        "ordinal": failed_request["ordinal"],
        "request_id": failed_request["request_id"],
        "status": "EXCLUDED_AUTHORITATIVE_HTTP_404",
        "url": failed_request["url"],
    }
    exclusions = _validate_known_exclusions(
        [first_exclusion, second_exclusion]
    )
    source_union_release_id = old_authority[
        "source_union_release_id"
    ]
    authority: dict[str, object] = {
        "first_remaining_request_id": remaining[0]["request_id"],
        "known_exclusion_count": len(exclusions),
        "known_exclusion_set_id": sha256_json(exclusions),
        "last_remaining_request_id": remaining[-1]["request_id"],
        "predecessor_approval_path": predecessor_approval_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "predecessor_approval_receipt_id": approval_id,
        "predecessor_approval_sha256": sha256_file(
            predecessor_approval_path
        ),
        "predecessor_failure_id": failure["failure_id"],
        "predecessor_failure_path": predecessor_failure_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "predecessor_failure_sha256": sha256_file(
            predecessor_failure_path
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
        "remaining_request_count": len(remaining),
        "source_union_release_id": source_union_release_id,
    }
    _validate_authority(authority)
    return authority, remaining, descriptors, exclusions


def build_reconciliation_plan(
    *,
    authority: Mapping[str, object],
    remaining_requests: Sequence[object],
    known_exclusions: Sequence[object],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    _validate_authority(authority)
    requests = _validate_remaining_requests(remaining_requests)
    exclusions = _validate_known_exclusions(known_exclusions)
    if (
        sha256_json(exclusions) != authority["known_exclusion_set_id"]
        or {item["url"] for item in exclusions}
        & {item["url"] for item in requests}
    ):
        raise ContractError(
            "CME attachment-reconciliation exclusions differ from authority"
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
            "CME attachment-reconciliation hashes are invalid"
        )
    scope: dict[str, object] = {
        "allow_http_404_exclusion_and_continue": True,
        "allow_redirects": False,
        "authority": dict(authority),
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "known_exclusions": exclusions,
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
                "cme_historical_notice_attachment_reconciliation_failure_"
                "{plan_prefix}.json"
            ),
            "manifest_template": (
                "manifests/data_releases/reference/{release_id}.json"
            ),
            "publication_lock": "state/locks/data-publication.lock",
            "staging_root": "state/data_publication_staging",
        },
        "purpose": (
            "REUSE_14_HASH_VERIFIED_PAYLOADS_BIND_TWO_KNOWN_404S_AND_"
            "RECONCILE_THE_REMAINING_781_URLS_AS_VALID_PAYLOAD_OR_404"
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


def validate_reconciliation_plan(
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
            "CME attachment-reconciliation plan schema is invalid"
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
        or not isinstance(scope.get("known_exclusions"), list)
        or not isinstance(scope.get("implementation_sha256"), dict)
    ):
        raise IntegrityError(
            "CME attachment-reconciliation plan identity is invalid"
        )
    expected = build_reconciliation_plan(
        authority=scope["authority"],
        remaining_requests=scope["requests"],
        known_exclusions=scope["known_exclusions"],
        implementation_sha256=scope["implementation_sha256"],
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME attachment-reconciliation plan differs from implementation"
        )
    return dict(payload)


def validate_reconciliation_approval(
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
        raise NoticeAttachmentReconciliationError(
            "CME attachment reconciliation lacks exact approval"
        )
    return str(approval["approval_receipt_id"])


def _failure_path(root: Path, plan_id: str) -> Path:
    return (
        root
        / "reports"
        / "exchange_calendar"
        / (
            "cme_historical_notice_attachment_reconciliation_failure_"
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


def _network_exclusion(
    *,
    spec: Mapping[str, object],
    exc: NoticeAttachmentRequestError,
) -> dict[str, object] | None:
    evidence = exc.evidence()
    details = evidence.get("safe_details")
    if (
        evidence.get("failure_code") != "HTTP_STATUS_REJECTED"
        or not isinstance(details, dict)
        or details.get("http_status") != 404
    ):
        return None
    return {
        "classification": evidence,
        "evidence_id": sha256_json(
            {
                "classification": evidence,
                "ordinal": spec["ordinal"],
                "request_id": spec["request_id"],
                "url": spec["url"],
            }
        ),
        "evidence_kind": "RECONCILIATION_NETWORK_404",
        "evidence_path": "EMBEDDED_CAPTURE_RECEIPT",
        "evidence_sha256": sha256_json(evidence),
        "ordinal": spec["ordinal"],
        "request_id": spec["request_id"],
        "status": "EXCLUDED_AUTHORITATIVE_HTTP_404",
        "url": spec["url"],
    }


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
        "exclusions_preserved": list(exclusions),
        "exclusions_preserved_count": len(exclusions),
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


def capture_attachment_reconciliation(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_reconciliation_plan(
        _canonical_object(
            plan_path,
            description="CME attachment-reconciliation plan",
        )
    )
    approval = _canonical_object(
        approval_path,
        description="CME attachment-reconciliation approval",
    )
    approval_id = validate_reconciliation_approval(
        approval,
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    root = publisher.boundary.active_root
    if scope["implementation_sha256"] != implementation_hashes(root):
        raise NoticeAttachmentReconciliationError(
            "CME attachment-reconciliation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived, remaining, _descriptors, known_exclusions = (
        reconciliation_authority(
            predecessor_plan_path=root
            / str(authority["predecessor_plan_path"]),
            predecessor_approval_path=root
            / str(authority["predecessor_approval_path"]),
            predecessor_failure_path=root
            / str(authority["predecessor_failure_path"]),
            boundary=publisher.boundary,
        )
    )
    expected = build_reconciliation_plan(
        authority=derived,
        remaining_requests=remaining,
        known_exclusions=known_exclusions,
        implementation_sha256=implementation_hashes(root),
    )
    if authority != derived or plan != expected:
        raise NoticeAttachmentReconciliationError(
            "CME attachment-reconciliation evidence changed"
        )
    failure_path = _failure_path(root, str(plan["plan_id"]))
    prior_release = _existing_release_for_plan(root, str(plan["plan_id"]))
    if failure_path.exists() or prior_release is not None:
        raise NoticeAttachmentReconciliationError(
            "CME attachment-reconciliation already has an outcome"
        )
    predecessor_failure = _canonical_object(
        root / str(authority["predecessor_failure_path"]),
        description="CME attachment-reconciliation predecessor failure",
    )
    reused_raw = predecessor_failure["responses_preserved"]
    assert isinstance(reused_raw, list)
    source_stage = root / str(authority["preserved_stage_relative_path"])
    requests = scope["requests"]
    assert isinstance(requests, list)
    allowed = {
        str(item["url"]) for item in requests if isinstance(item, dict)
    }
    excluded_urls = {str(item["url"]) for item in known_exclusions}
    if (
        len(allowed) != NETWORK_REQUESTS
        or allowed & excluded_urls
    ):
        raise NoticeAttachmentReconciliationError(
            "CME attachment-reconciliation allowlist is invalid"
        )
    stage = publisher.create_stage("cme_notice_attachment_reconciliation")
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    responses: list[dict[str, object]] = []
    exclusions = [dict(item) for item in known_exclusions]
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
            raise NoticeAttachmentReconciliationError(
                "CME attachment-reconciliation copy verification failed"
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
        raise NoticeAttachmentReconciliationError(
            "CME attachment-reconciliation reuse set changed"
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
                failure = _failure_report(
                    plan=plan,
                    plan_path=plan_path,
                    approval_id=approval_id,
                    stage=stage,
                    attempted=attempted,
                    responses=responses,
                    exclusions=exclusions,
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
                raise NoticeAttachmentReconciliationError(
                    "CME attachment-reconciliation duration limit reached"
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
                except NoticeAttachmentRequestError as exc:
                    exclusion = _network_exclusion(spec=spec, exc=exc)
                    if exclusion is not None:
                        exclusions.append(exclusion)
                    else:
                        failures.append(
                            attachment_failure_evidence(exc, spec=spec)
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
                failure = _failure_report(
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
                _write_create_only(failure_path, failure)
                raise NoticeAttachmentReconciliationError(
                    "CME attachment reconciliation stopped on failure"
                )
    responses.sort(key=lambda item: int(item["ordinal"]))
    exclusions.sort(key=lambda item: int(item["ordinal"]))
    elapsed = int((monotonic_time.monotonic() - started) * 1_000)
    resolved_ordinals = {
        int(item["ordinal"]) for item in responses
    } | {int(item["ordinal"]) for item in exclusions}
    if (
        elapsed > MAX_DURATION_SECONDS * 1_000
        or attempted != NETWORK_REQUESTS
        or len(responses) + len(exclusions) != TOTAL_CANDIDATES
        or resolved_ordinals != set(range(1, TOTAL_CANDIDATES + 1))
        or network_bytes > MAX_NETWORK_BYTES
        or total_bytes > MAX_TOTAL_BYTES
    ):
        failure = _failure_report(
            plan=plan,
            plan_path=plan_path,
            approval_id=approval_id,
            stage=stage,
            attempted=attempted,
            responses=responses,
            exclusions=exclusions,
            failures=[
                {
                    "error_class": "FINAL_RECONCILIATION_BOUND_FAILED",
                    "failure_code": "FINAL_RECONCILIATION_BOUND_FAILED",
                    "safe_details": {},
                }
            ],
            elapsed_milliseconds=elapsed,
            boundary=publisher.boundary,
        )
        _write_create_only(failure_path, failure)
        raise NoticeAttachmentReconciliationError(
            "CME attachment reconciliation is incomplete"
        )
    capture_core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "authority": authority,
        "bounds": {
            "allow_http_404_exclusion_and_continue": True,
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
        "exclusions": exclusions,
        "exclusions_count": len(exclusions),
        "network_bytes": network_bytes,
        "network_request_count": NETWORK_REQUESTS,
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "resolved_candidate_count": TOTAL_CANDIDATES,
        "responses": responses,
        "responses_count": len(responses),
        "reused_response_count": REUSED_RESPONSES,
        "schema_version": CAPTURE_SCHEMA,
        "total_bytes": total_bytes,
        "unresolved_candidate_count": 0,
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
            "exclusions_count": len(exclusions),
            "plan_id": plan["plan_id"],
            "predecessor_failure_id": authority[
                "predecessor_failure_id"
            ],
            "unresolved_candidate_count": 0,
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
    load_attachment_reconciliation_capture(
        receipt,
        boundary=publisher.boundary,
    )
    return receipt


def load_attachment_reconciliation_capture(
    receipt: DataReleaseReceipt,
    *,
    boundary: RepoBoundary,
    verify_payload_files: bool = True,
) -> dict[str, object]:
    DataReleaseReceipt.from_dict(receipt.as_dict())
    if receipt.repository_id != boundary.repository_id:
        raise IntegrityError(
            "CME attachment-reconciliation receipt belongs elsewhere"
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
        or set(manifest.embedded_documents) != {"capture_receipt.json"}
    ):
        raise IntegrityError(
            "CME attachment-reconciliation release is invalid"
        )
    raw = manifest.embedded_documents["capture_receipt.json"]
    if not isinstance(raw, dict):
        raise IntegrityError(
            "CME attachment-reconciliation receipt is invalid"
        )
    core = dict(raw)
    capture_id = core.pop("capture_id", None)
    responses = raw.get("responses")
    exclusions = raw.get("exclusions")
    if (
        type(capture_id) is not str
        or capture_id != sha256_json(core)
        or raw.get("schema_version") != CAPTURE_SCHEMA
        or raw.get("operation") != OPERATION
        or raw.get("network_request_count") != NETWORK_REQUESTS
        or raw.get("reused_response_count") != REUSED_RESPONSES
        or raw.get("resolved_candidate_count") != TOTAL_CANDIDATES
        or raw.get("unresolved_candidate_count") != 0
        or not isinstance(responses, list)
        or not isinstance(exclusions, list)
        or len(responses) != raw.get("responses_count")
        or len(exclusions) != raw.get("exclusions_count")
        or len(responses) + len(exclusions) != TOTAL_CANDIDATES
        or len(manifest.files) != len(responses)
        or any(not isinstance(item, dict) for item in responses)
        or any(not isinstance(item, dict) for item in exclusions)
        or manifest.metadata.get("capture_id") != capture_id
        or manifest.metadata.get("unresolved_candidate_count") != 0
    ):
        raise IntegrityError(
            "CME attachment-reconciliation receipt contract is invalid"
        )
    for exclusion in exclusions:
        _validate_exclusion(exclusion)
    ordinals = {
        int(item["ordinal"])
        for item in responses + exclusions
        if isinstance(item, dict) and type(item.get("ordinal")) is int
    }
    if ordinals != set(range(1, TOTAL_CANDIDATES + 1)):
        raise IntegrityError(
            "CME attachment-reconciliation ordinal set is invalid"
        )
    return dict(raw)
