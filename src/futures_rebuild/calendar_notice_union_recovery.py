"""Hash-bound recovery for a stopped complete CME notice-document union."""

from __future__ import annotations

import json
import os
import re
import shutil
import time as monotonic_time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import RepoBoundary
from .calendar_notice_union import (
    FAILURE_SCHEMA as PREDECESSOR_FAILURE_SCHEMA,
)
from .calendar_notice_union import (
    PLAN_SCHEMA as PREDECESSOR_PLAN_SCHEMA,
)
from .calendar_notice_union import (
    RELEASE_KIND as UNION_RELEASE_KIND,
)
from .calendar_notice_union import validate_union_plan
from .canonical import canonical_bytes, fsync_directory, sha256_file, sha256_json
from .data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
    verify_data_release_manifest,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation


PLAN_SCHEMA = "cme_historical_notice_document_union_recovery_plan/1.0.0"
APPROVAL_SCHEMA = (
    "cme_historical_notice_document_union_recovery_approval/1.0.0"
)
CAPTURE_SCHEMA = "cme_historical_notice_document_union_capture/2.0.0"
FAILURE_SCHEMA = (
    "cme_historical_notice_document_union_recovery_failure/1.0.0"
)
OPERATION = (
    "CAPTURE_BOUNDED_PUBLIC_CME_HISTORICAL_NOTICE_DOCUMENT_UNION_RECOVERY"
)
RELEASE_KIND = UNION_RELEASE_KIND
TOTAL_REQUESTS = 1_273
REUSED_REQUESTS = 1_094
NETWORK_REQUESTS = TOTAL_REQUESTS - REUSED_REQUESTS
MAX_RESPONSE_BYTES = 1_048_576
MAX_NETWORK_BYTES = NETWORK_REQUESTS * MAX_RESPONSE_BYTES
MAX_TOTAL_BYTES = TOTAL_REQUESTS * MAX_RESPONSE_BYTES
MAX_DURATION_SECONDS = 1_800
REQUEST_TIMEOUT_SECONDS = 30
WORKERS = 2
REUSE_VERIFICATION_WORKERS = 16
IMPLEMENTATION_PATHS = (
    "configs/data_layout_contract.json",
    "configs/exchange_calendar_policy.json",
    "configs/source_contract.json",
    "pyproject.toml",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/calendar_cli.py",
    "src/futures_rebuild/calendar_notice_union.py",
    "src/futures_rebuild/calendar_notice_union_recovery.py",
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
    "DELETE_EDIT_MOVE_OR_OVERWRITE_PREDECESSOR_EVIDENCE",
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
    "NETWORK_OR_TIMEOUT_FAILURE",
    "PREDECESSOR_PLAN_FAILURE_OR_STAGE_DRIFT",
    "PRIOR_RECOVERY_OUTCOME_EXISTS",
    "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
    "REDIRECT_OR_URL_OUTSIDE_EXACT_CME_ALLOWLIST",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_SAFE_HEADERS = frozenset(
    {"cache-control", "content-type", "date", "etag", "last-modified"}
)
_AUTHORITY_KEYS = {
    "failure_id",
    "failure_report_path",
    "failure_report_sha256",
    "first_remaining_request_id",
    "predecessor_approval_receipt_id",
    "predecessor_plan_id",
    "predecessor_plan_path",
    "predecessor_plan_sha256",
    "remaining_request_count",
    "reused_response_count",
    "reused_response_set_id",
    "reused_total_bytes",
    "source_stage_relative_path",
}


class NoticeUnionRecoveryError(UnauthorizedOperation):
    """Raised before or during bounded CME notice-union recovery."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise NoticeUnionRecoveryError(
            "CME notice-union recovery rejected an HTTP redirect"
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
                "CME notice-union recovery implementation input is missing: "
                f"{relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _validate_predecessor_failure(
    failure: Mapping[str, object],
    *,
    predecessor_plan: Mapping[str, object],
    predecessor_plan_path: Path,
    boundary: RepoBoundary,
) -> tuple[Path, list[dict[str, object]], int]:
    core = dict(failure)
    failure_id = core.pop("failure_id", None)
    expected_keys = {
        "approval_receipt_id",
        "elapsed_milliseconds",
        "failed_requests",
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
    responses = failure.get("responses_preserved")
    failed = failure.get("failed_requests")
    scope = predecessor_plan.get("scope")
    if (
        set(core) != expected_keys
        or type(failure_id) is not str
        or failure_id != sha256_json(core)
        or failure.get("schema_version") != PREDECESSOR_FAILURE_SCHEMA
        or failure.get("status") != "STOPPED"
        or failure.get("plan_id") != predecessor_plan.get("plan_id")
        or failure.get("plan_sha256") != sha256_file(predecessor_plan_path)
        or failure.get("publication_occurred") is not False
        or failure.get("retries_performed") != 0
        or failure.get("network_requests_attempted") != REUSED_REQUESTS
        or failure.get("responses_preserved_count") != REUSED_REQUESTS
        or not isinstance(responses, list)
        or len(responses) != REUSED_REQUESTS
        or not isinstance(failed, list)
        or len(failed) != 1
        or not isinstance(failed[0], dict)
        or failed[0].get("error_class") != "DURATION_CEILING_REACHED"
        or not isinstance(scope, dict)
    ):
        raise IntegrityError(
            "CME notice-union predecessor failure contract is invalid"
        )
    requests = scope.get("requests")
    if not isinstance(requests, list) or len(requests) != TOTAL_REQUESTS:
        raise IntegrityError(
            "CME notice-union predecessor request set is invalid"
        )
    first_remaining = requests[REUSED_REQUESTS]
    if (
        not isinstance(first_remaining, dict)
        or failed[0].get("request_id") != first_remaining.get("request_id")
    ):
        raise IntegrityError(
            "CME notice-union predecessor stop position is invalid"
        )
    stage_relative = failure.get("stage_relative_path")
    if (
        type(stage_relative) is not str
        or not stage_relative.startswith("state/data_publication_staging/")
    ):
        raise IntegrityError(
            "CME notice-union predecessor stage path is invalid"
        )
    stage = boundary.assert_active_path(
        boundary.active_root / stage_relative,
        purpose="CME notice-union predecessor stage",
        subtree="state/data_publication_staging",
    )
    if not stage.is_dir():
        raise IntegrityError(
            "CME notice-union predecessor stage is unavailable"
        )
    files = sorted(path for path in stage.iterdir() if path.is_file())
    if len(files) != REUSED_REQUESTS or any(path.is_symlink() for path in files):
        raise IntegrityError(
            "CME notice-union predecessor stage file set is invalid"
        )
    total = 0
    normalized: list[dict[str, object]] = []
    expected_names: set[str] = set()
    files_to_hash: list[tuple[Path, str]] = []
    for index, response in enumerate(responses):
        request = requests[index]
        if not isinstance(response, dict) or not isinstance(request, dict):
            raise IntegrityError(
                "CME notice-union predecessor response is invalid"
            )
        request_id = request.get("request_id")
        logical = response.get("logical_path")
        if (
            response.get("ordinal") != index + 1
            or response.get("request_id") != request_id
            or response.get("url") != request.get("url")
            or response.get("matched_queries") != request.get("matched_queries")
            or response.get("metadata_title") != request.get("metadata_title")
            or response.get("request_kind")
            != "HISTORICAL_NOTICE_DOCUMENT_UNION"
            or response.get("content_type") != "text/html"
            or response.get("status_code") != 200
            or type(logical) is not str
            or type(request_id) is not str
            or Path(logical).name != f"{request_id}.html"
            or type(response.get("sha256")) is not str
            or _SHA256.fullmatch(str(response["sha256"])) is None
            or type(response.get("size")) is not int
            or int(response["size"]) < 1
            or int(response["size"]) > MAX_RESPONSE_BYTES
        ):
            raise IntegrityError(
                "CME notice-union predecessor response contract is invalid"
            )
        source = stage / Path(logical).name
        if (
            not source.is_file()
            or source.stat().st_size != response["size"]
        ):
            raise IntegrityError(
                "CME notice-union predecessor response bytes changed"
            )
        files_to_hash.append((source, str(response["sha256"])))
        expected_names.add(source.name)
        total += source.stat().st_size
        normalized.append(dict(response))
    if {path.name for path in files} != expected_names:
        raise IntegrityError(
            "CME notice-union predecessor stage has unexpected files"
        )
    with ThreadPoolExecutor(
        max_workers=REUSE_VERIFICATION_WORKERS
    ) as executor:
        hashed = [
            (source, expected, executor.submit(sha256_file, source))
            for source, expected in files_to_hash
        ]
        for _source, expected, future in hashed:
            if future.result() != expected:
                raise IntegrityError(
                    "CME notice-union predecessor response bytes changed"
                )
    return stage, normalized, total


def recovery_authority(
    *,
    predecessor_plan_path: Path,
    failure_report_path: Path,
    boundary: RepoBoundary,
) -> tuple[
    dict[str, object],
    dict[str, object],
    dict[str, object],
    list[dict[str, object]],
]:
    predecessor_plan = validate_union_plan(
        _canonical_object(
            predecessor_plan_path,
            description="CME notice-union predecessor plan",
        )
    )
    if predecessor_plan.get("schema_version") != PREDECESSOR_PLAN_SCHEMA:
        raise IntegrityError(
            "CME notice-union predecessor plan schema is invalid"
        )
    failure = _canonical_object(
        failure_report_path,
        description="CME notice-union predecessor failure report",
    )
    stage, reused, reused_total = _validate_predecessor_failure(
        failure,
        predecessor_plan=predecessor_plan,
        predecessor_plan_path=predecessor_plan_path,
        boundary=boundary,
    )
    scope = predecessor_plan["scope"]
    assert isinstance(scope, dict)
    requests = scope["requests"]
    assert isinstance(requests, list)
    remaining = [
        dict(item)
        for item in requests[REUSED_REQUESTS:]
        if isinstance(item, dict)
    ]
    if len(remaining) != NETWORK_REQUESTS:
        raise IntegrityError(
            "CME notice-union recovery request count is invalid"
        )
    first_remaining = remaining[0]
    authority = {
        "failure_id": failure["failure_id"],
        "failure_report_path": failure_report_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "failure_report_sha256": sha256_file(failure_report_path),
        "first_remaining_request_id": first_remaining["request_id"],
        "predecessor_approval_receipt_id": failure["approval_receipt_id"],
        "predecessor_plan_id": predecessor_plan["plan_id"],
        "predecessor_plan_path": predecessor_plan_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "predecessor_plan_sha256": sha256_file(predecessor_plan_path),
        "remaining_request_count": NETWORK_REQUESTS,
        "reused_response_count": REUSED_REQUESTS,
        "reused_response_set_id": sha256_json(reused),
        "reused_total_bytes": reused_total,
        "source_stage_relative_path": stage.relative_to(
            boundary.active_root
        ).as_posix(),
    }
    return authority, predecessor_plan, failure, remaining


def _validate_authority(authority: Mapping[str, object]) -> None:
    if set(authority) != _AUTHORITY_KEYS:
        raise ContractError(
            "CME notice-union recovery authority schema is invalid"
        )
    for key in (
        "failure_id",
        "failure_report_sha256",
        "predecessor_approval_receipt_id",
        "predecessor_plan_id",
        "predecessor_plan_sha256",
        "reused_response_set_id",
    ):
        value = authority.get(key)
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            raise ContractError(
                "CME notice-union recovery authority hash is invalid"
            )
    if (
        authority.get("remaining_request_count") != NETWORK_REQUESTS
        or authority.get("reused_response_count") != REUSED_REQUESTS
        or type(authority.get("reused_total_bytes")) is not int
        or int(authority["reused_total_bytes"]) < REUSED_REQUESTS
        or int(authority["reused_total_bytes"]) > MAX_TOTAL_BYTES
    ):
        raise ContractError(
            "CME notice-union recovery authority counts are invalid"
        )
    for key in (
        "failure_report_path",
        "first_remaining_request_id",
        "predecessor_plan_path",
        "source_stage_relative_path",
    ):
        if type(authority.get(key)) is not str or not authority[key]:
            raise ContractError(
                "CME notice-union recovery authority path is invalid"
            )


def _validate_remaining_requests(
    requests: Sequence[object],
) -> list[dict[str, object]]:
    if len(requests) != NETWORK_REQUESTS:
        raise ContractError(
            "CME notice-union recovery request count is invalid"
        )
    normalized: list[dict[str, object]] = []
    previous_url = ""
    for index, request in enumerate(requests, start=REUSED_REQUESTS + 1):
        if (
            not isinstance(request, dict)
            or request.get("ordinal") != index
            or type(request.get("url")) is not str
            or str(request["url"]) <= previous_url
            or type(request.get("request_id")) is not str
            or type(request.get("metadata_title")) is not str
            or not isinstance(request.get("matched_queries"), list)
            or not request["matched_queries"]
            or request.get("request_kind")
            != "HISTORICAL_NOTICE_DOCUMENT_UNION"
            or request.get("accept") != "text/html"
        ):
            raise ContractError(
                "CME notice-union recovery request schema is invalid"
            )
        previous_url = str(request["url"])
        normalized.append(dict(request))
    return normalized


def build_recovery_plan(
    *,
    authority: Mapping[str, object],
    remaining_requests: Sequence[object],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    _validate_authority(authority)
    requests = _validate_remaining_requests(remaining_requests)
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
    ):
        raise ContractError(
            "CME notice-union recovery implementation hashes are invalid"
        )
    scope: dict[str, object] = {
        "allow_redirects": False,
        "authority": dict(authority),
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
                "{request_id}.html"
            ),
            "failure_report": (
                "reports/exchange_calendar/"
                "cme_historical_notice_document_union_recovery_failure_"
                "{plan_prefix}.json"
            ),
            "manifest_template": (
                "manifests/data_releases/reference/{release_id}.json"
            ),
            "publication_lock": "state/locks/data-publication.lock",
            "staging_root": "state/data_publication_staging",
        },
        "purpose": (
            "REUSE_1094_HASH_VERIFIED_PREDECESSOR_RESPONSES_AND_CAPTURE_"
            "ONLY_THE_REMAINING_179_NOTICE_HTML_DOCUMENTS"
        ),
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "requests": requests,
        "retries": 0,
        "reused_response_count": REUSED_REQUESTS,
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
            "CME notice-union recovery plan schema is invalid"
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
            "CME notice-union recovery plan identity is invalid"
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
            "CME notice-union recovery plan scope is invalid"
        )
    expected = build_recovery_plan(
        authority=authority,
        remaining_requests=requests,
        implementation_sha256=implementation,
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "CME notice-union recovery plan differs from implementation"
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
        raise NoticeUnionRecoveryError(
            "CME notice-union recovery lacks exact hash-bound approval"
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
        raise NoticeUnionRecoveryError(
            "CME notice-union recovery URL is outside the exact allowlist"
        )


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
                raise NoticeUnionRecoveryError(
                    "CME notice-union recovery response is not exact HTTP 200"
                )
            if response.headers.get_content_type() != "text/html":
                raise NoticeUnionRecoveryError(
                    "CME notice-union recovery content type is unexpected"
                )
            body = response.read(MAX_RESPONSE_BYTES + 1)
            if len(body) > MAX_RESPONSE_BYTES:
                raise NoticeUnionRecoveryError(
                    "CME notice-union recovery response byte ceiling exceeded"
                )
            safe_headers = {
                key.lower(): value
                for key, value in response.headers.items()
                if key.lower() in _SAFE_HEADERS
            }
    except NoticeUnionRecoveryError:
        raise
    except (
        urllib.error.HTTPError,
        urllib.error.URLError,
        TimeoutError,
    ) as exc:
        raise NoticeUnionRecoveryError(
            "CME notice-union recovery request failed"
        ) from exc
    received = datetime.now(timezone.utc).replace(microsecond=0)
    return (
        body,
        dict(sorted(safe_headers.items())),
        received.isoformat().replace("+00:00", "Z"),
    )


def _failure_path(root: Path, plan_id: str) -> Path:
    return (
        root
        / "reports"
        / "exchange_calendar"
        / (
            "cme_historical_notice_document_union_recovery_failure_"
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
    network_responses = [
        dict(response)
        for response in responses
        if response.get("acquisition") == "NETWORK"
    ]
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "elapsed_milliseconds": elapsed_milliseconds,
        "failed_requests": list(failures),
        "network_requests_attempted": attempted,
        "network_responses_preserved": network_responses,
        "network_responses_preserved_count": len(network_responses),
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "publication_occurred": False,
        "retries_performed": 0,
        "reused_response_count": REUSED_REQUESTS,
        "schema_version": FAILURE_SCHEMA,
        "stage_relative_path": stage.relative_to(
            boundary.active_root
        ).as_posix(),
        "status": "STOPPED",
    }
    return {**core, "failure_id": sha256_json(core)}


def capture_recovery_union(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: PhasePublisher,
) -> DataReleaseReceipt:
    plan = validate_recovery_plan(
        _canonical_object(
            plan_path, description="CME notice-union recovery plan"
        )
    )
    approval = _canonical_object(
        approval_path, description="CME notice-union recovery approval"
    )
    approval_id = validate_recovery_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    root = publisher.boundary.active_root
    if scope["implementation_sha256"] != implementation_hashes(root):
        raise NoticeUnionRecoveryError(
            "CME notice-union recovery implementation hashes drifted"
        )
    authority = scope["authority"]
    assert isinstance(authority, dict)
    derived, predecessor_plan, failure, remaining = recovery_authority(
        predecessor_plan_path=root / str(authority["predecessor_plan_path"]),
        failure_report_path=root / str(authority["failure_report_path"]),
        boundary=publisher.boundary,
    )
    if authority != derived or scope["requests"] != remaining:
        raise NoticeUnionRecoveryError(
            "CME notice-union recovery predecessor evidence changed"
        )
    failure_path = _failure_path(root, str(plan["plan_id"]))
    prior_release = _existing_release_for_plan(root, str(plan["plan_id"]))
    if failure_path.exists() or prior_release is not None:
        raise NoticeUnionRecoveryError(
            "CME notice-union recovery approval already has a durable outcome"
        )
    source_stage = root / str(authority["source_stage_relative_path"])
    reused_raw = failure["responses_preserved"]
    assert isinstance(reused_raw, list)
    requests = scope["requests"]
    assert isinstance(requests, list)
    allowed = {str(item["url"]) for item in requests if isinstance(item, dict)}
    if len(allowed) != NETWORK_REQUESTS:
        raise NoticeUnionRecoveryError(
            "CME notice-union recovery allowlist is invalid"
        )
    stage = publisher.create_stage("cme_notice_document_union_recovery")
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
            raise NoticeUnionRecoveryError(
                "CME notice-union recovery copy verification failed"
            )
        logical = str(raw["logical_path"])
        logical_paths[name] = logical
        staged_paths[logical] = name
        total_bytes += target.stat().st_size
        responses.append(
            {**dict(raw), "acquisition": "REUSED_HASH_VERIFIED_STAGE"}
        )
    if (
        len(responses) != REUSED_REQUESTS
        or total_bytes != authority["reused_total_bytes"]
    ):
        raise NoticeUnionRecoveryError(
            "CME notice-union recovery reuse count changed"
        )
    started = monotonic_time.monotonic()
    network_bytes = 0
    attempted = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        for offset in range(0, len(requests), WORKERS):
            elapsed = int((monotonic_time.monotonic() - started) * 1000)
            if elapsed >= MAX_DURATION_SECONDS * 1000:
                failure_payload = _recovery_failure_report(
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
                _write_create_only(failure_path, failure_payload)
                raise NoticeUnionRecoveryError(
                    "CME notice-union recovery duration ceiling reached"
                )
            batch = requests[offset : offset + WORKERS]
            futures = [
                (spec, executor.submit(_fetch, spec, allowed=allowed))
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
                network_bytes += len(body)
                total_bytes += len(body)
                responses.append(
                    {
                        "acquisition": "NETWORK",
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
                failure_payload = _recovery_failure_report(
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
                _write_create_only(failure_path, failure_payload)
                raise NoticeUnionRecoveryError(
                    "CME notice-union recovery stopped on request failure"
                )
    elapsed = int((monotonic_time.monotonic() - started) * 1000)
    responses.sort(key=lambda item: int(item["ordinal"]))
    if (
        elapsed > MAX_DURATION_SECONDS * 1000
        or attempted != NETWORK_REQUESTS
        or len(responses) != TOTAL_REQUESTS
        or network_bytes > MAX_NETWORK_BYTES
        or total_bytes > MAX_TOTAL_BYTES
    ):
        failure_payload = _recovery_failure_report(
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
        _write_create_only(failure_path, failure_payload)
        raise NoticeUnionRecoveryError(
            "CME notice-union recovery final completion bound failed"
        )
    predecessor_scope = predecessor_plan["scope"]
    assert isinstance(predecessor_scope, dict)
    predecessor_authority = predecessor_scope["authority"]
    assert isinstance(predecessor_authority, dict)
    core: dict[str, object] = {
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
        "network_bytes": network_bytes,
        "network_request_count": NETWORK_REQUESTS,
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "responses": responses,
        "reused_response_count": REUSED_REQUESTS,
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
                    str(predecessor_authority["pagination_release_id"]),
                    str(predecessor_authority["probe_release_id"]),
                }
            )
        ),
        embedded_documents={"capture_receipt.json": capture},
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture["capture_id"],
            "predecessor_failure_id": authority["failure_id"],
            "predecessor_plan_id": authority["predecessor_plan_id"],
            "plan_id": plan["plan_id"],
        },
    )
    manifest_path = publisher.publish(
        stage, manifest, staged_paths=staged_paths
    )
    receipt = DataReleaseReceipt.from_manifest(
        manifest_path, publisher.boundary
    )
    load_recovery_union_capture(receipt, boundary=publisher.boundary)
    return receipt


def load_recovery_union_capture(
    receipt: DataReleaseReceipt,
    *,
    boundary: RepoBoundary,
    verify_payload_files: bool = True,
) -> dict[str, object]:
    DataReleaseReceipt.from_dict(receipt.as_dict())
    if receipt.repository_id != boundary.repository_id:
        raise IntegrityError(
            "CME notice-union recovery receipt belongs to another repository"
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
        or len(manifest.files) != TOTAL_REQUESTS
        or set(manifest.embedded_documents) != {"capture_receipt.json"}
    ):
        raise IntegrityError(
            "CME notice-union recovery release is invalid"
        )
    if verify_payload_files:
        files_to_verify = [
            (
                entry,
                boundary.active_root
                / manifest.physical_relative_path(entry),
            )
            for entry in manifest.files
        ]
        with ThreadPoolExecutor(
            max_workers=REUSE_VERIFICATION_WORKERS
        ) as executor:
            verified = [
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
                for entry, physical in files_to_verify
            ]
            for entry, future in verified:
                size, digest = future.result()
                if size != entry.size or digest != entry.sha256:
                    raise IntegrityError(
                        "CME notice-union recovery manifested bytes changed"
                    )
    raw = manifest.embedded_documents["capture_receipt.json"]
    if not isinstance(raw, dict):
        raise IntegrityError(
            "CME notice-union recovery receipt is invalid"
        )
    core = dict(raw)
    capture_id = core.pop("capture_id", None)
    responses = raw.get("responses")
    if (
        type(capture_id) is not str
        or capture_id != sha256_json(core)
        or raw.get("schema_version") != CAPTURE_SCHEMA
        or raw.get("operation") != OPERATION
        or raw.get("network_request_count") != NETWORK_REQUESTS
        or raw.get("reused_response_count") != REUSED_REQUESTS
        or not isinstance(responses, list)
        or len(responses) != TOTAL_REQUESTS
        or manifest.metadata.get("capture_id") != capture_id
    ):
        raise IntegrityError(
            "CME notice-union recovery contract is invalid"
        )
    total = 0
    network_total = 0
    urls: set[str] = set()
    entries = {entry.logical_path: entry for entry in manifest.files}
    for index, response in enumerate(responses, start=1):
        expected_acquisition = (
            "REUSED_HASH_VERIFIED_STAGE"
            if index <= REUSED_REQUESTS
            else "NETWORK"
        )
        if (
            not isinstance(response, dict)
            or response.get("ordinal") != index
            or response.get("acquisition") != expected_acquisition
            or response.get("content_type") != "text/html"
            or response.get("status_code") != 200
            or type(response.get("logical_path")) is not str
            or type(response.get("url")) is not str
            or response["url"] in urls
        ):
            raise IntegrityError(
                "CME notice-union recovery response contract is invalid"
            )
        urls.add(str(response["url"]))
        entry = entries.get(str(response["logical_path"]))
        if (
            entry is None
            or entry.size != response.get("size")
            or entry.sha256 != response.get("sha256")
        ):
            raise IntegrityError(
                "CME notice-union recovery response bytes changed"
            )
        physical = boundary.active_root / manifest.physical_relative_path(
            entry
        )
        if not physical.is_file():
            raise IntegrityError(
                "CME notice-union recovery response file is absent"
            )
        total += entry.size
        if response["acquisition"] == "NETWORK":
            network_total += entry.size
    if (
        total != raw.get("total_bytes")
        or network_total != raw.get("network_bytes")
    ):
        raise IntegrityError(
            "CME notice-union recovery byte totals are invalid"
        )
    return dict(raw)
