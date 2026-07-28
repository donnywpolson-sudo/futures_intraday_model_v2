"""Single-request raw CDX diagnostic after a fail-closed metadata lookup."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import RepoBoundary
from .calendar_archived_cme_gap_discovery import (
    OPERATION as PREDECESSOR_OPERATION,
    _NoRedirect,
    _validate_cdx_url,
    validate_approval as validate_predecessor_approval,
    validate_plan as validate_predecessor_plan,
)
from .canonical import (
    canonical_bytes,
    fsync_directory,
    sha256_bytes,
    sha256_file,
    sha256_json,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .source_contract import legacy_roots_from_contract


PLAN_SCHEMA = "archived_cme_holiday_gap_raw_diagnostic_plan/1.0.0"
APPROVAL_SCHEMA = "archived_cme_holiday_gap_raw_diagnostic_approval/1.0.0"
RESULT_SCHEMA = "archived_cme_holiday_gap_raw_diagnostic/1.0.0"
FAILURE_SCHEMA = "archived_cme_holiday_gap_index_failure_assessment/1.0.0"
OPERATION = "CAPTURE_SINGLE_PUBLIC_ARCHIVED_CME_CDX_RAW_DIAGNOSTIC"
MAX_REQUESTS = 1
WORKERS = 1
MAX_DURATION_SECONDS = 120
REQUEST_TIMEOUT_SECONDS = 60
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
IMPLEMENTATION_PATHS = tuple(
    sorted(
        (
            "configs/source_contract.json",
            "src/futures_rebuild/boundary.py",
            "src/futures_rebuild/calendar_archived_cme_gap_diagnostic.py",
            "src/futures_rebuild/calendar_archived_cme_gap_discovery.py",
            "src/futures_rebuild/canonical.py",
            "src/futures_rebuild/source_contract.py",
        )
    )
)
FORBIDDEN_ACTIONS = (
    "ACCEPT_OR_ACTIVATE_CALENDAR",
    "CALL_CME_DATABENTO_OR_ANY_UNLISTED_ENDPOINT",
    "DOWNLOAD_ARCHIVED_DOCUMENT_PAYLOAD",
    "FOLLOW_CDX_RESULT_OR_REDIRECT",
    "PARSE_OR_NORMALIZE_CDX_ROWS",
    "RETRY_REQUEST",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "DURATION_OR_BYTE_BOUND_REACHED",
    "IMPLEMENTATION_HASH_DRIFT",
    "NETWORK_TIMEOUT_OR_NON_200_RESPONSE",
    "OUTPUT_ALREADY_EXISTS",
    "REDIRECT_OR_UNLISTED_URL",
    "SOURCE_OR_FAILURE_ASSESSMENT_DRIFT",
    "UNDECLARED_OUTPUT",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ArchivedCmeGapDiagnosticError(UnauthorizedOperation):
    """Raised before or during the single raw CDX diagnostic."""


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
    result: dict[str, str] = {}
    for relative in IMPLEMENTATION_PATHS:
        path = repository_root / relative
        if not path.is_file():
            raise IntegrityError(
                "archive-gap diagnostic input is missing: "
                f"{relative}"
            )
        result[relative] = sha256_file(path)
    return result


def diagnostic_authority(
    *,
    predecessor_plan_path: Path,
    predecessor_approval_path: Path,
    failure_assessment_path: Path,
    boundary: RepoBoundary,
) -> tuple[dict[str, object], dict[str, object]]:
    for path, purpose, subtree in (
        (
            predecessor_plan_path,
            "consumed archive-gap predecessor plan",
            "reports/exchange_calendar",
        ),
        (
            predecessor_approval_path,
            "consumed archive-gap predecessor approval",
            "configs",
        ),
        (
            failure_assessment_path,
            "archive-gap predecessor failure assessment",
            "reports/exchange_calendar",
        ),
    ):
        boundary.assert_active_path(path, purpose=purpose, subtree=subtree)
    predecessor = validate_predecessor_plan(
        _canonical_object(
            predecessor_plan_path,
            description="archive-gap predecessor plan",
        )
    )
    predecessor_approval = validate_predecessor_approval(
        approval=_canonical_object(
            predecessor_approval_path,
            description="archive-gap predecessor approval",
        ),
        plan=predecessor,
        plan_sha256=sha256_file(predecessor_plan_path),
    )
    failure = _canonical_object(
        failure_assessment_path,
        description="archive-gap predecessor failure assessment",
    )
    if (
        failure.get("schema_version") != FAILURE_SCHEMA
        or failure.get("status") != "STOPPED_FAIL_CLOSED"
        or failure.get("error_class")
        != "CDX_RESPONSE_ROW_SCHEMA_MISMATCH"
        or failure.get("plan_id") != predecessor["plan_id"]
        or failure.get("plan_sha256") != sha256_file(predecessor_plan_path)
        or failure.get("approval_receipt_id")
        != predecessor_approval["approval_receipt_id"]
        or failure.get("network_request_submission_count") != 59
        or failure.get("output_absent") is not True
    ):
        raise IntegrityError(
            "archive-gap predecessor failure assessment is invalid"
        )
    requests = predecessor["scope"].get("requests")
    if not isinstance(requests, list) or len(requests) != 59:
        raise IntegrityError("archive-gap predecessor requests are invalid")
    request = dict(requests[0])
    authority = {
        "failure_assessment_path": failure_assessment_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "failure_assessment_sha256": sha256_file(failure_assessment_path),
        "predecessor_approval_receipt_id": predecessor_approval[
            "approval_receipt_id"
        ],
        "predecessor_plan_id": predecessor["plan_id"],
        "predecessor_plan_path": predecessor_plan_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "predecessor_plan_sha256": sha256_file(predecessor_plan_path),
    }
    return authority, request


def build_plan(
    *,
    authority: Mapping[str, object],
    request: Mapping[str, object],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
        or type(authority.get("failure_assessment_sha256")) is not str
        or _SHA256.fullmatch(str(authority["failure_assessment_sha256"]))
        is None
    ):
        raise ContractError("archive-gap diagnostic authority is invalid")
    original_url = str(request.get("original_cme_url"))
    cdx_url = str(request.get("cdx_url"))
    _validate_cdx_url(cdx_url, original_url=original_url)
    diagnostic_request = {
        "accept": "application/json, text/plain",
        "cdx_url": cdx_url,
        "expected_content_types": [
            "application/json",
            "text/json",
            "text/plain",
        ],
        "original_cme_url": original_url,
        "predecessor_request_id": request.get("request_id"),
        "request_id": "archived-cme-gap-raw-diagnostic-0001",
    }
    scope: dict[str, object] = {
        "allow_redirects": False,
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "max_duration_seconds": MAX_DURATION_SECONDS,
        "max_requests": MAX_REQUESTS,
        "max_response_bytes": MAX_RESPONSE_BYTES,
        "output_path": (
            "reports/exchange_calendar/"
            "archived_cme_holiday_gap_raw_diagnostic_{plan_prefix}.json"
        ),
        "purpose": (
            "PRESERVE_ONE_RAW_CDX_METADATA_RESPONSE_BEFORE_"
            "ANY_SCHEMA_ADAPTATION"
        ),
        "request": diagnostic_request,
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "retries": 0,
        "source_authority": dict(authority),
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


def validate_plan(payload: Mapping[str, object]) -> dict[str, object]:
    core = dict(payload)
    plan_id = core.pop("plan_id", None)
    scope = payload.get("scope")
    if (
        type(plan_id) is not str
        or _SHA256.fullmatch(plan_id) is None
        or plan_id != sha256_json(core)
        or payload.get("schema_version") != PLAN_SCHEMA
        or payload.get("operation") != OPERATION
        or payload.get("classification")
        != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload.get("execution_authorized") is not False
        or not isinstance(scope, dict)
        or scope.get("max_requests") != 1
        or scope.get("workers") != 1
        or scope.get("retries") != 0
        or scope.get("allow_redirects") is not False
        or scope.get("forbidden_actions") != list(FORBIDDEN_ACTIONS)
        or scope.get("stop_conditions") != list(STOP_CONDITIONS)
        or tuple(scope.get("implementation_sha256", {}))
        != IMPLEMENTATION_PATHS
        or not isinstance(scope.get("request"), dict)
    ):
        raise IntegrityError("archive-gap diagnostic plan is invalid")
    _validate_cdx_url(
        str(scope["request"].get("cdx_url")),
        original_url=str(scope["request"].get("original_cme_url")),
    )
    return dict(payload)


def validate_approval(
    *,
    approval: Mapping[str, object],
    plan: Mapping[str, object],
    plan_sha256: str,
) -> dict[str, object]:
    if (
        approval.get("schema_version") != APPROVAL_SCHEMA
        or approval.get("status") != "APPROVED"
        or approval.get("operation") != OPERATION
        or approval.get("plan_id") != plan.get("plan_id")
        or approval.get("plan_sha256") != plan_sha256
        or type(approval.get("approval_receipt_id")) is not str
        or _SHA256.fullmatch(str(approval["approval_receipt_id"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"])) is None
    ):
        raise ArchivedCmeGapDiagnosticError(
            "archive-gap diagnostic approval is missing or mismatched"
        )
    return dict(approval)


def execute(
    *,
    plan_path: Path,
    approval_path: Path,
    output_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    for path, purpose, subtree in (
        (
            plan_path,
            "archive-gap raw diagnostic plan",
            "reports/exchange_calendar",
        ),
        (approval_path, "archive-gap raw diagnostic approval", "configs"),
        (
            output_path,
            "archive-gap raw diagnostic result",
            "reports/exchange_calendar",
        ),
    ):
        boundary.assert_active_path(path, purpose=purpose, subtree=subtree)
    plan = validate_plan(
        _canonical_object(
            plan_path,
            description="archive-gap raw diagnostic plan",
        )
    )
    approval = validate_approval(
        approval=_canonical_object(
            approval_path,
            description="archive-gap raw diagnostic approval",
        ),
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    scope = plan["scope"]
    expected_output = str(scope["output_path"]).format(
        plan_prefix=str(plan["plan_id"])[:8]
    )
    if output_path.relative_to(boundary.active_root).as_posix() != expected_output:
        raise ArchivedCmeGapDiagnosticError(
            "archive-gap diagnostic output path drifted"
        )
    if output_path.exists():
        raise ArchivedCmeGapDiagnosticError(
            "archive-gap diagnostic output already exists"
        )
    if implementation_hashes(boundary.active_root) != scope[
        "implementation_sha256"
    ]:
        raise ArchivedCmeGapDiagnosticError(
            "archive-gap diagnostic implementation hashes drifted"
        )
    request = scope["request"]
    original_url = str(request["original_cme_url"])
    url = str(request["cdx_url"])
    _validate_cdx_url(url, original_url=original_url)
    opener = urllib.request.build_opener(_NoRedirect())
    req = urllib.request.Request(
        url,
        headers={
            "Accept": str(request["accept"]),
            "User-Agent": "futures-intraday-model-v2-calendar-audit/1.0",
        },
        method="GET",
    )
    try:
        with opener.open(req, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            status = int(response.status)
            content_type = response.headers.get_content_type().lower()
            body = response.read(MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        raise ArchivedCmeGapDiagnosticError(
            f"archive-gap diagnostic HTTP failure: {exc.code}"
        ) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise ArchivedCmeGapDiagnosticError(
            "archive-gap diagnostic network request failed"
        ) from exc
    if (
        status != 200
        or content_type not in request["expected_content_types"]
        or len(body) > MAX_RESPONSE_BYTES
    ):
        raise ArchivedCmeGapDiagnosticError(
            "archive-gap diagnostic response failed its bound"
        )
    core: dict[str, object] = {
        "approval_receipt_id": approval["approval_receipt_id"],
        "body_base64": base64.b64encode(body).decode("ascii"),
        "body_sha256": sha256_bytes(body),
        "classification": (
            "RAW_CDX_METADATA_DIAGNOSTIC_NOT_CALENDAR_AUTHORITY"
        ),
        "content_type": content_type,
        "network_request_count": 1,
        "original_cme_url": original_url,
        "plan_id": plan["plan_id"],
        "schema_version": RESULT_SCHEMA,
        "size": len(body),
        "status": "RAW_DIAGNOSTIC_COMPLETE",
        "status_code": status,
    }
    result = {**core, "result_id": sha256_json(core)}
    _write_create_only(output_path, result)
    return result


def _boundary(repository_root: Path, source_contract_path: Path) -> RepoBoundary:
    payload = json.loads(source_contract_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ContractError("source contract must be an object")
    boundary = RepoBoundary(
        Path(str(payload["active_repository"])),
        legacy_roots=legacy_roots_from_contract(payload),
        foreign_roots=(
            Path.home() / "Desktop" / "US_stocks_swing_model",
            Path.home() / "Desktop" / "US_stocks_swing_model_v2",
        ),
    )
    boundary.assert_active_root(repository_root)
    return boundary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--source-contract",
        type=Path,
        default=Path("configs/source_contract.json"),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    plan_command = commands.add_parser("plan")
    plan_command.add_argument("--predecessor-plan", type=Path, required=True)
    plan_command.add_argument(
        "--predecessor-approval", type=Path, required=True
    )
    plan_command.add_argument(
        "--failure-assessment", type=Path, required=True
    )
    plan_command.add_argument("--output", type=Path, required=True)
    execute_command = commands.add_parser("execute")
    execute_command.add_argument("--plan", type=Path, required=True)
    execute_command.add_argument("--approval", type=Path, required=True)
    execute_command.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    root = args.repository_root.resolve()
    contract = (
        args.source_contract
        if args.source_contract.is_absolute()
        else root / args.source_contract
    )
    boundary = _boundary(root, contract)

    def rooted(path: Path) -> Path:
        return path if path.is_absolute() else root / path

    if args.command == "plan":
        authority, request = diagnostic_authority(
            predecessor_plan_path=rooted(args.predecessor_plan),
            predecessor_approval_path=rooted(args.predecessor_approval),
            failure_assessment_path=rooted(args.failure_assessment),
            boundary=boundary,
        )
        payload = build_plan(
            authority=authority,
            request=request,
            implementation_sha256=implementation_hashes(root),
        )
        _write_create_only(rooted(args.output), payload)
    else:
        execute(
            plan_path=rooted(args.plan),
            approval_path=rooted(args.approval),
            output_path=rooted(args.output),
            boundary=boundary,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
