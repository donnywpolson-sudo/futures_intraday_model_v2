"""Recover archive-index discovery using one preserved raw CDX response."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import RepoBoundary
from .calendar_archived_cme_gap_diagnostic import (
    validate_approval as validate_diagnostic_approval,
)
from .calendar_archived_cme_gap_diagnostic import (
    validate_plan as validate_diagnostic_plan,
)
from .calendar_archived_cme_gap_discovery import (
    _fetch,
    parse_cdx_payload,
    validate_approval as validate_predecessor_approval,
)
from .calendar_archived_cme_gap_discovery import (
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


PLAN_SCHEMA = (
    "archived_cme_holiday_gap_index_discovery_recovery_plan/1.0.0"
)
APPROVAL_SCHEMA = (
    "archived_cme_holiday_gap_index_discovery_recovery_approval/1.0.0"
)
RESULT_SCHEMA = (
    "archived_cme_holiday_gap_index_discovery_recovery/1.0.0"
)
OPERATION = "RECOVER_BOUNDED_PUBLIC_ARCHIVED_CME_HOLIDAY_GAP_INDEX"
TOTAL_URLS = 59
REUSED_RESPONSES = 1
NETWORK_REQUESTS = 58
WORKERS = 2
MAX_DURATION_SECONDS = 900
MAX_TOTAL_BYTES = 64 * 1024 * 1024
IMPLEMENTATION_PATHS = tuple(
    sorted(
        (
            "configs/source_contract.json",
            "src/futures_rebuild/boundary.py",
            "src/futures_rebuild/calendar_archived_cme_gap_diagnostic.py",
            "src/futures_rebuild/calendar_archived_cme_gap_discovery.py",
            "src/futures_rebuild/calendar_archived_cme_gap_discovery_recovery.py",
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
    "INFER_OR_PUBLISH_CALENDAR_INTERVAL",
    "MATERIALIZE_FOUNDATION",
    "READ_OR_EXPOSE_CREDENTIAL",
    "REQUEST_REUSED_DIAGNOSTIC_URL",
    "RETRY_REQUEST",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "CONTENT_TYPE_OR_SCHEMA_MISMATCH",
    "DIAGNOSTIC_OR_PREDECESSOR_EVIDENCE_DRIFT",
    "DURATION_OR_BYTE_BOUND_REACHED",
    "IMPLEMENTATION_HASH_DRIFT",
    "NETWORK_TIMEOUT_OR_NON_200_RESPONSE",
    "OUTPUT_ALREADY_EXISTS",
    "REDIRECT_OR_UNLISTED_URL",
    "UNDECLARED_OUTPUT",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ArchivedCmeGapRecoveryError(UnauthorizedOperation):
    """Raised before or during this exact recovery authority class."""


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


def _validate_receipt(payload: Mapping[str, object]) -> None:
    core = dict(payload)
    receipt_id = core.pop("approval_receipt_id", None)
    if (
        type(receipt_id) is not str
        or _SHA256.fullmatch(receipt_id) is None
        or receipt_id != sha256_json(core)
    ):
        raise IntegrityError("archive-gap approval receipt self-hash is invalid")


def implementation_hashes(repository_root: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for relative in IMPLEMENTATION_PATHS:
        path = repository_root / relative
        if not path.is_file():
            raise IntegrityError(
                f"archive-gap recovery implementation is missing: {relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def recovery_authority(
    *,
    predecessor_plan_path: Path,
    predecessor_approval_path: Path,
    failure_assessment_path: Path,
    diagnostic_plan_path: Path,
    diagnostic_approval_path: Path,
    diagnostic_result_path: Path,
    boundary: RepoBoundary,
) -> tuple[
    dict[str, object],
    list[dict[str, object]],
    dict[str, object],
]:
    paths = (
        (
            predecessor_plan_path,
            "archive-gap predecessor plan",
            "reports/exchange_calendar",
        ),
        (
            predecessor_approval_path,
            "archive-gap predecessor approval",
            "configs",
        ),
        (
            failure_assessment_path,
            "archive-gap predecessor failure",
            "reports/exchange_calendar",
        ),
        (
            diagnostic_plan_path,
            "archive-gap raw diagnostic plan",
            "reports/exchange_calendar",
        ),
        (
            diagnostic_approval_path,
            "archive-gap raw diagnostic approval",
            "configs",
        ),
        (
            diagnostic_result_path,
            "archive-gap raw diagnostic result",
            "reports/exchange_calendar",
        ),
    )
    for path, purpose, subtree in paths:
        boundary.assert_active_path(path, purpose=purpose, subtree=subtree)

    predecessor_plan = validate_predecessor_plan(
        _canonical_object(
            predecessor_plan_path,
            description="archive-gap predecessor plan",
        )
    )
    predecessor_approval = _canonical_object(
        predecessor_approval_path,
        description="archive-gap predecessor approval",
    )
    validate_predecessor_approval(
        approval=predecessor_approval,
        plan=predecessor_plan,
        plan_sha256=sha256_file(predecessor_plan_path),
    )
    _validate_receipt(predecessor_approval)

    failure = _canonical_object(
        failure_assessment_path,
        description="archive-gap predecessor failure",
    )
    if (
        failure.get("schema_version")
        != "archived_cme_holiday_gap_index_failure_assessment/1.0.0"
        or failure.get("status") != "STOPPED_FAIL_CLOSED"
        or failure.get("error_class") != "CDX_RESPONSE_ROW_SCHEMA_MISMATCH"
        or failure.get("output_absent") is not True
        or failure.get("plan_id") != predecessor_plan["plan_id"]
        or failure.get("plan_sha256") != sha256_file(predecessor_plan_path)
        or failure.get("approval_receipt_id")
        != predecessor_approval["approval_receipt_id"]
    ):
        raise IntegrityError("archive-gap predecessor failure is invalid")

    diagnostic_plan = validate_diagnostic_plan(
        _canonical_object(
            diagnostic_plan_path,
            description="archive-gap raw diagnostic plan",
        )
    )
    diagnostic_approval = _canonical_object(
        diagnostic_approval_path,
        description="archive-gap raw diagnostic approval",
    )
    validate_diagnostic_approval(
        approval=diagnostic_approval,
        plan=diagnostic_plan,
        plan_sha256=sha256_file(diagnostic_plan_path),
    )
    _validate_receipt(diagnostic_approval)

    predecessor_requests = predecessor_plan["scope"]["requests"]
    if (
        not isinstance(predecessor_requests, list)
        or len(predecessor_requests) != TOTAL_URLS
        or any(not isinstance(item, dict) for item in predecessor_requests)
    ):
        raise IntegrityError("archive-gap predecessor request set is invalid")
    first = dict(predecessor_requests[0])
    diagnostic_request = diagnostic_plan["scope"]["request"]
    diagnostic_source = diagnostic_plan["scope"]["source_authority"]
    if (
        not isinstance(diagnostic_request, dict)
        or not isinstance(diagnostic_source, dict)
        or diagnostic_request.get("original_cme_url")
        != first.get("original_cme_url")
        or diagnostic_request.get("cdx_url") != first.get("cdx_url")
        or diagnostic_request.get("predecessor_request_id")
        != first.get("request_id")
        or diagnostic_source.get("predecessor_plan_id")
        != predecessor_plan["plan_id"]
        or diagnostic_source.get("predecessor_plan_sha256")
        != sha256_file(predecessor_plan_path)
        or diagnostic_source.get("predecessor_approval_receipt_id")
        != predecessor_approval["approval_receipt_id"]
        or diagnostic_source.get("failure_assessment_sha256")
        != sha256_file(failure_assessment_path)
    ):
        raise IntegrityError("archive-gap diagnostic source binding is invalid")

    diagnostic = _canonical_object(
        diagnostic_result_path,
        description="archive-gap raw diagnostic result",
    )
    diagnostic_core = dict(diagnostic)
    diagnostic_result_id = diagnostic_core.pop("result_id", None)
    try:
        body = base64.b64decode(
            str(diagnostic.get("body_base64")),
            validate=True,
        )
    except (ValueError, TypeError) as exc:
        raise IntegrityError("archive-gap diagnostic body is invalid") from exc
    if (
        diagnostic.get("schema_version")
        != "archived_cme_holiday_gap_raw_diagnostic/1.0.0"
        or diagnostic.get("status") != "RAW_DIAGNOSTIC_COMPLETE"
        or diagnostic.get("classification")
        != "RAW_CDX_METADATA_DIAGNOSTIC_NOT_CALENDAR_AUTHORITY"
        or diagnostic.get("plan_id") != diagnostic_plan["plan_id"]
        or diagnostic.get("approval_receipt_id")
        != diagnostic_approval["approval_receipt_id"]
        or diagnostic.get("network_request_count") != 1
        or diagnostic.get("status_code") != 200
        or diagnostic.get("original_cme_url")
        != first.get("original_cme_url")
        or diagnostic.get("size") != len(body)
        or diagnostic.get("body_sha256") != sha256_bytes(body)
        or diagnostic_result_id != sha256_json(diagnostic_core)
    ):
        raise IntegrityError("archive-gap diagnostic result is invalid")

    snapshots = parse_cdx_payload(
        body=body,
        original_url=str(first["original_cme_url"]),
    )
    reused_response: dict[str, object] = {
        "acquisition": "REUSED_HASH_VERIFIED_RAW_DIAGNOSTIC",
        "body_base64": diagnostic["body_base64"],
        "body_sha256": diagnostic["body_sha256"],
        "content_type": diagnostic["content_type"],
        "ordinal": first["ordinal"],
        "original_cme_url": first["original_cme_url"],
        "request_id": first["request_id"],
        "size": diagnostic["size"],
        "snapshot_count": len(snapshots),
        "snapshots": snapshots,
        "status_code": diagnostic["status_code"],
    }
    remaining = [
        dict(item)
        for item in predecessor_requests[1:]
        if isinstance(item, dict)
    ]
    if (
        len(remaining) != NETWORK_REQUESTS
        or [int(item["ordinal"]) for item in remaining]
        != list(range(2, TOTAL_URLS + 1))
    ):
        raise IntegrityError("archive-gap remaining request set is invalid")

    authority = {
        "diagnostic_approval_receipt_id": diagnostic_approval[
            "approval_receipt_id"
        ],
        "diagnostic_approval_path": diagnostic_approval_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "diagnostic_approval_sha256": sha256_file(diagnostic_approval_path),
        "diagnostic_plan_id": diagnostic_plan["plan_id"],
        "diagnostic_plan_path": diagnostic_plan_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "diagnostic_plan_sha256": sha256_file(diagnostic_plan_path),
        "diagnostic_result_id": diagnostic_result_id,
        "diagnostic_result_path": diagnostic_result_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "diagnostic_result_sha256": sha256_file(diagnostic_result_path),
        "failure_assessment_path": failure_assessment_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "failure_assessment_sha256": sha256_file(failure_assessment_path),
        "predecessor_approval_receipt_id": predecessor_approval[
            "approval_receipt_id"
        ],
        "predecessor_approval_path": predecessor_approval_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "predecessor_approval_sha256": sha256_file(
            predecessor_approval_path
        ),
        "predecessor_plan_id": predecessor_plan["plan_id"],
        "predecessor_plan_path": predecessor_plan_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "predecessor_plan_sha256": sha256_file(predecessor_plan_path),
        "reused_request_id": first["request_id"],
        "reused_response_sha256": sha256_json(reused_response),
    }
    return authority, remaining, reused_response


def build_recovery_plan(
    *,
    authority: Mapping[str, object],
    remaining_requests: Sequence[Mapping[str, object]],
    reused_response: Mapping[str, object],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    implementation = dict(sorted(implementation_sha256.items()))
    requests = [dict(item) for item in remaining_requests]
    reused = dict(reused_response)
    if (
        tuple(implementation) != IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
        or len(requests) != NETWORK_REQUESTS
        or [item.get("ordinal") for item in requests]
        != list(range(2, TOTAL_URLS + 1))
        or reused.get("ordinal") != 1
        or authority.get("reused_response_sha256") != sha256_json(reused)
    ):
        raise ContractError("archive-gap recovery plan inputs are invalid")
    scope: dict[str, object] = {
        "allow_redirects": False,
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "max_duration_seconds": MAX_DURATION_SECONDS,
        "max_network_requests": NETWORK_REQUESTS,
        "max_total_bytes": MAX_TOTAL_BYTES,
        "output_path": (
            "reports/exchange_calendar/"
            "archived_cme_holiday_gap_index_recovery_result_"
            "{plan_prefix}.json"
        ),
        "purpose": (
            "REUSE_ONE_HASH_VERIFIED_RAW_CDX_RESPONSE_AND_REQUEST_ONLY_"
            "THE_REMAINING_58_EXACT_CME_URLS"
        ),
        "requests": requests,
        "retries": 0,
        "reused_response": reused,
        "reused_response_count": REUSED_RESPONSES,
        "source_authority": dict(authority),
        "stop_conditions": list(STOP_CONDITIONS),
        "total_url_count": TOTAL_URLS,
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
        or scope.get("max_network_requests") != NETWORK_REQUESTS
        or scope.get("reused_response_count") != REUSED_RESPONSES
        or scope.get("total_url_count") != TOTAL_URLS
        or scope.get("workers") != WORKERS
        or scope.get("max_duration_seconds") != MAX_DURATION_SECONDS
        or scope.get("max_total_bytes") != MAX_TOTAL_BYTES
        or scope.get("allow_redirects") is not False
        or scope.get("retries") != 0
        or scope.get("forbidden_actions") != list(FORBIDDEN_ACTIONS)
        or scope.get("stop_conditions") != list(STOP_CONDITIONS)
        or tuple(scope.get("implementation_sha256", {}))
        != IMPLEMENTATION_PATHS
        or not isinstance(scope.get("requests"), list)
        or len(scope["requests"]) != NETWORK_REQUESTS
        or any(not isinstance(item, dict) for item in scope["requests"])
        or [item.get("ordinal") for item in scope["requests"]]
        != list(range(2, TOTAL_URLS + 1))
        or not isinstance(scope.get("reused_response"), dict)
        or scope["reused_response"].get("ordinal") != 1
        or not isinstance(scope.get("source_authority"), dict)
        or scope["source_authority"].get("reused_response_sha256")
        != sha256_json(scope["reused_response"])
    ):
        raise IntegrityError("archive-gap recovery plan is invalid")
    return dict(payload)


def validate_recovery_approval(
    approval: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> dict[str, object]:
    if (
        approval.get("schema_version") != APPROVAL_SCHEMA
        or approval.get("status") != "APPROVED"
        or approval.get("operation") != OPERATION
        or approval.get("plan_id") != plan.get("plan_id")
        or approval.get("plan_sha256") != plan_sha256
    ):
        raise ArchivedCmeGapRecoveryError(
            "archive-gap recovery approval is missing or mismatched"
        )
    _validate_receipt(approval)
    return dict(approval)


def _reconstruct_plan(
    *,
    plan: Mapping[str, object],
    boundary: RepoBoundary,
) -> dict[str, object]:
    scope = plan["scope"]
    authority = scope["source_authority"]
    assert isinstance(authority, dict)
    reconstructed_authority, requests, reused = recovery_authority(
        predecessor_plan_path=boundary.active_root
        / str(authority["predecessor_plan_path"]),
        predecessor_approval_path=boundary.active_root
        / str(authority["predecessor_approval_path"]),
        failure_assessment_path=boundary.active_root
        / str(authority["failure_assessment_path"]),
        diagnostic_plan_path=boundary.active_root
        / str(authority["diagnostic_plan_path"]),
        diagnostic_approval_path=boundary.active_root
        / str(authority["diagnostic_approval_path"]),
        diagnostic_result_path=boundary.active_root
        / str(authority["diagnostic_result_path"]),
        boundary=boundary,
    )
    return build_recovery_plan(
        authority=reconstructed_authority,
        remaining_requests=requests,
        reused_response=reused,
        implementation_sha256=scope["implementation_sha256"],
    )


def execute_recovery(
    *,
    plan_path: Path,
    approval_path: Path,
    output_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    for path, purpose, subtree in (
        (
            plan_path,
            "archive-gap recovery plan",
            "reports/exchange_calendar",
        ),
        (approval_path, "archive-gap recovery approval", "configs"),
        (
            output_path,
            "archive-gap recovery result",
            "reports/exchange_calendar",
        ),
    ):
        boundary.assert_active_path(path, purpose=purpose, subtree=subtree)
    plan = validate_recovery_plan(
        _canonical_object(plan_path, description="archive-gap recovery plan")
    )
    approval = validate_recovery_approval(
        _canonical_object(
            approval_path,
            description="archive-gap recovery approval",
        ),
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    scope = plan["scope"]
    expected_output = str(scope["output_path"]).format(
        plan_prefix=str(plan["plan_id"])[:8]
    )
    if output_path.relative_to(boundary.active_root).as_posix() != expected_output:
        raise ArchivedCmeGapRecoveryError(
            "archive-gap recovery output path drifted"
        )
    if output_path.exists():
        raise ArchivedCmeGapRecoveryError(
            "archive-gap recovery output already exists"
        )
    if _reconstruct_plan(plan=plan, boundary=boundary) != plan:
        raise IntegrityError("archive-gap recovery source evidence drifted")
    if implementation_hashes(boundary.active_root) != scope[
        "implementation_sha256"
    ]:
        raise ArchivedCmeGapRecoveryError(
            "archive-gap recovery implementation hashes drifted"
        )

    started = time.monotonic()
    responses: list[dict[str, object]] = [dict(scope["reused_response"])]
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(_fetch, request): request
            for request in scope["requests"]
        }
        for future in as_completed(futures):
            response = future.result()
            response["acquisition"] = "NETWORK_CAPTURED"
            responses.append(response)
            if time.monotonic() - started > MAX_DURATION_SECONDS:
                raise ArchivedCmeGapRecoveryError(
                    "archive-gap recovery duration bound exceeded"
                )
    responses.sort(key=lambda item: int(item["ordinal"]))
    if (
        len(responses) != TOTAL_URLS
        or [int(item["ordinal"]) for item in responses]
        != list(range(1, TOTAL_URLS + 1))
    ):
        raise ArchivedCmeGapRecoveryError(
            "archive-gap recovery response set is incomplete"
        )
    total_bytes = sum(int(item["size"]) for item in responses)
    if total_bytes > MAX_TOTAL_BYTES:
        raise ArchivedCmeGapRecoveryError(
            "archive-gap recovery byte bound exceeded"
        )
    snapshots = [
        {
            "digest": snapshot["digest"],
            "length": snapshot["length"],
            "mimetype": snapshot["mimetype"],
            "original": snapshot["original"],
            "request_id": response["request_id"],
            "timestamp": snapshot["timestamp"],
        }
        for response in responses
        for snapshot in response["snapshots"]
    ]
    core: dict[str, object] = {
        "approval_receipt_id": approval["approval_receipt_id"],
        "classification": (
            "SECONDARY_ARCHIVE_METADATA_ONLY_NOT_CALENDAR_AUTHORITY"
        ),
        "network_request_count": NETWORK_REQUESTS,
        "plan_id": plan["plan_id"],
        "response_count": len(responses),
        "responses": responses,
        "reused_response_count": REUSED_RESPONSES,
        "schema_version": RESULT_SCHEMA,
        "snapshot_count": len(snapshots),
        "snapshot_set_id": sha256_json(snapshots),
        "source_authority": scope["source_authority"],
        "status": "BOUNDED_METADATA_RECOVERY_COMPLETE",
        "total_bytes": total_bytes,
        "urls_with_snapshots": sum(
            int(item["snapshot_count"]) > 0 for item in responses
        ),
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
        "--predecessor-approval",
        type=Path,
        required=True,
    )
    plan_command.add_argument(
        "--failure-assessment",
        type=Path,
        required=True,
    )
    plan_command.add_argument("--diagnostic-plan", type=Path, required=True)
    plan_command.add_argument(
        "--diagnostic-approval",
        type=Path,
        required=True,
    )
    plan_command.add_argument("--diagnostic-result", type=Path, required=True)
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
        authority, requests, reused = recovery_authority(
            predecessor_plan_path=rooted(args.predecessor_plan),
            predecessor_approval_path=rooted(args.predecessor_approval),
            failure_assessment_path=rooted(args.failure_assessment),
            diagnostic_plan_path=rooted(args.diagnostic_plan),
            diagnostic_approval_path=rooted(args.diagnostic_approval),
            diagnostic_result_path=rooted(args.diagnostic_result),
            boundary=boundary,
        )
        plan = build_recovery_plan(
            authority=authority,
            remaining_requests=requests,
            reused_response=reused,
            implementation_sha256=implementation_hashes(root),
        )
        _write_create_only(rooted(args.output), plan)
    else:
        execute_recovery(
            plan_path=rooted(args.plan),
            approval_path=rooted(args.approval),
            output_path=rooted(args.output),
            boundary=boundary,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
