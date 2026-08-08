"""Bounded metadata-only discovery for missing archived CME holiday files."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import RepoBoundary
from .calendar_historical_globex_evidence import RESULT_SCHEMA as EVIDENCE_SCHEMA
from .calendar_holiday_schedule_capture import (
    CAPTURE_SCHEMA as HOLIDAY_CAPTURE_SCHEMA,
    RELEASE_KIND as HOLIDAY_RELEASE_KIND,
)
from .canonical import (
    canonical_bytes,
    fsync_directory,
    sha256_bytes,
    sha256_file,
    sha256_json,
)
from .data_layout import verify_data_release_manifest
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .source_contract import legacy_roots_from_contract


PLAN_SCHEMA = "archived_cme_holiday_gap_index_discovery_plan/1.0.0"
APPROVAL_SCHEMA = "archived_cme_holiday_gap_index_discovery_approval/1.0.0"
RESULT_SCHEMA = "archived_cme_holiday_gap_index_discovery/1.0.0"
OPERATION = "CAPTURE_BOUNDED_PUBLIC_ARCHIVED_CME_HOLIDAY_GAP_INDEX"
MAX_REQUESTS = 59
WORKERS = 2
MAX_DURATION_SECONDS = 900
REQUEST_TIMEOUT_SECONDS = 30
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_TOTAL_BYTES = 64 * 1024 * 1024
IMPLEMENTATION_PATHS = tuple(
    sorted(
        (
            "configs/source_contract.json",
            "src/futures_rebuild/boundary.py",
            "src/futures_rebuild/calendar_archived_cme_gap_discovery.py",
            "src/futures_rebuild/calendar_historical_globex_evidence.py",
            "src/futures_rebuild/calendar_holiday_schedule_capture.py",
            "src/futures_rebuild/canonical.py",
            "src/futures_rebuild/data_layout.py",
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
    "RETRY_REQUEST",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "CONTENT_TYPE_OR_SCHEMA_MISMATCH",
    "DURATION_OR_BYTE_BOUND_REACHED",
    "IMPLEMENTATION_HASH_DRIFT",
    "NETWORK_TIMEOUT_OR_NON_200_RESPONSE",
    "OUTPUT_ALREADY_EXISTS",
    "REDIRECT_OR_UNLISTED_URL",
    "SOURCE_CAPTURE_OR_GAP_SET_DRIFT",
    "UNDECLARED_OUTPUT",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CDX_FIELDS = (
    "timestamp",
    "original",
    "digest",
    "statuscode",
    "mimetype",
    "length",
)


class ArchivedCmeGapDiscoveryError(UnauthorizedOperation):
    """Raised before or during this exact archive-index authority class."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ArchivedCmeGapDiscoveryError(
            "archive-index redirects are forbidden"
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
                "archive-gap implementation input is missing: "
                f"{relative}"
            )
        result[relative] = sha256_file(path)
    return result


def _cdx_url(original_url: str) -> str:
    query = urllib.parse.urlencode(
        (
            ("url", original_url),
            ("output", "json"),
            ("fl", ",".join(_CDX_FIELDS)),
            ("filter", "statuscode:200"),
            ("collapse", "digest"),
        )
    )
    return f"https://web.archive.org/cdx/search/cdx?{query}"


def _validate_cdx_url(url: str, *, original_url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "web.archive.org"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.path != "/cdx/search/cdx"
        or parsed.fragment
        or query
        != [
            ("url", original_url),
            ("output", "json"),
            ("fl", ",".join(_CDX_FIELDS)),
            ("filter", "statuscode:200"),
            ("collapse", "digest"),
        ]
    ):
        raise ContractError("archive-gap CDX URL is invalid")


def source_authority(
    *,
    holiday_manifest_path: Path,
    evidence_result_path: Path,
    boundary: RepoBoundary,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    boundary.assert_active_path(
        holiday_manifest_path,
        purpose="accepted CME holiday-file capture manifest",
        subtree="manifests/data_releases/reference",
    )
    manifest = verify_data_release_manifest(
        holiday_manifest_path,
        boundary,
        verify_files=True,
    )
    capture = manifest.embedded_documents.get("capture_receipt.json")
    if (
        manifest.release_kind != HOLIDAY_RELEASE_KIND
        or manifest.schema_version != HOLIDAY_CAPTURE_SCHEMA
        or not isinstance(capture, dict)
        or capture.get("capture_id") != manifest.metadata.get("capture_id")
        or capture.get("response_count") != 5
        or capture.get("exclusion_count") != MAX_REQUESTS
        or capture.get("unresolved_candidate_count") != 0
        or not isinstance(capture.get("exclusions"), list)
        or len(capture["exclusions"]) != MAX_REQUESTS
    ):
        raise IntegrityError("CME holiday gap source release is invalid")
    gaps: list[dict[str, object]] = []
    for item in capture["exclusions"]:
        if (
            not isinstance(item, dict)
            or item.get("exclusion_code") != "HTTP_404_NOT_FOUND"
            or item.get("http_status") != 404
            or type(item.get("ordinal")) is not int
            or type(item.get("request_id")) is not str
            or type(item.get("url")) is not str
        ):
            raise IntegrityError("CME holiday gap descriptor is invalid")
        gaps.append(
            {
                "cme_request_id": item["request_id"],
                "original_url": item["url"],
                "source_ordinal": item["ordinal"],
            }
        )
    gaps.sort(key=lambda item: str(item["original_url"]))
    if len({item["original_url"] for item in gaps}) != MAX_REQUESTS:
        raise IntegrityError("CME holiday gap URLs are not unique")

    boundary.assert_active_path(
        evidence_result_path,
        purpose="completed historical Globex candidate-evidence result",
        subtree="reports/exchange_calendar",
    )
    evidence = _canonical_object(
        evidence_result_path,
        description="historical Globex candidate-evidence result",
    )
    core = dict(evidence)
    result_id = core.pop("result_id", None)
    if (
        evidence.get("schema_version") != EVIDENCE_SCHEMA
        or evidence.get("status") != "OFFLINE_EXTRACTION_COMPLETE"
        or evidence.get("classification")
        != "CANDIDATE_EVIDENCE_ONLY_NOT_CALENDAR_AUTHORITY"
        or evidence.get("accepted_calendar_interval_count") != 0
        or evidence.get("network_request_count") != 0
        or result_id != sha256_json(core)
    ):
        raise IntegrityError(
            "historical Globex candidate-evidence result is invalid"
        )
    authority = {
        "cme_capture_id": capture["capture_id"],
        "cme_gap_set_id": sha256_json(gaps),
        "cme_manifest_path": holiday_manifest_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "cme_manifest_sha256": sha256_file(holiday_manifest_path),
        "cme_release_id": manifest.release_id,
        "evidence_result_id": result_id,
        "evidence_result_path": evidence_result_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "evidence_result_sha256": sha256_file(evidence_result_path),
    }
    return authority, gaps


def _requests(gaps: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    if len(gaps) != MAX_REQUESTS:
        raise ContractError("archive-gap request count is invalid")
    requests: list[dict[str, object]] = []
    for ordinal, gap in enumerate(gaps, start=1):
        original_url = str(gap.get("original_url"))
        cme_parsed = urllib.parse.urlparse(original_url)
        if (
            cme_parsed.scheme != "https"
            or cme_parsed.hostname != "www.cmegroup.com"
            or not cme_parsed.path.startswith(
                "/tools-information/holiday-calendar/files/"
            )
            or cme_parsed.query
            or cme_parsed.fragment
        ):
            raise ContractError("archive-gap original CME URL is invalid")
        cdx_url = _cdx_url(original_url)
        _validate_cdx_url(cdx_url, original_url=original_url)
        digest = hashlib.sha256(original_url.encode("utf-8")).hexdigest()[:12]
        requests.append(
            {
                "accept": "application/json, text/plain",
                "cdx_url": cdx_url,
                "cme_request_id": gap["cme_request_id"],
                "expected_content_types": [
                    "application/json",
                    "text/json",
                    "text/plain",
                ],
                "ordinal": ordinal,
                "original_cme_url": original_url,
                "request_id": f"archived-cme-gap-{ordinal:04d}-{digest}",
                "source_ordinal": gap["source_ordinal"],
            }
        )
    return requests


def build_plan(
    *,
    authority: Mapping[str, object],
    gaps: Sequence[Mapping[str, object]],
    implementation_sha256: Mapping[str, str],
) -> dict[str, object]:
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
        or type(authority.get("cme_gap_set_id")) is not str
        or _SHA256.fullmatch(str(authority["cme_gap_set_id"])) is None
    ):
        raise ContractError("archive-gap plan authority is invalid")
    requests = _requests(gaps)
    scope: dict[str, object] = {
        "allow_redirects": False,
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "max_duration_seconds": MAX_DURATION_SECONDS,
        "max_requests": MAX_REQUESTS,
        "max_response_bytes": MAX_RESPONSE_BYTES,
        "max_total_bytes": MAX_TOTAL_BYTES,
        "output_path": (
            "reports/exchange_calendar/"
            "archived_cme_holiday_gap_index_result_{plan_prefix}.json"
        ),
        "purpose": (
            "DISCOVER_ARCHIVE_INDEX_METADATA_FOR_EXACT_59_MISSING_"
            "OFFICIAL_CME_URLS_WITHOUT_DOWNLOADING_ARCHIVED_BYTES"
        ),
        "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "requests": requests,
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
        or scope.get("max_requests") != MAX_REQUESTS
        or scope.get("workers") != WORKERS
        or scope.get("max_duration_seconds") != MAX_DURATION_SECONDS
        or scope.get("allow_redirects") is not False
        or scope.get("retries") != 0
        or scope.get("forbidden_actions") != list(FORBIDDEN_ACTIONS)
        or scope.get("stop_conditions") != list(STOP_CONDITIONS)
        or tuple(scope.get("implementation_sha256", {}))
        != IMPLEMENTATION_PATHS
        or not isinstance(scope.get("requests"), list)
        or len(scope["requests"]) != MAX_REQUESTS
    ):
        raise IntegrityError("archive-gap discovery plan is invalid")
    for request in scope["requests"]:
        if not isinstance(request, dict):
            raise IntegrityError("archive-gap request is invalid")
        _validate_cdx_url(
            str(request.get("cdx_url")),
            original_url=str(request.get("original_cme_url")),
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
        raise ArchivedCmeGapDiscoveryError(
            "archive-gap discovery approval is missing or mismatched"
        )
    return dict(approval)


def parse_cdx_payload(
    *,
    body: bytes,
    original_url: str,
) -> list[dict[str, object]]:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("archive-gap CDX response is invalid JSON") from exc
    if payload == []:
        return []
    if (
        not isinstance(payload, list)
        or not payload
        or payload[0] != list(_CDX_FIELDS)
    ):
        raise IntegrityError("archive-gap CDX response schema is invalid")
    snapshots: list[dict[str, object]] = []
    for row in payload[1:]:
        if (
            not isinstance(row, list)
            or len(row) != len(_CDX_FIELDS)
            or not _same_cme_original_url(
                observed=str(row[1]),
                requested=original_url,
            )
            or row[3] != "200"
            or not re.fullmatch(r"\d{14}", str(row[0]))
            or not str(row[2])
            or not str(row[5]).isdigit()
        ):
            raise IntegrityError("archive-gap CDX response row is invalid")
        snapshots.append(dict(zip(_CDX_FIELDS, row, strict=True)))
    snapshots.sort(
        key=lambda item: (
            str(item["timestamp"]),
            str(item["digest"]),
        )
    )
    return snapshots


def _same_cme_original_url(*, observed: str, requested: str) -> bool:
    """Allow only the Archive's default-port HTTP form of one exact CME URL."""

    try:
        observed_url = urllib.parse.urlparse(observed)
        requested_url = urllib.parse.urlparse(requested)
        observed_port = observed_url.port
        requested_port = requested_url.port
    except ValueError:
        return False
    if (
        requested_url.scheme != "https"
        or requested_url.hostname != "www.cmegroup.com"
        or requested_url.username is not None
        or requested_url.password is not None
        or requested_port not in {None, 443}
        or not requested_url.path.startswith(
            "/tools-information/holiday-calendar/files/"
        )
        or requested_url.query
        or requested_url.fragment
    ):
        return False
    if (
        observed_url.scheme not in {"http", "https"}
        or observed_url.hostname != "www.cmegroup.com"
        or observed_url.username is not None
        or observed_url.password is not None
        or observed_url.path != requested_url.path
        or observed_url.query
        or observed_url.fragment
    ):
        return False
    allowed_observed_ports = (
        {None, 80} if observed_url.scheme == "http" else {None, 443}
    )
    return observed_port in allowed_observed_ports


def _fetch(request: Mapping[str, object]) -> dict[str, object]:
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
            content_type = (
                response.headers.get_content_type().lower()
            )
            body = response.read(MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        raise ArchivedCmeGapDiscoveryError(
            f"archive-gap HTTP failure: {exc.code}"
        ) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise ArchivedCmeGapDiscoveryError(
            "archive-gap network request failed"
        ) from exc
    if (
        status != 200
        or content_type not in request["expected_content_types"]
        or len(body) > MAX_RESPONSE_BYTES
    ):
        raise ArchivedCmeGapDiscoveryError(
            "archive-gap response failed its bound"
        )
    snapshots = parse_cdx_payload(body=body, original_url=original_url)
    return {
        "body_base64": base64.b64encode(body).decode("ascii"),
        "body_sha256": sha256_bytes(body),
        "content_type": content_type,
        "ordinal": request["ordinal"],
        "original_cme_url": original_url,
        "request_id": request["request_id"],
        "size": len(body),
        "snapshot_count": len(snapshots),
        "snapshots": snapshots,
        "status_code": status,
    }


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
            "archive-gap discovery plan",
            "reports/exchange_calendar",
        ),
        (approval_path, "archive-gap discovery approval", "configs"),
        (
            output_path,
            "archive-gap discovery result",
            "reports/exchange_calendar",
        ),
    ):
        boundary.assert_active_path(path, purpose=purpose, subtree=subtree)
    plan = validate_plan(
        _canonical_object(plan_path, description="archive-gap discovery plan")
    )
    approval = validate_approval(
        approval=_canonical_object(
            approval_path,
            description="archive-gap discovery approval",
        ),
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    scope = plan["scope"]
    expected_output = str(scope["output_path"]).format(
        plan_prefix=str(plan["plan_id"])[:8]
    )
    if output_path.relative_to(boundary.active_root).as_posix() != expected_output:
        raise ArchivedCmeGapDiscoveryError(
            "archive-gap discovery output path drifted"
        )
    if output_path.exists():
        raise ArchivedCmeGapDiscoveryError(
            "archive-gap discovery output already exists"
        )
    if implementation_hashes(boundary.active_root) != scope[
        "implementation_sha256"
    ]:
        raise ArchivedCmeGapDiscoveryError(
            "archive-gap implementation hashes drifted"
        )
    requests = scope["requests"]
    responses: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(_fetch, item): item for item in requests}
        for future in as_completed(futures):
            responses.append(future.result())
    responses.sort(key=lambda item: int(item["ordinal"]))
    total_bytes = sum(int(item["size"]) for item in responses)
    if total_bytes > MAX_TOTAL_BYTES:
        raise ArchivedCmeGapDiscoveryError(
            "archive-gap total byte bound exceeded"
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
        "network_request_count": len(responses),
        "plan_id": plan["plan_id"],
        "response_count": len(responses),
        "responses": responses,
        "schema_version": RESULT_SCHEMA,
        "snapshot_count": len(snapshots),
        "snapshot_set_id": sha256_json(snapshots),
        "source_authority": scope["source_authority"],
        "status": "BOUNDED_METADATA_DISCOVERY_COMPLETE",
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
    plan_command.add_argument("--holiday-manifest", type=Path, required=True)
    plan_command.add_argument("--evidence-result", type=Path, required=True)
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
        authority, gaps = source_authority(
            holiday_manifest_path=rooted(args.holiday_manifest),
            evidence_result_path=rooted(args.evidence_result),
            boundary=boundary,
        )
        payload = build_plan(
            authority=authority,
            gaps=gaps,
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
