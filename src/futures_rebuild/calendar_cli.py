"""Plan, capture, parse, diff, activate, and verify CME exchange calendars."""

from __future__ import annotations

import argparse
import json
import os
import re
import time as monotonic_time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, fsync_directory, sha256_file, sha256_json
from .data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .exchange_calendar import (
    ACTIVE_POINTER_SCHEMA,
    CAPTURE_RELEASE_KIND,
    CAPTURE_SCHEMA_VERSION,
    CME_TIMEZONE,
    PARSER_VERSION,
    VerifiedExchangeCalendar,
    active_pointer_payload,
    approved_research_markets,
    diff_exchange_calendars,
    generate_mapping_candidates,
    load_active_calendar_index,
    load_calendar_index,
    load_exchange_calendar_policy,
    load_exchange_calendar_policy,
    publish_calendar_index,
    publish_verified_exchange_calendar,
    verify_calendar_freshness,
)
from .source_contract import legacy_roots_from_contract


CAPTURE_PLAN_SCHEMA = "cme_calendar_capture_plan/1.0.0"
CAPTURE_APPROVAL_SCHEMA = "cme_calendar_capture_approval/1.0.0"
CAPTURE_OPERATION = "CAPTURE_BOUNDED_PUBLIC_CME_TRADING_HOURS"
CAPTURE_GROUP_IDS = (
    "22",
    "58",
    "133",
    "300",
    "316",
    "425",
    "437",
    "5201",
    "8478",
    "10191",
)
LANDING_PAGE_URL = "https://www.cmegroup.com/trading-hours.html"
FILTERS_URL = "https://www.cmegroup.com/services/trading-hours-filters?isProtected"
SCHEDULE_URL = "https://www.cmegroup.com/services/trading-hours-by-product"
_SAFE_HEADERS = frozenset(
    {"cache-control", "content-type", "date", "etag", "last-modified"}
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class CalendarCaptureError(UnauthorizedOperation):
    """Raised before or during a bounded public CME capture."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise CalendarCaptureError("CME capture rejected an HTTP redirect")


def _canonical_object(path: Path, *, description: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is not readable JSON") from exc
    if (
        not isinstance(payload, dict)
        or raw != canonical_bytes(payload) + b"\n"
    ):
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def _publisher(boundary: RepoBoundary, *, scope: Mapping[str, str]) -> AtomicPublisher:
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope=scope,
    )
    return AtomicPublisher(
        boundary=boundary,
        operation_receipt=operation,
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )


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
    boundary.assert_active_path(
        source_contract_path, purpose="source contract", subtree="configs"
    )
    return boundary


def _three_day_windows(start: date, end: date) -> list[tuple[date, date, date, date]]:
    result: list[tuple[date, date, date, date]] = []
    current = start
    while current <= end:
        core_end = min(end, current + timedelta(days=2))
        result.append(
            (
                current,
                core_end,
                current - timedelta(days=1),
                core_end + timedelta(days=1),
            )
        )
        current = core_end + timedelta(days=1)
    return result


def build_capture_plan(
    *,
    mode: str,
    coverage_start: date,
    coverage_end: date,
    product_ids: Sequence[str] = (),
    predecessor_capture_release_id: str | None = None,
) -> dict[str, object]:
    normalized_mode = mode.upper().replace("-", "_")
    if normalized_mode not in {"BOOTSTRAP", "STEADY_STATE"}:
        raise ContractError("calendar capture mode is invalid")
    normalized_products = tuple(sorted(set(product_ids)))
    if normalized_products != tuple(product_ids) or any(
        not item or not item.isdigit() for item in normalized_products
    ):
        raise ContractError("CME product IDs must be unique sorted decimal identifiers")
    if normalized_mode == "BOOTSTRAP" and normalized_products:
        raise ContractError("bootstrap capture cannot pin product IDs")
    if normalized_mode == "STEADY_STATE" and len(normalized_products) != 41:
        raise ContractError("steady-state capture requires exactly 41 CME product IDs")
    if coverage_start > coverage_end:
        raise ContractError("calendar capture coverage range is invalid")
    requests: list[dict[str, object]] = [
        {
            "accept": "text/html",
            "request_id": "landing-page",
            "request_kind": "LANDING_PAGE",
            "url": LANDING_PAGE_URL,
        },
        {
            "accept": "application/json",
            "request_id": "filters",
            "request_kind": "FILTERS",
            "url": FILTERS_URL,
        },
    ]
    page_numbers = (1,)
    identifier = ",".join(
        CAPTURE_GROUP_IDS if normalized_mode == "BOOTSTRAP" else normalized_products
    )
    for window_number, (core_start, core_end, source_start, source_end) in enumerate(
        _three_day_windows(coverage_start, coverage_end), start=1
    ):
        for page_number in page_numbers:
            query = urllib.parse.urlencode(
                {
                    "cleared": "Futures",
                    "fromEventDate": source_start.isoformat(),
                    "id": identifier,
                    "isProtected": "",
                    "pageNumber": page_number,
                    "pageSize": 999,
                    "sortAsc": "true",
                    "toEventDate": source_end.isoformat(),
                }
            ).replace("isProtected=", "isProtected")
            requests.append(
                {
                    "accept": "application/json",
                    "core_end_trade_date": core_end.isoformat(),
                    "core_start_trade_date": core_start.isoformat(),
                    "request_id": f"schedule-{window_number:03d}-p{page_number}",
                    "request_kind": "SCHEDULE",
                    "url": f"{SCHEDULE_URL}?{query}",
                }
            )
    maximum_requests = 96 if normalized_mode == "BOOTSTRAP" else 40
    if len(requests) > maximum_requests:
        raise ContractError("calendar capture plan exceeds its request ceiling")
    scope: dict[str, object] = {
        "allow_redirects": False,
        "coverage_end_trade_date": coverage_end.isoformat(),
        "coverage_start_trade_date": coverage_start.isoformat(),
        "max_duration_seconds": 900,
        "max_requests": maximum_requests,
        "max_total_bytes": 268_435_456,
        "mode": normalized_mode,
        "predecessor_capture_release_id": predecessor_capture_release_id,
        "product_ids": list(normalized_products),
        "requests": requests,
        "retries": 0,
        "workers": 1,
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": CAPTURE_OPERATION,
        "schema_version": CAPTURE_PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_capture_plan(payload: Mapping[str, object]) -> dict[str, object]:
    if set(payload) != {
        "classification",
        "execution_authorized",
        "operation",
        "plan_id",
        "schema_version",
        "scope",
    }:
        raise IntegrityError("CME capture plan schema is invalid")
    core = {key: payload[key] for key in payload if key != "plan_id"}
    scope = payload.get("scope")
    if (
        payload.get("schema_version") != CAPTURE_PLAN_SCHEMA
        or payload.get("classification") != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload.get("execution_authorized") is not False
        or payload.get("operation") != CAPTURE_OPERATION
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
        or set(scope)
        != {
            "allow_redirects",
            "coverage_end_trade_date",
            "coverage_start_trade_date",
            "max_duration_seconds",
            "max_requests",
            "max_total_bytes",
            "mode",
            "predecessor_capture_release_id",
            "product_ids",
            "requests",
            "retries",
            "workers",
        }
    ):
        raise IntegrityError("CME capture plan identity is invalid")
    expected = build_capture_plan(
        mode=str(scope["mode"]),
        coverage_start=date.fromisoformat(str(scope["coverage_start_trade_date"])),
        coverage_end=date.fromisoformat(str(scope["coverage_end_trade_date"])),
        product_ids=tuple(scope["product_ids"]) if isinstance(scope["product_ids"], list) else (),
        predecessor_capture_release_id=(
            str(scope["predecessor_capture_release_id"])
            if scope["predecessor_capture_release_id"] is not None
            else None
        ),
    )
    if dict(payload) != expected:
        raise IntegrityError("CME capture plan differs from the bounded implementation")
    return dict(payload)


def validate_capture_approval(
    approval: Mapping[str, object], *, plan: Mapping[str, object], plan_sha256: str
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
    if set(approval) != {*core_keys, "approval_receipt_id"}:
        raise CalendarCaptureError("CME capture approval schema is invalid")
    core = {key: approval[key] for key in core_keys}
    if (
        approval.get("schema_version") != CAPTURE_APPROVAL_SCHEMA
        or approval.get("status") != "APPROVED"
        or approval.get("operation") != CAPTURE_OPERATION
        or approval.get("plan_id") != plan["plan_id"]
        or approval.get("plan_sha256") != plan_sha256
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"])) is None
        or approval.get("approval_receipt_id") != sha256_json(core)
    ):
        raise CalendarCaptureError(
            "CME capture lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _safe_url(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or parsed.path
        not in {
            "/trading-hours.html",
            "/services/trading-hours-filters",
            "/services/trading-hours-by-product",
        }
    ):
        raise CalendarCaptureError("CME capture URL is outside the exact allowlist")


def capture_cme_calendar(
    *,
    plan_path: Path,
    approval_path: Path,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    plan = validate_capture_plan(
        _canonical_object(plan_path, description="CME capture plan")
    )
    approval = _canonical_object(
        approval_path, description="CME capture approval"
    )
    approval_id = validate_capture_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan["scope"]
    assert isinstance(scope, dict)
    requests = scope["requests"]
    assert isinstance(requests, list)
    if len(requests) > int(scope["max_requests"]):
        raise CalendarCaptureError("CME capture request ceiling is exceeded")
    stage = publisher.create_stage("cme_calendar_capture")
    opener = urllib.request.build_opener(_NoRedirect())
    started = monotonic_time.monotonic()
    retrieved_at = datetime.now(timezone.utc).replace(microsecond=0)
    total_bytes = 0
    responses: list[dict[str, object]] = []
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    for ordinal, request_spec in enumerate(requests, start=1):
        if not isinstance(request_spec, dict):
            raise CalendarCaptureError("CME capture request is invalid")
        if monotonic_time.monotonic() - started > int(
            scope["max_duration_seconds"]
        ):
            raise CalendarCaptureError("CME capture duration ceiling is exceeded")
        url = str(request_spec["url"])
        _safe_url(url)
        request = urllib.request.Request(
            url,
            headers={
                "Accept": str(request_spec["accept"]),
                "User-Agent": "futures-intraday-model-v2-calendar/1.0",
            },
            method="GET",
        )
        try:
            with opener.open(request, timeout=30) as response:
                if response.status != 200 or response.geturl() != url:
                    raise CalendarCaptureError("CME capture response is not exact HTTP 200")
                content_type = response.headers.get_content_type()
                expected_content_type = str(request_spec["accept"])
                if content_type != expected_content_type:
                    raise CalendarCaptureError(
                        "CME capture response content type is unexpected"
                    )
                remaining = int(scope["max_total_bytes"]) - total_bytes
                body = response.read(remaining + 1)
                if len(body) > remaining:
                    raise CalendarCaptureError("CME capture byte ceiling is exceeded")
                safe_headers = {
                    key.lower(): value
                    for key, value in response.headers.items()
                    if key.lower() in _SAFE_HEADERS
                }
        except CalendarCaptureError:
            raise
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            raise CalendarCaptureError(
                f"CME capture request {ordinal} failed before publication"
            ) from exc
        total_bytes += len(body)
        request_id = str(request_spec["request_id"])
        suffix = "html" if content_type == "text/html" else "json"
        staged_name = f"{ordinal:03d}-{request_id}.{suffix}"
        staged = stage / staged_name
        staged.write_bytes(body)
        logical = f"data/reference/exchange_calendars/{staged_name}"
        logical_paths[staged_name] = logical
        staged_paths[logical] = staged_name
        received_at = datetime.now(timezone.utc).replace(microsecond=0)
        responses.append(
            {
                "content_type": content_type,
                "logical_path": logical,
                "received_at_utc": received_at.isoformat().replace("+00:00", "Z"),
                "request_id": request_id,
                "request_kind": request_spec["request_kind"],
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
    if elapsed_milliseconds > int(scope["max_duration_seconds"]) * 1000:
        raise CalendarCaptureError("CME capture duration ceiling is exceeded")
    core: dict[str, object] = {
        "approval_receipt_id": approval_id,
        "bounds": {
            "allow_redirects": scope["allow_redirects"],
            "max_duration_seconds": scope["max_duration_seconds"],
            "max_requests": scope["max_requests"],
            "max_total_bytes": scope["max_total_bytes"],
            "retries": scope["retries"],
            "workers": scope["workers"],
        },
        "coverage_end_trade_date": scope["coverage_end_trade_date"],
        "coverage_start_trade_date": scope["coverage_start_trade_date"],
        "elapsed_milliseconds": elapsed_milliseconds,
        "mode": scope["mode"],
        "parser_version": PARSER_VERSION,
        "plan_id": plan["plan_id"],
        "predecessor_capture_release_id": scope[
            "predecessor_capture_release_id"
        ],
        "request_count": len(responses),
        "responses": responses,
        "retrieved_at_utc": retrieved_at.isoformat().replace("+00:00", "Z"),
        "schema_version": CAPTURE_SCHEMA_VERSION,
        "total_bytes": total_bytes,
    }
    capture_receipt = {**core, "capture_id": sha256_json(core)}
    predecessor = scope["predecessor_capture_release_id"]
    release = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=CAPTURE_RELEASE_KIND,
        schema_version=CAPTURE_SCHEMA_VERSION,
        logical_paths=logical_paths,
        source_release_ids=(str(predecessor),) if predecessor is not None else (),
        embedded_documents={"capture_receipt.json": capture_receipt},
        metadata={
            "approval_receipt_id": approval_id,
            "capture_id": capture_receipt["capture_id"],
            "coverage_end_trade_date": scope["coverage_end_trade_date"],
            "coverage_start_trade_date": scope["coverage_start_trade_date"],
            "parser_version": PARSER_VERSION,
            "plan_id": plan["plan_id"],
            "retrieved_at_utc": core["retrieved_at_utc"],
        },
    )
    manifest_path = publisher.publish(stage, release, staged_paths=staged_paths)
    receipt = VerifiedReleaseReceipt.from_manifest(manifest_path, publisher.boundary)
    from .exchange_calendar import load_cme_capture

    load_cme_capture(receipt, boundary=publisher.boundary)
    return receipt


def _receipt_from_manifest(
    path: Path, *, boundary: RepoBoundary
) -> VerifiedReleaseReceipt:
    return VerifiedReleaseReceipt.from_manifest(path, boundary)


def _write_canonical(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_bytes(dict(payload)) + b"\n"
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        remaining = memoryview(encoded)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError("canonical file write made no progress")
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _replace_active_pointer(
    path: Path, payload: Mapping[str, object], *, boundary: RepoBoundary
) -> None:
    target = boundary.assert_active_path(
        path, purpose="active calendar pointer", subtree="configs"
    )
    temporary = target.with_name(f".{target.name}.new")
    boundary.assert_active_path(
        temporary, purpose="active calendar pointer staging", subtree="configs"
    )
    if temporary.exists():
        raise IntegrityError("active calendar pointer staging path already exists")
    _write_canonical(temporary, payload)
    os.replace(temporary, target)
    fsync_directory(target.parent)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--source-contract", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan")
    plan.add_argument("--mode", choices=("bootstrap", "steady-state"), required=True)
    plan.add_argument("--coverage-start", type=date.fromisoformat, required=True)
    plan.add_argument("--coverage-end", type=date.fromisoformat, required=True)
    plan.add_argument("--product-id", action="append", default=[])
    plan.add_argument("--predecessor-capture-release-id")
    plan.add_argument("--output", type=Path, required=True)

    capture = commands.add_parser("capture")
    capture.add_argument("--plan", type=Path, required=True)
    capture.add_argument("--approval", type=Path, required=True)
    capture.add_argument("--execute", action="store_true")

    parse = commands.add_parser("parse")
    parse.add_argument("--capture-manifest", type=Path, required=True)
    parse.add_argument("--mapping-approval", type=Path)
    parse.add_argument("--mapping-candidates-output", type=Path)
    parse.add_argument("--predecessor-calendar-manifest", type=Path)
    parse.add_argument("--execute", action="store_true")

    diff = commands.add_parser("diff")
    diff.add_argument("--candidate-calendar-manifest", type=Path, required=True)
    diff.add_argument("--predecessor-calendar-manifest", type=Path)
    diff.add_argument("--output", type=Path)

    activate = commands.add_parser("activate")
    activate.add_argument("--candidate-calendar-manifest", type=Path, required=True)
    activate.add_argument("--predecessor-index-manifest", type=Path)
    activate.add_argument("--activation-approval", type=Path, required=True)
    activate.add_argument(
        "--active-pointer",
        type=Path,
        default=Path("configs/active_exchange_calendar.json"),
    )
    activate.add_argument("--execute", action="store_true")

    verify = commands.add_parser("verify")
    verify.add_argument(
        "--active-pointer",
        type=Path,
        default=Path("configs/active_exchange_calendar.json"),
    )
    verify.add_argument("--at-utc")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    repository_root = args.repository_root.resolve(strict=True)
    source_contract = args.source_contract.resolve(strict=True)
    boundary = _boundary(repository_root, source_contract)
    load_exchange_calendar_policy(
        boundary.active_root / "configs" / "exchange_calendar_policy.json"
    )
    markets = approved_research_markets(
        boundary.active_root / "configs" / "research_universe_contract.json"
    )
    if args.command == "plan":
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output, purpose="CME capture plan", subtree="reports"
        )
        payload = build_capture_plan(
            mode=args.mode,
            coverage_start=args.coverage_start,
            coverage_end=args.coverage_end,
            product_ids=tuple(args.product_id),
            predecessor_capture_release_id=args.predecessor_capture_release_id,
        )
        _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "capture":
        if not args.execute:
            parser.error("CME capture requires explicit --execute")
        publisher = _publisher(
            boundary,
            scope={
                "approval_path": str(args.approval),
                "capture_plan_path": str(args.plan),
            },
        )
        receipt = capture_cme_calendar(
            plan_path=args.plan.resolve(strict=True),
            approval_path=args.approval.resolve(strict=True),
            publisher=publisher,
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "parse":
        capture = _receipt_from_manifest(
            args.capture_manifest.resolve(strict=True), boundary=boundary
        )
        if args.mapping_approval is None:
            if args.execute:
                parser.error(
                    "mapping-candidate generation is offline and does not use --execute"
                )
            if args.mapping_candidates_output is None:
                parser.error(
                    "parse without an approval requires --mapping-candidates-output"
                )
            candidates = generate_mapping_candidates(
                capture,
                boundary=boundary,
                expected_markets=markets,
            )
            output = (
                args.mapping_candidates_output
                if args.mapping_candidates_output.is_absolute()
                else boundary.active_root / args.mapping_candidates_output
            )
            boundary.assert_active_path(
                output,
                purpose="CME product mapping candidates",
                subtree="reports",
            )
            _write_canonical(output, candidates)
            print(canonical_bytes(candidates).decode("utf-8"))
            return 0
        if not args.execute:
            parser.error("calendar parsing publication requires explicit --execute")
        predecessor = (
            _receipt_from_manifest(
                args.predecessor_calendar_manifest.resolve(strict=True),
                boundary=boundary,
            )
            if args.predecessor_calendar_manifest is not None
            else None
        )
        mapping = _canonical_object(
            args.mapping_approval.resolve(strict=True),
            description="CME product mapping approval",
        )
        publisher = _publisher(
            boundary,
            scope={
                "capture_release_id": capture.release_id,
                "mapping_approval_receipt_id": str(mapping.get("approval_receipt_id")),
            },
        )
        receipt = publish_verified_exchange_calendar(
            capture_receipt=capture,
            mapping_approval=mapping,
            expected_markets=markets,
            publisher=publisher,
            predecessor_calendar_receipt=predecessor,
        )
        print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
        return 0
    if args.command == "diff":
        candidate_receipt = _receipt_from_manifest(
            args.candidate_calendar_manifest.resolve(strict=True),
            boundary=boundary,
        )
        candidate = VerifiedExchangeCalendar.from_release(
            candidate_receipt, boundary=boundary, expected_markets=markets
        )
        predecessor = None
        if args.predecessor_calendar_manifest is not None:
            predecessor_receipt = _receipt_from_manifest(
                args.predecessor_calendar_manifest.resolve(strict=True),
                boundary=boundary,
            )
            predecessor = VerifiedExchangeCalendar.from_release(
                predecessor_receipt, boundary=boundary, expected_markets=markets
            )
        payload = diff_exchange_calendars(predecessor, candidate)
        if args.output is not None:
            output = (
                args.output
                if args.output.is_absolute()
                else boundary.active_root / args.output
            )
            boundary.assert_active_path(
                output, purpose="calendar diff report", subtree="reports"
            )
            _write_canonical(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if args.command == "activate":
        if not args.execute:
            parser.error("calendar activation requires explicit --execute")
        candidate = _receipt_from_manifest(
            args.candidate_calendar_manifest.resolve(strict=True),
            boundary=boundary,
        )
        predecessor = (
            _receipt_from_manifest(
                args.predecessor_index_manifest.resolve(strict=True),
                boundary=boundary,
            )
            if args.predecessor_index_manifest is not None
            else None
        )
        approval = _canonical_object(
            args.activation_approval.resolve(strict=True),
            description="calendar activation approval",
        )
        publisher = _publisher(
            boundary,
            scope={
                "activation_approval_receipt_id": str(
                    approval.get("approval_receipt_id")
                ),
                "candidate_calendar_release_id": candidate.release_id,
            },
        )
        index_receipt = publish_calendar_index(
            candidate_calendar_receipt=candidate,
            activation_approval=approval,
            publisher=publisher,
            expected_markets=markets,
            predecessor_index_receipt=predecessor,
        )
        index = load_calendar_index(
            index_receipt, boundary=boundary, expected_markets=markets
        )
        pointer = active_pointer_payload(
            index_receipt,
            activation_approval_receipt_id=str(
                approval["approval_receipt_id"]
            ),
            activated_at_utc=str(approval["approved_at"]),
        )
        pointer_path = (
            args.active_pointer
            if args.active_pointer.is_absolute()
            else boundary.active_root / args.active_pointer
        )
        _replace_active_pointer(pointer_path, pointer, boundary=boundary)
        load_active_calendar_index(
            boundary=boundary, expected_markets=markets, path=pointer_path
        )
        print(
            canonical_bytes(
                {
                    "calendar_index_receipt": index.receipt.as_dict(),
                    "pointer": pointer,
                }
            ).decode("utf-8")
        )
        return 0
    if args.command == "verify":
        pointer_path = (
            args.active_pointer
            if args.active_pointer.is_absolute()
            else boundary.active_root / args.active_pointer
        )
        index = load_active_calendar_index(
            boundary=boundary, expected_markets=markets, path=pointer_path
        )
        now = (
            datetime.fromisoformat(args.at_utc.replace("Z", "+00:00"))
            if args.at_utc
            else datetime.now(timezone.utc)
        )
        result = verify_calendar_freshness(
            index, expected_markets=markets, now=now
        )
        print(canonical_bytes(result).decode("utf-8"))
        return 0
    raise AssertionError("unreachable calendar command")


if __name__ == "__main__":
    raise SystemExit(main())
