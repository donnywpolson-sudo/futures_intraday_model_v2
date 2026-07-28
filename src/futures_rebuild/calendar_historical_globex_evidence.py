"""Fail-closed inspection and extraction planning for historical CME Globex evidence."""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from .boundary import RepoBoundary
from .calendar_holiday_schedule_capture import (
    CAPTURE_SCHEMA as HOLIDAY_CAPTURE_SCHEMA,
    RELEASE_KIND as HOLIDAY_RELEASE_KIND,
)
from .calendar_holiday_schedule_discovery import _pdf_reader
from .calendar_notice_attachment_capture import (
    RELEASE_KIND as NOTICE_RELEASE_KIND,
)
from .calendar_notice_attachment_reconciliation_recovery import (
    CAPTURE_SCHEMA as NOTICE_CAPTURE_SCHEMA,
)
from .canonical import (
    canonical_bytes,
    fsync_directory,
    sha256_file,
    sha256_json,
)
from .data_layout import verify_data_release_manifest
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .source_contract import legacy_roots_from_contract


CAPTURE_ASSESSMENT_SCHEMA = (
    "cme_historical_holiday_schedule_capture_assessment/1.0.0"
)
PLAN_SCHEMA = "cme_historical_globex_evidence_extraction_plan/1.0.0"
APPROVAL_SCHEMA = (
    "cme_historical_globex_evidence_extraction_approval/1.0.0"
)
RESULT_SCHEMA = "cme_historical_globex_evidence_extraction/1.0.0"
OPERATION = "EXTRACT_OFFLINE_CME_HISTORICAL_GLOBEX_EVIDENCE"
MAX_SOURCE_FILES = 621
MAX_PDF_PAGES = 2048
MAX_DURATION_SECONDS = 1800
IMPLEMENTATION_PATHS = tuple(
    sorted(
        (
            "configs/source_contract.json",
            "pyproject.toml",
            "src/futures_rebuild/boundary.py",
            "src/futures_rebuild/calendar_historical_globex_evidence.py",
            "src/futures_rebuild/calendar_holiday_schedule_capture.py",
            "src/futures_rebuild/calendar_holiday_schedule_discovery.py",
            "src/futures_rebuild/canonical.py",
            "src/futures_rebuild/data_layout.py",
            "src/futures_rebuild/source_contract.py",
        )
    )
)
FORBIDDEN_ACTIONS = (
    "ACCESS_HOLDOUT_FORWARD_OUTCOMES_OR_PREDICTIONS",
    "ACTIVATE_OR_ACCEPT_CALENDAR",
    "CALL_ANY_NETWORK_OR_PROVIDER_ENDPOINT",
    "DELETE_OR_OVERWRITE_ACCEPTED_RELEASE",
    "EXECUTE_OR_FOLLOW_EMBEDDED_CONTENT",
    "FIT_OR_EVALUATE_MODEL",
    "INFER_RECURRING_OR_MISSING_SCHEDULES",
    "MATERIALIZE_FOUNDATION",
    "PLACE_OR_ROUTE_ORDER",
    "PUBLISH_CALENDAR_INTERVALS",
    "READ_OR_EXPOSE_CREDENTIAL",
)
STOP_CONDITIONS = (
    "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
    "IMPLEMENTATION_HASH_DRIFT",
    "MALFORMED_OR_UNSUPPORTED_SOURCE_FILE",
    "MAX_DURATION_OR_PAGE_BOUND_REACHED",
    "OUTPUT_ALREADY_EXISTS",
    "SOURCE_RELEASE_OR_MANIFEST_DRIFT",
    "UNDECLARED_OUTPUT_OR_NETWORK_ACTIVITY",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SCHEDULE_VERB = re.compile(
    r"\b(?:open|opens|opened|re-?open|re-?opens|close|closes|closed)\b",
    re.IGNORECASE,
)
_TIME_TOKEN = re.compile(
    r"\b(?:[0-2]?\d(?::\d\d)?\s*(?:a\.?m\.?|p\.?m\.?|am|pm|"
    r"CT|CST|CDT|ET|EST|EDT)|noon|midnight)\b",
    re.IGNORECASE,
)
_DATE_TOKEN = re.compile(
    r"\b(?:January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?"
    r"(?:,\s*20\d{2})?\b|\b20\d{2}-\d{2}-\d{2}\b",
    re.IGNORECASE,
)
_YEAR_TOKEN = re.compile(r"\b20(?:1[0-9]|2[0-6])\b")


class HistoricalGlobexEvidenceError(UnauthorizedOperation):
    """Raised before an unapproved or drifted real-source extraction."""


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
                "historical Globex evidence implementation input is missing: "
                f"{relative}"
            )
        hashes[relative] = sha256_file(path)
    return hashes


def _capture_source(
    *,
    manifest_path: Path,
    boundary: RepoBoundary,
    expected_release_kind: str,
    expected_capture_schema: str,
    expected_response_count: int,
) -> tuple[dict[str, object], list[dict[str, object]], Path]:
    boundary.assert_active_path(
        manifest_path,
        purpose="accepted historical CME source manifest",
        subtree="manifests/data_releases/reference",
    )
    manifest = verify_data_release_manifest(
        manifest_path,
        boundary,
        verify_files=True,
    )
    capture = manifest.embedded_documents.get("capture_receipt.json")
    if (
        manifest.release_kind != expected_release_kind
        or manifest.schema_version != expected_capture_schema
        or not isinstance(capture, dict)
        or capture.get("capture_id") != manifest.metadata.get("capture_id")
        or capture.get("unresolved_candidate_count") != 0
        or not isinstance(capture.get("responses"), list)
        or len(capture["responses"]) != expected_response_count
    ):
        raise IntegrityError("accepted historical CME source release is invalid")
    responses: list[dict[str, object]] = []
    for response in capture["responses"]:
        if (
            not isinstance(response, dict)
            or type(response.get("logical_path")) is not str
            or type(response.get("request_id")) is not str
            or type(response.get("ordinal")) is not int
            or type(response.get("sha256")) is not str
            or _SHA256.fullmatch(str(response["sha256"])) is None
            or type(response.get("size")) is not int
            or int(response["size"]) <= 0
        ):
            raise IntegrityError(
                "historical CME source response descriptor is invalid"
            )
        responses.append(dict(response))
    release_root = (
        boundary.active_root
        / "data"
        / "reference"
        / "exchange_calendars"
        / manifest.release_id
    )
    descriptor_set = [
        {
            "logical_path": str(item["logical_path"]),
            "ordinal": int(item["ordinal"]),
            "request_id": str(item["request_id"]),
            "sha256": str(item["sha256"]),
            "size": int(item["size"]),
        }
        for item in responses
    ]
    source = {
        "capture_id": capture["capture_id"],
        "file_descriptor_set_id": sha256_json(descriptor_set),
        "manifest_path": manifest_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "manifest_sha256": sha256_file(manifest_path),
        "release_id": manifest.release_id,
        "release_kind": manifest.release_kind,
        "response_count": len(responses),
        "schema_version": manifest.schema_version,
    }
    return source, responses, release_root


def _classify_captured_pdf(text: str) -> str:
    normalized = " ".join(text.split()).lower()
    if "chicago trading floor holiday schedule" in normalized:
        return "CHICAGO_TRADING_FLOOR_SCHEDULE_NOT_GLOBEX_AUTHORITY"
    if "new york trading floor holiday schedule" in normalized:
        return "NEW_YORK_TRADING_FLOOR_SCHEDULE_NOT_GLOBEX_AUTHORITY"
    if "clearport holiday schedule" in normalized:
        return "CLEARPORT_SCHEDULE_NOT_GLOBEX_AUTHORITY"
    if (
        "cme globex" in normalized
        and "abbreviated session" in normalized
        and "clearing" in normalized
    ):
        return (
            "CLEARING_ADVISORY_NOT_ACCEPTED_AS_EXACT_41_PRODUCT_"
            "GLOBEX_AUTHORITY"
        )
    return "UNRESOLVED_PDF_NOT_ACCEPTED_AS_GLOBEX_AUTHORITY"


def assess_holiday_schedule_capture(
    *,
    manifest_path: Path,
    boundary: RepoBoundary,
    required_coverage_start_trade_date: date,
    required_coverage_end_trade_date: date,
) -> dict[str, object]:
    source, responses, release_root = _capture_source(
        manifest_path=manifest_path,
        boundary=boundary,
        expected_release_kind=HOLIDAY_RELEASE_KIND,
        expected_capture_schema=HOLIDAY_CAPTURE_SCHEMA,
        expected_response_count=5,
    )
    inspected: list[dict[str, object]] = []
    pdf_pages = 0
    parser_warning_codes: list[str] = []
    for response in responses:
        physical = release_root / Path(str(response["logical_path"])).name
        suffix = physical.suffix.lower()
        if suffix == ".pdf":
            warning_codes: list[str] = []
            reader = _pdf_reader(physical, warning_codes=warning_codes)
            page_text = [(page.extract_text() or "") for page in reader.pages]
            pdf_pages += len(page_text)
            parser_warning_codes.extend(warning_codes)
            inspected.append(
                {
                    "classification": _classify_captured_pdf(
                        "\n".join(page_text)
                    ),
                    "ordinal": response["ordinal"],
                    "page_count": len(page_text),
                    "request_id": response["request_id"],
                    "sha256": response["sha256"],
                    "size": response["size"],
                    "url": response.get("url"),
                }
            )
        else:
            inspected.append(
                {
                    "classification": (
                        "SPREADSHEET_NOT_ACCEPTED_AS_GLOBEX_AUTHORITY"
                    ),
                    "ordinal": response["ordinal"],
                    "request_id": response["request_id"],
                    "sha256": response["sha256"],
                    "size": response["size"],
                    "url": response.get("url"),
                }
            )
    classifications = sorted(
        {str(item["classification"]) for item in inspected}
    )
    core: dict[str, object] = {
        "accepted_calendar_interval_count": 0,
        "classification": (
            "INSUFFICIENT_FOR_41_PRODUCT_HISTORICAL_GLOBEX_CALENDAR"
        ),
        "captured_response_count": len(responses),
        "coverage_conclusion": (
            "CAPTURED_FILES_DO_NOT_ESTABLISH_REQUIRED_HISTORICAL_"
            "GLOBEX_PRODUCT_SESSION_COVERAGE"
        ),
        "inspected_files": inspected,
        "observed_classifications": classifications,
        "parser_warning_codes": sorted(set(parser_warning_codes)),
        "parser_warning_count": len(parser_warning_codes),
        "pdf_page_count": pdf_pages,
        "required_coverage_end_trade_date": (
            required_coverage_end_trade_date.isoformat()
        ),
        "required_coverage_start_trade_date": (
            required_coverage_start_trade_date.isoformat()
        ),
        "schema_version": CAPTURE_ASSESSMENT_SCHEMA,
        "source": source,
        "status": "OFFLINE_CAPTURE_INSPECTION_COMPLETE",
    }
    return {**core, "assessment_id": sha256_json(core)}


def validate_capture_assessment(
    payload: Mapping[str, object],
) -> dict[str, object]:
    core = dict(payload)
    assessment_id = core.pop("assessment_id", None)
    files = payload.get("inspected_files")
    if (
        type(assessment_id) is not str
        or _SHA256.fullmatch(assessment_id) is None
        or assessment_id != sha256_json(core)
        or payload.get("schema_version") != CAPTURE_ASSESSMENT_SCHEMA
        or payload.get("status") != "OFFLINE_CAPTURE_INSPECTION_COMPLETE"
        or payload.get("classification")
        != "INSUFFICIENT_FOR_41_PRODUCT_HISTORICAL_GLOBEX_CALENDAR"
        or payload.get("accepted_calendar_interval_count") != 0
        or payload.get("captured_response_count") != 5
        or payload.get("pdf_page_count") != 5
        or not isinstance(files, list)
        or len(files) != 5
    ):
        raise IntegrityError(
            "historical CME holiday capture assessment is invalid"
        )
    return dict(payload)


def _source_authority(
    *,
    notice_manifest_path: Path,
    holiday_manifest_path: Path,
    assessment_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    notice_source, _, _ = _capture_source(
        manifest_path=notice_manifest_path,
        boundary=boundary,
        expected_release_kind=NOTICE_RELEASE_KIND,
        expected_capture_schema=NOTICE_CAPTURE_SCHEMA,
        expected_response_count=616,
    )
    holiday_source, _, _ = _capture_source(
        manifest_path=holiday_manifest_path,
        boundary=boundary,
        expected_release_kind=HOLIDAY_RELEASE_KIND,
        expected_capture_schema=HOLIDAY_CAPTURE_SCHEMA,
        expected_response_count=5,
    )
    boundary.assert_active_path(
        assessment_path,
        purpose="historical CME holiday capture assessment",
        subtree="reports/exchange_calendar",
    )
    assessment = validate_capture_assessment(
        _canonical_object(
            assessment_path,
            description="historical CME holiday capture assessment",
        )
    )
    if assessment.get("source") != holiday_source:
        raise IntegrityError(
            "historical CME holiday capture assessment source drifted"
        )
    return {
        "capture_assessment_id": assessment["assessment_id"],
        "capture_assessment_path": assessment_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "capture_assessment_sha256": sha256_file(assessment_path),
        "holiday_schedule_source": holiday_source,
        "notice_attachment_source": notice_source,
    }


def build_extraction_plan(
    *,
    source_authority: Mapping[str, object],
    implementation_sha256: Mapping[str, str],
    required_coverage_start_trade_date: date,
    required_coverage_end_trade_date: date,
) -> dict[str, object]:
    implementation = dict(sorted(implementation_sha256.items()))
    if (
        tuple(implementation) != IMPLEMENTATION_PATHS
        or any(
            type(value) is not str or _SHA256.fullmatch(value) is None
            for value in implementation.values()
        )
    ):
        raise ContractError(
            "historical Globex evidence implementation hashes are invalid"
        )
    notice_source = source_authority.get("notice_attachment_source")
    holiday_source = source_authority.get("holiday_schedule_source")
    if (
        not isinstance(notice_source, dict)
        or notice_source.get("response_count") != 616
        or not isinstance(holiday_source, dict)
        or holiday_source.get("response_count") != 5
        or type(source_authority.get("capture_assessment_id")) is not str
        or _SHA256.fullmatch(
            str(source_authority["capture_assessment_id"])
        )
        is None
    ):
        raise ContractError(
            "historical Globex evidence source authority is invalid"
        )
    scope: dict[str, object] = {
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "implementation_sha256": implementation,
        "max_duration_seconds": MAX_DURATION_SECONDS,
        "max_pdf_pages": MAX_PDF_PAGES,
        "max_source_files": MAX_SOURCE_FILES,
        "network_request_limit": 0,
        "output_path": (
            "reports/exchange_calendar/"
            "cme_historical_globex_evidence_extraction_{plan_prefix}.json"
        ),
        "purpose": (
            "SURFACE_SOURCE_CITED_EXPLICIT_GLOBEX_TIME_PASSAGES_"
            "WITHOUT_CREATING_CALENDAR_INTERVALS"
        ),
        "required_coverage_end_trade_date": (
            required_coverage_end_trade_date.isoformat()
        ),
        "required_coverage_start_trade_date": (
            required_coverage_start_trade_date.isoformat()
        ),
        "source_authority": dict(source_authority),
        "stop_conditions": list(STOP_CONDITIONS),
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": OPERATION,
        "schema_version": PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_extraction_plan(
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
        or scope.get("max_source_files") != MAX_SOURCE_FILES
        or scope.get("max_pdf_pages") != MAX_PDF_PAGES
        or scope.get("max_duration_seconds") != MAX_DURATION_SECONDS
        or scope.get("network_request_limit") != 0
        or scope.get("forbidden_actions") != list(FORBIDDEN_ACTIONS)
        or scope.get("stop_conditions") != list(STOP_CONDITIONS)
        or tuple(scope.get("implementation_sha256", {}))
        != IMPLEMENTATION_PATHS
    ):
        raise IntegrityError(
            "historical Globex evidence extraction plan is invalid"
        )
    return dict(payload)


def validate_extraction_approval(
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
        raise HistoricalGlobexEvidenceError(
            "historical Globex evidence approval is missing or mismatched"
        )
    return dict(approval)


def candidate_passages(text: str) -> list[dict[str, object]]:
    """Return conservative candidate passages without interpreting schedules."""

    normalized = " ".join(text.split())
    if not normalized:
        return []
    lower = normalized.lower()
    matches: list[dict[str, object]] = []
    seen: set[str] = set()
    for globex_match in re.finditer(r"\b(?:cme\s+)?globex\b", lower):
        start = max(0, globex_match.start() - 450)
        end = min(len(normalized), globex_match.end() + 900)
        passage = normalized[start:end].strip()
        if (
            not _SCHEDULE_VERB.search(passage)
            or not _TIME_TOKEN.search(passage)
            or not _DATE_TOKEN.search(passage)
        ):
            continue
        passage_hash = sha256_json({"passage": passage})
        if passage_hash in seen:
            continue
        seen.add(passage_hash)
        matches.append(
            {
                "passage": passage,
                "passage_sha256": passage_hash,
                "year_hints": sorted(
                    {int(item) for item in _YEAR_TOKEN.findall(passage)}
                ),
            }
        )
    return matches


def _iter_source_pdfs(
    *,
    source: Mapping[str, object],
    boundary: RepoBoundary,
) -> Iterable[tuple[dict[str, object], Path]]:
    manifest_path = boundary.active_root / str(source["manifest_path"])
    expected_kind = str(source["release_kind"])
    expected_schema = str(source["schema_version"])
    expected_count = int(source["response_count"])
    _, responses, release_root = _capture_source(
        manifest_path=manifest_path,
        boundary=boundary,
        expected_release_kind=expected_kind,
        expected_capture_schema=expected_schema,
        expected_response_count=expected_count,
    )
    for response in responses:
        physical = release_root / Path(str(response["logical_path"])).name
        if physical.suffix.lower() == ".pdf":
            yield response, physical


def extract_globex_evidence(
    *,
    plan_path: Path,
    approval_path: Path,
    output_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    boundary.assert_active_path(
        plan_path,
        purpose="historical Globex evidence extraction plan",
        subtree="reports/exchange_calendar",
    )
    boundary.assert_active_path(
        approval_path,
        purpose="historical Globex evidence extraction approval",
        subtree="configs",
    )
    boundary.assert_active_path(
        output_path,
        purpose="historical Globex evidence extraction result",
        subtree="reports/exchange_calendar",
    )
    plan = validate_extraction_plan(
        _canonical_object(
            plan_path,
            description="historical Globex evidence extraction plan",
        )
    )
    approval = validate_extraction_approval(
        approval=_canonical_object(
            approval_path,
            description="historical Globex evidence extraction approval",
        ),
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    scope = plan["scope"]
    if not isinstance(scope, dict):
        raise IntegrityError("historical Globex evidence scope is invalid")
    expected_output = str(scope["output_path"]).format(
        plan_prefix=str(plan["plan_id"])[:8]
    )
    if output_path.relative_to(boundary.active_root).as_posix() != expected_output:
        raise HistoricalGlobexEvidenceError(
            "historical Globex evidence output path drifted"
        )
    current_hashes = implementation_hashes(boundary.active_root)
    if current_hashes != scope.get("implementation_sha256"):
        raise HistoricalGlobexEvidenceError(
            "historical Globex evidence implementation hashes drifted"
        )
    authority = scope.get("source_authority")
    if not isinstance(authority, dict):
        raise IntegrityError(
            "historical Globex evidence source authority is invalid"
        )
    evidence: list[dict[str, object]] = []
    scanned_files = 0
    scanned_pages = 0
    parser_warning_codes: list[str] = []
    for source_name in ("notice_attachment_source", "holiday_schedule_source"):
        source = authority.get(source_name)
        if not isinstance(source, dict):
            raise IntegrityError(
                "historical Globex evidence source descriptor is invalid"
            )
        for response, physical in _iter_source_pdfs(
            source=source,
            boundary=boundary,
        ):
            scanned_files += 1
            warning_codes: list[str] = []
            reader = _pdf_reader(physical, warning_codes=warning_codes)
            parser_warning_codes.extend(warning_codes)
            for page_index, page in enumerate(reader.pages):
                scanned_pages += 1
                if scanned_pages > MAX_PDF_PAGES:
                    raise HistoricalGlobexEvidenceError(
                        "historical Globex evidence page bound exceeded"
                    )
                for passage in candidate_passages(page.extract_text() or ""):
                    record = {
                        **passage,
                        "evidence_id": sha256_json(
                            {
                                "page_index": page_index,
                                "passage_sha256": passage["passage_sha256"],
                                "request_id": response["request_id"],
                                "source_release_id": source["release_id"],
                            }
                        ),
                        "page_index": page_index,
                        "request_id": response["request_id"],
                        "source_release_id": source["release_id"],
                        "source_sha256": response["sha256"],
                        "source_url": response.get("url"),
                    }
                    evidence.append(record)
    evidence.sort(
        key=lambda item: (
            str(item["source_release_id"]),
            str(item["request_id"]),
            int(item["page_index"]),
            str(item["passage_sha256"]),
        )
    )
    core: dict[str, object] = {
        "accepted_calendar_interval_count": 0,
        "approval_receipt_id": approval["approval_receipt_id"],
        "candidate_evidence_count": len(evidence),
        "candidate_evidence_set_id": sha256_json(evidence),
        "candidate_passages": evidence,
        "classification": "CANDIDATE_EVIDENCE_ONLY_NOT_CALENDAR_AUTHORITY",
        "network_request_count": 0,
        "parser_warning_codes": sorted(set(parser_warning_codes)),
        "parser_warning_count": len(parser_warning_codes),
        "plan_id": plan["plan_id"],
        "scanned_pdf_file_count": scanned_files,
        "scanned_pdf_page_count": scanned_pages,
        "schema_version": RESULT_SCHEMA,
        "source_authority": authority,
        "status": "OFFLINE_EXTRACTION_COMPLETE",
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
    assess = commands.add_parser("assess")
    assess.add_argument("--holiday-manifest", type=Path, required=True)
    assess.add_argument("--output", type=Path, required=True)
    assess.add_argument("--coverage-start", type=date.fromisoformat, required=True)
    assess.add_argument("--coverage-end", type=date.fromisoformat, required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--notice-manifest", type=Path, required=True)
    plan.add_argument("--holiday-manifest", type=Path, required=True)
    plan.add_argument("--assessment", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--coverage-start", type=date.fromisoformat, required=True)
    plan.add_argument("--coverage-end", type=date.fromisoformat, required=True)
    extract = commands.add_parser("extract")
    extract.add_argument("--plan", type=Path, required=True)
    extract.add_argument("--approval", type=Path, required=True)
    extract.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    root = args.repository_root.resolve()
    source_contract = (
        args.source_contract
        if args.source_contract.is_absolute()
        else root / args.source_contract
    )
    boundary = _boundary(root, source_contract)

    def rooted(path: Path) -> Path:
        return path if path.is_absolute() else root / path

    if args.command == "assess":
        payload = assess_holiday_schedule_capture(
            manifest_path=rooted(args.holiday_manifest),
            boundary=boundary,
            required_coverage_start_trade_date=args.coverage_start,
            required_coverage_end_trade_date=args.coverage_end,
        )
        _write_create_only(rooted(args.output), payload)
    elif args.command == "plan":
        authority = _source_authority(
            notice_manifest_path=rooted(args.notice_manifest),
            holiday_manifest_path=rooted(args.holiday_manifest),
            assessment_path=rooted(args.assessment),
            boundary=boundary,
        )
        payload = build_extraction_plan(
            source_authority=authority,
            implementation_sha256=implementation_hashes(root),
            required_coverage_start_trade_date=args.coverage_start,
            required_coverage_end_trade_date=args.coverage_end,
        )
        _write_create_only(rooted(args.output), payload)
    else:
        extract_globex_evidence(
            plan_path=rooted(args.plan),
            approval_path=rooted(args.approval),
            output_path=rooted(args.output),
            boundary=boundary,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
