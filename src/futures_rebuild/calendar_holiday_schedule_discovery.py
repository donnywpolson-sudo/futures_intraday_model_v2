"""Offline discovery of authoritative CME holiday-schedule document links."""

from __future__ import annotations

import importlib.metadata
import logging
import re
import urllib.parse
from collections import defaultdict
from pathlib import Path
from typing import Mapping

from .boundary import RepoBoundary
from .calendar_notice_attachment_capture import RELEASE_KIND
from .calendar_notice_attachment_reconciliation_recovery import (
    CAPTURE_SCHEMA as SOURCE_CAPTURE_SCHEMA,
)
from .canonical import sha256_file, sha256_json
from .data_layout import verify_data_release_manifest
from .errors import ContractError, IntegrityError


ASSESSMENT_SCHEMA = "cme_historical_holiday_schedule_discovery/1.0.0"
PARSER_NAME = "pypdf"
PARSER_VERSION = "6.14.2"
HOLIDAY_FILE_PREFIX = (
    "/tools-information/holiday-calendar/files/"
)
ALLOWED_EXTENSIONS = (".pdf", ".xls", ".xlsx")
_URL_PATTERN = re.compile(r"https?://[^\s<>\]\)]+", re.IGNORECASE)
_YEAR_PATTERN = re.compile(r"(?:^|[^0-9])(20(?:1[0-9]|2[0-9]))(?:[^0-9]|$)")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class _ParserWarningCollector(logging.Handler):
    """Collect bounded parser diagnostics without preserving free-form text."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.codes: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if message.startswith("Ignoring wrong pointing object "):
            self.codes.append("WRONG_POINTING_OBJECT_IGNORED")
        else:
            self.codes.append("OTHER_PYPDF_WARNING")


def _pdf_reader(path: Path, *, warning_codes: list[str]):
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise ContractError(
            "pypdf==6.14.2 is required for offline holiday discovery"
        ) from exc
    version = importlib.metadata.version(PARSER_NAME)
    if version != PARSER_VERSION:
        raise ContractError(
            "offline holiday discovery requires pypdf==6.14.2"
        )
    collector = _ParserWarningCollector()
    logger = logging.getLogger("pypdf._reader")
    prior_propagate = logger.propagate
    logger.addHandler(collector)
    logger.propagate = False
    try:
        reader = PdfReader(str(path))
    finally:
        logger.propagate = prior_propagate
        logger.removeHandler(collector)
    warning_codes.extend(collector.codes)
    return reader


def _normalized_candidate(raw: str) -> str | None:
    value = raw.strip().rstrip(".,;").replace("http://", "https://", 1)
    parsed = urllib.parse.urlparse(value)
    extension = Path(parsed.path).suffix.lower()
    if (
        parsed.scheme != "https"
        or parsed.hostname not in {"cmegroup.com", "www.cmegroup.com"}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith(HOLIDAY_FILE_PREFIX)
        or extension not in ALLOWED_EXTENSIONS
    ):
        return None
    return urllib.parse.urlunparse(
        ("https", "www.cmegroup.com", parsed.path, "", "", "")
    )


def _page_links(page) -> tuple[set[str], set[str]]:
    annotations: set[str] = set()
    for reference in page.get("/Annots") or []:
        try:
            action = reference.get_object().get("/A")
            uri = action.get("/URI") if action else None
        except Exception:
            uri = None
        if uri:
            annotations.add(str(uri))
    text = page.extract_text() or ""
    text = re.sub(
        r"(?<=[A-Za-z0-9])-\s+(?=[A-Za-z0-9])",
        "-",
        text,
    )
    extracted = {
        match.rstrip(".,;")
        for match in _URL_PATTERN.findall(text)
    }
    return annotations, extracted


def build_holiday_schedule_discovery(
    *,
    source_manifest_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    boundary.assert_active_path(
        source_manifest_path,
        purpose="accepted CME attachment manifest",
        subtree="manifests/data_releases/reference",
    )
    manifest = verify_data_release_manifest(
        source_manifest_path,
        boundary,
        verify_files=True,
    )
    capture = manifest.embedded_documents.get("capture_receipt.json")
    if (
        manifest.release_kind != RELEASE_KIND
        or manifest.schema_version != SOURCE_CAPTURE_SCHEMA
        or not isinstance(capture, dict)
        or capture.get("capture_id") != manifest.metadata.get("capture_id")
        or capture.get("unresolved_candidate_count") != 0
        or not isinstance(capture.get("responses"), list)
    ):
        raise IntegrityError(
            "CME holiday discovery source release is invalid"
        )
    release_root = (
        boundary.active_root
        / "data"
        / "reference"
        / "exchange_calendars"
        / manifest.release_id
    )
    evidence: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "annotation_pages": set(),
            "source_ordinals": set(),
            "source_request_ids": set(),
            "text_pages": set(),
        }
    )
    extraction_errors: list[dict[str, object]] = []
    parser_warning_codes: list[str] = []
    parser_warning_sources: set[str] = set()
    unresolved_holiday_url_tokens: set[str] = set()
    scanned_pages = 0
    for response in capture["responses"]:
        if not isinstance(response, dict):
            raise IntegrityError(
                "CME holiday discovery response descriptor is invalid"
            )
        logical_path = response.get("logical_path")
        if type(logical_path) is not str:
            raise IntegrityError(
                "CME holiday discovery response path is invalid"
            )
        physical = release_root / Path(logical_path).name
        if physical.suffix.lower() != ".pdf":
            continue
        try:
            warning_codes: list[str] = []
            reader = _pdf_reader(
                physical,
                warning_codes=warning_codes,
            )
            if warning_codes:
                parser_warning_codes.extend(warning_codes)
                parser_warning_sources.add(str(response["request_id"]))
            for page_index, page in enumerate(reader.pages):
                scanned_pages += 1
                annotations, extracted = _page_links(page)
                for kind, candidates in (
                    ("annotation_pages", annotations),
                    ("text_pages", extracted),
                ):
                    for raw in candidates:
                        candidate = _normalized_candidate(raw)
                        if candidate is None:
                            if HOLIDAY_FILE_PREFIX in raw:
                                unresolved_holiday_url_tokens.add(
                                    sha256_json({"raw_url_token": raw})
                                )
                            continue
                        record = evidence[candidate]
                        record[kind].add(page_index)
                        record["source_ordinals"].add(response["ordinal"])
                        record["source_request_ids"].add(
                            response["request_id"]
                        )
        except Exception as exc:
            extraction_errors.append(
                {
                    "error_class": type(exc).__name__,
                    "ordinal": response["ordinal"],
                    "request_id": response["request_id"],
                }
            )
    candidates: list[dict[str, object]] = []
    for ordinal, url in enumerate(sorted(evidence), start=1):
        record = evidence[url]
        evidence_kinds = []
        if record["annotation_pages"]:
            evidence_kinds.append("PDF_ANNOTATION_URI")
        if record["text_pages"]:
            evidence_kinds.append("PDF_EXTRACTED_TEXT_URL")
        year_match = _YEAR_PATTERN.search(Path(url).name)
        candidates.append(
            {
                "candidate_id": sha256_json(
                    {"ordinal": ordinal, "url": url}
                ),
                "evidence_kinds": evidence_kinds,
                "extension": Path(url).suffix.lower(),
                "ordinal": ordinal,
                "source_annotation_pages": sorted(
                    record["annotation_pages"]
                ),
                "source_ordinals": sorted(record["source_ordinals"]),
                "source_request_ids": sorted(
                    record["source_request_ids"]
                ),
                "source_text_pages": sorted(record["text_pages"]),
                "url": url,
                "year_hint": (
                    int(year_match.group(1)) if year_match else None
                ),
            }
        )
    if (
        extraction_errors
        or unresolved_holiday_url_tokens
        or not candidates
    ):
        raise IntegrityError(
            "CME holiday discovery is incomplete"
        )
    core: dict[str, object] = {
        "candidate_count": len(candidates),
        "candidate_set_id": sha256_json(candidates),
        "candidates": candidates,
        "classification": (
            "ADDITIONAL_AUTHORITATIVE_HOLIDAY_SCHEDULE_FILES_REQUIRED"
        ),
        "coverage_conclusion": (
            "CURRENT_ATTACHMENT_RELEASE_DOES_NOT_CLOSE_HISTORICAL_"
            "CALENDAR_COVERAGE"
        ),
        "extraction_error_count": 0,
        "parser": {
            "name": PARSER_NAME,
            "version": PARSER_VERSION,
        },
        "parser_warning_codes": sorted(set(parser_warning_codes)),
        "parser_warning_count": len(parser_warning_codes),
        "parser_warning_source_request_ids": sorted(
            parser_warning_sources
        ),
        "scanned_page_count": scanned_pages,
        "schema_version": ASSESSMENT_SCHEMA,
        "source_capture_id": capture["capture_id"],
        "source_manifest_path": source_manifest_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "source_manifest_sha256": sha256_file(source_manifest_path),
        "source_release_id": manifest.release_id,
        "source_response_count": len(capture["responses"]),
        "status": "OFFLINE_DISCOVERY_COMPLETE",
        "unresolved_holiday_url_token_count": 0,
    }
    return {**core, "assessment_id": sha256_json(core)}


def validate_holiday_schedule_discovery(
    payload: Mapping[str, object],
) -> dict[str, object]:
    core = dict(payload)
    assessment_id = core.pop("assessment_id", None)
    candidates = payload.get("candidates")
    parser = payload.get("parser")
    if (
        type(assessment_id) is not str
        or _SHA256.fullmatch(assessment_id) is None
        or assessment_id != sha256_json(core)
        or payload.get("schema_version") != ASSESSMENT_SCHEMA
        or payload.get("status") != "OFFLINE_DISCOVERY_COMPLETE"
        or payload.get("classification")
        != "ADDITIONAL_AUTHORITATIVE_HOLIDAY_SCHEDULE_FILES_REQUIRED"
        or payload.get("coverage_conclusion")
        != (
            "CURRENT_ATTACHMENT_RELEASE_DOES_NOT_CLOSE_HISTORICAL_"
            "CALENDAR_COVERAGE"
        )
        or payload.get("extraction_error_count") != 0
        or payload.get("unresolved_holiday_url_token_count") != 0
        or not isinstance(parser, dict)
        or parser
        != {"name": PARSER_NAME, "version": PARSER_VERSION}
        or type(payload.get("parser_warning_count")) is not int
        or int(payload["parser_warning_count"]) < 0
        or not isinstance(payload.get("parser_warning_codes"), list)
        or any(
            item
            not in {
                "OTHER_PYPDF_WARNING",
                "WRONG_POINTING_OBJECT_IGNORED",
            }
            for item in payload["parser_warning_codes"]
        )
        or not isinstance(
            payload.get("parser_warning_source_request_ids"),
            list,
        )
        or (
            int(payload["parser_warning_count"]) == 0
            and (
                payload["parser_warning_codes"]
                or payload["parser_warning_source_request_ids"]
            )
        )
        or (
            int(payload["parser_warning_count"]) > 0
            and (
                not payload["parser_warning_codes"]
                or not payload["parser_warning_source_request_ids"]
            )
        )
        or not isinstance(candidates, list)
        or payload.get("candidate_count") != len(candidates)
        or payload.get("candidate_set_id") != sha256_json(candidates)
        or len({item.get("url") for item in candidates if isinstance(item, dict)})
        != len(candidates)
        or any(
            not isinstance(item, dict)
            or item.get("ordinal") != ordinal
            or _normalized_candidate(str(item.get("url"))) != item.get("url")
            for ordinal, item in enumerate(candidates, start=1)
        )
    ):
        raise IntegrityError(
            "CME holiday discovery assessment is invalid"
        )
    return dict(payload)
