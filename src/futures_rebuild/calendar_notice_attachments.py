"""Offline attachment discovery for an accepted CME notice HTML union."""

from __future__ import annotations

import hashlib
import html
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
from pathlib import Path
from typing import Mapping

from .boundary import RepoBoundary
from .calendar_notice_union_recovery import (
    load_recovery_union_capture,
)
from .canonical import sha256_file, sha256_json
from .data_layout import (
    DataReleaseReceipt,
    verify_data_release_manifest,
)
from .errors import IntegrityError


ASSESSMENT_SCHEMA = (
    "cme_historical_notice_attachment_discovery_assessment/1.1.0"
)
PARSE_WORKERS = 16
_FILE_EXTENSIONS = frozenset(
    {".csv", ".doc", ".docx", ".pdf", ".txt", ".xls", ".xlsx", ".zip"}
)
_SCHEDULE_LANGUAGE = re.compile(
    r"\b(?:trading hours?|holiday|session|close[ds]?|closing|reopen(?:ing|s|ed)?|"
    r"early close|late open|central time|settlement)\b",
    re.IGNORECASE,
)
_EXPLICIT_TIME = re.compile(
    r"\b(?:[01]?\d|2[0-3])(?::[0-5]\d)?\s*"
    r"(?:a\.?m\.?|p\.?m\.?|ct|central time)\b",
    re.IGNORECASE,
)
_RELEVANT_LINK_TEXT = re.compile(
    r"\b(?:full text|advisory|attachment|download|holiday schedule|"
    r"trading hours?|schedule)\b",
    re.IGNORECASE,
)


class _ArticleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.article_count = 0
        self._article_container_tag: str | None = None
        self._article_depth = 0
        self._current_anchor: dict[str, object] | None = None
        self._text: list[str] = []
        self.links: list[dict[str, object]] = []

    @property
    def in_article(self) -> bool:
        return self._article_depth > 0

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        attributes = {
            key.lower(): value or "" for key, value in attrs
        }
        lowered = tag.lower()
        if not self.in_article:
            classes = set(attributes.get("class", "").split())
            is_legacy = (
                lowered == "li" and "cmeAdvisoryContent" in classes
            )
            is_current = (
                lowered == "div"
                and "advisory-notice-header" in classes
            )
            if is_legacy or is_current:
                self.article_count += 1
                self._article_container_tag = lowered
                self._article_depth = 1
                file_reference = attributes.get(
                    "data-file-reference", ""
                ).strip()
                if file_reference:
                    self.links.append(
                        {
                            "href": file_reference,
                            "rel": ["embedded-file-reference"],
                            "text": attributes.get("data-title", ""),
                        }
                    )
            return
        if lowered == self._article_container_tag:
            self._article_depth += 1
        if lowered == "a":
            self._current_anchor = {
                "href": attributes.get("href", ""),
                "rel": sorted(
                    set(attributes.get("rel", "").lower().split())
                ),
                "text_parts": [],
            }

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if not self.in_article:
            return
        if lowered == "a" and self._current_anchor is not None:
            text_parts = self._current_anchor.pop("text_parts")
            assert isinstance(text_parts, list)
            self.links.append(
                {
                    **self._current_anchor,
                    "text": " ".join(" ".join(text_parts).split()),
                }
            )
            self._current_anchor = None
        if lowered == self._article_container_tag:
            self._article_depth -= 1
            if self._article_depth == 0:
                self._article_container_tag = None

    def handle_data(self, data: str) -> None:
        if not self.in_article:
            return
        value = " ".join(data.split())
        if not value:
            return
        self._text.append(value)
        if self._current_anchor is not None:
            text_parts = self._current_anchor["text_parts"]
            assert isinstance(text_parts, list)
            text_parts.append(value)

    @property
    def article_text(self) -> str:
        return " ".join(self._text)


def _normalized_title_token(title: str) -> str:
    return "".join(character for character in title.lower() if character.isalnum())


def _normalized_url(
    href: str,
    *,
    notice_url: str,
) -> tuple[str | None, str | None]:
    value = html.unescape(href).strip()
    if not value or value.startswith(("#", "javascript:", "mailto:")):
        return None, "NON_DOCUMENT_OR_FRAGMENT_LINK"
    joined = urllib.parse.urljoin(notice_url, value)
    parsed = urllib.parse.urlparse(joined)
    if (
        parsed.hostname is None
        or parsed.hostname.lower() != "www.cmegroup.com"
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None, "NON_CME_HOST"
    if parsed.query or parsed.fragment:
        return None, "QUERY_OR_FRAGMENT_PRESENT"
    if Path(parsed.path).suffix.lower() not in _FILE_EXTENSIONS:
        return None, "NON_ATTACHMENT_EXTENSION"
    canonical = urllib.parse.urlunparse(
        ("https", "www.cmegroup.com", parsed.path, "", "", "")
    )
    return canonical, None


def _link_reasons(
    *,
    url: str,
    rel: list[object],
    text: str,
    title: str,
) -> list[str]:
    path = urllib.parse.urlparse(url).path.lower()
    filename_token = _normalized_title_token(Path(path).stem)
    title_token = _normalized_title_token(title)
    reasons: list[str] = []
    if "embedded-file-reference" in rel:
        reasons.append("EMBEDDED_FILE_REFERENCE")
    if "download" in rel:
        reasons.append("REL_DOWNLOAD")
    if path.startswith("/notices/"):
        reasons.append("NOTICE_PATH")
    if (
        path.startswith("/tools-information/lookups/advisories/")
        and "/files/" in path
    ):
        reasons.append("ADVISORY_FILES_PATH")
    if _RELEVANT_LINK_TEXT.search(text):
        reasons.append("RELEVANT_LINK_TEXT")
    if (
        len(title_token) >= 5
        and (
            title_token in filename_token
            or filename_token in title_token
        )
    ):
        reasons.append("NOTICE_TITLE_FILENAME_MATCH")
    return sorted(set(reasons))


def _parse_page(
    response: Mapping[str, object],
    physical: Path,
) -> dict[str, object]:
    raw = physical.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if (
        digest != response.get("sha256")
        or len(raw) != response.get("size")
    ):
        raise IntegrityError(
            "CME notice attachment source bytes changed"
        )
    try:
        document = raw.decode("utf-8")
    except UnicodeDecodeError:
        document = raw.decode("utf-8", errors="replace")
    parser = _ArticleParser()
    parser.feed(document)
    parser.close()
    article_text = parser.article_text
    notice_url = str(response["url"])
    title = str(response["metadata_title"])
    candidates: list[dict[str, object]] = []
    exclusions: list[dict[str, object]] = []
    article_file_links = 0
    for link in parser.links:
        href = str(link["href"])
        url, rejection = _normalized_url(href, notice_url=notice_url)
        if rejection is not None:
            if rejection != "NON_ATTACHMENT_EXTENSION":
                exclusions.append(
                    {
                        "href": href,
                        "reason": rejection,
                        "text": link["text"],
                    }
                )
            continue
        assert url is not None
        article_file_links += 1
        rel = link["rel"]
        assert isinstance(rel, list)
        reasons = _link_reasons(
            url=url,
            rel=rel,
            text=str(link["text"]),
            title=title,
        )
        item = {
            "discovery_reasons": reasons,
            "extension": Path(urllib.parse.urlparse(url).path).suffix.lower(),
            "href": href,
            "link_text": link["text"],
            "url": url,
        }
        if reasons:
            candidates.append(item)
        else:
            exclusions.append(
                {
                    **item,
                    "reason": "NO_NOTICE_ATTACHMENT_SIGNAL",
                }
            )
    has_schedule_language = _SCHEDULE_LANGUAGE.search(article_text) is not None
    has_explicit_time = _EXPLICIT_TIME.search(article_text) is not None
    if has_schedule_language and has_explicit_time:
        inline_status = "INLINE_EXPLICIT_TIME_EVIDENCE_PRESENT"
    elif has_schedule_language:
        inline_status = "INLINE_CALENDAR_LANGUAGE_WITHOUT_EXPLICIT_TIME"
    else:
        inline_status = "NO_INLINE_CALENDAR_LANGUAGE"
    return {
        "article_container_count": parser.article_count,
        "article_file_link_count": article_file_links,
        "article_text_character_count": len(article_text),
        "article_text_sha256": hashlib.sha256(
            article_text.encode("utf-8")
        ).hexdigest(),
        "candidate_links": candidates,
        "excluded_file_or_external_links": exclusions,
        "inline_evidence_status": inline_status,
        "logical_path": response["logical_path"],
        "matched_queries": response["matched_queries"],
        "metadata_title": title,
        "notice_url": notice_url,
        "ordinal": response["ordinal"],
        "request_id": response["request_id"],
        "source_sha256": digest,
    }


def build_attachment_assessment(
    *,
    union_manifest_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    receipt = DataReleaseReceipt.from_manifest(
        union_manifest_path,
        boundary,
        verify_files=False,
    )
    capture = load_recovery_union_capture(
        receipt,
        boundary=boundary,
        verify_payload_files=False,
    )
    manifest = verify_data_release_manifest(
        union_manifest_path,
        boundary,
        verify_files=False,
    )
    entries = {entry.logical_path: entry for entry in manifest.files}
    responses = capture.get("responses")
    if not isinstance(responses, list) or len(responses) != len(manifest.files):
        raise IntegrityError(
            "CME notice attachment assessment source is incomplete"
        )
    with ThreadPoolExecutor(max_workers=PARSE_WORKERS) as executor:
        futures = []
        for response in responses:
            if not isinstance(response, dict):
                raise IntegrityError(
                    "CME notice attachment source response is invalid"
                )
            entry = entries.get(str(response.get("logical_path")))
            if entry is None:
                raise IntegrityError(
                    "CME notice attachment source file is absent"
                )
            physical = (
                boundary.active_root
                / manifest.physical_relative_path(entry)
            )
            futures.append(
                executor.submit(_parse_page, response, physical)
            )
        pages = [future.result() for future in futures]
    pages.sort(key=lambda item: int(item["ordinal"]))
    if any(item["article_container_count"] != 1 for item in pages):
        status = "INCOMPLETE_ARTICLE_CONTAINER_COVERAGE"
    else:
        status = "COMPLETE_OFFLINE_ATTACHMENT_DISCOVERY"
    attachment_sources: dict[str, dict[str, object]] = {}
    excluded_urls: dict[str, dict[str, object]] = {}
    for page in pages:
        for link in page["candidate_links"]:
            assert isinstance(link, dict)
            url = str(link["url"])
            aggregate = attachment_sources.setdefault(
                url,
                {
                    "discovery_reasons": set(),
                    "extension": link["extension"],
                    "link_texts": set(),
                    "source_notice_request_ids": set(),
                    "source_notice_urls": set(),
                    "source_titles": set(),
                    "url": url,
                },
            )
            aggregate["discovery_reasons"].update(link["discovery_reasons"])  # type: ignore[union-attr]
            aggregate["link_texts"].add(link["link_text"])  # type: ignore[union-attr]
            aggregate["source_notice_request_ids"].add(page["request_id"])  # type: ignore[union-attr]
            aggregate["source_notice_urls"].add(page["notice_url"])  # type: ignore[union-attr]
            aggregate["source_titles"].add(page["metadata_title"])  # type: ignore[union-attr]
        for link in page["excluded_file_or_external_links"]:
            assert isinstance(link, dict)
            url = str(link.get("url") or link.get("href"))
            aggregate = excluded_urls.setdefault(
                url,
                {
                    "reasons": set(),
                    "source_notice_request_ids": set(),
                    "url_or_href": url,
                },
            )
            aggregate["reasons"].add(link["reason"])  # type: ignore[union-attr]
            aggregate["source_notice_request_ids"].add(page["request_id"])  # type: ignore[union-attr]
    attachments = []
    for url in sorted(attachment_sources):
        item = attachment_sources[url]
        attachments.append(
            {
                key: sorted(value) if isinstance(value, set) else value
                for key, value in item.items()
            }
        )
    exclusions = []
    for url in sorted(excluded_urls):
        item = excluded_urls[url]
        exclusions.append(
            {
                key: sorted(value) if isinstance(value, set) else value
                for key, value in item.items()
            }
        )
    inline_counts: dict[str, int] = {}
    for page in pages:
        value = str(page["inline_evidence_status"])
        inline_counts[value] = inline_counts.get(value, 0) + 1
    core: dict[str, object] = {
        "attachment_candidate_count": len(attachments),
        "attachment_candidates": attachments,
        "excluded_unique_link_count": len(exclusions),
        "excluded_unique_links": exclusions,
        "inline_evidence_status_counts": dict(sorted(inline_counts.items())),
        "page_count": len(pages),
        "pages": pages,
        "parse_workers": PARSE_WORKERS,
        "purpose": (
            "OFFLINE_DISCOVERY_ONLY_NOT_ACCEPTED_HISTORICAL_CALENDAR_EVIDENCE"
        ),
        "schema_version": ASSESSMENT_SCHEMA,
        "status": status,
        "union_capture_id": capture["capture_id"],
        "union_manifest_path": union_manifest_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "union_manifest_sha256": sha256_file(union_manifest_path),
        "union_release_id": receipt.release_id,
    }
    return {**core, "assessment_id": sha256_json(core)}
