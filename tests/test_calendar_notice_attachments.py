from pathlib import Path
from types import SimpleNamespace

import hashlib
import futures_rebuild.calendar_notice_attachments as attachment_module
from futures_rebuild.calendar_notice_attachments import (
    _parse_page,
    build_attachment_assessment,
)


def _response(
    *,
    logical_path: str,
    ordinal: int = 1,
    payload: bytes = b"",
) -> dict[str, object]:
    return {
        "logical_path": logical_path,
        "matched_queries": ["holiday"],
        "metadata_title": "Holiday Trading Schedule",
        "notice_url": (
            "https://www.cmegroup.com/notices/clearing/2018/12/"
            f"notice-{ordinal}.html"
        ),
        "ordinal": ordinal,
        "request_id": f"request-{ordinal}",
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size": len(payload),
        "url": (
            "https://www.cmegroup.com/notices/clearing/2018/12/"
            f"notice-{ordinal}.html"
        ),
    }


def test_article_parser_selects_only_cme_notice_attachments(
    tmp_path: Path,
) -> None:
    document = """
    <html>
      <li class="cmeAdvisoryContent cmeClearContent">
        Trading hours close at 12:00 p.m. CT.
        <a rel="download" href="/notices/clearing/2018/12/Chadv18-474.pdf">
          Full text advisory
        </a>
        <a href="https://example.com/other.pdf">External PDF</a>
        <a href="/content/cmegroup/en/footer/legal.pdf">Legal PDF</a>
      </li>
      <footer>
        <a rel="download" href="/notices/footer-unrelated.pdf">Footer PDF</a>
      </footer>
    </html>
    """.encode()
    physical = tmp_path / "notice.html"
    physical.write_bytes(document)

    parsed = _parse_page(
        _response(
            logical_path="data/reference/notice.html",
            payload=document,
        ),
        physical,
    )

    assert parsed["article_container_count"] == 1
    assert parsed["inline_evidence_status"] == (
        "INLINE_EXPLICIT_TIME_EVIDENCE_PRESENT"
    )
    assert [item["url"] for item in parsed["candidate_links"]] == [
        "https://www.cmegroup.com/notices/clearing/2018/12/"
        "Chadv18-474.pdf"
    ]
    assert {
        item["reason"]
        for item in parsed["excluded_file_or_external_links"]
    } == {"NON_CME_HOST", "NO_NOTICE_ATTACHMENT_SIGNAL"}


def test_article_parser_reports_missing_article_container(
    tmp_path: Path,
) -> None:
    physical = tmp_path / "notice.html"
    physical.write_text(
        '<a rel="download" href="/notices/unrelated.pdf">download</a>',
        encoding="utf-8",
    )

    payload = physical.read_bytes()
    parsed = _parse_page(
        _response(
            logical_path="data/reference/notice.html",
            payload=payload,
        ),
        physical,
    )

    assert parsed["article_container_count"] == 0
    assert parsed["candidate_links"] == []
    assert parsed["inline_evidence_status"] == "NO_INLINE_CALENDAR_LANGUAGE"


def test_current_advisory_header_exposes_embedded_pdf(
    tmp_path: Path,
) -> None:
    document = b"""
    <div class="component react advisory-notice-header"
         data-title="Holiday Hours"
         data-file-reference="/content/dam/cmegroup/notices/holiday.pdf"
         data-is-pdf="true">
      <div path="text">
        <p>Trading closes at 1:00 p.m. CT.</p>
      </div>
    </div>
    <footer>
      <a href="/notices/unrelated.pdf">Unrelated footer PDF</a>
    </footer>
    """
    physical = tmp_path / "current.html"
    physical.write_bytes(document)

    parsed = _parse_page(
        _response(
            logical_path="data/reference/current.html",
            payload=document,
        ),
        physical,
    )

    assert parsed["article_container_count"] == 1
    assert parsed["inline_evidence_status"] == (
        "INLINE_EXPLICIT_TIME_EVIDENCE_PRESENT"
    )
    assert parsed["candidate_links"] == [
        {
            "discovery_reasons": [
                "EMBEDDED_FILE_REFERENCE",
                "NOTICE_TITLE_FILENAME_MATCH",
            ],
            "extension": ".pdf",
            "href": "/content/dam/cmegroup/notices/holiday.pdf",
            "link_text": "Holiday Hours",
            "url": (
                "https://www.cmegroup.com/content/dam/cmegroup/"
                "notices/holiday.pdf"
            ),
        }
    ]


def test_assessment_deduplicates_attachment_and_binds_sources(
    boundary,
    monkeypatch,
) -> None:
    logical_paths = [
        "data/reference/exchange_calendars/notice-1.html",
        "data/reference/exchange_calendars/notice-2.html",
    ]
    shared_pdf = "/notices/clearing/2018/12/Chadv18-474.pdf"
    payloads: list[bytes] = []
    for ordinal, logical_path in enumerate(logical_paths, start=1):
        physical = boundary.active_root / logical_path
        physical.parent.mkdir(parents=True, exist_ok=True)
        payload = (
            (
                '<li class="cmeAdvisoryContent cmeClearContent">'
                "Holiday trading hours. "
                f'<a rel="download" href="{shared_pdf}">Full text</a>'
                "</li>"
            ).encode("utf-8")
        )
        physical.write_bytes(payload)
        payloads.append(payload)
    manifest_path = (
        boundary.active_root
        / "manifests"
        / "data_releases"
        / "reference"
        / "union.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("{}\n", encoding="utf-8")
    entries = [
        SimpleNamespace(logical_path=logical_path)
        for logical_path in logical_paths
    ]

    class FakeManifest:
        files = entries

        @staticmethod
        def physical_relative_path(entry):
            return Path(entry.logical_path)

    monkeypatch.setattr(
        attachment_module.DataReleaseReceipt,
        "from_manifest",
        classmethod(
            lambda _cls, *_args, **_kwargs: SimpleNamespace(
                release_id="a" * 64
            )
        ),
    )
    monkeypatch.setattr(
        attachment_module,
        "verify_data_release_manifest",
        lambda *_args, **_kwargs: FakeManifest(),
    )
    monkeypatch.setattr(
        attachment_module,
        "load_recovery_union_capture",
        lambda *_args, **_kwargs: {
            "capture_id": "b" * 64,
            "responses": [
                _response(
                    logical_path=logical_path,
                    ordinal=ordinal,
                    payload=payloads[ordinal - 1],
                )
                for ordinal, logical_path in enumerate(
                    logical_paths, start=1
                )
            ],
        },
    )

    assessment = build_attachment_assessment(
        union_manifest_path=manifest_path,
        boundary=boundary,
    )

    assert assessment["status"] == "COMPLETE_OFFLINE_ATTACHMENT_DISCOVERY"
    assert assessment["page_count"] == 2
    assert assessment["attachment_candidate_count"] == 1
    candidate = assessment["attachment_candidates"][0]
    assert candidate["source_notice_request_ids"] == [
        "request-1",
        "request-2",
    ]
    assert candidate["url"] == (
        "https://www.cmegroup.com/notices/clearing/2018/12/"
        "Chadv18-474.pdf"
    )
