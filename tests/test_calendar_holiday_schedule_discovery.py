import copy

import pytest

from futures_rebuild.calendar_holiday_schedule_discovery import (
    PARSER_NAME,
    PARSER_VERSION,
    _normalized_candidate,
    _page_links,
    validate_holiday_schedule_discovery,
)
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError


class _Reference:
    def __init__(self, value):
        self.value = value

    def get_object(self):
        return self.value


class _Page(dict):
    def __init__(self, *, annotations, text):
        super().__init__({"/Annots": annotations})
        self.text = text

    def extract_text(self):
        return self.text


def _assessment():
    candidates = [
        {
            "candidate_id": sha256_json(
                {
                    "ordinal": 1,
                    "url": (
                        "https://www.cmegroup.com/tools-information/"
                        "holiday-calendar/files/holiday_schedule_2012.pdf"
                    ),
                }
            ),
            "evidence_kinds": ["PDF_ANNOTATION_URI"],
            "extension": ".pdf",
            "ordinal": 1,
            "source_annotation_pages": [0],
            "source_ordinals": [11],
            "source_request_ids": ["attachment-0011-deadbeef0000"],
            "source_text_pages": [],
            "url": (
                "https://www.cmegroup.com/tools-information/"
                "holiday-calendar/files/holiday_schedule_2012.pdf"
            ),
            "year_hint": 2012,
        }
    ]
    core = {
        "candidate_count": 1,
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
        "parser": {"name": PARSER_NAME, "version": PARSER_VERSION},
        "parser_warning_codes": ["WRONG_POINTING_OBJECT_IGNORED"],
        "parser_warning_count": 1,
        "parser_warning_source_request_ids": [
            "attachment-0011-deadbeef0000"
        ],
        "scanned_page_count": 1,
        "schema_version": (
            "cme_historical_holiday_schedule_discovery/1.0.0"
        ),
        "source_capture_id": "a" * 64,
        "source_manifest_path": (
            "manifests/data_releases/reference/" + "b" * 64 + ".json"
        ),
        "source_manifest_sha256": "c" * 64,
        "source_release_id": "b" * 64,
        "source_response_count": 616,
        "status": "OFFLINE_DISCOVERY_COMPLETE",
        "unresolved_holiday_url_token_count": 0,
    }
    return {**core, "assessment_id": sha256_json(core)}


def test_normalizes_only_exact_cme_holiday_files():
    assert _normalized_candidate(
        "http://cmegroup.com/tools-information/holiday-calendar/"
        "files/holiday_schedule_2012.PDF"
    ) == (
        "https://www.cmegroup.com/tools-information/holiday-calendar/"
        "files/holiday_schedule_2012.PDF"
    )
    assert (
        _normalized_candidate(
            "https://www.cmegroup.com/tools-information/holiday-calendar/"
            "files/schedule.xlsx"
        )
        is not None
    )
    assert _normalized_candidate(
        "https://example.com/tools-information/holiday-calendar/files/x.pdf"
    ) is None
    assert _normalized_candidate(
        "https://www.cmegroup.com/tools-information/holiday-calendar/"
        "files/x.pdf?download=1"
    ) is None


def test_extracts_annotation_and_rejoins_known_text_wraps():
    url = (
        "https://www.cmegroup.com/tools-information/holiday-calendar/"
        "files/holiday_schedule_2012.pdf"
    )
    page = _Page(
        annotations=[
            _Reference({"/A": {"/URI": url}}),
            _Reference({"/A": None}),
        ],
        text=(
            "https://www.cmegroup.com/tools-\n"
            "information/holiday-\ncalendar/files/2012-\n"
            "thanksgiving.xls"
        ),
    )
    annotations, text = _page_links(page)
    assert annotations == {url}
    assert (
        "https://www.cmegroup.com/tools-information/holiday-calendar/"
        "files/2012-thanksgiving.xls"
    ) in text


def test_discovery_validator_fails_closed_on_tamper():
    payload = _assessment()
    assert validate_holiday_schedule_discovery(payload) == payload
    tampered = copy.deepcopy(payload)
    tampered["candidate_count"] = 2
    with pytest.raises(IntegrityError):
        validate_holiday_schedule_discovery(tampered)
    tampered = copy.deepcopy(payload)
    tampered["parser_warning_codes"] = ["UNBOUNDED_FREE_FORM"]
    core = dict(tampered)
    core.pop("assessment_id")
    tampered["assessment_id"] = sha256_json(core)
    with pytest.raises(IntegrityError):
        validate_holiday_schedule_discovery(tampered)
