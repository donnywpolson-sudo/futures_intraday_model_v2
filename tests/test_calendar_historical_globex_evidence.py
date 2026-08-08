from __future__ import annotations

from datetime import date

import pytest

from futures_rebuild.calendar_historical_globex_evidence import (
    APPROVAL_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    _classify_captured_pdf,
    build_extraction_plan,
    candidate_passages,
    validate_extraction_approval,
    validate_extraction_plan,
)
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


def _source(name: str, count: int) -> dict[str, object]:
    digest = sha256_json({"source": name})
    return {
        "capture_id": digest,
        "file_descriptor_set_id": digest,
        "manifest_path": f"manifests/data_releases/reference/{digest}.json",
        "manifest_sha256": digest,
        "release_id": digest,
        "release_kind": name,
        "response_count": count,
        "schema_version": f"{name}/1.0.0",
    }


def _plan() -> dict[str, object]:
    digest = "a" * 64
    return build_extraction_plan(
        source_authority={
            "capture_assessment_id": digest,
            "capture_assessment_path": (
                "reports/exchange_calendar/assessment.json"
            ),
            "capture_assessment_sha256": digest,
            "holiday_schedule_source": _source("holiday", 5),
            "notice_attachment_source": _source("notice", 616),
        },
        implementation_sha256={
            path: sha256_json({"path": path}) for path in IMPLEMENTATION_PATHS
        },
        required_coverage_start_trade_date=date(2010, 6, 6),
        required_coverage_end_trade_date=date(2026, 7, 13),
    )


def test_candidate_passages_require_globex_date_time_and_schedule_verb():
    text = (
        "Please note the holiday processing schedule for Monday, "
        "January 21, 2013. CME GLOBEX will open on Sunday January 20, "
        "2013 at 5 pm CST, close at 12:15 pm on Monday, January 21, "
        "2013, and re-open at 5 pm CST."
    )
    result = candidate_passages(text)
    assert len(result) == 1
    assert result[0]["year_hints"] == [2013]
    assert result[0]["passage_sha256"] == sha256_json(
        {"passage": result[0]["passage"]}
    )
    assert candidate_passages(
        "CME Globex holiday hours are available at the linked page."
    ) == []
    assert candidate_passages(
        "The market opens at 5 pm CST on Monday, January 21, 2013."
    ) == []
    assert (
        _classify_captured_pdf(
            "CME Globex and CME ClearPort trading details are linked. "
            "Equities are open for an abbreviated session on April 2, "
            "2021. Clearing confirmation runs at 7:30 am."
        )
        == (
            "CLEARING_ADVISORY_NOT_ACCEPTED_AS_EXACT_41_PRODUCT_"
            "GLOBEX_AUTHORITY"
        )
    )


def test_plan_is_hash_bound_fail_closed_and_network_free():
    plan = _plan()
    assert validate_extraction_plan(plan) == plan
    assert plan["scope"]["network_request_limit"] == 0
    assert plan["scope"]["max_source_files"] == 621
    assert plan["scope"]["max_pdf_pages"] == 2048
    assert plan["execution_authorized"] is False

    tampered = dict(plan)
    tampered["execution_authorized"] = True
    with pytest.raises(IntegrityError):
        validate_extraction_plan(tampered)


def test_approval_must_match_exact_plan_and_file_hash():
    plan = _plan()
    plan_sha256 = sha256_json({"file": "plan"})
    approval = {
        "approval_receipt_id": "b" * 64,
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "c" * 64,
    }
    assert (
        validate_extraction_approval(
            approval=approval,
            plan=plan,
            plan_sha256=plan_sha256,
        )
        == approval
    )
    wrong = dict(approval)
    wrong["plan_sha256"] = "d" * 64
    with pytest.raises(UnauthorizedOperation):
        validate_extraction_approval(
            approval=wrong,
            plan=plan,
            plan_sha256=plan_sha256,
        )
