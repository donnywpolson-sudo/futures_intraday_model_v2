from __future__ import annotations

import pytest

from futures_rebuild.calendar_archived_cme_gap_diagnostic import (
    APPROVAL_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    build_plan,
    validate_approval,
    validate_plan,
)
from futures_rebuild.calendar_archived_cme_gap_discovery import _cdx_url
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


def _plan() -> dict[str, object]:
    digest = "a" * 64
    original = (
        "https://www.cmegroup.com/tools-information/"
        "holiday-calendar/files/2010-4th-of-july.pdf"
    )
    return build_plan(
        authority={
            "failure_assessment_path": (
                "reports/exchange_calendar/failure.json"
            ),
            "failure_assessment_sha256": digest,
            "predecessor_approval_receipt_id": digest,
            "predecessor_plan_id": digest,
            "predecessor_plan_path": (
                "reports/exchange_calendar/predecessor.json"
            ),
            "predecessor_plan_sha256": digest,
        },
        request={
            "cdx_url": _cdx_url(original),
            "original_cme_url": original,
            "request_id": "archived-cme-gap-0001-5482bf9a6205",
        },
        implementation_sha256={
            path: sha256_json({"path": path}) for path in IMPLEMENTATION_PATHS
        },
    )


def test_diagnostic_plan_is_single_request_raw_only():
    plan = _plan()
    assert validate_plan(plan) == plan
    assert plan["scope"]["max_requests"] == 1
    assert plan["scope"]["workers"] == 1
    assert plan["scope"]["retries"] == 0
    assert "PARSE_OR_NORMALIZE_CDX_ROWS" in plan["scope"]["forbidden_actions"]
    assert (
        plan["scope"]["request"]["request_id"]
        == "archived-cme-gap-raw-diagnostic-0001"
    )
    tampered = dict(plan)
    tampered["execution_authorized"] = True
    with pytest.raises(IntegrityError):
        validate_plan(tampered)


def test_diagnostic_approval_must_match_exact_plan_and_hash():
    plan = _plan()
    plan_sha256 = sha256_json({"plan": "file"})
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
        validate_approval(
            approval=approval,
            plan=plan,
            plan_sha256=plan_sha256,
        )
        == approval
    )
    wrong = dict(approval)
    wrong["plan_sha256"] = "d" * 64
    with pytest.raises(UnauthorizedOperation):
        validate_approval(
            approval=wrong,
            plan=plan,
            plan_sha256=plan_sha256,
        )
