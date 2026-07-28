from __future__ import annotations

import json

import pytest

from futures_rebuild.calendar_archived_cme_gap_discovery import (
    APPROVAL_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    _cdx_url,
    build_plan,
    parse_cdx_payload,
    validate_approval,
    validate_plan,
)
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


def _gaps() -> list[dict[str, object]]:
    return [
        {
            "cme_request_id": f"request-{ordinal}",
            "original_url": (
                "https://www.cmegroup.com/tools-information/"
                f"holiday-calendar/files/missing-{ordinal:02d}.pdf"
            ),
            "source_ordinal": ordinal,
        }
        for ordinal in range(1, 60)
    ]


def _plan() -> dict[str, object]:
    digest = "a" * 64
    return build_plan(
        authority={
            "cme_capture_id": digest,
            "cme_gap_set_id": sha256_json(_gaps()),
            "cme_manifest_path": "manifests/data_releases/reference/a.json",
            "cme_manifest_sha256": digest,
            "cme_release_id": digest,
            "evidence_result_id": digest,
            "evidence_result_path": "reports/exchange_calendar/evidence.json",
            "evidence_result_sha256": digest,
        },
        gaps=_gaps(),
        implementation_sha256={
            path: sha256_json({"path": path}) for path in IMPLEMENTATION_PATHS
        },
    )


def test_plan_binds_only_exact_59_missing_cme_urls():
    plan = _plan()
    assert validate_plan(plan) == plan
    assert plan["scope"]["max_requests"] == 59
    assert plan["scope"]["workers"] == 2
    assert plan["scope"]["allow_redirects"] is False
    assert plan["scope"]["retries"] == 0
    assert len(plan["scope"]["requests"]) == 59
    assert all(
        request["cdx_url"] == _cdx_url(request["original_cme_url"])
        for request in plan["scope"]["requests"]
    )
    tampered = dict(plan)
    tampered["execution_authorized"] = True
    with pytest.raises(IntegrityError):
        validate_plan(tampered)


def test_cdx_parser_accepts_exact_rows_and_rejects_other_original():
    original = _gaps()[0]["original_url"]
    body = json.dumps(
        [
            [
                "timestamp",
                "original",
                "digest",
                "statuscode",
                "mimetype",
                "length",
            ],
            [
                "20140102030405",
                original,
                "ABC123",
                "200",
                "application/pdf",
                "12345",
            ],
        ]
    ).encode()
    rows = parse_cdx_payload(body=body, original_url=original)
    assert rows[0]["timestamp"] == "20140102030405"
    assert parse_cdx_payload(body=b"[]", original_url=original) == []
    wrong = json.loads(body)
    wrong[1][1] = "https://example.com/not-cme.pdf"
    with pytest.raises(IntegrityError):
        parse_cdx_payload(
            body=json.dumps(wrong).encode(),
            original_url=original,
        )


def test_cdx_parser_accepts_only_default_port_cme_scheme_equivalence():
    requested = _gaps()[0]["original_url"]
    path = requested.removeprefix("https://www.cmegroup.com")

    def body(observed: str) -> bytes:
        return json.dumps(
            [
                list(
                    (
                        "timestamp",
                        "original",
                        "digest",
                        "statuscode",
                        "mimetype",
                        "length",
                    )
                ),
                [
                    "20100602005637",
                    observed,
                    "7DXFJGF2NESYHYQDG6BPFM5SIBPBS3ZU",
                    "200",
                    "application/pdf",
                    "37207",
                ],
            ]
        ).encode()

    accepted = parse_cdx_payload(
        body=body(f"http://www.cmegroup.com:80{path}"),
        original_url=requested,
    )
    assert accepted[0]["original"] == f"http://www.cmegroup.com:80{path}"

    for observed in (
        f"http://www.cmegroup.com:81{path}",
        f"http://archive.cmegroup.com:80{path}",
        f"http://user@www.cmegroup.com:80{path}",
        f"http://www.cmegroup.com:80{path}?changed=1",
        f"http://www.cmegroup.com:80{path}-changed",
    ):
        with pytest.raises(IntegrityError):
            parse_cdx_payload(
                body=body(observed),
                original_url=requested,
            )


def test_approval_must_match_exact_plan_and_hash():
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
