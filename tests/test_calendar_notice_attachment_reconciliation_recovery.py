import hashlib
from pathlib import Path

import pytest

import futures_rebuild.calendar_notice_attachment_reconciliation_recovery as module
from futures_rebuild.calendar_notice_attachment_capture import (
    NoticeAttachmentRequestError,
)
from futures_rebuild.calendar_notice_attachment_reconciliation_recovery import (
    APPROVAL_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    build_interruption_evidence,
    build_recovery_plan,
    capture_reconciliation_recovery,
    load_reconciliation_recovery_capture,
    validate_interruption_evidence,
    validate_recovery_approval,
    validate_recovery_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.data_layout import PhasePublisher
from futures_rebuild.errors import IntegrityError


def _request(ordinal: int) -> dict[str, object]:
    request_id = f"attachment-{ordinal:04d}"
    return {
        "accept": "application/octet-stream, application/pdf",
        "discovery_reasons": ["NOTICE_PATH"],
        "expected_content_types": [
            "application/octet-stream",
            "application/pdf",
        ],
        "extension": ".pdf",
        "link_texts": ["Full text"],
        "logical_path": (
            "data/reference/exchange_calendars/"
            f"{request_id}.pdf"
        ),
        "ordinal": ordinal,
        "request_id": request_id,
        "request_kind": "HISTORICAL_NOTICE_ATTACHMENT_CAPTURE",
        "source_notice_request_ids": [f"notice-{ordinal:04d}"],
        "source_notice_urls": [
            "https://www.cmegroup.com/notices/test/"
            f"notice-{ordinal:04d}.html"
        ],
        "source_titles": [f"Notice {ordinal:04d}"],
        "url": (
            "https://www.cmegroup.com/notices/test/"
            f"attachment-{ordinal:04d}.pdf"
        ),
    }


def _exclusion(ordinal: int) -> dict[str, object]:
    request = _request(ordinal)
    return {
        "classification": {
            "error_class": "NoticeAttachmentRequestError",
            "failure_code": "HTTP_STATUS_REJECTED",
            "safe_details": {
                "content_type": "text/html",
                "http_status": 404,
            },
        },
        "evidence_id": ("a" if ordinal == 14 else "b") * 64,
        "evidence_kind": (
            "DIAGNOSTIC_RESULT"
            if ordinal == 14
            else "RECOVERY_FAILURE"
        ),
        "evidence_path": f"reports/evidence-{ordinal}.json",
        "evidence_sha256": f"{ordinal - 10}" * 64,
        "ordinal": ordinal,
        "request_id": request["request_id"],
        "status": "EXCLUDED_AUTHORITATIVE_HTTP_404",
        "url": request["url"],
    }


def _possible() -> list[dict[str, object]]:
    return [
        {
            "ordinal": ordinal,
            "prior_attempt_status": "POSSIBLY_IN_FLIGHT_NOT_ESTABLISHED",
            "request_id": _request(ordinal)["request_id"],
            "url": _request(ordinal)["url"],
        }
        for ordinal in (57, 58)
    ]


def _authority(
    *,
    exclusions: list[dict[str, object]],
    possible: list[dict[str, object]],
    preserved_total_bytes: int,
) -> dict[str, object]:
    return {
        "completed_network_first_ordinal": 17,
        "completed_network_last_ordinal": 56,
        "completed_network_response_count": 40,
        "first_remaining_request_id": _request(57)["request_id"],
        "interruption_id": "1" * 64,
        "interruption_path": "reports/interruption.json",
        "interruption_sha256": "2" * 64,
        "known_exclusion_count": 2,
        "known_exclusion_set_id": sha256_json(exclusions),
        "last_remaining_request_id": _request(797)["request_id"],
        "network_attempt_count_lower_bound": 40,
        "network_attempt_count_upper_bound": 42,
        "plan_id": "3" * 64,
        "plan_sha256": "4" * 64,
        "possibly_in_flight_request_count": 2,
        "possibly_in_flight_request_set_id": sha256_json(possible),
        "preserved_response_count": 54,
        "preserved_response_set_id": "5" * 64,
        "preserved_stage_file_set_id": "6" * 64,
        "preserved_stage_relative_path": (
            "state/data_publication_staging/interrupted"
        ),
        "preserved_total_bytes": preserved_total_bytes,
        "remaining_request_count": 741,
        "source_approval_receipt_id": "7" * 64,
        "source_approval_sha256": "8" * 64,
        "source_union_release_id": "9" * 64,
    }


def _implementation() -> dict[str, str]:
    return {relative: "a" * 64 for relative in IMPLEMENTATION_PATHS}


def _descriptors(stage: Path) -> tuple[list[dict[str, object]], int]:
    descriptors: list[dict[str, object]] = []
    total = 0
    ordinals = (*range(1, 14), 16, *range(17, 57))
    for ordinal in ordinals:
        request = _request(ordinal)
        body = f"%PDF-preserved-{ordinal}\n".encode("ascii")
        name = Path(str(request["logical_path"])).name
        (stage / name).write_bytes(body)
        total += len(body)
        descriptors.append(
            {
                "acquisition": "INTERRUPTED_STAGE_HASH_VERIFIED",
                "discovery_reasons": request["discovery_reasons"],
                "extension": ".pdf",
                "logical_path": request["logical_path"],
                "ordinal": ordinal,
                "payload_signature": "PDF_SIGNATURE_VERIFIED",
                "request_id": request["request_id"],
                "request_kind": request["request_kind"],
                "response_metadata_status": (
                    "TRANSPORT_METADATA_NOT_PRESERVED_BY_WRAPPER_"
                    "INTERRUPTION"
                ),
                "sha256": hashlib.sha256(body).hexdigest(),
                "size": len(body),
                "source_notice_request_ids": request[
                    "source_notice_request_ids"
                ],
                "source_titles": request["source_titles"],
                "url": request["url"],
                "writer_contract_status": (
                    "PAYLOAD_WRITTEN_ONLY_AFTER_ACCEPTED_HTTP_200_"
                    "CONTENT_TYPE_AND_URL"
                ),
            }
        )
    return descriptors, total


def test_interruption_builder_binds_exact_stage_and_attempt_uncertainty(
    boundary,
    monkeypatch,
) -> None:
    plan_path = boundary.active_root / "reports/exchange_calendar/plan.json"
    approval_path = boundary.active_root / "configs/approval.json"
    stage = (
        boundary.active_root
        / "state/data_publication_staging/interrupted"
    )
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    approval_path.parent.mkdir(parents=True, exist_ok=True)
    stage.mkdir(parents=True, exist_ok=True)
    plan_path.write_bytes(canonical_bytes({"plan": True}) + b"\n")
    approval_path.write_bytes(canonical_bytes({"approval": True}) + b"\n")
    descriptors, _total = _descriptors(stage)
    requests = [_request(ordinal) for ordinal in range(17, 798)]
    exclusions = [_exclusion(14), _exclusion(15)]
    plan = {
        "plan_id": "3" * 64,
        "scope": {
            "authority": {"source_union_release_id": "9" * 64}
        },
    }
    approval = {"approval_receipt_id": "7" * 64}
    reused = [
        descriptor
        for descriptor in descriptors
        if descriptor["ordinal"] in {*range(1, 14), 16}
    ]
    monkeypatch.setattr(
        module,
        "_source_context",
        lambda **_kwargs: (
            plan,
            approval,
            requests,
            reused,
            exclusions,
        ),
    )
    monkeypatch.setattr(
        module,
        "_original_requests",
        lambda *_args, **_kwargs: [
            _request(ordinal) for ordinal in range(1, 798)
        ],
    )
    monkeypatch.setattr(
        module,
        "predecessor_failure_path",
        lambda *_args: boundary.active_root / "absent-failure.json",
    )
    monkeypatch.setattr(
        module,
        "predecessor_release_for_plan",
        lambda *_args: None,
    )

    evidence = build_interruption_evidence(
        plan_path=plan_path,
        approval_path=approval_path,
        stage_path=stage,
        observed_at_utc="2026-07-27T21:42:00Z",
        wrapper_exit_code=124,
        wrapper_timeout_seconds=10,
        boundary=boundary,
    )

    assert validate_interruption_evidence(evidence) == evidence
    assert evidence["preserved_response_count"] == 54
    assert evidence["completed_network_response_count"] == 40
    assert evidence["network_attempt_count_lower_bound"] == 40
    assert evidence["network_attempt_count_upper_bound"] == 42
    assert [
        item["ordinal"]
        for item in evidence["possibly_in_flight_requests"]
    ] == [57, 58]

    tampered = dict(evidence)
    tampered["preserved_total_bytes"] += 1
    with pytest.raises(IntegrityError):
        validate_interruption_evidence(tampered)


def test_recovery_plan_binds_only_ordinals_57_through_797() -> None:
    exclusions = [_exclusion(14), _exclusion(15)]
    possible = _possible()
    requests = [_request(ordinal) for ordinal in range(57, 798)]
    plan = build_recovery_plan(
        authority=_authority(
            exclusions=exclusions,
            possible=possible,
            preserved_total_bytes=9_818_402,
        ),
        remaining_requests=requests,
        known_exclusions=exclusions,
        possibly_in_flight_requests=possible,
        implementation_sha256=_implementation(),
    )

    assert validate_recovery_plan(plan) == plan
    assert plan["execution_authorized"] is False
    assert plan["scope"]["max_network_requests"] == 741
    assert plan["scope"]["requests"][0]["ordinal"] == 57
    assert plan["scope"]["requests"][-1]["ordinal"] == 797
    assert [
        item["ordinal"]
        for item in plan["scope"]["possibly_repeated_requests"]
    ] == [57, 58]
    requested_ordinals = {
        item["ordinal"] for item in plan["scope"]["requests"]
    }
    assert requested_ordinals == set(range(57, 798))
    assert not requested_ordinals & set(range(1, 57))


def _capture_setup(
    boundary,
    operation_factory,
    monkeypatch,
):
    source_stage = (
        boundary.active_root
        / "state/data_publication_staging/interrupted"
    )
    source_stage.mkdir(parents=True, exist_ok=True)
    descriptors, total = _descriptors(source_stage)
    exclusions = [_exclusion(14), _exclusion(15)]
    possible = _possible()
    authority = _authority(
        exclusions=exclusions,
        possible=possible,
        preserved_total_bytes=total,
    )
    requests = [_request(ordinal) for ordinal in range(57, 798)]
    implementation = _implementation()
    plan = build_recovery_plan(
        authority=authority,
        remaining_requests=requests,
        known_exclusions=exclusions,
        possibly_in_flight_requests=possible,
        implementation_sha256=implementation,
    )
    plan_path = boundary.active_root / "reports/plan.json"
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    approval_core = {
        "approved_at": "2026-07-27T22:00:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": hashlib.sha256(
            plan_path.read_bytes()
        ).hexdigest(),
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "b" * 64,
    }
    approval = {
        **approval_core,
        "approval_receipt_id": sha256_json(approval_core),
    }
    approval_path = boundary.active_root / "configs/approval.json"
    approval_path.parent.mkdir(parents=True, exist_ok=True)
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    monkeypatch.setattr(
        module, "implementation_hashes", lambda _root: implementation
    )
    monkeypatch.setattr(
        module,
        "recovery_authority",
        lambda **_kwargs: (
            authority,
            requests,
            descriptors,
            exclusions,
            possible,
        ),
    )
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=(
            boundary.active_root / "state/locks/data-publication.lock"
        ),
    )
    return (
        plan,
        plan_path,
        approval,
        approval_path,
        publisher,
    )


def test_recovery_capture_continues_on_404_and_publishes_complete_union(
    boundary,
    operation_factory,
    monkeypatch,
) -> None:
    (
        plan,
        plan_path,
        approval,
        approval_path,
        publisher,
    ) = _capture_setup(
        boundary,
        operation_factory,
        monkeypatch,
    )

    def fake_fetch(spec, **_kwargs):
        if spec["ordinal"] == 57:
            raise NoticeAttachmentRequestError(
                "not found",
                failure_code="HTTP_STATUS_REJECTED",
                safe_details={
                    "content_type": "text/html",
                    "http_status": 404,
                },
            )
        return (
            f"%PDF-network-{spec['ordinal']}\n".encode("ascii"),
            "application/pdf",
            {"content-type": "application/pdf"},
            "2026-07-27T22:01:00Z",
        )

    monkeypatch.setattr(module, "fetch_attachment", fake_fetch)

    assert validate_recovery_approval(
        approval,
        plan=plan,
        plan_sha256=hashlib.sha256(plan_path.read_bytes()).hexdigest(),
    ) == approval["approval_receipt_id"]
    receipt = capture_reconciliation_recovery(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=publisher,
    )
    capture = load_reconciliation_recovery_capture(
        receipt,
        boundary=boundary,
    )

    assert capture["network_request_count"] == 741
    assert capture["reused_response_count"] == 54
    assert capture["possibly_repeated_request_count"] == 2
    assert capture["resolved_candidate_count"] == 797
    assert capture["unresolved_candidate_count"] == 0
    assert {item["ordinal"] for item in capture["exclusions"]} == {
        14,
        15,
        57,
    }


def test_recovery_capture_fails_closed_on_non_404(
    boundary,
    operation_factory,
    monkeypatch,
) -> None:
    (
        _plan,
        plan_path,
        _approval,
        approval_path,
        publisher,
    ) = _capture_setup(
        boundary,
        operation_factory,
        monkeypatch,
    )

    def fake_fetch(spec, **_kwargs):
        raise NoticeAttachmentRequestError(
            "server error",
            failure_code="HTTP_STATUS_REJECTED",
            safe_details={
                "content_type": "text/html",
                "http_status": 500,
            },
        )

    monkeypatch.setattr(module, "fetch_attachment", fake_fetch)

    with pytest.raises(
        module.NoticeAttachmentReconciliationRecoveryError
    ):
        capture_reconciliation_recovery(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=publisher,
        )

    failures = list(
        (boundary.active_root / "reports/exchange_calendar").glob(
            "cme_historical_notice_attachment_reconciliation_"
            "recovery_failure_*.json"
        )
    )
    assert len(failures) == 1
    assert b'"http_status":500' in failures[0].read_bytes()
