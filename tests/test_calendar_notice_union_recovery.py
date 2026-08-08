import hashlib
import json
import shutil
from email.message import Message
from pathlib import Path

import pytest

import futures_rebuild.calendar_notice_union as union_module
import futures_rebuild.calendar_notice_union_recovery as recovery_module
from futures_rebuild.calendar_notice_union import build_union_plan
from futures_rebuild.calendar_notice_union_recovery import (
    APPROVAL_SCHEMA,
    CAPTURE_SCHEMA,
    IMPLEMENTATION_PATHS,
    OPERATION,
    NoticeUnionRecoveryError,
    build_recovery_plan,
    capture_recovery_union,
    implementation_hashes,
    load_recovery_union_capture,
    recovery_authority,
    validate_recovery_plan,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.data_layout import PhasePublisher
from futures_rebuild.errors import IntegrityError


REPO = Path(__file__).resolve().parents[1]


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _copy_file(active: Path, relative: str) -> None:
    target = active / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(REPO / relative, target)


def _publisher(boundary, operation_factory) -> PhasePublisher:
    return PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root
        / "state"
        / "locks"
        / "data-publication.lock",
    )


def _synthetic_authority() -> dict[str, object]:
    return {
        "assessment_id": "a" * 64,
        "assessment_path": "reports/exchange_calendar/assessment.json",
        "assessment_sha256": "b" * 64,
        "index_id": "c" * 64,
        "index_path": "reports/exchange_calendar/index.json",
        "index_sha256": "d" * 64,
        "pagination_release_id": "e" * 64,
        "probe_capture_id": "f" * 64,
        "probe_manifest_path": (
            "manifests/data_releases/reference/probe.json"
        ),
        "probe_manifest_sha256": "1" * 64,
        "probe_release_id": "2" * 64,
    }


def _synthetic_candidates(count: int) -> list[dict[str, object]]:
    return [
        {
            "queries": ["holiday"],
            "relative_url": f"/notices/test/{ordinal:04d}.html",
            "title": f"Notice {ordinal:04d}",
            "url": (
                "https://www.cmegroup.com/notices/test/"
                f"{ordinal:04d}.html"
            ),
        }
        for ordinal in range(1, count + 1)
    ]


def _bind_small_counts(monkeypatch) -> None:
    monkeypatch.setattr(union_module, "MAX_REQUESTS", 3)
    monkeypatch.setattr(recovery_module, "TOTAL_REQUESTS", 3)
    monkeypatch.setattr(recovery_module, "REUSED_REQUESTS", 2)
    monkeypatch.setattr(recovery_module, "NETWORK_REQUESTS", 1)
    monkeypatch.setattr(
        recovery_module,
        "MAX_NETWORK_BYTES",
        recovery_module.MAX_RESPONSE_BYTES,
    )
    monkeypatch.setattr(
        recovery_module,
        "MAX_TOTAL_BYTES",
        3 * recovery_module.MAX_RESPONSE_BYTES,
    )


def _predecessor_evidence(boundary, monkeypatch):
    _bind_small_counts(monkeypatch)
    candidates = _synthetic_candidates(3)
    predecessor = build_union_plan(
        authority=_synthetic_authority(),
        candidates=candidates,
        implementation_sha256={
            relative: "f" * 64
            for relative in union_module.IMPLEMENTATION_PATHS
        },
    )
    predecessor_path = (
        boundary.active_root / "reports" / "predecessor-plan.json"
    )
    predecessor_path.parent.mkdir(parents=True)
    predecessor_path.write_bytes(canonical_bytes(predecessor) + b"\n")
    stage = (
        boundary.active_root
        / "state"
        / "data_publication_staging"
        / "predecessor"
    )
    stage.mkdir(parents=True)
    requests = predecessor["scope"]["requests"]
    responses = []
    for request in requests[:2]:
        body = f"<html>{request['url']}</html>\n".encode()
        name = f"{request['request_id']}.html"
        target = stage / name
        target.write_bytes(body)
        responses.append(
            {
                "content_type": "text/html",
                "logical_path": (
                    f"data/reference/exchange_calendars/{name}"
                ),
                "matched_queries": request["matched_queries"],
                "metadata_title": request["metadata_title"],
                "ordinal": request["ordinal"],
                "received_at_utc": "2026-07-27T14:00:00Z",
                "request_id": request["request_id"],
                "request_kind": "HISTORICAL_NOTICE_DOCUMENT_UNION",
                "safe_headers": {"content-type": "text/html"},
                "sha256": _sha256_bytes(body),
                "size": len(body),
                "status_code": 200,
                "url": request["url"],
            }
        )
    failure_core = {
        "approval_receipt_id": "9" * 64,
        "elapsed_milliseconds": 5_403_000,
        "failed_requests": [
            {
                "error_class": "DURATION_CEILING_REACHED",
                "request_id": requests[2]["request_id"],
            }
        ],
        "network_requests_attempted": 2,
        "plan_id": predecessor["plan_id"],
        "plan_sha256": _sha256_bytes(predecessor_path.read_bytes()),
        "publication_occurred": False,
        "responses_preserved": responses,
        "responses_preserved_count": 2,
        "retries_performed": 0,
        "schema_version": union_module.FAILURE_SCHEMA,
        "stage_relative_path": (
            "state/data_publication_staging/predecessor"
        ),
        "status": "STOPPED",
    }
    failure = {
        **failure_core,
        "failure_id": sha256_json(failure_core),
    }
    failure_path = (
        boundary.active_root / "reports" / "predecessor-failure.json"
    )
    failure_path.write_bytes(canonical_bytes(failure) + b"\n")
    return predecessor_path, failure_path, stage


def _copy_implementation_closure(active: Path) -> None:
    for relative in IMPLEMENTATION_PATHS:
        _copy_file(active, relative)


def _approval(plan: dict[str, object], plan_sha256: str):
    core = {
        "approved_at": "2026-07-27T16:30:00Z",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "8" * 64,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def _plan(boundary, predecessor_path, failure_path):
    authority, _predecessor, _failure, remaining = recovery_authority(
        predecessor_plan_path=predecessor_path,
        failure_report_path=failure_path,
        boundary=boundary,
    )
    _copy_implementation_closure(boundary.active_root)
    plan = build_recovery_plan(
        authority=authority,
        remaining_requests=remaining,
        implementation_sha256=implementation_hashes(
            boundary.active_root
        ),
    )
    assert validate_recovery_plan(plan) == plan
    plan_path = boundary.active_root / "reports" / "recovery-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    return plan, plan_path


def test_recovery_reuses_exact_stage_and_requests_only_remainder(
    boundary, operation_factory, monkeypatch
) -> None:
    predecessor_path, failure_path, _stage = _predecessor_evidence(
        boundary, monkeypatch
    )
    plan, plan_path = _plan(
        boundary, predecessor_path, failure_path
    )
    approval = _approval(plan, _sha256_bytes(plan_path.read_bytes()))
    approval_path = boundary.active_root / "configs" / "approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    opened: list[str] = []

    class FakeResponse:
        status = 200

        def __init__(self, url: str) -> None:
            self.url = url
            self.headers = Message()
            self.headers["Content-Type"] = "text/html; charset=UTF-8"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return self.url

        def read(self, maximum):
            assert maximum == 1_048_577
            return f"<html>{self.url}</html>\n".encode()

    class FakeOpener:
        def open(self, request, timeout):
            assert timeout == 30
            opened.append(request.full_url)
            return FakeResponse(request.full_url)

    monkeypatch.setattr(
        recovery_module.urllib.request,
        "build_opener",
        lambda *_args: FakeOpener(),
    )
    receipt = capture_recovery_union(
        plan_path=plan_path,
        approval_path=approval_path,
        publisher=_publisher(boundary, operation_factory),
    )
    monkeypatch.setattr(
        type(receipt),
        "resolve_file",
        lambda *_args, **_kwargs: pytest.fail(
            "loader repeated full manifest verification through resolve_file"
        ),
    )
    monkeypatch.setattr(
        type(receipt),
        "embedded_document",
        lambda *_args, **_kwargs: pytest.fail(
            "loader repeated full manifest verification for embedded receipt"
        ),
    )
    monkeypatch.setattr(
        type(receipt),
        "verify",
        lambda *_args, **_kwargs: pytest.fail(
            "loader used the sequential full-manifest verifier"
        ),
    )
    result = load_recovery_union_capture(receipt, boundary=boundary)
    assert opened == [plan["scope"]["requests"][0]["url"]]
    assert result["reused_response_count"] == 2
    assert result["network_request_count"] == 1
    assert result["schema_version"] == CAPTURE_SCHEMA
    assert [
        item["acquisition"] for item in result["responses"]
    ] == [
        "REUSED_HASH_VERIFIED_STAGE",
        "REUSED_HASH_VERIFIED_STAGE",
        "NETWORK",
    ]


def test_recovery_rejects_bad_approval_before_network(
    boundary, operation_factory, monkeypatch
) -> None:
    predecessor_path, failure_path, _stage = _predecessor_evidence(
        boundary, monkeypatch
    )
    plan, plan_path = _plan(
        boundary, predecessor_path, failure_path
    )
    approval = _approval(plan, "0" * 64)
    approval_path = boundary.active_root / "configs" / "approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    monkeypatch.setattr(
        recovery_module.urllib.request,
        "build_opener",
        lambda *_args: pytest.fail("network opened before approval"),
    )
    with pytest.raises(
        NoticeUnionRecoveryError, match="exact hash-bound approval"
    ):
        capture_recovery_union(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=_publisher(boundary, operation_factory),
        )


def test_recovery_rejects_reused_byte_drift_before_network(
    boundary, operation_factory, monkeypatch
) -> None:
    predecessor_path, failure_path, stage = _predecessor_evidence(
        boundary, monkeypatch
    )
    plan, plan_path = _plan(
        boundary, predecessor_path, failure_path
    )
    approval = _approval(plan, _sha256_bytes(plan_path.read_bytes()))
    approval_path = boundary.active_root / "configs" / "approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    first = sorted(stage.glob("*.html"))[0]
    first.write_bytes(first.read_bytes() + b"drift")
    monkeypatch.setattr(
        recovery_module.urllib.request,
        "build_opener",
        lambda *_args: pytest.fail("network opened after reuse drift"),
    )
    with pytest.raises(
        IntegrityError, match="response bytes changed"
    ):
        capture_recovery_union(
            plan_path=plan_path,
            approval_path=approval_path,
            publisher=_publisher(boundary, operation_factory),
        )
