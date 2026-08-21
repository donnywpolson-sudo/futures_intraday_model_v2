from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild import ohlcv_completion_campaign as campaign
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
    _personal_approval_line,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.ohlcv_historical_backfill_v3 import BATCH_SCHEMA, END_EXCLUSIVE


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(value) + b"\n")


def _batch(root: Path) -> tuple[Path, dict[str, object], str]:
    request = {
        "compression": "zstd",
        "dataset": "GLBX.MDP3",
        "encoding": "dbn",
        "end": END_EXCLUSIVE,
        "map_symbols": False,
        "market": "MSF",
        "schema": "ohlcv-1h",
        "split_duration": "year",
        "split_symbols": False,
        "start": "2025-01-01T00:00:00Z",
        "stype_in": "continuous",
        "stype_out": "instrument_id",
        "symbols": ["MSF.v.0"],
    }
    core: dict[str, object] = {
        "authority": {
            "active_data_mutation": False,
            "credential_access": False,
            "provider_network_access": False,
            "publication": False,
            "status": "PREPARED_REQUIRES_SEPARATE_QUOTE_ACQUISITION_AND_PUBLICATION_APPROVALS",
        },
        "bindings": {},
        "canaries": ["MSF"],
        "end_exclusive": END_EXCLUSIVE,
        "estimates": {"current_free_bytes": 10**9},
        "execution_limits": {
            "provider_cost_cap_usd": "0.0",
            "provider_request_count": 1,
            "target_dbn_file_count_maximum": 2,
        },
        "execution_policy": {
            "automatic_continuation_after_canaries": False,
            "canary_markets": ["MSF"],
            "expired_job_replacement_authorized": False,
        },
        "intervals": [{
            "end_exclusive": END_EXCLUSIVE,
            "estimated_final_bytes_high": 1000,
            "market": "MSF",
            "schemas": ["ohlcv-1h"],
            "start_inclusive": "2025-01-01T00:00:00Z",
        }],
        "parent_plan_id": "a" * 64,
        "provider": "Databento",
        "publication": {"mode": "PLAIN_FILE_ADDITIVE_ABSENT_MARKET_DIRECTORIES_THEN_POINTER_SUCCESSOR"},
        "requests": [request],
        "schema_version": BATCH_SCHEMA,
        "selection": {"MSF": ["ohlcv-1h"]},
        "universe": {"root_count": 58},
    }
    document = {**core, "plan_id": sha256_json(core)}
    path = root / "reports/ohlcv_58_completion/test/batch.json"
    _write(path, document)
    return path, document, sha256_file(path)


def test_quote_gate_rejects_local_receipt_before_provider_access(tmp_path: Path) -> None:
    batch_path, _, digest = _batch(tmp_path)
    local = OperationReceipt.issue_local(
        RepoBoundary(tmp_path),
        operation=campaign.QUOTE_OPERATION,
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={},
    )

    with pytest.raises(UnauthorizedOperation, match="classification"):
        campaign.quote_authorized(
            tmp_path,
            batch_path=batch_path,
            batch_sha256=digest,
            output_path=Path("reports/ohlcv_58_completion/test/quote.json"),
            authorization=local,
            provider_factory=lambda _: (_ for _ in ()).throw(AssertionError("provider touched")),
        )


def test_prepare_manifest_binds_storage_and_record_quote(tmp_path: Path) -> None:
    batch_path, batch, digest = _batch(tmp_path)
    quote = {
        "estimated_data_cost_usd": "0.0",
        "plan_id": batch["plan_id"],
        "plan_sha256": digest,
        "quotes": [{
            "api_billable_uncompressed_bytes": 123,
            "estimated_data_cost_usd": "0.0",
            "market": "MSF",
            "provider_record_count": 45,
            "schema": "ohlcv-1h",
        }],
        "status": "PASS_WITHIN_APPROVED_ZERO_COST_CAP",
    }
    quote_path = tmp_path / "reports/ohlcv_58_completion/test/quote.json"
    _write(quote_path, quote)
    output = tmp_path / "reports/ohlcv_58_completion/test/manifest.jsonl"

    result = campaign.prepare_execution_manifest(
        tmp_path,
        batch_path=batch_path,
        batch_sha256=digest,
        quote_path=quote_path,
        output_path=Path("reports/ohlcv_58_completion/test/manifest.jsonl"),
    )

    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert result["selected_targets"] == 2
    assert {row["schema"] for row in rows} == {"ohlcv-1h"}
    assert {row["year"] for row in rows} == {2025, 2026}
    assert all(row["execution_action"] == "DOWNLOAD_VALIDATE_INSTALL_ABSENT_TARGET_ONLY" for row in rows)


def test_authorized_batch_uses_project_root_for_credential_loader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    batch_path, batch, batch_sha = _batch(tmp_path)
    quote = {
        "estimated_data_cost_usd": "0.0",
        "plan_id": batch["plan_id"],
        "plan_sha256": batch_sha,
        "status": "PASS_WITHIN_APPROVED_ZERO_COST_CAP",
    }
    quote_path = tmp_path / "reports/ohlcv_58_completion/test/quote.json"
    _write(quote_path, quote)
    manifest_path = tmp_path / "reports/ohlcv_58_completion/test/manifest.jsonl"
    manifest_path.write_bytes(b"{}\n")
    manifest_sha = sha256_file(manifest_path)
    state_root = Path("state/ohlcv_58_completion") / str(batch["plan_id"])
    required_reuse_job_id = "GLBX-20260821-TESTJOB01"
    for relative in (
        "src/futures_rebuild/ohlcv_completion_campaign.py",
        "src/futures_rebuild/ohlcv_historical_backfill.py",
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(relative.encode("ascii"))

    full_scope = campaign.required_acquisition_scope(
        tmp_path,
        batch,
        batch_sha,
        sha256_file(quote_path),
        manifest_sha,
        state_root,
        required_reuse_job_id,
    )
    scope = {
        key: value
        for key, value in full_scope.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    approval_line = _personal_approval_line(
        campaign.ACQUISITION_OPERATION, str(batch["plan_id"]), batch_sha
    )
    receipt = OperationReceipt.issue_user_approved(
        RepoBoundary(tmp_path),
        operation=campaign.ACQUISITION_OPERATION,
        classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        scope=scope,
        approval_command=campaign.ACQUISITION_OPERATION,
        approval_plan_id=str(batch["plan_id"]),
        approval_plan_sha256=batch_sha,
        approval_line=approval_line,
    )
    provider_roots: list[Path] = []
    execution_roots: list[Path] = []
    sentinel = object()

    def provider_factory(provider_root: Path) -> object:
        provider_roots.append(provider_root)
        return sentinel

    def fake_execute_manifest(**kwargs: object) -> dict[str, int]:
        execution_roots.append(kwargs["root"])
        assert kwargs["reuse_job_id"] == required_reuse_job_id
        factory = kwargs["provider_factory"]
        assert callable(factory)
        assert factory(kwargs["root"]) is sentinel
        return {"actions": 1}

    monkeypatch.setattr(campaign, "execute_manifest", fake_execute_manifest)
    result = campaign.execute_authorized_batch(
        tmp_path,
        batch_path=batch_path,
        batch_sha256=batch_sha,
        quote_path=quote_path,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
        state_root=state_root,
        authorization=receipt,
        required_reuse_job_id=required_reuse_job_id,
        provider_factory=provider_factory,
    )

    assert result["status"] == "ACQUISITION_EXECUTED"
    assert provider_roots == [tmp_path.resolve()]
    assert provider_roots[0] != (tmp_path / state_root).resolve()
    if campaign.os.name == "nt":
        assert str(execution_roots[0]).startswith("\\\\?\\")
    else:
        assert execution_roots == [(tmp_path / state_root).resolve()]


def test_build_no_data_manifest_successor_binds_exact_predecessor_and_evidence(tmp_path: Path) -> None:
    target_id = "5" * 64
    predecessor = tmp_path / "reports/ohlcv_58_completion/test/predecessor.jsonl"
    predecessor.parent.mkdir(parents=True)
    predecessor_row = {
        "activation_status": "NOT_ACTIVE_PENDING_EXPLICIT_EXECUTION",
        "current_state": "MISSING",
        "execution_action": "DOWNLOAD_VALIDATE_INSTALL_ABSENT_TARGET_ONLY",
        "expected_incremental_bytes": 10,
        "intended_end_exclusive": "2024-01-01T00:00:00Z",
        "intended_start_inclusive": "2023-01-01T00:00:00Z",
        "manifest_schema": "ohlcv_historical_backfill_manifest/1.0.0",
        "market": "MJY",
        "provider_record_count": None,
        "schema": "ohlcv-1d",
        "target_id": target_id,
    }
    predecessor.write_bytes(canonical_bytes(predecessor_row) + b"\n")
    predecessor_sha = sha256_file(predecessor)
    evidence_path = tmp_path / "reports/ohlcv_58_completion/test/evidence.json"
    evidence = {
        "job": {"cost_usd": "0", "job_id": "GLBX-TEST", "progress": 100, "state": "done"},
        "metadata_probe": {
            "end_exclusive": "2024-01-01T00:00:00Z",
            "error_code": "symbology_invalid_request",
            "error_message": "None of the symbols could be resolved",
            "http_status": 422,
            "start_inclusive": "2023-01-01T00:00:00Z",
        },
        "provider_file_manifest": {"provider_manifest_hash": "6" * 64},
        "request": {
            "compression": "zstd",
            "dataset": "GLBX.MDP3",
            "encoding": "dbn",
            "end": "2026-07-14T00:00:00Z",
            "map_symbols": False,
            "market": "MJY",
            "schema": "ohlcv-1d",
            "split_duration": "year",
            "split_symbols": False,
            "start": "2018-01-01T00:00:00Z",
            "stype_in": "continuous",
            "stype_out": "instrument_id",
            "symbols": ["MJY.v.0"],
        },
        "schema_version": "ohlcv_provider_no_data_evidence/1.0.0",
        "target": {"target_id": target_id},
    }
    _write(evidence_path, evidence)
    evidence_sha = sha256_file(evidence_path)
    output = Path("reports/ohlcv_58_completion/test/successor.jsonl")

    result = campaign.build_no_data_manifest_successor(
        tmp_path,
        predecessor_manifest_path=predecessor,
        predecessor_manifest_sha256=predecessor_sha,
        output_path=output,
        target_id=target_id,
        evidence_path=evidence_path,
        evidence_sha256=evidence_sha,
    )

    successor = json.loads((tmp_path / output).read_text(encoding="utf-8"))
    assert result["selected_targets"] == 1
    assert successor["manifest_predecessor_sha256"] == predecessor_sha
    assert successor["current_state"] == "NO_DATA_CONFIRMED"
    assert successor["execution_action"] == "NO_FILE_CREATE"
    assert successor["provider_record_count"] == 0
    assert successor["no_data_evidence"]["evidence_sha256"] == evidence_sha


def test_authorized_batch_accepts_multiple_targets_bound_to_one_evidence_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch_path, batch, batch_sha = _batch(tmp_path)
    quote_path = tmp_path / "reports/ohlcv_58_completion/test/quote.json"
    _write(
        quote_path,
        {
            "estimated_data_cost_usd": "0.0",
            "plan_id": batch["plan_id"],
            "plan_sha256": batch_sha,
            "status": "PASS_WITHIN_APPROVED_ZERO_COST_CAP",
        },
    )
    evidence_path = tmp_path / "reports/ohlcv_58_completion/test/evidence-set.json"
    _write(evidence_path, {"schema_version": "test-evidence-set", "targets": ["a", "b"]})
    evidence_sha = sha256_file(evidence_path)
    predecessor_sha = "9" * 64
    evidence_binding = {
        "evidence_path": evidence_path.relative_to(tmp_path).as_posix(),
        "evidence_sha256": evidence_sha,
        "job_id": "GLBX-TEST",
        "provider_error_code": "symbology_invalid_request",
        "provider_error_message": "None of the symbols could be resolved",
        "provider_error_status": 422,
        "provider_manifest_hash": "8" * 64,
        "request_fingerprint": "7" * 64,
        "schema_version": "ohlcv_provider_no_data_evidence/1.0.0",
    }
    manifest_path = tmp_path / "reports/ohlcv_58_completion/test/multi-successor.jsonl"
    manifest_path.write_bytes(
        b"".join(
            canonical_bytes(
                {
                    "current_state": "NO_DATA_CONFIRMED",
                    "execution_action": "NO_FILE_CREATE",
                    "manifest_predecessor_sha256": predecessor_sha,
                    "manifest_schema": "ohlcv_historical_backfill_manifest/1.0.0",
                    "no_data_evidence": evidence_binding,
                    "provider_record_count": 0,
                    "target_id": digit * 64,
                }
            )
            + b"\n"
            for digit in ("1", "2")
        )
    )
    manifest_sha = sha256_file(manifest_path)
    state_root = Path("state/ohlcv_58_completion") / str(batch["plan_id"])
    for relative in (
        "src/futures_rebuild/ohlcv_completion_campaign.py",
        "src/futures_rebuild/ohlcv_historical_backfill.py",
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(relative.encode("ascii"))
    scope = campaign.required_acquisition_scope(
        tmp_path,
        batch,
        batch_sha,
        sha256_file(quote_path),
        manifest_sha,
        state_root,
        predecessor_manifest_sha256=predecessor_sha,
        no_data_evidence_sha256=evidence_sha,
    )
    receipt = OperationReceipt.issue_user_approved(
        RepoBoundary(tmp_path),
        operation=campaign.ACQUISITION_OPERATION,
        classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        scope={
            key: value
            for key, value in scope.items()
            if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
        },
        approval_command=campaign.ACQUISITION_OPERATION,
        approval_plan_id=str(batch["plan_id"]),
        approval_plan_sha256=batch_sha,
        approval_line=_personal_approval_line(
            campaign.ACQUISITION_OPERATION,
            str(batch["plan_id"]),
            batch_sha,
        ),
    )
    calls = 0

    def fake_execute_manifest(**kwargs: object) -> dict[str, int]:
        nonlocal calls
        calls += 1
        assert kwargs["resume_from_manifest_sha256"] == predecessor_sha
        return {"actions": 1}

    monkeypatch.setattr(campaign, "execute_manifest", fake_execute_manifest)
    result = campaign.execute_authorized_batch(
        tmp_path,
        batch_path=batch_path,
        batch_sha256=batch_sha,
        quote_path=quote_path,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
        state_root=state_root,
        authorization=receipt,
        predecessor_manifest_sha256=predecessor_sha,
        no_data_evidence_path=evidence_path,
        no_data_evidence_sha256=evidence_sha,
        provider_factory=lambda _: object(),
    )

    assert result["status"] == "ACQUISITION_EXECUTED"
    assert calls == 1
