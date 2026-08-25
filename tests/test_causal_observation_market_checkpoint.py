from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.causal_full_build_durable_host import expected_durable_host_plan
from futures_rebuild.causal_observation_foundation import (
    CAUSAL_OBSERVATION_CONTRACT_ID,
    required_market_checkpoint_scope,
)
from futures_rebuild.causal_observation_market_checkpoint import (
    CHECKPOINT_SET_SCHEMA,
    MARKET_ORDER,
    PLAN_SCHEMA,
    certify_complete_checkpoint_set,
    checkpoint_set_identity,
    run_authorized_market_checkpoint,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.research_gateway_policy import (
    CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
)


H = "a" * 64
ATTEMPT_ID = "f" * 64


def _checkpoint_set() -> dict[str, object]:
    return {
        "schema_version": CHECKPOINT_SET_SCHEMA,
        "market_order": list(MARKET_ORDER),
        "source_contract_id": "b" * 64,
        "canonical_release_id": "c" * 64,
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "development_end_exclusive": "2025-07-13T22:00:00Z",
        "writer_configuration": {
            "format": "PARQUET",
            "compression": "ZSTD",
            "compression_level": 9,
            "partitioning": "market/year/month",
        },
        "implementation_bindings": {"synthetic.py": H},
    }


def _plan(market: str = "ES", attempt_id: str = ATTEMPT_ID) -> dict[str, object]:
    checkpoint_set = _checkpoint_set()
    core: dict[str, object] = {
        "schema_version": PLAN_SCHEMA,
        "operation": CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        "target_market": market,
        "attempt_id": attempt_id,
        "checkpoint_set": checkpoint_set,
        "checkpoint_set_id": checkpoint_set_identity(checkpoint_set),
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source": {
            "source_contract_id": "b" * 64,
            "canonical_release_id": "c" * 64,
            "inventory_path": "inventory.json",
            "inventory_sha256": H,
            "exact_source_entries_sha256": "d" * 64,
            "exact_dbn_entries_sha256": "e" * 64,
            "exact_source_entry_count": 20,
            "exact_dbn_file_count": 10,
            "exact_sidecar_file_count": 10,
            "total_source_bytes": 1_200,
            "maximum_payload_bytes": 1_000,
            "work_unit_count": 2,
        },
        "output_staging_path": (
            "state/data_publication_staging/"
            f"causal_observation_full_development_bounded_2025_v9/{market}/{attempt_id}"
        ),
        "development_end_exclusive": "2025-07-13T22:00:00Z",
        "holdout_allowed": False,
        "forward_allowed": False,
        "provider_calls": 0,
        "execution_authorized": False,
        "authority": {
            "activation": False,
            "evaluation": False,
            "features": False,
            "fitting": False,
            "forward": False,
            "holdout": False,
            "mechanism": False,
            "outcomes": False,
            "prediction": False,
            "provider": False,
            "publication": False,
            "wfa": False,
        },
        "limits": {
            "maximum_payload_bytes": 1_000,
            "maximum_decoded_records": 10_000,
            "maximum_output_bytes": 10_000,
            "maximum_partition_count": 100,
        },
        "execution": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": 216_000,
            "maximum_workers": 1,
            "python_executable": ".venv/Scripts/python.exe",
            "databento_version": "0.78.0",
        },
        "durable_host": expected_durable_host_plan(market, attempt_id),
        "economics": {
            "rulebook_path": "configs/contract_economics_rules.json",
            "rulebook_sha256": (
                "6a43960f252dc9103ea39f5ef4d082a71aa3aeefe89370c528dc29ac319e0f33"
            ),
            "provider_null_fallback_only": True,
            "negative_or_contradictory_provider_value": "FAIL_CLOSED",
        },
        "reuse_failed_market_partitions": False,
    }
    return {**core, "plan_id": sha256_json(core)}


def _result(market: str, checkpoint_set_id: str) -> dict[str, object]:
    core = {
        "schema_version": "development_causal_observation_full_build_result/1.0.0",
        "status": "PASS_COMPLETE_MARKET_CHECKPOINT_INACTIVE",
        "target_market": market,
        "attempt_id": ATTEMPT_ID,
        "checkpoint_set_id": checkpoint_set_id,
        "source_contract_id": "b" * 64,
        "source_release_id": "c" * 64,
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "complete_market_checkpoint": True,
        "reusable_in_same_checkpoint_set": True,
        "provider_calls": 0,
        "holdout_rows": 0,
        "forward_rows": 0,
        "outcomes": 0,
        "features": 0,
        "wfa": 0,
        "fitting": 0,
        "predictions": 0,
        "evaluations": 0,
        "mechanism_executions": 0,
        "publication_authorized": False,
        "activation_authorized": False,
        "partitions": [
            {
                "market": market,
                "year": 2010,
                "interval": "2010-01-01_2010-02-01",
            }
        ],
    }
    return {**core, "result_id": sha256_json(core)}


def test_each_market_receipt_scope_is_independent_and_exact() -> None:
    es = _plan("ES")
    gc = _plan("GC")
    es_scope = required_market_checkpoint_scope(
        plan=es,
        plan_sha256=H,
        source_contract_id="b" * 64,
        canonical_release_id="c" * 64,
    )
    gc_scope = required_market_checkpoint_scope(
        plan=gc,
        plan_sha256=H,
        source_contract_id="b" * 64,
        canonical_release_id="c" * 64,
    )
    assert es_scope["target_market"] == "ES"
    assert es_scope["attempt_id"] == ATTEMPT_ID
    assert gc_scope["target_market"] == "GC"
    assert es_scope["approval_plan_id"] != gc_scope["approval_plan_id"]
    assert es_scope["output_staging_path"].endswith(f"/ES/{ATTEMPT_ID}")
    assert gc_scope["output_staging_path"].endswith(f"/GC/{ATTEMPT_ID}")


def test_failed_market_retry_has_new_plan_scope_output_and_host_evidence() -> None:
    first = _plan("GC", "e" * 64)
    retry = _plan("GC", "f" * 64)
    assert first["plan_id"] != retry["plan_id"]
    assert first["output_staging_path"] != retry["output_staging_path"]
    assert first["durable_host"] != retry["durable_host"]


def test_final_set_requires_all_41_same_semantic_identity() -> None:
    checkpoint_set = _checkpoint_set()
    identity = checkpoint_set_identity(checkpoint_set)
    results = [_result(market, identity) for market in MARKET_ORDER]
    certificate = certify_complete_checkpoint_set(
        checkpoint_set=checkpoint_set, market_results=results
    )
    assert certificate["status"] == "PASS_41_MARKET_CHECKPOINT_SET_INACTIVE"
    assert certificate["market_count"] == 41
    with pytest.raises(UnauthorizedOperation, match="exact 41-market"):
        certify_complete_checkpoint_set(
            checkpoint_set=checkpoint_set, market_results=results[:-1]
        )
    incompatible = list(results)
    incompatible[3] = _result("CL", "f" * 64)
    with pytest.raises(IntegrityError, match="incompatible"):
        certify_complete_checkpoint_set(
            checkpoint_set=checkpoint_set, market_results=incompatible
        )


def test_failed_market_does_not_mutate_completed_other_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = _plan("GC")
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan, sort_keys=True) + "\n")
    es = tmp_path / (
        "state/data_publication_staging/"
        f"causal_observation_full_development_bounded_2025_v9/ES/{ATTEMPT_ID}/"
        "market_checkpoint.json"
    )
    es.parent.mkdir(parents=True)
    es.write_text("certified-es\n")
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_checkpoint.validate_market_checkpoint_plan",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_checkpoint._load_market_entries",
        lambda *_: (),
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_checkpoint."
        "validate_complete_development_boundary_metadata",
        lambda *_, **__: None,
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_checkpoint.select_exact_standard_source_entries",
        lambda *_, **__: (),
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_checkpoint.issue_current_source_closure_context",
        lambda *_: object(),
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_checkpoint.shutil.disk_usage",
        lambda *_: SimpleNamespace(free=10**15),
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_checkpoint."
        "validate_market_checkpoint_execution_environment",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_checkpoint.authorize_market_checkpoint_row_read",
        lambda **_: SimpleNamespace(receipt_id="1" * 64),
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_checkpoint._execute",
        lambda **_: (_ for _ in ()).throw(RuntimeError("synthetic GC failure")),
    )
    monkeypatch.setattr(
        "futures_rebuild.causal_observation_market_checkpoint._load_economics_rulebook",
        lambda *_: object(),
    )
    with pytest.raises(RuntimeError, match="synthetic GC failure"):
        run_authorized_market_checkpoint(
            repository_root=tmp_path,
            receipt=object(),  # type: ignore[arg-type]
            plan_path=plan_path,
        )
    assert es.read_text() == "certified-es\n"
    gc_failure = json.loads(
        (
            tmp_path
            / "state/data_publication_staging/"
            f"causal_observation_full_development_bounded_2025_v9/GC/{ATTEMPT_ID}/"
            "failure.json"
        ).read_text()
    )
    assert gc_failure["target_market"] == "GC"
    assert gc_failure["completed_other_market_checkpoints_affected"] is False
    assert gc_failure["failed_market_partition_reuse_authorized"] is False
