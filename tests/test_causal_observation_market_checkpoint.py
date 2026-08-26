from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild.canonical import io_path, sha256_file, sha256_json
from futures_rebuild.causal_full_build_durable_host import expected_durable_host_plan
from futures_rebuild.causal_observation_foundation import (
    CAUSAL_OBSERVATION_CONTRACT_ID,
    required_market_checkpoint_scope,
)
from futures_rebuild.causal_observation_full_build import (
    WORK_UNIT_REUSE_SCHEMA,
    WORK_UNIT_SEAL_SCHEMA,
    _load_v10_reused_work_units,
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
            "data/causally_gated_normalized/v10/"
            f"{checkpoint_set_identity(checkpoint_set)}/{market}/{attempt_id}"
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


def _sealed_reuse_fixture(
    root: Path,
) -> tuple[dict[str, object], tuple[dict[str, object], ...], Path, Path]:
    predecessor_attempt = "e" * 64
    retry_attempt = "f" * 64
    plan = _plan("ES", retry_attempt)
    checkpoint_set_id = str(plan["checkpoint_set_id"])
    predecessor = (
        root
        / "data/causally_gated_normalized/v10"
        / checkpoint_set_id
        / "ES"
        / predecessor_attempt
    )
    candidate = predecessor / "sealed/2010/2010-01-01_2010-02-01/candidate"
    io_path(candidate).mkdir(parents=True)
    output_file = candidate / "observations.parquet"
    io_path(output_file).write_bytes(b"sealed-year")
    file_entry = {
        "path": output_file.relative_to(root).as_posix(),
        "size": io_path(output_file).stat().st_size,
        "sha256": sha256_file(output_file),
    }
    partitions = [
        {
            "market": "ES",
            "year": 2010,
            "interval": "2010-01-01_2010-02-01",
            "release_id": "1" * 64,
            "certificate_id": "2" * 64,
            "inventory_sha256": "3" * 64,
            "output_bytes": io_path(output_file).stat().st_size,
            "stage": candidate.parent.relative_to(root).as_posix(),
        }
    ]
    selected = tuple(
        {
            "market": "ES",
            "year": year,
            "family": family,
            "kind": "DBN",
            "path": f"source/{year}/{family}.dbn.zst",
            "size_bytes": 10,
        }
        for year in (2010, 2011)
        for family in ("definition", "ohlcv_1m")
    )
    unit_entries = tuple(sorted(selected[:2], key=lambda item: str(item["path"])))
    last = {"bar_end_ns": 100, "row_id": "4" * 64}
    support = [[100, "STATUS", "5" * 64]]
    seal_core = {
        "schema_version": WORK_UNIT_SEAL_SCHEMA,
        "status": "PASS_SEALED_INACTIVE_WORK_UNIT",
        "market": "ES",
        "year": 2010,
        "checkpoint_set_id": checkpoint_set_id,
        "plan_id": "6" * 64,
        "source_entries_sha256": sha256_json(list(unit_entries)),
        "decoded_record_count": 20,
        "partition_count": 1,
        "partitions": partitions,
        "partitions_sha256": sha256_json(partitions),
        "files": [file_entry],
        "files_sha256": sha256_json([file_entry]),
        "output_bytes": io_path(output_file).stat().st_size,
        "last_observation": last,
        "last_observation_sha256": sha256_json(last),
        "carried_support": support,
        "carried_support_sha256": sha256_json(support),
        "publication_authorized": False,
        "activation_authorized": False,
    }
    seal = {**seal_core, "seal_id": sha256_json(seal_core)}
    seal_path = predecessor / "work_unit_seals/2010.json"
    io_path(seal_path.parent).mkdir(parents=True)
    io_path(seal_path).write_text(
        json.dumps(seal, sort_keys=True, separators=(",", ":")) + "\n"
    )
    failure = {
        "status": "FAILED_MARKET_TERMINAL_OTHER_CHECKPOINTS_UNAFFECTED",
        "target_market": "ES",
        "checkpoint_set_id": checkpoint_set_id,
        "plan_id": "6" * 64,
        "sealed_work_units_reusable_if_all_bindings_match": True,
    }
    failure_path = predecessor / "failure.json"
    io_path(failure_path).write_text(
        json.dumps(failure, sort_keys=True, separators=(",", ":")) + "\n"
    )
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    core["sealed_work_unit_reuse"] = {
        "schema_version": WORK_UNIT_REUSE_SCHEMA,
        "predecessor_output_staging_path": predecessor.relative_to(root).as_posix(),
        "predecessor_plan_id": "6" * 64,
        "predecessor_failure_path": failure_path.relative_to(root).as_posix(),
        "predecessor_failure_sha256": sha256_file(failure_path),
        "seals": [
            {
                "year": 2010,
                "path": seal_path.relative_to(root).as_posix(),
                "sha256": sha256_file(seal_path),
            }
        ],
    }
    retry = {**core, "plan_id": sha256_json(core)}
    current = root / str(retry["output_staging_path"])
    return retry, selected, current, output_file


def test_fresh_attempt_reuses_only_an_exact_immutable_sealed_year_prefix(
    tmp_path: Path,
) -> None:
    plan, selected, current, output_file = _sealed_reuse_fixture(tmp_path)
    reused = _load_v10_reused_work_units(
        root=tmp_path,
        plan=plan,
        selected=selected,
        current_output=current,
    )
    assert list(reused) == [2010]
    assert reused[2010]["decoded_record_count"] == 20
    io_path(output_file).write_bytes(b"changed")
    with pytest.raises(IntegrityError, match="files differ"):
        _load_v10_reused_work_units(
            root=tmp_path,
            plan=plan,
            selected=selected,
            current_output=current,
        )


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
        "data/causally_gated_normalized/v10/"
        f"{checkpoint_set_identity(_checkpoint_set())}/ES/{ATTEMPT_ID}/"
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
            / "data/causally_gated_normalized/v10/"
            / checkpoint_set_identity(_checkpoint_set())
            / f"GC/{ATTEMPT_ID}/"
            "failure.json"
        ).read_text()
    )
    assert gc_failure["target_market"] == "GC"
    assert gc_failure["completed_other_market_checkpoints_affected"] is False
    assert gc_failure["sealed_work_unit_count"] == 0
    assert gc_failure["sealed_work_units_reusable_if_all_bindings_match"] is False
    assert gc_failure["unsealed_work_unit_reuse_authorized"] is False
