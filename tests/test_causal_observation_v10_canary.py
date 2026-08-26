from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.causal_full_build_durable_host import expected_durable_host_plan
from futures_rebuild.causal_observation_foundation import (
    CAUSAL_OBSERVATION_CONTRACT_ID,
    required_v10_es_2025_canary_scope,
)
from futures_rebuild.causal_observation_full_build import (
    _execution_outcome,
)
from futures_rebuild.causal_observation_market_checkpoint import (
    CHECKPOINT_SET_SCHEMA,
    MARKET_ORDER,
    checkpoint_set_identity,
)
from futures_rebuild.causal_observation_v10_campaign import CampaignState, transition
from futures_rebuild.causal_observation_v10_canary import (
    MAXIMUM_DECODED_RECORDS,
    REQUIRED_CANARY_BINDINGS,
    _validate_entry_set,
    validate_v10_es_2025_canary_plan,
    v10_es_2025_inventory_document,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.research_gateway_policy import (
    CAUSAL_OBSERVATION_V10_CANARY_OPERATION,
)


ROOT = Path(__file__).resolve().parents[1]
ATTEMPT = "a" * 64


def _checkpoint_set() -> dict[str, object]:
    bindings = {
        relative: sha256_file(ROOT / relative)
        for relative in (
            "scripts/start_causal_full_build_v10_worker.ps1",
            "src/futures_rebuild/canonical.py",
            "src/futures_rebuild/causal_full_build_durable_host.py",
            "src/futures_rebuild/causal_observation_foundation.py",
            "src/futures_rebuild/causal_observation_full_build.py",
            "src/futures_rebuild/causal_observation_market_checkpoint.py",
            "src/futures_rebuild/causal_observation_parquet.py",
            "src/futures_rebuild/causal_observation_verifier.py",
            "src/futures_rebuild/data_layout.py",
        )
    }
    active = __import__("json").loads((ROOT / "configs/source_contract.json").read_text())
    return {
        "schema_version": CHECKPOINT_SET_SCHEMA,
        "market_order": list(MARKET_ORDER),
        "source_contract_id": active["contract_id"],
        "canonical_release_id": active["active_canonical_source"]["release_id"],
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "development_end_exclusive": "2025-07-13T22:00:00Z",
        "writer_configuration": {
            "format": "PARQUET",
            "compression": "ZSTD",
            "compression_level": 9,
            "partitioning": "market/year/month",
        },
        "implementation_bindings": bindings,
    }


def _plan() -> dict[str, object]:
    checkpoint_set = _checkpoint_set()
    source_contract = __import__("json").loads(
        (ROOT / "configs/source_contract.json").read_text()
    )
    core: dict[str, object] = {
        "schema_version": "development_causal_observation_v10_es_2025_canary_plan/1.0.0",
        "operation": CAUSAL_OBSERVATION_V10_CANARY_OPERATION,
        "execution_role": "V10_ES_2025_CANARY",
        "target_market": "ES",
        "target_year": 2025,
        "attempt_id": ATTEMPT,
        "checkpoint_set": checkpoint_set,
        "checkpoint_set_id": checkpoint_set_identity(checkpoint_set),
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source": {
            "source_contract_id": source_contract["contract_id"],
            "canonical_release_id": source_contract["active_canonical_source"]["release_id"],
            "inventory_path": "reports/future-v10-canary/inventory.json",
            "inventory_sha256": "1" * 64,
            "exact_source_entries_sha256": "2" * 64,
            "exact_dbn_entries_sha256": "3" * 64,
            "exact_source_entry_count": 14,
            "exact_dbn_file_count": 7,
            "exact_sidecar_file_count": 7,
            "total_source_bytes": 69_984_372,
            "maximum_payload_bytes": 69_971_994,
            "work_unit_count": 1,
        },
        "output_staging_path": f"data/causally_gated_normalized/v10/_canary/ES/{ATTEMPT}",
        "development_start_inclusive": "2025-01-01T00:00:00Z",
        "development_end_exclusive": "2025-07-13T22:00:00Z",
        "holdout_allowed": False,
        "forward_allowed": False,
        "provider_calls": 0,
        "execution_authorized": False,
        "complete_market_checkpoint": False,
        "reusable_in_same_checkpoint_set": False,
        "can_seed_complete_market_checkpoint": False,
        "authority": {name: False for name in (
            "activation", "evaluation", "features", "fitting", "forward", "holdout",
            "mechanism", "outcomes", "prediction", "provider", "publication", "wfa",
        )},
        "limits": {
            "maximum_payload_bytes": 69_971_994,
            "maximum_payload_bytes_per_decode": 69_971_994,
            "maximum_payload_bytes_total": 139_943_988,
            "maximum_decoded_records": MAXIMUM_DECODED_RECORDS,
            "maximum_output_bytes": 800_000_000,
            "maximum_partition_count": 7,
        },
        "execution": {
            "producer_decodes": 1,
            "independent_replay_decodes": 1,
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": 21_600,
            "maximum_workers": 1,
            "python_executable": ".venv/Scripts/python.exe",
            "databento_version": "0.78.0",
        },
        "durable_host": expected_durable_host_plan("ES", ATTEMPT),
        "task_cleanup": {
            "task_name": expected_durable_host_plan("ES", ATTEMPT)["task_name"],
            "unregister_after_terminal_evidence": True,
            "unregister_before_terminal_evidence": False,
        },
        "economics": {
            "rulebook_path": "configs/contract_economics_rules.json",
            "rulebook_sha256": "6a43960f252dc9103ea39f5ef4d082a71aa3aeefe89370c528dc29ac319e0f33",
            "provider_null_fallback_only": True,
            "negative_or_contradictory_provider_value": "FAIL_CLOSED",
        },
        "canary_implementation_bindings": {
            relative: sha256_file(ROOT / relative) for relative in REQUIRED_CANARY_BINDINGS
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def _result() -> dict[str, object]:
    core = {
        "schema_version": "development_causal_observation_v10_es_2025_canary_result/1.0.0",
        "status": "PASS_V10_ES_2025_CANARY_VERIFIED_INACTIVE",
        "target_market": "ES",
        "target_year": 2025,
        "complete_market_checkpoint": False,
        "reusable_in_same_checkpoint_set": False,
        "can_seed_complete_market_checkpoint": False,
        "campaign_advancement_eligible": True,
        "publication_authorized": False,
        "activation_authorized": False,
    }
    return {**core, "result_id": sha256_json(core)}


def test_exact_canary_plan_is_metadata_valid_and_two_decode_bounded() -> None:
    plan = _plan()
    validate_v10_es_2025_canary_plan(ROOT, plan)
    scope = required_v10_es_2025_canary_scope(
        plan=plan,
        plan_sha256="4" * 64,
        source_contract_id=str(plan["source"]["source_contract_id"]),
        canonical_release_id=str(plan["source"]["canonical_release_id"]),
    )
    assert scope["maximum_payload_bytes_total"] == "139943988"
    assert scope["producer_decodes"] == scope["independent_replay_decodes"] == "1"
    assert scope["can_seed_complete_market_checkpoint"] == "false"


def test_active_metadata_selects_exact_seven_es_2025_source_pairs_without_rows() -> None:
    inventory = v10_es_2025_inventory_document(ROOT)
    assert inventory["source_entry_count"] == 14
    assert inventory["dbn_file_count"] == inventory["sidecar_file_count"] == 7
    assert inventory["payload_bytes_per_decode"] == 69_971_994
    assert inventory["payload_files_opened"] == inventory["rows_read"] == 0
    assert {item["family"] for item in inventory["entries"]} == {
        "definition", "ohlcv_1d", "ohlcv_1h", "ohlcv_1m", "ohlcv_1s",
        "statistics", "status",
    }


@pytest.mark.parametrize("mutation", ("missing", "extra", "wrong_market", "wrong_family"))
def test_source_scope_rejects_missing_extra_or_wrong_registered_family(mutation: str) -> None:
    entries = deepcopy(v10_es_2025_inventory_document(ROOT)["entries"])
    if mutation == "missing":
        entries.pop()
    elif mutation == "extra":
        entries.append(deepcopy(entries[0]))
    elif mutation == "wrong_market":
        entries[0]["market"] = "NQ"
    else:
        entries[0]["family"] = "trades"
    with pytest.raises(IntegrityError, match="scope"):
        _validate_entry_set(entries)


@pytest.mark.parametrize(
    ("path", "value"),
    (
        (("target_market",), "NQ"),
        (("target_year",), 2024),
        (("development_end_exclusive",), "2025-07-14T00:00:00Z"),
        (("source", "exact_dbn_file_count"), 8),
        (("source", "total_source_bytes"), 69_984_373),
        (("limits", "maximum_payload_bytes_total"), 139_943_989),
        (("execution", "maximum_retries"), 1),
        (("can_seed_complete_market_checkpoint",), True),
    ),
)
def test_canary_plan_drift_fails_before_row_authority(path: tuple[str, ...], value: object) -> None:
    plan = deepcopy(_plan())
    target = plan
    for key in path[:-1]:
        target = target[key]  # type: ignore[index,assignment]
    target[path[-1]] = value  # type: ignore[index]
    core = {key: item for key, item in plan.items() if key != "plan_id"}
    plan["plan_id"] = sha256_json(core)
    with pytest.raises((UnauthorizedOperation, IntegrityError)):
        validate_v10_es_2025_canary_plan(ROOT, plan)


def test_canary_role_never_reports_complete_or_reusable_market() -> None:
    status, filename, complete = _execution_outcome(_plan())
    assert status == "PASS_V10_ES_2025_CANARY_PRODUCER_INACTIVE"
    assert filename == "canary_producer_result.json"
    assert complete is False


def test_campaign_rejects_generic_pass_and_binds_exact_canary_identity() -> None:
    canary_phase = transition(CampaignState(), "PASS")
    with pytest.raises(UnauthorizedOperation, match="exact canary"):
        transition(canary_phase, "PASS")
    forged = _result()
    forged["target_year"] = 2024
    with pytest.raises(IntegrityError, match="differs"):
        transition(canary_phase, "CANARY_VERIFIED", evidence=forged)
    advanced = transition(canary_phase, "CANARY_VERIFIED", evidence=_result())
    assert advanced.phase == "NORMALIZATION" and advanced.market == "ES"


def test_canary_destination_is_deep_but_contained_and_separate_from_market_checkpoint() -> None:
    plan = _plan()
    destination = ROOT / str(plan["output_staging_path"])
    assert len(str(destination / "sealed/2025/2025-07-01_2025-07-13T220000Z/candidate/observations.parquet")) > 220
    assert "/_canary/ES/" in destination.as_posix()
    assert f"/{plan['checkpoint_set_id']}/ES/" not in destination.as_posix()
