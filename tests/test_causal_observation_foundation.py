from __future__ import annotations

import json
import hashlib
import subprocess
from collections.abc import Mapping
from pathlib import Path

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
    _personal_approval_line,
)
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.causal_observation_foundation import (
    ACTIVE_SOURCE_CONTRACT_ID,
    CAUSAL_OBSERVATION_CONTRACT_ID,
    SYNTHETIC_RELEASE_ID,
    authorize_canary_row_read,
    issue_synthetic_observation_context,
    prepare_observation_partition,
    prepared_inventory,
    publish_prepared_observation_partition,
    required_canary_scope,
)
from futures_rebuild.causal_observation_verifier import verify_observation_candidate
from futures_rebuild.causal_source_closure import select_exact_standard_source_entries
from futures_rebuild.data_layout import PhasePublisher
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation
from futures_rebuild.foundation.records import INT64_NULL, ProviderBar
from futures_rebuild.foundation_operation_firewall import issue_current_source_closure_context
from futures_rebuild.research_gateway_policy import (
    CAUSAL_OBSERVATION_CANARY_OPERATION,
    require_current_real_history_operation,
)


ROOT = Path(__file__).resolve().parents[1]
FREEZE_ROOT = (
    ROOT
    / "reports/post_rebuild_causal_foundation_contract_freeze"
    / "pcfcf_20260822T2102038182073Z_5004bb2f"
)
CANARY = FREEZE_ROOT / "REPRESENTATIVE_CANARY_PLAN.json"
FROZEN_CONTRACT = ROOT / "configs/causal_observation_contract_v1.json"
CANARY_OPERATION_PLAN = ROOT / "configs/causal_observation_canary_plan_v1.json"
H = "a" * 64


def _provider_bar(**changes: object) -> ProviderBar:
    values: dict[str, object] = {
        "dataset": "GLBX.MDP3",
        "market": "CL",
        "publisher_id": 1,
        "instrument_id": 2,
        "event_at_ns": 1_700_000_000_000_000_000,
        "open_nano": -100_000_000_000,
        "high_nano": -50_000_000_000,
        "low_nano": -150_000_000_000,
        "close_nano": -75_000_000_000,
        "volume": 10,
        "source_release_id": H,
        "source_manifest_sha256": H,
        "source_file_path": "synthetic/ohlcv.dbn.zst",
        "source_file_sha256": H,
        "row_sha256": H,
    }
    values.update(changes)
    return ProviderBar(**values)  # type: ignore[arg-type]


def test_frozen_contract_is_the_exact_approved_nonactive_artifact() -> None:
    report_contract = FREEZE_ROOT / "CAUSAL_OBSERVATION_CONTRACT.json"
    assert sha256_file(FROZEN_CONTRACT) == (
        "e7fdc45fd045c78bb34fe084580f0b04e98eb42667a12622c8135b1ba0221bc4"
    )
    assert FROZEN_CONTRACT.read_bytes() == report_contract.read_bytes()
    contract = json.loads(FROZEN_CONTRACT.read_text(encoding="utf-8"))
    assert contract["contract_id"] == CAUSAL_OBSERVATION_CONTRACT_ID
    assert not any(contract["authority"].values())
    assert contract["time_segmentation"]["sealed_holdout"] == "FORBIDDEN"
    assert contract["time_segmentation"]["forward"] == "FORBIDDEN"


def test_canary_operation_plan_is_exact_and_bound_to_executed_predecessor() -> None:
    plan = json.loads(CANARY_OPERATION_PLAN.read_text(encoding="utf-8"))
    scope = required_canary_scope(
        plan=plan,
        plan_sha256=sha256_file(CANARY_OPERATION_PLAN),
    )
    predecessor = _canary()
    assert plan["source"]["exact_source_entries_sha256"] == sha256_json(
        predecessor["source_files"]
    )
    assert plan["source"]["exact_source_entry_count"] == 66
    assert plan["source"]["exact_dbn_file_count"] == 33
    assert plan["source"]["exact_sidecar_file_count"] == 33
    assert plan["source"]["total_source_bytes"] == 176_952_087
    assert plan["one_use_authorization"]["issued"] is False
    assert plan["one_use_authorization"]["consumed"] is False
    assert plan["execution_authorized"] is False
    assert scope["maximum_payload_bytes"] == "176929782"
    execution_commit = "d3f60621201bebb95eaad7b5fa2de6da10b3bb31"
    for path, digest in plan["implementation_bindings"].items():
        committed = subprocess.check_output(
            ["git", "show", f"{execution_commit}:{path}"], cwd=ROOT
        )
        assert hashlib.sha256(committed).hexdigest() == digest


def test_provider_bar_accepts_positive_zero_and_negative_futures_prices() -> None:
    assert _provider_bar().prices[0] < 0
    assert _provider_bar(
        open_nano=0, high_nano=0, low_nano=0, close_nano=0
    ).prices == (0, 0, 0, 0)
    assert _provider_bar(
        open_nano=100, high_nano=200, low_nano=50, close_nano=150
    ).prices[0] > 0


@pytest.mark.parametrize(
    "changes",
    [
        {"open_nano": 10, "high_nano": 5, "low_nano": 0, "close_nano": 8},
        {"volume": -1},
        {"open_nano": INT64_NULL},
        {"open_nano": float("nan")},
    ],
)
def test_provider_bar_rejects_invalid_ohlc_volume_and_numeric_state(
    changes: dict[str, object]
) -> None:
    with pytest.raises(ContractError):
        _provider_bar(**changes)


def _canary() -> dict[str, object]:
    return json.loads(CANARY.read_text(encoding="utf-8"))


def test_exact_v4_selector_admits_only_packet_bound_standard_development_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _canary()
    context = issue_current_source_closure_context(ROOT)
    opened_dbn: list[str] = []
    original = Path.open

    def guarded(path: Path, *args: object, **kwargs: object):
        if path.name.endswith(".dbn.zst"):
            opened_dbn.append(path.as_posix())
            raise AssertionError("DBN payload open is forbidden")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded)
    selected = select_exact_standard_source_entries(
        ROOT,
        operation_context=context,
        source_entries=plan["source_files"],  # type: ignore[arg-type]
        windows=plan["windows"],  # type: ignore[arg-type]
    )
    assert len(selected) == 66
    assert sum(item["kind"] == "DBN" for item in selected) == 33
    assert sum(item["kind"] == "SIDECAR" for item in selected) == 33
    assert {item["market"] for item in selected} == {"ES", "CL", "ZN", "6E", "GC", "ZC", "LE"}
    assert opened_dbn == []


def test_exact_v4_selector_rejects_holdout_micro_and_unlisted_fallback() -> None:
    plan = _canary()
    context = issue_current_source_closure_context(ROOT)
    windows = {key: dict(value) for key, value in plan["windows"].items()}
    windows["ES"]["end"] = "2025-07-14T00:00:00Z"
    with pytest.raises(UnauthorizedOperation, match="development boundary"):
        select_exact_standard_source_entries(
            ROOT,
            operation_context=context,
            source_entries=plan["source_files"],  # type: ignore[arg-type]
            windows=windows,
        )

    inventory_binding = json.loads((ROOT / "configs/source_contract.json").read_text())["complete_inventory"]
    inventory = json.loads((ROOT / inventory_binding["path"]).read_text())
    micro = [item for item in inventory["entries"] if item["market"] == "MES"][:2]
    with pytest.raises(UnauthorizedOperation, match="nonstandard"):
        select_exact_standard_source_entries(
            ROOT,
            operation_context=context,
            source_entries=micro,
            windows={"MES": {"start": "2024-01-01T00:00:00Z", "end": "2024-01-02T00:00:00Z"}},
        )

    changed = [dict(item) for item in plan["source_files"]]
    changed[0]["path"] = "data/causally_gated_normalized/latest/legacy.dbn.zst"
    with pytest.raises(IntegrityError, match="active exact inventory"):
        select_exact_standard_source_entries(
            ROOT,
            operation_context=context,
            source_entries=changed,
            windows=plan["windows"],  # type: ignore[arg-type]
        )


def _operation_plan() -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": "causal_observation_canary_operation/1.0.0",
        "operation": CAUSAL_OBSERVATION_CANARY_OPERATION,
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source": {
            "source_contract_id": ACTIVE_SOURCE_CONTRACT_ID,
            "canonical_release_id": "9867aedac9cfe732d015489fc4093ffc4aaab5ad698b75a5fa00ca7e1f457995",
            "exact_source_entries_sha256": "b" * 64,
        },
        "output_staging_path": "state/data_publication_staging/canary-exact",
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
            "maximum_payload_bytes": 176_929_782,
            "maximum_decoded_records": 176_929_782,
            "maximum_output_bytes": 176_929_782,
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def test_future_canary_scope_is_exact_and_one_use_in_an_isolated_repository(
    tmp_path: Path,
) -> None:
    boundary = RepoBoundary(tmp_path)
    plan = _operation_plan()
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan, sort_keys=True, separators=(",", ":")) + "\n")
    scope = required_canary_scope(plan=plan, plan_sha256=sha256_file(plan_path))
    base = {key: value for key, value in scope.items() if not key.startswith("approval_")}
    approval = _personal_approval_line(
        CAUSAL_OBSERVATION_CANARY_OPERATION,
        str(plan["plan_id"]),
        sha256_file(plan_path),
    )
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=CAUSAL_OBSERVATION_CANARY_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=base,
        approval_command=CAUSAL_OBSERVATION_CANARY_OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=sha256_file(plan_path),
        approval_line=approval,
    )
    context = authorize_canary_row_read(
        boundary=boundary,
        receipt=receipt,
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    assert context.synthetic is False
    with pytest.raises(UnauthorizedOperation, match="already used"):
        authorize_canary_row_read(
            boundary=boundary,
            receipt=receipt,
            plan=plan,
            plan_sha256=sha256_file(plan_path),
        )


def _observation(context, **changes: object) -> dict[str, object]:
    core: dict[str, object] = {
        "market": "CL",
        "source_contract_id": context.source_contract_id,
        "source_release_id": context.source_release_id,
        "source_file_path": "synthetic/ohlcv_1m.dbn.zst",
        "source_file_sha256": "c" * 64,
        "source_row_sha256": "d" * 64,
        "source_cadence": "1m",
        "bar_start_ns": 1_700_000_000_000_000_000,
        "bar_end_ns": 1_700_000_060_000_000_000,
        "source_timestamp_ns": 1_700_000_000_000_000_000,
        "available_at_ns": 1_700_000_060_000_000_000,
        "decision_eligible_at_ns": 1_700_000_061_000_000_000,
        "publisher_id": 1,
        "instrument_id": 2,
        "raw_symbol": "CLF4",
        "actual_contract": "CLF4",
        "definition_source_file_path": "synthetic/definition.dbn.zst",
        "definition_source_file_sha256": "8" * 64,
        "definition_row_sha256": "9" * 64,
        "definition_event_at_ns": 1_699_999_000_000_000_000,
        "definition_received_at_ns": 1_699_999_001_000_000_000,
        "listing_activation_ns": 1_699_000_000_000_000_000,
        "expiration_ns": 1_710_000_000_000_000_000,
        "open_nano": -100_000_000_000,
        "high_nano": -50_000_000_000,
        "low_nano": -150_000_000_000,
        "close_nano": -75_000_000_000,
        "volume": 10,
        "currency": "USD",
        "min_price_increment_nano": 10_000_000,
        "multiplier_nano": 1_000_000_000,
        "project_session_id": "PROJECT-2023-11-14",
        "project_trade_date": "2023-11-14",
        "project_grouping_start_ns": 1_699_980_000_000_000_000,
        "project_grouping_end_ns": 1_700_060_000_000_000_000,
        "project_timezone": "America/Chicago",
        "official_schedule_state": "UNKNOWN_FAIL_CLOSED",
    }
    core.update(changes)
    return {"row_id": sha256_json(core), **core}


def _evidence(row: Mapping[str, object]):
    row_id = str(row["row_id"])
    missing = {
        "evidence_id": "7" * 64,
        "observation_row_id": row_id,
        "market": "CL",
        "interval_start_ns": row["bar_start_ns"],
        "interval_end_ns": row["bar_end_ns"],
        "state": "OBSERVED_VALID",
        "authority": "DECODED_SOURCE_ROW",
        "evidence_sha256": "e" * 64,
    }
    roll = {
        "row_id": row_id,
        "actual_contract_before": "CLF4",
        "actual_contract_after": "CLF4",
        "effective_time_ns": None,
        "causal_selection_evidence_sha256": "f" * 64,
        "roll_flag": False,
        "price_discontinuity_flag": False,
        "crossing_status": "NO_CROSSING",
    }
    quality = {
        "row_id": row_id,
        "row_identity_sha256": row_id,
        "ohlc_valid": True,
        "volume_valid": True,
        "timestamp_order_valid": True,
        "duplicate_state": "UNIQUE",
        "source_contract_id": row["source_contract_id"],
        "source_release_id": row["source_release_id"],
        "source_file_sha256": row["source_file_sha256"],
        "quality_flags": ["NEGATIVE_PRICE_PROVIDER_VALID"],
    }
    cadence = {
        "comparison_id": "1" * 64,
        "row_id": row_id,
        "source_cadence": "1m",
        "comparison_cadence": "1h",
        "interval_boundary_compatible": True,
        "result": "DISAGREEMENT",
        "exception_state": "PRESERVE_BOTH_NO_OVERWRITE",
    }
    return missing, roll, quality, cadence


def _publisher(tmp_path: Path):
    boundary = RepoBoundary(tmp_path)
    receipt = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    return boundary, PhasePublisher(
        boundary=boundary,
        operation_receipt=receipt,
        lock_path=tmp_path / "state/locks/causal-observation.lock",
    )


def test_observation_only_producer_and_independent_verifier(
    tmp_path: Path,
) -> None:
    boundary, publisher = _publisher(tmp_path)
    context = issue_synthetic_observation_context(boundary=boundary, fixture_id="2" * 64)
    row = _observation(context)
    missing, roll, quality, cadence = _evidence(row)
    gap = {
        **missing,
        "evidence_id": "6" * 64,
        "observation_row_id": None,
        "interval_start_ns": int(row["bar_end_ns"]),
        "interval_end_ns": int(row["bar_end_ns"]) + 60_000_000_000,
        "state": "UNKNOWN_FAIL_CLOSED",
        "authority": "SCHEDULE_AUTHORITY_UNRESOLVED",
    }
    prepared = prepare_observation_partition(
        publisher=publisher,
        context=context,
        market="CL",
        year=2023,
        interval="2023-04-01_2023-04-02",
        observations=[row],
        missingness=[missing, gap],
        rolls=[roll],
        quality=[quality],
        cadence=[cadence],
    )
    inventory = prepared_inventory(prepared)
    certificate = verify_observation_candidate(
        stage=prepared.stage,
        manifest=prepared.manifest,
    )
    assert certificate["status"] == "PASS_SYNTHETIC_OR_AUTHORIZED_CANDIDATE_ONLY_NOT_PUBLISHED"
    assert certificate["producer_success_flag_accepted"] is False
    assert certificate["outcome_count"] == certificate["feature_count"] == 0
    assert certificate["counts"]["missingness"] == 2
    assert inventory["producer_success_is_not_certification"] is True
    assert prepared.manifest.phase == "causally_gated_normalized"
    assert not (tmp_path / "data/releases/foundation").exists()
    repeated = prepare_observation_partition(
        publisher=publisher,
        context=context,
        market="CL",
        year=2023,
        interval="2023-04-01_2023-04-02",
        observations=[row],
        missingness=[missing, gap],
        rolls=[roll],
        quality=[quality],
        cadence=[cadence],
    )
    assert repeated.manifest.as_dict() == prepared.manifest.as_dict()
    assert prepared_inventory(repeated)["stage_file_sha256"] == inventory["stage_file_sha256"]
    with pytest.raises(UnauthorizedOperation, match="not publication-authorized"):
        publish_prepared_observation_partition(
            prepared,
            publisher=publisher,
            context=context,
        )


def test_evidence_fails_closed_for_unsupported_closure_and_cadence_overwrite(
    tmp_path: Path,
) -> None:
    boundary, publisher = _publisher(tmp_path)
    context = issue_synthetic_observation_context(boundary=boundary, fixture_id="3" * 64)
    row = _observation(context)
    missing, roll, quality, cadence = _evidence(row)
    missing["state"] = "MARKET_CLOSED"
    missing["authority"] = "OBSERVED_ABSENCE"
    missing["observation_row_id"] = None
    with pytest.raises(UnauthorizedOperation, match="cannot imply"):
        prepare_observation_partition(
            publisher=publisher,
            context=context,
            market="CL",
            year=2023,
            interval="2023-04-01_2023-04-02",
            observations=[row], missingness=[missing], rolls=[roll], quality=[quality], cadence=[cadence],
        )
    cadence["exception_state"] = "NONE"
    missing["state"] = "OBSERVED_VALID"
    missing["authority"] = "DECODED_SOURCE_ROW"
    missing["observation_row_id"] = row["row_id"]
    with pytest.raises(ContractError, match="explicit exception"):
        prepare_observation_partition(
            publisher=publisher,
            context=context,
            market="CL",
            year=2023,
            interval="2023-04-01_2023-04-02",
            observations=[row], missingness=[missing], rolls=[roll], quality=[quality], cadence=[cadence],
        )


def test_current_canary_operation_is_preparatory_not_trial_execution() -> None:
    require_current_real_history_operation(
        CAUSAL_OBSERVATION_CANARY_OPERATION,
        {"operation_kind": "DEVELOPMENT_CAUSAL_OBSERVATION_ONLY"},
    )
