from __future__ import annotations

import ast
import json
from pathlib import Path

import databento_dbn as dbn
import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.foundation.orchestrator import (
    OUTCOME_DEFERRED_UNTIL,
    OUTCOME_SOURCE_ROLE,
    FoundationOrchestrator,
    load_foundation_set,
    load_outcome_source_input,
)
from futures_rebuild.foundation.selection import publish_source_selection
from futures_rebuild.foundation.snapshot import PublishedSourceSnapshot
from futures_rebuild.foundation.support import VerifiedFoundationPolicies
from futures_rebuild.producer_bridge import (
    CausalFeatureSpec,
    load_actual_contract_definitions,
    load_actual_contract_economics,
    load_causal_feature_release,
    load_versioned_session_policy,
)
from futures_rebuild.release import AtomicPublisher, VerifiedReleaseReceipt
from futures_rebuild.source_symbology import build_query_contract


REPO = Path(__file__).resolve().parents[1]
START_NS = 1_704_067_200_000_000_000
END_NS = 1_735_689_600_000_000_000


class SyntheticInterruption(RuntimeError):
    pass


def _definition_bytes() -> bytes:
    metadata = dbn.Metadata(
        dataset="GLBX.MDP3",
        start=START_NS,
        end=END_NS,
        stype_in=dbn.SType.PARENT,
        stype_out=dbn.SType.INSTRUMENT_ID,
        schema=dbn.Schema.DEFINITION,
        symbols=["ES.FUT"],
        ts_out=False,
    )
    record = dbn.InstrumentDefMsg(
        publisher_id=1,
        instrument_id=123,
        ts_event=START_NS,
        ts_recv=START_NS + 1,
        min_price_increment=250_000_000,
        display_factor=1_000_000_000,
        raw_symbol="ESZ4",
        asset="ES",
        security_type="FUT",
        instrument_class=dbn.InstrumentClass.FUTURE,
        security_update_action=dbn.SecurityUpdateAction.ADD,
        unit_of_measure_qty=50_000_000_000,
        currency="USD",
        group="ES",
        exchange="XCME",
        unit_of_measure="IPNT",
    )
    return metadata.encode() + bytes(record)


def _bar_bytes() -> bytes:
    metadata = dbn.Metadata(
        dataset="GLBX.MDP3",
        start=START_NS,
        end=END_NS,
        stype_in=dbn.SType.CONTINUOUS,
        stype_out=dbn.SType.INSTRUMENT_ID,
        schema=dbn.Schema.OHLCV_1M,
        symbols=["ES.v.0"],
        ts_out=False,
    )
    record = dbn.OHLCVMsg(
        dbn.RType.OHLCV_1M,
        1,
        123,
        START_NS,
        5_000_000_000_000,
        5_001_000_000_000,
        4_999_000_000_000,
        5_000_500_000_000,
        100,
    )
    return metadata.encode() + bytes(record)


def _status_bytes() -> bytes:
    metadata = dbn.Metadata(
        dataset="GLBX.MDP3",
        start=START_NS,
        end=END_NS,
        stype_in=dbn.SType.PARENT,
        stype_out=dbn.SType.INSTRUMENT_ID,
        schema=dbn.Schema.STATUS,
        symbols=["ES.FUT"],
        ts_out=False,
    )
    record = dbn.StatusMsg(
        1,
        123,
        START_NS,
        START_NS + 1,
        action=dbn.StatusAction.TRADING,
        reason=dbn.StatusReason.NONE,
        trading_event=dbn.TradingEvent.NONE,
        is_trading=dbn.TriState.YES,
        is_quoting=dbn.TriState.YES,
        is_short_sell_restricted=dbn.TriState.NO,
    )
    return metadata.encode() + bytes(record)


def _statistics_bytes() -> bytes:
    metadata = dbn.Metadata(
        dataset="GLBX.MDP3",
        start=START_NS,
        end=END_NS,
        stype_in=dbn.SType.PARENT,
        stype_out=dbn.SType.INSTRUMENT_ID,
        schema=dbn.Schema.STATISTICS,
        symbols=["ES.FUT"],
        ts_out=False,
    )
    record = dbn.StatMsg(
        1,
        123,
        START_NS,
        START_NS + 1,
        START_NS,
        5_000_000_000_000,
        100,
        dbn.StatType.OPEN_INTEREST,
        sequence=1,
        ts_in_delta=1,
        channel_id=1,
        update_action=dbn.StatUpdateAction.NEW,
        stat_flags=0,
    )
    return metadata.encode() + bytes(record)


def _copy_configs(boundary) -> None:
    for name in (
        "contract_economics_rules.json",
        "environment.lock.json",
        "foundation_policy.json",
        "foundation_coverage_policy.json",
        "known_anomalies.json",
        "mechanical_feature_spec.json",
        "session_policy.json",
        "statistics_foundation_roles.json",
    ):
        (boundary.active_root / "configs" / name).write_bytes(
            (REPO / "configs" / name).read_bytes()
        )
    # The production policy intentionally requires a large, broadly resolved
    # foundation.  This one-row DBN fixture tests mechanics, not production
    # sufficiency, so it uses an explicit fixture-only policy rather than
    # weakening the tracked production gate.
    synthetic_coverage = {
        "minimum_bar_rows": 1,
        "minimum_statistics_source_market_year_fraction": "1",
        "minimum_status_eligible_rows": 1,
        "minimum_status_gated_feature_ready_fraction": "1",
        "minimum_status_gated_feature_ready_rows": 1,
        "minimum_status_resolved_decision_fraction": "1",
        "minimum_status_source_market_year_fraction": "1",
        "policy_version": "1.0.0",
    }
    (boundary.active_root / "configs" / "foundation_coverage_policy.json").write_bytes(
        canonical_bytes(synthetic_coverage) + b"\n"
    )


def _snapshot(boundary) -> PublishedSourceSnapshot:
    payloads = {
        "dbn/definition/ES/2024/2024-01-01_2025-01-01.dbn.zst": _definition_bytes(),
        "dbn/definition/ES/2024/2024-01-01_2025-01-01.dbn.zst.manifest.json": b"{}\n",
        "dbn/ohlcv_1m/ES/2024/2024-01-01_2025-01-01.dbn.zst": _bar_bytes(),
        "dbn/ohlcv_1m/ES/2024/2024-01-01_2025-01-01.dbn.zst.manifest.json": b"{}\n",
        "dbn/statistics/ES/2024/2024-01-01_2025-01-01.dbn.zst": _statistics_bytes(),
        "dbn/statistics/ES/2024/2024-01-01_2025-01-01.dbn.zst.manifest.json": b"{}\n",
        "dbn/status/ES/2024/2024-01-01_2025-01-01.dbn.zst": _status_bytes(),
        "dbn/status/ES/2024/2024-01-01_2025-01-01.dbn.zst.manifest.json": b"{}\n",
    }
    files = [
        {
            "path": path,
            "sha256": __import__("hashlib").sha256(content).hexdigest(),
            "size": len(content),
        }
        for path, content in sorted(payloads.items())
    ]
    core = {
        "approval_id": "c" * 64,
        "files": files,
        "files_index_sha256": sha256_json(files),
        "inventory_sha256": "b" * 64,
        "manifest_sha256": "a" * 64,
        "migration_implementation_sha256": "d" * 64,
        "receipt_version": "1.0.0",
        "status": "COMPLETE_VERIFIED_IMMUTABLE",
        "total_bytes": sum(item["size"] for item in files),
        "total_files": len(files),
        "user_authorization_id": "e" * 64,
    }
    receipt = {**core, "source_snapshot_id": sha256_json(core)}
    root = (
        boundary.active_root
        / "data"
        / "vault"
        / "source_snapshots"
        / receipt["source_snapshot_id"]
    )
    for relative, content in payloads.items():
        target = root / Path(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    (root / "SOURCE_SNAPSHOT_RECEIPT.json").write_bytes(
        canonical_bytes(receipt) + b"\n"
    )
    return PublishedSourceSnapshot.open(root, boundary=boundary)


def _selection_receipt(
    boundary, operation_factory, snapshot, *, false_continuous_status: bool = False
):
    publisher = AtomicPublisher(
        boundary.active_root
        / "data"
        / "vault"
        / ".staging"
        / "releases"
        / "selection",
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / "selection.lock",
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
    )
    definitions = snapshot.file(
        "dbn/definition/ES/2024/2024-01-01_2025-01-01.dbn.zst"
    )
    bars = snapshot.file(
        "dbn/ohlcv_1m/ES/2024/2024-01-01_2025-01-01.dbn.zst"
    )
    statistics = snapshot.file(
        "dbn/statistics/ES/2024/2024-01-01_2025-01-01.dbn.zst"
    )
    status = snapshot.file(
        "dbn/status/ES/2024/2024-01-01_2025-01-01.dbn.zst"
    )
    files = []
    for family, schema, binding in (
        ("dbn_definition", "definition", definitions),
        ("dbn_ohlcv_1m", "ohlcv-1m", bars),
        ("dbn_statistics", "statistics", statistics),
        ("dbn_status", "status", status),
    ):
        parent = schema in {"definition", "statistics", "status"}
        if schema == "status" and false_continuous_status:
            parent = False
        stype_in = "parent" if parent else "continuous"
        symbols = ["ES.FUT" if parent else "ES.v.0"]
        query = build_query_contract(
            schema=schema,
            market="ES",
            start="2024-01-01",
            end="2025-01-01",
            stype_in=stype_in,
            symbols=symbols,
        )
        sidecar = snapshot.file(f"{binding.relative_path}.manifest.json")
        entry = {
                "coverage_disposition": "AUTHORITATIVE_INTERVAL",
                "end": "2025-01-01",
                "family": family,
                "market": "ES",
                "path": f"data/{binding.relative_path}",
                "query_contract": query,
                "query_contract_id": query["query_contract_id"],
                "query_mode_id": query["query_mode_id"],
                "query_stype_in": stype_in,
                "query_symbols": symbols,
                "schema": schema,
                "sha256": binding.sha256,
                "sidecar_path": f"data/{sidecar.relative_path}",
                "sidecar_sha256": sidecar.sha256,
                "sidecar_size": sidecar.size,
                "size": binding.size,
                "start": "2024-01-01",
                "year": 2024,
            }
        files.append({**entry, "validation_sha256": sha256_json(entry)})
    core = {
        "catalog_contract_version": "2.0.0",
        "dataset": "GLBX.MDP3",
        "families": [
            {"family": name}
            for name in (
                "dbn_definition",
                "dbn_ohlcv_1m",
                "dbn_statistics",
                "dbn_status",
            )
        ],
        "files": files,
        "selection_policy": "EXACT_CONTRACT_ALL_FILES_NO_RECURSIVE_NEWEST",
        "selection_scope": "FILTERED",
        "source_scope": "VERIFIED_PUBLISHED_SOURCE_SNAPSHOT",
        "source_snapshot_id": snapshot.source_snapshot_id,
    }
    selection = {**core, "selection_manifest_id": sha256_json(core)}
    return publish_source_selection(
        selection, snapshot=snapshot, publisher=publisher
    )


def _setup(boundary, operation_factory):
    _copy_configs(boundary)
    snapshot = _snapshot(boundary)
    selection_receipt = _selection_receipt(boundary, operation_factory, snapshot)
    spec = CausalFeatureSpec(
        ("bar_body_fraction", "bar_return", "intrabar_range_fraction", "volume"),
        60,
        300,
    )
    return snapshot, selection_receipt, spec


def _orchestrator(boundary, operation_factory) -> FoundationOrchestrator:
    return FoundationOrchestrator(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        batch_rows=1,
    )


def test_selected_query_contract_must_match_exact_dbn_header(
    boundary, operation_factory
) -> None:
    _copy_configs(boundary)
    snapshot = _snapshot(boundary)
    selection_receipt = _selection_receipt(
        boundary,
        operation_factory,
        snapshot,
        false_continuous_status=True,
    )
    spec = CausalFeatureSpec(
        ("bar_body_fraction", "bar_return", "intrabar_range_fraction", "volume"),
        60,
        300,
    )
    with pytest.raises(IntegrityError, match="exact foundation contract"):
        _orchestrator(boundary, operation_factory).run(
            source_snapshot_root=snapshot.root,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
        )


def test_foundation_run_resumes_after_durable_phase_and_is_idempotent(
    boundary, operation_factory
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)
    phases: list[str] = []

    def interrupt(phase: str) -> None:
        phases.append(phase)
        if phase.endswith(":causal"):
            raise SyntheticInterruption("synthetic stop after durable causal checkpoint")

    with pytest.raises(SyntheticInterruption):
        _orchestrator(boundary, operation_factory).run(
            source_snapshot_root=snapshot.root,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
            after_checkpoint=interrupt,
        )
    assert any(phase.endswith(":causal") for phase in phases)
    checkpoint_paths = list(
        (boundary.active_root / "data" / "vault" / ".staging" / "foundation_runs").glob(
            "*/checkpoint.json"
        )
    )
    assert len(checkpoint_paths) == 1
    partial = json.loads(checkpoint_paths[0].read_text(encoding="utf-8"))
    assert partial["status"] == "RUNNING"
    assert partial["completed"]["intervals"]

    result = _orchestrator(boundary, operation_factory).run(
        source_snapshot_root=snapshot.root,
        source_selection_receipt=selection_receipt,
        feature_spec=spec,
    )
    assert result.completed_phase_count == 12
    foundation_set = load_foundation_set(
        result.foundation_set_receipt, boundary=boundary
    )
    assert foundation_set["dependency_closure_complete"] is True
    assert foundation_set["interval_count"] == 1
    assert foundation_set["outcome_contract"] == {
        "deferred_until": OUTCOME_DEFERRED_UNTIL,
        "labels_materialized": False,
        "prediction_ledger_read": False,
        "role": OUTCOME_SOURCE_ROLE,
    }
    interval = foundation_set["intervals"][0]
    outcome_receipt = VerifiedReleaseReceipt.from_dict(
        interval["outcome_source_input_release_receipt"]
    )
    outcome_input = load_outcome_source_input(outcome_receipt, boundary=boundary)
    assert outcome_input["outcomes_materialized"] is False
    assert outcome_input["labels_materialized"] is False
    assert outcome_input["prediction_ledger_read"] is False
    feature_receipt = VerifiedReleaseReceipt.from_dict(
        interval["feature_input_release_receipt"]
    )
    assert feature_receipt.release_id not in outcome_receipt.verify(
        boundary
    ).source_release_ids
    assert outcome_receipt.release_id not in feature_receipt.verify(
        boundary
    ).source_release_ids
    with pytest.raises(IntegrityError):
        load_outcome_source_input(feature_receipt, boundary=boundary)
    policy_receipt = VerifiedReleaseReceipt.from_dict(
        foundation_set["foundation_policy_receipt"]
    )
    policies = VerifiedFoundationPolicies.from_release(
        policy_receipt, boundary=boundary
    )
    session_policy = load_versioned_session_policy(
        VerifiedReleaseReceipt.from_dict(foundation_set["session_policy_receipt"]),
        policies=policies,
        boundary=boundary,
    )
    raw_receipt = VerifiedReleaseReceipt.from_dict(interval["raw_release_receipt"])
    causal_receipt = VerifiedReleaseReceipt.from_dict(
        interval["causal_release_receipt"]
    )
    definitions = load_actual_contract_definitions(
        VerifiedReleaseReceipt.from_dict(interval["definition_release_receipt"]),
        raw_receipt=raw_receipt,
        policies=policies,
        boundary=boundary,
    )
    economics = load_actual_contract_economics(
        VerifiedReleaseReceipt.from_dict(interval["economics_release_receipt"]),
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    )
    with pytest.raises(IntegrityError):
        load_causal_feature_release(
            outcome_receipt,
            causal_receipt=causal_receipt,
            definitions=definitions,
            economics_registry=economics,
            policies=policies,
            session_policy=session_policy,
            boundary=boundary,
        )

    releases_before = sorted(
        path.name
        for path in (boundary.active_root / "data" / "vault" / "releases").iterdir()
        if path.is_dir()
    )
    repeated = _orchestrator(boundary, operation_factory).run(
        source_snapshot_root=snapshot.root,
        source_selection_receipt=selection_receipt,
        feature_spec=spec,
    )
    releases_after = sorted(
        path.name
        for path in (boundary.active_root / "data" / "vault" / "releases").iterdir()
        if path.is_dir()
    )
    assert repeated.run_id == result.run_id
    assert repeated.foundation_set_receipt == result.foundation_set_receipt
    assert releases_after == releases_before


def test_resume_rejects_checkpoint_and_accepted_release_tampering(
    boundary, operation_factory
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)

    def interrupt(phase: str) -> None:
        if phase.endswith(":causal"):
            raise SyntheticInterruption

    with pytest.raises(SyntheticInterruption):
        _orchestrator(boundary, operation_factory).run(
            source_snapshot_root=snapshot.root,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
            after_checkpoint=interrupt,
        )
    checkpoint_path = next(
        (boundary.active_root / "data" / "vault" / ".staging" / "foundation_runs").glob(
            "*/checkpoint.json"
        )
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    interval = next(iter(checkpoint["completed"]["intervals"].values()))
    raw_root = boundary.active_root / interval["raw"]["relative_root"]
    bars_path = next(raw_root.glob("raw/*/*/*/bars.parquet"))
    bars_path.write_bytes(bars_path.read_bytes() + b"tamper")
    with pytest.raises(IntegrityError):
        _orchestrator(boundary, operation_factory).run(
            source_snapshot_root=snapshot.root,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
        )


def test_resume_rejects_rehashed_checkpoint_with_wrong_upstream_receipt(
    boundary, operation_factory
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)

    def interrupt(phase: str) -> None:
        if phase.endswith(":causal"):
            raise SyntheticInterruption

    with pytest.raises(SyntheticInterruption):
        _orchestrator(boundary, operation_factory).run(
            source_snapshot_root=snapshot.root,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
            after_checkpoint=interrupt,
        )
    checkpoint_path = next(
        (boundary.active_root / "data" / "vault" / ".staging" / "foundation_runs").glob(
            "*/checkpoint.json"
        )
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    interval = next(iter(checkpoint["completed"]["intervals"].values()))
    interval["raw"] = dict(interval["causal"])
    core = {key: checkpoint[key] for key in checkpoint if key != "checkpoint_id"}
    checkpoint["checkpoint_id"] = sha256_json(core)
    checkpoint_path.write_bytes(canonical_bytes(checkpoint) + b"\n")
    with pytest.raises(IntegrityError):
        _orchestrator(boundary, operation_factory).run(
            source_snapshot_root=snapshot.root,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
        )


def test_status_eligibility_phase_is_restart_safe_and_tamper_evident(
    boundary, operation_factory
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)

    def interrupt(phase: str) -> None:
        if phase.endswith(":status_eligibility"):
            raise SyntheticInterruption

    with pytest.raises(SyntheticInterruption):
        _orchestrator(boundary, operation_factory).run(
            source_snapshot_root=snapshot.root,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
            after_checkpoint=interrupt,
        )
    checkpoint_path = next(
        (boundary.active_root / "data" / "vault" / ".staging" / "foundation_runs").glob(
            "*/checkpoint.json"
        )
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    interval = next(iter(checkpoint["completed"]["intervals"].values()))
    status_receipt = VerifiedReleaseReceipt.from_dict(interval["status_eligibility"])
    rows_path = (
        boundary.active_root
        / status_receipt.relative_root
        / "status_eligibility_rows.jsonl"
    )
    rows_path.write_bytes(rows_path.read_bytes() + b"poison\n")
    with pytest.raises(IntegrityError):
        _orchestrator(boundary, operation_factory).run(
            source_snapshot_root=snapshot.root,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
        )


def test_statistics_ledger_release_is_restart_safe_and_tamper_evident(
    boundary, operation_factory
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)

    def interrupt(phase: str) -> None:
        if phase == "market_state":
            raise SyntheticInterruption

    with pytest.raises(SyntheticInterruption):
        _orchestrator(boundary, operation_factory).run(
            source_snapshot_root=snapshot.root,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
            after_checkpoint=interrupt,
        )
    checkpoint_path = next(
        (boundary.active_root / "data" / "vault" / ".staging" / "foundation_runs").glob(
            "*/checkpoint.json"
        )
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    market_state = VerifiedReleaseReceipt.from_dict(
        checkpoint["completed"]["market_state"]
    )
    ledger_path = next(
        (boundary.active_root / market_state.relative_root / "l").glob("*/*/*.jsonl")
    )
    ledger_path.write_bytes(ledger_path.read_bytes() + b"poison\n")
    with pytest.raises(IntegrityError):
        _orchestrator(boundary, operation_factory).run(
            source_snapshot_root=snapshot.root,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
        )


def test_outcome_source_release_is_physically_separate_and_tamper_evident(
    boundary, operation_factory
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)
    result = _orchestrator(boundary, operation_factory).run(
        source_snapshot_root=snapshot.root,
        source_selection_receipt=selection_receipt,
        feature_spec=spec,
    )
    foundation_set = load_foundation_set(
        result.foundation_set_receipt, boundary=boundary
    )
    interval = foundation_set["intervals"][0]
    outcome_receipt = VerifiedReleaseReceipt.from_dict(
        interval["outcome_source_input_release_receipt"]
    )
    feature_receipt = VerifiedReleaseReceipt.from_dict(
        interval["feature_input_release_receipt"]
    )
    assert outcome_receipt.release_id != feature_receipt.release_id
    outcome_path = (
        boundary.active_root
        / outcome_receipt.relative_root
        / "outcome_source_input.json"
    )
    outcome_path.write_bytes(outcome_path.read_bytes() + b"tamper")
    with pytest.raises(IntegrityError):
        load_outcome_source_input(outcome_receipt, boundary=boundary)


def test_orchestrator_has_no_research_model_outcome_or_provider_execution_imports() -> None:
    path = REPO / "src" / "futures_rebuild" / "foundation" / "orchestrator.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: set[str] = set()
    imported_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            modules.add(node.module or "")
            imported_names.update(alias.name for alias in node.names)
    assert not any(
        token in module
        for module in modules
        for token in ("historical_", ".research", ".inference", "requests", "urllib")
    )
    assert {
        "generate_causal_outcomes",
        "publish_causal_outcome_release",
        "PredictionLedger",
    }.isdisjoint(imported_names)
