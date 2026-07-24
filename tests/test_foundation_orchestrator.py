from __future__ import annotations

import ast
import json
from collections import namedtuple
from pathlib import Path
from types import SimpleNamespace

import databento_dbn as dbn
import pytest

import futures_rebuild.foundation.market_state as market_state_module
import futures_rebuild.foundation.orchestrator as orchestrator_module
import futures_rebuild.foundation.resources as resource_module
import futures_rebuild.foundation.selection as selection_module
import futures_rebuild.producer_bridge as producer_bridge_module
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.foundation.orchestrator import (
    OUTCOME_DEFERRED_UNTIL,
    OUTCOME_SOURCE_ROLE,
    FoundationOrchestrator,
    load_feature_source_input,
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
from futures_rebuild.data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)
from futures_rebuild.source_symbology import build_query_contract


REPO = Path(__file__).resolve().parents[1]
START_NS = 1_704_067_200_000_000_000
END_NS = 1_735_689_600_000_000_000


class SyntheticInterruption(RuntimeError):
    pass


DiskUsage = namedtuple("usage", "total used free")


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
        ts_event=START_NS + 1,
        ts_recv=START_NS,
        activation=START_NS - 86_400_000_000_000,
        expiration=END_NS,
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
        "foundation_resource_policy.json",
        "known_anomalies.json",
        "mechanical_feature_spec.json",
        "provider_data_epochs.json",
        "session_policy.json",
        "status_research_scope_policy.json",
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
    synthetic_scope = {
        "alignment": (
            "FIRST_COMPLETE_UTC_YEAR_AFTER_PROVIDER_STATUS_LAUNCH_MONTH"
        ),
        "policy_version": "1.0.0",
        "pre_scope_disposition": "ABSTAIN_PRE_STATUS_CAPABILITY_EPOCH",
        "provider_status_launch_month": "2023-07",
        "require_all_selected_intervals_at_or_after_start": True,
        "research_interval_start": "2024-01-01",
        "source_urls": [
            "https://databento.com/blog/status-schema-cme",
            "https://databento.com/docs/schemas-and-data-formats/status",
        ],
    }
    (
        boundary.active_root / "configs" / "status_research_scope_policy.json"
    ).write_bytes(canonical_bytes(synthetic_scope) + b"\n")


def _snapshot(boundary, operation_factory) -> PublishedSourceSnapshot:
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
    publisher = AtomicPublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "dbn.lock",
    )
    stage = publisher.create_stage("dbn")
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    for relative, content in payloads.items():
        target = stage / Path(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        logical = f"data/{relative}"
        logical_paths[relative] = logical
        staged_paths[logical] = relative
    manifest = DataReleaseManifest.build(
        stage,
        phase="dbn",
        release_kind="futures_phase1a_verified_dbn",
        schema_version="1.0.0",
        logical_paths=logical_paths,
    )
    manifest_path = publisher.publish(
        stage, manifest, staged_paths=staged_paths
    )
    return PublishedSourceSnapshot.open(manifest_path, boundary=boundary)


def _selection_receipt(
    boundary, operation_factory, snapshot, *, false_continuous_status: bool = False
):
    publisher = AtomicPublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "selection.lock",
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
        "source_scope": "VERIFIED_LAYOUT_V2_DBN_RELEASE",
        "known_anomalies_sha256": sha256_file(
            boundary.active_root / "configs" / "known_anomalies.json"
        ),
        "source_dbn_files_index_sha256": snapshot.files_index_sha256,
        "source_dbn_manifest_sha256": snapshot.source_manifest_sha256,
        "source_dbn_release_id": snapshot.source_release_id,
    }
    selection = {**core, "selection_manifest_id": sha256_json(core)}
    return publish_source_selection(
        selection, snapshot=snapshot, publisher=publisher
    )


def _setup(boundary, operation_factory):
    _copy_configs(boundary)
    snapshot = _snapshot(boundary, operation_factory)
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


def test_new_run_resolves_selection_once_and_capacity_failure_writes_no_checkpoint(
    boundary, operation_factory, monkeypatch
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)
    original = selection_module.resolve_foundation_selection
    calls = 0

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(selection_module, "resolve_foundation_selection", counted)
    result = _orchestrator(boundary, operation_factory).run(
        source_dbn_manifest=snapshot.manifest_path,
        source_selection_receipt=selection_receipt,
        feature_spec=spec,
    )
    assert result.completed_phase_count == 12
    assert calls == 1

    other = FoundationOrchestrator(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        batch_rows=2,
    )
    monkeypatch.setattr(
        resource_module.shutil,
        "disk_usage",
        lambda _path: DiskUsage(1_000_000_000_000, 0, 1),
    )
    run_root = boundary.active_root / "state" / "foundation_runs_v2"
    before = {path.name for path in run_root.iterdir()} if run_root.exists() else set()
    with pytest.raises(IntegrityError, match="capacity admission"):
        other.run(
            source_dbn_manifest=snapshot.manifest_path,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
        )
    after = {path.name for path in run_root.iterdir()} if run_root.exists() else set()
    assert after == before


def test_new_interval_uses_one_bounded_columnar_economics_scan(
    boundary, operation_factory, monkeypatch
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)
    original = producer_bridge_module._iter_causal_rows
    scans = 0

    def counted(*args, **kwargs):
        nonlocal scans
        scans += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(producer_bridge_module, "_iter_causal_rows", counted)
    result = _orchestrator(boundary, operation_factory).run(
        source_dbn_manifest=snapshot.manifest_path,
        source_selection_receipt=selection_receipt,
        feature_spec=spec,
    )

    assert result.as_dict()["status"] == "COMPLETE_DEPENDENCY_CLOSED_NON_ALPHA"
    # The immutable causal receipt is the row-level authority. Economics scans
    # once; compact status and feature contracts do not rescan all causal rows.
    assert scans == 1


def test_new_interval_never_materializes_record_batches_as_row_dicts(
    boundary, operation_factory, monkeypatch
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)
    original_parquet_file = producer_bridge_module.pq.ParquetFile

    class BatchWithoutRowDictMaterialization:
        def __init__(self, batch) -> None:
            self._batch = batch

        def __getattr__(self, name):
            return getattr(self._batch, name)

        @property
        def num_rows(self):
            return self._batch.num_rows

        def column(self, index):
            return self._batch.column(index)

        def to_pylist(self):
            raise AssertionError("record batch was materialized as row dictionaries")

    class ParquetFileWithoutRowDictMaterialization:
        def __init__(self, path) -> None:
            self._parquet = original_parquet_file(path)

        def __getattr__(self, name):
            return getattr(self._parquet, name)

        @property
        def schema_arrow(self):
            return self._parquet.schema_arrow

        def iter_batches(self, *args, **kwargs):
            for batch in self._parquet.iter_batches(*args, **kwargs):
                yield BatchWithoutRowDictMaterialization(batch)

    monkeypatch.setattr(
        producer_bridge_module,
        "pq",
        SimpleNamespace(ParquetFile=ParquetFileWithoutRowDictMaterialization),
    )
    result = _orchestrator(boundary, operation_factory).run(
        source_dbn_manifest=snapshot.manifest_path,
        source_selection_receipt=selection_receipt,
        feature_spec=spec,
    )

    assert result.as_dict()["status"] == "COMPLETE_DEPENDENCY_CLOSED_NON_ALPHA"


def test_foundation_defers_feature_row_materialization(
    boundary, operation_factory, monkeypatch
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)

    def forbidden_feature_row_parse(*args, **kwargs):
        raise AssertionError("newly published features rebuilt full FeatureRow objects")

    monkeypatch.setattr(
        producer_bridge_module,
        "_parse_feature_row",
        forbidden_feature_row_parse,
    )
    result = _orchestrator(boundary, operation_factory).run(
        source_dbn_manifest=snapshot.manifest_path,
        source_selection_receipt=selection_receipt,
        feature_spec=spec,
    )

    assert result.as_dict()["status"] == "COMPLETE_DEPENDENCY_CLOSED_NON_ALPHA"
    foundation = load_foundation_set(
        result.foundation_set_receipt, boundary=boundary
    )
    feature_receipt = VerifiedReleaseReceipt.from_dict(
        foundation["intervals"][0]["feature_input_release_receipt"]
    )
    manifest = feature_receipt.verify(boundary)
    assert [Path(item.logical_path).name for item in manifest.files] == [
        "feature_source_input.json"
    ]
    feature_input = load_feature_source_input(
        feature_receipt, boundary=boundary
    )
    assert feature_input["features_materialized"] is False
    assert feature_input["fit_or_global_state"] is False
    assert feature_input["uses_future_outcome"] is False
    assert feature_input["prediction_ledger_read"] is False


def test_feature_source_input_is_tamper_evident(
    boundary, operation_factory
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)
    result = _orchestrator(boundary, operation_factory).run(
        source_dbn_manifest=snapshot.manifest_path,
        source_selection_receipt=selection_receipt,
        feature_spec=spec,
    )
    foundation = load_foundation_set(
        result.foundation_set_receipt, boundary=boundary
    )
    feature_receipt = VerifiedReleaseReceipt.from_dict(
        foundation["intervals"][0]["feature_input_release_receipt"]
    )
    path = feature_receipt.resolve_unique_filename(
        "feature_source_input.json", boundary
    )
    path.write_bytes(path.read_bytes() + b"tamper")

    with pytest.raises(IntegrityError):
        load_feature_source_input(feature_receipt, boundary=boundary)


def test_selected_query_contract_must_match_exact_dbn_header(
    boundary, operation_factory
) -> None:
    _copy_configs(boundary)
    snapshot = _snapshot(boundary, operation_factory)
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
            source_dbn_manifest=snapshot.manifest_path,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
        )


def test_foundation_run_resumes_after_durable_phase_and_is_idempotent(
    boundary, operation_factory, monkeypatch
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)
    phases: list[str] = []

    def interrupt(phase: str) -> None:
        phases.append(phase)
        if phase.endswith(":causal"):
            raise SyntheticInterruption("synthetic stop after durable causal checkpoint")

    with pytest.raises(SyntheticInterruption):
        _orchestrator(boundary, operation_factory).run(
            source_dbn_manifest=snapshot.manifest_path,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
            after_checkpoint=interrupt,
        )
    assert any(phase.endswith(":causal") for phase in phases)
    checkpoint_paths = list(
        (boundary.active_root / "state" / "foundation_runs_v2").glob(
            "*/checkpoint.json"
        )
    )
    assert len(checkpoint_paths) == 1
    partial = json.loads(checkpoint_paths[0].read_text(encoding="utf-8"))
    assert partial["status"] == "RUNNING"
    assert partial["completed"]["intervals"]

    def forbidden_full_market_state_revalidation(*args, **kwargs):
        raise AssertionError("durably checkpointed market state was fully revalidated")

    monkeypatch.setattr(
        market_state_module,
        "_validate_output_rows",
        forbidden_full_market_state_revalidation,
    )
    result = _orchestrator(boundary, operation_factory).run(
        source_dbn_manifest=snapshot.manifest_path,
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
    feature_input = load_feature_source_input(feature_receipt, boundary=boundary)
    assert feature_input["features_materialized"] is False
    assert feature_input["feature_ready_rows"] == interval["feature_ready_rows"]
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

    manifest_root = boundary.active_root / "manifests" / "data_releases"
    releases_before = sorted(
        path.relative_to(manifest_root).as_posix()
        for path in manifest_root.rglob("*.json")
    )
    repeated = _orchestrator(boundary, operation_factory).run(
        source_dbn_manifest=snapshot.manifest_path,
        source_selection_receipt=selection_receipt,
        feature_spec=spec,
    )
    releases_after = sorted(
        path.relative_to(manifest_root).as_posix()
        for path in manifest_root.rglob("*.json")
    )
    assert repeated.run_id == result.run_id
    assert repeated.foundation_set_receipt == result.foundation_set_receipt
    assert releases_after == releases_before


def test_market_state_source_checkpoint_survives_late_kill_without_redecode(
    boundary, operation_factory, monkeypatch
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)
    original_checkpoint = market_state_module._write_market_state_progress
    checkpointed: list[str] = []

    def interrupt_after_first_checkpoint(**kwargs):
        output = original_checkpoint(**kwargs)
        if not checkpointed:
            checkpointed.append(str(output["output_path"]))
            raise SyntheticInterruption(
                "synthetic late kill after durable market-state source checkpoint"
            )
        return output

    monkeypatch.setattr(
        market_state_module,
        "_write_market_state_progress",
        interrupt_after_first_checkpoint,
    )
    with pytest.raises(SyntheticInterruption):
        _orchestrator(boundary, operation_factory).run(
            source_dbn_manifest=snapshot.manifest_path,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
        )
    assert len(checkpointed) == 1
    progress = list(
        (boundary.active_root / "state" / "msr").glob(
            "*/progress/status/*.json"
        )
    )
    assert len(progress) == 1

    monkeypatch.setattr(
        market_state_module,
        "_write_market_state_progress",
        original_checkpoint,
    )

    def forbidden_status_redecode(*_args, **_kwargs):
        raise AssertionError("checkpointed status source was decoded again")

    monkeypatch.setattr(
        market_state_module, "iter_statuses", forbidden_status_redecode
    )
    result = _orchestrator(boundary, operation_factory).run(
        source_dbn_manifest=snapshot.manifest_path,
        source_selection_receipt=selection_receipt,
        feature_spec=spec,
    )
    assert result.completed_phase_count == 12
    assert not list(
        (boundary.active_root / "state" / "data_publication_staging").glob(
            "market_state_foundation-*"
        )
    )


def test_market_state_source_checkpoint_rejects_staged_output_tampering(
    boundary, operation_factory, monkeypatch
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)
    original_checkpoint = market_state_module._write_market_state_progress
    interrupted = False

    def interrupt_after_checkpoint(**kwargs):
        nonlocal interrupted
        output = original_checkpoint(**kwargs)
        if not interrupted:
            interrupted = True
            raise SyntheticInterruption
        return output

    monkeypatch.setattr(
        market_state_module,
        "_write_market_state_progress",
        interrupt_after_checkpoint,
    )
    with pytest.raises(SyntheticInterruption):
        _orchestrator(boundary, operation_factory).run(
            source_dbn_manifest=snapshot.manifest_path,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
        )
    stage = next(
        (boundary.active_root / "state" / "data_publication_staging").glob(
            "market_state_foundation-*"
        )
    )
    status_output = next((stage / "data" / "market_state" / "status").rglob("*.jsonl"))
    tampered = bytearray(status_output.read_bytes())
    tampered[0] = ord("[")
    status_output.write_bytes(tampered)
    monkeypatch.setattr(
        market_state_module,
        "_write_market_state_progress",
        original_checkpoint,
    )
    with pytest.raises(IntegrityError, match="fresh stage manifest"):
        _orchestrator(boundary, operation_factory).run(
            source_dbn_manifest=snapshot.manifest_path,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
        )


def test_market_state_quarantines_proven_interrupted_statistics_prefix(
    boundary, operation_factory, monkeypatch
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)
    original_checkpoint = market_state_module._write_market_state_progress
    interrupted = False

    def interrupt_after_status_checkpoint(**kwargs):
        nonlocal interrupted
        output = original_checkpoint(**kwargs)
        if not interrupted:
            interrupted = True
            raise SyntheticInterruption
        return output

    monkeypatch.setattr(
        market_state_module,
        "_write_market_state_progress",
        interrupt_after_status_checkpoint,
    )
    with pytest.raises(SyntheticInterruption):
        _orchestrator(boundary, operation_factory).run(
            source_dbn_manifest=snapshot.manifest_path,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
        )
    _, resolved = selection_module.load_source_selection_with_resolution(
        selection_receipt,
        snapshot=snapshot,
        boundary=boundary,
    )
    item = resolved.statistics_files[0]
    record = next(
        market_state_module.iter_statistics(
            item.binding,
            market=item.market,
            expected_query_contract=item.query_contract,
            batch_rows=1,
        )
    )
    stage = next(
        (boundary.active_root / "state" / "data_publication_staging").glob(
            "market_state_foundation-*"
        )
    )
    output_path = market_state_module._output_path("statistics", item)
    raw = stage / output_path
    ledger = stage / (
        output_path.removesuffix(".jsonl") + ".ledger.jsonl"
    )
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_bytes(canonical_bytes(record.as_dict()) + b"\n")
    ledger.write_bytes(b"")

    monkeypatch.setattr(
        market_state_module,
        "_write_market_state_progress",
        original_checkpoint,
    )
    result = _orchestrator(boundary, operation_factory).run(
        source_dbn_manifest=snapshot.manifest_path,
        source_selection_receipt=selection_receipt,
        feature_spec=spec,
    )
    assert result.completed_phase_count == 12
    quarantine_documents = list(
        (boundary.active_root / "state" / "msr").glob(
            "*/q/*/quarantine.json"
        )
    )
    assert len(quarantine_documents) == 1
    quarantine_root = quarantine_documents[0].parent
    assert (quarantine_root / "raw.interrupted.jsonl").read_bytes() == (
        canonical_bytes(record.as_dict()) + b"\n"
    )
    assert (quarantine_root / "ledger.interrupted.jsonl").read_bytes() == b""


def test_resume_rejects_checkpoint_and_accepted_release_tampering(
    boundary, operation_factory
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)

    def interrupt(phase: str) -> None:
        if phase.endswith(":causal"):
            raise SyntheticInterruption

    with pytest.raises(SyntheticInterruption):
        _orchestrator(boundary, operation_factory).run(
            source_dbn_manifest=snapshot.manifest_path,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
            after_checkpoint=interrupt,
        )
    checkpoint_path = next(
        (boundary.active_root / "state" / "foundation_runs_v2").glob(
            "*/checkpoint.json"
        )
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    interval = next(iter(checkpoint["completed"]["intervals"].values()))
    raw_receipt = VerifiedReleaseReceipt.from_dict(interval["raw"])
    bars_path = raw_receipt.resolve_unique_filename("bars.parquet", boundary)
    bars_path.write_bytes(bars_path.read_bytes() + b"tamper")
    with pytest.raises(IntegrityError):
        _orchestrator(boundary, operation_factory).run(
            source_dbn_manifest=snapshot.manifest_path,
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
            source_dbn_manifest=snapshot.manifest_path,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
            after_checkpoint=interrupt,
        )
    checkpoint_path = next(
        (boundary.active_root / "state" / "foundation_runs_v2").glob(
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
            source_dbn_manifest=snapshot.manifest_path,
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
            source_dbn_manifest=snapshot.manifest_path,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
            after_checkpoint=interrupt,
        )
    checkpoint_path = next(
        (boundary.active_root / "state" / "foundation_runs_v2").glob(
            "*/checkpoint.json"
        )
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    interval = next(iter(checkpoint["completed"]["intervals"].values()))
    status_receipt = VerifiedReleaseReceipt.from_dict(interval["status_eligibility"])
    rows_path = status_receipt.resolve_unique_filename(
        "status_eligible_keys.parquet", boundary
    )
    rows_path.write_bytes(rows_path.read_bytes() + b"poison\n")
    with pytest.raises(IntegrityError):
        _orchestrator(boundary, operation_factory).run(
            source_dbn_manifest=snapshot.manifest_path,
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
            source_dbn_manifest=snapshot.manifest_path,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
            after_checkpoint=interrupt,
        )
    checkpoint_path = next(
        (boundary.active_root / "state" / "foundation_runs_v2").glob(
            "*/checkpoint.json"
        )
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    market_state = VerifiedReleaseReceipt.from_dict(
        checkpoint["completed"]["market_state"]
    )
    market_state_manifest = market_state.verify(boundary)
    ledger_entry = next(
        item
        for item in market_state_manifest.files
        if item.logical_path.endswith(".jsonl")
    )
    ledger_path = market_state.resolve_file(ledger_entry.logical_path, boundary)
    ledger_path.write_bytes(ledger_path.read_bytes() + b"poison\n")
    with pytest.raises(IntegrityError):
        _orchestrator(boundary, operation_factory).run(
            source_dbn_manifest=snapshot.manifest_path,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
        )


def test_market_state_publication_is_adopted_after_precheckpoint_kill(
    boundary, operation_factory, monkeypatch
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)
    original_load = orchestrator_module.load_market_state_foundation
    interrupted = False

    def interrupt_first_load(*args, **kwargs):
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise SyntheticInterruption("synthetic kill after commit-last publication")
        return original_load(*args, **kwargs)

    monkeypatch.setattr(
        orchestrator_module, "load_market_state_foundation", interrupt_first_load
    )
    with pytest.raises(SyntheticInterruption):
        _orchestrator(boundary, operation_factory).run(
            source_dbn_manifest=snapshot.manifest_path,
            source_selection_receipt=selection_receipt,
            feature_spec=spec,
        )
    checkpoint_path = next(
        (boundary.active_root / "state" / "foundation_runs_v2").glob(
            "*/checkpoint.json"
        )
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert "market_state" not in checkpoint["completed"]
    assert len(
        list(
            (
                boundary.active_root
                / "manifests"
                / "data_releases"
                / "market_state"
            ).glob("*.json")
        )
    ) == 1
    assert not list(
        (boundary.active_root / "state" / "data_publication_staging").glob(
            "market_state_foundation-*"
        )
    )

    monkeypatch.setattr(
        orchestrator_module, "load_market_state_foundation", original_load
    )

    def forbidden_decode(*_args, **_kwargs):
        raise AssertionError("published market-state bytes were decoded again")

    monkeypatch.setattr(market_state_module, "iter_statuses", forbidden_decode)
    monkeypatch.setattr(market_state_module, "iter_statistics", forbidden_decode)
    result = _orchestrator(boundary, operation_factory).run(
        source_dbn_manifest=snapshot.manifest_path,
        source_selection_receipt=selection_receipt,
        feature_spec=spec,
    )
    assert result.as_dict()["status"] == "COMPLETE_DEPENDENCY_CLOSED_NON_ALPHA"


def test_outcome_source_release_is_physically_separate_and_tamper_evident(
    boundary, operation_factory
) -> None:
    snapshot, selection_receipt, spec = _setup(boundary, operation_factory)
    result = _orchestrator(boundary, operation_factory).run(
        source_dbn_manifest=snapshot.manifest_path,
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
    outcome_path = outcome_receipt.resolve_unique_filename(
        "outcome_source_input.json", boundary
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


def test_market_state_verification_is_linear_in_release_size() -> None:
    path = REPO / "src" / "futures_rebuild" / "foundation" / "market_state.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    loader = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "load_market_state_foundation"
    )
    publisher = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "publish_market_state_foundation"
    )
    loaded_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        and node.name == "LoadedMarketStateFoundation"
    )
    status_iterator = next(
        node
        for node in loaded_class.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "iter_status_records"
    )

    receipt_verify_calls = [
        node
        for node in ast.walk(loader)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "verify"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "receipt"
    ]
    assert len(receipt_verify_calls) == 1
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"embedded_document", "resolve_file"}
        for node in ast.walk(loader)
    )
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "load_market_state_foundation"
        for node in ast.walk(publisher)
    )
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "resolve_file"
        for node in ast.walk(status_iterator)
    )
