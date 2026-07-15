from __future__ import annotations

from dataclasses import asdict, replace
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.clock import SyntheticClock
from futures_rebuild.errors import ContractError, IntegrityError
from futures_rebuild.foundation.materialize import (
    RAW_RELEASE_KIND,
    RAW_SCHEMA_VERSION,
    materialize_causal_interval,
)
from futures_rebuild.foundation.parquet import DEFINITION_SCHEMA, RAW_BAR_SCHEMA
from futures_rebuild.foundation.records import ProviderBar, ProviderDefinition
from futures_rebuild.foundation.support import (
    VerifiedFoundationPolicies,
    publish_foundation_policies,
)
from futures_rebuild.ledger import LedgerHeadContract, PredictionLedger
from futures_rebuild.producer_bridge import (
    CAUSAL_OUTCOME_LABEL_METHOD_ID,
    CausalOutcomeContext,
    CausalFeatureSpec,
    generate_causal_outcomes,
    load_actual_contract_definitions,
    load_actual_contract_economics,
    load_causal_feature_release,
    load_causal_outcome_release,
    load_outcome_release,
    load_versioned_session_policy,
    publish_actual_contract_definitions,
    publish_actual_contract_economics,
    publish_causal_feature_release,
    publish_causal_outcome_release,
    publish_outcome_release,
    publish_versioned_session_policy,
)
from futures_rebuild.release import (
    AtomicPublisher,
    ReleaseManifest,
    VerifiedReleaseReceipt,
)
from futures_rebuild.source_symbology import build_query_contract
from futures_rebuild.schemas import (
    OutcomeRow,
    OutcomeStatus,
    PredictionRow,
    prediction_id_for,
)


REPO = Path(__file__).resolve().parents[1]
EVENT_NS = 1_704_067_200_000_000_000
SOURCE_SNAPSHOT_ID = "a" * 64
SOURCE_MANIFEST_SHA256 = "b" * 64
DEFINITION_FILE_SHA256 = "c" * 64
BAR_FILE_SHA256 = "d" * 64
DEFINITION_ROW_SHA256 = "e" * 64


def _publisher(boundary, operation_factory) -> AtomicPublisher:
    return AtomicPublisher(
        boundary.active_root
        / "data"
        / "vault"
        / ".staging"
        / "releases"
        / "producer-bridge",
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / "producer-bridge.lock",
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
    )


def _copy_active_configs(boundary) -> None:
    for name in (
        "contract_economics_rules.json",
        "environment.lock.json",
        "foundation_policy.json",
        "known_anomalies.json",
        "session_policy.json",
    ):
        (boundary.active_root / "configs" / name).write_bytes(
            (REPO / "configs" / name).read_bytes()
        )


def _write_parquet(path: Path, schema: pa.Schema, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=schema),
        path,
        compression="zstd",
        compression_level=9,
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
        data_page_version="2.0",
        use_deprecated_int96_timestamps=False,
    )


def _publish_raw_interval(
    boundary,
    publisher: AtomicPublisher,
    *,
    label_path: bool = False,
    omit_minute: int | None = None,
    roll_minute: int | None = None,
) -> VerifiedReleaseReceipt:
    def provider_definition(
        instrument_id: int, raw_symbol: str, row_sha256: str
    ) -> ProviderDefinition:
        return ProviderDefinition(
            dataset="GLBX.MDP3",
            market="ES",
            publisher_id=1,
            instrument_id=instrument_id,
            ts_event_ns=EVENT_NS,
            ts_recv_ns=EVENT_NS + 1,
            raw_symbol=raw_symbol,
            exchange="XCME",
            currency="USD",
            min_price_increment_nano=250_000_000,
            unit_of_measure_qty_nano=50_000_000_000,
            unit_of_measure="IPNT",
            source_release_id=SOURCE_SNAPSHOT_ID,
            source_manifest_sha256=SOURCE_MANIFEST_SHA256,
            source_file_path=(
                "dbn/definition/ES/2024/2024-01-01_2025-01-01.dbn.zst"
            ),
            source_file_sha256=DEFINITION_FILE_SHA256,
            row_sha256=row_sha256,
        )

    definition = provider_definition(123, "ESZ4", DEFINITION_ROW_SHA256)
    definitions = [definition]
    if roll_minute is not None:
        definitions.append(provider_definition(456, "ESH5", "8" * 64))

    if label_path:
        bars = []
        for minute in range(7):
            if minute == omit_minute:
                continue
            instrument_id = (
                456
                if roll_minute is not None and minute >= roll_minute
                else 123
            )
            opening = 5_000_000_000_000 + minute * 250_000_000
            bars.append(
                ProviderBar(
                    dataset="GLBX.MDP3",
                    market="ES",
                    publisher_id=1,
                    instrument_id=instrument_id,
                    event_at_ns=EVENT_NS + minute * 60_000_000_000,
                    open_nano=opening,
                    high_nano=opening + 500_000_000,
                    low_nano=opening - 500_000_000,
                    close_nano=opening + 250_000_000,
                    volume=100 + minute,
                    source_release_id=SOURCE_SNAPSHOT_ID,
                    source_manifest_sha256=SOURCE_MANIFEST_SHA256,
                    source_file_path=(
                        "dbn/ohlcv_1m/ES/2024/2024-01-01_2025-01-01.dbn.zst"
                    ),
                    source_file_sha256=BAR_FILE_SHA256,
                    row_sha256=sha256_json(
                        {"instrument_id": instrument_id, "minute": minute}
                    ),
                )
            )
    else:
        bars = [
            ProviderBar(
                dataset="GLBX.MDP3",
                market="ES",
                publisher_id=1,
                instrument_id=instrument_id,
                event_at_ns=EVENT_NS,
                open_nano=5_000_000_000_000,
                high_nano=5_001_000_000_000,
                low_nano=4_999_000_000_000,
                close_nano=5_000_500_000_000,
                volume=100 + index,
                source_release_id=SOURCE_SNAPSHOT_ID,
                source_manifest_sha256=SOURCE_MANIFEST_SHA256,
                source_file_path=(
                    "dbn/ohlcv_1m/ES/2024/2024-01-01_2025-01-01.dbn.zst"
                ),
                source_file_sha256=BAR_FILE_SHA256,
                row_sha256=row_hash,
            )
            for index, (instrument_id, row_hash) in enumerate(
                ((123, "f" * 64), (999, "9" * 64))
            )
        ]
    logical_root = "raw/ES/2024/2024-01-01_2025-01-01"
    stage = publisher.create_stage("synthetic_raw_interval")
    bars_path = stage / logical_root / "bars.parquet"
    definitions_path = stage / logical_root / "definitions.parquet"
    _write_parquet(bars_path, RAW_BAR_SCHEMA, [asdict(row) for row in bars])
    _write_parquet(
        definitions_path, DEFINITION_SCHEMA, [asdict(item) for item in definitions]
    )
    definition_query = build_query_contract(
        schema="definition",
        market="ES",
        start="2024-01-01",
        end="2025-01-01",
        stype_in="parent",
        symbols=["ES.FUT"],
    )
    bar_query = build_query_contract(
        schema="ohlcv-1m",
        market="ES",
        start="2024-01-01",
        end="2025-01-01",
        stype_in="continuous",
        symbols=["ES.v.0"],
    )
    core = {
        "bar_query_contract": bar_query,
        "bar_query_contract_id": bar_query["query_contract_id"],
        "bar_rows": len(bars),
        "bars_parquet_sha256": sha256_file(bars_path),
        "bars_schema": RAW_BAR_SCHEMA.metadata[b"schema_id"].decode("ascii"),
        "definition_rows_scanned": len(definitions),
        "definition_rows_selected": len(definitions),
        "definition_query_contract": definition_query,
        "definition_query_contract_id": definition_query["query_contract_id"],
        "definitions_parquet_sha256": sha256_file(definitions_path),
        "definitions_schema": DEFINITION_SCHEMA.metadata[b"schema_id"].decode(
            "ascii"
        ),
        "foundation_transforms": [
            "EXACT_DBN_DECODE",
            "ACTUAL_BAR_INSTRUMENT_ID_SELECTION",
            "NANOUNITS_PRESERVED_AS_INT64",
            "UTC_NANOSECONDS_PRESERVED_AS_INT64",
        ],
        "learned_or_outcome_informed_transform_count": 0,
        "logical_root": logical_root,
        "market": "ES",
        "source_bar_file_path": bars[0].source_file_path,
        "source_bar_file_sha256": BAR_FILE_SHA256,
        "source_definition_file_path": definition.source_file_path,
        "source_definition_file_sha256": DEFINITION_FILE_SHA256,
        "source_manifest_sha256": SOURCE_MANIFEST_SHA256,
        "source_selection_release_id": "1" * 64,
        "source_snapshot_id": SOURCE_SNAPSHOT_ID,
        "year": 2024,
    }
    interval_receipt = {**core, "interval_id": sha256_json(core)}
    (stage / logical_root / "interval_receipt.json").write_bytes(
        canonical_bytes(interval_receipt) + b"\n"
    )
    manifest = ReleaseManifest.build(
        stage,
        release_kind=RAW_RELEASE_KIND,
        schema_version=RAW_SCHEMA_VERSION,
        source_release_ids=(SOURCE_SNAPSHOT_ID, "1" * 64),
        metadata={
            "interval_id": interval_receipt["interval_id"],
            "logical_root": logical_root,
            "market": "ES",
            "year": 2024,
        },
    )
    release = publisher.publish(stage, manifest)
    return VerifiedReleaseReceipt.from_release(release, boundary)


def _foundation_chain(
    boundary,
    operation_factory,
    *,
    label_path: bool = False,
    omit_minute: int | None = None,
    roll_minute: int | None = None,
):
    _copy_active_configs(boundary)
    publisher = _publisher(boundary, operation_factory)
    policy_receipt = publish_foundation_policies(
        boundary=boundary,
        publisher=publisher,
        config_root=boundary.active_root / "configs",
    )
    policies = VerifiedFoundationPolicies.from_release(
        policy_receipt, boundary=boundary
    )
    raw_receipt = _publish_raw_interval(
        boundary,
        publisher,
        label_path=label_path,
        omit_minute=omit_minute,
        roll_minute=roll_minute,
    )
    causal_receipt = materialize_causal_interval(
        raw_receipt=raw_receipt,
        policies=policies,
        publisher=publisher,
        batch_rows=1,
    )
    return publisher, policies, raw_receipt, causal_receipt


def _bridge_chain(
    boundary,
    operation_factory,
    *,
    label_path: bool = False,
    omit_minute: int | None = None,
    roll_minute: int | None = None,
):
    publisher, policies, raw_receipt, causal_receipt = _foundation_chain(
        boundary,
        operation_factory,
        label_path=label_path,
        omit_minute=omit_minute,
        roll_minute=roll_minute,
    )
    session_receipt = publish_versioned_session_policy(
        policies=policies, boundary=boundary, publisher=publisher
    )
    session_policy = load_versioned_session_policy(
        session_receipt, policies=policies, boundary=boundary
    )
    definition_receipt = publish_actual_contract_definitions(
        raw_receipt=raw_receipt,
        policies=policies,
        boundary=boundary,
        publisher=publisher,
    )
    definitions = load_actual_contract_definitions(
        definition_receipt,
        raw_receipt=raw_receipt,
        policies=policies,
        boundary=boundary,
    )
    economics_receipt = publish_actual_contract_economics(
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
        publisher=publisher,
    )
    economics = load_actual_contract_economics(
        economics_receipt,
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    )
    return (
        publisher,
        policies,
        raw_receipt,
        causal_receipt,
        session_policy,
        definitions,
        economics,
    )


def _prediction(feature_row, economics_record_id: str, *, recorded_at=None):
    recorded = feature_row.decision_at if recorded_at is None else recorded_at
    fields = {
        "bundle_id": "2" * 64,
        "actual": feature_row.actual,
        "decision_at": feature_row.decision_at,
        "recorded_at": recorded,
        "source_release_id": feature_row.source_release_id,
        "source_release_receipt_id": feature_row.source_release_receipt.receipt_id,
        "economics_record_id": economics_record_id,
        "feature_row_id": feature_row.row_id,
        "planned_entry_at": feature_row.planned_entry_at,
        "label_unlock_at": feature_row.label_unlock_at,
    }
    return PredictionRow(
        prediction_id=prediction_id_for(**fields),
        **fields,
        abstained=False,
        abstention_reasons=(),
        expected_return=0.001,
        probability_up=0.55,
        probability_down=0.35,
        probability_neutral=0.10,
        uncertainty=0.20,
    )


def _missing_economics_prediction(feature_row) -> PredictionRow:
    fields = {
        "bundle_id": "2" * 64,
        "actual": feature_row.actual,
        "decision_at": feature_row.decision_at,
        "recorded_at": feature_row.decision_at,
        "source_release_id": feature_row.source_release_id,
        "source_release_receipt_id": feature_row.source_release_receipt.receipt_id,
        "economics_record_id": "0" * 64,
        "feature_row_id": feature_row.row_id,
        "planned_entry_at": feature_row.planned_entry_at,
        "label_unlock_at": feature_row.label_unlock_at,
    }
    return PredictionRow(
        prediction_id=prediction_id_for(**fields),
        **fields,
        abstained=True,
        abstention_reasons=("MISSING_OR_AMBIGUOUS_ECONOMICS",),
        expected_return=None,
        probability_up=None,
        probability_down=None,
        probability_neutral=None,
        uncertainty=None,
    )


def _ledger(boundary, operation_factory, at):
    receipt = operation_factory("APPEND_PREDICTION")
    clock = SyntheticClock(boundary, receipt, at)
    ledger = PredictionLedger(
        boundary.active_root / "state" / "predictions",
        boundary.active_root / "state" / "locks" / "ledger.lock",
        boundary.active_root / "state" / "anchors",
        boundary.active_root / "state" / "ledger_heads" / "head.json",
        max_append_delay=timedelta(minutes=5),
        clock=clock,
        boundary=boundary,
        operation_receipt=receipt,
    )
    return ledger, clock


def test_bridge_preserves_exact_identity_economics_and_unresolved_census(
    boundary, operation_factory
) -> None:
    (
        publisher,
        policies,
        _raw_receipt,
        causal_receipt,
        session_policy,
        definitions,
        economics,
    ) = _bridge_chain(boundary, operation_factory)

    provider = definitions.provider_record(DEFINITION_ROW_SHA256).provider
    assert provider.ts_event_ns == EVENT_NS
    assert provider.ts_recv_ns == EVENT_NS + 1
    spec = CausalFeatureSpec(
        feature_names=("volume", "bar_return"),
        entry_delay_seconds=60,
        label_horizon_seconds=300,
    )
    feature_receipt = publish_causal_feature_release(
        causal_receipt=causal_receipt,
        definitions=definitions,
        economics_registry=economics,
        policies=policies,
        session_policy=session_policy,
        feature_spec=spec,
        boundary=boundary,
        publisher=publisher,
    )
    loaded = load_causal_feature_release(
        feature_receipt,
        causal_receipt=causal_receipt,
        definitions=definitions,
        economics_registry=economics,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    )

    assert loaded.total_upstream_rows == 2
    assert loaded.unresolved_upstream_rows == 1
    assert len(loaded.rows) == 1
    assert tuple(loaded.rows[0].values) == spec.feature_names
    assert loaded.rows[0].values["volume"] == 100
    assert loaded.rows[0].values["bar_return"] == pytest.approx(0.0001)
    resolved = economics.resolve(
        loaded.rows[0].actual, loaded.rows[0].decision_at
    )
    assert str(resolved.point_value) == "50"
    assert str(resolved.tick_size) == "0.25"
    assert str(resolved.tick_value) == "12.50"
    assert session_policy.exchange_session_date(
        "XCME", loaded.rows[0].bar_event_at
    ).isoformat() == "2024-01-01"


def test_outcome_release_requires_prediction_census_and_retains_unresolved(
    boundary, operation_factory
) -> None:
    (
        publisher,
        policies,
        _raw_receipt,
        causal_receipt,
        session_policy,
        definitions,
        economics,
    ) = _bridge_chain(boundary, operation_factory)
    feature_receipt = publish_causal_feature_release(
        causal_receipt=causal_receipt,
        definitions=definitions,
        economics_registry=economics,
        policies=policies,
        session_policy=session_policy,
        feature_spec=CausalFeatureSpec(("bar_return",), 60, 300),
        boundary=boundary,
        publisher=publisher,
    )
    feature = load_causal_feature_release(
        feature_receipt,
        causal_receipt=causal_receipt,
        definitions=definitions,
        economics_registry=economics,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    ).rows[0]
    economics_record = economics.resolve(feature.actual, feature.decision_at)
    ledger, clock = _ledger(boundary, operation_factory, feature.decision_at)
    with pytest.raises(ContractError, match="empty ledger"):
        ledger.issue_census()
    prediction = _prediction(feature, economics_record.record_id)
    appended = ledger.append(prediction, expected_head=LedgerHeadContract.genesis())
    census = ledger.issue_census()
    unresolved = OutcomeRow(
        prediction_id=prediction.prediction_id,
        actual=prediction.actual,
        decision_at=prediction.decision_at,
        label_end_at=prediction.label_unlock_at,
        matured_at=prediction.label_unlock_at,
        source_release_id=causal_receipt.release_id,
        interval_contract_segment_hashes=(prediction.actual.contract_segment_hash,),
        included_in_coverage_denominator=True,
        status=OutcomeStatus.MISSING_SOURCE,
        price_return=None,
    )
    wrong_actual = replace(prediction.actual, instrument_id=124)
    mismatched = replace(
        unresolved,
        actual=wrong_actual,
        interval_contract_segment_hashes=(wrong_actual.contract_segment_hash,),
    )
    with pytest.raises(ContractError, match="exact prediction"):
        publish_outcome_release(
            outcomes=(mismatched,),
            prediction_census=census,
            prediction_ledger=ledger,
            source_receipts=(causal_receipt,),
            boundary=boundary,
            publisher=publisher,
        )
    outcome_receipt = publish_outcome_release(
        outcomes=(unresolved,),
        prediction_census=census,
        prediction_ledger=ledger,
        source_receipts=(causal_receipt,),
        boundary=boundary,
        publisher=publisher,
    )
    loaded = load_outcome_release(
        outcome_receipt,
        prediction_census=census,
        prediction_ledger=ledger,
        source_receipts=(causal_receipt,),
        boundary=boundary,
    )
    assert loaded.coverage.denominator_count == 1
    assert loaded.coverage.resolved_count == 0
    assert loaded.coverage.unresolved_count == 1
    assert loaded.label_method_id is None

    later = feature.decision_at + timedelta(seconds=1)
    clock.set(later)
    ledger.append(
        _prediction(feature, economics_record.record_id, recorded_at=later),
        expected_head=appended.head,
    )
    with pytest.raises(IntegrityError, match="forged|stale|truncated"):
        load_outcome_release(
            outcome_receipt,
            prediction_census=census,
            prediction_ledger=ledger,
            source_receipts=(causal_receipt,),
            boundary=boundary,
        )


@pytest.mark.parametrize(
    ("omit_minute", "roll_minute", "missing_economics", "expected_status"),
    (
        (None, None, False, OutcomeStatus.MATURED),
        (4, None, False, OutcomeStatus.MISSING_SOURCE),
        (None, 4, False, OutcomeStatus.ROLL_UNRESOLVED),
        (None, None, True, OutcomeStatus.MISSING_SOURCE),
    ),
)
def test_causal_outcome_generation_is_deterministic_and_never_crosses_segments(
    boundary,
    operation_factory,
    omit_minute,
    roll_minute,
    missing_economics,
    expected_status,
) -> None:
    (
        publisher,
        policies,
        _raw_receipt,
        causal_receipt,
        session_policy,
        definitions,
        economics,
    ) = _bridge_chain(
        boundary,
        operation_factory,
        label_path=True,
        omit_minute=omit_minute,
        roll_minute=roll_minute,
    )
    feature_receipt = publish_causal_feature_release(
        causal_receipt=causal_receipt,
        definitions=definitions,
        economics_registry=economics,
        policies=policies,
        session_policy=session_policy,
        feature_spec=CausalFeatureSpec(("bar_return",), 55, 235),
        boundary=boundary,
        publisher=publisher,
    )
    feature = load_causal_feature_release(
        feature_receipt,
        causal_receipt=causal_receipt,
        definitions=definitions,
        economics_registry=economics,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    ).rows[0]
    economics_record = economics.resolve(feature.actual, feature.decision_at)
    ledger, _clock = _ledger(boundary, operation_factory, feature.decision_at)
    prediction = (
        _missing_economics_prediction(feature)
        if missing_economics
        else _prediction(feature, economics_record.record_id)
    )
    ledger.append(prediction, expected_head=LedgerHeadContract.genesis())
    census = ledger.issue_census()
    context = CausalOutcomeContext(
        causal_receipt,
        definitions,
        economics,
        policies,
        session_policy,
    )

    first = generate_causal_outcomes(
        prediction_census=census,
        prediction_ledger=ledger,
        context=context,
        boundary=boundary,
    )
    second = generate_causal_outcomes(
        prediction_census=census,
        prediction_ledger=ledger,
        context=context,
        boundary=boundary,
    )
    assert first == second
    assert len(first) == 1
    assert first[0].status is expected_status
    assert first[0].included_in_coverage_denominator is True
    assert first[0].matured_at >= prediction.label_unlock_at
    if expected_status is OutcomeStatus.MATURED:
        assert first[0].price_return == pytest.approx(
            5_001_250_000_000 / 5_000_500_000_000 - 1
        )
        assert len(set(first[0].interval_contract_segment_hashes)) == 1
    elif expected_status is OutcomeStatus.ROLL_UNRESOLVED:
        assert first[0].price_return is None
        assert len(set(first[0].interval_contract_segment_hashes)) == 2
    else:
        assert first[0].price_return is None

    assert CAUSAL_OUTCOME_LABEL_METHOD_ID.endswith("OPEN_TO_EVENT_OPEN_1M_V1")
    outcome_receipt = publish_causal_outcome_release(
        prediction_census=census,
        prediction_ledger=ledger,
        context=context,
        boundary=boundary,
        publisher=publisher,
    )
    loaded = load_causal_outcome_release(
        outcome_receipt,
        prediction_census=census,
        prediction_ledger=ledger,
        context=context,
        boundary=boundary,
    )
    assert loaded.outcomes == first
    assert loaded.label_method_id == CAUSAL_OUTCOME_LABEL_METHOD_ID
    assert loaded.coverage.denominator_count == 1
    assert loaded.coverage.resolved_count == (
        1 if expected_status is OutcomeStatus.MATURED else 0
    )


def test_feature_bridge_rejects_noncausal_release_role(
    boundary, operation_factory
) -> None:
    (
        publisher,
        policies,
        raw_receipt,
        _causal_receipt,
        session_policy,
        definitions,
        economics,
    ) = _bridge_chain(boundary, operation_factory)
    with pytest.raises(IntegrityError, match="causal interval release"):
        publish_causal_feature_release(
            causal_receipt=raw_receipt,
            definitions=definitions,
            economics_registry=economics,
            policies=policies,
            session_policy=session_policy,
            feature_spec=CausalFeatureSpec(("volume",), 60, 300),
            boundary=boundary,
            publisher=publisher,
        )
