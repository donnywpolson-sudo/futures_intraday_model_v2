from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import databento_dbn as dbn
import pytest

import futures_rebuild.foundation.materialize as materialize_module
import futures_rebuild.foundation.parquet as parquet_module
import futures_rebuild.producer_bridge as producer_bridge_module
from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.boundary import OperationClassification, OperationReceipt
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.data_layout import (
    DataReleaseManifest,
    PhasePublisher as AtomicPublisher,
)
from futures_rebuild.errors import ContractError, IntegrityError
from futures_rebuild.foundation import (
    EconomicsRuleBook,
    DefinitionIndex,
    FoundationPolicy,
    KnownAnomalyPolicy,
    PublishedSourceSnapshot,
    ProviderDefinition,
    build_causal_bar,
    iter_bars,
    iter_definitions,
)
from futures_rebuild.foundation.records import INT64_NULL
from futures_rebuild.foundation.materialize import (
    load_causal_interval,
    load_raw_interval,
    materialize_causal_interval,
    materialize_raw_interval,
)
from futures_rebuild.foundation.parquet import (
    iter_raw_bars,
    read_definitions,
    write_causal_bars,
)
from futures_rebuild.foundation.support import (
    VerifiedFoundationPolicies,
    publish_foundation_policies,
)
from futures_rebuild.foundation.snapshot import DbnReleaseFile, dbn_filename_interval
from futures_rebuild.source_symbology import build_query_contract


REPO = Path(__file__).resolve().parents[1]
START_NS = 1_704_067_200_000_000_000
END_NS = 1_735_689_600_000_000_000
BOUNDED_END_NS = 1_752_444_000_000_000_000


def _query(schema: str) -> dict[str, object]:
    parent = schema == "definition"
    return build_query_contract(
        schema=schema,
        market="ES",
        start="2024-01-01",
        end="2025-01-01",
        stype_in="parent" if parent else "continuous",
        symbols=["ES.FUT" if parent else "ES.v.0"],
    )


class TestSessionPolicy:
    def exchange_session_date(self, exchange: str, event_at: datetime) -> date:
        assert exchange == "XCME"
        return event_at.date()


def _definition_bytes(*, symbol: str = "ES.FUT") -> bytes:
    metadata = dbn.Metadata(
        dataset="GLBX.MDP3",
        start=START_NS,
        end=END_NS,
        stype_in=dbn.SType.PARENT,
        stype_out=dbn.SType.INSTRUMENT_ID,
        schema=dbn.Schema.DEFINITION,
        symbols=[symbol],
        ts_out=False,
    )
    record = dbn.InstrumentDefMsg(
        publisher_id=1,
        instrument_id=123,
        # The provider event and index clocks are intentionally independent.
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


def _bar_bytes(*, symbol: str = "ES.v.0") -> bytes:
    metadata = dbn.Metadata(
        dataset="GLBX.MDP3",
        start=START_NS,
        end=END_NS,
        stype_in=dbn.SType.CONTINUOUS,
        stype_out=dbn.SType.INSTRUMENT_ID,
        schema=dbn.Schema.OHLCV_1M,
        symbols=[symbol],
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


def _snapshot(tmp_path: Path) -> tuple[PublishedSourceSnapshot, RepoBoundary]:
    active = tmp_path / "active"
    active.mkdir(parents=True)
    boundary = RepoBoundary(active)
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    publisher = AtomicPublisher(
        boundary=boundary,
        operation_receipt=operation,
        lock_path=active / "state" / "locks" / "dbn.lock",
    )
    stage = publisher.create_stage("dbn")
    payloads = {
        "dbn/definition/ES/2024/2024-01-01_2025-01-01.dbn.zst": _definition_bytes(),
        "dbn/definition/ES/2024/2024-01-01_2025-01-01.dbn.zst.manifest.json": b"{}\n",
        "dbn/ohlcv_1m/ES/2024/2024-01-01_2025-01-01.dbn.zst": _bar_bytes(),
        "dbn/ohlcv_1m/ES/2024/2024-01-01_2025-01-01.dbn.zst.manifest.json": b"{}\n",
    }
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
    manifest_path = publisher.publish(stage, manifest, staged_paths=staged_paths)
    return PublishedSourceSnapshot.open(manifest_path, boundary=boundary), boundary


def _bindings(snapshot: PublishedSourceSnapshot):
    definition = snapshot.dbn_file(
        schema="definition",
        market="ES",
        year=2024,
        filename="2024-01-01_2025-01-01.dbn.zst",
    )
    bar = snapshot.dbn_file(
        schema="ohlcv-1m",
        market="ES",
        year=2024,
        filename="2024-01-01_2025-01-01.dbn.zst",
    )
    return definition, bar


def test_offline_dbn_decode_retains_exact_nanoseconds_and_provenance(tmp_path: Path):
    snapshot, _ = _snapshot(tmp_path)
    definition_binding, bar_binding = _bindings(snapshot)
    definitions = list(
        iter_definitions(
            definition_binding,
            market="ES",
            expected_query_contract=_query("definition"),
            batch_rows=1,
        )
    )
    bars = list(
        iter_bars(
            bar_binding,
            market="ES",
            expected_query_contract=_query("ohlcv-1m"),
            batch_rows=1,
        )
    )

    assert len(definitions) == len(bars) == 1
    assert definitions[0].ts_recv_ns == START_NS
    assert definitions[0].ts_event_ns == START_NS + 1
    assert definitions[0].ts_recv_ns < definitions[0].ts_event_ns
    assert definitions[0].instrument_id_date_utc == "2024-01-01"
    assert definitions[0].activation_ns == START_NS - 86_400_000_000_000
    assert definitions[0].expiration_ns == END_NS
    assert bars[0].event_at_ns == START_NS
    assert definitions[0].source_release_id == snapshot.source_release_id
    assert definitions[0].source_file_sha256 == sha256_file(definition_binding.path)
    assert bars[0].source_file_path.startswith("dbn/ohlcv_1m/ES/2024/")
    assert definitions[0].row_sha256 != bars[0].row_sha256


def test_registered_hardlinked_timestamped_dbn_uses_exact_intraday_end(
    tmp_path: Path,
) -> None:
    payload = _definition_bytes()
    seed = tmp_path / "seed.dbn.zst"
    seed.write_bytes(payload)
    target = tmp_path / "2025-01-01_2025-07-13T220000Z.dbn.zst"
    target.hardlink_to(seed)
    binding = DbnReleaseFile(
        logical_path=target.name,
        physical_path=target,
        relative_path=target.name,
        size=len(payload),
        sha256=sha256_file(target, reject_hardlinks=False),
        source_release_id="a" * 64,
        source_manifest_sha256="b" * 64,
        files_index_sha256="c" * 64,
    )
    with pytest.raises(ContractError, match="hard-linked"):
        binding.verify()
    registered = replace(binding, allow_registered_hardlinks=True)
    assert registered.verify() == target
    assert dbn_filename_interval(target.name) == (
        "2025-01-01T00:00:00Z",
        "2025-07-13T22:00:00Z",
    )
    seed.write_bytes(payload + b"drift")
    with pytest.raises(IntegrityError, match="central manifest"):
        registered.verify()


def test_timestamped_dbn_metadata_end_is_not_truncated_to_midnight(
    tmp_path: Path,
) -> None:
    start_ns = int(
        datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000_000
    )
    metadata = dbn.Metadata(
        dataset="GLBX.MDP3",
        start=start_ns,
        end=BOUNDED_END_NS,
        stype_in=dbn.SType.PARENT,
        stype_out=dbn.SType.INSTRUMENT_ID,
        schema=dbn.Schema.DEFINITION,
        symbols=["6A.FUT"],
        ts_out=False,
    )
    record = dbn.InstrumentDefMsg(
        publisher_id=1,
        instrument_id=123,
        ts_event=start_ns + 1,
        ts_recv=start_ns,
        activation=start_ns,
        expiration=BOUNDED_END_NS,
        min_price_increment=1,
        display_factor=1_000_000_000,
        raw_symbol="6AH5",
        asset="6A",
        security_type="FUT",
        instrument_class=dbn.InstrumentClass.FUTURE,
        security_update_action=dbn.SecurityUpdateAction.ADD,
        unit_of_measure_qty=1_000_000_000,
        currency="USD",
        group="6A",
        exchange="XCME",
        unit_of_measure="IPNT",
    )
    target = tmp_path / "2025-01-01_2025-07-13T220000Z.dbn.zst"
    target.write_bytes(metadata.encode() + bytes(record))
    binding = DbnReleaseFile(
        logical_path=target.name,
        physical_path=target,
        relative_path=target.name,
        size=target.stat().st_size,
        sha256=sha256_file(target),
        source_release_id="a" * 64,
        source_manifest_sha256="b" * 64,
        files_index_sha256="c" * 64,
    )
    records = list(
        iter_definitions(
            binding,
            market="6A",
            expected_query_contract=build_query_contract(
                schema="definition",
                market="6A",
                start="2025-01-01T00:00:00Z",
                end="2025-07-13T22:00:00Z",
                stype_in="parent",
                symbols=["6A.FUT"],
            ),
            batch_rows=1,
        )
    )
    assert len(records) == 1


def test_causal_bar_uses_actual_instrument_economics_and_exact_availability(tmp_path: Path):
    snapshot, _ = _snapshot(tmp_path)
    definition_binding, bar_binding = _bindings(snapshot)
    definitions = list(
        iter_definitions(
            definition_binding,
            market="ES",
            expected_query_contract=_query("definition"),
            batch_rows=1,
        )
    )
    bar = next(
        iter_bars(
            bar_binding,
            market="ES",
            expected_query_contract=_query("ohlcv-1m"),
            batch_rows=1,
        )
    )
    policy = FoundationPolicy.from_file(REPO / "configs" / "foundation_policy.json")
    anomalies = KnownAnomalyPolicy.from_file(
        REPO / "configs" / "known_anomalies.json",
        expected_sha256=policy.known_anomalies_sha256,
    )
    economics = EconomicsRuleBook.from_file(
        REPO / "configs" / "contract_economics_rules.json"
    )
    event = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ContractError, match="not modeled available"):
        build_causal_bar(
            bar,
            definitions,
            decision_at=event + timedelta(seconds=64, microseconds=999_999),
            policy=policy,
            anomaly_policy=anomalies,
            session_policy=TestSessionPolicy(),
            economics_rules=economics,
        )

    result = build_causal_bar(
        bar,
        DefinitionIndex(definitions),
        decision_at=event + timedelta(seconds=65),
        policy=policy,
        anomaly_policy=anomalies,
        session_policy=TestSessionPolicy(),
        economics_rules=economics,
    )
    assert result.event_at_ns == START_NS
    assert result.available_at_ns == START_NS + 65_000_000_000
    assert result.actual.instrument_id == 123
    assert result.actual.raw_symbol == "ESZ4"
    assert result.economics.point_value == 50
    assert result.economics.tick_value == 12.5
    assert result.prediction_in_coverage_denominator is True
    assert result.disposition.value == "ELIGIBLE"


def test_definition_replay_is_same_day_receive_ordered_and_defers_intrabar_updates(
    tmp_path: Path,
) -> None:
    snapshot, _ = _snapshot(tmp_path)
    definition_binding, bar_binding = _bindings(snapshot)
    definition = next(
        iter_definitions(
            definition_binding,
            market="ES",
            expected_query_contract=_query("definition"),
            batch_rows=1,
        )
    )
    bar = next(
        iter_bars(
            bar_binding,
            market="ES",
            expected_query_contract=_query("ohlcv-1m"),
            batch_rows=1,
        )
    )
    decision = datetime(2024, 1, 1, 0, 1, 5, tzinfo=timezone.utc)

    next_day = replace(
        definition,
        instrument_id_date_utc="2024-01-02",
        ts_recv_ns=START_NS + 86_400_000_000_000,
        row_ordinal=1,
        row_sha256="1" * 64,
    )
    with pytest.raises(ContractError, match="same-day"):
        DefinitionIndex((next_day,)).resolve(bar, decision_at=decision)

    intrabar = replace(
        definition,
        ts_recv_ns=START_NS + 30_000_000_000,
        security_update_action="MODIFY",
        raw_symbol="ESZ4_CHANGED",
        row_ordinal=1,
        row_sha256="2" * 64,
    )
    index = DefinitionIndex((definition, intrabar))
    selected = index.resolve(bar, decision_at=decision)
    assert selected.row_sha256 == definition.row_sha256
    assert selected.raw_symbol == "ESZ4"

    next_bar = replace(
        bar,
        event_at_ns=START_NS + 60_000_000_000,
        row_sha256="7" * 64,
    )
    selected_next = index.resolve(next_bar, decision_at=decision)
    assert selected_next.row_sha256 == intrabar.row_sha256
    assert selected_next.raw_symbol == "ESZ4_CHANGED"

    tombstone = replace(
        definition,
        security_update_action="DELETE",
        row_ordinal=1,
        row_sha256="3" * 64,
    )
    with pytest.raises(ContractError, match="deleted"):
        DefinitionIndex((definition, tombstone)).resolve(bar, decision_at=decision)


def test_definition_equal_receive_cross_file_conflict_and_lifecycle_fail_closed(
    tmp_path: Path,
) -> None:
    snapshot, _ = _snapshot(tmp_path)
    definition_binding, bar_binding = _bindings(snapshot)
    definition = next(
        iter_definitions(
            definition_binding,
            market="ES",
            expected_query_contract=_query("definition"),
            batch_rows=1,
        )
    )
    bar = next(
        iter_bars(
            bar_binding,
            market="ES",
            expected_query_contract=_query("ohlcv-1m"),
            batch_rows=1,
        )
    )
    decision = datetime(2024, 1, 1, 0, 1, 5, tzinfo=timezone.utc)
    conflict = replace(
        definition,
        raw_symbol="CONFLICT",
        source_file_path="dbn/definition/ES/2024/conflict.dbn.zst",
        source_file_sha256="4" * 64,
        row_sha256="5" * 64,
    )
    with pytest.raises(ContractError, match="equal-receive cross-file"):
        DefinitionIndex((definition, conflict)).resolve(bar, decision_at=decision)

    inactive = replace(
        definition,
        activation_ns=START_NS + 1,
        row_sha256="6" * 64,
    )
    with pytest.raises(ContractError, match="does not cover"):
        DefinitionIndex((inactive,)).resolve(bar, decision_at=decision)


def test_reported_definition_lifecycle_epoch_is_quarantined() -> None:
    policy = FoundationPolicy.from_file(REPO / "configs" / "foundation_policy.json")
    quarantined_ns = int(
        datetime(2016, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000_000
    )
    with pytest.raises(ContractError, match="quarantined"):
        policy.assert_definition_lifecycle_trusted(quarantined_ns)


def test_snapshot_and_decoder_fail_closed_on_mutation_or_metadata_drift(tmp_path: Path):
    snapshot, _ = _snapshot(tmp_path)
    _, bar_binding = _bindings(snapshot)
    original = bar_binding.path.read_bytes()
    bar_binding.path.write_bytes(original + b"x")
    with pytest.raises(IntegrityError, match="central manifest"):
        list(
            iter_bars(
                bar_binding,
                market="ES",
                expected_query_contract=_query("ohlcv-1m"),
                batch_rows=1,
            )
        )


def test_economics_rulebook_rejects_string_disguised_as_source_list(tmp_path: Path):
    payload = json.loads(
        (REPO / "configs" / "contract_economics_rules.json").read_text(encoding="utf-8")
    )
    payload["rules"][0]["source_ids"] = "CME_FX_PRODUCT_GUIDE"
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(IntegrityError, match="exact string list"):
        EconomicsRuleBook.from_file(path)


def test_economics_rulebook_treats_mutable_urls_as_non_authoritative_and_fails_on_provider_sentinel() -> None:
    payload = json.loads(
        (REPO / "configs" / "contract_economics_rules.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["authority_policy"] == {
        "eligible_contract_requires_provider_unit_qty_match": True,
        "mutable_public_urls_authorize_economics": False,
        "non_multiplier_provider_unit_markets": ["ZQ"],
        "provider_sentinel_allowed": False,
        "rulebook_hash_bound_into_every_economics_record": True,
    }
    assert [
        source_id
        for source_id, source in payload["verification_sources"].items()
        if source["authoritative"]
    ] == ["DATABENTO_DEFINITION_GLBX_MDP3"]

    definition = ProviderDefinition(
        dataset="GLBX.MDP3",
        market="ES",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc="2024-01-01",
        ts_event_ns=START_NS,
        ts_recv_ns=START_NS + 1,
        activation_ns=START_NS - 86_400_000_000_000,
        expiration_ns=END_NS,
        security_update_action="ADD",
        instrument_class="FUTURE",
        security_type="FUT",
        raw_symbol="ESZ4",
        exchange="XCME",
        currency="USD",
        min_price_increment_nano=250_000_000,
        unit_of_measure_qty_nano=INT64_NULL,
        unit_of_measure="INDEX",
        source_release_id="a" * 64,
        source_manifest_sha256="b" * 64,
        source_file_path="dbn/definition/ES/2024/example.dbn.zst",
        source_file_sha256="c" * 64,
        row_ordinal=0,
        row_sha256="d" * 64,
    )
    resolved = EconomicsRuleBook.from_file(
        REPO / "configs" / "contract_economics_rules.json"
    ).resolve("ES", definition)
    assert resolved.provider_unit_qty_state == "RULEBOOK_VALUE_PROVIDER_UNIT_QTY_UNAVAILABLE"


@pytest.mark.parametrize(
    (
        "market",
        "unit_qty",
        "min_tick_nano",
        "point_value",
        "tick_value",
        "quote_convention",
    ),
    [
        ("6N", 100_000, 50_000, "100000", "5", "USD_PER_NZD"),
        ("6S", 125_000, 50_000, "125000", "6.25", "USD_PER_CHF"),
        ("BTC", 5, 5_000_000_000, "5", "25", "USD_PER_BITCOIN"),
        ("ETH", 50, 500_000_000, "50", "25", "USD_PER_ETHER"),
        ("GF", 50_000, 25_000_000, "500", "12.5", "CENTS_PER_POUND"),
        ("PA", 100, 500_000_000, "100", "50", "USD_PER_TROY_OUNCE"),
        ("PL", 50, 100_000_000, "50", "5", "USD_PER_TROY_OUNCE"),
        ("ZQ", 4_167, 2_500_000, "4167", "10.4175", "USD_PER_IMM_INDEX_POINT"),
    ],
)
def test_eight_market_successor_economics_are_provider_quantity_bound(
    market: str,
    unit_qty: int,
    min_tick_nano: int,
    point_value: str,
    tick_value: str,
    quote_convention: str,
) -> None:
    payload = json.loads(
        (REPO / "configs" / "research_universe_contract.json").read_text(
            encoding="utf-8"
        )
    )
    rulebook = EconomicsRuleBook.from_file(
        REPO / "configs" / "contract_economics_rules.json"
    )
    approved_markets = {
        symbol for tier in payload["tiers"] for symbol in tier["symbols"]
    }
    assert set(rulebook.rules) == approved_markets

    definition = ProviderDefinition(
        dataset="GLBX.MDP3",
        market=market,
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc="2024-01-01",
        ts_event_ns=START_NS,
        ts_recv_ns=START_NS + 1,
        activation_ns=START_NS - 86_400_000_000_000,
        expiration_ns=END_NS,
        security_update_action="ADD",
        instrument_class="FUTURE",
        security_type="FUT",
        raw_symbol=f"{market}Z5",
        exchange="XCME",
        currency="USD",
        min_price_increment_nano=min_tick_nano,
        unit_of_measure_qty_nano=unit_qty * 1_000_000_000,
        unit_of_measure="PROVIDER_BOUND",
        source_release_id=(
            "086282eaef7b36a61626f88d93d06c93"
            "b87c1cb3407c936d065d0d1b9d98599e"
        ),
        source_manifest_sha256=(
            "c2584d5e1a65103f8651a871de6f704a"
            "c31ec2c2f7ec5c2e1a941aae6a4dc8fd"
        ),
        source_file_path=(
            f"dbn/definition/{market}/2025/"
            "2025-01-01_2026-01-01.dbn.zst"
        ),
        source_file_sha256="c" * 64,
        row_ordinal=0,
        row_sha256="d" * 64,
    )
    resolved = rulebook.resolve(market, definition)
    assert resolved.point_value == Decimal(point_value)
    assert resolved.tick_value == Decimal(tick_value)
    assert resolved.quote_convention == quote_convention

    changed_unit = replace(
        definition,
        unit_of_measure_qty_nano=(unit_qty + 1) * 1_000_000_000,
        row_sha256="e" * 64,
    )
    if market == "ZQ":
        assert rulebook.resolve(market, changed_unit).provider_unit_qty_state == (
            "RULEBOOK_VALUE_PROVIDER_UNIT_QTY_UNAVAILABLE"
        )
    else:
        with pytest.raises(ContractError, match="contradicts the pinned market rule"):
            rulebook.resolve(market, changed_unit)


def test_economics_asset_classes_cover_the_exact_41_market_universe() -> None:
    universe = json.loads(
        (REPO / "configs" / "research_universe_contract.json").read_text(
            encoding="utf-8"
        )
    )
    approved = {
        symbol for tier in universe["tiers"] for symbol in tier["symbols"]
    }
    assert set(producer_bridge_module._ASSET_CLASSES) == approved
    assert {
        market: producer_bridge_module._ASSET_CLASSES[market]
        for market in sorted({"6N", "6S", "BTC", "ETH", "GF", "PA", "PL", "ZQ"})
    } == {
        "6N": "FX",
        "6S": "FX",
        "BTC": "CRYPTO",
        "ETH": "CRYPTO",
        "GF": "AGRICULTURE",
        "PA": "METALS",
        "PL": "METALS",
        "ZQ": "RATES",
    }


def test_phase1b_materialization_is_release_bound_and_reproducible(tmp_path: Path):
    snapshot, boundary = _snapshot(tmp_path)
    definition_binding, bar_binding = _bindings(snapshot)
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    publisher = AtomicPublisher(
        boundary=boundary,
        operation_receipt=operation,
        lock_path=boundary.active_root / "state" / "locks" / "phase1b.lock",
    )
    kwargs = {
        "definition_binding": definition_binding,
        "bar_binding": bar_binding,
        "definition_query_contract": _query("definition"),
        "bar_query_contract": _query("ohlcv-1m"),
        "market": "ES",
        "year": 2024,
        "filename": "2024-01-01_2025-01-01.dbn.zst",
        "source_selection_release_id": "c" * 64,
        "publisher": publisher,
        "batch_rows": 1,
    }
    first = materialize_raw_interval(**kwargs)
    second = materialize_raw_interval(**kwargs)
    assert first.release_id == second.release_id

    loaded = load_raw_interval(first, boundary=boundary)
    bars = list(iter_raw_bars(loaded.bars_path, batch_rows=1))
    definitions = read_definitions(loaded.definitions_path, batch_rows=1)
    assert len(bars) == len(definitions) == 1
    assert bars[0].instrument_id == definitions[0].instrument_id == 123
    assert loaded.interval_receipt["learned_or_outcome_informed_transform_count"] == 0


def test_phase2_causal_release_consumes_only_verified_raw_and_policy_releases(
    tmp_path: Path, monkeypatch
):
    snapshot, boundary = _snapshot(tmp_path)
    definition_binding, bar_binding = _bindings(snapshot)
    config_root = boundary.active_root / "configs"
    config_root.mkdir(parents=True, exist_ok=True)
    for name in (
        "contract_economics_rules.json",
        "foundation_policy.json",
        "known_anomalies.json",
        "provider_data_epochs.json",
        "session_policy.json",
    ):
        (config_root / name).write_bytes((REPO / "configs" / name).read_bytes())
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    publisher = AtomicPublisher(
        boundary=boundary,
        operation_receipt=operation,
        lock_path=boundary.active_root / "state" / "locks" / "causal.lock",
    )
    policy_receipt = publish_foundation_policies(
        boundary=boundary,
        publisher=publisher,
        config_root=config_root,
    )
    policies = VerifiedFoundationPolicies.from_release(
        policy_receipt, boundary=boundary
    )
    raw_receipt = materialize_raw_interval(
        definition_binding=definition_binding,
        bar_binding=bar_binding,
        definition_query_contract=_query("definition"),
        bar_query_contract=_query("ohlcv-1m"),
        market="ES",
        year=2024,
        filename="2024-01-01_2025-01-01.dbn.zst",
        source_selection_release_id="c" * 64,
        publisher=publisher,
        batch_rows=1,
    )
    monkeypatch.setattr(
        materialize_module,
        "read_raw_bar_audit",
        lambda *_args, **_kwargs: pytest.fail(
            "same-process raw publication must reuse its producer census"
        ),
    )
    monkeypatch.setattr(
        materialize_module,
        "read_definition_audit",
        lambda *_args, **_kwargs: pytest.fail(
            "same-process definition publication must reuse its producer census"
        ),
    )
    causal_receipt = materialize_causal_interval(
        raw_receipt=raw_receipt,
        policies=policies,
        publisher=publisher,
        batch_rows=1,
    )
    monkeypatch.setattr(
        materialize_module,
        "read_causal_bar_census",
        lambda *_args, **_kwargs: pytest.fail(
            "same-process causal publication must reuse its producer census"
        ),
    )
    bars_path, report = load_causal_interval(causal_receipt, boundary=boundary)
    assert bars_path.is_file()
    assert report["row_count"] == 1
    assert report["disposition_counts"] == {"ELIGIBLE": 1}
    assert report["prediction_in_coverage_denominator_rows"] == 1
    assert report["learned_or_outcome_informed_transform_count"] == 0

    loaded_raw = load_raw_interval(raw_receipt, boundary=boundary)
    reference_path = tmp_path / "reference-causal-bars.parquet"
    monkeypatch.setattr(
        parquet_module,
        "_fast_causal_batch",
        lambda *_args, **_kwargs: None,
    )
    reference_census = write_causal_bars(
        raw_bars_path=loaded_raw.bars_path,
        definitions_path=loaded_raw.definitions_path,
        policies=policies,
        source_raw_release_id=raw_receipt.release_id,
        output=reference_path,
        batch_rows=1,
    )
    assert reference_census == (1, {"ELIGIBLE": 1}, {"GLBX_MDP3_CAPTURE_TIME": 1})
    assert reference_path.read_bytes() == bars_path.read_bytes()


def test_certification_rehydrates_embedded_predecessor_policy_release(
    tmp_path: Path,
) -> None:
    boundary = RepoBoundary(active_root=tmp_path)
    config_root = boundary.active_root / "configs"
    config_root.mkdir(parents=True)
    for name in (
        "contract_economics_rules.json",
        "foundation_policy.json",
        "known_anomalies.json",
        "provider_data_epochs.json",
        "session_policy.json",
    ):
        (config_root / name).write_bytes((REPO / "configs" / name).read_bytes())
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    publisher = AtomicPublisher(
        boundary=boundary,
        operation_receipt=operation,
        lock_path=boundary.active_root / "state" / "locks" / "policy.lock",
    )
    receipt = publish_foundation_policies(
        boundary=boundary,
        publisher=publisher,
        config_root=config_root,
    )
    manifest = receipt.verify(boundary)
    embedded_economics = manifest.embedded_documents[
        "contract_economics_rules.json"
    ]
    active_economics = json.loads(
        (config_root / "contract_economics_rules.json").read_text(
            encoding="utf-8"
        )
    )
    active_economics["valid_from"] = "2099-01-01"
    (config_root / "contract_economics_rules.json").write_bytes(
        canonical_bytes(active_economics) + b"\n"
    )

    with pytest.raises(
        IntegrityError, match="active foundation policies differ"
    ):
        VerifiedFoundationPolicies.from_release(receipt, boundary=boundary)

    policy_workspace = (
        boundary.active_root
        / "state"
        / "predecessor-policy"
    )
    policies = VerifiedFoundationPolicies.from_embedded_release(
        receipt,
        boundary=boundary,
        workspace=policy_workspace,
        required_market="ES",
    )

    assert policies.policy_set_id == manifest.metadata["policy_set_id"]
    assert policies.economics.rulebook_hash == sha256_json(embedded_economics)
    assert policies.economics.rulebook_hash != sha256_json(active_economics)
    assert (
        policy_workspace / "contract_economics_rules.json"
    ).read_bytes() == canonical_bytes(embedded_economics) + b"\n"
    policies.verify()
    with pytest.raises(IntegrityError, match="workspace already exists"):
        VerifiedFoundationPolicies.from_embedded_release(
            receipt,
            boundary=boundary,
            workspace=policy_workspace,
            required_market="ES",
        )


def test_embedded_predecessor_economics_requires_candidate_market() -> None:
    payload = json.loads(
        (REPO / "configs" / "contract_economics_rules.json").read_text(
            encoding="utf-8"
        )
    )
    payload["rules_version"] = "1.2.0"
    payload["rules"] = payload["rules"][:33]
    payload["verification_sources"]["DATABENTO_DEFINITION_GLBX_MDP3"][
        "locator"
    ] = (
        f"manifests/data_releases/dbn/{'b' * 64}.json"
        "#data/dbn/definition/{market}/{year}/{filename}"
    )

    rulebook = EconomicsRuleBook.from_embedded_payload(
        payload,
        required_market="ES",
    )

    assert len(rulebook.rules) == 33
    assert rulebook.rulebook_hash == sha256_json(payload)
    with pytest.raises(IntegrityError, match="schema/policy is invalid"):
        EconomicsRuleBook.from_payload(payload)
    with pytest.raises(
        IntegrityError, match="does not cover the required market"
    ):
        EconomicsRuleBook.from_embedded_payload(
            payload,
            required_market="ZW",
        )
