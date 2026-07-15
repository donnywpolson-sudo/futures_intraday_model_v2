from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import databento_dbn as dbn
import pytest

from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.boundary import OperationClassification, OperationReceipt
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
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
from futures_rebuild.foundation.parquet import iter_raw_bars, read_definitions
from futures_rebuild.foundation.support import (
    VerifiedFoundationPolicies,
    publish_foundation_policies,
)
from futures_rebuild.release import AtomicPublisher


REPO = Path(__file__).resolve().parents[1]
START_NS = 1_704_067_200_000_000_000
END_NS = 1_735_689_600_000_000_000


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
    payloads = {
        "dbn/definition/ES/2024/2024-01-01_2025-01-01.dbn.zst": _definition_bytes(),
        "dbn/definition/ES/2024/2024-01-01_2025-01-01.dbn.zst.manifest.json": b"{}\n",
        "dbn/ohlcv_1m/ES/2024/2024-01-01_2025-01-01.dbn.zst": _bar_bytes(),
        "dbn/ohlcv_1m/ES/2024/2024-01-01_2025-01-01.dbn.zst.manifest.json": b"{}\n",
    }
    files = [
        {
            "path": path,
            "sha256": __import__("hashlib").sha256(content).hexdigest(),
            "size": len(content),
        }
        for path, content in sorted(payloads.items())
    ]
    semantics = {
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
    receipt = {**semantics, "source_snapshot_id": sha256_json(semantics)}
    root = active / "data" / "vault" / "source_snapshots" / receipt["source_snapshot_id"]
    for relative, content in payloads.items():
        target = root / Path(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    (root / "SOURCE_SNAPSHOT_RECEIPT.json").write_bytes(canonical_bytes(receipt) + b"\n")
    active.mkdir(parents=True, exist_ok=True)
    boundary = RepoBoundary(active)
    return PublishedSourceSnapshot.open(root, boundary=boundary), boundary


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
    definitions = list(iter_definitions(definition_binding, market="ES", batch_rows=1))
    bars = list(iter_bars(bar_binding, market="ES", batch_rows=1))

    assert len(definitions) == len(bars) == 1
    assert definitions[0].ts_recv_ns == START_NS + 1
    assert bars[0].event_at_ns == START_NS
    assert definitions[0].source_release_id == snapshot.source_snapshot_id
    assert definitions[0].source_file_sha256 == sha256_file(definition_binding.path)
    assert bars[0].source_file_path.startswith("dbn/ohlcv_1m/ES/2024/")
    assert definitions[0].row_sha256 != bars[0].row_sha256


def test_causal_bar_uses_actual_instrument_economics_and_exact_availability(tmp_path: Path):
    snapshot, _ = _snapshot(tmp_path)
    definition_binding, bar_binding = _bindings(snapshot)
    definitions = list(iter_definitions(definition_binding, market="ES", batch_rows=1))
    bar = next(iter_bars(bar_binding, market="ES", batch_rows=1))
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


def test_snapshot_and_decoder_fail_closed_on_mutation_or_metadata_drift(tmp_path: Path):
    snapshot, _ = _snapshot(tmp_path)
    _, bar_binding = _bindings(snapshot)
    original = bar_binding.path.read_bytes()
    bar_binding.path.write_bytes(original + b"x")
    with pytest.raises(IntegrityError, match="accepted index"):
        list(iter_bars(bar_binding, market="ES", batch_rows=1))


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
        ts_event_ns=START_NS,
        ts_recv_ns=START_NS + 1,
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
        row_sha256="d" * 64,
    )
    with pytest.raises(ContractError, match="economics fail closed"):
        EconomicsRuleBook.from_file(
            REPO / "configs" / "contract_economics_rules.json"
        ).resolve("ES", definition)


def test_phase1b_materialization_is_release_bound_and_reproducible(tmp_path: Path):
    snapshot, boundary = _snapshot(tmp_path)
    definition_binding, bar_binding = _bindings(snapshot)
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    publisher = AtomicPublisher(
        boundary.active_root / "data" / "vault" / ".staging" / "releases" / "phase1b",
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / "phase1b.lock",
        boundary=boundary,
        operation_receipt=operation,
    )
    kwargs = {
        "definition_binding": definition_binding,
        "bar_binding": bar_binding,
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


def test_phase2_causal_release_consumes_only_verified_raw_and_policy_releases(tmp_path: Path):
    snapshot, boundary = _snapshot(tmp_path)
    definition_binding, bar_binding = _bindings(snapshot)
    config_root = boundary.active_root / "configs"
    config_root.mkdir(parents=True, exist_ok=True)
    for name in (
        "contract_economics_rules.json",
        "foundation_policy.json",
        "known_anomalies.json",
        "session_policy.json",
    ):
        (config_root / name).write_bytes((REPO / "configs" / name).read_bytes())
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    publisher = AtomicPublisher(
        boundary.active_root / "data" / "vault" / ".staging" / "releases" / "causal",
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / "causal.lock",
        boundary=boundary,
        operation_receipt=operation,
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
        market="ES",
        year=2024,
        filename="2024-01-01_2025-01-01.dbn.zst",
        source_selection_release_id="c" * 64,
        publisher=publisher,
        batch_rows=1,
    )
    causal_receipt = materialize_causal_interval(
        raw_receipt=raw_receipt,
        policies=policies,
        publisher=publisher,
        batch_rows=1,
    )
    bars_path, report = load_causal_interval(causal_receipt, boundary=boundary)
    assert bars_path.is_file()
    assert report["row_count"] == 1
    assert report["disposition_counts"] == {"ELIGIBLE": 1}
    assert report["prediction_in_coverage_denominator_rows"] == 1
    assert report["learned_or_outcome_informed_transform_count"] == 0
