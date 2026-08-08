from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from futures_rebuild.cme_contract_economics import (
    CME_EVIDENCE_RELEASE_KIND,
    CME_EVIDENCE_GAP_REPORT_RELEASE_KIND,
    CapturedCmeSource,
    DbnEconomicsCrosscheck,
    VerifiedCmeEvidenceGapReport,
    VerifiedCmeEvidenceRegistry,
    validate_phase8_authoritative_economics,
    crosscheck_cme_against_dbn,
    prepare_phase8_cme_evidence_capture,
    _publish_phase8_authoritative_actual_economics,
    _publish_cme_contract_economics_evidence,
    _publish_cme_contract_economics_gap_report,
)
from futures_rebuild.data_layout import PhasePublisher
from futures_rebuild.economics import VerifiedEconomicsRegistry
from futures_rebuild.foundation.economics import EconomicsRuleBook
from futures_rebuild.errors import ContractError, IntegrityError


UTC = timezone.utc


def _records(at: datetime):
    values = (
        ("0" * 63 + "1", "ES", "E-mini S&P 500", "EQUITY_INDEX", "50", "50", "0.25", "12.50", "USD_PER_INDEX_POINT", "CME_ES_SPEC"),
        ("1" * 64, "CL", "WTI Crude Oil", "ENERGY", "1000", "1000", "0.01", "10", "USD_PER_BARREL", "CME_CL_SPEC"),
        ("2" * 64, "ZN", "10-Year Treasury Note", "RATES", "1000", "100000", "0.015625", "15.625", "PERCENT_OF_PAR", "CME_ZN_SPEC"),
        ("3" * 64, "6E", "Euro FX", "FX", "125000", "125000", "0.00005", "6.25", "USD_PER_EUR", "CME_6E_SPEC"),
    )
    return [
        {
            "actual_identity_hash": identity,
            "asset_class": asset_class,
            "available_at": datetime(2021, 1, 1, tzinfo=UTC).isoformat(),
            "contract_unit_quantity": unit_quantity,
            "currency": "USD",
            "effective_at": datetime(2020, 1, 1, tzinfo=UTC).isoformat(),
            "market": market,
            "point_value": point,
            "product_family": family,
            "quote_convention_id": quote,
            "source_ids": [source_id],
            "tick_size": tick_size,
            "tick_value": tick_value,
        }
        for identity, market, family, asset_class, point, unit_quantity, tick_size, tick_value, quote, source_id in values
    ]


def _sources(at: datetime):
    return tuple(
        CapturedCmeSource(
            source_id=source_id,
            market=market,
            locator=f"https://www.cmegroup.com/rulebook/{market.lower()}-2017.pdf",
            document_kind="baseline",
            published_at=datetime(2017, 1, 1, tzinfo=UTC),
            effective_from=datetime(2017, 1, 1, tzinfo=UTC),
            effective_until=datetime(2023, 1, 1, tzinfo=UTC),
            retrieved_at=at - timedelta(days=1),
            content=f"official CME {market} source snapshot".encode("utf-8"),
        )
        for source_id, market in (
            ("CME_6E_SPEC", "6E"),
            ("CME_CL_SPEC", "CL"),
            ("CME_ES_SPEC", "ES"),
            ("CME_ZN_SPEC", "ZN"),
        )
    )


def _publish(boundary, operation_factory, at, records=None, sources=None, expected=None):
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )
    return _publish_cme_contract_economics_evidence(
        sources=_sources(at) if sources is None else sources,
        records=_records(at) if records is None else records,
        expected_actual_identity_hashes=(
            [record["actual_identity_hash"] for record in (_records(at) if records is None else records)]
            if expected is None else expected
        ),
        boundary=boundary,
        publisher=publisher,
    )


def test_cme_evidence_is_immutable_primary_coverage_for_phase8_markets(
    boundary, operation_factory
) -> None:
    at = datetime(2026, 8, 1, tzinfo=UTC)
    receipt = _publish(boundary, operation_factory, at)
    registry = VerifiedCmeEvidenceRegistry.from_release(receipt, boundary)

    assert receipt.release_kind == CME_EVIDENCE_RELEASE_KIND
    assert {record.market for record in registry.records.values()} == {"ES", "CL", "ZN", "6E"}
    assert registry.resolve("2" * 64).tick_value == registry.resolve("2" * 64).tick_size * registry.resolve("2" * 64).point_value


@pytest.mark.parametrize(
    "change",
    (
        lambda records: records.__setitem__(0, {**records[0], "tick_value": "1"}),
        lambda records: records.__setitem__(1, {**records[1], "effective_at": "2024-01-01T00:00:00+00:00"}),
        lambda records: records.__setitem__(2, {**records[2], "source_ids": ["CME_MISSING"]}),
    ),
)
def test_cme_evidence_rejects_invalid_or_future_known_records(
    boundary, operation_factory, change
) -> None:
    at = datetime(2026, 8, 1, tzinfo=UTC)
    records = _records(at)
    change(records)
    with pytest.raises(IntegrityError):
        _publish(boundary, operation_factory, at, records=records)


def test_cme_evidence_rejects_mutable_non_cme_locator(boundary, operation_factory) -> None:
    at = datetime(2026, 8, 1, tzinfo=UTC)
    sources = tuple(
        replace(source, locator="https://example.test/es")
        if source.source_id == "CME_ES_SPEC" else source
        for source in _sources(at)
    )
    with pytest.raises(IntegrityError):
        _publish(boundary, operation_factory, at, sources=sources)


def test_cme_evidence_detects_snapshot_tampering(boundary, operation_factory) -> None:
    at = datetime(2026, 8, 1, tzinfo=UTC)
    receipt = _publish(boundary, operation_factory, at)
    path = receipt.resolve_file("data/reference/economics/CME_ES_SPEC.bin", boundary)
    path.write_bytes(b"tampered")
    with pytest.raises(IntegrityError):
        VerifiedCmeEvidenceRegistry.from_release(receipt, boundary)


def test_cme_evidence_fails_closed_when_actual_contract_is_not_covered(
    boundary, operation_factory
) -> None:
    at = datetime(2026, 8, 1, tzinfo=UTC)
    registry = VerifiedCmeEvidenceRegistry.from_release(
        _publish(boundary, operation_factory, at), boundary
    )
    with pytest.raises(IntegrityError, match="does not cover"):
        registry.resolve("f" * 64)


def test_cme_capture_preparation_is_bounded_and_never_executes() -> None:
    prepared = prepare_phase8_cme_evidence_capture()
    assert prepared["status"] == "CONFIRMATION_REQUIRED"
    assert prepared["scope"]["markets"] == "ES, CL, ZN, 6E"
    assert prepared["scope"]["maximum_source_snapshots"] == "48"
    assert prepared["scope"]["provider_calls"] == "0"
    assert "approval_to_paste" not in prepared


def test_cme_evidence_requires_complete_dated_chain_for_every_market(
    boundary, operation_factory
) -> None:
    at = datetime(2026, 8, 1, tzinfo=UTC)
    sources = tuple(
        replace(source, effective_until=datetime(2021, 12, 31, tzinfo=UTC))
        if source.market == "ZN" else source
        for source in _sources(at)
    )
    with pytest.raises(IntegrityError, match="ends before 2022"):
        _publish(boundary, operation_factory, at, sources=sources)


def test_cme_evidence_rejects_a_current_product_page_and_more_than_48_sources(
    boundary, operation_factory
) -> None:
    at = datetime(2026, 8, 1, tzinfo=UTC)
    current_page = tuple(
        replace(source, locator="https://www.cmegroup.com/markets/fx/g10/euro-fx.contractSpecs.html")
        if source.market == "6E" else source
        for source in _sources(at)
    )
    with pytest.raises(IntegrityError, match="invalid or mutable"):
        _publish(boundary, operation_factory, at, sources=current_page)
    sources = _sources(at) * 12 + (_sources(at)[0],)
    with pytest.raises(ContractError, match="snapshot limit"):
        _publish(boundary, operation_factory, at, sources=sources)


def test_cme_evidence_rejects_partial_actual_identity_coverage(
    boundary, operation_factory
) -> None:
    at = datetime(2026, 8, 1, tzinfo=UTC)
    with pytest.raises(IntegrityError, match="expected actual identities"):
        _publish(
            boundary, operation_factory, at,
            records=_records(at)[:-1],
            expected=[record["actual_identity_hash"] for record in _records(at)],
        )


def test_gap_report_is_readable_but_never_authoritative_economics(
    boundary, operation_factory
) -> None:
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )
    receipt = _publish_cme_contract_economics_gap_report(
        uncovered_intervals=[{
            "market": "ZN",
            "from": "2019-01-01T00:00:00+00:00",
            "until": "2019-12-31T23:59:59+00:00",
            "reason": "No dated CME-hosted amendment chain found",
        }],
        inspected_sources=[{
            "market": "ZN",
            "locator": "https://www.cmegroup.com/rulebook/CME/",
            "retrieved_at": "2026-08-01T00:00:00+00:00",
            "content_sha256": "a" * 64,
        }],
        boundary=boundary,
        publisher=publisher,
    )
    report = VerifiedCmeEvidenceGapReport.from_release(receipt, boundary)
    assert receipt.release_kind == CME_EVIDENCE_GAP_REPORT_RELEASE_KIND
    assert report.uncovered_intervals[0]["market"] == "ZN"
    with pytest.raises(IntegrityError, match="wrong contract"):
        VerifiedCmeEvidenceRegistry.from_release(receipt, boundary)


def test_cme_primary_values_require_matching_dbn_contract_fields(
    boundary, operation_factory
) -> None:
    at = datetime(2026, 8, 1, tzinfo=UTC)
    registry = VerifiedCmeEvidenceRegistry.from_release(
        _publish(boundary, operation_factory, at), boundary
    )
    record = crosscheck_cme_against_dbn(
        registry,
        DbnEconomicsCrosscheck(
            actual_identity_hash="2" * 64,
            market="ZN",
            currency="USD",
            tick_size=registry.resolve("2" * 64).tick_size,
            contract_unit_quantity=registry.resolve("2" * 64).contract_unit_quantity,
        ),
    )
    assert record.market == "ZN"
    with pytest.raises(IntegrityError, match="contradicts"):
        crosscheck_cme_against_dbn(
            registry,
            DbnEconomicsCrosscheck(
                actual_identity_hash="2" * 64,
                market="ZN",
                currency="USD",
                tick_size=record.tick_size,
                contract_unit_quantity=record.contract_unit_quantity + 1,
            ),
        )


def test_phase8_actual_registry_requires_cme_provenance_and_exact_crosscheck(
    boundary, operation_factory, release_factory
) -> None:
    at = datetime(2026, 8, 1, tzinfo=UTC)
    evidence_receipt = _publish(boundary, operation_factory, at)
    evidence = VerifiedCmeEvidenceRegistry.from_release(evidence_receipt, boundary)
    checks = [
        DbnEconomicsCrosscheck(
            actual_identity_hash=identity,
            market=record.market,
            currency=record.currency,
            tick_size=record.tick_size,
            contract_unit_quantity=record.contract_unit_quantity,
        )
        for identity, record in sorted(evidence.records.items())
    ]
    payload = {
        "schema_version": "1.1.0",
        "records": [
            {
                "actual_identity_hash": identity,
                "ambiguity_reasons": [],
                "asset_class": record.asset_class,
                "available_at": record.available_at.isoformat(),
                "currency": record.currency,
                "effective_at": record.effective_at.isoformat(),
                "point_value": str(record.point_value),
                "quote_convention_id": record.quote_convention_id,
                "source_fields_used": ["cme_captured_contract_economics", "provider_unit_of_measure_qty"],
                "source_received_at": record.available_at.isoformat(),
                "tick_size": str(record.tick_size),
                "tick_value": str(record.tick_value),
                "verification_source_ids": sorted([*record.source_ids, "DATABENTO_DEFINITION_GLBX_MDP3"]),
            }
            for identity, record in sorted(evidence.records.items())
        ],
    }
    _, receipt = release_factory(
        release_kind="actual_contract_economics",
        filename="contract_economics.json",
        content=payload,
        schema_version="1.1.0",
        source_release_ids=(evidence_receipt.release_id,),
    )
    registry = VerifiedEconomicsRegistry.from_release(receipt, boundary)
    validate_phase8_authoritative_economics(registry, evidence, checks)

    _, unbound_receipt = release_factory(
        release_kind="actual_contract_economics",
        filename="contract_economics.json",
        content=payload,
        schema_version="1.1.0",
    )
    with pytest.raises(IntegrityError, match="CME evidence provenance"):
        validate_phase8_authoritative_economics(
            VerifiedEconomicsRegistry.from_release(unbound_receipt, boundary), evidence, checks
        )


def test_phase8_publisher_derives_registry_from_cme_and_dbn(
    boundary, operation_factory, release_factory
) -> None:
    at = datetime(2026, 8, 1, tzinfo=UTC)
    evidence = VerifiedCmeEvidenceRegistry.from_release(
        _publish(boundary, operation_factory, at), boundary
    )
    checks = [
        DbnEconomicsCrosscheck(identity, record.market, record.currency, record.tick_size, record.contract_unit_quantity)
        for identity, record in sorted(evidence.records.items())
    ]
    upstream = [
        release_factory(release_kind=kind, filename=f"{index}.json", content={"synthetic": index})[1]
        for index, kind in enumerate(("causal", "definitions", "policies", "session"), start=1)
    ]
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )
    root = Path(__file__).parents[1]
    receipt = _publish_phase8_authoritative_actual_economics(
        evidence=evidence,
        dbn_checks=checks,
        rulebook=EconomicsRuleBook.from_file(root / "configs" / "contract_economics_rules.json"),
        causal_receipt=upstream[0],
        definition_receipt=upstream[1],
        policy_receipt=upstream[2],
        session_receipt=upstream[3],
        boundary=boundary,
        publisher=publisher,
    )
    registry = VerifiedEconomicsRegistry.from_release(receipt, boundary)
    validate_phase8_authoritative_economics(registry, evidence, checks)
