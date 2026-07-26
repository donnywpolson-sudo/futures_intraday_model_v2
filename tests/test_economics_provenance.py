import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from futures_rebuild.economics import VerifiedEconomicsRegistry
from futures_rebuild.errors import ContractError, IntegrityError
from futures_rebuild.identity import ActualContractIdentity
from futures_rebuild.inference import VerifiedIdentityRegistry


UTC = timezone.utc


def _definition_registry(release_factory, boundary, decision):
    _, receipt = release_factory(
        release_kind="actual_contract_definitions",
        filename="identities.json",
        content={
            "schema_version": "1.0.0",
            "records": [
                {
                    "available_at": (decision - timedelta(days=1)).isoformat(),
                    "currency": "USD",
                    "dataset": "GLBX.MDP3",
                    "effective_at": (decision - timedelta(days=2)).isoformat(),
                    "exchange": "XCME",
                    "instrument_id": 12345,
                    "min_tick": "0.25",
                    "multiplier": "50",
                    "publisher_id": 1,
                    "raw_symbol": "ESZ6",
                    "source_received_at": (
                        decision - timedelta(days=1, seconds=1)
                    ).isoformat(),
                }
            ],
        },
    )
    registry = VerifiedIdentityRegistry.from_release(receipt, boundary)
    definition = next(iter(registry.definitions.values())).definition
    actual = ActualContractIdentity.from_definition(
        definition,
        instrument_id_date_utc=date(2026, 7, 14),
        exchange_session_date=date(2026, 7, 14),
    )
    return registry, actual


def _economics_payload(actual, decision, **overrides):
    record = {
        "actual_identity_hash": actual.identity_hash,
        "ambiguity_reasons": [],
        "asset_class": "EQUITY_INDEX",
        "available_at": (decision - timedelta(hours=1)).isoformat(),
        "currency": "USD",
        "effective_at": (decision - timedelta(days=1)).isoformat(),
        "point_value": "50",
        "quote_convention_id": "INDEX_POINTS",
        "source_fields_used": ["min_price_increment", "unit_of_measure_qty"],
        "source_received_at": (decision - timedelta(hours=2)).isoformat(),
        "tick_size": "0.25",
        "tick_value": "12.50",
        "verification_source_ids": ["cme_contract_spec"],
    }
    record.update(overrides)
    return {"schema_version": "1.0.0", "records": [record]}


def test_identity_and_economics_registries_derive_only_from_verified_releases(
    release_factory, boundary, decision
) -> None:
    identities, actual = _definition_registry(release_factory, boundary, decision)
    _, receipt = release_factory(
        release_kind="actual_contract_economics",
        filename="contract_economics.json",
        content=_economics_payload(actual, decision),
    )
    economics = VerifiedEconomicsRegistry.from_release(receipt, boundary)
    resolved = economics.resolve(actual, decision)
    assert identities.contains(actual, decision - timedelta(hours=1), decision)
    assert resolved.tick_value == resolved.tick_size * resolved.point_value
    assert "min_price_increment_amount" not in resolved.source_fields_used


def test_preverified_economics_resolution_is_bound_to_the_verified_registry_hash(
    release_factory, boundary, decision
) -> None:
    _, actual = _definition_registry(release_factory, boundary, decision)
    _, receipt = release_factory(
        release_kind="actual_contract_economics",
        filename="contract_economics.json",
        content=_economics_payload(actual, decision),
    )
    economics = VerifiedEconomicsRegistry.from_release(receipt, boundary)

    economics.verify()
    resolved = economics._resolve_preverified(
        actual,
        decision,
        expected_registry_hash=economics.registry_hash,
    )
    assert resolved.actual_identity_hash == actual.identity_hash
    with pytest.raises(IntegrityError, match="preverified snapshot hash changed"):
        economics._resolve_preverified(
            actual,
            decision,
            expected_registry_hash="0" * 64,
        )


@pytest.mark.parametrize(
    "overrides",
    (
        {"ambiguity_reasons": ["conflicting rate convention"]},
        {"tick_value": "0"},
        {"source_fields_used": ["min_price_increment_amount"]},
        {
            "asset_class": "RATES",
            "verification_source_ids": ["one_source"],
            "quote_convention_id": "",
        },
    ),
)
def test_ambiguous_or_unverified_economics_fail_closed(
    release_factory, boundary, decision, overrides
) -> None:
    _, actual = _definition_registry(release_factory, boundary, decision)
    _, receipt = release_factory(
        release_kind="actual_contract_economics",
        filename="contract_economics.json",
        content=_economics_payload(actual, decision, **overrides),
    )
    with pytest.raises(IntegrityError):
        VerifiedEconomicsRegistry.from_release(receipt, boundary)


def test_legacy_costs_evidence_cannot_become_active_economics(
    release_factory, boundary
) -> None:
    _, receipt = release_factory(
        release_kind="migration_evidence",
        filename="costs.yaml",
        content="ES:\n  tick_value: 12.5\n",
        metadata={"disposition": "economics_reconciliation_evidence_never_authoritative"},
    )
    with pytest.raises(IntegrityError, match="wrong release kind"):
        VerifiedEconomicsRegistry.from_release(receipt, boundary)


def test_legacy_costs_are_exact_hash_copy_evidence_only() -> None:
    manifest_path = Path(__file__).parents[1] / "configs" / "migration_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["copy_authorized"] is False
    entries = [
        item for item in manifest["entries"]
        if item["family"] == "legacy_costs_policy"
    ]
    assert entries == [
        {
            "destination": "evidence/configs/costs.yaml",
            "disposition": "economics_reconciliation_evidence_never_authoritative",
            "expected_bytes": 18_393,
            "expected_files": 1,
            "expected_sha256": (
                "e20fdb449f6dbdcd43184771eff439494207eb2b85f01172bd02434f0f4667d9"
            ),
            "family": "legacy_costs_policy",
            "kind": "file",
            "source": "configs/costs.yaml",
        }
    ]


def test_economics_release_tamper_invalidates_registry(
    release_factory, boundary, decision
) -> None:
    _, actual = _definition_registry(release_factory, boundary, decision)
    release, receipt = release_factory(
        release_kind="actual_contract_economics",
        filename="contract_economics.json",
        content=_economics_payload(actual, decision),
    )
    registry = VerifiedEconomicsRegistry.from_release(receipt, boundary)
    (release / "contract_economics.json").write_text('{"tampered":true}', encoding="utf-8")
    with pytest.raises(IntegrityError):
        registry.verify()
