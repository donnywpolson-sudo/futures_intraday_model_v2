from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from futures_rebuild.boundary import (
    EXTERNAL_AUTHORITY_KEYS,
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.clock import (
    ProductionClock,
    SyntheticClock,
    issue_production_clock,
    require_trusted_clock,
)
from futures_rebuild.economics import VerifiedEconomicsRegistry
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation
from futures_rebuild.identity import ActualContractIdentity
from futures_rebuild.inference import VerifiedIdentityRegistry
from futures_rebuild.session_policy import VerifiedSessionPolicy


def _definition_payload(decision, *, effective_at=None):
    return {
        "schema_version": "1.0.0",
        "records": [
            {
                "available_at": (decision - timedelta(seconds=10)).isoformat(),
                "currency": "USD",
                "dataset": "GLBX.MDP3",
                "effective_at": (
                    effective_at or decision - timedelta(days=2)
                ).isoformat(),
                "exchange": "XCME",
                "instrument_id": 12345,
                "min_tick": "0.25",
                "multiplier": "50",
                "publisher_id": 1,
                "raw_symbol": "ESZ6",
                "source_received_at": (decision - timedelta(seconds=20)).isoformat(),
            }
        ],
    }


def test_authority_registry_lifecycle_scope_and_types_are_fail_closed(
    boundary, operation_factory
) -> None:
    with pytest.raises(TypeError):
        EXTERNAL_AUTHORITY_KEYS["TEST"] = object()

    receipt = operation_factory("SYNTHETIC_OP", scope={"role": "mechanics"})
    receipt.verify(
        boundary,
        operation="SYNTHETIC_OP",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
        required_scope={"role": "mechanics"},
    )
    with pytest.raises(UnauthorizedOperation, match="exact required scope"):
        receipt.verify(
            boundary,
            operation="SYNTHETIC_OP",
            required_scope={"role": "mechanics", "extra": "forbidden"},
        )

    noncanonical = receipt.as_dict()
    noncanonical["scope"] = [["role", "mechanics"], ["role", "duplicate"]]
    with pytest.raises(IntegrityError, match="canonical"):
        OperationReceipt.from_dict(noncanonical).verify(boundary)
    wrong_bool = receipt.as_dict()
    wrong_bool["externally_authorized"] = 0
    with pytest.raises(IntegrityError, match="field types"):
        OperationReceipt.from_dict(wrong_bool)
    noncanonical_time = receipt.as_dict()
    noncanonical_time["issued_at"] = str(noncanonical_time["issued_at"]).replace(
        "+00:00", "Z"
    )
    with pytest.raises(IntegrityError, match="canonically encoded"):
        OperationReceipt.from_dict(noncanonical_time)
    with pytest.raises(UnauthorizedOperation, match="lifecycle"):
        replace(
            receipt,
            not_before=datetime.now(timezone.utc) + timedelta(hours=1),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=2),
        ).verify(boundary)


def test_clock_capabilities_are_exact_operation_and_repository_bound(
    boundary, operation_factory, tmp_path, decision
) -> None:
    controlled = operation_factory(
        "REGISTER_TRIAL",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
    )
    with pytest.raises(ContractError, match="factory"):
        ProductionClock(boundary, controlled, _factory_token=object())
    production = issue_production_clock(boundary, controlled)
    assert require_trusted_clock(
        production,
        boundary=boundary,
        operation_receipt=controlled,
        allow_synthetic=False,
    ) is production

    other_receipt = operation_factory(
        "REGISTER_TRIAL",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
    )
    with pytest.raises(ContractError, match="another repository or operation"):
        require_trusted_clock(
            production,
            boundary=boundary,
            operation_receipt=other_receipt,
            allow_synthetic=False,
        )

    synthetic_receipt = operation_factory("INFER")
    synthetic = SyntheticClock(boundary, synthetic_receipt, decision)
    with pytest.raises(ContractError, match="exact repository-issued"):
        require_trusted_clock(
            synthetic,
            boundary=boundary,
            operation_receipt=synthetic_receipt,
            allow_synthetic=False,
        )
    with pytest.raises(TypeError, match="final"):
        class ForbiddenSyntheticSubclass(SyntheticClock):
            pass

    foreign_active = tmp_path / "foreign-active"
    foreign_active.mkdir()
    (foreign_active / "configs").mkdir()
    (foreign_active / "bundles").mkdir()
    foreign = RepoBoundary(foreign_active.resolve())
    with pytest.raises(ContractError, match="another repository or operation"):
        require_trusted_clock(
            production,
            boundary=foreign,
            operation_receipt=controlled,
            allow_synthetic=False,
        )


def test_verified_registries_cannot_be_directly_constructed_and_reparse_bytes(
    boundary, release_factory, decision
) -> None:
    _, identity_receipt = release_factory(
        release_kind="actual_contract_definitions",
        filename="identities.json",
        content=_definition_payload(decision),
    )
    identities = VerifiedIdentityRegistry.from_release(identity_receipt, boundary)
    with pytest.raises(TypeError):
        VerifiedIdentityRegistry(
            identities.release_receipt,
            identities.definitions,
            identities.registry_hash,
            boundary,
        )

    observation = next(iter(identities.definitions.values()))
    actual = ActualContractIdentity.from_definition(
        observation.definition,
        instrument_id_date_utc=decision.date(),
        exchange_session_date=decision.date(),
    )
    assert identities.contains(actual, decision - timedelta(minutes=1), decision)

    _, late_receipt = release_factory(
        release_kind="actual_contract_definitions",
        filename="identities.json",
        content=_definition_payload(
            decision, effective_at=decision - timedelta(seconds=30)
        ),
    )
    late = VerifiedIdentityRegistry.from_release(late_receipt, boundary)
    late_definition = next(iter(late.definitions.values())).definition
    late_actual = ActualContractIdentity.from_definition(
        late_definition,
        instrument_id_date_utc=decision.date(),
        exchange_session_date=decision.date(),
    )
    assert not late.contains(late_actual, decision - timedelta(minutes=1), decision)


def test_economics_and_session_factories_bind_actual_contract_fields(
    boundary, release_factory, decision
) -> None:
    _, identity_receipt = release_factory(
        release_kind="actual_contract_definitions",
        filename="identities.json",
        content=_definition_payload(decision),
    )
    identities = VerifiedIdentityRegistry.from_release(identity_receipt, boundary)
    definition = next(iter(identities.definitions.values())).definition
    actual = ActualContractIdentity.from_definition(
        definition,
        instrument_id_date_utc=decision.date(),
        exchange_session_date=decision.date(),
    )
    _, economics_receipt = release_factory(
        release_kind="actual_contract_economics",
        filename="contract_economics.json",
        content={
            "schema_version": "1.0.0",
            "records": [
                {
                    "actual_identity_hash": actual.identity_hash,
                    "ambiguity_reasons": [],
                    "asset_class": "EQUITY_INDEX",
                    "available_at": (decision - timedelta(seconds=10)).isoformat(),
                    "currency": "USD",
                    "effective_at": (decision - timedelta(days=1)).isoformat(),
                    "point_value": "50",
                    "quote_convention_id": "INDEX_POINTS",
                    "source_fields_used": [
                        "min_price_increment",
                        "unit_of_measure_qty",
                    ],
                    "source_received_at": (
                        decision - timedelta(seconds=20)
                    ).isoformat(),
                    "tick_size": "0.50",
                    "tick_value": "25.00",
                    "verification_source_ids": ["cme_contract_spec"],
                }
            ],
        },
    )
    economics = VerifiedEconomicsRegistry.from_release(economics_receipt, boundary)
    with pytest.raises(ContractError, match="mismatched"):
        economics.resolve(actual, decision)
    with pytest.raises(TypeError):
        VerifiedEconomicsRegistry(
            economics.release_receipt,
            economics.records,
            economics.registry_hash,
            boundary,
        )

    _, policy_receipt = release_factory(
        release_kind="versioned_session_policy",
        filename="session_policy.json",
        content={
            "policy_version": "1.0.0",
            "rules": [
                {
                    "exchange": "XCME",
                    "post_roll_day_offset": 1,
                    "session_roll_local": "17:00:00",
                    "timezone": "America/Chicago",
                }
            ],
        },
    )
    policy = VerifiedSessionPolicy.from_release(policy_receipt, boundary)
    with pytest.raises(TypeError):
        VerifiedSessionPolicy(
            policy.receipt,
            policy.rules,
            policy.policy_hash,
            boundary,
        )
