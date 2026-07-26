from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from futures_rebuild.errors import ContractError, IntegrityError
from futures_rebuild.identity import (
    ActualContractIdentity,
    AsOfRollLedger,
    ContractDefinition,
    DefinitionObservation,
    EligibilityObservation,
    RetrospectiveMappingInterval,
    RollSelectionObservation,
    assert_single_actual_contract_segment,
    resolve_bar_identity,
)
from futures_rebuild.session_policy import VerifiedSessionPolicy
from futures_rebuild.time_contracts import (
    AvailabilityBasis,
    BarObservation,
    CausalTimestamp,
)


UTC = timezone.utc
POLICY_HASH = "9" * 64


def _definition(actual: ActualContractIdentity) -> ContractDefinition:
    return ContractDefinition(
        dataset=actual.dataset,
        publisher_id=actual.publisher_id,
        instrument_id=actual.instrument_id,
        raw_symbol=actual.raw_symbol,
        exchange=actual.exchange,
        definition_release_id=actual.definition_release_id,
        definition_manifest_sha256=actual.definition_manifest_sha256,
        definition_row_id=actual.definition_row_id,
        currency=actual.currency,
        multiplier=actual.multiplier,
        min_tick=actual.min_tick,
    )


def _policy(release_factory, boundary) -> VerifiedSessionPolicy:
    _, receipt = release_factory(
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
    return VerifiedSessionPolicy.from_release(receipt, boundary)


def _modeled_bar(start: datetime, *, latency: timedelta = timedelta(0)) -> BarObservation:
    available = start + timedelta(minutes=1) + latency
    return BarObservation(
        start,
        timedelta(minutes=1),
        available,
        available + timedelta(seconds=1),
        available + timedelta(minutes=1),
        AvailabilityBasis.MODELED_INTERVAL_END_PLUS_PINNED_LATENCY,
        POLICY_HASH,
        datetime(2026, 7, 15, tzinfo=UTC),
        modeled_publication_latency=latency,
    )


def test_same_bar_or_same_instant_entry_is_rejected() -> None:
    start = datetime(2026, 7, 14, 15, 0, tzinfo=UTC)
    with pytest.raises(ContractError):
        BarObservation(
            start,
            timedelta(minutes=1),
            start,
            start,
            start,
            AvailabilityBasis.MODELED_INTERVAL_END_PLUS_PINNED_LATENCY,
            POLICY_HASH,
            datetime(2026, 7, 15, tzinfo=UTC),
            modeled_publication_latency=timedelta(0),
        )
    with pytest.raises(ContractError):
        replace(_modeled_bar(start), planned_entry_at=start + timedelta(minutes=1))


def test_historical_ohlcv_uses_modeled_availability_without_invented_ts_recv() -> None:
    start = datetime(2026, 7, 14, 15, 0, tzinfo=UTC)
    item = _modeled_bar(start, latency=timedelta(seconds=2))
    assert item.source_received_at is None
    assert item.available_at == item.bar_end + timedelta(seconds=2)
    with pytest.raises(ContractError):
        replace(item, source_received_at=item.bar_end)
    with pytest.raises(ContractError, match="OHLCV bars have no ts_recv"):
        replace(
            item,
            availability_basis=AvailabilityBasis.PROVIDER_TS_RECV,
            source_received_at=item.bar_end,
            modeled_publication_latency=None,
        )


def test_completed_bar_and_later_entry_pass() -> None:
    start = datetime(2026, 7, 14, 15, 0, tzinfo=UTC)
    item = _modeled_bar(start)
    assert item.bar_end == start + timedelta(minutes=1)


def test_naive_and_out_of_order_timestamps_fail() -> None:
    naive = datetime(2026, 1, 1)
    aware = datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(ContractError):
        CausalTimestamp(naive, aware, aware)
    with pytest.raises(ContractError):
        CausalTimestamp(aware, aware - timedelta(seconds=1), aware)


def test_future_roll_observation_cannot_change_earlier_selection(contract, decision) -> None:
    next_contract = replace(
        contract,
        instrument_id=67890,
        raw_symbol="ESH7",
        definition_row_id="c" * 64,
    )
    prior = RollSelectionObservation(
        "ES.v.0", contract, decision - timedelta(days=1),
        decision - timedelta(hours=1), decision - timedelta(hours=2), "selection-release",
    )
    future = RollSelectionObservation(
        "ES.v.0", next_contract, decision + timedelta(minutes=1),
        decision + timedelta(minutes=2), decision, "selection-release",
    )
    assert AsOfRollLedger((prior, future)).select("ES.v.0", decision) == contract


def test_eligibility_tie_fails_independent_of_input_order(contract, decision) -> None:
    first = EligibilityObservation(
        contract, decision - timedelta(days=1), decision - timedelta(hours=1),
        True, (), "status-a",
    )
    conflict = EligibilityObservation(
        contract, first.effective_at, first.available_at, False, ("HALTED",), "status-b"
    )
    with pytest.raises(ContractError, match="eligibility"):
        AsOfRollLedger((), (first, conflict))
    with pytest.raises(ContractError, match="eligibility"):
        AsOfRollLedger((), (conflict, first))


def test_definition_requires_received_time_but_provider_clocks_are_independent(
    contract, decision
) -> None:
    with pytest.raises(TypeError):
        DefinitionObservation(  # type: ignore[call-arg]
            _definition(contract), decision - timedelta(days=2),
            decision - timedelta(days=1), contract.definition_release_id
        )
    with pytest.raises(ContractError):
        DefinitionObservation(
            _definition(contract), decision, decision - timedelta(seconds=1),
            contract.definition_release_id, decision + timedelta(seconds=1)
        )
    announced_before_activation = DefinitionObservation(
        _definition(contract),
        decision + timedelta(days=1),
        decision,
        contract.definition_release_id,
        decision - timedelta(seconds=1),
        expires_at=decision + timedelta(days=30),
        provider_event_at=decision + timedelta(seconds=1),
    )
    assert announced_before_activation.source_received_at < announced_before_activation.effective_at


def test_one_definition_resolves_multiple_bar_dates_and_reuse_is_asof(
    contract, decision, release_factory, boundary
) -> None:
    policy = _policy(release_factory, boundary)
    known = DefinitionObservation(
        _definition(contract), decision - timedelta(days=30),
        decision - timedelta(days=29), contract.definition_release_id,
        decision - timedelta(days=29, seconds=1),
    )
    first_event = datetime(2026, 7, 14, 15, 0, tzinfo=UTC)
    second_event = datetime(2026, 7, 15, 15, 0, tzinfo=UTC)
    first = resolve_bar_identity(
        contract.dataset, contract.publisher_id, contract.instrument_id, first_event,
        (known,), decision_at=decision, session_policy=policy,
    )
    second = resolve_bar_identity(
        contract.dataset, contract.publisher_id, contract.instrument_id, second_event,
        (known,), decision_at=second_event + timedelta(minutes=1), session_policy=policy,
    )
    assert first.instrument_id_date_utc == date(2026, 7, 14)
    assert second.instrument_id_date_utc == date(2026, 7, 15)
    reused_definition = replace(
        _definition(contract),
        raw_symbol="ESH7",
        definition_row_id="e" * 64,
    )
    reuse = DefinitionObservation(
        reused_definition,
        datetime(2026, 7, 16, tzinfo=UTC),
        datetime(2026, 7, 16, 0, 0, 1, tzinfo=UTC),
        reused_definition.definition_release_id,
        datetime(2026, 7, 16, 0, 0, 0, 500000, tzinfo=UTC),
    )
    reused = resolve_bar_identity(
        contract.dataset, contract.publisher_id, contract.instrument_id,
        datetime(2026, 7, 16, 15, tzinfo=UTC), (known, reuse),
        decision_at=datetime(2026, 7, 16, 15, 1, tzinfo=UTC), session_policy=policy,
    )
    assert first.raw_symbol == contract.raw_symbol and reused.raw_symbol == "ESH7"


def test_future_mapping_and_definition_cannot_change_earlier_identity(
    contract, decision, release_factory, boundary
) -> None:
    policy = _policy(release_factory, boundary)
    mapping = RetrospectiveMappingInterval(
        "ES.v.0", contract.instrument_id, decision - timedelta(days=1), decision + timedelta(days=30)
    )
    known = DefinitionObservation(
        _definition(contract), decision - timedelta(days=10), decision - timedelta(days=9),
        contract.definition_release_id, decision - timedelta(days=9, seconds=1),
    )
    future_definition = replace(
        _definition(contract), raw_symbol="WRONG", definition_row_id="f" * 64
    )
    future = DefinitionObservation(
        future_definition, decision - timedelta(days=10), decision + timedelta(seconds=2),
        future_definition.definition_release_id, decision + timedelta(seconds=1),
    )
    event = decision - timedelta(minutes=1)
    observed = resolve_bar_identity(
        contract.dataset, contract.publisher_id, contract.instrument_id, event,
        (known, future), decision_at=decision, session_policy=policy,
        retrospective_mappings=(mapping,),
    )
    assert observed.raw_symbol == contract.raw_symbol


def test_definition_change_within_observation_window_fails_closed(
    contract, decision, release_factory, boundary
) -> None:
    policy = _policy(release_factory, boundary)
    event = decision - timedelta(minutes=10)
    known = DefinitionObservation(
        _definition(contract), event - timedelta(days=2), event - timedelta(days=1),
        contract.definition_release_id, event - timedelta(days=1, seconds=1),
    )
    later_definition = replace(
        _definition(contract), raw_symbol="LATER", definition_row_id="d" * 64
    )
    later = DefinitionObservation(
        later_definition,
        event + timedelta(minutes=1),
        event + timedelta(minutes=3),
        later_definition.definition_release_id,
        event + timedelta(minutes=2),
    )
    with pytest.raises(ContractError, match="changed within"):
        resolve_bar_identity(
            contract.dataset,
            contract.publisher_id,
            contract.instrument_id,
            event,
            (known, later),
            decision_at=decision,
            session_policy=policy,
        )


def test_definition_index_date_prevents_cross_day_instrument_id_reuse(
    contract, release_factory, boundary
) -> None:
    policy = _policy(release_factory, boundary)
    event = datetime(2026, 7, 14, 15, tzinfo=UTC)
    observation = DefinitionObservation(
        _definition(contract),
        event - timedelta(days=30),
        event - timedelta(hours=1),
        contract.definition_release_id,
        event - timedelta(hours=1),
        expires_at=event + timedelta(days=30),
        definition_index_date_utc=event.date(),
        source_file_path="dbn/definition/ES/2026/example.dbn.zst",
    )
    resolved = resolve_bar_identity(
        contract.dataset,
        contract.publisher_id,
        contract.instrument_id,
        event,
        (observation,),
        decision_at=event + timedelta(minutes=1),
        session_policy=policy,
    )
    assert resolved.instrument_id_date_utc == event.date()
    with pytest.raises(ContractError, match="same-day"):
        resolve_bar_identity(
            contract.dataset,
            contract.publisher_id,
            contract.instrument_id,
            event + timedelta(days=1),
            (observation,),
            decision_at=event + timedelta(days=1, minutes=1),
            session_policy=policy,
        )


def test_utc_midnight_mapping_change_is_a_hard_contract_segment_boundary(
    contract, decision, release_factory, boundary
) -> None:
    policy = _policy(release_factory, boundary)
    front_definition = replace(
        _definition(contract), raw_symbol="ESU6", definition_row_id="1" * 64
    )
    back_definition = replace(
        _definition(contract), instrument_id=contract.instrument_id + 1,
        raw_symbol="ESZ6", definition_row_id="2" * 64,
    )
    available = datetime(2026, 7, 14, 20, tzinfo=UTC)
    observations = (
        DefinitionObservation(
            front_definition, available - timedelta(days=1), available,
            front_definition.definition_release_id, available - timedelta(seconds=1),
        ),
        DefinitionObservation(
            back_definition, available - timedelta(days=1), available,
            back_definition.definition_release_id, available - timedelta(seconds=1),
        ),
    )
    before_midnight = datetime(2026, 7, 14, 23, 59, tzinfo=UTC)
    after_midnight = datetime(2026, 7, 15, 0, 1, tzinfo=UTC)
    front = resolve_bar_identity(
        contract.dataset, contract.publisher_id, front_definition.instrument_id,
        before_midnight, observations, decision_at=before_midnight,
        session_policy=policy,
    )
    back = resolve_bar_identity(
        contract.dataset, contract.publisher_id, back_definition.instrument_id,
        after_midnight, observations, decision_at=after_midnight,
        session_policy=policy,
    )
    assert front.exchange_session_date == back.exchange_session_date
    with pytest.raises(ContractError, match="instrument_id boundary"):
        assert_single_actual_contract_segment(
            (front, back), purpose="synthetic midnight label"
        )


def test_same_actual_contract_can_remain_one_segment_across_utc_dates(contract) -> None:
    next_date = replace(
        contract,
        instrument_id_date_utc=contract.instrument_id_date_utc + timedelta(days=1),
    )
    assert contract.contract_segment_hash == next_date.contract_segment_hash
    assert_single_actual_contract_segment(
        (contract, next_date), purpose="same-contract lookback"
    )


def test_session_policy_release_tamper_fails_on_use(release_factory, boundary) -> None:
    policy = _policy(release_factory, boundary)
    path = boundary.active_root / policy.receipt.manifest_path
    path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(IntegrityError):
        policy.exchange_session_date("XCME", datetime(2026, 7, 14, tzinfo=UTC))


def test_preverified_session_date_requires_exact_policy_hash(
    release_factory, boundary
) -> None:
    policy = _policy(release_factory, boundary)
    policy.verify()
    event = datetime(2026, 7, 14, tzinfo=UTC)
    assert policy._exchange_session_date_preverified(
        "XCME",
        event,
        expected_policy_hash=policy.policy_hash,
    ) == policy.exchange_session_date("XCME", event)
    with pytest.raises(IntegrityError, match="policy hash changed"):
        policy._exchange_session_date_preverified(
            "XCME",
            event,
            expected_policy_hash="0" * 64,
        )


def test_full_identity_hash_changes_with_economics(contract) -> None:
    variants = (
        replace(contract, exchange="XCBT"),
        replace(contract, currency="EUR"),
        replace(contract, multiplier=Decimal("25")),
        replace(contract, min_tick=Decimal("0.5")),
    )
    assert all(item.identity_hash != contract.identity_hash for item in variants)
    assert contract.identity_hash == replace(contract).identity_hash
