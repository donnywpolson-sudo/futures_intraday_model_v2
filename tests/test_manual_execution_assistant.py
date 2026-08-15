from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from futures_rebuild.live_cockpit.execution.config import (
    load_capability_evidence,
    load_execution_config,
    stage_capability,
    validate_capability_evidence,
    validate_execution_config,
)
from futures_rebuild.live_cockpit.execution.manual_assistant import (
    ExecutionCapability,
    ManualAuthority,
    ManualExecutionAssistant,
    ManualReadinessInputs,
    ManualStateStore,
    ManualTradeState,
)
from futures_rebuild.live_cockpit.execution.runtime import ExecutionRuntime
from futures_rebuild.live_cockpit.protocol import PROTOCOL_VERSION, validate_command


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 8, 12, 15, 0, tzinfo=timezone.utc)


def readiness(*, now: datetime = NOW, stale: str | None = None) -> ManualReadinessInputs:
    current = now - timedelta(seconds=10)
    old = now - timedelta(minutes=10)
    return ManualReadinessInputs(
        now=now,
        market_data_at=old if stale == "market" else current,
        model_setup_at=old if stale == "model" else current,
        strategy_policy_status="UNBOUND" if stale == "policy" else "OOS_PROMOTED",
        session_status="UNBOUND" if stale == "session" else "OPEN_ENTRY_PERMITTED",
        session_record_at=old if stale == "session" else current,
        news_status="UNBOUND" if stale == "news" else "ENTRY_PERMITTED",
        news_record_at=old if stale == "news" else current,
        price_limit_status="UNBOUND" if stale == "limit" else "ENTRY_PERMITTED",
        price_limit_record_at=old if stale == "limit" else current,
    )


def snapshot_payload(alias: str = "Rapid Sim 1") -> dict[str, object]:
    return {
        "profile_id": "mff_rapid_eod_50k_2026_08_10",
        "stage": "sim_funded",
        "account_alias": alias,
        "nominal_plan_size_usd": 50_000,
        "realized_balance_usd": 0,
        "active_eod_floor_usd": -2_000,
        "floor_lock_status": "UNLOCKED",
        "current_session_realized_pnl_usd": 0,
        "open_positions": [],
        "working_entry_orders": [],
        "protective_orders": [],
        "payout_state": "NOT_ELIGIBLE",
        "reconciliation_notes": "Operator reviewed the simulated account.",
    }


def ticket_payload(contract: str = "MESU6", quantity: int = 1) -> dict[str, object]:
    signal = "ES" if contract.startswith("MES") else "CL" if contract.startswith("MCL") else "6E" if contract.startswith("M6E") else "ZN"
    prices = {
        "ES": (5482.25, 5476.25, 5494.25),
        "CL": (64.25, 63.95, 64.85),
        "6E": (1.1750, 1.1720, 1.1810),
        "ZN": (112.0, 111.0, 113.0),
    }[signal]
    return {
        "profile_id": "mff_rapid_eod_50k_2026_08_10",
        "stage": "sim_funded",
        "account_alias": "Rapid Sim 1",
        "signal_instrument": signal,
        "execution_contract": contract,
        "side": "BUY",
        "order_type": "LIMIT",
        "entry_price": prices[0],
        "stop_price": prices[1],
        "target_price": prices[2],
        "quantity": quantity,
        "strategy_candidate_id": "coarse-3",
    }


def submitted_report() -> dict[str, object]:
    return {"actual_submission_time": NOW.isoformat()}


def fill_report(*, contract: str = "MESU6", quantity: int = 1, price: float = 5482.5) -> dict[str, object]:
    return {
        "actual_contract": contract,
        "actual_side": "BUY",
        "actual_quantity": quantity,
        "actual_fill_price": price,
        "actual_stop": 5476.25,
        "actual_target": 5494.25,
        "actual_fill_time": (NOW + timedelta(seconds=1)).isoformat(),
        "actual_fees": 0,
    }


def assistant(tmp_path: Path) -> ManualExecutionAssistant:
    value = ManualExecutionAssistant(repository_root=ROOT, state_root=tmp_path)
    value.record_snapshot(snapshot_payload(), confirmation="RECONCILE Rapid Sim 1", now=NOW)
    return value


def test_mff_stage_capabilities_are_exact_and_future_values_remain_representable() -> None:
    config = load_execution_config(root=ROOT)
    connection = config["connections"][config["active_connection_id"]]
    for stage in ("evaluation", "sim_funded"):
        capability = stage_capability(connection, stage=stage)
        assert capability["execution_capability"] == ExecutionCapability.MANUAL_ONLY.value
        assert capability["direct_api_read_access"] is False
        assert capability["direct_api_order_access"] is False
        assert capability["provider_api_readiness"] is False
        assert capability["automatic_execution_authorized"] is False
    live = stage_capability(connection, stage="live")
    assert live["execution_capability"] == "UNCONFIRMED"
    assert live["entitlement_status"] == "PENDING_ACTUAL_LIVE_ACCOUNT_VERIFICATION"
    assert {member.value for member in ExecutionCapability} == {"MANUAL_ONLY", "READ_ONLY_API", "ORDER_API"}
    unknown = deepcopy(config)
    unknown["connections"][unknown["active_connection_id"]]["stage_capabilities"]["evaluation"]["execution_capability"] = "MAGIC_API"
    with pytest.raises(ValueError, match="manual-only"):
        validate_execution_config(unknown)


@pytest.mark.parametrize("stale", ["market", "model", "policy", "session", "news", "limit"])
def test_manual_preview_is_independent_and_stale_inputs_block_readiness(tmp_path: Path, stale: str) -> None:
    service = assistant(tmp_path)
    ticket = service.prepare_ticket(ticket_payload(), inputs=readiness(stale=stale))
    assert ticket.manual_ticket_preview_available is True
    assert ticket.manual_assistant_readiness is False
    assert ticket.provider_api_readiness is False
    assert ticket.automatic_execution_authorized is False


def test_operator_snapshot_is_bound_expires_and_restart_is_stale(tmp_path: Path) -> None:
    service = assistant(tmp_path)
    ready = service.prepare_ticket(ticket_payload(), inputs=readiness())
    snapshot = service.snapshot
    assert snapshot is not None
    assert snapshot.source_authority is ManualAuthority.OPERATOR_REPORTED
    assert snapshot.stage == "sim_funded"
    assert snapshot.profile_id == "mff_rapid_eod_50k_2026_08_10"
    assert snapshot.account_alias == "Rapid Sim 1"
    assert snapshot.current(now=NOW + timedelta(minutes=16), profile_id=snapshot.profile_id, stage=snapshot.stage, alias=snapshot.account_alias, restart_stale=False) is False
    restarted = ManualExecutionAssistant(repository_root=ROOT, state_root=tmp_path)
    assert restarted.restart_stale is True
    assert restarted.status_payload()["account_binding"] == "UNSET"
    stale_ticket = restarted.tickets[ready.ticket_id]
    assert stale_ticket.state is ManualTradeState.BLOCKED
    assert stale_ticket.manual_assistant_readiness is False
    assert stale_ticket.approved_quantity == 0
    assert stale_ticket.authority is ManualAuthority.MODEL_CALCULATED


def test_corrupt_state_fails_closed_without_overwriting_corrupt_bytes(tmp_path: Path) -> None:
    path = tmp_path / "manual_execution_state.json"
    path.write_bytes(b"{corrupt-bytes")
    before = path.read_bytes()
    store = ManualStateStore(tmp_path)
    assert store.corrupt is True
    service = ManualExecutionAssistant(repository_root=ROOT, state_root=tmp_path)
    assert service.status_payload()["state_uncertain"] is True
    assert path.read_bytes() == before


@pytest.mark.parametrize("contract", ["MESU6", "MCLU6", "M6EU6"])
def test_verified_micros_prepare_with_authoritative_runtime_and_provisional_labels(tmp_path: Path, contract: str) -> None:
    service = assistant(tmp_path)
    ticket = service.prepare_ticket(ticket_payload(contract), inputs=readiness())
    assert ticket.state is ManualTradeState.READY_FOR_MANUAL_ENTRY
    assert ticket.manual_assistant_readiness is True
    assert ticket.approved_quantity == 1
    assert ticket.authoritative_maximum_quantity >= 1
    assert ticket.cost_status == "PROVISIONAL_NOT_OFFICIAL"
    assert ticket.projected_micro_equivalents <= 30


@pytest.mark.parametrize("contract", ["ZNU6", "ESU6", "CLU6", "6EU6"])
def test_zn_and_minis_fail_closed(tmp_path: Path, contract: str) -> None:
    service = assistant(tmp_path)
    with pytest.raises(ValueError):
        service.prepare_ticket(ticket_payload(contract), inputs=readiness())


def test_quantity_stop_and_tick_alignment_fail_closed(tmp_path: Path) -> None:
    service = assistant(tmp_path)
    with pytest.raises(ValueError, match="quantity"):
        service.prepare_ticket(ticket_payload(quantity=0), inputs=readiness())
    missing = {**ticket_payload(), "stop_price": 5482.25}
    with pytest.raises(ValueError, match="positive risk"):
        service.prepare_ticket(missing, inputs=readiness())
    off_tick = {**ticket_payload(), "entry_price": 5482.26}
    with pytest.raises(ValueError, match="tick aligned"):
        service.prepare_ticket(off_tick, inputs=readiness())


def test_submitted_unknown_exposure_and_unprotected_fill_block_new_tickets(tmp_path: Path) -> None:
    service = assistant(tmp_path)
    ticket = service.prepare_ticket(ticket_payload(), inputs=readiness())
    service.transition(ticket.ticket_id, "OPERATOR_REPORTED_SUBMITTED", {"actual_submission_time": NOW.isoformat()}, now=NOW)
    service.transition(ticket.ticket_id, "OPERATOR_REPORTED_FILLED", fill_report(), now=NOW)
    service.record_snapshot(snapshot_payload(), confirmation="RECONCILE Rapid Sim 1", now=NOW + timedelta(seconds=1))
    blocked = service.prepare_ticket({**ticket_payload("MCLU6"), "account_alias": "Rapid Sim 1"}, inputs=readiness(now=NOW + timedelta(seconds=1)))
    assert "UNPROTECTED_POSITION" in blocked.blocker_reason_codes


def test_state_machine_valid_path_invalid_transition_and_no_broker_authority(tmp_path: Path) -> None:
    service = assistant(tmp_path)
    ticket = service.prepare_ticket(ticket_payload(quantity=2), inputs=readiness())
    submitted = service.transition(ticket.ticket_id, "OPERATOR_REPORTED_SUBMITTED", submitted_report(), now=NOW)
    assert submitted.authority is ManualAuthority.OPERATOR_REPORTED
    partial = service.transition(ticket.ticket_id, "OPERATOR_REPORTED_PARTIALLY_FILLED", {"partial_fills": [{"quantity": 1, "price": 5482.5, "time": (NOW + timedelta(milliseconds=500)).isoformat()}]}, now=NOW)
    assert partial.state is ManualTradeState.OPERATOR_REPORTED_PARTIALLY_FILLED
    filled = service.transition(ticket.ticket_id, "OPERATOR_REPORTED_FILLED", fill_report(quantity=2), now=NOW)
    protected = service.transition(ticket.ticket_id, "OPERATOR_CONFIRMED_PROTECTED", {"actual_stop": 5476.25, "confirmed_at": (NOW + timedelta(seconds=1)).isoformat()}, now=NOW)
    assert protected.authority is ManualAuthority.OPERATOR_CONFIRMED
    service.transition(ticket.ticket_id, "OPERATOR_REPORTED_CLOSED", {"actual_exit_price": 5494.25, "actual_exit_time": (NOW + timedelta(seconds=2)).isoformat(), "actual_fees": 0}, now=NOW)
    reconciled = service.transition(ticket.ticket_id, "OPERATOR_RECONCILED", {"reconciliation_notes": "Operator reconciled final state."}, now=NOW)
    assert reconciled.state is ManualTradeState.OPERATOR_RECONCILED
    assert reconciled.authority is not ManualAuthority.BROKER_CONFIRMED
    with pytest.raises(ValueError, match="invalid manual transition"):
        service.transition(ticket.ticket_id, "OPERATOR_REPORTED_SUBMITTED", {}, now=NOW)


def test_copy_summary_and_planned_actual_comparison_are_truthful(tmp_path: Path) -> None:
    service = assistant(tmp_path)
    ticket = service.prepare_ticket(ticket_payload(), inputs=readiness())
    summary = service.copy_summary(ticket.ticket_id)
    assert "Execution capability: MANUAL_ONLY" in summary
    assert "MESU6" in summary
    assert "PROVISIONAL_NOT_OFFICIAL" in summary
    assert "NO ORDER HAS BEEN TRANSMITTED BY FUTURESLIVECOCKPIT" in summary
    assert "account_id" not in summary and "token" not in summary.lower()
    service.transition(ticket.ticket_id, "OPERATOR_REPORTED_SUBMITTED", submitted_report(), now=NOW)
    service.transition(ticket.ticket_id, "OPERATOR_REPORTED_FILLED", {**fill_report(contract="MESZ6", quantity=2), "actual_stop": 5475.25, "actual_target": 5494.0}, now=NOW)
    comparison = service.comparison(ticket.ticket_id)
    assert comparison["entry_slippage_ticks"] == "1"
    assert comparison["entry_slippage_usd"] == "2.50"
    assert "WRONG_CONTRACT_MONTH_OR_SYMBOL" in comparison["alerts"]
    assert "QUANTITY_ABOVE_AUTHORIZED" in comparison["alerts"]
    assert "STOP_PRICE_MISMATCH" in comparison["alerts"]
    assert "ACTUAL_RISK_ABOVE_AUTHORIZED_RISK" in comparison["alerts"]
    assert comparison["operator_reported"] is True


def test_manual_runtime_does_not_read_binding_or_construct_provider_clients(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import futures_rebuild.live_cockpit.execution.runtime as runtime_module
    monkeypatch.setattr(runtime_module, "load_account_binding", lambda **_kwargs: pytest.fail("manual mode must not read API binding"))
    runtime = ExecutionRuntime(root=ROOT, state_root=tmp_path)
    payload = runtime.capability_payload()
    assert payload["mode"] == "MFF_MANUAL_ASSISTANT"
    assert payload["order_paths_reachable"] is False
    assert payload["account_binding_present"] is False
    assert runtime.adapter.provider_id == "NONE"
    assert runtime.self_check_payload()["credential_check_mode"] == "NOT_ACCESSED"


def test_manual_actions_never_invoke_any_supplied_provider_adapter(tmp_path: Path) -> None:
    class FailIfCalled:
        provider_id = "tradovate"
        connected = False

        def __getattr__(self, name: str):
            raise AssertionError(f"manual mode touched provider adapter method: {name}")

    runtime = ExecutionRuntime(root=ROOT, state_root=tmp_path, adapter=FailIfCalled())
    assert runtime.adapter.provider_id == "NONE"
    runtime.record_operator_snapshot(snapshot_payload(), confirmation="RECONCILE Rapid Sim 1")
    result = runtime.prepare_manual_ticket(ticket_payload())
    assert result["ok"] is True
    runtime.shutdown()


def test_manual_protocol_is_narrow_versioned_and_rejects_secret_fields() -> None:
    command = {
        "v": PROTOCOL_VERSION,
        "type": "PREPARE_MANUAL_TICKET",
        "payload": ticket_payload(),
    }
    validate_command(command)
    with pytest.raises(ValueError, match="forbidden secret"):
        validate_command({**command, "payload": {**command["payload"], "accessToken": "no"}})
    with pytest.raises(ValueError, match="fields are not exact"):
        validate_command({**command, "payload": {**command["payload"], "method": "place_order"}})


def test_evidence_record_is_bounded_nonsecret_and_narrow() -> None:
    document = load_capability_evidence(root=ROOT)
    evidence = document["evidence"][0]
    assert evidence["source_type"] == "USER_SUPPLIED_FIRST_PARTY_SUPPORT_RESPONSE"
    assert evidence["scope"] == ["evaluation", "rapid_eod_sim_funded"]
    assert "DOES_NOT_ESTABLISH_FUTURE_MFF_LIVE_ACCOUNT_CAPABILITY" in evidence["limitations"]
    assert "DOES_NOT_AUTHORIZE_PROVIDER_CONNECTION" in evidence["limitations"]
    assert evidence["transcript_handling"] == "FULL_TRANSCRIPT_EXCLUDED_FROM_GIT_USER_CONTROLLED_OUTSIDE_REPOSITORY"
    secret = deepcopy(document)
    secret["evidence"][0]["chatId"] = "not-allowed"
    with pytest.raises(ValueError, match="secret-bearing"):
        validate_capability_evidence(secret)
    overclaim = deepcopy(document)
    overclaim["evidence"][0]["supported_configuration_fields"].append("live.execution_capability")
    with pytest.raises(ValueError, match="unexpected fields"):
        validate_capability_evidence(overclaim)


def test_quantity_rounds_down_and_contract_maturity_and_direction_fail_closed(tmp_path: Path) -> None:
    service = assistant(tmp_path)
    rounded = service.prepare_ticket(ticket_payload(quantity=1.9), inputs=readiness())
    assert rounded.requested_quantity == 1
    assert rounded.contract_month_code == "U"
    assert rounded.contract_year == 2026
    with pytest.raises(ValueError, match="maturity"):
        service.prepare_ticket(ticket_payload("MESH6"), inputs=readiness())
    with pytest.raises(ValueError, match="directionally valid"):
        service.prepare_ticket({**ticket_payload("MCLU6"), "stop_price": 64.55}, inputs=readiness())


def test_submitted_and_partial_exposure_block_until_reconciled(tmp_path: Path) -> None:
    service = assistant(tmp_path)
    ticket = service.prepare_ticket(ticket_payload(quantity=2), inputs=readiness())
    service.transition(ticket.ticket_id, "OPERATOR_REPORTED_SUBMITTED", submitted_report(), now=NOW)
    service.record_snapshot(snapshot_payload(), confirmation="RECONCILE Rapid Sim 1", now=NOW + timedelta(seconds=1))
    submitted_block = service.prepare_ticket(ticket_payload("MCLU6"), inputs=readiness(now=NOW + timedelta(seconds=1)))
    assert "UNRESOLVED_MANUAL_ORDER_OR_POSITION" in submitted_block.blocker_reason_codes
    partial = service.transition(
        ticket.ticket_id,
        "OPERATOR_REPORTED_PARTIALLY_FILLED",
        {"partial_fills": [{"quantity": 1, "price": 5482.5, "time": (NOW + timedelta(seconds=2)).isoformat()}]},
        now=NOW + timedelta(seconds=2),
    )
    assert partial.actual["actual_quantity"] == 1
    uncertain = service.transition(
        ticket.ticket_id,
        "OPERATOR_REPORTED_CANCELLED",
        {"cancelled_at": (NOW + timedelta(seconds=3)).isoformat()},
        now=NOW + timedelta(seconds=3),
    )
    assert uncertain.state is ManualTradeState.STATE_UNCERTAIN


def test_equivalent_ticket_working_order_and_same_underlying_hedge_fail_closed(tmp_path: Path) -> None:
    service = assistant(tmp_path)
    first = service.prepare_ticket(ticket_payload(), inputs=readiness())
    duplicate = service.prepare_ticket(ticket_payload(), inputs=readiness())
    assert first.state is ManualTradeState.READY_FOR_MANUAL_ENTRY
    assert "EQUIVALENT_PENDING_TICKET" in duplicate.blocker_reason_codes

    hedged_snapshot = snapshot_payload()
    hedged_snapshot["open_positions"] = [{
        "signal_root": "ES", "execution_symbol": "MES", "execution_contract": "MESU6",
        "side": "LONG", "quantity": 1, "stop_ticks": 24,
        "protection_status": "CONFIRMED_WORKING",
    }]
    other = ManualExecutionAssistant(repository_root=ROOT, state_root=tmp_path / "hedge")
    other.record_snapshot(hedged_snapshot, confirmation="RECONCILE Rapid Sim 1", now=NOW)
    sell = {**ticket_payload(), "side": "SELL", "stop_price": 5488.25, "target_price": 5470.25}
    hedge = other.prepare_ticket(sell, inputs=readiness())
    assert "SAME_UNDERLYING_HEDGE_PROHIBITED" in hedge.blocker_reason_codes

    working_snapshot = snapshot_payload()
    working_snapshot["working_entry_orders"] = [{
        "signal_root": "ES", "execution_symbol": "MES", "execution_contract": "MESU6",
        "side": "BUY", "quantity": 0, "requested_quantity": 1, "stop_ticks": 24,
        "fill_status": "UNKNOWN", "order_type": "LIMIT", "entry_price": 5482.25,
    }]
    working = ManualExecutionAssistant(repository_root=ROOT, state_root=tmp_path / "working")
    working.record_snapshot(working_snapshot, confirmation="RECONCILE Rapid Sim 1", now=NOW)
    blocked = working.prepare_ticket(ticket_payload(), inputs=readiness())
    assert "DUPLICATE_WORKING_ORDER" in blocked.blocker_reason_codes


def test_unprotected_snapshot_and_reported_close_remain_blocking(tmp_path: Path) -> None:
    value = snapshot_payload()
    value["open_positions"] = [{
        "signal_root": "CL", "execution_symbol": "MCL", "execution_contract": "MCLU6",
        "side": "LONG", "quantity": 1, "stop_ticks": 30,
        "protection_status": "NOT_CONFIRMED",
    }]
    service = ManualExecutionAssistant(repository_root=ROOT, state_root=tmp_path)
    service.record_snapshot(value, confirmation="RECONCILE Rapid Sim 1", now=NOW)
    blocked = service.prepare_ticket(ticket_payload(), inputs=readiness())
    assert "UNPROTECTED_POSITION" in blocked.blocker_reason_codes

    lifecycle = assistant(tmp_path / "lifecycle")
    ticket = lifecycle.prepare_ticket(ticket_payload(), inputs=readiness())
    lifecycle.transition(ticket.ticket_id, "OPERATOR_REPORTED_SUBMITTED", submitted_report(), now=NOW)
    lifecycle.transition(ticket.ticket_id, "OPERATOR_REPORTED_FILLED", fill_report(), now=NOW)
    lifecycle.transition(ticket.ticket_id, "OPERATOR_CONFIRMED_PROTECTED", {"actual_stop": 5476.25, "confirmed_at": (NOW + timedelta(seconds=2)).isoformat()}, now=NOW)
    lifecycle.transition(ticket.ticket_id, "OPERATOR_REPORTED_CLOSED", {"actual_exit_price": 5494.25, "actual_exit_time": (NOW + timedelta(seconds=3)).isoformat(), "actual_fees": 3}, now=NOW)
    lifecycle.record_snapshot(snapshot_payload(), confirmation="RECONCILE Rapid Sim 1", now=NOW + timedelta(seconds=4))
    still_blocked = lifecycle.prepare_ticket(ticket_payload("MCLU6"), inputs=readiness(now=NOW + timedelta(seconds=4)))
    assert "UNRESOLVED_MANUAL_ORDER_OR_POSITION" in still_blocked.blocker_reason_codes


def test_blocked_copy_is_non_executable_and_material_comparison_sets_uncertain(tmp_path: Path) -> None:
    service = assistant(tmp_path)
    blocked = service.prepare_ticket(ticket_payload(), inputs=readiness(stale="market"))
    text = service.copy_summary(blocked.ticket_id)
    assert "Quantity: 0 micros" in text
    assert "BLOCKED PREVIEW - DO NOT ENTER OR TRANSMIT" in text
    assert "Enter and verify this order manually" not in text

    service = assistant(tmp_path / "comparison")
    ticket = service.prepare_ticket(ticket_payload(), inputs=readiness())
    service.transition(ticket.ticket_id, "OPERATOR_REPORTED_SUBMITTED", submitted_report(), now=NOW)
    service.transition(ticket.ticket_id, "OPERATOR_REPORTED_FILLED", {**fill_report(), "actual_contract": "MESZ6"}, now=NOW)
    result = service.comparison(ticket.ticket_id)
    assert result["resulting_state"] == "STATE_UNCERTAIN"
    assert service.status_payload()["state_uncertain"] is True


def test_nested_secret_account_and_private_path_data_are_rejected(tmp_path: Path) -> None:
    service = ManualExecutionAssistant(repository_root=ROOT, state_root=tmp_path)
    for forbidden in (
        {"accessToken": "secret"},
        {"nested": {"accountId": 42}},
        {"note": r"C:\\Users\\operator\\private.txt"},
    ):
        value = snapshot_payload()
        value["open_positions"] = [{
            "signal_root": "ES", "execution_symbol": "MES", "execution_contract": "MESU6",
            "side": "LONG", "quantity": 1, "stop_ticks": 24,
            "protection_status": forbidden,
        }]
        with pytest.raises(ValueError, match="forbidden|private path"):
            service.record_snapshot(value, confirmation="RECONCILE Rapid Sim 1", now=NOW)


def test_journal_corruption_is_detected_and_corrupt_bytes_are_preserved_on_reconciliation(tmp_path: Path) -> None:
    service = assistant(tmp_path)
    journal = tmp_path / "manual_execution_events.jsonl"
    corrupt = journal.read_bytes() + b"{broken\n"
    journal.write_bytes(corrupt)
    restarted = ManualExecutionAssistant(repository_root=ROOT, state_root=tmp_path)
    assert restarted.status_payload()["state_uncertain"] is True
    assert journal.read_bytes() == corrupt
    restarted.record_snapshot(snapshot_payload(), confirmation="RECONCILE Rapid Sim 1", now=NOW + timedelta(seconds=1))
    archives = list(tmp_path.glob("manual_execution_events.corrupt-*.jsonl"))
    assert len(archives) == 1
    assert archives[0].read_bytes() == corrupt
    assert journal.read_text(encoding="utf-8").count("\n") == 1


def test_manual_protocol_rejects_nested_account_private_path_and_nonexact_reports() -> None:
    base = {
        "v": PROTOCOL_VERSION,
        "type": "TRANSITION_MANUAL_TICKET",
        "payload": {
            "ticket_id": "manual-123",
            "target": "OPERATOR_REPORTED_SUBMITTED",
            "report": {"actual_submission_time": NOW.isoformat()},
        },
    }
    validate_command(base)
    with pytest.raises(ValueError, match="forbidden account"):
        validate_command({**base, "payload": {**base["payload"], "report": {"nested": {"providerAccountId": 7}}}})
    with pytest.raises(ValueError, match="forbidden account"):
        validate_command({**base, "payload": {**base["payload"], "report": {"operator_notes": r"C:\\Users\\operator\\state.json"}}})
    with pytest.raises(ValueError, match="fields are not exact"):
        validate_command({**base, "payload": {**base["payload"], "report": {"actual_submission_time": NOW.isoformat(), "extra": "x"}}})


def test_all_manual_runtime_paths_and_restart_leave_provider_adapter_untouched(tmp_path: Path) -> None:
    class FailIfTouched:
        provider_id = "LOCAL_EXECUTION_SIMULATOR"
        connected = False

        def __getattr__(self, name: str):
            raise AssertionError(f"manual workflow touched provider adapter member: {name}")

    def ready_runtime(name: str) -> tuple[ExecutionRuntime, dict[str, object]]:
        runtime = ExecutionRuntime(root=ROOT, state_root=tmp_path / name, adapter=FailIfTouched())
        assert runtime.capability_payload()["provider_api_readiness"] is False
        assert runtime.self_check_payload()["credential_check_mode"] == "NOT_ACCESSED"
        runtime.record_operator_snapshot(snapshot_payload(), confirmation="RECONCILE Rapid Sim 1")
        result = runtime.prepare_manual_ticket(ticket_payload(quantity=2))
        assert result["ticket"]["state"] == "READY_FOR_MANUAL_ENTRY"
        assert "NO ORDER HAS BEEN TRANSMITTED" in result["copy_text"]
        return runtime, result

    runtime, result = ready_runtime("filled")
    ticket_id = str(result["ticket"]["ticket_id"])
    runtime.transition_manual_ticket(ticket_id, "OPERATOR_REPORTED_SUBMITTED", submitted_report())
    runtime.transition_manual_ticket(ticket_id, "OPERATOR_REPORTED_PARTIALLY_FILLED", {"partial_fills": [{"quantity": 1, "price": 5482.25, "time": (NOW + timedelta(milliseconds=500)).isoformat()}]})
    runtime.transition_manual_ticket(ticket_id, "OPERATOR_REPORTED_FILLED", fill_report(quantity=2, price=5482.25))
    runtime.transition_manual_ticket(ticket_id, "OPERATOR_CONFIRMED_PROTECTED", {"actual_stop": 5476.25, "confirmed_at": (NOW + timedelta(seconds=2)).isoformat()})
    compared = runtime.compare_manual_ticket(ticket_id, {
        "actual_contract": "MESU6", "actual_side": "BUY", "actual_quantity": 2,
        "actual_fill_price": 5482.25, "actual_stop": 5476.25, "actual_target": 5494.25,
        "actual_fees": 3,
    })
    assert compared["comparison"]["operator_reported"] is True
    runtime.transition_manual_ticket(ticket_id, "OPERATOR_REPORTED_CLOSED", {"actual_exit_price": 5494.25, "actual_exit_time": (NOW + timedelta(seconds=3)).isoformat(), "actual_fees": 3})
    runtime.transition_manual_ticket(ticket_id, "OPERATOR_RECONCILED", {"reconciliation_notes": "Final state reconciled."})
    restarted = ExecutionRuntime(root=ROOT, state_root=tmp_path / "filled", adapter=FailIfTouched())
    assert restarted.manual_assistant.status_payload()["operator_snapshot_stale"] is True
    assert restarted.manual_assistant.store.persisted["pending_automatic_action"] is False

    for name, terminal, report in (
        ("rejected", "OPERATOR_REPORTED_REJECTED", {"actual_rejection_reason": "Operator reported rejection"}),
        ("cancelled", "OPERATOR_REPORTED_CANCELLED", {"cancelled_at": NOW.isoformat()}),
    ):
        other, prepared = ready_runtime(name)
        other_id = str(prepared["ticket"]["ticket_id"])
        other.transition_manual_ticket(other_id, "OPERATOR_REPORTED_SUBMITTED", submitted_report())
        other.transition_manual_ticket(other_id, terminal, report)
        other.transition_manual_ticket(other_id, "OPERATOR_RECONCILED", {"reconciliation_notes": "Terminal state reconciled."})

    abandoned, prepared = ready_runtime("abandoned")
    abandoned.transition_manual_ticket(str(prepared["ticket"]["ticket_id"]), "ABANDONED", {"operator_notes": "Operator abandoned local ticket."})
    uncertain, prepared = ready_runtime("uncertain")
    uncertain_id = str(prepared["ticket"]["ticket_id"])
    uncertain.transition_manual_ticket(uncertain_id, "STATE_UNCERTAIN", {"operator_notes": "Operator cannot determine state."})
    uncertain.transition_manual_ticket(uncertain_id, "OPERATOR_RECONCILED", {"reconciliation_notes": "Uncertain state reconciled."})
