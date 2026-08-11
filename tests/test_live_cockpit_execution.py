from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path

import pytest

from futures_rebuild.errors import ContractError
from futures_rebuild.live_cockpit.execution.arm_state import ArmState
from futures_rebuild.live_cockpit.execution.config import (
    load_execution_config,
    validate_execution_config,
)
from futures_rebuild.live_cockpit.execution.credential_store import (
    CredentialReference,
    MemoryCredentialStore,
    WindowsCredentialStore,
    redact,
)
from futures_rebuild.live_cockpit.execution.domain import (
    AccountBinding,
    EventOrigin,
    ExecutionMode,
    IntentSource,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    public_payload,
)
from futures_rebuild.live_cockpit.execution.errors import (
    ExecutionBlocked,
    TransportError,
    UnknownBrokerState,
)
from futures_rebuild.live_cockpit.execution.fake import (
    FakeHttpTransport,
    FakeWebSocketTransport,
    LocalExecutionSimulator,
)
from futures_rebuild.live_cockpit.execution.gate import (
    ProviderGateContext,
    evaluate_provider_intent,
    local_simulator_sizing,
)
from futures_rebuild.live_cockpit.execution.order_ledger import OrderLedger
from futures_rebuild.live_cockpit.execution.preparation import (
    LIVE_ORDER_OPERATION,
    READ_ONLY_OPERATION,
    SIM_ORDER_OPERATION,
    prepare_scope,
)
from futures_rebuild.live_cockpit.execution.reconciliation import (
    Reconciler,
    broker_snapshot_from_entities,
)
from futures_rebuild.live_cockpit.execution.runtime import ExecutionRuntime
from futures_rebuild.live_cockpit.execution.tradovate_auth import (
    AccessToken,
    TokenManager,
    TradovateAuthMaterial,
)
from futures_rebuild.live_cockpit.execution.tradovate_adapter import TradovateAdapter
from futures_rebuild.live_cockpit.execution.tradovate_rest import (
    OperationAuthority,
    ServiceRoots,
    TradovateRestClient,
    TransportResponse,
)
from futures_rebuild.live_cockpit.execution.tradovate_websocket import (
    TradovateUserSync,
    parse_server_frame,
    request_frame,
)
from futures_rebuild.live_cockpit.protocol import (
    PROTOCOL_VERSION,
    event,
    validate_command,
    validate_event,
)
from futures_rebuild.live_cockpit.single_instance import SingleInstance
from futures_rebuild.prop_firm_account_runtime import PortfolioRiskState


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 8, 11, 18, 0, tzinfo=timezone.utc)
HASH = "a" * 64


def binding() -> AccountBinding:
    return AccountBinding(
        binding_id="binding-1",
        provider_id="my_funded_futures",
        platform_id="tradovate",
        account_stage="sim_funded",
        environment="demo",
        account_id=101,
        account_spec="MFF-SIM-101",
        user_id=202,
        profile_id="mff_rapid_eod_50k_2026_08_10",
        profile_hash=HASH,
        connection_id="mff-tradovate",
        connection_hash=HASH,
        instrument_mapping_id="mff-micro-map",
        instrument_mapping_hash=HASH,
        cost_profile_id="mff-costs",
        cost_profile_hash=HASH,
        created_at=NOW,
        evidence_reference="operator-confirmed fixture",
        binding_hash=HASH,
    )


def intent(*, intent_id: str = "intent-1", quantity: int = 2) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        created_at=NOW,
        source=IntentSource.MODEL_PROPOSAL,
        account_binding_id="binding-1",
        account_binding_hash=HASH,
        profile_id="mff_rapid_eod_50k_2026_08_10",
        profile_hash=HASH,
        account_stage="sim_funded",
        signal_instrument="ES",
        execution_symbol="MES",
        contract_id=301,
        underlying_risk_group="SP500_EQUITY_INDEX",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        requested_quantity=quantity,
        entry_price=5000.0,
        stop_price=4995.0,
        target_price=5010.0,
        time_in_force="GTC",
        market_data_at=NOW,
        broker_state_at=NOW,
        calendar_hash=HASH,
        news_hash=HASH,
        price_limit_hash=HASH,
        strategy_policy_id="coarse-3",
        strategy_policy_hash=HASH,
        cost_profile_id="mff-costs",
        cost_profile_hash=HASH,
    )


def token() -> AccessToken:
    return AccessToken("test-token", NOW + timedelta(minutes=90), 202, "Active")


def test_execution_configuration_is_explicit_and_fail_closed() -> None:
    config = load_execution_config(root=ROOT)
    validate_execution_config(config)
    connection = config["connections"][config["active_connection_id"]]
    assert config["startup_mode"] == "OBSERVATION_ONLY"
    assert connection["entitlement_status"] == "UNCONFIRMED"
    assert connection["official_cost_profile"] == "UNSET"
    assert connection["production_readiness"] is False
    assert connection["execution_authorized"] is False
    assert all(value["exact_account_binding"] == "UNSET" for value in connection["stage_bindings"].values())


def test_missing_or_invalid_execution_config_degrades_to_observation_only(tmp_path: Path) -> None:
    runtime = ExecutionRuntime(root=tmp_path)
    payload = runtime.capability_payload()
    assert payload["mode"] == "OBSERVATION_ONLY"
    assert payload["order_paths_reachable"] is False
    assert "EXECUTION_CONFIG_MISSING_OR_INVALID" in payload["blockers"]


def test_runtime_never_constructs_or_connects_a_provider_adapter() -> None:
    runtime = ExecutionRuntime(root=ROOT)
    payload = runtime.capability_payload()
    assert runtime.adapter.provider_id == "NONE"
    assert runtime.adapter.connected is False
    assert payload["provider_connection_opened"] is False
    assert payload["account_stage"] == "sim_funded"
    assert payload["entitlement_status"] == "UNCONFIRMED"
    assert payload["exact_costs_verified"] is False
    assert payload["production_readiness"] is False
    assert payload["armed"] is False


def test_execution_self_check_never_reads_the_credential_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        WindowsCredentialStore,
        "exists",
        lambda *_args, **_kwargs: pytest.fail("self-check must not access credentials"),
    )

    payload = ExecutionRuntime(root=ROOT).self_check_payload()

    assert payload["credential_check_mode"] == "NOT_ACCESSED"
    assert payload["credential_configured"] is None


def test_demo_runtime_is_visibly_local_and_never_provider_backed() -> None:
    runtime = ExecutionRuntime(root=ROOT, adapter=LocalExecutionSimulator())
    payload = runtime.capability_payload()
    assert payload["mode"] == "LOCAL_EXECUTION_SIMULATOR"
    assert payload["origin"] == "LOCAL_SIMULATOR"
    assert payload["simulated"] is True
    assert payload["provider_id"] == "LOCAL_EXECUTION_SIMULATOR"
    assert payload["provider_connection_opened"] is False
    assert payload["execution_authorized"] is False


def test_arm_state_is_memory_only_exact_and_never_restored() -> None:
    state = ArmState(mode=ExecutionMode.MFF_TRADOVATE_SIM_FUNDED)
    with pytest.raises(ExecutionBlocked, match="production readiness"):
        state.arm(binding=binding(), confirmation="ARM MFF-SIM-101 sim_funded", now=NOW, production_readiness=False)
    armed = state.arm(binding=binding(), confirmation="ARM MFF-SIM-101 sim_funded", now=NOW, production_readiness=True)
    assert armed.armed is True
    assert ArmState(mode=ExecutionMode.MFF_TRADOVATE_SIM_FUNDED).snapshot(now=NOW).armed is False
    assert state.snapshot(now=NOW + timedelta(minutes=6)).armed is False


def test_protocol_v2_rejects_unknown_secret_malformed_and_oversized_messages() -> None:
    capability = ExecutionRuntime(root=ROOT).capability_payload()
    message = event("execution_capability", capability)
    validate_event(message)
    assert message["v"] == PROTOCOL_VERSION == 2
    with pytest.raises(ValueError, match="forbidden secret"):
        event("bootstrap", {"access_token": "do-not-leak"})
    for field in ("accessToken", "apiKey", "clientSecret"):
        with pytest.raises(ValueError, match="forbidden secret"):
            event("bootstrap", {field: "do-not-leak"})
    with pytest.raises(ValueError, match="unsupported cockpit event"):
        event("provider_passthrough", {})
    with pytest.raises(ValueError, match="oversized"):
        event("bootstrap", {"text": "x" * 5_000_000})


def test_commands_are_enumerated_exact_and_require_a_stop() -> None:
    command = {
        "v": 2,
        "type": "PREVIEW_ORDER_INTENT",
        "payload": {
            "execution_symbol": "MES",
            "side": "BUY",
            "order_type": "LIMIT",
            "price": 5000.0,
            "stop_price": 4995.0,
            "target_price": 5010.0,
            "quantity": 1,
        },
    }
    validate_command(command)
    with pytest.raises(ValueError, match="protective stop"):
        validate_command({**command, "payload": {**command["payload"], "stop_price": None}})
    with pytest.raises(ValueError, match="forbidden secret"):
        validate_command({**command, "payload": {**command["payload"], "token": "x"}})


def test_preview_bridge_enforces_the_exact_protocol_payload() -> None:
    runtime = ExecutionRuntime(root=ROOT)
    valid = {
        "execution_symbol": "MES",
        "side": "BUY",
        "order_type": "LIMIT",
        "price": 5000.0,
        "stop_price": 4995.0,
        "target_price": 5010.0,
        "quantity": 1,
    }

    assert runtime.preview_order(valid)["blockers"] != ["PREVIEW_PAYLOAD_INVALID"]
    assert runtime.preview_order({**valid, "accessToken": "x"}) == {
        "ok": False,
        "authoritative_maximum": 0,
        "blockers": ["PREVIEW_PAYLOAD_INVALID"],
    }


def test_backend_credential_reference_supports_create_read_and_revoke_without_logging() -> None:
    store = MemoryCredentialStore()
    reference = CredentialReference("WINDOWS_CREDENTIAL_MANAGER", "futures_intraday_model_v2/tradovate")
    secret = json.dumps({"name": "n", "password": "p", "app_id": "a", "app_version": "1", "cid": "c", "secret": "s", "device_id": "d"})
    store.write(reference.target, secret)
    material = TradovateAuthMaterial.from_reference(store=store, reference=reference)
    assert material.request_body()["sec"] == "s"
    assert store.exists(reference.target) is True
    assert store.delete(reference.target) is True
    assert store.exists(reference.target) is False
    assert redact("Authorization: Bearer sensitive") == "credential operation failed (redacted)"


def test_service_roots_cannot_mix_demo_and_live() -> None:
    demo = ServiceRoots("demo", "https://demo.tradovateapi.com/v1", "wss://demo.tradovateapi.com/v1/websocket")
    assert demo.environment == "demo"
    with pytest.raises(ValueError, match="exact matching environment"):
        ServiceRoots("demo", "https://demo.tradovateapi.com/v1", "wss://live.tradovateapi.com/v1/websocket")


def rest_client(responses: list[TransportResponse | BaseException], *, read: bool = True, change: bool = False) -> tuple[TradovateRestClient, FakeHttpTransport]:
    transport = FakeHttpTransport(responses)
    client = TradovateRestClient(
        roots=ServiceRoots("demo", "https://demo.tradovateapi.com/v1", "wss://demo.tradovateapi.com/v1/websocket"),
        transport=transport,
        authority=OperationAuthority(read, change, True, None),
    )
    return client, transport


def test_rest_reads_retry_safely_but_order_submission_never_retries() -> None:
    client, transport = rest_client([TransportResponse(500, {}), TransportResponse(200, [{"id": 101}])])
    assert client.accounts(token()) == [{"id": 101}]
    assert len(transport.requests) == 2
    client, transport = rest_client([TransportResponse(500, {}), TransportResponse(200, {"orderId": 1})], change=True)
    with pytest.raises(TransportError, match="server error"):
        client.place_order(token(), {"accountId": 101})
    assert len(transport.requests) == 1


def test_lost_order_response_is_unknown_and_not_retried() -> None:
    client, transport = rest_client([TimeoutError("lost response"), TransportResponse(200, {"orderId": 1})], change=True)
    with pytest.raises(UnknownBrokerState, match="reconcile without retry"):
        client.place_order(token(), {"accountId": 101})
    assert len(transport.requests) == 1


def test_http_permission_and_business_errors_fail_closed() -> None:
    for response in (
        TransportResponse(401, {}),
        TransportResponse(403, {}),
        TransportResponse(200, {"errorText": "denied"}),
        TransportResponse(200, {"failureText": "rejected"}),
    ):
        client, _ = rest_client([response], change=True)
        with pytest.raises(ExecutionBlocked):
            client.place_order(token(), {})


def test_token_lifecycle_is_memory_only_and_renewal_aware() -> None:
    manager = TokenManager()
    manager.set(token())
    assert manager.current(now=NOW).renewal_due(now=NOW + timedelta(minutes=76)) is True
    manager.clear()
    with pytest.raises(ExecutionBlocked, match="absent or expired"):
        manager.current(now=NOW)


def test_websocket_authorizes_and_scopes_user_sync_to_exact_account() -> None:
    socket = FakeWebSocketTransport(['a["{\\"s\\":200,\\"i\\":1}"]'])
    sync = TradovateUserSync(
        roots=ServiceRoots("demo", "https://demo.tradovateapi.com/v1", "wss://demo.tradovateapi.com/v1/websocket"),
        transport=socket,
    )
    sync.open(token=token(), user_id=202, account_id=101)
    assert socket.opened_url == "wss://demo.tradovateapi.com/v1/websocket"
    assert socket.sent[0] == request_frame("authorize", 0, "test-token")
    assert '"accounts":[101]' in socket.sent[1]
    assert parse_server_frame("h") == []
    assert sync.receive() == [{"s": 200, "i": 1}]
    sync.close()


def test_tradovate_adapter_preserves_gtc_and_blocks_manual_automation_ambiguity() -> None:
    client, _ = rest_client([], change=True)
    sync = TradovateUserSync(
        roots=ServiceRoots("demo", "https://demo.tradovateapi.com/v1", "wss://demo.tradovateapi.com/v1/websocket"),
        transport=FakeWebSocketTransport(),
    )
    adapter = TradovateAdapter(
        binding=binding(), rest=client, user_sync=sync, token_manager=TokenManager(), contract_symbols={301: "MESM6"}
    )
    assert adapter._entry_body(intent())["timeInForce"] == "GTC"
    with pytest.raises(ExecutionBlocked, match="isAutomated treatment is unconfirmed"):
        adapter._entry_body(replace(intent(), source=IntentSource.MANUAL))


def raw_entities() -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    accounts = [
        {"id": 999, "name": "WRONG", "userId": 202, "active": True, "readonly": False},
        {"id": 101, "name": "MFF-SIM-101", "userId": 202, "active": True, "readonly": False, "cashBalance": 100.0, "equity": 95.0},
    ]
    positions = [{"id": 401, "accountId": 101, "contractId": 301, "netPos": -1, "netPrice": 5000.0}]
    orders = [{"id": 501, "accountId": 101, "contractId": 301, "clOrdId": "intent-1", "action": "Sell", "orderType": "Stop", "orderQty": 1, "filledQty": 0, "ordStatus": "Working", "stopPrice": 5005.0}]
    fills = [{"id": 601, "orderId": 501, "contractId": 301, "action": "Sell", "qty": 1, "price": 5000.0}]
    return accounts, positions, orders, fills


def test_reconciliation_never_selects_first_account_and_accepts_signed_positions() -> None:
    accounts, positions, orders, fills = raw_entities()
    snapshot = broker_snapshot_from_entities(
        binding=binding(), accounts=accounts, positions=positions, orders=orders, fills=fills,
        contract_symbols={301: "MESM6"}, observed_at=NOW, sequence=1,
    )
    assert snapshot.account is not None and snapshot.account.account_id == 101
    assert snapshot.positions[0].net_quantity == -1
    assert snapshot.orders[0].origin is EventOrigin.PROVIDER_BACKED


def test_wrong_account_duplicate_and_out_of_order_state_fail_closed() -> None:
    accounts, positions, orders, fills = raw_entities()
    with pytest.raises(UnknownBrokerState, match="exact bound"):
        broker_snapshot_from_entities(binding=binding(), accounts=accounts[:1], positions=[], orders=[], fills=[], contract_symbols={}, observed_at=NOW)
    with pytest.raises(UnknownBrokerState, match="duplicate provider order"):
        broker_snapshot_from_entities(binding=binding(), accounts=accounts, positions=[], orders=orders * 2, fills=[], contract_symbols={301: "MESM6"}, observed_at=NOW)
    current = broker_snapshot_from_entities(binding=binding(), accounts=accounts, positions=positions, orders=orders, fills=fills, contract_symbols={301: "MESM6"}, observed_at=NOW, sequence=2)
    older = broker_snapshot_from_entities(binding=binding(), accounts=accounts, positions=positions, orders=orders, fills=fills, contract_symbols={301: "MESM6"}, observed_at=NOW - timedelta(seconds=1), sequence=1)
    reconciler = Reconciler(binding=binding())
    reconciler.apply(current, known_provider_order_ids={501})
    assert reconciler.apply(older).status == "IGNORED_OUT_OF_ORDER"


def test_order_ledger_is_hash_chained_idempotent_and_secret_free(tmp_path: Path) -> None:
    ledger = OrderLedger(tmp_path / "execution.jsonl")
    first = ledger.bind_intent(intent_id="intent-1", provider_order_id=501)
    assert first["previous_hash"] == "0" * 64
    assert ledger.intent_provider_order("intent-1") == 501
    with pytest.raises(UnknownBrokerState, match="already"):
        ledger.bind_intent(intent_id="intent-1", provider_order_id=502)
    with pytest.raises(ValueError, match="forbidden secret"):
        ledger.append(event_type="BAD", payload={"token": "x"})
    for field in ("accessToken", "apiKey", "clientSecret"):
        with pytest.raises(ValueError, match="forbidden secret"):
            ledger.append(event_type="BAD", payload={field: "x"})
    ledger.path.write_text(ledger.path.read_text(encoding="utf-8") + "{", encoding="utf-8")
    with pytest.raises(UnknownBrokerState, match="incomplete"):
        ledger.read()


def test_local_simulator_is_idempotent_and_updates_native_brackets_on_partial_fill() -> None:
    simulator = LocalExecutionSimulator(account_id=101, account_spec="LOCAL-SIM", user_id=202)
    order = simulator.submit(intent())
    assert simulator.submit(intent()).provider_order_id == order.provider_order_id
    simulator.fill(provider_order_id=order.provider_order_id, quantity=1, price=5000.0)
    partial = simulator.reconcile()
    entry = next(value for value in partial.orders if value.provider_order_id == order.provider_order_id)
    children = [value for value in partial.orders if value.parent_order_id == order.provider_order_id]
    assert entry.status is OrderStatus.PARTIALLY_FILLED
    assert len(children) == 2 and {value.quantity for value in children} == {1}
    assert len({value.oco_id for value in children}) == 1
    simulator.fill(provider_order_id=order.provider_order_id, quantity=1, price=5000.25)
    full = simulator.reconcile()
    assert next(value for value in full.orders if value.provider_order_id == order.provider_order_id).status is OrderStatus.FILLED
    assert {value.quantity for value in full.orders if value.parent_order_id == order.provider_order_id} == {2}
    assert simulator.flatten_position(301).net_quantity == 0
    assert all(value.status is OrderStatus.CANCELED for value in simulator.reconcile().orders if value.parent_order_id == order.provider_order_id)


def test_nested_broker_snapshot_public_payload_is_json_safe() -> None:
    accounts, positions, orders, fills = raw_entities()
    snapshot = broker_snapshot_from_entities(
        binding=binding(), accounts=accounts, positions=positions, orders=orders,
        fills=fills, contract_symbols={301: "MESM6"}, observed_at=NOW, sequence=1,
    )

    payload = public_payload(snapshot)
    json.dumps(payload)
    assert payload["account"]["observed_at"] == NOW.isoformat()
    assert payload["orders"][0]["status"] == "WORKING"


def test_local_simulator_rejection_creates_no_position_and_disconnect_is_visible() -> None:
    simulator = LocalExecutionSimulator(account_id=101, account_spec="LOCAL-SIM", user_id=202)
    order = simulator.submit(intent(intent_id="reject-me", quantity=1))
    assert simulator.reject(provider_order_id=order.provider_order_id).status is OrderStatus.REJECTED
    assert simulator.reconcile().positions == ()
    simulator.close()
    assert simulator.connected is False


def test_existing_mff_runtime_sizes_only_verified_micro_mappings() -> None:
    portfolio = PortfolioRiskState((), (), Decimal("0"), Decimal("0"), Decimal("-2000"))
    for signal, execution in (("ES", "MES"), ("CL", "MCL"), ("6E", "M6E")):
        result = local_simulator_sizing(root=ROOT, signal_root=signal, execution_symbol=execution, stop_ticks=10, portfolio_state=portfolio)
        assert 0 < result.quantity <= 30
        assert result.projected_micro_equivalent <= 30
        assert result.production_readiness is False
    with pytest.raises(ContractError):
        local_simulator_sizing(root=ROOT, signal_root="ZN", execution_symbol="ZN", stop_ticks=10, portfolio_state=portfolio)


def test_provider_gate_requires_exact_micro_and_hash_bound_identity() -> None:
    portfolio = PortfolioRiskState((), (), Decimal("0"), Decimal("0"), Decimal("-2000"))
    runtime_identity = {
        "profile_id": "mff_rapid_eod_50k_2026_08_10",
        "profile_hash": HASH,
        "account_stage": "sim_funded",
        "strategy_policy_id": "coarse-3",
        "strategy_policy_hash": HASH,
        "execution_instrument_mapping_id": "mff-micro-map",
        "execution_instrument_mapping_hash": HASH,
        "execution_cost_profile_id": "mff-costs",
        "execution_cost_profile_hash": HASH,
    }
    context = ProviderGateContext(
        root=ROOT, binding=binding(),
        arm_state=ArmState(mode=ExecutionMode.MFF_TRADOVATE_SIM_FUNDED), now=NOW,
        entitlement_confirmed=False, endpoint_confirmed=False,
        account_synchronized=False, reconciliation_status="NOT_RECONCILED",
        external_state_status="UNKNOWN", production_readiness=False,
        exact_costs_verified=False, configured_stage="sim_funded",
        observed_stage="sim_funded", kill_switch_engaged=False,
        news_events=(), news_status="UNKNOWN", restricted_news_categories=set(),
        current_price=5000.0, reference_price=5000.0,
        lower_price_limit=None, upper_price_limit=None, price_limit_status="UNKNOWN",
        existing_exposures=(), recent_order_timestamps=(), working_orders=(),
        portfolio_state=portfolio, runtime_identity=runtime_identity,
        strategy_candidate_id="coarse-3", stop_ticks=10,
    )

    wrong_mapping = evaluate_provider_intent(
        replace(intent(), execution_symbol="MCL"), context
    )
    assert "EXECUTION_INSTRUMENT_NOT_VERIFIED_MICRO" in wrong_mapping.blockers
    wrong_profile = evaluate_provider_intent(
        replace(intent(), profile_id="another-profile"), context
    )
    assert "PROFILE_BINDING_MISMATCH" in wrong_profile.blockers
    wrong_runtime = evaluate_provider_intent(
        intent(), replace(context, runtime_identity={**runtime_identity, "profile_hash": "b" * 64})
    )
    assert "RUNTIME_IDENTITY_BINDING_MISMATCH" in wrong_runtime.blockers


def test_ui_is_truthfully_disabled_and_has_no_order_submission_bridge() -> None:
    html = (ROOT / "src/futures_rebuild/live_cockpit/assets/index.html").read_text(encoding="utf-8")
    script = (ROOT / "src/futures_rebuild/live_cockpit/assets/app.js").read_text(encoding="utf-8")
    app = (ROOT / "src/futures_rebuild/live_cockpit/app.py").read_text(encoding="utf-8")
    assert "MFF SIM FUNDED" in html
    assert 'class="order-ticket" disabled' in html
    assert html.count("Flatten all") == 1
    assert "execution_blocker_list" not in app
    assert "submit_order" not in app
    assert "accessToken" not in script and "api_key" not in script.lower()
    assert "LOCAL SIMULATOR - SYNTHETIC ONLY" in script
    assert '"minmax(0, 1fr) 304px"' in script


def test_prepare_only_operations_are_bounded_and_never_authorize_provider_use(
    tmp_path: Path,
) -> None:
    executable = tmp_path / "FuturesLiveCockpit.exe"
    executable.write_bytes(b"offline synthetic cockpit fixture")
    read_only = prepare_scope(root=ROOT, executable=executable, operation=READ_ONLY_OPERATION)
    assert read_only["provider_connection_authorized"] is False
    assert read_only["execution_authorized"] is False
    assert "place" in read_only["forbidden_operations"]
    sim = prepare_scope(root=ROOT, executable=executable, operation=SIM_ORDER_OPERATION)
    assert sim["maximum_micro_quantity"] == 1
    assert sim["native_protective_stop_required"] is True
    assert sim["classification"] == "BLOCKED_PREPARE_ONLY"
    live = prepare_scope(root=ROOT, executable=executable, operation=LIVE_ORDER_OPERATION)
    assert live["classification"] == "BLOCKED_NOT_EXECUTABLE"
    assert live["execution_authorized"] is False


def test_package_spec_includes_execution_configs_but_no_binding_or_secret() -> None:
    spec = (ROOT / "FuturesLiveCockpit/_internal/FuturesLiveCockpit.spec").read_text(encoding="utf-8")
    package = (ROOT / "src/futures_rebuild/live_cockpit/package_candidate.py").read_text(encoding="utf-8")
    assert "prop_firm_execution_connections.json" in spec
    assert "execution/tradovate_adapter.py" in package
    assert "execution_binding.json" not in spec
    assert "api.env" not in spec


def test_single_instance_lock_blocks_a_second_execution_engine(tmp_path: Path) -> None:
    first = SingleInstance(tmp_path / "cockpit.lock")
    second = SingleInstance(tmp_path / "cockpit.lock")
    assert first.acquire() is True
    try:
        assert second.acquire() is False
    finally:
        first.release()
    assert second.acquire() is True
    second.release()
