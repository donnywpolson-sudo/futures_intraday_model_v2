"""Generated-data checks only; no historical observations or provider access."""
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import socket

import pytest

from futures_rebuild.historical_simulation import (
    Settings, demo_bars, no_trade, save_demo, simulate,
)
from futures_rebuild.simulation_records import encoded, load_bundle, validate_bundle
from futures_rebuild.live_cockpit.replay import ReplayCockpitEngine


def bars(prices=(100, 102, 103, 99)):
    return [replace(b, open=str(p), high=str(p + 1), low=str(p - 1), close=str(p))
            for b, p in zip(demo_bars(), prices)]


def run(sequence, decisions, settings=Settings()):
    return simulate(sequence, lambda c: decisions[len(c.bars) - 1],
                    strategy="test", settings=settings, synthetic=True)


def test_hand_calculated_long_reversal_exit_and_next_open():
    result = run(bars(), [1, -1, 0, None])
    a = result["summary"]["account"]
    # Buy 102.25; sell two at 102.75 (close long +$25, open short);
    # buy 99.25 (close short +$175). Four sides cost $8, slippage $50.
    assert Decimal(a["realized_pnl"]) == 200
    assert Decimal(a["commissions"]) == 8
    assert Decimal(a["slippage_cost"]) == 50
    assert Decimal(a["total_costs"]) == 58
    assert Decimal(a["equity"]) == 10192
    assert a["position"] == 0
    assert not result["records"][0]["fills"]
    assert result["records"][2]["fills"][0]["quantity"] == 2
    assert Decimal(result["records"][1]["account"]["drawdown"]) == Decimal("14.50")


def test_short_open_position_and_pending_cancel_not_fictional_exit():
    result = run(bars((100, 102, 103)), [-1, None, 0])
    summary = result["summary"]
    assert summary["open_position"] and summary["pending_decision_cancelled"]
    assert summary["account"]["position"] == -1
    assert Decimal(summary["account"]["unrealized_pnl"]) == Decimal("-62.50")
    assert Decimal(summary["account"]["equity"]) == Decimal("9935.50")
    assert sum(len(row["fills"]) for row in result["records"]) == 1


def test_flat_and_hold_do_not_create_extra_fills():
    empty = simulate(bars(), no_trade, strategy="no-trade", synthetic=True)
    assert Decimal(empty["summary"]["account"]["equity"]) == 10000
    assert all(not r["fills"] for r in empty["records"])
    held = run(bars(), [1, 1, None, 1])
    assert sum(len(r["fills"]) for r in held["records"]) == 1
    assert not held["summary"]["pending_decision_cancelled"]


def test_strategy_sees_immutable_prefix_and_current_mark_only():
    seen = []
    def decide(context):
        seen.append(context)
        assert len(context.bars) == len(seen)
        assert context.bars[-1] == bars()[len(seen) - 1]
        with pytest.raises(AttributeError):
            context.account.position = 1
        return None
    simulate(iter(bars()), decide, strategy="prefix", synthetic=True)
    assert [len(c.bars) for c in seen] == [1, 2, 3, 4]


@pytest.mark.parametrize("change", [
    {"open": "NaN"}, {"close": "Infinity"}, {"high": "0"}, {"open": "100.01"},
    {"volume": -1}, {"volume": True}, {"contract": "OTHER"},
    {"opened_at": "2020-01-06T14:31:00"},
])
def test_invalid_bars_fail_before_strategy(change):
    sequence = bars()
    sequence[1] = replace(sequence[1], **change)
    calls = []
    with pytest.raises((ValueError, TypeError)):
        simulate(sequence, lambda c: calls.append(c), strategy="bad", synthetic=True)
    assert len(calls) == 1


@pytest.mark.parametrize("sequence", [[], [bars()[0], bars()[0]], [bars()[0], bars()[2]]])
def test_empty_duplicate_and_gap_rejected(sequence):
    with pytest.raises(ValueError):
        simulate(sequence, no_trade, strategy="bad", synthetic=True)


@pytest.mark.parametrize("decision", [True, 2, "1", float("nan")])
def test_invalid_target_rejected(decision):
    with pytest.raises(ValueError):
        run(bars(), [decision] * 4)


def test_strategy_error_propagates_and_real_history_is_disabled():
    def broken(context):
        raise RuntimeError("fixture failure")
    with pytest.raises(RuntimeError, match="fixture failure"):
        simulate(bars(), broken, strategy="broken", synthetic=True)
    with pytest.raises(ValueError, match="not enabled"):
        simulate(bars(), no_trade, strategy="real", synthetic=False)


def test_roundtrip_determinism_and_no_overwrite(tmp_path):
    first = save_demo(tmp_path / "a")
    second = save_demo(tmp_path / "b")
    assert encoded(first) == encoded(second)
    assert load_bundle(tmp_path / "a") == first
    with pytest.raises(FileExistsError):
        save_demo(tmp_path / "a")


def test_demo_matches_independently_calculated_fills_and_equity(tmp_path):
    bundle = save_demo(tmp_path / "demo")
    fills = [fill for row in bundle["records"] for fill in row["fills"]]
    assert [(f["side"], f["quantity"], Decimal(f["price"])) for f in fills] == [
        ("BUY", 1, Decimal("4999.50")), ("SELL", 2, Decimal("4998.50")),
        ("BUY", 1, Decimal("4998.75")), ("SELL", 1, Decimal("4999.00")),
        ("BUY", 2, Decimal("4999.50")), ("SELL", 1, Decimal("4998.50")),
    ]
    # Four completed legs: -$50, -$12.50, -$25, -$50; eight sides at $2.
    account = bundle["summary"]["account"]
    assert Decimal(account["realized_pnl"]) == Decimal("-137.50")
    assert Decimal(account["commissions"]) == 16
    assert Decimal(account["slippage_cost"]) == 100
    assert Decimal(account["total_costs"]) == 116
    assert Decimal(account["equity"]) == Decimal("9846.50")
    assert account["position"] == 0


def test_interrupted_output_not_complete(tmp_path, monkeypatch):
    from futures_rebuild import historical_simulation as module
    def fail(*args, **kwargs):
        raise RuntimeError("interrupted")
    monkeypatch.setattr(module, "simulate", fail)
    with pytest.raises(RuntimeError):
        save_demo(tmp_path / "failed")
    with pytest.raises(FileNotFoundError):
        load_bundle(tmp_path / "failed")


@pytest.mark.parametrize("fault", ["version", "partial", "future-fill", "hash", "summary", "extra", "oversize"])
def test_bad_bundles_rejected(tmp_path, fault):
    directory = tmp_path / "bad"
    bundle = save_demo(directory)
    if fault == "version": bundle["schema"] = "offline-simulation/99"
    elif fault == "partial": bundle["status"] = "RUNNING"
    elif fault == "future-fill": bundle["records"][1]["fills"][0]["decision_index"] = 1
    elif fault == "hash": bundle["metadata"]["input_sha256"] = "bad"
    elif fault == "summary": bundle["summary"]["account"] = {}
    elif fault == "extra": bundle["execute"] = "anything"
    raw = encoded(bundle) if fault != "oversize" else b" " * 8_000_001
    (directory / "result.json").write_bytes(raw)
    with pytest.raises(ValueError):
        ReplayCockpitEngine(directory)


def test_replay_cursor_pause_restart_speed_and_hidden_future(tmp_path):
    directory = tmp_path / "run"
    expected = save_demo(directory)
    for speed in (1, 10, 100):
        viewer = ReplayCockpitEngine(directory)
        assert viewer.snapshot()["payload"]["records"] == []
        viewer.command("step")
        prefix = viewer.snapshot()["payload"]
        assert len(prefix["records"]) == 1 and prefix["summary"] is None
        viewer.command("speed", speed)
        viewer.command("play")
        viewer.advance(1 / speed)
        assert viewer.cursor == 2
        viewer.command("pause")
        viewer.advance(999)
        assert viewer.cursor == 2
        viewer.command("play")
        viewer.advance(999)
        final = viewer.snapshot()["payload"]
        assert final["summary"] == expected["summary"] and not final["playing"]
        assert final["records"] == expected["records"]
        viewer.command("restart")
        assert viewer.snapshot()["payload"]["records"] == []
        assert viewer.snapshot()["payload"]["summary"] is None
        viewer.stop()


def test_network_and_provider_free_runner_and_entrypoint(tmp_path, monkeypatch):
    from futures_rebuild.live_cockpit import app, credentials
    def forbidden(*args, **kwargs):
        raise AssertionError("provider/credential/network access")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(app, "LiveCockpitEngine", forbidden)
    monkeypatch.setattr(credentials, "default_credential_locator_path", forbidden)
    directory = tmp_path / "run"
    save_demo(directory)
    observed = {}
    def desktop(**kwargs):
        observed.update(kwargs)
        assert kwargs["engine"].bootstrap_event()["payload"]["max_provider_sessions"] == 0
        return 0
    monkeypatch.setattr(app, "run_desktop", desktop)
    assert app.main(["--replay-run", str(directory)]) == 0
    assert observed["replay"] is True and observed["state_path"].parent.name == "replay"
    with pytest.raises(SystemExit):
        app.main(["--demo", "--replay-run", str(directory)])
    source = Path(__file__).parents[1] / "src/futures_rebuild/live_cockpit/replay.py"
    assert "historical_simulation" not in source.read_text()


def test_controller_coalesces_prefixes_and_live_rejects_commands(tmp_path):
    from futures_rebuild.live_cockpit.app import CockpitApi, CockpitController
    directory = tmp_path / "run"
    save_demo(directory)
    engine = ReplayCockpitEngine(directory)
    controller = CockpitController(engine, state_path=tmp_path / "preferences.json")
    controller.publish(engine.snapshot())
    engine.command("step")
    controller.publish(engine.snapshot())
    messages = controller.poll_events()
    assert len(messages) == 1 and messages[0]["payload"]["cursor"] == 1
    assert CockpitApi(controller).replay_command("restart") == {"ok": True}
    controller.engine = object()
    with pytest.raises(ValueError):
        CockpitApi(controller).replay_command("play")


def test_replay_protocol_rejects_malformed_metadata_and_early_summary(tmp_path):
    from futures_rebuild.live_cockpit.protocol import validate_event
    directory = tmp_path / "run"
    save_demo(directory)
    engine = ReplayCockpitEngine(directory)
    message = engine.snapshot()
    message["payload"]["metadata"]["synthetic"] = False
    with pytest.raises(ValueError):
        validate_event(message)
    message = engine.snapshot()
    message["payload"]["summary"] = {}
    with pytest.raises(ValueError):
        validate_event(message)
