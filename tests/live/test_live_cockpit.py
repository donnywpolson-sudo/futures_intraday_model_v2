from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import futures_rebuild.live_cockpit.app as cockpit_app
import futures_rebuild.live_cockpit.cache as cockpit_cache
import futures_rebuild.live_cockpit.engine as cockpit_engine
from futures_rebuild.live_cockpit.app import (
    CockpitApi,
    CockpitController,
    assets_dir,
    desktop_asset_target,
    load_state,
    self_check,
)
from futures_rebuild.live_cockpit.cache import BarCache
from futures_rebuild.live_cockpit.engine import (
    DEFAULT_VISUAL_UPDATE_MODE,
    HISTORY_REQUEST_TIMEOUT_SECONDS,
    MIN_RENDER_HZ,
    SYMBOL_RESOLUTION_ATTEMPTS,
    SYMBOL_REQUEST_TIMEOUT_SECONDS,
    VISUAL_UPDATE_HZ,
    DemoCockpitEngine,
    LiveCockpitEngine,
    MAX_RENDER_HZ,
    VisualUpdateState,
)
from futures_rebuild.live_cockpit.history import (
    HistoryBinding,
    group_history_chunks,
    missing_intervals,
    promote_market,
)
from futures_rebuild.live_cockpit.market_groups import load_alpha_tier_grouping
from futures_rebuild.live_cockpit.predictions import NullPredictionSource
from futures_rebuild.live_cockpit.protocol import (
    direction_entropy,
    event,
    serialize_bar,
    validate_data_health_payload,
    validate_event,
    validate_history_cache_payload,
    validate_prediction_payload,
)
from futures_rebuild.live_cockpit.feed import (
    TradeCandleAggregator,
    chart_market_universe,
    trading_day_start,
)


def _bar(timestamp: datetime, close: float = 100.0) -> dict[str, object]:
    return {
        "time": timestamp,
        "open": close - 0.25,
        "high": close + 0.5,
        "low": close - 0.5,
        "close": close,
        "volume": 25,
    }


def _wait_until(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition was not met before timeout")


def test_alpha_tier_grouping_is_an_exact_unique_4_12_25_partition() -> None:
    markets = chart_market_universe()
    assert len(markets) == 41
    assert {"6N", "6S", "ZQ", "GF", "BTC", "ETH", "PA", "PL"} <= {
        market.symbol for market in markets
    }
    grouping = load_alpha_tier_grouping(
        Path("configs/alpha_tiered.yaml"), [market.symbol for market in markets]
    )
    assert grouping.available is True
    assert [group["market_count"] for group in grouping.groups] == [4, 12, 25]
    assert len(grouping.market_groups) == 41
    assert grouping.market_groups["ES"] == "tier_1_core"
    assert grouping.market_groups["NQ"] == "tier_2_additions"
    assert grouping.market_groups["RTY"] == "tier_3_additions"


def test_alpha_tier_grouping_falls_back_without_exposing_config_errors(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "alpha_tiered.yaml"
    config_path.write_text(
        "profiles:\n"
        "  tier_1_research: {markets: [ES, NQ]}\n"
        "  tier_2_research: {markets: [ES]}\n"
        "  tier_3_research: {markets: [ES, NQ]}\n",
        encoding="utf-8",
    )
    grouping = load_alpha_tier_grouping(config_path, ["ES", "NQ"])
    assert grouping.available is False
    assert grouping.market_groups == {}
    assert grouping.capability_payload() == {
        "alpha_tiers_available": False,
        "alpha_tier_groups": [],
    }


def test_visual_update_state_validates_modes_and_applies_adaptive_floor() -> None:
    updates = VisualUpdateState()
    assert updates.mode == DEFAULT_VISUAL_UPDATE_MODE
    assert updates.effective_hz() == 10.0
    assert updates.set_mode("high") is True
    assert updates.effective_hz() == MAX_RENDER_HZ
    assert updates.set_active(False) == MIN_RENDER_HZ
    assert updates.set_mode("unbounded") is False
    assert updates.mode == "high"
    assert updates.set_active(True) == VISUAL_UPDATE_HZ["high"]


def test_protocol_is_versioned_and_serializes_bars() -> None:
    message = event("bar_update", {"bar": serialize_bar(_bar(datetime(2026, 7, 13, tzinfo=timezone.utc)))})
    validate_event(message)
    assert message["v"] == 3
    assert message["payload"]["bar"]["time"] == 1_783_900_800


def test_history_cache_contract_is_bounded_and_rejects_secrets() -> None:
    payload = {
        "state": "CONFIRMATION_REQUIRED",
        "ready_markets": 4,
        "total_markets": 41,
        "queued_markets": 37,
        "active_market": None,
        "estimated_cost_usd": 0.0123,
        "estimate_expires_at": 2_000,
        "plan_id": "bounded-plan-id",
        "paused": False,
        "message": "Confirm the exact missing-data plan.",
    }
    validate_history_cache_payload(payload)
    validate_event(event("history_cache_status", payload))
    error_payload = {
        **payload,
        "state": "ERROR",
        "plan_id": None,
        "estimated_cost_usd": None,
        "estimate_expires_at": None,
        "failure_category": "UNAVAILABLE",
        "diagnostic": {
            "phase": "COST_ESTIMATE",
            "chunk_number": None,
            "requested_start": 1_000,
            "requested_end": 2_000,
            "download_began": False,
        },
    }
    validate_history_cache_payload(error_payload)
    validate_event(event("history_cache_status", error_payload))
    unsafe = {
        **error_payload,
        "failure_category": "https://provider.invalid/?key=SECRET",
    }
    with pytest.raises(ValueError, match="unsupported history-cache failure"):
        validate_history_cache_payload(unsafe)
    with pytest.raises(ValueError, match="fields are not exact"):
        validate_history_cache_payload(
            {
                **error_payload,
                "diagnostic": {
                    **error_payload["diagnostic"],
                    "provider_text": "SECRET",
                },
            }
        )
    with pytest.raises(ValueError, match="one-based"):
        validate_history_cache_payload(
            {
                **error_payload,
                "diagnostic": {
                    **error_payload["diagnostic"],
                    "chunk_number": 0,
                },
            }
        )


def _ready_prediction_payload() -> dict[str, object]:
    probabilities = {"long": 0.64, "flat": 0.21, "short": 0.15}
    return {
        "market": "ES",
        "contract": "ESM6",
        "instrument_id": 123,
        "timeframe": "1m",
        "generation": 2,
        "prediction_id": "synthetic:ES:1m:1900:ready",
        "prediction_time": 2_000,
        "input_bar_time": 1_900,
        "state": "READY",
        "source": "SYNTHETIC_DEMO",
        "synthetic": True,
        "observation_only": True,
        "model": {
            "id": "synthetic-shadow-demo",
            "version": "1",
            "strategy": "direction-probability-ui",
        },
        "forecast": {
            "direction": "LONG",
            "horizon_seconds": 900,
            "probabilities": probabilities,
            "expected_return": 0.00045,
            "direction_entropy": direction_entropy(probabilities),
        },
        "reason_codes": [],
    }


def test_prediction_contract_is_bounded_completed_bar_only_and_redacts_reasons() -> None:
    payload = _ready_prediction_payload()
    validate_prediction_payload(payload)
    validate_event(event("prediction_update", payload))

    invalid_sum = json.loads(json.dumps(payload))
    invalid_sum["forecast"]["probabilities"]["long"] = 0.65
    with pytest.raises(ValueError, match="sum to 1"):
        validate_prediction_payload(invalid_sum)

    invalid_bounds = json.loads(json.dumps(payload))
    invalid_bounds["forecast"]["probabilities"] = {
        "long": 1.1,
        "flat": -0.1,
        "short": 0.0,
    }
    with pytest.raises(ValueError, match=r"within \[0, 1\]"):
        validate_prediction_payload(invalid_bounds)

    active_input = json.loads(json.dumps(payload))
    active_input["input_bar_time"] = 1_960
    with pytest.raises(ValueError, match="input bar must be completed"):
        validate_prediction_payload(active_input)

    non_ready_forecast = json.loads(json.dumps(payload))
    non_ready_forecast["state"] = "ABSTAIN"
    non_ready_forecast["reason_codes"] = ["DATA_INCOMPLETE"]
    with pytest.raises(ValueError, match="cannot carry a forecast"):
        validate_prediction_payload(non_ready_forecast)

    unsafe_reason = json.loads(json.dumps(payload))
    unsafe_reason["state"] = "ERROR"
    unsafe_reason["forecast"] = None
    unsafe_reason["reason_codes"] = ["https://provider.invalid/?key=SYNTHETIC_SECRET"]
    with pytest.raises(ValueError, match="unsupported reason code"):
        validate_prediction_payload(unsafe_reason)


def test_data_health_contract_does_not_invent_unevaluated_gap_counts() -> None:
    payload = {
        "market": "ES",
        "contract": "ESM6",
        "instrument_id": 123,
        "timeframe": "1m",
        "generation": 2,
        "evaluated_at": 2_000,
        "last_bar_time": 1_940,
        "state": "CURRENT",
        "history": {
            "state": "COMPLETE",
            "requested_hours": 168.0,
            "coverage_hours": 167.0,
            "bar_count": 5_000,
        },
        "continuity": {
            "state": "NOT_EVALUATED",
            "unexpected_gap_count": None,
            "largest_gap_seconds": None,
        },
        "reason_codes": ["CONTINUITY_NOT_EVALUATED"],
    }
    validate_data_health_payload(payload)
    payload["continuity"]["unexpected_gap_count"] = 0
    with pytest.raises(ValueError, match="cannot claim numeric gaps"):
        validate_data_health_payload(payload)


def test_trade_and_provider_ohlcv_aggregation_are_equivalent() -> None:
    minute = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)
    aggregator = TradeCandleAggregator(timeframe_seconds=60, timeframe="1m")
    candle = None
    for seconds, price, size in (
        (1, 100.0, 1),
        (10, 101.0, 2),
        (20, 99.0, 1),
        (40, 100.5, 2),
    ):
        candle = aggregator.apply_trade(
            SimpleNamespace(
                ts_event=minute + timedelta(seconds=seconds),
                price=int(price * 1_000_000_000),
                size=size,
            )
        )
    provider = cockpit_engine.ohlcv_record_to_candle(
        SimpleNamespace(
            ts_event=minute,
            open=100_000_000_000,
            high=101_000_000_000,
            low=99_000_000_000,
            close=100_500_000_000,
            volume=6,
        )
    )
    assert candle is not None
    assert {key: candle[key] for key in ("open", "high", "low", "close", "volume")} == {
        key: provider[key] for key in ("open", "high", "low", "close", "volume")
    }


def test_session_boundaries_use_readable_names() -> None:
    bars = [
        _bar(datetime(2026, 7, 13, 21, 0, tzinfo=timezone.utc)),
        _bar(datetime(2026, 7, 14, 14, 0, tzinfo=timezone.utc)),
    ]
    markers = cockpit_engine._session_markers(bars, "1m")
    assert {marker["text"] for marker in markers} == {"Globex", "RTH"}


def test_recent_gap_replay_start_is_bounded_and_honors_watermark() -> None:
    now = datetime(2026, 7, 14, 12, 0, 45, tzinfo=timezone.utc)
    earliest = datetime(2026, 7, 13, 12, 2, tzinfo=timezone.utc)
    bars = [
        _bar(earliest),
        _bar(earliest + timedelta(minutes=1)),
        _bar(earliest + timedelta(minutes=3)),
    ]
    assert cockpit_engine._recent_gap_replay_start(bars, now=now) == (
        earliest + timedelta(minutes=2)
    )
    assert cockpit_engine._recent_gap_replay_start(
        bars,
        now=now,
        lower_bound=now.replace(second=0, microsecond=0),
    ) is None


def test_replay_bar_upsert_replaces_live_bar_and_preserves_order() -> None:
    minute = datetime(2026, 7, 14, 11, 55, tzinfo=timezone.utc)
    bars = [_bar(minute, 100.0), _bar(minute + timedelta(minutes=2), 102.0)]
    assert cockpit_engine._upsert_sorted_bar(
        bars, _bar(minute + timedelta(minutes=1), 101.0)
    ) is True
    assert cockpit_engine._upsert_sorted_bar(bars, _bar(minute, 99.0)) is False
    assert [bar["close"] for bar in bars] == [99.0, 101.0, 102.0]


def test_bar_cache_roundtrip_retention_and_contract_isolation(tmp_path: Path) -> None:
    now = datetime(2026, 7, 13, 12, tzinfo=timezone.utc)
    cache = BarCache(tmp_path / "bars.sqlite3", retention_days=8, max_rows=10)
    try:
        cache.put_bars(
            dataset="GLBX.MDP3",
            instrument_id=101,
            raw_symbol="ESU6",
            bars=[_bar(now - timedelta(days=10)), _bar(now, 101.0)],
            now=now,
        )
        cache.put_bars(
            dataset="GLBX.MDP3",
            instrument_id=202,
            raw_symbol="NQU6",
            bars=[_bar(now, 202.0)],
            now=now,
        )
        es = cache.get_bars(
            dataset="GLBX.MDP3",
            instrument_id=101,
            start=now - timedelta(days=20),
        )
        nq = cache.get_bars(
            dataset="GLBX.MDP3",
            instrument_id=202,
            start=now - timedelta(days=1),
        )
        assert [bar["close"] for bar in es] == [101.0]
        assert [bar["close"] for bar in nq] == [202.0]
        assert cache.count() == 2
    finally:
        cache.close()


def test_market_bindings_persist_only_for_the_current_trading_session(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    session_start = trading_day_start(now)
    cache = BarCache(tmp_path / "bars.sqlite3")
    try:
        cache.put_market_binding(
            dataset="GLBX.MDP3",
            market="ES",
            raw_symbol="ESU6",
            instrument_id=101,
            session_start=session_start,
            now=now,
        )
        assert cache.get_market_bindings(
            dataset="GLBX.MDP3",
            session_start=session_start,
        ) == {"ES": ("ESU6", 101)}
        assert cache.get_market_bindings(
            dataset="GLBX.MDP3",
            session_start=session_start + timedelta(days=1),
        ) == {}
    finally:
        cache.close()


def test_history_coverage_merges_prunes_and_zero_bar_ranges(tmp_path: Path) -> None:
    now = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    cache = BarCache(tmp_path / "bars.sqlite3", retention_days=8, max_rows=100)
    try:
        cache.record_coverage(
            dataset="GLBX.MDP3",
            instrument_id=101,
            raw_symbol="ESU6",
            start=now - timedelta(hours=3),
            end=now - timedelta(hours=2),
            now=now,
        )
        cache.record_coverage(
            dataset="GLBX.MDP3",
            instrument_id=101,
            raw_symbol="ESU6",
            start=now - timedelta(hours=2),
            end=now - timedelta(hours=1),
            now=now,
        )
        assert cache.get_coverage(
            dataset="GLBX.MDP3",
            instrument_id=101,
            start=now - timedelta(hours=4),
            end=now,
        ) == [(now - timedelta(hours=3), now - timedelta(hours=1))]
        assert cache.count() == 0
    finally:
        cache.close()


def test_history_plan_fills_only_gaps_chunks_newest_first_and_promotes() -> None:
    end = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    start = end - timedelta(hours=72)
    coverage = [
        (start, start + timedelta(hours=20)),
        (start + timedelta(hours=30), end - timedelta(hours=4)),
    ]
    missing = missing_intervals(start=start, end=end, coverage=coverage)
    assert missing == [
        (start + timedelta(hours=20), start + timedelta(hours=30)),
        (end - timedelta(hours=4), end),
    ]
    bindings = [
        HistoryBinding("ES", "ESU6", 101),
        HistoryBinding("NQ", "NQU6", 202),
    ]
    chunks = group_history_chunks(
        bindings,
        {101: [(start, end)], 202: [(start, end)]},
    )
    assert len(chunks) == 3
    assert chunks[0].end == end
    assert all(chunk.end - chunk.start <= timedelta(hours=24) for chunk in chunks)
    assert all(len(chunk.bindings) == 2 for chunk in chunks)
    promoted = promote_market(chunks, "NQ")
    assert [chunk.bindings[0].market for chunk in promoted[:3]] == ["NQ"] * 3
    assert sum(len(chunk.bindings) for chunk in promoted) == 6


def test_corrupt_cache_is_preserved_and_rebuilt(tmp_path: Path) -> None:
    path = tmp_path / "bars.sqlite3"
    path.write_bytes(b"not a sqlite database")
    cache = BarCache(path)
    try:
        assert cache.count() == 0
        assert path.is_file()
        assert len(list(tmp_path.glob("bars.sqlite3.corrupt-*"))) == 1
    finally:
        cache.close()


def test_demo_engine_exposes_41_markets_states_and_cached_timeframes() -> None:
    engine = DemoCockpitEngine(market="ES", timeframe="1m")
    messages: list[dict[str, object]] = []
    try:
        bootstrap = engine.bootstrap_event()
        payload = bootstrap["payload"]
        assert len(payload["markets"]) == 41
        market_names = {
            market["symbol"]: market["name"] for market in payload["markets"]
        }
        assert set(market_names) == set(market_names.values())
        assert payload["market_grouping_capability"] == {
            "alpha_tiers_available": True,
            "alpha_tier_groups": [
                {"id": "tier_1_core", "label": "Tier 1 · Core", "market_count": 4},
                {
                    "id": "tier_2_additions",
                    "label": "Tier 2 · Additions",
                    "market_count": 12,
                },
                {
                    "id": "tier_3_additions",
                    "label": "Tier 3 · Additions",
                    "market_count": 25,
                },
            ],
        }
        assert payload["visual_update_capability"] == {
            "default_mode": "smooth",
            "modes": {"efficient": 5.0, "smooth": 10.0, "high": 15.0},
            "adaptive_floor_hz": 5.0,
        }
        assert sum(
            market["alpha_tier_group"] is not None for market in payload["markets"]
        ) == 41
        engine.start(messages.append)
        states = {
            message["payload"]["state"]
            for message in messages
            if message["type"] == "market_status"
        }
        assert {"LIVE", "WAITING", "STALE", "ERROR"} <= states
        assert engine.select_timeframe("15m") is True
        snapshot = [message for message in messages if message["type"] == "chart_snapshot"][-1]
        assert snapshot["payload"]["timeframe"] == "15m"
        assert snapshot["payload"]["source"] == "timeframe-cache"
        assert engine.select_market("NQ") is True
        assert [message for message in messages if message["type"] == "chart_snapshot"][-1]["payload"]["market"] == "NQ"
    finally:
        engine.stop()


def test_demo_prediction_scenarios_are_deterministic_and_use_completed_bars() -> None:
    engine = DemoCockpitEngine(market="ES", timeframe="1m")
    messages: list[dict[str, object]] = []
    expected = {
        "ES": ("READY", "LONG", []),
        "NQ": ("READY", "SHORT", []),
        "RTY": ("ABSTAIN", None, ["MODEL_ABSTAINED"]),
        "YM": ("WARMING_UP", None, ["FEATURE_WARMUP_INCOMPLETE"]),
        "CL": ("STALE", None, ["DATA_STALE"]),
        "NG": ("ERROR", None, ["SYNTHETIC_DEMO_ERROR"]),
        "GC": ("ABSTAIN", None, ["OUTSIDE_DEMO_SCENARIO"]),
    }
    try:
        engine.start(messages.append)
        for market, (prediction_state, direction, reasons) in expected.items():
            if market != "ES":
                assert engine.select_market(market) is True
            prediction = [
                message
                for message in messages
                if message["type"] == "prediction_update"
                and message["payload"]["market"] == market
            ][-1]["payload"]
            snapshot = [
                message
                for message in messages
                if message["type"] == "chart_snapshot"
                and message["payload"]["market"] == market
            ][-1]["payload"]
            assert prediction["state"] == prediction_state
            assert prediction["synthetic"] is True
            assert prediction["observation_only"] is True
            assert prediction["input_bar_time"] == snapshot["bars"][-2]["time"]
            assert prediction["reason_codes"] == reasons
            if direction is None:
                assert prediction["forecast"] is None
            else:
                assert prediction["forecast"]["direction"] == direction
    finally:
        engine.stop()


def test_demo_data_health_exposes_all_display_states_without_provider_sessions() -> None:
    engine = DemoCockpitEngine(market="ES", timeframe="1m")
    messages: list[dict[str, object]] = []
    expected = {
        "ES": ("CURRENT", "PASS"),
        "RTY": ("DEGRADED", "PASS"),
        "YM": ("DEGRADED", "WARN"),
        "CL": ("STALE", "PASS"),
        "NG": ("UNKNOWN", "NOT_EVALUATED"),
    }
    try:
        bootstrap = engine.bootstrap_event()["payload"]
        assert bootstrap["max_provider_sessions"] == 0
        assert bootstrap["prediction_capability"] == {
            "mode": "synthetic_demo",
            "synthetic": True,
            "observation_only": True,
        }
        assert bootstrap["history_cache_capability"]["enabled"] is False
        engine.start(messages.append)
        for market, (health_state, continuity_state) in expected.items():
            if market != "ES":
                assert engine.select_market(market) is True
            health = [
                message
                for message in messages
                if message["type"] == "data_health"
                and message["payload"]["market"] == market
            ][-1]["payload"]
            assert health["state"] == health_state
            assert health["continuity"]["state"] == continuity_state
    finally:
        engine.stop()


def test_live_cockpit_prediction_source_fails_closed_without_new_sessions(tmp_path: Path) -> None:
    engine = LiveCockpitEngine(cache_path=tmp_path / "bars.sqlite3", env={})
    try:
        bootstrap = engine.bootstrap_event()["payload"]
        assert bootstrap["prediction_capability"] == {
            "mode": "offline",
            "synthetic": False,
            "observation_only": True,
        }
        assert bootstrap["max_provider_sessions"] == 2
        assert bootstrap["history_cache_capability"] == {
            "enabled": True,
            "cost_confirmation_required": True,
            "history_hours": 168,
            "market_count": 41,
        }
        assert isinstance(engine._prediction_source, NullPredictionSource)
        assert engine._live_sessions_started == 0
        assert engine._active_live_clients == {}
        source = (Path(cockpit_engine.__file__).parent / "predictions.py").read_text(
            encoding="utf-8"
        )
        assert "live_shadow_runner" not in source
        assert "scripts." not in source
    finally:
        engine.stop()


class _FakeWindow:
    def __init__(self) -> None:
        self.scripts: list[str] = []
        self.fullscreen_toggles = 0

    def evaluate_js(self, script: str) -> None:
        self.scripts.append(script)

    def toggle_fullscreen(self) -> None:
        self.fullscreen_toggles += 1


class _FakeEngine:
    def __init__(self) -> None:
        self.market = "ES"
        self.timeframe = "1m"
        self.stopped = False
        self.history_retries = 0
        self.history_confirmations: list[str] = []
        self.history_pauses: list[bool] = []
        self.history_estimate_retries = 0
        self.visual_update_mode = "smooth"
        self.visual_update_active = True

    def bootstrap_event(self):
        return event("bootstrap", {"markets": [], "selected_market": self.market, "timeframe": self.timeframe})

    def start(self, publish):
        publish(event("feed_status", {"scope": "focus", "state": "LIVE", "message": "ready"}))

    def select_market(self, market: str) -> bool:
        self.market = market
        return market in {"ES", "NQ"}

    def select_timeframe(self, timeframe: str) -> bool:
        self.timeframe = timeframe
        return timeframe in {"1m", "5m"}

    def retry_history(self) -> bool:
        self.history_retries += 1
        return True

    def confirm_history_cache(self, plan_id: str) -> bool:
        self.history_confirmations.append(plan_id)
        return True

    def set_history_cache_paused(self, paused: bool) -> bool:
        self.history_pauses.append(paused)
        return True

    def retry_history_cache_estimate(self) -> bool:
        self.history_estimate_retries += 1
        return True

    def set_visual_update_mode(self, mode: str) -> bool:
        if mode not in cockpit_engine.VISUAL_UPDATE_HZ:
            return False
        self.visual_update_mode = mode
        return True

    def set_visual_update_active(self, active: bool) -> float:
        self.visual_update_active = active
        return (
            cockpit_engine.VISUAL_UPDATE_HZ[self.visual_update_mode]
            if active
            else cockpit_engine.MIN_RENDER_HZ
        )

    def stop(self) -> None:
        self.stopped = True


def test_controller_bridges_events_and_persists_only_preferences(tmp_path: Path) -> None:
    engine = _FakeEngine()
    state_path = tmp_path / "state.json"
    controller = CockpitController(engine, state_path=state_path)
    window = _FakeWindow()
    controller.attach_window(window)
    assert controller.bootstrap()["type"] == "bootstrap"
    _wait_until(lambda: bool(controller._pending))
    messages = controller.poll_events()
    assert any(message["type"] == "feed_status" for message in messages)
    assert window.scripts == []
    assert controller.select_market("NQ") == {"ok": True, "generation": 0}
    assert controller.select_timeframe("5m") == {"ok": True}
    assert controller.retry_history() == {"ok": True, "generation": 0}
    assert engine.history_retries == 1
    assert controller.confirm_history_cache("plan-1") == {"ok": True}
    assert controller.set_history_cache_paused(True) == {"ok": True}
    assert controller.set_history_cache_paused("yes") == {"ok": False}
    assert controller.retry_history_cache_estimate() == {"ok": True}
    assert engine.history_confirmations == ["plan-1"]
    assert engine.history_pauses == [True]
    assert engine.history_estimate_retries == 1
    assert controller.set_ui_preferences(
        {
            "show_volume": False,
            "show_predictions": True,
            "prediction_panel_open": True,
            "visual_update_mode": "high",
            "market_grouping_mode": "alpha_tier",
            "sector_group_order": ["rates", "energy", "rates", "../bad"],
            "collapsed_alpha_tier_groups": ["tier_3_additions"],
            "unknown_key": True,
            "show_session_boundaries": "not-a-boolean",
        }
    ) == {
        "ok": True,
        "ui_preferences": {
            "show_volume": False,
            "show_predictions": True,
            "prediction_panel_open": True,
            "visual_update_mode": "high",
            "market_grouping_mode": "alpha_tier",
            "sector_group_order": ["rates", "energy"],
            "collapsed_alpha_tier_groups": ["tier_3_additions"],
        },
    }
    assert engine.visual_update_mode == "high"
    assert controller.set_visual_update_active(False) == {
        "ok": True,
        "effective_hz": 5.0,
    }
    assert controller.set_visual_update_active("no") == {
        "ok": False,
        "effective_hz": None,
    }
    assert controller.toggle_fullscreen() == {"ok": True, "fullscreen": True}
    assert controller.toggle_fullscreen() == {"ok": True, "fullscreen": False}
    assert window.fullscreen_toggles == 2
    assert load_state(state_path) == {
        "market": "NQ",
        "timeframe": "5m",
        "ui_preferences": {
            "show_volume": False,
            "show_predictions": True,
            "prediction_panel_open": True,
            "visual_update_mode": "high",
            "market_grouping_mode": "alpha_tier",
            "sector_group_order": ["rates", "energy"],
            "collapsed_alpha_tier_groups": ["tier_3_additions"],
        },
    }
    controller.stop()
    assert engine.stopped is True


def test_pywebview_api_keeps_controller_graph_private(tmp_path: Path) -> None:
    controller = CockpitController(_FakeEngine(), state_path=tmp_path / "state.json")
    api = CockpitApi(controller)
    assert not hasattr(api, "controller")
    assert api._controller is controller
    assert api.toggle_fullscreen() == {"ok": False, "fullscreen": False}
    assert api.set_ui_preferences({"show_volume": False}) == {
        "ok": True,
        "ui_preferences": {"show_volume": False},
    }
    assert api.set_visual_update_active(False) == {
        "ok": True,
        "effective_hz": 5.0,
    }


def test_controller_coalesces_only_matching_pending_bar_updates(tmp_path: Path) -> None:
    controller = CockpitController(_FakeEngine(), state_path=tmp_path / "state.json")
    first = event(
        "bar_update",
        {"market": "ES", "timeframe": "1m", "generation": 1, "bar": {"close": 1}},
    )
    newest = event(
        "bar_update",
        {"market": "ES", "timeframe": "1m", "generation": 1, "bar": {"close": 2}},
    )
    other_generation = event(
        "bar_update",
        {"market": "ES", "timeframe": "1m", "generation": 2, "bar": {"close": 3}},
    )
    status = event("feed_status", {"scope": "focus", "state": "LIVE", "message": "ready"})
    controller.publish(first)
    controller.publish(status)
    controller.publish(newest)
    controller.publish(other_generation)
    messages = controller.poll_events()
    assert [message["type"] for message in messages] == ["feed_status", "bar_update", "bar_update"]
    assert messages[1]["payload"]["bar"]["close"] == 2
    assert messages[2]["payload"]["generation"] == 2


class _FakeTimeseries:
    TIMEOUT = 999
    outcomes: list[object] = []
    calls: list[dict[str, object]] = []

    def get_range(self, **kwargs):
        self.__class__.calls.append(dict(kwargs))
        if not self.__class__.outcomes:
            return object()
        outcome = self.__class__.outcomes.pop(0)
        if callable(outcome):
            outcome = outcome(kwargs)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class _FakeMetadata:
    TIMEOUT = 999
    outcomes: list[object] = []
    calls: list[dict[str, object]] = []
    range_outcomes: list[object] = []
    range_calls: list[dict[str, object]] = []

    def get_dataset_range(self, **kwargs):
        self.__class__.range_calls.append(dict(kwargs))
        if not self.__class__.range_outcomes:
            return {
                "start": "2010-01-01T00:00:00Z",
                "end": "2099-01-01T00:00:00Z",
                "schema": {
                    "ohlcv-1m": {
                        "start": "2010-01-01T00:00:00Z",
                        "end": "2099-01-01T00:00:00Z",
                    }
                },
            }
        outcome = self.__class__.range_outcomes.pop(0)
        if callable(outcome):
            outcome = outcome(kwargs)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def get_cost(self, **kwargs):
        self.__class__.calls.append(dict(kwargs))
        if not self.__class__.outcomes:
            return 0.0
        outcome = self.__class__.outcomes.pop(0)
        if callable(outcome):
            outcome = outcome(kwargs)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class _FakeHistorical:
    def __init__(self, **_kwargs) -> None:
        self.symbology = SimpleNamespace(TIMEOUT=999)
        self.timeseries = _FakeTimeseries()
        self.metadata = _FakeMetadata()


class _FakeLive:
    instances: list["_FakeLive"] = []
    active = 0
    max_active = 0
    fail_overview = False

    def __init__(self, **_kwargs) -> None:
        self.subscription: dict[str, object] = {}
        self.subscriptions: list[dict[str, object]] = []
        self.callback = None
        self.started = False
        self.stopped = False
        self.__class__.instances.append(self)

    def subscribe(self, **kwargs) -> None:
        self.subscription = kwargs
        self.subscriptions.append(kwargs)

    def add_callback(self, callback) -> None:
        self.callback = callback

    def start(self) -> None:
        if self.fail_overview and self.subscription.get("schema") == "ohlcv-1m":
            raise RuntimeError("overview unavailable")
        self.started = True
        self.__class__.active += 1
        self.__class__.max_active = max(self.__class__.max_active, self.__class__.active)

    def stop(self) -> None:
        if self.started and not self.stopped:
            self.__class__.active -= 1
        self.stopped = True

    def block_for_close(self, **_kwargs) -> None:
        return None


class _FakeDb:
    Live = _FakeLive
    Historical = _FakeHistorical


@pytest.fixture(autouse=True)
def _reset_fake_live() -> None:
    _FakeLive.instances = []
    _FakeLive.active = 0
    _FakeLive.max_active = 0
    _FakeLive.fail_overview = False
    _FakeTimeseries.outcomes = []
    _FakeTimeseries.calls = []
    _FakeMetadata.outcomes = []
    _FakeMetadata.calls = []
    _FakeMetadata.range_outcomes = []
    _FakeMetadata.range_calls = []


def _patch_live_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cockpit_engine, "FOCUS_MAPPING_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(
        cockpit_engine,
        "resolve_cockpit_api_key_source",
        lambda _env: SimpleNamespace(key="db-test", source="test"),
    )
    monkeypatch.setattr(
        cockpit_engine,
        "resolve_single_instrument",
        lambda _historical, **kwargs: SimpleNamespace(
            market=kwargs["market"],
            raw_symbol=f"{kwargs['market']}U6",
            instrument_id={"ES": 101, "NQ": 202, "CL": 303}.get(kwargs["market"], 999),
        ),
    )
    monkeypatch.setattr(cockpit_engine, "historical_store_to_candles", lambda _store: [])


def _bind_all_history_markets(engine: LiveCockpitEngine) -> list[HistoryBinding]:
    bindings = [
        HistoryBinding(info.symbol, f"{info.symbol}U6", 1_000 + index)
        for index, info in enumerate(engine.markets)
    ]
    for binding in bindings:
        engine._remember_binding(binding)
    return bindings


def _dataset_range(*, schema_end: object) -> dict[str, object]:
    return {
        "start": "2010-01-01T00:00:00Z",
        "end": "2099-01-01T00:00:00Z",
        "schema": {
            "ohlcv-1m": {
                "start": "2010-01-01T00:00:00Z",
                "end": schema_end,
            }
        },
    }


def test_market_switch_publishes_persisted_cache_before_focus_swap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_live_dependencies(monkeypatch)
    cache_path = tmp_path / "bars.sqlite3"
    now = datetime.now(timezone.utc)
    minute = now.replace(second=0, microsecond=0) - timedelta(minutes=2)
    cache = BarCache(cache_path)
    try:
        cache.put_market_binding(
            dataset="GLBX.MDP3",
            market="NQ",
            raw_symbol="NQU6",
            instrument_id=202,
            session_start=trading_day_start(now),
            now=now,
        )
        cache.put_bars(
            dataset="GLBX.MDP3",
            instrument_id=202,
            raw_symbol="NQU6",
            bars=[_bar(minute, 202.0)],
            now=now,
        )
    finally:
        cache.close()

    engine = LiveCockpitEngine(
        cache_path=cache_path,
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    messages: list[dict[str, object]] = []
    old_focus = _FakeLive()
    old_focus.subscribe(
        dataset="GLBX.MDP3",
        schema="trades",
        symbols=101,
        stype_in="instrument_id",
    )
    old_focus.start()
    try:
        engine._publish = messages.append
        engine._api_key = "db-test"
        engine._historical = _FakeHistorical()
        engine._focus_client = old_focus

        assert engine.select_market("NQ") is True
        cached_snapshot = next(
            message
            for message in messages
            if message["type"] == "chart_snapshot"
            and message["payload"]["source"] == "selection-cache"
        )
        assert cached_snapshot["payload"]["market"] == "NQ"
        assert cached_snapshot["payload"]["contract"] == "NQU6"
        assert cached_snapshot["payload"]["bars"][-1]["close"] == 202.0
        assert old_focus.stopped is False

        engine._activate_focus("NQ", engine.generation)
        assert old_focus.stopped is True
        assert any(
            item.subscription.get("schema") == "trades"
            and item.subscription.get("symbols") == 202
            for item in _FakeLive.instances
        )
        assert _FakeLive.max_active == 1
    finally:
        engine.stop()
    assert _FakeLive.active == 0


def test_live_engine_serializes_switches_and_never_exceeds_two_sessions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_live_dependencies(monkeypatch)
    messages: list[dict[str, object]] = []
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    try:
        engine.start(messages.append)
        _wait_until(lambda: len(_FakeLive.instances) >= 2)
        assert engine._historical.symbology.TIMEOUT == SYMBOL_REQUEST_TIMEOUT_SECONDS
        assert engine._historical.timeseries.TIMEOUT == HISTORY_REQUEST_TIMEOUT_SECONDS
        assert engine._historical.metadata.TIMEOUT == HISTORY_REQUEST_TIMEOUT_SECONDS
        assert any(
            message["type"] == "chart_snapshot"
            and message["payload"]["source"] == "contract-resolved"
            for message in messages
        )
        assert _FakeLive.instances[0].subscription["schema"] == "ohlcv-1m"
        assert len(_FakeLive.instances[0].subscription["symbols"]) == 41
        assert _FakeLive.instances[1].subscription["schema"] == "trades"
        assert [
            subscription["schema"]
            for subscription in _FakeLive.instances[1].subscriptions
        ] == ["ohlcv-1m", "trades"]
        replay_start = _FakeLive.instances[1].subscriptions[0]["start"]
        assert isinstance(replay_start, datetime)
        assert datetime.now(timezone.utc) - replay_start < timedelta(hours=24)
        assert _FakeLive.max_active == 2

        replay_minute = datetime.now(timezone.utc).replace(
            second=0, microsecond=0
        ) - timedelta(minutes=3)
        _FakeLive.instances[1].callback(
            SimpleNamespace(
                ts_event=replay_minute,
                open=100_000_000_000,
                high=102_000_000_000,
                low=99_000_000_000,
                close=101_000_000_000,
                volume=6,
            )
        )
        _wait_until(
            lambda: any(
                message["type"] == "chart_snapshot"
                and message["payload"]["source"] == "recent-replay"
                for message in messages
            )
        )
        assert engine.runtime_metrics()["replay_subscriptions"] == 1
        assert engine.runtime_metrics()["max_live_sessions"] == 2

        engine.select_market("NQ")
        engine.select_market("CL")
        _wait_until(
            lambda: any(
                item.subscription.get("schema") == "trades"
                and item.subscription.get("symbols") == 303
                for item in _FakeLive.instances
            )
        )
        assert _FakeLive.max_active == 2
        assert not any(
            item.subscription.get("schema") == "trades"
            and item.subscription.get("symbols") == 202
            for item in _FakeLive.instances
        )
    finally:
        engine.stop()
    assert _FakeLive.active == 0


def test_stale_replay_record_is_discarded(tmp_path: Path) -> None:
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    engine.generation = 2
    engine._raw_bars = [_bar(datetime(2026, 7, 14, 11, 0, tzinfo=timezone.utc))]
    engine._on_focus_record(
        SimpleNamespace(
            ts_event=datetime(2026, 7, 14, 11, 1, tzinfo=timezone.utc),
            open=100_000_000_000,
            high=102_000_000_000,
            low=99_000_000_000,
            close=101_000_000_000,
            volume=6,
        ),
        generation=1,
        aggregator=TradeCandleAggregator(timeframe_seconds=60, timeframe="1m"),
    )
    assert len(engine._raw_bars) == 1
    assert engine._pending_snapshot_generation is None
    engine.stop()


def test_history_backfill_does_not_block_focus_start_or_next_switch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_live_dependencies(monkeypatch)
    monkeypatch.setattr(cockpit_engine, "HISTORY_MAPPING_WAIT_SECONDS", 0.0)
    history_started = threading.Event()
    release_history = threading.Event()

    def delayed_history(_kwargs):
        history_started.set()
        assert release_history.wait(timeout=2.0)
        return []

    _FakeTimeseries.outcomes = [delayed_history, *([[]] * 20)]
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    instrument_ids = {"ES": 101, "NQ": 202, "CL": 303}
    for index, info in enumerate(engine.markets):
        engine._remember_binding(
            HistoryBinding(
                info.symbol,
                f"{info.symbol}U6",
                instrument_ids.get(info.symbol, 1_000 + index),
            )
        )
    messages: list[dict[str, object]] = []
    try:
        engine.start(messages.append)
        _wait_until(
            lambda: any(
                message["type"] == "history_cache_status"
                and message["payload"]["state"] == "CONFIRMATION_REQUIRED"
                for message in messages
            )
        )
        assert _FakeTimeseries.calls == []
        assert _FakeMetadata.range_calls == [{"dataset": "GLBX.MDP3"}]
        assert len(_FakeMetadata.calls) == 7
        assert all(len(call["symbols"]) == 41 for call in _FakeMetadata.calls)
        _wait_until(
            lambda: any(
                item.subscription.get("schema") == "trades"
                and item.subscription.get("symbols") == 101
                for item in _FakeLive.instances
            )
        )
        plan_id = next(
            message["payload"]["plan_id"]
            for message in messages
            if message["type"] == "history_cache_status"
            and message["payload"]["state"] == "CONFIRMATION_REQUIRED"
        )
        assert engine.select_market("NQ") is True
        assert engine.confirm_history_cache(plan_id) is True
        assert engine.confirm_history_cache(plan_id) is False
        assert history_started.wait(timeout=2.0)
        assert engine.select_market("CL") is True
        _wait_until(
            lambda: any(
                item.subscription.get("schema") == "trades"
                and item.subscription.get("symbols") == 303
                for item in _FakeLive.instances
            )
        )
        assert release_history.is_set() is False
        assert _FakeLive.max_active == 2
        assert engine.runtime_metrics()["history_requests"] == 1
        assert engine.set_history_cache_paused(True) is True
        release_history.set()
        time.sleep(0.1)
        assert engine.runtime_metrics()["history_requests"] == 1
        assert engine.set_history_cache_paused(False) is True
        _wait_until(lambda: engine.runtime_metrics()["history_requests"] > 1)
    finally:
        release_history.set()
        engine.stop()
    assert _FakeLive.active == 0


def test_history_failure_keeps_live_connected_and_manual_retry_merges_completed_bars(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_live_dependencies(monkeypatch)
    monkeypatch.setattr(cockpit_engine, "HISTORY_MAPPING_WAIT_SECONDS", 0.0)
    fixed_now = datetime(2026, 7, 14, 5, 24, 30, tzinfo=timezone.utc)
    minute = fixed_now.replace(second=0, microsecond=0)

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is not None else fixed_now.replace(tzinfo=None)

    secret = "db-history-secret"
    _FakeTimeseries.outcomes = [TimeoutError(f"read timed out {secret}")]
    monkeypatch.setattr(cockpit_engine, "datetime", _FixedDateTime)
    monkeypatch.setattr(cockpit_cache, "datetime", _FixedDateTime)

    messages: list[dict[str, object]] = []
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    instrument_ids = {"ES": 101, "NQ": 202, "CL": 303}
    for index, info in enumerate(engine.markets):
        engine._remember_binding(
            HistoryBinding(
                info.symbol,
                f"{info.symbol}U6",
                instrument_ids.get(info.symbol, 1_000 + index),
            )
        )
    try:
        assert engine.cache is not None
        engine.cache.put_bars(
            dataset="GLBX.MDP3",
            instrument_id=101,
            raw_symbol="ESU6",
            bars=[_bar(minute - timedelta(minutes=2), 100.0)],
            now=fixed_now,
        )
        engine.start(messages.append)
        _wait_until(
            lambda: any(
                message["type"] == "history_cache_status"
                and message["payload"]["state"] == "CONFIRMATION_REQUIRED"
                for message in messages
            )
        )
        assert _FakeTimeseries.calls == []
        first_plan = next(
            message["payload"]["plan_id"]
            for message in messages
            if message["type"] == "history_cache_status"
            and message["payload"]["state"] == "CONFIRMATION_REQUIRED"
        )
        assert engine.confirm_history_cache(first_plan) is True
        _wait_until(
            lambda: any(
                message["type"] == "history_cache_status"
                and message["payload"]["state"] == "ERROR"
                for message in messages
            )
        )
        _wait_until(
            lambda: any(
                item.subscription.get("schema") == "trades"
                for item in _FakeLive.instances
            )
        )
        trade_client = next(
            item for item in _FakeLive.instances if item.subscription.get("schema") == "trades"
        )
        trade_client.callback(
            SimpleNamespace(
                ts_event=minute + timedelta(seconds=10),
                price=101_000_000_000,
                size=2,
            )
        )
        _wait_until(
            lambda: any(
                message["type"] == "feed_status"
                and message["payload"].get("scope") == "focus"
                and message["payload"].get("state") == "LIVE"
                for message in messages
            )
        )

        history_error = next(
            message["payload"]
            for message in messages
            if message["type"] == "history_cache_status"
            and message["payload"].get("state") == "ERROR"
        )
        assert history_error["failure_category"] == "TIMEOUT"
        assert secret not in json.dumps(history_error)
        assert engine.runtime_metrics()["history_failure"]["failure_category"] == "TIMEOUT"

        live_instances = len(_FakeLive.instances)
        historical_records = [
            SimpleNamespace(
                instrument_id=101,
                ts_event=minute - timedelta(minutes=2),
                open=199_750_000_000,
                high=200_500_000_000,
                low=199_500_000_000,
                close=200_000_000_000,
                volume=25,
            ),
            SimpleNamespace(
                instrument_id=101,
                ts_event=minute,
                open=998_750_000_000,
                high=999_500_000_000,
                low=998_500_000_000,
                close=999_000_000_000,
                volume=25,
            ),
        ]
        _FakeTimeseries.outcomes = [historical_records, *([[]] * 10)]
        assert engine.retry_history() is True
        assert engine.retry_history() is False
        _wait_until(
            lambda: any(
                message["type"] == "history_cache_status"
                and message["payload"].get("state") == "CONFIRMATION_REQUIRED"
                and message["payload"].get("plan_id") != first_plan
                for message in messages
            )
        )
        second_plan = next(
            message["payload"]["plan_id"]
            for message in reversed(messages)
            if message["type"] == "history_cache_status"
            and message["payload"].get("state") == "CONFIRMATION_REQUIRED"
        )
        assert engine.confirm_history_cache(second_plan) is True
        _wait_until(
            lambda: any(
                message["type"] == "history_cache_status"
                and message["payload"].get("state") == "COMPLETE"
                for message in messages
            )
        )
        snapshot = [
            message["payload"]
            for message in messages
            if message["type"] == "chart_snapshot"
            and message["payload"].get("source") == "historical"
        ][-1]
        bars = {bar["time"]: bar for bar in snapshot["bars"]}
        assert bars[int((minute - timedelta(minutes=2)).timestamp())]["close"] == 200.0
        assert bars[int(minute.timestamp())]["close"] == 101.0
        assert len(_FakeLive.instances) == live_instances
        assert _FakeLive.max_active == 2
        assert engine.runtime_metrics()["history_requests"] == 8
        assert engine.runtime_metrics()["history_plan_confirmations"] == 2
        assert engine.runtime_metrics()["history_failure"] is None
    finally:
        engine.stop()


@pytest.mark.parametrize(
    ("range_value", "expected_category"),
    [
        ({}, "DATA_AVAILABILITY"),
        ({"schema": {}}, "DATA_AVAILABILITY"),
        (
            {"schema": {"ohlcv-1m": {"end": "not-a-timestamp"}}},
            "DATA_AVAILABILITY",
        ),
        (
            {"schema": {"ohlcv-1m": {"end": "2026-07-14T05:24:00"}}},
            "DATA_AVAILABILITY",
        ),
        (
            {"schema": {"ohlcv-1m": {"end": "2026-07-07T05:24:00Z"}}},
            "DATA_AVAILABILITY",
        ),
    ],
)
def test_history_availability_invalid_boundaries_fail_before_cost_or_download(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    range_value: object,
    expected_category: str,
) -> None:
    fixed_now = datetime(2026, 7, 14, 5, 24, 30, tzinfo=timezone.utc)

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is not None else fixed_now.replace(tzinfo=None)

    monkeypatch.setattr(cockpit_engine, "datetime", _FixedDateTime)
    _FakeMetadata.range_outcomes = [range_value]
    messages: list[dict[str, object]] = []
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    try:
        engine._historical = _FakeHistorical()
        engine._publish = messages.append
        _bind_all_history_markets(engine)
        engine._prepare_history_plan()
        error_payload = next(
            message["payload"]
            for message in messages
            if message["type"] == "history_cache_status"
            and message["payload"].get("state") == "ERROR"
        )
        assert error_payload["failure_category"] == expected_category
        assert error_payload["diagnostic"]["phase"] == "DATASET_RANGE"
        assert error_payload["diagnostic"]["download_began"] is False
        assert _FakeMetadata.range_calls == [{"dataset": "GLBX.MDP3"}]
        assert _FakeMetadata.calls == []
        assert _FakeTimeseries.calls == []
    finally:
        engine.stop()


@pytest.mark.parametrize(
    ("failure", "expected_category"),
    [
        (TimeoutError("range timeout db-range-secret"), "TIMEOUT"),
        (ConnectionError("connection db-range-secret"), "CONNECTION"),
        (RuntimeError("authentication failed db-range-secret"), "AUTHORIZATION"),
        (RuntimeError("provider body db-range-secret"), "UNAVAILABLE"),
    ],
)
def test_history_availability_provider_failures_are_bounded_and_sanitized(
    tmp_path: Path,
    failure: Exception,
    expected_category: str,
) -> None:
    _FakeMetadata.range_outcomes = [failure]
    messages: list[dict[str, object]] = []
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    try:
        engine._historical = _FakeHistorical()
        engine._publish = messages.append
        _bind_all_history_markets(engine)
        engine._prepare_history_plan()
        error_payload = next(
            message["payload"]
            for message in messages
            if message["type"] == "history_cache_status"
            and message["payload"].get("state") == "ERROR"
        )
        assert error_payload["failure_category"] == expected_category
        assert error_payload["diagnostic"]["phase"] == "DATASET_RANGE"
        serialized = json.dumps(
            {
                "events": messages,
                "metrics": engine.runtime_metrics(),
            }
        )
        assert "db-range-secret" not in serialized
        assert "provider body" not in serialized
        assert _FakeMetadata.calls == []
        assert _FakeTimeseries.calls == []
    finally:
        engine.stop()


def test_history_availability_end_equal_to_completed_minute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixed_now = datetime(2026, 7, 14, 5, 24, 30, tzinfo=timezone.utc)
    completed_minute = fixed_now.replace(second=0, microsecond=0)

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is not None else fixed_now.replace(tzinfo=None)

    monkeypatch.setattr(cockpit_engine, "datetime", _FixedDateTime)
    _FakeMetadata.range_outcomes = [
        _dataset_range(schema_end=completed_minute.isoformat())
    ]
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    try:
        engine._historical = _FakeHistorical()
        engine._publish = lambda _message: None
        _bind_all_history_markets(engine)
        engine._prepare_history_plan()
        with engine._history_lock:
            assert engine._history_plan is not None
            assert engine._history_plan.target_end == completed_minute
            assert all(
                chunk.end <= completed_minute for chunk in engine._history_plan.chunks
            )
        assert len(_FakeMetadata.calls) == 7
        assert _FakeTimeseries.calls == []
    finally:
        engine.stop()


def test_history_availability_boundary_clamps_cost_and_download_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_live_dependencies(monkeypatch)
    monkeypatch.setattr(cockpit_engine, "HISTORY_MAPPING_WAIT_SECONDS", 0.0)
    fixed_now = datetime(2026, 7, 14, 5, 24, 30, tzinfo=timezone.utc)
    available_end = datetime(2026, 7, 14, tzinfo=timezone.utc)

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is not None else fixed_now.replace(tzinfo=None)

    _FakeMetadata.range_outcomes = [
        {
            "start": "2010-01-01T00:00:00Z",
            "end": "2026-07-14T05:24:00Z",
            "schema": {
                "ohlcv-1m": {
                    "start": "2010-01-01T00:00:00Z",
                    "end": available_end.isoformat(),
                }
            },
        }
    ]
    _FakeTimeseries.outcomes = [*([[]] * 10)]
    monkeypatch.setattr(cockpit_engine, "datetime", _FixedDateTime)

    messages: list[dict[str, object]] = []
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    instrument_ids = {"ES": 101, "NQ": 202, "CL": 303}
    for index, info in enumerate(engine.markets):
        engine._remember_binding(
            HistoryBinding(
                info.symbol,
                f"{info.symbol}U6",
                instrument_ids.get(info.symbol, 1_000 + index),
            )
        )
    try:
        engine.start(messages.append)
        _wait_until(
            lambda: any(
                message["type"] == "history_cache_status"
                and message["payload"].get("state") == "CONFIRMATION_REQUIRED"
                for message in messages
            )
        )
        plan_id = next(
            message["payload"]["plan_id"]
            for message in messages
            if message["type"] == "history_cache_status"
            and message["payload"].get("state") == "CONFIRMATION_REQUIRED"
        )
        with engine._history_lock:
            assert engine._history_plan is not None
            assert engine._history_plan.target_end == available_end
            assert all(
                chunk.end <= available_end for chunk in engine._history_plan.chunks
            )
        assert len(_FakeMetadata.range_calls) == 1
        assert len(_FakeMetadata.calls) == 7
        assert all(call["end"] <= available_end for call in _FakeMetadata.calls)
        assert _FakeTimeseries.calls == []
        assert engine.confirm_history_cache(plan_id) is True
        _wait_until(
            lambda: any(
                message["type"] == "history_cache_status"
                and message["payload"].get("state") == "PARTIAL"
                for message in messages
            )
        )
        assert len(_FakeTimeseries.calls) == 7
        assert all(call["end"] <= available_end for call in _FakeTimeseries.calls)
        assert not any(
            message["type"] == "history_cache_status"
            and message["payload"].get("state") == "ERROR"
            for message in messages
        )
        assert engine.runtime_metrics()["history_requests"] == 7
        assert engine.runtime_metrics()["history_dataset_range_requests"] == 1
        assert engine.runtime_metrics()["history_failure"] is None
    finally:
        engine.stop()


def test_stale_history_plan_is_rejected_and_switch_discards_old_focus_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cockpit_engine, "HISTORY_MAPPING_WAIT_SECONDS", 0.0)
    started = threading.Event()
    release = threading.Event()

    def delayed_history(_kwargs):
        started.set()
        assert release.wait(timeout=2.0)
        return []

    messages: list[dict[str, object]] = []
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    try:
        engine._historical = _FakeHistorical()
        engine._publish = messages.append
        for index, info in enumerate(engine.markets):
            engine._remember_binding(
                HistoryBinding(
                    info.symbol,
                    f"{info.symbol}U6",
                    {"ES": 101, "NQ": 202}.get(info.symbol, 1_000 + index),
                )
            )
        engine.generation = 1
        engine._contract = "ESU6"
        engine._resolved_instrument_id = 101
        engine._ensure_history_worker()
        _wait_until(
            lambda: any(
                message["type"] == "history_cache_status"
                and message["payload"].get("state") == "CONFIRMATION_REQUIRED"
                for message in messages
            )
        )
        first_plan = next(
            message["payload"]["plan_id"]
            for message in messages
            if message["type"] == "history_cache_status"
            and message["payload"].get("state") == "CONFIRMATION_REQUIRED"
        )
        assert engine.confirm_history_cache("wrong-plan") is False
        with engine._history_lock:
            assert engine._history_plan is not None
            engine._history_plan.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        assert engine.confirm_history_cache(first_plan) is False
        assert _FakeTimeseries.calls == []
        assert engine.retry_history() is True
        _wait_until(
            lambda: any(
                message["type"] == "history_cache_status"
                and message["payload"].get("state") == "CONFIRMATION_REQUIRED"
                and message["payload"].get("plan_id") != first_plan
                for message in messages
            )
        )
        second_plan = next(
            message["payload"]["plan_id"]
            for message in reversed(messages)
            if message["type"] == "history_cache_status"
            and message["payload"].get("state") == "CONFIRMATION_REQUIRED"
        )
        _FakeTimeseries.outcomes = [delayed_history, *([[]] * 10)]
        assert engine.confirm_history_cache(second_plan) is True
        assert started.wait(timeout=2.0)
        with engine._lock:
            engine.market = "NQ"
            engine._contract = "NQU6"
            engine._resolved_instrument_id = 202
            engine.generation = 2
        release.set()
        _wait_until(
            lambda: any(
                message["type"] == "history_cache_status"
                and message["payload"].get("state") == "COMPLETE"
                for message in messages
            )
        )
        historical_snapshots = [
            message["payload"]
            for message in messages
            if message["type"] == "chart_snapshot"
            and message["payload"].get("source") == "historical"
        ]
        assert all(snapshot["market"] == "NQ" for snapshot in historical_snapshots)
        assert engine.runtime_metrics()["history_plan_confirmations"] == 1
    finally:
        release.set()
        engine.stop()


@pytest.mark.parametrize(
    ("failure", "expected_category"),
    [
        (TimeoutError("estimate timed out db-estimate-secret"), "TIMEOUT"),
        (ConnectionError("connection db-estimate-secret"), "CONNECTION"),
        (
            RuntimeError("authentication failed db-estimate-secret"),
            "AUTHORIZATION",
        ),
        (RuntimeError("provider body db-estimate-secret"), "UNAVAILABLE"),
    ],
)
def test_history_cost_estimate_failure_is_redacted_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    expected_category: str,
) -> None:
    monkeypatch.setattr(cockpit_engine, "HISTORY_MAPPING_WAIT_SECONDS", 0.0)
    secret = "db-estimate-secret"
    _FakeMetadata.outcomes = [failure]
    messages: list[dict[str, object]] = []
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    try:
        engine._historical = _FakeHistorical()
        engine._publish = messages.append
        for index, info in enumerate(engine.markets):
            engine._remember_binding(
                HistoryBinding(info.symbol, f"{info.symbol}U6", 1_000 + index)
            )
        engine._ensure_history_worker()
        _wait_until(
            lambda: any(
                message["type"] == "history_cache_status"
                and message["payload"].get("state") == "ERROR"
                for message in messages
            )
        )
        error_payload = next(
            message["payload"]
            for message in messages
            if message["type"] == "history_cache_status"
            and message["payload"].get("state") == "ERROR"
        )
        assert error_payload["failure_category"] == expected_category
        assert error_payload["diagnostic"] == {
            "phase": "COST_ESTIMATE",
            "chunk_number": None,
            "requested_start": error_payload["diagnostic"]["requested_start"],
            "requested_end": error_payload["diagnostic"]["requested_end"],
            "download_began": False,
        }
        assert secret not in json.dumps(error_payload)
        assert "provider body" not in json.dumps(
            {"events": messages, "metrics": engine.runtime_metrics()}
        )
        assert _FakeTimeseries.calls == []
        assert engine.runtime_metrics()["live_sessions_started"] == 0
    finally:
        engine.stop()


@pytest.mark.parametrize(
    ("live_event", "expected_state"),
    [
        (None, "DEGRADED"),
        ((3, "ESU6", timedelta(seconds=30)), "CURRENT"),
        ((3, "ESU6", timedelta(seconds=151)), "DEGRADED"),
        ((3, "ESU6", timedelta(seconds=-1)), "DEGRADED"),
        ((3, "NQU6", timedelta(seconds=30)), "DEGRADED"),
        ((2, "ESU6", timedelta(seconds=30)), "DEGRADED"),
    ],
)
def test_history_cache_complete_requires_fresh_matching_live_tail(
    tmp_path: Path,
    live_event: tuple[int, str, timedelta] | None,
    expected_state: str,
) -> None:
    evaluated_at = datetime(2026, 7, 14, 5, 24, 30, tzinfo=timezone.utc)
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    try:
        engine.generation = 3
        engine._history_complete_generation = 3
        if live_event is not None:
            generation, contract, age = live_event
            engine._focus_last_live_event = (
                generation,
                contract,
                evaluated_at - age,
            )
        payload = engine._focus_health_payload(
            market="ES",
            contract="ESU6",
            instrument_id=1_000,
            timeframe="1m",
            bars=[_bar(evaluated_at - timedelta(minutes=1))],
            generation=3,
            evaluated_at=evaluated_at,
        )
        assert payload["state"] == expected_state
        if expected_state == "CURRENT":
            assert "DATA_STALE" not in payload["reason_codes"]
        else:
            assert "DATA_STALE" in payload["reason_codes"]
    finally:
        engine.stop()


def test_history_cache_incomplete_never_becomes_current_from_fresh_live_tail(
    tmp_path: Path,
) -> None:
    evaluated_at = datetime(2026, 7, 14, 5, 24, 30, tzinfo=timezone.utc)
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    try:
        engine.generation = 3
        engine._focus_last_live_event = (
            3,
            "ESU6",
            evaluated_at - timedelta(seconds=30),
        )
        payload = engine._focus_health_payload(
            market="ES",
            contract="ESU6",
            instrument_id=1_000,
            timeframe="1m",
            bars=[_bar(evaluated_at - timedelta(minutes=1))],
            generation=3,
            history_state="PARTIAL",
            evaluated_at=evaluated_at,
        )
        assert payload["state"] == "DEGRADED"
        assert "HISTORY_PARTIAL" in payload["reason_codes"]
        empty_payload = engine._focus_health_payload(
            market="ES",
            contract="ESU6",
            instrument_id=1_000,
            timeframe="1m",
            bars=[],
            generation=3,
            history_state="LOADING",
            evaluated_at=evaluated_at,
        )
        assert empty_payload["state"] == "DEGRADED"
        assert "HISTORY_LOADING" in empty_payload["reason_codes"]
    finally:
        engine.stop()


def test_history_cache_daily_advancement_estimates_only_new_delta(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    current = [datetime(2026, 7, 14, 5, 24, 30, tzinfo=timezone.utc)]

    class _AdvancingDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = current[0]
            return value if tz is not None else value.replace(tzinfo=None)

    monkeypatch.setattr(cockpit_engine, "datetime", _AdvancingDateTime)
    first_end = current[0].replace(second=0, microsecond=0)
    second_end = first_end + timedelta(days=1)
    _FakeMetadata.range_outcomes = [
        _dataset_range(schema_end=first_end.isoformat()),
        _dataset_range(schema_end=second_end.isoformat()),
    ]
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    try:
        engine._historical = _FakeHistorical()
        engine._publish = lambda _message: None
        bindings = _bind_all_history_markets(engine)
        engine._prepare_history_plan()
        with engine._history_lock:
            assert engine._history_plan is not None
            first_plan_id = engine._history_plan.plan_id
            first_start = engine._history_plan.target_start
            assert engine._history_plan.target_end == first_end
        assert engine.cache is not None
        for binding in bindings:
            engine.cache.record_coverage(
                dataset="GLBX.MDP3",
                instrument_id=binding.instrument_id,
                raw_symbol=binding.contract,
                start=first_start,
                end=first_end,
                now=first_end,
            )
        current[0] = current[0] + timedelta(days=1)
        engine._prepare_history_plan()
        with engine._history_lock:
            assert engine._history_plan is not None
            second_plan = engine._history_plan
            assert second_plan.plan_id != first_plan_id
            assert second_plan.confirmed is False
            assert second_plan.target_end == second_end
            assert len(second_plan.chunks) == 1
            assert second_plan.chunks[0].start == first_end
            assert second_plan.chunks[0].end == second_end
            assert len(second_plan.chunks[0].bindings) == 41
        assert len(_FakeMetadata.range_calls) == 2
        assert len(_FakeMetadata.calls) == 8
        assert _FakeTimeseries.calls == []
    finally:
        engine.stop()


def test_focus_resolution_timeout_retries_once_then_connects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_live_dependencies(monkeypatch)
    calls = 0

    def resolve_after_timeout(_historical, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("read timed out")
        return SimpleNamespace(
            market=kwargs["market"], raw_symbol="ESU6", instrument_id=101
        )

    monkeypatch.setattr(cockpit_engine, "resolve_single_instrument", resolve_after_timeout)
    monkeypatch.setattr(cockpit_engine, "SYMBOL_RESOLUTION_RETRY_DELAY_SECONDS", 0.01)
    messages: list[dict[str, object]] = []
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    try:
        engine.start(messages.append)
        _wait_until(
            lambda: any(
                item.subscription.get("schema") == "trades"
                for item in _FakeLive.instances
            )
        )
        assert calls == SYMBOL_RESOLUTION_ATTEMPTS
        assert any(
            message["type"] == "feed_status"
            and "retrying ES (2/2)" in message["payload"]["message"]
            for message in messages
        )
        assert _FakeLive.max_active == 2
    finally:
        engine.stop()


def test_focus_prefers_bounded_live_mapping_before_historical_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_live_dependencies(monkeypatch)
    monkeypatch.setattr(cockpit_engine, "FOCUS_MAPPING_WAIT_SECONDS", 0.25)
    monkeypatch.setattr(
        cockpit_engine,
        "resolve_single_instrument",
        lambda *_args, **_kwargs: pytest.fail("historical resolution was called"),
    )
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    binding = HistoryBinding("ES", "ESU6", 101)
    timer = threading.Timer(0.01, engine._remember_binding, args=(binding,))
    timer.start()
    try:
        assert engine._wait_for_live_binding("ES", engine.generation) == binding
    finally:
        timer.join()
        engine.stop()


def test_failed_focus_resolution_can_retry_same_market_without_raw_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_live_dependencies(monkeypatch)
    calls = 0

    def resolve_on_manual_retry(_historical, **kwargs):
        nonlocal calls
        calls += 1
        if calls <= SYMBOL_RESOLUTION_ATTEMPTS:
            raise TimeoutError("HTTPSConnectionPool(host='hist.databento.com'): secret detail")
        return SimpleNamespace(
            market=kwargs["market"], raw_symbol="ESU6", instrument_id=101
        )

    monkeypatch.setattr(cockpit_engine, "resolve_single_instrument", resolve_on_manual_retry)
    monkeypatch.setattr(cockpit_engine, "SYMBOL_RESOLUTION_RETRY_DELAY_SECONDS", 0.01)
    messages: list[dict[str, object]] = []
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    try:
        engine.start(messages.append)
        _wait_until(
            lambda: any(
                message["type"] == "feed_status"
                and message["payload"]["scope"] == "focus"
                and message["payload"]["state"] == "ERROR"
                for message in messages
            )
        )
        failure = [
            message["payload"]["message"]
            for message in messages
            if message["type"] == "feed_status"
            and message["payload"]["scope"] == "focus"
            and message["payload"]["state"] == "ERROR"
        ][-1]
        assert "select Retry" in failure
        assert "hist.databento.com" not in failure
        assert engine.select_market("ES") is True
        _wait_until(
            lambda: any(
                item.subscription.get("schema") == "trades"
                for item in _FakeLive.instances
            )
        )
        assert calls == SYMBOL_RESOLUTION_ATTEMPTS + 1
        assert engine.generation == 2
    finally:
        engine.stop()


def test_overview_failure_degrades_without_blocking_focus(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_live_dependencies(monkeypatch)
    _FakeLive.fail_overview = True
    messages: list[dict[str, object]] = []
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_FakeDb,
    )
    try:
        engine.start(messages.append)
        _wait_until(
            lambda: any(item.subscription.get("schema") == "trades" for item in _FakeLive.instances)
        )
        assert any(
            message["type"] == "feed_status"
            and message["payload"]["scope"] == "overview"
            and message["payload"]["state"] == "ERROR"
            for message in messages
        )
        assert any(item.subscription.get("schema") == "trades" for item in _FakeLive.instances)
    finally:
        engine.stop()


def test_authentication_failure_does_not_start_sessions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        cockpit_engine, "resolve_cockpit_api_key_source", lambda _env: None
    )
    messages: list[dict[str, object]] = []
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        env={},
        db_module=_FakeDb,
    )
    try:
        engine.start(messages.append)
        assert _FakeLive.instances == []
        assert messages[-1]["payload"]["state"] == "ERROR"
        assert "API key" in messages[-1]["payload"]["message"]
    finally:
        engine.stop()


def test_late_focus_generation_is_ignored(tmp_path: Path) -> None:
    engine = LiveCockpitEngine(cache_path=tmp_path / "bars.sqlite3", db_module=_FakeDb)
    try:
        engine.generation = 2
        record = SimpleNamespace(
            ts_event=datetime.now(timezone.utc),
            price=100_000_000_000,
            size=1,
        )
        engine._on_focus_record(
            record,
            generation=1,
            aggregator=TradeCandleAggregator(timeframe_seconds=60, timeframe="1m"),
        )
        assert engine._pending_update is None
    finally:
        engine.stop()


@pytest.mark.parametrize("record_kind", ["trade", "ohlcv"])
def test_focus_health_uses_observed_event_time_when_provider_clock_leads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_kind: str,
) -> None:
    event_time = datetime(2026, 7, 30, 18, 3, tzinfo=timezone.utc)
    monkeypatch.setattr(
        cockpit_engine,
        "now_utc",
        lambda: event_time - timedelta(milliseconds=100),
    )
    engine = LiveCockpitEngine(
        cache_path=tmp_path / "bars.sqlite3",
        db_module=_FakeDb,
    )
    messages: list[dict[str, object]] = []
    try:
        engine._publish = messages.append
        engine.generation = 1
        engine._contract = "ESU6"
        engine._resolved_instrument_id = 101
        if record_kind == "trade":
            record = SimpleNamespace(
                ts_event=event_time,
                price=100_000_000_000,
                size=1,
            )
        else:
            record = SimpleNamespace(
                ts_event=event_time,
                open=100_000_000_000,
                high=101_000_000_000,
                low=99_000_000_000,
                close=100_500_000_000,
                volume=6,
            )
        engine._on_focus_record(
            record,
            generation=1,
            aggregator=TradeCandleAggregator(
                timeframe_seconds=60,
                timeframe="1m",
            ),
        )
        health = [
            message["payload"]
            for message in messages
            if message["type"] == "data_health"
        ][-1]
        assert health["evaluated_at"] == int(event_time.timestamp())
        assert health["last_bar_time"] == int(event_time.timestamp())
    finally:
        engine.stop()


def test_focus_trades_are_coalesced_to_one_pending_visual_update(tmp_path: Path) -> None:
    engine = LiveCockpitEngine(cache_path=tmp_path / "bars.sqlite3", db_module=_FakeDb)
    messages: list[dict[str, object]] = []
    render_thread: threading.Thread | None = None
    try:
        engine._publish = messages.append
        engine.generation = 1
        engine._contract = "ESU6"
        engine._resolved_instrument_id = 101
        aggregator = TradeCandleAggregator(timeframe_seconds=60, timeframe="1m")
        minute = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        for index in range(20):
            engine._on_focus_record(
                SimpleNamespace(
                    ts_event=minute + timedelta(seconds=index),
                    price=100_000_000_000 + index * 10_000_000,
                    size=1,
                ),
                generation=1,
                aggregator=aggregator,
            )
        render_thread = threading.Thread(target=engine._render_worker, daemon=True)
        render_thread.start()
        _wait_until(lambda: any(message["type"] == "bar_update" for message in messages))
        time.sleep((1.0 / VISUAL_UPDATE_HZ[DEFAULT_VISUAL_UPDATE_MODE]) * 1.25)
        assert sum(message["type"] == "bar_update" for message in messages) == 1
        assert engine._raw_bars[-1]["volume"] == 20
    finally:
        engine.stop()
        if render_thread is not None:
            render_thread.join(timeout=1.0)


def test_overview_status_becomes_stale(tmp_path: Path) -> None:
    engine = LiveCockpitEngine(cache_path=tmp_path / "bars.sqlite3", env={})
    emitted: list[dict[str, object]] = []
    try:
        engine._publish = emitted.append
        now = datetime.now(timezone.utc)
        engine._overview_latest["ES"] = (6000.0, now - timedelta(seconds=151))
        engine._refresh_overview_staleness(now=now)
        assert emitted[-1]["type"] == "market_status"
        assert emitted[-1]["payload"]["state"] == "STALE"
    finally:
        engine.stop()


def test_overview_bars_are_batched_to_disk_and_reused_for_focus(tmp_path: Path) -> None:
    engine = LiveCockpitEngine(cache_path=tmp_path / "bars.sqlite3", env={})
    render_thread: threading.Thread | None = None
    minute = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    try:
        engine._on_overview_record(
            SimpleNamespace(
                stype_in_symbol="ES.v.0",
                stype_out_symbol="ESU6",
                instrument_id=101,
            )
        )
        engine._on_overview_record(
            SimpleNamespace(
                instrument_id=101,
                ts_event=minute,
                open=100_000_000_000,
                high=101_000_000_000,
                low=99_000_000_000,
                close=100_500_000_000,
                volume=42,
            )
        )
        render_thread = threading.Thread(target=engine._render_worker, daemon=True)
        render_thread.start()
        _wait_until(lambda: engine.runtime_metrics()["cache_writes"] == 1)
        assert engine.cache is not None
        cached = engine.cache.get_bars(
            dataset="GLBX.MDP3",
            instrument_id=101,
            start=minute - timedelta(minutes=1),
            end=minute + timedelta(minutes=1),
        )
        assert cached[-1]["close"] == 100.5
        assert engine._overview_latest_bars["ES"][1]["volume"] == 42
        assert engine._market_bindings["ES"] == HistoryBinding("ES", "ESU6", 101)
    finally:
        engine.stop()
        if render_thread is not None:
            render_thread.join(timeout=1.0)


def test_self_check_is_offline_and_verifies_assets_and_webview(tmp_path: Path) -> None:
    program_files = tmp_path / "program-files"
    (program_files / "Microsoft" / "EdgeWebView" / "Application" / "123.0").mkdir(parents=True)
    result = self_check(
        env={
            "LOCALAPPDATA": str(tmp_path / "local"),
            "PROGRAMFILES(X86)": str(program_files),
            "PROGRAMFILES": str(tmp_path / "program-files-64"),
        }
    )
    assert result["status"] == "PASS"
    assert result["provider_connection_opened"] is False
    assert result["market_count"] == 41
    assert result["alpha_tier_grouping_valid"] is True
    assert all(result["assets"].values())
    assert result["asset_launch_target_local"] is True


def test_cli_self_check_is_provider_free_and_passes(tmp_path: Path) -> None:
    executable = Path(sys.executable).with_name("futures-live-cockpit.exe")
    env = dict(os.environ)
    env["LOCALAPPDATA"] = str(tmp_path / "localappdata")
    result = subprocess.run(
        [str(executable), "--self-check"],
        cwd=Path.cwd(),
        env=env,
        check=False,
        capture_output=True,
        text=True,
        shell=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "PASS"
    assert payload["provider_connection_opened"] is False


def test_desktop_uses_local_server_instead_of_file_uri(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: dict[str, object] = {}

    class _ClosedEvent:
        def __iadd__(self, callback):
            calls["closed_callback"] = callback
            return self

    class _DesktopWindow(_FakeWindow):
        def __init__(self) -> None:
            super().__init__()
            self.events = SimpleNamespace(closed=_ClosedEvent())

    window = _DesktopWindow()

    def create_window(*args, **kwargs):
        calls["create_args"] = args
        calls["create_kwargs"] = kwargs
        return window

    def start(**kwargs):
        calls["start_kwargs"] = kwargs
        calls["browser_arguments"] = os.environ.get(
            cockpit_app.WEBVIEW2_BROWSER_ARGUMENTS_ENV
        )

    monkeypatch.setitem(
        cockpit_app.sys.modules,
        "webview",
        SimpleNamespace(create_window=create_window, start=start),
    )
    engine = _FakeEngine()

    assert cockpit_app.run_desktop(
        engine=engine,
        state_path=tmp_path / "state.json",
        demo=True,
    ) == 0

    create_kwargs = calls["create_kwargs"]
    target = create_kwargs["url"]
    assert target == desktop_asset_target(demo=True)
    assert target.endswith("index.html?mode=demo")
    assert not target.lower().startswith("file://")
    assert calls["start_kwargs"] == {
        "gui": "edgechromium",
        "debug": False,
        "http_server": True,
    }
    browser_arguments = calls["browser_arguments"]
    assert browser_arguments.startswith(
        cockpit_app.DEMO_WEBVIEW2_BACKGROUND_ARGUMENT
    )
    assert "--proxy-server=http://127.0.0.1:" in browser_arguments
    assert cockpit_app.WEBVIEW2_BROWSER_ARGUMENTS_ENV not in os.environ
    assert engine.stopped is True


def test_demo_webview2_offline_environment_restores_inherited_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(cockpit_app.WEBVIEW2_BROWSER_ARGUMENTS_ENV, "--inherited")

    with cockpit_app.demo_webview2_offline_environment(demo=True):
        assert (
            os.environ[cockpit_app.WEBVIEW2_BROWSER_ARGUMENTS_ENV]
            .startswith(cockpit_app.DEMO_WEBVIEW2_BACKGROUND_ARGUMENT)
        )

    assert (
        os.environ[cockpit_app.WEBVIEW2_BROWSER_ARGUMENTS_ENV] == "--inherited"
    )


def test_normal_webview_environment_is_not_changed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(cockpit_app.WEBVIEW2_BROWSER_ARGUMENTS_ENV, "--normal-mode")

    with cockpit_app.demo_webview2_offline_environment(demo=False):
        assert (
            os.environ[cockpit_app.WEBVIEW2_BROWSER_ARGUMENTS_ENV]
            == "--normal-mode"
        )

    assert os.environ[cockpit_app.WEBVIEW2_BROWSER_ARGUMENTS_ENV] == "--normal-mode"


def test_self_check_inspects_installed_credentials_by_existence_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    program_files = tmp_path / "program-files"
    (program_files / "Microsoft" / "EdgeWebView" / "Application" / "123.0").mkdir(
        parents=True
    )
    locator_path = tmp_path / "credential-source.json"
    locator_path.write_bytes(b"intentionally invalid and unreadable as JSON")
    original_read_text = Path.read_text

    def reject_locator_content_read(path: Path, *args, **kwargs):
        if path == locator_path:
            raise AssertionError("self-check must not read credential locator contents")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(
        cockpit_app,
        "default_credential_locator_path",
        lambda: locator_path,
    )
    monkeypatch.setattr(
        cockpit_app,
        "default_repository_package_api_env_path",
        lambda: None,
    )
    monkeypatch.setattr(Path, "read_text", reject_locator_content_read)

    result = self_check(
        env={
            "LOCALAPPDATA": str(tmp_path / "local"),
            "PROGRAMFILES(X86)": str(program_files),
            "PROGRAMFILES": str(tmp_path / "program-files-64"),
        }
    )

    assert result["status"] == "PASS"
    assert result["credential_check_mode"] == "existence_only"
    assert result["credential_source_present"] is True
    assert result["api_key_configured"] is None
    assert result["credential_locator_present"] is True
    assert result["credential_locator_valid"] is None
    assert result["credential_error"] is None
    assert result["provider_connection_opened"] is False


def test_self_check_returns_bounded_failure_when_state_probe_is_denied(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    program_files = tmp_path / "program-files"
    (program_files / "Microsoft" / "EdgeWebView" / "Application" / "123.0").mkdir(
        parents=True
    )
    original_open = Path.open

    def deny_probe(path: Path, *args, **kwargs):
        if path.name.startswith("self-check-"):
            raise PermissionError("synthetic state directory denial")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", deny_probe)
    result = self_check(
        env={
            "LOCALAPPDATA": str(tmp_path / "local"),
            "PROGRAMFILES(X86)": str(program_files),
            "PROGRAMFILES": str(tmp_path / "program-files-64"),
        }
    )

    assert result["status"] == "FAIL"
    assert result["cache_writeable"] is False
    assert result["cache_error"] == "synthetic state directory denial"
    assert result["provider_connection_opened"] is False


def test_packaged_windowed_self_check_uses_exit_code_without_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cockpit_app, "self_check", lambda: {"status": "PASS"})
    monkeypatch.setattr(cockpit_app.sys, "stdout", None)

    assert cockpit_app.main(["--self-check"]) == 0


def test_local_chart_time_formatting_handles_pacific_time_and_dst() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is required for deterministic frontend formatter validation")
    script = r"""
const fs = require('fs');
global.window = {};
eval(fs.readFileSync(process.argv[1], 'utf8'));
const time = window.CockpitTime;
const values = {
  observedTick: time.formatLocalTickMark(Date.parse('2026-07-14T05:23:00Z') / 1000, 3, 'en-US'),
  observedCrosshair: time.formatLocalCrosshairTime(Date.parse('2026-07-14T05:23:00Z') / 1000, 'en-US'),
  beforeFallback: time.formatLocalCrosshairTime(Date.parse('2026-11-01T08:30:00Z') / 1000, 'en-US'),
  afterFallback: time.formatLocalCrosshairTime(Date.parse('2026-11-01T09:30:00Z') / 1000, 'en-US'),
};
process.stdout.write(JSON.stringify(values));
"""
    env = dict(os.environ)
    env["TZ"] = "America/Los_Angeles"
    result = subprocess.run(
        [node, "-e", script, str(assets_dir() / "time-format.js")],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
        env=env,
    )
    values = json.loads(result.stdout)
    assert values["observedTick"] == "22:23"
    assert "Jul 13, 2026" in values["observedCrosshair"]
    assert "22:23" in values["observedCrosshair"]
    assert values["observedCrosshair"].endswith("PDT")
    assert "01:30" in values["beforeFallback"]
    assert values["beforeFallback"].endswith("PDT")
    assert "01:30" in values["afterFallback"]
    assert values["afterFallback"].endswith("PST")


def test_frontend_is_local_attributed_and_bounded() -> None:
    html = (assets_dir() / "index.html").read_text(encoding="utf-8")
    javascript = (assets_dir() / "app.js").read_text(encoding="utf-8")
    stylesheet = (assets_dir() / "styles.css").read_text(encoding="utf-8")
    time_format = (assets_dir() / "time-format.js").read_text(encoding="utf-8")
    vendor = (assets_dir() / "lightweight-charts.standalone.production.js").read_text(
        encoding="utf-8"
    )
    assert "https://www.tradingview.com/" in html
    assert "https://unpkg.com" not in html
    assert "window.cockpit" in javascript
    assert 'window.addEventListener("pywebviewready"' in javascript
    assert 'document.addEventListener("pywebviewready"' not in javascript
    assert "window.pywebview.api.poll_events(POLL_EVENT_LIMIT)" in javascript
    assert "retrySelectedMarket" in javascript
    assert 'id="retry-focus"' in html
    assert 'id="retry-history"' in html
    assert 'id="history-cache-toggle"' in html
    assert 'id="history-cache-popover"' in html
    assert 'id="history-cache-confirm"' in html
    assert 'id="fullscreen-toggle"' in html
    assert "Reset view" in html
    assert "Full screen" in html
    assert 'class="chart-stage"' in html
    assert 'id="session-boundaries"' in html
    assert 'id="prediction-rail"' in html
    assert 'id="data-health-pill"' in html
    assert 'id="quote-open"' in html
    assert 'id="quote-high"' in html
    assert 'id="quote-low"' in html
    assert 'id="quote-close"' in html
    assert 'id="quote-volume"' in html
    assert "updateQuote" in javascript
    assert ".quote-strip" in stylesheet
    assert "elements.instrumentSymbol.textContent = state.selectedMarket" in javascript
    assert 'elements.instrumentMeta.textContent = "Continuous front contract' in javascript
    assert "const exactSymbolQuery" in javascript
    assert "if (exactSymbolQuery) return symbol === query" in javascript
    assert 'id="layers-menu"' in html
    assert 'id="group-by-sector"' in html
    assert 'id="group-by-alpha"' in html
    assert "Tier 1 · Core" in javascript
    assert "tier_2_additions" in javascript
    assert "tier_3_additions" in javascript
    assert "Alt+ArrowUp Alt+ArrowDown" in javascript
    assert "dragstart" in javascript
    assert "collapsed_alpha_tier_groups" in javascript
    assert "Clear search to reorder groups" in javascript
    assert "Chart smoothness" in html
    assert "High smoothness" in html
    assert "observePollHealth" in javascript
    assert "set_visual_update_active(active)" in javascript
    assert "RECOVERY_WINDOW_MS = 5000" in javascript
    assert "Expected move" in html
    assert "Bias confidence" in html
    assert "Target window" in html
    assert "Target by" in html
    assert "Direction entropy" not in html
    assert 'id="prediction-entropy"' not in html
    assert "Display only &mdash; no orders" in html
    assert "Market watch" in html
    assert "confirm_history_cache(planId)" in javascript
    assert "set_history_cache_paused(paused)" in javascript
    assert "retry_history_cache_estimate()" in javascript
    assert "toggle_fullscreen()" in javascript
    assert "resetChartView" in javascript
    assert "scheduleChartReset" in javascript
    assert "followLatestBar" in javascript
    assert "renderSessionBoundaries" in javascript
    assert "autoSize: false" in javascript
    assert "new ResizeObserver(queueChartResize)" in javascript
    assert "state.chart.resize(width, height, true)" in javascript
    assert "window.setTimeout(settleFullscreenChartLayout, 160)" in javascript
    assert "state.chart.timeScale().fitContent()" in javascript
    assert 'label.textContent = kind === "rth" ? "RTH open" : "Globex open"' in javascript
    assert "createSeriesMarkers(state.candleSeries, [])" in javascript
    assert "renderPredictionMarker" in javascript
    assert "expectedReturn * referenceClose" in javascript
    assert "formatted} pts" in javascript
    assert "expectedReturn * 10000" not in javascript
    assert "applyPrediction" in javascript
    assert "applyDataHealth" in javascript
    assert "payload?.contract === state.contract" in javascript
    assert "Number(payload?.generation) === state.generation" in javascript
    assert 'message.type === "prediction_update"' in javascript
    assert 'message.type === "data_health"' in javascript
    assert "MODEL_NOT_AUTHORIZED" in javascript
    assert "SYNTHETIC DEMO" in javascript
    assert 'ES: { direction: "LONG", probabilities: { long: 0.64, flat: 0.21, short: 0.15 }' in javascript
    assert 'NQ: { direction: "SHORT", probabilities: { long: 0.20, flat: 0.18, short: 0.62 }' in javascript
    assert 'RTY: ["ABSTAIN", "MODEL_ABSTAINED"]' in javascript
    assert 'YM: ["WARMING_UP", "FEATURE_WARMUP_INCOMPLETE"]' in javascript
    assert 'CL: ["STALE", "DATA_STALE"]' in javascript
    assert 'NG: ["ERROR", "SYNTHETIC_DEMO_ERROR"]' in javascript
    assert "window.innerWidth >= 1440" in javascript
    assert "set_ui_preferences(state.uiPreferences)" in javascript
    assert "scrollToRealTime()" in javascript
    assert '"timeframe-cache": "Cached + live"' in javascript
    assert "sourceLabel" in javascript
    assert "base: 0" in javascript
    assert "scaleMargins: { top: 0.16, bottom: 0 }" in javascript
    assert 'message.type === "history_cache_status"' in javascript
    assert 'state.source = "switching"' in javascript
    assert "const hasVisibleChart = state.barCount > 0" in javascript
    assert "Loading ${market} · showing ${previousContract || previousMarket}" in javascript
    assert '"selection-cache": "Cached bars"' in javascript
    assert 'state.source = "live-only"' in javascript
    assert 'elements.chartEmpty.classList.add("hidden")' in javascript
    assert "localization" in javascript
    assert "tickMarkFormatter" in javascript
    assert "formatLocalCrosshairTime" in time_format
    assert "formatLocalTickMark" in time_format
    assert "Computer local time" in html
    assert "state.browserDemo = false" in javascript
    assert "Market-data startup exceeded 60 seconds" in javascript
    assert "Market view unavailable" in javascript
    assert ".chart-stage" in stylesheet
    assert ".session-boundary-layer" in stylesheet
    assert ".prediction-rail" in stylesheet
    assert "grid-template-columns: minmax(0, 1fr) 304px" in stylesheet
    assert ".workspace-content.panel-collapsed" in stylesheet
    assert ".data-health-pill" in stylesheet
    assert ".history-cache-popover" in stylesheet
    assert ".control-button" in stylesheet
    assert "v5.1.0" in vendor
    assert ".market-grouping" in stylesheet
    assert ".market-group-drag" in stylesheet
    spec = Path(
        "FuturesLiveCockpit/_internal/FuturesLiveCockpit.spec"
    ).read_text(encoding="utf-8")
    assert "'configs' / 'alpha_tiered.yaml'" in spec
    assert MAX_RENDER_HZ == 15.0


def test_candidate_installer_retains_rollback_until_external_validation() -> None:
    installer = Path("scripts/install_live_cockpit_candidate.ps1").read_text(
        encoding="utf-8"
    )

    assert "INSTALLED_AND_OFFLINE_VALIDATED_ROLLBACK_RETAINED" in installer
    assert "rollback_path = $backupRoot" in installer
    assert "rollback_retained = (Test-Path -LiteralPath $backupRoot)" in installer
    assert "Remove-Item -LiteralPath $backupRoot -Recurse -Force" not in installer
    assert "Get-TreeFingerprint -Root $candidateRoot" in installer
    assert "Get-TreeFingerprint -Root $publishRoot" in installer
    assert "Get-TreeFingerprint -Root $backupRoot" in installer
    assert "Rollback tree verification failed before candidate installation" in installer
    assert "Installed tree differs from the validated candidate tree" in installer
    assert "Tree fingerprint root is link-like" in installer
    assert "Tree fingerprint contains a link-like entry" in installer
    assert "[int]$SelfCheckTimeoutSeconds = 60" in installer
    assert "$selfCheck.WaitForExit($SelfCheckTimeoutSeconds * 1000)" in installer
    assert "self-check exceeded $SelfCheckTimeoutSeconds seconds" in installer
    assert "@('/PID', [string]$selfCheck.Id, '/T', '/F')" in installer
