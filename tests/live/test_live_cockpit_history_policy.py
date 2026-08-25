from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import futures_rebuild.live_cockpit.app as cockpit_app
from futures_rebuild.live_cockpit.app import CockpitController, load_state, save_state
from futures_rebuild.live_cockpit.engine import LiveCockpitEngine
from futures_rebuild.live_cockpit.feed import chart_market_universe
from futures_rebuild.live_cockpit.protocol import PROTOCOL_VERSION, event
from futures_rebuild.live_cockpit.single_instance import SingleInstance


FINGERPRINT = "a" * 64


def test_live_engine_supports_an_explicit_one_market_canary_universe() -> None:
    es = next(info for info in chart_market_universe() if info.symbol == "ES")
    engine = LiveCockpitEngine(
        cache_path=None,
        cache_enabled=False,
        history_enabled=False,
        reconnect_enabled=False,
        markets=(es,),
    )

    assert [info.symbol for info in engine.markets] == ["ES"]


class _PolicyEngine:
    def __init__(self, *, retry_accepted: bool = True) -> None:
        self.confirmed: list[str] = []
        self.retry_accepted = retry_accepted
        self.retry_count = 0
        self.cancel_count = 0

    def confirm_history_cache(self, plan_id: str) -> bool:
        self.confirmed.append(plan_id)
        return True

    def retry_history_cache_estimate(self) -> bool:
        self.retry_count += 1
        return self.retry_accepted

    def cancel_history_cache_after_current_request(self) -> bool:
        self.cancel_count += 1
        return True


def _plan_status(
    *,
    estimate: object = 0.05,
    plan_id: str = "plan-1",
    range_key: str = "1W",
) -> dict[str, object]:
    return {
        "state": "CONFIRMATION_REQUIRED",
        "message": "A history update is ready for review",
        "plan_id": plan_id,
        "plan_fingerprint": FINGERPRINT,
        "estimated_cost_usd": estimate,
        "estimate_expires_at": int(
            (datetime.now(timezone.utc) + timedelta(minutes=10)).timestamp()
        ),
        "ready_markets": 0,
        "total_markets": 41,
        "queued_markets": 41,
        "paused": False,
        "range_key": range_key,
    }


def _cache_status(
    state: str,
    *,
    message: str,
    failure_category: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "state": state,
        "message": message,
        "ready_markets": 41 if state == "COMPLETE" else 0,
        "total_markets": 41,
        "queued_markets": 0,
        "paused": False,
    }
    if failure_category is not None:
        payload["failure_category"] = failure_category
    return payload


def _stored_policy(path: Path) -> dict[str, object]:
    return load_state(path)["history_update_policy"]


def test_policy_defaults_version_mismatch_and_malformed_state_fail_closed() -> None:
    default = cockpit_app.sanitize_history_update_policy(None)
    assert default["mode"] == "UNDECIDED"
    assert default["auto_blocked"] is False

    stale = cockpit_app.sanitize_history_update_policy(
        {**default, "policy_version": cockpit_app.HISTORY_POLICY_VERSION - 1}
    )
    assert stale["mode"] == "UNDECIDED"
    assert stale["last_auto_attempt_at"] is None

    malformed = cockpit_app.sanitize_history_update_policy(
        {**default, "mode": "AUTO", "auto_blocked": "false"}
    )
    assert malformed["mode"] == "MANUAL"
    assert malformed["auto_blocked"] is True
    assert malformed["block_reason"] == "MALFORMED_POLICY"


@pytest.mark.parametrize(
    ("estimate", "eligible", "reason"),
    [
        ("0.0499", True, None),
        ("0.0500", True, None),
        ("0.0501", False, "ABOVE_CAP"),
        (None, False, "ESTIMATE_INVALID"),
        ("-0.01", False, "ESTIMATE_INVALID"),
        ("NaN", False, "ESTIMATE_INVALID"),
        ("Infinity", False, "ESTIMATE_INVALID"),
        ("not-money", False, "ESTIMATE_INVALID"),
    ],
)
def test_automatic_cost_gate_uses_decimal_and_fails_closed(
    estimate: object,
    eligible: bool,
    reason: str | None,
) -> None:
    policy = cockpit_app.default_history_update_policy()
    policy["mode"] = "AUTO"
    result, result_reason, parsed = cockpit_app._automatic_history_eligibility(
        policy,
        _plan_status(estimate=estimate),
        now=datetime.now(timezone.utc),
    )
    assert result is eligible
    assert result_reason == reason
    if eligible:
        assert parsed == Decimal(str(estimate))


@pytest.mark.parametrize("range_key", ["1D", "1W"])
def test_automatic_small_repairs_allow_current_contract_ranges(range_key: str) -> None:
    policy = cockpit_app.default_history_update_policy()
    policy["mode"] = "AUTO"
    assert cockpit_app._automatic_history_eligibility(
        policy,
        _plan_status(range_key=range_key),
        now=datetime.now(timezone.utc),
    )[:2] == (True, None)


def test_positive_cost_one_month_history_requires_manual_review() -> None:
    policy = cockpit_app.default_history_update_policy()
    policy["mode"] = "AUTO"
    assert cockpit_app._automatic_history_eligibility(
        policy,
        _plan_status(range_key="1M"),
        now=datetime.now(timezone.utc),
    )[:2] == (False, "EXTENDED_RANGE_REQUIRES_REVIEW")


@pytest.mark.parametrize("range_key", ["1D", "1W", "1M"])
def test_exact_zero_cost_automatically_allows_every_supported_range(
    range_key: str,
) -> None:
    now = datetime.now(timezone.utc)
    policy = cockpit_app.default_history_update_policy()
    policy.update(
        {
            "mode": "AUTO",
            "auto_blocked": True,
            "block_reason": "AUTO_TIMEOUT",
            "last_auto_attempt_at": cockpit_app._utc_text(now),
            "last_auto_outcome": "ERROR",
        }
    )
    assert cockpit_app._automatic_history_eligibility(
        policy,
        _plan_status(estimate="0.0000", range_key=range_key),
        now=now,
    ) == (True, None, Decimal("0.0000"))


def test_zero_cost_does_not_bypass_manual_mode_user_cancel_or_invalid_plan() -> None:
    now = datetime.now(timezone.utc)
    manual = cockpit_app.default_history_update_policy()
    manual["mode"] = "MANUAL"
    assert cockpit_app._automatic_history_eligibility(
        manual,
        _plan_status(estimate=0, range_key="1M"),
        now=now,
    )[:2] == (False, "AUTOMATIC_OFF")

    canceled = cockpit_app.default_history_update_policy()
    canceled.update(
        {"mode": "AUTO", "auto_blocked": True, "block_reason": "USER_DISABLED"}
    )
    assert cockpit_app._automatic_history_eligibility(
        canceled,
        _plan_status(estimate=0, range_key="1M"),
        now=now,
    )[:2] == (False, "USER_DISABLED")

    expired = {
        **_plan_status(estimate=0, range_key="1M"),
        "estimate_expires_at": int(now.timestamp()) - 1,
    }
    automatic = cockpit_app.default_history_update_policy()
    automatic["mode"] = "AUTO"
    assert cockpit_app._automatic_history_eligibility(
        automatic,
        expired,
        now=now,
    )[:2] == (False, "PLAN_INVALID")


def test_zero_cost_plan_is_reserved_and_confirmed_exactly_once(tmp_path: Path) -> None:
    engine = _PolicyEngine()
    state_path = tmp_path / "state.json"
    policy = cockpit_app.default_history_update_policy()
    policy["mode"] = "AUTO"
    save_state(state_path, {"history_update_policy": policy})
    controller = CockpitController(engine, state_path=state_path)
    message = event(
        "history_cache_status",
        _plan_status(estimate=0.0, range_key="1M"),
    )

    controller.publish(message)
    controller.publish(message)

    assert engine.confirmed == ["plan-1"]
    status = controller.poll_events()[-1]["payload"]
    assert status["update_origin"] == "AUTO"


@pytest.mark.parametrize("removed_range", ["2W", "3M"])
def test_removed_persisted_ranges_migrate_to_one_week(removed_range: str) -> None:
    assert cockpit_app.normalize_persisted_chart_range(removed_range) == "1W"


def test_automatic_attempt_interval_is_one_rolling_24_hour_window() -> None:
    now = datetime.now(timezone.utc)
    policy = cockpit_app.default_history_update_policy()
    policy["mode"] = "AUTO"
    policy["last_auto_attempt_at"] = cockpit_app._utc_text(
        now - timedelta(hours=23, minutes=59)
    )
    assert cockpit_app._automatic_history_eligibility(
        policy, _plan_status(), now=now
    )[:2] == (False, "RECENT_ATTEMPT")

    policy["last_auto_attempt_at"] = cockpit_app._utc_text(
        now - timedelta(hours=24)
    )
    assert cockpit_app._automatic_history_eligibility(
        policy, _plan_status(), now=now
    )[:2] == (True, None)


def test_first_launch_and_manual_mode_never_auto_confirm(tmp_path: Path) -> None:
    engine = _PolicyEngine()
    state_path = tmp_path / "state.json"
    controller = CockpitController(engine, state_path=state_path)

    controller.publish(event("history_cache_status", _plan_status()))
    assert engine.confirmed == []
    assert controller.set_history_update_mode("MANUAL")["ok"] is True
    controller.publish(event("history_cache_status", _plan_status(plan_id="plan-2")))

    assert engine.confirmed == []
    assert _stored_policy(state_path)["mode"] == "MANUAL"


def test_auto_mode_records_once_before_confirmation_and_restart_blocks_retry(
    tmp_path: Path,
) -> None:
    engine = _PolicyEngine()
    state_path = tmp_path / "state.json"
    controller = CockpitController(engine, state_path=state_path)
    controller.publish(event("history_cache_status", _plan_status()))

    assert controller.set_history_update_mode("AUTO")["ok"] is True
    assert engine.confirmed == ["plan-1"]
    started = _stored_policy(state_path)
    assert started["last_auto_outcome"] == "STARTED"
    assert started["last_auto_plan_fingerprint"] == FINGERPRINT
    assert started["last_auto_attempt_at"] is not None

    controller.publish(event("history_cache_status", _plan_status(plan_id="plan-2")))
    assert engine.confirmed == ["plan-1"]

    CockpitController(_PolicyEngine(), state_path=state_path)
    recovered = _stored_policy(state_path)
    assert recovered["last_auto_outcome"] == "INTERRUPTED"
    assert recovered["auto_blocked"] is True
    assert recovered["block_reason"] == "INTERRUPTED"


def test_concurrent_auto_confirmation_reserves_only_one_attempt(tmp_path: Path) -> None:
    engine = _PolicyEngine()
    state_path = tmp_path / "state.json"
    policy = cockpit_app.default_history_update_policy()
    policy["mode"] = "AUTO"
    save_state(state_path, {"history_update_policy": policy})
    controller = CockpitController(engine, state_path=state_path)
    message = event("history_cache_status", _plan_status())
    barrier = threading.Barrier(3)

    def publish() -> None:
        barrier.wait()
        controller.publish(message)

    first = threading.Thread(target=publish)
    second = threading.Thread(target=publish)
    first.start()
    second.start()
    barrier.wait()
    first.join()
    second.join()

    assert engine.confirmed == ["plan-1"]
    assert _stored_policy(state_path)["last_auto_outcome"] == "STARTED"


def test_auto_failure_blocks_restart_and_manual_success_clears_block(tmp_path: Path) -> None:
    engine = _PolicyEngine()
    state_path = tmp_path / "state.json"
    controller = CockpitController(engine, state_path=state_path)
    controller.publish(event("history_cache_status", _plan_status()))
    controller.set_history_update_mode("AUTO")
    controller.publish(
        event(
            "history_cache_status",
            _cache_status("ERROR", failure_category="TIMEOUT", message="failed"),
        )
    )

    failed = _stored_policy(state_path)
    assert failed["last_auto_outcome"] == "ERROR"
    assert failed["auto_blocked"] is True
    assert failed["block_reason"] == "AUTO_TIMEOUT"
    assert CockpitController(_PolicyEngine(), state_path=state_path).retry_automatic_history()[
        "error"
    ] == "RECENT_ATTEMPT"

    controller.confirm_history_cache("manual-plan")
    controller.publish(
        event("history_cache_status", _cache_status("COMPLETE", message="ready"))
    )
    recovered = _stored_policy(state_path)
    assert recovered["auto_blocked"] is False
    assert recovered["block_reason"] is None


def test_unreviewed_startup_complete_does_not_clear_an_automatic_block(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    policy = cockpit_app.default_history_update_policy()
    policy.update(
        {
            "mode": "AUTO",
            "auto_blocked": True,
            "block_reason": "AUTO_TIMEOUT",
            "last_auto_outcome": "ERROR",
        }
    )
    save_state(state_path, {"history_update_policy": policy})
    controller = CockpitController(_PolicyEngine(), state_path=state_path)

    controller.publish(
        event("history_cache_status", _cache_status("COMPLETE", message="ready"))
    )

    assert _stored_policy(state_path)["auto_blocked"] is True


def test_reviewed_retry_obeys_time_limit_and_reblocks_if_plan_cannot_start(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    policy = cockpit_app.default_history_update_policy()
    policy.update(
        {
            "mode": "AUTO",
            "auto_blocked": True,
            "block_reason": "AUTO_TIMEOUT",
            "last_auto_outcome": "ERROR",
            "last_auto_attempt_at": cockpit_app._utc_text(
                datetime.now(timezone.utc) - timedelta(hours=25)
            ),
        }
    )
    save_state(state_path, {"history_update_policy": policy})
    engine = _PolicyEngine(retry_accepted=False)
    controller = CockpitController(engine, state_path=state_path)

    result = controller.retry_automatic_history()

    assert result["ok"] is False
    assert engine.retry_count == 1
    blocked = _stored_policy(state_path)
    assert blocked["auto_blocked"] is True
    assert blocked["block_reason"] == "AUTO_RETRY_NOT_STARTED"


def test_disabling_auto_during_active_request_stops_later_chunks(tmp_path: Path) -> None:
    engine = _PolicyEngine()
    state_path = tmp_path / "state.json"
    controller = CockpitController(engine, state_path=state_path)
    controller.publish(event("history_cache_status", _plan_status()))
    controller.set_history_update_mode("AUTO")

    result = controller.set_history_update_mode("MANUAL")

    assert result["ok"] is True
    assert engine.cancel_count == 1
    policy = _stored_policy(state_path)
    assert policy["mode"] == "MANUAL"
    assert policy["auto_blocked"] is True
    assert policy["block_reason"] == "USER_DISABLED"


def test_concurrent_preference_updates_preserve_history_safety_state(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    controller = CockpitController(_PolicyEngine(), state_path=state_path)
    policy = cockpit_app.default_history_update_policy()
    policy.update(
        {
            "mode": "AUTO",
            "auto_blocked": True,
            "block_reason": "AUTO_TIMEOUT",
            "last_auto_outcome": "ERROR",
        }
    )
    controller._write_history_policy(policy)
    barrier = threading.Barrier(3)

    def write_preferences(preferences: dict[str, object]) -> None:
        barrier.wait()
        for _ in range(20):
            controller.set_ui_preferences(preferences)

    first = threading.Thread(target=write_preferences, args=({"show_volume": False},))
    second = threading.Thread(
        target=write_preferences, args=({"show_predictions": True},)
    )
    first.start()
    second.start()
    barrier.wait()
    first.join()
    second.join()

    state = load_state(state_path)
    assert state["history_update_policy"]["auto_blocked"] is True
    assert state["history_update_policy"]["block_reason"] == "AUTO_TIMEOUT"
    assert state["ui_preferences"] == {"show_predictions": True}


def test_second_cockpit_instance_cannot_reach_history_startup(tmp_path: Path) -> None:
    lock_path = tmp_path / "cockpit-instance.lock"
    first = SingleInstance(lock_path)
    second = SingleInstance(lock_path)
    try:
        assert first.acquire() is True
        assert second.acquire() is False
    finally:
        second.release()
        first.release()


def test_demo_mode_uses_isolated_state_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: dict[str, object] = {}
    monkeypatch.setattr(cockpit_app, "app_data_dir", lambda: tmp_path)

    def fake_run_desktop(*, engine: object, state_path: Path, demo: bool) -> int:
        calls.update({"engine": engine, "state_path": state_path, "demo": demo})
        return 0

    monkeypatch.setattr(cockpit_app, "run_desktop", fake_run_desktop)

    assert cockpit_app.main(["--demo"]) == 0
    assert calls["state_path"] == tmp_path / "demo" / cockpit_app.STATE_FILENAME
    assert calls["demo"] is True
    assert not (tmp_path / cockpit_app.STATE_FILENAME).exists()
    assert (tmp_path / "demo" / cockpit_app.STATE_FILENAME).is_file()


def test_frontend_has_five_deterministic_simple_states_and_no_encoding_defect() -> None:
    asset_root = Path(cockpit_app.assets_dir())
    javascript = (asset_root / "app.js").read_text(encoding="utf-8")
    html = (asset_root / "index.html").read_text(encoding="utf-8")

    for scenario in ("consent", "updating", "ready", "review", "failure"):
        assert f'"{scenario}"' in javascript
    for label in (
        "History ready",
        "History updating",
        "History incomplete",
        "History review needed",
    ):
        assert label in javascript
    assert "`Feed ${simpleState}`" in javascript
    assert '`Analysis ${analysisReady ? "ready" : "paused"}`' in javascript
    assert "Repair small gaps automatically" in html
    assert "Always ask me" in html
    assert 'id="history-cache-affected"' in html
    assert 'id="history-cache-interval"' in html
    assert "total_markets: 5" in javascript
    assert "Deterministic 41-market demo" in javascript
    assert "33-market demo" not in javascript
    assert "Only one automatic repair attempt is allowed every 24 hours" in javascript
    assert "No download started" in javascript
    assert PROTOCOL_VERSION == 3
    assert "const PROTOCOL_VERSION = 3" in javascript
    assert "Â" not in javascript
    assert "Â" not in html
