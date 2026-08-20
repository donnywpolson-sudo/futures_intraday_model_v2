from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import futures_rebuild.live_cockpit.engine as cockpit_engine
import futures_rebuild.live_cockpit.smoke as cockpit_smoke
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.live_cockpit.approval import (
    APPROVAL_SCHEMA,
    OPERATION,
    RESULT_OUTPUT_RELATIVE,
    LiveSmokeApprovalError,
    build_live_smoke_plan,
)
from futures_rebuild.live_cockpit.engine import LiveCockpitEngine, provider_control_message
from futures_rebuild.live_cockpit.smoke import SmokeResult, execute_approved_smoke, run_smoke


_SMOKE_MINUTE = datetime.now(timezone.utc).replace(second=0, microsecond=0)


class _ForbiddenTimeseries:
    def get_range(self, **_kwargs):
        raise AssertionError("historical replay is forbidden in smoke mode")


class _Historical:
    def __init__(self, **_kwargs) -> None:
        self.timeseries = _ForbiddenTimeseries()


class SymbolMappingMsg:
    stype_in_symbol = "ES.v.0"
    instrument_id = 101


class Ohlcv1M:
    instrument_id = 101
    ts_event = _SMOKE_MINUTE
    open = 6_000_000_000_000
    high = 6_001_000_000_000
    low = 5_999_000_000_000
    close = 6_000_500_000_000
    volume = 100


class TradeMsg:
    ts_event = _SMOKE_MINUTE + timedelta(seconds=30)
    price = 6_000_500_000_000
    size = 2


class ErrorMsg:
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.err = message


class SystemMsg:
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.msg = message


class _Live:
    instances: list["_Live"] = []
    control_record: object | None = None
    emit_data = True

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.subscription: dict[str, object] = {}
        self.subscriptions: list[dict[str, object]] = []
        self.callback = None
        self.exception_callback = None
        self.stopped = False
        self.__class__.instances.append(self)

    def subscribe(self, **kwargs) -> None:
        self.subscription = kwargs
        self.subscriptions.append(kwargs)

    def add_callback(self, callback, exception_callback=None) -> None:
        self.callback = callback
        self.exception_callback = exception_callback

    def start(self) -> None:
        if self.control_record is not None:
            self.callback(self.control_record)
            return
        if not self.emit_data:
            return
        if self.subscription["schema"] == "ohlcv-1m":
            self.callback(SymbolMappingMsg())
            self.callback(Ohlcv1M())
        else:
            self.callback(TradeMsg())

    def stop(self) -> None:
        self.stopped = True

    def block_for_close(self, **_kwargs) -> None:
        return None


class _Db:
    Historical = _Historical
    Live = _Live


@pytest.fixture(autouse=True)
def _reset_live(monkeypatch: pytest.MonkeyPatch) -> None:
    _Live.instances = []
    _Live.control_record = None
    _Live.emit_data = True
    monkeypatch.setattr(cockpit_engine, "FOCUS_MAPPING_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cockpit_engine, "FOCUS_SWITCH_DEBOUNCE_SECONDS", 0.0)
    monkeypatch.setattr(cockpit_engine, "RENDER_INTERVAL_SECONDS_OVERRIDE", 0.01)
    monkeypatch.setattr(
        cockpit_engine,
        "resolve_single_instrument",
        lambda _historical, **_kwargs: SimpleNamespace(
            market="ES", raw_symbol="ESU6", instrument_id=101
        ),
    )


def test_smoke_passes_with_exact_two_sessions_and_no_history_or_cache(
    tmp_path: Path,
) -> None:
    result = run_smoke(
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_Db,
        duration_seconds=0.55,
        temp_root=tmp_path,
        poll_seconds=0.01,
    )

    assert result.status == "PASS"
    assert result.exit_code == 0
    assert len(_Live.instances) == 2
    assert _Live.instances[0].subscription == {
        "dataset": "GLBX.MDP3",
        "schema": "ohlcv-1m",
        "symbols": _Live.instances[0].subscription["symbols"],
        "stype_in": "continuous",
    }
    assert len(_Live.instances[0].subscription["symbols"]) == 41
    assert set(_Live.instances[0].subscription["symbols"]) == {
        f"{info.symbol}.v.0" for info in cockpit_engine.chart_market_universe()
    }
    assert _Live.instances[1].subscription == {
        "dataset": "GLBX.MDP3",
        "schema": "trades",
        "symbols": 101,
        "stype_in": "instrument_id",
    }
    assert len(_Live.instances[1].subscriptions) == 1
    assert all(item.kwargs["reconnect_policy"] == "none" for item in _Live.instances)
    assert all(item.stopped for item in _Live.instances)
    assert result.summary["resolved_contract"] == "ESU6"
    assert result.summary["metrics_after_stop"]["live_sessions_started"] == 2
    assert result.summary["metrics_after_stop"]["max_live_sessions"] == 2
    assert result.summary["metrics_after_stop"]["active_live_sessions"] == 0
    assert result.summary["metrics_after_stop"]["history_requests"] == 0
    assert result.summary["metrics_after_stop"]["cache_reads"] == 0
    assert result.summary["metrics_after_stop"]["cache_writes"] == 0
    assert not list((tmp_path / "FuturesLiveCockpit").glob("*.jsonl"))
    assert not list(tmp_path.rglob("*.sqlite3"))


def test_default_smoke_preserves_file_aware_credential_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    resolver_calls: list[object] = []

    def resolver(env):
        resolver_calls.append(env)
        return SimpleNamespace(key="db-file-test", source="file api.env")

    monkeypatch.setattr(cockpit_smoke, "resolve_cockpit_api_key_source", resolver)
    monkeypatch.setattr(cockpit_engine, "resolve_cockpit_api_key_source", resolver)

    result = run_smoke(
        env=None,
        db_module=_Db,
        duration_seconds=0.55,
        temp_root=tmp_path,
        poll_seconds=0.01,
    )

    assert result.status == "PASS"
    assert resolver_calls == [None, None]


def test_explicit_invalid_locator_fails_before_provider_or_environment_fallback(
    tmp_path: Path,
) -> None:
    locator = tmp_path / "credential-source.json"
    locator.write_text("not-json", encoding="utf-8")

    with pytest.raises(
        LiveSmokeApprovalError,
        match="approved credential locator could not resolve DATABENTO_API_KEY",
    ):
        run_smoke(
            env={"DATABENTO_API_KEY": "db-stale-environment-key"},
            db_module=_Db,
            duration_seconds=0.01,
            temp_root=tmp_path,
            approval_receipt_id="approved",
            locator_path=locator,
        )
    assert _Live.instances == []


@pytest.mark.parametrize("code", range(1, 8))
def test_all_databento_error_codes_fail_fast_and_retain_redacted_log(
    tmp_path: Path, code: int
) -> None:
    secret = "db-test-secret"
    _Live.control_record = ErrorMsg(code, f"provider rejected {secret}")

    result = run_smoke(
        env={"DATABENTO_API_KEY": secret},
        db_module=_Db,
        duration_seconds=1.0,
        temp_root=tmp_path,
        poll_seconds=0.01,
    )

    assert result.status == "FAIL"
    assert result.exit_code == 1
    assert result.summary["elapsed_seconds"] < 1.0
    log_path = Path(result.summary["log_path"])
    assert log_path.is_file()
    log_text = log_path.read_text(encoding="utf-8")
    assert secret not in log_text
    assert "[REDACTED]" in log_text


def test_slow_reader_system_message_fails_fast(tmp_path: Path) -> None:
    _Live.control_record = SystemMsg(2, "consumer is too slow")

    result = run_smoke(
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_Db,
        duration_seconds=1.0,
        temp_root=tmp_path,
        poll_seconds=0.01,
    )

    assert result.status == "FAIL"
    assert "SLOW_READER_WARNING" in result.summary["reasons"]


def test_healthy_sessions_without_records_are_inconclusive(tmp_path: Path) -> None:
    _Live.emit_data = False

    result = run_smoke(
        env={"DATABENTO_API_KEY": "db-test"},
        db_module=_Db,
        duration_seconds=0.35,
        temp_root=tmp_path,
        poll_seconds=0.01,
    )

    assert result.status == "INCONCLUSIVE_NO_DATA"
    assert result.exit_code == 3
    assert result.summary["missing_data"] == [
        "overview_market_updates",
        "focus_live_events",
        "bar_updates",
    ]
    assert Path(result.summary["log_path"]).is_file()


def test_provider_control_parser_distinguishes_error_and_system_code_two() -> None:
    error = provider_control_message(ErrorMsg(2, "key deactivated"))
    system = provider_control_message(SystemMsg(2, "slow reader"))

    assert error is not None
    assert error["provider_name"] == "API_KEY_DEACTIVATED"
    assert system is not None
    assert system["provider_name"] == "SLOW_READER_WARNING"


def test_late_generation_stays_ignored_with_cache_disabled() -> None:
    engine = LiveCockpitEngine(
        cache_path=None,
        cache_enabled=False,
        history_enabled=False,
        reconnect_enabled=False,
        db_module=_Db,
    )
    try:
        engine.generation = 2
        engine._on_focus_record(
            TradeMsg(),
            generation=1,
            aggregator=cockpit_engine.TradeCandleAggregator(
                timeframe_seconds=60, timeframe="1m"
            ),
        )
        assert engine._pending_update is None
    finally:
        engine.stop()


def _approved_execution_files(
    tmp_path: Path, executable: Path
) -> tuple[Path, Path, Path]:
    plan = build_live_smoke_plan(
        sha256_file(executable),
        source_revision="b" * 40,
        package_inputs=[
            {"path": "src/example.py", "bytes": 7, "sha256": "c" * 64},
        ],
    )
    plan_path = tmp_path / "live-smoke-plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    locator = tmp_path / "credential-source.json"
    locator.write_text("{}", encoding="utf-8")
    core = {
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "approved_at": "2026-08-12T09:00:00Z",
        "user_authorization_id": "8" * 64,
        "credential_locator_path": str(locator.resolve()),
        "credential_locator_sha256": sha256_file(locator),
    }
    approval = {**core, "approval_receipt_id": sha256_json(core)}
    approval_path = tmp_path / "live-smoke-approval.json"
    approval_path.write_bytes(canonical_bytes(approval) + b"\n")
    return plan_path, approval_path, locator


def test_approved_frozen_entrypoint_writes_one_create_only_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "FuturesLiveCockpit.exe"
    executable.write_bytes(b"frozen successor")
    plan_path, approval_path, locator = _approved_execution_files(
        tmp_path, executable
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cockpit_smoke.sys, "frozen", True, raising=False)
    monkeypatch.setattr(cockpit_smoke.sys, "executable", str(executable))
    monkeypatch.setattr(
        cockpit_smoke,
        "run_smoke",
        lambda **kwargs: SmokeResult(
            status="PASS",
            exit_code=0,
            summary={"approval_receipt_id": kwargs["approval_receipt_id"]},
        ),
    )
    result_output = tmp_path / RESULT_OUTPUT_RELATIVE

    assert execute_approved_smoke(
        plan_path=plan_path,
        approval_path=approval_path,
        credential_locator=locator,
        result_output=result_output,
    ) == 0
    payload = json.loads(result_output.read_text(encoding="utf-8"))
    assert payload["status"] == "PASS"
    assert payload["result_output_relative"] == RESULT_OUTPUT_RELATIVE
    assert payload["summary"]["runtime"] == {
        "frozen": True,
        "executable_sha256": sha256_file(executable),
    }
    assert payload["result_id"] == sha256_json(
        {key: value for key, value in payload.items() if key != "result_id"}
    )


def test_approved_frozen_entrypoint_refuses_existing_result_before_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = tmp_path / "FuturesLiveCockpit.exe"
    executable.write_bytes(b"frozen successor")
    plan_path, approval_path, locator = _approved_execution_files(
        tmp_path, executable
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cockpit_smoke.sys, "frozen", True, raising=False)
    monkeypatch.setattr(cockpit_smoke.sys, "executable", str(executable))
    monkeypatch.setattr(
        cockpit_smoke,
        "run_smoke",
        lambda **_kwargs: pytest.fail("provider smoke was called"),
    )
    result_output = tmp_path / RESULT_OUTPUT_RELATIVE
    result_output.parent.mkdir(parents=True)
    result_output.write_text("preserve", encoding="utf-8")

    with pytest.raises(LiveSmokeApprovalError, match="result already exists"):
        execute_approved_smoke(
            plan_path=plan_path,
            approval_path=approval_path,
            credential_locator=locator,
            result_output=result_output,
        )
    assert result_output.read_text(encoding="utf-8") == "preserve"
