from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import futures_rebuild.alpha_ladder_es_pilot_execution as pilot
from futures_rebuild.alpha_ladder_limit_readiness import CT, LimitBar
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]


def _bar(
    session: str,
    clock: time,
    *,
    open_price: Decimal,
    high: Decimal | None = None,
    low: Decimal | None = None,
    close: Decimal | None = None,
    available_delay: timedelta = timedelta(minutes=1, seconds=5),
) -> LimitBar:
    event = datetime.combine(date.fromisoformat(session), clock, CT)
    close = open_price if close is None else close
    return LimitBar(
        event_at=event,
        available_at=event + available_delay,
        identity="synthetic-es-contract",
        open=open_price,
        high=open_price + Decimal("0.50") if high is None else high,
        low=open_price - Decimal("0.50") if low is None else low,
        close=close,
        volume=Decimal("100"),
        tick_size=Decimal("0.25"),
        tick_value=Decimal("12.50"),
    )


def _session_bars(
    session: str,
    *,
    exit_open: Decimal = Decimal("101"),
    stop_on_exit: bool = False,
    late_feature_bar: bool = False,
) -> tuple[LimitBar, ...]:
    start = datetime.combine(date.fromisoformat(session), time(9, 29), CT)
    bars: list[LimitBar] = []
    for offset in range(82):
        event = start + timedelta(minutes=offset)
        price = Decimal("100") + Decimal((offset % 3) - 1) * Decimal("0.05")
        delay = timedelta(minutes=1, seconds=5)
        if late_feature_bar and event.time() == time(9, 59):
            delay = timedelta(minutes=1, seconds=6)
        bars.append(
            _bar(
                session,
                event.time(),
                open_price=price,
                close=price,
                available_delay=delay,
            )
        )
    # Trigger at 10:00 is known at 10:01:05.  The first eligible entry bar
    # starts at 10:02 and its interval terminalizes at 10:03.
    trigger_index = next(index for index, bar in enumerate(bars) if bar.event_at.time() == time(10, 0))
    trigger = bars[trigger_index]
    bars[trigger_index] = _bar(
        session,
        time(10, 0),
        open_price=Decimal("100"),
        close=Decimal("100"),
    )
    entry_index = next(index for index, bar in enumerate(bars) if bar.event_at.time() == time(10, 2))
    bars[entry_index] = _bar(
        session,
        time(10, 2),
        open_price=Decimal("100"),
        high=Decimal("100.50"),
        low=Decimal("99.50"),
        close=Decimal("100"),
    )
    exit_index = next(index for index, bar in enumerate(bars) if bar.event_at.time() == time(10, 34))
    bars[exit_index] = _bar(
        session,
        time(10, 34),
        open_price=exit_open,
        high=exit_open + Decimal("0.25"),
        low=Decimal("97") if stop_on_exit else exit_open - Decimal("0.25"),
        close=exit_open,
    )
    assert trigger.event_at == bars[trigger_index].event_at
    return tuple(bars)


def _mechanism() -> dict[str, object]:
    return json.loads((ROOT / pilot.MECHANISM_PATH).read_text(encoding="utf-8"))


def _weekdays(start: date, count: int) -> list[str]:
    result: list[str] = []
    current = start
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _gate_strategies(*, candidate_net: str = "100", trades: int = 8) -> dict[str, object]:
    coverage = {"expected_sessions": 63, "terminal_sessions": 63, "complete": True}
    candidate = {
        "stress": {
            "metrics": {
                "trade_count": trades,
                "net_pnl_usd": candidate_net,
                "maximum_continuous_drawdown_usd": "500",
                "coverage": coverage,
            }
        }
    }
    baselines = {
        name: {
            "stress": {
                "metrics": {
                    "net_pnl_usd": "0",
                    "trade_count": 0,
                    "maximum_continuous_drawdown_usd": "0",
                    "coverage": coverage,
                }
            }
        }
        for name in pilot.MANDATORY_BASELINES
    }
    return {"candidate": candidate, **baselines}


def test_plan_is_immutable_source_safe_and_has_no_direct_execution_cli(monkeypatch) -> None:
    original = pilot.sha256_file

    def reject_parquet(path: Path, *args, **kwargs):
        if path.suffix == ".parquet":
            raise AssertionError("plan validation opened a protected Parquet file")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(pilot, "sha256_file", reject_parquet)
    plan = pilot.load_plan(root=ROOT, verify_protected=False)
    assert plan["trial_id"] == pilot.TRIAL_ID
    assert list(plan["source_bindings"]) == [
        f"data/active/causally_gated_normalized/ES/{year}/{year}.parquet"
        for year in (2018, 2019, 2020)
    ]
    assert plan["authority"]["attempts"] == 1
    assert plan["authority"]["retries"] == 0
    assert not hasattr(pilot, "main")
    assert "alpha_ladder_es_pilot_execution" not in (
        ROOT / "pyproject.toml"
    ).read_text(encoding="utf-8").split("[project.scripts]", 1)[1].split("[", 1)[0]


def test_plan_or_gateway_binding_substitution_fails_closed() -> None:
    plan = pilot.load_plan(root=ROOT)
    changed = json.loads(json.dumps(plan))
    changed["trial_id"] = "f" * 64
    with pytest.raises(IntegrityError, match="plan changed"):
        pilot.validate_plan(changed, root=ROOT)
    head = "a" * 40
    scope = pilot.additional_execution_scope(root=ROOT, plan=plan, pushed_git_head=head)
    assert scope["execution_plan_id"] == plan["plan_id"]
    assert scope["execution_plan_sha256"] == sha256_file(ROOT / pilot.PLAN_PATH)
    assert scope["execution_output_root"] == pilot.OUTPUT_ROOT.as_posix()


def test_feature_and_entry_timing_are_causal() -> None:
    session = "2020-01-14"
    bars = _session_bars(session)
    context = pilot.compute_feature_context(session=session, bars=bars)
    assert context.bars[-1].event_at.time() == time(9, 59)
    assert context.bars[-1].available_at.time() == time(10, 0, 5)
    result = pilot.simulate_direction(
        context=context,
        bars=bars,
        direction="LONG",
        scenario="stress",
        mechanism=_mechanism(),
    )
    assert result.filled
    assert result.entry_at is not None and result.entry_at.time() == time(10, 3)
    assert result.entry_at > datetime.combine(date.fromisoformat(session), time(10, 0, 5), CT)


def test_late_feature_bar_is_excluded_not_leaked() -> None:
    session = "2020-01-14"
    context = pilot.compute_feature_context(
        session=session, bars=_session_bars(session, late_feature_bar=True)
    )
    assert context.bars[-1].event_at.time() == time(9, 58)
    assert all(bar.available_at <= datetime.combine(date.fromisoformat(session), time(10, 0, 5), CT) for bar in context.bars)


def test_ordered_argmax_tie_and_inclusive_hurdle_are_exact() -> None:
    assert pilot.select_candidate({"LONG": 0.25, "SHORT": 0.25}) == (
        "LONG",
        0.25,
        True,
    )
    assert pilot.select_candidate({"LONG": 0.249999, "SHORT": 0.10}) == (
        "LONG",
        0.249999,
        False,
    )


def test_stress_costs_and_reported_trade_exit_arithmetic_reconcile() -> None:
    session = "2020-01-14"
    bars = _session_bars(session, exit_open=Decimal("101"))
    context = pilot.compute_feature_context(session=session, bars=bars)
    result = pilot.simulate_direction(
        context=context,
        bars=bars,
        direction="LONG",
        scenario="stress",
        mechanism=_mechanism(),
    )
    assert result.disposition == "VERIFIED_CAUSAL_REPORTED_TRADE_EXIT_PROXY"
    assert result.gross_pnl_usd == Decimal("50")
    assert result.fees_usd == Decimal("10")
    assert result.slippage_usd == Decimal("50")
    assert result.net_pnl_usd == Decimal("-10")
    assert result.net_r == result.net_pnl_usd / result.planned_loss_usd


def test_protective_stop_precedes_scheduled_exit_on_same_bar() -> None:
    session = "2020-01-14"
    bars = _session_bars(session, exit_open=Decimal("100"), stop_on_exit=True)
    context = pilot.compute_feature_context(session=session, bars=bars)
    result = pilot.simulate_direction(
        context=context,
        bars=bars,
        direction="LONG",
        scenario="stress",
        mechanism=_mechanism(),
    )
    assert result.disposition == "VERIFIED_PROTECTIVE_STOP"
    assert result.exit_price == result.stop_price


def test_training_only_standardization_ignores_unrequested_sessions() -> None:
    sessions = _weekdays(date(2018, 1, 2), 14)
    selected = sessions[:12]
    bars = {
        session: _session_bars(
            session, exit_open=Decimal("99.50") + Decimal(index % 6) * Decimal("0.50")
        )
        for index, session in enumerate(sessions)
    }
    first, _ = pilot.fit_models(
        training_sessions=selected,
        bars_by_session=bars,
        mechanism=_mechanism(),
    )
    bars[sessions[-1]] = _session_bars(sessions[-1], exit_open=Decimal("110"))
    second, _ = pilot.fit_models(
        training_sessions=selected,
        bars_by_session=bars,
        mechanism=_mechanism(),
    )
    assert first == second
    assert first.target_counts["LONG"] == len(selected)
    assert first.target_counts["SHORT"] == len(selected)


def test_missing_session_fails_as_data_coverage_contradiction() -> None:
    with pytest.raises(pilot.DataCoverageError, match="21 causal feature bars"):
        pilot.compute_feature_context(session="2020-01-14", bars=())


def test_drawdown_guard_blocks_only_later_entries() -> None:
    sessions = ("2020-01-14", "2020-01-15")
    first = pilot.PathResult(
        "VERIFIED_PROTECTIVE_STOP",
        True,
        True,
        "LONG",
        "stress",
        entry_at=datetime(2020, 1, 14, 10, 3, tzinfo=CT),
        exit_at=datetime(2020, 1, 14, 10, 4, tzinfo=CT),
        entry_price=Decimal("100"),
        exit_price=Decimal("90"),
        net_pnl_usd=Decimal("-1600"),
        mark_net_pnls=(Decimal("-1600"),),
    )
    second = pilot.PathResult(
        "VERIFIED_CAUSAL_REPORTED_TRADE_EXIT_PROXY",
        True,
        True,
        "LONG",
        "stress",
        entry_at=datetime(2020, 1, 15, 10, 3, tzinfo=CT),
        exit_at=datetime(2020, 1, 15, 10, 34, tzinfo=CT),
        entry_price=Decimal("100"),
        exit_price=Decimal("101"),
        net_pnl_usd=Decimal("10"),
        mark_net_pnls=(Decimal("10"),),
    )
    account = pilot._account_path(sessions=sessions, actions={sessions[0]: first, sessions[1]: second})
    assert len(account["admitted"]) == 1
    assert account["daily"][1]["disposition"] == "DRAWDOWN_BLOCKED"
    assert account["maximum_continuous_drawdown_usd"] == "1600"


def test_independent_baselines_own_complete_63_session_accounts() -> None:
    all_sessions = _weekdays(date(2018, 1, 2), 75)
    training, evaluation = all_sessions[:12], all_sessions[12:]
    bars = {
        session: _session_bars(
            session,
            exit_open=Decimal("99.50") + Decimal(index % 7) * Decimal("0.50"),
        )
        for index, session in enumerate(all_sessions)
    }
    plan = {
        "session_scope": {
            "training_session_ids": training,
            "evaluation_session_ids": evaluation,
        }
    }
    result = pilot.evaluate_loaded_rows(plan=plan, mechanism=_mechanism(), bars_by_session=bars)
    assert len(result["predictions"]) == 63
    flat = result["strategies"]["flat_no_trade"]["stress"]
    assert flat["metrics"]["trade_count"] == 0
    assert flat["metrics"]["net_pnl_usd"] == "0"
    for name in pilot.MANDATORY_BASELINES:
        account = result["strategies"][name]["stress"]["account"]
        assert len(account["daily"]) == 63
        assert account is not result["strategies"]["candidate"]["stress"]["account"]


def test_pilot_gate_reconstructs_pass_and_fail_without_significance_claim() -> None:
    passing = _gate_strategies()
    assert pilot.classify_pilot_gate(passing) == ("PASS", [])
    failing = _gate_strategies(trades=7)
    decision, failed = pilot.classify_pilot_gate(failing)
    assert decision == "FAIL"
    assert failed == ["MINIMUM_EIGHT_TRADES"]
    baseline_failure = _gate_strategies()
    baseline_failure["risk_matched_always_long"]["stress"]["metrics"]["net_pnl_usd"] = "100"
    assert "BEAT_BASELINE__risk_matched_always_long" in pilot.classify_pilot_gate(baseline_failure)[1]


def test_create_only_writer_and_terminal_order_fail_closed(tmp_path) -> None:
    path = tmp_path / "artifact.json"
    payload = {"schema_version": "test/1.0.0", "value": 1}
    pilot._write_exclusive(path, payload)
    assert path.read_bytes() == canonical_bytes(payload) + b"\n"
    with pytest.raises(FileExistsError):
        pilot._write_exclusive(path, payload)
    source = pilot._sealed_outputs(
        root=ROOT,
        plan={"plan_id": "a" * 64, "source_bindings": {}},
        receipt=type("Receipt", (), {"receipt_id": "b" * 64})(),
        use_path=Path("state/authorization_uses/use.json"),
        source_audit={},
        evaluation={
            "model": {},
            "predictions": [],
            "strategies": {"candidate": {}},
            "decision": "FAIL",
            "failed_gates": ["TEST"],
        },
    )
    assert source[-1][0] == "pilot_decision.json"
    assert source[-2][0] == "terminal_report.json"


def test_plan_validation_refuses_existing_output_or_failure_root(tmp_path, monkeypatch) -> None:
    plan = pilot.load_plan(root=ROOT)
    monkeypatch.setattr(pilot, "build_plan", lambda root: plan)
    changed = json.loads(json.dumps(plan))
    changed["authority"]["output_root"] = "already"
    changed_core = {key: value for key, value in changed.items() if key != "plan_id"}
    changed["plan_id"] = pilot.sha256_json(changed_core)
    monkeypatch.setattr(pilot, "build_plan", lambda root: changed)
    (tmp_path / "already").mkdir()
    with pytest.raises(UnauthorizedOperation, match="output_root already exists"):
        pilot.validate_plan(changed, root=tmp_path)
