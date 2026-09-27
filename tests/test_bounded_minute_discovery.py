import numpy as np
import pytest
from datetime import date, timedelta

from futures_rebuild import bounded_minute_discovery as discovery
from futures_rebuild import rolling_hour_preparation as preparation
from futures_rebuild import rolling_hour_research as minute
from futures_rebuild import profit_aligned_research as profit
from futures_rebuild.canonical import sha256_json
from types import SimpleNamespace
from futures_rebuild.errors import IntegrityError

pytestmark = pytest.mark.current


def test_search_is_bounded_and_has_both_model_families():
    spec = discovery.specification()
    assert len(discovery.candidates()) == 90
    assert spec["configuration_charges"] == 360
    assert spec["minimum_reserved_fits"] == 11552
    assert {c["horizon_minutes"] for c in spec["candidates"]} == {15, 30, 60}
    assert {c["model_family"] for c in spec["candidates"]} == {"ridge", "xgboost"}
    assert not any(spec["authority"].values())


def test_expansion_uses_fit_rows_and_rejects_redundant_columns():
    rng = np.random.default_rng(74012)
    x = rng.normal(size=(120, len(discovery.FEATURE_NAMES)))
    y = x[:, discovery.FEATURE_NAMES.index("close_change_ticks_60")].copy()
    duplicate = discovery.FEATURE_NAMES.index("close_change_volatility_ticks_60")
    x[:, duplicate] = y
    fit = discovery.selected_features(x[:100], y[:100], added_count=4)
    assert "close_change_ticks_60" in fit
    assert "close_change_volatility_ticks_60" not in fit
    # The caller exposes only training rows; changes to held-out rows cannot alter selection.
    x[100:] *= 1000
    y[100:] *= -1000
    assert discovery.selected_features(x[:100], y[:100], added_count=4) == fit
    with pytest.raises(IntegrityError):
        discovery.selected_features(x, y, added_count=100)


def test_all_targets_use_one_causal_complete_path():
    session, rows, _ = preparation.synthetic_session(minutes=190)
    scheduled = minute.opportunity_grid(session)[0]
    path = rows[62:123]
    kwargs = {"actual_contract": rows[0]["actual_contract"]}
    assert discovery.fixed_horizon_targets(scheduled, path,
        as_of_ns=scheduled["decision_ns"], **kwargs) is None
    targets = discovery.fixed_horizon_targets(scheduled, path,
        as_of_ns=rows[-1]["decision_eligible_at_ns"], **kwargs)
    assert set(targets) == {15, 30, 60}
    assert targets[15]["exit_ns"] == scheduled["entry_ns"] + 15 * minute.MINUTE
    assert targets[60]["terminal_change_ticks"] == minute.build_outcome(
        path, scheduled, expected_contract=rows[0]["actual_contract"],
        as_of_ns=rows[-1]["decision_eligible_at_ns"])["fit_target"]
    with pytest.raises(IntegrityError):
        discovery.fixed_horizon_targets(scheduled, path[:-1],
            as_of_ns=rows[-1]["decision_eligible_at_ns"], **kwargs)


def test_price_proxy_replay_enforces_flat_only_and_zero_trade_dates():
    session, rows, _ = preparation.synthetic_session(minutes=190)
    grid = minute.opportunity_grid(session)[:20]
    by_start = {row["bar_start_ns"]: i for i, row in enumerate(rows)}
    opportunities = []
    for scheduled in grid:
        first = by_start[scheduled["entry_ns"]]
        targets = discovery.fixed_horizon_targets(scheduled, rows[first:first + 61],
            actual_contract=rows[0]["actual_contract"],
            as_of_ns=rows[-1]["decision_eligible_at_ns"])
        opportunities.append({**{key: scheduled[key] for key in
            ("market", "trade_date", "opportunity_id", "decision_ns", "entry_ns",
             "open_interval_start_ns", "open_interval_end_ns")},
            **targets[15], "horizon_minutes": 15, "tick_value_usd": 12.5,
            "actual_contract": rows[0]["actual_contract"]})
    another_day = "2020-01-02"
    result = discovery.proxy_replay([1000.] * len(opportunities), opportunities,
        [session.trade_date.isoformat(), another_day])
    assert result["state"] == "DESCRIPTIVE_PRICE_PROXY_ONLY"
    assert result["daily_trade_counts"][session.trade_date.isoformat()] == 2
    assert result["daily_stress_proxy_usd"][another_day] == "0"
    assert not any(result["authority"].values())
    with pytest.raises(IntegrityError):
        discovery.proxy_replay([1000.] * len(opportunities),
            [{**opportunities[0], "target_available_at_ns": 0}, *opportunities[1:]],
            [session.trade_date.isoformat(), another_day])


def test_complete_price_session_adapts_existing_minute_mechanics():
    session, rows, _ = preparation.synthetic_session(minutes=190)
    adapted = discovery.assemble_price_session(session, rows,
        as_of_ns=rows[-1]["decision_eligible_at_ns"])
    assert adapted["state"] == "SOURCE_ROWS_NOT_CERTIFIED"
    assert len(adapted["sample_ids"]) == len(minute.opportunity_grid(session))
    assert len(adapted["features"][0]) == len(discovery.FEATURE_NAMES)
    assert all(len(adapted["targets"][h]) == len(adapted["sample_ids"])
               for h in discovery.HORIZONS)
    result = discovery.proxy_replay([1000.] * len(adapted["sample_ids"]),
        adapted["opportunities"][15], [session.trade_date.isoformat()])
    assert result["state"] == "DESCRIPTIVE_PRICE_PROXY_ONLY"
    next_session, next_rows, _ = preparation.synthetic_session(day="2018-01-02", minutes=190)
    next_adapted = discovery.assemble_price_session(next_session, next_rows,
        as_of_ns=next_rows[-1]["decision_eligible_at_ns"])
    combined = discovery.combine_price_sessions([adapted, next_adapted],
        ["2018-01-01", "2018-01-02"])
    assert len(combined["sample_ids"]) == 2 * len(adapted["sample_ids"])
    with pytest.raises(IntegrityError):
        discovery.combine_price_sessions([adapted], ["2018-01-01", "2018-01-02"])


def test_preflight_fails_closed_without_quotes_and_scientific_evidence():
    owner = {"minimum_daily_net_usd": 10, "maximum_drawdown_usd": 1500,
             "maximum_attempts": 1, "maximum_fits": 10000,
             "maximum_seconds": 3600, "maximum_memory_bytes": 2**30,
             "maximum_output_bytes": 2**28, "maximum_additional_cost_usd": 0}
    report = discovery.preflight(owner, {})
    assert not report["reported_prerequisites_complete"]
    assert "certified_development_ohlcv" in report["blocks"]
    assert "bounded_resource_pilot" in report["blocks"]
    assert "FIT_BUDGET_BELOW_DECLARED_SEARCH" in report["blocks"]
    assert not any(report["authority"].values())
    with pytest.raises(IntegrityError):
        discovery.preflight({**owner, "minimum_daily_net_usd": 0}, {})


def test_nested_price_proxy_search_reselects_on_inner_folds(monkeypatch):
    one = next(c for c in discovery.candidates()
               if c["horizon_minutes"] == 15 and c["feature_procedure"] == "baseline"
               and c["model_family"] == "ridge" and c["model_parameter"] == 1.)
    monkeypatch.setattr(discovery, "candidates", lambda: (one,))
    dates = [(date(2018, 1, 1) + timedelta(days=i)).isoformat() for i in range(1010)]
    ids = [f"id-{i:04d}" for i in range(len(dates))]
    x = np.zeros((len(dates), len(discovery.FEATURE_NAMES)))
    x[:, discovery.FEATURE_NAMES.index(discovery.CORE_FEATURES[0])] = np.sin(np.arange(len(dates)))
    labels = {h: 20 * x[:, discovery.FEATURE_NAMES.index(discovery.CORE_FEATURES[0])]
              for h in discovery.HORIZONS}
    rows = {}
    for horizon in discovery.HORIZONS:
        rows[horizon] = []
        for i, day in enumerate(dates):
            decision_ns = (1_600_000_000 + i * 86400) * 10**9 + 5_000_000_000
            entry_ns = (decision_ns // discovery.minute.MINUTE + 1) * discovery.minute.MINUTE
            exit_ns = entry_ns + horizon * discovery.minute.MINUTE
            rows[horizon].append({"market": "ES", "trade_date": day,
                "opportunity_id": ids[i], "decision_ns": decision_ns,
                "entry_ns": entry_ns, "exit_ns": exit_ns,
                "open_interval_start_ns": entry_ns - 62 * discovery.minute.MINUTE,
                "open_interval_end_ns": exit_ns + discovery.minute.MINUTE,
                "actual_contract": "ESZ20",
                "horizon_minutes": horizon, "target_available_at_ns": exit_ns + discovery.minute.MINUTE + 5_000_000_000,
                "tick_value_usd": 12.5, "terminal_change_ticks": float(labels[horizon][i])})
    data = {"market": "ES", "calendar_dates": dates, "trade_dates": dates,
            "sample_ids": ids, "features": x, "targets": labels,
            "opportunities": rows}
    result = discovery.run_market_search(data, maximum_fits=100, maximum_seconds=60)
    assert result["state"] == "COMPLETE_DESCRIPTIVE_ONLY"
    assert len(result["outer_folds"]) == 8
    assert result["fits_charged"] == 40
    assert result["final_candidate_id"] == one["candidate_id"]
    assert all(len(fold["candidate_scores"]) == 1 for fold in result["outer_folds"])
    assert not any(result["authority"].values())


def test_real_price_adapter_requires_certified_point_in_time_bindings(monkeypatch):
    session, rows, _ = preparation.synthetic_session(minutes=125)
    contract_id = sha256_json({"price_adapter": "contract"})
    release_id = sha256_json({"price_adapter": "release"})
    for row in rows:
        row.update(source_contract_id=contract_id, source_release_id=release_id)
    class GeneratedCalendar:
        receipt = SimpleNamespace(release_id=sha256_json({"price_adapter": "calendar"}))
        sessions = {(session.market, session.trade_date): session}
        def verify(self):
            pass
    monkeypatch.setattr(minute, "VerifiedExchangeCalendar", GeneratedCalendar)
    calendar = GeneratedCalendar()
    day = session.trade_date.isoformat()
    binding = profit.bind({"market": session.market, "trade_date": day,
        "source_contract_id": contract_id, "source_release_id": release_id,
        "calendar_release_id": calendar.receipt.release_id,
        "calendar_evidence_id": "e" * 64, "contract_selection_evidence_id": "f" * 64,
        "source_certificate_id": "c" * 64, "actual_contract": rows[0]["actual_contract"],
        "calendar_known_at_ns": rows[0]["bar_start_ns"] - 1,
        "contract_selection_known_at_ns": rows[0]["bar_start_ns"] - 1,
        "point_in_time_evidence": "CERTIFIED_ORIGINAL_PUBLICATION",
        "source_file_sha256s": sorted(set(row["source_file_sha256"] for row in rows))}, "binding_id")
    args = {"sessions": (session,), "rows_by_date": {day: rows}, "calendar": calendar,
        "bindings": {day: binding}, "source_contract_id": contract_id,
        "source_release_id": release_id, "as_of_ns": rows[-1]["decision_eligible_at_ns"]}
    adapted = discovery.adapt_supplied_price_sessions(**args)
    assert len(adapted["sample_ids"]) == 3
    altered = dict(binding); altered.pop("binding_id")
    altered["contract_selection_known_at_ns"] = rows[-1]["bar_start_ns"]
    with pytest.raises(IntegrityError, match="known before open"):
        discovery.adapt_supplied_price_sessions(**{**args,
            "bindings": {day: profit.bind(altered, "binding_id")}})
