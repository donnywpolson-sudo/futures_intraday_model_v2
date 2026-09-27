"""Synthetic accounting fixtures only: no model fit, replay, or research evidence."""
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from futures_rebuild import l0_alpha_reporting as r
from futures_rebuild.canonical import sha256_json, write_new_canonical_authority
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.l0_alpha_report_demo import fixture_context, fixture_replay, fixture_inference, generate

pytestmark = pytest.mark.current


def pair():
    return (r.candidate_report(fixture_context("incumbent"), fixture_replay([-4, 5, 3, 0, 8, -6, -5, -3])),
            r.candidate_report(fixture_context("candidate"), fixture_replay([-3, 8, 4, 0, 10, -5, -6, -4])))


def test_paired_calendar_valid_zeros_and_hand_calculated_difference():
    a, b = pair()
    comparison = r.compare_candidates(a, b)
    assert comparison["status"] == "DESCRIPTIVE_COMPLETE"
    assert comparison["statistics"] == {"incumbent_mean_daily_usd": -.25, "candidate_mean_daily_usd": .5,
        "mean_daily_difference_usd": .75, "cumulative_difference_usd": 6., "observation_count": 8}
    assert comparison["daily_differences"][3]["difference_usd"] == 0
    assert comparison["attribution"] == "MODEL_ONLY"
    assert len(comparison["fold_results"]) == 2
    assert comparison["claims"]["better_than_incumbent_on_this_sample"] is True
    assert comparison["claims"]["independently_supported_improvement"]["value"] is None
    assert not comparison["confirmatory_eligible"] and not comparison["winner_selection_eligible"]
    assert comparison["uncertainty"]["availability"] == "UNAVAILABLE"


@pytest.mark.parametrize("damage", ["absent", "unresolved", "legacy_complete_unknown", "bad_cost", "nan", "extra_date"])
def test_missing_accounting_never_becomes_zero_or_full_run(damage):
    context = fixture_context()
    replay = fixture_replay([1] * 8)
    day = context["scope"]["dates"][3]
    if damage == "absent":
        replay["daily"].pop(day)
    elif damage == "unresolved":
        replay["unresolved_position"] = {"trade_date": day, "reason": "MISSING_EXIT_REFERENCE", "prior_state": "OPEN"}
    elif damage == "legacy_complete_unknown":
        replay["daily"][day].pop("complete")
    elif damage == "bad_cost":
        replay["daily"][day]["cost_usd"] = 100
    elif damage == "nan":
        replay["daily"][day]["net_usd"] = float("nan")
    else:
        replay["daily"]["2018-01-12"] = replay["daily"][day]
    report = r.candidate_report(context, replay)
    assert report["execution"]["status"] == "COMPLETE"  # Process completion is not accounting eligibility.
    assert report["execution"]["accounting_status"] == "INCOMPLETE_OR_UNAVAILABLE"
    assert report["metrics"]["net_usd"]["value"] is None
    assert report["drawdown"]["value"] is None
    result = r.compare_candidates(pair()[0], report)
    assert result["status"] == "INELIGIBLE"
    assert result["statistics"]["mean_daily_difference_usd"] is None
    assert not result["confirmatory_eligible"]
    if damage == "unresolved":
        assert report["coverage"]["available_dates"] == 3
    elif damage == "extra_date":
        assert report["coverage"]["excluded_dates"] == [{"date": "2018-01-12", "reason": "OUTSIDE_DECLARED_EVALUATION_CALENDAR"}]
    else:
        assert result["coverage"]["matched_dates"] == 7
        assert result["partial_diagnostic"]["observation_count"] == 7


@pytest.mark.parametrize("field,value", [("calendar_id", "another"), ("session_boundaries", {"start": "00:00", "end": "13:00"}),
    ("instruments", ["NQ"]), ("sizing_basis", "TWO_CONTRACTS"), ("evaluation_stage", "INNER_DEVELOPMENT"),
    ("cost_assumptions", {"basis": "DIFFERENT"}), ("execution_assumptions", "DIFFERENT"), ("opportunity_set", "DIFFERENT"),
    ("observation_cutoff_utc", None), ("data_kind", "HISTORICAL")])
def test_comparison_requires_complete_compatible_scope(field, value):
    context = fixture_context()
    context["scope"][field] = value
    report = r.candidate_report(context, fixture_replay([1] * 8))
    result = r.compare_candidates(pair()[0], report)
    assert result["status"] == "INELIGIBLE"
    assert result["partial_diagnostic"] is None
    assert result["daily_differences"] == []
    assert "INCOMPATIBLE_OR_UNKNOWN:" + field in result["reasons"]


def test_calendar_difference_not_silent_inner_join():
    ctx = fixture_context()
    ctx["scope"]["dates"] = ctx["scope"]["dates"][:-1]
    report = r.candidate_report(ctx, fixture_replay([1] * 7, dates=ctx["scope"]["dates"]))
    result = r.compare_candidates(pair()[0], report)
    assert result["coverage"]["expected_dates"] == 8
    assert len(result["coverage"]["missing_dates"]) == 1
    assert result["statistics"]["observation_count"] == 0


@pytest.mark.parametrize("status", ["FAILED", "INCOMPLETE", "ABANDONED"])
def test_failed_incomplete_abandoned_attempts_have_no_full_economics(status):
    report = r.candidate_report(fixture_context(), fixture_replay([1] * 8), execution_status=status, failure_reason="SYNTHETIC_FAILURE")
    assert report["execution"]["status"] == status
    assert report["metrics"]["net_usd"]["value"] is None
    assert not report["execution"]["valid_no_trade"]
    assert r.experiment_history([report])["observed_attempt_count"] == 1
    r.validate_report(report)


def test_no_trade_is_complete_but_does_not_invent_returns_or_trade_metrics():
    ctx = fixture_context("no_trade")
    ctx["benchmark_id"] = "no_trade"
    replay = fixture_replay([0] * 8)
    replay["trades"] = []
    report = r.candidate_report(ctx, replay)
    assert report["execution"] == {"status": "COMPLETE", "reason": None, "valid_no_trade": True, "accounting_status": "COMPLETE"}
    assert report["metrics"]["mean_daily_net_usd"]["value"] == 0
    assert report["metrics"]["daily_sample_variance"]["value"] == 0
    assert report["metrics"]["mean_net_per_trade"]["reason"] == "ZERO_TRADES"
    assert report["trade_diagnostics"]["turnover"]["value"] == 0
    assert report["drawdown"]["maximum_depth_usd"] == 0
    assert all(report["metrics"][k]["value"] is None for k in (*r.CAPITAL_METRICS, "deflated_sharpe", "var", "cvar"))
    assert r.compare_candidates(report, pair()[1], role="benchmark")["status"] == "DESCRIPTIVE_COMPLETE"
    ctx["benchmark_id"] = "undeclared_best_looking_benchmark"
    assert r.compare_candidates(r.candidate_report(ctx, replay), pair()[1], role="benchmark")["status"] == "INELIGIBLE"


def test_zero_new_trades_does_not_imply_zero_pnl_or_no_trade_policy():
    replay = fixture_replay([0] * 8)
    row = next(iter(replay["daily"].values()))
    row.update(gross_usd=3., net_usd=3.)
    report = r.candidate_report(fixture_context(), replay)
    assert report["metrics"]["net_usd"]["value"] == 3
    assert not report["execution"]["valid_no_trade"]


def test_drawdown_baseline_recovery_and_unfinished_episode():
    dates = fixture_context()["scope"]["dates"]
    result = r.drawdown(dates, [-5, 2, 3, 4, -2, 2, -3, -2])
    assert result["maximum_depth_usd"] == 5
    assert result["time_underwater_dates"] == 5
    assert result["completed_recoveries"] == 2
    assert result["episodes"][0]["peak_date"] is None
    assert result["episodes"][0]["recovery_date"] == dates[2]
    assert result["current_episode"]["status"] == "UNRECOVERED"
    assert result["current_episode"]["underwater_dates"] == 2
    assert result["current_episode"]["depth_usd"] == 5


def test_nonpositive_concentration_and_policy_confounds():
    ctx = fixture_context()
    report = r.candidate_report(ctx, fixture_replay([-1] * 8))
    assert report["profit_concentration"]["top_five_share_of_total_net_profit"]["reason"] == "NONPOSITIVE_TOTAL_NET_PROFIT"
    assert report["profit_concentration"]["top_five_share_of_positive_day_profits"]["value"] is None
    ctx["policy"]["entry"]["filter"] = "changed_after_results"
    changed = r.candidate_report(ctx, fixture_replay([1] * 8))
    comparison = r.compare_candidates(pair()[0], changed)
    assert comparison["attribution"] == "FULL_POLICY"
    assert comparison["undeclared_or_confounding_changes"] == ["entry.filter"]


def test_trade_costs_reconcile_once_and_fixed_trade_sensitivity_is_labeled():
    ctx = fixture_context()
    replay = fixture_replay([0] * 8)
    first = ctx["scope"]["dates"][0]
    replay["daily"][first] = {"gross_usd": 150., "cost_usd": 85., "net_usd": 65., "trades": 1, "complete": True}
    replay["trades"] = [{"gross_nano_usd": 150_000_000_000, "cost_nano_usd": 85_000_000_000, "net_nano_usd": 65_000_000_000,
                          "entry_ns": 0, "exit_ns": 60 * 60 * 10**9, "direction": 1, "tick_value_nano_usd": 12_500_000_000}]
    report = r.candidate_report(ctx, replay)
    trade = report["trade_diagnostics"]
    assert trade["itemized_costs"]["fees_usd"] == 10
    assert trade["itemized_costs"]["price_allowance_usd"] == 75
    assert trade["itemized_costs"]["already_deducted_in_net_pnl"]
    assert trade["exposure"]["long_contract_minutes"] == 60
    assert trade["turnover"]["value"] == 2
    sensitivity = trade["fixed_trade_cost_sensitivity"]
    assert sensitivity["scenarios"]["adverse"]["net_usd"] == report["metrics"]["net_usd"]["value"] == 65
    assert sensitivity["scenarios"]["severe"]["net_usd"] == -10
    assert "NOT_POLICY_REPLAY" in sensitivity["basis"]
    halves = []
    for i in range(2):
        half = deepcopy(ctx)
        half["scope"]["dates"] = ctx["scope"]["dates"][4*i:4*i+4]
        for key in ("fold_by_date", "segment_by_date"):
            half["scope"][key] = {d: ctx["scope"][key][d] for d in half["scope"]["dates"]}
        accounting = fixture_replay([0] * 4, dates=half["scope"]["dates"])
        accounting["daily"][half["scope"]["dates"][0]] = replay["daily"][first]
        accounting["trades"] = replay["trades"]
        halves.append(r.candidate_report(half, accounting))
    combined = r.combine_folds(ctx, halves)
    assert combined["metrics"]["net_usd"]["value"] == 130
    assert combined["trade_diagnostics"]["itemized_costs"]["fees_usd"] == 20
    assert combined["trade_diagnostics"]["fixed_trade_cost_sensitivity"]["scenarios"]["severe"]["net_usd"] == -20


def test_decision_to_fill_cost_drift_handles_signed_partial_missed_and_fees():
    base = {"order_id": "generated", "decision_time_utc": "2018-01-02T15:00:00Z", "decision_price": -10.,
            "requested_quantity": 2, "multiplier_usd_per_price_unit": 10, "side": "SELL", "execution_status": "PARTIAL",
            "fills": [{"fill_time_utc": "2018-01-02T15:00:01Z", "price": -11., "quantity": 1, "fee_usd": 2}],
            "assumed_cost_for_filled_quantity_usd": 5}
    result = r.execution_cost_drift([base])
    assert result["orders"][0]["observed_cost_usd"] == 12
    assert result["orders"][0]["observed_minus_assumed_usd"] == 7
    assert result["orders"][0]["unfilled_quantity"] == 1
    assert result["additional_pnl_deduction_usd"] == 0
    base.update(fills=[], execution_status="MISSED")
    assert r.execution_cost_drift([base])["cost_per_filled_unit"]["value"] is None
    assert r.execution_cost_drift(None)["availability"] == "UNAVAILABLE"


def test_declared_bootstrap_is_reproducible_segmented_and_joint(monkeypatch):
    a, b = pair()
    cfg = fixture_inference()
    original = r.stationary_bootstrap_index_rows
    calls = []
    def tracked(**kwargs):
        calls.append(kwargs)
        yield from original(**kwargs)
    monkeypatch.setattr(r, "stationary_bootstrap_index_rows", tracked)
    first = r.compare_candidates(a, b, inference=cfg)["uncertainty"]
    second = r.compare_candidates(a, b, inference=cfg)["uncertainty"]
    assert first == second
    assert first["availability"] == "AVAILABLE_DESCRIPTIVE_ONLY"
    assert first["segment_lengths"] == [4, 4]
    assert all(c["n_observations"] == 4 for c in calls)
    assert len(first["sensitivity"]) == 2
    assert len(first["sensitivity"][0]["drawdown_scenarios"]["incumbent_candidate_depth_usd"][0]) == 2
    # Manual index oracle: same dates in A and B, no independent resampling of columns.
    differences = np.array([1., 3., 1., 0., 2., 1., -1., -1.])
    samples = []
    generators = [original(n_observations=4, n_resamples=64, mean_block_length=2, seed=617 + i) for i in range(2)]
    for left, right in zip(*generators, strict=True):
        samples.append(np.mean(differences[np.r_[left, right + 4]]))
    np.testing.assert_allclose(first["sensitivity"][0]["interval_usd_per_date"], np.quantile(samples, [.05, .95]))
    assert "not adaptive-selection correction" in first["limitations"]


@pytest.mark.parametrize("mutation,reason", [({"declared_at_utc": "2024-01-01T00:00:00Z"}, "EXPOSURE"),
    ({"minimum_dates": 99}, "SUPPORT"), ({"mean_block_length": 1}, "BLOCK_LENGTH"),
    ({"minimum_segment_dates": 9}, "SUPPORT"), ({"interval_method": "AUTOMATIC_BEST"}, "UNSUPPORTED"),
    ({"confidence_level": None}, "INCOMPLETE"), ({"replicates": 9_000_000}, "BOUND"),
    ({"eligible_stages": ["INNER_DEVELOPMENT"]}, "STAGE")])
def test_unsupported_inference_has_reasons(mutation, reason):
    a, b = pair()
    result = r.compare_candidates(a, b, inference={**fixture_inference(), **mutation})["uncertainty"]
    assert result["availability"] == "UNAVAILABLE"
    assert reason in result["reason"]


def test_zero_variance_is_not_fake_significance_and_unknown_exposure_stays_unknown():
    a = r.candidate_report(fixture_context("a"), fixture_replay([1] * 8))
    b = r.candidate_report(fixture_context("b"), fixture_replay([2] * 8))
    assert "ZERO_VARIANCE" in r.compare_candidates(a, b, inference=fixture_inference())["uncertainty"]["reason"]
    ctx = fixture_context()
    ctx["lineage"].pop("first_result_exposure_utc")
    unknown = r.candidate_report(ctx, fixture_replay([1] * 8))
    assert unknown["information_use"]["independence"] == "UNKNOWN"
    assert "EXPOSURE" in r.compare_candidates(pair()[0], unknown, inference=fixture_inference())["uncertainty"]["reason"]
    ctx["lineage"]["informed_by_results"] = [{"result_id": "already_viewed", "change": "hours"}]
    assert "REUSE_IS_EXPLORATORY" in r.candidate_report(ctx, fixture_replay([1] * 8))["information_use"]["independence"]


def test_legacy_unknown_fields_and_counts_never_fabricate_history():
    ctx = {"scope": fixture_context()["scope"]}
    report = r.candidate_report(ctx, {"daily": {}})
    assert report["identity"]["hypothesis"] is None
    assert report["identity"]["run_id"] is None
    assert report["information_use"]["independence"] == "UNKNOWN"
    assert report["metrics"]["net_usd"]["value"] is None
    history = r.experiment_history([report, pair()[0]])
    assert history["observed_attempt_count"] == 1 and history["unknown_attempt_identity_records"] == 1
    assert history["effective_independent_trials"] is None and not history["selection_bias_removed"]


def test_joint_losses_keep_columns_and_do_not_create_equal_weight_portfolio():
    a, b = pair()
    joint = r.joint_loss_diagnostics([a, b], {"combination_experiment_id": None, "weights": None})
    assert joint["joint_loss_date_count"] == 4
    assert joint["joint_loss_vectors_usd"][0]["values"] == [-4, -3]
    assert joint["portfolio_performance"]["value"] is None
    zero = r.candidate_report(fixture_context("zero"), fixture_replay([0] * 8))
    assert r.joint_loss_diagnostics([a, zero])["correlation"]["reason"] == "ZERO_VARIANCE_STRATEGY"


class DoNotInspect:
    def __getattribute__(self, name):
        raise AssertionError("restricted payload inspected")


@pytest.mark.parametrize("stage", ["FINAL_SEALED", "FORWARD", "UNKNOWN"])
def test_protected_stage_rejected_before_payload_access(stage):
    ctx = fixture_context()
    ctx["scope"]["evaluation_stage"] = stage
    with pytest.raises(UnauthorizedOperation):
        r.candidate_report(ctx, DoNotInspect())
    metadata = r.candidate_report(ctx)
    assert metadata["daily"] == []
    assert metadata["metrics"]["net_usd"]["value"] is None


def test_protected_date_rejected_before_payload_and_report_cannot_pass_gate():
    ctx = fixture_context()
    ctx["scope"]["dates"] = ["2026-01-01"]
    with pytest.raises(UnauthorizedOperation):
        r.candidate_report(ctx, DoNotInspect())
    report = pair()[0]
    report["research_gate_passed"] = True
    report["report_id"] = sha256_json({k: v for k, v in report.items() if k != "report_id"})
    with pytest.raises(IntegrityError, match="gate"):
        r.validate_report(report)


def test_catalog_rejects_sealed_manifest_before_any_report_open(tmp_path, monkeypatch):
    ctx = fixture_context()["scope"]
    ctx["evaluation_stage"] = "FINAL_SEALED"
    entry = {"report_id": "a" * 64, "filename": "a" * 64 + ".json", "scope": ctx, "sha256": "b" * 64}
    manifest = {"schema_version": "l0_alpha_report_catalog/2", "entries": [entry], "comparisons": []}
    manifest["catalog_id"] = sha256_json(manifest)
    path = tmp_path / ("catalog-" + manifest["catalog_id"] + ".json")
    write_new_canonical_authority(path, manifest)
    original = Path.read_bytes
    def read(own):
        assert own.name == path.name, "report payload opened before scope gate"
        return original(own)
    monkeypatch.setattr(Path, "read_bytes", read)
    with pytest.raises(UnauthorizedOperation):
        r.load_catalog(path)


def test_unknown_legacy_and_sealed_metadata_can_display_without_observations(tmp_path):
    legacy = r.candidate_report({"candidate_id": "legacy_failed"}, execution_status="FAILED")
    ctx = fixture_context("sealed_metadata")
    ctx["scope"]["evaluation_stage"] = "FINAL_SEALED"
    ctx["scope"]["dates"] = ["2025-07-14"]
    sealed = r.candidate_report(ctx, execution_status="COMPLETE")
    view = r.load_catalog(r.write_catalog(tmp_path, [legacy, sealed]))
    assert len(view["reports"]) == 2
    assert all(not report["daily"] for report in view["reports"])
    assert all(report["metrics"]["net_usd"]["value"] is None for report in view["reports"])
    assert sealed["coverage"]["expected_dates"] == 1
    assert sealed["execution"]["status"] == "COMPLETE"
    assert not sealed["metric_eligibility"]["complete_reference_economics"]
    known_stage = r.candidate_report({"candidate_id": "legacy_stage_only", "scope": {"evaluation_stage": "INNER_DEVELOPMENT"}}, execution_status="FAILED")
    assert r.report_view([known_stage])["reports"][0]["scope"]["dates"] is None


def test_catalog_machine_human_and_cockpit_share_calculations(tmp_path):
    from futures_rebuild.live_cockpit.app import CockpitApi
    a, b = pair()
    request = {"incumbent_report_id": a["report_id"], "candidate_report_id": b["report_id"], "inference": fixture_inference()}
    path = r.write_catalog(tmp_path, [a, b], [request])
    expected = r.report_view([a, b], [request])
    loaded = r.load_catalog(path)
    assert loaded == expected
    assert path.with_suffix(".md").read_text(encoding="utf-8") == r.markdown_view(expected, detailed=False)
    assert (tmp_path / (a["report_id"] + ".md")).read_text(encoding="utf-8") == r.markdown_view(r.report_view([a]))
    api = CockpitApi(SimpleNamespace(candidate_reports=loaded))
    assert api.candidate_reports() == expected
    assert CockpitApi(SimpleNamespace(candidate_reports=None)).candidate_reports()["reports"] == []
    assert path == r.write_catalog(tmp_path, [a, b], [request])


def test_reporting_store_uses_lifecycle_hooks_without_fit_replay_or_registration(tmp_path, monkeypatch):
    from futures_rebuild import l0_alpha_preparation as p
    from futures_rebuild.l0_alpha_report_store import ReportingStore
    def forbidden(*args, **kwargs):
        raise AssertionError("research path called by reporting")
    for name in ("run_market_wfa", "fit_model", "predict", "replay_window", "final_refit", "evaluate_wfa"):
        monkeypatch.setattr(p.research, name, forbidden)
    monkeypatch.setattr(p, "execute_registered_market", forbidden)
    ctx = fixture_context()
    store = ReportingStore(tmp_path, {"data": "SYNTHETIC_NONREGISTERABLE"}, report_contexts={"attempt": ctx, "failed": fixture_context("failed")},
                           clock=lambda: "2023-01-02T18:00:01Z")
    def produce():
        store.save("attempt/replay", fixture_replay([0] * 8))
        assert store.reports[("attempt", "adverse")]["execution"]["status"] == "INCOMPLETE"
        return {"status": "VALID"}
    store.checkpoint("attempt", produce)
    old_catalog = store.latest_catalog.read_bytes()
    assert store.checkpoint("attempt", forbidden) == {"status": "VALID"}
    assert store.reports[("attempt", "adverse")]["execution"]["valid_no_trade"]
    with pytest.raises(RuntimeError):
        store.checkpoint("failed", lambda: (_ for _ in ()).throw(RuntimeError("generated failure")))
    failed_catalog = r.load_catalog(store.latest_catalog)
    assert any(x["execution"]["status"] == "FAILED" for x in failed_catalog["reports"])
    assert failed_catalog["history"]["observed_attempt_count"] == 2
    assert store.path("failed/failure").is_file()
    assert old_catalog != store.latest_catalog.read_bytes()
    with pytest.raises(IntegrityError, match="no duplicate"):
        store.checkpoint("failed", forbidden)
    with pytest.raises(IntegrityError, match="explicit previous"):
        ReportingStore(tmp_path, store.context, report_contexts=store.report_contexts)
    resumed = ReportingStore(tmp_path, store.context, report_contexts=store.declared_report_contexts, previous_catalog=store.latest_catalog)
    assert resumed.reports == store.reports
    # Two operations with the same dates cannot become two independent outer folds.
    resumed.save("run", {"synthetic": "PRECOMPUTED_FIXTURE_ONLY"})
    assert resumed.report_failure["status"] == "UNAVAILABLE_REPORTING_FAILED"
    assert resumed.report_failure["error_type"] == "IntegrityError"


def test_declared_fold_aggregation_retains_missing_failed_dates_and_counts(tmp_path):
    from futures_rebuild.l0_alpha_report_store import ReportingStore
    full = fixture_context()
    declarations = {}
    for i in range(2):
        ctx = deepcopy(full)
        ctx["scope"]["dates"] = full["scope"]["dates"][4*i:4*i+4]
        for key in ("fold_by_date", "segment_by_date"):
            ctx["scope"][key] = {d: full["scope"][key][d] for d in ctx["scope"]["dates"]}
        declarations[f"outer_{i}/selected"] = ctx
    store = ReportingStore(tmp_path, {"scope": "SYNTHETIC"}, report_contexts=declarations,
                           clock=lambda: "2023-01-02T18:00:01Z")
    for op, ctx in declarations.items():
        def produce():
            store.save(op + "/adverse", fixture_replay([0, 3, -1, -2], dates=ctx["scope"]["dates"]))
            return {"reports": "PRECOMPUTED_ONLY"}
        store.checkpoint(op, produce)
    store.save("run", {"fixture": "PRECOMPUTED;NEVER_INSPECTED_BY_REPORTING"})
    aggregate = store.reports[("outer_aggregate", "adverse")]
    assert aggregate["execution"]["status"] == "COMPLETE"
    assert aggregate["metrics"]["net_usd"]["value"] == 0
    assert len(aggregate["folds"]) == 2
    assert r.experiment_history(list(store.reports.values()))["observed_attempt_count"] == 2
    partial = r.combine_folds(full, [store.reports[("outer_0/selected", "adverse")]])
    assert partial["execution"]["status"] == "INCOMPLETE"
    assert len(partial["coverage"]["missing_or_unresolved_dates"]) == 4
    failed = r.candidate_report(declarations["outer_1/selected"], execution_status="FAILED")
    result = r.combine_folds(full, [store.reports[("outer_0/selected", "adverse")], failed])
    assert result["execution"]["status"] == "FAILED"
    assert result["metrics"]["net_usd"]["value"] is None


def test_current_declared_context_projection_reads_configuration_only():
    from futures_rebuild.l0_alpha_report_store import l0_context, operation_contexts
    bundle = json.loads((Path(__file__).resolve().parents[1] / "configs/l0_alpha_candidate_v7.json").read_text())
    base = l0_context(bundle, "ES", experiment_id="SYNTHETIC_METADATA", run_id="NOT_EXECUTED")
    contexts = operation_contexts(bundle, base)
    assert len(contexts) == 8 * (10 * 4 + 1)
    assert base["scope"]["observation_cutoff_utc"] is None
    assert base["lineage"]["historical_search"]["charges_are_observed_attempts"] is False
    assert base["policy"]["refitting"]["final_rule"] == bundle["design"]["final_rule"]
    assert all(ctx["scope"]["evaluation_stage"] in r.DEVELOPMENT_STAGES for ctx in contexts.values())


@pytest.mark.parametrize("field", ["drawdown", "comparison_eligibility", "information_use"])
def test_derived_report_diagnostics_cannot_be_forged_even_with_new_hash(field):
    _, report = pair()
    if field == "drawdown":
        report[field]["maximum_depth_usd"] = 0
    elif field == "comparison_eligibility":
        report[field]["reasons"] = ["invented"]
    else:
        report[field]["independence"] = "INDEPENDENT_CONFIRMATION"
    report["report_id"] = sha256_json({k: v for k, v in report.items() if k != "report_id"})
    with pytest.raises(IntegrityError):
        r.validate_report(report)


def test_synthetic_demo_is_deterministic_without_network_or_research(tmp_path, monkeypatch):
    import socket
    def forbidden(*args, **kwargs):
        raise AssertionError("network called")
    monkeypatch.setattr(socket, "socket", forbidden)
    result = generate(tmp_path)
    assert result == generate(tmp_path)
    html = Path(result["cockpit_preview"]).read_text(encoding="utf-8")
    assert "window.l0CandidateReportDemo=" in html
    assert '<script src="app.js' not in html
    assert '<script src="lightweight' not in html
    assert "candidate-reports-dialog" in html
    assert "SYNTHETIC_REPORT_FIXTURE_NOT_RESEARCH_EVIDENCE" in html
    assert "pywebview.api.candidate_reports" in html
