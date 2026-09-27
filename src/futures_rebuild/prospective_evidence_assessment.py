"""Synthetic-only assessment of a prospective bounded-score test, not a qualifier."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
from scipy.stats import beta

from .canonical import sha256_file, sha256_json, write_new_canonical_authority
from .errors import IntegrityError
from .fixed_hour_search_accounting import CLAIMS
from .fixed_hour_terminal_successor import AUTHORITY

PLAN = Path("configs/prospective_evidence_assessment_v1.json")
RESULT = Path("state/unpublished_evidence/prospective_evidence_assessment_v1/report.json")
BETS = np.array([.05, .1, .2, .4])
SESSIONS = (252, 504, 1008)
SCENARIOS = ("iid_null", "common_shock_null", "predictable_volatility_null",
             "negative_drift_null", "partial_null", "edge_005", "edge_010", "edge_020")


def design():
    core = {"schema_version": "prospective_evidence_assessment/1.0.0",
        "state": "SYNTHETIC_ASSESSMENT_ONLY_NOT_A_PIPELINE_SUCCESSOR",
        "historical_upper_bound": "NOT_ESTABLISHED_FROM_AVAILABLE_EVIDENCE",
        "retained_charged_floor": 469, "fresh_data_rule": "ONLY_OUTCOMES_OCCURRING_AFTER_FUTURE_CAMPAIGN_FREEZE_AND_AUTHORIZATION",
        "historical_or_final_holdout_relabeling": False,
        "freeze": ["ONE_COMBINED_MODEL_AND_ONE_RIDGE_BENCHMARK_PER_MARKET", "FEATURE_TRANSFORMS",
            "TRADING_AND_COST_RULES", "ALL_28_CLAIMS", "DATA_COMPLETENESS", "ALPHA_ALLOCATION"],
        "retraining_or_model_changes_during_campaign": False,
        "prediction_recording": "IMMUTABLE_BEFORE_EACH_OPPORTUNITY_OUTCOME",
        "sampling_unit": "COMPLETE_COMMON_TRADE_DATE_SESSION_BALANCED_NET_TICKS",
        "bounded_score": "clip((combined_daily_net-baseline_daily_net-hurdle)/100,-1,1)",
        "hurdle": "0.01_TICK_FOR_RIDGE_DIFFERENTIAL;ZERO_FOR_OTHER_BASELINES",
        "target_claim": "POSITIVE_BOUNDED_AFTER_COST_SCORE_EVIDENCE",
        "uncapped_expected_profitability_claim": False,
        "null": "E[score_t|all_information_before_session_t]<=0_FOR_EVERY_t",
        "serial_dependence_rule": "PERMITTED_ONLY_WHEN_CONDITIONAL_NULL_HOLDS_NOT_MERELY_UNCONDITIONAL_ZERO_MEAN",
        "e_process": "mean_over_fixed_bets(product_over_dates(1+bet*score))",
        "fixed_bets": BETS.tolist(), "claims": list(CLAIMS),
        "program_alpha": .05, "campaign_alpha_rule": "0.05/2**campaign_index;index_starts_at_1;NO_RESET",
        "assessed_campaign_index": 1, "campaign_alpha": .025,
        "claim_crossing_threshold": 1120., "stop_rule": "ANYTIME_CROSSING;MAXIMUM_1008_SESSIONS",
        "missing_session_rule": "HALT_AS_INCOMPLETE;NO_SILENT_DROP_OR_ZERO_FILL",
        "statistical_rejection_is_market_promotion": False,
        "synthetic": {"replicates_per_scenario": 2000, "seed": 942071,
            "horizons_sessions": list(SESSIONS), "scenarios": list(SCENARIOS),
            "simultaneous_interval_error_budget": .05,
            "interval_method": "CLOPPER_PEARSON_BONFERRONI_OVER_ALL_REPORTED_INTERVALS",
            "null_fwer_upper_limit": .05,
            "strong_edge_joint_seven_claim_power_lower_limit_at_1008": .8,
            "runtime_limit_seconds": 300, "numpy_array_working_set_limit_mib": 64},
        "limits": ["NO_HISTORICAL_COUNT_RECOVERY", "NO_RAW_EXPECTANCY_INFERENCE_FROM_CLIPPING",
            "NO_VALIDITY_CLAIM_FOR_ARBITRARY_ZERO_UNCONDITIONAL_MEAN_SERIES",
            "SIMULATES_SCORE_PROCESS_NOT_TRAINING_OR_EXECUTION", "NO_MODEL_ALPHA_OR_LIVE_FILL_VALIDATION"],
        "sources": ["https://arxiv.org/abs/1810.08240", "https://doi.org/10.1093/jrsssb/qkad009"],
        "authority": AUTHORITY}
    return {**core, "assessment_id": sha256_json(core)}


def bounded_scores(combined, baselines):
    combined, baselines = np.asarray(combined, dtype=float), np.asarray(baselines, dtype=float)
    if combined.ndim != 2 or combined.shape[1] != 4 or baselines.shape != (*combined.shape, 7):
        raise IntegrityError("four markets and seven baselines per market required")
    if not len(combined) or not np.isfinite(combined).all() or not np.isfinite(baselines).all():
        raise IntegrityError("incomplete or nonfinite daily results")
    hurdle = np.zeros(7); hurdle[-1] = .01
    return np.clip((combined[:, :, None] - baselines - hurdle) / 100., -1., 1.).reshape(len(combined), 28)


def advance(log_wealth, scores):
    x = np.asarray(scores, dtype=float)
    if x.shape != log_wealth.shape[:-1] or log_wealth.shape[-1] != 4 or not np.isfinite(x).all() or np.any(abs(x) > 1):
        raise IntegrityError("bounded finite scores aligned with four fixed bets required")
    if not np.isfinite(log_wealth).all():
        raise IntegrityError("invalid betting state")
    updated = log_wealth + np.log1p(x[..., None] * BETS)
    log_e = np.logaddexp.reduce(updated, axis=-1) - np.log(4.)
    return updated, log_e


def crossings(scores):
    x = np.asarray(scores, dtype=float)
    if x.ndim != 2 or x.shape[1] != 28 or not 1 <= len(x) <= 1008:
        raise IntegrityError("one complete 28-claim campaign of at most 1008 sessions required")
    state = np.zeros((28, 4))
    crossed = np.zeros(28, dtype=bool)
    for row in x:
        state, value = advance(state, row)
        crossed |= value >= np.log(1120.)
    return {"claim_crossings": dict(zip(CLAIMS, crossed.tolist(), strict=True)),
            "statistical_rejections_only": True, "promotion_authorized": False, "authority": AUTHORITY}


def _interval(k, n, tail):
    return {"count": int(k), "replicates": n, "rate": k / n,
        "lower": float(beta.ppf(tail, k, n-k+1)) if k else 0.,
        "upper": float(beta.ppf(1-tail, k+1, n-k)) if k < n else 1.}


def simulate():
    spec = design()["synthetic"]
    n = spec["replicates_per_scenario"]
    # Two-sided simultaneous bounds over three metrics, three horizons, eight
    # scenarios. All scenarios/seeds/acceptance limits are frozen before running.
    tail = .05 / (2 * 3 * 3 * len(SCENARIOS))
    rng = np.random.default_rng(spec["seed"])
    started = time.perf_counter()
    reports = {}
    for scenario in SCENARIOS:
        state = np.zeros((n, 28, 4))
        crossed = np.zeros((n, 28), dtype=bool)
        volatility = np.full((n, 1), .75)
        true_null = np.ones(28, dtype=bool)
        if scenario == "partial_null" or scenario.startswith("edge_"):
            true_null[:7] = False
        power = {"edge_005": .05, "edge_010": .10, "edge_020": .20, "partial_null": .20}.get(scenario, 0.)
        snapshots = {}
        scenario_start = time.perf_counter()
        for day in range(1, 1009):
            scores = np.where(rng.random((n, 28)) < .5, 1., -1.)
            if scenario == "common_shock_null":
                scores[:] = scores[:, :1]
            elif scenario == "predictable_volatility_null":
                scores *= volatility
                volatility = .2 + .8 * (scores[:, :1] > 0)
            elif scenario == "negative_drift_null":
                scores = np.where(rng.random((n, 28)) < .45, 1., -1.)
            elif power:
                # Shared signal shock gives a coherent joint-claim stress case;
                # remaining markets retain independent true-null scores.
                shape = (n, 7) if scenario == "partial_null" else (n, 1)
                scores[:, :7] = np.where(rng.random(shape) < (1+power)/2, 1., -1.)
            state, log_e = advance(state, scores)
            crossed |= log_e >= np.log(1120.)
            if day in SESSIONS:
                snapshots[str(day)] = {
                    "false_any_true_null_claim": _interval(int(np.any(crossed[:, true_null], axis=1).sum()), n, tail),
                    "first_claim_detection": _interval(int(crossed[:, 0].sum()), n, tail),
                    "first_market_all_seven_claims": _interval(int(np.all(crossed[:, :7], axis=1).sum()), n, tail)}
        reports[scenario] = {"horizons": snapshots, "runtime_seconds": time.perf_counter()-scenario_start,
            "conditional_null_satisfied_on_true_null_columns": True}
        print(f"synthetic scenario complete: {scenario}", flush=True)
    elapsed = time.perf_counter()-started
    null_pass = all(r["horizons"]["1008"]["false_any_true_null_claim"]["upper"] <= .05 for r in reports.values())
    power_pass = reports["edge_020"]["horizons"]["1008"]["first_market_all_seven_claims"]["lower"] >= .8
    # Conservative array bound includes simultaneous old/new state, broadcasting
    # temporaries and RNG arrays. Not a claim about whole-process native RSS.
    array_mib = n * 28 * 8 * (4 * 6 + 8) / 1024**2
    checks = {"simulated_null_fwer": null_pass, "strong_edge_power": power_pass,
        "runtime": elapsed <= 300, "array_working_set": array_mib <= 64}
    core = {"assessment_id": design()["assessment_id"], "scenarios": reports,
        "checks": checks, "state": "PASSED_SCOPED_SYNTHETIC_ASSESSMENT" if all(checks.values()) else "FAILED_PREDECLARED_ASSESSMENT_LIMITS",
        "runtime_seconds": elapsed, "estimated_numpy_array_working_set_mib": array_mib,
        "whole_process_peak_rss_measured": False, "confidence_bounds_simultaneous": True,
        "learned_model_or_price_bar_simulation": False, "historical_upper_bound_established": False,
        "pipeline_qualification_ready": False, "authority": AUTHORITY}
    return {**core, "report_id": sha256_json(core)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--synthetic", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    if args.prepare == args.synthetic:
        parser.error("choose exactly --prepare or --synthetic; no historical data command exists")
    bindings = ("configs/legacy_trial_penalty_decision.json", "configs/fixed_hour_search_accounting_v1.json",
        "configs/fixed_hour_model_comparison_v1.json", "src/futures_rebuild/legacy_trial_census.py", __file__)
    plan = {"design": design(), "source_hashes": {str(Path(p).relative_to(root)) if Path(p).is_absolute() else p: sha256_file(root / p) for p in bindings}}
    plan["plan_id"] = sha256_json(plan)
    if args.prepare:
        write_new_canonical_authority(root / PLAN, plan)
        print(json.dumps({"plan": PLAN.as_posix(), "plan_id": plan["plan_id"]}))
    else:
        if json.loads((root / PLAN).read_bytes()) != plan:
            raise IntegrityError("synthetic declaration or source bindings drifted")
        report = simulate()
        report["plan_id"] = plan["plan_id"]
        report.pop("report_id")
        report["report_id"] = sha256_json(report)
        write_new_canonical_authority(root / RESULT, report)
        print(json.dumps({"result": RESULT.as_posix(), "state": report["state"], "checks": report["checks"]}))


if __name__ == "__main__":
    main()
