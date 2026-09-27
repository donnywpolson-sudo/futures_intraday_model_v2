"""Frozen, synthetic-only finite-sample validation of the fresh decision rule."""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
import json
from importlib.metadata import version
from pathlib import Path
import platform
import time

import numpy as np
from scipy.stats import beta

from .canonical import sha256_file, write_new_canonical_authority
from .errors import IntegrityError
from .fixed_hour_model_comparison_preparation import IMPLEMENTATION, ledger_snapshot
from .fixed_hour_model_comparison import VERSIONS
from .profit_aligned_research import AUTHORITY, MARKETS, bind, design, identity, mechanism_id
from .profit_aligned_confirmation import HORIZON, campaign_alpha, decision_from_daily

CONFIG = Path("configs/fixed_hour_model_comparison_v6.json")
EVIDENCE = Path("state/unpublished_evidence/profit_aligned_research_v1")
REPLICATIONS = 2000
SIMULATION_SEED = 942071
SCENARIOS = (
    ("iid_normal", "supported_null", 0., 0., 0.),
    ("identical_markets", "supported_null", 0., 1., 0.),
    ("serial_ar05", "supported_null", .5, 0., 0.),
    ("cross_correlated", "supported_null", 0., .75, 0.),
    ("student_t5_ar05", "supported_null", .5, .5, 0.),
    ("garch", "supported_null", 0., .5, 0.),
    ("rare_large_loss", "supported_null", 0., 0., 0.),
    ("negative_drift", "supported_null", .5, .5, -.1),
    ("partial_null", "supported_partial_null", .5, .5, .3),
    ("weak_power", "power", .5, .5, .05),
    ("moderate_power", "power", .5, .5, .1),
    ("strong_power", "power", .5, .5, .3),
    ("strong_persistence_ar09", "robustness", .9, .5, 0.),
    ("structural_break", "robustness", .5, .5, 0.),
)


def specification():
    return bind({"mechanism_id": mechanism_id(), "replications_per_scenario": REPLICATIONS,
        "scenario_fields": ["name", "role", "ar", "cross_correlation", "drift_in_long_run_sd"],
        "scenarios": [list(s) for s in SCENARIOS], "seed": SIMULATION_SEED, "burn_in": 1024,
        "daily_innovation_scale_usd": 100., "student_df": 5,
        "garch_parameters": {"omega": .05, "alpha": .05, "beta": .9},
        "rare_loss": {"probability": .01, "loss": 20., "mean_compensation": .2, "normal_noise_sd": .2},
        "structural_break_half_period_drift": [.35, -.35],
        "positive_drift_market": "ES", "negative_drift_markets": list(MARKETS),
        "production_kernel": "decision_from_daily;252_dates;9999_draws;4_slots;campaign_index_1",
        "nonrejecting_resampling_futility": "REPORT_P_ONE_WHEN_62_EXCEEDANCES_PRECLUDE_ANY_CAMPAIGN_REJECTION",
        "fixed_trade_counts": [500] * 4, "null_metric": "ANY_TRUE_NULL_MARKET_QUALIFIED",
        "power_metric": "ES_QUALIFIED", "interval": "EXACT_CLOPPER_PEARSON_SIMULTANEOUS_95_PERCENT",
        "interval_count": len(SCENARIOS) * 2, "maximum_supported_null_upper_bound": .025,
        "minimum_strong_power_lower_bound": .8, "robustness_gates_promotion": False,
        "scope": "FROZEN_MODEL_CONDITIONAL_RAW_PNL_CONFIRMATION;UPSTREAM_CUSTODY_TESTED_SEPARATELY",
        "not_proof_for_unmodeled_distributions": True, "authority": AUTHORITY}, "validation_design_id")


def implementation_files(root):
    names = set(IMPLEMENTATION) | {"profit_aligned_research.py", "profit_aligned_confirmation.py",
        "profit_aligned_validation.py", "canonical.py", "research/contracts.py"}
    return {"src/futures_rebuild/" + name: sha256_file(root / "src/futures_rebuild" / name) for name in sorted(names)}


def runtime_versions():
    observed = {name: version(name) for name in VERSIONS}
    if observed != VERSIONS:
        raise IntegrityError("research dependency versions differ from frozen numerical environment")
    return {**observed, "python": platform.python_version()}


def prepare(root):
    census = ledger_snapshot(root)
    configuration_ids = [f"{mechanism_id()}/{m}/{c['candidate_id']}" for m in MARKETS for c in design()["candidates"]]
    registered = set(configuration_ids) & {i.removeprefix("candidate:") for i in census["candidate_identities"] if i.startswith("candidate:")}
    # Accounting is preserved; it is neither an observed-trial census nor an
    # upper bound and supplies no historical statistical qualification.
    phases = {p: bind({"phase": p, "mechanism_id": mechanism_id(), "authority": AUTHORITY,
        "state": "INACTIVE_SUCCESSOR", "rules": design()}, "contract_id") for p in ("3", "4", "5", "6", "ladder")}
    bundle = bind({"schema_version": "profit_aligned_preparation/1.0.0", "specification": design(),
        "preserved_preparation": {"path": "configs/fixed_hour_model_comparison_v5.json",
            "sha256": sha256_file(root / "configs/fixed_hour_model_comparison_v5.json"),
            "change": "REJECT_MISSING_OR_EXTRA_VALIDATION_SCENARIOS;STATISTICAL_DESIGN_UNCHANGED"},
        "phase_contracts": phases, "validation_design": specification(),
        "implementation_files": implementation_files(root), "runtime_versions": runtime_versions(), "prior_accounting": census,
        "new_configuration_ids": configuration_ids, "already_registered_successor_ids": sorted(registered),
        "combined_accounting_floor": census["burden"]["combined_burden"] + 180 - len(registered),
        "new_configuration_charges": 180, "historical_upper_bound_established": False,
        "historical_qualification": "BLOCKED_CENSUS_UNRESOLVED",
        "fresh_confirmation": "BLOCKED_UNTIL_VALIDATION_AND_SOURCE_CERTIFICATION",
        "source_bound_readiness": "NOT_CERTIFIED", "state": "PREPARED_INACTIVE_NOT_REGISTERED",
        "authority": AUTHORITY}, "bundle_id")
    write_new_canonical_authority(root / CONFIG, bundle)
    return bundle


def validate_bundle(bundle, root):
    identity(bundle, "bundle_id")
    phases = {p: bind({"phase": p, "mechanism_id": mechanism_id(), "authority": AUTHORITY,
        "state": "INACTIVE_SUCCESSOR", "rules": design()}, "contract_id") for p in ("3", "4", "5", "6", "ladder")}
    if (bundle["specification"] != design() or bundle["validation_design"] != specification()
            or bundle["implementation_files"] != implementation_files(root)
            or bundle["phase_contracts"] != phases
            or bundle["preserved_preparation"]["sha256"] != sha256_file(root / "configs/fixed_hour_model_comparison_v5.json")
            or bundle["runtime_versions"] != runtime_versions() or any(bundle["authority"].values())):
        raise IntegrityError("frozen synthetic declaration or implementation changed")


def generated_pnl(scenario_index, replication):
    name, _, phi, rho, drift = SCENARIOS[scenario_index]
    rng = np.random.default_rng(np.random.SeedSequence([SIMULATION_SEED, scenario_index, replication]))
    n = HORIZON + 1024
    if name == "student_t5_ar05":
        independent, common = rng.standard_t(5, (n, 4)), rng.standard_t(5, (n, 1))
        independent *= np.sqrt(3 / 5)
        common *= np.sqrt(3 / 5)
    else:
        independent, common = rng.normal(size=(n, 4)), rng.normal(size=(n, 1))
    shocks = np.sqrt(rho) * common + np.sqrt(1 - rho) * independent
    if name == "rare_large_loss":
        shocks = .2 - 20. * (rng.random((n, 4)) < .01) + .2 * independent
    if name == "garch":
        variance = np.ones(4)
        for t in range(n):
            shocks[t] *= np.sqrt(variance)
            variance = .05 + .05 * shocks[t]**2 + .9 * variance
    path = np.zeros((n, 4))
    for t in range(1, n):
        path[t] = phi * path[t - 1] + shocks[t]
    values = path[-HORIZON:] * 100.
    if drift < 0:
        values += drift * 100. / (1 - phi)
    elif drift > 0:
        values[:, 0] += drift * 100. / (1 - phi)
    if name == "structural_break":
        values[:HORIZON // 2] += .35 * 100. / (1 - phi)
        values[HORIZON // 2:] -= .35 * 100. / (1 - phi)
    return values


def _batch(job):
    scenario, start, stop = job
    false_rejections = es_rejections = 0
    positive = SCENARIOS[scenario][4] > 0
    for rep in range(start, stop):
        result = decision_from_daily(generated_pnl(scenario, rep))
        qualified = [result["market_decisions"][m]["qualified"] for m in MARKETS]
        false_rejections += int(any(qualified[1:] if positive else qualified))
        es_rejections += int(qualified[0])
    return scenario, start, stop, false_rejections, es_rejections


def interval(successes, trials):
    if type(successes) is not int or type(trials) is not int or not 0 <= successes <= trials or trials < 1:
        raise IntegrityError("invalid Monte Carlo counts")
    tail = .05 / (2 * len(SCENARIOS) * 2)
    return {"count": successes, "replications": trials, "rate": successes / trials,
        "lower": 0. if successes == 0 else float(beta.ppf(tail, successes, trials - successes + 1)),
        "upper": 1. if successes == trials else float(beta.ppf(1 - tail, successes + 1, trials - successes))}


def summarize(counts):
    if len(counts) != len(SCENARIOS):
        raise IntegrityError("validation requires every declared scenario exactly once")
    reports = []
    for scenario, (completed, false_rejections, es_rejections) in enumerate(counts):
        if completed != REPLICATIONS:
            raise IntegrityError("partial Monte Carlo run cannot be certified")
        name, role, *_ = SCENARIOS[scenario]
        fwer, power = interval(false_rejections, completed), interval(es_rejections, completed)
        required_null = role.startswith("supported") or role == "power"
        null_pass = fwer["upper"] <= campaign_alpha(1)
        power_pass = power["lower"] >= .8 if name == "strong_power" else None
        reports.append({"scenario": name, "role": role, "false_positive_family": fwer,
            "es_detection": power, "null_gate_required": required_null,
            "null_gate_passed": null_pass if required_null else None, "strong_power_gate_passed": power_pass})
    passed = all(r["null_gate_passed"] is not False and r["strong_power_gate_passed"] is not False for r in reports)
    return reports, passed


def run_synthetic(root, *, workers=2):
    if workers not in (1, 2):
        raise IntegrityError("bounded synthetic run permits one or two workers")
    bundle = json.loads((root / CONFIG).read_bytes())
    validate_bundle(bundle, root)
    path = root / EVIDENCE / bundle["bundle_id"]
    path.mkdir(parents=True, exist_ok=True)
    # Declaration bytes exist before the first replication; result files never
    # rewrite them, even when calibration fails.
    write_new_canonical_authority(path / "declaration.json", bundle)
    started = time.monotonic()
    counts = [[0, 0, 0] for _ in SCENARIOS]
    jobs = [(s, start, min(start + 100, REPLICATIONS)) for s in range(len(SCENARIOS)) for start in range(0, REPLICATIONS, 100)]
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for s, start, stop, false_rejections, es_rejections in executor.map(_batch, jobs, chunksize=1):
            counts[s][0] += stop - start
            counts[s][1] += false_rejections
            counts[s][2] += es_rejections
            print(f"{SCENARIOS[s][0]}: {counts[s][0]}/{REPLICATIONS}", flush=True)
    validate_bundle(bundle, root)
    reports, passed = summarize(counts)
    report = bind({"bundle_id": bundle["bundle_id"], "validation_design_id": specification()["validation_design_id"],
        "implementation_files": bundle["implementation_files"], "completed_replications": sum(c[0] for c in counts),
        "scenarios": reports, "counts": counts, "statistical_gate_passed": passed,
        "state": "PASSED_SYNTHETIC_CALIBRATION_ONLY" if passed else "BLOCKED_STATISTICAL_VALIDATION_FAILED",
        "source_bound_readiness": "NOT_CERTIFIED", "activation_ready": False,
        "qualification_ready": False, "elapsed_seconds": time.monotonic() - started,
        "workers": workers, "authority": AUTHORITY}, "report_id")
    write_new_canonical_authority(path / "report.json", report)
    return report, path / "report.json"


def validate_report(report, bundle, root):
    validate_bundle(bundle, root)
    identity(report, "report_id")
    reports, passed = summarize(report["counts"])
    if (report["bundle_id"] != bundle["bundle_id"] or report["scenarios"] != reports
            or report["statistical_gate_passed"] != passed or report["completed_replications"] != REPLICATIONS * len(SCENARIOS)
            or report["implementation_files"] != bundle["implementation_files"]
            or report["validation_design_id"] != specification()["validation_design_id"]
            or report["activation_ready"] is not False or report["qualification_ready"] is not False
            or report["authority"] != AUTHORITY
            or report["state"] != ("PASSED_SYNTHETIC_CALIBRATION_ONLY" if passed else "BLOCKED_STATISTICAL_VALIDATION_FAILED")):
        raise IntegrityError("synthetic report is incomplete or differs from frozen declaration")
    return passed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--run-synthetic", action="store_true")
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    if args.prepare == args.run_synthetic:
        parser.error("select exactly one prepare or synthetic operation")
    root = Path.cwd()
    if args.prepare:
        print(prepare(root)["bundle_id"])
    else:
        report, path = run_synthetic(root, workers=args.workers)
        print(json.dumps({"state": report["state"], "path": str(path)}))


if __name__ == "__main__":
    main()
