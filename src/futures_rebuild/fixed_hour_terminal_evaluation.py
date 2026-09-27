"""Outcome-only reporting and independent qualification for the inactive successor."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np

from .canonical import sha256_json
from .errors import IntegrityError
from .fixed_hour_terminal_successor import (
    AUTHORITY, MARKETS, TerminalDataset, candidate_definitions, daily_means, design,
    provisional_costs, terminal_directions, validate_labels,
)
from .research.contracts import ResearchContractError, finite_float64
from .research.dsr import deflated_sharpe_ratio
from .research.hac import newey_west_mean
from .research.multiple_testing import romano_wolf_from_differentials

BASELINES = tuple(design()["phase_6"]["baselines"])


def _identity(payload: Mapping, field: str) -> None:
    if payload.get(field) != sha256_json({k: v for k, v in payload.items() if k != field}):
        raise IntegrityError(f"invalid {field}")


def coverage_report(*, scheduled: int, insufficient_history: int, valid_feature: int,
                    invalid_outcome: int, quality_errors: int) -> dict:
    args = (scheduled, insufficient_history, valid_feature, invalid_outcome, quality_errors)
    if any(type(n) is not int or n < 0 for n in args):
        raise IntegrityError("coverage counts must be nonnegative integers")
    eligible = scheduled - insufficient_history
    if eligible < 0 or valid_feature > eligible or invalid_outcome > valid_feature:
        raise IntegrityError("coverage counts do not reconcile")
    valid_outcome = valid_feature - invalid_outcome
    return {"scheduled": scheduled, "insufficient_history": insufficient_history,
            "successor_eligible": eligible, "valid_feature": valid_feature,
            "invalid_feature": eligible - valid_feature, "invalid_outcome": invalid_outcome,
            "valid_outcome": valid_outcome, "quality_errors": quality_errors,
            "eligible_schedule_coverage": valid_outcome / eligible if eligible else 0.,
            "original_schedule_coverage": valid_outcome / scheduled if scheduled else 0.}


def expected_costs(evidence: Mapping | None, *, market: str, decision_ns: np.ndarray,
                   tick_values: np.ndarray, trusted_certification_ids: frozenset[str],
                   evaluation_frozen_at_ns: int | None = None) -> tuple[np.ndarray | None, str]:
    """Validate an external certification input, never infer costs from P&L.

    The L1 helper's midpoint half-spread estimate is deliberately not reused:
    this interface requires independently certified open-print-relative costs.
    Trusted identities must be supplied by a future authorized verifier, not
    taken from the evidence itself. The evaluation freeze is likewise supplied
    by the trusted caller and must match the bound certificate. Certification
    may occur after historical decisions; calibration observations may not.
    This pure function grants no such trust.
    """
    missing = (None, "EXPECTED_COST_UNCALIBRATED")
    if evaluation_frozen_at_ns is not None and type(evaluation_frozen_at_ns) is not int:
        raise IntegrityError("evaluation freeze must be an explicit integer timestamp")
    if evidence is None or evaluation_frozen_at_ns is None:
        return missing
    required = {"market", "certification_id", "source_release_id", "independent_of_candidate_results",
                "price_reference_basis", "certified_at_ns", "observations_end_ns", "effective_start_ns",
                "effective_end_ns", "round_trip_fee_usd", "adverse_ticks_round_trip", "coverage_ratio",
                "all_verified_globex_intervals", "evaluation_frozen_at_ns", "evidence_id"}
    if set(evidence) != required:
        raise IntegrityError("expected-cost evidence fields are incomplete or unknown")
    _identity(evidence, "evidence_id")
    if evidence["certification_id"] not in trusted_certification_ids:
        return missing
    if (evidence["market"] != market or evidence["price_reference_basis"] != "ONE_MINUTE_OPEN_PRINT_PROXY"
        or evidence["independent_of_candidate_results"] is not True):
        raise IntegrityError("cost evidence has wrong market, reference basis or result dependency")
    if any(type(evidence[k]) is not str or len(evidence[k]) != 64 or any(c not in "0123456789abcdef" for c in evidence[k])
           for k in ("certification_id", "source_release_id")):
        raise IntegrityError("cost source identity is invalid")
    times = [evidence[k] for k in ("observations_end_ns", "certified_at_ns", "effective_start_ns", "effective_end_ns")]
    if (any(type(t) is not int for t in times)
        or type(evidence["evaluation_frozen_at_ns"]) is not int
        or evidence["evaluation_frozen_at_ns"] != evaluation_frozen_at_ns
        or not times[0] <= times[1] <= evaluation_frozen_at_ns
        or not times[0] <= times[2] < times[3]):
        raise IntegrityError("cost certification timing is invalid")
    if decision_ns.dtype != np.int64 or decision_ns.ndim != 1 or not len(decision_ns):
        raise IntegrityError("cost decisions must be int64 timestamps")
    v = finite_float64(tick_values, name="cost tick values", ndim=1)
    if v.shape != decision_ns.shape or np.any(v <= 0):
        raise IntegrityError("cost tick economics mismatch")
    if (np.any(decision_ns < times[2]) or np.any(decision_ns >= times[3])
        or evidence["all_verified_globex_intervals"] is not True):
        return missing
    fee, ticks, coverage = [evidence[k] for k in ("round_trip_fee_usd", "adverse_ticks_round_trip", "coverage_ratio")]
    if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not np.isfinite(v) for v in (fee, ticks, coverage)) or fee < 0 or ticks < 0 or not 0 <= coverage <= 1:
        raise IntegrityError("invalid expected-cost economics")
    if coverage < 0.98:
        return missing
    return ticks + fee / v, "CERTIFIED_EXPECTED_COST_PROXY_NOT_LIVE_FILL_EVIDENCE"


def risk_diagnostics(net_ticks: np.ndarray, tick_values: np.ndarray, dates: Sequence[str],
                     directions: np.ndarray, upside: np.ndarray, downside: np.ndarray) -> dict:
    """Fixed-exit cumulative P&L; never an intrabar mark-to-market claim."""
    for x in (net_ticks, tick_values, directions, upside, downside):
        finite_float64(x, name="risk array", ndim=1)
    if any(len(x) != len(dates) for x in (net_ticks, tick_values, directions, upside, downside)) or np.any(tick_values <= 0) or not np.isin(directions, (-1., 0., 1.)).all():
        raise IntegrityError("risk inputs mismatch")
    report = {}
    for unit, values in (("ticks", net_ticks), ("usd", net_ticks * tick_values)):
        days = np.array([np.sum(values[np.array(dates) == day]) for day in sorted(set(dates))])
        curve = np.r_[0., np.cumsum(values)]
        report[unit] = {"daily_pnl_variance": float(np.var(days, ddof=0)),
                        "maximum_fixed_exit_drawdown": float(np.max(np.maximum.accumulate(curve) - curve)),
                        "total_pnl": float(np.sum(values))}
    traded = directions != 0
    adverse = np.where(directions > 0, downside, upside)[traded]
    report["adverse_excursion"] = {
        "mean_ticks": float(np.mean(adverse)) if len(adverse) else None,
        "maximum_ticks": float(np.max(adverse)) if len(adverse) else None,
        "mean_usd": float(np.mean(adverse * tick_values[traded])) if len(adverse) else None,
    }
    return {**report, "opportunities": len(dates), "trades": int(np.count_nonzero(traded)),
            "trade_dates": len(set(dates)), "capital_return_claim": False, "portfolio_claim": False,
            "intrabar_equity_drawdown_claim": False, "variance_ddof": 0}


def evaluate_outer_predictions(dataset: TerminalDataset, run: Mapping, *, upside_ticks: np.ndarray,
                               downside_ticks: np.ndarray, tick_values_usd: np.ndarray,
                               quartiles: np.ndarray, decision_ns: np.ndarray, coverage: Mapping,
                               cost_evidence: Mapping | None = None,
                               trusted_cost_certifications: frozenset[str] = frozenset(),
                               evaluation_frozen_at_ns: int | None = None) -> dict:
    _identity(run, "run_id")
    if run["mechanism_id"] != design()["mechanism_id"] or run["market"] != dataset.market or run["global_dates_id"] != sha256_json(list(dataset.global_dates)):
        raise IntegrityError("prediction run is not bound to dataset")
    validate_labels(dataset.terminal_ticks, upside_ticks, downside_ticks)
    v = finite_float64(tick_values_usd, name="tick values", ndim=1)
    if v.shape != dataset.terminal_ticks.shape or not np.array_equal(v, dataset.tick_values_usd) or np.any(v <= 0) or quartiles.shape != v.shape or not np.isin(quartiles, (1, 2, 3, 4)).all():
        raise IntegrityError("evaluation economics/quartiles mismatch")
    if decision_ns.dtype != np.int64 or decision_ns.shape != v.shape or np.any(np.diff(decision_ns) <= 0):
        raise IntegrityError("evaluation decisions must be ordered int64 timestamps")
    indices = np.array([i for fold in run["outer_folds"] for i in fold["audit_indices"]], dtype=np.int64)
    if len(np.unique(indices)) != len(indices) or np.any(indices < 0) or np.any(indices >= len(v)) or np.any(np.diff(indices) <= 0):
        raise IntegrityError("outer indices repeat or are invalid")
    expected_ids = [dataset.sample_ids[i] for i in indices]
    if expected_ids != [s for fold in run["outer_folds"] for s in fold["audit_sample_ids"]]:
        raise IntegrityError("outer opportunity identities do not match")
    d = np.array([p for fold in run["outer_folds"] for p in fold["directions"]], dtype=np.float64)
    if d.shape != indices.shape or not np.isin(d, (-1., 0., 1.)).all():
        raise IntegrityError("invalid frozen directions")
    costs = provisional_costs(dataset.market, v[indices])
    predictions = np.array([p for f in run["outer_folds"] for p in f["predictions"]], dtype=np.float64)
    if not np.array_equal(d, terminal_directions(predictions, costs["stress"])):
        raise IntegrityError("directions differ from frozen terminal rule")
    if not np.allclose(dataset.stress_cost_ticks[indices], costs["stress"], rtol=0, atol=1e-12):
        raise IntegrityError("dataset costs differ from frozen stress floor")
    dates = tuple(dataset.trade_dates[i] for i in indices)
    actual = dataset.terminal_ticks[indices]
    gross = d * actual
    net = {"gross": gross, **{name: gross - np.abs(d) * c for name, c in costs.items()}}
    expected, cost_status = expected_costs(cost_evidence, market=dataset.market, decision_ns=decision_ns[indices],
        tick_values=v[indices], trusted_certification_ids=trusted_cost_certifications,
        evaluation_frozen_at_ns=evaluation_frozen_at_ns)
    if expected is not None:
        net["expected"] = gross - np.abs(d) * expected
    baseline_daily = {}
    for name in BASELINES:
        signals = np.array([s for fold in run["outer_folds"] for s in fold["baseline_directions"][name]], dtype=np.float64)
        baseline_daily[name] = daily_means(signals * actual - np.abs(signals) * costs["stress"], dates).tolist()
    count_inputs = {k: coverage[k] for k in ("scheduled", "insufficient_history", "valid_feature", "invalid_outcome", "quality_errors")}
    checked_coverage = coverage_report(**count_inputs)
    if dict(coverage) != checked_coverage or checked_coverage["valid_outcome"] != len(indices):
        raise IntegrityError("coverage must reconcile to scored outer rows")
    segments = {}
    for q in (1, 2, 3, 4):
        mask = quartiles[indices] == q
        days = tuple(dates[i] for i in np.flatnonzero(mask))
        segments[str(q)] = {"dates": len(set(days)), "mean_stress_net_ticks": float(np.mean(daily_means(net["stress"][mask], days))) if days else None}
    core = {"mechanism_id": design()["mechanism_id"], "market": dataset.market, "run_id": run["run_id"],
        "global_dates_id": run["global_dates_id"], "status": "EVALUATED_MECHANICS_ONLY",
        "audit_sample_ids": expected_ids, "trade_dates": sorted(set(dates)),
        "daily_stress_net_ticks": daily_means(net["stress"], dates).tolist(), "baseline_daily_stress_net_ticks": baseline_daily,
        "scenarios": {name: risk_diagnostics(values, v[indices], dates, d, upside_ticks[indices], downside_ticks[indices]) for name, values in net.items()},
        "expected_cost_status": cost_status, "coverage": checked_coverage, "segments": segments,
        "evaluation_frozen_at_ns": evaluation_frozen_at_ns,
        "cost_evidence_id": cost_evidence["evidence_id"] if expected is not None else None,
        "trades": int(np.count_nonzero(d)), "terminal_mse_ticks_squared": float(np.mean((np.array([p for f in run["outer_folds"] for p in f["predictions"]]) - actual) ** 2)),
        "authority": AUTHORITY}
    return {**core, "result_id": sha256_json(core)}


def terminal_failure(market: str, *, reason: str, engineering_failure: bool = False) -> dict:
    if market not in MARKETS or not isinstance(reason, str) or not reason.strip() or type(engineering_failure) is not bool:
        raise IntegrityError("invalid terminal market failure")
    core = {"mechanism_id": design()["mechanism_id"], "market": market, "reason": reason,
            "status": "ENGINEERING_FAILURE" if engineering_failure else "TERMINAL_ECONOMIC_FAILURE", "authority": AUTHORITY}
    return {**core, "result_id": sha256_json(core)}


def trial_burden(prior_floor: int, ledger_entries: Sequence[Mapping]) -> dict:
    """Deduplicate exact declared identities; never count files as independent trials.

    Entries are pre-result declarations, not outcome records. A caller compiling
    a ledger must explicitly bind the known historical floor and its coverage.
    """
    if type(prior_floor) is not int or prior_floor < 181:
        raise IntegrityError("predecessor trial burden cannot be reduced below 181")
    seen = {}
    for entry in ledger_entries:
        if set(entry) != {"trial_id", "count", "included_in_prior_floor", "configuration_ids"}:
            raise IntegrityError("trial declaration fields are incomplete")
        if type(entry["trial_id"]) is not str or not entry["trial_id"] or type(entry["count"]) is not int or entry["count"] < 1 or type(entry["included_in_prior_floor"]) is not bool:
            raise IntegrityError("invalid trial burden entry")
        if entry["trial_id"] in seen and seen[entry["trial_id"]] != dict(entry):
            raise IntegrityError("conflicting duplicate trial declaration")
        seen[entry["trial_id"]] = dict(entry)
    new_ids = {f"{m}/{c['candidate_id']}" for m in MARKETS for c in candidate_definitions()}
    registered_new = set()
    extra = 0
    for entry in seen.values():
        config_ids = entry["configuration_ids"]
        if not isinstance(config_ids, list) or len(config_ids) != len(set(config_ids)) or any(not isinstance(s, str) for s in config_ids):
            raise IntegrityError("invalid configuration census")
        overlap = set(config_ids) & new_ids
        if overlap:
            if set(config_ids) != overlap or entry["count"] != len(overlap) or entry["included_in_prior_floor"] or registered_new & overlap:
                raise IntegrityError("successor configurations double counted or mixed")
            registered_new |= overlap
        elif not entry["included_in_prior_floor"]:
            extra += entry["count"]
    core = {"prior_floor": prior_floor, "additional_prior_trials": extra,
            "new_configurations": 108, "already_registered_successor_configurations": len(registered_new),
            "combined_burden": prior_floor + extra + 108, "ledger_entries": sorted(seen.values(), key=lambda e: e["trial_id"])}
    return {**core, "census_id": sha256_json(core)}


def qualify_markets(results: Mapping[str, Mapping], *, burden: Mapping, dsr_censuses: Mapping[str, Mapping]) -> dict:
    """Compute independent decisions; absent evidence never becomes a pass."""
    if set(results) != set(MARKETS):
        raise IntegrityError("all four results or explicit failures are required; no macro substitution")
    _identity(burden, "census_id")
    if trial_burden(burden["prior_floor"], burden["ledger_entries"]) != dict(burden):
        raise IntegrityError("trial census arithmetic drift")
    for market, result in results.items():
        _identity(result, "result_id")
        if result["market"] != market or result["mechanism_id"] != design()["mechanism_id"]:
            raise IntegrityError("qualification identity mismatch")
        if result["status"] not in {"EVALUATED_MECHANICS_ONLY", "TERMINAL_ECONOMIC_FAILURE", "ENGINEERING_FAILURE"}:
            raise IntegrityError("unknown market result state")
    evaluated = [m for m in MARKETS if results[m]["status"] == "EVALUATED_MECHANICS_ONLY"]
    if len({(results[m]["global_dates_id"], tuple(results[m]["trade_dates"])) for m in evaluated}) > 1:
        raise IntegrityError("inference requires identical common trade dates; no inner join dropping dates")
    declared_ids = tuple(f"{m}:{b}" for m in MARKETS for b in BASELINES)
    adjusted = {h: 1.0 for h in declared_ids}
    valid_ids, columns, inference_errors = [], [], []
    settings = design()["phase_6"]
    for market in evaluated:
        result = results[market]
        if set(result["baseline_daily_stress_net_ticks"]) != set(BASELINES):
            raise IntegrityError("every baseline must be independently accounted for")
        y = np.array(result["daily_stress_net_ticks"], dtype=np.float64)
        finite_float64(y, name="inference daily means", ndim=1)
        if len(y) != len(result["trade_dates"]):
            raise IntegrityError("inference date length mismatch")
        for baseline in BASELINES:
            b = np.array(result["baseline_daily_stress_net_ticks"][baseline], dtype=np.float64)
            if b.shape != y.shape or not np.isfinite(b).all():
                raise IntegrityError("baseline dates do not align")
            diff = y - b
            try:
                estimate = newey_west_mean(diff, lag=settings["hac_lag"])
                if estimate.status != "OK":
                    raise ResearchContractError("degenerate differential")
                valid_ids.append(f"{market}:{baseline}")
                columns.append(diff)
            except ResearchContractError as exc:
                inference_errors.append(f"{market}:{baseline}:{exc}")
    if columns:
        try:
            rw = romano_wolf_from_differentials(np.column_stack(columns), hypothesis_ids=tuple(valid_ids),
                hac_lag=settings["hac_lag"], mean_block_length=10., n_resamples=999, seed=62817)
            adjusted.update(dict(zip(valid_ids, rw.adjusted_p_values.tolist(), strict=True)))
        except ResearchContractError as exc:
            inference_errors.append(str(exc))
    # Terminal failures and untestable hypotheses retain p=1 in the complete
    # declared family. No alpha or hypothesis slots are reallocated to survivors.
    decisions = {}
    for market in MARKETS:
        r = results[market]
        reasons = []
        hac_lower = dsr_probability = None
        if r["status"] != "EVALUATED_MECHANICS_ONLY":
            reasons.append(r["status"])
        else:
            y = np.array(r["daily_stress_net_ticks"], dtype=np.float64)
            if len(y) < 252: reasons.append("INSUFFICIENT_DATES")
            if r["trades"] < 500: reasons.append("INSUFFICIENT_TRADES")
            if r["coverage"]["eligible_schedule_coverage"] < .98: reasons.append("INSUFFICIENT_COVERAGE")
            if r["coverage"]["quality_errors"] != 0: reasons.append("QUALITY_ERRORS")
            try:
                h = newey_west_mean(y, lag=5)
                hac_lower = h.mean - settings["hac_one_sided_z"] * h.standard_error
                if h.status != "OK" or h.mean <= 0 or hac_lower <= 0: reasons.append("STRESS_NET_HAC_FAIL")
            except ResearchContractError: reasons.append("HAC_UNAVAILABLE")
            for segment in r["segments"].values():
                if segment["dates"] >= 63 and (segment["mean_stress_net_ticks"] is None or segment["mean_stress_net_ticks"] < 0):
                    reasons.append("SEGMENT_STRESS_FAIL")
            if any(adjusted[f"{market}:{b}"] > .05 for b in BASELINES): reasons.append("JOINT_BASELINE_INFERENCE_FAIL")
            census = dsr_censuses.get(market)
            if census is None:
                reasons.append("COMPLETE_DSR_CENSUS_UNAVAILABLE")
            else:
                try:
                    if census["burden_id"] != burden["census_id"] or census["result_id"] != r["result_id"]:
                        raise IntegrityError("DSR evidence is not bound to result and burden")
                    if len(census["trial_ids"]) != burden["combined_burden"] or len(set(census["trial_ids"])) != len(census["trial_ids"]):
                        raise IntegrityError("DSR needs a complete unique trial identity census")
                    dsr = deflated_sharpe_ratio(y, np.array(census["trial_sharpes"], dtype=np.float64), raw_trial_count=burden["combined_burden"],
                        selected_trial_index=census["selected_trial_index"], selection_rule="PREREGISTERED_INNER_WFA_WINNER")
                    dsr_probability = dsr.probability
                    if dsr_probability < settings["dsr_min_probability"]: reasons.append("DSR_FAIL")
                except (KeyError, ResearchContractError, IntegrityError): reasons.append("COMPLETE_DSR_CENSUS_INVALID")
        decisions[market] = {"qualified": not reasons, "reasons": reasons, "hac_lower": hac_lower, "dsr_probability": dsr_probability,
                             "source_result_id": r["result_id"]}
    core = {"mechanism_id": design()["mechanism_id"], "state": "JOINT_SYNTHETIC_QUALIFICATION_MECHANICS_ONLY",
            "market_decisions": decisions, "phase_12_eligible_markets": [m for m in MARKETS if decisions[m]["qualified"]],
            "declared_hypothesis_ids": list(declared_ids), "adjusted_p_values": adjusted,
            "inference_errors": inference_errors, "trial_burden_id": burden["census_id"],
            "engineering_failures": [m for m in MARKETS if results[m]["status"] == "ENGINEERING_FAILURE"],
            "claim": "INDEPENDENT_MARKETS_NO_TRANSFERABILITY_OR_PORTFOLIO_CLAIM", "authority": AUTHORITY}
    return {**core, "qualification_id": sha256_json(core)}


def downstream_eligibility(qualification: Mapping, stress_results: Mapping[str, str]) -> dict:
    _identity(qualification, "qualification_id")
    eligible = qualification["phase_12_eligible_markets"]
    if not set(stress_results) <= set(eligible) or any(s not in {"PASS", "FAIL"} for s in stress_results.values()):
        raise IntegrityError("only qualified markets may receive one-shot stress results")
    return {"phase_12": eligible, "refit_without_evaluation": [m for m in eligible if stress_results.get(m) == "PASS"],
            "closed_after_stress": [m for m in eligible if stress_results.get(m) == "FAIL"],
            "final_holdout_access": False, "common_final_252_trade_dates": ["2025-07-14", "2026-07-13"], "authority": AUTHORITY}
