"""Explicit model-comparison evaluation; no predecessor identity substitution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np

from .canonical import sha256_json
from .errors import IntegrityError
from .fixed_hour_terminal_successor import (
    AUTHORITY, MARKETS, TerminalDataset, daily_means,
    provisional_costs, terminal_directions, validate_labels,
)
from .research.contracts import ResearchContractError, finite_float64
from .research.dsr import deflated_sharpe_ratio
from .research.hac import newey_west_mean
from .research.multiple_testing import romano_wolf_from_differentials

from .fixed_hour_model_comparison import candidate_definitions, design, predict, run_market_wfa
from .fixed_hour_terminal_successor import fold_schedule, FEATURE_NAMES
from .fixed_hour_terminal_evaluation import coverage_report, expected_costs, risk_diagnostics

BASELINES = (*design()["phase_6"]["baselines"], "ridge_only")


def _identity(payload: Mapping, field: str) -> None:
    if payload.get(field) != sha256_json({k: v for k, v in payload.items() if k != field}):
        raise IntegrityError(f"invalid {field}")


def evaluate_outer_predictions(dataset: TerminalDataset, run: Mapping, *, upside_ticks: np.ndarray,
                               downside_ticks: np.ndarray, tick_values_usd: np.ndarray,
                               quartiles: np.ndarray, decision_ns: np.ndarray, coverage: Mapping,
                               cost_evidence: Mapping | None = None,
                               trusted_cost_certifications: frozenset[str] = frozenset(),
                               evaluation_frozen_at_ns: int | None = None) -> dict:
    _identity(run, "run_id")
    if run["mechanism_id"] != design()["mechanism_id"] or run["market"] != dataset.market or run["global_dates_id"] != sha256_json(list(dataset.global_dates)):
        raise IntegrityError("prediction run is not bound to dataset")
    # A self-consistent model hash cannot prove selection or training provenance.
    if dict(run) != run_market_wfa(dataset):
        raise IntegrityError("run differs from deterministic selection and fitting replay")
    splits = fold_schedule(dataset)
    if len(run["outer_folds"]) != len(splits):
        raise IntegrityError("complete frozen outer folds required")
    for fold, split in zip(run["outer_folds"], splits, strict=True):
        if fold["audit_indices"] != split.audit_indices.tolist() or fold["fit_sample_ids"] != [dataset.sample_ids[i] for i in split.fit_indices]:
            raise IntegrityError("outer fold identities differ from frozen schedule")
        for name in ("combined", "ridge_only"):
            stream = fold["streams"][name]
            fitted = stream["fitted_model"]
            if (fitted["fit_sample_ids"] != fold["fit_sample_ids"]
                or fitted["fit_trade_dates"] != [dataset.trade_dates[i] for i in split.fit_indices]
                or stream["candidate_id"] != fitted["candidate"]["candidate_id"]
                or (name == "ridge_only" and fitted["candidate"]["family"] != "ridge")):
                raise IntegrityError("fitted stream training/candidate binding mismatch")
            if not np.array_equal(predict(fitted, dataset.features[split.audit_indices]), stream["predictions"]):
                raise IntegrityError("predictions differ from frozen model")
        previous = np.sign(dataset.features[split.audit_indices, FEATURE_NAMES.index("close_change_ticks_1")])
        expected_signals = {"flat_no_trade": np.zeros(len(previous)),
            "fold_local_unconditional_direction": np.full(len(previous), np.sign(np.mean(dataset.terminal_ticks[split.fit_indices]))),
            "previous_bar_sign_momentum": previous, "previous_bar_sign_reversal": -previous,
            "always_long": np.ones(len(previous)), "always_short": -np.ones(len(previous))}
        if set(fold["baseline_directions"]) != set(expected_signals) or any(
            not np.array_equal(fold["baseline_directions"][k], v) for k, v in expected_signals.items()):
            raise IntegrityError("baseline directions differ from frozen procedure")
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
    d = np.array([p for fold in run["outer_folds"] for p in fold["streams"]["combined"]["directions"]], dtype=np.float64)
    if d.shape != indices.shape or not np.isin(d, (-1., 0., 1.)).all():
        raise IntegrityError("invalid frozen directions")
    costs = provisional_costs(dataset.market, v[indices])
    predictions = np.array([p for f in run["outer_folds"] for p in f["streams"]["combined"]["predictions"]], dtype=np.float64)
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
        signals = np.array([s for fold in run["outer_folds"] for s in
            (fold["streams"]["ridge_only"]["directions"] if name == "ridge_only" else fold["baseline_directions"][name])], dtype=np.float64)
        if name == "ridge_only":
            ridge_predictions = np.array([p for fold in run["outer_folds"] for p in fold["streams"]["ridge_only"]["predictions"]])
            if not np.array_equal(signals, terminal_directions(ridge_predictions, costs["stress"])):
                raise IntegrityError("Ridge benchmark directions differ from frozen rule")
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
        "trades": int(np.count_nonzero(d)), "terminal_mse_ticks_squared": float(np.mean((np.array([p for f in run["outer_folds"] for p in f["streams"]["combined"]["predictions"]]) - actual) ** 2)),
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
    if type(prior_floor) is not int or prior_floor < 289:
        raise IntegrityError("predecessor trial burden cannot be reduced below 289")
    seen = {}
    for entry in ledger_entries:
        if set(entry) != {"trial_id", "count", "included_in_prior_floor", "configuration_ids"}:
            raise IntegrityError("trial declaration fields are incomplete")
        if type(entry["trial_id"]) is not str or not entry["trial_id"] or type(entry["count"]) is not int or entry["count"] < 1 or type(entry["included_in_prior_floor"]) is not bool:
            raise IntegrityError("invalid trial burden entry")
        if entry["trial_id"] in seen and seen[entry["trial_id"]] != dict(entry):
            raise IntegrityError("conflicting duplicate trial declaration")
        seen[entry["trial_id"]] = dict(entry)
    new_ids = {f"{design()['mechanism_id']}/{m}/{c['candidate_id']}" for m in MARKETS for c in candidate_definitions()}
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
            "new_configurations": 180, "already_registered_successor_configurations": len(registered_new),
            "combined_burden": prior_floor + extra + 180, "ledger_entries": sorted(seen.values(), key=lambda e: e["trial_id"])}
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
        added_mean = added_lower = None
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
            differential = y - np.array(r["baseline_daily_stress_net_ticks"]["ridge_only"])
            try:
                added = newey_west_mean(differential, lag=5)
                added_mean = added.mean
                added_lower = added.mean - settings["hac_one_sided_z"] * added.standard_error
                if (added.status != "OK" or added.mean <= .01
                    or added_lower <= 0
                    or adjusted[f"{market}:ridge_only"] > .05):
                    reasons.append("NO_CREDIBLE_ADDED_VALUE_OVER_RIDGE")
            except ResearchContractError:
                reasons.append("ADDED_VALUE_UNAVAILABLE")
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
                             "added_value_mean_ticks": added_mean, "added_value_hac_lower_ticks": added_lower,
                             "added_value_adjusted_p": adjusted[f"{market}:ridge_only"], "ridge_fallback_allowed": False,
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
    if qualification["mechanism_id"] != design()["mechanism_id"]:
        raise IntegrityError("wrong qualification mechanism")
    eligible = qualification["phase_12_eligible_markets"]
    if not set(stress_results) <= set(eligible) or any(s not in {"PASS", "FAIL"} for s in stress_results.values()):
        raise IntegrityError("only qualified markets may receive one-shot stress results")
    return {"phase_12": eligible, "refit_without_evaluation": [m for m in eligible if stress_results.get(m) == "PASS"],
            "closed_after_stress": [m for m in eligible if stress_results.get(m) == "FAIL"],
            "final_holdout_access": False, "common_final_252_trade_dates": ["2025-07-14", "2026-07-13"], "authority": AUTHORITY}
