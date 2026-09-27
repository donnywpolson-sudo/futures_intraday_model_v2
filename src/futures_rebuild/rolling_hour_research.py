"""Inactive minute-entry research. In-memory mechanics, never a data/trading runner.

Predecessor numerical helpers are reused, not their qualification or authority.
All public result identities bind this distinct, counted experiment.
"""
from __future__ import annotations

from dataclasses import dataclass
from collections import Counter
from functools import lru_cache

import numpy as np

from . import fixed_hour_model_comparison as comparison
from . import fixed_hour_terminal_successor as terminal
from . import profit_aligned_research as profit
from .canonical import sha256_json
from .errors import IntegrityError
from .exchange_calendar import CalendarSession, VerifiedExchangeCalendar
from .fixed_hour_multifamily_discovery import multifamily_feature_values
from .fixed_hour_outcome_foundation import SYNTHETIC_SOURCE_CONTRACT_ID, SYNTHETIC_SOURCE_RELEASE_ID
from .fixed_hour_ridge_audit_successor import strict_outcome_path, validate_fixed_hour_rows
from .fixed_hour_terminal_evaluation import coverage_report
from .research.contracts import ResearchContractError, finite_float64
from .research.splits import SessionWindow, TemporalSamples, nested_chronological_splits

MINUTE = profit.MINUTE
DELAY = 5_000_000_000
MARKETS = terminal.MARKETS
AUTHORITY = {**terminal.AUTHORITY, "publication": False, "registration": False}
PACKS = ("baseline", "context", "context_interactions")
BASELINES = tuple(comparison.design()["phase_6"]["baselines"])


class SelectedModelFailure(comparison.LeafSupportError):
    def __init__(self, message, partial_run):
        super().__init__(message)
        self.partial_run = partial_run


def candidate_definitions():
    return tuple(c for c in comparison.candidate_definitions() if c["pack"] in PACKS)


def design():
    return profit.bind({
        "schema_version": "rolling_hour_research/3.0.0",
        "state": "IMPLEMENTED_INACTIVE_SYNTHETIC_ONLY", "authority": AUTHORITY,
        "markets": list(MARKETS), "timezone": "America/Los_Angeles",
        "new_configuration_charges": 60, "candidates": list(candidate_definitions()),
        "predecessor_mechanism_id": "19fc5bc7de91d28d302d1da2a2710132c5d31007b2b5f42fbe91fa2f9d799470",
        "training": {
            "objective": "AVERAGE_OBSERVED_DATE_OF_WITHIN_DATE_SQUARED_ERROR",
            "weights": "1/(OBSERVED_TRAINING_DATES*ELIGIBLE_ROWS_ON_DATE);SUM_ONE",
            "overlap": "RETAIN_ALL_ELIGIBLE_MINUTES;NO_INDEPENDENCE_CLAIM;NO_UNIQUENESS_WEIGHTING_OR_BOOTSTRAP",
            "ridge": "SUM_WEIGHTED_SQUARED_ERROR+LAMBDA*SUM_SQUARED_STANDARDIZED_COEFFICIENTS;UNPENALIZED_INTERCEPT",
            "ridge_penalties": [.1, 1., 10.],
            "transforms": "TRAIN_ONLY_WEIGHTED_MEAN_AND_POPULATION_VARIANCE;CONSTANT_DUPLICATE_REMOVAL",
            "xgboost": {"weight_sum": 1., "lambda": .01, "alpha": 0., "gamma": 0.,
                "min_child_weight": .05, "minimum_leaf_dates": 20,
                "loss": "HALF_WEIGHTED_SQUARED_ERROR;LAMBDA_OVER_TWO_TIMES_LEAF_WEIGHT_SQUARED",
                "other_parameters": "UNCHANGED_PREDECESSOR_TWO_SHALLOW_PROFILES"},
            "minimum_inner_observed_dates": 252, "minimum_outer_and_final_observed_dates": 505,
            "support_interpretation": "PROJECT_DESIGN_MINIMA_NOT_STATISTICAL_GUARANTEES",
            "same_procedure": "INNER_OUTER_AND_FINAL_USE_FIT_MODEL",
        },
        "phase_3": {
            "hypothesis_status": "UNPROVEN_NOT_A_LITERATURE_REPLICATION",
            "question": "Can minute-refreshed OHLCV forecasts support positive daily net dollars from flat-only fixed-hour entries?",
            "competing_explanations": ["unconditional_exposure", "noise_and_search_selection", "coverage_changes", "turnover_costs"],
            "primary_claim": "POSITIVE_UNCLIPPED_MEAN_DAILY_STRESS_NET_USD",
            "current_conclusion_scope": "DESCRIPTIVE_ONLY_NO_PROFITABILITY_OR_PREDICTIVE_ALPHA_QUALIFICATION",
            "historical_evidence": "DEVELOPMENT_ONLY_NO_CENSUS_BYPASS",
            "closed_experiments": "NO_RETRY_RESCUE_OR_RETROACTIVE_REDECLARATION",
        },
        "phase_4": {
            "input_cadence": "1m", "lookback_bars": 61, "decision_cadence_minutes": 1,
            "decision_delay_ns": DELAY, "entry": "NEXT_MINUTE_BOUNDARY_AFTER_DECISION",
            "first_entry_offset_minutes": 62, "exit_minutes_after_entry": 60,
            "target": "TERMINAL_OPEN_PRINT_CHANGE_TICKS_NOT_EXECUTABLE_PNL",
            "diagnostic_excursions": "NEVER_FEATURES_TARGETS_OR_EXIT_RULES",
            "path": "SAME_VERIFIED_OPEN_INTERVAL_AND_ACTUAL_CONTRACT_INCLUDING_COMPLETE_EXIT_BAR",
            "maturity": "ALL_PATH_ROWS_DECISION_ELIGIBLE_BEFORE_LABEL_USE",
            "overlapping_labels_independent": False,
        },
        "phase_5": {
            "feature_names": list(terminal.FEATURE_NAMES), "packs": list(PACKS),
            "pack_sizes": {p: len(terminal.PACKS[p]) for p in PACKS},
            "candidate_feature_union_size": len(set(n for p in PACKS for n in terminal.PACKS[p])),
            "matched_comparisons": [
                {"without": "baseline", "with": "context", "question": "Added 60-minute history and verified interval context as a bundle"},
                {"without": "context", "with": "context_interactions", "question": "Added six fixed time-context interactions as a bundle"},
            ],
            "comparison_interpretation": "DESCRIPTIVE_OUTER_DIFFERENCES_NOT_FEATURE_CAUSALITY_OR_PROMOTION",
            "selection": "15_PREDECLARED_CANDIDATES_INNER_CHRONOLOGICAL_FOLDS_ONLY",
            "transforms": "TRAIN_ONLY_CONSTANT_DUPLICATE_REMOVAL_AND_RIDGE_STANDARDIZATION",
            "new_formulas": [], "feature_importance_driven_expansion": False,
            "ohlcv_1s": "PRESERVED_NOT_CONSUMED;SEPARATE_DECLARED_MATCHED_TEST_REQUIRED",
            "sidecars": "SOURCE_CONTRACT_CLOCK_QUALITY_ELIGIBILITY_ONLY;NO_RETROSPECTIVE_METADATA_FEATURES",
        },
        "phase_6": {
            "position": "ONE_STANDARD_CONTRACT_PER_MARKET;ENTRY_WHEN_FLAT_ONLY;NO_STACKING",
            "pending_entry": "RESERVES_MARKET_UNTIL_ENTRY_OR_ABSTENTION",
            "forecast_while_held": "RETAIN_FORECAST;NO_REENTRY_REVERSAL_OR_EXIT_CHANGE",
            "exit": "FIXED_60_MINUTES;NO_STOP_TARGET_TRAILING_OR_EXTENSION",
            "session_risk": "NO_ENTRY_UNLESS_COMPLETE_HORIZON_FITS_SAME_OPEN_INTERVAL",
            "operational_failure": "MISSING_ENTERED_EXIT_INVALIDATES_EVIDENCE;NO_ASSUMED_FILL",
            "risk_scope": "FIXED_SIZE_TIME_AND_SESSION_LIMITS;NO_LIVE_ACCOUNT_RISK_OR_DEPLOYABILITY_CLAIM",
            "entry_threshold": "PREDICTION_STRICTLY_EXCEEDS_DECISION_KNOWN_SPREAD_FEE_AND_STRESS_ALLOWANCE",
            "costs": profit.design()["execution"], "cost_policy": "REUSED_FROZEN_QUOTE_KERNEL;EXPECTED_COSTS_UNCALIBRATED",
            "selection_score": "MEAN_DAILY_SUMMED_STRESS_NET_USD_INCLUDING_ZERO_TRADE_DATES",
            "ties": "WITHIN_0.01_USD_PER_DAY;RIDGE_FEWER_COLUMNS_STRONGER_REGULARIZATION_STABLE_ID",
            "outer_folds": 8, "inner_folds": 4, "audit_dates": 63, "embargo_dates": 1,
            "purge": "TRAIN_LABELS_STRICTLY_KNOWN_BEFORE_AUDIT_DECISIONS_PLUS_WHOLE_DATE_SPLITS",
            "freeze": "FORMULAS_SEARCH_SELECTOR_POSITION_COSTS_AND_INFERENCE_BEFORE_OUTER_RESULTS",
            "refit": "SELECTED_MODEL_TRANSFORMS_REFIT_ONLY_ON_EACH_OUTER_TRAINING_SET",
            "baselines": list(BASELINES), "portfolio_claim": False,
            "calendar": "INDEPENDENT_DECLARED_SESSIONS;EMPTY_DATES_RETAINED;MISSING_DATES_REJECTED",
            "diagnostics": {"selection_effect": "NONE", "threshold_margin_band_ticks": 1.,
                "forecast_comparison": {
                    "question": "Do the profit-selected model's hour-change forecasts outperform zero change and the training-fold mean?",
                    "primary_benchmark": "zero_change", "secondary_benchmark": "training_mean",
                    "training_mean": "ROW_MEAN_OF_ELIGIBLE_OUTER_TRAINING_TARGETS;FROZEN_BEFORE_AUDIT",
                    "primary_accuracy_score": "MEAN_OF_WITHIN_DATE_MSE_TICKS_SQUARED;PER_MARKET",
                    "rows": "ALL_OUTER_FORECASTS_INCLUDING_WHILE_HELD;IDENTICAL_ROWS_FOR_ALL_STREAMS",
                    "empty_dates": "REPORT_COVERAGE;NO_ERROR_OBSERVATION_OR_ZERO_IMPUTATION",
                    "skill": "1-MODEL_MSE/BENCHMARK_MSE;NULL_WHEN_BENCHMARK_MSE_IS_ZERO",
                    "qualification": "DESCRIPTIVE_ONLY;DEPENDENCE_AND_MULTIPLE_CLAIM_INFERENCE_NOT_VALIDATED"},
                "failed_pack_refit": "REPORT_WHOLE_CONTRAST_UNAVAILABLE;NO_SELECTED_POLICY_VETO_OR_SUBSTITUTE",
                "outputs": ["forecast_error_row_and_date_means", "rmse", "threshold_proximity",
                    "daily_trade_counts", "fixed_duration_exposure", "overlapping_label_count", "forecast_comparison"]},
        },
        "ladder": {"route": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15],
            "phases_10_11": "NOT_APPLICABLE", "phase_12_15": "BLOCKED_NO_SUCCESSOR_QUALIFICATION",
            "historical_holdout_is_fresh_confirmation": False,
            "final_candidate_rule": "LAST_OUTER_FOLD_INNER_WINNER;NEVER_OUTER_SCORE_WINNER",
            "descriptive_final_refit": "SAME_CANDIDATE_ALL_ELIGIBLE_2018_2022_DECLARED_DATA;NO_NEW_EVALUATION",
            "phase_12_rule": "ONE_SHOT_2023_TO_DEVELOPMENT_CUTOFF;SAME_MINUTE_POLICY;NO_RETUNING",
            "phase_13_rule": "SAME_CANDIDATE_REFIT_2018_TO_CUTOFF;NO_EVALUATION",
            "phase_14_rule": "SEALED_HISTORICAL_EVALUATION_NOT_FRESH_PROSPECTIVE_EVIDENCE",
            "phase_15_rule": "ONLY_POST_FREEZE_OUTCOMES;FROZEN_MODEL_AND_POLICY;NO_RESTART_OR_ALPHA_RESET",
            "handoff_implementation": "DESCRIPTIVE_SELECTION_PERIOD_REFIT_IMPLEMENTED;PHASE_12_15_REMAIN_BLOCKED"},
        "statistical_qualification": qualification_status(),
    }, "mechanism_id")


@lru_cache(maxsize=1)
def mechanism_id():
    return design()["mechanism_id"]


def qualification_status():
    """No boolean, inherited certificate, or clipped-score test can open this gate."""
    return {
        "state": "BLOCKED_RAW_PNL_INFERENCE_NOT_VALIDATED",
        "disposition": "DESCRIPTIVE_ONLY;NO_JUSTIFIED_RAW_PNL_METHOD_AVAILABLE",
        "historical_use_disposition": "NO_NEW_HISTORICAL_EXECUTION;CENSUS_UNRESOLVED",
        "moment_bound": None, "dependence_model": None, "replacement_method": None,
        "qualification_ready": False, "activation_ready": False,
        "historical_census": "BLOCKED_CENSUS_RECOVERY_UNRESOLVED",
        "source_bound_readiness": "NOT_CERTIFIED", "expected_costs": "EXPECTED_COST_UNCALIBRATED",
        "failed_bootstrap": "BLOCKED_STATISTICAL_VALIDATION_FAILED",
        "bounded_score_substitution": "FORBIDDEN_DIFFERENT_ESTIMAND",
        "required_next_method": "JUSTIFIED_RAW_PNL_INFERENCE_WITH_EXPLICIT_TAIL_AND_DEPENDENCE_ASSUMPTIONS",
        "regression_requirement": "ALL_14_PREDECESSOR_SCENARIOS_2000_REPLICATIONS_EACH;NO_DROPPED_FAILURES",
        "maximum_null_simultaneous_upper_bound": .025, "minimum_strong_power_lower_bound": .8,
        "no_threshold_tuning_to_seen_simulations": True,
        "phase_12_eligible_markets": [], "authority": AUTHORITY,
    }


def opportunity_grid(session, *, calendar=None, source_contract_id=SYNTHETIC_SOURCE_CONTRACT_ID,
                     source_release_id=SYNTHETIC_SOURCE_RELEASE_ID):
    """One decision per eligible minute; session completeness is caller-certified."""
    if not isinstance(session, CalendarSession) or session.market not in MARKETS:
        raise IntegrityError("one declared market calendar session required")
    for value in (source_contract_id, source_release_id):
        profit.digest(value)
    if calendar is None:
        if (source_contract_id, source_release_id) != (SYNTHETIC_SOURCE_CONTRACT_ID, SYNTHETIC_SOURCE_RELEASE_ID):
            raise IntegrityError("real source identities require a verified calendar")
        calendar_id, authority = sha256_json({"synthetic_calendar": "rolling_hour_v1"}), "SYNTHETIC_ENGINEERING_ONLY"
    else:
        if not isinstance(calendar, VerifiedExchangeCalendar):
            raise IntegrityError("verified calendar required")
        calendar.verify()
        if calendar.sessions.get((session.market, session.trade_date)) != session:
            raise IntegrityError("session is absent from verified calendar")
        calendar_id, authority = calendar.receipt.release_id, "VERIFIED_RELEASE"
    output, previous_end = [], -1
    for interval in session.intervals:
        start = int(interval.starts_at_utc.timestamp()) * 10**9
        end = int(interval.ends_at_utc.timestamp()) * 10**9
        if start < previous_end or end <= start or start % MINUTE or end % MINUTE:
            raise IntegrityError("calendar intervals overlap or have non-minute boundaries")
        previous_end = end
        if interval.state != "OPEN":
            continue
        for anchor in range(start + 61 * MINUTE, end - 62 * MINUTE + 1, MINUTE):
            output.append(profit.bind({"schema_version": "rolling_hour_opportunity/1.0.0",
                "mechanism_id": mechanism_id(), "market": session.market, "trade_date": session.trade_date.isoformat(),
                "calendar_release_id": calendar_id, "calendar_authority": authority,
                "source_contract_id": source_contract_id, "source_release_id": source_release_id,
                "open_interval_start_ns": start, "open_interval_end_ns": end,
                "decision_ns": anchor + DELAY, "entry_ns": anchor + MINUTE,
                "exit_ns": anchor + 61 * MINUTE}, "opportunity_id"))
    return output


def validate_schedule(row):
    profit.identity(row, "opportunity_id")
    if row.get("schema_version") != "rolling_hour_opportunity/1.0.0" or row.get("mechanism_id") != mechanism_id():
        raise IntegrityError("wrong minute schedule identity")
    if row.get("market") not in MARKETS:
        raise IntegrityError("unsupported market")
    profit.ordered_dates([row.get("trade_date")])
    for key in ("calendar_release_id", "source_contract_id", "source_release_id"):
        profit.digest(row.get(key))
    if row.get("calendar_authority") not in {"VERIFIED_RELEASE", "SYNTHETIC_ENGINEERING_ONLY"}:
        raise IntegrityError("missing calendar authority")
    for key in ("open_interval_start_ns", "open_interval_end_ns", "decision_ns", "entry_ns", "exit_ns"):
        profit.integer(row.get(key), minimum=1)
    start, end = row["open_interval_start_ns"], row["open_interval_end_ns"]
    anchor = row["decision_ns"] - DELAY
    if (start % MINUTE or end % MINUTE or anchor % MINUTE or anchor < start + 61 * MINUTE
            or row["entry_ns"] != anchor + MINUTE or row["exit_ns"] != anchor + 61 * MINUTE
            or row["exit_ns"] + MINUTE > end):
        raise IntegrityError("minute schedule crosses history or session boundary")


def build_feature_record(window, scheduled):
    """Accept exactly the causal window; never inspect future outcome rows."""
    validate_schedule(scheduled)
    end = scheduled["decision_ns"] - DELAY
    if [r.get("bar_start_ns") for r in window] != list(range(end - 61 * MINUTE, end, MINUTE)):
        raise IntegrityError("features require 61 ordered consecutive minute bars")
    kwargs = {"expected_" + k: scheduled[k] for k in ("market", "trade_date", "source_contract_id", "source_release_id")}
    contract = window[0].get("actual_contract")
    validate_fixed_hour_rows(window, expected_contract=contract, **kwargs)
    if any(r["decision_eligible_at_ns"] > scheduled["decision_ns"] for r in window):
        raise IntegrityError("feature or sidecar availability is future-known")
    if len({(r["min_price_increment_nano"], r["multiplier_nano"]) for r in window}) != 1:
        raise IntegrityError("feature contract economics changed")
    values, _ = multifamily_feature_values(window[-21:], scheduled["decision_ns"], **kwargs)
    tick = window[0]["min_price_increment_nano"]
    changes = np.array([(b["close_nano"] - a["close_nano"]) / tick for a, b in zip(window, window[1:])])
    values[terminal.LONG_NAMES[0]] = (window[-1]["close_nano"] - window[0]["close_nano"]) / tick
    values[terminal.LONG_NAMES[1]] = float(np.std(changes))
    quartile = min(4, 1 + 4 * (scheduled["decision_ns"] - scheduled["open_interval_start_ns"])
                   // (scheduled["open_interval_end_ns"] - scheduled["open_interval_start_ns"]))
    values.update({name: float(quartile == q) for q, name in zip((2, 3, 4), terminal.QUARTILE_NAMES)})
    for q in (2, 3, 4):
        for name in (terminal.CORE_FEATURES[0], terminal.CORE_FEATURES[4]):
            values[f"{name}_times_q{q}"] = values[name] * float(quartile == q)
    if tuple(values) != terminal.FEATURE_NAMES or not np.isfinite(list(values.values())).all():
        raise IntegrityError("invalid feature library")
    return profit.bind({"mechanism_id": mechanism_id(), "opportunity_id": scheduled["opportunity_id"],
        "feature_values": values, "actual_contract": contract, "quartile": quartile,
        "tick_nano": tick, "tick_value_nano_usd": tick * window[0]["multiplier_nano"] // 10**9,
        "available_at_ns": max(r["decision_eligible_at_ns"] for r in window),
        "source_row_ids": [r["row_id"] for r in window],
        "source_row_sha256s": [r["source_row_sha256"] for r in window]}, "feature_id")


def build_outcome(path_rows, scheduled, *, expected_contract, as_of_ns):
    validate_schedule(scheduled)
    profit.integer(as_of_ns, minimum=1)
    base = {"mechanism_id": mechanism_id(), "opportunity_id": scheduled["opportunity_id"],
            "fit_target": None, "diagnostics": None, "target_available_at_ns": None}
    # Do not inspect or expose future path values or future quality failures.
    if as_of_ns < scheduled["exit_ns"] + MINUTE + DELAY:
        return profit.bind({**base, "status": "UNMATURED"}, "outcome_id")
    path = strict_outcome_path(path_rows, scheduled["decision_ns"], expected_contract=expected_contract,
        **{"expected_" + k: scheduled[k] for k in ("market", "trade_date", "source_contract_id", "source_release_id")})
    if path["score_status"] != "SCORABLE":
        return profit.bind({**base, "status": path["score_status"]}, "outcome_id")
    rows = [*path["path"], path["exit"]]
    available = max(r["decision_eligible_at_ns"] for r in rows)
    if available > as_of_ns:
        return profit.bind({**base, "status": "UNMATURED"}, "outcome_id")
    if len({(r["min_price_increment_nano"], r["multiplier_nano"]) for r in rows}) != 1:
        raise IntegrityError("outcome contract economics changed")
    tick, entry = rows[0]["min_price_increment_nano"], rows[0]["open_nano"]
    target = (rows[-1]["open_nano"] - entry) / tick
    up = max(0, path["extrema"]["maximum_high_nano"] - entry) / tick
    down = max(0, entry - path["extrema"]["minimum_low_nano"]) / tick
    terminal.validate_labels(*(np.array([v]) for v in (target, up, down)))
    return profit.bind({**base, "status": "MATURED", "fit_target": target,
        "target_available_at_ns": available, "tick_nano": tick,
        "tick_value_nano_usd": tick * rows[0]["multiplier_nano"] // 10**9,
        "diagnostics": {"maximum_upside_excursion_ticks": up, "maximum_downside_excursion_ticks": down},
        "source_row_ids": [r["row_id"] for r in rows],
        "source_row_sha256s": [r["source_row_sha256"] for r in rows]}, "outcome_id")


def assemble_session(session, rows, *, as_of_ns, quotes, calendar=None,
                     source_contract_id=SYNTHETIC_SOURCE_CONTRACT_ID, source_release_id=SYNTHETIC_SOURCE_RELEASE_ID):
    """Retain every scheduled decision, including invalid features/outcomes."""
    grid = opportunity_grid(session, calendar=calendar, source_contract_id=source_contract_id, source_release_id=source_release_id)
    indexed = {r["bar_start_ns"]: r for r in rows}
    if len(indexed) != len(rows):
        raise IntegrityError("duplicate source bar")
    if set(quotes) - {s["opportunity_id"] for s in grid}:
        raise IntegrityError("quote rows outside declared schedule")
    records = []
    for scheduled in grid:
        end = scheduled["decision_ns"] - DELAY
        history = [indexed[t] for t in range(end - 61 * MINUTE, end, MINUTE) if t in indexed]
        record = {"schedule": scheduled, "feature": None, "outcome": None, "status": "INVALID_FEATURE"}
        try:
            feature = build_feature_record(history, scheduled)
            record["feature"] = feature
            path = [indexed[t] for t in range(scheduled["entry_ns"], scheduled["exit_ns"] + MINUTE, MINUTE) if t in indexed]
            outcome = build_outcome(path, scheduled, expected_contract=feature["actual_contract"], as_of_ns=as_of_ns)
            record.update(outcome=outcome, status=outcome["status"])
            record["execution"] = {**scheduled, "schedule": scheduled,
                "feature_id": feature["feature_id"], "outcome_id": outcome["outcome_id"],
                "actual_contract": feature["actual_contract"], "quartile": feature["quartile"],
                "tick_nano": feature["tick_nano"], "tick_value_nano_usd": feature["tick_value_nano_usd"],
                "feature_available_at_ns": feature["available_at_ns"],
                "target_available_at_ns": outcome["target_available_at_ns"],
                **{key: quotes.get(scheduled["opportunity_id"], {}).get(key) for key in ("decision_quote", "entry_quote", "exit_quote")}}
        except IntegrityError as exc:
            record.update(status="INVALID_FEATURE" if record["feature"] is None else "INVALID_OUTCOME", error=str(exc))
        records.append(record)
    return profit.bind({"mechanism_id": mechanism_id(), "market": session.market,
        "trade_date": session.trade_date.isoformat(), "schedule_ids": [s["opportunity_id"] for s in grid],
        "open_intervals": [[int(i.starts_at_utc.timestamp()) * 10**9, int(i.ends_at_utc.timestamp()) * 10**9]
                           for i in session.intervals if i.state == "OPEN"],
        "records": records, "authority": AUTHORITY}, "session_id")


@dataclass(frozen=True)
class RollingDataset:
    """Complete session products, preserving invalid rows until explicit rejection."""
    batches: tuple[dict, ...]
    policy: dict
    declared_sessions: tuple[CalendarSession, ...]
    calendar: VerifiedExchangeCalendar | None = None
    source_contract_id: str = SYNTHETIC_SOURCE_CONTRACT_ID
    source_release_id: str = SYNTHETIC_SOURCE_RELEASE_ID

    def __post_init__(self):
        if not self.batches:
            raise IntegrityError("complete session products required")
        if not self.declared_sessions or any(not isinstance(s, CalendarSession) for s in self.declared_sessions):
            raise IntegrityError("independently declared complete calendar required")
        dates = profit.ordered_dates([s.trade_date.isoformat() for s in self.declared_sessions])
        if any(not "2018-01-01" <= day < "2023-01-01" for day in dates):
            raise IntegrityError("declared calendar exceeds the selection period")
        if [b["trade_date"] for b in self.batches] != list(dates):
            raise IntegrityError("missing, extra or reordered declared calendar dates")
        market = self.batches[0]["market"]
        execution, features, targets = [], [], []
        for batch, session in zip(self.batches, self.declared_sessions, strict=True):
            profit.identity(batch, "session_id")
            if batch["mechanism_id"] != mechanism_id() or batch["market"] != market:
                raise IntegrityError("session mechanism or market changed")
            if [r["schedule"]["opportunity_id"] for r in batch["records"]] != batch["schedule_ids"]:
                raise IntegrityError("session schedule was filtered")
            intervals = [[int(i.starts_at_utc.timestamp()) * 10**9, int(i.ends_at_utc.timestamp()) * 10**9]
                         for i in session.intervals if i.state == "OPEN"]
            expected = opportunity_grid(session, calendar=self.calendar,
                source_contract_id=self.source_contract_id, source_release_id=self.source_release_id)
            if session.market != market or batch["open_intervals"] != intervals:
                raise IntegrityError("declared calendar market or intervals differ")
            if [r["schedule"] for r in batch["records"]] != expected:
                raise IntegrityError("complete declared minute grid required")
            for record in batch["records"]:
                if record["status"] != "MATURED":
                    raise IntegrityError("incomplete features/outcomes cannot be removed from fitting")
                f, o, row = record["feature"], record["outcome"], record["execution"]
                validate_schedule(record["schedule"])
                profit.identity(f, "feature_id"); profit.identity(o, "outcome_id")
                if (f["opportunity_id"] != row["opportunity_id"] or o["opportunity_id"] != row["opportunity_id"]
                        or f["mechanism_id"] != mechanism_id() or o["mechanism_id"] != mechanism_id()
                        or row["schedule"] != record["schedule"] or row["feature_id"] != f["feature_id"] or row["outcome_id"] != o["outcome_id"]
                        or row["trade_date"] != batch["trade_date"]
                        or any(row[k] != record["schedule"][k] for k in record["schedule"])
                        or any(row[k] != f[k] for k in ("actual_contract", "quartile", "tick_nano", "tick_value_nano_usd"))
                        or row["feature_available_at_ns"] != f["available_at_ns"]
                        or row["target_available_at_ns"] != o["target_available_at_ns"]
                        or (f["tick_nano"], f["tick_value_nano_usd"]) != (o["tick_nano"], o["tick_value_nano_usd"])):
                    raise IntegrityError("feature/target/schedule alignment differs")
                features.append([f["feature_values"][n] for n in terminal.FEATURE_NAMES])
                targets.append(o["fit_target"]); execution.append(row)
        if not execution:
            raise IntegrityError("fitting needs observations; the declared calendar has none")
        tick_values = np.array([r["tick_value_nano_usd"] / 1e9 for r in execution])
        observed_dates = tuple(sorted(set(r["trade_date"] for r in execution)))
        d = terminal.TerminalDataset(market, tuple(r["opportunity_id"] for r in execution),
            tuple(r["trade_date"] for r in execution), np.array(features), np.array(targets),
            terminal.provisional_costs(market, tick_values)["stress"], observed_dates, tick_values)
        coverage = coverage_report(scheduled=len(execution), insufficient_history=0,
            valid_feature=len(execution), invalid_outcome=0, quality_errors=0)
        checked = profit.ProfitDataset(d, tuple(execution), self.policy, coverage)
        object.__setattr__(self, "terminal_data", checked.terminal_data)
        object.__setattr__(self, "opportunities", checked.opportunities)
        object.__setattr__(self, "calendar_dates", dates)
        object.__setattr__(self, "_evidence_id", self.evidence_id())

    def evidence_id(self):
        calendar = [[s.market, s.trade_date.isoformat(), [i.as_dict() for i in s.intervals]]
                    for s in self.declared_sessions]
        return sha256_json([self.batches, self.policy, calendar, self.source_contract_id, self.source_release_id])

    def verify(self):
        if self.calendar is not None:
            self.calendar.verify()
        if self._evidence_id != self.evidence_id():
            raise IntegrityError("session evidence mutated after construction")


def fold_schedule(dataset):
    """Reuse chronological splitting, with the declared calendar as its clock.

    Empty dates have no fitting rows but retain their place in audits and embargo.
    Predecessor datasets and their observed-date invariants remain unchanged.
    """
    dates = dataset.calendar_dates
    if len(dates) < 1010:
        raise IntegrityError("eight-fold WFA needs 1010 complete declared dates")
    ordinal = {d: i for i, d in enumerate(dates)}
    sessions = np.array([ordinal[d] for d in dataset.terminal_data.trade_dates], dtype=np.int64)
    samples = TemporalSamples(sessions, sessions, sessions + 1, sessions + 1)
    outer = [SessionWindow(len(dates) - 504 + i * 63, len(dates) - 504 + (i + 1) * 63) for i in range(8)]
    inner = [[SessionWindow(w.start - 253 + i * 63, w.start - 253 + (i + 1) * 63)
              for i in range(4)] for w in outer]
    folds = nested_chronological_splits(samples, outer, inner, session_embargo=1,
        minimum_fit_samples=252, minimum_audit_samples=1)
    for split in folds:
        for fold in split.inner_folds:
            training_support(dataset, fold.fit_indices, fit_kind="inner")
        training_support(dataset, split.fit_indices, fit_kind="outer")
    return folds


def training_support(dataset, indices, *, fit_kind):
    minimum = {"inner": 252, "outer": 505, "final": 505}.get(fit_kind)
    d = dataset.terminal_data
    indices = np.asarray(indices)
    if (minimum is None or indices.ndim != 1 or indices.dtype.kind not in "iu"
            or not len(indices) or np.any(indices < 0) or np.any(indices >= len(d.trade_dates))
            or np.any(np.diff(indices.astype(np.int64)) <= 0)):
        raise IntegrityError("ordered unique fitting indices and declared fit kind required")
    dates = [d.trade_dates[i] for i in indices]
    observed = len(set(dates))
    if observed < minimum:
        raise IntegrityError(f"{fit_kind} training has {observed} observed dates; requires {minimum} (rows={len(indices)})")
    return dates


def training_weights(dates):
    counts = Counter(dates)
    if not counts:
        raise IntegrityError("training weights need observed dates")
    return np.array([1. / (len(counts) * counts[day]) for day in dates])


def score_predictions(predictions, rows, policy, dates, *, forced_directions=False):
    """One chronological position per market, including pending-entry occupancy.

    Settlement can inspect future exit evidence; entry/occupancy decisions cannot.
    The immutable predecessor trades/ledger are numerical-kernel audit records.
    """
    p = finite_float64(np.asarray(predictions), name="minute predictions", ndim=1)
    if len(p) != len(rows) or (forced_directions and not np.isin(p, [-1, 0, 1]).all()):
        raise IntegrityError("unaligned predictions or invalid baseline signs")
    profit.validate_policy(policy)
    trades, decisions, occupied_until, last_decision = [], [], -1, -1
    market = rows[0]["market"] if rows else None
    for prediction, row in zip(p.tolist(), rows, strict=True):
        profit.validate_opportunity(row)
        if row["market"] != market or row["decision_ns"] <= last_decision:
            raise IntegrityError("position replay requires one ordered market")
        last_decision = row["decision_ns"]
        direction, reason = profit.decision(prediction, row, policy)
        if forced_directions and reason != "DECISION_QUOTE_ABSTENTION":
            direction = int(prediction)
        if row["decision_ns"] < occupied_until:
            direction, reason = 0, "POSITION_HELD_FORECAST_ONLY"
        # Reserve at entry independently of whether the later exit was profitable.
        if direction and profit.usable_quote(row.get("entry_quote"), at_ns=row["entry_ns"],
                market=row["market"], contract=row["actual_contract"], tick_nano=row["tick_nano"], policy=policy):
            occupied_until = row["exit_ns"]
        trade = profit._settle(direction, reason, prediction, row, policy)
        if trade["net_usd"] is None:
            raise IntegrityError("entered position lacks exit evidence; entire replay is incomplete")
        trades.append(trade)
        decisions.append({"opportunity_id": row["opportunity_id"], "decision_ns": row["decision_ns"],
            "entry_ns": row["entry_ns"], "exit_ns": row["exit_ns"], "prediction_ticks": prediction,
            "status": trade["status"], "executed_direction": trade["executed_direction"], "kernel_trade_id": trade["trade_id"]})
    ledger = profit.daily_ledger(trades, expected_ids=[r["opportunity_id"] for r in rows], dates=dates)
    return profit.bind({"mechanism_id": mechanism_id(), "policy_id": policy["policy_id"],
        "decisions": decisions, "numerical_kernel_ledger": ledger,
        "mean_daily_stress_usd": ledger["mean_daily_stress_usd"],
        "state": "DEVELOPMENT_MECHANICS_NOT_QUALIFICATION", "authority": AUTHORITY}, "replay_id")


def fit_model(dataset, indices, candidate, *, fit_kind="inner"):
    c = next((c for c in candidate_definitions() if c["candidate_id"] == candidate.get("candidate_id")), None)
    if c is None or any(candidate.get(k) != v for k, v in c.items()):
        raise IntegrityError("undeclared candidate")
    d = dataset.terminal_data
    dates = training_support(dataset, indices, fit_kind=fit_kind)
    x = finite_float64(d.features[indices], name="training features", ndim=2)
    y = finite_float64(d.terminal_ticks[indices], name="training labels", ndim=1)
    w = training_weights(dates)
    columns = terminal.retained_columns(x, c["feature_names"])
    mean = float(w @ y)
    kernel = {"candidate": c, "columns": columns, "fit_kind": fit_kind,
        "ordered_feature_names": list(terminal.FEATURE_NAMES), "dependency_versions": comparison.VERSIONS,
        "fit_sample_ids": [d.sample_ids[i] for i in indices], "observed_training_dates": len(set(dates)),
        "training_id": sha256_json([x.tolist(), y.tolist(), dates]), "training_mean_ticks": mean,
        "weight_sum": float(w.sum()), "training_convention": design()["training"]}
    if c["family"] == "ridge":
        means = w @ x[:, columns]
        scales = np.sqrt(w @ ((x[:, columns] - means) ** 2))
        z = (x[:, columns] - means) / scales
        coefficients = np.linalg.solve(z.T @ (w[:, None] * z) + c["penalty"] * np.eye(len(columns)),
            z.T @ (w * (y - mean))) if columns else np.empty(0)
        ridge = {"columns": columns, "feature_names": [terminal.FEATURE_NAMES[i] for i in columns],
            "means": means.tolist(), "scales": scales.tolist(), "weights": coefficients.tolist(),
            "intercept_ticks": mean, "candidate_id": c["candidate_id"], "target_unit": "ticks"}
        kernel["ridge"] = profit.bind(ridge, "fitted_model_id")
    elif columns:
        xgb = comparison._xgb()
        params = dict(comparison.design()["phase_5"]["xgboost"])
        for key in ("rounds", "minimum_leaf_dates", "early_stopping", "seed_search"):
            params.pop(key)
        params.update(max_depth=c["max_depth"], base_score=mean, min_child_weight=.05,
            **{"lambda": .01, "alpha": 0., "gamma": 0.})
        matrix = xgb.DMatrix(x[:, columns], label=y, weight=w)
        booster = xgb.train(params, matrix, num_boost_round=100)
        kernel.update(parameters=params, minimum_leaf_dates=comparison.check_leaf_dates(
            booster.predict(matrix, pred_leaf=True), dates), model_json=bytes(booster.save_raw(raw_format="json")).decode())
    else:
        comparison._xgb()
    return profit.bind({"mechanism_id": mechanism_id(), "kernel_model": kernel,
        "evidence_id": dataset._evidence_id, "authority": AUTHORITY}, "model_id")


def predict(model, features):
    profit.identity(model, "model_id")
    if model.get("mechanism_id") != mechanism_id():
        raise IntegrityError("wrong model mechanism")
    kernel = model["kernel_model"]
    if kernel["dependency_versions"] != comparison.VERSIONS:
        raise IntegrityError("model dependency versions differ")
    x = finite_float64(features, name="prediction features", ndim=2)
    if x.shape[1] != len(terminal.FEATURE_NAMES):
        raise IntegrityError("prediction feature width differs")
    if kernel["candidate"]["family"] == "ridge":
        return terminal.apply_fitted(kernel["ridge"], x)
    if not kernel["columns"]:
        return np.full(len(x), kernel["training_mean_ticks"])
    xgb = comparison._xgb()
    booster = xgb.Booster(params={"nthread": 1, "device": "cpu"})
    booster.load_model(bytearray(kernel["model_json"], "utf-8"))
    return finite_float64(booster.predict(xgb.DMatrix(x[:, kernel["columns"]])).astype(np.float64),
        name="predictions", ndim=1)


def run_market_wfa(dataset):
    """Select on inner policy P&L; outer outcomes never select candidates or packs."""
    if not isinstance(dataset, RollingDataset):
        raise IntegrityError("minute-session dataset required")
    dataset.verify()
    d, outputs = dataset.terminal_data, []
    for number, split in enumerate(fold_schedule(dataset)):
        scores = []
        for candidate in candidate_definitions():
            daily, counts, manifests = [], [], []
            try:
                for fold in split.inner_folds:
                    f, a = fold.fit_indices, fold.audit_indices
                    profit.check_split_availability(dataset, f, a)
                    model = fit_model(dataset, f, candidate)
                    prediction, columns = predict(model, d.features[a]), model["kernel_model"]["columns"]
                    dates = dataset.calendar_dates[fold.validation_window.start:fold.validation_window.stop]
                    replay = score_predictions(prediction, [dataset.opportunities[i] for i in a], dataset.policy, dates)
                    daily.extend(float(replay["numerical_kernel_ledger"]["daily_usd"][day]["stress"]) for day in dates)
                    counts.append(len(columns))
                    manifests.append({"audit_dates": list(dates), "fit_ids": [d.sample_ids[i] for i in f],
                        "audit_ids": [d.sample_ids[i] for i in a], "replay_id": replay["replay_id"],
                        "predictions": prediction.tolist(),
                        "daily_stress_usd": replay["numerical_kernel_ledger"]["daily_usd"]})
                scores.append({**candidate, "status": "VALID", "score": float(np.mean(daily)),
                    "retained_count": float(np.mean(counts)), "inner_manifests": manifests})
            except comparison.LeafSupportError as exc:
                scores.append({**candidate, "status": "INVALID_LEAF_SUPPORT", "reason": str(exc),
                    "completed_inner_manifests": manifests, "completed_daily_scores": daily})
        selected = comparison.choose_candidate(scores)
        chosen = {"selected": selected, **{pack: comparison.choose_candidate([s for s in scores if s["pack"] == pack]) for pack in PACKS}}
        f, a = split.fit_indices, split.audit_indices
        profit.check_split_availability(dataset, f, a)
        streams = {}
        for name, candidate in chosen.items():
            try:
                model = fit_model(dataset, f, candidate, fit_kind="outer")
            except comparison.LeafSupportError as exc:
                if name == "selected":
                    raise SelectedModelFailure(
                        f"selected outer refit failed at fold {number}: {candidate['candidate_id']}: {exc}",
                        {"completed_outer_folds": outputs, "failed_outer_fold": number,
                         "candidate_scores": scores, "selected_candidate": candidate}) from exc
                streams[name] = {"status": "INVALID_OUTER_REFIT", "reason": str(exc),
                    "candidate_id": candidate["candidate_id"], "model": None, "predictions": None}
                continue
            streams[name] = {"status": "FITTED", "model": model, "predictions": predict(model, d.features[a]).tolist()}
        outputs.append({"outer_fold": number, "fit_ids": [d.sample_ids[i] for i in f],
            "forecast_benchmarks": {"zero_change": 0., "training_mean": float(np.mean(d.terminal_ticks[f]))},
            "audit_dates": list(dataset.calendar_dates[split.test_window.start:split.test_window.stop]),
            "audit_indices": a.tolist(), "audit_ids": [d.sample_ids[i] for i in a],
            "candidate_scores": scores, "streams": streams})
    return profit.bind({"mechanism_id": mechanism_id(), "market": d.market,
        "evidence_id": dataset._evidence_id, "outer_folds": outputs,
        "state": "FROZEN_PREDICTIONS_MECHANICS_ONLY", "authority": AUTHORITY}, "run_id")


def final_refit(dataset, run):
    """Descriptive development model, not a Phase 13 or confirmation handoff."""
    dataset.verify()
    profit.identity(run, "run_id")
    if (run["mechanism_id"] != mechanism_id() or run["evidence_id"] != dataset._evidence_id
            or len(run["outer_folds"]) != 8 or run["market"] != dataset.terminal_data.market):
        raise IntegrityError("complete matching eight-fold run required for final refit")
    last = run["outer_folds"][-1]
    chosen = comparison.choose_candidate(last["candidate_scores"])
    if last["streams"]["selected"]["model"]["kernel_model"]["candidate"]["candidate_id"] != chosen["candidate_id"]:
        raise IntegrityError("final candidate differs from last inner winner")
    model = fit_model(dataset, np.arange(len(dataset.opportunities)), chosen, fit_kind="final")
    return profit.bind({"mechanism_id": mechanism_id(), "run_id": run["run_id"],
        "model": model, "candidate_rule": design()["ladder"]["final_candidate_rule"],
        "scope": "DECLARED_2018_2022_SELECTION_DATA_ONLY", "confirmation_opened": False,
        "state": "DESCRIPTIVE_RESEARCH_MODEL_NOT_QUALIFIED", "authority": AUTHORITY}, "final_id")


def evaluate_wfa(dataset, run):
    profit.identity(run, "run_id")
    if run != run_market_wfa(dataset):
        raise IntegrityError("selection, fitting, or saved predictions fail deterministic replay")
    d = dataset.terminal_data
    indices = [i for fold in run["outer_folds"] for i in fold["audit_indices"]]
    rows = [dataset.opportunities[i] for i in indices]
    dates = [day for fold in run["outer_folds"] for day in fold["audit_dates"]]
    reports = {}
    for name in ("selected", *PACKS):
        failed = [{"outer_fold": f["outer_fold"], **f["streams"][name]} for f in run["outer_folds"]
                  if f["streams"][name]["status"] != "FITTED"]
        if failed:
            # A partial pack replay would compare a selected subset of dates.
            reports[name] = {"state": "INCOMPLETE_DIAGNOSTIC_REFIT", "failed_folds": failed,
                "mean_daily_stress_usd": None, "descriptive_only": True}
        else:
            reports[name] = score_predictions([p for f in run["outer_folds"] for p in f["streams"][name]["predictions"]],
                rows, dataset.policy, dates)
    signs = {name: [] for name in BASELINES}
    for split in fold_schedule(dataset):
        a = split.audit_indices
        previous = np.sign(d.features[a, terminal.FEATURE_NAMES.index("close_change_ticks_1")])
        streams = {"flat_no_trade": np.zeros(len(a)), "always_long": np.ones(len(a)), "always_short": -np.ones(len(a)),
            "previous_bar_sign_momentum": previous, "previous_bar_sign_reversal": -previous,
            "fold_local_unconditional_direction": np.full(len(a), np.sign(np.mean(d.terminal_ticks[split.fit_indices])))}
        for name in signs:
            signs[name].extend(streams[name].tolist())
    reports.update({name: score_predictions(p, rows, dataset.policy, dates, forced_directions=True) for name, p in signs.items()})
    contrasts = []
    for c in design()["phase_5"]["matched_comparisons"]:
        values = [reports[c[k]]["mean_daily_stress_usd"] for k in ("with", "without")]
        complete = all(v is not None for v in values)
        contrasts.append({**c, "mean_daily_stress_usd_difference": values[0] - values[1] if complete else None,
            "complete": complete, "descriptive_only": True,
            "unavailable_reason": None if complete else "INCOMPLETE_DIAGNOSTIC_REFIT"})
    try:
        diagnostics = prediction_diagnostics(dataset, run, reports["selected"])
    except (IntegrityError, ResearchContractError, FloatingPointError) as exc:
        diagnostics = {"state": "INCOMPLETE_FORECAST_DIAGNOSTICS", "reason": str(exc),
            "qualification_ready": False, "selected_policy_effect": "NONE"}
    return profit.bind({"mechanism_id": mechanism_id(), "run_id": run["run_id"],
        "reports": reports, "matched_comparisons": contrasts, "qualification": qualification_status(),
        "diagnostics": diagnostics,
        "state": "DEVELOPMENT_EVIDENCE_NOT_QUALIFICATION", "authority": AUTHORITY}, "result_id")


def prediction_diagnostics(dataset, run, replay):
    """Predeclared descriptive outputs, never inputs to candidate selection."""
    indices = [i for fold in run["outer_folds"] for i in fold["audit_indices"]]
    predictions = np.array([v for fold in run["outer_folds"] for v in fold["streams"]["selected"]["predictions"]])
    errors = predictions - dataset.terminal_data.terminal_ticks[indices]
    rows = [dataset.opportunities[i] for i in indices]
    row_dates = [row["trade_date"] for row in rows]
    forecast_dates = sorted(set(row_dates))
    audit_dates = [day for fold in run["outer_folds"] for day in fold["audit_dates"]]
    streams = {"model": predictions, **{name: np.array([
        fold["forecast_benchmarks"][name] for fold in run["outer_folds"] for _ in fold["audit_indices"]])
        for name in ("zero_change", "training_mean")}}
    accuracy = {}
    for name, values in streams.items():
        if values.shape != (len(indices),):
            raise IntegrityError("forecast comparison rows are not aligned")
        with np.errstate(over="ignore", invalid="ignore"):
            squared = finite_float64((values - dataset.terminal_data.terminal_ticks[indices])**2,
                name="forecast squared errors", ndim=1)
            daily = finite_float64(terminal.daily_means(squared, row_dates), name="daily forecast MSE", ndim=1)
            mse = float(np.mean(daily))
        if not np.isfinite(mse):
            raise IntegrityError("forecast MSE is not finite")
        accuracy[name] = {"date_balanced_mse_ticks_squared": mse,
            "daily_mse_ticks_squared": dict(zip(forecast_dates, daily.tolist(), strict=True))}
    for name in ("zero_change", "training_mean"):
        baseline = accuracy[name]["date_balanced_mse_ticks_squared"]
        model = accuracy["model"]["date_balanced_mse_ticks_squared"]
        skill = 1 - model / baseline if baseline else None
        if skill is not None and not np.isfinite(skill):
            raise IntegrityError("forecast skill is not finite")
        accuracy[name].update(mse_improvement_ticks_squared=baseline - model, skill=skill,
            skill_unavailable_reason=None if baseline else "ZERO_BENCHMARK_MSE")
    near, quoted = 0, 0
    for prediction, row in zip(predictions, rows, strict=True):
        quote = row["decision_quote"]
        if not profit.usable_quote(quote, at_ns=row["decision_ns"], market=row["market"],
                contract=row["actual_contract"], tick_nano=row["tick_nano"], policy=dataset.policy):
            continue
        cost = (quote["ask_nano"] - quote["bid_nano"]) / row["tick_nano"]
        cost += dataset.policy["round_trip_fee_usd"] / (row["tick_value_nano_usd"] / 1e9)
        cost += terminal.ADVERSE_TICKS["stress"][MARKETS.index(row["market"])]
        quoted += 1
        near += int(abs(abs(prediction) - cost) <= 1.)
    ledger = replay["numerical_kernel_ledger"]
    return {"descriptive_only": True, "selection_effect": "NONE", "forecast_count": len(rows),
        "forecast_comparison": {"descriptive_only": True, "selection_effect": "NONE",
            "market": dataset.terminal_data.market, "primary_benchmark": "zero_change",
            "primary_accuracy_score": "MEAN_OF_WITHIN_DATE_MSE_TICKS_SQUARED;PER_MARKET",
            "forecast_count": len(rows), "audit_date_count": len(audit_dates),
            "forecast_date_count": len(forecast_dates),
            "empty_audit_dates": sorted(set(audit_dates) - set(forecast_dates)), "streams": accuracy,
            "qualification_ready": False,
            "qualification": "DESCRIPTIVE_ONLY;DEPENDENCE_AND_MULTIPLE_CLAIM_INFERENCE_NOT_VALIDATED"},
        "rmse_ticks": float(np.sqrt(np.mean(errors**2))), "row_mean_error_ticks": float(np.mean(errors)),
        "date_mean_error_ticks": float(np.mean(terminal.daily_means(errors, [r["trade_date"] for r in rows]))),
        "error_date_scope": "DATES_WITH_FORECASTS_ONLY;EMPTY_DATES_HAVE_NO_ERROR_OBSERVATION",
        "decision_quotes_usable": quoted, "within_one_tick_of_threshold": near,
        "executed_trades": ledger["executed_trades"], "exposure_minutes": ledger["executed_trades"] * 60,
        "zero_trade_dates": sum(n == 0 for n in ledger["daily_trade_counts"].values()),
        "overlapping_adjacent_labels": sum(a["exit_ns"] > b["entry_ns"] for a, b in zip(rows, rows[1:])),
        "independent_observation_claim": False}
