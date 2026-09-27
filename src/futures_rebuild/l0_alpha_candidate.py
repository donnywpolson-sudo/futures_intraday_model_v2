"""Fixed-window L0 development experiment; pure mechanics, no data reader.

The independent clock and forecasts precede outcome attachment. Existing
strict source certificates and historical access controls are not weakened.
"""
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import numpy as np

from . import rolling_hour_research as old
from .canonical import sha256_json
from .errors import IntegrityError
from .research.splits import SessionWindow, TemporalSamples, nested_chronological_splits

MINUTE, DELAY = old.MINUTE, old.DELAY
MARKETS = old.MARKETS
ZONE = ZoneInfo("America/Los_Angeles")
FEATURES = (*old.terminal.CORE_FEATURES, *old.terminal.LONG_NAMES)
PACKS = {"baseline": FEATURES[:5], "hour_context": FEATURES}
SCENARIOS = ("base", "adverse", "severe")
ALLOWANCES = {m: dict(zip(SCENARIOS, (3, 6, 12) if m in ("ES", "NQ") else (5, 10, 20))) for m in MARKETS}
FEE_USD = 10
bind, identity = old.profit.bind, old.profit.identity


def weekday_dates(start="2018-01-01", end="2023-01-01"):
    first, stop = date.fromisoformat(start), date.fromisoformat(end)
    if not date(2018, 1, 1) <= first < stop <= date(2023, 1, 1):
        raise IntegrityError("development dates must stay within 2018-2022")
    return tuple((first + timedelta(days=i)).isoformat() for i in range((stop - first).days)
                 if (first + timedelta(days=i)).weekday() < 5)


@lru_cache(maxsize=1310)
def decision_times(day):
    day = date.fromisoformat(day)
    if day.weekday() >= 5:
        raise IntegrityError("weekday decision date required")
    start = int(datetime.combine(day, time(6, 30), ZONE).timestamp()) * 10**9
    return tuple(start + i * MINUTE + DELAY for i in range(330))


def candidate_definitions():
    return tuple({"candidate_id": f"{pack}:ridge={penalty:g}", "family": "ridge", "pack": pack,
                  "feature_names": list(names), "penalty": penalty}
                 for pack, names in PACKS.items() for penalty in (.1, 1., 10.)) + tuple(
        {"candidate_id": f"{pack}:xgboost:depth={depth}", "family": "xgboost", "pack": pack,
         "feature_names": list(names), "max_depth": depth}
        for pack, names in PACKS.items() for depth in (2, 3))


def fold_windows(dates):
    """Freeze ranges on dates, before exclusions, features, labels or performance."""
    if len(dates) < 1010:
        raise IntegrityError("eight outer folds require at least 1010 independent dates")
    outer = [SessionWindow(len(dates) - 504 + i * 63, len(dates) - 504 + (i + 1) * 63) for i in range(8)]
    inner = [[SessionWindow(w.start - 253 + i * 63, w.start - 253 + (i + 1) * 63) for i in range(4)] for w in outer]
    return outer, inner


def frozen_ranges(dates):
    outer, inner = fold_windows(dates)
    def record(w):
        return {"start_index": w.start, "stop_index": w.stop, "first_date": dates[w.start],
                "last_date": dates[w.stop - 1], "audit_dates": list(dates[w.start:w.stop])}
    return [{"outer": record(w), "inner": [record(v) for v in nested]}
            for w, nested in zip(outer, inner, strict=True)]


def design():
    return bind({"schema_version": "l0_alpha_candidate/1", "research_markets": list(MARKETS),
        "selection_period": ["2018-01-01", "2023-01-01"], "timezone": "America/Los_Angeles",
        "calendar": "INDEPENDENT_WEEKDAYS_INCLUDING_HOLIDAYS_AND_EMPTY_DATES",
        "decision_window": ["06:30:05", "11:59:05"], "last_entry": "12:00", "last_exit": "13:00",
        "entry_calendar": "KNOWN_OPEN_THROUGH_STRICTLY_BEFORE_CLOSE;UNKNOWN_ABSTAINS;NO_FORECAST_FILTER",
        "calendar_views": ["evidence_only", "assumed_availability"],
        "entry": "NEXT_MINUTE_BOUNDARY", "holding_minutes": 60, "history_bars": 61,
        "refresh": "BETWEEN_63_WEEKDAY_OUTER_PERIODS_ONLY;QUARTERLY_RESEARCH_APPROXIMATION",
        "availability": "BAR_END_PLUS_5_SECONDS_ASSUMPTION;REVISIONS_AND_APPLICATION_DELIVERY_UNVERIFIED",
        "contract_mapping": "OBSERVED_SEQUENCE;CONDITIONAL_UNLESS_MAPPING_TIMING_VERIFIED;RESET_ON_CHANGE",
        "feature_names": list(FEATURES), "packs": {k: list(v) for k, v in PACKS.items()},
        "volume_proxy": "PRICE_DIRECTION_WEIGHTED_VOLUME_NOT_AGGRESSOR_ORDER_FLOW",
        "candidates": list(candidate_definitions()), "configuration_charges": 40,
        "training": old.design()["training"],
        "xgboost": {**old.comparison.design()["phase_5"]["xgboost"],
                    "lambda": .01, "min_child_weight": .05, "base_score": "DATE_BALANCED_TRAINING_MEAN"},
        "folds": frozen_ranges(weekday_dates()), "embargo_dates": 1,
        "purge": "EXISTING_HALF_OPEN_DATE_SPLITS_PLUS_STRICT_LABEL_MATURITY_BEFORE_FIRST_SCHEDULED_DECISION",
        "target": "SAME_CONTRACT_NEXT_MINUTE_OPEN_TO_OPEN_60_MINUTES_LATER;REFERENCE_NOT_FILL",
        "selection": "MAX_MEAN_DAILY_ADVERSE_NET_USD_COMMON_AUDIT_DATE_DENOMINATOR",
        "ties": "EXACT_SCORE;NO_TRADE_AT_ZERO;RIDGE;FEWER_FEATURES;STRONGER_PENALTY_OR_SHALLOWER_TREE;ID",
        "position": "ONE_STANDARD_CONTRACT;PENDING_AND_OPEN_OCCUPY_MARKET;SCHEDULED_EXIT",
        "missing_execution": "PRESERVE_LEDGER_AND_POSITION;STOP_MARKET;NO_REPLACEMENT_OR_COMPLETE_ECONOMICS",
        "costs": {"round_trip_allowances_ticks": ALLOWANCES, "round_trip_fee_usd": FEE_USD,
                  "claim": "FROZEN_ASSUMPTIONS_NOT_ESTIMATES_OR_UPPER_BOUNDS",
                  "fee_evidence": "MUST_RESOLVE_APPLICABLE_INDEPENDENT_EVIDENCE_BEFORE_HISTORICAL_FREEZE"},
        "comparisons": ["no_trade", "date_balanced_training_mean", "always_long", "always_short"],
        "sensitivities": "COMPLETE_BASE_AND_SEVERE_REPLAYS_SEPARATE_FROM_FIXED_ADVERSE_TRADE_REPRICING",
        "final_rule": "LAST_OUTER_INNER_WINNER_REFIT_ALL_MATURE_PERMITTED_DEVELOPMENT_LABELS;SAVE_NO_TRADE",
        "claim": "DEVELOPMENT_CANDIDATE_UNDER_DISCLOSED_ASSUMPTIONS;NO_ALPHA_OR_DEPLOYMENT_CLAIM",
        "l1": {"role": "FROZEN_VALIDATION_ONLY", "bbo_offsets_seconds": [0, 1, 5],
               "matching": "EXACT_SAME_CONTRACT_INTERVAL_END;ASK_BUYS_BID_SELLS;NO_FAVORABLE_NEAREST_SEARCH",
               "tbbo": "TRADE_CONDITIONED_ONLY", "quote_freshness": "UNKNOWN_UNLESS_LAST_UPDATE_OBSERVED",
               "costs": "QUOTE_SIDE_PNL_MINUS_FEES;ADDITIONAL_SLIPPAGE_SEPARATE;NO_DOUBLE_SPREAD",
               "protected_dates": "CONSUMED_INFORMATION_EVEN_IF_EXECUTION_ONLY"},
        "confirmation": "NOT_AUTHORIZED_OR_READY;PREDECESSOR_STATISTICAL_FAILURE_PRESERVED",
        "expansion": "CLOSE_CAMPAIGN_FIRST;SEPARATE_FROZEN_ELIGIBILITY_AND_SEARCH_DECLARATION",
        "authority": old.AUTHORITY}, "mechanism_id")


@lru_cache(maxsize=1)
def mechanism_id():
    return design()["mechanism_id"]


def feature_values(window):
    """The existing five formulas plus the existing hour return/volatility.

    Subtract integer nano prices before float conversion, including below zero.
    Caller validates the exact causal 61-bar same-contract window.
    """
    tick = window[0]["min_price_increment_nano"]
    changes = np.array([(b["close_nano"] - a["close_nano"]) / tick for a, b in zip(window, window[1:])])
    tail = window[-21:]
    high, low = max(r["high_nano"] for r in tail), min(r["low_nano"] for r in tail)
    volumes = np.array([r["volume"] for r in tail], dtype=float)
    return [(tail[-1]["close_nano"] - tail[0]["close_nano"]) / tick, float(np.std(changes[-20:])),
            2 * (tail[-1]["close_nano"] - low) / (high - low) - 1 if high != low else 0.,
            float((1 + volumes[-1]) / (1 + np.median(volumes[:-1])) - 1),
            float(np.sign(changes[-20:]) @ volumes[1:] / volumes[1:].sum()),
            (window[-1]["close_nano"] - window[0]["close_nano"]) / tick, float(np.std(changes))]


def validate_rows(rows, market, day, source_contract_id, source_release_id):
    """Numerical validation without claiming calendar or delivery certification."""
    old.validate_fixed_hour_rows(rows)
    previous = -1
    for row in rows:
        if (row.get("market") != market or row.get("project_trade_date") != day
                or row.get("source_cadence") != "1m" or row.get("source_contract_id") != source_contract_id
                or row.get("source_release_id") != source_release_id or not row.get("actual_contract")
                or row["bar_start_ns"] <= previous or row["bar_start_ns"] % MINUTE):
            raise IntegrityError("source identity, cadence, date or ordering differs")
        for key in ("row_id", "source_row_sha256", "source_file_sha256", "definition_source_file_sha256", "definition_row_sha256"):
            old.profit.digest(row.get(key))
        for key in ("open_nano", "high_nano", "low_nano", "close_nano"):
            if not -(2**63) < row[key] < 2**63 - 1:
                raise IntegrityError("price exceeds canonical signed int64 range")
        previous = row["bar_start_ns"]


def validate_events(events):
    for event in events:
        if event.get("state") not in ("OPEN", "CLOSED", "HALTED", "NO_TRADES", "SOURCE_OUTAGE"):
            raise IntegrityError("unknown coverage or status event")
        for key in ("start_ns", "end_ns", "known_at_ns"):
            old.profit.integer(event.get(key), minimum=0)
        if event["end_ns"] <= event["start_ns"]:
            raise IntegrityError("invalid event interval")
        old.profit.digest(event.get("evidence_sha256"))


def causal_state(events, at):
    known = [e for e in events if e["known_at_ns"] <= at and e["start_ns"] <= at < e["end_ns"]
             and e["state"] in ("OPEN", "CLOSED", "HALTED")]
    if not known:
        return None
    latest = max(e["known_at_ns"] for e in known)
    states = {e["state"] for e in known if e["known_at_ns"] == latest}
    if len(states) != 1:
        raise IntegrityError("ambiguous known-at-time trading state")
    return states.pop()


def build_decisions(market, day, rows, *, as_of_ns, events=()):
    """No future target or execution price is an input to eligibility."""
    validate_events(events)
    indexed = {r["bar_start_ns"]: r for r in rows}
    records, features = [], []
    for at in decision_times(day):
        end = at - DELAY
        record = {"forecast_id": sha256_json([mechanism_id(), market, at]), "market": market,
                  "trade_date": day, "decision_ns": at, "entry_ns": end + MINUTE,
                  "exit_ns": end + 61 * MINUTE, "status": "UNAVAILABLE_HISTORY",
                  "actual_contract": None, "tick_nano": None, "tick_value_nano_usd": None}
        values = [0.] * len(FEATURES)
        state = causal_state(events, at)
        if at > as_of_ns:
            record["status"] = "DECISION_NOT_REACHED"
        elif state in ("CLOSED", "HALTED"):
            record["status"] = "VERIFIED_CLOSED" if state == "CLOSED" else "CAUSAL_HALT_ABSTENTION"
        else:
            history = [indexed[t] for t in range(end - 61 * MINUTE, end, MINUTE) if t in indexed]
            if len(history) != 61:
                coverage = {e["state"] for e in events if e["start_ns"] < end and end - 61 * MINUTE < e["end_ns"]
                            and e["state"] in ("NO_TRADES", "SOURCE_OUTAGE")}
                # Retrospective diagnostics explain absence; they never change the decision.
                record["coverage"] = sorted(coverage) or ["UNRESOLVED_COVERAGE"]
            elif len({r["actual_contract"] for r in history}) != 1:
                record["status"] = "CONTRACT_HISTORY_RESET"
            elif any(type(r.get("definition_received_at_ns")) is not int or r["definition_received_at_ns"] > at
                     or max(r["decision_eligible_at_ns"], r["bar_end_ns"] + DELAY) > at for r in history):
                record["status"] = "UNAVAILABLE_HISTORY_OR_METADATA"
            elif len({(r["min_price_increment_nano"], r["multiplier_nano"]) for r in history}) != 1:
                record["status"] = "CONTRACT_UNITS_RESET"
            else:
                units = history[-1]["min_price_increment_nano"] * history[-1]["multiplier_nano"]
                if units % 10**9:
                    raise IntegrityError("tick dollars are not exactly representable in nano USD")
                record.update(status="ELIGIBLE", actual_contract=history[-1]["actual_contract"],
                              tick_nano=history[-1]["min_price_increment_nano"], tick_value_nano_usd=units // 10**9)
                values = feature_values(history)
                if not np.isfinite(values).all():
                    raise IntegrityError("nonfinite feature values")
        records.append(record)
        features.append(values)
    return records, features


def attach_outcomes(decisions, rows, *, as_of_ns):
    indexed = {r["bar_start_ns"]: r for r in rows}
    outcomes = []
    for record in decisions:
        outcome = {"status": "NO_FORECAST", "target_ticks": None, "known_at_ns": None}
        if record["status"] == "ELIGIBLE":
            endpoints = []
            for side in ("entry", "exit"):
                at = record[side + "_ns"]
                row = indexed.get(at)
                status, price = "MISSING_REFERENCE", None
                if as_of_ns < at + MINUTE + DELAY:
                    status = "UNMATURED"
                elif row is not None:
                    if row["actual_contract"] != record["actual_contract"]:
                        status = "CONTRACT_MISMATCH"
                    elif (row["min_price_increment_nano"] != record["tick_nano"] or
                          row["min_price_increment_nano"] * row["multiplier_nano"] != record["tick_value_nano_usd"] * 10**9):
                        status = "CONTRACT_UNITS_MISMATCH"
                    elif row["decision_eligible_at_ns"] > as_of_ns:
                        status = "UNMATURED"
                    else:
                        status, price = "RESOLVED", row["open_nano"]
                        endpoints.append(max(row["decision_eligible_at_ns"], at + MINUTE + DELAY))
                outcome.update({side + "_status": status, side + "_open_nano": price})
            outcome["status"] = "MATURED" if len(endpoints) == 2 else "UNAVAILABLE_TARGET"
            if len(endpoints) == 2:
                outcome.update(target_ticks=(outcome["exit_open_nano"] - outcome["entry_open_nano"]) / record["tick_nano"],
                               known_at_ns=max(endpoints))
        outcomes.append(outcome)
    return outcomes


@dataclass
class Dataset:
    market: str
    calendar_dates: tuple
    decisions: tuple
    features: np.ndarray
    outcomes: tuple
    source_facts: tuple
    as_of_ns: int

    def __post_init__(self):
        if self.market not in MARKETS or not self.calendar_dates:
            raise IntegrityError("one declared market and calendar required")
        expected = weekday_dates(self.calendar_dates[0], (date.fromisoformat(self.calendar_dates[-1]) + timedelta(days=1)).isoformat())
        if self.calendar_dates != expected:
            raise IntegrityError("independent weekday dates were filtered or reordered")
        if len(self.decisions) != 330 * len(expected) or len(self.outcomes) != len(self.decisions):
            raise IntegrityError("complete decision and outcome ledgers required")
        self.features = old.finite_float64(np.asarray(self.features, dtype=float), name="features", ndim=2)
        if self.features.shape != (len(self.decisions), len(FEATURES)):
            raise IntegrityError("unaligned feature array")
        for i, r in enumerate(self.decisions):
            day = expected[i // 330]
            at = decision_times(day)[i % 330]
            if (r["market"] != self.market or r["trade_date"] != day or r["decision_ns"] != at
                    or r["entry_ns"] != at - DELAY + MINUTE or r["exit_ns"] != at - DELAY + 61 * MINUTE
                    or r["forecast_id"] != sha256_json([mechanism_id(), self.market, at])):
                raise IntegrityError("decision ledger differs from the fixed clock")
            outcome = self.outcomes[i]
            if r["status"] == "ELIGIBLE":
                if (not isinstance(r["actual_contract"], str) or not r["actual_contract"] or
                        any(type(r[k]) is not int or r[k] <= 0 for k in ("tick_nano", "tick_value_nano_usd"))):
                    raise IntegrityError("forecast lacks exact contract units")
                if outcome["status"] == "MATURED":
                    if (any(outcome.get(side + "_status") != "RESOLVED" or
                            type(outcome.get(side + "_open_nano")) is not int or
                            outcome[side + "_open_nano"] % r["tick_nano"] for side in ("entry", "exit"))
                            or type(outcome.get("known_at_ns")) is not int
                            or not r["exit_ns"] + MINUTE + DELAY <= outcome["known_at_ns"] <= self.as_of_ns
                            or outcome["target_ticks"] != (outcome["exit_open_nano"] - outcome["entry_open_nano"]) / r["tick_nano"]):
                        raise IntegrityError("mature label differs from exact reference endpoints or availability")
                elif outcome["status"] != "UNAVAILABLE_TARGET" or outcome["target_ticks"] is not None:
                    raise IntegrityError("invalid unavailable target")
            elif outcome["status"] != "NO_FORECAST" or np.any(self.features[i]):
                raise IntegrityError("abstention cannot enter fitting or carry features")
        self.fit_positions = np.array([i for i, o in enumerate(self.outcomes) if o["status"] == "MATURED"], dtype=np.int64)
        self.terminal_data = SimpleNamespace(market=self.market,
            trade_dates=tuple(self.decisions[i]["trade_date"] for i in self.fit_positions),
            sample_ids=tuple(self.decisions[i]["forecast_id"] for i in self.fit_positions),
            features=self.features[self.fit_positions],
            terminal_ticks=np.array([self.outcomes[i]["target_ticks"] for i in self.fit_positions]))
        self.features.flags.writeable = False
        self.terminal_data.features.flags.writeable = False
        self.terminal_data.terminal_ticks.flags.writeable = False
        self.fit_positions.flags.writeable = False
        self._evidence_id = self.evidence_id()

    def evidence_id(self):
        return sha256_json([self.market, self.calendar_dates, self.decisions, self.features.tolist(),
                            self.outcomes, self.source_facts, self.as_of_ns, self.fit_positions.tolist(),
                            self.terminal_data.features.tolist(), self.terminal_data.terminal_ticks.tolist(),
                            self.terminal_data.sample_ids, self.terminal_data.trade_dates])

    def verify(self):
        if self._evidence_id != self.evidence_id():
            raise IntegrityError("dataset mutated after construction")


def assemble_dataset(market, daily_inputs, *, dates=None, as_of_ns,
                     source_contract_id=old.SYNTHETIC_SOURCE_CONTRACT_ID,
                     source_release_id=old.SYNTHETIC_SOURCE_RELEASE_ID):
    """Stream complete daily input tuples (date, rows, events) into one market.

    This pure adapter grants no reading or certification authority. Historical
    callers must enter through the existing source admission and certified gateway.
    """
    dates = weekday_dates() if dates is None else tuple(dates)
    old.profit.integer(as_of_ns, minimum=1)
    old.profit.digest(source_contract_id)
    old.profit.digest(source_release_id)
    iterator = iter(daily_inputs)
    decisions, features, outcomes, facts = [], [], [], []
    for day in dates:
        try:
            supplied, rows, events = next(iterator)
        except StopIteration as exc:
            raise IntegrityError("missing independent weekday input") from exc
        if supplied != day:
            raise IntegrityError("daily source stream was filtered or reordered")
        rows, events = list(rows), list(events)
        validate_rows(rows, market, day, source_contract_id, source_release_id)
        records, values = build_decisions(market, day, rows, as_of_ns=as_of_ns, events=events)
        decisions.extend(records)
        features.extend(values)
        # Outcome attachment is a separate pass after the complete day's decisions.
        outcomes.extend(attach_outcomes(records, rows, as_of_ns=as_of_ns))
        facts.append({"trade_date": day, "source_rows_sha256": sha256_json(rows), "rows": len(rows),
                      "events_sha256": sha256_json(events), "source_contract_id": source_contract_id,
                      "source_release_id": source_release_id,
                      "definition_timing": "ROW_METADATA_CHECKED_NOT_INDEPENDENTLY_CERTIFIED",
                      "availability": "DECLARED_5_SECOND_ASSUMPTION;HISTORICAL_REVISIONS_UNRESOLVED"})
    if next(iterator, None) is not None:
        raise IntegrityError("extra daily source input")
    return Dataset(market, dates, tuple(decisions), np.array(features), tuple(outcomes), tuple(facts), as_of_ns)


def fold_schedule(dataset):
    """Run the existing purge/embargo kernel on dates, then map fitting rows.

    All labels are same-date endpoints; strict timestamp maturity is applied
    separately. Date compression avoids a quadratic minute-by-minute purge.
    """
    dates = dataset.calendar_dates
    outer, inner = fold_windows(dates)
    ordinals = np.arange(len(dates), dtype=np.int64)
    splits = nested_chronological_splits(TemporalSamples(ordinals, ordinals, ordinals + 1, ordinals + 1),
        outer, inner, session_embargo=1, minimum_fit_samples=252, minimum_audit_samples=1)
    ordinal = {d: i for i, d in enumerate(dates)}
    fit_days = np.array([ordinal[d] for d in dataset.terminal_data.trade_dates])
    def indices(allowed, window, kind):
        at = decision_times(dates[window.start])[0]
        selected = np.flatnonzero(np.isin(fit_days, allowed) & np.array([
            dataset.outcomes[p]["known_at_ns"] < at for p in dataset.fit_positions])).astype(np.int64)
        old.training_support(dataset, selected, fit_kind=kind)
        return selected
    return tuple({"outer_window": s.test_window, "fit": indices(s.fit_indices, s.test_window, "outer"),
        "inner": tuple({"window": f.validation_window, "fit": indices(f.fit_indices, f.validation_window, "inner")}
                       for f in s.inner_folds)} for s in splits)


def source_completeness(dataset):
    """Pre-selection accounting only; missing targets never select forecasts."""
    counts = Counter(r["status"] for r in dataset.decisions)
    targets = Counter(o["status"] for o in dataset.outcomes)
    unresolved = [{"forecast_id": r["forecast_id"], "trade_date": r["trade_date"],
                   "entry": o.get("entry_status"), "exit": o.get("exit_status")}
                  for r, o in zip(dataset.decisions, dataset.outcomes, strict=True)
                  if r["status"] == "ELIGIBLE" and o["status"] != "MATURED"]
    support = []
    final_count = None
    try:
        for number, split in enumerate(fold_schedule(dataset)):
            for role, f in [("outer", split["fit"]), *((f"inner_{j}", inner["fit"]) for j, inner in enumerate(split["inner"]))]:
                support.append({"outer_fold": number, "role": role, "training_rows": len(f),
                                "observed_training_dates": len(set(dataset.terminal_data.trade_dates[i] for i in f))})
        final_dates = old.training_support(dataset, np.arange(len(dataset.fit_positions)), fit_kind="final")
        final_count = len(set(final_dates))
        status, failure = "SUPPORTED", None
    except (IntegrityError, old.ResearchContractError) as exc:
        status, failure = "INSUFFICIENT_SUPPORT", str(exc)
    return {"decisions": dict(counts), "targets": dict(targets), "unresolved_references": unresolved,
            "fold_support": support, "final_observed_training_dates": final_count,
            "support_status": status, "support_failure": failure,
            "execution_status": "UNRESOLVED_REFERENCES" if unresolved else "REFERENCE_ENDPOINTS_COMPLETE",
            "source_certified": False, "assumptions": design()["availability"]}
