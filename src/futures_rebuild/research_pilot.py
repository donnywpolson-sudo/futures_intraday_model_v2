"""Inactive Milestone-1 research pilot. Pure in-memory inputs; no historical reader.

This is the successor implementation exercised by tests, not a demo engine.
Calendar/source permits and ready-at timestamps are supplied synthetic fixtures.
No API here registers studies, grants access, qualifies an edge, or places orders.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import calendar as civil_calendar
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np

from .canonical import sha256_json
from .discovery import bind, identity, validate_study
from .errors import IntegrityError, UnauthorizedOperation
from .research.contracts import (
    SyntheticOnlyPermit, explicit_int, explicit_real, finite_float64,
    require_synthetic_permit,
)
from .research.economics import _price_ticks

PACIFIC = ZoneInfo("America/Los_Angeles")
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
POLICIES = ("EQUAL_SESSION", "DECAY_365_25")
AGGREGATION = "EQUAL_SCORABLE_SESSION_MEAN_OF_WITHIN_SESSION_MSE"


def ns(value: datetime) -> int:
    if value.tzinfo is None:
        raise IntegrityError("timezone-aware timestamp required")
    delta = value.astimezone(UTC) - EPOCH
    return (delta.days * 86400 + delta.seconds) * 10**9 + delta.microseconds * 1000


def local_midnight(day: date) -> int:
    return ns(datetime.combine(day, datetime.min.time(), PACIFIC))


DEVELOPMENT_END = ns(datetime(2025, 7, 13, 22, tzinfo=UTC))


def protocol() -> dict:
    return bind({
        "schema_version": "research_pilot_protocol/1",
        "state": "INACTIVE_MILESTONE_1",
        "timezone": "America/Los_Angeles",
        "development_end_exclusive_ns": DEVELOPMENT_END,
        "holdout": {"first_trade_date": "2025-07-14", "last_trade_date": "2026-07-13",
                    "sessions": 252,
                    "manifest_id": "ad1baab30b52fdda95e72777a4930ea18bd78037925cc36a27594f484c876549"},
        "recent_development": {"start": "2023-01-01",
                               "claim": "DEVELOPMENT_EVIDENCE_ONLY_NOT_CURRENT_DEPLOYMENT_APPLICABILITY"},
        "training": {"years": 5, "policies": list(POLICIES), "half_life_days": 365.25,
                     "objective": "SUM_ONE_WEIGHTED_SSE_PLUS_RIDGE;UNPENALIZED_INTERCEPT"},
        "inner_selection": {"aggregation": AGGREGATION, "pool": "SCORABLE_DATES_ACROSS_FOUR_QUARTERS",
                            "tie_order": list(POLICIES)},
        "outer_evaluation": {"aggregation": AGGREGATION, "pool": "SCORABLE_DATES_ACROSS_OUTER_QUARTERS"},
        "benchmarks": {"zero": "ZERO_TICKS", "mean": "EQUAL_SESSION_TRAINING_MEAN",
                       "scoring_aggregation": AGGREGATION},
        "schedule": {"inner_quarters": 4, "embargo_open_sessions": 1,
                     "inner_tail": "TRUNCATE_AT_OUTER_SELECTION_CUTOFF",
                     "failed_update": "NO_NEW_ENTRIES;PRESERVE_EXISTING_POSITIONS"},
        "macro_disposition": "NO_ELIGIBLE_MACRO_DIVERSIFIER",
        "authority": dict(historical_read=False, activation=False, qualification=False,
                          holdout=False, forward=False, trading=False),
    }, "protocol_id")


@dataclass(frozen=True)
class Session:
    day: str
    start_ns: int
    end_ns: int
    first_decision_ns: int
    state: str = "OPEN"


@dataclass(frozen=True)
class Decision:
    sample_id: str
    market: str
    contract: str
    session: str
    decision_ns: int
    feature_available_ns: int
    features: tuple[float, ...] | None
    entry_ns: int
    exit_ns: int


@dataclass(frozen=True)
class Outcome:
    sample_id: str
    entry_contract: str
    exit_contract: str
    entry_nano: int | None
    exit_nano: int | None
    known_ns: int


@dataclass(frozen=True)
class Instrument:
    contract: str
    tick_nano: int
    tick_value_cents: int
    multiplier: int
    currency: str = "USD"

    def validate(self):
        if not self.contract or self.currency != "USD":
            raise IntegrityError("pilot requires explicit USD contract economics")
        for value in (self.tick_nano, self.tick_value_cents, self.multiplier):
            if explicit_int(value, name="contract units") <= 0:
                raise IntegrityError("positive contract units required")
        if self.tick_nano * self.multiplier * 100 != self.tick_value_cents * 10**9:
            raise IntegrityError("tick, multiplier and tick value disagree")

    def ticks(self, price: int) -> int:
        self.validate()
        value = explicit_int(price, name="price_nano")
        return _price_ticks(Decimal(value) / 10**9, Decimal(self.tick_nano) / 10**9, name="price")


@dataclass(frozen=True)
class Inputs:
    decisions: tuple[Decision, ...]
    outcomes: tuple[Outcome, ...]
    sessions: tuple[Session, ...]
    instruments: tuple[Instrument, ...]
    source_start_ns: int
    source_end_ns: int
    permit: SyntheticOnlyPermit

    def digest(self):
        core = asdict(self)
        del core["permit"]
        return sha256_json(core)

    def validate(self):
        require_synthetic_permit(self.permit)
        if self.permit.dataset_sha256 != self.digest():
            raise IntegrityError("synthetic permit does not bind exact inputs")
        if not self.source_start_ns < self.source_end_ns <= DEVELOPMENT_END:
            raise UnauthorizedOperation("pilot source envelope must remain pre-holdout")
        validate_calendar(self.sessions)
        calendar = {s.day: s for s in self.sessions}
        instruments = {v.contract: v for v in self.instruments}
        if len(instruments) != len(self.instruments) or not instruments:
            raise IntegrityError("unique contract economics required")
        for item in instruments.values():
            item.validate()
        ids = set()
        width = None
        for d in self.decisions:
            if d.sample_id in ids or d.session not in calendar or d.contract not in instruments:
                raise IntegrityError("duplicate decision or unknown session/contract")
            ids.add(d.sample_id)
            s = calendar[d.session]
            if not (self.source_start_ns <= d.decision_ns < self.source_end_ns
                    and s.state == "OPEN" and s.start_ns <= d.decision_ns < s.end_ns
                    and d.decision_ns < d.entry_ns < d.exit_ns):
                raise IntegrityError("invalid decision clock or source scope")
            if not d.sample_id or not d.market:
                raise IntegrityError("explicit decision identity and features required")
            if d.features is None:
                continue
            x = finite_float64(np.asarray(d.features, dtype=np.float64), name="features", ndim=1)
            if width is None:
                width = len(x)
            if width != len(x):
                raise IntegrityError("feature width changed")
        outcome_ids = [v.sample_id for v in self.outcomes]
        if len(set(outcome_ids)) != len(outcome_ids) or set(outcome_ids) != ids:
            raise IntegrityError("one explicit outcome disposition per decision required")
        decisions = {d.sample_id: d for d in self.decisions}
        for o in self.outcomes:
            if o.known_ns < decisions[o.sample_id].exit_ns:
                raise IntegrityError("label cannot mature before its exit")


def validate_calendar(sessions):
    if not sessions or len({s.day for s in sessions}) != len(sessions):
        raise IntegrityError("unique explicit accounting calendar required")
    previous_end = None
    for s in sessions:
        day = date.fromisoformat(s.day)
        if (s.state not in {"OPEN", "CLOSED", "UNKNOWN"} or s.start_ns != local_midnight(day)
                or s.end_ns != local_midnight(day + timedelta(days=1))
                or not s.start_ns <= s.first_decision_ns < s.end_ns
                or (previous_end is not None and s.start_ns != previous_end)):
            raise IntegrityError("complete ordered Pacific accounting dates required")
        previous_end = s.end_ns


def quarter(day: date, offset=0) -> date:
    index = day.year * 4 + (day.month - 1) // 3 + offset
    year, q = divmod(index, 4)
    return date(year, q * 3 + 1, 1)


def five_years_before(at: int) -> int:
    stamp = (EPOCH + timedelta(microseconds=at // 1000)).astimezone(PACIFIC)
    year = stamp.year - 5
    day = min(stamp.day, civil_calendar.monthrange(year, stamp.month)[1])
    return ns(stamp.replace(year=year, day=day))


def endpoint(value: str) -> int:
    return local_midnight(date.fromisoformat(value)) if len(value) == 10 else ns(datetime.fromisoformat(value))


def schedule(sessions, start: str, end: str) -> tuple[dict, ...]:
    validate_calendar(sessions)
    a = date.fromisoformat(start)
    stop_ns = endpoint(end)
    last_local = (EPOCH + timedelta(microseconds=(stop_ns - 1) // 1000)).astimezone(PACIFIC).date()
    b = last_local + timedelta(days=1)
    if a != quarter(a) or not local_midnight(a) < stop_ns <= DEVELOPMENT_END:
        raise UnauthorizedOperation("quarter start and pre-holdout exclusive end required")
    if sessions[0].day > quarter(a, -4).isoformat() or sessions[-1].end_ns < local_midnight(b):
        raise IntegrityError("calendar does not cover selection and evaluation")
    if any(s.state == "UNKNOWN" for s in sessions):
        raise IntegrityError("calendar UNKNOWN cannot be converted to an ordinary session")
    opened = [s for s in sessions if s.state == "OPEN"]

    def window(q, stop, selection_cutoff=None):
        candidates = [s for s in opened if q.isoformat() <= s.day < stop.isoformat()]
        if not candidates:
            raise IntegrityError("quarter has no declared open sessions")
        first = candidates[0]
        before = [s for s in opened if s.day < first.day]
        if not before:
            raise IntegrityError("one complete preceding open session required")
        cutoff = before[-1].start_ns
        end_ns = local_midnight(stop)
        if selection_cutoff is not None:
            end_ns = min(end_ns, selection_cutoff)
        return {"quarter": q.isoformat(), "start_ns": local_midnight(q), "stop_ns": end_ns,
                "cutoff_ns": cutoff, "lower_ns": five_years_before(cutoff),
                "activation_ns": first.first_decision_ns,
                "cutoff_pacific": (EPOCH + timedelta(microseconds=cutoff // 1000)).astimezone(PACIFIC).isoformat(),
                "activation_pacific": (EPOCH + timedelta(microseconds=first.first_decision_ns // 1000)).astimezone(PACIFIC).isoformat(),
                "embargo_session": before[-1].day,
                "partial": end_ns < local_midnight(quarter(q, 1))}
    result = []
    q = a
    while q < b:
        outer = window(q, min(quarter(q, 1), b))
        outer["stop_ns"] = min(outer["stop_ns"], stop_ns)
        outer["partial"] = outer["stop_ns"] < local_midnight(quarter(q, 1))
        inner = [window(quarter(q, i), quarter(q, i + 1), outer["cutoff_ns"]) for i in range(-4, 0)]
        result.append({"outer": outer, "inner": inner})
        q = quarter(q, 1)
    return tuple(result)


def prepare(study: Mapping | None) -> dict:
    if study and study.get("estimator") == CLOSE_ESTIMATOR:
        return close_prepare(study)
    state = {"implementation": "INACTIVE", "qualification": "UNAVAILABLE_NO_ADOPTED_METHOD",
              "historical_execution": False, "deployment": "NOT_ASSESSED",
             "chronological_stage": "GENERATED_DEVELOPMENT_MECHANICS",
             "claim": "PREDICTIVE_DESCRIPTIVE",
             "recent_claim": protocol()["recent_development"]["claim"]}
    required = {"protocol_id", "history", "kind", "markets", "route", "ridge_penalty",
                "minimum_train_sessions", "risk", "fixture_only", "clock_offsets_seconds"}
    missing = sorted(k for k in required if not study or study.get(k) is None)
    if missing:
        return {**state, "status": "MISSING_INPUTS", "missing": missing}
    if study["protocol_id"] != protocol()["protocol_id"] or study["fixture_only"] is not True:
        raise UnauthorizedOperation("only an explicit inactive synthetic pilot declaration is accepted")
    validate_study(study["history"])
    if study["kind"] != study["history"]["kind"]:
        raise IntegrityError("study kind differs from existing discovery declaration")
    if study["kind"] != "PREDICTIVE":
        return {**state, "status": "UNIMPLEMENTED_STUDY_KIND", "missing": ["claim-specific selection path"]}
    if (explicit_real(study["ridge_penalty"], name="ridge") <= 0
            or explicit_int(study["minimum_train_sessions"], name="support") < 2
            or not study["markets"] or len(set(study["markets"])) != len(study["markets"])):
        raise IntegrityError("explicit finite fixture model/support/markets required")
    offsets = study["clock_offsets_seconds"]
    if not offsets or offsets != sorted(set(offsets)) or any(type(v) is not int or v < 0 for v in offsets):
        raise IntegrityError("explicit ordered synthetic decision offsets required")
    validate_route(study["route"], study["markets"])
    Risk(**study["risk"]).validate(study["markets"])
    return {**state, "status": "READY_SYNTHETIC_ONLY", "missing": []}


def validate_route(route, markets):
    if (set(route) != {"tiers", "anchor", "required_anchor", "allow_inconclusive_expansion",
                      "macro_disposition"}
            or not route["tiers"] or any(type(t) is not int or not 0 <= t <= 3 for t in route["tiers"])
            or route["tiers"] != sorted(set(route["tiers"]))
            or route["anchor"] not in markets
            or type(route["required_anchor"]) is not bool
            or type(route["allow_inconclusive_expansion"]) is not bool
            or route["macro_disposition"] != "NO_ELIGIBLE_MACRO_DIVERSIFIER"):
        raise IntegrityError("explicit scoped route without macro rescue required")


def tier_disposition(route, tier, result, *, expansion_authorized=False):
    if tier not in route["tiers"]:
        raise IntegrityError("tier outside declared route")
    if result == "REQUIRED_FAILURE":
        return "STOP_REQUIRED_CLAIM"
    if result in {"EXECUTION_INCOMPLETE", "UNAVAILABLE_QUALIFICATION"}:
        return result
    if result not in {"INCONCLUSIVE", "DESCRIPTIVE_COMPLETE"}:
        raise IntegrityError("pilot cannot fabricate scientific qualification")
    next_tier = next((t for t in route["tiers"] if t > tier), None)
    if next_tier is None:
        return "TERMINAL_DECLARED_SCOPE_UNQUALIFIED"
    if result == "INCONCLUSIVE" and not route["allow_inconclusive_expansion"]:
        return "STOP_INSUFFICIENT_EVIDENCE"
    return "EXPLORATORY_CONTINUATION_ONLY" if expansion_authorized else "AWAIT_EXACT_EXPANSION_AUTHORITY"


def target(decision: Decision, outcome: Outcome, instrument: Instrument):
    if (outcome.entry_nano is None or outcome.exit_nano is None
            or outcome.entry_contract != decision.contract or outcome.exit_contract != decision.contract):
        return None
    return float(instrument.ticks(outcome.exit_nano) - instrument.ticks(outcome.entry_nano))


def row_weights(dates: Sequence[str], cutoff: int, policy: str) -> np.ndarray:
    if policy not in POLICIES or not dates:
        raise IntegrityError("declared weighting policy and nonempty training dates required")
    counts = Counter(dates)
    anchor = (EPOCH + timedelta(microseconds=cutoff // 1000)).astimezone(PACIFIC).date()
    ages = {d: (anchor - date.fromisoformat(d)).days for d in counts}
    if min(ages.values()) < 0:
        raise IntegrityError("future training date")
    mass = {d: 1. if policy == POLICIES[0] else 2. ** (-ages[d] / 365.25) for d in counts}
    total = sum(mass.values())
    return np.array([mass[d] / total / counts[d] for d in dates], dtype=np.float64)


def fitting_identity(study):
    # Economic costs and allocation do not affect this predictive fit.
    return sha256_json({k: study[k] for k in
                       ("protocol_id", "history", "kind", "markets", "ridge_penalty",
                        "minimum_train_sessions", "clock_offsets_seconds")})


def fit(data: Inputs, market: str, window: Mapping, policy: str, study: Mapping, *, ready_at: int) -> dict:
    if study.get("estimator") == CLOSE_ESTIMATOR:
        return close_fit(data, market, window, policy, study, ready_at=ready_at)
    data.validate()
    if prepare(study)["status"] != "READY_SYNTHETIC_ONLY":
        return {"status": "MISSING_INPUTS"}
    if not window["cutoff_ns"] <= ready_at or not window["cutoff_ns"] < window["activation_ns"]:
        raise IntegrityError("fit readiness/activation precedes its information cutoff")
    if market not in study["markets"] or window["lower_ns"] != five_years_before(window["cutoff_ns"]):
        raise IntegrityError("fit market or five-year interval differs")
    if not data.source_start_ns <= window["lower_ns"] < window["cutoff_ns"] <= min(data.source_end_ns, DEVELOPMENT_END):
        return {"status": "INSUFFICIENT_HISTORY", "market": market}
    outcomes = {v.sample_id: v for v in data.outcomes}
    instruments = {v.contract: v for v in data.instruments}
    selected = []
    for d in sorted(data.decisions, key=lambda r: (r.decision_ns, r.sample_id)):
        o = outcomes[d.sample_id]
        if (d.market == market and window["lower_ns"] <= d.decision_ns < window["cutoff_ns"]
                and d.features is not None and d.feature_available_ns <= d.decision_ns
                and o.known_ns < window["cutoff_ns"]):
            y = target(d, o, instruments[d.contract])
            if y is not None:
                selected.append((d, y))
    dates = [d.session for d, _ in selected]
    if len(set(dates)) < study["minimum_train_sessions"]:
        return {"status": "INSUFFICIENT_SUPPORT", "market": market, "sessions": len(set(dates))}
    x = finite_float64(np.array([d.features for d, _ in selected], dtype=np.float64), name="fit X", ndim=2)
    y = finite_float64(np.array([y for _, y in selected], dtype=np.float64), name="fit y", ndim=1)
    w = row_weights(dates, window["cutoff_ns"], policy)
    columns = []
    for i in range(x.shape[1]):
        if np.ptp(x[:, i]) and not any(np.array_equal(x[:, i], x[:, j]) for j in columns):
            columns.append(i)
    # Same sum-one standardized Ridge objective as the frozen L0 arithmetic;
    # its wrapper cannot accept these memberships/weights without changing legacy bindings.
    means = w @ x[:, columns]
    scales = np.sqrt(w @ ((x[:, columns] - means) ** 2))
    z = (x[:, columns] - means) / scales
    intercept = float(w @ y)
    beta = np.linalg.solve(z.T @ (w[:, None] * z) + study["ridge_penalty"] * np.eye(len(columns)),
                           z.T @ (w * (y - intercept))) if columns else np.empty(0)
    baseline_w = row_weights(dates, window["cutoff_ns"], POLICIES[0])
    if not all(np.isfinite(v).all() for v in (means, scales, beta)) or np.any(scales <= 0):
        raise IntegrityError("nonfinite or degenerate fitted transformation")
    session_mass = {}
    for d, weight in zip(dates, w):
        session_mass[d] = session_mass.get(d, 0.) + float(weight)
    return bind({"status": "FITTED", "protocol_id": protocol()["protocol_id"],
                 "study_id": fitting_identity(study), "input_id": data.digest(), "market": market,
                 "policy": policy, "cutoff_ns": window["cutoff_ns"], "lower_ns": window["lower_ns"],
                  "activation_ns": window["activation_ns"], "ready_at_ns": ready_at,
                 "evaluation_stop_ns": window["stop_ns"],
                 "feature_width": x.shape[1], "columns": columns, "means": means.tolist(),
                 "scales": scales.tolist(), "beta": beta.tolist(), "intercept": intercept,
                 "equal_session_training_mean": float(baseline_w @ y),
                 "fit_ids": [d.sample_id for d, _ in selected],
                 "latest_label_ns": max(outcomes[d.sample_id].known_ns for d, _ in selected),
                 "training_sessions": len(set(dates)), "training_rows": len(dates),
                 "session_weight_max": max(session_mass.values()),
                 "session_weight_inverse_hhi": 1. / sum(v*v for v in session_mass.values()),
                 "support_claim": "WEIGHT_CONCENTRATION_NOT_INDEPENDENT_SAMPLE_SIZE",
                 "numpy_version": np.__version__}, "model_id")


def predict(model: Mapping, decisions: Sequence[Decision]) -> tuple[dict, ...]:
    identity(model, "model_id")
    is_close = model["protocol_id"] == close_protocol()["protocol_id"]
    if (not is_close and model["protocol_id"] != protocol()["protocol_id"]) or model["numpy_version"] != np.__version__:
        raise IntegrityError("model protocol/dependency identity differs")
    records = []
    for d in decisions:
        if is_close and (d.decision_ns != close_time(d.session, 15, 29, 5)
                         or d.entry_ns != close_time(d.session, 15, 30)
                         or d.exit_ns != close_time(d.session, 16, 0)):
            raise IntegrityError("close forecast clock differs")
        if d.market != model["market"] or (d.features is not None and len(d.features) != model["feature_width"]):
            raise IntegrityError("forecast identity or feature order/width differs")
        if not model["activation_ns"] <= d.decision_ns < model["evaluation_stop_ns"] <= DEVELOPMENT_END:
            raise IntegrityError("prediction outside the model's scheduled development interval")
        status = "FORECAST"
        if model["ready_at_ns"] > model["activation_ns"]:
            status = "ABSTAIN_FAILED_UPDATE"
        elif d.decision_ns < model["activation_ns"]:
            raise IntegrityError("model used before scheduled activation")
        elif d.features is None or d.feature_available_ns > d.decision_ns:
            status = "UNAVAILABLE_CAUSAL_INPUT"
        value = None
        if status == "FORECAST":
            x = finite_float64(np.array(d.features, dtype=np.float64), name="forecast X", ndim=1)
            z = (x[model["columns"]] - np.array(model["means"])) / np.array(model["scales"])
            value = explicit_real(z @ np.array(model["beta"]) + model["intercept"], name="forecast")
        records.append(bind({"sample_id": d.sample_id, "session": d.session,
                             "decision_ns": d.decision_ns, "model_id": model["model_id"],
                             "status": status, "prediction_ticks": value,
                             "zero_ticks": 0., "mean_ticks": model["equal_session_training_mean"],
                             **({"weighted_intercept_ticks": model["intercept"], "modal_sign": model["modal_sign"],
                                 "raw_slope": model["raw_slope"], "decision_id": sha256_json(asdict(d)),
                                 "continuation": "POSITIVE_UNQUALIFIED" if model["raw_slope"] > 0
                                 else "NONPOSITIVE_NO_REVERSAL_RESCUE"} if is_close else {})},
                            "forecast_id"))
    return tuple(records)


def evaluate(records, decisions, outcomes, instruments, *, as_of: int, aggregation: str) -> dict:
    if aggregation != AGGREGATION:
        raise IntegrityError("evaluation aggregation must be declared independently of fit weights")
    dmap = {d.sample_id: d for d in decisions}
    omap = {o.sample_id: o for o in outcomes}
    units = {v.contract: v for v in instruments}
    if len(dmap) != len(decisions) or len(omap) != len(outcomes):
        raise IntegrityError("duplicate score join")
    seen, daily = set(), {}
    missing = 0
    for r in records:
        identity(r, "forecast_id")
        if r["sample_id"] in seen or r["sample_id"] not in dmap or r["sample_id"] not in omap:
            raise IntegrityError("duplicate or missing forecast/target identity")
        seen.add(r["sample_id"])
        d, o = dmap[r["sample_id"]], omap[r["sample_id"]]
        if (r["decision_ns"], r["session"]) != (d.decision_ns, d.session):
            raise IntegrityError("forecast/target timing join differs")
        if r["status"] != "FORECAST":
            continue
        y = target(d, o, units[d.contract]) if o.known_ns <= as_of else None
        if y is None:
            missing += 1
            continue
        losses = [(r[k] - y) ** 2 for k in ("prediction_ticks", "zero_ticks", "mean_ticks")]
        daily.setdefault(d.session, []).append(losses)
    means = {d: np.mean(values, axis=0).tolist() for d, values in sorted(daily.items())}
    summary = np.mean(list(means.values()), axis=0).tolist() if means else None
    return {"status": "COMPLETE" if summary is not None and not missing else "INCOMPLETE_OUTCOMES",
            "aggregation": aggregation, "mse": summary, "daily_mse": means,
            "expected_decisions": len(records), "forecasts": sum(r["status"] == "FORECAST" for r in records),
            "scored_targets": sum(len(v) for v in daily.values()), "missing_targets": missing,
            "unscorable_sessions": sorted({r["session"] for r in records} - means.keys()),
            "claim": "SYNTHETIC_DESCRIPTIVE_OBSERVED_TARGET_MSE_NOT_DIRECTION_OR_ALPHA"}


@dataclass(frozen=True)
class Risk:
    market_priority: tuple[str, ...] | list[str]
    max_quantity: int
    per_market_quantity: int
    risk_per_contract_cents: int
    max_reserved_risk_cents: int
    round_trip_fee_cents: int
    round_trip_slippage_ticks: int

    def validate(self, markets):
        if set(self.market_priority) != set(markets) or len(set(self.market_priority)) != len(self.market_priority):
            raise IntegrityError("explicit deterministic priority for every market required")
        for key in ("max_quantity", "per_market_quantity", "risk_per_contract_cents", "max_reserved_risk_cents"):
            if explicit_int(getattr(self, key), name=key) <= 0:
                raise IntegrityError("positive fixture risk limits required")
        for key in ("round_trip_fee_cents", "round_trip_slippage_ticks"):
            if explicit_int(getattr(self, key), name=key) < 0:
                raise IntegrityError("negative costs")


@dataclass(frozen=True)
class Signal:
    forecast_id: str
    model_id: str
    market: str
    contract: str
    decision_ns: int
    entry_ns: int
    exit_ns: int
    prediction_ticks: float
    quantity: int = 1


@dataclass(frozen=True)
class Reference:
    contract: str
    price_nano: int | None
    observed_ns: int
    available_ns: int


class Portfolio:
    """Persistent USD reference accounting, not a fill or funded-account engine.

    ponytail: one position per market and market-priority allocation; broader
    netting/order behavior belongs to a later declared execution policy.
    """
    def __init__(self, instruments: Sequence[Instrument], risk: Risk):
        risk.validate(risk.market_priority)
        self.risk = risk
        self.instruments = {v.contract: v for v in instruments}
        if not instruments or len(self.instruments) != len(instruments):
            raise IntegrityError("unique explicit portfolio contract units required")
        for v in instruments:
            v.validate()
        self.active = {}
        self.positions = {}
        self.events = []
        self.seen = set()
        self.last_ns = -1
        self.realized_gross_cents = 0
        self.cost_cents = 0
        self.cash_cents = 0
        self.peak_equity_cents = 0
        self.drawdown_cents = 0
        self.equity_cents = 0
        self.unresolved = None

    def _price(self, references, contract, at):
        ref = references.get(contract)
        if (ref is None or ref.contract != contract or ref.price_nano is None
                or ref.observed_ns != at or ref.available_ns > at):
            return None
        return self.instruments[contract].ticks(ref.price_nano)

    def step(self, at: int, references: Mapping[str, Reference], *, signals=(), updates=None, mark=False):
        if at <= self.last_ns:
            raise IntegrityError("portfolio clock must advance; combine simultaneous signals")
        self.last_ns = at
        if updates:
            for market, model_id in updates.items():
                if market not in self.risk.market_priority:
                    raise IntegrityError("unknown update market")
                self.active[market] = model_id  # None explicitly means failed update.
        if self.unresolved:
            return
        for market, p in list(self.positions.items()):
            side = "entry" if p["entry_ticks"] is None else "exit"
            due = p["signal"].entry_ns if side == "entry" else p["signal"].exit_ns
            if due > at:
                continue
            price = self._price(references, p["signal"].contract, at) if due == at else None
            if price is None:
                self.unresolved = {"market": market, "side": side, "at_ns": at, "reason": "MISSING_SCHEDULED_REFERENCE"}
                self.events.append(self.unresolved)
                return
            if side == "entry":
                p["entry_ticks"] = price
                self.events.append({"event": "ENTRY_REFERENCE", "market": market, "at_ns": at})
            else:
                units = self.instruments[p["signal"].contract]
                gross = p["direction"] * (price - p["entry_ticks"]) * units.tick_value_cents * p["quantity"]
                costs = (self.risk.round_trip_fee_cents
                         + self.risk.round_trip_slippage_ticks * units.tick_value_cents) * p["quantity"]
                self.realized_gross_cents += gross
                self.cost_cents += costs
                self.cash_cents += gross - costs
                self.events.append({"event": "EXIT_REFERENCE", "market": market, "at_ns": at,
                                    "originating_model_id": p["signal"].model_id,
                                    "gross_cents": gross, "cost_cents": costs, "net_cents": gross - costs})
                del self.positions[market]
        if mark:
            for market, p in self.positions.items():
                if p["entry_ticks"] is not None and self._price(references, p["signal"].contract, at) is None:
                    self.unresolved = {"market": market, "side": "mark", "at_ns": at,
                                       "reason": "MISSING_OPEN_POSITION_MARK"}
                    self.equity_cents = None
                    return
        if any(s.market not in self.risk.market_priority for s in signals):
            raise IntegrityError("unknown signal market")
        ordered = sorted(signals, key=lambda s: (self.risk.market_priority.index(s.market), s.forecast_id))
        for s in ordered:
            if (s.forecast_id in self.seen or s.decision_ns != at or s.contract not in self.instruments
                    or not at < s.entry_ns < s.exit_ns or explicit_int(s.quantity, name="quantity") <= 0):
                raise IntegrityError("duplicate/invalid signal identity, units or clock")
            self.seen.add(s.forecast_id)
            prediction = explicit_real(s.prediction_ticks, name="prediction")
            units = self.instruments[s.contract]
            threshold = self.risk.round_trip_fee_cents / units.tick_value_cents + self.risk.round_trip_slippage_ticks
            status = "PENDING_ENTRY"
            if self.active.get(s.market) != s.model_id:
                status = "ABSTAIN_FAILED_OR_STALE_UPDATE"
            elif s.market in self.positions:
                status = "OCCUPIED_FORECAST_ONLY"
            elif abs(prediction) <= threshold:
                status = "ABSTAIN_COST_THRESHOLD"
            used_quantity = sum(p["quantity"] for p in self.positions.values())
            free_quantity = self.risk.max_quantity - used_quantity
            free_risk = self.risk.max_reserved_risk_cents - used_quantity * self.risk.risk_per_contract_cents
            quantity = min(s.quantity, self.risk.per_market_quantity, free_quantity,
                           free_risk // self.risk.risk_per_contract_cents)
            if status == "PENDING_ENTRY" and quantity <= 0:
                status = "ABSTAIN_SHARED_RISK"
            if status == "PENDING_ENTRY":
                self.positions[s.market] = {"signal": s, "quantity": quantity,
                                           "direction": 1 if prediction > 0 else -1, "entry_ticks": None}
            self.events.append({"event": status, "forecast_id": s.forecast_id, "market": s.market,
                                "at_ns": at, "requested_quantity": s.quantity,
                                "quantity": quantity if status == "PENDING_ENTRY" else 0})
        if mark or not self.positions:
            equity = self.cash_cents
            for market, p in self.positions.items():
                if p["entry_ticks"] is None:
                    continue
                price = self._price(references, p["signal"].contract, at)
                if price is None:
                    self.unresolved = {"market": market, "side": "mark", "at_ns": at, "reason": "MISSING_OPEN_POSITION_MARK"}
                    self.equity_cents = None
                    return
                units = self.instruments[p["signal"].contract]
                exit_cost = self.risk.round_trip_fee_cents + self.risk.round_trip_slippage_ticks * units.tick_value_cents
                equity += (p["direction"] * (price - p["entry_ticks"]) * units.tick_value_cents - exit_cost) * p["quantity"]
            self.equity_cents = equity
            self.peak_equity_cents = max(self.peak_equity_cents, equity)
            self.drawdown_cents = max(self.drawdown_cents, self.peak_equity_cents - equity)
        else:
            self.equity_cents = None  # No invented current mark at non-accounting events.

    def result(self):
        complete = self.unresolved is None and not self.positions
        return {"complete": complete, "net_cents": self.cash_cents if complete else None,
                "realized_prefix_cents": self.cash_cents, "gross_cents": self.realized_gross_cents,
                "cost_cents": self.cost_cents, "equity_cents": self.equity_cents if not self.unresolved else None,
                "drawdown_at_supplied_marks_cents": self.drawdown_cents,
                "open_or_pending": len(self.positions), "unresolved": self.unresolved,
                "events": list(self.events),
                "claim": "SYNTHETIC_REFERENCE_ACCOUNTING;NOT_OBSERVED_FILLS_OR_INTRABAR_ACCOUNT_RISK"}


def run_pilot(data: Inputs, study: Mapping, start: str, end: str) -> dict:
    """Actual inactive successor: nested fitting -> saved forecasts -> scoring -> joint replay."""
    if study.get("estimator") == CLOSE_ESTIMATOR:
        return run_close_pilot(data, study, start, end)
    readiness = prepare(study)
    if readiness["status"] != "READY_SYNTHETIC_ONLY":
        return readiness
    data.validate()
    if set(d.market for d in data.decisions) != set(study["markets"]):
        raise IntegrityError("input market population differs from study")
    plans = schedule(data.sessions, start, end)
    end_ns = endpoint(end)
    first_inner = plans[0]["inner"][0]["start_ns"]
    expected = {(market, sess.day, sess.first_decision_ns + offset * 10**9)
                for sess in data.sessions if first_inner <= sess.first_decision_ns < end_ns and sess.state == "OPEN"
                for market in study["markets"] for offset in study["clock_offsets_seconds"]
                if sess.first_decision_ns + offset * 10**9 < end_ns}
    actual = [(d.market, d.session, d.decision_ns) for d in data.decisions if first_inner <= d.decision_ns < end_ns]
    if len(actual) != len(set(actual)) or set(actual) != expected:
        return {**readiness, "status": "MISSING_OR_DUPLICATE_OPPORTUNITIES",
                "expected": len(expected), "actual": len(actual)}
    fits, all_records, selections, by_market_records = [], [], [], {}
    updates = {}
    ordered_decisions = sorted(data.decisions, key=lambda d: (d.decision_ns, d.market, d.sample_id))
    for plan in plans:
        outer = plan["outer"]
        for market in study["markets"]:
            candidates = []
            for policy in POLICIES:
                records = []
                for inner in plan["inner"]:
                    model = fit(data, market, inner, policy, study, ready_at=inner["activation_ns"])
                    if model["status"] != "FITTED":
                        return {**readiness, "status": model["status"], "fit": model, "completed_fits": len(fits)}
                    fits.append(model)
                    decisions = [d for d in ordered_decisions if d.market == market
                                 and inner["start_ns"] <= d.decision_ns < inner["stop_ns"]]
                    records.extend(predict(model, decisions))
                score = evaluate(records, data.decisions, data.outcomes, data.instruments,
                                 as_of=outer["cutoff_ns"] - 1,
                                 aggregation=protocol()["inner_selection"]["aggregation"])
                if score["status"] != "COMPLETE":
                    return {**readiness, "status": "INNER_EVIDENCE_INCOMPLETE", "score": score,
                            "completed_fits": len(fits)}
                candidates.append({"policy": policy, "score": score})
            winner = min(candidates, key=lambda c: (c["score"]["mse"][0], POLICIES.index(c["policy"])))
            model = fit(data, market, outer, winner["policy"], study, ready_at=outer["activation_ns"])
            if model["status"] != "FITTED":
                return {**readiness, "status": model["status"], "fit": model, "completed_fits": len(fits)}
            fits.append(model)
            decisions = [d for d in ordered_decisions if d.market == market
                         and outer["start_ns"] <= d.decision_ns < outer["stop_ns"]]
            records = predict(model, decisions)
            all_records.extend(records)
            by_market_records.setdefault(market, []).extend(records)
            updates.setdefault(outer["activation_ns"], {})[market] = model["model_id"]
            selections.append({"quarter": outer["quarter"], "market": market, "candidates": candidates,
                                "winner": winner["policy"], "model_id": model["model_id"],
                               "inner_tail_excluded_decisions": sum(
                                   d.market == market and outer["cutoff_ns"] <= d.decision_ns < outer["start_ns"]
                                   for d in ordered_decisions)})
    # The forecast record contains no target/exit observation. Outcomes join only after prediction.
    forecasts = {r["sample_id"]: r for r in all_records}
    outcomes = {o.sample_id: o for o in data.outcomes}
    frames = {}
    for at, value in updates.items():
        frames.setdefault(at, {"signals": [], "references": {}, "updates": {}})["updates"].update(value)
    for d in ordered_decisions:
        if d.sample_id not in forecasts:
            continue
        r = forecasts[d.sample_id]
        frame = frames.setdefault(d.decision_ns, {"signals": [], "references": {}, "updates": {}})
        if r["status"] == "FORECAST":
            frame["signals"].append(Signal(r["forecast_id"], r["model_id"], d.market, d.contract,
                                            d.decision_ns, d.entry_ns, d.exit_ns, r["prediction_ticks"]))
        o = outcomes[d.sample_id]
        for at, contract, price in ((d.entry_ns, o.entry_contract, o.entry_nano),
                                    (d.exit_ns, o.exit_contract, o.exit_nano)):
            refs = frames.setdefault(at, {"signals": [], "references": {}, "updates": {}})["references"]
            reference = Reference(contract, price, at, at)
            if contract in refs and refs[contract] != reference:
                raise IntegrityError("conflicting simultaneous reference observations")
            refs[contract] = reference
    book = Portfolio(data.instruments, Risk(**study["risk"]))
    for at, frame in sorted(frames.items()):
        if at >= end_ns:
            break
        book.step(at, frame["references"], signals=frame["signals"], updates=frame["updates"])
    metrics = {market: evaluate(records, data.decisions, data.outcomes, data.instruments,
                               as_of=min(data.source_end_ns, end_ns),
                               aggregation=protocol()["outer_evaluation"]["aggregation"])
               for market, records in by_market_records.items()}
    recent_metrics = {market: evaluate([r for r in records if r["session"] >= "2023-01-01"],
                                       data.decisions, data.outcomes, data.instruments,
                                       as_of=min(data.source_end_ns, end_ns),
                                       aggregation=protocol()["outer_evaluation"]["aggregation"])
                      for market, records in by_market_records.items()}
    complete = all(m["status"] == "COMPLETE" for m in metrics.values()) and book.result()["complete"]
    return bind({"status": "COMPLETE_SYNTHETIC_MECHANICS" if complete else "INCOMPLETE_EVIDENCE",
                 "protocol": protocol(),
                 "study_id": sha256_json(study), "input_id": data.digest(), "readiness": readiness,
                 "schedule": plans, "fit_count": len(fits), "fits": fits, "selections": selections,
                  "forecasts": all_records, "outer_metrics": metrics, "recent_metrics": recent_metrics,
                 "portfolio": book.result(), "tier_route": study["route"],
                 "qualifications": dict(predictive="NOT_QUALIFIED", economic="UNAVAILABLE_NO_ADOPTED_METHOD",
                                        incremental="NOT_ASSESSED", deployment="NOT_ASSESSED"),
                 "test_doubles": ["supplied generated accounting calendar and source history",
                                  "simulated scheduled model ready-at timestamps",
                                  "opening-price reference fills with declared synthetic costs"],
                 "recent_development_claim": protocol()["recent_development"]["claim"]}, "run_id")


# Versioned ES-only predictive integration. M1 protocol and economics stay unchanged.
CLOSE_ESTIMATOR = "ES_CLOSE_WLS_V1"
CLOSE_DECLARATION = "4a1ea185dc0aa36b8ead679cbdf7bee464920ef5b1d1d2e972e9cae34887acc7"
NEW_YORK = ZoneInfo("America/New_York")


def close_protocol():
    value = protocol()
    del value["protocol_id"]
    value.update(schema_version="research_pilot_close_protocol/1",
                 state="INACTIVE_SYNTHETIC_CLOSE_INTEGRATION", estimator=CLOSE_ESTIMATOR,
                 declaration_sha256=CLOSE_DECLARATION, event_timezone="America/New_York")
    value["training"]["objective"] = "SUM_ONE_WEIGHTED_SSE;UNPENALIZED_INTERCEPT;ONE_PREDICTOR"
    value["clock"] = {"previous": "PREVIOUS_CASH_SESSION_15:59_CLOSE",
                      "signal": "15:28_CLOSE", "decision": "15:29:05",
                      "entry": "15:30_OPEN", "exit": "16:00_OPEN",
                      "minimum_bar_delay_seconds": 5}
    return bind(value, "protocol_id")


def close_prepare(study):
    state = {"implementation": "INACTIVE", "historical_execution": False,
             "qualification": "UNAVAILABLE_NO_ADOPTED_METHOD", "deployment": "NOT_ASSESSED",
             "economic": "UNAVAILABLE_UNBOUND_POLICY", "claim": "PREDICTIVE_DESCRIPTIVE",
             "recent_claim": protocol()["recent_development"]["claim"]}
    required = ("protocol_id", "estimator", "history", "kind", "markets", "route",
                "fixture_only", "minimum_train_sessions", "support_scope")
    missing = [k for k in required if not study or study.get(k) is None]
    if missing:
        return {**state, "status": "MISSING_INPUTS", "missing": missing}
    if (study["protocol_id"] != close_protocol()["protocol_id"]
            or study["estimator"] != CLOSE_ESTIMATOR or study["fixture_only"] is not True
            or study["support_scope"] != "SYNTHETIC_ONLY"):
        raise UnauthorizedOperation("only the declared synthetic close integration is accepted")
    validate_study(study["history"])
    if (study["kind"] != "PREDICTIVE" or study["history"]["kind"] != "PREDICTIVE"
            or study["history"]["declaration"]["sha256"] != CLOSE_DECLARATION
            or study["markets"] != ["ES"]):
        raise IntegrityError("close declaration, kind or terminal ES scope differs")
    route = study["route"]
    validate_route(route, ["ES"])
    if route["tiers"] != [0] or not route["required_anchor"] or route["allow_inconclusive_expansion"]:
        raise IntegrityError("ES Tier 0 specialist has no expansion or rescue")
    if explicit_int(study["minimum_train_sessions"], name="fixture support") < 2:
        raise IntegrityError("at least two distinct fixture sessions required")
    if any(k in study for k in ("ridge_penalty", "clock_offsets_seconds")):
        raise IntegrityError("no Ridge penalty or alternative clock in close declaration")
    return {**state, "status": "READY_SYNTHETIC_ONLY", "missing": [],
            "historical_missing": ["SOURCE_ROLE_ADMISSION", "CALENDAR_AND_CONTRACT_EVIDENCE",
                                   "EXACT_READ_AUTHORITY", "REAL_SUPPORT_AND_QUALIFICATION"]}


def close_time(day, hour, minute, second=0):
    d = date.fromisoformat(day)
    return ns(datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=NEW_YORK))


@dataclass(frozen=True)
class CashDay:
    day: str
    state: str  # FULL, SHORT, CLOSED, UNKNOWN; supplied evidence, never inferred from bars.
    known_ns: int | None
    evidence_id: str


@dataclass(frozen=True)
class CloseContract:
    day: str
    contract: str
    instrument_id: int
    publisher_id: int
    specification_id: str
    definition_available_ns: int | None
    selected_available_ns: int | None
    selection_evidence_id: str
    listed_ns: int
    expires_ns: int


@dataclass(frozen=True)
class CloseBar:
    contract: str
    instrument_id: int
    publisher_id: int
    specification_id: str
    start_ns: int
    end_ns: int
    source_timestamp_ns: int
    available_ns: int | None
    eligible_ns: int | None
    definition_available_ns: int | None
    open_nano: int | None
    close_nano: int | None
    source_id: str
    row_id: str


@dataclass(frozen=True)
class CloseInputs(Inputs):
    opportunities: tuple[dict, ...]
    source_identity: str
    calendar_identity: str
    vintage_assumption: str
    source_record_digest: str

    def validate(self):
        super().validate()
        if not all((self.source_identity, self.calendar_identity, self.vintage_assumption, self.source_record_digest)):
            raise IntegrityError("explicit close source/calendar/vintage bindings required")
        days = [v["session"] for v in self.opportunities]
        if days != [s.day for s in self.sessions]:
            raise IntegrityError("complete ordered outcome-independent opportunity calendar required")
        if self.sessions[0].start_ns > self.source_start_ns or self.sessions[-1].end_ns < self.source_end_ns:
            raise IntegrityError("close calendar must cover its declared source envelope")
        expected = {v["sample_id"] for v in self.opportunities if v["has_decision"]}
        if expected != {d.sample_id for d in self.decisions}:
            raise IntegrityError("close opportunity/decision membership differs")
        if len({d.session for d in self.decisions}) != len(self.decisions):
            raise IntegrityError("one close decision per session required")
        opmap = {o["sample_id"]: o for o in self.opportunities}
        for d in self.decisions:
            op = opmap[d.sample_id]
            if (op["session"], op["decision_ns"], op["contract"]) != (d.session, d.decision_ns, d.contract):
                raise IntegrityError("close opportunity identity differs")
            if (d.features is not None) != (op["input_status"] == "ELIGIBLE_INPUT"):
                raise IntegrityError("close input disposition differs from features")
            if (d.market != "ES" or d.decision_ns != close_time(d.session, 15, 29, 5)
                    or d.entry_ns != close_time(d.session, 15, 30)
                    or d.exit_ns != close_time(d.session, 16, 0)
                    or (d.features is not None and len(d.features) != 1)):
                raise IntegrityError("close market, clock or single predictor differs")


def close_source_digest(cash_days, choices, bars, instruments, **scope):
    return sha256_json({"cash": [asdict(v) for v in cash_days],
                       "choices": [asdict(v) for v in choices],
                       "bars": [asdict(v) for v in bars],
                       "instruments": [asdict(v) for v in instruments], "scope": scope})


def close_inputs(cash_days, choices, bars, instruments, *, source_start_ns, source_end_ns,
                 source_identity, vintage_assumption, permit):
    """Pure digest-bound synthetic transformation; no reader or historical access."""
    require_synthetic_permit(permit)
    source_digest = close_source_digest(cash_days, choices, bars, instruments,
                                       source_start_ns=source_start_ns, source_end_ns=source_end_ns,
                                       source_identity=source_identity, vintage_assumption=vintage_assumption)
    if permit.dataset_sha256 != source_digest:
        raise IntegrityError("synthetic permit does not bind exact supplied close records")
    if not source_start_ns < source_end_ns <= DEVELOPMENT_END:
        raise UnauthorizedOperation("close source envelope must remain pre-holdout")
    cash = sorted(cash_days, key=lambda c: c.day)
    if len({c.day for c in cash}) != len(cash):
        raise IntegrityError("duplicate cash date")
    if any(c.state not in {"FULL", "SHORT", "CLOSED", "UNKNOWN"} for c in cash):
        raise IntegrityError("unknown cash disposition")
    selected = {c.day: c for c in choices}
    units = {v.contract: v for v in instruments}
    if len(selected) != len(choices) or len(units) != len(instruments) or not units:
        raise IntegrityError("duplicate choice or contract")
    for unit in units.values():
        unit.validate()
    lookup = {}
    for bar in bars:
        lookup.setdefault((bar.contract, bar.start_ns), []).append(bar)

    def reference(choice, stamp, field):
        found = lookup.get((choice.contract, stamp), [])
        if len(found) != 1:
            return None, None, "MISSING_EXACT_BAR" if not found else "AMBIGUOUS_EXACT_BAR"
        bar = found[0]
        if (type(bar.instrument_id) is not int or type(bar.publisher_id) is not int
                or (bar.instrument_id, bar.publisher_id, bar.specification_id)
                != (choice.instrument_id, choice.publisher_id, choice.specification_id)
                or bar.source_id != source_identity or not bar.row_id
                or bar.source_timestamp_ns != stamp or bar.end_ns != stamp + 60*10**9):
            return None, None, "INVALID_BAR_IDENTITY_OR_INTERVAL"
        availability = (bar.available_ns, bar.eligible_ns, bar.definition_available_ns,
                        choice.definition_available_ns)
        if any(type(t) is not int for t in availability):
            return None, None, "UNKNOWN_BAR_AVAILABILITY"
        if (not bar.end_ns <= bar.available_ns <= bar.eligible_ns
                or bar.definition_available_ns > bar.available_ns):
            return None, None, "INVALID_BAR_AVAILABILITY"
        at = max(stamp + 65*10**9, *availability)
        price = getattr(bar, field)
        if price is None:
            return None, at, "MISSING_REFERENCE_PRICE"
        try:
            units[choice.contract].ticks(price)
        except (ValueError, IntegrityError):
            return None, at, "INVALID_REFERENCE_PRICE"
        return price, at, "OBSERVED_REFERENCE"

    sessions, decisions, outcomes, ledger = [], [], [], []
    previous_cash = None
    # Unknown calendar dates break the predecessor chain; SHORT is retained, never skipped.
    for c in cash:
        day = date.fromisoformat(c.day)
        at, entry, exit_ = (close_time(c.day, 15, 29, 5), close_time(c.day, 15, 30),
                            close_time(c.day, 16, 0))
        known = (type(c.known_ns) is int and c.known_ns <= at and bool(c.evidence_id))
        state = ("UNKNOWN" if c.state == "UNKNOWN" or not known else
                 "CLOSED" if c.state == "CLOSED" else "OPEN")
        sessions.append(Session(c.day, local_midnight(day), local_midnight(day+timedelta(days=1)), at, state))
        prior = previous_cash
        if state == "UNKNOWN":
            previous_cash = None
        elif c.state != "CLOSED":
            previous_cash = c
        status = "ELIGIBLE_INPUT"
        if state == "UNKNOWN":
            status = "UNKNOWN_CALENDAR"
        elif c.state == "CLOSED":
            status = "CALENDAR_CLOSED"
        elif c.state != "FULL":
            status = "CALENDAR_SHORT"
        elif prior is None:
            status = "UNAVAILABLE_PREVIOUS_CASH_SESSION"
        elif prior.state != "FULL":
            status = "PREVIOUS_CASH_SESSION_SHORT"
        elif prior.known_ns is None or prior.known_ns > at or not prior.evidence_id:
            status = "UNKNOWN_PREVIOUS_CALENDAR"
        choice = selected.get(c.day)
        choice_ok = (choice is not None and choice.contract in units
                     and type(choice.selected_available_ns) is int
                     and type(choice.definition_available_ns) is int
                     and max(choice.selected_available_ns, choice.definition_available_ns) <= at
                     and type(choice.instrument_id) is int and choice.instrument_id > 0
                     and type(choice.publisher_id) is int and choice.publisher_id > 0
                     and bool(choice.specification_id) and bool(choice.selection_evidence_id)
                     and choice.listed_ns <= at < exit_ + 60*10**9 < choice.expires_ns)
        if status == "ELIGIBLE_INPUT" and not choice_ok:
            status = "UNAVAILABLE_CAUSAL_CONTRACT"
        sample = f"ES_CLOSE:{c.day}"
        features, input_at = None, at
        if status == "ELIGIBLE_INPUT":
            prior_stamp = close_time(prior.day, 15, 59)
            pv, pa, ps = reference(choice, prior_stamp, "close_nano")
            sv, sa, ss = reference(choice, close_time(c.day, 15, 28), "close_nano")
            if choice.listed_ns > prior_stamp:
                status = "UNAVAILABLE_PREVIOUS_REFERENCE"
            elif pv is None or sv is None:
                status = "UNAVAILABLE_PREVIOUS_REFERENCE:" + ps if pv is None else "UNAVAILABLE_SIGNAL:" + ss
            elif max(pa, sa) > at:
                status = "UNAVAILABLE_LATE_INPUT"
            else:
                input_at = max(pa, sa, choice.selected_available_ns, choice.definition_available_ns)
                features = (float(units[choice.contract].ticks(sv)-units[choice.contract].ticks(pv)),)
        # No fabricated contract for missing selection. Calendar ledger still contains the date.
        has_decision = state == "OPEN" and choice_ok and source_start_ns <= at < source_end_ns
        row = {"sample_id": sample, "session": c.day, "decision_ns": at, "input_status": status,
               "calendar_state": c.state, "has_decision": bool(has_decision),
               "contract": choice.contract if has_decision else None,
               "previous_cash_session": prior.day if prior else None,
               "input_available_ns": input_at if features is not None else None,
               "outcome_status": "NOT_APPLICABLE", "outcome_known_ns": None}
        row["information_id"] = sha256_json({
            "cash": asdict(c), "previous_cash": asdict(prior) if prior else None,
            "choice": asdict(choice) if choice else None,
            "input_bars": [asdict(v) for stamp in
                           ([close_time(prior.day, 15, 59), close_time(c.day, 15, 28)] if prior and choice else [])
                           for v in lookup.get((choice.contract, stamp), [])]})
        if has_decision:
            decision = Decision(sample, "ES", choice.contract, c.day, at, input_at, features, entry, exit_)
            decisions.append(decision)
            # Only now inspect future reference records. They cannot change the input disposition.
            ep, ea, es = reference(choice, entry, "open_nano")
            xp, xa, xs = reference(choice, exit_, "open_nano")
            known_at = max(ea, xa) if ea is not None and xa is not None else None
            row["outcome_status"] = "OBSERVED" if ep is not None and xp is not None else f"{es};{xs}"
            row["outcome_known_ns"] = known_at
            # M1 Outcome needs an integer. End is an ordering sentinel for unknown maturity,
            # never a claim of availability; null prices and the ledger retain the unknown.
            outcomes.append(Outcome(sample, choice.contract, choice.contract, ep, xp,
                                    known_at if known_at is not None else source_end_ns))
        ledger.append(row)
    result = CloseInputs(tuple(decisions), tuple(outcomes), tuple(sessions), tuple(instruments),
                         source_start_ns, source_end_ns, permit, tuple(ledger), source_identity,
                         sha256_json([asdict(c) for c in cash]), vintage_assumption, source_digest)
    # Derive an output digest only after validating the exact raw synthetic input permit.
    # The same synthetic-only flags persist; no historical capability is created.
    result = replace(result, permit=replace(permit, dataset_sha256=result.digest()))
    result.validate()
    return result


def _close_wls(x, y, w):
    """Actual one-predictor WLS kernel, also checked by independent rational fixture oracles."""
    x, y, w = (finite_float64(np.asarray(v, dtype=np.float64), name=n, ndim=1)
               for v, n in ((x, "WLS x"), (y, "WLS y"), (w, "WLS weights")))
    if len(x) < 2 or x.shape != y.shape or y.shape != w.shape or np.any(w <= 0) or not np.isclose(w.sum(), 1., rtol=0, atol=1e-12):
        raise IntegrityError("identified WLS population and normalized positive weights required")
    mean, intercept = float(w @ x), float(w @ y)
    variance = float(w @ (x-mean)**2)
    if not np.isfinite(variance) or variance <= 0:
        return {"status": "UNAVAILABLE_ZERO_VARIANCE"}
    scale = float(np.sqrt(variance))
    z = (x-mean)/scale
    beta, _, rank, _ = np.linalg.lstsq(np.sqrt(w)[:, None]*z[:, None],
                                      np.sqrt(w)*(y-intercept), rcond=None)
    if rank != 1 or not np.isfinite(beta).all():
        return {"status": "UNAVAILABLE_RANK_OR_NUMERICS"}
    return {"status": "FITTED", "means": [mean], "scales": [scale],
            "intercept": intercept, "beta": beta.tolist(), "raw_slope": float(beta[0]/scale)}


def close_fitting_identity(study):
    return sha256_json({k: study[k] for k in ("protocol_id", "estimator", "history",
                         "minimum_train_sessions", "support_scope", "markets", "kind")})


def close_fit(data, market, window, policy, study, *, ready_at):
    data.validate()
    if not isinstance(data, CloseInputs):
        raise IntegrityError("close adapter inputs required")
    if close_prepare(study)["status"] != "READY_SYNTHETIC_ONLY":
        return {"status": "MISSING_INPUTS"}
    if market != "ES" or window["lower_ns"] != five_years_before(window["cutoff_ns"]):
        raise IntegrityError("close market or trailing five-year history differs")
    if not window["cutoff_ns"] <= ready_at or not window["cutoff_ns"] < window["activation_ns"]:
        raise IntegrityError("invalid close fit readiness")
    if not data.source_start_ns <= window["lower_ns"] < window["cutoff_ns"] <= data.source_end_ns:
        return {"status": "INSUFFICIENT_HISTORY"}
    outcomes = {o.sample_id: o for o in data.outcomes}
    units = {u.contract: u for u in data.instruments}
    rows, excluded = [], Counter()
    for d in sorted(data.decisions, key=lambda d: (d.decision_ns, d.sample_id)):
        if not window["lower_ns"] <= d.decision_ns < window["cutoff_ns"]:
            continue
        o = outcomes[d.sample_id]
        if d.features is None or d.feature_available_ns > d.decision_ns:
            excluded["UNAVAILABLE_INPUT"] += 1
        elif o.known_ns >= window["cutoff_ns"]:
            excluded["IMMATURE_OR_UNKNOWN_LABEL"] += 1
        else:
            y = target(d, o, units[d.contract])
            if y is None:
                excluded["UNOBSERVABLE_TARGET"] += 1
            else:
                rows.append((d, o, y))
    if len(rows) < study["minimum_train_sessions"]:
        return {"status": "INSUFFICIENT_SUPPORT", "training_sessions": len(rows), "excluded": dict(excluded)}
    dates = [d.session for d, _, _ in rows]
    weights = row_weights(dates, window["cutoff_ns"], policy)
    y = np.asarray([y for _, _, y in rows])
    result = _close_wls([d.features[0] for d, _, _ in rows], y, weights)
    if result["status"] != "FITTED":
        return {**result, "excluded": dict(excluded)}
    signs = Counter(int(np.sign(v)) for v in y)
    modal = max((0, 1, -1), key=lambda s: signs[s])  # Stable declared tie order.
    provenance = {o["sample_id"]: o["information_id"] for o in data.opportunities}
    training = [(asdict(d), asdict(o), y, provenance[d.sample_id]) for d, o, y in rows]
    study_id = close_fitting_identity(study)
    return bind({**result, "protocol_id": close_protocol()["protocol_id"], "study_id": study_id,
                 "input_id": sha256_json({"source": data.source_identity, "calendar": data.calendar_identity,
                                          "vintage": data.vintage_assumption, "training": training}),
                 "market": "ES", "policy": policy, "cutoff_ns": window["cutoff_ns"],
                 "lower_ns": window["lower_ns"], "activation_ns": window["activation_ns"],
                 "ready_at_ns": ready_at, "evaluation_stop_ns": window["stop_ns"],
                 "feature_width": 1, "columns": [0], "equal_session_training_mean": float(np.mean(y)),
                 "modal_sign": modal, "fit_ids": [d.sample_id for d, _, _ in rows],
                 "training_sessions": len(rows), "training_rows": len(rows),
                 "latest_label_ns": max(o.known_ns for _, o, _ in rows),
                 "session_weight_max": float(max(weights)), "session_weight_inverse_hhi": float(1/(weights @ weights)),
                 "support_claim": "WEIGHT_CONCENTRATION_NOT_INDEPENDENT_SAMPLE_SIZE",
                 "excluded": dict(excluded), "numpy_version": np.__version__}, "model_id")


def close_score(records, data, *, as_of, aggregation):
    data.validate()
    if aggregation != AGGREGATION:
        raise IntegrityError("close evaluation weights must remain equal session")
    decisions = {d.sample_id: d for d in data.decisions}
    outcomes = {o.sample_id: o for o in data.outcomes}
    units = {u.contract: u for u in data.instruments}
    opportunities = {o["sample_id"]: o for o in data.opportunities}
    seen, rows, confusion, baseline_confusion = set(), [], Counter(), Counter()
    missing, statuses = 0, Counter()
    for r in records:
        identity(r, "forecast_id")
        key = r["sample_id"]
        if key in seen or key not in opportunities:
            raise IntegrityError("duplicate or unknown close forecast")
        seen.add(key)
        op = opportunities[key]
        if (r["session"], r["decision_ns"]) != (op["session"], op["decision_ns"]):
            raise IntegrityError("close forecast timing join differs")
        statuses[r["status"]] += 1
        if r["status"] != "FORECAST":
            continue
        d, o = decisions[key], outcomes[key]
        if r["decision_id"] != sha256_json(asdict(d)):
            raise IntegrityError("forecast input identity changed")
        y = target(d, o, units[d.contract]) if o.known_ns <= as_of else None
        if y is None:
            missing += 1
            continue
        losses = {k: (explicit_real(r[k], name=k)-y)**2 for k in
                  ("prediction_ticks", "mean_ticks", "zero_ticks", "weighted_intercept_ticks")}
        actual, predicted, modal = int(np.sign(y)), int(np.sign(r["prediction_ticks"])), r["modal_sign"]
        confusion[(actual, predicted)] += 1
        baseline_confusion[(actual, modal)] += 1
        rows.append({"sample_id": key, "session": d.session, "target_ticks": y,
                     "losses": losses, "delta_mse": losses["mean_ticks"]-losses["prediction_ticks"],
                     "sign_correct": int(actual == predicted), "modal_correct": int(actual == modal)})
    def direction(counts):
        support = {str(c): sum(n for (a, _), n in counts.items() if a == c) for c in (0, 1, -1)}
        recalls = {str(c): counts[(c, c)]/support[str(c)] if support[str(c)] else None for c in (0, 1, -1)}
        return {"confusion": {f"{a}:{b}": counts[(a,b)] for a in (0,1,-1) for b in (0,1,-1)},
                "class_support": support, "recall": recalls,
                "raw_accuracy": sum(counts[(c,c)] for c in (0,1,-1))/len(rows) if rows else None,
                "balanced_accuracy": sum(recalls.values())/3 if all(v is not None for v in recalls.values()) else None}
    losses = {k: float(np.mean([r["losses"][k] for r in rows])) for k in
              ("prediction_ticks", "mean_ticks", "zero_ticks", "weighted_intercept_ticks")} if rows else None
    benefit = float(np.mean([r["delta_mse"] for r in rows])) if rows else None
    return {"status": "DESCRIPTIVE_COMPLETE" if rows and not missing else "INCOMPLETE_OUTCOMES",
            "aggregation": aggregation, "rows": rows, "scorable_ids": sorted(r["sample_id"] for r in rows),
            "mean_losses": losses, "mean_delta_mse": benefit,
            "relative_improvement": benefit/losses["mean_ticks"] if losses and losses["mean_ticks"] > 0 else None,
            "directional": direction(confusion), "modal_baseline": direction(baseline_confusion),
            "paired_accuracy_benefit": float(np.mean([r["sign_correct"]-r["modal_correct"] for r in rows])) if rows else None,
            "expected_opportunities": len(records), "forecasts": statuses["FORECAST"],
            "scorable_sessions": len(rows), "missing_outcomes": missing, "statuses": dict(statuses),
            "input_dispositions": dict(Counter(opportunities[k]["input_status"] for k in seen)),
            "qualification": "UNAVAILABLE_NO_ADOPTED_METHOD"}


def close_select(candidates):
    if ([c["policy"] for c in candidates] != list(POLICIES)
            or any(c.get("failed_fits") or not c["score"]["scorable_ids"] for c in candidates)):
        return None
    if candidates[0]["score"]["scorable_ids"] != candidates[1]["score"]["scorable_ids"]:
        return None
    return min(candidates, key=lambda c: (c["score"]["mean_losses"]["prediction_ticks"],
                                          POLICIES.index(c["policy"])))["policy"]


class CloseState:
    """Serializable scheduled predictive state; no positions, IO or execution authority."""
    def __init__(self, study):
        if close_prepare(study)["status"] != "READY_SYNTHETIC_ONLY":
            raise IntegrityError("complete synthetic close study required")
        self.study_id = sha256_json(study)
        self.fitting_id = close_fitting_identity(study)
        self.dispositions = {}
        self.updates = []
        self.model = None

    def update(self, window, model, selection):
        if self.updates and window["activation_ns"] <= self.updates[-1]["activation_ns"]:
            raise IntegrityError("scheduled close updates must advance")
        if model is not None:
            identity(model, "model_id")
            if (model["study_id"] != self.fitting_id or model["activation_ns"] != window["activation_ns"]
                    or model["evaluation_stop_ns"] != window["stop_ns"]
                    or model["protocol_id"] != close_protocol()["protocol_id"]):
                raise IntegrityError("model/update identity differs")
            if model["ready_at_ns"] > window["activation_ns"]:
                model = None
        self.model = model
        self.updates.append({"activation_ns": window["activation_ns"], "stop_ns": window["stop_ns"],
                             "model_id": model["model_id"] if model else None, "selection": selection,
                             "status": "READY" if model else "FAILED_UPDATE_NO_NEW_ENTRIES"})

    def snapshot(self):
        return bind({"protocol_id": close_protocol()["protocol_id"], "study_id": self.study_id,
                     "updates": self.updates, "model": self.model,
                     "missing_input_and_update_dispositions": self.dispositions}, "state_id")

    @classmethod
    def restore(cls, value, study):
        identity(value, "state_id")
        result = cls(study)
        if value["study_id"] != result.study_id or value["protocol_id"] != close_protocol()["protocol_id"]:
            raise IntegrityError("saved close state identity differs")
        updates = value["updates"]
        if any(u["activation_ns"] >= u["stop_ns"] for u in updates) or any(
                a["activation_ns"] >= b["activation_ns"] for a, b in zip(updates, updates[1:])):
            raise IntegrityError("invalid saved update chronology")
        if value["model"] is not None:
            identity(value["model"], "model_id")
            if (value["model"]["study_id"] != result.fitting_id
                    or value["model"]["protocol_id"] != close_protocol()["protocol_id"]):
                raise IntegrityError("saved model belongs to another close study")
            if not value["updates"] or value["updates"][-1]["model_id"] != value["model"]["model_id"]:
                raise IntegrityError("saved update/model mismatch")
        result.updates = list(value["updates"])
        result.dispositions = dict(value["missing_input_and_update_dispositions"])
        result.model = value["model"]
        return result

    def forecasts(self, opportunities, decisions):
        dmap = {d.sample_id: d for d in decisions}
        result = []
        for op in opportunities:
            d = dmap.get(op["sample_id"])
            model = self.model
            if op["input_status"] != "ELIGIBLE_INPUT":
                status = op["input_status"]
            elif model is None:
                status = "ABSTAIN_FAILED_UPDATE"
            elif not model["activation_ns"] <= op["decision_ns"] < model["evaluation_stop_ns"]:
                status = "ABSTAIN_EXPIRED_OR_INACTIVE_MODEL"
            else:
                result.extend(predict(model, [d]))
                continue
            disposition = {"decision_ns": op["decision_ns"], "status": status}
            prior = self.dispositions.setdefault(op["sample_id"], disposition)
            if prior != disposition:
                raise IntegrityError("saved close abstention cannot be rewritten")
            result.append(bind({"sample_id": op["sample_id"], "session": op["session"],
                                "decision_ns": op["decision_ns"], "status": status,
                                "prediction_ticks": None, "model_id": model["model_id"] if model else None},
                               "forecast_id"))
        return tuple(result)


def run_close_pilot(data, study, start, end, *, ready_at=None):
    """Real close adapter outputs -> two-policy inner selection -> selected outer predictions."""
    readiness = close_prepare(study)
    if readiness["status"] != "READY_SYNTHETIC_ONLY":
        return readiness
    data.validate()
    if not isinstance(data, CloseInputs):
        raise IntegrityError("close adapter inputs required")
    if not data.source_start_ns <= endpoint(start) < endpoint(end) <= data.source_end_ns:
        return {**readiness, "status": "INSUFFICIENT_SOURCE_ENVELOPE"}
    if any(s.state == "UNKNOWN" for s in data.sessions):
        return {**readiness, "status": "UNAVAILABLE_CALENDAR", "opportunities": data.opportunities}
    plans = schedule(data.sessions, start, end)
    fits, selections, records, quarterly = [], [], [], []
    state = CloseState(study)
    for plan in plans:
        outer, candidates = plan["outer"], []
        memberships = {}
        for policy in POLICIES:
            inner_records, failures = [], []
            for inner in plan["inner"]:
                model = fit(data, "ES", inner, policy, study,
                            ready_at=(ready_at or {}).get(inner["activation_ns"], inner["activation_ns"]))
                fits.append({"role": "INNER", "quarter": inner["quarter"], "policy": policy, "model": model})
                if model["status"] != "FITTED" or model["ready_at_ns"] > inner["activation_ns"]:
                    failures.append({"quarter": inner["quarter"], "status": model["status"]
                                     if model["status"] != "FITTED" else "LATE_UPDATE"})
                    continue
                prior = memberships.setdefault(inner["quarter"], model["fit_ids"])
                if prior != model["fit_ids"]:
                    failures.append({"quarter": inner["quarter"], "status": "TRAINING_MEMBERSHIP_MISMATCH"})
                    continue
                inner_state = CloseState(study)
                inner_state.update(inner, model, {"policy": policy})
                ops = [o for o in data.opportunities if inner["start_ns"] <= o["decision_ns"] < inner["stop_ns"]]
                inner_records.extend(inner_state.forecasts(ops, data.decisions))
            score = close_score(inner_records, data, as_of=outer["cutoff_ns"]-1, aggregation=AGGREGATION)
            candidates.append({"policy": policy, "score": score, "failed_fits": failures})
        winner = close_select(candidates)
        model = None
        if winner is not None:
            fitted = fit(data, "ES", outer, winner, study,
                         ready_at=(ready_at or {}).get(outer["activation_ns"], outer["activation_ns"]))
            fits.append({"role": "OUTER", "quarter": outer["quarter"], "policy": winner, "model": fitted})
            if fitted["status"] == "FITTED":
                model = fitted
        selection = {"quarter": outer["quarter"], "winner": winner, "candidates": candidates}
        state.update(outer, model, selection)
        selections.append(selection)
        ops = [o for o in data.opportunities if outer["start_ns"] <= o["decision_ns"] < outer["stop_ns"]]
        batch = state.forecasts(ops, data.decisions)
        records.extend(batch)
        quarterly.append(close_score(batch, data, as_of=outer["stop_ns"]-1, aggregation=AGGREGATION))
    as_of = min(endpoint(end), data.source_end_ns)-1
    score = close_score(records, data, as_of=as_of, aggregation=AGGREGATION)
    recent = close_score([r for r in records if r["session"] >= "2023-01-01"], data,
                         as_of=as_of, aggregation=AGGREGATION)
    return bind({"status": "COMPLETE_SYNTHETIC_MECHANICS" if all(u["status"] == "READY" for u in state.updates)
                 and not score["missing_outcomes"] else "INCOMPLETE_EVIDENCE",
                 "readiness": readiness, "protocol": close_protocol(), "fit_count": len(fits),
                 "fits": fits, "selections": selections, "schedule": plans, "forecasts": records,
                 "outer_metrics": score, "recent_metrics": recent, "quarterly_metrics": quarterly,
                 "persistent_state": state.snapshot(),
                 "qualifications": {"implementation": "CHECKS_REQUIRED",
                                    "positive_continuation": "UNAVAILABLE_NO_ADOPTED_METHOD",
                                    "predictive_numerical": "UNAVAILABLE_NO_ADOPTED_METHOD",
                                    "predictive_directional": "UNAVAILABLE_NO_ADOPTED_METHOD",
                                    "economic": "UNAVAILABLE_UNBOUND_POLICY", "incremental": "NOT_ASSESSED",
                                    "deployment": "NOT_ASSESSED"},
                 "test_doubles": ["GENERATED_CALENDAR_SOURCE_AND_AVAILABILITY",
                                  "SIMULATED_FIT_READY_AT;NO_EXECUTION_POLICY"],
                 "recent_development_claim": protocol()["recent_development"]["claim"]}, "run_id")
