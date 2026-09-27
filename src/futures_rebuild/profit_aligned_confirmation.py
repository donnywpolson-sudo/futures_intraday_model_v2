"""Fresh 252-session confirmation mechanics; all authority remains disabled."""
from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
import json
from pathlib import Path

import numpy as np

from .canonical import write_new_canonical_authority
from .errors import IntegrityError
from .profit_aligned_research import (AUTHORITY, MARKETS, MINUTE, bind, daily_ledger,
    digest, identity, integer, mechanism_id, ordered_dates, predict, trade_record, validate_policy)
from .fixed_hour_terminal_successor import FEATURE_NAMES
from .research.bootstrap import stationary_bootstrap_indices
from .research.contracts import finite_float64
from .research.hac import _newey_west_arrays

HORIZON = 252
RESAMPLES = 9999
SEED = 62817
SCHEDULE_FIELDS = ("opportunity_id", "decision_ns", "entry_ns", "exit_ns",
                   "actual_contract", "tick_nano", "tick_value_nano_usd", "quartile")


def campaign_alpha(index):
    integer(index, minimum=1)
    if index > 1020:
        raise IntegrityError("campaign allocation exhausted floating-point resolution")
    return .05 / 2.**index


@lru_cache(maxsize=1)
def bootstrap_indices():
    rows = stationary_bootstrap_indices(n_observations=HORIZON, n_resamples=RESAMPLES,
        mean_block_length=10., seed=SEED)
    rows.flags.writeable = False
    return rows


def raw_p_values(daily_usd, *, full_resampling=False):
    """The production test kernel used unchanged by Monte Carlo validation.

    Negative sample means cannot qualify; reporting p=1 for them is conservative.
    A degenerate observed/bootstrap studentizer makes that whole claim untestable.
    No observations or bootstrap draws are silently removed.
    """
    values = finite_float64(daily_usd, name="raw daily dollar P&L", ndim=2)
    if values.shape != (HORIZON, len(MARKETS)):
        raise IntegrityError("confirmation requires exactly 252 dates by four markets")
    means, _, se, _ = _newey_west_arrays(values.T, lag=5)
    active = np.flatnonzero((means > 0) & (se > 0))
    p = np.ones(4)
    if not len(active):
        return p
    observed = means[active] / se[active]
    centered = values[:, active] - means[active]
    counts = np.zeros(len(active), dtype=np.int64)
    invalid = np.zeros(len(active), dtype=bool)
    indices = bootstrap_indices()
    pending = np.arange(len(active))
    for start in range(0, RESAMPLES, 256):
        samples = centered[:, pending][indices[start:start + 256]]
        b = len(samples)
        mu, _, errors, _ = _newey_west_arrays(samples.transpose(0, 2, 1).reshape(-1, HORIZON), lag=5)
        mu, errors = mu.reshape(b, -1), errors.reshape(b, -1)
        invalid[pending] |= np.any(errors <= 0, axis=0)
        statistics = np.divide(mu, errors, out=np.full_like(mu, np.inf), where=errors > 0)
        counts[pending] += np.count_nonzero(statistics >= observed[pending], axis=0)
        if not full_resampling:
            # Once 63 exceedances are inevitable, no campaign allocation can
            # reject. Conservatively report p=1; never stop a possible rejection.
            pending = pending[(counts[pending] < 62) & ~invalid[pending]]
            if not len(pending):
                break
    p[active] = (counts + 1.) / (RESAMPLES + 1.)
    p[active[invalid]] = 1.
    if not full_resampling:
        p[active[counts >= 62]] = 1.
    return p


def decision_from_daily(daily_usd, *, index=1, trade_counts=(500, 500, 500, 500), invalid_markets=()):
    """Numerical decision only: not an activation or real-data qualification route."""
    values = finite_float64(daily_usd, name="daily P&L", ndim=2)
    if values.shape != (HORIZON, 4) or len(trade_counts) != 4 or not set(invalid_markets) <= set(MARKETS):
        raise IntegrityError("confirmation dimensions or market identities differ")
    for count in trade_counts:
        integer(count)
    alpha = campaign_alpha(index)
    adjusted = np.minimum(1., raw_p_values(values) * 4.)
    enough_resolution = 4. / (RESAMPLES + 1.) <= alpha
    decisions = {}
    for j, market in enumerate(MARKETS):
        mean, total = float(np.mean(values[:, j])), float(np.sum(values[:, j]))
        p = 1. if market in invalid_markets else float(adjusted[j])
        if market in invalid_markets:
            status = "INVALID_INCOMPLETE_EVIDENCE"
        elif mean <= 0 or total <= 0:
            status = "NONPOSITIVE_OBSERVED_ECONOMICS"
        elif trade_counts[j] < 500 or not enough_resolution or p > alpha:
            status = "INCONCLUSIVE"
        else:
            status = "QUALIFIED_MECHANICS_ONLY"
        decisions[market] = {"status": status, "qualified": status == "QUALIFIED_MECHANICS_ONLY",
            "mean_daily_stress_usd": mean, "total_stress_usd": total,
            "adjusted_p": p, "executed_trades": trade_counts[j]}
    return {"market_decisions": decisions, "campaign_alpha": alpha, "campaign_index": index,
        "declared_claim_count": 4, "bootstrap_resolution_sufficient": enough_resolution,
        "claim": "ASSUMPTION_DEPENDENT_QUOTE_BASED_RAW_PNL_EVIDENCE", "authority": AUTHORITY}


def freeze_campaign(*, frozen_at_ns, sessions, session_bounds, calendar_id, models, policy, schedule, prior_campaigns):
    """Prepare immutable synthetic campaign identity; never activates collection.

    The trusted future adapter must certify source clocks, the exact schedule,
    final model fitting and append-only custody. Shape/hash checks are not trust.
    """
    integer(frozen_at_ns, minimum=1)
    digest(calendar_id)
    dates = ordered_dates(sessions)
    if len(dates) != HORIZON or set(models) != set(MARKETS) or set(schedule) != set(MARKETS):
        raise IntegrityError("freeze requires four models and exactly 252 dates")
    freeze_date = datetime.fromtimestamp(frozen_at_ns / 1e9, timezone.utc).date().isoformat()
    if dates[0] <= freeze_date:
        raise IntegrityError("campaign dates must occur after freeze, never reused history")
    validate_policy(policy)
    if set(session_bounds) != set(dates):
        raise IntegrityError("certified session boundaries must cover the frozen calendar")
    last_end = frozen_at_ns
    for day in dates:
        bounds = session_bounds[day]
        if set(bounds) != {"start_ns", "end_ns"}:
            raise IntegrityError("invalid session boundary fields")
        integer(bounds["start_ns"], minimum=1)
        integer(bounds["end_ns"], minimum=1)
        if not last_end <= bounds["start_ns"] < bounds["end_ns"]:
            raise IntegrityError("sessions overlap or precede freeze")
        last_end = bounds["end_ns"]
    previous = None
    for k, old in enumerate(prior_campaigns, start=1):
        identity(old, "campaign_id")
        if old["campaign_index"] != k or old["previous_campaign_id"] != previous:
            raise IntegrityError("campaign allocation ledger resets or forks")
        previous = old["campaign_id"]
    if prior_campaigns and dates[0] <= prior_campaigns[-1]["sessions"][-1]:
        raise IntegrityError("campaign outcomes cannot be reused")
    for market in MARKETS:
        identity(models[market], "model_id")
        integer(models[market].get("frozen_at_ns"), minimum=1)
        integer(models[market].get("maximum_target_available_at_ns"), minimum=1)
        digest(models[market].get("development_stress_id"))
        if not models[market]["maximum_target_available_at_ns"] < models[market]["frozen_at_ns"] <= frozen_at_ns:
            raise IntegrityError("final model is not frozen after labels and before campaign")
        if (models[market]["mechanism_id"] != mechanism_id() or models[market]["market"] != market
                or models[market]["fit_kind"] != "development_refit"):
            raise IntegrityError("wrong frozen model mechanism")
        predict(models[market], np.zeros((1, len(FEATURE_NAMES))))
        entries = schedule[market]
        if set(entries) != set(dates):
            raise IntegrityError("schedule must cover every date, including declared zero-opportunity dates")
        seen = set()
        previous_decision = frozen_at_ns
        for day in dates:
            for entry in entries[day]:
                if set(entry) != set(SCHEDULE_FIELDS):
                    raise IntegrityError("freeze schedule must exclude outcomes and quotes")
                for t in ("decision_ns", "entry_ns", "exit_ns"):
                    integer(entry[t], minimum=1)
                integer(entry["tick_nano"], minimum=1)
                integer(entry["tick_value_nano_usd"], minimum=1)
                integer(entry["quartile"], minimum=1)
                if (entry["quartile"] > 4 or not isinstance(entry["actual_contract"], str) or not entry["actual_contract"]
                        or not session_bounds[day]["start_ns"] <= entry["decision_ns"]
                        < entry["exit_ns"] <= session_bounds[day]["end_ns"]):
                    raise IntegrityError("opportunity outside certified session or contract missing")
                if (not isinstance(entry["opportunity_id"], str) or not entry["opportunity_id"]
                        or entry["opportunity_id"] in seen or entry["decision_ns"] <= previous_decision
                        or entry["entry_ns"] != (entry["decision_ns"] // MINUTE + 1) * MINUTE
                        or entry["exit_ns"] != entry["entry_ns"] + 60 * MINUTE):
                    raise IntegrityError("frozen schedule is invalid, reused or not future")
                seen.add(entry["opportunity_id"])
                previous_decision = entry["decision_ns"]
    return bind({"schema_version": "profit_fresh_campaign/1.0.0", "mechanism_id": mechanism_id(),
        "state": "PREPARED_SYNTHETIC_ONLY_NOT_ACTIVE", "campaign_index": len(prior_campaigns) + 1,
        "previous_campaign_id": previous, "frozen_at_ns": frozen_at_ns, "calendar_id": calendar_id,
        "sessions": list(dates), "session_bounds": session_bounds,
        "models": models, "policy": policy, "schedule": schedule,
        "source_bound_readiness": "NOT_CERTIFIED", "statistical_validation": "REQUIRES_BOUND_PASS",
        "authority": AUTHORITY}, "campaign_id")


class SyntheticCampaignStore:
    """Create-only files demonstrate custody; deliberately no real-history route.

    A prior prefix must be replayed, not replaced. File creation is exclusive, so
    duplicate writers cannot overwrite predictions, outcomes or alpha charges.
    Identical writes are idempotent. This is not tamper-proof external custody.
    """
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def _read(self, name):
        return json.loads((self.directory / name).read_bytes())

    def reserve(self, **inputs):
        paths = sorted(self.directory.glob("campaign-*.json"))
        if [p.name for p in paths] != [f"campaign-{i:06d}.json" for i in range(1, len(paths) + 1)]:
            raise IntegrityError("allocation ledger has missing indices")
        prior = [self._read(p.name) for p in paths]
        campaign = freeze_campaign(**inputs, prior_campaigns=prior)
        write_new_canonical_authority(self.directory / f"campaign-{campaign['campaign_index']:06d}.json", campaign)
        return campaign

    def _campaign(self, campaign):
        identity(campaign, "campaign_id")
        if self._read(f"campaign-{campaign['campaign_index']:06d}.json") != campaign:
            raise IntegrityError("campaign differs from durable allocation")
        if campaign["mechanism_id"] != mechanism_id() or any(campaign["authority"].values()):
            raise IntegrityError("store accepts inactive synthetic mechanics only")

    def record_prediction(self, campaign, *, market, day, row, features, feature_available_at_ns, recorded_at_ns):
        self._campaign(campaign)
        if market not in MARKETS or day not in campaign["sessions"]:
            raise IntegrityError("prediction outside frozen scope")
        if "entry_quote" in row or "exit_quote" in row:
            raise IntegrityError("outcomes cannot enter the prediction interface")
        fields = {k: row[k] for k in SCHEDULE_FIELDS}
        if fields not in campaign["schedule"][market][day] or row["trade_date"] != day or row["market"] != market:
            raise IntegrityError("prediction differs from frozen opportunity")
        integer(recorded_at_ns, minimum=1)
        integer(feature_available_at_ns, minimum=1)
        if not campaign["frozen_at_ns"] < recorded_at_ns <= row["decision_ns"] or feature_available_at_ns > recorded_at_ns:
            raise IntegrityError("prediction must be durable by the decision, before outcomes")
        x = finite_float64(features, name="prediction features", ndim=1)
        p = float(predict(campaign["models"][market], x[None, :])[0])
        # Only decision-time evidence enters this record. Entry/exit observations
        # must arrive later via the separate outcome interface.
        allowed = ("opportunity_id", "market", "trade_date", "actual_contract", "decision_ns",
                   "entry_ns", "exit_ns", "tick_nano", "tick_value_nano_usd", "quartile", "decision_quote")
        decision_row = {k: row[k] for k in allowed}
        from .profit_aligned_research import decision
        direction, reason = decision(p, decision_row, campaign["policy"])
        item = bind({"campaign_id": campaign["campaign_id"], "market": market, "day": day,
            "recorded_at_ns": recorded_at_ns, "row": decision_row, "features": x.tolist(),
            "feature_available_at_ns": feature_available_at_ns,
            "prediction": p, "direction": direction, "reason": reason,
            "model_id": campaign["models"][market]["model_id"]}, "prediction_id")
        key = digest(bind(fields, "id")["id"])
        write_new_canonical_authority(self.directory / f"prediction-{campaign['campaign_index']}-{market}-{key}.json", item)
        return item

    def record_day(self, campaign, *, market, day, outcomes, recorded_at_ns):
        self._campaign(campaign)
        item = self._build_day(campaign, market=market, day=day, outcomes=outcomes, recorded_at_ns=recorded_at_ns)
        write_new_canonical_authority(self.directory / f"day-{campaign['campaign_index']}-{market}-{day}.json", item)
        return item

    def _build_day(self, campaign, *, market, day, outcomes, recorded_at_ns):
        if market not in MARKETS or day not in campaign["sessions"]:
            raise IntegrityError("day outside frozen scope")
        expected = campaign["schedule"][market][day]
        integer(recorded_at_ns, minimum=1)
        if recorded_at_ns < campaign["session_bounds"][day]["end_ns"]:
            raise IntegrityError("daily evidence is not yet complete")
        if [o.get("opportunity_id") for o in outcomes] != [e["opportunity_id"] for e in expected]:
            raise IntegrityError("daily outcomes omit or reorder scheduled opportunities")
        records = []
        for entry, outcome in zip(expected, outcomes, strict=True):
            key = bind(entry, "id")["id"]
            saved = self._read(f"prediction-{campaign['campaign_index']}-{market}-{key}.json")
            identity(saved, "prediction_id")
            if (saved["campaign_id"] != campaign["campaign_id"] or saved["market"] != market or saved["day"] != day
                    or saved["model_id"] != campaign["models"][market]["model_id"]
                    or {k: saved["row"][k] for k in SCHEDULE_FIELDS} != entry
                    or saved["row"]["market"] != market or saved["row"]["trade_date"] != day
                    or not campaign["frozen_at_ns"] < saved["recorded_at_ns"] <= entry["decision_ns"]
                    or saved["feature_available_at_ns"] > saved["recorded_at_ns"]):
                raise IntegrityError("prediction custody is invalid")
            if saved["prediction"] != float(predict(campaign["models"][market], np.array([saved["features"]]))[0]):
                raise IntegrityError("prediction is not from frozen model")
            if set(outcome) != {"opportunity_id", "entry_quote", "exit_quote"} or recorded_at_ns < entry["exit_ns"]:
                raise IntegrityError("outcome fields or maturity differ")
            row = {**saved["row"], "entry_quote": outcome["entry_quote"], "exit_quote": outcome["exit_quote"]}
            records.append(trade_record(saved["prediction"], row, campaign["policy"]))
        invalid = any(r["net_usd"] is None for r in records)
        ledger = None if invalid else daily_ledger(records, expected_ids=[e["opportunity_id"] for e in expected], dates=[day])
        item = bind({"campaign_id": campaign["campaign_id"], "market": market, "day": day,
            "recorded_at_ns": recorded_at_ns, "outcomes": outcomes, "trades": records, "ledger": ledger,
            "status": "INVALID_INCOMPLETE_EVIDENCE" if invalid else "COMPLETE"}, "day_id")
        return item

    def evaluate(self, campaign, *, as_of_ns):
        self._campaign(campaign)
        integer(as_of_ns, minimum=1)
        end = campaign["session_bounds"][campaign["sessions"][-1]]["end_ns"]
        if as_of_ns < end:
            raise IntegrityError("early confirmation evaluation is forbidden")
        values = np.zeros((HORIZON, 4))
        counts = [0] * 4
        invalid = set()
        for j, market in enumerate(MARKETS):
            for i, day in enumerate(campaign["sessions"]):
                path = self.directory / f"day-{campaign['campaign_index']}-{market}-{day}.json"
                if not path.is_file():
                    invalid.add(market)
                    continue
                saved = self._read(path.name)
                identity(saved, "day_id")
                if saved["campaign_id"] != campaign["campaign_id"] or saved["market"] != market or saved["day"] != day or saved["recorded_at_ns"] > as_of_ns:
                    raise IntegrityError("day custody changed")
                rebuilt = self._build_day(campaign, market=market, day=day, outcomes=saved["outcomes"], recorded_at_ns=saved["recorded_at_ns"])
                if saved != rebuilt:
                    raise IntegrityError("daily trades differ from frozen predictions and raw quotes")
                if saved["status"] != "COMPLETE":
                    invalid.add(market)
                    continue
                expected = [e["opportunity_id"] for e in campaign["schedule"][market][day]]
                ledger = daily_ledger(saved["trades"], expected_ids=expected, dates=[day])
                if ledger != saved["ledger"]:
                    raise IntegrityError("daily accounting differs from immutable trades")
                values[i, j] = float(ledger["total_usd"]["stress"])
                counts[j] += ledger["executed_trades"]
        # Placeholder zeros never enter a claim: the entire incomplete market
        # is marked untestable and receives p=1, without reallocating its slot.
        for j, market in enumerate(MARKETS):
            if market in invalid:
                values[:, j] = 0.
        result = decision_from_daily(values, index=campaign["campaign_index"], trade_counts=counts, invalid_markets=invalid)
        return bind({**result, "campaign_id": campaign["campaign_id"],
            "source_bound_readiness": "NOT_CERTIFIED", "activation_ready": False,
            "qualification_ready": False,
            "state": "SYNTHETIC_CONFIRMATION_ONLY"}, "result_id")
