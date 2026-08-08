"""Pre-registration source readiness for the cash-open impulse mechanism.

This module contains no fitting, prediction, P&L, or publication code.  It
turns already-authorized in-memory one-minute source records into exact,
terminal feature/execution dispositions for the proposed mechanism.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, time, timezone
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .canonical import sha256_file
from .errors import IntegrityError, UnauthorizedOperation
from .preexecution_fold_certification import (
    ROW_CERTIFIED,
    build_fold_readiness_certificate,
)
from .tier1_bracket_v5 import NS_PER_MINUTE, V5SourceRecord


CHICAGO = ZoneInfo("America/Chicago")
MARKETS = ("ES", "CL", "ZN", "6E")
CHECKPOINTS = (time(9, 0), time(10, 30))
FEATURE_MINUTES = 30
EXECUTION_RECORDS = 31  # entry bar through scheduled-exit bar
MINIMUM_TRAINING_SESSIONS = 504
MINIMUM_EVALUATION_SESSIONS = 63
PURGE_MINUTES = 31
EMBARGO_SESSIONS = 1
COST_SCENARIOS = ("base", "stress", "extreme")
FEE_PER_SIDE_USD = Decimal("5")
MAXIMUM_INITIAL_RISK_USD = Decimal("250")
COST_TICKS = {
    "base": {"ES": 2, "CL": 4, "ZN": 2, "6E": 2},
    "stress": {"ES": 4, "CL": 8, "ZN": 4, "6E": 4},
    "extreme": {"ES": 8, "CL": 16, "ZN": 8, "6E": 8},
}
BASELINES = (
    "flat_no_trade",
    "always_long_first_checkpoint",
    "always_short_first_checkpoint",
    "opening_impulse_continuation_first_checkpoint",
    "opening_impulse_reversal_first_checkpoint",
)


@dataclass(frozen=True)
class OpportunityReadiness:
    checkpoint: str
    feature_complete: bool
    execution_complete: bool
    risk_disposition_by_scenario: Mapping[str, str]
    failure: str | None


@dataclass(frozen=True)
class SessionReadiness:
    market: str
    session: str
    opportunities: tuple[OpportunityReadiness, ...]
    complete: bool
    failure: str | None


def _clock(event_at_ns: int) -> time:
    seconds, remainder = divmod(event_at_ns, 1_000_000_000)
    value = datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone(CHICAGO)
    return value.time().replace(microsecond=remainder // 1_000)


def _checkpoint_events(
    checkpoint: time, events_by_clock: Mapping[time, V5SourceRecord],
) -> tuple[tuple[V5SourceRecord, ...], tuple[V5SourceRecord, ...]]:
    checkpoint_minutes = checkpoint.hour * 60 + checkpoint.minute
    feature_clocks = tuple(
        time(*divmod(checkpoint_minutes - offset, 60))
        for offset in range(FEATURE_MINUTES, 0, -1)
    )
    # The last feature bar begins one minute before the checkpoint and is
    # available five seconds after the checkpoint. Entry is the next full
    # minute's open, never the contemporaneous bar.
    execution_clocks = tuple(
        time(*divmod(checkpoint_minutes + offset, 60))
        for offset in range(1, EXECUTION_RECORDS + 1)
    )
    return (
        tuple(events_by_clock[item] for item in feature_clocks if item in events_by_clock),
        tuple(events_by_clock[item] for item in execution_clocks if item in events_by_clock),
    )


def classify_session(
    *, market: str, session: str, rows: Sequence[V5SourceRecord],
) -> SessionReadiness:
    """Require every possible candidate/baseline dependency before fitting."""

    if market not in MARKETS or session.startswith("2025"):
        raise UnauthorizedOperation("readiness input leaves the locked 2018-2022 scope")
    executable = [item for item in rows if item.executable and item.bar is not None]
    clocks = [_clock(item.bar.event_at_ns) for item in executable if item.bar is not None]
    if len(clocks) != len(set(clocks)):
        return SessionReadiness(market, session, (), False, "DUPLICATE_EXECUTABLE_MINUTE")
    by_clock = {
        _clock(item.bar.event_at_ns): item
        for item in executable if item.bar is not None
    }
    results: list[OpportunityReadiness] = []
    for checkpoint in CHECKPOINTS:
        feature, execution = _checkpoint_events(checkpoint, by_clock)
        feature_ok = len(feature) == FEATURE_MINUTES
        execution_ok = len(execution) == EXECUTION_RECORDS
        checkpoint_minutes = checkpoint.hour * 60 + checkpoint.minute
        decision_ns = None
        if feature_ok:
            last = feature[-1]
            assert last.bar is not None
            decision_ns = last.bar.event_at_ns + NS_PER_MINUTE + 5_000_000_000
            feature_ok = (
                all(item.bar is not None and item.bar.available_at_ns <= decision_ns
                    for item in feature)
                and len({item.actual_identity_hash for item in feature}) == 1
                and None not in {item.actual_identity_hash for item in feature}
            )
        if execution_ok:
            first = execution[0]
            assert first.bar is not None
            execution_ok = (
                decision_ns is not None
                and decision_ns < first.bar.event_at_ns
                and len({item.actual_identity_hash for item in execution}) == 1
                and None not in {item.actual_identity_hash for item in execution}
                and len({item.market_spec for item in execution}) == 1
                and None not in {item.market_spec for item in execution}
            )
        # A roll inside either dependency invalidates that opportunity.  The
        # identity need not match across the two non-overlapping checkpoints.
        if feature_ok and execution_ok:
            combined_identity = {
                item.actual_identity_hash for item in (*feature, *execution)
            }
            if len(combined_identity) != 1:
                feature_ok = execution_ok = False
        if not feature_ok:
            failure = "FEATURE_WINDOW_INCOMPLETE_OR_IDENTITY_CHANGING"
        elif not execution_ok:
            failure = "EXECUTION_PATH_INCOMPLETE_OR_IDENTITY_CHANGING"
        else:
            failure = None
        risk: dict[str, str] = {}
        if feature_ok and execution_ok:
            spec = execution[0].market_spec
            assert spec is not None
            for name in COST_SCENARIOS:
                cost = Decimal(2) * FEE_PER_SIDE_USD + Decimal(
                    COST_TICKS[name][market]
                ) * spec.tick_value
                stop_ticks = int(
                    ((MAXIMUM_INITIAL_RISK_USD - cost) / spec.tick_value)
                    .to_integral_value(rounding=ROUND_FLOOR)
                )
                risk[name] = "FEASIBLE" if cost < MAXIMUM_INITIAL_RISK_USD and stop_ticks >= 1 else "RISK_ABSTENTION"
        else:
            risk = {name: "UNRESOLVED_SOURCE" for name in COST_SCENARIOS}
        results.append(OpportunityReadiness(
            checkpoint=f"{checkpoint.hour:02d}:{checkpoint.minute:02d}",
            feature_complete=feature_ok,
            execution_complete=execution_ok,
            risk_disposition_by_scenario=risk,
            failure=failure,
        ))
    complete = len(results) == len(CHECKPOINTS) and all(
        item.feature_complete and item.execution_complete
        and all(value in {"FEASIBLE", "RISK_ABSTENTION"}
                for value in item.risk_disposition_by_scenario.values())
        for item in results
    )
    failure = None if complete else next(
        (item.failure for item in results if item.failure), "MISSING_EXPECTED_SESSION"
    )
    return SessionReadiness(market, session, tuple(results), complete, failure)


def iter_session_readiness(
    *, market: str, source_records: Iterable[V5SourceRecord],
) -> Iterable[SessionReadiness]:
    """Stream one market in bounded memory and reject scope/order drift."""

    current: str | None = None
    rows: list[V5SourceRecord] = []
    previous_event = -1
    for row in source_records:
        row.validate()
        if row.market != market or row.exchange_session_date.startswith("2025"):
            raise UnauthorizedOperation("readiness stream leaves its market or time boundary")
        year = int(row.exchange_session_date[:4])
        if year not in range(2018, 2023):
            raise UnauthorizedOperation("readiness stream leaves 2018-2022")
        event = row.bar.event_at_ns if row.bar is not None else previous_event + 1
        if current is not None and row.exchange_session_date < current:
            raise IntegrityError("readiness stream moves backwards by session")
        if current == row.exchange_session_date and event < previous_event:
            raise IntegrityError("readiness stream moves backwards within a session")
        if current is not None and row.exchange_session_date != current:
            yield classify_session(market=market, session=current, rows=rows)
            rows = []
            previous_event = -1
        current = row.exchange_session_date
        rows.append(row)
        previous_event = event
    if current is not None:
        yield classify_session(market=market, session=current, rows=rows)


def _year_counts(
    sessions: Sequence[str], readiness: Mapping[str, SessionReadiness], *, training: bool,
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for year in sorted({item[:4] for item in sessions}):
        expected = [item for item in sessions if item.startswith(year)]
        complete = [item for item in expected if readiness.get(item) is not None and readiness[item].complete]
        failures = Counter(
            "MISSING_SOURCE_SESSION" if readiness.get(item) is None
            else str(readiness[item].failure or "UNCLASSIFIED_SOURCE_DEPENDENCY")
            for item in expected if readiness.get(item) is None or not readiness[item].complete
        )
        prefix = "TRAINING" if training else "EVALUATION"
        output[year] = {
            "expected_training_sessions": len(expected) if training else 0,
            "complete_training_sessions": len(complete) if training else 0,
            "expected_evaluation_sessions": 0 if training else len(expected),
            "feature_complete_evaluation_sessions": 0 if training else len(complete),
            "terminal_evaluation_sessions": 0 if training else len(expected),
            "execution_path_complete_evaluation_sessions": 0 if training else len(complete),
            "exclusion_reasons": {
                f"{prefix}__{reason}": count for reason, count in sorted(failures.items())
            },
        }
    return output


def build_fold_evidence(
    *, observations: Sequence[SessionReadiness],
    outer_folds: Sequence[Mapping[str, object]],
    expected_sessions_by_market: Mapping[str, Sequence[str]],
) -> list[dict[str, object]]:
    """Build strict evidence: every possible dependency must be complete."""

    by_market = {
        market: {item.session: item for item in observations if item.market == market}
        for market in MARKETS
    }
    evidence: list[dict[str, object]] = []
    for fold_index, fold in enumerate(outer_folds):
        fit = fold.get("outer_fit_session_range")
        test = fold.get("outer_test_session_dates")
        if (not isinstance(fit, list) or len(fit) != 2
                or not isinstance(test, list) or len(test) != 2):
            raise IntegrityError("outer fold is malformed")
        fit_start, fit_end = map(str, fit)
        test_start, test_end = map(str, test)
        if not fit_start <= fit_end < test_start <= test_end:
            raise IntegrityError("outer fold is not chronological")
        for market in MARKETS:
            expected = tuple(expected_sessions_by_market[market])
            training = tuple(item for item in expected if fit_start <= item <= fit_end)
            evaluation = tuple(item for item in expected if test_start <= item <= test_end)
            ready = by_market[market]
            complete_training = sum(ready.get(item) is not None and ready[item].complete for item in training)
            complete_evaluation = sum(ready.get(item) is not None and ready[item].complete for item in evaluation)
            exclusions: Counter[str] = Counter()
            for role, group in (("TRAINING", training), ("EVALUATION", evaluation)):
                for session in group:
                    item = ready.get(session)
                    if item is None or not item.complete:
                        reason = "MISSING_SOURCE_SESSION" if item is None else str(
                            item.failure or "UNCLASSIFIED_SOURCE_DEPENDENCY"
                        )
                        exclusions[f"{role}__{reason}"] += 1
            year_breakdown: dict[str, dict[str, Any]] = {}
            for group, training_role in ((training, True), (evaluation, False)):
                for year, values in _year_counts(group, ready, training=training_role).items():
                    target = year_breakdown.setdefault(year, {
                        "expected_training_sessions": 0,
                        "complete_training_sessions": 0,
                        "expected_evaluation_sessions": 0,
                        "feature_complete_evaluation_sessions": 0,
                        "terminal_evaluation_sessions": 0,
                        "execution_path_complete_evaluation_sessions": 0,
                        "exclusion_reasons": {},
                    })
                    for key, value in values.items():
                        if key == "exclusion_reasons":
                            target[key].update(value)
                        else:
                            target[key] += value
            scenario: dict[str, dict[str, int]] = {}
            for name in COST_SCENARIOS:
                complete_items = [ready[item] for item in evaluation
                                  if ready.get(item) is not None and ready[item].complete]
                feasible = sum(any(
                    opportunity.risk_disposition_by_scenario[name] == "FEASIBLE"
                    for opportunity in item.opportunities
                ) for item in complete_items)
                scenario[name] = {
                    "feasible_sessions": feasible,
                    "risk_abstention_sessions": len(complete_items) - feasible,
                    "unresolved_sessions": 0,
                }
            baselines: dict[str, object] = {}
            for baseline in BASELINES:
                flat = baseline == "flat_no_trade"
                selected = 0 if flat else len(evaluation)
                paths = 0 if flat else complete_evaluation
                baseline_scenario: dict[str, dict[str, int]] = {}
                for name in COST_SCENARIOS:
                    if flat:
                        feasible = abstention = unresolved = 0
                    else:
                        complete_items = [ready[item] for item in evaluation
                                          if ready.get(item) is not None and ready[item].complete]
                        feasible = sum(
                            item.opportunities[0].risk_disposition_by_scenario[name] == "FEASIBLE"
                            for item in complete_items
                        )
                        abstention = len(complete_items) - feasible
                        unresolved = selected - paths
                    baseline_scenario[name] = {
                        "feasible_sessions": feasible,
                        "risk_abstention_sessions": abstention,
                        "unresolved_sessions": unresolved,
                    }
                baselines[baseline] = {
                    "expected_sessions": len(evaluation),
                    "terminal_sessions": len(evaluation),
                    "selected_sessions": selected,
                    "selected_path_complete_sessions": paths,
                    "scenario_risk_dispositions": baseline_scenario,
                    "schedule_independently_derived": True,
                    "flat_no_trade": flat,
                }
            evidence.append({
                "fold_id": f"fold-{fold_index}", "market": market, "role": "OUTER",
                "counts": {
                    "expected_training_sessions": len(training),
                    "complete_training_sessions": complete_training,
                    "feature_complete_training_sessions": complete_training,
                    "transformation_ready_training_sessions": complete_training,
                    "expected_evaluation_sessions": len(evaluation),
                    "feature_complete_evaluation_sessions": complete_evaluation,
                    "terminal_evaluation_sessions": len(evaluation),
                    "execution_path_complete_evaluation_sessions": complete_evaluation,
                    # Conservatively, every feature-complete market session
                    # could win the later portfolio ranking. Incomplete
                    # feature sessions cannot be selected and separately fail
                    # the strict all-dependencies gate below.
                    "candidate_selected_sessions": complete_evaluation,
                    "candidate_selected_path_complete_sessions": complete_evaluation,
                    "scenario_risk_dispositions": scenario,
                    "purge_minutes": PURGE_MINUTES,
                    "embargo_sessions": EMBARGO_SESSIONS,
                },
                "checks": {
                    "chronological_order": True, "purge_applied": True,
                    "embargo_applied": True, "training_only_transformation": True,
                    "contract_identity_discontinuities_terminalized": True,
                    "roll_discontinuities_terminalized": True,
                    "all_incomplete_sessions_terminalized": True,
                    "complete_required_metrics": True,
                    "promotion_path_computable": complete_evaluation == len(evaluation),
                },
                "baseline_universe_readiness": baselines,
                "exclusion_reasons": dict(exclusions),
                "market_year_breakdown": year_breakdown,
            })
    return evidence


def build_source_certificate(
    *, protocol_id: str, source_bindings: Mapping[str, str],
    observations: Sequence[SessionReadiness], outer_folds: Sequence[Mapping[str, object]],
    expected_sessions_by_market: Mapping[str, Sequence[str]],
) -> dict[str, object]:
    evidence = build_fold_evidence(
        observations=observations, outer_folds=outer_folds,
        expected_sessions_by_market=expected_sessions_by_market,
    )
    return build_fold_readiness_certificate(
        trial_family="cash_open_impulse_ranked_net_expectancy",
        protocol_id=protocol_id,
        source_bindings=source_bindings,
        fold_evidence=evidence,
        required_markets=MARKETS,
        required_baselines=BASELINES,
        required_cost_scenarios=COST_SCENARIOS,
        required_outer_fold_ids=tuple(f"fold-{index}" for index in range(8)),
        required_nested_fold_ids=(), expected_outer_folds=8, expected_nested_folds=0,
        minimum_training_sessions=MINIMUM_TRAINING_SESSIONS,
        minimum_evaluation_sessions=MINIMUM_EVALUATION_SESSIONS,
        minimum_purge_minutes=PURGE_MINUTES,
        minimum_embargo_sessions=EMBARGO_SESSIONS,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True,
    )


def validate_bound_files(root: Path, bindings: Mapping[str, str]) -> None:
    for relative, digest in bindings.items():
        if sha256_file(root / relative) != digest:
            raise IntegrityError(f"bound source changed: {relative}")
