"""Inactive Phase 4 foundation for directional 60-minute path targets."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone

from .canonical import sha256_json
from .errors import IntegrityError
from .exchange_calendar import CalendarSession, TradingStateInterval, VerifiedExchangeCalendar
from .fixed_hour_ridge_audit_successor import MINUTE, OUTCOME_BARS, strict_outcome_path
from .time_policy import PROJECT_TIMEZONE_NAME


SCHEMA = "fixed_hour_directional_outcome_foundation/1.4.0"
OUTPUT_SCHEMA = "fixed_hour_directional_path_outcome/1.2.0"
OPPORTUNITY_SCHEMA = "fixed_hour_globex_opportunity/1.1.0"
FEATURE_LOOKBACK_MINUTES = 21
DECISION_DELAY_SECONDS = 5
HOUR = 60 * MINUTE
SYNTHETIC_SOURCE_CONTRACT_ID = sha256_json({"fixed_hour": "synthetic_source_contract_v1"})
SYNTHETIC_SOURCE_RELEASE_ID = sha256_json({"fixed_hour": "synthetic_source_release_v1"})


def frozen_outcome_foundation_contract() -> dict[str, object]:
    """Return the exact inactive Stage 3 target definition."""

    core: dict[str, object] = {
        "schema_version": SCHEMA,
        "state": "PREPARED_NOT_ACTIVE",
        "authority": {
            "development_rows": False,
            "holdout_or_forward": False,
            "provider_or_network": False,
            "registration_or_execution": False,
            "trading": False,
        },
        "stage_2_5_successor_required": True,
        "output_schema_version": OUTPUT_SCHEMA,
        "opportunity_schema_version": OPPORTUNITY_SCHEMA,
        "timing": {
            "timezone": PROJECT_TIMEZONE_NAME,
            "scope": "PREDECLARED_PLUS_22_MINUTE_ALIGNMENT_IN_EVERY_VERIFIED_GLOBEX_OPEN_INTERVAL",
            "first_decision": "AFTER_21_CONSECUTIVE_OPEN_BARS_PRODUCING_20_TICK_CHANGES",
            "cadence_minutes": 60,
            "decision_delay_seconds": DECISION_DELAY_SECONDS,
            "entry": "NEXT_ONE_MINUTE_BAR_OPEN_PRINT_PROXY",
            "path_minutes": 60,
            "terminal_exit": "ENTRY_PLUS_60_MINUTES_BAR_OPEN_PRINT_PROXY",
            "interval_boundary": "FEATURE_ENTRY_PATH_AND_EXIT_BAR_MUST_FIT_ONE_OPEN_INTERVAL",
            "alignment_claim_ceiling": "ONLY_THIS_FIXED_ALIGNMENT_NOT_ALL_POSSIBLE_HOURLY_STARTS",
        },
        "model_target_components": [
            "terminal_change_ticks",
            "maximum_upside_excursion_ticks",
            "maximum_downside_excursion_ticks",
        ],
        "derived_directional_quality": {
            "long_raw_mfe_mae": "MAXIMUM_UPSIDE_EXCURSION_TICKS/MAX(MAXIMUM_DOWNSIDE_EXCURSION_TICKS,1)",
            "short_raw_mfe_mae": "MAXIMUM_DOWNSIDE_EXCURSION_TICKS/MAX(MAXIMUM_UPSIDE_EXCURSION_TICKS,1)",
            "raw_path_favorable": "RATIO_STRICTLY_GREATER_THAN_1",
            "tradable_favorable": "DEFER_TO_COST_ADJUSTED_STAGE_5_RATIO_STRICTLY_GREATER_THAN_1",
            "cost_adjustment": "DEFER_TO_FROZEN_STAGE_5_COST_MODEL",
        },
        "maturity": {
            "available_at": "MAX_DECISION_ELIGIBLE_AT_NS_ACROSS_ENTRY_PATH_AND_EXIT",
            "before_available_at": "UNMATURED_TARGETS_WITHHELD",
            "missing_or_sparse_path": "INDETERMINATE_NO_IMPUTATION_NO_ZERO_LABEL",
            "contract_roll": "ROLL_UNRESOLVED_NO_LABEL",
        },
        "separation": {
            "feature_columns_in_outcome": False,
            "outcome_columns_available_to_feature_construction": False,
            "costs_embedded_in_targets": False,
            "entry_exit_are_executable_fills": False,
        },
        "downstream_constraints": {
            "mfe_mae_is_realized_pnl": False,
            "mfe_mae_encodes_excursion_order": False,
            "excursion_timestamps_must_remain_visible": True,
            "fixed_exit_pnl_must_remain_visible": True,
            "stage_5_must_freeze_entry_threshold_and_exit_rule": True,
            "wfa_split_unit": "COMPLETE_MARKET_TRADE_DATE",
            "within_session_rows_are_independent": False,
            "outer_results_may_change_targets": False,
            "path_dependent_exit_claims_allowed": False,
            "stop_target_or_trailing_exit_claims_allowed": False,
        },
    }
    return {**core, "contract_id": sha256_json(core)}


def validate_outcome_foundation_contract(
    payload: Mapping[str, object],
) -> dict[str, object]:
    expected = frozen_outcome_foundation_contract()
    if dict(payload) != expected:
        raise IntegrityError("directional outcome-foundation contract drifted")
    return dict(payload)


def _hourly_opportunity_grid(
    session: CalendarSession,
    *,
    calendar_release_id: str,
    calendar_authority: str,
    source_contract_id: str,
    source_release_id: str,
) -> list[dict[str, object]]:
    market = session.market
    trade_date = session.trade_date.isoformat()

    if (
        type(market) is not str or not market or not market.isascii()
        or type(trade_date) is not str
        or type(calendar_release_id) is not str
        or calendar_authority not in {"VERIFIED_RELEASE", "SYNTHETIC_ENGINEERING_ONLY"}
        or any(
            type(value) is not str or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
            for value in (calendar_release_id, source_contract_id, source_release_id)
        )
    ):
        raise IntegrityError("hourly opportunity identity is invalid")
    try:
        date.fromisoformat(trade_date)
    except ValueError as exc:
        raise IntegrityError("hourly opportunity trade date is invalid") from exc
    opportunities: list[dict[str, object]] = []
    previous_end = -1
    for interval in (item for item in session.intervals if item.state == "OPEN"):
        if not isinstance(interval, TradingStateInterval) or interval.state != "OPEN":
            raise IntegrityError("hourly opportunity grid requires OPEN calendar intervals")
        start = int(interval.starts_at_utc.timestamp()) * (MINUTE // 60)
        end = int(interval.ends_at_utc.timestamp()) * (MINUTE // 60)
        if start < previous_end:
            raise IntegrityError("hourly opportunity intervals overlap or are unordered")
        anchor = start + FEATURE_LOOKBACK_MINUTES * MINUTE
        while True:
            decision = anchor + DECISION_DELAY_SECONDS * (MINUTE // 60)
            entry = anchor + MINUTE
            terminal_exit = entry + OUTCOME_BARS * MINUTE
            if terminal_exit + MINUTE > end:
                break
            core: dict[str, object] = {
                "schema_version": OPPORTUNITY_SCHEMA,
                "outcome_foundation_contract_id": frozen_outcome_foundation_contract()["contract_id"],
                "market": market,
                "trade_date": trade_date,
                "calendar_release_id": calendar_release_id,
                "calendar_authority": calendar_authority,
                "source_contract_id": source_contract_id,
                "source_release_id": source_release_id,
                "open_interval_start_ns": start,
                "open_interval_end_ns": end,
                "interval_opportunity_ordinal": (anchor - start - FEATURE_LOOKBACK_MINUTES * MINUTE) // HOUR,
                "decision_at_ns": decision,
                "entry_bar_start_ns": entry,
                "terminal_exit_bar_start_ns": terminal_exit,
            }
            opportunities.append({**core, "opportunity_id": sha256_json(core)})
            anchor += HOUR
        previous_end = end
    return opportunities


def globex_hourly_opportunity_grid(
    calendar: VerifiedExchangeCalendar,
    *,
    market: str,
    trade_date: str,
    source_contract_id: str,
    source_release_id: str,
) -> list[dict[str, object]]:
    """Build opportunities only from a verified calendar release."""

    if not isinstance(calendar, VerifiedExchangeCalendar):
        raise IntegrityError("hourly opportunity grid requires a verified calendar")
    calendar.verify()
    try:
        parsed = date.fromisoformat(trade_date)
        session = calendar.sessions[(market, parsed)]
    except (ValueError, KeyError) as exc:
        raise IntegrityError("verified calendar lacks the requested market/trade date") from exc
    return _hourly_opportunity_grid(
        session,
        calendar_release_id=calendar.receipt.release_id,
        calendar_authority="VERIFIED_RELEASE",
        source_contract_id=source_contract_id,
        source_release_id=source_release_id,
    )


def synthetic_globex_hourly_opportunity_grid(
    session: CalendarSession,
    *,
    calendar_release_id: str,
) -> list[dict[str, object]]:
    """Build engineering-only opportunities that carry no real-data authority."""

    if not isinstance(session, CalendarSession):
        raise IntegrityError("synthetic schedule requires one calendar session")
    return _hourly_opportunity_grid(
        session,
        calendar_release_id=calendar_release_id,
        calendar_authority="SYNTHETIC_ENGINEERING_ONLY",
        source_contract_id=SYNTHETIC_SOURCE_CONTRACT_ID,
        source_release_id=SYNTHETIC_SOURCE_RELEASE_ID,
    )


def validate_globex_opportunity(
    payload: Mapping[str, object], *, calendar: VerifiedExchangeCalendar | None = None,
) -> dict[str, object]:
    """Require an opportunity emitted by the frozen calendar grid."""

    required = {
        "schema_version", "outcome_foundation_contract_id", "market", "trade_date",
        "calendar_release_id", "calendar_authority", "source_contract_id", "source_release_id",
        "open_interval_start_ns", "open_interval_end_ns",
        "interval_opportunity_ordinal", "decision_at_ns", "entry_bar_start_ns",
        "terminal_exit_bar_start_ns", "opportunity_id",
    }
    if set(payload) != required:
        raise IntegrityError("hourly opportunity fields are not exact")
    core = {key: value for key, value in payload.items() if key != "opportunity_id"}
    if (
        payload.get("schema_version") != OPPORTUNITY_SCHEMA
        or payload.get("outcome_foundation_contract_id")
        != frozen_outcome_foundation_contract()["contract_id"]
        or payload.get("opportunity_id") != sha256_json(core)
    ):
        raise IntegrityError("hourly opportunity identity changed")
    if any(type(payload.get(name)) is not int for name in (
        "open_interval_start_ns", "open_interval_end_ns", "interval_opportunity_ordinal",
        "decision_at_ns", "entry_bar_start_ns", "terminal_exit_bar_start_ns",
    )):
        raise IntegrityError("hourly opportunity timing is invalid")
    start = int(payload["open_interval_start_ns"])
    end = int(payload["open_interval_end_ns"])
    ordinal = int(payload["interval_opportunity_ordinal"])
    anchor = start + FEATURE_LOOKBACK_MINUTES * MINUTE + ordinal * HOUR
    if (
        ordinal < 0
        or payload["decision_at_ns"] != anchor + DECISION_DELAY_SECONDS * (MINUTE // 60)
        or payload["entry_bar_start_ns"] != anchor + MINUTE
        or payload["terminal_exit_bar_start_ns"] != anchor + MINUTE + OUTCOME_BARS * MINUTE
        or int(payload["terminal_exit_bar_start_ns"]) + MINUTE > end
    ):
        raise IntegrityError("hourly opportunity is outside the frozen grid")
    authority = payload.get("calendar_authority")
    if authority == "VERIFIED_RELEASE":
        if not isinstance(calendar, VerifiedExchangeCalendar):
            raise IntegrityError("verified opportunity requires its calendar authority")
        calendar.verify()
        try:
            session = calendar.sessions[(str(payload["market"]), date.fromisoformat(str(payload["trade_date"])))]
        except (ValueError, KeyError) as exc:
            raise IntegrityError("opportunity market/trade date is absent from its calendar") from exc
        if payload["calendar_release_id"] != calendar.receipt.release_id:
            raise IntegrityError("opportunity calendar release differs from authority")
    elif authority == "SYNTHETIC_ENGINEERING_ONLY":
        if calendar is not None or (
            payload.get("source_contract_id") != SYNTHETIC_SOURCE_CONTRACT_ID
            or payload.get("source_release_id") != SYNTHETIC_SOURCE_RELEASE_ID
        ):
            raise IntegrityError("synthetic opportunity authority is invalid")
        session = CalendarSession(
            str(payload["market"]), date.fromisoformat(str(payload["trade_date"])),
            (TradingStateInterval(
                "OPEN",
                datetime.fromtimestamp(start / 1_000_000_000, tz=timezone.utc),
                datetime.fromtimestamp(end / 1_000_000_000, tz=timezone.utc),
            ),),
        )
    else:
        raise IntegrityError("opportunity calendar authority is invalid")
    generated = _hourly_opportunity_grid(
        session,
        calendar_release_id=str(payload["calendar_release_id"]),
        calendar_authority=str(authority),
        source_contract_id=str(payload["source_contract_id"]),
        source_release_id=str(payload["source_release_id"]),
    )
    if dict(payload) not in generated:
        raise IntegrityError("hourly opportunity was not emitted by the frozen grid")
    return dict(payload)


def build_directional_path_outcome(
    rows: Sequence[Mapping[str, object]],
    opportunity: Mapping[str, object],
    *,
    expected_contract: str,
    as_of_ns: int,
    calendar: VerifiedExchangeCalendar | None = None,
) -> dict[str, object]:
    """Build target components without exposing them before the path matures."""

    if (
        type(as_of_ns) is not int
        or type(expected_contract) is not str
        or not expected_contract
    ):
        raise IntegrityError("directional outcome identity is invalid")
    scheduled = validate_globex_opportunity(opportunity, calendar=calendar)
    decision_at_ns = int(scheduled["decision_at_ns"])
    contract = frozen_outcome_foundation_contract()
    outcome = strict_outcome_path(
        rows, decision_at_ns, expected_contract=expected_contract,
        expected_market=str(scheduled["market"]),
        expected_trade_date=str(scheduled["trade_date"]),
        expected_source_contract_id=str(scheduled["source_contract_id"]),
        expected_source_release_id=str(scheduled["source_release_id"]),
    )
    status = str(outcome.get("score_status"))
    base: dict[str, object] = {
        "schema_version": OUTPUT_SCHEMA,
        "outcome_foundation_contract_id": contract["contract_id"],
        "opportunity_id": scheduled["opportunity_id"],
        "market": scheduled["market"],
        "trade_date": scheduled["trade_date"],
        "calendar_release_id": scheduled["calendar_release_id"],
        "calendar_authority": scheduled["calendar_authority"],
        "source_contract_id": scheduled["source_contract_id"],
        "source_release_id": scheduled["source_release_id"],
        "decision_at_ns": decision_at_ns,
        "expected_contract": expected_contract,
    }
    if status != "SCORABLE":
        core = {
            **base,
            "status": status,
            "target_available_at_ns": None,
            "targets": None,
        }
        return {**core, "outcome_id": sha256_json(core)}

    entry = outcome["entry"]
    exit_row = outcome["exit"]
    path = outcome["path"]
    assert isinstance(entry, Mapping) and isinstance(exit_row, Mapping) and isinstance(path, list)
    target_rows = [*path, exit_row]
    required = (
        "bar_start_ns", "bar_end_ns", "available_at_ns", "decision_eligible_at_ns",
        "open_nano", "high_nano", "low_nano", "close_nano",
        "min_price_increment_nano", "multiplier_nano",
    )
    for row in target_rows:
        if any(type(row.get(name)) is not int for name in required):
            raise IntegrityError("directional outcome row has invalid types")
        open_, high, low, close = (
            int(row[name]) for name in ("open_nano", "high_nano", "low_nano", "close_nano")
        )
        if (
            high < max(open_, close)
            or low > min(open_, close)
            or high < low
            or int(row["min_price_increment_nano"]) <= 0
            or int(row["multiplier_nano"]) <= 0
            or any(value % int(row["min_price_increment_nano"]) for value in (open_, high, low, close))
            or not int(row["bar_end_ns"]) <= int(row["available_at_ns"]) <= int(row["decision_eligible_at_ns"])
        ):
            raise IntegrityError("directional outcome row violates price, timing, or economics rules")
    economics = {
        (int(row["min_price_increment_nano"]), int(row["multiplier_nano"]))
        for row in target_rows
    }
    if len(economics) != 1:
        raise IntegrityError("directional outcome economics change inside the path")

    tick, _ = next(iter(economics))
    entry_open = int(entry["open_nano"])
    exit_open = int(exit_row["open_nano"])
    extrema = outcome["extrema"]
    assert isinstance(extrema, Mapping)
    maximum_nano = int(extrema["maximum_high_nano"])
    minimum_nano = int(extrema["minimum_low_nano"])
    upside = max(maximum_nano - entry_open, 0) / tick
    downside = max(entry_open - minimum_nano, 0) / tick
    terminal = (exit_open - entry_open) / tick
    if not all(math.isfinite(value) for value in (upside, downside, terminal)):
        raise IntegrityError("directional outcome target is nonfinite")
    available_at = max(int(row["decision_eligible_at_ns"]) for row in target_rows)
    if as_of_ns < available_at:
        core = {
            **base,
            "status": "UNMATURED",
            "target_available_at_ns": available_at,
            "targets": None,
        }
        return {**core, "outcome_id": sha256_json(core)}

    targets = {
        "terminal_change_ticks": terminal,
        "maximum_upside_excursion_ticks": upside,
        "maximum_downside_excursion_ticks": downside,
        "long_raw_mfe_mae": upside / max(downside, 1.0),
        "short_raw_mfe_mae": downside / max(upside, 1.0),
    }
    core = {
        **base,
        "status": "MATURED",
        "target_available_at_ns": available_at,
        "targets": targets,
        "audit": {
            "entry_bar_start_ns": int(entry["bar_start_ns"]),
            "terminal_exit_bar_start_ns": int(exit_row["bar_start_ns"]),
            "maximum_upside_bar_start_ns": int(extrema["maximum_high_bar_start_ns"]),
            "maximum_upside_price_source": extrema["maximum_price_source"],
            "maximum_downside_bar_start_ns": int(extrema["minimum_low_bar_start_ns"]),
            "maximum_downside_price_source": extrema["minimum_price_source"],
            "path_bar_starts_ns": [int(row["bar_start_ns"]) for row in path],
            "source_row_ids": [str(row["row_id"]) for row in target_rows],
            "source_row_sha256s": [str(row["source_row_sha256"]) for row in target_rows],
        },
    }
    return {**core, "outcome_id": sha256_json(core)}
