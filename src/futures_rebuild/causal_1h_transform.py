"""Bounded one-day 1h transformation; historical authority is supplied elsewhere.

Extracted from the baseline 1m producer. The 1m implementation remains untouched.
"""
from collections import defaultdict
from bisect import bisect_left
from collections.abc import Mapping
from types import SimpleNamespace
from .canonical import sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .foundation.records import ProviderBar
from .foundation.identity import DefinitionIndex
from .foundation.economics import EconomicsRuleBook
from .causal_observation_canary import (_causal_definition, _resolve_multiplier,
    _project_grouping, _bar_aggregate, _bar_dict, ECONOMICS_RULEBOOK_SHA256)
SECOND_NS=1_000_000_000
MINUTE_NS=60*SECOND_NS
HOUR_NS=60*MINUTE_NS
DAY_NS=24*HOUR_NS
AVAILABILITY_LAG_NS=5*SECOND_NS

def _comparison(
    *,
    row_id: str,
    comparison_cadence: str,
    observed: Mapping[str, int],
    reference: Mapping[str, int] | None,
    complete: bool,
) -> dict[str, object]:
    if reference is None:
        result, exception = "SOURCE_MISSING", "REFERENCE_SOURCE_MISSING_NO_OVERWRITE"
    elif not complete:
        result, exception = "NOT_COMPARABLE", "INCOMPLETE_INTERVAL_NO_OVERWRITE"
    else:
        equal = all(observed[name] == reference[name] for name in ("open_nano", "high_nano", "low_nano", "close_nano", "volume"))
        result = "MATCH" if equal else "DISAGREEMENT"
        exception = "NONE" if equal else "PRESERVE_BOTH_NO_OVERWRITE"
    core = {
        "row_id": row_id,
        "source_cadence": "1h",
        "comparison_cadence": comparison_cadence,
        "interval_boundary_compatible": True,
        "result": result,
        "exception_state": exception,
    }
    return {"comparison_id": sha256_json(core), **core}

def build_day(
    *,
    context: SimpleNamespace,
    market: str,
    window: Mapping[str, object],
    decoded: SimpleNamespace,
    allowed_roots: frozenset[str],
    economics_rulebook: EconomicsRuleBook,
    prior_observation: Mapping[str, object] | None = None,
) -> tuple[dict, dict]:
    """Build one deterministic market candidate from already-authorized rows."""

    if market not in allowed_roots:
        raise UnauthorizedOperation("causal observation market is outside the exact scope")
    start_ns, end_ns = window["start_ns"], window["end_ns"]
    definitions = DefinitionIndex(decoded.definitions)
    support_index = sorted(decoded.support_rows)
    support_times = [r[0] for r in support_index]
    bars = tuple(sorted(decoded.primary_1h, key=lambda row: (row.event_at_ns, row.row_sha256)))
    if not bars or any(not start_ns <= row.event_at_ns < end_ns for row in bars):
        raise IntegrityError("canary primary rows are empty or outside the exact window")
    if len({(row.event_at_ns, row.instrument_id) for row in bars}) != len(bars):
        raise IntegrityError("canary primary source has duplicate time/instrument rows")

    observations: list[dict[str, object]] = []
    missingness: list[dict[str, object]] = []
    rolls: list[dict[str, object]] = []
    quality: list[dict[str, object]] = []
    cadence: list[dict[str, object]] = []
    initial_prior = dict(prior_observation) if prior_observation is not None else None
    previous_observation = initial_prior

    for bar in bars:
        bar_end = bar.event_at_ns + HOUR_NS
        available = bar_end + AVAILABILITY_LAG_NS
        definition = _causal_definition(definitions, bar, available)
        multiplier, multiplier_state = _resolve_multiplier(
            rulebook=economics_rulebook,
            market=market,
            definition=definition,
        )
        session_id, trade_date, group_start, group_end = _project_grouping(bar.event_at_ns)
        core: dict[str, object] = {
            "market": market,
            "source_contract_id": context.source_contract_id,
            "source_release_id": context.source_release_id,
            "source_file_path": bar.source_file_path,
            "source_file_sha256": bar.source_file_sha256,
            "source_row_sha256": bar.row_sha256,
            "source_cadence": "1h",
            "bar_start_ns": bar.event_at_ns,
            "bar_end_ns": bar_end,
            "source_timestamp_ns": bar.event_at_ns,
            "available_at_ns": available,
            "decision_eligible_at_ns": available,
            "publisher_id": bar.publisher_id,
            "instrument_id": bar.instrument_id,
            "raw_symbol": definition.raw_symbol,
            "actual_contract": definition.raw_symbol,
            "definition_source_file_path": definition.source_file_path,
            "definition_source_file_sha256": definition.source_file_sha256,
            "definition_row_sha256": definition.row_sha256,
            "definition_event_at_ns": definition.ts_event_ns,
            "definition_received_at_ns": definition.ts_recv_ns,
            "listing_activation_ns": definition.activation_ns,
            "expiration_ns": definition.expiration_ns,
            "open_nano": bar.open_nano,
            "high_nano": bar.high_nano,
            "low_nano": bar.low_nano,
            "close_nano": bar.close_nano,
            "volume": bar.volume,
            "currency": definition.currency,
            "min_price_increment_nano": definition.min_price_increment_nano,
            "multiplier_nano": multiplier,
            "project_session_id": session_id,
            "project_trade_date": trade_date,
            "project_grouping_start_ns": group_start,
            "project_grouping_end_ns": group_end,
            "project_timezone": "America/Chicago",
            "official_schedule_state": "UNKNOWN_FAIL_CLOSED",
        }
        row = {"row_id": sha256_json(core), **core}
        observations.append(row)
        evidence = {
            "market": market,
            "source_row_sha256": bar.row_sha256,
            "interval_start_ns": bar.event_at_ns,
            "interval_end_ns": bar_end,
            "authority": "DECODED_CANONICAL_SOURCE_ROW",
        }
        evidence_sha = sha256_json(evidence)
        missing_core = {
            "observation_row_id": row["row_id"],
            "market": market,
            "interval_start_ns": bar.event_at_ns,
            "interval_end_ns": bar_end,
            "state": "OBSERVED_VALID",
            "authority": "DECODED_CANONICAL_SOURCE_ROW",
            "evidence_sha256": evidence_sha,
        }
        missingness.append({"evidence_id": sha256_json(missing_core), **missing_core})
        prior_contract = str(previous_observation["actual_contract"]) if previous_observation else definition.raw_symbol
        is_roll = previous_observation is not None and prior_contract != definition.raw_symbol
        rolls.append(
            {
                "row_id": row["row_id"],
                "actual_contract_before": prior_contract,
                "actual_contract_after": definition.raw_symbol,
                "effective_time_ns": bar.event_at_ns if is_roll else None,
                "causal_selection_evidence_sha256": sha256_json(
                    {
                        "definition_row_sha256": definition.row_sha256,
                        "definition_received_at_ns": definition.ts_recv_ns,
                        "prior_contract": prior_contract,
                    }
                ),
                "roll_flag": is_roll,
                "price_discontinuity_flag": bool(
                    is_roll and int(previous_observation["close_nano"]) != bar.open_nano
                ) if previous_observation else False,
                "crossing_status": "ROLL_BOUNDARY_UNADJUSTED" if is_roll else "NO_CROSSING",
            }
        )
        flags = [
            "OHLC_VOLUME_TIMESTAMP_VALID",
            f"MULTIPLIER_{multiplier_state}",
            f"ECONOMICS_RULEBOOK_SHA256_{ECONOMICS_RULEBOOK_SHA256}",
        ]
        if min(bar.open_nano, bar.high_nano, bar.low_nano, bar.close_nano) < 0:
            flags.append("PROVIDER_VALID_NEGATIVE_PRICE")
        quality.append(
            {
                "row_id": row["row_id"],
                "row_identity_sha256": row["row_id"],
                "ohlc_valid": True,
                "volume_valid": True,
                "timestamp_order_valid": True,
                "duplicate_state": "UNIQUE",
                "source_contract_id": context.source_contract_id,
                "source_release_id": context.source_release_id,
                "source_file_sha256": bar.source_file_sha256,
                "quality_flags": flags,
            }
        )
        previous_observation = row

    if initial_prior is not None and int(initial_prior["bar_end_ns"]) >= start_ns:
        gap_start = int(initial_prior["bar_end_ns"])
        gap_end = int(observations[0]["bar_start_ns"])
        if gap_end > gap_start:
            support = [
                {"event_at_ns": ts, "family": family, "row_sha256": row_sha}
                for ts, family, row_sha in support_index[bisect_left(support_times, gap_start):bisect_left(support_times, gap_end)]
                if gap_start <= ts < gap_end
            ]
            evidence_sha = sha256_json(
                {"gap_start_ns": gap_start, "gap_end_ns": gap_end, "support_rows": support}
            )
            gap_core = {
                "observation_row_id": None,
                "market": market,
                "interval_start_ns": gap_start,
                "interval_end_ns": gap_end,
                "state": "UNKNOWN_FAIL_CLOSED",
                "authority": "OBSERVED_ABSENCE_WITH_STATUS_REVIEW_NO_SCHEDULE_AUTHORITY",
                "evidence_sha256": evidence_sha,
            }
            missingness.append({"evidence_id": sha256_json(gap_core), **gap_core})

    for left, right in zip(observations, observations[1:]):
        gap_start = int(left["bar_end_ns"])
        gap_end = int(right["bar_start_ns"])
        if gap_end <= gap_start:
            continue
        support = [
            {"event_at_ns": ts, "family": family, "row_sha256": row_sha}
            for ts, family, row_sha in support_index[bisect_left(support_times, gap_start):bisect_left(support_times, gap_end)]
            if gap_start <= ts < gap_end
        ]
        evidence_sha = sha256_json(
            {"gap_start_ns": gap_start, "gap_end_ns": gap_end, "support_rows": support}
        )
        gap_core = {
            "observation_row_id": None,
            "market": market,
            "interval_start_ns": gap_start,
            "interval_end_ns": gap_end,
            "state": "UNKNOWN_FAIL_CLOSED",
            "authority": "OBSERVED_ABSENCE_WITH_STATUS_REVIEW_NO_SCHEDULE_AUTHORITY",
            "evidence_sha256": evidence_sha,
        }
        missingness.append({"evidence_id": sha256_json(gap_core), **gap_core})

    for bar, observation in zip(bars, observations, strict=True):
        start = bar.event_at_ns
        minutes = [decoded.reference_1m[t] for t in sorted(decoded.reference_1m)
                   if start <= t < start + HOUR_NS]
        complete = (len(minutes) == 60 and
                    [r.event_at_ns for r in minutes] == list(range(start, start + HOUR_NS, MINUTE_NS)) and
                    {(r.publisher_id, r.instrument_id) for r in minutes} == {(bar.publisher_id, bar.instrument_id)})
        cadence.append(_comparison(row_id=observation['row_id'], comparison_cadence='1m',
            observed=_bar_dict(bar), reference=_bar_aggregate(minutes) if minutes else None, complete=complete))

    return {"observations": observations, "missingness": missingness, "roll": rolls, "quality": quality, "cadence": cadence}, observations[-1]
