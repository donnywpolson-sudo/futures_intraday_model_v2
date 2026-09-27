"""Independent day reconstruction. Never imports the 1h producer transform."""
from collections import defaultdict
from bisect import bisect_left
from collections.abc import Mapping
from types import SimpleNamespace
from .canonical import sha256_json
from .errors import IntegrityError
from .foundation.records import ProviderBar, NANO, ns_to_datetime
from .foundation.identity import DefinitionIndex
from .foundation.economics import EconomicsRuleBook
from .causal_observation_market_certification import _project_grouping, _aggregate, ECONOMICS_RULEBOOK_SHA256
SECOND_NS=1_000_000_000
MINUTE_NS=60*SECOND_NS
HOUR_NS=3600*SECOND_NS
DAY_NS=86400*SECOND_NS
AVAILABILITY_LAG_NS=5*SECOND_NS

def _comparison(
    row_id: str,
    cadence: str,
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
        result, exception = ("MATCH", "NONE") if equal else ("DISAGREEMENT", "PRESERVE_BOTH_NO_OVERWRITE")
    core = {
        "row_id": row_id,
        "source_cadence": "1h",
        "comparison_cadence": cadence,
        "interval_boundary_compatible": True,
        "result": result,
        "exception_state": exception,
    }
    return {"comparison_id": sha256_json(core), **core}

def reconstruct_day(
    *,
    market: str,
    decoded: SimpleNamespace,
    start_ns: int,
    end_ns: int,
    source_contract_id: str,
    source_release_id: str,
    rulebook: EconomicsRuleBook,
    prior: Mapping[str, object] | None,
) -> tuple[dict[str, list[dict[str, object]]], Mapping[str, object]]:
    definitions = DefinitionIndex(decoded.definitions)
    support_index = sorted(decoded.support_rows)
    support_times = [r[0] for r in support_index]
    bars = sorted(
        (row for row in decoded.primary_1h if start_ns <= row.event_at_ns < end_ns),
        key=lambda row: (row.event_at_ns, row.row_sha256),
    )
    if not bars or len({(row.event_at_ns, row.instrument_id) for row in bars}) != len(bars):
        raise IntegrityError("independent replay primary rows are empty or duplicate")
    observations: list[dict[str, object]] = []
    missingness: list[dict[str, object]] = []
    rolls: list[dict[str, object]] = []
    quality: list[dict[str, object]] = []
    cadence: list[dict[str, object]] = []
    previous = dict(prior) if prior is not None else None
    initial_prior = previous
    for bar in bars:
        bar_end = bar.event_at_ns + HOUR_NS
        available = bar_end + AVAILABILITY_LAG_NS
        definition = definitions.resolve(
            bar,
            decision_at=ns_to_datetime(available, "independent replay availability"),
        )
        resolved = rulebook.resolve(market, definition)
        multiplier = rulebook.rules[market].expected_unit_qty * NANO
        if multiplier != multiplier.to_integral_value() or multiplier <= 0:
            raise IntegrityError("independent replay multiplier is invalid")
        session, trade_date, grouping_start, grouping_end = _project_grouping(bar.event_at_ns)
        core: dict[str, object] = {
            "market": market,
            "source_contract_id": source_contract_id,
            "source_release_id": source_release_id,
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
            "multiplier_nano": int(multiplier),
            "project_session_id": session,
            "project_trade_date": trade_date,
            "project_grouping_start_ns": grouping_start,
            "project_grouping_end_ns": grouping_end,
            "project_timezone": "America/Chicago",
            "official_schedule_state": "UNKNOWN_FAIL_CLOSED",
        }
        observation = {"row_id": sha256_json(core), **core}
        observations.append(observation)
        evidence = {
            "market": market,
            "source_row_sha256": bar.row_sha256,
            "interval_start_ns": bar.event_at_ns,
            "interval_end_ns": bar_end,
            "authority": "DECODED_CANONICAL_SOURCE_ROW",
        }
        missing_core = {
            "observation_row_id": observation["row_id"],
            "market": market,
            "interval_start_ns": bar.event_at_ns,
            "interval_end_ns": bar_end,
            "state": "OBSERVED_VALID",
            "authority": "DECODED_CANONICAL_SOURCE_ROW",
            "evidence_sha256": sha256_json(evidence),
        }
        missingness.append({"evidence_id": sha256_json(missing_core), **missing_core})
        prior_contract = str(previous["actual_contract"]) if previous else definition.raw_symbol
        roll = previous is not None and prior_contract != definition.raw_symbol
        rolls.append({
            "row_id": observation["row_id"],
            "actual_contract_before": prior_contract,
            "actual_contract_after": definition.raw_symbol,
            "effective_time_ns": bar.event_at_ns if roll else None,
            "causal_selection_evidence_sha256": sha256_json({
                "definition_row_sha256": definition.row_sha256,
                "definition_received_at_ns": definition.ts_recv_ns,
                "prior_contract": prior_contract,
            }),
            "roll_flag": roll,
            "price_discontinuity_flag": bool(roll and int(previous["close_nano"]) != bar.open_nano) if previous else False,
            "crossing_status": "ROLL_BOUNDARY_UNADJUSTED" if roll else "NO_CROSSING",
        })
        flags = [
            "OHLC_VOLUME_TIMESTAMP_VALID",
            f"MULTIPLIER_{resolved.provider_unit_qty_state}",
            f"ECONOMICS_RULEBOOK_SHA256_{ECONOMICS_RULEBOOK_SHA256}",
        ]
        if min(bar.open_nano, bar.high_nano, bar.low_nano, bar.close_nano) < 0:
            flags.append("PROVIDER_VALID_NEGATIVE_PRICE")
        quality.append({
            "row_id": observation["row_id"],
            "row_identity_sha256": observation["row_id"],
            "ohlc_valid": True,
            "volume_valid": True,
            "timestamp_order_valid": True,
            "duplicate_state": "UNIQUE",
            "source_contract_id": source_contract_id,
            "source_release_id": source_release_id,
            "source_file_sha256": bar.source_file_sha256,
            "quality_flags": flags,
        })
        previous = observation

    pairs = []
    if initial_prior is not None and int(initial_prior["bar_end_ns"]) >= start_ns:
        pairs.append((initial_prior, observations[0]))
    pairs.extend(zip(observations, observations[1:]))
    for left, right in pairs:
        gap_start, gap_end = int(left["bar_end_ns"]), int(right["bar_start_ns"])
        if gap_end <= gap_start:
            continue
        support = [
            {"event_at_ns": ts, "family": family, "row_sha256": row_sha}
            for ts, family, row_sha in support_index[bisect_left(support_times, gap_start):bisect_left(support_times, gap_end)]
            if gap_start <= ts < gap_end
        ]
        gap_core = {
            "observation_row_id": None,
            "market": market,
            "interval_start_ns": gap_start,
            "interval_end_ns": gap_end,
            "state": "UNKNOWN_FAIL_CLOSED",
            "authority": "OBSERVED_ABSENCE_WITH_STATUS_REVIEW_NO_SCHEDULE_AUTHORITY",
            "evidence_sha256": sha256_json({"gap_start_ns": gap_start, "gap_end_ns": gap_end, "support_rows": support}),
        }
        missingness.append({"evidence_id": sha256_json(gap_core), **gap_core})

    for observation in observations:
        start = observation['bar_start_ns']
        minute_rows = sorted((r for r in decoded.reference_1m.values()
                              if start <= r.event_at_ns < observation['bar_end_ns']), key=lambda r: r.event_at_ns)
        comparable = len(minute_rows) == 60
        for number, row in enumerate(minute_rows):
            comparable = comparable and row.event_at_ns == start + number * MINUTE_NS
            comparable = comparable and (row.publisher_id, row.instrument_id) == (observation['publisher_id'], observation['instrument_id'])
        values = {key: observation[key] for key in ('open_nano','high_nano','low_nano','close_nano','volume')}
        cadence.append(_comparison(observation['row_id'], '1m', values,
                                   _aggregate(minute_rows) if minute_rows else None, comparable))

    return {
        "observations": observations,
        "missingness": missingness,
        "roll": rolls,
        "quality": quality,
        "cadence": cadence,
    }, observations[-1]
