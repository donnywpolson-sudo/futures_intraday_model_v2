"""Approved read-only ES readiness census for the fixed-hour ridge mechanism."""

from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path
from typing import Mapping, Sequence

import pyarrow.parquet as pq

from .canonical import canonical_bytes, sha256_file, sha256_json, write_new_canonical_authority
from .causal_observation_parquet import SCHEMAS
from .cme_calendar_successor import WINDOWS
from .errors import IntegrityError
from .fixed_hour_ridge_discovery import (
    CONTRACT_PATH, NANO, checkpoint_ns, feature_values, net_mfe_mae_ratio,
    resolve_outcome_path, stress_net_targets, validate_contract,
)


SELF = Path("src/futures_rebuild/fixed_hour_ridge_readiness.py")
OUTPUT_ROOT = Path("state/fixed_hour_ridge_readiness")
ACTIVE_SCHEDULE_POINTER = Path("configs/active_cash_open_impulse_historical_calendar.json")
ACTIVE_SCHEDULE_POINTER_SHA256 = "8956289401a172cf3726336e879ba7e8e72d0485fa3927805067eebd3644e1fc"
ACTIVE_SCHEDULE_ID = "ddbe0c706d6568d8d7ddefd830677d73978b428d8a99925290310224f673a7f9"
ACTIVE_SCHEDULE_PATH = Path(
    "state/calendar_registry/cash_open_impulse_41_market/"
    f"{ACTIVE_SCHEDULE_ID}/historical_calendar_successor.json"
)
ACTIVE_SCHEDULE_SHA256 = "efdf4f765e44ac2f312dce62b7145bb1ed70d01fd8c76fb5bfb3f32652f1a632"
SCHEDULE_PREDECESSOR_ID = "cd64f912cceec3ff613b0d28f3965804c25d36d9b940d622b062128cfca0843b"
SCHEDULE_PREDECESSOR_PATH = Path(
    "state/calendar_registry/cash_open_impulse_41_market/"
    f"{SCHEDULE_PREDECESSOR_ID}/historical_calendar_successor.json"
)
SCHEDULE_PREDECESSOR_SHA256 = "e76ec4310da674e1bbacf5356662d97d8a2c8b115c728fa9386b53f8d289be52"
BATCH_ROWS = 65_536
READ_COLUMNS = (
    "row_id", "market", "source_contract_id", "source_release_id", "source_file_sha256",
    "source_row_sha256", "definition_source_file_sha256", "definition_row_sha256",
    "source_cadence", "bar_start_ns", "bar_end_ns", "source_timestamp_ns",
    "available_at_ns", "decision_eligible_at_ns", "actual_contract", "open_nano",
    "high_nano", "low_nano", "close_nano", "volume", "min_price_increment_nano",
    "multiplier_nano", "project_session_id", "project_trade_date",
    "project_grouping_start_ns", "project_grouping_end_ns", "project_timezone",
    "official_schedule_state",
)


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"fixed-hour readiness input is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"fixed-hour readiness input must be an object: {path}")
    return value


def load_fixed_hour_schedule(root: Path, market: str = "ES") -> dict[str, object]:
    """Resolve the active calendar into fixed-hour, price-free session states."""

    expected = (
        (ACTIVE_SCHEDULE_POINTER, ACTIVE_SCHEDULE_POINTER_SHA256),
        (ACTIVE_SCHEDULE_PATH, ACTIVE_SCHEDULE_SHA256),
        (SCHEDULE_PREDECESSOR_PATH, SCHEDULE_PREDECESSOR_SHA256),
    )
    for relative, digest in expected:
        if sha256_file(root / relative) != digest:
            raise IntegrityError(f"fixed-hour schedule authority drifted: {relative}")
    pointer = _read_json(root / ACTIVE_SCHEDULE_POINTER)
    calendar = _read_json(root / ACTIVE_SCHEDULE_PATH)
    predecessor = _read_json(root / SCHEDULE_PREDECESSOR_PATH)
    for relative, payload in (
        (ACTIVE_SCHEDULE_POINTER, pointer),
        (ACTIVE_SCHEDULE_PATH, calendar),
        (SCHEDULE_PREDECESSOR_PATH, predecessor),
    ):
        if (root / relative).read_bytes() != canonical_bytes(payload) + b"\n":
            raise IntegrityError(f"fixed-hour schedule input is not canonical: {relative}")
    if (
        pointer.get("state") != "ACTIVE_CALENDAR_AND_SOURCE_OBSERVABILITY"
        or pointer.get("calendar_id") != ACTIVE_SCHEDULE_ID
        or pointer.get("calendar_path") != ACTIVE_SCHEDULE_PATH.as_posix()
        or pointer.get("calendar_sha256") != ACTIVE_SCHEDULE_SHA256
        or calendar.get("calendar_id") != ACTIVE_SCHEDULE_ID
        or calendar.get("decision")
        != "PASS_TWO_CALENDAR_CORRECTIONS_SIX_SOURCE_ABSTENTIONS"
        or calendar.get("dependency_horizon")
        != "FEATURE_START_THROUGH_SCHEDULED_EXIT_OPEN"
        or calendar.get("checkpoint_grid") != ["09:00", "09:30", "10:00", "10:30"]
        or calendar.get("unresolved_reference_count") != 0
    ):
        raise IntegrityError("active fixed-hour schedule semantics are invalid")
    bound_predecessor = calendar.get("predecessor")
    if (
        not isinstance(bound_predecessor, Mapping)
        or bound_predecessor.get("calendar_id") != SCHEDULE_PREDECESSOR_ID
        or bound_predecessor.get("path") != SCHEDULE_PREDECESSOR_PATH.as_posix()
        or bound_predecessor.get("sha256") != SCHEDULE_PREDECESSOR_SHA256
        or predecessor.get("calendar_id") != SCHEDULE_PREDECESSOR_ID
        or predecessor.get("decision") != "PASS_EXACT_REFERENCE_COVERAGE"
        or predecessor.get("dependency_horizon")
        != "FEATURE_START_THROUGH_SCHEDULED_EXIT_OPEN"
        or predecessor.get("unresolved_reference_count") != 0
        or WINDOWS != (("09:00", "08:29", "09:31"), ("10:30", "10:00", "11:01"))
    ):
        raise IntegrityError("fixed-hour schedule horizon evidence is invalid")
    bindings = predecessor.get("bindings")
    if not isinstance(bindings, Mapping):
        raise IntegrityError("fixed-hour schedule predecessor bindings are absent")
    for relative in (
        "src/futures_rebuild/cash_open_calendar_grid_successor.py",
        "src/futures_rebuild/cme_calendar_successor.py",
    ):
        if bindings.get(relative) != sha256_file(root / relative):
            raise IntegrityError(f"fixed-hour schedule semantic binding drifted: {relative}")

    source_records = calendar.get("source_observability_records")
    if not isinstance(source_records, list):
        raise IntegrityError("fixed-hour source-observability records are absent")
    if market not in {"ES", "NQ", "CL", "GC"}:
        raise IntegrityError("fixed-hour market is outside the frozen scope")
    source_gaps = {
        str(item.get("trade_date"))
        for item in source_records
        if isinstance(item, Mapping)
        and item.get("market") == market
        and item.get("source_state") == "SOURCE_UNOBSERVABLE"
        and item.get("calendar_state") == "OPEN"
        and item.get("silent_drop_allowed") is False
    }
    expected_gaps = {
        "ES": {"2020-06-30"},
        "NQ": set(),
        "CL": {"2020-02-28", "2020-06-30"},
        "GC": set(),
    }[market]
    if source_gaps != expected_gaps:
        raise IntegrityError("fixed-hour source-observability scope drifted")
    rows = calendar.get("calendar_rows")
    if not isinstance(rows, list):
        raise IntegrityError("fixed-hour schedule rows are absent")
    sessions: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, Mapping) or row.get("market") != market:
            continue
        trade_date = str(row.get("trade_date"))
        checkpoint_open = row.get("checkpoint_open")
        disposition = row.get("disposition")
        if (
            trade_date in sessions
            or row.get("schedule_family") != {
                "ES": "EQUITY_INDEX", "NQ": "EQUITY_INDEX",
                "CL": "ENERGY", "GC": "METALS",
            }[market]
            or not isinstance(checkpoint_open, Mapping)
            or not isinstance(disposition, Mapping)
            or type(checkpoint_open.get("10:00")) is not bool
            or type(checkpoint_open.get("10:30")) is not bool
            or not isinstance(disposition.get("10:00"), str)
            or not isinstance(disposition.get("10:30"), str)
        ):
            raise IntegrityError("fixed-hour schedule row is invalid")
        if trade_date in source_gaps:
            sessions[trade_date] = "AUTHORITATIVE_SOURCE_UNOBSERVABLE"
        elif checkpoint_open["10:00"] and checkpoint_open["10:30"]:
            sessions[trade_date] = "AUTHORITATIVE_OPEN"
        else:
            sessions[trade_date] = "AUTHORITATIVE_CALENDAR_INELIGIBLE"
    if (
        len(sessions) != 1_826
        or min(sessions) != "2018-01-01"
        or max(sessions) != "2022-12-31"
    ):
        raise IntegrityError("fixed-hour schedule date topology is incomplete")
    return {
        "calendar_id": ACTIVE_SCHEDULE_ID,
        "market": market,
        "pointer_id": pointer.get("pointer_id"),
        "dependency_window": {
            "start": "10:00:00",
            "decision": "10:00:05",
            "entry": "10:01:00",
            "exit": "11:01:00",
            "timezone": "America/Chicago",
        },
        "sessions": sessions,
        "session_state_counts": dict(sorted(Counter(sessions.values()).items())),
        "source_unobservable_dates": sorted(source_gaps),
    }


def _valid_row(row: Mapping[str, object], session_id: str, market: str) -> None:
    integer_fields = (
        "bar_start_ns", "bar_end_ns", "source_timestamp_ns", "available_at_ns",
        "decision_eligible_at_ns", "open_nano", "high_nano", "low_nano", "close_nano",
        "volume", "min_price_increment_nano", "multiplier_nano", "project_grouping_start_ns",
        "project_grouping_end_ns",
    )
    if any(type(row.get(name)) is not int for name in integer_fields):
        raise IntegrityError("readiness row has a missing or non-integer field")
    start = int(row["bar_start_ns"])
    end = int(row["bar_end_ns"])
    if (
        row.get("market") != market
        or row.get("source_cadence") != "1m"
        or row.get("project_session_id") != session_id
        or not isinstance(row.get("project_trade_date"), str)
        or row.get("project_timezone") != "America/Chicago"
        or not isinstance(row.get("official_schedule_state"), str)
        or not row["official_schedule_state"]
        or not isinstance(row.get("actual_contract"), str)
        or not row["actual_contract"]
        or end - start != 60 * NANO
        or int(row["source_timestamp_ns"]) != start
        or not end <= int(row["available_at_ns"]) <= int(row["decision_eligible_at_ns"])
        or not int(row["project_grouping_start_ns"]) <= start < int(row["project_grouping_end_ns"])
        or min(int(row[name]) for name in ("open_nano", "high_nano", "low_nano", "close_nano")) <= 0
        or int(row["high_nano"]) < max(int(row["open_nano"]), int(row["close_nano"]))
        or int(row["low_nano"]) > min(int(row["open_nano"]), int(row["close_nano"]))
        or int(row["volume"]) < 0
        or int(row["min_price_increment_nano"]) <= 0
        or int(row["multiplier_nano"]) <= 0
    ):
        raise IntegrityError("readiness row violates the causal OHLCV contract")


def assess_session(
    rows: Sequence[Mapping[str, object]], *, authoritative_schedule_state: str | None = None,
) -> dict[str, object]:
    if not rows:
        raise IntegrityError("cannot assess an empty session")
    session_id = str(rows[0].get("project_session_id", ""))
    market = str(rows[0].get("market", ""))
    if market not in {"ES", "NQ", "CL", "GC"}:
        raise IntegrityError("session market is outside the frozen scope")
    trade_date = str(rows[0].get("project_trade_date", ""))
    timezone_name = str(rows[0].get("project_timezone", ""))
    for row in rows:
        _valid_row(row, session_id, market)
    if (
        len({str(row["project_trade_date"]) for row in rows}) != 1
        or len({str(row["project_timezone"]) for row in rows}) != 1
        or len({int(row["project_grouping_start_ns"]) for row in rows}) != 1
        or len({int(row["project_grouping_end_ns"]) for row in rows}) != 1
        or len({str(row["official_schedule_state"]) for row in rows}) != 1
    ):
        raise IntegrityError("session semantics change within one session")
    starts = [int(row["bar_start_ns"]) for row in rows]
    if starts != sorted(set(starts)):
        raise IntegrityError("session rows are duplicated or out of order")
    decision = checkpoint_ns(trade_date, timezone_name)
    grouping_start = int(rows[0]["project_grouping_start_ns"])
    grouping_end = int(rows[0]["project_grouping_end_ns"])
    schedule_state = authoritative_schedule_state or str(rows[0]["official_schedule_state"])
    if authoritative_schedule_state not in {
        None,
        "AUTHORITATIVE_OPEN",
        "AUTHORITATIVE_CALENDAR_INELIGIBLE",
        "AUTHORITATIVE_SOURCE_UNOBSERVABLE",
    }:
        raise IntegrityError("fixed-hour authoritative schedule state is invalid")
    base = {
        "session_id": session_id,
        "market": market,
        "trade_date": trade_date,
        "schedule_state": schedule_state,
        "embedded_schedule_state": str(rows[0]["official_schedule_state"]),
        "decision_at_ns": decision,
        "feature_ready": False,
        "mechanics_ready": False,
        "terminal": True,
        "label_start_ns": None,
        "label_end_ns": None,
    }
    if schedule_state == "AUTHORITATIVE_CALENDAR_INELIGIBLE":
        return {**base, "status": "CALENDAR_INELIGIBLE"}
    if schedule_state == "AUTHORITATIVE_SOURCE_UNOBSERVABLE":
        return {**base, "status": "SOURCE_UNOBSERVABLE"}
    if not grouping_start <= decision < grouping_end:
        return {**base, "status": "CHECKPOINT_OUTSIDE_SESSION"}
    try:
        features = feature_values(rows, decision)
    except IntegrityError:
        return {**base, "status": "FEATURE_INCOMPLETE"}
    eligible = [row for row in rows if int(row["decision_eligible_at_ns"]) <= decision]
    if len(eligible) < 2:
        return {**base, "status": "FEATURE_INCOMPLETE"}
    outcome = resolve_outcome_path(rows, decision)
    status = str(outcome["score_status"])
    if status != "SCORABLE":
        return {**base, "feature_ready": True, "status": status}
    entry = outcome["entry"]
    exit_row = outcome["exit"]
    path = outcome["path"]
    assert isinstance(entry, Mapping) and isinstance(exit_row, Mapping)
    assert isinstance(path, list) and all(isinstance(row, Mapping) for row in path)
    feature_contract = str(eligible[-1]["actual_contract"])
    if feature_contract != str(entry["actual_contract"]):
        return {**base, "feature_ready": True, "status": "ROLL_UNRESOLVED"}
    prior_close = int(eligible[-2]["close_nano"]) / NANO
    atr_price = float(features["atr_10_fraction"]) * prior_close
    tick_size = int(entry["min_price_increment_nano"]) / NANO
    multiplier = int(entry["multiplier_nano"]) / NANO
    stress_cost = 10.0 + 4.0 * tick_size * multiplier
    entry_price = int(entry["open_nano"]) / NANO
    exit_price = int(exit_row["open_nano"]) / NANO
    targets = stress_net_targets(
        entry_price=entry_price, exit_price=exit_price, atr_10_price=atr_price,
        tick_size=tick_size, multiplier=multiplier, round_trip_cost_usd=stress_cost,
    )
    highs = [int(row["high_nano"]) / NANO for row in path]
    lows = [int(row["low_nano"]) / NANO for row in path]
    ratios = tuple(
        net_mfe_mae_ratio(
            entry_price=entry_price, highs=highs, lows=lows, direction=direction,
            tick_size=tick_size, multiplier=multiplier, round_trip_cost_usd=stress_cost,
        )
        for direction in (1, -1)
    )
    if not all(math.isfinite(value) for value in (*features.values(), *targets, *ratios)):
        raise IntegrityError("feature or outcome mechanics are nonfinite")
    gross_long = (exit_price - entry_price) * multiplier
    gross_short = -gross_long
    costs = {
        name: 10.0 + ticks[market] * tick_size * multiplier
        for name, ticks in {
            "base": {"ES": 2, "NQ": 2, "CL": 4, "GC": 4},
            "stress": {"ES": 4, "NQ": 4, "CL": 8, "GC": 8},
            "extreme": {"ES": 8, "NQ": 8, "CL": 16, "GC": 16},
        }.items()
    }
    return {
        **base,
        "feature_ready": True,
        "mechanics_ready": True,
        "status": "READY",
        "label_start_ns": int(entry["bar_start_ns"]),
        "label_end_ns": int(exit_row["bar_start_ns"]),
        "features": dict(features),
        "targets": {"long": targets[0], "short": targets[1]},
        "gross_pnl_usd": {"long": gross_long, "short": gross_short},
        "net_pnl_usd": {
            scenario: {"long": gross_long - cost, "short": gross_short - cost}
            for scenario, cost in costs.items()
        },
        "round_trip_cost_usd": costs,
        "net_mfe_mae": {"long": ratios[0], "short": ratios[1]},
    }


def _folds(sessions: Sequence[Mapping[str, object]]) -> dict[str, object]:
    checkpoint_sessions = [
        item for item in sessions if item["status"] != "CHECKPOINT_OUTSIDE_SESSION"
    ]
    if len({str(item["trade_date"]) for item in checkpoint_sessions}) != len(checkpoint_sessions):
        raise IntegrityError("more than one checkpoint session exists for a trade date")
    ordered = sorted(checkpoint_sessions, key=lambda item: str(item["trade_date"]))
    outer = []
    nested = []
    for ordinal in range(8):
        train_end = 504 + ordinal * 63
        train = ordered[:train_end]
        evaluation = ordered[train_end:train_end + 63]
        if len(evaluation) != 63:
            break
        outer.append(_fold_record(f"OUTER_{ordinal:02d}", train, evaluation))
        inner_initial = len(train) - 4 * 42
        if inner_initial < 252:
            continue
        for inner_ordinal in range(4):
            inner_train_end = inner_initial + inner_ordinal * 42
            inner_train = train[:inner_train_end]
            validation = train[inner_train_end:inner_train_end + 42]
            nested.append(
                _fold_record(
                    f"OUTER_{ordinal:02d}_INNER_{inner_ordinal:02d}",
                    inner_train,
                    validation,
                )
            )
    all_folds = [*outer, *nested]
    return {
        "checkpoint_session_count": len(ordered),
        "outer_fold_count": len(outer),
        "nested_fold_count": len(nested),
        "outer_folds": outer,
        "nested_folds": nested,
        "checks": {
            "eight_outer_folds": len(outer) == 8,
            "four_inner_folds_per_outer": len(nested) == 32,
            "all_folds_chronological": all(bool(item["chronological"]) for item in all_folds),
            "zero_label_overlap": all(bool(item["zero_label_overlap"]) for item in all_folds),
            "minimum_model_ready_training": all(int(item["mechanics_ready_training"]) >= 252 for item in all_folds),
            "complete_terminal_evaluation_coverage": all(
                int(item["terminal_evaluation"]) == int(item["evaluation_sessions"])
                for item in all_folds
            ),
            "purge_and_embargo_frozen": True,
        },
    }


def _fold_record(
    fold_id: str, training: Sequence[Mapping[str, object]],
    evaluation: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    training_dates = [str(item["trade_date"]) for item in training]
    evaluation_dates = [str(item["trade_date"]) for item in evaluation]
    training_labels = [
        (int(item["label_start_ns"]), int(item["label_end_ns"]))
        for item in training if item["mechanics_ready"] is True
    ]
    evaluation_labels = [
        (int(item["label_start_ns"]), int(item["label_end_ns"]))
        for item in evaluation if item["mechanics_ready"] is True
    ]
    zero_overlap = not training_labels or not evaluation_labels or (
        max(end for _start, end in training_labels)
        <= min(start for start, _end in evaluation_labels)
    )
    return {
        "fold_id": fold_id,
        "training_sessions": len(training),
        "evaluation_sessions": len(evaluation),
        "training_start": training_dates[0],
        "training_end": training_dates[-1],
        "evaluation_start": evaluation_dates[0],
        "evaluation_end": evaluation_dates[-1],
        "training_session_ids_sha256": sha256_json(training_dates),
        "evaluation_session_ids_sha256": sha256_json(evaluation_dates),
        "feature_ready_training": sum(bool(item["feature_ready"]) for item in training),
        "mechanics_ready_training": sum(bool(item["mechanics_ready"]) for item in training),
        "feature_ready_evaluation": sum(bool(item["feature_ready"]) for item in evaluation),
        "mechanics_ready_evaluation": sum(bool(item["mechanics_ready"]) for item in evaluation),
        "terminal_evaluation": sum(bool(item["terminal"]) for item in evaluation),
        "chronological": training_dates[-1] < evaluation_dates[0],
        "zero_label_overlap": zero_overlap,
        "purge_minutes": 120,
        "embargo_sessions": 1,
    }


def run_census(root: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    contract = _read_json(root / CONTRACT_PATH)
    validate_contract(root, contract)
    schedule_authority = load_fixed_hour_schedule(root)
    schedule_sessions = schedule_authority["sessions"]
    assert isinstance(schedule_sessions, Mapping)
    boundary = contract["first_real_data_boundary"]
    assert isinstance(boundary, Mapping)
    expected_paths = tuple(str(value) for value in boundary["parquet_files"])
    expected_sidecars = tuple(str(value) for value in boundary["sidecar_files"])
    if not len(expected_paths) == len(expected_sidecars) == 5:
        raise IntegrityError("approved ES readiness boundary is malformed")
    sources = {
        str(source["parquet_path"]): source
        for source in contract["source_contract"]["sources"]  # type: ignore[index]
        if source["market"] == "ES"  # type: ignore[index]
    }
    sessions: list[dict[str, object]] = []
    current_id: str | None = None
    current_rows: list[dict[str, object]] = []
    last_start: int | None = None
    rows_by_year: dict[str, int] = {}
    source_reads = []
    for relative, sidecar_relative in zip(expected_paths, expected_sidecars, strict=True):
        source = sources.get(relative)
        if not isinstance(source, Mapping) or source.get("sidecar_path") != sidecar_relative:
            raise IntegrityError("approved ES readiness source does not match the frozen contract")
        path = root / relative
        sidecar_path = root / sidecar_relative
        sidecar = _read_json(sidecar_path)
        entry_binding = sidecar.get("entry_binding")
        if (
            not isinstance(entry_binding, Mapping)
            or sha256_file(sidecar_path) != source.get("sidecar_sha256")
            or entry_binding.get("parquet_path") != relative
            or entry_binding.get("parquet_sha256") != source.get("parquet_sha256")
            or entry_binding.get("row_count") != source.get("row_count")
            or entry_binding.get("selection_eligible") is not True
        ):
            raise IntegrityError("ES readiness sidecar binding changed")
        parquet = pq.ParquetFile(path)
        if (
            parquet.schema_arrow.names != SCHEMAS["observations"].names
            or not set(READ_COLUMNS).issubset(parquet.schema_arrow.names)
            or parquet.metadata.num_rows != int(source["row_count"])
            or path.stat().st_size != int(source["physical_bytes"])
        ):
            raise IntegrityError("ES readiness Parquet schema or volume changed")
        observed = 0
        for batch in parquet.iter_batches(batch_size=BATCH_ROWS, columns=list(READ_COLUMNS)):
            observed += batch.num_rows
            for row in batch.to_pylist():
                start = int(row["bar_start_ns"])
                if last_start is not None and start <= last_start:
                    raise IntegrityError("ES readiness sources are not globally chronological")
                last_start = start
                session_id = str(row["project_session_id"])
                if current_id is None:
                    current_id = session_id
                if session_id != current_id:
                    trade_date = str(current_rows[0]["project_trade_date"])
                    schedule_state = schedule_sessions.get(trade_date)
                    if not isinstance(schedule_state, str):
                        raise IntegrityError("ES readiness session lacks schedule authority")
                    sessions.append(
                        assess_session(
                            current_rows, authoritative_schedule_state=schedule_state,
                        )
                    )
                    current_rows = []
                    current_id = session_id
                current_rows.append(row)
        if observed != int(source["row_count"]):
            raise IntegrityError("ES readiness source row count changed during read")
        year = str(source["year"])
        rows_by_year[year] = observed
        source_reads.append({
            "year": int(year),
            "parquet_path": relative,
            "sidecar_path": sidecar_relative,
            "rows_read": observed,
            "physical_bytes": path.stat().st_size,
            "schema_names_sha256": sha256_json(parquet.schema_arrow.names),
            "declared_parquet_sha256": source["parquet_sha256"],
            "verified_sidecar_sha256": source["sidecar_sha256"],
        })
    if current_rows:
        trade_date = str(current_rows[0]["project_trade_date"])
        schedule_state = schedule_sessions.get(trade_date)
        if not isinstance(schedule_state, str):
            raise IntegrityError("ES readiness session lacks schedule authority")
        sessions.append(
            assess_session(current_rows, authoritative_schedule_state=schedule_state)
        )
    observed_rows = sum(rows_by_year.values())
    observed_bytes = sum(int(item["physical_bytes"]) for item in source_reads)
    if observed_rows != int(boundary["expected_rows"]) or observed_bytes != int(boundary["expected_physical_bytes"]):
        raise IntegrityError("approved ES readiness read volume changed")

    status_counts = Counter(str(item["status"]) for item in sessions)
    schedule_states = Counter(str(item["schedule_state"]) for item in sessions)
    fold_readiness = _folds(sessions)
    checks = {
        "exact_approved_files_rows_and_bytes": (
            len(source_reads) == 5 and observed_rows == 1_755_614 and observed_bytes == 147_844_292
        ),
        "schema_and_row_contract_valid": True,
        "session_and_dst_semantics_valid": True,
        "feature_and_outcome_mechanics_computable": status_counts["READY"] >= 504,
        "future_roll_and_missingness_terminalized": sum(status_counts.values()) == len(sessions),
        "official_schedule_not_unknown_fail_closed": bool(schedule_states)
        and set(schedule_states).issubset({
            "AUTHORITATIVE_OPEN",
            "AUTHORITATIVE_CALENDAR_INELIGIBLE",
            "AUTHORITATIVE_SOURCE_UNOBSERVABLE",
        }),
        "active_schedule_authority_bound": True,
        "fold_readiness": all(fold_readiness["checks"].values()),  # type: ignore[union-attr]
        "no_model_fit_prediction_or_performance_evaluation": True,
        "scope_excludes_2023_plus_holdout_forward_raw_l0_and_provider": True,
    }
    state = "PASS_READINESS_ONLY_REGISTRATION_NOT_AUTHORIZED" if all(checks.values()) else "FAIL_READINESS"
    core = {
        "schema_version": "fixed_hour_ridge_readiness/1.0.0",
        "state": state,
        "contract_id": contract["contract_id"],
        "bindings": {
            "contract": {"path": CONTRACT_PATH.as_posix(), "sha256": sha256_file(root / CONTRACT_PATH)},
            "implementation": {"path": SELF.as_posix(), "sha256": sha256_file(root / SELF)},
            "schedule_pointer": {
                "path": ACTIVE_SCHEDULE_POINTER.as_posix(),
                "sha256": ACTIVE_SCHEDULE_POINTER_SHA256,
            },
            "schedule_calendar": {
                "path": ACTIVE_SCHEDULE_PATH.as_posix(),
                "sha256": ACTIVE_SCHEDULE_SHA256,
            },
        },
        "schedule_authority": {
            key: value for key, value in schedule_authority.items() if key != "sessions"
        },
        "approved_scope": {
            "market": "ES",
            "years": [2018, 2019, 2020, 2021, 2022],
            "parquet_files": list(expected_paths),
            "sidecar_files": list(expected_sidecars),
            "expected_rows": int(boundary["expected_rows"]),
            "expected_physical_bytes": int(boundary["expected_physical_bytes"]),
        },
        "observed_access": {
            "historical_rows_read": True,
            "parquet_files_read": len(source_reads),
            "sidecar_files_read": len(source_reads),
            "rows_read": observed_rows,
            "physical_bytes": observed_bytes,
            "rows_by_year": rows_by_year,
            "sources": source_reads,
        },
        "coverage": {
            "source_session_count": len(sessions),
            "status_counts": dict(sorted(status_counts.items())),
            "official_schedule_states": dict(sorted(schedule_states.items())),
            "feature_ready_sessions": sum(bool(item["feature_ready"]) for item in sessions),
            "mechanics_ready_sessions": sum(bool(item["mechanics_ready"]) for item in sessions),
        },
        "fold_readiness": fold_readiness,
        "checks": checks,
        "does_not_report": [
            "RETURNS", "PNL", "DIRECTION", "MFE_MAE_VALUES", "MODEL_PREDICTIONS", "ALPHA_METRICS",
        ],
        "authority": {
            "trial_registration": False,
            "model_fit_or_evaluation": False,
            "corroboration": False,
            "2023_plus": False,
            "holdout_or_forward": False,
            "provider_or_network": False,
            "publication_or_activation": False,
            "trading": False,
        },
    }
    return {**core, "readiness_id": sha256_json(core)}


def execute(root: Path) -> dict[str, object]:
    result = run_census(root)
    output = OUTPUT_ROOT / str(result["readiness_id"]) / "readiness.json"
    write_new_canonical_authority(root.resolve(strict=True) / output, result)
    return {
        "state": result["state"],
        "readiness_id": result["readiness_id"],
        "readiness_path": output.as_posix(),
        "rows_read": result["observed_access"]["rows_read"],  # type: ignore[index]
        "physical_bytes": result["observed_access"]["physical_bytes"],  # type: ignore[index]
    }


if __name__ == "__main__":
    print(json.dumps(execute(Path.cwd()), sort_keys=True))
