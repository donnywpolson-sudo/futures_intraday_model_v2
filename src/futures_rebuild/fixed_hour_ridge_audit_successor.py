"""Preparation-only corrections for any successor to the closed ridge pilot."""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from .canonical import sha256_file, sha256_json, write_new_canonical_authority
from .causal_observation_parquet import SCHEMAS
from .errors import IntegrityError
from .fixed_hour_ridge_discovery import (
    CONTRACT_PATH,
    NANO,
    feature_values,
    is_feature_eligible,
    resolve_outcome_path,
)
from .fixed_hour_ridge_governed import _fit_candidate, _score, evaluate_pilot, nested_select
from .fixed_hour_ridge_readiness import (
    BATCH_ROWS,
    READ_COLUMNS,
    assess_session,
    load_fixed_hour_schedule,
)


MINUTE = 60 * NANO
FEATURE_BARS = 20
OUTCOME_BARS = 60
INNER_FOLDS = 4
INNER_VALIDATION_SESSIONS = 42
INNER_MINIMUM_TRADES = 6  # 8/63 pilot activity floor scaled to a 42-session fold.
EMBARGO_SESSIONS = 1
PURGE_MINUTES = 120
MECHANISM_ID = "5c4c0019cf285781e207cc6556a678cc353a053d8d424dd162ce5e9d01e5bab6"
RESULT_ID = "74b00a56c672c8c45ae0d53a351bd5392e23f46efbfa28c76847bcb48d9df239"
MECHANISM_ROOT = Path("state/fixed_hour_ridge_governed") / MECHANISM_ID
FORENSIC_ROOT = Path("state/fixed_hour_ridge_forensic_audits")
APPROVED_YEARS = (2018, 2019, 2020)
APPROVED_ROWS = 1_048_132
APPROVED_BYTES = 88_028_968
REQUIRED_EXECUTION_AUDIT_FIELDS = {
    "actual_contract",
    "feature_first_bar_start_ns",
    "feature_last_bar_end_ns",
    "entry_bar_start_ns",
    "entry_open_nano",
    "exit_bar_start_ns",
    "exit_open_nano",
    "maximum_high_nano",
    "maximum_high_bar_start_ns",
    "maximum_price_source",
    "minimum_low_nano",
    "minimum_low_bar_start_ns",
    "minimum_price_source",
    "observed_path_bars",
}

_SHA256 = re.compile(r"[0-9a-f]{64}")


def validate_fixed_hour_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    expected_market: str | None = None,
    expected_trade_date: str | None = None,
    expected_source_contract_id: str | None = None,
    expected_source_release_id: str | None = None,
    expected_contract: str | None = None,
) -> None:
    """Validate the canonical identity and one-minute semantics of selected rows."""

    starts: list[int] = []
    row_ids: list[str] = []
    digest_fields = (
        "row_id", "source_contract_id", "source_release_id", "source_file_sha256",
        "source_row_sha256", "definition_source_file_sha256", "definition_row_sha256",
    )
    required_ints = (
        "bar_start_ns", "bar_end_ns", "source_timestamp_ns", "available_at_ns",
        "decision_eligible_at_ns", "open_nano", "high_nano", "low_nano",
        "close_nano", "volume", "min_price_increment_nano", "multiplier_nano",
    )
    require_provenance = expected_market is not None
    if require_provenance and any(value is None for value in (
        expected_trade_date, expected_source_contract_id,
        expected_source_release_id, expected_contract,
    )):
        raise IntegrityError("fixed-hour expected provenance is incomplete")
    for row in rows:
        if any(type(row.get(name)) is not int for name in required_ints):
            raise IntegrityError("fixed-hour row has invalid canonical types")
        if require_provenance and any(
            type(row.get(name)) is not str or _SHA256.fullmatch(str(row[name])) is None
            for name in digest_fields
        ):
            raise IntegrityError("fixed-hour row provenance is absent or malformed")
        start, end = int(row["bar_start_ns"]), int(row["bar_end_ns"])
        available, eligible = int(row["available_at_ns"]), int(row["decision_eligible_at_ns"])
        opening, high, low, closing = (
            int(row[name]) for name in ("open_nano", "high_nano", "low_nano", "close_nano")
        )
        tick = int(row["min_price_increment_nano"])
        if (
            end - start != MINUTE
            or int(row["source_timestamp_ns"]) != start
            or not end <= available <= eligible
            or high < max(opening, closing)
            or low > min(opening, closing)
            or high < low
            or int(row["volume"]) <= 0
            or tick <= 0
            or int(row["multiplier_nano"]) <= 0
            or any(price % tick for price in (opening, high, low, closing))
            or (require_provenance and row.get("source_cadence") != "1m")
            or (require_provenance and row.get("official_schedule_state") != "AUTHORITATIVE_APPLICABLE")
        ):
            raise IntegrityError("fixed-hour row violates one-minute OHLCV semantics")
        if expected_market is not None and row.get("market") != expected_market:
            raise IntegrityError("fixed-hour row market differs from opportunity")
        if expected_trade_date is not None and row.get("project_trade_date") != expected_trade_date:
            raise IntegrityError("fixed-hour row trade date differs from opportunity")
        if expected_source_contract_id is not None and row.get("source_contract_id") != expected_source_contract_id:
            raise IntegrityError("fixed-hour row source contract differs from opportunity")
        if expected_source_release_id is not None and row.get("source_release_id") != expected_source_release_id:
            raise IntegrityError("fixed-hour row source release differs from opportunity")
        if expected_contract is not None and row.get("actual_contract") != expected_contract:
            raise IntegrityError("fixed-hour row actual contract differs from opportunity")
        starts.append(start)
        if require_provenance:
            row_ids.append(str(row["row_id"]))
    if len(starts) != len(set(starts)) or (
        require_provenance and len(row_ids) != len(set(row_ids))
    ):
        raise IntegrityError("fixed-hour dependency rows are duplicated")


def strict_feature_values(
    rows: Sequence[Mapping[str, object]], decision_at_ns: int,
) -> tuple[dict[str, float], dict[str, object]]:
    """Build features only from the exact, continuous, single-contract window."""

    eligible = sorted(
        (row for row in rows if is_feature_eligible(row, decision_at_ns)),
        key=lambda row: int(row["bar_start_ns"]),
    )
    if len(eligible) < FEATURE_BARS:
        raise IntegrityError("strict feature window has fewer than 20 eligible bars")
    window = eligible[-FEATURE_BARS:]
    starts = [int(row["bar_start_ns"]) for row in window]
    ends = [int(row["bar_end_ns"]) for row in window]
    if (
        any(end - start != MINUTE for start, end in zip(starts, ends, strict=True))
        or any(right != left + MINUTE for left, right in zip(starts, starts[1:]))
        or ends[-1] != decision_at_ns - 5 * NANO
    ):
        raise IntegrityError("strict feature window is not the consecutive 20 minutes before the checkpoint")
    contracts = {row.get("actual_contract") for row in window}
    if len(contracts) != 1 or not isinstance(next(iter(contracts)), str) or not next(iter(contracts)):
        raise IntegrityError("strict feature window crosses or lacks actual-contract identity")
    return feature_values(window, decision_at_ns), {
        "actual_contract": next(iter(contracts)),
        "first_bar_start_ns": starts[0],
        "last_bar_end_ns": ends[-1],
        "bar_count": len(window),
        "consecutive_one_minute_bars": True,
    }


def strict_outcome_path(
    rows: Sequence[Mapping[str, object]],
    decision_at_ns: int,
    *,
    expected_contract: str,
    expected_market: str | None = None,
    expected_trade_date: str | None = None,
    expected_source_contract_id: str | None = None,
    expected_source_release_id: str | None = None,
) -> dict[str, object]:
    """Fail closed when L0 OHLCV cannot prove the complete 60-minute path."""

    outcome = dict(resolve_outcome_path(rows, decision_at_ns))
    if outcome.get("score_status") != "SCORABLE":
        return outcome
    entry = outcome["entry"]
    exit_row = outcome["exit"]
    path = outcome["path"]
    assert isinstance(entry, Mapping) and isinstance(exit_row, Mapping) and isinstance(path, list)
    expected_entry = (decision_at_ns // MINUTE + 1) * MINUTE
    entry_start = int(entry["bar_start_ns"])
    exit_start = int(exit_row["bar_start_ns"])
    starts = [int(row["bar_start_ns"]) for row in path]
    expected_starts = list(range(expected_entry, expected_entry + OUTCOME_BARS * MINUTE, MINUTE))
    if entry_start != expected_entry or exit_start != expected_entry + OUTCOME_BARS * MINUTE:
        return {
            "prediction_recorded": True,
            "score_status": "INDETERMINATE_ENTRY_OR_EXIT_ALIGNMENT",
            "observed_entry_start_ns": entry_start,
            "observed_exit_start_ns": exit_start,
        }
    if len(starts) != len(set(starts)):
        raise IntegrityError("fixed-hour outcome path contains duplicate bar starts")
    if starts != expected_starts:
        return {
            "prediction_recorded": True,
            "score_status": "INDETERMINATE_SPARSE_OHLCV_PATH",
            "observed_path_bars": len(starts),
            "expected_path_bars": OUTCOME_BARS,
            "missing_bar_starts_ns": [value for value in expected_starts if value not in set(starts)],
        }
    if expected_market is not None:
        validate_fixed_hour_rows(
            [*path, exit_row],
            expected_market=expected_market,
            expected_trade_date=expected_trade_date,
            expected_source_contract_id=expected_source_contract_id,
            expected_source_release_id=expected_source_release_id,
            expected_contract=expected_contract,
        )
    elif any(int(row["bar_end_ns"]) - int(row["bar_start_ns"]) != MINUTE for row in [*path, exit_row]):
        raise IntegrityError("fixed-hour outcome contains a non-one-minute bar")
    contracts = {row.get("actual_contract") for row in [entry, *path, exit_row]}
    if contracts != {expected_contract}:
        return {"prediction_recorded": True, "score_status": "ROLL_UNRESOLVED"}
    max_row = max(path, key=lambda row: int(row["high_nano"]))
    min_row = min(path, key=lambda row: int(row["low_nano"]))
    exit_open = int(exit_row["open_nano"])
    maximum = max(int(max_row["high_nano"]), exit_open)
    minimum = min(int(min_row["low_nano"]), exit_open)
    maximum_source = "EXIT_OPEN" if exit_open > int(max_row["high_nano"]) else "PATH_HIGH"
    minimum_source = "EXIT_OPEN" if exit_open < int(min_row["low_nano"]) else "PATH_LOW"
    return {
        **outcome,
        "coverage": {
            "observed_path_bars": len(path),
            "expected_path_bars": OUTCOME_BARS,
            "consecutive_one_minute_bars": True,
        },
        "extrema": {
            "maximum_high_nano": maximum,
            "maximum_high_bar_start_ns": int(
                exit_row["bar_start_ns"] if maximum_source == "EXIT_OPEN" else max_row["bar_start_ns"]
            ),
            "maximum_price_source": maximum_source,
            "minimum_low_nano": minimum,
            "minimum_low_bar_start_ns": int(
                exit_row["bar_start_ns"] if minimum_source == "EXIT_OPEN" else min_row["bar_start_ns"]
            ),
            "minimum_price_source": minimum_source,
        },
    }


def purged_embargoed_inner_folds(
    ready: Mapping[str, Mapping[str, object]], training_ids: Sequence[str],
) -> list[dict[str, object]]:
    """Construct four folds with an observed embargo session and timestamp purge."""

    required = INNER_FOLDS * (INNER_VALIDATION_SESSIONS + EMBARGO_SESSIONS)
    initial = len(training_ids) - required
    if initial < 252:
        raise IntegrityError("strict inner folds cannot supply minimum training history")
    folds = []
    for ordinal in range(INNER_FOLDS):
        train_end = initial + ordinal * (INNER_VALIDATION_SESSIONS + EMBARGO_SESSIONS)
        embargo = tuple(training_ids[train_end:train_end + EMBARGO_SESSIONS])
        validation = tuple(
            training_ids[
                train_end + EMBARGO_SESSIONS:
                train_end + EMBARGO_SESSIONS + INNER_VALIDATION_SESSIONS
            ]
        )
        if len(embargo) != EMBARGO_SESSIONS or len(validation) != INNER_VALIDATION_SESSIONS:
            raise IntegrityError("strict inner fold is incomplete")
        validation_start = min(int(ready[item]["label_start_ns"]) for item in validation)
        purge_cutoff = validation_start - PURGE_MINUTES * MINUTE
        proposed = tuple(training_ids[:train_end])
        training = tuple(
            item for item in proposed if int(ready[item]["label_end_ns"]) <= purge_cutoff
        )
        purged = tuple(item for item in proposed if item not in set(training))
        if len(training) < 252 or set(training) & (set(embargo) | set(validation)):
            raise IntegrityError("strict inner fold isolation failed")
        folds.append({
            "fold_id": f"INNER_{ordinal:02d}",
            "training_session_ids": list(training),
            "purged_session_ids": list(purged),
            "embargo_session_ids": list(embargo),
            "validation_session_ids": list(validation),
            "purge_minutes": PURGE_MINUTES,
            "embargo_sessions": EMBARGO_SESSIONS,
        })
    return folds


def purged_embargoed_outer_pilot(
    ready: Mapping[str, Mapping[str, object]], session_ids: Sequence[str],
) -> dict[str, object]:
    """Keep 504 training and 63 evaluation sessions separated by one session."""

    if len(session_ids) < 568:
        raise IntegrityError("strict outer pilot needs 568 executable sessions")
    proposed = tuple(session_ids[:504])
    embargo = tuple(session_ids[504:505])
    evaluation = tuple(session_ids[505:568])
    evaluation_start = min(int(ready[item]["label_start_ns"]) for item in evaluation)
    purge_cutoff = evaluation_start - PURGE_MINUTES * MINUTE
    training = tuple(
        item for item in proposed if int(ready[item]["label_end_ns"]) <= purge_cutoff
    )
    purged = tuple(item for item in proposed if item not in set(training))
    if len(training) != 504 or set(training) & (set(embargo) | set(evaluation)):
        raise IntegrityError("strict outer pilot isolation failed")
    return {
        "training_session_ids": list(training),
        "purged_session_ids": list(purged),
        "embargo_session_ids": list(embargo),
        "evaluation_session_ids": list(evaluation),
        "purge_minutes": PURGE_MINUTES,
        "embargo_sessions": EMBARGO_SESSIONS,
    }


def select_activity_eligible_candidate(
    candidate_order: Sequence[str],
    scores: Mapping[str, Sequence[float]],
    trade_counts: Mapping[str, Sequence[int]],
) -> str:
    """Do not let inactive candidates win merely because zero beats a loss."""

    eligible = []
    for ordinal, candidate_id in enumerate(candidate_order):
        candidate_scores = tuple(scores.get(candidate_id, ()))
        counts = tuple(trade_counts.get(candidate_id, ()))
        score = median(candidate_scores) if candidate_scores else float("-inf")
        if (
            len(candidate_scores) == len(counts) == INNER_FOLDS
            and all(math.isfinite(value) for value in candidate_scores)
            and all(count >= INNER_MINIMUM_TRADES for count in counts)
            and score > 0.0
        ):
            eligible.append((score, -ordinal, candidate_id))
    if not eligible:
        raise IntegrityError("no candidate meets the preregistered inner-fold activity floor")
    return max(eligible)[2]


def execution_audit_coordinates(
    feature_audit: Mapping[str, object], outcome: Mapping[str, object],
) -> dict[str, object]:
    """Bind the exact feature, fill-proxy, path, and extrema coordinates."""

    if outcome.get("score_status") != "SCORABLE":
        raise IntegrityError("cannot audit an unscorable outcome")
    entry = outcome.get("entry")
    exit_row = outcome.get("exit")
    coverage = outcome.get("coverage")
    extrema = outcome.get("extrema")
    if not all(isinstance(value, Mapping) for value in (entry, exit_row, coverage, extrema)):
        raise IntegrityError("strict outcome lacks audit coordinates")
    assert isinstance(entry, Mapping) and isinstance(exit_row, Mapping)
    assert isinstance(coverage, Mapping) and isinstance(extrema, Mapping)
    audit = {
        "actual_contract": feature_audit["actual_contract"],
        "feature_first_bar_start_ns": feature_audit["first_bar_start_ns"],
        "feature_last_bar_end_ns": feature_audit["last_bar_end_ns"],
        "entry_bar_start_ns": entry["bar_start_ns"],
        "entry_open_nano": entry["open_nano"],
        "exit_bar_start_ns": exit_row["bar_start_ns"],
        "exit_open_nano": exit_row["open_nano"],
        "maximum_high_nano": extrema["maximum_high_nano"],
        "maximum_high_bar_start_ns": extrema["maximum_high_bar_start_ns"],
        "maximum_price_source": extrema["maximum_price_source"],
        "minimum_low_nano": extrema["minimum_low_nano"],
        "minimum_low_bar_start_ns": extrema["minimum_low_bar_start_ns"],
        "minimum_price_source": extrema["minimum_price_source"],
        "observed_path_bars": coverage["observed_path_bars"],
    }
    if set(audit) != REQUIRED_EXECUTION_AUDIT_FIELDS:
        raise IntegrityError("execution audit coordinates are incomplete")
    return audit


def session_ledger_record(
    row: Mapping[str, object],
    *,
    intent: int,
    predicted_long_r: float,
    predicted_short_r: float,
) -> dict[str, object]:
    """Return the minimum record needed to audit every trade and abstention."""

    if intent not in {-1, 0, 1} or not all(
        math.isfinite(value) for value in (predicted_long_r, predicted_short_r)
    ):
        raise IntegrityError("invalid auditable prediction")
    audit = row.get("execution_audit")
    if not isinstance(audit, Mapping) or not REQUIRED_EXECUTION_AUDIT_FIELDS <= set(audit):
        raise IntegrityError("session lacks strict execution audit coordinates")
    direction = "long" if intent == 1 else "short" if intent == -1 else None
    if direction is None:
        reason = (
            "PREDICTIONS_TIED" if predicted_long_r == predicted_short_r
            else "BEST_PREDICTED_STRESS_NET_R_NOT_POSITIVE"
        )
        gross = 0.0
        net = {scenario: 0.0 for scenario in ("base", "stress", "extreme")}
        ratio = None
    else:
        reason = "TRADE"
        gross = float(row["gross_pnl_usd"][direction])  # type: ignore[index]
        net = {
            scenario: float(values[direction])
            for scenario, values in row["net_pnl_usd"].items()  # type: ignore[union-attr]
        }
        ratio = float(row["net_mfe_mae"][direction])  # type: ignore[index]
    return {
        "session_id": row["session_id"],
        "trade_date": row["trade_date"],
        "predicted_long_stress_net_r": predicted_long_r,
        "predicted_short_stress_net_r": predicted_short_r,
        "intent": intent,
        "direction": direction,
        "disposition": reason,
        "execution": dict(audit),
        "gross_pnl_usd": gross,
        "net_pnl_usd": net,
        "net_mfe_mae": ratio,
        "round_trip_cost_usd": dict(row["round_trip_cost_usd"]),  # type: ignore[arg-type]
    }


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"forensic input is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"forensic input is not an object: {path}")
    return value


def _strictly_audit_session(
    rows: Sequence[Mapping[str, object]], schedule_state: str,
) -> dict[str, object]:
    assessed = dict(assess_session(rows, authoritative_schedule_state=schedule_state))
    if assessed.get("status") != "READY":
        assessed["strict_audit_status"] = f"ORIGINAL_{assessed['status']}"
        return assessed
    decision = int(assessed["decision_at_ns"])
    try:
        _features, feature_audit = strict_feature_values(rows, decision)
    except IntegrityError as exc:
        assessed["strict_audit_status"] = "FEATURE_WINDOW_INDETERMINATE"
        assessed["strict_audit_reason"] = str(exc)
        return assessed
    outcome = strict_outcome_path(
        rows, decision, expected_contract=str(feature_audit["actual_contract"]),
    )
    if outcome.get("score_status") != "SCORABLE":
        assessed["strict_audit_status"] = str(outcome["score_status"])
        assessed["strict_outcome"] = outcome
        return assessed
    audit = execution_audit_coordinates(feature_audit, outcome)
    chicago = ZoneInfo("America/Chicago")
    for prefix in ("entry", "exit"):
        timestamp = int(audit[f"{prefix}_bar_start_ns"])
        audit[f"{prefix}_at_utc"] = datetime.fromtimestamp(
            timestamp / NANO, timezone.utc,
        ).isoformat()
        audit[f"{prefix}_at_chicago"] = datetime.fromtimestamp(
            timestamp / NANO, timezone.utc,
        ).astimezone(chicago).isoformat()
        audit[f"{prefix}_price"] = int(audit[f"{prefix}_open_nano"]) / NANO
    assessed["execution_audit"] = audit
    assessed["strict_audit_status"] = "PASS"
    return assessed


def _scan_approved_history(
    root: Path, protocol: Mapping[str, object],
) -> tuple[dict[str, dict[str, object]], dict[str, str], dict[str, object]]:
    schedule = load_fixed_hour_schedule(root, "ES")
    schedule_sessions = schedule["sessions"]
    assert isinstance(schedule_sessions, Mapping)
    source_contract = protocol.get("source_contract")
    if not isinstance(source_contract, Mapping):
        raise IntegrityError("forensic protocol lacks source contract")
    sources = sorted(
        (
            item for item in source_contract["sources"]  # type: ignore[index]
            if item["market"] == "ES" and int(item["year"]) in APPROVED_YEARS
        ),
        key=lambda item: int(item["year"]),
    )
    if len(sources) != 3 or tuple(int(item["year"]) for item in sources) != APPROVED_YEARS:
        raise IntegrityError("forensic replay did not resolve exact ES 2018-2020 sources")
    if (
        sum(int(item["row_count"]) for item in sources) != APPROVED_ROWS
        or sum(int(item["physical_bytes"]) for item in sources) != APPROVED_BYTES
    ):
        raise IntegrityError("forensic source volume changed")

    sessions: list[dict[str, object]] = []
    bindings: dict[str, str] = {}
    current_id: str | None = None
    current_rows: list[dict[str, object]] = []
    last_start: int | None = None
    rows_read = bytes_read = 0

    def finish() -> None:
        nonlocal current_rows
        if not current_rows:
            return
        trade_date = str(current_rows[0]["project_trade_date"])
        state = schedule_sessions.get(trade_date)
        if not isinstance(state, str):
            raise IntegrityError("forensic session lacks calendar authority")
        sessions.append(_strictly_audit_session(current_rows, state))
        current_rows = []

    for source in sources:
        relative = str(source["parquet_path"])
        sidecar_relative = str(source["sidecar_path"])
        path = root / relative
        sidecar_path = root / sidecar_relative
        sidecar = _read_json(sidecar_path)
        entry = sidecar.get("entry_binding")
        if (
            not isinstance(entry, Mapping)
            or sha256_file(path) != source["parquet_sha256"]
            or sha256_file(sidecar_path) != source["sidecar_sha256"]
            or entry.get("parquet_path") != relative
            or entry.get("parquet_sha256") != source["parquet_sha256"]
            or entry.get("row_count") != source["row_count"]
            or entry.get("selection_eligible") is not True
        ):
            raise IntegrityError("forensic source or sidecar binding changed")
        parquet = pq.ParquetFile(path)
        if (
            parquet.schema_arrow.names != SCHEMAS["observations"].names
            or parquet.metadata.num_rows != source["row_count"]
            or path.stat().st_size != source["physical_bytes"]
        ):
            raise IntegrityError("forensic Parquet schema or volume changed")
        observed = 0
        for batch in parquet.iter_batches(batch_size=BATCH_ROWS, columns=list(READ_COLUMNS)):
            observed += batch.num_rows
            for row in batch.to_pylist():
                start = int(row["bar_start_ns"])
                if last_start is not None and start <= last_start:
                    raise IntegrityError("forensic sources are not globally chronological")
                last_start = start
                session_id = str(row["project_session_id"])
                if current_id is None:
                    current_id = session_id
                if session_id != current_id:
                    finish()
                    current_id = session_id
                current_rows.append(row)
        if observed != source["row_count"]:
            raise IntegrityError("forensic source row count changed during read")
        rows_read += observed
        bytes_read += path.stat().st_size
        bindings[relative] = str(source["parquet_sha256"])
        bindings[sidecar_relative] = str(source["sidecar_sha256"])
    finish()
    if (rows_read, bytes_read) != (APPROVED_ROWS, APPROVED_BYTES):
        raise IntegrityError("forensic replay exceeded or missed approved volume")
    ready = {
        str(item["trade_date"]): item for item in sessions if item.get("status") == "READY"
    }
    if len(ready) != sum(item.get("status") == "READY" for item in sessions):
        raise IntegrityError("forensic replay has duplicate ready trade dates")
    return dict(sorted(ready.items())), bindings, {
        "market": "ES",
        "years": list(APPROVED_YEARS),
        "parquet_files": 3,
        "rows_read": rows_read,
        "physical_bytes": bytes_read,
        "sessions_assessed": len(sessions),
        "original_status_counts": dict(sorted(Counter(str(item["status"]) for item in sessions).items())),
    }


def _fallback_ledger_record(
    row: Mapping[str, object], intent: int, predicted_long: float, predicted_short: float,
) -> dict[str, object]:
    direction = "long" if intent == 1 else "short" if intent == -1 else None
    return {
        "session_id": row["session_id"],
        "trade_date": row["trade_date"],
        "predicted_long_stress_net_r": predicted_long,
        "predicted_short_stress_net_r": predicted_short,
        "intent": intent,
        "direction": direction,
        "disposition": "TRADE" if direction else "ABSTAIN",
        "strict_audit_status": row["strict_audit_status"],
        "execution": None,
        "gross_pnl_usd": 0.0 if direction is None else float(row["gross_pnl_usd"][direction]),  # type: ignore[index]
        "net_pnl_usd": {
            scenario: 0.0 if direction is None else float(values[direction])
            for scenario, values in row["net_pnl_usd"].items()  # type: ignore[union-attr]
        },
        "net_mfe_mae": None if direction is None else float(row["net_mfe_mae"][direction]),  # type: ignore[index]
        "round_trip_cost_usd": dict(row["round_trip_cost_usd"]),  # type: ignore[arg-type]
    }


def run_forensic_replay(root: Path) -> dict[str, object]:
    """Reproduce and audit the closed pilot without changing its official result."""

    root = root.resolve(strict=True)
    mechanism_path = MECHANISM_ROOT / "mechanism.json"
    result_path = MECHANISM_ROOT / "es_pilot_result.json"
    sessions_path = MECHANISM_ROOT / "pilot_sessions.json"
    readiness_path = MECHANISM_ROOT / "pilot_readiness.json"
    mechanism = _read_json(root / mechanism_path)
    official = _read_json(root / result_path)
    manifest = _read_json(root / sessions_path)
    readiness = _read_json(root / readiness_path)
    protocol = _read_json(root / CONTRACT_PATH)
    if mechanism.get("mechanism_id") != MECHANISM_ID or official.get("result_id") != RESULT_ID:
        raise IntegrityError("forensic replay resolved the wrong sealed mechanism or result")
    if sha256_json({key: value for key, value in official.items() if key != "result_id"}) != RESULT_ID:
        raise IntegrityError("sealed result identity does not reproduce")
    code_bindings = mechanism.get("code_bindings")
    fixed_binding = mechanism.get("fixed_hour_protocol_binding")
    if (
        not isinstance(code_bindings, Mapping)
        or not isinstance(fixed_binding, Mapping)
        or any(sha256_file(root / str(path)) != digest for path, digest in code_bindings.items())
        or sha256_file(root / CONTRACT_PATH) != fixed_binding.get("sha256")
        or readiness.get("source_bindings", {}).get(sessions_path.as_posix()) != sha256_file(root / sessions_path)  # type: ignore[union-attr]
    ):
        raise IntegrityError("sealed forensic implementation or manifest binding changed")

    ready, source_bindings, scope = _scan_approved_history(root, protocol)
    training_ids = tuple(str(value) for value in manifest["training_session_ids"])  # type: ignore[index]
    evaluation_ids = tuple(str(value) for value in manifest["evaluation_session_ids"])  # type: ignore[index]
    if len(training_ids) != 504 or len(evaluation_ids) != 63 or not set((*training_ids, *evaluation_ids)) <= set(ready):
        raise IntegrityError("sealed pilot sessions are not reproducible from approved history")
    reproduced = evaluate_pilot(protocol, ready, training_ids, evaluation_ids)
    official_result = official.get("result")
    if reproduced != official_result:
        raise IntegrityError("original pilot result did not reproduce exactly")
    selected, _scores = nested_select(protocol, ready, training_ids)
    model = _fit_candidate(selected, [ready[item] for item in training_ids])

    records = []
    for session_id in evaluation_ids:
        row = ready[session_id]
        intent, predicted_long, predicted_short = _score(model, selected, row)
        if row.get("strict_audit_status") == "PASS":
            record = session_ledger_record(
                row,
                intent=intent,
                predicted_long_r=predicted_long,
                predicted_short_r=predicted_short,
            )
            record["strict_audit_status"] = "PASS"
        else:
            record = _fallback_ledger_record(row, intent, predicted_long, predicted_short)
        records.append(record)

    candidate = official_result["strategies"]["candidate"]  # type: ignore[index]
    reconciliation = {}
    for scenario in ("base", "stress", "extreme"):
        pnl = sum(float(record["net_pnl_usd"][scenario]) for record in records)  # type: ignore[index]
        trades = sum(int(record["intent"]) != 0 for record in records)
        expected = candidate[scenario]  # type: ignore[index]
        reconciliation[scenario] = {
            "ledger_net_pnl_usd": pnl,
            "sealed_net_pnl_usd": expected["net_pnl_usd"],
            "ledger_trade_count": trades,
            "sealed_trade_count": expected["trade_count"],
            "exact_match": pnl == expected["net_pnl_usd"] and trades == expected["trade_count"],
        }
    if not all(item["exact_match"] for item in reconciliation.values()):
        raise IntegrityError("forensic ledger does not reconcile to sealed candidate metrics")
    cost_consistent = all(
        math.isclose(
            float(record["gross_pnl_usd"]) - float(record["round_trip_cost_usd"][scenario]),  # type: ignore[index]
            float(record["net_pnl_usd"][scenario]),  # type: ignore[index]
        )
        for record in records if record["intent"] != 0
        for scenario in ("base", "stress", "extreme")
    )
    stress_trades = [
        float(record["net_pnl_usd"]["stress"])  # type: ignore[index]
        for record in records if record["intent"] != 0
    ]
    stress_total = sum(stress_trades)
    largest_share = max(stress_trades) / stress_total if stress_trades and stress_total > 0 else None
    ordered_ready = tuple(ready)
    positions = {session_id: ordinal for ordinal, session_id in enumerate(ordered_ready)}
    outer_skipped = positions[evaluation_ids[0]] - positions[training_ids[-1]] - 1
    purge_gap_minutes = (
        int(ready[evaluation_ids[0]]["label_start_ns"])
        - int(ready[training_ids[-1]]["label_end_ns"])
    ) / MINUTE
    strict_training = Counter(str(ready[item]["strict_audit_status"]) for item in training_ids)
    strict_evaluation = Counter(str(ready[item]["strict_audit_status"]) for item in evaluation_ids)

    ledger_core = {
        "schema_version": "fixed_hour_ridge_forensic_session_ledger/1.0.0",
        "state": "SEALED_READ_ONLY_REPLAY_NO_RESEARCH_AUTHORITY",
        "mechanism_id": MECHANISM_ID,
        "result_id": RESULT_ID,
        "records": records,
    }
    ledger = {**ledger_core, "ledger_id": sha256_json(ledger_core)}
    report_core = {
        "schema_version": "fixed_hour_ridge_forensic_audit/1.0.0",
        "state": "COMPLETED_READ_ONLY_AUDIT_OFFICIAL_RESULT_UNCHANGED",
        "mechanism_id": MECHANISM_ID,
        "result_id": RESULT_ID,
        "approved_scope": scope,
        "source_bindings": source_bindings,
        "original_result_reproduced_exactly": True,
        "selected_candidate_id": selected["candidate_id"],
        "ledger_id": ledger["ledger_id"],
        "ledger_reconciliation": reconciliation,
        "cost_arithmetic_consistent": cost_consistent,
        "strict_training_status_counts": dict(sorted(strict_training.items())),
        "strict_evaluation_status_counts": dict(sorted(strict_evaluation.items())),
        "strict_trade_records_passed": sum(
            record["intent"] != 0 and record["strict_audit_status"] == "PASS" for record in records
        ),
        "trade_count": len(stress_trades),
        "stress_trade_pnls_usd": stress_trades,
        "largest_session_contribution_share": largest_share,
        "concentration_gate_lte_0_50": largest_share is not None and largest_share <= 0.50,
        "original_outer_purge_gap_minutes": purge_gap_minutes,
        "original_outer_skipped_embargo_sessions": outer_skipped,
        "original_purge_120_minutes_satisfied": purge_gap_minutes >= PURGE_MINUTES,
        "original_embargo_one_session_applied": outer_skipped >= EMBARGO_SESSIONS,
        "execution_reality": "L0_OHLCV_TRADE_PRINT_PROXY_NOT_BID_ASK_OR_LIVE_FILL_EVIDENCE",
        "official_pilot_decision": official_result["pilot_decision"],  # type: ignore[index]
        "official_decision_changed": False,
        "authority": {
            "new_trial": False,
            "model_tuning_or_selection_change": False,
            "publication_or_activation": False,
            "holdout_or_forward": False,
            "provider_or_network": False,
            "trading": False,
        },
    }
    report = {**report_core, "audit_id": sha256_json(report_core)}
    output = FORENSIC_ROOT / str(report["audit_id"])
    ledger_path = output / "session_ledger.json"
    report_path = output / "audit_report.json"
    write_new_canonical_authority(root / ledger_path, ledger)
    write_new_canonical_authority(root / report_path, report)
    return {
        "status": report["state"],
        "audit_id": report["audit_id"],
        "audit_report_path": report_path.as_posix(),
        "session_ledger_path": ledger_path.as_posix(),
        "trade_count": len(stress_trades),
        "strict_trade_records_passed": report["strict_trade_records_passed"],
        "largest_session_contribution_share": largest_share,
        "official_pilot_decision": official_result["pilot_decision"],  # type: ignore[index]
    }


if __name__ == "__main__":
    print(json.dumps(run_forensic_replay(Path.cwd()), sort_keys=True, separators=(",", ":")))
