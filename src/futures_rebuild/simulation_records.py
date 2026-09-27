"""Data-only contract for offline simulation results; no strategy or execution imports."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

SCHEMA = "offline-simulation/1"
HISTORICAL_SCHEMA = "offline-simulation/2"
DIAGNOSTIC_SCHEMA = "offline-simulation/3"
LIBRARY_SCHEMA = "offline-session-library/1"
OOS_CONTEXT = "Historical OOS development data — previously examined; not untouched test data."
HISTORICAL_LABEL = "HISTORICAL MODEL REPLAY — SIMULATED"
NO_POLICY = "No trading policy — trades and P&L unavailable."
LABEL = "SYNTHETIC SIMULATION — TEST STRATEGY"
MAX_BARS = 1440
MAX_BYTES = 8_000_000
ACCOUNT_FIELDS = {"position", "realized_pnl", "unrealized_pnl", "commissions", "slippage_cost", "total_costs", "equity", "drawdown"}


def decimal(value: object) -> Decimal:
    if not isinstance(value, (str, int, Decimal)) or isinstance(value, bool):
        raise ValueError("Expected an exact decimal string or integer")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("Invalid decimal") from exc
    if not result.is_finite() or abs(result) > Decimal("1e12"):
        raise ValueError("Nonfinite or oversized decimal")
    return result


def utc(value: str) -> datetime:
    result = datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("Timestamps must be UTC")
    return result.astimezone(timezone.utc)


def exact(value: object, fields: set[str]) -> None:
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("Invalid result fields")


def position(value: object) -> None:
    if type(value) is not int or value not in (-1, 0, 1):
        raise ValueError("Position must be -1, 0, or 1")


def encoded(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def validate_records(records: object, contract: str, tick_size: object) -> None:
    if not isinstance(records, list) or not 1 <= len(records) <= MAX_BARS:
        raise ValueError("A run requires 1–1440 bars")
    tick = decimal(tick_size)
    if tick <= 0:
        raise ValueError("Invalid tick size")
    previous = None
    for index, row in enumerate(records):
        exact(row, {"bar", "decision", "fills", "account"})
        bar = row["bar"]
        exact(bar, {"contract", "opened_at", "closed_at", "open", "high", "low", "close", "volume"})
        start, end = utc(bar["opened_at"]), utc(bar["closed_at"])
        if bar["contract"] != contract or end - start != timedelta(minutes=1):
            raise ValueError("Expected one contract and one-minute bars")
        if start.second or start.microsecond or (previous is not None and start != previous):
            raise ValueError("Duplicate, out-of-order, or missing bar")
        previous = end
        prices = [decimal(bar[k]) for k in ("open", "high", "low", "close")]
        o, h, l, c = prices
        if l > min(o, c) or h < max(o, c) or l > h or any(p % tick for p in prices):
            raise ValueError("Invalid OHLC or off-tick price")
        if type(bar["volume"]) is not int or bar["volume"] < 0:
            raise ValueError("Invalid volume")
        if row["decision"] is not None:
            position(row["decision"])
        exact(row["account"], ACCOUNT_FIELDS)
        position(row["account"]["position"])
        for key in ACCOUNT_FIELDS - {"position"}:
            number = decimal(row["account"][key])
            if key in {"commissions", "slippage_cost", "total_costs", "drawdown"} and number < 0:
                raise ValueError("Negative cost or drawdown")
        if not isinstance(row["fills"], list) or len(row["fills"]) > 1:
            raise ValueError("Invalid fills")
        for fill in row["fills"]:
            exact(fill, {"time", "side", "quantity", "price", "commission", "decision_index"})
            if index == 0 or type(fill["decision_index"]) is not int or fill["decision_index"] != index - 1:
                raise ValueError("Fill must reference the preceding bar's decision")
            if utc(fill["time"]) != start or fill["side"] not in {"BUY", "SELL"}:
                raise ValueError("Invalid fill timing or side")
            if type(fill["quantity"]) is not int or fill["quantity"] not in (1, 2):
                raise ValueError("Invalid fill quantity")
            if decimal(fill["price"]) % tick or decimal(fill["commission"]) < 0:
                raise ValueError("Invalid fill price or commission")


def validate_metadata(meta: object) -> None:
    if isinstance(meta, dict) and meta.get("kind") == "historical_diagnostics":
        validate_diagnostic_metadata(meta)
        return
    if isinstance(meta, dict) and meta.get("kind") == "historical_predictions":
        validate_historical_metadata(meta)
        return
    exact(meta, {"label", "synthetic", "market", "contract", "strategy", "input_sha256", "settings"})
    if meta["synthetic"] is not True or meta["label"] != LABEL:
        raise ValueError("Only explicitly synthetic results are supported in v1")
    for key in ("market", "contract", "strategy"):
        if not isinstance(meta[key], str) or not 1 <= len(meta[key]) <= 100:
            raise ValueError("Invalid run identity")
    digest = meta["input_sha256"]
    if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError("Invalid input identity")
    settings = meta["settings"]
    exact(settings, {"initial_balance", "tick_size", "point_value", "commission_per_side", "slippage_ticks"})
    for key in ("initial_balance", "tick_size", "point_value"):
        if decimal(settings[key]) <= 0:
            raise ValueError("Invalid economics")
    if decimal(settings["commission_per_side"]) < 0 or type(settings["slippage_ticks"]) is not int or not 0 <= settings["slippage_ticks"] <= 100:
        raise ValueError("Invalid costs")


def validate_bundle(bundle: object) -> None:
    exact(bundle, {"schema", "status", "metadata", "records", "summary"})
    if bundle["schema"] == DIAGNOSTIC_SCHEMA:
        validate_diagnostic_bundle(bundle)
        return
    if bundle["schema"] == HISTORICAL_SCHEMA:
        validate_historical_bundle(bundle)
        return
    if bundle["schema"] != SCHEMA or bundle["status"] != "COMPLETE":
        raise ValueError("Unsupported or incomplete simulation")
    meta = bundle["metadata"]
    validate_metadata(meta)
    settings = meta["settings"]
    validate_records(bundle["records"], meta["contract"], settings["tick_size"])
    bars = [r["bar"] for r in bundle["records"]]
    if hashlib.sha256(encoded(bars)).hexdigest() != meta["input_sha256"]:
        raise ValueError("Input identity mismatch")
    exact(bundle["summary"], {"account", "open_position", "pending_decision_cancelled"})
    summary = bundle["summary"]
    last = bundle["records"][-1]
    if summary["account"] != last["account"] or type(summary["open_position"]) is not bool or summary["open_position"] != (last["account"]["position"] != 0):
        raise ValueError("Invalid final account")
    if type(summary["pending_decision_cancelled"]) is not bool or summary["pending_decision_cancelled"] != (last["decision"] is not None and last["decision"] != last["account"]["position"]):
        raise ValueError("Invalid final pending decision")


def load_bundle(directory: Path) -> dict:
    path = directory / "result.json"
    with path.open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("Oversized simulation result")
    bundle = json.loads(raw)
    try:
        validate_bundle(bundle)
    except (TypeError, KeyError, OverflowError) as exc:
        raise ValueError("Malformed saved simulation results") from exc
    return bundle


def digest(value: object) -> None:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("Invalid content identity")


def validate_historical_metadata(meta: object) -> None:
    exact(meta, {"kind", "label", "synthetic", "market", "contract", "session", "strategy",
                 "model_id", "training_cutoff", "feature_names", "forecast_units", "horizon_minutes",
                 "target", "disposition", "input_sha256", "spec_id", "source_release", "tick_size",
                 "availability_assumption", "trading_policy"})
    if (meta["kind"] != "historical_predictions" or meta["synthetic"] is not False
            or meta["label"] != HISTORICAL_LABEL or meta["trading_policy"] is not None
            or meta["forecast_units"] != "ticks" or type(meta["horizon_minutes"]) is not int
            or meta["horizon_minutes"] != 60):
        raise ValueError("Unsupported historical prediction mode")
    for key in ("market", "contract", "session", "strategy", "target", "disposition", "source_release", "availability_assumption"):
        if not isinstance(meta[key], str) or not 1 <= len(meta[key]) <= 300:
            raise ValueError("Invalid historical identity")
    datetime.strptime(meta["session"], "%Y-%m-%d")
    utc(meta["training_cutoff"])
    for key in ("model_id", "input_sha256", "spec_id"):
        digest(meta[key])
    names = meta["feature_names"]
    if (not isinstance(names, list) or not 1 <= len(names) <= 32
            or any(not isinstance(n, str) or not 1 <= len(n) <= 100 for n in names)
            or len(set(names)) != len(names) or decimal(meta["tick_size"]) <= 0):
        raise ValueError("Invalid feature schema")


def validate_historical_records(records: object, meta: dict) -> None:
    if not isinstance(records, list) or not 1 <= len(records) <= MAX_BARS:
        raise ValueError("Expected bounded historical records")
    previous = None
    for row in records:
        exact(row, {"bar", "available_at", "inspection"})
        bar = row["bar"]
        exact(bar, {"contract", "opened_at", "closed_at", "open", "high", "low", "close", "volume"})
        start, end, clock = utc(bar["opened_at"]), utc(bar["closed_at"]), utc(row["available_at"])
        if (bar["contract"] != meta["contract"] or end - start != timedelta(minutes=1)
                or start.second or start.microsecond or (previous is not None and start != previous)
                or clock != end + timedelta(seconds=5)):
            raise ValueError("Historical bar timing or identity differs")
        previous = end
        o, h, l, c = [decimal(bar[k]) for k in ("open", "high", "low", "close")]
        if (l > min(o, c) or h < max(o, c) or l > h
                or any(p % decimal(meta["tick_size"]) for p in (o, h, l, c))
                or type(bar["volume"]) is not int or bar["volume"] < 0):
            raise ValueError("Invalid historical OHLCV")
        info = row["inspection"]
        exact(info, {"status", "reasons", "decision_at", "feature_window_start", "features", "prediction"})
        if (info["status"] not in {"CONTEXT", "ELIGIBLE", "REFUSED"}
                or not isinstance(info["reasons"], list) or not info["reasons"]
                or any(not isinstance(x, str) or not 1 <= len(x) <= 200 for x in info["reasons"])):
            raise ValueError("Invalid recorded eligibility")
        if info["status"] == "CONTEXT":
            if any(info[k] is not None for k in ("decision_at", "feature_window_start", "features", "prediction")):
                raise ValueError("Context cannot contain forecasts")
            continue
        at = utc(info["decision_at"])
        if (at != clock or utc(meta["training_cutoff"]) >= at
                or utc(info["feature_window_start"]) != end - timedelta(minutes=61)):
            raise ValueError("Noncausal historical decision")
        if info["status"] == "REFUSED":
            if info["features"] is not None or info["prediction"] is not None:
                raise ValueError("Refused inputs cannot produce predictions")
        else:
            if not isinstance(info["features"], list) or len(info["features"]) != len(meta["feature_names"]):
                raise ValueError("Feature order/width differs")
            for value in [*info["features"], info["prediction"]]:
                decimal(value)


def validate_historical_bundle(bundle: dict) -> None:
    if bundle["status"] != "COMPLETE":
        raise ValueError("Incomplete historical run")
    meta, rows = bundle["metadata"], bundle["records"]
    validate_historical_metadata(meta)
    validate_historical_records(rows, meta)
    if hashlib.sha256(encoded([r["bar"] for r in rows])).hexdigest() != meta["input_sha256"]:
        raise ValueError("Historical input identity mismatch")
    expected = {"predictions": sum(r["inspection"]["status"] == "ELIGIBLE" for r in rows),
                "refused": sum(r["inspection"]["status"] == "REFUSED" for r in rows), "message": NO_POLICY}
    if bundle["summary"] != expected:
        raise ValueError("Invalid historical summary")


def validate_diagnostic_metadata(meta: dict) -> None:
    if meta.get("evaluation_context") != OOS_CONTEXT or meta.get("kind") != "historical_diagnostics":
        raise ValueError("Historical OOS development label required")
    validate_historical_metadata({**{k: v for k, v in meta.items() if k != "evaluation_context"},
                                  "kind": "historical_predictions"})


def validate_diagnostic_records(records: list, meta: dict) -> None:
    """Validate a visible prefix, including only outcomes that have matured in it."""
    if not isinstance(records, list) or not 1 <= len(records) <= MAX_BARS:
        raise ValueError("Expected bounded diagnostic records")
    base = []
    for row in records:
        exact(row, {"bar", "available_at", "inspection", "outcomes"})
        base.append({k: v for k, v in row.items() if k != "outcomes"})
    validate_historical_records(base, meta)
    by_time = {utc(r["bar"]["opened_at"]): r for r in records}
    seen = set()
    for row in records:
        events = row["outcomes"]
        if not isinstance(events, list) or len(events) > 1:
            raise ValueError("Invalid outcome events")
        for outcome in events:
            exact(outcome, {"forecast_id", "decision_index", "entry_at", "exit_at", "available_at", "actual_ticks", "error_ticks"})
            digest(outcome["forecast_id"])
            index = outcome["decision_index"]
            if type(index) is not int or not 0 <= index < len(records) or index in seen:
                raise ValueError("Invalid or duplicate outcome reference")
            decision = records[index]["inspection"]
            if decision["status"] != "ELIGIBLE":
                raise ValueError("Outcome requires a recorded forecast")
            entry, end, known = (utc(outcome[k]) for k in ("entry_at", "exit_at", "available_at"))
            at = utc(decision["decision_at"])
            if (entry != at - timedelta(seconds=5) + timedelta(minutes=1)
                    or end != entry + timedelta(minutes=60)
                    or known != end + timedelta(minutes=1, seconds=5)
                    or known != utc(row["available_at"]) or entry not in by_time or end not in by_time):
                raise ValueError("Premature or incompatible outcome timing")
            actual = (decimal(by_time[end]["bar"]["open"]) - decimal(by_time[entry]["bar"]["open"])) / decimal(meta["tick_size"])
            if (decimal(outcome["actual_ticks"]) != actual
                    or decimal(outcome["error_ticks"]) != actual - decimal(decision["prediction"])):
                raise ValueError("Outcome movement or signed error differs")
            seen.add(index)
    clock = utc(records[-1]["available_at"])
    expected = {i for i, r in enumerate(records) if r["inspection"]["status"] == "ELIGIBLE"
                and utc(r["inspection"]["decision_at"]) + timedelta(minutes=62) <= clock}
    if seen != expected:
        raise ValueError("Missing matured outcome")


def validate_diagnostic_bundle(bundle: dict) -> None:
    if bundle["status"] != "COMPLETE":
        raise ValueError("Incomplete historical diagnostics")
    meta, rows = bundle["metadata"], bundle["records"]
    validate_diagnostic_metadata(meta)
    validate_diagnostic_records(rows, meta)
    if hashlib.sha256(encoded([r["bar"] for r in rows])).hexdigest() != meta["input_sha256"]:
        raise ValueError("Historical input identity mismatch")
    count = sum(r["inspection"]["status"] == "ELIGIBLE" for r in rows)
    outcomes = sum(len(r["outcomes"]) for r in rows)
    expected = {"predictions": count, "refused": sum(r["inspection"]["status"] == "REFUSED" for r in rows),
                "outcomes": outcomes, "message": NO_POLICY}
    if outcomes != count or bundle["summary"] != expected:
        raise ValueError("Incomplete outcome coverage or invalid summary")


def validate_library(library: dict) -> None:
    exact(library, {"schema", "status", "model_id", "spec_id", "evaluation_context", "sessions"})
    if (library["schema"] != LIBRARY_SCHEMA or library["status"] != "COMPLETE"
            or library["evaluation_context"] != OOS_CONTEXT):
        raise ValueError("Unsupported or incomplete session library")
    digest(library["model_id"])
    digest(library["spec_id"])
    entries = library["sessions"]
    if not isinstance(entries, list) or not 1 <= len(entries) <= 63:
        raise ValueError("Session library exceeds its bound")
    dates = []
    for entry in entries:
        exact(entry, {"session", "status", "contract", "path", "sha256", "reasons"})
        day = entry["session"]
        if not isinstance(day, str) or datetime.strptime(day, "%Y-%m-%d").strftime("%Y-%m-%d") != day:
            raise ValueError("Invalid session date")
        dates.append(day)
        reasons = entry["reasons"]
        if not isinstance(reasons, list) or len(reasons) > 20 or any(not isinstance(r, str) or not 1 <= len(r) <= 300 for r in reasons):
            raise ValueError("Invalid exclusion reasons")
        if entry["status"] == "INCLUDED":
            if (entry["path"] != f"{day}/result.json" or reasons or not isinstance(entry["contract"], str)
                    or not 1 <= len(entry["contract"]) <= 100):
                raise ValueError("Invalid included session")
            digest(entry["sha256"])
        elif entry["status"] == "EXCLUDED":
            if not reasons or any(entry[k] is not None for k in ("path", "sha256", "contract")):
                raise ValueError("Excluded sessions cannot name playable results")
        else:
            raise ValueError("Unfinished library session")
    if dates != sorted(set(dates)):
        raise ValueError("Duplicate or unordered library dates")


def load_library(path: Path) -> dict:
    with path.open("rb") as stream:
        raw = stream.read(100_001)
    if len(raw) > 100_000:
        raise ValueError("Oversized library index")
    try:
        library = json.loads(raw)
        validate_library(library)
    except (TypeError, KeyError, OverflowError) as exc:
        raise ValueError("Malformed session library") from exc
    return library


def load_library_session(directory: Path, library: dict, entry: dict) -> tuple[Path, dict]:
    root = directory.resolve(strict=True)
    path = (root / entry["path"]).resolve(strict=True)
    if not path.is_relative_to(root):
        raise ValueError("Session path escapes the library")
    # Size is bounded before either hashing or parsing untrusted saved content.
    if path.stat().st_size > MAX_BYTES:
        raise ValueError("Oversized session result")
    with path.open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("Oversized session result")
    if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
        raise ValueError("Session result changed")
    try:
        bundle = json.loads(raw)
        validate_bundle(bundle)
        meta = bundle["metadata"]
        if (bundle["schema"] != DIAGNOSTIC_SCHEMA or meta["model_id"] != library["model_id"]
                or meta["spec_id"] != library["spec_id"] or meta["session"] != entry["session"]
                or meta["contract"] != entry["contract"]):
            raise ValueError("Session provenance differs from library")
    except (TypeError, KeyError, OverflowError) as exc:
        raise ValueError("Malformed library session") from exc
    return path.parent, bundle
