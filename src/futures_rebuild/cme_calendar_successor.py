"""Prepare an inactive, reference-only 41-market cash-open calendar successor."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Iterable
from zipfile import ZipFile
from zoneinfo import ZoneInfo

from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .cme_calendar_source_adequacy import (
    FamilySchedule,
    ScheduleEvent,
    parse_family_schedule,
    read_xls_bytes,
    read_xls_cells,
)
from .data_layout import DataReleaseReceipt
from .errors import IntegrityError
from .historical_checkpoint_calendar import (
    EARLY_CLOSE,
    FULL_CLOSE,
    MARKET_CHECKPOINT_OVERRIDES,
    load_historical_checkpoint_calendar,
)


YEARS = range(2018, 2023)
MARKET_FAMILY = {
    **{m: "FX" for m in ("6A", "6B", "6C", "6E", "6J", "6M", "6N", "6S")},
    **{m: "EQUITY_INDEX" for m in ("ES", "NQ", "RTY", "YM")},
    **{m: "CRYPTO" for m in ("BTC", "ETH")},
    **{m: "ENERGY" for m in ("CL", "HO", "NG", "RB")},
    **{m: "METALS" for m in ("GC", "HG", "PA", "PL", "SI")},
    **{m: "RATES" for m in ("SR1", "SR3", "TN", "UB", "ZB", "ZF", "ZN", "ZQ", "ZT")},
    **{m: "LIVESTOCK" for m in ("GF", "HE", "LE")},
    **{m: "GRAIN_OILSEED" for m in ("KE", "ZC", "ZL", "ZM", "ZS", "ZW")},
}
FAMILY_PATTERN = {
    "FX": re.compile(r"FX(?: Products)?", re.I),
    "EQUITY_INDEX": re.compile(r"Equity(?: Products)?", re.I),
    "CRYPTO": re.compile(r"(?:Bitcoin|Cryptocurrency)", re.I),
    "ENERGY": re.compile(r"Energy.*Metal.*DME.*", re.I),
    "METALS": re.compile(r"Energy.*Metal.*DME.*", re.I),
    "RATES": re.compile(r"Interest Rate(?: Products)?", re.I),
    "LIVESTOCK": re.compile(r"Livestock", re.I),
    "GRAIN_OILSEED": re.compile(
        r"(?:Grains and Oilseeds|Grain & Oilseed|"
        r"Grains \(& Mini-Sized\) and Oilseeds \(& Mini-Sized\))",
        re.I,
    ),
}
WINDOWS = (("09:00", "08:29", "09:31"), ("10:30", "10:00", "11:01"))
CHICAGO = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")
FOUNDATION = Path("manifests/data_releases/foundation/637f16b3c23c9f2215858f49754965738fe9c00095661d7a29d6877d566ae5e3.json")
RECOVERY_RECORD = Path(
    "state/unpublished_evidence/cash_open_impulse_jan1_2019_calendar_recovery/"
    "fbca0af01eee949039e8efd572c0e298d5d9644b9bbaa8369708f0b876a55161/"
    "acquisition_record.json"
)
RECOVERY_RECORD_SHA256 = "2c0dcb6c12316ef582e220590c48a5ad29760a62c149412d685706931f43f0dc"
RECOVERY_RAW_SHA256 = "2684a5f1b3a9f65802f6911ca6089e2cb68c3cdf2520dfaf3cdcf3105328c188"


def _dates() -> Iterable[date]:
    value, end = date(2018, 1, 1), date(2022, 12, 31)
    while value <= end:
        yield value
        value += timedelta(days=1)


def _load_schedules(boundary: RepoBoundary) -> tuple[dict[tuple[str, date], list[tuple[str, str, FamilySchedule]]], str]:
    loaded = load_historical_checkpoint_calendar(boundary=boundary)
    manifest = loaded.capture_receipt.verify(boundary)
    result: dict[tuple[str, date], list[tuple[str, str, FamilySchedule]]] = defaultdict(list)
    for entry in manifest.files:
        path = loaded.capture_receipt.resolve_file(entry.logical_path, boundary)
        items: list[tuple[str, str, object]] = []
        if path.suffix.lower() == ".xls":
            items.append((entry.logical_path, entry.sha256, read_xls_cells(path)))
        elif path.suffix.lower() == ".zip":
            with ZipFile(path) as archive:
                for member in sorted(name for name in archive.namelist() if name.lower().endswith(".xls")):
                    raw = archive.read(member)
                    items.append((f"{entry.logical_path}::{member}", sha256(raw).hexdigest(), read_xls_bytes(raw)))
        for source, source_hash, cells in items:
            for family, pattern in FAMILY_PATTERN.items():
                try:
                    schedule = parse_family_schedule(cells, label_pattern=pattern)  # type: ignore[arg-type]
                except IntegrityError:
                    continue
                for observed in set(schedule.calendar_dates):
                    if observed.year in YEARS:
                        result[(family, observed)].append((source, source_hash, schedule))
    return result, loaded.capture_receipt.release_id


def _load_recovered_jan1_schedule(root: Path) -> tuple[dict[str, FamilySchedule], dict[str, object]]:
    record_path = root / RECOVERY_RECORD
    if sha256_file(record_path) != RECOVERY_RECORD_SHA256:
        raise IntegrityError("January 1 recovery record hash drifted")
    raw_record = record_path.read_bytes()
    record = json.loads(raw_record.decode("utf-8"))
    if raw_record != canonical_bytes(record) + b"\n":
        raise IntegrityError("January 1 recovery record is not canonical")
    record_core = {key: value for key, value in record.items() if key != "record_id"}
    if record.get("record_id") != sha256_json(record_core):
        raise IntegrityError("January 1 recovery record ID is invalid")
    if record.get("decision") != "PASS_EXACT_AUTHORITATIVE_SCHEDULE_ACQUIRED_INACTIVE":
        raise IntegrityError("January 1 recovery record is not an accepted inactive acquisition")
    raw_path = root / str(record["raw"]["path"])
    sidecar_path = root / str(record["sidecar"]["path"])
    if sha256_file(raw_path) != RECOVERY_RAW_SHA256 or record["raw"]["sha256"] != RECOVERY_RAW_SHA256:
        raise IntegrityError("January 1 recovered workbook hash drifted")
    if sha256_file(sidecar_path) != record["sidecar"]["sha256"]:
        raise IntegrityError("January 1 recovery sidecar hash drifted")
    cells = read_xls_bytes(raw_path.read_bytes())
    rows: dict[int, dict[int, str | float]] = {}
    for cell in cells:
        rows.setdefault(cell.row, {})[cell.column] = cell.value
    if " ".join(str(rows.get(0, {}).get(1, "")).split()) != (
        "CME Group Globex New Years Holiday Schedule: December 31, 2018 - January 2, 2019"
    ):
        raise IntegrityError("January 1 recovered workbook title is invalid")
    if " ".join(str(rows.get(2, {}).get(2, "")).split()) != "Tuesday, Jan 1":
        raise IntegrityError("January 1 recovered workbook target date is invalid")
    labels = {
        "EQUITY_INDEX": "Equity", "CRYPTO": "Bitcoin", "RATES": "Interest Rate",
        "FX": "FX", "ENERGY": "Energy, Metals & DME", "METALS": "Energy, Metals & DME",
        "GRAIN_OILSEED": "Grain & Oilseed", "LIVESTOCK": "Livestock",
    }
    day = date(2019, 1, 1)
    schedules: dict[str, FamilySchedule] = {}
    for family, label in labels.items():
        matches = [
            (row, values) for row, values in rows.items()
            if " ".join(str(values.get(0, "")).split()).casefold() == label.casefold()
        ]
        if len(matches) != 1:
            raise IntegrityError(f"January 1 recovered workbook lacks one exact {family} row")
        row, values = matches[0]
        disposition = " ".join(str(values.get(2, "")).split())
        if disposition.casefold() != "closed for new year's":
            raise IntegrityError(f"January 1 recovered workbook does not close {family}")
        schedules[family] = FamilySchedule(
            cells[0].sheet, row, label, (day,),
            (ScheduleEvent(datetime.combine(day, time()), 2, "CLOSED_ALL_DAY", disposition),),
        )
    binding = {
        "record_path": RECOVERY_RECORD.as_posix(),
        "record_sha256": RECOVERY_RECORD_SHA256,
        "raw_path": raw_path.relative_to(root).as_posix(),
        "raw_sha256": RECOVERY_RAW_SHA256,
        "sidecar_path": sidecar_path.relative_to(root).as_posix(),
        "sidecar_sha256": record["sidecar"]["sha256"],
        "archive_timestamp": record["archive_timestamp"],
        "original_url": record["original_url"],
    }
    return schedules, binding


def _state(schedule: FamilySchedule, start: datetime, end: datetime) -> bool:
    observed = schedule.state_at(start)
    if observed is None:
        # Holiday sheets begin while an already-open session is in progress.
        observed = True
    if not observed:
        return False
    return not any(
        start < event.at <= end and event.event in {"HALT", "CLOSE", "PREOPEN", "CLOSED_ALL_DAY"}
        for event in schedule.events
    )


def _definition_evidence(root: Path) -> tuple[dict[str, list[tuple[int, int]]], list[dict[str, object]]]:
    try:
        import databento as db
    except ImportError as exc:  # pragma: no cover - environment invariant
        raise IntegrityError("bound definition reader is unavailable") from exc
    foundation = json.loads((root / FOUNDATION).read_text(encoding="utf-8"))["embedded_documents"]["foundation_set.json"]
    intervals: dict[str, list[tuple[int, int]]] = defaultdict(list)
    bindings: dict[str, dict[str, object]] = {}
    month_codes = "FGHJKMNQUVXZ"
    for item in foundation["intervals"]:
        market, year = item["market"], int(item["year"])
        if market not in MARKET_FAMILY or year not in YEARS:
            continue
        path = root / "data" / item["definition_source_path"]
        if sha256_file(path) != item["definition_source_sha256"]:
            raise IntegrityError(f"definition hash drifted for {market} {year}")
        bindings[path.as_posix()] = {
            "path": path.relative_to(root).as_posix(),
            "sha256": item["definition_source_sha256"],
        }
        outright = re.compile(rf"^{re.escape(market)}[{month_codes}]\d{{1,2}}$")
        for record in db.DBNStore.from_file(path):
            if record.instrument_class != "F" or record.security_type != "FUT":
                continue
            if record.asset != market or outright.fullmatch(record.raw_symbol) is None:
                continue
            intervals[market].append((int(record.activation), int(record.expiration)))
    for market in MARKET_FAMILY:
        if not intervals[market]:
            raise IntegrityError(f"no exact outright definition interval for {market}")
    return intervals, sorted(bindings.values(), key=lambda item: str(item["path"]))


def _effective(intervals: list[tuple[int, int]], when: datetime) -> bool:
    ns = int(when.astimezone(UTC).timestamp() * 1_000_000_000)
    return any(start <= ns < end for start, end in intervals)


def _effective_interval_evidence(values: dict[str, list[tuple[int, int]]]) -> dict[str, list[dict[str, object]]]:
    result: dict[str, list[dict[str, object]]] = {}
    for market, raw in sorted(values.items()):
        merged: list[list[int]] = []
        for start, end in sorted(set(raw)):
            if end <= start:
                raise IntegrityError(f"definition effective interval is invalid for {market}")
            if merged and start <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], end)
            else:
                merged.append([start, end])
        result[market] = [
            {
                "activation_ns": start,
                "activation_utc": datetime.fromtimestamp(start / 1e9, UTC).isoformat(),
                "expiration_ns_exclusive": end,
                "expiration_utc_exclusive": datetime.fromtimestamp(end / 1e9, UTC).isoformat(),
            }
            for start, end in merged
        ]
    return result


def _economics_mapping(root: Path) -> dict[str, list[str]]:
    payload = json.loads((root / "configs/contract_economics_rules.json").read_text(encoding="utf-8"))
    observed = {item["market"]: list(item["source_ids"]) for item in payload["rules"]}
    if set(observed) != set(MARKET_FAMILY):
        raise IntegrityError("economics rulebook does not bind the exact 41-market universe")
    for market, family in MARKET_FAMILY.items():
        sources = observed[market]
        if family == "LIVESTOCK" and not any("LIVESTOCK" in value or "CONTRACT_SPECIFICATIONS" in value for value in sources):
            raise IntegrityError(f"livestock mapping lacks exact economics authority for {market}")
        if family == "GRAIN_OILSEED" and "CME_AG_STANDARD_TICKS" not in sources:
            raise IntegrityError(f"grain mapping lacks exact economics authority for {market}")
    return dict(sorted(observed.items()))


def build_successor(*, root: Path) -> dict[str, object]:
    boundary = RepoBoundary(root)
    schedules, capture_release = _load_schedules(boundary)
    recovered_schedules, recovery_binding = _load_recovered_jan1_schedule(root)
    recovered_date = date(2019, 1, 1)
    for family, schedule in recovered_schedules.items():
        schedules[(family, recovered_date)].append(
            (str(recovery_binding["raw_path"]), str(recovery_binding["raw_sha256"]), schedule)
        )
    definitions, definition_bindings = _definition_evidence(root)
    economics_sources = _economics_mapping(root)
    exceptional = set(FULL_CLOSE) | set(EARLY_CLOSE) | {key[0] for key in MARKET_CHECKPOINT_OVERRIDES}
    rows: list[dict[str, object]] = []
    unresolved: list[dict[str, str]] = []
    for market, family in sorted(MARKET_FAMILY.items()):
        for day in _dates():
            checkpoints: dict[str, bool] = {}
            reasons: dict[str, str] = {}
            for checkpoint, start_text, end_text in WINDOWS:
                start = datetime.combine(day, time.fromisoformat(start_text))
                end = datetime.combine(day, time.fromisoformat(end_text))
                effective = _effective(definitions[market], start.replace(tzinfo=CHICAGO))
                if not effective:
                    checkpoints[checkpoint] = False
                    reasons[checkpoint] = "PRODUCT_NOT_EFFECTIVE"
                elif day.weekday() >= 5:
                    checkpoints[checkpoint] = False
                    reasons[checkpoint] = "WEEKEND_CLOSED"
                elif day.isoformat() not in exceptional:
                    checkpoints[checkpoint] = True
                    reasons[checkpoint] = "REGULAR_WEEKDAY_REFERENCE_RULE"
                else:
                    candidates = schedules.get((family, day), [])
                    full_candidates = [item for item in candidates if "compact" not in item[0].lower()]
                    if full_candidates:
                        candidates = full_candidates
                    if not candidates:
                        checkpoints[checkpoint] = False
                        reasons[checkpoint] = "UNVERIFIED_REFERENCE_ABSTENTION"
                        unresolved.append({"market": market, "family": family, "date": day.isoformat(), "checkpoint": checkpoint})
                    else:
                        values = {_state(schedule, start, end) for _, _, schedule in candidates}
                        if len(values) != 1:
                            checkpoints[checkpoint] = False
                            reasons[checkpoint] = "CONFLICTING_REFERENCE_ABSTENTION"
                            unresolved.append({"market": market, "family": family, "date": day.isoformat(), "checkpoint": checkpoint})
                        else:
                            checkpoints[checkpoint] = values.pop()
                            reasons[checkpoint] = "EXACT_CME_FAMILY_SCHEDULE"
            rows.append({
                "market": market, "schedule_family": family, "trade_date": day.isoformat(),
                "checkpoint_open": checkpoints, "disposition": reasons,
            })
    core = {
        "schema_version": "cash_open_impulse_41_market_calendar_successor_preparation/1.0.0",
        "status": "PREPARED_INACTIVE_UNPUBLISHED",
        "decision": "PASS_EXACT_REFERENCE_COVERAGE" if not unresolved else "FAIL_UNRESOLVED_REFERENCE_COVERAGE",
        "authority": {"price_rows_read": False, "provider_network_credentials_accessed": False, "year_2025_accessed": False, "published": False, "active": False},
        "market_to_schedule_family": dict(sorted(MARKET_FAMILY.items())),
        "market_economics_source_ids": economics_sources,
        "calendar_capture_release_id": capture_release,
        "additive_january_1_2019_recovery": recovery_binding,
        "definition_bindings": definition_bindings,
        "product_effective_intervals": _effective_interval_evidence(definitions),
        "calendar_rows": rows,
        "unresolved_reference_count": len(unresolved),
        "unresolved_reference_states": unresolved,
        "bindings": {
            FOUNDATION.as_posix(): sha256_file(root / FOUNDATION),
            "configs/contract_economics_rules.json": sha256_file(root / "configs/contract_economics_rules.json"),
            "configs/tier1_historical_checkpoint_calendar_v5.json": sha256_file(root / "configs/tier1_historical_checkpoint_calendar_v5.json"),
            "src/futures_rebuild/cme_calendar_source_adequacy.py": sha256_file(root / "src/futures_rebuild/cme_calendar_source_adequacy.py"),
            "src/futures_rebuild/cme_calendar_successor.py": sha256_file(Path(__file__)),
            "scripts/prepare_cash_open_41_market_calendar_successor.py": sha256_file(root / "scripts/prepare_cash_open_41_market_calendar_successor.py"),
            RECOVERY_RECORD.as_posix(): RECOVERY_RECORD_SHA256,
        },
    }
    return {**core, "calendar_id": sha256_json(core)}
