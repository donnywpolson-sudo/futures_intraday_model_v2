"""One-use, pre-result ZN/6E execution-proxy gate."""

from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from .active_data_view import validate_catalog
from .alpha_ladder_execution_design import (
    EVIDENCE_FIELDS,
    MACRO_CANDIDATES,
    UNKNOWN,
    load_execution_design_contract,
    select_macro_execution_candidate,
)
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, io_path, sha256_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .historical_checkpoint_calendar import (
    EARLY_CLOSE,
    FULL_CLOSE,
    build_checkpoint_sessions,
)


OPERATION = "RUN_ZN_6E_EXECUTION_PROXY_GATE_ONCE"
PLAN_PATH = Path("configs/alpha_ladder_zn_6e_execution_proxy_plan_v1.json")
ACTIVE_DESIGN_PATH = Path("configs/active_alpha_ladder_execution_design.json")
CATALOG_PATH = Path("data/active/catalog.json")
HISTORICAL_CALENDAR_PATH = Path("configs/tier1_historical_checkpoint_calendar_v5.json")
CORRECTED_CALENDAR_PATH = Path("configs/active_cash_open_impulse_historical_calendar.json")
ACTIVE_MACRO_PATH = Path("configs/active_alpha_ladder_macro_market.json")
EVIDENCE_ROOT = Path("state/unpublished_evidence/alpha_ladder_zn_6e_execution_proxy")
SELECTION_ROOT = Path("state/alpha_ladder_macro_selection")
YEARS = tuple(range(2018, 2023))
SCHEDULE_SHA256 = "d30c7da4f38c2098fa1352335c5612fe1d4d583bef51036056c395314954fd63"
V10_RELEASE = "c10ee5951a386916d1aacdc4105ac7513db32d79349a8a0a91f48e90f42fb476"
CHECKPOINTS = {
    "ZN": "7cfee8171dc589b79f1d21eabb429ae6552215a3c0c89a23373f21d32193ff19",
    "6E": "80f5d3af598d00217c01ba72e170f577bcc0a762ab8d194f6f967775b9f60f20",
}
TABLES = ("observations", "missingness", "roll", "quality")
ECONOMICS_RULES_PATH = Path("configs/contract_economics_rules.json")
POINT_VALUES = {"ZN": Decimal("1000"), "6E": Decimal("125000")}
MAX_FILES = 480
MAX_ROWS = 4_000_000
MAX_BYTES = 512 * 1024 * 1024
CHICAGO = ZoneInfo("America/Chicago")


def _read_canonical(path: Path) -> dict[str, object]:
    try:
        raw = io_path(path).read_bytes()
        value = json.loads(raw)
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"canonical input is unreadable: {path}") from exc
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"input is not canonical JSON: {path}")
    return value


def _create_json(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(io_path(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
    except FileExistsError as exc:
        raise UnauthorizedOperation(f"create-only output already exists: {path}") from exc
    try:
        os.write(fd, canonical_bytes(value) + b"\n")
        os.fsync(fd)
    finally:
        os.close(fd)


def _binding(root: Path, relative: Path) -> dict[str, str]:
    return {
        "path": relative.as_posix(),
        "sha256": sha256_file(root / relative, reject_hardlinks=False),
    }


def _shared_dates() -> tuple[str, ...]:
    sessions = build_checkpoint_sessions()
    states = {(str(x["market"]), str(x["trade_date"])): x for x in sessions}
    result: list[str] = []
    for market, date_text in sorted(states):
        if market != "ZN" or date_text < "2018-01-02" or date_text > "2022-12-30":
            continue
        day = datetime.fromisoformat(date_text).date()
        if (
            day.weekday() >= 5
            or date_text in FULL_CLOSE
            or date_text in EARLY_CLOSE
            or date_text == "2018-12-05"
        ):
            continue
        if not all(
            all(bool(value) for value in states[(candidate, date_text)]["checkpoint_open"].values())
            for candidate in MACRO_CANDIDATES
        ):
            continue
        result.append(date_text)
    dates = tuple(result)
    if len(dates) != 1251 or sha256_json(list(dates)) != SCHEDULE_SHA256:
        raise IntegrityError("shared full-session calendar identity mismatch")
    return dates


def _plan_core(root: Path) -> dict[str, object]:
    design_pointer = _read_canonical(root / ACTIVE_DESIGN_PATH)
    if (
        design_pointer.get("target_path") != ACTIVE_DESIGN_PATH.as_posix()
        or design_pointer.get("state") != "EXECUTION_DESIGN_IDENTITY_ONLY"
        or design_pointer.get("pointer_id")
        != sha256_json({k: v for k, v in design_pointer.items() if k != "pointer_id"})
    ):
        raise IntegrityError("active execution-design pointer is invalid")
    contract = load_execution_design_contract(root)
    if design_pointer["execution_design_binding"]["contract_id"] != contract["contract_id"]:  # type: ignore[index]
        raise IntegrityError("active execution-design contract binding drifted")

    catalog = _read_canonical(root / CATALOG_PATH)
    validate_catalog(catalog)
    entries = [
        item for item in catalog["entries"]  # type: ignore[index]
        if item["market"] in MACRO_CANDIDATES and item["year"] in YEARS
    ]
    if len(entries) != 10 or any(
        item["cohort"] != "DISCOVERY_SELECTION"
        or item["selection_eligible"] is not True
        or "SELECTION" not in item["permitted_uses"]
        for item in entries
    ):
        raise UnauthorizedOperation("catalog does not authorize exactly ZN/6E 2018-2022 selection")

    seals: list[dict[str, object]] = []
    files: list[dict[str, object]] = []
    for market in MACRO_CANDIDATES:
        checkpoint_root = Path("data/causally_gated_normalized/v10") / V10_RELEASE / market / CHECKPOINTS[market]
        checkpoint = checkpoint_root / "market_checkpoint.json"
        for year in YEARS:
            seal_path = checkpoint_root / "work_unit_seals" / f"{year}.json"
            seal = _read_canonical(root / seal_path)
            if seal.get("market") != market or seal.get("year") != year:
                raise IntegrityError("V10 work-unit seal scope mismatch")
            selected = [
                item for item in seal.get("files", [])
                if Path(str(item.get("path", ""))).stem in TABLES
            ]
            if len(selected) != 48:
                raise IntegrityError("work-unit seal does not bind 48 required files")
            files.extend(selected)
            seals.append({"market": market, "year": year, **_binding(root, seal_path), "seal_id": seal["seal_id"]})
        seals.append({"market": market, "checkpoint_id": CHECKPOINTS[market], **_binding(root, checkpoint)})
    if len(files) != MAX_FILES or sum(int(item["size"]) for item in files) > MAX_BYTES:
        raise IntegrityError("bound file inventory exceeds the frozen limit")
    if sum(int(item["row_count"]) for item in entries) > MAX_ROWS:
        raise IntegrityError("bound observation inventory exceeds the frozen row limit")

    try:
        rules = json.loads(io_path(root / ECONOMICS_RULES_PATH).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError("contract-economics rules are unreadable") from exc
    if not isinstance(rules, dict):
        raise IntegrityError("contract-economics rules are malformed")
    actual_point_values = {
        item["market"]: Decimal(str(item["point_value"]))
        for item in rules.get("rules", [])
        if item.get("market") in MACRO_CANDIDATES
    }
    if actual_point_values != POINT_VALUES:
        raise IntegrityError("certified ZN/6E point-value rules drifted")

    dates = _shared_dates()
    return {
        "schema_version": "alpha_ladder_zn_6e_execution_proxy_plan/1.0.0",
        "state": "PREPARED_REQUIRES_EXACT_APPROVAL",
        "operation": OPERATION,
        "attempts": 1,
        "retries": 0,
        "markets": list(MACRO_CANDIDATES),
        "years": list(YEARS),
        "selection_eligible_period": {"start": dates[0], "end": dates[-1]},
        "schedule": {
            "session_count": len(dates),
            "dates_sha256": SCHEDULE_SHA256,
            "expected_minutes_per_session": 385,
            "calendar_bindings": [
                _binding(root, HISTORICAL_CALENDAR_PATH),
                _binding(root, CORRECTED_CALENDAR_PATH),
            ],
            "excluded_correction": "2018-12-05",
        },
        "bindings": {
            "active_execution_design": {**_binding(root, ACTIVE_DESIGN_PATH), "pointer_id": design_pointer["pointer_id"]},
            "active_catalog": {**_binding(root, CATALOG_PATH), "catalog_sha256": catalog["catalog_sha256"]},
            "execution_design_contract_id": contract["contract_id"],
            "contract_economics_rules": _binding(root, ECONOMICS_RULES_PATH),
            "v10_release": V10_RELEASE,
            "checkpoints_and_work_unit_seals": seals,
            "files": sorted(files, key=lambda item: str(item["path"])),
            "implementation": {
                "runner": _binding(root, Path("src/futures_rebuild/alpha_ladder_zn_6e_execution_proxy.py")),
                "frozen_evaluator": _binding(root, Path("src/futures_rebuild/alpha_ladder_execution_design.py")),
                "receipt_boundary": _binding(root, Path("src/futures_rebuild/boundary.py")),
                "gateway_policy": _binding(root, Path("src/futures_rebuild/research_gateway_policy.py")),
            },
        },
        "limits": {
            "market_year_partitions": 10,
            "files": MAX_FILES,
            "observation_rows": MAX_ROWS,
            "bytes": MAX_BYTES,
        },
        "outputs": {
            "evidence_root": EVIDENCE_ROOT.as_posix(),
            "selection_root": SELECTION_ROOT.as_posix(),
            "active_macro_pointer": ACTIVE_MACRO_PATH.as_posix(),
            "create_only": True,
            "raw_rows_forbidden": True,
        },
        "authority": {
            "authorization_receipt_issued": False,
            "market_row_read": False,
            "macro_selection": False,
            "research_results": False,
            "provider_or_network": False,
            "final_252_or_forward": False,
        },
    }


def prepare_execution_proxy_plan(root: Path) -> dict[str, object]:
    """Create or verify the inactive, metadata-only one-use plan."""

    root = root.resolve()
    core = _plan_core(root)
    plan = {**core, "plan_id": sha256_json(core)}
    path = root / PLAN_PATH
    if io_path(path).exists():
        if _read_canonical(path) != plan:
            raise IntegrityError("prepared execution-proxy plan drifted")
    else:
        _create_json(path, plan)
    return plan


def _load_plan(root: Path) -> dict[str, object]:
    observed = _read_canonical(root / PLAN_PATH)
    expected_core = _plan_core(root)
    expected = {**expected_core, "plan_id": sha256_json(expected_core)}
    if observed != expected:
        raise IntegrityError("execution-proxy plan or a metadata binding drifted")
    return observed


def required_scope(root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "operation_kind": "PRE_RESULT_EXECUTION_PROXY_AND_AUTOMATIC_MACRO_FREEZE",
        "markets": "ZN,6E",
        "years": "2018,2019,2020,2021,2022",
        "schedule_sha256": SCHEDULE_SHA256,
        "active_design_pointer_id": str(plan["bindings"]["active_execution_design"]["pointer_id"]),  # type: ignore[index]
        "active_catalog_sha256": str(plan["bindings"]["active_catalog"]["catalog_sha256"]),  # type: ignore[index]
        "plan_id": str(plan["plan_id"]),
        "plan_sha256": sha256_file(root / PLAN_PATH, reject_hardlinks=False),
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH, reject_hardlinks=False),
    }


def _unknown() -> dict[str, object]:
    return {field: None for field in EVIDENCE_FIELDS}


def _ratio(numerator: int, denominator: int) -> str:
    return str(Decimal(numerator) / Decimal(denominator)) if denominator else "0"


def _valid_metadata(row: Mapping[str, object], available: int, start: int) -> bool:
    try:
        activation = int(row["listing_activation_ns"])
        expiration = int(row["expiration_ns"])
        return (
            int(row["instrument_id"]) > 0
            and int(row["publisher_id"]) > 0
            and bool(row["actual_contract"])
            and bool(row["raw_symbol"])
            and str(row["currency"]) == "USD"
            and int(row["definition_event_at_ns"]) <= int(row["definition_received_at_ns"]) <= available
            and (activation in {0, 2**64 - 1} or activation <= start)
            and (expiration in {0, 2**64 - 1} or expiration > start)
            and int(row["min_price_increment_nano"]) > 0
            and int(row["multiplier_nano"]) > 0
        )
    except (KeyError, TypeError, ValueError):
        return False


def aggregate_execution_proxy(
    contract: Mapping[str, object],
    market: str,
    scheduled_dates: Sequence[str],
    bundles: Iterable[Mapping[str, Sequence[Mapping[str, object]]]],
) -> dict[str, object]:
    """Construct only the aggregate evidence accepted by the frozen evaluator."""

    if market not in MACRO_CANDIDATES or not scheduled_dates or any(
        date[:4] not in {str(year) for year in YEARS} for date in scheduled_dates
    ):
        raise UnauthorizedOperation("execution proxy is restricted to ZN/6E 2018-2022")
    scheduled = set(scheduled_dates)
    rows: dict[tuple[str, int], tuple[Mapping[str, object], bool, bool, bool]] = {}
    duplicates: set[tuple[str, int]] = set()
    roll_dates: set[str] = set()
    identities: dict[str, set[str]] = defaultdict(set)
    malformed = False
    for bundle in bundles:
        observations = bundle.get("observations", ())
        try:
            missing = {int(x["observation_ordinal"]): x for x in bundle.get("missingness", ()) if x.get("observation_ordinal") is not None}
            quality = {int(x["observation_ordinal"]): x for x in bundle.get("quality", ())}
            roll = {int(x["observation_ordinal"]): x for x in bundle.get("roll", ())}
        except (KeyError, TypeError, ValueError):
            return _unknown()
        if len(quality) != len(observations) or len(roll) != len(observations):
            return _unknown()
        for ordinal, row in enumerate(observations):
            try:
                date_text = str(row["project_trade_date"])
                start = int(row["bar_start_ns"])
                local = datetime.fromtimestamp(start / 1_000_000_000, timezone.utc).astimezone(CHICAGO)
                if date_text not in scheduled:
                    continue
                roll_row = roll[ordinal]
                if bool(roll_row.get("roll_flag")):
                    roll_dates.add(date_text)
                if not (time(8, 35) <= local.time().replace(tzinfo=None) <= time(14, 59)):
                    continue
                key = (date_text, start)
                if key in rows:
                    duplicates.add(key)
                    continue
                available = int(row["available_at_ns"])
                q = quality[ordinal]
                missing_ok = missing.get(ordinal, {}).get("state") == "OBSERVED_VALID"
                missing_row = missing.get(ordinal, {})
                missing_ok = (
                    missing_ok
                    and missing_row.get("market") == market
                    and int(missing_row.get("interval_start_ns", -1)) == start
                    and int(missing_row.get("interval_end_ns", -1)) == int(row["bar_end_ns"])
                )
                quality_ok = (
                    q.get("ohlc_valid") is True
                    and q.get("volume_valid") is True
                    and q.get("timestamp_order_valid") is True
                    and q.get("duplicate_state") == "UNIQUE"
                )
                causal = (
                    row.get("market") == market
                    and row.get("project_timezone") == "America/Chicago"
                    and int(row["bar_end_ns"]) == start + 60_000_000_000
                    and available <= start + 65_000_000_000
                    and int(row["decision_eligible_at_ns"]) <= start + 65_000_000_000
                )
                metadata_ok = _valid_metadata(row, available, start)
                identities[date_text].add(str(row["actual_contract"]))
                rows[key] = (row, bool(missing_ok and quality_ok and causal), metadata_ok, quality_ok)
            except (KeyError, TypeError, ValueError, OSError, OverflowError):
                malformed = True
    if malformed or duplicates:
        return _unknown()

    nonroll = [date for date in scheduled_dates if date not in roll_dates]
    expected = len(nonroll) * 385
    complete = metadata_complete = zero_volume = quality_errors = 0
    movements: list[Decimal] = []
    tick_values: set[Decimal] = set()
    identity_violations = sum(len(identities[date]) > 1 for date in nonroll)
    for date_text in nonroll:
        midnight = datetime.combine(datetime.fromisoformat(date_text).date(), time(), CHICAGO)
        minute_rows: dict[int, tuple[Mapping[str, object], bool, bool, bool]] = {}
        for minute in range(8 * 60 + 35, 15 * 60):
            local = midnight.replace(hour=minute // 60, minute=minute % 60)
            start = int(local.astimezone(timezone.utc).timestamp() * 1_000_000_000)
            item = rows.get((date_text, start))
            if item:
                minute_rows[minute] = item
                valid, metadata_ok = item[1], item[2]
                metadata_complete += int(metadata_ok)
                complete += int(valid and metadata_ok)
                quality_errors += int(not item[3])
                if valid and metadata_ok:
                    zero_volume += int(int(item[0]["volume"]) == 0)
        for entry_minute in range(8 * 60 + 36, 14 * 60 + 26):
            pair = minute_rows.get(entry_minute), minute_rows.get(entry_minute + 30)
            if not all(pair) or not all(item[1] and item[2] for item in pair):  # type: ignore[union-attr]
                continue
            entry, exit_ = pair[0][0], pair[1][0]  # type: ignore[index]
            if entry["actual_contract"] != exit_["actual_contract"]:
                identity_violations += 1
                continue
            tick = Decimal(int(entry["min_price_increment_nano"]))
            if tick != Decimal(int(exit_["min_price_increment_nano"])):
                return _unknown()
            movements.append(abs(Decimal(int(exit_["open_nano"])) - Decimal(int(entry["open_nano"]))) / tick)
            expected_multiplier = POINT_VALUES[market] * (Decimal("100") if market == "ZN" else Decimal("1"))
            if Decimal(int(entry["multiplier_nano"])) / Decimal(10) ** 9 != expected_multiplier:
                return _unknown()
            tick_values.add(tick / Decimal(10) ** 9 * POINT_VALUES[market])
    if not movements or len(tick_values) != 1 or expected == 0:
        return _unknown()
    movement = median(movements)
    tick_value = next(iter(tick_values))
    friction = Decimal(str(contract["cost_model"]["adverse_ticks_round_trip"][market])) + Decimal("10.00") / tick_value  # type: ignore[index]
    return {
        "target_horizon_movement_ticks": str(movement),
        "conservative_round_trip_friction_ticks": {"value": str(friction), "tick_value_usd": str(tick_value)},
        "movement_to_cost_ratio": str(movement / friction),
        "active_minute_coverage": {"usable_sessions": len(nonroll), "usable_observations": complete, "ratio": _ratio(complete, expected)},
        "zero_volume_minute_fraction": _ratio(zero_volume, complete),
        "missingness_and_continuity": {
            "unexpected_gap_or_stale_fraction": _ratio(expected - complete, expected),
            "point_in_time_metadata_coverage_ratio": _ratio(metadata_complete, expected),
            "quality_error_count": quality_errors,
        },
        "roll_behavior": {"excluded_roll_dates": len(roll_dates), "identity_violation_count": identity_violations},
    }


def _read_bound_bundles(
    root: Path, plan: Mapping[str, object], market: str
) -> Iterable[dict[str, object]]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    selected = [item for item in plan["bindings"]["files"] if f"/{market}/" in item["path"]]  # type: ignore[index]
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for item in selected:
        grouped[Path(str(item["path"])).parent.as_posix()].append(item)
    if len(selected) != MAX_FILES // 2 or any(len(items) != 4 for items in grouped.values()):
        raise IntegrityError("execution-proxy file inventory is incomplete")
    observation_rows = total_bytes = file_count = 0
    for directory in sorted(grouped):
        bundle: dict[str, object] = {}
        for item in sorted(grouped[directory], key=lambda value: str(value["path"])):
            file_count += 1
            total_bytes += int(item["size"])
            if file_count > MAX_FILES or total_bytes > MAX_BYTES:
                raise IntegrityError("execution-proxy file limit breached")
            path = root / str(item["path"])
            data = io_path(path).read_bytes()
            if len(data) != item["size"] or sha256_bytes(data) != item["sha256"]:
                raise IntegrityError("bound V10 file hash drifted")
            table = pq.ParquetFile(pa.BufferReader(data)).read().to_pylist()
            bundle[path.stem] = table
            if path.stem == "observations":
                observation_rows += len(table)
                if observation_rows > MAX_ROWS:
                    raise IntegrityError("execution-proxy observation row limit breached")
        if set(bundle) != set(TABLES):
            raise IntegrityError("execution-proxy monthly ledger bundle is incomplete")
        yield bundle


def execute_authorized_execution_proxy(
    root: Path, authorization: OperationReceipt
) -> dict[str, object]:
    """Consume one exact receipt, read once, and freeze only a deterministic winner."""

    root = root.resolve()
    plan = _load_plan(root)
    if io_path(root / ACTIVE_MACRO_PATH).exists():
        raise UnauthorizedOperation("active macro pointer already exists")
    boundary = RepoBoundary(root)
    scope = required_scope(root, plan)
    claim = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    run_id = sha256_json({"plan_id": plan["plan_id"], "receipt_id": authorization.receipt_id})
    run_root = root / EVIDENCE_ROOT / run_id
    try:
        contract = load_execution_design_contract(root)
        dates = _shared_dates()
        evidence = {
            market: aggregate_execution_proxy(contract, market, dates, _read_bound_bundles(root, plan, market))
            for market in MACRO_CANDIDATES
        }
        result = select_macro_execution_candidate(contract, evidence)
    except Exception as exc:
        _create_json(run_root / "terminal.json", {
            "schema_version": "alpha_ladder_zn_6e_execution_proxy_terminal/1.0.0",
            "run_id": run_id,
            "outcome": UNKNOWN,
            "selected_market": None,
            "macro_pointer_created": False,
            "error_type": type(exc).__name__,
        })
        raise
    evidence_payload = {
        "schema_version": "alpha_ladder_zn_6e_execution_proxy_evidence/1.0.0",
        "run_id": run_id,
        "plan_id": plan["plan_id"],
        "receipt_id": authorization.receipt_id,
        "authorization_use_sha256": sha256_file(claim, reject_hardlinks=False),
        "schedule_sha256": SCHEDULE_SHA256,
        "aggregate_evidence": evidence,
        "selector_result": result,
    }
    _create_json(run_root / "evidence.json", evidence_payload)
    selected = result["selected_market"]
    if selected is not None:
        record_core = {
            "schema_version": "alpha_ladder_macro_selection/1.0.0",
            "market": selected,
            "run_id": run_id,
            "evidence_sha256": sha256_file(run_root / "evidence.json", reject_hardlinks=False),
            "execution_design_pointer_id": plan["bindings"]["active_execution_design"]["pointer_id"],  # type: ignore[index]
        }
        record = {**record_core, "selection_id": sha256_json(record_core)}
        record_path = root / SELECTION_ROOT / str(record["selection_id"]) / "selection.json"
        _create_json(record_path, record)
        pointer_core = {
            "schema_version": "active_alpha_ladder_macro_market/1.0.0",
            "state": "ACTIVE_MACRO_SELECTION",
            "market": selected,
            "selection_id": record["selection_id"],
            "selection_path": record_path.relative_to(root).as_posix(),
            "selection_sha256": sha256_file(record_path, reject_hardlinks=False),
        }
        _create_json(root / ACTIVE_MACRO_PATH, {**pointer_core, "pointer_id": sha256_json(pointer_core)})
    terminal = {
        "schema_version": "alpha_ladder_zn_6e_execution_proxy_terminal/1.0.0",
        "run_id": run_id,
        "outcome": result["outcome"],
        "selected_market": selected,
        "macro_pointer_created": selected is not None,
    }
    _create_json(run_root / "terminal.json", terminal)
    return terminal
