"""One-use execution proxy for SR3, ZB, and 6J only."""

from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, time, timezone
from decimal import Decimal
from pathlib import Path, PurePosixPath
from statistics import median
from typing import Iterable, Mapping, Sequence

from .active_data_view import validate_catalog
from .alpha_ladder_macro_successor import (
    ELIGIBLE, EVIDENCE_FIELDS, INELIGIBLE, NEW_CANDIDATES, NO_ELIGIBLE,
    UNKNOWN, load_successor_contract, select_successor_macro,
)
from .alpha_ladder_macro_successor_identity import (
    EXECUTION_POINTER_PATH, LADDER_POINTER_PATH, RANK1_POINTER_PATH,
    build_identities,
)
from .alpha_ladder_zn_6e_execution_proxy import (
    CHICAGO, CORRECTED_CALENDAR_PATH, HISTORICAL_CALENDAR_PATH, TABLES,
    _ratio, _shared_dates, _valid_metadata,
)
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, io_path, sha256_bytes, sha256_file, sha256_json
from .causal_nine_market_successor import CONTRACT_PATH as CATALOG_CONTRACT_PATH
from .errors import IntegrityError, UnauthorizedOperation


OPERATION = "RUN_ALPHA_LADDER_MACRO_SUCCESSOR_EXECUTION_PROXY_ONCE"
PLAN_PATH = Path("configs/alpha_ladder_macro_successor_execution_proxy_plan_v1.json")
CATALOG_PATH = Path("data/active/catalog.json")
ECONOMICS_PATH = Path("configs/contract_economics_rules.json")
EVIDENCE_ROOT = Path("state/unpublished_evidence/alpha_ladder_macro_successor_execution_proxy")
SELECTION_ROOT = Path("state/alpha_ladder_macro_selection_successor")
CLOSURE_ROOT = Path("state/alpha_ladder_macro_successor_terminal_closure")
ACTIVE_MACRO_PATH = Path("configs/active_alpha_ladder_macro_market.json")
YEARS = tuple(range(2018, 2023))
SCHEDULE_SHA256 = "d30c7da4f38c2098fa1352335c5612fe1d4d583bef51036056c395314954fd63"


def _read(path: Path) -> dict[str, object]:
    raw = io_path(path).read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"selector input is not canonical: {path}")
    return value


def _create(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(io_path(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
    except FileExistsError as exc:
        raise UnauthorizedOperation(f"create-only selector output exists: {path}") from exc
    try:
        os.write(descriptor, canonical_bytes(value) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _binding(root: Path, path: Path) -> dict[str, str]:
    return {"path": path.as_posix(), "sha256": sha256_file(root / path, reject_hardlinks=False)}


def _point_rules(root: Path) -> dict[str, dict[str, Decimal]]:
    rules = _read(root / ECONOMICS_PATH)
    selected = {
        str(item["market"]): {
            "point_value": Decimal(str(item["point_value"])),
            "expected_unit_qty": Decimal(str(item["expected_unit_qty"])),
        }
        for item in rules.get("rules", ())  # type: ignore[union-attr]
        if isinstance(item, Mapping) and item.get("market") in NEW_CANDIDATES
    }
    if set(selected) != set(NEW_CANDIDATES):
        raise IntegrityError("new-candidate economics rules are incomplete")
    return selected


def _plan_core(root: Path) -> dict[str, object]:
    identities = build_identities(root)
    for name, path in (
        ("ladder", LADDER_POINTER_PATH), ("execution", EXECUTION_POINTER_PATH),
        ("rank1", RANK1_POINTER_PATH),
    ):
        if _read(root / path) != identities[name]:
            raise IntegrityError(f"active successor identity drifted: {name}")
    freeze = load_successor_contract(root)
    catalog_contract = _read(root / CATALOG_CONTRACT_PATH)
    catalog = _read(root / CATALOG_PATH)
    validate_catalog(catalog)
    selected_entries = [
        item for item in catalog["entries"]  # type: ignore[index]
        if item["market"] in NEW_CANDIDATES and item["year"] in YEARS
    ]
    if len(selected_entries) != 15 or any(
        item["cohort"] != "DISCOVERY_SELECTION"
        or item["selection_eligible"] is not True
        or "SELECTION" not in item["permitted_uses"]
        for item in selected_entries
    ):
        raise UnauthorizedOperation("catalog does not authorize exactly SR3/ZB/6J 2018-2022")
    certificates = {}
    files = []
    candidate_limits = {}
    for binding in catalog_contract.get("certificates", ()):
        if not isinstance(binding, Mapping) or binding.get("market") not in NEW_CANDIDATES:
            continue
        market = str(binding["market"])
        path = Path(str(binding["path"]))
        if sha256_file(root / path) != binding.get("sha256"):
            raise IntegrityError("selector certificate binding drifted")
        certificate = _read(root / path)
        inventory = certificate["replay_evidence"]["output_inventory"]  # type: ignore[index]
        selected = [
            dict(item) for item in inventory  # type: ignore[union-attr]
            if isinstance(item, Mapping)
            and PurePosixPath(str(item.get("path", ""))).stem in TABLES
            and any(f"/sealed/{year}/" in str(item.get("path", "")) for year in YEARS)
        ]
        grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
        for item in selected:
            grouped[PurePosixPath(str(item["path"])).parent.as_posix()].append(item)
        if not grouped or any(
            {PurePosixPath(str(item["path"])).stem for item in items} != set(TABLES)
            for items in grouped.values()
        ):
            raise IntegrityError("certificate selector inventory has an incomplete monthly bundle")
        observation_rows = sum(
            int(item["row_count"])
            for item in selected
            if PurePosixPath(str(item["path"])).stem == "observations"
        )
        catalog_rows = sum(
            int(item["row_count"])
            for item in selected_entries
            if item["market"] == market
        )
        if observation_rows != catalog_rows:
            raise IntegrityError("certificate selector rows differ from the active catalog")
        files.extend(selected)
        candidate_limits[market] = {
            "bytes": sum(int(item["size"]) for item in selected),
            "files": len(selected),
            "monthly_bundles": len(grouped),
            "observation_rows": observation_rows,
        }
        certificates[market] = {
            "attempt_id": certificate["attempt_id"], "certificate_id": certificate["certificate_id"],
            **_binding(root, path),
        }
    if tuple(certificates) != NEW_CANDIDATES:
        raise IntegrityError("selector requires ordered SR3, ZB, and 6J certificates")
    _point_rules(root)
    dates = _shared_dates()
    active_calendar = freeze["measurement_schedule"]["active_41_market_calendar"]  # type: ignore[index]
    active_calendar_path = Path(str(active_calendar["path"]))
    if sha256_file(root / active_calendar_path) != active_calendar["sha256"]:
        raise IntegrityError("active 41-market calendar binding drifted")
    bindings = {
        "candidate_certificates": certificates,
        "catalog": {**_binding(root, CATALOG_PATH), "catalog_sha256": catalog["catalog_sha256"]},
        "catalog_contract": {**_binding(root, CATALOG_CONTRACT_PATH), "contract_id": catalog_contract["contract_id"]},
        "calendars": [
            _binding(root, HISTORICAL_CALENDAR_PATH), _binding(root, CORRECTED_CALENDAR_PATH),
            _binding(root, active_calendar_path),
        ],
        "economics": _binding(root, ECONOMICS_PATH),
        "execution_design": {**_binding(root, EXECUTION_POINTER_PATH), "pointer_id": identities["execution"]["pointer_id"]},
        "files": sorted(files, key=lambda item: str(item["path"])),
        "ladder": {**_binding(root, LADDER_POINTER_PATH), "pointer_id": identities["ladder"]["pointer_id"]},
        "rank1": {**_binding(root, RANK1_POINTER_PATH), "pointer_id": identities["rank1"]["pointer_id"]},
        "selector_freeze": _binding(root, Path("configs/alpha_ladder_macro_successor_contract_v1.json")),
        "predecessor_closure": _binding(root, Path("state/alpha_ladder_macro_selection_closure/zn_6e_terminal_v1.json")),
        "implementation": {
            "runner": _binding(root, Path("src/futures_rebuild/alpha_ladder_macro_successor_execution.py")),
            "selector": _binding(root, Path("src/futures_rebuild/alpha_ladder_macro_successor.py")),
            "receipt_boundary": _binding(root, Path("src/futures_rebuild/boundary.py")),
            "gateway_policy": _binding(root, Path("src/futures_rebuild/research_gateway_policy.py")),
            "schedule_source": _binding(root, Path("src/futures_rebuild/alpha_ladder_zn_6e_execution_proxy.py")),
        },
    }
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_macro_successor_execution_proxy_plan/1.0.0",
        "state": "PREPARED_REQUIRES_EXACT_APPROVAL", "operation": OPERATION,
        "attempts": 1, "retries": 0, "markets": list(NEW_CANDIDATES), "years": list(YEARS),
        "schedule": {"dates_sha256": SCHEDULE_SHA256, "session_count": len(dates),
                     "start": dates[0], "end": dates[-1], "expected_minutes_per_session": 385},
        "bindings": bindings,
        "limits": {
            "bytes": sum(int(item["size"]) for item in files), "files": len(files),
            "by_market": candidate_limits,
            "market_year_partitions": len(selected_entries),
            "observation_rows": sum(int(item["row_count"]) for item in selected_entries),
        },
        "outputs": {"evidence_root": EVIDENCE_ROOT.as_posix(), "selection_root": SELECTION_ROOT.as_posix(),
                    "closure_root": CLOSURE_ROOT.as_posix(), "active_macro_pointer": ACTIVE_MACRO_PATH.as_posix(),
                    "create_only": True, "raw_rows_forbidden": True},
        "authority": {"authorization_receipt_issued": False, "market_row_read": False,
                      "macro_selection": False, "provider_or_network": False,
                      "final_252_or_forward": False, "research_results": False},
    }
    return core


def prepare_plan(repository_root: Path) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    core = _plan_core(root)
    plan = {**core, "plan_id": sha256_json(core)}
    if (root / PLAN_PATH).exists():
        if _read(root / PLAN_PATH) != plan:
            raise IntegrityError("successor selector plan drifted")
    else:
        _create(root / PLAN_PATH, plan)
    return plan


def load_plan(repository_root: Path) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    observed = _read(root / PLAN_PATH)
    core = _plan_core(root)
    expected = {**core, "plan_id": sha256_json(core)}
    if observed != expected:
        raise IntegrityError("successor selector plan or binding drifted")
    return observed


def required_scope(root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    bindings = plan["bindings"]  # type: ignore[index]
    return {
        "operation_kind": "PRE_RESULT_MACRO_SUCCESSOR_EXECUTION_PROXY_AND_SEAL",
        "markets": ",".join(NEW_CANDIDATES), "years": ",".join(map(str, YEARS)),
        "predecessor_closure_sha256": str(bindings["predecessor_closure"]["sha256"]),
        "candidate_order_sha256": sha256_json(list(load_successor_contract(root)["candidate_order"])),
        "carried_evidence_sha256": sha256_json(load_successor_contract(root)["carried_candidate_results"]),
        "rank1_pointer_id": str(bindings["rank1"]["pointer_id"]),
        "ladder_pointer_id": str(bindings["ladder"]["pointer_id"]),
        "execution_design_pointer_id": str(bindings["execution_design"]["pointer_id"]),
        "catalog_sha256": str(bindings["catalog"]["catalog_sha256"]),
        "certificates_sha256": sha256_json(bindings["candidate_certificates"]),
        "calendars_sha256": sha256_json(bindings["calendars"]),
        "schedule_sha256": SCHEDULE_SHA256,
        "limits_sha256": sha256_json(plan["limits"]),
        "implementation_sha256": sha256_json(bindings["implementation"]),
        "plan_id": str(plan["plan_id"]), "plan_sha256": sha256_file(root / PLAN_PATH),
        "approval_command": OPERATION, "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _unknown() -> dict[str, object]:
    return {field: None for field in EVIDENCE_FIELDS}


def aggregate_candidate(
    contract: Mapping[str, object], market: str, scheduled_dates: Sequence[str],
    bundles: Iterable[Mapping[str, Sequence[Mapping[str, object]]]],
    economics: Mapping[str, Decimal],
) -> dict[str, object]:
    """Construct only the seven frozen aggregate evidence fields."""

    if market not in NEW_CANDIDATES or not scheduled_dates:
        raise UnauthorizedOperation("selector aggregation is SR3/ZB/6J-only")
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
                date_text, start = str(row["project_trade_date"]), int(row["bar_start_ns"])
                local = datetime.fromtimestamp(start / 1_000_000_000, timezone.utc).astimezone(CHICAGO)
                if date_text not in scheduled:
                    continue
                roll_row = roll[ordinal]
                if bool(roll_row.get("roll_flag")):
                    roll_dates.add(date_text)
                if not time(8, 35) <= local.time().replace(tzinfo=None) <= time(14, 59):
                    continue
                key = date_text, start
                if key in rows:
                    duplicates.add(key)
                    continue
                available, q = int(row["available_at_ns"]), quality[ordinal]
                missing_row = missing.get(ordinal, {})
                missing_ok = (
                    missing_row.get("state") == "OBSERVED_VALID" and missing_row.get("market") == market
                    and int(missing_row.get("interval_start_ns", -1)) == start
                    and int(missing_row.get("interval_end_ns", -1)) == int(row["bar_end_ns"])
                )
                quality_ok = (
                    q.get("ohlc_valid") is True and q.get("volume_valid") is True
                    and q.get("timestamp_order_valid") is True and q.get("duplicate_state") == "UNIQUE"
                )
                causal = (
                    row.get("market") == market and row.get("project_timezone") == "America/Chicago"
                    and int(row["bar_end_ns"]) == start + 60_000_000_000
                    and available <= start + 65_000_000_000
                    and int(row["decision_eligible_at_ns"]) <= start + 65_000_000_000
                )
                metadata_ok = _valid_metadata(row, available, start)
                identities[date_text].add(str(row["actual_contract"]))
                rows[key] = row, bool(missing_ok and quality_ok and causal), metadata_ok, quality_ok
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
        minute_rows = {}
        for minute in range(8 * 60 + 35, 15 * 60):
            local = midnight.replace(hour=minute // 60, minute=minute % 60)
            start = int(local.astimezone(timezone.utc).timestamp() * 1_000_000_000)
            item = rows.get((date_text, start))
            if item:
                minute_rows[minute] = item
                metadata_complete += int(item[2])
                complete += int(item[1] and item[2])
                quality_errors += int(not item[3])
                if item[1] and item[2]:
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
            if Decimal(int(entry["multiplier_nano"])) / Decimal(10) ** 9 != economics["expected_unit_qty"]:
                return _unknown()
            tick_values.add(tick / Decimal(10) ** 9 * economics["point_value"])
    if not movements or len(tick_values) != 1 or expected == 0:
        return _unknown()
    movement, tick_value = median(movements), next(iter(tick_values))
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


def _bundles(root: Path, plan: Mapping[str, object], market: str) -> Iterable[dict[str, object]]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    files = [item for item in plan["bindings"]["files"] if f"/{market}/" in item["path"]]  # type: ignore[index]
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for item in files:
        grouped[PurePosixPath(str(item["path"])).parent.as_posix()].append(item)
    market_limits = plan["limits"]["by_market"][market]  # type: ignore[index]
    if (
        len(files) != market_limits["files"]
        or len(grouped) != market_limits["monthly_bundles"]
        or any({PurePosixPath(str(item["path"])).stem for item in items} != set(TABLES) for items in grouped.values())
    ):
        raise IntegrityError("selector file inventory is incomplete")
    rows = total_bytes = file_count = 0
    for directory in sorted(grouped):
        bundle = {}
        for item in sorted(grouped[directory], key=lambda value: str(value["path"])):
            file_count += 1
            total_bytes += int(item["size"])
            if file_count > market_limits["files"] or total_bytes > market_limits["bytes"]:
                raise IntegrityError("selector file limit breached")
            data = io_path(root / PurePosixPath(str(item["path"]))).read_bytes()
            if len(data) != item["size"] or sha256_bytes(data) != item["sha256"]:
                raise IntegrityError("bound selector input drifted")
            table = pq.ParquetFile(pa.BufferReader(data)).read().to_pylist()
            bundle[PurePosixPath(str(item["path"])).stem] = table
            if PurePosixPath(str(item["path"])).stem == "observations":
                rows += len(table)
                if rows > market_limits["observation_rows"]:
                    raise IntegrityError("selector observation-row limit breached")
        if set(bundle) != set(TABLES):
            raise IntegrityError("selector monthly bundle is incomplete")
        yield bundle


def execute_once(repository_root: Path, authorization: OperationReceipt) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    plan = load_plan(root)
    run_id = sha256_json({"plan_id": plan["plan_id"], "receipt_id": authorization.receipt_id})
    run_root = root / EVIDENCE_ROOT / run_id
    if (root / ACTIVE_MACRO_PATH).exists() or run_root.exists():
        raise UnauthorizedOperation("macro selection or one-use output already exists")
    claim = authorization.consume(
        RepoBoundary(root), operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root, plan),
    )
    try:
        contract, dates, rules = load_successor_contract(root), _shared_dates(), _point_rules(root)
        aggregates = {
            market: aggregate_candidate(contract, market, dates, _bundles(root, plan, market), rules[market])
            for market in NEW_CANDIDATES
        }
        result = select_successor_macro(contract, aggregates)
    except Exception as exc:
        terminal_core = {
            "schema_version": "alpha_ladder_macro_successor_execution_proxy_terminal/1.0.0",
            "run_id": run_id, "outcome": UNKNOWN, "selected_market": None,
            "macro_pointer_created": False, "retry_allowed": False,
            "error_type": type(exc).__name__,
        }
        _create(run_root / "terminal.json", {**terminal_core, "terminal_id": sha256_json(terminal_core)})
        raise
    evidence_core = {
        "schema_version": "alpha_ladder_macro_successor_execution_proxy_evidence/1.0.0",
        "run_id": run_id, "plan_id": plan["plan_id"], "receipt_id": authorization.receipt_id,
        "authorization_use_sha256": sha256_file(claim), "schedule_sha256": SCHEDULE_SHA256,
        "aggregate_evidence": aggregates, "selector_result": result, "raw_rows_included": False,
    }
    evidence = {**evidence_core, "evidence_id": sha256_json(evidence_core)}
    _create(run_root / "evidence.json", evidence)
    selected = result["selected_market"]
    pointer_created = False
    if result["outcome"] == NO_ELIGIBLE:
        closure_core = {
            "schema_version": "alpha_ladder_macro_successor_terminal_closure/1.0.0",
            "run_id": run_id, "outcome": NO_ELIGIBLE, "selected_market": None,
            "evidence_sha256": sha256_file(run_root / "evidence.json"), "retry_allowed": False,
        }
        closure = {**closure_core, "closure_id": sha256_json(closure_core)}
        _create(root / CLOSURE_ROOT / str(closure["closure_id"]) / "closure.json", closure)
    elif selected is not None:
        record_core = {
            "schema_version": "alpha_ladder_macro_selection_successor/1.0.0",
            "market": selected, "run_id": run_id,
            "evidence_sha256": sha256_file(run_root / "evidence.json"),
            "rank1_pointer_id": plan["bindings"]["rank1"]["pointer_id"],  # type: ignore[index]
        }
        record = {**record_core, "selection_id": sha256_json(record_core)}
        record_path = root / SELECTION_ROOT / str(record["selection_id"]) / "selection.json"
        _create(record_path, record)
        pointer_core = {
            "schema_version": "active_alpha_ladder_macro_market/2.0.0",
            "state": "ACTIVE_SEALED_MACRO_SELECTION", "market": selected,
            "selection_id": record["selection_id"], "selection_path": record_path.relative_to(root).as_posix(),
            "selection_sha256": sha256_file(record_path),
        }
        _create(root / ACTIVE_MACRO_PATH, {**pointer_core, "pointer_id": sha256_json(pointer_core)})
        pointer_created = True
    terminal_core = {
        "schema_version": "alpha_ladder_macro_successor_execution_proxy_terminal/1.0.0",
        "run_id": run_id, "outcome": result["outcome"], "selected_market": selected,
        "macro_pointer_created": pointer_created, "retry_allowed": False,
    }
    terminal = {**terminal_core, "terminal_id": sha256_json(terminal_core)}
    _create(run_root / "terminal.json", terminal)
    return terminal
