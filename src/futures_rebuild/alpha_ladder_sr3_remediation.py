"""Preparation and gated SR3-only remediation for the consumed macro selector."""

from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, time, timezone
from decimal import Decimal
from pathlib import Path, PurePosixPath
from statistics import median
from typing import Iterable, Mapping, Sequence

from .alpha_ladder_macro_successor import (
    EVIDENCE_FIELDS, NO_ELIGIBLE, UNKNOWN, load_successor_contract,
    select_successor_macro,
)
from .alpha_ladder_macro_successor_execution import (
    ACTIVE_MACRO_PATH, CLOSURE_ROOT, ECONOMICS_PATH, EVIDENCE_ROOT,
    PLAN_PATH as CONSUMED_PLAN_PATH, SELECTION_ROOT, _bundles, _create,
    _point_rules,
)
from .alpha_ladder_zn_6e_execution_proxy import CHICAGO, _ratio, _shared_dates, _valid_metadata
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, io_path, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


CONSUMED_RUN_ID = "7c731a1f496b7cdeb3ebf0238d5f60249ecd40aaf79ce0ef950d1ba0ea4dd99f"
CONSUMED_ROOT = EVIDENCE_ROOT / CONSUMED_RUN_ID
HISTORY_ROOT = Path("state/alpha_ladder_macro_successor_unknown_closure")
POLICY_PATH = Path("configs/alpha_ladder_sr3_remediation_policy_v1.json")
DIAGNOSTIC_PLAN_PATH = Path("configs/alpha_ladder_sr3_diagnostic_plan_v1.json")
DIAGNOSTIC_ROOT = Path("state/unpublished_evidence/alpha_ladder_sr3_unknown_diagnostic")
REMEDIATION_PLAN_PATH = Path("configs/alpha_ladder_sr3_remediation_execution_plan_v1.json")
REMEDIATION_ROOT = Path("state/unpublished_evidence/alpha_ladder_sr3_remediation")
SYNTHETIC_CERTIFICATE_PATH = Path("state/alpha_ladder_sr3_remediation/synthetic_certificate.json")
DIAGNOSTIC_OPERATION = "DIAGNOSE_ALPHA_LADDER_SR3_UNKNOWN_ONCE"
REMEDIATION_OPERATION = "RUN_ALPHA_LADDER_SR3_REMEDIATION_ONCE"
YEARS = tuple(range(2018, 2023))
CAUSE_PRIORITY = (
    "MALFORMED_OR_DUPLICATE_INPUT",
    "PAIR_TICK_CHANGED",
    "UNIT_QUANTITY_MISMATCH",
    "NO_USABLE_HORIZON_PAIRS",
    "MULTIPLE_POINT_IN_TIME_TICK_VALUES",
    "ZERO_EXPECTED_SESSIONS",
    "UNCLASSIFIED_FAIL_CLOSED",
)
REMEDIABLE_CAUSE = "MULTIPLE_POINT_IN_TIME_TICK_VALUES"


def _read(path: Path) -> dict[str, object]:
    raw = io_path(path).read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"SR3 remediation input is not canonical: {path}")
    return value


def _write_new_or_exact(path: Path, value: Mapping[str, object]) -> None:
    rendered = canonical_bytes(value) + b"\n"
    if path.exists():
        if io_path(path).read_bytes() != rendered:
            raise IntegrityError(f"SR3 remediation artifact drifted: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(io_path(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
    try:
        os.write(descriptor, rendered)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _self_hashed(core: Mapping[str, object], field: str) -> dict[str, object]:
    return {**core, field: sha256_json(dict(core))}


def _binding(root: Path, path: Path) -> dict[str, object]:
    return {
        "path": path.as_posix(),
        "sha256": sha256_file(root / path, reject_hardlinks=False),
        "size": io_path(root / path).stat().st_size,
    }


def build_consumed_closure(root: Path) -> dict[str, object]:
    evidence = _read(root / CONSUMED_ROOT / "evidence.json")
    terminal = _read(root / CONSUMED_ROOT / "terminal.json")
    plan = _read(root / CONSUMED_PLAN_PATH)
    claim_path = Path("state/authorization_uses") / f"{evidence['receipt_id']}.json"
    if (
        evidence.get("run_id") != CONSUMED_RUN_ID
        or evidence.get("plan_id") != plan.get("plan_id")
        or evidence.get("selector_result", {}).get("outcome") != UNKNOWN  # type: ignore[union-attr]
        or terminal.get("outcome") != UNKNOWN
        or terminal.get("retry_allowed") is not False
        or terminal.get("macro_pointer_created") is not False
        or terminal.get("selected_market") is not None
        or sha256_file(root / claim_path) != evidence.get("authorization_use_sha256")
        or evidence.get("evidence_id") != sha256_json({key: value for key, value in evidence.items() if key != "evidence_id"})
        or terminal.get("terminal_id") != sha256_json({key: value for key, value in terminal.items() if key != "terminal_id"})
        or sha256_file(root / Path("src/futures_rebuild/alpha_ladder_macro_successor_execution.py"))
        != plan["bindings"]["implementation"]["runner"]["sha256"]  # type: ignore[index]
        or sha256_file(root / Path("src/futures_rebuild/alpha_ladder_macro_successor.py"))
        != plan["bindings"]["implementation"]["selector"]["sha256"]  # type: ignore[index]
    ):
        raise IntegrityError("consumed macro successor history is incompatible")
    paths = (
        CONSUMED_PLAN_PATH,
        CONSUMED_ROOT / "evidence.json",
        CONSUMED_ROOT / "terminal.json",
        claim_path,
        Path("src/futures_rebuild/alpha_ladder_macro_successor_execution.py"),
        Path("src/futures_rebuild/alpha_ladder_macro_successor.py"),
        Path("configs/causal_nine_market_successor_contract_v1.json"),
        Path("reports/causal_nine_market_active_release/0e75293106d747e82048341a8ae054537e0abb856db905cddf021cd998652d1a/terminal.json"),
    )
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_macro_successor_unknown_closure/1.0.0",
        "state": "POST_RESULT_HISTORY_CLOSED_UNKNOWN_FAIL_CLOSED",
        "classification": "POST_RESULT_HISTORICAL_CLOSURE_NOT_PRE_RESULT_FREEZE",
        "run_id": CONSUMED_RUN_ID,
        "attempt_consumed": True,
        "outcome": UNKNOWN,
        "selected_market": None,
        "macro_pointer_created": False,
        "retry_allowed": False,
        "artifacts_were_untracked_at_closure_creation": [
            CONSUMED_PLAN_PATH.as_posix(),
            (CONSUMED_ROOT / "evidence.json").as_posix(),
            (CONSUMED_ROOT / "terminal.json").as_posix(),
        ],
        "artifact_bindings": [_binding(root, path) for path in paths],
        "authority": {
            "diagnostic_row_read": False,
            "macro_selection": False,
            "provider_or_network": False,
            "remediation_row_read": False,
            "research_or_phase3_8": False,
            "trading": False,
        },
    }
    return _self_hashed(core, "closure_id")


def build_policy(root: Path, closure: Mapping[str, object]) -> dict[str, object]:
    contract = load_successor_contract(root)
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_sr3_remediation_policy/1.0.0",
        "state": "FROZEN_PRE_DIAGNOSTIC_PREPARATION_ONLY",
        "market": "SR3",
        "years": list(YEARS),
        "consumed_closure": {
            "closure_id": closure["closure_id"],
            "path": (HISTORY_ROOT / str(closure["closure_id"]) / "closure.json").as_posix(),
        },
        "diagnostic_causes": list(CAUSE_PRIORITY),
        "remediable_causes": [REMEDIABLE_CAUSE],
        "correction": {
            "adverse_ticks_round_trip": contract["cost_model"]["adverse_ticks_round_trip"]["SR3"],  # type: ignore[index]
            "fee_round_trip_usd": contract["cost_model"]["fee_round_trip_usd"],  # type: ignore[index]
            "pair_friction_formula": "adverse_ticks_round_trip + fee_round_trip_usd / entry_point_in_time_tick_value_usd",
            "aggregate_friction": "MAXIMUM_VALID_PAIR_FRICTION",
            "aggregate_movement": "MEDIAN_VALID_30_MINUTE_MOVEMENT_TICKS",
            "movement_to_cost_ratio": "aggregate_movement / aggregate_friction",
            "reported_tick_value": "MINIMUM_VALID_ENTRY_POINT_IN_TIME_TICK_VALUE_USD",
        },
        "unchanged": {
            "candidate_order": contract["candidate_order"],
            "cost_model": contract["cost_model"],
            "execution_proxy_gates": contract["execution_proxy_gates"],
            "measurement_schedule": contract["measurement_schedule"],
            "selection_rule": contract["selection_rule"],
            "carried_results": ["ZN", "6E", "ZB", "6J"],
        },
        "nonremediable_action": UNKNOWN,
        "authority": {
            "diagnostic_row_read": False,
            "macro_selection": False,
            "provider_or_network": False,
            "remediation_row_read": False,
            "research_or_phase3_8": False,
            "trading": False,
        },
    }
    return _self_hashed(core, "policy_id")


def _scan(
    scheduled_dates: Sequence[str],
    bundles: Iterable[Mapping[str, Sequence[Mapping[str, object]]]],
    economics: Mapping[str, Decimal],
) -> dict[str, object]:
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
            malformed = True
            continue
        if len(quality) != len(observations) or len(roll) != len(observations):
            malformed = True
            continue
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
                    missing_row.get("state") == "OBSERVED_VALID"
                    and missing_row.get("market") == "SR3"
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
                    row.get("market") == "SR3"
                    and row.get("project_timezone") == "America/Chicago"
                    and int(row["bar_end_ns"]) == start + 60_000_000_000
                    and available <= start + 65_000_000_000
                    and int(row["decision_eligible_at_ns"]) <= start + 65_000_000_000
                )
                rows[key] = row, bool(missing_ok and quality_ok and causal), _valid_metadata(row, available, start), quality_ok
                identities[date_text].add(str(row["actual_contract"]))
            except (KeyError, TypeError, ValueError, OSError, OverflowError):
                malformed = True
    causes: set[str] = set()
    if malformed or duplicates:
        causes.add("MALFORMED_OR_DUPLICATE_INPUT")
    nonroll = [date for date in scheduled_dates if date not in roll_dates]
    expected = len(nonroll) * 385
    if expected == 0:
        causes.add("ZERO_EXPECTED_SESSIONS")
    complete = metadata_complete = zero_volume = quality_errors = 0
    movements: list[Decimal] = []
    frictions: list[Decimal] = []
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
                causes.add("PAIR_TICK_CHANGED")
                continue
            if Decimal(int(entry["multiplier_nano"])) / Decimal(10) ** 9 != economics["expected_unit_qty"]:
                causes.add("UNIT_QUANTITY_MISMATCH")
                continue
            tick_value = tick / Decimal(10) ** 9 * economics["point_value"]
            if tick_value <= 0:
                causes.add("MALFORMED_OR_DUPLICATE_INPUT")
                continue
            movements.append(abs(Decimal(int(exit_["open_nano"])) - Decimal(int(entry["open_nano"]))) / tick)
            tick_values.add(tick_value)
            frictions.append(Decimal("4") + Decimal("10") / tick_value)
    if not movements:
        causes.add("NO_USABLE_HORIZON_PAIRS")
    if len(tick_values) > 1:
        causes.add(REMEDIABLE_CAUSE)
    if not causes and (not frictions or expected == 0):
        causes.add("UNCLASSIFIED_FAIL_CLOSED")
    primary = next((cause for cause in CAUSE_PRIORITY if cause in causes), None)
    return {
        "causes": tuple(cause for cause in CAUSE_PRIORITY if cause in causes),
        "primary_cause": primary,
        "expected": expected,
        "complete": complete,
        "metadata_complete": metadata_complete,
        "zero_volume": zero_volume,
        "quality_errors": quality_errors,
        "movements": movements,
        "frictions": frictions,
        "tick_values": tick_values,
        "nonroll_sessions": len(nonroll),
        "roll_dates": len(roll_dates),
        "identity_violations": identity_violations,
    }


def diagnose_sr3(
    scheduled_dates: Sequence[str],
    bundles: Iterable[Mapping[str, Sequence[Mapping[str, object]]]],
    economics: Mapping[str, Decimal],
) -> dict[str, object]:
    scan = _scan(scheduled_dates, bundles, economics)
    return {
        "schema_version": "alpha_ladder_sr3_unknown_diagnostic_evidence/1.0.0",
        "market": "SR3",
        "primary_cause": scan["primary_cause"],
        "causes": list(scan["causes"]),
        "structural_counts": {
            "distinct_point_in_time_tick_values": len(scan["tick_values"]),  # type: ignore[arg-type]
            "expected_sessions": scan["nonroll_sessions"],
            "roll_dates_excluded": scan["roll_dates"],
            "valid_horizon_pairs": len(scan["movements"]),  # type: ignore[arg-type]
        },
        "remediation_allowed": scan["causes"] == (REMEDIABLE_CAUSE,),
        "raw_rows_included": False,
        "result_metrics_included": False,
    }


def aggregate_sr3_conservative(
    contract: Mapping[str, object],
    scheduled_dates: Sequence[str],
    bundles: Iterable[Mapping[str, Sequence[Mapping[str, object]]]],
    economics: Mapping[str, Decimal],
) -> dict[str, object]:
    cost_model = contract.get("cost_model")
    if (
        not isinstance(cost_model, Mapping)
        or cost_model.get("fee_round_trip_usd") != "10.00"
        or not isinstance(cost_model.get("adverse_ticks_round_trip"), Mapping)
        or cost_model["adverse_ticks_round_trip"].get("SR3") != 4  # type: ignore[union-attr]
    ):
        raise IntegrityError("frozen SR3 cost model drifted")
    scan = _scan(scheduled_dates, bundles, economics)
    if any(cause != REMEDIABLE_CAUSE for cause in scan["causes"]):  # type: ignore[union-attr]
        return {field: None for field in EVIDENCE_FIELDS}
    movements = scan["movements"]
    frictions = scan["frictions"]
    tick_values = scan["tick_values"]
    expected = int(scan["expected"])
    if not movements or not frictions or not tick_values or expected == 0:  # type: ignore[truthy-bool]
        return {field: None for field in EVIDENCE_FIELDS}
    movement = median(movements)  # type: ignore[arg-type]
    friction = max(frictions)  # type: ignore[arg-type]
    minimum_tick = min(tick_values)  # type: ignore[arg-type]
    complete = int(scan["complete"])
    return {
        "target_horizon_movement_ticks": str(movement),
        "conservative_round_trip_friction_ticks": {
            "value": str(friction),
            "tick_value_usd": str(minimum_tick),
        },
        "movement_to_cost_ratio": str(movement / friction),
        "active_minute_coverage": {
            "usable_sessions": scan["nonroll_sessions"],
            "usable_observations": complete,
            "ratio": _ratio(complete, expected),
        },
        "zero_volume_minute_fraction": _ratio(int(scan["zero_volume"]), complete),
        "missingness_and_continuity": {
            "unexpected_gap_or_stale_fraction": _ratio(expected - complete, expected),
            "point_in_time_metadata_coverage_ratio": _ratio(int(scan["metadata_complete"]), expected),
            "quality_error_count": scan["quality_errors"],
        },
        "roll_behavior": {
            "excluded_roll_dates": scan["roll_dates"],
            "identity_violation_count": scan["identity_violations"],
        },
    }


def _synthetic_bundle(date_text: str, tick: int) -> dict[str, list[dict[str, object]]]:
    midnight = datetime.combine(datetime.fromisoformat(date_text).date(), time(), CHICAGO)
    observations: list[dict[str, object]] = []
    missingness: list[dict[str, object]] = []
    quality: list[dict[str, object]] = []
    roll: list[dict[str, object]] = []
    for ordinal, minute in enumerate(range(8 * 60 + 35, 15 * 60)):
        local = midnight.replace(hour=minute // 60, minute=minute % 60)
        start = int(local.timestamp() * 1_000_000_000)
        end = start + 60_000_000_000
        observations.append({
            "actual_contract": "SR3Z20", "available_at_ns": end,
            "bar_end_ns": end, "bar_start_ns": start, "currency": "USD",
            "decision_eligible_at_ns": end, "definition_event_at_ns": start,
            "definition_received_at_ns": start, "expiration_ns": 2**64 - 1,
            "instrument_id": 1, "listing_activation_ns": 0, "market": "SR3",
            "min_price_increment_nano": tick, "multiplier_nano": 2_500_000_000_000,
            "open_nano": 95_000_000_000 + ordinal * tick,
            "project_timezone": "America/Chicago", "project_trade_date": date_text,
            "publisher_id": 1, "raw_symbol": "SR3Z20", "volume": 1,
        })
        missingness.append({
            "observation_ordinal": ordinal, "state": "OBSERVED_VALID", "market": "SR3",
            "interval_start_ns": start, "interval_end_ns": end,
        })
        quality.append({
            "observation_ordinal": ordinal, "ohlc_valid": True, "volume_valid": True,
            "timestamp_order_valid": True, "duplicate_state": "UNIQUE",
        })
        roll.append({"observation_ordinal": ordinal, "roll_flag": False})
    return {"observations": observations, "missingness": missingness, "quality": quality, "roll": roll}


def build_synthetic_certificate(root: Path, policy: Mapping[str, object]) -> dict[str, object]:
    contract = load_successor_contract(root)
    economics = {"point_value": Decimal("2500"), "expected_unit_qty": Decimal("2500")}
    dates = ("2020-01-02", "2020-01-03")
    bundles = (_synthetic_bundle(dates[0], 2_500_000), _synthetic_bundle(dates[1], 5_000_000))
    diagnostic = diagnose_sr3(dates, bundles, economics)
    result = aggregate_sr3_conservative(contract, dates, bundles, economics)
    checks = {
        "multi_tick_diagnosed": diagnostic["causes"] == [REMEDIABLE_CAUSE],
        "only_multi_tick_remediable": diagnostic["remediation_allowed"] is True,
        "maximum_friction_used": result["conservative_round_trip_friction_ticks"] == {
            "value": "5.6", "tick_value_usd": "6.2500",
        },
        "median_movement_preserved": result["target_horizon_movement_ticks"] == "30",
        "ratio_uses_maximum_friction": result["movement_to_cost_ratio"] == str(Decimal("30") / Decimal("5.6")),
    }
    if not all(checks.values()):
        raise IntegrityError("SR3 remediation synthetic mechanics failed")
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_sr3_remediation_synthetic_certificate/1.0.0",
        "state": "PASS_SYNTHETIC_MECHANICS_ONLY",
        "market": "SR3",
        "policy_id": policy["policy_id"],
        "policy_sha256": sha256_json(dict(policy)),
        "implementation_sha256": sha256_file(root / Path("src/futures_rebuild/alpha_ladder_sr3_remediation.py")),
        "checks": checks,
        "real_rows_read": False,
        "macro_selection_authorized": False,
        "limitations": ["SYNTHETIC_ONLY", "NO_REAL_ROW_OR_RESEARCH_AUTHORITY"],
    }
    return _self_hashed(core, "certificate_id")


def build_diagnostic_plan(
    root: Path, closure: Mapping[str, object], policy: Mapping[str, object], certificate: Mapping[str, object]
) -> dict[str, object]:
    consumed = _read(root / CONSUMED_PLAN_PATH)
    files = [dict(item) for item in consumed["bindings"]["files"] if "/SR3/" in item["path"]]  # type: ignore[index]
    market_limits = dict(consumed["limits"]["by_market"]["SR3"])  # type: ignore[index]
    if len(files) != market_limits["files"]:
        raise IntegrityError("consumed SR3 inventory differs from its frozen limit")
    limits = {
        "bytes": market_limits["bytes"],
        "files": market_limits["files"],
        "market_year_partitions": 5,
        "observation_rows": market_limits["observation_rows"],
        "by_market": {"SR3": market_limits},
    }
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_sr3_unknown_diagnostic_plan/1.0.0",
        "state": "PREPARED_REQUIRES_EXACT_APPROVAL",
        "operation": DIAGNOSTIC_OPERATION,
        "attempts": 1,
        "retries": 0,
        "market": "SR3",
        "years": list(YEARS),
        "schedule": consumed["schedule"],
        "bindings": {
            "consumed_closure": {
                **_binding(root, HISTORY_ROOT / str(closure["closure_id"]) / "closure.json"),
                "closure_id": closure["closure_id"],
            },
            "consumed_plan": {**_binding(root, CONSUMED_PLAN_PATH), "plan_id": consumed["plan_id"]},
            "consumed_evidence": _binding(root, CONSUMED_ROOT / "evidence.json"),
            "policy": {**_binding(root, POLICY_PATH), "policy_id": policy["policy_id"]},
            "synthetic_certificate": {
                **_binding(root, SYNTHETIC_CERTIFICATE_PATH),
                "certificate_id": certificate["certificate_id"],
            },
            "certificate": consumed["bindings"]["candidate_certificates"]["SR3"],  # type: ignore[index]
            "catalog": consumed["bindings"]["catalog"],  # type: ignore[index]
            "calendars": consumed["bindings"]["calendars"],  # type: ignore[index]
            "economics": consumed["bindings"]["economics"],  # type: ignore[index]
            "execution_design": consumed["bindings"]["execution_design"],  # type: ignore[index]
            "files": files,
            "ladder": consumed["bindings"]["ladder"],  # type: ignore[index]
            "predecessor_closure": consumed["bindings"]["predecessor_closure"],  # type: ignore[index]
            "rank1": consumed["bindings"]["rank1"],  # type: ignore[index]
            "selector_freeze": consumed["bindings"]["selector_freeze"],  # type: ignore[index]
            "implementation": {
                "diagnostic": _binding(root, Path("src/futures_rebuild/alpha_ladder_sr3_remediation.py")),
                "receipt_boundary": consumed["bindings"]["implementation"]["receipt_boundary"],  # type: ignore[index]
                "gateway_policy": _binding(root, Path("src/futures_rebuild/research_gateway_policy.py")),
            },
        },
        "limits": limits,
        "outputs": {
            "root": DIAGNOSTIC_ROOT.as_posix(),
            "create_only": True,
            "raw_rows_forbidden": True,
            "result_metrics_forbidden": True,
        },
        "authority": {
            "authorization_receipt_issued": False,
            "market_row_read": False,
            "macro_selection": False,
            "provider_or_network": False,
            "research_results": False,
        },
    }
    return _self_hashed(core, "plan_id")


def prepare_package(repository_root: Path) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    closure = build_consumed_closure(root)
    closure_path = HISTORY_ROOT / str(closure["closure_id"]) / "closure.json"
    _write_new_or_exact(root / closure_path, closure)
    policy = build_policy(root, closure)
    _write_new_or_exact(root / POLICY_PATH, policy)
    certificate = build_synthetic_certificate(root, policy)
    _write_new_or_exact(root / SYNTHETIC_CERTIFICATE_PATH, certificate)
    plan = build_diagnostic_plan(root, closure, policy, certificate)
    _write_new_or_exact(root / DIAGNOSTIC_PLAN_PATH, plan)
    return {
        "closure_path": closure_path.as_posix(), "closure_id": closure["closure_id"],
        "policy_id": policy["policy_id"], "certificate_id": certificate["certificate_id"],
        "diagnostic_plan_id": plan["plan_id"], "state": "PREPARED_NO_REAL_ROWS_READ",
    }


def _load_diagnostic_plan(root: Path) -> dict[str, object]:
    plan = _read(root / DIAGNOSTIC_PLAN_PATH)
    for binding in (
        plan["bindings"]["consumed_closure"], plan["bindings"]["consumed_plan"],  # type: ignore[index]
        plan["bindings"]["consumed_evidence"], plan["bindings"]["policy"],  # type: ignore[index]
        plan["bindings"]["synthetic_certificate"], plan["bindings"]["certificate"],  # type: ignore[index]
        plan["bindings"]["catalog"], plan["bindings"]["economics"],  # type: ignore[index]
        plan["bindings"]["execution_design"], plan["bindings"]["ladder"],  # type: ignore[index]
        plan["bindings"]["predecessor_closure"], plan["bindings"]["rank1"],  # type: ignore[index]
        plan["bindings"]["selector_freeze"],  # type: ignore[index]
        *plan["bindings"]["calendars"], *plan["bindings"]["implementation"].values(),  # type: ignore[index]
        *plan["bindings"]["files"],  # type: ignore[index]
    ):
        if sha256_file(root / PurePosixPath(str(binding["path"])), reject_hardlinks=False) != binding["sha256"]:
            raise IntegrityError("SR3 diagnostic binding drifted")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    if (
        plan.get("plan_id") != sha256_json(core)
        or plan.get("operation") != DIAGNOSTIC_OPERATION
        or plan.get("attempts") != 1
        or plan.get("retries") != 0
        or plan.get("market") != "SR3"
        or plan.get("years") != list(YEARS)
        or plan.get("outputs") != {
            "root": DIAGNOSTIC_ROOT.as_posix(),
            "create_only": True,
            "raw_rows_forbidden": True,
            "result_metrics_forbidden": True,
        }
    ):
        raise IntegrityError("SR3 diagnostic plan identity drifted")
    return plan


def diagnostic_required_scope(root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    bindings = plan["bindings"]  # type: ignore[index]
    return {
        "operation_kind": "POST_RESULT_SR3_STRUCTURAL_DIAGNOSTIC_ONLY",
        "market": "SR3",
        "years": ",".join(map(str, YEARS)),
        "consumed_closure_id": str(bindings["consumed_closure"]["closure_id"]),
        "policy_id": str(bindings["policy"]["policy_id"]),
        "catalog_sha256": str(bindings["catalog"]["catalog_sha256"]),
        "limits_sha256": sha256_json(plan["limits"]),
        "implementation_sha256": sha256_json(bindings["implementation"]),
        "plan_id": str(plan["plan_id"]),
        "plan_sha256": sha256_file(root / DIAGNOSTIC_PLAN_PATH),
        "approval_command": DIAGNOSTIC_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / DIAGNOSTIC_PLAN_PATH),
    }


def execute_diagnostic_once(repository_root: Path, authorization: OperationReceipt) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    plan = _load_diagnostic_plan(root)
    run_id = sha256_json({"plan_id": plan["plan_id"], "receipt_id": authorization.receipt_id})
    run_root = root / DIAGNOSTIC_ROOT / run_id
    if run_root.exists():
        raise UnauthorizedOperation("SR3 diagnostic one-use output already exists")
    claim = authorization.consume(
        RepoBoundary(root), operation=DIAGNOSTIC_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=diagnostic_required_scope(root, plan),
    )
    diagnostic = diagnose_sr3(_shared_dates(), _bundles(root, plan, "SR3"), _point_rules(root)["SR3"])
    evidence_core = {
        **diagnostic,
        "run_id": run_id,
        "plan_id": plan["plan_id"],
        "receipt_id": authorization.receipt_id,
        "authorization_use_sha256": sha256_file(claim),
    }
    evidence = _self_hashed(evidence_core, "evidence_id")
    _create(run_root / "diagnostic.json", evidence)
    terminal_core = {
        "schema_version": "alpha_ladder_sr3_unknown_diagnostic_terminal/1.0.0",
        "run_id": run_id,
        "outcome": "REMEDIABLE_CONFIRMED" if diagnostic["remediation_allowed"] else UNKNOWN,
        "primary_cause": diagnostic["primary_cause"],
        "remediation_allowed": diagnostic["remediation_allowed"],
        "retry_allowed": False,
    }
    terminal = _self_hashed(terminal_core, "terminal_id")
    _create(run_root / "terminal.json", terminal)
    return terminal


def build_remediation_plan(repository_root: Path, diagnostic_path: str) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    diagnostic_relative = PurePosixPath(diagnostic_path)
    if (
        diagnostic_relative.is_absolute()
        or ".." in diagnostic_relative.parts
        or diagnostic_relative.name != "diagnostic.json"
        or diagnostic_relative.parts[:len(DIAGNOSTIC_ROOT.parts)] != DIAGNOSTIC_ROOT.parts
    ):
        raise UnauthorizedOperation("SR3 diagnostic path escapes repository")
    diagnostic = _read(root / diagnostic_relative)
    diagnostic_core = {key: value for key, value in diagnostic.items() if key != "evidence_id"}
    if (
        diagnostic.get("evidence_id") != sha256_json(diagnostic_core)
        or diagnostic.get("causes") != [REMEDIABLE_CAUSE]
        or diagnostic.get("remediation_allowed") is not True
        or diagnostic.get("raw_rows_included") is not False
        or diagnostic.get("result_metrics_included") is not False
    ):
        raise UnauthorizedOperation("SR3 diagnostic does not authorize remediation preparation")
    plan = _load_diagnostic_plan(root)
    consumed_evidence = _read(root / CONSUMED_ROOT / "evidence.json")
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_sr3_remediation_execution_plan/1.0.0",
        "state": "PREPARED_REQUIRES_EXACT_APPROVAL",
        "operation": REMEDIATION_OPERATION,
        "attempts": 1,
        "retries": 0,
        "market": "SR3",
        "years": list(YEARS),
        "schedule": plan["schedule"],
        "bindings": {
            **plan["bindings"],  # type: ignore[arg-type]
            "diagnostic": {**_binding(root, Path(diagnostic_relative.as_posix())), "evidence_id": diagnostic["evidence_id"]},
            "carried_evidence": {
                **_binding(root, CONSUMED_ROOT / "evidence.json"),
                "evidence_id": consumed_evidence["evidence_id"],
                "markets": ["ZN", "6E", "ZB", "6J"],
            },
        },
        "limits": plan["limits"],
        "outputs": {
            "root": REMEDIATION_ROOT.as_posix(),
            "selection_root": SELECTION_ROOT.as_posix(),
            "closure_root": CLOSURE_ROOT.as_posix(),
            "active_macro_pointer": ACTIVE_MACRO_PATH.as_posix(),
            "create_only": True,
            "raw_rows_forbidden": True,
        },
        "authority": {
            "authorization_receipt_issued": False,
            "market_row_read": False,
            "macro_selection": False,
            "provider_or_network": False,
            "research_results": False,
        },
    }
    result = _self_hashed(core, "plan_id")
    _write_new_or_exact(root / REMEDIATION_PLAN_PATH, result)
    return result


def _load_remediation_plan(root: Path) -> dict[str, object]:
    plan = _read(root / REMEDIATION_PLAN_PATH)
    diagnostic = _read(root / PurePosixPath(str(plan["bindings"]["diagnostic"]["path"])))  # type: ignore[index]
    rebuilt = build_remediation_plan(root, str(plan["bindings"]["diagnostic"]["path"]))  # type: ignore[index]
    if plan != rebuilt or diagnostic.get("causes") != [REMEDIABLE_CAUSE]:
        raise IntegrityError("SR3 remediation plan drifted")
    return plan


def remediation_required_scope(root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    bindings = plan["bindings"]  # type: ignore[index]
    return {
        "operation_kind": "POST_DIAGNOSTIC_SR3_REMEDIATION_AND_SEAL",
        "market": "SR3",
        "years": ",".join(map(str, YEARS)),
        "consumed_closure_id": str(bindings["consumed_closure"]["closure_id"]),
        "policy_id": str(bindings["policy"]["policy_id"]),
        "diagnostic_evidence_id": str(bindings["diagnostic"]["evidence_id"]),
        "carried_evidence_id": str(bindings["carried_evidence"]["evidence_id"]),
        "catalog_sha256": str(bindings["catalog"]["catalog_sha256"]),
        "limits_sha256": sha256_json(plan["limits"]),
        "implementation_sha256": sha256_json(bindings["implementation"]),
        "plan_id": str(plan["plan_id"]),
        "plan_sha256": sha256_file(root / REMEDIATION_PLAN_PATH),
        "approval_command": REMEDIATION_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / REMEDIATION_PLAN_PATH),
    }


def execute_remediation_once(repository_root: Path, authorization: OperationReceipt) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    plan = _load_remediation_plan(root)
    run_id = sha256_json({"plan_id": plan["plan_id"], "receipt_id": authorization.receipt_id})
    run_root = root / REMEDIATION_ROOT / run_id
    if run_root.exists() or (root / ACTIVE_MACRO_PATH).exists():
        raise UnauthorizedOperation("SR3 remediation or macro selection already exists")
    claim = authorization.consume(
        RepoBoundary(root), operation=REMEDIATION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=remediation_required_scope(root, plan),
    )
    contract = load_successor_contract(root)
    sr3 = aggregate_sr3_conservative(
        contract, _shared_dates(), _bundles(root, plan, "SR3"), _point_rules(root)["SR3"]
    )
    carried = _read(root / CONSUMED_ROOT / "evidence.json")["aggregate_evidence"]
    new_evidence = {"SR3": sr3, "ZB": carried["ZB"], "6J": carried["6J"]}  # type: ignore[index]
    result = select_successor_macro(contract, new_evidence)
    evidence_core = {
        "schema_version": "alpha_ladder_sr3_remediation_evidence/1.0.0",
        "run_id": run_id,
        "plan_id": plan["plan_id"],
        "receipt_id": authorization.receipt_id,
        "authorization_use_sha256": sha256_file(claim),
        "sr3_aggregate_evidence": sr3,
        "carried_markets": ["ZN", "6E", "ZB", "6J"],
        "selector_result": result,
        "raw_rows_included": False,
    }
    evidence = _self_hashed(evidence_core, "evidence_id")
    _create(run_root / "evidence.json", evidence)
    selected = result["selected_market"]
    pointer_created = False
    if result["outcome"] == NO_ELIGIBLE:
        closure_core = {
            "schema_version": "alpha_ladder_macro_successor_terminal_closure/2.0.0",
            "run_id": run_id, "outcome": NO_ELIGIBLE, "selected_market": None,
            "evidence_sha256": sha256_file(run_root / "evidence.json"), "retry_allowed": False,
        }
        closure = _self_hashed(closure_core, "closure_id")
        _create(root / CLOSURE_ROOT / str(closure["closure_id"]) / "closure.json", closure)
    elif selected is not None:
        record_core = {
            "schema_version": "alpha_ladder_macro_selection_successor/2.0.0",
            "market": selected, "run_id": run_id,
            "evidence_sha256": sha256_file(run_root / "evidence.json"),
            "rank1_pointer_id": plan["bindings"]["rank1"]["pointer_id"],  # type: ignore[index]
        }
        record = _self_hashed(record_core, "selection_id")
        record_path = root / SELECTION_ROOT / str(record["selection_id"]) / "selection.json"
        _create(record_path, record)
        pointer_core = {
            "schema_version": "active_alpha_ladder_macro_market/2.0.0",
            "state": "ACTIVE_SEALED_MACRO_SELECTION", "market": selected,
            "selection_id": record["selection_id"],
            "selection_path": record_path.relative_to(root).as_posix(),
            "selection_sha256": sha256_file(record_path),
        }
        _create(root / ACTIVE_MACRO_PATH, _self_hashed(pointer_core, "pointer_id"))
        pointer_created = True
    terminal_core = {
        "schema_version": "alpha_ladder_sr3_remediation_terminal/1.0.0",
        "run_id": run_id, "outcome": result["outcome"], "selected_market": selected,
        "macro_pointer_created": pointer_created, "retry_allowed": False,
    }
    terminal = _self_hashed(terminal_core, "terminal_id")
    _create(run_root / "terminal.json", terminal)
    return terminal
