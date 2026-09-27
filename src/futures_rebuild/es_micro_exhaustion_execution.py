"""One approved, source-bound ES micro-exhaustion development screen."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

from .alpha_research_ladder import load_active_ladder
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json, write_new_canonical_authority
from .errors import IntegrityError
from .es_micro_exhaustion_preparation import build_program, signal_direction
from .intraday_alpha_execution_v12 import _active_es_row_to_bar
from .intraday_alpha_pipeline_v12 import (
    CausalBar,
    _es_active_source_binding,
    assert_authority_not_blocked,
)
from .research_gateway_policy import (
    CERTIFIED_GATEWAY_SCHEMA,
    CERTIFIED_TRIAL_EXECUTION_OPERATION,
    scientific_governance_binding,
)


PROGRAM_ID = "4fa73a9a8547e49e67c2b1b1eafc31cbb1278ae5118080144b9ec58bcc64e340"
PROGRAM_PATH = Path("state/intraday_research_program_registry") / PROGRAM_ID / "program.json"
TRIAL_ROOT = Path("state/trial_registry/es_micro_exhaustion")
RESULT_ROOT = Path("state/intraday_research_official_results")
SELF = Path("src/futures_rebuild/es_micro_exhaustion_execution.py")
SCREEN_YEARS = (2018, 2019, 2020)
FIELDS = (
    "market",
    "project_trade_date",
    "bar_start_ns",
    "available_at_ns",
    "actual_contract",
    "open_nano",
    "high_nano",
    "low_nano",
    "close_nano",
    "volume",
    "min_price_increment_nano",
    "multiplier_nano",
    "source_cadence",
)
CHICAGO = ZoneInfo("America/Chicago")
MES_MULTIPLIER = 5.0
MES_TICK_VALUE = 1.25
COSTS = (2.5, 5.0, 10.0)


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"unreadable JSON: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"JSON object required: {path}")
    return value


def _program(root: Path) -> dict[str, object]:
    path = root / PROGRAM_PATH
    raw = path.read_bytes()
    program = _read_json(path)
    if (
        raw != canonical_bytes(program) + b"\n"
        or program.get("program_id") != PROGRAM_ID
        or program != build_program(root)
    ):
        raise IntegrityError("micro-exhaustion program binding changed")
    return program


def build_trial_registration(root: Path) -> dict[str, object]:
    """Create the durable declaration without opening a historical row."""

    root = root.resolve(strict=True)
    program = _program(root)
    contract, profile = load_active_ladder(root)
    governance = scientific_governance_binding(root)
    interpretation = {
        "position_normalization": "ONE_MES_CONTRACT",
        "exit_rule": "TIME_EXIT_ONLY_NO_INTRABAR_STOP",
        "mes_multiplier_usd_per_point": MES_MULTIPLIER,
        "mes_tick_value_usd": MES_TICK_VALUE,
        "cost_sensitivities_usd_round_trip": list(COSTS),
        "gate_cost_usd": 5.0,
        "net_mfe_mae": "MAX(MFE_MINUS_5,0)/MAX(MAE_PLUS_5,1.25)",
        "concentration": "POSITIVE_NET_PNL_AT_5_USD_COST",
        "conditional_wfa": "NOT_EXECUTED_BY_THIS_SCREEN",
    }
    screen_sources = [
        source for source in program["sources"]  # type: ignore[index]
        if source["year"] in SCREEN_YEARS
    ]
    readiness_core = {
        "program_id": PROGRAM_ID,
        "screen_sources_sha256": sha256_json(screen_sources),
        "implementation_sha256": sha256_file(root / SELF),
        "interpretation_sha256": sha256_json(interpretation),
    }
    core = {
        "schema_version": "es_micro_exhaustion_trial_registration/1.0.0",
        "state": "REGISTERED_NOT_CLAIMED_NOT_EXECUTED",
        "trial_family": "ES_VOLUME_ACCELERATED_MICRO_EXHAUSTION",
        "protocol_id": PROGRAM_ID,
        "program": {
            "program_id": PROGRAM_ID,
            "path": PROGRAM_PATH.as_posix(),
            "sha256": sha256_file(root / PROGRAM_PATH),
        },
        "implementation": {"path": SELF.as_posix(), "sha256": sha256_file(root / SELF)},
        "readiness": {**readiness_core, "readiness_id": sha256_json(readiness_core)},
        "screen_years": list(SCREEN_YEARS),
        "evaluation_interpretation": interpretation,
        "alpha_ladder_binding": {
            "contract_id": contract["contract_id"],
            "profile_id": profile["profile_id"],
            "stage": "pilot",
        },
        "scientific_governance_binding": governance,
        "predecessor": program["bindings"]["prior_continuation_result"],  # type: ignore[index]
        "candidate_ids": [program["candidate"]["candidate_id"]],  # type: ignore[index]
        "research_family_accounting": program["research_family_accounting"],
        "authority": {
            "historical_rows": False,
            "conditional_wfa": False,
            "provider_or_network": False,
            "holdout_or_forward": False,
            "publication": False,
            "trading": False,
        },
    }
    return {**core, "trial_id": sha256_json(core)}


def prepare_trial_registration(root: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    for existing in sorted((root / TRIAL_ROOT).glob("*.json")):
        registration = _read_json(existing)
        implementation = registration.get("implementation")
        if (
            registration.get("protocol_id") == PROGRAM_ID
            and isinstance(implementation, dict)
            and implementation.get("sha256") == sha256_file(root / SELF)
        ):
            return {
                "trial_id": registration["trial_id"],
                "registration_path": existing.relative_to(root).as_posix(),
                "registration_sha256": sha256_file(existing),
            }
    registration = build_trial_registration(root)
    relative = TRIAL_ROOT / f"{registration['trial_id']}.json"
    write_new_canonical_authority(root / relative, registration)
    return {
        "trial_id": registration["trial_id"],
        "registration_path": relative.as_posix(),
        "registration_sha256": sha256_file(root / relative),
    }


def _validate_active_screen_sources(
    root: Path, boundary: RepoBoundary, program: Mapping[str, object]
) -> list[dict[str, object]]:
    declared = {
        int(source["year"]): source
        for source in program["sources"]  # type: ignore[index]
        if int(source["year"]) in SCREEN_YEARS
    }
    if set(declared) != set(SCREEN_YEARS):
        raise IntegrityError("screen source declaration is incomplete")
    current = []
    for year in SCREEN_YEARS:
        active = _es_active_source_binding(root, boundary, year)
        source = declared[year]
        if (
            source["path"] != active["parquet_path"]
            or source["sha256"] != active["parquet_sha256"]
        ):
            raise IntegrityError("active ES screen source changed")
        current.append(source)
    return current


def execution_scope(
    root: Path, registration_path: Path, registration_sha256: str
) -> dict[str, str]:
    """Derive the exact single-use scope before any Parquet row is opened."""

    root = root.resolve(strict=True)
    registration_path = registration_path.resolve(strict=True)
    try:
        relative = registration_path.relative_to(root)
    except ValueError as exc:
        raise IntegrityError("micro-exhaustion registration leaves the repository") from exc
    registration = _read_json(registration_path)
    core = {key: value for key, value in registration.items() if key != "trial_id"}
    program = _program(root)
    screen_sources = [
        source for source in program["sources"]  # type: ignore[index]
        if source["year"] in SCREEN_YEARS
    ]
    interpretation = registration.get("evaluation_interpretation")
    readiness = registration.get("readiness")
    implementation = registration.get("implementation")
    program_binding = registration.get("program")
    readiness_core = {
        "program_id": PROGRAM_ID,
        "screen_sources_sha256": sha256_json(screen_sources),
        "implementation_sha256": sha256_file(root / SELF),
        "interpretation_sha256": sha256_json(interpretation),
    }
    if (
        relative.parent != TRIAL_ROOT
        or sha256_file(registration_path) != registration_sha256
        or registration_path.read_bytes() != canonical_bytes(registration) + b"\n"
        or registration.get("trial_id") != sha256_json(core)
        or registration_path.stem != registration.get("trial_id")
        or registration.get("schema_version")
        != "es_micro_exhaustion_trial_registration/1.0.0"
        or registration.get("state") != "REGISTERED_NOT_CLAIMED_NOT_EXECUTED"
        or not isinstance(interpretation, dict)
        or not isinstance(readiness, dict)
        or not isinstance(implementation, dict)
        or not isinstance(program_binding, dict)
        or program_binding
        != {"program_id": PROGRAM_ID, "path": PROGRAM_PATH.as_posix(), "sha256": sha256_file(root / PROGRAM_PATH)}
        or implementation != {"path": SELF.as_posix(), "sha256": sha256_file(root / SELF)}
        or readiness != {**readiness_core, "readiness_id": sha256_json(readiness_core)}
        or registration.get("screen_years") != list(SCREEN_YEARS)
        or registration.get("candidate_ids") != [program["candidate"]["candidate_id"]]  # type: ignore[index]
        or registration.get("research_family_accounting") != program["research_family_accounting"]
        or registration.get("predecessor") != program["bindings"]["prior_continuation_result"]  # type: ignore[index]
        or any(registration.get("authority", {}).values())
    ):
        raise IntegrityError("micro-exhaustion registration binding changed")
    boundary = RepoBoundary(root)
    sources = _validate_active_screen_sources(root, boundary, program)
    contract, profile = load_active_ladder(root)
    ladder = registration["alpha_ladder_binding"]
    governance = registration["scientific_governance_binding"]
    current_governance = scientific_governance_binding(root)
    if (
        ladder
        != {"contract_id": contract["contract_id"], "profile_id": profile["profile_id"], "stage": "pilot"}
        or governance["scientific_policy"] != current_governance["scientific_policy"]
        or governance["legacy_attempts"] != current_governance["legacy_attempts"]
    ):
        raise IntegrityError("micro-exhaustion governance binding changed")
    return {
        "gateway_schema": CERTIFIED_GATEWAY_SCHEMA,
        "operation_kind": "TRIAL_HISTORICAL_EXECUTION",
        "trial_id": str(registration["trial_id"]),
        "trial_family": str(registration["trial_family"]),
        "protocol_id": PROGRAM_ID,
        "registration_path": relative.as_posix(),
        "registration_sha256": registration_sha256,
        "readiness_certificate_id": str(readiness["readiness_id"]),
        "readiness_evidence_sha256": registration_sha256,
        "alpha_ladder_contract_id": str(contract["contract_id"]),
        "alpha_ladder_profile_id": str(profile["profile_id"]),
        "alpha_ladder_stage": "pilot",
        "mechanism_sha256": str(registration["implementation"]["sha256"]),
        "predecessor_decision_sha256": str(registration["predecessor"]["sha256"]),
        "session_manifest_sha256": sha256_json(sources),
        "pilot_evaluation_session_ids_sha256": sha256_json({"screen_years": list(SCREEN_YEARS)}),
        "scientific_policy_sha256": str(governance["scientific_policy"]["sha256"]),
        "global_trial_ledger_head": str(governance["global_trial_ledger"]["head"]),
        "conservative_legacy_attempt_count": str(governance["legacy_attempts"]["conservative_count"]),
        "intraday_authority_id": str(registration["trial_id"]),
        "markets": "ES",
        "years": ",".join(str(year) for year in SCREEN_YEARS),
        "provider_access": "false",
        "holdout_or_forward_access": "false",
    }


def _load_screen_bars(
    root: Path, boundary: RepoBoundary, program: Mapping[str, object]
) -> tuple[list[CausalBar], dict[str, object]]:
    """Open only the three approved development Parquets after receipt consumption."""

    sources = _validate_active_screen_sources(root, boundary, program)
    bars: list[CausalBar] = []
    rows_by_year: dict[str, int] = {}
    try:
        import pyarrow.parquet as pq

        for source in sources:
            year = int(source["year"])
            path = boundary.assert_active_path(
                root / str(source["path"]),
                purpose="micro-exhaustion screen input",
                subtree="data/active",
            )
            reader = pq.ParquetFile(path)
            if (
                not set(FIELDS).issubset(reader.schema_arrow.names)
                or reader.metadata.num_rows != int(source["row_count"])
                or path.stat().st_size != int(source["byte_count"])
            ):
                raise IntegrityError("micro-exhaustion source schema or volume changed")
            rows = 0
            for batch in reader.iter_batches(batch_size=65_536, columns=list(FIELDS)):
                rows += batch.num_rows
                for raw in batch.to_pylist():
                    bar = _active_es_row_to_bar(raw, year)
                    if bar is not None:
                        bars.append(bar)
            if rows != int(source["row_count"]):
                raise IntegrityError("micro-exhaustion source row count changed during read")
            rows_by_year[str(year)] = rows
    except IntegrityError:
        raise
    except Exception as exc:
        raise IntegrityError("approved ES screen inputs could not be read") from exc
    return bars, {
        "partitions_read": len(sources),
        "rows_read": sum(rows_by_year.values()),
        "rows_by_year": rows_by_year,
        "physical_bytes": sum(int(source["byte_count"]) for source in sources),
        "conditional_wfa_partitions_read": 0,
        "conditional_wfa_rows_read": 0,
    }


def _complete_sessions(
    bars: Sequence[CausalBar],
) -> tuple[list[tuple[str, tuple[CausalBar, ...]]], dict[str, int]]:
    grouped: dict[str, dict[int, CausalBar]] = defaultdict(dict)
    duplicate = False
    for bar in bars:
        local = bar.event_at.astimezone(CHICAGO)
        minute = local.hour * 60 + local.minute - 510
        if local.date().isoformat() != bar.exchange_session_date or local.second or minute not in range(391):
            raise IntegrityError("ES screen row has invalid Chicago session semantics")
        if bar.available_at < bar.event_at + timedelta(seconds=60):
            raise IntegrityError("ES screen row violates availability_at")
        duplicate |= minute in grouped[bar.exchange_session_date]
        grouped[bar.exchange_session_date][minute] = bar
    if duplicate:
        raise IntegrityError("ES screen contains a duplicate session minute")
    complete: list[tuple[str, tuple[CausalBar, ...]]] = []
    incomplete = 0
    roll = 0
    for date in sorted(grouped):
        rows = grouped[date]
        if set(rows) != set(range(391)):
            incomplete += 1
            continue
        ordered = tuple(rows[index] for index in range(391))
        if len({row.actual_identity for row in ordered[:390]}) != 1:
            roll += 1
            continue
        complete.append((date, ordered))
    return complete, {
        "source_session_count": len(grouped),
        "complete_single_contract_session_count": len(complete),
        "incomplete_session_count": incomplete,
        "contract_change_session_count": roll,
    }


def _trade_outcome(
    date: str,
    rows: Sequence[CausalBar],
    event: int,
    direction: int,
    return_z: float,
    volume_z: float,
) -> dict[str, object]:
    entry = event + 2
    exit_minute = entry + 15
    decision = rows[event].event_at + timedelta(seconds=65)
    if not (rows[entry - 1].event_at <= decision < rows[entry].event_at):
        raise IntegrityError("frozen entry timing is not satisfied")
    entry_price = rows[entry].open
    exit_price = rows[exit_minute].open
    favorable = 0.0
    adverse = 0.0
    for row in rows[entry:exit_minute]:
        favorable = max(favorable, direction * ((row.high if direction > 0 else row.low) - entry_price))
        adverse = max(adverse, -direction * ((row.low if direction > 0 else row.high) - entry_price))
    terminal = direction * (exit_price - entry_price)
    favorable = max(favorable, terminal, 0.0)
    adverse = max(adverse, -terminal, 0.0)
    gross = terminal * MES_MULTIPLIER
    mfe = favorable * MES_MULTIPLIER
    mae = adverse * MES_MULTIPLIER
    nets = {f"{cost:.2f}": gross - cost for cost in COSTS}
    net_ratio = max(mfe - 5.0, 0.0) / max(mae + 5.0, MES_TICK_VALUE)
    event_core = {
        "session_date": date,
        "event_at_ns": int(rows[event].event_at.timestamp() * 1_000_000_000),
        "actual_contract": rows[event].actual_identity,
        "candidate_id": "a9d9bee6fe7b90eb051ce9b87103010bcd808b3c387f587be52b882dbcadf6d9",
    }
    return {
        **event_core,
        "event_id": sha256_json(event_core),
        "event_minute": event,
        "entry_minute": entry,
        "exit_minute": exit_minute,
        "direction": direction,
        "return_z": return_z,
        "volume_surprise_z": volume_z,
        "gross_pnl_usd": gross,
        "net_pnl_usd": nets,
        "mfe_usd": mfe,
        "mae_usd": mae,
        "net_mfe_mae_at_5_usd": net_ratio,
    }


def _first_trade(
    sessions: Sequence[tuple[str, tuple[CausalBar, ...]]], index: int
) -> dict[str, object] | None:
    if index < 63:
        return None
    date, current = sessions[index]
    prior = [rows for _, rows in sessions[index - 63:index]]
    for event in range(15, 369):
        decision = current[event].event_at + timedelta(seconds=65)
        if current[event].available_at > decision or current[event - 15].available_at > decision:
            continue
        current_return = math.log(current[event].close / current[event - 15].close)
        prior_returns = np.asarray(
            [math.log(rows[event].close / rows[event - 15].close) for rows in prior],
            dtype=np.float64,
        )
        return_scale = float(np.std(prior_returns, ddof=1))
        if return_scale <= 0.0:
            continue
        return_z = (current_return - float(np.mean(prior_returns))) / return_scale
        identity = current[event].actual_identity
        matching_volume = np.asarray(
            [math.log1p(rows[event].volume) for rows in prior if rows[event].actual_identity == identity],
            dtype=np.float64,
        )
        if len(matching_volume) < 20:
            continue
        volume_scale = float(np.std(matching_volume, ddof=1))
        if volume_scale <= 0.0:
            continue
        volume_z = (math.log1p(current[event].volume) - float(np.mean(matching_volume))) / volume_scale
        counter_return = math.log(current[event].close / current[event].open)
        direction = signal_direction(return_z, counter_return, volume_z)
        if direction:
            return _trade_outcome(date, current, event, direction, return_z, volume_z)
    return None


def _screen_summary(
    trades: Sequence[Mapping[str, object]], session_accounting: Mapping[str, int]
) -> dict[str, object]:
    annual_5 = {
        str(year): float(
            sum(
                float(trade["net_pnl_usd"]["5.00"])  # type: ignore[index]
                for trade in trades
                if str(trade["session_date"]).startswith(str(year))
            )
        )
        for year in SCREEN_YEARS
    }
    cost_results = {
        f"{cost:.2f}": {
            "total_net_pnl_usd": float(sum(float(trade["net_pnl_usd"][f"{cost:.2f}"]) for trade in trades)),  # type: ignore[index]
            "annual_net_pnl_usd": {
                str(year): float(
                    sum(
                        float(trade["net_pnl_usd"][f"{cost:.2f}"])  # type: ignore[index]
                        for trade in trades
                        if str(trade["session_date"]).startswith(str(year))
                    )
                )
                for year in SCREEN_YEARS
            },
        }
        for cost in COSTS
    }
    positive_trades = sorted(
        (float(trade["net_pnl_usd"]["5.00"]) for trade in trades if float(trade["net_pnl_usd"]["5.00"]) > 0),  # type: ignore[index]
        reverse=True,
    )
    positive_total = sum(positive_trades)
    top_five_share = sum(positive_trades[:5]) / positive_total if positive_total > 0 else None
    positive_year_total = sum(value for value in annual_5.values() if value > 0)
    single_year_share = (
        max(annual_5.values()) / positive_year_total if positive_year_total > 0 else None
    )
    count = len(trades)
    gross = float(sum(float(trade["gross_pnl_usd"]) for trade in trades))
    median_ratio = (
        float(np.median([float(trade["net_mfe_mae_at_5_usd"]) for trade in trades]))
        if trades
        else None
    )
    checks = {
        "minimum_60_opportunities": count >= 60,
        "average_gross_at_least_20_usd": count > 0 and gross / count >= 20.0,
        "positive_total_pnl_after_10_usd_cost": cost_results["10.00"]["total_net_pnl_usd"] > 0,
        "all_three_years_positive_after_5_usd_cost": all(value > 0 for value in annual_5.values()),
        "median_net_mfe_mae_strictly_above_one": median_ratio is not None and median_ratio > 1.0,
        "top_five_positive_pnl_share_at_most_half": top_five_share is not None and top_five_share <= 0.5,
        "single_year_positive_pnl_share_at_most_0_7": single_year_share is not None and single_year_share <= 0.7,
    }
    passed = all(checks.values())
    return {
        "stage": "DEVELOPMENT_SCREEN_2018_2020",
        "candidate_id": "a9d9bee6fe7b90eb051ce9b87103010bcd808b3c387f587be52b882dbcadf6d9",
        "opportunity_count": count,
        "gross_pnl_usd": gross,
        "average_gross_pnl_usd_per_trade": gross / count if count else None,
        "cost_results": cost_results,
        "median_realized_net_mfe_mae_at_5_usd": median_ratio,
        "top_five_positive_pnl_share_at_5_usd": top_five_share,
        "single_year_positive_pnl_share_at_5_usd": single_year_share,
        "session_accounting": dict(session_accounting),
        "gate_checks": checks,
        "passed": passed,
        "decision": (
            "PREPARE_CONDITIONAL_WFA_ONLY_NO_AUTOMATIC_AUTHORITY"
            if passed
            else "STOP_NO_ECONOMIC_EDGE_CLOSE_CANDIDATE"
        ),
        "trades": list(trades),
        "trade_records_sha256": sha256_json(list(trades)),
    }


def evaluate_screen(bars: Sequence[CausalBar]) -> dict[str, object]:
    sessions, accounting = _complete_sessions(bars)
    years = {int(date[:4]) for date, _ in sessions}
    if years != set(SCREEN_YEARS) or len(sessions) < 64:
        raise IntegrityError("screen lacks complete sessions from every approved year")
    trades = [trade for index in range(len(sessions)) if (trade := _first_trade(sessions, index)) is not None]
    accounting = {
        **accounting,
        "lookback_only_session_count": 63,
        "evaluated_session_count": len(sessions) - 63,
        "no_qualifying_event_session_count": len(sessions) - 63 - len(trades),
    }
    return _screen_summary(trades, accounting)


def execute_screen_once(
    root: Path,
    boundary: RepoBoundary,
    receipt: OperationReceipt,
    registration_path: Path,
    registration_sha256: str,
) -> dict[str, object]:
    """Consume approval, evaluate 2018-2020, and never open conditional WFA rows."""

    if type(receipt) is not OperationReceipt:
        raise IntegrityError("an actual OperationReceipt is required")
    root = boundary.assert_active_root(root)
    expected_scope = execution_scope(root, registration_path, registration_sha256)
    observed_scope = dict(receipt.scope)
    if {
        key: value
        for key, value in observed_scope.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    } != expected_scope:
        raise IntegrityError("receipt is not bound to the frozen micro-exhaustion screen")
    registration = _read_json(registration_path)
    authority_id = str(registration["trial_id"])
    assert_authority_not_blocked(root, authority_id)
    use_path = receipt.consume(
        boundary,
        operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=observed_scope,
    )
    bars, read_accounting = _load_screen_bars(root, boundary, _program(root))
    screen = evaluate_screen(bars)
    screen["source_read_accounting"] = read_accounting
    official = {
        "schema_version": "intraday_research_official_historical/1.2.3",
        "authority_id": authority_id,
        "registration": {"path": str(registration_path.relative_to(root)), "sha256": registration_sha256},
        "consumed_use": {"path": str(use_path.relative_to(root)), "sha256": sha256_file(use_path)},
        "calculations": [screen],
        "portfolio": None,
        "evidence_label": "SCREEN_PASSED_WFA_NOT_EXECUTED" if screen["passed"] else "NO_EDGE",
        "next_step_authorized": False,
        "conditional_wfa_rows_read": 0,
        "provider_or_network_access": False,
        "holdout_or_forward_access": False,
    }
    official["official_result_id"] = sha256_json(official)
    path = root / RESULT_ROOT / str(official["official_result_id"]) / "result.json"
    write_new_canonical_authority(path, official)
    return official
