"""Preparation-only ES micro-exhaustion candidate; never opens market rows."""

from __future__ import annotations

import json
import math
from pathlib import Path

from .canonical import sha256_file, sha256_json, write_new_canonical_authority
from .errors import IntegrityError
from .intraday_alpha_discovery import enumerate_programs


PROGRAM_ROOT = Path("state/intraday_research_program_registry")
CATALOG = Path("data/active/catalog.json")
ACTIVE_POINTERS = (
    Path("configs/active_alpha_historical_research_design.json"),
    Path("configs/active_alpha_ladder_execution_design.json"),
    Path("configs/active_alpha_research_ladder.json"),
)
PRIOR_RESULT = Path(
    "state/intraday_research_official_results/"
    "5923a09a9719cd18d8925d2ba6b3f2a88ce959adcbdf288f926956b4631d2d2a/result.json"
)
SELF = Path("src/futures_rebuild/es_micro_exhaustion_preparation.py")
YEARS = tuple(range(2018, 2023))


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"unreadable JSON: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"JSON object required: {path}")
    return value


def _binding(root: Path, path: Path) -> dict[str, str]:
    return {"path": path.as_posix(), "sha256": sha256_file(root / path)}


def signal_direction(
    return_15_minute_z: float,
    counter_bar_return: float,
    volume_surprise_z: float,
) -> int:
    """Return the frozen trade direction from already-causal feature values."""

    values = (return_15_minute_z, counter_bar_return, volume_surprise_z)
    if not all(math.isfinite(value) for value in values):
        return 0
    if (
        abs(return_15_minute_z) < 2.0
        or volume_surprise_z < 1.0
        or return_15_minute_z * counter_bar_return >= 0.0
    ):
        return 0
    return -1 if return_15_minute_z > 0.0 else 1


def build_program(root: Path) -> dict[str, object]:
    """Freeze one candidate from catalog metadata without opening Parquet rows."""

    root = root.resolve(strict=True)
    catalog = _read_json(root / CATALOG)
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are missing")
    sources = sorted(
        (
            entry
            for entry in entries
            if isinstance(entry, dict)
            and entry.get("market") == "ES"
            and entry.get("year") in YEARS
        ),
        key=lambda entry: int(entry["year"]),
    )
    if (
        [entry.get("year") for entry in sources] != list(YEARS)
        or any(
            entry.get("disposition") != "RESEARCH_READY_CAUSAL_PRICE"
            or entry.get("selection_eligible") is not True
            for entry in sources
        )
    ):
        raise IntegrityError("the exact active ES 2018-2022 catalog scope is unavailable")
    source_bindings = []
    for entry in sources:
        path = Path(str(entry.get("parquet_path", "")))
        if not path.is_relative_to(Path("data/active")) or not (root / path).is_file():
            raise IntegrityError("candidate source escapes the active catalog")
        source_bindings.append(
            {
                "year": entry["year"],
                "path": path.as_posix(),
                "sha256": entry["parquet_sha256"],
                "row_count": entry["row_count"],
                "byte_count": (root / path).stat().st_size,
                "coverage_start": entry["coverage_start"],
                "coverage_end": entry["coverage_end"],
            }
        )

    prior = _read_json(root / PRIOR_RESULT)
    if (
        prior.get("official_result_id")
        != "5923a09a9719cd18d8925d2ba6b3f2a88ce959adcbdf288f926956b4631d2d2a"
        or prior.get("evidence_label") != "NO_EDGE"
        or prior.get("next_step_authorized") is not False
    ):
        raise IntegrityError("the prior continuation result binding changed")
    reversal = next(
        item
        for item in enumerate_programs()
        if item["program_name"] == "LIQUIDITY_SHOCK_REVERSAL"
    )
    candidate = {
        "mechanism": "ES_VOLUME_ACCELERATED_MICRO_EXHAUSTION",
        "return_15_minute_z_threshold": 2.0,
        "counter_bar_rule": "ONE_MINUTE_CLOSE_MINUS_OPEN_SIGN_OPPOSES_15_MINUTE_RETURN",
        "volume_surprise_z_threshold": 1.0,
        "holding_horizon_minutes": 15,
    }
    candidate["candidate_id"] = sha256_json(candidate)
    core: dict[str, object] = {
        "schema_version": "es_micro_exhaustion_program/1.0.0",
        "state": "PRE_RESULT_PREPARATION_ONLY_NOT_ACTIVE_NOT_REGISTERED",
        "hypothesis": (
            "After an unusually large 15-minute ES move, a high-volume one-minute "
            "counter-move predicts further 15-minute mean reversion after costs."
        ),
        "claim_ceiling": "ROBUST_PREDICTIVE_ASSOCIATION_NOT_CAUSAL_ABSORPTION",
        "candidate": candidate,
        "candidate_count": 1,
        "scope": {
            "market": "ES",
            "screen_years": [2018, 2019, 2020],
            "conditional_wfa_years": [2021, 2022],
            "timezone": "America/Chicago",
            "eligible_session": "[08:30:00,15:00:00)",
            "first_qualifying_event_only": True,
            "maximum_entries_per_session": 1,
            "full_session_excluded_if_actual_contract_changes": True,
        },
        "fields": [
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
        ],
        "features": {
            "return_15_minute": "LOG(CLOSE_T/CLOSE_T_MINUS_15)",
            "return_z": "CENTERED_SAMPLE_Z_VS_PRECEDING_63_COMPLETE_SESSIONS_SAME_CHICAGO_MINUTE",
            "counter_bar": "LOG(CLOSE_T/OPEN_T)_SIGN_OPPOSES_RETURN_15_MINUTE",
            "volume_z": "LOG1P_VOLUME_CENTERED_SAMPLE_Z_VS_MATCHING_ACTUAL_CONTRACT_IN_PRECEDING_63_COMPLETE_SESSIONS_SAME_CHICAGO_MINUTE",
            "minimum_matching_prior_sessions": 20,
            "missing_zero_stale_or_incomplete": "ABSTAIN",
            "mfe_mae_are_outcome_only": True,
        },
        "timing": {
            "bar_interval": "[BAR_START,BAR_START_PLUS_60_SECONDS)",
            "decision": "TRIGGER_BAR_START_PLUS_65_SECONDS",
            "entry": "OPEN_OF_FIRST_FULL_ONE_MINUTE_BAR_STARTING_STRICTLY_AFTER_DECISION",
            "entry_offset_from_trigger_bars": 2,
            "exit": "OPEN_EXACTLY_15_CLOCK_MINUTES_AFTER_ENTRY",
            "latest_entry_chicago": "14:40:00",
            "feature_label_overlap": False,
            "same_actual_contract_for_feature_entry_exit": True,
        },
        "folds": {
            "screen": "2018_THROUGH_2020_ONLY",
            "wfa": "2021_THROUGH_2022_ONLY_IF_SCREEN_PASSES",
            "chronological": True,
            "purge_minutes": 60,
            "embargo_sessions": 1,
            "transforms": "CAUSAL_PRIOR_SESSION_ONLY_NO_OUTCOME_FIT",
        },
        "execution": {
            "signal_symbol": "ES",
            "execution_proxy": "MES",
            "cost_sensitivities_usd_per_contract": [2.5, 5.0, 10.0],
            "exact_live_cost_claim": False,
            "same_bar_fill": "FORBIDDEN",
        },
        "screen_gate": {
            "minimum_opportunities": 60,
            "minimum_average_gross_usd_per_trade": 20.0,
            "positive_total_pnl_after_10_usd_cost": True,
            "positive_5_usd_cost_years_required": 3,
            "median_realized_net_mfe_mae_strictly_greater_than": 1.0,
            "top_five_positive_pnl_share_maximum": 0.5,
            "single_year_positive_pnl_share_maximum": 0.7,
            "failure_action": "CLOSE_CANDIDATE_NO_RETRY_NO_CONTINUATION_RESCUE",
            "pass_action": "PREPARE_CONDITIONAL_WFA_ONLY_NO_AUTOMATIC_AUTHORITY",
        },
        "wfa_gate": {
            "minimum_trades": 100,
            "minimum_positive_outer_folds": 5,
            "both_audit_years_positive_at_10_usd_cost": True,
            "stationary_bootstrap_and_adjusted_lower_bounds_strictly_positive": True,
            "must_beat_zero_unfiltered_reversal_and_risk_matched_directional_baselines": True,
        },
        "research_family_accounting": {
            "prior_conservative_individual_count": 168,
            "new_candidate_count_if_evaluated": 1,
            "conservative_cumulative_individual_count_if_evaluated": 169,
            "related_prepared_family": "LIQUIDITY_SHOCK_REVERSAL",
            "related_prepared_candidate_ids": [item["candidate_id"] for item in reversal["candidates"]],
            "continuation_result_cannot_be_rescued_or_reinterpreted": True,
        },
        "bindings": {
            "catalog": _binding(root, CATALOG),
            "active_pointers": {path.as_posix(): sha256_file(root / path) for path in ACTIVE_POINTERS},
            "prior_continuation_result": {
                "official_result_id": prior["official_result_id"],
                **_binding(root, PRIOR_RESULT),
            },
            "implementation": _binding(root, SELF),
        },
        "sources": source_bindings,
        "expected_volume": {
            "partitions": len(source_bindings),
            "catalog_rows": sum(int(item["row_count"]) for item in source_bindings),
            "physical_bytes": sum(int(item["byte_count"]) for item in source_bindings),
        },
        "authority": {
            "market_row_read": False,
            "feature_or_outcome_construction": False,
            "model_fit_or_prediction": False,
            "trial_registration": False,
            "historical_execution": False,
            "wfa": False,
            "challenge_or_holdout": False,
            "provider_or_network": False,
            "publication_or_activation": False,
            "trading": False,
        },
    }
    return {**core, "program_id": sha256_json(core)}


def prepare_program(root: Path) -> dict[str, object]:
    program = build_program(root)
    path = PROGRAM_ROOT / str(program["program_id"]) / "program.json"
    write_new_canonical_authority(root.resolve(strict=True) / path, program)
    return {
        "program_id": program["program_id"],
        "program_path": path.as_posix(),
        "status": "PREPARED_NOT_ACTIVE_REAL_DATA_REQUIRES_SEPARATE_APPROVAL",
    }
