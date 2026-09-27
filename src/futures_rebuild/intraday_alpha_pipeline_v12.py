"""Fail-closed v1.2 intraday research preparation and pure mechanics.

No function in this module opens market data.  Authorized row opening lives in
``intraday_alpha_execution_v12`` and is reachable only after the certified
gateway consumes an ``OperationReceipt``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING
from functools import lru_cache
from hashlib import sha256
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

from .canonical import canonical_bytes, sha256_file, sha256_json, write_new_canonical_authority
from .boundary import RepoBoundary
from .errors import IntegrityError
from .intraday_alpha_discovery import enumerate_programs
from .alpha_research_ladder import load_active_ladder
from .research_gateway_policy import scientific_governance_binding
from .research.cscv import exhaustive_cscv_pbo
from .research.bootstrap import stationary_bootstrap_indices
from .research.contracts import ResearchContractError
from .research.multiple_testing import romano_wolf_from_differentials, romano_wolf_stepdown
from .research.splits import SessionWindow, TemporalSamples, nested_chronological_splits
from .prop_firm_account_runtime import (
    resolve_execution_instrument,
    validate_execution_cost_profile,
    validate_instrument_mapping,
)


PROGRAM_SCHEMA = "intraday_research_program/1.2.3"
DECISION_SCHEMA = "intraday_research_decision/1.2.3"
SYNTHETIC_SCHEMA = "intraday_research_synthetic_acceptance/1.2.3"
SYNTHETIC_MAX_ELAPSED_SECONDS = 108_000
AUTHORITY_SCHEMA = "intraday_research_authority_successor/1.2.3"
FAILURE_SCHEMA = "intraday_research_failure/1.0.0"
MONITOR_SCHEMA = "intraday_research_monitor_checkpoint/1.2.3"
SOURCE_REGISTRATION_SCHEMA = "intraday_research_source_registration/1.2.3"
ROW_COMPLETENESS_SCHEMA = "intraday_research_row_completeness/1.2.3"
SYNTHETIC_CHUNK_SCHEMA = "intraday_research_synthetic_chunk/1.2.3"
SYNTHETIC_RUN_SCHEMA = "intraday_research_synthetic_run/1.2.3"
CHICAGO = ZoneInfo("America/Chicago")

PROGRAM_ROOT = Path("state/intraday_research_program_registry")
ES_FLOW_TRIAL_ROOT = Path("state/trial_registry/es_flow_rescue")
AUTHORITY_ROOT = Path("state/intraday_research_authority_registry")
FAILURE_ROOT = Path("state/intraday_research_failure_registry")
MONITOR_ROOT = Path("state/intraday_research_monitor_registry")
SYNTHETIC_ROOT = Path("state/intraday_research_synthetic_acceptance_registry")
SYNTHETIC_RUN_ROOT = Path("state/intraday_research_synthetic_run_registry")
SOURCE_REGISTRATION_ROOT = Path("state/intraday_research_source_registration_registry")
CATALOG = Path("data/active/catalog.json")
DBN_POINTER = Path("configs/active_dbn_congruence_release_v1.json")
SOURCE_CONTRACT = Path("configs/source_contract.json")
HISTORICAL_CALENDAR_POINTER = Path("configs/active_cash_open_impulse_historical_calendar.json")
ECONOMICS_RULEBOOK = Path("configs/contract_economics_rules.json")
ACTIVE_POINTERS = (
    Path("configs/active_alpha_historical_research_design.json"),
    Path("configs/active_alpha_ladder_execution_design.json"),
    Path("configs/active_alpha_research_ladder.json"),
)

ES_FLOW_SCREEN_STAGE = "ES_FLOW_OPPORTUNITY_SCREEN"
ES_FLOW_SCREEN_YEARS = tuple(range(2018, 2023))
ES_FLOW_DEVELOPMENT_YEARS = (2018, 2019, 2020)
ES_FLOW_AUDIT_YEARS = (2021, 2022)
ES_FLOW_HORIZONS = (15, 60)
ES_FLOW_COSTS = (2.5, 5.0, 10.0)
ES_FLOW_FEATURES = (
    "signed_15_minute_return",
    "return_z_score",
    "volume_surprise_z_score",
    "atr20_divided_by_price",
    "realized_volatility_20_minute",
    "minute_of_regular_session",
)
ES_FLOW_BASELINES = (
    "ZERO_NO_TRADE",
    "UNFILTERED_CONTINUATION",
    "MATCHED_REVERSAL",
    "RISK_MATCHED_ALWAYS_LONG",
    "RISK_MATCHED_ALWAYS_SHORT",
)
ES_FLOW_BOOTSTRAP_SEED = 20260807
ES_FLOW_BOOTSTRAP_RESAMPLES = 10_000
ES_FLOW_BOOTSTRAP_BLOCK = 5.0
MES_COST_PROFILE_ID = "mff_micro_provisional_stress_v1"
MES_MAPPING = Path("configs/prop_firm_execution_instruments.json")
MES_COSTS = Path("configs/prop_firm_execution_costs.json")
MES_RISK = Path("configs/prop_firm_risk_profile.json")
ES_PRIOR_TRIAL = Path("state/trial_registry/alpha_ladder_es_pilot/a6ae7b8394906c3661b9f1456f30cf513d5a1df43a072c9e8a601bc8989c82bc.json")

FLAWED_AUTHORITY_ID = "a4b63e32095c780ae703c8b6ed66470cebafdbdce26c353def3056359a8f1e2b"
FLAWED_CERTIFICATE_ID = "e27b2fdb14eb76452a121d9f1e939316599436bd459d2193035ad6f70f16d109"
FLAWED_PROGRAM_ID = "fb343e4089b4b9e07d75bedb587e17e66bccbe15d4d3143cc18caaf75ca20b64"
FLAWED_AUTHORITY = AUTHORITY_ROOT / FLAWED_AUTHORITY_ID / "authority.json"
FLAWED_CERTIFICATE = Path("state/intraday_research_validation_registry") / FLAWED_CERTIFICATE_ID / "certificate.json"
DENSE_GRID_AUTHORITY_ID = "e11208ae6692aeda8aba2f9bb810863673942e482b61c731d0ed64b35e6bdf84"
DENSE_GRID_PROGRAM_ID = "fc9d1604b9fa738c4cc89f2e15f509d77e63e2e9f225807059550736c14daff1"
DENSE_GRID_AUTHORITY = AUTHORITY_ROOT / DENSE_GRID_AUTHORITY_ID / "authority.json"

BASELINES = ("ZERO", "UNCONDITIONAL", "MOMENTUM", "REVERSAL", "ALWAYS_LONG", "ALWAYS_SHORT")
EXPOSURES = ("intercept", "open_direction", "trend_60m", "realized_volatility_60m", "liquidity_surprise", "opening_gap", "minute_of_session")
FAILURE_FINDINGS = (
    "FORGEABLE_SERIALIZED_SOURCE_CONTEXT",
    "DISCONNECTED_MATERIALIZER_EVALUATOR",
    "NON_CAUSAL_OR_UNENFORCED_TIMING",
    "NON_FROZEN_WFA_SCHEDULE",
    "CALLER_SUPPLIED_CONTROLS_AND_RESIDUALS",
    "FLATTENED_MARKET_INFERENCE",
    "SYNTHETIC_SUMMARY_SUBSTITUTED_FOR_EXACT_PIPELINE",
    "MISSING_PORTFOLIO_AND_MONITORING_MECHANICS",
    "VALIDATOR_TRUSTED_PRODUCER_FLAGS",
    "DENSE_PRINTED_BAR_GRID_ASSUMPTION_CONTRADICTED_REGISTERED_OHLCV",
)


def _read_json(path: Path) -> dict[str, object]:
    import json

    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"unreadable JSON: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"JSON object required: {path}")
    return value


def _identified(core: dict[str, object], field: str) -> dict[str, object]:
    return {**core, field: sha256_json(core)}


def _binding(root: Path, path: Path) -> dict[str, str]:
    return {"path": path.as_posix(), "sha256": sha256_file(root / path)}


def build_failure_record(root: Path) -> dict[str, object]:
    """Bind and permanently quarantine the exact flawed preparation."""

    root = root.resolve(strict=True)
    authority = _read_json(root / FLAWED_AUTHORITY)
    certificate = _read_json(root / FLAWED_CERTIFICATE)
    dense_grid = _read_json(root / DENSE_GRID_AUTHORITY)
    if authority.get("authority_id") != FLAWED_AUTHORITY_ID or certificate.get("validation_id") != FLAWED_CERTIFICATE_ID or dense_grid.get("authority_id") != DENSE_GRID_AUTHORITY_ID:
        raise IntegrityError("flawed predecessor identity changed")
    core = {
        "schema_version": FAILURE_SCHEMA,
        "state": "SUPERSEDED_NON_ACTIVATABLE_BLOCKED_REMEDIATION",
        "predecessor": {
            "authority": {"authority_id": FLAWED_AUTHORITY_ID, **_binding(root, FLAWED_AUTHORITY)},
            "certificate": {"validation_id": FLAWED_CERTIFICATE_ID, **_binding(root, FLAWED_CERTIFICATE)},
            "program_id": FLAWED_PROGRAM_ID,
        },
        "superseded_candidate": {
            "authority": {"authority_id": DENSE_GRID_AUTHORITY_ID, **_binding(root, DENSE_GRID_AUTHORITY)},
            "program_id": DENSE_GRID_PROGRAM_ID,
            "reason": "REGISTERED_OHLCV_HAS_CAUSAL_SPARSE_INTERVALS_AND_MUST_NOT_BE_DENSIFIED_AS_OBSERVED_DATA",
        },
        "audit_findings": list(FAILURE_FINDINGS),
        "effect": "ALL_PREPARATION_AND_ACTIVATION_PATHS_MUST_REJECT_PREDECESSOR",
        "authority": {"activation": False, "rows": False, "provider": False, "holdout": False, "forward": False, "trading": False},
    }
    return _identified(core, "failure_id")


def assert_authority_not_blocked(root: Path, authority_id: str) -> None:
    if authority_id in {FLAWED_AUTHORITY_ID, DENSE_GRID_AUTHORITY_ID}:
        raise IntegrityError("intraday authority is permanently blocked by its failure record")
    for path in (root / FAILURE_ROOT).glob("*/failure.json"):
        value = _read_json(path)
        predecessor = value.get("predecessor")
        if isinstance(predecessor, dict) and isinstance(predecessor.get("authority"), dict) and predecessor["authority"].get("authority_id") == authority_id:
            raise IntegrityError("intraday authority is listed in the failure registry")
        superseded = value.get("superseded_candidate")
        if isinstance(superseded, dict) and isinstance(superseded.get("authority"), dict) and superseded["authority"].get("authority_id") == authority_id:
            raise IntegrityError("intraday authority is listed in the failure registry")


def build_program(root: Path, failure: Mapping[str, object]) -> dict[str, object]:
    catalog = _read_json(root / CATALOG)
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("catalog entries are missing")
    markets = sorted({str(row.get("market")) for row in entries if isinstance(row, dict)})
    if markets != ["6E", "6J", "CL", "ES", "GC", "NQ", "SR3", "ZB", "ZN"]:
        raise IntegrityError("nine-market catalog changed")
    programs = enumerate_programs()
    flow = next(program for program in programs if program["program_name"] == "FLOW_CONTINUATION")
    if flow["candidates"][0] != {
        **{key: flow["candidates"][0][key] for key in ("mechanism", "ordinal", "candidate_id")},
        "return_z_threshold": 1.5,
        "volume_surprise_threshold": 0.5,
        "holding_horizon_minutes": 15,
    } or flow["candidates"][0]["ordinal"] != 0:
        raise IntegrityError("frozen FLOW_CONTINUATION candidate ordinal zero changed")
    core = {
        "schema_version": PROGRAM_SCHEMA,
        "state": "PRE_RESULT_PREPARATION_ONLY_NOT_ACTIVE",
        "failure_record": {"failure_id": failure["failure_id"], "path": (FAILURE_ROOT / str(failure["failure_id"]) / "failure.json").as_posix()},
        "programs": programs,
        "candidate_count": 36,
        "search_accounting": {"legacy": 105, "abandoned_pre_result": 27, "current": 36, "individual": 168, "portfolio": 169, "dsr": "DIAGNOSTIC_ONLY"},
        "timing": {
            "timezone": "America/Chicago", "bar_interval": "[event_at,event_at+60s)",
            "decision_delay_seconds": 65, "entry_rule": "FIRST_BAR_START_STRICTLY_AFTER_DECISION",
            "maximum_asof_age_seconds": 60, "minimum_complete_feature_lookback_minutes": 60, "latest_entries_chicago": {"15": "14:40", "30": "14:25", "60": "13:55"}, "flat_by_chicago": "15:00",
        },
        "schedule": {"previous_infeasible_minimum_complete_dates": 1008, "required_complete_dates": 1010, "minimum_reconciliation": "1008_CANNOT_SIMULTANEOUSLY_SUPPLY_504_PRIOR_FIT_DATES_504_AUDIT_DATES_ONE_SESSION_EMBARGO_AND_FOUR_INNER_BLOCKS_WITH_252_MINIMUM_FIT_DATES", "outer_folds": 8, "outer_dates": 63, "inner_folds": 4, "inner_dates": 63, "minimum_outer_fit_dates": 504, "minimum_inner_fit_dates": 252, "embargo_sessions": 1, "maximum_label_minutes": 60},
        "historical_interval": {"start_inclusive": "2018-01-02T00:00:00Z", "end_exclusive": "2025-07-13T22:00:00Z"},
        "sources": {"one_minute": "SPARSE_PRINTED_BARS_EXPLICITLY_MASKED_NO_FILL", "one_second": "SPARSE_PRINTED_BARS_REQUIRED_COMPLETE_ONLY_FOR_REALIZED_ADVERSE_PATH", "calendar": "DBN_EMPIRICAL_OBSERVABILITY_SUCCESSOR"},
        "costs_usd_per_trade": {"gross": 0.0, "base": 2.5, "stress": 5.0, "extreme": 10.0},
        "inference": {"synchronized_exchange_dates": True, "stationary_bootstrap_shared_indices": True, "familywise_threshold_individual": 0.05 / 168, "familywise_threshold_portfolio": 0.05 / 169, "mees_usd": 10.0, "candidate_array_order": "CAMPAIGN_EXCHANGE_SESSION_DATE_MARKET_CANDIDATE"},
        "monitoring": {"append_only_hash_chain": True, "inferential_state": "BLOCKED_MISSING_FINITE_SESSION_PNL_BOUND", "promotion_forbidden": True},
        "bindings": {
            "catalog": _binding(root, CATALOG),
            "implementation": {
                path.as_posix(): sha256_file(root / path)
                for path in (
                    Path("src/futures_rebuild/intraday_alpha_pipeline_v12.py"),
                    Path("src/futures_rebuild/intraday_alpha_execution_v12.py"),
                    Path("src/futures_rebuild/research/splits.py"),
                    Path("src/futures_rebuild/research/bootstrap.py"),
                    Path("src/futures_rebuild/research/hac.py"),
                    Path("src/futures_rebuild/research/cscv.py"),
                    Path("src/futures_rebuild/research/multiple_testing.py"),
                    Path("scripts/run_intraday_synthetic_certification_v12.py"),
                    Path("scripts/independent_validate_intraday_alpha_pipeline_v12.py"),
                )
            },
        },
        "authority": {"rows": False, "provider": False, "activation": False, "holdout": False, "forward": False, "trading": False},
    }
    return _identified(core, "program_id")


def _es_flow_candidates() -> list[dict[str, object]]:
    flow = next(program for program in enumerate_programs() if program["program_name"] == "FLOW_CONTINUATION")
    candidates = [candidate for candidate in flow["candidates"] if candidate["holding_horizon_minutes"] in ES_FLOW_HORIZONS]
    expected = {
        (return_z, volume_z, horizon)
        for return_z in (1.5, 2.0)
        for volume_z in (0.5, 1.0)
        for horizon in ES_FLOW_HORIZONS
    }
    if len(candidates) != 8 or {
        (candidate["return_z_threshold"], candidate["volume_surprise_threshold"], candidate["holding_horizon_minutes"])
        for candidate in candidates
    } != expected:
        raise IntegrityError("frozen eight-candidate ES rescue family changed")
    return candidates


def build_es_flow_economics(root: Path) -> dict[str, object]:
    """Resolve MES economics from hash-bound project profiles, never caller input."""

    root = root.resolve(strict=True)
    mapping_document = _read_json(root / MES_MAPPING)
    cost_document = _read_json(root / MES_COSTS)
    risk_document = _read_json(root / MES_RISK)
    try:
        mapping_id = str(mapping_document["active_mapping_id"])
        mapping = mapping_document["mappings"][mapping_id]  # type: ignore[index]
        validate_instrument_mapping(mapping)  # type: ignore[arg-type]
        execution = resolve_execution_instrument(mapping, "ES")  # type: ignore[arg-type]
        cost = cost_document["cost_profiles"][MES_COST_PROFILE_ID]  # type: ignore[index]
        validate_execution_cost_profile(cost)  # type: ignore[arg-type]
        risk_id = str(risk_document["active_profile_id"])
        profile = risk_document["profiles"][risk_id]  # type: ignore[index]
        limits = profile["project_limits"]  # type: ignore[index]
        multiplier = Decimal(str(execution["contract_multiplier"]))
        tick_size = Decimal(str(execution["tick_size"]))
        tick_value = Decimal(str(execution["tick_value_usd"]))
        stress_cost = Decimal(str(cost["round_turn_commission_usd"]["MES"])) + Decimal(str(cost["expected_slippage_usd"]["MES"]))  # type: ignore[index]
        initial_risk = Decimal(str(limits["maximum_initial_risk_usd"]))  # type: ignore[index]
        daily_stop = Decimal(str(limits["daily_stop_loss_usd"]))  # type: ignore[index]
        total_drawdown = Decimal(str(limits["maximum_total_drawdown_usd"]))  # type: ignore[index]
        micros = int(limits["maximum_standard_contract_equivalents"]) * int(profile["firm_limits"]["micro_contracts_per_standard_contract"])  # type: ignore[index]
    except Exception as exc:
        raise IntegrityError("MES execution economics or project risk profile is incomplete") from exc
    if (
        execution.get("execution_symbol") != "MES"
        or (multiplier, tick_size, tick_value) != (Decimal("5"), Decimal("0.25"), Decimal("1.25"))
        or stress_cost <= 0
        or cost.get("provisional") is not True  # type: ignore[union-attr]
        or cost.get("research_proxy_allowed") is not True  # type: ignore[union-attr]
        or (initial_risk, daily_stop, total_drawdown, micros) != (Decimal("250"), Decimal("500"), Decimal("1500"), 10)
    ):
        raise IntegrityError("frozen MES mapping, provisional costs, or project limits changed")
    core = {
        "signal_symbol": "ES",
        "execution_symbol": "MES",
        "evidence_designation": "MES_EXECUTION_PROXY",
        "mapping_id": mapping_id,
        "cost_profile_id": MES_COST_PROFILE_ID,
        "risk_profile_id": risk_id,
        "contract_multiplier_usd_per_point": float(multiplier),
        "tick_size": float(tick_size),
        "tick_value_usd": float(tick_value),
        "stress_round_trip_cost_usd_per_contract": float(stress_cost),
        "maximum_initial_risk_usd": float(initial_risk),
        "daily_stop_loss_usd": float(daily_stop),
        "maximum_total_drawdown_usd": float(total_drawdown),
        "maximum_micro_contracts": micros,
        "bindings": {
            "mapping": _binding(root, MES_MAPPING),
            "cost": _binding(root, MES_COSTS),
            "risk": _binding(root, MES_RISK),
        },
    }
    return _identified(core, "economics_id")


def _es_active_source_binding(root: Path, boundary: RepoBoundary, year: int) -> dict[str, object]:
    parquet_relative = Path("data/active/causally_gated_normalized/ES") / str(year) / f"{year}.parquet"
    sidecar_relative = Path(f"{parquet_relative.as_posix()}.manifest.json")
    parquet = boundary.assert_active_path(root / parquet_relative, purpose="ES rescue Parquet", subtree="data/active")
    sidecar = boundary.assert_active_path(root / sidecar_relative, purpose="ES rescue sidecar", subtree="data/active")
    manifest = _read_json(sidecar)
    _verified_identity(manifest, "sidecar_id")
    access = manifest.get("access_policy_binding")
    entry = manifest.get("entry_binding")
    if not isinstance(access, Mapping) or not isinstance(entry, Mapping):
        raise IntegrityError("ES rescue active source manifest is incomplete")
    source_bindings = entry.get("source_bindings")
    if (
        manifest.get("schema_version") != "causal_active_market_year_manifest/1.0.0"
        or access.get("market") != "ES"
        or access.get("year") != year
        or access.get("capability") != "RESEARCH_READY_CAUSAL_PRICE"
        or access.get("selection_eligible") is not True
        or "DISCOVERY_RESEARCH" not in access.get("permitted_uses", [])
        or entry.get("market") != "ES"
        or entry.get("year") != year
        or entry.get("disposition") != "RESEARCH_READY_CAUSAL_PRICE"
        or entry.get("parquet_path") != parquet_relative.as_posix()
        or not isinstance(source_bindings, list)
        or not source_bindings
    ):
        raise IntegrityError("ES rescue source is not an eligible hash-bound active view")
    parquet_sha = str(entry.get("parquet_sha256", ""))
    if len(parquet_sha) != 64 or not parquet.is_file() or sha256_file(parquet) != parquet_sha:
        raise IntegrityError("ES rescue active Parquet hash changed")
    return {
        "market": "ES",
        "year": year,
        "parquet_path": parquet_relative.as_posix(),
        "parquet_sha256": parquet_sha,
        "sidecar": _binding(root, sidecar_relative),
        "sidecar_id": manifest.get("sidecar_id"),
        "active_view_id": access.get("active_view_id"),
        "source_binding_count": len(source_bindings),
    }


def build_es_flow_screen_program(root: Path) -> dict[str, object]:
    """Bind the eight-candidate ES-to-MES rescue without opening a data row."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    inputs = [_es_active_source_binding(root, boundary, year) for year in ES_FLOW_SCREEN_YEARS]
    core = {
        "schema_version": PROGRAM_SCHEMA,
        "state": "PRE_RESULT_PREPARATION_ONLY_NOT_ACTIVE",
        "stage": ES_FLOW_SCREEN_STAGE,
        "hypothesis": "An unusually large, high-volume 15-minute ES move predicts continuation after entry over either 15 or 60 minutes.",
        "markets": ["ES"],
        "historical_interval": {
            "source_years": list(ES_FLOW_SCREEN_YEARS),
            "screen_years": list(ES_FLOW_DEVELOPMENT_YEARS),
            "wfa_years": list(ES_FLOW_AUDIT_YEARS),
            "excluded_after_refit": [2023, 2024, 2025],
            "all_historical_data_previously_exposed": True,
        },
        "candidates": _es_flow_candidates(),
        "candidate_count": 8,
        "research_family_accounting": {
            "family": "ES_HIGH_VOLUME_15_MINUTE_MOVE_CONTINUATION",
            "member_count": 8,
            "conservative_cumulative_trial_count": 168,
            "counting_rule": "EIGHT_MEMBERS_ALREADY_INCLUDED_IN_THE_PRIOR_DECLARED_36_CANDIDATES_NO_DOUBLE_COUNT",
            "known_prior_es_attempt": _binding(root, ES_PRIOR_TRIAL),
        },
        "standardization": {
            "lookback_complete_sessions": 63,
            "same_minute_of_session": True,
            "return_window_minutes": 15,
            "volume_transform": "log1p",
            "sample_standard_deviation_ddof": 1,
            "volume_reference_same_actual_contract_only": True,
            "minimum_matching_prior_sessions": 20,
        },
        "timing": {
            "timezone": "America/Chicago",
            "decision_delay_seconds": 65,
            "entry_rule": "FIRST_BAR_START_STRICTLY_AFTER_DECISION",
            "holding_horizon_minutes": list(ES_FLOW_HORIZONS),
            "shared_latest_entry_chicago": "13:55",
            "minimum_completed_causal_bars_before_event": 20,
            "contract_identity_continuity": True,
        },
        "selection": "FIRST_QUALIFYING_EVENT_PER_SESSION_PER_THRESHOLD_SHARED_BY_BOTH_HORIZONS",
        "execution": {
            "protective_stop": "1.5_X_CAUSAL_ATR20_ROUNDED_OUTWARD_TO_MES_TICK",
            "maximum_entries_per_session": 1,
            "cost_sensitivities_usd_per_contract": list(ES_FLOW_COSTS),
            "economics": build_es_flow_economics(root),
        },
        "outcomes": {
            "window": "ENTRY_THROUGH_ACTUAL_STOP_OR_TIME_EXIT_INCLUSIVE_OF_EXIT_FILL",
            "stop_bar_order": "STOP_BEFORE_ANY_SAME_BAR_FAVORABLE_EXCURSION",
            "gap_stop_fill": "ADVERSE_BAR_OPEN",
            "raw_mfe_mae": "MFE/MAX(MAE,ONE_MES_TICK)",
            "net_mfe_mae": "MAX(MFE-ROUND_TRIP_COST,0)/MAX(MAE+ROUND_TRIP_COST,ONE_MES_TICK)",
            "reported_units": ["USD", "PLANNED_INITIAL_RISK_MULTIPLE"],
            "mfe_mae_forbidden_as_features": True,
        },
        "ridge": {
            "implementation": "NUMPY_FIXED_MULTI_OUTPUT_RIDGE",
            "penalty": 1.0,
            "features": list(ES_FLOW_FEATURES),
            "targets": ["terminal_net_pnl_r", "mfe_r", "mae_r"],
            "terminal_prediction_floor_r": 0.25,
            "predicted_net_mfe_mae_strictly_greater_than": 1.0,
            "fit_only_standardization": True,
        },
        "screen_gate": {
            "minimum_opportunities": 60,
            "positive_stress_mes_proxy_pnl": True,
            "minimum_positive_stress_years": 2,
            "median_realized_net_mfe_mae_strictly_greater_than": 1.0,
            "passing_action": "CONTINUE_TO_HISTORICAL_WFA",
            "failing_action": "STOP_NO_ECONOMIC_EDGE",
            "passing_scope": "ALL_EIGHT_DECLARED_CANDIDATES_NO_SCREEN_SELECTION",
        },
        "wfa": {
            "outer_folds": 8,
            "audit_sessions_per_fold": 63,
            "inner_folds": 0,
            "bootstrap_resamples": ES_FLOW_BOOTSTRAP_RESAMPLES,
            "bootstrap_seed": ES_FLOW_BOOTSTRAP_SEED,
            "mean_block_sessions": ES_FLOW_BOOTSTRAP_BLOCK,
            "baselines": list(ES_FLOW_BASELINES),
            "romano_wolf_complete_family_contrasts": 8 * len(ES_FLOW_BASELINES),
            "qualification_gate": {
                "minimum_trades": 100,
                "primary_and_all_adjusted_lower_bounds_above_zero": True,
                "minimum_positive_folds": 5,
                "both_audit_years_positive": True,
                "median_realized_net_mfe_mae_above_one": True,
                "top_five_positive_pnl_share_maximum": 0.5,
                "single_year_positive_pnl_share_maximum": 0.7,
                "ridge_must_improve_unfiltered": True,
            },
        },
        "forward_confirmation": {
            "paper_only": True,
            "start": "FIRST_COMPLETE_SESSION_STRICTLY_AFTER_FROZEN_ARTIFACT",
            "required_execution_data": "MES_TIMESTAMPED_EXECUTABLE_BID_ASK_OR_EQUIVALENT",
            "minimum_trades": 100,
            "minimum_calendar_months": 6,
            "maximum_trades_if_inconclusive": 200,
            "maximum_calendar_months_if_inconclusive": 12,
            "pass_gate": {
                "observed_cost_net_pnl_positive": True,
                "bootstrap_lower_bound_above_zero": True,
                "positive_completed_month_fraction": "AT_LEAST_TWO_THIRDS",
                "median_realized_net_mfe_mae_above_one": True,
                "top_five_positive_pnl_share_maximum": 0.5,
                "scheduled_session_feed_coverage_minimum": 0.95,
            },
            "provider_access_requires_separate_approval": True,
        },
        "sources": inputs,
        "source_policy": "ACTIVE_ES_PARQUET_ONLY_NO_RAW_DBN_NO_FILL",
        "bindings": {
            "implementation": {
                path.as_posix(): sha256_file(root / path)
                for path in (
                    Path("src/futures_rebuild/intraday_alpha_pipeline_v12.py"),
                    Path("src/futures_rebuild/intraday_alpha_execution_v12.py"),
                )
            }
        },
        "evidence_policy": {"historical_pass": "READY_FOR_FORWARD_PAPER_TEST", "forward_pass": "FORWARD_PAPER_VALIDATED", "historical_confirmation_forbidden": True},
        "performance": {
            "screen_maximum_seconds": 900,
            "wfa_maximum_seconds": 1800,
            "maximum_ram_bytes": 8 * 1024**3,
            "overrun_action": "PROFILE_EXISTING_ARRAY_PATH_NO_REDUCED_STATISTICS_NO_DISTRIBUTED_PROJECT",
        },
        "authority": {"rows": False, "provider": False, "wfa": False, "portfolio": False, "holdout": False, "forward": False, "trading": False},
    }
    return _identified(core, "program_id")


def prepare_es_flow_screen(root: Path) -> dict[str, object]:
    program = build_es_flow_screen_program(root)
    path = PROGRAM_ROOT / str(program["program_id"]) / "program.json"
    write_new_canonical_authority(root.resolve(strict=True) / path, program)
    registration = build_es_flow_registration(root, program)
    registration_path = ES_FLOW_TRIAL_ROOT / f"{registration['trial_id']}.json"
    write_new_canonical_authority(root.resolve(strict=True) / registration_path, registration)
    return {
        "program_id": program["program_id"],
        "program_path": path.as_posix(),
        "trial_id": registration["trial_id"],
        "registration_path": registration_path.as_posix(),
        "registration_sha256": sha256_file(root / registration_path),
        "status": "PREPARED_NOT_ACTIVE_REAL_DATA_EVALUATION_REQUIRES_SEPARATE_APPROVAL",
    }


def build_es_flow_registration(
    root: Path, program: Mapping[str, object]
) -> dict[str, object]:
    """Register the frozen rescue in the existing trial ledger without granting access."""

    root = root.resolve(strict=True)
    if dict(program) != build_es_flow_screen_program(root):
        raise IntegrityError("ES flow registration requires the current frozen program")
    contract, profile = load_active_ladder(root)
    governance = scientific_governance_binding(root)
    readiness = {
        "program_id": program["program_id"],
        "sources_sha256": sha256_json(program["sources"]),
        "implementation_sha256": sha256_json(program["bindings"]["implementation"]),
        "historical_interval_sha256": sha256_json(program["historical_interval"]),
    }
    core = {
        "schema_version": "es_flow_rescue_trial_registration/1.0.0",
        "state": "REGISTERED_NOT_CLAIMED_NOT_EXECUTED",
        "trial_family": program["research_family_accounting"]["family"],
        "protocol_id": program["program_id"],
        "program": {
            "program_id": program["program_id"],
            "path": (PROGRAM_ROOT / str(program["program_id"]) / "program.json").as_posix(),
            "sha256": sha256(canonical_bytes(program) + b"\n").hexdigest(),
        },
        "readiness": {**readiness, "readiness_id": sha256_json(readiness)},
        "alpha_ladder_binding": {
            "contract_id": contract["contract_id"],
            "profile_id": profile["profile_id"],
            "stage": "pilot",
        },
        "scientific_governance_binding": governance,
        "predecessor": program["research_family_accounting"]["known_prior_es_attempt"],
        "candidate_ids": [candidate["candidate_id"] for candidate in program["candidates"]],
        "authority": {
            "historical_rows": False,
            "provider_or_network": False,
            "holdout_or_forward": False,
            "publication": False,
            "trading": False,
        },
    }
    return _identified(core, "trial_id")


def build_source_registration(root: Path, program: Mapping[str, object]) -> dict[str, object]:
    """Bind the exact source inventory without opening any DBN payload."""

    source_contract = _read_json(root / SOURCE_CONTRACT)
    complete = source_contract.get("complete_inventory")
    active_source = source_contract.get("active_canonical_source")
    if not isinstance(complete, Mapping) or not isinstance(active_source, Mapping):
        raise IntegrityError("active source contract is incomplete")
    inventory_path = Path(str(complete.get("path", "")))
    inventory = _read_json(root / inventory_path)
    if (
        complete.get("sha256") != sha256_file(root / inventory_path)
        or complete.get("inventory_id") != inventory.get("inventory_id")
        or active_source.get("pointer_sha256") != sha256_file(root / DBN_POINTER)
    ):
        raise IntegrityError("source contract inventory or active pointer binding changed")
    markets = {"ES", "NQ", "CL", "GC", "ZN", "ZB"}
    families = {"definition", "ohlcv_1m", "ohlcv_1s"}
    selected = [
        row for row in inventory.get("entries", [])
        if isinstance(row, dict)
        and row.get("admitted_standard_foundation") is True
        and row.get("market") in markets
        and row.get("family") in families
        and row.get("year") in range(2018, 2026)
        and row.get("kind") in {"DBN", "SIDECAR"}
    ]
    pairs: list[dict[str, object]] = []
    grouped: dict[tuple[str, str, int], dict[str, dict[str, object]]] = {}
    for row in selected:
        grouped.setdefault((str(row["market"]), str(row["family"]), int(row["year"])), {})[str(row["kind"])] = row
    for key in sorted(grouped):
        pair = grouped[key]
        if set(pair) != {"DBN", "SIDECAR"}:
            raise IntegrityError("registered DBN/sidecar pair is incomplete")
        dbn, sidecar = pair["DBN"], pair["SIDECAR"]
        if (
            sidecar["path"] != f"{dbn['path']}.manifest.json"
            or any(dbn[name] != sidecar[name] for name in ("market", "family", "year", "interval_start_inclusive", "interval_end_exclusive"))
        ):
            raise IntegrityError("registered DBN/sidecar identities differ")
        for row in (dbn, sidecar):
            path = root / str(row["path"])
            if not path.is_file() or path.stat().st_size != row["size_bytes"]:
                raise IntegrityError("registered source path or size changed")
        if sha256_file(root / str(sidecar["path"]), reject_hardlinks=False) != sidecar["sha256"]:
            raise IntegrityError("registered sidecar hash changed")
        pairs.append({"market": key[0], "family": key[1], "year": key[2], "dbn": dbn, "sidecar": sidecar})
    if len(pairs) != 144 or sum(int(pair["dbn"]["size_bytes"]) for pair in pairs) < 3_000_000_000:
        raise IntegrityError("exact 144-DBN historical source scope is unavailable")
    bindings = {
        path.as_posix(): _binding(root, path)
        for path in (DBN_POINTER, SOURCE_CONTRACT, inventory_path, CATALOG, HISTORICAL_CALENDAR_POINTER, ECONOMICS_RULEBOOK)
    }
    core = {
        "schema_version": SOURCE_REGISTRATION_SCHEMA,
        "state": "METADATA_BOUND_ROW_COMPLETENESS_AUDIT_REQUIRED",
        "program_id": program["program_id"],
        "source_release": {
            "release_id": active_source["release_id"],
            "release_manifest_sha256": active_source["release_manifest_sha256"],
            "inventory_sha256": inventory["content_inventory_sha256"],
        },
        "historical_interval": {"start_inclusive": "2018-01-02T00:00:00Z", "end_exclusive": "2025-07-13T22:00:00Z"},
        "calendar_policy": "DBN_EMPIRICAL_OBSERVABILITY_SUCCESSOR",
        "markets": sorted(markets),
        "source_pairs": pairs,
        "dbn_count": 144,
        "sidecar_count": 144,
        "dbn_size_bytes": sum(int(pair["dbn"]["size_bytes"]) for pair in pairs),
        "bindings": bindings,
        "row_completeness": {"status": "BLOCKED_PENDING_SEPARATELY_APPROVED_SCAN", "certificate": None, "calendar_successor": None},
        "requirements": {
            "printed_ohlcv_is_sparse": True,
            "minute_session_boundaries_chicago": ["08:30:00", "15:00:00"],
            "second_session_span_chicago": ["08:30:00", "14:59:00"],
            "missing_minute_policy": "EXPLICIT_UNOBSERVED_MASK_ABSTAIN_NO_FILL",
            "missing_required_adverse_second_policy": "TAIL_INCOMPLETE_BLOCK_PROMOTION",
            "incomplete_session_policy": "EXCLUDE_BEFORE_FOLDS_NEVER_ZERO_RETURN",
            "minimum_common_sessions": 1010,
            "fallback": False,
        },
        "authority": {"rows": False, "provider": False, "holdout": False, "forward": False, "evaluation": False, "activation": False, "trading": False},
    }
    return _identified(core, "registration_id")


def build_certified_source_registration(
    root: Path, predecessor: Mapping[str, object], certificate_path: Path, calendar_path: Path
) -> dict[str, object]:
    """Create, never edit, the row-certified successor to a metadata registration."""

    _verified_identity(predecessor, "registration_id")
    certificate = _read_json(root / certificate_path)
    calendar = _read_json(root / calendar_path)
    if (
        predecessor.get("state") != "METADATA_BOUND_ROW_COMPLETENESS_AUDIT_REQUIRED"
        or certificate.get("schema_version") != ROW_COMPLETENESS_SCHEMA
        or certificate.get("status") != "PASS_COMPLETE"
        or certificate.get("source_registration_id") != predecessor.get("registration_id")
        or certificate.get("common_exchange_session_dates_sha256") != sha256_json(calendar.get("common_exchange_session_dates"))
        or len(calendar.get("common_exchange_session_dates", ())) < 1010
    ):
        raise IntegrityError("row-completeness evidence cannot certify the source registration")
    core = {
        **{key: value for key, value in predecessor.items() if key not in {"registration_id", "state", "row_completeness"}},
        "state": "ROW_COMPLETE_CERTIFIED_NOT_ACTIVE",
        "predecessor_registration_id": predecessor["registration_id"],
        "row_completeness": {
            "status": "COMPLETE",
            "certificate": {"certificate_id": certificate["certificate_id"], **_binding(root, certificate_path)},
            "calendar_successor": {"calendar_id": calendar["calendar_id"], **_binding(root, calendar_path)},
        },
    }
    return _identified(core, "registration_id")


@dataclass(frozen=True)
class CausalBar:
    market: str
    exchange_session_date: str
    event_at: datetime
    available_at: datetime
    actual_identity: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    tick_size: float
    tick_value: float
    campaign_id: int = 0


@dataclass(frozen=True)
class CausalSecondBar:
    market: str
    exchange_session_date: str
    event_at: datetime
    available_at: datetime
    actual_identity: str
    open: float
    high: float
    low: float
    close: float
    campaign_id: int = 0


@dataclass(frozen=True)
class MinuteBarCube:
    """Validated campaign x date x market x minute causal observations."""

    exchange_session_dates: tuple[str, ...]
    markets: tuple[str, ...]
    identity_labels: tuple[str, ...]
    event_at_ns: np.ndarray
    available_at_ns: np.ndarray
    identity_code: np.ndarray
    observed: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    tick_size: np.ndarray
    tick_value: np.ndarray

    @property
    def campaign_count(self) -> int:
        return int(self.open.shape[0])

    def validate(self) -> None:
        expected = (self.campaign_count, len(self.exchange_session_dates), len(self.markets), 391)
        integer_arrays = (self.event_at_ns, self.available_at_ns, self.identity_code)
        real_arrays = (self.open, self.high, self.low, self.close, self.volume, self.tick_size, self.tick_value)
        if self.campaign_count < 1 or self.observed.shape != expected or any(value.shape != expected for value in (*integer_arrays, *real_arrays)):
            raise IntegrityError("minute cube arrays are not campaign x date x market x 391")
        if self.event_at_ns.dtype != np.dtype(np.int64) or self.available_at_ns.dtype != np.dtype(np.int64) or self.identity_code.dtype != np.dtype(np.int32):
            raise IntegrityError("minute cube integer dtypes are invalid")
        if self.observed.dtype != np.dtype(np.bool_):
            raise IntegrityError("minute cube observation mask is invalid")
        if any(value.dtype != np.dtype(np.float64) or np.any(~np.isfinite(value)) for value in real_arrays):
            raise IntegrityError("minute cube real arrays must be finite float64")
        if np.any(self.available_at_ns < self.event_at_ns + 60_000_000_000) or np.any(self.tick_size <= 0) or np.any(self.tick_value <= 0) or np.any(self.volume < 0):
            raise IntegrityError("minute cube availability or economics are invalid")
        if np.any(self.low > np.minimum(self.open, self.close)) or np.any(self.high < np.maximum(self.open, self.close)):
            raise IntegrityError("minute cube OHLC values are malformed")
        if not self.identity_labels or np.any(self.identity_code < 0) or np.any(self.identity_code >= len(self.identity_labels)):
            raise IntegrityError("minute cube identity codes are invalid")
        spacing = np.diff(self.event_at_ns, axis=3)
        if np.any(spacing != 60_000_000_000):
            raise IntegrityError("minute cube contains a missing or duplicate minute")
        if (
            not self.exchange_session_dates
            or tuple(sorted(set(self.exchange_session_dates))) != self.exchange_session_dates
            or not self.markets
            or len(set(self.markets)) != len(self.markets)
        ):
            raise IntegrityError("minute cube date or market identities are not canonical")
        expected_starts = np.empty(len(self.exchange_session_dates), dtype=np.int64)
        for index, raw_date in enumerate(self.exchange_session_dates):
            try:
                start = datetime.fromisoformat(raw_date).replace(hour=8, minute=30, tzinfo=CHICAGO)
            except (TypeError, ValueError) as exc:
                raise IntegrityError("exchange-session date is malformed") from exc
            expected_starts[index] = _datetime_ns(start)
        if np.any(self.event_at_ns[..., 0] != expected_starts[None, :, None]):
            raise IntegrityError("minute cube is not on the certified 08:30-15:00 Chicago session grid")


def _datetime_ns(value: datetime) -> int:
    utc = _chicago(value, "timestamp").astimezone(timezone.utc)
    return int(utc.timestamp()) * 1_000_000_000 + utc.microsecond * 1_000


def _chicago(value: datetime, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise IntegrityError(f"{name} must be timezone-aware")
    return value.astimezone(CHICAGO)


def _schedule(dates: Sequence[str]) -> tuple[object, ...]:
    return _schedule_cached(tuple(dates))


@lru_cache(maxsize=8)
def _schedule_cached(ordered: tuple[str, ...]) -> tuple[object, ...]:
    if len(ordered) < 1010 or len(set(ordered)) != len(ordered) or ordered != tuple(sorted(ordered)):
        raise IntegrityError("1,010 complete dates are required by the frozen fit minima and one-session embargo")
    n = len(ordered)
    decision = np.arange(n, dtype=np.int64) * 1440 + 600
    samples = TemporalSamples(decision, decision.copy(), decision + 60, decision + 60)
    outer_start = n - 504
    outer = [SessionWindow((outer_start + 63 * fold) * 1440, (outer_start + 63 * (fold + 1)) * 1440) for fold in range(8)]
    inner = []
    for window in outer:
        end_session = window.start // 1440 - 1
        inner.append(tuple(SessionWindow((end_session - 252 + 63 * fold) * 1440, (end_session - 189 + 63 * fold) * 1440) for fold in range(4)))
    folds = nested_chronological_splits(samples, outer, inner, session_embargo=1440, minimum_fit_samples=252, minimum_audit_samples=63)
    if any(len(fold.fit_indices) < 504 or len(fold.audit_indices) != 63 for fold in folds):
        raise IntegrityError("frozen outer schedule minima failed")
    return folds


def _split_records(dates: Sequence[str], folds: Sequence[object]) -> list[dict[str, object]]:
    records = []
    for outer_id, fold in enumerate(folds):
        core = {
            "outer_fold": outer_id,
            "fit_dates": [dates[int(index)] for index in fold.fit_indices],
            "audit_dates": [dates[int(index)] for index in fold.audit_indices],
            "inner": [
                {
                    "inner_fold": inner_id,
                    "fit_dates": [dates[int(index)] for index in inner.fit_indices],
                    "audit_dates": [dates[int(index)] for index in inner.audit_indices],
                }
                for inner_id, inner in enumerate(fold.inner_folds)
            ],
            "purge_minutes": 60,
            "embargo_exchange_sessions": 1,
        }
        records.append({**core, "split_id": sha256_json(core)})
    return records


def validate_causal_bars(
    bars: Sequence[CausalBar], required_markets: Sequence[str], *, require_wfa_schedule: bool = True
) -> tuple[list[CausalBar], tuple[str, ...]]:
    if not bars:
        raise IntegrityError("bar cube is empty")
    seen: set[tuple[int, str, str, datetime]] = set()
    grouped: dict[tuple[int, str], dict[str, list[datetime]]] = {}
    checked: list[CausalBar] = []
    for bar in bars:
        event = _chicago(bar.event_at, "event_at")
        available = _chicago(bar.available_at, "available_at")
        if available < event + timedelta(seconds=60) or not bar.actual_identity:
            raise IntegrityError("bar availability or actual-contract identity is invalid")
        if type(bar.campaign_id) is not int or bar.campaign_id < 0:
            raise IntegrityError("campaign identity is invalid")
        if bar.market not in required_markets:
            raise IntegrityError("bar market escapes the frozen program scope")
        if any(not np.isfinite(value) for value in (bar.open, bar.high, bar.low, bar.close, bar.volume, bar.tick_size, bar.tick_value)) or bar.volume < 0 or bar.tick_size <= 0 or bar.tick_value <= 0 or bar.low > min(bar.open, bar.close) or bar.high < max(bar.open, bar.close):
            raise IntegrityError("bar values are malformed")
        key = (bar.campaign_id, bar.exchange_session_date, bar.market, event)
        if key in seen:
            raise IntegrityError("duplicate minute bar")
        seen.add(key)
        grouped.setdefault((bar.campaign_id, bar.exchange_session_date), {}).setdefault(bar.market, []).append(event)
        checked.append(bar)
    scope = set(required_markets)
    campaigns = sorted({bar.campaign_id for bar in checked})
    if campaigns != list(range(len(campaigns))):
        raise IntegrityError("campaign identities must be contiguous from zero")
    complete_by_campaign: list[set[str]] = []
    for campaign in campaigns:
        complete: set[str] = set()
        for (observed_campaign, date), by_market in grouped.items():
            if observed_campaign != campaign or set(by_market) != scope:
                continue
            grids = []
            for market in sorted(scope):
                times = sorted(by_market[market])
                if not times or (times[0].hour, times[0].minute, times[0].second) != (8, 30, 0) or (times[-1].hour, times[-1].minute, times[-1].second) != (15, 0, 0):
                    break
                if any(event.second or event.microsecond for event in times) or any(right <= left for left, right in zip(times, times[1:])):
                    break
                grids.append((times[0], times[-1]))
            else:
                if len(set(grids)) == 1:
                    complete.add(date)
        complete_by_campaign.append(complete)
    common = set.intersection(*complete_by_campaign) if complete_by_campaign else set()
    dates = tuple(sorted(common))
    if require_wfa_schedule:
        _schedule(dates)
    date_scope = set(dates)
    retained = [bar for bar in checked if bar.exchange_session_date in date_scope]
    return sorted(retained, key=lambda row: (row.campaign_id, row.exchange_session_date, row.market, row.event_at)), dates


def validate_causal_seconds(
    bars: Sequence[CausalSecondBar],
    *,
    required_markets: Sequence[str],
    campaign_count: int,
    dates: Sequence[str],
) -> tuple[CausalSecondBar, ...]:
    seen: set[tuple[int, str, str, datetime]] = set()
    checked: list[CausalSecondBar] = []
    date_scope = set(dates)
    market_scope = set(required_markets)
    for bar in bars:
        event = _chicago(bar.event_at, "second.event_at")
        available = _chicago(bar.available_at, "second.available_at")
        if bar.campaign_id not in range(campaign_count) or bar.exchange_session_date not in date_scope or bar.market not in market_scope:
            raise IntegrityError("one-second bar escapes the materialized cube")
        if available < event + timedelta(seconds=1) or event.microsecond or not bar.actual_identity:
            raise IntegrityError("one-second availability or identity is invalid")
        if any(not np.isfinite(value) for value in (bar.open, bar.high, bar.low, bar.close)) or bar.low > min(bar.open, bar.close) or bar.high < max(bar.open, bar.close):
            raise IntegrityError("one-second values are malformed")
        key = (bar.campaign_id, bar.exchange_session_date, bar.market, event)
        if key in seen:
            raise IntegrityError("duplicate one-second bar")
        seen.add(key)
        checked.append(bar)
    return tuple(sorted(checked, key=lambda row: (row.campaign_id, row.exchange_session_date, row.market, row.event_at)))


def _minute_cube(
    rows: Sequence[CausalBar],
    *,
    dates: Sequence[str],
    markets: Sequence[str],
    campaign_count: int,
) -> MinuteBarCube:
    shape = (campaign_count, len(dates), len(markets), 391)
    event_rows = _synthetic_event_rows(tuple(dates))
    event = np.broadcast_to(event_rows[None, :, None, :], shape).copy()
    available = event + 60_000_000_000
    observed = np.zeros(shape, dtype=np.bool_)
    labels = ("__UNOBSERVED__", *sorted({row.actual_identity for row in rows}))
    identity = np.zeros(shape, dtype=np.int32)
    arrays = [np.zeros(shape, dtype=np.float64) for _ in range(5)] + [np.ones(shape, dtype=np.float64) for _ in range(2)]
    identity_index = {label: index for index, label in enumerate(labels)}
    date_index = {date: index for index, date in enumerate(dates)}
    market_index = {market: index for index, market in enumerate(markets)}
    for row in rows:
        campaign = row.campaign_id
        date = date_index[row.exchange_session_date]
        market = market_index[row.market]
        chicago = _chicago(row.event_at, "event_at")
        minute = (chicago.hour * 60 + chicago.minute) - (8 * 60 + 30)
        if minute not in range(391) or observed[campaign, date, market, minute]:
            raise IntegrityError("minute cube session contains a duplicate or out-of-grid row")
        available[campaign, date, market, minute] = _datetime_ns(row.available_at)
        identity[campaign, date, market, minute] = identity_index[row.actual_identity]
        for target, value in zip(arrays, (row.open, row.high, row.low, row.close, row.volume, row.tick_size, row.tick_value), strict=True):
            target[campaign, date, market, minute] = value
        observed[campaign, date, market, minute] = True
    cube = MinuteBarCube(tuple(dates), tuple(markets), labels, event, available, identity, observed, *arrays)
    cube.validate()
    return cube


def _second_paths(materialized: Mapping[str, object]) -> dict[tuple[int, int, str], tuple[CausalSecondBar, ...]]:
    dates = tuple(materialized["exchange_session_dates"])
    date_index = {date: index for index, date in enumerate(dates)}
    paths: dict[tuple[int, int, str], list[CausalSecondBar]] = {}
    for row in materialized["one_second_bars"]:
        if not isinstance(row, CausalSecondBar):
            raise IntegrityError("materialized one-second rows are invalid")
        paths.setdefault((row.campaign_id, date_index[row.exchange_session_date], row.market), []).append(row)
    return {key: tuple(sorted(rows, key=lambda row: row.event_at)) for key, rows in paths.items()}


def _backward_asof_bar(
    path: Sequence[CausalBar], *, leader_at: datetime, decision_at: datetime
) -> tuple[int, CausalBar] | None:
    leader = _chicago(leader_at, "leader.event_at")
    decision = _chicago(decision_at, "decision_at")
    eligible = [(index, row) for index, row in enumerate(path) if _chicago(row.event_at, "follower.event_at") <= leader]
    if not eligible:
        return None
    index, row = eligible[-1]
    if leader - _chicago(row.event_at, "follower.event_at") > timedelta(seconds=60) or _chicago(row.available_at, "follower.available_at") > decision:
        return None
    return index, row


def _adverse_path(
    seconds: Mapping[tuple[int, int, str], Sequence[CausalSecondBar]],
    *, campaign: int,
    date_index: int,
    market: str,
    identity: str,
    entry_at: datetime,
    exit_at: datetime,
    entry_price: float,
    direction: int,
    tick_size: float,
    tick_value: float,
) -> float:
    rows = [row for row in seconds.get((campaign, date_index, market), ()) if entry_at <= _chicago(row.event_at, "second.event_at") < exit_at]
    expected = int((exit_at - entry_at).total_seconds())
    if len(rows) != expected or any(_chicago(right.event_at, "second.event_at") - _chicago(left.event_at, "second.event_at") != timedelta(seconds=1) for left, right in zip(rows, rows[1:])) or any(row.actual_identity != identity or _chicago(row.available_at, "second.available_at") > _chicago(row.event_at, "second.event_at") + timedelta(seconds=1) for row in rows):
        return float("nan")
    worst = min((row.low - entry_price) * direction if direction > 0 else (entry_price - row.high) for row in rows)
    return float(worst / tick_size * tick_value)


def _array_trade(
    cube: MinuteBarCube,
    seconds: Mapping[tuple[int, int, str], Sequence[CausalSecondBar]],
    *,
    campaign: int,
    date: int,
    market_position: int,
    event_minute: int,
    lookback: int,
    horizon: int,
    direction: int,
    synthetic_campaign_id: str | None = None,
    synthetic_kind: str | None = None,
    adverse_required: bool = True,
    exposure_required: bool = True,
) -> tuple[float, float, np.ndarray] | None:
    decision_ns = int(cube.event_at_ns[campaign, date, market_position, event_minute]) + 65_000_000_000
    dependency_start = event_minute - max(lookback, 60 if exposure_required else lookback)
    if dependency_start < 0 or not np.all(cube.observed[campaign, date, market_position, dependency_start:event_minute + 1]) or np.any(cube.available_at_ns[campaign, date, market_position, dependency_start:event_minute + 1] > decision_ns):
        return None
    entry = event_minute + 2
    if (
        entry >= cube.event_at_ns.shape[3]
        or cube.event_at_ns[campaign, date, market_position, entry] <= decision_ns
        or cube.event_at_ns[campaign, date, market_position, entry - 1] > decision_ns
    ):
        return None
    exit_minute = entry + horizon
    if entry > {15: 370, 30: 355, 60: 325}[horizon] or exit_minute > 390:
        return None
    if not np.all(cube.observed[campaign, date, market_position, event_minute - lookback:exit_minute + 1]):
        return None
    identities = cube.identity_code[campaign, date, market_position, event_minute - lookback:exit_minute + 1]
    if np.any(identities != identities[-1]):
        return None
    tick_size = float(cube.tick_size[campaign, date, market_position, entry])
    tick_value = float(cube.tick_value[campaign, date, market_position, entry])
    entry_price = float(cube.open[campaign, date, market_position, entry])
    exit_price = float(cube.open[campaign, date, market_position, exit_minute])
    gross = round(direction * (exit_price - entry_price) / tick_size * tick_value, 10)
    if not adverse_required and not exposure_required:
        return float(gross), 0.0, np.zeros(len(EXPOSURES), dtype=np.float64)
    adverse = 0.0
    if adverse_required:
        identity = cube.identity_labels[int(identities[-1])]
        entry_at = datetime.fromtimestamp(int(cube.event_at_ns[campaign, date, market_position, entry]) / 1_000_000_000, tz=timezone.utc).astimezone(CHICAGO)
        exit_at = datetime.fromtimestamp(int(cube.event_at_ns[campaign, date, market_position, exit_minute]) / 1_000_000_000, tz=timezone.utc).astimezone(CHICAGO)
        market = cube.markets[market_position]
        if synthetic_campaign_id is None:
            adverse = _adverse_path(seconds, campaign=campaign, date_index=date, market=market, identity=identity, entry_at=entry_at, exit_at=exit_at, entry_price=entry_price, direction=direction, tick_size=tick_size, tick_value=tick_value)
        else:
            seed = int(sha256_json({"campaign_id": synthetic_campaign_id, "date": date, "market": market, "entry": entry, "horizon": horizon})[:16], 16)
            if synthetic_kind == "MISSINGNESS" and seed % 17 == 0:
                adverse = float("nan")
            else:
                generator = np.random.Generator(np.random.PCG64(seed))
                count = horizon * 60
                terminal = exit_price - entry_price
                bridge = np.linspace(0.0, terminal, count, endpoint=False) + np.cumsum(generator.normal(0.0, tick_size * 0.08, size=count))
                adverse = float(np.min(direction * bridge) / tick_size * tick_value)
    if not exposure_required:
        return float(gross), adverse, np.zeros(len(EXPOSURES), dtype=np.float64)
    if event_minute >= 60:
        trailing = cube.close[campaign, date, market_position, event_minute - 60:event_minute + 1]
        scale = np.maximum(np.abs(trailing[:-1]), cube.tick_size[campaign, date, market_position, event_minute - 60:event_minute])
        one_minute = np.diff(trailing) / scale
        trend_scale = max(abs(float(trailing[0])), tick_size)
        trend = float((trailing[-1] - trailing[0]) / trend_scale)
    else:
        one_minute = np.zeros(60)
        trend = 0.0
    previous_close = float(cube.open[campaign, date, market_position, 0] if date == 0 else cube.close[campaign, date - 1, market_position, -1])
    opening = float(cube.open[campaign, date, market_position, 0])
    gap = 0.0 if previous_close == 0 else opening / previous_close - 1.0
    exposure = np.asarray((1.0, float(np.sign(cube.close[campaign, date, market_position, event_minute] - opening)), trend, float(np.std(one_minute, ddof=1)), 0.0, gap, event_minute / 390.0), dtype=np.float64)
    return float(gross), adverse, exposure


def _causal_feature_arrays(cube: MinuteBarCube, window: int) -> tuple[np.ndarray, np.ndarray]:
    prior = cube.close[..., :-window]
    denominator = np.maximum(np.abs(prior), cube.tick_size[..., :-window])
    returns = (cube.close[..., window:] - prior) / denominator
    log_volume = np.log1p(cube.volume[..., window:])
    availability = np.lib.stride_tricks.sliding_window_view(cube.available_at_ns, window + 1, axis=3)
    identities = np.lib.stride_tricks.sliding_window_view(cube.identity_code, window + 1, axis=3)
    observed = np.lib.stride_tricks.sliding_window_view(cube.observed, window + 1, axis=3)
    valid = (
        (np.max(availability, axis=4) <= cube.event_at_ns[..., window:] + 65_000_000_000)
        & np.all(identities == identities[..., -1:], axis=4)
        & np.all(observed, axis=4)
    )
    return np.where(valid, returns, np.nan), np.where(valid, log_volume, np.nan)


def _frozen_es_rescue_economics(specification: Mapping[str, object]) -> dict[str, object]:
    execution = specification.get("execution")
    if not isinstance(execution, Mapping):
        raise IntegrityError("ES rescue execution specification is missing")
    expected = build_es_flow_economics(Path(__file__).resolve().parents[2])
    if execution.get("economics") != expected:
        raise IntegrityError("caller-supplied or stale MES economics are forbidden")
    return expected


def _validate_es_rescue_specification(specification: Mapping[str, object]) -> None:
    expected = build_es_flow_screen_program(Path(__file__).resolve().parents[2])
    if specification != expected:
        raise IntegrityError("ES rescue program or a hash-bound dependency changed")


def _es_rescue_feature(
    cube: MinuteBarCube,
    causal_returns: np.ndarray,
    causal_log_volume: np.ndarray,
    *,
    date_index: int,
    event_minute: int,
) -> tuple[np.ndarray, int, float, float] | None:
    if date_index < 63 or event_minute < 20 or event_minute > 323:
        return None
    offset = event_minute - 15
    current_return = float(causal_returns[0, date_index, 0, offset])
    current_volume = float(causal_log_volume[0, date_index, 0, offset])
    prior_return = causal_returns[0, date_index - 63:date_index, 0, offset]
    if not np.isfinite(current_return) or not np.isfinite(current_volume) or not np.all(np.isfinite(prior_return)):
        return None
    return_scale = float(np.std(prior_return, ddof=1))
    if return_scale <= 0:
        return None
    identity = int(cube.identity_code[0, date_index, 0, event_minute])
    prior_identity = cube.identity_code[0, date_index - 63:date_index, 0, event_minute]
    matching_volume = causal_log_volume[0, date_index - 63:date_index, 0, offset][prior_identity == identity]
    matching_volume = matching_volume[np.isfinite(matching_volume)]
    if len(matching_volume) < 20:
        return None
    volume_scale = float(np.std(matching_volume, ddof=1))
    if volume_scale <= 0:
        return None
    decision_ns = int(cube.event_at_ns[0, date_index, 0, event_minute]) + 65_000_000_000
    dependency = slice(event_minute - 20, event_minute + 1)
    if (
        not np.all(cube.observed[0, date_index, 0, dependency])
        or np.any(cube.available_at_ns[0, date_index, 0, dependency] > decision_ns)
        or np.any(cube.identity_code[0, date_index, 0, dependency] != identity)
    ):
        return None
    high = cube.high[0, date_index, 0, event_minute - 19:event_minute + 1]
    low = cube.low[0, date_index, 0, event_minute - 19:event_minute + 1]
    previous_close = cube.close[0, date_index, 0, event_minute - 20:event_minute]
    true_range = np.maximum(high - low, np.maximum(np.abs(high - previous_close), np.abs(low - previous_close)))
    atr20 = float(np.mean(true_range))
    price = float(cube.close[0, date_index, 0, event_minute])
    one_minute_returns = np.diff(cube.close[0, date_index, 0, dependency]) / np.maximum(
        np.abs(cube.close[0, date_index, 0, event_minute - 20:event_minute]),
        cube.tick_size[0, date_index, 0, event_minute - 20:event_minute],
    )
    direction = int(np.sign(current_return))
    if direction == 0 or atr20 <= 0 or price <= 0 or not np.all(np.isfinite(one_minute_returns)):
        return None
    return_z = (current_return - float(np.mean(prior_return))) / return_scale
    volume_z = (current_volume - float(np.mean(matching_volume))) / volume_scale
    features = np.asarray(
        (
            current_return,
            return_z,
            volume_z,
            atr20 / price,
            float(np.std(one_minute_returns, ddof=1)),
            float(event_minute),
        ),
        dtype=np.float64,
    )
    if not np.all(np.isfinite(features)):
        return None
    return features, direction, atr20, float(abs(return_z))


def _mes_quantity(atr20_points: float, economics: Mapping[str, object]) -> tuple[int, int, float, float]:
    tick = Decimal(str(economics["tick_size"]))
    tick_value = Decimal(str(economics["tick_value_usd"]))
    cost = Decimal(str(economics["stress_round_trip_cost_usd_per_contract"]))
    cap = Decimal(str(economics["maximum_initial_risk_usd"]))
    stop_ticks = max(1, int((Decimal(str(atr20_points)) * Decimal("1.5") / tick).to_integral_value(rounding=ROUND_CEILING)))
    risk_per_contract = Decimal(stop_ticks) * tick_value + cost
    quantity = min(int(economics["maximum_micro_contracts"]), int(cap // risk_per_contract))
    return quantity, stop_ticks, float(risk_per_contract), float(risk_per_contract * quantity)


def _mes_proxy_outcome(
    cube: MinuteBarCube,
    *,
    date_index: int,
    event_minute: int,
    horizon: int,
    direction: int,
    atr20_points: float,
    economics: Mapping[str, object],
) -> dict[str, object]:
    if horizon not in ES_FLOW_HORIZONS or direction not in (-1, 1):
        raise IntegrityError("MES proxy outcome parameters are invalid")
    quantity, stop_ticks, risk_per_contract, planned_risk = _mes_quantity(atr20_points, economics)
    common = {
        "horizon_minutes": horizon,
        "direction": direction,
        "quantity": quantity,
        "stop_ticks": stop_ticks,
        "stop_risk_plus_stress_cost_usd_per_contract": risk_per_contract,
        "planned_initial_risk_usd": planned_risk,
    }
    if quantity == 0:
        return {**common, "disposition": "RISK_ABSTENTION", "abstention_reason": "ZERO_QUANTITY_UNDER_250_USD_CAP"}
    event_ns = int(cube.event_at_ns[0, date_index, 0, event_minute])
    decision_ns = event_ns + 65_000_000_000
    entry = event_minute + 2
    exit_minute = entry + horizon
    if entry > 325 or exit_minute > 390 or cube.event_at_ns[0, date_index, 0, entry] <= decision_ns or cube.event_at_ns[0, date_index, 0, entry - 1] > decision_ns:
        return {**common, "disposition": "INCOMPLETE_PATH", "abstention_reason": "ENTRY_OR_HORIZON_OUTSIDE_SHARED_WINDOW"}
    path = slice(event_minute - 20, exit_minute + 1)
    identity = int(cube.identity_code[0, date_index, 0, event_minute])
    if not np.all(cube.observed[0, date_index, 0, path]) or np.any(cube.identity_code[0, date_index, 0, path] != identity):
        return {**common, "disposition": "INCOMPLETE_PATH", "abstention_reason": "MISSING_BAR_OR_CONTRACT_IDENTITY_CHANGE"}
    entry_price = float(cube.open[0, date_index, 0, entry])
    tick = float(economics["tick_size"])
    multiplier = float(economics["contract_multiplier_usd_per_point"])
    cost_per_contract = float(economics["stress_round_trip_cost_usd_per_contract"])
    stop_distance = stop_ticks * tick
    stop_price = entry_price - direction * stop_distance
    favorable_points = 0.0
    adverse_points = 0.0
    fill_price: float | None = None
    fill_minute = exit_minute
    exit_reason = "TIME_EXIT"
    for minute in range(entry, exit_minute):
        open_price = float(cube.open[0, date_index, 0, minute])
        high = float(cube.high[0, date_index, 0, minute])
        low = float(cube.low[0, date_index, 0, minute])
        gap_through = open_price <= stop_price if direction > 0 else open_price >= stop_price
        stop_crossed = low <= stop_price if direction > 0 else high >= stop_price
        if gap_through:
            fill_price = open_price
            fill_minute = minute
            adverse_points = max(adverse_points, max(-direction * (open_price - entry_price), 0.0))
            exit_reason = "GAP_THROUGH_STOP"
            break
        if stop_crossed:
            fill_price = stop_price
            fill_minute = minute
            adverse_points = max(adverse_points, stop_distance)
            exit_reason = "PROTECTIVE_STOP_PESSIMISTIC_BAR_ORDER"
            break
        favorable_points = max(favorable_points, max(direction * ((high if direction > 0 else low) - entry_price), 0.0))
        adverse_points = max(adverse_points, max(-direction * ((low if direction > 0 else high) - entry_price), 0.0))
    if fill_price is None:
        fill_price = float(cube.open[0, date_index, 0, exit_minute])
        terminal_points = direction * (fill_price - entry_price)
        favorable_points = max(favorable_points, max(terminal_points, 0.0))
        adverse_points = max(adverse_points, max(-terminal_points, 0.0))
    gross = direction * (fill_price - entry_price) * multiplier * quantity
    cost = cost_per_contract * quantity
    terminal_net = gross - cost
    mfe = favorable_points * multiplier * quantity
    mae = adverse_points * multiplier * quantity
    tick_risk = float(economics["tick_value_usd"]) * quantity
    raw_ratio = mfe / max(mae, tick_risk)
    net_ratio = max(mfe - cost, 0.0) / max(mae + cost, tick_risk)
    sensitivities = {
        f"{round_trip:.2f}": {
            "terminal_net_pnl_usd": gross - round_trip * quantity,
            "net_mfe_mae": max(mfe - round_trip * quantity, 0.0) / max(mae + round_trip * quantity, tick_risk),
        }
        for round_trip in ES_FLOW_COSTS
    }
    return {
        **common,
        "disposition": "TRADE",
        "abstention_reason": None,
        "entry_minute": entry,
        "exit_minute": fill_minute,
        "exit_at_ns": int(cube.event_at_ns[0, date_index, 0, fill_minute]),
        "entry_at_ns": int(cube.event_at_ns[0, date_index, 0, entry]),
        "entry_price": entry_price,
        "exit_price": fill_price,
        "exit_reason": exit_reason,
        "gross_pnl_usd": float(gross),
        "round_trip_cost_usd": float(cost),
        "terminal_net_pnl_usd": float(terminal_net),
        "mfe_usd": float(mfe),
        "mae_usd": float(mae),
        "terminal_net_pnl_r": float(terminal_net / planned_risk),
        "mfe_r": float(mfe / planned_risk),
        "mae_r": float(mae / planned_risk),
        "raw_mfe_mae": float(raw_ratio),
        "net_mfe_mae": float(net_ratio),
        "cost_sensitivities": sensitivities,
    }


def _es_rescue_rows(
    specification: Mapping[str, object], cube: MinuteBarCube
) -> dict[str, list[dict[str, object]]]:
    candidates = list(specification["candidates"])  # type: ignore[arg-type]
    economics = _frozen_es_rescue_economics(specification)
    causal_returns, causal_volume = _causal_feature_arrays(cube, 15)
    grouped: dict[tuple[float, float], list[Mapping[str, object]]] = {}
    for candidate in candidates:
        grouped.setdefault((float(candidate["return_z_threshold"]), float(candidate["volume_surprise_threshold"])), []).append(candidate)
    rows = {str(candidate["candidate_id"]): [] for candidate in candidates}
    for date_index, session_date in enumerate(cube.exchange_session_dates):
        if date_index < 63:
            for candidate in candidates:
                rows[str(candidate["candidate_id"])].append({"session_date": session_date, "disposition": "LOOKBACK_ONLY"})
            continue
        selected: dict[tuple[float, float], dict[str, object]] = {}
        session_has_valid_features = False
        for minute in range(20, 324):
            feature = _es_rescue_feature(cube, causal_returns, causal_volume, date_index=date_index, event_minute=minute)
            if feature is None:
                continue
            session_has_valid_features = True
            values, direction, atr20, absolute_return_z = feature
            for thresholds in grouped:
                if thresholds in selected or absolute_return_z < thresholds[0] or values[2] < thresholds[1]:
                    continue
                identity_code = int(cube.identity_code[0, date_index, 0, minute])
                event_core = {
                    "session_date": session_date,
                    "event_at_ns": int(cube.event_at_ns[0, date_index, 0, minute]),
                    "entry_at_ns": int(cube.event_at_ns[0, date_index, 0, minute + 2]),
                    "event_minute": minute,
                    "actual_contract_identity": cube.identity_labels[identity_code],
                    "return_z_threshold": thresholds[0],
                    "volume_surprise_threshold": thresholds[1],
                }
                selected[thresholds] = {
                    **event_core,
                    "shared_event_id": sha256_json(event_core),
                    "features": {name: float(value) for name, value in zip(ES_FLOW_FEATURES, values, strict=True)},
                    "direction": direction,
                    "atr20_points": atr20,
                }
            if len(selected) == len(grouped):
                break
        for thresholds, pair in grouped.items():
            event = selected.get(thresholds)
            for candidate in pair:
                candidate_id = str(candidate["candidate_id"])
                if event is None:
                    rows[candidate_id].append({
                        "session_date": session_date,
                        "disposition": "NO_QUALIFYING_EVENT" if session_has_valid_features else "INVALID_FEATURE_SESSION",
                    })
                    continue
                outcome = _mes_proxy_outcome(
                    cube,
                    date_index=date_index,
                    event_minute=int(event["event_minute"]),
                    horizon=int(candidate["holding_horizon_minutes"]),
                    direction=int(event["direction"]),
                    atr20_points=float(event["atr20_points"]),
                    economics=economics,
                )
                rows[candidate_id].append({**event, **outcome})
    return rows


def evaluate_es_flow_screen(
    specification: Mapping[str, object], bars: Sequence[CausalBar] | MinuteBarCube
) -> dict[str, object]:
    """Cheaply falsify the frozen family using 2018-2020 outcomes only."""

    _verified_identity(specification, "program_id")
    economics = _frozen_es_rescue_economics(specification)
    _validate_es_rescue_specification(specification)
    candidates = specification.get("candidates")
    if (
        specification.get("schema_version") != PROGRAM_SCHEMA
        or specification.get("stage") != ES_FLOW_SCREEN_STAGE
        or specification.get("markets") != ["ES"]
        or candidates != _es_flow_candidates()
        or specification.get("candidate_count") != 8
    ):
        raise IntegrityError("ES rescue specification is not the frozen program")
    if isinstance(bars, MinuteBarCube):
        cube = bars
        cube.validate()
    else:
        try:
            escaped = any(type(row) is not CausalBar or row.market != "ES" or int(row.exchange_session_date[:4]) not in ES_FLOW_DEVELOPMENT_YEARS for row in bars)
        except (TypeError, ValueError) as exc:
            raise IntegrityError("screen bars escape the ES 2018-2020 development scope") from exc
        if escaped:
            raise IntegrityError("screen bars escape the ES 2018-2020 development scope")
        checked, dates = validate_causal_bars(bars, ("ES",), require_wfa_schedule=False)
        cube = _minute_cube(checked, dates=dates, markets=("ES",), campaign_count=1)
    if cube.campaign_count != 1 or cube.markets != ("ES",):
        raise IntegrityError("ES flow screen requires one historical ES campaign")
    try:
        years = tuple(int(value[:4]) for value in cube.exchange_session_dates)
    except (TypeError, ValueError) as exc:
        raise IntegrityError("ES flow screen dates are malformed") from exc
    if set(years) != set(ES_FLOW_DEVELOPMENT_YEARS) or any(year not in ES_FLOW_DEVELOPMENT_YEARS for year in years) or len(years) < 64:
        raise IntegrityError("ES flow screen accepts only development dates from every year 2018-2020")

    rows = _es_rescue_rows(specification, cube)
    screen_indices = list(range(63, len(years)))
    results: list[dict[str, object]] = []
    passing: list[str] = []
    for candidate in candidates:
        candidate_id = str(candidate["candidate_id"])
        observations = [rows[candidate_id][index] for index in screen_indices]
        trades = [row for row in observations if row["disposition"] == "TRADE"]
        count = len(trades)
        gross = float(sum(float(row["gross_pnl_usd"]) for row in trades))
        annual_net = {
            str(year): float(sum(float(row["terminal_net_pnl_usd"]) for row in trades if int(str(row["session_date"])[:4]) == year))
            for year in ES_FLOW_DEVELOPMENT_YEARS
        }
        costs = {
            f"{cost:.2f}": {
                "total_pnl_usd": float(sum(float(row["cost_sensitivities"][f"{cost:.2f}"]["terminal_net_pnl_usd"]) for row in trades)),
                "annual_pnl_usd": {
                    str(year): float(sum(
                        float(row["cost_sensitivities"][f"{cost:.2f}"]["terminal_net_pnl_usd"])
                        for row in trades if int(str(row["session_date"])[:4]) == year
                    ))
                    for year in ES_FLOW_DEVELOPMENT_YEARS
                },
            }
            for cost in ES_FLOW_COSTS
        }
        median_ratio = float(np.median([float(row["net_mfe_mae"]) for row in trades])) if trades else None
        accounting = len(observations) == len(screen_indices) and all(row.get("disposition") in {
            "TRADE", "RISK_ABSTENTION", "INCOMPLETE_PATH", "NO_QUALIFYING_EVENT", "INVALID_FEATURE_SESSION"
        } for row in observations)
        gate_checks = {
            "minimum_60_risk_eligible_opportunities": count >= 60,
            "positive_profile_stress_mes_proxy_pnl": sum(annual_net.values()) > 0,
            "positive_stress_pnl_in_two_of_three_years": sum(value > 0 for value in annual_net.values()) >= 2,
            "median_realized_net_mfe_mae_above_one": median_ratio is not None and median_ratio > 1.0,
            "complete_session_and_abstention_accounting": accounting,
        }
        passed = all(gate_checks.values())
        if passed:
            passing.append(candidate_id)
        results.append({
            **candidate,
            "opportunity_count": count,
            "gross_pnl_usd": gross,
            "profile_stress_net_pnl_usd": float(sum(annual_net.values())),
            "annual_profile_stress_net_pnl_usd": annual_net,
            "median_realized_net_mfe_mae": median_ratio,
            "cost_results": costs,
            "screen_session_count": len(screen_indices),
            "risk_abstention_count": sum(row["disposition"] == "RISK_ABSTENTION" for row in observations),
            "missing_or_invalid_session_count": sum(row["disposition"] in {"INCOMPLETE_PATH", "INVALID_FEATURE_SESSION"} for row in observations),
            "no_qualifying_event_count": sum(row["disposition"] == "NO_QUALIFYING_EVENT" for row in observations),
            "opportunities": trades,
            "gate_checks": gate_checks,
            "gate_passed": passed,
        })
    core = {
        "schema_version": DECISION_SCHEMA,
        "stage": ES_FLOW_SCREEN_STAGE,
        "program_id": specification["program_id"],
        "evidence_scope": "HISTORICAL_SCREEN_2018_2020_ONLY_PREVIOUSLY_EXPOSED",
        "evidence_label": "HISTORICAL_SCREEN_NOT_CONFIRMATION",
        "economics_binding": {"economics_id": economics["economics_id"], "bindings": economics["bindings"]},
        "variants": results,
        "qualifying_candidate_ids": passing,
        "decision": "CONTINUE_TO_HISTORICAL_WFA" if passing else "STOP_NO_ECONOMIC_EDGE",
        "wfa_candidate_ids": [str(candidate["candidate_id"]) for candidate in candidates] if passing else [],
        "post_2020_outcomes_computed_or_exposed": False,
        "next_step_authorized": False,
    }
    return _identified(core, "calculation_id")


def _ridge_model(features: np.ndarray, targets: np.ndarray) -> dict[str, object]:
    if features.ndim != 2 or features.shape[1] != len(ES_FLOW_FEATURES) or targets.shape != (features.shape[0], 3):
        raise IntegrityError("Ridge training arrays have the wrong shape")
    if len(features) < 7 or not np.all(np.isfinite(features)) or not np.all(np.isfinite(targets)):
        raise IntegrityError("Ridge requires finite eligible training rows")
    mean = np.mean(features, axis=0)
    scale = np.std(features, axis=0, ddof=1)
    invariant = [name for name, value in zip(ES_FLOW_FEATURES, scale, strict=True) if not np.isfinite(value) or value <= 0]
    if invariant:
        raise IntegrityError(f"Ridge feature does not vary: {','.join(invariant)}")
    standardized = (features - mean) / scale
    design = np.column_stack((np.ones(len(features)), standardized))
    penalty = np.eye(design.shape[1], dtype=np.float64)
    penalty[0, 0] = 0.0
    coefficients = np.linalg.solve(design.T @ design + penalty, design.T @ targets)
    return {
        "ridge_penalty": 1.0,
        "feature_names": list(ES_FLOW_FEATURES),
        "target_names": ["terminal_net_pnl_r", "mfe_r", "mae_r"],
        "feature_mean": mean.tolist(),
        "feature_scale": scale.tolist(),
        "intercept": coefficients[0].tolist(),
        "coefficients": coefficients[1:].tolist(),
        "training_row_count": len(features),
    }


def _ridge_predict(model: Mapping[str, object], features: np.ndarray) -> np.ndarray:
    mean = np.asarray(model["feature_mean"], dtype=np.float64)
    scale = np.asarray(model["feature_scale"], dtype=np.float64)
    intercept = np.asarray(model["intercept"], dtype=np.float64)
    coefficients = np.asarray(model["coefficients"], dtype=np.float64)
    prediction = intercept + ((features - mean) / scale) @ coefficients
    prediction[1:] = np.maximum(prediction[1:], 0.0)
    return prediction


def _drawdown(values: np.ndarray) -> float:
    equity = np.concatenate((np.zeros(1, dtype=np.float64), np.cumsum(values, dtype=np.float64)))
    return float(np.max(np.maximum.accumulate(equity) - equity))


def _positive_concentration(values: np.ndarray, years: np.ndarray) -> tuple[float, float]:
    positive = values[values > 0]
    total = float(np.sum(positive))
    if total <= 0:
        return float("inf"), float("inf")
    top_five = float(np.sum(np.sort(positive)[-5:]) / total)
    year_share = max(float(np.sum(values[(years == year) & (values > 0)])) / total for year in ES_FLOW_AUDIT_YEARS)
    return top_five, year_share


def _bootstrap_lower_total(values: np.ndarray, indices: np.ndarray) -> float:
    return float(np.quantile(np.sum(values[indices], axis=1), 0.05))


def _familywise_lower_totals(differentials: np.ndarray, indices: np.ndarray) -> np.ndarray:
    observed = np.mean(differentials, axis=0)
    centered = differentials - observed
    bootstrap_error = -np.mean(centered[indices], axis=1)
    critical = float(np.quantile(np.max(bootstrap_error, axis=1), 0.95))
    return (observed - critical) * len(differentials)


def _baseline_outcome(
    name: str,
    row: Mapping[str, object],
    cube: MinuteBarCube,
    date_index: int,
    economics: Mapping[str, object],
) -> float:
    if name == "ZERO_NO_TRADE" or "shared_event_id" not in row:
        return 0.0
    if name == "UNFILTERED_CONTINUATION":
        return float(row["terminal_net_pnl_usd"]) if row.get("disposition") == "TRADE" else 0.0
    signal_direction = int(row["direction"])
    direction = {
        "MATCHED_REVERSAL": -signal_direction,
        "RISK_MATCHED_ALWAYS_LONG": 1,
        "RISK_MATCHED_ALWAYS_SHORT": -1,
    }[name]
    outcome = _mes_proxy_outcome(
        cube,
        date_index=date_index,
        event_minute=int(row["event_minute"]),
        horizon=int(row["horizon_minutes"]),
        direction=direction,
        atr20_points=float(row["atr20_points"]),
        economics=economics,
    )
    return float(outcome["terminal_net_pnl_usd"]) if outcome["disposition"] == "TRADE" else 0.0


def _choose_es_rescue_candidate(
    qualifying: Sequence[Mapping[str, object]],
    daily: Mapping[str, np.ndarray],
    bootstrap_indices: np.ndarray,
) -> tuple[str | None, list[dict[str, object]]]:
    by_threshold: dict[tuple[float, float], list[Mapping[str, object]]] = {}
    for row in qualifying:
        by_threshold.setdefault((float(row["return_z_threshold"]), float(row["volume_surprise_threshold"])), []).append(row)
    paired: dict[tuple[float, float], tuple[Mapping[str, object], Mapping[str, object]]] = {}
    for thresholds, rows in by_threshold.items():
        ordered = sorted(rows, key=lambda row: int(row["holding_horizon_minutes"]))
        if len(ordered) == 2:
            paired[thresholds] = (ordered[0], ordered[1])
    paired_bounds: dict[tuple[float, float], tuple[float, float]] = {}
    if paired:
        keys = sorted(paired)
        differences = np.column_stack([
            daily[str(paired[key][0]["candidate_id"])] - daily[str(paired[key][1]["candidate_id"])]
            for key in keys
        ])
        observed = np.sum(differences, axis=0)
        centered_bootstrap = np.sum((differences - np.mean(differences, axis=0))[bootstrap_indices], axis=1)
        lower_critical = float(np.quantile(np.max(-centered_bootstrap, axis=1), 0.95))
        upper_critical = float(np.quantile(np.max(centered_bootstrap, axis=1), 0.95))
        paired_bounds = {
            key: (float(observed[index] - lower_critical), float(observed[index] + upper_critical))
            for index, key in enumerate(keys)
        }
    threshold_winners: list[Mapping[str, object]] = []
    comparisons: list[dict[str, object]] = []
    for thresholds, rows in sorted(by_threshold.items()):
        ordered = sorted(rows, key=lambda row: int(row["holding_horizon_minutes"]))
        if len(ordered) == 1:
            threshold_winners.append(ordered[0])
            comparisons.append({"thresholds": list(thresholds), "selected_horizon": ordered[0]["holding_horizon_minutes"], "reason": "ONLY_QUALIFYING_HORIZON"})
            continue
        fifteen, sixty = ordered
        lower, upper = paired_bounds[thresholds]
        if lower > 0:
            winner, reason = fifteen, "PAIRED_ADJUSTED_DIFFERENCE_FAVORS_15"
        elif upper < 0:
            winner, reason = sixty, "PAIRED_ADJUSTED_DIFFERENCE_FAVORS_60"
        else:
            winner, reason = fifteen, "INCONCLUSIVE_DEFAULT_TO_15"
        threshold_winners.append(winner)
        comparisons.append({
            "thresholds": list(thresholds),
            "fifteen_minus_sixty_lower_usd": lower,
            "fifteen_minus_sixty_upper_usd": upper,
            "selected_horizon": winner["holding_horizon_minutes"],
            "reason": reason,
        })
    if not threshold_winners:
        return None, comparisons
    selected = sorted(
        threshold_winners,
        key=lambda row: (
            -float(row["stress_net_pnl_usd"]),
            float(row["return_z_threshold"]),
            float(row["volume_surprise_threshold"]),
            str(row["candidate_id"]),
        ),
    )[0]
    return str(selected["candidate_id"]), comparisons


def evaluate_es_flow_wfa(
    specification: Mapping[str, object],
    bars: Sequence[CausalBar] | MinuteBarCube,
    screen_result: Mapping[str, object],
    *,
    bootstrap_resamples: int = ES_FLOW_BOOTSTRAP_RESAMPLES,
) -> dict[str, object]:
    """Run the frozen eight-fold 2021-2022 historical audit and selection."""

    _verified_identity(specification, "program_id")
    economics = _frozen_es_rescue_economics(specification)
    _validate_es_rescue_specification(specification)
    _verified_identity(screen_result, "calculation_id")
    if (
        screen_result.get("program_id") != specification["program_id"]
        or screen_result.get("decision") != "CONTINUE_TO_HISTORICAL_WFA"
        or screen_result.get("wfa_candidate_ids") != [str(candidate["candidate_id"]) for candidate in _es_flow_candidates()]
    ):
        raise IntegrityError("historical WFA requires the passing 2018-2020 screen for all eight candidates")
    if specification.get("candidates") != _es_flow_candidates() or bootstrap_resamples != ES_FLOW_BOOTSTRAP_RESAMPLES:
        raise IntegrityError("historical WFA settings are not frozen")
    if isinstance(bars, MinuteBarCube):
        cube = bars
        cube.validate()
    else:
        checked, dates = validate_causal_bars(bars, ("ES",))
        cube = _minute_cube(checked, dates=dates, markets=("ES",), campaign_count=1)
    if cube.campaign_count != 1 or cube.markets != ("ES",):
        raise IntegrityError("ES rescue WFA requires one historical ES campaign")
    years = np.asarray([int(value[:4]) for value in cube.exchange_session_dates], dtype=np.int64)
    if set(years.tolist()) != set(ES_FLOW_SCREEN_YEARS):
        raise IntegrityError("ES rescue WFA requires only complete 2018-2022 source years")
    folds = _schedule(cube.exchange_session_dates)
    audit_indices = np.concatenate([fold.audit_indices for fold in folds]).astype(np.int64)
    if len(audit_indices) != 504 or set(years[audit_indices].tolist()) != set(ES_FLOW_AUDIT_YEARS):
        raise IntegrityError("frozen outer audit folds must cover only 2021-2022")
    rows = _es_rescue_rows(specification, cube)
    candidates = list(specification["candidates"])  # type: ignore[arg-type]
    candidate_ids = [str(candidate["candidate_id"]) for candidate in candidates]
    position = {int(date_index): offset for offset, date_index in enumerate(audit_indices)}
    daily = {candidate_id: np.zeros(504, dtype=np.float64) for candidate_id in candidate_ids}
    baselines = {
        candidate_id: {name: np.zeros(504, dtype=np.float64) for name in ES_FLOW_BASELINES}
        for candidate_id in candidate_ids
    }
    session_records = {candidate_id: [] for candidate_id in candidate_ids}
    fold_models = {candidate_id: [] for candidate_id in candidate_ids}
    for fold_number, fold in enumerate(folds):
        for candidate in candidates:
            candidate_id = str(candidate["candidate_id"])
            training_rows = [rows[candidate_id][int(index)] for index in fold.fit_indices if rows[candidate_id][int(index)].get("disposition") == "TRADE"]
            model: dict[str, object] | None = None
            failure: str | None = None
            try:
                model = _ridge_model(
                    np.asarray([[float(row["features"][name]) for name in ES_FLOW_FEATURES] for row in training_rows], dtype=np.float64),
                    np.asarray([[float(row[name]) for name in ("terminal_net_pnl_r", "mfe_r", "mae_r")] for row in training_rows], dtype=np.float64),
                )
            except IntegrityError as exc:
                failure = str(exc)
            fold_models[candidate_id].append({
                "outer_fold": fold_number,
                "training_start": cube.exchange_session_dates[int(fold.fit_indices[0])],
                "training_end": cube.exchange_session_dates[int(fold.fit_indices[-1])],
                "audit_start": cube.exchange_session_dates[int(fold.audit_indices[0])],
                "audit_end": cube.exchange_session_dates[int(fold.audit_indices[-1])],
                "model": model,
                "failure": failure,
            })
            for date_index_value in fold.audit_indices:
                date_index = int(date_index_value)
                output_index = position[date_index]
                row = rows[candidate_id][date_index]
                prediction: dict[str, object] = {
                    "terminal_net_pnl_r": None,
                    "mfe_r": None,
                    "mae_r": None,
                    "net_mfe_mae": None,
                    "ridge_filter_passed": False,
                    "reason": failure or str(row["disposition"]),
                }
                take_trade = False
                if model is not None and row.get("disposition") == "TRADE":
                    feature_vector = np.asarray([float(row["features"][name]) for name in ES_FLOW_FEATURES], dtype=np.float64)
                    predicted = _ridge_predict(model, feature_vector)
                    cost_r = float(row["round_trip_cost_usd"]) / float(row["planned_initial_risk_usd"])
                    tick_r = float(economics["tick_value_usd"]) * int(row["quantity"]) / float(row["planned_initial_risk_usd"])
                    predicted_ratio = max(float(predicted[1]) - cost_r, 0.0) / max(float(predicted[2]) + cost_r, tick_r)
                    take_trade = float(predicted[0]) >= 0.25 and predicted_ratio > 1.0
                    prediction = {
                        "terminal_net_pnl_r": float(predicted[0]),
                        "mfe_r": float(predicted[1]),
                        "mae_r": float(predicted[2]),
                        "net_mfe_mae": float(predicted_ratio),
                        "ridge_filter_passed": take_trade,
                        "reason": "PASS" if take_trade else "PREDICTION_HURDLE",
                    }
                actual = {
                    name: row.get(name)
                    for name in (
                        "terminal_net_pnl_usd", "terminal_net_pnl_r", "mfe_usd", "mae_usd", "mfe_r", "mae_r",
                        "raw_mfe_mae", "net_mfe_mae", "exit_reason", "quantity", "planned_initial_risk_usd", "abstention_reason",
                    )
                }
                if take_trade:
                    daily[candidate_id][output_index] = float(row["terminal_net_pnl_usd"])
                for baseline in ES_FLOW_BASELINES:
                    baselines[candidate_id][baseline][output_index] = _baseline_outcome(baseline, row, cube, date_index, economics)
                session_records[candidate_id].append({
                    "session_date": row["session_date"],
                    "outer_fold": fold_number,
                    "training_end": cube.exchange_session_dates[int(fold.fit_indices[-1])],
                    "shared_event_id": row.get("shared_event_id"),
                    "event_at_ns": row.get("event_at_ns"),
                    "entry_at_ns": row.get("entry_at_ns"),
                    "actual_contract_identity": row.get("actual_contract_identity"),
                    "source_and_execution": "ES_SIGNAL_MES_EXECUTION_PROXY",
                    "horizon_minutes": candidate["holding_horizon_minutes"],
                    "prediction_emitted_before_outcome_attachment": True,
                    "prediction": prediction,
                    "actual": actual,
                    "daily_net_pnl_usd": float(daily[candidate_id][output_index]),
                    "disposition": "RIDGE_TRADE" if take_trade else row["disposition"] if row["disposition"] != "TRADE" else "RIDGE_FILTERED_NO_TRADE",
                })
    bootstrap_indices = stationary_bootstrap_indices(
        n_observations=504,
        n_resamples=bootstrap_resamples,
        mean_block_length=ES_FLOW_BOOTSTRAP_BLOCK,
        seed=ES_FLOW_BOOTSTRAP_SEED,
    )
    contrast_ids: list[str] = []
    contrast_columns: list[np.ndarray] = []
    for candidate_id in candidate_ids:
        for baseline in ES_FLOW_BASELINES:
            contrast_ids.append(f"{candidate_id}:{baseline}")
            contrast_columns.append(daily[candidate_id] - baselines[candidate_id][baseline])
    differentials = np.column_stack(contrast_columns)
    adjusted_p_values = np.ones(len(contrast_ids), dtype=np.float64)
    nondegenerate = np.flatnonzero(np.std(differentials, axis=0, ddof=1) > 0)
    if len(nondegenerate):
        romano = romano_wolf_from_differentials(
            differentials[:, nondegenerate],
            hypothesis_ids=tuple(contrast_ids[index] for index in nondegenerate),
            hac_lag=5,
            mean_block_length=ES_FLOW_BOOTSTRAP_BLOCK,
            n_resamples=bootstrap_resamples,
            seed=ES_FLOW_BOOTSTRAP_SEED,
        )
        adjusted_p_values[nondegenerate] = romano.adjusted_p_values
    adjusted_lower = _familywise_lower_totals(differentials, bootstrap_indices)
    contrast_position = {name: index for index, name in enumerate(contrast_ids)}
    summaries: list[dict[str, object]] = []
    qualifying: list[dict[str, object]] = []
    audit_years = years[audit_indices]
    for candidate in candidates:
        candidate_id = str(candidate["candidate_id"])
        pnl = daily[candidate_id]
        records = session_records[candidate_id]
        trade_records = [record for record in records if record["disposition"] == "RIDGE_TRADE"]
        fold_pnl = [float(np.sum(pnl[fold * 63:(fold + 1) * 63])) for fold in range(8)]
        year_pnl = {str(year): float(np.sum(pnl[audit_years == year])) for year in ES_FLOW_AUDIT_YEARS}
        top_five, year_share = _positive_concentration(pnl, audit_years)
        contrasts = {
            baseline: {
                "net_pnl_difference_usd": float(np.sum(pnl - baselines[candidate_id][baseline])),
                "romano_wolf_adjusted_p_value": float(adjusted_p_values[contrast_position[f"{candidate_id}:{baseline}"]]),
                "adjusted_lower_bound_usd": float(adjusted_lower[contrast_position[f"{candidate_id}:{baseline}"]]),
            }
            for baseline in ES_FLOW_BASELINES
        }
        realized_ratios = [float(record["actual"]["net_mfe_mae"]) for record in trade_records]
        median_ratio = float(np.median(realized_ratios)) if realized_ratios else None
        max_risk = max((float(record["actual"]["planned_initial_risk_usd"]) for record in trade_records), default=0.0)
        checks = {
            "minimum_100_outer_fold_trades": len(trade_records) >= 100,
            "positive_stress_net_pnl": float(np.sum(pnl)) > 0,
            "primary_bootstrap_lower_bound_above_zero": _bootstrap_lower_total(pnl, bootstrap_indices) > 0,
            "adjusted_lower_bound_above_every_baseline": all(value["adjusted_lower_bound_usd"] > 0 for value in contrasts.values()),
            "romano_wolf_adjusted_significance_against_every_baseline": all(value["romano_wolf_adjusted_p_value"] <= 0.05 for value in contrasts.values()),
            "five_of_eight_positive_folds": sum(value > 0 for value in fold_pnl) >= 5,
            "positive_2021_and_2022": all(value > 0 for value in year_pnl.values()),
            "median_realized_net_mfe_mae_above_one": median_ratio is not None and median_ratio > 1.0,
            "top_five_positive_pnl_share_at_most_half": top_five <= 0.5,
            "single_year_positive_pnl_share_at_most_70_percent": year_share <= 0.7,
            "maximum_initial_risk_respected": max_risk <= float(economics["maximum_initial_risk_usd"]),
            "daily_loss_limit_respected": float(np.min(pnl)) >= -float(economics["daily_stop_loss_usd"]),
            "drawdown_limit_respected": _drawdown(pnl) <= float(economics["maximum_total_drawdown_usd"]),
            "complete_session_cost_exit_and_abstention_accounting": len(records) == 504,
            "ridge_improves_unfiltered_out_of_sample": float(np.sum(pnl)) > float(np.sum(baselines[candidate_id]["UNFILTERED_CONTINUATION"])),
        }
        summary = {
            **candidate,
            "trade_count": len(trade_records),
            "stress_net_pnl_usd": float(np.sum(pnl)),
            "primary_bootstrap_lower_bound_usd": _bootstrap_lower_total(pnl, bootstrap_indices),
            "fold_net_pnl_usd": fold_pnl,
            "annual_net_pnl_usd": year_pnl,
            "median_realized_net_mfe_mae": median_ratio,
            "top_five_positive_pnl_share": top_five,
            "maximum_single_year_positive_pnl_share": year_share,
            "maximum_drawdown_usd": _drawdown(pnl),
            "baseline_comparisons": contrasts,
            "gate_checks": checks,
            "gate_passed": all(checks.values()),
            "outer_models": fold_models[candidate_id],
            "audit_sessions": records,
        }
        summaries.append(summary)
        if summary["gate_passed"]:
            qualifying.append(summary)
    selected_id, horizon_comparisons = _choose_es_rescue_candidate(qualifying, daily, bootstrap_indices)
    frozen: dict[str, object] | None = None
    if selected_id is not None:
        selected = next(candidate for candidate in candidates if candidate["candidate_id"] == selected_id)
        selected_rows = [row for row in rows[selected_id] if row.get("disposition") == "TRADE"]
        final_model = _ridge_model(
            np.asarray([[float(row["features"][name]) for name in ES_FLOW_FEATURES] for row in selected_rows], dtype=np.float64),
            np.asarray([[float(row[name]) for name in ("terminal_net_pnl_r", "mfe_r", "mae_r")] for row in selected_rows], dtype=np.float64),
        )
        frozen_core = {
            "program_id": specification["program_id"],
            "candidate": selected,
            "model": final_model,
            "training_interval": "2018-2022_PREVIOUSLY_EXPOSED",
            "event_entry_stop_cost_risk_and_decision_rules": {
                "selection": specification["selection"],
                "timing": specification["timing"],
                "execution": specification["execution"],
                "outcomes": specification["outcomes"],
                "ridge": specification["ridge"],
            },
            "code_hashes": specification.get("bindings"),
            "created_by_historical_result": True,
            "retraining_or_material_change_starts_new_forward_test": True,
        }
        frozen = _identified(frozen_core, "frozen_artifact_id")
    core = {
        "schema_version": DECISION_SCHEMA,
        "stage": "ES_FLOW_HISTORICAL_WFA",
        "program_id": specification["program_id"],
        "screen_calculation_id": screen_result["calculation_id"],
        "evidence_scope": "INTERNAL_CHRONOLOGICAL_HISTORICAL_EVIDENCE_2021_2022_NOT_CONFIRMATION",
        "evidence_label": "READY_FOR_FORWARD_PAPER_TEST" if selected_id is not None else "NO_HISTORICAL_EDGE",
        "economics_binding": {"economics_id": economics["economics_id"], "bindings": economics["bindings"]},
        "bootstrap": {
            "resamples": bootstrap_resamples,
            "seed": ES_FLOW_BOOTSTRAP_SEED,
            "mean_block_sessions": ES_FLOW_BOOTSTRAP_BLOCK,
            "romano_wolf_hypotheses": len(contrast_ids),
            "degenerate_contrasts_retained_as_non_rejections": len(contrast_ids) - len(nondegenerate),
        },
        "candidates": summaries,
        "qualifying_candidate_ids": [str(row["candidate_id"]) for row in qualifying],
        "horizon_comparisons": horizon_comparisons,
        "selected_candidate_id": selected_id,
        "frozen_artifact": frozen,
        "decision": "READY_FOR_FORWARD_PAPER_TEST" if selected_id is not None else "STOP_NO_HISTORICAL_EDGE",
        "evaluated_2023_2025": False,
        "forward_or_trading_authorized": False,
    }
    return _identified(core, "calculation_id")


def build_es_forward_confirmation_contract(
    frozen_artifact: Mapping[str, object], frozen_at_utc: datetime
) -> dict[str, object]:
    """Freeze the paper-only forward clock; this grants no feed or order authority."""

    _verified_identity(frozen_artifact, "frozen_artifact_id")
    if frozen_at_utc.tzinfo is None or frozen_at_utc.utcoffset() is None:
        raise IntegrityError("forward freeze timestamp must be timezone-aware")
    candidate = frozen_artifact.get("candidate")
    model = frozen_artifact.get("model")
    if not isinstance(candidate, Mapping) or candidate not in _es_flow_candidates() or not isinstance(model, Mapping):
        raise IntegrityError("forward contract requires the selected frozen ES rescue artifact")
    core = {
        "state": "PAPER_ONLY_PROVIDER_APPROVAL_REQUIRED",
        "frozen_artifact": dict(frozen_artifact),
        "frozen_at_utc": frozen_at_utc.astimezone(timezone.utc).isoformat(),
        "first_eligible_session_rule": "FIRST_COMPLETE_SESSION_STRICTLY_AFTER_FREEZE",
        "required_data": "MES_TIMESTAMPED_EXECUTABLE_BID_ASK_WITH_ACTUAL_CONTRACT_IDENTITY_AND_CAUSAL_ES_SIGNAL",
        "paper_fill_rule": "BUY_AT_ASK_SELL_AT_BID_STOP_GAP_AT_FIRST_ADVERSE_EXECUTABLE_QUOTE",
        "missing_or_stale_quote_rule": "ABSTAIN",
        "minimum_evaluation": {"paper_trades": 100, "calendar_months": 6},
        "inconclusive_limit": {"paper_trades": 200, "calendar_months": 12},
        "bootstrap": {"seed": ES_FLOW_BOOTSTRAP_SEED, "resamples": ES_FLOW_BOOTSTRAP_RESAMPLES, "mean_block_sessions": ES_FLOW_BOOTSTRAP_BLOCK},
        "risk_limits_usd": {"initial": 250.0, "daily": 500.0, "drawdown": 1500.0},
        "feed_coverage_minimum": 0.95,
        "orders_authorized": False,
    }
    return _identified(core, "forward_contract_id")


def _forward_trade(
    contract: Mapping[str, object], session: Mapping[str, object]
) -> dict[str, object]:
    artifact = contract["frozen_artifact"]
    candidate = artifact["candidate"]  # type: ignore[index]
    model = artifact["model"]  # type: ignore[index]
    signal = session.get("signal")
    if signal is None:
        return {"disposition": "NO_SIGNAL", "terminal_net_pnl_usd": 0.0}
    if not isinstance(signal, Mapping):
        raise IntegrityError("forward signal must be an object")
    if signal.get("frozen_artifact_id") != artifact["frozen_artifact_id"]:  # type: ignore[index]
        raise IntegrityError("forward session changed the frozen artifact")
    if signal.get("causal_timing_valid") is not True or signal.get("actual_contract_identity_valid") is not True:
        raise IntegrityError("forward causal timing or contract identity violation")
    mes_identity = session.get("mes_actual_contract_identity")
    if not isinstance(mes_identity, str) or not mes_identity or not isinstance(signal.get("es_actual_contract_identity"), str) or not signal["es_actual_contract_identity"]:
        raise IntegrityError("forward ES and MES actual contract identities are required")
    features = signal.get("features")
    if not isinstance(features, Mapping) or set(features) != set(ES_FLOW_FEATURES):
        raise IntegrityError("forward signal features do not match the frozen model")
    vector = np.asarray([float(features[name]) for name in ES_FLOW_FEATURES], dtype=np.float64)
    if not np.all(np.isfinite(vector)):
        raise IntegrityError("forward signal features must be finite")
    if abs(vector[1]) < float(candidate["return_z_threshold"]) or vector[2] < float(candidate["volume_surprise_threshold"]):  # type: ignore[index]
        return {"disposition": "NO_SIGNAL", "terminal_net_pnl_usd": 0.0}
    prediction = _ridge_predict(model, vector)  # type: ignore[arg-type]
    economics = artifact["event_entry_stop_cost_risk_and_decision_rules"]["execution"]["economics"]  # type: ignore[index]
    quantity, stop_ticks, _risk_per_contract, planned_risk = _mes_quantity(float(signal["atr20_points"]), economics)  # type: ignore[arg-type]
    if quantity == 0:
        return {"disposition": "RISK_ABSTENTION", "terminal_net_pnl_usd": 0.0, "planned_initial_risk_usd": 0.0}
    frozen_cost_r = float(economics["stress_round_trip_cost_usd_per_contract"]) * quantity / planned_risk  # type: ignore[index]
    tick_r = float(economics["tick_value_usd"]) * quantity / planned_risk  # type: ignore[index]
    predicted_ratio = max(float(prediction[1]) - frozen_cost_r, 0.0) / max(float(prediction[2]) + frozen_cost_r, tick_r)
    if float(prediction[0]) < 0.25 or predicted_ratio <= 1.0:
        return {"disposition": "RIDGE_FILTERED_NO_TRADE", "terminal_net_pnl_usd": 0.0}
    if session.get("execution_data_kind") != "MES_EXECUTABLE_BID_ASK" or session.get("feed_complete") is not True:
        return {"disposition": "QUOTE_ABSTENTION", "terminal_net_pnl_usd": 0.0}
    quotes = session.get("quotes")
    fee_schedule = session.get("fee_schedule")
    if (
        not isinstance(quotes, list)
        or not quotes
        or not isinstance(fee_schedule, Mapping)
        or fee_schedule.get("independently_resolved") is not True
        or not isinstance(fee_schedule.get("fee_schedule_id"), str)
        or not isinstance(fee_schedule.get("fee_schedule_sha256"), str)
        or len(str(fee_schedule["fee_schedule_sha256"])) != 64
    ):
        return {"disposition": "QUOTE_OR_FEE_ABSTENTION", "terminal_net_pnl_usd": 0.0}
    decision_ns = int(signal["decision_at_ns"])
    horizon_ns = int(candidate["holding_horizon_minutes"]) * 60_000_000_000  # type: ignore[index]
    clean_quotes: list[tuple[int, float, float]] = []
    for quote in quotes:
        if not isinstance(quote, Mapping) or quote.get("stale") is not False:
            return {"disposition": "QUOTE_ABSTENTION", "terminal_net_pnl_usd": 0.0}
        if quote.get("actual_contract_identity") != mes_identity:
            raise IntegrityError("forward MES quote contract identity changed")
        try:
            at_ns, bid, ask = int(quote["at_ns"]), float(quote["bid"]), float(quote["ask"])
        except (KeyError, TypeError, ValueError) as exc:
            raise IntegrityError("forward quote is malformed") from exc
        if not np.isfinite(bid) or not np.isfinite(ask) or bid <= 0 or ask < bid:
            raise IntegrityError("forward executable quote is invalid")
        clean_quotes.append((at_ns, bid, ask))
    clean_quotes.sort()
    eligible = [quote for quote in clean_quotes if quote[0] > decision_ns]
    if not eligible:
        return {"disposition": "QUOTE_ABSTENTION", "terminal_net_pnl_usd": 0.0}
    direction = 1 if vector[0] > 0 else -1
    entry_at, entry_bid, entry_ask = eligible[0]
    entry_price = entry_ask if direction > 0 else entry_bid
    time_exit_ns = entry_at + horizon_ns
    tick = float(economics["tick_size"])  # type: ignore[index]
    multiplier = float(economics["contract_multiplier_usd_per_point"])  # type: ignore[index]
    stop_price = entry_price - direction * stop_ticks * tick
    favorable = 0.0
    adverse = 0.0
    exit_price: float | None = None
    exit_reason = "TIME_EXIT"
    for at_ns, bid, ask in eligible[1:]:
        executable = bid if direction > 0 else ask
        move = direction * (executable - entry_price)
        favorable = max(favorable, max(move, 0.0))
        adverse = max(adverse, max(-move, 0.0))
        if (direction > 0 and executable <= stop_price) or (direction < 0 and executable >= stop_price):
            exit_price = executable
            exit_reason = "STOP_OR_GAP_AT_FIRST_ADVERSE_EXECUTABLE_QUOTE"
            break
        if at_ns >= time_exit_ns:
            exit_price = executable
            break
    if exit_price is None:
        return {"disposition": "QUOTE_ABSTENTION", "terminal_net_pnl_usd": 0.0}
    try:
        observed_cost_per_contract = float(fee_schedule["observed_round_trip_cost_usd_per_contract"])
    except (KeyError, TypeError, ValueError) as exc:
        raise IntegrityError("forward fee schedule is incomplete") from exc
    if not np.isfinite(observed_cost_per_contract) or observed_cost_per_contract < 0:
        raise IntegrityError("forward observed cost is invalid")
    gross = direction * (exit_price - entry_price) * multiplier * quantity
    cost = observed_cost_per_contract * quantity
    mfe = favorable * multiplier * quantity
    mae = adverse * multiplier * quantity
    tick_dollars = float(economics["tick_value_usd"]) * quantity  # type: ignore[index]
    return {
        "disposition": "PAPER_TRADE",
        "quantity": quantity,
        "planned_initial_risk_usd": planned_risk,
        "entry_at_ns": entry_at,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "observed_round_trip_cost_usd": cost,
        "terminal_net_pnl_usd": gross - cost,
        "mfe_usd": mfe,
        "mae_usd": mae,
        "raw_mfe_mae": mfe / max(mae, tick_dollars),
        "net_mfe_mae": max(mfe - cost, 0.0) / max(mae + cost, tick_dollars),
    }


def evaluate_es_forward_confirmation(
    contract: Mapping[str, object], sessions: Sequence[Mapping[str, object]]
) -> dict[str, object]:
    """Evaluate immutable, executable-quote MES paper sessions; never submit orders."""

    _verified_identity(contract, "forward_contract_id")
    if contract.get("orders_authorized") is not False or contract.get("state") != "PAPER_ONLY_PROVIDER_APPROVAL_REQUIRED":
        raise IntegrityError("forward contract is not paper-only")
    frozen_at = datetime.fromisoformat(str(contract["frozen_at_utc"]))
    if not sessions:
        return {"status": "CONTINUE_COLLECTING_FORWARD_EVIDENCE", "trade_count": 0, "authoritative": False, "orders_authorized": False}
    ordered = sorted(sessions, key=lambda row: str(row.get("session_date", "")))
    dates = [str(row.get("session_date", "")) for row in ordered]
    if len(set(dates)) != len(dates) or dates != sorted(dates):
        raise IntegrityError("forward sessions must have unique chronological dates")
    if any(datetime.fromisoformat(value).date() <= frozen_at.date() for value in dates):
        raise IntegrityError("forward sessions cannot be backfilled on or before the freeze")
    if any(row.get("scheduled_session") is not True for row in ordered):
        raise IntegrityError("forward input must account for every scheduled session")
    if any(row.get("execution_data_kind") == "OHLCV" for row in ordered):
        raise IntegrityError("OHLCV-only MES data cannot confirm execution")
    results = []
    for row in ordered:
        try:
            results.append(_forward_trade(contract, row))
        except IntegrityError as exc:
            if "causal timing or contract identity" in str(exc) or "quote contract identity changed" in str(exc):
                return {
                    "status": "FAILED_FORWARD_CAUSAL_OR_IDENTITY_VIOLATION",
                    "failed_session_date": row["session_date"],
                    "authoritative": False,
                    "orders_authorized": False,
                }
            raise
    trades = [result for result in results if result["disposition"] == "PAPER_TRADE"]
    pnl = np.asarray([float(result["terminal_net_pnl_usd"]) for result in results], dtype=np.float64)
    months = np.asarray([value[:7] for value in dates])
    unique_months = sorted(set(months.tolist()))
    month_pnl = {month: float(np.sum(pnl[months == month])) for month in unique_months}
    completed_months = unique_months if ordered[-1].get("calendar_month_complete") is True else unique_months[:-1]
    feed_coverage = sum(row.get("feed_complete") is True and row.get("execution_data_kind") == "MES_EXECUTABLE_BID_ASK" for row in ordered) / len(ordered)
    risk_breach = (
        any(float(result.get("planned_initial_risk_usd", 0.0)) > 250.0 for result in results)
        or float(np.min(pnl)) < -500.0
        or _drawdown(pnl) > 1500.0
    )
    if risk_breach:
        return {"status": "FAILED_FORWARD_RISK_LIMIT", "trade_count": len(trades), "authoritative": False, "orders_authorized": False}
    trade_count = len(trades)
    month_count = len(completed_months)
    minimum_reached = trade_count >= 100 and month_count >= 6
    maximum_reached = trade_count >= 200 or month_count >= 12
    trade_pnl = np.asarray([float(result["terminal_net_pnl_usd"]) for result in trades], dtype=np.float64)
    lower = None
    median_ratio = None
    top_five = None
    if trade_count >= 2:
        indices = stationary_bootstrap_indices(
            n_observations=trade_count,
            n_resamples=ES_FLOW_BOOTSTRAP_RESAMPLES,
            mean_block_length=min(ES_FLOW_BOOTSTRAP_BLOCK, float(trade_count)),
            seed=ES_FLOW_BOOTSTRAP_SEED,
        )
        lower = _bootstrap_lower_total(trade_pnl, indices)
        median_ratio = float(np.median([float(result["net_mfe_mae"]) for result in trades]))
        positive = trade_pnl[trade_pnl > 0]
        top_five = float(np.sum(np.sort(positive)[-5:]) / np.sum(positive)) if len(positive) else float("inf")
    checks = {
        "positive_observed_cost_net_pnl": trade_count > 0 and float(np.sum(trade_pnl)) > 0,
        "bootstrap_lower_bound_above_zero": lower is not None and lower > 0,
        "two_thirds_completed_months_positive": month_count > 0 and sum(month_pnl[month] > 0 for month in completed_months) >= int(np.ceil(2 * month_count / 3)),
        "median_realized_net_mfe_mae_above_one": median_ratio is not None and median_ratio > 1.0,
        "top_five_positive_pnl_share_at_most_half": top_five is not None and top_five <= 0.5,
        "scheduled_session_feed_coverage_at_least_95_percent": feed_coverage >= 0.95,
        "no_project_risk_limit_breach": True,
    }
    if maximum_reached and not minimum_reached:
        status = "INSUFFICIENT_FORWARD_EVIDENCE"
    elif minimum_reached and all(checks.values()):
        status = "FORWARD_PAPER_VALIDATED"
    elif minimum_reached and not checks["bootstrap_lower_bound_above_zero"] and all(value for name, value in checks.items() if name != "bootstrap_lower_bound_above_zero"):
        status = "INSUFFICIENT_FORWARD_EVIDENCE" if maximum_reached else "CONTINUE_UNCHANGED_INCONCLUSIVE"
    elif minimum_reached:
        status = "FAILED_FORWARD_CONFIRMATION"
    else:
        status = "CONTINUE_COLLECTING_FORWARD_EVIDENCE"
    core = {
        "forward_contract_id": contract["forward_contract_id"],
        "evidence_label": "NON_AUTHORITATIVE_FORWARD_MECHANICS_ONLY",
        "authoritative": False,
        "status": status,
        "trade_count": trade_count,
        "calendar_month_count": month_count,
        "feed_coverage": feed_coverage,
        "observed_cost_net_pnl_usd": float(np.sum(trade_pnl)) if trade_count else 0.0,
        "bootstrap_lower_bound_usd": lower,
        "monthly_net_pnl_usd": month_pnl,
        "median_realized_net_mfe_mae": median_ratio,
        "top_five_positive_pnl_share": top_five,
        "maximum_drawdown_usd": _drawdown(pnl),
        "gate_checks": checks,
        "session_results": results,
        "orders_authorized": False,
    }
    return _identified(core, "forward_evaluation_id")


def _fold_candidate_cube(
    program: Mapping[str, object],
    materialized: Mapping[str, object],
    *,
    fit_indices: np.ndarray,
    audit_indices: np.ndarray,
    include_controls: bool = True,
    include_adverse: bool = True,
    include_exposures: bool = True,
    causal_features: tuple[np.ndarray, np.ndarray] | None = None,
    candidate_indices: Sequence[int] | None = None,
    trade_cache: dict[tuple[int, int, int, int, int, int, bool, bool], tuple[float, float, np.ndarray] | None] | None = None,
) -> dict[str, np.ndarray]:
    cube = materialized.get("minute_cube")
    if not isinstance(cube, MinuteBarCube):
        raise IntegrityError("validated minute cube is missing")
    seconds = _second_paths(materialized)
    output_markets = tuple(materialized["markets"])
    output_positions = [cube.markets.index(market) for market in output_markets]
    all_candidates = tuple(program["candidates"])
    selected_candidates = tuple(range(len(all_candidates))) if candidate_indices is None else tuple(candidate_indices)
    if not selected_candidates or any(index not in range(len(all_candidates)) for index in selected_candidates):
        raise IntegrityError("candidate subset is empty or invalid")
    candidates = tuple(all_candidates[index] for index in selected_candidates)
    campaigns = cube.campaign_count
    shape = (campaigns, len(audit_indices), len(output_markets), len(candidates))
    gross = np.zeros(shape, dtype=np.float64)
    trades = np.zeros(shape, dtype=np.bool_)
    adverse = np.full(shape, np.nan, dtype=np.float64)
    direction = np.zeros(shape, dtype=np.int8)
    event_minute = np.full(shape, -1, dtype=np.int16)
    exposures = np.zeros(shape + (len(EXPOSURES),), dtype=np.float64)
    name = str(program["program_name"])
    stress_cost = float(materialized["stress_cost_usd"])
    extreme_cost = float(materialized["extreme_cost_usd"])
    controls = np.zeros(shape + ((1 if name == "CROSS_MARKET_INFORMATION_TRANSFER" else 2),), dtype=np.float64)
    window = int(program["trigger_window_minutes"])
    causal_returns, causal_log_volume = causal_features or _causal_feature_arrays(cube, window)
    return_mean = np.nanmean(causal_returns[:, fit_indices], axis=1)
    return_scale = np.nanstd(causal_returns[:, fit_indices], axis=1, ddof=1)
    volume_mean = np.nanmean(causal_log_volume[:, fit_indices], axis=1)
    volume_scale = np.nanstd(causal_log_volume[:, fit_indices], axis=1, ddof=1)
    return_scale[return_scale == 0] = np.nan
    volume_scale[volume_scale == 0] = np.nan
    betas: dict[tuple[int, str], float] = {}
    if name == "CROSS_MARKET_INFORMATION_TRANSFER":
        for campaign in range(campaigns):
            for pair in program["pairs"]:
                leader_position = cube.markets.index(str(pair["leader"]))
                follower = str(pair["follower"])
                follower_position = cube.markets.index(follower)
                x = causal_returns[campaign, fit_indices, leader_position].reshape(-1)
                y = causal_returns[campaign, fit_indices, follower_position].reshape(-1)
                finite = np.isfinite(x) & np.isfinite(y)
                x = x[finite]
                y = y[finite]
                if len(x) < 2:
                    betas[(campaign, follower)] = 0.0
                    continue
                centered = x - np.mean(x)
                denominator = float(np.dot(centered, centered))
                betas[(campaign, follower)] = 0.0 if denominator == 0 else float(np.dot(centered, y - np.mean(y)) / denominator)
    cache = {} if trade_cache is None else trade_cache

    def cached_trade(campaign: int, date: int, market_position: int, minute: int, horizon: int, intent: int, adverse_needed: bool, exposures_needed: bool) -> tuple[float, float, np.ndarray] | None:
        key = (campaign, date, market_position, minute, horizon, intent, adverse_needed, exposures_needed)
        if key not in cache:
            cache[key] = _array_trade(cube, seconds, campaign=campaign, date=date, market_position=market_position, event_minute=minute, lookback=window, horizon=horizon, direction=intent, synthetic_campaign_id=(str(materialized["campaign_ids"][campaign]) if materialized.get("synthetic") is True else None), synthetic_kind=(str(materialized.get("synthetic_kind")) if materialized.get("synthetic") is True else None), adverse_required=adverse_needed, exposure_required=exposures_needed)
        return cache[key]
    audit = np.asarray(audit_indices, dtype=np.int64)
    first_minute = max(window, 60)
    for campaign in range(campaigns):
        for output_position, market in enumerate(output_markets):
            market_position = output_positions[output_position]
            leader = market
            leader_position = market_position
            beta = 1.0
            if name == "CROSS_MARKET_INFORMATION_TRANSFER":
                leader = "ES" if market == "NQ" else "ZN"
                leader_position = cube.markets.index(leader)
                beta = betas[(campaign, market)]
            raw = causal_returns[campaign, audit, leader_position]
            return_z = (raw - return_mean[campaign, leader_position]) / return_scale[campaign, leader_position]
            volume_z = (causal_log_volume[campaign, audit, leader_position] - volume_mean[campaign, leader_position]) / volume_scale[campaign, leader_position]
            leader_available_windows = np.lib.stride_tricks.sliding_window_view(
                cube.available_at_ns[campaign, audit, leader_position], window + 1, axis=1
            )
            leader_identity_windows = np.lib.stride_tricks.sliding_window_view(
                cube.identity_code[campaign, audit, leader_position], window + 1, axis=1
            )
            leader_observed_windows = np.lib.stride_tricks.sliding_window_view(
                cube.observed[campaign, audit, leader_position], window + 1, axis=1
            )
            leader_decisions = cube.event_at_ns[campaign, audit, leader_position, window:] + 65_000_000_000
            leader_dependencies_ready = np.max(leader_available_windows, axis=2) <= leader_decisions
            leader_identity_continuous = np.all(
                leader_identity_windows == leader_identity_windows[..., -1:], axis=2
            ) & np.all(leader_observed_windows, axis=2)
            for candidate_position, candidate in enumerate(candidates):
                horizon = int(candidate["holding_horizon_minutes"])
                threshold = float(candidate.get("return_z_threshold", candidate.get("leader_return_z_threshold")))
                secondary = float(candidate.get("volume_surprise_threshold", candidate.get("incorporation_maximum_fraction", 0.0)))
                latest_event = {15: 368, 30: 353, 60: 323}[horizon]
                start = first_minute - window
                stop = latest_event - window + 1
                candidate_mask = (
                    np.isfinite(return_z[:, start:stop])
                    & np.isfinite(volume_z[:, start:stop])
                    & (np.abs(return_z[:, start:stop]) >= threshold)
                    & leader_dependencies_ready[:, start:stop]
                    & leader_identity_continuous[:, start:stop]
                )
                if name == "CROSS_MARKET_INFORMATION_TRANSFER":
                    follower = causal_returns[campaign, audit, market_position, start:stop]
                    implied = beta * raw[:, start:stop]
                    incorporation = np.divide(np.abs(follower), np.abs(implied), out=np.full_like(follower, np.inf), where=implied != 0)
                    leader_ns = cube.event_at_ns[campaign, audit, leader_position, first_minute:latest_event + 1]
                    follower_ns = cube.event_at_ns[campaign, audit, market_position, first_minute:latest_event + 1]
                    follower_available = cube.available_at_ns[campaign, audit, market_position, first_minute:latest_event + 1]
                    follower_available_windows = np.lib.stride_tricks.sliding_window_view(
                        cube.available_at_ns[campaign, audit, market_position], window + 1, axis=1
                    )[:, start:stop]
                    follower_identity_windows = np.lib.stride_tricks.sliding_window_view(
                        cube.identity_code[campaign, audit, market_position], window + 1, axis=1
                    )[:, start:stop]
                    follower_observed_windows = np.lib.stride_tricks.sliding_window_view(
                        cube.observed[campaign, audit, market_position], window + 1, axis=1
                    )[:, start:stop]
                    candidate_mask &= (
                        (beta != 0)
                        & (incorporation <= secondary)
                        & (follower_ns <= leader_ns)
                        & (leader_ns - follower_ns <= 60_000_000_000)
                        & (np.max(follower_available_windows, axis=2) <= leader_ns + 65_000_000_000)
                        & np.all(follower_identity_windows == follower_identity_windows[..., -1:], axis=2)
                        & np.all(follower_observed_windows, axis=2)
                    )
                else:
                    candidate_mask &= volume_z[:, start:stop] >= secondary
                qualifying = np.any(candidate_mask, axis=1)
                first_offsets = np.argmax(candidate_mask, axis=1)
                qualifying_positions = np.flatnonzero(qualifying)
                if not include_controls and not include_adverse and not include_exposures and len(qualifying_positions):
                    dates_vector = audit[qualifying_positions]
                    minutes_vector = first_minute + first_offsets[qualifying_positions]
                    raw_returns = raw[qualifying_positions, minutes_vector - window]
                    intents = np.sign(beta * raw_returns if name == "CROSS_MARKET_INFORMATION_TRANSFER" else raw_returns).astype(np.int8)
                    if name == "LIQUIDITY_SHOCK_REVERSAL":
                        intents *= -1
                    nonzero = intents != 0
                    qualifying_positions = qualifying_positions[nonzero]
                    dates_vector = dates_vector[nonzero]
                    minutes_vector = minutes_vector[nonzero]
                    intents = intents[nonzero]
                    if len(qualifying_positions):
                        decision_ns = cube.event_at_ns[campaign, dates_vector, market_position, minutes_vector] + 65_000_000_000
                        lookback_minutes = minutes_vector[:, None] + np.arange(-window, 1)
                        entries = minutes_vector + 2
                        exits = entries + horizon
                        identity_minutes = minutes_vector[:, None] + np.arange(-window, horizon + 3)
                        identities = cube.identity_code[campaign, dates_vector[:, None], market_position, identity_minutes]
                        valid = (
                            np.all(cube.available_at_ns[campaign, dates_vector[:, None], market_position, lookback_minutes] <= decision_ns[:, None], axis=1)
                            & np.all(cube.observed[campaign, dates_vector[:, None], market_position, lookback_minutes], axis=1)
                            & (cube.event_at_ns[campaign, dates_vector, market_position, entries] > decision_ns)
                            & (cube.event_at_ns[campaign, dates_vector, market_position, entries - 1] <= decision_ns)
                            & np.all(cube.observed[campaign, dates_vector[:, None], market_position, identity_minutes], axis=1)
                            & np.all(identities == identities[:, -1:], axis=1)
                        )
                        qualifying_positions = qualifying_positions[valid]
                        dates_vector = dates_vector[valid]
                        entries = entries[valid]
                        exits = exits[valid]
                        intents = intents[valid]
                        if len(qualifying_positions):
                            tick_sizes = cube.tick_size[campaign, dates_vector, market_position, entries]
                            pnls = np.round(
                                intents
                                * (cube.open[campaign, dates_vector, market_position, exits] - cube.open[campaign, dates_vector, market_position, entries])
                                / tick_sizes
                                * cube.tick_value[campaign, dates_vector, market_position, entries],
                                10,
                            )
                            gross[campaign, qualifying_positions, output_position, candidate_position] = pnls
                            trades[campaign, qualifying_positions, output_position, candidate_position] = True
                            direction[campaign, qualifying_positions, output_position, candidate_position] = intents
                            event_minute[campaign, qualifying_positions, output_position, candidate_position] = minutes_vector[valid]
                    continue
                for audit_position in qualifying_positions:
                    date = int(audit[audit_position])
                    minute = first_minute + int(first_offsets[audit_position])
                    raw_return = float(raw[audit_position, minute - window])
                    if name == "CROSS_MARKET_INFORMATION_TRANSFER":
                        intent = int(np.sign(beta * raw_return))
                        decision_ns = int(cube.event_at_ns[campaign, date, leader_position, minute]) + 65_000_000_000
                        if np.any(cube.available_at_ns[campaign, date, leader_position, minute - window:minute + 1] > decision_ns):
                            continue
                    else:
                        intent = int(np.sign(raw_return)) * (-1 if name == "LIQUIDITY_SHOCK_REVERSAL" else 1)
                    if intent == 0:
                        continue
                    realized = cached_trade(campaign, date, market_position, minute, horizon, intent, include_adverse, include_exposures)
                    if realized is None:
                        continue
                    pnl, adverse_value, exposure = realized
                    exposure = exposure.copy()
                    exposure[EXPOSURES.index("liquidity_surprise")] = float(volume_z[audit_position, minute - window])
                    gross[campaign, audit_position, output_position, candidate_position] = pnl
                    trades[campaign, audit_position, output_position, candidate_position] = True
                    adverse[campaign, audit_position, output_position, candidate_position] = adverse_value
                    direction[campaign, audit_position, output_position, candidate_position] = intent
                    event_minute[campaign, audit_position, output_position, candidate_position] = minute
                    exposures[campaign, audit_position, output_position, candidate_position] = exposure
                causal_signal = leader_dependencies_ready[:, start:stop] & leader_identity_continuous[:, start:stop]
                if include_controls and name != "CROSS_MARKET_INFORMATION_TRANSFER":
                    low_mask = np.isfinite(return_z[:, start:stop]) & np.isfinite(volume_z[:, start:stop]) & (np.abs(return_z[:, start:stop]) >= threshold) & (volume_z[:, start:stop] <= 0.0) & causal_signal
                    control_masks = [low_mask]
                    if name == "LIQUIDITY_SHOCK_REVERSAL":
                        control_masks.append(np.isfinite(return_z[:, start:stop]) & (np.abs(return_z[:, start:stop]) >= threshold) & causal_signal)
                    for control_id, control_mask in enumerate(control_masks):
                        control_qualifying = np.any(control_mask, axis=1)
                        control_first = np.argmax(control_mask, axis=1)
                        for audit_position in np.flatnonzero(control_qualifying):
                            date = int(audit[audit_position])
                            minute = first_minute + int(control_first[audit_position])
                            intent = int(np.sign(raw[audit_position, minute - window])) * (-1 if name == "LIQUIDITY_SHOCK_REVERSAL" else 1)
                            if intent == 0:
                                continue
                            realized = cached_trade(campaign, date, market_position, minute, horizon, intent, False, False)
                            if realized is not None:
                                controls[campaign, audit_position, output_position, candidate_position, control_id] = realized[0] - stress_cost
                elif include_controls:
                    follower_mean = return_mean[campaign, market_position]
                    follower_scale = return_scale[campaign, market_position]
                    follower_raw = causal_returns[campaign, audit, market_position]
                    follower_z = (follower_raw - follower_mean) / follower_scale
                    inverse_beta = 0.0 if beta == 0 else 1.0 / beta
                    implied_leader = inverse_beta * follower_raw[:, start:stop]
                    incorporation = np.divide(np.abs(raw[:, start:stop]), np.abs(implied_leader), out=np.full_like(implied_leader, np.inf), where=implied_leader != 0)
                    follower_decisions = follower_ns + 65_000_000_000
                    swapped_mask = (
                        np.isfinite(follower_z[:, start:stop])
                        & (np.abs(follower_z[:, start:stop]) >= threshold)
                        & (inverse_beta != 0)
                        & (incorporation <= secondary)
                        & causal_signal
                        & (np.max(follower_available_windows, axis=2) <= follower_decisions)
                        & np.all(follower_identity_windows == follower_identity_windows[..., -1:], axis=2)
                        & np.all(follower_observed_windows, axis=2)
                    )
                    swapped_qualifying = np.any(swapped_mask, axis=1)
                    swapped_first = np.argmax(swapped_mask, axis=1)
                    for audit_position in np.flatnonzero(swapped_qualifying):
                        date = int(audit[audit_position])
                        minute = first_minute + int(swapped_first[audit_position])
                        intent = int(np.sign(implied_leader[audit_position, int(swapped_first[audit_position])]))
                        realized = cached_trade(campaign, date, leader_position, minute, horizon, intent, False, False)
                        if realized is not None:
                            controls[campaign, audit_position, output_position, candidate_position, 0] = realized[0] - stress_cost
    stress = gross - trades * stress_cost
    if include_controls and name == "FLOW_CONTINUATION":
        for campaign in range(campaigns):
            for output_position, market_position in enumerate(output_positions):
                for candidate_position, candidate in enumerate(candidates):
                    horizon = int(candidate["holding_horizon_minutes"])
                    for audit_position, raw_date in enumerate(audit):
                        source = audit_position - 5
                        if source < 0:
                            continue
                        minute = int(event_minute[campaign, source, output_position, candidate_position])
                        intent = int(direction[campaign, source, output_position, candidate_position])
                        if minute < 0 or intent == 0:
                            continue
                        realized = cached_trade(campaign, int(raw_date), market_position, minute, horizon, intent, False, False)
                        if realized is not None:
                            controls[campaign, audit_position, output_position, candidate_position, 1] = realized[0] - stress_cost
    return {"gross": gross, "base": gross - trades * (stress_cost / 2.0), "stress": stress, "extreme": gross - trades * extreme_cost, "trades": trades, "adverse": adverse, "direction": direction, "event_minute": event_minute, "exposures": exposures, "controls": controls}


def _ols(train_x: np.ndarray, train_y: np.ndarray, test_x: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    coefficients = np.linalg.lstsq(train_x, train_y, rcond=None)[0]
    return coefficients, test_x @ coefficients


def _shared_date_lower(
    values: np.ndarray,
    *,
    indices: np.ndarray | None = None,
    seed: int | None = None,
    resamples: int | None = None,
) -> float:
    """Bootstrap dates once; market columns remain synchronized."""

    if values.ndim != 2 or len(values) < 2:
        raise IntegrityError("date-by-market values are required")
    if indices is None:
        if seed is None or resamples is None:
            raise IntegrityError("shared bootstrap indices or seed and resamples are required")
        indices = stationary_bootstrap_indices(
            n_observations=len(values),
            n_resamples=resamples,
            mean_block_length=min(20.0, float(len(values))),
            seed=seed,
        )
    if indices.dtype != np.dtype(np.int64) or indices.ndim != 2 or indices.shape[1] != len(values) or np.any((indices < 0) | (indices >= len(values))):
        raise IntegrityError("shared bootstrap indices do not align with exchange dates")
    date_means = np.mean(values, axis=1)
    means = np.mean(date_means[indices], axis=1)
    return float(np.quantile(means, 0.05, method="lower"))


def _tail_gates(values: np.ndarray, adverse: np.ndarray) -> tuple[dict[str, object], list[str]]:
    date_pnl = np.mean(values, axis=1)
    equity = np.cumsum(date_pnl)
    peaks = np.maximum.accumulate(np.r_[0.0, equity])[1:]
    drawdown = float(np.max(peaks - equity))
    duration = longest = 0
    for below_peak in equity < peaks:
        duration = duration + 1 if below_peak else 0
        longest = max(longest, duration)
    total = float(np.sum(date_pnl))
    positive = np.maximum(date_pnl, 0.0)
    single = float(np.max(positive) / total) if total > 0 else 1.0
    five = float(max(np.sum(date_pnl[i:i + 5]) for i in range(len(date_pnl) - 4)) / total) if total > 0 else 1.0
    best_five_removed = float(np.sum(np.sort(date_pnl)[:-5]))
    ordered = np.sort(date_pnl)
    es25 = float(np.mean(ordered[:max(1, int(np.ceil(len(ordered) * 0.025)))]))
    es5 = float(np.mean(ordered[:max(1, int(np.ceil(len(ordered) * 0.05)))]))
    centered = date_pnl - np.mean(date_pnl)
    variance = float(np.mean(centered ** 2))
    finite_adverse = adverse[np.isfinite(adverse)]
    report = {"maximum_drawdown_usd": drawdown, "maximum_drawdown_duration_sessions": longest, "best_five_removed_pnl_usd": best_five_removed, "single_session_profit_share": single, "contiguous_five_profit_share": five, "expected_shortfall_2_5": es25, "expected_shortfall_5": es5, "skew": 0.0 if variance == 0 else float(np.mean(centered ** 3) / variance ** 1.5), "kurtosis_non_excess": 0.0 if variance == 0 else float(np.mean(centered ** 4) / variance ** 2), "one_second_adverse_minimum_usd": None if len(finite_adverse) == 0 else float(np.min(finite_adverse)), "one_second_complete": bool(np.all(np.isfinite(adverse)))}
    failures = []
    if drawdown > 1500:
        failures.append("MAXIMUM_DRAWDOWN")
    if best_five_removed <= 0:
        failures.append("BEST_FIVE_REMOVED")
    if max(single, five) > 0.5:
        failures.append("EVENT_CONCENTRATION")
    if not report["one_second_complete"]:
        failures.append("ONE_SECOND_ADVERSE_PATH")
    return report, failures


def _scientific_report(
    *,
    gross: np.ndarray,
    base: np.ndarray,
    stress: np.ndarray,
    extreme: np.ndarray,
    direction: np.ndarray,
    regimes: Mapping[str, np.ndarray],
    adverse: np.ndarray,
) -> tuple[dict[str, object], list[str]]:
    tail, failures = _tail_gates(stress, adverse)
    session = np.mean(stress, axis=1)
    ordered = np.sort(session)
    regime_report: dict[str, object] = {}
    for name, labels in regimes.items():
        if labels.shape != stress.shape:
            raise IntegrityError("regime labels must align date by market")
        regime_report[name] = {
            label: float(np.mean(stress[labels == label]))
            for label in sorted(set(labels.reshape(-1).tolist()))
            if label != "UNOBSERVED"
        }
    report = {
        "costs": {
            name: {"mean_usd": float(np.mean(values)), "total_usd": float(np.sum(values))}
            for name, values in (("gross", gross), ("base", base), ("stress", stress), ("extreme", extreme))
        },
        "direction_attribution": {
            "long_usd": float(np.sum(stress[direction > 0])),
            "short_usd": float(np.sum(stress[direction < 0])),
            "flat_usd": float(np.sum(stress[direction == 0])),
        },
        "regimes": regime_report,
        "worst_sessions": {
            str(count): {"sum_usd": float(np.sum(ordered[:count])), "values": ordered[:count].tolist()}
            for count in (1, 5, 10, 20)
        },
        "tail": tail,
    }
    return report, failures


def materialize_program_inputs(
    specification: Mapping[str, object],
    program_name: str,
    *,
    bars: Sequence[CausalBar] | MinuteBarCube,
    one_second_bars: Sequence[CausalSecondBar],
    stress_cost_usd: float,
    extreme_cost_usd: float,
    campaign_ids: Sequence[str] | None = None,
    resamples: int = 999,
) -> dict[str, object]:
    """Create the sole evaluator input cube from causal, registration-bound facts.

    Callers provide observations, never folds, controls, residuals, regimes,
    baselines, selection results, or evidence.  Those are derived here or in
    the pure evaluator.
    """

    if specification.get("schema_version") != PROGRAM_SCHEMA:
        raise IntegrityError("the frozen program schema is required")
    program = next((p for p in specification["programs"] if p["program_name"] == program_name), None)
    if program is None:
        raise IntegrityError("unknown program")
    output_scope = tuple(sorted(tuple(program.get("markets", ())) or ("NQ", "ZB")))
    source_scope = output_scope if program_name != "CROSS_MARKET_INFORMATION_TRANSFER" else ("ES", "NQ", "ZB", "ZN")
    if isinstance(bars, MinuteBarCube):
        bars.validate()
        if bars.markets != source_scope:
            raise IntegrityError("minute cube markets do not match the frozen source scope")
        dates = bars.exchange_session_dates
        _schedule(dates)
        campaign_count = bars.campaign_count
        cube = bars
    else:
        minute_rows, dates = validate_causal_bars(bars, source_scope)
        campaign_count = 1 + max(row.campaign_id for row in minute_rows)
        cube = _minute_cube(minute_rows, dates=dates, markets=source_scope, campaign_count=campaign_count)
    ids = tuple(campaign_ids or ("REAL_BATCH_0",))
    if len(ids) != campaign_count or len(set(ids)) != campaign_count or any(not isinstance(value, str) or not value for value in ids):
        raise IntegrityError("one unique nonempty campaign ID is required per campaign")
    seconds = validate_causal_seconds(one_second_bars, required_markets=output_scope, campaign_count=campaign_count, dates=dates)
    stress_cost = float(stress_cost_usd)
    extreme_cost = float(extreme_cost_usd)
    frozen_costs = specification.get("costs_usd_per_trade")
    if (
        not isinstance(frozen_costs, Mapping)
        or not np.isfinite(stress_cost)
        or not np.isfinite(extreme_cost)
        or stress_cost != float(frozen_costs.get("stress", np.nan))
        or extreme_cost != float(frozen_costs.get("extreme", np.nan))
    ):
        raise IntegrityError("cost ladder is invalid")
    if type(resamples) is not int or resamples < 1:
        raise IntegrityError("inference resample count is invalid")
    inference_seed = int(sha256_json({"program_id": specification["program_id"], "program_name": program_name, "campaign_ids": ids})[:16], 16)
    return {
        "program_name": program_name,
        "exchange_session_dates": dates,
        "markets": output_scope,
        "source_markets": source_scope,
        "campaign_count": campaign_count,
        "campaign_ids": ids,
        "minute_cube": cube,
        "one_second_bars": seconds,
        "stress_cost_usd": stress_cost,
        "extreme_cost_usd": extreme_cost,
        "seed": inference_seed,
        "resamples": int(resamples),
        "canonical_order": "CAMPAIGN_EXCHANGE_SESSION_DATE_MARKET_EVENT_AT",
        "caller_supplied_results": False,
    }


def _synthetic_dates(count: int = 1010) -> tuple[str, ...]:
    result: list[str] = []
    current = datetime(2018, 1, 2)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.date().isoformat())
        current += timedelta(days=1)
    return tuple(result)


@lru_cache(maxsize=4)
def _synthetic_event_rows(dates: tuple[str, ...]) -> np.ndarray:
    rows = np.empty((len(dates), 391), dtype=np.int64)
    for date_index, raw_date in enumerate(dates):
        start = datetime.fromisoformat(raw_date).replace(hour=8, minute=30, tzinfo=CHICAGO)
        rows[date_index] = np.asarray([_datetime_ns(start + timedelta(minutes=minute)) for minute in range(391)], dtype=np.int64)
    rows.flags.writeable = False
    return rows


def _known_edge_series(active: np.ndarray, signs: np.ndarray, z: float) -> np.ndarray:
    """Make each frozen 63-session audit block have one bounded z-score."""

    values = np.where(active, signs, 0.0).astype(np.float64)
    for start in range(506, len(values), 63):
        fit = values[:start - 1]
        mean = float(np.mean(fit))
        scale = float(np.std(fit, ddof=1))
        stop = min(len(values), start + 63)
        values[start:stop] = np.where(
            active[start:stop], mean + signs[start:stop] * z * scale, mean
        )
    return values


def _known_edge_minute_cube(campaign_ids: Sequence[str]) -> MinuteBarCube:
    dates = _synthetic_dates(1010)
    markets = ("CL", "ES", "GC", "NQ")
    campaigns, date_count, market_count, minutes = len(campaign_ids), len(dates), len(markets), 391
    shape = (campaigns, date_count, market_count, minutes)
    event = np.broadcast_to(_synthetic_event_rows(dates)[None, :, None, :], shape).copy()
    available = event + 60_000_000_000
    open_values = np.empty(shape, dtype=np.float64)
    close = np.empty(shape, dtype=np.float64)
    high = np.empty(shape, dtype=np.float64)
    low = np.empty(shape, dtype=np.float64)
    volume = np.empty(shape, dtype=np.float64)
    tick_size = np.empty(shape, dtype=np.float64)
    tick_value = np.empty(shape, dtype=np.float64)
    roll_count = (date_count + 62) // 63 + 1
    labels = tuple(f"{market}-R{roll:02d}" for market in markets for roll in range(roll_count))
    label_index = {label: index for index, label in enumerate(labels)}
    identity = np.empty(shape, dtype=np.int32)
    economics = {"CL": (0.01, 10.0, 70.0, 2), "ES": (0.25, 12.5, 4500.0, 2), "GC": (0.1, 10.0, 1800.0, 2), "NQ": (0.25, 5.0, 15000.0, 3)}
    session = np.arange(date_count)
    signs = np.where((session // 2) % 2 == 0, 1.0, -1.0)
    for campaign in range(campaigns):
        for market_position, market in enumerate(markets):
            tick, value, base, exit_ticks = economics[market]
            active = (session % 2 == 0) if market in {"ES", "NQ"} else (session % 2 == 1)
            return_signal = _known_edge_series(active, signs, 1.72)
            volume_signal = _known_edge_series(active, np.ones(date_count), 0.72)
            open_values[campaign, :, market_position, :] = base
            close[campaign, :, market_position, :] = base
            volume[campaign, :, market_position, :] = np.expm1(5.0)
            close[campaign, :, market_position, 120] = base * (1.0 + 0.0005 * return_signal)
            close[campaign, :, market_position, 119] = close[campaign, :, market_position, 120]
            volume[campaign, :, market_position, 120] = np.expm1(5.0 + 0.05 * volume_signal)
            for date in np.flatnonzero(active):
                direction = signs[date]
                open_values[campaign, date, market_position, 137] = base + direction * exit_ticks * tick
            high[campaign, :, market_position] = np.maximum(open_values[campaign, :, market_position], close[campaign, :, market_position]) + tick
            low[campaign, :, market_position] = np.minimum(open_values[campaign, :, market_position], close[campaign, :, market_position]) - tick
            tick_size[campaign, :, market_position, :] = tick
            tick_value[campaign, :, market_position, :] = value
            for date in range(date_count):
                identity[campaign, date, market_position, :] = label_index[f"{market}-R{date // 63:02d}"]
    cube = MinuteBarCube(dates, markets, labels, event, available, identity, np.ones(shape, dtype=np.bool_), open_values, high, low, close, volume, tick_size, tick_value)
    cube.validate()
    return cube


def generate_synthetic_minute_cube(
    *,
    program_name: str,
    campaign_ids: Sequence[str],
    kind: str,
) -> MinuteBarCube:
    """Generate deterministic market-shaped synthetic causal observations."""

    ids = tuple(campaign_ids)
    if not ids or len(set(ids)) != len(ids) or kind not in {"NULL", "KNOWN_EDGE", "MISSINGNESS", "GAMMA_TRAP"}:
        raise IntegrityError("synthetic campaign declaration is invalid")
    if kind == "KNOWN_EDGE" and program_name == "FLOW_CONTINUATION":
        return _known_edge_minute_cube(ids)
    markets = ("CL", "ES", "GC", "NQ") if program_name != "CROSS_MARKET_INFORMATION_TRANSFER" else ("ES", "NQ", "ZB", "ZN")
    source_dates = _synthetic_dates(1012)
    missing_session_positions = {211, 733} if len(source_dates) >= 1012 else set()
    dates = tuple(raw_date for index, raw_date in enumerate(source_dates) if index not in missing_session_positions)
    campaigns, date_count, market_count, minutes = len(ids), len(dates), len(markets), 391
    shape = (campaigns, date_count, market_count, minutes)
    event = np.broadcast_to(_synthetic_event_rows(dates)[None, :, None, :], shape).copy()
    available = event + 60_000_000_000
    open_values = np.empty(shape, dtype=np.float64)
    high = np.empty(shape, dtype=np.float64)
    low = np.empty(shape, dtype=np.float64)
    close = np.empty(shape, dtype=np.float64)
    volume = np.empty(shape, dtype=np.float64)
    tick_by_market = {"CL": (0.01, 10.0, 70.0), "ES": (0.25, 12.5, 4500.0), "GC": (0.1, 10.0, 1800.0), "NQ": (0.25, 5.0, 15000.0), "ZB": (1 / 32, 31.25, 130.0), "ZN": (1 / 64, 15.625, 110.0)}
    tick_size = np.empty(shape, dtype=np.float64)
    tick_value = np.empty(shape, dtype=np.float64)
    roll_count = (date_count + 62) // 63 + 1
    labels = tuple(f"{market}-R{roll:02d}" for market in markets for roll in range(roll_count))
    label_index = {label: index for index, label in enumerate(labels)}
    identity = np.empty(shape, dtype=np.int32)
    minute_axis = np.arange(minutes, dtype=np.float64)
    seasonality = 0.65 + 1.35 * np.abs(minute_axis - 195.0) / 195.0
    market_loading = np.linspace(0.75, 1.15, market_count)
    for campaign, campaign_id in enumerate(ids):
        seed = int(sha256_json({"campaign_id": campaign_id})[:16], 16)
        generator = np.random.Generator(np.random.PCG64(seed))
        volatility_state = np.empty(date_count, dtype=np.float64)
        volatility_state[0] = 1.0
        innovations = generator.normal(0.0, 0.18, size=date_count)
        for date in range(1, date_count):
            volatility_state[date] = max(0.35, 0.92 * volatility_state[date - 1] + 0.08 + innovations[date])
        shared = generator.normal(size=(date_count, minutes))
        idiosyncratic = generator.normal(size=(date_count, market_count, minutes))
        returns = (shared[:, None, :] * market_loading[None, :, None] + 0.55 * idiosyncratic) * (0.00012 * volatility_state[:, None, None] * seasonality[None, None, :])
        tail_mask = generator.random((date_count, minutes)) < 0.00045
        tail_shocks = generator.normal(0.0, 0.012, size=(date_count, minutes)) * tail_mask
        returns += tail_shocks[:, None, :] * market_loading[None, :, None]
        if kind == "KNOWN_EDGE" and program_name == "FLOW_CONTINUATION":
            event_dates = np.flatnonzero(generator.random(date_count) < 0.12)
            signs = generator.choice(np.asarray((-1.0, 1.0)), size=len(event_dates))
            for position, date in enumerate(event_dates):
                sign = signs[position]
                returns[date, :, 120] += sign * 0.0035
                returns[date, :, 123:138] += sign * 0.00008
        sparse_sessions = generator.random((date_count, market_count)) < 0.035
        stale_sessions = generator.random((date_count, market_count)) < 0.015
        intraday_rolls = generator.random((date_count, market_count)) < 0.01
        for market_position, market in enumerate(markets):
            tick, value, starting_price = tick_by_market[market]
            tick_size[campaign, :, market_position, :] = tick
            tick_value[campaign, :, market_position, :] = value
            previous_close = starting_price
            for date in range(date_count):
                overnight_gap = generator.normal(0.0, 0.0018 * volatility_state[date])
                session_open = previous_close * np.exp(overnight_gap)
                path = session_open * np.exp(np.cumsum(returns[date, market_position]))
                opens = np.r_[session_open, path[:-1]]
                spread = np.maximum(tick, np.abs(path - opens) * 0.35 + generator.exponential(tick * 0.25, size=minutes))
                open_values[campaign, date, market_position] = opens
                close[campaign, date, market_position] = path
                high[campaign, date, market_position] = np.maximum(opens, path) + spread
                low[campaign, date, market_position] = np.minimum(opens, path) - spread
                activity = (1.0 + 1400.0 * np.abs(returns[date, market_position])) * (1.3 - 0.55 * np.abs(minute_axis - 195.0) / 195.0)
                volume[campaign, date, market_position] = np.maximum(0.0, 150.0 * activity * generator.lognormal(0.0, 0.32, size=minutes))
                if sparse_sessions[date, market_position]:
                    volume[campaign, date, market_position] *= 0.01
                if stale_sessions[date, market_position]:
                    available[campaign, date, market_position, 120] = event[campaign, date, market_position, 120] + 70_000_000_000
                roll = date // 63
                identity[campaign, date, market_position] = label_index[f"{market}-R{roll:02d}"]
                if intraday_rolls[date, market_position]:
                    identity[campaign, date, market_position, 200:] = label_index[f"{market}-R{roll + 1:02d}"]
                previous_close = float(path[-1])
    cube = MinuteBarCube(dates, markets, labels, event, available, identity, np.ones(shape, dtype=np.bool_), open_values, high, low, close, volume, tick_size, tick_value)
    cube.validate()
    return cube


def _merkle_digest(leaves: Sequence[str]) -> str:
    level = list(leaves)
    if not level:
        raise IntegrityError("Merkle tree requires at least one campaign outcome")
    while len(level) > 1:
        if len(level) % 2:
            level.append(level[-1])
        level = [sha256_json({"left": level[index], "right": level[index + 1]}) for index in range(0, len(level), 2)]
    return level[0]


def _known_edge_path(cube: MinuteBarCube, campaign: int) -> np.ndarray:
    path = np.zeros((len(cube.exchange_session_dates), len(cube.markets)), dtype=np.float64)
    for date in range(len(cube.exchange_session_dates)):
        active = {"ES", "NQ"} if date % 2 == 0 else {"CL", "GC"}
        direction = 1 if (date // 2) % 2 == 0 else -1
        for market_position, market in enumerate(cube.markets):
            if market not in active:
                continue
            trade = _array_trade(
                cube, {}, campaign=campaign, date=date, market_position=market_position,
                event_minute=120, lookback=15, horizon=15, direction=direction,
                adverse_required=False, exposure_required=False,
            )
            if trade is None:
                raise IntegrityError("frozen known-edge trade did not materialize")
            path[date, market_position] = trade[0] - 5.0
    if not np.array_equal(np.sum(path, axis=1), np.full(len(path), 30.0)):
        raise IntegrityError("frozen known-edge path is not exactly $30 per complete session")
    return path


def run_synthetic_campaign_chunk(
    specification: Mapping[str, object],
    *,
    campaign_ids: Sequence[str],
    kind: str,
    resamples: int,
) -> dict[str, object]:
    """Run a bounded chunk through the production materializer and evaluator."""

    ids = tuple(campaign_ids)
    if not ids or resamples < 1:
        raise IntegrityError("synthetic campaign chunk is empty or invalid")
    decisions_by_program: dict[str, tuple[dict[str, object], ...]] = {}
    known_edge_paths: tuple[np.ndarray, ...] = ()
    for program in specification["programs"]:
        name = str(program["program_name"])
        cube = generate_synthetic_minute_cube(program_name=name, campaign_ids=ids, kind=kind)
        materialized = materialize_program_inputs(specification, name, bars=cube, one_second_bars=(), stress_cost_usd=5.0, extreme_cost_usd=10.0, campaign_ids=ids, resamples=resamples)
        materialized["synthetic"] = True
        materialized["synthetic_kind"] = kind
        decisions_by_program[name] = evaluate_program_decision_batch(specification, materialized)
        if kind == "KNOWN_EDGE" and name == "FLOW_CONTINUATION":
            known_edge_paths = tuple(_known_edge_path(cube, campaign) for campaign in range(len(ids)))
    terminals: list[dict[str, object]] = []
    for campaign, campaign_id in enumerate(ids):
        program_decisions = [decisions_by_program[name][campaign] for name in sorted(decisions_by_program)]
        passing = [decision for decision in program_decisions if decision["outcome"] == "PASS"]
        portfolio = evaluate_portfolio_decision(specification, program_decisions) if len(passing) >= 2 else None
        edge_detected = False
        edge_path_id = None
        if kind == "KNOWN_EDGE":
            flow = decisions_by_program["FLOW_CONTINUATION"][campaign]
            expected = known_edge_paths[campaign][-504:]
            observed = np.asarray(flow["stress_date_market"], dtype=np.float64)
            edge_path_id = sha256_json(expected.tolist())
            edge_detected = bool(
                flow["outcome"] == "PASS"
                and flow["selected_candidate_indices"] == [0] * 8
                and np.array_equal(observed, expected)
            )
        terminal = {
            "campaign_id": campaign_id,
            "kind": kind,
            "program_outcomes": {decision["program_name"]: decision["outcome"] for decision in program_decisions},
            "program_calculation_ids": {decision["program_name"]: decision["calculation_id"] for decision in program_decisions},
            "portfolio_outcome": None if portfolio is None else portfolio["outcome"],
            "portfolio_calculation_id": None if portfolio is None else portfolio["calculation_id"],
            "promoted": bool(passing or (portfolio is not None and portfolio["outcome"] == "PASS")),
            "known_edge_detected": edge_detected,
            "known_edge_path_sha256": edge_path_id,
        }
        terminal["terminal_id"] = sha256_json(terminal)
        terminals.append(terminal)
    core = {
        "schema_version": SYNTHETIC_CHUNK_SCHEMA,
        "kind": kind,
        "campaign_ids": list(ids),
        "resamples": resamples,
        "terminal_outcomes": terminals,
        "merkle_digest": _merkle_digest([str(item["terminal_id"]) for item in terminals]),
        "stores_intermediate_statistics": False,
        "production_materializer_and_evaluator": True,
    }
    return _identified(core, "chunk_id")


def synthetic_certification_manifest(
    specification: Mapping[str, object], *, chunk_size: int, workers: int = 1,
    performance_evidence: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if type(chunk_size) is not int or chunk_size < 1 or type(workers) is not int or workers < 1:
        raise IntegrityError("synthetic chunk size and worker count must be positive")
    core = {
        "schema_version": SYNTHETIC_SCHEMA,
        "state": "RUNNER_PREPARED_FINAL_CAMPAIGNS_NOT_EXECUTED",
        "program_id": specification["program_id"],
        "program": {
            "program_id": specification["program_id"],
            "path": (PROGRAM_ROOT / str(specification["program_id"]) / "program.json").as_posix(),
        },
        "campaigns": {"null": 10_000, "known_edge": 5_000, "known_edge_usd_per_complete_session": 30.0},
        "romano_wolf_bootstrap_resamples": 10_000,
        "known_edge_target": {"program_name": "FLOW_CONTINUATION", "other_programs": "NULL"},
        "chunk_size": chunk_size,
        "workers": workers,
        "performance_limits": {"maximum_elapsed_seconds": SYNTHETIC_MAX_ELAPSED_SECONDS, "maximum_physical_ram_fraction": 0.5},
        "performance_evidence": None if performance_evidence is None else dict(performance_evidence),
        "campaign_id_formats": {"null": "NULL_{ordinal:05d}", "known_edge": "KNOWN_EDGE_{ordinal:05d}"},
        "seed_derivation": "FIRST_64_BITS_SHA256_CANONICAL_CAMPAIGN_ID",
        "required_pipeline": ["MATERIALIZATION", "NESTED_WFA", "SELECTION", "BASELINES", "CONTROLS", "EXPOSURE_RESIDUALS", "REGIMES", "PBO", "SYNCHRONIZED_BOOTSTRAP", "ROMANO_WOLF", "REPLICATION", "TAILS", "PORTFOLIO_169", "TERMINAL_DECISION"],
        "terminal_storage_only": True,
        "final_execution_performed": False,
    }
    return _identified(core, "manifest_id")


def _verified_identity(value: Mapping[str, object], field: str) -> None:
    identity = value.get(field)
    if not isinstance(identity, str) or identity != sha256_json({key: item for key, item in value.items() if key != field}):
        raise IntegrityError(f"{field} is invalid")


def _write_exclusive(path: Path, value: Mapping[str, object]) -> None:
    expected = canonical_bytes(value) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(expected)
    except FileExistsError:
        if path.read_bytes() != expected:
            raise IntegrityError(f"immutable record collision: {path}")


def _load_chunk(path: Path, expected: Mapping[str, object]) -> dict[str, object]:
    value = _read_json(path)
    if path.read_bytes() != canonical_bytes(value) + b"\n":
        raise IntegrityError("synthetic chunk bytes are not canonical")
    _verified_identity(value, "record_id")
    for key, item in expected.items():
        if value.get(key) != item:
            raise IntegrityError("synthetic chunk binding changed")
    return value


def _run_synthetic_job(job: tuple[dict[str, object], tuple[str, ...], str, int]) -> dict[str, object]:
    specification, campaign_ids, kind, resamples = job
    return run_synthetic_campaign_chunk(
        specification, campaign_ids=campaign_ids, kind=kind, resamples=resamples
    )


def _peak_working_set_bytes() -> int:
    import ctypes
    import os

    if os.name != "nt":
        import resource
        return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024
    class Counters(ctypes.Structure):
        _fields_ = [("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong), ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t), ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t), ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t), ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t)]
    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    ctypes.windll.kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    ctypes.windll.psapi.GetProcessMemoryInfo.argtypes = (ctypes.c_void_p, ctypes.POINTER(Counters), ctypes.c_ulong)
    ctypes.windll.psapi.GetProcessMemoryInfo.restype = ctypes.c_int
    if not ctypes.windll.psapi.GetProcessMemoryInfo(ctypes.windll.kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
        raise IntegrityError("cannot measure process memory")
    return int(counters.PeakWorkingSetSize)


def _physical_memory_bytes() -> int:
    import ctypes
    from ctypes import wintypes

    class Status(ctypes.Structure):
        _fields_ = [("dwLength", wintypes.DWORD), ("dwMemoryLoad", wintypes.DWORD), ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong), ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong), ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong), ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
    status = Status()
    status.dwLength = ctypes.sizeof(status)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
        raise IntegrityError("cannot measure physical memory")
    return int(status.ullTotalPhys)


def _run_benchmark_job(job: tuple[dict[str, object], tuple[str, ...], str, int]) -> tuple[dict[str, object], int]:
    return _run_synthetic_job(job), _peak_working_set_bytes()


def benchmark_synthetic_configuration(
    specification: Mapping[str, object], *,
    chunk_sizes: Sequence[int] = (1,),
    worker_counts: Sequence[int] = (16,),
) -> dict[str, object]:
    """Benchmark the frozen 32+32 sample and require scalar terminal-byte identity."""

    import time

    ids_by_kind = {
        "NULL": tuple(f"BENCH_NULL_{ordinal:05d}" for ordinal in range(32)),
        "KNOWN_EDGE": tuple(f"BENCH_EDGE_{ordinal:05d}" for ordinal in range(32)),
    }
    scalar_jobs = [
        (dict(specification), (campaign_id,), kind, 10_000)
        for kind in ("NULL", "KNOWN_EDGE")
        for campaign_id in ids_by_kind[kind]
    ]
    scalar: list[dict[str, object]] = []
    with ProcessPoolExecutor(max_workers=max(worker_counts)) as executor:
        for chunk, _peak in executor.map(_run_benchmark_job, scalar_jobs, chunksize=1):
            scalar.extend(chunk["terminal_outcomes"])
    scalar_bytes = canonical_bytes(scalar)
    total_memory = _physical_memory_bytes()
    results = []
    for workers in worker_counts:
        for chunk_size in chunk_sizes:
            if type(workers) is not int or workers < 1 or type(chunk_size) is not int or chunk_size < 1:
                raise IntegrityError("benchmark configurations must be positive integers")
            jobs = [
                (dict(specification), ids[start:start + chunk_size], kind, 10_000)
                for kind, ids in ids_by_kind.items()
                for start in range(0, len(ids), chunk_size)
            ]
            terminals: list[dict[str, object]] = []
            peak = 0
            started = time.perf_counter()
            with ProcessPoolExecutor(max_workers=workers) as executor:
                for chunk, worker_peak in executor.map(_run_benchmark_job, jobs, chunksize=1):
                    terminals.extend(chunk["terminal_outcomes"])
                    peak = max(peak, worker_peak)
            elapsed = time.perf_counter() - started
            identical = canonical_bytes(terminals) == scalar_bytes
            projected = elapsed / 64.0 * 15_000.0
            ram_fraction = peak * workers / total_memory
            results.append({"chunk_size": chunk_size, "workers": workers, "elapsed_seconds": elapsed, "projected_exact_seconds": projected, "peak_worker_bytes": peak, "projected_peak_ram_fraction": ram_fraction, "terminal_bytes_equal_scalar": identical, "qualifies": bool(identical and projected <= SYNTHETIC_MAX_ELAPSED_SECONDS and ram_fraction <= 0.5)})
    passing = [row for row in results if row["qualifies"]]
    selected = min(passing, key=lambda row: (row["projected_exact_seconds"], row["projected_peak_ram_fraction"], row["workers"])) if passing else None
    core = {"schema_version": "intraday_research_synthetic_performance/1.2.3", "program_id": specification["program_id"], "sample": {"null": 32, "known_edge": 32, "resamples": 10_000}, "scalar_terminal_sha256": sha256_json(scalar), "results": results, "qualifies": selected is not None, "selected_chunk_size": None if selected is None else selected["chunk_size"], "selected_workers": None if selected is None else selected["workers"]}
    return _identified(core, "performance_id")


def _wilson_upper(successes: int, trials: int) -> float:
    if trials < 1 or successes not in range(trials + 1):
        raise IntegrityError("Wilson inputs are invalid")
    z = 1.6448536269514722
    proportion = successes / trials
    denominator = 1.0 + z * z / trials
    center = proportion + z * z / (2.0 * trials)
    radius = z * np.sqrt(proportion * (1.0 - proportion) / trials + z * z / (4.0 * trials * trials))
    return float((center + radius) / denominator)


def _romano_wolf_mean_fallback(
    matrix: np.ndarray, hypothesis_ids: Sequence[str], *, resamples: int, seed: int
) -> list[float]:
    """Use the same stationary dates with a max-mean statistic if HAC is degenerate."""

    indices = stationary_bootstrap_indices(
        n_observations=len(matrix), n_resamples=resamples,
        mean_block_length=20.0, seed=seed,
    )
    centered = matrix - np.mean(matrix, axis=0)
    bootstrap = np.empty((resamples, matrix.shape[1]), dtype=np.float64)
    for start in range(0, resamples, 128):
        rows = indices[start:start + 128]
        bootstrap[start:start + len(rows)] = np.mean(centered[rows], axis=1)
    return romano_wolf_stepdown(
        np.mean(matrix, axis=0), bootstrap,
        hypothesis_ids=tuple(hypothesis_ids), tail="greater", minimum_resamples=999,
    ).adjusted_p_values.tolist()


def synthetic_gamma_trap_outcomes() -> list[dict[str, object]]:
    """Exercise the production promotion tail gates with five known traps."""

    paths = {
        "SHORT_VOLATILITY": np.r_[np.full(503, 4.0), -2_000.0],
        "LIQUIDITY_GAP": np.r_[np.full(503, 3.0), -1_800.0],
        "LEVERAGED_CRASH": np.r_[np.full(503, 20.0), -5_000.0],
        "EVENT_CONCENTRATION": np.r_[np.full(499, 0.1), np.full(5, 100.0)],
        "SMOOTH_UNTIL_BLOWUP": np.r_[np.full(503, 5.0), -2_000.0],
    }
    outcomes = []
    for trap_id, path in paths.items():
        report, failures = _tail_gates(path[:, None], np.full((len(path), 1), -1.0))
        outcomes.append({"trap_id": trap_id, "passed_promotion_tail_gates": not failures, "failed_gates": failures, "tail_report_id": sha256_json(report)})
    return outcomes


def run_synthetic_certification(
    specification: Mapping[str, object],
    *,
    manifest: Mapping[str, object] | None = None,
    run_root: Path | None = None,
    run_ordinal: int | None = None,
    null_campaigns: int | None = None,
    known_edge_campaigns: int | None = None,
    chunk_size: int | None = None,
    resamples: int | None = None,
    exact: bool,
) -> dict[str, object]:
    """Execute smoke settings or one immutable manifest-driven exact run."""

    import time
    started = time.perf_counter()
    if exact:
        if any(value is not None for value in (null_campaigns, known_edge_campaigns, chunk_size, resamples)):
            raise IntegrityError("exact certification rejects CLI scientific-setting overrides")
        if manifest is None or run_root is None or type(run_ordinal) is not int or run_ordinal < 1:
            raise IntegrityError("exact certification requires a manifest, run root, and positive run ordinal")
        _verified_identity(manifest, "manifest_id")
        _verified_identity(specification, "program_id")
        if manifest.get("schema_version") != SYNTHETIC_SCHEMA or manifest.get("program_id") != specification.get("program_id"):
            raise IntegrityError("exact certification manifest binding is invalid")
        campaigns = manifest.get("campaigns")
        if not isinstance(campaigns, Mapping):
            raise IntegrityError("exact certification campaign settings are missing")
        null_campaigns = int(campaigns.get("null", 0))
        known_edge_campaigns = int(campaigns.get("known_edge", 0))
        chunk_size = int(manifest.get("chunk_size", 0))
        resamples = int(manifest.get("romano_wolf_bootstrap_resamples", 0))
        workers = int(manifest.get("workers", 0))
        performance = manifest.get("performance_evidence")
        if (
            (null_campaigns, known_edge_campaigns, resamples) != (10_000, 5_000, 10_000)
            or workers < 1
            or not isinstance(performance, Mapping)
            or performance.get("qualifies") is not True
            or performance.get("selected_chunk_size") != chunk_size
            or performance.get("selected_workers") != workers
        ):
            raise IntegrityError("exact manifest settings are unsupported or incomplete")
    elif manifest is not None or run_root is not None or run_ordinal is not None:
        raise IntegrityError("smoke mode cannot consume exact-run state")
    else:
        workers = 1
    assert null_campaigns is not None and known_edge_campaigns is not None and chunk_size is not None and resamples is not None
    if min(null_campaigns, known_edge_campaigns, chunk_size, resamples) < 1:
        raise IntegrityError("synthetic certification settings are invalid")
    run_id = None if not exact else sha256_json({"manifest_id": manifest["manifest_id"], "run_ordinal": run_ordinal})
    chunk_root = None if run_root is None else run_root / str(run_id) / "chunks"
    descriptors: list[tuple[tuple[str, int], tuple[str, ...], dict[str, object], Path | None]] = []
    for kind, count, prefix in (("NULL", null_campaigns, "NULL"), ("KNOWN_EDGE", known_edge_campaigns, "KNOWN_EDGE")):
        for start in range(0, count, chunk_size):
            stop = min(count, start + chunk_size)
            ids = tuple(f"{prefix}_{ordinal:05d}" for ordinal in range(start, stop))
            expected = {
                "schema_version": SYNTHETIC_CHUNK_SCHEMA,
                "manifest_id": None if manifest is None else manifest["manifest_id"],
                "program_id": specification["program_id"],
                "kind": kind,
                "ordinal_start_inclusive": start,
                "ordinal_end_exclusive": stop,
                "campaign_ids": list(ids),
                "settings": {"resamples": resamples, "chunk_size": chunk_size, "workers": workers},
            }
            path = None if chunk_root is None else chunk_root / f"{kind}_{start:05d}_{stop:05d}.json"
            descriptors.append(((kind, start), ids, expected, path))
    expected_names = {path.name for _, _, _, path in descriptors if path is not None}
    if chunk_root is not None:
        observed_names = {path.name for path in chunk_root.glob("*.json")}
        if not observed_names <= expected_names:
            raise IntegrityError("synthetic chunk registry has an overlap or unexpected campaign range")
        if (chunk_root.parent / "completed_run.json").exists() and observed_names != expected_names:
            raise IntegrityError("completed synthetic run has a chunk gap")
    chunks: dict[tuple[str, int], Mapping[str, object]] = {}
    missing: list[tuple[tuple[str, int], tuple[str, ...], dict[str, object], Path | None]] = []
    for key, ids, expected, path in descriptors:
        if path is not None and path.exists():
            record = _load_chunk(path, expected)
            chunk = record.get("chunk")
            if not isinstance(chunk, Mapping):
                raise IntegrityError("synthetic chunk payload is malformed")
            chunks[key] = chunk
        else:
            missing.append((key, ids, expected, path))
    jobs = ((dict(specification), ids, str(expected["kind"]), resamples) for _, ids, expected, _ in missing)
    if workers == 1:
        generated = map(_run_synthetic_job, jobs)
    else:
        executor = ProcessPoolExecutor(max_workers=workers)
        generated = executor.map(_run_synthetic_job, jobs, chunksize=1)
    try:
        for (key, _ids, expected, path), chunk in zip(missing, generated, strict=True):
            chunks[key] = chunk
            if path is not None:
                _write_exclusive(path, _identified({**expected, "chunk": chunk}, "record_id"))
    finally:
        if workers != 1:
            executor.shutdown(wait=True, cancel_futures=True)
    terminals: list[dict[str, object]] = []
    chunk_ids: list[str] = []
    for key, ids, _expected, _path in descriptors:
        chunk = chunks[key]
        if chunk.get("campaign_ids") != list(ids):
            raise IntegrityError("synthetic chunk campaign order changed")
        terminals.extend(chunk["terminal_outcomes"])
        chunk_ids.append(str(chunk["chunk_id"]))
    if chunk_root is not None and {path.name for path in chunk_root.glob("*.json")} != expected_names:
        raise IntegrityError("synthetic chunk registry has a gap")
    null = [item for item in terminals if item["kind"] == "NULL"]
    edge = [item for item in terminals if item["kind"] == "KNOWN_EDGE"]
    false_promotions = sum(bool(item["promoted"]) for item in null)
    edge_promotions = sum(bool(item["known_edge_detected"]) for item in edge)
    gamma_traps = synthetic_gamma_trap_outcomes()
    gamma_rejected = all(not bool(item["passed_promotion_tail_gates"]) for item in gamma_traps)
    completed_path = None if run_root is None else run_root / str(run_id) / "completed_run.json"
    if completed_path is not None and completed_path.exists():
        existing = _read_json(completed_path)
        if completed_path.read_bytes() != canonical_bytes(existing) + b"\n":
            raise IntegrityError("completed synthetic run bytes are not canonical")
        _verified_identity(existing, "acceptance_id")
        if existing.get("terminal_outcomes") != terminals or existing.get("merkle_digest") != _merkle_digest([str(item["terminal_id"]) for item in terminals]):
            raise IntegrityError("completed synthetic run differs from its immutable chunks")
        return existing
    elapsed = time.perf_counter() - started
    selected_performance = None
    if exact:
        selected_performance = next(
            (row for row in performance.get("results", ()) if row.get("chunk_size") == chunk_size and row.get("workers") == workers),
            None,
        )
        if not isinstance(selected_performance, Mapping):
            raise IntegrityError("selected performance benchmark row is missing")
    memory_fraction = None if selected_performance is None else float(selected_performance["projected_peak_ram_fraction"])
    performance_passed = bool(not exact or (elapsed <= SYNTHETIC_MAX_ELAPSED_SECONDS and memory_fraction is not None and memory_fraction <= 0.5))
    core = {
        "schema_version": SYNTHETIC_SCHEMA,
        "state": "EXACT_COMPLETE" if exact else "SMOKE_ONLY_NOT_CERTIFICATION",
        "program_id": specification["program_id"],
        "manifest_id": None if manifest is None else manifest["manifest_id"],
        "run_id": run_id,
        "run_ordinal": run_ordinal,
        "settings": {"null_campaigns": null_campaigns, "known_edge_campaigns": known_edge_campaigns, "chunk_size": chunk_size, "resamples": resamples, "workers": workers, "exact": exact},
        "null": {"false_promotions": false_promotions, "wilson_upper_95": _wilson_upper(false_promotions, null_campaigns), "required_maximum": 0.05},
        "known_edge": {"promotions": edge_promotions, "power": edge_promotions / known_edge_campaigns, "required_minimum": 0.8, "edge_usd_per_complete_session": 30.0},
        "gamma_traps": gamma_traps,
        "performance": {"elapsed_seconds": elapsed, "peak_ram_fraction_bound": memory_fraction, "limits_passed": performance_passed},
        "terminal_outcomes": terminals,
        "chunk_ids": chunk_ids,
        "merkle_digest": _merkle_digest([str(item["terminal_id"]) for item in terminals]),
        "acceptance_passed": bool(exact and _wilson_upper(false_promotions, null_campaigns) <= 0.05 and edge_promotions / known_edge_campaigns >= 0.8 and gamma_rejected and performance_passed),
        "stores_intermediate_statistics": False,
    }
    completed = _identified(core, "acceptance_id")
    if completed_path is not None:
        _write_exclusive(completed_path, completed)
    return completed


def _evaluate_program_decision_one(specification: Mapping[str, object], materialized_program_inputs: Mapping[str, object]) -> dict[str, object]:
    """Pure calculation only.  It never emits authority or historical labels."""

    if specification.get("schema_version") != PROGRAM_SCHEMA:
        raise IntegrityError("the frozen program schema is required")
    name = materialized_program_inputs.get("program_name")
    program = next((p for p in specification["programs"] if p["program_name"] == name), None)
    if program is None:
        raise IntegrityError("unknown program")
    dates = tuple(materialized_program_inputs.get("exchange_session_dates", ()))
    markets = tuple(materialized_program_inputs.get("markets", ()))
    forbidden = {"gross", "base", "stress", "extreme", "controls", "folds", "residuals", "baselines", "regimes", "tail", "tail_report", "predictions", "source_context"}
    if forbidden & set(materialized_program_inputs):
        raise IntegrityError("caller-supplied scientific evidence is forbidden")
    if materialized_program_inputs.get("caller_supplied_results") is not False or not isinstance(materialized_program_inputs.get("minute_cube"), MinuteBarCube):
        raise IntegrityError("only the canonical causal-bar materializer output is accepted")
    if int(materialized_program_inputs.get("campaign_count", 0)) != 1:
        raise IntegrityError("scalar decision evaluation requires exactly one campaign")
    folds = _schedule(dates)
    split_records = _split_records(dates, folds)
    materialized_program_inputs["minute_cube"].validate()
    causal_features = _causal_feature_arrays(materialized_program_inputs["minute_cube"], int(program["trigger_window_minutes"]))
    trade_cache: dict[tuple[int, int, int, int, int, int, bool, bool], tuple[float, float, np.ndarray] | None] = {}
    selected = np.empty(8, dtype=np.int64)
    selection_medians = np.empty((8, 12), dtype=np.float64)
    oos_gross = np.zeros((len(dates), len(markets)), dtype=np.float64)
    oos_base = np.zeros_like(oos_gross)
    oos = np.zeros((len(dates), len(markets)), dtype=np.float64)
    oos_extreme = np.zeros_like(oos)
    oos_trades = np.zeros_like(oos, dtype=np.bool_)
    oos_adverse = np.zeros_like(oos)
    oos_direction = np.zeros_like(oos, dtype=np.int8)
    oos_event_minute = np.full_like(oos, -1, dtype=np.int16)
    oos_controls: np.ndarray | None = None
    candidate_outer = np.zeros((len(dates), len(markets), 12), dtype=np.float64)
    baselines = np.zeros((len(dates), len(markets), len(BASELINES)), dtype=np.float64)
    predictions = np.zeros_like(oos)
    coefficients: list[dict[str, object]] = []
    regime_definitions: list[dict[str, object]] = []
    fold_fit_winners: dict[int, np.ndarray] = {}
    fold_training_variance = np.zeros(8, dtype=np.float64)
    fold_regimes: dict[str, np.ndarray] = {name: np.full((len(dates), len(markets)), "UNOBSERVED", dtype="U10") for name in ("volatility", "trend", "gap", "liquidity")}
    for fold_id, fold in enumerate(folds):
        inner_scores = np.full((4, 12), -1.0e12, dtype=np.float64)
        for inner_id, inner in enumerate(fold.inner_folds):
            inner_cube = _fold_candidate_cube(program, materialized_program_inputs, fit_indices=inner.fit_indices, audit_indices=inner.audit_indices, include_controls=False, include_adverse=False, include_exposures=False, causal_features=causal_features, trade_cache=trade_cache)
            values = np.mean(inner_cube["stress"][0], axis=1)
            scale = np.std(values, axis=0, ddof=1)
            inner_scores[inner_id] = np.divide(np.mean(values, axis=0), scale, out=np.full(12, -1.0e12), where=scale > 0)
        medians = np.median(inner_scores, axis=0)
        selection_medians[fold_id] = medians
        if not np.any(np.isfinite(medians)):
            winner = 0
        else:
            winner = int(np.flatnonzero(medians == np.max(medians))[0])
        selected[fold_id] = winner
        audit = fold.audit_indices
        fit = fold.fit_indices
        outer_fit = _fold_candidate_cube(program, materialized_program_inputs, fit_indices=fit, audit_indices=fit, include_controls=False, include_adverse=False, include_exposures=True, causal_features=causal_features, candidate_indices=(winner,), trade_cache=trade_cache)
        outer_audit = _fold_candidate_cube(program, materialized_program_inputs, fit_indices=fit, audit_indices=audit, causal_features=causal_features, trade_cache=trade_cache)
        candidate_outer[audit] = outer_audit["stress"][0]
        winner_fit = outer_fit["stress"][0, :, :, 0]
        fit_long_move = outer_fit["gross"][0, :, :, 0] * outer_fit["direction"][0, :, :, 0]
        fit_trades = outer_fit["trades"][0, :, :, 0]
        fold_fit_winners[fold_id] = winner_fit
        fold_training_variance[fold_id] = float(np.var(np.mean(winner_fit, axis=1), ddof=1))
        winner_controls = outer_audit["controls"][0, :, :, winner, :]
        if oos_controls is None:
            oos_controls = np.zeros((len(dates), len(markets), winner_controls.shape[-1]), dtype=np.float64)
        oos_controls[audit] = winner_controls
        for market_id in range(len(markets)):
            oos_gross[audit, market_id] = outer_audit["gross"][0, :, market_id, winner]
            oos_base[audit, market_id] = outer_audit["base"][0, :, market_id, winner]
            oos[audit, market_id] = outer_audit["stress"][0, :, market_id, winner]
            oos_extreme[audit, market_id] = outer_audit["extreme"][0, :, market_id, winner]
            oos_trades[audit, market_id] = outer_audit["trades"][0, :, market_id, winner]
            oos_direction[audit, market_id] = outer_audit["direction"][0, :, market_id, winner]
            oos_event_minute[audit, market_id] = outer_audit["event_minute"][0, :, market_id, winner]
            raw_adverse = outer_audit["adverse"][0, :, market_id, winner]
            oos_adverse[audit, market_id] = np.where(oos_trades[audit, market_id], raw_adverse, 0.0)
            train_x = outer_fit["exposures"][0, :, market_id, 0, :]
            train_y = winner_fit[:, market_id]
            test_x = outer_audit["exposures"][0, :, market_id, winner, :]
            coef, pred = _ols(train_x, train_y, test_x)
            predictions[audit, market_id] = pred
            coefficients.append({"fold": fold_id, "market": markets[market_id], "values": coef.tolist(), "fit_id": sha256_json({"dates": [dates[i] for i in fit], "market": markets[market_id], "candidate": winner})})
            regime_columns = {"trend": 2, "volatility": 3, "liquidity": 4, "gap": 5}
            for regime, column in regime_columns.items():
                boundaries = np.quantile(train_x[:, column], (1 / 3, 2 / 3))
                regime_definitions.append({"fold": fold_id, "market": markets[market_id], "regime": regime, "boundaries": boundaries.tolist(), "fit_id": sha256_json({"dates": [dates[i] for i in fit], "market": markets[market_id], "candidate": winner, "regime": regime})})
                labels = np.where(test_x[:, column] <= boundaries[0], "LOW", np.where(test_x[:, column] <= boundaries[1], "MID", "HIGH"))
                for position, date_index in enumerate(audit):
                    fold_regimes[regime][int(date_index), market_id] = str(labels[position])
            traded = oos_trades[audit, market_id]
            long_move = outer_audit["gross"][0, :, market_id, winner] * outer_audit["direction"][0, :, market_id, winner]
            fit_market = fit_long_move[:, market_id][fit_trades[:, market_id]]
            unconditional = 0 if len(fit_market) == 0 else int(np.sign(np.mean(fit_market)))
            previous = np.zeros(len(audit), dtype=np.int8)
            for position in np.flatnonzero(traded):
                minute = int(oos_event_minute[int(audit[position]), market_id])
                previous[position] = int(np.sign(
                    materialized_program_inputs["minute_cube"].close[0, int(audit[position]), materialized_program_inputs["minute_cube"].markets.index(markets[market_id]), minute]
                    - materialized_program_inputs["minute_cube"].close[0, int(audit[position]), materialized_program_inputs["minute_cube"].markets.index(markets[market_id]), minute - 1]
                ))
            directions = (
                np.zeros(len(audit), dtype=np.int8),
                np.full(len(audit), unconditional, dtype=np.int8),
                previous,
                -previous,
                np.ones(len(audit), dtype=np.int8),
                -np.ones(len(audit), dtype=np.int8),
            )
            for baseline_id, baseline_direction in enumerate(directions):
                baselines[audit, market_id, baseline_id] = np.where(
                    traded & (baseline_direction != 0),
                    baseline_direction * long_move - float(materialized_program_inputs["stress_cost_usd"]),
                    0.0,
                )
    residual = oos - predictions
    active = np.arange(len(dates)) >= len(dates) - 504
    date_returns = np.sum(oos[active], axis=1)
    candidate_matrix = np.sum(candidate_outer[active], axis=1)
    try:
        pbo = exhaustive_cscv_pbo(candidate_matrix, strategy_ids=tuple(str(c["candidate_id"]) for c in program["candidates"]), blocks=8, metric="sharpe", tie_tolerance=1e-12)
        pbo_value = pbo.pbo_conservative
    except ResearchContractError:
        pbo_value = (
            0.0
            if np.all(candidate_matrix[:, 0] > 0)
            and np.all(candidate_matrix[:, 0, None] > candidate_matrix[:, 1:])
            else None
        )
    differentials = [date_returns]
    hypothesis_ids = ["ZERO"]
    differentials.extend(np.sum(oos[active] - baselines[active, :, i], axis=1) for i in range(len(BASELINES)))
    hypothesis_ids.extend(f"BASELINE_{name}" for name in BASELINES)
    for baseline_id, baseline_name in enumerate(BASELINES):
        for market_id, market in enumerate(markets):
            differentials.append(oos[active, market_id] - baselines[active, market_id, baseline_id])
            hypothesis_ids.append(f"BASELINE_{baseline_name}_MARKET_{market}")
    for market_id in range(len(markets)):
        differentials.append(oos[active, market_id])
        hypothesis_ids.append(f"MARKET_{markets[market_id]}")
    families: dict[str, list[int]] = {}
    family_name = {"ES": "EQUITY", "NQ": "EQUITY", "CL": "ENERGY", "GC": "METALS", "ZN": "RATES", "ZB": "RATES"}
    for market_id, market in enumerate(markets):
        families.setdefault(family_name[market], []).append(market_id)
    for family, indices in sorted(families.items()):
        differentials.append(np.mean(oos[active][:, indices], axis=1))
        hypothesis_ids.append(f"FAMILY_{family}")
    for market_id, market in enumerate(markets):
        keep = [index for index in range(len(markets)) if index != market_id]
        differentials.append(np.mean(oos[active][:, keep], axis=1))
        hypothesis_ids.append(f"LOMO_{market}")
    assert oos_controls is not None
    control_active = oos_controls[active]
    for control_id in range(control_active.shape[-1]):
        control_date = np.mean(control_active[..., control_id], axis=1)
        differentials.append(date_returns - control_date)
        hypothesis_ids.append(f"CONTROL_{control_id}")
    differentials.append(np.sum(residual[active], axis=1))
    hypothesis_ids.append("RESIDUAL_AGGREGATE")
    for market_id, market in enumerate(markets):
        differentials.append(residual[active, market_id])
        hypothesis_ids.append(f"RESIDUAL_MARKET_{market}")
    matrix = np.column_stack(differentials)
    rw_inconclusive = False
    matrix_mean = np.mean(matrix, axis=0)
    matrix_scale = np.std(matrix, axis=0)
    fixed = matrix_scale <= np.finfo(np.float64).eps * np.maximum(1.0, np.abs(matrix_mean)) * 64.0
    adjusted_array = np.where(matrix_mean > 0, 0.0, 1.0)
    try:
        if np.any(~fixed):
            rw = romano_wolf_from_differentials(
                matrix[:, ~fixed],
                hypothesis_ids=tuple(np.asarray(hypothesis_ids)[~fixed].tolist()),
                hac_lag=4,
                mean_block_length=20.0,
                n_resamples=int(materialized_program_inputs.get("resamples", 999)),
                seed=int(materialized_program_inputs.get("seed", 0)),
                minimum_resamples=999,
            )
            adjusted_array[~fixed] = rw.adjusted_p_values
        adjusted = adjusted_array.tolist()
    except ResearchContractError:
        try:
            adjusted = _romano_wolf_mean_fallback(
                matrix, hypothesis_ids,
                resamples=int(materialized_program_inputs.get("resamples", 999)),
                seed=int(materialized_program_inputs.get("seed", 0)),
            )
        except ResearchContractError:
            adjusted = []
            rw_inconclusive = True
    failures = []
    inconclusive = []
    if float(np.mean(date_returns)) <= 0 or float(np.mean(np.mean(oos_extreme[active], axis=1))) <= 0:
        failures.append("STRESS_OR_EXTREME_MEAN")
    resamples = int(materialized_program_inputs.get("resamples", 999))
    seed = int(materialized_program_inputs.get("seed", 0))
    shared_indices = stationary_bootstrap_indices(
        n_observations=int(np.count_nonzero(active)),
        n_resamples=resamples,
        mean_block_length=20.0,
        seed=seed + 11,
    )
    aggregate_lower = _shared_date_lower(date_returns[:, None], indices=shared_indices)
    if aggregate_lower <= 10.0:
        failures.append("MEES_CONFIDENCE_LOWER_BOUND")
    market_lowers = {}
    for i, market in enumerate(markets):
        traded = oos_trades[active, i]
        market_lowers[market] = (
            float("-inf")
            if np.count_nonzero(traded) < 2
            else _shared_date_lower(
                oos[active, i:i + 1][traded], seed=seed + 100 + i, resamples=resamples
            )
        )
    if any(
        not np.any(oos_trades[active, i])
        or float(np.mean(oos[active, i][oos_trades[active, i]])) <= 0
        or float(np.mean(oos_extreme[active, i][oos_trades[active, i]])) <= 0
        or market_lowers[market] < 10.0
        for i, market in enumerate(markets)
    ):
        failures.append("MARKET_REPLICATION")
    residual_lowers = {market: _shared_date_lower(residual[active][:, i:i + 1], indices=shared_indices) for i, market in enumerate(markets)}
    if any(float(np.mean(residual[active, i])) <= 0 or residual_lowers[market] <= 0 for i, market in enumerate(markets)):
        failures.append("RESIDUAL_ALPHA")
    trade_counts = {market: int(np.count_nonzero(oos_trades[active, index])) for index, market in enumerate(markets)}
    if any(count < 40 for count in trade_counts.values()):
        failures.append("MINIMUM_TRADES")
    fold_means = [float(np.mean(oos[fold.audit_indices])) for fold in folds]
    fold_means_by_market = {market: [float(np.mean(oos[fold.audit_indices, market_id])) for fold in folds] for market_id, market in enumerate(markets)}
    if sum(value > 0 for value in fold_means) < 5:
        failures.append("POSITIVE_OUTER_FOLDS")
    if any(sum(value > 0 for value in values) < 5 for values in fold_means_by_market.values()):
        failures.append("MARKET_POSITIVE_OUTER_FOLDS")
    if np.any(np.mean(control_active, axis=0) > 0):
        failures.append("NEGATIVE_CONTROLS")
    family_lowers = {name: _shared_date_lower(oos[active][:, market_indices], indices=shared_indices) for name, market_indices in sorted(families.items())}
    lomo_lowers = {market: _shared_date_lower(oos[active][:, [i for i in range(len(markets)) if i != market_id]], indices=shared_indices) for market_id, market in enumerate(markets)}
    if any(value <= 0 for value in (*family_lowers.values(), *lomo_lowers.values())):
        failures.append("FAMILY_OR_LEAVE_ONE_OUT_REPLICATION")
    scientific_report, tail_failures = _scientific_report(gross=oos_gross[active], base=oos_base[active], stress=oos[active], extreme=oos_extreme[active], direction=oos_direction[active], regimes={name: labels[active] for name, labels in fold_regimes.items()}, adverse=oos_adverse[active])
    tail = scientific_report["tail"]
    for failure in tail_failures:
        (inconclusive if failure == "ONE_SECOND_ADVERSE_PATH" else failures).append(failure)
    market_tail_reports = {}
    for market_id, market in enumerate(markets):
        market_tail, market_tail_failures = _tail_gates(oos[active, market_id:market_id + 1], oos_adverse[active, market_id:market_id + 1])
        market_tail_reports[market] = market_tail
        for failure in market_tail_failures:
            target = inconclusive if failure == "ONE_SECOND_ADVERSE_PATH" else failures
            target.append(f"MARKET_{market}_{failure}")
    if pbo_value is None:
        inconclusive.append("PBO_INCONCLUSIVE")
    if rw_inconclusive:
        inconclusive.append("ROMANO_WOLF_INCONCLUSIVE")
    elif any(
        value > 0.05 / 168
        for hypothesis, value in zip(hypothesis_ids, adjusted, strict=True)
        if hypothesis == "ZERO" or (hypothesis.startswith("BASELINE_") and "_MARKET_" not in hypothesis) or hypothesis.startswith("CONTROL_")
    ):
        failures.append("ROMANO_WOLF")
    outcome = "FAIL" if failures else ("INCONCLUSIVE" if inconclusive else "PASS")
    core = {
        "schema_version": DECISION_SCHEMA, "program_id": specification["program_id"], "program_name": name,
        "selected_candidate_indices": selected.tolist(), "inner_median_stress_sharpes": selection_medians.tolist(), "exchange_session_dates": list(dates[-504:]), "markets": list(markets), "split_records": split_records,
        "candidate_ids": [candidate["candidate_id"] for candidate in program["candidates"]], "candidate_stress_date_market": candidate_outer[active].tolist(),
        "cost_paths": {"gross": oos_gross[active].tolist(), "base": oos_base[active].tolist(), "stress": oos[active].tolist(), "extreme": oos_extreme[active].tolist()}, "outer_training_session_returns": [np.mean(fold_fit_winners[fold], axis=1).tolist() for fold in range(8)],
        "stress_date_market": oos[active].tolist(), "extreme_date_market": oos_extreme[active].tolist(), "adverse_date_market": oos_adverse[active].tolist(), "trade_flags": oos_trades[active].tolist(), "trade_counts": trade_counts, "direction": oos_direction[active].tolist(), "event_minute": oos_event_minute[active].tolist(), "outer_fold_means": fold_means, "outer_fold_means_by_market": fold_means_by_market, "outer_training_variance": fold_training_variance.tolist(),
        "baseline_ids": list(BASELINES), "baseline_stress_date_market": baselines[active].tolist(), "negative_control_ids": (["LOW_VOLUME", "BLOCK_DERANGED_TIMING"] if name == "FLOW_CONTINUATION" else (["LOW_VOLUME", "UNCONDITIONAL_MEAN_REVERSION"] if name == "LIQUIDITY_SHOCK_REVERSAL" else ["SWAPPED_LEADER_FOLLOWER"])), "negative_control_stress_date_market": oos_controls[active].tolist(),
        "exposure_coefficients": coefficients, "exposure_predictions": predictions[active].tolist(), "residuals": residual[active].tolist(),
        "residual_reconstruction_sha256": sha256_json({"stress": oos[active].tolist(), "predictions": predictions[active].tolist(), "residuals": residual[active].tolist()}), "regime_definitions": regime_definitions, "regimes": {name: labels[active].tolist() for name, labels in fold_regimes.items()}, "scientific_report": scientific_report,
        "pbo_conservative": pbo_value, "dsr": {"status": "DIAGNOSTIC_UNAVAILABLE_MISSING_LEGACY_TRIAL_SHARPES", "promotion_gate": False}, "romano_wolf_hypothesis_ids": hypothesis_ids, "romano_wolf_adjusted_p": adjusted,
        "bootstrap_lower": {"aggregate_mees": aggregate_lower, "markets": market_lowers, "families": family_lowers, "leave_one_market_out": lomo_lowers, "residual_markets": residual_lowers}, "tail_report": tail, "market_tail_reports": market_tail_reports, "failed_gates": sorted(set(failures)), "inconclusive_gates": sorted(set(inconclusive)),
        "outcome": outcome, "authoritative": False, "evidence_label": "NO_EDGE",
    }
    return _identified(core, "calculation_id")


def _slice_campaign(materialized: Mapping[str, object], campaign: int) -> dict[str, object]:
    cube = materialized.get("minute_cube")
    if not isinstance(cube, MinuteBarCube) or campaign not in range(cube.campaign_count):
        raise IntegrityError("campaign slice is invalid")
    sliced_cube = MinuteBarCube(
        cube.exchange_session_dates,
        cube.markets,
        cube.identity_labels,
        cube.event_at_ns[campaign:campaign + 1],
        cube.available_at_ns[campaign:campaign + 1],
        cube.identity_code[campaign:campaign + 1],
        cube.observed[campaign:campaign + 1],
        cube.open[campaign:campaign + 1],
        cube.high[campaign:campaign + 1],
        cube.low[campaign:campaign + 1],
        cube.close[campaign:campaign + 1],
        cube.volume[campaign:campaign + 1],
        cube.tick_size[campaign:campaign + 1],
        cube.tick_value[campaign:campaign + 1],
    )
    seconds = tuple(replace(row, campaign_id=0) for row in materialized["one_second_bars"] if row.campaign_id == campaign)
    campaign_id = str(materialized["campaign_ids"][campaign])
    seed = (
        int(sha256_json({"campaign_id": campaign_id})[:16], 16)
        if materialized.get("synthetic") is True
        else int(sha256_json({"base_seed": materialized["seed"], "campaign_id": campaign_id})[:16], 16)
    )
    return {**materialized, "campaign_count": 1, "campaign_ids": (campaign_id,), "minute_cube": sliced_cube, "one_second_bars": seconds, "seed": seed}


def evaluate_program_decision_batch(
    specification: Mapping[str, object], materialized_program_inputs: Mapping[str, object]
) -> tuple[dict[str, object], ...]:
    count = int(materialized_program_inputs.get("campaign_count", 0))
    if count < 1:
        raise IntegrityError("at least one campaign is required")
    return tuple(
        _evaluate_program_decision_one(specification, _slice_campaign(materialized_program_inputs, campaign))
        for campaign in range(count)
    )


def evaluate_program_decision(
    specification: Mapping[str, object], materialized_program_inputs: Mapping[str, object]
) -> dict[str, object]:
    """Evaluate one real/synthetic campaign through the shared batch route."""

    if int(materialized_program_inputs.get("campaign_count", 0)) != 1:
        raise IntegrityError("use evaluate_program_decision_batch for multiple campaigns")
    return evaluate_program_decision_batch(specification, materialized_program_inputs)[0]


def evaluate_portfolio_decision(specification: Mapping[str, object], decisions: Sequence[Mapping[str, object]]) -> dict[str, object]:
    passed = [item for item in decisions if item.get("outcome") == "PASS"]
    if len(passed) < 2:
        raise IntegrityError("portfolio trial 169 requires at least two individually passing programs")
    if len({tuple(item["exchange_session_dates"]) for item in passed}) != 1:
        raise IntegrityError("portfolio sleeves must share exact synchronized dates")
    dates = tuple(passed[0]["exchange_session_dates"])
    union_markets = tuple(sorted({market for item in passed for market in item["markets"]}))
    sleeve_count = len(passed)
    gross = np.zeros((sleeve_count, len(dates), len(union_markets)), dtype=np.float64)
    base = np.zeros_like(gross)
    stress = np.zeros((sleeve_count, len(dates), len(union_markets)), dtype=np.float64)
    extreme = np.zeros_like(stress)
    adverse = np.zeros_like(stress)
    direction = np.zeros_like(stress, dtype=np.int8)
    trades = np.zeros_like(stress, dtype=np.bool_)
    residuals = np.zeros_like(stress)
    variances = np.asarray([item["outer_training_variance"] for item in passed], dtype=np.float64)
    if variances.shape != (sleeve_count, 8) or np.any(variances <= 0):
        raise IntegrityError("portfolio requires eight positive training-only sleeve variances")
    for sleeve, item in enumerate(passed):
        positions = [union_markets.index(market) for market in item["markets"]]
        cost_paths = item.get("cost_paths")
        if not isinstance(cost_paths, Mapping):
            raise IntegrityError("portfolio sleeves require complete cost paths")
        gross[sleeve][:, positions] = np.asarray(cost_paths["gross"], dtype=np.float64)
        base[sleeve][:, positions] = np.asarray(cost_paths["base"], dtype=np.float64)
        stress[sleeve][:, positions] = np.asarray(item["stress_date_market"], dtype=np.float64)
        extreme[sleeve][:, positions] = np.asarray(item["extreme_date_market"], dtype=np.float64)
        adverse[sleeve][:, positions] = np.asarray(item["adverse_date_market"], dtype=np.float64)
        direction[sleeve][:, positions] = np.asarray(item["direction"], dtype=np.int8)
        trades[sleeve][:, positions] = np.asarray(item["trade_flags"], dtype=np.bool_)
        residuals[sleeve][:, positions] = np.asarray(item["residuals"], dtype=np.float64)
    fold_weights = np.zeros((8, sleeve_count), dtype=np.float64)
    variance_contributions = np.zeros_like(fold_weights)
    portfolio_stress = np.zeros((len(dates), len(union_markets)), dtype=np.float64)
    portfolio_gross = np.zeros_like(portfolio_stress)
    portfolio_base = np.zeros_like(portfolio_stress)
    portfolio_extreme = np.zeros_like(portfolio_stress)
    portfolio_adverse = np.zeros_like(portfolio_stress)
    portfolio_trades = np.zeros_like(portfolio_stress, dtype=np.bool_)
    portfolio_direction = np.zeros_like(portfolio_stress, dtype=np.int8)
    portfolio_residual = np.zeros_like(portfolio_stress)
    for fold in range(8):
        training = np.column_stack([
            np.asarray(item["outer_training_session_returns"][fold], dtype=np.float64)
            for item in passed
        ])
        if training.ndim != 2 or training.shape[1] != sleeve_count or len(training) < 2 or np.any(~np.isfinite(training)):
            raise IntegrityError("portfolio training-only sleeve paths are incomplete")
        observed_variances = np.var(training, axis=0, ddof=1)
        if not np.allclose(observed_variances, variances[:, fold], rtol=1e-12, atol=1e-12):
            raise IntegrityError("portfolio sleeve variances do not match bound training paths")
        inverse_volatility = 1.0 / np.sqrt(variances[:, fold])
        weights = inverse_volatility / np.sum(inverse_volatility)
        fold_weights[fold] = weights
        covariance = np.asarray(np.cov(training, rowvar=False, ddof=1), dtype=np.float64)
        total_variance = float(weights @ covariance @ weights)
        if not np.isfinite(total_variance) or total_variance <= 0:
            variance_contributions[fold] = np.inf
        else:
            variance_contributions[fold] = weights * (covariance @ weights) / total_variance
        rows = slice(fold * 63, (fold + 1) * 63)
        for date in range(fold * 63, (fold + 1) * 63):
            for market in range(len(union_markets)):
                intents = direction[:, date, market]
                if np.any(intents > 0) and np.any(intents < 0):
                    continue
                portfolio_gross[date, market] = float(np.dot(weights, gross[:, date, market]))
                portfolio_base[date, market] = float(np.dot(weights, base[:, date, market]))
                portfolio_stress[date, market] = float(np.dot(weights, stress[:, date, market]))
                portfolio_extreme[date, market] = float(np.dot(weights, extreme[:, date, market]))
                portfolio_adverse[date, market] = float(np.dot(weights, adverse[:, date, market]))
                portfolio_residual[date, market] = float(np.dot(weights, residuals[:, date, market]))
                portfolio_trades[date, market] = bool(np.any(trades[:, date, market]))
                portfolio_direction[date, market] = int(np.sign(np.sum(intents)))
    failures = []
    if np.any(variance_contributions > 0.6):
        failures.append("SLEEVE_VARIANCE_CONTRIBUTION")
    shared_indices = stationary_bootstrap_indices(
        n_observations=len(dates), n_resamples=999, mean_block_length=20.0, seed=169
    )
    lower = _shared_date_lower(portfolio_stress, indices=shared_indices)
    if lower <= 10.0 or float(np.mean(portfolio_stress)) <= 0 or float(np.mean(portfolio_extreme)) <= 0:
        failures.append("PROFITABILITY_OR_MEES")
    fold_means = [float(np.mean(portfolio_stress[fold * 63:(fold + 1) * 63])) for fold in range(8)]
    fold_means_by_market = {market: [float(np.mean(portfolio_stress[fold * 63:(fold + 1) * 63, market_id])) for fold in range(8)] for market_id, market in enumerate(union_markets)}
    if sum(value > 0 for value in fold_means) < 5 or int(np.count_nonzero(portfolio_trades)) < 40:
        failures.append("TRADES_OR_POSITIVE_FOLDS")
    market_lowers = {market: _shared_date_lower(portfolio_stress[:, index:index + 1], indices=shared_indices) for index, market in enumerate(union_markets)}
    if any(market_lowers[market] <= 10.0 or float(np.mean(portfolio_extreme[:, index])) <= 0 or np.count_nonzero(portfolio_trades[:, index]) < 40 or sum(value > 0 for value in fold_means_by_market[market]) < 5 for index, market in enumerate(union_markets)):
        failures.append("MARKET_REPLICATION")
    family_name = {"ES": "EQUITY", "NQ": "EQUITY", "CL": "ENERGY", "GC": "METALS", "ZN": "RATES", "ZB": "RATES"}
    families: dict[str, list[int]] = {}
    for index, market in enumerate(union_markets):
        families.setdefault(family_name[market], []).append(index)
    family_lowers = {name: _shared_date_lower(portfolio_stress[:, positions], indices=shared_indices) for name, positions in sorted(families.items())}
    lomo_lowers = {market: _shared_date_lower(portfolio_stress[:, [index for index in range(len(union_markets)) if index != market_index]], indices=shared_indices) for market_index, market in enumerate(union_markets)} if len(union_markets) > 1 else {}
    if any(value <= 0 for value in (*family_lowers.values(), *lomo_lowers.values())):
        failures.append("FAMILY_OR_LEAVE_ONE_OUT_REPLICATION")
    residual_lower = _shared_date_lower(portfolio_residual, indices=shared_indices)
    if residual_lower <= 0 or float(np.mean(portfolio_residual)) <= 0:
        failures.append("RESIDUAL_ALPHA")
    scientific_report, tail_failures = _scientific_report(
        gross=portfolio_gross,
        base=portfolio_base,
        stress=portfolio_stress,
        extreme=portfolio_extreme,
        direction=portfolio_direction,
        regimes={},
        adverse=portfolio_adverse,
    )
    tail = scientific_report["tail"]
    inconclusive = [failure for failure in tail_failures if failure == "ONE_SECOND_ADVERSE_PATH"]
    failures.extend(failure for failure in tail_failures if failure != "ONE_SECOND_ADVERSE_PATH")
    market_tail_reports = {}
    for market_id, market in enumerate(union_markets):
        market_tail, market_tail_failures = _tail_gates(portfolio_stress[:, market_id:market_id + 1], portfolio_adverse[:, market_id:market_id + 1])
        market_tail_reports[market] = market_tail
        for failure in market_tail_failures:
            target = inconclusive if failure == "ONE_SECOND_ADVERSE_PATH" else failures
            target.append(f"MARKET_{market}_{failure}")
    differential_columns = [np.mean(portfolio_stress, axis=1)]
    hypothesis_ids = ["ZERO"]
    differential_columns.extend(np.mean(portfolio_stress - stress[index], axis=1) for index in range(sleeve_count))
    hypothesis_ids.extend(f"SLEEVE_{index}" for index in range(sleeve_count))
    for sleeve in range(sleeve_count):
        for market_id, market in enumerate(union_markets):
            differential_columns.append(portfolio_stress[:, market_id] - stress[sleeve, :, market_id])
            hypothesis_ids.append(f"SLEEVE_{sleeve}_MARKET_{market}")
    for index, market in enumerate(union_markets):
        differential_columns.append(portfolio_stress[:, index])
        hypothesis_ids.append(f"MARKET_{market}")
    for name, positions in sorted(families.items()):
        differential_columns.append(np.mean(portfolio_stress[:, positions], axis=1))
        hypothesis_ids.append(f"FAMILY_{name}")
    for market_index, market in enumerate(union_markets):
        keep = [index for index in range(len(union_markets)) if index != market_index]
        if keep:
            differential_columns.append(np.mean(portfolio_stress[:, keep], axis=1))
            hypothesis_ids.append(f"LOMO_{market}")
    differential_columns.append(np.mean(portfolio_residual, axis=1))
    hypothesis_ids.append("RESIDUAL_AGGREGATE")
    for index, market in enumerate(union_markets):
        differential_columns.append(portfolio_residual[:, index])
        hypothesis_ids.append(f"RESIDUAL_MARKET_{market}")
    differentials = np.column_stack(differential_columns)
    rw_inconclusive = False
    try:
        rw = romano_wolf_from_differentials(differentials, hypothesis_ids=tuple(hypothesis_ids), hac_lag=4, mean_block_length=20.0, n_resamples=999, seed=169, minimum_resamples=999)
        adjusted = rw.adjusted_p_values
    except ResearchContractError:
        adjusted = np.ones(1 + sleeve_count, dtype=np.float64)
        rw_inconclusive = True
    if rw_inconclusive:
        inconclusive.append("ROMANO_WOLF_INCONCLUSIVE")
    elif np.any(adjusted > 0.05 / 169):
        failures.append("ROMANO_WOLF_169")
    outcome = "FAIL" if failures else ("INCONCLUSIVE" if inconclusive else "PASS")
    core = {"schema_version": DECISION_SCHEMA, "trial": 169, "included_program_ids": [item["program_id"] for item in passed], "exchange_session_dates": list(dates), "markets": list(union_markets), "fold_weights": fold_weights.tolist(), "variance_contributions": variance_contributions.tolist(), "outer_fold_means": fold_means, "outer_fold_means_by_market": fold_means_by_market, "cost_paths": {"gross": portfolio_gross.tolist(), "base": portfolio_base.tolist(), "stress": portfolio_stress.tolist(), "extreme": portfolio_extreme.tolist()}, "stress_date_market": portfolio_stress.tolist(), "extreme_date_market": portfolio_extreme.tolist(), "adverse_date_market": portfolio_adverse.tolist(), "residual_date_market": portfolio_residual.tolist(), "direction": portfolio_direction.tolist(), "trade_flags": portfolio_trades.tolist(), "bootstrap_lower": {"aggregate_mees": lower, "markets": market_lowers, "families": family_lowers, "leave_one_market_out": lomo_lowers, "residual": residual_lower}, "romano_wolf_hypothesis_ids": hypothesis_ids, "romano_wolf_adjusted_p": adjusted.tolist(), "scientific_report": scientific_report, "tail_report": tail, "market_tail_reports": market_tail_reports, "failed_gates": sorted(set(failures)), "inconclusive_gates": sorted(set(inconclusive)), "outcome": outcome, "authoritative": False, "evidence_label": "NO_EDGE"}
    return _identified(core, "calculation_id")


def _validate_monitor_chain(
    root: Path,
    monitor_root: Path,
    latest: Path,
    existing: Sequence[Path],
    authority_id: str,
) -> dict[str, object]:
    remaining = {path.resolve(strict=True) for path in existing}
    current = latest.resolve(strict=True)
    later_session: str | None = None
    latest_value: dict[str, object] | None = None
    while True:
        if current not in remaining:
            raise IntegrityError("monitor chain contains a cycle, branch, or missing predecessor")
        remaining.remove(current)
        value = _read_json(current)
        core = {key: item for key, item in value.items() if key != "checkpoint_id"}
        if value.get("authority_id") != authority_id or value.get("checkpoint_id") != sha256_json(core):
            raise IntegrityError("monitor checkpoint identity is invalid")
        session = str(value.get("session_id", ""))
        if not session or (later_session is not None and session >= later_session):
            raise IntegrityError("monitor session identities are not strictly increasing")
        if latest_value is None:
            latest_value = value
        prior = value.get("prior")
        if prior is None:
            break
        if not isinstance(prior, Mapping) or not isinstance(prior.get("path"), str):
            raise IntegrityError("monitor predecessor binding is malformed")
        relative = Path(str(prior["path"]))
        if relative.is_absolute():
            raise IntegrityError("monitor predecessor path must be repository-relative")
        predecessor = (root / relative).resolve(strict=True)
        try:
            predecessor.relative_to(monitor_root.resolve(strict=False))
        except ValueError as exc:
            raise IntegrityError("monitor predecessor escapes its authority chain") from exc
        if prior.get("sha256") != sha256_file(predecessor):
            raise IntegrityError("monitor predecessor hash changed")
        predecessor_value = _read_json(predecessor)
        if prior.get("checkpoint_id") != predecessor_value.get("checkpoint_id"):
            raise IntegrityError("monitor predecessor identity changed")
        later_session = session
        current = predecessor
    if remaining:
        raise IntegrityError("monitor registry contains an orphaned or skipped checkpoint")
    assert latest_value is not None
    return latest_value


def advance_forward_monitor(
    root: Path,
    authority_id: str,
    prior_checkpoint_path: Path | None,
    session_id: str,
    descriptive: Mapping[str, object],
    *,
    action: str = "MAINTAIN",
) -> dict[str, object]:
    assert_authority_not_blocked(root, authority_id)
    if action not in {"MAINTAIN", "DOWNGRADE", "PAUSE", "RETIRE"}:
        raise IntegrityError("monitor action cannot promote, redesign, or rescue evidence")
    allowed = {"stress_pnl", "extreme_pnl", "costs", "exposures", "drawdown", "concentration", "tail"}
    if not isinstance(descriptive, Mapping) or not set(descriptive) <= allowed or not descriptive:
        raise IntegrityError("monitor checkpoint contains non-descriptive or unsupported fields")
    monitor_root = root / MONITOR_ROOT / authority_id
    if not isinstance(session_id, str) or not session_id:
        raise IntegrityError("monitor session identity is required")
    existing = list(monitor_root.glob("*/checkpoint.json")) if monitor_root.exists() else []
    latest_existing = max(existing, key=lambda path: str(_read_json(path).get("session_id"))) if existing else None
    latest_value = None if latest_existing is None else _validate_monitor_chain(root, monitor_root, latest_existing, existing, authority_id)
    previous = None
    if prior_checkpoint_path is not None:
        if prior_checkpoint_path.is_absolute():
            raise IntegrityError("prior monitor checkpoint path must be repository-relative")
        absolute_prior = (root / prior_checkpoint_path).resolve(strict=True)
        try:
            absolute_prior.relative_to(monitor_root.resolve(strict=False))
        except ValueError as exc:
            raise IntegrityError("prior monitor checkpoint escapes its authority chain") from exc
        previous = _read_json(absolute_prior)
        if previous != latest_value:
            raise IntegrityError("monitor chain cannot skip the validated latest checkpoint")
        if str(previous.get("session_id")) >= session_id:
            raise IntegrityError("monitor session identity must strictly increase")
        if latest_existing is not None and absolute_prior != latest_existing.resolve(strict=True):
            raise IntegrityError("monitor chain cannot skip the latest checkpoint")
    elif existing:
        raise IntegrityError("an existing monitor chain requires its latest checkpoint")
    core = {"schema_version": MONITOR_SCHEMA, "authority_id": authority_id, "session_id": session_id, "prior": None if previous is None else {"checkpoint_id": previous["checkpoint_id"], "path": prior_checkpoint_path.as_posix(), "sha256": sha256_file(root / prior_checkpoint_path)}, "descriptive": dict(descriptive), "action": action, "inferential_state": "BLOCKED_MISSING_FINITE_SESSION_PNL_BOUND", "permitted_actions": ["MAINTAIN", "DOWNGRADE", "PAUSE", "RETIRE"], "promotion_forbidden": True, "redesign_forbidden": True, "historical_rescue_forbidden": True}
    checkpoint = _identified(core, "checkpoint_id")
    path = MONITOR_ROOT / authority_id / str(checkpoint["checkpoint_id"]) / "checkpoint.json"
    write_new_canonical_authority(root / path, checkpoint)
    return checkpoint


def build_blocked_authority(root: Path, program: Mapping[str, object], failure: Mapping[str, object], synthetic: Mapping[str, object], source: Mapping[str, object]) -> dict[str, object]:
    core = {"schema_version": AUTHORITY_SCHEMA, "state": "PREPARED_NOT_ACTIVE", "program": {"program_id": program["program_id"], "path": (PROGRAM_ROOT / str(program["program_id"]) / "program.json").as_posix()}, "failure_record": {"failure_id": failure["failure_id"], "path": (FAILURE_ROOT / str(failure["failure_id"]) / "failure.json").as_posix()}, "synthetic_acceptance": {"manifest_id": synthetic["manifest_id"], "path": (SYNTHETIC_ROOT / str(synthetic["manifest_id"]) / "manifest.json").as_posix()}, "source_registration": {"registration_id": source["registration_id"], "path": (SOURCE_REGISTRATION_ROOT / str(source["registration_id"]) / "registration.json").as_posix()}, "certification": {"unsatisfied_gates": ["PERFORMANCE_CONFIGURATION_CERTIFIED", "EXACT_10000_NULL_AND_5000_KNOWN_EDGE_CERTIFICATION_TWICE", "CERTIFIED_ONE_SECOND_SOURCE_BINDING", "INDEPENDENT_FRESH_PROCESS_REPRODUCTION"]}, "activation": {"candidate_pointer_bytes_prepared": False, "preimages": {path.as_posix(): sha256_file(root / path) for path in ACTIVE_POINTERS}, "unsatisfied_gates": ["SEPARATE_ACTIVATION_APPROVAL"]}, "authority": {"activation": False, "rows": False, "provider": False, "holdout": False, "forward": False, "publication": False, "trading": False, "git": False}}
    return _identified(core, "authority_id")


def build_certified_inactive_authority(
    root: Path,
    prepared: Mapping[str, object],
    source: Mapping[str, object],
    first_run_path: Path,
    second_run_path: Path,
) -> dict[str, object]:
    """Issue only the non-active authority after every certification gate is observed."""

    _verified_identity(prepared, "authority_id")
    _verified_identity(source, "registration_id")
    first = _read_json(root / first_run_path)
    second = _read_json(root / second_run_path)
    for run in (first, second):
        _verified_identity(run, "acceptance_id")
    if (
        source.get("state") != "ROW_COMPLETE_CERTIFIED_NOT_ACTIVE"
        or source.get("program_id") != prepared.get("program", {}).get("program_id")
        or first.get("acceptance_passed") is not True
        or second.get("acceptance_passed") is not True
        or first.get("program_id") != prepared.get("program", {}).get("program_id")
        or second.get("program_id") != first.get("program_id")
        or first.get("run_ordinal") != 1
        or second.get("run_ordinal") != 2
        or canonical_bytes(first.get("terminal_outcomes")) != canonical_bytes(second.get("terminal_outcomes"))
        or first.get("merkle_digest") != second.get("merkle_digest")
        or first.get("manifest_id") != prepared.get("synthetic_acceptance", {}).get("manifest_id")
        or second.get("manifest_id") != first.get("manifest_id")
        or any(sha256_file(root / Path(path)) != digest for path, digest in prepared.get("activation", {}).get("preimages", {}).items())
    ):
        raise IntegrityError("inactive authority certification gates are not all satisfied")
    core = {
        **{key: value for key, value in prepared.items() if key not in {"authority_id", "state", "source_registration", "certification"}},
        "state": "INACTIVE_CERTIFIED_NOT_ACTIVE",
        "predecessor_authority_id": prepared["authority_id"],
        "source_registration": {"registration_id": source["registration_id"], "path": (SOURCE_REGISTRATION_ROOT / str(source["registration_id"]) / "registration.json").as_posix()},
        "certification": {
            "runs": [
                {"acceptance_id": first["acceptance_id"], **_binding(root, first_run_path)},
                {"acceptance_id": second["acceptance_id"], **_binding(root, second_run_path)},
            ],
            "terminal_sha256": sha256_json(first["terminal_outcomes"]),
            "merkle_digest": first["merkle_digest"],
            "unsatisfied_gates": [],
        },
    }
    return _identified(core, "authority_id")


def prepare_inactive_successor(root: Path, performance_evidence: Mapping[str, object] | None = None) -> dict[str, object]:
    root = root.resolve(strict=True)
    if not isinstance(performance_evidence, Mapping) or performance_evidence.get("qualifies") is not True:
        raise IntegrityError("a qualifying scalar-equivalent performance benchmark is required before successor artifacts")
    chunk_size = int(performance_evidence.get("selected_chunk_size", 0))
    workers = int(performance_evidence.get("selected_workers", 0))
    failure = build_failure_record(root)
    failure_path = FAILURE_ROOT / str(failure["failure_id"]) / "failure.json"
    write_new_canonical_authority(root / failure_path, failure)
    program = build_program(root, failure)
    if performance_evidence.get("program_id") != program["program_id"]:
        raise IntegrityError("performance benchmark is not bound to the current program")
    program_path = PROGRAM_ROOT / str(program["program_id"]) / "program.json"
    write_new_canonical_authority(root / program_path, program)
    source = build_source_registration(root, program)
    source_path = SOURCE_REGISTRATION_ROOT / str(source["registration_id"]) / "registration.json"
    write_new_canonical_authority(root / source_path, source)
    synthetic = synthetic_certification_manifest(program, chunk_size=chunk_size, workers=workers, performance_evidence=performance_evidence)
    synthetic_path = SYNTHETIC_ROOT / str(synthetic["manifest_id"]) / "manifest.json"
    write_new_canonical_authority(root / synthetic_path, synthetic)
    authority = build_blocked_authority(root, program, failure, synthetic, source)
    authority_path = AUTHORITY_ROOT / str(authority["authority_id"]) / "authority.json"
    write_new_canonical_authority(root / authority_path, authority)
    return {"failure_id": failure["failure_id"], "failure_path": failure_path.as_posix(), "program_id": program["program_id"], "program_path": program_path.as_posix(), "source_registration_id": source["registration_id"], "source_registration_path": source_path.as_posix(), "synthetic_manifest_id": synthetic["manifest_id"], "synthetic_manifest_path": synthetic_path.as_posix(), "authority_id": authority["authority_id"], "authority_path": authority_path.as_posix(), "status": "PREPARED_NOT_ACTIVE_BLOCKED_UNSATISFIED_CERTIFICATION_GATES"}
