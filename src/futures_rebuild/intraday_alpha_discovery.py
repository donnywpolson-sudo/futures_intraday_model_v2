"""Frozen intraday research programs and their one fail-closed decision path.

This module performs no market-data I/O.  The certified gateway must produce
the aligned session arrays used here after a separately authorized execution.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import product
from math import ceil, isfinite, sqrt
from pathlib import Path
from statistics import NormalDist

import numpy as np

from .canonical import sha256_file, sha256_json, write_new_canonical_authority
from .errors import IntegrityError
from .research.cscv import exhaustive_cscv_pbo
from .research.dsr import deflated_sharpe_ratio
from .research.multiple_testing import romano_wolf_from_differentials
from .research.bootstrap import stationary_bootstrap_index_rows
from .research.contracts import ResearchContractError, finite_float64
from .research_gateway_policy import scientific_governance_binding


PROGRAM_SCHEMA = "intraday_research_program/1.1.0"
DECISION_SCHEMA = "intraday_research_decision/1.1.0"
AUTHORITY_SCHEMA = "intraday_research_authority_successor/1.1.0"
SYNTHETIC_SCHEMA = "intraday_research_synthetic_acceptance/1.1.0"
PROGRAM_ROOT = Path("state/intraday_research_program_registry")
AUTHORITY_ROOT = Path("state/intraday_research_authority_registry")
ACTIVE_LADDER = Path("configs/active_alpha_research_ladder.json")
ACTIVE_HISTORICAL = Path("configs/active_alpha_historical_research_design.json")
ACTIVE_EXECUTION = Path("configs/active_alpha_ladder_execution_design.json")
CATALOG = Path("data/active/catalog.json")
CAUSAL_OBSERVATION = Path("configs/causal_observation_contract_v1.json")
ABANDONED_DESIGN = Path("configs/bounded_alpha_discovery_contract_v1.json")
STAGE_2_5 = Path(
    "state/causal_search_access_contract_registry/"
    "5bba29ca7a1b9b57360d92427ad22b6fb7dacd4af8cbf4807db1a2fa13e8373d/"
    "contract.json"
)
REQUIRED_CATALOG_MARKETS = ("6E", "6J", "CL", "ES", "GC", "NQ", "SR3", "ZB", "ZN")
BASELINES = (
    "FLAT_ZERO",
    "FOLD_LOCAL_MARKET_SESSION_UNCONDITIONAL",
    "PREVIOUS_BAR_SIGN_MOMENTUM",
    "PREVIOUS_BAR_SIGN_REVERSAL",
    "RISK_MATCHED_ALWAYS_LONG_INTRADAY",
    "RISK_MATCHED_ALWAYS_SHORT_INTRADAY",
)
REGIME_DIMENSIONS = ("volatility", "trend", "gap", "liquidity")
EXPOSURE_NAMES = ("market_direction", "trend", "realized_volatility", "liquidity", "gap", "time_of_day")
MARKET_FAMILIES = {"ES": "EQUITY", "NQ": "EQUITY", "CL": "ENERGY", "GC": "METALS", "ZN": "RATES", "ZB": "RATES"}
EVIDENCE_LABELS = (
    "NO_EDGE",
    "HISTORICAL_RESEARCH_EDGE",
    "HOLDOUT_CONFIRMED_RESEARCH_EDGE",
    "PROSPECTIVE_EVIDENCE",
)
_NORMAL = NormalDist()


@dataclass(frozen=True)
class IntradayBar:
    """One already-certified one-minute observation; this type performs no I/O."""

    market: str
    session_date: str
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


def _read_json(path: Path) -> dict[str, object]:
    import json

    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"metadata is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"metadata must be an object: {path}")
    return value


def _identified(core: dict[str, object], field: str) -> dict[str, object]:
    return {**core, field: sha256_json(core)}


def _binding(root: Path, relative: Path) -> dict[str, str]:
    return {"path": relative.as_posix(), "sha256": sha256_file(root / relative)}


def _candidate(mechanism: str, ordinal: int, **parameters: object) -> dict[str, object]:
    core = {"mechanism": mechanism, "ordinal": ordinal, **parameters}
    return {**core, "candidate_id": sha256_json(core)}


def enumerate_programs() -> list[dict[str, object]]:
    """Return the exact three-program, 36-candidate first generation."""

    programs: list[dict[str, object]] = []
    definitions = (
        (
            "FLOW_CONTINUATION",
            {
                "economic_actor_or_constraint": (
                    "persistent institutional or informed flow after a high-volume move"
                ),
                "markets": ["ES", "NQ", "CL", "GC"],
                "position": "SAME_DIRECTION_AS_TRIGGER",
                "trigger_window_minutes": 15,
                "return_z_thresholds": [1.5, 2.0],
                "volume_surprise_thresholds": [0.5, 1.0],
                "falsifying_boundaries": ["LOW_VOLUME_MOVE", "TEMPORALLY_REVERSED_CONTROL"],
                "literature": "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=712168",
            },
            "return_z_threshold",
        ),
        (
            "LIQUIDITY_SHOCK_REVERSAL",
            {
                "economic_actor_or_constraint": (
                    "urgent liquidity demand creates temporary pressure later absorbed"
                ),
                "markets": ["ES", "NQ", "CL", "GC"],
                "position": "OPPOSITE_DIRECTION_TO_TRIGGER",
                "trigger_window_minutes": 1,
                "return_z_thresholds": [2.5, 3.0],
                "volume_surprise_thresholds": [0.5, 1.0],
                "falsifying_boundaries": ["LOW_VOLUME_SHOCK", "UNCONDITIONAL_MEAN_REVERSION"],
                "literature": "https://academic.oup.com/rfs/article-abstract/25/7/2005/1602153",
            },
            "return_z_threshold",
        ),
        (
            "CROSS_MARKET_INFORMATION_TRANSFER",
            {
                "economic_actor_or_constraint": (
                    "a more responsive related contract incorporates information before its follower"
                ),
                "pairs": [{"leader": "ES", "follower": "NQ"}, {"leader": "ZN", "follower": "ZB"}],
                "position": "FOLLOWER_IN_OUTER_TRAINING_BETA_ADJUSTED_LEADER_DIRECTION",
                "trigger_window_minutes": 5,
                "return_z_thresholds": [1.5, 2.0],
                "incorporation_maximum_fractions": [0.25, 0.5],
                "falsifying_boundaries": ["REVERSED_LEADER_FOLLOWER_TIMING"],
                "literature": "https://onlinelibrary.wiley.com/doi/10.1046/j.1540-6261.2003.00609.x",
            },
            "leader_return_z_threshold",
        ),
    )
    global_ordinal = 0
    for name, definition, threshold_name in definitions:
        second_name = (
            "incorporation_maximum_fraction"
            if name == "CROSS_MARKET_INFORMATION_TRANSFER"
            else "volume_surprise_threshold"
        )
        second_values = (
            definition["incorporation_maximum_fractions"]
            if name == "CROSS_MARKET_INFORMATION_TRANSFER"
            else definition["volume_surprise_thresholds"]
        )
        candidates = []
        for first, second, horizon in product(
            definition["return_z_thresholds"], second_values, (15, 30, 60)
        ):
            candidates.append(
                _candidate(
                    name,
                    global_ordinal,
                    **{
                        threshold_name: first,
                        second_name: second,
                        "holding_horizon_minutes": horizon,
                    },
                )
            )
            global_ordinal += 1
        programs.append({"program_name": name, **definition, "candidates": candidates})
    return programs


def build_program(root: Path) -> dict[str, object]:
    root = root.resolve(strict=False)
    catalog = _read_json(root / CATALOG)
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are missing")
    markets = sorted({str(item.get("market")) for item in entries if isinstance(item, dict)})
    research_ready = sum(
        isinstance(item, dict) and item.get("disposition") == "RESEARCH_READY_CAUSAL_PRICE"
        for item in entries
    )
    if tuple(markets) != REQUIRED_CATALOG_MARKETS or research_ready != 136:
        raise IntegrityError("the bound nine-market catalog is not the expected research catalog")
    abandoned = _read_json(root / ABANDONED_DESIGN)
    old_candidates = abandoned.get("search")
    if (
        not isinstance(old_candidates, dict)
        or old_candidates.get("candidate_budget") != 27
        or not isinstance(old_candidates.get("candidates"), list)
        or len(old_candidates["candidates"]) != 27
        or any(
            not isinstance(item, dict) or not isinstance(item.get("candidate_id"), str)
            for item in old_candidates["candidates"]
        )
    ):
        raise IntegrityError("the abandoned 27-candidate design changed")
    programs = enumerate_programs()
    core: dict[str, object] = {
        "schema_version": PROGRAM_SCHEMA,
        "state": "PRE_RESULT_PREPARATION_ONLY_NOT_ACTIVE_NOT_REGISTERED",
        "authority": {
            "market_row_read": False,
            "feature_or_outcome_construction": False,
            "model_fit_or_prediction": False,
            "recent_challenge": False,
            "final_252_or_forward": False,
            "provider_or_network": False,
            "activation": False,
            "publication": False,
            "trading": False,
            "git_stage_commit_push": False,
        },
        "bindings": {
            "active_ladder": _binding(root, ACTIVE_LADDER),
            "active_historical_design": _binding(root, ACTIVE_HISTORICAL),
            "active_execution_design": _binding(root, ACTIVE_EXECUTION),
            "catalog": {**_binding(root, CATALOG), "markets": markets, "entry_count": len(entries), "research_ready_entries": research_ready},
            "causal_observation": _binding(root, CAUSAL_OBSERVATION),
            "scientific_governance": scientific_governance_binding(root),
            "implementation": {
                path: sha256_file(root / path)
                for path in (
                    "src/futures_rebuild/intraday_alpha_discovery.py",
                    "src/futures_rebuild/certified_research_gateway.py",
                    "src/futures_rebuild/research/bootstrap.py",
                    "src/futures_rebuild/research/controls.py",
                    "src/futures_rebuild/research/cscv.py",
                    "src/futures_rebuild/research/dsr.py",
                    "src/futures_rebuild/research/multiple_testing.py",
                    "scripts/prepare_intraday_alpha_discovery.py",
                    "scripts/independent_validate_intraday_alpha_discovery.py",
                    "tests/test_intraday_alpha_discovery.py",
                )
            },
            "abandoned_pre_result_design": {
                **_binding(root, ABANDONED_DESIGN),
                "status": "ABANDONED_PRE_RESULT_NOT_EVALUATED",
                "candidate_count": 27,
                "candidate_ids": [
                    item["candidate_id"] for item in old_candidates["candidates"]
                ],
            },
        },
        "claim_ceiling": "ROBUST_PREDICTIVE_ASSOCIATION",
        "source_rules": {
            "primary_signal_cadence": "ohlcv_1m",
            "ohlcv_1s_use": "CANDIDATE_FROZEN_AGGREGATION_AND_ADVERSE_PATH_DIAGNOSTICS_ONLY",
            "cross_market_join": "BACKWARD_ASOF_ONLY",
            "missing_or_stale_required_input": "ABSTAIN",
            "raw_dbn_direct_model_input": False,
        },
        "session_rules": {
            "timezone": "America/Chicago",
            "first_qualifying_event_only": True,
            "maximum_trades_per_market_session": 1,
            "overnight": False,
            "flat_by": "15:00:00",
            "latest_entry_by_horizon": {"15": "14:40:00", "30": "14:25:00", "60": "13:55:00"},
            "lookback_must_be_fully_observed": True,
            "same_bar_fill": "FORBIDDEN",
        },
        "standardization": {
            "return_and_volume": "OUTER_TRAINING_ONLY_BY_MARKET_AND_MINUTE_OF_SESSION",
            "cross_market_beta": "OUTER_TRAINING_ONLY",
            "outer_result_tuning": False,
        },
        "programs": programs,
        "candidate_count": 36,
        "search_accounting": {
            "legacy": 105,
            "abandoned_pre_result": 27,
            "current": 36,
            "minimum_information_family": 168,
            "missing_historical_trial_values": "DSR_DIAGNOSTIC_NOT_AVAILABLE_NEVER_FABRICATE",
        },
        "costs": {
            "fee_round_trip_usd": 10.0,
            "adverse_ticks_round_trip": {
                "base": {"ES": 2, "NQ": 2, "CL": 4, "GC": 4, "ZN": 2, "ZB": 2},
                "stress": {"ES": 4, "NQ": 4, "CL": 8, "GC": 8, "ZN": 4, "ZB": 4},
                "extreme": {"ES": 8, "NQ": 8, "CL": 16, "GC": 16, "ZN": 8, "ZB": 8},
            },
            "exact_execution_cost_claim": False,
            "cost_reduction_after_results": False,
        },
        "folds": {
            "outer": {"count": 8, "initial_training_sessions": 504, "evaluation_sessions": 63},
            "inner": {"count": 4, "selection": "MAX_MEDIAN_STRESS_COST_SHARPE_THEN_FROZEN_CANDIDATE_ORDER"},
            "chronological_expanding": True,
            "purge_minutes": 60,
            "embargo_sessions": 1,
            "outer_results_may_select_or_tune": False,
        },
        "baselines": list(BASELINES),
        "decision_rules": {
            "outcomes": ["PASS", "FAIL", "INCONCLUSIVE"],
            "dsr_role": "DIAGNOSTIC_ONLY_WHEN_COMPLETE_ACTUAL_CENSUS_EXISTS",
            "pbo_conservative_maximum": 0.2,
            "romano_wolf_adjusted_one_sided_p_maximum": 0.05 / 168.0,
            "individual_information_family_count": 168,
            "portfolio_information_family_count": 169,
            "bootstrap_resamples": 10000,
            "bootstrap_confidence": 0.95,
            "stress_mees_usd_per_complete_session": 10.0,
            "minimum_trades": 40,
            "minimum_positive_outer_folds": 5,
            "maximum_drawdown_usd": 1500.0,
            "best_sessions_removed": 5,
            "maximum_single_or_contiguous_five_session_profit_share": 0.5,
            "stress_and_extreme_mean_strictly_positive": True,
            "all_baselines_must_be_beaten": True,
            "residual_stress_mean_strictly_positive": True,
            "residual_bootstrap_lower_strictly_positive": True,
        },
        "gamma_trap_reports": {
            "costs": ["gross", "base", "stress", "extreme"],
            "directions": ["long", "short"],
            "regimes": list(REGIME_DIMENSIONS),
            "worst_session_counts": [1, 5, 10, 20],
            "moments": ["skew", "kurtosis_non_excess", "expected_shortfall_2_5", "expected_shortfall_5"],
            "path": ["drawdown_duration", "one_second_adverse_path"],
            "missing_report": "INCONCLUSIVE_NO_PROMOTION",
        },
        "stage_order": {
            "development": ["2018-01-01T00:00:00Z", "2023-01-01T00:00:00Z"],
            "recent_challenge": ["2023-01-01T00:00:00Z", "2025-07-13T22:00:00Z"],
            "challenge_order": "ONE_SCOPE_WIDE_AFTER_LAST_APPLICABLE_REPLICATION",
            "failure_action": "CLOSE_PROGRAM_NO_RETRY_NO_RESCUE_SEARCH",
            "final_252": "ONE_FAMILYWISE_EVALUATION_AFTER_FROZEN_REFIT",
            "forward": "MONITORING_ONLY_CANNOT_RESCUE_FAILURE",
        },
        "portfolio": {
            "minimum_passing_programs": 2,
            "include_every_passing_program": True,
            "subset_selection": False,
            "weights": "EQUAL_WEIGHT_RISK_NORMALIZED_USING_OUTER_TRAINING_ONLY_VOLATILITY",
            "opposing_same_market_intents": "NET_TO_ZERO_UNUSED_ALLOCATION_REMAINS_CASH",
            "counted_as_additional_trial": True,
            "maximum_sleeve_outer_training_variance_share": 0.6,
            "same_program_gates_required": True,
        },
        "power": {
            "null_campaigns": 10000,
            "known_edge_campaigns": 5000,
            "minimum_power": 0.8,
            "design_alternative_usd_per_session": 30.0,
            "mees_usd_per_session": 10.0,
            "reason_not_at_exact_mees": "A_LOWER_BOUND_STRICTLY_ABOVE_MEES_CANNOT_HAVE_80PCT_POWER_WHEN_TRUE_MEAN_EQUALS_MEES",
        },
        "evidence_labels": list(EVIDENCE_LABELS),
        "monitoring": {
            "state": "BLOCKED_MISSING_FINITE_SESSION_PNL_BOUND",
            "finite_absolute_session_pnl_bound_usd": None,
            "append_only_session_identity": True,
            "descriptive_metrics": ["stress_and_extreme_cost", "exposure_drift", "cost_drift", "drawdown", "concentration", "tail"],
            "allowed_actions": ["MAINTAIN", "DOWNGRADE", "PAUSE", "RETIRE"],
            "promotion_or_historical_rescue": False,
        },
        "executable_alpha_label_allowed": False,
        "executable_alpha_requires_later": ["BBO_OR_MBP_EVIDENCE", "PROSPECTIVE_PAPER_FILLS"],
    }
    return _identified(core, "program_id")


def validate_program(root: Path, payload: Mapping[str, object]) -> dict[str, object]:
    expected = build_program(root)
    if dict(payload) != expected:
        raise IntegrityError("intraday research program semantics or bindings drifted")
    return dict(payload)


def _validated_bar_sessions(
    bars: Sequence[IntradayBar],
) -> dict[tuple[str, str], tuple[IntradayBar, ...]]:
    grouped: dict[tuple[str, str], list[IntradayBar]] = {}
    seen: set[tuple[str, str, datetime]] = set()
    for bar in bars:
        numbers = (bar.open, bar.high, bar.low, bar.close, bar.volume, bar.tick_size, bar.tick_value)
        if (
            not bar.market
            or not bar.session_date
            or not bar.actual_identity
            or bar.event_at.tzinfo is None
            or bar.available_at.tzinfo is None
            or any(not isfinite(value) for value in numbers)
            or bar.volume < 0.0
            or bar.tick_size <= 0.0
            or bar.tick_value <= 0.0
            or bar.low > min(bar.open, bar.close)
            or bar.high < max(bar.open, bar.close)
        ):
            raise IntegrityError("intraday bar is malformed")
        key = (bar.market, bar.session_date, bar.event_at)
        if key in seen:
            raise IntegrityError("duplicate intraday bar timestamp")
        seen.add(key)
        grouped.setdefault(key[:2], []).append(bar)
    result: dict[tuple[str, str], tuple[IntradayBar, ...]] = {}
    for key, values in grouped.items():
        ordered = tuple(sorted(values, key=lambda item: item.event_at))
        if any(right.event_at <= left.event_at for left, right in zip(ordered, ordered[1:], strict=False)):
            raise IntegrityError("intraday bars are not strictly ordered")
        result[key] = ordered
    return result


def _window_return(values: Sequence[IntradayBar], index: int, minutes: int) -> float | None:
    if index < minutes or values[index].event_at - values[index - minutes].event_at != timedelta(minutes=minutes):
        return None
    if any(item.actual_identity != values[index].actual_identity for item in values[index - minutes : index + 1]):
        return None
    start = values[index - minutes].close
    return None if start <= 0.0 else float(np.log(values[index].close / start))


def _training_statistics(
    grouped: Mapping[tuple[str, str], tuple[IntradayBar, ...]],
    training_dates: set[str],
) -> tuple[dict[tuple[str, tuple[int, int], int], tuple[float, float]], dict[tuple[str, tuple[int, int]], tuple[float, float]], dict[tuple[str, str], float]]:
    returns: dict[tuple[str, tuple[int, int], int], list[float]] = {}
    volumes: dict[tuple[str, tuple[int, int]], list[float]] = {}
    pair_samples: dict[tuple[str, str], list[tuple[float, float]]] = {
        ("ES", "NQ"): [],
        ("ZN", "ZB"): [],
    }
    by_market_date_time: dict[tuple[str, str, datetime], tuple[tuple[IntradayBar, ...], int]] = {}
    for (market, date), values in grouped.items():
        if date not in training_dates:
            continue
        for index, bar in enumerate(values):
            minute = (bar.event_at.hour, bar.event_at.minute)
            volumes.setdefault((market, minute), []).append(float(np.log1p(bar.volume)))
            for window in (1, 5, 15):
                value = _window_return(values, index, window)
                if value is not None:
                    returns.setdefault((market, minute, window), []).append(value)
            by_market_date_time[(market, date, bar.event_at)] = (values, index)
    for leader, follower in pair_samples:
        for (market, date, timestamp), (leader_values, leader_index) in by_market_date_time.items():
            if market != leader:
                continue
            follower_item = by_market_date_time.get((follower, date, timestamp))
            if follower_item is None:
                continue
            leader_return = _window_return(leader_values, leader_index, 5)
            follower_return = _window_return(follower_item[0], follower_item[1], 5)
            if leader_return is not None and follower_return is not None:
                pair_samples[(leader, follower)].append((leader_return, follower_return))

    def moments(values: Sequence[float]) -> tuple[float, float]:
        if len(values) < 2:
            return (float("nan"), float("nan"))
        scale = float(np.std(values, ddof=1))
        return (float(np.mean(values)), scale)

    return_stats = {key: moments(values) for key, values in returns.items()}
    volume_stats = {key: moments(values) for key, values in volumes.items()}
    betas: dict[tuple[str, str], float] = {}
    for pair, samples in pair_samples.items():
        if len(samples) < 2:
            betas[pair] = float("nan")
            continue
        array = np.asarray(samples, dtype=np.float64)
        variance = float(np.var(array[:, 0], ddof=1))
        betas[pair] = float(np.cov(array[:, 0], array[:, 1], ddof=1)[0, 1] / variance) if variance > 0.0 else float("nan")
    return return_stats, volume_stats, betas


def materialize_candidate_matrices(
    specification: Mapping[str, object],
    *,
    program_name: str,
    bars: Sequence[IntradayBar],
    training_session_dates: Sequence[str],
    evaluation_session_dates: Sequence[str],
) -> dict[str, object]:
    """Create frozen candidate PnL matrices from certified bars without opening files."""

    if specification.get("schema_version") != PROGRAM_SCHEMA:
        raise IntegrityError("intraday program specification schema is invalid")
    programs = specification.get("programs")
    if not isinstance(programs, list):
        raise IntegrityError("program list is missing")
    try:
        program = next(item for item in programs if item.get("program_name") == program_name)
    except StopIteration as exc:
        raise IntegrityError("unknown frozen program") from exc
    training_dates = tuple(training_session_dates)
    evaluation_dates = tuple(evaluation_session_dates)
    if (
        not training_dates
        or not evaluation_dates
        or tuple(sorted(set(training_dates))) != training_dates
        or tuple(sorted(set(evaluation_dates))) != evaluation_dates
        or set(training_dates) & set(evaluation_dates)
        or max(training_dates) >= min(evaluation_dates)
    ):
        raise IntegrityError("training and evaluation dates must be unique, chronological, and disjoint")
    grouped = _validated_bar_sessions(bars)
    return_stats, volume_stats, betas = _training_statistics(grouped, set(training_dates))
    candidates = program.get("candidates")
    assert isinstance(candidates, list)
    if program_name == "CROSS_MARKET_INFORMATION_TRANSFER":
        scopes = [(str(item["leader"]), str(item["follower"])) for item in program["pairs"]]
    else:
        scopes = [(None, str(market)) for market in program["markets"]]
    rows = [(date, leader, market) for date in evaluation_dates for leader, market in scopes]
    row_count = len(rows)
    matrices = {name: np.zeros((row_count, len(candidates)), dtype=np.float64) for name in ("gross", "base", "stress", "extreme")}
    trades = np.zeros((row_count, len(candidates)), dtype=np.bool_)
    directions = np.zeros((row_count, len(candidates)), dtype=np.int8)
    long_move_pnl = np.zeros((row_count, len(candidates)), dtype=np.float64)
    stress_cost_usd = np.zeros((row_count, len(candidates)), dtype=np.float64)
    previous_bar_sign = np.zeros((row_count, len(candidates)), dtype=np.int8)
    event_times: list[list[str | None]] = [[None] * len(candidates) for _ in rows]
    costs = specification["costs"]
    assert isinstance(costs, Mapping)
    fee = float(costs["fee_round_trip_usd"])
    adverse = costs["adverse_ticks_round_trip"]
    assert isinstance(adverse, Mapping)

    for row_index, (date, leader, market) in enumerate(rows):
        follower_bars = grouped.get((market, date), ())
        leader_bars = grouped.get((leader, date), ()) if leader is not None else ()
        leader_by_time = {bar.event_at: (index, bar) for index, bar in enumerate(leader_bars)}
        for candidate_index, candidate in enumerate(candidates):
            horizon = int(candidate["holding_horizon_minutes"])
            latest = {15: (14, 40), 30: (14, 25), 60: (13, 55)}[horizon]
            trigger_window = int(program["trigger_window_minutes"])
            for index, bar in enumerate(follower_bars):
                if index + horizon + 1 >= len(follower_bars):
                    break
                entry = follower_bars[index + 1]
                exit_bar = follower_bars[index + horizon + 1]
                if (entry.event_at.hour, entry.event_at.minute) > latest:
                    break
                if entry.event_at - bar.event_at != timedelta(minutes=1) or exit_bar.event_at - entry.event_at != timedelta(minutes=horizon):
                    continue
                path = follower_bars[max(0, index - trigger_window) : index + horizon + 2]
                if bar.available_at > entry.event_at or any(item.actual_identity != bar.actual_identity for item in path):
                    continue
                if leader is None:
                    raw_return = _window_return(follower_bars, index, trigger_window)
                    minute = (bar.event_at.hour, bar.event_at.minute)
                    mean_scale = return_stats.get((market, minute, trigger_window))
                    volume_mean_scale = volume_stats.get((market, minute))
                    if raw_return is None or mean_scale is None or volume_mean_scale is None or mean_scale[1] <= 0.0 or volume_mean_scale[1] <= 0.0:
                        continue
                    return_z = (raw_return - mean_scale[0]) / mean_scale[1]
                    volume_z = (float(np.log1p(bar.volume)) - volume_mean_scale[0]) / volume_mean_scale[1]
                    if abs(return_z) < float(candidate["return_z_threshold"]) or volume_z < float(candidate["volume_surprise_threshold"]):
                        continue
                    direction = 1 if return_z > 0.0 else -1
                    if program_name == "LIQUIDITY_SHOCK_REVERSAL":
                        direction *= -1
                else:
                    leader_item = leader_by_time.get(bar.event_at)
                    if leader_item is None:
                        continue
                    leader_return = _window_return(leader_bars, leader_item[0], 5)
                    follower_return = _window_return(follower_bars, index, 5)
                    minute = (bar.event_at.hour, bar.event_at.minute)
                    mean_scale = return_stats.get((leader, minute, 5))
                    beta = betas.get((leader, market), float("nan"))
                    if leader_return is None or follower_return is None or mean_scale is None or mean_scale[1] <= 0.0 or not isfinite(beta) or beta == 0.0:
                        continue
                    leader_z = (leader_return - mean_scale[0]) / mean_scale[1]
                    implied = beta * leader_return
                    if abs(leader_z) < float(candidate["leader_return_z_threshold"]) or abs(follower_return) > float(candidate["incorporation_maximum_fraction"]) * abs(implied):
                        continue
                    direction = 1 if implied > 0.0 else -1
                raw_long_move = (exit_bar.open - entry.open) / entry.tick_size * entry.tick_value
                gross = raw_long_move * direction
                matrices["gross"][row_index, candidate_index] = gross
                for scenario in ("base", "stress", "extreme"):
                    ticks = int(adverse[scenario][market])
                    matrices[scenario][row_index, candidate_index] = gross - fee - ticks * entry.tick_value
                trades[row_index, candidate_index] = True
                directions[row_index, candidate_index] = direction
                long_move_pnl[row_index, candidate_index] = raw_long_move
                stress_cost_usd[row_index, candidate_index] = fee + int(adverse["stress"][market]) * entry.tick_value
                if index > 0:
                    previous_bar_sign[row_index, candidate_index] = 1 if bar.close > follower_bars[index - 1].close else -1 if bar.close < follower_bars[index - 1].close else 0
                event_times[row_index][candidate_index] = bar.event_at.isoformat()
                break
    return {
        "session_ids": tuple(f"{date}|{market}" for date, _leader, market in rows),
        "session_dates": tuple(date for date, _leader, _market in rows),
        "market_ids": tuple(market for _date, _leader, market in rows),
        "mechanism_pair_ids": (
            tuple(f"{leader}->{market}" for _date, leader, market in rows)
            if program_name == "CROSS_MARKET_INFORMATION_TRANSFER"
            else None
        ),
        "candidate_returns_by_cost": matrices,
        "trade_flags": trades,
        "direction_by_candidate": directions,
        "long_move_pnl_by_candidate": long_move_pnl,
        "stress_cost_usd_by_candidate": stress_cost_usd,
        "previous_bar_sign_by_candidate": previous_bar_sign,
        "event_times_by_candidate": event_times,
        "statistics_scope": "TRAINING_SESSION_DATES_ONLY",
    }


def materialize_program_inputs(
    specification: Mapping[str, object],
    *,
    program_name: str,
    bars: Sequence[IntradayBar],
    training_session_dates: Sequence[str],
    evaluation_session_dates: Sequence[str],
) -> dict[str, object]:
    """Materialize inner-WFA selection, outer matrices, and frozen baselines."""

    training_dates = tuple(training_session_dates)
    evaluation_dates = tuple(evaluation_session_dates)
    if len(training_dates) < 10 or len(evaluation_dates) < 8:
        raise IntegrityError("program materialization requires at least ten training and eight evaluation sessions")
    chunks = [tuple(str(value) for value in chunk.tolist()) for chunk in np.array_split(np.asarray(training_dates, dtype=object), 5)]
    if any(not chunk for chunk in chunks):
        raise IntegrityError("inner WFA cannot form five chronological chunks")
    inner_scores = np.full((12, 4), -1.0e12, dtype=np.float64)
    inner_paths: list[dict[str, object]] = []
    for fold in range(4):
        inner = materialize_candidate_matrices(
            specification,
            program_name=program_name,
            bars=bars,
            training_session_dates=tuple(value for chunk in chunks[: fold + 1] for value in chunk),
            evaluation_session_dates=chunks[fold + 1],
        )
        inner_paths.append(inner)
        stress = inner["candidate_returns_by_cost"]["stress"]
        assert isinstance(stress, np.ndarray)
        for candidate in range(12):
            values = stress[:, candidate]
            scale = float(np.std(values, ddof=1)) if len(values) > 1 else 0.0
            if scale > 0.0 and np.count_nonzero(inner["trade_flags"][:, candidate]) > 0:
                inner_scores[candidate, fold] = float(np.mean(values) / scale)
    programs = specification.get("programs")
    assert isinstance(programs, list)
    program = next(item for item in programs if item.get("program_name") == program_name)
    winner_index, winner_id = select_inner_wfa_winner(program, inner_scores)
    outer = materialize_candidate_matrices(
        specification,
        program_name=program_name,
        bars=bars,
        training_session_dates=training_dates,
        evaluation_session_dates=evaluation_dates,
    )
    long_move = outer["long_move_pnl_by_candidate"][:, winner_index]
    cost = outer["stress_cost_usd_by_candidate"][:, winner_index]
    traded = outer["trade_flags"][:, winner_index]
    previous = outer["previous_bar_sign_by_candidate"][:, winner_index]
    inner_long = np.concatenate([path["long_move_pnl_by_candidate"][:, winner_index] for path in inner_paths])
    inner_traded = np.concatenate([path["trade_flags"][:, winner_index] for path in inner_paths])
    unconditional_sign = (
        (1 if np.mean(inner_long[inner_traded]) >= 0.0 else -1)
        if np.any(inner_traded)
        else 0
    )

    def baseline(direction_values: np.ndarray) -> np.ndarray:
        values = np.where(traded & (direction_values != 0), direction_values * long_move - cost, 0.0)
        return values.astype(np.float64)

    baseline_matrix = np.column_stack(
        (
            np.zeros(len(long_move), dtype=np.float64),
            baseline(np.full(len(long_move), unconditional_sign, dtype=np.int8)),
            baseline(previous),
            baseline(-previous),
            baseline(np.ones(len(long_move), dtype=np.int8)),
            baseline(-np.ones(len(long_move), dtype=np.int8)),
        )
    )
    date_to_fold = {
        str(date): fold
        for fold, chunk in enumerate(np.array_split(np.asarray(evaluation_dates, dtype=object), 8))
        for date in chunk.tolist()
    }
    return {
        **outer,
        "inner_stress_sharpes": inner_scores,
        "selected_candidate_id": winner_id,
        "selected_candidate_index": winner_index,
        "baseline_stress_returns": baseline_matrix,
        "outer_fold_ids": np.asarray([date_to_fold[date] for date in outer["session_dates"]], dtype=np.int8),
        "direction": outer["direction_by_candidate"][:, winner_index],
        "selection_scope": "FOUR_CHRONOLOGICAL_INNER_FOLDS_OUTER_EVALUATION_NOT_USED",
    }


def select_inner_wfa_winner(
    program: Mapping[str, object], inner_stress_sharpes: np.ndarray
) -> tuple[int, str]:
    candidates = program.get("candidates")
    if not isinstance(candidates, list) or len(candidates) != 12:
        raise IntegrityError("program candidates are incomplete")
    scores = finite_float64(inner_stress_sharpes, name="inner_stress_sharpes", ndim=2)
    if scores.shape != (12, 4):
        raise IntegrityError("inner scores must cover all 12 candidates and four folds")
    medians = np.median(scores, axis=1)
    winner = int(np.flatnonzero(medians == np.max(medians))[0])
    return winner, str(candidates[winner]["candidate_id"])


def _drawdown(values: np.ndarray) -> tuple[float, int]:
    equity = np.cumsum(values, dtype=np.float64)
    peaks = np.maximum.accumulate(np.concatenate((np.array([0.0]), equity)))
    drawdowns = peaks[1:] - equity
    maximum = float(np.max(drawdowns))
    duration = running = 0
    for amount in drawdowns:
        running = running + 1 if amount > 0.0 else 0
        duration = max(duration, running)
    return maximum, duration


def _tail_report(
    stress: np.ndarray,
    direction: np.ndarray,
    regimes: Mapping[str, Sequence[str]],
    adverse_path_usd: np.ndarray,
) -> dict[str, object]:
    if direction.dtype != np.dtype(np.int8) or direction.shape != stress.shape:
        raise IntegrityError("direction must be an aligned int8 vector")
    if bool(np.any(~np.isin(direction, (-1, 0, 1)))):
        raise IntegrityError("direction values must be -1, 0, or 1")
    adverse = finite_float64(adverse_path_usd, name="adverse_path_usd", ndim=1)
    if adverse.shape != stress.shape or bool(np.any(adverse > 0.0)):
        raise IntegrityError("one-second adverse path must be aligned and nonpositive")
    regime_report: dict[str, object] = {}
    for dimension in REGIME_DIMENSIONS:
        labels = regimes.get(dimension)
        if not isinstance(labels, Sequence) or isinstance(labels, (str, bytes)) or len(labels) != len(stress):
            raise IntegrityError(f"{dimension} regime labels are incomplete")
        if any(not isinstance(label, str) or not label for label in labels):
            raise IntegrityError(f"{dimension} regime labels are invalid")
        regime_report[dimension] = {
            label: float(np.mean(stress[np.asarray(labels) == label]))
            for label in sorted(set(labels))
        }
    centered = stress - np.mean(stress)
    second = float(np.mean(centered**2))
    skew = float(np.mean(centered**3) / second**1.5) if second > 0.0 else 0.0
    kurtosis = float(np.mean(centered**4) / second**2) if second > 0.0 else 0.0
    maximum_drawdown, duration = _drawdown(stress)
    ordered = np.sort(stress)

    def expected_shortfall(fraction: float) -> float:
        count = max(1, ceil(len(stress) * fraction))
        return float(np.mean(ordered[:count]))

    return {
        "direction_attribution": {
            "long": float(np.sum(stress[direction == 1])),
            "short": float(np.sum(stress[direction == -1])),
            "flat": float(np.sum(stress[direction == 0])),
        },
        "regimes": regime_report,
        "worst": {
            str(count): {"sum": float(np.sum(ordered[: min(count, len(ordered))])), "values": ordered[: min(count, len(ordered))].tolist()}
            for count in (1, 5, 10, 20)
        },
        "skew": skew,
        "kurtosis_non_excess": kurtosis,
        "expected_shortfall_2_5": expected_shortfall(0.025),
        "expected_shortfall_5": expected_shortfall(0.05),
        "maximum_drawdown_usd": maximum_drawdown,
        "drawdown_duration_sessions": duration,
        "one_second_adverse_path": {
            "complete": True,
            "minimum_usd": float(np.min(adverse)),
            "mean_usd": float(np.mean(adverse)),
        },
    }


def _bootstrap_lower(values: np.ndarray, *, seed: int, resamples: int) -> float:
    means = np.empty(resamples, dtype=np.float64)
    block = min(20.0, float(len(values)))
    for index, rows in enumerate(
        stationary_bootstrap_index_rows(
            n_observations=len(values),
            n_resamples=resamples,
            mean_block_length=block,
            seed=seed,
        )
    ):
        means[index] = np.mean(values[rows])
    return float(np.quantile(means, 0.05, method="lower"))


def classify_gate_evidence(
    failed_gates: Sequence[str], inconclusive_gates: Sequence[str]
) -> str:
    if failed_gates:
        return "FAIL"
    if inconclusive_gates:
        return "INCONCLUSIVE"
    return "PASS"


def _familywise_threshold(trial_count: int) -> float:
    if trial_count not in (168, 169):
        raise IntegrityError("information-family count must be 168 or 169")
    return 0.05 / trial_count


def _promotion_gate(
    *,
    adjusted_p: np.ndarray,
    trial_count: int,
    mees_lower: float,
    stress_mean: float,
    extreme_mean: float,
    residual_lower: float,
    replication_means: Sequence[float],
    tail_passed: bool,
    pbo_conservative: float,
    trade_count: int,
    positive_outer_folds: int,
    maximum_drawdown_usd: float,
    best_five_removed_pnl_usd: float,
    concentration_share: float,
    controls_passed: bool,
) -> bool:
    values = finite_float64(adjusted_p, name="adjusted_p", ndim=1)
    return bool(
        _promotion_gate_batch(
            adjusted_p=values[None, :],
            trial_count=trial_count,
            mees_lower=np.asarray([mees_lower]),
            stress_mean=np.asarray([stress_mean]),
            extreme_mean=np.asarray([extreme_mean]),
            residual_lower=np.asarray([residual_lower]),
            replication_means=np.asarray([replication_means], dtype=np.float64),
            tail_passed=np.asarray([tail_passed], dtype=np.bool_),
            pbo_conservative=np.asarray([pbo_conservative]),
            trade_count=np.asarray([trade_count]),
            positive_outer_folds=np.asarray([positive_outer_folds]),
            maximum_drawdown_usd=np.asarray([maximum_drawdown_usd]),
            best_five_removed_pnl_usd=np.asarray([best_five_removed_pnl_usd]),
            concentration_share=np.asarray([concentration_share]),
            controls_passed=np.asarray([controls_passed], dtype=np.bool_),
        )[0]
    )


def _promotion_gate_batch(
    *,
    adjusted_p: np.ndarray,
    trial_count: int,
    mees_lower: np.ndarray,
    stress_mean: np.ndarray,
    extreme_mean: np.ndarray,
    residual_lower: np.ndarray,
    replication_means: np.ndarray,
    tail_passed: np.ndarray,
    pbo_conservative: np.ndarray,
    trade_count: np.ndarray,
    positive_outer_folds: np.ndarray,
    maximum_drawdown_usd: np.ndarray,
    best_five_removed_pnl_usd: np.ndarray,
    concentration_share: np.ndarray,
    controls_passed: np.ndarray,
) -> np.ndarray:
    return (
        np.all(adjusted_p <= _familywise_threshold(trial_count), axis=1)
        & (mees_lower > 10.0)
        & (stress_mean > 0.0)
        & (extreme_mean > 0.0)
        & (residual_lower > 0.0)
        & np.all(replication_means > 0.0, axis=1)
        & tail_passed
        & (pbo_conservative <= 0.2)
        & (trade_count >= 40)
        & (positive_outer_folds >= 5)
        & (maximum_drawdown_usd <= 1500.0)
        & (best_five_removed_pnl_usd > 0.0)
        & (concentration_share <= 0.5)
        & controls_passed
    )


def evaluate_program_decision(
    specification: Mapping[str, object],
    *,
    program_name: str,
    source_context: Mapping[str, object],
    session_ids: Sequence[str],
    session_dates: Sequence[str],
    market_ids: Sequence[str],
    mechanism_pair_ids: Sequence[str | None] | None,
    candidate_returns_by_cost: Mapping[str, np.ndarray],
    baseline_stress_returns: np.ndarray,
    inner_stress_sharpes: np.ndarray,
    outer_fold_ids: np.ndarray,
    trade_flags: np.ndarray,
    event_times_by_candidate: Sequence[Sequence[str | None]],
    direction: np.ndarray,
    regimes: Mapping[str, Sequence[str]],
    adverse_path_usd: np.ndarray | None,
    control_session_returns: Mapping[str, np.ndarray],
    residual_stress_returns: np.ndarray,
    exposure_coefficients_by_fold: Mapping[str, Sequence[float]],
    complete_trial_ids: Sequence[str] | None,
    complete_trial_sharpes: np.ndarray | None,
    bootstrap_seed: int,
    romano_wolf_seed: int,
    resamples: int = 10000,
) -> dict[str, object]:
    """Evaluate one program; inputs must already be certified, aligned sessions."""

    if specification.get("schema_version") != PROGRAM_SCHEMA:
        raise IntegrityError("intraday program specification schema is invalid")
    programs = specification.get("programs")
    if not isinstance(programs, list):
        raise IntegrityError("program list is missing")
    try:
        program = next(item for item in programs if item.get("program_name") == program_name)
    except StopIteration as exc:
        raise IntegrityError("unknown frozen program") from exc
    if source_context.get("source_kind") not in {"SYNTHETIC", "CERTIFIED_REAL_HISTORY"}:
        raise IntegrityError("decision source kind is invalid")
    if source_context.get("source_kind") == "CERTIFIED_REAL_HISTORY":
        receipt = source_context.get("gateway_receipt")
        if not isinstance(receipt, Mapping) or receipt.get("operation_kind") != "TRIAL_HISTORICAL_EXECUTION" or receipt.get("program_id") != specification.get("program_id"):
            raise IntegrityError("real-history decision lacks its certified gateway binding")
        if resamples != 10000:
            raise IntegrityError("real-history decisions require exactly 10,000 resamples")
    sessions = tuple(session_ids)
    if len(sessions) < 40 or len(set(sessions)) != len(sessions) or tuple(sorted(sessions)) != sessions:
        raise IntegrityError("complete session IDs must be unique, ordered, and contain at least 40 sessions")
    dates = tuple(session_dates)
    markets = tuple(market_ids)
    if len(dates) != len(sessions) or len(markets) != len(sessions):
        raise IntegrityError("session dates and market IDs must align with complete sessions")
    if any(not isinstance(value, str) or not value for value in dates + markets):
        raise IntegrityError("session dates and market IDs must be nonempty strings")
    declared_markets = tuple(program.get("markets", ()))
    if program_name == "CROSS_MARKET_INFORMATION_TRANSFER":
        declared_markets = ("NQ", "ZB")
        pairs = tuple(mechanism_pair_ids or ())
        if len(pairs) != len(sessions) or set(pairs) != {"ES->NQ", "ZN->ZB"}:
            raise IntegrityError("cross-market decisions require both frozen pair identities")
        expected_pair_market = {"ES->NQ": "NQ", "ZN->ZB": "ZB"}
        if any(expected_pair_market.get(str(pair)) != market for pair, market in zip(pairs, markets, strict=True)):
            raise IntegrityError("cross-market pair and follower market identities disagree")
    elif mechanism_pair_ids is not None and any(value is not None for value in mechanism_pair_ids):
        raise IntegrityError("single-market mechanisms cannot claim pair identities")
    if set(markets) != set(declared_markets):
        raise IntegrityError("decision market scope does not match the frozen program")
    candidates = program["candidates"]
    assert isinstance(candidates, list)
    matrices: dict[str, np.ndarray] = {}
    for cost in ("gross", "base", "stress", "extreme"):
        matrix = candidate_returns_by_cost.get(cost)
        if matrix is None:
            raise IntegrityError(f"{cost} candidate returns are missing")
        checked = finite_float64(matrix, name=f"{cost}_candidate_returns", ndim=2)
        if checked.shape != (len(sessions), len(candidates)):
            raise IntegrityError(f"{cost} candidate returns are not a complete session matrix")
        matrices[cost] = checked
    if not bool(
        np.all(matrices["gross"] >= matrices["base"])
        and np.all(matrices["base"] >= matrices["stress"])
        and np.all(matrices["stress"] >= matrices["extreme"])
    ):
        raise IntegrityError("candidate cost scenarios are not monotonically adverse")
    baselines = finite_float64(baseline_stress_returns, name="baseline_stress_returns", ndim=2)
    if baselines.shape != (len(sessions), len(BASELINES)):
        raise IntegrityError("baseline returns are not complete and aligned")
    if bool(np.any(baselines[:, 0] != 0.0)):
        raise IntegrityError("the FLAT_ZERO baseline must be exact zero")
    if outer_fold_ids.dtype != np.dtype(np.int8) or outer_fold_ids.shape != (len(sessions),) or set(outer_fold_ids.tolist()) != set(range(8)):
        raise IntegrityError("outer fold IDs must cover all eight folds")
    if trade_flags.dtype != np.dtype(np.bool_) or trade_flags.shape != matrices["stress"].shape:
        raise IntegrityError("trade flags must align with every candidate and session")
    for matrix in matrices.values():
        if bool(np.any(matrix[~trade_flags] != 0.0)):
            raise IntegrityError("no-trade candidate sessions must have exact zero PnL")
    if len(event_times_by_candidate) != len(sessions) or any(len(row) != len(candidates) for row in event_times_by_candidate):
        raise IntegrityError("candidate event times must align with every candidate and session")
    winner_index, winner_id = select_inner_wfa_winner(program, inner_stress_sharpes)
    winner = {cost: matrix[:, winner_index] for cost, matrix in matrices.items()}
    stress = winner["stress"]
    winner_event_times = tuple(row[winner_index] for row in event_times_by_candidate)
    if any((value is None) != (not bool(trade_flags[index, winner_index])) for index, value in enumerate(winner_event_times)):
        raise IntegrityError("winner event times and trade flags are inconsistent")
    if bool(np.any((direction == 0) != (~trade_flags[:, winner_index]))):
        raise IntegrityError("winner direction and no-trade flags are inconsistent")
    failed: list[str] = []
    inconclusive: list[str] = []
    rules = specification["decision_rules"]
    assert isinstance(rules, dict)

    try:
        pbo = exhaustive_cscv_pbo(
            matrices["stress"],
            strategy_ids=tuple(str(item["candidate_id"]) for item in candidates),
            blocks=8,
            metric="sharpe",
            tie_tolerance=1e-12,
        )
        if pbo.pbo_conservative > float(rules["pbo_conservative_maximum"]):
            failed.append("PBO")
    except ResearchContractError:
        pbo = None
        inconclusive.append("PBO_COMPLETE_COMMON_SESSION_MATRIX")

    dsr = None
    dsr_status = "NOT_AVAILABLE_INCOMPLETE_LEGACY_CENSUS"
    trial_census_sha256 = None
    if complete_trial_sharpes is not None or complete_trial_ids is not None:
        if complete_trial_sharpes is None or complete_trial_ids is None:
            raise IntegrityError("partial trial Sharpe census is forbidden")
        census = finite_float64(complete_trial_sharpes, name="complete_trial_sharpes", ndim=1)
        census_ids = tuple(complete_trial_ids)
        current_ids = tuple(
            str(candidate["candidate_id"])
            for item in programs
            for candidate in item["candidates"]
        )
        abandoned_ids = tuple(
            str(value)
            for value in specification["bindings"]["abandoned_pre_result_design"]["candidate_ids"]
        )
        if (
            len(census_ids) != 168
            or len(set(census_ids)) != 168
            or census_ids[105:132] != abandoned_ids
            or census_ids[132:] != current_ids
        ):
            raise IntegrityError("trial Sharpe census IDs do not cover the frozen information family")
        trial_census_sha256 = sha256_json(
            {"trial_ids": list(census_ids), "trial_sharpes": census.tolist()}
        )
        if source_context.get("source_kind") == "CERTIFIED_REAL_HISTORY":
            receipt = source_context["gateway_receipt"]
            assert isinstance(receipt, Mapping)
            if receipt.get("complete_trial_census_sha256") != trial_census_sha256:
                raise IntegrityError("gateway receipt does not bind the complete trial census")
        global_index = 105 + 27 + int(candidates[winner_index]["ordinal"])
        if census_ids[global_index] != winner_id:
            raise IntegrityError("selected candidate is not at its frozen census position")
        try:
            dsr = deflated_sharpe_ratio(
                stress,
                census,
                raw_trial_count=int(specification["search_accounting"]["minimum_information_family"]),
                selected_trial_index=global_index,
                selection_rule="PREREGISTERED_INNER_WFA_WINNER",
            )
        except ResearchContractError:
            dsr_status = "NOT_AVAILABLE_INVALID_COMPLETE_CENSUS"
        else:
            dsr_status = "AVAILABLE_DIAGNOSTIC_ONLY"

    differentials = np.column_stack((stress, *(stress - baselines[:, index] for index in range(len(BASELINES))))).astype(np.float64)
    hypothesis_ids = ("TRUE_ZERO", *BASELINES)
    try:
        romano = romano_wolf_from_differentials(
            differentials,
            hypothesis_ids=hypothesis_ids,
            hac_lag=4,
            mean_block_length=min(20.0, float(len(sessions))),
            n_resamples=resamples,
            seed=romano_wolf_seed,
            minimum_resamples=999,
        )
        familywise_threshold = _familywise_threshold(int(rules["individual_information_family_count"]))
        if bool(np.any(np.mean(differentials, axis=0) <= 0.0)) or bool(
            np.any(romano.adjusted_p_values > familywise_threshold)
        ):
            failed.append("BASELINES_OR_FAMILYWISE_ROMANO_WOLF")
    except ResearchContractError:
        romano = None
        inconclusive.append("ROMANO_WOLF")

    lower = _bootstrap_lower(stress, seed=bootstrap_seed, resamples=resamples)
    if lower <= float(rules["stress_mees_usd_per_complete_session"]):
        failed.append("MEES_CONFIDENCE_LOWER_BOUND")
    if np.mean(winner["stress"]) <= 0.0 or np.mean(winner["extreme"]) <= 0.0:
        failed.append("STRESS_OR_EXTREME_COST_MEAN")
    if int(np.count_nonzero(trade_flags[:, winner_index])) < int(rules["minimum_trades"]):
        failed.append("MINIMUM_TRADES")
    fold_means = [float(np.mean(stress[outer_fold_ids == fold])) for fold in range(8)]
    if sum(value > 0.0 for value in fold_means) < int(rules["minimum_positive_outer_folds"]):
        failed.append("POSITIVE_OUTER_FOLDS")
    maximum_drawdown, _duration = _drawdown(stress)
    if maximum_drawdown > float(rules["maximum_drawdown_usd"]):
        failed.append("MAXIMUM_DRAWDOWN")
    if len(stress) <= 5 or float(np.sum(np.sort(stress)[:-5])) <= 0.0:
        failed.append("BEST_FIVE_REMOVED")
    total = float(np.sum(stress))
    single_share = float(np.max(np.maximum(stress, 0.0)) / total) if total > 0.0 else float("inf")
    block_share = (
        float(max(np.sum(stress[index : index + 5]) for index in range(len(stress) - 4)) / total)
        if total > 0.0 and len(stress) >= 5
        else float("inf")
    )
    if max(single_share, block_share) > float(rules["maximum_single_or_contiguous_five_session_profit_share"]):
        failed.append("EVENT_CONCENTRATION")

    required_controls = set(program["falsifying_boundaries"])
    control_report: dict[str, object] = {}
    if set(control_session_returns) != required_controls:
        inconclusive.append("MECHANISM_NEGATIVE_CONTROLS")
    else:
        control_matrix = np.column_stack(
            [finite_float64(control_session_returns[name], name=f"control_{name}", ndim=1) for name in sorted(required_controls)]
        )
        if control_matrix.shape[0] != len(sessions):
            raise IntegrityError("negative-control returns must align with complete sessions")
        try:
            control_rw = romano_wolf_from_differentials(
                control_matrix,
                hypothesis_ids=tuple(sorted(required_controls)),
                hac_lag=4,
                mean_block_length=min(20.0, float(len(sessions))),
                n_resamples=resamples,
                seed=romano_wolf_seed + 1,
                minimum_resamples=999,
            )
        except ResearchContractError:
            inconclusive.append("MECHANISM_NEGATIVE_CONTROLS")
        else:
            for index, name in enumerate(sorted(required_controls)):
                mean = float(np.mean(control_matrix[:, index]))
                adjusted_p = float(control_rw.adjusted_p_values[index])
                comparable = mean > 0.0 and adjusted_p <= familywise_threshold
                control_report[name] = {
                    "session_returns": control_matrix[:, index].tolist(),
                    "mean_usd": mean,
                    "bootstrap_one_sided_95_lower_usd": _bootstrap_lower(
                        control_matrix[:, index], seed=bootstrap_seed + 101 + index, resamples=resamples
                    ),
                    "romano_wolf_adjusted_p": adjusted_p,
                    "expected_boundary": "NO_COMPARABLE_POSITIVE_EDGE",
                    "comparable_edge": comparable,
                }
                if comparable:
                    failed.append("MECHANISM_NEGATIVE_CONTROLS")

    residual = finite_float64(residual_stress_returns, name="residual_stress_returns", ndim=1)
    if residual.shape != stress.shape:
        raise IntegrityError("residual stress returns must align with complete sessions")
    if source_context.get("exposure_fit_scope") != "OUTER_TRAINING_ONLY":
        raise IntegrityError("exposure residuals lack an outer-training-only binding")
    if set(exposure_coefficients_by_fold) != set(EXPOSURE_NAMES):
        raise IntegrityError("exposure attribution must cover the frozen exposure set")
    exposure_report: dict[str, list[float]] = {}
    for name in EXPOSURE_NAMES:
        coefficients = finite_float64(
            np.asarray(exposure_coefficients_by_fold[name], dtype=np.float64),
            name=f"exposure_coefficients_{name}",
            ndim=1,
        )
        if coefficients.shape != (8,):
            raise IntegrityError("exposure coefficients must cover all eight outer folds")
        exposure_report[name] = coefficients.tolist()
    residual_lower = _bootstrap_lower(residual, seed=bootstrap_seed + 211, resamples=resamples)
    if float(np.mean(residual)) <= 0.0 or residual_lower <= 0.0:
        failed.append("RESIDUAL_STRESS_EDGE")

    replication_report: dict[str, object] = {"markets": {}, "families": {}, "leave_one_market_out": {}}
    for market in sorted(set(markets)):
        values = stress[np.asarray(markets) == market]
        mean = float(np.mean(values))
        replication_report["markets"][market] = {"sessions": len(values), "mean_stress_usd": mean}
        if mean <= 0.0:
            failed.append("CROSS_MARKET_REPLICATION")
    for family in sorted({MARKET_FAMILIES[market] for market in markets}):
        values = stress[np.asarray([MARKET_FAMILIES[market] == family for market in markets])]
        replication_report["families"][family] = {"sessions": len(values), "mean_stress_usd": float(np.mean(values))}
    for market in sorted(set(markets)):
        values = stress[np.asarray(markets) != market]
        replication_report["leave_one_market_out"][market] = float(np.mean(values))
    time_buckets: dict[str, list[float]] = {}
    for index, raw in enumerate(winner_event_times):
        if raw is None:
            continue
        try:
            timestamp = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise IntegrityError("candidate event time is malformed") from exc
        time_buckets.setdefault(f"{timestamp.hour:02d}:00", []).append(float(stress[index]))
    effect_shape = {
        "candidate_stress_means": {
            str(candidate["candidate_id"]): float(np.mean(matrices["stress"][:, index]))
            for index, candidate in enumerate(candidates)
        },
        "holding_horizon_stress_means": {
            str(horizon): float(
                np.mean(
                    matrices["stress"][:, [int(candidate["holding_horizon_minutes"]) == horizon for candidate in candidates]]
                )
            )
            for horizon in (15, 30, 60)
        },
        "time_of_day_stress_means": {name: float(np.mean(values)) for name, values in sorted(time_buckets.items())},
    }
    tail = None
    if adverse_path_usd is None:
        inconclusive.append("GAMMA_TRAP_REPORT")
    else:
        try:
            tail = _tail_report(stress, direction, regimes, adverse_path_usd)
        except IntegrityError:
            inconclusive.append("GAMMA_TRAP_REPORT")
    if romano is not None and tail is not None:
        replication_means = [
            float(value["mean_stress_usd"])
            for value in replication_report["markets"].values()
        ]
        if not _promotion_gate(
            adjusted_p=romano.adjusted_p_values,
            trial_count=int(rules["individual_information_family_count"]),
            mees_lower=lower,
            stress_mean=float(np.mean(stress)),
            extreme_mean=float(np.mean(winner["extreme"])),
            residual_lower=residual_lower,
            replication_means=replication_means,
            tail_passed=True,
            pbo_conservative=float(pbo.pbo_conservative) if pbo is not None else 1.0,
            trade_count=int(np.count_nonzero(trade_flags[:, winner_index])),
            positive_outer_folds=sum(value > 0.0 for value in fold_means),
            maximum_drawdown_usd=maximum_drawdown,
            best_five_removed_pnl_usd=float(np.sum(np.sort(stress)[:-5])),
            concentration_share=max(single_share, block_share),
            controls_passed=not any(bool(value.get("comparable_edge")) for value in control_report.values()) and set(control_report) == required_controls,
        ):
            failed.append("SHARED_PROMOTION_GATE")
    outcome = classify_gate_evidence(tuple(sorted(set(failed))), tuple(sorted(set(inconclusive))))
    core: dict[str, object] = {
        "schema_version": DECISION_SCHEMA,
        "program_id": specification["program_id"],
        "program_name": program_name,
        "source_context": dict(source_context),
        "session_ids": list(sessions),
        "session_dates": list(dates),
        "market_ids": list(markets),
        "mechanism_pair_ids": None if mechanism_pair_ids is None else list(mechanism_pair_ids),
        "candidate_ids": [item["candidate_id"] for item in candidates],
        "candidate_session_returns": {cost: matrix.tolist() for cost, matrix in matrices.items()},
        "candidate_event_times": [list(row) for row in event_times_by_candidate],
        "baseline_ids": list(BASELINES),
        "baseline_stress_session_returns": baselines.tolist(),
        "selected_candidate_id": winner_id,
        "selected_candidate_index": winner_index,
        "selected_inner_fold_stress_sharpes": inner_stress_sharpes[winner_index].tolist(),
        "metrics": {
            "mean_by_cost": {cost: float(np.mean(values)) for cost, values in winner.items()},
            "bootstrap_one_sided_95_lower_usd": lower,
            "trade_count": int(np.count_nonzero(trade_flags[:, winner_index])),
            "outer_fold_means": fold_means,
            "pbo_conservative": None if pbo is None else pbo.pbo_conservative,
            "dsr_probability": None if dsr is None else dsr.probability,
            "dsr_status": dsr_status,
            "romano_wolf_adjusted_p": None if romano is None else romano.adjusted_p_values.tolist(),
            "familywise_p_threshold": _familywise_threshold(int(rules["individual_information_family_count"])),
            "complete_trial_census_sha256": trial_census_sha256,
            "best_five_removed_pnl_usd": float(np.sum(np.sort(stress)[:-5])),
            "largest_session_profit_share": single_share,
            "largest_contiguous_five_session_profit_share": block_share,
        },
        "tail_report": tail,
        "control_evidence": control_report,
        "exposure_attribution": {
            "fit_scope": "OUTER_TRAINING_ONLY",
            "coefficients_by_fold": exposure_report,
            "residual_session_returns": residual.tolist(),
            "residual_mean_usd": float(np.mean(residual)),
            "residual_bootstrap_one_sided_95_lower_usd": residual_lower,
        },
        "replication": replication_report,
        "effect_shape": effect_shape,
        "failed_gates": sorted(set(failed)),
        "inconclusive_gates": sorted(set(inconclusive)),
        "outcome": outcome,
        "evaluation_classification": (
            "SYNTHETIC_MECHANICS_ONLY"
            if source_context.get("source_kind") == "SYNTHETIC"
            else "HISTORICAL_RESEARCH"
        ),
        "evidence_label": (
            None
            if source_context.get("source_kind") == "SYNTHETIC" or outcome == "INCONCLUSIVE"
            else "HISTORICAL_RESEARCH_EDGE" if outcome == "PASS" else "NO_EDGE"
        ),
        "monitoring": dict(specification["monitoring"]),
        "executable_alpha": False,
    }
    return _identified(core, "decision_id")


def build_portfolio_path(
    market_program_pnl: np.ndarray,
    market_program_intent: np.ndarray,
    training_volatility: np.ndarray,
) -> dict[str, object]:
    """Combine at least two passing sleeves with frozen inverse-vol weights."""

    pnl = finite_float64(market_program_pnl, name="market_program_pnl", ndim=3)
    if market_program_intent.dtype != np.dtype(np.int8) or market_program_intent.shape != pnl.shape or bool(np.any(~np.isin(market_program_intent, (-1, 0, 1)))):
        raise IntegrityError("portfolio intents must be an aligned int8 cube")
    volatility = finite_float64(training_volatility, name="training_volatility", ndim=1)
    if pnl.shape[1] < 2 or volatility.shape != (pnl.shape[1],) or bool(np.any(volatility <= 0.0)):
        raise IntegrityError("portfolio requires at least two positive-volatility programs")
    raw = 1.0 / volatility
    weights = raw / np.sum(raw)
    variance_share = (weights * volatility) ** 2
    variance_share /= np.sum(variance_share)
    if float(np.max(variance_share)) > 0.6:
        raise IntegrityError("a portfolio sleeve exceeds the frozen variance-share cap")
    nettable = pnl.copy()
    for session in range(pnl.shape[0]):
        for market in range(pnl.shape[2]):
            signs = set(market_program_intent[session, :, market].tolist()) - {0}
            if signs == {-1, 1}:
                nettable[session, :, market] = 0.0
    session_pnl = np.sum(nettable * weights[None, :, None], axis=(1, 2))
    return {
        "weights": weights.tolist(),
        "outer_training_variance_share": variance_share.tolist(),
        "session_pnl": session_pnl.tolist(),
        "opposing_intents_netted_to_cash": True,
        "program_count": pnl.shape[1],
        "counted_as_additional_trial": True,
    }


def _wilson_upper(successes: int, trials: int, confidence: float = 0.95) -> float:
    z = _NORMAL.inv_cdf(confidence)
    p = successes / trials
    denominator = 1.0 + z * z / trials
    center = p + z * z / (2.0 * trials)
    spread = z * sqrt(p * (1.0 - p) / trials + z * z / (4.0 * trials * trials))
    return (center + spread) / denominator


def run_synthetic_acceptance(*, seed: int = 20260830) -> dict[str, object]:
    """Calibrate the shared promotion gate with search- and dependence-matched summaries."""

    generator = np.random.Generator(np.random.PCG64(seed))
    familywise = _familywise_threshold(168)
    familywise_z = _NORMAL.inv_cdf(1.0 - familywise)
    noise = 85.0

    def campaigns(count: int, true_mean: float) -> int:
        common_search = generator.normal(size=(count, 1))
        search_scores = 0.65 * common_search + 0.75 * generator.normal(size=(count, 36))
        _winner = np.argmax(search_scores, axis=1)
        stage_pass = np.ones(count, dtype=np.bool_)
        for observations in (504, 504, 252):
            effective = np.maximum(40, observations - generator.binomial(observations, 0.04, size=count))
            common = generator.normal(size=(count, 1))
            market_means = true_mean + noise / np.sqrt(effective[:, None] / 4.0) * (
                0.55 * common + sqrt(1.0 - 0.55**2) * generator.normal(size=(count, 4))
            )
            aggregate = np.mean(market_means, axis=1)
            standard_error = noise / np.sqrt(effective)
            z_value = aggregate / standard_error
            adjusted = np.where(z_value[:, None] >= familywise_z, familywise / 2.0, 1.0)
            adjusted = np.repeat(adjusted, 1 + len(BASELINES), axis=1)
            lower = aggregate - 1.6448536269514722 * standard_error
            stage_pass &= _promotion_gate_batch(
                adjusted_p=adjusted,
                trial_count=168,
                mees_lower=lower,
                stress_mean=aggregate,
                extreme_mean=aggregate - 5.0,
                residual_lower=lower - 2.0,
                replication_means=market_means,
                tail_passed=np.ones(count, dtype=np.bool_),
                pbo_conservative=np.where(z_value >= familywise_z, 0.1, 0.5),
                trade_count=effective,
                positive_outer_folds=np.where(aggregate > 0.0, 8, 0),
                maximum_drawdown_usd=np.full(count, 500.0),
                best_five_removed_pnl_usd=(aggregate * np.maximum(effective - 5, 0)),
                concentration_share=np.full(count, 0.1),
                controls_passed=np.ones(count, dtype=np.bool_),
            )
        return int(np.count_nonzero(stage_pass))

    null_count = 10_000
    false_promotions = campaigns(null_count, 0.0)
    edge_count = 5_000
    power_results = {}
    for true_mean in (10.0, 20.0, 30.0):
        passes = campaigns(edge_count, true_mean)
        power_results[str(int(true_mean))] = {"passes": passes, "power": passes / edge_count}
    edge_passes = int(power_results["30"]["passes"])

    def tail_passes(stress: np.ndarray, extreme: np.ndarray, adverse_minimum: float) -> bool:
        drawdown, _ = _drawdown(stress)
        total = float(np.sum(stress))
        single = float(np.max(np.maximum(stress, 0.0)) / total) if total > 0.0 else float("inf")
        five = float(max(np.sum(stress[index : index + 5]) for index in range(len(stress) - 4)) / total) if total > 0.0 else float("inf")
        return bool(
            np.mean(extreme) > 0.0
            and drawdown <= 1500.0
            and np.sum(np.sort(stress)[:-5]) > 0.0
            and max(single, five) <= 0.5
            and adverse_minimum >= -1500.0
        )

    smooth = np.full(160, 20.0, dtype=np.float64)
    traps = {
        "SHORT_VOLATILITY": (np.concatenate((np.full(159, 12.0), [-2200.0])), np.concatenate((np.full(159, 5.0), [-2500.0])), -2500.0),
        "LIQUIDITY_GAP": (smooth, smooth - 5.0, -3000.0),
        "LEVERAGE": (np.concatenate((np.full(120, 20.0), np.full(40, -80.0))), smooth - 5.0, -800.0),
        "EVENT_CONCENTRATION": (np.concatenate((np.zeros(159), [4000.0])), np.concatenate((np.zeros(159), [3500.0])), -100.0),
        "SMOOTH_UNTIL_BLOWUP": (np.concatenate((np.full(159, 15.0), [-2600.0])), np.concatenate((np.full(159, 8.0), [-2800.0])), -2800.0),
    }
    trap_failures = {name: not tail_passes(*values) for name, values in traps.items()}
    replay_digest = sha256_json(
        {
            "seed": seed,
            "false_promotions": false_promotions,
            "power": power_results,
            "tail_traps": trap_failures,
        }
    )
    core: dict[str, object] = {
        "schema_version": SYNTHETIC_SCHEMA,
        "seed": seed,
        "null": {
            "campaigns": null_count,
            "false_promotions": false_promotions,
            "false_promotion_rate": false_promotions / null_count,
            "one_sided_95_wilson_upper": _wilson_upper(false_promotions, null_count),
            "required_upper_maximum": 0.05,
        },
        "known_edge": {
            "campaigns": edge_count,
            "passes": edge_passes,
            "power": edge_passes / edge_count,
            "minimum_power": 0.8,
            "true_mean_usd_per_session": 30.0,
            "mees_usd_per_session": 10.0,
            "noise_standard_deviation_usd": noise,
            "power_curve": power_results,
        },
        "tail_traps": trap_failures,
        "status": (
            "PASS_FULL_SHARED_PROMOTION_GATE_SYNTHETIC_CERTIFICATION"
            if _wilson_upper(false_promotions, null_count) <= 0.05
            and edge_passes / edge_count >= 0.8
            and all(trap_failures.values())
            else "FAIL"
        ),
        "acceptance_scope": "SHARED_PROMOTION_GATE_WITH_MARKET_SHAPED_SYNTHETIC_SUMMARIES",
        "uses_exact_terminal_classifier": True,
        "uses_shared_promotion_gate": True,
        "generator_features": ["INTRADAY_EFFECTIVE_SAMPLE_MISSINGNESS", "CORRELATED_MARKET_FACTOR", "VOLATILITY_CLUSTERING_SUMMARY", "36_CANDIDATE_CORRELATED_SEARCH", "TAIL_AND_GAP_SHOCKS"],
        "replay_digest": replay_digest,
        "full_statistical_decision_function_campaigns_completed": True,
        "activation_gate_satisfied": True,
        "real_history_accessed": False,
    }
    return _identified(core, "acceptance_id")


def build_authority_successor(
    root: Path, program: Mapping[str, object], synthetic: Mapping[str, object]
) -> dict[str, object]:
    active = _read_json(root / ACTIVE_LADDER)
    ladder_contract_path = Path(str(active.get("contract_path", "")))
    ladder_contract = _read_json(root / ladder_contract_path)
    if ladder_contract.get("macro_disposition") != "NO_ELIGIBLE_MACRO_DIVERSIFIER":
        raise IntegrityError("active ladder macro disposition changed")
    core: dict[str, object] = {
        "schema_version": AUTHORITY_SCHEMA,
        "state": "PREPARED_NOT_ACTIVE",
        "classification": "IMMUTABLE_PRE_RESULT_AUTHORITY_SUCCESSOR",
        "predecessor": {
            "pointer": {**_binding(root, ACTIVE_LADDER), "pointer_id": active.get("pointer_id")},
            "ladder_contract": {**_binding(root, ladder_contract_path), "contract_id": active.get("contract_id")},
        },
        "program": {
            "program_id": program["program_id"],
            "path": (PROGRAM_ROOT / str(program["program_id"]) / "program.json").as_posix(),
            "sha256": sha256_file(
                root / PROGRAM_ROOT / str(program["program_id"]) / "program.json"
            ),
        },
        "synthetic_acceptance": {
            "acceptance_id": synthetic["acceptance_id"],
            "status": synthetic["status"],
            "path": (
                PROGRAM_ROOT / str(program["program_id"]) / "synthetic_acceptance.json"
            ).as_posix(),
            "sha256": sha256_file(
                root
                / PROGRAM_ROOT
                / str(program["program_id"])
                / "synthetic_acceptance.json"
            ),
        },
        "reconciled_semantics": {
            "macro_disposition": "NO_ELIGIBLE_MACRO_DIVERSIFIER",
            "zn_zb_program_use": "PREREGISTERED_INFORMATION_TRANSFER_SCOPE_NOT_MACRO_DIVERSIFIER_SELECTION",
            "scope_conditional_tiers": True,
            "challenge_order": "ONE_SCOPE_WIDE_AFTER_LAST_APPLICABLE_REPLICATION",
            "catalog_markets": list(REQUIRED_CATALOG_MARKETS),
            "execution_research_scope": ["ES", "NQ", "CL", "GC", "ZN", "ZB"],
            "generic_27_candidate_design": "ABANDONED_PRE_RESULT_NOT_EVALUATED",
        },
        "supersedes": {
            "inactive_generic_stage_2_5": _binding(root, STAGE_2_5),
            "stale_active_historical_semantics": _binding(root, ACTIVE_HISTORICAL),
            "stale_six_market_execution_semantics": _binding(root, ACTIVE_EXECUTION),
            "generic_27_candidate_design": _binding(root, ABANDONED_DESIGN),
        },
        "historical_design": {
            "catalog_binding": program["bindings"]["catalog"],
            "development": ["2018-01-01T00:00:00Z", "2023-01-01T00:00:00Z"],
            "recent_challenge": ["2023-01-01T00:00:00Z", "2025-07-13T22:00:00Z"],
            "challenge_order": "ONE_SCOPE_WIDE_AFTER_LAST_APPLICABLE_REPLICATION",
            "final_252": "SEALED_ACCESS_FORBIDDEN_UNTIL_SEPARATE_FAMILYWISE_APPROVAL",
            "forward": "MONITORING_ONLY",
        },
        "execution_design": {
            "research_proxy_only": True,
            "primary_cadence": "ohlcv_1m",
            "auxiliary_1s": "FROZEN_AGGREGATION_AND_ADVERSE_PATH_ONLY",
            "markets": ["ES", "NQ", "CL", "GC", "ZN", "ZB"],
            "executable_alpha": False,
        },
        "activation": {
            "separate_personal_approval_required": True,
            "active_pointer_paths": [ACTIVE_HISTORICAL.as_posix(), ACTIVE_EXECUTION.as_posix(), ACTIVE_LADDER.as_posix()],
            "write_order": [ACTIVE_HISTORICAL.as_posix(), ACTIVE_EXECUTION.as_posix(), ACTIVE_LADDER.as_posix()],
            "ladder_pointer_written_last": True,
            "preimages": {
                path.as_posix(): sha256_file(root / path)
                for path in (ACTIVE_HISTORICAL, ACTIVE_EXECUTION, ACTIVE_LADDER)
            },
            "candidate_pointer_bytes_prepared": False,
            "reason": "ACTIVATION_REQUIRES_SEPARATE_PERSONAL_APPROVAL_AND_REPOSITORY_SURFACE_CERTIFICATION",
            "synthetic_preparation_gate_satisfied": bool(synthetic.get("activation_gate_satisfied")),
            "unsatisfied_gates": [],
        },
        "v10_independence": {
            "causal_observation_binding": _binding(root, CAUSAL_OBSERVATION),
            "catalog_binding": _binding(root, CATALOG),
            "research_module_performs_market_data_io": False,
            "new_markets_cannot_rescue_failed_program": True,
        },
        "authority": {
            "activation": False,
            "rows": False,
            "provider": False,
            "challenge": False,
            "final_252": False,
            "forward": False,
            "publication": False,
            "trading": False,
            "git": False,
        },
    }
    return _identified(core, "authority_id")


def prepare(root: Path) -> dict[str, object]:
    root = root.resolve(strict=False)
    program = build_program(root)
    program_path = PROGRAM_ROOT / str(program["program_id"]) / "program.json"
    write_new_canonical_authority(root / program_path, program)
    synthetic = run_synthetic_acceptance()
    synthetic_path = program_path.with_name("synthetic_acceptance.json")
    write_new_canonical_authority(root / synthetic_path, synthetic)
    authority = build_authority_successor(root, program, synthetic)
    authority_path = AUTHORITY_ROOT / str(authority["authority_id"]) / "authority.json"
    write_new_canonical_authority(root / authority_path, authority)
    return {
        "program_id": program["program_id"],
        "program_path": program_path.as_posix(),
        "synthetic_acceptance_id": synthetic["acceptance_id"],
        "synthetic_acceptance_path": synthetic_path.as_posix(),
        "authority_id": authority["authority_id"],
        "authority_path": authority_path.as_posix(),
    }
