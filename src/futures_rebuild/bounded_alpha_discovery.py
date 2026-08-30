"""Pre-result freeze for one bounded, automated Alpha discovery mechanism."""

from __future__ import annotations

import json
import math
import os
from itertools import product
from pathlib import Path
from statistics import median
from typing import Mapping, Sequence

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError


CONTRACT_PATH = Path("configs/bounded_alpha_discovery_contract_v1.json")
CERTIFICATE_ROOT = Path("state/preexecution_certificates/bounded_alpha_discovery")
CATALOG_PATH = Path("data/active/catalog.json")
HISTORICAL_DESIGN_PATH = Path("configs/active_alpha_historical_research_design.json")
PREDECESSOR_LADDER_PATH = Path("configs/active_alpha_research_ladder_macro_successor.json")
MACRO_CLOSURE_PATH = Path(
    "state/alpha_ladder_macro_successor_terminal_closure/"
    "7ff6ded9e3cbb45b17cb127783f3065f4e9f6f80ed38c3c623b1c15319c35cab/closure.json"
)
ACTIVE_MACRO_PATH = Path("data/active/alpha_ladder_macro_selection.json")
MARKETS = ("ES", "NQ", "CL", "GC")
FEATURE_PACKS = ("TREND", "TREND_VOLATILITY", "TREND_VOLATILITY_VOLUME")
HORIZONS = (30, 60, 120)
RIDGE_PENALTIES = ("0.1", "1.0", "10.0")


def _read(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"bounded discovery input is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"bounded discovery input must be an object: {path}")
    return value


def enumerate_candidates() -> list[dict[str, object]]:
    """Return the complete deterministic 27-candidate search space."""

    candidates = []
    for ordinal, (feature_pack, horizon, penalty) in enumerate(
        product(FEATURE_PACKS, HORIZONS, RIDGE_PENALTIES)
    ):
        core = {
            "ordinal": ordinal,
            "feature_pack": feature_pack,
            "holding_horizon_minutes": horizon,
            "model_family": "MARKET_SPECIFIC_TWO_TARGET_RIDGE",
            "ridge_penalty": penalty,
        }
        candidates.append({**core, "candidate_id": sha256_json(core)})
    return candidates


def build_contract(root: Path) -> dict[str, object]:
    """Build the exact preparation-only contract without opening market rows."""

    root = root.resolve(strict=False)
    catalog = _read(root / CATALOG_PATH)
    design = _read(root / HISTORICAL_DESIGN_PATH)
    ladder = _read(root / PREDECESSOR_LADDER_PATH)
    closure = _read(root / MACRO_CLOSURE_PATH)
    entries = catalog.get("entries")
    if not isinstance(entries, list) or len(entries) != 144:
        raise IntegrityError("bounded discovery requires the certified 144-entry catalog")
    eligible = {
        (entry.get("market"), entry.get("year"))
        for entry in entries
        if isinstance(entry, dict) and entry.get("selection_eligible") is True
    }
    required = {(market, year) for market in MARKETS for year in range(2018, 2023)}
    if not required.issubset(eligible):
        raise IntegrityError("bounded discovery selection rows are not all catalog-eligible")
    if (
        closure.get("outcome") != "NO_ELIGIBLE_MACRO_DIVERSIFIER"
        or closure.get("retry_allowed") is not False
        or closure.get("selected_market") is not None
        or (root / ACTIVE_MACRO_PATH).exists()
    ):
        raise IntegrityError("bounded discovery requires the terminal no-macro closure")
    candidates = enumerate_candidates()
    core: dict[str, object] = {
        "schema_version": "bounded_alpha_discovery_contract/1.0.0",
        "state": "PRE_RESULT_PREPARATION_ONLY_NOT_REGISTERED",
        "classification": "NEW_COUNTED_MECHANISM_RESTARTING_AT_SYNTHETIC_ENGINEERING",
        "hypothesis": (
            "A bounded search over causal intraday trend, volatility, and volume state "
            "can select on ES one stable after-cost predictor that replicates unchanged "
            "to NQ, CL, and GC."
        ),
        "bindings": {
            "catalog": {
                "path": CATALOG_PATH.as_posix(),
                "sha256": sha256_file(root / CATALOG_PATH),
                "plan_id": catalog.get("plan_id"),
                "entry_count": len(entries),
            },
            "historical_design": {
                "path": HISTORICAL_DESIGN_PATH.as_posix(),
                "sha256": sha256_file(root / HISTORICAL_DESIGN_PATH),
                "pointer_id": design.get("pointer_id"),
            },
            "terminal_macro_closure": {
                "path": MACRO_CLOSURE_PATH.as_posix(),
                "sha256": sha256_file(root / MACRO_CLOSURE_PATH),
                "closure_id": closure.get("closure_id"),
                "outcome": closure.get("outcome"),
            },
            "predecessor_ladder": {
                "path": PREDECESSOR_LADDER_PATH.as_posix(),
                "sha256": sha256_file(root / PREDECESSOR_LADDER_PATH),
                "pointer_id": ladder.get("pointer_id"),
                "role": "IMMUTABLE_PREDECESSOR_NOT_EXECUTION_AUTHORITY",
            },
        },
        "market_progression": {
            "tier_0_discovery_market": "ES",
            "tier_1_replication_markets": list(MARKETS),
            "tier_1_incremental_markets": ["NQ", "CL", "GC"],
            "required_independent_anchors": ["ES", "CL", "GC"],
            "nq_is_correlated_extension": True,
            "nq_cannot_rescue_anchor_failure": True,
            "macro_diversification_claim": False,
            "macro_candidates_excluded_because": "TERMINAL_EXECUTION_INELIGIBILITY_NOT_ALPHA_RESULTS",
        },
        "decision_and_execution": {
            "checkpoint": "10:00:05_AMERICA_CHICAGO",
            "one_ranked_intent_per_session": True,
            "entry": "FIRST_LATER_CAUSAL_REPORTED_BAR_OPEN_WITHIN_120_SECONDS",
            "exit": "FIRST_CAUSAL_REPORTED_BAR_AT_OR_AFTER_ENTRY_PLUS_FROZEN_HORIZON_WITHIN_120_SECONDS",
            "same_actual_contract_identity_required": True,
            "missing_stale_ambiguous_or_nonfinite": "EXPLICIT_ABSTENTION",
        },
        "features": {
            "availability": "EVENT_AND_AVAILABLE_TIMES_NOT_LATER_THAN_DECISION_TIME",
            "training_only_standardization": "PER_MARKET_PER_OUTER_FOLD_MEAN_AND_POPULATION_STD",
            "definitions": {
                "log_return_1": "LN(CLOSE_T/CLOSE_T_MINUS_1)",
                "log_return_5": "LN(CLOSE_T/CLOSE_T_MINUS_5)",
                "log_return_10": "LN(CLOSE_T/CLOSE_T_MINUS_10)",
                "intrabar_range_fraction": "(HIGH_T-LOW_T)/CLOSE_T_MINUS_1",
                "atr_10_fraction": "MEAN_LAST_10_TRUE_RANGES/CLOSE_T_MINUS_1",
                "realized_volatility_10": "POPULATION_STD_LAST_10_LOG_RETURN_1",
                "log1p_volume": "LN(1+NONNEGATIVE_VOLUME_T)",
                "volume_zscore_20": "(VOLUME_T-MEAN_LAST_20)/POPULATION_STD_LAST_20",
            },
            "packs": {
                "TREND": ["log_return_1", "log_return_5", "log_return_10"],
                "TREND_VOLATILITY": [
                    "log_return_1", "log_return_5", "log_return_10",
                    "intrabar_range_fraction", "atr_10_fraction", "realized_volatility_10",
                ],
                "TREND_VOLATILITY_VOLUME": [
                    "log_return_1", "log_return_5", "log_return_10",
                    "intrabar_range_fraction", "atr_10_fraction", "realized_volatility_10",
                    "log1p_volume", "volume_zscore_20",
                ],
            },
        },
        "search": {
            "candidate_budget": len(candidates),
            "candidates": candidates,
            "adaptive_candidate_creation": False,
            "candidate_retry": False,
            "selection_market": "ES_ONLY",
            "selection_rule": "HIGHEST_MEDIAN_INNER_FOLD_STRESS_NET_DAILY_SHARPE_THEN_CANDIDATE_ORDER",
            "winner_count_maximum": 1,
        },
        "folds": {
            "selection_start_inclusive": "2018-01-01T00:00:00Z",
            "selection_end_exclusive": "2023-01-01T00:00:00Z",
            "outer": {"count": 8, "initial_training_sessions": 504, "evaluation_sessions": 63},
            "inner_per_outer_training": {
                "count": 3, "minimum_training_sessions": 252, "evaluation_sessions": 63,
            },
            "purge_minutes": 120,
            "embargo_sessions": 1,
            "chronological_expanding_only": True,
            "outer_results_may_select_candidate": False,
        },
        "costs": {
            "round_trip_fee_usd": "10.00",
            "promotion_scenario": "stress",
            "round_trip_adverse_ticks": {
                "base": {"ES": 2, "NQ": 2, "CL": 4, "GC": 4},
                "stress": {"ES": 4, "NQ": 4, "CL": 8, "GC": 8},
                "extreme": {"ES": 8, "NQ": 8, "CL": 16, "GC": 16},
            },
            "may_be_reduced_after_results": False,
        },
        "baselines": [
            "flat_no_trade", "fold_local_unconditional_direction",
            "previous_reported_bar_sign_momentum", "previous_reported_bar_sign_reversal",
            "risk_matched_always_long", "risk_matched_always_short",
        ],
        "multiplicity": {
            "method": "CENTERED_SESSION_BLOCK_BOOTSTRAP_MAX_CANDIDATE_STATISTIC",
            "resamples": 5000,
            "session_block_length": 20,
            "seed": 106,
            "family_size": len(candidates),
            "tier_0_adjusted_p_maximum": "0.10",
            "tier_1_adjusted_p_maximum": "0.05",
        },
        "promotion": {
            "common": [
                "STRESS_NET_PNL_POSITIVE", "BEAT_ZERO_AND_EVERY_MANDATORY_BASELINE",
                "COMPLETE_COVERAGE_LINEAGE_AND_AUTHORITY", "MAXIMUM_DRAWDOWN_USD_NOT_OVER_1500",
            ],
            "tier_0_es": {
                "minimum_trades": 40, "positive_outer_folds_required": 5,
                "outer_fold_count": 8, "multiplicity_adjusted": True,
                "failure_action": "CLOSE_MECHANISM_NO_RETRY",
            },
            "tier_1": {
                "minimum_trades": 120, "positive_markets_required": 3,
                "market_count": 4, "required_positive_markets": ["ES", "CL", "GC"],
                "positive_years_required": 3, "year_count": 5,
                "positive_outer_folds_required": 5, "outer_fold_count": 8,
                "same_candidate_and_parameters_all_markets": True,
                "failure_action": "CLOSE_MECHANISM_NO_RETRY",
            },
        },
        "recent_regime_challenge": {
            "start_inclusive": "2023-01-01T00:00:00Z",
            "end_exclusive": "2025-07-13T22:00:00Z",
            "maximum_attempts": 1,
            "tuning_design_change_or_retry_after_result": False,
            "same_frozen_candidate_required": True,
            "required_positive_markets": ["ES", "CL", "GC"],
        },
        "final_252": {"access_allowed": False, "status": "INACCESSIBLE_UNRESOLVED_AUTHORITY"},
        "gateway": {
            "registration_and_claim": "CertifiedResearchGateway",
            "historical_operation": "EXECUTE_CERTIFIED_TRIAL_HISTORICAL_SCREEN",
            "readiness_certificate_required": True,
            "single_use_receipt_required": True,
        },
        "terminal_outcomes": ["ONE_WINNER_FROZEN", "NO_ELIGIBLE_ALPHA_CANDIDATE"],
        "authority": {
            "market_row_read": False, "feature_or_outcome_construction": False,
            "model_fit_or_prediction": False, "wfa_or_economics": False,
            "trial_registration": False, "recent_challenge": False,
            "final_252_or_forward": False, "provider_or_network": False,
            "trading": False, "push": False,
        },
    }
    return {**core, "contract_id": sha256_json(core)}


def validate_contract(root: Path, payload: Mapping[str, object]) -> dict[str, object]:
    expected = build_contract(root)
    if dict(payload) != expected:
        raise IntegrityError("bounded discovery contract drifted from the frozen design")
    return expected


def select_candidate(
    contract: Mapping[str, object], scores: Mapping[str, Sequence[float]],
) -> str:
    """Apply the frozen complete-family selection rule to synthetic or future scores."""

    candidates = contract.get("search", {}).get("candidates")  # type: ignore[union-attr]
    if not isinstance(candidates, list):
        raise IntegrityError("bounded discovery candidates are missing")
    ordered = [str(candidate["candidate_id"]) for candidate in candidates]
    if set(scores) != set(ordered) or any(len(values) != 3 for values in scores.values()):
        raise IntegrityError("candidate scores must cover all 27 candidates and three inner folds")
    if any(
        type(value) not in {int, float} or not math.isfinite(value)
        for values in scores.values() for value in values
    ):
        raise IntegrityError("candidate scores must be finite numbers")
    ranked = sorted(ordered, key=lambda item: (-median(scores[item]), ordered.index(item)))
    return ranked[0]


def build_synthetic_certificate(root: Path, contract: Mapping[str, object]) -> dict[str, object]:
    frozen = validate_contract(root, contract)
    ordered = [item["candidate_id"] for item in frozen["search"]["candidates"]]  # type: ignore[index]
    zeros = {str(candidate_id): (0.0, 0.0, 0.0) for candidate_id in ordered}
    winner = dict(zeros)
    winner[str(ordered[-1])] = (1.0, 1.0, 1.0)
    checks = {
        "candidate_count_exact": len(ordered) == 27 and len(set(ordered)) == 27,
        "deterministic_tie_break": select_candidate(frozen, zeros) == ordered[0],
        "best_median_selected": select_candidate(frozen, winner) == ordered[-1],
        "no_real_rows_opened": True,
        "historical_execution_blocked": True,
    }
    if not all(checks.values()):
        raise IntegrityError("bounded discovery synthetic mechanics failed")
    core = {
        "schema_version": "bounded_alpha_discovery_synthetic_certificate/1.0.0",
        "state": "PASS_SYNTHETIC_MECHANICS_ONLY",
        "contract_id": frozen["contract_id"],
        "contract_path": CONTRACT_PATH.as_posix(),
        "contract_sha256": sha256_file(root / CONTRACT_PATH),
        "implementation": {
            "path": "src/futures_rebuild/bounded_alpha_discovery.py",
            "sha256": sha256_file(root / "src/futures_rebuild/bounded_alpha_discovery.py"),
        },
        "checks": checks,
        "does_not_certify": [
            "SOURCE_OR_ROW_READINESS", "ALPHA", "WFA", "ECONOMICS",
            "RECENT_REGIME_CHALLENGE", "FINAL_252", "LIVE_EXECUTION",
        ],
        "authority": {"historical_row_read": False, "trial_execution": False},
    }
    return {**core, "certificate_id": sha256_json(core)}


def _write_new_or_exact(path: Path, payload: Mapping[str, object]) -> None:
    rendered = canonical_bytes(dict(payload)) + b"\n"
    if path.exists():
        if path.read_bytes() != rendered:
            raise IntegrityError(f"bounded discovery artifact drifted: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
    try:
        os.write(descriptor, rendered)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def prepare(root: Path) -> dict[str, str]:
    """Freeze the contract and synthetic certificate; grant no row authority."""

    contract = build_contract(root)
    _write_new_or_exact(root / CONTRACT_PATH, contract)
    certificate = build_synthetic_certificate(root, contract)
    certificate_path = CERTIFICATE_ROOT / str(certificate["certificate_id"]) / "certificate.json"
    _write_new_or_exact(root / certificate_path, certificate)
    return {
        "contract_id": str(contract["contract_id"]),
        "contract_path": CONTRACT_PATH.as_posix(),
        "certificate_id": str(certificate["certificate_id"]),
        "certificate_path": certificate_path.as_posix(),
        "state": "PREPARED_SYNTHETIC_ONLY_NO_ROW_AUTHORITY",
    }
