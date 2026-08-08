"""Baseline-coverage successor for the incomplete V7 model fit.

V8 preserves V7's candidate and source behavior.  Its only research change is
to represent an unavailable required fold-local baseline estimate as an
explicit abstention and to make baseline eligibility coverage a hard,
predeclared decision gate.
"""

from __future__ import annotations

import json
import math
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from decimal import Decimal
from pathlib import Path
from statistics import fmean

import numpy as np

from . import tier1_bracket_v4 as v4
from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v6 as v6
from . import tier1_bracket_v7 as v7
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment


V7_TRIAL_ID = "b2b1b19967418ad9d3a66f70e4fa15732f8065d9abf9f32489b2e2d44072a64e"
V7_REGISTRY = Path("state/trial_registry/tier1_bracket_successor_v7") / f"{V7_TRIAL_ID}.json"
V7_EVENT = Path("state/trial_events/tier1_bracket_successor_v7") / f"{V7_TRIAL_ID}.json"
V7_RETIREMENT_PREPARATION = Path("configs/tier1_bracket_v7_retirement_preparation.json")
V8_CONTRACT = Path("configs/tier1_bracket_successor_v8.json")
V7_RETIREMENT_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_v7_retirement")
V7_RETIREMENT_EVENT_ROOT = Path("state/trial_events/tier1_bracket_v7_retirement")
V8_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_successor_v8")
V8_EVENT_ROOT = Path("state/trial_events/tier1_bracket_successor_v8")
V7_EXECUTION_ARTIFACTS = (
    Path("configs/tier1_bracket_successor_v7_historical_execution_plan.json"),
    Path("configs/tier1_bracket_successor_v7_historical_retry_plan.json"),
    Path("state/authorization_uses/af8ce1dd9daa561761621a2ef7d641c812f39875079edd6fa27ee94a272e8ede.json"),
    Path("state/authorization_uses/98e688ef10d8e389b9abe49ebe01e1b6daa916bb348d5a435673efc2312ab552.json"),
)


def _load(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid V8 JSON artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"V8 artifact is not an object: {path.as_posix()}")
    return value


def load_v8_contract(*, root: Path) -> tuple[dict[str, object], dict[str, object]]:
    delta = _load(root / V8_CONTRACT)
    rule = delta.get("required_baseline_coverage_successor")
    authority = delta.get("authority")
    inherited_path = delta.get("inherited_v7_contract_path")
    inherited_hash = delta.get("inherited_v7_contract_sha256")
    if (
        delta.get("schema_version") != "tier1_bracket_successor_v8_contract/1.0.0"
        or delta.get("state") != "PREPARED_NOT_REGISTERED"
        or delta.get("supersedes_v7_trial_id") != V7_TRIAL_ID
        or inherited_path != "configs/tier1_bracket_successor_v7.json"
        or not v5._hex64(inherited_hash)
        or sha256_file(root / str(inherited_path)) != inherited_hash
        or not isinstance(rule, dict)
        or rule.get("empty_training_cell")
        != "EXPLICIT_BASELINE_ABSTENTION_NO_DIRECTION_NO_SCORE_NO_FALLBACK_NO_POOLING"
        or rule.get("minimum_overall_eligible_rate") != "0.95"
        or rule.get("minimum_each_market_year_eligible_rate") != "0.90"
        or rule.get("coverage_failure_classification") != "INCONCLUSIVE_DATA_OR_COVERAGE"
        or not isinstance(authority, dict)
        or authority.get("publication_requires_separate_approval") is not True
        or authority.get("holdout_or_forward_access") is not False
    ):
        raise IntegrityError("V8 baseline-coverage contract is incomplete or drifted")
    inherited, _ = v7.load_v7_contract(root=root)
    return inherited, delta


@dataclass(frozen=True)
class PreparedV7RetirementV8:
    record_id: str
    canonical_payload: Mapping[str, object]


@dataclass(frozen=True)
class PreparedV8Registration:
    trial_id: str
    canonical_payload: Mapping[str, object]


@dataclass(frozen=True)
class FrozenPredictionV8:
    opportunity_id: str
    market: str
    year: int
    session: str
    checkpoint: str
    outer_fold: int
    long_predicted_net_r: float
    short_predicted_net_r: float
    selected_direction: str
    selected_predicted_net_r: float
    fold_local_direction: str | None
    fold_local_score: float | None
    bar_return_1: float


def prepare_v7_retirement_v8(*, root: Path) -> PreparedV7RetirementV8:
    preparation = _load(root / V7_RETIREMENT_PREPARATION)
    registry = _load(root / V7_REGISTRY)
    event = _load(root / V7_EVENT)
    bindings = registry.get("bindings")
    if (
        preparation.get("trial_id") != V7_TRIAL_ID
        or preparation.get("disposition")
        != "INVALID_PARTIAL_MODEL_FIT_REQUIRED_BASELINE_COVERAGE_EXCEPTION"
        or preparation.get("historical_source_rows_opened") is not True
        or preparation.get("predictions_generated") is not False
        or registry.get("trial_id") != V7_TRIAL_ID
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or event.get("trial_id") != V7_TRIAL_ID
        or not isinstance(bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("V7 retirement preparation or registered bytes are invalid")
    preserved = dict(bindings)
    for path in (V7_REGISTRY, V7_EVENT, *V7_EXECUTION_ARTIFACTS):
        preserved[path.as_posix()] = sha256_file(root / path)
    core = {**preparation, "preserved_v7_sha256": dict(sorted(preserved.items()))}
    return PreparedV7RetirementV8(sha256_json(core), core)


def prepare_v8_registration(*, root: Path) -> PreparedV8Registration:
    _, delta = load_v8_contract(root=root)
    retirement = prepare_v7_retirement_v8(root=root)
    registry = _load(root / V7_REGISTRY)
    prior_bindings = registry.get("bindings")
    sources = registry.get("source_bindings")
    if not isinstance(prior_bindings, dict) or not isinstance(sources, list):
        raise IntegrityError("V7 lineage is incomplete for V8 registration")
    bindings = dict(prior_bindings)
    new_paths = (
        V7_RETIREMENT_PREPARATION,
        V8_CONTRACT,
        Path("src/futures_rebuild/tier1_bracket_v8.py"),
        Path("tests/test_tier1_bracket_v8.py"),
        Path("tests/conftest.py"),
        V7_REGISTRY,
        V7_EVENT,
        *V7_EXECUTION_ARTIFACTS,
    )
    bindings.update({path.as_posix(): sha256_file(root / path) for path in new_paths})
    source_binding_id = v5.source_binding_id_from_metadata_v5(sources)
    core = {
        "schema_version": "tier1_bracket_successor_v8_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": delta["classification"],
        "supersedes_v7_trial_id": V7_TRIAL_ID,
        "v7_retirement_record_id": retirement.record_id,
        "change_scope": "REQUIRED_BASELINE_ABSTENTION_AND_COVERAGE_GATE_ONLY",
        "inherited_v7_contract_sha256": delta["inherited_v7_contract_sha256"],
        "bindings": bindings,
        "calendar_release_id": registry["calendar_release_id"],
        "dependency_lock_receipt_id": registry["dependency_lock_receipt_id"],
        "source_bindings": sorted(
            (dict(item) for item in sources),
            key=lambda item: (str(item["market"]), int(item["year"])),
        ),
        "source_binding_id": source_binding_id,
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "publication": False,
        "holdout_or_forward_access": False,
        "provider_access": False,
        "trading": False,
    }
    return PreparedV8Registration(sha256_json(core), core)


def persist_v7_retirement_v8(
    *, root: Path, prepared: PreparedV7RetirementV8,
) -> dict[str, str]:
    if prepared.record_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V7 retirement identity is invalid")
    preserved = prepared.canonical_payload.get("preserved_v7_sha256")
    if not isinstance(preserved, dict) or any(
        sha256_file(root / path) != digest for path, digest in preserved.items()
    ):
        raise IntegrityError("preserved V7 bytes changed after retirement preparation")
    registry = V7_RETIREMENT_REGISTRY_ROOT / f"{prepared.record_id}.json"
    event = V7_RETIREMENT_EVENT_ROOT / f"{prepared.record_id}.json"
    registry_path, event_path = root / registry, root / event
    if registry_path.exists() or event_path.exists():
        raise IntegrityError("V7 retirement publication is create-only")
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    event_path.parent.mkdir(parents=True, exist_ok=True)
    with registry_path.open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.canonical_payload,
            "state": "RETIRED_INVALID_AFTER_SOURCE_ACCESS_BEFORE_PREDICTIONS",
        }) + b"\n")
    with event_path.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_bracket_v7_retirement_event/1.0.0",
            "event_type": "RETIRED",
            "trial_id": V7_TRIAL_ID,
            "record_id": prepared.record_id,
        }) + b"\n")
    return {
        "record_id": prepared.record_id,
        "registry_path": registry.as_posix(),
        "event_path": event.as_posix(),
    }


def persist_v8_registration(
    *, root: Path, prepared: PreparedV8Registration,
) -> dict[str, str]:
    if prepared.trial_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V8 trial identity is invalid")
    bindings = prepared.canonical_payload.get("bindings")
    if not isinstance(bindings, dict) or any(
        sha256_file(root / path) != digest for path, digest in bindings.items()
    ):
        raise IntegrityError("V8 registration binding changed after preparation")
    retirement_id = prepared.canonical_payload.get("v7_retirement_record_id")
    if not v5._hex64(retirement_id):
        raise IntegrityError("V8 registration lacks a V7 retirement identity")
    retirement = _load(root / V7_RETIREMENT_REGISTRY_ROOT / f"{retirement_id}.json")
    if (
        retirement.get("state") != "RETIRED_INVALID_AFTER_SOURCE_ACCESS_BEFORE_PREDICTIONS"
        or sha256_json({
            **retirement, "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        }) != retirement_id
    ):
        raise IntegrityError("published V7 retirement is absent or inconsistent")
    registry = V8_REGISTRY_ROOT / f"{prepared.trial_id}.json"
    event = V8_EVENT_ROOT / f"{prepared.trial_id}.json"
    registry_path, event_path = root / registry, root / event
    if registry_path.exists() or event_path.exists():
        raise IntegrityError("V8 registration publication is create-only")
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    event_path.parent.mkdir(parents=True, exist_ok=True)
    with registry_path.open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.canonical_payload,
            "state": "REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
            "trial_id": prepared.trial_id,
        }) + b"\n")
    with event_path.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_bracket_successor_v8_event/1.0.0",
            "event_type": "DECLARED",
            "trial_id": prepared.trial_id,
            "source_row_access": False,
            "model_fit": False,
            "prediction_generation": False,
            "historical_evaluation": False,
            "holdout_or_forward_access": False,
        }) + b"\n")
    return {
        "trial_id": prepared.trial_id,
        "registry_path": registry.as_posix(),
        "event_path": event.as_posix(),
    }


def fit_predict_v8(
    *, rows: Sequence[v5.MaterializedRowV5], folds: Sequence[v4.FoldSpec],
) -> v4.ModelFitResult:
    """Preserve V7 candidate math; abstain when a baseline cell is empty."""

    if len(folds) != 8 or {fold.outer_fold for fold in folds} != set(range(8)):
        raise IntegrityError("V8 requires exactly eight outer folds")
    all_sessions = sorted({row.expected.exchange_session_date for row in rows})
    session_order = {session: index for index, session in enumerate(all_sessions)}
    test_owners: dict[str, int] = {}
    for fold in folds:
        if not fold.training_sessions or not fold.test_sessions:
            raise IntegrityError("fold training or test sessions are empty")
        if set(fold.training_sessions).intersection(fold.test_sessions):
            raise IntegrityError("fold training and test sessions overlap")
        if max(session_order[item] for item in fold.training_sessions) >= min(
            session_order[item] for item in fold.test_sessions
        ):
            raise IntegrityError("fold is not chronological")
        for session in fold.test_sessions:
            if session in test_owners:
                raise IntegrityError("test session belongs to multiple folds")
            test_owners[session] = fold.outer_fold
    models: list[dict[str, object]] = []
    predictions: list[FrozenPredictionV8] = []
    exclusions = 0
    for fold in sorted(folds, key=lambda item: item.outer_fold):
        for market in v5.MARKETS:
            training_all = [
                row for row in rows
                if row.expected.market == market
                and row.expected.exchange_session_date in fold.training_sessions
                and row.features is not None
            ]
            training = [row for row in training_all if row.outcomes is not None]
            exclusions += len(training_all) - len(training)
            testing = [
                row for row in rows
                if row.expected.market == market
                and row.expected.exchange_session_date in fold.test_sessions
                and row.features is not None
                and row.ledger.prediction_produced
            ]
            if not training or not testing:
                raise IntegrityError("market-fold lacks training or prediction coverage")
            x = np.asarray(
                [[float(row.features[name]) for name in v4.FEATURE_NAMES] for row in training],
                dtype=np.float64,
            )
            center = x.mean(axis=0)
            scale = x.std(axis=0, ddof=0)
            if not np.isfinite(x).all() or not np.isfinite(scale).all():
                raise IntegrityError("training-only standardization is non-finite")
            constant_features = [
                v4.FEATURE_NAMES[index] for index, value in enumerate(scale) if value == 0
            ]
            scale = np.where(scale == 0, 1.0, scale)
            z = (x - center) / scale
            design = np.column_stack((np.ones(len(z)), z))
            y_long = np.asarray([v4._stress_r(row, "long") for row in training], dtype=np.float64)
            y_short = np.asarray([v4._stress_r(row, "short") for row in training], dtype=np.float64)
            penalty = np.eye(design.shape[1], dtype=np.float64)
            penalty[0, 0] = 0.0
            try:
                long_coef = np.linalg.solve(design.T @ design + penalty, design.T @ y_long)
                short_coef = np.linalg.solve(design.T @ design + penalty, design.T @ y_short)
            except np.linalg.LinAlgError as exc:
                raise IntegrityError("V8 Ridge fit is singular") from exc
            checkpoint_means: dict[str, dict[str, object]] = {}
            for checkpoint in v5.CHECKPOINTS:
                subset = [row for row in training if row.expected.checkpoint == checkpoint]
                if not subset:
                    checkpoint_means[checkpoint] = {
                        "status": "NO_TRAINING_OUTCOME_ABSTAIN",
                        "direction": None,
                        "score": None,
                        "training_rows": 0,
                    }
                    continue
                long_mean = fmean(v4._stress_r(row, "long") for row in subset)  # type: ignore[arg-type]
                short_mean = fmean(v4._stress_r(row, "short") for row in subset)  # type: ignore[arg-type]
                direction = "long" if long_mean >= short_mean else "short"
                checkpoint_means[checkpoint] = {
                    "status": "ESTIMATED",
                    "direction": direction,
                    "score": max(long_mean, short_mean),
                    "training_rows": len(subset),
                }
            models.append({
                "outer_fold": fold.outer_fold,
                "market": market,
                "training_opportunity_ids": sorted(row.expected.opportunity_id for row in training),
                "training_rows": len(training),
                "training_outcome_exclusions": len(training_all) - len(training),
                "feature_center": center.tolist(),
                "feature_population_std": scale.tolist(),
                "constant_training_features_scaled_to_one": constant_features,
                "long_coefficients": long_coef.tolist(),
                "short_coefficients": short_coef.tolist(),
                "fold_local_unconditional": checkpoint_means,
            })
            for row in testing:
                assert row.features is not None
                vector = np.asarray(
                    [float(row.features[name]) for name in v4.FEATURE_NAMES], dtype=np.float64,
                )
                design_row = np.concatenate(([1.0], (vector - center) / scale))
                long_value = float(design_row @ long_coef)
                short_value = float(design_row @ short_coef)
                if not math.isfinite(long_value) or not math.isfinite(short_value):
                    raise IntegrityError("frozen prediction is non-finite")
                if long_value == short_value:
                    direction, selected = "neutral", long_value
                elif long_value > short_value:
                    direction, selected = "long", long_value
                else:
                    direction, selected = "short", short_value
                if selected < 0.25:
                    direction = "neutral"
                baseline = checkpoint_means[row.expected.checkpoint]
                predictions.append(FrozenPredictionV8(
                    row.expected.opportunity_id, market, row.expected.year,
                    row.expected.exchange_session_date, row.expected.checkpoint,
                    fold.outer_fold, long_value, short_value, direction, selected,
                    (
                        str(baseline["direction"])
                        if baseline["direction"] is not None else None
                    ),
                    (
                        float(baseline["score"])
                        if baseline["score"] is not None else None
                    ),
                    float(row.features["bar_return_1"]),
                ))
    predicted_ids = [item.opportunity_id for item in predictions]
    if len(predicted_ids) != len(set(predicted_ids)):
        raise IntegrityError("frozen predictions are duplicated")
    expected_prediction_ids = {
        row.expected.opportunity_id for row in rows
        if row.ledger.prediction_produced and row.features is not None
        and row.expected.exchange_session_date in test_owners
    }
    if set(predicted_ids) != expected_prediction_ids:
        raise IntegrityError("frozen prediction coverage does not reconcile")
    core: dict[str, object] = {
        "schema_version": "tier1_bracket_successor_v8_models/1.0.0",
        "feature_names": list(v4.FEATURE_NAMES),
        "model_family": "MARKET_SPECIFIC_TWO_TARGET_RIDGE",
        "ridge_penalty": 1.0,
        "training_only_standardization": True,
        "candidate_math": "FROZEN_IDENTICAL_TO_V7",
        "fold_local_empty_cell": "EXPLICIT_ABSTENTION",
        "pre_prediction_risk_cap_usd": "250",
        "source_eligibility": "DISPOSITION_GATED",
        "training_statistic_lineage": "NESTED_CROSSFIT_REQUIRED_FOR_POWER",
        "models": models,
    }
    return v4.ModelFitResult(
        {**core, "model_bundle_id": sha256_json(core)},
        tuple(sorted(predictions, key=lambda item: (item.session, item.checkpoint, item.market))),
        exclusions,
    )


def plan_strategy_v8(
    *, strategy: str, predictions: Sequence[FrozenPredictionV8],
    rows: Sequence[v5.MaterializedRowV5], scenario: str,
) -> v5.PlannedStrategyV5:
    if strategy != "fold_local_unconditional_return_by_market_session":
        return v5.plan_strategy_v5(
            strategy=strategy, predictions=predictions, rows=rows, scenario=scenario,
        )
    unavailable = {
        prediction.opportunity_id for prediction in predictions
        if prediction.fold_local_direction is None
    }
    eligible = tuple(
        prediction for prediction in predictions
        if prediction.fold_local_direction is not None
    )
    plan = v5.plan_strategy_v5(
        strategy=strategy, predictions=eligible, rows=rows, scenario=scenario,
    )
    terminals = dict(plan.preliminary_terminals)
    terminals.update({item: "BASELINE_TRAINING_COVERAGE_ABSTENTION" for item in unavailable})
    return v5.PlannedStrategyV5(strategy, plan.trades, dict(sorted(terminals.items())))


def evaluate_strategies_v8(
    *, predictions: Sequence[FrozenPredictionV8], rows: Sequence[v5.MaterializedRowV5],
    strategies: Sequence[str],
) -> Mapping[str, Mapping[str, v5.AccountPathV5]]:
    prediction_ids = tuple(prediction.opportunity_id for prediction in predictions)
    if len(prediction_ids) != len(set(prediction_ids)):
        raise IntegrityError("V8 frozen predictions are duplicated")
    result: dict[str, Mapping[str, v5.AccountPathV5]] = {}
    for scenario in ("base", "stress", "extreme"):
        plans = {
            strategy: plan_strategy_v8(
                strategy=strategy, predictions=predictions, rows=rows, scenario=scenario,
            )
            for strategy in strategies
        }
        paths = v5.simulate_independent_strategy_paths_v5(
            plans_by_strategy={name: plan.trades for name, plan in plans.items()},
            opportunity_ids_by_strategy={name: prediction_ids for name in plans},
        )
        reconciled: dict[str, v5.AccountPathV5] = {}
        for name, path in paths.items():
            terminals = dict(path.terminal_dispositions)
            for opportunity_id, disposition in plans[name].preliminary_terminals.items():
                if terminals[opportunity_id] == "NO_SIGNAL":
                    terminals[opportunity_id] = disposition
            if set(terminals) != set(prediction_ids):
                raise IntegrityError("V8 strategy terminal ledger does not reconcile")
            reconciled[name] = v5.AccountPathV5(
                path.strategy, path.admitted, dict(sorted(terminals.items())),
                path.equity_marks, path.session_net_pnl_usd,
                path.ending_equity_usd, path.maximum_continuous_drawdown_usd,
                path.complete,
            )
        result[scenario] = reconciled
    return result


def evaluate_required_baseline_coverage_v8(
    predictions: Sequence[FrozenPredictionV8],
) -> dict[str, object]:
    required = {
        f"{market}/{year}" for market in v5.MARKETS for year in range(2020, 2023)
    }
    expected: dict[str, int] = {}
    eligible: dict[str, int] = {}
    for prediction in predictions:
        key = f"{prediction.market}/{prediction.year}"
        expected[key] = expected.get(key, 0) + 1
        eligible[key] = eligible.get(key, 0) + int(
            prediction.fold_local_direction in {"long", "short"}
            and prediction.fold_local_score is not None
        )
    if not predictions or set(expected) != required or set(eligible) != required:
        return {"status": "INVALID", "passed": False}
    rates = {key: eligible[key] / expected[key] for key in sorted(required)}
    overall = sum(eligible.values()) / sum(expected.values())
    passed = overall >= 0.95 and min(rates.values()) >= 0.90
    return {
        "status": "PASS" if passed else "INCONCLUSIVE_DATA_OR_COVERAGE",
        "passed": passed,
        "expected_prediction_opportunities": sum(expected.values()),
        "eligible_prediction_opportunities": sum(eligible.values()),
        "overall_eligible_rate": overall,
        "minimum_overall_eligible_rate": 0.95,
        "market_year_eligible_rates": rates,
        "minimum_each_market_year_eligible_rate": 0.90,
    }


def build_nested_crossfit_evidence_v8(
    *, rows: Sequence[v5.MaterializedRowV5],
) -> v5.CrossfitEvidenceBundleV5:
    sessions = sorted({
        row.expected.exchange_session_date for row in rows if row.expected.year in {2018, 2019}
    })
    seed_size = max(30, math.ceil(len(sessions) * 0.40))
    evaluation_sessions = sessions[seed_size:]
    if seed_size >= len(sessions) or len(evaluation_sessions) < 8:
        raise IntegrityError("training history cannot support nested crossfit")
    quotient, remainder = divmod(len(evaluation_sessions), 8)
    folds: list[v4.FoldSpec] = []
    owners: dict[str, int] = {}
    start = 0
    for index in range(8):
        size = quotient + (1 if index < remainder else 0)
        test = evaluation_sessions[start:start + size]
        first = sessions.index(test[0])
        training = sessions[:first - 1]
        if not training:
            raise IntegrityError("nested crossfit embargo leaves no training history")
        folds.append(v4.FoldSpec(index, tuple(training), tuple(test)))
        owners.update({session: index for session in test})
        start += size
    crossfit_rows: list[v5.MaterializedRowV5] = []
    for row in rows:
        session = row.expected.exchange_session_date
        predict = session in owners and row.features is not None and row.outcomes is not None
        if predict:
            if row.ledger.feature_event_at_ns is None or row.ledger.feature_available_at_ns is None:
                raise IntegrityError("nested crossfit row lacks causal feature lineage")
            ledger = v5.OpportunityRecordV5(
                row.expected.opportunity_id, row.expected.market,
                row.expected.exchange_session_date, row.expected.checkpoint,
                row.expected.decision_at_ns, "PREDICTION_PRODUCED", True,
                row.ledger.feature_event_at_ns, row.ledger.feature_available_at_ns,
                outcome_coverage=row.ledger.outcome_coverage,
            )
            crossfit_rows.append(replace(row, ledger=ledger))
        else:
            crossfit_rows.append(row)
    model = fit_predict_v8(rows=crossfit_rows, folds=folds)
    evaluation = evaluate_strategies_v8(
        predictions=model.predictions, rows=crossfit_rows,
        strategies=v5.REQUIRED_ACTIVE_STRATEGIES_V5,
    )["stress"]
    ordered_sessions = tuple(evaluation_sessions)
    candidate = evaluation["candidate"]
    differential = {
        baseline: tuple(
            float((
                candidate.session_net_pnl_usd.get(session, Decimal("0"))
                - evaluation[baseline].session_net_pnl_usd.get(session, Decimal("0"))
            ) / Decimal("100000"))
            for session in ordered_sessions
        )
        for baseline in v5.REQUIRED_ACTIVE_STRATEGIES_V5[1:]
    }
    sleeve_ids = tuple(
        f"{market}/{checkpoint}/{direction}"
        for market in v5.MARKETS for checkpoint in v5.CHECKPOINTS
        for direction in ("long", "short")
    )
    contributions = {
        sleeve: {session: Decimal("0") for session in ordered_sessions}
        for sleeve in sleeve_ids
    }
    for trade in candidate.admitted:
        contributions[f"{trade.market}/{trade.checkpoint}/{trade.direction}"][trade.session] += trade.fill.net_pnl_usd
    sleeve_returns = {
        sleeve: tuple(
            float(contributions[sleeve][session] / Decimal("100000"))
            for session in ordered_sessions
        )
        for sleeve in sleeve_ids
    }
    return v5.CrossfitEvidenceBundleV5(
        ordered_sessions, tuple(owners[session] for session in ordered_sessions),
        differential, sleeve_returns,
    )


def derive_v8_decision(
    *, evaluation: Mapping[str, Mapping[str, v5.AccountPathV5]],
    evaluation_sessions: Sequence[str], coverage: v5.CoverageEvidence,
    baseline_coverage: Mapping[str, object],
    crossfit: v5.CrossfitEvidenceBundleV5, seed: int,
) -> dict[str, object]:
    inherited = v5.derive_v5_decision(
        evaluation=evaluation, evaluation_sessions=evaluation_sessions,
        coverage=coverage, crossfit=crossfit, seed=seed,
    )
    core = dict(inherited)
    core.pop("decision_id", None)
    core["schema_version"] = "tier1_bracket_successor_v8_decision/1.0.0"
    core["required_baseline_coverage"] = dict(baseline_coverage)
    if inherited.get("classification") != "INVALID" and baseline_coverage.get("status") != "PASS":
        core["classification"] = "INCONCLUSIVE_DATA_OR_COVERAGE"
    return {**core, "decision_id": sha256_json(core)}


def run_v8_pipeline(
    *, streams: Mapping[tuple[str, int], Sequence[v5.V5SourceRecord] | Iterator[v5.V5SourceRecord]],
    census: Sequence[v5.CensusCheckpoint], contract: Mapping[str, object],
    trial_id: str, runtime_receipt: Mapping[str, object],
) -> v5.V5PipelineResult:
    if not v5._hex64(trial_id):
        raise IntegrityError("V8 pipeline requires a registered trial identity")
    folds = v5.build_v5_folds_from_census(census)
    prediction_sessions = tuple(session for fold in folds for session in fold.test_sessions)
    rows = v5.materialize_v5_streams(
        streams=streams, census=census, contract=contract,
        prediction_scope_sessions=prediction_sessions,
    )
    model = fit_predict_v8(rows=rows, folds=folds)
    evaluation = evaluate_strategies_v8(
        predictions=model.predictions, rows=rows,
        strategies=v5.REQUIRED_ACTIVE_STRATEGIES_V5,
    )
    opportunity_metadata = {
        prediction.opportunity_id: (prediction.market, prediction.year)
        for prediction in model.predictions
    }
    segmented: dict[str, Mapping[str, v5.AccountPathV5]] = {}
    for strategy in v5.REQUIRED_ACTIVE_STRATEGIES_V5:
        plan = plan_strategy_v8(
            strategy=strategy, predictions=model.predictions, rows=rows, scenario="stress",
        )
        segmented[strategy] = v5.segmented_account_views_v5(
            strategy=strategy, planned_trades=plan.trades,
            opportunity_market_year=opportunity_metadata,
        )
    prediction_scope = set(prediction_sessions)
    calendar_open_ids = {item.expected.opportunity_id for item in census if item.calendar_open}
    evaluation_rows = [
        row for row in rows if row.expected.exchange_session_date in prediction_scope
    ]
    market_year_expected: dict[str, int] = {}
    market_year_features: dict[str, int] = {}
    for row in evaluation_rows:
        if row.expected.opportunity_id not in calendar_open_ids:
            continue
        key = f"{row.expected.market}/{row.expected.year}"
        market_year_expected[key] = market_year_expected.get(key, 0) + 1
        market_year_features[key] = market_year_features.get(key, 0) + int(row.features is not None)
    coverage_evidence = v5.CoverageEvidence(
        expected=len(evaluation_rows), terminal=len(evaluation_rows),
        causal_feature_expected=sum(
            row.expected.opportunity_id in calendar_open_ids for row in evaluation_rows
        ),
        causal_feature_eligible=sum(row.features is not None for row in evaluation_rows),
        predictions=len(model.predictions), market_year_expected=market_year_expected,
        market_year_feature_eligible=market_year_features,
    )
    coverage_result = v5.evaluate_coverage_gate(coverage_evidence)
    baseline_coverage = evaluate_required_baseline_coverage_v8(model.predictions)
    crossfit = build_nested_crossfit_evidence_v8(rows=rows)
    seed = int(trial_id[:16], 16)
    decision = derive_v8_decision(
        evaluation=evaluation, evaluation_sessions=prediction_sessions,
        coverage=coverage_evidence, baseline_coverage=baseline_coverage,
        crossfit=crossfit, seed=seed,
    )
    opportunity_ledger = [
        asdict(record) for record in v5.finalize_candidate_ledger_v5(
            rows=rows, candidate_path=evaluation["stress"]["candidate"],
        )
    ]
    fills: list[dict[str, object]] = []
    marks: list[dict[str, object]] = []
    for scenario, paths in evaluation.items():
        for strategy, path in paths.items():
            for trade in path.admitted:
                fills.append({
                    "scenario": scenario, "strategy": strategy,
                    "opportunity_id": trade.opportunity_id, "market": trade.market,
                    "year": trade.year, "session": trade.session,
                    "checkpoint": trade.checkpoint, "direction": trade.direction,
                    "fill": asdict(trade.fill),
                })
            for at_ns, opportunity_id, kind, equity in path.equity_marks:
                marks.append({
                    "scenario": scenario, "strategy": strategy, "at_ns": at_ns,
                    "opportunity_id": opportunity_id, "kind": kind,
                    "equity_usd": str(equity),
                })
    segmented_payload = {
        strategy: {
            key: {
                "ending_equity_usd": str(path.ending_equity_usd),
                "maximum_continuous_drawdown_usd": str(path.maximum_continuous_drawdown_usd),
                "complete": path.complete,
                "terminal_dispositions": dict(path.terminal_dispositions),
                "admitted_fills": [
                    {
                        "opportunity_id": trade.opportunity_id, "market": trade.market,
                        "year": trade.year, "session": trade.session,
                        "checkpoint": trade.checkpoint, "direction": trade.direction,
                        "fill": asdict(trade.fill),
                    }
                    for trade in path.admitted
                ],
                "continuous_equity_marks": [
                    {
                        "at_ns": at_ns, "opportunity_id": opportunity_id,
                        "kind": kind, "equity_usd": str(equity),
                    }
                    for at_ns, opportunity_id, kind, equity in path.equity_marks
                ],
            }
            for key, path in views.items()
        }
        for strategy, views in segmented.items()
    }
    artifacts = v5.EvidenceArtifactsV5(
        model=model.canonical_model_payload,
        predictions=tuple(asdict(item) for item in model.predictions),
        opportunity_ledger=tuple(opportunity_ledger), fills=tuple(fills),
        continuous_equity_marks=tuple(marks), segmented_metrics=segmented_payload,
        inference={
            "crossfit_sessions": list(crossfit.sessions),
            "crossfit_fold_ids": list(crossfit.fold_ids),
            "coverage": coverage_result,
            "required_baseline_coverage": baseline_coverage,
        },
        decision=decision, runtime_receipt=runtime_receipt,
    )
    v5.build_evidence_manifest_v5(trial_id=trial_id, artifacts=artifacts)
    return v5.V5PipelineResult(
        rows, model, evaluation, segmented, crossfit, coverage_result, decision, artifacts,
    )


def verify_historical_operation_receipt_v8(
    *, boundary: RepoBoundary, receipt: OperationReceipt, trial_id: str,
    source_binding_id: str, output_root: Path,
) -> str:
    if not v5._hex64(trial_id) or not v5._hex64(source_binding_id):
        raise UnauthorizedOperation("V8 historical receipt scope is invalid")
    boundary.assert_active_path(output_root.absolute(), purpose="V8 historical output root")
    required = {
        "trial_id": trial_id, "source_binding_id": source_binding_id,
        "output_root": output_root.as_posix(), "holdout_or_forward_access": "false",
        "provider_access": "false", "publication": "false",
    }
    receipt.verify(
        boundary, operation="EXECUTE_TIER1_BRACKET_SUCCESSOR_V8_HISTORICAL_SCREEN",
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
    )
    observed = dict(receipt.scope)
    approval = {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    if set(observed) != set(required) | approval or any(
        observed.get(key) != value for key, value in required.items()
    ):
        raise UnauthorizedOperation("V8 receipt does not grant the exact historical scope")
    if not receipt.single_use or not receipt.externally_authorized:
        raise UnauthorizedOperation("V8 execution requires single-use external authority")
    return receipt.receipt_id


def claim_historical_operation_receipt_v8(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_binding_id: str, output_root: Path,
) -> Path:
    receipt_id = verify_historical_operation_receipt_v8(
        boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_binding_id=source_binding_id, output_root=output_root,
    )
    claim = root / "state/authorization_uses" / f"{receipt_id}.json"
    boundary.assert_active_path(
        claim.absolute(), purpose="V8 authorization use", subtree="state/authorization_uses",
    )
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        with claim.open("xb") as stream:
            stream.write(canonical_bytes({
                "schema_version": "tier1_bracket_v8_authorization_use/1.0.0",
                "receipt_id": receipt_id, "trial_id": trial_id,
                "source_binding_id": source_binding_id,
                "output_root": output_root.as_posix(),
                "holdout_or_forward_access": False,
            }) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("V8 historical receipt was already consumed") from exc
    return claim


def authorized_source_streams_v8(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path], output_root: Path,
) -> tuple[
    Mapping[tuple[str, int], Iterator[v5.V5SourceRecord]],
    Mapping[tuple[str, int], v6.SourceIntegrityAuditV6],
]:
    if any(year == 2025 for _, year in source_paths):
        raise UnauthorizedOperation("2025 holdout path is rejected before open")
    registry = _load(root / V8_REGISTRY_ROOT / f"{trial_id}.json")
    bindings = registry.get("bindings")
    if (
        registry.get("trial_id") != trial_id
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or registry.get("holdout_or_forward_access") is not False
        or not isinstance(bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
    ):
        raise UnauthorizedOperation("registered V8 declaration is unavailable or drifted")
    require_locked_repository_environment(root)
    raw = registry.get("source_bindings")
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise IntegrityError("registered V8 source bindings are absent")
    binding_id = v5.source_binding_id_from_metadata_v5(raw)
    expected = {
        (str(item["market"]), int(item["year"])): str(item["source_parquet_sha256"])
        for item in raw
    }
    if registry.get("source_binding_id") != binding_id or set(source_paths) != set(expected):
        raise IntegrityError("V8 source binding or path map is inconsistent")
    claim_historical_operation_receipt_v8(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_binding_id=binding_id, output_root=output_root,
    )
    for key, path in source_paths.items():
        if sha256_file(path) != expected[key]:
            raise IntegrityError("V8 source bytes differ from registration")
    audits = {key: v6.SourceIntegrityAuditV6(key[0]) for key in sorted(source_paths)}
    streams = {
        key: v6.iter_source_records_from_parquet_v6(
            market=key[0], path=source_paths[key], audit=audits[key],
        )
        for key in sorted(source_paths)
    }
    return streams, audits


def execute_authorized_v8(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path], output_root: Path,
) -> v6.V6PipelineResult:
    streams, audits = authorized_source_streams_v8(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_paths=source_paths, output_root=output_root,
    )
    registry = _load(root / V8_REGISTRY_ROOT / f"{trial_id}.json")
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(registry["calendar_release_id"]),
    )
    inherited, _ = load_v8_contract(root=root)
    base = run_v8_pipeline(
        streams=streams, census=v5.build_expected_census_from_calendar(sessions=sessions),
        contract=inherited, trial_id=trial_id,
        runtime_receipt=v5.prepare_runtime_receipt_v5(root=root, trial_id=trial_id),
    )
    audit_payload = {
        f"{market}/{year}": audit.as_dict()
        for (market, year), audit in sorted(audits.items())
    }
    return v6.V6PipelineResult(base, audit_payload)


def build_evidence_manifest_v8(
    *, trial_id: str, result: v6.V6PipelineResult,
) -> dict[str, object]:
    payloads = v6._evidence_payloads_v6(result)
    if not result.base.evidence.predictions or not result.base.evidence.opportunity_ledger:
        raise IntegrityError("V8 evidence lacks frozen predictions or opportunity rows")
    files = {
        f"{name}.json": sha256_bytes(canonical_bytes({"payload": payload}) + b"\n")
        for name, payload in sorted(payloads.items())
    }
    core = {
        "schema_version": "tier1_bracket_successor_v8_evidence_manifest/1.0.0",
        "trial_id": trial_id, "files": files,
    }
    return {**core, "manifest_id": sha256_json(core)}


def persist_evidence_bundle_v8(
    *, boundary: RepoBoundary, output_root: Path, trial_id: str,
    result: v6.V6PipelineResult,
) -> dict[str, str]:
    manifest = build_evidence_manifest_v8(trial_id=trial_id, result=result)
    boundary.assert_active_path(output_root.absolute(), purpose="V8 evidence output root")
    destination = output_root / trial_id / str(manifest["manifest_id"])
    if destination.exists():
        raise IntegrityError("V8 evidence publication is create-only")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(
        prefix=f".staging-{manifest['manifest_id']}-", dir=destination.parent,
    ))
    payloads = v6._evidence_payloads_v6(result)
    for filename, expected_hash in manifest["files"].items():
        name = filename.removesuffix(".json")
        path = staging / filename
        with path.open("xb") as stream:
            stream.write(canonical_bytes({"payload": payloads[name]}) + b"\n")
        if sha256_file(path) != expected_hash:
            raise IntegrityError("persisted V8 evidence hash mismatch")
    manifest_path = staging / "manifest.json"
    with manifest_path.open("xb") as stream:
        stream.write(canonical_bytes(manifest) + b"\n")
    if destination.exists():
        raise IntegrityError("V8 evidence destination appeared during publication")
    staging.replace(destination)
    final_manifest = destination / "manifest.json"
    return {
        "manifest_id": str(manifest["manifest_id"]),
        "manifest_path": final_manifest.as_posix(),
        "manifest_sha256": sha256_file(final_manifest),
    }
