"""Market-fold coverage successor for the incomplete V8 model fit.

V9 preserves every available-cell candidate calculation.  Empty test cells are
diagnostics, while eligible test opportunities without a trainable market-fold
model become explicit pre-prediction abstentions and remain in coverage gates.
"""

from __future__ import annotations

import json
import math
import tempfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from decimal import Decimal
from pathlib import Path
from statistics import fmean

import numpy as np

from . import tier1_bracket_v4 as v4
from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v6 as v6
from . import tier1_bracket_v8 as v8
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment


V8_TRIAL_ID = "136f1d9c7779f48565e7100bb890c148f6aed751faaa84670795b45348cf6233"
V8_REGISTRY = Path("state/trial_registry/tier1_bracket_successor_v8") / f"{V8_TRIAL_ID}.json"
V8_EVENT = Path("state/trial_events/tier1_bracket_successor_v8") / f"{V8_TRIAL_ID}.json"
V8_RETIREMENT_PREPARATION = Path("configs/tier1_bracket_v8_retirement_preparation.json")
V9_CONTRACT = Path("configs/tier1_bracket_successor_v9.json")
V8_EXECUTION_PLAN = Path("configs/tier1_bracket_successor_v8_historical_execution_plan.json")
V8_AUTHORIZATION_USE = Path(
    "state/authorization_uses/8ddd5886bc02b93873a3b34811dced29f03123f6cf60927f775df66698e0c82f.json"
)
V8_RETIREMENT_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_v8_retirement")
V8_RETIREMENT_EVENT_ROOT = Path("state/trial_events/tier1_bracket_v8_retirement")
V9_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_successor_v9")
V9_EVENT_ROOT = Path("state/trial_events/tier1_bracket_successor_v9")


def _load(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid V9 JSON artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"V9 artifact is not an object: {path.as_posix()}")
    return value


def load_v9_contract(*, root: Path) -> tuple[dict[str, object], dict[str, object]]:
    delta = _load(root / V9_CONTRACT)
    rule = delta.get("market_fold_coverage_successor")
    authority = delta.get("authority")
    inherited_path = delta.get("inherited_v8_contract_path")
    inherited_hash = delta.get("inherited_v8_contract_sha256")
    if (
        delta.get("schema_version") != "tier1_bracket_successor_v9_contract/1.0.0"
        or delta.get("state") != "PREPARED_NOT_REGISTERED"
        or delta.get("supersedes_v8_trial_id") != V8_TRIAL_ID
        or inherited_path != "configs/tier1_bracket_successor_v8.json"
        or not v5._hex64(inherited_hash)
        or sha256_file(root / str(inherited_path)) != inherited_hash
        or not isinstance(rule, dict)
        or rule.get("empty_test_partition")
        != "MODEL_CELL_RECORDED_NO_FIT_NO_PREDICTIONS_NO_EXCEPTION"
        or rule.get("empty_training_partition_with_eligible_test_rows")
        != "EVERY_AFFECTED_OPPORTUNITY_EXPLICIT_PRE_PREDICTION_ABSTENTION"
        or rule.get("training_fallback_pooling_or_borrowing") != "FORBIDDEN"
        or rule.get("nested_crossfit_coverage_denominator")
        != "EVERY_CALENDAR_OPEN_CHECKPOINT_IN_CROSSFIT_EVALUATION_SESSIONS"
        or rule.get("nested_crossfit_minimum_overall_statistic_eligibility") != "0.95"
        or rule.get("nested_crossfit_minimum_each_market_statistic_eligibility") != "0.90"
        or rule.get("nested_crossfit_minimum_overall_model_availability") != "0.99"
        or rule.get("nested_crossfit_minimum_each_market_availability") != "0.90"
        or not isinstance(authority, dict)
        or authority.get("publication_requires_separate_approval") is not True
        or authority.get("holdout_or_forward_access") is not False
    ):
        raise IntegrityError("V9 market-fold coverage contract is incomplete or drifted")
    inherited, _ = v8.load_v8_contract(root=root)
    return inherited, delta


@dataclass(frozen=True)
class PreparedV8RetirementV9:
    record_id: str
    canonical_payload: Mapping[str, object]


@dataclass(frozen=True)
class PreparedV9Registration:
    trial_id: str
    canonical_payload: Mapping[str, object]


@dataclass(frozen=True)
class ModelFitResultV9:
    canonical_model_payload: Mapping[str, object]
    predictions: tuple[v8.FrozenPredictionV8, ...]
    training_outcome_exclusions: int
    model_unavailable_opportunity_ids: tuple[str, ...]


@dataclass(frozen=True)
class CrossfitEvidenceV9:
    base: v5.CrossfitEvidenceBundleV5
    model_availability: Mapping[str, object]


@dataclass(frozen=True)
class OpportunityRecordV9(v5.OpportunityRecordV5):
    def validate(self) -> None:
        if self.terminal_disposition != "MODEL_TRAINING_COVERAGE_ABSTENTION":
            super().validate()
            return
        if (
            not self.opportunity_id or self.market not in v5.MARKETS
            or self.checkpoint not in v5.CHECKPOINTS or self.prediction_produced
            or self.order_submitted_at_ns is not None or self.fill_at_ns is not None
            or type(self.feature_event_at_ns) is not int
            or type(self.feature_available_at_ns) is not int
            or self.feature_event_at_ns > self.feature_available_at_ns
            or self.feature_available_at_ns > self.decision_at_ns
        ):
            raise IntegrityError("V9 model-training abstention is inconsistent")


def prepare_v8_retirement_v9(*, root: Path) -> PreparedV8RetirementV9:
    preparation = _load(root / V8_RETIREMENT_PREPARATION)
    registry = _load(root / V8_REGISTRY)
    event = _load(root / V8_EVENT)
    bindings = registry.get("bindings")
    if (
        preparation.get("trial_id") != V8_TRIAL_ID
        or preparation.get("disposition")
        != "INVALID_PARTIAL_MODEL_FIT_MARKET_FOLD_COVERAGE_EXCEPTION"
        or preparation.get("historical_source_rows_opened") is not True
        or preparation.get("predictions_generated") is not False
        or registry.get("trial_id") != V8_TRIAL_ID
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or event.get("trial_id") != V8_TRIAL_ID
        or not isinstance(bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("V8 retirement preparation or registered bytes are invalid")
    preserved = dict(bindings)
    for path in (V8_REGISTRY, V8_EVENT, V8_EXECUTION_PLAN, V8_AUTHORIZATION_USE):
        preserved[path.as_posix()] = sha256_file(root / path)
    core = {**preparation, "preserved_v8_sha256": dict(sorted(preserved.items()))}
    return PreparedV8RetirementV9(sha256_json(core), core)


def prepare_v9_registration(*, root: Path) -> PreparedV9Registration:
    _, delta = load_v9_contract(root=root)
    retirement = prepare_v8_retirement_v9(root=root)
    registry = _load(root / V8_REGISTRY)
    prior_bindings = registry.get("bindings")
    sources = registry.get("source_bindings")
    if not isinstance(prior_bindings, dict) or not isinstance(sources, list):
        raise IntegrityError("V8 lineage is incomplete for V9 registration")
    bindings = dict(prior_bindings)
    new_paths = (
        V8_RETIREMENT_PREPARATION,
        V9_CONTRACT,
        V8_EXECUTION_PLAN,
        V8_AUTHORIZATION_USE,
        Path("src/futures_rebuild/tier1_bracket_v9.py"),
        Path("tests/test_tier1_bracket_v9.py"),
        Path("tests/conftest.py"),
        V8_REGISTRY,
        V8_EVENT,
    )
    bindings.update({path.as_posix(): sha256_file(root / path) for path in new_paths})
    source_binding_id = v5.source_binding_id_from_metadata_v5(sources)
    core = {
        "schema_version": "tier1_bracket_successor_v9_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": delta["classification"],
        "supersedes_v8_trial_id": V8_TRIAL_ID,
        "v8_retirement_record_id": retirement.record_id,
        "change_scope": "MARKET_FOLD_TRAINING_AND_TEST_COVERAGE_REPRESENTATION_ONLY",
        "inherited_v8_contract_sha256": delta["inherited_v8_contract_sha256"],
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
    return PreparedV9Registration(sha256_json(core), core)


def persist_v8_retirement_v9(
    *, root: Path, prepared: PreparedV8RetirementV9,
) -> dict[str, str]:
    if prepared.record_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V8 retirement identity is invalid")
    preserved = prepared.canonical_payload.get("preserved_v8_sha256")
    if not isinstance(preserved, dict) or any(
        sha256_file(root / path) != digest for path, digest in preserved.items()
    ):
        raise IntegrityError("preserved V8 bytes changed after retirement preparation")
    registry = V8_RETIREMENT_REGISTRY_ROOT / f"{prepared.record_id}.json"
    event = V8_RETIREMENT_EVENT_ROOT / f"{prepared.record_id}.json"
    registry_path, event_path = root / registry, root / event
    if registry_path.exists() or event_path.exists():
        raise IntegrityError("V8 retirement publication is create-only")
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    event_path.parent.mkdir(parents=True, exist_ok=True)
    with registry_path.open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.canonical_payload,
            "state": "RETIRED_INVALID_AFTER_SOURCE_ACCESS_BEFORE_PREDICTIONS",
        }) + b"\n")
    with event_path.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_bracket_v8_retirement_event/1.0.0",
            "event_type": "RETIRED", "trial_id": V8_TRIAL_ID,
            "record_id": prepared.record_id,
        }) + b"\n")
    return {
        "record_id": prepared.record_id,
        "registry_path": registry.as_posix(), "event_path": event.as_posix(),
    }


def persist_v9_registration(
    *, root: Path, prepared: PreparedV9Registration,
) -> dict[str, str]:
    if prepared.trial_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V9 trial identity is invalid")
    bindings = prepared.canonical_payload.get("bindings")
    if not isinstance(bindings, dict) or any(
        sha256_file(root / path) != digest for path, digest in bindings.items()
    ):
        raise IntegrityError("V9 registration binding changed after preparation")
    retirement_id = prepared.canonical_payload.get("v8_retirement_record_id")
    if not v5._hex64(retirement_id):
        raise IntegrityError("V9 registration lacks a V8 retirement identity")
    retirement = _load(root / V8_RETIREMENT_REGISTRY_ROOT / f"{retirement_id}.json")
    if (
        retirement.get("state") != "RETIRED_INVALID_AFTER_SOURCE_ACCESS_BEFORE_PREDICTIONS"
        or sha256_json({
            **retirement, "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        }) != retirement_id
    ):
        raise IntegrityError("published V8 retirement is absent or inconsistent")
    registry = V9_REGISTRY_ROOT / f"{prepared.trial_id}.json"
    event = V9_EVENT_ROOT / f"{prepared.trial_id}.json"
    registry_path, event_path = root / registry, root / event
    if registry_path.exists() or event_path.exists():
        raise IntegrityError("V9 registration publication is create-only")
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
            "schema_version": "tier1_bracket_successor_v9_event/1.0.0",
            "event_type": "DECLARED", "trial_id": prepared.trial_id,
            "source_row_access": False, "model_fit": False,
            "prediction_generation": False, "historical_evaluation": False,
            "holdout_or_forward_access": False,
        }) + b"\n")
    return {
        "trial_id": prepared.trial_id,
        "registry_path": registry.as_posix(), "event_path": event.as_posix(),
    }


def fit_predict_v9(
    *, rows: Sequence[v5.MaterializedRowV5], folds: Sequence[v4.FoldSpec],
) -> ModelFitResultV9:
    if len(folds) != 8 or {fold.outer_fold for fold in folds} != set(range(8)):
        raise IntegrityError("V9 requires exactly eight outer folds")
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
    predictions: list[v8.FrozenPredictionV8] = []
    unavailable: list[str] = []
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
                and row.features is not None and row.ledger.prediction_produced
            ]
            if not testing:
                models.append({
                    "outer_fold": fold.outer_fold, "market": market,
                    "status": "NO_TEST_PREDICTION_ROWS_NO_FIT",
                    "training_rows": len(training), "testing_rows": 0,
                    "training_outcome_exclusions": len(training_all) - len(training),
                })
                continue
            if not training:
                ids = sorted(row.expected.opportunity_id for row in testing)
                unavailable.extend(ids)
                models.append({
                    "outer_fold": fold.outer_fold, "market": market,
                    "status": "NO_TRAINING_OUTCOMES_EXPLICIT_PREDICTION_ABSTENTION",
                    "training_rows": 0, "testing_rows": len(testing),
                    "model_unavailable_opportunity_ids": ids,
                    "training_outcome_exclusions": len(training_all),
                })
                continue
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
                raise IntegrityError("V9 Ridge fit is singular") from exc
            checkpoint_means: dict[str, dict[str, object]] = {}
            for checkpoint in v5.CHECKPOINTS:
                subset = [row for row in training if row.expected.checkpoint == checkpoint]
                if not subset:
                    checkpoint_means[checkpoint] = {
                        "status": "NO_TRAINING_OUTCOME_ABSTAIN", "direction": None,
                        "score": None, "training_rows": 0,
                    }
                    continue
                long_mean = fmean(v4._stress_r(row, "long") for row in subset)  # type: ignore[arg-type]
                short_mean = fmean(v4._stress_r(row, "short") for row in subset)  # type: ignore[arg-type]
                direction = "long" if long_mean >= short_mean else "short"
                checkpoint_means[checkpoint] = {
                    "status": "ESTIMATED", "direction": direction,
                    "score": max(long_mean, short_mean), "training_rows": len(subset),
                }
            models.append({
                "outer_fold": fold.outer_fold, "market": market, "status": "FITTED",
                "training_opportunity_ids": sorted(row.expected.opportunity_id for row in training),
                "training_rows": len(training), "testing_rows": len(testing),
                "training_outcome_exclusions": len(training_all) - len(training),
                "feature_center": center.tolist(), "feature_population_std": scale.tolist(),
                "constant_training_features_scaled_to_one": constant_features,
                "long_coefficients": long_coef.tolist(), "short_coefficients": short_coef.tolist(),
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
                predictions.append(v8.FrozenPredictionV8(
                    row.expected.opportunity_id, market, row.expected.year,
                    row.expected.exchange_session_date, row.expected.checkpoint,
                    fold.outer_fold, long_value, short_value, direction, selected,
                    str(baseline["direction"]) if baseline["direction"] is not None else None,
                    float(baseline["score"]) if baseline["score"] is not None else None,
                    float(row.features["bar_return_1"]),
                ))
    predicted_ids = [item.opportunity_id for item in predictions]
    unavailable_ids = tuple(sorted(unavailable))
    if (
        len(predicted_ids) != len(set(predicted_ids))
        or len(unavailable_ids) != len(set(unavailable_ids))
        or set(predicted_ids).intersection(unavailable_ids)
    ):
        raise IntegrityError("V9 prediction and model-abstention identities overlap or duplicate")
    expected_ids = {
        row.expected.opportunity_id for row in rows
        if row.ledger.prediction_produced and row.features is not None
        and row.expected.exchange_session_date in test_owners
    }
    if set(predicted_ids) | set(unavailable_ids) != expected_ids:
        raise IntegrityError("V9 prediction plus model-abstention coverage does not reconcile")
    core: dict[str, object] = {
        "schema_version": "tier1_bracket_successor_v9_models/1.0.0",
        "feature_names": list(v4.FEATURE_NAMES),
        "model_family": "MARKET_SPECIFIC_TWO_TARGET_RIDGE", "ridge_penalty": 1.0,
        "training_only_standardization": True,
        "available_cell_candidate_math": "FROZEN_IDENTICAL_TO_V8",
        "empty_test_cell": "RECORDED_NO_FIT",
        "empty_training_cell": "EXPLICIT_PREDICTION_ABSTENTION_NO_FALLBACK",
        "fold_local_empty_cell": "EXPLICIT_BASELINE_ABSTENTION",
        "pre_prediction_risk_cap_usd": "250", "source_eligibility": "DISPOSITION_GATED",
        "training_statistic_lineage": "NESTED_CROSSFIT_REQUIRED_FOR_POWER",
        "models": models,
        "model_unavailable_opportunity_ids": list(unavailable_ids),
    }
    return ModelFitResultV9(
        {**core, "model_bundle_id": sha256_json(core)},
        tuple(sorted(predictions, key=lambda item: (item.session, item.checkpoint, item.market))),
        exclusions, unavailable_ids,
    )


def apply_model_unavailable_abstentions_v9(
    *, rows: Sequence[v5.MaterializedRowV5], opportunity_ids: Sequence[str],
) -> tuple[v5.MaterializedRowV5, ...]:
    unavailable = set(opportunity_ids)
    if len(unavailable) != len(opportunity_ids):
        raise IntegrityError("V9 model-unavailable identities are duplicated")
    observed: set[str] = set()
    output: list[v5.MaterializedRowV5] = []
    for row in rows:
        if row.expected.opportunity_id not in unavailable:
            output.append(row)
            continue
        if not row.ledger.prediction_produced or row.features is None:
            raise IntegrityError("V9 model abstention is not feature-eligible")
        observed.add(row.expected.opportunity_id)
        ledger = OpportunityRecordV9(
            opportunity_id=row.ledger.opportunity_id,
            market=row.ledger.market,
            exchange_session_date=row.ledger.exchange_session_date,
            checkpoint=row.ledger.checkpoint,
            decision_at_ns=row.ledger.decision_at_ns,
            terminal_disposition="MODEL_TRAINING_COVERAGE_ABSTENTION",
            prediction_produced=False,
            feature_event_at_ns=row.ledger.feature_event_at_ns,
            feature_available_at_ns=row.ledger.feature_available_at_ns,
            order_submitted_at_ns=None,
            fill_at_ns=None,
            outcome_coverage=row.ledger.outcome_coverage,
        )
        ledger.validate()
        output.append(replace(row, ledger=ledger))
    if observed != unavailable:
        raise IntegrityError("V9 model abstentions do not reconcile to materialized rows")
    return tuple(output)


def evaluate_crossfit_model_availability_v9(
    *, rows: Sequence[v5.MaterializedRowV5], unavailable_ids: Sequence[str],
) -> dict[str, object]:
    expected = CounterByMarket({market: 0 for market in v5.MARKETS})
    statistic_eligible = CounterByMarket({market: 0 for market in v5.MARKETS})
    unavailable_by_market = {market: 0 for market in v5.MARKETS}
    by_id = {row.expected.opportunity_id: row for row in rows}
    if len(by_id) != len(rows):
        return {"status": "INVALID", "passed": False}
    for row in rows:
        if row.ledger.terminal_disposition != "CALENDAR_CLOSED":
            expected[row.expected.market] += 1
            statistic_eligible[row.expected.market] += int(row.ledger.prediction_produced)
    for opportunity_id in unavailable_ids:
        row = by_id.get(opportunity_id)
        if row is None or not row.ledger.prediction_produced:
            return {"status": "INVALID", "passed": False}
        unavailable_by_market[row.expected.market] += 1
    if any(expected[market] <= 0 for market in v5.MARKETS):
        return {"status": "INVALID", "passed": False}
    model_available = {
        market: statistic_eligible[market] - unavailable_by_market[market]
        for market in v5.MARKETS
    }
    statistic_rates = {
        market: statistic_eligible[market] / expected[market] for market in v5.MARKETS
    }
    model_rates = {
        market: (
            model_available[market] / statistic_eligible[market]
            if statistic_eligible[market] else 0.0
        )
        for market in v5.MARKETS
    }
    statistic_overall = sum(statistic_eligible.values()) / sum(expected.values())
    model_overall = (
        sum(model_available.values()) / sum(statistic_eligible.values())
        if sum(statistic_eligible.values()) else 0.0
    )
    passed = (
        statistic_overall >= 0.95 and min(statistic_rates.values()) >= 0.90
        and model_overall >= 0.99 and min(model_rates.values()) >= 0.90
    )
    return {
        "status": "PASS" if passed else "INCONCLUSIVE_DATA_OR_POWER",
        "passed": passed,
        "calendar_open_expected_opportunities": sum(expected.values()),
        "statistic_eligible_opportunities": sum(statistic_eligible.values()),
        "overall_statistic_eligibility_rate": statistic_overall,
        "minimum_overall_statistic_eligibility_rate": 0.95,
        "market_statistic_eligibility_rates": dict(sorted(statistic_rates.items())),
        "minimum_each_market_statistic_eligibility_rate": 0.90,
        "model_available_opportunities": sum(model_available.values()),
        "overall_model_availability_rate": model_overall,
        "minimum_overall_model_availability_rate": 0.99,
        "market_model_availability_rates": dict(sorted(model_rates.items())),
        "minimum_each_market_model_availability_rate": 0.90,
    }


class CounterByMarket(dict[str, int]):
    pass


def build_nested_crossfit_evidence_v9(
    *, rows: Sequence[v5.MaterializedRowV5],
) -> CrossfitEvidenceV9:
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
    model = fit_predict_v9(rows=crossfit_rows, folds=folds)
    availability_rows = tuple(
        row for row in crossfit_rows if row.expected.exchange_session_date in owners
    )
    availability = evaluate_crossfit_model_availability_v9(
        rows=availability_rows,
        unavailable_ids=model.model_unavailable_opportunity_ids,
    )
    adjusted = apply_model_unavailable_abstentions_v9(
        rows=crossfit_rows, opportunity_ids=model.model_unavailable_opportunity_ids,
    )
    evaluation = v8.evaluate_strategies_v8(
        predictions=model.predictions, rows=adjusted,
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
    base = v5.CrossfitEvidenceBundleV5(
        ordered_sessions, tuple(owners[session] for session in ordered_sessions),
        differential, sleeve_returns,
    )
    return CrossfitEvidenceV9(base, availability)


def derive_v9_decision(
    *, evaluation: Mapping[str, Mapping[str, v5.AccountPathV5]],
    evaluation_sessions: Sequence[str], coverage: v5.CoverageEvidence,
    baseline_coverage: Mapping[str, object], crossfit: CrossfitEvidenceV9,
    seed: int,
) -> dict[str, object]:
    inherited = v8.derive_v8_decision(
        evaluation=evaluation, evaluation_sessions=evaluation_sessions,
        coverage=coverage, baseline_coverage=baseline_coverage,
        crossfit=crossfit.base, seed=seed,
    )
    core = dict(inherited)
    core.pop("decision_id", None)
    core["schema_version"] = "tier1_bracket_successor_v9_decision/1.0.0"
    core["nested_crossfit_model_availability"] = dict(crossfit.model_availability)
    if (
        inherited.get("classification") not in {"INVALID", "INCONCLUSIVE_DATA_OR_COVERAGE"}
        and crossfit.model_availability.get("status") != "PASS"
    ):
        core["classification"] = "INCONCLUSIVE_DATA_OR_POWER"
    return {**core, "decision_id": sha256_json(core)}


def run_v9_pipeline(
    *, streams: Mapping[tuple[str, int], Iterable[v5.V5SourceRecord]],
    census: Sequence[v5.CensusCheckpoint], contract: Mapping[str, object],
    trial_id: str, runtime_receipt: Mapping[str, object],
) -> v5.V5PipelineResult:
    if not v5._hex64(trial_id):
        raise IntegrityError("V9 pipeline requires a registered trial identity")
    folds = v5.build_v5_folds_from_census(census)
    prediction_sessions = tuple(session for fold in folds for session in fold.test_sessions)
    raw_rows = v5.materialize_v5_streams(
        streams=streams, census=census, contract=contract,
        prediction_scope_sessions=prediction_sessions,
    )
    model = fit_predict_v9(rows=raw_rows, folds=folds)
    rows = apply_model_unavailable_abstentions_v9(
        rows=raw_rows, opportunity_ids=model.model_unavailable_opportunity_ids,
    )
    evaluation = v8.evaluate_strategies_v8(
        predictions=model.predictions, rows=rows,
        strategies=v5.REQUIRED_ACTIVE_STRATEGIES_V5,
    )
    opportunity_metadata = {
        prediction.opportunity_id: (prediction.market, prediction.year)
        for prediction in model.predictions
    }
    segmented: dict[str, Mapping[str, v5.AccountPathV5]] = {}
    for strategy in v5.REQUIRED_ACTIVE_STRATEGIES_V5:
        plan = v8.plan_strategy_v8(
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
    baseline_coverage = v8.evaluate_required_baseline_coverage_v8(model.predictions)
    crossfit = build_nested_crossfit_evidence_v9(rows=rows)
    seed = int(trial_id[:16], 16)
    decision = derive_v9_decision(
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
            "crossfit_sessions": list(crossfit.base.sessions),
            "crossfit_fold_ids": list(crossfit.base.fold_ids),
            "coverage": coverage_result,
            "required_baseline_coverage": baseline_coverage,
            "nested_crossfit_model_availability": crossfit.model_availability,
            "outer_model_unavailable_opportunity_ids": list(
                model.model_unavailable_opportunity_ids
            ),
        },
        decision=decision, runtime_receipt=runtime_receipt,
    )
    v5.build_evidence_manifest_v5(trial_id=trial_id, artifacts=artifacts)
    # V5PipelineResult's model annotation predates V9, but the immutable fields
    # are a strict superset and are consumed structurally.
    return v5.V5PipelineResult(  # type: ignore[arg-type]
        rows, model, evaluation, segmented, crossfit.base, coverage_result,
        decision, artifacts,
    )


def verify_historical_operation_receipt_v9(
    *, boundary: RepoBoundary, receipt: OperationReceipt, trial_id: str,
    source_binding_id: str, output_root: Path,
) -> str:
    if not v5._hex64(trial_id) or not v5._hex64(source_binding_id):
        raise UnauthorizedOperation("V9 historical receipt scope is invalid")
    boundary.assert_active_path(output_root.absolute(), purpose="V9 historical output root")
    required = {
        "trial_id": trial_id, "source_binding_id": source_binding_id,
        "output_root": output_root.as_posix(), "holdout_or_forward_access": "false",
        "provider_access": "false", "publication": "false",
    }
    receipt.verify(
        boundary, operation="EXECUTE_TIER1_BRACKET_SUCCESSOR_V9_HISTORICAL_SCREEN",
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
    )
    observed = dict(receipt.scope)
    approval = {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    if set(observed) != set(required) | approval or any(
        observed.get(key) != value for key, value in required.items()
    ):
        raise UnauthorizedOperation("V9 receipt does not grant the exact historical scope")
    if not receipt.single_use or not receipt.externally_authorized:
        raise UnauthorizedOperation("V9 execution requires single-use external authority")
    return receipt.receipt_id


def claim_historical_operation_receipt_v9(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_binding_id: str, output_root: Path,
) -> Path:
    receipt_id = verify_historical_operation_receipt_v9(
        boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_binding_id=source_binding_id, output_root=output_root,
    )
    claim = root / "state/authorization_uses" / f"{receipt_id}.json"
    boundary.assert_active_path(
        claim.absolute(), purpose="V9 authorization use", subtree="state/authorization_uses",
    )
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        with claim.open("xb") as stream:
            stream.write(canonical_bytes({
                "schema_version": "tier1_bracket_v9_authorization_use/1.0.0",
                "receipt_id": receipt_id, "trial_id": trial_id,
                "source_binding_id": source_binding_id,
                "output_root": output_root.as_posix(),
                "holdout_or_forward_access": False,
            }) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("V9 historical receipt was already consumed") from exc
    return claim


def authorized_source_streams_v9(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path], output_root: Path,
) -> tuple[
    Mapping[tuple[str, int], Iterator[v5.V5SourceRecord]],
    Mapping[tuple[str, int], v6.SourceIntegrityAuditV6],
]:
    if any(year == 2025 for _, year in source_paths):
        raise UnauthorizedOperation("2025 holdout path is rejected before open")
    registry = _load(root / V9_REGISTRY_ROOT / f"{trial_id}.json")
    bindings = registry.get("bindings")
    if (
        registry.get("trial_id") != trial_id
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or registry.get("holdout_or_forward_access") is not False
        or not isinstance(bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
    ):
        raise UnauthorizedOperation("registered V9 declaration is unavailable or drifted")
    require_locked_repository_environment(root)
    raw = registry.get("source_bindings")
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise IntegrityError("registered V9 source bindings are absent")
    binding_id = v5.source_binding_id_from_metadata_v5(raw)
    expected = {
        (str(item["market"]), int(item["year"])): str(item["source_parquet_sha256"])
        for item in raw
    }
    if registry.get("source_binding_id") != binding_id or set(source_paths) != set(expected):
        raise IntegrityError("V9 source binding or path map is inconsistent")
    claim_historical_operation_receipt_v9(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_binding_id=binding_id, output_root=output_root,
    )
    for key, path in source_paths.items():
        if sha256_file(path) != expected[key]:
            raise IntegrityError("V9 source bytes differ from registration")
    audits = {key: v6.SourceIntegrityAuditV6(key[0]) for key in sorted(source_paths)}
    streams = {
        key: v6.iter_source_records_from_parquet_v6(
            market=key[0], path=source_paths[key], audit=audits[key],
        )
        for key in sorted(source_paths)
    }
    return streams, audits


def execute_authorized_v9(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path], output_root: Path,
) -> v6.V6PipelineResult:
    streams, audits = authorized_source_streams_v9(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_paths=source_paths, output_root=output_root,
    )
    registry = _load(root / V9_REGISTRY_ROOT / f"{trial_id}.json")
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(registry["calendar_release_id"]),
    )
    inherited, _ = load_v9_contract(root=root)
    base = run_v9_pipeline(
        streams=streams, census=v5.build_expected_census_from_calendar(sessions=sessions),
        contract=inherited, trial_id=trial_id,
        runtime_receipt=v5.prepare_runtime_receipt_v5(root=root, trial_id=trial_id),
    )
    audit_payload = {
        f"{market}/{year}": audit.as_dict()
        for (market, year), audit in sorted(audits.items())
    }
    return v6.V6PipelineResult(base, audit_payload)


def build_evidence_manifest_v9(
    *, trial_id: str, result: v6.V6PipelineResult,
) -> dict[str, object]:
    payloads = v6._evidence_payloads_v6(result)
    if not result.base.evidence.predictions or not result.base.evidence.opportunity_ledger:
        raise IntegrityError("V9 evidence lacks frozen predictions or opportunity rows")
    files = {
        f"{name}.json": sha256_bytes(canonical_bytes({"payload": payload}) + b"\n")
        for name, payload in sorted(payloads.items())
    }
    core = {
        "schema_version": "tier1_bracket_successor_v9_evidence_manifest/1.0.0",
        "trial_id": trial_id, "files": files,
    }
    return {**core, "manifest_id": sha256_json(core)}


def persist_evidence_bundle_v9(
    *, boundary: RepoBoundary, output_root: Path, trial_id: str,
    result: v6.V6PipelineResult,
) -> dict[str, str]:
    manifest = build_evidence_manifest_v9(trial_id=trial_id, result=result)
    boundary.assert_active_path(output_root.absolute(), purpose="V9 evidence output root")
    destination = output_root / trial_id / str(manifest["manifest_id"])
    if destination.exists():
        raise IntegrityError("V9 evidence publication is create-only")
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
            raise IntegrityError("persisted V9 evidence hash mismatch")
    manifest_path = staging / "manifest.json"
    with manifest_path.open("xb") as stream:
        stream.write(canonical_bytes(manifest) + b"\n")
    if destination.exists():
        raise IntegrityError("V9 evidence destination appeared during publication")
    staging.replace(destination)
    final_manifest = destination / "manifest.json"
    return {
        "manifest_id": str(manifest["manifest_id"]),
        "manifest_path": final_manifest.as_posix(),
        "manifest_sha256": sha256_file(final_manifest),
    }
