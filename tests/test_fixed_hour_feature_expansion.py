from __future__ import annotations

import inspect
import json
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest

from futures_rebuild.canonical import canonical_bytes
from futures_rebuild.errors import IntegrityError
from futures_rebuild.fixed_hour_feature_expansion import (
    CORE_FEATURES,
    AUDIT_TRADE_DATES,
    EXPANSION_CANDIDATES,
    ExpansionFold,
    Tier0Dataset,
    discover_fold_features,
    frozen_feature_foundation_contract,
    select_expansion_candidate,
    select_fold_features,
    run_tier0_wfa_builder,
    run_synthetic_tier0_wfa_builder,
    optional_feature_stability,
)
from futures_rebuild.fixed_hour_multifamily_discovery import FEATURE_NAMES
from futures_rebuild.fixed_hour_outcome_foundation import frozen_outcome_foundation_contract


pytestmark = pytest.mark.current
ROOT = Path(__file__).resolve().parents[1]


def _fold(index: int) -> ExpansionFold:
    rng = np.random.default_rng(700 + index)
    fit_x = rng.normal(size=(252, len(FEATURE_NAMES)))
    audit_x = rng.normal(size=(63, len(FEATURE_NAMES)))
    core_index = FEATURE_NAMES.index("close_change_ticks_20")
    expanded = [FEATURE_NAMES.index(name) for name in EXPANSION_CANDIDATES[:4]]
    fit_signal = 8.0 * fit_x[:, core_index] + sum(
        (4.0 - position * 0.5) * fit_x[:, column]
        for position, column in enumerate(expanded)
    )
    audit_signal = 8.0 * audit_x[:, core_index] + sum(
        (4.0 - position * 0.5) * audit_x[:, column]
        for position, column in enumerate(expanded)
    )
    fit_y = np.column_stack((fit_signal, 12.0 + fit_signal, 12.0 - fit_signal))
    audit_y = np.column_stack((audit_signal, 12.0 + audit_signal, 12.0 - audit_signal))
    first = date(2020, 1, 1) + timedelta(days=index * 100)
    fit_dates = tuple(
        (first + timedelta(days=row)).isoformat() for row in range(252)
    )
    audit_dates = tuple(
        (first + timedelta(days=253 + row)).isoformat() for row in range(63)
    )
    markets = ("ES",)
    return ExpansionFold(
        fold_id=f"inner-{index}",
        fit_sample_ids=tuple(f"fit-{index}-{row:03d}" for row in range(252)),
        audit_sample_ids=tuple(f"audit-{index}-{row:03d}" for row in range(63)),
        fit_trade_dates=fit_dates,
        audit_trade_dates=audit_dates,
        fit_market_ids=("ES",) * 252,
        audit_market_ids=("ES",) * 63,
        audit_stress_cost_ticks=np.asarray(
            [4.0 for _ in range(63)]
        ),
        fit_features=fit_x,
        fit_targets=fit_y,
        audit_features=audit_x,
        audit_targets=audit_y,
    )


def test_restart_contract_freezes_five_family_core_and_nine_way_expansion() -> None:
    contract = frozen_feature_foundation_contract()
    assert sum(len(names) for names in contract["core_library"].values()) == 5
    assert set(name for names in contract["core_library"].values() for name in names) == set(CORE_FEATURES)
    assert set(contract["expanded_feature_names"]) == set(EXPANSION_CANDIDATES)
    assert contract["stage_order"] == [
        "BASELINE_FEATURES",
        "BOUNDED_PREDEFINED_FEATURE_RANKING",
        "FEATURE_SELECTION",
        "FREEZE_FEATURE_PROCEDURE",
    ]
    assert contract["feature_discovery"]["attempt_budget"] == 13
    assert contract["outcome_foundation_contract_id"] == frozen_outcome_foundation_contract()["contract_id"]
    assert contract["candidate_budget"] == len(contract["candidates"]) == 9
    assert contract["selector"]["fit_scope"] == "INNER_TRAIN_ONLY"
    assert contract["selector"]["objective"] == (
        "SESSION_AND_MARKET_BALANCED_STRESS_NET_TICKS_THEN_TARGET_MSE"
    )
    assert contract["stability"] == {
        "metric": "MEAN_PAIRWISE_JACCARD",
        "threshold": 0.6,
        "feature_names": list(EXPANSION_CANDIDATES),
        "failure_action": "FAIL_PROMOTION",
    }
    assert not any(contract["authority"].values())

    path = ROOT / "configs/fixed_hour_feature_expansion_foundation_v3.json"
    published = json.loads(path.read_text(encoding="utf-8"))
    assert published == contract
    assert path.read_bytes() == canonical_bytes(contract) + b"\n"


def test_expansion_selection_records_every_fold_and_fit_sample() -> None:
    folds = tuple(_fold(index) for index in range(4))
    result = select_expansion_candidate(folds)
    assert result["state"] == "PASS_SYNTHETIC_SELECTION_MECHANICS_ONLY"
    assert result["stability"]["passed"] is True
    assert len(result["candidates"]) == 9
    for candidate in result["candidates"]:
        assert len(candidate["folds"]) == 4
        for source, recorded in zip(folds, candidate["folds"], strict=True):
            assert recorded["fit_sample_ids"] == list(source.fit_sample_ids)
            assert recorded["audit_sample_ids"] == list(source.audit_sample_ids)
            assert recorded["fit_trade_dates"] == list(source.fit_trade_dates)
            assert recorded["audit_trade_dates"] == list(source.audit_trade_dates)
            assert set(recorded["discovered_feature_names"]) == set(EXPANSION_CANDIDATES)
            assert set(CORE_FEATURES) <= set(recorded["selected_feature_names"])


def test_feature_choice_uses_fit_rows_only() -> None:
    fold = _fold(0)
    assert set(inspect.signature(discover_fold_features).parameters) == {
        "fit_features", "fit_targets",
    }
    assert set(inspect.signature(select_fold_features).parameters) == {
        "fit_features", "discovered_feature_names", "maximum_added_features",
    }
    discovered = discover_fold_features(fold.fit_features, fold.fit_targets)
    selected = select_fold_features(
        fold.fit_features, discovered, maximum_added_features=4
    )
    assert selected == select_fold_features(
        fold.fit_features, discovered, maximum_added_features=4
    )
    with pytest.raises(IntegrityError, match="outside the frozen search"):
        select_fold_features(fold.fit_features, discovered, maximum_added_features=3)

    constant = fold.fit_features.copy()
    constant[:, FEATURE_NAMES.index(EXPANSION_CANDIDATES[0])] = 1.0
    reordered = (EXPANSION_CANDIDATES[0],) + tuple(
        name for name in EXPANSION_CANDIDATES if name != EXPANSION_CANDIDATES[0]
    )
    assert EXPANSION_CANDIDATES[0] not in select_fold_features(
        constant, reordered, maximum_added_features=4,
    )


def test_redundancy_threshold_and_market_pooling_fail_closed() -> None:
    fold = _fold(0)
    lookup = {name: FEATURE_NAMES.index(name) for name in FEATURE_NAMES}
    x = fold.fit_features.copy()
    base = np.linspace(-1.0, 1.0, len(x))
    orthogonal = np.sin(np.linspace(0.0, 8.0 * np.pi, len(x)))
    orthogonal -= np.dot(orthogonal, base) / np.dot(base, base) * base
    orthogonal /= np.std(orthogonal)
    base /= np.std(base)
    candidate_name = EXPANSION_CANDIDATES[0]
    x[:, lookup[CORE_FEATURES[0]]] = base
    x[:, lookup[candidate_name]] = 0.8999 * base + np.sqrt(1 - 0.8999**2) * orthogonal
    ordered = (candidate_name,) + tuple(name for name in EXPANSION_CANDIDATES if name != candidate_name)
    assert candidate_name in select_fold_features(x, ordered, maximum_added_features=4)
    x[:, lookup[candidate_name]] = base
    assert candidate_name not in select_fold_features(x, ordered, maximum_added_features=4)

    with pytest.raises(IntegrityError, match="one market"):
        ExpansionFold(
            **{**fold.__dict__, "fit_market_ids": ("NQ",) + fold.fit_market_ids[1:]}
        )

    short = replace(
        fold,
        audit_sample_ids=fold.audit_sample_ids[:-1],
        audit_trade_dates=fold.audit_trade_dates[:-1],
        audit_market_ids=fold.audit_market_ids[:-1],
        audit_stress_cost_ticks=fold.audit_stress_cost_ticks[:-1],
        audit_features=fold.audit_features[:-1],
        audit_targets=fold.audit_targets[:-1],
    )
    with pytest.raises(IntegrityError, match="exactly 63 audit"):
        select_expansion_candidate((short, _fold(1), _fold(2), _fold(3)))
    long = replace(
        fold,
        audit_sample_ids=(*fold.audit_sample_ids, "audit-0-extra"),
        audit_trade_dates=(*fold.audit_trade_dates, "2021-01-01"),
        audit_market_ids=(*fold.audit_market_ids, "ES"),
        audit_stress_cost_ticks=np.append(fold.audit_stress_cost_ticks, 4.0),
        audit_features=np.vstack((fold.audit_features, fold.audit_features[-1])),
        audit_targets=np.vstack((fold.audit_targets, fold.audit_targets[-1])),
    )
    with pytest.raises(IntegrityError, match="exactly 63 audit"):
        select_expansion_candidate((long, _fold(1), _fold(2), _fold(3)))
    with pytest.raises(IntegrityError, match="four inner folds"):
        select_expansion_candidate((_fold(0), _fold(1), _fold(2)))


def test_optional_feature_stability_failure_and_baseline_only_behavior() -> None:
    score, passed = optional_feature_stability([
        (*CORE_FEATURES, EXPANSION_CANDIDATES[0]),
        (*CORE_FEATURES, EXPANSION_CANDIDATES[1]),
    ])
    assert score == 0.0 and passed is False
    assert optional_feature_stability([CORE_FEATURES, CORE_FEATURES]) == (None, True)
    assert frozen_feature_foundation_contract()["stability"]["failure_action"] == "FAIL_PROMOTION"


def _tier0_dataset(date_count: int = 1010) -> Tier0Dataset:
    rng = np.random.default_rng(812)
    x = np.zeros((date_count, len(FEATURE_NAMES)), dtype=np.float64)
    x[:, FEATURE_NAMES.index(CORE_FEATURES[0])] = rng.normal(scale=0.05, size=date_count)
    x[:, FEATURE_NAMES.index(EXPANSION_CANDIDATES[0])] = rng.normal(size=date_count)
    signal = 8.0 * x[:, FEATURE_NAMES.index(EXPANSION_CANDIDATES[0])]
    y = np.column_stack((signal, 12.0 + signal, 12.0 - signal))
    dates = tuple((date(2018, 1, 1) + timedelta(days=index)).isoformat() for index in range(date_count))
    return Tier0Dataset(
        sample_ids=tuple(f"es-{index:04d}" for index in range(date_count)),
        trade_dates=dates,
        market_ids=("ES",) * date_count,
        features=x,
        targets=y,
        stress_cost_ticks=np.full(date_count, 1.0),
    )


def test_complete_tier0_builder_enforces_dates_and_never_uses_outer_targets() -> None:
    dataset = _tier0_dataset()
    result = run_tier0_wfa_builder(dataset)
    assert result["state"] == "BUILDER_COMPLETE_OUTER_OUTCOMES_UNSEEN"
    assert len(result["outer_folds"]) == 8
    assert all(len(fold["audit_trade_dates"]) == AUDIT_TRADE_DATES for fold in result["outer_folds"])
    assert len({sample for fold in result["outer_folds"] for sample in fold["audit_sample_ids"]}) == 8 * AUDIT_TRADE_DATES
    for fold in result["outer_folds"]:
        assert max(fold["fit_trade_dates"]) < fold["embargo_trade_date"] < min(fold["audit_trade_dates"])
        assert len(fold["inner_fold_manifests"]) == 4
        for inner in fold["inner_fold_manifests"]:
            assert len(inner["fit_trade_dates"]) >= 252
            assert len(inner["audit_trade_dates"]) == 63
            assert max(inner["fit_trade_dates"]) < inner["embargo_trade_date"] < min(inner["audit_trade_dates"])

    changed = dataset.targets.copy()
    changed[-AUDIT_TRADE_DATES:] += 1_000_000.0
    replay = run_tier0_wfa_builder(Tier0Dataset(
        dataset.sample_ids, dataset.trade_dates, dataset.market_ids,
        dataset.features, changed, dataset.stress_cost_ticks,
    ))
    assert [fold["selected_feature_names"] for fold in replay["outer_folds"]] == [fold["selected_feature_names"] for fold in result["outer_folds"]]
    assert [fold["predictions"] for fold in replay["outer_folds"]] == [fold["predictions"] for fold in result["outer_folds"]]

    with pytest.raises(IntegrityError, match="1010 complete trade dates"):
        run_tier0_wfa_builder(_tier0_dataset(1009))
    with pytest.raises(IntegrityError, match="ES-only"):
        Tier0Dataset(
            dataset.sample_ids, dataset.trade_dates, ("NQ",) + dataset.market_ids[1:],
            dataset.features, dataset.targets, dataset.stress_cost_ticks,
        )


def test_published_synthetic_tier0_builder_report_is_exact() -> None:
    result = run_synthetic_tier0_wfa_builder()
    path = (
        ROOT / "state/unpublished_evidence/fixed_hour_globex_tier0_synthetic"
        / str(result["run_id"]) / "builder.json"
    )
    assert json.loads(path.read_text(encoding="utf-8")) == result
    assert path.read_bytes() == canonical_bytes(result) + b"\n"
    assert result["state"] == "BUILDER_COMPLETE_OUTER_OUTCOMES_UNSEEN"
    assert not any(result["authority"].values())
