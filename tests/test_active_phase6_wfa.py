from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.active_phase5_splits import FEATURE_METHOD, OUTCOME_METHOD, MARKETS, YEARS
from futures_rebuild.active_phase6_wfa import (
    Tier1Phase6Runner,
    _ridge_from_sufficient_statistics,
    prepare_phase6_prediction_only_trial,
    prepare_tier1_phase6_binding,
)
import numpy as np
from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _synthetic_root(tmp_path: Path) -> tuple[RepoBoundary, list[dict[str, object]]]:
    pairs: list[dict[str, object]] = []
    for market_index, market in enumerate(MARKETS):
        for year in YEARS:
            ordinal = market_index * len(YEARS) + year - YEARS[0]
            outcome_id = f"{ordinal + 1:064x}"
            feature_id = f"{ordinal + 101:064x}"
            source = f"{ordinal + 201:064x}"
            pair = {
                "feature_release_id": feature_id,
                "market": market,
                "outcome_release_id": outcome_id,
                "source_parquet_sha256": source,
                "year": year,
            }
            pairs.append(pair)
            outcome = tmp_path / "data" / "outcomes" / OUTCOME_METHOD / market / str(year) / str(year) / outcome_id / "outcomes.parquet"
            feature = tmp_path / "data" / "features" / FEATURE_METHOD / market / str(year) / str(year) / feature_id / "features.parquet"
            outcome.parent.mkdir(parents=True, exist_ok=True)
            feature.parent.mkdir(parents=True, exist_ok=True)
            outcome.touch()
            feature.touch()
            _write_json(tmp_path / "manifests" / "data_releases" / "outcomes" / f"{outcome_id}.json", {"source_parquet_sha256": source})
            _write_json(tmp_path / "manifests" / "data_releases" / "features" / f"{feature_id}.json", {"source_parquet_sha256": source})
    _write_json(
        tmp_path / "manifests" / "split_plans" / "tier1_core" / "synthetic.json",
        {"input_pairs": pairs, "outer_folds": [{"fold": index} for index in range(8)]},
    )
    return RepoBoundary(active_root=tmp_path), pairs


def test_phase6_preflight_binds_all_twenty_pairs_without_trial_state(tmp_path: Path) -> None:
    boundary, pairs = _synthetic_root(tmp_path)

    binding = prepare_tier1_phase6_binding(boundary=boundary)
    template = binding.trial_declaration_template()

    assert binding.outer_fold_count == 8
    assert list(binding.input_pairs) == pairs
    assert template["trial_registration"] == "NOT_REGISTERED"
    assert template["prediction_release"] == "NOT_CREATED"
    assert not (tmp_path / "state").exists()
    with pytest.raises(UnauthorizedOperation, match="separately approved"):
        Tier1Phase6Runner(binding).execute()


def test_phase6_preflight_rejects_a_phase5_pair_mismatch(tmp_path: Path) -> None:
    boundary, _ = _synthetic_root(tmp_path)
    manifest = tmp_path / "manifests" / "split_plans" / "tier1_core" / "synthetic.json"
    value = json.loads(manifest.read_text(encoding="utf-8"))
    value["input_pairs"][0]["feature_release_id"] = "f" * 64
    _write_json(manifest, value)

    with pytest.raises(IntegrityError, match="do not exactly bind"):
        prepare_tier1_phase6_binding(boundary=boundary)


def test_prediction_only_contract_requires_registration_before_outcome_open(tmp_path: Path) -> None:
    boundary, pairs = _synthetic_root(tmp_path)
    contract = prepare_phase6_prediction_only_trial(
        binding=prepare_tier1_phase6_binding(boundary=boundary)
    )

    declaration = contract.declaration()
    assert declaration["input_pairs"] == pairs
    assert declaration["prediction_only"] is True
    assert declaration["economics_evaluation"] is False
    assert declaration["holdout_or_forward_access"] is False
    with pytest.raises(UnauthorizedOperation, match="must precede"):
        contract.authorize_outcome_row_open()

    contract.register_in_memory()
    contract.authorize_outcome_row_open()
    assert contract.outcome_rows_opened is True
    with pytest.raises(UnauthorizedOperation, match="cannot register"):
        contract.register_in_memory()


def test_fixed_ridge_uses_an_unpenalized_intercept() -> None:
    x = np.asarray([[1.0, 0.0], [1.0, 1.0], [1.0, 2.0]], dtype=np.float64)
    y = np.asarray([1.0, 3.0, 5.0], dtype=np.float64)

    coefficients = _ridge_from_sufficient_statistics(x.T @ x, x.T @ y)

    assert coefficients.shape == (2,)
    assert coefficients[0] > 1.0
    assert 0.0 < coefficients[1] < 2.0
