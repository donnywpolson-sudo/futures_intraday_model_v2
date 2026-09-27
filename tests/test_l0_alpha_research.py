from copy import deepcopy
import json
from types import SimpleNamespace

import numpy as np
import pytest

from futures_rebuild import l0_alpha_candidate as c
from futures_rebuild import l0_alpha_research as r
from futures_rebuild import l0_alpha_preparation as p
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation

pytestmark = pytest.mark.current


@pytest.fixture(scope="module")
def full_dataset():
    return p.synthetic_dataset("ES")


def test_equal_date_training_weights_and_no_trade_simplicity_ties():
    np.testing.assert_allclose(c.old.training_weights(["a", "a", "b"]), [.25, .25, .5])
    scores = [{**candidate, "score": 0., "status": "VALID"} for candidate in c.candidate_definitions()]
    selected = r.choose_candidate(scores)
    assert selected["policy"] == "no_trade" and selected["score"] == 0
    assert selected["candidate"]["candidate_id"] == "baseline:ridge=10"
    scores[0]["score"] = 1e-12
    assert r.choose_candidate(scores)["candidate"]["candidate_id"] == scores[0]["candidate_id"]
    assert r.choose_candidate(scores)["policy"] == "model"
    scores[0]["score"] = -1
    assert r.choose_candidate(scores)["policy"] == "no_trade"
    for item in scores:
        item["status"] = "INVALID_LEAF_SUPPORT"
    with pytest.raises(IntegrityError):
        r.choose_candidate(scores)


def test_date_compressed_splits_preserve_existing_purge_and_embargo(full_dataset):
    new = c.fold_schedule(full_dataset)
    old = c.old.fold_schedule(full_dataset)
    assert len(new) == len(old) == 8
    for actual, expected in zip(new, old, strict=True):
        assert actual["outer_window"] == expected.test_window
        np.testing.assert_array_equal(actual["fit"], expected.fit_indices)
        for inner, old_inner in zip(actual["inner"], expected.inner_folds, strict=True):
            np.testing.assert_array_equal(inner["fit"], old_inner.fit_indices)
            assert inner["window"] == old_inner.validation_window
    support = c.source_completeness(full_dataset)
    assert support["support_status"] == "SUPPORTED" and len(support["fold_support"]) == 40
    assert support["execution_status"] == "REFERENCE_ENDPOINTS_COMPLETE"
    assert support["source_certified"] is False


@pytest.mark.parametrize("family", ("ridge", "xgboost"))
def test_train_only_fit_reload_and_regularization(full_dataset, family):
    split = c.fold_schedule(full_dataset)[0]
    indices = split["fit"]
    candidate = next(v for v in c.candidate_definitions() if v["family"] == family and v["pack"] == "hour_context")
    model = r.fit_model(full_dataset, indices, candidate, fit_kind="outer")
    batch = r.forecast_batch(full_dataset, model, split["outer_window"])
    assert len(batch["predictions"]) == 63 * 330 and sum(p is not None for p in batch["predictions"]) == 63
    assert model["latest_label_ns"] < c.decision_times(batch["first_date"])[0]
    expected = r.predict(model, full_dataset.features[:100])
    np.testing.assert_array_equal(expected, r.predict(json.loads(json.dumps(model)), full_dataset.features[:100]))
    if family == "ridge":
        d = full_dataset.terminal_data
        w = c.old.training_weights([d.trade_dates[i] for i in indices])
        assert model["training_mean_ticks"] == float(w @ d.terminal_ticks[indices])
        x = d.features[indices][:, model["columns"]]
        np.testing.assert_allclose(model["means"], w @ x)
    else:
        assert model["parameters"]["lambda"] == .01 and model["parameters"]["min_child_weight"] == .05
        assert model["minimum_leaf_dates"] >= 20
    future = SimpleNamespace(**full_dataset.__dict__)
    future.terminal_data = SimpleNamespace(**full_dataset.terminal_data.__dict__)
    future.terminal_data.features = future.terminal_data.features.copy()
    future.terminal_data.terminal_ticks = future.terminal_data.terminal_ticks.copy()
    future.terminal_data.features[indices[-1] + 1:] += 100_000
    future.terminal_data.terminal_ticks[indices[-1] + 1:] -= 100_000
    refitted = r.fit_model(future, indices, candidate, fit_kind="outer")
    np.testing.assert_array_equal(expected, r.predict(refitted, full_dataset.features[:100]))
    changed = deepcopy(model)
    changed["feature_names"].reverse()
    changed = c.bind({k: v for k, v in changed.items() if k != "model_id"}, "model_id")
    with pytest.raises(IntegrityError, match="feature order"):
        r.predict(changed, full_dataset.features[:1])


def test_strict_label_maturity_and_observed_date_minima(full_dataset):
    changed = SimpleNamespace(**full_dataset.__dict__)
    changed.outcomes = [dict(o) for o in full_dataset.outcomes]
    first = c.fold_schedule(full_dataset)[0]
    i = first["fit"][-1]
    pindex = full_dataset.fit_positions[i]
    changed.outcomes[pindex]["known_at_ns"] = c.decision_times(full_dataset.calendar_dates[first["outer_window"].start])[0]
    assert i not in c.fold_schedule(changed)[0]["fit"]
    with pytest.raises(IntegrityError, match="requires 505"):
        r.fit_model(full_dataset, np.arange(504), c.candidate_definitions()[0], fit_kind="outer")


def test_checkpoint_reuse_tampering_interruption_and_containment(tmp_path):
    store = p.ResearchStore(tmp_path / "results", {"data": "synthetic"})
    calls = []
    def compute():
        calls.append(1)
        store.save("stage/model", {"weights": [1., 2.]})
        return {"score": 1.}
    assert store.checkpoint("stage", compute) == store.checkpoint("stage", compute)
    assert calls == [1]
    copied = store.path("different_stage/completed")
    copied.parent.mkdir()
    copied.write_bytes(store.path("stage/completed").read_bytes())
    with pytest.raises(IntegrityError, match="operation"):
        store.checkpoint("different_stage", compute)
    assert calls == [1]
    store.path("stage/model").write_text("{}")
    with pytest.raises(IntegrityError, match="changed"):
        store.checkpoint("stage", compute)
    def fail():
        raise RuntimeError("injected interruption")
    with pytest.raises(RuntimeError):
        store.checkpoint("interrupted", fail)
    with pytest.raises(IntegrityError, match="no duplicate"):
        store.checkpoint("interrupted", compute)
    with pytest.raises(IntegrityError, match="escapes"):
        store.save("../outside", {})
    bounded = p.ResearchStore(tmp_path / "bounded", {}, maximum_bytes=2)
    with pytest.raises(IntegrityError, match="budget"):
        bounded.save("large", {"data": 1})


def test_ledger_preserves_all_abstentions_without_materializing_duplicate_clock(full_dataset):
    saved = p.saved_dataset(full_dataset)
    covered = [row["index"] for row in saved["eligible"]]
    covered += [i for item in saved["abstention_runs"] for i in range(item["start"], item["stop"])]
    assert sorted(covered) == list(range(430650))
    assert len(saved["eligible"]) == 1305
    assert saved["evidence_id"] == full_dataset._evidence_id


def test_historical_reader_never_called_without_existing_gateway():
    calls = []
    with pytest.raises(UnauthorizedOperation, match="gateway"):
        p.execute_registered_market(object(), registration_path="unused", registration_sha256="a" * 64,
            receipt=None, market="ES", expected_dataset_id="b" * 64,
            source_contract_id="c" * 64, source_release_id="d" * 64, reader=lambda receipt: calls.append(1))
    assert calls == []


def test_no_trade_retains_fitted_artifact_and_outer_reselection_is_rejected(full_dataset, monkeypatch):
    # Zero forecasts exercise the genuine search, ledger and final-fit path.
    # The fit tests above independently check numerical forecasts and reload.
    monkeypatch.setattr(r, "predict", lambda model, features: np.zeros(len(features)))
    run = r.run_market_wfa(full_dataset, legacy=True)
    assert all(f["selected"]["policy"] == "no_trade" for f in run["outer_folds"])
    final = r.final_refit(full_dataset, run)
    assert final["policy"] == "no_trade" and final["model"]["training_rows"] == 1305
    assert final["model"]["candidate"]["candidate_id"] == "baseline:ridge=10" and not final["active"]
    assert r.evaluate_wfa(run)["outcome"] == "NO_PROMISING_STRATEGY"
    changed = deepcopy(run)
    changed["outer_folds"][-1]["selected"]["policy"] = "model"
    changed = c.bind({k: v for k, v in changed.items() if k != "run_id"}, "run_id")
    with pytest.raises(IntegrityError, match="inner selection"):
        r.final_refit(full_dataset, changed)
