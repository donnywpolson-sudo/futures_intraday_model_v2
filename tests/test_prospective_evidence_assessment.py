import json
from pathlib import Path

import numpy as np
import pytest

from futures_rebuild import prospective_evidence_assessment as assessment
from futures_rebuild.errors import IntegrityError
from futures_rebuild.canonical import sha256_file, sha256_json

pytestmark = pytest.mark.current


def test_betting_kernel_matches_direct_products_and_mixture():
    scores = np.array([[.2] * 28, [-.8] * 28, [.9] * 28])
    state = np.zeros((28, 4))
    for row in scores:
        state, log_e = assessment.advance(state, row)
    direct = np.prod(1 + scores[:, :, None] * assessment.BETS, axis=0).mean(axis=-1)
    np.testing.assert_allclose(np.exp(log_e), direct, rtol=1e-14)
    assert not any(assessment.crossings(np.zeros((1008, 28)))["claim_crossings"].values())
    assert all(assessment.crossings(np.ones((1008, 28)))["claim_crossings"].values())
    assert assessment.crossings(np.ones((1008, 28)))["promotion_authorized"] is False


@pytest.mark.parametrize("bad", [np.full((1, 28), np.nan), np.ones((1, 28))*1.01,
    np.empty((0, 28)), np.ones((1009, 28)), np.ones((100, 27))])
def test_reject_invalid_campaign_scores(bad):
    with pytest.raises(IntegrityError): assessment.crossings(bad)


def test_scores_preserve_units_and_explicit_ridge_hurdle():
    net = np.full((2, 4), 20.)
    baseline = np.zeros((2, 4, 7))
    scores = assessment.bounded_scores(net, baseline).reshape(2, 4, 7)
    np.testing.assert_array_equal(scores[:, :, :6], np.full((2, 4, 6), .2))
    np.testing.assert_allclose(scores[:, :, 6], .1999)
    baseline[0, 0, 0] = np.nan
    with pytest.raises(IntegrityError): assessment.bounded_scores(net, baseline)


def test_positive_bounded_score_does_not_prove_uncapped_profit():
    net = np.full((100, 4), 20.); net[-1] = -10000.
    scores = assessment.bounded_scores(net, np.zeros((100, 4, 7)))
    assert net.mean() < 0 and scores.mean() > 0
    assert assessment.design()["uncapped_expected_profitability_claim"] is False


def test_unconditional_zero_mean_is_not_the_required_null():
    # One common random sign held forever has mean zero at each date but, after
    # date one, violates the conditional null in the positive branch. Exactly
    # half the paths reject: this is NOT a valid-null calibration scenario.
    positive = assessment.crossings(np.ones((1008, 28)))
    negative = assessment.crossings(-np.ones((1008, 28)))
    assert any(positive["claim_crossings"].values())
    assert not any(negative["claim_crossings"].values())
    assert "UNCONDITIONAL" in assessment.design()["serial_dependence_rule"]


def test_alpha_spending_and_conditional_null_identity():
    spec = assessment.design()
    assert sum(.05 / 2**i for i in range(1, 40)) < .05
    assert spec["claim_crossing_threshold"] == 28 / spec["campaign_alpha"]
    # Under a zero-mean +/-1 innovation every fixed bet has conditional expected
    # multiplier one, regardless of any past-predictable volatility scale.
    for scale in (.2, .75, 1.):
        np.testing.assert_array_equal(((1+assessment.BETS*scale)+(1-assessment.BETS*scale))/2, np.ones(4))
    assert not any(spec["authority"].values())


def test_frozen_synthetic_report():
    root = Path(__file__).resolve().parents[1]
    plan = json.loads((root / assessment.PLAN).read_bytes())
    report = json.loads((root / assessment.RESULT).read_bytes())
    assert plan["plan_id"] == sha256_json({k: v for k, v in plan.items() if k != "plan_id"})
    assert report["report_id"] == sha256_json({k: v for k, v in report.items() if k != "report_id"})
    assert all(sha256_file(root / path) == digest for path, digest in plan["source_hashes"].items())
    assert plan["design"] == assessment.design()
    assert report["plan_id"] == plan["plan_id"]
    assert set(report["scenarios"]) == set(assessment.SCENARIOS)
    assert all(set(r["horizons"]) == {"252", "504", "1008"} for r in report["scenarios"].values())
    assert all(r["horizons"]["1008"]["false_any_true_null_claim"]["replicates"] == 2000 for r in report["scenarios"].values())
    assert report["pipeline_qualification_ready"] is False
    assert not any(report["authority"].values())
