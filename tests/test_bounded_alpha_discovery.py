from pathlib import Path

import pytest

from futures_rebuild.bounded_alpha_discovery import (
    CONTRACT_PATH, build_contract, build_synthetic_certificate,
    enumerate_candidates, select_candidate, validate_contract,
)
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.current


def test_bounded_discovery_freeze_is_exact_and_fail_closed() -> None:
    contract = build_contract(ROOT)
    assert validate_contract(ROOT, contract) == contract
    assert contract["market_progression"]["tier_0_discovery_market"] == "ES"
    assert contract["market_progression"]["required_independent_anchors"] == ["ES", "CL", "GC"]
    assert contract["market_progression"]["macro_diversification_claim"] is False
    assert contract["search"]["candidate_budget"] == 27
    assert enumerate_candidates() == contract["search"]["candidates"]
    assert contract["folds"]["outer_results_may_select_candidate"] is False
    assert contract["recent_regime_challenge"]["maximum_attempts"] == 1
    assert contract["final_252"]["access_allowed"] is False
    assert not any(contract["authority"].values())

    ids = [candidate["candidate_id"] for candidate in contract["search"]["candidates"]]
    scores = {candidate_id: (0.0, 0.0, 0.0) for candidate_id in ids}
    assert select_candidate(contract, scores) == ids[0]
    with pytest.raises(IntegrityError, match="cover all 27"):
        select_candidate(contract, dict(list(scores.items())[:-1]))


def test_published_contract_and_synthetic_certificate_are_reproducible() -> None:
    published = __import__("json").loads((ROOT / CONTRACT_PATH).read_bytes())
    assert validate_contract(ROOT, published) == published
    certificate = build_synthetic_certificate(ROOT, published)
    assert certificate["state"] == "PASS_SYNTHETIC_MECHANICS_ONLY"
    assert certificate["authority"] == {
        "historical_row_read": False, "trial_execution": False,
    }
