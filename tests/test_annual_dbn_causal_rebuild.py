import math

from futures_rebuild.annual_dbn_causal_rebuild import _projection
from futures_rebuild.annual_dbn_causal_campaign import _market_attempt_id
from futures_rebuild.causal_observation_full_build import EXPECTED_WORK_UNIT_COUNT
from futures_rebuild.causal_observation_market_checkpoint import MARKET_ORDER


def test_runtime_projection_uses_complete_measured_market_evidence() -> None:
    evidence = [
        {"market": market, "elapsed_seconds": index + 1}
        for index, market in enumerate(MARKET_ORDER)
    ]
    projection = _projection(evidence, total_work_units=EXPECTED_WORK_UNIT_COUNT)
    observed = sum(range(1, len(MARKET_ORDER) + 1))
    assert projection["observed_elapsed_seconds"] == observed
    assert projection["projected_runtime_high_seconds"] == math.ceil(observed * 1.2)
    assert projection["completed_work_unit_count"] == EXPECTED_WORK_UNIT_COUNT


def test_market_attempt_identity_is_scoped_to_one_retryable_market() -> None:
    values = {
        "campaign_id": "a" * 64,
        "market": "ES",
        "ordinal": 1,
        "source_entries_sha256": "b" * 64,
        "predecessor_certificate_id": None,
        "reuse_failure_sha256": None,
    }
    first = _market_attempt_id(**values)
    assert first == _market_attempt_id(**values)
    assert first != _market_attempt_id(**{**values, "market": "NQ"})
    assert first != _market_attempt_id(**{**values, "ordinal": 2})
    assert first != _market_attempt_id(
        **{**values, "reuse_failure_sha256": "c" * 64}
    )
