from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_frozen_quote_recovery_adequacy import (
    CausalBboObservation,
    QuoteCoverage,
    QuoteRecoveryTarget,
    adjudicate_quote_coverage,
    build_frozen_quote_target_specs,
    classify_quote_target,
    load_quote_recovery_adequacy_plan,
)
from futures_rebuild.tier1_frozen_quote_recovery_cost import load_diagnostic_record


ROOT = Path(__file__).resolve().parents[1]
SECOND = 1_000_000_000


def _target(*, category="ENTRY"):
    return QuoteRecoveryTarget("a" * 64, "b" * 64, "ES", category, 1_000 * SECOND, 11)


def _quote(*, available_at, instrument_id=11, bid=5_000, ask=5_001, ordinal=0):
    return CausalBboObservation(
        "b" * 64, "ES", instrument_id, available_at - 1,
        available_at, bid, ask, 2, 3, ordinal,
    )


def test_entry_uses_first_post_arrival_quote_never_a_pre_action_quote() -> None:
    target = _target()
    arrival, _ = target.window()
    coverage, selected = classify_quote_target(
        target=target,
        observations=(
            _quote(available_at=arrival - SECOND, ordinal=0),
            _quote(available_at=arrival + 2 * SECOND, ordinal=2),
            _quote(available_at=arrival + SECOND, ordinal=1),
        ),
    )
    assert coverage.status == "COMPLETE"
    assert selected is not None and selected.available_at_ns == arrival + SECOND
    assert set(coverage.as_price_free_dict()).isdisjoint({
        "bid_price_nano", "ask_price_nano", "bid_size", "ask_size",
    })


@pytest.mark.parametrize("fault", ["crossed", "empty", "foreign"])
def test_invalid_or_foreign_book_fails_closed(fault: str) -> None:
    target = _target()
    arrival, _ = target.window()
    kwargs = {"available_at": arrival}
    if fault == "crossed":
        kwargs.update(bid=5_001, ask=5_000)
    elif fault == "empty":
        quote = _quote(**kwargs)
        quote = CausalBboObservation(**{**quote.__dict__, "ask_size": 0})
        with pytest.raises(IntegrityError, match="two-sided book"):
            classify_quote_target(target=target, observations=(quote,))
        return
    else:
        kwargs.update(instrument_id=12)
    if fault == "crossed":
        with pytest.raises(IntegrityError, match="two-sided book"):
            classify_quote_target(target=target, observations=(_quote(**kwargs),))
    else:
        coverage, selected = classify_quote_target(
            target=target, observations=(_quote(**kwargs),),
        )
        assert coverage.status == "EXPLICIT_UNAVAILABLE" and selected is None


def test_liquidation_quote_after_five_minute_deadline_is_unavailable() -> None:
    target = _target(category="LIQUIDATION")
    _, deadline = target.window()
    coverage, selected = classify_quote_target(
        target=target, observations=(_quote(available_at=deadline + SECOND),),
    )
    assert coverage.status == "EXPLICIT_UNAVAILABLE" and selected is None


def test_frozen_33_target_gate_requires_every_quote_complete() -> None:
    coverage = [
        QuoteCoverage(
            f"{index:064x}", "b" * 64, "ES",
            "ENTRY" if index < 27 else "LIQUIDATION",
            "COMPLETE", None, 100 + index, index,
        )
        for index in range(33)
    ]
    expected = [item.opportunity_id for item in coverage]
    assert adjudicate_quote_coverage(
        coverage=coverage, expected_ids=expected,
    )["decision"] == "PASS"
    coverage[-1] = QuoteCoverage(
        **{**coverage[-1].__dict__, "status": "EXPLICIT_UNAVAILABLE", "reason": "missing"}
    )
    assert adjudicate_quote_coverage(
        coverage=coverage, expected_ids=expected,
    )["decision"] == "FAIL"


def test_all_33_diagnostic_targets_bind_once_to_the_30_frozen_queries() -> None:
    specs = build_frozen_quote_target_specs(
        diagnostic_record=load_diagnostic_record(root=ROOT),
    )
    assert len(specs) == len({item.opportunity_id for item in specs}) == 33
    assert len({item.query_id for item in specs}) == 30
    assert {item.category for item in specs} == {"ENTRY", "LIQUIDATION"}
    assert all(item.bind_causal_identity(11).expected_instrument_id == 11 for item in specs)


def test_quote_adequacy_plan_is_hash_bound_and_non_authorizing() -> None:
    plan = load_quote_recovery_adequacy_plan(root=ROOT)
    assert plan["plan_id"] == (
        "a17199f24248452c2b6adb3d4485d6d399d20df370b0452017ebf2ed0ad38099"
    )
    assert plan["adjudication"]["every_target_must_pass"] is True
    assert plan["adjudication"]["prices_published"] is False
    assert plan["adjudication"]["source_activated"] is False
    assert set(plan["forbidden_actions"].values()) == {True}
