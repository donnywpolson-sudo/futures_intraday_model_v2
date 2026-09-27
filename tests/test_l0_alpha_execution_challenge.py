from copy import deepcopy

import pytest

from futures_rebuild import l0_alpha_candidate as c
from futures_rebuild import l0_alpha_execution_challenge as l1
from futures_rebuild import l0_alpha_preparation as prep
from futures_rebuild.errors import IntegrityError

pytestmark = pytest.mark.current


def fixture(schema="BBO-1s", direction=1):
    at = c.decision_times("2024-01-02")[0] - c.DELAY + c.MINUTE
    final = c.bind({"mechanism_id": c.mechanism_id(), "market": "ES", "policy": "model",
                    "latest_label_ns": prep.AS_OF_NS}, "final_id")
    trade = {"forecast_id": "a" * 64, "market": "ES", "actual_contract": "ESH4", "trade_date": "2024-01-02",
             "entry_ns": at, "exit_ns": at + 60 * c.MINUTE, "direction": direction,
             "tick_nano": 250_000_000, "tick_value_nano_usd": 12_500_000_000}
    plan = l1.freeze_challenge(final, [trade], dates=["2024-01-02", "2024-01-03"], source_release_id="b" * 64,
                               schema=schema, forecast_ledger_sha256="c" * 64, frozen_at_ns=at + 24 * 60 * c.MINUTE)
    quotes = []
    for offset in (0, 1, 5):
        for side in ("entry", "exit"):
            at_ns = trade[side + "_ns"] + offset * l1.SECOND
            bid = (-10 if side == "entry" else -5) * trade["tick_nano"]
            quotes.append({"market": "ES", "actual_contract": "ESH4", "source_release_id": "b" * 64,
                "schema": schema, "ts_recv": at_ns, "interval_end_ns": at_ns,
                "bid_nano": bid, "ask_nano": bid + trade["tick_nano"], "bid_size": 1, "ask_size": 2,
                "quality_valid": True})
    return final, trade, plan, quotes


@pytest.mark.parametrize("direction,net", [(1, 40.), (-1, -85.)])
def test_side_correct_negative_quotes_fees_and_no_double_spread(direction, net):
    final, trade, plan, quotes = fixture(direction=direction)
    result = l1.evaluate_challenge(plan, final, [trade], quotes)
    for offset in ("0", "1", "5"):
        r = result["timing_offsets_seconds"][offset]
        assert r["complete"] and r["quote_side_net_usd"] == net
        assert r["daily_quote_side_net_usd"]["2024-01-03"] == 0
        assert r["quote_status_counts"] == {"RESOLVED_FRESHNESS_UNKNOWN": 2}
        assert r["additional_slippage_sensitivity_usd"]["1"] == net - 12.5
        assert r["trades"][0]["quotes"][0]["bid_size"] == 1
        assert r["trades"][0]["quotes"][0]["ask_size"] == 2
    assert not result["qualification_ready"] and not result["deployment_ready"]


def test_exact_interval_no_favorable_search_missing_stale_and_crossed_unresolved():
    final, trade, plan, quotes = fixture()
    missing = l1.evaluate_challenge(plan, final, [trade], quotes[1:])
    assert not missing["timing_offsets_seconds"]["0"]["complete"]
    assert missing["timing_offsets_seconds"]["0"]["quote_side_net_usd"] is None
    assert missing["timing_offsets_seconds"]["1"]["complete"]
    quotes[0]["last_quote_update_ns"] = quotes[0]["ts_recv"] - 10 * l1.SECOND
    assert l1.evaluate_challenge(plan, final, [trade], quotes)["timing_offsets_seconds"]["0"]["quote_status_counts"]["STALE_QUOTE"] == 1
    quotes[0].pop("last_quote_update_ns")
    quotes[0]["bid_nano"] = quotes[0]["ask_nano"] + trade["tick_nano"]
    assert not l1.evaluate_challenge(plan, final, [trade], quotes)["timing_offsets_seconds"]["0"]["complete"]
    wrong = deepcopy(quotes)
    wrong[0]["ts_recv"] -= 1
    with pytest.raises(IntegrityError, match="outside"):
        l1.evaluate_challenge(plan, final, [trade], wrong)
    with pytest.raises(IntegrityError, match="duplicate"):
        l1.evaluate_challenge(plan, final, [trade], quotes + quotes[:1])


def test_timing_sensitivity_and_protocol_revisions_cannot_reselect():
    final, trade, plan, quotes = fixture()
    quotes[2]["ask_nano"] += 2 * trade["tick_nano"]
    result = l1.evaluate_challenge(plan, final, [trade], quotes)
    assert result["timing_offsets_seconds"]["1"]["quote_side_net_usd"] == 15.
    assert result["timing_offsets_seconds"]["0"]["quote_side_net_usd"] == 40.
    changed = c.bind({**{k: v for k, v in plan.items() if k != "challenge_id"}, "offset_seconds": [1]}, "challenge_id")
    with pytest.raises(IntegrityError, match="changed"):
        l1.evaluate_challenge(changed, final, [trade], quotes)
    trade["direction"] = -1
    with pytest.raises(IntegrityError, match="changed"):
        l1.evaluate_challenge(plan, final, [trade], quotes)


def test_tbbo_is_trade_conditioned_and_no_trade_artifacts_have_no_trades():
    final, trade, plan, quotes = fixture("TBBO")
    result = l1.evaluate_challenge(plan, final, [trade], quotes)
    assert result["interpretation"] == "TRADE_CONDITIONED_REFERENCE_ONLY"
    final = c.bind({**{k: v for k, v in final.items() if k != "final_id"}, "policy": "no_trade"}, "final_id")
    with pytest.raises(IntegrityError, match="no-trade"):
        l1.freeze_challenge(final, [trade], dates=plan["dates"], source_release_id=plan["source_release_id"], schema="TBBO",
                           forecast_ledger_sha256=plan["forecast_ledger_sha256"], frozen_at_ns=plan["frozen_at_ns"])
