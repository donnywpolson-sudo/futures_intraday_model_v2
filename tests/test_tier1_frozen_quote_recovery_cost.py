from __future__ import annotations

from pathlib import Path
from inspect import signature

import pytest
from databento.historical.api.metadata import MetadataHttpAPI

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_frozen_quote_recovery_cost import (
    MAXIMUM_PROVIDER_CALLS,
    build_quote_cost_queries,
    load_diagnostic_record,
    load_quote_recovery_cost_plan,
    quote_costs,
)


ROOT = Path(__file__).resolve().parents[1]


def _queries():
    return build_quote_cost_queries(
        diagnostic_record=load_diagnostic_record(root=ROOT),
    )


def test_quote_cost_queries_cover_exactly_the_33_unresolved_gaps() -> None:
    queries = _queries()
    opportunities = [item for query in queries for item in query.opportunity_ids]
    assert len(queries) == MAXIMUM_PROVIDER_CALLS == 30
    assert len(opportunities) == len(set(opportunities)) == 33
    assert all(query.start[:4] in {"2018", "2019", "2020", "2021", "2022"} for query in queries)
    assert all(query.end[:4] in {"2018", "2019", "2020", "2021", "2022"} for query in queries)
    assert all(set(query.categories) <= {"ENTRY", "LIQUIDATION"} for query in queries)
    assert sum(len(query.symbols) == 4 for query in queries) == 1


def test_provider_query_contract_is_metadata_cost_only_and_price_independent() -> None:
    queries = _queries()
    calls = []

    def fake_get_cost(**kwargs):
        calls.append(kwargs)
        return "0.125"

    results = quote_costs(queries=queries, get_cost=fake_get_cost)
    assert len(calls) == len(results) == 30
    assert all(call["dataset"] == "GLBX.MDP3" for call in calls)
    assert all(call["schema"] == "bbo-1s" for call in calls)
    assert all(set(call) == {
        "dataset", "schema", "stype_in", "symbols", "start", "end",
    } for call in calls)
    assert all(item["provider_row_downloaded"] is False for item in results)
    assert all(item["estimated_data_cost_usd"] == "0.125" for item in results)


def test_provider_kwargs_match_the_pinned_metadata_api_signature() -> None:
    accepted = set(signature(MetadataHttpAPI.get_cost).parameters) - {"self"}
    provider_keys = set(_queries()[0].provider_kwargs())
    assert provider_keys <= accepted
    assert "stype_out" not in provider_keys


@pytest.mark.parametrize("invalid", (True, "-0.01", "NaN", "Infinity"))
def test_provider_cost_estimate_fails_closed(invalid) -> None:
    with pytest.raises(IntegrityError, match="cost estimate"):
        quote_costs(
            queries=_queries(),
            get_cost=lambda **kwargs: invalid,
        )


def test_quote_cost_plan_is_hash_bound_and_non_authorizing() -> None:
    plan = load_quote_recovery_cost_plan(root=ROOT)
    assert plan["plan_id"] == (
        "1b06c702474b518acc0a10139a880aae1608adc99a3c8c6c576ad40631f23689"
    )
    assert plan["maximum_external_cost_usd"] == "0"
    assert plan["maximum_provider_metadata_calls"] == 30
    assert plan["credential_source"] == "file api.env"
    assert plan["quote_window_semantics"]["quote_data_adopted_as_research_source"] is False
    assert set(plan["forbidden_actions"].values()) == {True}
