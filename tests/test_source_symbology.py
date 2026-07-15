import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.source_symbology import (
    build_query_contract,
    require_allowed_query_symbology,
    require_query_contract,
)


@pytest.mark.parametrize(
    ("schema", "stype_in", "symbols"),
    [
        ("definition", "parent", ["ES.FUT"]),
        ("ohlcv-1m", "continuous", ["ES.v.0"]),
        ("statistics", "continuous", ["ES.v.0"]),
        ("statistics", "parent", ["ES.FUT"]),
        ("status", "continuous", ["ES.v.0"]),
        ("status", "parent", ["ES.FUT"]),
    ],
)
def test_exact_schema_aware_query_epochs_are_explicitly_supported(
    schema, stype_in, symbols
) -> None:
    assert require_allowed_query_symbology(
        schema=schema,
        market="ES",
        stype_in=stype_in,
        symbols=symbols,
    ) == (stype_in, tuple(symbols))


@pytest.mark.parametrize(
    ("schema", "stype_in", "symbols"),
    [
        ("definition", "continuous", ["ES.v.0"]),
        ("ohlcv-1m", "parent", ["ES.FUT"]),
        ("status", "continuous", ["NQ.v.0"]),
        ("statistics", "parent", ["ES.v.0"]),
        ("status", "raw_symbol", ["ESZ4"]),
    ],
)
def test_query_epoch_mismatch_fails_closed(schema, stype_in, symbols) -> None:
    with pytest.raises(IntegrityError, match="query symbology"):
        require_allowed_query_symbology(
            schema=schema,
            market="ES",
            stype_in=stype_in,
            symbols=symbols,
        )


def test_query_contract_is_content_addressed_to_exact_interval() -> None:
    first = build_query_contract(
        schema="status",
        market="ES",
        start="2024-01-01",
        end="2025-01-01",
        stype_in="parent",
        symbols=["ES.FUT"],
    )
    second = build_query_contract(
        schema="status",
        market="ES",
        start="2023-01-01",
        end="2024-01-01",
        stype_in="parent",
        symbols=["ES.FUT"],
    )
    assert first["query_mode_id"] == second["query_mode_id"]
    assert first["query_contract_id"] != second["query_contract_id"]
    assert require_query_contract(first) == first


def test_query_contract_mutation_fails_even_when_fields_remain_individually_allowed() -> None:
    contract = build_query_contract(
        schema="statistics",
        market="ES",
        start="2024-01-01",
        end="2025-01-01",
        stype_in="continuous",
        symbols=["ES.v.0"],
    )
    contract["stype_in"] = "parent"
    contract["symbols"] = ["ES.FUT"]
    with pytest.raises(IntegrityError, match="content address"):
        require_query_contract(contract)
