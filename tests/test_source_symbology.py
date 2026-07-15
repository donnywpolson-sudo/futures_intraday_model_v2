import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.source_symbology import require_allowed_query_symbology


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
