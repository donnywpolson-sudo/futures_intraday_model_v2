"""Metadata-only entrypoint for the OHLCV-1D/1H backfill quote."""

from futures_rebuild.ohlcv_historical_backfill import quote_cli


if __name__ == "__main__":
    raise SystemExit(quote_cli())
