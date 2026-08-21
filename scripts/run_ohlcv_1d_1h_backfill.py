"""Dry-run-default entrypoint for the OHLCV-1D/1H backfill executor."""

from futures_rebuild.ohlcv_historical_backfill import execute_cli


if __name__ == "__main__":
    raise SystemExit(execute_cli())
