"""Offline simulation: generate a demo or prepare a bounded historical inspection."""
from __future__ import annotations

import argparse
import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterable

from .simulation_records import LABEL, MAX_BARS, MAX_BYTES, SCHEMA, decimal, encoded, position, validate_bundle, validate_records


@dataclass(frozen=True)
class Bar:
    contract: str
    opened_at: str
    closed_at: str
    open: str
    high: str
    low: str
    close: str
    volume: int


@dataclass(frozen=True)
class Settings:
    initial_balance: str = "10000"
    tick_size: str = "0.25"
    point_value: str = "50"
    commission_per_side: str = "2"
    slippage_ticks: int = 1


@dataclass(frozen=True)
class Account:
    position: int = 0
    realized_pnl: str = "0"
    unrealized_pnl: str = "0"
    commissions: str = "0"
    slippage_cost: str = "0"
    equity: str = "10000"
    drawdown: str = "0"
    total_costs: str = "0"


@dataclass(frozen=True)
class Context:
    bars: tuple[Bar, ...]
    account: Account


def no_trade(context: Context) -> None:
    return None


def scripted_test(context: Context) -> int | None:
    """Fixed mechanics fixture, not a discovered trading strategy."""
    return {1: 1, 12: -1, 24: 0, 36: -1, 48: 1, 58: 0}.get(len(context.bars))


def simulate(bars: Iterable[Bar], decide: Callable[[Context], int | None], *,
             strategy: str, settings: Settings = Settings(), market: str = "ES",
             synthetic: bool) -> dict:
    if synthetic is not True:
        raise ValueError("Real-history evaluation is not enabled in this milestone")
    initial, point, tick, commission = (decimal(getattr(settings, k)) for k in
                                      ("initial_balance", "point_value", "tick_size", "commission_per_side"))
    if min(initial, point, tick) <= 0 or commission < 0 or type(settings.slippage_ticks) is not int or not 0 <= settings.slippage_ticks <= 100:
        raise ValueError("Invalid simulation settings")
    slip = tick * settings.slippage_ticks
    held = 0
    entry = realized = fees = slippage = Decimal(0)
    peak = initial
    pending = None
    history: list[Bar] = []
    rows: list[dict] = []
    account = Account(equity=str(initial))
    for bar in bars:
        if not isinstance(bar, Bar) or len(history) >= MAX_BARS:
            raise ValueError("Expected bounded normalized bars")
        # Validate the incoming bar and its boundary before invoking any strategy.
        check = {"bar": asdict(bar), "decision": None, "fills": [], "account": asdict(account)}
        validate_records(([{**rows[-1], "fills": []}] if rows else []) + [check],
                         history[0].contract if history else bar.contract, settings.tick_size)
        fills = []
        if pending is not None and pending != held:
            delta = pending - held
            direction = 1 if delta > 0 else -1
            price = decimal(bar.open) + direction * slip
            qty = abs(delta)
            if held:
                realized += (price - entry) * held * point
            fees += commission * qty
            slippage += slip * qty * point
            fills.append({"time": bar.opened_at, "side": "BUY" if delta > 0 else "SELL",
                          "quantity": qty, "price": str(price), "commission": str(commission * qty),
                          "decision_index": len(rows) - 1})
            held = pending
            entry = price if held else Decimal(0)
        unrealized = (decimal(bar.close) - entry) * held * point
        equity = initial + realized + unrealized - fees
        peak = max(peak, equity)
        account = Account(held, str(realized), str(unrealized), str(fees), str(slippage),
                          str(equity), str(peak - equity), str(fees + slippage))
        history.append(bar)
        pending = decide(Context(tuple(history), account))
        if pending is not None:
            position(pending)
        rows.append({"bar": asdict(bar), "decision": pending, "fills": fills, "account": asdict(account)})
    if not rows:
        raise ValueError("No bars supplied")
    bundle = {"schema": SCHEMA, "status": "COMPLETE", "metadata": {
        "label": LABEL, "synthetic": True, "market": market, "contract": history[0].contract,
        "strategy": strategy, "input_sha256": hashlib.sha256(encoded([r["bar"] for r in rows])).hexdigest(),
        "settings": asdict(settings)}, "records": rows,
        "summary": {"account": asdict(account), "open_position": held != 0,
                    "pending_decision_cancelled": pending is not None and pending != held}}
    validate_bundle(bundle)
    return bundle


def demo_bars() -> Iterable[Bar]:
    start = datetime(2020, 1, 6, 14, 30, tzinfo=timezone.utc)
    price = Decimal("5000")
    for index in range(60):
        end = start + timedelta(minutes=1)
        close = price + Decimal((index % 7) - 3) * Decimal("0.25")
        yield Bar("ES-SYNTHETIC", start.isoformat(), end.isoformat(), str(price),
                  str(max(price, close) + 1), str(min(price, close) - 1), str(close), 100 + index)
        start, price = end, close


def save_demo(directory: Path) -> dict:
    directory.mkdir(parents=True, exist_ok=False)
    # A failure leaves no result.json: incomplete output is never replayable.
    bundle = simulate(demo_bars(), scripted_test, strategy="scripted-mechanics-v1", synthetic=True)
    raw = encoded(bundle)
    if len(raw) > MAX_BYTES:
        raise ValueError("Oversized simulation output")
    temporary = directory / "result.partial"
    with temporary.open("xb") as stream:
        stream.write(raw)
    temporary.replace(directory / "result.json")
    return bundle


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--demo", action="store_true")
    mode.add_argument("--prepare-historical", type=Path, metavar="SPEC", help="Write a metadata-only run specification; no historical execution.")
    mode.add_argument("--historical-spec", type=Path, metavar="SPEC", help="Validate a prepared specification; historical execution requires the approved task.")
    mode.add_argument("--prepare-library", type=Path, metavar="SPEC", help="Prepare the bounded ES session-library declaration; no historical execution.")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.demo:
            save_demo(args.output)
        else:
            import json
            from .historical_model_inspection import prepare_file, validate_spec, prepare_library_file, validate_library_spec
            root = Path(__file__).resolve().parents[2]
            if args.prepare_library:
                spec = prepare_library_file(root, args.prepare_library, args.output)
            elif args.prepare_historical:
                spec = prepare_file(root, args.prepare_historical, args.output)
            else:
                spec = json.loads(args.historical_spec.read_bytes())
                if (root / spec["output"]).resolve() != args.output.resolve():
                    raise ValueError("Output differs from the prepared scope")
                (validate_library_spec if spec.get("schema") == "historical-model-library/1" else validate_spec)(root, spec)
            print(f"PREPARED ONLY — approval required before historical reads/inference\nSessions: {spec.get('sessions', [spec['session']])}\nModel: {spec['model_id']}\nOutput: {spec['output']}\n{spec['physical_read_scope']}")
            return 0
    except (OSError, ValueError) as exc:
        parser.exit(2, f"Simulation failed: {exc}\n")
    print(f"{LABEL}\nSaved: {args.output / 'result.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
