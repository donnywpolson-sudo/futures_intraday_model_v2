"""Command-line interface for the stable closure workflow."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from futures_rebuild.canonical import canonical_bytes

from .engine import read_status


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="futures-closure-workflow")
    subparsers = parser.add_subparsers(dest="command", required=True)
    status = subparsers.add_parser("status")
    status.add_argument("--run-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    print(canonical_bytes(read_status(args.run_root)).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
