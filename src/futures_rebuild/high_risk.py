"""Prepare plain-language descriptions of high-risk work.

This module deliberately has no execution command.  Conversational approval is
an orchestration responsibility, not a token that a repository CLI can mint or
validate.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from typing import Any


CONFIRMATION_SCHEMA = "high_risk_confirmation/1.0.0"


def confirmation_required(
    operation: str,
    *,
    scope: Mapping[str, Any] | None = None,
    outputs: Sequence[str] = (),
    preservation: str = "Preserve accepted data and unrelated work; do not overwrite outputs.",
) -> dict[str, Any]:
    """Return the one human-readable preparation result for a risky action."""

    if not operation.strip():
        raise ValueError("operation is required")
    return {
        "schema_version": CONFIRMATION_SCHEMA,
        "status": "CONFIRMATION_REQUIRED",
        "operation": operation.strip(),
        "summary": "Codex must obtain one plain-language user confirmation before this operation.",
        "scope": dict(sorted((scope or {}).items())),
        "outputs": list(outputs),
        "preservation": preservation,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="futures-high-risk-prepare",
        description="Describe a high-risk operation; this command never executes it.",
    )
    parser.add_argument("--operation", required=True)
    parser.add_argument("--scope", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--output", action="append", default=[])
    parser.add_argument("--preservation")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    scope: dict[str, str] = {}
    for item in args.scope:
        key, separator, value = item.partition("=")
        if not separator or not key or not value:
            raise SystemExit("--scope values must be KEY=VALUE")
        scope[key] = value
    result = confirmation_required(
        args.operation,
        scope=scope,
        outputs=args.output,
        preservation=args.preservation or "Preserve accepted data and unrelated work; do not overwrite outputs.",
    )
    print(json.dumps(result, sort_keys=True))
    return 0
