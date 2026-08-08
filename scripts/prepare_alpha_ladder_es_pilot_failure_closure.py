"""Prepare, but never publish, the counted Alpha ES pilot FAIL closure."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.alpha_ladder_es_pilot_failure_closure import (
    prepare_failure_closure,
)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    print(json.dumps(prepare_failure_closure(root=root), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
