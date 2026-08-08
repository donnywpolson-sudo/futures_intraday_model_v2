"""Publish the audited overnight-reversal closure after separate approval."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.overnight_inventory_reversal_closure_publication import (
    publish_closure_clarification,
)


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    result = publish_closure_clarification(root=ROOT)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
