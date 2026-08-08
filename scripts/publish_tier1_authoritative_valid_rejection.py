"""Publish the approved valid-rejection closure and retire the active pointer."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.tier1_authoritative_valid_rejection import (
    publish_valid_rejection_closure,
    verify_published_valid_rejection,
)


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    result = publish_valid_rejection_closure(root=ROOT)
    verification = verify_published_valid_rejection(root=ROOT)
    print(json.dumps({**result, "verification": verification}, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
