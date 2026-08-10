"""Create the source-safe Apex micro Phase 1B/2 preparation contract."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.micro_alpha_phase1b2_preparation import (
    OUTPUT_PATH,
    build_prepare_only_contract,
)


ROOT = Path(__file__).resolve().parents[1]


def write_create_only(*, root: Path = ROOT) -> dict[str, object]:
    contract = build_prepare_only_contract(root=root)
    output = root / OUTPUT_PATH
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as stream:
        stream.write(canonical_bytes(contract) + b"\n")
    return contract


def main() -> int:
    contract = write_create_only(root=ROOT)
    print(
        json.dumps(
            {
                "contract_id": contract["contract_id"],
                "sha256": sha256_file(ROOT / OUTPUT_PATH),
                "state": contract["state"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
