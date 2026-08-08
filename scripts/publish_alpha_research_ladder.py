"""Publish and activate the exact prepared Alpha ladder with pointer rollback."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

from futures_rebuild.alpha_research_ladder import (
    ACTIVE_POINTER_PATH,
    build_active_pointer,
    load_active_ladder,
    validate_contract,
    validate_profile,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_ID = "d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18"
PROFILE_ID = "a2088ceb344f1aa44bf3a663ca2e2036e0cbea575e5521d04976ef0443a53210"
PREP = Path("state/unpublished_evidence/alpha_research_ladder_preparation") / CONTRACT_ID
TARGET = Path("state/alpha_ladder_registry") / CONTRACT_ID
FILES = {
    "universe_contract.json": "0e258e6d7b375763d9e2d4795ecd7e9f1ee8e2d83ae0993f1f8305558900c453",
    "alpha_tiered.yaml": "f7a914d275aca3ecfa41486fd4cf9dbeab5d5e4bf4a41bf577ac8af13f73cf39",
    "invalid_preparations.json": "c142cded10886c7acd80ac5bb0ff5cce83d7214ed21b004c3b909f6dbd9259b9",
    "es_2018_alpha_profile_preparation.yaml": "f66109b982e4ecaaf5eef3c9426bdc34f6fd5b1da0959fd90e559fe18a07ffe2",
}
FOCUSED_TESTS = (
    "tests/test_alpha_ladder_frozen_mechanism.py",
    "tests/test_alpha_research_ladder.py",
    "tests/test_certified_research_gateway.py",
    "tests/test_preexecution_fold_certification.py",
)


def _write_once(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def main() -> int:
    pointer_path = ROOT / ACTIVE_POINTER_PATH
    target_root = ROOT / TARGET
    if pointer_path.exists():
        raise IntegrityError("Alpha ladder pointer already exists")
    if target_root.exists():
        raise IntegrityError("Alpha ladder publication target already exists")

    prepared: dict[str, bytes] = {}
    for name, expected in FILES.items():
        source = ROOT / PREP / name
        if sha256_file(source) != expected:
            raise IntegrityError(f"prepared Alpha ladder artifact changed: {name}")
        prepared[name] = source.read_bytes()

    for name, raw in prepared.items():
        _write_once(target_root / name, raw)
        if (target_root / name).read_bytes() != raw:
            raise IntegrityError(f"published Alpha ladder bytes differ: {name}")

    contract_path = target_root / "universe_contract.json"
    profile_path = target_root / "alpha_tiered.yaml"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    profile = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    validate_contract(contract)
    validate_profile(profile, root=ROOT)
    if contract.get("contract_id") != CONTRACT_ID or profile.get("profile_id") != PROFILE_ID:
        raise IntegrityError("published Alpha ladder identity changed")

    pointer = build_active_pointer(
        contract_path=(TARGET / "universe_contract.json").as_posix(),
        contract_sha256=FILES["universe_contract.json"],
        contract_id=CONTRACT_ID,
        profile_path=(TARGET / "alpha_tiered.yaml").as_posix(),
        profile_sha256=FILES["alpha_tiered.yaml"],
        profile_id=PROFILE_ID,
    )
    try:
        _write_once(pointer_path, canonical_bytes(pointer) + b"\n")
        loaded_contract, loaded_profile = load_active_ladder(ROOT)
        if (
            loaded_contract.get("contract_id") != CONTRACT_ID
            or loaded_profile.get("profile_id") != PROFILE_ID
        ):
            raise IntegrityError("post-activation registered context is mismatched")
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-m", "high_risk", *FOCUSED_TESTS],
            cwd=ROOT, check=False,
        )
        if result.returncode != 0:
            raise IntegrityError("post-activation focused synthetic suite failed")
    except BaseException:
        pointer_path.unlink(missing_ok=True)
        raise

    print(json.dumps({
        "contract_id": CONTRACT_ID,
        "profile_id": PROFILE_ID,
        "pointer_path": ACTIVE_POINTER_PATH.as_posix(),
        "pointer_sha256": sha256_file(pointer_path),
        "published_paths": [(TARGET / name).as_posix() for name in FILES],
        "post_activation_validation": "PASS",
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
