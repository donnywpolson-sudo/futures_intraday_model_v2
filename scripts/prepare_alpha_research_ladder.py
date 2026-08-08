"""Prepare the inactive Alpha ladder and invalid-preparation evidence only."""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import yaml

from futures_rebuild.alpha_research_ladder import (
    build_contract,
    build_profile,
    validate_contract,
    validate_profile,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json


ROOT = Path(__file__).resolve().parents[1]
PREDECESSOR = Path("configs/research_universe_contract.json")
CURRENT_PROFILE = Path("configs/alpha_tiered.yaml")
SIX_MARKET_PREPARATION = Path("configs/tier1_trade_triggered_trial_protocol.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/alpha_research_ladder_preparation")
PRIOR_DEFECTIVE_PREPARATION = Path(
    "state/unpublished_evidence/alpha_research_ladder_preparation/"
    "db38f7254775c4bd8ade1aae130607746810867d13ec289bebdcbd1fc4e96576"
)


def _create(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(raw)
    except FileExistsError:
        if path.read_bytes() != raw:
            raise RuntimeError(f"prepared artifact already exists with different bytes: {path}")


def _canonical_json(payload: dict[str, object]) -> bytes:
    return canonical_bytes(payload) + b"\n"


def main() -> int:
    predecessor_sha = sha256_file(ROOT / PREDECESSOR)
    contract = build_contract(
        predecessor_path=PREDECESSOR.as_posix(), predecessor_sha256=predecessor_sha,
    )
    validate_contract(contract)
    contract_id = str(contract["contract_id"])
    destination = ROOT / OUTPUT_ROOT / contract_id
    prepared_contract_relative = (
        OUTPUT_ROOT / contract_id / "universe_contract.json"
    ).as_posix()
    contract_relative = (
        Path("state/alpha_ladder_registry") / contract_id / "universe_contract.json"
    ).as_posix()
    contract_raw = _canonical_json(contract)
    contract_sha = sha256(contract_raw).hexdigest()
    profile = build_profile(
        contract_path=contract_relative,
        contract_sha256=contract_sha,
        contract_id=contract_id,
    )
    profile_raw = yaml.safe_dump(profile, sort_keys=False).encode("utf-8")

    _create(destination / "universe_contract.json", contract_raw)
    _create(destination / "alpha_tiered.yaml", profile_raw)
    validate_profile(
        profile, root=ROOT,
        prepared_contract_path=ROOT / prepared_contract_relative,
    )

    preserved_profile_path = destination / "es_2018_alpha_profile_preparation.yaml"
    prior_preserved_profile = (
        ROOT / PRIOR_DEFECTIVE_PREPARATION / "es_2018_alpha_profile_preparation.yaml"
    )
    current_profile_raw = (
        preserved_profile_path.read_bytes()
        if preserved_profile_path.exists()
        else prior_preserved_profile.read_bytes()
        if prior_preserved_profile.exists()
        else (ROOT / CURRENT_PROFILE).read_bytes()
    )
    six_market_raw = (ROOT / SIX_MARKET_PREPARATION).read_bytes()
    invalid_core: dict[str, object] = {
        "schema_version": "alpha_ladder_invalid_preparations/1.0.0",
        "classification": "UNPUBLISHED_PREPARATION_ONLY",
        "successor_contract_id": contract_id,
        "records": [
            {
                "reason": "PILOT_CALENDAR_YEAR_SCOPE_REPLACED_BY_EXECUTABLE_FOLD_SCOPE",
                "source_path": CURRENT_PROFILE.as_posix(),
                "source_sha256_at_preparation": sha256(current_profile_raw).hexdigest(),
                "preserved_copy": "es_2018_alpha_profile_preparation.yaml",
                "economic_outcomes_opened": False,
            },
            {
                "reason": "ALPHA_TIER_SCOPE_MISMATCH",
                "source_path": SIX_MARKET_PREPARATION.as_posix(),
                "source_sha256_at_preparation": sha256(six_market_raw).hexdigest(),
                "selected_markets": ["CL", "ES", "NG", "NQ", "RTY", "YM"],
                "required_tier_1_markets": ["ES", "CL", "ZN", "6E"],
                "economic_outcomes_opened": False,
            },
            {
                "reason": "PREPARED_PROFILE_BOUND_UNPUBLISHED_PATH_NOT_FINAL_REGISTRY",
                "source_path": PRIOR_DEFECTIVE_PREPARATION.as_posix(),
                "source_sha256_at_preparation": sha256_json({
                    "contract": sha256_file(
                        ROOT / PRIOR_DEFECTIVE_PREPARATION / "universe_contract.json"
                    ),
                    "profile": sha256_file(
                        ROOT / PRIOR_DEFECTIVE_PREPARATION / "alpha_tiered.yaml"
                    ),
                }),
                "economic_outcomes_opened": False,
            },
        ],
        "publication_authorized": False,
    }
    invalid = {**invalid_core, "record_id": sha256_json(invalid_core)}
    _create(preserved_profile_path, current_profile_raw)
    _create(destination / "invalid_preparations.json", _canonical_json(invalid))

    result = {
        "contract_id": contract_id,
        "contract_path": contract_relative,
        "prepared_contract_path": prepared_contract_relative,
        "contract_sha256": contract_sha,
        "profile_path": (OUTPUT_ROOT / contract_id / "alpha_tiered.yaml").as_posix(),
        "profile_sha256": sha256(profile_raw).hexdigest(),
        "profile_id": profile["profile_id"],
        "invalid_preparations_id": invalid["record_id"],
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
