"""Prepare the immutable Apex integer-micro successor without provider access."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.alpha_research_architecture import (
    build_architecture_contract,
    build_micro_contract,
    build_micro_profile,
    build_prepared_micro_pointer,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.micro_alpha_databento_preflight import (
    OBSOLETE_PLAN_ID,
    OBSOLETE_PLAN_PATH,
    OBSOLETE_PLAN_SHA256,
    PLAN_PATH,
    REFERENCE_PATH,
    SUPERSESSION_PATH,
    build_plan,
    load_obsolete_plan,
)
from futures_rebuild.micro_alpha_pipeline import build_product_reference_requirements


ROOT = Path(__file__).resolve().parents[1]


def _create_only(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(payload) + b"\n"
    if path.exists():
        if path.read_bytes() != raw:
            raise RuntimeError(f"prepared artifact differs: {path}")
        return
    with path.open("xb") as stream:
        stream.write(raw)


def main() -> int:
    obsolete = load_obsolete_plan(root=ROOT)
    old_preparation = ROOT / "state" / "unpublished_evidence" / "apex_micro_ladder_preparation" / "febccafd953e8bd7323930ae7beb8d381242e5adb31300795f31e5ce092245ab"
    expected_old_hashes = {
        "alpha_tiered.json": "2b3e0556950fc694579926aa3676c1ec8b963027b82ae6869589b010fbfd9953",
        "prepared_active_pointer.json": "9a951e7ff6c0fe94ad467af93f563b5483d59eea78cbd85b04a9e8e91c800add",
        "universe_contract.json": "1ba9d40dc0f223adc5958f4d06d61f8574a4fa09b3f9ccaeadeb3b5e0d09b55b",
    }
    observed_old_hashes = {
        name: sha256_file(old_preparation / name) for name in expected_old_hashes
    }
    if observed_old_hashes != expected_old_hashes:
        raise RuntimeError("obsolete Apex micro preparation was not preserved byte-for-byte")

    micro = build_micro_contract()
    profile = build_micro_profile(contract_id=str(micro["contract_id"]))
    registry = ROOT / "state" / "unpublished_evidence" / "apex_micro_ladder_preparation_v2" / str(micro["contract_id"])
    contract_path = registry / "universe_contract.json"
    profile_path = registry / "alpha_tiered.json"
    _create_only(contract_path, micro)
    _create_only(profile_path, profile)
    pointer = build_prepared_micro_pointer(
        contract_path=contract_path.relative_to(ROOT).as_posix(),
        contract_sha256=sha256_file(contract_path),
        contract_id=str(micro["contract_id"]),
        profile_path=profile_path.relative_to(ROOT).as_posix(),
        profile_sha256=sha256_file(profile_path),
        profile_id=str(profile["profile_id"]),
    )
    pointer_path = registry / "prepared_active_pointer.json"
    _create_only(pointer_path, pointer)

    standard_pointer = ROOT / "configs" / "active_alpha_research_ladder.json"
    standard = json.loads(standard_pointer.read_text(encoding="utf-8"))
    architecture = build_architecture_contract(
        standard_pointer_sha256=sha256_file(standard_pointer),
        standard_contract_id=str(standard["contract_id"]),
        micro_contract_id=str(micro["contract_id"]),
        micro_profile_id=str(profile["profile_id"]),
        micro_pointer_id=str(pointer["pointer_id"]),
    )
    architecture_path = (
        ROOT / "state" / "unpublished_evidence" / "alpha_research_architecture_v2"
        / str(architecture["architecture_id"]) / "architecture.json"
    )
    _create_only(architecture_path, architecture)
    references = build_product_reference_requirements()
    _create_only(ROOT / REFERENCE_PATH, references)
    supersession_core = {
        "schema_version": "apex_micro_preparation_supersession/1.0.0",
        "classification": "SUPERSEDED_PREPARATION — MICRO_TIER1_SCOPE_RECONCILIATION",
        "state": "UNPUBLISHED_IMMUTABLE_CLASSIFICATION",
        "obsolete_preflight": {
            "path": OBSOLETE_PLAN_PATH.as_posix(),
            "plan_id": OBSOLETE_PLAN_ID,
            "sha256": OBSOLETE_PLAN_SHA256,
            "observed_plan_id": obsolete["plan_id"],
            "execution_as_current_forbidden": True,
        },
        "obsolete_preparation": {
            "contract_id": "febccafd953e8bd7323930ae7beb8d381242e5adb31300795f31e5ce092245ab",
            "path": old_preparation.relative_to(ROOT).as_posix(),
            "file_sha256": expected_old_hashes,
            "preservation": "BYTE_FOR_BYTE_NO_OVERWRITE_DELETE_RELABEL_OR_PUBLICATION",
        },
        "reason": "TIER1_MUST_REPRESENT_EQUITY_ENERGY_METALS_AND_FX",
        "corrected_tier_1": ["MES", "MCL", "MGC", "M6E"],
        "successor_contract_id": micro["contract_id"],
        "successor_profile_id": profile["profile_id"],
        "successor_prepared_pointer_id": pointer["pointer_id"],
        "successor_architecture_id": architecture["architecture_id"],
    }
    supersession = {
        **supersession_core,
        "supersession_id": sha256_json(supersession_core),
    }
    _create_only(ROOT / SUPERSESSION_PATH, supersession)
    _create_only(ROOT / PLAN_PATH, build_plan(root=ROOT))
    print(json.dumps({
        "architecture_id": architecture["architecture_id"],
        "micro_contract_id": micro["contract_id"],
        "micro_profile_id": profile["profile_id"],
        "micro_prepared_pointer_id": pointer["pointer_id"],
        "preflight_plan_id": build_plan(root=ROOT)["plan_id"],
        "obsolete_preflight_plan_id": OBSOLETE_PLAN_ID,
        "supersession_id": supersession["supersession_id"],
        "state": "PREPARED_NOT_EXECUTED",
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
