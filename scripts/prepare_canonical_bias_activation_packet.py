"""Prepare exact active-pointer bytes without mutating live pointers."""

from __future__ import annotations

import argparse
import base64
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

from futures_rebuild.alpha_research_ladder import build_active_pointer
from futures_rebuild.bias_robustness_successor import (
    build_active_execution_design,
    build_active_historical_design,
    require_stage3_active_authority,
)
from futures_rebuild.canonical import (
    canonical_authority_file_bytes, read_canonical_authority, sha256_bytes,
    sha256_file, sha256_json, write_new_canonical_authority,
)


ROOT = Path(__file__).resolve().parents[1]
PREDECESSOR_PACKET = Path("reports/post_stage_2_5_bias_robustness_successor/corrected_activation_v2/b73d17cb326c088e79333bc5c94d21b0821f28e6a90f7402b6d472d2923a3e68/activation_packet.json")
OUTPUT_ROOT = Path("reports/post_stage_2_5_bias_robustness_successor/corrected_activation_v3")
POINTERS = (
    Path("configs/active_alpha_historical_research_design.json"),
    Path("configs/active_alpha_ladder_execution_design.json"),
    Path("configs/active_alpha_research_ladder.json"),
)
SOURCE_PATHS = (
    Path("src/futures_rebuild/canonical.py"),
    Path("src/futures_rebuild/alpha_research_ladder.py"),
    Path("src/futures_rebuild/bias_robustness_successor.py"),
    Path("src/futures_rebuild/repository_surface.py"),
    Path("scripts/prepare_canonical_bias_activation_packet.py"),
    Path("scripts/independent_validate_canonical_bias_activation_packet.py"),
    Path("tests/test_canonical_bias_activation_packet.py"),
    Path("tests/test_causal_search_access_contract.py"),
    Path("tests/test_certified_research_gateway.py"),
    Path("tests/test_preexecution_fold_certification.py"),
    Path("tests/test_profiles_and_pipeline.py"),
    Path("tests/test_repository_surface.py"),
)


def load(path: Path) -> dict[str, object]:
    value = json.loads((ROOT / path).read_bytes())
    assert isinstance(value, dict)
    return value


def encoded_pointer(path: Path, payload: dict[str, object]) -> dict[str, object]:
    predecessor = (ROOT / path).read_bytes()
    successor = canonical_authority_file_bytes(payload)
    return {
        "predecessor_bytes_base64": base64.b64encode(predecessor).decode("ascii"),
        "predecessor_sha256": sha256_bytes(predecessor),
        "successor_bytes_base64": base64.b64encode(successor).decode("ascii"),
        "successor_sha256": sha256_bytes(successor),
        "successor_byte_count": len(successor),
    }


def build_plan() -> tuple[dict[str, object], Path]:
    predecessor_packet = read_canonical_authority(ROOT / PREDECESSOR_PACKET, identity_field="packet_id")
    bundle_path = Path(str(predecessor_packet["bundle"]["path"]))
    bundle = read_canonical_authority(ROOT / bundle_path, expected_sha256=str(predecessor_packet["bundle"]["sha256"]), identity_field="bundle_id")
    artifacts = bundle["artifacts"]
    ladder_item, profile_item = artifacts["ladder"], artifacts["profile"]
    historical_item, execution_item = artifacts["historical_design"], artifacts["execution_design"]
    ladder = load(Path(str(ladder_item["path"])))
    profile = load(Path(str(profile_item["path"])))
    historical_source = load(Path(str(historical_item["path"])))
    execution_source = load(Path(str(execution_item["path"])))
    old_ladder, old_historical, old_execution = (load(path) for path in (POINTERS[2], POINTERS[0], POINTERS[1]))

    active_ladder = build_active_pointer(
        contract_path=str(ladder_item["path"]), contract_sha256=str(ladder_item["sha256"]),
        contract_id=str(ladder_item["id"]), profile_path=str(profile_item["path"]),
        profile_sha256=str(profile_item["sha256"]), profile_id=str(profile_item["id"]),
    )
    active_historical = build_active_historical_design(
        old_historical, predecessor_sha256=sha256_file(ROOT / POINTERS[0]),
        source_design=historical_source, source_path=str(historical_item["path"]),
        source_sha256=str(historical_item["sha256"]), ladder_contract_id=str(ladder["contract_id"]),
        ladder_profile_id=str(profile["profile_id"]),
    )
    active_execution = build_active_execution_design(
        old_execution, predecessor_sha256=sha256_file(ROOT / POINTERS[1]),
        source_design=execution_source, source_path=str(execution_item["path"]),
        source_sha256=str(execution_item["sha256"]), ladder_contract_id=str(ladder["contract_id"]),
        ladder_profile_id=str(profile["profile_id"]),
    )
    successors = {
        POINTERS[0].as_posix(): encoded_pointer(POINTERS[0], active_historical),
        POINTERS[1].as_posix(): encoded_pointer(POINTERS[1], active_execution),
        POINTERS[2].as_posix(): encoded_pointer(POINTERS[2], active_ladder),
    }
    core = {
        "schema_version": "bias_robustness_canonical_activation_plan/2.0.0",
        "state": "PREPARED_NOT_ACTIVE",
        "predecessor_packet": {"packet_id": predecessor_packet["packet_id"], "path": PREDECESSOR_PACKET.as_posix(), "sha256": sha256_file(ROOT / PREDECESSOR_PACKET)},
        "bundle": predecessor_packet["bundle"],
        "git": {"branch": subprocess.check_output(["git", "branch", "--show-current"], cwd=ROOT, text=True).strip(), "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(), "index_sha256": sha256_bytes((ROOT / ".git/index").read_bytes())},
        "pointer_successors": successors,
        "write_order": [POINTERS[0].as_posix(), POINTERS[1].as_posix(), POINTERS[2].as_posix()],
        "rollback_order": [POINTERS[2].as_posix(), POINTERS[1].as_posix(), POINTERS[0].as_posix()],
        "ladder_pointer_written_last": True,
        "source_hashes": {path.as_posix(): sha256_file(ROOT / path) for path in SOURCE_PATHS},
        "required_pre_pointer_checks": ["PACKET_AND_SOURCE_HASHES", "ACTIVE_PREIMAGES", "PROSPECTIVE_ACTIVE_STATE", "ROW_OPENER_SENTINEL"],
        "required_post_pointer_checks": ["ACTIVE_STATE", "POINTER_SUCCESSOR_HASHES", "AFFECTED_REGRESSIONS", "INDEPENDENT_REPEAT", "ROW_OPENER_SENTINEL"],
        "authority": {"rows": False, "provider": False, "challenge": False, "refit": False, "final_252": False, "forward": False, "trading": False, "staging": False, "commit": False, "push": False},
    }
    plan = {**core, "plan_id": sha256_json(core)}
    path = OUTPUT_ROOT / str(plan["plan_id"]) / "activation_plan.json"
    write_new_canonical_authority(ROOT / path, plan)
    return plan, path


def verify_prospective(plan: dict[str, object]) -> None:
    copies = {
        Path(item["path"]) for item in load(Path(str(plan["bundle"]["path"]))) ["artifacts"].values()
    } | {
        Path("state/causal_search_access_contract_registry/5bff4a0914247a49db2f552caa199232490bab5d277051d468db01c539d11741/contract.json"),
        Path("configs/bounded_alpha_discovery_contract_v1.json"),
        Path("state/alpha_ladder_macro_successor_terminal_closure/7ff6ded9e3cbb45b17cb127783f3065f4e9f6f80ed38c3c623b1c15319c35cab/closure.json"),
    }
    with tempfile.TemporaryDirectory(prefix="bias-activation-") as temporary:
        candidate = Path(temporary)
        for relative in copies:
            target = candidate / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, target)
        for relative, item in plan["pointer_successors"].items():
            target = candidate / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(base64.b64decode(item["successor_bytes_base64"], validate=True))
        contract, profile, stage = require_stage3_active_authority(candidate)
        assert contract["contract_id"] == load(Path(str(plan["bundle"]["path"]))) ["artifacts"]["ladder"]["id"]
        assert profile["profile_id"] and stage["contract_id"]


def prepare_packet(certificate_path: Path) -> tuple[dict[str, object], Path]:
    plan, plan_path = build_plan()
    verify_prospective(plan)
    certificate = read_canonical_authority(ROOT / certificate_path, identity_field="validation_id")
    if certificate.get("plan_id") != plan["plan_id"] or certificate.get("plan_sha256") != sha256_file(ROOT / plan_path):
        raise RuntimeError("independent certificate does not bind this activation plan")
    core = {
        "schema_version": "bias_robustness_canonical_activation_packet/2.0.0",
        "state": "PREPARED_NOT_ACTIVE", "activation": False,
        "activation_plan": {"plan_id": plan["plan_id"], "path": plan_path.as_posix(), "sha256": sha256_file(ROOT / plan_path)},
        "independent_validation": {"validation_id": certificate["validation_id"], "path": certificate_path.as_posix(), "sha256": sha256_file(ROOT / certificate_path)},
        "pointer_successors_sha256": sha256_json(plan["pointer_successors"]),
        "write_order": plan["write_order"], "rollback_order": plan["rollback_order"],
        "ladder_pointer_written_last": True,
        "approval_required": "ACTIVATE_ONLY_THIS_EXACT_PACKET",
        "authority": plan["authority"],
    }
    packet = {**core, "packet_id": sha256_json(core)}
    path = OUTPUT_ROOT / str(packet["packet_id"]) / "activation_packet.json"
    write_new_canonical_authority(ROOT / path, packet)
    return packet, path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--certificate", type=Path)
    args = parser.parse_args()
    if args.certificate:
        packet, path = prepare_packet(args.certificate)
        print(json.dumps({"packet_id": packet["packet_id"], "path": path.as_posix(), "sha256": sha256_file(ROOT / path)}, sort_keys=True, separators=(",", ":")))
    else:
        plan, path = build_plan()
        verify_prospective(plan)
        print(json.dumps({"plan_id": plan["plan_id"], "path": path.as_posix(), "sha256": sha256_file(ROOT / path)}, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
