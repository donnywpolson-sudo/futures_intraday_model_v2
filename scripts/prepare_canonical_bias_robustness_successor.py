"""Prepare the inactive canonical successor; never mutate active pointers."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import yaml

from futures_rebuild.alpha_research_ladder import (
    build_canonical_bias_remediation_successor_contract,
    build_canonical_bias_remediation_successor_profile,
    validate_contract as validate_ladder,
    validate_profile,
)
from futures_rebuild.bias_robustness_successor import validate_contract as validate_stage
from futures_rebuild.canonical import (
    canonical_authority_file_bytes,
    read_canonical_authority,
    sha256_bytes,
    sha256_file,
    sha256_json,
    write_new_canonical_authority,
)


ROOT = Path(__file__).resolve().parents[1]
OLD_STAGE = Path("state/causal_search_access_contract_registry/5bff4a0914247a49db2f552caa199232490bab5d277051d468db01c539d11741/contract.json")
OLD_LADDER = Path("state/alpha_ladder_registry/2bc9e04a5f53b57c1c70517ee3b098b05784fd4b918b00596e7a724e83e11c87/universe_contract.json")
OLD_PROFILE = OLD_LADDER.with_name("alpha_tiered.yaml")
OLD_DESIGN_ROOT = Path("state/bias_robustness_successor_registry/df2980b778cdbf22b1f9529dc27dae09e21ade47bfa972262f5e6db7ee11076d")
OLD_HISTORICAL = OLD_DESIGN_ROOT / "historical_design.json"
OLD_EXECUTION = OLD_DESIGN_ROOT / "execution_design.json"
OLD_PACKET = Path("reports/post_stage_2_5_bias_robustness_successor/activation_review_packet.json")
OLD_BUNDLE_ID = "b6ad4c5c178dbacea877c5a761186998569c62f4e73d95600ebd81f9c788ab57"
OUTPUT_ROOT = Path("reports/post_stage_2_5_bias_robustness_successor/corrected_activation_v2")
ACTIVE_PATHS = (
    Path("configs/active_alpha_research_ladder.json"),
    Path("configs/active_alpha_historical_research_design.json"),
    Path("configs/active_alpha_ladder_execution_design.json"),
    Path("data/active/catalog.json"),
    Path("state/final_evaluation_session_manifest_registry/ad1baab30b52fdda95e72777a4930ea18bd78037925cc36a27594f484c876549/final_252_session_manifest.json"),
)
TASK_SOURCE_PATHS = (
    Path("src/futures_rebuild/canonical.py"),
    Path("src/futures_rebuild/alpha_research_ladder.py"),
    Path("src/futures_rebuild/bias_robustness_successor.py"),
    Path("src/futures_rebuild/causal_search_access_contract.py"),
    Path("scripts/prepare_canonical_bias_robustness_successor.py"),
    Path("scripts/independent_validate_canonical_bias_robustness_successor.py"),
    Path("tests/test_causal_search_access_contract.py"),
    Path("tests/test_canonical_authority_successor.py"),
)
SUPERSEDED_TASK_EVIDENCE = (
    Path("state/bias_robustness_validation_registry/ee9ca7d259d70dbf266abb3c4b674352c4a80929b7382e584e27234f1f07d4b3/certificate.json"),
    Path("state/bias_robustness_validation_registry/cf01e76cf791226606f1732af88999feba86ddc6b0855b1a3b52a6d4fd7e7296/certificate.json"),
    Path("state/bias_robustness_failure_registry/4625a802194fe5225d3ee88476940aee68145dbd884d4ccc498e7584c99f1261/failure.json"),
    Path("reports/post_stage_2_5_bias_robustness_successor/corrected_activation_v2/82b5ca3225433345c9c6bce5166b88ab926d7ff3f36f6ab69722821760ac0533/activation_packet.json"),
    Path("reports/post_stage_2_5_bias_robustness_successor/corrected_activation_v2/253cba542f66ae43db460cfe4199e9bd43fe60a2d292d915464043843eca6c84/activation_packet.json"),
)


def load(path: Path) -> dict[str, object]:
    value = json.loads((ROOT / path).read_bytes())
    assert isinstance(value, dict)
    return value


def binding(path: Path, identity: str, field: str, schema: str | None = None) -> dict[str, object]:
    result: dict[str, object] = {field: identity, "path": path.as_posix(), "sha256": sha256_file(ROOT / path)}
    if schema is not None:
        result["schema_version"] = schema
    return result


def identified(core: dict[str, object], field: str) -> dict[str, object]:
    return {**core, field: sha256_json(core)}


def prepare_core() -> dict[str, object]:
    old_stage = load(OLD_STAGE)
    old_packet = load(OLD_PACKET)
    if sha256_file(ROOT / OLD_STAGE) != "3c1070ba3c70498c0f66bfebb2cd428a92853d94074b8a0fa1ff28f5b83d0f96" or old_packet["successor"]["stage_2_5"]["sha256"] != sha256_file(ROOT / OLD_STAGE):
        raise RuntimeError("failed predecessor bytes no longer match the failed packet")
    stage_core = {key: value for key, value in old_stage.items() if key != "contract_id"}
    stage_core["schema_version"] = "causal_search_access_contract/2.1.0"
    stage_core["predecessor"] = {
        "contract_id": old_stage["contract_id"], "path": OLD_STAGE.as_posix(),
        "sha256": sha256_file(ROOT / OLD_STAGE),
    }
    stage_core["supersession"] = {
        "predecessor_bundle_id": OLD_BUNDLE_ID,
        "reason": "NONCANONICAL_TERMINAL_NEWLINE",
        "preservation_status": "PRESERVED_IMMUTABLE_INACTIVE_EVIDENCE",
        "scientific_reinterpretation": False,
    }
    stage = identified(stage_core, "contract_id")
    stage_path = Path("state/causal_search_access_contract_registry") / str(stage["contract_id"]) / "contract.json"
    write_new_canonical_authority(ROOT / stage_path, stage)
    validate_stage(stage, root=ROOT)
    stage_binding = binding(stage_path, str(stage["contract_id"]), "contract_id", str(stage["schema_version"]))

    old_ladder = load(OLD_LADDER)
    ladder = build_canonical_bias_remediation_successor_contract(
        old_ladder, predecessor_path=OLD_LADDER.as_posix(),
        predecessor_sha256=sha256_file(ROOT / OLD_LADDER), stage_2_5_binding=stage_binding,
    )
    ladder_path = Path("state/alpha_ladder_registry") / str(ladder["contract_id"]) / "universe_contract.json"
    write_new_canonical_authority(ROOT / ladder_path, ladder)
    validate_ladder(ladder)

    old_profile = yaml.safe_load((ROOT / OLD_PROFILE).read_text(encoding="utf-8"))
    assert isinstance(old_profile, dict)
    profile = build_canonical_bias_remediation_successor_profile(
        old_profile, predecessor_path=OLD_PROFILE.as_posix(),
        predecessor_sha256=sha256_file(ROOT / OLD_PROFILE),
        contract_path=ladder_path.as_posix(), contract_sha256=sha256_file(ROOT / ladder_path),
        contract_id=str(ladder["contract_id"]), stage_2_5_binding=stage_binding,
    )
    profile_path = ladder_path.with_name("alpha_tiered.yaml")
    write_new_canonical_authority(ROOT / profile_path, profile)
    validate_profile(profile, root=ROOT)

    old_historical = load(OLD_HISTORICAL)
    historical_core = {key: value for key, value in old_historical.items() if key != "design_id"}
    historical_core["schema_version"] = "inactive_alpha_historical_research_design_successor/2.0.0"
    historical_core["predecessor"] = binding(OLD_HISTORICAL, str(old_historical["design_id"]), "design_id")
    historical_core["supersession_reason"] = "NONCANONICAL_TERMINAL_NEWLINE_DEPENDENCY_REBIND"
    historical_core["stage_2_5_binding"] = stage_binding
    historical_core["ladder_binding"] = {
        "contract_id": ladder["contract_id"], "contract_path": ladder_path.as_posix(),
        "contract_sha256": sha256_file(ROOT / ladder_path), "profile_id": profile["profile_id"],
        "profile_path": profile_path.as_posix(), "profile_sha256": sha256_file(ROOT / profile_path),
    }
    historical = identified(historical_core, "design_id")
    design_root = Path("state/bias_robustness_successor_registry") / str(historical["design_id"])
    historical_path = design_root / "historical_design.json"
    write_new_canonical_authority(ROOT / historical_path, historical)

    old_execution = load(OLD_EXECUTION)
    execution_core = {key: value for key, value in old_execution.items() if key != "execution_design_id"}
    execution_core["schema_version"] = "inactive_alpha_execution_design_successor/2.0.0"
    execution_core["predecessor"] = binding(OLD_EXECUTION, str(old_execution["execution_design_id"]), "execution_design_id")
    execution_core["supersession_reason"] = "NONCANONICAL_TERMINAL_NEWLINE_DEPENDENCY_REBIND"
    execution_core["stage_2_5_contract_id"] = stage["contract_id"]
    execution_core["stage_2_5_binding"] = stage_binding
    execution_core["ladder_contract_id"] = ladder["contract_id"]
    execution = identified(execution_core, "execution_design_id")
    execution_path = design_root / "execution_design.json"
    write_new_canonical_authority(ROOT / execution_path, execution)

    artifacts = {
        "stage_2_5": binding(stage_path, str(stage["contract_id"]), "id", str(stage["schema_version"])),
        "ladder": binding(ladder_path, str(ladder["contract_id"]), "id", str(ladder["schema_version"])),
        "profile": binding(profile_path, str(profile["profile_id"]), "id", str(profile["schema_version"])),
        "historical_design": binding(historical_path, str(historical["design_id"]), "id", str(historical["schema_version"])),
        "execution_design": binding(execution_path, str(execution["execution_design_id"]), "id", str(execution["schema_version"])),
    }
    bundle_core = {
        "schema_version": "bias_robustness_canonical_successor_bundle/1.0.0",
        "state": "PREPARED_NOT_ACTIVE", "supersedes_bundle_id": OLD_BUNDLE_ID,
        "supersession_reason": "NONCANONICAL_TERMINAL_NEWLINE",
        "artifacts": artifacts,
        "dependency_order": ["stage_2_5", "ladder", "profile", "historical_design", "execution_design"],
        "scientific_semantics": "IDENTICAL_EXCEPT_LINEAGE_CANONICALIZATION_METADATA",
        "authority": {"activation": False, "rows": False, "provider": False, "challenge": False, "refit": False, "final_252": False, "forward": False, "trading": False},
    }
    bundle = identified(bundle_core, "bundle_id")
    bundle_path = Path("state/bias_robustness_canonical_successor_registry") / str(bundle["bundle_id"]) / "bundle.json"
    write_new_canonical_authority(ROOT / bundle_path, bundle)

    old_raw = (ROOT / OLD_STAGE).read_bytes()
    expected_old = canonical_authority_file_bytes(old_stage)
    first_difference = next(index for index, pair in enumerate(zip(old_raw, expected_old)) if pair[0] != pair[1]) if old_raw[:len(expected_old)] != expected_old else len(expected_old)
    failure_core = {
        "schema_version": "bias_robustness_failed_pre_activation_evidence/1.0.0",
        "status": "FAILED_PRE_ACTIVATION_NONCANONICAL_AUTHORITY_BYTES",
        "classification": "NONCANONICAL_IMMUTABLE_INACTIVE_ARTIFACT",
        "defective_stage_2_5": binding(OLD_STAGE, str(old_stage["contract_id"]), "id", str(old_stage["schema_version"])),
        "prior_bundle_id": OLD_BUNDLE_ID,
        "prior_activation_packet": {"path": OLD_PACKET.as_posix(), "sha256": sha256_file(ROOT / OLD_PACKET)},
        "prior_independent_validation": {"validation_id": "a261a50f714f6f09b9c2d4759eeb75ca0cbf936ddd4f8a5f483501476bb2ea3e", "script_path": "scripts/independent_validate_bias_robustness_successor.py", "script_sha256": sha256_file(ROOT / Path("scripts/independent_validate_bias_robustness_successor.py"))},
        "failed_pre_activation_review": {"status": "BLOCKED_PRE_ACTIVATION_VALIDATION_FAILED", "active_mutation": False, "row_access": False},
        "defect": {"actual_byte_count": len(old_raw), "expected_byte_count": len(expected_old), "first_differing_offset": first_difference, "final_bytes_hex": old_raw[-8:].hex(), "expected_final_bytes_hex": expected_old[-8:].hex(), "packet_bytes_matched": True, "packet_drift": False, "reason": "NONCANONICAL_TERMINAL_NEWLINE"},
        "root_cause": {"producer": "DIRECT_PATCH_MATERIALIZATION_OF_UNTRACKED_IMMUTABLE_FILE", "caller": "PREVIOUS_STAGE_2_5_SUCCESSOR_PREPARATION_TASK", "serializer_contract": "canonical_bytes(payload)_PLUS_CALLER_LF", "write_mode": "TEXT_PATCH_MATERIALIZATION", "duplicate_newline_cause": "SERIALIZED_AUTHORITY_LINE_PLUS_ADDITIONAL_TERMINAL_BLANK_LINE", "packet_builder": "DIRECT_FINAL_FILE_SHA256_BINDING_WITHOUT_CANONICAL_BYTE_ASSERTION", "validation_function": "independent_validate_bias_robustness_successor.load_json", "prior_validation_gap": "PARSE_IDENTITY_AND_BOUND_HASH_CHECKED_BUT_EXACT_CANONICAL_STAGE_BYTES_NOT_CHECKED", "corrected_boundary": "canonical_authority_file_bytes_AND_write_new_canonical_authority"},
        "new_successor_id": stage["contract_id"],
        "preservation": {"old_artifact": "PRESERVED_BYTE_FOR_BYTE", "old_bundle": "PRESERVED_BYTE_FOR_BYTE", "old_packet": "PRESERVED_BYTE_FOR_BYTE"},
        "authority": {"active_mutation": False, "row_access": False, "provider": False},
    }
    failure = identified(failure_core, "evidence_id")
    failure_path = Path("state/bias_robustness_failure_registry") / str(failure["evidence_id"]) / "failure.json"
    write_new_canonical_authority(ROOT / failure_path, failure)
    return {"bundle": bundle, "bundle_path": bundle_path, "failure": failure, "failure_path": failure_path}


def prepare_packet(certificate_path: Path) -> dict[str, object]:
    certificate = read_canonical_authority(ROOT / certificate_path, identity_field="validation_id")
    result = prepare_core()
    bundle = result["bundle"]
    bundle_path = result["bundle_path"]
    active = {path.as_posix(): sha256_file(ROOT / path) for path in ACTIVE_PATHS}
    paths = [Path(item["path"]) for item in bundle["artifacts"].values()]
    immutable_paths = paths + [bundle_path, result["failure_path"], certificate_path]
    packet_core = {
        "schema_version": "bias_robustness_canonical_activation_packet/1.0.0",
        "state": "PREPARED_NOT_ACTIVE", "activation": False,
        "git": {"branch": subprocess.check_output(["git", "branch", "--show-current"], cwd=ROOT, text=True).strip(), "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(), "index_sha256": sha256_bytes((ROOT / ".git/index").read_bytes())},
        "supersedes_bundle_id": OLD_BUNDLE_ID,
        "supersession_reason": "NONCANONICAL_IMMUTABLE_AUTHORITY_BYTES",
        "bundle": binding(bundle_path, str(bundle["bundle_id"]), "id", str(bundle["schema_version"])),
        "independent_validation": binding(certificate_path, str(certificate["validation_id"]), "id", str(certificate["schema_version"])),
        "failure_evidence": binding(result["failure_path"], str(result["failure"]["evidence_id"]), "id", str(result["failure"]["schema_version"])),
        "superseded_task_preparation_evidence": {path.as_posix(): sha256_file(ROOT / path) for path in SUPERSEDED_TASK_EVIDENCE},
        "superseded_task_preparation_status": "PRESERVED_INACTIVE_NOT_ACTIVATION_AUTHORITY",
        "active_preimages": active,
        "immutable_create_paths": [path.as_posix() for path in immutable_paths],
        "mutable_non_pointer_paths": [],
        "pointer_candidate_paths": [path.as_posix() for path in ACTIVE_PATHS[:3]],
        "path_count": len(immutable_paths) + 3,
        "successor_byte_count": sum((ROOT / path).stat().st_size for path in immutable_paths),
        "successor_hashes": {path.as_posix(): sha256_file(ROOT / path) for path in immutable_paths},
        "source_hashes": {path.as_posix(): sha256_file(ROOT / path) for path in TASK_SOURCE_PATHS},
        "activation_order": ["VERIFY_PREIMAGES", "VERIFY_IMMUTABLE_SUCCESSORS", "RUN_PRE_POINTER_TESTS", "WRITE_MUTABLE_NON_POINTER_PATHS", "WRITE_POINTERS_LAST", "RUN_POST_POINTER_TESTS", "RUN_INDEPENDENT_REPEAT"],
        "pointer_last_required": True,
        "rollback": {"restore_exact_preimages": True, "preserve_immutable_successors_as_inactive_evidence": True, "no_broad_git_rollback": True},
        "required_checks": {"pre_pointer": ["CANONICAL_BYTES", "DEPENDENCY_CLOSURE", "ROW_OPENER_SENTINEL", "ACTIVE_PREIMAGES"], "post_pointer": ["ACTIVE_FUTURE_STATE", "REPOSITORY_SURFACE", "GENERATED_VIEWS", "ROW_OPENER_SENTINEL"], "independent_repeat": True},
        "expectations": {"repository_surface": "EXACT", "generated_views": "EXACT", "row_opener_invocations": 0},
        "authority": {"data": False, "provider": False, "challenge": False, "refit": False, "final_252": False, "forward": False, "trading": False, "staging": False, "commit": False, "push": False},
    }
    packet = identified(packet_core, "packet_id")
    packet_path = OUTPUT_ROOT / str(packet["packet_id"]) / "activation_packet.json"
    write_new_canonical_authority(ROOT / packet_path, packet)
    return {"packet": packet, "packet_path": packet_path, **result}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--certificate", type=Path)
    args = parser.parse_args()
    result = prepare_packet(args.certificate) if args.certificate else prepare_core()
    summary = {key: value.as_posix() if isinstance(value, Path) else value.get(key + "_id", value.get("bundle_id", value.get("evidence_id"))) if isinstance(value, dict) else value for key, value in result.items()}
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
