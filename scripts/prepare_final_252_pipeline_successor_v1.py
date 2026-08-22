"""Prepare the immutable non-active final-252 ladder and activation packet."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.alpha_research_ladder import build_active_pointer
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.final_evaluation_recalibration import (
    ACTIVATION_SCHEMA,
    build_contamination_classification,
    build_contract,
    build_human_attestation,
    build_profile,
    require_canonical_file,
    validate_contract,
    validate_manifest,
    validate_profile,
)


ROOT = Path(__file__).resolve().parents[1]
PREP = ROOT / "state/data_publication_staging/final_evaluation_session_manifest/purpose_limited_final_252_v2/preparation"
SUCCESSOR = PREP / "pipeline_successor_v2"
MANIFEST_PATH = PREP / "final_252_session_manifest.json"
BOUNDED_FACTS_PATH = PREP / "bounded_official_facts.json"
CERTIFICATE_PATH = PREP / "independent_certificate.json"
AUDIT_PATH = PREP / "contamination_audit.json"
FINAL_REGISTRY = ROOT / "state/final_evaluation_session_manifest_registry/0ff48f99d8b6d3a262ddf0a060bea8e733fc95aa7c4b4d43f19a0f78b107d4d1"
ACTIVE_POINTER_PATH = ROOT / "configs/active_alpha_research_ladder.json"
FAILED_CLOSURE_PATH = ROOT / "state/trial_registry/alpha_ladder_es_pilot_terminal_closure/6b9ab13e0f1400af9f3dc5abce99d9afe9540bd439bbc38685a04d62ccef44c7.json"


def write_create_only(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    fd = os.open(path, flags)
    try:
        os.write(fd, canonical_bytes(value) + b"\n")
        os.fsync(fd)
    finally:
        os.close(fd)


def copy_create_only(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    fd = os.open(destination, flags)
    try:
        os.write(fd, source.read_bytes())
        os.fsync(fd)
    finally:
        os.close(fd)


def identified(core: dict[str, object], field: str) -> dict[str, object]:
    return {**core, field: sha256_json(core)}


def main() -> int:
    for destination in (SUCCESSOR, FINAL_REGISTRY):
        if destination.exists():
            raise FileExistsError(f"create-only destination exists: {destination}")
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    certificate = json.loads(CERTIFICATE_PATH.read_text(encoding="utf-8"))
    audit = json.loads(AUDIT_PATH.read_text(encoding="utf-8"))
    validate_manifest(manifest)
    if certificate["status"] != "CERTIFIED_NON_ACTIVE" or certificate["manifest_id"] != manifest["manifest_id"]:
        raise ValueError("independent certificate does not bind the manifest")
    if audit["machine_evidence_result"] != "NO_RESEARCH_SELECTION_ACCESS_PROVEN" or audit["exact_session_overlap_records"] or audit["metadata_parse_failures"]:
        raise ValueError("machine contamination audit is not clean")

    attestation = build_human_attestation(
        manifest_id=manifest["manifest_id"],
        ordered_session_sha256=manifest["ordered_session_sha256"],
        machine_audit_id=audit["audit_id"],
    )
    classification = build_contamination_classification(
        manifest=manifest,
        certificate_id=certificate["certificate_id"],
        machine_audit=audit,
        attestation=attestation,
    )
    copy_create_only(MANIFEST_PATH, FINAL_REGISTRY / "final_252_session_manifest.json")
    copy_create_only(BOUNDED_FACTS_PATH, FINAL_REGISTRY / "bounded_official_facts.json")
    copy_create_only(CERTIFICATE_PATH, FINAL_REGISTRY / "independent_certificate.json")
    copy_create_only(AUDIT_PATH, FINAL_REGISTRY / "machine_contamination_audit.json")
    write_create_only(FINAL_REGISTRY / "human_use_attestation.json", attestation)
    write_create_only(FINAL_REGISTRY / "contamination_classification.json", classification)

    active_pointer = json.loads(ACTIVE_POINTER_PATH.read_text(encoding="utf-8"))
    failed = json.loads(FAILED_CLOSURE_PATH.read_text(encoding="utf-8"))
    final_binding = {
        "manifest_path": (FINAL_REGISTRY / "final_252_session_manifest.json").relative_to(ROOT).as_posix(),
        "manifest_sha256": sha256_file(FINAL_REGISTRY / "final_252_session_manifest.json"),
        "manifest_id": manifest["manifest_id"],
        "ordered_session_sha256": manifest["ordered_session_sha256"],
        "session_count": 252,
        "first_session_id": "2025-07-14",
        "last_session_id": "2026-07-13",
        "development_end_exclusive": manifest["development_end_exclusive"],
        "forward_start": manifest["forward_start"],
        "independent_certificate_path": (FINAL_REGISTRY / "independent_certificate.json").relative_to(ROOT).as_posix(),
        "independent_certificate_sha256": sha256_file(FINAL_REGISTRY / "independent_certificate.json"),
        "independent_certificate_id": certificate["certificate_id"],
        "contamination_classification_path": (FINAL_REGISTRY / "contamination_classification.json").relative_to(ROOT).as_posix(),
        "contamination_classification_sha256": sha256_file(FINAL_REGISTRY / "contamination_classification.json"),
        "contamination_classification_id": classification["classification_id"],
        "classification": "RESEARCH_SELECTION_PRISTINE",
        "nomenclature": "Final Sealed 252-Session Holdout",
        "general_historical_session_authority": False,
        "evaluation_authority": False,
    }
    predecessor = {
        "path": active_pointer["contract_path"], "sha256": active_pointer["contract_sha256"],
        "contract_id": active_pointer["contract_id"], "preserved_byte_for_byte": True,
    }
    failed_binding = {
        "mechanism_id": failed["mechanism_id"], "trial_id": failed["trial_id"],
        "closure_id": failed["closure_id"], "closure_path": FAILED_CLOSURE_PATH.relative_to(ROOT).as_posix(),
        "closure_sha256": sha256_file(FAILED_CLOSURE_PATH), "status": "CLOSED_FAILED_AT_TIER_0_ES_QUALIFICATION",
        "retry_authorized": False, "tier_1_advancement_authorized": False,
    }
    contract = build_contract(predecessor=predecessor, final_binding=final_binding, failed_mechanism=failed_binding)
    validate_contract(contract)
    registry = ROOT / "state/alpha_ladder_registry" / contract["contract_id"]
    contract_path = registry / "universe_contract.json"
    write_create_only(contract_path, contract)
    profile = build_profile(
        contract_path=contract_path.relative_to(ROOT).as_posix(),
        contract_sha256=sha256_file(contract_path), contract_id=contract["contract_id"],
        final_binding=final_binding,
    )
    profile_path = registry / "alpha_tiered.yaml"
    write_create_only(profile_path, profile)
    validate_profile(profile, contract=contract, contract_path=contract_path)
    require_canonical_file(contract_path, contract)
    require_canonical_file(profile_path, profile)

    desired_pointer = build_active_pointer(
        contract_path=contract_path.relative_to(ROOT).as_posix(), contract_sha256=sha256_file(contract_path),
        contract_id=contract["contract_id"], profile_path=profile_path.relative_to(ROOT).as_posix(),
        profile_sha256=sha256_file(profile_path), profile_id=profile["profile_id"],
    )
    doc_section = (
        "## Certified final-evaluation boundary\n\n"
        "The project uses one research-selection-pristine **Final Sealed 252-Session Holdout**: "
        "trade dates 2025-07-14 through 2026-07-13, manifest "
        f"`{manifest['manifest_id']}`. Development ends exclusively at "
        "2025-07-13T22:00:00Z and forward monitoring begins at 2026-07-14T00:00:00Z. "
        "The manifest is purpose-limited, grants no row or evaluation access, creates no market-specific "
        "or micro holdout, and is not a general exchange calendar. Complete 2018-cutoff project-session "
        "continuity remains unresolved for portions of 2023-2024 and is not claimed.\n\n"
        "The user-facing pipeline is: Canonical Source Foundation; Research Design and Mechanism Freeze; "
        "Tier 0 Engineering and ES Qualification; Tier 1 Four-Market Confirmation; Tier 2 Balanced "
        "16-Market Replication; Tier 3 Full 41-Market Replication; Final Project-Level 252-Session "
        "Evaluation; Post-Cutoff Forward Monitoring. Existing Phase 1A-11 labels remain internal "
        "synthetic/capability terminology only. The previous counted mechanism remains closed after "
        "Tier-0 ES failure, and the next mechanism is not started.\n"
    )
    documentation_core: dict[str, object] = {
        "schema_version": "final_252_documentation_successor/1.0.0",
        "state": "PREPARED_NOT_APPLIED",
        "targets": [
            {"path": name, "predecessor_sha256": sha256_file(ROOT / name), "operation": "APPEND_OR_RECONCILE_SINGLE_CURRENT_SECTION", "required_section": doc_section}
            for name in ("PROJECT_OUTLINE.md", "README.md", "CURRENT_WORKFLOW.md")
        ],
        "repository_surface_followup": {
            "registry_path": "configs/repository_surface.json",
            "predecessor_sha256": sha256_file(ROOT / "configs/repository_surface.json"),
            "add_exact_entries_for": [
                "src/futures_rebuild/final_evaluation_recalibration.py",
                "scripts/prepare_final_252_pipeline_successor_v1.py",
                "tests/test_final_evaluation_recalibration.py",
            ],
            "regenerate": ["SOURCE_OF_TRUTH.md", "PIPELINE_FOLDER_MAP.md", "ACTIVE_SOURCE_FILES.txt"],
            "generator": "python -m futures_rebuild.repository_surface --print-<view>",
        },
        "authority": {"publication": False, "active_pointer": False, "git": False},
    }
    documentation = identified(documentation_core, "documentation_successor_id")
    write_create_only(SUCCESSOR / "documentation_successor.json", documentation)

    activation_core: dict[str, object] = {
        "schema_version": ACTIVATION_SCHEMA,
        "state": "PREPARED_PUBLICATION_AND_ACTIVE_POINTER_APPROVAL_REQUIRED",
        "manifest_binding": final_binding,
        "semantic_contract_id": manifest["bindings"]["semantic_contract_id"],
        "bounded_source_facts_id": manifest["bindings"]["bounded_source_facts_id"],
        "source_bundle_id": manifest["bindings"]["source_bundle_id"],
        "contamination_classification": "RESEARCH_SELECTION_PRISTINE",
        "nomenclature": "Final Sealed 252-Session Holdout",
        "human_attestation": {
            "path": (FINAL_REGISTRY / "human_use_attestation.json").relative_to(ROOT).as_posix(),
            "sha256": sha256_file(FINAL_REGISTRY / "human_use_attestation.json"),
            "attestation_id": attestation["attestation_id"],
        },
        "ladder_successor": {
            "contract_path": contract_path.relative_to(ROOT).as_posix(), "contract_sha256": sha256_file(contract_path), "contract_id": contract["contract_id"],
            "profile_path": profile_path.relative_to(ROOT).as_posix(), "profile_sha256": sha256_file(profile_path), "profile_id": profile["profile_id"],
            "desired_active_pointer": desired_pointer,
        },
        "documentation_successor": {
            "path": (SUCCESSOR / "documentation_successor.json").relative_to(ROOT).as_posix(),
            "sha256": sha256_file(SUCCESSOR / "documentation_successor.json"),
            "documentation_successor_id": documentation["documentation_successor_id"],
        },
        "activation_actions_in_order": [
            "REVALIDATE_ALL_BOUND_HASHES_AND_NO_CONFLICTING_WRITER",
            "APPLY_CURRENT_DOCUMENTATION_AND_REPOSITORY_SURFACE_SUCCESSORS",
            "REGENERATE_OWNED_VIEWS",
            "RUN_TARGETED_AND_CURRENT_HIGH_RISK_TESTS",
            "WRITE_CONFIGS_ACTIVE_ALPHA_RESEARCH_LADDER_LAST_AND_ATOMICALLY",
            "READ_BACK_AND_REVALIDATE",
        ],
        "exact_current_files_and_pointers_to_change": [
            "PROJECT_OUTLINE.md", "README.md", "CURRENT_WORKFLOW.md",
            "configs/repository_surface.json", "SOURCE_OF_TRUTH.md",
            "PIPELINE_FOLDER_MAP.md", "ACTIVE_SOURCE_FILES.txt",
            "configs/active_alpha_research_ladder.json",
        ],
        "rollback": {"active_pointer_path": "configs/active_alpha_research_ladder.json", "predecessor_sha256": sha256_file(ACTIVE_POINTER_PATH), "predecessor_pointer_id": active_pointer["pointer_id"], "restore_current_files_if_activation_validation_fails": True},
        "limitations": ["COMPLETE_2018_CUTOFF_SESSION_INDEX_REMAINS_UNRESOLVED_2023_2024", "NO_GENERAL_HISTORICAL_SESSION_POINTER_CHANGE"],
        "zero_access_and_mutation": {"provider_calls": 0, "protected_value_reads": 0, "canonical_dbn_mutations": 0, "mechanism_executions": 0, "general_historical_pointer_mutations": 0, "git_stage_commit_push_actions": 0},
        "approval_required": "PUBLISH_SUCCESSORS_APPLY_CURRENT_DOCUMENTATION_AND_ATOMICALLY_ACTIVATE_ALPHA_LADDER_POINTER",
    }
    activation = identified(activation_core, "activation_packet_id")
    write_create_only(PREP / "pipeline_activation_packet_v2.json", activation)
    print(canonical_bytes({
        "attestation_id": attestation["attestation_id"], "classification_id": classification["classification_id"],
        "contract_id": contract["contract_id"], "profile_id": profile["profile_id"],
        "documentation_successor_id": documentation["documentation_successor_id"],
        "activation_packet_id": activation["activation_packet_id"],
    }).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
