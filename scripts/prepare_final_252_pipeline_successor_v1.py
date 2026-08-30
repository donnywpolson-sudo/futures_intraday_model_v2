"""Prepare the immutable non-active final-252 ladder and activation packet."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from pathlib import Path

from futures_rebuild.alpha_research_ladder import (
    build_active_pointer,
    build_qualification_successor_contract,
    build_qualification_successor_profile,
    validate_contract,
    validate_profile,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.final_evaluation_recalibration import (
    ACTIVATION_SCHEMA,
    build_bounded_official_facts,
    build_contamination_classification,
    build_human_attestation,
    build_independent_certificate,
    build_machine_contamination_audit,
    build_manifest,
    require_canonical_file,
    trade_dates_from_bounded_facts,
    validate_bounded_official_facts,
    validate_independent_certificate,
    validate_manifest,
    restoration_disposition,
)


ROOT = Path(__file__).resolve().parents[1]
PREP = ROOT / "state/data_publication_staging/final_evaluation_session_manifest/purpose_limited_final_252_v2/preparation"
ACTIVE_POINTER_PATH = ROOT / "configs/active_alpha_research_ladder.json"


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


def _source_documents() -> list[dict[str, object]]:
    return [
        {
            "publisher": "CME_GROUP", "document_id": "MSN11-29-24",
            "url": "https://www.cmegroup.com/content/dam/cmegroup/notices/market-regulation/2024/11/msn11-29-2024holidayprocessinglargetrader2025.pdf",
            "bounded_fact": "2025_CME_OBSERVED_HOLIDAY_TRADE_DATES",
        },
        {
            "publisher": "CME_GROUP", "document_id": "MSN01-12-26",
            "url": "https://www.cmegroup.com/content/dam/cmegroup/notices/market-regulation/2026/01/msn01-09-2026revisedholidayprocessinglargetrader2026.pdf",
            "bounded_fact": "2026_CME_OBSERVED_HOLIDAY_TRADE_DATES",
        },
        {
            "publisher": "CME_GROUP",
            "document_id": "2026_GOOD_FRIDAY_CLEARING_ADVISORY",
            "url": "https://www.cmegroup.com/tools-information/holiday-calendar/files/2026/2026-good-friday-clearing-advisory.pdf",
            "bounded_fact": "CME_TRANSMITS_TRADES_WITH_TRADE_DATE_2026_04_03",
        },
    ]


def _contamination_audit(
    *, root: Path, manifest: dict[str, object],
) -> dict[str, object]:
    session_ids = {
        str(item["trade_date"]) for item in manifest["sessions"]
    }
    result_keys = re.compile(
        r'"(?:net_pnl|sharpe|returns|model_comparison|position_size|performance_result)"',
        re.IGNORECASE,
    )
    scanned: list[str] = []
    overlaps: list[object] = []
    parse_failures: list[object] = []
    for top in ("reports", "state", "configs"):
        for path in (root / top).rglob("*"):
            if not path.is_file() or path.suffix.lower() not in {
                ".json", ".md", ".txt", ".yaml", ".yml",
            }:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            matched_sessions = sorted(value for value in session_ids if value in text)
            if not matched_sessions:
                continue
            relative = path.relative_to(root).as_posix()
            scanned.append(relative)
            if path.suffix.lower() == ".json":
                try:
                    json.loads(text)
                except ValueError as exc:
                    parse_failures.append({"path": relative, "error": type(exc).__name__})
                    continue
            if result_keys.search(text):
                overlaps.append({"path": relative, "session_ids": matched_sessions})
    history = subprocess.run(
        ["git", "log", "--all", "--format=%H", "-G", "2025-07-14|2026-07-13", "--", "reports", "state", "configs"],
        cwd=root, check=True, capture_output=True, text=True,
    ).stdout.splitlines()
    return build_machine_contamination_audit(
        manifest=manifest, scanned_paths=scanned,
        scanned_git_commits=sorted(set(history)),
        exact_session_overlap_records=overlaps,
        metadata_parse_failures=parse_failures,
    )


def prepare_inputs(preparation_root: Path = PREP) -> int:
    facts = build_bounded_official_facts(source_documents=_source_documents())
    manifest = build_manifest(
        trade_dates=trade_dates_from_bounded_facts(facts),
        bindings={
            "bounded_source_facts_id": facts["bounded_source_facts_id"],
            "source_bundle_id": facts["source_bundle_id"],
            "semantic_contract_id": facts["semantic_contract_id"],
        },
    )
    audit = _contamination_audit(root=ROOT, manifest=manifest)
    write_create_only(preparation_root / "bounded_official_facts.json", facts)
    write_create_only(preparation_root / "final_252_session_manifest.json", manifest)
    write_create_only(preparation_root / "contamination_audit.json", audit)
    print(canonical_bytes({
        "bounded_source_facts_id": facts["bounded_source_facts_id"],
        "manifest_id": manifest["manifest_id"],
        "ordered_session_sha256": manifest["ordered_session_sha256"],
        "audit_id": audit["audit_id"],
        "restoration_disposition": restoration_disposition(manifest),
    }).decode("utf-8"))
    return 0


def certify_inputs(preparation_root: Path = PREP) -> int:
    facts = json.loads(
        (preparation_root / "bounded_official_facts.json").read_text(encoding="utf-8")
    )
    manifest = json.loads(
        (preparation_root / "final_252_session_manifest.json").read_text(encoding="utf-8")
    )
    validate_bounded_official_facts(facts)
    validate_manifest(manifest)
    certificate = build_independent_certificate(manifest=manifest, facts=facts)
    validate_independent_certificate(certificate, manifest=manifest, facts=facts)
    write_create_only(preparation_root / "independent_certificate.json", certificate)
    print(canonical_bytes({"certificate_id": certificate["certificate_id"]}).decode("utf-8"))
    return 0


def validate_activation(preparation_root: Path = PREP) -> int:
    packet = json.loads(
        (preparation_root / "pipeline_activation_packet_v2.json").read_text(
            encoding="utf-8"
        )
    )
    core = dict(packet)
    packet_id = core.pop("activation_packet_id", None)
    if (
        packet_id != sha256_json(core)
        or packet.get("schema_version") != ACTIVATION_SCHEMA
        or packet.get("state")
        != "PREPARED_PUBLICATION_AND_ACTIVE_POINTER_APPROVAL_REQUIRED"
    ):
        raise ValueError("activation packet identity is invalid")
    rollback = packet["rollback"]
    if sha256_file(ACTIVE_POINTER_PATH) != rollback["predecessor_sha256"]:
        raise ValueError("active Alpha pointer drifted")
    ladder = packet["ladder_successor"]
    contract_path = ROOT / ladder["contract_path"]
    profile_path = ROOT / ladder["profile_path"]
    if (
        sha256_file(contract_path) != ladder["contract_sha256"]
        or sha256_file(profile_path) != ladder["profile_sha256"]
    ):
        raise ValueError("prepared ladder successor drifted")
    contract = validate_contract(json.loads(contract_path.read_text(encoding="utf-8")))
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    validate_profile(profile, root=ROOT, prepared_contract_path=contract_path)
    if (
        contract["contract_id"] != ladder["contract_id"]
        or profile["profile_id"] != ladder["profile_id"]
    ):
        raise ValueError("prepared ladder identities drifted")
    manifest_path = ROOT / packet["manifest_binding"]["manifest_path"]
    facts_path = manifest_path.with_name("bounded_official_facts.json")
    certificate_path = ROOT / packet["manifest_binding"]["independent_certificate_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    facts = json.loads(facts_path.read_text(encoding="utf-8"))
    certificate = json.loads(certificate_path.read_text(encoding="utf-8"))
    validate_manifest(manifest)
    validate_bounded_official_facts(facts)
    validate_independent_certificate(certificate, manifest=manifest, facts=facts)
    catalog = json.loads((ROOT / "data/active/catalog.json").read_text(encoding="utf-8"))
    ready = [item for item in catalog["entries"] if item["disposition"] == "RESEARCH_READY_CAUSAL_PRICE"]
    markets = {item["market"] for item in ready}
    if (
        len(ready) != 136
        or markets != {"ES", "NQ", "CL", "GC", "ZN", "6E", "SR3", "ZB", "6J"}
    ):
        raise ValueError("active nine-market catalog drifted")
    validation_core: dict[str, object] = {
        "schema_version": "final_252_pipeline_activation_validation/1.0.0",
        "status": "INDEPENDENTLY_VALIDATED_APPROVAL_REQUIRED",
        "activation_packet_id": packet_id,
        "active_predecessor_pointer_sha256": rollback["predecessor_sha256"],
        "manifest_id": manifest["manifest_id"],
        "certificate_id": certificate["certificate_id"],
        "contract_id": contract["contract_id"],
        "profile_id": profile["profile_id"],
        "active_catalog_market_count": len(markets),
        "active_catalog_market_year_count": len(ready),
        "pointer_mutated": False,
        "holdout_or_forward_access": False,
    }
    validation = identified(validation_core, "validation_id")
    validation_path = preparation_root / "pipeline_activation_validation_v2.json"
    if validation_path.exists():
        require_canonical_file(validation_path, validation)
    else:
        write_create_only(validation_path, validation)
    print(canonical_bytes(validation).decode("utf-8"))
    return 0


def activate_approved(preparation_root: Path = PREP) -> int:
    validate_activation(preparation_root)
    packet = json.loads(
        (preparation_root / "pipeline_activation_packet_v2.json").read_text(
            encoding="utf-8"
        )
    )
    ladder = packet["ladder_successor"]
    desired = ladder["desired_active_pointer"]
    rollback = packet["rollback"]
    if (
        sha256_file(ACTIVE_POINTER_PATH) != rollback["predecessor_sha256"]
        or json.loads(ACTIVE_POINTER_PATH.read_text(encoding="utf-8"))[
            "pointer_id"
        ]
        != rollback["predecessor_pointer_id"]
    ):
        raise ValueError("active Alpha predecessor changed before activation")
    receipt_path = (
        ROOT / "state/alpha_ladder_registry" / ladder["contract_id"]
        / "final_252_activation_receipt.json"
    )
    if receipt_path.exists():
        raise FileExistsError(f"create-only destination exists: {receipt_path}")

    from futures_rebuild.cash_open_calendar_publication import (
        _activate_pointer_with_rollback,
    )
    from futures_rebuild.repository_surface import (
        validate_active_alpha_authority_closure,
        validate_repository_checkout,
    )

    def postcheck() -> None:
        active = json.loads(ACTIVE_POINTER_PATH.read_text(encoding="utf-8"))
        if active != desired:
            raise ValueError("active Alpha pointer read-back differs")
        closure = validate_active_alpha_authority_closure(ROOT)
        validate_repository_checkout(ROOT)
        receipt_core: dict[str, object] = {
            "schema_version": "final_252_alpha_ladder_activation_receipt/1.0.0",
            "status": "ACTIVE_NON_AUTHORIZING_FINAL_252_AUTHORITY",
            "activation_packet_id": packet["activation_packet_id"],
            "predecessor_pointer_id": rollback["predecessor_pointer_id"],
            "predecessor_pointer_sha256": rollback["predecessor_sha256"],
            "active_pointer_id": desired["pointer_id"],
            "active_pointer_sha256": sha256_file(ACTIVE_POINTER_PATH),
            "contract_id": closure["contract_id"],
            "profile_id": closure["profile_id"],
            "manifest_id": packet["manifest_binding"]["manifest_id"],
            "ordered_session_sha256": packet["manifest_binding"][
                "ordered_session_sha256"
            ],
            "authority": {
                "holdout_value_access": False, "forward_value_access": False,
                "evaluation": False, "row_read": False, "trading": False,
            },
        }
        write_create_only(receipt_path, identified(receipt_core, "receipt_id"))

    _activate_pointer_with_rollback(
        pointer_path=ACTIVE_POINTER_PATH,
        new_bytes=canonical_bytes(desired) + b"\n",
        postcheck=postcheck,
    )
    print(canonical_bytes({
        "active_pointer_id": desired["pointer_id"],
        "active_pointer_sha256": sha256_file(ACTIVE_POINTER_PATH),
        "receipt_path": receipt_path.relative_to(ROOT).as_posix(),
    }).decode("utf-8"))
    return 0


def main(preparation_root: Path = PREP) -> int:
    preparation_root = preparation_root.resolve()
    successor = preparation_root / "pipeline_successor_v2"
    manifest_path = preparation_root / "final_252_session_manifest.json"
    bounded_facts_path = preparation_root / "bounded_official_facts.json"
    certificate_path = preparation_root / "independent_certificate.json"
    audit_path = preparation_root / "contamination_audit.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validate_manifest(manifest)
    disposition = restoration_disposition(manifest)
    final_registry = (
        ROOT / "state/final_evaluation_session_manifest_registry"
        / str(manifest["manifest_id"])
    )
    for destination in (successor, final_registry):
        if destination.exists():
            raise FileExistsError(f"create-only destination exists: {destination}")
    certificate = json.loads(certificate_path.read_text(encoding="utf-8"))
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
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
    copy_create_only(manifest_path, final_registry / "final_252_session_manifest.json")
    copy_create_only(bounded_facts_path, final_registry / "bounded_official_facts.json")
    copy_create_only(certificate_path, final_registry / "independent_certificate.json")
    copy_create_only(audit_path, final_registry / "machine_contamination_audit.json")
    write_create_only(final_registry / "human_use_attestation.json", attestation)
    write_create_only(final_registry / "contamination_classification.json", classification)

    active_pointer = json.loads(ACTIVE_POINTER_PATH.read_text(encoding="utf-8"))
    active_contract = json.loads(
        (ROOT / active_pointer["contract_path"]).read_text(encoding="utf-8")
    )
    failed = active_contract["prior_failed_mechanism"]
    final_binding = {
        "manifest_path": (final_registry / "final_252_session_manifest.json").relative_to(ROOT).as_posix(),
        "manifest_sha256": sha256_file(final_registry / "final_252_session_manifest.json"),
        "manifest_id": manifest["manifest_id"],
        "ordered_session_sha256": manifest["ordered_session_sha256"],
        "session_count": 252,
        "first_session_id": "2025-07-14",
        "last_session_id": "2026-07-13",
        "development_end_exclusive": manifest["development_end_exclusive"],
        "forward_start": manifest["forward_start"],
        "independent_certificate_path": (final_registry / "independent_certificate.json").relative_to(ROOT).as_posix(),
        "independent_certificate_sha256": sha256_file(final_registry / "independent_certificate.json"),
        "independent_certificate_id": certificate["certificate_id"],
        "contamination_classification_path": (final_registry / "contamination_classification.json").relative_to(ROOT).as_posix(),
        "contamination_classification_sha256": sha256_file(final_registry / "contamination_classification.json"),
        "contamination_classification_id": classification["classification_id"],
        "classification": "RESEARCH_SELECTION_PRISTINE",
        "nomenclature": "Final Sealed 252-Session Holdout",
        "general_historical_session_authority": False,
        "evaluation_authority": False,
        "restoration_disposition": disposition,
    }
    contract = build_qualification_successor_contract(
        predecessor_path=active_pointer["contract_path"],
        predecessor_sha256=active_pointer["contract_sha256"],
        predecessor_contract_id=active_pointer["contract_id"],
        predecessor_profile_id=active_pointer["profile_id"],
        predecessor_pointer_id=active_pointer["pointer_id"],
        prior_failure_closure_id=failed["closure_id"],
        prior_failed_mechanism_id=failed["mechanism_id"],
        final_252_binding=final_binding,
    )
    validate_contract(contract)
    registry = ROOT / "state/alpha_ladder_registry" / contract["contract_id"]
    contract_path = registry / "universe_contract.json"
    write_create_only(contract_path, contract)
    profile = build_qualification_successor_profile(
        contract_path=contract_path.relative_to(ROOT).as_posix(),
        contract_sha256=sha256_file(contract_path), contract_id=contract["contract_id"],
        final_252_binding=final_binding,
    )
    profile_path = registry / "alpha_tiered.yaml"
    write_create_only(profile_path, profile)
    validate_profile(profile, root=ROOT, prepared_contract_path=contract_path)
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
    write_create_only(successor / "documentation_successor.json", documentation)

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
            "path": (final_registry / "human_use_attestation.json").relative_to(ROOT).as_posix(),
            "sha256": sha256_file(final_registry / "human_use_attestation.json"),
            "attestation_id": attestation["attestation_id"],
        },
        "ladder_successor": {
            "contract_path": contract_path.relative_to(ROOT).as_posix(), "contract_sha256": sha256_file(contract_path), "contract_id": contract["contract_id"],
            "profile_path": profile_path.relative_to(ROOT).as_posix(), "profile_sha256": sha256_file(profile_path), "profile_id": profile["profile_id"],
            "desired_active_pointer": desired_pointer,
        },
        "documentation_successor": {
            "path": (successor / "documentation_successor.json").relative_to(ROOT).as_posix(),
            "sha256": sha256_file(successor / "documentation_successor.json"),
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
    write_create_only(preparation_root / "pipeline_activation_packet_v2.json", activation)
    print(canonical_bytes({
        "attestation_id": attestation["attestation_id"], "classification_id": classification["classification_id"],
        "contract_id": contract["contract_id"], "profile_id": profile["profile_id"],
        "documentation_successor_id": documentation["documentation_successor_id"],
        "activation_packet_id": activation["activation_packet_id"],
    }).decode("utf-8"))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--preparation-root", type=Path, default=PREP)
    parser.add_argument("--prepare-inputs", action="store_true")
    parser.add_argument("--certify-inputs", action="store_true")
    parser.add_argument("--validate-activation", action="store_true")
    parser.add_argument("--activate-approved", action="store_true")
    arguments = parser.parse_args()
    if sum((arguments.prepare_inputs, arguments.certify_inputs, arguments.validate_activation, arguments.activate_approved)) > 1:
        parser.error("choose one preparation mode")
    if arguments.prepare_inputs:
        raise SystemExit(prepare_inputs(arguments.preparation_root))
    if arguments.certify_inputs:
        raise SystemExit(certify_inputs(arguments.preparation_root))
    if arguments.validate_activation:
        raise SystemExit(validate_activation(arguments.preparation_root))
    if arguments.activate_approved:
        raise SystemExit(activate_approved(arguments.preparation_root))
    raise SystemExit(main(arguments.preparation_root))
