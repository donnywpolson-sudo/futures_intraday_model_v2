"""Explain standard and micro data-folder authority without opening payloads."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError
if __package__:
    from scripts.audit_standard_data_topology_source_safe import build_report as build_v1
else:
    from audit_standard_data_topology_source_safe import build_report as build_v1


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/data_topology_source_safe_audit_v2/report.json"
)
CUSTODY_TERMINAL = Path(
    "state/unpublished_evidence/apex_micro_v24_custody_repair_v2/terminal.json"
)
ACTIVE_MICRO_POINTER = Path("configs/active_micro_alpha_research_ladder.json")
ACTIVE_MICRO_CATALOG = Path("data/active/catalogs/apex_micro.json")


ROOT_ROLES = {
    "active": "AUTHORITATIVE_CATALOG_RESOLUTION_ROOT",
    "causally_gated_normalized": "CONTENT_ADDRESSED_IMMUTABLE_PHASE2_RELEASE_HISTORY",
    "dbn": "PHASE1A_SOURCE_CUSTODY_NOT_RESEARCH_EVIDENCE",
    "raw": "CONTENT_ADDRESSED_IMMUTABLE_PHASE1B_RELEASE_HISTORY",
    "market_state": "DIAGNOSTIC_SOURCE_RELEASE_HISTORY",
    "outcome_sources": "EXECUTION_EVIDENCE_SOURCE_RELEASE_HISTORY",
    "reference": "VERSIONED_REFERENCE_DATA",
    "vault": "IMMUTABLE_SOURCE_SNAPSHOT_VAULT",
    "features": "PROTECTED_DERIVED_OR_LEGACY_NO_DIRECT_CURRENT_ROUTE",
    "outcomes": "PROTECTED_DERIVED_OR_LEGACY_NO_DIRECT_CURRENT_ROUTE",
    "predictions": "PROTECTED_DERIVED_OR_LEGACY_NO_DIRECT_CURRENT_ROUTE",
    "evaluations": "PROTECTED_DERIVED_OR_LEGACY_NO_DIRECT_CURRENT_ROUTE",
    "status_eligibility": "PROTECTED_DERIVED_OR_LEGACY_NO_DIRECT_CURRENT_ROUTE",
}


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{description} is not an object")
    return value


def _inventory(path: Path) -> dict[str, int]:
    files = [item for item in path.rglob("*") if item.is_file()]
    links = [item for item in files if item.is_symlink()]
    return {
        "file_count": len(files),
        "symlink_file_count": len(links),
        "byte_count_from_filesystem_metadata": sum(
            item.stat().st_size for item in files if not item.is_symlink()
        ),
    }


def build_report(*, root: Path = ROOT) -> dict[str, object]:
    root = root.resolve(strict=True)
    standard = build_v1(root=root)
    terminal_path = root / CUSTODY_TERMINAL
    terminal = _object(terminal_path, "micro custody terminal")
    terminal_core = dict(terminal)
    terminal_id = terminal_core.pop("terminal_id", None)
    if (
        terminal_id != sha256_json(terminal_core)
        or terminal.get("state") != "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY_REPAIRED"
        or terminal.get("completed_alias_removal_count") != 320
        or terminal.get("dbn_rows_decoded") != 0
        or terminal.get("payloads_opened_for_row_access") != 0
        or terminal.get("catalog_or_pointer_activated") is not False
    ):
        raise IntegrityError("micro custody evidence drifted")
    if (root / ACTIVE_MICRO_POINTER).exists() or (root / ACTIVE_MICRO_CATALOG).exists():
        raise IntegrityError("micro pointer or catalog unexpectedly exists")

    data_root = root / "data"
    observed = sorted(item.name for item in data_root.iterdir() if item.is_dir())
    roles = {
        name: {
            "role": ROOT_ROLES.get(
                name, "UNCLASSIFIED_REQUIRES_REVIEW_NOT_AN_ACTIVE_SOURCE"
            ),
            "active_by_directory_presence": False,
            "inventory": _inventory(data_root / name),
        }
        for name in observed
    }
    roles["active"]["active_by_directory_presence"] = False
    roles["active"]["resolution_rule"] = "RESOLVE_ONLY_THROUGH_DATA_ACTIVE_CATALOG_JSON"

    core: dict[str, object] = {
        "schema_version": "data_topology_source_safe_audit/2.0.0",
        "state": "PASS_SOURCE_SAFE_AUTHORITY_AND_FOLDER_ROLE_AUDIT",
        "standard_lane": {
            "source_of_truth": "data/active/catalog.json",
            "catalog_sha256": standard["catalog"]["file_sha256"],
            "active_market_year_count": standard["catalog"]["active_market_year_count"],
            "market_count": len(standard["markets_observed"]),
            "years": standard["years_observed"],
            "phase1b_release_history": "data/raw",
            "phase2_release_history": "data/causally_gated_normalized",
            "catalog_selected_active_view": "data/active/causally_gated_normalized",
            "duplicate_named_phase2_roots_are_conflicting_active_sources": False,
            "lineage": standard["lineage"],
        },
        "micro_lane": {
            "state": "PHASE1A_INACTIVE_CUSTODY_COMPLETE_PHASE1B2_PREPARE_ONLY",
            "phase1a_source_root": "data/dbn",
            "custody_terminal_path": CUSTODY_TERMINAL.as_posix(),
            "custody_terminal_id": terminal_id,
            "custody_terminal_sha256": sha256_file(terminal_path),
            "dbn_count": 160,
            "adjacent_sidecar_count": 160,
            "dbn_bytes": 1_849_575_228,
            "active_pointer": "ABSENT",
            "active_catalog": "ABSENT",
            "raw_files_are_research_evidence": False,
        },
        "data_root_inventory": roles,
        "authority_rules": {
            "standard_lane_resolves_only_through_active_catalog": True,
            "micro_lane_has_no_active_resolution": True,
            "release_history_may_have_multiple_immutable_generations": True,
            "folder_name_or_presence_never_proves_gate_passage": True,
            "cleanup_may_not_merge_delete_move_or_relabel_data_roots": True,
            "actual_cleanup_requires_separate_exact_candidate_manifest_and_approval": True,
        },
        "cleanup_conclusion": {
            "data_root_cleanup_candidate_count": 0,
            "standard_phase2_roots_require_merge": False,
            "recommended_current_action": "PRESERVE_ALL_DATA_ROOTS_AND_USE_CATALOG_AUTHORITY",
            "cache_cleanup_deferred_until_AFTER_PIPELINE_WRITES_AND_EXACT_REVALIDATION": True,
        },
        "payload_safety": {
            "dbn_payloads_opened": 0,
            "parquet_payloads_opened": 0,
            "historical_rows_read": 0,
            "payload_hashes_recomputed": False,
            "year_2025_or_2026_payload_opened": False,
            "inventories_use_filesystem_metadata_only": True,
        },
    }
    return {**core, "report_id": sha256_json(core)}


def write_create_only(*, root: Path = ROOT) -> dict[str, object]:
    report = build_report(root=root)
    output = root / OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as stream:
        stream.write(canonical_bytes(report) + b"\n")
    return report


def main() -> int:
    report = write_create_only(root=ROOT)
    print(
        json.dumps(
            {
                "report_id": report["report_id"],
                "sha256": sha256_file(ROOT / OUTPUT),
                "state": report["state"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
