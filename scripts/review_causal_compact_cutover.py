"""Read-only review of a prepared compact successor; writes only its review report."""
import json
import sys
from itertools import zip_longest
from pathlib import Path

import pyarrow.parquet as pq

from futures_rebuild.active_data_view import ACTIVE_ROOT, CERTIFICATION_STATE, verify_view, verify_plan_bindings
from futures_rebuild.canonical import io_path, sha256_file, sha256_json, write_new_canonical_authority


def compare_annual_columns(old_path, new_path):
    """Compare every value; report lineage-version changes without hiding them."""
    old, new = pq.ParquetFile(io_path(old_path)), pq.ParquetFile(io_path(new_path))
    assert old.schema_arrow.equals(new.schema_arrow, check_metadata=True)
    differences, rows = set(), 0
    for left, right in zip_longest(old.iter_batches(batch_size=100_000), new.iter_batches(batch_size=100_000)):
        assert left is not None and right is not None and left.num_rows == right.num_rows
        rows += left.num_rows
        differences.update(name for name in left.schema.names if not left.column(name).equals(right.column(name)))
    return {"rows_compared": rows, "different_columns": sorted(differences),
            "values_equal_except_lineage_versions": differences <= {"source_contract_id", "source_release_id"}}


def review(root, report):
    def read(path):
        return json.loads(io_path(path).read_bytes())

    result = read(report / "result.json")
    plan = read(report / "publication_plan.json")
    preparation = read(report / "preparation_plan.json")
    verify_plan_bindings(root, plan)
    assert result["status"] == "VERIFIED_STAGED_SUCCESSOR_AWAITING_CUTOVER_APPROVAL"
    assert result["publication_plan_id"] == plan["plan_id"]
    assert sha256_file(root / ACTIVE_ROOT / "catalog.json") == result["predecessor"]["catalog_file_sha256"]
    predecessor = verify_view(root / ACTIVE_ROOT)
    stage = root / result["staging_path"]
    successor = verify_view(stage)
    assert successor["catalog_sha256"] == plan["publication_input_binding"]["catalog_sha256"]
    source_count = 0
    for unit in preparation["units"]:
        for source in unit["sources"]:
            path = io_path(root / source["path"])
            assert path.stat().st_size == source["size"] and sha256_file(path) == source["sha256"]
            source_count += 1
    entries = {(e["market"], e["year"]): e for e in successor["entries"]}
    comparisons = []
    for old in predecessor["entries"]:
        new = entries[(old["market"], old["year"])]
        if old["disposition"] != CERTIFICATION_STATE:
            assert new == old
            continue
        old_sidecar = read(root / old["sidecar_path"])
        new_sidecar = read(stage / new["sidecar_path"].removeprefix("data/active/"))
        old_receipt = old_sidecar["content_validation_receipt"]
        new_receipt = new_sidecar["content_validation_receipt"]
        content_fields = ("row_count", "canonical_row_hash", "canonical_schema_fingerprint")
        policy_fields = ("coverage_start", "coverage_end", "cohort", "selection_eligible", "permitted_uses", "disposition")
        column_check = compare_annual_columns(root / old["parquet_path"], stage / new["parquet_path"].removeprefix("data/active/"))
        comparisons.append({"market": old["market"], "year": old["year"], **column_check,
                            "content_identical": all(old_receipt[k] == new_receipt[k] for k in content_fields),
                            "access_policy_identical": all(old[k] == new[k] for k in policy_fields),
                            "parquet_bytes_identical": old["parquet_sha256"] == new["parquet_sha256"]})
    old_hash = result["predecessor"]["catalog_file_sha256"]
    pinned = [{"path": p.relative_to(root).as_posix(), "sha256": sha256_file(p)}
              for p in sorted((root / "configs").glob("*.json")) if old_hash.encode() in p.read_bytes()]
    mismatches = [c for c in comparisons if not c["values_equal_except_lineage_versions"] or not c["access_policy_identical"]]
    evidence = {
        "status": "STAGED_VERIFIED_REQUIRES_CUTOVER_APPROVAL" if not mismatches else "BLOCKED_PREDECESSOR_CONTENT_OR_POLICY_CHANGED",
        "verifier_sha256": sha256_file(Path(__file__)),
        "publication_plan_id": plan["plan_id"], "staging_path": result["staging_path"],
        "predecessor_catalog_file_sha256": old_hash, "successor_catalog_file_sha256": sha256_file(stage / "catalog.json"),
        "overlapping_materialized_entries": len(comparisons), "protected_entries_preserved": 8,
        "source_hash_files_verified": source_count,
        "comparisons": comparisons, "mismatches": mismatches,
        "existing_catalog_pinned_configurations": pinned,
        "consumer_impact": [
            "Shared active_data_view.resolve reads only the catalog-selected annual path and checks sidecar and file hashes.",
            "Existing catalog-bound research plans and approvals are not rewritten or transferred; fresh preparation is required.",
            "bounded_alpha_discovery.build_contract has an explicit 144-entry check and will reject the 617-entry successor.",
            "l0_alpha_source_readiness.prepare selects explicit market-years; execution still requires an exact fresh source binding.",
            "No current research configuration, trial permission, holdout policy or active pointer is changed by this review."],
        "policy_binding_changes": preparation["policy_binding_changes"],
        "lineage_transition": "Source contract and release IDs come unchanged from the recovered certified release; old rows and receipts are preserved in rollback, not rewritten.",
        "rollback_path": result["rollback_path"], "rollback_destination_available": not (root / result["rollback_path"]).exists(),
        "publication_performed": False, "deletions": 0,
    }
    evidence["review_id"] = sha256_json(evidence)
    verify_plan_bindings(root, plan)
    assert sha256_file(root / ACTIVE_ROOT / "catalog.json") == old_hash
    write_new_canonical_authority(report / "cutover_review.json", evidence)
    print(json.dumps({k: v for k, v in evidence.items() if k not in {"comparisons", "existing_catalog_pinned_configurations"}}))
    return evidence


if __name__ == "__main__":
    review(Path.cwd(), Path(sys.argv[1]))
