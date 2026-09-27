"""Verify and stage original policy metadata only; never install live authority."""
import json
import shutil
from pathlib import Path

from futures_rebuild.canonical import sha256_file, sha256_json, write_new_canonical_authority


def identity(document, key):
    assert document[key] == sha256_json({k: v for k, v in document.items() if k != key}), key


def main():
    root = Path(__file__).resolve().parents[1]
    preserved = root / "reports/custody_successor_v1/policy_recovery/original"
    publication = "reports/raw_layer_authority_cutover/rlac_20260814T0642492268888Z_0b571482"
    out = root / "reports/custody_successor_v1/policy_recovery"
    protected = ["configs/repository_surface.json", "data/active/catalog.json"]
    before = {p: sha256_file(root / p) for p in protected}

    def read(relative):
        sha256_file(preserved / relative)  # Reject link-like authority inputs.
        return json.loads((preserved / relative).read_bytes())

    v3_path = "configs/data_surface_registry_v3.json"
    v3 = read(v3_path)
    identity(v3, "registry_id")
    assert v3["registry_id"] == "613ffe158c89ee2f4faf59b0ffa636809d7bbe627b43cc9c37c4ec56d4ed7ac6"
    cert_path = publication + "/RAW_LAYER_RETIREMENT_CERTIFICATE.json"
    cert = read(cert_path)
    identity(cert, "retirement_certificate_id")
    assert cert["status"] == "PASS"
    assert cert["documentation_governance"]["current_contract_ids"]["data_surface_registry_id"] == v3["registry_id"]
    records = [x for x in cert["documentation_governance"]["changed_documents"] if x["path"] == v3_path]
    assert len(records) == 1
    record = records[0]
    assert sha256_file(preserved / v3_path) == record["after_sha256"]
    assert (preserved / v3_path).stat().st_size == record["after_bytes"] == 1527
    assert sha256_file(preserved / record["proposed_bytes_path"]) == record["after_sha256"]

    parent = v3["parent_registry"]
    v2 = read(parent["path"])
    identity(v2, "registry_id")
    assert v2["registry_id"] == parent["registry_id"]
    assert sha256_file(preserved / parent["path"]) == parent["sha256"]
    v1 = v2["parent_registry"]
    assert sha256_file(root / v1["path"]) == v1["sha256"]
    assert read(v1["path"])["registry_id"] == v1["registry_id"]
    direct = v3["active_authority"]
    registry = read(direct["direct_registry_path"])
    identity(registry, "registry_id")
    assert registry["registry_id"] == direct["direct_registry_id"]
    assert sha256_file(preserved / direct["direct_registry_path"]) == direct["direct_registry_sha256"]
    assert cert["catalog_and_registry_identities"]["direct_authority_registry_id"] == registry["registry_id"]

    paths = [v3_path, parent["path"], direct["direct_registry_path"], cert_path, record["proposed_bytes_path"]]
    recovered = []
    for relative in paths:
        source, target = preserved / relative, out / "original" / relative
        expected = sha256_file(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        if source != target and not target.exists():
            shutil.copyfile(source, target)
        assert sha256_file(target) == expected
        recovered.append({"path": relative, "size": source.stat().st_size, "sha256": expected})

    missing = [p for p in paths[:3] if not (root / p).exists()]
    assert before == {p: sha256_file(root / p) for p in protected}
    assert missing == paths[:3]
    proof = {"status": "PASS_EXACT_HISTORICAL_METADATA_RECOVERY_NOT_ACTIVATED",
             "source_root": str(preserved), "policy_id": v3["registry_id"],
             "publication_certificate_id": cert["retirement_certificate_id"],
             "recovered": recovered, "missing_live_metadata": missing,
             "unchanged_live_hashes": before, "payloads_read": False,
             "live_authority_installed": False, "storage_cutover_ready": False,
             "limitation": "Original metadata recovered; current full dependency closure and custody successor integration remain unverified."}
    proof["proof_id"] = sha256_json(proof)
    write_new_canonical_authority(out / "verification.json", proof)
    catalog = json.loads((root / "data/active/catalog.json").read_bytes())
    by_path = {entry["parquet_path"]: entry for entry in catalog["entries"] if entry.get("parquet_path")}
    comparisons = []
    for partition in registry["partitions"]:
        current = by_path.get(partition["accepted_causal_path"])
        comparisons.append({"market": partition["market"], "year": partition["year"],
                            "path": partition["accepted_causal_path"],
                            "current_catalog_entry_present": current is not None,
                            "recorded_hash_matches": current is not None and current["parquet_sha256"] == partition["accepted_causal_sha256"],
                            "historical_receipt_present": (root / partition["direct_equivalence_receipt_path"]).is_file()})
    compatibility = {"status": "HISTORICAL_POLICY_NOT_CURRENT_CATALOG_COMPATIBLE",
                     "recovery_proof_id": proof["proof_id"], "current_catalog_sha256": before["data/active/catalog.json"],
                     "historical_partitions": len(comparisons), "current_catalog_entries": len(catalog["entries"]),
                     "matching_recorded_hashes": sum(x["recorded_hash_matches"] for x in comparisons),
                     "missing_historical_receipts": sum(not x["historical_receipt_present"] for x in comparisons),
                     "comparisons": comparisons, "payloads_read": False,
                     "decision": "Do not reactivate old direct authority. Prepare a custody-only successor preserving current catalog and certificates; historical hash differences do not demonstrate damaged data."}
    assert compatibility["matching_recorded_hashes"] < len(comparisons)
    assert before == {p: sha256_file(root / p) for p in protected}
    compatibility["audit_id"] = sha256_json(compatibility)
    write_new_canonical_authority(out / "current_compatibility.json", compatibility)
    print(json.dumps({"recovery_proof_id": proof["proof_id"], "recovered_files": len(recovered),
                      **{k: v for k, v in compatibility.items() if k != "comparisons"}}, indent=2))


if __name__ == "__main__":
    main()
