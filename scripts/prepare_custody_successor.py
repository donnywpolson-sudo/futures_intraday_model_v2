"""Build inactive consumer candidates and an exact storage-cutover plan only."""
import ast
import difflib
import hashlib
import json
import shutil
import time
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, io_path, sha256_file, sha256_json, write_new_canonical_authority as write
from futures_rebuild.custody_successor import OLD_ROOT, NEW_ROOT, POLICY_LIMITS, require, validate_policy

ROOT = Path(__file__).resolve().parents[1]
BASE = Path("reports/causal_compact_successor_v1/e564263f41f2aee7cccdfe5e8b101076e77174348c2f2dac428532d51c6f0e02/gate_c_preparation/3344ad38a152a7a3f375313e5afbc41a49107ef4b8b92a106af11b2f3421cef9/manifest.json")
OUTPUT = Path("reports/custody_successor_v1")


def preserved_certificates(root):
    """Verify existing certificate identities without replaying or decoding data."""
    result_path = Path("reports/gate_b_binding_compatibility_v1/result.json")
    result = json.loads((root / result_path).read_bytes())
    complete_path = Path(result["complete_certificate_path"])
    require(sha256_file(root / complete_path) == result["complete_certificate_sha256"], "complete certificate bytes differ")
    complete = json.loads((root / complete_path).read_bytes())
    require(complete["certificate_id"] == sha256_json({k: v for k, v in complete.items() if k != "certificate_id"})
            and complete["market_count"] == 41 and complete["market_rebuilds"] == 0, "complete certificate differs")
    require(len(complete["market_order"]) == len(complete["market_certificate_ids"]) == 41, "certificate count differs")
    bindings = {result_path.as_posix(): sha256_file(root / result_path), complete_path.as_posix(): result["complete_certificate_sha256"]}
    for market, expected in zip(complete["market_order"], complete["market_certificate_ids"]):
        matches = []
        for path in (root / complete_path.parent.parent / market).glob("*.json"):
            if path.name.endswith(".failure.json"):
                continue
            certificate = json.loads(path.read_bytes())
            if certificate.get("certificate_id") == expected:
                require(certificate["market"] == market and expected == sha256_json({k: v for k, v in certificate.items() if k != "certificate_id"}),
                        "market certificate identity differs")
                matches.append(path)
        require(len(matches) == 1, f"market certificate absent or ambiguous: {market}")
        bindings[matches[0].relative_to(root).as_posix()] = sha256_file(matches[0])
    return bindings


def candidates(root):
    """Exact mechanical edits, with original bytes saved for installation rollback."""
    names = ["canonical", "boundary", "tbbo_16_market_acquisition", "tbbo_custody_publication",
             "tbbo_quality_certification", "tbbo_catalog_admission"]
    result = {}
    for name in names:
        relative = f"src/futures_rebuild/{name}.py"
        original = (root / relative).read_text(encoding="utf-8")
        text = original
        if name not in {"canonical", "boundary"}:
            text = text.replace("from __future__ import annotations\n", "from __future__ import annotations\n\nfrom .custody_successor import resolve_snapshot, binding_matches\n", 1)
        def edit(old, new, expected):
            nonlocal text
            require(text.count(old) == expected, f"candidate source shape changed: {relative}: {old}")
            text = text.replace(old, new)
        if name == "canonical":
            edit('        raise ContractError(f"hard-linked files are forbidden: {path}")',
                 '        from .custody_successor import registered_hardlink\n        if not registered_hardlink(path):\n            raise ContractError(f"hard-linked files are forbidden: {path}")', 1)
        elif name == "boundary":
            edit('        candidate = self.assert_active_path(\n            path, purpose="source snapshot", subtree="data/vault/source_snapshots"\n        )',
                 '        from .custody_successor import resolve_snapshot, NEW_ROOT, OLD_ROOT\n        path = resolve_snapshot(self.active_root, path)\n        subtree = NEW_ROOT if path.is_relative_to(self.active_root / NEW_ROOT) else OLD_ROOT\n        candidate = self.assert_active_path(\n            path, purpose="source snapshot", subtree=subtree.as_posix()\n        )', 1)
            edit('(self.active_root / "data" / "vault" / "source_snapshots").resolve(', '(self.active_root / subtree).resolve(', 1)
        else:
            edit('plan.get("implementation_sha256") != sha256_file(Path(__file__))',
                 'not binding_matches(root, Path(__file__), plan.get("implementation_sha256"))',
                 1 if name in {"tbbo_16_market_acquisition", "tbbo_quality_certification"} else 0)
            if name == "tbbo_16_market_acquisition":
                edit('                    raise IntegrityError(f"tree contains a hard link: {child}")',
                     '                    from .custody_successor import registered_hardlink\n                    if not registered_hardlink(child):\n                        raise IntegrityError(f"tree contains a hard link: {child}")', 1)
            if name == "tbbo_custody_publication":
                edit('root / str(preparation["vault_snapshot_path"])', 'resolve_snapshot(root, str(preparation["vault_snapshot_path"]))', 2)
                edit('publication.get("publication_implementation_sha256") != sha256_file(Path(__file__))',
                     'not binding_matches(root, Path(__file__), publication.get("publication_implementation_sha256"))', 1)
            if name == "tbbo_quality_certification":
                edit('root / str(registry["vault_snapshot_path"])', 'resolve_snapshot(root, str(registry["vault_snapshot_path"]))', 2)
                edit('root / VAULT_ROOT / TBBO_SNAPSHOT_ID', 'resolve_snapshot(root, VAULT_ROOT / TBBO_SNAPSHOT_ID)', 1)
                edit('snapshot = root / VAULT_ROOT / snapshot_id', 'snapshot = resolve_snapshot(root, VAULT_ROOT / snapshot_id)', 1)
                edit('root / vault / str(item["dbn"]["path"])', 'resolve_snapshot(root, vault) / str(item["dbn"]["path"])', 1)
            if name == "tbbo_catalog_admission":
                edit('root / str(custody["vault_snapshot_path"])', 'resolve_snapshot(root, str(custody["vault_snapshot_path"]))', 2)
                edit('implementation.get("sha256") != sha256_file(Path(__file__))',
                     'not binding_matches(root, Path(__file__), implementation.get("sha256"))', 1)
        ast.parse(text)
        require(text != original, f"candidate edit was empty: {relative}")
        result[relative] = text.encode("utf-8")
    return result


def prepare(root=ROOT):
    started = time.monotonic()
    baseline = json.loads((root / BASE).read_bytes())
    require(baseline["manifest_id"] == sha256_json({k: v for k, v in baseline.items() if k != "manifest_id"}), "baseline identity differs")
    require(not (root / "configs/active_custody_successor.json").exists() and not (root / NEW_ROOT).exists(), "custody state already changed")
    require(sha256_file(root / "data/active/catalog.json") == baseline["active_catalog_sha256"], "normalized active catalog changed")
    records = baseline["snapshot_files"]
    for item in records + [p["canonical"] for p in baseline["counterpart_comparisons"]]:
        path = io_path(root / item["path"])
        stat = path.stat()
        require((stat.st_size, str(stat.st_ino), stat.st_dev, stat.st_nlink) ==
                (item["size"], item["file_id"], item["volume_id"], item["link_count"])
                and sha256_file(path) == item["sha256"], f"custody input changed: {item['path']}")
    ids = sorted({Path(item["path"]).relative_to(OLD_ROOT).parts[0] for item in records})
    require(len(ids) == 2 and len(records) == 448, "snapshot scope differs")
    snapshots = [{"snapshot_id": sid, "old_path": (OLD_ROOT / sid).as_posix(), "new_path": (NEW_ROOT / sid).as_posix()} for sid in ids]
    pairs = []
    for item in baseline["counterpart_comparisons"]:
        if not item["same_bytes"] or item["same_file_identity"]:
            continue
        original, canonical = item["snapshot"], item["canonical"]
        require(original["path"].endswith(".dbn.zst") and "/tbbo/" in original["path"]
                and original["link_count"] == canonical["link_count"] == 1, "duplicate scope differs")
        pairs.append({"original_path": original["path"].replace(OLD_ROOT.as_posix(), NEW_ROOT.as_posix(), 1),
                      "canonical_path": canonical["path"], "original_file_id": original["file_id"],
                      "displaced_file_id": canonical["file_id"], "volume_id": original["volume_id"],
                      "size": original["size"], "sha256": original["sha256"],
                      "displaced_allocated_bytes": canonical["allocated_bytes"]})
    require(len(pairs) == 192, "expected exactly 192 duplicate DBNs")
    generated = candidates(root)
    successors = {path: {"predecessor_sha256": sha256_file(root / path), "successor_sha256": hashlib.sha256(data).hexdigest()}
                  for path, data in generated.items()}
    recovery_path = OUTPUT / "policy_recovery/verification.json"
    recovery = json.loads((root / recovery_path).read_bytes())
    require(recovery["proof_id"] == sha256_json({k: v for k, v in recovery.items() if k != "proof_id"})
            and recovery["status"] == "PASS_EXACT_HISTORICAL_METADATA_RECOVERY_NOT_ACTIVATED", "policy recovery differs")
    for item in recovery["recovered"]:
        require(sha256_file(root / OUTPUT / "policy_recovery/original" / item["path"]) == item["sha256"],
                "recovered historical evidence differs")
    from futures_rebuild.repository_surface import resolve_surface_entry
    surface = json.loads((root / "configs/repository_surface.json").read_bytes())
    for snapshot in snapshots:
        entry = resolve_surface_entry(surface, snapshot["new_path"])
        require(entry is not None and entry["deletion_policy"] in {"PRESERVE", "NO_AUTOMATIC_DELETE"},
                "successor snapshot root lacks existing preservation classification")
    policy = {"scope": "EXACT_CUSTODY_LOCATIONS_ONLY", "limits": POLICY_LIMITS,
              "snapshot_roots": [s["new_path"] for s in snapshots],
              "canonical_aliases": [p["canonical_path"] for p in pairs],
              "catalog_path": "data/active/catalog.json", "catalog_sha256": baseline["active_catalog_sha256"],
              "historical_policy_role": "EVIDENCE_ONLY_NOT_INHERITED_AUTHORITY",
              "historical_recovery_path": recovery_path.as_posix(),
              "historical_recovery_sha256": sha256_file(root / recovery_path),
              "repository_surface_sha256": sha256_file(root / "configs/repository_surface.json")}
    policy["policy_id"] = sha256_json(policy)
    core = {"schema_version": "custody_successor/1", "snapshots": snapshots, "pairs": pairs,
            "custody_policy": policy,
            "preserved_certificate_bindings": preserved_certificates(root),
            "original_inventory": records, "implementation_successors": successors,
            "validator_sha256": sha256_file(root / "src/futures_rebuild/custody_successor.py"),
            "normalized_active_catalog_sha256": baseline["active_catalog_sha256"],
            "baseline_manifest_sha256": sha256_file(root / BASE), "historical_evidence_rewrite": False}
    manifest = {**core, "manifest_id": sha256_json(core)}
    validate_policy(root, manifest)
    out = root / OUTPUT / manifest["manifest_id"]
    write(out / "manifest.json", manifest)
    patch = []
    for path, data in generated.items():
        target = out / "candidate" / path
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            require(target.read_bytes() == data, "candidate artifact changed")
        else:
            # Mechanical, exact-count successor rendering; never edits live source.
            target.write_bytes(data)
        backup = out / "predecessor" / path
        backup.parent.mkdir(parents=True, exist_ok=True)
        if not backup.exists():
            shutil.copyfile(root / path, backup)
        require(sha256_file(backup) == successors[path]["predecessor_sha256"], "predecessor source backup differs")
        patch.extend(difflib.unified_diff((root/path).read_text().splitlines(True), data.decode().splitlines(True), fromfile=path, tofile=path))
    patch_path = out / "consumer_changes.patch"
    patch_data = "".join(patch).encode()
    if patch_path.exists():
        require(patch_path.read_bytes() == patch_data, "candidate diff changed")
    else:
        patch_path.write_bytes(patch_data)
    plan = {"status": "PREPARED_NOT_ACTIVATED_TESTS_REQUIRED", "manifest_id": manifest["manifest_id"],
            "snapshot_moves": snapshots, "canonical_replacements": pairs,
            "displaced_root": f"state/custody_successor_rollback/{manifest['manifest_id']}/canonical",
            "minimum_free_bytes": 64 * 2**30, "maximum_added_payload_bytes": 0,
            "maximum_added_metadata_bytes": 64 * 2**20,
            "maximum_pre_mutation_seconds": 900, "copy_payloads": False,
            "activation_approved": False, "deletion_approved": False,
            "potential_reclaimable_bytes": sum(p["displaced_allocated_bytes"] for p in pairs),
            "next_boundary": "EXACT_STORAGE_CUTOVER_APPROVAL_AFTER_CANDIDATE_TESTS",
            "preparer_sha256": sha256_file(Path(__file__))}
    write(out / "cutover_plan.json", plan)
    require(sha256_file(root / "data/active/catalog.json") == baseline["active_catalog_sha256"], "active data changed during preparation")
    print(json.dumps({"output": out.relative_to(root).as_posix(), "manifest_id": manifest["manifest_id"],
                      "elapsed_seconds": round(time.monotonic()-started,3), "live_mutations": 0, "potential_bytes": plan["potential_reclaimable_bytes"]}))
    return out


if __name__ == "__main__":
    prepare()
