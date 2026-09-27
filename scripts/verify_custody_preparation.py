"""Check inactive candidates against the actual active view; no publication API."""
import importlib
import json
import shutil
import sys
import time
from pathlib import Path
from types import ModuleType

from futures_rebuild.canonical import sha256_file, sha256_json, write_new_canonical_authority as write
from futures_rebuild.custody_successor import validate_policy, require, POINTER, NEW_ROOT


def verify(folder):
    root = Path(__file__).resolve().parents[1]
    out = (root / folder).resolve(strict=True)
    require(out.is_relative_to(root / "reports/custody_successor_v1"), "candidate outside task reports")
    started = time.monotonic()
    manifest = json.loads((out / "manifest.json").read_bytes())
    require(manifest["manifest_id"] == sha256_json({k: v for k, v in manifest.items() if k != "manifest_id"}), "manifest identity differs")
    validate_policy(root, manifest)
    policy = manifest["custody_policy"]
    require(sha256_file(root / "configs/repository_surface.json") == policy["repository_surface_sha256"], "repository classification drifted")
    require(sha256_file(root / "src/futures_rebuild/custody_successor.py") == manifest["validator_sha256"], "validator drifted")
    for relative, expected in manifest["implementation_successors"].items():
        require(sha256_file(root / relative) == expected["predecessor_sha256"], "live consumer changed")
        require(sha256_file(out / "predecessor" / relative) == expected["predecessor_sha256"], "rollback code changed")
        require(sha256_file(out / "candidate" / relative) == expected["successor_sha256"], "candidate changed")
    for relative, expected in manifest["preserved_certificate_bindings"].items():
        require(sha256_file(root / relative) == expected, "preserved certificate changed")
    namespace = "_custody_view_verification"
    package = ModuleType(namespace)
    package.__path__ = [str(out / "candidate/src/futures_rebuild"), str(root / "src/futures_rebuild")]
    sys.modules[namespace] = package
    try:
        view = importlib.import_module(namespace + ".active_data_view")
        catalog = view.verify_view(root / "data/active")
        # One complete existing verifier pass, not 609 repeated whole-view scans.
        annual = [x for x in catalog["entries"] if x["disposition"] == view.CERTIFICATION_STATE]
        require(len(annual) == 609 and len({x["market"] for x in annual}) == 41
                and len(catalog["entries"]) == 617, "active catalog scope differs")
        require(sum(x["row_count"] for x in annual) == 139892740, "certified catalog row total differs")
    finally:
        for name in list(sys.modules):
            if name == namespace or name.startswith(namespace + "."):
                del sys.modules[name]
    require(not (root / POINTER).exists() and not (root / NEW_ROOT).exists(), "unexpected custody activation")
    validate_policy(root, manifest)
    free = shutil.disk_usage(root).free
    require(free >= 64 * 2**30, "free-space floor failed")
    proof = {"status": "PASS_INACTIVE_POLICY_AND_CURRENT_VIEW_COMPATIBILITY",
             "manifest_id": manifest["manifest_id"], "policy_id": policy["policy_id"],
             "active_catalog_sha256": policy["catalog_sha256"], "catalog_entries": 617,
             "annual_payloads_and_sidecars_verified": 609, "markets": 41,
             "preserved_certificate_bindings": manifest["preserved_certificate_bindings"],
             "free_bytes_observed": free, "elapsed_seconds": round(time.monotonic() - started, 3),
             "verifier_sha256": sha256_file(Path(__file__)), "payload_decoding": False,
             "live_storage_or_authority_changes": 0, "market_rebuilds": 0, "deletions": 0,
             "post_cutover_pair_and_consumer_verification_still_required": True}
    proof["verification_id"] = sha256_json(proof)
    write(out / "preparation_checks" / proof["verification_id"] / "verification.json", proof)
    print(json.dumps({k: v for k, v in proof.items() if k != "preserved_certificate_bindings"}, indent=2))


if __name__ == "__main__":
    verify(sys.argv[1])
