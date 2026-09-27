"""Issue/independently verify the inactive aggregate; never starts a market worker."""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

from futures_rebuild.canonical import io_path, sha256_file, sha256_json, write_new_canonical_authority
from futures_rebuild.causal_market_binding_compatibility import (
    audit_campaign, verify_output_metadata, ORIGINAL_CERTIFICATE_IDS_SHA256,
)
from futures_rebuild.causal_observation_market_certification import certify_complete_market_certificate_set
from futures_rebuild.errors import IntegrityError

REPORT = Path("reports/gate_b_binding_compatibility_v1")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true", help="Independent saved-set verification; no replays")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    started = time.monotonic()
    baseline = json.loads((root / REPORT / "baseline_audit.json").read_bytes())
    manifest, certificates = audit_campaign(root, saved_snapshots=baseline["snapshots"])
    # Original artifacts and their exact bytes must also match the initial audit.
    if manifest["preserved_artifacts"] != baseline["preserved_artifacts"]:
        raise IntegrityError("original evidence changed since baseline audit")
    print(f"Audit passed: {len(certificates)} markets, {len(manifest['binding_groups'])} bindings", flush=True)
    if args.verify:
        saved = json.loads((root / REPORT / "compatibility_manifest.json").read_bytes())
        result = json.loads((root / REPORT / "result.json").read_bytes())
        certificate_path = root / result["complete_certificate_path"]
        complete = json.loads(certificate_path.read_bytes())
        if saved != manifest or complete["compatibility_manifest_id"] != manifest["manifest_id"]:
            raise IntegrityError("saved compatibility manifest differs")
        core = {k: v for k, v in complete.items() if k != "certificate_id"}
        ids = [c["certificate_id"] for c in certificates]
        if (complete["certificate_id"] != sha256_json(core)
                or sha256_file(certificate_path) != result["complete_certificate_sha256"]
                or complete["market_certificate_ids"] != ids
                or complete["market_certificate_ids_sha256"] != ORIGINAL_CERTIFICATE_IDS_SHA256
                or complete["market_count"] != 41
                or complete["checkpoint_set_id"] != manifest["checkpoint_set_id"]
                or complete["publication_authorized"] is not False
                or complete["activation_authorized"] is not False
                or complete["fresh_execution_authorized"] is not False):
            raise IntegrityError("successor certificate identity/authority differs")
        paths, total_bytes, observations, partitions = set(), 0, 0, 0
        # Independent traversal and hashlib implementation; do not reuse the issuer.
        for cert in certificates:
            evidence = cert["replay_evidence"]
            if (cert["certificate_id"] != sha256_json({k: v for k, v in cert.items() if k != "certificate_id"})
                    or cert["replay_passes"] != 2
                    or evidence["evidence_id"] != sha256_json({k: v for k, v in evidence.items() if k != "evidence_id"})
                    or cert["replay_evidence_id"] != evidence["evidence_id"]
                    or sha256_json(evidence["output_inventory"]) != evidence["output_inventory_sha256"]):
                raise IntegrityError("original certificate/replay evidence differs")
            for item in evidence["output_inventory"]:
                path = root / item["path"]
                if not path.resolve().is_relative_to(root) or item["path"] in paths:
                    raise IntegrityError("duplicate or escaped output path")
                with io_path(path).open("rb") as stream:
                    digest = hashlib.file_digest(stream, "sha256").hexdigest()
                if digest != item["sha256"] or io_path(path).stat().st_size != item["size"]:
                    raise IntegrityError("independent output hash/size differs")
                paths.add(item["path"])
                total_bytes += item["size"]
            observations += evidence["observation_count"]
            partitions += evidence["partition_count"]
            print(f"Independently verified {cert['market']}", flush=True)
        if (total_bytes != complete["total_output_bytes"] or observations != complete["total_observation_count"]
                or partitions != complete["total_partition_count"] or len(paths) != complete["complete_output_file_count"]
                or sha256_json(sorted(paths)) != complete["complete_output_paths_sha256"]):
            raise IntegrityError("independent complete-set totals differ")
        metadata = verify_output_metadata(root, certificates)
        if metadata != json.loads((root / REPORT / "output_metadata_validation.json").read_bytes()):
            raise IntegrityError("independent metadata verification differs")
        final, _ = audit_campaign(root, saved_snapshots=baseline["snapshots"])
        if final != manifest:
            raise IntegrityError("evidence or implementation changed during independent verification")
        verification = {"status": "PASS_INDEPENDENT_COMPLETE_SET_VERIFICATION", "certificate_id": complete["certificate_id"],
                        "market_count": 41, "file_count": len(paths), "output_bytes": total_bytes,
                        "observation_count": observations, "partition_count": partitions,
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                        "verifier_sha256": sha256_file(Path(__file__)),
                        "active_catalog_unchanged": True, "original_evidence_unchanged": True,
                        "market_rebuilds": 0, "additional_replays": 0}
        write_new_canonical_authority(root / REPORT / "independent_validation.json", verification)
        print(json.dumps(verification), flush=True)
        return

    metadata = verify_output_metadata(root, certificates)
    print("All output schemas, footer counts and checkpoint partitions passed; hashing complete outputs", flush=True)
    complete = certify_complete_market_certificate_set(
        repository_root=root, checkpoint_set=manifest["checkpoint_set"],
        market_certificates=certificates, compatibility_manifest=manifest,
    )
    final, _ = audit_campaign(root, saved_snapshots=baseline["snapshots"])
    if final != manifest:
        raise IntegrityError("evidence or implementation changed during complete-set validation")
    target = (Path("state/causal_observation_market_certificates") / manifest["checkpoint_set_id"]
              / "complete_set" / f"{complete['certificate_id']}.json")
    write_new_canonical_authority(root / REPORT / "compatibility_manifest.json", manifest)
    write_new_canonical_authority(root / REPORT / "output_metadata_validation.json", metadata)
    write_new_canonical_authority(root / target, complete)
    result = {"status": "PASS_SUCCESSOR_COMPLETE_SET_INACTIVE", "complete_certificate_path": target.as_posix(),
              "complete_certificate_id": complete["certificate_id"], "complete_certificate_sha256": sha256_file(root / target),
              "compatibility_manifest_id": manifest["manifest_id"], "original_terminal_preserved": True,
              "market_rebuilds": 0, "additional_replays": 0, "publication_authorized": False, "activation_authorized": False,
              "elapsed_seconds": round(time.monotonic() - started, 3), "runner_sha256": sha256_file(Path(__file__))}
    write_new_canonical_authority(root / REPORT / "result.json", result)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
