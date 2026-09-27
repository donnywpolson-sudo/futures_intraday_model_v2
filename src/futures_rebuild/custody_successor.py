"""Exact, opt-in custody relocation and hard-link validation. No mutation API."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath

from .errors import IntegrityError

POINTER = Path("configs/active_custody_successor.json")
OLD_ROOT = Path("data/vault/source_snapshots")
PREVIOUS_ROOT = Path("data/source_snapshots")
NEW_ROOT = Path("data/dbn/_source_snapshots")
POLICY_LIMITS = {"research_access": False, "catalog_replacement": False,
                 "historical_authority_activation": False, "deletion": False,
                 "payload_rewrite": False}


def require(condition, message):
    if not condition:
        raise IntegrityError(message)


def digest(path):
    from .canonical import io_path, assert_no_linklike_ancestors
    assert_no_linklike_ancestors(path)
    h = hashlib.sha256()
    with io_path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read(path):
    from .canonical import io_path, assert_plain_file
    # Authority documents must never receive a payload hard-link exception.
    require(io_path(path).stat().st_nlink == 1, "linked custody authority document")
    assert_plain_file(path)
    return json.loads(io_path(path).read_bytes())


def checked_path(root, relative):
    from .canonical import assert_no_linklike_ancestors
    pure = PurePosixPath(relative)
    require(not pure.is_absolute() and ".." not in pure.parts and "\\" not in relative and ":" not in relative,
            "custody path escapes repository")
    path = root / relative
    assert_no_linklike_ancestors(path)
    require(path.resolve().is_relative_to(root.resolve()), "custody path escapes repository")
    return path


def load_active(root):
    from .canonical import io_path, sha256_json
    pointer_path = root / POINTER
    if not io_path(pointer_path).exists():
        return None
    pointer = read(pointer_path)
    require(pointer.get("state") == "ACTIVE" and pointer.get("pointer_id") == sha256_json(
        {k: v for k, v in pointer.items() if k != "pointer_id"}), "invalid custody activation pointer")
    manifest_path = checked_path(root, pointer["manifest_path"])
    require(pointer["manifest_path"] == f"state/custody_successor_registry/{pointer['manifest_id']}/manifest.json",
            "custody manifest location differs")
    require(digest(manifest_path) == pointer["manifest_sha256"], "custody manifest bytes differ")
    manifest = read(manifest_path)
    require(manifest.get("schema_version") == "custody_successor/1" and manifest.get("manifest_id") == pointer["manifest_id"]
            and manifest["manifest_id"] == sha256_json({k: v for k, v in manifest.items() if k != "manifest_id"}),
            "custody manifest identity differs")
    require(digest(Path(__file__)) == manifest["validator_sha256"], "custody validator implementation changed")
    validate_policy(root, manifest)
    receipt_path = checked_path(root, pointer["verification_path"])
    require(receipt_path.parent == manifest_path.parent and receipt_path.name == "verification.json",
            "custody verification location differs")
    require(digest(receipt_path) == pointer["verification_sha256"], "custody verification changed")
    receipt = read(receipt_path)
    require(receipt.get("status") == "PASS_REPLACEMENT_CUSTODY" and receipt.get("manifest_id") == manifest["manifest_id"]
            and receipt.get("receipt_id") == sha256_json({k: v for k, v in receipt.items() if k != "receipt_id"}),
            "custody replacement is not verified")
    return manifest


def validate_policy(root, manifest):
    """A custody-only overlay, not a replacement for research/data selection."""
    from .canonical import sha256_json
    policy = manifest.get("custody_policy", {})
    require(policy.get("policy_id") == sha256_json({k: v for k, v in policy.items() if k != "policy_id"})
            and policy.get("scope") == "EXACT_CUSTODY_LOCATIONS_ONLY"
            and policy.get("limits") == POLICY_LIMITS, "custody-only policy differs")
    require(policy.get("snapshot_roots") == [s["new_path"] for s in manifest["snapshots"]]
            and policy.get("canonical_aliases") == [p["canonical_path"] for p in manifest["pairs"]],
            "custody policy scope differs from exact inventory")
    require(policy.get("catalog_path") == 'data/causally_gated_normalized/_control/catalog.json'
            and policy.get("catalog_sha256") == manifest.get("normalized_active_catalog_sha256")
            and digest(root / 'data/causally_gated_normalized/_control/catalog.json') == policy["catalog_sha256"],
            "current catalog differs; custody policy cannot activate an old release")
    require(policy.get("historical_policy_role") == "EVIDENCE_ONLY_NOT_INHERITED_AUTHORITY",
            "historical policy cannot grant successor authority")


def resolve_snapshot(root, historical_path):
    """Translate only declared historical snapshot roots, never arbitrary paths."""
    old = Path(historical_path)
    if old.is_absolute():
        old = old.relative_to(root)
    checked_path(root, old.as_posix())
    manifest = load_active(root)
    if manifest is None:
        require(not old.is_relative_to(NEW_ROOT) and not old.is_relative_to(PREVIOUS_ROOT),
                "snapshot relocation is not activated")
        return root / old
    matches = [s for s in manifest["snapshots"] if old.as_posix() in (s["old_path"], s["new_path"], (PREVIOUS_ROOT / s["snapshot_id"]).as_posix())]
    if not matches:
        require(not old.is_relative_to(NEW_ROOT) and not old.is_relative_to(OLD_ROOT) and not old.is_relative_to(PREVIOUS_ROOT), "unregistered successor snapshot")
        return root / old
    require(len(matches) == 1, "ambiguous custody snapshot mapping")
    path = checked_path(root, matches[0]["new_path"])
    require(path.is_dir(), "activated custody snapshot is absent; no vault fallback")
    return path


def binding_matches(root, path, recorded_hash):
    """Accept an exact predecessor code binding only under its activated successor."""
    actual = digest(path)
    if actual == recorded_hash:
        return True
    manifest = load_active(root)
    if manifest is None:
        return False
    relative = path.resolve().relative_to(root.resolve()).as_posix()
    expected = manifest["implementation_successors"].get(relative)
    return expected == {"predecessor_sha256": recorded_hash, "successor_sha256": actual}


def registered_hardlink(path):
    """Only the registered original+canonical pair may bypass single-link policy."""
    from .canonical import io_path
    absolute = Path(str(path).removeprefix("\\\\?\\")).absolute()
    roots = [p.parent for p in absolute.parents if p.name == "data"]
    if len(roots) != 1:
        return False
    root = roots[0]
    relative = absolute.relative_to(root).as_posix()
    if not (relative.startswith("data/dbn/tbbo/") or relative.startswith("data/dbn/_source_snapshots/")):
        return False
    manifest = load_active(root)
    if manifest is None:
        return False
    pairs = [p for p in manifest["pairs"] if relative in (p["original_path"], p["canonical_path"])]
    require(len(pairs) == 1, "hard link is not one exact registered custody pair")
    validate_pair(root, pairs[0])
    return True


def validate_pair(root, pair):
    """Physical verification before activation; grants no global read exception."""
    from .canonical import io_path
    require(pair["canonical_path"].startswith("data/dbn/tbbo/")
            and pair["original_path"].startswith("data/dbn/_source_snapshots/")
            and pair["canonical_path"].endswith(".dbn.zst") and pair["original_path"].endswith(".dbn.zst"),
            "custody hard-link scope differs")
    original, canonical = (checked_path(root, pair[k]) for k in ("original_path", "canonical_path"))
    left, right = io_path(original).stat(), io_path(canonical).stat()
    require(left.st_nlink == right.st_nlink == 2 and left.st_ino == right.st_ino and left.st_dev == right.st_dev
            and str(left.st_ino) == pair["original_file_id"] and left.st_dev == pair["volume_id"]
            and left.st_size == right.st_size == pair["size"], "registered custody pair identity/link count differs")
    require(digest(original) == pair["sha256"], "registered custody payload changed")
    return True


def validate_source_partition(root, partition_contract):
    """Account for exact custody files before excluding them from source selection.

    Metadata only: this is not payload certification or research admission.
    """
    from .canonical import io_path, iter_plain_files
    manifest = load_active(root)
    require(manifest is not None and manifest.get("pairs") == [],
            "two-folder custody partition requires a verified single-link successor")
    pointer = read(root / POINTER)
    require(partition_contract == {
        "root": NEW_ROOT.as_posix(), "selection_eligible": False,
        "manifest_path": pointer["manifest_path"], "manifest_sha256": pointer["manifest_sha256"],
        "file_count": len(manifest["original_inventory"]),
        "total_bytes": sum(item["size"] for item in manifest["original_inventory"])},
        "source contract does not bind the exact excluded custody partition")
    expected = {}
    snapshot_ids = [s["snapshot_id"] for s in manifest["snapshots"]]
    require(len(set(snapshot_ids)) == len(snapshot_ids) and all(
        len(s) == 64 and all(c in "0123456789abcdef" for c in s) for s in snapshot_ids),
        "custody snapshot identities differ")
    for item in manifest["original_inventory"]:
        historical = PurePosixPath(item["path"])
        matches = [s for s in manifest["snapshots"]
                   if historical.is_relative_to(PurePosixPath(s["old_path"]))]
        require(len(matches) == 1, "custody inventory snapshot is not exact")
        snapshot = matches[0]
        require(snapshot["new_path"] == (NEW_ROOT / snapshot["snapshot_id"]).as_posix(),
                "custody partition destination differs")
        relative = historical.relative_to(snapshot["old_path"])
        target = checked_path(root, (PurePosixPath(snapshot["new_path"]) / relative).as_posix())
        key = target.relative_to(root).as_posix()
        require(key not in expected, "duplicate custody inventory path")
        expected[key] = item["size"]
    partition = checked_path(root, NEW_ROOT.as_posix())
    require({p.name for p in io_path(partition).iterdir()} == set(snapshot_ids),
            "custody partition contains unregistered snapshot roots")
    actual = {p.relative_to(root).as_posix(): io_path(p).stat().st_size
              for p in iter_plain_files(partition)}
    require(actual == expected, "custody partition contains missing, extra or resized files")
    return partition
