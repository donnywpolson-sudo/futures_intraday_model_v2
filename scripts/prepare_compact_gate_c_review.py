"""Read-only custody/duplicate census; never grants or performs deletion."""
import ctypes
import json
import subprocess
from collections import Counter
from ctypes import wintypes
from pathlib import Path

from futures_rebuild.canonical import io_path, iter_plain_files, sha256_file, sha256_json, write_new_canonical_authority

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports/causal_compact_successor_v1/e564263f41f2aee7cccdfe5e8b101076e77174348c2f2dac428532d51c6f0e02"


def allocation(path):
    """NTFS allocation, not logical size; file identity/link count come from Path.stat."""
    class StandardInfo(ctypes.Structure):
        _fields_ = [("allocation", ctypes.c_longlong), ("end", ctypes.c_longlong),
                    ("links", wintypes.DWORD), ("pending", ctypes.c_ubyte), ("directory", ctypes.c_ubyte)]
    api = ctypes.WinDLL("kernel32", use_last_error=True)
    api.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
                               wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    api.CreateFileW.restype = wintypes.HANDLE
    api.GetFileInformationByHandleEx.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    api.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = api.CreateFileW(str(io_path(path)), 0x80, 7, None, 3, 0, None)
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        info = StandardInfo()
        if not api.GetFileInformationByHandleEx(handle, 1, ctypes.byref(info), ctypes.sizeof(info)):
            raise ctypes.WinError(ctypes.get_last_error())
        assert not info.directory and not info.pending
        return info.allocation
    finally:
        api.CloseHandle(handle)


def file_record(path):
    stat = io_path(path).stat()
    record = {"path": path.relative_to(ROOT).as_posix(), "absolute_path": str(path.resolve()),
              "size": stat.st_size, "sha256": sha256_file(io_path(path)),
              "volume_id": stat.st_dev, "file_id": str(stat.st_ino), "link_count": stat.st_nlink,
              "allocated_bytes": allocation(path)}
    after = io_path(path).stat()
    assert (stat.st_size, stat.st_mtime_ns, stat.st_ino, stat.st_nlink) == (after.st_size, after.st_mtime_ns, after.st_ino, after.st_nlink)
    return record


def prepare():
    publication = json.loads((REPORT / "approved_cutover/publication_receipt.json").read_bytes())
    assert publication["state"] == "PUBLISHED_VERIFIED"
    active = ROOT / "data/active/catalog.json"
    active_hash = sha256_file(active)
    assert active_hash == "900c9223214d2a884ca6d2f1d902440b71563fcd5200ce09e4ae7f948b41c4fe"
    schemas = {}
    for path in iter_plain_files(ROOT / "data/dbn", reject_hardlinks=False):
        schema = path.relative_to(ROOT / "data/dbn").parts[0]
        stat = io_path(path).stat()
        item = schemas.setdefault(schema, {"files": 0, "logical_bytes": 0, "multi_link_files": 0})
        item["files"] += 1
        item["logical_bytes"] += stat.st_size
        item["multi_link_files"] += stat.st_nlink > 1
    vault = ROOT / "data/vault/source_snapshots"
    records, comparisons = [], []
    for path in iter_plain_files(vault, reject_hardlinks=False):
        if path.name == ".gitkeep":
            continue
        record = file_record(path)
        records.append(record)
        relative = path.relative_to(vault)
        counterpart = ROOT / "data" / Path(*relative.parts[1:])
        if io_path(counterpart).is_file():
            canonical = file_record(counterpart)
            comparisons.append({"snapshot": record, "canonical": canonical,
                                "same_bytes": record["size"] == canonical["size"] and record["sha256"] == canonical["sha256"],
                                "same_file_identity": (record["volume_id"], record["file_id"]) == (canonical["volume_id"], canonical["file_id"])})
    duplicate = [p for p in comparisons if p["same_bytes"] and not p["same_file_identity"]]
    # Count at most one unlink of a distinct, singly-linked copy in each pair.
    potential = { (p["snapshot"]["volume_id"], p["snapshot"]["file_id"]): p["snapshot"]["allocated_bytes"]
                  for p in duplicate if p["snapshot"]["link_count"] == 1 }
    refs = subprocess.run(["rg", "-l", "-F", "data/vault", "src/futures_rebuild", "configs"],
                          cwd=ROOT, text=True, capture_output=True)
    assert refs.returncode in (0, 1)
    evidence = {
        "status": "BLOCKED_CUSTODY_SUCCESSOR_REQUIRED_BEFORE_DELETION", "active_catalog_sha256": active_hash,
        "publication_receipt_id": publication["publication_receipt_id"], "canonical_schema_inventory": schemas,
        "snapshot_files": records, "counterpart_comparisons": comparisons,
        "snapshot_file_count": len(records), "snapshot_logical_bytes": sum(p["size"] for p in records),
        "byte_identical_distinct_pairs": len(duplicate), "potential_duplicate_allocation_bytes": sum(potential.values()),
        "potential_is_not_deletion_authority": True,
        "canary_exists": (ROOT / "data/causally_gated_normalized/v10/_canary").exists(),
        "successor_source_snapshots_root_exists": (ROOT / "data/source_snapshots").exists(),
        "vault_code_and_configuration_references": sorted(refs.stdout.splitlines()),
        "protected": ["data/active", "data/causally_gated_normalized/v10", "data/dbn", "data/vault",
                      publication["rollback_path"], "All source/market/complete-set certificates and original receipts",
                      "All diagnostic .parent DBN/manifest pairs", "The definition/status custody supplement"],
        "deletion_manifest": {"entries": [], "file_count": 0, "reclaimable_bytes_now": 0,
                              "execution_enabled": False, "owner_deletion_approval": False},
        "blockers": ["Immutable snapshots and canonical copies still have custody/certificate roles.",
                     "No verified successor custody mapping to data/source_snapshots exists.",
                     "No writer-exclusion proof for deletion has been established; it must be fresh at execution.",
                     "Do not retire active rollback, V10 evidence, diagnostic parent pairs, or the small L1 supplement."],
        "payload_decoding": False, "publication_or_custody_mutations": 0, "deletions": 0,
        "preparer_sha256": sha256_file(Path(__file__)),
    }
    assert not evidence["canary_exists"]
    assert sha256_file(active) == active_hash
    evidence["manifest_id"] = sha256_json(evidence)
    out = REPORT / "gate_c_preparation" / evidence["manifest_id"]
    write_new_canonical_authority(out / "manifest.json", evidence)
    print(json.dumps({k: v for k, v in evidence.items() if k not in {"snapshot_files", "counterpart_comparisons"}}))


if __name__ == "__main__":
    prepare()
