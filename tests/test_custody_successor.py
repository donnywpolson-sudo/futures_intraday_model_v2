"""Synthetic custody routing, exact-link exceptions, and reversible rename rehearsal."""
import importlib.util
import importlib
import hashlib
import os
import runpy
import shutil
import sys
from types import ModuleType
from pathlib import Path

import pytest

from futures_rebuild import custody_successor as custody
from futures_rebuild.canonical import sha256_file, sha256_json, write_new_canonical_authority as write
from futures_rebuild.errors import ContractError, IntegrityError

pytestmark = pytest.mark.high_risk
ROOT = Path(__file__).resolve().parents[1]


def prepared_candidates():
    active = custody.load_active(ROOT)
    if active is None:
        return runpy.run_path(str(ROOT / "scripts/prepare_custody_successor.py"))["candidates"](ROOT), None
    folder = ROOT / "reports/custody_successor_v1" / active["manifest_id"] / "candidate"
    files = {p: (folder / p).read_bytes() for p in active["implementation_successors"]}
    assert all(hashlib.sha256(data).hexdigest() == active["implementation_successors"][p]["successor_sha256"]
               for p, data in files.items())
    return files, active["implementation_successors"]


def fixture(root, successors=None):
    old = root / custody.OLD_ROOT / ("a" * 64)
    new = root / custody.NEW_ROOT / ("a" * 64)
    original = new / "dbn/tbbo/ES/2024/2024-01-01_2024-02-01.dbn.zst"
    canonical = root / "data/dbn/tbbo/ES/2024/2024-01-01_2024-02-01.dbn.zst"
    old.mkdir(parents=True)
    original.parent.mkdir(parents=True)
    canonical.parent.mkdir(parents=True)
    original.write_bytes(b"synthetic custody bytes")
    expected = sha256_file(original)
    os.link(original, canonical)
    stat = original.stat()
    pair = {"original_path": original.relative_to(root).as_posix(), "canonical_path": canonical.relative_to(root).as_posix(),
            "original_file_id": str(stat.st_ino), "volume_id": stat.st_dev, "size": stat.st_size, "sha256": expected}
    core = {"schema_version": "custody_successor/1", "snapshots": [{"old_path": old.relative_to(root).as_posix(), "new_path": new.relative_to(root).as_posix()}],
            "pairs": [pair], "implementation_successors": successors or {}, "validator_sha256": sha256_file(Path(custody.__file__))}
    write(root / "data/active/catalog.json", {"synthetic": True})
    core["normalized_active_catalog_sha256"] = sha256_file(root / "data/active/catalog.json")
    policy = {"scope": "EXACT_CUSTODY_LOCATIONS_ONLY", "limits": custody.POLICY_LIMITS,
              "snapshot_roots": [new.relative_to(root).as_posix()], "canonical_aliases": [pair["canonical_path"]],
              "catalog_path": "data/active/catalog.json", "catalog_sha256": core["normalized_active_catalog_sha256"],
              "historical_policy_role": "EVIDENCE_ONLY_NOT_INHERITED_AUTHORITY"}
    policy["policy_id"] = sha256_json(policy)
    core["custody_policy"] = policy
    manifest = {**core, "manifest_id": sha256_json(core)}
    folder = root / "state/custody_successor_registry" / manifest["manifest_id"]
    write(folder / "manifest.json", manifest)
    receipt = {"status": "PASS_REPLACEMENT_CUSTODY", "manifest_id": manifest["manifest_id"]}
    receipt["receipt_id"] = sha256_json(receipt)
    write(folder / "verification.json", receipt)
    pointer = {"state": "ACTIVE", "manifest_id": manifest["manifest_id"],
               "manifest_path": (folder / "manifest.json").relative_to(root).as_posix(),
               "manifest_sha256": sha256_file(folder / "manifest.json"),
               "verification_path": (folder / "verification.json").relative_to(root).as_posix(),
               "verification_sha256": sha256_file(folder / "verification.json")}
    pointer["pointer_id"] = sha256_json(pointer)
    write(root / custody.POINTER, pointer)
    return old, new, original, canonical, pair


def test_exact_pair_and_candidate_common_guard(tmp_path):
    old, new, original, canonical, pair = fixture(tmp_path)
    assert custody.resolve_snapshot(tmp_path, old) == new
    assert custody.registered_hardlink(original) and custody.registered_hardlink(canonical)
    assert custody.validate_pair(tmp_path, pair)
    generated, _ = prepared_candidates()
    candidate = tmp_path / "candidate_canonical.py"
    candidate.write_bytes(generated["src/futures_rebuild/canonical.py"])
    spec = importlib.util.spec_from_file_location("futures_rebuild._custody_candidate_test", candidate)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.sha256_file(canonical) == pair["sha256"]
    unrelated = tmp_path / "unrelated"
    unrelated.write_bytes(b"other")
    os.link(unrelated, tmp_path / "other_alias")
    with pytest.raises(ContractError, match="hard-linked"):
        module.sha256_file(unrelated)


@pytest.mark.parametrize("damage", ["third_link", "changed_bytes", "wrong_identity", "missing_new_root", "tampered_pointer"])
def test_successor_fails_closed(tmp_path, damage):
    old, new, original, canonical, pair = fixture(tmp_path)
    if damage == "third_link":
        os.link(original, tmp_path / "third")
    elif damage == "changed_bytes":
        original.write_bytes(b"changed custody bytes!")
    elif damage == "wrong_identity":
        canonical.unlink()
        canonical.write_bytes(b"synthetic custody bytes")
    elif damage == "missing_new_root":
        new.rename(new.with_name("missing"))
    else:
        (tmp_path / custody.POINTER).write_bytes(b"{}")
    with pytest.raises((IntegrityError, FileNotFoundError)):
        if damage == "missing_new_root":
            custody.resolve_snapshot(tmp_path, old)
        else:
            custody.registered_hardlink(canonical)


def test_inactive_and_unregistered_mapping(tmp_path):
    historical = custody.OLD_ROOT / ("b" * 64)
    assert custody.resolve_snapshot(tmp_path, historical) == tmp_path / historical
    fixture(tmp_path)
    with pytest.raises(IntegrityError, match="unregistered"):
        custody.resolve_snapshot(tmp_path, historical)
    with pytest.raises(IntegrityError, match="escapes"):
        custody.resolve_snapshot(tmp_path, "../outside")


def test_implementation_binding_requires_exact_activated_successor(tmp_path, monkeypatch):
    source = tmp_path / "consumer.py"
    source.write_bytes(b"old implementation")
    predecessor = sha256_file(source)
    assert custody.binding_matches(tmp_path, source, predecessor)
    source.write_bytes(b"custody-only successor")
    assert not custody.binding_matches(tmp_path, source, predecessor)
    manifest = {"implementation_successors": {"consumer.py": {
        "predecessor_sha256": predecessor, "successor_sha256": sha256_file(source)}}}
    monkeypatch.setattr(custody, "load_active", lambda root: manifest)
    assert custody.binding_matches(tmp_path, source, predecessor)
    assert not custody.binding_matches(tmp_path, source, "wrong predecessor")
    source.write_bytes(b"unreviewed implementation")
    assert not custody.binding_matches(tmp_path, source, predecessor)


@pytest.mark.parametrize("change", ["catalog", "research", "alias", "historical_authority"])
def test_custody_policy_rejects_scope_or_catalog_drift(tmp_path, change):
    fixture(tmp_path)
    manifest = custody.load_active(tmp_path)
    policy = manifest["custody_policy"]
    if change == "catalog":
        (tmp_path / "data/active/catalog.json").write_bytes(b'{"old_release": true}')
    elif change == "research":
        policy["limits"] = {**policy["limits"], "research_access": True}
    elif change == "alias":
        policy["canonical_aliases"].append("data/dbn/other.dbn.zst")
    else:
        policy["historical_policy_role"] = "INHERITED_ACTIVE_AUTHORITY"
    policy["policy_id"] = sha256_json({k: v for k, v in policy.items() if k != "policy_id"})
    with pytest.raises(IntegrityError):
        custody.validate_policy(tmp_path, manifest)


@pytest.mark.parametrize("fail_after", [1, 2, 3])
def test_each_storage_step_can_restore_original_identity(tmp_path, fail_after):
    old, new, rollback = (tmp_path / name for name in ("old", "new", "rollback"))
    old.mkdir()
    source = old / "original.dbn.zst"
    source.write_bytes(b"retained original")
    canonical = tmp_path / "canonical.dbn.zst"
    canonical.write_bytes(source.read_bytes())
    ids = (source.stat().st_ino, canonical.stat().st_ino)
    undo = []
    try:
        old.rename(new)
        undo.append(lambda: new.rename(old))
        if fail_after == 1: raise RuntimeError("interrupted")
        canonical.rename(rollback)
        undo.append(lambda: rollback.rename(canonical))
        if fail_after == 2: raise RuntimeError("interrupted")
        os.link(new / source.name, canonical)
        undo.append(canonical.unlink)
        raise RuntimeError("interrupted")
    except RuntimeError:
        for action in reversed(undo): action()
    assert source.read_bytes() == canonical.read_bytes() == b"retained original"
    assert (source.stat().st_ino, canonical.stat().st_ino) == ids
    assert source.stat().st_nlink == canonical.stat().st_nlink == 1


def test_all_six_candidates_together_without_live_installation(tmp_path, monkeypatch):
    from futures_rebuild import tbbo_16_market_acquisition as live_acquisition
    from futures_rebuild import tbbo_quality_certification as live_quality
    from futures_rebuild import tbbo_catalog_admission as live_admission
    generated, active_successors = prepared_candidates()
    for relative, data in generated.items():
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    metadata = {live_acquisition.PLAN_PATH, live_quality.PLAN_PATH, live_admission.PLAN_PATH,
                live_quality.ACQUISITION_PLAN_PATH, live_quality.HISTORICAL_POLICY_PATH,
                live_quality.TBBO_REGISTRY, live_quality.STATUS_SESSION_DIAGNOSTIC_PATH,
                Path("src/futures_rebuild/source_symbology.py")}
    for relative in metadata:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    successors = active_successors or {p: {"predecessor_sha256": sha256_file(ROOT / p),
                      "successor_sha256": hashlib.sha256(data).hexdigest()} for p, data in generated.items()}
    old, new, original, canonical, pair = fixture(tmp_path, successors)
    sidecar = Path(str(original) + ".manifest.json")
    write(sidecar, {"synthetic": True})
    namespace = "_custody_isolated_test"
    package = ModuleType(namespace)
    package.__path__ = [str(tmp_path / "src/futures_rebuild"), str(ROOT / "src/futures_rebuild")]
    monkeypatch.setitem(sys.modules, namespace, package)
    try:
        modules = {Path(p).stem: importlib.import_module(namespace + "." + Path(p).stem) for p in generated}
        acquisition, quality, admission, publication = (modules[n] for n in (
            "tbbo_16_market_acquisition", "tbbo_quality_certification", "tbbo_catalog_admission", "tbbo_custody_publication"))
        for module in (acquisition, quality, admission):
            assert module.load_plan(root=tmp_path)["plan_id"]
        assert modules["canonical"].sha256_file(canonical) == pair["sha256"]
        assert modules["boundary"].RepoBoundary(tmp_path).assert_snapshot_path(old) == new
        assert len(acquisition.plain_tree_files(new, reject_hardlinks=True)) == 2
        entry = {"dbn": {"path": original.relative_to(new).as_posix(), "size": original.stat().st_size, "sha256": pair["sha256"]},
                 "sidecar": {"path": sidecar.relative_to(new).as_posix(), "size": sidecar.stat().st_size, "sha256": sha256_file(sidecar)}}
        publication._verify_published(tmp_path, {"vault_snapshot_path": old.relative_to(tmp_path).as_posix(), "entries": [entry]})
        support = {"active_definition_status_bindings": [
            {"market": "ES", "schema": "status", "path": f"data/dbn/status/ES/{year}.dbn.zst", "sha256": "a" * 64}
            for year in (2024, 2025)],
            "supplement": {"vault_snapshot_path": old.relative_to(tmp_path).as_posix(),
                           "entries": [{"request": {"market": "ES", "schema": "status"}, "dbn": entry["dbn"]}]}}
        sources = quality._support_sources(support, root=tmp_path, market="ES", schema="status")
        assert (original, pair["sha256"]) in sources and len(sources) == 3
        # Synthetic one-entry custody exercises the admission reader's mapped path;
        # exact real plans above remain byte-identical, not rewritten for this test.
        registry = {"vault_snapshot_path": old.relative_to(tmp_path).as_posix(), "entries": [entry],
                    "snapshot_id": "synthetic", "dbn_file_count": 1, "sidecar_file_count": 1,
                    "total_payload_bytes": original.stat().st_size}
        certificate = {"certificate_id": "synthetic", "state": "CERTIFIED_ROW_QUALITY_WITH_MANDATORY_EXCLUSIONS_NOT_CATALOG_ADMITTED",
                       "research_catalog_admitted": False, "alpha_research_authorized": False,
                       "markets": list(admission.MARKETS), "market_results": [{"quality_passed": True}]}
        write(tmp_path / "synthetic_registry.json", registry)
        write(tmp_path / "synthetic_certificate.json", certificate)
        plan = {"custody_registry_path": "synthetic_registry.json", "quality_certificate_path": "synthetic_certificate.json",
                "custody_registry_sha256": sha256_file(tmp_path / "synthetic_registry.json"),
                "quality_certificate_sha256": sha256_file(tmp_path / "synthetic_certificate.json"),
                "snapshot_id": "synthetic", "quality_certificate_id": "synthetic", "total_payload_bytes": original.stat().st_size}
        monkeypatch.setattr(admission, "load_plan", lambda **kwargs: plan)
        monkeypatch.setattr(admission, "DBN_FILE_COUNT", 1)
        monkeypatch.setattr(admission, "SIDECAR_FILE_COUNT", 1)
        assert admission._bound_sources(root=tmp_path, verify_payloads=True)[1] == registry
    finally:
        for name in list(sys.modules):
            if name.startswith(namespace + "."):
                del sys.modules[name]
