"""Windows writer exclusion and real journal rollback, using tiny synthetic files."""
import os
import runpy
from pathlib import Path

import pytest

pytestmark = pytest.mark.high_risk
API = runpy.run_path(str(Path(__file__).resolve().parents[1] / "reports/custody_successor_v1/approved_transaction.py"))


def test_writer_exclusion_survives_rename_and_link(tmp_path):
    old = tmp_path / "old"
    old.mkdir()
    file = old / "source.dbn.zst"
    file.write_bytes(b"original")
    with API["exclude_writer"](file):
        with pytest.raises(OSError):
            file.open("r+b")
        (tmp_path / "new").mkdir()
        file.rename(tmp_path / "new/source.dbn.zst")
        original = tmp_path / "new/source.dbn.zst"
        os.link(original, tmp_path / "alias")
        with pytest.raises(OSError):
            (tmp_path / "alias").open("r+b")
        assert original.read_bytes() == b"original"


def test_existing_writer_blocks_guard(tmp_path):
    path = tmp_path / "source"
    path.write_bytes(b"original")
    with path.open("r+b"):
        with pytest.raises(OSError):
            with API["exclude_writer"](path):
                pytest.fail("guard admitted an existing writer")


def test_exact_deletion_rejects_drift_and_preserves_retained_hardlinks(tmp_path):
    deletion = runpy.run_path(str(Path(__file__).resolve().parents[1] / "reports/custody_successor_v1/approved_deletion.py"))
    target, original, alias = (tmp_path / name for name in ("copy.dbn.zst", "original", "alias"))
    target.write_bytes(b"source")
    original.write_bytes(b"source")
    os.link(original, alias)
    entry = {"path": target.name, "absolute_path": str(target.resolve()), **API["physical"](target)}
    from futures_rebuild.errors import IntegrityError
    with API["exclude_writer"](target), API["exclude_writer"](original, rename=False), API["exclude_writer"](alias, rename=False):
        with pytest.raises(IntegrityError):
            deletion["unlink_exact"](tmp_path, {**entry, "file_id": "wrong"}, tmp_path / "journal", 0)
        assert target.exists() and not (tmp_path / "journal").exists()
        with pytest.raises(OSError):
            alias.unlink()
        deletion["unlink_exact"](tmp_path, entry, tmp_path / "journal", 0)
    assert not target.exists()
    assert original.read_bytes() == alias.read_bytes() == b"source"
    assert original.stat().st_ino == alias.stat().st_ino and original.stat().st_nlink == 2
    assert len(list((tmp_path / "journal").glob("*.json"))) == 2


@pytest.mark.parametrize("failure_step", [1, 2, 3, 4, 5])
def test_real_journal_reverses_each_transaction_step(tmp_path, failure_step):
    snapshot = tmp_path / "old"
    snapshot.mkdir()
    (snapshot / "original").write_bytes(b"source")
    canonical = tmp_path / "canonical"
    canonical.write_bytes(b"source")
    code = tmp_path / "code"
    code.write_bytes(b"old code")
    candidate = tmp_path / "candidate"
    candidate.write_bytes(b"new code")
    old_ids = {p: p.stat().st_ino for p in (snapshot, canonical, code, candidate)}
    journal = API["Journal"](tmp_path, tmp_path / "journal")
    steps = [("move", snapshot / "original", tmp_path / "new/original"),
             ("move", canonical, tmp_path / "rollback/canonical"),
             ("link", tmp_path / "new/original", canonical),
             ("move", code, tmp_path / "rollback/code"),
             ("move", candidate, code)]
    for step in steps[:failure_step]:
        journal.perform(*step)
    # Recover from durable intents, as after interruption, not only RAM state.
    recovered = API["Journal"](tmp_path, tmp_path / "journal")
    import json
    recovered.operations = [json.loads(p.read_bytes()) for p in sorted((tmp_path / "journal").glob("*.json"))]
    recovered.rollback()
    assert all(p.stat().st_ino == inode for p, inode in old_ids.items())
    assert canonical.stat().st_nlink == 1 and code.read_bytes() == b"old code"
