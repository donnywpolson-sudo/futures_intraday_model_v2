from __future__ import annotations

from pathlib import Path

import pytest

import futures_rebuild.active_data_full_certification as full_certification
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError


def test_full_source_inventory_hashes_each_unique_object_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = tmp_path / "first.bin"
    second = tmp_path / "second.bin"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    plan = {
        "limits": {
            "maximum_source_bytes": first.stat().st_size + second.stat().st_size,
            "maximum_source_files": 2,
        },
        "source_objects": [
            {
                "path": "first.bin",
                "sha256": sha256_file(first),
                "size": first.stat().st_size,
            },
            {
                "path": "second.bin",
                "sha256": sha256_file(second),
                "size": second.stat().st_size,
            },
        ],
    }
    calls: list[Path] = []
    real_hash = full_certification.sha256_file

    def counted(path: Path) -> str:
        calls.append(path)
        return real_hash(path)

    monkeypatch.setattr(full_certification, "sha256_file", counted)
    result = full_certification.verify_source_inventory_once(tmp_path, plan)
    assert result["source_files"] == 2
    assert calls == [first, second]


def test_full_source_inventory_rejects_duplicate_paths(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"source")
    item = {
        "path": "source.bin",
        "sha256": sha256_file(source),
        "size": source.stat().st_size,
    }
    with pytest.raises(IntegrityError, match="duplicated"):
        full_certification.verify_source_inventory_once(
            tmp_path,
            {
                "limits": {
                    "maximum_source_bytes": source.stat().st_size * 2,
                    "maximum_source_files": 2,
                },
                "source_objects": [item, dict(item)],
            },
        )
