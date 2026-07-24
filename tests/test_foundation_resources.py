from __future__ import annotations

from collections import namedtuple
from pathlib import Path

import pytest

import futures_rebuild.foundation.resources as resources
from futures_rebuild.errors import IntegrityError
from futures_rebuild.foundation.resources import (
    FoundationResourcePolicy,
    assert_capacity_admission,
    assert_runtime_capacity,
    selected_compressed_bytes,
)


REPO = Path(__file__).resolve().parents[1]
DiskUsage = namedtuple("usage", "total used free")


def _policy() -> FoundationResourcePolicy:
    return FoundationResourcePolicy.from_file(
        REPO / "configs" / "foundation_resource_policy.json"
    )


def test_resource_policy_and_selected_byte_census_are_exact() -> None:
    policy = _policy()
    assert policy.selected_compressed_input_multiplier == 32
    assert policy.reserve_bytes(1_000_000_000_000) == 100_000_000_000
    assert selected_compressed_bytes(
        {"files": [{"path": "a", "size": 10}, {"path": "b", "size": 20}]}
    ) == 30
    with pytest.raises(IntegrityError, match="duplicated"):
        selected_compressed_bytes(
            {"files": [{"path": "a", "size": 10}, {"path": "a", "size": 20}]}
        )


def test_capacity_admission_and_runtime_guard_fail_before_writes(monkeypatch) -> None:
    policy = _policy()
    selection = {"files": [{"path": "a", "size": 1_000_000_000}]}
    monkeypatch.setattr(
        resources.shutil,
        "disk_usage",
        lambda _path: DiskUsage(1_000_000_000_000, 0, 200_000_000_000),
    )
    receipt = assert_capacity_admission(
        volume_path=Path("."), selection=selection, policy=policy
    )
    assert receipt["status"] == "PASS"
    monkeypatch.setattr(
        resources.shutil,
        "disk_usage",
        lambda _path: DiskUsage(1_000_000_000_000, 0, 120_000_000_000),
    )
    with pytest.raises(IntegrityError, match="admission failed"):
        assert_capacity_admission(
            volume_path=Path("."), selection=selection, policy=policy
        )
    monkeypatch.setattr(
        resources.shutil,
        "disk_usage",
        lambda _path: DiskUsage(1_000_000_000_000, 0, 100_000_000_000),
    )
    with pytest.raises(IntegrityError, match="reserve"):
        assert_runtime_capacity(volume_path=Path("."), policy=policy)
