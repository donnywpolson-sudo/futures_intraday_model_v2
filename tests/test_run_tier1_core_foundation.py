from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _runner_module():
    path = Path(__file__).parents[1] / "scripts" / "run_tier1_core_foundation.py"
    spec = importlib.util.spec_from_file_location("tier1_core_runner", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_pairs_accepts_only_unique_core_pairs() -> None:
    runner = _runner_module()

    assert runner._parse_pairs(["ZN-2021", "6E-2018"]) == (("ZN", 2021), ("6E", 2018))
    with pytest.raises(ValueError, match="unsupported or duplicate"):
        runner._parse_pairs(["ES-2018", "ES-2018"])
    with pytest.raises(ValueError, match="unsupported or duplicate"):
        runner._parse_pairs(["GC-2018"])


def test_release_state_requires_payload_manifest_and_report(boundary) -> None:
    runner = _runner_module()
    release_id = "a" * 64
    payload = (
        boundary.active_root
        / "data/outcomes/active_es_60s_300s_v2/ZN/2021/2021"
        / release_id
        / "outcomes.parquet"
    )
    payload.parent.mkdir(parents=True)
    payload.write_bytes(b"test")
    manifest = boundary.active_root / "manifests/data_releases/outcomes" / f"{release_id}.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("{}", encoding="utf-8")

    with pytest.raises(RuntimeError, match="partial"):
        runner._release_state(boundary=boundary, market="ZN", year=2021, kind="outcomes")

    report = boundary.active_root / "reports/phase3_outcomes/tier1_core/ZN/2021" / release_id / "report.json"
    report.parent.mkdir(parents=True)
    report.write_text("{}", encoding="utf-8")

    assert runner._release_state(boundary=boundary, market="ZN", year=2021, kind="outcomes") == "complete"
