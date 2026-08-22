from __future__ import annotations

import importlib.util
import json
import os
import runpy
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

import futures_rebuild
from futures_rebuild import dual_resolution_foundation as legacy_foundation
from futures_rebuild.foundation_operation_firewall import (
    RETIRED_STATUS,
    RetiredFoundationOperation,
)


REPO = Path(__file__).resolve().parents[1]
RUNNER = REPO / "scripts/run_dual_resolution_tier01_foundation.py"
PROBE_ID = "retirement-firewall-probe"
PROBE_ROOT = REPO / "reports/dual_resolution_tier01_foundation" / PROBE_ID
VALID_ARGV = [str(RUNNER), "preflight", "--run-id", PROBE_ID]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def _assert_no_effects() -> None:
    assert not PROBE_ROOT.exists()
    assert not (REPO / "state/locks/dual_resolution_tier01_foundation.lock").exists()


def _subprocess(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO / "src")
    return subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True, timeout=30)


def test_direct_subprocess_and_altered_cwd_reject_before_effects(tmp_path: Path) -> None:
    for cwd in (REPO, tmp_path):
        result = _subprocess([sys.executable, *VALID_ARGV], cwd=cwd)
        assert result.returncode != 0
        assert RETIRED_STATUS in result.stderr
        _assert_no_effects()


def test_bare_subprocess_parser_exit_is_effect_free() -> None:
    result = _subprocess([sys.executable, str(RUNNER)], cwd=REPO)
    assert result.returncode == 2
    assert "required" in result.stderr
    _assert_no_effects()


def test_runpy_after_package_and_legacy_module_preimport_fails(monkeypatch) -> None:
    assert futures_rebuild is sys.modules["futures_rebuild"]
    assert legacy_foundation is sys.modules["futures_rebuild.dual_resolution_foundation"]
    monkeypatch.setattr(sys, "argv", VALID_ARGV)
    with pytest.raises(RetiredFoundationOperation, match=RETIRED_STATUS):
        runpy.run_path(str(RUNNER), run_name="__main__")
    _assert_no_effects()


def test_runpy_before_package_import_in_fresh_interpreter_fails() -> None:
    code = (
        "import runpy,sys; "
        f"sys.argv={VALID_ARGV!r}; "
        f"runpy.run_path({str(RUNNER)!r},run_name='__main__')"
    )
    result = _subprocess([sys.executable, "-c", code], cwd=REPO)
    assert result.returncode != 0 and RETIRED_STATUS in result.stderr
    _assert_no_effects()


def test_importlib_loading_after_package_preimport_then_direct_main_fails(monkeypatch) -> None:
    spec = importlib.util.spec_from_file_location("retired_foundation_probe", RUNNER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(sys, "argv", VALID_ARGV)
    with pytest.raises(RetiredFoundationOperation, match=RETIRED_STATUS):
        module.main()
    _assert_no_effects()


def test_exec_compile_after_package_preimport_fails(monkeypatch) -> None:
    namespace = {"__name__": "__main__", "__file__": str(RUNNER)}
    monkeypatch.setattr(sys, "argv", VALID_ARGV)
    with pytest.raises(RetiredFoundationOperation, match=RETIRED_STATUS):
        exec(compile(RUNNER.read_bytes(), str(RUNNER), "exec"), namespace)
    _assert_no_effects()


def test_direct_exposed_main_and_run_constructor_fail_before_effects(monkeypatch) -> None:
    spec = importlib.util.spec_from_file_location("retired_foundation_direct", RUNNER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(sys, "argv", VALID_ARGV)
    with pytest.raises(RetiredFoundationOperation, match=RETIRED_STATUS):
        module.main()
    with pytest.raises(RetiredFoundationOperation, match=RETIRED_STATUS):
        module.FoundationRun(REPO, PROBE_ID)
    _assert_no_effects()


def test_discovery_guard_precedes_git_source_discovery(monkeypatch) -> None:
    calls = 0

    def forbidden(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("source discovery was reached")

    monkeypatch.setattr(legacy_foundation.subprocess, "run", forbidden)
    with pytest.raises(RetiredFoundationOperation, match=RETIRED_STATUS):
        legacy_foundation.discover_repository(REPO)
    assert calls == 0
    _assert_no_effects()


def test_current_public_surfaces_and_selector_have_no_legacy_fallback() -> None:
    surface = json.loads((REPO / "configs/repository_surface.json").read_text(encoding="utf-8"))
    runner_entries = [
        entry for entry in surface["entries"]
        if entry["path_or_pattern"] == "scripts/run_dual_resolution_tier01_foundation.py"
    ]
    assert len(runner_entries) == 1
    assert runner_entries[0]["classification"] == "HISTORICAL_HASH_BOUND"
    assert runner_entries[0]["authority_role"] == "RETIRED_DUAL_RESOLUTION_FOUNDATION_RUNNER"
    project = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    for target in project["project"]["scripts"].values():
        assert "dual_resolution" not in target
    selector = (REPO / "src/futures_rebuild/causal_source_closure.py").read_text(encoding="utf-8")
    assert "dual_resolution" not in selector
    assert "run_dual_resolution_tier01_foundation" not in selector
