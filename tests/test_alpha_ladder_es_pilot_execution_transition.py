from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

import futures_rebuild.alpha_ladder_es_pilot_execution as pilot
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]


def _copy(root: Path, relative: Path) -> None:
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / relative, target)


def _preexecution_root(tmp_path: Path) -> Path:
    plan = json.loads((ROOT / pilot.PLAN_PATH).read_text(encoding="utf-8"))
    _copy(tmp_path, pilot.PLAN_PATH)
    for relative in plan["immutable_bindings"]:
        _copy(tmp_path, Path(relative))
    return tmp_path


def test_preexecution_plan_remains_source_safe_in_a_clean_shadow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _preexecution_root(tmp_path)
    original = pilot.sha256_file

    def reject_parquet(path: Path, *args, **kwargs):
        if path.suffix == ".parquet":
            raise AssertionError("plan validation opened a protected Parquet file")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(pilot, "sha256_file", reject_parquet)
    plan = pilot.load_plan(root=root, verify_protected=False)
    assert plan["trial_id"] == pilot.TRIAL_ID
    assert list(plan["source_bindings"]) == [
        f"data/active/causally_gated_normalized/ES/{year}/{year}.parquet"
        for year in (2018, 2019, 2020)
    ]
    assert plan["authority"]["attempts"] == 1
    assert plan["authority"]["retries"] == 0
    assert not hasattr(pilot, "main")
    assert "alpha_ladder_es_pilot_execution" not in (
        root / "pyproject.toml"
    ).read_text(encoding="utf-8").split("[project.scripts]", 1)[1].split("[", 1)[0]


def test_preexecution_binding_substitution_still_fails_closed_in_shadow(
    tmp_path: Path,
) -> None:
    root = _preexecution_root(tmp_path)
    plan = pilot.load_plan(root=root)
    changed = json.loads(json.dumps(plan))
    changed["trial_id"] = "f" * 64
    with pytest.raises(IntegrityError, match="plan changed"):
        pilot.validate_plan(changed, root=root)
    scope = pilot.additional_execution_scope(
        root=root,
        plan=plan,
        pushed_git_head="a" * 40,
    )
    assert scope["execution_plan_id"] == plan["plan_id"]
    assert scope["execution_plan_sha256"] == sha256_file(root / pilot.PLAN_PATH)
    assert scope["execution_output_root"] == pilot.OUTPUT_ROOT.as_posix()


def test_plan_is_valid_before_execution_and_refuses_completed_live_state(
    tmp_path: Path,
) -> None:
    root = _preexecution_root(tmp_path)
    plan = pilot.load_plan(root=root)
    assert plan["state"] == "PREPARED_NOT_AUTHORIZED_NOT_EXECUTED"
    (root / pilot.OUTPUT_ROOT).mkdir(parents=True)
    with pytest.raises(UnauthorizedOperation, match="output_root already exists"):
        pilot.load_plan(root=root)

    assert (ROOT / pilot.OUTPUT_ROOT / "pilot_decision.json").is_file()
    with pytest.raises(UnauthorizedOperation, match="output_root already exists"):
        pilot.load_plan(root=ROOT)
