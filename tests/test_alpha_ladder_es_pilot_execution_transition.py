from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import yaml

import futures_rebuild.alpha_ladder_es_pilot_execution as pilot
from futures_rebuild.alpha_research_ladder import build_active_pointer
from futures_rebuild.canonical import canonical_bytes
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
        path = Path(relative)
        if path.as_posix() == "configs/active_alpha_research_ladder.json":
            contract_path = next(
                item for item in plan["immutable_bindings"]
                if item.endswith("/universe_contract.json")
                and "state/alpha_ladder_registry/" in item
            )
            profile_path = next(
                item for item in plan["immutable_bindings"]
                if item.endswith("/alpha_tiered.yaml")
                and "state/alpha_ladder_registry/" in item
            )
            contract = json.loads((ROOT / contract_path).read_text(encoding="utf-8"))
            profile = yaml.safe_load((ROOT / profile_path).read_text(encoding="utf-8"))
            pointer = build_active_pointer(
                contract_path=contract_path,
                contract_sha256=plan["immutable_bindings"][contract_path],
                contract_id=str(contract["contract_id"]),
                profile_path=profile_path,
                profile_sha256=plan["immutable_bindings"][profile_path],
                profile_id=str(profile["profile_id"]),
            )
            target = tmp_path / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(canonical_bytes(pointer) + b"\n")
        else:
            _copy(tmp_path, path)
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
    with pytest.raises(IntegrityError, match="plan changed"):
        pilot.load_plan(root=root, verify_protected=False)
    plan = json.loads((root / pilot.PLAN_PATH).read_text(encoding="utf-8"))
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
    plan = json.loads((root / pilot.PLAN_PATH).read_text(encoding="utf-8"))
    with pytest.raises(IntegrityError, match="plan changed"):
        pilot.load_plan(root=root)
    changed = json.loads(json.dumps(plan))
    changed["trial_id"] = "f" * 64
    with pytest.raises(IntegrityError, match="plan changed"):
        pilot.validate_plan(changed, root=root)
    with pytest.raises(IntegrityError, match="plan changed"):
        pilot.additional_execution_scope(
            root=root,
            plan=plan,
            pushed_git_head="a" * 40,
        )


def test_plan_is_valid_before_execution_and_refuses_completed_live_state(
    tmp_path: Path,
) -> None:
    root = _preexecution_root(tmp_path)
    plan = json.loads((root / pilot.PLAN_PATH).read_text(encoding="utf-8"))
    assert plan["state"] == "PREPARED_NOT_AUTHORIZED_NOT_EXECUTED"
    with pytest.raises(IntegrityError, match="plan changed"):
        pilot.load_plan(root=root)

    assert (ROOT / pilot.OUTPUT_ROOT / "pilot_decision.json").is_file()
    with pytest.raises((IntegrityError, UnauthorizedOperation)):
        pilot.load_plan(root=ROOT)
