from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_limit_exit_closure_publication import (
    CLOSURE_ID,
    PREPARED_CLOSURE_PATH,
    PREPARED_CLOSURE_SHA256,
    build_published_closure,
    load_prepared_closure,
    publish_closure,
    registry_path,
    verify_published_closure,
)
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]


def _copy(root: Path, relative: Path) -> None:
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / relative, target)


def _shadow_root(tmp_path: Path) -> Path:
    prepared = load_prepared_closure(root=ROOT)
    _copy(tmp_path, PREPARED_CLOSURE_PATH)
    for relative in prepared["bindings"]:
        _copy(tmp_path, Path(relative))
    pointer = tmp_path / "configs/active_alpha_research_ladder.json"
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_bytes(b'{"sentinel":true}\n')
    return tmp_path


def test_prepared_closure_is_exact_and_not_an_economic_failure() -> None:
    closure = load_prepared_closure(root=ROOT)
    assert closure["closure_id"] == CLOSURE_ID
    assert sha256_file(ROOT / PREPARED_CLOSURE_PATH) == PREPARED_CLOSURE_SHA256
    assert closure["economic_result"] == "NOT_PRODUCED"
    assert closure["strategy_failure"] is False
    assert closure["pilot_registration_status"] == "FORBIDDEN"


def test_published_record_is_closed_but_never_activates_anything() -> None:
    published = build_published_closure(root=ROOT)
    assert published["closure_id"] == CLOSURE_ID
    assert published["state"] == "CLOSED_PRE_REGISTRATION_SOURCE_INCOMPATIBLE"
    assert published["publication"] is True
    assert published["active_pointer_mutated"] is False
    assert published["activation_authorized"] is False


def test_shadow_publication_is_create_only_idempotent_and_pointer_stable(
    tmp_path: Path,
) -> None:
    root = _shadow_root(tmp_path)
    pointer = root / "configs/active_alpha_research_ladder.json"
    before = pointer.read_bytes()
    first = publish_closure(root=root)
    second = publish_closure(root=root)
    assert first == second == verify_published_closure(root=root)
    assert (root / registry_path()).is_file()
    assert pointer.read_bytes() == before


def test_publication_fails_closed_on_prepared_source_drift(tmp_path: Path) -> None:
    root = _shadow_root(tmp_path)
    source = root / PREPARED_CLOSURE_PATH
    value = json.loads(source.read_text(encoding="utf-8"))
    value["minimum_training_shortfall_sessions"] = 8
    source.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(IntegrityError):
        publish_closure(root=root)
