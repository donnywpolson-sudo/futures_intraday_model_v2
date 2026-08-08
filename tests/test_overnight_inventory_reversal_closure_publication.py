from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.overnight_inventory_reversal_closure_publication import (
    ACTIVE_POINTER,
    CLARIFICATION_SOURCE,
    EVENT_SOURCE,
    READINESS_SOURCE,
    load_closure_publication,
    publish_closure_clarification,
)


ROOT = Path(__file__).resolve().parents[1]
ORIGINAL_CLOSURE = Path(
    "state/unpublished_evidence/overnight_inventory_reversal/"
    "24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c/"
    "terminal_closure.json"
)
AUTHORIZATION_USE = Path(
    "state/authorization_uses/"
    "6fc820d1327dce34af854b424e1b72ef9022985f1a3f4020e6c56eb0e6b223d0.json"
)


def _copy(root: Path, relative: Path) -> None:
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / relative, target)


def _prepared_root(tmp_path: Path) -> Path:
    for relative in (
        CLARIFICATION_SOURCE,
        EVENT_SOURCE,
        READINESS_SOURCE,
        ORIGINAL_CLOSURE,
        AUTHORIZATION_USE,
        ACTIVE_POINTER,
    ):
        _copy(tmp_path, relative)
    return tmp_path


def test_live_preparation_is_complete_but_not_published() -> None:
    prepared = load_closure_publication(root=ROOT)
    assert prepared["clarification_id"] == (
        "d4f97ae68be1dd0074dd20917d61fdb99a4da5e2c552239e8d401403223ea643"
    )
    assert prepared["report_id"] == (
        "abc910ff3a7cc5d96c59b000d74b9751b5d2fab19409653befcfa8d03f85b440"
    )


def test_publication_is_create_only_idempotent_and_pointer_stable(tmp_path) -> None:
    root = _prepared_root(tmp_path)
    pointer_before = (root / ACTIVE_POINTER).read_bytes()
    first = publish_closure_clarification(root=root)
    second = publish_closure_clarification(root=root)
    assert first == second
    assert (root / ACTIVE_POINTER).read_bytes() == pointer_before
    assert (root / first["closure_path"]).read_bytes() == (root / CLARIFICATION_SOURCE).read_bytes()
    assert (root / first["readiness_path"]).read_bytes() == (root / READINESS_SOURCE).read_bytes()
    assert (root / first["event_path"]).read_bytes() == (root / EVENT_SOURCE).read_bytes()


def test_publication_fails_closed_on_readiness_drift(tmp_path) -> None:
    root = _prepared_root(tmp_path)
    (root / READINESS_SOURCE).write_bytes(b"changed\n")
    with pytest.raises((IntegrityError, ValueError)):
        publish_closure_clarification(root=root)
