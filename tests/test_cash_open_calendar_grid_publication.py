from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.cash_open_calendar_grid_publication import (
    ACTIVE_POINTER_PATH,
    CALENDAR_ID,
    CALENDAR_SHA256,
    _activate_pointer_with_rollback,
    load_active_grid_calendar,
    publication_documents,
)


ROOT = Path(__file__).resolve().parents[1]


def test_grid_publication_documents_are_hash_bound_and_prepared() -> None:
    active = json.loads((ROOT / ACTIVE_POINTER_PATH).read_text(encoding="utf-8"))
    if active["calendar_id"] == CALENDAR_ID:
        assert load_active_grid_calendar(root=ROOT)["calendar_id"] == CALENDAR_ID
        return
    documents = publication_documents(root=ROOT)
    for name, identity in (
        ("registration", "registration_id"),
        ("event", "event_id"),
        ("pointer", "pointer_id"),
    ):
        payload = documents[name]
        assert payload[identity] == sha256_json(
            {key: value for key, value in payload.items() if key != identity}
        )
    assert documents["pointer"]["calendar_id"] == CALENDAR_ID
    assert documents["pointer"]["calendar_sha256"] == CALENDAR_SHA256
    assert documents["registration"]["authority"]["price_rows_read"] is False


def test_active_pointer_is_transition_stable() -> None:
    pointer = json.loads((ROOT / ACTIVE_POINTER_PATH).read_text(encoding="utf-8"))
    if pointer["calendar_id"] == CALENDAR_ID:
        calendar = load_active_grid_calendar(root=ROOT)
        assert calendar["checkpoint_grid"] == ["09:00", "09:30", "10:00", "10:30"]
    else:
        assert pointer["calendar_id"] == "54bc5550a0ba28af2a509fb32c756b39041686ba10ffa6bd832e6d96469c0397"
        assert publication_documents(root=ROOT)["pointer"]["calendar_id"] == CALENDAR_ID


def test_grid_pointer_rollback_restores_predecessor_bytes(tmp_path: Path) -> None:
    pointer = tmp_path / "active.json"
    old = canonical_bytes({"calendar": "old"}) + b"\n"
    pointer.write_bytes(old)
    with pytest.raises(RuntimeError, match="postcheck"):
        _activate_pointer_with_rollback(
            pointer_path=pointer,
            new_bytes=b"new\n",
            postcheck=lambda: (_ for _ in ()).throw(RuntimeError("postcheck")),
        )
    assert pointer.read_bytes() == old
