from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.cash_open_calendar_publication import (
    ACTIVE_POINTER_PATH,
    CALENDAR_ID,
    CALENDAR_SHA256,
    GENERAL_POINTER_PATH,
    GENERAL_POINTER_SHA256,
    _activate_pointer_with_rollback,
    load_active_calendar,
    publication_documents,
)


ROOT = Path(__file__).resolve().parents[1]


def test_publication_documents_bind_the_exact_prepared_successor() -> None:
    documents = publication_documents(root=ROOT)
    registration, event, pointer = (
        documents["registration"], documents["event"], documents["pointer"]
    )
    assert registration["calendar_id"] == pointer["calendar_id"] == CALENDAR_ID
    assert registration["calendar_sha256"] == pointer["calendar_sha256"] == CALENDAR_SHA256
    assert registration["registration_id"] == sha256_json(
        {key: value for key, value in registration.items() if key != "registration_id"}
    )
    assert event["event_id"] == sha256_json(
        {key: value for key, value in event.items() if key != "event_id"}
    )
    assert pointer["pointer_id"] == sha256_json(
        {key: value for key, value in pointer.items() if key != "pointer_id"}
    )
    assert pointer["general_exchange_calendar_pointer"] == {
        "path": GENERAL_POINTER_PATH.as_posix(), "sha256": GENERAL_POINTER_SHA256
    }
    assert registration["authority"]["price_rows_read"] is False
    assert registration["authority"]["year_2025_accessed"] is False


def test_active_loader_is_transition_stable() -> None:
    pointer = ROOT / ACTIVE_POINTER_PATH
    if pointer.exists():
        selected = __import__("json").loads(pointer.read_text(encoding="utf-8"))
        if selected["calendar_id"] == CALENDAR_ID:
            payload = load_active_calendar(root=ROOT)
            assert payload["calendar_id"] == CALENDAR_ID
            assert payload["unresolved_reference_count"] == 0
        else:
            from futures_rebuild.cash_open_calendar_grid_publication import (
                CALENDAR_ID as GRID_CALENDAR_ID,
                load_active_grid_calendar,
            )
            assert selected["calendar_id"] == GRID_CALENDAR_ID
            assert load_active_grid_calendar(root=ROOT)["calendar_id"] == GRID_CALENDAR_ID
    else:
        documents = publication_documents(root=ROOT)
        assert documents["pointer"]["state"] == "ACTIVE_REFERENCE_ONLY"


def test_published_records_are_exact_when_present() -> None:
    documents = publication_documents(root=ROOT)
    registration_path = ROOT / documents["pointer"]["registration_path"]
    event_path = ROOT / documents["pointer"]["event_path"]
    if registration_path.exists() or event_path.exists():
        assert registration_path.read_bytes() == canonical_bytes(documents["registration"]) + b"\n"
        assert event_path.read_bytes() == canonical_bytes(documents["event"]) + b"\n"


def test_new_pointer_is_removed_when_postcheck_fails(tmp_path: Path) -> None:
    pointer = tmp_path / "active.json"
    with pytest.raises(RuntimeError, match="postcheck"):
        _activate_pointer_with_rollback(
            pointer_path=pointer,
            new_bytes=b"new\n",
            postcheck=lambda: (_ for _ in ()).throw(RuntimeError("postcheck")),
        )
    assert not pointer.exists()


def test_existing_pointer_is_restored_byte_for_byte_when_postcheck_fails(tmp_path: Path) -> None:
    pointer = tmp_path / "active.json"
    old = canonical_bytes({"pointer": "old"}) + b"\n"
    pointer.write_bytes(old)
    with pytest.raises(RuntimeError, match="postcheck"):
        _activate_pointer_with_rollback(
            pointer_path=pointer,
            new_bytes=b"new\n",
            postcheck=lambda: (_ for _ in ()).throw(RuntimeError("postcheck")),
        )
    assert pointer.read_bytes() == old
