from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_calendar_observability_publication import (
    ACTIVE_POINTER_PATH,
    ACTIVE_POINTER_SHA256,
    CALENDAR_ID,
    CALENDAR_SHA256,
    MECHANISM_ID,
    PUBLISHED_CALENDAR_PATH,
    PREDECESSOR_CALENDAR_ID,
    PREDECESSOR_POINTER_SNAPSHOT_PATH,
    _activate_pointer_with_rollback,
    publication_documents,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json


ROOT = Path(__file__).resolve().parents[1]
V1_REGISTRATION_PATH = (
    PUBLISHED_CALENDAR_PATH.parent / "registration.json"
)
V1_EVENT_PATH = Path(
    "state/calendar_events/cash_open_impulse_41_market/"
    "cc8c62b968b4a6fafef96b0c747896449dc6b735ca6fdb593e1fb51ecde6bbf5.json"
)


def _v1_documents_or_preserved_records():
    if sha256_file(ROOT / ACTIVE_POINTER_PATH) == ACTIVE_POINTER_SHA256:
        return publication_documents(root=ROOT)
    registration = json.loads(
        (ROOT / V1_REGISTRATION_PATH).read_text(encoding="utf-8")
    )
    event = json.loads((ROOT / V1_EVENT_PATH).read_text(encoding="utf-8"))
    return {"registration": registration, "event": event, "pointer": None}


def test_publication_documents_are_deterministic_and_do_not_register_mechanism():
    first = _v1_documents_or_preserved_records()
    second = _v1_documents_or_preserved_records()
    assert first == second
    registration = first["registration"]
    event = first["event"]
    pointer = first["pointer"]
    assert registration["registration_id"] == sha256_json(
        {key: value for key, value in registration.items() if key != "registration_id"}
    )
    assert event["event_id"] == sha256_json(
        {key: value for key, value in event.items() if key != "event_id"}
    )
    if pointer is not None:
        assert pointer["pointer_id"] == sha256_json(
            {key: value for key, value in pointer.items() if key != "pointer_id"}
        )
        assert registration["calendar_id"] == pointer["calendar_id"] == CALENDAR_ID
        assert registration["mechanism_id"] == pointer["mechanism_id"] == MECHANISM_ID
        assert pointer["mechanism_registered"] is False
    else:
        assert registration["calendar_id"] == CALENDAR_ID
        assert registration["mechanism_id"] == MECHANISM_ID
    assert registration["mechanism_registered"] is False
    assert event["mechanism_registration_performed"] is False
    assert registration["calendar_correction_count"] == 2
    assert registration["source_observability_record_count"] == 6


def test_registration_binds_immutable_snapshot_not_mutable_active_pointer():
    registration = _v1_documents_or_preserved_records()["registration"]
    bindings = registration["bindings"]
    assert PREDECESSOR_POINTER_SNAPSHOT_PATH.as_posix() in bindings
    assert bindings[PREDECESSOR_POINTER_SNAPSHOT_PATH.as_posix()] == (
        ACTIVE_POINTER_SHA256
    )
    assert ACTIVE_POINTER_PATH.as_posix() not in bindings


def test_pointer_activation_rolls_back_exact_bytes_on_failed_postcheck(tmp_path: Path):
    pointer = tmp_path / "pointer.json"
    old = canonical_bytes({"state": "OLD"}) + b"\n"
    new = canonical_bytes({"state": "NEW"}) + b"\n"
    pointer.write_bytes(old)

    def fail() -> None:
        raise RuntimeError("synthetic postcheck failure")

    with pytest.raises(RuntimeError, match="synthetic postcheck failure"):
        _activate_pointer_with_rollback(
            pointer_path=pointer,
            new_bytes=new,
            postcheck=fail,
        )
    assert pointer.read_bytes() == old


def test_live_lifecycle_is_transition_stable_before_or_after_activation():
    pointer_path = ROOT / ACTIVE_POINTER_PATH
    digest = sha256_file(pointer_path)
    if digest == ACTIVE_POINTER_SHA256:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        assert pointer["calendar_id"] == PREDECESSOR_CALENDAR_ID
        assert sha256_file(ROOT / PUBLISHED_CALENDAR_PATH) == CALENDAR_SHA256
        assert sha256_file(ROOT / PREDECESSOR_POINTER_SNAPSHOT_PATH) == (
            ACTIVE_POINTER_SHA256
        )
    else:
        from futures_rebuild.alpha_ladder_calendar_observability_publication_v2 import (
            load_active_successor as load_active_successor_v2,
        )

        calendar = load_active_successor_v2(root=ROOT)
        assert calendar["calendar_id"] == CALENDAR_ID
        assert sha256_file(ROOT / PUBLISHED_CALENDAR_PATH) == CALENDAR_SHA256
