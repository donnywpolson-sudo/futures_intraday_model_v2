from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.alpha_ladder_calendar_observability_publication_v2 import (
    ACTIVE_POINTER_PATH,
    ACTIVE_POINTER_SHA256,
    CALENDAR_ID,
    CALENDAR_SHA256,
    FAILURE_PATH,
    MECHANISM_ID,
    PREDECESSOR_POINTER_SNAPSHOT_PATH,
    PUBLISHED_CALENDAR_PATH,
    REGISTRATION_PATH,
    V1_EVENT_PATH,
    V1_EVENT_SHA256,
    V1_REGISTRATION_PATH,
    V1_REGISTRATION_SHA256,
    _validate_successor_transition_stable,
    load_active_successor,
    publication_documents,
)
from futures_rebuild.canonical import sha256_file, sha256_json


ROOT = Path(__file__).resolve().parents[1]


def test_v1_failure_is_preserved_and_v2_documents_are_deterministic():
    assert sha256_file(ROOT / V1_REGISTRATION_PATH) == V1_REGISTRATION_SHA256
    assert sha256_file(ROOT / V1_EVENT_PATH) == V1_EVENT_SHA256
    first = publication_documents(root=ROOT)
    second = publication_documents(root=ROOT)
    assert first == second
    failure = first["failure"]
    registration = first["registration"]
    event = first["event"]
    pointer = first["pointer"]
    assert failure["failure_id"] == sha256_json(
        {key: value for key, value in failure.items() if key != "failure_id"}
    )
    assert failure["predecessor_pointer_restored"] is True
    assert failure["successor_bytes_changed"] is False
    assert registration["registration_id"] == sha256_json(
        {key: value for key, value in registration.items() if key != "registration_id"}
    )
    assert event["event_id"] == sha256_json(
        {key: value for key, value in event.items() if key != "event_id"}
    )
    assert pointer["pointer_id"] == sha256_json(
        {key: value for key, value in pointer.items() if key != "pointer_id"}
    )
    assert pointer["calendar_id"] == CALENDAR_ID
    assert pointer["mechanism_id"] == MECHANISM_ID
    assert pointer["mechanism_registered"] is False


def test_transition_validation_uses_exact_immutable_pointer_snapshot():
    successor = json.loads(
        (ROOT / PUBLISHED_CALENDAR_PATH).read_text(encoding="utf-8")
    )
    _validate_successor_transition_stable(successor, root=ROOT)
    assert sha256_file(ROOT / PREDECESSOR_POINTER_SNAPSHOT_PATH) == (
        ACTIVE_POINTER_SHA256
    )
    assert successor["bindings"][ACTIVE_POINTER_PATH.as_posix()] == (
        ACTIVE_POINTER_SHA256
    )


def test_v2_lifecycle_is_transition_stable_before_or_after_activation():
    documents = publication_documents(root=ROOT)
    current = json.loads((ROOT / ACTIVE_POINTER_PATH).read_text(encoding="utf-8"))
    if current.get("calendar_id") != CALENDAR_ID:
        assert sha256_file(ROOT / ACTIVE_POINTER_PATH) == ACTIVE_POINTER_SHA256
        assert not (ROOT / FAILURE_PATH).exists()
        assert not (ROOT / REGISTRATION_PATH).exists()
    else:
        calendar = load_active_successor(root=ROOT)
        assert calendar["calendar_id"] == CALENDAR_ID
        assert sha256_file(ROOT / PUBLISHED_CALENDAR_PATH) == CALENDAR_SHA256
        assert current == documents["pointer"]
