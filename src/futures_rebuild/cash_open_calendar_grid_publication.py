"""Create-only publication lifecycle for the prepared four-checkpoint calendar."""

from __future__ import annotations

import json
import os
from pathlib import Path

from .canonical import canonical_bytes, sha256_bytes, sha256_file, sha256_json
from .cash_open_calendar_publication import _activate_pointer_with_rollback
from .errors import IntegrityError


CALENDAR_ID = "cd64f912cceec3ff613b0d28f3965804c25d36d9b940d622b062128cfca0843b"
CALENDAR_SHA256 = "e76ec4310da674e1bbacf5356662d97d8a2c8b115c728fa9386b53f8d289be52"
PREPARED_PATH = Path(
    "state/unpublished_evidence/cash_open_impulse_41_market_calendar_grid_successor/"
    f"{CALENDAR_ID}/historical_calendar_successor.json"
)
REGISTRY_DIR = Path("state/calendar_registry/cash_open_impulse_41_market") / CALENDAR_ID
PUBLISHED_CALENDAR_PATH = REGISTRY_DIR / "historical_calendar_successor.json"
REGISTRATION_PATH = REGISTRY_DIR / "registration.json"
EVENT_DIR = Path("state/calendar_events/cash_open_impulse_41_market")
ACTIVE_POINTER_PATH = Path("configs/active_cash_open_impulse_historical_calendar.json")
PREDECESSOR_POINTER_SHA256 = "6f534035bd3707d0a1c5937af5d338947509ee105b2c2656570f1dd06ff84132"
PREDECESSOR_CALENDAR_ID = "54bc5550a0ba28af2a509fb32c756b39041686ba10ffa6bd832e6d96469c0397"


def _read_canonical(path: Path, *, name: str) -> dict[str, object]:
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"{name} is invalid") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{name} is not canonical")
    return payload


def _validate_calendar(path: Path) -> dict[str, object]:
    if sha256_file(path) != CALENDAR_SHA256:
        raise IntegrityError("four-checkpoint calendar hash drifted")
    payload = _read_canonical(path, name="four-checkpoint calendar")
    core = {key: value for key, value in payload.items() if key != "calendar_id"}
    if payload.get("calendar_id") != CALENDAR_ID or sha256_json(core) != CALENDAR_ID:
        raise IntegrityError("four-checkpoint calendar identity is invalid")
    if (
        payload.get("decision") != "PASS_EXACT_REFERENCE_COVERAGE"
        or payload.get("checkpoint_grid") != ["09:00", "09:30", "10:00", "10:30"]
        or payload.get("unresolved_reference_count") != 0
        or len(payload.get("calendar_rows", [])) != 74_866
    ):
        raise IntegrityError("four-checkpoint calendar coverage is invalid")
    bindings = payload.get("bindings")
    if not isinstance(bindings, dict):
        raise IntegrityError("four-checkpoint calendar bindings are invalid")
    root = path.resolve().parents[4]
    for relative, expected in bindings.items():
        if sha256_file(root / str(relative)) != expected:
            raise IntegrityError(f"four-checkpoint calendar binding drifted: {relative}")
    return payload


def publication_documents(*, root: Path) -> dict[str, dict[str, object]]:
    calendar = _validate_calendar(root / PREPARED_PATH)
    if sha256_file(root / ACTIVE_POINTER_PATH) != PREDECESSOR_POINTER_SHA256:
        raise IntegrityError("cash-open calendar predecessor pointer drifted")
    predecessor = _read_canonical(root / ACTIVE_POINTER_PATH, name="predecessor pointer")
    if predecessor.get("calendar_id") != PREDECESSOR_CALENDAR_ID:
        raise IntegrityError("cash-open calendar predecessor identity drifted")
    registration_core: dict[str, object] = {
        "schema_version": "cash_open_impulse_historical_calendar_registration/2.0.0",
        "calendar_id": CALENDAR_ID,
        "calendar_path": PUBLISHED_CALENDAR_PATH.as_posix(),
        "calendar_sha256": CALENDAR_SHA256,
        "decision": calendar["decision"],
        "predecessor_calendar_id": PREDECESSOR_CALENDAR_ID,
        "predecessor_pointer_sha256": PREDECESSOR_POINTER_SHA256,
        "preparation_path": PREPARED_PATH.as_posix(),
        "preparation_sha256": CALENDAR_SHA256,
        "publication_mode": "CREATE_ONLY_EXACT_BYTE_COPY",
        "authority": {
            "external_cost_usd": "0",
            "price_rows_read": False,
            "provider_network_credentials_accessed": False,
            "research_parameters_changed": False,
            "year_2025_accessed": False,
        },
    }
    registration = {**registration_core, "registration_id": sha256_json(registration_core)}
    registration_sha = sha256_bytes(canonical_bytes(registration) + b"\n")
    event_core: dict[str, object] = {
        "schema_version": "cash_open_impulse_historical_calendar_event/2.0.0",
        "event_type": "PUBLISHED_AND_ACTIVATED_GRID_SUCCESSOR",
        "calendar_id": CALENDAR_ID,
        "predecessor_calendar_id": PREDECESSOR_CALENDAR_ID,
        "registration_id": registration["registration_id"],
    }
    event = {**event_core, "event_id": sha256_json(event_core)}
    event_path = EVENT_DIR / f"{event['event_id']}.json"
    event_sha = sha256_bytes(canonical_bytes(event) + b"\n")
    pointer_core: dict[str, object] = {
        "schema_version": "active_cash_open_impulse_historical_calendar/2.0.0",
        "state": "ACTIVE_REFERENCE_ONLY",
        "calendar_id": CALENDAR_ID,
        "calendar_path": PUBLISHED_CALENDAR_PATH.as_posix(),
        "calendar_sha256": CALENDAR_SHA256,
        "registration_id": registration["registration_id"],
        "registration_path": REGISTRATION_PATH.as_posix(),
        "registration_sha256": registration_sha,
        "event_id": event["event_id"],
        "event_path": event_path.as_posix(),
        "event_sha256": event_sha,
        "predecessor": {
            "calendar_id": PREDECESSOR_CALENDAR_ID,
            "pointer_sha256": PREDECESSOR_POINTER_SHA256,
        },
    }
    pointer = {**pointer_core, "pointer_id": sha256_json(pointer_core)}
    return {"registration": registration, "event": event, "pointer": pointer}


def load_active_grid_calendar(*, root: Path, pointer_path: Path | None = None) -> dict[str, object]:
    selected = pointer_path or root / ACTIVE_POINTER_PATH
    pointer = _read_canonical(selected, name="active grid calendar pointer")
    core = {key: value for key, value in pointer.items() if key != "pointer_id"}
    if pointer.get("pointer_id") != sha256_json(core) or pointer.get("calendar_id") != CALENDAR_ID:
        raise IntegrityError("active grid calendar pointer identity is invalid")
    for path_key, hash_key, name in (
        ("calendar_path", "calendar_sha256", "calendar"),
        ("registration_path", "registration_sha256", "registration"),
        ("event_path", "event_sha256", "event"),
    ):
        if sha256_file(root / str(pointer[path_key])) != pointer[hash_key]:
            raise IntegrityError(f"active grid calendar {name} hash drifted")
    registration = _read_canonical(root / str(pointer["registration_path"]), name="grid registration")
    event = _read_canonical(root / str(pointer["event_path"]), name="grid event")
    if registration.get("registration_id") != pointer.get("registration_id"):
        raise IntegrityError("grid registration identity differs from pointer")
    if event.get("event_id") != pointer.get("event_id"):
        raise IntegrityError("grid event identity differs from pointer")
    return _validate_calendar(root / str(pointer["calendar_path"]))


def persist_publication(*, root: Path) -> dict[str, str]:
    documents = publication_documents(root=root)
    event_path = EVENT_DIR / f"{documents['event']['event_id']}.json"
    destinations = (root / PUBLISHED_CALENDAR_PATH, root / REGISTRATION_PATH, root / event_path)
    if any(path.exists() for path in destinations):
        raise IntegrityError("grid calendar publication destination already exists")
    for path in destinations:
        path.parent.mkdir(parents=True, exist_ok=True)
    payloads = (
        (root / PUBLISHED_CALENDAR_PATH, (root / PREPARED_PATH).read_bytes()),
        (root / REGISTRATION_PATH, canonical_bytes(documents["registration"]) + b"\n"),
        (root / event_path, canonical_bytes(documents["event"]) + b"\n"),
    )
    for path, raw in payloads:
        with path.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    _activate_pointer_with_rollback(
        pointer_path=root / ACTIVE_POINTER_PATH,
        new_bytes=canonical_bytes(documents["pointer"]) + b"\n",
        postcheck=lambda: load_active_grid_calendar(root=root),
    )
    return {
        "calendar_id": CALENDAR_ID,
        "registration_id": str(documents["registration"]["registration_id"]),
        "event_id": str(documents["event"]["event_id"]),
        "pointer_id": str(documents["pointer"]["pointer_id"]),
    }


def activate_existing_publication(*, root: Path) -> dict[str, str]:
    """Activate exact existing records after a rolled-back postcheck."""

    documents = publication_documents(root=root)
    event_path = EVENT_DIR / f"{documents['event']['event_id']}.json"
    expected = (
        (root / PUBLISHED_CALENDAR_PATH, (root / PREPARED_PATH).read_bytes()),
        (root / REGISTRATION_PATH, canonical_bytes(documents["registration"]) + b"\n"),
        (root / event_path, canonical_bytes(documents["event"]) + b"\n"),
    )
    for path, raw in expected:
        if not path.exists() or path.read_bytes() != raw:
            raise IntegrityError(f"published grid calendar artifact drifted: {path}")
    _activate_pointer_with_rollback(
        pointer_path=root / ACTIVE_POINTER_PATH,
        new_bytes=canonical_bytes(documents["pointer"]) + b"\n",
        postcheck=lambda: load_active_grid_calendar(root=root),
    )
    return {
        "calendar_id": CALENDAR_ID,
        "registration_id": str(documents["registration"]["registration_id"]),
        "event_id": str(documents["event"]["event_id"]),
        "pointer_id": str(documents["pointer"]["pointer_id"]),
    }
