"""Publish and activate the certified cash-open historical calendar.

This uses a mechanism-specific pointer.  The general exchange-calendar pointer
is a forward-looking authority with an incompatible schema and must remain
unchanged.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Callable

from .canonical import canonical_bytes, fsync_directory, sha256_bytes, sha256_file, sha256_json
from .errors import IntegrityError


CALENDAR_ID = "54bc5550a0ba28af2a509fb32c756b39041686ba10ffa6bd832e6d96469c0397"
CALENDAR_SHA256 = "7860a57f7b64288be333d82cfc7e0f1b889c06304f9cedbb3a8abb3caff795ec"
PREPARED_PATH = Path(
    "state/unpublished_evidence/cash_open_impulse_41_market_calendar_successor_preparation/"
    f"{CALENDAR_ID}/historical_calendar_successor.json"
)
REGISTRY_DIR = Path("state/calendar_registry/cash_open_impulse_41_market") / CALENDAR_ID
PUBLISHED_CALENDAR_PATH = REGISTRY_DIR / "historical_calendar_successor.json"
REGISTRATION_PATH = REGISTRY_DIR / "registration.json"
EVENT_DIR = Path("state/calendar_events/cash_open_impulse_41_market")
ACTIVE_POINTER_PATH = Path("configs/active_cash_open_impulse_historical_calendar.json")
GENERAL_POINTER_PATH = Path("configs/active_exchange_calendar.json")
GENERAL_POINTER_SHA256 = "a6fc445a116023820ab009d0a87f85682cbf35873230ce8bf75ecdf764b5b9ca"
FALLBACK_POINTER_PATH = Path("configs/tier1_historical_checkpoint_calendar_v5.json")
FALLBACK_POINTER_SHA256 = "a328798dca5175456f824e6c0df74398d604f9d57ee43969bdfe82ff12c2fdd8"


def _read_canonical(path: Path, *, name: str) -> dict[str, object]:
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"{name} is not canonical JSON") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{name} is not canonical JSON")
    return payload


def _validate_calendar(path: Path, *, expected_sha256: str = CALENDAR_SHA256) -> dict[str, object]:
    if sha256_file(path) != expected_sha256:
        raise IntegrityError("cash-open historical calendar hash drifted")
    payload = _read_canonical(path, name="cash-open historical calendar")
    core = {key: value for key, value in payload.items() if key != "calendar_id"}
    if payload.get("calendar_id") != CALENDAR_ID or sha256_json(core) != CALENDAR_ID:
        raise IntegrityError("cash-open historical calendar identity is invalid")
    if payload.get("decision") != "PASS_EXACT_REFERENCE_COVERAGE":
        raise IntegrityError("cash-open historical calendar did not pass exact coverage")
    if payload.get("status") != "PREPARED_INACTIVE_UNPUBLISHED":
        raise IntegrityError("cash-open historical calendar preparation status drifted")
    if payload.get("unresolved_reference_count") != 0 or payload.get("unresolved_reference_states") != []:
        raise IntegrityError("cash-open historical calendar has unresolved reference states")
    rows = payload.get("calendar_rows")
    if not isinstance(rows, list) or len(rows) != 74_866:
        raise IntegrityError("cash-open historical calendar row coverage is incomplete")
    if {str(row.get("trade_date", ""))[:4] for row in rows if isinstance(row, dict)} != {
        "2018", "2019", "2020", "2021", "2022"
    }:
        raise IntegrityError("cash-open historical calendar year coverage is invalid")
    authority = payload.get("authority")
    if authority != {
        "active": False,
        "price_rows_read": False,
        "provider_network_credentials_accessed": False,
        "published": False,
        "year_2025_accessed": False,
    }:
        raise IntegrityError("cash-open historical calendar preparation authority drifted")
    bindings = payload.get("bindings")
    if not isinstance(bindings, dict):
        raise IntegrityError("cash-open historical calendar bindings are invalid")
    # Both the preparation and registry layouts are four directories beneath
    # the repository root: state/<class>/<mechanism>/<calendar-id>/<file>.
    root = path.resolve().parents[4]
    for relative, expected in bindings.items():
        if not isinstance(relative, str) or not isinstance(expected, str):
            raise IntegrityError("cash-open historical calendar binding is invalid")
        if sha256_file(root / relative) != expected:
            raise IntegrityError(f"cash-open historical calendar binding drifted: {relative}")
    return payload


def publication_documents(*, root: Path) -> dict[str, dict[str, object]]:
    """Build deterministic registration, event, and pointer documents."""

    prepared = _validate_calendar(root / PREPARED_PATH)
    if sha256_file(root / GENERAL_POINTER_PATH) != GENERAL_POINTER_SHA256:
        raise IntegrityError("general exchange-calendar pointer drifted")
    if sha256_file(root / FALLBACK_POINTER_PATH) != FALLBACK_POINTER_SHA256:
        raise IntegrityError("fallback historical-calendar pointer drifted")
    registration_core: dict[str, object] = {
        "authority": {
            "active_data_mutation": "DEDICATED_POINTER_ONLY",
            "external_cost_usd": "0",
            "price_rows_read": False,
            "provider_network_credentials_accessed": False,
            "research_parameters_changed": False,
            "year_2025_accessed": False,
        },
        "calendar_id": CALENDAR_ID,
        "calendar_path": PUBLISHED_CALENDAR_PATH.as_posix(),
        "calendar_sha256": CALENDAR_SHA256,
        "decision": prepared["decision"],
        "preparation_path": PREPARED_PATH.as_posix(),
        "preparation_sha256": CALENDAR_SHA256,
        "publication_mode": "CREATE_ONLY_EXACT_BYTE_COPY",
        "schema_version": "cash_open_impulse_historical_calendar_registration/1.0.0",
    }
    registration = {**registration_core, "registration_id": sha256_json(registration_core)}
    registration_sha = sha256_bytes(canonical_bytes(registration) + b"\n")
    event_core: dict[str, object] = {
        "calendar_id": CALENDAR_ID,
        "event_type": "PUBLISHED_AND_ACTIVATED",
        "registration_id": registration["registration_id"],
        "schema_version": "cash_open_impulse_historical_calendar_event/1.0.0",
    }
    event = {**event_core, "event_id": sha256_json(event_core)}
    event_path = EVENT_DIR / f"{event['event_id']}.json"
    event_sha = sha256_bytes(canonical_bytes(event) + b"\n")
    pointer_core: dict[str, object] = {
        "calendar_id": CALENDAR_ID,
        "calendar_path": PUBLISHED_CALENDAR_PATH.as_posix(),
        "calendar_sha256": CALENDAR_SHA256,
        "event_id": event["event_id"],
        "event_path": event_path.as_posix(),
        "event_sha256": event_sha,
        "fallback_historical_calendar_pointer": {
            "path": FALLBACK_POINTER_PATH.as_posix(),
            "sha256": FALLBACK_POINTER_SHA256,
        },
        "general_exchange_calendar_pointer": {
            "path": GENERAL_POINTER_PATH.as_posix(),
            "sha256": GENERAL_POINTER_SHA256,
        },
        "registration_id": registration["registration_id"],
        "registration_path": REGISTRATION_PATH.as_posix(),
        "registration_sha256": registration_sha,
        "schema_version": "active_cash_open_impulse_historical_calendar/1.0.0",
        "state": "ACTIVE_REFERENCE_ONLY",
    }
    pointer = {**pointer_core, "pointer_id": sha256_json(pointer_core)}
    return {"registration": registration, "event": event, "pointer": pointer}


def load_active_calendar(*, root: Path, pointer_path: Path | None = None) -> dict[str, object]:
    """Load the active mechanism-specific calendar through its complete closure."""

    selected = pointer_path or root / ACTIVE_POINTER_PATH
    pointer = _read_canonical(selected, name="active cash-open historical-calendar pointer")
    pointer_core = {key: value for key, value in pointer.items() if key != "pointer_id"}
    if pointer.get("pointer_id") != sha256_json(pointer_core):
        raise IntegrityError("active cash-open historical-calendar pointer identity is invalid")
    if pointer.get("calendar_id") != CALENDAR_ID or pointer.get("state") != "ACTIVE_REFERENCE_ONLY":
        raise IntegrityError("active cash-open historical-calendar pointer selects an invalid state")
    for binding_name in ("general_exchange_calendar_pointer", "fallback_historical_calendar_pointer"):
        binding = pointer.get(binding_name)
        if not isinstance(binding, dict) or sha256_file(root / str(binding.get("path"))) != binding.get("sha256"):
            raise IntegrityError(f"{binding_name} drifted")
    registration_path = root / str(pointer["registration_path"])
    event_path = root / str(pointer["event_path"])
    calendar_path = root / str(pointer["calendar_path"])
    if sha256_file(registration_path) != pointer.get("registration_sha256"):
        raise IntegrityError("calendar registration hash drifted")
    if sha256_file(event_path) != pointer.get("event_sha256"):
        raise IntegrityError("calendar activation event hash drifted")
    if sha256_file(calendar_path) != pointer.get("calendar_sha256"):
        raise IntegrityError("published calendar hash drifted")
    registration = _read_canonical(registration_path, name="calendar registration")
    event = _read_canonical(event_path, name="calendar activation event")
    registration_core = {key: value for key, value in registration.items() if key != "registration_id"}
    event_core = {key: value for key, value in event.items() if key != "event_id"}
    if registration.get("registration_id") != sha256_json(registration_core):
        raise IntegrityError("calendar registration identity is invalid")
    if event.get("event_id") != sha256_json(event_core):
        raise IntegrityError("calendar activation event identity is invalid")
    if registration.get("registration_id") != pointer.get("registration_id"):
        raise IntegrityError("calendar pointer registration identity differs")
    if event.get("event_id") != pointer.get("event_id"):
        raise IntegrityError("calendar pointer event identity differs")
    if event.get("registration_id") != registration.get("registration_id"):
        raise IntegrityError("calendar event registration identity differs")
    return _validate_calendar(calendar_path)


def _activate_pointer_with_rollback(
    *, pointer_path: Path, new_bytes: bytes, postcheck: Callable[[], object]
) -> None:
    """Activate last and restore the exact prior pointer state on failure."""

    old_bytes = pointer_path.read_bytes() if pointer_path.exists() else None
    descriptor, temporary_text = tempfile.mkstemp(
        prefix=f".{pointer_path.name}.", suffix=".new", dir=pointer_path.parent
    )
    temporary = Path(temporary_text)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(new_bytes)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, pointer_path)
        fsync_directory(pointer_path.parent)
        try:
            postcheck()
        except Exception:
            if old_bytes is None:
                pointer_path.unlink(missing_ok=False)
                fsync_directory(pointer_path.parent)
            else:
                rollback = pointer_path.with_suffix(pointer_path.suffix + ".rollback")
                with rollback.open("xb") as stream:
                    stream.write(old_bytes)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(rollback, pointer_path)
                fsync_directory(pointer_path.parent)
            raise
    finally:
        if temporary.exists():
            temporary.unlink()


def persist_publication(*, root: Path) -> dict[str, str]:
    """Create immutable publication records and activate the pointer last."""

    documents = publication_documents(root=root)
    if (root / ACTIVE_POINTER_PATH).exists():
        raise IntegrityError("dedicated active cash-open calendar pointer already exists")
    event_path = EVENT_DIR / f"{documents['event']['event_id']}.json"
    destinations = (root / PUBLISHED_CALENDAR_PATH, root / REGISTRATION_PATH, root / event_path)
    if any(path.exists() for path in destinations):
        raise IntegrityError("calendar publication destination already exists")
    for path in destinations:
        path.parent.mkdir(parents=True, exist_ok=True)
    source_bytes = (root / PREPARED_PATH).read_bytes()
    payloads = (
        (root / PUBLISHED_CALENDAR_PATH, source_bytes),
        (root / REGISTRATION_PATH, canonical_bytes(documents["registration"]) + b"\n"),
        (root / event_path, canonical_bytes(documents["event"]) + b"\n"),
    )
    for path, raw in payloads:
        with path.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        fsync_directory(path.parent)
    pointer_bytes = canonical_bytes(documents["pointer"]) + b"\n"
    _activate_pointer_with_rollback(
        pointer_path=root / ACTIVE_POINTER_PATH,
        new_bytes=pointer_bytes,
        postcheck=lambda: load_active_calendar(root=root),
    )
    return {
        "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
        "calendar_id": CALENDAR_ID,
        "event_id": str(documents["event"]["event_id"]),
        "pointer_id": str(documents["pointer"]["pointer_id"]),
        "registration_id": str(documents["registration"]["registration_id"]),
    }


def activate_existing_publication(*, root: Path) -> dict[str, str]:
    """Activate already-created immutable records after a rolled-back postcheck."""

    documents = publication_documents(root=root)
    if (root / ACTIVE_POINTER_PATH).exists():
        raise IntegrityError("dedicated active cash-open calendar pointer already exists")
    event_path = EVENT_DIR / f"{documents['event']['event_id']}.json"
    expected = (
        (root / PUBLISHED_CALENDAR_PATH, (root / PREPARED_PATH).read_bytes()),
        (root / REGISTRATION_PATH, canonical_bytes(documents["registration"]) + b"\n"),
        (root / event_path, canonical_bytes(documents["event"]) + b"\n"),
    )
    for path, raw in expected:
        if not path.exists() or path.read_bytes() != raw:
            raise IntegrityError(f"published calendar artifact drifted: {path}")
    _activate_pointer_with_rollback(
        pointer_path=root / ACTIVE_POINTER_PATH,
        new_bytes=canonical_bytes(documents["pointer"]) + b"\n",
        postcheck=lambda: load_active_calendar(root=root),
    )
    return {
        "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
        "calendar_id": CALENDAR_ID,
        "event_id": str(documents["event"]["event_id"]),
        "pointer_id": str(documents["pointer"]["pointer_id"]),
        "registration_id": str(documents["registration"]["registration_id"]),
    }
