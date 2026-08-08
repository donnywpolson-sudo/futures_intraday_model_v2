"""Create-only publication for the Alpha calendar-observability successor."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path

from .alpha_ladder_calendar_observability_successor import (
    ACTIVE_POINTER_PATH,
    ACTIVE_POINTER_SHA256,
    MECHANISM_ID,
    MECHANISM_PATH,
    MECHANISM_SHA256,
    PREDECESSOR_CALENDAR_ID,
    PREDECESSOR_CALENDAR_PATH,
    PREDECESSOR_CALENDAR_SHA256,
    PROVENANCE_REPORT_ID,
    PROVENANCE_REPORT_PATH,
    PROVENANCE_REPORT_SHA256,
    validate_successor,
)
from .canonical import (
    canonical_bytes,
    fsync_directory,
    sha256_bytes,
    sha256_file,
    sha256_json,
)
from .cash_open_calendar_publication import _activate_pointer_with_rollback
from .errors import IntegrityError


CALENDAR_ID = "ddbe0c706d6568d8d7ddefd830677d73978b428d8a99925290310224f673a7f9"
CALENDAR_SHA256 = "efdf4f765e44ac2f312dce62b7145bb1ed70d01fd8c76fb5bfb3f32652f1a632"
PREPARED_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_calendar_observability_successor/"
    f"{CALENDAR_ID}/historical_calendar_successor.json"
)
REGISTRY_DIR = Path("state/calendar_registry/cash_open_impulse_41_market") / CALENDAR_ID
PUBLISHED_CALENDAR_PATH = REGISTRY_DIR / "historical_calendar_successor.json"
PREDECESSOR_POINTER_SNAPSHOT_PATH = REGISTRY_DIR / "predecessor_active_pointer.json"
REGISTRATION_PATH = REGISTRY_DIR / "registration.json"
EVENT_DIR = Path("state/calendar_events/cash_open_impulse_41_market")
MODULE_PATH = Path(
    "src/futures_rebuild/alpha_ladder_calendar_observability_publication.py"
)
PUBLISH_SCRIPT_PATH = Path(
    "scripts/publish_alpha_ladder_calendar_observability_successor.py"
)


def _read_canonical(path: Path, *, name: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{name} is unreadable") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{name} is not canonical JSON")
    return payload


def _validate_predecessor_pointer(pointer: Mapping[str, object]) -> None:
    core = {key: value for key, value in pointer.items() if key != "pointer_id"}
    if (
        pointer.get("pointer_id") != sha256_json(core)
        or pointer.get("calendar_id") != PREDECESSOR_CALENDAR_ID
        or pointer.get("calendar_path") != PREDECESSOR_CALENDAR_PATH.as_posix()
        or pointer.get("calendar_sha256") != PREDECESSOR_CALENDAR_SHA256
    ):
        raise IntegrityError("predecessor active calendar pointer is invalid")


def _predecessor_pointer(*, root: Path) -> dict[str, object]:
    """Load immutable transition evidence before or after activation."""

    snapshot = root / PREDECESSOR_POINTER_SNAPSHOT_PATH
    if snapshot.exists():
        if sha256_file(snapshot) != ACTIVE_POINTER_SHA256:
            raise IntegrityError("predecessor pointer snapshot hash drifted")
        pointer = _read_canonical(snapshot, name="predecessor pointer snapshot")
    else:
        active = root / ACTIVE_POINTER_PATH
        if sha256_file(active) != ACTIVE_POINTER_SHA256:
            raise IntegrityError("active predecessor pointer hash drifted")
        pointer = _read_canonical(active, name="active predecessor pointer")
    _validate_predecessor_pointer(pointer)
    return pointer


def _prepared_successor(*, root: Path) -> dict[str, object]:
    path = root / PREPARED_PATH
    if sha256_file(path) != CALENDAR_SHA256:
        raise IntegrityError("prepared calendar-observability successor hash drifted")
    successor = _read_canonical(path, name="prepared calendar-observability successor")
    if successor.get("calendar_id") != CALENDAR_ID:
        raise IntegrityError("prepared calendar-observability identity drifted")
    validate_successor(successor, root=root)
    return successor


def publication_documents(*, root: Path) -> dict[str, dict[str, object]]:
    successor = _prepared_successor(root=root)
    _predecessor_pointer(root=root)
    immutable_bindings = dict(
        sorted(
            {
                PREPARED_PATH.as_posix(): CALENDAR_SHA256,
                PREDECESSOR_CALENDAR_PATH.as_posix(): PREDECESSOR_CALENDAR_SHA256,
                PREDECESSOR_POINTER_SNAPSHOT_PATH.as_posix(): ACTIVE_POINTER_SHA256,
                PROVENANCE_REPORT_PATH.as_posix(): PROVENANCE_REPORT_SHA256,
                MECHANISM_PATH.as_posix(): MECHANISM_SHA256,
                MODULE_PATH.as_posix(): sha256_file(root / MODULE_PATH),
                PUBLISH_SCRIPT_PATH.as_posix(): sha256_file(root / PUBLISH_SCRIPT_PATH),
            }.items()
        )
    )
    registration_core: dict[str, object] = {
        "schema_version": (
            "cash_open_impulse_calendar_observability_registration/1.0.0"
        ),
        "calendar_id": CALENDAR_ID,
        "calendar_path": PUBLISHED_CALENDAR_PATH.as_posix(),
        "calendar_sha256": CALENDAR_SHA256,
        "preparation_path": PREPARED_PATH.as_posix(),
        "preparation_sha256": CALENDAR_SHA256,
        "predecessor_calendar_id": PREDECESSOR_CALENDAR_ID,
        "predecessor_calendar_sha256": PREDECESSOR_CALENDAR_SHA256,
        "predecessor_pointer_snapshot_path": (
            PREDECESSOR_POINTER_SNAPSHOT_PATH.as_posix()
        ),
        "predecessor_pointer_sha256": ACTIVE_POINTER_SHA256,
        "provenance_report_id": PROVENANCE_REPORT_ID,
        "calendar_correction_count": successor["calendar_correction_count"],
        "source_observability_record_count": successor[
            "source_observability_record_count"
        ],
        "publication_mode": "CREATE_ONLY_EXACT_BYTE_COPY_POINTER_LAST",
        "mechanism_id": MECHANISM_ID,
        "mechanism_registered": False,
        "bindings": immutable_bindings,
        "authority": {
            "external_cost_usd": "0",
            "historical_rows_read": False,
            "mechanism_registration": False,
            "performance_evaluation": False,
            "provider_network_credentials_accessed": False,
            "year_2025_accessed": False,
        },
    }
    registration = {
        **registration_core,
        "registration_id": sha256_json(registration_core),
    }
    registration_sha = sha256_bytes(canonical_bytes(registration) + b"\n")
    event_core: dict[str, object] = {
        "schema_version": "cash_open_impulse_calendar_observability_event/1.0.0",
        "event_type": "PUBLISHED_AND_ACTIVATED_CALENDAR_OBSERVABILITY_SUCCESSOR",
        "calendar_id": CALENDAR_ID,
        "predecessor_calendar_id": PREDECESSOR_CALENDAR_ID,
        "registration_id": registration["registration_id"],
        "calendar_correction_count": 2,
        "source_observability_record_count": 6,
        "mechanism_id": MECHANISM_ID,
        "mechanism_registration_performed": False,
    }
    event = {**event_core, "event_id": sha256_json(event_core)}
    event_path = EVENT_DIR / f"{event['event_id']}.json"
    event_sha = sha256_bytes(canonical_bytes(event) + b"\n")
    pointer_core: dict[str, object] = {
        "schema_version": "active_cash_open_impulse_historical_calendar/3.0.0",
        "state": "ACTIVE_CALENDAR_AND_SOURCE_OBSERVABILITY",
        "calendar_id": CALENDAR_ID,
        "calendar_path": PUBLISHED_CALENDAR_PATH.as_posix(),
        "calendar_sha256": CALENDAR_SHA256,
        "calendar_correction_count": 2,
        "source_observability_record_count": 6,
        "registration_id": registration["registration_id"],
        "registration_path": REGISTRATION_PATH.as_posix(),
        "registration_sha256": registration_sha,
        "event_id": event["event_id"],
        "event_path": event_path.as_posix(),
        "event_sha256": event_sha,
        "predecessor": {
            "calendar_id": PREDECESSOR_CALENDAR_ID,
            "pointer_snapshot_path": PREDECESSOR_POINTER_SNAPSHOT_PATH.as_posix(),
            "pointer_sha256": ACTIVE_POINTER_SHA256,
        },
        "mechanism_id": MECHANISM_ID,
        "mechanism_registered": False,
    }
    pointer = {**pointer_core, "pointer_id": sha256_json(pointer_core)}
    return {
        "registration": registration,
        "event": event,
        "pointer": pointer,
    }


def _write_new_or_exact(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        )
    except FileExistsError:
        if path.read_bytes() != raw:
            raise IntegrityError(f"existing publication artifact differs: {path}")
        return
    try:
        os.write(descriptor, raw)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(path.parent)


def load_active_successor(*, root: Path) -> dict[str, object]:
    pointer = _read_canonical(
        root / ACTIVE_POINTER_PATH,
        name="active calendar-observability pointer",
    )
    pointer_core = {
        key: value for key, value in pointer.items() if key != "pointer_id"
    }
    if (
        pointer.get("pointer_id") != sha256_json(pointer_core)
        or pointer.get("calendar_id") != CALENDAR_ID
        or pointer.get("state") != "ACTIVE_CALENDAR_AND_SOURCE_OBSERVABILITY"
        or pointer.get("calendar_correction_count") != 2
        or pointer.get("source_observability_record_count") != 6
        or pointer.get("mechanism_id") != MECHANISM_ID
        or pointer.get("mechanism_registered") is not False
    ):
        raise IntegrityError("active calendar-observability pointer is invalid")
    expected_paths = (
        ("calendar_path", "calendar_sha256", "published calendar"),
        ("registration_path", "registration_sha256", "calendar registration"),
        ("event_path", "event_sha256", "activation event"),
    )
    for path_field, hash_field, name in expected_paths:
        path = root / str(pointer.get(path_field))
        if sha256_file(path) != pointer.get(hash_field):
            raise IntegrityError(f"{name} hash drifted")
    if (
        sha256_file(root / PREDECESSOR_POINTER_SNAPSHOT_PATH)
        != ACTIVE_POINTER_SHA256
    ):
        raise IntegrityError("predecessor pointer snapshot drifted")
    registration = _read_canonical(
        root / REGISTRATION_PATH, name="calendar-observability registration"
    )
    event = _read_canonical(
        root / str(pointer["event_path"]), name="calendar-observability event"
    )
    registration_core = {
        key: value for key, value in registration.items() if key != "registration_id"
    }
    event_core = {key: value for key, value in event.items() if key != "event_id"}
    if (
        registration.get("registration_id") != sha256_json(registration_core)
        or registration.get("registration_id") != pointer.get("registration_id")
        or registration.get("mechanism_registered") is not False
        or registration.get("calendar_correction_count") != 2
        or registration.get("source_observability_record_count") != 6
        or event.get("event_id") != sha256_json(event_core)
        or event.get("event_id") != pointer.get("event_id")
        or event.get("registration_id") != registration.get("registration_id")
        or event.get("mechanism_registration_performed") is not False
    ):
        raise IntegrityError("calendar-observability lifecycle identity is invalid")
    bindings = registration.get("bindings")
    if not isinstance(bindings, Mapping):
        raise IntegrityError("calendar-observability registration bindings are absent")
    for relative, digest in bindings.items():
        if sha256_file(root / str(relative)) != digest:
            raise IntegrityError(
                f"calendar-observability registration binding drifted: {relative}"
            )
    calendar = _read_canonical(
        root / PUBLISHED_CALENDAR_PATH,
        name="published calendar-observability successor",
    )
    validate_successor(calendar, root=root)
    if (
        calendar.get("calendar_id") != CALENDAR_ID
        or (root / PUBLISHED_CALENDAR_PATH).read_bytes()
        != (root / PREPARED_PATH).read_bytes()
        or sha256_file(root / MECHANISM_PATH) != MECHANISM_SHA256
    ):
        raise IntegrityError("published successor or mechanism preservation drifted")
    _predecessor_pointer(root=root)
    return calendar


def persist_publication(*, root: Path) -> dict[str, str]:
    """Publish immutable records, activate last, and roll back on postcheck failure."""

    documents = publication_documents(root=root)
    event_path = EVENT_DIR / f"{documents['event']['event_id']}.json"
    pointer_bytes = canonical_bytes(documents["pointer"]) + b"\n"
    current_bytes = (root / ACTIVE_POINTER_PATH).read_bytes()
    if current_bytes == pointer_bytes:
        load_active_successor(root=root)
    else:
        if sha256_bytes(current_bytes) != ACTIVE_POINTER_SHA256:
            raise IntegrityError("active calendar pointer is neither predecessor nor successor")
        payloads = (
            (root / PREDECESSOR_POINTER_SNAPSHOT_PATH, current_bytes),
            (root / PUBLISHED_CALENDAR_PATH, (root / PREPARED_PATH).read_bytes()),
            (
                root / REGISTRATION_PATH,
                canonical_bytes(documents["registration"]) + b"\n",
            ),
            (root / event_path, canonical_bytes(documents["event"]) + b"\n"),
        )
        for path, raw in payloads:
            _write_new_or_exact(path, raw)
        _activate_pointer_with_rollback(
            pointer_path=root / ACTIVE_POINTER_PATH,
            new_bytes=pointer_bytes,
            postcheck=lambda: load_active_successor(root=root),
        )
    return {
        "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
        "calendar_id": CALENDAR_ID,
        "calendar_sha256": CALENDAR_SHA256,
        "event_id": str(documents["event"]["event_id"]),
        "pointer_id": str(documents["pointer"]["pointer_id"]),
        "registration_id": str(documents["registration"]["registration_id"]),
        "mechanism_registered": "false",
    }
