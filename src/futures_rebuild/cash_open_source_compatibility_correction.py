"""Preserve the mutable-pointer-bound preparation and build its correction."""

from __future__ import annotations

import json
from pathlib import Path

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError


INVALID_CALENDAR_ID = "fa9dc5cd31282c27ddd958cf2baa5bb29ec05361dc68f2f7a5e8663e76846105"
INVALID_CALENDAR_SHA256 = "c4755773bd13f2a67618c1398c2c4416843af8800272101f869b2fa34db7413f"
INVALID_CALENDAR_PATH = Path(
    "state/unpublished_evidence/cash_open_impulse_41_market_calendar_grid_successor/"
    f"{INVALID_CALENDAR_ID}/historical_calendar_successor.json"
)
INVALID_SPEC_PATH = Path("configs/cash_open_41_market_source_compatibility_spec.json")
INVALID_SPEC_SHA256 = "93751a2f5efe3b6fd2047ad7fa7d31e1c8dc723305284bca6ca0f5e6c293c0b1"
CORRECTED_CALENDAR_ID = "cd64f912cceec3ff613b0d28f3965804c25d36d9b940d622b062128cfca0843b"
CORRECTED_CALENDAR_SHA256 = "e76ec4310da674e1bbacf5356662d97d8a2c8b115c728fa9386b53f8d289be52"
CORRECTED_CALENDAR_PATH = Path(
    "state/unpublished_evidence/cash_open_impulse_41_market_calendar_grid_successor/"
    f"{CORRECTED_CALENDAR_ID}/historical_calendar_successor.json"
)
CORRECTED_SPEC_PATH = Path("configs/cash_open_41_market_source_compatibility_spec_v2.json")


def _object(path: Path) -> dict[str, object]:
    raw = path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"preparation is not canonical: {path}")
    return payload


def build_correction(*, root: Path) -> tuple[dict[str, object], dict[str, object]]:
    for path, expected in (
        (INVALID_CALENDAR_PATH, INVALID_CALENDAR_SHA256),
        (INVALID_SPEC_PATH, INVALID_SPEC_SHA256),
        (CORRECTED_CALENDAR_PATH, CORRECTED_CALENDAR_SHA256),
    ):
        if sha256_file(root / path) != expected:
            raise IntegrityError(f"preparation hash drifted: {path}")
    invalid_calendar = _object(root / INVALID_CALENDAR_PATH)
    invalid_spec = _object(root / INVALID_SPEC_PATH)
    corrected_calendar = _object(root / CORRECTED_CALENDAR_PATH)
    if (
        invalid_calendar.get("calendar_id") != INVALID_CALENDAR_ID
        or invalid_spec.get("prepared_calendar", {}).get("calendar_id") != INVALID_CALENDAR_ID
        or corrected_calendar.get("calendar_id") != CORRECTED_CALENDAR_ID
    ):
        raise IntegrityError("calendar correction identity is invalid")
    invalid_core: dict[str, object] = {
        "schema_version": "cash_open_source_compatibility_preparation_invalidity/1.0.0",
        "classification": "INVALID_PRE_DATA_MUTABLE_POINTER_BINDING",
        "invalid_calendar_id": INVALID_CALENDAR_ID,
        "invalid_spec_id": invalid_spec["spec_id"],
        "reason": "ACTIVATION_WOULD_CHANGE_A_PATH_BOUND_INSIDE_THE_CALENDAR_DEPENDENCY_CLOSURE",
        "economic_result": "NOT_PRODUCED",
        "bindings": {
            INVALID_CALENDAR_PATH.as_posix(): INVALID_CALENDAR_SHA256,
            INVALID_SPEC_PATH.as_posix(): INVALID_SPEC_SHA256,
        },
        "authority": {
            "published": False,
            "historical_rows_read": False,
            "model_fit": False,
            "prediction_generation": False,
            "performance_evaluation": False,
            "year_2025_accessed": False,
        },
    }
    invalidity = {**invalid_core, "record_id": sha256_json(invalid_core)}
    corrected_core = {
        key: value for key, value in invalid_spec.items() if key != "spec_id"
    }
    corrected_core["schema_version"] = "cash_open_41_market_source_compatibility_spec/2.0.0"
    corrected_core["supersedes_invalid_spec_id"] = invalid_spec["spec_id"]
    corrected_core["invalidity_record_id"] = invalidity["record_id"]
    corrected_core["prepared_calendar"] = {
        "path": CORRECTED_CALENDAR_PATH.as_posix(),
        "sha256": CORRECTED_CALENDAR_SHA256,
        "calendar_id": CORRECTED_CALENDAR_ID,
        "active": False,
    }
    bindings = dict(corrected_core["bindings"])
    bindings.pop(INVALID_CALENDAR_PATH.as_posix())
    bindings[CORRECTED_CALENDAR_PATH.as_posix()] = CORRECTED_CALENDAR_SHA256
    corrected_core["bindings"] = dict(sorted(bindings.items()))
    corrected = {**corrected_core, "spec_id": sha256_json(corrected_core)}
    return invalidity, corrected
