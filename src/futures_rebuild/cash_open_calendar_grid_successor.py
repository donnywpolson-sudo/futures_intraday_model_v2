"""Prepare an inactive four-checkpoint successor to the active cash-open calendar."""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from pathlib import Path

from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .cme_calendar_successor import (
    CHICAGO,
    MARKET_FAMILY,
    _load_recovered_jan1_schedule,
    _load_schedules,
    _state,
)
from .errors import IntegrityError


BASE_CALENDAR_ID = "54bc5550a0ba28af2a509fb32c756b39041686ba10ffa6bd832e6d96469c0397"
BASE_CALENDAR_SHA256 = "7860a57f7b64288be333d82cfc7e0f1b889c06304f9cedbb3a8abb3caff795ec"
BASE_CALENDAR_PATH = Path(
    "state/calendar_registry/cash_open_impulse_41_market/"
    f"{BASE_CALENDAR_ID}/historical_calendar_successor.json"
)
ACTIVE_POINTER_PATH = Path("configs/active_cash_open_impulse_historical_calendar.json")
ACTIVE_POINTER_SHA256 = "6f534035bd3707d0a1c5937af5d338947509ee105b2c2656570f1dd06ff84132"
PREDECESSOR_SNAPSHOT_PATH = Path(
    "state/unpublished_evidence/cash_open_calendar_predecessor_snapshot/"
    f"{ACTIVE_POINTER_SHA256}/active_cash_open_impulse_historical_calendar.json"
)
CHECKPOINTS = ("09:00", "09:30", "10:00", "10:30")


def _read_canonical(path: Path) -> dict[str, object]:
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"calendar input is invalid: {path}") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"calendar input is not canonical: {path}")
    return payload


def _window(day: date, checkpoint: str) -> tuple[datetime, datetime]:
    clock = time.fromisoformat(checkpoint)
    center = datetime.combine(day, clock)
    return center - timedelta(minutes=30), center + timedelta(minutes=31)


def build_grid_successor(*, root: Path) -> dict[str, object]:
    if sha256_file(root / BASE_CALENDAR_PATH) != BASE_CALENDAR_SHA256:
        raise IntegrityError("active cash-open calendar bytes drifted")
    if sha256_file(root / ACTIVE_POINTER_PATH) != ACTIVE_POINTER_SHA256:
        raise IntegrityError("active cash-open calendar pointer drifted")
    if sha256_file(root / PREDECESSOR_SNAPSHOT_PATH) != ACTIVE_POINTER_SHA256:
        raise IntegrityError("immutable predecessor pointer snapshot drifted")
    if (root / PREDECESSOR_SNAPSHOT_PATH).read_bytes() != (root / ACTIVE_POINTER_PATH).read_bytes():
        raise IntegrityError("active pointer differs from immutable predecessor snapshot")
    base = _read_canonical(root / BASE_CALENDAR_PATH)
    if base.get("calendar_id") != BASE_CALENDAR_ID:
        raise IntegrityError("active cash-open calendar identity drifted")

    schedules, capture_release = _load_schedules(RepoBoundary(root))
    recovered, recovery_binding = _load_recovered_jan1_schedule(root)
    for family, schedule in recovered.items():
        schedules[(family, date(2019, 1, 1))].append(
            (
                str(recovery_binding["raw_path"]),
                str(recovery_binding["raw_sha256"]),
                schedule,
            )
        )
    if capture_release != base.get("calendar_capture_release_id"):
        raise IntegrityError("calendar capture release differs from active calendar")

    output_rows: list[dict[str, object]] = []
    unresolved: list[dict[str, str]] = []
    rows = base.get("calendar_rows")
    if not isinstance(rows, list) or len(rows) != 74_866:
        raise IntegrityError("active calendar row topology is incomplete")
    for raw_row in rows:
        if not isinstance(raw_row, dict):
            raise IntegrityError("active calendar row is invalid")
        market = str(raw_row["market"])
        family = str(raw_row["schedule_family"])
        day = date.fromisoformat(str(raw_row["trade_date"]))
        old_open = raw_row.get("checkpoint_open")
        old_disposition = raw_row.get("disposition")
        if not isinstance(old_open, dict) or not isinstance(old_disposition, dict):
            raise IntegrityError("active calendar checkpoint state is invalid")
        reference_reason = str(old_disposition.get("09:00"))
        checkpoint_open: dict[str, bool] = {}
        disposition: dict[str, str] = {}
        for checkpoint in CHECKPOINTS:
            if checkpoint in {"09:00", "10:30"}:
                checkpoint_open[checkpoint] = bool(old_open[checkpoint])
                disposition[checkpoint] = str(old_disposition[checkpoint])
                continue
            if reference_reason in {
                "PRODUCT_NOT_EFFECTIVE",
                "WEEKEND_CLOSED",
                "REGULAR_WEEKDAY_REFERENCE_RULE",
            }:
                checkpoint_open[checkpoint] = reference_reason == "REGULAR_WEEKDAY_REFERENCE_RULE"
                disposition[checkpoint] = reference_reason
                continue
            candidates = schedules.get((family, day), [])
            full = [item for item in candidates if "compact" not in item[0].lower()]
            if full:
                candidates = full
            if not candidates:
                checkpoint_open[checkpoint] = False
                disposition[checkpoint] = "UNVERIFIED_REFERENCE_ABSTENTION"
                unresolved.append(
                    {"market": market, "family": family, "date": day.isoformat(), "checkpoint": checkpoint}
                )
                continue
            start, end = _window(day, checkpoint)
            values = {_state(schedule, start, end) for _, _, schedule in candidates}
            if len(values) != 1:
                checkpoint_open[checkpoint] = False
                disposition[checkpoint] = "CONFLICTING_REFERENCE_ABSTENTION"
                unresolved.append(
                    {"market": market, "family": family, "date": day.isoformat(), "checkpoint": checkpoint}
                )
            else:
                checkpoint_open[checkpoint] = values.pop()
                disposition[checkpoint] = "EXACT_CME_FAMILY_SCHEDULE"
        output_rows.append(
            {
                "checkpoint_open": checkpoint_open,
                "disposition": disposition,
                "market": market,
                "schedule_family": family,
                "trade_date": day.isoformat(),
            }
        )

    core: dict[str, object] = {
        "schema_version": "cash_open_impulse_41_market_calendar_grid_successor/1.0.0",
        "status": "PREPARED_INACTIVE_UNPUBLISHED",
        "decision": (
            "PASS_EXACT_REFERENCE_COVERAGE"
            if not unresolved
            else "FAIL_UNRESOLVED_REFERENCE_COVERAGE"
        ),
        "authority": {
            "active": False,
            "price_rows_read": False,
            "provider_network_credentials_accessed": False,
            "published": False,
            "year_2025_accessed": False,
        },
        "checkpoint_grid": list(CHECKPOINTS),
        "dependency_horizon": "FEATURE_START_THROUGH_SCHEDULED_EXIT_OPEN",
        "predecessor": {
            "calendar_id": BASE_CALENDAR_ID,
            "path": BASE_CALENDAR_PATH.as_posix(),
            "sha256": BASE_CALENDAR_SHA256,
        },
        "predecessor_pointer_snapshot": {
            "path": PREDECESSOR_SNAPSHOT_PATH.as_posix(),
            "sha256": ACTIVE_POINTER_SHA256,
        },
        "calendar_capture_release_id": capture_release,
        "market_to_schedule_family": base["market_to_schedule_family"],
        "market_economics_source_ids": base["market_economics_source_ids"],
        "definition_bindings": base["definition_bindings"],
        "product_effective_intervals": base["product_effective_intervals"],
        "additive_january_1_2019_recovery": recovery_binding,
        "calendar_rows": output_rows,
        "unresolved_reference_count": len(unresolved),
        "unresolved_reference_states": unresolved,
        "bindings": {
            BASE_CALENDAR_PATH.as_posix(): BASE_CALENDAR_SHA256,
            PREDECESSOR_SNAPSHOT_PATH.as_posix(): ACTIVE_POINTER_SHA256,
            "src/futures_rebuild/cash_open_calendar_grid_successor.py": sha256_file(Path(__file__)),
            "src/futures_rebuild/cme_calendar_successor.py": sha256_file(
                root / "src/futures_rebuild/cme_calendar_successor.py"
            ),
            "scripts/prepare_cash_open_calendar_grid_successor.py": sha256_file(
                root / "scripts/prepare_cash_open_calendar_grid_successor.py"
            ),
        },
    }
    return {**core, "calendar_id": sha256_json(core)}
