"""Prepare a price-free calendar and source-observability successor.

The successor keeps exchange-calendar state separate from source observability:
two proven December 5, 2018 closures correct the calendar, while six sessions
with no reported bars in the bound local releases remain calendar-open and are
recorded as explicit source-unobservable research abstentions.

Concrete risk prevented: a missing source window must not be mistaken for an
exchange closure, a verified no-trade session, or silently removed evidence.
Decision improved: later readiness work can distinguish calendar eligibility
from source eligibility before constructing or certifying folds.  A calendar
boolean alone is insufficient because an open market and an absent bound source
are different facts with different remediation paths.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path

from .canonical import canonical_bytes, fsync_directory, sha256_file, sha256_json
from .errors import IntegrityError


ACTIVE_POINTER_PATH = Path(
    "configs/active_cash_open_impulse_historical_calendar.json"
)
ACTIVE_POINTER_SHA256 = (
    "f48d39156375e6f0152e5e380b4e73ce372399e22844e35ca4ad15f4227f6e27"
)
PREDECESSOR_CALENDAR_ID = (
    "cd64f912cceec3ff613b0d28f3965804c25d36d9b940d622b062128cfca0843b"
)
PREDECESSOR_CALENDAR_PATH = Path(
    "state/calendar_registry/cash_open_impulse_41_market/"
    f"{PREDECESSOR_CALENDAR_ID}/historical_calendar_successor.json"
)
PREDECESSOR_CALENDAR_SHA256 = (
    "e76ec4310da674e1bbacf5356662d97d8a2c8b115c728fa9386b53f8d289be52"
)
PROVENANCE_REPORT_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_missing_session_provenance_audit/"
    "provenance_report.json"
)
PROVENANCE_REPORT_ID = (
    "ca6e3173dbd986c959b2f59f80349f68d9aafba53caaf2b8f2d40feb27907ec3"
)
PROVENANCE_REPORT_SHA256 = (
    "5f26125eaa880064de02fb66389927f12ad34a00a06907529ff0c46df16f3bd7"
)
MECHANISM_ID = (
    "50dfc52cb5b4145dcbd6a761b3c626dae28c0aa974f6db35a1b60099297034e5"
)
MECHANISM_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_reported_trade_exit_successor/"
    f"{MECHANISM_ID}/mechanism.json"
)
MECHANISM_SHA256 = (
    "ddd1a3549ebad192fec3e00059170d3404a4cbb3d61e44cd347db56d4146941d"
)
MODULE_PATH = Path(
    "src/futures_rebuild/alpha_ladder_calendar_observability_successor.py"
)
PREPARE_SCRIPT_PATH = Path(
    "scripts/prepare_alpha_ladder_calendar_observability_successor.py"
)
OUTPUT_ROOT = Path(
    "state/unpublished_evidence/alpha_ladder_calendar_observability_successor"
)

CHECKPOINTS = ("09:00", "09:30", "10:00", "10:30")
CALENDAR_CORRECTIONS = {
    ("ES", "2018-12-05"): {
        "authoritative_checkpoint_state": "CLOSED_BY_08_30_CT",
        "semantic_basis": "EQUITY_PRODUCTS_ABBREVIATED_SESSION_ENDED_08_30_CT",
        "disposition": "AUTHORITATIVE_CME_CLOSED_AFTER_08_30_CT",
    },
    ("ZN", "2018-12-05"): {
        "authoritative_checkpoint_state": "CLOSED",
        "semantic_basis": (
            "INTEREST_RATE_PRODUCTS_DID_NOT_REOPEN_UNTIL_TRADE_DATE_2018_12_06"
        ),
        "disposition": "AUTHORITATIVE_CME_CLOSED_UNTIL_2018_12_06_TRADE_DATE",
    },
}
SOURCE_UNOBSERVABLE = (
    ("CL", "2020-02-28"),
    ("ZN", "2020-02-28"),
    ("6E", "2020-06-30"),
    ("CL", "2020-06-30"),
    ("ES", "2020-06-30"),
    ("ZN", "2020-06-30"),
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


def _require_bound_inputs(root: Path) -> tuple[dict[str, object], dict[str, object]]:
    expected = (
        (ACTIVE_POINTER_PATH, ACTIVE_POINTER_SHA256, "active calendar pointer"),
        (
            PREDECESSOR_CALENDAR_PATH,
            PREDECESSOR_CALENDAR_SHA256,
            "predecessor calendar",
        ),
        (PROVENANCE_REPORT_PATH, PROVENANCE_REPORT_SHA256, "provenance report"),
        (MECHANISM_PATH, MECHANISM_SHA256, "counted mechanism"),
    )
    for relative, digest, name in expected:
        if sha256_file(root / relative) != digest:
            raise IntegrityError(f"{name} hash drifted")
    pointer = _read_canonical(root / ACTIVE_POINTER_PATH, name="active calendar pointer")
    if (
        pointer.get("calendar_id") != PREDECESSOR_CALENDAR_ID
        or pointer.get("calendar_path") != PREDECESSOR_CALENDAR_PATH.as_posix()
        or pointer.get("calendar_sha256") != PREDECESSOR_CALENDAR_SHA256
    ):
        raise IntegrityError("active calendar pointer no longer selects the predecessor")
    calendar = _read_canonical(root / PREDECESSOR_CALENDAR_PATH, name="predecessor calendar")
    if calendar.get("calendar_id") != PREDECESSOR_CALENDAR_ID:
        raise IntegrityError("predecessor calendar identity drifted")
    report = _read_canonical(root / PROVENANCE_REPORT_PATH, name="provenance report")
    if (
        report.get("report_id") != PROVENANCE_REPORT_ID
        or report.get("state")
        != "SEALED_UNPUBLISHED_PRICE_FREE_PROVENANCE_AUDIT"
        or report.get("price_free_output") is not True
        or report.get("mechanism_id") != MECHANISM_ID
        or report.get("classification_counts")
        != {"CALENDAR_CLOSURE": 2, "RAW_SOURCE_ABSENCE": 6}
    ):
        raise IntegrityError("sealed provenance report semantics drifted")
    return calendar, report


def _result_index(report: Mapping[str, object]) -> dict[tuple[str, str], Mapping[str, object]]:
    results = report.get("results")
    if not isinstance(results, list) or len(results) != 8:
        raise IntegrityError("provenance report target topology is invalid")
    indexed: dict[tuple[str, str], Mapping[str, object]] = {}
    for raw in results:
        if not isinstance(raw, Mapping):
            raise IntegrityError("provenance result is invalid")
        key = (str(raw.get("market")), str(raw.get("session")))
        if key in indexed:
            raise IntegrityError("provenance report contains duplicate targets")
        indexed[key] = raw
    expected = set(CALENDAR_CORRECTIONS) | set(SOURCE_UNOBSERVABLE)
    if set(indexed) != expected:
        raise IntegrityError("provenance report target scope drifted")
    return indexed


def _calendar_rows(calendar: Mapping[str, object]) -> list[dict[str, object]]:
    rows = calendar.get("calendar_rows")
    if not isinstance(rows, list) or len(rows) != 74_866:
        raise IntegrityError("predecessor calendar row topology is incomplete")
    output: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise IntegrityError("predecessor calendar row is invalid")
        row = deepcopy(dict(raw))
        key = (str(row.get("market")), str(row.get("trade_date")))
        if key in seen:
            raise IntegrityError("predecessor calendar rows are not unique")
        seen.add(key)
        if key in CALENDAR_CORRECTIONS:
            correction = CALENDAR_CORRECTIONS[key]
            row["checkpoint_open"] = {checkpoint: False for checkpoint in CHECKPOINTS}
            row["disposition"] = {
                checkpoint: correction["disposition"] for checkpoint in CHECKPOINTS
            }
        output.append(row)
    if not set(CALENDAR_CORRECTIONS).issubset(seen):
        raise IntegrityError("calendar correction targets are absent")
    return output


def _correction_records(
    report_index: Mapping[tuple[str, str], Mapping[str, object]],
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for market, session in sorted(CALENDAR_CORRECTIONS):
        expected = CALENDAR_CORRECTIONS[(market, session)]
        result = report_index[(market, session)]
        if (
            result.get("classification") != "CALENDAR_CLOSURE"
            or result.get("classification_detail")
            != "AUTHORITATIVE_CME_SCHEDULE_CORRECTION"
            or result.get("authoritative_calendar_correction")
            != {
                "authoritative_checkpoint_state": expected[
                    "authoritative_checkpoint_state"
                ],
                "semantic_basis": expected["semantic_basis"],
            }
        ):
            raise IntegrityError(f"calendar correction evidence drifted for {market} {session}")
        records.append(
            {
                "market": market,
                "trade_date": session,
                "previous_checkpoint_open": {
                    checkpoint: True for checkpoint in CHECKPOINTS
                },
                "corrected_checkpoint_open": {
                    checkpoint: False for checkpoint in CHECKPOINTS
                },
                "corrected_disposition": {
                    checkpoint: expected["disposition"] for checkpoint in CHECKPOINTS
                },
                "authoritative_checkpoint_state": expected[
                    "authoritative_checkpoint_state"
                ],
                "semantic_basis": expected["semantic_basis"],
            }
        )
    return records


def _source_records(
    report_index: Mapping[tuple[str, str], Mapping[str, object]],
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for market, session in SOURCE_UNOBSERVABLE:
        result = report_index[(market, session)]
        inputs = result.get("classification_inputs")
        active_calendar = result.get("active_calendar")
        if (
            result.get("classification") != "RAW_SOURCE_ABSENCE"
            or result.get("classification_detail")
            != "NO_PROVIDER_REPORTED_BARS_AND_NO_NO_TRADE_PROOF"
            or not isinstance(inputs, Mapping)
            or any(
                inputs.get(field) != 0
                for field in (
                    "active_causal_count",
                    "alternate_causal_count",
                    "any_causal_count",
                    "causal_mislabeled_count",
                    "raw_count",
                    "dbn_1m_count",
                    "dbn_1s_count",
                )
            )
            or inputs.get("independent_trade_stream_complete") is not False
            or inputs.get("independent_trade_count") is not None
            or not isinstance(active_calendar, Mapping)
            or active_calendar.get("calendar_admitted_10_00") is not True
        ):
            raise IntegrityError(f"source-unobservable evidence drifted for {market} {session}")
        records.append(
            {
                "market": market,
                "trade_date": session,
                "checkpoint": "10:00",
                "dependency_window": {
                    "start_inclusive": "09:30:00",
                    "end_exclusive": "10:00:00",
                    "timezone": "America/Chicago",
                },
                "calendar_state": "OPEN",
                "source_state": "SOURCE_UNOBSERVABLE",
                "research_disposition": "EXPLICIT_SOURCE_UNOBSERVABLE_ABSTENTION",
                "classification_detail": (
                    "NO_PROVIDER_REPORTED_BARS_AND_NO_NO_TRADE_PROOF"
                ),
                "bound_local_reported_bar_count": 0,
                "normalization_loss_detected": False,
                "verified_no_trade_claim": False,
                "silent_drop_allowed": False,
                "required_checkpoint_accounting": True,
                "current_mechanism_registration_effect": (
                    "FAIL_CLOSED_UNTIL_AUTHORITATIVE_SOURCE_RECOVERY_OR_A_NEW_"
                    "COUNTED_SOURCE_ELIGIBILITY_SEMANTIC"
                ),
            }
        )
    return records


def build_successor(*, root: Path) -> dict[str, object]:
    """Build the inactive successor only from sealed, price-free evidence."""

    calendar, report = _require_bound_inputs(root)
    report_index = _result_index(report)
    rows = _calendar_rows(calendar)
    corrections = _correction_records(report_index)
    source_records = _source_records(report_index)
    inherited = {
        key: deepcopy(value)
        for key, value in calendar.items()
        if key
        not in {
            "authority",
            "bindings",
            "calendar_id",
            "calendar_rows",
            "decision",
            "predecessor",
            "status",
            "schema_version",
        }
    }
    core: dict[str, object] = {
        **inherited,
        "schema_version": "alpha_ladder_calendar_observability_successor/1.0.0",
        "status": "PREPARED_INACTIVE_UNPUBLISHED",
        "decision": "PASS_TWO_CALENDAR_CORRECTIONS_SIX_SOURCE_ABSTENTIONS",
        "authority": {
            "active": False,
            "historical_rows_reread": False,
            "mechanism_registered": False,
            "mechanism_changed": False,
            "performance_evaluation": False,
            "provider_network_credentials_accessed": False,
            "published": False,
            "year_2025_accessed": False,
        },
        "predecessor": {
            "calendar_id": PREDECESSOR_CALENDAR_ID,
            "path": PREDECESSOR_CALENDAR_PATH.as_posix(),
            "sha256": PREDECESSOR_CALENDAR_SHA256,
            "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
            "active_pointer_sha256": ACTIVE_POINTER_SHA256,
        },
        "provenance_audit": {
            "report_id": PROVENANCE_REPORT_ID,
            "path": PROVENANCE_REPORT_PATH.as_posix(),
            "sha256": PROVENANCE_REPORT_SHA256,
            "price_free": True,
        },
        "calendar_correction_count": len(corrections),
        "calendar_corrections": corrections,
        "source_observability_record_count": len(source_records),
        "source_observability_records": source_records,
        "source_recovery": {
            "authoritative_rows_recovered": False,
            "existing_bound_local_releases_exhausted_by_provenance_audit": True,
            "external_provider_access_attempted": False,
            "chosen_remediation": "EXPLICIT_SOURCE_UNOBSERVABLE_SESSIONS",
        },
        "registration_gate": {
            "mechanism_id": MECHANISM_ID,
            "registration_allowed": False,
            "reason": (
                "CALENDAR_AND_SOURCE_SUCCESSOR_IS_INACTIVE_AND_THE_LOCKED_"
                "MECHANISM_REQUIRES_A_NEW_100_PERCENT_READINESS_CERTIFICATE"
            ),
            "source_unobservable_sessions_may_be_silently_removed": False,
            "source_unobservable_sessions_are_verified_no_trade": False,
        },
        "calendar_rows": rows,
        "bindings": dict(
            sorted(
                {
                    ACTIVE_POINTER_PATH.as_posix(): ACTIVE_POINTER_SHA256,
                    PREDECESSOR_CALENDAR_PATH.as_posix(): PREDECESSOR_CALENDAR_SHA256,
                    PROVENANCE_REPORT_PATH.as_posix(): PROVENANCE_REPORT_SHA256,
                    MECHANISM_PATH.as_posix(): MECHANISM_SHA256,
                    MODULE_PATH.as_posix(): sha256_file(root / MODULE_PATH),
                    PREPARE_SCRIPT_PATH.as_posix(): sha256_file(
                        root / PREPARE_SCRIPT_PATH
                    ),
                }.items()
            )
        ),
    }
    successor = {**core, "calendar_id": sha256_json(core)}
    validate_successor(successor, root=root)
    return successor


def validate_successor(
    successor: Mapping[str, object], *, root: Path, verify_bindings: bool = True
) -> None:
    """Fail closed unless only the proven calendar rows changed."""

    core = {key: value for key, value in successor.items() if key != "calendar_id"}
    if (
        successor.get("calendar_id") != sha256_json(core)
        or successor.get("schema_version")
        != "alpha_ladder_calendar_observability_successor/1.0.0"
        or successor.get("status") != "PREPARED_INACTIVE_UNPUBLISHED"
        or successor.get("decision")
        != "PASS_TWO_CALENDAR_CORRECTIONS_SIX_SOURCE_ABSTENTIONS"
        or successor.get("calendar_correction_count") != 2
        or successor.get("source_observability_record_count") != 6
    ):
        raise IntegrityError("calendar-observability successor identity is invalid")
    authority = successor.get("authority")
    gate = successor.get("registration_gate")
    if (
        not isinstance(authority, Mapping)
        or authority.get("active") is not False
        or authority.get("historical_rows_reread") is not False
        or authority.get("mechanism_registered") is not False
        or authority.get("year_2025_accessed") is not False
        or not isinstance(gate, Mapping)
        or gate.get("registration_allowed") is not False
        or gate.get("source_unobservable_sessions_may_be_silently_removed")
        is not False
    ):
        raise IntegrityError("calendar-observability authority boundary is invalid")
    predecessor = _read_canonical(root / PREDECESSOR_CALENDAR_PATH, name="predecessor calendar")
    before_rows = predecessor.get("calendar_rows")
    after_rows = successor.get("calendar_rows")
    if (
        not isinstance(before_rows, list)
        or not isinstance(after_rows, list)
        or len(before_rows) != 74_866
        or len(after_rows) != 74_866
    ):
        raise IntegrityError("calendar-observability row topology is invalid")
    changed: set[tuple[str, str]] = set()
    for before, after in zip(before_rows, after_rows, strict=True):
        if not isinstance(before, Mapping) or not isinstance(after, Mapping):
            raise IntegrityError("calendar-observability row is invalid")
        before_key = (str(before.get("market")), str(before.get("trade_date")))
        after_key = (str(after.get("market")), str(after.get("trade_date")))
        if before_key != after_key:
            raise IntegrityError("calendar-observability row ordering changed")
        if before != after:
            changed.add(before_key)
            expected = CALENDAR_CORRECTIONS.get(before_key)
            if (
                expected is None
                or before.get("checkpoint_open")
                != {checkpoint: True for checkpoint in CHECKPOINTS}
                or after.get("checkpoint_open")
                != {checkpoint: False for checkpoint in CHECKPOINTS}
                or after.get("disposition")
                != {
                    checkpoint: expected["disposition"]
                    for checkpoint in CHECKPOINTS
                }
            ):
                raise IntegrityError("an unproven calendar change was introduced")
    if changed != set(CALENDAR_CORRECTIONS):
        raise IntegrityError("the exact two calendar corrections were not isolated")
    source_records = successor.get("source_observability_records")
    if not isinstance(source_records, list):
        raise IntegrityError("source-observability records are absent")
    observed = {
        (str(item.get("market")), str(item.get("trade_date")))
        for item in source_records
        if isinstance(item, Mapping)
        and item.get("source_state") == "SOURCE_UNOBSERVABLE"
        and item.get("calendar_state") == "OPEN"
        and item.get("required_checkpoint_accounting") is True
        and item.get("verified_no_trade_claim") is False
    }
    if observed != set(SOURCE_UNOBSERVABLE) or len(source_records) != 6:
        raise IntegrityError("source-unobservable scope or semantics drifted")
    after_index = {
        (str(item["market"]), str(item["trade_date"])): item
        for item in after_rows
        if isinstance(item, Mapping)
    }
    for key in SOURCE_UNOBSERVABLE:
        if after_index[key]["checkpoint_open"]["10:00"] is not True:
            raise IntegrityError("source-unobservable session was relabelled as closed")
    if verify_bindings:
        bindings = successor.get("bindings")
        if not isinstance(bindings, Mapping):
            raise IntegrityError("calendar-observability bindings are absent")
        for relative, digest in bindings.items():
            if sha256_file(root / str(relative)) != digest:
                raise IntegrityError(
                    f"calendar-observability binding drifted: {relative}"
                )


def source_disposition(
    successor: Mapping[str, object], *, market: str, trade_date: str
) -> str:
    """Return the explicit override without claiming unscanned sessions observable."""

    corrections = successor.get("calendar_corrections")
    source_records = successor.get("source_observability_records")
    if not isinstance(corrections, list) or not isinstance(source_records, list):
        raise IntegrityError("calendar-observability successor is incomplete")
    key = (market, trade_date)
    if any(
        isinstance(item, Mapping)
        and (item.get("market"), item.get("trade_date")) == key
        for item in corrections
    ):
        return "CALENDAR_CLOSED"
    if any(
        isinstance(item, Mapping)
        and (item.get("market"), item.get("trade_date")) == key
        for item in source_records
    ):
        return "SOURCE_UNOBSERVABLE_EXPLICIT_ABSTENTION"
    return "NO_OVERRIDE_REQUIRES_ROW_CERTIFICATION"


def persist_preparation(*, root: Path) -> Path:
    successor = build_successor(root=root)
    calendar_id = str(successor["calendar_id"])
    path = OUTPUT_ROOT / calendar_id / "historical_calendar_successor.json"
    destination = root / path
    encoded = canonical_bytes(successor) + b"\n"
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            destination,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        )
    except FileExistsError:
        if destination.read_bytes() != encoded:
            raise IntegrityError("existing calendar-observability preparation differs")
        return path
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(destination.parent)
    return path
