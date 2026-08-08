"""Prepare the full-regular, source-observable counted Alpha successor.

This module is deliberately pre-data.  It reads only immutable mechanism,
calendar, and sealed price-free diagnostic artifacts.  Historical price rows
remain behind the separately authorized readiness-census boundary.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections import Counter
from collections.abc import Mapping
from pathlib import Path

from .alpha_ladder_reported_trade_exit_successor import (
    validate_successor as validate_reported_trade_successor,
)
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


PREDECESSOR_ID = "50dfc52cb5b4145dcbd6a761b3c626dae28c0aa974f6db35a1b60099297034e5"
PREDECESSOR_SHA256 = "ddd1a3549ebad192fec3e00059170d3404a4cbb3d61e44cd347db56d4146941d"
PREDECESSOR_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_reported_trade_exit_successor/"
    f"{PREDECESSOR_ID}/mechanism.json"
)

READINESS_REPORT_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_reported_trade_exit_readiness/"
    "readiness_report.json"
)
READINESS_REPORT_ID = "aba1c5f17fb8f3806c14dc36a1ce79c8130944eaa5a4dfe56dea5884ef7dbc93"
TIER1_CERTIFICATE_PATH = READINESS_REPORT_PATH.with_name("tier1_readiness_certificate.json")
TIER1_CERTIFICATE_ID = "a3189e7e1333a93e733af9e03a79e65b043871c304c704e068d0e11ebf5ab41b"
DIAGNOSTIC_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_reported_trade_exit_feature_gap_diagnostic/"
    "diagnostic_report.json"
)
DIAGNOSTIC_ID = "afef7a5849352a57b60d8daa197c0bc325892045f2158a689ec5d9daf4914235"
DIAGNOSTIC_SHA256 = "77e70f7ceb8965d6a0cbf9984eabb249026aed99d04965791b328269d5b4f71b"
PROVENANCE_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_missing_session_provenance_audit/"
    "provenance_report.json"
)
PROVENANCE_ID = "ca6e3173dbd986c959b2f59f80349f68d9aafba53caaf2b8f2d40feb27907ec3"
PROVENANCE_SHA256 = "5f26125eaa880064de02fb66389927f12ad34a00a06907529ff0c46df16f3bd7"

CALENDAR_ID = "ddbe0c706d6568d8d7ddefd830677d73978b428d8a99925290310224f673a7f9"
CALENDAR_SHA256 = "efdf4f765e44ac2f312dce62b7145bb1ed70d01fd8c76fb5bfb3f32652f1a632"
CALENDAR_PATH = Path(
    "state/calendar_registry/cash_open_impulse_41_market/"
    f"{CALENDAR_ID}/historical_calendar_successor.json"
)
CALENDAR_REGISTRATION_PATH = CALENDAR_PATH.with_name("registration_v2.json")
CALENDAR_REGISTRATION_ID = "eb57241b1214e2dc85e8a36695059da3e3e0e65222eb4f2e8e7808b0f6a3ff6b"
CALENDAR_REGISTRATION_SHA256 = "704fb8cdb80e44b64eefba4464ce30f76ffc99163a77bfad7ece1b0cfdb21fab"
CALENDAR_EVENT_PATH = Path(
    "state/calendar_events/cash_open_impulse_41_market/"
    "1ac22903b81ca4dd21b24957a09ee95bc93f8d66d52445791cd13de27e11f823.json"
)
CALENDAR_EVENT_ID = "1ac22903b81ca4dd21b24957a09ee95bc93f8d66d52445791cd13de27e11f823"
CALENDAR_EVENT_SHA256 = "e338ef8f5578c13a28d4767cf2eaf2b5cdada3e826795f79200931867e4b9a2a"
ACTIVE_CALENDAR_POINTER_PATH = Path("configs/active_cash_open_impulse_historical_calendar.json")
ACTIVE_CALENDAR_POINTER_ID = "bfc3036f739f7fac592e9f7ebf6ff9ee225c8f257d6f1e324875d74a0cec35e4"
ACTIVE_CALENDAR_POINTER_SHA256 = "8956289401a172cf3726336e879ba7e8e72d0485fa3927805067eebd3644e1fc"

MODULE_PATH = Path(
    "src/futures_rebuild/alpha_ladder_full_regular_source_observable_successor.py"
)
PREPARE_SCRIPT_PATH = Path(
    "scripts/prepare_alpha_ladder_full_regular_source_observable_successor.py"
)
CLOSURE_ROOT = Path(
    "state/unpublished_evidence/alpha_ladder_reported_trade_exit_source_incompatibility_closure"
)
SUCCESSOR_ROOT = Path(
    "state/unpublished_evidence/alpha_ladder_full_regular_source_observable_successor"
)

CORE_MARKETS = ("ES", "CL", "ZN", "6E")
CHECKPOINT = "10:00"
REGULAR_DISPOSITION = "REGULAR_WEEKDAY_REFERENCE_RULE"
SPECIAL_DISPOSITION = "EXACT_CME_FAMILY_SCHEDULE"
CALENDAR_CLOSED = "CALENDAR_CLOSED"
HOLIDAY_ABSTENTION = "HOLIDAY_MODIFIED_SCHEDULE_ABSTENTION"
SOURCE_ABSTENTION = "SOURCE_UNOBSERVABLE_ABSTENTION"
ELIGIBLE = "ELIGIBLE_FULL_REGULAR_SOURCE_OBSERVABLE"

EXPECTED_MARKET_COUNTS = {
    "ES": {
        "calendar_rows": 1826,
        "calendar_closed": 536,
        "calendar_open": 1290,
        "holiday_modified_abstentions": 41,
        "source_unobservable_abstentions": 1,
        "eligible_fold_sessions": 1248,
    },
    "CL": {
        "calendar_rows": 1826,
        "calendar_closed": 535,
        "calendar_open": 1291,
        "holiday_modified_abstentions": 41,
        "source_unobservable_abstentions": 2,
        "eligible_fold_sessions": 1248,
    },
    "ZN": {
        "calendar_rows": 1826,
        "calendar_closed": 536,
        "calendar_open": 1290,
        "holiday_modified_abstentions": 41,
        "source_unobservable_abstentions": 2,
        "eligible_fold_sessions": 1247,
    },
    "6E": {
        "calendar_rows": 1826,
        "calendar_closed": 536,
        "calendar_open": 1290,
        "holiday_modified_abstentions": 40,
        "source_unobservable_abstentions": 1,
        "eligible_fold_sessions": 1249,
    },
}

OUTCOME_ACCESS = {
    "returns_read": False,
    "predictions_generated": False,
    "models_fit": False,
    "economic_evaluation": False,
    "year_2025_access": False,
}
AUTHORITY = {
    "historical_rows": False,
    "registration": False,
    "execution": False,
    "publication": False,
    "holdout_2025": False,
    "provider_network_credentials": False,
    "trading": False,
}


def _load(path: Path) -> dict[str, object]:
    raw = path.read_bytes()
    if not raw.endswith(b"\n") or raw[:-1].endswith(b"\n"):
        raise IntegrityError(f"artifact is not canonical single-line JSON: {path}")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise IntegrityError(f"artifact is not an object: {path}")
    return value


def _load_predecessor(*, root: Path) -> dict[str, object]:
    mechanism_path = root / PREDECESSOR_PATH
    mechanism = _load(mechanism_path)
    if (
        sha256_file(mechanism_path) != PREDECESSOR_SHA256
        or mechanism.get("mechanism_id") != PREDECESSOR_ID
        or mechanism.get("state")
        != "PREPARED_UNPUBLISHED_UNREGISTERED_TIER0_RESTART_REQUIRED"
    ):
        raise IntegrityError("reported-trade predecessor changed")
    predecessor_binding = mechanism.get("predecessor")
    if not isinstance(predecessor_binding, Mapping):
        raise IntegrityError("reported-trade predecessor binding is missing")
    old_predecessor = _load(root / str(predecessor_binding["path"]))
    old_closure = _load(root / str(predecessor_binding["closure_path"]))
    validate_reported_trade_successor(
        mechanism,
        predecessor=old_predecessor,
        closure=old_closure,
        root=root,
    )
    return mechanism


def _validate_active_pointer(*, root: Path) -> dict[str, object]:
    path = root / ACTIVE_CALENDAR_POINTER_PATH
    pointer = _load(path)
    if (
        sha256_file(path) != ACTIVE_CALENDAR_POINTER_SHA256
        or pointer.get("pointer_id") != ACTIVE_CALENDAR_POINTER_ID
        or pointer.get("calendar_id") != CALENDAR_ID
        or pointer.get("calendar_path") != CALENDAR_PATH.as_posix()
        or pointer.get("calendar_sha256") != CALENDAR_SHA256
        or pointer.get("registration_id") != CALENDAR_REGISTRATION_ID
        or pointer.get("event_id") != CALENDAR_EVENT_ID
        or pointer.get("state") != "ACTIVE_CALENDAR_AND_SOURCE_OBSERVABILITY"
    ):
        raise IntegrityError("active calendar context changed before preparation")
    return pointer


def _load_calendar(*, root: Path, require_active_context: bool) -> dict[str, object]:
    path = root / CALENDAR_PATH
    calendar = _load(path)
    registration = _load(root / CALENDAR_REGISTRATION_PATH)
    event = _load(root / CALENDAR_EVENT_PATH)
    if require_active_context:
        _validate_active_pointer(root=root)
    if (
        sha256_file(path) != CALENDAR_SHA256
        or calendar.get("calendar_id") != CALENDAR_ID
        or calendar.get("source_observability_record_count") != 6
        or sha256_file(root / CALENDAR_REGISTRATION_PATH)
        != CALENDAR_REGISTRATION_SHA256
        or registration.get("registration_id") != CALENDAR_REGISTRATION_ID
        or sha256_file(root / CALENDAR_EVENT_PATH) != CALENDAR_EVENT_SHA256
        or event.get("event_id") != CALENDAR_EVENT_ID
    ):
        raise IntegrityError("immutable calendar authority changed")
    return calendar


def _source_unobservable_keys(calendar: Mapping[str, object]) -> frozenset[tuple[str, str, str]]:
    records = calendar.get("source_observability_records")
    if not isinstance(records, list) or len(records) != 6:
        raise IntegrityError("source-observability inventory changed")
    keys: set[tuple[str, str, str]] = set()
    for record in records:
        if not isinstance(record, Mapping):
            raise IntegrityError("source-observability record is invalid")
        key = (
            str(record.get("market")),
            str(record.get("trade_date")),
            str(record.get("checkpoint")),
        )
        if (
            key in keys
            or key[0] not in CORE_MARKETS
            or key[2] != CHECKPOINT
            or record.get("source_state") != "SOURCE_UNOBSERVABLE"
            or record.get("research_disposition")
            != "EXPLICIT_SOURCE_UNOBSERVABLE_ABSTENTION"
            or record.get("required_checkpoint_accounting") is not True
            or record.get("silent_drop_allowed") is not False
            or record.get("verified_no_trade_claim") is not False
        ):
            raise IntegrityError("source-observability record weakened or changed")
        keys.add(key)
    return frozenset(keys)


def classify_calendar_session(
    row: Mapping[str, object],
    *,
    source_unobservable_keys: frozenset[tuple[str, str, str]],
) -> str:
    """Classify one checkpoint without consulting price or return data."""

    market = str(row.get("market"))
    trade_date = str(row.get("trade_date"))
    checkpoint_open = row.get("checkpoint_open")
    dispositions = row.get("disposition")
    if (
        market not in CORE_MARKETS
        or not isinstance(checkpoint_open, Mapping)
        or not isinstance(dispositions, Mapping)
        or not isinstance(checkpoint_open.get(CHECKPOINT), bool)
        or not isinstance(dispositions.get(CHECKPOINT), str)
    ):
        raise IntegrityError("calendar row cannot be classified")
    if checkpoint_open[CHECKPOINT] is False:
        return CALENDAR_CLOSED
    disposition = dispositions[CHECKPOINT]
    if disposition == SPECIAL_DISPOSITION:
        return HOLIDAY_ABSTENTION
    if disposition != REGULAR_DISPOSITION:
        raise IntegrityError("unknown open calendar disposition fails closed")
    if (market, trade_date, CHECKPOINT) in source_unobservable_keys:
        return SOURCE_ABSTENTION
    return ELIGIBLE


def build_calendar_accounting(calendar: Mapping[str, object]) -> dict[str, object]:
    rows = calendar.get("calendar_rows")
    if not isinstance(rows, list):
        raise IntegrityError("calendar rows are missing")
    source_keys = _source_unobservable_keys(calendar)
    inventory: list[dict[str, str]] = []
    by_market: dict[str, dict[str, int]] = {}
    by_market_year: dict[str, dict[str, int]] = {}
    seen_source_keys: set[tuple[str, str, str]] = set()
    for market in CORE_MARKETS:
        counts: Counter[str] = Counter()
        market_year: dict[str, Counter[str]] = {}
        market_rows = [row for row in rows if isinstance(row, Mapping)
                       and row.get("market") == market]
        for row in market_rows:
            trade_date = str(row.get("trade_date"))
            if not trade_date.startswith(("2018-", "2019-", "2020-", "2021-", "2022-")):
                raise IntegrityError("calendar accounting escaped 2018-2022")
            disposition = classify_calendar_session(
                row, source_unobservable_keys=source_keys)
            if disposition == SOURCE_ABSTENTION:
                seen_source_keys.add((market, trade_date, CHECKPOINT))
            counts[disposition] += 1
            year_counts = market_year.setdefault(trade_date[:4], Counter())
            year_counts[disposition] += 1
            inventory.append({
                "market": market,
                "trade_date": trade_date,
                "checkpoint": CHECKPOINT,
                "disposition": disposition,
            })
        summary = {
            "calendar_rows": len(market_rows),
            "calendar_closed": counts[CALENDAR_CLOSED],
            "calendar_open": (
                counts[HOLIDAY_ABSTENTION]
                + counts[SOURCE_ABSTENTION]
                + counts[ELIGIBLE]
            ),
            "holiday_modified_abstentions": counts[HOLIDAY_ABSTENTION],
            "source_unobservable_abstentions": counts[SOURCE_ABSTENTION],
            "eligible_fold_sessions": counts[ELIGIBLE],
        }
        if summary != EXPECTED_MARKET_COUNTS[market]:
            raise IntegrityError(f"calendar accounting changed for {market}: {summary}")
        if summary["calendar_closed"] + summary["calendar_open"] != summary["calendar_rows"]:
            raise IntegrityError("calendar accounting silently dropped a row")
        by_market[market] = summary
        for year, year_counter in sorted(market_year.items()):
            total = sum(year_counter.values())
            by_market_year[f"{market}/{year}"] = {
                "calendar_rows": total,
                "calendar_closed": year_counter[CALENDAR_CLOSED],
                "holiday_modified_abstentions": year_counter[HOLIDAY_ABSTENTION],
                "source_unobservable_abstentions": year_counter[SOURCE_ABSTENTION],
                "eligible_fold_sessions": year_counter[ELIGIBLE],
            }
    if seen_source_keys != set(source_keys):
        raise IntegrityError("a source-unobservable session escaped explicit accounting")
    inventory.sort(key=lambda item: (item["market"], item["trade_date"], item["checkpoint"]))
    totals = {
        key: sum(market_counts[key] for market_counts in by_market.values())
        for key in next(iter(EXPECTED_MARKET_COUNTS.values()))
    }
    if totals != {
        "calendar_rows": 7304,
        "calendar_closed": 2143,
        "calendar_open": 5161,
        "holiday_modified_abstentions": 163,
        "source_unobservable_abstentions": 6,
        "eligible_fold_sessions": 4992,
    }:
        raise IntegrityError("aggregate calendar accounting changed")
    return {
        "totals": totals,
        "by_market": by_market,
        "by_market_year": dict(sorted(by_market_year.items())),
        "inventory_sha256": sha256_json(inventory),
        "inventory_record_count": len(inventory),
    }


def _validate_failure_evidence(*, root: Path) -> None:
    readiness = _load(root / READINESS_REPORT_PATH)
    tier1 = _load(root / TIER1_CERTIFICATE_PATH)
    diagnostic = _load(root / DIAGNOSTIC_PATH)
    provenance = _load(root / PROVENANCE_PATH)
    reconciliation = diagnostic.get("reconciliation")
    if (
        readiness.get("report_id") != READINESS_REPORT_ID
        or readiness.get("mechanism_id") != PREDECESSOR_ID
        or readiness.get("state") != "SEALED_UNPUBLISHED_ROW_CERTIFIED_READINESS"
        or readiness.get("pilot_decision") != "PASS"
        or readiness.get("tier1_decision") != "FAIL"
        or readiness.get("combined_registration_ready") is not False
        or tier1.get("certificate_id") != TIER1_CERTIFICATE_ID
        or tier1.get("overall_decision") != "FAIL"
        or not isinstance(tier1.get("fold_market_results"), list)
        or len(tier1["fold_market_results"]) != 32
        or diagnostic.get("report_id") != DIAGNOSTIC_ID
        or diagnostic.get("mechanism_id") != PREDECESSOR_ID
        or sha256_file(root / DIAGNOSTIC_PATH) != DIAGNOSTIC_SHA256
        or not isinstance(reconciliation, Mapping)
        or reconciliation.get("unique_feature_gap_session_count") != 12
        or reconciliation.get("classification_counts") != {
            "INSUFFICIENT_REPORTED_BAR_HISTORY": 3,
            "SOURCE_SESSION_ABSENT": 8,
            "WINDOW_END_TOO_EARLY": 1,
        }
        or provenance.get("report_id") != PROVENANCE_ID
        or provenance.get("mechanism_id") != PREDECESSOR_ID
        or sha256_file(root / PROVENANCE_PATH) != PROVENANCE_SHA256
        or provenance.get("classification_counts") != {
            "CALENDAR_CLOSURE": 2,
            "RAW_SOURCE_ABSENCE": 6,
        }
    ):
        raise IntegrityError("source-incompatibility evidence changed")


def _immutable_evidence_bindings(*, root: Path) -> dict[str, str]:
    expected = {
        PREDECESSOR_PATH.as_posix(): PREDECESSOR_SHA256,
        READINESS_REPORT_PATH.as_posix(): sha256_file(root / READINESS_REPORT_PATH),
        TIER1_CERTIFICATE_PATH.as_posix(): sha256_file(root / TIER1_CERTIFICATE_PATH),
        DIAGNOSTIC_PATH.as_posix(): DIAGNOSTIC_SHA256,
        PROVENANCE_PATH.as_posix(): PROVENANCE_SHA256,
        CALENDAR_PATH.as_posix(): CALENDAR_SHA256,
        CALENDAR_REGISTRATION_PATH.as_posix(): CALENDAR_REGISTRATION_SHA256,
        CALENDAR_EVENT_PATH.as_posix(): CALENDAR_EVENT_SHA256,
    }
    for path, digest in expected.items():
        if sha256_file(root / path) != digest:
            raise IntegrityError(f"immutable evidence binding changed: {path}")
    return dict(sorted(expected.items()))


def build_closure(*, root: Path) -> dict[str, object]:
    _load_predecessor(root=root)
    _validate_failure_evidence(root=root)
    calendar = _load_calendar(root=root, require_active_context=True)
    accounting = build_calendar_accounting(calendar)
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_pre_registration_source_incompatibility_closure/1.0.0",
        "state": "PREPARED_UNPUBLISHED_TERMINAL_CLOSURE",
        "classification": "PRE_REGISTRATION_SOURCE_INCOMPATIBLE",
        "classification_detail": "FOLD_CONSTRUCTION_PRECEDED_FULL_REGULAR_AND_SOURCE_OBSERVABILITY_ELIGIBILITY",
        "mechanism_id": PREDECESSOR_ID,
        "mechanism_path": PREDECESSOR_PATH.as_posix(),
        "mechanism_sha256": PREDECESSOR_SHA256,
        "tier0_status": "PASS_SYNTHETIC_ONLY",
        "pilot_registration_status": "FORBIDDEN",
        "historical_execution_status": "READINESS_ONLY_NO_ECONOMIC_EXECUTION",
        "economic_result": "NOT_PRODUCED",
        "strategy_failure": False,
        "profitability_conclusion": False,
        "source_compatibility_conclusion": "FAIL",
        "exact_failure_reconciliation": {
            "failed_fold_market_results": 9,
            "unique_feature_gap_market_sessions": 12,
            "calendar_closures_corrected": 2,
            "explicit_source_unobservable_sessions": 6,
            "holiday_modified_sparse_sessions": 4,
            "late_availability_identity_economics_or_geometry_faults": 0,
        },
        "corrective_decision": {
            "authoritative_full_regular_eligibility_before_folds": True,
            "explicit_source_observability_eligibility_before_folds": True,
            "all_excluded_sessions_remain_in_checkpoint_accounting": True,
            "new_100_percent_row_readiness_census_required": True,
        },
        "predata_calendar_accounting": accounting,
        "incremental_retry_allowed": False,
        "parameter_rescue_allowed": False,
        "new_counted_mechanism_required": True,
        "preservation": "MECHANISM_READINESS_DIAGNOSTIC_PROVENANCE_AND_CALENDAR_BYTES_UNCHANGED",
        "publication_authorized": False,
        "activation_authorized": False,
        "bindings": _immutable_evidence_bindings(root=root),
    }
    return {**core, "closure_id": sha256_json(core)}


def closure_path(closure: Mapping[str, object]) -> Path:
    return CLOSURE_ROOT / str(closure["closure_id"]) / "closure.json"


def validate_closure(closure: Mapping[str, object], *, root: Path) -> dict[str, object]:
    core = {key: value for key, value in closure.items() if key != "closure_id"}
    expected_bindings = _immutable_evidence_bindings(root=root)
    correction = closure.get("corrective_decision")
    reconciliation = closure.get("exact_failure_reconciliation")
    if (
        closure.get("closure_id") != sha256_json(core)
        or closure.get("classification") != "PRE_REGISTRATION_SOURCE_INCOMPATIBLE"
        or closure.get("mechanism_id") != PREDECESSOR_ID
        or closure.get("strategy_failure") is not False
        or closure.get("economic_result") != "NOT_PRODUCED"
        or closure.get("profitability_conclusion") is not False
        or closure.get("pilot_registration_status") != "FORBIDDEN"
        or closure.get("incremental_retry_allowed") is not False
        or closure.get("parameter_rescue_allowed") is not False
        or closure.get("publication_authorized") is not False
        or closure.get("activation_authorized") is not False
        or not isinstance(correction, Mapping)
        or correction.get("all_excluded_sessions_remain_in_checkpoint_accounting") is not True
        or correction.get("new_100_percent_row_readiness_census_required") is not True
        or not isinstance(reconciliation, Mapping)
        or reconciliation.get("unique_feature_gap_market_sessions") != 12
        or closure.get("bindings") != expected_bindings
    ):
        raise IntegrityError("source-incompatibility closure is invalid")
    return dict(closure)


def _calendar_authority() -> dict[str, object]:
    return {
        "calendar_id": CALENDAR_ID,
        "calendar_path": CALENDAR_PATH.as_posix(),
        "calendar_sha256": CALENDAR_SHA256,
        "registration_id": CALENDAR_REGISTRATION_ID,
        "registration_path": CALENDAR_REGISTRATION_PATH.as_posix(),
        "registration_sha256": CALENDAR_REGISTRATION_SHA256,
        "activation_event_id": CALENDAR_EVENT_ID,
        "activation_event_path": CALENDAR_EVENT_PATH.as_posix(),
        "activation_event_sha256": CALENDAR_EVENT_SHA256,
        "observed_active_pointer_id_at_preparation": ACTIVE_CALENDAR_POINTER_ID,
        "observed_active_pointer_sha256_at_preparation": ACTIVE_CALENDAR_POINTER_SHA256,
        "mutable_pointer_is_not_an_immutable_mechanism_binding": True,
    }


def _successor_fold_construction(predecessor: Mapping[str, object]) -> dict[str, object]:
    predecessor_folds = predecessor.get("fold_construction")
    if not isinstance(predecessor_folds, Mapping):
        raise IntegrityError("predecessor fold construction is missing")
    folds = copy.deepcopy(dict(predecessor_folds))
    folds.update({
        "calendar_basis": "FULL_REGULAR_SOURCE_OBSERVABLE_SESSIONS_BEFORE_FOLD_CONSTRUCTION",
        "eligibility_applied_before_fold_construction": True,
        "excluded_sessions_retained_in_checkpoint_accounting": True,
    })
    return folds


def _successor_source_gate(predecessor: Mapping[str, object]) -> dict[str, object]:
    predecessor_gate = predecessor.get("source_compatibility_gate")
    if not isinstance(predecessor_gate, Mapping):
        raise IntegrityError("predecessor source gate is missing")
    gate = copy.deepcopy(dict(predecessor_gate))
    gate.update({
        "status": "UNPROVEN_REQUIRES_NEW_100_PERCENT_ROW_CERTIFIED_CENSUS",
        "calendar_and_source_eligibility_before_folds": True,
        "full_regular_session_required": True,
        "explicit_source_unobservable_session_eligible": False,
        "holiday_modified_session_eligible": False,
        "excluded_checkpoint_accounting_percent": 100,
        "row_census_may_silently_drop_sessions": False,
    })
    return gate


def _session_eligibility(accounting: Mapping[str, object]) -> dict[str, object]:
    return {
        "checkpoint": CHECKPOINT,
        "decision_source": "HASH_BOUND_AUTHORITATIVE_CALENDAR_AND_EXPLICIT_SOURCE_OBSERVABILITY_ONLY",
        "accounting_universe": "EVERY_BOUND_CALENDAR_MARKET_DATE_CHECKPOINT_ROW",
        "fold_eligible_if_all": [
            "CHECKPOINT_OPEN_TRUE",
            "DISPOSITION_EQUALS_REGULAR_WEEKDAY_REFERENCE_RULE",
            "NO_MATCHING_EXPLICIT_SOURCE_UNOBSERVABLE_RECORD",
        ],
        "calendar_closed_disposition": CALENDAR_CLOSED,
        "holiday_modified_disposition": HOLIDAY_ABSTENTION,
        "source_unobservable_disposition": SOURCE_ABSTENTION,
        "eligible_disposition": ELIGIBLE,
        "unknown_open_calendar_disposition": "FAIL_CLOSED",
        "candidate_and_active_baselines_use_same_eligibility_predicate": True,
        "active_baselines_keep_independent_scheduling": True,
        "silent_drop_allowed": False,
        "eligibility_selected_using_returns": False,
        "row_level_source_completeness_claim": False,
        "later_100_percent_row_readiness_census_required": True,
        "predata_calendar_accounting": copy.deepcopy(dict(accounting)),
    }


def _source_design(closure: Mapping[str, object]) -> dict[str, object]:
    return {
        "classification": "FULL_REGULAR_AND_EXPLICIT_SOURCE_OBSERVABILITY_SUCCESSOR",
        "predecessor_mechanism_id": PREDECESSOR_ID,
        "predecessor_closure_id": closure["closure_id"],
        "only_semantic_change": "SESSION_ELIGIBILITY_BEFORE_FOLD_CONSTRUCTION",
        "economic_parameters_changed": False,
        "cost_risk_baseline_or_promotion_standard_changed": False,
        "no_future_complete_path_filter": True,
        "all_excluded_sessions_explicitly_accounted": True,
        "new_100_percent_census_required": True,
        "no_return_or_economic_outcome_used": True,
    }


def _successor_bindings(*, root: Path, closure: Mapping[str, object]) -> dict[str, str]:
    return dict(sorted({
        PREDECESSOR_PATH.as_posix(): PREDECESSOR_SHA256,
        closure_path(closure).as_posix(): hashlib.sha256(
            canonical_bytes(closure) + b"\n").hexdigest(),
        CALENDAR_PATH.as_posix(): CALENDAR_SHA256,
        CALENDAR_REGISTRATION_PATH.as_posix(): CALENDAR_REGISTRATION_SHA256,
        CALENDAR_EVENT_PATH.as_posix(): CALENDAR_EVENT_SHA256,
        DIAGNOSTIC_PATH.as_posix(): DIAGNOSTIC_SHA256,
        PROVENANCE_PATH.as_posix(): PROVENANCE_SHA256,
        MODULE_PATH.as_posix(): sha256_file(root / MODULE_PATH),
        PREPARE_SCRIPT_PATH.as_posix(): sha256_file(root / PREPARE_SCRIPT_PATH),
    }.items()))


def build_successor(*, root: Path, closure: Mapping[str, object]) -> dict[str, object]:
    predecessor = _load_predecessor(root=root)
    validate_closure(closure, root=root)
    calendar = _load_calendar(root=root, require_active_context=True)
    accounting = build_calendar_accounting(calendar)
    core = copy.deepcopy({key: value for key, value in predecessor.items()
                          if key != "mechanism_id"})
    fold_construction = _successor_fold_construction(predecessor)
    gate = _successor_source_gate(predecessor)
    core.update({
        "schema_version": "alpha_ladder_full_regular_source_observable_successor/1.0.0",
        "state": "PREPARED_UNPUBLISHED_UNREGISTERED_TIER0_RESTART_REQUIRED",
        "classification": "NEW_COUNTED_ALPHA_MECHANISM_SOURCE_COMPATIBILITY_UNPROVEN",
        "restart_stage": "tier_0",
        "predecessor": {
            "mechanism_id": PREDECESSOR_ID,
            "path": PREDECESSOR_PATH.as_posix(),
            "sha256": PREDECESSOR_SHA256,
            "closure_id": closure["closure_id"],
            "closure_path": closure_path(closure).as_posix(),
        },
        "fold_construction": fold_construction,
        "source_compatibility_gate": gate,
        "session_eligibility": _session_eligibility(accounting),
        "calendar_authority": _calendar_authority(),
        "source_design_binding": _source_design(closure),
        "outcome_access": copy.deepcopy(OUTCOME_ACCESS),
        "authority": copy.deepcopy(AUTHORITY),
        "bindings": _successor_bindings(root=root, closure=closure),
    })
    return {**core, "mechanism_id": sha256_json(core)}


def successor_path(mechanism: Mapping[str, object]) -> Path:
    return SUCCESSOR_ROOT / str(mechanism["mechanism_id"]) / "mechanism.json"


def validate_successor(
    mechanism: Mapping[str, object],
    *,
    predecessor: Mapping[str, object],
    closure: Mapping[str, object],
    root: Path,
) -> dict[str, object]:
    core = {key: value for key, value in mechanism.items() if key != "mechanism_id"}
    allowed_changes = {
        "schema_version", "state", "classification", "restart_stage", "predecessor",
        "fold_construction", "source_compatibility_gate", "session_eligibility",
        "calendar_authority", "source_design_binding", "outcome_access", "authority",
        "bindings",
    }
    for field in set(predecessor) - {"mechanism_id"} - allowed_changes:
        if mechanism.get(field) != predecessor.get(field):
            raise UnauthorizedOperation(f"successor changed retained field: {field}")
    predecessor_folds = predecessor.get("fold_construction")
    folds = mechanism.get("fold_construction")
    if not isinstance(predecessor_folds, Mapping) or not isinstance(folds, Mapping):
        raise IntegrityError("fold construction is missing")
    for field, value in predecessor_folds.items():
        if field != "calendar_basis" and folds.get(field) != value:
            raise UnauthorizedOperation(f"successor changed locked fold field: {field}")
    gate = mechanism.get("source_compatibility_gate")
    eligibility = mechanism.get("session_eligibility")
    design = mechanism.get("source_design_binding")
    predecessor_binding = mechanism.get("predecessor")
    expected_bindings = _successor_bindings(root=root, closure=closure)
    expected_predecessor = {
        "mechanism_id": PREDECESSOR_ID,
        "path": PREDECESSOR_PATH.as_posix(),
        "sha256": PREDECESSOR_SHA256,
        "closure_id": closure["closure_id"],
        "closure_path": closure_path(closure).as_posix(),
    }
    calendar = _load_calendar(root=root, require_active_context=False)
    expected_accounting = build_calendar_accounting(calendar)
    if (
        mechanism.get("mechanism_id") != sha256_json(core)
        or mechanism.get("state")
        != "PREPARED_UNPUBLISHED_UNREGISTERED_TIER0_RESTART_REQUIRED"
        or mechanism.get("classification")
        != "NEW_COUNTED_ALPHA_MECHANISM_SOURCE_COMPATIBILITY_UNPROVEN"
        or mechanism.get("restart_stage") != "tier_0"
        or predecessor_binding != expected_predecessor
        or dict(folds) != _successor_fold_construction(predecessor)
        or not isinstance(gate, Mapping)
        or dict(gate) != _successor_source_gate(predecessor)
        or not isinstance(eligibility, Mapping)
        or dict(eligibility) != _session_eligibility(expected_accounting)
        or not isinstance(design, Mapping)
        or dict(design) != _source_design(closure)
        or mechanism.get("calendar_authority") != _calendar_authority()
        or mechanism.get("outcome_access") != OUTCOME_ACCESS
        or mechanism.get("authority") != AUTHORITY
        or mechanism.get("bindings") != expected_bindings
    ):
        raise IntegrityError("full-regular source-observable successor is not fail closed")
    validate_closure(closure, root=root)
    return dict(mechanism)
