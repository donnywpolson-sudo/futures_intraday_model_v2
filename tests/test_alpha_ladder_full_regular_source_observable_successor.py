from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_full_regular_source_observable_successor import (
    CALENDAR_CLOSED,
    CORE_MARKETS,
    ELIGIBLE,
    EXPECTED_MARKET_COUNTS,
    HOLIDAY_ABSTENTION,
    PREDECESSOR_ID,
    PREDECESSOR_PATH,
    PREDECESSOR_SHA256,
    SOURCE_ABSTENTION,
    build_closure,
    build_successor,
    classify_calendar_session,
    closure_path,
    successor_path,
    validate_closure,
    validate_successor,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]


def _read(path: Path) -> dict[str, object]:
    raw = path.read_bytes()
    assert raw.endswith(b"\n")
    assert not raw[:-1].endswith(b"\n")
    value = json.loads(raw)
    assert isinstance(value, dict)
    return value


def _resign(payload: dict[str, object], identity_field: str) -> None:
    payload[identity_field] = sha256_json(
        {key: value for key, value in payload.items() if key != identity_field}
    )


@pytest.fixture(scope="module")
def prepared() -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    predecessor = _read(ROOT / PREDECESSOR_PATH)
    closure = build_closure(root=ROOT)
    successor = build_successor(root=ROOT, closure=closure)
    validate_closure(closure, root=ROOT)
    validate_successor(
        successor,
        predecessor=predecessor,
        closure=closure,
        root=ROOT,
    )
    return predecessor, closure, successor


def _row(*, opened: bool, disposition: str, market: str = "ES",
         trade_date: str = "2020-01-02") -> dict[str, object]:
    return {
        "market": market,
        "trade_date": trade_date,
        "checkpoint_open": {"10:00": opened},
        "disposition": {"10:00": disposition},
    }


def test_calendar_classifier_is_general_and_price_free() -> None:
    source_key = frozenset({("ES", "2020-01-02", "10:00")})
    assert classify_calendar_session(
        _row(opened=False, disposition="EXACT_CME_FAMILY_SCHEDULE"),
        source_unobservable_keys=frozenset(),
    ) == CALENDAR_CLOSED
    assert classify_calendar_session(
        _row(opened=True, disposition="EXACT_CME_FAMILY_SCHEDULE"),
        source_unobservable_keys=frozenset(),
    ) == HOLIDAY_ABSTENTION
    assert classify_calendar_session(
        _row(opened=True, disposition="REGULAR_WEEKDAY_REFERENCE_RULE"),
        source_unobservable_keys=source_key,
    ) == SOURCE_ABSTENTION
    assert classify_calendar_session(
        _row(opened=True, disposition="REGULAR_WEEKDAY_REFERENCE_RULE"),
        source_unobservable_keys=frozenset(),
    ) == ELIGIBLE
    with pytest.raises(IntegrityError, match="unknown open calendar disposition"):
        classify_calendar_session(
            _row(opened=True, disposition="UNRECOGNIZED"),
            source_unobservable_keys=frozenset(),
        )


def test_every_calendar_row_and_exclusion_is_accounted(
    prepared: tuple[dict[str, object], dict[str, object], dict[str, object]],
) -> None:
    _, closure, successor = prepared
    accounting = successor["session_eligibility"]["predata_calendar_accounting"]
    assert isinstance(accounting, Mapping)
    assert accounting["inventory_record_count"] == 7304
    assert accounting["totals"] == {
        "calendar_rows": 7304,
        "calendar_closed": 2143,
        "calendar_open": 5161,
        "holiday_modified_abstentions": 163,
        "source_unobservable_abstentions": 6,
        "eligible_fold_sessions": 4992,
    }
    assert accounting["by_market"] == EXPECTED_MARKET_COUNTS
    assert len(accounting["by_market_year"]) == len(CORE_MARKETS) * 5
    assert closure["predata_calendar_accounting"] == accounting
    for counts in accounting["by_market"].values():
        assert counts["calendar_closed"] + counts["calendar_open"] == counts["calendar_rows"]
        assert (
            counts["holiday_modified_abstentions"]
            + counts["source_unobservable_abstentions"]
            + counts["eligible_fold_sessions"]
        ) == counts["calendar_open"]


def test_closure_is_source_incompatibility_not_strategy_failure(
    prepared: tuple[dict[str, object], dict[str, object], dict[str, object]],
) -> None:
    _, closure, _ = prepared
    assert closure["classification"] == "PRE_REGISTRATION_SOURCE_INCOMPATIBLE"
    assert closure["mechanism_id"] == PREDECESSOR_ID
    assert closure["strategy_failure"] is False
    assert closure["profitability_conclusion"] is False
    assert closure["economic_result"] == "NOT_PRODUCED"
    assert closure["pilot_registration_status"] == "FORBIDDEN"
    assert closure["incremental_retry_allowed"] is False
    assert closure["parameter_rescue_allowed"] is False
    assert closure["exact_failure_reconciliation"] == {
        "failed_fold_market_results": 9,
        "unique_feature_gap_market_sessions": 12,
        "calendar_closures_corrected": 2,
        "explicit_source_unobservable_sessions": 6,
        "holiday_modified_sparse_sessions": 4,
        "late_availability_identity_economics_or_geometry_faults": 0,
    }


def test_successor_restarts_at_tier0_and_changes_only_session_eligibility(
    prepared: tuple[dict[str, object], dict[str, object], dict[str, object]],
) -> None:
    predecessor, closure, successor = prepared
    assert successor["mechanism_id"] != PREDECESSOR_ID
    assert successor["restart_stage"] == "tier_0"
    assert successor["state"] == "PREPARED_UNPUBLISHED_UNREGISTERED_TIER0_RESTART_REQUIRED"
    assert successor["predecessor"]["closure_id"] == closure["closure_id"]
    protected = {
        "baselines", "checkpoint", "costs", "entry_rules", "exit_rules", "features",
        "ladder_binding", "live_readiness", "metrics", "model_family",
        "model_parameters", "promotion_gates", "ranking", "research_only", "sizing",
        "statistics", "stop", "transformations",
    }
    for field in protected:
        assert successor[field] == predecessor[field], field
    predecessor_folds = predecessor["fold_construction"]
    successor_folds = successor["fold_construction"]
    for field, value in predecessor_folds.items():
        if field != "calendar_basis":
            assert successor_folds[field] == value
    assert successor_folds["calendar_basis"] == (
        "FULL_REGULAR_SOURCE_OBSERVABLE_SESSIONS_BEFORE_FOLD_CONSTRUCTION"
    )
    assert successor["source_design_binding"]["only_semantic_change"] == (
        "SESSION_ELIGIBILITY_BEFORE_FOLD_CONSTRUCTION"
    )
    assert successor["source_design_binding"]["economic_parameters_changed"] is False


def test_baselines_remain_independent_and_share_only_the_eligibility_predicate(
    prepared: tuple[dict[str, object], dict[str, object], dict[str, object]],
) -> None:
    predecessor, _, successor = prepared
    assert successor["baselines"] == predecessor["baselines"]
    assert successor["baselines"]["candidate_schedule_reuse"] is False
    assert successor["session_eligibility"][
        "candidate_and_active_baselines_use_same_eligibility_predicate"
    ] is True
    assert successor["session_eligibility"][
        "active_baselines_keep_independent_scheduling"
    ] is True


@pytest.mark.parametrize(
    ("mutation", "error_type"),
    [
        (lambda value: value["costs"].update({"round_trip_fee_usd": "0"}),
         UnauthorizedOperation),
        (lambda value: value["sizing"].update({"maximum_planned_loss_usd": "999"}),
         UnauthorizedOperation),
        (lambda value: value["promotion_gates"]["common"].update(
            {"maximum_continuous_drawdown_usd": "9999"}), UnauthorizedOperation),
        (lambda value: value["fold_construction"].update({"purge_minutes": 0}),
         UnauthorizedOperation),
        (lambda value: value["source_compatibility_gate"].update(
            {"holiday_modified_session_eligible": True}), IntegrityError),
        (lambda value: value["session_eligibility"].update({"silent_drop_allowed": True}),
         IntegrityError),
        (lambda value: value["session_eligibility"].update(
            {"eligibility_selected_using_returns": True}), IntegrityError),
    ],
)
def test_successor_fails_closed_on_drift(
    prepared: tuple[dict[str, object], dict[str, object], dict[str, object]],
    mutation: object,
    error_type: type[Exception],
) -> None:
    predecessor, closure, successor = prepared
    tampered = copy.deepcopy(successor)
    mutation(tampered)
    _resign(tampered, "mechanism_id")
    with pytest.raises(error_type):
        validate_successor(
            tampered,
            predecessor=predecessor,
            closure=closure,
            root=ROOT,
        )


def test_closure_cannot_be_reclassified_after_resigning(
    prepared: tuple[dict[str, object], dict[str, object], dict[str, object]],
) -> None:
    _, closure, _ = prepared
    tampered = copy.deepcopy(closure)
    tampered["classification"] = "STRATEGY_FAILED"
    _resign(tampered, "closure_id")
    with pytest.raises(IntegrityError, match="closure is invalid"):
        validate_closure(tampered, root=ROOT)


def test_prepared_artifacts_are_exact_create_only_outputs(
    prepared: tuple[dict[str, object], dict[str, object], dict[str, object]],
) -> None:
    predecessor, closure, successor = prepared
    closure_file = ROOT / closure_path(closure)
    successor_file = ROOT / successor_path(successor)
    assert closure_file.read_bytes() == canonical_bytes(closure) + b"\n"
    assert successor_file.read_bytes() == canonical_bytes(successor) + b"\n"
    assert sha256_file(ROOT / PREDECESSOR_PATH) == PREDECESSOR_SHA256
    assert predecessor["mechanism_id"] == PREDECESSOR_ID
    assert successor["authority"] == {
        "historical_rows": False,
        "registration": False,
        "execution": False,
        "publication": False,
        "holdout_2025": False,
        "provider_network_credentials": False,
        "trading": False,
    }
