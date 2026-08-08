from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_calendar_observability_successor import (
    ACTIVE_POINTER_PATH,
    ACTIVE_POINTER_SHA256,
    CALENDAR_CORRECTIONS,
    CHECKPOINTS,
    SOURCE_UNOBSERVABLE,
    build_successor,
    source_disposition,
    validate_successor,
)
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
PREPARED_PATH = ROOT / (
    "state/unpublished_evidence/alpha_ladder_calendar_observability_successor/"
    "ddbe0c706d6568d8d7ddefd830677d73978b428d8a99925290310224f673a7f9/"
    "historical_calendar_successor.json"
)
PREPARED_SHA256 = (
    "efdf4f765e44ac2f312dce62b7145bb1ed70d01fd8c76fb5bfb3f32652f1a632"
)


@pytest.fixture(scope="module")
def successor():
    prepared = json.loads(PREPARED_PATH.read_text(encoding="utf-8"))
    if sha256_file(ROOT / ACTIVE_POINTER_PATH) == ACTIVE_POINTER_SHA256:
        assert build_successor(root=ROOT) == prepared
    else:
        from futures_rebuild.alpha_ladder_calendar_observability_publication_v2 import (
            _validate_successor_transition_stable,
        )

        _validate_successor_transition_stable(prepared, root=ROOT)
    return prepared


def test_successor_is_hash_bound_inactive_and_blocks_registration(successor):
    assert successor["calendar_id"] == sha256_json(
        {key: value for key, value in successor.items() if key != "calendar_id"}
    )
    assert successor["status"] == "PREPARED_INACTIVE_UNPUBLISHED"
    assert successor["authority"]["active"] is False
    assert successor["authority"]["historical_rows_reread"] is False
    assert successor["authority"]["mechanism_registered"] is False
    assert successor["registration_gate"]["registration_allowed"] is False
    assert successor["registration_gate"][
        "source_unobservable_sessions_may_be_silently_removed"
    ] is False


def test_prepared_successor_exact_bytes_validate():
    assert sha256_file(PREPARED_PATH) == PREPARED_SHA256
    prepared = json.loads(PREPARED_PATH.read_text(encoding="utf-8"))
    if sha256_file(ROOT / ACTIVE_POINTER_PATH) == ACTIVE_POINTER_SHA256:
        validate_successor(prepared, root=ROOT)
    else:
        from futures_rebuild.alpha_ladder_calendar_observability_publication_v2 import (
            _validate_successor_transition_stable,
        )

        _validate_successor_transition_stable(prepared, root=ROOT)


def test_only_two_proven_calendar_admissions_change(successor):
    changed = {
        (item["market"], item["trade_date"])
        for item in successor["calendar_corrections"]
    }
    assert changed == set(CALENDAR_CORRECTIONS)
    by_key = {
        (item["market"], item["trade_date"]): item
        for item in successor["calendar_rows"]
    }
    for key, correction in CALENDAR_CORRECTIONS.items():
        assert by_key[key]["checkpoint_open"] == {
            checkpoint: False for checkpoint in CHECKPOINTS
        }
        assert by_key[key]["disposition"] == {
            checkpoint: correction["disposition"] for checkpoint in CHECKPOINTS
        }
        assert source_disposition(
            successor, market=key[0], trade_date=key[1]
        ) == "CALENDAR_CLOSED"


def test_six_source_gaps_remain_calendar_open_and_explicit(successor):
    records = {
        (item["market"], item["trade_date"]): item
        for item in successor["source_observability_records"]
    }
    rows = {
        (item["market"], item["trade_date"]): item
        for item in successor["calendar_rows"]
    }
    assert set(records) == set(SOURCE_UNOBSERVABLE)
    for key in SOURCE_UNOBSERVABLE:
        record = records[key]
        assert rows[key]["checkpoint_open"]["10:00"] is True
        assert record["calendar_state"] == "OPEN"
        assert record["source_state"] == "SOURCE_UNOBSERVABLE"
        assert record["research_disposition"] == (
            "EXPLICIT_SOURCE_UNOBSERVABLE_ABSTENTION"
        )
        assert record["verified_no_trade_claim"] is False
        assert record["silent_drop_allowed"] is False
        assert record["required_checkpoint_accounting"] is True
        assert source_disposition(
            successor, market=key[0], trade_date=key[1]
        ) == "SOURCE_UNOBSERVABLE_EXPLICIT_ABSTENTION"


def test_unscoped_session_is_not_falsely_certified_observable(successor):
    assert source_disposition(
        successor, market="ES", trade_date="2020-07-01"
    ) == "NO_OVERRIDE_REQUIRES_ROW_CERTIFICATION"


def test_calendar_tampering_fails_closed(successor):
    tampered = deepcopy(successor)
    row = next(
        item
        for item in tampered["calendar_rows"]
        if item["market"] == "CL" and item["trade_date"] == "2020-02-28"
    )
    row["checkpoint_open"]["10:00"] = False
    core = {key: value for key, value in tampered.items() if key != "calendar_id"}
    tampered["calendar_id"] = sha256_json(core)
    with pytest.raises(IntegrityError, match="unproven calendar change"):
        validate_successor(tampered, root=ROOT, verify_bindings=False)


def test_source_gap_cannot_be_relabelled_verified_no_trade(successor):
    tampered = deepcopy(successor)
    tampered["source_observability_records"][0]["verified_no_trade_claim"] = True
    core = {key: value for key, value in tampered.items() if key != "calendar_id"}
    tampered["calendar_id"] = sha256_json(core)
    with pytest.raises(IntegrityError, match="source-unobservable scope"):
        validate_successor(tampered, root=ROOT, verify_bindings=False)


def test_source_gap_cannot_be_silently_dropped(successor):
    tampered = deepcopy(successor)
    tampered["source_observability_records"].pop()
    tampered["source_observability_record_count"] = 5
    core = {key: value for key, value in tampered.items() if key != "calendar_id"}
    tampered["calendar_id"] = sha256_json(core)
    with pytest.raises(IntegrityError, match="identity is invalid"):
        validate_successor(tampered, root=ROOT, verify_bindings=False)


def test_prepared_scope_never_reaches_2025(successor):
    years = {item["trade_date"][:4] for item in successor["calendar_rows"]}
    assert years == {"2018", "2019", "2020", "2021", "2022"}
    assert successor["authority"]["year_2025_accessed"] is False
