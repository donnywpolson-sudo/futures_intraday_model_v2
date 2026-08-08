from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from futures_rebuild.cash_open_impulse_readiness import (
    BASELINES,
    COST_SCENARIOS,
    MARKETS,
    build_source_certificate,
    classify_session,
)
from futures_rebuild.cash_open_impulse_census import load_plan, required_scope
from futures_rebuild.cash_open_impulse_census_v2 import load_plan_v2, required_scope_v2
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_bracket_post_audit import CausalBar
from futures_rebuild.tier1_bracket_v4 import MarketSpec
from futures_rebuild.tier1_bracket_v5 import V5SourceRecord


CHICAGO = ZoneInfo("America/Chicago")
SPEC = MarketSpec(Decimal("0.25"), Decimal("12.5"), Decimal("50"))


def _record(market: str, session: date, clock: time, *, identity: str = "front") -> V5SourceRecord:
    local = datetime.combine(session, clock, tzinfo=CHICAGO)
    event = int(local.timestamp()) * 1_000_000_000
    key = f"{market}/{session}/{clock}/{identity}"
    return V5SourceRecord(
        market=market, exchange_session_date=session.isoformat(), disposition="ELIGIBLE",
        bar=CausalBar(
            event, event + 60_000_000_000, event + 65_000_000_000,
            Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100.5"), True,
        ),
        volume=1.0,
        actual_identity_hash=hashlib.sha256(identity.encode()).hexdigest(),
        source_row_sha256=hashlib.sha256(key.encode()).hexdigest(),
        market_spec=SPEC,
    )


def _session(market: str, session: date) -> list[V5SourceRecord]:
    clocks: set[time] = set()
    for checkpoint in (time(9), time(10, 30)):
        minute = checkpoint.hour * 60 + checkpoint.minute
        for offset in range(-30, 0):
            clocks.add(time(*divmod(minute + offset, 60)))
        for offset in range(1, 32):
            clocks.add(time(*divmod(minute + offset, 60)))
    return [_record(market, session, clock) for clock in sorted(clocks)]


def _folds(first: str, last: str) -> list[dict[str, object]]:
    return [
        {"outer_fit_session_range": [first, last],
         "outer_test_session_dates": [first, last]}
        for _ in range(8)
    ]


def test_complete_same_session_dependencies_are_causal() -> None:
    day = date(2020, 1, 2)
    result = classify_session(market="ES", session=day.isoformat(), rows=_session("ES", day))
    assert result.complete is True
    assert [item.checkpoint for item in result.opportunities] == ["09:00", "10:30"]
    assert all(item.feature_complete and item.execution_complete for item in result.opportunities)
    assert all(set(item.risk_disposition_by_scenario.values()) == {"FEASIBLE"}
               for item in result.opportunities)


def test_contemporaneous_or_missing_entry_bar_fails_closed() -> None:
    day = date(2020, 1, 2)
    rows = [item for item in _session("ES", day) if item.bar is not None
            and datetime.fromtimestamp(item.bar.event_at_ns / 1e9, tz=CHICAGO).time() != time(9, 1)]
    result = classify_session(market="ES", session=day.isoformat(), rows=rows)
    assert result.complete is False
    assert result.opportunities[0].execution_complete is False


def test_roll_inside_dependency_fails_closed() -> None:
    day = date(2020, 1, 2)
    rows = _session("ES", day)
    target = next(index for index, item in enumerate(rows)
                  if item.bar is not None
                  and datetime.fromtimestamp(item.bar.event_at_ns / 1e9, tz=CHICAGO).time() == time(8, 50))
    rows[target] = _record("ES", day, time(8, 50), identity="next")
    result = classify_session(market="ES", session=day.isoformat(), rows=rows)
    assert result.complete is False
    assert "IDENTITY" in str(result.failure)


def test_duplicate_minute_and_2025_are_rejected() -> None:
    day = date(2020, 1, 2)
    rows = _session("ES", day)
    duplicate = classify_session(
        market="ES", session=day.isoformat(), rows=[*rows, rows[0]],
    )
    assert duplicate.failure == "DUPLICATE_EXECUTABLE_MINUTE"
    with pytest.raises(UnauthorizedOperation):
        classify_session(market="ES", session="2025-01-02", rows=[])


def test_certificate_is_fail_closed_until_every_fold_market_is_complete(tmp_path) -> None:
    source = tmp_path / "source.json"
    source.write_text("immutable", encoding="utf-8")
    binding = {source.relative_to(tmp_path).as_posix(): hashlib.sha256(source.read_bytes()).hexdigest()}
    start = date(2018, 1, 2)
    sessions = [(start + timedelta(days=index)).isoformat() for index in range(568)]
    observations = []
    # Synthetic mechanics only use repeated complete dispositions; the generic
    # certificate's source binding is still real to the temporary repository.
    for market in MARKETS:
        template = classify_session(market=market, session=sessions[0], rows=_session(market, start))
        observations.extend(
            deepcopy(template).__class__(market, session, template.opportunities, True, None)
            for session in sessions
        )
    folds = [{
        "outer_fit_session_range": [sessions[0], sessions[503]],
        "outer_test_session_dates": [sessions[505], sessions[567]],
    } for _ in range(8)]
    certificate = build_source_certificate(
        protocol_id="synthetic-protocol", source_bindings=binding,
        observations=observations, outer_folds=folds,
        expected_sessions_by_market={market: sessions for market in MARKETS},
    )
    assert certificate["overall_decision"] == "PASS"
    assert certificate["requirements"]["required_baselines"] == list(BASELINES)
    assert certificate["requirements"]["required_cost_scenarios"] == list(COST_SCENARIOS)

    damaged = [item for item in observations
               if not (item.market == "ES" and item.session == sessions[520])]
    failed = build_source_certificate(
        protocol_id="synthetic-protocol", source_bindings=binding,
        observations=damaged, outer_folds=folds,
        expected_sessions_by_market={market: sessions for market in MARKETS},
    )
    assert failed["overall_decision"] == "FAIL"
    es = next(item for item in failed["fold_market_results"]
              if item["fold_id"] == "fold-0" and item["market"] == "ES")
    assert "MINIMUM_FEATURE_COMPLETE_EVALUATION_SESSIONS" in es["failed_gates"]
    assert "PROMOTION_PATH_COMPUTABLE" in es["failed_gates"]


def test_unknown_or_changed_bound_source_is_detected(tmp_path) -> None:
    from futures_rebuild.cash_open_impulse_readiness import validate_bound_files

    source = tmp_path / "source"
    source.write_bytes(b"a")
    bindings = {"source": hashlib.sha256(b"a").hexdigest()}
    validate_bound_files(tmp_path, bindings)
    source.write_bytes(b"b")
    with pytest.raises(IntegrityError, match="bound source changed"):
        validate_bound_files(tmp_path, bindings)


def test_pre_registration_protocol_and_single_use_plan_are_hash_bound() -> None:
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    protocol = json.loads(
        (root / "configs/cash_open_impulse_pre_registration_protocol.json")
        .read_text(encoding="utf-8")
    )
    assert sha256_json(protocol) == (
        "3b8e09d65015afd33fc033aa72c8bb0be22425cafac8b8b145eeccb639258067"
    )
    assert protocol["state"] == "PREPARED_NOT_REGISTERED_ROW_CERTIFICATION_REQUIRED"
    assert protocol["authority"]["historical_row_read"] is False
    plan_path = root / "configs/cash_open_impulse_fold_readiness_census_plan.json"
    assert sha256_file(plan_path) == (
        "8ffc086fb96a46e5c6f22ac7fc441db87036759111180c2f45155c7de996ccf7"
    )
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan["plan_id"] == sha256_json(
        {key: value for key, value in plan.items() if key != "plan_id"}
    )
    from futures_rebuild.research_gateway_policy import require_current_real_history_operation
    with pytest.raises(UnauthorizedOperation, match="retired outside"):
        require_current_real_history_operation(plan["operation"], {})


def test_host_successor_preserves_consumed_attempt_and_changes_no_research_semantics() -> None:
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    plan_path = root / "configs/cash_open_impulse_fold_readiness_census_v2_plan.json"
    assert sha256_file(plan_path) == (
        "5d1327839614611bd4adf9356386e141803974da093c456eb1a9fe5cd97a9b0e"
    )
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan["plan_id"] == sha256_json(
        {key: value for key, value in plan.items() if key != "plan_id"}
    )
    predecessor = plan["consumed_predecessor"]
    assert predecessor["authorization_consumed"] is True
    assert predecessor["workers_started"] is False
    assert predecessor["decoded_rows"] == 0
    assert predecessor["output_created"] is False
    assert predecessor["retry_authorized"] is False
    assert plan["research_semantics_changed"] is False
    from futures_rebuild.research_gateway_policy import require_current_real_history_operation
    with pytest.raises(UnauthorizedOperation, match="retired outside"):
        require_current_real_history_operation(plan["operation"], {})
