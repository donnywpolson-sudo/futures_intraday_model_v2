"""Generated records only; the calendar double is not historical certification."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import numpy as np
import pytest

from futures_rebuild import rolling_hour_preparation as prep
from futures_rebuild import rolling_hour_research as r
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.foundation.market_state import StatisticsRolePolicy
from futures_rebuild.foundation.records import ProviderDefinition, StatusRecordV1, StatisticsRecordV1, INT64_NULL
from futures_rebuild.rolling_hour_reference_audit import audit_source_joins
from futures_rebuild.rolling_hour_source import adapt_source_sessions

pytestmark = pytest.mark.current


def inputs(monkeypatch, market="ES", minutes=125):
    session, rows, old_quotes = prep.synthetic_session(market=market, minutes=minutes)
    contract, release, quote_release = [sha256_json({"generated_join_test": name}) for name in ("contract", "price", "quote")]
    start = rows[0]["bar_start_ns"]
    day = session.trade_date.isoformat()
    definition = ProviderDefinition(dataset="GLBX.MDP3", market=market, publisher_id=1,
        instrument_id=123, instrument_id_date_utc=day, ts_event_ns=start-1, ts_recv_ns=start-1,
        activation_ns=start-r.MINUTE, expiration_ns=start+1440*r.MINUTE, security_update_action="ADD",
        instrument_class="FUTURE", security_type="FUT", raw_symbol=rows[0]["actual_contract"],
        exchange="XCME", currency="USD", min_price_increment_nano=rows[0]["min_price_increment_nano"],
        unit_of_measure_qty_nano=50*10**9, unit_of_measure="IPNT", source_release_id=release,
        source_manifest_sha256=contract, source_file_path="generated/definition.dbn.zst",
        source_file_sha256=rows[0]["definition_source_file_sha256"], row_ordinal=0,
        row_sha256=rows[0]["definition_row_sha256"])
    for row in rows:
        row.update(source_contract_id=contract, source_release_id=release, publisher_id=1,
                   instrument_id=123, source_file_path="generated/price.dbn.zst")
    common = dict(dataset="GLBX.MDP3", market=market, publisher_id=1, instrument_id=123,
        instrument_id_date_utc=day, ts_event_ns=start-1, ts_recv_ns=start-1,
        source_release_id=release, source_manifest_sha256=contract, row_ordinal=0)
    status = StatusRecordV1(**common, action="TRADING", reason="NONE", trading_event="NONE",
        is_trading="YES", is_quoting="YES", is_short_sell_restricted="NO",
        source_file_path="generated/status.dbn.zst", source_file_sha256="c"*64, row_sha256="d"*64)
    statistic = StatisticsRecordV1(**common, ts_ref_ns=start-1, ts_in_delta=1, stat_type="OPEN_INTEREST",
        update_action="NEW", price_nano=10**9, quantity=100, sequence=0, channel_id=1, flags=0,
        source_file_path="generated/statistics.dbn.zst", source_file_sha256="e"*64, row_sha256="f"*64)
    class GeneratedCalendar:
        receipt = SimpleNamespace(release_id=sha256_json({"generated_calendar": market}))
        def __init__(self):
            self.sessions = {(market, session.trade_date): session}
        def verify(self):
            pass
    monkeypatch.setattr(r, "VerifiedExchangeCalendar", GeneratedCalendar)
    calendar = GeneratedCalendar()
    grid = r.opportunity_grid(session, calendar=calendar, source_contract_id=contract, source_release_id=release)
    quotes = {s["opportunity_id"]: deepcopy(q) for s, q in zip(grid, old_quotes.values(), strict=True)}
    for fields in quotes.values():
        for quote in fields.values():
            quote["source_release_id"] = quote_release
    policy = prep.synthetic_policy()
    policy = r.profit.bind({**{k:v for k,v in policy.items() if k != "policy_id"}, "source_release_id": quote_release}, "policy_id")
    binding = r.profit.bind(dict(market=market, trade_date=day, source_contract_id=contract,
        source_release_id=release, calendar_release_id=calendar.receipt.release_id,
        calendar_evidence_id="1"*64, contract_selection_evidence_id="2"*64, source_certificate_id="3"*64,
        actual_contract=rows[0]["actual_contract"], calendar_known_at_ns=start-1,
        contract_selection_known_at_ns=start-1, point_in_time_evidence="CERTIFIED_ORIGINAL_PUBLICATION",
        source_file_sha256s=sorted({row["source_file_sha256"] for row in rows})), "binding_id")
    source = dict(sessions=(session,), rows_by_date={day: rows}, quotes_by_date={day: quotes},
        calendar=calendar, source_contract_id=contract, source_release_id=release, policy=policy,
        bindings={day:binding}, as_of_ns=rows[-1]["decision_eligible_at_ns"])
    reference = dict(definitions=[definition], statuses=[status], statistics=[statistic],
        statistics_roles=StatisticsRolePolicy({"OPEN_INTEREST":"LIQUIDITY_DIAGNOSTIC"}),
        reference_sources={name:dict(source_release_id=release, source_manifest_sha256=contract,
            source_file_sha256s=[record.source_file_sha256]) for name, record in
            (("definition", definition), ("status", status), ("statistics", statistic))})
    return source, reference


def first(report, role="decision"):
    return report["dates"][0]["opportunities"][0]["checks"][role]


@pytest.mark.parametrize("market", r.MARKETS)
def test_compatible_audit_preserves_predictions_scores_and_input_bytes(monkeypatch, market):
    source, reference = inputs(monkeypatch, market)
    before_hash = sha256_json({k:source[k] for k in ("rows_by_date", "quotes_by_date", "bindings", "policy")})
    before = adapt_source_sessions(**source)
    report = audit_source_joins(**source, **reference)
    after = adapt_source_sessions(**source)
    assert report["complete"] and not report["historical_execution_authorized"]
    assert report["dates"][0]["expected_minutes"] == report["dates"][0]["observed_minutes"] == 125
    assert len(report["dates"][0]["minute_joins"]) == 125
    assert len(report["dates"][0]["opportunities"]) == 3
    assert before_hash == sha256_json({k:source[k] for k in ("rows_by_date", "quotes_by_date", "bindings", "policy")})
    np.testing.assert_array_equal(before.terminal_data.features, after.terminal_data.features)
    # A deterministic 252-date fit exercises the real frozen prediction path.
    width = len(r.terminal.FEATURE_NAMES)
    x = np.arange(252*width, dtype=float).reshape(252,width) % 17
    train = SimpleNamespace(_evidence_id="4"*64, terminal_data=SimpleNamespace(features=x,
        terminal_ticks=x[:,0], trade_dates=tuple(str(i) for i in range(252)), sample_ids=tuple(map(str,range(252)))))
    model = r.fit_model(train, np.arange(252), r.candidate_definitions()[0])
    p = r.predict(model, before.terminal_data.features)
    q = r.predict(model, after.terminal_data.features)
    np.testing.assert_array_equal(p, q)
    assert r.score_predictions(p, before.opportunities, before.policy, before.calendar_dates) == r.score_predictions(q, after.opportunities, after.policy, after.calendar_dates)


def test_later_halt_old_event_and_future_statistics_do_not_rewrite_earlier_state(monkeypatch):
    source, reference = inputs(monkeypatch)
    before = audit_source_joins(**source, **reference)
    at = before["dates"][0]["opportunities"][0]["schedule"]["decision_ns"]
    reference["statuses"].append(replace(reference["statuses"][0], ts_recv_ns=at+1,
        action="HALT", is_trading="NO", row_ordinal=1, row_sha256="5"*64))
    reference["statistics"].append(replace(reference["statistics"][0], ts_recv_ns=at+1,
        stat_type="HIGHEST_BID", row_ordinal=1, row_sha256="6"*64))
    after = audit_source_joins(**source, **reference)
    assert first(before) == first(after)
    assert first(after, "entry")["status"]["status_disposition"] == "STATUS_HALTED"
    assert not after["complete"]
    assert [p["schedule"] for p in before["dates"][0]["opportunities"]] == [p["schedule"] for p in after["dates"][0]["opportunities"]]
    day = next(iter(source["rows_by_date"]))
    schedule = before["dates"][0]["opportunities"][0]["schedule"]
    feature = r.build_feature_record(source["rows_by_date"][day][:61], schedule)
    assert feature == r.build_feature_record(source["rows_by_date"][day][:61], after["dates"][0]["opportunities"][0]["schedule"])


@pytest.mark.parametrize("family", ["statuses", "statistics", "definitions"])
def test_receive_before_event_uses_receive_clock(monkeypatch, family):
    source, reference = inputs(monkeypatch)
    reference[family][0] = replace(reference[family][0], ts_event_ns=source["as_of_ns"]+r.MINUTE)
    assert audit_source_joins(**source, **reference)["complete"]


@pytest.mark.parametrize("family,change", [
    ("statuses", {"action":"HALT"}), ("statistics", {"quantity":101}),
    ("definitions", {"security_update_action":"DELETE"})])
def test_equal_receive_cross_file_conflicts_preserve_audit_and_block(monkeypatch, family, change):
    source, reference = inputs(monkeypatch)
    reference[family].append(replace(reference[family][0], source_file_path="generated/conflict.dbn.zst", row_sha256="7"*64, **change))
    report = audit_source_joins(**source, **reference)
    assert not report["complete"]
    assert len(report["dates"][0]["opportunities"]) == 3
    assert "conflict" in str(report)


@pytest.mark.parametrize("change", [{"instrument_id":124}, {"publisher_id":2},
    {"ts_recv_ns":1514735999999999999, "instrument_id_date_utc":"2017-12-31"}])
def test_status_cannot_borrow_another_contract_publisher_or_utc_date(monkeypatch, change):
    source, reference = inputs(monkeypatch)
    reference["statuses"][0] = replace(reference["statuses"][0], **change)
    report = audit_source_joins(**source, **reference)
    assert not report["complete"]
    assert first(report)["status"]["status_disposition"] == "STATUS_UNRESOLVED"


@pytest.mark.parametrize("change,expected", [
    ({"security_update_action":"DELETE"}, "deleted"),
    ({"instrument_class":"FUTURE_SPREAD"}, "outright"),
    ({"instrument_id":124}, "same-day"),
    ({"ts_recv_ns":1514735999999999999, "instrument_id_date_utc":"2017-12-31"}, "same-day"),
    ({"raw_symbol":"ESH9"}, "differs"),
    ({"ts_recv_ns":1514822400000000001}, "bar start")])
def test_definition_replay_fails_closed_before_any_dataset(monkeypatch, change, expected):
    source, reference = inputs(monkeypatch)
    reference["definitions"][0] = replace(reference["definitions"][0], **change)
    def forbidden(**kwargs):
        pytest.fail("failed reference joins reached dataset construction")
    monkeypatch.setattr("futures_rebuild.rolling_hour_reference_audit.adapt_source_sessions", forbidden)
    report = audit_source_joins(**source, **reference)
    assert not report["complete"] and expected in str(report)


def test_equal_receive_definition_modify_follows_source_ordinal(monkeypatch):
    source, reference = inputs(monkeypatch)
    old = reference["definitions"][0]
    modified = replace(old, security_update_action="MODIFY", row_ordinal=1, row_sha256="8"*64)
    reference["definitions"] = [modified, old]
    for rows in source["rows_by_date"].values():
        for row in rows:
            row["definition_row_sha256"] = modified.row_sha256
    report = audit_source_joins(**source, **reference)
    assert report["complete"]
    assert {j["definition_row_id"] for j in report["dates"][0]["minute_joins"]} == {modified.row_sha256}


def test_statistics_late_delete_and_utc_reset_do_not_backfill(monkeypatch):
    source, reference = inputs(monkeypatch)
    before = audit_source_joins(**source, **reference)
    at = before["dates"][0]["opportunities"][0]["schedule"]["decision_ns"]
    reference["statistics"].append(replace(reference["statistics"][0], ts_recv_ns=at+1,
        update_action="DELETE", row_ordinal=1, row_sha256="9"*64))
    after = audit_source_joins(**source, **reference)
    assert first(before) == first(after)
    assert first(after, "entry")["statistics"][0]["state"] == "DELETED"
    reference["statistics"] = [replace(reference["statistics"][0],
        ts_recv_ns=1514735999999999999, instrument_id_date_utc="2017-12-31")]
    reset = first(audit_source_joins(**source, **reference))
    assert reset["statistics"] == [] and reset["statistics_availability"] == "NO_RECEIVED_KEYS"


@pytest.mark.parametrize("change,state", [({"update_action":"DELETE"}, "DELETED"),
    ({"update_action":"UNKNOWN_17"}, "UNKNOWN_UPDATE_ACTION"),
    ({"price_nano":INT64_NULL}, "NEW_UNKNOWN_VALUE"),
    ({"stat_type":"HIGHEST_BID"}, "NEW_UNDECLARED_ROLE")])
def test_statistics_are_diagnostics_never_features_or_quotes(monkeypatch, change, state):
    source, reference = inputs(monkeypatch)
    reference["statistics"][0] = replace(reference["statistics"][0], **change)
    report = audit_source_joins(**source, **reference)
    diagnostic = first(report)["statistics"][0]
    assert report["complete"] and diagnostic["state"] == state
    assert not diagnostic["feature_eligible"] and "price_nano" not in diagnostic and "quantity" not in diagnostic
    source["quotes_by_date"][next(iter(source["quotes_by_date"]))] = {}
    assert not audit_source_joins(**source, **reference)["complete"]


@pytest.mark.parametrize("failure", ["retrospective", "publication", "late_selection", "quotes", "minutes", "maturity"])
def test_existing_handoff_requirements_remain_blocked(monkeypatch, failure):
    source, reference = inputs(monkeypatch)
    day = next(iter(source["bindings"]))
    binding = source["bindings"][day]
    if failure in ("retrospective", "publication", "late_selection"):
        binding.pop("binding_id")
        if failure == "retrospective":
            binding["point_in_time_evidence"] = "VERIFIED_RETROSPECTIVE"
        elif failure == "publication":
            binding.pop("calendar_known_at_ns")
        else:
            binding["contract_selection_known_at_ns"] = source["as_of_ns"]
        source["bindings"][day] = r.profit.bind(binding, "binding_id")
    elif failure == "quotes":
        source["quotes_by_date"][day] = {}
    elif failure == "minutes":
        source["rows_by_date"][day].pop()
    else:
        source["as_of_ns"] = source["rows_by_date"][day][70]["decision_eligible_at_ns"]
    report = audit_source_joins(**source, **reference)
    assert not report["complete"]
    assert len(report["dates"][0]["opportunities"]) == 3
    assert report["errors"] or report["dates"][0]["reasons"]


def test_early_close_and_empty_date_are_retained_reproducibly(monkeypatch):
    source, reference = inputs(monkeypatch, minutes=123)
    session = source["sessions"][0]
    empty = replace(session, trade_date=session.trade_date+timedelta(days=1),
        intervals=tuple(replace(i, state="CLOSED", starts_at_utc=i.starts_at_utc+timedelta(days=1),
                                ends_at_utc=i.ends_at_utc+timedelta(days=1)) for i in session.intervals))
    day = empty.trade_date.isoformat()
    source["sessions"] += (empty,)
    source["calendar"].sessions[(empty.market, empty.trade_date)] = empty
    source["rows_by_date"][day] = []
    source["quotes_by_date"][day] = {}
    binding = dict(next(iter(source["bindings"].values())))
    binding.pop("binding_id")
    source["bindings"][day] = r.profit.bind({**binding, "trade_date":day}, "binding_id")
    before = audit_source_joins(**source, **reference)
    assert before["complete"] and before == audit_source_joins(**source, **reference)
    assert len(before["dates"]) == 2
    assert len(before["dates"][0]["opportunities"]) == 1
    assert before["dates"][1]["opportunities"] == [] and before["dates"][1]["observed_minutes"] == 0


def test_reference_scope_mismatch_rejected(monkeypatch):
    source, reference = inputs(monkeypatch)
    reference["statuses"][0] = replace(reference["statuses"][0], market="NQ")
    with pytest.raises(IntegrityError, match="outside declared"):
        audit_source_joins(**source, **reference)
