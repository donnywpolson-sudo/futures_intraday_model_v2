from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

from futures_rebuild.canonical import canonical_bytes
from futures_rebuild.errors import ContractError, UnauthorizedOperation
from futures_rebuild.foundation.market_state import StatisticsRolePolicy, _statistics_ledger_event
from futures_rebuild.foundation.records import INT64_NULL, ProviderDefinition, StatisticsRecordV1
from futures_rebuild.foundation.statistics_v2 import (
    AsOfStatisticsV2, DecodedStatisticV2, interpret_statistic, observation_identity,
)
from futures_rebuild.model_input_preparation import (
    DELAY, EXCLUSIONS, MINUTE, WindowRequest, structural_masks, window_result,
)

pytestmark = pytest.mark.current
T = 1577836800000000000  # 2020-01-01 UTC, synthetic values only.
REPO = Path(__file__).resolve().parents[1]


def bar(i=0, **changes):
    start = T + i*MINUTE
    return dict(dict(market="ES", publisher_id=1, instrument_id=123, actual_contract="ESH0", currency="USD",
        min_price_increment_nano=250000000, multiplier_nano=50000000000,
        listing_activation_ns=T-1, expiration_ns=T+1000*MINUTE,
        bar_start_ns=start, bar_end_ns=start+MINUTE, source_timestamp_ns=start,
        source_cadence="1m", available_at_ns=start+MINUTE+DELAY,
        decision_eligible_at_ns=start+MINUTE+DELAY, definition_received_at_ns=T,
        open_nano=1_000_000_000, high_nano=1_000_000_000, low_nano=1_000_000_000,
        close_nano=1_000_000_000, volume=1), **changes)


def stat(i=0, **changes):
    return replace(StatisticsRecordV1(dataset="GLBX.MDP3", market="ES", publisher_id=1,
        instrument_id=123, instrument_id_date_utc="2020-01-01", ts_event_ns=T,
        ts_recv_ns=T+10+i, ts_ref_ns=T, ts_in_delta=0, stat_type="OPEN_INTEREST",
        update_action="NEW", price_nano=INT64_NULL, quantity=2706698, sequence=i,
        channel_id=1, flags=0, source_release_id="a"*64, source_manifest_sha256="b"*64,
        source_file_path="dbn/statistics/ES/2020/test.dbn.zst", source_file_sha256="c"*64,
        row_ordinal=i, row_sha256=f"{i+1:064x}"), **changes)


def definition(**changes):
    return replace(ProviderDefinition(dataset="GLBX.MDP3", market="ES", publisher_id=1,
        instrument_id=123, instrument_id_date_utc="2020-01-01", ts_event_ns=T, ts_recv_ns=T,
        activation_ns=T-1, expiration_ns=T+1000*MINUTE, security_update_action="ADD",
        instrument_class="FUTURE", security_type="FUT", raw_symbol="ESH0", exchange="XCME",
        currency="USD", min_price_increment_nano=250000000, unit_of_measure_qty_nano=50000000000,
        unit_of_measure="IPNT", source_release_id="a"*64, source_manifest_sha256="b"*64,
        source_file_path="dbn/definition/ES/2020/test.dbn.zst", source_file_sha256="d"*64,
        row_ordinal=0, row_sha256="e"*64), **changes)


def ledger(*values, definitions=None):
    economics = SimpleNamespace(resolve=lambda market, definition: SimpleNamespace(point_value=Decimal(50)))
    return AsOfStatisticsV2(values, definitions=[definition()] if definitions is None else definitions,
                            economics=economics)


def query(store, decision=T+100, **changes):
    request = dict(observation=bar(), decision_at_ns=decision, segment_start_ns=T, max_age_ns=1000)
    request.update(changes)
    return store.batch_asof([request])[0]


def test_gaps_duplicates_order_restart_and_unchanged_observations():
    rows = [bar(i) for i in range(4)]
    request = WindowRequest(T, T+4*MINUTE, T+4*MINUTE+DELAY)
    assert window_result(rows, True, request, market="ES")["observations"] is rows
    for bad in (rows[:1]+rows[2:], rows[:1]+rows, list(reversed(rows))):
        assert window_result(bad, True, request, market="ES")["status"] == "REFUSED"
    assert window_result(rows[2:], True, replace(request, start_ns=T+2*MINUTE), market="ES")["status"] == "ELIGIBLE"
    assert window_result(rows, False, request, market="ES")["status"] == "REFUSED"
    assert "PREMATURE_INPUT" in window_result(rows, True, replace(request, decision_at_ns=T+4*MINUTE), market="ES")["reasons"]


@pytest.mark.parametrize("field,value", [("actual_contract", "ESM0"), ("publisher_id", 2),
    ("instrument_id", 456), ("currency", "EUR"), ("min_price_increment_nano", 125000000),
    ("multiplier_nano", 25000000000), ("listing_activation_ns", T-2)])
def test_identity_changes_reset_features_and_targets(field, value):
    rows = [bar(), bar(1, **{field:value})]
    request = WindowRequest(T, T+2*MINUTE, T+2*MINUTE+DELAY)
    assert "IDENTITY_CHANGE_OR_UNRESOLVED" in window_result(rows, True, request, market="ES")["reasons"]
    target = WindowRequest(T+MINUTE, T+2*MINUTE, T+DELAY, "target", T+3*MINUTE,
                           observation_identity(rows[0]), T+DELAY)
    assert window_result(rows[1:], True, target, market="ES")["status"] == "INDETERMINATE"


@pytest.mark.parametrize("market,start,end", EXCLUSIONS)
def test_exclusions_are_half_open_even_when_source_bar_is_missing(market, start, end):
    for lo, hi, excluded in ((start-MINUTE,start,False), (start,start+MINUTE,True),
                            (end-MINUTE,end,True), (end,end+MINUTE,False)):
        result = window_result([], True, WindowRequest(lo,hi,hi+DELAY),market=market)
        assert ("DISPUTED_INTERVAL" in result["reasons"]) == excluded


def test_incomplete_outcomes_retained_and_entry_later_than_signal():
    target = WindowRequest(T+MINUTE,T+4*MINUTE,T+DELAY,"target",T+5*MINUTE,
                           observation_identity(bar()),T+DELAY)
    complete = [bar(i) for i in (1,2,3)]
    assert window_result(complete,True,target,market="ES")["status"] == "COMPLETE"
    assert window_result(complete[:1],True,target,market="ES")["status"] == "INDETERMINATE"
    early = replace(target,start_ns=T)
    assert "ENTRY_NOT_LATER_THAN_SIGNAL" in window_result([bar()]+complete,True,early,market="ES")["reasons"]


def test_vector_structural_checks():
    rows = [bar(),bar(1,available_at_ns=T+2*MINUTE),bar(2,instrument_id=0)]
    quality = pa.Table.from_pylist([dict(ohlc_valid=True, volume_valid=True,
        timestamp_order_valid=True, duplicate_state="UNIQUE") for _ in rows])
    masks=structural_masks(pa.Table.from_pylist(rows),quality)
    assert masks["TIMING_REFUSAL"].tolist() == [False,True,False]
    assert masks["IDENTITY_UNRESOLVED"].tolist() == [False,False,True]


@pytest.mark.parametrize("bits", [32,64])
def test_field_specific_statistics_and_zero(bits):
    missing = (1 << (bits-1))-1
    oi = interpret_statistic(DecodedStatisticV2(stat(),bits))
    assert (oi["reason"],oi["quantity"],oi["price_nano"]) == ("KNOWN",2706698,None)
    assert interpret_statistic(DecodedStatisticV2(stat(quantity=0),bits))["quantity"] == 0
    assert interpret_statistic(DecodedStatisticV2(stat(quantity=missing),bits))["reason"] == "UNDEFINED_QUANTITY"
    settlement = interpret_statistic(DecodedStatisticV2(stat(stat_type="SETTLEMENT_PRICE",price_nano=100,quantity=missing),bits))
    assert (settlement["reason"],settlement["price_nano"],settlement["quantity"]) == ("KNOWN",100,None)
    assert interpret_statistic(DecodedStatisticV2(stat(quantity=2**31-1),64))["quantity"] == 2**31-1


def test_receipt_asof_revisions_deletion_staleness_and_no_resurrection():
    store=ledger(DecodedStatisticV2(stat(),32),DecodedStatisticV2(stat(1,ts_recv_ns=T+200,quantity=0),32),
                 DecodedStatisticV2(stat(2,ts_recv_ns=T+300,update_action="DELETE"),32))
    results=store.batch_asof([dict(observation=bar(),decision_at_ns=t,segment_start_ns=T,max_age_ns=1000)
                              for t in (T+250,T+5,T+350,T+100)])
    assert [r["OPEN_INTEREST"]["quantity"] for r in results] == [0,None,None,2706698]
    assert results[2]["OPEN_INTEREST"]["reason"] == "DELETED"
    assert query(store,T+150,max_age_ns=50)["OPEN_INTEREST"]["reason"] == "STALE"
    assert query(store,max_age_ns=None)["OPEN_INTEREST"]["candidate_feature_eligible"] is False
    assert query(store)["OPEN_INTEREST"]["feature_eligible"] is False
    assert query(store,segment_start_ns=T+50)["OPEN_INTEREST"]["reason"] == "NO_ASOF_VALUE"


def test_settlement_declaration_conflicts_future_reference_and_identity_reuse():
    settlement=DecodedStatisticV2(stat(stat_type="SETTLEMENT_PRICE",price_nano=123,flags=1),32)
    store=ledger(settlement)
    assert query(store)["SETTLEMENT_PRICE"]["candidate_feature_eligible"] is False
    assert query(store,settlement_flags=1)["SETTLEMENT_PRICE"]["candidate_feature_eligible"] is True
    assert query(store,settlement_flags=2)["SETTLEMENT_PRICE"]["reason"] == "NO_ASOF_VALUE"
    conflict=DecodedStatisticV2(stat(1,ts_recv_ns=T+10,quantity=9,source_file_path="dbn/statistics/ES/2020/other.dbn.zst"),32)
    assert query(ledger(DecodedStatisticV2(stat(),32),conflict))["OPEN_INTEREST"]["reason"] == "CONFLICTING_RECORDS"
    future=DecodedStatisticV2(stat(ts_ref_ns=T+200),32)
    assert query(ledger(future))["OPEN_INTEREST"]["reason"] == "NO_ASOF_VALUE"
    assert query(ledger(DecodedStatisticV2(stat(),32)),observation=bar(actual_contract="ESM0"))["OPEN_INTEREST"]["quantity"] is None
    assert query(ledger(DecodedStatisticV2(stat(),32),definitions=[]))["OPEN_INTEREST"]["reason"] == "IDENTITY_UNRESOLVED"


def test_legacy_ledger_bytes_do_not_adopt_v2_interpretation():
    roles=StatisticsRolePolicy.from_file(REPO/"configs/statistics_foundation_roles.json")
    record=stat()
    before=canonical_bytes(_statistics_ledger_event(record,roles=roles))
    assert b'"state_after":"NEW_UNKNOWN_VALUE"' in before
    assert interpret_statistic(DecodedStatisticV2(record,32))["quantity"] == 2706698
    assert canonical_bytes(_statistics_ledger_event(record,roles=roles)) == before
    old_settlement=_statistics_ledger_event(stat(stat_type="SETTLEMENT_PRICE",price_nano=123,quantity=2**31-1),roles=roles)
    assert old_settlement["quantity_after"] == 2**31-1
    assert old_settlement["feature_eligible"] is False


def test_invalid_request_and_width_refuse():
    with pytest.raises(ContractError):
        WindowRequest(T,T+MINUTE,T,"target")
    with pytest.raises(ContractError):
        DecodedStatisticV2(stat(quantity=2**31),32)


def test_optional_signal_scope_refuses_before_source_access(monkeypatch,tmp_path):
    import futures_rebuild.model_input_preparation as p
    monkeypatch.setattr(p.active_data_view,"resolve",lambda **kwargs:pytest.fail("Source must stay unopened"))
    with pytest.raises(UnauthorizedOperation,match="2018-2022"):
        p.prepare_inputs(tmp_path,market="ES",year=2023,requests=[],statistics=object())


def test_future_conflict_is_invisible_and_settlement_delete_never_resurrects():
    original=DecodedStatisticV2(stat(stat_type="SETTLEMENT_PRICE",price_nano=123,flags=7),32)
    deleted=DecodedStatisticV2(stat(1,stat_type="SETTLEMENT_PRICE",ts_recv_ns=T+200,
                                   update_action="DELETE",flags=0),32)
    store=ledger(original,deleted)
    assert query(store,T+100,settlement_flags=7)["SETTLEMENT_PRICE"]["price_nano"] == 123
    assert query(store,T+200,settlement_flags=7)["SETTLEMENT_PRICE"]["reason"] == "DELETED"
    conflict=DecodedStatisticV2(stat(2,ts_recv_ns=T+10,quantity=2,flags=1,
        source_file_path="dbn/statistics/ES/2020/other.dbn.zst"),32)
    assert query(ledger(DecodedStatisticV2(stat(),32),conflict))["OPEN_INTEREST"]["reason"] == "CONFLICTING_RECORDS"


@pytest.mark.parametrize("bits",[32,64])
def test_v2_decoder_takes_quantity_width_from_actual_dtype(monkeypatch,bits):
    import numpy as np
    import futures_rebuild.foundation.statistics_v2 as v2
    fields=[(name,"<u8") for name in ("ts_recv","ts_event","ts_ref","publisher_id","instrument_id",
        "sequence","channel_id","stat_flags","stat_type","update_action","ts_in_delta")]
    chunk=np.zeros(1,dtype=fields+[("price","<i8"),("quantity",f"<i{bits//8}")])
    for name,value in dict(ts_recv=T+10,ts_event=T,ts_ref=T,publisher_id=1,instrument_id=123,
            stat_type=9,update_action=1,price=INT64_NULL,quantity=2**31-1).items():chunk[name]=value
    monkeypatch.setattr(v2,"_chunks",lambda *a,**k:iter([chunk]))
    binding=SimpleNamespace(source_release_id="a"*64,source_manifest_sha256="b"*64,
                            relative_path="dbn/statistics/ES/2020/test.dbn.zst",sha256="c"*64)
    decoded=list(v2.iter_statistics_v2(binding,market="ES",expected_query_contract={}))[0]
    assert decoded.quantity_bits == bits
    assert v2.interpret_statistic(decoded)["quantity"] == (None if bits==32 else 2**31-1)


def test_preparation_integrates_catalog_checks_quality_and_segmented_statistics(monkeypatch,tmp_path):
    import futures_rebuild.model_input_preparation as p
    rows=[bar(0,actual_contract="ESZ9"),bar(1),bar(2)]
    quality=pa.record_batch(dict(observation_ordinal=[0,1,2],ohlc_valid=[True]*3,
        volume_valid=[True]*3,timestamp_order_valid=[True]*3,duplicate_state=["UNIQUE"]*3))
    observed=pa.RecordBatch.from_pylist(rows)
    calls=[]
    monkeypatch.setattr(p.active_data_view,"resolve",lambda **kwargs:calls.append(kwargs))
    monkeypatch.setattr(p.active_data_view,"_load_view_catalog",lambda *a,**k:{"entries":[
        dict(market="ES",year=2020,cohort="DISCOVERY",permitted_uses=["LINEAGE_AUDIT"],selection_eligible=True)]})
    monkeypatch.setattr(p,"verified_batches",lambda *a:iter([(observed,quality,p.structural_masks(observed,quality))]))
    # A synthetic codec stand-in; the real codec is exercised by the local integration audit.
    monkeypatch.setattr(p,"_decode_physical",lambda name,row:row)
    request=WindowRequest(T+MINUTE,T+3*MINUTE,T+3*MINUTE+DELAY)
    class Statistics:
        def batch_asof(self,requests):
            assert requests[0]["segment_start_ns"] == T+MINUTE
            return [{"diagnostic":True}]
    result=p.prepare_inputs(tmp_path,market="ES",year=2020,requests=[request],statistics=Statistics())[0]
    assert calls[0]["purpose"] == "LINEAGE_AUDIT"
    assert result["status"]=="ELIGIBLE" and result["statistics"]=={"diagnostic":True}
    assert result["research_authority"] is False
