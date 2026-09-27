"""Synthetic checks through the active entry point and existing preparation."""
from dataclasses import replace

import pyarrow as pa
import pytest

from futures_rebuild import active_data_view, annual_foundation_release
from futures_rebuild import model_input_admission as admission
from futures_rebuild import model_input_preparation as preparation
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.causal_timeframe_storage import signed
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation
from futures_rebuild.foundation.statistics_v2 import DecodedStatisticV2, observation_identity
from tests.test_model_input_preparation import T, bar, ledger, stat

pytestmark = pytest.mark.current
M, D = preparation.MINUTE, preparation.DELAY


def install(root):
    for relative in admission.BINDINGS:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"synthetic binding")
    value = signed(dict(schema=admission.SCHEMA, status=admission.STATE,
        research_authority=False, statistics_feature_eligible=False,
        bindings={p:sha256_file(root/p) for p in admission.BINDINGS}))
    (root/admission.POINTER).write_bytes(canonical_bytes(value)+b"\n")
    return value


def source(monkeypatch, rows, *, market="ES", year=2020, uses=("LINEAGE_AUDIT", "SELECTION")):
    entry = dict(market=market, year=year, disposition="MATERIALIZED", annual_release_key="synthetic",
                 cohort="DISCOVERY", permitted_uses=list(uses), selection_eligible=True)
    monkeypatch.setattr(active_data_view, "_load_view_catalog", lambda *a, **k:{"entries":[entry]})
    monkeypatch.setattr(annual_foundation_release, "verify_minute", lambda root, entry:root/"synthetic")
    q = pa.record_batch(dict(observation_ordinal=list(range(len(rows))), ohlc_valid=[True]*len(rows),
        volume_valid=[True]*len(rows), timestamp_order_valid=[True]*len(rows), duplicate_state=["UNIQUE"]*len(rows)))
    obs = pa.RecordBatch.from_pylist(rows)
    monkeypatch.setattr(preparation, "verified_batches", lambda *a:iter([(obs,q,preparation.structural_masks(obs,q))]))
    monkeypatch.setattr(preparation, "_decode_physical", lambda name,row:row)


def test_active_path_keeps_all_results_and_original_prices(monkeypatch, tmp_path):
    policy = install(tmp_path)
    rows = [bar(0), bar(1), bar(3), bar(4), bar(5, actual_contract="ESM0")]
    source(monkeypatch, rows)
    req = lambda lo,hi:admission.WindowRequest(T+lo*M,T+hi*M,T+10*M)
    target = admission.WindowRequest(T+M,T+4*M,T+D,"target",T+10*M,observation_identity(bar()),T+D)
    requests = [req(0,2), req(0,4), req(3,5), req(4,6),
                replace(req(0,2),decision_at_ns=T+2*M), target]
    results = admission.read_windows(tmp_path,market="ES",year=2020,requests=requests,purpose="SELECTION")
    assert [r["status"] for r in results] == ["ELIGIBLE","REFUSED","ELIGIBLE","REFUSED","REFUSED","INDETERMINATE"]
    assert [{k:r[k] for k in rows[0]} for r in results[0]["observations"]] == rows[:2]
    assert all(r["input_admission_id"] == policy["id"] and r["research_authority"] is False for r in results)


@pytest.mark.parametrize("market,start,end", preparation.EXCLUSIONS)
def test_active_disputes_and_restart(monkeypatch,tmp_path,market,start,end):
    install(tmp_path)
    rows=[bar(market=market,bar_start_ns=t,bar_end_ns=t+M,source_timestamp_ns=t,
              available_at_ns=t+M+D,decision_eligible_at_ns=t+M+D,
              definition_received_at_ns=start-M,listing_activation_ns=start-M,expiration_ns=end+10*M)
          for t in range(start,end+2*M,M)]
    year=2015 if market=="6N" else 2020
    source(monkeypatch,rows,market=market,year=year)
    requests=[admission.WindowRequest(start,end,end+D),admission.WindowRequest(end,end+2*M,end+2*M+D)]
    results=admission.read_windows(tmp_path,market=market,year=year,requests=requests)
    assert [r["status"] for r in results] == ["REFUSED","ELIGIBLE"]
    assert "DISPUTED_INTERVAL" in results[0]["reasons"]


def test_source_permissions_status_and_unavailable_history_still_refuse(monkeypatch,tmp_path):
    install(tmp_path)
    source(monkeypatch,[bar()],uses=("LINEAGE_AUDIT",))
    args=dict(market="ES",year=2020,requests=[admission.WindowRequest(T,T+M,T+M+D)])
    with pytest.raises(UnauthorizedOperation,match="cohort"):
        admission.read_windows(tmp_path,**args,purpose="SELECTION")
    with pytest.raises(UnauthorizedOperation,match="pre-2025"):
        admission.read_windows(tmp_path,**args,require_status=True)
    with pytest.raises(UnauthorizedOperation,match="Status-dependent"):
        admission.read_windows(tmp_path,**{**args,"year":2025},require_status=True)
    with pytest.raises(IntegrityError,match="absent"):
        admission.read_windows(tmp_path,**{**args,"year":2019})


def test_optional_values_stay_diagnostic_with_explicit_declarations(monkeypatch,tmp_path):
    install(tmp_path)
    source(monkeypatch,[bar(),bar(1)])
    statistics=ledger(DecodedStatisticV2(stat(),32))
    args=dict(market="ES",year=2020,requests=[admission.WindowRequest(T,T+2*M,T+2*M+D)])
    result=admission.read_windows(tmp_path,**args,statistics=statistics,max_age_ns=3*M,settlement_flags=7)[0]
    oi=result["statistics"]["OPEN_INTEREST"]
    assert oi["quantity"] == 2706698 and oi["candidate_feature_eligible"] is True
    assert oi["feature_eligible"] is False and oi["research_authority"] is False
    with pytest.raises(ContractError,match="V2"):
        admission.read_windows(tmp_path,**args,statistics=object())


@pytest.mark.parametrize("damage",["missing","inactive","omit_binding","code","during_read"])
def test_policy_fails_closed_before_return(monkeypatch,tmp_path,damage):
    policy=install(tmp_path)
    source(monkeypatch,[bar()])
    path=tmp_path/admission.POINTER
    if damage=="missing":path.unlink()
    elif damage in ("inactive","omit_binding"):
        if damage=="inactive":policy["status"]="PREPARED_INACTIVE"
        else:policy["bindings"].pop(admission.BINDINGS[0])
        policy.pop("id")
        path.write_bytes(canonical_bytes(signed(policy))+b"\n")
    elif damage=="code":(tmp_path/admission.BINDINGS[-1]).write_bytes(b"changed")
    else:
        original=admission.prepare_inputs
        def changed(*args,**kwargs):
            results=original(*args,**kwargs)
            policy["extra"]=True
            policy.pop("id")
            path.write_bytes(canonical_bytes(signed(policy))+b"\n")
            return results
        monkeypatch.setattr(admission,"prepare_inputs",changed)
    with pytest.raises((FileNotFoundError,IntegrityError,UnauthorizedOperation)):
        admission.read_windows(tmp_path,market="ES",year=2020,
            requests=[admission.WindowRequest(T,T+M,T+M+D)])
