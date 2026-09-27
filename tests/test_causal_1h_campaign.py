"""Generated checks for the approved hourly campaign and its existing contracts."""
from dataclasses import replace
import importlib.util
from pathlib import Path
import sys
import json
import pytest

from futures_rebuild.causal_observation_foundation import V10_CHECKPOINT_MARKETS
from futures_rebuild.causal_1h import CONTRACT, build_candidate, certify_candidate
from futures_rebuild.causal_timeframe_storage import read_record
from tests.test_causal_1h import sample, spec, RULEBOOK, LIMITS

pytestmark = pytest.mark.current


@pytest.mark.parametrize('market',V10_CHECKPOINT_MARKETS)
def test_registered_market_independent_roundtrip(tmp_path,market):
    assert CONTRACT['markets']==list(V10_CHECKPOINT_MARKETS)
    s=replace(spec(),market=market)
    day=sample(n=1)
    unit=int(RULEBOOK.rules[market].expected_unit_qty*10**9)
    definition=replace(day.definitions[0],market=market,raw_symbol=market+'H4',unit_of_measure_qty_nano=unit)
    day=replace(day,definitions=(definition,),bars=tuple(replace(b,market=market) for b in day.bars),
        references={'1m':tuple(replace(b,market=market) for b in day.references['1m'])})
    candidate,_=build_candidate(tmp_path,s,lambda:iter([day]),RULEBOOK,LIMITS)
    certificate,_=certify_candidate(tmp_path,s,lambda:iter([day]),RULEBOOK,LIMITS)
    assert certificate['passes'][0]==certificate['passes'][1]
    assert candidate['observations']==1 and candidate['comparison_counts']['MATCH']==1
    assert read_record(tmp_path/'candidate.json')['spec']['market']==market


def campaign_module():
    path=Path(__file__).resolve().parents[1]/'state/unpublished_evidence/causal_1h_campaign_v1/campaign.py'
    module_spec=importlib.util.spec_from_file_location('hourly_campaign_test',path)
    module=importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def test_all_market_receipt_labels_meet_existing_approval_contract():
    from futures_rebuild.boundary import _personal_approval_line
    campaign=campaign_module()
    for market in V10_CHECKPOINT_MARKETS:
        command=campaign.history_command(market)
        assert _personal_approval_line(command,'a'*64,'b'*64).startswith('APPROVE HOURLY_')


def test_campaign_requires_pinned_project_interpreter(monkeypatch):
    import databento
    campaign=campaign_module()
    monkeypatch.setattr(campaign.sys,'executable',str(campaign.ROOT/'.venv/Scripts/python.exe'))
    monkeypatch.setattr(databento,'__version__','0.78.0')
    campaign.check_runtime()
    monkeypatch.setattr(databento,'__version__','0.79.0')
    with pytest.raises(AssertionError,match='Pinned offline decoder'):
        campaign.check_runtime()


def test_common_native_start_and_batch_order(monkeypatch):
    campaign=campaign_module()
    monkeypatch.setattr(campaign,'V10_CHECKPOINT_MARKETS',('TN','ZL','BTC'))
    entries=[]
    for market in campaign.V10_CHECKPOINT_MARKETS:
        for family in ('definition','ohlcv_1h','ohlcv_1m'):
            start=('2017-11-19' if market=='BTC' else
                ('2016-01-01' if market=='TN' else '2011-01-01') if family=='ohlcv_1h' else '2010-06-06')
            entries.append(dict(market=market,family=family,kind='DBN',admitted_standard_foundation=True,
                interval_start_inclusive=start+'T00:00:00Z'))
    scopes=campaign.market_scopes(entries)
    assert [scopes[m]['start_date'] for m in campaign.V10_CHECKPOINT_MARKETS]==['2016-01-01','2011-01-01','2017-11-19']
    groups=campaign.groups(list(range(37)))
    assert len(groups)==10 and groups[-1]==[36]
    assert [m for group in groups for m in group]==list(range(37))


def test_publication_job_contains_worker_before_work(tmp_path):
    if sys.platform!='win32': pytest.skip('Windows supervision')
    campaign=campaign_module()
    result=campaign.bounded_child([sys.executable,'-B','-c',
        "import json,sys; from futures_rebuild.causal_1h_native import _verify_aggregate_job; "
        "r=json.loads(sys.stdin.buffer.readline()); _verify_aggregate_job(r['job'],4*2**30)"],
        {},4*2**30,60,tmp_path/'worker.log')
    assert result['returncode']==0 and result['workers_stopped']


def test_campaign_isolates_failure_flushes_partial_batch_and_skips_completed(tmp_path,monkeypatch):
    campaign=campaign_module()
    order=['6A','6B','6C','6E','6J']
    monkeypatch.setattr(campaign,'HERE',tmp_path)
    monkeypatch.setattr(campaign,'ROOT',tmp_path)
    monkeypatch.setattr(campaign,'STATE',tmp_path/'coordination.json')
    monkeypatch.setattr(campaign,'check_binding',lambda:{'order':order,'scopes':dict.fromkeys(order,{})})
    monkeypatch.setattr(campaign,'check_host',lambda:None)
    monkeypatch.setattr(campaign,'report',lambda:None)
    campaign.save(tmp_path/'coordination.json',{'owner_task':campaign.OWNER})
    campaign.save(tmp_path/'status.json',dict(state='READY',markets={m:{'state':'PENDING'} for m in order},attempts=0,batches=[]))
    calls=[]
    batches=[]
    def history(market,window):
        calls.append(market)
        return {'state':'BLOCKED','error':'causal guard'} if market=='6B' else {'state':'CERTIFIED','candidate':market}
    def publish(candidates,number):
        batches.append((number,candidates))
        path=tmp_path/campaign.pub.INDEX
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(str(number))
        return {'index_sha256':campaign.sha256_file(path),'plan_id':str(number)}
    monkeypatch.setattr(campaign,'history_market',history)
    monkeypatch.setattr(campaign,'publish_batch',publish)
    campaign.campaign()
    assert calls==order and batches==[(1,['6A','6C','6E']),(2,['6J'])]
    assert campaign.load(tmp_path/'status.json')['state']=='PARTIAL_BLOCKED'
    campaign.save(tmp_path/'coordination.json',{'owner_task':campaign.OWNER},mutable=True)
    campaign.campaign()
    assert calls==order and len(batches)==2


def test_native_preparation_refuses_gap_without_truncation(tmp_path,monkeypatch):
    from futures_rebuild import causal_1h_native as native, causal_source_closure as closure
    from futures_rebuild import foundation_operation_firewall as firewall
    from futures_rebuild.errors import IntegrityError
    from futures_rebuild.causal_1h import SECOND,DAY
    (tmp_path/'configs').mkdir()
    (tmp_path/'configs/source_contract.json').write_text(json.dumps({'complete_inventory':{'path':'inventory.json'},
        'contract_id':'a'*64,'active_canonical_source':{'release_id':'b'*64}}))
    (tmp_path/'inventory.json').write_text('{"entries":[]}')
    monkeypatch.setattr(closure,'validate_source_contract_metadata',lambda *a,**k:None)
    monkeypatch.setattr(closure,'validate_full_build_selection_contract',lambda *a,**k:None)
    monkeypatch.setattr(closure,'select_preferred_standard_source_entries',lambda *a:[])
    monkeypatch.setattr(firewall,'issue_current_source_closure_context',lambda *a:None)
    start,end=native.month_window('2024-03')
    units={f:[(start,end,None,None)] for f in ('definition','ohlcv_1h','ohlcv_1m')}
    units['ohlcv_1h']=[(start,start+DAY,None,None),(start+DAY+SECOND,end,None,None)]
    monkeypatch.setattr(native.HistoricalDbnSources,'_validated',lambda self:units)
    with pytest.raises(IntegrityError,match='coverage has a gap'):
        native.prepare(tmp_path,market='TN',month='2024-03')


def test_hourly_support_selection_preserves_pairs_and_rejects_ambiguity(tmp_path,monkeypatch):
    from futures_rebuild import causal_1h_native as native, causal_observation_canary as canary
    from futures_rebuild.errors import IntegrityError
    def pair(name,start,family='status'):
        e=dict(path=name+'.dbn.zst',kind='DBN',market='RTY',family=family,year=2017,
            interval_start_inclusive=start,interval_end_exclusive='2018-01-01',sha256='a'*64,size_bytes=1)
        return [e,{**e,'path':e['path']+'.manifest.json','kind':'SIDECAR'}]
    broad=pair('annual','2017-01-01')
    exact=pair('inception','2017-06-05')
    def query(root,e):
        from futures_rebuild.source_symbology import build_query_contract
        return build_query_contract(start=e['interval_start_inclusive'],end=e['interval_end_exclusive'],
            market=e['market'],schema=e['family'],stype_in='continuous',symbols=['RTY.v.0'])
    monkeypatch.setattr(canary,'_query_contract',query)
    start=native.timestamp('2017-06-05')
    selected,excluded=native._select_scope_support(tmp_path,broad+exact,start)
    assert selected==tuple(exact) and excluded[0]['entries']==broad
    # No global interval deduplication: bars and unresolved support overlap stay visible.
    for entries in (pair('a','2017-01-01','ohlcv_1h')+pair('b','2017-06-05','ohlcv_1h'),
                    broad+pair('partial','2017-06-06'),broad+exact+pair('duplicate','2017-06-05')):
        assert native._select_scope_support(tmp_path,entries,start)==(tuple(entries),[])
    monkeypatch.setattr(canary,'_query_contract',lambda root,e:query(root,e)|
        {'symbols':['OTHER'] if e['path'].startswith('annual') else ['RTY.v.0']})
    with pytest.raises(IntegrityError,match='semantics differ'):
        native._select_scope_support(tmp_path,broad+exact,start)
