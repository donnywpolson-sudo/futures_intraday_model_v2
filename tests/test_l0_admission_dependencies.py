"""Synthetic metadata only; these fixtures are never historical evidence."""
from copy import deepcopy
from datetime import datetime
from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytestmark = pytest.mark.current
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import prepare_l0_admission_dependencies as dep
sys.path.pop(0)
from futures_rebuild import l0_alpha_candidate as c
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError
from futures_rebuild.l0_alpha_replay import replay


def row():
    at=int(datetime.fromisoformat('2018-01-02T14:29:00+00:00').timestamp())*10**9
    return dict(market='ES',source_contract_id='1'*64,source_release_id='2'*64,source_row_sha256='3'*64,
        source_cadence='1m',bar_start_ns=at,bar_end_ns=at+c.MINUTE,decision_eligible_at_ns=at+c.MINUTE+c.DELAY,
        publisher_id=1,instrument_id=123,raw_symbol='ESH8',actual_contract='ESH8',
        definition_source_file_path='synthetic-definition-only.json',definition_source_file_sha256='4'*64,
        definition_row_sha256='5'*64,definition_event_at_ns=at-c.MINUTE,definition_received_at_ns=at-c.MINUTE,
        listing_activation_ns=at-10*c.MINUTE,expiration_ns=at+100*c.MINUTE,
        min_price_increment_nano=250000000,multiplier_nano=50*10**9,project_trade_date='2018-01-02')


def test_projection_rejects_extra_columns_scope_duplicates_and_ambiguity():
    r=row(); source={'market':'ES','year':2018}; symbols=['ESH8']
    assert dep.validate_rows([r],source,symbols)['rows']==1
    for rows,match in [([{**r,'open_nano':0}], 'columns'),([r,r],'duplicate'),
                       ([{**r,'market':'NQ'}],'scope'),([{**r,'instrument_id':None}],'identity'),
                       ([{**r,'bar_end_ns':r['bar_end_ns']+1}],'time')]:
        with pytest.raises(IntegrityError,match=match): dep.validate_rows(rows,source,symbols)
    second={**r,'bar_start_ns':r['bar_start_ns']+c.MINUTE,'bar_end_ns':r['bar_end_ns']+c.MINUTE,
            'source_row_sha256':'6'*64,'definition_row_sha256':'7'*64}
    with pytest.raises(IntegrityError,match='ambiguous definition'): dep.validate_rows([r,second],source,symbols)


def test_exact_termination_join_availability_and_dst():
    r=row(); decision=r['bar_end_ns']+c.DELAY
    selection={**r,'decision_ns':decision,'exit_ns':decision+60*c.MINUTE}
    last=int(datetime.fromisoformat('2018-03-16T13:30:00+00:00').timestamp())*10**9
    rule={k:r[k] for k in ('market','publisher_id','instrument_id','actual_contract','definition_row_sha256')}
    rule.update(effective_start_ns=0,effective_end_ns=last+c.MINUTE,available_by_ns=decision-1,
        source_binding={'path':'SYNTHETIC_ONLY.json','sha256':'8'*64},section='fixture',contract_month='2018-03',venue='CME',
        fact_kind='LAST_TRADING_TIME',last_trading_local='2018-03-16T08:30:00',timezone='America/Chicago',last_trading_ns=last)
    assert dep.termination_permission(selection,[])=='UNKNOWN'
    assert dep.termination_permission(selection,[rule])=='BEFORE_TERMINATION'
    assert dep.termination_permission({**selection,'exit_ns':last},[rule])=='BLOCKED_BY_TERMINATION'
    assert dep.termination_permission(selection,[{**rule,'available_by_ns':decision+1}])=='UNKNOWN'
    assert dep.termination_permission(selection,[{**rule,'effective_end_ns':selection['exit_ns']}])=='UNKNOWN'
    assert dep.termination_permission(selection,[{**rule,'instrument_id':456}])=='UNKNOWN'
    with pytest.raises(IntegrityError,match='ambiguous'): dep.termination_permission(selection,[rule,rule])
    with pytest.raises(IntegrityError,match='not last trading'): dep.termination_permission(selection,[{**rule,'fact_kind':'EXPIRATION'}])
    with pytest.raises(IntegrityError,match='timezone'): dep.termination_permission(selection,[{**rule,'last_trading_ns':last+3600*10**9}])
    with pytest.raises(IntegrityError,match='nonexistent'): dep.termination_permission(selection,[{**rule,'last_trading_local':'2018-03-11T02:30:00'}])
    with pytest.raises(IntegrityError,match='ambiguous local'): dep.termination_permission(selection,[{**rule,'last_trading_local':'2018-11-04T01:30:00'}])


def test_synthetic_import_stale_binding_and_truncation(tmp_path,monkeypatch):
    monkeypatch.setattr(dep,'OUTPUT',tmp_path)
    source={'market':'ES','year':2018,'row_count':1}
    spec=c.bind({'schema_version':'SYNTHETIC_ONLY','calendar_id':'a'*64,'source_files':[source],
                 'observed_symbols':{'ES':['ESH8']}},'specification_id')
    path=tmp_path/'metadata_export/ES/2018.parquet';path.parent.mkdir(parents=True)
    raw=row()
    for k in raw:
        if dep.EXPORT_SCHEMA.field(k).type==pa.binary(32):raw[k]=bytes.fromhex(raw[k])
    pq.write_table(pa.Table.from_pylist([raw],schema=dep.EXPORT_SCHEMA),path)
    receipt=c.bind({'specification_id':spec['specification_id'],'calendar_id':spec['calendar_id'],
        'source_files':spec['source_files'],'evidence_class':'METADATA_PROJECTION_NOT_ADMISSION',
        'outputs':{'ES/2018.parquet':{'sha256':sha256_file(path),'rows':1}}},'export_id')
    assert dep.validate_export(tmp_path,spec,receipt)['admission']=='NOT_EVALUATED'
    bad=deepcopy(receipt);bad.pop('export_id');bad['calendar_id']='b'*64
    with pytest.raises(IntegrityError,match='stale'):dep.validate_export(tmp_path,spec,c.bind(bad,'export_id'))
    bad=deepcopy(receipt);bad.pop('export_id');bad['outputs']['ES/2018.parquet']['rows']=2
    with pytest.raises(IntegrityError,match='truncated'):dep.validate_export(tmp_path,spec,c.bind(bad,'export_id'))


def test_unsupported_training_mean_remains_rejected():
    with pytest.raises(IntegrityError,match='fixed benchmark'):
        replay([],[],None,[],policy='date_balanced_training_mean',timing_only=True)


def test_candidate_proposal_preserves_science_and_is_not_accepted():
    import json
    root=Path(__file__).resolve().parents[1]
    original=json.loads((root/'configs/l0_alpha_candidate_v6.json').read_text())
    proposed=json.loads((dep.OUTPUT/'proposed_bundle.json').read_text())
    assert {k for k in original if original[k]!=proposed[k]}=={'entry_calendar','bundle_id'}
    contract=json.loads((dep.OUTPUT/'proposed_contract.json').read_text())
    with pytest.raises(IntegrityError,match='binding changed'):dep.admission.validate_contract(root,contract)


def proposed_module(name):
    import types
    module=types.ModuleType('futures_rebuild._synthetic_proposal_'+name)
    module.__package__='futures_rebuild'
    path=dep.OUTPUT/('proposed_'+name+'.py')
    exec(compile(path.read_text(),str(path),'exec'),module.__dict__)
    return module


def test_proposed_mean_timing_retains_occupancy_and_requires_real_inputs():
    module=proposed_module('l0_alpha_replay')
    day='2018-01-02';records=[];outcomes=[]
    for i,at in enumerate(c.decision_times(day)):
        entry=(at//c.MINUTE+1)*c.MINUTE
        records.append(dict(market='ES',trade_date=day,decision_ns=at,entry_ns=entry,exit_ns=entry+60*c.MINUTE,
            status='ELIGIBLE',forecast_id=str(i),actual_contract='SYNTHETIC_ONLY',tick_nano=1,tick_value_nano_usd=10**9))
        outcomes.append(dict(entry_status='RESOLVED',exit_status='RESOLVED',entry_open_nano=100,exit_open_nano=100))
    mean=module.replay(records,outcomes,[20.]*330,[day],policy='date_balanced_training_mean',timing_only=True,legacy=True)
    fixed=module.replay(records,outcomes,None,[day],policy='always_long',timing_only=True,legacy=True)
    assert mean['events']==fixed['events'] and mean['resolved_round_trips']>0
    assert mean['mode']=='TIMING_ONLY_NO_PROFITABILITY' and 'daily' not in mean and 'net_usd' not in mean
    for predictions,match in [(None,'requires'),([float('nan')]*330,'finite'),([20.]*329+[21.],'fixed')]:
        with pytest.raises(IntegrityError,match=match):
            module.replay(records,outcomes,predictions,[day],policy='date_balanced_training_mean',timing_only=True,legacy=True)
    missing=deepcopy(outcomes);missing[0]['exit_status']='MISSING'
    result=module.replay(records,missing,[20.]*330,[day],policy='date_balanced_training_mean',timing_only=True,legacy=True)
    assert not result['complete'] and result['unresolved_position']['side']=='exit'
    assert result['attempted_entries']==1


def test_proposed_final_count_exports_exact_supported_dates_only(monkeypatch):
    from types import SimpleNamespace
    module=proposed_module('l0_alpha_candidate')
    monkeypatch.setattr(module,'fold_schedule',lambda dataset: ())
    for count in (505,504):
        dataset=SimpleNamespace(decisions=[],outcomes=[],fit_positions=list(range(count)),
            terminal_data=SimpleNamespace(trade_dates=list(c.weekday_dates()[:count])))
        result=module.source_completeness(dataset)
        assert result['final_observed_training_dates']==(505 if count==505 else None)
        assert result['support_status']==('SUPPORTED' if count==505 else 'INSUFFICIENT_SUPPORT')
