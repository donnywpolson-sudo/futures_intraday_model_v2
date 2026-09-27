import json
import numpy as np
import pytest
from futures_rebuild import rolling_hour_owned_inputs as a
from futures_rebuild.boundary import RepoBoundary, OperationReceipt, OperationClassification
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import UnauthorizedOperation


def rows(family):
    base = [("ts_event","u8"),("ts_recv","u8"),("publisher_id","u2"),("instrument_id","u4")]
    extra = {"definition":[("instrument_class","S1"),("security_update_action","S1")],
        "statistics":[("stat_type","u2"),("update_action","u1")],
        "status":[("action","u2"),("is_trading","S1"),("is_quoting","S1")]}[family]
    values = np.zeros(2,dtype=base+extra)
    values['ts_event'] = [1514851200000000000,1514851200000000020]
    values['ts_recv'] = [1514851200000000001,1514851200000000021]
    values['publisher_id'] = 1
    values['instrument_id'] = 2
    if family=='definition':
        values['instrument_class'] = b'F'
        values['security_update_action'] = b'A'
    elif family=='statistics':
        values['stat_type'] = 9
        values['update_action'] = 1
    else:
        values['action'] = 1
        values['is_trading'] = b'Y'
        values['is_quoting'] = b'N'
    return values


@pytest.mark.current
def test_presence_is_causal_and_empty_status_is_not_coverage():
    definitions = {}
    counts = []
    d = a.summarize([rows('definition')],'definition',2018,definitions,counts.append)
    s = a.summarize([rows('statistics')],'statistics',2018,definitions,counts.append)
    assert d['records']==2 and d['definition_instrument_days']==1
    assert s['definition_presence']=={'present_by_receive_only':1,'present_by_event':1}
    assert s['types']=={'OPEN_INTEREST':2}
    assert a.summarize([rows('status')],'status',2018,definitions,counts.append)['records']==2
    empty = a.summarize([], 'status',2018,definitions,counts.append)
    assert empty['records']==0 and empty['ts_event_range_ns'] is None and not empty['received_utc_dates']
    assert sum(counts)==6


@pytest.mark.current
def test_assessment_synthetic_integration_and_no_unapproved_read(tmp_path,monkeypatch):
    root = tmp_path
    (root/a.PLAN).parent.mkdir(parents=True)
    (root/a.PLAN).write_text('{}')
    manifest = root/'source.manifest.json'
    manifest.write_text('{}')
    sources = [{'DBN':{'market':'ES','year':2018,'family':family},
        'SIDECAR':{'path':manifest.name,'sha256':sha256_file(manifest)},'query_contract':{}}
        for family in ('definition','statistics','status')]
    plan = {'plan_id':'a'*64,'purpose':'SYNTHETIC','sources':sources,'output_directory':'evidence',
        'record_limit':100,'hard_stop_minutes':1,'working_memory_bytes_max':2**31,'output_bytes_max':2**26,
        'source_contract':{'active_canonical_source':{'release_id':'b'*64,'release_manifest_sha256':'c'*64},
            'complete_inventory':{'content_inventory_sha256':'d'*64}}}
    monkeypatch.setattr(a,'load_plan',lambda root:plan)
    monkeypatch.setattr(a,'_query_contract',lambda *args:{})
    monkeypatch.setattr(a,'_binding',lambda **kwargs:None)
    calls=[]

    def chunks(binding,**kwargs):
        calls.append(kwargs['schema'])
        if kwargs['schema']!='status':
            yield rows(kwargs['schema'])

    monkeypatch.setattr(a,'_chunks',chunks)
    boundary=RepoBoundary(root)
    local=OperationReceipt.issue_local(boundary,operation=a.OPERATION,
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,scope={})
    with pytest.raises(UnauthorizedOperation):
        a.execute_once(root,local)
    assert not calls
    digest=sha256_file(root/a.PLAN)
    receipt=OperationReceipt.issue_user_approved(boundary,operation=a.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,scope=a.scope(root,plan),
        approval_command=a.OPERATION,approval_plan_id=plan['plan_id'],approval_plan_sha256=digest,
        approval_line=f"APPROVE {a.OPERATION} PLAN {plan['plan_id']} SHA256 {digest}")
    report=a.execute_once(root,receipt)
    assert report['files_attempted']==3 and not report['failures'] and report['records_counted']==4
    assert json.loads((root/'evidence/ES_2018_status.json').read_bytes())['state']=='COMPLETE_EMPTY'
    with pytest.raises((FileExistsError,UnauthorizedOperation)):
        a.execute_once(root,receipt)
    assert len(calls)==3
