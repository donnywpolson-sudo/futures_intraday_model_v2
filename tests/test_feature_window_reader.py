"""Synthetic feature assembly; no real calendars, price files or active readers."""
from pathlib import Path
import json
import pytest

from futures_rebuild import feature_window_reader as f
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.causal_timeframe_storage import signed
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation

pytestmark = pytest.mark.current
LEFT = 1577836800*10**9  # Generated timeline anchored at 2020-01-01 UTC.


def write(root, name, value):
    path=root/name
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_bytes(canonical_bytes(signed(value))+b'\n')
    return dict(path=name,sha256=sha256_file(path))


class AdmittedFixture:
    def __init__(self, rows, during=None):
        self.rows={r['bar_start_ns']:r for r in rows}
        self.calls=[]
        self.during=during

    def read_window(self, **request):
        self.calls.append(request)
        step=f.HOUR if next(iter(self.rows.values()))['origin']=='NATIVE_1H' else f.DAY
        result=[]
        for start in range(request['start_ns'],request['end_ns'],step):
            if start not in self.rows:
                raise UnauthorizedOperation('Unknown or excluded observation')
            row=self.rows[start]
            if row['decision_eligible_at_ns']>request['as_of_ns']:
                raise UnauthorizedOperation('Premature observation')
            result.append(row)
        if self.during:
            self.during()
        return result


def setup(root, *, tf='1h', closure=(2,4), present=(0,1,4,5), known=0, certificate_status='PASS_CLOSED_INTERVAL_CERTIFICATION'):
    step=f.HOUR if tf=='1h' else f.DAY
    right=LEFT+6*step
    for name in f.REQUIRED_BINDINGS:
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('{}')
    write(root,'data/causally_gated_normalized/_control/timeframes.json',dict(entries=[dict(market='TEST',timeframe=tf,
        coverage_start_ns=LEFT,coverage_end_ns=right,disposition='CERTIFIED')]))
    catalog=dict(entries=[dict(market='TEST',coverage_start='2020-01-01',coverage_end='2021-01-01',
        disposition='RESEARCH_READY_CAUSAL_PRICE',permitted_uses=['LINEAGE_AUDIT','FEATURE_GENERATION'])])
    (root/'data/causally_gated_normalized/_control/catalog.json').write_text(json.dumps(catalog))
    bindings={p:sha256_file(root/p) for p in f.REQUIRED_BINDINGS}
    ev=write(root,'evidence.json',dict(generated=True))
    closures=[] if closure is None else [dict(market='TEST',start_ns=LEFT+closure[0]*step,
        end_ns=LEFT+closure[1]*step,available_at_ns=known,timeframes=[tf],evidence_ids=['fixture'],
        product_venue_qualified=True,schedule_qualified=True,availability_qualified=True,
        no_conflicts=True,observations_absent_verified=True)]
    cert=write(root,'certificate.json',dict(status=certificate_status,bindings=bindings,evidence={'fixture':ev},closures=closures))
    policy=dict(schema='feature_window_policy/1',source='native',contract_policy='RESET_AT_IDENTITY_CHANGE',
        bindings=bindings,closure_certificate=cert,start_ns=LEFT,end_ns=right,
        supported=[dict(market='TEST',timeframe=tf,lookbacks=[1,2,3,4,5],purposes=['LINEAGE_AUDIT','FEATURE_GENERATION'])])
    write(root,'policy.json',policy)
    rows=[dict(market='TEST',publisher_id=1,instrument_id=7,actual_contract='TESTH0',currency='USD',
        min_price_increment_nano=1,multiplier_nano=2,bar_start_ns=LEFT+i*step,
        bar_end_ns=LEFT+(i+1)*step,available_at_ns=LEFT+(i+1)*step+f.LAG,
        decision_eligible_at_ns=LEFT+(i+1)*step+f.LAG,
        origin='NATIVE_1H' if tf=='1h' else 'NATIVE_1D',quality_disposition='GENERATED_ACCEPTED',
        row_id=str(i),open_nano=100+i,close_nano=101+i) for i in present]
    fake=AdmittedFixture(rows)
    return policy,fake,dict(market='TEST',timeframe=tf,start_ns=LEFT,end_ns=right,
        as_of_ns=right+f.LAG,lookback_bars=4,purpose='FEATURE_GENERATION')


def reader(root, tf, fake):
    result=f.FeatureWindowReader(root,preview_manifest='policy.json')
    result._readers[tf]=fake
    return result


@pytest.mark.parametrize('tf',['1h','1d'])
def test_closure_stitch_preserves_rows_and_uses_two_public_segments(tmp_path,tf):
    policy,fake,request=setup(tmp_path,tf=tf)
    r=reader(tmp_path,tf,fake)
    result=r.read_feature_window(**request)
    assert [x['row_id'] for x in result['rows']]==['0','1','4','5']
    assert result['rows']==list(fake.rows.values())
    assert len(fake.calls)==2 and all(x['purpose']=='FEATURE_GENERATION' for x in fake.calls)
    assert result['elapsed_span_ns']==request['end_ns']-LEFT
    assert result['final_observation_age_ns']==0
    assert len(result['skipped_closures'])==1
    assert result['segment_identity']['actual_contract']=='TESTH0'
    # Same reader instance is retained, with no per-bar construction.
    assert r._reader(tf) is fake


def test_closed_endpoint_is_explicit_and_unknown_trailing_gap_never_falls_back(tmp_path):
    policy,fake,request=setup(tmp_path,closure=(4,6),present=(0,1,2,3))
    r=reader(tmp_path,'1h',fake)
    result=r.read_feature_window(**request)
    assert result['final_observation_age_ns']==2*f.HOUR
    assert result['observation_age_at_decision_ns']==2*f.HOUR+f.LAG
    fake.rows.pop(LEFT+3*f.HOUR)
    with pytest.raises(f.FeatureWindowRefused,match='ADMITTED_SEGMENT_REFUSED'):
        r.read_feature_window(**request)


@pytest.mark.parametrize('closure,present',[(None,(0,1,4,5)),((2,4),(0,1,4))])
def test_unknown_internal_and_trailing_gaps_refuse(tmp_path,closure,present):
    _,fake,request=setup(tmp_path,closure=closure,present=present)
    with pytest.raises(f.FeatureWindowRefused,match='ADMITTED_SEGMENT_REFUSED'):
        reader(tmp_path,'1h',fake).read_feature_window(**request)


@pytest.mark.parametrize('key,value',[('actual_contract','TESTM0'),('instrument_id',8),('publisher_id',2),
    ('currency','EUR'),('multiplier_nano',3),('min_price_increment_nano',2)])
def test_resets_between_admitted_segments(tmp_path,key,value):
    _,fake,request=setup(tmp_path)
    fake.rows[LEFT+4*f.HOUR][key]=value
    with pytest.raises(f.FeatureWindowRefused,match='IDENTITY_RESET'):
        reader(tmp_path,'1h',fake).read_feature_window(**request)


def test_late_calendar_becomes_usable_only_at_decision(tmp_path):
    known=LEFT+6*f.HOUR+f.LAG+1
    _,fake,request=setup(tmp_path,known=known)
    r=reader(tmp_path,'1h',fake)
    with pytest.raises(f.FeatureWindowRefused):r.read_feature_window(**request)
    assert len(r.read_feature_window(**(request|{'as_of_ns':known}))['rows'])==4


@pytest.mark.parametrize('change',[{'source':'minute_derived'},{'timeframe':'1m'},{'lookback_bars':True},
    {'start_ns':LEFT-1},{'end_ns':f.CUTOFF+f.HOUR},{'as_of_ns':LEFT}])
def test_unsupported_arguments_fail(tmp_path,change):
    _,fake,request=setup(tmp_path)
    with pytest.raises((ContractError,f.FeatureWindowRefused)):
        reader(tmp_path,'1h',fake).read_feature_window(**(request|change))


def test_calendar_candidates_are_rejected(tmp_path):
    setup(tmp_path,certificate_status='PASS_CALENDAR_CONJUNCTION_DIAGNOSTIC_ONLY')
    with pytest.raises(IntegrityError,match='not certified'):
        f.FeatureWindowReader(tmp_path,preview_manifest='policy.json')


def test_absence_proof_required_and_source_specific(tmp_path):
    policy,fake,request=setup(tmp_path)
    cert=json.loads((tmp_path/'certificate.json').read_text());cert.pop('id')
    cert['closures'][0]['observations_absent_verified']=False
    policy['closure_certificate']=write(tmp_path,'certificate.json',cert)
    write(tmp_path,'policy.json',policy)
    with pytest.raises(IntegrityError,match='Incomplete closure'):
        reader(tmp_path,'1h',fake)


def test_unavailable_delivery_not_overridden_by_closure(tmp_path):
    policy,fake,request=setup(tmp_path)
    index=dict(entries=[dict(market='TEST',timeframe='1h',coverage_start_ns=LEFT,
        coverage_end_ns=LEFT+2*f.HOUR,disposition='CERTIFIED'),
        dict(market='TEST',timeframe='1h',coverage_start_ns=LEFT+2*f.HOUR,
        coverage_end_ns=LEFT+4*f.HOUR,disposition='UNAVAILABLE'),
        dict(market='TEST',timeframe='1h',coverage_start_ns=LEFT+4*f.HOUR,
        coverage_end_ns=LEFT+6*f.HOUR,disposition='CERTIFIED')])
    ref=write(tmp_path,'data/causally_gated_normalized/_control/timeframes.json',index)
    policy['bindings'][ref['path']]=ref['sha256']
    cert=json.loads((tmp_path/'certificate.json').read_text());cert.pop('id')
    cert['bindings'][ref['path']]=ref['sha256']
    policy['closure_certificate']=write(tmp_path,'certificate.json',cert);write(tmp_path,'policy.json',policy)
    with pytest.raises(f.FeatureWindowRefused,match='UNAVAILABLE_DELIVERY'):
        reader(tmp_path,'1h',fake).read_feature_window(**request)
    assert not fake.calls


def test_policy_and_evidence_tampering_before_and_during_read(tmp_path):
    _,fake,request=setup(tmp_path)
    r=reader(tmp_path,'1h',fake)
    fake.during=lambda:(tmp_path/'evidence.json').write_text('{}')
    with pytest.raises(IntegrityError,match='binding changed'):
        r.read_feature_window(**request)
    assert len(fake.calls)==2
    with pytest.raises(IntegrityError,match='binding changed'):
        r.read_feature_window(**request)


def test_no_active_pointer_refuses(tmp_path):
    with pytest.raises(IntegrityError,match='required file does not exist'):f.FeatureWindowReader(tmp_path)


def test_active_requires_positive_real_validation(tmp_path):
    policy,_,_=setup(tmp_path)
    validation=write(tmp_path,'validation.json',dict(status='PASS_RECOVERY_PHASE',
        validation_status='PASS_FEATURE_WINDOW_VALIDATION',policy_sha256=sha256_file(tmp_path/'policy.json'),
        supported=policy['supported'],demonstrated_new_windows=0))
    supervision=write(tmp_path,'supervision.json',dict(status='PASS_SUPERVISED_PHASE',worker_result_sha256=validation['sha256']))
    write(tmp_path,f.POINTER,dict(schema='active_feature_window_admission/1',
        manifest={'path':'policy.json','sha256':sha256_file(tmp_path/'policy.json')},validation=validation,supervision=supervision))
    with pytest.raises(IntegrityError,match='real-window validation'):f.FeatureWindowReader(tmp_path)


def test_union_before_alignment_and_partial_closure_not_skipped():
    h=f.HOUR
    closures=[dict(start_ns=LEFT+2*h,end_ns=LEFT+2*h+h//2,state='CLOSED',available_at_ns=0,evidence_id='a'),
        dict(start_ns=LEFT+2*h+h//2,end_ns=LEFT+4*h,state='CLOSED',available_at_ns=0,evidence_id='b')]
    result=f.plan_segments(closures=closures,start_ns=LEFT,end_ns=LEFT+6*h,as_of_ns=LEFT+6*h,step=h,lookback_bars=4)
    assert len(result['reads'])==2
    assert result['skipped_closures'][0]['evidence_ids']==['a','b']
    partial=f.plan_segments(closures=closures[:1],start_ns=LEFT,end_ns=LEFT+6*h,as_of_ns=LEFT+6*h,step=h,lookback_bars=4)
    assert partial['skipped_closures']==[]


def test_permission_span_and_overlapping_entries_fail_closed():
    entries=[dict(a=0,b=2,uses=['LINEAGE_AUDIT']),dict(a=2,b=4,uses=['LINEAGE_AUDIT','FEATURE_GENERATION'])]
    f.require_span(entries,start_ns=0,end_ns=4,start_key='a',end_key='b',accepts=lambda x:'LINEAGE_AUDIT' in x['uses'],reason='PERMISSION')
    with pytest.raises(f.FeatureWindowRefused):
        f.require_span(entries,start_ns=0,end_ns=4,start_key='a',end_key='b',accepts=lambda x:'FEATURE_GENERATION' in x['uses'],reason='PERMISSION')
    entries[1]['a']=1
    with pytest.raises(f.FeatureWindowRefused):
        f.require_span(entries,start_ns=0,end_ns=4,start_key='a',end_key='b',accepts=lambda x:True,reason='AMBIGUOUS')


@pytest.mark.parametrize('tf', ['1h', '1d'])
def test_exact_availability_and_previous_completed_bar(tmp_path, tf):
    _, fake, request = setup(tmp_path, tf=tf, closure=None, present=tuple(range(6)))
    step = f.HOUR if tf == '1h' else f.DAY
    r = reader(tmp_path, tf, fake)
    assert not (tmp_path/'data/active').exists()
    with pytest.raises(f.FeatureWindowRefused, match='ADMITTED_SEGMENT_REFUSED'):
        r.read_feature_window(**(request | {'as_of_ns': request['as_of_ns']-1}))
    assert len(r.read_feature_window(**request)['rows']) == 4
    # The following bar is forming. An explicitly requested preceding endpoint
    # remains valid; this does not authorize automatically retreating endpoints.
    previous = request | {'end_ns': LEFT+5*step, 'as_of_ns': LEFT+5*step+f.LAG}
    assert r.read_feature_window(**previous)['rows'][-1]['bar_end_ns'] == previous['end_ns']


def test_duplicate_admitted_rows_and_denied_purpose(tmp_path):
    _, fake, request = setup(tmp_path)
    r = reader(tmp_path, '1h', fake)
    with pytest.raises(f.FeatureWindowRefused, match='UNSUPPORTED_MARKET_HORIZON_PURPOSE'):
        r.read_feature_window(**(request | {'purpose': 'UNDECLARED_USE'}))
    assert not fake.calls
    original = fake.read_window
    def duplicate(**kwargs):
        rows = original(**kwargs)
        return rows + [rows[-1]]
    fake.read_window = duplicate
    with pytest.raises(IntegrityError, match='incomplete feature segment'):
        r.read_feature_window(**request)
