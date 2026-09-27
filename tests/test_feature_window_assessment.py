"""Generated endpoint census cross-checked against the public feature API."""
import pytest
import numpy as np

from futures_rebuild import feature_window_assessment as a
from futures_rebuild.errors import IntegrityError
from futures_rebuild.feature_window_reader import FeatureWindowRefused
from tests.test_feature_window_reader import LEFT, setup, reader

pytestmark=pytest.mark.current


def inputs(fake, *, tf='1h', closures=True):
    step=a.HOUR if tf=='1h' else a.DAY
    rows=[dict(r,quality_accepted=True,permitted_uses=['FEATURE_GENERATION']) for r in fake.rows.values()]
    return dict(market='TEST',timeframe=tf,left=LEFT,right=LEFT+6*step,rows=rows,
        permissions=[dict(start_ns=LEFT,end_ns=LEFT+6*step,permitted_uses=['FEATURE_GENERATION'])],
        unavailable=[],closures=[] if not closures else [dict(start_ns=LEFT+2*step,end_ns=LEFT+4*step,
            available_at_ns=0,state='CLOSED',evidence_id='fixture')],lengths=[1,2,3,4,5],purposes=['FEATURE_GENERATION'])


@pytest.mark.parametrize('tf',['1h','1d'])
def test_same_endpoints_match_public_reader(tmp_path,tf):
    _,fake,request=setup(tmp_path,tf=tf)
    r=reader(tmp_path,tf,fake);args=inputs(fake,tf=tf)
    census=a.compare_endpoints(**args)
    step=a.HOUR if tf=='1h' else a.DAY
    for length in args['lengths']:
        success=0
        for endpoint in range(1,6):
            end=LEFT+endpoint*step
            try:r.read_feature_window(**(request|dict(end_ns=end,as_of_ns=end+a.LAG,lookback_bars=length)))
            except FeatureWindowRefused:pass
            else:success+=1
        record=next(x for x in census if x['method']=='calendar_native_bars' and x['lookback_bars']==length)
        assert record['endpoints']==6 and record['readable_windows']==success
        assert record['unique_refusals']+success==6
    third=[x for x in census if x['lookback_bars']==3]
    assert third[0]['readable_windows']==0
    assert third[1]['readable_windows']==third[1]['newly_readable_from_certified_closures']==1


@pytest.mark.parametrize('change,reason',[
    ('unknown','missing_coverage'),('late','missing_coverage'),('identity','contract_or_unit'),
    ('excluded','excluded_observation'),('delivery','unavailable_delivery'),
    ('permission','permission_or_cohort'),('premature','timing')])
def test_blockers_and_overlapping_refusals(tmp_path,change,reason):
    _,fake,_=setup(tmp_path);args=inputs(fake);args['lengths']=[3]
    if change=='unknown':args['closures']=[]
    if change=='late':args['closures'][0]['available_at_ns']=LEFT+6*a.HOUR
    if change=='identity':args['rows'][2]['actual_contract']='TESTM0'
    if change=='excluded':args['rows'][2]['quality_accepted']=False
    if change=='delivery':args['unavailable']=[(LEFT+2*a.HOUR,LEFT+4*a.HOUR)]
    if change=='permission':args['permissions'][0]['end_ns']=LEFT+3*a.HOUR
    if change=='premature':args['rows'][2]['decision_eligible_at_ns']=LEFT+6*a.HOUR
    record=a.compare_endpoints(**args)[1]
    assert record['readable_windows']==0 and record['unique_refusals']==6
    assert record['overlapping_reasons'][reason]>0
    assert sum(record['overlapping_reasons'].values())>=record['unique_refusals']


def test_unknown_trailing_gap_no_retreat(tmp_path):
    _,fake,_=setup(tmp_path,present=(0,1),closure=(2,4))
    args=inputs(fake);args['lengths']=[2]
    record=a.compare_endpoints(**args)[1]
    # Endpoints 2,3,4 have two bars and only certified trailing closures.
    # Endpoint 5 is unknown and must not return the same old successful history.
    assert record['readable_windows']==3


def test_dense_methods_identical_and_outside_unavailable_ignored(tmp_path):
    _,fake,_=setup(tmp_path,present=tuple(range(6)),closure=None)
    args=inputs(fake,closures=False)
    args['unavailable']=[(LEFT-3*a.HOUR,LEFT-a.HOUR),(LEFT+9*a.HOUR,LEFT+10*a.HOUR)]
    records=a.compare_endpoints(**args)
    for length in args['lengths']:
        strict,calendar=[x for x in records if x['lookback_bars']==length]
        assert strict['readable_windows']==calendar['readable_windows']==6-length
        assert calendar['newly_readable_from_certified_closures']==0


def test_overlapping_partial_proofs_need_both_to_be_known():
    h=a.HOUR
    closures=[dict(start_ns=LEFT,end_ns=LEFT+h//2,available_at_ns=10,state='CLOSED',evidence_id='a'),
        dict(start_ns=LEFT+h//2,end_ns=LEFT+h,available_at_ns=20,state='CLOSED',evidence_id='b')]
    result=a.closure_availability(closures,left=LEFT,right=LEFT+2*h,step=h)
    assert list(result)==[20,np.iinfo(np.int64).max]
    closures.append(dict(start_ns=LEFT,end_ns=LEFT+h,available_at_ns=15,state='CLOSED',evidence_id='c'))
    assert a.closure_availability(closures,left=LEFT,right=LEFT+2*h,step=h)[0]==15


def test_bad_certificate_and_duplicate_rows_refused(tmp_path):
    _,fake,_=setup(tmp_path);args=inputs(fake)
    args['closures'][0]['start_ns']=LEFT
    with pytest.raises(IntegrityError,match='conflicts'):a.compare_endpoints(**args)
    args['closures']=[];args['rows'].append(args['rows'][0])
    with pytest.raises(IntegrityError,match='duplicate'):a.compare_endpoints(**args)


def test_range_maxima_matches_brute_force():
    values=np.array([3,8,4,2,9,1]);starts=np.array([0,0,1,1,3,3])
    assert list(a.range_maxima(values,starts))==[max(values[s:e],default=0) for e,s in enumerate(starts)]
