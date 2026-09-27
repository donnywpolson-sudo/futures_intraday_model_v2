"""Generated observations only. No readers, research access or fitting."""
from copy import deepcopy

import numpy as np
import pytest

from futures_rebuild import mean_reversion_baseline as m
from futures_rebuild.errors import IntegrityError
from futures_rebuild.research.splits import TemporalSamples, purge_and_post_embargo_indices

pytestmark = pytest.mark.current


def fixture():
    decision=m.decision_times('2020-01-02')[0]
    start=decision-m.LAG-61*m.MINUTE
    rows=[]
    for i in range(424):
        t=start+i*m.MINUTE
        price=4000*m.NS+(i%5)*m.NS//4
        if i==60: price-=10*m.NS
        rows.append(dict(market='ES',source_cadence='1m',actual_contract='ESH0',
            publisher_id=1,instrument_id=17,currency='USD',min_price_increment_nano=m.NS//4,
            multiplier_nano=50*m.NS,bar_start_ns=t,bar_end_ns=t+m.MINUTE,
            available_at_ns=t+m.MINUTE+m.LAG,decision_eligible_at_ns=t+m.MINUTE+m.LAG,
            definition_received_at_ns=t-1,listing_activation_ns=start-1,
            expiration_ns=start+1000*m.MINUTE,open_nano=price,close_nano=price,
            high_nano=price+m.NS,low_nano=price-m.NS,volume=10))
    return decision,rows


def test_features_use_61_completed_minutes_and_frozen_previous_mean():
    decision,rows=fixture(); index=m.indexed_rows(rows)
    f=m.feature_at(index,decision)
    assert f['direction']==1 and f['history_start_ns']==rows[0]['bar_start_ns']
    assert f['reference_mean_nano']==sum(r['close_nano'] for r in rows[:60])/60
    assert set(f['features'])==set(m.FEATURES)
    changed=deepcopy(rows)
    for r in changed[61:]: r['close_nano']+=999*m.NS
    assert m.feature_at(m.indexed_rows(changed),decision)==f
    late=deepcopy(rows); late[60]['decision_eligible_at_ns']=decision+1
    with pytest.raises(m.Unavailable,match='causal_availability'):
        m.feature_at(m.indexed_rows(late),decision)
    with pytest.raises(m.Unavailable,match='feature_warmup'):
        m.feature_at(m.indexed_rows(rows[1:]),decision)
    with pytest.raises(m.Unavailable,match='session_restriction'):
        m.feature_at(index,decision-1)


@pytest.mark.parametrize('h',[15,30,60])
@pytest.mark.parametrize('direction',[-1,1])
def test_exact_horizons_signs_frozen_mean_and_exit_high_exclusion(h,direction):
    decision,rows=fixture(); index=m.indexed_rows(rows)
    f=m.feature_at(index,decision)
    entry=f['entry_ns']; price=4000*m.NS
    f.update(direction=direction,reference_mean_nano=price+direction*5*m.NS)
    for i in range(h+1):
        r=index[entry+i*m.MINUTE]; p=price+direction*i*m.NS//4
        r.update(open_nano=p,close_nano=p,high_nano=p,low_nano=p)
    exit_row=index[entry+h*m.MINUTE]
    exit_row['high_nano']+=1000*m.NS
    result=m.label_for(index,f,h,as_of_ns=exit_row['decision_eligible_at_ns'])
    assert result['raw_return_ticks']==direction*h
    assert result['directed_return_ticks']==h
    assert result['mae_ticks']==0 and result['mfe_ticks']==h
    assert result['frozen_mean_nano']==f['reference_mean_nano']
    assert result['exit_ns']==entry+h*m.MINUTE
    with pytest.raises(m.Unavailable,match='label_not_mature'):
        m.label_for(index,f,h,as_of_ns=result['label_known_at_ns']-1)


def test_path_ambiguity_missing_future_and_no_feature_filtering():
    decision,rows=fixture(); index=m.indexed_rows(rows); f=m.feature_at(index,decision)
    entry=index[f['entry_ns']]; p=entry['open_nano']
    f.update(direction=1,reference_mean_nano=p+2*m.NS)
    entry.update(high_nano=p+3*m.NS,low_nano=p-3*m.NS)
    result=m.label_for(index,f,15,as_of_ns=rows[-1]['decision_eligible_at_ns'])
    assert result['path_status']=='AMBIGUOUS_SAME_MINUTE'
    f_before=m.feature_at(index,decision)
    index.pop(f['entry_ns']+5*m.MINUTE)
    assert m.feature_at(index,decision)==f_before
    with pytest.raises(m.Unavailable,match='missing_future_horizon'):
        m.label_for(index,f,15,as_of_ns=rows[-1]['decision_eligible_at_ns'])


@pytest.mark.parametrize('field,value',[('actual_contract','ESM0'),('publisher_id',2),('instrument_id',18)])
def test_feature_and_future_identity_resets(field,value):
    decision,rows=fixture(); index=m.indexed_rows(rows); f=m.feature_at(index,decision)
    future=index[f['entry_ns']]; future[field]=value
    with pytest.raises(m.Unavailable,match='contract_or_unit_transition'):
        m.label_for(index,f,15,as_of_ns=rows[-1]['decision_eligible_at_ns'])
    rows[30][field]=value
    with pytest.raises(m.Unavailable,match='contract_or_unit_transition'):
        m.feature_at(index,decision)


def test_duplicate_units_zero_scale_dst_and_coverage_counts():
    decision,rows=fixture()
    with pytest.raises(IntegrityError,match='Duplicate'):
        m.indexed_rows([rows[0],rows[0]])
    bad=deepcopy(rows);bad[0]['multiplier_nano']=5*m.NS
    with pytest.raises(IntegrityError):m.feature_at(m.indexed_rows(bad),decision)
    flat=deepcopy(rows)
    for r in flat[:61]:r.update(open_nano=4000*m.NS,close_nano=4000*m.NS,high_nano=4000*m.NS,low_nano=4000*m.NS)
    with pytest.raises(m.Unavailable,match='zero_displacement_scale'):
        m.feature_at(m.indexed_rows(flat),decision)
    assert m.decision_times('2020-03-09')[0]-m.decision_times('2020-03-06')[0]==(72-1)*60*m.MINUTE
    assert m.decision_times('2020-01-04')==()
    counts=m.coverage_day('2020-01-02',rows,as_of_ns=rows[-1]['decision_eligible_at_ns'])
    for c in counts:
        assert c['candidates']==300 and c['label_valid']+c['unique_refused']==300
        assert c['eligible_events']==c['long_events']+c['short_events']
        assert not any(k in c for k in ('raw_return_ticks','mae_ticks','mfe_ticks'))


def test_existing_purge_excludes_overlap_at_each_horizon():
    for h in m.HORIZONS:
        samples=TemporalSamples(decision_session=np.array([0,h-1,h+1],dtype=np.int64),
            label_start=np.array([1,h,h+2],dtype=np.int64),
            label_end=np.array([h+1,2*h,2*h+2],dtype=np.int64),
            label_known_session=np.array([h+1,2*h,2*h+2],dtype=np.int64))
        assert purge_and_post_embargo_indices(samples,np.array([0],dtype=np.int64),
                   np.array([1],dtype=np.int64),post_embargo_sessions=0).size==0


def test_readiness_rejects_fake_receipt_before_source_access(tmp_path):
    from futures_rebuild import mean_reversion_readiness as r
    from futures_rebuild.errors import UnauthorizedOperation
    with pytest.raises(UnauthorizedOperation,match='Real readiness receipt'):
        r.execute_once(tmp_path,{},object())
    assert list(tmp_path.iterdir())==[]


def test_coverage_summary_hand_counts_and_empty_day():
    from futures_rebuild import mean_reversion_readiness as r
    decision,rows=fixture()
    records=m.coverage_day('2020-01-02',rows,as_of_ns=rows[-1]['decision_eligible_at_ns'])
    missing=m.coverage_day('2020-01-03',[],as_of_ns=rows[-1]['decision_eligible_at_ns'])
    for x in missing:
        assert x['unique_refused']==300 and x['reasons']['missing_feature_inputs']==300
        assert x['reasons']['feature_warmup']==300
    out=r.summarize(records+missing,[dict(outer=dict(audit_dates=['2020-01-02','2020-01-03'],
        first_date='2020-01-02',start_index=r.weekday_dates().index('2020-01-02')))])
    for profile in out['year_profiles']:
        if profile['year']==2020:
            assert profile['candidates']==600 and profile['label_valid']==300
            assert profile['unique_refused']==300 and profile['retained_fraction']==0.5
    assert not out['all_support_floors_pass']


def test_entry_gap_retains_event_excursions_and_expiry_blocks():
    decision,rows=fixture(); index=m.indexed_rows(rows); f=m.feature_at(index,decision)
    f['reference_mean_nano']=index[f['entry_ns']]['open_nano']-m.NS
    result=m.label_for(index,f,15,as_of_ns=rows[-1]['decision_eligible_at_ns'])
    assert result['path_status']=='MEAN_REACHED_BEFORE_ENTRY' and result['adverse_boundary_nano'] is None
    assert result['mae_ticks']>=0 and result['mfe_ticks']>=0
    index[f['entry_ns']]['expiration_ns']=f['entry_ns']
    with pytest.raises(m.Unavailable,match='contract_lifecycle'):
        m.label_for(index,f,15,as_of_ns=rows[-1]['decision_eligible_at_ns'])


def test_training_coverage_respects_maturity_and_date_embargo():
    from futures_rebuild import mean_reversion_readiness as r
    _,rows=fixture()
    records=m.coverage_day('2020-01-02',rows,as_of_ns=rows[-1]['decision_eligible_at_ns'])
    first='2020-01-06'; start=r.weekday_dates().index(first)
    folds=[dict(outer=dict(first_date=first,start_index=start,audit_dates=[first]))]
    allowed=r.summarize(records,folds)['training_profiles']
    assert all(x['label_valid']==300 for x in allowed)
    # Mature labels become unavailable for fitting if their source receipt is
    # known on the held-out date, even though their event date is earlier.
    later=deepcopy(records)
    for record in later:
        record['maturity_by_date']={first:record['maturity_by_date']['2020-01-02']}
    assert all(x['label_valid']==0 for x in r.summarize(later,folds)['training_profiles'])
    embargo=deepcopy(records)
    for record in embargo:
        record['day']='2020-01-03'
        record['maturity_by_date']={'2020-01-03':record['maturity_by_date']['2020-01-02']}
    assert all(x['label_valid']==0 for x in r.summarize(embargo,folds)['training_profiles'])


def test_missing_labels_preserve_prelabel_concentration_counts():
    decision,rows=fixture()
    complete=m.coverage_day('2020-01-02',rows,as_of_ns=rows[-1]['decision_eligible_at_ns'])
    missing=[row for row in rows if row['bar_start_ns']!=decision-m.LAG+15*m.MINUTE]
    counts=m.coverage_day('2020-01-02',missing,as_of_ns=rows[-1]['decision_eligible_at_ns'])
    # Initial displaced event survives as a feature event but loses its label.
    for record in counts:
        assert record['feature_long_events']>=record['long_events']
        assert sum(record['feature_events_by_hour'].values()) == record['feature_long_events']+record['feature_short_events']
        assert sum(record['events_by_hour'].values()) == record['eligible_events']
        assert record['unique_refused'] > complete[0]['unique_refused']
