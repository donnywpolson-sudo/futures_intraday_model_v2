"""Generated inputs only: completed identity, independent tables and retained rows."""
from copy import deepcopy
from dataclasses import asdict, replace

import pytest

from futures_rebuild import daily_identity_recovery as recovery, causal_1d as daily
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError, ContractError
from tests.test_daily_identity_review import evidence
from tests.test_causal_1d import BASE, DAY, RULEBOOK, sample, spec, LIMITS

pytestmark = pytest.mark.current


def fixture():
    item,hours=evidence()
    item['reason']='DEFINITION_NOT_KNOWN_AT_BAR_START'
    empty={k:[] for k in ('observations','missingness','roll','quality','cadence')}
    coverage=[dict(day_ns=BASE,start_ns=BASE,end_ns=BASE+DAY,observations=0,source_primary_rows=1,
        quarantined=1,quarantine=[item],excluded_start=0,excluded_availability=0,
        state='DEFINITION_NOT_KNOWN_AT_BAR_START',comparison_counts={})]
    return empty,coverage,{BASE:(item,hours)}, {'reference_hashes':{hours[0]['source_file_sha256']},'rulebook':RULEBOOK}


def test_independent_five_tables_roundtrip_and_original_bytes_preserved(tmp_path):
    from futures_rebuild.causal_timeframe_storage import MonthlyWriter, Budget, iter_bundle_groups
    original,coverage,inputs,kwargs=fixture()
    before=deepcopy((original,coverage,inputs))
    produced=recovery.recover_month(original,coverage,inputs,**kwargs)
    for _ in range(2):
        assert recovery.recover_month(original,coverage,inputs,**kwargs,replay=True)==produced
    assert before==(original,coverage,inputs)
    tables=produced['tables']
    assert all(len(v)==1 for v in tables.values())
    row=tables['observations'][0]
    source=inputs[BASE][0]['source_row']
    for key in ('open_nano','high_nano','low_nano','close_nano','volume'):
        assert row[key]==source[key]
    assert row['definition_received_at_ns']==inputs[BASE][0]['matching_definitions'][0]['ts_recv_ns']>BASE
    assert row['available_at_ns']==row['decision_eligible_at_ns']==BASE+DAY+recovery.LAG
    recovery.verify_recovered_timing(row,tables['quality'][0],produced['evidence'][0])
    assert produced['coverage'][0]['quarantine']==[]
    writer=MonthlyWriter(tmp_path/'month',Budget(tmp_path,LIMITS))
    try: writer.write(tables)
    finally: writer.close()
    assert next(iter_bundle_groups(tmp_path/'month'))==tables


def test_recovery_repairs_following_roll_and_preserves_retained_ohlcv():
    original,coverage,inputs,kwargs=fixture()
    later,_=daily.produce_day(sample(BASE+DAY,symbol='ESM4'),spec(BASE+DAY,BASE+3*DAY),RULEBOOK,None)
    original=later
    coverage.append(dict(day_ns=BASE+DAY,start_ns=BASE+DAY,end_ns=BASE+2*DAY,
        observations=1,source_primary_rows=1,quarantined=0,quarantine=[],excluded_start=0,
        excluded_availability=0,state='OBSERVED_WITH_EXPLICIT_GAPS',comparison_counts={}))
    produced=recovery.recover_month(original,coverage,inputs,**kwargs)
    assert produced==recovery.recover_month(original,coverage,inputs,**kwargs,replay=True)
    assert produced['tables']['observations'][1]==later['observations'][0]
    assert later['roll'][0]['roll_flag'] is False
    assert produced['tables']['roll'][1]['roll_flag'] is True
    assert produced['tables']['roll'][1]['actual_contract_before']=='ESH4'
    # The same prior carries across month/year processing; no reset at a seal.
    following=recovery.recover_month(later,[coverage[1]],{},**kwargs,prior=produced['tables']['observations'][0])
    assert following['tables']['roll']==produced['tables']['roll'][1:]


@pytest.mark.parametrize('case',['missing','duplicate_day','changed_source','different_quarantine','excluded',
    'publisher','derived','late','delete','mismatch','overlap','cutoff'])
def test_recovery_fails_closed(case):
    original,coverage,inputs,kwargs=fixture()
    item,hours=inputs[BASE]
    if case=='missing': inputs={}
    elif case=='duplicate_day': coverage*=2
    elif case=='changed_source': item['source_row']['event_at_ns']+=DAY
    elif case=='different_quarantine': item['reason']='OTHER'
    elif case=='excluded': hours[0]['quality_disposition']='EXCLUDED'
    elif case=='publisher': hours[0]['publisher_id']+=1
    elif case=='derived': hours[0]['origin']='DERIVED_1H'
    elif case=='late': item['matching_definitions'][0]['ts_recv_ns']=BASE+DAY+recovery.LAG+1
    elif case=='delete': item['matching_definitions'][0]['security_update_action']='DELETE'
    elif case=='mismatch': item['source_row']['volume']+=1
    elif case=='overlap': original['missingness']=[dict(observation_row_id=None,interval_start_ns=BASE,interval_end_ns=BASE+DAY)]
    elif case=='cutoff': item['source_row']['event_at_ns']=recovery.CUTOFF//DAY*DAY
    with pytest.raises((IntegrityError,ContractError,ValueError)):
        recovery.recover_month(original,coverage,inputs,**kwargs)


def test_prior_known_identity_uses_old_pipeline_and_cannot_be_relabeled():
    item,hours=evidence()
    item['matching_definitions'][0]['ts_recv_ns']=BASE
    with pytest.raises(IntegrityError,match='original quarantine'):
        recovery.recover_day(item,hours,reference_hashes={hours[0]['source_file_sha256']},rulebook=RULEBOOK)


@pytest.mark.parametrize('field',['row_id','source_row_sha256','definition_received_at_ns','available_at_ns','first_reference_start_ns'])
def test_rehashed_timing_evidence_does_not_change_row_contract(field):
    original,coverage,inputs,kwargs=fixture()
    produced=recovery.recover_month(original,coverage,inputs,**kwargs)
    proof=produced['evidence'][0]
    proof[field]=('f'*64 if isinstance(proof[field],str) else proof[field]+DAY)
    proof['id']=sha256_json({k:v for k,v in proof.items() if k!='id'})
    with pytest.raises(IntegrityError):
        recovery.verify_recovered_timing(produced['tables']['observations'][0],produced['tables']['quality'][0],proof)
