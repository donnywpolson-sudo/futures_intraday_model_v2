"""Synthetic diagnostics; never read the active historical release."""
from dataclasses import asdict, replace
import inspect
import random

import pytest

from futures_rebuild import coverage_remediation as rem
from futures_rebuild.errors import IntegrityError
from tests.test_causal_1d import BASE, sample

pytestmark=pytest.mark.current


def row(i, *, identity='ESH4', delay=0):
    start=BASE+i*rem.DAY
    return dict(market='ES',publisher_id=1,instrument_id=1,actual_contract=identity,
        currency='USD',min_price_increment_nano=1,multiplier_nano=50,
        bar_start_ns=start,bar_end_ns=start+rem.DAY,
        available_at_ns=start+rem.DAY+rem.LAG,
        decision_eligible_at_ns=start+rem.DAY+rem.LAG+delay)


def profile(rows, *, n=6,lengths=(1,5),permissions=None,quarantines=()):
    return rem.window_profile(market='ES',step=rem.DAY,left=BASE,right=BASE+n*rem.DAY,
        rows=rows,permissions=permissions if permissions is not None else [dict(
            start_ns=BASE,end_ns=BASE+n*rem.DAY,permitted_uses=['LINEAGE_AUDIT'])],
        quarantines=quarantines,lengths=lengths,purposes=['LINEAGE_AUDIT'])


def test_dense_windows_and_no_weekend_compression():
    result=profile([row(i) for i in range(6)])
    assert [(r['candidate_windows'],r['readable_windows']) for r in result]==[(6,6),(2,2)]
    result=profile([row(i) for i in range(6) if i!=2])
    assert [(r['readable_windows'],r['reason_counts']['missing_coverage']) for r in result]==[(5,1),(0,2)]


def test_overlapping_reasons_count_refusal_once():
    permissions=[dict(start_ns=BASE,end_ns=BASE+2*rem.DAY,permitted_uses=['LINEAGE_AUDIT'])]
    r=profile([row(0),row(1),row(3,identity='ESM4'),row(4),row(5)],permissions=permissions,
              quarantines=[BASE+2*rem.DAY])[-1]
    assert r['refused_windows']==2
    assert r['reason_counts']['quarantine']==2
    assert r['reason_counts']['permission_or_cohort']==2
    assert r['reason_counts']['contract_or_unit']==2
    assert r['reason_counts']['missing_coverage']==0


def test_transition_before_window_does_not_contaminate_it():
    r=profile([row(0),row(2,identity='ESM4'),row(3,identity='ESM4')],n=4,lengths=(2,))[0]
    assert r['readable_windows']==1
    assert r['reason_counts']['contract_or_unit']==0


def test_availability_compared_with_window_end_not_individual_end():
    rows=[row(0,delay=rem.DAY),row(1)]
    assert profile(rows,n=2,lengths=(1,))[0]['reason_counts']['timing']==1
    assert profile(rows,n=2,lengths=(2,))[0]['readable_windows']==1


def test_permissions_across_cohorts_require_whole_window():
    entries=[dict(start_ns=BASE,end_ns=BASE+rem.DAY,permitted_uses=['LINEAGE_AUDIT']),
             dict(start_ns=BASE+rem.DAY,end_ns=BASE+2*rem.DAY,permitted_uses=[])]
    assert profile([row(0),row(1)],n=2,lengths=(2,),permissions=entries)[0]['readable_windows']==0
    entries[1]['permitted_uses']=['LINEAGE_AUDIT']
    assert profile([row(0),row(1)],n=2,lengths=(2,),permissions=entries)[0]['readable_windows']==1


def test_profile_rejects_duplicates_and_invalid_timing():
    with pytest.raises(IntegrityError,match='Duplicate'): profile([row(0),row(0)])
    invalid=row(0); invalid['available_at_ns']-=1
    with pytest.raises(IntegrityError,match='timing'): profile([invalid])


def test_prefix_counts_equal_independent_bruteforce():
    rng=random.Random(15)
    rows=[row(i,identity=rng.choice(['A','B']),delay=rng.choice([0,2*rem.DAY]))
          for i in range(24) if rng.random()>.2]
    by_start={r['bar_start_ns']:r for r in rows}
    for length in (1,5,20):
        accepted=0
        for i in range(25-length):
            selected=[by_start.get(BASE+j*rem.DAY) for j in range(i,i+length)]
            if any(r is None for r in selected): continue
            if len({r['actual_contract'] for r in selected})!=1: continue
            if max(r['decision_eligible_at_ns'] for r in selected)>BASE+(i+length)*rem.DAY+rem.LAG: continue
            accepted+=1
        assert profile(rows,n=24,lengths=(length,))[0]['readable_windows']==accepted


def quarantine(definitions):
    return dict(source_row=asdict(sample().bars[0]),matching_definitions=[asdict(d) for d in definitions])


def test_exact_midnight_late_and_absent_definitions():
    definition=sample().definitions[0]
    assert rem.classify_quarantine(quarantine([definition]))['eligible_baselines']==1
    late=replace(definition,ts_recv_ns=BASE+1)
    assert rem.classify_quarantine(quarantine([late]))['reason']=='LATE_SAME_DAY_DEFINITION'
    assert rem.classify_quarantine(quarantine([]))['reason']=='ABSENT_MATCHING_IDENTITY'


def test_date_scoped_identity_never_carried_forward():
    definition=replace(sample().definitions[0],instrument_id_date_utc=rem.utc(BASE-1).date().isoformat(),ts_recv_ns=BASE-1)
    assert rem.classify_quarantine(quarantine([definition]))['reason']=='IDENTITY_MISMATCH'


@pytest.mark.parametrize('kind',['deleted','expired','conflict','publisher'])
def test_invalid_or_ambiguous_definition_cannot_clear_case(kind):
    definition=sample().definitions[0]
    if kind=='deleted': definition=replace(definition,security_update_action='DELETE')
    if kind=='expired': definition=replace(definition,expiration_ns=BASE)
    if kind=='publisher': definition=replace(definition,publisher_id=999)
    definitions=[definition]
    if kind=='conflict': definitions.append(replace(definition,source_file_path='synthetic/other.dbn',raw_symbol='ESM4'))
    result=rem.classify_quarantine(quarantine(definitions))
    assert result['recovered_rows']==0
    assert result['reason'] in ('AMBIGUOUS_OR_INVALID_DEFINITION','IDENTITY_MISMATCH')


def test_cli_is_preparation_only():
    source=inspect.getsource(rem.main)
    assert 'worker(' not in source and 'supervise(' not in source
    assert 'historical_execution=False' in source
    assert 'exist_ok=False' in source


def test_hash_tampering_fails(tmp_path):
    path=tmp_path/'record.txt'; path.write_text('before')
    bindings={'record.txt':rem.sha256_file(path)}
    path.write_text('after')
    class Budget:
        def hash(self,p): return rem.sha256_file(p)
    with pytest.raises(IntegrityError,match='Input changed'):
        rem.verify(tmp_path,bindings,Budget())


def test_current_daily_raw_guards_without_obsolete_preparation_hash(tmp_path):
    from tests.test_daily_quarantine import publish
    from tests.test_causal_1d import empty, spec, DAY
    from futures_rebuild.daily_foundation_admission import check_raw_routes
    reader=publish(tmp_path,[sample(),empty(BASE+DAY)],spec())
    assert check_raw_routes(tmp_path,reader.index)==['publication','timeframe_view','active_data_view']


def test_hourly_lengths_and_exact_cutoff():
    left=rem.CUTOFF-120*rem.HOUR
    rows=[]
    for i in range(120):
        r=row(0);start=left+i*rem.HOUR
        r.update(bar_start_ns=start,bar_end_ns=start+rem.HOUR,
                 available_at_ns=start+rem.HOUR+rem.LAG,decision_eligible_at_ns=start+rem.HOUR+rem.LAG)
        rows.append(r)
    result=rem.window_profile(market='ES',step=rem.HOUR,left=left,right=rem.CUTOFF,
        rows=rows,permissions=[dict(start_ns=left,end_ns=rem.CUTOFF,permitted_uses=['LINEAGE_AUDIT'])],
        quarantines=[],lengths=[1,6,24,120],purposes=['LINEAGE_AUDIT'])
    assert [r['readable_windows'] for r in result]==[120,115,97,1]


def test_month_and_year_boundaries_are_not_compressed():
    from datetime import datetime,timezone
    left=int(datetime(2022,12,31,tzinfo=timezone.utc).timestamp())*rem.SECOND
    rows=[]
    for i in range(2):
        r=row(0); start=left+i*rem.DAY
        r.update(bar_start_ns=start,bar_end_ns=start+rem.DAY,
            available_at_ns=start+rem.DAY+rem.LAG,decision_eligible_at_ns=start+rem.DAY+rem.LAG)
        rows.append(r)
    result=rem.window_profile(market='ES',step=rem.DAY,left=left,right=left+2*rem.DAY,
        rows=rows,permissions=[dict(start_ns=left,end_ns=left+2*rem.DAY,permitted_uses=['LINEAGE_AUDIT'])],
        quarantines=[],lengths=[1,2],purposes=['LINEAGE_AUDIT'])
    assert [(r['year'],r['window_bars'],r['readable_windows']) for r in result]==[(2022,1,1),(2023,1,1),(2022,2,1)]


def prepared_task(tmp_path):
    code=tmp_path/'worker.py';code.write_text('synthetic')
    plan=rem.signed(dict(implementation={'worker.py':rem.sha256_file(code)},limits=rem.asdict(rem.LIMITS)))
    relative=rem.HERE+'/'+plan['id']+'/plan.json'
    path=tmp_path/relative;path.parent.mkdir(parents=True)
    path.write_bytes(rem.canonical_bytes(plan))
    auth=rem.signed(dict(status='OWNER_APPROVED_IN_THIS_TASK',plan_id=plan['id'],
        plan_sha256=rem.sha256_file(path),operation='OFFLINE_COVERAGE_DIAGNOSTICS_NO_ACTIVATION'))
    (path.parent/'authorization.json').write_bytes(rem.canonical_bytes(auth))
    return relative,relative.replace('plan.json','authorization.json')


def test_changed_prepared_code_prevents_worker_launch(tmp_path):
    plan,auth=prepared_task(tmp_path)
    (tmp_path/'worker.py').write_text('tampered')
    with pytest.raises(IntegrityError,match='implementation changed'):
        rem.supervise(tmp_path,plan,auth)


@pytest.mark.parametrize('failure',['deadline exceeded','worker memory exceeded','missing worker evidence'])
def test_supervisor_retains_failure_without_retry(tmp_path,monkeypatch,failure):
    from contextlib import nullcontext
    from futures_rebuild import causal_1h_native,causal_timeframe_execution
    plan,auth=prepared_task(tmp_path)
    calls=[]
    def fail(*args,**kwargs):
        calls.append(1)
        raise IntegrityError(failure)
    monkeypatch.setattr(causal_1h_native,'supervise',fail)
    monkeypatch.setattr(causal_timeframe_execution,'execution_lease',lambda root:nullcontext())
    monkeypatch.setattr(rem,'memory_peak',lambda:1000)
    result=rem.supervise(tmp_path,plan,auth)
    assert result['status']=='FAILED_SUPERVISED_DIAGNOSTICS'
    assert failure in result['error'] and len(calls)==1
    assert (tmp_path/plan).with_name('supervision.json').exists()
    with pytest.raises(IntegrityError,match='No automatic'):
        rem.supervise(tmp_path,plan,auth)
