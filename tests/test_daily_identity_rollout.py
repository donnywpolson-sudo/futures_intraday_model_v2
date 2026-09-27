"""Generated failures at every coordinated replacement and supervised resource gate."""
from pathlib import Path
import sys
import time

import pytest

from futures_rebuild import daily_identity_rollout as rollout
from futures_rebuild.canonical import sha256_file, canonical_bytes
from futures_rebuild.causal_timeframe_storage import Limits, read_record, signed
from futures_rebuild.errors import IntegrityError

pytestmark=pytest.mark.current


@pytest.mark.parametrize('failed_at',range(7))
def test_six_control_failures_and_failed_default_restore_exact_predecessors(tmp_path,monkeypatch,failed_at):
    import shutil
    root=Path(__file__).resolve().parents[1]
    relative='state/unpublished_evidence/daily_foundation_v1/prepare_coordinated_publication.py'
    target=tmp_path/relative; target.parent.mkdir(parents=True); shutil.copyfile(root/relative,target)
    changes={}; expected={}; originals={}
    for number,path in enumerate(rollout.CHANGES):
        target=tmp_path/path; target.parent.mkdir(parents=True,exist_ok=True)
        # Originally absent daily pointer exercises reverse restoration of absence.
        original=None if number==5 else ('old-'+str(number)).encode()
        if original is not None: target.write_bytes(original)
        originals[path]=original
        expected[path]=sha256_file(target) if original is not None else None
        candidate='state/unpublished_evidence/generated/'+str(number)
        target=tmp_path/candidate; target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(('new-'+str(number)).encode()); changes[path]=candidate
    original_replace=rollout.os.replace
    writes=[]
    def replace(source,destination):
        if str(source).endswith('.pending'):
            index=int(Path(source).stem)
            if index==failed_at: raise OSError('injected control replacement failure')
            writes.append(index)
        return original_replace(source,destination)
    monkeypatch.setattr(rollout.os,'replace',replace)
    def defaults(): raise IntegrityError('injected default read failure')
    with pytest.raises((OSError,IntegrityError)):
        rollout.activate_passing(tmp_path,changes,expected,rollback='state/unpublished_evidence/rollback',verify_default=defaults)
    for path,original in originals.items():
        target=tmp_path/path
        assert target.read_bytes()==original if original is not None else not target.exists()
    assert writes==list(range(min(failed_at,6)))


@pytest.mark.parametrize('field,message',[('coordinator','Coordinator'),('worker','Worker'),('aggregate','Aggregate'),('deadline','Deadline')])
def test_distinct_resource_gate_failures(field,message):
    measured=dict(coordinator={'peak_bytes':2},worker={'peak_bytes':3})
    args=dict(coordinator_limit=3,worker_limit=4,aggregate_limit=6,deadline=time.time()+30)
    if field=='coordinator': measured['coordinator']['peak_bytes']=4
    elif field=='worker': measured['worker']['peak_bytes']=5
    elif field=='aggregate': args['aggregate_limit']=4
    else: args['deadline']=time.time()-1
    with pytest.raises(IntegrityError,match=message): rollout.resource_gate(measured,**args)


@pytest.mark.parametrize('exit_code',[0,3])
def test_supervisor_records_actual_job_and_process_peaks_on_success_and_failure(tmp_path,exit_code):
    command=[sys.executable,'-B','-c',
        'import sys,json; request=json.loads(sys.stdin.buffer.readline()); data=bytearray(32*2**20); '
        'data[-1]=1; sys.exit('+str(exit_code)+')']
    path=tmp_path/'supervision.json'
    limits=Limits(4*2**20,512*2**20,20,minimum_free_bytes=1,maximum_files=10)
    if exit_code:
        with pytest.raises(IntegrityError):
            rollout.supervise(command,tmp_path,{'phase':'generated'},limits,diagnostic=path)
    else:
        rollout.supervise(command,tmp_path,{'phase':'generated'},limits,diagnostic=path)
    result=read_record(path)
    assert result['exit_code']==exit_code
    assert result['peak_job_committed_bytes']>=32*2**20
    assert result['worker']['peak_working_set_bytes']>0
    assert result['worker']['peak_committed_bytes']>0
    assert result['coordinator']['peak_bytes']>0
    assert result['status']==('FAILED_SUPERVISION' if exit_code else 'PASS_SUPERVISED_PHASE')


@pytest.mark.parametrize('fault',['plan_changed','missing_worker_evidence','wrong_previous_phase'])
def test_coordinator_refuses_unbound_phase_evidence(tmp_path,monkeypatch,fault):
    from dataclasses import asdict
    def save(path,value):
        target=tmp_path/path; target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(canonical_bytes(signed(value))+b'\n'); return sha256_file(target)
    digest=save('plan.json',dict(prepared=True))
    scope=signed(dict(schema='daily_identity_recovery_coordinator/1',plan_id='a'*64,
        plan=dict(path='plan.json',sha256=digest),output='out',limits=asdict(Limits(1024,1024,30))))
    save('auth.json',dict(status='OWNER_APPROVED_IN_THIS_TASK',plan_id=scope['plan_id'],
        coordinator_scope_id=scope['id'],plan_sha256=digest,started_epoch=time.time(),deadline_epoch=time.time()+20))
    monkeypatch.setattr(rollout,'memory',lambda:dict(peak_bytes=1))
    if fault=='plan_changed': (tmp_path/'plan.json').write_bytes(b'changed')
    elif fault=='wrong_previous_phase':
        save('out/phases/reconstruct/acceptance.json',dict(status='FAILED_PHASE_ACCEPTANCE'))
    else:
        def missing(*args,diagnostic,**kwargs):
            diagnostic.parent.mkdir(parents=True,exist_ok=True)
            diagnostic.write_bytes(canonical_bytes(signed(dict(status='PASS_SUPERVISED_PHASE')))+b'\n')
            return dict(worker_result_sha256='b'*64)
        monkeypatch.setattr(rollout,'supervise',missing)
    with pytest.raises((IntegrityError,FileNotFoundError)):
        rollout.run_phase(tmp_path,scope,'auth.json','stage' if fault=='wrong_previous_phase' else 'reconstruct')
    if fault=='missing_worker_evidence':
        assert read_record(tmp_path/'out/phases/reconstruct/acceptance.json')['status']=='FAILED_PHASE_ACCEPTANCE'
