"""Generated approval/resource evidence only; no real-data evaluation."""
from pathlib import Path
import time
import pytest

from futures_rebuild import annual_foundation_integration_audit as audit
from futures_rebuild.canonical import sha256_file
from futures_rebuild.causal_timeframe_storage import signed,read_record
from futures_rebuild.errors import IntegrityError
from tests.test_annual_foundation_reader_preview import save

pytestmark=pytest.mark.current


def records(root):
    scope=dict(schema='annual_reader_integration_scope/1',output=audit.OUTPUT,
        active_control_replacements=[],activation=False,implementation={},candidate_bindings={},
        maximum_allocation_bytes=2*2**30,known_prior_measured_writes=123,
        limits=dict(maximum_seconds=5400,maximum_memory_bytes=5*2**30,maximum_written_bytes=2*2**30,
                    maximum_files=2000,minimum_free_bytes=64*2**30))
    path='state/unpublished_evidence/generated_scope.json'
    digest=save(root/path,scope);scope=read_record(root/path)
    auth=dict(status='OWNER_APPROVED_IN_THIS_TASK',scope_id=scope['id'],scope_sha256=digest,
              started_epoch=time.time(),deadline_epoch=time.time()+600)
    apath='state/unpublished_evidence/generated_authorization.json';save(root/apath,auth)
    return path,apath,scope,auth


@pytest.mark.parametrize('change',[{'status':'NOT_APPROVED'}, {'scope_sha256':'0'*64},
    {'scope_id':'0'*64},{'deadline_epoch':0}])
def test_no_implicit_historical_approval(tmp_path,change):
    path,_,scope,auth=records(tmp_path)
    with pytest.raises(IntegrityError,match='approval'):
        audit.validate_authorization(tmp_path,scope,auth|change,path)


def test_protected_or_unlisted_parquet_is_refused_before_open(tmp_path):
    import pyarrow.parquet as pq
    with audit.physical_reads(tmp_path,{'physical_inputs':{}}):
        with pytest.raises(IntegrityError,match='unapproved physical'):
            pq.ParquetFile(tmp_path/'data/dbn/ohlcv_1m/NQ/2026/all.dbn.parquet')
        with pytest.raises(IntegrityError,match='unapproved physical'):
            pq.read_table(tmp_path/'undeclared.parquet')


@pytest.mark.parametrize('failed',[None,'daily_original','deadline'])
def test_coordinator_runs_only_reader_phases_and_never_retries(tmp_path,monkeypatch,failed):
    from futures_rebuild import daily_identity_rollout
    path,apath,scope,auth=records(tmp_path)
    calls=[]
    def supervise(command,root,request,limits,diagnostic):
        phase=request['phase'];calls.append(phase)
        if phase==failed: raise IntegrityError('generated worker failure')
        save(root/request['result_path'],dict(status='PASS_RECOVERY_PHASE',phase=phase,
            new_application_bytes_before_terminal=0,cases={},checks=[]))
        result=dict(status='PASS_SUPERVISED_PHASE',seconds=0.01,
            coordinator={'peak_bytes':100},worker={'peak_bytes':100})
        save(diagnostic,result)
        return result
    monkeypatch.setattr(daily_identity_rollout,'supervise',supervise)
    if failed=='deadline':
        def gate(*args,**kwargs): raise IntegrityError('generated deadline failure')
        monkeypatch.setattr(daily_identity_rollout,'resource_gate',gate)
    if failed:
        with pytest.raises(IntegrityError): audit.run(tmp_path,scope_path=path,authorization_path=apath)
    else:
        assert audit.run(tmp_path,scope_path=path,authorization_path=apath)['status'].startswith('PASS_')
        assert calls==list(audit.PHASES)
    count=len(calls)
    with pytest.raises(FileExistsError): audit.run(tmp_path,scope_path=path,authorization_path=apath)
    assert len(calls)==count
    result=read_record(tmp_path/audit.OUTPUT/'completion.json')
    assert result['active_control_replacements']==[]
    assert not (tmp_path/'data/active').exists()
