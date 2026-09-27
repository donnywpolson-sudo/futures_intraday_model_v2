"""Private pilot orchestration, generated physical inputs only."""
import io
import json
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from futures_rebuild import annual_foundation_pilot as pilot
from futures_rebuild import daily_identity_rollout as supervision
from futures_rebuild.canonical import canonical_bytes, sha256_file, io_path
from futures_rebuild.causal_timeframe_storage import signed, read_record
from futures_rebuild.errors import IntegrityError
from tests.test_annual_foundation_storage import source, pack, budget, FILENAMES

pytestmark=pytest.mark.current


def save(root,path,value):
    p=root/path; p.parent.mkdir(parents=True,exist_ok=True)
    p.write_bytes(canonical_bytes(signed(value))+b'\n'); return p


def fixture_plan(root):
    s=source(root,'2024-01',groups=2)
    compact='data/active/causally_gated_normalized/ES/2024/2024.parquet'
    target=root/compact; target.parent.mkdir(parents=True)
    pq.write_table(pq.read_table(root/s['directory']/'observations.parquet'),target)
    catalog=save(root,'data/active/catalog.json',dict(entries=[dict(parquet_path=compact,
        parquet_sha256=sha256_file(target),coverage_end='2024-02-01')]))
    unit=dict(market='ES',year=2024,timeframe='1m',sources=[s],coverage=[dict(month='2024-01',state='CERTIFIED')])
    payloads={s['directory']+'/'+f:{**s['files'][n],'role':'SELECTED_MONTHLY_TABLE'} for n,f in FILENAMES.items()}
    payloads[compact]=dict(sha256=sha256_file(target),size=target.stat().st_size,role='CURRENT_MINUTE_EQUIVALENCE')
    path=save(root,'plan.json',dict(units=[unit],physical_payloads=payloads,
         metadata_and_implementation_bindings={'data/active/catalog.json':sha256_file(catalog)}))
    return path,unit


def fixture_scope(root):
    from dataclasses import asdict
    from futures_rebuild.causal_timeframe_storage import Limits
    plan,_=fixture_plan(root)
    scope=save(root,'scope.json',dict(output=pilot.OUTPUT,plan=dict(path='plan.json',sha256=sha256_file(plan)),
         limits=asdict(Limits(64*2**20,5*2**30,180,maximum_files=2000,maximum_group_bytes=64*2**20)),
         maximum_allocation_bytes=64*2**20,known_prior_measured_writes=123))
    auth=save(root,'auth.json',dict(status='OWNER_APPROVED_IN_THIS_TASK',scope_id=read_record(scope)['id'],
         scope_sha256=sha256_file(scope),started_epoch=time.time(),deadline_epoch=time.time()+120))
    return scope,auth


def fake_supervise(monkeypatch):
    def call(command,root,request,limits,*,diagnostic):
        request={**request,'supervision_job_name':'synthetic'}
        monkeypatch.chdir(root)
        monkeypatch.setattr(sys,'stdin',io.TextIOWrapper(io.BytesIO(canonical_bytes(request))))
        pilot.worker()
        sample=supervision.memory()
        result=dict(status='PASS_SUPERVISED_PHASE',worker=sample,coordinator=sample)
        save(root,diagnostic.relative_to(io_path(root)),result)
        return result
    monkeypatch.setattr(supervision,'supervise',call)
    monkeypatch.setattr(supervision,'verify_job',lambda *args:None)


def test_real_orchestration_with_generated_inputs(tmp_path,monkeypatch):
    fixture_scope(tmp_path); fake_supervise(monkeypatch)
    result=pilot.run(tmp_path,scope_path='scope.json',authorization_path='auth.json')
    assert result['status']=='PASS_ISOLATED_PILOT_NOT_ADMITTED'
    phases=read_record(tmp_path/pilot.OUTPUT/'compare.json')
    assert phases['units'][0]['comparison']['counts']['observations']==2
    assert phases['units'][0]['comparison']['compact_equivalence']['row_count']==2
    assert len(result['phases'])==2
    with pytest.raises(FileExistsError):
        pilot.run(tmp_path,scope_path='scope.json',authorization_path='auth.json')


def test_fresh_supervised_workers_with_generated_inputs(tmp_path):
    fixture_scope(tmp_path)
    result=pilot.run(tmp_path,scope_path='scope.json',authorization_path='auth.json')
    assert result['status']=='PASS_ISOLATED_PILOT_NOT_ADMITTED'
    directory=tmp_path/pilot.OUTPUT
    evidence={name:read_record(directory/(name+'.json')) for name in
              ('pack','compare','pack_supervision','compare_supervision','completion')}
    import os
    output=os.environ.get('ANNUAL_PILOT_SYNTHETIC_REPORT')
    if output:
        with Path(output).open('xb') as f: f.write(canonical_bytes(signed(evidence))+b'\n')


@pytest.mark.parametrize('fault',['membership','row_value'])
def test_independent_comparison_rejects_self_consistent_wrong_candidate(tmp_path,fault):
    s=source(tmp_path,'2024-01')
    directory,manifest,tracker=pack(tmp_path,[s])
    unit={k:manifest[k] for k in ('market','timeframe','year','sources','coverage')}
    # A self-consistent candidate with changed declared coverage must not be certified
    # just because its own record has a valid digest.
    wrong=dict(manifest)
    if fault=='membership':
        wrong['coverage']=manifest['coverage'][:1]
    else:
        import pyarrow as pa
        from futures_rebuild.annual_foundation_storage import _digest, validate_candidate
        target=directory/'observations.parquet'; table=pq.read_table(target)
        index=table.schema.get_field_index('volume')
        table=table.set_column(index,table.schema.field(index),pa.array([999],type=table.schema.field(index).type))
        pq.write_table(table,target)
        wrong['files']['observations'].update(size=target.stat().st_size,sha256=sha256_file(target))
        wrong['groups'][0]['tables']['observations']['digest']=_digest(pq.read_table(target))
    wrong.pop('id'); save(tmp_path,directory.relative_to(io_path(tmp_path))/'manifest.json',wrong)
    if fault=='row_value':
        assert validate_candidate(directory,manifest_sha256=sha256_file(directory/'manifest.json'))['status']=='PASS_INACTIVE_PACKAGING_READBACK'
    with pytest.raises(IntegrityError,match='membership|row or field mismatch'):
        pilot.compare_sources(tmp_path,directory,unit,manifest_sha256=sha256_file(directory/'manifest.json'),budget=tracker)


@pytest.mark.parametrize('fault',['changed_binding','deadline','missing_worker','interrupted_write'])
def test_failures_keep_originals_and_write_terminal_evidence(tmp_path,monkeypatch,fault):
    fixture_scope(tmp_path); fake_supervise(monkeypatch)
    sources={p:sha256_file(p) for p in (tmp_path/'data').rglob('*.parquet')}
    if fault=='changed_binding':
        (tmp_path/'data/active/catalog.json').write_bytes(b'changed')
    elif fault=='deadline':
        a=read_record(tmp_path/'auth.json'); a.pop('id'); a['deadline_epoch']=time.time()-1
        save(tmp_path,'auth.json',a)
    else:
        def fail(*args,diagnostic,**kwargs):
            if fault=='interrupted_write':
                target=tmp_path/pilot.OUTPUT/'partial.parquet'; target.write_bytes(b'partial')
            save(tmp_path,diagnostic.relative_to(io_path(tmp_path)),dict(status='FAILED_SUPERVISION'))
            raise IntegrityError('injected worker failure')
        monkeypatch.setattr(supervision,'supervise',fail)
    with pytest.raises((IntegrityError,SystemExit)):
        pilot.run(tmp_path,scope_path='scope.json',authorization_path='auth.json')
    assert all(sha256_file(p)==h for p,h in sources.items())
    completion=tmp_path/pilot.OUTPUT/'completion.json'
    if fault=='deadline': assert not completion.exists()
    else:
        result=read_record(completion)
        assert result['status']=='FAILED_PILOT'
        if fault in ('missing_worker','interrupted_write'):
            assert result['write_accounting']=='INCOMPLETE_WORKER_TERMINAL_EVIDENCE'
    if fault=='interrupted_write': assert (tmp_path/pilot.OUTPUT/'partial.parquet').read_bytes()==b'partial'
