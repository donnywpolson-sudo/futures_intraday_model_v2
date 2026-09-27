"""Production receipt verification against generated, exactly adopted scopes."""
from pathlib import Path
import importlib.util
import io
import shutil
import sys

import pytest

from futures_rebuild import daily_identity_authorization as auth, boundary
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.causal_timeframe_storage import read_record, signed
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation

pytestmark=pytest.mark.current
ROOT=Path(__file__).resolve().parents[1]


def write(root,path,value):
    target=root/path; target.parent.mkdir(parents=True,exist_ok=True)
    target.write_bytes(canonical_bytes(signed(value))+b'\n'); return sha256_file(target)


def prepared(root):
    for path in (auth.MODULE,auth.POLICY):
        target=root/path; target.parent.mkdir(parents=True,exist_ok=True); shutil.copyfile(ROOT/path,target)
    planpath=auth.BASE+'/plan.json'
    digest=write(root,planpath,dict(historical_execution=False))
    plan=read_record(root/planpath)
    scopepath=auth.BASE+'/coordinator_scope.json'
    write(root,scopepath,dict(schema='daily_identity_recovery_coordinator/1',
        plan_id=plan['id'],plan=dict(path=planpath,sha256=digest)))
    successor,adoption=auth.prepare_adoption(root,scopepath)
    (root/auth.POLICY).write_bytes(successor)
    write(root,auth.ADOPTION,{k:v for k,v in adoption.items() if k!='id'})
    namespace=dict(__name__='futures_rebuild._generated_recovery_gateway',__package__='futures_rebuild',__file__=str(root/auth.POLICY))
    exec(compile(successor,str(root/auth.POLICY),'exec'),namespace)
    operation_scope=dict(plan_id=plan['id'],claim=auth.CLAIM)
    receipt=boundary.OperationReceipt.issue_user_approved(boundary.RepoBoundary(root),operation=auth.OPERATION,
        classification=boundary.OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,scope=operation_scope,
        approval_command=auth.COMMAND,approval_plan_id=plan['id'],approval_plan_sha256=digest,
        approval_line=boundary._personal_approval_line(auth.COMMAND,plan['id'],digest))
    return receipt,namespace,planpath,scopepath


def verify(root,receipt):
    receipt.verify(boundary.RepoBoundary(root),operation=auth.OPERATION,
        classification=boundary.OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,required_scope=dict(receipt.scope))


def test_real_receipt_gate_adoption_and_single_use(tmp_path,monkeypatch):
    receipt,policy,_,_=prepared(tmp_path)
    # Reproduces the precise omitted check, using the unchanged production gate.
    with pytest.raises(UnauthorizedOperation,match='retired'): verify(tmp_path,receipt)
    monkeypatch.setattr(boundary,'require_current_real_history_operation',policy['require_current_real_history_operation'])
    verify(tmp_path,receipt)
    args=dict(operation=auth.OPERATION,classification=receipt.classification,required_scope=dict(receipt.scope))
    receipt.consume(boundary.RepoBoundary(tmp_path),**args)
    receipt.assert_consumed(boundary.RepoBoundary(tmp_path),**args)
    with pytest.raises(UnauthorizedOperation,match='already used'): receipt.consume(boundary.RepoBoundary(tmp_path),**args)
    from futures_rebuild import research_gateway_policy as original
    assert policy['PREPARATORY_REAL_HISTORY_OPERATIONS']==original.PREPARATORY_REAL_HISTORY_OPERATIONS
    assert policy['CERTIFIED_TRIAL_EXECUTION_OPERATION']==original.CERTIFIED_TRIAL_EXECUTION_OPERATION
    with pytest.raises(UnauthorizedOperation): policy['require_current_real_history_operation']('UNREGISTERED_RESEARCH',{})


@pytest.mark.parametrize('changed',['absent','plan','scope','implementation','gateway','wrong_claim','wrong_plan','extra_scope'])
def test_production_receipt_gate_fails_closed(tmp_path,monkeypatch,changed):
    receipt,policy,planpath,scopepath=prepared(tmp_path)
    monkeypatch.setattr(boundary,'require_current_real_history_operation',policy['require_current_real_history_operation'])
    if changed=='absent': (tmp_path/auth.ADOPTION).rename(tmp_path/'saved_adoption.json')
    elif changed in ('plan','scope','implementation','gateway'):
        path={'plan':planpath,'scope':scopepath,'implementation':auth.MODULE,'gateway':auth.POLICY}[changed]
        with (tmp_path/path).open('ab') as stream: stream.write(b' ')
    else:
        scope=dict(receipt.scope)
        if changed=='wrong_claim': scope['claim']='FIT_MODEL'
        elif changed=='wrong_plan': scope['plan_id']='a'*64
        else: scope['extra']='unapproved'
        with pytest.raises(UnauthorizedOperation): auth.require_scope(tmp_path,scope)
        return
    with pytest.raises((IntegrityError,UnauthorizedOperation,FileNotFoundError)): verify(tmp_path,receipt)


def test_versioned_worker_retains_early_exception(tmp_path,monkeypatch):
    path=ROOT/auth.BASE/'rollout.py'
    spec=importlib.util.spec_from_file_location('futures_rebuild._generated_rollout_v2',path)
    module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    (tmp_path/auth.BASE).mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys,'stdin',io.TextIOWrapper(io.BytesIO(b'{"phase":"reconstruct","authorization":"absent.json"}\n')))
    assert module.worker()==1
    result=read_record(tmp_path/auth.BASE/'startup_failure.json')
    assert result['status']=='FAILED_WORKER_STARTUP' and result['error_type']=='IntegrityError'
    assert 'absent.json' in result['error']
    assert result['memory']['peak_bytes']>0


def test_supervisor_keeps_exact_receipt_startup_failure(tmp_path):
    import time
    from futures_rebuild.causal_timeframe_storage import Limits
    source=ROOT/auth.BASE/'rollout.py'
    target=tmp_path/auth.BASE/'rollout.py'; target.parent.mkdir(parents=True)
    shutil.copyfile(source,target)
    spec=importlib.util.spec_from_file_location('futures_rebuild._generated_startup_supervisor',source)
    module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    planpath=auth.BASE+'/plan.json'
    digest=write(tmp_path,planpath,dict(limits={'maximum_memory_bytes':5*2**30}))
    plan=read_record(tmp_path/planpath)
    receipt=boundary.OperationReceipt.issue_user_approved(boundary.RepoBoundary(tmp_path),operation=auth.OPERATION,
        classification=boundary.OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={'plan_id':plan['id'],'claim':auth.CLAIM},approval_command=auth.COMMAND,
        approval_plan_id=plan['id'],approval_plan_sha256=digest,
        approval_line=boundary._personal_approval_line(auth.COMMAND,plan['id'],digest))
    write(tmp_path,'auth.json',dict(status='OWNER_APPROVED_IN_THIS_TASK',deadline_epoch=time.time()+30,
        plan_id=plan['id'],plan_sha256=digest,historical_receipt=receipt.as_dict()))
    command=[sys.executable,'-B','-c',
        'import importlib.util; s=importlib.util.spec_from_file_location("futures_rebuild._generated_worker",'+repr(str(target))+'); '
        'm=importlib.util.module_from_spec(s); s.loader.exec_module(m); raise SystemExit(m.worker())']
    with pytest.raises(IntegrityError,match='retired'):
        module.supervise(command,tmp_path,dict(phase='reconstruct',authorization='auth.json',plan=planpath),
            Limits(2**20,5*2**30,30),diagnostic=tmp_path/'supervision.json')
    result=read_record(tmp_path/'supervision.json')
    assert result['error_type']=='UnauthorizedOperation' and result['startup_evidence_sha256']
    assert result['process_peak_evidence']=='WORKER_TERMINAL_LIFETIME_PEAKS_AND_JOB_PEAK'


@pytest.mark.parametrize('failed_at',range(9))
def test_authorization_then_six_controls_restore_all_predecessors(tmp_path,monkeypatch,failed_at):
    source=ROOT/auth.BASE/'rollout.py'
    spec=importlib.util.spec_from_file_location('futures_rebuild._generated_nested_rollout',source)
    module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    helper_path='state/unpublished_evidence/daily_foundation_v1/prepare_coordinated_publication.py'
    target=tmp_path/helper_path; target.parent.mkdir(parents=True); shutil.copyfile(ROOT/helper_path,target)
    paths=[auth.POLICY,auth.ADOPTION,*module.CHANGES]
    changes={}; expected={}; originals={}
    for n,path in enumerate(paths):
        target=tmp_path/path; target.parent.mkdir(parents=True,exist_ok=True)
        original=None if n==1 else f'old{n}'.encode()
        if original is not None: target.write_bytes(original)
        originals[path]=original; expected[path]=sha256_file(target) if original is not None else None
        candidate=f'state/unpublished_evidence/generated/new{n}'
        target=tmp_path/candidate; target.parent.mkdir(parents=True,exist_ok=True); target.write_bytes(f'new{n}'.encode())
        changes[path]=candidate
    original_replace=module.os.replace
    count=0
    def replace(source,destination):
        nonlocal count
        if str(source).endswith('.pending'):
            current=count; count+=1
            if current==failed_at: raise OSError('injected adoption/commit failure')
        return original_replace(source,destination)
    monkeypatch.setattr(module.os,'replace',replace)
    def defaults(): raise IntegrityError('injected final acceptance failure')
    def data_commit():
        module.activate_passing(tmp_path,{p:changes[p] for p in paths[2:]},{p:expected[p] for p in paths[2:]},
            rollback='state/unpublished_evidence/data_rollback',verify_default=defaults)
    with pytest.raises((OSError,IntegrityError)):
        module.helper(tmp_path).activate_controls(tmp_path,{p:changes[p] for p in paths[:2]},
            expected={p:expected[p] for p in paths[:2]},rollback_directory='state/unpublished_evidence/auth_rollback',validate=data_commit)
    for path,raw in originals.items():
        assert (tmp_path/path).read_bytes()==raw if raw is not None else not (tmp_path/path).exists()
