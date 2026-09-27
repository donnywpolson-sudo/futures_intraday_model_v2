"""Generated DBN, real receipt checks, dormant gateway and publication recovery."""
from dataclasses import replace,asdict
from pathlib import Path
import json
import os
import sys
import pytest

from futures_rebuild import causal_1s_native as native
from futures_rebuild import causal_timeframe_view as view
from futures_rebuild import boundary as boundary_module
from futures_rebuild.boundary import RepoBoundary,OperationReceipt,OperationClassification
from futures_rebuild.canonical import sha256_file,sha256_json,canonical_bytes
from futures_rebuild.causal_timeframe_storage import signed,contained,read_record,allocation
from futures_rebuild.causal_1s import build_candidate,certify_candidate
from futures_rebuild.errors import IntegrityError,UnauthorizedOperation,ContractError
from tests.test_causal_1s import sample,spec,LIMITS,RULEBOOK,generated_dbn,BASE,DAY

pytestmark=pytest.mark.current


def put(root,path,value):
    target=contained(root,path)
    target.parent.mkdir(parents=True,exist_ok=True)
    target.write_bytes(canonical_bytes(value)+b'\n')
    return target


def approved(root,operation,scope,*,history=False):
    return OperationReceipt.issue_user_approved(RepoBoundary(root),operation=operation,
        classification=(OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION if history
                        else OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION),
        scope=scope,approval_command='GENERATED_FIXTURE_ONLY',approval_plan_id='a'*64,
        approval_plan_sha256='b'*64,
        approval_line='APPROVE GENERATED_FIXTURE_ONLY PLAN '+'a'*64+' SHA256 '+'b'*64)


def native_fixture(root,monkeypatch,*,start_date=None):
    first=BASE-3*DAY
    last=first+31*DAY
    if start_date is not None:
        first=native.timestamp(start_date)
    entries=generated_dbn(root,compressed=True,window=(first,last))
    moved=[]
    for e in entries:
        old=contained(root,e['path'])
        relative=e['path'].replace('synthetic/','data/dbn/',1)
        target=contained(root,relative)
        target.parent.mkdir(parents=True,exist_ok=True)
        os.rename(old,target)
        if e['kind']=='SIDECAR':
            value=json.loads(target.read_bytes())
            value['path']=value['path'].replace('synthetic/','data/dbn/',1)
            target.write_bytes(canonical_bytes(value)+b'\n')
        moved.append({**e,'path':relative,'sha256':sha256_file(target),'size_bytes':target.stat().st_size})
    s=replace(spec(first,last),synthetic_only=False,source_manifest_id=sha256_json(moved))
    op=signed({'repository_root':str(root),'spec':asdict(s),'spec_id':s.id,'month':'2024-03','through_month':'2024-03','start_date':start_date,
        'entries':moved,'limits':asdict(LIMITS),'resume':False,
        'output':native.BASE+'/generated_run','controls':{},'implementation':{}})
    operation_path=native.BASE+'/operation.json'
    put(root,operation_path,op)
    # Install the actual proposed gateway code into this generated repository only.
    live=Path(boundary_module.__file__).parent/'research_gateway_policy.py'
    policy=contained(root,'src/futures_rebuild/research_gateway_policy.py')
    policy.parent.mkdir(parents=True)
    policy.write_bytes(live.read_bytes())
    code,adoption=native.prepare_adoption(root,op,operation_path)
    policy.write_bytes(code.encode())
    put(root,native.ADOPTION,adoption)
    (root/'configs/contract_economics_rules.json').write_bytes(
        (Path(native.__file__).resolve().parents[2]/'configs/contract_economics_rules.json').read_bytes())
    namespace={'__file__':str(policy),'__name__':'futures_rebuild.fixture_gateway','__package__':'futures_rebuild'}
    exec(compile(code,str(policy),'exec'),namespace)
    monkeypatch.setattr(boundary_module,'require_current_real_history_operation',namespace['require_current_real_history_operation'])
    receipt=approved(root,native.OPERATION,native.scope(op),history=True)
    return op,receipt,s


def test_native_guard_denies_before_output_or_source_open(tmp_path):
    with pytest.raises(UnauthorizedOperation):
        build_candidate(tmp_path/'absent',replace(spec(),synthetic_only=False),lambda:iter([sample()]),RULEBOOK,LIMITS)
    assert not (tmp_path/'absent').exists()
    with pytest.raises(UnauthorizedOperation): native.HistoricalDbnSources(tmp_path,{},None)


def test_month_window_preserves_partial_development_boundary():
    from futures_rebuild.causal_1s import CUTOFF
    start,end=native.month_window('2024-03')
    assert end-start==31*DAY
    assert native.month_window('2025-07')[1]==CUTOFF
    with pytest.raises(UnauthorizedOperation): native.month_window('2025-08')


@pytest.mark.parametrize('start_date',[None,'2024-03-04'])
def test_prepare_exact_partial_start_with_real_sidecar_checks(tmp_path,monkeypatch,start_date):
    from futures_rebuild import causal_source_closure as closure
    from futures_rebuild import foundation_operation_firewall as firewall
    from futures_rebuild import causal_1s_sources as sources
    op,_,s=native_fixture(tmp_path,monkeypatch,start_date=start_date)
    entries=[{**e,'admitted_standard_foundation':True,
        'interval_start_inclusive':start_date or '2024-03-01',
        'interval_end_exclusive':'2024-04-01'} for e in op['entries']]
    put(tmp_path,'inventory.json',{'entries':entries})
    put(tmp_path,'configs/source_contract.json',{'contract_id':s.source_contract_id,
        'complete_inventory':{'path':'inventory.json'},
        'active_canonical_source':{'release_id':s.source_release_id}})
    for path in ('configs/active_dbn_congruence_release_v1.json',
                 'configs/active_alpha_research_ladder.json',
                 'configs/causal_observation_contract_v1.json','data/active/catalog.json'):
        put(tmp_path,path,{'fixture':True})
    # Only registry authority is substituted. Selection, sidecars, coverage and dates are real.
    monkeypatch.setattr(firewall,'issue_current_source_closure_context',lambda root:None)
    monkeypatch.setattr(closure,'validate_source_contract_metadata',lambda *a,**k:None)
    monkeypatch.setattr(closure,'validate_full_build_selection_contract',lambda *a:None)
    for name in ('iter_bars','iter_definitions'):
        monkeypatch.setattr(sources,name,lambda *a,**k:pytest.fail('preparation opened a payload'))
    prepared=native.prepare(tmp_path,start_date=start_date)
    assert prepared['spec']['start_ns']==s.start_ns
    assert prepared['spec']['end_ns']==s.end_ns
    assert prepared['start_date']==start_date
    later=native.prepare(tmp_path,start_date='2024-03-05')
    assert later['id']!=prepared['id'] and later['spec_id']!=prepared['spec_id']
    if start_date is not None:
        with pytest.raises(IntegrityError,match='coverage has a gap'):
            native.prepare(tmp_path)
    else:
        assert native.prepare(tmp_path)==prepared


@pytest.mark.parametrize('date',['2024-3-04','2024-03-32','2024-02-29','2024-04-01',
    '2024-03-04T00:00:00Z',True,42,''])
def test_invalid_start_date_rejected_before_metadata(tmp_path,date):
    with pytest.raises(ContractError): native.prepare(tmp_path,start_date=date)


def test_explicit_start_cannot_cross_development_cutoff(tmp_path):
    with pytest.raises(ContractError):
        native.prepare(tmp_path,month='2025-07',start_date='2025-07-14')


def test_native_receipt_and_actual_dbn_complete_replay(tmp_path,monkeypatch):
    op,receipt,s=native_fixture(tmp_path,monkeypatch)
    with pytest.raises(UnauthorizedOperation):
        native.HistoricalDbnSources(tmp_path,op,receipt,_worker_token=native._WORKER_TOKEN)
    required=native.authorize(tmp_path,op,receipt,consumed=False)
    receipt.consume(RepoBoundary(tmp_path),operation=native.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,required_scope=required)
    factory=native.HistoricalDbnSources(tmp_path,op,receipt,_worker_token=native._WORKER_TOKEN)
    destination=contained(tmp_path,op['output'])
    limits=replace(LIMITS,maximum_written_bytes=LIMITS.maximum_written_bytes-128*1024)
    candidate,_=build_candidate(destination,s,factory,RULEBOOK,limits)
    cert,_=certify_candidate(destination,s,factory,RULEBOOK,limits)
    assert candidate['observations']==60
    assert cert['status']=='HISTORICAL_RECONSTRUCTION_CERTIFIED_NOT_ACTIVE'
    assert not any(cert['authority'].values())
    assert len(list(destination.rglob('*.parquet')))==5
    # Installed scope alone is not reusable authority, and a scope mutation fails.
    with pytest.raises(UnauthorizedOperation): receipt.consume(RepoBoundary(tmp_path),operation=native.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,required_scope=required)
    with pytest.raises((IntegrityError,UnauthorizedOperation)):
        native.authorize(tmp_path,{**op,'id':'f'*64},receipt,consumed=True)
    adopted=read_record(contained(tmp_path,native.ADOPTION))
    put(tmp_path,native.ADOPTION,signed({**{k:v for k,v in adopted.items() if k!='id'},'state':'REVOKED'}))
    with pytest.raises(UnauthorizedOperation): factory.check()


def test_live_gateway_still_denies_native_operation(tmp_path):
    from futures_rebuild.research_gateway_policy import require_current_real_history_operation
    with pytest.raises(UnauthorizedOperation):
        require_current_real_history_operation(native.OPERATION,{'operation_id':'a'*64,'claim':native.CLAIM})


def _generated_worker():
    """Only the registry-preparation result is substituted; real worker/gateway/DBN remain."""
    from io import BytesIO
    from types import SimpleNamespace
    request=json.loads(sys.stdin.buffer.readline())
    op=request['operation']
    policy=Path(op['repository_root'])/'src/futures_rebuild/research_gateway_policy.py'
    namespace={'__file__':str(policy),'__name__':'futures_rebuild.fixture_gateway','__package__':'futures_rebuild'}
    exec(compile(policy.read_text(),str(policy),'exec'),namespace)
    boundary_module.require_current_real_history_operation=namespace['require_current_real_history_operation']
    def prepared(*a,**kw):
        assert kw['start_date']==op['start_date']
        return op
    native.prepare=prepared
    if request.get('fixture_failure'):
        from futures_rebuild import causal_1s_sources as sources
        original=sources.iter_bars
        def reversed_primary(*args,**kwargs):
            rows=list(original(*args,**kwargs))
            yield from reversed(rows) if kwargs['schema']=='ohlcv-1s' else rows
        sources.iter_bars=reversed_primary
    sys.stdin=SimpleNamespace(buffer=BytesIO(canonical_bytes(request)+b'\n'))
    try: native._worker()
    except BaseException:
        import traceback
        (Path(op['repository_root'])/'generated_failure.txt').write_text(traceback.format_exc()[-8192:])
        raise


@pytest.mark.skipif(os.name!='nt',reason='Windows native supervisor')
@pytest.mark.parametrize('start_date',[None,'2024-03-04'])
def test_complete_supervised_month_on_generated_native_dbn(tmp_path,monkeypatch,start_date):
    op,receipt,s=native_fixture(tmp_path,monkeypatch,start_date=start_date)
    def prepared(*a,**kw):
        assert kw['start_date']==start_date
        return op
    monkeypatch.setattr(native,'prepare',prepared)
    original=native.supervise
    def generated(command,root,request,limits):
        return original([sys.executable,'-B','-c',
            'from tests.test_causal_1s_native import _generated_worker; _generated_worker()'],root,request,limits)
    monkeypatch.setattr(native,'supervise',generated)
    # A fixture cwd has no installed tests package; import only this verified code checkout.
    monkeypatch.setenv('PYTHONPATH',str(Path(__file__).resolve().parents[1])+os.pathsep+str(Path(native.__file__).resolve().parents[1]))
    try: certificate=native.execute_once(tmp_path,op,receipt)
    except BaseException:
        failure=tmp_path/'generated_failure.txt'
        if failure.exists(): pytest.fail(failure.read_text())
        raise
    assert certificate['passes'][0]['rows']==60
    output=contained(tmp_path,op['output'])
    finish=read_record(output/f"supervision/{op['id']}.finish.json")
    assert finish['status']=='COMPLETE' and finish['logs_bytes']==0
    coverage=read_record(output/'2024-03.seal.json')['coverage']
    assert len(coverage)==(31 if start_date is None else 28)
    assert coverage[0]['start_ns']==s.start_ns
    assert not (tmp_path/'data/active').exists()
    assert len(list(output.rglob('*.parquet')))==5
    original_payloads={p:(p.stat().st_ino,sha256_file(p)) for p in output.rglob('*.parquet')}
    resumed=signed({**{k:v for k,v in op.items() if k!='id'},'resume':True})
    operation_path=native.BASE+'/resume-operation.json'
    operation_file=put(tmp_path,operation_path,resumed)
    gateway,adopted=native.prepare_adoption(tmp_path,resumed,operation_path)
    assert gateway.encode()==contained(tmp_path,'src/futures_rebuild/research_gateway_policy.py').read_bytes()
    put(tmp_path,native.ADOPTION,adopted)
    monkeypatch.setattr(native,'prepare',lambda *a,**kw:resumed)
    resume_receipt=approved(tmp_path,native.OPERATION,native.scope(resumed),history=True)
    assert native.execute_once(tmp_path,resumed,resume_receipt)==certificate
    assert {p:(p.stat().st_ino,sha256_file(p)) for p in original_payloads}==original_payloads
    assert len(list((output/'supervision').glob('*.finish.json')))==2
    resumed_finish=read_record(output/f"supervision/{resumed['id']}.finish.json")
    assert resumed_finish['child_application_written_bytes']>finish['child_application_written_bytes']


def test_failure_diagnostics_omit_dynamic_values_and_bound_traceback():
    from types import SimpleNamespace
    secret='unlogged-market-value-'+'9'*10000
    def fail(depth):
        if depth: return fail(depth-1)
        raise ValueError(secret)
    try:
        try: fail(20)
        except ValueError as exc: raise IntegrityError('static guard failed') from exc
    except IntegrityError as exc:
        record=signed({'phase':'BUILD','error_type':type(exc).__name__,**native.failure_details(exc,
            SimpleNamespace(processing_day_ns=BASE,source_family='ohlcv_1s',
                source_paths={'ohlcv_1s':'data/dbn/ohlcv_1s/ES/generated.dbn.zst'}))})
    raw=canonical_bytes(record)
    assert len(raw)+1<4096 and secret.encode() not in raw
    assert record['errors'][0]['message']=='static guard failed'
    assert record['errors'][1]['message']=='DYNAMIC_MESSAGE_OMITTED'
    assert len(record['errors'][1]['traceback'])==4
    assert all(set(f)=={'file','function','line'} for err in record['errors'] for f in err['traceback'])


@pytest.mark.skipif(os.name!='nt',reason='Windows native supervisor')
def test_supervised_source_failure_retains_safe_diagnostics(tmp_path,monkeypatch):
    op,receipt,s=native_fixture(tmp_path,monkeypatch)
    monkeypatch.setattr(native,'prepare',lambda *a,**kw:op)
    original=native.supervise
    def generated(command,root,request,limits):
        return original([sys.executable,'-B','-c',
            'from tests.test_causal_1s_native import _generated_worker; _generated_worker()'],
            root,{**request,'fixture_failure':True},limits)
    monkeypatch.setattr(native,'supervise',generated)
    monkeypatch.setenv('PYTHONPATH',str(Path(__file__).resolve().parents[1])+os.pathsep+str(Path(native.__file__).resolve().parents[1]))
    with pytest.raises(IntegrityError,match='worker failed'):
        native.execute_once(tmp_path,op,receipt)
    output=contained(tmp_path,op['output'])
    path=output/f"worker.{op['id']}.failure.json"
    failure=read_record(path)
    assert path.stat().st_size<4096
    assert failure['phase']=='BUILD' and failure['error_type']=='IntegrityError'
    assert failure['errors'][0]['message']=='source ordering regressed'
    assert failure['errors'][0]['traceback'][-1]['function']=='_records'
    assert failure['processing_day_ns']==BASE
    assert failure['last_read_family']=='ohlcv_1s'
    assert failure['last_read_source'].startswith('data/dbn/')
    assert read_record(output/f"supervision/{op['id']}.finish.json")['status']=='FAILED'
    assert not (output/'certificate.json').exists() and not (tmp_path/'data/active').exists()


def test_prior_state_converges_after_first_observed_day():
    from futures_rebuild.causal_1s import produce_day
    # The only cross-day producer state is the final observation, independent
    # of the incoming state. A preceding observed month warms a diagnostic scope.
    day=sample()
    _,last=produce_day(day,spec(),RULEBOOK,None)
    different={**last,'actual_contract':'DIFFERENT','bar_end_ns':BASE-DAY}
    tables,warmed=produce_day(day,spec(),RULEBOOK,different)
    assert warmed==last
    following=sample(day=BASE+DAY)
    later_spec=spec(BASE,BASE+2*DAY)
    assert produce_day(following,later_spec,RULEBOOK,warmed)==produce_day(following,later_spec,RULEBOOK,last)


def test_source_diagnostic_context_is_reset_before_each_pass(monkeypatch):
    from futures_rebuild.causal_1s_sources import SyntheticDbnSources
    source=SyntheticDbnSources.__new__(SyntheticDbnSources)
    source.processing_day_ns=BASE
    source.source_family='ohlcv_1s'
    source.source_paths={'ohlcv_1s':'previous-pass'}
    def fail(): raise IntegrityError('fixture metadata failure')
    monkeypatch.setattr(source,'_validated',fail)
    with pytest.raises(IntegrityError,match='fixture metadata failure'): next(source())
    assert source.processing_day_ns is source.source_family is None
    assert source.source_paths=={}


@pytest.mark.skipif(os.name!='nt',reason='Windows production supervisor')
def test_supervisor_stops_blocked_worker_and_memory_limit(tmp_path):
    cap=replace(LIMITS,maximum_seconds=1)
    with pytest.raises(IntegrityError,match='runtime'):
        native.supervise([sys.executable,'-B','-c','import sys,time;sys.stdin.readline();time.sleep(30)'],tmp_path,{},cap)
    with pytest.raises(IntegrityError,match='worker failed'):
        native.supervise([sys.executable,'-B','-c','import sys;sys.stdin.readline();x=bytearray(256*1024*1024)'],
            tmp_path,{},replace(LIMITS,maximum_memory_bytes=96*2**20))


def publication_fixture(root,*,legacy=False):
    relative='state/unpublished_evidence/generated/candidate.json'
    destination=contained(root,str(Path(relative).parent).replace('\\','/'))
    build_candidate(destination,spec(),lambda:iter([sample()]),RULEBOOK,LIMITS)
    certify_candidate(destination,spec(),lambda:iter([sample()]),RULEBOOK,LIMITS)
    if legacy:
        from tests.test_active_data_view import _write_stage,H
        stage,_=_write_stage(root,plan_id=H['active'],markets=[('ES',2024)])
        (root/'data').mkdir(parents=True,exist_ok=True)
        stage.rename(root/'data/active')
    else: put(root,'data/active/catalog.json',{'legacy_fixture':True})
    (root/'state/locks').mkdir(parents=True,exist_ok=True)
    plan=view.prepare_publication(root,candidates=[relative],expected=[('ES','2024-03')])
    receipt=approved(root,'PUBLISH_CAUSAL_1S_RELEASE_ONCE',
        {'publication_id':plan['id'],'limits_id':sha256_json(asdict(LIMITS))})
    return destination,plan,receipt


@pytest.mark.parametrize('interrupt',['none','rename','index'])
def test_publication_is_rename_only_and_recovers_before_index(tmp_path,monkeypatch,interrupt):
    destination,plan,receipt=publication_fixture(tmp_path)
    originals={p.name:(p.stat().st_ino,sha256_file(p)) for p in destination.rglob('*.parquet')}
    old_replace,old_rename=os.replace,os.rename
    if interrupt=='index':
        def broken(a,b):
            if str(b).endswith('timeframes.json'): raise RuntimeError('interrupted index')
            return old_replace(a,b)
        monkeypatch.setattr(os,'replace',broken)
    elif interrupt=='rename':
        def broken(a,b):
            old_rename(a,b)
            raise RuntimeError('interrupted after move')
        monkeypatch.setattr(os,'rename',broken)
    if interrupt!='none':
        with pytest.raises(RuntimeError): view.publish(tmp_path,plan,receipt,LIMITS)
        assert not (tmp_path/view.INDEX).exists()
        assert {p.name for p in (tmp_path/'data/active').iterdir()}=={'catalog.json'}
        monkeypatch.setattr(os,'replace',old_replace)
        monkeypatch.setattr(os,'rename',old_rename)
    index=view.publish(tmp_path,plan,receipt,LIMITS)
    paths=view.resolve_partitions(repository_root=tmp_path,market='ES',year=2024,timeframe='1s',purpose='SYNTHETIC_TEST')
    assert len(paths)==1
    assert view.registered_active_metadata(tmp_path/'data/active')=={'timeframes.json'}
    assert {p.name:(p.stat().st_ino,sha256_file(p)) for p in paths[0].parent.glob('*.parquet')}==originals
    assert not list(destination.rglob('*.parquet'))
    assert view.publish(tmp_path,plan,receipt,LIMITS)==index
    with pytest.raises(UnauthorizedOperation): view.resolve_partitions(repository_root=tmp_path,market='ES',year=2024,timeframe='1s',purpose='RESEARCH')
    assert allocation(tmp_path,128)['retained_allocated_bytes']>0


def test_publication_rejects_partial_declared_campaign_and_predecessor_drift(tmp_path):
    destination,plan,receipt=publication_fixture(tmp_path)
    with pytest.raises(IntegrityError,match='every declared'):
        view.prepare_publication(tmp_path,candidates=plan['candidates'],expected=[('ES','2024-03'),('NQ','2024-03')])
    put(tmp_path,view.INDEX,signed({'unexpected_revision':True}))
    with pytest.raises(IntegrityError,match='predecessor'):
        view.publish(tmp_path,plan,receipt,LIMITS)
    assert len(list(destination.rglob('*.parquet')))==5


def test_v2_publication_keeps_legacy_resolver_available_on_interruption(tmp_path,monkeypatch):
    from futures_rebuild import active_data_view as active
    _,plan,receipt=publication_fixture(tmp_path,legacy=True)
    kwargs=dict(repository_root=tmp_path,market='ES',year=2024,purpose='LINEAGE_AUDIT')
    before=active.resolve(**kwargs)
    digest=sha256_file(before)
    original=os.replace
    def fail_commit(a,b):
        if str(b).endswith('timeframes.json'): raise RuntimeError('fixture commit interruption')
        return original(a,b)
    monkeypatch.setattr(os,'replace',fail_commit)
    with pytest.raises(RuntimeError): view.publish(tmp_path,plan,receipt,LIMITS)
    assert active.resolve(**kwargs)==before and sha256_file(before)==digest
    monkeypatch.setattr(os,'replace',original)
    view.publish(tmp_path,plan,receipt,LIMITS)
    monkeypatch.setattr(view,'_validate_release',lambda *a:pytest.fail('legacy resolver opened 1s payloads'))
    assert active.resolve(**kwargs)==before
    assert active.resolve_partitions(**kwargs,timeframe='1m')==(before,)
    assert sha256_file(before)==digest
