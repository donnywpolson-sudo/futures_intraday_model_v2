"""Generated source through dedicated supervisor, native worker and OS limits."""
from dataclasses import asdict,replace
from pathlib import Path
import io,json,os,sys
import pytest
from futures_rebuild import causal_1d_native as native,boundary as boundary_module
from futures_rebuild.canonical import canonical_bytes,sha256_json,sha256_file
from futures_rebuild.causal_timeframe_storage import read_record,signed,contained
from futures_rebuild.errors import IntegrityError,UnauthorizedOperation
from tests.test_causal_1d import generated_dbn,spec,LIMITS,BASE,DAY
from tests.test_causal_1s_native import approved,put

pytestmark=pytest.mark.current


def fixture(root,monkeypatch):
    entries=generated_dbn(root)
    moved=[]
    for e in entries:
        old=contained(root,e['path'])
        relative=e['path'].replace('synthetic/','data/dbn/',1)
        target=contained(root,relative)
        target.parent.mkdir(parents=True,exist_ok=True)
        os.rename(old,target)
        if e['kind']=='SIDECAR':
            value=json.loads(target.read_bytes())
            value['path']=relative.removesuffix('.manifest.json')
            target.write_bytes(canonical_bytes(value)+b'\n')
        moved.append({**e,'path':relative,'sha256':sha256_file(target),'size_bytes':target.stat().st_size})
    s=replace(spec(),synthetic_only=False,source_manifest_id=sha256_json(moved))
    op=signed({'schema':'causal_1d_native_operation/1','operation':native.OPERATION,'claim':native.CLAIM,
        'repository_root':str(root),'spec':asdict(s),'spec_id':s.id,'month':'2024-03','through_month':'2024-03','start_date':None,
        'entries':moved,'limits':asdict(LIMITS),'aggregate_memory_bytes':LIMITS.maximum_memory_bytes+native.SUPERVISOR_ALLOWANCE,
        'resume':False,'output':native.BASE+'/generated','controls':{},'implementation':native.implementation_binding(),'contract':native.CONTRACT,'environment':native.ENVIRONMENT})
    operation_path=native.BASE+'/operation.json'
    put(root,operation_path,op)
    for name in op['implementation']:
        target=contained(root,'src/futures_rebuild/'+name)
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes((Path(native.__file__).parent/name).read_bytes())
    live=Path(boundary_module.__file__).parent/'research_gateway_policy.py'
    policy=contained(root,'src/futures_rebuild/research_gateway_policy.py')
    policy.parent.mkdir(parents=True,exist_ok=True)
    policy.write_bytes(live.read_bytes())
    code,adoption=native.prepare_adoption(root,op,operation_path)
    policy.write_text(code)
    # Hash exactly the bytes written on this platform.
    adoption=signed({**{k:v for k,v in adoption.items() if k!='id'},'gateway_sha256':sha256_file(policy)})
    put(root,native.ADOPTION,adoption)
    (root/'configs/contract_economics_rules.json').write_bytes((Path(native.__file__).resolve().parents[2]/'configs/contract_economics_rules.json').read_bytes())
    configure(root,op,monkeypatch)
    receipt=approved(root,native.OPERATION,native.scope(op),history=True)
    return op,receipt


def configure(root,op,monkeypatch=None):
    policy=contained(root,'src/futures_rebuild/research_gateway_policy.py')
    namespace={'__file__':str(policy),'__name__':'futures_rebuild.fixture_gateway','__package__':'futures_rebuild'}
    exec(compile(policy.read_text(),str(policy),'exec'),namespace)
    if monkeypatch is None:
        boundary_module.require_current_real_history_operation=namespace['require_current_real_history_operation']
        native.prepare=lambda *a,**k:op
    else:
        monkeypatch.setattr(boundary_module,'require_current_real_history_operation',namespace['require_current_real_history_operation'])
        monkeypatch.setattr(native,'prepare',lambda *a,**k:op)


def generated_worker():
    raw=sys.stdin.buffer.readline(2**20)
    request=json.loads(raw)
    configure(Path(request['operation']['repository_root']),request['operation'])
    sys.stdin=io.TextIOWrapper(io.BytesIO(raw))
    native._worker()


def generated_supervisor():
    raw=sys.stdin.buffer.readline(2**20)
    request=json.loads(raw)
    configure(Path(request['operation']['repository_root']),request['operation'])
    original=native.supervise
    native.supervise=lambda command,root,request,limits:original([sys.executable,'-B','-c',
        'from tests.test_causal_1d_native import generated_worker; generated_worker()'],root,request,limits)
    sys.stdin=io.TextIOWrapper(io.BytesIO(raw))
    native._supervisor_entry()


@pytest.mark.skipif(os.name!='nt',reason='Windows supervision')
def test_supervised_generated_build_and_two_replays(tmp_path,monkeypatch):
    original_gateway=boundary_module.require_current_real_history_operation
    op,receipt=fixture(tmp_path,monkeypatch)
    original=native.subprocess.Popen
    def generated(command,**kwargs):
        return original([sys.executable,'-B','-c',
            'from tests.test_causal_1d_native import generated_supervisor; generated_supervisor()'],**kwargs)
    monkeypatch.setattr(native.subprocess,'Popen',generated)
    root=Path(__file__).resolve().parents[1]
    monkeypatch.setenv('PYTHONPATH',str(root)+os.pathsep+str(root/'src'))
    try:
        cert=native.execute_once(tmp_path,op,receipt)
        assert cert['passes'][0]==cert['passes'][1] and cert['passes'][0]['rows']==1
        aggregate=read_record(contained(tmp_path,op['output'])/'aggregate_memory.json')
        assert 0<aggregate['peak_supervisor_worker_commit_bytes']<=op['aggregate_memory_bytes']
        assert aggregate['coordinator_outside_job'] is True
        assert aggregate['coordinator_peak_memory_bytes']>0
        assert (contained(tmp_path,op['output'])/'provenance.json').exists()
        from futures_rebuild import causal_timeframe_publication as pub
        catalog=contained(tmp_path,'data/active/catalog.json')
        catalog.parent.mkdir(parents=True,exist_ok=True)
        catalog.write_text('{}')
        candidate_path=op['output']+'/candidate.json'
        plan=pub.prepare_publication(tmp_path,candidates=[candidate_path])
        publish_receipt=approved(tmp_path,pub.OPERATION,
            {'publication_id':plan['id'],'limits_id':sha256_json(asdict(LIMITS))})
        index=pub.publish(tmp_path,plan,publish_receipt,LIMITS)
        paths=pub.resolve_partitions(tmp_path,index,market='ES',year=2024,timeframe='1d',purpose='LINEAGE_AUDIT')
        assert len(paths)==1
        release=pub.validate_release(tmp_path,index['entries'][0])
        assert release['candidate']['source_inventory']==op['entries']
        assert release['candidate']['operation']==op
        assert release['candidate']['permitted_uses']==['LINEAGE_AUDIT']
        with pytest.raises(UnauthorizedOperation):
            pub.resolve_partitions(tmp_path,index,market='ES',year=2024,timeframe='1d',purpose='RESEARCH')
        proof=read_record(contained(tmp_path,op['output'])/'provenance.json')
        for field in ('source_inventory','operation'):
            record=dict(release['candidate'])
            record[field]=[]
            with pytest.raises(IntegrityError,match='daily source inventory or operation'):
                pub.verify_provenance(tmp_path,record,cert,proof)
        proof.pop('aggregate_memory')
        with pytest.raises(IntegrityError,match='aggregate supervision required'):
            pub.verify_provenance(tmp_path,release['candidate'],cert,proof)
    finally: boundary_module.require_current_real_history_operation=original_gateway


def test_private_supervisor_requires_actual_job():
    with pytest.raises(UnauthorizedOperation): native._verify_aggregate_job('Local\\nonexistent_daily_job',2**30)


def test_historical_builder_rejects_plain_source_factory(tmp_path):
    from futures_rebuild.causal_1d import build_candidate
    from tests.test_causal_1d import sample,RULEBOOK
    with pytest.raises(UnauthorizedOperation):
        build_candidate(tmp_path/'absent',replace(spec(),synthetic_only=False),lambda:iter([sample()]),RULEBOOK,LIMITS)
    assert not (tmp_path/'absent').exists()
