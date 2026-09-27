"""Synthetic publication, immutable provenance and concurrent exclusion."""
from dataclasses import asdict,replace
from pathlib import Path
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier,Event
import pytest
from futures_rebuild import causal_timeframe_publication as pub
from futures_rebuild import causal_1h_native as native
from futures_rebuild.causal_timeframe_execution import execution_lease
from futures_rebuild.causal_timeframe_storage import read_record,signed,contained
from futures_rebuild.canonical import canonical_bytes,sha256_json,sha256_file
from futures_rebuild.errors import IntegrityError,UnauthorizedOperation,LeaseBusy
from futures_rebuild.causal_1h import build_candidate,certify_candidate,CONTRACT,ENVIRONMENT,implementation_binding
from tests.test_causal_1h import sample,spec,LIMITS,RULEBOOK,BASE,DAY
from tests.test_causal_1s_native import approved,put

pytestmark=pytest.mark.current


def candidate(root,name='one',start=BASE,end=BASE+DAY):
    relative=f'state/unpublished_evidence/hourly/{name}/candidate.json'
    s=spec(start,end)
    days=[sample(d) for d in range(start//DAY*DAY,end,DAY)]
    build_candidate(root/Path(relative).parent,s,lambda:iter(days),RULEBOOK,LIMITS)
    certify_candidate(root/Path(relative).parent,s,lambda:iter(days),RULEBOOK,LIMITS)
    p=root/'data/active/catalog.json'
    p.parent.mkdir(parents=True,exist_ok=True)
    if not p.exists(): p.write_text('{}')
    return relative


def publication_receipt(root,plan):
    return approved(root,pub.OPERATION,{'publication_id':plan['id'],'limits_id':sha256_json(asdict(LIMITS))})


def test_publish_and_resolve_without_payload_copy(tmp_path):
    relative=candidate(tmp_path)
    plan=pub.prepare_publication(tmp_path,candidates=[relative])
    receipt=publication_receipt(tmp_path,plan)
    result=pub.publish(tmp_path,plan,receipt,LIMITS)
    paths=pub.resolve_partitions(tmp_path,result,market='ES',year=2024,timeframe='1h',purpose='SYNTHETIC_TEST')
    assert len(paths)==1 and '/1h/' in paths[0].as_posix()
    assert not list((tmp_path/'data/active').rglob('*.parquet'))
    assert pub.publish(tmp_path,plan,receipt,LIMITS)==result
    with pytest.raises(UnauthorizedOperation):
        pub.resolve_partitions(tmp_path,result,market='ES',year=2024,timeframe='1h',purpose='LINEAGE_AUDIT')
    with pytest.raises(IntegrityError): pub.prepare_publication(tmp_path,candidates=[candidate(tmp_path,'duplicate')])


def test_partial_year_scope_and_preserve_other_month(tmp_path):
    from datetime import datetime,timezone
    start=int(datetime(2023,12,31,tzinfo=timezone.utc).timestamp())*10**9
    relative=candidate(tmp_path,start=start,end=start+2*DAY)
    plan=pub.prepare_publication(tmp_path,candidates=[relative])
    index=pub.publish(tmp_path,plan,publication_receipt(tmp_path,plan),LIMITS)
    assert len(pub.resolve_partitions(tmp_path,index,market='ES',year=2024,timeframe='1h',purpose='SYNTHETIC_TEST'))==1
    january=index['entries'][1]
    assert january['coverage_end_ns']-january['coverage_start_ns']==DAY
    later=candidate(tmp_path,'later',start=BASE,end=BASE+DAY)
    successor=pub.prepare_publication(tmp_path,candidates=[later])
    assert successor['index']['entries'][:2]==index['entries']


@pytest.mark.parametrize('kind',['payload','certificate','coverage','authority'])
def test_invalid_candidate_refused_before_move(tmp_path,kind):
    relative=candidate(tmp_path)
    directory=tmp_path/Path(relative).parent
    if kind=='payload':
        path=next(directory.rglob('*.parquet'))
        path.write_bytes(path.read_bytes()+b'x')
    else:
        path=directory/('candidate.json' if kind=='coverage' else 'certificate.json')
        value=read_record(path)
        if kind=='coverage': value['months']=[]
        elif kind=='authority': value['authority']['research']=True
        else: value['passes'][1]['rows']+=1
        put(tmp_path,path.relative_to(tmp_path).as_posix(),signed({k:v for k,v in value.items() if k!='id'}))
    with pytest.raises((IntegrityError,ValueError)):
        pub.prepare_publication(tmp_path,candidates=[relative])
    assert not (tmp_path/'data/causally_gated_normalized').exists()


def test_interrupted_move_can_recover_and_stale_index_cannot(tmp_path,monkeypatch):
    relative=candidate(tmp_path)
    plan=pub.prepare_publication(tmp_path,candidates=[relative])
    receipt=publication_receipt(tmp_path,plan)
    original=pub.validate_release
    with monkeypatch.context() as patch:
        patch.setattr(pub,'validate_release',lambda *a,**k:(_ for _ in ()).throw(IntegrityError('interrupted after move')))
        with pytest.raises(IntegrityError,match='interrupted'): pub.publish(tmp_path,plan,receipt,LIMITS)
    assert not (tmp_path/pub.INDEX).exists()
    index=pub.publish(tmp_path,plan,receipt,LIMITS)
    assert index['entries'][0]['timeframe']=='1h'


def test_interrupted_final_index_commit_recovers(tmp_path,monkeypatch):
    relative=candidate(tmp_path)
    plan=pub.prepare_publication(tmp_path,candidates=[relative])
    receipt=publication_receipt(tmp_path,plan)
    with monkeypatch.context() as patch:
        patch.setattr(pub.os,'replace',lambda *args:(_ for _ in ()).throw(OSError('index commit interrupted')))
        with pytest.raises(OSError,match='index commit interrupted'):
            pub.publish(tmp_path,plan,receipt,LIMITS)
    assert not (tmp_path/pub.INDEX).exists()
    index=pub.publish(tmp_path,plan,receipt,LIMITS)
    assert index['entries'][0]['timeframe']=='1h'


def test_lease_race_excludes_one_writer(tmp_path):
    barrier=Barrier(2)
    held=Event()
    def attempt():
        barrier.wait(timeout=5)
        try:
            with execution_lease(tmp_path):
                assert held.wait(timeout=5)
                return 'acquired'
        except LeaseBusy:
            held.set()
            return 'excluded'
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:attempt(),range(2)))
    assert sorted(results)==['acquired','excluded']


def test_lease_exclusion_precedes_authorization(tmp_path,monkeypatch):
    called=[]
    monkeypatch.setattr(native,'authorize',lambda *a,**k:called.append(True))
    with execution_lease(tmp_path):
        with pytest.raises(LeaseBusy): native.execute_once(tmp_path,{},None)
    assert called==[]


def test_frozen_historical_provenance_and_consumed_approval(tmp_path,monkeypatch):
    from futures_rebuild import boundary as b
    monkeypatch.setattr(b,'require_current_real_history_operation',lambda *a,**k:None)
    s=replace(spec(),synthetic_only=False,source_manifest_id=sha256_json([]))
    op=signed({'schema':'causal_1h_native_operation/1','operation':native.OPERATION,'repository_root':str(tmp_path),'spec':asdict(s),'spec_id':s.id,
               'entries':[],'contract':CONTRACT,'implementation':implementation_binding(),'environment':ENVIRONMENT,'aggregate_memory_bytes':4*2**30})
    receipt=approved(tmp_path,native.OPERATION,{'operation_id':op['id'],'claim':pub.CLAIM},history=True)
    cert={'id':'b'*64}
    record={'spec':asdict(s),'spec_id':s.id,'contract_id':sha256_json(CONTRACT)}
    proof={'operation':op,'receipt':receipt.as_dict(),'supervision':signed({'operation_id':op['id'],
        'status':'COMPLETE','spec_id':s.id,'certificate_id':cert['id']})}
    aggregate=put(tmp_path,'state/aggregate.json',signed({'operation_id':op['id'],'status':'COMPLETE',
        'maximum_supervisor_worker_commit_bytes':4*2**30,'peak_supervisor_worker_commit_bytes':1000,
        'coordinator_outside_job':True,'coordinator_peak_memory_bytes':1000}))
    proof['aggregate_memory']={'path':'state/aggregate.json','sha256':sha256_file(aggregate)}
    with pytest.raises((IntegrityError,FileNotFoundError)): pub.verify_provenance(tmp_path,record,cert,proof)
    receipt.consume(b.RepoBoundary(tmp_path),operation=native.OPERATION,classification=b.OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,required_scope=dict(receipt.scope))
    from futures_rebuild import causal_1h
    monkeypatch.setattr(causal_1h,'implementation_binding',lambda:{'different':'f'*64})
    pub.verify_provenance(tmp_path,record,cert,proof)
    measured=read_record(aggregate)
    for peak in (0,-1,None,True):
        invalid={k:v for k,v in measured.items() if k!='id'}
        invalid['coordinator_peak_memory_bytes']=peak
        put(tmp_path,'state/aggregate.json',signed(invalid))
        proof['aggregate_memory']['sha256']=sha256_file(aggregate)
        with pytest.raises(IntegrityError,match='coordinator memory'):
            pub.verify_provenance(tmp_path,record,cert,proof)
    put(tmp_path,'state/aggregate.json',measured)
    proof['aggregate_memory']['sha256']=sha256_file(aggregate)
    proof['operation']['implementation']['causal_1h.py']='f'*64
    with pytest.raises(IntegrityError): pub.verify_provenance(tmp_path,record,cert,proof)


def test_v3_dispatch_preserves_minute_view(tmp_path):
    from tests.test_active_data_view import _write_stage,H
    from futures_rebuild import active_data_view as active
    stage,_=_write_stage(tmp_path,plan_id=H['active'],markets=[('ES',2024)])
    destination=tmp_path/'data/active'
    destination.parent.mkdir(parents=True,exist_ok=True)
    stage.rename(destination)
    kwargs=dict(repository_root=tmp_path,market='ES',year=2024,purpose='LINEAGE_AUDIT')
    before=active.resolve(**kwargs)
    relative=candidate(tmp_path)
    plan=pub.prepare_publication(tmp_path,candidates=[relative])
    pub.publish(tmp_path,plan,publication_receipt(tmp_path,plan),LIMITS)
    assert active.resolve(**kwargs)==before
    assert active.resolve_partitions(**kwargs,timeframe='1m')==(before,)
    assert len(active.resolve_partitions(repository_root=tmp_path,market='ES',year=2024,timeframe='1h',purpose='SYNTHETIC_TEST'))==1


def test_legacy_second_index_successor_preserves_manifest_bytes(tmp_path):
    from futures_rebuild import causal_timeframe_view as legacy
    from futures_rebuild.causal_1s import build_candidate as build_s,certify_candidate as certify_s
    from tests.test_causal_1s import sample as sample_s,spec as spec_s
    relative='state/unpublished_evidence/seconds/candidate.json'
    output=tmp_path/Path(relative).parent
    build_s(output,spec_s(),lambda:iter([sample_s()]),RULEBOOK,LIMITS)
    certify_s(output,spec_s(),lambda:iter([sample_s()]),RULEBOOK,LIMITS)
    p=tmp_path/'data/active/catalog.json'
    p.parent.mkdir(parents=True)
    p.write_text('{}')
    proposal=legacy.prepare_publication(tmp_path,candidates=[relative],expected=[['ES','2024-03']])
    receipt=approved(tmp_path,'PUBLISH_CAUSAL_1S_RELEASE_ONCE',{'publication_id':proposal['id'],'limits_id':sha256_json(asdict(LIMITS))})
    old=legacy.publish(tmp_path,proposal,receipt,LIMITS)
    manifest=contained(tmp_path,old['entries'][0]['manifest'])
    before=manifest.read_bytes()
    hour=candidate(tmp_path)
    plan=pub.prepare_publication(tmp_path,candidates=[hour])
    index=pub.publish(tmp_path,plan,publication_receipt(tmp_path,plan),LIMITS)
    assert manifest.read_bytes()==before
    assert {e['timeframe'] for e in index['entries']}=={'1s','1h'}
    assert len(pub.resolve_partitions(tmp_path,index,market='ES',year=2024,timeframe='1s',purpose='SYNTHETIC_TEST'))==1


def test_missing_month_and_wrong_footer_metadata_refused(tmp_path):
    relative=candidate(tmp_path)
    seal_path=tmp_path/Path(relative).parent/'2024-03.seal.json'
    seal=read_record(seal_path)
    seal['table_metadata']['observations']['rows']+=1
    put(tmp_path,seal_path.relative_to(tmp_path).as_posix(),signed({k:v for k,v in seal.items() if k!='id'}))
    with pytest.raises(IntegrityError): pub.prepare_publication(tmp_path,candidates=[relative])
