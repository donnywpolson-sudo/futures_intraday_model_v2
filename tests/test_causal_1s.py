"""Generated-data checks only; no canonical data or adopted authority."""
from dataclasses import replace
from pathlib import Path
from datetime import datetime, timezone
import json
import pytest

from futures_rebuild.canonical import sha256_json, sha256_file
from futures_rebuild.causal_1s import (Spec,DayInput,SECOND,DAY,CUTOFF,build_candidate,
    certify_candidate,produce_day,reconstruct,stamp,validate_day)
from futures_rebuild.causal_timeframe_storage import Limits, Budget, read_record, signed, contained, iter_bundle_groups
from futures_rebuild.causal_timeframe_view import prepare_reference,prepare_index,resolve_partitions
from futures_rebuild.errors import IntegrityError,UnauthorizedOperation,ContractError
from tests.test_causal_observation_canary import _bar,_definition,RULEBOOK,H,START_NS

pytestmark=pytest.mark.current
LIMITS=Limits(256*2**20,4*2**30,300,minimum_free_bytes=1)
BASE=START_NS//DAY*DAY


def sample(day=BASE,n=60,offset=60):
    defs=(_definition(instrument_id_date_utc=stamp(day).date().isoformat(),
        ts_event_ns=day+1,ts_recv_ns=day+2,activation_ns=day-SECOND,expiration_ns=day+2*DAY),)
    bars=tuple(_bar(0,row_digit='2',event_at_ns=day+(offset+i)*SECOND,
                source_file_path='synthetic/ohlcv_1s.dbn.zst',row_sha256=sha256_json([day,i])) for i in range(n))
    ref=replace(bars[0],event_at_ns=day+offset*SECOND,volume=10*n,source_file_path='synthetic/ohlcv_1m.dbn.zst')
    return DayInput(day,defs,bars,{'1m':(ref,), '1h':(), '1d':()})


def spec(start=BASE,end=BASE+DAY):
    return Spec('ES',start,end,H,H,'a'*64)


def test_streamed_roundtrip_independent_certification_and_resume(tmp_path):
    d=sample()
    s=spec()
    candidate,resources=build_candidate(tmp_path,s,lambda:iter([d]),RULEBOOK,LIMITS)
    before={p:sha256_file(p) for p in tmp_path.rglob('*.parquet')}
    certificate,checks=certify_candidate(tmp_path,s,lambda:iter([d]),RULEBOOK,LIMITS)
    assert candidate['observations']==60
    assert certificate['passes'][0]==certificate['passes'][1]
    assert len(before)==5
    assert resources['application_written_bytes']>=sum(p.stat().st_size for p in before)
    assert checks['application_written_bytes']<10_000
    _,resume=build_candidate(tmp_path,s,lambda:iter([d]),RULEBOOK,LIMITS,resume=True)
    assert resume['application_written_bytes']<10_000  # run accounting only; payload unchanged
    assert {p:sha256_file(p) for p in before}==before
    tables=next(iter_bundle_groups(tmp_path/'ES'/str(stamp(BASE).year)/'1s'/stamp(BASE).strftime('%Y-%m')/'candidate'))
    assert tables['observations'][0]['bar_end_ns']-tables['observations'][0]['bar_start_ns']==SECOND
    assert tables['cadence'][0]['result']=='MATCH'
    assert tables['cadence'][0]['source_cadence']=='1s'


def test_exact_cutoff_and_missing_edges():
    assert datetime.fromtimestamp(CUTOFF//SECOND,timezone.utc).isoformat()=='2025-07-13T22:00:00+00:00'
    d=sample(n=2)
    s=spec(end=d.bars[1].event_at_ns+6*SECOND)
    tables,_=produce_day(d,s,RULEBOOK,None)
    assert len(tables['observations'])==1
    assert tables==reconstruct(d,s,RULEBOOK,None)[0]
    assert len(tables['missingness'])==3


@pytest.mark.parametrize('change', ['duplicate','order','wrong_market','wrong_release','real_path','alignment','reference_duplicate'])
def test_source_day_rejects_ambiguity(change):
    d=sample(n=2)
    if change=='duplicate': d=replace(d,bars=(d.bars[0],d.bars[0]))
    elif change=='order': d=replace(d,bars=d.bars[::-1])
    elif change=='reference_duplicate': d=replace(d,references={**d.references,'1m':d.references['1m']*2})
    else:
        field,value={'wrong_market':('market','NQ'),'wrong_release':('source_release_id','f'*64),
            'real_path':('source_file_path','data/dbn/ohlcv_1s/ES/raw.dbn.zst'),
            'alignment':('event_at_ns',d.bars[0].event_at_ns+1)}[change]
        d=replace(d,bars=(replace(d.bars[0],**{field:value}),d.bars[1]))
    with pytest.raises((IntegrityError,UnauthorizedOperation)):
        validate_day(d,spec(),LIMITS)


def test_sparse_roll_and_negative_prices():
    d=sample(n=3)
    extra=replace(d.definitions[0],instrument_id=3,raw_symbol='ESM4',row_sha256='3'*64,row_ordinal=1)
    d=replace(d,definitions=d.definitions+(extra,),bars=(d.bars[0],replace(d.bars[2],instrument_id=3)))
    a,last=produce_day(d,spec(),RULEBOOK,None)
    b,_=reconstruct(d,spec(),RULEBOOK,None)
    assert a==b
    assert a['roll'][-1]['roll_flag']
    assert a['cadence'][0]['result']=='NOT_COMPARABLE'
    assert a['observations'][0]['open_nano']<0
    assert any(x['observation_row_id'] is None for x in a['missingness'])


def test_late_definition_rejected():
    d=sample()
    d=replace(d,definitions=(replace(d.definitions[0],ts_recv_ns=d.bars[0].event_at_ns+SECOND),))
    with pytest.raises(ContractError): produce_day(d,spec(),RULEBOOK,None)
    with pytest.raises(ContractError): reconstruct(d,spec(),RULEBOOK,None)


def test_multiday_empty_day_and_month_boundary(tmp_path):
    start=int(datetime(2024,3,31,tzinfo=timezone.utc).timestamp())*SECOND
    one=sample(start)
    empty=replace(sample(start+DAY),bars=(),references={'1m':(),'1h':(),'1d':()})
    three=sample(start+2*DAY)
    factory=lambda:iter([one,empty,three])
    s=spec(start,start+3*DAY)
    result,_=build_candidate(tmp_path,s,factory,RULEBOOK,LIMITS)
    cert,_=certify_candidate(tmp_path,s,factory,RULEBOOK,LIMITS)
    assert result['months']==['2024-03','2024-04']
    assert len(list(tmp_path.rglob('*.parquet')))==10
    seal=read_record(tmp_path/'2024-04.seal.json')
    assert seal['coverage'][0]['state']=='SOURCE_UNAVAILABLE'
    assert cert['passes'][0]['rows']==120


def test_missing_day_and_changed_resume_rejected(tmp_path):
    s=spec(end=BASE+2*DAY)
    with pytest.raises(IntegrityError,match='ended'):
        build_candidate(tmp_path,s,lambda:iter([sample()]),RULEBOOK,LIMITS)
    assert not (tmp_path/'candidate.json').exists()
    assert list(tmp_path.rglob('*.parquet'))  # interrupted bytes preserved


def test_resume_preserves_partial_month_and_matches_fresh(tmp_path):
    start=int(datetime(2024,3,31,tzinfo=timezone.utc).timestamp())*SECOND
    s=spec(start,start+3*DAY)
    days=[sample(start+i*DAY) for i in range(3)]
    def interrupted():
        yield from days[:2]
        raise RuntimeError('simulated interruption')
    interrupted_root=tmp_path/'resume'
    with pytest.raises(RuntimeError):
        build_candidate(interrupted_root,s,interrupted,RULEBOOK,LIMITS)
    march=read_record(interrupted_root/'2024-03.seal.json')
    result,resources=build_candidate(interrupted_root,s,lambda:iter(days),RULEBOOK,LIMITS,resume=True)
    assert read_record(interrupted_root/'2024-03.seal.json')==march
    assert list((interrupted_root/'interrupted').rglob('*.parquet'))
    fresh,_=build_candidate(tmp_path/'fresh',s,lambda:iter(days),RULEBOOK,LIMITS)
    assert result==fresh
    certify_candidate(interrupted_root,s,lambda:iter(days),RULEBOOK,LIMITS)
    assert resources['cumulative_application_written_bytes']>resources['application_written_bytes']


def test_corruption_and_independent_transform_mutation(tmp_path,monkeypatch):
    s=spec()
    build_candidate(tmp_path,s,lambda:iter([sample()]),RULEBOOK,LIMITS)
    from futures_rebuild import causal_1s_replay as replay
    original=replay.reconstruct_day
    def wrong(**kwargs):
        tables,last=original(**kwargs)
        tables['observations'][0]['available_at_ns']+=1
        return tables,last
    monkeypatch.setattr(replay,'reconstruct_day',wrong)
    with pytest.raises(IntegrityError,match='reconstruction'):
        certify_candidate(tmp_path,s,lambda:iter([sample()]),RULEBOOK,LIMITS)
    assert not (tmp_path/'certificate.json').exists()


def test_budget_refuses_write_and_does_not_delete(tmp_path):
    budget=Budget(tmp_path,replace(LIMITS,maximum_written_bytes=3))
    with budget.open(tmp_path/'small') as f:
        f.write(b'abc')
        with pytest.raises(IntegrityError): f.write(b'd')
    assert (tmp_path/'small').read_bytes()==b'abc'
    assert budget.written==3


def test_reference_view_uses_original_files_and_rejects_research(tmp_path):
    root=tmp_path
    relative='data/causally_gated_normalized/synthetic-candidate/candidate.json'
    destination=root/Path(relative).parent
    s=spec()
    build_candidate(destination,s,lambda:iter([sample()]),RULEBOOK,LIMITS)
    certify_candidate(destination,s,lambda:iter([sample()]),RULEBOOK,LIMITS)
    ref=prepare_reference(root,relative)
    rp=f'data/active/causally_gated_normalized/ES/2024/1s/manifest.json'
    p=contained(root,rp)
    p.parent.mkdir(parents=True)
    budget=Budget(root,LIMITS)
    budget.save(p,ref)
    legacy=root/'data/active/catalog.json'
    legacy.write_text('{}')
    index=prepare_index(root,[rp])
    budget.save(root/'data/active/timeframes.json',index)
    paths=resolve_partitions(repository_root=root,market='ES',year=2024,timeframe='1s',purpose='SYNTHETIC_TEST')
    assert len(paths)==1 and 'causally_gated_normalized' in str(paths[0])
    assert not list((root/'data/active').rglob('*.parquet'))
    for purpose in ('LINEAGE_AUDIT','SELECTION','FEATURE_GENERATION'):
        with pytest.raises(UnauthorizedOperation):
            resolve_partitions(repository_root=root,market='ES',year=2024,timeframe='1s',purpose=purpose)
    legacy.write_text('{"changed":true}')
    with pytest.raises(IntegrityError,match='binding differs|drifted'):
        resolve_partitions(repository_root=root,market='ES',year=2024,timeframe='1s',purpose='SYNTHETIC_TEST')


@pytest.mark.parametrize('value',['../escape','x/../../escape','C:/outside','/absolute','x\\evil'])
def test_containment(tmp_path,value):
    with pytest.raises(IntegrityError): contained(tmp_path,value)


def test_legacy_and_explicit_timeframes_coexist(tmp_path,monkeypatch):
    from tests.test_active_data_view import _write_stage, H as LEGACY_H
    from futures_rebuild import active_data_view as active
    import futures_rebuild.causal_timeframe_view as view
    stage,_=_write_stage(tmp_path,plan_id=LEGACY_H['active'],markets=[('ES',2024)])
    destination=tmp_path/'data/active'
    destination.parent.mkdir(parents=True,exist_ok=True)
    stage.rename(destination)
    kwargs=dict(repository_root=tmp_path,market='ES',year=2024,purpose='LINEAGE_AUDIT')
    before=active.resolve(**kwargs)
    assert active.resolve_partitions(**kwargs,timeframe='1m')==(before,)
    relative='data/causally_gated_normalized/synthetic-candidate/candidate.json'
    output=tmp_path/Path(relative).parent
    build_candidate(output,spec(),lambda:iter([sample()]),RULEBOOK,LIMITS)
    certify_candidate(output,spec(),lambda:iter([sample()]),RULEBOOK,LIMITS)
    rp='data/active/causally_gated_normalized/ES/2024/1s/manifest.json'
    path=contained(tmp_path,rp)
    path.parent.mkdir(parents=True)
    budget=Budget(tmp_path,LIMITS)
    budget.save(path,prepare_reference(tmp_path,relative))
    budget.save(destination/'timeframes.json',prepare_index(tmp_path,[rp]))
    # 1m resolution must not touch the 1s payload or transform.
    monkeypatch.setattr(view,'prepare_reference',lambda *a,**k:pytest.fail('1s payload read during 1m resolution'))
    assert active.resolve(**kwargs)==before
    assert active.resolve_partitions(**kwargs,timeframe='1m')==(before,)
    (path.parent/'unexpected.json').write_text('{}')
    with pytest.raises(IntegrityError,match='noncanonical'):
        active.resolve(**kwargs)


@pytest.mark.parametrize('kind',['disk','memory','time'])
def test_resource_limit_stops_before_publication(tmp_path,monkeypatch,kind):
    from futures_rebuild import causal_timeframe_storage as storage
    if kind=='disk':
        from types import SimpleNamespace
        monkeypatch.setattr(storage.shutil,'disk_usage',lambda p:SimpleNamespace(free=0))
    elif kind=='memory':
        monkeypatch.setattr(storage,'memory_peak',lambda:LIMITS.maximum_memory_bytes+1)
    else:
        ticks=iter([0, LIMITS.maximum_seconds+1])
        monkeypatch.setattr(storage.time,'monotonic',lambda:next(ticks))
    with pytest.raises(IntegrityError,match='resource limit'):
        build_candidate(tmp_path,spec(),lambda:iter([sample()]),RULEBOOK,LIMITS)
    assert not list(tmp_path.rglob('*.parquet'))
    assert not (tmp_path/'certificate.json').exists()


def test_changed_sealed_input_cannot_resume(tmp_path):
    build_candidate(tmp_path,spec(),lambda:iter([sample()]),RULEBOOK,LIMITS)
    d=sample()
    d=replace(d,bars=(replace(d.bars[0],volume=11),)+d.bars[1:])
    with pytest.raises(IntegrityError,match='input/state differs'):
        build_candidate(tmp_path,spec(),lambda:iter([d]),RULEBOOK,LIMITS,resume=True)


def test_future_definition_does_not_change_past_observations():
    d=sample(n=2)
    future=replace(d.definitions[0],ts_event_ns=d.bars[-1].event_at_ns+SECOND,
                   ts_recv_ns=d.bars[-1].event_at_ns+2*SECOND,row_sha256='f'*64,row_ordinal=2)
    later=replace(d,definitions=d.definitions+(future,))
    baseline=produce_day(d,spec(),RULEBOOK,None)[0]
    assert produce_day(later,spec(),RULEBOOK,None)[0]==baseline
    assert reconstruct(later,spec(),RULEBOOK,None)[0]==baseline


def test_economics_substitution_and_unregistered_payload_rejected(tmp_path):
    with pytest.raises(IntegrityError,match='economics'):
        build_candidate(tmp_path/'wrong',spec(),lambda:iter([sample()]),
                        replace(RULEBOOK,rulebook_hash='f'*64),LIMITS)
    root=tmp_path/'candidate'
    build_candidate(root,spec(),lambda:iter([sample()]),RULEBOOK,LIMITS)
    payload=next(root.rglob('*.parquet'))
    (payload.parent/'unregistered.bin').write_bytes(b'not a monthly table')
    with pytest.raises(IntegrityError,match='unregistered'):
        certify_candidate(root,spec(),lambda:iter([sample()]),RULEBOOK,LIMITS)
    assert not (root/'certificate.json').exists()


def test_year_specific_reference_and_failed_finish_rejected(tmp_path):
    start=int(datetime(2023,12,31,tzinfo=timezone.utc).timestamp())*SECOND
    s=spec(start,start+2*DAY)
    days=[sample(start),sample(start+DAY)]
    relative='data/causally_gated_normalized/synthetic-candidate/candidate.json'
    root=tmp_path/Path(relative).parent
    build_candidate(root,s,lambda:iter(days),RULEBOOK,LIMITS)
    certify_candidate(root,s,lambda:iter(days),RULEBOOK,LIMITS)
    one=prepare_reference(tmp_path,relative,year=2023)
    two=prepare_reference(tmp_path,relative,year=2024)
    assert [f['month'] for f in one['files']]==['2023-12']
    assert [f['month'] for f in two['files']]==['2024-01']
    finish=root/'runs/000002.finish.json'
    payload=read_record(finish)
    payload.pop('id')
    payload['status']='FAILED'
    finish.write_text(json.dumps(signed(payload)))
    with pytest.raises(IntegrityError,match='resource-accounted'):
        prepare_reference(tmp_path,relative,year=2024)


def generated_dbn(root,*,compressed=False,window=None):
    import databento_dbn as dbn
    from futures_rebuild.canonical import io_path
    entries=[]
    window_start,window_end=(BASE,BASE+DAY) if window is None else window
    start,end=stamp(window_start).date().isoformat(),stamp(window_end).date().isoformat()
    for family in ('definition','ohlcv_1s','ohlcv_1m'):
        schema=family.replace('_','-')
        symbols=['ES.FUT'] if family=='definition' else ['ES.v.0']
        stype=dbn.SType.PARENT if family=='definition' else dbn.SType.CONTINUOUS
        metadata=dbn.Metadata(dataset='GLBX.MDP3',start=window_start,end=window_end,stype_in=stype,
            stype_out=dbn.SType.INSTRUMENT_ID,schema=getattr(dbn.Schema,family.upper()),symbols=symbols,ts_out=False)
        if family=='definition':
            records=[dbn.InstrumentDefMsg(publisher_id=1,instrument_id=2,ts_event=BASE+1,ts_recv=BASE+2,
                activation=BASE-SECOND,expiration=BASE+2*DAY,min_price_increment=250_000_000,
                display_factor=SECOND,raw_symbol='ESH4',asset='ES',security_type='FUT',
                instrument_class=dbn.InstrumentClass.FUTURE,security_update_action=dbn.SecurityUpdateAction.ADD,
                unit_of_measure_qty=50*SECOND,currency='USD',group='ES',exchange='XCME',unit_of_measure='USD')]
        else:
            count=60 if family=='ohlcv_1s' else 1
            records=[dbn.OHLCVMsg(rtype=dbn.RType.OHLCV_1S if count==60 else dbn.RType.OHLCV_1M,
                publisher_id=1,instrument_id=2,ts_event=BASE+(60+i)*SECOND,open=-100_000_000,
                high=-50_000_000,low=-150_000_000,close=-75_000_000,volume=10 if count==60 else 600) for i in range(count)]
        relative=f'synthetic/{family}/ES/2024/{start}_{end}.dbn.zst'
        path=contained(root,relative)
        path.parent.mkdir(parents=True,exist_ok=True)
        raw=metadata.encode()+b''.join(bytes(r) for r in records)
        if compressed:
            import pyarrow as pa
            raw=pa.compress(raw,codec='zstd').to_pybytes()
        path.write_bytes(raw)
        digest=sha256_file(path)
        payload={'path':relative,'file_sha256':digest,'file_size_bytes':path.stat().st_size,
            'schema':schema,'start':start,'end':end,'symbols_requested':symbols,
            'market':'ES','stype_in':str(stype)}
        side=contained(root,relative+'.manifest.json')
        side.write_text(json.dumps(payload))
        for kind,p,rel in (('DBN',path,relative),('SIDECAR',side,relative+'.manifest.json')):
            entries.append({'kind':kind,'path':rel,'family':family,'market':'ES',
                'sha256':sha256_file(p),'size_bytes':p.stat().st_size,'year':2024})
    return tuple(entries)


@pytest.mark.parametrize('compressed',[False,True])
def test_native_dbn_sidecars_and_chunk_invariance(tmp_path,compressed):
    from futures_rebuild.causal_1s_sources import SyntheticDbnSources
    entries=generated_dbn(tmp_path,compressed=compressed)
    s=replace(spec(),source_manifest_id=sha256_json(entries))
    one=SyntheticDbnSources(tmp_path,s,entries,LIMITS,batch_rows=1)
    many=SyntheticDbnSources(tmp_path,s,entries,LIMITS,batch_rows=37)
    assert tuple(one())==tuple(many())
    assert one.decoded_rows==62
    build_candidate(tmp_path/'candidate',s,one,RULEBOOK,LIMITS)
    cert,_=certify_candidate(tmp_path/'candidate',s,many,RULEBOOK,LIMITS)
    assert cert['passes'][0]['rows']==60


@pytest.mark.parametrize('bad',['missing_sidecar','wrong_sidecar','wrong_schema','protected'])
def test_dbn_rejected_before_open(tmp_path,monkeypatch,bad):
    from futures_rebuild import causal_1s_sources as sources
    entries=list(generated_dbn(tmp_path))
    if bad=='missing_sidecar': entries.pop()
    elif bad=='wrong_sidecar': entries[-1]={**entries[-1],'sha256':'f'*64}
    else:
        side=entries[-1]
        p=contained(tmp_path,side['path'])
        value=json.loads(p.read_bytes())
        if bad=='wrong_schema': value['schema']='ohlcv-1h'
        else: value['end']='2026-01-01'
        p.write_text(json.dumps(value))
        entries[-1]={**side,'sha256':sha256_file(p),'size_bytes':p.stat().st_size}
    s=replace(spec(),source_manifest_id=sha256_json(tuple(entries)))
    monkeypatch.setattr(sources,'iter_bars',lambda *a,**k:pytest.fail('DBN opened before all sidecars validated'))
    monkeypatch.setattr(sources,'iter_definitions',lambda *a,**k:pytest.fail('DBN opened before all sidecars validated'))
    with pytest.raises((IntegrityError,UnauthorizedOperation)):
        tuple(sources.SyntheticDbnSources(tmp_path,s,entries,LIMITS)())
