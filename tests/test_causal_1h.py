"""Generated hourly data only; native decoder and persisted replay contracts."""
from dataclasses import replace
from datetime import datetime, timezone
import json
import pytest
from futures_rebuild.canonical import sha256_json, sha256_file, canonical_bytes
from futures_rebuild.causal_1h import (Spec,DayInput,SECOND,HOUR,DAY,CUTOFF,produce_day,
    reconstruct,validate_day,build_candidate,certify_candidate,stamp)
from futures_rebuild.causal_timeframe_storage import Limits,read_record,contained,signed
from futures_rebuild.errors import IntegrityError,UnauthorizedOperation,ContractError
from tests.test_causal_observation_canary import _bar,_definition,RULEBOOK,H,START_NS

pytestmark=pytest.mark.current
BASE=START_NS//DAY*DAY
LIMITS=Limits(64*2**20,3*2**30,120,minimum_free_bytes=1)


def sample(day=BASE,n=2):
    definition=_definition(instrument_id_date_utc=stamp(day).date().isoformat(),
        ts_event_ns=day+1,ts_recv_ns=day+2,activation_ns=day-SECOND,expiration_ns=day+2*DAY)
    bars=tuple(_bar(0,row_digit='2',event_at_ns=day+(i+1)*HOUR,volume=600,
        source_file_path='synthetic/ohlcv_1h.dbn.zst',row_sha256=sha256_json([day,'h',i])) for i in range(n))
    minutes=tuple(replace(bar,event_at_ns=bar.event_at_ns+i*60*SECOND,volume=10,
        source_file_path='synthetic/ohlcv_1m.dbn.zst',row_sha256=sha256_json([day,'m',j,i]))
        for j,bar in enumerate(bars) for i in range(60))
    return DayInput(day,(definition,),bars,{'1m':minutes})


def spec(start=BASE,end=BASE+DAY): return Spec('ES',start,end,H,H,'a'*64)


def test_hourly_roundtrip(tmp_path):
    d,s=sample(),spec()
    result,resources=build_candidate(tmp_path,s,lambda:iter([d]),RULEBOOK,LIMITS)
    before={p:sha256_file(p) for p in tmp_path.rglob('*.parquet')}
    cert,_=certify_candidate(tmp_path,s,lambda:iter([d]),RULEBOOK,LIMITS)
    assert result['observations']==2 and len(before)==5
    assert cert['passes'][0]==cert['passes'][1]
    assert result['identity']==s.identity
    assert before=={p:sha256_file(p) for p in before}
    tables,_=produce_day(d,s,RULEBOOK,None)
    assert tables==reconstruct(d,s,RULEBOOK,None)[0]
    assert tables['observations'][0]['bar_end_ns']==d.bars[0].event_at_ns+HOUR
    assert tables['observations'][0]['available_at_ns']==d.bars[0].event_at_ns+HOUR+5*SECOND
    assert [r['result'] for r in tables['cadence']]==['MATCH','MATCH']
    assert all(r['source_cadence']=='1h' for r in tables['observations'])


@pytest.mark.parametrize('kind',['gap','missing','mismatch','contract'])
def test_reference_dispositions(kind):
    d=sample(n=1)
    refs=d.references['1m']
    if kind=='gap': refs=refs[:-1]
    elif kind=='missing': refs=()
    elif kind=='mismatch': refs=(replace(refs[0],volume=99),)+refs[1:]
    else: refs=(replace(refs[0],instrument_id=3),)+refs[1:]
    d=replace(d,references={'1m':refs})
    a,_=produce_day(d,spec(),RULEBOOK,None)
    assert a==reconstruct(d,spec(),RULEBOOK,None)[0]
    assert a['cadence'][0]['result']=={'gap':'NOT_COMPARABLE','missing':'SOURCE_MISSING','mismatch':'DISAGREEMENT','contract':'NOT_COMPARABLE'}[kind]
    assert a['observations'][0]['volume']==600


@pytest.mark.parametrize('change',['duplicate','alignment','late_definition','date','wrong_source'])
def test_fail_closed(change):
    d=sample()
    if change=='duplicate': d=replace(d,bars=d.bars+d.bars[-1:])
    elif change=='alignment': d=replace(d,bars=(replace(d.bars[0],event_at_ns=d.bars[0].event_at_ns+SECOND),))
    elif change=='late_definition': d=replace(d,definitions=(replace(d.definitions[0],ts_recv_ns=d.bars[0].event_at_ns+1),))
    elif change=='date': d=replace(d,definitions=(sample(BASE+DAY).definitions[0],))
    else: d=replace(d,bars=(replace(d.bars[0],source_release_id='f'*64),))
    with pytest.raises((IntegrityError,UnauthorizedOperation,ContractError)):
        validate_day(d,spec(),LIMITS)
        produce_day(d,spec(),RULEBOOK,None)


def test_cutoff_and_support_row_limit():
    day=CUTOFF//DAY*DAY
    d=sample(day,n=1)
    d=replace(d,bars=(replace(d.bars[0],event_at_ns=CUTOFF-HOUR),),references={'1m':()})
    assert produce_day(d,spec(day,CUTOFF),RULEBOOK,None)==(None,None)
    assert reconstruct(d,spec(day,CUTOFF),RULEBOOK,None)==(None,None)
    d=sample()
    count=len(d.definitions)+len(d.bars)+len(d.references['1m'])
    validate_day(d,spec(),replace(LIMITS,maximum_day_source_rows=count))
    with pytest.raises(IntegrityError,match='source-state'):
        validate_day(d,spec(),replace(LIMITS,maximum_day_source_rows=count-1))


def test_partial_year_empty_month_roll_and_resume(tmp_path):
    start=int(datetime(2023,12,31,tzinfo=timezone.utc).timestamp())*SECOND
    one=sample(start,n=1)
    empty=replace(sample(start+DAY,n=1),bars=(),references={'1m':()})
    three=sample(start+2*DAY,n=1)
    three=replace(three,definitions=(replace(three.definitions[0],raw_symbol='ESM4'),))
    s=spec(start,start+3*DAY)
    days=[one,empty,three]
    result,_=build_candidate(tmp_path,s,lambda:iter(days),RULEBOOK,LIMITS)
    certify_candidate(tmp_path,s,lambda:iter(days),RULEBOOK,LIMITS)
    assert result['months']==['2023-12','2024-01']
    first=produce_day(one,s,RULEBOOK,None)[1]
    assert produce_day(three,s,RULEBOOK,first)[0]['roll'][0]['roll_flag']
    hashes={p:sha256_file(p) for p in tmp_path.rglob('*.parquet')}
    build_candidate(tmp_path,s,lambda:iter(days),RULEBOOK,LIMITS,resume=True)
    assert {p:sha256_file(p) for p in hashes}==hashes


def test_replay_failure_and_missing_day(tmp_path,monkeypatch):
    from futures_rebuild import causal_1h_replay as replay
    build_candidate(tmp_path,spec(),lambda:iter([sample()]),RULEBOOK,LIMITS)
    original=replay.reconstruct_day
    def wrong(**kwargs):
        tables,prior=original(**kwargs)
        tables['observations'][0]['volume']+=1
        return tables,prior
    monkeypatch.setattr(replay,'reconstruct_day',wrong)
    with pytest.raises(IntegrityError,match='reconstruction'):
        certify_candidate(tmp_path,spec(),lambda:iter([sample()]),RULEBOOK,LIMITS)
    assert not (tmp_path/'certificate.json').exists()


def generated_dbn(root):
    """Same native DBN metadata/sidecars as the existing 1s generated fixture."""
    from tests.test_causal_1s import generated_dbn as base_fixture
    import databento_dbn as dbn
    entries=list(base_fixture(root))
    # Replace generated 1s fixture with hourly metadata; only selected entries reach the adapter.
    entries=[e for e in entries if e['family']!='ohlcv_1s']
    minute=[e for e in entries if e['family']=='ohlcv_1m' and e['kind']=='DBN'][0]
    for family,count,step in [('ohlcv_1h',1,HOUR),('ohlcv_1m',60,60*SECOND)]:
        relative=f'synthetic/{family}/ES/2024/{stamp(BASE).date()}_{stamp(BASE+DAY).date()}.dbn.zst'
        metadata=dbn.Metadata(dataset='GLBX.MDP3',start=BASE,end=BASE+DAY,stype_in=dbn.SType.CONTINUOUS,
            stype_out=dbn.SType.INSTRUMENT_ID,schema=getattr(dbn.Schema,family.upper()),symbols=['ES.v.0'],ts_out=False)
        records=[dbn.OHLCVMsg(rtype=getattr(dbn.RType,family.upper()),publisher_id=1,instrument_id=2,
            ts_event=BASE+HOUR+i*step,open=-100000000,high=-50000000,low=-150000000,close=-75000000,
            volume=600 if count==1 else 10) for i in range(count)]
        path=contained(root,relative)
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(metadata.encode()+b''.join(bytes(r) for r in records))
        side=contained(root,relative+'.manifest.json')
        side.write_text(json.dumps({'path':relative,'file_sha256':sha256_file(path),'file_size_bytes':path.stat().st_size,
            'schema':family.replace('_','-'),'start':str(stamp(BASE).date()),'end':str(stamp(BASE+DAY).date()),
            'symbols_requested':['ES.v.0'],'market':'ES','stype_in':'continuous'}))
        entries=[e for e in entries if e['family']!=family]
        for kind,p in [('DBN',path),('SIDECAR',side)]:
            entries.append({'kind':kind,'path':relative+('.manifest.json' if kind=='SIDECAR' else ''),'family':family,'market':'ES',
                            'sha256':sha256_file(p),'size_bytes':p.stat().st_size,'year':2024})
    return tuple(entries)


def test_native_decode_and_sidecar_admission(tmp_path):
    from futures_rebuild.causal_1h_sources import SyntheticDbnSources
    entries=generated_dbn(tmp_path)
    s=replace(spec(),source_manifest_id=sha256_json(entries))
    factory=SyntheticDbnSources(tmp_path,s,entries,LIMITS)
    candidate,_=build_candidate(tmp_path/'output',s,factory,RULEBOOK,LIMITS)
    cert,_=certify_candidate(tmp_path/'output',s,factory,RULEBOOK,LIMITS)
    assert candidate['observations']==1 and cert['passes'][0]['rows']==1
    selected=next(e for e in entries if e['family']=='ohlcv_1h' and e['kind']=='SIDECAR')
    contained(tmp_path,selected['path']).write_text('{}')
    with pytest.raises(IntegrityError,match='sidecar'): list(factory())


@pytest.mark.parametrize('change',['missing','protected','schema'])
def test_metadata_rejected_before_decoder_open(tmp_path,monkeypatch,change):
    from futures_rebuild import causal_1h_sources as sources
    entries=list(generated_dbn(tmp_path))
    position=next(i for i,e in enumerate(entries) if e['family']=='ohlcv_1h' and e['kind']=='SIDECAR')
    if change=='missing': entries.pop(position)
    else:
        entry=entries[position]
        path=contained(tmp_path,entry['path'])
        value=json.loads(path.read_bytes())
        if change=='protected': value['end']='2026-01-01'
        else: value['schema']='ohlcv-1d'
        path.write_bytes(canonical_bytes(value)+b'\n')
        entries[position]={**entry,'sha256':sha256_file(path),'size_bytes':path.stat().st_size}
    s=replace(spec(),source_manifest_id=sha256_json(entries))
    monkeypatch.setattr(sources,'iter_bars',lambda *a,**k:pytest.fail('payload opened before metadata rejection'))
    with pytest.raises((IntegrityError,UnauthorizedOperation,ContractError)):
        list(sources.SyntheticDbnSources(tmp_path,s,entries,LIMITS)())
