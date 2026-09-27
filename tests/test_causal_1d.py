"""Generated daily observations only; no historical payload or adopted scope."""
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from futures_rebuild import causal_1d as daily
from futures_rebuild.canonical import sha256_json, sha256_file
from futures_rebuild.causal_timeframe_storage import Limits, read_record, iter_bundle_groups
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation
from tests.test_causal_observation_canary import _bar, _definition, RULEBOOK, H

pytestmark = pytest.mark.current
SECOND, DAY = daily.SECOND, daily.DAY
HOUR = 3600 * SECOND
BASE = int(datetime(2024, 3, 4, tzinfo=timezone.utc).timestamp()) * SECOND
LIMITS = Limits(128*2**20, 4*2**30, 180, minimum_free_bytes=1, maximum_files=2048)


def sample(day=BASE, hours=24, symbol='ESH4'):
    definition = _definition(instrument_id_date_utc=daily.stamp(day).date().isoformat(),
        ts_event_ns=day, ts_recv_ns=day, activation_ns=day-DAY, expiration_ns=day+10*DAY,
        raw_symbol=symbol)
    bar = _bar(0, row_digit='2', event_at_ns=day, volume=240,
        source_file_path='synthetic/ohlcv_1d.dbn.zst')
    refs = tuple(replace(bar, event_at_ns=day+i*HOUR, volume=10,
        source_file_path='synthetic/ohlcv_1h.dbn.zst', row_sha256=sha256_json([day,i]))
        for i in range(hours))
    return daily.DayInput(day, (definition,), (bar,), {'1h':refs})


def empty(day):
    return daily.DayInput(day, (), (), {'1h':()})


def spec(start=BASE, end=BASE+2*DAY):
    return daily.Spec('ES', start, end, H, H, 'b'*64)


def daily_publication_fixture(root):
    """Shared index regression input; creates only generated candidate evidence."""
    from futures_rebuild.causal_timeframe_storage import contained
    relative='state/unpublished_evidence/causal_1d_generated/candidate.json'
    output=contained(root,'state/unpublished_evidence/causal_1d_generated')
    factory=lambda:iter([sample(),empty(BASE+DAY)])
    daily.build_candidate(output,spec(),factory,RULEBOOK,LIMITS)
    daily.certify_candidate(output,spec(),factory,RULEBOOK,LIMITS)
    return relative


def test_full_daily_roundtrip_and_two_replays(tmp_path):
    days = [sample(), empty(BASE+DAY)]
    factory = lambda: iter(days)
    candidate, resources = daily.build_candidate(tmp_path, spec(), factory, RULEBOOK, LIMITS)
    files = {p:sha256_file(p) for p in tmp_path.rglob('*.parquet')}
    certificate, checks = daily.certify_candidate(tmp_path, spec(), factory, RULEBOOK, LIMITS)
    assert len(files)==5 and candidate['observations']==1
    assert certificate['passes'][0]==certificate['passes'][1]
    assert candidate['comparison_counts']==dict(MATCH=1,DISAGREEMENT=0,NOT_COMPARABLE=0,SOURCE_MISSING=0)
    assert candidate['provenance']['contract']['daily_semantics']=='PROVIDER_UTC_DAY_NOT_EXCHANGE_SESSION'
    assert resources['application_written_bytes']>=sum(p.stat().st_size for p in files)
    daily.build_candidate(tmp_path, spec(), factory, RULEBOOK, LIMITS, resume=True)
    assert files=={p:sha256_file(p) for p in files}
    table=next(iter_bundle_groups(tmp_path/'ES/2024/1d/2024-03/candidate'))
    row=table['observations'][0]
    assert row['bar_end_ns']-row['bar_start_ns']==DAY
    assert row['available_at_ns']==BASE+DAY+5*SECOND
    assert row['open_nano']<0 and table['cadence'][0]['source_cadence']=='1d'


@pytest.mark.parametrize('count,change,result', [(24,False,'MATCH'),(23,False,'NOT_COMPARABLE'),
    (0,False,'SOURCE_MISSING'),(24,True,'DISAGREEMENT')])
def test_hourly_comparability_is_reported_not_required(count,change,result):
    d=sample(hours=count)
    if change:
        d=replace(d,bars=(replace(d.bars[0],volume=241),))
    actual,_=daily.produce_day(d,spec(),RULEBOOK,None)
    expected,_=daily.reconstruct(d,spec(),RULEBOOK,None)
    assert actual==expected and actual['cadence'][0]['result']==result
    assert actual['observations'][0]['volume']==(241 if change else 240)


def test_hourly_contract_mismatch_is_not_comparable():
    d=sample()
    d=replace(d,references={'1h':(replace(d.references['1h'][0],instrument_id=3),)+d.references['1h'][1:]})
    assert daily.produce_day(d,spec(),RULEBOOK,None)[0]==daily.reconstruct(d,spec(),RULEBOOK,None)[0]
    assert daily.produce_day(d,spec(),RULEBOOK,None)[0]['cadence'][0]['result']=='NOT_COMPARABLE'


@pytest.mark.parametrize('offset,expected', [(5,0),(6,1)])
def test_strict_end_availability(offset,expected):
    s=spec(end=BASE+DAY+offset*SECOND)
    a,_=daily.produce_day(sample(),s,RULEBOOK,None)
    b,_=daily.reconstruct(sample(),s,RULEBOOK,None)
    assert a==b
    assert (len(a['observations']) if a else 0)==expected


def test_partial_start_and_protected_daily_bar_excluded():
    assert daily.produce_day(sample(),spec(start=BASE+SECOND),RULEBOOK,None)[0] is None
    last=daily.CUTOFF//DAY*DAY
    s=spec(last-DAY,daily.CUTOFF)
    assert daily.produce_day(sample(last),s,RULEBOOK,None)[0] is None
    assert daily.produce_day(sample(last-DAY),s,RULEBOOK,None)[0] is not None


def test_all_declared_standard_markets_and_no_micro_expansion():
    assert set(daily.CONTRACT['markets']) == set(daily.V10_CHECKPOINT_MARKETS)
    assert len(daily.CONTRACT['markets']) == 41
    for market in daily.V10_CHECKPOINT_MARKETS:
        assert replace(spec(),market=market).market == market
    with pytest.raises(UnauthorizedOperation):
        replace(spec(),market='MES')


def test_month_end_storage_boundary_is_not_release_availability(tmp_path):
    start=int(datetime(2024,3,31,tzinfo=timezone.utc).timestamp())*SECOND
    s=replace(spec(start,start+DAY),availability_end_ns=start+DAY+6*SECOND)
    factory=lambda:iter([sample(start)])
    candidate,_=daily.build_candidate(tmp_path,s,factory,RULEBOOK,LIMITS)
    cert,_=daily.certify_candidate(tmp_path,s,factory,RULEBOOK,LIMITS)
    assert candidate['observations']==1 and candidate['months']==['2024-03']
    assert cert['passes'][0]==cert['passes'][1]
    seal=read_record(tmp_path/'2024-03.seal.json')
    assert seal['coverage'][0]['excluded_availability']==0
    assert daily.produce_day(sample(start+DAY),s,RULEBOOK,None)[0] is None
    assert daily.reconstruct(sample(start+DAY),s,RULEBOOK,None)[0] is None


@pytest.mark.parametrize('cutoff',[True,BASE, daily.CUTOFF+SECOND, BASE+2*DAY+1])
def test_invalid_separate_availability_cutoff(cutoff):
    with pytest.raises(ContractError,match='availability cutoff'):
        replace(spec(),availability_end_ns=cutoff)


def test_definition_failure_retains_exact_source_identity(tmp_path):
    d=sample()
    d=replace(d,definitions=(replace(d.definitions[0],security_update_action='DELETE'),))
    with pytest.raises(ContractError,match='definition'):
        daily.build_candidate(tmp_path,spec(),lambda:iter([d,empty(BASE+DAY)]),RULEBOOK,LIMITS)
    failures=list(tmp_path.glob('*.source_failure.json'))
    assert len(failures)==1
    failure=read_record(failures[0])
    assert failure['day_ns']==BASE and failure['source_rows'][0]['row_sha256']==d.bars[0].row_sha256
    assert failure['matching_day_definitions'][0]['security_update_action']=='DELETE'
    assert not (tmp_path/'candidate.json').exists()


@pytest.mark.parametrize('change', ['late','absent','deleted','previous_date','conflict'])
def test_midnight_identity_fails_closed(change):
    d=sample()
    if change=='late':
        d=replace(d,definitions=(replace(d.definitions[0],ts_recv_ns=BASE+1),))
    elif change=='absent':
        d=replace(d,definitions=())
    elif change=='deleted':
        d=replace(d,definitions=(replace(d.definitions[0],security_update_action='DELETE'),))
    elif change=='previous_date':
        d=replace(d,definitions=(replace(d.definitions[0],instrument_id_date_utc='2024-03-03',ts_recv_ns=BASE-DAY),))
    else:
        d=replace(d,definitions=d.definitions+(replace(d.definitions[0],source_file_path='synthetic/conflict.dbn',raw_symbol='ESM4'),))
    for fn in (daily.produce_day,daily.reconstruct):
        with pytest.raises(ContractError): fn(d,spec(),RULEBOOK,None)


@pytest.mark.parametrize('change',['duplicate','alignment','market','release','real_path','reference_duplicate','support'])
def test_source_day_validation(change):
    d=sample()
    if change=='duplicate': d=replace(d,bars=d.bars*2)
    elif change=='reference_duplicate': d=replace(d,references={'1h':d.references['1h']*2})
    elif change=='support': d=replace(d,support=((BASE,'status',H),))
    else:
        k,v={'alignment':('event_at_ns',BASE+SECOND),'market':('market','NQ'),
             'release':('source_release_id','c'*64),'real_path':('source_file_path','data/dbn/ohlcv_1d/ES/raw.dbn.zst')}[change]
        d=replace(d,bars=(replace(d.bars[0],**{k:v}),))
    with pytest.raises((IntegrityError,UnauthorizedOperation)):
        daily.validate_day(d,spec(),LIMITS)


@pytest.mark.parametrize('date', ['2024-02-28','2024-03-09','2024-11-02','2024-12-31'])
def test_calendar_and_storage_boundaries_preserve_roll_state(tmp_path,date):
    start=int(datetime.fromisoformat(date).replace(tzinfo=timezone.utc).timestamp())*SECOND
    days=[sample(start),empty(start+DAY),sample(start+2*DAY,symbol='ESM4'),empty(start+3*DAY)]
    s=spec(start,start+4*DAY)
    candidate,_=daily.build_candidate(tmp_path,s,lambda:iter(days),RULEBOOK,LIMITS)
    daily.certify_candidate(tmp_path,s,lambda:iter(days),RULEBOOK,LIMITS)
    assert candidate['observations']==2
    previous=daily.produce_day(days[0],s,RULEBOOK,None)[1]
    a,_=daily.produce_day(days[2],s,RULEBOOK,previous)
    assert a==daily.reconstruct(days[2],s,RULEBOOK,previous)[0]
    assert a['roll'][0]['roll_flag'] and a['observations'][0]['bar_end_ns']==start+3*DAY


def test_interruption_resume_preserves_evidence(tmp_path):
    start=int(datetime(2024,3,31,tzinfo=timezone.utc).timestamp())*SECOND
    days=[sample(start),sample(start+DAY),empty(start+2*DAY)]
    s=spec(start,start+3*DAY)
    def broken():
        yield from days[:2]
        raise RuntimeError('generated interruption')
    with pytest.raises(RuntimeError): daily.build_candidate(tmp_path/'resume',s,broken,RULEBOOK,LIMITS)
    sealed=read_record(tmp_path/'resume/2024-03.seal.json')
    result,_=daily.build_candidate(tmp_path/'resume',s,lambda:iter(days),RULEBOOK,LIMITS,resume=True)
    fresh,_=daily.build_candidate(tmp_path/'fresh',s,lambda:iter(days),RULEBOOK,LIMITS)
    assert result==fresh and read_record(tmp_path/'resume/2024-03.seal.json')==sealed
    assert list((tmp_path/'resume/interrupted').rglob('*.parquet'))
    daily.certify_candidate(tmp_path/'resume',s,lambda:iter(days),RULEBOOK,LIMITS)


def test_wrong_replay_and_corrupt_payload_rejected(tmp_path,monkeypatch):
    from futures_rebuild import causal_1d_replay as replay
    factory=lambda:iter([sample(),empty(BASE+DAY)])
    daily.build_candidate(tmp_path,spec(),factory,RULEBOOK,LIMITS)
    original=replay.reconstruct_day
    def wrong(**kwargs):
        tables,last=original(**kwargs)
        tables['observations'][0]['available_at_ns']+=1
        return tables,last
    monkeypatch.setattr(replay,'reconstruct_day',wrong)
    with pytest.raises(IntegrityError,match='reconstruction'):
        daily.certify_candidate(tmp_path,spec(),factory,RULEBOOK,LIMITS)
    monkeypatch.setattr(replay,'reconstruct_day',original)
    path=next(tmp_path.rglob('observations.parquet'))
    path.write_bytes(path.read_bytes()+b'x')
    with pytest.raises(IntegrityError,match='changed'):
        daily.certify_candidate(tmp_path,spec(),factory,RULEBOOK,LIMITS)


def test_history_denied_before_creation(tmp_path):
    from futures_rebuild import causal_1d_native as native
    with pytest.raises(UnauthorizedOperation):
        daily.build_candidate(tmp_path/'absent',replace(spec(),synthetic_only=False),lambda:iter([]),RULEBOOK,LIMITS)
    assert not (tmp_path/'absent').exists()
    with pytest.raises(UnauthorizedOperation): native.HistoricalDbnSources(tmp_path,{},None)
    with pytest.raises(UnauthorizedOperation): native.execute_once(tmp_path,{},None)


@pytest.mark.parametrize('kind',['disk','memory','time','writes','rows','group'])
def test_resource_failure_preserves_inactive_state(tmp_path,monkeypatch,kind):
    from futures_rebuild import causal_timeframe_storage as storage
    from types import SimpleNamespace
    limits=LIMITS
    if kind=='disk': monkeypatch.setattr(storage.shutil,'disk_usage',lambda p:SimpleNamespace(free=0))
    elif kind=='memory': monkeypatch.setattr(storage,'memory_peak',lambda:limits.maximum_memory_bytes+1)
    elif kind=='time':
        ticks=iter(range(0,100000,1000))
        monkeypatch.setattr(storage.time,'monotonic',lambda:next(ticks))
    elif kind=='writes': limits=replace(limits,maximum_written_bytes=100)
    elif kind=='rows': limits=replace(limits,maximum_day_source_rows=1)
    else: limits=replace(limits,maximum_group_bytes=1)
    with pytest.raises(IntegrityError):
        daily.build_candidate(tmp_path,spec(),lambda:iter([sample(),empty(BASE+DAY)]),RULEBOOK,limits)
    assert not (tmp_path/'certificate.json').exists()


def generated_dbn(root, compressed=False):
    import json
    import databento_dbn as dbn
    import pyarrow as pa
    from futures_rebuild.causal_timeframe_storage import contained
    entries=[]
    end=BASE+2*DAY
    for family in ('definition','ohlcv_1d','ohlcv_1h'):
        symbols=['ES.FUT'] if family=='definition' else ['ES.v.0']
        stype=dbn.SType.PARENT if family=='definition' else dbn.SType.CONTINUOUS
        metadata=dbn.Metadata(dataset='GLBX.MDP3',start=BASE,end=end,stype_in=stype,
            stype_out=dbn.SType.INSTRUMENT_ID,schema=getattr(dbn.Schema,family.upper()),symbols=symbols,ts_out=False)
        if family=='definition':
            records=[dbn.InstrumentDefMsg(publisher_id=1,instrument_id=2,ts_event=BASE,ts_recv=BASE,
                activation=BASE-DAY,expiration=end,min_price_increment=250_000_000,display_factor=SECOND,
                raw_symbol='ESH4',asset='ES',security_type='FUT',instrument_class=dbn.InstrumentClass.FUTURE,
                security_update_action=dbn.SecurityUpdateAction.ADD,unit_of_measure_qty=50*SECOND,
                currency='USD',group='ES',exchange='XCME',unit_of_measure='USD')]
        else:
            count=1 if family=='ohlcv_1d' else 24
            records=[dbn.OHLCVMsg(rtype=getattr(dbn.RType,family.upper()),publisher_id=1,
                instrument_id=2,ts_event=BASE+i*HOUR,open=-100_000_000,high=-50_000_000,
                low=-150_000_000,close=-75_000_000,volume=240 if count==1 else 10) for i in range(count)]
        relative=f'synthetic/{family}/ES/2024/2024-03-04_2024-03-06.dbn.zst'
        path=contained(root,relative)
        path.parent.mkdir(parents=True,exist_ok=True)
        raw=metadata.encode()+b''.join(bytes(r) for r in records)
        path.write_bytes(pa.compress(raw,codec='zstd').to_pybytes() if compressed else raw)
        payload={'path':relative,'file_sha256':sha256_file(path),'file_size_bytes':path.stat().st_size,
            'schema':family.replace('_','-'),'start':'2024-03-04','end':'2024-03-06',
            'symbols_requested':symbols,'market':'ES','stype_in':str(stype)}
        side=contained(root,relative+'.manifest.json')
        side.write_text(json.dumps(payload))
        for kind,p,rel in [('DBN',path,relative),('SIDECAR',side,relative+'.manifest.json')]:
            entries.append({'kind':kind,'path':rel,'family':family,'market':'ES',
                'sha256':sha256_file(p),'size_bytes':p.stat().st_size,'year':2024})
    return tuple(entries)


@pytest.mark.parametrize('compressed',[False,True])
def test_real_decoder_generated_dbn_and_sidecars(tmp_path,compressed):
    from futures_rebuild.causal_1d_sources import SyntheticDbnSources
    entries=generated_dbn(tmp_path,compressed)
    s=replace(spec(),source_manifest_id=sha256_json(entries))
    one=SyntheticDbnSources(tmp_path,s,entries,LIMITS,batch_rows=1)
    many=SyntheticDbnSources(tmp_path,s,entries,LIMITS,batch_rows=37)
    assert tuple(one())==tuple(many()) and one.decoded_rows==26
    candidate,_=daily.build_candidate(tmp_path/'candidate',s,one,RULEBOOK,LIMITS)
    cert,_=daily.certify_candidate(tmp_path/'candidate',s,many,RULEBOOK,LIMITS)
    assert cert['passes'][0]['rows']==1
    assert candidate['source_inventory']==list(entries)
    seal=read_record(tmp_path/'candidate/2024-03.seal.json')
    assert seal['table_metadata']['observations']['rows']==1


@pytest.mark.parametrize('bad',['missing','hash','schema','protected','wrong_dbn_size','wrong_symbols','unknown_family'])
def test_sidecars_rejected_before_any_decoder_open(tmp_path,monkeypatch,bad):
    import json
    from futures_rebuild import causal_1d_sources as sources
    entries=list(generated_dbn(tmp_path))
    if bad=='missing': entries.pop()
    elif bad=='hash': entries[-1]={**entries[-1],'sha256':'f'*64}
    elif bad=='unknown_family': entries[-1]={**entries[-1],'family':'ohlcv_1s'}
    else:
        side=entries[-1]
        p=tmp_path/side['path']
        value=json.loads(p.read_bytes())
        if bad=='schema': value['schema']='ohlcv-1s'
        elif bad=='protected': value['end']='2026-01-01'
        elif bad=='wrong_dbn_size': value['file_size_bytes']+=1
        else: value['symbols_requested']=['NQ.v.0']
        p.write_text(json.dumps(value))
        entries[-1]={**side,'sha256':sha256_file(p),'size_bytes':p.stat().st_size}
    s=replace(spec(),source_manifest_id=sha256_json(entries))
    def forbidden(*args,**kwargs): pytest.fail('decoder opened before metadata rejection')
    monkeypatch.setattr(sources,'iter_bars',forbidden)
    monkeypatch.setattr(sources,'iter_definitions',forbidden)
    with pytest.raises((IntegrityError,UnauthorizedOperation,ContractError)):
        tuple(sources.SyntheticDbnSources(tmp_path,s,entries,LIMITS)())


@pytest.mark.parametrize('field',['identity','provenance','source_inventory','permitted_uses'])
def test_rebound_bad_candidate_metadata_is_not_certified(tmp_path,field):
    import json
    from futures_rebuild.causal_timeframe_storage import signed
    factory=lambda:iter([sample(),empty(BASE+DAY)])
    daily.build_candidate(tmp_path,spec(),factory,RULEBOOK,LIMITS)
    path=tmp_path/'candidate.json'
    candidate=read_record(path)
    candidate.pop('id')
    candidate[field]=['BROKEN']
    path.write_text(json.dumps(signed(candidate)))
    with pytest.raises(IntegrityError): daily.certify_candidate(tmp_path,spec(),factory,RULEBOOK,LIMITS)


def test_competing_native_launch_excluded_before_authorization(tmp_path,monkeypatch):
    from futures_rebuild import causal_1d_native as native
    from futures_rebuild.causal_timeframe_execution import execution_lease
    from futures_rebuild.errors import LeaseBusy
    def forbidden(*args,**kwargs): pytest.fail('authorization reached while execution lease is busy')
    monkeypatch.setattr(native,'authorize',forbidden)
    with execution_lease(tmp_path):
        with pytest.raises(LeaseBusy): native.execute_once(tmp_path,{},None)
    assert not (tmp_path/'state/authorization_uses').exists()


def test_shared_lease_competing_process(tmp_path):
    import subprocess
    import sys
    from futures_rebuild.causal_timeframe_execution import execution_lease
    code='''from pathlib import Path
import sys
from futures_rebuild.causal_1d_native import execute_once
from futures_rebuild.errors import LeaseBusy
try:
    execute_once(Path(sys.argv[1]), {}, None)
except LeaseBusy:
    print('EXCLUDED_WITHOUT_RECEIPT')
else:
    raise AssertionError('concurrent launch was admitted')
'''
    with execution_lease(tmp_path):
        result=subprocess.run([sys.executable,'-B','-c',code,str(tmp_path)],text=True,capture_output=True,timeout=30)
    assert result.returncode==0, result.stderr
    assert result.stdout.strip()=='EXCLUDED_WITHOUT_RECEIPT'
    assert not (tmp_path/'state/authorization_uses').exists()


def test_generated_history_adapter_enforces_receipt_and_scope(tmp_path,monkeypatch):
    """All 'data/dbn' files here are generated inside this temporary fixture."""
    import json
    import os
    from dataclasses import asdict
    from futures_rebuild import causal_1d_native as native, boundary
    from futures_rebuild.boundary import RepoBoundary,OperationReceipt,OperationClassification
    from futures_rebuild.canonical import canonical_bytes
    from futures_rebuild.causal_timeframe_storage import signed,contained
    entries=[]
    for entry in generated_dbn(tmp_path,True):
        old=contained(tmp_path,entry['path'])
        rel=entry['path'].replace('synthetic/','data/dbn/',1)
        target=contained(tmp_path,rel)
        target.parent.mkdir(parents=True,exist_ok=True)
        os.rename(old,target)
        if entry['kind']=='SIDECAR':
            value=json.loads(target.read_bytes())
            value['path']=value['path'].replace('synthetic/','data/dbn/',1)
            target.write_bytes(canonical_bytes(value)+b'\n')
        entries.append({**entry,'path':rel,'sha256':sha256_file(target),'size_bytes':target.stat().st_size})
    s=replace(spec(),synthetic_only=False,source_manifest_id=sha256_json(entries))
    op=signed({'schema':'causal_1d_native_operation/1','operation':native.OPERATION,
        'repository_root':str(tmp_path),'spec':asdict(s),'spec_id':s.id,'contract':daily.CONTRACT,
        'environment':daily.ENVIRONMENT,'month':'2024-03','through_month':'2024-03','start_date':'2024-03-04',
        'entries':entries,'limits':asdict(LIMITS),'resume':False,'aggregate_memory_bytes':5*2**30,
        'output':native.BASE+'/generated_run','controls':{},'implementation':{}})
    def put(rel,value):
        path=contained(tmp_path,rel)
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(canonical_bytes(value)+b'\n')
    rel=native.BASE+'/operation.json'
    put(rel,op)
    policy=contained(tmp_path,'src/futures_rebuild/research_gateway_policy.py')
    policy.parent.mkdir(parents=True,exist_ok=True)
    policy.write_bytes((Path(native.__file__).parent/'research_gateway_policy.py').read_bytes())
    code,adoption=native.prepare_adoption(tmp_path,op,rel)
    policy.write_bytes(code.encode())
    put(native.ADOPTION,adoption)
    namespace={'__file__':str(policy),'__name__':'futures_rebuild.generated_gateway','__package__':'futures_rebuild'}
    exec(compile(code,str(policy),'exec'),namespace)
    monkeypatch.setattr(boundary,'require_current_real_history_operation',namespace['require_current_real_history_operation'])
    receipt=OperationReceipt.issue_user_approved(RepoBoundary(tmp_path),operation=native.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=native.scope(op),approval_command='GENERATED_FIXTURE_ONLY',approval_plan_id='a'*64,
        approval_plan_sha256='b'*64,approval_line='APPROVE GENERATED_FIXTURE_ONLY PLAN '+'a'*64+' SHA256 '+'b'*64)
    with pytest.raises(UnauthorizedOperation):
        native.HistoricalDbnSources(tmp_path,op,receipt,_worker_token=native._WORKER_TOKEN)
    required=native.authorize(tmp_path,op,receipt,consumed=False)
    receipt.consume(RepoBoundary(tmp_path),operation=native.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,required_scope=required)
    factory=native.HistoricalDbnSources(tmp_path,op,receipt,_worker_token=native._WORKER_TOKEN)
    limits=replace(LIMITS,maximum_written_bytes=LIMITS.maximum_written_bytes-128*1024)
    out=contained(tmp_path,op['output'])
    candidate,_=daily.build_candidate(out,s,factory,RULEBOOK,limits)
    certificate,_=daily.certify_candidate(out,s,factory,RULEBOOK,limits)
    assert candidate['operation']==op and candidate['source_inventory']==entries
    assert candidate['permitted_uses']==['LINEAGE_AUDIT']
    assert certificate['passes'][0]['rows']==1
    with pytest.raises(UnauthorizedOperation):
        receipt.consume(RepoBoundary(tmp_path),operation=native.OPERATION,
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,required_scope=required)
    adoption.pop('id')
    put(native.ADOPTION,signed({**adoption,'state':'REVOKED'}))
    with pytest.raises(UnauthorizedOperation): factory.check()


def test_windows_supervisor_stops_timeout_and_excess_memory(tmp_path):
    import os
    import sys
    from futures_rebuild import causal_1d_native as native
    if os.name!='nt': pytest.skip('Windows job supervision')
    cap=replace(LIMITS,maximum_seconds=1,maximum_memory_bytes=128*2**20)
    with pytest.raises(IntegrityError,match='runtime'):
        native.supervise([sys.executable,'-B','-c','import sys,time;sys.stdin.readline();time.sleep(30)'],tmp_path,{},cap)
    with pytest.raises(IntegrityError,match='failed or exceeded'):
        native.supervise([sys.executable,'-B','-c','import sys;sys.stdin.readline();x=bytearray(256*1024*1024)'],
                         tmp_path,{},replace(cap,maximum_seconds=20))
