"""Generated complete reader stacks; never historical payloads or live controls."""
from contextlib import contextmanager
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import shutil
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json, io_path
from futures_rebuild.causal_timeframe_storage import signed, read_record
from futures_rebuild.causal_observation_parquet import FILENAMES, read_bundle
from futures_rebuild import annual_foundation_integration as integration
from futures_rebuild import annual_foundation_release as annual
from futures_rebuild import annual_foundation_storage as storage
from futures_rebuild import causal_1d, hourly_source_admission as hourly, daily_source_admission as daily
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from tests.test_annual_foundation_storage import source, budget
from tests.test_annual_foundation_integration import publish_generated
from tests.test_active_data_view import _write_stage

pytestmark = pytest.mark.current
ROOT = Path(__file__).resolve().parents[1]
HERE = ROOT/'state/unpublished_evidence/annual_dependency_retirement_v1'
spec = importlib.util.spec_from_file_location('retirement_prepare', HERE/'prepare.py')
prep = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prep)


def save(root, path, value):
    target = root/path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(canonical_bytes(signed({k:v for k,v in value.items() if k != 'id'}))+b'\n')
    return dict(path=path, sha256=sha256_file(target))


@contextmanager
def projected(root, overrides):
    name = 'futures_rebuild.annual_foundation_independence'
    spec = importlib.util.spec_from_file_location(name, root/'src/futures_rebuild/annual_foundation_independence.py')
    module = importlib.util.module_from_spec(spec)
    old = sys.modules.get(name)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    try:
        with integration.projected(root, overrides):
            yield module
    finally:
        if old is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = old


def fixture(root, *, quality_disposition='CORROBORATED_BY_FULL_MINUTES'):
    """Build signed generated evidence, not a historical certification claim."""
    root = io_path(root.resolve())
    names = set(causal_1d.implementation_binding()) | set(prep.FILES) | {
        Path(n).name for n in hourly.IMPLEMENTATION} | {'annual_foundation_storage.py',
        'active_data_view.py','daily_foundation_admission.py'}
    for name in names:
        target = root/'src/futures_rebuild'/name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT/'src/futures_rebuild'/name, target)
    shutil.copyfile(HERE/'runtime.py', root/'src/futures_rebuild/annual_foundation_independence.py')
    stage, catalog = _write_stage(root, plan_id='a'*64, markets=[('ES',2024)])
    shutil.copytree(stage, root/'data/active')
    minute_entry = catalog['entries'][0]
    permission = json.loads((root/minute_entry['sidecar_path']).read_bytes())
    entries, units, rows, manifests = [], {}, {}, {}
    for timeframe, duration in [('1m',60*10**9),('1h',3600*10**9),('1d',86400*10**9)]:
        scratch = root/('generated_'+timeframe)
        s = source(scratch, '2024-01', rows=2 if timeframe == '1m' else 1)
        old = scratch/s['directory']
        relative = f'data/causally_gated_normalized/ES/2024/{timeframe}/2024-01/'+timeframe[-1]*64
        # Valid hex specification IDs; distinct directories already encode cadence.
        relative = relative.rsplit('/',1)[0]+'/'+'a'*64
        dest = root/relative; dest.parent.mkdir(parents=True,exist_ok=True); old.rename(dest)
        s['directory'] = relative
        start = s['start_ns']
        path = dest/'observations.parquet'
        table = pq.read_table(path)
        values = dict(source_cadence=timeframe, bar_start_ns=start, bar_end_ns=start+duration,
                      available_at_ns=start+duration+5*10**9, decision_eligible_at_ns=start+duration+5*10**9)
        for field, value in values.items():
            vals = [value + i*duration if field != 'source_cadence' else value for i in range(table.num_rows)]
            table = table.set_column(table.schema.get_field_index(field), table.schema.field(field),
                                    pa.array(vals,type=table.schema.field(field).type))
        pq.write_table(table,path)
        s['files']['observations'] = dict(sha256=sha256_file(path),size=path.stat().st_size)
        release = dict(permitted_uses=['LINEAGE_AUDIT'], seal=dict(files=s['files']))
        original = save(root, relative+'/manifest.json', release)
        tracker = budget(root,attempt='generated_'+timeframe)
        directory, packing = storage.pack_candidate(root,market='ES',timeframe=timeframe,year=2024,
            sources=[s],coverage=[dict(month='2024-01',state='CERTIFIED')],budget=tracker)
        comparison = dict(status='PASS_INDEPENDENT_FIVE_TABLE_EQUIVALENCE',
            counts={k:v['rows'] for k,v in packing['files'].items()},compact_equivalence={'generated':True})
        if timeframe == '1m':
            minute_entry['annual_release_key'] = packing['release_key']
            minute_entry['parquet_path'],minute_entry['sidecar_path'] = annual.minute_paths(minute_entry)
            minute_entry['parquet_sha256'] = packing['files']['observations']['sha256']
            permission['entry_binding'] = {k:v for k,v in minute_entry.items() if k != 'sidecar_sha256'}
            permission['sidecar_id'] = sha256_json({k:v for k,v in permission.items() if k != 'sidecar_id'})
        reference, final = publish_generated(root,directory,packing,comparison,
            **({'minute_permission':permission} if timeframe == '1m' else {}))
        wrapper = read_record(final/'manifest.json')
        month = {}
        if timeframe == '1m':
            minute_entry['sidecar_sha256'] = reference['sha256']
        else:
            entry = dict(timeframe=timeframe,market='ES',month='2024-01',spec_id='a'*64,
                disposition='CERTIFIED',directory=relative,manifest=original['path'],
                manifest_sha256=original['sha256'],coverage_start_ns=start,coverage_end_ns=s['end_ns'],annual_release=reference)
            entries.append(entry)
            month['2024-01'] = dict(entry=entry,release=read_record(root/original['path']))
            if timeframe == '1d':
                month['2024-01']['daily'] = dict(purpose='LINEAGE_AUDIT',recovery_evidence=[],coverage=[dict(
                    day_ns=start,start_ns=start,end_ns=start+duration,state='OBSERVED_WITH_EXPLICIT_GAPS',
                    quarantined=0,quarantine=[],observations=1,source_primary_rows=1,excluded_start=0,excluded_availability=0)])
        units[wrapper['id']] = save(root,'state/independent/units/'+timeframe+'.json',dict(
            schema='annual_independence_unit/1',status='PASS_PREDECESSOR_AND_ANNUAL_EQUIVALENCE',
            wrapper=reference,wrapper_id=wrapper['id'],packing_id=packing['id'],
            counts=comparison['counts'],months=month))
        rows[timeframe] = read_bundle(dest)['observations']
        manifests[timeframe] = reference
    catalog['catalog_sha256'] = sha256_json({k:v for k,v in catalog.items() if k != 'catalog_sha256'})
    (root/'data/active/catalog.json').write_bytes(canonical_bytes(catalog)+b'\n')
    entries.sort(key=lambda e:(e['timeframe'],e['market'],e['month']))
    index = signed(dict(schema='causal_timeframe_index/3',status='PUBLISHED_CAUSAL_RECONSTRUCTION_ONLY',
        entries=entries,expected=[[e['timeframe'],e['market'],e['month']] for e in entries],
        legacy_1m=dict(path='data/active/catalog.json',sha256=sha256_file(root/'data/active/catalog.json')),
        annual_packaging={'retained_provenance':'generated'},publication={'retained_provenance':'generated'}))
    save(root,'data/active/timeframes.json',index)
    overrides = {}
    for name in prep.FILES:
        relative = 'src/futures_rebuild/'+name
        target = root/'state/unpublished_evidence/candidate'/name
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_text(prep.render(name,(ROOT/relative).read_text()),newline='\n')
        shutil.copyfile(target,root/relative)
        overrides[relative] = target.relative_to(root).as_posix()
    code = {p.relative_to(root).as_posix():sha256_file(p) for p in (root/'src/futures_rebuild').glob('*.py')}
    bindings = code | {p:sha256_file(root/p) for p in ('data/active/catalog.json','data/active/timeframes.json')}
    manifest = save(root,'state/independent/manifest.json',dict(schema='annual_foundation_independence/1',
        status='PREPARED_NOT_ACTIVE',permission_expansion=False,bindings=bindings,units=units,index_id=index['id'],
        wrappers={k:read_record(root/v['path'])['wrapper'] for k,v in units.items()}))
    result = save(root,'state/independent/validation.json',dict(status='PASS_ANNUAL_INDEPENDENCE',manifest=manifest,
        unit_count=len(units),checks=dict(population=True,permissions=True,isolated_cold_readers=True,raw_guards=True,rollback=True)))
    supervision = save(root,'state/independent/supervision.json',dict(status='PASS_SUPERVISED_PHASE',worker_result_sha256=result['sha256']))
    save(root,prep.POINTER,dict(schema='active_annual_foundation_independence/1',manifest=manifest,validation=result,supervision=supervision))
    # Construct actual hourly quality evidence and both public admission stacks.
    quality = [dict(row_id=bytes.fromhex(r['row_id']),market='ES',month='2024-01',start_ns=r['bar_start_ns'],
                    minute_count=60,quality_disposition=quality_disposition) for r in rows['1h']]
    qpath = root/'state/independent/hourly.parquet'; pq.write_table(pa.Table.from_pylist(quality),qpath)
    derived=dict(rows['1h'][0],market='ZQ',actual_contract='ZQF4',origin='DERIVED_1H_FROM_RETAINED_1M',
        market_coverage='NOT_ESTABLISHED',disposition='CANDIDATE_OBSERVED_MINUTES_ONLY',
        minute_count=1,full_minute_grid=False,constituent_row_ids=['generated-minute'])
    derived['id']=sha256_json(derived)
    dpath = root/'state/independent/derived.parquet'
    pq.write_table(pa.table({k:pa.array([v],type=pa.uint64() if k=='expiration_ns' else None)
                            for k,v in derived.items()}),dpath)
    census = save(root,'state/independent/census.json',dict(input_sha256={p:bindings[p] for p in ('data/active/catalog.json','data/active/timeframes.json')}))
    qm = save(root,'state/independent/quality_manifest.json',dict(census_sha256=census['sha256'],rows=1,quality_sha256=sha256_file(qpath)))
    dr = save(root,'state/independent/derived_result.json',dict(rows=1,candidate_sha256=sha256_file(dpath)))
    hmanifest = save(root,'state/independent/hourly_reader.json',dict(schema='hourly_quality_reader/1',permitted_uses=['LINEAGE_AUDIT'],
        policy='NATIVE_DEFAULT_EXPLICIT_MINUTE_DERIVED_DENSE_SINGLE_CONTRACT',implementation=code,
        evidence=dict(census=census,quality_manifest=qm,quality={'path':qpath.relative_to(root).as_posix()},
            derived_result=dr,derived={'path':dpath.relative_to(root).as_posix()})))
    hp = save(root,hourly.QUALITY_POINTER,dict(schema='active_hourly_quality_reader/1',manifest=hmanifest))
    foundation_paths = [hourly.FOUNDATION+'/supervision.json',hourly.FOUNDATION+'/execution/result.json']
    for p in foundation_paths: save(root,p,dict(generated=True))
    hb = bindings | {hourly.QUALITY_POINTER:hp['sha256']} | {p:sha256_file(root/p) for p in foundation_paths}
    hm = save(root,'state/independent/hourly_admission.json',dict(schema='hourly_source_admission/1',quality_pointer=hourly.QUALITY_POINTER,
        derived_permitted_uses=['LINEAGE_AUDIT'],bindings=hb,entries=hourly.policy_entries(index,catalog)))
    hr = save(root,'state/independent/hourly_result.json',dict(status='PASS_FOUNDATION_WITH_COVERAGE_LIMITATIONS',source_admission_manifest_sha256=hm['sha256']))
    hs = save(root,'state/independent/hourly_supervision.json',dict(status='PASS_SUPERVISED_FOUNDATION_AUDIT',worker_result_sha256=hr['sha256']))
    save(root,hourly.POINTER,dict(schema='active_hourly_source_admission/1',manifest=hm,result=hr,supervision=hs))
    quality = [dict(row_id=r['row_id'],market='ES',start_ns=r['bar_start_ns'],reason='FULL_HOURLY_AGREEMENT') for r in rows['1d']]
    qr = save(root,'state/independent/daily_quality.json',dict(status='PASS_DAILY_QUALITY_ACCOUNTING',rows=quality))
    with projected(root,overrides):
        dm = save(root,'state/independent/daily_admission.json',dict(schema='daily_source_admission/1',source='native',model_specific_eligibility='NOT_DECLARED',
            bindings=bindings | {minute_entry['sidecar_path']:minute_entry['sidecar_sha256']},
            daily_implementation=causal_1d.implementation_binding(),quality=qr,entries=daily.policy_entries(quality,catalog)))
    da = save(root,'state/independent/daily_audit.json',dict(status='PASS_DAILY_FOUNDATION_WITH_LIMITATIONS',admission_sha256=dm['sha256']))
    ds = save(root,'state/independent/daily_supervision.json',dict(status='PASS_SUPERVISED_DAILY_FOUNDATION',result_sha256=da['sha256']))
    save(root,daily.POINTER,dict(schema='active_daily_source_admission/1',manifest=dm,audit=da,supervision=ds))
    return root, overrides, rows, manifests


def args(row):
    return dict(market=row['market'],start_ns=row['bar_start_ns'],end_ns=row['bar_end_ns'],
                as_of_ns=row['decision_eligible_at_ns'],purpose='FEATURE_GENERATION')


def test_cold_public_readers_and_integrity_without_predecessors(tmp_path):
    root, overrides, rows, manifests = fixture(tmp_path)
    (root/'data/causally_gated_normalized/ES').rename(root/'parked_legacy')
    # Original comparison files are absent too. The current annual payloads remain.
    for p in root.glob('state/unpublished_evidence/generated*'):
        if p.is_dir(): p.rename(root/('parked_'+p.name))
    with projected(root,overrides) as runtime:
        from futures_rebuild import active_data_view
        readers = [('1h',hourly.HourlySourceReader(root)),('1d',daily.DailySourceReader(root))]
        for tf, reader in readers:
            got = reader.read_window(**args(rows[tf][0]))
            assert len(got) == 1
            assert all(got[0][k] == v for k,v in rows[tf][0].items())
            assert 'FEATURE_GENERATION' in reader.permitted_uses(market='ES',start_ns=got[0]['bar_start_ns'],end_ns=got[0]['bar_end_ns'])
            with pytest.raises(UnauthorizedOperation): reader.read_window(**(args(rows[tf][0])|{'purpose':'SELECTION'}))
            with pytest.raises(UnauthorizedOperation): reader.read_window(**(args(rows[tf][0])|{'as_of_ns':got[0]['decision_eligible_at_ns']-1}))
        minute = active_data_view.resolve(repository_root=root,market='ES',year=2024,purpose='FEATURE_GENERATION')
        assert '/ohlcv_1m/' in minute.as_posix()
        for tf, ref in manifests.items():
            assert runtime.audit_unit(root,ref)['observations'] == len(rows[tf])
        from futures_rebuild import causal_timeframe_publication as publication
        for tf in ('1h','1d'):
            with pytest.raises(UnauthorizedOperation):
                publication.resolve_partitions(root,{},market='ES',year=2024,timeframe=tf,purpose='LINEAGE_AUDIT')


@pytest.mark.parametrize('target',['pointer','unit','validation','supervision','implementation','index','annual_payload'])
def test_missing_or_changed_independence_evidence_refuses(tmp_path,target):
    root, overrides, rows, manifests = fixture(tmp_path)
    paths = dict(pointer=prep.POINTER,unit='state/independent/units/1h.json',validation='state/independent/validation.json',
        supervision='state/independent/supervision.json',implementation='src/futures_rebuild/annual_foundation_independence.py',
        index='data/active/timeframes.json',annual_payload=str(Path(manifests['1h']['path']).with_name('observations.parquet')))
    with projected(root,overrides):
        reader = hourly.HourlySourceReader(root)
        path = root/paths[target]
        path.rename(path.with_name(path.name+'.parked'))
        with pytest.raises((IntegrityError,OSError)):
            reader.read_window(**args(rows['1h'][0]))
        if target not in ('unit','annual_payload'):
            with pytest.raises((IntegrityError,OSError)):
                reader.permitted_uses(market='UNKNOWN',start_ns=rows['1h'][0]['bar_start_ns'],end_ns=rows['1h'][0]['bar_end_ns'])


def test_successor_recipe_rejects_repeat_application():
    for name in prep.FILES:
        text = prep.render(name,(ROOT/'src/futures_rebuild'/name).read_text())
        with pytest.raises(IntegrityError,match='already applied'): prep.render(name,text)


def test_independent_comparison_and_physical_allowlist(tmp_path):
    root, _, _, manifests = fixture(tmp_path)
    tracker = budget(root,attempt='comparison')
    for ref in manifests.values():
        packing = read_record(root/ref['path'])['packing']
        inputs = {}
        for s in packing['sources']:
            inputs.update({s['directory']+'/'+FILENAMES[n]:i for n,i in s['files'].items()})
        inputs.update({str(Path(ref['path']).parent.as_posix())+'/'+FILENAMES[n]:i for n,i in packing['files'].items()})
        assert prep.compare_unit(root,ref,inputs,tracker)['counts']['observations'] == packing['files']['observations']['rows']
        removed = inputs.pop(next(iter(inputs)))
        with pytest.raises(IntegrityError,match='outside the approved scope'):
            prep.compare_unit(root,ref,inputs,tracker)


def test_real_constructor_refuses_gaps_and_unsupported_daily_source(tmp_path):
    root, overrides, rows, _ = fixture(tmp_path)
    with projected(root,overrides):
        for tf,reader in [('1h',hourly.HourlySourceReader(root)),('1d',daily.DailySourceReader(root))]:
            request=args(rows[tf][0]); duration=request['end_ns']-request['start_ns']
            with pytest.raises(UnauthorizedOperation):
                reader.read_window(**(request|{'end_ns':request['end_ns']+duration,'as_of_ns':request['as_of_ns']+duration}))
        with pytest.raises(Exception) as error:
            daily.DailySourceReader(root).read_window(**(args(rows['1d'][0])|{'source':'minute_derived'}))
        assert 'native' in str(error.value)


def test_recovered_daily_identity_survives_removal_of_both_generations(tmp_path):
    from tests.test_daily_identity_publication import fixture as recovered_fixture, campaign, BASE, DAY
    from futures_rebuild import daily_quality_reader
    recovery_root=tmp_path/'recovery'; recovery_root.mkdir()
    _,_,old_overrides,entry,_=recovered_fixture(recovery_root)
    with campaign.projected_successor(recovery_root,old_overrides):
        expected=daily_quality_reader.DailyReader(recovery_root).read_window(market='ES',start_ns=BASE,end_ns=BASE+DAY,
            as_of_ns=BASE+DAY+5*10**9,purpose='SYNTHETIC_TEST')
    root,overrides,_,_=fixture(tmp_path/'independent')
    release=read_record(io_path(recovery_root/entry['manifest']))
    dest=root/entry['directory']; dest.parent.mkdir(parents=True,exist_ok=True)
    shutil.copytree(io_path(recovery_root/entry['directory']),dest)
    source=dict(month=entry['month'],directory=entry['directory'],start_ns=entry['coverage_start_ns'],
        end_ns=entry['coverage_end_ns'],files={n:{k:i[k] for k in ('size','sha256')} for n,i in release['files'].items()})
    directory,packing=storage.pack_candidate(root,market='ES',timeframe='1d',year=2024,sources=[source],
        coverage=[dict(month=entry['month'],state='CERTIFIED')],budget=budget(root,attempt='recovered'))
    comparison=dict(status='PASS_INDEPENDENT_FIVE_TABLE_EQUIVALENCE',counts={k:v['rows'] for k,v in packing['files'].items()})
    ref,destination=publish_generated(root,directory,packing,comparison)
    wrapper=read_record(destination/'manifest.json'); entry=entry|{'annual_release':ref}
    index=read_record(root/'data/active/timeframes.json');index['entries']=[e for e in index['entries'] if e['timeframe']!='1d']+[entry]
    index['entries'].sort(key=lambda e:(e['timeframe'],e['market'],e['month']))
    index['expected']=[[e['timeframe'],e['market'],e['month']] for e in index['entries']]
    save(root,'data/active/timeframes.json',index);index=read_record(root/'data/active/timeframes.json')
    unit=save(root,'state/independent/units/recovered.json',dict(schema='annual_independence_unit/1',
        status='PASS_PREDECESSOR_AND_ANNUAL_EQUIVALENCE',wrapper=ref,wrapper_id=wrapper['id'],packing_id=packing['id'],
        counts=comparison['counts'],months={entry['month']:dict(entry=entry,release=release,
            daily=dict(purpose='SYNTHETIC_TEST',coverage=release['coverage'],recovery_evidence=release['evidence']))}))
    manifest=read_record(root/'state/independent/manifest.json')
    manifest['units']={k:v for k,v in manifest['units'].items() if not v['path'].endswith('/1d.json')}
    manifest['units'][wrapper['id']]=unit;manifest['index_id']=index['id']
    manifest['wrappers']={k:read_record(root/v['path'])['wrapper'] for k,v in manifest['units'].items()}
    manifest['bindings']['data/active/timeframes.json']=sha256_file(root/'data/active/timeframes.json')
    manifest_ref=save(root,'state/independent/manifest.json',manifest)
    result=read_record(root/'state/independent/validation.json');result['manifest']=manifest_ref
    result_ref=save(root,'state/independent/validation.json',result)
    supervision=save(root,'state/independent/supervision.json',dict(status='PASS_SUPERVISED_PHASE',worker_result_sha256=result_ref['sha256']))
    save(root,prep.POINTER,dict(schema='active_annual_foundation_independence/1',manifest=manifest_ref,validation=result_ref,supervision=supervision))
    (root/'data/causally_gated_normalized/ES').rename(root/'parked_both_monthly_generations')
    with projected(root,overrides):
        got=daily_quality_reader.DailyReader(root).read_window(market='ES',start_ns=BASE,end_ns=BASE+DAY,
            as_of_ns=BASE+DAY+5*10**9,purpose='SYNTHETIC_TEST')
        assert got==expected and got[0]['definition_received_at_ns']>BASE


@pytest.mark.parametrize('failure',list(range(len(prep.CONTROL_ORDER)))+['default','deadline','memory'])
def test_exact_candidate_control_order_rolls_back(tmp_path,monkeypatch,failure):
    from tests.test_daily_rollout_memory import helper
    changes,expected={},{}
    for i,name in enumerate(prep.CONTROL_ORDER):
        path=tmp_path/name;path.parent.mkdir(parents=True,exist_ok=True)
        if name not in (prep.POINTER,'src/futures_rebuild/annual_foundation_independence.py'):
            path.write_bytes(('before:'+name).encode())
        expected[name]=sha256_file(path) if path.exists() else None
        candidate=tmp_path/('candidate_'+str(i));candidate.write_bytes(('after:'+name).encode())
        changes[name]=candidate.name
    replace=helper.os.replace
    def fail(source,dest):
        if type(failure) is int and Path(source).name==str(failure)+'.pending':
            raise OSError('generated replacement failure')
        return replace(source,dest)
    monkeypatch.setattr(helper.os,'replace',fail)
    def validate():
        if isinstance(failure,str): raise IntegrityError('generated '+failure)
    with pytest.raises((OSError,IntegrityError)):
        helper.activate_controls(tmp_path,changes,expected=expected,rollback_directory='rollback',validate=validate)
    assert {p:sha256_file(tmp_path/p) if (tmp_path/p).exists() else None for p in expected}==expected


def test_policy_successors_change_bindings_only():
    quality=dict(implementation={'src/futures_rebuild/hourly_quality_reader.py':'old'},evidence={'quality':'same'},permitted_uses=['LINEAGE_AUDIT'])
    hour=dict(bindings={'configs/active_hourly_quality_reader.json':'prior'},entries=[{'permitted_uses':['LINEAGE_AUDIT']}],derived_permitted_uses=['LINEAGE_AUDIT'])
    day=dict(bindings={},daily_implementation={'daily_quality_reader.py':'old'},quality={'rows':'same'},entries=[{'reason':'EXCLUDED_HOURLY_REFERENCE'}])
    result=prep.policy_successors(quality,hour,day,{'src/futures_rebuild/hourly_quality_reader.py':'new','src/futures_rebuild/daily_quality_reader.py':'new'},'next')
    assert result[0]['evidence']==quality['evidence'] and result[1]['entries']==hour['entries'] and result[2]['entries']==day['entries']
    assert result[1]['bindings']['configs/active_hourly_quality_reader.json']=='next'
    assert result[2]['daily_implementation']['daily_quality_reader.py']=='new'
    assert day['daily_implementation']['daily_quality_reader.py']=='old'


def test_partial_population_cannot_answer_permission_queries(tmp_path):
    root,overrides,rows,_=fixture(tmp_path)
    manifest=read_record(root/'state/independent/manifest.json')
    key=next(iter(manifest['units']));manifest['units'].pop(key);manifest['wrappers'].pop(key)
    ref=save(root,'state/independent/manifest.json',manifest)
    result=read_record(root/'state/independent/validation.json');result.update(manifest=ref,unit_count=2)
    rr=save(root,'state/independent/validation.json',result)
    sr=save(root,'state/independent/supervision.json',dict(status='PASS_SUPERVISED_PHASE',worker_result_sha256=rr['sha256']))
    save(root,prep.POINTER,dict(schema='active_annual_foundation_independence/1',manifest=ref,validation=rr,supervision=sr))
    with projected(root,overrides):
        with pytest.raises(IntegrityError,match='omits or duplicates'):
            hourly.HourlySourceReader(root)


def test_protected_physical_boundary_rejected_before_payload_hash(tmp_path,monkeypatch):
    root,_,_,refs=fixture(tmp_path)
    from futures_rebuild.causal_timeframe_publication import CUTOFF
    ref=refs['1h'];wrapper=read_record(root/ref['path'])
    wrapper['packing']['sources'][0]['end_ns']=CUTOFF+1
    changed=save(root,ref['path'],wrapper)
    tracker=budget(root,attempt='protected')
    def forbidden(*args): raise AssertionError('Protected payload opened')
    monkeypatch.setattr(tracker,'hash',forbidden)
    with pytest.raises(IntegrityError,match='Protected source boundary'):
        prep.compare_unit(root,changed,{},tracker)


def test_worker_rejects_unapproved_scope_before_opening_inputs(tmp_path):
    spec=importlib.util.spec_from_file_location('retirement_worker',HERE/'worker.py')
    worker=importlib.util.module_from_spec(spec);spec.loader.exec_module(worker)
    with pytest.raises(IntegrityError,match='Only the bounded diagnostic'):
        worker.validate_request(tmp_path,dict(phase='activate'))
    with pytest.raises(IntegrityError):
        worker.validate_request(tmp_path,dict(phase='independence_pilot',scope='missing.json',authorization='missing.json'))


def test_existing_readers_reject_replacement_even_with_valid_authority(tmp_path):
    root,overrides,rows,_=fixture(tmp_path)
    with projected(root,overrides):
        from futures_rebuild import daily_quality_reader
        h=hourly.HourlySourceReader(root);d=daily.DailySourceReader(root);q=daily_quality_reader.DailyReader(root)
        pointer=read_record(root/prep.POINTER);pointer['revision_note']='generated replacement'
        save(root,prep.POINTER,pointer)
        for reader,tf in ((h,'1h'),(d,'1d')):
            with pytest.raises(IntegrityError,match='pointer changed'):
                reader.permitted_uses(market='UNKNOWN',start_ns=rows[tf][0]['bar_start_ns'],end_ns=rows[tf][0]['bar_end_ns'])
        with pytest.raises(IntegrityError,match='pointer changed'):
            q.read_window(**(args(rows['1d'][0])|{'purpose':'LINEAGE_AUDIT'}))


def test_metadata_inventory_accounts_for_minute_packing_sources(tmp_path):
    root,_,_,_=fixture(tmp_path)
    tracker=budget(root,attempt='inventory')
    output=tracker.directory.relative_to(root).as_posix()
    inventory=prep.inventory(root,output,tracker)
    result=prep.classify_packing_dependencies(root,inventory,output,tracker)
    assert result['annual_units']==3 and result['source_directories']==3
    assert result['counts']=={'ANNUAL_PACKING_PREDECESSOR_KEEP':18}
    with (root/result['files']['path']).open('rb') as stream:
        assert all(json.loads(line)['retirement_authorized'] is False for line in stream)


def test_future_authority_preparation_reuses_certificates_without_legacy_files(tmp_path):
    root,_,_,_=fixture(tmp_path)
    current=read_record(root/'state/independent/manifest.json')
    records=[(r,read_record(root/r['path'])) for r in current['units'].values()]
    index=read_record(root/'data/active/timeframes.json');catalog=json.loads((root/'data/active/catalog.json').read_bytes())
    (root/'data/causally_gated_normalized/ES').rename(root/'parked')
    result=prep.prepare_authority(index,catalog,records,current['bindings'])
    assert result['status']=='PREPARED_NOT_ACTIVE' and result['units']==current['units']
    with pytest.raises(IntegrityError,match='Incomplete proposed annual population'):
        prep.prepare_authority(index,catalog,records[:-1],current['bindings'])
    record=records[0][1];record['status']='PREPARED_NEEDS_INDEPENDENT_COMPARISON'
    with pytest.raises(IntegrityError,match='passing immutable'):
        prep.prepare_authority(index,catalog,records,current['bindings'])


def test_worker_binds_absent_controls_and_existing_bytes(tmp_path):
    spec=importlib.util.spec_from_file_location('retirement_worker',HERE/'worker.py')
    worker=importlib.util.module_from_spec(spec);spec.loader.exec_module(worker)
    path=tmp_path/'existing';path.write_bytes(b'original')
    controls={'existing':sha256_file(path),'new_pointer':None}
    worker.check_predecessors(tmp_path,controls)
    (tmp_path/'new_pointer').write_bytes(b'unexpected')
    with pytest.raises(IntegrityError,match='control changed'):
        worker.check_predecessors(tmp_path,controls)


def test_exclusions_and_explicit_audit_only_derived_survive(tmp_path):
    root,overrides,rows,_=fixture(tmp_path,quality_disposition='QUARANTINE_UNRESOLVED')
    (root/'data/causally_gated_normalized/ES').rename(root/'parked_legacy')
    with projected(root,overrides):
        reader=hourly.HourlySourceReader(root)
        request=args(rows['1h'][0])
        with pytest.raises(UnauthorizedOperation): reader.read_window(**request)
        derived=request|{'market':'ZQ','source':'minute_derived','purpose':'LINEAGE_AUDIT'}
        got=reader.read_window(**derived)
        assert len(got)==1 and got[0]['market_coverage']=='NOT_ESTABLISHED' and not got[0]['full_minute_grid']
        for change in ({'purpose':'FEATURE_GENERATION'},{'source':'native'}):
            with pytest.raises(UnauthorizedOperation): reader.read_window(**(derived|change))
