"""Generated two-unit conversion; no migration or activation is executed."""
import json
from pathlib import Path
import pytest

from futures_rebuild import annual_foundation_migration as migration
from futures_rebuild.canonical import sha256_file, canonical_bytes
from futures_rebuild.causal_timeframe_storage import signed, Budget, read_record
from futures_rebuild.errors import IntegrityError
from tests.test_annual_foundation_storage import source, budget

pytestmark = pytest.mark.current


def inputs(root):
    units, physical, entries = [], {}, []
    for year in (2023, 2024):
        src = source(root, f'{year}-01', rows=3)
        for name, info in src['files'].items():
            physical[src['directory']+'/'+name+'.parquet'] = info|{'role':'SELECTED_MONTHLY_TABLE'}
        compact_path=src['directory']+'/observations.parquet'
        physical[compact_path]['role']='CURRENT_MINUTE_EQUIVALENCE'
        compact=dict(path=compact_path,sha256=src['files']['observations']['sha256'])
        units.append(dict(market='ES',year=year,timeframe='1m',sources=[src],
                          coverage=[dict(month=src['month'],state='CERTIFIED')],compact_reference=compact))
        entries.append(dict(parquet_path=compact_path,parquet_sha256=compact['sha256'],coverage_end=f'{year}-02-01'))
    path=root/'data/active/catalog.json';path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'entries':entries}))
    return dict(units=units,physical_payloads=physical,
                metadata_and_implementation_bindings={'data/active/catalog.json':sha256_file(path)})


def test_sequential_population_compares_each_unit_once_and_retains_sources(tmp_path, monkeypatch):
    plan=inputs(tmp_path);tracker=budget(tmp_path,'migration_generated')
    before={p:sha256_file(tmp_path/p) for p in plan['physical_payloads']}
    calls=[];original=migration._check_inputs
    def checked(*args):
        calls.append(True)
        return original(*args)
    monkeypatch.setattr(migration,'_check_inputs',checked)
    packed=migration.perform(tmp_path,plan,'pack',tracker)
    tracker.save(tracker.directory/'pack.json',signed(dict(units=packed)))
    compared=migration.perform(tmp_path,plan,'compare',tracker)
    assert len(compared)==2 and len(calls)==4
    assert all(r['comparison']['status']=='PASS_INDEPENDENT_FIVE_TABLE_EQUIVALENCE' for r in compared)
    assert all(r['comparison']['compact_equivalence'] is not None for r in compared)
    assert len(list(tracker.directory.glob('compare_*.json')))==2
    assert {p:sha256_file(tmp_path/p) for p in before}==before
    assert not (tmp_path/'configs').exists()
    with pytest.raises(IntegrityError,match='Unsupported'):
        migration.perform(tmp_path,plan,'activate',tracker)


def test_metadata_unit_preparation_preserves_cutoff_and_unavailable_months(tmp_path):
    from reports.dbn_layout_alignment_20260915.prepare_full_migration import units_from_inventory
    from futures_rebuild.annual_foundation_storage import CUTOFF
    plan=inputs(tmp_path)
    inventory=dict(units=[],physical_files=plan['physical_payloads'])
    catalog=json.loads((tmp_path/'data/active/catalog.json').read_bytes())
    extra={}
    for unit,entry in zip(plan['units'],catalog['entries'],strict=True):
        entry.update(market=unit['market'],year=unit['year'])
        s=unit['sources'][0]
        extra[entry['parquet_path']]=plan['physical_payloads'][entry['parquet_path']]|dict(end_ns=s['end_ns'])
        s=s|dict(interval=f"{unit['year']}-01-01_{unit['year']}-02-01",
            files={n:s['directory']+'/'+n+'.parquet' for n in s['files']})
        inventory['units'].append(unit|dict(sources=[s]))
    original=inventory['units'][0]
    hourly=original|dict(timeframe='1h');inventory['units'].append(hourly)
    index=dict(entries=[dict(timeframe='1h',market='ES',year=2023,month='2023-01',disposition='CERTIFIED'),
                        dict(timeframe='1h',market='ES',year=2023,month='2023-02',disposition='UNAVAILABLE')])
    units,physical=units_from_inventory(inventory,index,catalog,extra)
    assert units[-1]['coverage'][-1]==dict(month='2023-02',state='UNAVAILABLE')
    assert units[0]['sources']==plan['units'][0]['sources']
    extra[catalog['entries'][0]['parquet_path']]['end_ns']=CUTOFF+1
    with pytest.raises(IntegrityError,match='Protected'):
        units_from_inventory(inventory,index,catalog,extra)


def test_changed_payload_and_incomplete_population_fail_closed(tmp_path):
    plan=inputs(tmp_path);tracker=budget(tmp_path,'migration_generated')
    packed=migration.perform(tmp_path,plan,'pack',tracker)
    tracker.save(tracker.directory/'pack.json',signed(dict(units=packed[:1])))
    with pytest.raises(IntegrityError,match='Incomplete packed'):
        migration.perform(tmp_path,plan,'compare',tracker)
    target=tmp_path/next(iter(plan['physical_payloads']))
    target.write_bytes(target.read_bytes()+b'changed')
    with pytest.raises(IntegrityError,match='Approved payload changed'):
        migration.perform(tmp_path,plan,'compare',tracker)


@pytest.mark.parametrize('failure',[None,'default','deadline','memory'])
def test_metered_activation_retains_exact_predecessors_and_counts_restoration(tmp_path,monkeypatch,failure):
    from tests.test_daily_rollout_memory import helper
    from futures_rebuild import daily_identity_rollout
    tracker=budget(tmp_path,'metered_controls')
    reserve=tracker.terminal_reserve
    monkeypatch.setattr(daily_identity_rollout,'helper',lambda root:helper)
    changes={};expected={};old={}
    for i in range(3):
        target=f'configs/control{i}.json';candidate=f'prepared/control{i}.json'
        (tmp_path/candidate).parent.mkdir(parents=True,exist_ok=True)
        (tmp_path/candidate).write_bytes(b'new control '+bytes([i]))
        (tmp_path/target).parent.mkdir(parents=True,exist_ok=True)
        if i<2:(tmp_path/target).write_bytes(b'old'+bytes([i]))
        old[target]=(tmp_path/target).read_bytes() if i<2 else None
        changes[target]=candidate;expected[target]=sha256_file(tmp_path/target) if i<2 else None
    def validate():
        if failure=='deadline':tracker.started-=tracker.limits.maximum_seconds+1
        if failure=='memory':tracker.peak_memory=tracker.limits.maximum_memory_bytes+1
        if failure=='default':raise RuntimeError('default failed')
    kwargs=dict(expected=expected,rollback_directory=migration.BASE+'/metered_controls/rollback',
                validate=validate,budget=tracker)
    if failure:
        with pytest.raises((RuntimeError,IntegrityError)):
            migration.activate_controls(tmp_path,changes,**kwargs)
        assert {p:(tmp_path/p).read_bytes() if (tmp_path/p).exists() else None for p in changes}==old
    else:
        migration.activate_controls(tmp_path,changes,**kwargs)
        assert all((tmp_path/p).read_bytes()==(tmp_path/c).read_bytes() for p,c in changes.items())
    assert tracker.written==sum((tmp_path/c).stat().st_size for c in changes.values())+sum(len(v) for v in old.values() if v)*(2 if failure else 1)
    assert tracker.terminal_reserve==reserve and not tracker.finalizing


def test_promotion_moves_only_new_payloads_and_preserves_source_and_evidence(tmp_path):
    from futures_rebuild.annual_foundation_release import SCHEMA
    plan=inputs(tmp_path);tracker=budget(tmp_path,'migration_v1_payloads')
    packed=migration.perform(tmp_path,plan,'pack',tracker)
    source_dir=tracker.directory/packed[0]['directory']
    packing=read_record(source_dir/'manifest.json')
    wrapper=signed(dict(schema=SCHEMA,status='CERTIFIED_PACKAGING_NO_PERMISSION_EXPANSION',packing=packing))
    manifest=tracker.directory/'reviewed_wrapper.json'
    tracker.save(manifest,wrapper)
    reference=dict(path=migration.OUTPUT+'/reviewed_wrapper.json',sha256=sha256_file(manifest))
    # Publication uses the existing journal Budget, not the staging-only writer.
    journal=Budget(tracker.directory,tracker.limits)
    destination=tmp_path/migration.promote_unit(tmp_path,manifest_reference=reference,budget=journal)
    assert (source_dir/'manifest.json').exists() and manifest.exists()
    for name,info in packing['files'].items():
        assert not (source_dir/info['filename']).exists()
        assert sha256_file(destination/info['filename'])==info['sha256']
    assert all(sha256_file(tmp_path/p)==info['sha256'] for p,info in plan['physical_payloads'].items())
    with pytest.raises(IntegrityError,match='already exists'):
        migration.promote_unit(tmp_path,manifest_reference=reference,budget=journal)
    with pytest.raises(IntegrityError,match='Task-owned'):
        migration.promote_unit(tmp_path,manifest_reference=reference|{'path':'data/active/catalog.json'},budget=journal)


@pytest.mark.parametrize('tamper',[None,'incomplete','supervision','population'])
def test_pointer_preparation_requires_passing_bound_full_population(tmp_path,tamper):
    tracker=budget(tmp_path,'pointer_generated')
    def save(name,value):
        path=tracker.directory/name;tracker.save(path,signed(value))
        return dict(path=path.relative_to(tracker.directory).as_posix(),sha256=sha256_file(path))
    def reference(name,value):
        ref=save(name,value);ref['path']=migration.BASE+'/pointer_generated/'+ref['path'];return ref
    policies={};populations={}
    for lane in ('hourly','daily'):
        pointer='configs/active_'+lane+'_source_admission.json'
        path=tmp_path/pointer;path.parent.mkdir(parents=True,exist_ok=True)
        key='result' if lane=='hourly' else 'audit'
        path.write_bytes(canonical_bytes(signed({key:dict(path='retained.json',sha256='a'*64)})))
        policies[pointer]=dict(path=lane+'_policy.json',sha256='b'*64,entry_count=3,entries_sha256='c'*64)
        populations[lane]=dict(entries=3,entries_sha256='c'*64)
    config=reference('configuration.json',dict(policies=policies))
    validation=dict(status='PASS_ANNUAL_FOUNDATION_VALIDATION',configuration_sha256=config['sha256'],
        fully_compared_annual_units=1817,observation_counts={'1m':139892740,'1h':3023365,'1d':167609},
        policy_populations=populations,representative_reader_checks={'hourly':[{'accepted':False}],'daily':[{'accepted':True}]})
    if tamper=='incomplete':validation['fully_compared_annual_units']=5
    if tamper=='population':validation['policy_populations']['daily']['entries']=2
    checked=reference('validation.json',validation)
    supervised=reference('supervision.json',dict(status='PASS_SUPERVISED_PHASE',
        worker_result_sha256='0'*64 if tamper=='supervision' else checked['sha256']))
    if tamper:
        with pytest.raises(IntegrityError):
            migration.prepare_admission_pointers(tmp_path,configuration=config,validation=checked,supervision=supervised,budget=tracker)
    else:
        refs=migration.prepare_admission_pointers(tmp_path,configuration=config,validation=checked,supervision=supervised,budget=tracker)
        assert set(refs)==set(policies)
        for target,ref in refs.items():
            assert read_record(tmp_path/ref['path'])['manifest']['sha256']=='b'*64
            assert 'manifest' not in json.loads((tmp_path/target).read_bytes())
