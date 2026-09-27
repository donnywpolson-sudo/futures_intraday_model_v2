"""Generated data only: production successor methods, immutable old controls."""
from copy import deepcopy
from pathlib import Path
import shutil

import pytest

from futures_rebuild import annual_foundation_integration as integration
from futures_rebuild import annual_foundation_release as release
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json, io_path
from futures_rebuild.causal_timeframe_storage import contained, read_record, signed
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from tests.test_annual_foundation_storage import source, budget, pack
from tests.test_annual_foundation_reader_preview import setup, save
from futures_rebuild.annual_foundation_pilot import compare_sources

pytestmark = pytest.mark.current
ROOT = Path(__file__).resolve().parents[1]


def successors(root):
    for name in (*integration.FILES, 'annual_foundation_release.py', 'annual_foundation_integration.py'):
        dest = root/'src/futures_rebuild'/name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT/'src/futures_rebuild'/name, dest)
    changes = integration.prepare(root, budget(root, 'integration'))
    return changes, {k:v['candidate']['path'] for k,v in changes.items()}


def publish_generated(root, directory, manifest, comparison, *, minute_permission=None):
    """Synthetic fixture publication; no application execution authority."""
    prefix = 'state/unpublished_evidence/generated'
    result_path = prefix+'/compare.json'
    digest = save(root/result_path, dict(status='PASS_RECOVERY_PHASE', phase='compare', units=[dict(
        manifest_sha256=sha256_file(directory/'manifest.json'), comparison=comparison)]))
    supervision = prefix+'/supervision.json'
    sup_hash = save(root/supervision, dict(status='PASS_SUPERVISED_PHASE', worker_result_sha256=digest))
    dest = root/release.directory(manifest['timeframe'], manifest['market'], manifest['year'], manifest['release_key'])
    dest.mkdir(parents=True)
    for filename in ('observations.parquet','quality.parquet','cadence.parquet','missingness.parquet','roll.parquet'):
        shutil.copyfile(directory/filename, dest/filename)
    wrapped = dict(schema=release.SCHEMA, status='CERTIFIED_PACKAGING_NO_PERMISSION_EXPANSION',
        packing=manifest, comparison=dict(path=result_path, sha256=digest),
        supervision=dict(path=supervision, sha256=sup_hash),
        implementation={n:sha256_file(root/n) for n in (
            'src/futures_rebuild/annual_foundation_release.py','src/futures_rebuild/annual_foundation_storage.py')})
    if minute_permission is not None:
        wrapped['minute_permission'] = minute_permission
    digest = save(dest/'manifest.json', wrapped)
    return dict(path=(dest/'manifest.json').relative_to(root).as_posix(), sha256=digest), dest


def hourly(root):
    reader, candidates, request, directory = setup(root)
    changes, overrides = successors(root)
    original = reader.quality_reader
    manifest = read_record(directory/'manifest.json')
    ref = next(iter(candidates.values()))['comparison']
    comparison = read_record(root/ref['path'])['units'][0]['comparison']
    annual_ref, dest = publish_generated(root, directory, manifest, comparison)
    # Candidate code hashes, as they will appear under projection.
    wrapped = read_record(dest/'manifest.json')
    wrapped['implementation']['src/futures_rebuild/annual_foundation_storage.py'] = changes['src/futures_rebuild/annual_foundation_storage.py']['candidate']['sha256']
    annual_ref['sha256'] = save(dest/'manifest.json', {k:v for k,v in wrapped.items() if k != 'id'})
    original.entries[('ES','2024-01')]['annual_release'] = annual_ref
    return reader, request, overrides, dest


def test_render_is_exact_and_projection_restores_modules(tmp_path):
    changes, overrides = successors(tmp_path)
    from futures_rebuild import active_data_view as minute
    before = minute.resolve
    with integration.projected(tmp_path, overrides):
        assert minute.resolve is not before
        assert 'verify_minute' in minute.resolve.__code__.co_names
    assert minute.resolve is before
    for name in integration.FILES:
        original = (ROOT/'src/futures_rebuild'/name).read_text()
        assert integration.render(name, original)
    with pytest.raises(IntegrityError, match='predecessor differs'):
        integration.render('hourly_quality_reader.py', 'changed')
    assert all(sha256_file(tmp_path/k) == v['predecessor']['sha256'] for k,v in changes.items())


def test_hourly_annual_public_methods_equal_legacy_and_refuse_boundaries(tmp_path):
    reader, request, overrides, dest = hourly(tmp_path)
    # Existing reader remains bound to monthly bytes, providing an independent oracle.
    expected = reader.read_window(**request)
    from futures_rebuild import hourly_quality_reader as quality
    from futures_rebuild import hourly_source_admission as admission
    with integration.projected(tmp_path, overrides):
        new = object.__new__(admission.HourlySourceReader)
        new.__dict__.update(reader.__dict__)
        new.quality_reader = object.__new__(quality.HourlyReader)
        new.quality_reader.__dict__.update(reader.quality_reader.__dict__)
        assert new.read_window(**request) == expected
        for change, message in [({'purpose':'SELECTION'}, 'not admitted'),
                ({'as_of_ns':request['as_of_ns']-1}, 'not yet available'),
                ({'end_ns':request['end_ns']+quality.HOUR}, 'coverage is absent')]:
            with pytest.raises(UnauthorizedOperation, match=message):
                new.read_window(**(request|change))
        # Changes are checked on every request, not trusted from a previous read.
        with (dest/'observations.parquet').open('ab') as stream:
            stream.write(b'changed')
        with pytest.raises(IntegrityError, match='changed'):
            new.read_window(**request)


@pytest.mark.parametrize('mutation', ['inactive','comparison','implementation','wrong_month','unavailable','wrong_source'])
def test_packaging_cannot_supply_authority_or_change_sources(tmp_path, mutation):
    reader, request, overrides, dest = hourly(tmp_path)
    entry = deepcopy(reader.quality_reader.entries[('ES','2024-01')])
    with integration.projected(tmp_path, overrides):
        if mutation == 'comparison':
            with (tmp_path/'state/unpublished_evidence/generated/compare.json').open('ab') as stream:
                stream.write(b'changed')
        elif mutation in ('inactive','implementation'):
            value = read_record(dest/'manifest.json')
            if mutation == 'inactive': value['status'] = 'PREPARED_NOT_ADMITTED'
            else: value['implementation'] = {}
            entry['annual_release']['sha256'] = save(dest/'manifest.json', {k:v for k,v in value.items() if k!='id'})
        elif mutation == 'wrong_month': entry['month'] = '2024-02'
        elif mutation == 'unavailable': entry['disposition'] = 'UNAVAILABLE'
        else: entry['directory'] += '_other'
        with pytest.raises(IntegrityError):
            release.validate_month(tmp_path, entry)


def test_annual_storage_does_not_decode_other_months(tmp_path, monkeypatch):
    sources = [source(tmp_path, m) for m in ('2024-01','2024-02')]
    directory, manifest, tracker = pack(tmp_path, sources)
    compare = compare_sources(tmp_path, directory, {k:manifest[k] for k in ('sources','year','market','timeframe','coverage')},
        manifest_sha256=sha256_file(directory/'manifest.json'), budget=tracker,
        compact=dict(path=(directory/'observations.parquet').relative_to(io_path(tmp_path)).as_posix(),sha256=sha256_file(directory/'observations.parquet')))
    changes, overrides = successors(tmp_path)
    ref, dest = publish_generated(tmp_path, directory, manifest, compare)
    from futures_rebuild import annual_foundation_storage as storage
    with integration.projected(tmp_path, overrides):
        calls = []
        original = storage._read_candidate.__globals__['_group']
        def observed(file, index, limit):
            calls.append(index)
            return original(file, index, limit)
        monkeypatch.setitem(storage._read_candidate.__globals__, '_group', observed)
        groups = list(storage._read_candidate(dest, manifest_sha256=ref['sha256'],
            months=['2024-02'], maximum_group_bytes=64*2**20, _packing=manifest))
        assert len(groups) == 1 and calls == [1]*5
        with pytest.raises(UnauthorizedOperation, match='unavailable'):
            list(storage._read_candidate(dest, manifest_sha256=ref['sha256'],
                months=['2024-03'], maximum_group_bytes=64*2**20, _packing=manifest))


def test_minute_public_resolver_and_policy_sidecar_keep_return_type(tmp_path):
    import json
    from tests.test_active_data_view import _write_stage
    from futures_rebuild import active_data_view as minute
    from futures_rebuild.daily_foundation_admission import validate_permission_sidecars
    stage, catalog = _write_stage(tmp_path, plan_id='a'*64, markets=[('ES',2024)])
    shutil.copytree(stage, tmp_path/'data/active')
    src = source(tmp_path, '2024-01', rows=2)
    directory, manifest, tracker = pack(tmp_path, [src])
    comparison = compare_sources(tmp_path, directory,
        {k:manifest[k] for k in ('sources','year','market','timeframe','coverage')},
        manifest_sha256=sha256_file(directory/'manifest.json'), budget=tracker,
        compact=dict(path=src['directory']+'/observations.parquet',sha256=src['files']['observations']['sha256']))
    changes, overrides = successors(tmp_path)
    entry = deepcopy(catalog['entries'][0])
    permission = json.loads((tmp_path/entry['sidecar_path']).read_bytes())
    entry['annual_release_key'] = manifest['release_key']
    entry['parquet_path'], entry['sidecar_path'] = release.minute_paths(entry)
    entry['parquet_sha256'] = manifest['files']['observations']['sha256']
    permission['entry_binding'] = {k:v for k,v in entry.items() if k != 'sidecar_sha256'}
    permission['sidecar_id'] = sha256_json({k:v for k,v in permission.items() if k != 'sidecar_id'})
    ref, dest = publish_generated(tmp_path, directory, manifest, comparison, minute_permission=permission)
    wrapped = read_record(dest/'manifest.json')
    wrapped['implementation']['src/futures_rebuild/annual_foundation_storage.py'] = changes['src/futures_rebuild/annual_foundation_storage.py']['candidate']['sha256']
    entry['sidecar_sha256'] = save(dest/'manifest.json', {k:v for k,v in wrapped.items() if k!='id'})
    catalog['entries'] = [entry]
    catalog['catalog_sha256'] = sha256_json({k:v for k,v in catalog.items() if k!='catalog_sha256'})
    (tmp_path/'data/active/catalog.json').write_bytes(canonical_bytes(catalog)+b'\n')
    with integration.projected(tmp_path, overrides):
        got = minute.resolve(repository_root=tmp_path,market='ES',year=2024,purpose='FEATURE_GENERATION')
        assert isinstance(got, Path) and got.name == 'observations.parquet'
        assert sha256_file(got) == entry['parquet_sha256']
        assert minute.verify_view(tmp_path/'data/active') == catalog
        from futures_rebuild import daily_foundation_admission
        daily_foundation_admission.validate_permission_sidecars(tmp_path, catalog,
            {entry['sidecar_path']:entry['sidecar_sha256']})
        with pytest.raises(UnauthorizedOperation):
            minute.resolve(repository_root=tmp_path,market='ES',year=2024,purpose='SELECTION')


def recovery_binding(root, changes, overrides):
    """Construct exact prior two-fix evidence from generated certificate metadata."""
    src = ROOT/'src/futures_rebuild/hourly_source_admission.py'
    shutil.copyfile(src, root/'src/futures_rebuild/hourly_source_admission.py')
    fixes = {
        'hourly_quality_reader.py':("if timeframe == '1h':", "if timeframe == '1h' and os.path.lexists(contained(Path(root), POINTER)):"),
        'hourly_source_admission.py':('        self._unchanged()\n        if (type(market)', '        if (type(market)'),
        'daily_identity_publication.py':('Exact-predecessor recovery certification', 'Original recovery certification'),
    }
    current = {n:sha256_file(root/n) for n in changes}
    current['src/futures_rebuild/hourly_source_admission.py'] = sha256_file(src)
    original = dict(current)
    original_files = {}
    for name, (after, before) in fixes.items():
        text = (root/'src/futures_rebuild'/name).read_text()
        assert text.count(after) == 1
        path = root/'state/unpublished_evidence/generated_original'/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text.replace(after,before), newline='\n')
        original['src/futures_rebuild/'+name] = sha256_file(path)
        original_files['src/futures_rebuild/'+name] = dict(path=path.relative_to(root).as_posix(),sha256=sha256_file(path))
    cert = dict(passes=[{'count':2},{'count':2}],implementation=original,
        legacy_implementation={Path(n).name:h for n,h in current.items()})
    reference = dict(path='state/unpublished_evidence/generated/certificate.json',sha256=save(root/'state/unpublished_evidence/generated/certificate.json',cert))
    previous = dict(schema='daily_recovery_reader_binding/1',policy='EXACT_TWO_READER_FIXES_RETAIN_RECONSTRUCTION_V1',
        certificate=reference,original_implementation=original,replacement_implementation=current,
        original_files=original_files,permission_expansion=False,original_passes_sha256=sha256_json(cert['passes']))
    path = 'state/unpublished_evidence/generated/previous_binding.json'
    old_ref = dict(path=path,sha256=save(root/path,previous))
    prior = 'state/unpublished_evidence/generated/previous_pointer.json'
    prior_ref = dict(path=prior,sha256=save(root/prior,dict(schema='active_daily_recovery_reader_binding/1',manifest=old_ref)))
    record = dict(schema='daily_recovery_packaging_binding/1',certificate=reference,
        original_passes_sha256=sha256_json(cert['passes']),permission_expansion=False,changes=changes,
        predecessor_pointer=prior_ref,support_implementation={
            'src/futures_rebuild/'+n:sha256_file(root/'src/futures_rebuild'/n)
            for n in ('annual_foundation_release.py','annual_foundation_integration.py')})
    path = 'state/unpublished_evidence/generated/packaging_binding.json'
    ref = dict(path=path,sha256=save(root/path,record))
    ptr = 'state/unpublished_evidence/generated/packaging_pointer.json'
    save(root/ptr,dict(schema='active_daily_recovery_packaging_binding/1',manifest=ref))
    overrides['configs/active_daily_recovery_reader_binding.json'] = ptr
    return reference, cert, record


@pytest.mark.parametrize('tamper',[None,'patch','predecessor','passes','transform'])
def test_recovery_bridge_only_accepts_exact_packaging_changes(tmp_path, tamper):
    changes, overrides = successors(tmp_path)
    reference, cert, binding = recovery_binding(tmp_path, changes, overrides)
    if tamper == 'patch':
        with (tmp_path/overrides['src/futures_rebuild/hourly_quality_reader.py']).open('a') as stream:
            stream.write('\n# extra change\n')
    elif tamper == 'predecessor':
        with (tmp_path/changes['src/futures_rebuild/active_data_view.py']['predecessor']['path']).open('a') as stream:
            stream.write('\n# changed\n')
    elif tamper == 'passes': cert['passes'][0]['count'] = 3
    elif tamper == 'transform': cert['legacy_implementation']['active_data_view.py'] = '0'*64
    with integration.projected(tmp_path, overrides):
        if tamper is None:
            assert release.recovery_packaging(tmp_path, reference, cert)
        else:
            with pytest.raises(IntegrityError):
                release.recovery_packaging(tmp_path, reference, cert)


def test_daily_successor_uses_annual_groups_and_preserves_causal_checks(tmp_path):
    from futures_rebuild import causal_1d as daily
    from futures_rebuild import causal_timeframe_publication as pub
    from futures_rebuild import daily_quality_reader
    from tests.test_causal_1d import sample, spec, RULEBOOK, LIMITS, BASE, DAY
    changes, overrides = successors(tmp_path)
    for name in daily.implementation_binding():
        path=tmp_path/'src/futures_rebuild'/name
        if not path.exists():
            path.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(ROOT/'src/futures_rebuild'/name,path)
    catalog=tmp_path/'data/active/catalog.json'
    catalog.parent.mkdir(parents=True);catalog.write_text('{}')
    (tmp_path/'configs').mkdir(exist_ok=True)
    shutil.copyfile(ROOT/'configs/contract_economics_rules.json',tmp_path/'configs/contract_economics_rules.json')
    with integration.projected(tmp_path,overrides):
        relative='state/unpublished_evidence/generated_daily/candidate.json'
        target=contained(tmp_path,str(Path(relative).parent).replace('\\','/'))
        spec_value=spec(BASE,BASE+2*DAY)
        data=lambda:iter([sample(BASE),sample(BASE+DAY)])
        daily.build_candidate(target,spec_value,data,RULEBOOK,LIMITS)
        daily.certify_candidate(target,spec_value,data,RULEBOOK,LIMITS)
        proposal=pub.prepare_publication(tmp_path,candidates=[relative])
        # Synthetic monthly publication fixture; no authorization bypass in readers.
        from tests.test_daily_identity_publication import publication_receipt
        index=pub.publish(tmp_path,proposal,publication_receipt(tmp_path,proposal),LIMITS)
        reader=daily_quality_reader.DailyReader(tmp_path)
        request=dict(market='ES',start_ns=BASE,end_ns=BASE+DAY,as_of_ns=BASE+DAY+5*10**9,purpose='SYNTHETIC_TEST')
        expected=reader.read_window(**request)
        entry=index['entries'][0]
        value=read_record(contained(tmp_path,entry['manifest']))
        s=dict(month=entry['month'],directory=entry['directory'],start_ns=entry['coverage_start_ns'],
               end_ns=entry['coverage_end_ns'],files={n:{k:v[k] for k in ('sha256','size')} for n,v in value['seal']['files'].items()})
        tracker=budget(tmp_path,'daily_pack')
        from futures_rebuild import annual_foundation_storage
        unit=dict(market='ES',timeframe='1d',year=2024,sources=[s],coverage=[dict(month=s['month'],state='CERTIFIED')])
        directory,packing=annual_foundation_storage.pack_candidate(tmp_path,**unit,budget=tracker)
        comparison=compare_sources(tmp_path,directory,unit,manifest_sha256=sha256_file(directory/'manifest.json'),budget=tracker)
        ref,dest=publish_generated(tmp_path,directory,packing,comparison)
        wrapper=read_record(dest/'manifest.json')
        wrapper['implementation']['src/futures_rebuild/annual_foundation_storage.py']=changes['src/futures_rebuild/annual_foundation_storage.py']['candidate']['sha256']
        ref['sha256']=save(dest/'manifest.json',{k:v for k,v in wrapper.items() if k!='id'})
        entry['annual_release']=ref
        # Synthetic index with no historical publication claim.
        save(tmp_path/'data/active/timeframes.json',{k:v for k,v in index.items() if k not in ('id','publication')})
        reader=daily_quality_reader.DailyReader(tmp_path)
        assert reader.read_window(**request)==expected
        with pytest.raises(UnauthorizedOperation,match='not yet available'):
            reader.read_window(**(request|{'as_of_ns':request['as_of_ns']-1}))
        with pytest.raises(UnauthorizedOperation,match='derived'):
            reader.read_window(**(request|{'source':'minute_derived'}))
