"""Generated candidate bytes through unmodified public admission/quality methods."""
from copy import deepcopy

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild import annual_foundation_reader_preview as preview
from futures_rebuild import annual_foundation_storage as annual
from futures_rebuild.annual_foundation_pilot import compare_sources
from futures_rebuild.canonical import canonical_bytes, sha256_file, io_path
from futures_rebuild.causal_observation_parquet import FILENAMES, read_table
from futures_rebuild.causal_timeframe_storage import contained, signed
from futures_rebuild.hourly_quality_reader import HourlyReader, HOUR
from futures_rebuild.hourly_source_admission import HourlySourceReader, policy_entries
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from tests.test_annual_foundation_storage import source, budget
from tests.test_hourly_source_admission import ceiling

pytestmark = pytest.mark.current


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(signed(value))+b'\n')
    return sha256_file(path)


def setup(root):
    root = io_path(root)
    s = source(root, '2024-01')
    path = contained(root, s['directory']+'/observations.parquet')
    with pq.ParquetFile(path) as file:
        table = file.read()
    start = s['start_ns']
    for name, value in dict(source_cadence='1h', bar_start_ns=start, bar_end_ns=start+HOUR,
            available_at_ns=start+HOUR+5*10**9, decision_eligible_at_ns=start+HOUR+5*10**9).items():
        field = table.schema.field(name)
        table = table.set_column(table.schema.get_field_index(name), field, pa.array([value], type=field.type))
    pq.write_table(table, path)
    s['files']['observations'] = dict(size=path.stat().st_size, sha256=sha256_file(path))
    original = dict(permitted_uses=['LINEAGE_AUDIT'], seal=dict(files=s['files']))
    original_path = contained(root, s['directory']+'/manifest.json')
    digest = save(original_path, original)
    tracker = budget(root)
    unit = dict(market='ES', timeframe='1h', year=2024, sources=[s],
                coverage=[dict(month=s['month'],state='CERTIFIED')])
    directory, manifest = annual.pack_candidate(root, **unit, budget=tracker)
    manifest_hash = sha256_file(directory/'manifest.json')
    comparison = compare_sources(root, directory, unit, manifest_sha256=manifest_hash, budget=tracker)
    record = dict(directory=directory.relative_to(tracker.directory).as_posix(),
                  manifest_sha256=manifest_hash, comparison=comparison)
    report_path = tracker.directory/'compare.json'
    report_hash = save(report_path, dict(status='PASS_RECOVERY_PHASE', phase='compare', units=[record]))
    candidates = {('1h','ES',2024):dict(directory=directory.relative_to(root).as_posix(),
        manifest_sha256=manifest_hash, comparison=dict(path=report_path.relative_to(root).as_posix(),sha256=report_hash))}
    entry = dict(market='ES',timeframe='1h',month=s['month'],disposition='CERTIFIED',
        directory=s['directory'],manifest=s['directory']+'/manifest.json',manifest_sha256=digest,
        coverage_start_ns=start,coverage_end_ns=s['end_ns'])
    original_rows = read_table(path, name='observations')
    quality = object.__new__(HourlyReader)
    quality.root=root; quality.bindings={entry['manifest']:digest}
    quality.entries={('ES',s['month']):entry}; quality.index={'entries':[entry]}
    quality.census={'input_sha256':{s['directory']+'/observations.parquet':s['files']['observations']['sha256']}}
    quality.quality_groups={'ES:'+s['month']:pa.Table.from_pylist([
        dict(row_id=bytes.fromhex(r['row_id']),market='ES',start_ns=r['bar_start_ns'],minute_count=60,
             quality_disposition='CORROBORATED_BY_FULL_MINUTES') for r in original_rows])}
    quality.derived=[]
    reader=object.__new__(HourlySourceReader)
    reader.root=root; reader.bindings=dict(quality.bindings); reader.quality_reader=quality
    reader._entries={(e['market'],e['month']):e for e in policy_entries(quality.index,{'entries':[ceiling(2024)]})}
    request=dict(market='ES',start_ns=start,end_ns=start+HOUR,as_of_ns=start+HOUR+5*10**9,purpose='FEATURE_GENERATION')
    return reader,candidates,request,directory


def test_annual_bytes_preserve_public_rows_and_do_not_patch_original(tmp_path):
    reader, candidates, request, directory = setup(tmp_path)
    original = reader.quality_reader._native_month
    expected = reader.read_window(**request)
    assert preview.read_window_preview(reader,candidates=candidates,request=request) == expected
    assert reader.quality_reader._native_month == original
    assert preview.read_window_preview(reader,candidates=candidates,request=request) == expected
    with pytest.raises(UnauthorizedOperation,match='not yet available'):
        preview.read_window_preview(reader,candidates=candidates,request=request|{'as_of_ns':request['as_of_ns']-1})
    with pytest.raises(UnauthorizedOperation,match='coverage is absent'):
        preview.read_window_preview(reader,candidates=candidates,request=request|{'end_ns':request['end_ns']+HOUR})
    with pytest.raises(IntegrityError,match='native only'):
        preview.read_window_preview(reader,candidates=candidates,request=request|{'source':'minute_derived'})


def test_denied_purpose_precedes_candidate_access(tmp_path, monkeypatch):
    reader,candidates,request,_ = setup(tmp_path)
    def forbidden(*args, **kwargs):
        raise AssertionError('Candidate was accessed despite denied purpose')
    monkeypatch.setattr(preview,'read_record',forbidden)
    with pytest.raises(UnauthorizedOperation,match='Purpose denied'):
        preview.read_window_preview(reader,candidates=candidates,request=request|{'purpose':'SELECTION'})


@pytest.mark.parametrize('kind',['candidate','source_manifest','comparison','missing_binding','excluded'])
def test_preview_failures_preserve_original_reader(tmp_path,kind):
    reader,candidates,request,directory=setup(tmp_path)
    original=reader.quality_reader._native_month
    if kind=='excluded':
        group=reader.quality_reader.quality_groups['ES:2024-01'].to_pylist()
        group[0]['quality_disposition']='QUARANTINE_UNRESOLVED'
        reader.quality_reader.quality_groups['ES:2024-01']=pa.Table.from_pylist(group)
    else:
        path={'candidate':directory/'quality.parquet',
              'source_manifest':contained(tmp_path,reader.quality_reader.entries[('ES','2024-01')]['manifest']),
              'comparison':directory.parents[3]/'compare.json',
              'missing_binding':contained(tmp_path,reader.quality_reader.entries[('ES','2024-01')]['manifest'])}[kind]
        if kind=='missing_binding': path.unlink()  # Generated test fixture only.
        else:
            with path.open('ab') as f: f.write(b'tampered')
    with pytest.raises((IntegrityError,UnauthorizedOperation,OSError)):
        preview.read_window_preview(reader,candidates=candidates,request=request)
    assert reader.quality_reader._native_month == original


def test_daily_decoder_substitution_preserves_recovered_timing(tmp_path):
    from tests.test_daily_identity_publication import fixture, campaign, BASE, DAY
    from futures_rebuild.daily_quality_reader import DailyReader
    from futures_rebuild.causal_observation_parquet import read_bundle
    _,index,overrides,entry,_=fixture(tmp_path)
    with campaign.projected_successor(tmp_path,overrides):
        reader=DailyReader(tmp_path)
        bundle=read_bundle(contained(tmp_path,entry['directory']))
        calls=[]
        def decoder(path):
            assert path == contained(tmp_path,entry['directory'])
            calls.append(path)
            return deepcopy(bundle)
        method=preview._replace_decoder(reader.read_window,'read_bundle',decoder)
        assert method.__func__.__code__ is reader.read_window.__func__.__code__
        args=dict(market='ES',start_ns=BASE,end_ns=BASE+DAY,as_of_ns=BASE+DAY+5*10**9,purpose='SYNTHETIC_TEST')
        assert method(**args)==reader.read_window(**args)
        assert len(calls)==1
        with pytest.raises(UnauthorizedOperation,match='not yet available'):
            method(**(args|{'as_of_ns':args['as_of_ns']-1}))
