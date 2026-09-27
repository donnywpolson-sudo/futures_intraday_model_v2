"""Generated inputs only; no historical payload or active control access."""
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from futures_rebuild import annual_foundation_storage as annual
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.causal_observation_parquet import (
    FILENAMES, SCHEMAS, _encode_group, _encode_physical, read_bundle,
)
from futures_rebuild.causal_timeframe_storage import Limits, signed
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from tests.test_causal_observation_parquet import _tables

pytestmark = pytest.mark.current


def source(root, month, cadence=True, groups=1, rows=1, tracker=None):
    directory = root / 'data/causally_gated_normalized/synthetic' / month
    directory.mkdir(parents=True)
    left, right = annual._period(month)
    tables = _tables()
    for name, filename in FILENAMES.items():
        output = tracker.open(directory / filename) if tracker else str(directory / filename)
        with pq.ParquetWriter(output, SCHEMAS[name]) as writer:
            for i in range(groups):
                if name == 'cadence' and not cadence:
                    continue
                physical = [_encode_physical(name, r) for r in
                            _encode_group(name, tables[name], tables['observations'])]
                if name == 'observations':
                    for field in ('bar_start_ns', 'bar_end_ns', 'source_timestamp_ns',
                                  'available_at_ns', 'decision_eligible_at_ns'):
                        physical[0][field] += left + i * rows * 60 * 10**9
                if name == 'missingness':
                    for field in ('interval_start_ns', 'interval_end_ns'):
                        physical[0][field] += left + i * rows * 60 * 10**9
                table = pa.Table.from_pylist(physical, schema=SCHEMAS[name]).take(pa.array([0]*rows))
                for field in ('bar_start_ns','bar_end_ns','source_timestamp_ns','available_at_ns',
                              'decision_eligible_at_ns','interval_start_ns','interval_end_ns'):
                    if field in table.column_names:
                        table = table.set_column(table.schema.get_field_index(field), table.schema.field(field),
                             pc.add(table[field], pa.array([j*60*10**9 for j in range(rows)], type=pa.int64())))
                if 'observation_ordinal' in table.column_names:
                    field = table.schema.field('observation_ordinal')
                    table = table.set_column(table.schema.get_field_index(field.name), field,
                                              pa.array(range(rows), type=field.type))
                writer.write_table(table)
        if tracker:
            output.close()
    return dict(month=month, directory=directory.relative_to(root).as_posix(),
                start_ns=left, end_ns=right, files={name: dict(size=(directory/f).stat().st_size,
                    sha256=sha256_file(directory/f)) for name, f in FILENAMES.items()})


def budget(root, attempt='test', writes=64*2**20):
    return annual.CandidateBudget(root, attempt, Limits(writes, 5*2**30, 180,
                                  minimum_free_bytes=1, maximum_group_bytes=64*2**20))


def pack(root, sources, *, tracker=None):
    tracker = tracker or budget(root)
    coverage = [dict(month=s['month'], state='CERTIFIED') for s in sources]
    coverage.append(dict(month='2024-03', state='UNAVAILABLE'))
    directory, manifest = annual.pack_candidate(root, market='ES', timeframe='1m', year=2024,
                                sources=sources, coverage=coverage, budget=tracker)
    return directory, manifest, tracker


@pytest.mark.parametrize('cadences', [(True, True), (False, False), (True, False), (False, True)])
def test_exact_five_table_roundtrip_and_counted_footers(tmp_path, cadences):
    sources = [source(tmp_path, f'2024-0{i+1}', cadence=c, groups=2) for i,c in enumerate(cadences)]
    before = {p: sha256_file(p) for p in tmp_path.rglob('*.parquet')}
    directory, manifest, tracker = pack(tmp_path, sources)
    for s in sources:
        groups = list(annual.read_candidate_month(directory,
                      manifest_sha256=sha256_file(directory/'manifest.json'), month=s['month']))
        assert {n: [r for g in groups for r in g[n]] for n in FILENAMES} == read_bundle(tmp_path/s['directory'])
    assert tracker.written == sum(p.stat().st_size for p in directory.iterdir())
    assert tracker.files == 6
    assert all(sha256_file(p) == h for p,h in before.items())
    assert manifest['status'] == 'PREPARED_NOT_ADMITTED'
    with pytest.raises(UnauthorizedOperation):
        list(annual.read_candidate_month(directory, manifest_sha256=sha256_file(directory/'manifest.json'), month='2024-03'))


def test_only_requested_month_groups_are_decoded(tmp_path, monkeypatch):
    directory, _, _ = pack(tmp_path, [source(tmp_path, '2024-01'), source(tmp_path, '2024-02')])
    calls = []
    original = annual._group
    def recorded(file, index, limit):
        calls.append(index)
        return original(file, index, limit)
    monkeypatch.setattr(annual, '_group', recorded)
    list(annual.read_candidate_month(directory, manifest_sha256=sha256_file(directory/'manifest.json'), month='2024-02'))
    assert calls == [1]*5


def test_writes_reject_escape_existing_and_source_alias(tmp_path):
    s = source(tmp_path, '2024-01')
    original = tmp_path/s['directory']/'observations.parquet'
    digest = sha256_file(original)
    tracker = budget(tmp_path)
    for destination in (original, tracker.directory/'../escape', tmp_path/'data/dbn/new.dbn'):
        with pytest.raises(IntegrityError):
            tracker.open(destination)
    linked = tracker.directory/'alias'
    os.link(original, linked)
    with pytest.raises(FileExistsError):
        tracker.open(linked)
    with pytest.raises(FileExistsError):
        budget(tmp_path)
    with pytest.raises(IntegrityError):
        budget(tmp_path, '../escape')
    assert sha256_file(original, reject_hardlinks=False) == digest and linked.exists()


def test_tampering_sources_outputs_and_group_maps(tmp_path):
    s = source(tmp_path, '2024-01')
    changed = {**s, 'files': {**s['files'], 'observations': {**s['files']['observations'], 'sha256': '0'*64}}}
    with pytest.raises(IntegrityError, match='Source binding'):
        pack(tmp_path, [changed], tracker=budget(tmp_path, 'bad_source'))
    directory, manifest, _ = pack(tmp_path, [s])
    manifest['groups'][0]['tables']['quality']['group'] = None
    malformed = signed({k:v for k,v in manifest.items() if k != 'id'})
    (directory/'manifest.json').write_bytes(canonical_bytes(malformed))
    with pytest.raises(IntegrityError, match='group map'):
        list(annual.read_candidate_month(directory, manifest_sha256=sha256_file(directory/'manifest.json'), month='2024-01'))
    directory2, _, _ = pack(tmp_path, [s], tracker=budget(tmp_path, 'tampered_output'))
    with (directory2/'quality.parquet').open('ab') as handle:
        handle.write(b'tampered')
    with pytest.raises(IntegrityError, match='table changed'):
        list(annual.read_candidate_month(directory2, manifest_sha256=sha256_file(directory2/'manifest.json'), month='2024-01'))


def test_cutoff_rejected_before_hashing(tmp_path, monkeypatch):
    tracker = budget(tmp_path)
    def forbidden(path):
        raise AssertionError('protected file opened')
    monkeypatch.setattr(tracker, 'hash', forbidden)
    left, right = annual._period('2025-07')
    with pytest.raises(IntegrityError, match='development month'):
        annual._verify_sources(tmp_path, [dict(month='2025-07', start_ns=left, end_ns=right)], 2025, tracker)


def test_failure_retains_exclusively_created_output(tmp_path):
    tracker = budget(tmp_path, writes=2*2**20+4)
    with tracker.open(tracker.directory/'partial') as handle:
        handle.write(b'1234')
        with pytest.raises(IntegrityError, match='resource limit'):
            handle.write(b'5')
    assert (tracker.directory/'partial').read_bytes() == b'1234'
    assert tracker.written == 4


def test_inactive_index_preserves_membership_and_predecessor():
    original = {'id':'a'*64, 'entries': [dict(timeframe='1h', market='ES', month='2024-01', disposition='CERTIFIED'),
                dict(timeframe='1h', market='ES', month='2024-02', disposition='UNAVAILABLE')]}
    candidates = {('1h','ES',2024):dict(months=['2024-01'], manifest_sha256='b'*64)}
    prepared = annual.prepared_month_index(original, candidates)
    assert prepared['predecessor_id'] == original['id']
    assert prepared['entries'][1] == original['entries'][1]
    assert 'annual_candidate' not in original['entries'][0]
    from futures_rebuild.causal_timeframe_publication import validate_index
    with pytest.raises(IntegrityError, match='staging envelopes'):
        validate_index(Path('.'), prepared)
    with pytest.raises(IntegrityError, match='membership'):
        annual.prepared_month_index(original, {('1h','ES',2024):dict(months=['2024-01', '2024-02'])})


def test_single_final_census_hashes_annual_files_once_per_boundary(tmp_path, monkeypatch):
    directory, _, _ = pack(tmp_path, [source(tmp_path, '2024-01', groups=2), source(tmp_path, '2024-02')])
    calls = []
    original = annual.sha256_file
    def counted(path):
        calls.append(Path(path).name)
        return original(path)
    digest = original(directory/'manifest.json')
    monkeypatch.setattr(annual, 'sha256_file', counted)
    result = annual.validate_candidate(directory, manifest_sha256=digest)
    assert result['exhaustive_censuses'] == 1
    assert result['counts']['2024-01']['observations'] == 2
    assert result['counts']['2024-02']['observations'] == 1
    assert all(calls.count(f) == 2 for f in FILENAMES.values())


@pytest.mark.parametrize('kind', ['memory','deadline','free_space'])
def test_separate_resource_failure_diagnostics(tmp_path, monkeypatch, kind):
    tracker=budget(tmp_path)
    if kind=='memory':
        monkeypatch.setattr(annual,'memory_peak',lambda: 6*2**30)
        message='Memory resource limit'
    elif kind=='deadline':
        tracker.started-=1000
        message='Deadline resource limit'
    else:
        from collections import namedtuple
        monkeypatch.setattr(annual.shutil,'disk_usage',lambda _:namedtuple('Usage','free')(0))
        message='Free-space resource limit'
    with pytest.raises(IntegrityError,match=message):
        tracker.open(tracker.directory/'must_not_exist')
    assert not (tracker.directory/'must_not_exist').exists()
