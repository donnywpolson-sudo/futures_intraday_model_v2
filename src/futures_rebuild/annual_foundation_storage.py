"""Inactive annual packaging only. No publication, source admission or raw DBN I/O.

Physical groups retain their ordinals. An explicit map represents absent evidence;
the active monthly codec and all public readers remain unchanged.
"""
from contextlib import ExitStack
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import re
import shutil
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .canonical import io_path, sha256_file, sha256_json
from .causal_observation_parquet import FILENAMES, SCHEMAS, _decode_group, _decode_physical
from .causal_timeframe_storage import Budget, contained, read_record, signed, memory_peak
from .errors import IntegrityError, UnauthorizedOperation

BASE = 'state/unpublished_evidence/annual_foundation_migration_v1'
SCHEMA = 'inactive_annual_foundation/1'
CUTOFF = 1752444000 * 10**9


def require(ok, message):
    if not ok:
        raise IntegrityError(message)


class CandidateBudget(Budget):
    """Exclusive, counted writes confined to one newly created attempt."""
    def __init__(self, root, attempt, limits):
        require(bool(re.fullmatch(r'[a-z0-9_]{1,64}', attempt)), 'Invalid attempt name')
        self.root = Path(root).resolve()
        directory = contained(self.root, BASE + '/' + attempt)
        directory.mkdir(parents=True, exist_ok=False)
        super().__init__(directory, limits)
        self.terminal_reserve = 2 * 2**20

    def open(self, path):
        path = io_path(Path(path))
        require(path.is_relative_to(self.directory), 'Write outside candidate attempt')
        relative = path.relative_to(self.directory).as_posix()
        path = contained(self.directory, relative)
        # Budget.open uses xb, rejects linklike ancestors, and charges every write.
        return super().open(path)

    def check(self, reserve=0):
        require(type(reserve) is int and reserve >= 0, 'Invalid reservation')
        self.peak_memory = max(self.peak_memory, memory_peak())
        used = self.written + reserve + self.terminal_reserve
        require(used <= self.limits.maximum_written_bytes,
                f'Write resource limit: {used} > {self.limits.maximum_written_bytes}')
        if not self.finalizing:
            require(self.peak_memory <= self.limits.maximum_memory_bytes,
                    f'Memory resource limit: {self.peak_memory} > {self.limits.maximum_memory_bytes}')
            elapsed = time.monotonic()-self.started
            require(elapsed <= self.limits.maximum_seconds,
                    f'Deadline resource limit: {elapsed} > {self.limits.maximum_seconds}')
        free = shutil.disk_usage(self.directory).free
        needed = self.limits.minimum_free_bytes + reserve + self.terminal_reserve
        require(free >= needed, f'Free-space resource limit: {free} < {needed}')


def _period(month):
    require(bool(re.fullmatch(r'20\d\d-(0[1-9]|1[0-2])', month)), 'Invalid month')
    year, number = map(int, month.split('-'))
    left = int(datetime(year, number, 1, tzinfo=timezone.utc).timestamp()) * 10**9
    right = int(datetime(year + (number == 12), number % 12 + 1, 1,
                         tzinfo=timezone.utc).timestamp()) * 10**9
    return left, right


def _verify_sources(root, sources, year, budget):
    seen = set()
    for source in sources:
        month = source['month']
        left, right = _period(month)
        require(month not in seen and month.startswith(str(year) + '-'), 'Duplicate/wrong year source')
        seen.add(month)
        require(left <= source['start_ns'] < source['end_ns'] <= min(right, CUTOFF),
                'Physical input exceeds declared development month')
        require(source['directory'].startswith('data/causally_gated_normalized/'),
                'Only declared normalized monthly inputs are accepted; no raw DBN')
        require(set(source['files']) == set(FILENAMES), 'All five input bindings required')
        for name, filename in FILENAMES.items():
            info = source['files'][name]
            path = contained(root, source['directory'] + '/' + filename)
            require(path.stat().st_size == info['size'] and budget.hash(path) == info['sha256'],
                    'Source binding changed: ' + str(path))
    require([s['month'] for s in sources] == sorted(seen), 'Source order differs')


def _group(file, index, limit):
    meta = file.metadata.row_group(index)
    require(meta.total_byte_size <= limit and meta.num_rows <= 250_000,
            'Oversized physical group refused before decoding')
    table = file.read_row_group(index)
    require(table.nbytes <= limit, 'Decoded group exceeds budget')
    return table


def _digest(table):
    digest = sha256(table.schema.serialize().to_pybytes())
    for batch in table.combine_chunks().to_batches():
        digest.update(batch.serialize().to_pybytes())
    return digest.hexdigest()


def pack_candidate(root, *, market, timeframe, year, sources, coverage, budget):
    """Approved-task primitive; caller must authorize physical input reads.

    The resulting manifest is explicitly inactive and is not publication proof.
    No CLI dispatches this function and it never modifies active controls.
    """
    root = Path(root).resolve()
    require(root == budget.root and bool(re.fullmatch(r'[A-Z0-9]{1,8}', market)), 'Invalid root/market')
    require(timeframe in ('1m', '1h', '1d') and type(year) is int and 2010 <= year <= 2025,
            'Unsupported annual source')
    require(sources, 'An unavailable year cannot create observations')
    _verify_sources(root, sources, year, budget)
    require(len(coverage) == len({c['month'] for c in coverage}), 'Duplicate coverage')
    for c in coverage:
        _period(c['month'])
        require(c['month'].startswith(str(year) + '-') and c['state'] in ('CERTIFIED', 'UNAVAILABLE'),
                'Invalid coverage state')
    require({c['month'] for c in coverage if c['state'] == 'CERTIFIED'} == {s['month'] for s in sources},
            'Coverage/source membership differs')
    identity = dict(market=market, timeframe=timeframe, year=year, sources=sources,
                    coverage=coverage, implementation_sha256=sha256_file(Path(__file__)))
    release_key = sha256_json(identity)
    relative = f'ohlcv_{timeframe}/{market}/{year}/{release_key}'
    directory = contained(budget.directory, relative)
    directory.mkdir(parents=True, exist_ok=False)
    maps = []
    counts = {name: 0 for name in FILENAMES}
    next_group = {name: 0 for name in FILENAMES}
    # No cleanup-on-failure: exclusively owned incomplete files remain diagnostic.
    with ExitStack() as stack:
        streams = {name: stack.enter_context(budget.open(directory / filename))
                   for name, filename in FILENAMES.items()}
        writers = {name: stack.enter_context(pq.ParquetWriter(streams[name], SCHEMAS[name],
                    compression='zstd', compression_level=9, use_dictionary=True,
                    write_statistics=True, version='2.6', data_page_version='2.0'))
                   for name in FILENAMES}
        for source in sources:
            with ExitStack() as inputs:
                files = {name: inputs.enter_context(pq.ParquetFile(contained(root,
                    source['directory'] + '/' + filename))) for name, filename in FILENAMES.items()}
                n = files['observations'].metadata.num_row_groups
                require(n > 0, 'No observation groups')
                for name, file in files.items():
                    require(file.schema_arrow.equals(SCHEMAS[name], check_metadata=True), 'Schema differs')
                    allowed = (0, n) if name == 'cadence' else (n,)
                    require(file.metadata.num_row_groups in allowed, 'Source group alignment differs')
                for source_group in range(n):
                    budget.check()
                    mapping = dict(month=source['month'], source_group=source_group, tables={})
                    for name, file in files.items():
                        table = (_group(file, source_group, budget.limits.maximum_group_bytes)
                                 if file.metadata.num_row_groups else pa.Table.from_batches([], SCHEMAS[name]))
                        if name == 'observations':
                            require(table.num_rows > 0, 'Empty observation group')
                            left = pc.min(table['bar_start_ns']).as_py()
                            right = pc.max(table['bar_end_ns']).as_py()
                            require(source['start_ns'] <= left < right <= source['end_ns'],
                                    'Observation outside declared physical input')
                            require(pc.all(pc.equal(table['market'], market)).as_py()
                                    and pc.all(pc.equal(table['source_cadence'], timeframe)).as_py(),
                                    'Market/timeframe differs')
                        position = next_group[name] if table.num_rows else None
                        mapping['tables'][name] = dict(group=position, rows=table.num_rows,
                                                       digest=_digest(table))
                        if position is not None:
                            budget.check(table.nbytes * 2 + 2**20)
                            writers[name].write_table(table, row_group_size=table.num_rows)
                            next_group[name] += 1
                        counts[name] += table.num_rows
                        del table
                    maps.append(mapping)
        for writer in writers.values():
            writer.close()
        import os
        for stream in streams.values():
            stream.flush()
            os.fsync(stream.fileno())
    _verify_sources(root, sources, year, budget)
    files = {name: dict(filename=filename, size=(directory / filename).stat().st_size,
                       sha256=budget.hash(directory / filename), rows=counts[name],
                       row_groups=next_group[name]) for name, filename in FILENAMES.items()}
    manifest = signed(dict(schema=SCHEMA, status='PREPARED_NOT_ADMITTED', source='native',
                           **identity, release_key=release_key, groups=maps, files=files))
    budget.save(directory / 'manifest.json', manifest)
    return directory, manifest


def _read_candidate(directory, *, manifest_sha256, months, maximum_group_bytes, _packing=None):
    """Consume fully before accepting results; final hashes verify the read boundary."""
    directory = io_path(Path(directory))
    manifest_path = directory / 'manifest.json'
    require(sha256_file(manifest_path) == manifest_sha256, 'Candidate manifest changed')
    manifest = read_record(manifest_path)
    if _packing is not None:
        require(manifest.get("packing") == _packing, "Annual embedded packing differs")
        manifest = _packing
    require(manifest['schema'] == SCHEMA and manifest['status'] == 'PREPARED_NOT_ADMITTED',
            'Not an inactive annual candidate')
    require(set(manifest['files']) == set(FILENAMES), 'Incomplete candidate tables')
    keys = [(g['month'], g['source_group']) for g in manifest['groups']]
    require(keys == sorted(set(keys)), 'Duplicate or unordered source group')
    for source in manifest['sources']:
        groups = [g['source_group'] for g in manifest['groups'] if g['month'] == source['month']]
        require(groups == list(range(len(groups))) and all(type(g) is int for g in groups),
                'Source group map is incomplete')
    require({g['month'] for g in manifest['groups']} ==
            {c['month'] for c in manifest['coverage'] if c['state'] == 'CERTIFIED'},
            'Group coverage differs')
    for name, info in manifest['files'].items():
        refs = [g['tables'][name] for g in manifest['groups']]
        require(all(type(r['rows']) is int and r['rows'] >= 0
                    and (r['group'] is None or type(r['group']) is int) for r in refs),
                'Invalid evidence group ordinal')
        require([r['group'] for r in refs if r['group'] is not None] == list(range(info['row_groups']))
                and sum(r['rows'] for r in refs) == info['rows']
                and all((r['group'] is None) == (r['rows'] == 0) for r in refs),
                'Evidence group map is incomplete or ambiguous')
    for month in months:
        states = [c['state'] for c in manifest['coverage'] if c['month'] == month]
        if states != ['CERTIFIED']:
            raise UnauthorizedOperation('Requested month is unavailable')
    selected = [g for g in manifest['groups'] if g['month'] in months]
    require(selected, 'Missing annual month map')
    with ExitStack() as stack:
        files = {}
        for name, info in manifest['files'].items():
            require(info['filename'] == FILENAMES[name], 'Candidate table path differs')
            path = contained(directory, info['filename'])
            require(path.stat().st_size == info['size'] and sha256_file(path) == info['sha256'],
                    'Candidate table changed')
            files[name] = stack.enter_context(pq.ParquetFile(path))
            require(files[name].schema_arrow.equals(SCHEMAS[name], check_metadata=True)
                    and files[name].metadata.num_rows == info['rows']
                    and files[name].metadata.num_row_groups == info['row_groups'], 'Candidate footer differs')
        require(set(files) == set(FILENAMES), 'Incomplete candidate tables')
        for mapping in selected:
            logical = {}
            for name in FILENAMES:
                ref = mapping['tables'][name]
                table = (pa.Table.from_batches([], SCHEMAS[name]) if ref['group'] is None
                         else _group(files[name], ref['group'], maximum_group_bytes))
                require(table.num_rows == ref['rows'] and _digest(table) == ref['digest'],
                        'Candidate group differs')
                physical = [_decode_physical(name, row) for row in table.to_pylist()]
                logical[name] = _decode_group(name, physical, logical.get('observations', ()))
            yield mapping['month'], logical
        for name, info in manifest['files'].items():
            require(sha256_file(contained(directory, info['filename'])) == info['sha256'],
                    'Candidate changed during read')
        require(sha256_file(manifest_path) == manifest_sha256, 'Candidate manifest changed during read')


def read_candidate_month(directory, *, manifest_sha256, month, maximum_group_bytes=64*2**20):
    """Bounded preparation reader, NOT a public source-permission reader."""
    for _, logical in _read_candidate(directory, manifest_sha256=manifest_sha256,
                                     months=[month], maximum_group_bytes=maximum_group_bytes):
        yield logical


def validate_candidate(directory, *, manifest_sha256, maximum_group_bytes=64*2**20):
    """One exhaustive readback, hashing each annual file once at each boundary."""
    manifest_path = io_path(Path(directory)) / 'manifest.json'
    require(sha256_file(manifest_path) == manifest_sha256, 'Candidate manifest changed')
    manifest = read_record(manifest_path)
    months = [s['month'] for s in manifest['sources']]
    counts = {m: {n: 0 for n in FILENAMES} for m in months}
    for month, logical in _read_candidate(directory, manifest_sha256=manifest_sha256,
                                         months=months, maximum_group_bytes=maximum_group_bytes):
        for name, rows in logical.items():
            counts[month][name] += len(rows)
    return dict(status='PASS_INACTIVE_PACKAGING_READBACK', exhaustive_censuses=1,
                manifest_sha256=manifest_sha256, counts=counts,
                source_permissions_validated=False)


def prepared_month_index(original, candidates):
    """Retain logical month keys; normal publication readers reject this envelope."""
    entries = []
    used = set()
    for entry in original['entries']:
        key = (entry['timeframe'], entry['market'], int(entry['month'][:4]))
        if key in candidates and entry['disposition'] == 'CERTIFIED':
            reference = candidates[key]
            require(entry['month'] in reference['months'], 'Candidate omits selected month')
            entries.append({**entry, 'annual_candidate': reference})
            used.add(key)
        else:
            entries.append(dict(entry))
    require(used == set(candidates), 'Candidate outside existing selected population')
    for key, reference in candidates.items():
        expected = {e['month'] for e in original['entries'] if
                    (e['timeframe'], e['market'], int(e['month'][:4])) == key
                    and e['disposition'] == 'CERTIFIED'}
        require(len(reference['months']) == len(expected) and set(reference['months']) == expected,
                'Candidate changes selected month membership')
    return signed(dict(schema='inactive_annual_index/1', status='PREPARED_NOT_ADMITTED',
                       predecessor_id=original['id'], entries=entries))
