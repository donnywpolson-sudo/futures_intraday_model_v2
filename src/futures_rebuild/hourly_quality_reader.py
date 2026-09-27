"""Quality-aware hourly windows; explicit preview or approved active pointer."""
from pathlib import Path
import json
import os

import pyarrow.compute as pc
import pyarrow.parquet as pq

from .canonical import sha256_file, sha256_json
from .causal_observation_parquet import read_table
from .causal_timeframe_storage import contained, read_record
from .errors import ContractError, IntegrityError, UnauthorizedOperation

POINTER = 'configs/active_hourly_quality_reader.json'
HOUR = 3_600_000_000_000
CUTOFF = 1752444000 * 10**9
NATIVE_ALLOWED = frozenset({'CORROBORATED_BY_FULL_MINUTES',
                          'CORROBORATED_BY_SPARSE_MINUTE_AGGREGATE',
                          'CORROBORATED_BY_SECONDS_MINUTE_REFERENCE_EXCEPTION'})
IDENTITY = ('market', 'publisher_id', 'instrument_id', 'actual_contract',
            'currency', 'min_price_increment_nano', 'multiplier_nano')


def require_window_reader(root, timeframe):
    """Path-only hourly access cannot enforce the activated row policy."""
    if timeframe == '1h':
        raise UnauthorizedOperation('Hourly quality policy requires HourlyReader.read_window; raw partition access refused')


def validate_window(rows, *, market, start_ns, end_ns, as_of_ns, source):
    if source not in ('native', 'minute_derived'):
        raise ContractError('Choose native or minute_derived explicitly')
    if (any(type(v) is not int for v in (start_ns, end_ns, as_of_ns))
            or not 0 < start_ns < end_ns <= CUTOFF or start_ns % HOUR or end_ns % HOUR):
        raise ContractError('Window must be aligned, half-open, and within development coverage')
    if ([r['bar_start_ns'] for r in rows] != list(range(start_ns, end_ns, HOUR))
            or any(r['market'] != market or r['bar_end_ns'] != r['bar_start_ns'] + HOUR for r in rows)):
        raise UnauthorizedOperation('Required hourly coverage is absent, duplicated, or out of order')
    for row in rows:
        if any(row[k] != rows[0][k] for k in IDENTITY):
            raise UnauthorizedOperation('Window crosses a contract or unit change')
        available = row['available_at_ns']
        eligible = row['decision_eligible_at_ns']
        if available < row['bar_end_ns'] + 5_000_000_000 or eligible < available:
            raise IntegrityError('Hourly availability violates the declared delay')
        if eligible > as_of_ns:
            raise UnauthorizedOperation('Hourly bar is not yet available at the requested decision time')
        if source == 'native':
            if row['origin'] != 'NATIVE_1H' or row['quality_disposition'] not in NATIVE_ALLOWED:
                raise UnauthorizedOperation('Unresolved native hourly quality')
        elif (row['origin'] != 'DERIVED_1H_FROM_RETAINED_1M'
              or row['quality_disposition'] != 'CANDIDATE_OBSERVED_MINUTES_ONLY'):
            raise UnauthorizedOperation('Unresolved derived hourly quality')
    return rows


class HourlyReader:
    """Audit-only windows. A preview path never grants publication or research use.

    Each window is dense and uses one source and contract. Sparse observed-minute
    aggregates retain their coverage labels; no missing bar is filled or skipped.
    """

    def __init__(self, repository_root, *, preview_manifest=None):
        self.root = Path(repository_root).resolve()
        self.bindings = {}
        if preview_manifest is None:
            pointer = self._record(POINTER)
            if pointer.get('schema') != 'active_hourly_quality_reader/1':
                raise IntegrityError('Unsupported hourly reader pointer')
            manifest_path = pointer['manifest']['path']
            self._bind(manifest_path, pointer['manifest']['sha256'])
        else:
            manifest_path = preview_manifest
        manifest = self._record(manifest_path)
        if (manifest.get('schema') != 'hourly_quality_reader/1'
                or manifest.get('permitted_uses') != ['LINEAGE_AUDIT']
                or manifest.get('policy') != 'NATIVE_DEFAULT_EXPLICIT_MINUTE_DERIVED_DENSE_SINGLE_CONTRACT'):
            raise UnauthorizedOperation('Hourly reader policy differs')
        self.manifest = manifest
        for relative, digest in manifest['implementation'].items():
            self._bind(relative, digest)
        evidence = manifest['evidence']
        self.census = self._record(evidence['census']['path'], evidence['census']['sha256'])
        quality = self._record(evidence['quality_manifest']['path'], evidence['quality_manifest']['sha256'])
        if quality['census_sha256'] != evidence['census']['sha256']:
            raise IntegrityError('Quality evidence refers to another census')
        quality_path = self._bind(evidence['quality']['path'], quality['quality_sha256'])
        self.index = self._record('data/causally_gated_normalized/_control/timeframes.json', self.census['input_sha256']['data/causally_gated_normalized/_control/timeframes.json'])
        self._bind('data/causally_gated_normalized/_control/catalog.json', self.census['input_sha256']['data/causally_gated_normalized/_control/catalog.json'])
        from .causal_timeframe_publication import validate_index
        validate_index(self.root, self.index)
        if 'publication' not in self.index:
            raise UnauthorizedOperation('Native index lacks publication evidence')
        self.entries = {(e['market'], e['month']): e for e in self.index['entries'] if e['timeframe'] == '1h'}
        self.quality = pq.read_table(quality_path, columns=['row_id', 'market', 'month', 'start_ns',
                                                         'minute_count', 'quality_disposition'])
        if self.quality.num_rows != quality['rows']:
            raise IntegrityError('Quality row count differs')
        # The existing quality file is ordered by market/month; keep Arrow slices,
        # avoiding a multi-million-entry Python dictionary or repeated full scans.
        groups = pc.run_end_encode(pc.binary_join_element_wise(self.quality['market'], self.quality['month'], ':').combine_chunks())
        self.quality_groups = {}
        start = 0
        for key, end in zip(groups.values.to_pylist(), groups.run_ends.to_pylist(), strict=True):
            if key in self.quality_groups:
                raise IntegrityError('Quality partitions are not contiguous')
            self.quality_groups[key] = self.quality.slice(start, end - start)
            start = end
        derived_result = json.loads(self._bind(evidence['derived_result']['path'], evidence['derived_result']['sha256']).read_bytes())
        derived_path = self._bind(evidence['derived']['path'], derived_result['candidate_sha256'])
        self.derived = pq.read_table(derived_path).to_pylist()
        if len(self.derived) != derived_result['rows']:
            raise IntegrityError('Derived row count differs')
        for row in self.derived:
            if (row['id'] != sha256_json({k: v for k, v in row.items() if k != 'id'})
                    or row['market'] != 'ZQ' or row['market_coverage'] != 'NOT_ESTABLISHED'
                    or row['disposition'] != 'CANDIDATE_OBSERVED_MINUTES_ONLY'):
                raise IntegrityError('Derived candidate identity or coverage differs')
        if len({r['bar_start_ns'] for r in self.derived}) != len(self.derived):
            raise IntegrityError('Duplicate derived hour')
        self._unchanged()

    def _bind(self, relative, expected=None):
        path = contained(self.root, relative)
        digest = sha256_file(path)
        if expected is not None and digest != expected:
            raise IntegrityError('Hourly reader input changed: ' + relative)
        self.bindings[relative] = digest
        return path

    def _record(self, relative, expected=None):
        return read_record(self._bind(relative, expected))

    def _unchanged(self):
        from .annual_foundation_independence import verify_reader
        verify_reader(self.root, self.bindings)
        for relative, digest in self.bindings.items():
            if sha256_file(contained(self.root, relative)) != digest:
                raise IntegrityError('Hourly reader binding changed during use: ' + relative)

    def _native_month(self, market, month, *, _annual_rows=None):
        entry = self.entries.get((market, month))
        if entry is None or entry['disposition'] != 'CERTIFIED':
            raise UnauthorizedOperation('Native hourly month is unavailable')
        if 'annual_release' in entry:
            from .annual_foundation_independence import monthly_release
            from .annual_foundation_release import read_months
            release = monthly_release(self.root, entry)
            rows = (read_months(self.root, [entry])[month]['observations']
                    if _annual_rows is None else _annual_rows)
        else:
            path = contained(self.root, entry['manifest'])
            if sha256_file(path) != entry['manifest_sha256']:
                raise IntegrityError('Native monthly manifest changed')
            release = read_record(path)
            relative = entry['directory'] + '/observations.parquet'
            expected = self.census['input_sha256'][relative]
            payload = contained(self.root, relative)
            if sha256_file(payload) != expected:
                raise IntegrityError('Native hourly payload changed')
            rows = read_table(payload, name='observations')
        if release['permitted_uses'] != ['LINEAGE_AUDIT']:
            raise UnauthorizedOperation('Native monthly purpose differs')
        group = self.quality_groups.get(market + ':' + month)
        quality = [] if group is None else group.to_pylist()
        by_id = {q['row_id'].hex(): q for q in quality}
        if len(rows) != len(by_id) or len(by_id) != len(quality) or set(by_id) != {r['row_id'] for r in rows}:
            raise IntegrityError('Native quality row correspondence differs')
        for row in rows:
            q = by_id[row['row_id']]
            if (q['market'], q['start_ns']) != (row['market'], row['bar_start_ns']) or row['source_cadence'] != '1h':
                raise IntegrityError('Native quality identity differs')
            row.update(origin='NATIVE_1H', quality_disposition=q['quality_disposition'],
                       reference_minute_count=q['minute_count'])
        if 'annual_release' in entry:
            monthly_release(self.root, entry)
        elif sha256_file(payload) != expected or sha256_file(path) != entry['manifest_sha256']:
            raise IntegrityError('Native hourly input changed during read')
        return rows

    def read_window(self, *, market, start_ns, end_ns, as_of_ns, source='native', purpose='LINEAGE_AUDIT'):
        if purpose != 'LINEAGE_AUDIT':
            raise UnauthorizedOperation('Hourly quality reader grants no research or training admission')
        if source not in ('native', 'minute_derived'):
            raise ContractError('Choose native or minute_derived explicitly')
        if (any(type(v) is not int for v in (start_ns, end_ns, as_of_ns))
                or not 0 < start_ns < end_ns <= CUTOFF or start_ns % HOUR or end_ns % HOUR):
            raise ContractError('Invalid hourly development window')
        self._unchanged()
        if source == 'native':
            from .causal_timeframe_publication import months
            from .annual_foundation_release import read_months
            selected = months(start_ns, end_ns)
            annual = [self.entries[(market, m)] for m in selected
                      if 'annual_release' in self.entries.get((market, m), {})]
            bundles = read_months(self.root, annual)
            rows = [r for month in selected for r in self._native_month(market, month,
                    _annual_rows=bundles[month]['observations'] if month in bundles else None)
                    if start_ns <= r['bar_start_ns'] < end_ns]
        else:
            rows = [dict(r, quality_disposition=r['disposition'], decision_eligible_at_ns=r['available_at_ns'],
                         constituent_row_ids=list(r['constituent_row_ids']))
                    for r in self.derived if r['market'] == market and start_ns <= r['bar_start_ns'] < end_ns]
        result = validate_window(rows, market=market, start_ns=start_ns, end_ns=end_ns, as_of_ns=as_of_ns, source=source)
        self._unchanged()
        return result
