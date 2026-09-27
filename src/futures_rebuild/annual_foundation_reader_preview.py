"""Task-only annual-byte preview through existing admission and quality methods.

This is an integration diagnostic, not a reader successor or publication API.
All original validation still runs. Only the two physical decoding calls on a
private reader instance use independently compared annual candidate bytes.
No module globals, active controls, certificates or permissions are replaced.
"""
from copy import copy, deepcopy
from pathlib import Path
from types import FunctionType, MethodType

from .annual_foundation_storage import _read_candidate, require
from .canonical import sha256_file
from .causal_observation_parquet import FILENAMES
from .causal_timeframe_publication import months
from .causal_timeframe_storage import contained, read_record
from .errors import UnauthorizedOperation


def _replace_decoder(method, name, decoder):
    """Same method bytecode and validation, one instance-local I/O substitution."""
    original = method.__func__
    require(name in original.__code__.co_names and name in original.__globals__,
            'Reader no longer uses the reviewed decoding boundary')
    namespace = dict(original.__globals__, **{name: decoder})
    function = FunctionType(original.__code__, namespace, original.__name__,
                            original.__defaults__, original.__closure__)
    function.__kwdefaults__ = original.__kwdefaults__
    return MethodType(function, method.__self__)


def read_window_preview(reader, *, candidates, request):
    """Run an already constructed admitted reader against exact annual inputs.

    Caller owns historical-read approval and supervision. Candidates are explicit
    references to passing independent comparison records; they grant no purpose.
    A fresh private instance is used for each request; no cross-request cache.
    """
    from .hourly_source_admission import HourlySourceReader
    from .daily_source_admission import DailySourceReader
    require(type(reader) in (HourlySourceReader, DailySourceReader), 'Unsupported preview reader')
    timeframe = '1h' if type(reader) is HourlySourceReader else '1d'
    require(request.get('source', 'native') == 'native', 'Annual preview supports native only')
    uses = reader.permitted_uses(market=request['market'], start_ns=request['start_ns'],
                                end_ns=request['end_ns'], source='native')
    if request.get('purpose', 'LINEAGE_AUDIT') not in uses:
        raise UnauthorizedOperation('Purpose denied before candidate decoding')
    root = reader.root
    selected_months = months(request['start_ns'], request['end_ns'])
    selected = {}
    checks = {}
    for month in selected_months:
        key = (timeframe, request['market'], int(month[:4]))
        require(key in candidates, 'No compared annual candidate for requested year')
        selected.setdefault(key, []).append(month)
    bundles = {}
    for key, selected_in_year in selected.items():
        ref = candidates[key]
        report_path = contained(root, ref['comparison']['path'])
        require(sha256_file(report_path) == ref['comparison']['sha256'], 'Comparison evidence changed')
        checks[report_path] = ref['comparison']['sha256']
        report = read_record(report_path)
        require(report.get('status') == 'PASS_RECOVERY_PHASE' and report.get('phase') == 'compare',
                'Passing independent comparison required')
        matches = [u for u in report['units'] if u['manifest_sha256'] == ref['manifest_sha256']]
        require(len(matches) == 1 and matches[0]['comparison']['status'] ==
                'PASS_INDEPENDENT_FIVE_TABLE_EQUIVALENCE', 'Annual unit was not independently compared')
        # Pilot records store paths relative to their own attempt directory.
        directory = contained(report_path.parent, matches[0]['directory'])
        require(directory == contained(root, ref['directory']), 'Comparison refers to another destination')
        manifest_path = directory / 'manifest.json'
        require(sha256_file(manifest_path) == ref['manifest_sha256'], 'Compared manifest changed')
        checks[manifest_path] = ref['manifest_sha256']
        manifest = read_record(manifest_path)
        require((manifest['timeframe'], manifest['market'], manifest['year']) == key,
                'Candidate identity differs from requested scope')
        for name, info in manifest['files'].items():
            checks[contained(directory, FILENAMES[name])] = info['sha256']
        sources = {s['month']: s for s in manifest['sources']}
        for month in selected_in_year:
            entries = [e for e in reader.quality_reader.index['entries'] if
                       (e['timeframe'], e['market'], e['month']) == (timeframe, key[1], month)]
            if len(entries) != 1 or entries[0]['disposition'] != 'CERTIFIED':
                raise UnauthorizedOperation('Unavailable native month remains unavailable')
            entry = entries[0]
            require(month in sources and sources[month]['directory'] == entry['directory']
                    and sources[month]['start_ns'] == entry['coverage_start_ns']
                    and sources[month]['end_ns'] == entry['coverage_end_ns'],
                    'Annual source selection differs from admitted monthly release')
            original_path = contained(root, entry['manifest'])
            require(sha256_file(original_path) == entry['manifest_sha256'], 'Source manifest changed')
            checks[original_path] = entry['manifest_sha256']
            original = read_record(original_path)
            files = original.get('files') or original['seal']['files']
            require(sources[month]['files'] == {n: {k: f[k] for k in ('sha256', 'size')}
                                               for n, f in files.items()},
                    'Annual candidate is not bound to these exact source tables')
            bundles[contained(root, entry['directory'])] = {n: [] for n in FILENAMES}
        # One annual boundary for all requested months. No unrelated group decode.
        for month, logical in _read_candidate(directory, manifest_sha256=ref['manifest_sha256'],
                        months=selected_in_year, maximum_group_bytes=64*2**20):
            destination = bundles[contained(root, sources[month]['directory'])]
            for name, rows in logical.items():
                destination[name].extend(rows)
    private = copy(reader)
    private.quality_reader = copy(reader.quality_reader)
    if timeframe == '1h':
        def observations(path, *, name):
            require(name == 'observations' and Path(path).name == FILENAMES[name],
                    'Unexpected hourly decoding request')
            return deepcopy(bundles[Path(path).parent][name])
        private.quality_reader._native_month = _replace_decoder(
            private.quality_reader._native_month, 'read_table', observations)
    else:
        def bundle(path):
            require(Path(path) in bundles, 'Unapproved monthly decoding request')
            return deepcopy(bundles[Path(path)])
        private.quality_reader.read_window = _replace_decoder(
            private.quality_reader.read_window, 'read_bundle', bundle)
    rows = private.read_window(**request)
    for path, digest in checks.items():
        require(sha256_file(path) == digest, 'Annual preview binding changed during use')
    reader._unchanged()
    return rows
