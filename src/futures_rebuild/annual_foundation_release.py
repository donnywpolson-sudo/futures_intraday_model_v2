"""Annual packaging compatibility; inactive until referenced by admitted controls.

Reconstruction, row quality and source permissions remain owned by the existing
readers. This module verifies packaging evidence and selects physical groups.
"""
from pathlib import Path

from .canonical import canonical_bytes, sha256_file, sha256_json
from .causal_timeframe_storage import contained, read_record
from .causal_observation_parquet import FILENAMES
from .errors import IntegrityError

SCHEMA = 'annual_foundation_release/1'


def require(ok, message):
    if not ok:
        raise IntegrityError(message)


def reference_record(root, reference):
    require(set(reference) == {'path', 'sha256'}, 'Exact annual evidence reference required')
    path = contained(root, reference['path'])
    require(sha256_file(path) == reference['sha256'], 'Annual evidence changed: '+reference['path'])
    result = read_record(path)
    require(sha256_file(path) == reference['sha256'], 'Annual evidence changed during read')
    return result


def directory(timeframe, market, year, release_key):
    import re
    require(timeframe in ('1s', '1m', '1h', '1d') and re.fullmatch(r'[A-Z0-9]{1,8}', market)
            and type(year) is int and 2010 <= year <= 2025
            and re.fullmatch(r'[a-f0-9]{64}', release_key), 'Invalid annual storage identity')
    return f'data/causally_gated_normalized/ohlcv_{timeframe}/{market}/{year}/{release_key}'


def validate_manifest(root, manifest):
    from .annual_foundation_independence import validate_manifest as validate
    return validate(root, manifest)


def load(root, reference):
    manifest = reference_record(root, reference)
    packing = validate_manifest(root, manifest)
    expected = directory(packing['timeframe'], packing['market'], packing['year'], packing['release_key'])
    require(reference['path'] == expected+'/manifest.json', 'Annual manifest path differs')
    return manifest


def validate_month(root, entry):
    from .annual_foundation_independence import monthly
    return monthly(root, entry)[0]


def validate_month_reference(entry):
    import re
    ref = entry['annual_release']
    require(set(ref) == {'path','sha256'} and re.fullmatch(r'[a-f0-9]{64}', ref['sha256']),
            'Invalid annual month reference')
    parts = ref['path'].split('/')
    require(len(parts) == 7 and parts[-1] == 'manifest.json'
            and ref['path'] == directory(entry['timeframe'], entry['market'],
                int(entry['month'][:4]), parts[-2])+'/manifest.json', 'Annual month path differs')


def read_months(root, entries):
    """One annual read boundary per requested year, not per bar or month."""
    from .annual_foundation_storage import _read_candidate
    grouped = {}
    for entry in entries:
        ref = entry['annual_release']
        grouped.setdefault((ref['path'], ref['sha256']), []).append(entry)
    result = {}
    for (path, digest), selected in grouped.items():
        require(len({e['month'] for e in selected}) == len(selected), 'Duplicate annual month request')
        manifest = validate_month(root, selected[0])
        for entry in selected[1:]:
            require(validate_month(root, entry) == manifest, 'Annual month reference differs')
        for month, group in _read_candidate(contained(root, path).parent,
                manifest_sha256=digest, months=[e['month'] for e in selected],
                maximum_group_bytes=64*2**20, _packing=manifest['packing']):
            bundle = result.setdefault(month, {name:[] for name in FILENAMES})
            for name, rows in group.items():
                bundle[name].extend(rows)
        # Exhaust the iterator above: its terminal payload hashes are mandatory.
        for entry in selected:
            validate_month(root, entry)
    return result


def minute_paths(entry):
    base = directory('1m', entry['market'], entry['year'], entry['annual_release_key'])
    return base+'/observations.parquet', base+'/manifest.json'


def minute_permission(root, entry):
    manifest = load(root, {'path':entry['sidecar_path'], 'sha256':entry['sidecar_sha256']})
    packing = manifest['packing']
    require((packing['timeframe'], packing['market'], packing['year'], packing['release_key']) ==
            ('1m', entry['market'], entry['year'], entry['annual_release_key'])
            and packing['files']['observations']['sha256'] == entry['parquet_sha256']
            and packing['files']['observations']['rows'] == entry['row_count'],
            'Minute annual payload differs from catalog')
    sidecar = manifest['minute_permission']
    require(sidecar['entry_binding'] == {k:v for k,v in entry.items() if k != 'sidecar_sha256'},
            'Annual minute permission binding differs')
    return sidecar


def verify_minute(root, entry):
    from . import active_data_view as minute
    sidecar = minute_permission(root, entry)
    require(sidecar.get('schema_version') == minute.SIDECAR_SCHEMA
            and sidecar.get('sidecar_id') == sha256_json({k:v for k,v in sidecar.items() if k != 'sidecar_id'})
            and minute.validate_content_validation_receipt(sidecar['content_validation_receipt']) == entry['content_validation_receipt_id']
            and minute.validate_access_policy_binding(sidecar['access_policy_binding']) == entry['access_policy_binding_id'],
            'Annual minute permission certificate differs')
    path = contained(root, entry['parquet_path'])
    require(sha256_file(path) == entry['parquet_sha256'], 'Annual minute observations changed')
    minute_permission(root, entry)
    return path


def predecessor_index(root, index):
    """Packaging retains the exact original monthly publication and cohort scope."""
    import json
    from . import active_data_view as minute
    from .causal_timeframe_storage import signed
    ref = index['annual_packaging']['predecessor_catalog']
    path = contained(root, ref['path'])
    require(sha256_file(path) == ref['sha256'], 'Pre-packaging minute catalog changed')
    original = json.loads(path.read_bytes())
    current = json.loads(contained(root, 'data/causally_gated_normalized/_control/catalog.json').read_bytes())
    minute.validate_catalog(original)
    minute.validate_catalog(current)
    require(len(original['entries']) == len(current['entries']), 'Packaging changes minute membership')
    paths = {'parquet_path','parquet_sha256','sidecar_path','sidecar_sha256','annual_release_key'}
    for old, new in zip(original['entries'], current['entries'], strict=True):
        require({k:v for k,v in old.items() if k not in paths} ==
                {k:v for k,v in new.items() if k not in paths}, 'Packaging changes minute permissions or content identity')
        require(old == new or 'annual_release_key' in new, 'Uncertified minute storage change')
    result = {k:v for k,v in index.items() if k not in ('id','annual_packaging')}
    result['entries'] = [{k:v for k,v in e.items() if k != 'annual_release'} for e in index['entries']]
    result['legacy_1m'] = {'path':'data/active/catalog.json', 'sha256':ref['sha256']}
    return signed(result)


def recovery_packaging(root, reference, cert):
    from .annual_foundation_independence import authority
    evidence = authority(root).get('recovery')
    if evidence is None or evidence['certificate'] != reference:
        return False
    require(evidence['passes_sha256'] == sha256_json(cert['passes']),
            'Independent daily recovery evidence changed')
    return True
