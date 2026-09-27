"""Candidate annual authority. Not installed; no publication or activation API.

The installed pointer must bind a passing supervised equivalence census. Legacy
paths in that census are provenance, never fallback file locations.
"""
from functools import lru_cache
import json
from pathlib import Path

from .canonical import sha256_file, sha256_json
from .causal_timeframe_storage import contained, read_record
from .causal_observation_parquet import FILENAMES
from .errors import IntegrityError

POINTER = 'configs/active_annual_foundation_independence.json'
SCHEMA = 'annual_foundation_independence/1'
IMPLEMENTATION = 'src/futures_rebuild/annual_foundation_independence.py'


def require(ok, message):
    if not ok:
        raise IntegrityError(message)


@lru_cache(maxsize=16)
def _record(path, digest):
    path = Path(path)
    require(sha256_file(path) == digest, 'Independence evidence changed')
    result = read_record(path)
    require(sha256_file(path) == digest, 'Independence evidence changed during read')
    return result


def record(root, reference):
    require(type(reference) is dict and set(reference) == {'path', 'sha256'},
            'Exact independence evidence reference required')
    path = contained(Path(root), reference['path'])
    require(sha256_file(path) == reference['sha256'], 'Independence evidence changed')
    result = _record(str(path), reference['sha256'])
    require(result.get('id') == sha256_json({k:v for k,v in result.items() if k != 'id'}),
            'Cached independence evidence was modified')
    return result


def authority(root):
    root = Path(root)
    pointer_path = contained(root, POINTER)
    pointer_digest = sha256_file(pointer_path)
    pointer = record(root, dict(path=POINTER, sha256=pointer_digest))
    require(pointer.get('schema') == 'active_annual_foundation_independence/1',
            'Independent annual publication authority required')
    manifest = record(root, pointer['manifest'])
    result = record(root, pointer['validation'])
    supervision = record(root, pointer['supervision'])
    require(manifest.get('schema') == SCHEMA and manifest.get('status') == 'PREPARED_NOT_ACTIVE'
            and manifest.get('permission_expansion') is False
            and result.get('status') == 'PASS_ANNUAL_INDEPENDENCE'
            and result.get('manifest') == manifest.get('control_relocation', {}).get('predecessor', pointer['manifest'])
            and result.get('unit_count') == len(manifest['units'])
            and result.get('checks') == dict(population=True, permissions=True,
                isolated_cold_readers=True, raw_guards=True, rollback=True)
            and supervision.get('status') == 'PASS_SUPERVISED_PHASE'
            and supervision.get('worker_result_sha256') == pointer['validation']['sha256'],
            'Complete supervised independence validation required')
    if 'control_relocation' in manifest:
        verify_control_relocation(root, pointer, manifest)
    verify_population(root, manifest)
    require(sha256_file(pointer_path) == pointer_digest, 'Independence pointer changed during read')
    return manifest


def verify_reader(root, bindings):
    """Pin the authority for a reader lifetime, including policy-only queries."""
    digest = sha256_file(contained(Path(root), POINTER))
    require(bindings.setdefault(POINTER,digest) == digest, 'Independence pointer changed during reader use')
    authority(root)
    require(sha256_file(contained(Path(root), POINTER)) == digest, 'Independence pointer changed during read')


def unit(root, wrapper):
    manifest = authority(root)
    key = wrapper.get('id')
    require(key == sha256_json({k: v for k, v in wrapper.items() if k != 'id'})
            and key in manifest['units'], 'Annual wrapper is outside certified independence population')
    result = record(root, manifest['units'][key])
    require(result.get('schema') == 'annual_independence_unit/1'
            and result.get('status') == 'PASS_PREDECESSOR_AND_ANNUAL_EQUIVALENCE'
            and result.get('wrapper_id') == key
            and result.get('wrapper') == manifest['wrappers'][key]
            and result.get('packing_id') == wrapper['packing']['id']
            and result.get('counts') == {k: v['rows'] for k, v in wrapper['packing']['files'].items()}
            and set(wrapper['packing']['files']) == set(FILENAMES),
            'Annual independent unit certificate differs')
    require(record(root, result['wrapper']) == wrapper, 'Annual wrapper bytes changed')
    packing = wrapper['packing']
    require(packing['id'] == sha256_json({k:v for k,v in packing.items() if k != 'id'})
            and packing.get('source') == 'native', 'Invalid native packing identity')
    return result


def validate_manifest(root, wrapper):
    unit(root, wrapper)
    return wrapper['packing']


def monthly(root, entry):
    from .annual_foundation_release import validate_month_reference, directory
    validate_month_reference(entry)
    wrapper = record(root, entry['annual_release'])
    certificate = unit(root, wrapper)
    packing = wrapper['packing']
    require(entry['annual_release']['path'] == directory(packing['timeframe'], packing['market'],
            packing['year'], packing['release_key']) + '/manifest.json', 'Annual release path differs')
    month = certificate['months'].get(entry['month'])
    require(month is not None and month['entry'] == entry and entry['disposition'] == 'CERTIFIED',
            'Monthly coverage is not in the independent certificate')
    return wrapper, month


def monthly_release(root, entry):
    # Consumers receive the original record. It is immutable embedded evidence,
    # not a claim that its retired physical files still exist.
    return monthly(root, entry)[1]['release']


def validate_release(root, entry):
    wrapper, month = monthly(root, entry)
    parent = contained(Path(root), entry['annual_release']['path']).parent
    for name, info in wrapper['packing']['files'].items():
        require(info['filename'] == FILENAMES[name], 'Unexpected annual payload name')
        path = contained(parent, info['filename'])
        require(path.stat().st_size == info['size'] and sha256_file(path) == info['sha256'],
                'Independent annual payload changed')
    monthly(root, entry)
    return month['release']


def validate_index(root, index):
    manifest = authority(root)
    require(index.get('id') == manifest['index_id']
            and index['id'] == sha256_json({k:v for k,v in index.items() if k != 'id'}),
            'Index is outside the independently certified population')


def daily_metadata(root, entry):
    _, month = monthly(root, entry)
    return month['daily']


def audit_unit(root, wrapper_reference):
    """Operational five-table integrity audit; never opens predecessor payloads."""
    from .annual_foundation_storage import _read_candidate
    wrapper = record(root, wrapper_reference)
    certificate = unit(root, wrapper)
    parent = contained(Path(root), wrapper_reference['path']).parent
    totals = dict.fromkeys(FILENAMES, 0)
    for _, tables in _read_candidate(parent, manifest_sha256=wrapper_reference['sha256'],
            months=[s['month'] for s in wrapper['packing']['sources']],
            maximum_group_bytes=64*2**20, _packing=wrapper['packing']):
        for name, rows in tables.items():
            totals[name] += len(rows)
    require(totals == certificate['counts'], 'Independent population counts differ')
    unit(root, wrapper)
    return totals


def verify_population(root, manifest):
    """Shared census checks; does not grant publication authority."""
    root = Path(root)
    require(manifest.get("schema") == SCHEMA and manifest.get("status") == "PREPARED_NOT_ACTIVE"
            and manifest.get("permission_expansion") is False, "Invalid independent population")
    required = {IMPLEMENTATION, 'src/futures_rebuild/annual_foundation_release.py',
        'src/futures_rebuild/annual_foundation_storage.py',
        'src/futures_rebuild/hourly_quality_reader.py', 'src/futures_rebuild/daily_quality_reader.py',
        'src/futures_rebuild/causal_timeframe_publication.py',
        'src/futures_rebuild/hourly_source_admission.py', 'src/futures_rebuild/daily_source_admission.py',
        'data/causally_gated_normalized/_control/catalog.json', 'data/causally_gated_normalized/_control/timeframes.json'}
    require(required <= manifest['bindings'].keys(), 'Incomplete independence runtime bindings')
    for relative, digest in manifest['bindings'].items():
        require(sha256_file(contained(root, relative)) == digest,
                'Independent runtime binding changed: ' + relative)
    index = record(root, dict(path='data/causally_gated_normalized/_control/timeframes.json',
                             sha256=manifest['bindings']['data/causally_gated_normalized/_control/timeframes.json']))
    catalog = json.loads(contained(root,'data/causally_gated_normalized/_control/catalog.json').read_bytes())
    expected = {(e['annual_release']['path'],e['annual_release']['sha256'])
                for e in index['entries'] if 'annual_release' in e}
    expected.update((e['sidecar_path'],e['sidecar_sha256']) for e in catalog['entries'] if 'annual_release_key' in e)
    actual = {(r['path'],r['sha256']) for r in manifest['wrappers'].values()}
    require(set(manifest['units']) == set(manifest['wrappers']) and expected == actual
            and len(actual) == len(manifest['units']) > 0,
            'Independent authority omits or duplicates active annual coverage')



def verify_control_relocation(root, pointer, manifest):
    """Reuse population evidence only for this exact metadata-only relocation."""
    relocation = manifest['control_relocation']
    previous = record(root, relocation['predecessor'])
    require('control_relocation' not in previous, 'Chained relocation requires separate review')
    for field in ('schema', 'status', 'permission_expansion', 'units', 'wrappers', 'recovery'):
        require(manifest.get(field) == previous.get(field), 'Relocation changed certified population: '+field)
    old_index = record(root, relocation['index_before'])
    new_index = read_record(contained(Path(root), 'data/causally_gated_normalized/_control/timeframes.json'))
    restored = {k:v for k,v in new_index.items() if k != 'id'}
    restored['legacy_1m'] = dict(restored['legacy_1m'], path='data/active/catalog.json')
    require(sha256_json(restored) == old_index['id'] and manifest['index_id'] == new_index['id'],
            'Relocation changed timeframe coverage or publication history')
    old = previous['bindings']; new = manifest['bindings']
    require(relocation['index_before']['sha256'] == old['data/active/timeframes.json'],
            'Relocation predecessor index is not the certified binding')
    aliases = {'data/active/catalog.json':'data/causally_gated_normalized/_control/catalog.json',
               'data/active/timeframes.json':'data/causally_gated_normalized/_control/timeframes.json'}
    require(set(new) == {aliases.get(k,k) for k in old}, 'Relocation changed binding membership')
    require(new[aliases['data/active/catalog.json']] == old['data/active/catalog.json'],
            'Relocation changed minute catalog bytes')
    changed = {k:dict(before=v, after=new[aliases.get(k,k)]) for k,v in old.items()
               if new[aliases.get(k,k)] != v}
    require(changed == relocation['changed_bindings'], 'Relocation implementation delta differs')
    require(all(k == 'data/active/timeframes.json' or k.startswith('src/futures_rebuild/')
                for k in changed), 'Relocation changed non-implementation evidence')
    checked = record(root, pointer['relocation_validation'])
    supervision = record(root, pointer['relocation_supervision'])
    require(checked.get('status') == 'PASS_CONTROL_ROOT_METADATA'
            and checked.get('manifest') == pointer['manifest']
            and checked.get('predecessor') == relocation['predecessor']
            and checked.get('checks') == dict(population_unchanged=True, permissions_unchanged=True,
                implementation_delta=True, synthetic_checks=True)
            and supervision.get('status') == 'PASS_SUPERVISED_PHASE'
            and supervision.get('worker_result_sha256') == pointer['relocation_validation']['sha256'],
            'Supervised control relocation equivalence required')
