"""Exact-predecessor recovery certification and inactive publication preparation."""
from pathlib import Path
from functools import lru_cache

from .canonical import sha256_file, sha256_json, io_path
from .causal_timeframe_storage import contained as canonical_contained, read_record, signed, table_metadata
from .causal_observation_parquet import FILENAMES
from .daily_identity_review import RULE, require

SCHEMA = 'daily_identity_recovery_month/1'
OPERATION = 'RECOVER_CERTIFY_NATIVE_DAILY_IDENTITY_ONCE'


def contained(root, relative):
    return canonical_contained(io_path(Path(root)),relative)


@lru_cache(maxsize=1)
def _certificate_record(path, digest):
    # One immutable certificate per release. Rehash on every use below; avoid
    # parsing the same 6,316-month census for every monthly payload validation.
    require(sha256_file(path)==digest, 'Daily recovery certificate changed while loading')
    return read_record(path)


def certificate(root, reference):
    path = contained(Path(root), reference['path'])
    require(sha256_file(path)==reference['sha256'], 'Daily recovery certificate changed')
    cert = _certificate_record(path,reference['sha256'])
    require(cert.get('schema')=='daily_identity_recovery_certificate/1'
        and cert.get('status')=='PASS_DAILY_IDENTITY_RECOVERY_CERTIFICATION'
        and cert.get('rule')==RULE and cert.get('research_execution') is False
        and len(cert.get('passes', []))==2 and cert['passes'][0]==cert['passes'][1],
        'Complete independent daily recovery certification required')
    packaging = _verify_reader_binding_successor(root, reference, cert)
    required = {'src/futures_rebuild/daily_identity_recovery.py', 'src/futures_rebuild/daily_identity_transform.py',
        'src/futures_rebuild/daily_identity_replay.py', 'src/futures_rebuild/daily_identity_publication.py',
        'src/futures_rebuild/daily_quality_reader.py', 'src/futures_rebuild/causal_timeframe_publication.py'}
    require(required <= set(cert['implementation']), 'Incomplete recovery implementation binding')
    from .causal_1d import implementation_binding, ENVIRONMENT
    require(cert.get('environment')==ENVIRONMENT, 'Recovery runtime environment differs')
    current = implementation_binding()
    require(set(current)==set(cert['legacy_implementation']), 'Incomplete legacy implementation binding')
    require(packaging or all(current[k]==v for k,v in cert['legacy_implementation'].items() if k!='daily_quality_reader.py'),
            'Unchanged daily dependencies differ from certified baseline')
    if cert.get('synthetic_only') is not True:
        from .causal_timeframe_publication import consumed_evidence, CLAIM
        consumed_evidence(root, cert['receipt'], OPERATION, {'plan_id':cert['plan_id'], 'claim':CLAIM})
    return cert


def legacy_identity(root, index, candidate):
    """Only the exact prior daily release set may retain its historical binding."""
    from .causal_1d import implementation_binding, CONTRACT_ID
    if candidate['contract_id']==CONTRACT_ID and candidate['identity']['implementation']==implementation_binding():
        return True
    if 'daily_identity_recovery' not in index:
        return False
    cert = certificate(root, index['daily_identity_recovery'])
    return (cert['synthetic_only']==candidate['spec']['synthetic_only']
        and candidate['spec_id'] in cert['legacy_spec_ids']
        and candidate['contract_id']==CONTRACT_ID
        and candidate['identity']['implementation']==cert['legacy_implementation'])


def validate_content(root, entry, release, directory, *, published):
    """Validate an explicitly recovered native month, preserving the old release."""
    from .causal_timeframe_publication import validate_release
    root = Path(root)
    require(release.get('schema')==SCHEMA and release.get('rule')==RULE
        and entry['timeframe']=='1d' and release.get('permitted_uses')==['LINEAGE_AUDIT'],
        'Invalid native daily recovery release')
    cert = certificate(root, release['certificate'])
    body = {k:v for k,v in release.items() if k not in ('id','certificate')}
    require(release['id']==sha256_json({k:v for k,v in release.items() if k!='id'}), 'Recovery month identity changed')
    outputs = cert['passes'][0]['outputs']
    key = (entry['market'],entry['month'])
    matches = [e for e in outputs if (e['market'],e['month'])==key]
    require(len(matches)==1 and matches[0]['content_id']==sha256_json(body), 'Recovery month is not certified')
    predecessor = release['predecessor']
    require((predecessor['market'],predecessor['month'])==key and predecessor['timeframe']=='1d'
        and predecessor['disposition']=='CERTIFIED' and entry['spec_id']==cert['plan_id'], 'Recovery predecessor differs')
    original = validate_release(root, predecessor)
    require(original.get('schema') != SCHEMA, 'Unreviewed chained identity recovery is unsupported')
    require(legacy_identity(root, {'daily_identity_recovery':release['certificate']}, original['candidate']),
            'Recovery parent implementation differs')
    require(entry['coverage_start_ns']==predecessor['coverage_start_ns']
        and entry['coverage_end_ns']==predecessor['coverage_end_ns'], 'Recovery changed monthly boundaries')
    files = release['files']
    require(set(files)==set(FILENAMES), 'Recovery requires all five tables')
    require({p.name for p in directory.iterdir()}==set(FILENAMES.values()) | ({'manifest.json'} if published else set()),
            'Unexpected recovery payload files')
    for table, info in files.items():
        path = contained(directory,info['filename'])
        require(info['filename']==FILENAMES[table] and path.stat().st_size==info['size']
            and sha256_file(path)==info['sha256'], 'Recovery payload changed')
    require(table_metadata(directory)==release['table_metadata'], 'Recovery table metadata differs')
    return release


def replacement_index(root, old, replacements, certification):
    """Prepare only; normal readers still require an ordinary publication proof."""
    from .causal_timeframe_publication import validate_index
    validate_index(root,old)
    cert = certificate(root,certification)
    require(cert['predecessor_index_sha256']==sha256_file(contained(Path(root),'data/active/timeframes.json')),
            'Recovery predecessor index changed')
    existing = {(e['timeframe'],e['market'],e['month']):e for e in old['entries']}
    expected = {(e['market'],e['month']):e for e in cert['passes'][0]['outputs']}
    seen = set()
    for before, after in replacements:
        key = (before['timeframe'],before['market'],before['month'])
        require(key not in seen and existing.get(key)==before and key[0]=='1d'
            and (after['timeframe'],after['market'],after['month'])==key
            and key[1:] in expected and after['spec_id']==cert['plan_id']
            and before['disposition']==after['disposition']=='CERTIFIED', 'Ambiguous or changed monthly replacement')
        seen.add(key)
        existing[key] = after
    require({key[1:] for key in seen}==set(expected), 'Incomplete replacement census')
    entries = [existing[k] for k in sorted(existing)]
    prepared = signed(dict(schema=old['schema'],status=old['status'],legacy_1m=old['legacy_1m'],
        expected=[[e['timeframe'],e['market'],e['month']] for e in entries],entries=entries,
        daily_identity_recovery=certification))
    validate_index(root,prepared)
    return prepared


def _verify_reader_binding_successor(root, reference, cert):
    """Retain reconstruction only for the two exact, non-transforming reader fixes.

    No historical certificate is rewritten. All other implementation hashes must
    still match it. The separate active pointer is installed only by the approved
    coordinated publication after unchanged payload and reader validation.
    """
    from .annual_foundation_release import recovery_packaging
    if recovery_packaging(root, reference, cert):
        return True
    original = cert['implementation']
    current = {name: sha256_file(contained(root,name)) for name in original}
    if current == original:
        return
    pointer = read_record(contained(root,'configs/active_daily_recovery_reader_binding.json'))
    require(pointer.get('schema')=='active_daily_recovery_reader_binding/1', 'Recovery reader binding pointer differs')
    binding_ref = pointer['manifest']
    binding_path = contained(root,binding_ref['path'])
    require(sha256_file(binding_path)==binding_ref['sha256'], 'Recovery reader binding manifest changed')
    binding = read_record(binding_path)
    fixes = {
        'src/futures_rebuild/hourly_quality_reader.py':
            ("if timeframe == '1h' and os.path.lexists(contained(Path(root), POINTER)):",
             "if timeframe == '1h':"),
        'src/futures_rebuild/hourly_source_admission.py':
            ('        """Policy-only query. Returned permissions do not establish bar quality."""\n',
             '        """Policy-only query. Returned permissions do not establish bar quality."""\n        self._unchanged()\n'),
    }
    changed = set(fixes)|{'src/futures_rebuild/daily_identity_publication.py'}
    require(binding.get('schema')=='daily_recovery_reader_binding/1'
        and binding.get('policy')=='EXACT_TWO_READER_FIXES_RETAIN_RECONSTRUCTION_V1'
        and binding.get('certificate')==reference
        and binding.get('original_implementation')==original
        and binding.get('replacement_implementation')==current
        and binding.get('permission_expansion') is False
        and binding.get('original_passes_sha256')==sha256_json(cert['passes'])
        and {name for name in original if current[name]!=original[name]}==changed
        and set(binding.get('original_files',{}))==changed,
        'Unapproved recovery implementation change')
    for name, source in binding['original_files'].items():
        path = contained(root,source['path'])
        require(source['sha256']==original[name] and sha256_file(path)==original[name],
                'Retained predecessor implementation changed')
        if name in fixes:
            before, after = fixes[name]
            text = path.read_text()
            require(text.count(before)==1 and contained(root,name).read_text()==text.replace(before,after),
                    'Recovery binding permits only the exact two reader fixes')
