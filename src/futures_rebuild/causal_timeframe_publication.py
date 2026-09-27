"""Version-three immutable timeframe references and receipt-bound publication.

Historical provenance validation is evidence verification, never execution authority.
"""
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from dataclasses import asdict
from hashlib import sha256
import json
import os

from .canonical import canonical_bytes, sha256_file, sha256_json
from .causal_timeframe_storage import contained, signed, read_record, Budget, file_inventory, table_metadata
from .causal_timeframe_execution import execution_lease
from .causal_observation_parquet import FILENAMES, SCHEMAS
from .causal_observation_foundation import ECONOMICS_RULEBOOK_SHA256, V10_CHECKPOINT_MARKETS
from .boundary import (OperationReceipt, OperationClassification, RepoBoundary,
    PERSONAL_APPROVAL_AUTHORITY_ID, PERSONAL_APPROVAL_ALGORITHM, EXTERNAL_AUTHORITY_REGISTRY_HASH,
    _verify_personal_approval_digest)
from .errors import IntegrityError, UnauthorizedOperation, ContractError

INDEX = 'data/causally_gated_normalized/_control/timeframes.json'
SCHEMA = 'causal_timeframe_index/3'
OPERATION = 'PUBLISH_CAUSAL_TIMEFRAME_RELEASE_ONCE'
CLAIM = 'CAUSAL_RECONSTRUCTION_ONLY_NO_RESEARCH_OR_PUBLICATION'
CUTOFF = 1752444000 * 10**9
SUPPORTED = ('1s', '1h', '1d')
APPROVAL_KEYS = {'approval_command','approval_plan_id','approval_plan_sha256'}


def identity(value):
    if value.get('id') != sha256_json({k:v for k,v in value.items() if k != 'id'}):
        raise IntegrityError('timeframe evidence identity differs')


def months(start, end):
    if not 0 < start < end <= CUTOFF:
        raise IntegrityError('invalid development coverage')
    dt = datetime.fromtimestamp(start // 10**9, timezone.utc).replace(day=1,hour=0,minute=0,second=0)
    result = []
    while int(dt.timestamp()) * 10**9 < end:
        result.append(dt.strftime('%Y-%m'))
        dt = dt.replace(year=dt.year + (dt.month == 12), month=dt.month % 12 + 1)
    return result


def consumed_evidence(root, raw, operation, scope):
    """Verify archived approval and exact use record without reviving expired authority."""
    receipt = OperationReceipt.from_dict(raw)
    bound = dict(receipt.scope)
    if ({k:v for k,v in bound.items() if k not in APPROVAL_KEYS} != scope
            or set(bound) != set(scope) | APPROVAL_KEYS
            or receipt.operation != operation or receipt.repository_id != RepoBoundary(root).repository_id
            or receipt.classification not in (OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
                                              OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION)
            or not receipt.externally_authorized or not receipt.single_use
            or receipt.authority_key_id != PERSONAL_APPROVAL_AUTHORITY_ID
            or receipt.authority_registry_hash != EXTERNAL_AUTHORITY_REGISTRY_HASH
            or receipt.signature_algorithm != PERSONAL_APPROVAL_ALGORITHM
            or not receipt.issued_at <= receipt.not_before < receipt.expires_at
            or not _verify_personal_approval_digest(scope=receipt.scope, approval_digest=receipt.signature_hex)):
        raise UnauthorizedOperation('historical approval provenance is invalid')
    core = receipt._core(receipt.operation,receipt.repository_id,receipt.classification,receipt.scope,
        receipt.externally_authorized,receipt.authority_key_id,receipt.signature_algorithm,
        receipt.issued_at,receipt.not_before,receipt.expires_at,receipt.nonce,receipt.authority_registry_hash,receipt.single_use)
    path = contained(root, f'state/authorization_uses/{receipt.receipt_id}.json')
    if sha256_json(core) != receipt.receipt_id or path.read_bytes() != canonical_bytes(receipt._authorization_use_payload()) + b'\n':
        raise IntegrityError('historical authorization use differs')
    return receipt


def verify_provenance(root, candidate, cert, provenance):
    if provenance and set(provenance)=={'path','sha256'}:
        path=contained(root,provenance['path'])
        if sha256_file(path)!=provenance['sha256']: raise IntegrityError('operation provenance file changed')
        provenance=read_record(path)
    spec = candidate['spec']
    if spec['timeframe'] not in SUPPORTED:
        raise IntegrityError('unsupported timeframe evidence')
    if spec['timeframe']=='1s':
        from .causal_1s import Spec as EvidenceSpec
    elif spec['timeframe']=='1h':
        from .causal_1h import Spec as EvidenceSpec
    else:
        from .causal_1d import Spec as EvidenceSpec
    EvidenceSpec(**spec)  # Validate scope without recomputing a historical identity.
    if spec['synthetic_only']:
        frozen = candidate.get('identity')
        if not frozen:
            # Legacy synthetic fixtures remain synthetic and must match their current implementation.
            from .causal_1s import Spec
            if spec['timeframe'] != '1s' or Spec(**spec).id != candidate['spec_id']:
                raise IntegrityError('legacy synthetic identity differs')
            return
    else:
        if not provenance:
            raise UnauthorizedOperation('historical operation and approval provenance required')
        op = provenance['operation']
        identity(op)
        expected_operation = f"BUILD_CERTIFY_CAUSAL_{spec['timeframe'].upper()}_RANGE_ONCE"
        if (op.get('schema')!=f"causal_{spec['timeframe']}_native_operation/1"
                or op['operation'] != expected_operation or op['spec'] != spec or op['spec_id'] != candidate['spec_id']
                or Path(op['repository_root']).resolve() != Path(root).resolve()
                or sha256_json(op['entries']) != spec['source_manifest_id']):
            raise IntegrityError('historical operation/source differs')
        receipt = consumed_evidence(root, provenance['receipt'], expected_operation,
                                    {'operation_id':op['id'], 'claim':CLAIM})
        if receipt.classification != OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION:
            raise UnauthorizedOperation('historical reconstruction requires historical approval')
        finish = provenance['supervision']
        identity(finish)
        if (finish['operation_id'] != op['id'] or finish['status'] != 'COMPLETE'
                or finish['spec_id'] != candidate['spec_id'] or finish['certificate_id'] != cert['id']):
            raise IntegrityError('complete native supervision differs')
        if spec['timeframe'] in ('1h','1d'):
            aggregate=provenance.get('aggregate_memory')
            if not aggregate: raise IntegrityError('complete aggregate supervision required')
            path=contained(root,aggregate['path'])
            if sha256_file(path)!=aggregate['sha256']: raise IntegrityError('aggregate supervision changed')
            measured=read_record(path)
            coordinator_peak=measured.get('coordinator_peak_memory_bytes')
            if (measured.get('coordinator_outside_job') is not True
                    or type(coordinator_peak) is not int or coordinator_peak<=0):
                raise IntegrityError('complete coordinator memory measurement required')
            if (measured['operation_id']!=op['id'] or measured['status']!='COMPLETE'
                    or measured['maximum_supervisor_worker_commit_bytes']!=op['aggregate_memory_bytes']
                    or not 0<measured['peak_supervisor_worker_commit_bytes']<=op['aggregate_memory_bytes']):
                raise IntegrityError('aggregate supervision does not certify this operation')
        frozen = {'spec':spec, 'contract_id':sha256_json(op['contract']),
                  'economics':ECONOMICS_RULEBOOK_SHA256, 'implementation':op['implementation'], 'environment':op['environment']}
    if (sha256_json(frozen) != candidate['spec_id'] or frozen['spec'] != spec
            or frozen['contract_id'] != candidate['contract_id']):
        raise IntegrityError('frozen implementation identity differs')
    if spec['timeframe']=='1d':
        metadata=candidate['provenance']
        if (candidate['identity']!=frozen or sha256_json(metadata['contract'])!=frozen['contract_id']
                or metadata['implementation']!=frozen['implementation'] or metadata['environment']!=frozen['environment']
                or candidate['permitted_uses']!=(['SYNTHETIC_TEST'] if spec['synthetic_only'] else ['LINEAGE_AUDIT'])):
            raise IntegrityError('daily frozen metadata or access policy differs')
        if not spec['synthetic_only'] and (candidate['operation']!=op or candidate['source_inventory']!=op['entries']):
            raise IntegrityError('daily source inventory or operation provenance differs')


def validate_release(root, entry):
    if 'annual_release' in entry:
        from .annual_foundation_independence import validate_release as independent_release
        return independent_release(root, entry)
    path = contained(root, entry['manifest'])
    if sha256_file(path) != entry['manifest_sha256']:
        raise IntegrityError('immutable manifest changed')
    release = read_record(path)
    return _validate_content(root,entry,release,contained(root,entry['directory']),published=True)


def _validate_content(root, entry, release, directory, *, published):
    if release.get('schema') == 'daily_identity_recovery_month/1':
        from .daily_identity_publication import validate_content
        return validate_content(root, entry, release, directory, published=published)
    identity(release)
    if release.get('schema') not in ('causal_timeframe_month/3','causal_1s_published_month/1'):
        raise IntegrityError('unknown monthly manifest schema')
    candidate, cert, seal, finish = (release[k] for k in ('candidate','certificate','seal','certification_finish'))
    for obj in (candidate, cert, seal, finish): identity(obj)
    spec = candidate['spec']
    expected_status = 'SYNTHETIC_ONLY_NOT_HISTORICAL_CERTIFICATION' if spec['synthetic_only'] else 'HISTORICAL_RECONSTRUCTION_CERTIFIED_NOT_ACTIVE'
    if (cert['candidate_id'] != candidate['id'] or cert['spec_id'] != candidate['spec_id']
            or cert['contract_id'] != candidate['contract_id'] or cert['status'] != expected_status
            or any(cert['authority'].values()) or len(cert['passes']) != 2 or cert['passes'][0] != cert['passes'][1]
            or cert['passes'][0]['rows'] != candidate['observations']
            or candidate['months'] != months(spec['start_ns'],spec['end_ns'])
            or cert['passes'][0]['months'] != candidate['months'] or len(candidate['seals']) != len(candidate['months'])
            or seal['id'] != candidate['seals'][candidate['months'].index(entry['month'])]
            or seal['spec_id'] != candidate['spec_id'] or seal['timeframe'] != spec['timeframe']
            or finish['kind'] != 'CERTIFY' or finish['status'] != 'COMPLETE'
            or finish['result_id'] != cert['id'] or finish['spec_id'] != candidate['spec_id']
            or (entry['timeframe'],entry['market'],entry['spec_id']) != (spec['timeframe'],spec['market'],candidate['spec_id'])
            or entry['coverage_start_ns'] != seal['coverage'][0]['start_ns']
            or entry['coverage_end_ns'] != seal['coverage'][-1]['end_ns']):
        raise IntegrityError('complete certification or coverage differs')
    verify_provenance(root,candidate,cert,release.get('provenance') or entry.get('provenance'))
    if 'contract' in candidate and sha256_json(candidate['contract'])!=candidate['contract_id']:
        raise IntegrityError('causal contract metadata differs')
    if spec['timeframe'] in ('1h','1d') and candidate['comparison_counts']!=cert['passes'][0]['comparison_counts']:
        raise IntegrityError('reconciliation totals differ')
    allowed = ['SYNTHETIC_TEST'] if spec['synthetic_only'] else ['LINEAGE_AUDIT']
    legacy=release.get('schema')=='causal_1s_published_month/1' and spec['timeframe']=='1s'
    if not legacy and release['permitted_uses'] != allowed: raise UnauthorizedOperation('release access policy differs')
    expected_files = set(FILENAMES.values()) | ({'manifest.json'} if published else set())
    if set(p.name for p in directory.iterdir()) != expected_files:
        raise IntegrityError('unexpected monthly payload membership')
    for name, info in seal['files'].items():
        path = contained(directory,info['filename'])
        if info['filename'] != FILENAMES[name] or path.stat().st_size != info['size'] or sha256_file(path) != info['sha256']:
            raise IntegrityError('monthly payload changed')
    if set(seal['files']) != set(FILENAMES): raise IntegrityError('five logical tables required')
    if spec['timeframe']!='1s' and table_metadata(directory)!=seal['table_metadata']:
        raise IntegrityError('published table schema/count metadata differs')
    return release


def validate_index(root, index):
    identity(index)
    if index.get('schema') != SCHEMA:
        raise IntegrityError('Publication index required; staging envelopes are not readable')
    keys = [(e['timeframe'],e['market'],e['month']) for e in index['entries']]
    if (index['schema'] != SCHEMA or index['status'] != 'PUBLISHED_CAUSAL_RECONSTRUCTION_ONLY'
            or keys != sorted(set(keys)) or index['expected'] != [list(k) for k in keys]
            or index['legacy_1m'] != {'path':'data/causally_gated_normalized/_control/catalog.json','sha256':sha256_file(contained(root,'data/causally_gated_normalized/_control/catalog.json'))}):
        raise IntegrityError('timeframe index identity, coverage or legacy binding differs')
    for e in index['entries']:
        if e['timeframe'] not in SUPPORTED or e['market'] not in V10_CHECKPOINT_MARKETS:
            raise IntegrityError('unsupported timeframe or market')
        if months(e['coverage_start_ns'],e['coverage_end_ns']) != [e['month']]:
            raise IntegrityError('monthly coverage differs')
        if 'annual_release' in e:
            from .annual_foundation_release import validate_month_reference
            if e['timeframe'] not in ('1s','1h','1d') or e['disposition'] != 'CERTIFIED':
                raise IntegrityError('Annual storage cannot authorize unavailable or unsupported entries')
            validate_month_reference(e)
        if e['disposition'] == 'CERTIFIED':
            directory = f"data/causally_gated_normalized/{e['market']}/{e['month'][:4]}/{e['timeframe']}/{e['month']}/{e['spec_id']}"
            if e['directory'] != directory or e['manifest'] != directory + '/manifest.json':
                raise IntegrityError('monthly release path differs')
        elif e['disposition'] not in ('UNAVAILABLE','QUARANTINED') or not e.get('reason'):
            raise IntegrityError('invalid coverage disposition')
    if 'annual_packaging' in index:
        from .annual_foundation_independence import validate_index as independent_index
        independent_index(root, index)
    elif 'publication' in index:
        publication = index['publication']
        plan = read_record(contained(root,publication['plan_path']))
        if sha256_file(contained(root,publication['plan_path'])) != publication['plan_sha256']:
            raise IntegrityError('publication plan changed')
        without_proof = signed({k:v for k,v in index.items() if k not in ('publication','id')})
        if 'annual_packaging' in index:
            from .annual_foundation_release import predecessor_index
            without_proof = predecessor_index(root, without_proof)
        if without_proof != plan['index']: raise IntegrityError('published index differs from approved plan')
        consumed_evidence(root,publication['receipt'],OPERATION,
            {'publication_id':plan['id'],'limits_id':publication['limits_id']})


def resolve_partitions(root, index, *, market, year, timeframe, purpose, require_status=False):
    import pyarrow.parquet as pq
    from .hourly_quality_reader import require_window_reader
    require_window_reader(root, timeframe)
    if timeframe == '1d':
        raise UnauthorizedOperation('Daily quality policy requires DailyReader.read_window; raw partition access refused')
    validate_index(root,index)
    if require_status: raise UnauthorizedOperation('status-qualified research is not admitted')
    entries = [e for e in index['entries'] if (e['timeframe'],e['market'],e['month'][:4]) == (timeframe,market,str(year))]
    if not entries or any(e['disposition'] != 'CERTIFIED' for e in entries):
        raise UnauthorizedOperation('declared timeframe coverage is unavailable')
    result = []
    verified_annual = {}
    for e in entries:
        annual_key = tuple(sorted(e['annual_release'].items())) if 'annual_release' in e else None
        if annual_key in verified_annual:
            from .annual_foundation_independence import monthly_release
            release = monthly_release(root, e)
        else:
            release = validate_release(root,e)
        spec = release['candidate']['spec']
        expected = [m for m in months(spec['start_ns'],spec['end_ns']) if m[:4] == str(year)]
        present = [x['month'] for x in entries if x['spec_id'] == e['spec_id']]
        if present != expected: raise IntegrityError('declared year interval has missing months')
        allowed = 'SYNTHETIC_TEST' if spec['synthetic_only'] else 'LINEAGE_AUDIT'
        if not spec['synthetic_only'] and 'publication' not in index:
            raise UnauthorizedOperation('historical active index lacks publication authority')
        if purpose != allowed: raise UnauthorizedOperation('timeframe purpose is not admitted')
        expected_rows = sum(d['observations'] for d in release['seal']['coverage'])
        if annual_key is not None:
            from .annual_foundation_release import load
            wrapper = load(root, e['annual_release'])
            packing = wrapper['packing']
            selected = [x['month'] for x in entries if x.get('annual_release') == e['annual_release']]
            if (packing['timeframe'], packing['market'], packing['year']) != (timeframe, market, year):
                raise IntegrityError('Annual partition identity differs')
            if set(selected) != {s['month'] for s in packing['sources']}:
                raise UnauthorizedOperation('Annual path would expose months outside admitted scope')
            path = contained(root,e['annual_release']['path']).parent / FILENAMES['observations']
            expected_rows = packing['files']['observations']['rows']
            if annual_key in verified_annual:
                continue
        else:
            path = contained(root,e['directory'] + '/' + FILENAMES['observations'])
        with pq.ParquetFile(path) as parquet:
            if (not parquet.schema_arrow.equals(SCHEMAS['observations'],check_metadata=True)
                    or parquet.metadata.num_rows != expected_rows):
                raise IntegrityError('observation schema or count differs')
            for group in range(parquet.metadata.num_row_groups):
                stats = parquet.metadata.row_group(group).column(SCHEMAS['observations'].get_field_index('source_cadence')).statistics
                if stats is None or stats.min != timeframe or stats.max != timeframe or stats.null_count:
                    raise IntegrityError('observation cadence differs')
        result.append(path)
        if annual_key is not None:
            verified_annual[annual_key] = path
    for key in verified_annual:
        entry = next(e for e in entries if tuple(sorted(e.get('annual_release', {}).items())) == key)
        validate_release(root, entry)
    if read_record(contained(root,INDEX)) != index: raise IntegrityError('active index changed during resolution')
    return tuple(result)


def prepare_publication(root, *, candidates, provenance_files=None, legacy_provenance=None):
    root = Path(root)
    old_path = contained(root,INDEX)
    old = read_record(old_path) if old_path.exists() else None
    return _prepare_publication(root, candidates=candidates, provenance_files=provenance_files,
        legacy_provenance=legacy_provenance, old=old,
        predecessor_sha256=sha256_file(old_path) if old_path.exists() else None)


def _prepare_publication(root, *, candidates, provenance_files, legacy_provenance, old, predecessor_sha256):
    legacy_provenance={} if legacy_provenance is None else dict(legacy_provenance)
    if old is not None:
        if old['schema']=='causal_timeframe_index/2':
            from .causal_timeframe_view import _validate_index_v2
            _validate_index_v2(root,old)
        else: validate_index(root,old)
    provenance_files = {} if provenance_files is None else dict(provenance_files)
    if set(provenance_files)-set(candidates): raise IntegrityError('unrelated provenance files')
    entries = [] if old is None else list(old['entries'])
    if old is not None and old['schema']=='causal_timeframe_index/2':
        upgraded=[]
        for original in entries:
            if original['disposition']!='CERTIFIED':
                raise IntegrityError('legacy unavailable coverage requires explicit certified scope evidence')
            release=read_record(contained(root,original['manifest']))
            entry={**original,'coverage_start_ns':release['seal']['coverage'][0]['start_ns'],
                   'coverage_end_ns':release['seal']['coverage'][-1]['end_ns']}
            if not release['candidate']['spec']['synthetic_only']:
                relative=legacy_provenance.get(original['manifest'])
                if relative is None: raise UnauthorizedOperation('legacy historical provenance must be supplied explicitly')
                entry['provenance']={'path':relative,'sha256':sha256_file(contained(root,relative))}
            validate_release(root,entry)
            upgraded.append(entry)
        entries=upgraded
    moves = []
    for relative in candidates:
        if not relative.startswith('state/unpublished_evidence/') or not relative.endswith('/candidate.json'):
            raise IntegrityError('task-owned candidate metadata required')
        path = contained(root,relative)
        candidate = read_record(path)
        cert = read_record(path.parent/'certificate.json')
        spec = candidate['spec']
        if (candidate.get('schema')!=f"causal_{spec['timeframe']}_candidate/1"
                or cert.get('schema')!=f"causal_{spec['timeframe']}_certificate/1"
                or candidate['months']!=months(spec['start_ns'],spec['end_ns'])
                or len(candidate['months'])!=len(candidate['seals'])
                or cert['candidate_id']!=candidate['id'] or cert['spec_id']!=candidate['spec_id']
                or cert['contract_id']!=candidate['contract_id'] or any(cert['authority'].values())
                or len(cert['passes'])!=2 or cert['passes'][0]!=cert['passes'][1]
                or cert['passes'][0]['rows']!=candidate['observations']
                or cert['passes'][0]['months']!=candidate['months']):
            raise IntegrityError('candidate declared scope or complete certificate differs')
        finishes = [read_record(p) for p in (path.parent/'runs').glob('*.finish.json')]
        finishes = [f for f in finishes if f['kind']=='CERTIFY' and f['status']=='COMPLETE' and f['result_id']==cert['id']]
        if not finishes: raise IntegrityError('complete certification accounting required')
        provenance_path = contained(root,provenance_files[relative]) if relative in provenance_files else path.parent/'provenance.json'
        provenance = None if spec['synthetic_only'] else {'path':provenance_files.get(relative,str(PurePosixPath(relative).parent/'provenance.json')),'sha256':sha256_file(provenance_path)}
        verify_provenance(root,candidate,cert,provenance)
        for month,seal_id in zip(candidate['months'],candidate['seals'],strict=True):
            seal = read_record(path.parent/f'{month}.seal.json')
            if seal['id'] != seal_id: raise IntegrityError('month seal differs')
            e = {'timeframe':spec['timeframe'],'market':spec['market'],'month':month,'spec_id':candidate['spec_id'],
                 'coverage_start_ns':seal['coverage'][0]['start_ns'],'coverage_end_ns':seal['coverage'][-1]['end_ns']}
            if not seal['files']:
                entries.append({**e,'disposition':'UNAVAILABLE','reason':'CERTIFIED_SOURCE_UNAVAILABLE','seal_id':seal_id})
                continue
            directory = f"data/causally_gated_normalized/{spec['market']}/{month[:4]}/{spec['timeframe']}/{month}/{candidate['spec_id']}"
            release = signed({'schema':'causal_timeframe_month/3','candidate':candidate,'certificate':cert,
                'seal':seal,'certification_finish':finishes[-1],'provenance':provenance,
                'permitted_uses':['SYNTHETIC_TEST'] if spec['synthetic_only'] else ['LINEAGE_AUDIT']})
            e.update(disposition='CERTIFIED',directory=directory,manifest=directory+'/manifest.json',
                     manifest_sha256=sha256(canonical_bytes(release)+b'\n').hexdigest())
            entries.append(e)
            source=str(PurePosixPath(relative).parent/seal['directory'])
            _validate_content(root,e,release,contained(root,source),published=False)
            moves.append({'source':source,'entry':e,'release':release})
    entries.sort(key=lambda e:(e['timeframe'],e['market'],e['month']))
    index = signed({'schema':SCHEMA,'status':'PUBLISHED_CAUSAL_RECONSTRUCTION_ONLY',
        'legacy_1m':{'path':'data/causally_gated_normalized/_control/catalog.json','sha256':sha256_file(contained(root,'data/causally_gated_normalized/_control/catalog.json'))},
        'expected':[[e['timeframe'],e['market'],e['month']] for e in entries],'entries':entries,
        **({'daily_identity_recovery':old['daily_identity_recovery']}
           if old and 'daily_identity_recovery' in old else {})})
    validate_index(root,index)
    return signed({'schema':'causal_timeframe_publication/3','candidates':list(candidates),'provenance_files':provenance_files,'legacy_provenance':legacy_provenance,'moves':moves,'index':index,
                   'predecessor_sha256':predecessor_sha256})


def publish(root, plan, receipt, limits):
    from .active_data_view import PUBLICATION_LOCK
    from .locking import FileLease
    root = Path(root)
    identity(plan)
    required = {'publication_id':plan['id'],'limits_id':sha256_json(asdict(limits))}
    if type(receipt) is not OperationReceipt: raise UnauthorizedOperation('publication receipt required')
    required.update({k:v for k,v in receipt.scope if k in APPROVAL_KEYS})
    kwargs = dict(operation=OPERATION,classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,required_scope=required)
    with execution_lease(root), FileLease(root/PUBLICATION_LOCK):
        receipt.verify(RepoBoundary(root),**kwargs)
        journal = contained(root,'state/unpublished_evidence/causal_timeframe_publications/'+plan['id'])
        plan_relative='state/unpublished_evidence/causal_timeframe_publications/'+plan['id']+'/plan.json'
        published_index=signed({k:v for k,v in plan['index'].items() if k!='id'} | {'publication':{
            'plan_path':plan_relative,'plan_sha256':sha256(canonical_bytes(plan)+b'\n').hexdigest(),
            'receipt':receipt.as_dict(),'limits_id':sha256_json(asdict(limits))}})
        index_path = contained(root,INDEX)
        if journal.exists():
            saved = read_record(journal/'started.json')
            if saved['receipt_id'] != receipt.receipt_id: raise UnauthorizedOperation('publication recovery receipt differs')
            receipt.assert_consumed(RepoBoundary(root),**kwargs)
        else:
            if prepare_publication(root,candidates=plan['candidates'],provenance_files=plan['provenance_files'],legacy_provenance=plan['legacy_provenance']) != plan: raise IntegrityError('publication inputs changed')
        if index_path.exists() and read_record(index_path) == published_index:
            for entry in plan['index']['entries']:
                if entry['disposition']=='CERTIFIED': validate_release(root,entry)
            return published_index
        if (sha256_file(index_path) if index_path.exists() else None) != plan['predecessor_sha256']:
            raise IntegrityError('publication predecessor changed')
        if not journal.exists():
            receipt.consume(RepoBoundary(root),**kwargs)
            journal.mkdir(parents=True)
        budget = Budget(journal,limits)
        # Charge prior attempt metadata; immutable payloads move without copies.
        budget.written = sum(p.stat().st_size for p in journal.iterdir() if p.is_file())
        budget.written += sum(contained(root,m['entry']['manifest']).stat().st_size for m in plan['moves'] if contained(root,m['entry']['manifest']).exists())
        if not (journal/'started.json').exists(): budget.save(journal/'started.json',signed({'receipt_id':receipt.receipt_id,'plan_id':plan['id']}))
        if not (journal/'plan.json').exists(): budget.save(journal/'plan.json',plan)
        if index_path.exists() and not (journal/'previous_index.json').exists():
            with budget.open(journal/'previous_index.json') as f: f.write(index_path.read_bytes())
        _stage_moves(root,plan,budget)
        _validate_population(root,plan['index'],budget)
        if (sha256_file(index_path) if index_path.exists() else None) != plan['predecessor_sha256']:
            raise IntegrityError('publication predecessor changed before commit')
        pending = journal/'next_index.json'
        if pending.exists():
            if read_record(pending) != published_index: raise IntegrityError('interrupted index differs')
        else: budget.save(pending,published_index)
        budget.check()
        ready=signed({'status':'READY_FOR_INDEX_COMMIT','index_id':published_index['id'],'payload_bytes_copied':0})
        if (journal/'ready.json').exists():
            if read_record(journal/'ready.json') != ready: raise IntegrityError('interrupted ready record differs')
        else: budget.save(journal/'ready.json',ready)
        os.replace(pending,index_path)
        return published_index


def _stage_moves(root, plan, budget):
    for move in plan['moves']:
        budget.check()
        source, destination = contained(root,move['source']),contained(root,move['entry']['directory'])
        destination.parent.mkdir(parents=True,exist_ok=True)
        if source.exists():
            if destination.exists() or source.stat().st_dev != destination.parent.stat().st_dev:
                raise IntegrityError('publication requires absent same-volume destination')
            files = move['release']['files'] if move['release'].get('schema')=='daily_identity_recovery_month/1' else move['release']['seal']['files']
            if file_inventory(source) != files: raise IntegrityError('candidate payload changed')
            os.rename(source,destination)
        manifest = destination/'manifest.json'
        if not manifest.exists(): budget.save(manifest,move['release'])
        validate_release(root,move['entry'])


def _validate_population(root, index, budget):
    counts = {}
    for entry in index['entries']:
        budget.check()
        if entry['disposition']=='CERTIFIED':
            release = validate_release(root,entry)
            tables = release['table_metadata'] if release.get('schema')=='daily_identity_recovery_month/1' else release['seal'].get('table_metadata',{})
            for name, metadata in tables.items():
                key = entry['timeframe'] + ':' + name
                counts[key] = counts.get(key,0) + metadata['rows']
    validate_index(root,index)
    return counts
