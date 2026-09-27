"""Prepare and resolve manifest-backed timeframe views; no activation writer."""
from pathlib import Path, PurePosixPath
from dataclasses import asdict,replace
from hashlib import sha256
import json
import pyarrow.parquet as pq

from .canonical import canonical_bytes, sha256_file, sha256_json
from .causal_1s import Spec, CONTRACT_ID, CUTOFF, stamp
from .causal_timeframe_storage import contained, read_record, signed
from .causal_observation_parquet import SCHEMAS
from .errors import IntegrityError, UnauthorizedOperation, ContractError

INDEX='data/causally_gated_normalized/_control/timeframes.json'


def registered_active_metadata(active):
    """Validate the exact metadata extension without reading 1s payloads."""
    index=read_record(contained(active,'timeframes.json'))
    if index.get('schema')=='causal_timeframe_index/3':
        from .causal_timeframe_publication import validate_index
        validate_index(active.parents[2],index)
        return {'timeframes.json'}
    if index.get('schema')=='causal_timeframe_index/2':
        _validate_index_v2(active.parents[2],index)
        return {'timeframes.json'}
    if (index['schema']!='causal_timeframe_index/1'
            or index['status']!='SYNTHETIC_ONLY_NOT_ADOPTED'
            or index['legacy_1m']!={'path':'data/causally_gated_normalized/_control/catalog.json',
                                  'sha256':sha256_file(contained(active,'catalog.json'))}):
        raise IntegrityError('timeframe index or legacy binding differs')
    expected={'timeframes.json'}
    keys=set()
    for entry in index['entries']:
        key=(entry['market'],entry['year'],entry['timeframe'])
        relative=f"causally_gated_normalized/{key[0]}/{key[1]}/1s/manifest.json"
        if key in keys or key[2]!='1s' or entry['path']!='data/active/'+relative:
            raise IntegrityError('duplicate or noncanonical timeframe metadata')
        keys.add(key)
        path=contained(active,relative)
        manifest=read_record(path)
        if (sha256_file(path)!=entry['sha256']
                or (manifest['market'],manifest['year'],manifest['timeframe'])!=key
                or manifest['schema']!='causal_timeframe_reference/1'
                or manifest['status']!='SYNTHETIC_REFERENCE_NOT_ACTIVE'
                or manifest['permitted_uses']!=['SYNTHETIC_TEST']
                or not 0<manifest['start_ns']<manifest['end_ns']<=CUTOFF):
            raise IntegrityError('registered timeframe metadata differs')
        expected.add(relative)
    return expected


def prepare_reference(root, candidate_relative, *, year=None):
    """Return metadata only. Existing normalized payloads are never copied."""
    if not candidate_relative.startswith('data/causally_gated_normalized/'):
        raise IntegrityError('candidate must be in the declared normalized subtree')
    candidate_path=contained(root,candidate_relative)
    candidate=read_record(candidate_path)
    spec=Spec(**candidate['spec'])
    year=stamp(spec.start_ns).year if year is None else year
    if type(year) is not int or not stamp(spec.start_ns).year <= year <= stamp(spec.end_ns-1).year:
        raise IntegrityError('reference year outside candidate')
    cert_path=candidate_path.parent/'certificate.json'
    cert=read_record(cert_path)
    finishes=[read_record(p) for p in (candidate_path.parent/'runs').glob('*.finish.json')]
    if not any(r['kind']=='CERTIFY' and r['status']=='COMPLETE' and r['spec_id']==spec.id
               and r['result_id']==cert['id'] for r in finishes):
        raise IntegrityError('no completed resource-accounted certification')
    if (candidate['spec_id']!=spec.id or candidate['contract_id']!=CONTRACT_ID
            or cert['spec_id']!=spec.id or cert['candidate_id']!=candidate['id']
            or cert['status']!='SYNTHETIC_ONLY_NOT_HISTORICAL_CERTIFICATION'
            or len(cert['passes'])!=2 or cert['passes'][0]!=cert['passes'][1]
            or cert['passes'][0]['rows']!=candidate['observations']
            or any(cert['authority'].values())):
        raise IntegrityError('candidate certificate/bindings differ')
    files=[]
    coverage=[]
    for month,seal_id in zip(candidate['months'],candidate['seals'],strict=True):
        seal_path=candidate_path.parent/f'{month}.seal.json'
        seal=read_record(seal_path)
        if seal['id']!=seal_id or seal['spec_id']!=spec.id or seal['month']!=month or seal['timeframe']!='1s':
            raise IntegrityError('month seal differs')
        if month[:4]!=str(year):
            continue
        coverage.extend(seal['coverage'])
        if seal['files']:
            for name,f in seal['files'].items():
                relative=(PurePosixPath(candidate_relative).parent / seal['directory'] /f['filename']).as_posix()
                path=contained(root,relative)
                if sha256_file(path)!=f['sha256'] or path.stat().st_size!=f['size']:
                    raise IntegrityError('certified payload changed')
                if name=='observations':
                    files.append({'path':relative,'sha256':f['sha256'],'size':f['size'],
                        'month':month,'rows':sum(d['observations'] for d in seal['coverage'])})
    if not files or not candidate['observations']:
        raise IntegrityError('empty/unavailable candidate is not resolvable')
    return signed({'schema':'causal_timeframe_reference/1','timeframe':'1s','market':spec.market,
        'year':year,'start_ns':min(d['start_ns'] for d in coverage),'end_ns':max(d['end_ns'] for d in coverage),
        'spec_id':spec.id,'candidate_path':candidate_relative,'candidate_id':candidate['id'],
        'certificate_sha256':sha256_file(cert_path),'files':files,'coverage':coverage,
        'permitted_uses':['SYNTHETIC_TEST'],'status':'SYNTHETIC_REFERENCE_NOT_ACTIVE'})


def prepare_index(root, references):
    """Build a prospective index in memory; publication/adoption is absent."""
    entries=[]
    for relative in references:
        if not relative.startswith('data/active/causally_gated_normalized/') or not relative.endswith('/1s/manifest.json'):
            raise IntegrityError('reference path is not a timeframe manifest')
        value=read_record(contained(root,relative))
        if prepare_reference(root,value['candidate_path'],year=value['year'])!=value:
            raise IntegrityError('reference changed')
        entries.append({'market':value['market'],'year':value['year'],'timeframe':'1s',
                        'path':relative,'sha256':sha256_file(contained(root,relative))})
    entries.sort(key=lambda e:(e['market'],e['year'],e['timeframe']))
    keys=[(e['market'],e['year'],e['timeframe']) for e in entries]
    if len(set(keys))!=len(keys): raise IntegrityError('duplicate market/year/timeframe')
    legacy=contained(root,'data/causally_gated_normalized/_control/catalog.json')
    return signed({'schema':'causal_timeframe_index/1','status':'SYNTHETIC_ONLY_NOT_ADOPTED',
        'legacy_1m':{'path':'data/causally_gated_normalized/_control/catalog.json','sha256':sha256_file(legacy)},'entries':entries})


def resolve_partitions(*, repository_root, market, year, timeframe, purpose, require_status=False):
    from .active_data_view import resolve
    if timeframe not in ('1m','1s','1h','1d'): raise ContractError('unsupported explicit primary timeframe')
    root=Path(repository_root)
    index_path=contained(root,INDEX)
    if not index_path.exists():
        raise IntegrityError('Relocated timeframe authority is missing; no fallback')
    index=read_record(index_path)
    if index.get('schema') != 'causal_timeframe_index/3':
        raise UnauthorizedOperation('Relocated controls require the certified annual index')
    if index.get('schema')=='causal_timeframe_index/3':
        from .causal_timeframe_publication import validate_index, resolve_partitions as resolve_v3
        validate_index(root,index)
        if timeframe=='1m':
            return (resolve(repository_root=root,market=market,year=year,purpose=purpose,require_status=require_status),)
        return resolve_v3(root,index,market=market,year=year,timeframe=timeframe,purpose=purpose,require_status=require_status)
    if timeframe in ('1h','1d'): raise UnauthorizedOperation('requested timeframe requires v3 publication')
    if index.get('schema')=='causal_timeframe_index/2':
        _validate_index_v2(root,index)
        if timeframe=='1m':
            result=(resolve(repository_root=root,market=market,year=year,purpose=purpose,require_status=require_status),)
        else:
            if require_status: raise UnauthorizedOperation('1s status-qualified research is not admitted')
            entries=[e for e in index['entries'] if e['market']==market and e['month'][:4]==str(year)]
            if not entries or any(e['disposition']!='CERTIFIED' for e in entries):
                raise UnauthorizedOperation('requested 1s coverage is absent, unavailable or quarantined')
            result=[]
            for entry in entries:
                release=_validate_release(root,entry)
                allowed='SYNTHETIC_TEST' if release['candidate']['spec']['synthetic_only'] else 'LINEAGE_AUDIT'
                if purpose!=allowed: raise UnauthorizedOperation('1s purpose is not admitted')
                seal=release['seal']
                file=seal['files']['observations']
                payload=contained(root,entry['directory']+'/'+file['filename'])
                with pq.ParquetFile(payload) as parquet:
                    if (not parquet.schema_arrow.equals(SCHEMAS['observations'],check_metadata=True)
                            or parquet.metadata.num_rows!=sum(d['observations'] for d in seal['coverage'])):
                        raise IntegrityError('published observation schema or rows differ')
                    for group in range(parquet.metadata.num_row_groups):
                        stats=parquet.metadata.row_group(group).column(SCHEMAS['observations'].get_field_index('source_cadence')).statistics
                        if stats is None or stats.min!='1s' or stats.max!='1s' or stats.null_count:
                            raise IntegrityError('published cadence differs')
                result.append(payload)
            result=tuple(result)
        if read_record(index_path)!=index: raise IntegrityError('timeframe index changed during resolution')
        return result
    registered_active_metadata(contained(root,'data/causally_gated_normalized/_control'))
    if index['schema']!='causal_timeframe_index/1' or index['status']!='SYNTHETIC_ONLY_NOT_ADOPTED':
        raise UnauthorizedOperation('timeframe index is not a supported adopted revision')
    legacy=index['legacy_1m']
    if legacy['path']!='data/causally_gated_normalized/_control/catalog.json' or sha256_file(contained(root,legacy['path']))!=legacy['sha256']:
        raise IntegrityError('legacy 1m catalog drifted')
    if timeframe=='1m':
        result=resolve(repository_root=root,market=market,year=year,purpose=purpose,require_status=require_status)
        if read_record(index_path)!=index: raise IntegrityError('timeframe index changed during resolution')
        return (result,)
    if purpose!='SYNTHETIC_TEST' or require_status:
        raise UnauthorizedOperation('1s historical/research access is not adopted')
    keys=[(e['market'],e['year'],e['timeframe']) for e in index['entries']]
    if len(set(keys))!=len(keys): raise IntegrityError('duplicate timeframe entries')
    matches=[e for e in index['entries'] if (e['market'],e['year'],e['timeframe'])==(market,year,timeframe)]
    if len(matches)!=1: raise IntegrityError('exact timeframe absent or duplicated; no fallback')
    entry=matches[0]
    expected=f'data/active/causally_gated_normalized/{market}/{year}/1s/manifest.json'
    if entry['path']!=expected: raise IntegrityError('timeframe manifest path differs')
    path=contained(root,entry['path'])
    if sha256_file(path)!=entry['sha256']: raise IntegrityError('timeframe manifest changed')
    manifest=read_record(path)
    if (manifest['market'],manifest['year'],manifest['timeframe'])!=(market,year,timeframe):
        raise IntegrityError('timeframe selection differs')
    if not manifest['start_ns']<manifest['end_ns']<=CUTOFF or manifest['permitted_uses']!=['SYNTHETIC_TEST']:
        raise UnauthorizedOperation('protected or unauthorized coverage')
    if prepare_reference(root,manifest['candidate_path'],year=year)!=manifest:
        raise IntegrityError('certificate, membership or source drift')
    result=[]
    previous=None
    for f in manifest['files']:
        if f['month'][:4]!=str(year) or previous is not None and f['month']<=previous:
            raise IntegrityError('partition order/year differs')
        previous=f['month']
        payload=contained(root,f['path'])
        with pq.ParquetFile(payload) as parquet:
            if not parquet.schema_arrow.equals(SCHEMAS['observations'],check_metadata=True) or parquet.metadata.num_rows!=f['rows']:
                raise IntegrityError('observation schema/count differs')
            for i in range(parquet.metadata.num_row_groups):
                column=parquet.metadata.row_group(i).column(SCHEMAS['observations'].get_field_index('source_cadence'))
                stats=column.statistics
                if stats is None or stats.min!='1s' or stats.max!='1s' or stats.null_count:
                    raise IntegrityError('payload cadence is absent or mixed')
        result.append(payload)
    # Re-read the commit marker to detect concurrent publication during resolution.
    if read_record(index_path)!=index: raise IntegrityError('timeframe index changed during resolution')
    return tuple(result)


def _identity(value):
    if value.get('id')!=sha256_json({k:v for k,v in value.items() if k!='id'}):
        raise IntegrityError('publication object identity differs')


def _validate_index_v2(root,index):
    """Only the atomic index is installed in active; immutable manifests stay with releases.

    This keeps an interrupted manifest installation invisible to the legacy exact-tree reader.
    """
    _identity(index)
    if (index.get('status')!='PUBLISHED_CAUSAL_RECONSTRUCTION_ONLY'
            or index['legacy_1m']!={'path':'data/causally_gated_normalized/_control/catalog.json','sha256':sha256_file(contained(root,'data/causally_gated_normalized/_control/catalog.json'))}):
        raise IntegrityError('published timeframe index or predecessor 1m binding differs')
    expected=index['expected']
    keys=[(e['market'],e['month']) for e in index['entries']]
    if keys!=sorted(set(keys)) or expected!=[list(k) for k in keys]:
        raise IntegrityError('campaign coverage is incomplete or duplicated')
    from .causal_1s import V10_CHECKPOINT_MARKETS
    for e in index['entries']:
        from datetime import datetime,timezone
        try:
            start=datetime.strptime(e['month'],'%Y-%m').replace(tzinfo=timezone.utc)
            end=datetime(start.year+(start.month==12),start.month%12+1,1,tzinfo=timezone.utc)
        except (TypeError,ValueError) as exc: raise IntegrityError('invalid campaign month') from exc
        if (e['market'] not in V10_CHECKPOINT_MARKETS or e['timeframe']!='1s'
                or start.strftime('%Y-%m')!=e['month'] or int(start.timestamp())*1_000_000_000>=CUTOFF
                or e['disposition'] not in ('CERTIFIED','UNAVAILABLE','QUARANTINED')):
            raise IntegrityError('campaign cohort/timeframe/disposition differs')
        if e['disposition']=='CERTIFIED':
            directory=f"data/causally_gated_normalized/{e['market']}/{e['month'][:4]}/1s/{e['month']}/{e['spec_id']}"
            if e['directory']!=directory or e['manifest']!=directory+'/manifest.json':
                raise IntegrityError('noncanonical monthly release location')
            path=contained(root,e['manifest'])
            if sha256_file(path)!=e['manifest_sha256']: raise IntegrityError('monthly manifest changed')
        elif not e.get('reason'):
            raise IntegrityError('unavailable or quarantined period needs an explicit reason')


def _validate_release(root,entry):
    from .causal_timeframe_storage import file_inventory
    release=read_record(contained(root,entry['manifest']))
    candidate,cert,seal=release['candidate'],release['certificate'],release['seal']
    for value in (candidate,cert,seal,release['certification_finish']): _identity(value)
    spec=Spec(**candidate['spec'])
    status='SYNTHETIC_ONLY_NOT_HISTORICAL_CERTIFICATION' if spec.synthetic_only else 'HISTORICAL_RECONSTRUCTION_CERTIFIED_NOT_ACTIVE'
    finish=release['certification_finish']
    if (candidate['spec_id']!=spec.id or cert['spec_id']!=spec.id or entry['spec_id']!=spec.id
            or spec.market!=entry['market'] or cert['passes'][0]['months']!=candidate['months']
            or cert['candidate_id']!=candidate['id'] or cert['status']!=status
            or cert['contract_id']!=CONTRACT_ID or any(cert['authority'].values())
            or len(cert['passes'])!=2 or cert['passes'][0]!=cert['passes'][1]
            or cert['passes'][0]['rows']!=candidate['observations']
            or seal['id'] not in candidate['seals'] or seal['month']!=entry['month']
            or seal['spec_id']!=spec.id or seal['timeframe']!='1s'
            or finish['kind']!='CERTIFY' or finish['status']!='COMPLETE'
            or finish['result_id']!=cert['id'] or finish['spec_id']!=spec.id):
        raise IntegrityError('published certification, complete replay or scope differs')
    directory=contained(root,entry['directory'])
    from .causal_observation_parquet import FILENAMES
    if set(seal['files'])!=set(FILENAMES): raise IntegrityError('five monthly tables required')
    if {p.name for p in directory.iterdir()}!=set(FILENAMES.values())|{'manifest.json'}:
        raise IntegrityError('undeclared monthly payload or missing file')
    for f in seal['files'].values():
        path=contained(directory,f['filename'])
        if path.stat().st_size!=f['size'] or sha256_file(path)!=f['sha256']:
            raise IntegrityError('published monthly payload changed')
    if not spec.synthetic_only:
        supervision=release['supervision']
        _identity(supervision)
        if (supervision['status']!='COMPLETE' or supervision['spec_id']!=spec.id
                or supervision['certificate_id']!=cert['id']):
            raise IntegrityError('complete native supervision required')
    return release


def prepare_publication(root, *, candidates, dispositions=(), expected):
    """Exact complete campaign proposal; reads candidates, writes nothing."""
    from .causal_timeframe_storage import file_inventory
    root=Path(root)
    entries=[]
    moves=[]
    source_ids=set()
    for relative in candidates:
        if not relative.startswith('state/unpublished_evidence/') or not relative.endswith('/candidate.json'):
            raise IntegrityError('publication requires task-owned candidate metadata')
        candidate_path=contained(root,relative)
        candidate=read_record(candidate_path)
        spec=Spec(**candidate['spec'])
        cert=read_record(candidate_path.parent/'certificate.json')
        if (candidate['spec_id']!=spec.id or cert['candidate_id']!=candidate['id']
                or cert['spec_id']!=spec.id or any(cert['authority'].values())
                or len(cert['passes'])!=2 or cert['passes'][0]!=cert['passes'][1]
                or cert['passes'][0]['rows']!=candidate['observations']):
            raise IntegrityError('candidate is not independently certified')
        source_ids.add((spec.source_contract_id,spec.source_release_id,spec.synthetic_only))
        finishes=[read_record(p) for p in (candidate_path.parent/'runs').glob('*.finish.json')]
        finishes=[f for f in finishes if f['kind']=='CERTIFY' and f['status']=='COMPLETE'
                  and f['result_id']==cert['id'] and f['spec_id']==spec.id]
        if not finishes: raise IntegrityError('resource-accounted certification absent')
        from .causal_1s_native import completed_supervision
        supervision=None if spec.synthetic_only else completed_supervision(candidate_path.parent,spec.id,cert['id'])
        if supervision is not None and supervision['status']!='COMPLETE':
            raise IntegrityError('native operation did not complete within supervision')
        for month,seal_id in zip(candidate['months'],candidate['seals'],strict=True):
            seal=read_record(candidate_path.parent/f'{month}.seal.json')
            if seal['id']!=seal_id or seal['spec_id']!=spec.id or seal['month']!=month:
                raise IntegrityError('candidate seal membership differs')
            if not seal['files']:
                entries.append({'market':spec.market,'month':month,'timeframe':'1s',
                    'disposition':'UNAVAILABLE','reason':'CERTIFIED_SOURCE_UNAVAILABLE','seal_id':seal['id']})
                continue
            source=(PurePosixPath(relative).parent/seal['directory']).as_posix()
            directory=f'data/causally_gated_normalized/{spec.market}/{month[:4]}/1s/{month}/{spec.id}'
            payloads=contained(root,source)
            if payloads.exists():
                if file_inventory(payloads)!=seal['files']: raise IntegrityError('candidate payload differs')
            elif not contained(root,directory).exists():
                raise IntegrityError('candidate payload is missing')
            release=signed({'schema':'causal_1s_published_month/1','candidate':candidate,'certificate':cert,
                'seal':seal,'certification_finish':finishes[-1],'supervision':supervision})
            entry={'market':spec.market,'month':month,'timeframe':'1s','disposition':'CERTIFIED',
                'spec_id':spec.id,'directory':directory,'manifest':directory+'/manifest.json',
                'manifest_sha256':sha256(canonical_bytes(release)+b'\n').hexdigest()}
            entries.append(entry)
            moves.append({'source':source,'entry':entry,'release':release})
    if len(source_ids)>1: raise IntegrityError('mixed source releases or execution modes')
    entries.extend(dispositions)
    entries.sort(key=lambda e:(e['market'],e['month']))
    keys=[(e['market'],e['month']) for e in entries]
    if keys!=sorted(set(keys)) or sorted([list(k) for k in expected])!=[list(k) for k in keys]:
        raise IntegrityError('publication must account for every declared market/month exactly once')
    predecessor=contained(root,INDEX)
    return signed({'schema':'causal_1s_publication/1','expected':[list(k) for k in keys],
        'entries':entries,'moves':moves,'candidates':list(candidates),'dispositions':list(dispositions),
        'predecessor_sha256':sha256_file(predecessor) if predecessor.exists() else None,
        'legacy_1m':{'path':'data/causally_gated_normalized/_control/catalog.json','sha256':sha256_file(contained(root,'data/causally_gated_normalized/_control/catalog.json'))}})


def publish(root,plan,receipt,limits):
    """Separately approved same-volume moves; the active index is the last write."""
    import os
    from .boundary import RepoBoundary,OperationReceipt,OperationClassification
    from .locking import FileLease
    from .active_data_view import PUBLICATION_LOCK
    from .causal_timeframe_storage import Budget
    from .canonical import canonical_bytes
    root=Path(root)
    _identity(plan)
    if type(receipt) is not OperationReceipt: raise UnauthorizedOperation('exact publication receipt required')
    required={'publication_id':plan['id'],'limits_id':sha256_json(asdict(limits))}
    approval_keys={'approval_command','approval_plan_id','approval_plan_sha256'}
    if set(dict(receipt.scope))-set(required)-approval_keys: raise UnauthorizedOperation('extra publication scope')
    required.update({k:v for k,v in receipt.scope if k in approval_keys})
    kwargs=dict(operation='PUBLISH_CAUSAL_1S_RELEASE_ONCE',
        classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,required_scope=required)
    boundary=RepoBoundary(root)
    receipt.verify(boundary,**kwargs)
    journal=contained(root,'state/unpublished_evidence/causal_1s_publications/'+plan['id'])
    journal.mkdir(parents=True,exist_ok=True)
    with FileLease(root/PUBLICATION_LOCK):
        marker=journal/'started.json'
        if marker.exists():
            if read_record(marker)['receipt_id']!=receipt.receipt_id: raise UnauthorizedOperation('publication recovery receipt differs')
            receipt.assert_consumed(boundary,**kwargs)
        else:
            receipt.consume(boundary,**kwargs)
        index_path=contained(root,INDEX)
        index=signed({'schema':'causal_timeframe_index/2','status':'PUBLISHED_CAUSAL_RECONSTRUCTION_ONLY',
            'legacy_1m':plan['legacy_1m'],'expected':plan['expected'],'entries':plan['entries'],'publication_id':plan['id']})
        if index_path.exists() and read_record(index_path)==index:
            _validate_index_v2(root,index)
            return index
        if (sha256_file(index_path) if index_path.exists() else None)!=plan['predecessor_sha256']:
            raise IntegrityError('publication predecessor drift')
        if prepare_publication(root,candidates=plan['candidates'],dispositions=plan['dispositions'],expected=plan['expected'])!=plan:
            raise IntegrityError('publication inputs changed')
        starts=sorted(journal.glob('*.start.json'))
        previous=0
        for start in starts:
            finish=start.with_name(start.name.replace('.start.json','.finish.json'))
            previous+=(read_record(finish)['written_before_finish']+finish.stat().st_size
                       if finish.exists() else read_record(start)['maximum_written_bytes'])
        if previous>=limits.maximum_written_bytes:
            raise IntegrityError('publication recovery exhausted cumulative write cap')
        budget=Budget(journal,replace(limits,maximum_written_bytes=limits.maximum_written_bytes-previous))
        budget.written=(root/PUBLICATION_LOCK).stat().st_size
        budget.terminal_reserve=65536
        number=len(starts)+1
        budget.save(journal/f'{number:06d}.start.json',signed({'maximum_written_bytes':budget.limits.maximum_written_bytes}))
        finish_path=journal/f'{number:06d}.finish.json'
        try:
            if not marker.exists():
                budget.save(marker,signed({'receipt_id':receipt.receipt_id,'plan_id':plan['id']}))
            if index_path.exists() and not (journal/'previous_index.json').exists():
                with budget.open(journal/'previous_index.json') as f: f.write(index_path.read_bytes())
            for move in plan['moves']:
                receipt.assert_consumed(boundary,**kwargs)
                source=contained(root,move['source'])
                destination=contained(root,move['entry']['directory'])
                budget.check()
                destination.parent.mkdir(parents=True,exist_ok=True)
                if source.exists():
                    if destination.exists() or source.stat().st_dev!=destination.parent.stat().st_dev:
                        raise IntegrityError('rename requires absent destination on the source volume')
                    os.rename(source,destination)
                manifest=destination/'manifest.json'
                if not manifest.exists(): budget.save(manifest,move['release'])
                _validate_release(root,move['entry'])
            _validate_index_v2(root,index)
            if sha256_file(contained(root,'data/causally_gated_normalized/_control/catalog.json'))!=plan['legacy_1m']['sha256']:
                raise IntegrityError('legacy catalog drift during publication')
            temporary=journal/'next_index.json'
            if temporary.exists():
                if read_record(temporary)!=index: raise IntegrityError('interrupted index differs')
            else: budget.save(temporary,index)
            budget.check()
            receipt.assert_consumed(boundary,**kwargs)
            if (sha256_file(index_path) if index_path.exists() else None)!=plan['predecessor_sha256']:
                raise IntegrityError('predecessor changed before commit')
            budget.terminal_reserve=0
            budget.save(finish_path,signed({'status':'READY_FOR_INDEX_COMMIT',
                'written_before_finish':budget.written,'previous_attempt_written_bytes':previous,
                'resources':budget.record(),'payload_bytes_copied':0}))
            budget.check()
            os.replace(temporary,index_path)
            return index
        finally:
            budget.finalizing=True
            budget.terminal_reserve=0
            if not finish_path.exists():
                budget.save(finish_path,signed({'status':'FAILED',
                    'written_before_finish':budget.written,'previous_attempt_written_bytes':previous,
                    'resources':budget.record(),'payload_bytes_copied':0}))
