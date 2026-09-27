"""Task-only bounded physical-layout diagnostic. No bar decoding or publication."""
from pathlib import Path
import json
import time

from .annual_foundation_storage import BASE, CUTOFF, require
from .canonical import sha256_file
from .causal_observation_parquet import FILENAMES, SCHEMAS
from .causal_timeframe_storage import contained, read_record, signed, Budget, Limits, allocation

OUTPUT = BASE+'/integration_footer_audit_v1'


def census(root, inventory, budget):
    """Check every declared footer once; hashes before/after, zero decoded bars."""
    import pyarrow.parquet as pq
    from datetime import datetime, timezone
    expected = set()
    for unit in inventory['units']:
        for source in unit['sources']:
            if unit['timeframe']=='1m':
                left,right=source['interval'].split('_')
                end=int(datetime.fromisoformat(right.replace('Z','+00:00')).replace(tzinfo=timezone.utc).timestamp())*10**9
            else:
                end=source['end_ns']
            require(0<end<=CUTOFF, 'Protected physical source refused before opening')
            for name,relative in source['files'].items():
                require(name in FILENAMES and relative==source['directory']+'/'+FILENAMES[name]
                    and relative.startswith('data/causally_gated_normalized/'), 'Unexpected physical source')
                expected.add(relative)
    require(expected==set(inventory['physical_files']), 'Inventory membership differs')
    for relative,digest in inventory['metadata_bindings'].items():
        require(budget.hash(contained(root,relative))==digest,'Structural metadata changed: '+relative)
    measured={}
    for relative,info in inventory['physical_files'].items():
        budget.check()
        path=contained(root,relative)
        require(path.stat().st_size==info['size'] and budget.hash(path)==info['sha256'], 'Source bytes changed')
        name=Path(relative).stem
        with pq.ParquetFile(path) as file:
            require(file.schema_arrow.equals(SCHEMAS[name],check_metadata=True),'Source schema differs')
            groups=file.metadata.num_row_groups
            sizes=[(file.metadata.row_group(i).total_byte_size,file.metadata.row_group(i).num_rows)
                   for i in range(groups)]
            measured[relative]=dict(rows=file.metadata.num_rows,groups=groups,
                maximum_uncompressed_group_bytes=max((s[0] for s in sizes),default=0),
                maximum_group_rows=max((s[1] for s in sizes),default=0))
        require(budget.hash(path)==info['sha256'], 'Source changed during footer read')
    units=[]
    for unit in inventory['units']:
        if not unit['sources']: continue
        profile=dict(timeframe=unit['timeframe'],market=unit['market'],year=unit['year'],
                     groups=0,rows=0,maximum_uncompressed_group_bytes=0,maximum_group_rows=0)
        cadence=[]
        for source in unit['sources']:
            entries={n:measured[p] for n,p in source['files'].items()}
            n=entries['observations']['groups']
            require(n>0 and all(e['groups'] in ((0,n) if name=='cadence' else (n,))
                               for name,e in entries.items()), 'Ambiguous source group mapping')
            for name,e in entries.items():
                retained=source.get('metadata')
                if retained is not None:
                    require(e['rows']==retained[name]['rows'] and e['groups']==retained[name]['row_groups'],
                            'Footer differs from retained certification metadata')
                profile['maximum_uncompressed_group_bytes']=max(profile['maximum_uncompressed_group_bytes'],e['maximum_uncompressed_group_bytes'])
                profile['maximum_group_rows']=max(profile['maximum_group_rows'],e['maximum_group_rows'])
            profile['groups']+=n; profile['rows']+=entries['observations']['rows']
            cadence.append(entries['cadence']['rows']>0)
        require(profile['rows']==unit['rows'],'Annual row count differs from retained evidence')
        profile['mixed_empty_cadence']=len(set(cadence))>1
        profile['within_existing_group_limits']=(profile['maximum_uncompressed_group_bytes']<=64*2**20
                                                and profile['maximum_group_rows']<=250000)
        units.append(profile)
    for relative,digest in inventory['metadata_bindings'].items():
        require(budget.hash(contained(root,relative))==digest,'Structural metadata changed during audit')
    return dict(units=units,files=len(measured),decoded_observations=0,
        largest_groups=sorted(measured.items(),key=lambda item:item[1]['maximum_uncompressed_group_bytes'],reverse=True)[:10],
        all_within_existing_group_limits=all(u['within_existing_group_limits'] for u in units),
        unproven='Footer bounds do not measure Python decoder memory or source admission')


def worker():
    """Private supervised stdin protocol, not a repository execution CLI."""
    import sys
    from .daily_identity_rollout import verify_job, memory
    request=json.loads(sys.stdin.buffer.readline(2**20))
    root=Path.cwd().resolve()
    scope_path=contained(root,request['scope'])
    scope=read_record(scope_path)
    auth=read_record(contained(root,request['authorization']))
    require(scope['schema']=='annual_footer_audit_scope/1' and scope['output']==OUTPUT
        and scope['implementation_sha256']==sha256_file(Path(__file__))
        and auth['status']=='OWNER_APPROVED_IN_THIS_TASK' and auth['scope_id']==scope['id']
        and auth['scope_sha256']==sha256_file(scope_path) and time.time()<auth['deadline_epoch'],
        'Exact current diagnostic approval required')
    verify_job(request['supervision_job_name'],scope['limits']['maximum_memory_bytes'])
    for relative,digest in scope['implementation'].items():
        require(sha256_file(contained(root,relative))==digest,'Diagnostic implementation changed')
    directory=contained(root,OUTPUT)
    require(read_record(directory/'owner.json')['scope_id']==scope['id'],'Attempt ownership differs')
    budget=Budget(directory,Limits(**scope['limits'])); budget.written=request['ledger_seed']
    budget.terminal_reserve=2*2**20
    result=dict(status='FAILED_RECOVERY_PHASE',scope_id=scope['id'],phase='footers')
    try:
        ref=scope['inventory']; path=contained(root,ref['path'])
        require(budget.hash(path)==ref['sha256'],'Approved inventory changed')
        result.update(census(root,read_record(path),budget))
        require(budget.hash(path)==ref['sha256'],'Approved inventory changed during audit')
        require(time.time()<auth['deadline_epoch'],'Whole-operation deadline exceeded')
        budget.check()
        result['status']='PASS_RECOVERY_PHASE'
    except BaseException as exc:
        result.update(error_type=type(exc).__name__,error=str(exc)[:2000])
    finally:
        result['resources']=budget.record(); result['memory']=memory()
        result['new_application_bytes_before_terminal']=budget.written-request['ledger_seed']
        budget.finalizing=True; budget.terminal_reserve=0
        budget.save(directory/'result.json',signed(result))
    if result['status']!='PASS_RECOVERY_PHASE': raise SystemExit(1)


def run(root, *, scope_path, authorization_path):
    """One approved diagnostic, no retry, existing sources/controls never written."""
    import sys
    from .daily_identity_rollout import supervise, resource_gate
    root=Path(root).resolve()
    scope=read_record(contained(root,scope_path)); auth=read_record(contained(root,authorization_path))
    require(scope['schema']=='annual_footer_audit_scope/1' and scope['output']==OUTPUT
        and scope['implementation_sha256']==sha256_file(Path(__file__))
        and auth['status']=='OWNER_APPROVED_IN_THIS_TASK' and auth['scope_id']==scope['id']
        and auth['scope_sha256']==sha256_file(contained(root,scope_path))
        and 0<auth['deadline_epoch']-auth['started_epoch']<=5400 and time.time()<auth['deadline_epoch'],
        'Bounded current approval required')
    limits=Limits(**scope['limits'])
    require(limits.maximum_seconds<=5400 and limits.maximum_memory_bytes<=5*2**30
        and limits.maximum_written_bytes<=2*2**30 and limits.maximum_files<=2000
        and limits.minimum_free_bytes>=64*2**30,'Diagnostic exceeds approved ceilings')
    directory=contained(root,OUTPUT); directory.mkdir(parents=True,exist_ok=False)
    budget=Budget(directory,limits); budget.terminal_reserve=2*2**20
    budget.written=scope['known_prior_measured_writes']+contained(root,scope_path).stat().st_size+contained(root,authorization_path).stat().st_size
    summary=dict(status='FAILED_ANNUAL_FOOTER_AUDIT',scope_id=scope['id'])
    try:
        budget.save(directory/'owner.json',signed(dict(scope_id=scope['id'])))
        request=dict(phase='footers',scope=scope_path,authorization=authorization_path,
            deadline_epoch=auth['deadline_epoch'],ledger_seed=budget.written,result_path=OUTPUT+'/result.json')
        diagnostic=directory/'supervision.json'
        try:
            measured=supervise([sys.executable,'-c','from futures_rebuild.annual_foundation_footer_audit import worker; worker()'],
                                root,request,limits,diagnostic=diagnostic)
        finally:
            result_path=directory/'result.json'
            if result_path.exists():
                result=read_record(result_path)
                budget.written+=result['new_application_bytes_before_terminal']+result_path.stat().st_size
            else: summary['write_accounting']='INCOMPLETE_WORKER_TERMINAL_EVIDENCE'
            if diagnostic.exists(): budget.written+=diagnostic.stat().st_size
        summary['aggregate_peak_bound']=resource_gate(measured,coordinator_limit=960*2**20,worker_limit=5*2**30,
            aggregate_limit=6*2**30,deadline=auth['deadline_epoch'])
        summary['retained']=allocation(contained(root,BASE),2000)
        require(summary['retained']['retained_allocated_bytes']+2*2**20<=2*2**30,'Allocation limit exceeded')
        budget.check()
        summary['status']='PASS_READ_ONLY_FOOTER_AUDIT_NOT_MIGRATION_CERTIFICATION'
    except BaseException as exc:
        summary.update(error_type=type(exc).__name__,error=str(exc)[:2000])
    finally:
        budget.finalizing=True; budget.terminal_reserve=0
        summary['measured_write_ledger_before_terminal']=budget.written
        budget.save(directory/'completion.json',signed(summary))
    require(summary['status']=='PASS_READ_ONLY_FOOTER_AUDIT_NOT_MIGRATION_CERTIFICATION',summary.get('error','Diagnostic failed'))
    return summary
