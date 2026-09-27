"""Task-only annual packaging pilot. No CLI, source admission or publication."""
import json
import sys
import time
from pathlib import Path

BASE = 'state/unpublished_evidence/annual_foundation_migration_v1'
OUTPUT = BASE + '/historical_pilot_v1'


def compare_sources(root, directory, unit, *, manifest_sha256, budget, compact=None):
    """Read approved originals independently of digests asserted by the packer."""
    from contextlib import ExitStack
    from itertools import zip_longest
    import pyarrow.parquet as pq
    from .annual_foundation_storage import _verify_sources, _group, _read_candidate, require
    from .causal_observation_parquet import FILENAMES, SCHEMAS, _decode_group, _decode_physical
    from .causal_timeframe_storage import contained, read_record
    from .active_data_certification import compare_parquet_canonical
    _verify_sources(root, unit['sources'], unit['year'], budget)
    manifest = read_record(directory/'manifest.json')
    require(all(manifest[k] == unit[k] for k in ('market','year','timeframe','sources','coverage')),
            'Candidate changes approved membership')
    def originals():
        for source in unit['sources']:
            with ExitStack() as stack:
                files = {n:stack.enter_context(pq.ParquetFile(contained(root,source['directory']+'/'+f)))
                         for n,f in FILENAMES.items()}
                count=files['observations'].metadata.num_row_groups
                require(count>0,'Empty certified source')
                for name,file in files.items():
                    require(file.schema_arrow.equals(SCHEMAS[name],check_metadata=True)
                        and file.metadata.num_row_groups in ((0,count) if name=='cadence' else (count,)),
                        'Original source schema/group alignment differs')
                for index in range(count):
                    budget.check()
                    logical={}
                    for name,file in files.items():
                        physical=[] if not file.metadata.num_row_groups else [
                            _decode_physical(name,r) for r in _group(file,index,budget.limits.maximum_group_bytes).to_pylist()]
                        logical[name]=_decode_group(name,physical,logical.get('observations',()))
                    yield source['month'],logical
    actual=_read_candidate(directory,manifest_sha256=manifest_sha256,
        months=[s['month'] for s in unit['sources']],maximum_group_bytes=budget.limits.maximum_group_bytes)
    counts={n:0 for n in FILENAMES}; groups=0
    for expected,observed in zip_longest(originals(),actual):
        require(expected is not None and observed is not None and expected==observed,
                'Independent source/candidate row or field mismatch')
        for name,rows in expected[1].items(): counts[name]+=len(rows)
        groups+=1
    equivalence=None
    if compact:
        path=contained(root,compact['path'])
        require(budget.hash(path)==compact['sha256'],'Active compact input changed')
        equivalence=compare_parquet_canonical(path,directory/'observations.parquet',
                                             expected_schema=SCHEMAS['observations'],batch_rows=4096)
        require(budget.hash(path)==compact['sha256'],'Compact input changed during comparison')
    _verify_sources(root,unit['sources'],unit['year'],budget)
    return dict(status='PASS_INDEPENDENT_FIVE_TABLE_EQUIVALENCE',groups=groups,counts=counts,
                compact_equivalence=equivalence,source_permissions_validated=False)


def _check_inputs(root, plan, budget):
    from .annual_foundation_storage import require, _period
    from .causal_timeframe_storage import contained
    from .causal_observation_parquet import FILENAMES
    selected=set()
    for unit in plan['units']:
        for source in unit['sources']:
            left,right=_period(source['month'])
            require(left<=source['start_ns']<source['end_ns']<=min(right,1752444000*10**9),
                    'Protected or undeclared physical interval')
            for name,filename in FILENAMES.items():
                path=source['directory']+'/'+filename
                require(path.startswith('data/causally_gated_normalized/') and path in plan['physical_payloads']
                    and all(source['files'][name][k]==plan['physical_payloads'][path][k] for k in ('size','sha256')),
                    'Physical source is not in approved inventory')
                selected.add(path)
    compact={p for p,i in plan['physical_payloads'].items() if i['role']=='CURRENT_MINUTE_EQUIVALENCE'}
    require(selected|compact==set(plan['physical_payloads']),'Unexplained physical input')
    for path,digest in plan['metadata_and_implementation_bindings'].items():
        require(budget.hash(contained(root,path))==digest,'Prepared metadata/implementation changed: '+path)
    if compact:
        from datetime import datetime, timezone
        catalog=json.loads(contained(root,'data/active/catalog.json').read_bytes())
        entries=[e for e in catalog['entries'] if e.get('parquet_path') in compact]
        require(len(entries)==len(compact),'Compact reference is not in current catalog')
        for entry in entries:
            end=int(datetime.fromisoformat(entry['coverage_end'].replace('Z','+00:00'))
                    .replace(tzinfo=timezone.utc).timestamp())*10**9
            require(end<=1752444000*10**9 and entry['parquet_sha256']==plan['physical_payloads'][entry['parquet_path']]['sha256'],
                    'Compact file crosses protected boundary or binding differs')
    for path,info in plan['physical_payloads'].items():
        target=contained(root,path)
        require(target.stat().st_size==info['size'] and budget.hash(target)==info['sha256'],
                'Approved payload changed: '+path)


def worker():
    """Private supervised stdin protocol; only an explicitly approved task calls it."""
    from .annual_foundation_storage import CandidateBudget, require, pack_candidate, read_candidate_month
    from .causal_timeframe_storage import Budget, Limits, contained, read_record, signed, allocation
    from .canonical import sha256_file
    from .daily_identity_rollout import verify_job, memory
    request=json.loads(sys.stdin.buffer.readline(2**20))
    root=Path.cwd().resolve(); scope=read_record(contained(root,request['scope']))
    auth=read_record(contained(root,request['authorization']))
    require(auth['status']=='OWNER_APPROVED_IN_THIS_TASK' and auth['scope_id']==scope['id']
            and auth['scope_sha256']==sha256_file(contained(root,request['scope']))
            and time.time()<auth['deadline_epoch'],'Missing, changed or expired pilot authorization')
    require(scope['output']==OUTPUT and request['phase'] in ('pack','compare'),'Unsupported pilot phase')
    verify_job(request['supervision_job_name'],scope['limits']['maximum_memory_bytes'])
    require(sha256_file(contained(root,scope['plan']['path']))==scope['plan']['sha256'],'Pilot inventory changed')
    plan=read_record(contained(root,scope['plan']['path']))
    directory=contained(root,OUTPUT)
    owner=read_record(directory/'owner.json')
    require(owner['scope_id']==scope['id'],'Attempt ownership differs')
    class OwnedBudget(CandidateBudget):
        def __init__(self):
            self.root=root
            Budget.__init__(self,directory,Limits(**scope['limits']))
            self.terminal_reserve=2*2**20
    budget=OwnedBudget()
    budget.written=request['ledger_seed']
    result=dict(status='FAILED_RECOVERY_PHASE',phase=request['phase'],scope_id=scope['id'],units=[])
    try:
        _check_inputs(root,plan,budget)
        prior=None
        if request['phase']=='compare':
            require(sha256_file(directory/'pack.json')==request['pack_sha256'],'Pack evidence changed')
            prior=read_record(directory/'pack.json')
            require(prior['status']=='PASS_RECOVERY_PHASE' and prior['scope_id']==scope['id'],'No passing pack')
        for number,unit in enumerate(plan['units']):
            start=time.monotonic()
            if prior is None:
                candidate,manifest=pack_candidate(root,**unit,budget=budget)
                record=dict(directory=candidate.relative_to(directory).as_posix(),
                            manifest_sha256=budget.hash(candidate/'manifest.json'))
            else:
                record=dict(prior['units'][number])
                candidate=contained(directory,record['directory'])
                compact=None
                if unit['timeframe']=='1m':
                    matches=[dict(path=p,**i) for p,i in plan['physical_payloads'].items()
                        if i['role']=='CURRENT_MINUTE_EQUIVALENCE' and f"/{unit['market']}/{unit['year']}/" in p]
                    require(len(matches)==1,'Missing/ambiguous compact equivalence input'); compact=matches[0]
                record['comparison']=compare_sources(root,candidate,unit,
                    manifest_sha256=record['manifest_sha256'],budget=budget,compact=compact)
                record['comparison_seconds']=time.monotonic()-start
                read_start=time.monotonic()
                record['selected_month_rows']=sum(len(g['observations']) for g in read_candidate_month(candidate,
                    manifest_sha256=record['manifest_sha256'],month=unit['sources'][0]['month']))
                record['selected_month_seconds']=time.monotonic()-read_start
            record['phase_seconds']=time.monotonic()-start
            result['units'].append(record)
            budget.check()
            require(allocation(contained(root,BASE),scope['limits']['maximum_files'])['retained_allocated_bytes']
                    <=scope['maximum_allocation_bytes'],'Retained allocation limit exceeded')
        _check_inputs(root,plan,budget)
        require(time.time()<auth['deadline_epoch'],'Whole pilot deadline exceeded')
        result['status']='PASS_RECOVERY_PHASE'
    except BaseException as exc:
        result.update(error_type=type(exc).__name__,error=str(exc)[:2000])
    finally:
        result['resources_before_terminal']=budget.record(); result['memory']=memory()
        result['new_application_bytes_before_terminal']=budget.written-request['ledger_seed']
        budget.finalizing=True; budget.terminal_reserve=0
        budget.save(directory/(request['phase']+'.json'),signed(result))
    if result['status']!='PASS_RECOVERY_PHASE': raise SystemExit(1)


def run(root, *, scope_path, authorization_path):
    """Owner-approved task entry; deliberately absent from repository CLIs."""
    from .causal_timeframe_storage import contained, read_record, Budget, Limits, signed, allocation
    from .canonical import canonical_bytes, sha256_file
    from .daily_identity_rollout import supervise, resource_gate, memory
    from .annual_foundation_storage import require
    root=Path(root).resolve()
    for path in (scope_path,authorization_path):
        require(contained(root,path).stat().st_size<=2**20,'Oversized coordinator record')
    scope=read_record(contained(root,scope_path))
    auth=read_record(contained(root,authorization_path))
    require(auth['status']=='OWNER_APPROVED_IN_THIS_TASK' and auth['scope_id']==scope['id']
        and auth['scope_sha256']==sha256_file(contained(root,scope_path))
        and 0<auth['deadline_epoch']-auth['started_epoch']<=1800
        and time.time()<auth['deadline_epoch'] and scope['output']==OUTPUT,'Invalid pilot approval')
    configured=scope['limits']
    require(configured['maximum_memory_bytes']<=5*2**30 and configured['maximum_seconds']<=1800
        and configured['maximum_written_bytes']<=2*2**30 and configured['maximum_files']<=2000
        and configured['maximum_group_bytes']<=64*2**20 and configured['minimum_free_bytes']>=64*2**30
        and scope['maximum_allocation_bytes']<=2*2**30,'Pilot limits exceed approved defaults')
    require(memory()['peak_bytes']<=960*2**20,'Coordinator memory limit exceeded before preparation')
    directory=contained(root,OUTPUT); directory.mkdir(parents=True,exist_ok=False)
    limits=Limits(**scope['limits']); bookkeeping=Budget(directory,limits)
    bookkeeping.written=scope['known_prior_measured_writes']+contained(root,scope_path).stat().st_size+contained(root,authorization_path).stat().st_size
    bookkeeping.terminal_reserve=2*2**20
    summary=dict(status='FAILED_PILOT',scope_id=scope['id'],phases=[])
    try:
        bookkeeping.save(directory/'owner.json',signed(dict(scope_id=scope['id'])))
        for phase in ('pack','compare'):
            request=dict(scope=scope_path,authorization=authorization_path,phase=phase,
                ledger_seed=bookkeeping.written,deadline_epoch=auth['deadline_epoch'],result_path=OUTPUT+'/'+phase+'.json')
            if phase=='compare': request['pack_sha256']=sha256_file(directory/'pack.json')
            diagnostic=directory/(phase+'_supervision.json')
            try:
                measured=supervise([sys.executable,'-c','from futures_rebuild.annual_foundation_pilot import worker; worker()'],
                    root,request,limits,diagnostic=diagnostic)
            finally:
                result_path=directory/(phase+'.json')
                if result_path.exists():
                    result=read_record(result_path)
                    bookkeeping.written+=result['new_application_bytes_before_terminal']+result_path.stat().st_size
                else:
                    # A killed worker cannot attest footer/write totals. Preserve the failure;
                    # no complete resource pass or automatic continuation is possible.
                    summary['write_accounting']='INCOMPLETE_WORKER_TERMINAL_EVIDENCE'
                if diagnostic.exists(): bookkeeping.written+=diagnostic.stat().st_size
            aggregate=resource_gate(measured,coordinator_limit=960*2**20,worker_limit=5*2**30,
                                   aggregate_limit=6*2**30,deadline=auth['deadline_epoch'])
            summary['phases'].append(dict(phase=phase,supervision_sha256=sha256_file(diagnostic),aggregate_peak_bound=aggregate))
            bookkeeping.check()
        retained=allocation(contained(root,BASE),limits.maximum_files)
        require(retained['retained_allocated_bytes']<=scope['maximum_allocation_bytes'],'Allocation limit exceeded')
        summary.update(status='PASS_ISOLATED_PILOT_NOT_ADMITTED',retained=retained)
    except BaseException as exc:
        summary.update(error_type=type(exc).__name__,error=str(exc)[:2000])
    finally:
        summary['measured_write_ledger_before_terminal']=bookkeeping.written
        summary['coordinator_memory']=memory()
        bookkeeping.finalizing=True; bookkeeping.terminal_reserve=0
        bookkeeping.save(directory/'completion.json',signed(summary))
    require(summary['status']=='PASS_ISOLATED_PILOT_NOT_ADMITTED',summary.get('error','Pilot failed'))
    return summary
