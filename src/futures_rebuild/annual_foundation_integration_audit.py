"""Approved-task annual reader checks; no migration or activation entry point."""
from contextlib import contextmanager, nullcontext
from pathlib import Path
import json
import sys
import time

from .canonical import sha256_file, sha256_json, io_path
from .causal_timeframe_storage import Budget, Limits, contained, read_record, signed, allocation
from .annual_foundation_release import require
from .annual_foundation_integration import projected, BASE as PREPARATION
from .annual_foundation_storage import BASE, CUTOFF
from .errors import UnauthorizedOperation, ContractError, IntegrityError

OUTPUT = BASE+'/reader_validation_v1'
PHASES = ('select','hourly_original','hourly_candidate','daily_original','daily_candidate','minute')


def validate_authorization(root, scope, authorization, scope_path):
    require(scope.get('schema')=='annual_reader_integration_scope/1' and scope.get('output')==OUTPUT
            and scope.get('active_control_replacements')==[] and scope.get('activation') is False,
            'Read-only reader scope required')
    limits=scope['limits']
    require(limits['maximum_seconds']<=5400 and limits['maximum_memory_bytes']<=5*2**30
            and limits['maximum_written_bytes']<=2*2**30 and limits['maximum_files']<=2000
            and limits['minimum_free_bytes']>=64*2**30 and scope['maximum_allocation_bytes']<=2*2**30,
            'Reader scope exceeds prepared ceilings')
    require(authorization.get('status')=='OWNER_APPROVED_IN_THIS_TASK'
            and authorization.get('scope_id')==scope['id']
            and authorization.get('scope_sha256')==sha256_file(contained(root,scope_path))
            and 0 < authorization['deadline_epoch']-authorization['started_epoch'] <= 5400
            and time.time()<authorization['deadline_epoch'], 'Exact bounded historical approval required')
    for path,digest in scope['implementation'].items():
        require(sha256_file(contained(root,path))==digest, 'Prepared audit implementation changed')


def verify_inputs(root, scope, config, budget):
    for path,digest in config['inputs'].items():
        require(budget.hash(contained(root,path))==digest, 'Reader input binding changed: '+path)
    for path,info in config['physical_inputs'].items():
        require(not path.startswith('data/dbn/') and path.endswith('.parquet'), 'Raw/undeclared source refused')
        if 'end_ns' in info:
            require(0 < info['start_ns'] < info['end_ns'] <= CUTOFF, 'Protected physical boundary refused before open')
        target=contained(root,path)
        require(target.stat().st_size==info['size'] and budget.hash(target)==info['sha256'],
                'Exact physical input changed: '+path)
    for path,digest in scope['candidate_bindings'].items():
        require(budget.hash(contained(root,path))==digest,'Prepared candidate changed: '+path)


@contextmanager
def physical_reads(root, config):
    """Fail before any undeclared Parquet open, including a wider annual source."""
    import os
    import pyarrow.parquet as pq
    from unittest.mock import patch
    key=lambda p:os.path.normcase(str(io_path(Path(p).resolve())))
    approved={key(contained(root,p)) for p in config['physical_inputs']}
    def allowed(path):
        require(isinstance(path,(str,Path)) and key(path) in approved,
                'Reader attempted an unapproved physical Parquet input')
    original_file, original_read=pq.ParquetFile,pq.read_table
    def file(path,*args,**kwargs):
        allowed(path)
        return original_file(path,*args,**kwargs)
    def read(path,*args,**kwargs):
        allowed(path)
        return original_read(path,*args,**kwargs)
    with patch.object(pq,'ParquetFile',file),patch.object(pq,'read_table',read):
        yield


def select_cases(root, config):
    """Deterministic representative cases inside the five already-compared units."""
    from .annual_foundation_storage import _read_candidate
    from .hourly_source_admission import timestamp
    from .causal_timeframe_publication import months
    import pyarrow.parquet as pq
    cases={'hourly':[],'daily':[]}
    compared=read_record(contained(root,BASE+'/historical_pilot_v1/compare.json'))
    for unit,record in zip(config['pilot_units'],compared['units'],strict=True):
        if unit['timeframe']=='1m': continue
        directory=contained(root,BASE+'/historical_pilot_v1/'+record['directory'])
        packing=read_record(directory/'manifest.json')
        available=[c['month'] for c in packing['coverage'] if c['state']=='CERTIFIED']
        chosen=available if unit['timeframe']=='1d' else sorted({available[0],available[-1]})
        sample={}; recovered=None
        for month,logical in _read_candidate(directory,manifest_sha256=record['manifest_sha256'],months=chosen,maximum_group_bytes=64*2**20):
            for row in logical['observations']:
                sample.setdefault(month,row)
                if row['definition_received_at_ns']>row['bar_start_ns'] and recovered is None:
                    recovered=row
        rows=[sample[m] for m in (available[0],available[-1]) if m in sample]
        if recovered is not None: rows.append(recovered)
        lane='hourly' if unit['timeframe']=='1h' else 'daily'
        for i,row in enumerate(rows):
            request=dict(market=unit['market'],start_ns=row['bar_start_ns'],end_ns=row['bar_end_ns'],
                as_of_ns=row['decision_eligible_at_ns'],source='native',purpose='LINEAGE_AUDIT')
            label=f'{unit["market"]}_{unit["year"]}_{i}'
            cases[lane].append(dict(case=label,request=request,expected=True))
            cases[lane].append(dict(case=label+'_premature',request=request|{'as_of_ns':row['decision_eligible_at_ns']-1},expected=False))
            cases[lane].append(dict(case=label+'_forbidden',request=request|{'purpose':'TRADING'},expected=False))
    for case,market,start in (('GF_excluded','GF','2013-12-24T23:00:00Z'),
                              ('ZQ_missing_Feb','ZQ','2020-02-03T00:00:00Z'),
                              ('ZQ_missing_Mar','ZQ','2020-03-02T00:00:00Z')):
        ns=timestamp(start)
        cases['hourly'].append(dict(case=case,expected=False,request=dict(market=market,
            start_ns=ns,end_ns=ns+3600*10**9,as_of_ns=ns+3605*10**9,purpose='LINEAGE_AUDIT')))
    derived=next(p for p,i in config['physical_inputs'].items() if i['role']=='EXISTING_AUDIT_ONLY_DERIVED_ZQ')
    rows=pq.read_table(contained(root,derived)).to_pylist()
    row=next(r for r in rows if timestamp('2020-02-01')<=r['bar_start_ns']<timestamp('2020-04-01'))
    request=dict(market='ZQ',start_ns=row['bar_start_ns'],end_ns=row['bar_end_ns'],
        as_of_ns=row['available_at_ns'],source='minute_derived')
    cases['hourly'].append(dict(case='explicit_derived_audit',expected=True,request=request|{'purpose':'LINEAGE_AUDIT'}))
    cases['hourly'].append(dict(case='derived_feature_denied',expected=False,request=request|{'purpose':'FEATURE_GENERATION'}))
    require(all(0<len(v)<=32 for v in cases.values()), 'Representative request bound exceeded')
    return cases


def reader_checks(root, config, cases, lane, candidate):
    from . import hourly_source_admission as hourly
    from . import daily_source_admission as daily
    pointer='configs/active_'+('hourly' if lane=='hourly' else 'daily')+'_source_admission.json'
    cls=hourly.HourlySourceReader if lane=='hourly' else daily.DailySourceReader
    reader=cls(root,preview_manifest=config['policies'][pointer]['path']) if candidate else cls(root)
    population=dict(entries=len(reader.manifest['entries']),entries_sha256=sha256_json(reader.manifest['entries']))
    require(population['entries']==config['policies'][pointer]['entry_count']
            and population['entries_sha256']==config['policies'][pointer]['entries_sha256'],
            'Source permission population changed')
    checks=[]
    for case in cases[lane]:
        request=case['request']
        policy={k:request[k] for k in ('market','start_ns','end_ns')}
        policy['source']=request.get('source','native')
        permitted=reader.permitted_uses(**policy)
        try:
            rows=reader.read_window(**request)
            result=dict(accepted=True,rows=len(rows),rows_sha256=sha256_json(rows))
        except (UnauthorizedOperation,ContractError) as exc:
            result=dict(accepted=False,error_type=type(exc).__name__,error=str(exc))
        require(result['accepted']==case['expected'],'Representative reader expectation differs: '+case['case'])
        checks.append(dict(case=case['case'],request=request,permitted_uses=list(permitted),result=result))
    reader._unchanged()
    return checks,population


def minute_checks(root, config):
    from . import active_data_view as minute
    old=json.loads(contained(root,'data/active/catalog.json').read_bytes())
    checks=[]
    for candidate in (False,True):
        with projected(root,config['overrides']) if candidate else nullcontext():
            catalog=json.loads(contained(root,config['overrides']['data/active/catalog.json'] if candidate else 'data/active/catalog.json').read_bytes())
            minute.validate_catalog(catalog)
            for year in (2022,2025):
                entry=next(e for e in catalog['entries'] if (e['market'],e['year'])==('NQ',year))
                prior=next(e for e in old['entries'] if (e['market'],e['year'])==('NQ',year))
                require(entry['permitted_uses']==prior['permitted_uses'],'Minute permissions changed')
                for purpose in entry['permitted_uses']+['TRADING']:
                    try:
                        path=minute.resolve(repository_root=root,market='NQ',year=year,purpose=purpose)
                        accepted=True
                        require(sha256_file(path)==entry['parquet_sha256'],'Minute resolver returned wrong payload')
                    except UnauthorizedOperation:
                        accepted=False
                    require(accepted==(purpose!='TRADING'),'Minute resolver permission differs')
                    checks.append(dict(candidate=candidate,year=year,purpose=purpose,accepted=accepted))
    return checks


def raw_routes(root):
    from . import active_data_view, causal_timeframe_view, causal_timeframe_publication
    index=read_record(causal_timeframe_publication.contained(root,'data/active/timeframes.json'))
    checks=[]
    for timeframe in ('1h','1d'):
        args=dict(market='NQ',year=2022,timeframe=timeframe,purpose='LINEAGE_AUDIT')
        calls=(lambda:causal_timeframe_publication.resolve_partitions(root,index,**args),
               lambda:causal_timeframe_view.resolve_partitions(repository_root=root,**args),
               lambda:active_data_view.resolve_partitions(repository_root=root,**args))
        for n,call in enumerate(calls):
            try: call()
            except UnauthorizedOperation as exc:
                require('read_window' in str(exc),'Raw route refused for an unexpected reason')
            else: raise IntegrityError('Raw path route reopened')
            checks.append((timeframe,n))
    return checks


def worker():
    from .daily_identity_rollout import verify_job, memory
    request=json.loads(sys.stdin.buffer.readline(2**20));root=Path.cwd().resolve()
    scope=read_record(contained(root,request['scope']))
    auth=read_record(contained(root,request['authorization']))
    validate_authorization(root,scope,auth,request['scope'])
    require(request['phase'] in PHASES,'Unsupported reader-only phase')
    verify_job(request['supervision_job_name'],scope['limits']['maximum_memory_bytes'])
    directory=contained(root,OUTPUT)
    owner=read_record(directory/'owner.json')
    require(owner['scope_id']==scope['id'],'Reader validation ownership differs')
    config_path=contained(root,scope['configuration']['path'])
    require(sha256_file(config_path)==scope['configuration']['sha256'],'Prepared reader configuration changed')
    config=read_record(config_path)
    budget=Budget(directory,Limits(**scope['limits']));budget.written=request['ledger_seed']
    result=dict(status='FAILED_RECOVERY_PHASE',phase=request['phase'],scope_id=scope['id'])
    try:
        verify_inputs(root,scope,config,budget)
        with physical_reads(root,config):
            phase=request['phase']
            if phase=='select': result['cases']=select_cases(root,config)
            elif phase=='minute':
                result['checks']=minute_checks(root,config)
                with projected(root,config['overrides']): result['raw_routes']=raw_routes(root)
            else:
                saved=directory/'select.json'
                require(sha256_file(saved)==request['cases_sha256'],'Reader cases changed')
                cases=read_record(saved)['cases']
                lane,candidate=phase.split('_');candidate=candidate=='candidate'
                with projected(root,config['overrides']) if candidate else nullcontext():
                    result['checks'],result['policy_population']=reader_checks(root,config,cases,lane,candidate)
                if candidate:
                    original=directory/(lane+'_original.json')
                    require(sha256_file(original)==request['original_sha256'],'Original reader evidence changed')
                    require(result['checks']==read_record(original)['checks'],'Annual public reader differs from active reader')
        verify_inputs(root,scope,config,budget)
        require(time.time()<auth['deadline_epoch'],'Whole reader-check deadline exceeded')
        require(allocation(contained(root,BASE),scope['limits']['maximum_files'])['retained_allocated_bytes']+2*2**20
                <=scope['maximum_allocation_bytes'],'Retained allocation ceiling exceeded')
        result['status']='PASS_RECOVERY_PHASE'
    except BaseException as exc:
        result.update(error_type=type(exc).__name__,error=str(exc)[:2000])
    finally:
        result['memory']=memory();result['resources_before_terminal']=budget.record()
        result['new_application_bytes_before_terminal']=budget.written-request['ledger_seed']
        budget.finalizing=True;budget.save(directory/(request['phase']+'.json'),signed(result))
    if result['status']!='PASS_RECOVERY_PHASE': raise RuntimeError(result.get('error'))


def run(root, *, scope_path, authorization_path):
    """Task-only approved read checks. No retries, conversion or control mutation."""
    from .daily_identity_rollout import supervise, resource_gate
    root=Path(root).resolve();scope=read_record(contained(root,scope_path));auth=read_record(contained(root,authorization_path))
    validate_authorization(root,scope,auth,scope_path)
    directory=contained(root,OUTPUT);directory.mkdir(parents=True,exist_ok=False)
    budget=Budget(directory,Limits(**scope['limits']));budget.written=scope['known_prior_measured_writes']
    budget.written+=contained(root,scope_path).stat().st_size+contained(root,authorization_path).stat().st_size
    summary=dict(status='FAILED_READER_INTEGRATION',scope_id=scope['id'],phases=[],active_control_replacements=[])
    try:
        budget.save(directory/'owner.json',signed(dict(scope_id=scope['id'])))
        for phase in PHASES:
            request=dict(phase=phase,scope=scope_path,authorization=authorization_path,
                deadline_epoch=auth['deadline_epoch'],ledger_seed=budget.written,result_path=OUTPUT+'/'+phase+'.json')
            if phase not in ('select','minute'): request['cases_sha256']=sha256_file(directory/'select.json')
            if phase.endswith('_candidate'):
                request['original_sha256']=sha256_file(directory/(phase.replace('_candidate','_original')+'.json'))
            diagnostic=directory/(phase+'_supervision.json');result_path=directory/(phase+'.json')
            try:
                measured=supervise([sys.executable,'-c','from futures_rebuild.annual_foundation_integration_audit import worker;worker()'],
                    root,request,Limits(**scope['limits']),diagnostic=diagnostic)
            finally:
                if result_path.exists():
                    result=read_record(result_path)
                    budget.written+=result['new_application_bytes_before_terminal']+result_path.stat().st_size
                else: summary['write_accounting']='INCOMPLETE_WORKER_TERMINAL_EVIDENCE'
                if diagnostic.exists(): budget.written+=diagnostic.stat().st_size
            peak=resource_gate(measured,coordinator_limit=960*2**20,worker_limit=5*2**30,
                aggregate_limit=6*2**30,deadline=auth['deadline_epoch'])
            summary['phases'].append(dict(phase=phase,seconds=measured['seconds'],aggregate_peak_bound=peak))
            budget.check()
        summary['status']='PASS_ISOLATED_ANNUAL_READER_INTEGRATION_NOT_MIGRATION_CERTIFICATION'
    except BaseException as exc:
        summary.update(error_type=type(exc).__name__,error=str(exc)[:2000])
    finally:
        budget.finalizing=True
        summary['measured_write_ledger_before_terminal']=budget.written
        budget.save(directory/'completion.json',signed(summary))
    require(summary['status'].startswith('PASS_'),summary.get('error','Reader integration failed'))
    return summary
