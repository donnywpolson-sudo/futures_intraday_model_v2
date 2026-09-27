"""Task-only annual conversion worker. No CLI, publication or activation route."""
from pathlib import Path
import json
import sys
import time

from .annual_foundation_storage import BASE, CandidateBudget, pack_candidate, require
from .annual_foundation_pilot import _check_inputs, compare_sources
from .causal_timeframe_storage import Budget, Limits, contained, read_record, signed
from .canonical import sha256_file, io_path

OUTPUT = BASE+'/migration_v1_payloads'


def bounded_limits(scope, root):
    """Conservative allocation guard: no deletion, compression or rename credit."""
    from dataclasses import replace
    import ctypes
    import os
    limits=Limits(**scope['limits'])
    require(os.name=='nt','Migration requires Windows supervision')
    values=[ctypes.c_ulong() for _ in range(4)]
    require(ctypes.windll.kernel32.GetDiskFreeSpaceW(str(Path(root).anchor),*[ctypes.byref(v) for v in values])
        and values[0].value*values[1].value<=65536,'Unsupported allocation granularity')
    ceiling=(scope['known_prior_measured_writes']+scope['maximum_allocation_bytes']
             -scope['initial_retained_allocated_bytes']-limits.maximum_files*65536)
    require(ceiling>scope['known_prior_measured_writes'],'No allocation headroom')
    return replace(limits,maximum_written_bytes=min(limits.maximum_written_bytes,ceiling))


def activate_controls(root, changes, *, expected, rollback_directory, validate, budget):
    """Meter the existing atomic helper, reserving exact restoration writes first.

    Caller holds both leases and the separately approved passing migration scope.
    Default-worker reports retain their existing supervised write accounting.
    """
    from unittest.mock import patch
    from .daily_identity_rollout import helper
    from .causal_timeframe_storage import CountedOutput
    root=Path(root).resolve();rollback=contained(root,rollback_directory)
    old=sum(contained(root,p).stat().st_size for p in changes if contained(root,p).exists())
    new=sum(contained(root,p).stat().st_size for p in changes.values())
    budget.check(new+2*old+2*2**20)
    require(budget.existing_files+budget.files+3*len(changes)<=budget.limits.maximum_files,
            'Insufficient control rollback file reserve')
    original_open=Path.open;prior_reserve=budget.terminal_reserve;prior_finalizing=budget.finalizing
    budget.terminal_reserve+=old+2*2**20
    def opened(path,mode='r',*args,**kwargs):
        if mode=='xb' and io_path(Path(path).resolve()).is_relative_to(rollback):
            if str(path).endswith('.restore'):
                budget.finalizing=True
                backup=Path(path).with_suffix('.predecessor')
                budget.terminal_reserve-=backup.stat().st_size
            budget.check();budget.files+=1
            return CountedOutput(original_open(path,mode,*args,**kwargs),budget)
        return original_open(path,mode,*args,**kwargs)
    def checked():
        validate()
        budget.check()
    try:
        operation=helper(root)
        with patch.object(Path,'open',opened):
            operation.activate_controls(root,changes,expected=expected,
                rollback_directory=rollback_directory,validate=checked)
    finally:
        budget.terminal_reserve=prior_reserve;budget.finalizing=prior_finalizing


def promote_unit(root, *, manifest_reference, budget):
    """After the passing census, move only this attempt's new payloads; no controls.

    The task coordinator holds both publication leases and verifies the complete
    population/configuration before calling this primitive. Partial output stays
    in place on failure; an existing destination is never adopted automatically.
    """
    import os
    from .annual_foundation_release import directory as annual_directory, SCHEMA
    source_manifest=contained(root,manifest_reference['path'])
    require(manifest_reference['path'].startswith(BASE+'/'), 'Task-owned manifest required')
    require(sha256_file(source_manifest)==manifest_reference['sha256'],'Publication manifest changed')
    wrapper=read_record(source_manifest);packing=wrapper['packing']
    require(wrapper['schema']==SCHEMA and wrapper['status']=='CERTIFIED_PACKAGING_NO_PERMISSION_EXPANSION',
            'Compared annual wrapper required')
    relative=annual_directory(packing['timeframe'],packing['market'],packing['year'],packing['release_key'])
    source=contained(root,OUTPUT+'/'+relative.removeprefix('data/causally_gated_normalized/'))
    destination=contained(root,relative)
    require(not destination.exists(),'Annual destination already exists')
    original=read_record(source/'manifest.json')
    require(original==packing,'Publication source is not the exact packed unit')
    for name,info in packing['files'].items():
        require(info['filename']==name+'.parquet' and name in ('observations','quality','cadence','missingness','roll'),
                'Invalid annual payload filename')
        require(budget.hash(source/info['filename'])==info['sha256'],'Candidate changed before promotion')
    require(len(packing['files'])==5,'Incomplete annual unit')
    destination.parent.mkdir(parents=True,exist_ok=True)
    require(source.stat().st_dev==destination.parent.stat().st_dev,'Promotion requires one volume')
    destination.mkdir(exist_ok=False)
    for info in packing['files'].values():
        budget.check()
        os.rename(source/info['filename'],destination/info['filename'])
    # Preserve the original packing manifest and comparison evidence at their paths.
    with budget.open(destination/'manifest.json') as stream:
        stream.write(source_manifest.read_bytes())
    require(sha256_file(destination/'manifest.json')==manifest_reference['sha256'],'Promoted manifest differs')
    return relative


def prepare_admission_pointers(root, *, configuration, validation, supervision, budget):
    """Create inactive authority records only from passing full-population evidence."""
    from .annual_foundation_release import reference_record
    root=io_path(Path(root).resolve())
    def ref(path):
        return dict(path=io_path(path).relative_to(root).as_posix(),sha256=budget.hash(path))
    config=reference_record(root,configuration)
    checked=reference_record(root,validation)
    measured=reference_record(root,supervision)
    require(checked.get('status')=='PASS_ANNUAL_FOUNDATION_VALIDATION'
        and checked.get('configuration_sha256')==configuration['sha256']
        and checked.get('fully_compared_annual_units')==1817
        and checked.get('observation_counts')=={'1m':139892740,'1h':3023365,'1d':167609}
        and measured.get('status')=='PASS_SUPERVISED_PHASE'
        and measured.get('worker_result_sha256')==validation['sha256'],
        'Passing supervised complete population validation required')
    refs={}
    for lane in ('hourly','daily'):
        pointer='configs/active_'+lane+'_source_admission.json'
        policy=config['policies'][pointer]
        require(checked['policy_populations'][lane]=={
            'entries':policy['entry_count'],'entries_sha256':policy['entries_sha256']}
            and checked['representative_reader_checks'][lane], 'Complete policy and representative reader checks required')
        prior=read_record(contained(root,pointer))
        result=dict(status=('PASS_FOUNDATION_WITH_COVERAGE_LIMITATIONS' if lane=='hourly'
                            else 'PASS_DAILY_FOUNDATION_WITH_LIMITATIONS'),
            retained_population_audit=prior['result' if lane=='hourly' else 'audit'],
            annual_validation=validation,configuration=configuration,
            representative_reader_checks=checked['representative_reader_checks'][lane],
            model_specific_eligibility='NOT_DECLARED')
        result['source_admission_manifest_sha256' if lane=='hourly' else 'admission_sha256']=policy['sha256']
        path=budget.directory/(lane+'_admission_result.json');budget.save(path,signed(result))
        result_ref=ref(path)
        evidence=dict(status=('PASS_SUPERVISED_FOUNDATION_AUDIT' if lane=='hourly'
                              else 'PASS_SUPERVISED_DAILY_FOUNDATION'),annual_supervision=supervision)
        evidence['worker_result_sha256' if lane=='hourly' else 'result_sha256']=result_ref['sha256']
        path=budget.directory/(lane+'_admission_supervision.json');budget.save(path,signed(evidence))
        evidence_ref=ref(path)
        prepared=dict(schema='active_'+lane+'_source_admission/1',
            manifest={k:policy[k] for k in ('path','sha256')},supervision=evidence_ref)
        prepared['result' if lane=='hourly' else 'audit']=result_ref
        path=budget.directory/(lane+'_admission_pointer.json');budget.save(path,signed(prepared))
        refs[pointer]=ref(path)
    return refs


def perform(root, plan, phase, budget):
    """Sequential bounded units, then one independent complete comparison pass."""
    require(phase in ('pack','compare'), 'Unsupported migration phase')
    _check_inputs(root, plan, budget)
    records = []
    packed = read_record(budget.directory/'pack.json')['units'] if phase == 'compare' else None
    if packed is not None:
        require(len(packed) == len(plan['units']), 'Incomplete packed population')
    for number, unit in enumerate(plan['units']):
        budget.check()
        started = time.monotonic()
        if phase == 'pack':
            directory, manifest = pack_candidate(root,
                **{k:unit[k] for k in ('market','timeframe','year','sources','coverage')}, budget=budget)
            record = dict(directory=directory.relative_to(budget.directory).as_posix(),
                manifest_sha256=sha256_file(directory/'manifest.json'),
                counts={k:v['rows'] for k,v in manifest['files'].items()})
        else:
            record = dict(packed[number])
            directory = contained(budget.directory,record['directory'])
            manifest = read_record(directory/'manifest.json')
            require(all(manifest[k] == unit[k] for k in ('market','timeframe','year','sources','coverage')),
                    'Packed unit membership changed')
            record['comparison'] = compare_sources(root,directory,unit,
                manifest_sha256=record['manifest_sha256'],budget=budget,compact=unit.get('compact_reference'))
        record['seconds'] = time.monotonic()-started
        # These small checkpoints do not validate or authorize the population.
        budget.save(budget.directory/f'{phase}_{number:04d}.json',signed(record))
        records.append(record)
    _check_inputs(root,plan,budget)
    require(len(records) == len(plan['units']), 'Incomplete annual population')
    return records


def worker():
    """Only a separately approved future task may invoke this supervised protocol."""
    from .daily_identity_rollout import verify_job, memory
    request=json.loads(sys.stdin.buffer.readline(2**20));root=Path.cwd().resolve()
    scope=read_record(contained(root,request['scope']))
    auth=read_record(contained(root,request['authorization']))
    require(scope.get('schema')=='annual_foundation_migration_scope/1'
        and scope.get('conversion_output')==OUTPUT and scope.get('annual_units')==1817
        and scope.get('automatic_retry') is False, 'Exact annual migration scope required')
    require(auth.get('status')=='OWNER_APPROVED_IN_THIS_TASK' and auth.get('scope_id')==scope['id']
        and auth.get('scope_sha256')==sha256_file(contained(root,request['scope']))
        and 0 < auth['deadline_epoch']-auth['started_epoch'] <= scope['limits']['maximum_seconds']
        and time.time()<auth['deadline_epoch'], 'Separately approved migration required')
    phase=request['phase'];require(phase in ('pack','compare'),'No publication phase in conversion worker')
    limits=bounded_limits(scope,root);verify_job(request['supervision_job_name'],limits.maximum_memory_bytes)
    require(request['ledger_seed']>=scope['known_prior_measured_writes'],'Cumulative ledger cannot be reset')
    for path,digest in scope['execution_implementation'].items():
        require(sha256_file(contained(root,path))==digest,'Migration implementation changed')
    ref=scope['unit_inputs'];path=contained(root,ref['path'])
    require(sha256_file(path)==ref['sha256'],'Migration unit inventory changed')
    plan=read_record(path);require(len(plan['units'])==scope['annual_units'],'Migration population differs')
    directory=contained(root,OUTPUT)
    if phase=='pack':
        budget=CandidateBudget(root,'migration_v1_payloads',limits)
        budget.written=request['ledger_seed']
        budget.save(directory/'owner.json',signed(dict(scope_id=scope['id'])))
    else:
        require(read_record(directory/'owner.json')['scope_id']==scope['id'],'Candidate ownership differs')
        require(sha256_file(directory/'pack.json')==request['pack_sha256'],'Packed result changed')
        supervision=read_record(directory/'pack_supervision.json')
        require(supervision['status']=='PASS_SUPERVISED_PHASE'
            and supervision['worker_result_sha256']==request['pack_sha256'],'Passing supervised packing required')
        budget=Budget(directory,limits);budget.written=request['ledger_seed']
    budget.terminal_reserve=2*2**20
    budget.started=time.monotonic()-(time.time()-auth['started_epoch'])
    require(not (directory/(phase+'.json')).exists(),'Completed or failed phase cannot be retried')
    result=dict(status='FAILED_RECOVERY_PHASE',phase=phase,scope_id=scope['id'])
    try:
        result['units']=perform(root,plan,phase,budget)
        require(time.time()<auth['deadline_epoch'],'Whole migration deadline exceeded')
        result['status']='PASS_RECOVERY_PHASE'
    except BaseException as exc:
        result.update(error_type=type(exc).__name__,error=str(exc)[:2000])
    finally:
        result['memory']=memory();result['resources_before_terminal']=budget.record()
        result['new_application_bytes_before_terminal']=budget.written-request['ledger_seed']
        budget.finalizing=True;budget.save(directory/(phase+'.json'),signed(result))
    if result['status']!='PASS_RECOVERY_PHASE': raise RuntimeError(result.get('error'))
