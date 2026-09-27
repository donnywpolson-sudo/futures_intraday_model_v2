"""Task-only supervised phases and rollback-protected activation. No CLI execution."""
import ctypes
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4

from .canonical import canonical_bytes, sha256_file, sha256_json
from .causal_timeframe_storage import Budget, Limits, read_record, signed, allocation
from .daily_identity_campaign import HERE, CHANGES, contained, require

PHASES=('reconstruct','stage','admission','recheck','default')


def memory(handle=-1):
    class Counters(ctypes.Structure):
        _fields_=[('cb',ctypes.c_ulong),('faults',ctypes.c_ulong)]+[
            (n,ctypes.c_size_t) for n in ('peak_ws','ws','peak_paged','paged','peak_nonpaged',
                                        'nonpaged','commit','peak_commit')]
    c=Counters(); c.cb=ctypes.sizeof(c)
    fn=ctypes.windll.psapi.GetProcessMemoryInfo
    fn.argtypes=[ctypes.c_void_p,ctypes.c_void_p,ctypes.c_ulong]
    if not fn(ctypes.c_void_p(handle),ctypes.byref(c),c.cb): raise OSError('Process peak measurement failed')
    return dict(peak_working_set_bytes=c.peak_ws,peak_committed_bytes=c.peak_commit,
                peak_bytes=max(c.peak_ws,c.peak_commit))


def resource_gate(measured, *, coordinator_limit, worker_limit, aggregate_limit, deadline):
    require(measured['coordinator']['peak_bytes']<=coordinator_limit,
            f"Coordinator memory {measured['coordinator']['peak_bytes']} exceeds {coordinator_limit}")
    require(measured['worker']['peak_bytes']<=worker_limit,
            f"Worker memory {measured['worker']['peak_bytes']} exceeds {worker_limit}")
    total=measured['coordinator']['peak_bytes']+measured['worker']['peak_bytes']
    require(total<=aggregate_limit,f'Aggregate memory {total} exceeds {aggregate_limit}')
    require(time.time()<deadline,f'Deadline exceeded at {time.time()}; deadline {deadline}')
    return total


def supervise(command, root, request, limits, *, diagnostic):
    """Existing Windows job pattern, with terminal job/process peaks on failures."""
    from .durable_windows_task_transport import WindowsKillOnCloseJob, _ExtendedLimitInformation
    require(os.name=='nt','Windows supervision required')
    started=time.monotonic(); record=dict(status='FAILED_SUPERVISION',phase=request['phase'])
    child=None
    try:
        with WindowsKillOnCloseJob() as job:
            name='Local\\daily_identity_'+uuid4().hex
            handle=job._kernel32.CreateJobObjectW(None,name)
            require(bool(handle),'Unable to create named recovery job')
            job.close(); job._handle=handle
            limits_info=_ExtendedLimitInformation()
            limits_info.BasicLimitInformation.LimitFlags=0x2000|0x100|0x200
            limits_info.ProcessMemoryLimit=limits_info.JobMemoryLimit=limits.maximum_memory_bytes
            require(bool(job._kernel32.SetInformationJobObject(job._handle,9,ctypes.byref(limits_info),ctypes.sizeof(limits_info))),
                    'Unable to enforce recovery memory limits')
            try:
                child=subprocess.Popen(command,cwd=root,stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,creationflags=subprocess.CREATE_NO_WINDOW)
                job.assign(child)
                raw=canonical_bytes(request|{'supervision_job_name':name})+b'\n'
                require(len(raw)<=2**20,'Oversized supervisor request')
                try:
                    remaining=min(limits.maximum_seconds,request.get('deadline_epoch',time.time()+limits.maximum_seconds)-time.time())
                    require(remaining>0,'Whole-operation deadline elapsed before worker dispatch')
                    child.stdin.write(raw); child.stdin.close()
                    expires=time.monotonic()+remaining
                    observed=dict(peak_bytes=0,peak_working_set_bytes=0,peak_committed_bytes=0)
                    while child.poll() is None:
                        sample=memory(int(child._handle))
                        observed={k:max(observed[k],sample[k]) for k in observed}
                        if time.monotonic()>=expires:
                            raise subprocess.TimeoutExpired(command,remaining)
                        time.sleep(0.05)
                    record['worker']=observed
                except subprocess.TimeoutExpired:
                    record['error']='WORKER_DEADLINE_EXCEEDED'
                    job.terminate(); child.wait(timeout=10)
                record['exit_code']=child.returncode
                if child.returncode==0 and 'error' not in record: record['status']='PASS_SUPERVISED_PHASE'
            finally:
                if child is not None and child.poll() is None:
                    job.terminate(); child.wait(timeout=10)
                measured=_ExtendedLimitInformation()
                require(bool(job._kernel32.QueryInformationJobObject(job._handle,9,ctypes.byref(measured),ctypes.sizeof(measured),None)),
                        'Job peak measurement unavailable')
                record['peak_job_committed_bytes']=int(measured.PeakJobMemoryUsed)
                record['peak_worker_process_committed_bytes']=int(measured.PeakProcessMemoryUsed)
                if child is not None:
                    sample=memory(int(child._handle))
                    observed=record.get('worker',locals().get('observed',sample))
                    record['worker']={k:max(observed[k],sample[k]) for k in sample}
                    record['process_peak_evidence']='OBSERVED_LIVE_SAMPLES_LOWER_BOUND_UNLESS_TERMINAL_REPORT'
                record['coordinator']=memory()
                record['seconds']=time.monotonic()-started
                if 'result_path' in request:
                    path=contained(Path(root),request['result_path'])
                    if path.exists():
                        result=read_record(path)
                        record['worker_result_sha256']=sha256_file(path)
                        record['worker_result_bytes']=path.stat().st_size
                        record['worker_reported_memory']=result.get('memory')
                        if result.get('status')!='PASS_RECOVERY_PHASE': record['status']='FAILED_SUPERVISION'
                if record.get('worker'):
                    record['worker']['peak_bytes']=max(record['worker']['peak_bytes'],record['peak_job_committed_bytes'])
                    if record.get('worker_reported_memory'):
                        for key in record['worker']:
                            record['worker'][key]=max(record['worker'][key],record['worker_reported_memory'][key])
                        record['process_peak_evidence']='WORKER_TERMINAL_LIFETIME_PEAKS_AND_JOB_PEAK'
                    require(record['worker']['peak_bytes']<=limits.maximum_memory_bytes,'Worker peak exceeded its limit')
    except BaseException as exc:
        record.update(status='FAILED_SUPERVISION',error_type=type(exc).__name__,error=str(exc)[:1024])
    finally:
        record['seconds']=time.monotonic()-started
        diagnostic.parent.mkdir(parents=True,exist_ok=True)
        with diagnostic.open('xb') as stream: stream.write(canonical_bytes(signed(record))+b'\n')
    require(record['status']=='PASS_SUPERVISED_PHASE',record.get('error',f"Worker exit {record.get('exit_code')}"))
    return record


def verify_job(name, maximum):
    from .durable_windows_task_transport import _ExtendedLimitInformation
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.OpenJobObjectW.argtypes=[ctypes.c_ulong,ctypes.c_int,ctypes.c_wchar_p]
    kernel.OpenJobObjectW.restype=ctypes.c_void_p
    handle=kernel.OpenJobObjectW(4,False,name)
    require(bool(handle),'Named supervised job is absent')
    try:
        info=_ExtendedLimitInformation()
        kernel.QueryInformationJobObject.argtypes=[ctypes.c_void_p,ctypes.c_int,ctypes.c_void_p,ctypes.c_ulong,ctypes.c_void_p]
        require(bool(kernel.QueryInformationJobObject(handle,9,ctypes.byref(info),ctypes.sizeof(info),None)), 'Cannot query worker job')
        inside=ctypes.c_int()
        kernel.IsProcessInJob.argtypes=[ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p]
        require(bool(kernel.IsProcessInJob(ctypes.c_void_p(-1),handle,ctypes.byref(inside))) and bool(inside.value),
                'Worker is outside its supervised job')
        require(info.ProcessMemoryLimit==info.JobMemoryLimit==maximum
            and info.BasicLimitInformation.LimitFlags & 0x2300==0x2300,'Worker memory job differs')
    finally:
        kernel.CloseHandle.argtypes=[ctypes.c_void_p]; kernel.CloseHandle(handle)


def worker():
    """Private stdin protocol; historical authority is checked before data access."""
    from . import daily_identity_campaign as campaign
    from .boundary import OperationReceipt, OperationClassification, RepoBoundary
    from .causal_timeframe_publication import CLAIM, APPROVAL_KEYS
    from .daily_identity_publication import OPERATION
    request=json.loads(sys.stdin.buffer.readline(2**20))
    root=Path.cwd().resolve(); phase=request['phase']
    require(phase in PHASES,'Unsupported recovery phase')
    auth=read_record(contained(root,request['authorization']))
    require(auth['status']=='OWNER_APPROVED_IN_THIS_TASK' and time.time()<auth['deadline_epoch'], 'Missing or expired approval')
    plan_path=contained(root,request['plan'])
    require(sha256_file(plan_path)==auth['plan_sha256'],'Approved plan changed')
    plan=read_record(plan_path)
    require(plan['id']==auth['plan_id'],'Approved plan identity differs')
    verify_job(request['supervision_job_name'],plan['limits']['maximum_memory_bytes'])
    receipt=OperationReceipt.from_dict(auth['historical_receipt'])
    required={'plan_id':plan['id'],'claim':CLAIM}; received=dict(receipt.scope)
    require(set(received)==set(required)|APPROVAL_KEYS,'Historical scope differs')
    required.update({k:received[k] for k in APPROVAL_KEYS})
    method=receipt.verify if phase=='reconstruct' else receipt.assert_consumed
    method(RepoBoundary(root),operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,required_scope=required)
    directory=contained(root,plan['output']+'/phases/'+phase)
    directory.mkdir(parents=True,exist_ok=False)
    budget=Budget(contained(root,plan['output']),Limits(**plan['limits']))
    budget.terminal_reserve=4*2**20
    result=dict(status='FAILED_RECOVERY_PHASE',phase=phase)
    try:
        for path,digest in plan['preparation_bindings'].items():
            require(sha256_file(contained(root,path))==digest,'Prepared implementation changed')
        if phase=='reconstruct':
            cert=campaign.reconstruct_and_certify(root,plan,receipt,budget)
            result.update(certificate_id=cert['id'],recovered_rows=plan['expected_recovered'])
            proposal=campaign.prepare_replacement_proposal(root,plan)
            budget.save(contained(root,plan['output']+'/proposal.json'),proposal)
            result.update(proposal_id=proposal['id'],proposal_sha256=sha256_file(contained(root,plan['output']+'/proposal.json')))
        elif phase=='stage':
            previous=read_record(contained(root,plan['output']+'/phases/reconstruct/result.json'))
            require(previous['status']=='PASS_RECOVERY_PHASE'
                and previous['proposal_sha256']==sha256_file(contained(root,plan['output']+'/proposal.json')), 'Prepared proposal changed')
            proposal=read_record(contained(root,plan['output']+'/proposal.json'))
            candidate_receipt=OperationReceipt.from_dict(request['publication_receipt'])
            result.update(campaign.stage_replacement(root,plan,proposal,candidate_receipt,budget))
        elif phase=='admission':
            result.update(campaign.prepare_policies(root,plan,budget))
            result.update(campaign.coverage_after(root,plan,budget))
            result.update(campaign.reader_checks(root,plan,budget))
        elif phase=='recheck':
            campaign.verify_inputs(root,plan,budget)
            from .hourly_source_admission import verify_bindings
            with campaign.projected_successor(root,campaign.code_overrides()|{
                'data/active/timeframes.json':plan['output']+'/publication/index.json',
                'configs/active_hourly_quality_reader.json':plan['output']+'/policies/hourly/quality_pointer.json'}):
                for path in (plan['output']+'/policies/admission.json',plan['output']+'/policies/hourly/admission.json'):
                    manifest=read_record(contained(root,path)); verify_bindings(root,manifest['bindings'])
                    del manifest
        else:
            result.update(campaign.reader_checks(root,plan,budget,default=True))
        require(time.time()<auth['deadline_epoch'],'Approved whole-operation deadline exceeded')
        budget.check()
        result['status']='PASS_RECOVERY_PHASE'
    except BaseException as exc:
        result.update(status='FAILED_RECOVERY_PHASE',error_type=type(exc).__name__,error=str(exc)[:2048])
    finally:
        budget.finalizing=True; budget.terminal_reserve=0
        result.pop('id',None)
        result['resources']=budget.record(); result['memory']=memory()
        owned=allocation(contained(root,plan['output']),100000)
        proposal_path=contained(root,plan['output']+'/publication/plan.json')
        if proposal_path.exists():
            proposal=read_record(proposal_path)
            for move in proposal['moves']:
                destination=contained(root,move['entry']['directory'])
                if destination.exists():
                    extra=allocation(destination,100000)
                    owned={k:owned[k]+extra[k] for k in owned}
            del proposal
        result['owned_allocation']=owned
        result['memory']=memory()
        budget.save(directory/'result.json',signed(result))
    return 0 if result['status']=='PASS_RECOVERY_PHASE' else 1


def helper(root):
    path=contained(Path(root),'state/unpublished_evidence/daily_foundation_v1/prepare_coordinated_publication.py')
    spec=importlib.util.spec_from_file_location('daily_identity_control_commit',path)
    module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


def activate_passing(root, changes, predecessors, *, rollback, verify_default):
    """Caller holds the existing leases and approved scope; default failure rolls back."""
    require(list(changes)==list(CHANGES) and set(predecessors)==set(CHANGES),'Exact six-control order required')
    helper(root).activate_controls(root,changes,expected=predecessors,rollback_directory=rollback,validate=verify_default)


def prepare_controls(root, *, prefix, validation_path, supervision_path, budget):
    """Bind passing supervised preview evidence to the ordinary active pointers."""
    root=Path(root)
    validation=read_record(contained(root,validation_path))
    supervision=read_record(contained(root,supervision_path))
    require(validation['status']=='PASS_RECOVERY_PHASE' and validation['phase']=='admission'
        and supervision['status']=='PASS_SUPERVISED_PHASE'
        and supervision['worker_result_sha256']==sha256_file(contained(root,validation_path)),
        'Passing supervised admission validation is required')
    def ref(path): return dict(path=path,sha256=sha256_file(contained(root,path)))
    directory=contained(root,prefix+'/controls'); directory.mkdir(parents=True,exist_ok=False)
    audit={k:v for k,v in validation.items() if k!='id'}
    audit.update(status='PASS_DAILY_FOUNDATION_WITH_LIMITATIONS',admission_sha256=ref(prefix+'/policies/admission.json')['sha256'],
        integrity='CERTIFIED_NATIVE_RECOVERY_AND_BOUND_REPLAY',coverage='30_AVAILABILITY_EXCLUSIONS_21_UNAVAILABLE_MONTHS',
        reader_enforcement='PASS_PREPARED_PUBLIC_READERS_DEFAULT_CHECK_REQUIRED',
        source_permissions='MINUTE_MARKET_DATE_COHORT_CEILING',model_specific_eligibility='NOT_DECLARED')
    budget.save(directory/'daily_result.json',signed(audit))
    budget.save(directory/'daily_supervision.json',signed(dict(status='PASS_SUPERVISED_DAILY_FOUNDATION',
        result_sha256=ref(prefix+'/controls/daily_result.json')['sha256'],
        validation_supervision=ref(supervision_path),workers_stopped=True)))
    budget.save(directory/'daily_pointer.json',signed(dict(schema='active_daily_source_admission/1',
        manifest=ref(prefix+'/policies/admission.json'),audit=ref(prefix+'/controls/daily_result.json'),
        supervision=ref(prefix+'/controls/daily_supervision.json'))))
    old_pointer=read_record(contained(root,'configs/active_hourly_source_admission.json'))
    old_result=read_record(contained(root,old_pointer['result']['path']))
    hourly={k:v for k,v in old_result.items() if k!='id'}
    hourly.update(status='PASS_FOUNDATION_WITH_COVERAGE_LIMITATIONS',
        source_admission_manifest_sha256=ref(prefix+'/policies/hourly/admission.json')['sha256'],
        actual_window_reads=validation['hourly_reader_checks'],retained_population_result=old_pointer['result'],
        coordinated_daily_validation=ref(prefix+'/controls/daily_result.json'))
    budget.save(directory/'hourly_result.json',signed(hourly))
    budget.save(directory/'hourly_supervision.json',signed(dict(status='PASS_SUPERVISED_FOUNDATION_AUDIT',
        worker_result_sha256=ref(prefix+'/controls/hourly_result.json')['sha256'],
        integrated_supervision=ref(prefix+'/controls/daily_supervision.json'),workers_stopped=True)))
    budget.save(directory/'hourly_pointer.json',signed(dict(schema='active_hourly_source_admission/1',
        manifest=ref(prefix+'/policies/hourly/admission.json'),result=ref(prefix+'/controls/hourly_result.json'),
        supervision=ref(prefix+'/controls/hourly_supervision.json'))))
    changes=dict(zip(CHANGES,(HERE+'/candidate/causal_timeframe_publication.py',
        HERE+'/candidate/daily_quality_reader.py',prefix+'/publication/index.json',
        prefix+'/policies/hourly/quality_pointer.json',prefix+'/controls/hourly_pointer.json',
        prefix+'/controls/daily_pointer.json'),strict=True))
    return changes


def run_phase(root, scope, authorization_path, phase, *, publication_receipt=None):
    """Small coordinator entry: the full plan and manifests load only in the job."""
    root=Path(root).resolve()
    require(len(canonical_bytes(scope))<2**20 and phase in PHASES
        and scope.get('schema')=='daily_identity_recovery_coordinator/1'
        and scope['id']==sha256_json({k:v for k,v in scope.items() if k!='id'}), 'Small exact coordinator scope required')
    auth=read_record(contained(root,authorization_path))
    require(auth['status']=='OWNER_APPROVED_IN_THIS_TASK' and auth['plan_id']==scope['plan_id']
        and auth['coordinator_scope_id']==scope['id']
        and auth['plan_sha256']==scope['plan']['sha256']
        and sha256_file(contained(root,scope['plan']['path']))==scope['plan']['sha256']
        and 0<auth['deadline_epoch']-auth['started_epoch']<=scope['limits']['maximum_seconds']<=21600,
        'Invalid approved recovery scope')
    measured=memory()
    require(measured['peak_bytes']<=960*2**20,'Coordinator already exceeds 960 MiB before dispatch')
    require(time.time()<auth['deadline_epoch'],'Whole-operation deadline already elapsed')
    prior={'stage':'reconstruct','admission':'stage','recheck':'admission','default':'recheck'}.get(phase)
    if prior:
        accepted=read_record(contained(root,scope['output']+'/phases/'+prior+'/acceptance.json'))
        require(accepted['status']=='PASS_PHASE_ACCEPTANCE' and accepted['plan_id']==scope['plan_id']
            and accepted['worker_result_sha256']==sha256_file(contained(root,scope['output']+'/phases/'+prior+'/result.json'))
            and accepted['supervision_sha256']==sha256_file(contained(root,scope['output']+'/phases/'+prior+'/supervision.json')),
            'Previous phase has no bound complete resource acceptance')
    prefix=scope['output']+'/phases/'+phase
    diagnostic=contained(root,prefix+'/supervision.json')
    require(not diagnostic.exists() and not contained(root,prefix+'/result.json').exists(),
            'Phase already attempted; no automatic retry')
    request=dict(phase=phase,plan=scope['plan']['path'],authorization=authorization_path,
        deadline_epoch=auth['deadline_epoch'],result_path=prefix+'/result.json')
    if publication_receipt is not None: request['publication_receipt']=publication_receipt.as_dict()
    limits=Limits(**scope['limits'])
    supervision=supervise([sys.executable,'-B','-c',
        'from futures_rebuild.daily_identity_rollout import worker; raise SystemExit(worker())'],
        root,request,limits,diagnostic=diagnostic)
    acceptance=dict(status='FAILED_PHASE_ACCEPTANCE',phase=phase,plan_id=scope['plan_id'],
        worker_result_sha256=supervision['worker_result_sha256'],supervision_sha256=sha256_file(diagnostic))
    try:
        result=read_record(contained(root,prefix+'/result.json'))
        require(result['status']=='PASS_RECOVERY_PHASE' and supervision['worker_result_sha256']==sha256_file(contained(root,prefix+'/result.json')),
                'Supervised phase result differs')
        resource_gate(supervision,coordinator_limit=960*2**20,worker_limit=5*2**30,
                      aggregate_limit=6*2**30,deadline=auth['deadline_epoch'])
        written=scope['retained_cost_upper_bytes']+64*2**20
        for name in PHASES:
            path=contained(root,scope['output']+'/phases/'+name+'/result.json')
            if not path.exists(): continue
            previous=read_record(path)
            resources=previous['resources']
            written+=max(resources['application_written_bytes'],resources['process_write_transfer_bytes'] or 0)+path.stat().st_size+2**20
        require(written<=8*2**30,'Cumulative 8 GiB write allowance exceeded')
        owned=result['owned_allocation']
        require(owned['retained_allocated_bytes']+scope['retained_allocation_upper_bytes']+64*2**20<=10*2**30,
                'Cumulative 10 GiB allocation allowance exceeded')
        require(owned['retained_files']+scope['retained_file_count']+256<=100000,'Cumulative file allowance exceeded')
        import shutil
        require(shutil.disk_usage(root).free>=64*2**30,'64 GiB free-space reserve breached')
        acceptance.update(status='PASS_PHASE_ACCEPTANCE',cumulative_write_upper_bytes=written,worker=supervision['worker'],coordinator=supervision['coordinator'])
        return dict(result_path=prefix+'/result.json',supervision_path=prefix+'/supervision.json',
            worker_result_sha256=supervision['worker_result_sha256'],cumulative_write_upper_bytes=written,
            coordinator=supervision['coordinator'],worker=supervision['worker'])
    except BaseException as exc:
        acceptance.update(error_type=type(exc).__name__,error=str(exc)[:2048])
        raise
    finally:
        path=contained(root,prefix+'/acceptance.json')
        with path.open('xb') as stream: stream.write(canonical_bytes(signed(acceptance))+b'\n')


def activate(root, scope, authorization_path):
    """Recheck, six atomic controls, fresh defaults, final bindings, or rollback."""
    from .causal_timeframe_execution import execution_lease
    from .active_data_view import PUBLICATION_LOCK
    from .locking import FileLease
    root=Path(root).resolve()
    require(scope['publication_order']==list(CHANGES),'Approved control order changed')
    with execution_lease(root),FileLease(root/PUBLICATION_LOCK):
        run_phase(root,scope,authorization_path,'recheck')
        control_budget=Budget(contained(root,scope['output']),Limits(64*2**20,960*2**20,scope['limits']['maximum_seconds'],maximum_files=100000))
        changes=prepare_controls(root,prefix=scope['output'],
            validation_path=scope['output']+'/phases/admission/result.json',
            supervision_path=scope['output']+'/phases/admission/supervision.json',budget=control_budget)
        hashes={p:sha256_file(contained(root,candidate)) for p,candidate in changes.items()}
        def defaults():
            run_phase(root,scope,authorization_path,'default')
            for p,h in hashes.items(): require(sha256_file(contained(root,p))==h,'Final active binding changed: '+p)
            control_budget.check()
        activate_passing(root,changes,scope['predecessors'],rollback=scope['rollback'],verify_default=defaults)
        return dict(status='PASS_COORDINATED_NATIVE_DAILY_RECOVERY',active_hashes=hashes,rollback=scope['rollback'])
