"""Prepare native monthly builds; execution requires an adopted, one-use scope.

Importing this module grants no data access. There is deliberately no execution CLI.
"""
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
import ctypes
import json
import os
import shutil
import subprocess
import sys
import time
from uuid import uuid4

from .boundary import RepoBoundary, OperationReceipt, OperationClassification
from .canonical import canonical_bytes, sha256_file, sha256_json, io_path
from .causal_1s import Spec, SECOND, CUTOFF, CONTRACT, ENVIRONMENT, implementation_binding
from .causal_1s_sources import SyntheticDbnSources, FAMILIES, timestamp
from .causal_timeframe_storage import Limits, Budget, contained, signed, read_record
from .errors import IntegrityError, UnauthorizedOperation, ContractError

OPERATION='BUILD_CERTIFY_CAUSAL_1S_RANGE_ONCE'
ADOPTION='configs/approved_causal_1s_month.json'
CLAIM='CAUSAL_RECONSTRUCTION_ONLY_NO_RESEARCH_OR_PUBLICATION'
BASE='state/unpublished_evidence/causal_1s_native_v1'
APPROVAL_KEYS={'approval_command','approval_plan_id','approval_plan_sha256'}
PILOT_LIMITS=Limits(2**30,3*2**30,7200,maximum_files=128)
_WORKER_TOKEN=object()


def scope(op):
    return {'operation_id':op['id'],'claim':CLAIM}


def month_window(month):
    try:
        start=datetime.strptime(month,'%Y-%m').replace(tzinfo=timezone.utc)
        end=datetime(start.year+(start.month==12),start.month%12+1,1,tzinfo=timezone.utc)
    except (ValueError,TypeError) as exc:
        raise ContractError('exact YYYY-MM month required') from exc
    if start.strftime('%Y-%m')!=month or int(start.timestamp())*SECOND>=CUTOFF:
        raise UnauthorizedOperation('permitted development month required')
    return int(start.timestamp())*SECOND,min(int(end.timestamp())*SECOND,CUTOFF)


def prepare(root, *, market='ES', month='2024-03', through_month=None, start_date=None, limits=PILOT_LIMITS, resume=False):
    """Read registered metadata/sidecars and file sizes only, never payloads."""
    from .causal_source_closure import (validate_source_contract_metadata,
        validate_full_build_selection_contract,select_preferred_standard_source_entries)
    from .foundation_operation_firewall import issue_current_source_closure_context
    root=Path(root).resolve(strict=True)
    if type(resume) is not bool: raise ContractError('resume must be an explicit boolean')
    through_month=month if through_month is None else through_month
    start_ns=month_window(month)[0]
    end_ns=month_window(through_month)[1]
    if start_date is not None:
        try:
            start=datetime.strptime(start_date,'%Y-%m-%d').replace(tzinfo=timezone.utc)
        except (ValueError,TypeError) as exc:
            raise ContractError('exact YYYY-MM-DD start date required') from exc
        if start.strftime('%Y-%m-%d')!=start_date or start.strftime('%Y-%m')!=month:
            raise ContractError('start date must be canonical and within the first month')
        start_ns=int(start.timestamp())*SECOND
    if start_ns>=end_ns: raise ContractError('range ends before its first month')
    contract=json.loads(contained(root,'configs/source_contract.json').read_bytes())
    validate_source_contract_metadata(root,operation_context=issue_current_source_closure_context(root))
    inventory_path=contract['complete_inventory']['path']
    inventory=json.loads(contained(root,inventory_path).read_bytes())
    validate_full_build_selection_contract(contract,inventory['entries'])
    eligible=[e for e in inventory['entries'] if e['market']==market
        and int(month[:4])<=e['year']<=int(through_month[:4])
        and e['family'] in FAMILIES and e['admitted_standard_foundation'] is True
        and timestamp(e['interval_start_inclusive'])<end_ns
        and timestamp(e['interval_end_exclusive'])>start_ns]
    entries=tuple(select_preferred_standard_source_entries(root,eligible))
    spec=Spec(market,start_ns,end_ns,
        contract['contract_id'],contract['active_canonical_source']['release_id'],
        sha256_json(entries),synthetic_only=False)
    # Reuse the sidecar validator in metadata-only mode, with no decoder opened.
    reader=HistoricalDbnSources.__new__(HistoricalDbnSources)
    SyntheticDbnSources.__init__(reader,root,spec,entries,limits)
    reader._metadata_preparation=True
    units=reader._validated()
    for family in ('definition','ohlcv_1s','ohlcv_1m'):
        cursor=spec.start_ns
        for a,b,_,_ in units[family]:
            if a>cursor: raise IntegrityError('required physical source coverage has a gap')
            cursor=max(cursor,b)
        if cursor<spec.end_ns: raise IntegrityError('required physical source coverage is incomplete')
    for entry in entries:
        if contained(root,entry['path']).stat().st_size!=entry['size_bytes']:
            raise IntegrityError('registered source size changed')
    controls=['configs/source_contract.json',inventory_path,
        'configs/active_dbn_congruence_release_v1.json','configs/active_alpha_research_ladder.json',
        'configs/causal_observation_contract_v1.json','data/causally_gated_normalized/_control/catalog.json']
    return signed({'schema':'causal_1s_native_operation/1','operation':OPERATION,'claim':CLAIM,
        'repository_root':str(root),'spec':asdict(spec),'month':month,'through_month':through_month,'start_date':start_date,'spec_id':spec.id,
        'contract':CONTRACT,'environment':ENVIRONMENT,'implementation':implementation_binding(),
        'controls':{p:sha256_file(contained(root,p)) for p in controls},'entries':list(entries),
        'limits':asdict(limits),'output':f'{BASE}/runs/{market}/{month}/{spec.id}',
        'source_role':'NATIVE_1S_PRIMARY_SUCCESSOR_EXISTING_1M_UNCHANGED',
        'declared_source_bytes':sum(e['size_bytes'] for e in entries if e['kind']=='DBN'),
        'decode_passes':3,'source_integrity_plus_decode_read_multiplier':9,
        'resume':resume,'worker_count':1,'attempt_count':1,
        'authority':{'execution':False,'activation':False,'publication':False,'research':False}})


def require_scope(root, requested):
    """Narrow prospective gateway hook. Installing it is a separate approval."""
    adopted=read_record(contained(root,ADOPTION))
    path=adopted.get('operation_path','')
    if not path.startswith(BASE+'/') or not path.endswith('.json'):
        raise UnauthorizedOperation('invalid monthly operation reference')
    op=read_record(contained(root,path))
    bare={k:v for k,v in requested.items() if k not in APPROVAL_KEYS}
    if (adopted.get('state')!='ADOPTED_INACTIVE_RECEIPT_REQUIRED'
            or adopted.get('scope')!=scope(op) or bare!=scope(op)
            or adopted.get('operation')!=OPERATION
            or adopted.get('operation_sha256')!=sha256_file(contained(root,path))):
        raise UnauthorizedOperation('exact monthly native operation is not adopted')
    return op


def prepare_adoption(root,op,operation_path):
    """Return a reviewable gateway successor and inactive exact-scope config."""
    from hashlib import sha256
    path=contained(root,operation_path)
    if not operation_path.startswith(BASE+'/') or read_record(path)!=op:
        raise IntegrityError('exact prepared operation file required')
    original=contained(root,'src/futures_rebuild/research_gateway_policy.py').read_bytes().decode('utf-8')
    newline='\r\n' if '\r\n' in original else '\n'
    anchor='    from .discovery import require_adopted_scope'+newline
    if original.count(anchor)!=1:
        raise IntegrityError('gateway predecessor is ambiguous')
    hook=(f"    if operation == {OPERATION!r}:\n"
          "        from .causal_1s_native import require_scope\n"
          "        require_scope(Path(__file__).resolve().parents[2], scope)\n"
          "        return\n\n")
    hook=hook.replace('\n',newline)
    if OPERATION in original:
        if original.count(hook)!=1: raise IntegrityError('installed native hook differs')
        successor=original
    else: successor=original.replace(anchor,hook+anchor)
    adopted=signed({'schema':'causal_1s_native_adoption/1','state':'ADOPTED_INACTIVE_RECEIPT_REQUIRED',
        'operation':OPERATION,'scope':scope(op),'operation_path':operation_path,
        'operation_sha256':sha256_file(path),'gateway_sha256':sha256(successor.encode()).hexdigest(),
        'predecessor_gateway_sha256':sha256_file(contained(root,'src/futures_rebuild/research_gateway_policy.py'))})
    return successor,adopted


def authorize(root,op,receipt,*,consumed):
    if type(receipt) is not OperationReceipt:
        raise UnauthorizedOperation('exact external operation receipt required')
    if Path(op['repository_root']).resolve()!=Path(root).resolve():
        raise UnauthorizedOperation('receipt repository differs')
    required=scope(op)
    received=dict(receipt.scope)
    if set(received)-set(required)-APPROVAL_KEYS:
        raise UnauthorizedOperation('unexpected receipt fields')
    required.update({k:v for k,v in received.items() if k in APPROVAL_KEYS})
    if op.get('id')!=sha256_json({k:v for k,v in op.items() if k!='id'}):
        raise IntegrityError('monthly operation identity changed')
    method=receipt.assert_consumed if consumed else receipt.verify
    method(RepoBoundary(Path(root)),operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,required_scope=required)
    return required


class HistoricalDbnSources(SyntheticDbnSources):
    def __init__(self,root,op,receipt,*,_worker_token=None):
        if _worker_token is not _WORKER_TOKEN:
            raise UnauthorizedOperation('historical adapter is private to the supervised worker')
        self.op,self.receipt=op,receipt
        self._metadata_preparation=False
        authorize(root,op,receipt,consumed=True)
        super().__init__(Path(root),Spec(**op['spec']),op['entries'],Limits(**op['limits']),check=self._permission)
        adoption=read_record(contained(self.root,ADOPTION))
        gateway='src/futures_rebuild/research_gateway_policy.py'
        bindings={**op['controls'],**{'src/futures_rebuild/'+p:h for p,h in op['implementation'].items()},
                  gateway:adoption['gateway_sha256'],ADOPTION:sha256_file(contained(self.root,ADOPTION))}
        self._fingerprints={}
        # Keep all containment/link checks, without repeatedly resolving every
        # ancestor into Windows extended-path form for every decode batch.
        self._stat_root=io_path(self.root)
        for p,h in bindings.items():
            before=self._stat(p)
            if sha256_file(contained(self.root,p))!=h or self._stat(p)!=before:
                raise IntegrityError('source control or implementation changed before decoding')
            self._fingerprints[p]=before

    def _stat(self,p):
        s=contained(self._stat_root,p).stat()
        return s.st_size,s.st_mtime_ns,s.st_ino

    def _permission(self):
        authorize(self.root,self.op,self.receipt,consumed=True)
        if any(self._stat(p)!=v for p,v in self._fingerprints.items()):
            raise IntegrityError('source control changed during operation')

    def _admit(self):
        if not self.spec.synthetic_only and getattr(self,'_metadata_preparation',False):
            return  # Only _validated is used by prepare; __call__ below forbids decoding.
        self._permission()

    def __call__(self):
        if getattr(self,'_metadata_preparation',False):
            raise UnauthorizedOperation('metadata preparation cannot decode')
        self._permission()
        yield from super().__call__()

    def authorize_output(self,output,spec,limits,resume,kind):
        self._permission()
        if (Path(output).resolve()!=contained(self.root,self.op['output']).resolve()
                or spec!=self.spec or kind not in ('BUILD','CERTIFY')
                or kind=='BUILD' and resume!=self.op['resume'] or limits!=replace(self.limits,
                    maximum_written_bytes=self.limits.maximum_written_bytes-128*1024)):
            raise UnauthorizedOperation('native destination, limits, mode or resume differs')


def failure_details(exc,factory):
    """Bounded diagnostics: static guard messages and locations, never row values/locals."""
    def text(value,limit):
        return ''.join(c if 32<=ord(c)<=126 else '?' for c in str(value)[:limit])
    errors=[]
    seen=set()
    while exc is not None and id(exc) not in seen and len(errors)<2:
        seen.add(id(exc))
        frames=[]
        tb=exc.__traceback__
        code=None
        while tb is not None:
            code=tb.tb_frame.f_code
            frames.append({'file':text(Path(code.co_filename).name,96),
                'function':text(code.co_name,48),'line':tb.tb_lineno})
            frames=frames[-4:]
            tb=tb.tb_next
        # Dynamic decoder messages may contain market values. Only a literal
        # in the actual raising function can be persisted as the guard message.
        message=str(exc)
        static=code is not None and any(type(c) is str and c==message for c in code.co_consts)
        errors.append({'type':text(type(exc).__name__,64),
            'message':text(message,200) if static else 'DYNAMIC_MESSAGE_OMITTED',
            'traceback':frames[-4:]})
        exc=exc.__cause__ if exc.__cause__ is not None else (None if exc.__suppress_context__ else exc.__context__)
    family=getattr(factory,'source_family',None)
    return {'errors':errors,'processing_day_ns':getattr(factory,'processing_day_ns',None),
        'last_read_family':text(family,32) if family is not None else None,
        'last_read_source':text(getattr(factory,'source_paths',{}).get(family,''),192),
        'source_cursor_basis':'LAST_READ_OR_LOOKAHEAD_NOT_PROOF_OF_OFFENDING_SOURCE'}


def _worker():
    """Private child protocol: parent assigns the Windows job before sending input."""
    from .causal_1s import build_candidate,certify_candidate
    from .foundation.economics import EconomicsRuleBook
    request=json.loads(sys.stdin.buffer.readline(2**20))
    root=Path(request['operation']['repository_root'])
    op=request['operation']
    from .durable_windows_task_transport import _ExtendedLimitInformation
    information=_ExtendedLimitInformation()
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    opener=kernel.OpenJobObjectW
    opener.argtypes=[ctypes.c_ulong,ctypes.c_int,ctypes.c_wchar_p]
    opener.restype=ctypes.c_void_p
    handle=opener(4,False,request['supervision_job_name'])
    if not handle: raise UnauthorizedOperation('supervisor job is unavailable')
    query=kernel.QueryInformationJobObject
    query.argtypes=[ctypes.c_void_p,ctypes.c_int,ctypes.c_void_p,ctypes.c_ulong,ctypes.c_void_p]
    belongs=ctypes.c_int()
    kernel.IsProcessInJob.argtypes=[ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p]
    kernel.CloseHandle.argtypes=[ctypes.c_void_p]
    try:
        measured=query(handle,9,ctypes.byref(information),ctypes.sizeof(information),None)
        membership=kernel.IsProcessInJob(ctypes.c_void_p(-1),handle,ctypes.byref(belongs))
    finally: kernel.CloseHandle(handle)
    if (not measured or not membership or not belongs.value
            or information.BasicLimitInformation.LimitFlags & 0x2100 != 0x2100
            or information.ProcessMemoryLimit!=op['limits']['maximum_memory_bytes']):
        raise UnauthorizedOperation(f'worker job limits differ: query={measured}, flags={information.BasicLimitInformation.LimitFlags}, memory={information.ProcessMemoryLimit}')
    receipt=OperationReceipt.from_dict(request['receipt'])
    if prepare(root,market=op['spec']['market'],month=op['month'],through_month=op['through_month'],start_date=op['start_date'],limits=Limits(**op['limits']),resume=op['resume'])!=op:
        raise IntegrityError('prepared monthly operation is stale')
    factory=HistoricalDbnSources(root,op,receipt,_worker_token=_WORKER_TOKEN)
    limits=replace(factory.limits,maximum_written_bytes=factory.limits.maximum_written_bytes-128*1024)
    rulebook=EconomicsRuleBook.from_file(contained(root,'configs/contract_economics_rules.json'))
    output=contained(root,op['output'])
    phase='BUILD'
    try:
        build_candidate(output,factory.spec,factory,rulebook,limits,resume=op['resume'])
        phase='CERTIFY'
        certify_candidate(output,factory.spec,factory,rulebook,limits)
    except Exception as exc:
        failure=Budget(output,replace(limits,maximum_written_bytes=4096))
        failure.finalizing=True
        failure.save(output/f"worker.{op['id']}.failure.json",signed({'phase':phase,
            'error_type':type(exc).__name__,**failure_details(exc,factory)}))
        raise
    factory._permission()
    if prepare(root,market=op['spec']['market'],month=op['month'],through_month=op['through_month'],start_date=op['start_date'],limits=factory.limits,resume=op['resume'])!=op:
        raise IntegrityError('source or implementation changed during complete run')


def supervise(command,root,request,limits):
    """One child, OS commit-memory limit, whole-attempt timeout, no growing logs."""
    from .durable_windows_task_transport import WindowsKillOnCloseJob,_ExtendedLimitInformation
    if os.name!='nt': raise IntegrityError('native supervisor currently requires Windows')
    started=time.monotonic()
    with WindowsKillOnCloseJob() as job:
        # Querying a null job handle in a nested Codex job returns the outer job.
        # Name this owned job so the child checks its actual supervisor's limits.
        job_name='Local\\causal_1s_'+uuid4().hex
        named=job._kernel32.CreateJobObjectW(None,job_name)
        if not named: raise IntegrityError('named supervisor job could not be created')
        job.close()
        job._handle=named
        information=_ExtendedLimitInformation()
        information.BasicLimitInformation.LimitFlags=0x2000|0x100  # kill on close + process memory
        information.ProcessMemoryLimit=limits.maximum_memory_bytes
        if not job._kernel32.SetInformationJobObject(job._handle,9,ctypes.byref(information),ctypes.sizeof(information)):
            raise IntegrityError('OS memory limit could not be installed')
        child=subprocess.Popen(command,cwd=root,stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,creationflags=subprocess.CREATE_NO_WINDOW)
        try:
            job.assign(child)
            raw=canonical_bytes({**request,'supervision_job_name':job_name})+b'\n'
            if len(raw)>2**20: raise IntegrityError('bounded worker request exceeded')
            try:
                child.communicate(input=raw,timeout=max(0.001,limits.maximum_seconds-(time.monotonic()-started)))
                code=child.returncode
            except subprocess.TimeoutExpired:
                job.terminate()
                child.wait(timeout=10)
                raise IntegrityError('whole-attempt runtime limit reached')
            if code!=0: raise IntegrityError(f'native worker failed or exceeded memory cap: {code}')
        finally:
            if child.poll() is None:
                job.terminate()
                child.wait(timeout=10)
    return time.monotonic()-started


def execute_once(root,op,receipt):
    """Owner-approved historical operation only; never publishes or activates."""
    root=Path(root).resolve(strict=True)
    limits=Limits(**op['limits'])
    required=authorize(root,op,receipt,consumed=False)
    if prepare(root,market=op['spec']['market'],month=op['month'],through_month=op['through_month'],start_date=op['start_date'],limits=limits,resume=op['resume'])!=op:
        raise IntegrityError('prepared native operation is stale')
    destination=contained(root,op['output'])
    if destination.exists()!=op['resume']:
        raise IntegrityError('destination existence differs from the approved fresh/resume scope')
    if op['resume'] and read_record(destination/'attempt.json')['spec_id']!=op['spec_id']:
        raise IntegrityError('resumed source, implementation or semantics differ')
    # The conservative allocation bound assumes clusters no larger than 64 KiB.
    if os.name!='nt': raise IntegrityError('native execution requires Windows supervision')
    clusters=ctypes.c_ulong()
    sector=ctypes.c_ulong()
    free_clusters=ctypes.c_ulong()
    total_clusters=ctypes.c_ulong()
    if (not ctypes.windll.kernel32.GetDiskFreeSpaceW(str(root.anchor),ctypes.byref(clusters),
            ctypes.byref(sector),ctypes.byref(free_clusters),ctypes.byref(total_clusters))
            or clusters.value*sector.value>65536):
        raise IntegrityError('filesystem allocation bound is unsupported')
    allocation_bound=limits.maximum_written_bytes+limits.maximum_files*65536
    if shutil.disk_usage(root).free<limits.minimum_free_bytes+allocation_bound:
        raise IntegrityError('pilot capacity reserve insufficient')
    receipt.consume(RepoBoundary(root),operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,required_scope=required)
    destination.mkdir(parents=True,exist_ok=op['resume'])
    supervision=destination/'supervision'
    supervision.mkdir(exist_ok=op['resume'])
    # Reserve fixed metadata space from the shared application-write cap.
    budget=Budget(destination,replace(limits,maximum_written_bytes=128*1024))
    receipt_ids={read_record(p)['receipt_id'] for p in supervision.glob('*.start.json')}|{receipt.receipt_id}
    ledger_bytes=sum(contained(root,f'state/authorization_uses/{rid}.json').stat().st_size for rid in receipt_ids)
    budget.written=(sum(p.stat().st_size for p in supervision.iterdir())
                    +sum(p.stat().st_size for p in destination.glob('worker.*.failure.json'))+ledger_bytes)
    budget.save(supervision/f"{op['id']}.start.json",signed({'operation_id':op['id'],'receipt_id':receipt.receipt_id}))
    status='FAILED'
    started=time.monotonic()
    try:
        supervise([sys.executable,'-B','-c','from futures_rebuild.causal_1s_native import _worker; _worker()'],
            root,{'operation':op,'receipt':receipt.as_dict()},limits)
        certificate=read_record(destination/'certificate.json')
        if (certificate['spec_id']!=op['spec_id'] or certificate['status']!='HISTORICAL_RECONSTRUCTION_CERTIFIED_NOT_ACTIVE'
                or len(certificate['passes'])!=2 or certificate['passes'][0]!=certificate['passes'][1]):
            raise IntegrityError('worker certificate does not prove complete reconstruction')
        status='COMPLETE'
    finally:
        budget.finalizing=True
        finishes=[read_record(p) for p in (destination/'runs').glob('*.finish.json')]
        written=sum(f['written_before_finish']+len(canonical_bytes(f))+1 for f in finishes)
        resources=budget.record()
        budget.save(supervision/f"{op['id']}.finish.json",signed({'operation_id':op['id'],'status':status,
            'spec_id':op['spec_id'],'certificate_id':certificate['id'] if status=='COMPLETE' else None,
            'seconds':time.monotonic()-started,'supervisor_written_before_finish':budget.written,
            'child_application_written_bytes':written,'resources':resources,
            'authorization_ledger_bytes':ledger_bytes,'authorization_ledger_files':len(receipt_ids),
            'peak_allocation_bound_bytes':limits.maximum_written_bytes+limits.maximum_files*65536,
            'peak_allocation_basis':'CONSERVATIVE_64K_PER_FILE_SLACK_PLUS_CUMULATIVE_WRITE_CAP',
            'logs_bytes':0,'limits':asdict(limits),'publication':False}))
    return read_record(destination/'certificate.json')


def completed_supervision(directory,spec_id,certificate_id):
    """A previous failure cannot stand in for the completed whole-attempt proof."""
    records=[read_record(p) for p in (directory/'supervision').glob('*.finish.json')]
    matches=[r for r in records if r['status']=='COMPLETE' and r['spec_id']==spec_id
             and r['certificate_id']==certificate_id]
    if not matches: raise IntegrityError('complete supervised certification is absent')
    return sorted(matches,key=lambda r:r['operation_id'])[-1]
