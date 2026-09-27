"""Prepare a repeatable active-daily audit; historical execution is task-approved."""
from pathlib import Path
from dataclasses import asdict, replace
import json
import sys

from . import daily_foundation_admission as daily
from .daily_source_admission import DailySourceReader, POINTER
from .hourly_source_admission import verify_bindings
from .canonical import canonical_bytes, sha256_file
from .causal_timeframe_storage import contained, read_record, signed, Budget, memory_peak
from .causal_timeframe_execution import execution_lease

HERE='state/unpublished_evidence/daily_admission_repeat_audit_v1'
LIMITS=replace(daily.LIMITS,maximum_written_bytes=2*2**30,maximum_seconds=4*3600)
# Identity recovery preserved these source deliveries; its audit record no longer
# repeats this reference from the original admission audit.
RETAINED_SOURCE_PLAN=dict(
    path='state/unpublished_evidence/daily_admission_v1/e7bcf65f5c229b05432d8e9ca4e7fcd590d9b4ae5715b1b817f7df30b3d8103f/plan.json',
    sha256='212c6484b9b664197f07741da5c73eec68a902671b2f0c51ba4409f24a74cd99')


def prepare(root):
    root=Path(root).resolve()
    pointer=read_record(contained(root,POINTER))
    bindings={POINTER:sha256_file(contained(root,POINTER))}
    for ref in (pointer['manifest'],pointer['audit'],pointer['supervision']):
        bindings[ref['path']]=ref['sha256']
    verify_bindings(root,bindings)
    manifest=read_record(contained(root,pointer['manifest']['path']))
    bindings.update(manifest['bindings'])
    audit=read_record(contained(root,pointer['audit']['path']))
    source=audit.get('retained_source_plan')
    if source is None:
        daily.require(audit.get('integrity')=='CERTIFIED_NATIVE_RECOVERY_AND_BOUND_REPLAY'
            and audit.get('recovered_rows',0)>0
            and audit.get('quality')==manifest['quality']['path'],
            'Audit has no supported retained source inventory')
        source=RETAINED_SOURCE_PLAN
    bindings[source['path']]=source['sha256']
    verify_bindings(root,bindings)
    daily.require(manifest['daily_implementation']==daily.implementation_binding(),'Daily implementation changed')
    source_plan=read_record(contained(root,source['path']))
    return signed(dict(status='PREPARED_REQUIRES_OWNER_APPROVAL',schema='daily_admission_repeat_audit/1',
        bindings=bindings,source_bindings=source_plan['source_bindings'],limits=asdict(LIMITS),
        manifest=pointer['manifest'],quality=manifest['quality'],audit=pointer['audit'],
        implementation={'src/futures_rebuild/daily_admission_audit.py':sha256_file(contained(root,'src/futures_rebuild/daily_admission_audit.py'))},
        historical_execution=False,publication=False,model_specific_eligibility='NOT_DECLARED'))


def current_accounting(quality):
    daily.require(quality['baseline']==daily.BASELINE,'Quality baseline differs')
    recovered=quality.get('recovered_rows',0)
    daily.require(type(recovered) is int and 0<=recovered<=daily.BASELINE['quarantined']
        and quality.get('corrected_rows',0)==0,'Unsupported recovery accounting')
    expected=daily.BASELINE|dict(observations=daily.BASELINE['observations']+recovered,
        quarantined=daily.BASELINE['quarantined']-recovered)
    daily.require(quality.get('current_source_accounting',quality['baseline'])==expected
        and len(quality['rows'])==expected['observations'],'Current daily accounting differs')
    return expected


def reader_examples(root,plan,manifest,quality):
    if not quality.get('recovered_rows',0):
        return daily.reader_examples(manifest,quality,root)
    reference=plan['audit']
    verify_bindings(root,{reference['path']:reference['sha256']})
    audit=read_record(contained(root,reference['path']))
    checks=audit.get('daily_reader_checks',[])
    daily.require(audit.get('recovered_rows')==quality['recovered_rows'] and checks
        and all(c.get('result') in ('ACCEPTED','REFUSED') for c in checks)
        and len({c['case'] for c in checks})==len(checks),
        'Bound recovery reader cases are incomplete or ambiguous')
    # Historical seals retain quarantine evidence. Reuse the passing successor's
    # explicitly revised cases, including recovered-day and premature reads.
    return {c['case']:(c['request'],c['result']=='ACCEPTED') for c in checks}


def worker():
    request=json.loads(sys.stdin.buffer.readline(2**20))
    root=Path.cwd().resolve()
    plan=read_record(contained(root,request['plan_path']))
    auth=read_record(contained(root,request['authorization_path']))
    daily.require(auth['status']=='OWNER_APPROVED_IN_THIS_TASK' and auth['plan_id']==plan['id']
        and auth['plan_sha256']==sha256_file(contained(root,request['plan_path'])),'Repeat audit approval differs')
    verify_bindings(root,plan['implementation'])
    output=contained(root,request['operation']['output'])
    output.mkdir(parents=True,exist_ok=False)
    budget=Budget(output,replace(LIMITS,maximum_memory_bytes=5*2**30))
    result=dict(status='INCOMPLETE',plan_id=plan['id'])
    try:
        verify_bindings(root,plan['bindings'])
        verify_bindings(root,plan['source_bindings'])
        from . import causal_timeframe_publication as pub
        index=read_record(contained(root,pub.INDEX))
        tables=pub._validate_population(root,index,budget)
        daily_tables={k:v for k,v in tables.items() if k.startswith('1d:')}
        entries=[e for e in index['entries'] if e['timeframe']=='1d']
        quality=read_record(contained(root,plan['quality']['path']))
        accounting=current_accounting(quality)
        daily.require(len(daily_tables)==5 and set(daily_tables.values())=={accounting['observations']}
            and len(entries)==6862 and len({e['market'] for e in entries})==41
            and sum(e['disposition']=='UNAVAILABLE' for e in entries)==21,'Daily census differs')
        manifest=read_record(contained(root,plan['manifest']['path']))
        daily.require(len(manifest['entries'])==accounting['observations'],'Admission population differs')
        examples=reader_examples(root,plan,manifest,quality)
        counts=dict(baseline=quality['baseline'],current_source_accounting=accounting,
            quality_classes=quality['reason_counts'],
            admission_entries=len(manifest['entries']))
        del quality,manifest
        reader=DailySourceReader(root)
        checks=daily.check_reader_examples(reader,examples,budget)
        raw=daily.check_raw_routes(root,index)
        verify_bindings(root,plan['bindings'])
        result.update(status='PASS_ACTIVE_DAILY_FOUNDATION_AUDIT',table_counts=tables,
            **counts,representative_reader_checks=checks,blocked_raw_routes=raw,
            model_specific_eligibility='NOT_DECLARED')
    except Exception as exc:
        result.update(error_type=type(exc).__name__,error=str(exc))
    budget.finalizing=True
    result['resources']=budget.record()
    budget.save(output/'result.json',signed(result))
    return 0 if result['status'].startswith('PASS_') else 1


def supervise(root,plan_path,authorization_path):
    """Called only by the separately approved task, never by the preparation CLI."""
    from .causal_1h_native import supervise as native_supervise
    root=Path(root).resolve()
    plan=read_record(contained(root,plan_path))
    daily.require(plan_path==HERE+'/'+plan['id']+'/plan.json','Noncanonical audit plan')
    auth=read_record(contained(root,authorization_path))
    daily.require(auth['status']=='OWNER_APPROVED_IN_THIS_TASK' and auth['plan_id']==plan['id']
        and auth['plan_sha256']==sha256_file(contained(root,plan_path)),'Repeat audit approval differs')
    directory=contained(root,HERE+'/'+plan['id'])
    output=HERE+'/'+plan['id']+'/execution'
    daily.require(not contained(root,output).exists() and not (directory/'supervision.json').exists(),'Audit already attempted')
    record=dict(status='INCOMPLETE',plan_id=plan['id'])
    with execution_lease(root):
        try:
            daily.require(memory_peak()<2**30,'Coordinator reserve exceeded')
            seconds=native_supervise([sys.executable,'-B','-c',
                'from futures_rebuild.daily_admission_audit import worker; raise SystemExit(worker())'],root,
                dict(plan_path=plan_path,authorization_path=authorization_path,operation=dict(id=plan['id'],output=output)),
                replace(LIMITS,maximum_memory_bytes=5*2**30))
            result=read_record(contained(root,output+'/result.json'))
            peak=result['resources']['peak_process_memory_bytes']+2**30
            daily.require(result['status']=='PASS_ACTIVE_DAILY_FOUNDATION_AUDIT' and peak<=6*2**30,'Repeat audit failed')
            record.update(status='PASS_SUPERVISED_ACTIVE_DAILY_AUDIT',seconds=seconds,
                result_sha256=sha256_file(contained(root,output+'/result.json')),conservative_aggregate_peak_bytes=peak)
        except Exception as exc:
            record.update(error_type=type(exc).__name__,error=str(exc))
        with (directory/'supervision.json').open('xb') as stream: stream.write(canonical_bytes(signed(record))+b'\n')
    return record


def main():
    root=Path.cwd()
    plan=prepare(root)
    directory=contained(root,HERE+'/'+plan['id'])
    directory.mkdir(parents=True,exist_ok=False)
    (directory/'plan.json').write_bytes(canonical_bytes(plan)+b'\n')
    print(json.dumps(dict(status=plan['status'],plan_path=HERE+'/'+plan['id']+'/plan.json',
        execution='An approved task invokes daily_admission_audit.supervise; this command performs no historical audit.')))


if __name__=='__main__': main()
