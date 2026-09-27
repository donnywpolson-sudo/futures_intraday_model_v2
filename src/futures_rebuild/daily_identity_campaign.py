"""Task-only preparation, isolated validation and bounded native daily recovery.

No command in this module starts a historical operation or changes active controls.
"""
from contextlib import contextmanager, ExitStack
from importlib import import_module
from pathlib import Path
from unittest.mock import patch
from copy import deepcopy
import os

from .canonical import sha256_file, sha256_json, canonical_bytes, io_path
from .causal_timeframe_storage import contained as canonical_contained, read_record, signed, MonthlyWriter, file_inventory, table_metadata
from .daily_identity_review import require, RULE, DAY
from .daily_identity_recovery import recover_month

HERE = 'state/unpublished_evidence/daily_identity_recovery_v1/implementation_v1'
CODE = ('daily_identity_recovery.py','daily_identity_transform.py','daily_identity_replay.py',
        'daily_identity_publication.py','daily_identity_campaign.py','daily_identity_review.py',
        'hourly_quality_reader.py','hourly_source_admission.py','daily_source_admission.py',
        'daily_foundation_admission.py','daily_identity_rollout.py','coverage_remediation.py')
CHANGES = ('src/futures_rebuild/causal_timeframe_publication.py',
    'src/futures_rebuild/daily_quality_reader.py', 'data/active/timeframes.json',
    'configs/active_hourly_quality_reader.json', 'configs/active_hourly_source_admission.json',
    'configs/active_daily_source_admission.json')


def contained(root, relative):
    # Preserve every canonical path check; avoid resolving every Windows ancestor
    # repeatedly by supplying the already-supported extended root representation.
    return canonical_contained(io_path(Path(root)),relative)


@contextmanager
def projected_successor(root, overrides):
    """Project exact candidate files only in an isolated task worker.

    All normal validators run. Hash reads and module loading see the same prepared
    bytes; no binding is waived. This must not run in the live application.
    """
    root = Path(root).resolve()
    def path_key(path): return os.path.normcase(str(io_path(Path(path).resolve())))
    checked = {}
    for target, relative in overrides.items():
        original = contained(root,target)
        candidate = contained(root,relative)
        require(relative.startswith('state/unpublished_evidence/') and candidate.is_file(), 'Invalid preview file')
        checked[path_key(original)] = candidate
    def project(path):
        return checked.get(path_key(path),path)
    def projected_path(base, relative): return project(contained(base,relative))
    def projected_hash(path): return sha256_file(project(path))
    modules = [import_module('futures_rebuild.'+name) for name in (
        'causal_timeframe_publication','daily_quality_reader','daily_source_admission',
        'hourly_quality_reader','hourly_source_admission','daily_foundation_admission',
        'daily_identity_publication','causal_1d')]
    with ExitStack() as stack:
        stack.enter_context(patch.object(modules[-1],'__file__',str(contained(root,'src/futures_rebuild/causal_1d.py'))))
        for module in modules:
            if hasattr(module,'contained'): stack.enter_context(patch.object(module,'contained',projected_path))
            if hasattr(module,'sha256_file'): stack.enter_context(patch.object(module,'sha256_file',projected_hash))
        pub, reader, admission = modules[:3]
        for module, filename in ((pub,'causal_timeframe_publication.py'),(reader,'daily_quality_reader.py')):
            relative = 'src/futures_rebuild/'+filename
            require(relative in overrides, 'Prepared implementation file absent')
            path = contained(root,overrides[relative])
            namespace = dict(__name__='futures_rebuild._prepared_'+filename[:-3],__package__='futures_rebuild',__file__=str(path))
            exec(compile(path.read_text(),str(path),'exec'),namespace)
            namespace['contained'], namespace['sha256_file'] = projected_path, projected_hash
            for key,value in namespace.items():
                if callable(value) and getattr(value,'__module__',None)==namespace['__name__']:
                    stack.enter_context(patch.object(module,key,value))
        stack.enter_context(patch.object(admission,'DailyReader',reader.DailyReader))
        yield


def implementation(root):
    root=Path(root)
    from .causal_1d import implementation_binding
    result={'src/futures_rebuild/'+k:v for k,v in implementation_binding().items()}
    result.update({'src/futures_rebuild/'+p:sha256_file(contained(root,'src/futures_rebuild/'+p)) for p in CODE})
    for name in ('causal_timeframe_publication.py','daily_quality_reader.py'):
        result['src/futures_rebuild/'+name]=sha256_file(contained(root,HERE+'/candidate/'+name))
    return result


def reconstruct_months(root, entries, *, hourly_reader, rulebook, output, plan_id, budget, replay=False, expected=None):
    """One sequential pass over explicitly authorized certified daily entries.

    Normal predecessor verification authenticates source replay and all five old
    tables. Only changed months are written. Replay reconstructs from predecessors
    independently and compares logical output hashes, never producer row values.
    """
    from .causal_timeframe_publication import validate_release
    from .causal_observation_parquet import read_bundle
    from .daily_identity_publication import SCHEMA
    root=Path(root)
    keys=[(e['market'],e['month']) for e in entries]
    require(keys==sorted(set(keys)), 'Monthly reconstruction must be unique and chronological')
    results=[]
    last=None
    market=None
    cases=0
    observed=0
    for entry in entries:
        budget.check()
        require(entry['timeframe']=='1d', 'Only native daily entries may be reconstructed')
        if entry['market'] != market:
            last, market = None, entry['market']
        if entry['disposition']=='UNAVAILABLE':
            continue
        parent=validate_release(root,entry)
        require(parent['schema']!='daily_identity_recovery_month/1', 'Recovery successor already active')
        old=read_bundle(contained(root,entry['directory']))
        coverage=parent['seal']['coverage']
        inputs={}
        reference_hashes={e['sha256'] for e in parent['candidate']['source_inventory']
                          if e['kind']=='DBN' and e['family']=='ohlcv_1h'} if not parent['candidate']['spec']['synthetic_only'] else set()
        if any(c['quarantined'] for c in coverage):
            hours=hourly_reader._native_month(market,entry['month'])
            if parent['candidate']['spec']['synthetic_only']:
                reference_hashes={h['source_file_sha256'] for h in hours}
            for c in coverage:
                if c['quarantined']:
                    item,=c['quarantine']
                    inputs[c['day_ns']]=(item,[h for h in hours if c['day_ns']<=h['bar_start_ns']<c['day_ns']+DAY])
        revised=recover_month(old,coverage,inputs,reference_hashes=reference_hashes,rulebook=rulebook,prior=last,replay=replay)
        last=revised['last_observation']
        cases+=len(inputs)
        observed+=len(revised['tables']['observations'])
        changed=revised['tables']!=old or revised['coverage']!=coverage
        logical=sha256_json(dict(tables=revised['tables'],coverage=revised['coverage'],evidence=revised['evidence']))
        accounting=dict(market=market,month=entry['month'],changed=changed,logical_sha256=logical,
            recovered=len(inputs),observations=len(revised['tables']['observations']))
        if replay:
            require(expected is not None and accounting==expected[len(results)], 'Independent recovery reconstruction differs')
            if changed:
                directory=output+'/'+market+'/'+entry['month']
                base=read_record(contained(root,directory+'/content.json'))
                require(read_bundle(contained(root,directory+'/payload'))==revised['tables']
                    and base['coverage']==revised['coverage'] and base['evidence']==revised['evidence']
                    and base['predecessor']==entry
                    and file_inventory(contained(root,directory+'/payload'),budget)==base['files'],
                    'Independent reconstruction differs from written five-table output')
        elif changed:
            directory=output+'/'+market+'/'+entry['month']
            writer=MonthlyWriter(contained(root,directory+'/payload'),budget)
            try:
                tables=revised['tables']
                for row in tables['observations']:
                    rid=row['row_id']
                    group={k:[row] if k=='observations' else
                        [r for r in tables[k] if r.get('row_id',r.get('observation_row_id'))==rid]
                        for k in tables}
                    require(len(group['missingness'])==1, 'Recovery daily missingness group is ambiguous')
                    writer.write(group)
            finally: writer.close()
            files=file_inventory(contained(root,directory+'/payload'),budget)
            base=dict(schema=SCHEMA,rule=RULE,predecessor=entry,coverage=revised['coverage'],
                evidence=revised['evidence'],files=files,table_metadata=table_metadata(contained(root,directory+'/payload')),
                permitted_uses=['LINEAGE_AUDIT'])
            budget.save(contained(root,directory+'/content.json'),signed(base))
        results.append(accounting)
        budget.check()
    if replay: require(len(results)==len(expected), 'Independent monthly scope differs')
    hourly_reader._unchanged()
    return dict(months=results,recovered=cases,observations=observed)


def output_records(root, output, accounting):
    """Small certificate census; unchanged months stay in their original releases."""
    records=[]
    for month in accounting['months']:
        if month['changed']:
            base=read_record(contained(Path(root),output+'/'+month['market']+'/'+month['month']+'/content.json'))
            records.append(dict(market=month['market'],month=month['month'],content_id=base['id']))
    return records


def prepare(root):
    """Bind metadata and physical paths; do not read source rows or table payloads."""
    from dataclasses import asdict
    from .causal_timeframe_storage import Limits
    from .causal_1d import implementation_binding, ENVIRONMENT
    root=Path(root).resolve()
    scope_path='state/unpublished_evidence/daily_identity_recovery_v1/dependency_review_v1/scope.json'
    scope=read_record(contained(root,scope_path))
    for p,h in scope['bindings'].items(): require(sha256_file(contained(root,p))==h,'Prepared predecessor changed: '+p)
    review_prefix=scope['retained_assessment'].rsplit('/execution/',1)[0]
    review_plan=read_record(contained(root,review_prefix+'/plan.json'))
    old_coverage=read_record(contained(root,
        'state/unpublished_evidence/coverage_remediation_v1/f9c1d233fd48e4b54147a21672a49a870e1eee054167d6bf89b0918b6809089d/plan.json'))
    bindings=dict(scope['bindings'])
    bindings[scope_path]=sha256_file(contained(root,scope_path))
    inputs=dict(old_coverage['historical_bindings'])
    inputs.update(review_plan['inputs'])
    index=read_record(contained(root,'data/active/timeframes.json'))
    from .causal_observation_parquet import FILENAMES
    tables=[]
    for entry in index['entries']:
        if entry['timeframe']=='1d' and entry['disposition']=='CERTIFIED':
            inputs[entry['manifest']]=entry['manifest_sha256']
            for table,filename in FILENAMES.items():
                path=entry['directory']+'/'+filename
                tables.append(dict(path=path,size=contained(root,path).stat().st_size,
                    expected_hash_from_manifest=entry['manifest'],table=table))
    for p in (review_prefix+'/execution/cases.json',
              'state/unpublished_evidence/daily_foundation_v1/full_campaign_execution_v1/accounting.json',
              'state/unpublished_evidence/daily_foundation_v1/prepare_coordinated_publication.py',
              'state/unpublished_evidence/coverage_remediation_v1/f9c1d233fd48e4b54147a21672a49a870e1eee054167d6bf89b0918b6809089d/execution/coverage.json',
              'state/unpublished_evidence/daily_admission_v1/incremental_recovery_v1/execution/reader_examples_census_v2.json'):
        bindings[p]=sha256_file(contained(root,p))
    accounting=read_record(contained(root,'state/unpublished_evidence/daily_foundation_v1/full_campaign_execution_v1/accounting.json'))
    affected={m['market'] for m in review_plan['quarantine_months']}
    native=[e for e in index['entries'] if e['timeframe']=='1d']
    entries=[e for e in native if e['market'] in affected]
    require(len(entries)==6316 and len(native)==6862 and len(tables)==6841*5, 'Prepared monthly scope differs')
    candidate_code=implementation(root)
    preparation={p:sha256_file(contained(root,p)) for p in
        ('tests/test_daily_identity_recovery.py','tests/test_daily_identity_publication.py',
         'tests/test_daily_identity_rollout.py','tests/test_daily_identity_performance.py')}
    for p in candidate_code:
        if p not in CHANGES[:2]: preparation[p]=candidate_code[p]
    for target in CHANGES[:2]:
        p=HERE+'/candidate/'+Path(target).name
        preparation[p]=sha256_file(contained(root,p))
    limits=Limits(6*2**30,5*2**30,6*3600,maximum_files=50000)
    return signed(dict(schema='daily_identity_recovery_plan/1',status='PREPARED_NOT_AUTHORIZED',
        rule=RULE,scope=scope_path,bindings=bindings,preparation_bindings=preparation,
        implementation=candidate_code,legacy_implementation=implementation_binding(),environment=ENVIRONMENT,
        legacy_spec_ids=sorted({e['spec_id'] for e in native}),entries=entries,
        input_bindings=inputs,daily_tables=tables,
        physical_input_bytes=sum(contained(root,p).stat().st_size for p in inputs)+sum(e['size'] for e in tables),
        assessment_cases=review_prefix+'/execution/cases.json',expected_recovered=12583,
        expected_previous_observations=sum(m['source_accounting']['observations'] for m in accounting['monthly'] if m['market'] in affected),
        baseline=accounting['source_accounting'],limits=asdict(limits),
        maximum_coordinator_memory_bytes=960*2**20,maximum_aggregate_memory_bytes=6*2**30,
        maximum_cumulative_written_bytes=8*2**30,maximum_cumulative_files=100000,
        maximum_allocation_bytes=10*2**30,minimum_free_bytes=64*2**30,
        publication_order=list(CHANGES),predecessors={p:sha256_file(contained(root,p)) for p in CHANGES},
        output=HERE+'/execution',rollback=HERE+'/rollback',automatic_retry=False,
        historical_execution=False,conditional_publication=False,model_specific_eligibility='NOT_DECLARED'))


def verify_inputs(root, plan, budget):
    """Authorized worker: bind every physical file, including exact annual DBNs."""
    root=Path(root)
    from .causal_1d import ENVIRONMENT
    require(plan['environment']==ENVIRONMENT,'Prepared runtime environment changed')
    for path,digest in plan['bindings'].items(): require(budget.hash(contained(root,path))==digest,'Input binding changed: '+path)
    for path,digest in plan['preparation_bindings'].items(): require(budget.hash(contained(root,path))==digest,'Prepared code changed: '+path)
    for path,digest in plan['input_bindings'].items(): require(budget.hash(contained(root,path))==digest,'Historical input changed: '+path)
    last_path=None
    for table in plan['daily_tables']:
        if table['expected_hash_from_manifest']!=last_path:
            last_path=table['expected_hash_from_manifest']
            manifest=read_record(contained(root,last_path))
        expected=manifest['seal']['files'][table['table']]
        path=contained(root,table['path'])
        require(path.name==expected['filename'] and path.stat().st_size==expected['size']==table['size']
            and budget.hash(path)==expected['sha256'],'Daily table changed: '+table['path'])


def reconstruct_and_certify(root, plan, receipt, budget):
    """Supervised caller must supply exact external historical authorization."""
    from .boundary import OperationReceipt, OperationClassification, RepoBoundary
    from .causal_timeframe_publication import CLAIM, APPROVAL_KEYS
    from .daily_identity_publication import OPERATION
    from .hourly_quality_reader import HourlyReader
    from .causal_observation_canary import _load_economics_rulebook
    root=Path(root).resolve()
    require(type(receipt) is OperationReceipt and plan['id']==sha256_json({k:v for k,v in plan.items() if k!='id'}),
            'Exact plan and historical receipt required')
    scope={'plan_id':plan['id'],'claim':CLAIM}
    received=dict(receipt.scope)
    require(set(received)==set(scope)|APPROVAL_KEYS,'Unexpected historical receipt scope')
    scope.update({k:received[k] for k in APPROVAL_KEYS})
    kwargs=dict(operation=OPERATION,classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,required_scope=scope)
    receipt.verify(RepoBoundary(root),**kwargs)
    output=plan['output']+'/reconstruction'
    require(not contained(root,output).exists(),'Recovery already attempted; no automatic retry')
    verify_inputs(root,plan,budget)
    receipt.consume(RepoBoundary(root),**kwargs)
    contained(root,output).mkdir(parents=True,exist_ok=False)
    reader=HourlyReader(root)
    args=dict(hourly_reader=reader,rulebook=_load_economics_rulebook(root),output=output,
              plan_id=plan['id'],budget=budget)
    result=reconstruct_months(root,plan['entries'],**args)
    require(result['recovered']==plan['expected_recovered']
        and result['observations']==plan['expected_previous_observations']+result['recovered'], 'Recovery population differs')
    passes=[]
    for _ in range(2):
        rebuilt=reconstruct_months(root,plan['entries'],**args,replay=True,expected=result['months'])
        passes.append(dict(accounting=rebuilt,outputs=output_records(root,output,rebuilt)))
    require(passes[0]==passes[1],'Independent certification passes disagree')
    assessed=read_record(contained(root,plan['assessment_cases']))['cases']
    expected={(r['market'],r['start_ns'],r['source_row_id']) for r in assessed}
    actual=set()
    for entry in passes[0]['outputs']:
        content=read_record(contained(root,output+'/'+entry['market']+'/'+entry['month']+'/content.json'))
        for r in content['evidence']:
            key=(r['market'],r['start_ns'],r['source_row_sha256'])
            require(key not in actual,'Duplicate recovered case')
            actual.add(key)
    require(actual==expected and len(actual)==12583,'Recovery differs from the approved assessment population')
    verify_inputs(root,plan,budget)
    cert=signed(dict(schema='daily_identity_recovery_certificate/1',
        status='PASS_DAILY_IDENTITY_RECOVERY_CERTIFICATION',rule=RULE,research_execution=False,
        synthetic_only=False,passes=passes,implementation=plan['implementation'],environment=plan['environment'],
        legacy_implementation=plan['legacy_implementation'],legacy_spec_ids=plan['legacy_spec_ids'],
        plan_id=plan['id'],predecessor_index_sha256=plan['predecessors']['data/active/timeframes.json'],
        receipt=receipt.as_dict(),resources=budget.record()))
    budget.save(contained(root,plan['output']+'/certificate.json'),cert)
    return cert


def code_overrides():
    return {p:HERE+'/candidate/'+Path(p).name for p in CHANGES[:2]}


def prepare_replacement_proposal(root, plan):
    """Construct the exact inactive replacement proposal from passing output."""
    from .daily_identity_publication import certificate, replacement_index
    root=Path(root)
    relative=plan['output']+'/certificate.json'
    reference=dict(path=relative,sha256=sha256_file(contained(root,relative)))
    with projected_successor(root,code_overrides()):
        from .causal_timeframe_publication import _validate_content
        cert=certificate(root,reference)
        require(cert['plan_id']==plan['id'],'Recovery certificate belongs to another plan')
        old=read_record(contained(root,'data/active/timeframes.json'))
        moves=[]
        pairs=[]
        for record in cert['passes'][0]['outputs']:
            prefix=plan['output']+'/reconstruction/'+record['market']+'/'+record['month']
            body=read_record(contained(root,prefix+'/content.json'))
            release=signed({k:v for k,v in body.items() if k!='id'}|{'certificate':reference})
            before=release['predecessor']
            directory=f"data/causally_gated_normalized/{before['market']}/{before['month'][:4]}/1d/{before['month']}/{plan['id']}"
            from hashlib import sha256
            after=before|dict(spec_id=plan['id'],directory=directory,manifest=directory+'/manifest.json',
                             manifest_sha256=sha256(canonical_bytes(release)+b'\n').hexdigest())
            require(not contained(root,directory).exists(),'Unexpected successor destination')
            _validate_content(root,after,release,contained(root,prefix+'/payload'),published=False)
            pairs.append((before,after))
            moves.append(dict(source=prefix+'/payload',entry=after,release=release))
        index=replacement_index(root,old,pairs,reference)
    return signed(dict(schema='daily_identity_replacement_publication/1',recovery_plan_id=plan['id'],
        certificate=reference,moves=moves,index=index,predecessor_sha256=cert['predecessor_index_sha256']))


def stage_replacement(root, plan, proposal, receipt, budget):
    """Approved, inactive materialization plus exactly one final population census."""
    from .boundary import OperationReceipt, OperationClassification, RepoBoundary
    from .causal_timeframe_publication import OPERATION, APPROVAL_KEYS
    from dataclasses import asdict
    from hashlib import sha256
    root=Path(root)
    require(type(receipt) is OperationReceipt,'Publication receipt required')
    require(prepare_replacement_proposal(root,plan)==proposal,'Replacement proposal changed')
    required={'publication_id':proposal['id'],'limits_id':sha256_json(asdict(budget.limits))}
    received=dict(receipt.scope)
    require(set(received)==set(required)|APPROVAL_KEYS,'Publication authorization scope differs')
    required.update({k:received[k] for k in APPROVAL_KEYS})
    kwargs=dict(operation=OPERATION,classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,required_scope=required)
    receipt.verify(RepoBoundary(root),**kwargs)
    relative=plan['output']+'/publication/plan.json'
    directory=contained(root,plan['output']+'/publication')
    require(not directory.exists(),'Publication already attempted; no automatic retry')
    receipt.consume(RepoBoundary(root),**kwargs)
    directory.mkdir(parents=True,exist_ok=False)
    budget.save(contained(root,relative),proposal)
    staged=signed({k:v for k,v in proposal['index'].items() if k!='id'}|{'publication':dict(
        plan_path=relative,plan_sha256=sha256_file(contained(root,relative)),receipt=receipt.as_dict(),
        limits_id=sha256_json(asdict(budget.limits)))})
    with projected_successor(root,code_overrides()):
        from .causal_timeframe_publication import _stage_moves, _validate_population
        _stage_moves(root,proposal,budget)
        counts=_validate_population(root,staged,budget)
        require({k:v for k,v in counts.items() if k.startswith('1d:')}==
                {'1d:'+name:plan['baseline']['observations']+plan['expected_recovered']
                 for name in ('observations','missingness','roll','quality','cadence')},
                'Final daily publication population differs')
    require(sha256_file(contained(root,'data/active/timeframes.json'))==proposal['predecessor_sha256'],
            'Active index changed during inactive publication')
    budget.save(directory/'index.json',staged)
    census=signed(dict(status='PASS_COMPLETE_PUBLICATION_CENSUS',index_sha256=sha256_file(directory/'index.json'),
        table_counts=counts,exhaustive_publication_passes=1,resource_measurement=budget.record()))
    budget.save(directory/'census.json',census)
    return census


def hourly_successors(old, new, manifest, census, quality, *, prefix, publisher_hash):
    """Version of the existing hourly rebind helper for a daily replacement."""
    from hashlib import sha256
    def digest(value): return sha256(canonical_bytes(value)+b'\n').hexdigest()
    require([e for e in old['entries'] if e['timeframe']!='1d']==
            [e for e in new['entries'] if e['timeframe']!='1d'] and old['legacy_1m']==new['legacy_1m'],
            'Recovery changes hourly/minute/second coverage')
    require('daily_identity_recovery' in new,'Daily replacement certificate absent')
    next_census=signed({k:v for k,v in census.items() if k!='id'}|dict(
        status='REBOUND_UNCHANGED_HOURLY_POPULATION',successor_of_record_id=census['id'],
        input_sha256=census['input_sha256']|{'data/active/timeframes.json':digest(new)}))
    next_quality=signed({k:v for k,v in quality.items() if k!='id'}|dict(
        census_sha256=digest(next_census),successor_of_record_id=quality['id']))
    evidence=manifest['evidence']|dict(census=dict(path=prefix+'/census.json',sha256=digest(next_census)),
        quality_manifest=dict(path=prefix+'/quality_manifest.json',sha256=digest(next_quality)))
    next_manifest=signed({k:v for k,v in manifest.items() if k!='id'}|dict(evidence=evidence,
        implementation=manifest['implementation']|{'src/futures_rebuild/causal_timeframe_publication.py':publisher_hash},
        successor_of_record_id=manifest['id']))
    pointer=signed(dict(schema='active_hourly_quality_reader/1',
        manifest=dict(path=prefix+'/reader_manifest.json',sha256=digest(next_manifest))))
    return {'census.json':next_census,'quality_manifest.json':next_quality,
        'reader_manifest.json':next_manifest,'quality_pointer.json':pointer}


def prepare_policies(root, plan, budget):
    """Exhaustive admission entries; reader requests are performed separately."""
    from collections import Counter
    from . import daily_foundation_admission as daily, hourly_source_admission as hourly
    from .causal_observation_parquet import read_bundle
    root=Path(root)
    prefix=plan['output']+'/policies'
    contained(root,prefix).mkdir(parents=True,exist_ok=False)
    old=read_record(contained(root,'data/active/timeframes.json'))
    index=read_record(contained(root,plan['output']+'/publication/index.json'))
    census=read_record(contained(root,plan['output']+'/publication/census.json'))
    require(census['status']=='PASS_COMPLETE_PUBLICATION_CENSUS'
        and census['index_sha256']==sha256_file(contained(root,plan['output']+'/publication/index.json')),
        'Passing complete publication census is missing or stale')
    pointer=read_record(contained(root,hourly.QUALITY_POINTER))
    manifest=read_record(contained(root,pointer['manifest']['path']))
    bound_census=read_record(contained(root,manifest['evidence']['census']['path']))
    bound_quality=read_record(contained(root,manifest['evidence']['quality_manifest']['path']))
    for name,value in hourly_successors(old,index,manifest,bound_census,bound_quality,prefix=prefix+'/hourly',
            publisher_hash=plan['implementation'][CHANGES[0]]).items():
        target=contained(root,prefix+'/hourly/'+name); target.parent.mkdir(parents=True,exist_ok=True)
        budget.save(target,value)
    daily_pointer=read_record(contained(root,'configs/active_daily_source_admission.json'))
    admitted=read_record(contained(root,daily_pointer['manifest']['path']))
    old_quality_ref=admitted['quality']; del admitted
    require(sha256_file(contained(root,old_quality_ref['path']))==old_quality_ref['sha256'],'Old quality changed')
    quality=read_record(contained(root,old_quality_ref['path']))
    require(len(quality['rows'])==155026 and quality['recovered_rows']==0,'Unexpected daily quality predecessor')
    original_ids={r['row_id'] for r in quality['rows']}
    monthly={(m['market'],m['month']):m for m in quality['monthly']}
    recovered=0
    for entry in index['entries']:
        if entry['timeframe']!='1d' or entry['spec_id']!=plan['id']: continue
        release=read_record(contained(root,entry['manifest']))
        bundle=read_bundle(contained(root,entry['directory']))
        rows={r['row_id']:r for r in bundle['observations']}
        for proof in release['evidence']:
            row=rows[proof['row_id']]
            require(row['row_id'] not in original_ids,'Recovered policy duplicates old observation')
            original_ids.add(row['row_id'])
            count=len(proof['reference_row_ids'])
            require(count in (1,2,3),'Recovery reference population differs from completed assessment')
            quality['rows'].append(dict(market=row['market'],start_ns=row['bar_start_ns'],row_id=row['row_id'],
                reason='OBSERVED_HOURLY_AGREEMENT',original_comparison='NOT_COMPARABLE',
                observed_aggregate_agrees=True,coverage='OBSERVED_NATIVE_HOURS_ONLY',hourly_count=count,
                causes=['INCOMPLETE_24_HOUR_REFERENCE_COVERAGE'],evidence_requirement=None,
                contract_identity={k:row[k] for k in ('market','publisher_id','instrument_id','actual_contract',
                    'currency','min_price_increment_nano','multiplier_nano')},
                reference_row_ids=proof['reference_row_ids'],identity_rule=RULE,
                recovery_evidence_id=proof['id'],definition_received_at_ns=row['definition_received_at_ns']))
            recovered+=1
        m=monthly[(entry['market'],entry['month'])]
        m['predecessor_seal_id']=m.pop('seal_id'); m['predecessor_seal_path']=m.pop('seal_path')
        m['recovery_manifest']=dict(path=entry['manifest'],sha256=entry['manifest_sha256'])
        m['recovered_rows']=len(release['evidence'])
    require(recovered==12583 and len(quality['rows'])==len(original_ids)==167609,'Daily quality population differs')
    quality.pop('id')
    quality.update(plan_id=plan['id'],recovered_rows=recovered,
        reason_counts=dict(Counter(r['reason'] for r in quality['rows'])),
        comparison_counts=dict(Counter(r['original_comparison'] for r in quality['rows'])),
        current_source_accounting=quality['baseline']|dict(observations=167609,quarantined=0),
        predecessor_quality=old_quality_ref)
    require(quality['reason_counts']==dict(FULL_HOURLY_AGREEMENT=12438,OBSERVED_HOURLY_AGREEMENT=155168,
                                         EXCLUDED_HOURLY_REFERENCE=3),'Daily quality reasons differ')
    quality_path=prefix+'/quality.json'
    budget.save(contained(root,quality_path),signed(quality))
    overrides=code_overrides()|{'data/active/timeframes.json':plan['output']+'/publication/index.json',
                              hourly.QUALITY_POINTER:prefix+'/hourly/quality_pointer.json'}
    with projected_successor(root,overrides):
        next_hourly=hourly.prepare_manifest(root)
        old_pointer=read_record(contained(root,hourly.POINTER))
        old_admission=read_record(contained(root,old_pointer['manifest']['path']))
        require(all(next_hourly[k]==old_admission[k] for k in ('entries','derived_permitted_uses','expected_counts')),
                'Hourly permission drift')
        budget.save(contained(root,prefix+'/hourly/admission.json'),next_hourly)
        del old_admission,next_hourly
        next_daily=daily.prepare_manifest(root,quality_path,index)
        budget.save(contained(root,prefix+'/admission.json'),next_daily)
        retained=read_record(contained(root,
            'state/unpublished_evidence/daily_admission_v1/incremental_recovery_v1/execution/reader_examples_census_v2.json'))
        examples=deepcopy(retained['daily'])
        # Ensure the new gate itself, rather than only old readable days, is exercised.
        recovered_ids={r['row_id'] for r in quality['rows'] if 'identity_rule' in r}
        by_day={(e['market'],e['start_ns']):e for e in next_daily['entries']}
        expectation_changes=[]
        for name,(request,accepted) in list(examples.items()):
            if not name.startswith('quarantine_'): continue
            entry=by_day[(request['market'],request['start_ns'])]
            require(not accepted and entry['row_id'] in recovered_ids
                and request['end_ns']==entry['end_ns'] and request['as_of_ns']>=entry['end_ns']+5*10**9,
                'Retained quarantine example differs from certified recovery')
            permitted=request.get('purpose','LINEAGE_AUDIT') in entry['permitted_uses']
            examples[name]=(request,permitted)
            expectation_changes.append(dict(case=name,previously_accepted=False,now_accepted=permitted,
                                            certified_row_id=entry['row_id']))
        for e in next_daily['entries']:
            if e['row_id'] not in recovered_ids: continue
            if 'LINEAGE_AUDIT' not in e['permitted_uses']: continue
            args=dict(market=e['market'],start_ns=e['start_ns'],end_ns=e['end_ns'],
                as_of_ns=e['end_ns']+5*10**9,purpose='LINEAGE_AUDIT')
            examples.setdefault('recovered_'+e['market'],(args,True))
            examples.setdefault('recovered_premature',(args|{'as_of_ns':args['as_of_ns']-1},False))
        budget.save(contained(root,prefix+'/reader_examples.json'),signed(dict(daily=examples,hourly=retained['hourly'],
            retained_daily_cases=len(retained['daily']),expectation_changes=expectation_changes)))
    return dict(quality=quality_path,admission=prefix+'/admission.json',hourly_admission=prefix+'/hourly/admission.json',
                overrides=overrides,reason_counts=quality['reason_counts'],recovered_rows=recovered)


def reader_checks(root, plan, budget, *, default=False):
    """Representative public requests, distinct from the complete row census."""
    import gc, json
    from contextlib import nullcontext
    from . import daily_foundation_admission as daily, hourly_foundation_audit as hourly_audit
    from .daily_source_admission import DailySourceReader
    from .hourly_source_admission import HourlySourceReader
    root=Path(root)
    prefix=plan['output']+'/policies'
    examples=read_record(contained(root,prefix+'/reader_examples.json'))
    overrides=code_overrides()|{'data/active/timeframes.json':plan['output']+'/publication/index.json',
        'configs/active_hourly_quality_reader.json':prefix+'/hourly/quality_pointer.json'}
    with nullcontext() if default else projected_successor(root,overrides):
        reader=DailySourceReader(root,**({} if default else {'preview_manifest':prefix+'/admission.json'}))
        days=daily.check_reader_examples(reader,examples['daily'],budget)
        reader._unchanged(); del reader; gc.collect()
        reader=HourlySourceReader(root,**({} if default else {'preview_manifest':prefix+'/hourly/admission.json'}))
        hours=hourly_audit.check_reader_examples(reader,examples['hourly'],budget)
        reader._unchanged(); del reader; gc.collect()
        # Use the projected index during preparation; read the actual active index
        # after rollout. Both paths retain normal publication-proof verification.
        index_path='data/active/timeframes.json' if default else plan['output']+'/publication/index.json'
        index=read_record(contained(root,index_path))
        daily_raw=daily.check_raw_routes(root,index)
        hourly_raw=hourly_audit.check_raw_routes(root,index)
        catalog=json.loads(contained(root,'data/active/catalog.json').read_bytes())
        minutes=[]
        from .hourly_source_admission import timestamp
        for market in sorted({e['market'] for e in catalog['entries']}):
            candidates=[e for e in catalog['entries'] if e['market']==market and
                e['disposition']==daily.minute.CERTIFICATION_STATE and 'LINEAGE_AUDIT' in e['permitted_uses']
                and timestamp(e['coverage_end'])<=daily.CUTOFF]
            require(candidates,'Minute metadata example missing')
            e=candidates[0]
            paths=daily.minute.resolve_partitions(repository_root=root,market=market,year=e['year'],
                                                timeframe='1m',purpose='LINEAGE_AUDIT')
            minutes.append(dict(market=market,year=e['year'],partitions=len(paths)))
            budget.check()
    require(len(minutes)==41 and len(hours)==79 and examples['retained_daily_cases']==126,
            'Representative reader scope changed')
    return dict(daily_reader_checks=days,hourly_reader_checks=hours,minute_metadata_checks=minutes,
        daily_raw_routes=daily_raw,hourly_raw_routes=hourly_raw,
        expectation_changes=examples['expectation_changes'],exhaustive_population_check=False)


def coverage_after(root, plan, budget):
    """Repeat the existing UTC-grid accounting; reuse unchanged hourly profiles."""
    from .coverage_remediation import window_profile
    from .daily_identity_review import CUTOFF
    from .active_data_view import COHORT_PERMISSIONS
    from .causal_observation_parquet import read_table, read_bundle
    from .daily_identity_recovery import verify_recovered_timing
    root=Path(root)
    baseline_path='state/unpublished_evidence/coverage_remediation_v1/f9c1d233fd48e4b54147a21672a49a870e1eee054167d6bf89b0918b6809089d/execution/coverage.json'
    require(sha256_file(contained(root,baseline_path))==plan['bindings'][baseline_path],'Coverage baseline changed')
    baseline=read_record(contained(root,baseline_path))
    index=read_record(contained(root,plan['output']+'/publication/index.json'))
    admission=read_record(contained(root,plan['output']+'/policies/admission.json'))
    profiles=[]
    overrides=code_overrides()|{'data/active/timeframes.json':plan['output']+'/publication/index.json'}
    with projected_successor(root,overrides):
        from .causal_timeframe_publication import validate_release
        for market in sorted({e['market'] for e in index['entries'] if e['timeframe']=='1d'}):
            parts=[e for e in index['entries'] if (e['timeframe'],e['market'])==('1d',market)]
            rows=[]
            for entry in parts:
                if entry['disposition']=='UNAVAILABLE': continue
                release=validate_release(root,entry)
                loaded=read_table(contained(root,entry['directory']+'/observations.parquet'),name='observations')
                if release.get('schema')=='daily_identity_recovery_month/1':
                    quality={r['row_id']:r for r in read_bundle(contained(root,entry['directory']))['quality']}
                    proofs={r['row_id']:r for r in release['evidence']}
                    for row in loaded:
                        if row['row_id'] in proofs: verify_recovered_timing(row,quality[row['row_id']],proofs[row['row_id']])
                        else: require(row['definition_received_at_ns']<=row['bar_start_ns'],'Uncertified late daily definition')
                else:
                    require(all(r['definition_received_at_ns']<=r['bar_start_ns'] for r in loaded),'Legacy daily timing changed')
                rows.extend(loaded); budget.check()
            permissions=[e for e in admission['entries'] if e['market']==market]
            left=((min(e['coverage_start_ns'] for e in parts)+DAY-1)//DAY)*DAY
            right=min(CUTOFF,max(e['coverage_end_ns'] for e in parts))//DAY*DAY
            profiles.extend(window_profile(market=market,step=DAY,left=left,right=right,rows=rows,
                permissions=permissions,quarantines=[],lengths=[1,5,20,60],
                purposes=sorted({p for uses in COHORT_PERMISSIONS.values() for p in uses})))
    def key(row): return tuple(row[k] for k in ('market','timeframe','window_bars','year','purpose'))
    prior={key(r):r for r in baseline['profiles'] if r['timeframe']=='1d'}
    require({key(r) for r in profiles}==set(prior),'Coverage profile grid or purpose scope changed')
    differences=[]
    for row in profiles:
        old=prior[key(row)]
        require(row['candidate_windows']==old['candidate_windows'] and row['readable_windows']>=old['readable_windows'],
                'Recovery reduced a previously readable window or changed its grid')
        differences.append({k:row[k] for k in ('market','timeframe','window_bars','year','purpose')}|dict(
            before=old['readable_windows'],after=row['readable_windows'],newly_readable=row['readable_windows']-old['readable_windows']))
    report=signed(dict(status='PASS_STRUCTURAL_COVERAGE_ACCOUNTING',baseline=dict(path=baseline_path,sha256=plan['bindings'][baseline_path]),
        daily_profiles=profiles,daily_changes=differences,
        discovery_2018_2022=[r for r in differences if 2018<=r['year']<=2022],
        hourly_profiles=[r for r in baseline['profiles'] if r['timeframe']=='1h'],hourly_evidence='UNCHANGED_BOUND_BASELINE_REUSED',
        grid='CONSECUTIVE_UTC_INTERVALS_NO_CALENDAR_COMPRESSION',as_of='WINDOW_END_PLUS_FIVE_SECONDS',
        reason_counts_overlap=True,refused_totals_are_unique=True,model_specific_eligibility='NOT_DECLARED'))
    budget.save(contained(root,plan['output']+'/coverage.json'),report)
    return dict(coverage_report=plan['output']+'/coverage.json',profile_count=len(profiles),
                newly_readable_windows=sum(r['newly_readable'] for r in differences))
