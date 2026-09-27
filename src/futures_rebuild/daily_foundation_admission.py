"""Daily admission preparation and quality accounting; no execution CLI.

Historical validation and publication require the separate approved task scope.
The certified daily producer, replay implementation and reader remain unchanged.
"""
from collections import Counter
from dataclasses import asdict
from pathlib import Path
import json

from . import active_data_view as minute
from .canonical import canonical_bytes, sha256_file, sha256_json
from .causal_timeframe_storage import contained, read_record, signed, Limits
from .causal_1d import DAY, CUTOFF, implementation_binding
from .hourly_quality_reader import HOUR, NATIVE_ALLOWED
from .errors import ContractError, IntegrityError, UnauthorizedOperation

HERE = 'state/unpublished_evidence/daily_admission_v1'
CAMPAIGN = 'state/unpublished_evidence/daily_foundation_v1/full_campaign_execution_v1'
PREPARATION = 'state/unpublished_evidence/daily_foundation_v1/full_campaign_preparation_v1/plan.json'
GUARD = 'state/unpublished_evidence/daily_foundation_v1/remediation_preparation_v1/publication.candidate.py'
EXCEPTIONS = 'state/unpublished_evidence/hourly_exception_recovery_v1'
CONTROLS = ('data/causally_gated_normalized/_control/timeframes.json', 'data/causally_gated_normalized/_control/catalog.json',
    'configs/active_hourly_quality_reader.json', 'configs/active_hourly_source_admission.json',
    'configs/approved_causal_1d_range.json', 'src/futures_rebuild/causal_timeframe_publication.py',
    'src/futures_rebuild/research_gateway_policy.py', 'CURRENT_WORKFLOW.md')
BASELINE = dict(source_primary_rows=167639, observations=155026, quarantined=12583,
                excluded_availability=30, excluded_start=0)
# Existing hourly audit memory/time ceilings, with bounded publication staging.
# Byte-exact copies are required because canonical custody forbids hard links.
LIMITS = Limits(8*2**30, 6*2**30, 8*3600, maximum_files=100000)


def require(condition, message):
    if not condition:
        raise IntegrityError(message)


def validate_permission_sidecars(root, catalog, bindings):
    minute.validate_catalog(catalog)
    for entry in catalog['entries']:
        if entry['disposition'] != minute.CERTIFICATION_STATE:
            continue
        path = entry['sidecar_path']
        require(bindings.get(path) == entry['sidecar_sha256'], 'Minute permission sidecar is unbound')
        require(sha256_file(contained(root, path)) == entry['sidecar_sha256'], 'Minute sidecar changed')
        if 'annual_release_key' in entry:
            from .annual_foundation_release import minute_permission
            sidecar = minute_permission(root, entry)
        else:
            sidecar = json.loads(contained(root, path).read_bytes())
        require(sidecar.get('schema_version') == minute.SIDECAR_SCHEMA
            and sidecar.get('sidecar_id') == sha256_json({k:v for k,v in sidecar.items() if k != 'sidecar_id'})
            and sidecar.get('entry_binding') == {k:v for k,v in entry.items() if k != 'sidecar_sha256'}
            and minute.validate_access_policy_binding(sidecar['access_policy_binding']) == entry['access_policy_binding_id']
            and minute.validate_content_validation_receipt(sidecar['content_validation_receipt']) == entry['content_validation_receipt_id'],
            'Minute permission sidecar differs')


def classify(row, hours, *, original_comparison, source_hashes):
    """Compare only certified native references from the exact retained delivery.

    Multiple causes are retained. Observed-hour agreement describes fidelity to
    that observed series; it does not assert 24-hour or exchange-session coverage.
    """
    start = row['bar_start_ns']
    require(original_comparison in {'MATCH','NOT_COMPARABLE','SOURCE_MISSING','DISAGREEMENT'}, 'Unknown cadence class')
    hours = sorted(hours, key=lambda h:h['bar_start_ns'])
    require(all(start <= h['bar_start_ns'] < start+DAY for h in hours), 'Reference outside daily interval')
    identity = ('market','publisher_id','instrument_id','actual_contract',
                'currency','min_price_increment_nano','multiplier_nano')
    grid = [h['bar_start_ns'] for h in hours]
    causes = []
    if len(grid) != len(set(grid)) or any(t % HOUR for t in grid):
        causes.append('UNRESOLVED_REFERENCE')
    if any(h['source_file_sha256'] not in source_hashes for h in hours):
        causes.append('UNRESOLVED_REFERENCE')
    if any(any(h[k] != row[k] for k in identity) for h in hours):
        causes.append('CONTRACT_OR_PUBLISHER_DIFFERENCE')
    if any(h['quality_disposition'] not in NATIVE_ALLOWED for h in hours):
        causes.append('EXCLUDED_HOURLY_REFERENCE')
    complete = grid == list(range(start,start+DAY,HOUR))
    if not complete:
        causes.append('INSUFFICIENT_HOURLY_COVERAGE' if hours else 'REFERENCE_UNAVAILABLE')
    prices = ('open_nano','high_nano','low_nano','close_nano','volume')
    aggregate = None if not hours else dict(open_nano=hours[0]['open_nano'],
        high_nano=max(h['high_nano'] for h in hours), low_nano=min(h['low_nano'] for h in hours),
        close_nano=hours[-1]['close_nano'], volume=sum(h['volume'] for h in hours))
    equal = aggregate is not None and all(row[k] == aggregate[k] for k in prices)
    blocking = {'UNRESOLVED_REFERENCE','CONTRACT_OR_PUBLISHER_DIFFERENCE','EXCLUDED_HOURLY_REFERENCE'}
    if ((original_comparison == 'SOURCE_MISSING' and hours)
            or (original_comparison == 'NOT_COMPARABLE' and complete and not blocking & set(causes))):
        causes.append('UNRESOLVED_REFERENCE')
    if original_comparison == 'MATCH':
        require(complete and equal and 'CONTRACT_OR_PUBLISHER_DIFFERENCE' not in causes,
                'Retained matching comparison cannot be reproduced')
    if not hours:
        reason = 'REFERENCE_UNAVAILABLE'
    elif blocking & set(causes):
        reason = next(c for c in causes if c in blocking)
    elif equal:
        reason = 'FULL_HOURLY_AGREEMENT' if complete else 'OBSERVED_HOURLY_AGREEMENT'
    else:
        reason = 'REFERENCE_DISAGREEMENT' if complete else 'INSUFFICIENT_HOURLY_COVERAGE'
    requirement = None if reason in {'FULL_HOURLY_AGREEMENT','OBSERVED_HOURLY_AGREEMENT'} else (
        'Exact-contract native hourly/trade evidence for this UTC day, including publisher, '
        'timestamps, OHLCV, selection provenance and correction rules; or an explicit '
        'evidence-supported disposition. Do not fill absent hours.')
    return dict(market=row['market'], start_ns=start, row_id=row['row_id'],
        contract_identity={k:row[k] for k in identity},
        original_comparison=original_comparison, reason=reason, causes=sorted(set(causes)),
        hourly_count=len(hours), observed_aggregate_agrees=equal,
        coverage='FULL_24_UTC_HOURS' if complete else 'OBSERVED_HOURS_ONLY',
        reference_row_ids=[h['row_id'] for h in hours], evidence_requirement=requirement)


def prepare(root):
    """Bind metadata and source files without decoding historical bar values."""
    root = Path(root).resolve()
    campaign = read_record(contained(root, PREPARATION))
    accounting = read_record(contained(root, CAMPAIGN+'/accounting.json'))
    require(accounting['status'] == 'PASS_FULL_RECONSTRUCTION_NO_ADMISSION'
        and accounting['source_accounting'] == BASELINE and accounting['monthly_entries'] == 6862
        and accounting['certified_markets'] == 41, 'Complete daily campaign required')
    require(implementation_binding() == campaign['implementation'], 'Daily certification implementation changed')
    bindings = {p:sha256_file(contained(root,p)) for p in CONTROLS}
    for p,digest in campaign['preserved_bindings'].items():
        require(sha256_file(contained(root,p)) == digest, 'Campaign control changed: '+p)
    for p in (PREPARATION,CAMPAIGN+'/accounting.json',CAMPAIGN+'/result.json', GUARD,
              'state/unpublished_evidence/daily_foundation_v1/prepare_coordinated_publication.py',
              'src/futures_rebuild/daily_source_admission.py',
              'src/futures_rebuild/daily_foundation_admission.py',
              'src/futures_rebuild/hourly_source_admission.py',
              EXCEPTIONS+'/final_dispositions.json',EXCEPTIONS+'/cases.json',
              EXCEPTIONS+'/validation/result.json',EXCEPTIONS+'/public_evidence.json'):
        bindings[p] = sha256_file(contained(root,p))
    hourly_pointer = read_record(contained(root,'configs/active_hourly_quality_reader.json'))
    hourly_manifest_path = hourly_pointer['manifest']['path']
    bindings[hourly_manifest_path] = hourly_pointer['manifest']['sha256']
    hourly_manifest = read_record(contained(root,hourly_manifest_path))
    for evidence in hourly_manifest['evidence'].values():
        bindings[evidence['path']] = evidence['sha256']
    for path,digest in hourly_manifest['implementation'].items():
        bindings[path] = digest
    for path,digest in bindings.items():
        require(sha256_file(contained(root,path)) == digest, 'Preparation binding differs: '+path)
    catalog = json.loads(contained(root,'data/causally_gated_normalized/_control/catalog.json').read_bytes())
    for entry in catalog['entries']:
        if entry['disposition'] == minute.CERTIFICATION_STATE:
            bindings[entry['sidecar_path']] = entry['sidecar_sha256']
    validate_permission_sidecars(root,catalog,bindings)
    source_bindings = {}
    candidates, destinations = [], []
    by_market = {}
    for month in accounting['monthly']:
        by_market.setdefault(month['market'],[]).append(month)
    for item in campaign['operations']:
        require(sha256_file(contained(root,item['operation_path'])) == item['operation_sha256'], 'Operation changed')
        op = read_record(contained(root,item['operation_path']))
        for entry in op['entries']:
            old = source_bindings.setdefault(entry['path'],entry['sha256'])
            require(old == entry['sha256'], 'Ambiguous source binding')
        candidates.append(item['output']+'/candidate.json')
        for month in by_market[item['market']]:
            if month['state'] == 'SOURCE_UNAVAILABLE':
                continue
            value = month['month']
            destination = f"data/causally_gated_normalized/{item['market']}/{value[:4]}/1d/{value}/{op['spec_id']}"
            require(not contained(root,destination).exists(), 'Daily publication destination already exists: '+destination)
            destinations.append(destination)
    # Hashing for immutable-input binding is metadata preparation, not source replay.
    for path,digest in source_bindings.items():
        require(sha256_file(contained(root,path)) == digest, 'Retained source changed: '+path)
    index = read_record(contained(root,'data/causally_gated_normalized/_control/timeframes.json'))
    require(not any(e['timeframe'] == '1d' for e in index['entries']), 'Daily release already active')
    daily_pointer = 'configs/active_daily_source_admission.json'
    require(not contained(root,daily_pointer).exists(), 'Daily admission already exists')
    guard_before = contained(root,GUARD).with_name('publication.predecessor.sha256').read_text().strip()
    require(guard_before == bindings['src/futures_rebuild/causal_timeframe_publication.py'], 'Prepared guard is stale')
    return signed(dict(schema='daily_foundation_admission_preparation/1',
        status='PREPARED_REQUIRES_OWNER_APPROVAL', repository_root=str(root),
        bindings=bindings, source_bindings=source_bindings, candidates=candidates,
        publication_directories=destinations,
        daily_implementation=implementation_binding(), baseline=BASELINE,
        monthly_entries=6862, comparison_counts=accounting['comparison_counts'],
        limits=asdict(LIMITS), model_specific_eligibility='NOT_DECLARED',
        estimated_runtime_minutes=[45,150], maximum_allocated_output_bytes=10*2**30,
        historical_read_scope='Certified daily five-table bundles, native hourly observations/quality, '
            '2179 derived ZQ rows for unchanged-reader regression; hash-only retained source files; '
            'no new raw DBN decoding, minute-bar decoding, protected data or research execution',
        activation_targets=['src/futures_rebuild/causal_timeframe_publication.py',
            'data/causally_gated_normalized/_control/timeframes.json','configs/active_hourly_quality_reader.json',
            'configs/active_hourly_source_admission.json',daily_pointer],
        expected_daily_pointer=None, publication=False))


def scan_quality(root, plan, authorization, budget):
    """Approved full-population join against certified native hourly references.

    Reuse raw-source replay only after the exact source hashes remain current.
    This scan does not decode another delivery or rerun normalization.
    """
    from .hourly_source_admission import verify_bindings
    from .hourly_quality_reader import HourlyReader
    from .causal_timeframe_storage import iter_bundle_groups, file_inventory
    from .causal_timeframe_publication import verify_provenance
    require(authorization.get('plan_id') == plan['id']
        and authorization.get('status') == 'OWNER_APPROVED_IN_THIS_TASK', 'Exact daily audit approval required')
    require(plan['repository_root'] == str(Path(root).resolve()), 'Audit repository differs')
    verify_bindings(root,plan['bindings'])
    verify_bindings(root,plan['source_bindings'])
    require(implementation_binding() == plan['daily_implementation'], 'Daily implementation changed')
    retained = read_record(contained(root,CAMPAIGN+'/accounting.json'))
    seal_hashes = {m['seal_path']:m['seal_sha256'] for m in retained['monthly']}
    executions = json.loads(contained(root,CAMPAIGN+'/result.json').read_bytes())
    certificates = {e['output']+'/candidate.json':e['certificate_id'] for e in executions['results']}
    hourly = HourlyReader(root)
    final = json.loads(contained(root,EXCEPTIONS+'/final_dispositions.json').read_bytes())
    require(final['prepared_cases_sha256'] == plan['bindings'][EXCEPTIONS+'/cases.json']
        and final['validation_result_sha256'] == plan['bindings'][EXCEPTIONS+'/validation/result.json'],
        'Retained empty-month decisions changed')
    empty_cases = {(e['market'],e['month']):e for e in final['empty_month_cases']}
    rows, monthly = [], []
    totals, comparisons = Counter(), Counter({k:0 for k in plan['comparison_counts']})
    for relative in plan['candidates']:
        budget.check()
        directory = contained(root,relative).parent
        candidate = read_record(directory/'candidate.json')
        cert = read_record(directory/'certificate.json')
        require(cert['id'] == certificates[relative] and candidate['id'] == cert['candidate_id'],
                'Retained campaign certificate or candidate changed')
        verify_provenance(root,candidate,cert,read_record(directory/'provenance.json'))
        require(cert['passes'][0] == cert['passes'][1], 'Daily replay results differ')
        reference_hashes = {e['sha256'] for e in candidate['source_inventory']
                            if e['family'] == 'ohlcv_1h' and e['kind'] == 'DBN'}
        market = candidate['spec']['market']
        for month, seal_id in zip(candidate['months'],candidate['seals'],strict=True):
            seal_relative = relative.removesuffix('candidate.json')+month+'.seal.json'
            require(sha256_file(contained(root,seal_relative)) == seal_hashes[seal_relative],
                    'Retained full-audit seal binding changed')
            seal = read_record(directory/(month+'.seal.json'))
            require(seal['id'] == seal_id, 'Daily seal changed')
            for day in seal['coverage']:
                require(day['quarantined'] == len(day['quarantine']), 'Quarantine count differs')
                require(day['source_primary_rows'] == sum(day[k] for k in
                    ('observations','quarantined','excluded_start','excluded_availability')), 'Daily source loss')
                totals.update({k:day[k] for k in BASELINE})
            unavailable = not seal['files']
            monthly.append(dict(market=market,month=month,seal_id=seal_id,
                seal_path=relative.removesuffix('candidate.json')+month+'.seal.json',
                disposition='UNAVAILABLE' if unavailable else 'CERTIFIED_WITH_DAY_GATES',
                evidence_requirement=('Product/venue listing evidence for expected absence, or exact-contract '
                    'source rows and selection provenance for recovery') if unavailable else None))
            if unavailable:
                case = empty_cases.get((market,month))
                require(case is not None and case['readable'] is False and case['recovered_rows'] == 0,
                    'Missing retained absence disposition')
                monthly[-1].update(absence_disposition=case['disposition'],
                    evidence=case.get('evidence'),evidence_requirement=case.get('requirement'),
                    retained_dispositions=EXCEPTIONS+'/final_dispositions.json')
                continue
            require(file_inventory(directory/seal['directory'],budget) == seal['files'], 'Daily table bytes changed')
            entry = hourly.entries.get((market,month))
            hours = hourly._native_month(market,month) if entry and entry['disposition'] == 'CERTIFIED' else []
            by_day = {}
            for hour in hours:
                by_day.setdefault(hour['bar_start_ns']//DAY*DAY,[]).append(hour)
            for bundle in iter_bundle_groups(directory/seal['directory']):
                budget.check()
                cadence = {c['row_id']:c for c in bundle['cadence']}
                require(len(cadence) == len(bundle['observations']), 'Cadence correspondence differs')
                for row in bundle['observations']:
                    comparison = cadence[row['row_id']]['result']
                    comparisons[comparison] += 1
                    rows.append(classify(row,by_day.get(row['bar_start_ns'],[]),
                        original_comparison=comparison,source_hashes=reference_hashes))
            require(file_inventory(directory/seal['directory'],budget) == seal['files'],
                    'Daily table bytes changed during classification')
    require(dict(totals) == plan['baseline'] and dict(comparisons) == plan['comparison_counts']
        and len(rows) == BASELINE['observations'] and len(monthly) == 6862, 'Complete population accounting differs')
    require(len({(r['market'],r['start_ns']) for r in rows}) == len(rows), 'Duplicate daily quality interval')
    hourly._unchanged()
    verify_bindings(root,plan['bindings'])
    verify_bindings(root,plan['source_bindings'])
    return signed(dict(status='PASS_DAILY_QUALITY_ACCOUNTING',plan_id=plan['id'],
        baseline=dict(totals),comparison_counts=dict(comparisons),monthly=monthly,rows=rows,
        reason_counts=dict(Counter(r['reason'] for r in rows)),
        recovered_rows=0,corrected_rows=0,model_specific_eligibility='NOT_DECLARED'))


def prepare_manifest(root, quality_path, index):
    """Build an inactive manifest for an exact, already staged publication index."""
    from .daily_source_admission import policy_entries, SCHEMA
    from .causal_timeframe_publication import validate_index
    root = Path(root).resolve()
    validate_index(root,index)
    quality = read_record(contained(root,quality_path))
    require(quality['status'] == 'PASS_DAILY_QUALITY_ACCOUNTING'
        and quality['baseline'] == BASELINE, 'Complete daily quality evidence required')
    catalog = json.loads(contained(root,'data/causally_gated_normalized/_control/catalog.json').read_bytes())
    entries = policy_entries(quality['rows'],catalog)
    published = {(e['market'],e['month']):e for e in index['entries'] if e['timeframe'] == '1d'}
    require(len(published) == 6862, 'Published daily coverage differs')
    for month in quality['monthly']:
        entry = published.get((month['market'],month['month']))
        require(entry is not None and (entry['disposition'] == 'UNAVAILABLE') == (month['disposition'] == 'UNAVAILABLE'),
            'Published daily disposition differs')
    bindings = {p:sha256_file(contained(root,p)) for p in (
        'data/causally_gated_normalized/_control/catalog.json','src/futures_rebuild/daily_source_admission.py',
        'src/futures_rebuild/daily_foundation_admission.py',
        'src/futures_rebuild/hourly_source_admission.py',
        'src/futures_rebuild/hourly_quality_reader.py',CAMPAIGN+'/accounting.json',quality_path)}
    from hashlib import sha256
    bindings['data/causally_gated_normalized/_control/timeframes.json'] = sha256(canonical_bytes(index)+b'\n').hexdigest()
    for entry in catalog['entries']:
        if entry['disposition'] == minute.CERTIFICATION_STATE:
            bindings[entry['sidecar_path']] = entry['sidecar_sha256']
    validate_permission_sidecars(root,catalog,bindings)
    return signed(dict(schema=SCHEMA,source='native',bindings=bindings,
        daily_implementation=implementation_binding(),quality=dict(path=quality_path,sha256=bindings[quality_path]),
        entries=entries,model_specific_eligibility='NOT_DECLARED'))


def reader_examples(manifest, quality, root):
    """Representative real windows, explicitly separate from exhaustive accounting."""
    examples = {}
    rows = {(r['market'],r['start_ns']):r for r in quality['rows']}
    previous = {}
    for entry in manifest['entries']:
        start,end = entry['start_ns'],entry['end_ns']
        args = dict(market=entry['market'],start_ns=start,end_ns=end,as_of_ns=end+5*10**9)
        uses = entry['permitted_uses']
        if uses:
            examples.setdefault('market_'+entry['market'],(args|{'purpose':uses[0]},True))
            for purpose in uses:
                examples.setdefault('cohort_'+entry['cohort']+'_'+purpose,(args|{'purpose':purpose},True))
            examples.setdefault('reason_'+entry['reason'],(args|{'purpose':uses[0]},True))
            examples.setdefault('premature',(args|{'purpose':uses[0],'as_of_ns':end+5*10**9-1},False))
            examples.setdefault('training_denied',(args|{'purpose':'TRAINING'},False))
            examples.setdefault('derived_denied',(args|{'source':'minute_derived'},False))
        if 'FEATURE_GENERATION' not in uses:
            examples.setdefault('restricted_'+entry['reason'],(args|{'purpose':'FEATURE_GENERATION'},False))
        old = previous.get(entry['market'])
        if old and 'LINEAGE_AUDIT' in uses and 'LINEAGE_AUDIT' in old['permitted_uses']:
            old_row = rows[(old['market'],old['start_ns'])]
            row = rows[(entry['market'],start)]
            if old['end_ns'] < start:
                examples.setdefault('missing_day',(args|{'start_ns':old['start_ns']},False))
            elif old_row['contract_identity'] != row['contract_identity']:
                examples.setdefault('contract_change',(args|{'start_ns':old['start_ns']},False))
        previous[entry['market']] = entry
    for month in quality['monthly']:
        seal = read_record(contained(root,month['seal_path']))
        if month['disposition'] == 'UNAVAILABLE':
            start = seal['coverage'][0]['day_ns']
            examples['empty_'+month['market']+'_'+month['month']] = (
                dict(market=month['market'],start_ns=start,end_ns=start+DAY,as_of_ns=start+2*DAY),False)
        for day in seal['coverage']:
            if day['quarantined']:
                start = day['day_ns']
                examples.setdefault('quarantine_'+month['market'],(
                    dict(market=month['market'],start_ns=start,end_ns=start+DAY,as_of_ns=start+2*DAY),False))
    return examples


def check_reader_examples(reader, examples, budget=None):
    checks=[]
    for name,(request,accepted) in sorted(examples.items()):
        if budget: budget.check()
        try:
            rows=reader.read_window(**request)
        except (UnauthorizedOperation,ContractError) as exc:
            require(not accepted,'Daily public reader unexpectedly refused: '+name)
            checks.append(dict(case=name,request=request,result='REFUSED',reason=str(exc)))
        else:
            require(accepted and [r['bar_start_ns'] for r in rows] == list(range(request['start_ns'],request['end_ns'],DAY)),
                    'Daily public reader selection differs: '+name)
            checks.append(dict(case=name,request=request,result='ACCEPTED',rows=len(rows)))
    return checks


def check_raw_routes(root,index):
    from . import causal_timeframe_publication as publication, causal_timeframe_view
    args=dict(market='ES',year=2024,timeframe='1d',purpose='LINEAGE_AUDIT')
    for name,call in (
        ('publication',lambda:publication.resolve_partitions(root,index,**args)),
        ('timeframe_view',lambda:causal_timeframe_view.resolve_partitions(repository_root=root,**args)),
        ('active_data_view',lambda:minute.resolve_partitions(repository_root=root,**args))):
        try: call()
        except UnauthorizedOperation as exc:
            require('DailyReader.read_window' in str(exc),'Unexpected daily raw-path refusal: '+name)
        else: raise IntegrityError('Daily raw-path route admitted: '+name)
    return ['publication','timeframe_view','active_data_view']
