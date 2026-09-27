"""Prepare-only CLI and offline diagnostics for the completed 1h/1d foundations.

No publication, recovery authority, network access or research execution. Historical
execution is invoked by an approved task, never by the command-line entry point.
"""
from collections import Counter, defaultdict
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
import gc
import json
import sys

import numpy as np

from .canonical import canonical_bytes, sha256_file
from .causal_timeframe_storage import (Budget, Limits, allocation, contained,
                                      memory_peak, read_record, signed)
from .errors import IntegrityError, UnauthorizedOperation

HERE = 'state/unpublished_evidence/coverage_remediation_v1'
ACCOUNTING = 'state/unpublished_evidence/daily_foundation_v1/full_campaign_execution_v1/accounting.json'
EXCEPTIONS = 'state/unpublished_evidence/hourly_exception_recovery_v1'
SECOND = 10**9
HOUR = 3600 * SECOND
DAY = 24 * HOUR
CUTOFF = 1752444000 * SECOND
LAG = 5 * SECOND
LIMITS = Limits(2*2**30, 5*2**30, 4*3600, maximum_files=100000)
PREPARATION_WRITE_RESERVE = 64*2**20
IDENTITY = ('market', 'publisher_id', 'instrument_id', 'actual_contract',
            'currency', 'min_price_increment_nano', 'multiplier_nano')
REASONS = ('missing_coverage', 'quarantine', 'permission_or_cohort',
           'contract_or_unit', 'timing')
POINTERS = ('configs/active_daily_source_admission.json',
            'configs/active_hourly_source_admission.json',
            'configs/active_hourly_quality_reader.json')


def require(condition, message):
    if not condition:
        raise IntegrityError(message)


def utc(ns):
    return datetime.fromtimestamp(ns//SECOND, timezone.utc)


def classify_quarantine(item):
    """Classify retained evidence without conferring identity or recovery authority."""
    bar = item['source_row']
    start = bar['event_at_ns']
    require(start % DAY == 0, 'Quarantine is not a UTC daily bar')
    keys = ('source_release_id', 'source_manifest_sha256', 'dataset', 'market',
            'publisher_id', 'instrument_id')
    definitions = item['matching_definitions']
    exact = [d for d in definitions if all(d[k] == bar[k] for k in keys)
             and d['instrument_id_date_utc'] == utc(start).date().isoformat()]
    eligible = [d for d in exact if d['ts_recv_ns'] <= start]
    if eligible:
        reason = 'ELIGIBLE_BASELINE_REQUIRES_RESOLVER_REVIEW'
    elif definitions and not exact:
        reason = 'IDENTITY_MISMATCH'
    elif exact:
        reason = 'LATE_SAME_DAY_DEFINITION'
    else:
        reason = 'ABSENT_MATCHING_IDENTITY'
    from .foundation.identity import DefinitionIndex
    from .foundation.records import ProviderBar, ProviderDefinition
    from .errors import ContractError
    try:
        DefinitionIndex(tuple(ProviderDefinition(**d) for d in definitions)).resolve(
            ProviderBar(**bar), decision_at=utc(start))
    except ContractError as exc:
        if str(exc) != 'bar instrument has no same-day definition received by bar start':
            reason = 'AMBIGUOUS_OR_INVALID_DEFINITION'
    return dict(market=bar['market'], start_ns=start, weekday=utc(start).strftime('%A'),
                source_row_id=bar['row_sha256'], source_path=bar['source_file_path'],
                source_sha256=bar['source_file_sha256'], reason=reason,
                matching_definitions=len(exact), eligible_baselines=len(eligible),
                disposition='UNRESOLVED_QUARANTINED', recovered_rows=0,
                requirement='Exact date-specific active contract identity known by UTC bar start; '
                            'later definitions and prior-date numeric ID matches do not qualify.')


def window_profile(*, market, step, left, right, rows, permissions, quarantines,
                   lengths, purposes):
    """Linear prefix accounting on the complete UTC grid; no calendar compression.

    One entry per window start and length. Permission intervals cover complete bars.
    Years refer to window START. Reasons overlap; refused counts are a union.
    """
    require(step in (HOUR, DAY) and left % step == right % step == 0 and left < right <= CUTOFF,
            'Invalid profiling extent')
    n = (right-left)//step
    present = np.zeros(n, dtype=bool)
    quality_bad = np.zeros(n, dtype=bool)
    available = np.zeros(n, dtype=np.int64)
    identities = np.zeros(n, dtype=np.int32)
    identity_ids = {}
    for row in rows:
        start = row['bar_start_ns']
        require(start % step == 0 and left <= start < right and row['bar_end_ns'] == start+step
                and row['market'] == market, 'Row outside profile grid')
        i = (start-left)//step
        require(not present[i], 'Duplicate observation in profile')
        present[i] = True
        key = tuple(row[k] for k in IDENTITY)
        identities[i] = identity_ids.setdefault(key, len(identity_ids)+1)
        require(row['available_at_ns'] >= start+step+LAG
                and row['decision_eligible_at_ns'] >= row['available_at_ns'], 'Invalid causal timing')
        available[i] = row['decision_eligible_at_ns']
        quality_bad[i] = not row.get('quality_accepted', True)
    quarantine = quality_bad.copy()
    for start in quarantines:
        require(left <= start < right and start % step == 0, 'Quarantine outside grid')
        quarantine[(start-left)//step] = True
    missing = ~present & ~quarantine
    times = left + np.arange(n, dtype=np.int64)*step
    years = np.asarray([utc(int(t)).year for t in times], dtype=np.int16)
    masks = {p:np.zeros(n, dtype=bool) for p in purposes}
    occupied = np.zeros(n, dtype=bool)
    for entry in permissions:
        a, b = max(left,entry['start_ns']), min(right,entry['end_ns'])
        a = ((a+step-1)//step)*step
        b = (b//step)*step
        if a>=b:
            continue
        lo, hi = (a-left)//step, (b-left)//step
        require(not occupied[lo:hi].any(), 'Ambiguous permission intervals')
        occupied[lo:hi] = True
        for p in set(entry['permitted_uses']) & set(purposes):
            masks[p][lo:hi] = True

    def sums(values, length):
        prefix = np.concatenate(([0],np.cumsum(values, dtype=np.int64)))
        return prefix[length:] - prefix[:-length]

    result = []
    for length in lengths:
        if length>n:
            continue
        count = n-length+1
        base = {'missing_coverage':sums(missing,length)>0,
                'quarantine':sums(quarantine,length)>0}
        # Vectorized maximum over each requested window. Lengths are fixed and
        # small (at most 120); no whole-population Python window materialization.
        latest = np.zeros(count,dtype=np.int64)
        maximum_id = np.zeros(count,dtype=np.int32)
        minimum_id = np.full(count,np.iinfo(np.int32).max,dtype=np.int32)
        nonmissing_ids=np.where(present,identities,np.iinfo(np.int32).max)
        for offset in range(length):
            np.maximum(latest,available[offset:offset+count],out=latest)
            np.maximum(maximum_id,identities[offset:offset+count],out=maximum_id)
            np.minimum(minimum_id,nonmissing_ids[offset:offset+count],out=minimum_id)
        base['contract_or_unit'] = maximum_id>minimum_id
        base['timing'] = latest > times[:count]+length*step+LAG
        for purpose in purposes:
            reasons = dict(base,permission_or_cohort=sums(~masks[purpose],length)>0)
            refused = np.logical_or.reduce(list(reasons.values()))
            for year in np.unique(years[:count]):
                subset = years[:count]==year
                total = int(subset.sum())
                denied = int((refused & subset).sum())
                result.append(dict(market=market,timeframe='1d' if step==DAY else '1h',
                    window_bars=length,year=int(year),purpose=purpose,candidate_windows=total,
                    readable_windows=total-denied,refused_windows=denied,
                    reason_counts={k:int((v & subset).sum()) for k,v in reasons.items()}))
    return result


def prepare(root):
    """Read control/accounting metadata only; no bars, seals or DBN decoding."""
    root = Path(root).resolve()
    bindings = {}
    def record(path, expected=None):
        digest = sha256_file(contained(root,path))
        require(expected is None or digest==expected, 'Changed metadata: '+path)
        bindings[path] = digest
        value=json.loads(contained(root,path).read_bytes())
        if 'id' in value:
            require(value==signed({k:v for k,v in value.items() if k!='id'}),'Invalid record identity: '+path)
        return value
    accounting = record(ACCOUNTING)
    require(accounting['source_accounting'] == dict(source_primary_rows=167639,
        observations=155026,quarantined=12583,excluded_start=0,excluded_availability=30),
        'Daily baseline differs')
    old = record(EXCEPTIONS+'/cases.json')
    record(EXCEPTIONS+'/final_dispositions.json')
    record(EXCEPTIONS+'/acquisition_spec.json')
    contract = record('configs/source_contract.json')
    inventory = record(contract['complete_inventory']['path'],contract['complete_inventory']['sha256'])
    index = record('data/active/timeframes.json')
    require(all(sum(e['timeframe']==tf for e in index['entries'])==6862 for tf in ('1h','1d')),
            'Monthly baseline differs')
    source_plan = None
    for path in POINTERS:
        pointer = record(path)
        for ref in pointer.values():
            if isinstance(ref,dict) and set(ref)>={'path','sha256'}:
                value = record(ref['path'],ref['sha256'])
                for p,h in value.get('bindings',{}).items():
                    if p in bindings: require(bindings[p]==h,'Conflicting binding: '+p)
                    bindings[p]=h
                for p,h in value.get('implementation',{}).items(): bindings[p]=h
                for p,h in value.get('daily_implementation',{}).items():
                    bindings['src/futures_rebuild/'+p]=h
                if 'retained_source_plan' in value:
                    source_plan=value['retained_source_plan']
                if 'quality' in value and isinstance(value['quality'],dict):
                    q=value['quality']; bindings[q['path']]=q['sha256']
                for evidence in value.get('evidence',{}).values():
                    bindings[evidence['path']]=evidence['sha256']
                del value
        gc.collect()
    require(source_plan is not None,'Missing retained source evidence')
    prior = record(source_plan['path'],source_plan['sha256'])
    historical = dict(prior['source_bindings'])
    for p,h in old['historical_hash_bindings'].items():
        if p=='data/active/timeframes.json':
            # Daily activation intentionally replaced the shared index. Its
            # passing hourly census successor binds the current index; keep
            # validating the original immutable source/case evidence below.
            continue
        require(p not in historical or historical[p]==h,'Retained exception source binding differs: '+p)
        historical[p]=h
    for p,h in old['metadata_bindings'].items():
        if p.startswith('data/dbn/'):
            require(p not in historical or historical[p]==h,'Retained exception sidecar differs: '+p)
            historical[p]=h
    # All retained hourly source/table bindings are verified during the approved
    # worker, without decoding minute, second, or derived populations again.
    hp = record(POINTERS[2]); hm = record(hp['manifest']['path'],hp['manifest']['sha256'])
    census_ref = hm['evidence']['census']
    census = record(census_ref['path'],census_ref['sha256'])
    for p,h in census['input_sha256'].items():
        require(p not in historical or historical[p]==h,'Conflicting historical binding: '+p)
        historical[p]=h
    for m in accounting['monthly']:
        historical[m['seal_path']]=m['seal_sha256']
    for p in bindings.keys() & historical.keys():
        require(bindings[p]==historical[p],'Retained evidence no longer binds current inputs: '+p)
    scopes = {(m['market'],int(m['month'][:4])) for m in accounting['monthly']
              if m['source_accounting']['quarantined']}
    by_path = {e['path']:e for e in inventory['entries']}
    require(len(by_path)==len(inventory['entries']),'Duplicate registered inventory paths')
    definitions = []
    for entry in inventory['entries']:
        if entry['kind']!='DBN' or entry['family']!='definition' or (entry['market'],entry['year']) not in scopes:
            continue
        # Check the complete physical file before selecting it. Never filter a
        # protected physical source down to an apparently safe diagnostic window.
        end = datetime.fromisoformat(entry['interval_end_exclusive'].replace('Z','+00:00'))
        if end.tzinfo is None: end=end.replace(tzinfo=timezone.utc)
        if int(end.timestamp())*SECOND>CUTOFF: continue
        side=by_path.get(entry['path']+'.manifest.json')
        require(side is not None,'Unpaired definition source')
        record(side['path'],side['sha256'])
        definitions.append(dict(entry=entry,sidecar=side))
        historical[entry['path']]=entry['sha256']
    previous = {e['path']:e['sha256'] for group in old['inventory'] for e in group['entries']}
    changed = [e for e in inventory['entries'] if e['kind']=='DBN'
               and (e['market'],e['year']) in {(g['market'],g['year']) for g in old['inventory']}
               and previous.get(e['path'])!=e['sha256']]
    # Only immutable metadata/implementation hashes are verified in preparation.
    # Full historical binding checks are deliberately deferred to approval.
    for p,h in bindings.items():
        if not p.startswith('data/dbn/'):
            require(sha256_file(contained(root,p))==h,'Prepared binding differs: '+p)
    implementation = {p:sha256_file(contained(root,p)) for p in
        ('src/futures_rebuild/coverage_remediation.py','tests/test_coverage_remediation.py')}
    return signed(dict(schema='coverage_remediation/1',status='PREPARED_REQUIRES_OWNER_APPROVAL',
        bindings=bindings,historical_bindings=historical,implementation=implementation,
        definition_sources=definitions,new_hourly_sources=changed,
        retained_index_rebinding=dict(path='data/active/timeframes.json',
            prior_sha256=old['historical_hash_bindings']['data/active/timeframes.json'],
            current_sha256=bindings['data/active/timeframes.json'],
            evidence=census_ref,reason='Completed coordinated daily publication; current hourly census successor retained.'),
        monthly_scope=[{k:e[k] for k in ('timeframe','market','month','coverage_start_ns','coverage_end_ns')}
                       for e in index['entries'] if e['timeframe'] in ('1h','1d')],
        limits=asdict(LIMITS),maximum_aggregate_memory_bytes=6*2**30,
        maximum_coordinator_memory_bytes=960*2**20,maximum_allocation_bytes=2*2**30,
        preparation_write_reserve_bytes=PREPARATION_WRITE_RESERVE,
        estimate_minutes=[45,150],estimate_basis='Retained 14-minute hash recheck and 29-minute reader checks; '
            'one sequential definition scan plus vectorized structural accounting, four-hour hard cap.',
        input_definition_bytes=sum(e['entry']['size_bytes'] for e in definitions),
        window_lengths={'1d':[1,5,20,60],'1h':[1,6,24,120]},window_year='START_UTC_YEAR',
        calendar_compression=False,as_of='WINDOW_END_PLUS_FIVE_SECONDS',
        historical_execution=False,publication=False,active_pointer_changes=[],
        recovery_authority=False,model_specific_eligibility='NOT_DECLARED'))


def verify(root, bindings, budget):
    for path,digest in bindings.items():
        require(budget.hash(contained(root,path))==digest,'Input changed: '+path)


def daily_cases(root, accounting, budget):
    cases=[]
    for month in accounting['monthly']:
        if not month['source_accounting']['quarantined']:
            continue
        path=contained(root,month['seal_path'])
        require(budget.hash(path)==month['seal_sha256'],'Quarantine seal changed')
        seal=read_record(path)
        for day in seal['coverage']:
            for item in day['quarantine']:
                case=classify_quarantine(item)
                case.update(seal_path=month['seal_path'],seal_sha256=month['seal_sha256'],
                            definition_scan=dict(date_identity_records=0,eligible_records=0,
                                                 other_ids_same_date=0,eligible_evidence=[]))
                cases.append(case)
    require(len(cases)==12583 and len({c['source_row_id'] for c in cases})==12583,
            'Daily quarantine population differs')
    return cases


def scan_definitions(root, plan, cases, budget):
    from .causal_observation_canary import _query_contract
    from .foundation.decoder import iter_definitions
    from .foundation.snapshot import DbnReleaseFile
    targets=defaultdict(dict)
    # Read just the already bound quarantine evidence; preserve exact identity
    # fields separately from the compact report records.
    for path in sorted({c['seal_path'] for c in cases}):
        for day in read_record(contained(root,path))['coverage']:
            for q in day['quarantine']:
                b=q['source_row']
                targets[b['market']][(b['event_at_ns'],b['publisher_id'],b['instrument_id'])]=b['row_sha256']
    by_id={c['source_row_id']:c for c in cases}
    by_day=defaultdict(list)
    for market,values in targets.items():
        for (day,publisher,instrument),row_id in values.items():
            by_day[(market,day)].append((publisher,instrument,row_id))
    scans=[]
    for source in plan['definition_sources']:
        budget.check()
        entry,side=source['entry'],source['sidecar']
        query=_query_contract(root,{**side,'paired_dbn_sha256':entry['sha256'],
                                   'paired_dbn_size_bytes':entry['size_bytes']})
        # These are diagnostic records in a new bound inventory context. They
        # are never inserted into historical daily lineage or a live resolver.
        binding=DbnReleaseFile(entry['path'],contained(root,entry['path']),entry['path'],
            entry['size_bytes'],entry['sha256'],plan['id'],plan['id'],plan['id'])
        count=0
        for definition in iter_definitions(binding,market=entry['market'],
                                            expected_query_contract=query,batch_rows=4096):
            count+=1
            if count%4096==0: budget.check()
            day=(definition.ts_recv_ns//DAY)*DAY
            for publisher,instrument,row_id in by_day.get((entry['market'],day),()):
                scan=by_id[row_id]['definition_scan']
                if (publisher,instrument)!=(definition.publisher_id,definition.instrument_id):
                    scan['other_ids_same_date']+=1
                    continue
                scan['date_identity_records']+=1
                if definition.ts_recv_ns<=day:
                    scan['eligible_records']+=1
                    scan['eligible_evidence'].append(dict(source_path=entry['path'],source_sha256=entry['sha256'],
                        row_ordinal=definition.row_ordinal,row_sha256=definition.row_sha256,
                        ts_recv_ns=definition.ts_recv_ns,actual_contract=definition.raw_symbol,
                        disposition='REQUIRES_FULL_IDENTITY_AND_ECONOMICS_REVIEW'))
        require(budget.hash(contained(root,entry['path']))==entry['sha256'],'Definition changed during scan')
        scans.append(dict(path=entry['path'],sha256=entry['sha256'],decoded_records=count,
                          complete_physical_file=True))
    covered={(s['entry']['market'],s['entry']['year']) for s in plan['definition_sources']}
    for case in cases:
        case['registered_definition_scope_present']=(case['market'],utc(case['start_ns']).year) in covered
        if case['definition_scan']['eligible_records']:
            case['disposition']='POTENTIAL_RECOVERY_REQUIRES_SEPARATE_PREPARATION'
        elif case['reason']=='ABSENT_MATCHING_IDENTITY' and case['definition_scan']['date_identity_records']==0:
            case['reason']=('IDENTITY_MISMATCH' if case['definition_scan']['other_ids_same_date']
                            else 'ABSENT_MATCHING_IDENTITY')
    return scans


def profiles(root, plan, cases, budget):
    from .causal_observation_parquet import read_table
    from .causal_timeframe_publication import validate_index, validate_release
    from .daily_source_admission import DailySourceReader
    from .hourly_source_admission import HourlySourceReader
    from .hourly_quality_reader import NATIVE_ALLOWED
    from .active_data_view import COHORT_PERMISSIONS
    purposes=sorted({p for uses in COHORT_PERMISSIONS.values() for p in uses})
    index=read_record(contained(root,'data/active/timeframes.json'))
    validate_index(root,index)
    profile_rows=[]
    examples=[]
    totals=Counter()
    accepted_hourly=excluded_hourly=0
    for timeframe,reader_type,step,lengths in [('1d',DailySourceReader,DAY,[1,5,20,60]),
                                              ('1h',HourlySourceReader,HOUR,[1,6,24,120])]:
        reader=reader_type(root)
        for market in sorted({e['market'] for e in index['entries'] if e['timeframe']==timeframe}):
            parts=[e for e in index['entries'] if (e['timeframe'],e['market'])==(timeframe,market)]
            rows=[]
            for entry in parts:
                budget.check()
                if entry['disposition']=='UNAVAILABLE':
                    totals[timeframe+'_unavailable_months']+=1
                    continue
                if timeframe=='1d':
                    release=validate_release(root,entry)
                    loaded=read_table(contained(root,entry['directory']+'/observations.parquet'),name='observations')
                    for row in loaded:
                        require(row['definition_received_at_ns']<=row['bar_start_ns'],'Future daily definition')
                    del release
                else:
                    loaded=reader.quality_reader._native_month(market,entry['month'])
                    for row in loaded: row['quality_accepted']=row['quality_disposition'] in NATIVE_ALLOWED
                    accepted_hourly+=sum(r['quality_accepted'] for r in loaded)
                    excluded_hourly+=sum(not r['quality_accepted'] for r in loaded)
                rows.extend(loaded)
            rows.sort(key=lambda r:r['bar_start_ns'])
            totals[timeframe+'_observations']+=len(rows)
            left=((min(e['coverage_start_ns'] for e in parts)+step-1)//step)*step
            right=(min(CUTOFF,max(e['coverage_end_ns'] for e in parts))//step)*step
            entries=[e for e in reader.manifest['entries'] if e['market']==market]
            if timeframe=='1h':
                entries=[dict(start_ns=e['admitted_start_ns'],end_ns=e['admitted_end_ns'],
                              permitted_uses=e['permitted_uses']) for e in entries
                         if e['admitted_start_ns'] is not None and e['admitted_end_ns'] is not None]
            quarantines=[c['start_ns'] for c in cases if c['market']==market] if timeframe=='1d' else []
            profile_rows.extend(window_profile(market=market,step=step,left=left,right=right,
                rows=rows,permissions=entries,quarantines=quarantines,lengths=lengths,purposes=purposes))
            # One positive native single-bar check per market/timeframe plus a
            # premature request. These are representative, not a full-window replay.
            eligible=next((r for r in rows if r.get('quality_accepted',True)),None)
            if eligible:
                request=dict(market=market,start_ns=eligible['bar_start_ns'],end_ns=eligible['bar_end_ns'],
                             as_of_ns=eligible['decision_eligible_at_ns'],source='native',purpose='LINEAGE_AUDIT')
                got=reader.read_window(**request)
                require(len(got)==1 and got[0]['row_id']==eligible['row_id'],'Public reader differs')
                try: reader.read_window(**{**request,'as_of_ns':eligible['available_at_ns']-1})
                except UnauthorizedOperation: pass
                else: raise IntegrityError('Premature public read succeeded')
                examples.append(dict(timeframe=timeframe,market=market,request=request,accepted=1,premature_refused=1))
            if market=='ES' and eligible:
                for length in lengths:
                    start=eligible['bar_start_ns']; end=start+length*step
                    if end>right: continue
                    expected=window_profile(market=market,step=step,left=start,right=end,
                        rows=[r for r in rows if start<=r['bar_start_ns']<end],permissions=entries,
                        quarantines=[t for t in quarantines if start<=t<end],
                        lengths=[length],purposes=['LINEAGE_AUDIT'])[0]['readable_windows']==1
                    request=dict(market=market,start_ns=start,end_ns=end,as_of_ns=end+LAG,
                                 source='native',purpose='LINEAGE_AUDIT')
                    try:
                        reader.read_window(**request)
                        actual=True
                    except UnauthorizedOperation:
                        actual=False
                    require(actual==expected,'Structural profile differs from public reader')
                    examples.append(dict(timeframe=timeframe,market=market,request=request,
                                         profile_expected_readable=expected,public_readable=actual))
            budget.check()
            del rows,entries
        del reader
        gc.collect()
    require((accepted_hourly,excluded_hourly)==(3023362,3),'Hourly accepted/excluded population differs')
    require(totals==Counter({'1d_observations':155026,'1h_observations':3023365,
        '1d_unavailable_months':21,'1h_unavailable_months':21}),'Active population counts differ')
    return profile_rows,examples,dict(totals)


def worker():
    request=json.loads(sys.stdin.buffer.readline(2**20))
    root=Path.cwd().resolve()
    plan=read_record(contained(root,request['plan_path']))
    auth=read_record(contained(root,request['authorization_path']))
    require(auth['status']=='OWNER_APPROVED_IN_THIS_TASK' and auth['plan_id']==plan['id']
            and auth['plan_sha256']==sha256_file(contained(root,request['plan_path']))
            and auth['operation']=='OFFLINE_COVERAGE_DIAGNOSTICS_NO_ACTIVATION','Diagnostic approval differs')
    require(request['operation']['output']==HERE+'/'+plan['id']+'/execution','Output scope differs')
    output=contained(root,request['operation']['output'])
    output.mkdir(exist_ok=False)
    retained_before=allocation(contained(root,HERE),100000)
    charged_before=retained_before['retained_logical_bytes']+PREPARATION_WRITE_RESERVE
    require(charged_before+2**20<plan['limits']['maximum_written_bytes'],'Prior diagnostic artifacts exhaust budget')
    budget=Budget(output,replace(Limits(**plan['limits']),
        maximum_written_bytes=plan['limits']['maximum_written_bytes']-charged_before))
    budget.terminal_reserve=2**20
    result=dict(status='INCOMPLETE',plan_id=plan['id'],publication=False,recovered_rows=0,
                model_specific_eligibility='NOT_DECLARED',prior_attempt_and_preparation_bytes=charged_before)
    try:
        result['phase']='bindings_before'
        verify(root,plan['implementation'],budget)
        verify(root,plan['bindings'],budget)
        verify(root,plan['historical_bindings'],budget)
        result['phase']='quarantine_classification'
        cases=daily_cases(root,read_record(contained(root,ACCOUNTING)),budget)
        result['phase']='registered_definition_scan'
        scans=scan_definitions(root,plan,cases,budget)
        budget.save(output/'cases.json',signed(dict(cases=cases,definition_scans=scans,
            reason_counts=dict(Counter(c['reason'] for c in cases)),
            weekday_counts=dict(Counter(c['weekday'] for c in cases)),
            existing_hourly_dispositions=dict(path=EXCEPTIONS+'/final_dispositions.json',
                sha256=plan['bindings'][EXCEPTIONS+'/final_dispositions.json']),
            new_hourly_source_inventory=plan['new_hourly_sources'])))
        result['phase']='window_profiles_and_public_checks'
        coverage,checks,totals=profiles(root,plan,cases,budget)
        budget.save(output/'coverage.json',signed(dict(profiles=coverage,baseline=totals,
            representative_checks=checks,year_assignment='WINDOW_START_UTC',
            discovery_2018_2022=[r for r in coverage if 2018<=r['year']<=2022 and r['purpose']=='DISCOVERY_RESEARCH'],
            note='Strict UTC grids, overlapping reasons, unique refused totals; not expected trading intervals or alpha tests.')))
        result['phase']='raw_guards_and_bindings_after'
        from .daily_foundation_admission import check_raw_routes as daily_raw
        from .hourly_foundation_audit import check_raw_routes as hourly_raw
        index=read_record(contained(root,'data/active/timeframes.json'))
        result['raw_routes']={'1d':daily_raw(root,index),'1h':hourly_raw(root,index)}
        verify(root,plan['bindings'],budget)
        verify(root,plan['historical_bindings'],budget)
        potential=sum(c['disposition'].startswith('POTENTIAL') for c in cases)
        result.update(status='PASS_DIAGNOSTICS_NO_ACTIVATION',potential_recovery_cases=potential,
                      baseline=totals,representative_checks=len(checks),
                      next_step='PREPARE_EVIDENCE_SUPPORTED_SUCCESSOR' if potential else 'RETAIN_DOCUMENTED_LIMITATIONS')
        report=(f"# Coverage remediation diagnostics\n\nStatus: {result['status']}. No activation or recovered rows.\n\n"
            f"All {len(cases):,} daily quarantines classified; {len(scans)} complete definition files scanned. "
            f"Potential omitted eligible records affect {potential} cases and require separate identity/economics review.\n\n"
            "Integrity: relevant retained source, table and implementation bindings checked before and after. "
            "Unchanged reconstruction evidence reused; no new full source-bar replay claimed.\n\n"
            "Coverage: coverage.json reports strict UTC windows by market, start year and purpose. "
            "Missing UTC bars are not necessarily missing trading intervals. Refusal reasons overlap; totals do not.\n\n"
            f"Reader enforcement: {sum('accepted' in c for c in checks)} representative market/timeframe pairs, each with an accepted "
            f"single-bar request and a premature refusal, plus {sum('profile_expected_readable' in c for c in checks)} "
            "ES window/profile comparisons; these are separate from exhaustive structural accounting. "
            "All existing daily/hourly raw-path routes refused.\n\n"
            "Permissions: no changes. Three GF exclusions, 21 unavailable months, 2,179 audit-only derived ZQ "
            "candidates, daily definition quarantines, 30 timing exclusions and three audit-only daily reference "
            "cases remain enforced. Existing hourly dispositions are reused only with unchanged bindings.\n\n"
            "Model eligibility remains NOT_DECLARED. No change to UTC-day semantics, calendars or assumed latency. "
            "See cases.json for exact source requirements and result.json plus supervision.json for measured resources.\n")
        with budget.open(output/'REPORT.md') as stream: stream.write(report.encode('utf-8'))
    except Exception as exc:
        result.update(status='FAILED_DIAGNOSTICS_NO_ACTIVATION',error_type=type(exc).__name__,error=str(exc))
    budget.finalizing=True
    budget.terminal_reserve=0
    result['resources']=budget.record()
    budget.save(output/'result.json',signed(result))
    return 0 if result['status'].startswith('PASS') else 1


def supervise(root,plan_path,authorization_path):
    """Approved-task callable; cannot publish, rebuild, or retry an existing run."""
    from .causal_1h_native import supervise as supervised_process
    from .causal_timeframe_execution import execution_lease
    root=Path(root).resolve()
    plan=read_record(contained(root,plan_path))
    require(plan_path==HERE+'/'+plan['id']+'/plan.json','Noncanonical diagnostic plan')
    for p,h in plan['implementation'].items():
        require(sha256_file(contained(root,p))==h,'Diagnostic implementation changed')
    auth=read_record(contained(root,authorization_path))
    require(auth['status']=='OWNER_APPROVED_IN_THIS_TASK' and auth['plan_id']==plan['id']
            and auth['plan_sha256']==sha256_file(contained(root,plan_path))
            and auth['operation']=='OFFLINE_COVERAGE_DIAGNOSTICS_NO_ACTIVATION','Diagnostic approval differs')
    output=HERE+'/'+plan['id']+'/execution'
    require(not contained(root,output).exists()
            and not contained(root,HERE+'/'+plan['id']+'/supervision.json').exists(),
            'No automatic diagnostic retry')
    result=dict(status='INCOMPLETE',plan_id=plan['id'])
    with execution_lease(root):
        try:
            require(memory_peak()<=960*2**20,'Coordinator memory exceeds 960 MiB')
            elapsed=supervised_process([sys.executable,'-B','-c',
                'from futures_rebuild.coverage_remediation import worker; raise SystemExit(worker())'],
                root,dict(plan_path=plan_path,authorization_path=authorization_path,
                    operation=dict(id=plan['id'],output=output)),Limits(**plan['limits']))
            child=read_record(contained(root,output+'/result.json'))
            require(child['status']=='PASS_DIAGNOSTICS_NO_ACTIVATION','Worker did not pass')
            peak=memory_peak()+child['resources']['peak_process_memory_bytes']
            require(memory_peak()<=960*2**20 and peak<=6*2**30,'Coordinator or aggregate memory exceeded')
            result.update(status='PASS_SUPERVISED_DIAGNOSTICS',seconds=elapsed,
                          conservative_aggregate_peak_bytes=peak)
        except Exception as exc:
            result.update(status='FAILED_SUPERVISED_DIAGNOSTICS',error_type=type(exc).__name__,error=str(exc))
            child_path=contained(root,output+'/result.json')
            if child_path.exists():
                failed_child=read_record(child_path)
                result['worker_failure_evidence']=dict(path=output+'/result.json',
                    sha256=sha256_file(child_path),phase=failed_child.get('phase'),
                    resources=failed_child.get('resources'))
            else:
                result['worker_resource_measurement']='UNAVAILABLE_NO_TERMINAL_WORKER_RECORD_NOT_A_PASS'
        result['retained']=allocation(contained(root,HERE),100000)
        result['coordinator_peak_bytes']=memory_peak()
        result['terminal_reserve_bytes']=2**20
        result['cumulative_diagnostic_bytes_upper_bound']=(result['retained']['retained_logical_bytes']
                                                         +PREPARATION_WRITE_RESERVE+2**20)
        if (result['retained']['retained_allocated_bytes']+2**20>2*2**30
                or result['cumulative_diagnostic_bytes_upper_bound']>2*2**30):
            result.update(status='FAILED_SUPERVISED_DIAGNOSTICS',error='Diagnostic cumulative storage limit exceeded')
        path=contained(root,HERE+'/'+plan['id']+'/supervision.json')
        with path.open('xb') as stream: stream.write(canonical_bytes(signed(result))+b'\n')
    return result


def main():
    root=Path.cwd().resolve()
    plan=prepare(root)
    directory=contained(root,HERE+'/'+plan['id'])
    directory.mkdir(parents=True,exist_ok=False)
    (directory/'plan.json').write_bytes(canonical_bytes(plan)+b'\n')
    print(json.dumps(dict(status=plan['status'],plan_path=HERE+'/'+plan['id']+'/plan.json',
                         definition_files=len(plan['definition_sources']),
                         definition_bytes=plan['input_definition_bytes'],historical_execution=False)))


if __name__=='__main__':
    main()
