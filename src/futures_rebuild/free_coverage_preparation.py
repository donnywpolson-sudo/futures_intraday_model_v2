"""Local-only preparation and pure calendar mechanics; no admission authority.

The CLI only inventories registered metadata. Historical calendar evaluation is
a separate approved task. It never opens market payloads or changes controls.
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import shutil
import time

from .canonical import canonical_bytes, sha256_file
from .causal_timeframe_storage import contained, signed
from .errors import IntegrityError
from .exchange_calendar import _localize_unique
from .feature_calendar_feasibility import CONTROLS, DAY, HOUR, LEFT, RIGHT, interval_partition, require

HERE = 'state/unpublished_evidence/free_coverage_remediation_v1'
EXECUTIONS = (HERE+'/calendar_execution', HERE+'/calendar_retry_v2')
CALENDAR = 'state/unpublished_evidence/feature_calendar_qualification_v1'
PUBLIC = CALENDAR + '/public_review_v1'
AUDIT = 'reports/coverage_clarity_v1/coverage.json'
WEEKDAYS = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
EXACT_FAMILIES = {'GF': 'LIVESTOCK', 'HE': 'LIVESTOCK', 'LE': 'LIVESTOCK',
                  'RTY': 'EQUITY_INDEX', 'SR1': 'RATES', 'SR3': 'RATES'}


def ns(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        require(len(value) == 10, 'Naive non-date boundary')
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp()) * 10**9


def source_disposition(entry, cutoff, allowed_families):
    """Decide from registry metadata BEFORE opening even a source sidecar."""
    try:
        left, right = ns(entry['interval_start_inclusive']), ns(entry['interval_end_exclusive'])
    except (KeyError, TypeError, ValueError, IntegrityError):
        return 'REJECT_UNKNOWN_INTERVAL'
    if left >= right:
        return 'REJECT_INVALID_INTERVAL'
    if right > cutoff:
        return 'REJECT_WHOLE_FILE_PROTECTED_BOUNDARY'
    if entry['family'] not in allowed_families:
        return 'REJECT_UNDECLARED_FAMILY'
    if not entry.get('admitted_standard_foundation'):
        return 'INVENTORY_ONLY_SEPARATE_SOURCE_AUTHORITY_REQUIRED'
    return 'ELIGIBLE_FOR_SEPARATELY_APPROVED_DIAGNOSTIC'


def recurring_segments(claim, *, left, right):
    """Compile only exact-code baseline claims; output remains candidate evidence.

    Bounds are explicit. The complement describes the stated recurring rule,
    not certification that no exceptional opening or later amendment exists.
    """
    require(claim.get('status') == 'BASELINE_RULE_AND_EXACT_CODES_SUPPORTED'
            and claim.get('timezone') == 'America/Chicago', 'Unqualified baseline')
    require(type(left) is int and type(right) is int and left < right, 'Invalid extent')
    known = ns(claim['available_by_utc'])
    effective = int(_localize_unique(claim['effective_from_local']).timestamp()) * 10**9
    left = max(left, effective)
    if left >= right:
        return []
    rule = claim['open_rule']
    overnight = 'end_next_day' in rule
    days = rule.get('start_days' if overnight else 'weekdays')
    require(days and set(days) <= set(WEEKDAYS), 'Invalid recurring weekdays')
    # Extra pauses are deliberately not inferred from the base open rule.
    # RTY's separate pause/amendment requires its own exact exhibit review.
    first = datetime.fromtimestamp(left // 10**9, timezone.utc).date() - timedelta(days=2)
    last = datetime.fromtimestamp(right // 10**9, timezone.utc).date() + timedelta(days=1)
    openings = []
    day = first
    while day <= last:
        if WEEKDAYS[day.weekday()] in days:
            end_day = day + timedelta(days=int(overnight))
            a = int(_localize_unique(f'{day}T{rule["start"]}:00').timestamp()) * 10**9
            b = int(_localize_unique(f'{end_day}T{rule["end_next_day" if overnight else "end"]}:00').timestamp()) * 10**9
            require(a < b, 'Invalid recurring open interval')
            if a < right and b > left:
                openings.append((max(a, left), min(b, right)))
        day += timedelta(days=1)
    result, cursor = [], left
    for a, b in sorted(openings):
        require(a >= cursor, 'Overlapping recurring opens')
        if cursor < a:
            result.append(dict(start_ns=cursor, end_ns=a, state='CLOSED'))
        result.append(dict(start_ns=a, end_ns=b, state='OPEN'))
        cursor = b
    if cursor < right:
        result.append(dict(start_ns=cursor, end_ns=right, state='CLOSED'))
    return [x | dict(available_at_ns=known, evidence_id=claim['id'],
                    authority='BASELINE_CANDIDATE_ONLY') for x in result]


def assess_interval(baseline, exceptions, *, start_ns, end_ns, as_of_ns):
    """Conservative conjunction: no override, late-known or absent evidence grant.

    Agreement is a review candidate, not runtime permission. Family mapping,
    source provenance and completeness still require certification.
    """
    a = interval_partition(baseline, start_ns=start_ns, end_ns=end_ns, as_of_ns=as_of_ns)
    b = interval_partition(exceptions, start_ns=start_ns, end_ns=end_ns, as_of_ns=as_of_ns)
    edges = sorted({start_ns, end_ns, *(x['start_ns'] for x in a+b), *(x['end_ns'] for x in a+b)})
    result = []
    ai = bi = 0
    for left, right in zip(edges, edges[1:]):
        while a[ai]['end_ns'] <= left:
            ai += 1
        while b[bi]['end_ns'] <= left:
            bi += 1
        states = (a[ai]['state'], b[bi]['state'])
        state = ('CONFLICTING' if 'CONFLICTING' in states or set(states) == {'OPEN', 'CLOSED'}
                 else 'UNKNOWN' if 'UNKNOWN' in states
                 else 'CANDIDATE_CLOSED' if states == ('CLOSED', 'CLOSED') else 'OPEN')
        result.append(dict(start_ns=left, end_ns=right, state=state,
            evidence_ids=sorted(set(a[ai]['evidence_ids']+b[bi]['evidence_ids'])),
            runtime_authority=False))
    require(sum(x['end_ns']-x['start_ns'] for x in result) == end_ns-start_ns,
            'Candidate partition did not reconcile')
    return result


def assess_market(claim, records, *, left=LEFT, right=RIGHT, check=lambda: None):
    """Calendar interval census only, NOT feature-window or price coverage."""
    baseline = recurring_segments(claim, left=left, right=right)
    exceptions = [dict(x, available_at_ns=r['available_at_ns'], evidence_id=r['evidence_id'])
                  for r in records for x in r['segments']]

    def by_day(items):
        result = {}
        for item in items:
            for day in range(max(left, item['start_ns']//DAY*DAY), min(right, item['end_ns']), DAY):
                result.setdefault(day, []).append(item)
        return result

    base_days, exception_days = by_day(baseline), by_day(exceptions)
    years = {}
    for day in range(left, right, DAY):
        check()
        year = datetime.fromtimestamp(day//10**9, timezone.utc).year
        counts = years.setdefault(year, {tf: Counter() for tf in ('1h', '1d')})
        for tf, step in (('1h', HOUR), ('1d', DAY)):
            for start in range(day, day+DAY, step):
                parts = assess_interval(base_days.get(day, []), exception_days.get(day, []),
                    start_ns=start, end_ns=start+step, as_of_ns=start+step+5*10**9)
                states = {x['state'] for x in parts}
                counts[tf]['intervals'] += 1
                if states == {'CANDIDATE_CLOSED'}:
                    counts[tf]['candidate_fully_closed'] += 1
                else:
                    counts[tf]['not_candidate_fully_closed'] += 1
                for state in states:
                    counts[tf]['contains_'+state.lower()] += 1
    return {str(y): {tf: dict(c) for tf, c in vals.items()} for y, vals in years.items()}


def synthetic_benchmark():
    claim = dict(id='generated', status='BASELINE_RULE_AND_EXACT_CODES_SUPPORTED',
        timezone='America/Chicago', available_by_utc='2017-01-01T00:00:00Z',
        effective_from_local='2017-01-01T00:00:00',
        open_rule=dict(weekdays=list(WEEKDAYS[:5]), start='08:30', end='13:05'))
    left, right = ns('2020-01-01'), ns('2021-01-01')
    baseline = recurring_segments(claim, left=left, right=right)
    records = [dict(available_at_ns=0, evidence_id='generated', segments=baseline)]
    started = time.monotonic()
    result = assess_market(claim, records, left=left, right=right)
    seconds = time.monotonic()-started
    require(result['2020']['1h']['intervals'] == 366*24
            and result['2020']['1d']['intervals'] == 366, 'Synthetic denominator differs')
    return dict(seconds=seconds, generated_market_years=1, generated_intervals=366*25,
                projected_six_market_five_year_seconds=seconds*30,
                prices_read=False, runtime_authority=False)


def prepare(root):
    root = Path(root).resolve()
    output = contained(root, HERE)
    require(not output.exists(), 'Prior preparation retained; use an explicitly scoped successor')
    started = time.monotonic()
    bindings = {}

    def load(relative, expected=None):
        path = contained(root, relative)
        digest = sha256_file(path)
        require(expected is None or digest == expected, 'Metadata binding changed: '+relative)
        require(relative not in bindings or bindings[relative] == digest, 'Conflicting binding')
        bindings[relative] = digest
        return json.loads(path.read_bytes())

    audit = load(AUDIT)
    for path, digest in audit['input_sha256'].items():
        require(sha256_file(contained(root, path)) == digest, 'Coverage audit is stale: '+path)
        bindings[path] = digest
    source = load('configs/source_contract.json')
    inv = load(source['complete_inventory']['path'], source['complete_inventory']['sha256'])
    cutoff = ns(source['selection_policy']['development_end_exclusive'])
    for path in CONTROLS:
        load(path)
    for pointer in ('configs/active_hourly_source_admission.json', 'configs/active_daily_source_admission.json'):
        p = load(pointer)
        manifest = load(p['manifest']['path'], p['manifest']['sha256'])
        for path, digest in manifest['bindings'].items():
            require(sha256_file(contained(root, path)) == digest, 'Active admission binding changed: '+path)
            require(path not in bindings or bindings[path] == digest, 'Conflicting admission binding')
            bindings[path] = digest
    candidate = load(CALENDAR+'/candidate.json')
    verification = load(CALENDAR+'/verification.json')
    require(bindings[CALENDAR+'/candidate.json'] == verification['candidate_sha256'], 'Calendar candidate changed')
    completion = load(PUBLIC+'/completion.json')
    for name, digest in completion['evidence_hashes'].items():
        path = PUBLIC+'/'+name
        require(sha256_file(contained(root, path)) == digest, 'Public evidence changed: '+path)
        bindings[path] = digest
    claims = load(PUBLIC+'/rule_claims.json')['claims']
    exceptions = load('state/unpublished_evidence/hourly_exception_recovery_v1/final_dispositions.json')
    entries = inv['entries']
    sidecars = {x['path']: x for x in entries if x['kind'] == 'SIDECAR'}
    physical, query_counts = [], Counter()
    micros = set(source['universe']['deferred_micro_roots'])
    for entry in entries:
        if entry['kind'] != 'DBN':
            continue
        disposition = source_disposition(entry, cutoff, source['selection_policy']['allowed_families'])
        roles = []
        if entry['market'] in micros:
            roles.append('micro_inventory')
        if entry['market'] in ('ZL', 'ZM') and entry['year'] == 2010:
            roles.append('zl_zm_derived_feasibility')
        if entry['market'] == 'ZQ' and entry['year'] in (2018, 2019, 2020):
            roles.append('zq_existing_source_inventory')
        if entry['market'] == 'KE' and entry['year'] == 2013:
            roles.append('ke_existing_source_inventory')
        if entry['market'] == 'GF' and entry['year'] in (2010, 2012, 2013):
            roles.append('gf_existing_source_inventory')
        if 2018 <= entry['year'] <= 2022 and entry['market'] not in micros:
            roles.append('same_contract_inventory')
        if not roles:
            continue
        item = dict(entry, roles=roles, disposition=disposition, payload_opened=False)
        adjacent = sidecars.get(entry['path']+'.manifest.json')
        if adjacent is not None:
            item['sidecar'] = {k: adjacent[k] for k in ('path', 'sha256', 'size_bytes')}
        # Do not open protected or separately unauthorized micro sidecars.
        if disposition == 'ELIGIBLE_FOR_SEPARATELY_APPROVED_DIAGNOSTIC' and adjacent:
            meta = load(adjacent['path'], adjacent['sha256'])
            item['query_type'] = meta.get('stype_in', 'NOT_DECLARED_IN_ADJACENT_SIDECAR')
            item['requested_symbols'] = meta.get('symbols_requested', [])
            if 'same_contract_inventory' in roles and entry['family'].startswith('ohlcv_'):
                query_counts[item['query_type']] += 1
        physical.append(item)
    calendar_inputs = {}
    for item in candidate['records']:
        calendar_inputs[item['source']] = dict(path=item['source'], sha256=item['source_sha256'],
                                              role='RETAINED_CALENDAR_SOURCE_NO_PRICE_PAYLOAD')
    require(len(audit['active_markets']) == 41 and len(micros) == 17, 'Universe changed')
    cases = [
        dict(workstream='calendar', status='PREPARED_CANDIDATE', exact_code_baselines=sorted(EXACT_FAMILIES),
             requirement='Certify historical applicability, amendments and exception coverage; measure real window gain before reader admission.'),
        dict(workstream='same_contract', status='INVENTORIED_NOT_RECOVERED', ohlcv_query_types=dict(query_counts),
             requirement='Continuous-query deliveries do not establish complete pre-selection history; exact contract observations and mapping evidence must be demonstrated.'),
        dict(workstream='zl_zm_2010', status='PREPARED_INPUT_INVENTORY',
             requirement='Approved minute aggregation diagnostic, identity boundaries and coverage evidence; derived daily reader remains unsupported.'),
        dict(workstream='micro', status='INVENTORIED_SEPARATE_AUTHORITY_REQUIRED', markets=sorted(micros),
             requirement='Reuse applicable prior micro processing; source/admission authority must explicitly cover each addition.'),
        dict(workstream='zq', status='RETAINED_AUDIT_ONLY', candidates=2179,
             requirement='New exact selection/coverage evidence before reopening unchanged comparisons.'),
        dict(workstream='retained_exceptions', status='RETAINED_EXCLUSION_OR_EXPECTED_ABSENCE',
             prior_counts=exceptions['counts'], daily_recovered=12583, daily_availability_exclusions=30)]
    inventory = signed(dict(schema='free_coverage_inventory/1', workstreams=cases, physical_files=physical,
        raw_payloads_opened=0, protected_payloads_opened=0, source_cutoff_ns=cutoff,
        newly_readable_windows=None, recovered_rows=0, activated_rows=0))
    for path in ('src/futures_rebuild/free_coverage_preparation.py',
                 'src/futures_rebuild/feature_calendar_feasibility.py',
                 'src/futures_rebuild/daily_identity_rollout.py',
                 'src/futures_rebuild/exchange_calendar.py',
                 'src/futures_rebuild/causal_timeframe_storage.py',
                 'src/futures_rebuild/durable_windows_task_transport.py',
                 'src/futures_rebuild/canonical.py',
                 'tests/test_free_coverage_preparation.py'):
        bindings[path] = sha256_file(contained(root, path))
    benchmark = synthetic_benchmark()
    for path, digest in bindings.items():
        require(sha256_file(contained(root, path)) == digest, 'Preparation input changed: '+path)
    require(not contained(root, 'configs/active_feature_window_admission.json').exists(), 'Feature pointer already exists')
    prior = completion['cumulative_upper']
    scope = signed(dict(schema='free_coverage_calendar_scope/1', status='PREPARED_REQUIRES_OWNER_APPROVAL',
        operation='RETAINED_CALENDAR_CONJUNCTION_DIAGNOSTIC_ONLY', bindings=bindings,
        calendar_files=list(calendar_inputs.values()), market_data_inputs=[],
        claims_path=PUBLIC+'/rule_claims.json', candidate_path=CALENDAR+'/candidate.json',
        markets=audit['active_markets'], period=[LEFT, RIGHT],
        output=HERE+'/calendar_execution', expected_outputs=['candidate_assessment.json', 'result.json', 'supervision.json', 'terminal.json'],
        pointer_changes=[], publication=False, price_reads=False, dbn_decode=False, network=False,
        carried_prior_upper=prior, preparation_reserve=dict(written_bytes=64*2**20, allocation_bytes=64*2**20, files=500),
        cumulative_limits=dict(written_bytes=8*2**30, allocation_bytes=10*2**30, files=100000),
        limits=dict(maximum_written_bytes=128*2**20, maximum_memory_bytes=5*2**30,
                    maximum_seconds=1800, minimum_free_bytes=64*2**30, maximum_files=200),
        maximum_coordinator_memory_bytes=960*2**20, maximum_aggregate_memory_bytes=6*2**30,
        estimate_seconds=[30, 180], rollback='No control replacement; retain diagnostic output on success or failure',
        synthetic_benchmark=benchmark,
        estimate_basis='Generated one-market-year measured; extrapolate 30 exact-code market-years plus sequential binding/source hashing. Earlier 520-schedule replay was retained, not repeated.',
        active_feature_pointer_predecessor=None, snapshot_seconds=time.monotonic()-started,
        free_bytes=shutil.disk_usage(root).free,
        deferred='No price-window assessment or candidate activation is included in this first approval.'))
    output.mkdir(parents=True)
    for name, value in [('inventory.json', inventory), ('calendar_scope.json', scope)]:
        with (output/name).open('xb') as stream:
            stream.write(canonical_bytes(value)+b'\n')
    return dict(output=HERE, inventory_sha256=sha256_file(output/'inventory.json'),
                scope_sha256=sha256_file(output/'calendar_scope.json'), workstreams=len(cases),
                physical_files=len(physical), metadata_bindings=len(bindings), query_types=dict(query_counts))


def authorized_scope(root, scope_path, authorization_path):
    from .causal_timeframe_storage import read_record
    scope_file = contained(root, scope_path)
    scope = read_record(scope_file)
    authorization = read_record(contained(root, authorization_path))
    require(scope.get('schema') == 'free_coverage_calendar_scope/1'
            and scope.get('operation') == 'RETAINED_CALENDAR_CONJUNCTION_DIAGNOSTIC_ONLY'
            and scope.get('output') in EXECUTIONS
            and scope.get('publication') is False and scope.get('price_reads') is False
            and scope.get('dbn_decode') is False and scope.get('network') is False
            and scope.get('pointer_changes') == [] and scope.get('market_data_inputs') == [],
            'Unsupported diagnostic scope')
    require(authorization.get('operation') == scope['operation']
            and authorization.get('scope_sha256') == sha256_file(scope_file)
            and authorization.get('owner_approved') is True
            and type(authorization.get('deadline_epoch')) in (int, float)
            and time.time() < authorization['deadline_epoch'] <= time.time()+1800,
            'Exact unexpired owner-approved diagnostic scope required')
    require(not contained(root, 'configs/active_feature_window_admission.json').exists(),
            'Feature pointer predecessor changed')
    for path, digest in scope['bindings'].items():
        require(sha256_file(contained(root, path)) == digest, 'Prepared binding changed: '+path)
    return scope, authorization


def worker():
    """Supervised task entry only; CLI remains preparation-only."""
    import sys
    from .causal_timeframe_storage import Budget, Limits
    from .daily_identity_rollout import memory
    request = json.load(sys.stdin)
    root = Path.cwd().resolve()
    require(request.get('output') in EXECUTIONS, 'Unrecognized diagnostic output')
    output = contained(root, request['output'])
    result = dict(status='FAILED_CALENDAR_CONJUNCTION_DIAGNOSTIC', phase='authorization',
                  publication=False, market_data_reads=False)
    budget = None
    try:
        scope, auth = authorized_scope(root, request['scope'], request['authorization'])
        require(scope['output'] == request['output'], 'Worker output differs from approved scope')
        budget = Budget(output, Limits(**scope['limits']))
        budget.terminal_reserve = 2*2**20

        def check():
            require(time.time() < auth['deadline_epoch'], 'Calendar diagnostic deadline exceeded')
            budget.check()

        result['phase'] = 'source_bindings'
        for entry in scope['calendar_files']:
            check()
            require(sha256_file(Path(entry['path'])) == entry['sha256'], 'Calendar source changed')
        claims = json.loads(contained(root, scope['claims_path']).read_bytes())['claims']
        records = json.loads(contained(root, scope['candidate_path']).read_bytes())['records']
        assessments = []
        result['phase'] = 'calendar_conjunction'
        for market in scope['markets']:
            check()
            if market not in EXACT_FAMILIES:
                assessments.append(dict(market=market, status='UNRESOLVED_MAPPING_OR_BASELINE',
                    years=list(range(2018, 2023)), runtime_authority=False, window_gain=None))
                continue
            choices = [c for c in claims if market in c['markets']
                       and c['status'] == 'BASELINE_RULE_AND_EXACT_CODES_SUPPORTED']
            require(len(choices) == 1, 'Ambiguous exact-code baseline')
            family_records = [r for r in records if r['family'] == EXACT_FAMILIES[market]]
            counts = assess_market(choices[0], family_records, check=check)
            assessments.append(dict(market=market, status='CANDIDATE_CONJUNCTION_NOT_ADMISSION',
                years=counts, runtime_authority=False, window_gain=None,
                requirements=['Exact product applicability of family holiday entries',
                    'Availability provenance and schedule amendment/exception completeness',
                    'Price/quality/permission/identity checks at fixed feature endpoints']))
        require(len(assessments) == 41, 'All-market denominator changed')
        budget.save(output/'candidate_assessment.json', signed(dict(markets=assessments,
            status='PASS_CALENDAR_CONJUNCTION_DIAGNOSTIC_ONLY',
            runtime_closure_authority=False, new_readable_feature_windows=None,
            note='Counts concern individual calendar intervals, not readable histories or price availability.')))
        result['phase'] = 'final_bindings'
        authorized_scope(root, request['scope'], request['authorization'])
        for entry in scope['calendar_files']:
            check()
            require(sha256_file(Path(entry['path'])) == entry['sha256'], 'Calendar source changed during diagnostic')
        check()
        # Shared supervisor transport contract; diagnostic authority stays separate.
        result.update(status='PASS_RECOVERY_PHASE',
                      diagnostic_status='PASS_CALENDAR_CONJUNCTION_DIAGNOSTIC_ONLY')
    except BaseException as exc:
        result.update(error_type=type(exc).__name__, error=str(exc)[:2048])
    finally:
        result['memory'] = memory()
        if budget is not None:
            result['resources_before_terminal'] = budget.record()
            budget.finalizing = True
            budget.terminal_reserve = 0
            budget.save(output/'result.json', signed(result))
        else:
            with (output/'result.json').open('xb') as stream:
                stream.write(canonical_bytes(signed(result))+b'\n')
    return 0 if result['status'] == 'PASS_RECOVERY_PHASE' else 1


def run_approved(root, *, scope_path, authorization_path):
    """One approved diagnostic attempt; never staging, activation or automatic retry."""
    import sys
    from .causal_timeframe_storage import Limits, allocation, read_record
    from .daily_identity_rollout import memory, resource_gate, supervise
    root = Path(root).resolve()
    selected = read_record(contained(root, scope_path))['output']
    require(selected in EXECUTIONS, 'Unrecognized diagnostic output')
    output = contained(root, selected)
    require(not output.exists(), 'Prior diagnostic attempt retained; no automatic retry')
    output.mkdir()
    started = time.monotonic()
    terminal = dict(status='FAILED_CALENDAR_CONJUNCTION_DIAGNOSTIC', active_controls_changed=False)
    try:
        scope, auth = authorized_scope(root, scope_path, authorization_path)
        prior, reserve, caps = scope['carried_prior_upper'], scope['preparation_reserve'], scope['cumulative_limits']
        upper = {k: prior[k]+reserve[k]+(200 if k == 'files' else 128*2**20) for k in caps}
        require(all(upper[k] <= caps[k] for k in caps), 'Cumulative resource allowance insufficient')
        require(shutil.disk_usage(root).free >= 64*2**30+128*2**20, 'Free-space reserve insufficient')
        request = dict(phase='calendar_conjunction', scope=scope_path, authorization=authorization_path,
            deadline_epoch=auth['deadline_epoch'], result_path=selected+'/result.json', output=selected)
        measured = supervise([sys.executable, '-B', '-c',
            'from futures_rebuild.free_coverage_preparation import worker; raise SystemExit(worker())'],
            root, request, Limits(**scope['limits']), diagnostic=output/'supervision.json')
        terminal['supervision'] = measured
        resource_gate(measured, coordinator_limit=scope['maximum_coordinator_memory_bytes'],
            worker_limit=scope['limits']['maximum_memory_bytes'],
            aggregate_limit=scope['maximum_aggregate_memory_bytes'], deadline=auth['deadline_epoch'])
        result = read_record(output/'result.json')
        require(measured['status'] == 'PASS_SUPERVISED_PHASE'
                and result['status'] == 'PASS_RECOVERY_PHASE'
                and result.get('diagnostic_status') == 'PASS_CALENDAR_CONJUNCTION_DIAGNOSTIC_ONLY',
                'Historical diagnostic failed')
        authorized_scope(root, scope_path, authorization_path)
        owned = allocation(output, 200)
        counters = result['resources_before_terminal']
        require(max(counters['application_written_bytes'], counters.get('process_write_transfer_bytes') or 0)
                +2*2**20 <= 128*2**20 and owned['retained_allocated_bytes']+2*2**20 <= 128*2**20,
                'Diagnostic write/allocation allowance exceeded')
        terminal.update(status=result['diagnostic_status'], resources=counters, retained=owned,
                        cumulative_reserved_upper=upper, terminal_reserve_bytes=2*2**20)
        return terminal
    except BaseException as exc:
        terminal.update(error_type=type(exc).__name__, error=str(exc)[:2048])
        raise
    finally:
        terminal.update(seconds=time.monotonic()-started, memory=memory(), retained_before_terminal=allocation(output, 200))
        with (output/'terminal.json').open('xb') as stream:
            stream.write(canonical_bytes(signed(terminal))+b'\n')


if __name__ == '__main__':
    print(json.dumps(prepare(Path.cwd()), indent=2))
