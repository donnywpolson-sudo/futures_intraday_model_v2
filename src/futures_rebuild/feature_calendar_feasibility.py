"""Calendar feasibility preparation; never publication or feature-read authority.

The CLI inventories metadata only. Original workbook decoding is a separately
approved, supervised task. Existing calendars/readers are not modified.
"""
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
import io
import json
from pathlib import Path
from zipfile import ZipFile

from .canonical import canonical_bytes, sha256_file, assert_plain_file, assert_no_linklike_ancestors
from .causal_timeframe_storage import contained, signed
from .cme_calendar_source_adequacy import parse_family_schedule, read_xls_bytes
from .cme_calendar_successor import FAMILY_PATTERN, MARKET_FAMILY, RECOVERY_RECORD
from .errors import IntegrityError
from .exchange_calendar import _localize_unique

HERE = 'state/unpublished_evidence/feature_calendar_feasibility_v2'
RELOCATIONS = 'state/unpublished_evidence/feature_calendar_source_recovery_v1/relocations.json'
PLAN = 'state/unpublished_evidence/l0_alpha_gap_diagnosis_v2/plan.json'
TIMING = 'state/unpublished_evidence/rolling_hour_predictive_v1/source_readiness_v1/calendar_timing.json'
NUMERIC = 'state/unpublished_evidence/l0_alpha_gap_diagnosis_v2/calendar_numeric_evidence.json'
SECOND = 10**9
HOUR = 3600 * SECOND
DAY = 24 * HOUR
LEFT = 1514764800 * SECOND
RIGHT = 1672531200 * SECOND
CONTROLS = (
    'configs/active_daily_source_admission.json',
    'configs/active_hourly_source_admission.json',
    'configs/active_hourly_quality_reader.json',
    'configs/active_daily_recovery_reader_binding.json',
    'data/active/timeframes.json', 'data/active/catalog.json',
    'configs/approved_discovery_batch.json',
)


def require(condition, message):
    if not condition:
        raise IntegrityError(message)


def event_segments(events):
    """Pair explicit event boundaries only; reject ambiguous local timestamps.

These are family-level candidate intervals, not certified product closures.
No terminal CLOSE or CLOSED_ALL_DAY is extended to a fabricated boundary.
"""
    grouped = defaultdict(set)
    columns = defaultdict(list)
    for event in events:
        require(event.event in {'OPEN', 'CLOSE', 'HALT', 'PREOPEN', 'CLOSED_ALL_DAY'},
                'Unsupported calendar event')
        ns = int(_localize_unique(event.at.isoformat()).timestamp()) * SECOND
        grouped[ns].add('OPEN' if event.event == 'OPEN' else 'CLOSED')
        columns[ns].append(event.column)
    result = []
    times = sorted(grouped)
    for start, end in zip(times, times[1:]):
        states = grouped[start]
        result.append(dict(start_ns=start, end_ns=end,
            state=next(iter(states)) if len(states) == 1 else 'CONFLICTING',
            start_columns=sorted(set(columns[start])),
            end_columns=sorted(set(columns[end]))))
    return result


def interval_partition(segments, *, start_ns, end_ns, as_of_ns):
    """Disjoint coverage at one decision. Unknown/future evidence grants nothing."""
    require(all(type(x) is int for x in (start_ns, end_ns, as_of_ns))
            and 0 <= start_ns < end_ns <= as_of_ns, 'Invalid calendar query')
    edges = {start_ns, end_ns}
    usable = []
    for item in segments:
        a, b, known = item['start_ns'], item['end_ns'], item['available_at_ns']
        require(type(a) is int and type(b) is int and 0 <= a < b
                and item['state'] in {'OPEN', 'CLOSED', 'CONFLICTING'}
                and (known is None or type(known) is int and known >= 0)
                and type(item['evidence_id']) is str and bool(item['evidence_id']),
                'Invalid calendar evidence interval')
        if known is None or known > as_of_ns or a >= end_ns or b <= start_ns:
            continue
        usable.append(item)
        edges.update((max(a, start_ns), min(b, end_ns)))
    result = []
    ordered = sorted(edges)
    for a, b in zip(ordered, ordered[1:]):
        covering = [x for x in usable if x['start_ns'] <= a and b <= x['end_ns']]
        states = {x['state'] for x in covering}
        state = ('UNKNOWN' if not states else 'CONFLICTING' if len(states) > 1
                 or 'CONFLICTING' in states else next(iter(states)))
        result.append(dict(start_ns=a, end_ns=b, state=state,
            evidence_ids=sorted({x['evidence_id'] for x in covering})))
    return result


def closure_candidate_matrix(segments_by_family, markets):
    """All UTC dates, family evidence only. No observation or permission claims.

Each day is evaluated at its end plus five seconds. Later-known evidence remains
in the interval ledger for subsequent decisions; this matrix does not discard it.
"""
    indexed = defaultdict(list)
    for family, segments in segments_by_family.items():
        for item in segments:
            for day in range(max(LEFT, item['start_ns'] // DAY * DAY),
                             min(RIGHT, item['end_ns']), DAY):
                indexed[family, day].append(item)
    for market in sorted(markets):
        require(market in MARKET_FAMILY, 'Market absent from candidate family inventory')
        family = MARKET_FAMILY[market]
        for day in range(LEFT, RIGHT, DAY):
            pieces = interval_partition(indexed[family, day], start_ns=day,
                end_ns=day+DAY, as_of_ns=day+DAY+5*SECOND)
            totals = Counter()
            for p in pieces:
                totals[p['state']] += p['end_ns']-p['start_ns']
            require(sum(totals.values()) == DAY, 'Calendar partition does not reconcile')
            yield dict(market=market, date=datetime.fromtimestamp(day//SECOND, timezone.utc).date().isoformat(),
                candidate_family=family, candidate_state_ns=dict(totals),
                product_closure_authority=False,
                required_evidence='HISTORICAL_PRODUCT_VENUE_APPLICABILITY_AND_AVAILABILITY',
                newly_readable_windows=None)


def verify_file(item):
    path = Path(item['path'])
    assert_no_linklike_ancestors(path)
    assert_plain_file(path)
    require(path.stat().st_size == item['size_bytes'] and sha256_file(path) == item['sha256'],
            'Prepared input changed: '+str(path))
    return path


def relocated_input(registered_path, digest, entries):
    """Resolve only the exact original path/hash pair in verified custody evidence."""
    def key(value):
        return str(Path(value)).removeprefix('\\\\?\\').casefold()
    matches = [e for e in entries if key(e['registered_path']) == key(registered_path)
               and e['sha256'] == digest]
    require(len(matches) == 1, 'Missing or ambiguous registered calendar relocation')
    item = dict(matches[0])
    verify_file(item)
    return item | dict(status='PRESENT_REGISTERED_BYTES_VERIFIED')


def decode_sources(scope, check=lambda: None):
    """Approved worker only: exact registered files, sequential bounded XLS reads."""
    timings = json.loads(verify_file(scope['timing']).read_bytes())
    by_member = defaultdict(list)
    for row in timings['results']:
        by_member[row['source_key'], row['member']].append(row)
    expected = json.loads(verify_file(scope['numeric']).read_bytes())
    saved = {(str(Path(x['source'])), x['member'], x['family']): x
             for x in expected['parsed_schedules']}
    records, failures = [], []
    for source in scope['calendar_files']:
        check()
        path = verify_file(source)
        registered = Path(source.get('registered_path', source['path']))
        raw = path.read_bytes()
        require(sha256(raw).hexdigest() == source['sha256'], 'Source changed during read')
        if path.suffix.lower() == '.zip':
            with ZipFile(io.BytesIO(raw)) as archive:
                members = [x for x in archive.infolist() if x.filename.lower().endswith('.xls')]
                require(len({x.filename for x in members}) == len(members), 'Duplicate workbook destination')
                require(sum(x.file_size for x in members) <= 32*2**20, 'Oversized workbook archive')
                workbooks = [(x.filename, archive.read(x)) for x in members]
        else:
            require(path.suffix.lower() == '.xls', 'Non-workbook source refused')
            workbooks = [(registered.name, raw)]
        for member, data in workbooks:
            check()
            publications = by_member[registered.stem, member]
            captures = {x.get('recorded_capture_utc') for x in publications
                        if x.get('cdx_content_digest_matches') is True}
            require(len(captures) <= 1, 'Conflicting archive capture evidence')
            known = None
            if captures and None not in captures:
                captured = datetime.fromisoformat(next(iter(captures)))
                require(captured.tzinfo is not None, 'Naive archive capture time')
                known = int(captured.timestamp())*SECOND
            # The separate January 2019 acquisition is inventoried, but its
            # archive response/header provenance still needs review. Do not
            # promote its URL timestamp to a verified availability claim here.
            cells = read_xls_bytes(data)
            for family, pattern in sorted(FAMILY_PATTERN.items()):
                previous = saved.get((str(registered), member, family))
                try:
                    parsed = parse_family_schedule(cells, label_pattern=pattern)
                    events = [asdict(e) | {'at': e.at.isoformat()} for e in parsed.events]
                    if previous is not None:
                        require(previous['events'] == events and previous['member_sha256'] == sha256(data).hexdigest(),
                                'Retained parser evidence changed')
                    segments = event_segments(parsed.events)
                except IntegrityError as exc:
                    if previous is not None:
                        raise
                    failures.append(dict(source=str(path), member=member, family=family,
                                         reason=str(exc), disposition='UNRESOLVED_NO_CLOSURE_AUTHORITY'))
                    continue
                evidence_id = sha256(canonical_bytes(dict(source=source['sha256'], member=member,
                    member_sha256=sha256(data).hexdigest(), family=family, events=events))).hexdigest()
                records.append(dict(evidence_id=evidence_id, source=str(path), registered_source=str(registered),
                    source_sha256=source['sha256'],
                    member=member, member_sha256=sha256(data).hexdigest(), family=family,
                    label=parsed.label, sheet=parsed.sheet, row_zero_based=parsed.row,
                    events=events, available_at_ns=known,
                    availability_basis='RETAINED_CONTENT_MATCH_CAPTURE_REQUIRES_PROVENANCE_REVIEW' if known else 'NOT_ESTABLISHED',
                    authority='CANDIDATE_FAMILY_INTERVALS_ONLY', segments=segments))
        verify_file(source)
    return records, failures


def prepare(root):
    """Metadata-only, create-only preparation; never decode source workbooks."""
    root = Path(root).resolve()
    output = contained(root, HERE)
    require(not output.exists(), 'Existing attempt preserved; do not automatically retry')
    require(not contained(root, 'configs/active_feature_window_admission.json').exists(),
            'An existing feature policy requires a separately reviewed successor')
    from .causal_timeframe_storage import read_record
    relocations = read_record(contained(root, RELOCATIONS))
    require(relocations.get('schema') == 'feature_calendar_source_relocations/1'
            and relocations.get('status') == 'PASS_EXACT_REGISTERED_BYTES_LOCATED',
            'Verified local calendar relocation evidence required')
    for item in relocations['custody_bindings']:
        verify_file(item)

    def ref(relative, expected=None):
        path = contained(root, relative)
        if not path.exists():
            return relocated_input(str(path), expected, relocations['entries'])
        digest = sha256_file(path)
        require(expected is None or digest == expected, 'Registered metadata binding changed: '+relative)
        return dict(path=str(path), sha256=digest, size_bytes=path.stat().st_size, status='PRESENT_METADATA_BOUND')

    metadata = json.loads(contained(root, PLAN).read_bytes())
    physical = []
    for name, digest in metadata['calendar_sources'].items():
        physical.append(relocated_input(name, digest, relocations['entries']) |
                        dict(role='REGISTERED_HOLIDAY_WORKBOOK_BYTES_VERIFIED'))
    recovered = json.loads(contained(root, RECOVERY_RECORD.as_posix()).read_bytes())
    item = recovered['raw']
    path = contained(root, item['path'])
    physical.append(relocated_input(str(path), item['sha256'], relocations['entries']) |
                    dict(role='RETAINED_JANUARY_2019_RECOVERY_AVAILABILITY_REVIEW_REQUIRED'))
    references = [PLAN, TIMING, NUMERIC, RECOVERY_RECORD.as_posix(), recovered['sidecar']['path'],
        'configs/cme_product_mapping_approval.json', 'configs/exchange_calendar_policy.json',
        'configs/active_cash_open_impulse_historical_calendar.json',
        'state/unpublished_evidence/hourly_enforcement_repair_v1/certified_successor_v2/execution/terminal.json']
    active_calendar = json.loads(contained(root, references[-2]).read_bytes())
    references += [active_calendar['calendar_path'],
        'state/unpublished_evidence/alpha_ladder_missing_session_provenance_audit/provenance_report.json',
        RELOCATIONS, 'state/unpublished_evidence/feature_calendar_feasibility_v1/scope.json']
    expected = {recovered['sidecar']['path']: recovered['sidecar']['sha256'],
                active_calendar['calendar_path']: active_calendar['calendar_sha256']}
    implementation = ['src/futures_rebuild/'+name for name in (
        'feature_calendar_feasibility.py', 'cme_calendar_source_adequacy.py',
        'cme_calendar_successor.py', 'exchange_calendar.py', 'canonical.py',
        'causal_timeframe_storage.py', 'daily_identity_rollout.py', 'durable_windows_task_transport.py')]
    implementation += ['tests/test_feature_calendar_feasibility.py']
    prior = json.loads(contained(root, references[8]).read_bytes())['resources']
    benchmark = synthetic_benchmark()
    metadata_refs = [ref(x, expected.get(x)) for x in references] + relocations['custody_bindings']
    missing = [x for x in physical+metadata_refs if x.get('status') == 'MISSING_REGISTERED_INPUT']
    scope = signed(dict(schema='feature_calendar_feasibility_scope/1',
        status='BLOCKED_MISSING_REGISTERED_INPUTS' if missing else 'PREPARED_REQUIRES_OWNER_APPROVAL',
        operation='CALENDAR_EVIDENCE_DIAGNOSTIC_ONLY',
        calendar_files=physical, metadata=metadata_refs, missing_inputs=missing,
        timing=ref(TIMING), numeric=ref(NUMERIC), implementation=[ref(x) for x in implementation],
        controls=[ref(x) for x in CONTROLS], markets=sorted(MARKET_FAMILY),
        period=[LEFT, RIGHT], calendar_matrix_rows=41*1826,
        feature_pointer='configs/active_feature_window_admission.json', pointer_changes=[],
        price_reads=False, dbn_decode=False, network=False, publication=False,
        expected_outputs=['candidate_intervals.json', 'calendar_matrix.jsonl', 'report.json',
                          'result.json', 'supervision.json', 'acceptance.json', 'operation_terminal.json'],
        limits=dict(maximum_written_bytes=128*2**20, maximum_memory_bytes=5*2**30,
                    maximum_seconds=1800, minimum_free_bytes=64*2**30, maximum_files=200),
        maximum_coordinator_memory_bytes=960*2**20, maximum_aggregate_memory_bytes=6*2**30,
        cumulative_limits=dict(written_bytes=8*2**30, allocation_bytes=10*2**30, files=100000),
        carried_prior_upper=dict(written_bytes=prior['cumulative_write_upper_bytes'],
            allocation_bytes=prior['cumulative_allocation_upper_bytes'], files=prior['cumulative_files_upper']),
        preparation_reserve=dict(written_bytes=64*2**20, allocation_bytes=64*2**20, files=400),
        preparation_reserve_basis='Retain v1 32 MiB/200-file reserve plus 32 MiB/200 files for relocation and v2 preparation; no allowance reset',
        synthetic_benchmark=benchmark,
        estimate_seconds=[300, 900], estimate_basis='31 small local source files; synthetic matrix timing reported separately; no OHLCV census or reader initialization',
        rollback='No active controls changed; retain failed evidence; no automatic retry',
        downstream_gate='Review product applicability, event semantics and provenance before any observation join or feature implementation'))
    output.mkdir(parents=True)
    with (output/'scope.json').open('xb') as stream:
        stream.write(canonical_bytes(scope)+b'\n')
    return dict(path=str(output/'scope.json'), sha256=sha256_file(output/'scope.json'))


def synthetic_benchmark():
    """Generate/serialize the complete matrix shape without any historic input."""
    import time
    from .daily_identity_rollout import memory
    started = time.perf_counter()
    generated = {family: [dict(start_ns=t, end_ns=t+3*DAY, state='CLOSED',
        available_at_ns=t, evidence_id='synthetic') for t in range(LEFT, RIGHT, 14*DAY)]
        for family in set(MARKET_FAMILY.values())}
    size = count = 0
    for row in closure_candidate_matrix(generated, list(MARKET_FAMILY)):
        size += len(canonical_bytes(row))+1
        count += 1
    return dict(kind='GENERATED_INPUT_NO_SOURCE_DECODE_NO_DISK_OUTPUT', rows=count,
        serialized_bytes=size, seconds=time.perf_counter()-started, memory=memory(),
        limitation='Does not measure workbook parsing or real reader cost')


def approved_scope(root, scope_path, authorization_path):
    """Task approval binds the exact preparation; never accepts approval text."""
    import time
    from .causal_timeframe_storage import read_record
    scope_file = contained(root, scope_path)
    scope = read_record(scope_file)
    auth = read_record(contained(root, authorization_path))
    require(auth.get('status') == 'OWNER_APPROVED_IN_THIS_TASK'
            and auth.get('operation') == 'CALENDAR_EVIDENCE_DIAGNOSTIC_ONLY'
            and auth.get('scope_sha256') == sha256_file(scope_file)
            and auth.get('scope_id') == scope['id']
            and 0 < auth['deadline_epoch']-auth['started_epoch'] <= scope['limits']['maximum_seconds']
            and auth['started_epoch'] <= time.time() < auth['deadline_epoch'],
            'Exact unexpired diagnostic approval required')
    require(scope.get('schema') == 'feature_calendar_feasibility_scope/1'
            and scope.get('status') == 'PREPARED_REQUIRES_OWNER_APPROVAL'
            and not scope.get('missing_inputs')
            and scope.get('operation') == auth['operation']
            and scope.get('price_reads') is False and scope.get('dbn_decode') is False
            and scope.get('network') is False and scope.get('publication') is False
            and scope.get('pointer_changes') == [], 'Diagnostic scope expanded')
    for item in scope['implementation'] + scope['controls'] + scope['metadata']:
        verify_file(item)
    return scope, auth


def worker():
    """Private supervised task entry. No price data, reader or activation route."""
    import sys
    import time
    from .causal_timeframe_storage import Budget, Limits
    from .daily_identity_rollout import memory, verify_job
    request = json.loads(sys.stdin.buffer.readline(2**20))
    root = Path.cwd().resolve()
    output = contained(root, HERE+'/execution')
    # Terminal resource evidence is reserved even when binding approval fails.
    require(not output.exists(), 'Prior execution preserved; automatic retry refused')
    output.mkdir()
    budget = Budget(output, Limits(128*2**20, 5*2**30, 1800, maximum_files=200))
    budget.terminal_reserve = 2*2**20
    result = dict(status='FAILED_CALENDAR_FEASIBILITY', phase='approval_and_bindings')
    try:
        scope, auth = approved_scope(root, request['scope'], request['authorization'])
        verify_job(request['supervision_job_name'], scope['limits']['maximum_memory_bytes'])

        def check():
            require(time.time() < auth['deadline_epoch'], 'Calendar diagnostic deadline exceeded')
            require(memory()['peak_bytes'] <= scope['limits']['maximum_memory_bytes'],
                    'Calendar worker memory exceeded')
            budget.check()

        result['phase'] = 'source_decode'
        records, failures = decode_sources(scope, check)
        budget.save(output/'candidate_intervals.json', signed(dict(records=records, unresolved=failures,
            authority=False, automatic_eligibility=False)))
        family_segments = defaultdict(list)
        for record in records:
            for segment in record['segments']:
                family_segments[record['family']].append(segment | dict(
                    available_at_ns=record['available_at_ns'], evidence_id=record['evidence_id']))
        result['phase'] = 'calendar_candidate_matrix'
        counts = Counter()
        rows = 0
        with budget.open(output/'calendar_matrix.jsonl') as stream:
            for item in closure_candidate_matrix(family_segments, scope['markets']):
                if rows % 1826 == 0:
                    check()
                rows += 1
                if item['candidate_state_ns'].get('CLOSED', 0):
                    counts[item['market']] += 1
                stream.write(canonical_bytes(item)+b'\n')
        require(rows == scope['calendar_matrix_rows'], 'Incomplete market/date accounting')
        # A 2026 mapping approval does not establish historical availability.
        # Existing family labels are candidates, not independently reviewed
        # date-specific product/venue mappings. No mapped closure is certified.
        report = dict(status='CALENDAR_CANDIDATES_RECONCILED_REVIEW_REQUIRED', matrix_rows=rows,
            parsed_family_schedules=len(records), unresolved_family_schedules=len(failures),
            dates_with_candidate_closed_time=dict(counts), product_closures_certified=0,
            source_files=len(scope['calendar_files']), observation_rows_read=0,
            readable_window_gain=None, gain_status='NOT_MEASURED_NO_OBSERVATION_JOIN',
            feature_reader_implemented=False, feature_reader_published=False,
            required_evidence=[
                'Product-specific historical venue/family applicability and effective dates',
                'Archive capture/content chain, including January 2019 timestamp provenance',
                'Workbook notes and event semantics; no omitted closure exceptions',
                'Independent regular-hours boundaries for dates absent from holiday workbooks',
                'Approved observation join only after qualifying closure evidence exists'],
            retained_reference_only=['current product mapping approval',
                'checkpoint weekday templates', 'source-unobservable sessions',
                'post-event captures before their recorded availability'],
            endpoint_comparison=dict(start_ns=LEFT, end_ns_exclusive=RIGHT,
                hourly_lengths=[1,6,24,120], daily_lengths=[1,5,20,60],
                as_of='ENDPOINT_PLUS_FIVE_SECONDS', initial_search_boundary=LEFT,
                retained_report_grouping='WINDOW_START_YEAR_RECONCILE_SEPARATELY'),
            model_specific_eligibility='NOT_DECLARED')
        budget.save(output/'report.json', signed(report))
        result['phase'] = 'final_bindings'
        approved_scope(root, request['scope'], request['authorization'])
        for item in scope['calendar_files']:
            check()
            verify_file(item)
        check()
        # Reuse the supervisor's existing generic passing-phase envelope only;
        # the diagnostic report above grants no publication/closure authority.
        result.update(status='PASS_RECOVERY_PHASE', diagnostic_status=report['status'])
    except BaseException as exc:
        result.update(error_type=type(exc).__name__, error=str(exc)[:2048])
    finally:
        result['memory'] = memory()
        result['resources_before_terminal'] = budget.record()
        budget.finalizing = True
        budget.terminal_reserve = 0
        budget.save(output/'result.json', signed(result))
    return 0 if result['status'] == 'PASS_RECOVERY_PHASE' else 1


def _run_approved(root, *, scope_path, authorization_path):
    """Called by the approved task, not the preparation CLI. No automatic retry."""
    import shutil
    import sys
    from .causal_timeframe_storage import Limits, allocation, read_record
    from .daily_identity_rollout import supervise, resource_gate
    root = Path(root).resolve()
    scope, auth = approved_scope(root, scope_path, authorization_path)
    require(not contained(root, HERE+'/execution').exists(), 'Prior execution exists; no retry')
    require(shutil.disk_usage(root).free >= 64*2**30+128*2**20, 'Insufficient free-space reserve')
    # Reserve the entire bounded phase before launch; retain prior attempt costs.
    prior, reserve, limits = scope['carried_prior_upper'], scope['preparation_reserve'], scope['cumulative_limits']
    for key in ('written_bytes', 'allocation_bytes'):
        require(prior[key]+reserve[key]+128*2**20 <= limits[key], 'Cumulative '+key+' budget insufficient')
    require(prior['files']+reserve['files']+200 <= limits['files'], 'Cumulative file budget insufficient')
    request = dict(phase='calendar_feasibility', scope=scope_path, authorization=authorization_path,
        deadline_epoch=auth['deadline_epoch'], result_path=HERE+'/execution/result.json')
    measured = supervise([sys.executable, '-B', '-c',
        'from futures_rebuild.feature_calendar_feasibility import worker; raise SystemExit(worker())'],
        root, request, Limits(**scope['limits']), diagnostic=contained(root, HERE+'/execution/supervision.json'))
    resource_gate(measured, coordinator_limit=scope['maximum_coordinator_memory_bytes'],
        worker_limit=scope['limits']['maximum_memory_bytes'], aggregate_limit=scope['maximum_aggregate_memory_bytes'],
        deadline=auth['deadline_epoch'])
    approved_scope(root, scope_path, authorization_path)
    result = read_record(contained(root, request['result_path']))
    owned = allocation(contained(root, HERE), 400)
    counters = result['resources_before_terminal']
    written = max(counters['application_written_bytes'], counters.get('process_write_transfer_bytes') or 0)
    require(written+2*2**20 <= 128*2**20, 'Measured phase write cap exceeded')
    require(owned['retained_allocated_bytes'] <= 160*2**20, 'Measured allocation cap exceeded')
    record = signed(dict(status='PASS_CALENDAR_DIAGNOSTIC_ONLY', supervision=measured,
        cumulative_write_upper_bytes=prior['written_bytes']+reserve['written_bytes']+written+2*2**20,
        cumulative_allocation_upper_bytes=prior['allocation_bytes']+owned['retained_allocated_bytes']+reserve['allocation_bytes']+2*2**20,
        cumulative_files_upper=prior['files']+owned['retained_files']+reserve['files']+2,
        owned_allocation=owned, terminal_allowance_bytes=2*2**20,
        controls_unchanged=True, gain_measured=False, publication=False))
    require(record['cumulative_write_upper_bytes'] <= limits['written_bytes']
            and record['cumulative_allocation_upper_bytes'] <= limits['allocation_bytes']
            and record['cumulative_files_upper'] <= limits['files'], 'Cumulative resource acceptance failed')
    with contained(root, HERE+'/execution/acceptance.json').open('xb') as stream:
        stream.write(canonical_bytes(record)+b'\n')
    return record


def run_approved(root, *, scope_path, authorization_path):
    """Retain coordinator failure evidence even if no worker was dispatched."""
    import time
    from .daily_identity_rollout import memory
    root = Path(root).resolve()
    terminal = contained(root, HERE+'/execution/operation_terminal.json')
    require(not terminal.parent.exists(), 'Prior attempt retained; no automatic retry')
    started = time.monotonic()
    record = dict(status='FAILED_CALENDAR_DIAGNOSTIC', scope=scope_path, active_controls_changed=False)
    try:
        result = _run_approved(root, scope_path=scope_path, authorization_path=authorization_path)
        record.update(status=result['status'], acceptance=HERE+'/execution/acceptance.json')
        return result
    except BaseException as exc:
        record.update(error_type=type(exc).__name__, error=str(exc)[:2048])
        raise
    finally:
        record.update(seconds=time.monotonic()-started, coordinator_memory=memory())
        terminal.parent.mkdir(parents=True, exist_ok=True)
        with terminal.open('xb') as stream:
            stream.write(canonical_bytes(signed(record))+b'\n')


if __name__ == '__main__':
    print(json.dumps(prepare(Path.cwd())))
