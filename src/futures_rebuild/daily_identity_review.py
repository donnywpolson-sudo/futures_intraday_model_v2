"""Assess a completed-bar identity alternative; never recover or admit rows.

The CLI prepares a bounded diagnostic. Historical execution is task-approved.
The existing daily producer, shared identity resolver and readers stay unchanged.
"""
from collections import Counter, defaultdict
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
import gc
import json
import sys

from .canonical import canonical_bytes, sha256_file
from .causal_timeframe_storage import Budget, Limits, contained, read_record, signed
from .errors import ContractError, IntegrityError

HERE = 'state/unpublished_evidence/daily_identity_review_v1'
PRIOR = ('state/unpublished_evidence/coverage_remediation_v1/'
         'f9c1d233fd48e4b54147a21672a49a870e1eee054167d6bf89b0918b6809089d')
ACCOUNTING = 'state/unpublished_evidence/daily_foundation_v1/full_campaign_execution_v1/accounting.json'
OPERATION = 'ASSESS_DAILY_IDENTITY_FROM_ACCEPTED_HOURS_NO_ADMISSION'
HOUR, DAY, LAG = 3600*10**9, 86400*10**9, 5*10**9
CUTOFF = 1752444000*10**9
LIMITS = Limits(256*2**20, 5*2**30, 2*3600, maximum_files=256)
RULE = 'COMPLETED_NATIVE_DAY_IDENTITY_CORROBORATED_BY_ACCEPTED_NATIVE_HOURS_V1'


def require(condition, message):
    if not condition:
        raise IntegrityError(message)


def assess(item, hours, *, reference_hashes, rulebook):
    """Pure eligibility assessment. A supported result is not publication authority.

    Exact aggregate agreement covers fidelity to observed references, not proof
    of an exchange calendar or independent completeness of the provider feed.
    """
    from .foundation.records import ProviderBar, ProviderDefinition, ns_to_datetime
    from .foundation.identity import DefinitionIndex
    from .causal_observation_canary import _resolve_multiplier
    from .hourly_quality_reader import NATIVE_ALLOWED
    bar = ProviderBar(**item['source_row'])
    start, end = bar.event_at_ns, bar.event_at_ns + DAY
    require(start % DAY == 0 and 0 < start < end + LAG < CUTOFF,
            'Exact development UTC day and unchanged exclusive availability cutoff required')
    result = dict(market=bar.market, start_ns=start, source_row_id=bar.row_sha256,
                  rule=RULE, recovered_rows=0, admission=False,
                  decision_at_ns=end+LAG, original_identity_gate='RECEIVED_BY_BAR_START',
                  proposed_identity_gate='RESOLVED_FOR_COMPLETED_BAR_AT_DECISION_TIME')

    def refuse(reason):
        return dict(result, status='UNRESOLVED_QUARANTINED', reason=reason)

    if not hours:
        return refuse('NO_NATIVE_HOURLY_REFERENCES')
    hours = sorted(hours, key=lambda h: h['bar_start_ns'])
    times = [h['bar_start_ns'] for h in hours]
    if len(set(times)) != len(times) or any(t % HOUR or not start <= t < end for t in times):
        return refuse('INVALID_REFERENCE_INTERVALS')
    definitions = tuple(ProviderDefinition(**d) for d in item['matching_definitions'])
    if not definitions:
        return refuse('NO_MATCHING_DEFINITIONS')
    identity = ('dataset', 'market', 'publisher_id', 'instrument_id',
                'source_release_id', 'source_manifest_sha256')
    date = datetime.fromtimestamp(start//10**9, timezone.utc).date().isoformat()
    if any(any(getattr(d, k) != getattr(bar, k) for k in identity)
           or d.instrument_id_date_utc != date for d in definitions):
        return refuse('DEFINITION_DATE_OR_IDENTITY_MISMATCH')
    if any(d.ts_recv_ns > end+LAG for d in definitions):
        return refuse('DEFINITION_NOT_AVAILABLE_AT_DECISION')
    economics = ('raw_symbol', 'exchange', 'currency', 'min_price_increment_nano',
                 'unit_of_measure_qty_nano', 'unit_of_measure', 'activation_ns',
                 'expiration_ns', 'instrument_class', 'security_type')
    if (any(d.security_update_action not in ('ADD', 'MODIFY') for d in definitions)
            or len({tuple(getattr(d, k) for k in economics) for d in definitions}) != 1):
        return refuse('DELETION_OR_CONFLICTING_IDENTITY_LIFECYCLE_ECONOMICS')
    if any(not d.activation_ns <= start < end <= d.expiration_ns for d in definitions):
        return refuse('LIFECYCLE_DOES_NOT_COVER_WHOLE_NATIVE_DAY')
    index = DefinitionIndex(definitions)
    row_identity = ('actual_contract', 'currency', 'min_price_increment_nano', 'multiplier_nano')
    selected_definitions = set()
    for h in hours:
        if (h['source_cadence'] != '1h' or h['origin'] != 'NATIVE_1H'
                or h['quality_disposition'] not in NATIVE_ALLOWED):
            return refuse('EXCLUDED_OR_NON_NATIVE_REFERENCE')
        if (h['market'], h['publisher_id'], h['instrument_id'], h['source_release_id'], h['source_contract_id']) != (
                bar.market, bar.publisher_id, bar.instrument_id, bar.source_release_id, bar.source_manifest_sha256):
            return refuse('REFERENCE_IDENTITY_MISMATCH')
        if h['source_file_sha256'] not in reference_hashes:
            return refuse('REFERENCE_DELIVERY_DIFFERS')
        if (h['bar_end_ns'] != h['bar_start_ns']+HOUR
                or not h['bar_end_ns']+LAG <= h['available_at_ns'] <= h['decision_eligible_at_ns'] <= end+LAG):
            return refuse('REFERENCE_AVAILABILITY_DIFFERS')
        if any(h[k] != hours[0][k] for k in row_identity):
            return refuse('REFERENCE_CONTRACT_OR_UNIT_CHANGE')
        try:
            definition = index.resolve(replace(bar, event_at_ns=h['bar_start_ns']),
                                       decision_at=ns_to_datetime(end+LAG, 'daily decision'))
            multiplier, _ = _resolve_multiplier(rulebook=rulebook, market=bar.market, definition=definition)
        except ContractError:
            return refuse('REFERENCE_DEFINITION_NOT_CAUSALLY_RESOLVABLE')
        if (h['definition_row_sha256'] != definition.row_sha256
                or h['definition_received_at_ns'] != definition.ts_recv_ns
                or h['definition_source_file_sha256'] != definition.source_file_sha256
                or tuple(h[k] for k in row_identity) != (
                    definition.raw_symbol, definition.currency, definition.min_price_increment_nano, multiplier)):
            return refuse('REFERENCE_DEFINITION_OR_ECONOMICS_DIFFERS')
        selected_definitions.add(definition.row_sha256)
    aggregate = dict(open_nano=hours[0]['open_nano'], high_nano=max(h['high_nano'] for h in hours),
                     low_nano=min(h['low_nano'] for h in hours), close_nano=hours[-1]['close_nano'],
                     volume=sum(h['volume'] for h in hours))
    if any(getattr(bar, k) != v for k, v in aggregate.items()):
        return refuse('NATIVE_DAILY_OHLCV_DIFFERS_FROM_OBSERVED_HOURS')
    return dict(result, status='SUPPORTED_FOR_SUCCESSOR_REVIEW',
                reference_row_ids=[h['row_id'] for h in hours],
                definition_row_ids=sorted(selected_definitions),
                reference_hours=len(hours), first_reference_start_ns=times[0],
                coverage='OBSERVED_NATIVE_HOURS_ONLY', independent_market_coverage='NOT_ESTABLISHED',
                requirement='Separate native successor reconstruction, certification, admission review and activation')


def prepare(root):
    """Metadata and code only. Do not open quarantine seals or observation tables."""
    root = Path(root).resolve()
    prior = read_record(contained(root, PRIOR+'/plan.json'))
    bindings = dict(prior['bindings'])
    cases_path = PRIOR+'/execution/cases.json'
    cases = read_record(contained(root, cases_path))['cases']
    require(len(cases) == 12583, 'Quarantine baseline differs')
    seals = {c['seal_path']: c['seal_sha256'] for c in cases}
    sources = {c['source_path']: c['source_sha256'] for c in cases}
    for p in list(sources):
        sidecar = p+'.manifest.json'
        require(sidecar in prior['historical_bindings'], 'Native daily sidecar binding absent')
        sources[sidecar] = prior['historical_bindings'][sidecar]
    for pair in prior['definition_sources']:
        for e in (pair['entry'], pair['sidecar']):
            sources[e['path']] = e['sha256']
    for p in (PRIOR+'/plan.json', PRIOR+'/execution/result.json', PRIOR+'/supervision.json', cases_path, ACCOUNTING):
        bindings[p] = sha256_file(contained(root, p))
    result = read_record(contained(root, PRIOR+'/execution/result.json'))
    require(result['status'] == 'PASS_DIAGNOSTICS_NO_ACTIVATION' and result['potential_recovery_cases'] == 0,
            'Earlier-definition investigation is not the expected completed baseline')
    accounting = read_record(contained(root, ACCOUNTING))
    months = [m for m in accounting['monthly'] if m['source_accounting']['quarantined']]
    require({m['seal_path']: m['seal_sha256'] for m in months} == seals, 'Quarantine month inventory differs')
    scopes = {(m['market'], m['month']) for m in months}
    index = read_record(contained(root, 'data/active/timeframes.json'))
    hours = [e for e in index['entries'] if e['timeframe'] == '1h' and (e['market'], e['month']) in scopes]
    days = [e for e in index['entries'] if e['timeframe'] == '1d' and (e['market'], e['month']) in scopes]
    require(len(hours) == len(scopes), 'Hourly monthly coverage accounting differs')
    require(len(days) == len(scopes), 'Daily monthly coverage accounting differs')
    selected = {e['manifest']: e['manifest_sha256'] for e in hours+days if e['disposition'] == 'CERTIFIED'}
    for e in hours:
        if e['disposition'] == 'CERTIFIED':
            path = e['directory']+'/observations.parquet'
            selected[path] = prior['historical_bindings'][path]
    del prior, cases, accounting, index
    gc.collect()
    for p, digest in bindings.items():
        require(sha256_file(contained(root, p)) == digest, 'Prepared binding changed: '+p)
    bindings['configs/contract_economics_rules.json'] = sha256_file(contained(root,'configs/contract_economics_rules.json'))
    implementation = {p: sha256_file(contained(root, p)) for p in (
        'src/futures_rebuild/daily_identity_review.py', 'tests/test_daily_identity_review.py',
        'src/futures_rebuild/causal_1h_native.py')}
    inputs = seals | sources | selected
    return signed(dict(schema='daily_identity_review/1', status='PREPARED_REQUIRES_OWNER_APPROVAL',
        operation=OPERATION, rule=RULE, bindings=bindings, implementation=implementation,
        inputs=inputs, quarantine_seals=seals, hourly_months=hours, daily_months=days,
        quarantine_months=[{k:m[k] for k in ('market','month','seal_path','seal_sha256')} for m in months],
        expected_cases=12583, input_bytes=sum(contained(root,p).stat().st_size for p in inputs),
        dbn_operation='HASH_ONLY_NO_DECODING', limits=asdict(LIMITS),
        estimated_minutes=[15,45], maximum_coordinator_memory_bytes=960*2**20,
        maximum_aggregate_memory_bytes=6*2**30, publication=False, recovered_rows=0,
        prior_diagnostics_reused=PRIOR+'/execution/result.json', automatic_retry=False))


def worker():
    """Supervised, approved diagnostic only; never creates normalized output."""
    from .hourly_quality_reader import HourlyReader
    from .causal_observation_canary import _load_economics_rulebook
    request = json.loads(sys.stdin.buffer.readline(2**20))
    root = Path.cwd().resolve()
    plan = read_record(contained(root, request['plan_path']))
    auth = read_record(contained(root, request['authorization_path']))
    require(auth['status'] == 'OWNER_APPROVED_IN_THIS_TASK' and auth['operation'] == OPERATION
            and auth['plan_id'] == plan['id'] and auth['plan_sha256'] == sha256_file(contained(root, request['plan_path'])),
            'Exact daily identity diagnostic authorization required')
    require(request['plan_path'] == HERE+'/'+plan['id']+'/plan.json'
            and plan['operation'] == OPERATION and plan['rule'] == RULE
            and plan['publication'] is False and plan['limits'] == asdict(LIMITS),
            'Diagnostic plan scope or limits differ')
    output = contained(root, HERE+'/'+plan['id']+'/execution')
    require(request['operation']['output'] == HERE+'/'+plan['id']+'/execution', 'Output differs')
    output.mkdir(exist_ok=False)
    budget = Budget(output, LIMITS)
    budget.terminal_reserve = 2**20
    result = dict(status='INCOMPLETE', plan_id=plan['id'], admission=False, recovered_rows=0)

    def verify(bindings):
        for p, digest in bindings.items():
            require(budget.hash(contained(root,p)) == digest, 'Diagnostic input changed: '+p)

    try:
        result['phase'] = 'bindings_before'
        verify(plan['implementation']); verify(plan['bindings']); verify(plan['inputs'])
        reader = HourlyReader(root)
        rulebook = _load_economics_rulebook(root)
        result['phase'] = 'assess_quarantines'
        cases = []
        hourly = {(e['market'],e['month']):e for e in plan['hourly_months']}
        daily = {(e['market'],e['month']):e for e in plan['daily_months']}
        for month in plan['quarantine_months']:
            p = month['seal_path']
            budget.check()
            seal = read_record(contained(root,p))
            require(seal['month']==month['month'], 'Quarantine month differs')
            entry = hourly[(month['market'],month['month'])]
            hours = reader._native_month(entry['market'],entry['month']) if entry['disposition']=='CERTIFIED' else []
            by_day = defaultdict(list)
            for row in hours:
                by_day[row['bar_start_ns']//DAY*DAY].append(row)
            daily_entry = daily[(month['market'],month['month'])]
            reference_hashes = set()
            if daily_entry['disposition']=='CERTIFIED':
                release = read_record(contained(root,daily_entry['manifest']))
                require(release['seal']['id']==seal['id'], 'Published quarantine seal differs')
                reference_hashes = {e['sha256'] for e in release['candidate']['source_inventory']
                                    if e['kind']=='DBN' and e['family']=='ohlcv_1h'}
            for day in seal['coverage']:
                for item in day['quarantine']:
                    decision = assess(item, by_day[day['day_ns']], reference_hashes=reference_hashes, rulebook=rulebook)
                    cases.append(dict(decision, seal_path=p, seal_sha256=plan['quarantine_seals'][p]))
        require(len(cases)==plan['expected_cases'] and len({c['source_row_id'] for c in cases})==len(cases),
                'Complete quarantine accounting differs')
        reader._unchanged()
        result['phase'] = 'bindings_after'
        verify(plan['implementation']); verify(plan['bindings']); verify(plan['inputs'])
        budget.save(output/'cases.json', signed(dict(rule=RULE, admission=False, recovered_rows=0, cases=cases)))
        result.update(status='PASS_IDENTITY_ASSESSMENT_NO_ADMISSION', cases=len(cases),
                      dispositions=dict(Counter(c['status'] for c in cases)),
                      unresolved_reasons=dict(Counter(c['reason'] for c in cases if 'reason' in c)),
                      cases_sha256=sha256_file(output/'cases.json'))
    except Exception as exc:
        result.update(status='FAILED_IDENTITY_ASSESSMENT', error_type=type(exc).__name__, error=str(exc))
    budget.finalizing = True
    budget.terminal_reserve = 0
    result['resources'] = budget.record()
    budget.save(output/'result.json', signed(result))
    return 0 if result['status'].startswith('PASS_') else 1


def main():
    root = Path.cwd()
    plan = prepare(root)
    directory = contained(root,HERE+'/'+plan['id'])
    directory.mkdir(parents=True,exist_ok=False)
    (directory/'plan.json').write_bytes(canonical_bytes(plan)+b'\n')
    print(json.dumps(dict(status=plan['status'], plan_path=HERE+'/'+plan['id']+'/plan.json',
                         inputs=len(plan['inputs']), input_bytes=plan['input_bytes'], historical_execution=False)))


if __name__ == '__main__':
    main()
