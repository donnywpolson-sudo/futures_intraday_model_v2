"""Inactive native daily recovery from certified quarantines and accepted hours.

These pure transformations confer no historical execution or publication authority.
The existing daily producer, shared identity resolver and active reader are intact.
"""
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace

from .canonical import sha256_json
from .causal_timeframe_storage import signed
from .daily_identity_review import assess, RULE, DAY, LAG, CUTOFF, require
from .foundation.identity import DefinitionIndex
from .foundation.records import ProviderBar, ProviderDefinition, ns_to_datetime
from .errors import ContractError, IntegrityError


def completed_definition(bar, definitions, hours, *, reference_hashes, rulebook):
    """Select original evidence only after reproducing the old quarantine."""
    index = DefinitionIndex(definitions)
    try:
        index.resolve(bar, decision_at=ns_to_datetime(bar.event_at_ns, 'original day start'))
    except ContractError as error:
        if error.args != ('bar instrument has no same-day definition received by bar start',):
            raise
    else:
        raise IntegrityError('Completed-day recovery requires the original quarantine')
    result = assess(dict(source_row=asdict(bar), matching_definitions=[asdict(d) for d in definitions]),
                    hours, reference_hashes=reference_hashes, rulebook=rulebook)
    require(result['status'] == 'SUPPORTED_FOR_SUCCESSOR_REVIEW',
            'Unsupported completed-day identity: '+result.get('reason', 'unknown'))
    first = min(hours, key=lambda h: h['bar_start_ns'])
    # This changes the lookup instant only, never the native source bar or receive time.
    return index.resolve(replace(bar, event_at_ns=first['bar_start_ns']),
                         decision_at=ns_to_datetime(bar.event_at_ns+DAY+LAG, 'completed day'))


def recover_day(item, hours, *, reference_hashes, rulebook, prior=None, replay=False):
    """Independent producer/replay row assembly, using the same declared rule."""
    require(item.get('reason') in (None, 'DEFINITION_NOT_KNOWN_AT_BAR_START'),
            'Different quarantine mechanism is outside recovery scope')
    bar = ProviderBar(**item['source_row'])
    decoded = SimpleNamespace(primary_1d=(bar,),
        definitions=tuple(ProviderDefinition(**d) for d in item['matching_definitions']),
        reference_1h=tuple(hours), reference_hashes=frozenset(reference_hashes))
    if replay:
        from .daily_identity_replay import reconstruct_day
        tables, _ = reconstruct_day(market=bar.market, decoded=decoded,
            start_ns=bar.event_at_ns, end_ns=bar.event_at_ns+DAY,
            source_contract_id=bar.source_manifest_sha256, source_release_id=bar.source_release_id,
            rulebook=rulebook, prior=prior)
    else:
        from .daily_identity_transform import build_day
        tables, _ = build_day(context=SimpleNamespace(source_contract_id=bar.source_manifest_sha256,
            source_release_id=bar.source_release_id), market=bar.market,
            window=dict(start_ns=bar.event_at_ns, end_ns=bar.event_at_ns+DAY),
            decoded=decoded, allowed_roots=frozenset({bar.market}),
            economics_rulebook=rulebook, prior_observation=prior)
    return tables


def _roll(row, previous):
    before = previous['actual_contract'] if previous else row['actual_contract']
    changed = before != row['actual_contract']
    return dict(row_id=row['row_id'], actual_contract_before=before,
        actual_contract_after=row['actual_contract'], effective_time_ns=row['bar_start_ns'] if changed else None,
        causal_selection_evidence_sha256=sha256_json(dict(definition_row_sha256=row['definition_row_sha256'],
            definition_received_at_ns=row['definition_received_at_ns'], prior_contract=before)),
        roll_flag=changed, price_discontinuity_flag=bool(changed and previous['close_nano'] != row['open_nano']),
        crossing_status='ROLL_BOUNDARY_UNADJUSTED' if changed else 'NO_CROSSING')


def recover_month(original, coverage, inputs, *, reference_hashes, rulebook, prior=None, replay=False):
    """Recover exact quarantined UTC days and repair their neighboring roll links.

    Caller verifies original certificate/file bindings before supplying these rows.
    Inputs map UTC starts to (original quarantine record, quality-labeled hours).
    Every existing quarantine must have one supported input; partial recovery fails.
    """
    require(set(original) == {'observations','missingness','roll','quality','cadence'}, 'Five tables required')
    require(coverage and len({c['day_ns'] for c in coverage}) == len(coverage), 'Invalid monthly coverage')
    require([c['day_ns'] for c in coverage] == list(range(coverage[0]['day_ns'],coverage[-1]['day_ns']+DAY,DAY)),
            'Monthly coverage must contain every UTC day')
    quarantines = {c['day_ns']: c for c in coverage if c['quarantined']}
    require(set(inputs) == set(quarantines), 'Recovery scope differs from exact monthly quarantines')
    observations = {r['bar_start_ns']:r for r in original['observations']}
    require(len(observations) == len(original['observations']), 'Duplicate original day')
    require(not set(observations) & set(inputs), 'Recovery duplicates an accepted observation')
    identifiers = {r['row_id'] for r in observations.values()}
    require(len(identifiers)==len(observations), 'Duplicate original row identity')
    for table in ('roll','quality','cadence'):
        require(len(original[table])==len(identifiers) and {r['row_id'] for r in original[table]}==identifiers,
                'Original five-table correspondence differs')
    require(len(original['missingness'])==len(identifiers)
        and {r['observation_row_id'] for r in original['missingness']}==identifiers,
        'Original missingness link differs')
    result = deepcopy(original)
    result['roll'] = []
    revised_coverage = deepcopy(coverage)
    evidence = []
    last = prior
    for c in revised_coverage:
        start = c['day_ns']
        require(c['source_primary_rows'] == sum(c[k] for k in
            ('observations','quarantined','excluded_start','excluded_availability')), 'Source accounting differs')
        require((start in observations) == (c['observations']==1), 'Original coverage/observations differ')
        if start in inputs:
            item, hours = inputs[start]
            require(c['quarantine']==[item] and c['quarantined']==1 and c['observations']==0
                and c['excluded_start']==c['excluded_availability']==0
                and c['start_ns']==start and c['end_ns']==start+DAY, 'Original quarantine binding differs')
            require(item['source_row']['event_at_ns']==start, 'Recovery source day differs')
            recovered = recover_day(item, hours, reference_hashes=reference_hashes,
                                    rulebook=rulebook, prior=last, replay=replay)
            row, = recovered['observations']
            # A quarantine has no old observation. Never clip an unknown interval
            # spanning other days or replace unrelated missingness evidence.
            require(not any(m['interval_start_ns'] < start+DAY and m['interval_end_ns'] > start
                            for m in result['missingness']), 'Recovery overlaps retained missingness evidence')
            for name in result:
                if name != 'roll': result[name].extend(recovered[name])
            c.update(observations=1, quarantined=0, quarantine=[], state='OBSERVED_WITH_EXPLICIT_GAPS',
                     comparison_counts={k:sum(r['result']==k for r in recovered['cadence'])
                         for k in ('MATCH','DISAGREEMENT','NOT_COMPARABLE','SOURCE_MISSING')})
            evidence.append(signed(dict(rule=RULE, market=row['market'], start_ns=start, row_id=row['row_id'],
                source_row_sha256=row['source_row_sha256'], quarantine_sha256=sha256_json(item),
                reference_row_ids=[h['row_id'] for h in sorted(hours,key=lambda h:h['bar_start_ns'])],
                definition_row_sha256=row['definition_row_sha256'],
                definition_received_at_ns=row['definition_received_at_ns'],
                first_reference_start_ns=min(h['bar_start_ns'] for h in hours),
                coverage='OBSERVED_NATIVE_HOURS_ONLY', available_at_ns=start+DAY+LAG)))
        else:
            row = observations.get(start)
        if row is not None:
            if replay:
                # Independently reconstruct neighboring roll metadata from retained
                # row values, without trusting the producer's patched roll table.
                before = row['actual_contract'] if last is None else last['actual_contract']
                roll = before != row['actual_contract']
                roll_row = {'row_id':row['row_id'], 'actual_contract_before':before,
                    'actual_contract_after':row['actual_contract'],
                    'effective_time_ns':row['bar_start_ns'] if roll else None,
                    'causal_selection_evidence_sha256':sha256_json({'definition_row_sha256':row['definition_row_sha256'],
                        'definition_received_at_ns':row['definition_received_at_ns'], 'prior_contract':before}),
                    'roll_flag':roll, 'price_discontinuity_flag':bool(last and roll and last['close_nano']!=row['open_nano']),
                    'crossing_status':'ROLL_BOUNDARY_UNADJUSTED' if roll else 'NO_CROSSING'}
            else:
                roll_row = _roll(row, last)
            result['roll'].append(roll_row)
            last = row
    result['observations'].sort(key=lambda r:r['bar_start_ns'])
    order = {r['row_id']:i for i,r in enumerate(result['observations'])}
    for name in ('roll','quality','cadence'): result[name].sort(key=lambda r:order[r['row_id']])
    result['missingness'].sort(key=lambda r:(r['interval_start_ns'],r['interval_end_ns']))
    require(all(r in result['observations'] for r in original['observations']), 'Accepted native values changed')
    require(len(result['observations'])==len(original['observations'])+len(inputs), 'Recovery row count differs')
    return dict(tables=result, coverage=revised_coverage, evidence=evidence, last_observation=last)


def verify_recovered_timing(row, quality, evidence):
    """Reader check after the enclosing release/certificate has been authenticated."""
    require(evidence.get('id')==sha256_json({k:v for k,v in evidence.items() if k!='id'}), 'Recovery identity changed')
    require(evidence['rule']==RULE and evidence['row_id']==row['row_id']
        and evidence['market']==row['market'] and evidence['start_ns']==row['bar_start_ns']
        and evidence['source_row_sha256']==row['source_row_sha256']
        and evidence['definition_row_sha256']==row['definition_row_sha256']
        and evidence['definition_received_at_ns']==row['definition_received_at_ns']
        and row['bar_start_ns'] < row['definition_received_at_ns'] <= evidence['first_reference_start_ns'] < row['bar_end_ns']
        and row['bar_end_ns']==row['bar_start_ns']+DAY
        and row['available_at_ns']==row['decision_eligible_at_ns']==evidence['available_at_ns']==row['bar_end_ns']+LAG
        and row['available_at_ns'] < CUTOFF and row['bar_start_ns']%DAY==0
        and evidence['reference_row_ids'] and len(set(evidence['reference_row_ids']))==len(evidence['reference_row_ids'])
        and evidence['coverage']=='OBSERVED_NATIVE_HOURS_ONLY'
        and 'IDENTITY_RULE_'+RULE in quality['quality_flags']
        and quality['row_id']==quality['row_identity_sha256']==row['row_id'], 'Recovered daily timing or lineage differs')
