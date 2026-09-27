"""V9 source-readiness connection; source facts are distinct from clock assumptions.

No price reader, trial registration, model fit, or activation lives here.
The timestamp census is an upper bound until calendar and source quality pass.
"""
from collections import Counter
from datetime import date
from types import SimpleNamespace

import numpy as np

from . import rolling_hour_predictive as r
from .canonical import sha256_json
from .errors import IntegrityError


def source_facts_hash(bindings):
    """Exclude the certificate's own reference to avoid a circular hash dependency."""
    return sha256_json({day: {k: v for k, v in binding.items()
        if k not in ('binding_id', 'source_certificate_path', 'source_certificate_sha256')}
        for day, binding in bindings.items()})


def structural_support(columns):
    """Count exact causal windows/endpoints without reading price or volume values."""
    t = np.asarray(columns['bar_start_ns'], dtype=np.int64)
    day = np.asarray(columns['project_trade_date'])
    contract = np.asarray(columns['actual_contract'])
    available = np.asarray(columns['available_at_ns'], dtype=np.int64)
    eligible = np.asarray(columns['decision_eligible_at_ns'], dtype=np.int64)
    definition = np.asarray(columns['definition_received_at_ns'], dtype=np.int64)
    if any(a.shape != t.shape for a in (day, contract, available, eligible, definition)) or t.ndim != 1:
        raise IntegrityError('unaligned timestamp census')
    if len(t) and (np.any(np.diff(t) <= 0) or np.any(t % r.old.MINUTE)):
        raise IntegrityError('duplicate unordered or unaligned minute timestamps')
    if any(not '2018-01-01' <= str(d) < '2023-01-01' for d in set(day)):
        raise IntegrityError('source census exceeds development dates')
    if np.any(available < t + r.old.MINUTE) or np.any(eligible < available):
        raise IntegrityError('invalid source availability order')
    feature = np.zeros(len(t), dtype=bool)
    if len(t) >= 61:
        breaks = np.r_[0, ((np.diff(t) != r.old.MINUTE) | (day[1:] != day[:-1]) |
                          (contract[1:] != contract[:-1])).astype(int)].cumsum()
        same_window = breaks[60:] == breaks[:-60]
        known = np.lib.stride_tricks.sliding_window_view(np.maximum(eligible, definition), 61).max(axis=1)
        feature[60:] = same_window & (known <= t[60:] + r.old.MINUTE + r.old.DELAY)
    candidates = np.flatnonzero(feature)
    entries = np.searchsorted(t, t[candidates] + 2 * r.old.MINUTE)
    exits = np.searchsorted(t, t[candidates] + 62 * r.old.MINUTE)
    observed = np.zeros(len(candidates), dtype=bool)
    mismatch = np.zeros(len(candidates), dtype=bool)
    endpoints = (entries < len(t)) & (exits < len(t))
    positions = np.flatnonzero(endpoints)
    a, b, c = entries[positions], exits[positions], candidates[positions]
    exact = (t[a] == t[c] + 2 * r.old.MINUTE) & (t[b] == t[c] + 62 * r.old.MINUTE)
    exact &= (day[a] == day[c]) & (day[b] == day[c])
    matched = (contract[a] == contract[c]) & (contract[b] == contract[c])
    observed[positions] = exact & matched
    mismatch[positions] = exact & ~matched
    counts = Counter(str(d) for d in day[candidates])
    labels = Counter(str(d) for d in day[candidates[observed]])
    return {'rows': len(t), 'feature_window_candidates': int(feature.sum()),
        'same_contract_endpoint_candidates': int(observed.sum()),
        'different_contract_endpoints': int(mismatch.sum()),
        'missing_endpoint_candidates': int((~observed & ~mismatch).sum()),
        'contract_changes': int(np.sum(contract[1:] != contract[:-1])),
        'fixed_five_second_availability_rows': int(np.sum(available == t + r.old.MINUTE + r.old.DELAY)),
        'fixed_five_second_eligibility_rows': int(np.sum(eligible == t + r.old.MINUTE + r.old.DELAY)),
        'definition_after_bar_start_rows': int(np.sum(definition > t)),
        'candidate_dates': {str(d): {'feature_windows': counts[str(d)], 'endpoints': labels[str(d)]}
                            for d in sorted(set(day))},
        'claim': 'STRUCTURAL_UPPER_BOUND;CALENDAR_PRICE_QUALITY_AND_DELIVERY_NOT_CERTIFIED',
        'certified_training_dates': None}


def adapt_source_sessions(*, sessions, rows_by_date, calendar, source_contract_id,
                          source_release_id, bindings, as_of_ns):
    """In-memory structural bridge, called after gateway source-evidence validation.

    A binding describes certified evidence; it never grants data access.
    """
    old = r.old
    if not isinstance(calendar, old.VerifiedExchangeCalendar):
        raise IntegrityError('verified full exchange calendar required')
    calendar.verify()
    if (source_contract_id == old.SYNTHETIC_SOURCE_CONTRACT_ID or
            source_release_id == old.SYNTHETIC_SOURCE_RELEASE_ID):
        raise IntegrityError('historical bridge rejects synthetic source identities')
    for value in (source_contract_id, source_release_id):
        old.profit.digest(value)
    old.profit.integer(as_of_ns, minimum=1)
    sessions = tuple(sessions)
    dates = old.profit.ordered_dates([s.trade_date.isoformat() for s in sessions])
    if not dates or any(not '2018-01-01' <= d < '2023-01-01' for d in dates):
        raise IntegrityError('development calendar required')
    market = sessions[0].market
    if market not in old.MARKETS or any(s.market != market for s in sessions):
        raise IntegrityError('one current research market required')
    calendar.require_coverage([market], date(2018, 1, 1), date(2022, 12, 31))
    expected = sorted(s.trade_date.isoformat() for (m, _), s in calendar.sessions.items()
                      if m == market and '2018-01-01' <= s.trade_date.isoformat() < '2023-01-01')
    if list(dates) != expected or set(rows_by_date) != set(dates) or set(bindings) != set(dates):
        raise IntegrityError('complete independently declared development calendar required')
    batches, forecasts, observed = [], [], []
    for session, day in zip(sessions, dates, strict=True):
        binding = bindings[day]
        old.profit.identity(binding, 'binding_id')
        if (binding.get('mechanism_id') != r.mechanism_id() or binding.get('market') != market
                or binding.get('trade_date') != day or binding.get('source_contract_id') != source_contract_id
                or binding.get('source_release_id') != source_release_id
                or binding.get('calendar_release_id') != calendar.receipt.release_id):
            raise IntegrityError('predictive source binding scope differs')
        if binding.get('availability_basis') not in ('OBSERVED_RECEIPT_TIMES', 'VERIFIED_HISTORICALLY_APPLICABLE_PUBLICATION_RULE'):
            raise IntegrityError('fixed lag assumption is not observed or verified availability')
        if binding.get('calendar_basis') != 'CERTIFIED_ORIGINAL_PUBLICATION':
            raise IntegrityError('retrospective schedule is not prior calendar knowledge')
        for key in ('calendar_evidence_sha256', 'availability_evidence_sha256', 'source_certificate_sha256'):
            old.profit.digest(binding.get(key))
        known = binding.get('calendar_known_at_ns')
        old.profit.integer(known, minimum=0)
        files = binding.get('source_file_sha256s')
        if not isinstance(files, list) or not files:
            raise IntegrityError('certified source files required')
        for digest in files:
            old.profit.digest(digest)
        rows = rows_by_date[day]
        if any(row['source_file_sha256'] not in files or row['source_contract_id'] != source_contract_id
               or row['source_release_id'] != source_release_id for row in rows):
            raise IntegrityError('row outside certified source scope')
        indexed = {row['bar_start_ns']: row for row in rows}
        if len(indexed) != len(rows):
            raise IntegrityError('duplicate source minute')
        records = []
        for scheduled in old.opportunity_grid(session, calendar=calendar,
                source_contract_id=source_contract_id, source_release_id=source_release_id):
            if known > scheduled['decision_ns']:
                raise IntegrityError('calendar is future known')
            record = {'schedule': scheduled, 'feature': None, 'outcome': None, 'status': 'INVALID_FEATURE'}
            if scheduled['decision_ns'] > as_of_ns:
                record['status'] = 'DECISION_NOT_REACHED'
            else:
                end = scheduled['decision_ns'] - old.DELAY
                def projected(t):
                    # This is a derived calendar join, never a mutation of accepted rows.
                    row = indexed[t]
                    if not (scheduled['open_interval_start_ns'] <= row['bar_start_ns']
                            and row['bar_end_ns'] <= scheduled['open_interval_end_ns']):
                        raise IntegrityError('dependency crosses declared open interval')
                    return {**row, 'parent_row_id': row['row_id'],
                        'row_id': sha256_json([row['row_id'], calendar.receipt.release_id]),
                        'official_schedule_state': 'AUTHORITATIVE_APPLICABLE',
                        'decision_eligible_at_ns': max(row['decision_eligible_at_ns'], row['definition_received_at_ns'])}
                history = [projected(t) for t in range(end - 61 * old.MINUTE, end, old.MINUTE) if t in indexed]
                try:
                    if any(row['definition_received_at_ns'] > scheduled['decision_ns'] for row in history):
                        raise IntegrityError('instrument definition is future known')
                    feature = old.build_feature_record(history, scheduled)
                except IntegrityError as exc:
                    record['reason'] = str(exc)
                else:
                    endpoints = [projected(t) for t in (scheduled['entry_ns'], scheduled['exit_ns']) if t in indexed]
                    outcome = r.build_outcome(endpoints, scheduled, expected_contract=feature['actual_contract'],
                        expected_tick=feature['tick_nano'], as_of_ns=as_of_ns)
                    record.update(feature=feature, outcome=outcome, status=outcome['status'])
                    forecasts.append(record)
                    if outcome['status'] == 'MATURED':
                        observed.append(record)
            records.append(record)
        batches.append({'trade_date': day, 'records': records, 'source_binding': binding})
    d = SimpleNamespace(market=market, sample_ids=tuple(x['schedule']['opportunity_id'] for x in observed),
        trade_dates=tuple(x['schedule']['trade_date'] for x in observed),
        features=np.array([[x['feature']['feature_values'][n] for n in old.terminal.FEATURE_NAMES]
                           for x in observed], dtype=float).reshape((-1, len(old.terminal.FEATURE_NAMES))),
        terminal_ticks=np.array([x['outcome']['fit_target'] for x in observed], dtype=float))
    opportunities = tuple({**x['schedule'], 'target_available_at_ns': x['outcome']['target_available_at_ns']} for x in observed)
    return r.PredictiveDataset(tuple(batches), dates, d, opportunities, tuple(forecasts)).seal()


def fold_support(dataset):
    """Use the frozen clock and report support failures without fitting anything."""
    dataset.verify()
    try:
        folds = r.old.fold_schedule(dataset)
        results = []
        for number, split in enumerate(folds):
            for role, index, indices, window, kind in [
                    ('OUTER', number, split.fit_indices, split.test_window, 'outer'),
                    *(('INNER', j, f.fit_indices, f.validation_window, 'inner') for j, f in enumerate(split.inner_folds))]:
                fit = r.mature_fit_indices(dataset, indices, window, fit_kind=kind)
                results.append({'outer_fold': number, 'role': role, 'fold': index,
                    'observed_training_dates': len(set(dataset.terminal_data.trade_dates[i] for i in fit)),
                    'training_rows': len(fit), 'fit_ids_sha256': sha256_json([dataset.terminal_data.sample_ids[i] for i in fit])})
        r.old.training_support(dataset, np.arange(len(dataset.opportunities)), fit_kind='final')
        return {'status': 'PASS', 'folds': results, 'final_observed_dates': len(set(dataset.terminal_data.trade_dates))}
    except (IntegrityError, r.old.ResearchContractError) as exc:
        return {'status': 'BLOCKED', 'reason': str(exc), 'final_observed_dates': len(set(dataset.terminal_data.trade_dates))}
