"""Inactive predictive successor. Pure in-memory mechanics, no historical reader.

The numerical V7 helpers are reused without inheriting trading selection.
Real-source admission remains with the certified gateway, never a caller flag.
"""
from collections import Counter
from dataclasses import dataclass
from types import SimpleNamespace
from functools import lru_cache

import numpy as np

from . import rolling_hour_research as old
from . import rolling_hour_assumed_cost as v8
from .canonical import sha256_json
from .errors import IntegrityError


def design():
    return old.profit.bind({
        'schema_version': 'rolling_hour_predictive/1',
        'predecessor_mechanism_id': v8.design()['mechanism_id'],
        'research_markets': list(old.MARKETS), 'selection_period': ['2018-01-01', '2023-01-01'],
        'question': 'NEXT_MINUTE_OPEN_TO_OPEN_60_MINUTES_LATER_CHANGE_TICKS',
        'features': old.design()['phase_5'], 'training': old.design()['training'],
        'candidates': list(old.candidate_definitions()), 'new_configuration_charges': 60,
        'selection': 'MINIMUM_MEAN_OF_WITHIN_DATE_MSE_ACROSS_INNER_VALIDATION_DATES',
        'ties': 'EXACT_SCORE_THEN_CANDIDATE_ID_ASCENDING',
        'contract': 'OBSERVED_AT_DECISION;SAME_CONTRACT_61_BAR_HISTORY_AND_TARGET_ENDPOINTS',
        'label': 'VALID_ENDPOINTS_SUFFICE;NO_FILL;FUTURE_MISSINGNESS_NEVER_FILTERS_FORECASTS',
        'timing': {'decision_delay_ns': old.DELAY, 'label_maturity': 'BOTH_ENDPOINT_BARS_AVAILABLE',
            'calendar': 'ORIGINAL_SCHEDULE_KNOWN_BEFORE_DECISION'},
        'folds': {'outer': 8, 'inner': 4, 'audit_dates': 63, 'embargo_dates': 1,
            'clock': 'COMPLETE_DECLARED_CALENDAR_INCLUDING_EMPTY_DATES'},
        'benchmarks': ['ZERO_CHANGE', 'ROW_MEAN_OF_ELIGIBLE_OUTER_TRAINING_TARGETS'],
        'missing_outcomes': 'RETAIN_FORECAST;SCORE_OBSERVED_ONLY;REPORT_COUNTS_AND_EMPTY_DATES',
        'final_rule': 'LAST_OUTER_INNER_WINNER_REFIT_ALL_MATURE_OBSERVABLE_DEVELOPMENT_LABELS',
        'claims': 'DESCRIPTIVE_CONDITIONAL_OBSERVABLE_OUTCOMES;NO_ALPHA_OR_PROFITABILITY_QUALIFICATION',
        'authority': old.AUTHORITY}, 'mechanism_id')


@lru_cache(maxsize=1)
def mechanism_id():
    return design()['mechanism_id']


def build_outcome(rows, scheduled, *, expected_contract, expected_tick, as_of_ns):
    old.validate_schedule(scheduled)
    old.profit.integer(as_of_ns, minimum=1)
    base = {'mechanism_id': mechanism_id(), 'opportunity_id': scheduled['opportunity_id'],
        'fit_target': None, 'target_available_at_ns': None, 'diagnostics': None}
    def result(status, **extra):
        return old.profit.bind({**base, 'status': status, **extra}, 'outcome_id')
    if as_of_ns < scheduled['exit_ns'] + old.MINUTE + old.DELAY:
        return result('UNMATURED')
    endpoints = []
    for t in (scheduled['entry_ns'], scheduled['exit_ns']):
        found = [r for r in rows if r['bar_start_ns'] == t]
        if len(found) > 1:
            raise IntegrityError('duplicate outcome endpoint')
        if not found:
            return result('MISSING_ENDPOINT')
        endpoints.append(found[0])
    if any(r['decision_eligible_at_ns'] > as_of_ns for r in endpoints):
        return result('UNMATURED')
    if any(r['actual_contract'] != expected_contract for r in endpoints):
        return result('CONTRACT_MISMATCH')
    try:
        old.validate_fixed_hour_rows(endpoints, expected_contract=expected_contract,
            **{'expected_' + k: scheduled[k] for k in
               ('market', 'trade_date', 'source_contract_id', 'source_release_id')})
    except IntegrityError as exc:
        return result('INVALID_ENDPOINT', reason=str(exc))
    if any(r['min_price_increment_nano'] != expected_tick for r in endpoints):
        return result('INVALID_ENDPOINT', reason='endpoint tick differs from forecast')
    if endpoints[0]['multiplier_nano'] != endpoints[1]['multiplier_nano']:
        return result('INVALID_ENDPOINT', reason='endpoint contract units differ')
    target = (endpoints[1]['open_nano'] - endpoints[0]['open_nano']) / expected_tick
    if not np.isfinite(target):
        raise IntegrityError('nonfinite target')
    return result('MATURED', fit_target=target,
        target_available_at_ns=max(r['decision_eligible_at_ns'] for r in endpoints),
        source_row_ids=[r['row_id'] for r in endpoints],
        source_row_sha256s=[r['source_row_sha256'] for r in endpoints])


@dataclass
class PredictiveDataset:
    """All opportunities remain in batches; only observed labels enter fit arrays."""
    batches: tuple
    calendar_dates: tuple
    terminal_data: object
    opportunities: tuple
    forecast_records: tuple

    def evidence_id(self):
        d = self.terminal_data
        return sha256_json([self.batches, self.calendar_dates, d.market, d.sample_ids,
            d.trade_dates, d.features.tolist(), d.terminal_ticks.tolist(),
            self.opportunities, self.forecast_records])

    def seal(self):
        self.terminal_data.features.flags.writeable = False
        self.terminal_data.terminal_ticks.flags.writeable = False
        self._evidence_id = self.evidence_id()
        return self

    def verify(self):
        if self.evidence_id() != self._evidence_id:
            raise IntegrityError('predictive evidence mutated')


def assemble_dataset(sessions, rows_by_date, *, as_of_ns):
    """Synthetic adapter acceptance only, until real-source admission is certified.

    Deliberately no real-source flag or certificate-shaped dictionary bypass.
    """
    if not sessions:
        raise IntegrityError('complete declared calendar required')
    dates = old.profit.ordered_dates([s.trade_date.isoformat() for s in sessions])
    if set(rows_by_date) != set(dates) or any(not '2018-01-01' <= d < '2023-01-01' for d in dates):
        raise IntegrityError('complete development calendar required')
    market = sessions[0].market
    if market not in old.MARKETS or any(s.market != market for s in sessions):
        raise IntegrityError('one declared research market required')
    batches, forecasts, observed = [], [], []
    for session, day in zip(sessions, dates, strict=True):
        rows = rows_by_date[day]
        if any(r.get('source_contract_id') != old.SYNTHETIC_SOURCE_CONTRACT_ID
               or r.get('source_release_id') != old.SYNTHETIC_SOURCE_RELEASE_ID for r in rows):
            raise IntegrityError('historical source admission not certified')
        indexed = {r['bar_start_ns']: r for r in rows}
        if len(indexed) != len(rows):
            raise IntegrityError('duplicate source bar')
        records = []
        for scheduled in old.opportunity_grid(session):
            if scheduled['decision_ns'] > as_of_ns:
                records.append({'schedule': scheduled, 'feature': None, 'outcome': None, 'status': 'DECISION_NOT_REACHED'})
                continue
            end = scheduled['decision_ns'] - old.DELAY
            history = [indexed[t] for t in range(end - 61 * old.MINUTE, end, old.MINUTE) if t in indexed]
            record = {'schedule': scheduled, 'feature': None, 'outcome': None, 'status': 'INVALID_FEATURE'}
            try:
                feature = old.build_feature_record(history, scheduled)
            except IntegrityError as exc:
                record['reason'] = str(exc)
            else:
                path = [indexed[t] for t in range(scheduled['entry_ns'], scheduled['exit_ns'] + old.MINUTE, old.MINUTE)
                        if t in indexed]
                outcome = build_outcome(path, scheduled, expected_contract=feature['actual_contract'],
                    expected_tick=feature['tick_nano'], as_of_ns=as_of_ns)
                record.update(feature=feature, outcome=outcome, status=outcome['status'])
                forecasts.append(record)
                if outcome['status'] == 'MATURED':
                    observed.append(record)
            records.append(record)
        batches.append({'trade_date': day, 'records': records})
    x = np.array([[r['feature']['feature_values'][n] for n in old.terminal.FEATURE_NAMES] for r in observed],
                 dtype=float).reshape((-1, len(old.terminal.FEATURE_NAMES)))
    d = SimpleNamespace(market=market, sample_ids=tuple(r['schedule']['opportunity_id'] for r in observed),
        trade_dates=tuple(r['schedule']['trade_date'] for r in observed), features=x,
        terminal_ticks=np.array([r['outcome']['fit_target'] for r in observed], dtype=float))
    opportunities = tuple({**r['schedule'], 'target_available_at_ns': r['outcome']['target_available_at_ns']}
                          for r in observed)
    return PredictiveDataset(tuple(batches), dates, d, opportunities, tuple(forecasts)).seal()


def daily_mse(predictions, targets, dates):
    if not len(dates):
        raise IntegrityError('nonempty aligned scoring rows required')
    p = old.finite_float64(np.asarray(predictions, dtype=float), name='predictions', ndim=1)
    y = old.finite_float64(np.asarray(targets, dtype=float), name='targets', ndim=1)
    if p.shape != y.shape or len(dates) != len(y) or not len(y):
        raise IntegrityError('nonempty aligned scoring rows required')
    errors = (p - y) ** 2
    if not np.isfinite(errors).all():
        raise IntegrityError('nonfinite squared error')
    return {day: float(np.mean(errors[np.array(dates) == day])) for day in sorted(set(dates))}


def choose_candidate(scores):
    valid = [s for s in scores if s['status'] == 'VALID' and np.isfinite(s['score'])]
    if not valid:
        raise IntegrityError('no candidate completed all inner folds')
    return min(valid, key=lambda s: (s['score'], s['candidate_id']))


def fit_model(dataset, indices, candidate, *, fit_kind='inner'):
    return old.profit.bind({'mechanism_id': mechanism_id(), 'evidence_id': dataset._evidence_id,
        'fitting_kernel': old.fit_model(dataset, indices, candidate, fit_kind=fit_kind)}, 'model_id')


def predict(model, features):
    old.profit.identity(model, 'model_id')
    if model['mechanism_id'] != mechanism_id():
        raise IntegrityError('wrong predictive model identity')
    return old.predict(model['fitting_kernel'], features)


def mature_fit_indices(dataset, indices, window, *, fit_kind):
    dates = dataset.calendar_dates[window.start:window.stop]
    records = [r for r in dataset.forecast_records if r['schedule']['trade_date'] in dates]
    if not records:
        raise IntegrityError('audit window has no forecasts')
    cutoff = min(r['schedule']['decision_ns'] for r in records)
    selected = np.array([i for i in indices if dataset.opportunities[i]['target_available_at_ns'] < cutoff], dtype=int)
    old.training_support(dataset, selected, fit_kind=fit_kind)
    return selected


def run_market_wfa(dataset):
    dataset.verify()
    d, outputs = dataset.terminal_data, []
    for number, split in enumerate(old.fold_schedule(dataset)):
        scores = []
        try:
            for candidate in old.candidate_definitions():
                manifests, daily = [], {}
                try:
                    for fold in split.inner_folds:
                        f = mature_fit_indices(dataset, fold.fit_indices, fold.validation_window, fit_kind='inner')
                        a = fold.audit_indices
                        old.profit.check_split_availability(dataset, f, a)
                        model = fit_model(dataset, f, candidate)
                        predictions = predict(model, d.features[a])
                        errors = daily_mse(predictions, d.terminal_ticks[a], [d.trade_dates[i] for i in a])
                        if set(daily) & set(errors):
                            raise IntegrityError('overlapping inner validation dates')
                        daily.update(errors)
                        manifests.append({'fit_ids': [d.sample_ids[i] for i in f],
                            'audit_ids': [d.sample_ids[i] for i in a], 'predictions': predictions.tolist(),
                            'daily_mse': errors})
                    scores.append({**candidate, 'status': 'VALID', 'score': float(np.mean(list(daily.values()))),
                        'inner_manifests': manifests})
                except old.comparison.LeafSupportError as exc:
                    scores.append({**candidate, 'status': 'INVALID_LEAF_SUPPORT', 'reason': str(exc),
                        'completed_inner_manifests': manifests})
                except Exception as exc:
                    scores.append({**candidate, 'status': 'FAILED', 'reason': str(exc),
                        'completed_inner_manifests': manifests})
                    raise
            selected = choose_candidate(scores)
            f = mature_fit_indices(dataset, split.fit_indices, split.test_window, fit_kind='outer')
            window_dates = dataset.calendar_dates[split.test_window.start:split.test_window.stop]
            records = [r for r in dataset.forecast_records if r['schedule']['trade_date'] in window_dates]
            if not records or max(dataset.opportunities[i]['target_available_at_ns'] for i in f) >= min(
                    r['schedule']['decision_ns'] for r in records):
                raise IntegrityError('fit labels not known before all outer forecasts')
            model = fit_model(dataset, f, selected, fit_kind='outer')
            x = np.array([[r['feature']['feature_values'][n] for n in old.terminal.FEATURE_NAMES] for r in records])
            predictions = predict(model, x)
            mean = float(np.mean(d.terminal_ticks[f]))
            observed = [i for i, r in enumerate(records) if r['status'] == 'MATURED']
            targets = [records[i]['outcome']['fit_target'] for i in observed]
            dates = [records[i]['schedule']['trade_date'] for i in observed]
            errors = {name: daily_mse(values, targets, dates) for name, values in {
                'model': predictions[observed], 'zero_change': np.zeros(len(observed)),
                'training_mean': np.full(len(observed), mean)}.items()}
            outputs.append({'outer_fold': number, 'candidate_scores': scores, 'model': model,
                'selected_candidate_id': selected['candidate_id'], 'fit_ids': [d.sample_ids[i] for i in f],
                'forecast_ids': [r['schedule']['opportunity_id'] for r in records],
                'outcome_statuses': [r['status'] for r in records],
                'predictions': predictions.tolist(), 'training_mean': mean, 'daily_mse': errors,
                'coverage': {day: dict(Counter(r['status'] for b in dataset.batches if b['trade_date'] == day
                    for r in b['records'])) for day in window_dates},
                'unscorable_dates': [day for day in window_dates if day not in errors['model']]})
        except Exception as exc:
            raise old.SelectedModelFailure(str(exc), {'completed_outer_folds': outputs,
                'failed_outer_fold': number, 'candidate_scores': scores}) from exc
    return old.profit.bind({'mechanism_id': design()['mechanism_id'], 'evidence_id': dataset._evidence_id,
        'market': d.market, 'outer_folds': outputs, 'claim': design()['claims']}, 'run_id')


def evaluate_wfa(run):
    old.profit.identity(run, 'run_id')
    if run['mechanism_id'] != mechanism_id() or len(run['outer_folds']) != 8:
        raise IntegrityError('complete predictive nested run required')
    daily = {name: {} for name in ('model', 'zero_change', 'training_mean')}
    for fold in run['outer_folds']:
        for name in daily:
            if set(daily[name]) & set(fold['daily_mse'][name]):
                raise IntegrityError('outer scoring dates overlap')
            daily[name].update(fold['daily_mse'][name])
    if any(set(values) != set(daily['model']) for values in daily.values()):
        raise IntegrityError('benchmarks do not use common dates')
    mse = {name: float(np.mean(list(values.values()))) for name, values in daily.items()}
    return {'date_balanced_mse_ticks_squared': mse,
        'skill_against': {name: 1 - mse['model'] / mse[name] if mse[name] else None
                          for name in ('zero_change', 'training_mean')},
        'scorable_dates': len(daily['model']), 'claim': design()['claims']}


def final_refit(dataset, run):
    dataset.verify()
    old.profit.identity(run, 'run_id')
    if (run['mechanism_id'] != design()['mechanism_id'] or run['evidence_id'] != dataset._evidence_id
            or run['market'] != dataset.terminal_data.market or len(run['outer_folds']) != 8):
        raise IntegrityError('complete matching nested run required')
    winner = choose_candidate(run['outer_folds'][-1]['candidate_scores'])
    if winner['candidate_id'] != run['outer_folds'][-1]['selected_candidate_id']:
        raise IntegrityError('final selection differs')
    return old.profit.bind({'model': fit_model(dataset, np.arange(len(dataset.opportunities)), winner, fit_kind='final'),
        'run_id': run['run_id'], 'state': 'DESCRIPTIVE_NOT_QUALIFIED'}, 'final_id')
