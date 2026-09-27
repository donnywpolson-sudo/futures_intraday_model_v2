"""Inactive assumed-cost successor. In-memory inputs, never an access gateway."""
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace

import numpy as np

from . import rolling_hour_research as old
from . import profit_aligned_research as profit
from .canonical import sha256_json
from .errors import IntegrityError
from .fixed_hour_ridge_audit_successor import validate_fixed_hour_rows

MARKETS = tuple('ES NQ RTY YM CL NG RB HO GC SI HG PL SR3 SR1 ZQ TN ZT ZF ZN ZB UB 6A 6B 6C 6E 6J 6M 6N 6S ZC ZS ZL ZM ZW KE LE HE GF BTC ETH PA'.split())
SCENARIOS = ('base', 'adverse', 'severe')
AUTHORITY = old.AUTHORITY


def cost_policy():
    return profit.bind({'schema_version': 'assumed_cost_policy/1',
        'claim': 'UNCALIBRATED_ASSUMPTIONS_NOT_QUOTES_OR_UPPER_BOUNDS',
        'fee_rule': 'MAX_10_USD_AND_VERIFIED_APPLICABLE_ROUND_TRIP_FEE',
        'markets': {m: {'assumed_round_trip_fee_usd': '10',
            'spread_ticks': dict(zip(SCENARIOS, (1, 2, 4))),
            'additional_ticks': dict(zip(SCENARIOS, (2, 4, 8) if m in ('ES', 'NQ') else (4, 8, 16))),
            'fee_intervals': [], 'scope': 'CURRENT_RESEARCH' if m in old.MARKETS else 'COMPATIBILITY_ONLY'} for m in MARKETS},
        'authority': AUTHORITY}, 'policy_id')


def validate_policy(policy):
    profit.identity(policy, 'policy_id')
    reference = cost_policy()
    if set(policy) != set(reference) or any(policy[k] != reference[k] for k in reference if k not in ('markets', 'policy_id')):
        raise IntegrityError('assumed cost contract changed')
    if set(policy['markets']) != set(MARKETS):
        raise IntegrityError('exact 41-market policy required')
    for market, record in policy['markets'].items():
        expected = reference['markets'][market]
        if set(record) != set(expected) or any(record[k] != expected[k] for k in expected if k != 'fee_intervals'):
            raise IntegrityError('undeclared numerical assumptions')
        last = ''
        for interval in record['fee_intervals']:
            if set(interval) != {'start', 'end', 'round_trip_fee_usd', 'evidence_sha256'}:
                raise IntegrityError('exact fee interval fields required')
            from datetime import date
            date.fromisoformat(interval['start']); date.fromisoformat(interval['end'])
            if not last <= interval['start'] < interval['end']:
                raise IntegrityError('overlapping or unordered fee intervals')
            value = Decimal(interval['round_trip_fee_usd'])
            if not value.is_finite() or value < 0:
                raise IntegrityError('invalid documented fee')
            profit.digest(interval['evidence_sha256'])
            last = interval['end']


def cost(row, policy, scenario):
    if row.get('market') not in MARKETS or scenario not in SCENARIOS:
        raise IntegrityError('unknown market or scenario')
    units = row.get('tick_value_nano_usd')
    if type(units) is not int or units <= 0:
        raise IntegrityError('positive exact dollar tick units required')
    record = policy['markets'][row['market']]
    fee = Decimal(record['assumed_round_trip_fee_usd'])
    for interval in record['fee_intervals']:
        if interval['start'] <= row['trade_date'] < interval['end']:
            fee = max(fee, Decimal(interval['round_trip_fee_usd']))
    ticks = record['spread_ticks'][scenario] + record['additional_ticks'][scenario]
    return fee + Decimal(ticks) * Decimal(units) / Decimal(10**9)


def design():
    return profit.bind({'schema_version': 'rolling_hour_assumed_cost/1',
        'predecessor_mechanism_id': old.mechanism_id(), 'cost_policy_id': cost_policy()['policy_id'],
        'compatibility_markets': list(MARKETS), 'research_markets': list(old.MARKETS),
        'selection_period': ['2018-01-01', '2023-01-01'], 'candidates': list(old.candidate_definitions()),
        'labels': old.design()['phase_4'], 'features': old.design()['phase_5'],
        'forecast_question_and_metrics': old.design()['phase_6']['diagnostics']['forecast_comparison'],
        'folds_and_policy': {k: old.design()['phase_6'][k] for k in ('position', 'pending_entry',
            'forecast_while_held', 'exit', 'session_risk', 'ties', 'outer_folds', 'inner_folds',
            'audit_dates', 'embargo_dates', 'purge', 'refit', 'baselines', 'calendar')},
        'training': old.design()['training'], 'selection': 'MEAN_DAILY_ADVERSE_ASSUMED_NET_USD',
        'evaluation': 'PER_MARKET_EQUAL_DATE_MSE_ZERO_CHANGE_AND_ELIGIBLE_TRAINING_MEAN',
        'final_rule': 'LAST_OUTER_FOLD_INNER_WINNER_REFIT_ALL_SELECTION_ROWS',
        'edge': {'prior_complete_sessions': 20, 'selection_effect': 'NONE'},
        'claims': 'DESCRIPTIVE_CONDITIONAL_COSTS_ONLY', 'authority': AUTHORITY}, 'mechanism_id')


@dataclass(frozen=True)
class AssumedDataset:
    terminal_data: object
    opportunities: tuple
    batches: tuple
    declared_sessions: tuple
    policy: dict
    source_contract_id: str
    source_release_id: str
    calendar: object = None

    def __post_init__(self):
        validate_policy(self.policy)
        if self.policy != cost_policy():
            raise IntegrityError('different fee evidence requires a bound policy successor before fitting')
        dates = profit.ordered_dates([s.trade_date.isoformat() for s in self.declared_sessions])
        if not dates or any(not '2018-01-01' <= d < '2023-01-01' for d in dates):
            raise IntegrityError('current research remains 2018-2022')
        if [b['trade_date'] for b in self.batches] != list(dates):
            raise IntegrityError('complete calendar batch alignment required')
        d = self.terminal_data
        records = []
        for batch, session in zip(self.batches, self.declared_sessions, strict=True):
            profit.identity(batch, 'session_id')
            expected = old.opportunity_grid(session, calendar=self.calendar,
                source_contract_id=self.source_contract_id, source_release_id=self.source_release_id)
            if (batch['market'] != d.market or session.market != d.market
                    or [r['schedule'] for r in batch['records']] != expected
                    or batch['schedule_ids'] != [r['opportunity_id'] for r in expected]):
                raise IntegrityError('complete declared schedule required')
            records.extend(batch['records'])
        if d.market not in old.MARKETS or len(set(d.sample_ids)) != len(d.sample_ids) or not d.sample_ids:
            raise IntegrityError('current research dataset identity invalid')
        for key, shape in (('features', (len(d.sample_ids), len(old.terminal.FEATURE_NAMES))), ('terminal_ticks', (len(d.sample_ids),))):
            a = old.finite_float64(getattr(d, key), name=key, ndim=len(shape)).copy()
            if a.shape != shape:
                raise IntegrityError('unaligned fitting arrays')
            a.flags.writeable = False
            setattr(d, key, a)
        if len(self.opportunities) != len(d.sample_ids):
            raise IntegrityError('unaligned opportunity count')
        previous = -1
        for i, row in enumerate(self.opportunities):
            profit.validate_opportunity(row)
            record = records[i]
            f, o = record['feature'], record['outcome']
            profit.identity(f, 'feature_id'); profit.identity(o, 'outcome_id')
            if (record['execution'] != row or record['status'] != 'MATURED'
                    or any(row[k] != record['schedule'][k] for k in record['schedule'])
                    or row['feature_id'] != f['feature_id'] or row['outcome_id'] != o['outcome_id']
                    or row['feature_available_at_ns'] != f['available_at_ns']
                    or row['target_available_at_ns'] != o['target_available_at_ns']
                    or any(row[k] != f[k] or f[k] != o[k] for k in ('tick_nano', 'tick_value_nano_usd'))
                    or d.features[i].tolist() != [f['feature_values'][n] for n in old.terminal.FEATURE_NAMES]
                    or d.terminal_ticks[i] != o['fit_target']):
                raise IntegrityError('feature outcome execution separation differs')
            if (row['opportunity_id'] != d.sample_ids[i] or row['market'] != d.market
                    or row['trade_date'] != d.trade_dates[i] or row['trade_date'] not in dates
                    or row['decision_ns'] <= previous or row['feature_available_at_ns'] > row['decision_ns']
                    or row['target_available_at_ns'] < row['exit_ns']
                    or (row['exit_open_nano'] - row['entry_open_nano']) / row['tick_nano'] != d.terminal_ticks[i]
                    or any('quote' in k for k in row)):
                raise IntegrityError('price feature target clock alignment differs')
            previous = row['decision_ns']
        object.__setattr__(self, 'calendar_dates', dates)
        object.__setattr__(self, '_evidence_id', self.evidence_id())

    def evidence_id(self):
        d = self.terminal_data
        return sha256_json([d.market, self.batches, self.opportunities, self.policy, d.features.tolist(),
            d.terminal_ticks.tolist(), d.sample_ids, d.trade_dates, self.source_contract_id, self.source_release_id])

    def verify(self):
        if self.calendar is not None:
            self.calendar.verify()
        if self._evidence_id != self.evidence_id():
            raise IntegrityError('assumed dataset mutated')


def assemble_dataset(sessions, rows_by_date, *, policy, as_of_ns, calendar=None,
                     source_contract_id=old.SYNTHETIC_SOURCE_CONTRACT_ID,
                     source_release_id=old.SYNTHETIC_SOURCE_RELEASE_ID, bindings=None):
    """Structural bridge only; certified gateway owns every historical read."""
    validate_policy(policy)
    sessions = tuple(sessions)
    dates = profit.ordered_dates([s.trade_date.isoformat() for s in sessions])
    if not dates or set(rows_by_date) != set(dates) or len({s.market for s in sessions}) != 1:
        raise IntegrityError('one market and exact calendar date keys required')
    synthetic = (source_contract_id, source_release_id) == (old.SYNTHETIC_SOURCE_CONTRACT_ID, old.SYNTHETIC_SOURCE_RELEASE_ID)
    if not synthetic:
        if source_contract_id == old.SYNTHETIC_SOURCE_CONTRACT_ID or source_release_id == old.SYNTHETIC_SOURCE_RELEASE_ID:
            raise IntegrityError('mixed synthetic source identity')
        if not isinstance(calendar, old.VerifiedExchangeCalendar) or bindings is None or set(bindings) != set(dates):
            raise IntegrityError('verified calendar and exact source bindings required')
        calendar.verify()
        expected = sorted(s.trade_date.isoformat() for (market, _), s in calendar.sessions.items()
                          if market == sessions[0].market and dates[0] <= s.trade_date.isoformat() <= dates[-1])
        if list(dates) != expected:
            raise IntegrityError('omitted calendar date')
    batches, execution, features, targets = [], [], [], []
    for session in sessions:
        day = session.trade_date.isoformat()
        rows = rows_by_date[day]
        minutes = [t for interval in session.intervals if interval.state == 'OPEN'
                   for t in range(int(interval.starts_at_utc.timestamp()) * 10**9,
                                  int(interval.ends_at_utc.timestamp()) * 10**9, old.MINUTE)]
        if [r.get('bar_start_ns') for r in rows] != minutes:
            raise IntegrityError('complete open-interval price coverage required')
        contract = rows[0]['actual_contract'] if rows else None
        validate_fixed_hour_rows(rows, expected_market=session.market, expected_trade_date=day,
            expected_contract=contract, expected_source_contract_id=source_contract_id,
            expected_source_release_id=source_release_id)
        if not synthetic:
            b = bindings[day]
            profit.identity(b, 'binding_id')
            for key in ('calendar_evidence_id', 'contract_selection_evidence_id', 'source_certificate_id'):
                profit.digest(b.get(key))
            if (b.get('source_contract_id') != source_contract_id or b.get('source_release_id') != source_release_id
                    or b.get('calendar_release_id') != calendar.receipt.release_id or b.get('market') != session.market
                    or b.get('trade_date') != day or (rows and b.get('actual_contract') != contract)
                    or b.get('point_in_time_evidence') != 'CERTIFIED_ORIGINAL_PUBLICATION'):
                raise IntegrityError('source binding scope or publication evidence differs')
            for key in ('calendar_known_at_ns', 'contract_selection_known_at_ns'):
                at = b.get(key)
                if type(at) is not int or at < 0 or (minutes and at > minutes[0]):
                    raise IntegrityError('calendar or contract selection future known')
            allowed = b.get('source_file_sha256s')
            if not isinstance(allowed, list) or not allowed:
                raise IntegrityError('exact certified files required')
            for value in allowed:
                profit.digest(value)
            if any(row['source_file_sha256'] not in allowed for row in rows):
                raise IntegrityError('uncertified price file')
        batch = old.assemble_session(session, rows, as_of_ns=as_of_ns, quotes={}, calendar=calendar,
            source_contract_id=source_contract_id, source_release_id=source_release_id)
        indexed = {r['bar_start_ns']: r for r in rows}
        for record in batch['records']:
            if record['status'] != 'MATURED':
                raise IntegrityError('incomplete feature or label cannot be removed')
            row = {k: v for k, v in record['execution'].items() if 'quote' not in k}
            row.update(entry_open_nano=indexed[row['entry_ns']]['open_nano'],
                       exit_open_nano=indexed[row['exit_ns']]['open_nano'])
            record['execution'] = row
            execution.append(row)
            features.append([record['feature']['feature_values'][n] for n in old.terminal.FEATURE_NAMES])
            targets.append(record['outcome']['fit_target'])
        batch.pop('session_id')
        batch['source_binding'] = None if synthetic else bindings[day]
        batches.append(profit.bind(batch, 'session_id'))
    d = SimpleNamespace(market=sessions[0].market, sample_ids=tuple(r['opportunity_id'] for r in execution),
        trade_dates=tuple(r['trade_date'] for r in execution), features=np.array(features), terminal_ticks=np.array(targets))
    return AssumedDataset(d, tuple(execution), tuple(batches), sessions, policy,
                          source_contract_id, source_release_id, calendar)


def score_predictions(predictions, rows, policy, dates, *, scenario='adverse', forced_directions=False):
    validate_policy(policy)
    p = old.finite_float64(np.asarray(predictions), name='forecasts', ndim=1)
    dates = profit.ordered_dates(dates)
    if not dates or len(p) != len(rows) or (forced_directions and not np.isin(p, [-1, 0, 1]).all()):
        raise IntegrityError('unaligned forecasts or baseline directions')
    daily = {d: Decimal(0) for d in dates}
    counts = Counter({d: 0 for d in dates})
    trades, occupied, previous, market = [], -1, -1, rows[0]['market'] if rows else None
    for prediction, row in zip(p, rows, strict=True):
        if (row['market'] != market or row['trade_date'] not in daily or row['decision_ns'] <= previous
                or type(row['tick_nano']) is not int or row['tick_nano'] <= 0
                or row['entry_ns'] != (row['decision_ns'] // old.MINUTE + 1) * old.MINUTE
                or row['exit_ns'] != row['entry_ns'] + 60 * old.MINUTE):
            raise IntegrityError('chronological one-market complete replay required')
        previous = row['decision_ns']
        charge = cost(row, policy, scenario)
        value = Decimal(row['tick_value_nano_usd']) / Decimal(10**9)
        direction = int(np.sign(prediction)) if forced_directions or abs(prediction) > float(charge / value) else 0
        reason = 'BELOW_COST' if not direction else 'ASSUMED_EXECUTION'
        if row['decision_ns'] < occupied:
            direction, reason = 0, 'POSITION_HELD_FORECAST_ONLY'
        gross, fee, net = Decimal(0), Decimal(0), Decimal(0)
        if direction:
            occupied = row['exit_ns']
            gross = Decimal(direction * (row['exit_open_nano'] - row['entry_open_nano'])) / Decimal(row['tick_nano']) * value
            fee, net = charge, gross - charge
            counts[row['trade_date']] += 1
        daily[row['trade_date']] += net
        trades.append({'opportunity_id': row['opportunity_id'], 'trade_date': row['trade_date'],
            'direction': direction, 'status': reason, 'gross_usd': str(gross), 'charge_usd': str(fee),
            'net_usd': str(net), 'tick_value_nano_usd': row['tick_value_nano_usd']})
    total = sum(daily.values())
    count = sum(counts.values())
    return {'scenario': scenario, 'trades': trades, 'daily_usd': {d: str(v) for d, v in daily.items()},
        'daily_trade_counts': dict(counts), 'executed_trades': count, 'mean_daily_usd': float(total / len(dates)),
        'total_net_usd': str(total), 'additional_round_trip_break_even_usd': str(total / count) if count else None,
        'claim': 'CONDITIONAL_ASSUMED_COSTS_ONLY'}


def fit_model(dataset, indices, candidate, *, fit_kind='inner'):
    fitted = old.fit_model(dataset, indices, candidate, fit_kind=fit_kind)
    return profit.bind({'mechanism_id': design()['mechanism_id'], 'fitting_kernel': fitted,
        'evidence_id': dataset._evidence_id}, 'model_id')


def predict(model, features):
    profit.identity(model, 'model_id')
    if model['mechanism_id'] != design()['mechanism_id']:
        raise IntegrityError('wrong assumed-cost model identity')
    return old.predict(model['fitting_kernel'], features)


def run_market_wfa(dataset):
    dataset.verify()
    d, outputs = dataset.terminal_data, []
    for number, split in enumerate(old.fold_schedule(dataset)):
        scores = []
        for candidate in old.candidate_definitions():
            daily, counts, manifests = [], [], []
            try:
                for fold in split.inner_folds:
                    f, a = fold.fit_indices, fold.audit_indices
                    profit.check_split_availability(dataset, f, a)
                    model = fit_model(dataset, f, candidate)
                    predictions = predict(model, d.features[a])
                    dates = dataset.calendar_dates[fold.validation_window.start:fold.validation_window.stop]
                    replay = score_predictions(predictions, [dataset.opportunities[i] for i in a], dataset.policy, dates)
                    daily.extend(float(replay['daily_usd'][day]) for day in dates)
                    counts.append(len(model['fitting_kernel']['kernel_model']['columns']))
                    manifests.append({'audit_dates': list(dates), 'fit_ids': [d.sample_ids[i] for i in f],
                        'audit_ids': [d.sample_ids[i] for i in a], 'predictions': predictions.tolist(),
                        'daily_adverse_usd': replay['daily_usd']})
                scores.append({**candidate, 'status': 'VALID', 'score': float(np.mean(daily)),
                    'retained_count': float(np.mean(counts)), 'inner_manifests': manifests})
            except old.comparison.LeafSupportError as exc:
                scores.append({**candidate, 'status': 'INVALID_LEAF_SUPPORT', 'reason': str(exc),
                    'completed_inner_manifests': manifests, 'completed_daily_scores': daily})
        try:
            selected = old.comparison.choose_candidate(scores)
        except IntegrityError as exc:
            raise old.SelectedModelFailure(str(exc), {'completed_outer_folds': outputs,
                'failed_outer_fold': number, 'candidate_scores': scores}) from exc
        chosen, unavailable = {'selected': selected}, {}
        for pack in old.PACKS:
            valid = [s for s in scores if s['pack'] == pack and s['status'] == 'VALID']
            if valid:
                chosen[pack] = old.comparison.choose_candidate(valid)
            else:
                unavailable[pack] = {'status': 'NO_VALID_INNER_DIAGNOSTIC'}
        f, a = split.fit_indices, split.audit_indices
        profit.check_split_availability(dataset, f, a)
        streams = dict(unavailable)
        for name, candidate in chosen.items():
            try:
                model = fit_model(dataset, f, candidate, fit_kind='outer')
                streams[name] = {'status': 'FITTED', 'model': model, 'predictions': predict(model, d.features[a]).tolist()}
            except old.comparison.LeafSupportError as exc:
                if name == 'selected':
                    raise old.SelectedModelFailure(str(exc), {'completed_outer_folds': outputs,
                        'failed_outer_fold': number, 'candidate_scores': scores, 'selected_candidate': candidate}) from exc
                streams[name] = {'status': 'INVALID_OUTER_REFIT', 'reason': str(exc)}
        outputs.append({'outer_fold': number, 'fit_ids': [d.sample_ids[i] for i in f],
            'audit_indices': a.tolist(), 'audit_ids': [d.sample_ids[i] for i in a],
            'audit_dates': list(dataset.calendar_dates[split.test_window.start:split.test_window.stop]),
            'candidate_scores': scores, 'streams': streams,
            'forecast_benchmarks': {'zero_change': 0., 'training_mean': float(np.mean(d.terminal_ticks[f]))}})
    return profit.bind({'mechanism_id': design()['mechanism_id'], 'evidence_id': dataset._evidence_id,
        'market': d.market, 'outer_folds': outputs, 'authority': AUTHORITY}, 'run_id')


def final_refit(dataset, run):
    dataset.verify()
    profit.identity(run, 'run_id')
    if run['mechanism_id'] != design()['mechanism_id'] or run['evidence_id'] != dataset._evidence_id or len(run['outer_folds']) != 8:
        raise IntegrityError('complete matching WFA required')
    chosen = old.comparison.choose_candidate(run['outer_folds'][-1]['candidate_scores'])
    selected = run['outer_folds'][-1]['streams']['selected']['model']['fitting_kernel']['kernel_model']['candidate']
    if selected['candidate_id'] != chosen['candidate_id'] or run['market'] != dataset.terminal_data.market:
        raise IntegrityError('final candidate differs from predetermined last inner winner')
    model = fit_model(dataset, np.arange(len(dataset.opportunities)), chosen, fit_kind='final')
    return profit.bind({'model': model, 'run_id': run['run_id'], 'candidate_rule': design()['final_rule'],
        'state': 'DESCRIPTIVE_NOT_QUALIFIED', 'authority': AUTHORITY}, 'final_id')


def evaluate_wfa(dataset, run):
    dataset.verify()
    profit.identity(run, 'run_id')
    if run['evidence_id'] != dataset._evidence_id or run['mechanism_id'] != design()['mechanism_id']:
        raise IntegrityError('evaluation identity mismatch')
    d = dataset.terminal_data
    indices = [i for f in run['outer_folds'] for i in f['audit_indices']]
    rows = [dataset.opportunities[i] for i in indices]
    dates = [day for f in run['outer_folds'] for day in f['audit_dates']]
    reports = {}
    for name in ('selected', *old.PACKS):
        failed = [f['outer_fold'] for f in run['outer_folds'] if f['streams'][name]['status'] != 'FITTED']
        reports[name] = {'unavailable_folds': failed} if failed else {
            s: score_predictions([p for f in run['outer_folds'] for p in f['streams'][name]['predictions']],
                rows, dataset.policy, dates, scenario=s) for s in SCENARIOS}
    signs = {name: [] for name in old.BASELINES}
    for f in run['outer_folds']:
        a = f['audit_indices']
        previous = np.sign(d.features[a, old.terminal.FEATURE_NAMES.index('close_change_ticks_1')])
        values = {'flat_no_trade': np.zeros(len(a)), 'always_long': np.ones(len(a)),
            'always_short': -np.ones(len(a)), 'previous_bar_sign_momentum': previous,
            'previous_bar_sign_reversal': -previous,
            'fold_local_unconditional_direction': np.full(len(a), np.sign(f['forecast_benchmarks']['training_mean']))}
        for name in signs:
            signs[name].extend(values[name].tolist())
    reports.update({name: {s: score_predictions(p, rows, dataset.policy, dates, scenario=s,
        forced_directions=True) for s in SCENARIOS} for name, p in signs.items()})
    forecasts = {'model': [p for f in run['outer_folds'] for p in f['streams']['selected']['predictions']],
        **{name: [f['forecast_benchmarks'][name] for f in run['outer_folds'] for _ in f['audit_indices']]
           for name in ('zero_change', 'training_mean')}}
    try:
        accuracy = forecast_accuracy(forecasts, d.terminal_ticks[indices], [row['trade_date'] for row in rows])
    except (IntegrityError, old.ResearchContractError, FloatingPointError, ValueError) as exc:
        accuracy = {'state': 'UNAVAILABLE', 'reason': str(exc), 'selected_policy_effect': 'NONE'}
    selected = reports['selected']['adverse']
    fixed = {s: str(sum(Decimal(t['gross_usd']) - cost(r, dataset.policy, s)
        for t, r in zip(selected['trades'], rows, strict=True) if t['direction'])) for s in SCENARIOS}
    return profit.bind({'mechanism_id': design()['mechanism_id'], 'run_id': run['run_id'],
        'reports': reports, 'accuracy': accuracy, 'forecast_values': forecasts,
        'targets': d.terminal_ticks[indices].tolist(), 'audit_ids': [r['opportunity_id'] for r in rows],
        'fixed_adverse_trade_list_net_usd': fixed,
        'fixed_list_claim': 'COUNTERFACTUAL_REPRICING_NOT_FULL_POLICY_REPLAY',
        'qualification': old.qualification_status(), 'authority': AUTHORITY,
        'state': 'DESCRIPTIVE_CONDITIONAL_COSTS_NOT_QUALIFICATION'}, 'result_id')


def forecast_accuracy(forecasts, targets, row_dates):
    accuracy = {}
    for name, p in forecasts.items():
        squared = old.finite_float64((np.asarray(p) - targets)**2, name='squared errors', ndim=1)
        daily = old.terminal.daily_means(squared, row_dates)
        accuracy[name] = {'date_balanced_mse_ticks_squared': float(np.mean(daily)),
            'daily_mse': dict(zip(sorted(set(row_dates)), daily.tolist(), strict=True))}
    for name in ('zero_change', 'training_mean'):
        baseline = accuracy[name]['date_balanced_mse_ticks_squared']
        model_mse = accuracy['model']['date_balanced_mse_ticks_squared']
        accuracy[name].update(mse_improvement_ticks_squared=baseline-model_mse,
            skill=1-model_mse/baseline if baseline else None)
    return accuracy
