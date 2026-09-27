from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

import numpy as np
import pytest

from futures_rebuild import rolling_hour_assumed_cost as r
from futures_rebuild import rolling_hour_assumed_preparation as prep
from futures_rebuild import rolling_hour_edge as edge
from futures_rebuild.errors import IntegrityError

pytestmark = pytest.mark.current


def dataset():
    s, rows, _ = prep.prior.rich_synthetic_session('ES', '2018-01-01', 0)
    return r.assemble_dataset([s], {s.trade_date.isoformat(): rows}, policy=r.cost_policy(),
        as_of_ns=rows[-1]['decision_eligible_at_ns'])


def test_exact_all_market_cost_contract_and_policy_replay():
    assert len(prep.compatibility()) == 41
    d = dataset()
    predictions = np.full(len(d.opportunities), 5.)
    base = r.score_predictions(predictions, d.opportunities, d.policy, d.calendar_dates, scenario='base')
    adverse = r.score_predictions(predictions, d.opportunities, d.policy, d.calendar_dates)
    assert base['executed_trades'] > 1 and adverse['executed_trades'] == 0
    assert any(t['status'] == 'POSITION_HELD_FORECAST_ONLY' for t in base['trades'])
    assert sum(Decimal(t['net_usd']) for t in base['trades']) == Decimal(base['total_net_usd'])
    assert all(not any('quote' in k for k in row) for row in d.opportunities)
    p = deepcopy(d.policy)
    p['markets']['ES']['fee_intervals'] = [{'start': '2018-01-01', 'end': '2019-01-01',
        'round_trip_fee_usd': '12', 'evidence_sha256': 'c'*64}]
    p.pop('policy_id'); p = r.profit.bind(p, 'policy_id')
    r.validate_policy(p)
    assert r.cost(d.opportunities[0], p, 'base') == r.cost(d.opportunities[0], d.policy, 'base') + 2
    p['markets']['ES']['additional_ticks']['base'] = 0
    with pytest.raises(IntegrityError):
        r.validate_policy(p)


def test_price_adapter_rejects_incomplete_and_unbound_inputs():
    s, rows, _ = prep.prior.synthetic_session()
    args = dict(policy=r.cost_policy(), as_of_ns=rows[-1]['decision_eligible_at_ns'])
    with pytest.raises(IntegrityError, match='coverage'):
        r.assemble_dataset([s], {s.trade_date.isoformat(): rows[:-1]}, **args)
    with pytest.raises(IntegrityError, match='verified calendar'):
        r.assemble_dataset([s], {s.trade_date.isoformat(): rows}, source_contract_id='b'*64,
            source_release_id='c'*64, **args)
    d = dataset()
    d.opportunities[0]['entry_open_nano'] += 1
    with pytest.raises(IntegrityError, match='mutated'):
        d.verify()


def test_generated_certified_adapter_joins_and_future_evidence_rejection(monkeypatch):
    session, rows, _ = prep.prior.synthetic_session()
    contract, release = 'b'*64, 'c'*64
    for row in rows:
        row.update(source_contract_id=contract, source_release_id=release)
    class GeneratedCalendar:
        receipt = SimpleNamespace(release_id='d'*64)
        sessions = {(session.market, session.trade_date): session}
        def verify(self):
            pass
    monkeypatch.setattr(r.old, 'VerifiedExchangeCalendar', GeneratedCalendar)
    calendar, day = GeneratedCalendar(), session.trade_date.isoformat()
    binding = r.profit.bind({'market': 'ES', 'trade_date': day, 'source_contract_id': contract,
        'source_release_id': release, 'calendar_release_id': calendar.receipt.release_id,
        'calendar_evidence_id': 'e'*64, 'contract_selection_evidence_id': 'f'*64,
        'source_certificate_id': 'a'*64, 'actual_contract': rows[0]['actual_contract'],
        'calendar_known_at_ns': rows[0]['bar_start_ns']-1,
        'contract_selection_known_at_ns': rows[0]['bar_start_ns']-1,
        'point_in_time_evidence': 'CERTIFIED_ORIGINAL_PUBLICATION',
        'source_file_sha256s': sorted(set(row['source_file_sha256'] for row in rows))}, 'binding_id')
    args = dict(policy=r.cost_policy(), calendar=calendar, source_contract_id=contract,
        source_release_id=release, bindings={day: binding}, as_of_ns=rows[-1]['decision_eligible_at_ns'])
    d = r.assemble_dataset([session], {day: rows}, **args)
    assert len(d.opportunities) == 3 and d.batches[0]['source_binding'] == binding
    for key, value in [('calendar_known_at_ns', rows[-1]['bar_start_ns']),
            ('point_in_time_evidence', 'RETROSPECTIVE'), ('actual_contract', 'WRONG'),
            ('source_file_sha256s', ['0'*64])]:
        changed = {k: v for k, v in binding.items() if k != 'binding_id'}
        changed[key] = value
        with pytest.raises(IntegrityError):
            r.assemble_dataset([session], {day: rows}, **{**args, 'bindings': {day: r.profit.bind(changed, 'binding_id')}})


def test_equal_date_accuracy_and_empty_dates_not_imputed():
    targets = np.array([0., 0., 0.])
    accuracy = r.forecast_accuracy({'model': [2., 2., 4.], 'zero_change': [0., 0., 0.],
        'training_mean': [1., 1., 1.]}, targets, ['2018-01-01', '2018-01-01', '2018-01-03'])
    assert accuracy['model']['date_balanced_mse_ticks_squared'] == 10.
    assert len(accuracy['model']['daily_mse']) == 2
    assert accuracy['zero_change']['skill'] is None


@pytest.mark.parametrize('family', ['ridge', 'xgboost'])
def test_successor_fitting_uses_identical_normalized_math_every_path(family):
    n = 505
    x = np.zeros((n, len(r.old.terminal.FEATURE_NAMES)))
    x[:, 0] = np.arange(n) % 5
    y = 3*x[:, 0] + 7
    days = [(date(2018, 1, 1)+timedelta(days=i)).isoformat() for i in range(n)]
    def data(ix):
        return SimpleNamespace(_evidence_id='d'*64, terminal_data=SimpleNamespace(features=x[ix], terminal_ticks=y[ix],
            trade_dates=tuple(days[i] for i in ix), sample_ids=tuple(map(str, range(len(ix))))))
    d, repeated = data(np.arange(n)), data(np.repeat(np.arange(n), 2))
    c = next(c for c in r.old.candidate_definitions() if c['family'] == family)
    first = r.fit_model(d, np.arange(n), c)
    for kind in ('inner', 'outer', 'final'):
        fitted = r.fit_model(repeated, np.arange(n*2), c, fit_kind=kind)
        np.testing.assert_allclose(r.predict(first, x), r.predict(fitted, x), atol=1e-5)
    before = deepcopy(first)
    r.predict(first, x*10000)
    assert first == before
    with pytest.raises(IntegrityError, match='observed dates'):
        r.fit_model(d, np.arange(251), c)


def test_calendar_row_count_does_not_replace_observed_support():
    days = tuple((date(2018, 1, 1)+timedelta(days=i)).isoformat() for i in range(1010))
    d = SimpleNamespace(calendar_dates=days, terminal_data=SimpleNamespace(trade_dates=(days[0],)*238+(days[1],)*238+days[252:]))
    with pytest.raises(IntegrityError, match='2 observed dates; requires 252'):
        r.old.fold_schedule(d)


def edge_sessions(market='ES'):
    history = []
    for i in range(20):
        day = (date(2018, 1, 1)+timedelta(days=i)).isoformat()
        _, rows, _ = prep.prior.rich_synthetic_session('ES', day, i)
        for row in rows:
            row.update(market=market, actual_contract='SYNTHETIC_FIXED')
        history.append({'trade_date': day, 'rows': rows, 'complete_at_ns': rows[-1]['decision_eligible_at_ns'],
            'expected_bar_starts': [r['bar_start_ns'] for r in rows]})
    return history


def test_edge_all_markets_fixed_history_scale_and_failure_dispositions():
    history = edge_sessions()
    at = history[-1]['complete_at_ns'] + 1
    for market in r.MARKETS:
        for s in history:
            for row in s['rows']:
                row['market'] = market
        result = edge.diagnostic(market, 'SYNTHETIC_FIXED', at, history)
        assert result['signed_relative_spread'] is not None
        assert result['selected_policy_effect'] == 'NONE'
    values = np.array([[100., 102., 99., 101.], [101., 103., 100., 102.], [102., 104., 100., 101.], [101., 105., 100., 104.]])
    assert edge.signed_edge(values) == pytest.approx(edge.signed_edge(values*1000), abs=1e-12)
    assert np.isnan(edge.signed_edge(np.ones((20, 4))))
    history = edge_sessions()
    assert edge.diagnostic('ES', 'SYNTHETIC_FIXED', at-1, history)['state'] == 'UNAVAILABLE'
    history[0]['rows'][0]['open_nano'] = -1
    assert edge.diagnostic('ES', 'SYNTHETIC_FIXED', at, history)['reason'] == 'NONPOSITIVE_OR_NONFINITE_PRICE'
    history[0]['rows'].pop(0)
    assert edge.diagnostic('ES', 'SYNTHETIC_FIXED', at, history)['missing_minutes'] == 1
    history[0]['rows'][0]['actual_contract'] = 'SYNTHETIC_ROLL'
    assert edge.diagnostic('ES', 'SYNTHETIC_FIXED', at, history)['reason'] == 'CONTRACT_ROLL_OR_MARKET_MISMATCH'


@pytest.mark.parametrize('case', ['trend', 'jump', 'sparse'])
def test_edge_predeclared_stress_cases_do_not_influence_costs(case):
    history = edge_sessions()
    before = r.cost_policy()
    for s in history:
        if case == 'sparse':
            s['rows'] = s['rows'][::20]
        else:
            for i, row in enumerate(s['rows']):
                shift = i*100000000 if case == 'trend' else (100000000000 if i >= 60 else 0)
                for k in ('open_nano', 'high_nano', 'low_nano', 'close_nano'):
                    row[k] += shift
    result = edge.diagnostic('ES', 'SYNTHETIC_FIXED', history[-1]['complete_at_ns']+1, history)
    assert result['selected_policy_effect'] == 'NONE' and r.cost_policy() == before
    assert result['state'] in ('AVAILABLE_DIAGNOSTIC_ONLY', 'UNSTABLE_NEGATIVE', 'UNAVAILABLE')
