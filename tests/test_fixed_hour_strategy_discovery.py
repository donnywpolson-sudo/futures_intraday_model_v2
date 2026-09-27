from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

import numpy as np
import pytest

from futures_rebuild import fixed_hour_strategy_discovery as model
from futures_rebuild import fixed_hour_strategy_discovery_evaluation as evaluation
from futures_rebuild import fixed_hour_strategy_discovery_preparation as preparation
from futures_rebuild import fixed_hour_model_comparison as predecessor
from futures_rebuild import fixed_hour_model_comparison_evaluation as old_evaluation
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.research.contracts import ResearchContractError
from tests.test_fixed_hour_terminal_successor import synthetic_dataset, scheduled_fixture, bind

pytestmark = pytest.mark.current


def dataset(n=1010):
    old = synthetic_dataset(n=n)
    x = np.column_stack((old.features, old.features[:, :5] * .5))
    # Planted linear signal unrelated to the previous-bar-sign baseline.
    y = np.rint(80 * x[:, 3])
    return model.TerminalDataset(old.market, old.sample_ids, old.trade_dates,
        x, y, old.stress_cost_ticks, old.global_dates, old.tick_values_usd)


def report(d, run, extra=10):
    n = len(d.sample_ids)
    count = sum(len(f['audit_indices']) for f in run['outer_folds'])
    return evaluation.evaluate_outer_predictions(d, run,
        upside_ticks=np.maximum(d.terminal_ticks, 0) + extra,
        downside_ticks=np.maximum(-d.terminal_ticks, 0) + extra,
        tick_values_usd=d.tick_values_usd, quartiles=np.full(n, 2),
        decision_ns=np.arange(n, dtype=np.int64) * 60000000000,
        coverage=evaluation.coverage_report(scheduled=count + 504, insufficient_history=504,
            valid_feature=count, invalid_outcome=0, quality_errors=0))


def test_frozen_formulas_and_candidate_family():
    values = model.discovery_features(list(range(61)), 1)
    np.testing.assert_allclose(list(values.values()), [np.sqrt(5), np.sqrt(20), np.sqrt(60), 30, 1])
    assert not any(model.discovery_features([0] * 61, 1).values())
    np.testing.assert_allclose(list(model.discovery_features(list(range(0, -61, -1)), 1).values()),
        -np.array(list(values.values())))
    # Exact integer subtraction survives a level larger than float64 precision.
    assert model.discovery_features([10**25 + i for i in range(61)], 1) == values
    mixed = [0]
    for i in range(60):
        mixed.append(mixed[-1] + (2 if i % 2 == 0 else -1))
    f = model.discovery_features(mixed, 1)
    assert f['signed_squared_change_asymmetry_60'] == pytest.approx(3/5)
    assert f['normalized_momentum_60'] == pytest.approx(30 / 1.5 / np.sqrt(60))
    assert f['normalized_stretch_60'] == pytest.approx((mixed[-1] - sum(mixed)/61) / 1.5)
    for prices, tick in (([np.nan]*61, 1), ([np.inf]*61, 1), ([True]*61, 1),
                         ([0]*60, 1), ([0]*61, 0), (list(range(61)), 2), ([10**400*i for i in range(61)], 1)):
        with pytest.raises(IntegrityError):
            model.discovery_features(prices, tick)
    candidates = model.candidate_definitions()
    assert len(candidates) == len({c['candidate_id'] for c in candidates}) == 50
    assert len(model.FEATURE_NAMES) == 34 and len(model.PACKS) == 10
    for c in predecessor.candidate_definitions():
        assert c in candidates
    assert model.PACKS['context_discovery'] == (*model.terminal.PACKS['context'], *model.NEW_FEATURES)
    assert model.design()['total_market_configurations'] == 200
    assert predecessor.design()['total_market_configurations'] == 180
    assert model.design()['phase_6']['added_value']['promotion_gate'] is False
    assert 'promotion_gate' not in predecessor.design()['phase_6']['added_value']
    assert model.design()['phase_6']['primary_score'] == predecessor.design()['phase_6']['primary_score']
    assert model.design()['phase_5']['xgboost'] == predecessor.design()['phase_5']['xgboost']


def test_causal_grid_and_unchanged_predecessor_features():
    grid, rows = scheduled_fixture()
    assert model.build_feature_record(rows, grid[0])['status'] == 'INSUFFICIENT_HISTORY'
    old = model.terminal.build_feature_record(rows, grid[1])
    new = model.build_feature_record(rows, grid[1])
    assert list(new['feature_values']) == list(model.FEATURE_NAMES)
    assert {k: new['feature_values'][k] for k in old['feature_values']} == old['feature_values']
    assert new['feature_record_id'] != old['feature_record_id']
    future = deepcopy(rows)
    for row in future[81:]:
        row['close_nano'] += 1000000
    assert model.build_feature_record(future, grid[1]) == new
    for kind in ('availability', 'gap', 'roll', 'nonfinite'):
        bad = deepcopy(rows)
        if kind == 'availability': bad[40]['decision_eligible_at_ns'] = grid[1]['decision_at_ns'] + 1
        if kind == 'gap': del bad[40]
        if kind == 'roll': bad[40]['actual_contract'] = 'OTHER'
        if kind == 'nonfinite': bad[40]['close_nano'] = np.nan
        with pytest.raises((IntegrityError, ResearchContractError)):
            model.build_feature_record(bad, grid[1])
    for row in rows:
        for name in ('open_nano', 'high_nano', 'low_nano', 'close_nano'):
            row[name] -= 10**14
    negative = model.build_feature_record(rows, grid[1])
    assert all(np.isfinite(list(negative['feature_values'].values())))
    batch = model.assemble_opportunities(rows, grid, as_of_ns=rows[-1]['decision_eligible_at_ns'])
    prior = model.terminal.assemble_opportunities(rows, grid, as_of_ns=rows[-1]['decision_eligible_at_ns'])
    assert batch['coverage'] == prior['coverage']
    for a, b in zip(batch['records'], prior['records']):
        if a['outcome']:
            assert a['outcome']['fit_target'] == b['outcome']['fit_target']
            assert a['outcome']['mechanism_id'] == model.mechanism_id()


@pytest.mark.parametrize('family', ['ridge', 'xgboost'])
def test_new_pack_native_models_train_only_and_reject_tampering(family):
    d = dataset(300)
    candidate = next(c for c in model.candidate_definitions() if c['pack'] == 'context_discovery' and c['family'] == family)
    x = d.features.copy()
    x[:, -1] = 1
    fitted = model.fit_model(x, d.terminal_ticks, d.sample_ids, d.trade_dates, candidate)
    assert 33 not in fitted['columns']
    predicted = model.predict(fitted, x)
    np.testing.assert_array_equal(predicted, model.predict(json.loads(json.dumps(fitted)), x))
    x[:, -1] = 1e10
    np.testing.assert_array_equal(predicted, model.predict(fitted, x))
    bad = deepcopy(fitted); bad['training_mean_ticks'] += 1
    with pytest.raises(IntegrityError): model.predict(bad, x)
    x[0, -2] = np.nan
    with pytest.raises((IntegrityError, ResearchContractError)):
        model.fit_model(x, d.terminal_ticks, d.sample_ids, d.trade_dates, candidate)


@pytest.fixture(scope='module')
def evaluated():
    d = dataset()
    run = model.run_market_wfa(d)
    return d, run, report(d, run)


def test_full_selection_replay_samples_and_outer_isolation(evaluated, monkeypatch):
    d, run, r = evaluated
    assert len(run['outer_folds']) == 8
    assert r['expected_cost_status'] == 'EXPECTED_COST_UNCALIBRATED'
    assert set(r['execution_cost_margins']) == {'combined', 'ridge_only'}
    for fold, split in zip(run['outer_folds'], model.fold_schedule(d)):
        assert len(fold['candidate_scores']) == 50
        for name in ('combined', 'ridge_only'):
            assert fold['streams'][name]['fitted_model']['fit_sample_ids'] == fold['fit_sample_ids']
            assert len(fold['streams'][name]['predictions']) == len(split.audit_indices)
    first = model.fold_schedule(d)[0]
    y = d.terminal_ticks.copy(); y[first.audit_indices] *= -1
    monkeypatch.setattr(model, 'fold_schedule', lambda _: (first,))
    changed = model.run_market_wfa(replace(d, terminal_ticks=y))
    assert changed['outer_folds'][0] == run['outer_folds'][0]
    monkeypatch.undo()
    assert report(d, run, extra=100)['daily_stress_net_ticks'] == r['daily_stress_net_ticks']
    bad = deepcopy(run); bad['outer_folds'][0]['candidate_scores'][0]['score'] += 100
    with pytest.raises(IntegrityError, match='deterministic selection'):
        report(d, bind(bad, 'run_id'))


def qualification_inputs(r):
    burden = evaluation.trial_burden(649, [])
    results = {m: evaluation.terminal_failure(m, reason='synthetic terminal failure') for m in model.MARKETS}
    results['ES'] = r
    y = np.array(r['daily_stress_net_ticks'])
    n = burden['combined_burden']
    # Explicit fabricated historical census: a gate fixture, never real evidence.
    census = {'burden_id': burden['census_id'], 'result_id': r['result_id'],
        'trial_ids': [f'SYNTHETIC-{i}' for i in range(n)],
        'trial_sharpes': [*np.linspace(-.1, .1, n-1), float(np.mean(y)/np.std(y, ddof=1))],
        'selected_trial_index': n-1}
    return results, burden, {'ES': census}


def test_ridge_winner_qualifies_without_added_value_and_gates_still_hold(evaluated):
    _, run, r = evaluated
    assert all(f['streams']['combined']['fitted_model']['candidate']['family'] == 'ridge' for f in run['outer_folds'])
    assert r['daily_stress_net_ticks'] == r['baseline_daily_stress_net_ticks']['ridge_only']
    results, burden, censuses = qualification_inputs(r)
    q = evaluation.qualify_markets(results, burden=burden, dsr_censuses=censuses)
    assert q['phase_12_eligible_markets'] == ['ES'], q['market_decisions']['ES']
    assert len(q['declared_hypothesis_ids']) == 28
    assert q['adjusted_p_values']['ES:ridge_only'] == 1
    assert q['market_decisions']['ES']['added_value_status'] == 'NO_ADDED_VALUE'
    assert evaluation.downstream_eligibility(q, {'ES': 'PASS'})['refit_without_evaluation'] == ['ES']
    missing = evaluation.qualify_markets(results, burden=burden, dsr_censuses={})
    assert 'COMPLETE_DSR_CENSUS_UNAVAILABLE' in missing['market_decisions']['ES']['reasons']
    assert missing['phase_12_eligible_markets'] == []
    for kind in ('loss', 'baseline', 'segment', 'activity', 'malformed'):
        bad = deepcopy(r)
        if kind == 'loss': bad['daily_stress_net_ticks'] = (-abs(np.array(bad['daily_stress_net_ticks']))).tolist()
        if kind == 'baseline': bad['baseline_daily_stress_net_ticks']['always_long'] = r['daily_stress_net_ticks'][:]
        if kind == 'segment': bad['segments']['2']['mean_stress_net_ticks'] = -1
        if kind == 'activity': bad['trades'] = 1
        if kind == 'malformed': bad['baseline_daily_stress_net_ticks']['ridge_only'][0] = np.nan
        if kind == 'malformed':
            # Hashing itself rejects NaN; the qualifier independently validates arrays.
            with pytest.raises((IntegrityError, ResearchContractError, ValueError)):
                bad = bind(bad, 'result_id')
                evaluation.qualify_markets({**results, 'ES': bad}, burden=burden, dsr_censuses=censuses)
        else:
            bad = bind(bad, 'result_id')
            rr, bb, cc = qualification_inputs(bad)
            decision = evaluation.qualify_markets(rr, burden=bb, dsr_censuses=cc)
            assert not decision['market_decisions']['ES']['qualified']


def test_cost_margin_arithmetic_and_no_trades():
    args = (np.array([1., -1., 0.]), np.array([10., -2., 999.]), np.full(3, 12.5))
    r = evaluation.execution_cost_margins('ES', *args)
    assert r['trade_count'] == 2
    assert r['break_even_adverse_execution_usd_per_round_trip'] == 65
    assert r['additional_adverse_execution_usd_per_round_trip_beyond_stress'] == pytest.approx(15)
    loss = evaluation.execution_cost_margins('ES', args[0], -args[1], args[2])
    assert loss['break_even_adverse_execution_usd_per_round_trip'] == -85
    zero = evaluation.execution_cost_margins('ES', np.zeros(3), args[1], args[2])
    assert zero['status'] == 'UNAVAILABLE_NO_TRADES'
    assert zero['break_even_adverse_execution_usd_per_round_trip'] is None
    assert zero['additional_adverse_execution_usd_per_round_trip_beyond_stress'] is None
    with pytest.raises((IntegrityError, ResearchContractError)):
        evaluation.execution_cost_margins('ES', np.array([np.nan]), np.ones(1), np.ones(1))


def test_predecessor_native_predictions_and_selected_refit_failure(evaluated, monkeypatch):
    d, run, _ = evaluated
    for candidate in (predecessor.candidate_definitions()[0], predecessor.candidate_definitions()[27]):
        x, y, ids, dates = d.features[:600], d.terminal_ticks[:600], d.sample_ids[:600], d.trade_dates[:600]
        old = predecessor.fit_model(x[:, :29], y, ids, dates, candidate)
        new = model.fit_model(x, y, ids, dates, candidate)
        np.testing.assert_array_equal(predecessor.predict(old, x[:, :29]), model.predict(new, x))
    first = model.fold_schedule(d)[0]
    original = model.fit_model
    def fail_selected_ridge(x, y, ids, dates, candidate):
        if candidate['family'] == 'ridge':
            raise model.LeafSupportError('injected selected refit failure')
        return original(x, y, ids, dates, candidate)
    monkeypatch.setattr(model, 'fold_schedule', lambda _: (first,))
    monkeypatch.setattr(model, 'fit_model', fail_selected_ridge)
    with pytest.raises(model.LeafSupportError, match='selected refit'):
        model.run_market_wfa(d)


def test_accounting_deduplicates_registrations_and_preparation(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    raw = preparation.predecessor_ledger_snapshot(root)
    before = preparation.ledger_snapshot(root)
    assert before['burden']['new_configurations'] == 200
    assert before['burden']['combined_burden'] >= 849
    ids = [f"{model.mechanism_id()}/{m}/{c['candidate_id']}" for m in model.MARKETS for c in model.candidate_definitions()]
    entry = dict(trial_id=model.mechanism_id(), count=200, included_in_prior_floor=False, configuration_ids=ids)
    assert evaluation.trial_burden(649, [entry, entry])['combined_burden'] == 849
    with pytest.raises(IntegrityError):
        evaluation.trial_burden(649, [entry, {**entry, 'trial_id': 'overlapping'}])
    for count in (1, 200):
        monkeypatch.setattr(preparation, 'predecessor_ledger_snapshot', lambda _: {
            **raw, 'candidate_identities': raw['candidate_identities'] + ['candidate:' + i for i in ids[:count]]})
        after = preparation.ledger_snapshot(root)
        assert after['burden']['combined_burden'] == before['burden']['combined_burden']
        assert after['burden']['already_registered_successor_configurations'] == count
    all_ids = [f"candidate:{spec['mechanism_id']}/{m}/{c['candidate_id']}"
               for spec in (model.terminal.design(), predecessor.design(), preparation.profit.design(), model.design())
               for m in model.MARKETS for c in spec['candidates']]
    monkeypatch.setattr(preparation, 'predecessor_ledger_snapshot', lambda _: {
        **raw, 'candidate_identities': raw['candidate_identities'] + all_ids + all_ids})
    assert preparation.ledger_snapshot(root)['burden']['combined_burden'] == before['burden']['combined_burden']
    monkeypatch.undo()
    bundle = preparation.build_bundle(root)
    preparation.validate_bundle(bundle, root)
    assert set(bundle['phase_contracts']) == {'3', '4', '5', '6', 'ladder'}
    assert all(c['mechanism_id'] == model.mechanism_id() and not any(c['authority'].values()) for c in bundle['phase_contracts'].values())
    assert bundle['readiness']['historical_research'] == 'BLOCKED_CENSUS_RECOVERY_UNRESOLVED'
    assert bundle['readiness']['historical_search_upper_bound_established'] is False
    bad = deepcopy(bundle); bad['specification']['configurations_per_market'] = 49
    with pytest.raises(IntegrityError): preparation.validate_bundle(bind(bad, 'bundle_id'), root)
