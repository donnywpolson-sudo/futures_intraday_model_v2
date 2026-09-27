from copy import deepcopy
from dataclasses import replace
import json

import numpy as np
import pytest

from futures_rebuild import fixed_hour_model_comparison as model
from futures_rebuild import fixed_hour_model_comparison_evaluation as evaluation
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.research.contracts import ResearchContractError
from tests.test_fixed_hour_terminal_successor import synthetic_dataset, bind

pytestmark = pytest.mark.current


def test_frozen_search_and_ties():
    candidates = model.candidate_definitions()
    assert len(candidates) == 45 and len({c['candidate_id'] for c in candidates}) == 45
    assert model.design()['total_market_configurations'] == 180
    scores = [{**c, 'score': 1., 'retained_count': 5} for c in candidates]
    assert model.choose_candidate(scores)['family'] == 'ridge'
    assert model.choose_candidate(scores)['penalty'] == 10
    scores[-1]['score'] = 1.02
    assert model.choose_candidate(scores)['candidate_id'] == scores[-1]['candidate_id']
    old = [{k: v for k, v in s.items() if k != 'family'} for s in scores[:27]]
    assert model.choose_candidate(scores[:27])['candidate_id'] == model.prior.choose_candidate(old)['candidate_id']
    settings = model.design()['phase_5']['xgboost']
    assert settings['early_stopping'] is False and settings['rounds'] == 100
    assert settings['nthread'] == 1 and settings['seed'] == 62817


@pytest.mark.parametrize('candidate_index', [0, 27, 28])
def test_native_fit_roundtrip_constant_nonfinite_and_tamper(candidate_index):
    d = synthetic_dataset(n=300)
    c = model.candidate_definitions()[candidate_index]
    fitted = model.fit_model(d.features, d.terminal_ticks, d.sample_ids, d.trade_dates, c)
    p = model.predict(fitted, d.features)
    np.testing.assert_array_equal(p, model.predict(json.loads(json.dumps(fitted)), d.features))
    if c['family'] == 'xgboost':
        assert fitted['parameters']['max_depth'] == c['max_depth']
        assert fitted['parameters']['min_child_weight'] == 50
        assert fitted['minimum_observed_leaf_dates'] >= 20
        assert 'model_json' in fitted and 'pickle' not in fitted
    corrupted = deepcopy(fitted)
    corrupted['training_mean_ticks'] += 1
    with pytest.raises(IntegrityError): model.predict(corrupted, d.features)
    constant = model.fit_model(np.ones_like(d.features), d.terminal_ticks, d.sample_ids, d.trade_dates, c)
    assert constant['columns'] == []
    np.testing.assert_array_equal(model.predict(constant, d.features), np.full(len(p), np.mean(d.terminal_ticks)))
    bad = d.features.copy(); bad[0, 0] = np.nan
    with pytest.raises((IntegrityError, ResearchContractError)):
        model.fit_model(bad, d.terminal_ticks, d.sample_ids, d.trade_dates, c)
    with pytest.raises(IntegrityError):
        model.fit_model(d.features, d.terminal_ticks, d.sample_ids, d.trade_dates, {**c, 'feature_names': []})


def test_leaf_support_is_distinct_dates_not_rows():
    with pytest.raises(model.LeafSupportError):
        model.check_leaf_dates(np.zeros((1000, 100)), ['2018-01-01'] * 1000)
    assert model.check_leaf_dates(np.zeros((20, 100)), [str(i) for i in range(20)]) == 20


def test_xgboost_learns_bounded_nonlinearity():
    d = synthetic_dataset(n=600)
    y = np.where(abs(d.features[:, 0]) > .7, 60., -60.)
    f, a = np.arange(800), np.arange(800, 1200)
    predictions = []
    for c in (model.candidate_definitions()[0], model.candidate_definitions()[27]):
        fit = model.fit_model(d.features[f], y[f], tuple(d.sample_ids[i] for i in f), tuple(d.trade_dates[i] for i in f), c)
        predictions.append(model.predict(fit, d.features[a]))
    assert np.mean((predictions[1] - y[a]) ** 2) < np.mean((predictions[0] - y[a]) ** 2) * .5


@pytest.fixture(scope='module')
def run():
    d = synthetic_dataset()
    return d, model.run_market_wfa(d)


def report(d, run, extra=10, **cost_inputs):
    n = len(d.sample_ids)
    count = sum(len(f['audit_indices']) for f in run['outer_folds'])
    return evaluation.evaluate_outer_predictions(d, run,
        upside_ticks=np.maximum(d.terminal_ticks, 0) + extra,
        downside_ticks=np.maximum(-d.terminal_ticks, 0) + extra,
        tick_values_usd=d.tick_values_usd, quartiles=np.full(n, 2),
        decision_ns=np.arange(n, dtype=np.int64) * 60000000000,
        coverage=evaluation.coverage_report(scheduled=count + 504, insufficient_history=504,
            valid_feature=count, invalid_outcome=0, quality_errors=0), **cost_inputs)


@pytest.fixture(scope='module')
def evaluated(run):
    return report(*run)


def test_nested_ridge_reproduction_and_diagnostic_isolation(run):
    d, result = run
    old = model.prior.run_market_wfa(d)
    for new, previous in zip(result['outer_folds'], old['outer_folds']):
        assert len(new['candidate_scores']) == 45
        assert new['streams']['ridge_only']['candidate_id'] == previous['candidate_id']
        np.testing.assert_array_equal(new['streams']['ridge_only']['predictions'], previous['predictions'])
    a, b = report(d, result), report(d, result, 100)
    assert a['daily_stress_net_ticks'] == b['daily_stress_net_ticks']
    assert a['baseline_daily_stress_net_ticks'] == b['baseline_daily_stress_net_ticks']


def test_outer_labels_do_not_select_their_own_models(run, monkeypatch):
    d, result = run
    first = model.prior.fold_schedule(d)[0]
    y = d.terminal_ticks.copy(); y[first.audit_indices] *= -1
    # Exercise the actual first outer fold; later folds may legally train on it.
    monkeypatch.setattr(model.prior, 'fold_schedule', lambda _: [first])
    changed = model.run_market_wfa(replace(d, terminal_ticks=y))
    assert changed['outer_folds'][0] == result['outer_folds'][0]


def test_complete_family_no_added_value_no_fallback(evaluated):
    # Gate-arithmetic fixture: do not pass substituted streams through verification.
    r = deepcopy(evaluated)
    r['baseline_daily_stress_net_ticks']['ridge_only'] = r['daily_stress_net_ticks'][:]
    r = bind(r, 'result_id')
    burden = evaluation.trial_burden(289, [])
    assert burden['combined_burden'] == 469
    results = {m: evaluation.terminal_failure(m, reason='explicit failure') for m in model.prior.MARKETS}
    results['ES'] = r
    q = evaluation.qualify_markets(results, burden=burden, dsr_censuses={})
    assert len(q['declared_hypothesis_ids']) == 28
    assert q['adjusted_p_values']['ES:ridge_only'] == 1
    assert q['phase_12_eligible_markets'] == []
    assert evaluation.downstream_eligibility(q, {})['refit_without_evaluation'] == []
    with pytest.raises(IntegrityError): evaluation.downstream_eligibility(q, {'ES': 'PASS'})
    with pytest.raises(IntegrityError): evaluation.qualify_markets({'ES': r}, burden=burden, dsr_censuses={})
    ids = [f"{model.design()['mechanism_id']}/{m}/{c['candidate_id']}" for m in model.prior.MARKETS for c in model.candidate_definitions()]
    entry = {'trial_id': 'successor', 'count': 180, 'included_in_prior_floor': False, 'configuration_ids': ids}
    assert evaluation.trial_burden(289, [entry, entry])['combined_burden'] == 469


def test_qualification_positive_and_cost_failure_fixtures(evaluated):
    original = evaluated
    burden = evaluation.trial_burden(289, [])
    # Deliberately fabricated outcome-only fixture tests gate arithmetic, not alpha.
    def controlled(market, mean):
        r = deepcopy(original)
        r['market'] = market
        y = mean + np.sin(np.arange(len(r['trade_dates'])))
        r['daily_stress_net_ticks'] = y.tolist()
        r['baseline_daily_stress_net_ticks'] = {b: np.zeros(len(y)).tolist() for b in evaluation.BASELINES}
        r['segments'] = {str(q): {'dates': len(y), 'mean_stress_net_ticks': mean} for q in (1, 2, 3, 4)}
        r['trades'] = 1000
        r = bind(r, 'result_id')
        census = {'burden_id': burden['census_id'], 'result_id': r['result_id'],
            'trial_ids': [f'SYNTHETIC-{i}' for i in range(469)],
            'trial_sharpes': [*np.linspace(-.1, .1, 468), float(np.mean(y) / np.std(y, ddof=1))],
            'selected_trial_index': 468}
        return r, census
    for survivor, failed in [('ES', 'NQ'), ('NQ', 'ES')]:
        r, census = controlled(survivor, 10.)
        results = {m: evaluation.terminal_failure(m, reason='explicit economic failure') for m in model.prior.MARKETS}
        results[survivor] = r
        q = evaluation.qualify_markets(results, burden=burden, dsr_censuses={survivor: census})
        assert q['phase_12_eligible_markets'] == [survivor], q['market_decisions']
        assert not q['market_decisions'][failed]['qualified']
        assert evaluation.downstream_eligibility(q, {survivor: 'PASS'})['refit_without_evaluation'] == [survivor]
        assert evaluation.downstream_eligibility(q, {survivor: 'FAIL'})['refit_without_evaluation'] == []
    r, census = controlled('ES', -1.)
    results['ES'] = r
    q = evaluation.qualify_markets(results, burden=burden, dsr_censuses={'ES': census})
    assert 'STRESS_NET_HAC_FAIL' in q['market_decisions']['ES']['reasons']
    bad_dates = deepcopy(results['NQ']); bad_dates['trade_dates'] = bad_dates['trade_dates'][1:]
    with pytest.raises(IntegrityError):
        evaluation.qualify_markets({**results, 'NQ': bind(bad_dates, 'result_id')}, burden=burden, dsr_censuses={})


def test_dependency_and_training_only_transforms(monkeypatch):
    d = synthetic_dataset(n=300)
    x = d.features.copy(); x[:400, 1] = 1.; x[:400, 2] = x[:400, 0]
    c = next(c for c in model.candidate_definitions() if c['pack'] == 'existing_18' and c['family'] == 'xgboost')
    fitted = model.fit_model(x[:400], d.terminal_ticks[:400], d.sample_ids[:400], d.trade_dates[:400], c)
    assert 1 not in fitted['columns'] and 2 not in fitted['columns']
    changed = x[400:].copy(); changed[:, 1:3] = 100000.
    np.testing.assert_array_equal(model.predict(fitted, x[400:]), model.predict(fitted, changed))
    monkeypatch.setattr(model, 'version', lambda _: 'unapproved')
    with pytest.raises(IntegrityError): model.predict(fitted, x[400:])


def test_complete_replay_rejects_rehashed_procedural_and_training_changes():
    # Full frozen search and eight folds; constant features make refits cheap.
    d = synthetic_dataset()
    d = replace(d, features=np.zeros_like(d.features))
    result = model.run_market_wfa(d)
    assert report(d, result)['status'] == 'EVALUATED_MECHANICS_ONLY'
    decisions = np.arange(len(d.sample_ids), dtype=np.int64) * 60000000000
    start = int(decisions[result['outer_folds'][0]['audit_indices'][0]])
    end = int(decisions[-1]) + 1
    freeze = end + 2
    evidence = bind(dict(market='ES', certification_id='a' * 64, source_release_id='b' * 64,
        independent_of_candidate_results=True, price_reference_basis='ONE_MINUTE_OPEN_PRINT_PROXY',
        observations_end_ns=start - 1, effective_start_ns=start, effective_end_ns=end,
        certified_at_ns=end + 1, evaluation_frozen_at_ns=freeze, round_trip_fee_usd=10.,
        adverse_ticks_round_trip=2., coverage_ratio=1., all_verified_globex_intervals=True), 'evidence_id')
    certified = report(d, result, cost_evidence=evidence,
        trusted_cost_certifications=frozenset({'a' * 64}), evaluation_frozen_at_ns=freeze)
    assert certified['expected_cost_status'] == 'CERTIFIED_EXPECTED_COST_PROXY_NOT_LIVE_FILL_EVIDENCE'
    assert certified['evaluation_frozen_at_ns'] == freeze
    assert certified['cost_evidence_id'] == evidence['evidence_id']
    assert 'expected' in certified['scenarios']
    corruptions = []
    for field in ('candidate_scores', 'inner_manifests'):
        bad = deepcopy(result)
        for fold in bad['outer_folds']:
            fold[field] = []
        corruptions.append(bad)
    bad = deepcopy(result)
    bad['outer_folds'][0]['candidate_scores'][0]['score'] += 10000
    corruptions.append(bad)
    bad = deepcopy(result)
    bad['outer_folds'][0]['streams']['combined']['candidate_id'] = 'substituted-winner'
    corruptions.append(bad)
    bad = deepcopy(result)
    fold = bad['outer_folds'][0]
    split = model.prior.fold_schedule(d)[0]
    stream = fold['streams']['combined']
    fitted = model.fit_model(d.features[split.fit_indices], d.terminal_ticks[split.fit_indices] + 10000,
        tuple(d.sample_ids[i] for i in split.fit_indices), tuple(d.trade_dates[i] for i in split.fit_indices),
        stream['fitted_model']['candidate'])
    predictions = model.predict(fitted, d.features[split.audit_indices])
    stream.update(fitted_model=fitted, predictions=predictions.tolist(),
        directions=model.prior.terminal_directions(predictions, d.stress_cost_ticks[split.audit_indices]).tolist())
    corruptions.append(bad)
    for bad in corruptions:
        with pytest.raises(IntegrityError, match='deterministic selection and fitting replay'):
            report(d, bind(bad, 'run_id'))


def test_ledger_registered_successor_not_double_counted(monkeypatch):
    from pathlib import Path
    from futures_rebuild import fixed_hour_model_comparison_preparation as prep
    root = Path(__file__).resolve().parents[1]
    before = prep.predecessor_ledger_snapshot(root)
    ids = [f"candidate:{model.design()['mechanism_id']}/{m}/{c['candidate_id']}" for m in model.prior.MARKETS for c in model.candidate_definitions()]
    monkeypatch.setattr(prep, 'predecessor_ledger_snapshot', lambda _: {
        **before, 'candidate_identities': before['candidate_identities'] + ids})
    after = prep.ledger_snapshot(root)
    assert after['burden']['combined_burden'] == before['burden']['combined_burden'] + 180
    assert after['burden']['already_registered_successor_configurations'] == 180


def test_zero_signal_abstains_and_refit_support_failure_stops(monkeypatch):
    d = synthetic_dataset()
    d = replace(d, features=np.zeros_like(d.features), terminal_ticks=np.zeros_like(d.terminal_ticks))
    result = model.run_market_wfa(d)
    for f in result['outer_folds']:
        assert f['streams']['combined']['directions'] == [0.] * len(f['audit_indices'])
        assert f['streams']['combined']['candidate_id'] == f['streams']['ridge_only']['candidate_id']
    r = report(d, result)
    assert r['trades'] == 0 and set(r['daily_stress_net_ticks']) == {0.}
    # Inner Ridge scoring uses the proven kernel; a selected refit's engineering
    # error must escape instead of trying a replacement or promoting the benchmark.
    original = model.fit_model
    def failing_refit(x, y, ids, dates, candidate):
        if candidate['family'] == 'ridge':
            raise model.LeafSupportError('injected selected refit failure')
        return original(x, y, ids, dates, candidate)
    monkeypatch.setattr(model, 'fit_model', failing_refit)
    with pytest.raises(model.LeafSupportError): model.run_market_wfa(d)


def test_prepared_bundle_and_full_synthetic_evidence():
    from pathlib import Path
    from futures_rebuild import fixed_hour_model_comparison_preparation as prep
    root = Path(__file__).resolve().parents[1]
    bundle = json.loads((root / prep.CONFIG).read_bytes())
    prep.validate_bundle(bundle, root)
    assert bundle['trial_ledger']['burden']['combined_burden'] >= 469
    assert bundle['specification'] == model.design()
    assert bundle['schema_version'] == 'fixed_hour_model_comparison_preparation/2.0.0'
    assert bundle['readiness']['historical_research'] == 'BLOCKED_CENSUS_RECOVERY_UNRESOLVED'
    assert bundle['readiness']['source_bound_readiness'] == 'NOT_CERTIFIED'
    assert bundle['readiness']['expected_costs'] == 'EXPECTED_COST_UNCALIBRATED'
    assert bundle['readiness']['historical_search_upper_bound_established'] is False
    assert bundle['readiness']['profitability_established'] is False
    predecessor = json.loads((root / 'configs/fixed_hour_model_comparison_v1.json').read_bytes())
    assert predecessor['specification'] == bundle['specification']
    with pytest.raises(IntegrityError, match='drifted'):
        prep.validate_bundle(predecessor, root)
    assert set(bundle['phase_contracts']) == {'3', '4', '5', '6', 'ladder'}
    for contract in bundle['phase_contracts'].values():
        assert contract['mechanism_id'] == model.design()['mechanism_id']
        assert not any(contract['authority'].values())
    evidence = json.loads((root / bundle['synthetic_certificate']['path']).read_bytes())
    assert evidence['state'] == 'PASSED_ENGINEERING_ONLY'
    assert set(evidence['market_runs']) == set(model.prior.MARKETS)
    assert len(evidence['qualification']['declared_hypothesis_ids']) == 28
    for market in model.prior.MARKETS:
        run = evidence['market_runs'][market]
        assert run['outer_folds'] == 8 and run['observations'] == 2020
        assert run['candidate_count_per_fold'] == 45
        assert set(run['selected_candidates']) == {'combined', 'ridge_only'}
        assert evidence['construction'][market] == dict(scheduled=3030, insufficient_history=1010,
            valid_feature=2020, invalid_outcome=0, quality_errors=0)
    assert not any(evidence['authority'].values())
    assert evidence['downstream']['refit_without_evaluation'] == []
    tampered = deepcopy(bundle); tampered['specification']['configurations_per_market'] = 46
    with pytest.raises(IntegrityError): prep.validate_bundle(tampered, root)
