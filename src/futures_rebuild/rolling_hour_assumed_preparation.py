"""Inactive freeze and synthetic-only acceptance; no historical execution CLI."""
import argparse
from collections import Counter
from datetime import date, timedelta
import json
from pathlib import Path

import numpy as np

from . import rolling_hour_assumed_cost as r
from . import rolling_hour_edge as edge
from . import rolling_hour_preparation as prior
from .canonical import sha256_file, write_new_canonical_authority as write
from .errors import IntegrityError

CONFIG = 'configs/rolling_hour_research_v8.json'
EVIDENCE = 'state/unpublished_evidence/rolling_hour_assumed_cost_v1'
FILES = ('src/futures_rebuild/rolling_hour_assumed_cost.py',
    'src/futures_rebuild/rolling_hour_edge.py', 'src/futures_rebuild/rolling_hour_assumed_preparation.py',
    'tests/test_rolling_hour_assumed_cost.py')


def metadata(root):
    contract = json.loads((root / 'configs/source_contract.json').read_bytes())
    if contract['universe']['standard_roots'] != list(r.MARKETS):
        raise IntegrityError('source universe differs')
    source = contract['complete_inventory']
    if sha256_file(root / source['path']) != source['sha256']:
        raise IntegrityError('inventory binding differs')
    entries = json.loads((root / source['path']).read_bytes())['entries']
    families = ('ohlcv_1m', 'definition', 'statistics', 'status')
    entries = [e for e in entries if e['kind'] == 'DBN' and e['market'] in r.MARKETS and e['family'] in families]
    matrix = []
    for market in r.MARKETS:
        for year in range(2010, 2027):
            start, end = f'{year}-01-01', f'{year+1}-01-01'
            coverage = {f: [{k: e[k] for k in ('path', 'sha256', 'interval_start_inclusive', 'interval_end_exclusive',
                    'admitted_standard_foundation', 'identity_basis')} for e in entries
                if e['market'] == market and e['family'] == f and e['interval_start_inclusive'] < end
                and e['interval_end_exclusive'] > start] for f in families}
            matrix.append({'market': market, 'year': year, 'families': coverage,
                'scope': 'CURRENT_RESEARCH' if market in r.old.MARKETS and 2018 <= year <= 2022 else 'COMPATIBILITY_ONLY_NO_READ_AUTHORITY',
                'disposition': 'CATALOG_INTERVALS_ONLY' if all(coverage.values()) else 'MISSING_FAMILY_CATALOG_INTERVAL',
                'point_in_time_calendar': 'UNRESOLVED', 'contract_selection': 'UNRESOLVED',
                'contract_units': 'REQUIRE_APPLICABLE_CERTIFIED_ROW_UNITS', 'fee_status': 'ASSUMED_10_USD_FLOOR',
                'edge': 'APPLICABLE_INTERFACE_ESTIMATE_NOT_GUARANTEED'})
    return r.profit.bind({'inventory_path': source['path'], 'inventory_sha256': source['sha256'],
        'family_file_counts': dict(Counter(e['family'] for e in entries)), 'matrix': matrix,
        'decoded_observations': False, 'claim': 'INTERVAL_METADATA_NOT_OBSERVATION_COVERAGE'}, 'metadata_id')


def build_bundle(root):
    previous = json.loads((root / prior.CONFIG).read_bytes())
    prior.validate_bundle(previous, root)
    specification = r.design()
    return r.profit.bind({'schema_version': 'assumed_cost_preparation/1', 'specification': specification,
        'predecessor_bundle_id': previous['bundle_id'], 'predecessor_file_sha256': sha256_file(root / prior.CONFIG),
        'implementation_files': {p: sha256_file(root / p) for p in sorted(set(FILES) | set(previous['implementation_files']))},
        'runtime_versions': prior.failed_validation.runtime_versions(), 'cost_policy': r.cost_policy(),
        'metadata': metadata(root), 'preserved_accounting': previous['accounting'],
        'preserved_history_recovery': previous['history_recovery'], 'statistical_block': previous['statistical_block'],
        'accounting': {'new_configuration_ids': [f"{specification['mechanism_id']}/{m}/{c['candidate_id']}"
                for m in r.old.MARKETS for c in r.old.candidate_definitions()],
            'new_configuration_charges': 60, 'compatibility_configuration_charges': 0,
            'change_reason': 'ASSUMED_COST_SELECTION_AND_POLICY_REPLAY', 'repeated_preparation_is_new_trial': False,
            'historical_search_upper_bound_established': False, 'registered': False, 'results_viewed': 'SYNTHETIC_ONLY_FOR_SUCCESSOR'},
        'synthetic_acceptance': {'markets': list(r.old.MARKETS), 'dates': 1026, 'seed': 74161,
            'fixture': 'UNCHANGED_RICH_CALENDAR_V1', 'outer_folds': 8, 'inner_folds': 4, 'candidates': 15,
            'other_37_nested_runs': 'NOT_RUN_COMPATIBILITY_ONLY', 'failure_replacement': False},
        'stopping': 'RECORD_ALL_MARKET_FAILURES_NO_RESULT_INFORMED_REPLACEMENT_OR_RETRY',
        'historical_execution': 'BLOCKED_CALENDAR_ROLL_ACCOUNTING_REGISTRATION_AUTHORIZATION',
        'state': 'FROZEN_INACTIVE_NOT_REGISTERED', 'authority': r.AUTHORITY}, 'bundle_id')


def validate_bundle(bundle, root):
    r.profit.identity(bundle, 'bundle_id')
    if bundle != build_bundle(root):
        raise IntegrityError('successor binding changed')


def compatibility():
    """Synthetic dollar units only; never claims actual contract economics."""
    policy, checks = r.cost_policy(), []
    for market in r.MARKETS:
        row = {'market': market, 'trade_date': '2018-01-01', 'tick_value_nano_usd': 1250000000,
            'tick_nano': 250000000, 'decision_ns': 60*10**9, 'entry_ns': 120*10**9,
            'exit_ns': 3720*10**9, 'entry_open_nano': 100000000000, 'exit_open_nano': 102000000000,
            'opportunity_id': f'SYNTHETIC/{market}'}
        costs = [r.cost(row, policy, s) for s in r.SCENARIOS]
        assert costs[0] < costs[1] < costs[2]
        reports = {s: r.score_predictions([100.], [row], policy, [row['trade_date']], scenario=s) for s in r.SCENARIOS}
        assert all(x['executed_trades'] == 1 for x in reports.values())
        checks.append({'market': market, 'synthetic_tick_value_usd': '1.25',
            'costs_usd': dict(zip(r.SCENARIOS, map(str, costs))), 'cost_replay': 'PASSED',
            'edge_missing_history': edge.diagnostic(market, 'SYNTHETIC', 1, [])['reason'],
            'historical_units_verified': False})
    return checks


def synthetic_dataset(market):
    sessions, rows_by_date, diagnostics, history = [], {}, [], []
    for i in range(1026):
        day = (date(2018, 1, 1) + timedelta(days=i)).isoformat()
        session, rows, _ = prior.rich_synthetic_session(market, day, i)
        start = rows[0]['bar_start_ns']
        try:
            diagnostics.append(edge.diagnostic(market, rows[0]['actual_contract'], start, history[-20:]))
        except Exception as exc:
            diagnostics.append({'market': market, 'trade_date': day, 'state': 'DIAGNOSTIC_FAILED',
                'reason': f'{type(exc).__name__}: {exc}', 'selected_policy_effect': 'NONE'})
        history.append({'trade_date': day, 'complete_at_ns': rows[-1]['decision_eligible_at_ns'],
            'expected_bar_starts': [x['bar_start_ns'] for x in rows], 'rows': rows})
        sessions.append(session)
        rows_by_date[day] = rows
    return r.assemble_dataset(sessions, rows_by_date, policy=r.cost_policy(),
        as_of_ns=history[-1]['complete_at_ns']), diagnostics


def run_synthetic(root):
    bundle = json.loads((root / CONFIG).read_bytes())
    validate_bundle(bundle, root)
    output = root / EVIDENCE / bundle['bundle_id']
    write(output / 'compatibility.json', {'checks': compatibility(), 'market_year_metadata': bundle['metadata']})
    markets = {}
    for market in r.old.MARKETS:
        print(json.dumps({'market': market, 'state': 'SYNTHETIC_STARTED'}), flush=True)
        try:
            dataset, diagnostics = synthetic_dataset(market)
            write(output / market / 'edge.json', {'sessions': diagnostics, 'selected_policy_effect': 'NONE'})
            run = r.run_market_wfa(dataset)
            write(output / market / 'run.json', run)
            evaluation = r.evaluate_wfa(dataset, run)
            write(output / market / 'evaluation.json', evaluation)
            final = r.final_refit(dataset, run)
            write(output / market / 'final_model.json', final)
            loaded = json.loads((output / market / 'final_model.json').read_bytes())
            features = dataset.terminal_data.features
            predictions = r.predict(final['model'], features)
            np.testing.assert_array_equal(predictions, r.predict(loaded['model'], features))
            write(output / market / 'prediction_check.json', {'features': features.tolist(),
                'feature_names': list(r.old.terminal.FEATURE_NAMES), 'predictions': predictions.tolist(),
                'scope': 'ALL_SYNTHETIC_ROWS', 'final_id': final['final_id']})
            empty = [b['trade_date'] for b in dataset.batches if not b['records']]
            ledger = evaluation['reports']['selected']['adverse']
            empty_audit = sorted(set(empty) & set(ledger['daily_usd']))
            assert len(run['outer_folds']) == 8 and all(len(f['candidate_scores']) == 15 for f in run['outer_folds'])
            assert empty_audit and all(ledger['daily_usd'][d] == '0' for d in empty_audit)
            assert sorted(set(x['quartile'] for x in dataset.opportunities)) == [1, 2, 3, 4]
            assert max(ledger['daily_trade_counts'].values()) >= 2
            columns = {n for pack in r.old.PACKS for n in r.old.terminal.PACKS[pack]}
            assert all(np.ptp(features[:, r.old.terminal.FEATURE_NAMES.index(n)]) > 0 for n in columns)
            markets[market] = {'state': 'PASSED_SYNTHETIC_ENGINEERING', 'run_id': run['run_id'],
                'result_id': evaluation['result_id'], 'final_id': final['final_id'], 'rows': len(features),
                'dates': len(dataset.calendar_dates), 'empty_audit_dates': empty_audit,
                'diagnostic_refit_failures': {name: value for name, value in evaluation['reports'].items() if 'unavailable_folds' in value},
                'edge_dispositions': dict(Counter(x['state'] for x in diagnostics))}
        except Exception as exc:
            markets[market] = {'state': 'FAILED_SYNTHETIC', 'type': type(exc).__name__, 'reason': str(exc),
                'partial_run': getattr(exc, 'partial_run', None), 'replacement': False}
            write(output / market / 'failure.json', markets[market])
        print(json.dumps({'market': market, 'state': markets[market]['state']}), flush=True)
    passed = all(v['state'] == 'PASSED_SYNTHETIC_ENGINEERING' for v in markets.values())
    report = r.profit.bind({'bundle_id': bundle['bundle_id'], 'markets': markets,
        'state': 'PASSED_ENGINEERING_ONLY' if passed else 'FAILED_SYNTHETIC_ACCEPTANCE',
        'historical_evaluation': False, 'historical_model': False, 'predictive_usefulness': 'NOT_ESTABLISHED',
        'profitability_qualification': 'BLOCKED_PRESERVED_FAILED_TEST', 'authority': r.AUTHORITY}, 'report_id')
    write(output / 'report.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('freeze', 'validate', 'synthetic'))
    parser.add_argument('--root', type=Path, default=Path.cwd())
    args = parser.parse_args()
    if args.action == 'freeze':
        value = build_bundle(args.root)
        write(args.root / CONFIG, value)
        print(value['bundle_id'])
    elif args.action == 'validate':
        validate_bundle(json.loads((args.root / CONFIG).read_bytes()), args.root)
        print('VALID_INACTIVE_BINDING')
    else:
        print(json.dumps(run_synthetic(args.root)))


if __name__ == '__main__':
    main()
