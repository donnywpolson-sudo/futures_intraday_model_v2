"""Freeze/validate and generated-data acceptance. No historical execution CLI."""
import argparse
from datetime import date, timedelta
import json
from pathlib import Path

import numpy as np

from . import rolling_hour_predictive as r
from . import rolling_hour_assumed_preparation as predecessor
from .canonical import sha256_file, write_new_canonical_authority as write
from .errors import IntegrityError

CONFIG = 'configs/rolling_hour_research_v9.json'
EVIDENCE = 'state/unpublished_evidence/rolling_hour_predictive_v1'
FILES = ('src/futures_rebuild/rolling_hour_predictive.py',
         'src/futures_rebuild/rolling_hour_predictive_preparation.py', 'tests/test_rolling_hour_predictive.py')


def build_bundle(root):
    previous = json.loads((root / predecessor.CONFIG).read_bytes())
    predecessor.validate_bundle(previous, root)
    decision_path = 'configs/rolling_hour_descriptive_use_decision_v1.json'
    decision = json.loads((root / decision_path).read_bytes())
    disclosure = decision['legacy_disclosure']
    specification = r.design()
    return r.old.profit.bind({'schema_version': 'predictive_preparation/1', 'specification': specification,
        'predecessor_bundle_id': previous['bundle_id'],
        'predecessor_file_sha256': sha256_file(root / predecessor.CONFIG),
        'implementation_files': {p: sha256_file(root / p) for p in sorted(set(FILES) | set(previous['implementation_files']))},
        'runtime_versions': predecessor.prior.failed_validation.runtime_versions(),
        'accounting': {'new_configuration_ids': [f"{specification['mechanism_id']}/{m}/{c['candidate_id']}"
            for m in r.old.MARKETS for c in r.old.candidate_definitions()], 'new_configuration_charges': 60,
            'registered': False, 'repeated_preparation_is_new_trial': False,
            'historical_search_upper_bound_established': False},
        'preserved_accounting': previous['accounting'],
        'historical_use': {'predecessor_decision_path': decision_path,
            'predecessor_decision_sha256': sha256_file(root / decision_path),
            'unresolved_legacy_references': disclosure['missing_reference_count'],
            'retained_legacy_penalty': disclosure['preserved_penalty'],
            'successor_decision': 'REQUIRES_GATEWAY_BINDING;NOT_INHERITED_AUTHORITY'},
        'historical_execution': 'BLOCKED_SOURCE_TIMING_CALENDAR_CERTIFICATION_AND_GATEWAY_BINDING',
        'state': 'FROZEN_INACTIVE_NOT_REGISTERED', 'authority': r.old.AUTHORITY}, 'bundle_id')


def validate_bundle(root):
    bundle = json.loads((root / CONFIG).read_bytes())
    r.old.profit.identity(bundle, 'bundle_id')
    if bundle != build_bundle(root):
        raise IntegrityError('predictive binding changed')
    return bundle


def synthetic_dataset(market):
    sessions, rows_by_date = [], {}
    for i in range(1026):
        day = (date(2018, 1, 1) + timedelta(days=i)).isoformat()
        session, rows, _ = predecessor.prior.rich_synthetic_session(market, day, i)
        sessions.append(session)
        rows_by_date[day] = rows
    return r.assemble_dataset(sessions, rows_by_date, as_of_ns=rows[-1]['decision_eligible_at_ns'])


def run_synthetic(root):
    bundle = validate_bundle(root)
    output = root / EVIDENCE / bundle['bundle_id']
    results = {}
    for market in r.old.MARKETS:
        print(f'{market}: generated-data nested procedure', flush=True)
        dataset = synthetic_dataset(market)
        write(output / market / 'opportunities.json', {'evidence_id': dataset._evidence_id,
            'calendar_dates': list(dataset.calendar_dates), 'batches': list(dataset.batches),
            'source': 'GENERATED_ONLY'})
        try:
            run = r.run_market_wfa(dataset)
            write(output / market / 'run.json', run)
            final = r.final_refit(dataset, run)
            write(output / market / 'final_model.json', final)
            reloaded = json.loads((output / market / 'final_model.json').read_bytes())
            features = np.array([[record['feature']['feature_values'][name] for name in r.old.terminal.FEATURE_NAMES]
                                 for record in dataset.forecast_records])
            expected = r.predict(final['model'], features)
            np.testing.assert_array_equal(expected, r.predict(reloaded['model'], features))
            write(output / market / 'final_predictions.json', {
                'forecast_ids': [record['schedule']['opportunity_id'] for record in dataset.forecast_records],
                'predictions': expected.tolist(), 'final_id': final['final_id']})
            results[market] = {'status': 'SYNTHETIC_PASSED', 'observed_rows': len(dataset.opportunities),
                'forecast_rows': len(dataset.forecast_records), 'dates': len(dataset.calendar_dates),
                'outer_folds': len(run['outer_folds']), 'candidate_results': sum(len(f['candidate_scores']) for f in run['outer_folds']),
                'reload_predictions': 'EXACT', 'evaluation': r.evaluate_wfa(run)}
        except Exception as exc:
            results[market] = {'status': 'SYNTHETIC_FAILED', 'error': f'{type(exc).__name__}: {exc}'}
            write(output / market / 'failure.json', {**results[market], 'partial_run': getattr(exc, 'partial_run', None)})
        print(f"{market}: {results[market]['status']}", flush=True)
    report = {'bundle_id': bundle['bundle_id'], 'markets': results,
        'historical_data_read': False, 'historical_models_created': False,
        'status': 'SYNTHETIC_PASSED' if all(v['status'] == 'SYNTHETIC_PASSED' for v in results.values()) else 'SYNTHETIC_FAILED'}
    write(output / 'report.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('freeze', 'validate', 'synthetic'))
    args = parser.parse_args()
    root = Path.cwd()
    if args.action == 'freeze':
        bundle = build_bundle(root)
        write(root / CONFIG, bundle)
        print(bundle['bundle_id'])
    elif args.action == 'validate':
        print(validate_bundle(root)['bundle_id'])
    else:
        report = run_synthetic(root)
        print(json.dumps(report, indent=2))
        if report['status'] != 'SYNTHETIC_PASSED':
            raise SystemExit(1)


if __name__ == '__main__':
    main()
