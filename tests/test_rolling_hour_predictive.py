from copy import deepcopy
from datetime import date, timedelta
from types import SimpleNamespace

import numpy as np
import pytest

from futures_rebuild import rolling_hour_predictive as r
from futures_rebuild import rolling_hour_preparation as prior
from futures_rebuild.errors import IntegrityError

pytestmark = pytest.mark.current


def fixture():
    s, rows, _ = prior.rich_synthetic_session('ES', '2018-01-01', 1)
    return s, rows


def assemble(s, rows):
    return r.assemble_dataset([s], {s.trade_date.isoformat(): rows},
        as_of_ns=max(x['decision_eligible_at_ns'] for x in rows))


def test_endpoint_label_and_forecast_survive_missing_future_path():
    s, rows = fixture()
    before = assemble(s, rows)
    scheduled = before.forecast_records[0]['schedule']
    after = assemble(s, [x for x in rows if x['bar_start_ns'] != scheduled['entry_ns'] + r.old.MINUTE])
    assert before.forecast_records[0] == after.forecast_records[0]
    missing_exit = assemble(s, [x for x in rows if x['bar_start_ns'] != scheduled['exit_ns']])
    assert missing_exit.forecast_records[0]['feature'] == before.forecast_records[0]['feature']
    assert missing_exit.forecast_records[0]['status'] == 'MISSING_ENDPOINT'
    assert len(missing_exit.forecast_records) == len(before.forecast_records)


def test_roll_never_stitches_and_history_requires_same_contract():
    s, rows = fixture()
    original = assemble(s, rows)
    scheduled = original.forecast_records[0]['schedule']
    changed = deepcopy(rows)
    for row in changed:
        if row['bar_start_ns'] >= scheduled['entry_ns']:
            row['actual_contract'] = 'OTHER'
    after = assemble(s, changed)
    assert after.forecast_records[0]['feature'] == original.forecast_records[0]['feature']
    assert after.forecast_records[0]['status'] == 'CONTRACT_MISMATCH'
    changed[10]['actual_contract'] = 'OTHER'
    assert assemble(s, changed).batches[0]['records'][0]['status'] == 'INVALID_FEATURE'


def test_future_labels_do_not_change_features_and_unmatured_not_inspected():
    s, rows = fixture()
    d = assemble(s, rows)
    record = d.forecast_records[0]
    outcome = r.build_outcome([{'unreadable': True}], record['schedule'],
        expected_contract=record['feature']['actual_contract'], expected_tick=record['feature']['tick_nano'],
        as_of_ns=record['schedule']['decision_ns'])
    assert outcome['status'] == 'UNMATURED' and outcome['fit_target'] is None
    rows[0]['decision_eligible_at_ns'] = rows[-1]['decision_eligible_at_ns']
    assert assemble(s, rows).batches[0]['records'][0]['status'] == 'INVALID_FEATURE'


def test_equal_date_mse_and_exact_deterministic_tie():
    daily = r.daily_mse([2, 2, 4], [0, 0, 0], ['a', 'a', 'c'])
    assert daily == {'a': 4., 'c': 16.} and np.mean(list(daily.values())) == 10.
    scores = [{'candidate_id': name, 'status': 'VALID', 'score': 1.} for name in ['b', 'a']]
    assert r.choose_candidate(scores)['candidate_id'] == 'a'
    scores[0]['score'] -= 1e-12
    assert r.choose_candidate(scores)['candidate_id'] == 'b'
    with pytest.raises(IntegrityError):
        r.daily_mse([], [], [])


def test_empty_dates_retained_and_real_source_rejected():
    s, rows = fixture()
    d = r.assemble_dataset([s], {s.trade_date.isoformat(): []}, as_of_ns=rows[-1]['decision_eligible_at_ns'])
    assert len(d.calendar_dates) == 1 and len(d.forecast_records) == 0
    assert all(x['status'] == 'INVALID_FEATURE' for x in d.batches[0]['records'])
    rows[0]['source_release_id'] = 'b' * 64
    with pytest.raises(IntegrityError, match='not certified'):
        assemble(s, rows)


def test_mutation_and_duplicate_bar_rejected():
    s, rows = fixture()
    with pytest.raises(IntegrityError, match='duplicate'):
        assemble(s, rows + [rows[0]])
    d = assemble(s, rows)
    d.batches[0]['records'][0]['feature']['quartile'] = 99
    with pytest.raises(IntegrityError, match='mutated'):
        d.verify()


def test_late_training_labels_removed_before_support_check():
    dates = tuple((date(2018, 1, 1) + timedelta(days=i)).isoformat() for i in range(260))
    d = SimpleNamespace(calendar_dates=dates, terminal_data=SimpleNamespace(trade_dates=dates),
        forecast_records=({'schedule': {'trade_date': dates[-1], 'decision_ns': 100}},),
        opportunities=[{'target_available_at_ns': 1} for _ in dates])
    d.opportunities[0]['target_available_at_ns'] = 100
    window = SimpleNamespace(start=259, stop=260)
    selected = r.mature_fit_indices(d, np.arange(253), window, fit_kind='inner')
    assert len(selected) == 252 and 0 not in selected
    d.opportunities[1]['target_available_at_ns'] = 101
    with pytest.raises(IntegrityError):
        r.mature_fit_indices(d, np.arange(253), window, fit_kind='inner')


def test_tick_mismatch_is_unobservable_not_rescaled():
    s, rows = fixture()
    d = assemble(s, rows)
    record = d.forecast_records[0]
    result = r.build_outcome(rows, record['schedule'], expected_contract=record['feature']['actual_contract'],
        expected_tick=record['feature']['tick_nano'] * 2, as_of_ns=rows[-1]['decision_eligible_at_ns'])
    assert result['status'] == 'INVALID_ENDPOINT' and result['fit_target'] is None
