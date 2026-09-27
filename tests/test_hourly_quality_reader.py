"""Focused window policy and activation-bound path protection."""
import pytest

from futures_rebuild import hourly_quality_reader as h
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation

pytestmark = pytest.mark.current
START = 1_704_067_200_000_000_000


def bar(start=START, **changes):
    return dict(dict(market='ES', publisher_id=1, instrument_id=2, actual_contract='ESH4',
                     currency='USD', min_price_increment_nano=250_000_000, multiplier_nano=50_000_000_000,
                     bar_start_ns=start, bar_end_ns=start + h.HOUR,
                     available_at_ns=start + h.HOUR + 5_000_000_000,
                     decision_eligible_at_ns=start + h.HOUR + 5_000_000_000,
                     origin='NATIVE_1H', quality_disposition='CORROBORATED_BY_FULL_MINUTES'), **changes)


def check(rows, **changes):
    args = dict(market='ES', start_ns=START, end_ns=START + 2*h.HOUR,
                as_of_ns=START + 2*h.HOUR + 5_000_000_000, source='native')
    args.update(changes)
    return h.validate_window(rows, **args)


def test_completed_dense_window_and_exact_boundary():
    rows = [bar(), bar(START + h.HOUR)]
    assert check(rows) == rows
    with pytest.raises(UnauthorizedOperation, match='not yet available'):
        check(rows, as_of_ns=rows[-1]['decision_eligible_at_ns'] - 1)


@pytest.mark.parametrize('rows', [[], [bar()], [bar(), bar()], [bar(START+h.HOUR), bar()],
    [bar(), bar(START+h.HOUR, actual_contract='ESM4')],
    [bar(), bar(START+h.HOUR, multiplier_nano=1)],
    [bar(), bar(START+h.HOUR, quality_disposition='QUARANTINE_UNRESOLVED')],
    [bar(), bar(START+h.HOUR, quality_disposition='UNKNOWN')]])
def test_reject_gap_duplicate_order_contract_units_and_unresolved(rows):
    with pytest.raises(UnauthorizedOperation):
        check(rows)


def test_explicit_derived_choice_keeps_sparse_coverage_visible():
    row = bar(origin='DERIVED_1H_FROM_RETAINED_1M',
              quality_disposition='CANDIDATE_OBSERVED_MINUTES_ONLY', minute_count=2,
              market_coverage='NOT_ESTABLISHED', full_minute_grid=False)
    with pytest.raises(UnauthorizedOperation):
        check([row], end_ns=START+h.HOUR)
    result = check([row], end_ns=START+h.HOUR, source='minute_derived')
    assert result[0]['minute_count'] == 2 and not result[0]['full_minute_grid']
    with pytest.raises(UnauthorizedOperation):
        check([bar()], end_ns=START+h.HOUR, source='minute_derived')


def test_refuse_invalid_time_and_availability():
    for args in ({'start_ns': True}, {'end_ns': START+h.HOUR+1}, {'end_ns': h.CUTOFF+h.HOUR}):
        with pytest.raises(ContractError):
            check([], **args)
    with pytest.raises(IntegrityError):
        check([bar(available_at_ns=START)], end_ns=START+h.HOUR)


def test_read_window_calls_policy_and_never_falls_back(monkeypatch):
    reader = object.__new__(h.HourlyReader)
    reader.derived = [dict(bar(market='ZQ', origin='DERIVED_1H_FROM_RETAINED_1M'),
                           disposition='CANDIDATE_OBSERVED_MINUTES_ONLY', constituent_row_ids=['first'])]
    monkeypatch.setattr(reader, '_unchanged', lambda: None)
    def unavailable(*args):
        raise UnauthorizedOperation('Native month unavailable')
    monkeypatch.setattr(reader, '_native_month', unavailable)
    args = dict(market='ZQ', start_ns=START, end_ns=START+h.HOUR, as_of_ns=START+2*h.HOUR)
    with pytest.raises(UnauthorizedOperation, match='Native month unavailable'):
        reader.read_window(**args)
    assert reader.read_window(**args, source='minute_derived')[0]['origin'] == 'DERIVED_1H_FROM_RETAINED_1M'
    returned = reader.read_window(**args, source='minute_derived')
    returned[0]['constituent_row_ids'].append('caller edit')
    assert reader.read_window(**args, source='minute_derived')[0]['constituent_row_ids'] == ['first']
    with pytest.raises(UnauthorizedOperation, match='no research or training'):
        reader.read_window(**args, source='minute_derived', purpose='TRAINING')


def test_active_pointer_closes_all_public_hourly_path_routes(tmp_path):
    from futures_rebuild import active_data_view, causal_timeframe_view, causal_timeframe_publication
    p = tmp_path / h.POINTER
    p.parent.mkdir()
    p.write_text('invalid pointer still fails closed')
    kwargs = dict(market='ES', year=2024, timeframe='1h', purpose='LINEAGE_AUDIT')
    # Shared invariant owner also protects direct calls to the publication resolver.
    with pytest.raises(UnauthorizedOperation, match='HourlyReader.read_window'):
        causal_timeframe_publication.resolve_partitions(tmp_path, {}, **kwargs)
    # The view routes validate the index before reaching the shared owner; exercise
    # that routing using a sentinel to avoid constructing unrelated publication data.
    from unittest.mock import patch
    index = {'schema': 'causal_timeframe_index/3'}
    with patch.object(causal_timeframe_view, 'read_record', return_value=index), \
         patch.object(causal_timeframe_publication, 'validate_index'):
        for resolver in (active_data_view.resolve_partitions, causal_timeframe_view.resolve_partitions):
            with pytest.raises(UnauthorizedOperation, match='HourlyReader.read_window'):
                resolver(repository_root=tmp_path, **kwargs)
    h.require_window_reader(tmp_path, '1m')
    h.require_window_reader(tmp_path, '1s')


def test_detect_evidence_mutation_between_reads(tmp_path):
    from futures_rebuild.canonical import sha256_file
    p = tmp_path / 'evidence.json'
    p.write_text('original')
    reader = object.__new__(h.HourlyReader)
    reader.root = tmp_path
    reader.bindings = {'evidence.json': sha256_file(p)}
    reader._unchanged()
    p.write_text('changed')
    with pytest.raises(IntegrityError, match='changed during use'):
        reader._unchanged()
