"""Generated source-admission policy and public-reader regression checks."""
from copy import deepcopy
import json

import pytest

from futures_rebuild import hourly_source_admission as admission
from futures_rebuild import hourly_quality_reader as quality
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.causal_timeframe_storage import signed
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation
from tests.test_hourly_quality_reader import bar

pytestmark = pytest.mark.current


def scope(year, market='ES', month=None, disposition='CERTIFIED', **changes):
    month = month or f'{year}-01'
    start = admission.timestamp(month + '-01')
    end = min(start + 24 * quality.HOUR, quality.CUTOFF)
    return dict(dict(timeframe='1h', market=market, month=month, disposition=disposition,
                     coverage_start_ns=start, coverage_end_ns=end), **changes)


def ceiling(year, market='ES', **changes):
    end = '2025-07-13T22:00:00Z' if year == 2025 else f'{year+1}-01-01'
    cohort = admission.minute.cohort_for_coverage(year=year, coverage_start=f'{year}-01-01', coverage_end=end)
    return dict(dict(market=market, year=year, disposition=admission.minute.CERTIFICATION_STATE,
        coverage_start=f'{year}-01-01', coverage_end=end, cohort=cohort,
        permitted_uses=list(admission.minute.COHORT_PERMISSIONS[cohort])), **changes)


@pytest.mark.parametrize('year,uses', [
    (2010, {'DATA_QUALITY', 'LINEAGE_AUDIT'}), (2011, {'FORMATION_CONTEXT', 'LINEAGE_AUDIT'}),
    (2012, {'FEED_STRESS', 'LINEAGE_AUDIT'}), (2017, {'FEED_STRESS', 'LINEAGE_AUDIT'}),
    (2018, {'DISCOVERY_RESEARCH', 'FEATURE_GENERATION', 'LINEAGE_AUDIT', 'SELECTION'}),
    (2023, {'NON_SELECTION_RESEARCH', 'FEATURE_GENERATION', 'LINEAGE_AUDIT'}),
    (2025, {'NON_SELECTION_RESEARCH', 'FEATURE_GENERATION', 'LINEAGE_AUDIT'})])
def test_existing_minute_cohort_ceiling(year, uses):
    entries = admission.policy_entries({'entries': [scope(year)]}, {'entries': [ceiling(year)]})
    assert set(entries[0]['permitted_uses']) == uses


def test_missing_ambiguous_unavailable_and_protected_policy():
    index = {'entries': [scope(2024)]}
    assert admission.policy_entries(index, {'entries': []})[0]['permitted_uses'] == []
    assert admission.policy_entries({'entries': [scope(2024, disposition='UNAVAILABLE')]},
                                    {'entries': [ceiling(2024)]})[0]['permitted_uses'] == []
    protected = ceiling(2025, disposition='LOCKED_HOLDOUT_NOT_MATERIALIZED', permitted_uses=[])
    assert admission.policy_entries({'entries': [scope(2025)]}, {'entries': [protected]})[0]['permitted_uses'] == []
    for bad_index, bad_catalog in (({'entries': [scope(2024)] * 2}, {'entries': [ceiling(2024)]}),
                                    (index, {'entries': [ceiling(2024)] * 2})):
        with pytest.raises(IntegrityError, match='Ambiguous'):
            admission.policy_entries(bad_index, bad_catalog)
    with pytest.raises(IntegrityError, match='cohort permission'):
        admission.policy_entries(index, {'entries': [ceiling(2024, permitted_uses=['SELECTION'])]})


def policy_reader(parts, ceilings):
    reader = object.__new__(admission.HourlySourceReader)
    entries = admission.policy_entries({'entries': parts}, {'entries': ceilings})
    reader._entries = {(e['market'], e['month']): e for e in entries}
    reader._unchanged = lambda: None
    return reader


def test_cross_cohort_intersection_and_partial_minute_coverage():
    boundary = admission.timestamp('2023-01-01')
    reader = policy_reader([
        scope(2022, month='2022-12', coverage_start_ns=boundary-quality.HOUR, coverage_end_ns=boundary),
        scope(2023)], [ceiling(2022), ceiling(2023)])
    uses = reader.permitted_uses(market='ES', start_ns=boundary-quality.HOUR, end_ns=boundary+quality.HOUR)
    assert set(uses) == {'FEATURE_GENERATION', 'LINEAGE_AUDIT'}
    partial = policy_reader([scope(2023)], [ceiling(2023, coverage_start='2023-01-01T01:00:00Z')])
    assert partial.permitted_uses(market='ES', start_ns=boundary, end_ns=boundary+quality.HOUR) == ()
    assert 'FEATURE_GENERATION' in partial.permitted_uses(market='ES', start_ns=boundary+quality.HOUR, end_ns=boundary+2*quality.HOUR)
    native_partial = policy_reader([scope(2023, coverage_start_ns=boundary+quality.HOUR)], [ceiling(2023)])
    assert native_partial.permitted_uses(market='ES', start_ns=boundary, end_ns=boundary+quality.HOUR) == ()


def test_cutoff_and_argument_validation():
    reader = policy_reader([scope(2025, month='2025-07', coverage_end_ns=quality.CUTOFF)], [ceiling(2025)])
    args = dict(market='ES', start_ns=quality.CUTOFF-quality.HOUR, end_ns=quality.CUTOFF)
    assert 'NON_SELECTION_RESEARCH' in reader.permitted_uses(**args)
    for changes in ({'end_ns': quality.CUTOFF+quality.HOUR}, {'start_ns': True},
                    {'start_ns': quality.CUTOFF-1}, {'source': 'auto'}):
        with pytest.raises(ContractError):
            reader.permitted_uses(**dict(args, **changes))


def real_window_reader(rows, derived=()):
    reader = object.__new__(quality.HourlyReader)
    reader._unchanged = lambda: None
    reader.derived = list(derived)
    def native(market, month):
        selected = [r for r in rows if r['market'] == market]
        if not selected:
            raise UnauthorizedOperation('Native hourly month is unavailable')
        return selected
    reader._native_month = native
    return reader


def test_public_admission_preserves_quality_enforcement_and_rows():
    reader = policy_reader([scope(2024)], [ceiling(2024)])
    first = bar()
    args = dict(market='ES', start_ns=first['bar_start_ns'], end_ns=first['bar_end_ns'],
                as_of_ns=first['decision_eligible_at_ns'], purpose='FEATURE_GENERATION')
    reader.quality_reader = real_window_reader([first])
    assert reader.read_window(**args) == [first]
    for purpose in ('SELECTION', 'TRAINING', 'TRADING', 'OUTCOME_LABEL_PREDICTION_ACCESS', 'PRE_2025_STATUS_DEPENDENT_USE'):
        with pytest.raises(UnauthorizedOperation, match='not admitted'):
            reader.read_window(**dict(args, purpose=purpose))
    for changed in (dict(quality_disposition='QUARANTINE_UNRESOLVED'), dict(quality_disposition='UNKNOWN')):
        reader.quality_reader = real_window_reader([dict(first, **changed)])
        with pytest.raises(UnauthorizedOperation, match='Unresolved native'):
            reader.read_window(**args)
    reader.quality_reader = real_window_reader([first, bar(first['bar_end_ns'], actual_contract='ESM4')])
    with pytest.raises(UnauthorizedOperation, match='contract or unit'):
        reader.read_window(**dict(args, end_ns=first['bar_end_ns']+quality.HOUR, as_of_ns=first['decision_eligible_at_ns']+quality.HOUR))
    with pytest.raises(UnauthorizedOperation, match='not yet available'):
        reader.read_window(**dict(args, as_of_ns=args['as_of_ns']-1))
    with pytest.raises(UnauthorizedOperation, match='coverage is absent'):
        reader.read_window(**dict(args, end_ns=first['bar_end_ns']+2*quality.HOUR, as_of_ns=args['as_of_ns']+3*quality.HOUR))


def test_derived_audit_only_and_no_automatic_native_substitution():
    reader = policy_reader([scope(2024, market='ZQ', disposition='UNAVAILABLE')], [ceiling(2024, market='ZQ')])
    row = dict(bar(market='ZQ', origin='DERIVED_1H_FROM_RETAINED_1M'),
               disposition='CANDIDATE_OBSERVED_MINUTES_ONLY', constituent_row_ids=['a'],
               market_coverage='NOT_ESTABLISHED', minute_count=1, full_minute_grid=False)
    reader.quality_reader = real_window_reader([], [row])
    args = dict(market='ZQ', start_ns=row['bar_start_ns'], end_ns=row['bar_end_ns'], as_of_ns=row['available_at_ns'])
    with pytest.raises(UnauthorizedOperation):
        reader.read_window(**args)
    with pytest.raises(UnauthorizedOperation, match='not admitted'):
        reader.read_window(**args, source='minute_derived', purpose='FEATURE_GENERATION')
    result = reader.read_window(**args, source='minute_derived')
    assert result[0]['market_coverage'] == 'NOT_ESTABLISHED' and result[0]['minute_count'] == 1


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    def write(relative, value):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical_bytes(value))
        return {'path': relative, 'sha256': sha256_file(path)}
    # Catalog certification and base-reader construction have their own suites.
    # This fixture isolates admission binding and activation checks.
    monkeypatch.setattr(admission.minute, 'validate_catalog', lambda catalog: None)
    monkeypatch.setattr(admission, 'HourlyReader', lambda root: real_window_reader([bar()]))
    bindings = {}
    for relative, value in [('data/active/catalog.json', {'entries': [ceiling(2024)]}),
            ('data/active/timeframes.json', signed({'entries': [scope(2024)]})),
            (admission.QUALITY_POINTER, signed({'test': True})),
            (admission.FOUNDATION+'/supervision.json', signed({'test': True})),
            (admission.FOUNDATION+'/execution/result.json', signed({'test': True}))]:
        bindings[relative] = write(relative, value)['sha256']
    for relative in admission.IMPLEMENTATION:
        bindings[relative] = write(relative, {'generated_implementation': True})['sha256']
    core = dict(schema='hourly_source_admission/1', quality_pointer=admission.QUALITY_POINTER,
                bindings=bindings, derived_permitted_uses=['LINEAGE_AUDIT'],
                entries=admission.policy_entries({'entries': [scope(2024)]}, {'entries': [ceiling(2024)]}))
    manifest = write('prepared/manifest.json', signed(core))
    result = write('prepared/result.json', signed(dict(status='PASS_FOUNDATION_WITH_COVERAGE_LIMITATIONS',
                       source_admission_manifest_sha256=manifest['sha256'])))
    supervision = write('prepared/supervision.json', signed(dict(status='PASS_SUPERVISED_FOUNDATION_AUDIT',
                          worker_result_sha256=result['sha256'])))
    pointer = signed(dict(schema='active_hourly_source_admission/1', manifest=manifest,
                          result=result, supervision=supervision))
    return tmp_path, write, core, pointer


def test_preview_does_not_activate_and_active_requires_passing_binding(prepared):
    root, write, core, pointer = prepared
    reader = admission.HourlySourceReader(root, preview_manifest='prepared/manifest.json')
    assert not (root / admission.POINTER).exists()
    with pytest.raises(IntegrityError, match='does not exist'):
        admission.HourlySourceReader(root)
    write(admission.POINTER, pointer)
    active = admission.HourlySourceReader(root)
    row = bar()
    args = dict(market='ES', start_ns=row['bar_start_ns'], end_ns=row['bar_end_ns'], as_of_ns=row['available_at_ns'])
    assert active.read_window(**args) == reader.read_window(**args)
    write(admission.QUALITY_POINTER, signed({'changed': True}))
    with pytest.raises(IntegrityError, match='binding changed'):
        active.read_window(**args)


@pytest.mark.parametrize('kind', ['entry', 'derived', 'binding', 'unsigned', 'validation'])
def test_changed_policy_or_incomplete_validation_fails_closed(prepared, kind):
    root, write, core, pointer = prepared
    if kind == 'validation':
        pointer['result'] = write('prepared/failed.json', signed({'status': 'INCOMPLETE'}))
        write(admission.POINTER, signed({k: v for k, v in pointer.items() if k != 'id'}))
        with pytest.raises(IntegrityError):
            admission.HourlySourceReader(root)
        return
    core = deepcopy(core)
    if kind == 'entry': core['entries'][0]['permitted_uses'].append('SELECTION')
    elif kind == 'derived': core['derived_permitted_uses'].append('FEATURE_GENERATION')
    elif kind == 'binding': core['bindings'][admission.QUALITY_POINTER] = '0' * 64
    write('prepared/manifest.json', core if kind == 'unsigned' else signed(core))
    with pytest.raises(IntegrityError):
        admission.HourlySourceReader(root, preview_manifest='prepared/manifest.json')


@pytest.mark.parametrize('failure', [None, 'post_read', 'post_read_existing', 'predecessor', 'audit', 'approval'])
def test_conditional_publication_and_exact_pointer_rollback(prepared, monkeypatch, failure):
    from contextlib import nullcontext
    from futures_rebuild import hourly_foundation_audit as audit, causal_timeframe_execution
    root, write, core, original_pointer = prepared
    monkeypatch.setattr(audit, 'authorize', lambda *args: None)
    monkeypatch.setattr(audit, 'verify_bindings', lambda *args: None)
    monkeypatch.setattr(causal_timeframe_execution, 'execution_lease', lambda root: nullcontext())
    monkeypatch.setattr(audit, 'check_raw_routes', lambda *args: ['publication', 'timeframe_view', 'active_data_view'])
    plan = signed(dict(source_admission_manifest='prepared/manifest.json',
                       source_admission_manifest_sha256=sha256_file(root/'prepared/manifest.json')))
    write('prepared/plan.json', plan)
    prior_hash = None
    if failure == 'post_read_existing':
        prior_hash = write(admission.POINTER, original_pointer)['sha256']
    proposal = signed(dict(audit_plan='prepared/plan.json', manifest='prepared/manifest.json',
                           destination=admission.POINTER, expected_destination_sha256=prior_hash))
    write('prepared/proposal.json', proposal)
    write('prepared/authorization.json', {'publication_proposal_id': proposal['id'] if failure != 'approval' else 'wrong'})
    row = bar()
    args = dict(market='ES', start_ns=row['bar_start_ns'], end_ns=row['bar_end_ns'], as_of_ns=row['available_at_ns'])
    folder = audit.HERE + '/' + plan['id']
    result = write(folder+'/execution/result.json', signed(dict(plan_id=plan['id'],
        status='INCOMPLETE' if failure == 'audit' else 'PASS_FOUNDATION_WITH_COVERAGE_LIMITATIONS',
        source_admission_manifest_sha256=plan['source_admission_manifest_sha256'],
        actual_window_reads=[dict(case='native', request=args, result='ACCEPTED')])) )
    write(folder+'/supervision.json', signed(dict(status='PASS_SUPERVISED_FOUNDATION_AUDIT',
          plan_id=plan['id'], worker_result_sha256=result['sha256'])))
    if failure in ('post_read', 'post_read_existing'):
        def broken(*args, **kwargs):
            raise IntegrityError('injected active reader failure')
        monkeypatch.setattr(admission, 'HourlySourceReader', broken)
    if failure == 'predecessor':
        write(admission.POINTER, original_pointer)
    if failure:
        with pytest.raises((IntegrityError, UnauthorizedOperation)):
            admission.publish_prepared(root, 'prepared/proposal.json', 'prepared/authorization.json')
        assert (root/admission.POINTER).exists() is (failure in ('predecessor', 'post_read_existing'))
        if prior_hash:
            assert sha256_file(root/admission.POINTER) == prior_hash
        assert (root/admission.QUALITY_POINTER).exists()
    else:
        assert admission.publish_prepared(root, 'prepared/proposal.json', 'prepared/authorization.json')['status'] == 'ACTIVE_VERIFIED'
        assert admission.HourlySourceReader(root).read_window(**args) == [row]
