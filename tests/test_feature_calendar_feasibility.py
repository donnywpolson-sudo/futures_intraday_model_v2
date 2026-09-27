"""Synthetic-only checks: no retained calendars, market rows or active readers."""
from datetime import datetime
from hashlib import sha256
import io
import time
from zipfile import ZipFile

import pytest

from futures_rebuild import feature_calendar_feasibility as f
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.causal_timeframe_storage import signed
from futures_rebuild.cme_calendar_source_adequacy import ScheduleEvent, FamilySchedule
from futures_rebuild.errors import IntegrityError

pytestmark = pytest.mark.current


def event(at, kind='CLOSE', column=1):
    return ScheduleEvent(datetime.fromisoformat(at), column, kind, 'synthetic')


def segment(a, b, state='CLOSED', known=0, evidence='one'):
    return dict(start_ns=a, end_ns=b, state=state, available_at_ns=known, evidence_id=evidence)


def test_no_terminal_extension_even_all_day():
    assert f.event_segments([event('2020-01-01T00:00:00', 'CLOSED_ALL_DAY')]) == []
    pieces = f.event_segments([event('2020-01-01T12:00:00', 'OPEN'), event('2020-01-01T13:00:00')])
    assert len(pieces) == 1 and pieces[0]['state'] == 'OPEN'
    assert pieces[0]['end_ns']-pieces[0]['start_ns'] == f.HOUR


@pytest.mark.parametrize('wall', ['2020-03-08T02:30:00', '2020-11-01T01:30:00'])
def test_dst_ambiguous_and_nonexistent_fail(wall):
    with pytest.raises(IntegrityError, match='ambiguous or nonexistent'):
        f.event_segments([event(wall)])


@pytest.mark.parametrize('start,end,hours', [
    ('2020-03-07T17:00:00', '2020-03-08T17:00:00', 23),
    ('2020-10-31T17:00:00', '2020-11-01T17:00:00', 25),
    ('2020-07-03T12:00:00', '2020-07-05T17:00:00', 53),
])
def test_explicit_closures_preserve_elapsed_dst_time(start, end, hours):
    pieces = f.event_segments([event(start), event(end, 'OPEN', 2)])
    assert len(pieces) == 1 and pieces[0]['state'] == 'CLOSED'
    assert pieces[0]['end_ns']-pieces[0]['start_ns'] == hours*f.HOUR


def test_simultaneous_contradictory_events_do_not_choose_last():
    pieces = f.event_segments([event('2020-01-01T12:00:00'),
        event('2020-01-01T12:00:00', 'OPEN', 2), event('2020-01-01T13:00:00', 'OPEN', 3)])
    assert pieces[0]['state'] == 'CONFLICTING'


def test_union_duplicate_overlap_and_half_open_boundary():
    parts = f.interval_partition([segment(0, 7), segment(4, 10, evidence='two'),
        segment(10, 15, 'OPEN')], start_ns=0, end_ns=10, as_of_ns=10)
    assert sum(p['end_ns']-p['start_ns'] for p in parts if p['state'] == 'CLOSED') == 10
    assert parts[1]['evidence_ids'] == ['one', 'two']


def test_conflict_is_not_closed_and_partial_day_not_complete():
    parts = f.interval_partition([segment(2, 8), segment(4, 5, 'OPEN')],
        start_ns=0, end_ns=10, as_of_ns=10)
    assert [p['state'] for p in parts] == ['UNKNOWN', 'CLOSED', 'CONFLICTING', 'CLOSED', 'UNKNOWN']


@pytest.mark.parametrize('known,asof,expected', [(None, 15, 'UNKNOWN'), (16, 15, 'UNKNOWN'),
    (15, 15, 'CLOSED'), (12, 15, 'CLOSED')])
def test_availability_is_decision_relative(known, asof, expected):
    # The closure ended at ten; evidence first captured at twelve is usable at fifteen.
    parts = f.interval_partition([segment(0, 10, known=known)], start_ns=0, end_ns=10, as_of_ns=asof)
    assert parts[0]['state'] == expected


@pytest.mark.parametrize('change', [dict(start_ns=True), dict(end_ns=0), dict(available_at_ns=True),
    dict(state='ASSUMED_CLOSED'), dict(evidence_id='')])
def test_invalid_evidence_fails(change):
    with pytest.raises(IntegrityError):
        f.interval_partition([segment(0, 10) | change], start_ns=0, end_ns=10, as_of_ns=15)


def test_matrix_keeps_all_anchors_and_has_no_product_authority(monkeypatch):
    monkeypatch.setattr(f, 'LEFT', 2*f.DAY)
    monkeypatch.setattr(f, 'RIGHT', 4*f.DAY)
    rows = list(f.closure_candidate_matrix({'EQUITY_INDEX': [segment(2*f.DAY, 3*f.DAY)]}, ['ES', 'CL']))
    assert len(rows) == 4
    assert all(sum(x['candidate_state_ns'].values()) == f.DAY for x in rows)
    assert all(x['newly_readable_windows'] is None and not x['product_closure_authority'] for x in rows)
    assert rows[0]['candidate_state_ns'] == {'UNKNOWN': f.DAY}
    assert rows[2]['candidate_state_ns'] == {'CLOSED': f.DAY}


def put(path, obj):
    raw = canonical_bytes(obj)+b'\n'
    path.write_bytes(raw)
    return dict(path=str(path), sha256=sha256(raw).hexdigest(), size_bytes=len(raw))


def decode_scope(tmp_path, source_data, suffix='.xls'):
    path = tmp_path/('source'+suffix)
    path.write_bytes(source_data)
    source = dict(path=str(path), sha256=sha256_file(path), size_bytes=len(source_data))
    return dict(calendar_files=[source],
        timing=put(tmp_path/'timing.json', {'results': []}),
        numeric=put(tmp_path/'numeric.json', {'parsed_schedules': []}))


def test_source_tampering_rejected_before_decode(tmp_path):
    scope = decode_scope(tmp_path, b'fake')
    (tmp_path/'source.xls').write_bytes(b'evil')
    with pytest.raises(IntegrityError, match='Prepared input changed'):
        f.decode_sources(scope)


def test_duplicate_zip_members_rejected(tmp_path):
    buf = io.BytesIO()
    with ZipFile(buf, 'w') as z:
        z.writestr('duplicate.xls', b'fake')
        with pytest.warns(UserWarning):
            z.writestr('duplicate.xls', b'fake')
    with pytest.raises(IntegrityError, match='Duplicate workbook'):
        f.decode_sources(decode_scope(tmp_path, buf.getvalue(), '.zip'))


def test_parser_result_and_member_hash_must_match_retained(tmp_path, monkeypatch):
    scope = decode_scope(tmp_path, b'fake')
    monkeypatch.setattr(f, 'read_xls_bytes', lambda _: [])
    parsed = FamilySchedule('sheet', 1, 'Equity', (),
        (event('2020-01-01T12:00:00'), event('2020-01-01T13:00:00', 'OPEN', 2)))
    monkeypatch.setattr(f, 'parse_family_schedule', lambda *a, **kw: parsed)
    monkeypatch.setattr(f, 'FAMILY_PATTERN', {'EQUITY_INDEX': None})
    scope['numeric'] = put(tmp_path/'numeric.json', {'parsed_schedules': [dict(
        source=str(tmp_path/'source.xls'), member='source.xls', family='EQUITY_INDEX',
        events=[], member_sha256='0'*64)]})
    with pytest.raises(IntegrityError, match='Retained parser evidence changed'):
        f.decode_sources(scope)


def test_hash_named_copy_retains_original_availability_and_replay_identity(tmp_path, monkeypatch):
    from dataclasses import asdict
    scope = decode_scope(tmp_path, b'fake')
    source = scope['calendar_files'][0]
    original = tmp_path/'registered'/'2018_new_year.xls'
    source['registered_path'] = str(original)
    monkeypatch.setattr(f, 'read_xls_bytes', lambda _: [])
    parsed = FamilySchedule('sheet', 1, 'Equity', (),
        (event('2018-01-01T12:00:00'), event('2018-01-02T13:00:00', 'OPEN', 2)))
    monkeypatch.setattr(f, 'parse_family_schedule', lambda *a, **kw: parsed)
    monkeypatch.setattr(f, 'FAMILY_PATTERN', {'EQUITY_INDEX': None})
    scope['timing'] = put(tmp_path/'timing.json', {'results': [dict(
        source_key='2018_new_year', member=original.name, family='EQUITY_INDEX',
        cdx_content_digest_matches=True, recorded_capture_utc='2018-01-06T00:00:00+00:00')]})
    scope['numeric'] = put(tmp_path/'numeric.json', {'parsed_schedules': [dict(
        source=str(original), member=original.name, family='EQUITY_INDEX',
        events=[asdict(e) | {'at': e.at.isoformat()} for e in parsed.events],
        member_sha256=source['sha256'])]})
    records, failures = f.decode_sources(scope)
    assert not failures and len(records) == 1
    assert records[0]['member'] == original.name
    assert records[0]['registered_source'] == str(original)
    assert records[0]['available_at_ns'] == 1515196800*f.SECOND
    # A changed retained replay must still be rejected after physical relocation.
    scope['numeric'] = put(tmp_path/'numeric.json', {'parsed_schedules': [dict(
        source=str(original), member=original.name, family='EQUITY_INDEX',
        events=[], member_sha256=source['sha256'])]})
    with pytest.raises(IntegrityError, match='Retained parser evidence changed'):
        f.decode_sources(scope)


def test_relocation_requires_exact_unique_path_hash_and_existing_bytes(tmp_path):
    item = put(tmp_path/'copy.json', {'synthetic': True}) | {'registered_path': str(tmp_path/'old.json')}
    assert f.relocated_input(item['registered_path'], item['sha256'], [item])['path'] == item['path']
    for path, digest, entries in [(str(tmp_path/'different.json'), item['sha256'], [item]),
        (item['registered_path'], '0'*64, [item]), (item['registered_path'], item['sha256'], [item,item])]:
        with pytest.raises(IntegrityError, match='Missing or ambiguous'):
            f.relocated_input(path, digest, entries)
    put(tmp_path/'copy.json', {'changed': True})
    with pytest.raises(IntegrityError, match='Prepared input changed'):
        f.relocated_input(item['registered_path'], item['sha256'], [item])


def approval_fixture(tmp_path):
    import time
    scope = signed(dict(schema='feature_calendar_feasibility_scope/1', status='PREPARED_REQUIRES_OWNER_APPROVAL',
        operation='CALENDAR_EVIDENCE_DIAGNOSTIC_ONLY',
        price_reads=False, dbn_decode=False, network=False, publication=False, pointer_changes=[],
        limits={'maximum_seconds': 1800}, implementation=[], controls=[], metadata=[]))
    put(tmp_path/'scope.json', scope)
    auth = signed(dict(status='OWNER_APPROVED_IN_THIS_TASK', operation=scope['operation'],
        scope_sha256=sha256_file(tmp_path/'scope.json'), scope_id=scope['id'],
        started_epoch=time.time()-1, deadline_epoch=time.time()+60))
    put(tmp_path/'auth.json', auth)
    return scope, auth


def test_missing_or_tampered_authority_refused(tmp_path):
    scope, auth = approval_fixture(tmp_path)
    assert f.approved_scope(tmp_path, 'scope.json', 'auth.json')[0] == scope
    put(tmp_path/'scope.json', scope | {'publication': True})
    with pytest.raises(IntegrityError):
        f.approved_scope(tmp_path, 'scope.json', 'auth.json')


def test_changed_control_prevents_diagnostic(tmp_path):
    scope, auth = approval_fixture(tmp_path)
    control = put(tmp_path/'control.json', {'unchanged': True})
    scope = signed({k:v for k,v in scope.items() if k != 'id'} | {'controls': [control]})
    put(tmp_path/'scope.json', scope)
    auth = signed({k:v for k,v in auth.items() if k != 'id'} | dict(
        scope_sha256=sha256_file(tmp_path/'scope.json'), scope_id=scope['id']))
    put(tmp_path/'auth.json', auth)
    put(tmp_path/'control.json', {'unchanged': False})
    with pytest.raises(IntegrityError, match='Prepared input changed'):
        f.approved_scope(tmp_path, 'scope.json', 'auth.json')


def test_blocked_inventory_cannot_be_approved_into_execution(tmp_path):
    scope, auth = approval_fixture(tmp_path)
    scope = signed({k:v for k,v in scope.items() if k != 'id'} |
        {'status': 'BLOCKED_MISSING_REGISTERED_INPUTS', 'missing_inputs': [{'path': 'absent.xls'}]})
    put(tmp_path/'scope.json', scope)
    auth = signed({k:v for k,v in auth.items() if k != 'id'} | dict(
        scope_sha256=sha256_file(tmp_path/'scope.json'), scope_id=scope['id']))
    put(tmp_path/'auth.json', auth)
    with pytest.raises(IntegrityError, match='scope expanded'):
        f.approved_scope(tmp_path, 'scope.json', 'auth.json')


def test_full_synthetic_matrix_population():
    # Whole 41-market grid using generated evidence, not historic calendars.
    started = time.perf_counter()
    generated = {family: [segment(f.LEFT, f.LEFT+f.DAY)] for family in set(f.MARKET_FAMILY.values())}
    rows = sum(1 for _ in f.closure_candidate_matrix(generated, list(f.MARKET_FAMILY)))
    assert rows == 74866
    print(f'Full synthetic matrix: {rows} rows in {time.perf_counter()-started:.3f}s')


def test_missing_approval_worker_retains_failure_resources(tmp_path, monkeypatch):
    import sys
    from types import SimpleNamespace
    from futures_rebuild.causal_timeframe_storage import read_record
    monkeypatch.chdir(tmp_path)
    (tmp_path/f.HERE).mkdir(parents=True)
    monkeypatch.setattr(sys, 'stdin', SimpleNamespace(buffer=io.BytesIO(canonical_bytes(
        dict(scope='absent.json', authorization='absent_auth.json'))+b'\n')))
    assert f.worker() == 1
    result = read_record(tmp_path/f.HERE/'execution/result.json')
    assert result['status'] == 'FAILED_CALENDAR_FEASIBILITY'
    assert result['phase'] == 'approval_and_bindings'
    assert result['memory']['peak_bytes'] > 0
    assert result['resources_before_terminal']['application_written_bytes'] == 0


def test_coordinator_failure_records_resources_without_retry(tmp_path):
    from futures_rebuild.causal_timeframe_storage import read_record
    (tmp_path/f.HERE).mkdir(parents=True)
    with pytest.raises(IntegrityError, match='required file does not exist'):
        f.run_approved(tmp_path, scope_path='absent.json', authorization_path='auth.json')
    path = tmp_path/f.HERE/'execution/operation_terminal.json'
    result = read_record(path)
    assert result['status'] == 'FAILED_CALENDAR_DIAGNOSTIC'
    assert result['coordinator_memory']['peak_bytes'] > 0
    before = path.read_bytes()
    with pytest.raises(IntegrityError, match='no automatic retry'):
        f.run_approved(tmp_path, scope_path='absent.json', authorization_path='auth.json')
    assert path.read_bytes() == before
