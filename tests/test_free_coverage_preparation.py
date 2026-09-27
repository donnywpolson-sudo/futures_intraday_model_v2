"""Generated fixtures only: no DBN/price payloads or active reader construction."""
import pytest

from futures_rebuild import free_coverage_preparation as f
from futures_rebuild.errors import IntegrityError

pytestmark = pytest.mark.current


def claim(**changes):
    return dict(id='test', status='BASELINE_RULE_AND_EXACT_CODES_SUPPORTED',
        timezone='America/Chicago', available_by_utc='2019-01-01T00:00:00Z',
        effective_from_local='2019-01-01T00:00:00',
        open_rule=dict(start_days=list(f.WEEKDAYS[6:])+list(f.WEEKDAYS[:4]),
                       start='17:00', end_next_day='16:00')) | changes


def seg(a, b, state='CLOSED', known=0, evidence='one'):
    return dict(start_ns=a, end_ns=b, state=state, available_at_ns=known, evidence_id=evidence)


@pytest.mark.parametrize('end,admitted,expected', [
    ('2025-07-13T22:00:00Z', True, 'ELIGIBLE_FOR_SEPARATELY_APPROVED_DIAGNOSTIC'),
    ('2025-07-13T22:00:01Z', True, 'REJECT_WHOLE_FILE_PROTECTED_BOUNDARY'),
    ('2026-01-01', True, 'REJECT_WHOLE_FILE_PROTECTED_BOUNDARY'),
    ('2025-01-01', False, 'INVENTORY_ONLY_SEPARATE_SOURCE_AUTHORITY_REQUIRED')])
def test_whole_file_rejection_before_open(end, admitted, expected):
    entry = dict(interval_start_inclusive='2024-01-01', interval_end_exclusive=end,
                 family='ohlcv_1m', admitted_standard_foundation=admitted)
    assert f.source_disposition(entry, f.ns('2025-07-13T22:00:00Z'), ['ohlcv_1m']) == expected


def test_unknown_or_reversed_interval_and_forbidden_family():
    entry = dict(interval_start_inclusive='2024-01-01', interval_end_exclusive='invalid', family='trades')
    assert f.source_disposition(entry, f.RIGHT, []) == 'REJECT_UNKNOWN_INTERVAL'
    entry['interval_end_exclusive'] = '2023-01-01'
    assert f.source_disposition(entry, f.RIGHT, []) == 'REJECT_INVALID_INTERVAL'
    entry.update(interval_start_inclusive='2020-01-01', interval_end_exclusive='2021-01-01')
    assert f.source_disposition(entry, f.RIGHT, []) == 'REJECT_UNDECLARED_FAMILY'


@pytest.mark.parametrize('start,end,hours', [
    ('2020-03-06T22:00:00Z','2020-03-08T22:00:00Z',48),
    ('2020-10-30T21:00:00Z','2020-11-01T23:00:00Z',50)])
def test_dst_weekend_real_elapsed_boundaries(start, end, hours):
    left, right = f.ns(start), f.ns(end)
    pieces = f.recurring_segments(claim(), left=left, right=right)
    assert all(x['state'] == 'CLOSED' for x in pieces)
    assert sum(x['end_ns']-x['start_ns'] for x in pieces) == hours*f.HOUR


def test_baseline_only_and_unqualified_mapping_grant_nothing():
    a = f.assess_interval([seg(0, 10)], [], start_ns=0, end_ns=10, as_of_ns=11)
    assert a[0]['state'] == 'UNKNOWN' and not a[0]['runtime_authority']
    with pytest.raises(IntegrityError, match='Unqualified'):
        f.recurring_segments(claim(status='NAMED_PRODUCT_ONLY'), left=f.LEFT, right=f.RIGHT)


def test_overlap_union_and_open_conflict():
    parts = f.assess_interval([seg(0, 10)], [seg(0, 7), seg(4, 10), seg(5, 6, 'OPEN')],
                             start_ns=0, end_ns=10, as_of_ns=11)
    assert sum(x['end_ns']-x['start_ns'] for x in parts) == 10
    assert sum(x['end_ns']-x['start_ns'] for x in parts if x['state']=='CANDIDATE_CLOSED') == 9
    assert any(x['state']=='CONFLICTING' for x in parts)
    assert not any(x['runtime_authority'] for x in parts)


@pytest.mark.parametrize('known,asof,state', [(None,15,'UNKNOWN'), (16,15,'UNKNOWN'),
                                            (15,15,'CANDIDATE_CLOSED'), (12,15,'CANDIDATE_CLOSED')])
def test_evidence_known_by_decision_not_by_closure(known, asof, state):
    parts = f.assess_interval([seg(0,10)], [seg(0,10,known=known)],
                             start_ns=0, end_ns=10, as_of_ns=asof)
    assert parts[0]['state']==state


def test_partial_closure_and_terminal_unknown_not_extended():
    parts = f.assess_interval([seg(0,10)], [seg(0,5)], start_ns=0,end_ns=10,as_of_ns=15)
    assert [x['state'] for x in parts] == ['CANDIDATE_CLOSED','UNKNOWN']


def test_prelisting_and_pause_are_not_fabricated():
    c = claim(effective_from_local='2020-01-02T17:00:00')
    assert f.recurring_segments(c,left=f.ns('2020-01-01'),right=f.ns('2020-01-02'))==[]
    c['additional_pause'] = dict(start='15:15', end='15:30')
    p = f.recurring_segments(c,left=f.ns('2020-01-03T21:15:00Z'),right=f.ns('2020-01-03T21:30:00Z'))
    assert all(x['state']=='OPEN' for x in p)


def test_hand_computed_all_anchors_no_price_or_permission_claim():
    left, right = f.ns('2020-01-04'), f.ns('2020-01-05')
    record=dict(segments=[seg(left,right)],available_at_ns=0,evidence_id='synthetic')
    counts=f.assess_market(claim(),[record],left=left,right=right)['2020']
    assert counts['1h']['candidate_fully_closed']==24
    assert counts['1d']['candidate_fully_closed']==1
    unknown=f.assess_market(claim(),[],left=left,right=right)['2020']
    assert unknown['1h']['contains_unknown']==24
    assert unknown['1d']['not_candidate_fully_closed']==1


def test_approval_and_changed_binding_rejected(tmp_path):
    from futures_rebuild.canonical import canonical_bytes, sha256_file
    from futures_rebuild.causal_timeframe_storage import signed
    import time
    (tmp_path/'bound.json').write_text('{}')
    scope=dict(schema='free_coverage_calendar_scope/1',
        operation='RETAINED_CALENDAR_CONJUNCTION_DIAGNOSTIC_ONLY',
        output=f.HERE+'/calendar_execution', publication=False, price_reads=False,
        dbn_decode=False, network=False, pointer_changes=[], market_data_inputs=[],
        bindings={'bound.json':sha256_file(tmp_path/'bound.json')})
    (tmp_path/'scope.json').write_bytes(canonical_bytes(signed(scope)))
    auth=dict(operation=scope['operation'], scope_sha256=sha256_file(tmp_path/'scope.json'),
              owner_approved=True, deadline_epoch=time.time()+120)
    (tmp_path/'auth.json').write_bytes(canonical_bytes(signed(auth)))
    assert f.authorized_scope(tmp_path,'scope.json','auth.json')[0]['price_reads'] is False
    (tmp_path/'bound.json').write_text('{"changed":true}')
    with pytest.raises(IntegrityError, match='Prepared binding changed'):
        f.authorized_scope(tmp_path,'scope.json','auth.json')
    auth['owner_approved']=False
    (tmp_path/'auth.json').write_bytes(canonical_bytes(signed(auth)))
    with pytest.raises(IntegrityError, match='owner-approved'):
        f.authorized_scope(tmp_path,'scope.json','auth.json')
    (tmp_path/'scope.json').write_text('{}')
    with pytest.raises(IntegrityError, match='identity'):
        f.authorized_scope(tmp_path,'scope.json','auth.json')


def test_diagnostic_rejects_activation_scope(tmp_path):
    from futures_rebuild.canonical import canonical_bytes
    from futures_rebuild.causal_timeframe_storage import signed
    (tmp_path/'scope.json').write_bytes(canonical_bytes(signed({'schema':'activation'})))
    (tmp_path/'auth.json').write_bytes(canonical_bytes(signed({})))
    with pytest.raises(IntegrityError, match='Unsupported diagnostic scope'):
        f.authorized_scope(tmp_path,'scope.json','auth.json')


def test_generated_worker_uses_supervisor_transport_status(tmp_path, monkeypatch):
    """Exercise the real worker envelope with generated inputs, no active readers."""
    import io
    import json
    import sys
    import time
    from futures_rebuild.causal_timeframe_storage import read_record
    output = tmp_path/f.HERE/'calendar_execution'
    output.mkdir(parents=True)
    (tmp_path/'claims.json').write_text('{"claims":[]}')
    (tmp_path/'candidate.json').write_text('{"records":[]}')
    scope = dict(output=f.HERE+'/calendar_execution', calendar_files=[], claims_path='claims.json', candidate_path='candidate.json',
        markets=['GENERATED_'+str(i) for i in range(41)],
        limits=dict(maximum_written_bytes=2**20, maximum_memory_bytes=5*2**30,
                    maximum_seconds=30, minimum_free_bytes=1, maximum_files=10))
    # Allow room for the worker's explicit terminal reserve.
    scope['limits']['maximum_written_bytes'] = 4*2**20
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(f, 'authorized_scope', lambda *a: (scope, dict(deadline_epoch=time.time()+30)))
    monkeypatch.setattr(sys, 'stdin', io.StringIO(json.dumps({'scope':'generated','authorization':'generated','output':scope['output']})))
    assert f.worker() == 0
    result = read_record(output/'result.json')
    assert result['status'] == 'PASS_RECOVERY_PHASE'
    assert result['diagnostic_status'] == 'PASS_CALENDAR_CONJUNCTION_DIAGNOSTIC_ONLY'
    assert result['publication'] is False and result['market_data_reads'] is False
    assessment = read_record(output/'candidate_assessment.json')
    assert assessment['runtime_closure_authority'] is False
    assert assessment['new_readable_feature_windows'] is None


def test_existing_attempt_is_never_overwritten(tmp_path):
    from futures_rebuild.canonical import canonical_bytes
    from futures_rebuild.causal_timeframe_storage import signed
    for destination in f.EXECUTIONS:
        out=tmp_path/destination
        out.mkdir(parents=True)
        (out/'preserved.txt').write_text('keep')
        (tmp_path/'scope.json').write_bytes(canonical_bytes(signed({'output':destination})))
        with pytest.raises(IntegrityError, match='no automatic retry'):
            f.run_approved(tmp_path,scope_path='scope.json',authorization_path='absent.json')
        assert (out/'preserved.txt').read_text()=='keep'


def test_real_supervisor_accepts_generated_transport_envelope(tmp_path):
    import os
    import sys
    import time
    from futures_rebuild.causal_timeframe_storage import Limits
    from futures_rebuild.daily_identity_rollout import supervise
    if os.name!='nt':
        pytest.skip('Existing supervisor is Windows-only')
    # Exercise the actual shared status gate, using only generated metadata.
    code = '''import json,sys
from pathlib import Path
from futures_rebuild.canonical import canonical_bytes
from futures_rebuild.causal_timeframe_storage import signed
from futures_rebuild.daily_identity_rollout import memory
r=json.load(sys.stdin)
record=signed(dict(status='PASS_RECOVERY_PHASE',diagnostic_status='PASS_CALENDAR_CONJUNCTION_DIAGNOSTIC_ONLY',memory=memory()))
Path(r['result_path']).write_bytes(canonical_bytes(record)+b'\\n')
'''
    measured=supervise([sys.executable,'-B','-c',code],tmp_path,
        dict(phase='generated_transport_test',result_path='generated.json',deadline_epoch=time.time()+30),
        Limits(maximum_written_bytes=2**20,maximum_memory_bytes=5*2**30,maximum_seconds=30,minimum_free_bytes=1),
        diagnostic=tmp_path/'supervision.json')
    assert measured['status']=='PASS_SUPERVISED_PHASE' and measured['exit_code']==0
