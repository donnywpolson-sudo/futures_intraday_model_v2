"""Synthetic rollout-only, integrity, memory diagnostics and exact rollback checks."""
import importlib.util
from pathlib import Path
import time

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.causal_timeframe_storage import read_record, signed
from futures_rebuild.errors import IntegrityError

pytestmark = pytest.mark.current
ROOT = Path(__file__).resolve().parents[1]
REL = 'state/unpublished_evidence/daily_admission_v1/rollout_memory_v1'


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


sup = load('test_rollout_memory_supervisor', REL + '/supervisor.py')
coord = load('test_rollout_memory_coordinator', REL + '/coordinator.py')
helper = load('test_memory_rollout_helper',
    'state/unpublished_evidence/daily_foundation_v1/prepare_coordinated_publication.py')


class HashBudget:
    def hash(self, path):
        return sha256_file(path)

    def check(self):
        pass


def test_policy_hash_identity_binding_and_conflict(tmp_path):
    (tmp_path / 'payload').write_bytes(b'certified')
    value = signed(dict(bindings={'payload':sha256_file(tmp_path / 'payload')}, entries=[{'day':1}]))
    policy = tmp_path / 'policy.json'
    policy.write_bytes(canonical_bytes(value))
    references = {'policy.json':sha256_file(policy)}
    assert sup.manifest_bindings(tmp_path, references, {}, HashBudget()) == value['bindings']
    policy.write_bytes(canonical_bytes(dict(value, entries=[{'day':2}])))
    with pytest.raises(IntegrityError, match='bytes changed'):
        sup.manifest_bindings(tmp_path, references, {}, HashBudget())
    with pytest.raises(IntegrityError, match='identity differs'):
        sup.manifest_bindings(tmp_path, {'policy.json':sha256_file(policy)}, {}, HashBudget())
    policy.write_bytes(canonical_bytes(value))
    (tmp_path / 'payload').write_bytes(b'changed')
    with pytest.raises(IntegrityError, match='binding drift'):
        sup.manifest_bindings(tmp_path, references, {}, HashBudget())


@pytest.mark.parametrize('kind', ['stage', 'finalize', 'validate', 'preflight', 'reconstruction'])
def test_only_rollout_phases(kind):
    with pytest.raises(IntegrityError, match='only recheck and default'):
        sup.require_kind(kind)


def test_missing_or_changed_worker_evidence(tmp_path):
    with pytest.raises(IntegrityError):
        sup.verify_result(tmp_path, 'result.json', 'supervision.json')
    (tmp_path / 'result.json').write_bytes(canonical_bytes(signed(dict(status='PASS_RECHECK'))))
    (tmp_path / 'supervision.json').write_bytes(canonical_bytes(signed(dict(
        status='PASS_SUPERVISED_PHASE', result_sha256='0'*64))))
    with pytest.raises(IntegrityError, match='worker evidence'):
        sup.verify_result(tmp_path, 'result.json', 'supervision.json')


def test_separate_memory_deadline_and_early_failure_record(tmp_path, monkeypatch):
    monkeypatch.setattr(sup, 'memory', lambda:dict(peak_bytes=sup.COORDINATOR_LIMIT+1,
        peak_working_set_bytes=123, peak_committed_bytes=sup.COORDINATOR_LIMIT+1))
    with pytest.raises(IntegrityError, match='coordinator peak.*exceeds'):
        sup.coordinator_gate(time.time()+60, 'test')
    monkeypatch.setattr(sup, 'memory', lambda:dict(peak_bytes=123, peak_working_set_bytes=123, peak_committed_bytes=100))
    with pytest.raises(IntegrityError, match='deadline.*elapsed'):
        sup.coordinator_gate(time.time()-1, 'test')
    monkeypatch.setattr(coord, 'ROOT', tmp_path)
    monkeypatch.setattr(coord, 'REL', 'attempt')
    monkeypatch.setattr(coord.supervisor, 'authorize', lambda: (_ for _ in ()).throw(IntegrityError('no approval')))
    with pytest.raises(IntegrityError, match='no approval'):
        coord.run()
    result = read_record(tmp_path / 'attempt/execution/activation.json')
    assert result['status'] == 'INCOMPLETE' and result['phase'] == 'authorization'
    assert result['coordinator_memory']['peak_bytes'] > 0
    assert not result['control_changes']


@pytest.mark.parametrize('failure', [0, 1, 2, 3, 4, 'default', 'memory', 'deadline'])
def test_every_replacement_and_default_failure_restores_exact_predecessors(tmp_path, monkeypatch, failure):
    changes, expected = {}, {}
    for n in range(5):
        destination = tmp_path / f'control{n}'
        if n < 4:
            destination.write_bytes(f'original{n}'.encode())
        candidate = tmp_path / f'candidate{n}'
        candidate.write_bytes(f'candidate{n}'.encode())
        changes[destination.name] = candidate.name
        expected[destination.name] = sha256_file(destination) if n < 4 else None
    real_replace = helper.os.replace
    def replace(source, destination):
        if isinstance(failure, int) and Path(source).name == f'{failure}.pending':
            raise OSError('injected replacement failure')
        return real_replace(source, destination)
    monkeypatch.setattr(helper.os, 'replace', replace)
    def validate():
        if isinstance(failure, str):
            raise IntegrityError('injected ' + failure)
    with pytest.raises((OSError, IntegrityError)):
        helper.activate_controls(tmp_path, changes, expected=expected, rollback_directory='rollback', validate=validate)
    assert all((tmp_path / f'control{n}').read_bytes() == f'original{n}'.encode() for n in range(4))
    assert not (tmp_path / 'control4').exists()


def test_changed_predecessor_prevents_any_replacement(tmp_path):
    (tmp_path / 'control').write_bytes(b'new writer')
    (tmp_path / 'candidate').write_bytes(b'candidate')
    with pytest.raises(IntegrityError, match='predecessor changed'):
        helper.activate_controls(tmp_path, {'control':'candidate'}, expected={'control':'0'*64},
            rollback_directory='rollback', validate=lambda:None)
    assert (tmp_path / 'control').read_bytes() == b'new writer'


def test_full_coordinator_uses_small_worker_results(tmp_path, monkeypatch):
    """Exercise actual pointer construction and commit; synthetic workers supply evidence."""
    monkeypatch.setattr(coord, 'ROOT', tmp_path)
    monkeypatch.setattr(coord.supervisor, 'ROOT', tmp_path)
    monkeypatch.setattr(coord.supervisor, 'module', lambda *args:helper)
    def record(relative, value):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical_bytes(signed(value))+b'\n')
        return sha256_file(path)
    validation = dict(status='PASS_VALIDATE', admission_sha256='d'*64, hourly_admission_sha256='e'*64,
        hourly_reader_checks=[], resources=dict(application_written_bytes=1000))
    digest = record(coord.BASE+'/execution/validate_census_resume/result.json', validation)
    record(coord.BASE+'/execution/validate_result.json', validation)
    record(coord.BASE+'/execution/validate_census_resume/supervision.json', dict(
        status='PASS_SUPERVISED_PHASE', result_sha256=digest, conservative_aggregate_peak_bytes=1000000000))
    record(coord.BASE+'/execution/admission_census_v2.json', dict(bindings={}, entries=[]))
    record(coord.BASE+'/execution/hourly_census_v2/admission.json', dict(bindings={}, entries=[]))
    record(coord.OLD+'/execution/quality.json', dict(status='SYNTHETIC'))
    record(coord.REL+'/authorization.json', dict(status='SYNTHETIC_ONLY'))
    controls = ['src/futures_rebuild/causal_timeframe_publication.py', 'data/active/timeframes.json',
        'configs/active_hourly_quality_reader.json', 'configs/active_hourly_source_admission.json',
        'configs/active_daily_source_admission.json']
    predecessors = {p:record(p, dict(result=dict(path='old', sha256='a'*64))) for p in controls[:-1]}
    predecessors[controls[-1]] = None
    # Include the real index's serialized byte scale in control-copy peak accounting.
    predecessors[controls[1]] = record(controls[1], dict(padding='x'*8366859))
    for n in range(3): record('candidates/'+str(n), dict(candidate=n, padding='y'*8366859 if n == 1 else ''))
    prefix = coord.REL+'/execution'
    changes = dict(zip(controls, ['candidates/0', 'candidates/1', 'candidates/2',
        prefix+'/hourly/admission_pointer.json', prefix+'/daily_pointer.json']))
    record(coord.OLD+'/plan.json', dict(bindings={p:h for p,h in predecessors.items() if h}))
    plan = dict(changes=changes, predecessors=predecessors, retained_bindings={})
    auth = dict(started_epoch=time.time(), deadline_epoch=time.time()+120)
    monkeypatch.setattr(coord.supervisor, 'authorize', lambda:(plan,auth))
    kinds = []
    def phase(kind, previous=None):
        kinds.append(kind)
        result = dict(status='PASS_'+kind.upper(), allocation=dict(retained_files=30, retained_allocated_bytes=10000),
            resources=dict(application_written_bytes=1000))
        digest = record(prefix+'/'+kind+'/result.json', result)
        record(prefix+'/'+kind+'/supervision.json', dict(status='PASS_SUPERVISED_PHASE', result_sha256=digest,
            conservative_aggregate_peak_bytes=1000000000))
        return result
    monkeypatch.setattr(coord.supervisor, 'run_phase', phase)
    coord.run()
    assert kinds == ['recheck', 'default']
    result = read_record(tmp_path/prefix/'activation.json')
    assert result['status'] == 'PASS_COORDINATED_ACTIVE_READERS'
    assert all(sha256_file(tmp_path/p) == sha256_file(tmp_path/c) for p,c in changes.items())
    print('SYNTHETIC_COORDINATOR_MEMORY', result['coordinator_memory'])
